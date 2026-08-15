from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any

NotificationHandler = Callable[[dict[str, Any]], Awaitable[None]]
RequestHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class CodexError(RuntimeError):
    pass


class CodexAppServer:
    """Async JSON-RPC client for `codex app-server --stdio`."""

    def __init__(self, executable: str = "codex", request_timeout: float | None = None) -> None:
        self.executable = executable
        self.request_timeout = request_timeout or float(
            os.environ.get("TRIX_CODEX_REQUEST_TIMEOUT", "60")
        )
        self._process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._handlers: list[NotificationHandler] = []
        self._request_handlers: dict[str, RequestHandler] = {}
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._request_id = 0
        self._write_lock = asyncio.Lock()
        self._stderr_tail = ""

    def on_notification(self, handler: NotificationHandler) -> None:
        self._handlers.append(handler)

    def on_request(self, method: str, handler: RequestHandler) -> None:
        self._request_handlers[method] = handler

    async def start(self) -> None:
        if self._process is not None:
            return
        self._process = await asyncio.create_subprocess_exec(
            self.executable,
            "app-server",
            "--stdio",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._reader_task = asyncio.create_task(self._read_loop())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        await self.request(
            "initialize",
            {
                "clientInfo": {"name": "trix", "title": "Trix", "version": "0.1.0"},
                "capabilities": {"experimentalApi": True},
            },
        )
        await self.notify("initialized", {})

    async def close(self) -> None:
        process = self._process
        self._process = None
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                await process.wait()
        if self._reader_task is not None:
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)
            self._reader_task = None
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            await asyncio.gather(self._stderr_task, return_exceptions=True)
            self._stderr_task = None
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def create_thread(
        self,
        cwd: Path,
        instructions: str,
        dynamic_tools: list[dict[str, Any]],
        *,
        read_only: bool = False,
    ) -> str:
        result = await self.request(
            "thread/start",
            {
                "cwd": str(cwd),
                "developerInstructions": instructions,
                "approvalPolicy": "on-request",
                "sandbox": "read-only" if read_only else "workspace-write",
                "ephemeral": False,
                "dynamicTools": dynamic_tools,
            },
        )
        thread = result.get("thread", {})
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not isinstance(thread_id, str):
            raise CodexError("thread/start did not return a thread id")
        return thread_id

    async def start_turn(self, thread_id: str, prompt: str) -> str:
        result = await self.request(
            "turn/start",
            {"threadId": thread_id, "input": [{"type": "text", "text": prompt}]},
        )
        turn = result.get("turn", {})
        turn_id = turn.get("id") if isinstance(turn, dict) else None
        if not isinstance(turn_id, str):
            raise CodexError("turn/start did not return a turn id")
        return turn_id

    async def interrupt(self, thread_id: str, turn_id: str) -> None:
        await self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

    async def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._request_id += 1
        request_id = self._request_id
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await self._send(
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
            )
            async with asyncio.timeout(self.request_timeout):
                return await future
        except TimeoutError as error:
            raise CodexError(
                f"Codex App Server did not respond to {method!r} within "
                f"{self.request_timeout:g} seconds"
            ) from error
        finally:
            self._pending.pop(request_id, None)

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        await self._send({"jsonrpc": "2.0", "method": method, "params": params})

    async def _send(self, payload: dict[str, Any]) -> None:
        if self._process is None or self._process.stdin is None:
            raise CodexError("Codex App Server is not running")
        async with self._write_lock:
            self._process.stdin.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
            await self._process.stdin.drain()

    async def _read_loop(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        while line := await self._process.stdout.readline():
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            request_id = payload.get("id")
            if request_id is not None and (future := self._pending.pop(request_id, None)):
                if "error" in payload:
                    future.set_exception(CodexError(str(payload["error"])))
                else:
                    result = payload.get("result", {})
                    future.set_result(result if isinstance(result, dict) else {"value": result})
            elif request_id is not None and isinstance(payload.get("method"), str):
                self._run_background(self._handle_server_request(payload))
            elif "method" in payload:
                self._run_background(self._handle_notification(payload))
        detail = f": {self._stderr_tail}" if self._stderr_tail else ""
        error = CodexError(f"Codex App Server exited{detail}")
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()

    async def _read_stderr(self) -> None:
        assert self._process is not None and self._process.stderr is not None
        while line := await self._process.stderr.readline():
            self._stderr_tail = line.decode(errors="replace").strip()[-2000:]

    def _run_background(self, coroutine: Coroutine[Any, Any, None]) -> None:
        task: asyncio.Task[None] = asyncio.create_task(coroutine)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _handle_notification(self, payload: dict[str, Any]) -> None:
        await asyncio.gather(*(handler(payload) for handler in self._handlers))

    async def _handle_server_request(self, payload: dict[str, Any]) -> None:
        method = str(payload["method"])
        request_id = payload["id"]
        handler = self._request_handlers.get(method)
        if handler is None:
            await self._send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": f"Unsupported server request: {method}"},
                }
            )
            return
        try:
            result = await handler(payload)
            response = {"jsonrpc": "2.0", "id": request_id, "result": result}
        except Exception as error:
            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(error)},
            }
        await self._send(response)
