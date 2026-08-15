from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any

NotificationHandler = Callable[[dict[str, Any]], Awaitable[None]]
RequestHandler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
TransportHandler = Callable[[], Coroutine[Any, Any, None]]


class CodexError(RuntimeError):
    pass


class CodexAppServer:
    """Async JSON-RPC client for `codex app-server --stdio`."""

    def __init__(self, executable: str = "codex", request_timeout: float | None = None) -> None:
        self.executable = executable
        self.request_timeout = request_timeout or float(
            os.environ.get("TRIX_CODEX_REQUEST_TIMEOUT", "60")
        )
        self.start_timeout = float(os.environ.get("TRIX_CODEX_START_TIMEOUT", "180"))
        self._process: asyncio.subprocess.Process | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._handlers: list[NotificationHandler] = []
        self._request_handlers: dict[str, RequestHandler] = {}
        self._transport_handlers: list[TransportHandler] = []
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._request_id = 0
        self._write_lock = asyncio.Lock()
        self._recovery_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._last_recovery = 0.0
        self._closing = False
        self._stderr_tail = ""

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    def on_notification(self, handler: NotificationHandler) -> None:
        self._handlers.append(handler)

    def on_request(self, method: str, handler: RequestHandler) -> None:
        self._request_handlers[method] = handler

    def on_transport_lost(self, handler: TransportHandler) -> None:
        """Register a callback invoked when the app server dies unexpectedly."""
        self._transport_handlers.append(handler)

    async def start(self) -> None:
        async with self._start_lock:
            if self.is_running:
                return
            self._closing = False
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
                timeout=self.start_timeout,
            )
            await self.notify("initialized", {})

    async def close(self) -> None:
        self._closing = True
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
        self._fail_pending(CodexError("Codex App Server connection closed"))

    async def recover_transport(self) -> None:
        """Restart a wedged transport, coalescing concurrent recovery attempts.

        Callers await a usable transport: when another task already restarted the
        server moments ago this returns only after that restart finished, never
        while the process is still down.
        """
        async with self._recovery_lock:
            now = asyncio.get_running_loop().time()
            if self.is_running and now - self._last_recovery < 10:
                return
            await self.close()
            await self.start()
            self._last_recovery = asyncio.get_running_loop().time()

    async def ensure_running(self) -> None:
        if not self.is_running:
            await self.recover_transport()

    async def resume_thread(self, thread_id: str) -> str:
        result = await self.request(
            "thread/resume",
            {"threadId": thread_id, "excludeTurns": True},
            timeout=self.start_timeout,
        )
        thread = result.get("thread", {})
        resumed_id = thread.get("id") if isinstance(thread, dict) else None
        if not isinstance(resumed_id, str):
            raise CodexError("thread/resume did not return a thread id")
        return resumed_id

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
            timeout=self.start_timeout,
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

    async def steer_turn(self, thread_id: str, turn_id: str, prompt: str) -> str:
        """Inject input into a running turn instead of interrupting it.

        Fails when `turn_id` is not the thread's active turn, which is how callers
        learn that a turn they believed active has already ended.
        """
        result = await self.request(
            "turn/steer",
            {
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "input": [{"type": "text", "text": prompt}],
            },
        )
        steered_id = result.get("turnId")
        return steered_id if isinstance(steered_id, str) else turn_id

    async def interrupt(self, thread_id: str, turn_id: str) -> None:
        await self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

    async def request(
        self, method: str, params: dict[str, Any], *, timeout: float | None = None
    ) -> dict[str, Any]:
        self._request_id += 1
        request_id = self._request_id
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        deadline = timeout or self.request_timeout
        try:
            await self._send(
                {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
            )
            async with asyncio.timeout(deadline):
                return await future
        except TimeoutError as error:
            raise CodexError(
                f"Codex App Server did not respond to {method!r} within {deadline:g} seconds"
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
            if request_id is not None and isinstance(payload.get("method"), str):
                self._run_background(self._handle_server_request(payload))
            elif request_id is not None and (future := self._pending.pop(request_id, None)):
                if "error" in payload:
                    future.set_exception(CodexError(str(payload["error"])))
                else:
                    result = payload.get("result", {})
                    future.set_result(result if isinstance(result, dict) else {"value": result})
            elif "method" in payload:
                self._run_background(self._handle_notification(payload))
        detail = f": {self._stderr_tail}" if self._stderr_tail else ""
        self._fail_pending(CodexError(f"Codex App Server exited{detail}"))
        if not self._closing:
            for handler in list(self._transport_handlers):
                self._run_background(handler())

    def _fail_pending(self, error: CodexError) -> None:
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
        await asyncio.gather(
            *(handler(payload) for handler in self._handlers), return_exceptions=True
        )

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
