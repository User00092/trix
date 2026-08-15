from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from trix.codex import CodexAppServer, CodexError
from trix.models import AgentStatus, SessionStatus
from trix.orchestrator import Orchestrator
from trix.store import Store
from trix.tools import tools_for


class FakeCodex:
    def __init__(self) -> None:
        self.notification_handler: Any = None
        self.request_handler: Any = None
        self.threads: list[dict[str, Any]] = []
        self.turns: list[tuple[str, str]] = []

    def on_notification(self, handler: Any) -> None:
        self.notification_handler = handler

    def on_request(self, method: str, handler: Any) -> None:
        assert method == "item/tool/call"
        self.request_handler = handler

    async def start(self) -> None:
        pass

    async def create_thread(
        self,
        cwd: Path,
        instructions: str,
        dynamic_tools: list[dict[str, Any]],
        *,
        read_only: bool = False,
    ) -> str:
        thread_id = f"thread-{len(self.threads)}"
        self.threads.append(
            {
                "id": thread_id,
                "cwd": cwd,
                "instructions": instructions,
                "tools": dynamic_tools,
                "read_only": read_only,
            }
        )
        return thread_id

    async def start_turn(self, thread_id: str, prompt: str) -> str:
        self.turns.append((thread_id, prompt))
        return f"turn-{len(self.turns)}"

    async def interrupt(self, thread_id: str, turn_id: str) -> None:
        pass


@pytest.mark.asyncio
async def test_codex_request_times_out_and_cleans_pending_request() -> None:
    client = CodexAppServer(request_timeout=0.01)

    async def send(_payload: dict[str, Any]) -> None:
        pass

    client._send = send  # type: ignore[method-assign]
    with pytest.raises(CodexError, match="did not respond"):
        await client.request("thread/start", {})
    assert client._pending == {}


@pytest.mark.asyncio
async def test_idle_turn_fails_agent_and_session(tmp_path: Path) -> None:
    codex = FakeCodex()
    store = Store(tmp_path / "idle.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    orchestrator._turn_idle_timeout = 0.01
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)

    await asyncio.sleep(0.03)

    agent = store.get_agent(session.root_agent_id or "")
    assert agent is not None
    assert agent.status == AgentStatus.FAILED
    assert agent.current_turn_id is None
    assert store.get_session(session.id).status == SessionStatus.FAILED  # type: ignore[union-attr]
    assert store.list_events(session.id)[-1].event_type == "agent_failed"


@pytest.mark.asyncio
async def test_restart_reconciles_orphaned_sessions(tmp_path: Path) -> None:
    store = Store(tmp_path / "restart.db")
    first = Orchestrator(store, FakeCodex())  # type: ignore[arg-type]
    session = await first.create_session("Build", "Implement feature", str(tmp_path))
    await first.start_session(session.id)

    restarted = Orchestrator(store, FakeCodex())  # type: ignore[arg-type]
    assert await restarted.reconcile_orphaned_sessions() == 1

    agent = store.get_agent(session.root_agent_id or "")
    assert agent is not None
    assert agent.status == AgentStatus.FAILED
    assert store.get_session(session.id).status == SessionStatus.FAILED  # type: ignore[union-attr]
    assert store.list_events(session.id)[-1].event_type == "session_failed"


def tool_call(thread_id: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": 10,
        "method": "item/tool/call",
        "params": {
            "threadId": thread_id,
            "turnId": "turn",
            "callId": "call",
            "namespace": "trix",
            "tool": tool,
            "arguments": arguments,
        },
    }


@pytest.mark.asyncio
async def test_manager_delegates_and_reviews_worker(tmp_path: Path) -> None:
    codex = FakeCodex()
    store = Store(tmp_path / "trix.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)

    manager = store.get_agent(session.root_agent_id or "")
    assert manager is not None
    assert codex.threads[0]["read_only"] is True
    assert "executive agent" in codex.threads[0]["instructions"]

    spawned = await codex.request_handler(
        tool_call(
            manager.codex_thread_id or "",
            "spawn_agent",
            {"name": "Builder", "role": "Engineer", "task": "Implement the feature"},
        )
    )
    assert spawned["success"] is True
    child = next(item for item in store.list_agents(session.id) if item.parent_id == manager.id)
    assert codex.threads[1]["read_only"] is False

    submitted = await codex.request_handler(
        tool_call(
            child.codex_thread_id or "",
            "submit_report",
            {
                "summary": "Feature implemented",
                "files_changed": ["feature.py"],
                "commands_run": ["pytest -q"],
                "verification_results": {"passed": 4},
            },
        )
    )
    assert submitted["success"] is True
    report = store.reports_for_agent(child.id)[0]
    assert any(report.id in prompt for _, prompt in codex.turns)

    reviewed = await codex.request_handler(
        tool_call(
            manager.codex_thread_id or "",
            "review_report",
            {"report_id": report.id, "accepted": True, "feedback": "Verified"},
        )
    )
    assert reviewed["success"] is True
    assert store.get_agent(child.id).status == AgentStatus.COMPLETED  # type: ignore[union-attr]

    completed = await codex.request_handler(
        tool_call(
            manager.codex_thread_id or "",
            "complete_session",
            {"summary": "Accepted", "verification": ["Reviewed diff", "Tests passed"]},
        )
    )
    assert completed["success"] is True
    assert store.get_session(session.id).status == SessionStatus.COMPLETED  # type: ignore[union-attr]

    await codex.notification_handler(
        {
            "method": "turn/completed",
            "params": {"threadId": manager.codex_thread_id},
        }
    )
    finished_manager = store.get_agent(manager.id)
    assert finished_manager is not None
    assert finished_manager.status == AgentStatus.COMPLETED
    assert finished_manager.current_activity == "Accepted"


def test_leaf_does_not_receive_delegation_tool() -> None:
    from trix.models import Agent

    leaf = Agent(session_id="s", parent_id="p", depth=2, name="Leaf", role="Worker", task="Work")
    names = {tool["name"] for tool in tools_for(leaf)[0]["tools"]}
    assert "spawn_agent" not in names
    assert "submit_report" in names


def test_provided_repository_becomes_session_authority(tmp_path: Path) -> None:
    configured_elsewhere = tmp_path / "outside-any-launch-directory"
    configured_elsewhere.mkdir()
    orchestrator = Orchestrator(Store(tmp_path / "path.db"), FakeCodex())  # type: ignore[arg-type]
    assert orchestrator.validate_repository(str(configured_elsewhere)) == configured_elsewhere


def test_repository_must_exist(tmp_path: Path) -> None:
    orchestrator = Orchestrator(Store(tmp_path / "path.db"), FakeCodex())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="existing directory"):
        orchestrator.validate_repository(str(tmp_path / "missing"))


@pytest.mark.asyncio
async def test_app_server_responds_to_server_initiated_requests() -> None:
    client = CodexAppServer()
    sent: list[dict[str, Any]] = []

    async def send(payload: dict[str, Any]) -> None:
        sent.append(payload)

    async def handler(payload: dict[str, Any]) -> dict[str, Any]:
        assert payload["params"]["tool"] == "spawn_agent"
        return {"contentItems": [{"type": "inputText", "text": "spawned"}], "success": True}

    client._send = send  # type: ignore[method-assign]
    client.on_request("item/tool/call", handler)
    await client._handle_server_request(tool_call("thread", "spawn_agent", {}))
    assert sent == [
        {
            "jsonrpc": "2.0",
            "id": 10,
            "result": {
                "contentItems": [{"type": "inputText", "text": "spawned"}],
                "success": True,
            },
        }
    ]
