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
        self.request_handlers: dict[str, Any] = {}
        self.transport_handlers: list[Any] = []
        self.threads: list[dict[str, Any]] = []
        self.turns: list[tuple[str, str]] = []
        self.steers: list[tuple[str, str, str]] = []
        self.active_turns: dict[str, str] = {}
        self.steerable = True
        self.failing_threads: set[int] = set()
        self.interrupted: list[tuple[str, str]] = []

    def on_notification(self, handler: Any) -> None:
        self.notification_handler = handler

    def on_request(self, method: str, handler: Any) -> None:
        self.request_handlers[method] = handler
        if method == "item/tool/call":
            self.request_handler = handler

    def on_transport_lost(self, handler: Any) -> None:
        self.transport_handlers.append(handler)

    @property
    def is_running(self) -> bool:
        return True

    async def start(self) -> None:
        pass

    async def ensure_running(self) -> None:
        pass

    async def recover_transport(self) -> None:
        pass

    async def create_thread(
        self,
        cwd: Path,
        instructions: str,
        dynamic_tools: list[dict[str, Any]],
        *,
        read_only: bool = False,
    ) -> str:
        if len(self.threads) in self.failing_threads:
            raise CodexError("thread/start failed")
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
        turn_id = f"turn-{len(self.turns)}"
        self.active_turns[thread_id] = turn_id
        return turn_id

    async def steer_turn(self, thread_id: str, turn_id: str, prompt: str) -> str:
        if not self.steerable or self.active_turns.get(thread_id) != turn_id:
            raise CodexError("turn/steer rejected: turn is not active")
        self.steers.append((thread_id, turn_id, prompt))
        self.turns.append((thread_id, prompt))
        return turn_id

    async def resume_thread(self, thread_id: str) -> str:
        return thread_id

    async def interrupt(self, thread_id: str, turn_id: str) -> None:
        self.interrupted.append((thread_id, turn_id))
        if self.active_turns.get(thread_id) == turn_id:
            self.active_turns.pop(thread_id)


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
async def test_idle_turn_replaces_thread_without_failing_agent(tmp_path: Path) -> None:
    codex = FakeCodex()
    store = Store(tmp_path / "idle.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    orchestrator._turn_idle_timeout = 0.01
    orchestrator._supervisor_interval = 0.01
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)

    await asyncio.sleep(0.06)
    await orchestrator.aclose()

    agent = store.get_agent(session.root_agent_id or "")
    assert agent is not None
    assert agent.status == AgentStatus.WORKING
    assert agent.current_turn_id is not None
    assert agent.codex_thread_id != "thread-0"
    assert store.get_session(session.id).status == SessionStatus.RUNNING  # type: ignore[union-attr]
    assert store.list_events(session.id)[-1].event_type == "agent_thread_recovered"


@pytest.mark.asyncio
async def test_restart_resumes_orphaned_sessions(tmp_path: Path) -> None:
    store = Store(tmp_path / "restart.db")
    first = Orchestrator(store, FakeCodex())  # type: ignore[arg-type]
    session = await first.create_session("Build", "Implement feature", str(tmp_path))
    await first.start_session(session.id)

    await first.aclose()
    restarted = Orchestrator(store, FakeCodex())  # type: ignore[arg-type]
    assert await restarted.reconcile_orphaned_sessions() == 1
    await restarted.aclose()

    agent = store.get_agent(session.root_agent_id or "")
    assert agent is not None
    assert agent.status == AgentStatus.WORKING
    assert agent.codex_thread_id == "thread-0"
    assert agent.current_turn_id is not None
    assert store.get_session(session.id).status == SessionStatus.RUNNING  # type: ignore[union-attr]
    assert store.list_events(session.id)[-1].event_type == "session_recovered"


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


async def spawn_child(codex: FakeCodex, manager_thread: str, name: str) -> dict[str, Any]:
    return await codex.request_handler(
        tool_call(
            manager_thread,
            "spawn_agent",
            {"name": name, "role": "Engineer", "task": f"Do {name}'s work"},
        )
    )


def turn_completed(thread_id: str, turn_id: str) -> dict[str, Any]:
    return {
        "method": "turn/completed",
        "params": {"threadId": thread_id, "turn": {"id": turn_id, "status": "completed"}},
    }


@pytest.mark.asyncio
async def test_both_child_reports_reach_a_busy_manager(tmp_path: Path) -> None:
    codex = FakeCodex()
    store = Store(tmp_path / "reports.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)
    manager = store.get_agent(session.root_agent_id or "")
    assert manager is not None
    manager_thread = manager.codex_thread_id or ""

    await spawn_child(codex, manager_thread, "Builder")
    await spawn_child(codex, manager_thread, "Tester")
    children = [item for item in store.list_agents(session.id) if item.parent_id == manager.id]
    assert len(children) == 2

    for child in children:
        await codex.request_handler(
            tool_call(
                child.codex_thread_id or "",
                "submit_report",
                {"summary": f"{child.name} finished"},
            )
        )

    reports = [store.reports_for_agent(child.id)[0] for child in children]
    delivered = " ".join(prompt for _, prompt in codex.turns)
    assert all(report.id in delivered for report in reports)
    assert orchestrator._pending_messages[manager.id] == []
    assert codex.interrupted == []
    refreshed = store.get_agent(manager.id)
    assert refreshed is not None
    assert refreshed.current_turn_id is not None
    await orchestrator.aclose()


@pytest.mark.asyncio
async def test_stale_turn_completion_cannot_clear_the_live_turn(tmp_path: Path) -> None:
    codex = FakeCodex()
    store = Store(tmp_path / "stale.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)
    manager = store.get_agent(session.root_agent_id or "")
    assert manager is not None

    await codex.notification_handler(
        turn_completed(manager.codex_thread_id or "", "turn-superseded")
    )

    refreshed = store.get_agent(manager.id)
    assert refreshed is not None
    assert refreshed.current_turn_id == manager.current_turn_id
    assert refreshed.status == AgentStatus.WORKING
    await orchestrator.aclose()


@pytest.mark.asyncio
async def test_idle_manager_is_restarted_instead_of_stalling(tmp_path: Path) -> None:
    codex = FakeCodex()
    store = Store(tmp_path / "idle-manager.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    orchestrator._supervisor_interval = 0.01
    orchestrator._idle_nudge_seconds = 0.01
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)
    manager = store.get_agent(session.root_agent_id or "")
    assert manager is not None

    await codex.notification_handler(
        turn_completed(manager.codex_thread_id or "", manager.current_turn_id or "")
    )
    idle = store.get_agent(manager.id)
    assert idle is not None
    assert idle.status == AgentStatus.IDLE

    await asyncio.sleep(0.06)
    await orchestrator.aclose()

    nudged = store.get_agent(manager.id)
    assert nudged is not None
    assert nudged.status == AgentStatus.WORKING
    assert nudged.current_turn_id is not None
    assert any("trix.complete_session" in prompt for _, prompt in codex.turns)


@pytest.mark.asyncio
async def test_failed_worker_wakes_its_parent_and_unblocks_completion(tmp_path: Path) -> None:
    codex = FakeCodex()
    codex.failing_threads = {1}
    store = Store(tmp_path / "failure.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)
    manager = store.get_agent(session.root_agent_id or "")
    assert manager is not None

    spawned = await spawn_child(codex, manager.codex_thread_id or "", "Builder")
    assert spawned["success"] is False
    child = next(item for item in store.list_agents(session.id) if item.parent_id == manager.id)
    assert child.status == AgentStatus.FAILED

    assert any("ended as failed" in prompt for _, prompt in codex.turns)
    assert orchestrator._pending_messages[manager.id] == []

    completed = await codex.request_handler(
        tool_call(
            manager.codex_thread_id or "",
            "complete_session",
            {"summary": "Delivered without the failed worker", "verification": ["Reviewed diff"]},
        )
    )
    assert completed["success"] is True
    assert store.get_session(session.id).status == SessionStatus.COMPLETED  # type: ignore[union-attr]
    await orchestrator.aclose()


@pytest.mark.asyncio
async def test_dismissing_a_stuck_worker_frees_a_delegation_slot(tmp_path: Path) -> None:
    codex = FakeCodex()
    store = Store(tmp_path / "dismiss.db")
    orchestrator = Orchestrator(store, codex)  # type: ignore[arg-type]
    session = await orchestrator.create_session("Build", "Implement feature", str(tmp_path))
    await orchestrator.start_session(session.id)
    manager = store.get_agent(session.root_agent_id or "")
    assert manager is not None
    manager_thread = manager.codex_thread_id or ""

    await spawn_child(codex, manager_thread, "Builder")
    await spawn_child(codex, manager_thread, "Tester")
    stuck = next(item for item in store.list_agents(session.id) if item.name == "Builder")
    grandchild = await orchestrator.spawn(stuck.id, "Helper", "Engineer", "Assist the builder")

    blocked = await spawn_child(codex, manager_thread, "Third")
    assert blocked["success"] is False

    dismissed = await codex.request_handler(
        tool_call(
            manager_thread,
            "dismiss_agent",
            {"agent_id": stuck.id, "reason": "Unresponsive after repeated nudges"},
        )
    )
    assert dismissed["success"] is True
    assert store.get_agent(stuck.id).status == AgentStatus.CANCELLED  # type: ignore[union-attr]
    assert store.get_agent(grandchild.id).status == AgentStatus.CANCELLED  # type: ignore[union-attr]

    replacement = await spawn_child(codex, manager_thread, "Replacement")
    assert replacement["success"] is True
    await orchestrator.aclose()


def test_leaf_does_not_receive_delegation_tool() -> None:
    from trix.models import Agent

    leaf = Agent(session_id="s", parent_id="p", depth=2, name="Leaf", role="Worker", task="Work")
    names = {tool["name"] for tool in tools_for(leaf)[0]["tools"]}
    assert "spawn_agent" not in names
    assert "submit_report" in names
    assert "run_command" in names


def test_manager_cannot_use_supervised_command_tool() -> None:
    from trix.models import Agent

    manager = Agent(session_id="s", depth=0, name="Manager", role="Manager", task="Work")
    names = {tool["name"] for tool in tools_for(manager)[0]["tools"]}
    assert "run_command" not in names


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
