from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
from asyncio.subprocess import DEVNULL, PIPE
from collections import defaultdict
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from trix.codex import CodexAppServer, CodexError
from trix.events import (
    COMMAND_ENDED_EVENTS,
    TOOL_CALL_ENDED_EVENTS,
    TURN_ENDED_EVENTS,
    codex_turn_id,
    is_stream_delta,
    normalize_codex_event,
)
from trix.models import (
    RUNNING_AGENT_STATUSES,
    TERMINAL_AGENT_STATUSES,
    Agent,
    AgentReport,
    AgentStatus,
    Event,
    ReportStatus,
    SessionStatus,
    TrixSession,
    utc_now,
)
from trix.policies import DelegationPolicy, PolicyViolation
from trix.prompts import instructions_for
from trix.store import Store
from trix.tools import tools_for

LOGGER = logging.getLogger(__name__)

Subscriber = Callable[[Event], Awaitable[None]]


class Orchestrator:
    """Owns agent lifecycle, message delivery, and stall recovery for a Codex tree.

    Every agent is driven by exactly one Codex turn at a time.  A single supervisor
    loop — not a task per turn — detects agents that stopped making progress, so a
    stalled worker can never wedge its siblings, its parent, or the queue of reports
    waiting to be reviewed.
    """

    def __init__(self, store: Store, codex: CodexAppServer) -> None:
        self.store = store
        self.codex = codex
        self.policy = DelegationPolicy()
        self._subscribers: dict[str, set[Subscriber]] = defaultdict(set)
        self._thread_agents: dict[str, str] = {}
        self._pending_messages: dict[str, list[tuple[str, AgentStatus, str]]] = defaultdict(list)
        self._agent_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last_progress: dict[str, float] = {}
        self._active_tool_calls: dict[str, int] = defaultdict(int)
        self._active_command: dict[str, tuple[float, str]] = {}
        self._delivered_reports: dict[str, float] = {}
        self._recovery_attempts: dict[str, int] = defaultdict(int)
        self._drive_failures: dict[str, int] = defaultdict(int)
        self._nudges: dict[str, int] = defaultdict(int)
        self._turn_idle_timeout = float(os.environ.get("TRIX_TURN_IDLE_TIMEOUT", "900"))
        self._command_timeout = float(os.environ.get("TRIX_COMMAND_TIMEOUT", "300"))
        self._idle_nudge_seconds = float(os.environ.get("TRIX_IDLE_NUDGE_SECONDS", "25"))
        self._redelivery_seconds = float(os.environ.get("TRIX_REPORT_REDELIVERY", "120"))
        self._supervisor_interval = float(os.environ.get("TRIX_SUPERVISOR_INTERVAL", "5"))
        self._max_recovery_attempts = int(os.environ.get("TRIX_MAX_COMMAND_RECOVERIES", "2"))
        self._max_nudges = int(os.environ.get("TRIX_MAX_IDLE_NUDGES", "5"))
        self._supervisor: asyncio.Task[None] | None = None
        self._transport_dirty = False
        self._lock = asyncio.Lock()
        self.codex.on_notification(self._on_codex_event)
        self.codex.on_transport_lost(self._on_transport_lost)
        self.codex.on_request("item/tool/call", self._on_tool_call)
        self.codex.on_request("item/commandExecution/requestApproval", self._on_command_approval)
        self.codex.on_request("item/fileChange/requestApproval", self._on_file_change_approval)
        self.codex.on_request("item/permissions/requestApproval", self._on_permissions_request)
        self.codex.on_request("item/tool/requestUserInput", self._on_user_input_request)
        self.codex.on_request("mcpServer/elicitation/request", self._on_elicitation_request)
        self.codex.on_request("execCommandApproval", self._on_legacy_approval)
        self.codex.on_request("applyPatchApproval", self._on_legacy_approval)

    # ------------------------------------------------------------------ sessions

    def validate_repository(self, requested: str) -> Path:
        path = Path(requested).expanduser().resolve()
        if not path.is_dir():
            raise PolicyViolation("Repository path must be an existing directory")
        return path

    async def create_session(self, title: str, prompt: str, repository_path: str) -> TrixSession:
        repository = self.validate_repository(repository_path)
        session = self.store.save_session(
            TrixSession(title=title, user_prompt=prompt, repository_path=str(repository))
        )
        manager = self.store.save_agent(
            Agent(
                session_id=session.id,
                depth=0,
                name="Manager",
                role="Trix Manager",
                task=prompt,
            )
        )
        session.root_agent_id = manager.id
        self.store.save_session(session)
        await self.emit(
            Event(
                session_id=session.id,
                agent_id=manager.id,
                event_type="agent_created",
                message="Manager created",
            )
        )
        return session

    async def start_session(self, session_id: str) -> TrixSession:
        session = self._session(session_id)
        if session.status not in {SessionStatus.CREATED, SessionStatus.FAILED}:
            raise PolicyViolation(f"Cannot start a session in state {session.status}")
        session.status = SessionStatus.RUNNING
        session.started_at = utc_now()
        self.store.save_session(session)
        await self.codex.start()
        self._ensure_supervisor()
        assert session.root_agent_id is not None
        await self._start_agent(self._agent(session.root_agent_id))
        return session

    async def reconcile_orphaned_sessions(self) -> int:
        """Resume persisted agents after an application or transport restart."""
        reconciled = 0
        running: list[TrixSession] = []
        for session in self.store.list_sessions():
            agents = self.store.list_agents(session.id)
            timed_out = bool(agents) and all(
                agent.status in {AgentStatus.COMPLETED, AgentStatus.CANCELLED}
                or (
                    agent.status == AgentStatus.FAILED
                    and (agent.error or "").startswith("Codex turn produced no activity")
                )
                for agent in agents
            )
            if session.status == SessionStatus.RUNNING or (
                session.status == SessionStatus.FAILED and timed_out
            ):
                session.status = SessionStatus.RUNNING
                session.completed_at = None
                self.store.save_session(session)
                running.append(session)
        if not running:
            return 0
        await self.codex.start()
        self._ensure_supervisor()
        for session in running:
            awaiting_reports: list[tuple[Agent, AgentReport]] = []
            for agent in self.store.list_agents(session.id):
                recoverable_timeout = agent.status == AgentStatus.FAILED and (
                    agent.error or ""
                ).startswith("Codex turn produced no activity")
                if agent.status in {AgentStatus.COMPLETED, AgentStatus.CANCELLED} or (
                    agent.status == AgentStatus.FAILED and not recoverable_timeout
                ):
                    continue
                if agent.status == AgentStatus.AWAITING_VERIFICATION:
                    reports = self.store.reports_for_agent(agent.id)
                    if reports and reports[-1].status == ReportStatus.SUBMITTED:
                        awaiting_reports.append((agent, reports[-1]))
                    continue
                if not agent.codex_thread_id:
                    await self._start_agent(agent)
                    continue
                await self._resume_agent(
                    agent,
                    "Trix reconnected to this persisted Codex thread after its application "
                    "transport restarted. Continue the same assigned task from the repository's "
                    "current state. Treat any command that lacked a completion event as failed, "
                    "use a bounded-output fallback, and do not restart the task from scratch.",
                )
            for child, report in awaiting_reports:
                self._delivered_reports.pop(report.id, None)
                await self._deliver_to_parent(child, report)
            await self.emit(
                Event(
                    session_id=session.id,
                    agent_id=session.root_agent_id,
                    event_type="session_recovered",
                    message="Reconnected persisted Codex agents after application restart",
                )
            )
            reconciled += 1
        return reconciled

    async def cancel_session(self, session_id: str) -> TrixSession:
        session = self._session(session_id)
        for agent in self.store.list_agents(session_id):
            self._forget_runtime_state(agent.id)
            if agent.codex_thread_id and agent.current_turn_id:
                with contextlib.suppress(Exception):
                    await self.codex.interrupt(agent.codex_thread_id, agent.current_turn_id)
            if agent.status not in {AgentStatus.COMPLETED, AgentStatus.FAILED}:
                agent.status = AgentStatus.CANCELLED
                agent.completed_at = utc_now()
                agent.current_activity = "Cancelled"
                agent.current_turn_id = None
                self.store.save_agent(agent)
        session.status = SessionStatus.CANCELLED
        session.completed_at = utc_now()
        self.store.save_session(session)
        await self.emit(
            Event(
                session_id=session.id, event_type="session_cancelled", message="Session cancelled"
            )
        )
        return session

    async def complete_session(
        self, manager_id: str, summary: str, verification: list[str]
    ) -> TrixSession:
        manager = self._agent(manager_id)
        if manager.depth != 0:
            raise PolicyViolation("Only the Manager can complete a session")
        agents = [
            item for item in self.store.list_agents(manager.session_id) if item.id != manager.id
        ]
        active = [item for item in agents if item.status not in TERMINAL_AGENT_STATUSES]
        if active:
            names = ", ".join(f"{item.name} ({item.status})" for item in active)
            raise PolicyViolation(f"Cannot complete while agents are still active: {names}")
        unreviewed = [
            report
            for item in agents
            for report in self.store.reports_for_agent(item.id)
            if report.status == ReportStatus.SUBMITTED
        ]
        if unreviewed:
            ids = ", ".join(report.id for report in unreviewed)
            raise PolicyViolation(f"Cannot complete while reports are unreviewed: {ids}")
        if not verification:
            raise PolicyViolation("Final verification evidence is required")
        abandoned = [item.name for item in agents if item.status != AgentStatus.COMPLETED]
        manager.status = AgentStatus.COMPLETED
        manager.completed_at = utc_now()
        manager.current_activity = summary[:500]
        manager.current_turn_id = None
        self.store.save_agent(manager)
        self._forget_runtime_state(manager.id)
        session = self._session(manager.session_id)
        session.status = SessionStatus.COMPLETED
        session.completed_at = utc_now()
        self.store.save_session(session)
        await self.emit(
            Event(
                session_id=session.id,
                agent_id=manager.id,
                event_type="session_completed",
                message=summary,
                raw_event={"verification": verification, "abandoned_agents": abandoned},
            )
        )
        return session

    # -------------------------------------------------------------------- agents

    async def spawn(self, parent_id: str, name: str, role: str, task: str) -> Agent:
        async with self._lock:
            parent = self._agent(parent_id)
            agents = self.store.list_agents(parent.session_id)
            self.policy.validate_spawn(parent, agents)
            child = self.store.save_agent(
                Agent(
                    session_id=parent.session_id,
                    parent_id=parent.id,
                    depth=parent.depth + 1,
                    name=name,
                    role=role,
                    task=task,
                )
            )
        await self.emit(
            Event(
                session_id=parent.session_id,
                agent_id=child.id,
                event_type="agent_spawned",
                message=f"{parent.name} spawned {child.name}",
            )
        )
        await self._start_agent(child)
        return child

    async def _start_agent(self, agent: Agent) -> None:
        session = self._session(agent.session_id)
        failure: str | None = None
        async with self._agent_locks[agent.id]:
            agent = self._agent(agent.id)
            agent.status = AgentStatus.STARTING
            agent.started_at = utc_now()
            agent.current_activity = "Starting Codex thread"
            self.store.save_agent(agent)
            self._touch(agent.id)
            try:
                thread_id = await self.codex.create_thread(
                    Path(session.repository_path),
                    instructions_for(agent),
                    tools_for(agent),
                    read_only=agent.depth == 0,
                )
                agent.codex_thread_id = thread_id
                self._thread_agents[thread_id] = agent.id
                agent.status = AgentStatus.WORKING
                agent.current_activity = "Working on assigned task"
                agent.current_turn_id = await self.codex.start_turn(thread_id, agent.task)
                self.store.save_agent(agent)
                self._touch(agent.id)
            except Exception as error:
                failure = str(error) or "Codex failed to start"
                agent.status = AgentStatus.FAILED
                agent.error = failure
                agent.current_activity = "Codex failed to start"
                agent.completed_at = utc_now()
                agent.current_turn_id = None
                self.store.save_agent(agent)
        if failure is None:
            return
        if agent.parent_id is None:
            session.status = SessionStatus.FAILED
            session.completed_at = utc_now()
            self.store.save_session(session)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type="agent_failed",
                message=failure,
            )
        )
        if agent.parent_id is not None:
            await self._notify_parent_of_failure(agent)
        raise CodexError(failure)

    async def dismiss_agent(self, agent_id: str, reason: str) -> int:
        """Cancel an agent and its descendants so a dead branch cannot block the tree."""
        root = self._agent(agent_id)
        agents = self.store.list_agents(root.session_id)
        children: dict[str, list[Agent]] = defaultdict(list)
        for item in agents:
            if item.parent_id:
                children[item.parent_id].append(item)
        queue = [root]
        dismissed = 0
        while queue:
            agent = queue.pop()
            queue.extend(children.get(agent.id, []))
            async with self._agent_locks[agent.id]:
                agent = self._agent(agent.id)
                if agent.status in TERMINAL_AGENT_STATUSES:
                    continue
                if agent.codex_thread_id and agent.current_turn_id:
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(
                            self.codex.interrupt(agent.codex_thread_id, agent.current_turn_id),
                            timeout=10,
                        )
                agent.status = AgentStatus.CANCELLED
                agent.completed_at = utc_now()
                agent.current_turn_id = None
                agent.current_activity = f"Dismissed by parent: {reason}"[:500]
                self.store.save_agent(agent)
                self._forget_runtime_state(agent.id)
                dismissed += 1
            await self.emit(
                Event(
                    session_id=agent.session_id,
                    agent_id=agent.id,
                    event_type="agent_dismissed",
                    message=f"{agent.name} was dismissed: {reason}",
                )
            )
        return dismissed

    async def instruct(self, agent_id: str, message: str) -> Agent:
        agent = self._agent(agent_id)
        if not agent.codex_thread_id:
            raise PolicyViolation("Agent has no Codex thread")
        async with self._agent_locks[agent.id]:
            agent = self._agent(agent_id)
            delivered = await self._deliver_message(agent, message)
            if not delivered:
                raise PolicyViolation("Codex did not accept the instruction; it will be retried")
            agent.status = AgentStatus.WORKING
            agent.current_activity = "Following up on an instruction"
            self.store.save_agent(agent)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type="agent_instruction",
                message="Received a follow-up instruction",
            )
        )
        return agent

    # ------------------------------------------------------------------- reports

    async def submit_report(self, agent_id: str, report: AgentReport) -> AgentReport:
        agent = self._agent(agent_id)
        if report.agent_id != agent.id:
            raise PolicyViolation("Report agent id does not match URL agent id")
        children = [
            item for item in self.store.list_agents(agent.session_id) if item.parent_id == agent.id
        ]
        unfinished = [item for item in children if item.status not in TERMINAL_AGENT_STATUSES]
        if unfinished:
            raise PolicyViolation("Agent cannot report while direct children are unfinished")
        self.store.save_report(report)
        async with self._agent_locks[agent.id]:
            agent = self._agent(agent_id)
            agent.status = (
                AgentStatus.AWAITING_VERIFICATION if agent.parent_id else AgentStatus.VERIFYING
            )
            agent.current_activity = (
                "Awaiting parent verification"
                if agent.parent_id
                else "Performing final verification"
            )
            self.store.save_agent(agent)
            self._touch(agent.id)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type="report_submitted",
                message=f"{agent.name} submitted a completion report",
            )
        )
        if agent.parent_id:
            await self._deliver_to_parent(agent, report)
        return report

    async def review_report(self, report_id: str, accepted: bool, feedback: str) -> AgentReport:
        report = self.store.get_report(report_id)
        if report is None:
            raise KeyError(report_id)
        report.status = ReportStatus.ACCEPTED if accepted else ReportStatus.REJECTED
        report.parent_feedback = feedback
        report.reviewed_at = utc_now()
        self.store.save_report(report)
        self._delivered_reports.pop(report.id, None)
        agent = self._agent(report.agent_id)
        async with self._agent_locks[agent.id]:
            agent = self._agent(report.agent_id)
            if accepted:
                agent.status = AgentStatus.COMPLETED
                agent.completed_at = utc_now()
                agent.current_activity = "Work accepted"
                agent.current_turn_id = None
                self.store.save_agent(agent)
                self._forget_runtime_state(agent.id)
            else:
                agent.status = AgentStatus.WORKING
                agent.current_activity = "Addressing rejected report"
                agent.error = None
                self.store.save_agent(agent)
                self._nudges[agent.id] = 0
                self._touch(agent.id)
                await self._deliver_message(
                    agent,
                    "Your parent rejected report "
                    f"{report.id}. Feedback: {feedback or 'no feedback provided'}\n"
                    "Address the feedback and submit a new report with trix.submit_report.",
                )
                self.store.save_agent(agent)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type=f"report_{report.status}",
                message=f"{agent.name} report {report.status}",
            )
        )
        if accepted:
            await self._wake_parent(agent)
        return report

    # -------------------------------------------------------------- codex events

    async def _on_codex_event(self, payload: dict[str, Any]) -> None:
        if is_stream_delta(payload):
            return
        params: dict[str, Any] = (
            payload["params"] if isinstance(payload.get("params"), dict) else {}
        )
        thread_id = params.get("threadId")
        if not isinstance(thread_id, str) or thread_id not in self._thread_agents:
            return
        agent_id = self._thread_agents[thread_id]
        try:
            agent = self._agent(agent_id)
        except KeyError:
            return
        event = normalize_codex_event(agent.session_id, agent.id, payload)
        turn_id = codex_turn_id(payload)
        async with self._agent_locks[agent_id]:
            agent = self._agent(agent_id)
            stale = bool(turn_id and agent.current_turn_id and turn_id != agent.current_turn_id)
            if stale or agent.status in TERMINAL_AGENT_STATUSES:
                # A superseded turn, or work that finished through a tool call before
                # Codex closed the surrounding turn: record it, never reopen it.
                await self.emit(event)
                return
            if turn_id and not agent.current_turn_id and event.event_type not in TURN_ENDED_EVENTS:
                # `turn/started` can beat the `turn/start` response back to us.
                agent.current_turn_id = turn_id
            self._touch(agent_id)
            self._track_activity(agent, event)
            if event.event_type in TURN_ENDED_EVENTS:
                agent.current_turn_id = None
                self._active_command.pop(agent_id, None)
                self._recompute_after_turn(agent)
            elif event.event_type in {"agent_failed", "agent_retrying"}:
                # A Codex-level error ends work only if the turn itself ends; keep the
                # agent alive so the turn-ended path and the supervisor can recover it.
                agent.error = event.message
                agent.current_activity = event.message[:500]
            else:
                agent.current_activity = event.message[:500]
            self.store.save_agent(agent)
        await self.emit(event)
        if event.event_type in TURN_ENDED_EVENTS:
            await self._flush_pending(self._agent(agent_id))

    def _track_activity(self, agent: Agent, event: Event) -> None:
        """Track in-flight commands and real progress for stall detection.

        Only work with an effect on the repository or the agent tree clears the nudge
        budget; otherwise an agent that answers every nudge with prose alone would be
        nudged forever.
        """
        if event.event_type == "command_started":
            self._active_command[agent.id] = (self._now(), event.message)
        elif event.event_type in COMMAND_ENDED_EVENTS:
            self._active_command.pop(agent.id, None)
            self._recovery_attempts[agent.id] = 0
            self._nudges[agent.id] = 0
        elif event.event_type in TOOL_CALL_ENDED_EVENTS:
            self._recovery_attempts[agent.id] = 0
            self._nudges[agent.id] = 0
        elif event.event_type == "file_changed":
            self._nudges[agent.id] = 0

    def _recompute_after_turn(self, agent: Agent) -> None:
        """Derive the agent's state once its Codex turn has ended."""
        reports = self.store.reports_for_agent(agent.id)
        latest = reports[-1] if reports else None
        active_children = [
            item
            for item in self.store.list_agents(agent.session_id)
            if item.parent_id == agent.id and item.status not in TERMINAL_AGENT_STATUSES
        ]
        if latest is not None and latest.status == ReportStatus.SUBMITTED and agent.parent_id:
            agent.status = AgentStatus.AWAITING_VERIFICATION
            agent.current_activity = "Awaiting parent verification"
        elif active_children:
            agent.status = AgentStatus.WAITING_FOR_CHILDREN
            agent.current_activity = f"Waiting for {len(active_children)} child agent(s)"
        else:
            agent.status = AgentStatus.IDLE
            agent.current_activity = (
                "Idle between decisions" if agent.depth == 0 else "Idle; no report submitted yet"
            )

    async def _on_transport_lost(self) -> None:
        """Every live turn died with the app server; let the supervisor rebuild them."""
        self._transport_dirty = True

    # ------------------------------------------------------- codex tool requests

    async def _on_tool_call(self, payload: dict[str, Any]) -> dict[str, Any]:
        params = payload.get("params")
        if not isinstance(params, dict):
            return self._tool_result("Invalid tool parameters", success=False)
        thread_id = params.get("threadId")
        if not isinstance(thread_id, str) or thread_id not in self._thread_agents:
            return self._tool_result("Unknown Trix agent thread", success=False)
        agent = self._agent(self._thread_agents[thread_id])
        if params.get("namespace") != "trix":
            return self._tool_result("Unsupported tool namespace", success=False)
        arguments = params.get("arguments")
        if not isinstance(arguments, dict):
            arguments = {}
        self._active_tool_calls[agent.id] += 1
        self._touch(agent.id)
        try:
            result = await self._execute_tool(agent, str(params.get("tool", "")), arguments)
        except (KeyError, PolicyViolation, TypeError, ValueError) as error:
            return self._tool_result(str(error), success=False)
        except CodexError as error:
            return self._tool_result(f"Trix could not reach Codex: {error}", success=False)
        finally:
            self._active_tool_calls[agent.id] -= 1
            self._touch(agent.id)
        return self._tool_result(result)

    async def _on_command_approval(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._record_approval(payload, "a command that needs sandbox escalation")
        return {"decision": "decline"}

    async def _on_file_change_approval(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._record_approval(payload, "a file change outside the workspace sandbox")
        return {"decision": "decline"}

    async def _on_permissions_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._record_approval(payload, "additional filesystem or network permissions")
        return {"permissions": {}, "scope": "turn"}

    async def _on_user_input_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._record_approval(payload, "interactive user input")
        params = payload.get("params")
        questions = params.get("questions") if isinstance(params, dict) else None
        answers: dict[str, Any] = {}
        for question in questions if isinstance(questions, list) else []:
            if isinstance(question, dict) and isinstance(question.get("id"), str):
                answers[question["id"]] = {
                    "answers": ["No user is available; decide autonomously and continue."]
                }
        return {"answers": answers}

    async def _on_elicitation_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._record_approval(payload, "an MCP elicitation")
        return {"action": "decline"}

    async def _on_legacy_approval(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._record_approval(payload, "a legacy approval")
        return {"decision": "denied"}

    async def _record_approval(self, payload: dict[str, Any], subject: str) -> None:
        """Answer Codex approval prompts immediately; an unanswered prompt hangs a turn."""
        params = payload.get("params")
        thread_id = params.get("threadId") if isinstance(params, dict) else None
        agent_id = self._thread_agents.get(thread_id) if isinstance(thread_id, str) else None
        if agent_id is None:
            return
        agent = self.store.get_agent(agent_id)
        if agent is None:
            return
        self._touch(agent_id)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type="approval_declined",
                message=(
                    f"Trix declined {subject}. Work within the sandbox and treat this as a "
                    "failed operation rather than waiting for a human."
                ),
                raw_event=payload,
            )
        )

    async def _execute_tool(self, caller: Agent, tool: str, arguments: dict[str, Any]) -> str:
        available = {
            item["name"] for namespace in tools_for(caller) for item in namespace.get("tools", [])
        }
        if tool not in available:
            raise PolicyViolation(f"Tool {tool!r} is not available at depth {caller.depth}")
        if tool == "spawn_agent":
            child = await self.spawn(
                caller.id,
                self._string(arguments, "name"),
                self._string(arguments, "role"),
                self._string(arguments, "task"),
            )
            return f"Spawned {child.name} as agent {child.id} at depth {child.depth}."
        if tool == "list_agents":
            agents = self.store.list_agents(caller.session_id)
            return json.dumps(
                [
                    {
                        "id": item.id,
                        "parent_id": item.parent_id,
                        "depth": item.depth,
                        "name": item.name,
                        "role": item.role,
                        "status": item.status,
                        "activity": item.current_activity,
                        "error": item.error,
                    }
                    for item in agents
                ]
            )
        if tool == "get_agent":
            target = self._agent_in_session(caller, self._string(arguments, "agent_id"))
            return json.dumps(
                {
                    **target.model_dump(mode="json"),
                    "reports": [
                        report.model_dump(mode="json")
                        for report in self.store.reports_for_agent(target.id)
                    ],
                }
            )
        if tool == "send_instruction":
            target = self._direct_child(caller, self._string(arguments, "agent_id"))
            if target.status in TERMINAL_AGENT_STATUSES:
                raise PolicyViolation(
                    f"{target.name} is {target.status} and cannot receive instructions. "
                    "Spawn a replacement worker instead."
                )
            await self.instruct(target.id, self._string(arguments, "message"))
            return f"Instruction delivered to {target.name}."
        if tool == "dismiss_agent":
            target = self._direct_child(caller, self._string(arguments, "agent_id"))
            dismissed = await self.dismiss_agent(target.id, self._string(arguments, "reason"))
            return f"{target.name} and its descendants were cancelled ({dismissed} agents)."
        if tool == "review_report":
            report_id = self._string(arguments, "report_id")
            report = self.store.get_report(report_id)
            if report is None:
                raise KeyError(report_id)
            target = self._direct_child(caller, report.agent_id)
            accepted = arguments.get("accepted")
            if not isinstance(accepted, bool):
                raise TypeError("accepted must be a boolean")
            feedback = arguments.get("feedback", "")
            if not isinstance(feedback, str):
                raise TypeError("feedback must be a string")
            if report.status != ReportStatus.SUBMITTED:
                raise PolicyViolation(f"Report {report.id} was already {report.status}")
            await self.review_report(report.id, accepted, feedback)
            return f"{target.name}'s report was {'accepted' if accepted else 'rejected'}."
        if tool == "submit_report":
            fields = {
                key: value
                for key, value in arguments.items()
                if key in AgentReport.model_fields and key not in {"id", "agent_id", "status"}
            }
            report = AgentReport(agent_id=caller.id, **fields)
            await self.submit_report(caller.id, report)
            return f"Report {report.id} submitted to the parent for verification."
        if tool == "inspect_changes":
            return await self._inspect_changes(caller)
        if tool == "run_command":
            return await self._run_command(
                caller,
                self._string(arguments, "command"),
                arguments.get("timeout_seconds", 120),
            )
        if tool == "complete_session":
            verification = arguments.get("verification")
            if not isinstance(verification, list) or not all(
                isinstance(item, str) for item in verification
            ):
                raise TypeError("verification must be a list of strings")
            await self.complete_session(caller.id, self._string(arguments, "summary"), verification)
            return "Session completed and accepted."
        raise PolicyViolation(f"Unsupported Trix tool: {tool}")

    # ------------------------------------------------------------------ commands

    async def _inspect_changes(self, caller: Agent) -> str:
        session = self._session(caller.session_id)
        output: list[str] = []
        for command in (["git", "status", "--short"], ["git", "diff", "--stat"], ["git", "diff"]):
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=session.repository_path,
                stdin=DEVNULL,
                stdout=PIPE,
                stderr=PIPE,
                env=self._command_env(),
            )
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)
            except TimeoutError as error:
                await self._terminate(process)
                raise PolicyViolation(
                    f"{' '.join(command)} did not finish within 120 seconds"
                ) from error
            if process.returncode != 0:
                raise PolicyViolation(stderr.decode(errors="replace") or "Git inspection failed")
            output.append(f"$ {' '.join(command)}\n{stdout.decode(errors='replace')}")
        return "\n".join(output)[:50_000]

    async def _run_command(self, caller: Agent, command: str, timeout_seconds: Any) -> str:
        if caller.depth == 0:
            raise PolicyViolation("The Manager cannot run repository commands")
        if not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= 900:
            raise TypeError("timeout_seconds must be an integer from 1 through 900")
        session = self._session(caller.session_id)
        executable = (
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command]
            if sys.platform == "win32"
            else ["/bin/sh", "-lc", command]
        )
        # A new session/group makes the whole descendant tree killable on timeout.
        group: dict[str, Any] = {} if sys.platform == "win32" else {"start_new_session": True}
        process = await asyncio.create_subprocess_exec(
            *executable,
            cwd=session.repository_path,
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            env=self._command_env(),
            **group,
        )
        timed_out = False
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError:
            timed_out = True
            stdout, stderr = await self._terminate(process)
        limit = 50_000
        stdout_text = stdout.decode(errors="replace")
        stderr_text = stderr.decode(errors="replace")
        result = {
            "command": command,
            "exit_code": process.returncode,
            "timed_out": timed_out,
            "stdout": stdout_text[-limit:],
            "stderr": stderr_text[-limit:],
            "stdout_truncated": len(stdout_text) > limit,
            "stderr_truncated": len(stderr_text) > limit,
        }
        event_type = (
            "command_timed_out"
            if timed_out
            else ("command_completed" if process.returncode == 0 else "command_failed")
        )
        await self.emit(
            Event(
                session_id=caller.session_id,
                agent_id=caller.id,
                event_type=event_type,
                message=(
                    f"Supervised command timed out after {timeout_seconds} seconds"
                    if timed_out
                    else f"Supervised command exited with code {process.returncode}"
                ),
                raw_event=result,
            )
        )
        return json.dumps(result)

    @staticmethod
    def _command_env() -> dict[str, str]:
        """Keep supervised commands non-interactive so they cannot block on a prompt."""
        return {
            **os.environ,
            "GIT_PAGER": "cat",
            "PAGER": "cat",
            "GIT_TERMINAL_PROMPT": "0",
            "CI": "1",
            "PYTHONUNBUFFERED": "1",
        }

    @staticmethod
    async def _terminate(process: asyncio.subprocess.Process) -> tuple[bytes, bytes]:
        """Kill a command and its descendants, then drain whatever output exists."""
        if process.returncode is None:
            if sys.platform == "win32":
                killer = await asyncio.create_subprocess_exec(
                    "taskkill",
                    "/F",
                    "/T",
                    "/PID",
                    str(process.pid),
                    stdin=DEVNULL,
                    stdout=DEVNULL,
                    stderr=DEVNULL,
                )
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(killer.wait(), timeout=10)
            else:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            with contextlib.suppress(Exception):
                process.kill()
        try:
            return await asyncio.wait_for(process.communicate(), timeout=10)
        except Exception:
            # A surviving grandchild still holds the pipes; do not wait on it forever.
            return b"", b""

    # ------------------------------------------------------- message dispatching

    async def _deliver_to_parent(self, child: Agent, report: AgentReport) -> None:
        assert child.parent_id is not None
        parent = self._agent(child.parent_id)
        if parent.status in TERMINAL_AGENT_STATUSES:
            return
        message = (
            f"Child agent {child.name} ({child.id}) submitted report {report.id}.\n"
            f"Summary: {report.summary}\nFiles changed: {report.files_changed}\n"
            f"Verification: {report.verification_results}\nKnown issues: {report.known_issues}\n"
            "Inspect the actual changes and use trix.review_report to accept or reject it."
        )
        self._delivered_reports[report.id] = self._now()
        self._enqueue(parent.id, message, AgentStatus.VERIFYING, "Reviewing child work")
        await self._flush_pending(parent)

    async def _notify_parent_of_failure(self, child: Agent) -> None:
        """Never let a dead child leave its parent waiting for a report that cannot come."""
        if child.parent_id is None:
            return
        parent = self.store.get_agent(child.parent_id)
        if parent is None or parent.status in TERMINAL_AGENT_STATUSES:
            return
        self._enqueue(
            parent.id,
            f"Child agent {child.name} ({child.id}) ended as {child.status} without an accepted "
            f"report. Reason: {child.error or child.current_activity}\n"
            "Its task is unfinished. Inspect the repository, then either spawn a replacement "
            "worker for the remaining work or re-scope it; do not wait for this agent.",
            AgentStatus.WORKING,
            f"Handling {child.name}'s failure",
        )
        await self._flush_pending(parent)

    async def _wake_parent(self, child: Agent) -> None:
        """Re-drive a parent that has nothing left to wait for."""
        if child.parent_id is None:
            return
        parent = self.store.get_agent(child.parent_id)
        if parent is None or parent.status in TERMINAL_AGENT_STATUSES:
            return
        await self._flush_pending(parent)

    def _enqueue(self, agent_id: str, message: str, status: AgentStatus, activity: str) -> None:
        queue = self._pending_messages[agent_id]
        entry = (message, status, activity)
        if entry not in queue:
            queue.append(entry)

    async def _flush_pending(self, agent: Agent) -> None:
        async with self._agent_locks[agent.id]:
            await self._flush_pending_locked(self._agent(agent.id))

    async def _flush_pending_locked(self, agent: Agent) -> None:
        queue = self._pending_messages[agent.id]
        if not queue or agent.status in TERMINAL_AGENT_STATUSES or not agent.codex_thread_id:
            return
        message, status, activity = queue[0]
        if not await self._deliver_message(agent, message):
            agent.current_activity = "Waiting to reach Codex with a queued update"
            self.store.save_agent(agent)
            return
        queue.pop(0)
        agent.status = status
        agent.current_activity = activity
        agent.error = None
        self._nudges[agent.id] = 0
        self.store.save_agent(agent)
        self._touch(agent.id)

    async def _deliver_message(self, agent: Agent, message: str) -> bool:
        """Steer the live turn when there is one, otherwise start a fresh turn.

        Steering avoids the interrupt/restart race that used to drop the Manager's
        turn id and strand queued child reports.
        """
        if not agent.codex_thread_id:
            return False
        if agent.current_turn_id:
            try:
                agent.current_turn_id = await self.codex.steer_turn(
                    agent.codex_thread_id, agent.current_turn_id, message
                )
                self._drive_failures[agent.id] = 0
                self._touch(agent.id)
                return True
            except CodexError:
                # The turn ended (or was never active); fall through to a new turn.
                agent.current_turn_id = None
        return await self._start_turn(agent, message)

    async def _start_turn(self, agent: Agent, prompt: str) -> bool:
        if not agent.codex_thread_id:
            return False
        try:
            await self.codex.ensure_running()
            agent.current_turn_id = await self.codex.start_turn(agent.codex_thread_id, prompt)
        except Exception as first_error:
            try:
                resumed = await self.codex.resume_thread(agent.codex_thread_id)
                self._thread_agents.pop(agent.codex_thread_id, None)
                self._thread_agents[resumed] = agent.id
                agent.codex_thread_id = resumed
                agent.current_turn_id = await self.codex.start_turn(resumed, prompt)
            except Exception as error:
                agent.error = f"{first_error} / {error}"
                self._drive_failures[agent.id] += 1
                self.store.save_agent(agent)
                if self._drive_failures[agent.id] >= 2:
                    await self._replace_agent_thread_locked(agent, str(error))
                    return agent.current_turn_id is not None
                return False
        self._drive_failures[agent.id] = 0
        agent.error = None
        self.store.save_agent(agent)
        self._touch(agent.id)
        return True

    # ----------------------------------------------------------------- supervisor

    def _ensure_supervisor(self) -> None:
        if self._supervisor is None or self._supervisor.done():
            self._supervisor = asyncio.create_task(self._supervise())

    async def aclose(self) -> None:
        if self._supervisor is not None:
            self._supervisor.cancel()
            await asyncio.gather(self._supervisor, return_exceptions=True)
            self._supervisor = None

    async def _supervise(self) -> None:
        while True:
            await asyncio.sleep(self._supervisor_interval)
            try:
                await self._supervise_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - the loop must outlive any single tick
                LOGGER.exception("Trix supervisor tick failed")

    async def _supervise_once(self) -> None:
        if self._transport_dirty:
            self._transport_dirty = False
            await self._rebuild_after_transport_loss()
        for session in self.store.list_sessions():
            if session.status != SessionStatus.RUNNING:
                continue
            agents = self.store.list_agents(session.id)
            children: dict[str, list[Agent]] = defaultdict(list)
            for agent in agents:
                if agent.parent_id:
                    children[agent.parent_id].append(agent)
            for agent in agents:
                await self._supervise_agent(agent, children.get(agent.id, []))

    async def _supervise_agent(self, agent: Agent, children: list[Agent]) -> None:
        if agent.status in TERMINAL_AGENT_STATUSES:
            return
        now = self._now()
        if self._active_tool_calls[agent.id] > 0:
            # Trix is running work on the agent's behalf; that is progress, not a stall.
            self._touch(agent.id)
            return
        idle_for = now - self._last_progress.setdefault(agent.id, now)
        if self._pending_messages[agent.id]:
            await self._flush_pending(agent)
            return
        if agent.current_turn_id:
            command = self._active_command.get(agent.id)
            if command is not None and now - command[0] >= self._command_timeout:
                await self._recover_stalled_command(agent, command[1])
            elif idle_for >= self._turn_idle_timeout:
                await self._replace_agent_thread(
                    agent, f"Codex turn produced no activity for {idle_for:.0f} seconds"
                )
            return
        if not agent.codex_thread_id:
            # Spawned but never attached to Codex; start it rather than nudging nothing.
            if idle_for >= self._idle_nudge_seconds:
                with contextlib.suppress(Exception):
                    await self._start_agent(agent)
            return
        if agent.status == AgentStatus.AWAITING_VERIFICATION:
            await self._ensure_report_delivery(agent)
            return
        if agent.status == AgentStatus.WAITING_FOR_CHILDREN and any(
            child.status not in TERMINAL_AGENT_STATUSES for child in children
        ):
            return
        if idle_for >= self._idle_nudge_seconds:
            await self._nudge(agent, children)

    async def _ensure_report_delivery(self, agent: Agent) -> None:
        """Re-hand a submitted report to a parent that never started reviewing it."""
        reports = self.store.reports_for_agent(agent.id)
        if not reports or reports[-1].status != ReportStatus.SUBMITTED or not agent.parent_id:
            return
        report = reports[-1]
        parent = self.store.get_agent(agent.parent_id)
        if parent is None or parent.status in TERMINAL_AGENT_STATUSES:
            return
        if parent.current_turn_id or self._pending_messages[parent.id]:
            return
        delivered_at = self._delivered_reports.get(report.id, float("-inf"))
        if self._now() - delivered_at < self._redelivery_seconds:
            return
        await self._deliver_to_parent(agent, report)

    async def _nudge(self, agent: Agent, children: list[Agent]) -> None:
        async with self._agent_locks[agent.id]:
            agent = self._agent(agent.id)
            if agent.status in TERMINAL_AGENT_STATUSES or agent.current_turn_id:
                return
            if self._pending_messages[agent.id]:
                await self._flush_pending_locked(agent)
                return
            attempts = self._nudges[agent.id] + 1
            if attempts > self._max_nudges:
                if agent.status != AgentStatus.IDLE or not agent.error:
                    agent.status = AgentStatus.IDLE
                    agent.error = (
                        f"Codex stopped responding after {self._max_nudges} prompts; "
                        "send an instruction to continue this agent"
                    )
                    agent.current_activity = "Idle and unresponsive"
                    self.store.save_agent(agent)
                    await self.emit(
                        Event(
                            session_id=agent.session_id,
                            agent_id=agent.id,
                            event_type="agent_unresponsive",
                            message=agent.error,
                        )
                    )
                return
            self._nudges[agent.id] = attempts
            if not await self._start_turn(agent, self._nudge_prompt(agent, children)):
                return
            agent.status = AgentStatus.WORKING
            agent.current_activity = "Resuming after an idle period"
            self.store.save_agent(agent)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type="agent_nudged",
                message=f"Restarted {agent.name} after {self._idle_nudge_seconds:g}s of inactivity",
            )
        )

    def _nudge_prompt(self, agent: Agent, children: list[Agent]) -> str:
        digest = self._state_digest(agent, children)
        if agent.depth == 0:
            return (
                "Trix noticed you have no active work. Do not wait or poll.\n"
                f"{digest}\n"
                "Take the next executive action now: review any submitted report with "
                "trix.review_report, spawn a worker for unfinished or failed work, send a "
                "correction with trix.send_instruction, or, if every requirement is delivered "
                "and verified, call trix.complete_session."
            )
        return (
            "Trix noticed your Codex turn ended without finishing your assignment. Do not wait "
            "or poll.\n"
            f"{digest}\n"
            "Continue the work now, or call trix.submit_report with concrete evidence if the "
            "task is genuinely complete."
        )

    def _state_digest(self, agent: Agent, children: list[Agent]) -> str:
        lines = [f"Your assignment: {agent.task[:500]}"]
        if children:
            lines.append("Your direct children:")
            for child in children:
                reports = self.store.reports_for_agent(child.id)
                latest = reports[-1] if reports else None
                report_note = (
                    f", report {latest.id} is {latest.status}" if latest is not None else ""
                )
                detail = f" ({child.error})" if child.error else ""
                lines.append(f"- {child.name} [{child.id}] is {child.status}{report_note}{detail}")
        else:
            lines.append("You have no child agents.")
        return "\n".join(lines)

    async def _rebuild_after_transport_loss(self) -> None:
        """Bring every live agent back after the shared app server died."""
        await self.codex.ensure_running()
        for session in self.store.list_sessions():
            if session.status != SessionStatus.RUNNING:
                continue
            for agent in self.store.list_agents(session.id):
                if agent.status in TERMINAL_AGENT_STATUSES or not agent.codex_thread_id:
                    continue
                async with self._agent_locks[agent.id]:
                    agent = self._agent(agent.id)
                    agent.current_turn_id = None
                    self._active_command.pop(agent.id, None)
                    self.store.save_agent(agent)
                if agent.status in RUNNING_AGENT_STATUSES:
                    await self._resume_agent(
                        agent,
                        "The Codex app server restarted and interrupted your turn. Continue the "
                        "same assignment from the repository's current state; treat any command "
                        "without a completion event as failed.",
                    )

    async def _resume_agent(self, agent: Agent, prompt: str) -> None:
        async with self._agent_locks[agent.id]:
            agent = self._agent(agent.id)
            if agent.status in TERMINAL_AGENT_STATUSES or not agent.codex_thread_id:
                return
            try:
                resumed = await self.codex.resume_thread(agent.codex_thread_id)
                self._thread_agents.pop(agent.codex_thread_id, None)
                self._thread_agents[resumed] = agent.id
                agent.codex_thread_id = resumed
                agent.current_turn_id = await self.codex.start_turn(resumed, prompt)
                agent.status = AgentStatus.WORKING
                agent.error = None
                agent.completed_at = None
                agent.current_activity = "Resumed after a transport restart"
            except Exception as error:
                agent.status = AgentStatus.WORKING
                agent.error = str(error)
                agent.current_activity = "Waiting for persisted thread recovery"
            self.store.save_agent(agent)
            self._touch(agent.id)

    async def _recover_stalled_command(self, agent: Agent, command: str) -> None:
        async with self._agent_locks[agent.id]:
            agent = self._agent(agent.id)
            if agent.status in TERMINAL_AGENT_STATUSES or not agent.current_turn_id:
                return
            turn_id = agent.current_turn_id
            self._active_command.pop(agent.id, None)
            attempt = self._recovery_attempts[agent.id] + 1
            self._recovery_attempts[agent.id] = attempt
            message = (
                f"Command produced no terminal event within {self._command_timeout:g} seconds: "
                f"{command[:1000]}"
            )
            await self.emit(
                Event(
                    session_id=agent.session_id,
                    agent_id=agent.id,
                    event_type="command_timed_out",
                    message=message,
                )
            )
            if attempt > self._max_recovery_attempts:
                await self._replace_agent_thread_locked(agent, message)
                return
            if agent.codex_thread_id:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(
                        self.codex.interrupt(agent.codex_thread_id, turn_id), timeout=10
                    )
            agent.current_turn_id = None
            recovery = (
                "The previous shell command stalled without a completion event and was "
                "interrupted by Trix. Treat it as a failed command, preserve all completed work, "
                "and continue the same task using a bounded-output, non-interactive alternative. "
                "Prefer trix.run_command with an explicit timeout_seconds. On Windows, check "
                "Get-Command before using optional executables such as rg and fall back to "
                "PowerShell-native commands when unavailable."
            )
            if await self._start_turn(agent, recovery):
                agent.status = AgentStatus.WORKING
                agent.current_activity = f"Recovering from a stalled command (attempt {attempt})"
                self.store.save_agent(agent)

    async def _replace_agent_thread(self, agent: Agent, reason: str) -> None:
        async with self._agent_locks[agent.id]:
            await self._replace_agent_thread_locked(self._agent(agent.id), reason)

    async def _replace_agent_thread_locked(self, agent: Agent, reason: str) -> None:
        """Replace broken Codex state without failing the logical Trix agent."""
        if agent.status in TERMINAL_AGENT_STATUSES:
            return
        old_thread = agent.codex_thread_id
        session = self._session(agent.session_id)
        if old_thread and agent.current_turn_id:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    self.codex.interrupt(old_thread, agent.current_turn_id), timeout=10
                )
        try:
            await self.codex.ensure_running()
            thread_id = await self.codex.create_thread(
                Path(session.repository_path),
                instructions_for(agent),
                tools_for(agent),
                read_only=agent.depth == 0,
            )
            turn_id = await self.codex.start_turn(
                thread_id,
                f"Resume this agent's existing assignment: {agent.task}\n\n"
                f"The previous Codex thread was replaced because: {reason}\n"
                "A shell command failure is not an agent failure. Continue from the repository's "
                "current state, use portable bounded-output commands, verify existing work, and "
                "complete the original assignment.",
            )
        except Exception as error:
            agent.status = AgentStatus.WORKING
            agent.error = str(error)
            agent.current_turn_id = None
            agent.current_activity = "Waiting to retry Codex thread recovery"
            self.store.save_agent(agent)
            self._touch(agent.id)
            return
        if old_thread:
            self._thread_agents.pop(old_thread, None)
        self._thread_agents[thread_id] = agent.id
        agent.codex_thread_id = thread_id
        agent.current_turn_id = turn_id
        agent.status = AgentStatus.WORKING
        agent.error = None
        agent.current_activity = "Resumed on a replacement Codex thread"
        self._recovery_attempts[agent.id] = 0
        self._active_command.pop(agent.id, None)
        self.store.save_agent(agent)
        self._touch(agent.id)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type="agent_thread_recovered",
                message="Replaced an unresponsive Codex thread and resumed the same assignment",
            )
        )

    # ---------------------------------------------------------------- event fan-out

    async def emit(self, event: Event) -> None:
        self.store.add_event(event)
        await asyncio.gather(
            *(subscriber(event) for subscriber in list(self._subscribers[event.session_id])),
            return_exceptions=True,
        )

    def subscribe(self, session_id: str, subscriber: Subscriber) -> None:
        self._subscribers[session_id].add(subscriber)

    def unsubscribe(self, session_id: str, subscriber: Subscriber) -> None:
        self._subscribers[session_id].discard(subscriber)

    # ---------------------------------------------------------------------- utils

    @staticmethod
    def _now() -> float:
        return asyncio.get_running_loop().time()

    def _touch(self, agent_id: str) -> None:
        self._last_progress[agent_id] = self._now()

    def _forget_runtime_state(self, agent_id: str) -> None:
        self._pending_messages.pop(agent_id, None)
        self._active_command.pop(agent_id, None)
        self._last_progress.pop(agent_id, None)
        self._nudges.pop(agent_id, None)
        self._drive_failures.pop(agent_id, None)
        self._recovery_attempts.pop(agent_id, None)

    def _agent_in_session(self, caller: Agent, agent_id: str) -> Agent:
        target = self._agent(agent_id)
        if target.session_id != caller.session_id:
            raise PolicyViolation("Agent is outside the caller's session")
        return target

    def _direct_child(self, caller: Agent, agent_id: str) -> Agent:
        target = self._agent_in_session(caller, agent_id)
        if target.parent_id != caller.id:
            raise PolicyViolation("Agent is not a direct child of the caller")
        return target

    @staticmethod
    def _string(arguments: dict[str, Any], key: str) -> str:
        value = arguments.get(key)
        if not isinstance(value, str) or not value.strip():
            raise TypeError(f"{key} must be a non-empty string")
        return value

    @staticmethod
    def _tool_result(message: str, *, success: bool = True) -> dict[str, Any]:
        return {
            "contentItems": [{"type": "inputText", "text": message}],
            "success": success,
        }

    def _session(self, session_id: str) -> TrixSession:
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(session_id)
        return session

    def _agent(self, agent_id: str) -> Agent:
        agent = self.store.get_agent(agent_id)
        if agent is None:
            raise KeyError(agent_id)
        return agent
