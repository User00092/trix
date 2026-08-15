from __future__ import annotations

import asyncio
import json
import os
from asyncio.subprocess import PIPE
from collections import defaultdict
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from trix.codex import CodexAppServer
from trix.events import is_stream_delta, normalize_codex_event
from trix.models import (
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

Subscriber = Callable[[Event], Awaitable[None]]


class Orchestrator:
    def __init__(self, store: Store, codex: CodexAppServer) -> None:
        self.store = store
        self.codex = codex
        self.policy = DelegationPolicy()
        self._subscribers: dict[str, set[Subscriber]] = defaultdict(set)
        self._thread_agents: dict[str, str] = {}
        self._pending_messages: dict[str, list[str]] = defaultdict(list)
        self._turn_watchdogs: dict[str, asyncio.Task[None]] = {}
        self._turn_idle_timeout = float(os.environ.get("TRIX_TURN_IDLE_TIMEOUT", "900"))
        self._lock = asyncio.Lock()
        self.codex.on_notification(self._on_codex_event)
        self.codex.on_request("item/tool/call", self._on_tool_call)

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

    async def reconcile_orphaned_sessions(self) -> int:
        """Fail persisted active sessions that cannot survive an app restart."""
        reconciled = 0
        for session in self.store.list_sessions():
            if session.status != SessionStatus.RUNNING:
                continue
            message = "Trix restarted while this session was active; its Codex process was lost"
            for agent in self.store.list_agents(session.id):
                if agent.status in {
                    AgentStatus.COMPLETED,
                    AgentStatus.FAILED,
                    AgentStatus.CANCELLED,
                }:
                    continue
                agent.status = AgentStatus.FAILED
                agent.error = message
                agent.current_activity = "Stopped by application restart"
                agent.current_turn_id = None
                agent.completed_at = utc_now()
                self.store.save_agent(agent)
            session.status = SessionStatus.FAILED
            session.completed_at = utc_now()
            self.store.save_session(session)
            await self.emit(
                Event(
                    session_id=session.id,
                    agent_id=session.root_agent_id,
                    event_type="session_failed",
                    message=message,
                )
            )
            reconciled += 1
        return reconciled

    async def start_session(self, session_id: str) -> TrixSession:
        session = self._session(session_id)
        if session.status not in {SessionStatus.CREATED, SessionStatus.FAILED}:
            raise PolicyViolation(f"Cannot start a session in state {session.status}")
        session.status = SessionStatus.RUNNING
        session.started_at = utc_now()
        self.store.save_session(session)
        await self.codex.start()
        assert session.root_agent_id is not None
        await self._start_agent(self._agent(session.root_agent_id))
        return session

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
        agent.status = AgentStatus.STARTING
        agent.started_at = utc_now()
        agent.current_activity = "Starting Codex thread"
        self.store.save_agent(agent)
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
            turn_id = await self.codex.start_turn(thread_id, agent.task)
            agent.current_turn_id = turn_id
            self.store.save_agent(agent)
            self._watch_turn(agent, turn_id)
        except Exception as error:
            agent.status = AgentStatus.FAILED
            agent.error = str(error)
            agent.current_activity = "Codex failed to start"
            self.store.save_agent(agent)
            if agent.parent_id is None:
                session.status = SessionStatus.FAILED
                session.completed_at = utc_now()
                self.store.save_session(session)
            await self.emit(
                Event(
                    session_id=agent.session_id,
                    agent_id=agent.id,
                    event_type="agent_failed",
                    message=str(error),
                )
            )
            raise

    async def instruct(self, agent_id: str, message: str) -> Agent:
        agent = self._agent(agent_id)
        if not agent.codex_thread_id:
            raise PolicyViolation("Agent has no Codex thread")
        agent.current_turn_id = await self.codex.start_turn(agent.codex_thread_id, message)
        agent.status = AgentStatus.WORKING
        agent.current_activity = "Following up on an instruction"
        self.store.save_agent(agent)
        self._watch_turn(agent, agent.current_turn_id)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type="agent_instruction",
                message="Received a follow-up instruction",
            )
        )
        return agent

    async def submit_report(self, agent_id: str, report: AgentReport) -> AgentReport:
        agent = self._agent(agent_id)
        if report.agent_id != agent.id:
            raise PolicyViolation("Report agent id does not match URL agent id")
        children = [
            item for item in self.store.list_agents(agent.session_id) if item.parent_id == agent.id
        ]
        unfinished = [
            item
            for item in children
            if item.status not in {AgentStatus.COMPLETED, AgentStatus.FAILED, AgentStatus.CANCELLED}
        ]
        if unfinished:
            raise PolicyViolation("Agent cannot report while direct children are unfinished")
        self.store.save_report(report)
        agent.status = (
            AgentStatus.AWAITING_VERIFICATION if agent.parent_id else AgentStatus.VERIFYING
        )
        agent.current_activity = (
            "Awaiting parent verification" if agent.parent_id else "Performing final verification"
        )
        self.store.save_agent(agent)
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
        agent = self._agent(report.agent_id)
        report.status = ReportStatus.ACCEPTED if accepted else ReportStatus.REJECTED
        report.parent_feedback = feedback
        report.reviewed_at = utc_now()
        self.store.save_report(report)
        if accepted:
            agent.status = AgentStatus.COMPLETED
            agent.completed_at = utc_now()
            agent.current_activity = "Work accepted"
        else:
            agent.status = AgentStatus.WORKING
            agent.current_activity = "Addressing rejected report"
            if feedback and agent.codex_thread_id:
                agent.current_turn_id = await self.codex.start_turn(agent.codex_thread_id, feedback)
        self.store.save_agent(agent)
        await self.emit(
            Event(
                session_id=agent.session_id,
                agent_id=agent.id,
                event_type=f"report_{report.status}",
                message=f"{agent.name} report {report.status}",
            )
        )
        return report

    async def complete_session(
        self, manager_id: str, summary: str, verification: list[str]
    ) -> TrixSession:
        manager = self._agent(manager_id)
        if manager.depth != 0:
            raise PolicyViolation("Only the Manager can complete a session")
        agents = [
            item for item in self.store.list_agents(manager.session_id) if item.id != manager.id
        ]
        unfinished = [item for item in agents if item.status != AgentStatus.COMPLETED]
        if unfinished:
            names = ", ".join(item.name for item in unfinished)
            raise PolicyViolation(f"Cannot complete while work is unaccepted: {names}")
        if not verification:
            raise PolicyViolation("Final verification evidence is required")
        manager.status = AgentStatus.COMPLETED
        manager.completed_at = utc_now()
        manager.current_activity = summary[:500]
        self.store.save_agent(manager)
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
                raw_event={"verification": verification},
            )
        )
        return session

    async def cancel_session(self, session_id: str) -> TrixSession:
        session = self._session(session_id)
        for agent in self.store.list_agents(session_id):
            if agent.codex_thread_id and agent.current_turn_id:
                try:
                    await self.codex.interrupt(agent.codex_thread_id, agent.current_turn_id)
                except Exception:
                    pass
            if agent.status not in {AgentStatus.COMPLETED, AgentStatus.FAILED}:
                agent.status = AgentStatus.CANCELLED
                agent.completed_at = utc_now()
                agent.current_activity = "Cancelled"
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

    async def emit(self, event: Event) -> None:
        self.store.add_event(event)
        await asyncio.gather(
            *(subscriber(event) for subscriber in self._subscribers[event.session_id]),
            return_exceptions=True,
        )

    def subscribe(self, session_id: str, subscriber: Subscriber) -> None:
        self._subscribers[session_id].add(subscriber)

    def unsubscribe(self, session_id: str, subscriber: Subscriber) -> None:
        self._subscribers[session_id].discard(subscriber)

    async def _on_codex_event(self, payload: dict[str, Any]) -> None:
        if is_stream_delta(payload):
            return
        params: dict[str, Any] = (
            payload["params"] if isinstance(payload.get("params"), dict) else {}
        )
        thread_id = params.get("threadId")
        if not isinstance(thread_id, str) or thread_id not in self._thread_agents:
            return
        agent = self._agent(self._thread_agents[thread_id])
        event = normalize_codex_event(agent.session_id, agent.id, payload)
        if agent.status in {
            AgentStatus.COMPLETED,
            AgentStatus.FAILED,
            AgentStatus.CANCELLED,
        }:
            # Tool calls can move an agent into a terminal state before Codex emits
            # the notification that closes the surrounding turn.  Keep recording
            # that late notification, but never let it reopen finished work.
            await self.emit(event)
            return
        agent.current_activity = event.message[:500]
        if event.event_type == "turn_completed":
            self._cancel_turn_watchdog(agent.id)
            agent.current_turn_id = None
            reports = self.store.reports_for_agent(agent.id)
            children = [
                item
                for item in self.store.list_agents(agent.session_id)
                if item.parent_id == agent.id
            ]
            active_children = [
                item
                for item in children
                if item.status
                not in {AgentStatus.COMPLETED, AgentStatus.FAILED, AgentStatus.CANCELLED}
            ]
            if reports and reports[-1].status == ReportStatus.SUBMITTED:
                agent.status = AgentStatus.AWAITING_VERIFICATION
                agent.current_activity = "Awaiting parent verification"
            elif active_children:
                agent.status = AgentStatus.WAITING_FOR_CHILDREN
                agent.current_activity = "Waiting for child agents"
            elif agent.depth == 0:
                agent.status = AgentStatus.VERIFYING
                agent.current_activity = "Checking delegated work"
            else:
                agent.status = AgentStatus.REPORTING
                agent.current_activity = "Preparing completion report"
        elif event.event_type == "agent_failed":
            agent.status = AgentStatus.FAILED
        self.store.save_agent(agent)
        await self.emit(event)
        if agent.current_turn_id:
            self._watch_turn(agent, agent.current_turn_id)
        if event.event_type == "turn_completed":
            await self._flush_pending(agent)

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
        try:
            result = await self._execute_tool(agent, str(params.get("tool", "")), arguments)
        except (KeyError, PolicyViolation, TypeError, ValueError) as error:
            return self._tool_result(str(error), success=False)
        return self._tool_result(result)

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
            await self.instruct(target.id, self._string(arguments, "message"))
            return f"Instruction delivered to {target.name}."
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
        if tool == "complete_session":
            verification = arguments.get("verification")
            if not isinstance(verification, list) or not all(
                isinstance(item, str) for item in verification
            ):
                raise TypeError("verification must be a list of strings")
            await self.complete_session(caller.id, self._string(arguments, "summary"), verification)
            return "Session completed and accepted."
        raise PolicyViolation(f"Unsupported Trix tool: {tool}")

    async def _inspect_changes(self, caller: Agent) -> str:
        session = self._session(caller.session_id)
        output: list[str] = []
        for command in (["git", "status", "--short"], ["git", "diff", "--stat"], ["git", "diff"]):
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=session.repository_path,
                stdout=PIPE,
                stderr=PIPE,
            )
            stdout, stderr = await process.communicate()
            text = stdout.decode(errors="replace")
            if process.returncode != 0:
                raise PolicyViolation(stderr.decode(errors="replace") or "Git inspection failed")
            output.append(f"$ {' '.join(command)}\n{text}")
        return "\n".join(output)[:50_000]

    async def _deliver_to_parent(self, child: Agent, report: AgentReport) -> None:
        assert child.parent_id is not None
        parent = self._agent(child.parent_id)
        message = (
            f"Child agent {child.name} ({child.id}) submitted report {report.id}.\n"
            f"Summary: {report.summary}\nFiles changed: {report.files_changed}\n"
            f"Verification: {report.verification_results}\nKnown issues: {report.known_issues}\n"
            "Inspect the actual changes and use trix.review_report to accept or reject it."
        )
        self._pending_messages[parent.id].append(message)
        await self._flush_pending(parent)

    async def _flush_pending(self, agent: Agent) -> None:
        messages = self._pending_messages[agent.id]
        if not messages or not agent.codex_thread_id:
            return
        message = messages[0]
        try:
            agent.current_turn_id = await self.codex.start_turn(agent.codex_thread_id, message)
        except Exception:
            return
        messages.pop(0)
        agent.status = AgentStatus.VERIFYING
        agent.current_activity = "Reviewing child work"
        self.store.save_agent(agent)
        self._watch_turn(agent, agent.current_turn_id)

    def _watch_turn(self, agent: Agent, turn_id: str) -> None:
        """Start or refresh the inactivity watchdog for an active Codex turn."""
        self._cancel_turn_watchdog(agent.id)
        if self._turn_idle_timeout > 0:
            self._turn_watchdogs[agent.id] = asyncio.create_task(
                self._fail_idle_turn(agent.id, turn_id)
            )

    def _cancel_turn_watchdog(self, agent_id: str) -> None:
        task = self._turn_watchdogs.pop(agent_id, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def _fail_idle_turn(self, agent_id: str, turn_id: str) -> None:
        try:
            await asyncio.sleep(self._turn_idle_timeout)
            agent = self._agent(agent_id)
            if agent.current_turn_id != turn_id or agent.status not in {
                AgentStatus.STARTING,
                AgentStatus.WORKING,
                AgentStatus.PLANNING,
                AgentStatus.VERIFYING,
                AgentStatus.REPORTING,
            }:
                return
            if agent.codex_thread_id:
                try:
                    await self.codex.interrupt(agent.codex_thread_id, turn_id)
                except Exception:
                    pass
            message = (
                f"Codex turn produced no activity for {self._turn_idle_timeout:g} seconds"
            )
            agent.status = AgentStatus.FAILED
            agent.error = message
            agent.completed_at = utc_now()
            agent.current_activity = "Codex turn timed out"
            agent.current_turn_id = None
            self.store.save_agent(agent)
            session = self._session(agent.session_id)
            session.status = SessionStatus.FAILED
            session.completed_at = utc_now()
            self.store.save_session(session)
            await self.emit(
                Event(
                    session_id=agent.session_id,
                    agent_id=agent.id,
                    event_type="agent_failed",
                    message=message,
                )
            )
        finally:
            self._turn_watchdogs.pop(agent_id, None)

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
