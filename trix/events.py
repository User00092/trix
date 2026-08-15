from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from trix.models import Event

EventSink = Callable[[Event], Awaitable[None]]

#: Events that mean the Codex turn is no longer running, whatever its outcome.
TURN_ENDED_EVENTS = frozenset({"turn_completed", "turn_interrupted", "turn_failed"})
COMMAND_STARTED_EVENTS = frozenset({"command_started"})
COMMAND_ENDED_EVENTS = frozenset({"command_completed", "command_failed"})
TOOL_CALL_ENDED_EVENTS = frozenset({"tool_call_completed", "tool_call_failed"})


def is_stream_delta(payload: dict[str, Any]) -> bool:
    """Return true for high-frequency fragments superseded by completed item events."""
    method = str(payload.get("method", "")).lower()
    return method.endswith("delta") or method.endswith("/progress")


def codex_thread_id(payload: dict[str, Any]) -> str | None:
    params = payload.get("params")
    thread_id = params.get("threadId") if isinstance(params, dict) else None
    return thread_id if isinstance(thread_id, str) else None


def codex_turn_id(payload: dict[str, Any]) -> str | None:
    """Return the turn a notification belongs to, for both item and turn payloads."""
    params: dict[str, Any] = payload["params"] if isinstance(payload.get("params"), dict) else {}
    turn_id = params.get("turnId")
    if isinstance(turn_id, str):
        return turn_id
    turn = params.get("turn")
    if isinstance(turn, dict) and isinstance(turn.get("id"), str):
        return str(turn["id"])
    return None


def normalize_codex_event(session_id: str, agent_id: str, payload: dict[str, Any]) -> Event:
    method = str(payload.get("method", "codex_event"))
    params: dict[str, Any] = payload["params"] if isinstance(payload.get("params"), dict) else {}
    item: dict[str, Any] = params["item"] if isinstance(params.get("item"), dict) else {}
    item_type = str(item.get("type", "")).lower()
    started = method.endswith("started")

    event_type = method.replace("/", "_")
    message = method.replace("/", " ").replace("_", " ").capitalize()
    if "command" in item_type:
        command = item.get("command", item.get("cmd", "command"))
        if started:
            message = f"Running {command}"
            event_type = "command_started"
        else:
            exit_code = item.get("exitCode", item.get("exit_code"))
            failed = exit_code not in {None, 0} or str(item.get("status", "")).lower() in {
                "failed",
                "error",
                "declined",
            }
            message = (
                f"Command failed with exit code {exit_code}: {command}"
                if failed
                else f"Finished {command}"
            )
            event_type = "command_failed" if failed else "command_completed"
    elif "dynamictoolcall" in item_type or "mcptoolcall" in item_type:
        label = f"{item.get('namespace') or 'trix'}.{item.get('tool', 'tool')}"
        if started:
            message = f"Calling {label}"
            event_type = "tool_call_started"
        else:
            failed = str(item.get("status", "")).lower() == "failed" or item.get("success") is False
            message = f"{'Failed' if failed else 'Finished'} {label}"
            event_type = "tool_call_failed" if failed else "tool_call_completed"
    elif "filechange" in item_type or "file_change" in item_type:
        message = "Modifying repository files"
        event_type = "file_changed"
    elif "agentmessage" in item_type or "agent_message" in item_type:
        message = str(item.get("text", item.get("message", "Agent response updated")))
        event_type = "agent_message"
    elif "reasoning" in item_type:
        message = "Thinking"
        event_type = "agent_reasoning"
    elif method == "turn/started":
        message = "Codex turn started"
        event_type = "agent_started"
    elif method == "turn/completed":
        turn: dict[str, Any] = params["turn"] if isinstance(params.get("turn"), dict) else {}
        status = str(turn.get("status", "completed")).lower()
        turn_error: dict[str, Any] = turn["error"] if isinstance(turn.get("error"), dict) else {}
        if status == "failed":
            message = f"Codex turn failed: {turn_error.get('message', 'unknown error')}"
            event_type = "turn_failed"
        elif status == "interrupted":
            message = "Codex turn interrupted"
            event_type = "turn_interrupted"
        else:
            message = "Codex turn completed; awaiting structured report"
            event_type = "turn_completed"
    elif method == "error":
        failure: dict[str, Any] = params["error"] if isinstance(params.get("error"), dict) else {}
        detail = str(failure.get("message", params.get("message", "Codex reported an error")))
        if params.get("willRetry") is True:
            message = f"Codex is retrying after an error: {detail}"
            event_type = "agent_retrying"
        else:
            message = detail
            event_type = "agent_failed"
    elif method == "thread/status/changed":
        status_field = params.get("status")
        status = (
            str(status_field.get("type", "unknown"))
            if isinstance(status_field, dict)
            else str(status_field)
        )
        message = f"Codex thread is {status}"
        event_type = "thread_status_changed"
    return Event(
        session_id=session_id,
        agent_id=agent_id,
        event_type=event_type,
        message=message[:2000],
        raw_event=payload,
    )
