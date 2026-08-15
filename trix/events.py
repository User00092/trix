from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from trix.models import Event

EventSink = Callable[[Event], Awaitable[None]]


def is_stream_delta(payload: dict[str, Any]) -> bool:
    """Return true for high-frequency fragments superseded by completed item events."""
    method = str(payload.get("method", "")).lower()
    return method.endswith("/delta") or "outputdelta" in method


def normalize_codex_event(session_id: str, agent_id: str, payload: dict[str, Any]) -> Event:
    method = str(payload.get("method", "codex_event"))
    params: dict[str, Any] = payload["params"] if isinstance(payload.get("params"), dict) else {}
    item: dict[str, Any] = params["item"] if isinstance(params.get("item"), dict) else {}
    item_type = str(item.get("type", "")).lower()

    event_type = method.replace("/", "_")
    message = method.replace("/", " ").replace("_", " ").capitalize()
    if "command" in item_type:
        command = item.get("command", item.get("cmd", "command"))
        if method.endswith("started"):
            message = f"Running {command}"
            event_type = "command_started"
        else:
            exit_code = item.get("exitCode", item.get("exit_code"))
            failed = exit_code not in {None, 0} or str(item.get("status", "")).lower() in {
                "failed",
                "error",
            }
            message = (
                f"Command failed with exit code {exit_code}: {command}"
                if failed
                else f"Finished {command}"
            )
            event_type = "command_failed" if failed else "command_completed"
    elif "filechange" in item_type or "file_change" in item_type:
        message = "Modifying repository files"
        event_type = "file_changed"
    elif "agentmessage" in item_type or "agent_message" in item_type:
        message = str(item.get("text", item.get("message", "Agent response updated")))
        event_type = "agent_message"
    elif method == "turn/started":
        message = "Codex turn started"
        event_type = "agent_started"
    elif method == "turn/completed":
        message = "Codex turn completed; awaiting structured report"
        event_type = "turn_completed"
    elif method == "error":
        message = str(params.get("message", "Codex reported an error"))
        event_type = "agent_failed"
    return Event(
        session_id=session_id,
        agent_id=agent_id,
        event_type=event_type,
        message=message[:2000],
        raw_event=payload,
    )
