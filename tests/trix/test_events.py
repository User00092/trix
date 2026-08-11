from trix.events import is_stream_delta, normalize_codex_event


def test_command_event_is_semantic() -> None:
    event = normalize_codex_event(
        "session",
        "agent",
        {
            "method": "item/started",
            "params": {"item": {"type": "commandExecution", "command": "pytest -q"}},
        },
    )
    assert event.event_type == "command_started"
    assert "pytest -q" in event.message


def test_raw_event_is_preserved() -> None:
    payload = {"method": "turn/completed", "params": {"threadId": "thread"}}
    assert normalize_codex_event("session", "agent", payload).raw_event == payload


def test_high_frequency_deltas_are_filtered() -> None:
    assert is_stream_delta({"method": "item/agentMessage/delta"})
    assert is_stream_delta({"method": "item/commandExecution/outputDelta"})
    assert not is_stream_delta({"method": "item/completed"})
