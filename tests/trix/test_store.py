from __future__ import annotations

from trix.models import Agent, AgentReport, Event, TrixSession
from trix.store import Store


def test_round_trip_session_tree_report_and_events(tmp_path) -> None:
    store = Store(tmp_path / "trix.db")
    session = store.save_session(
        TrixSession(title="Build", user_prompt="Do it", repository_path="/tmp")
    )
    agent = store.save_agent(
        Agent(session_id=session.id, depth=0, name="Manager", role="Manager", task="Do it")
    )
    report = store.save_report(AgentReport(agent_id=agent.id, summary="Done"))
    event = store.add_event(
        Event(
            session_id=session.id,
            agent_id=agent.id,
            event_type="agent_created",
            message="Created",
            raw_event={"method": "thread/started"},
        )
    )

    assert store.get_session(session.id) == session
    assert store.list_agents(session.id) == [agent]
    assert store.reports_for_agent(agent.id) == [report]
    assert store.list_events(session.id)[0] == event
