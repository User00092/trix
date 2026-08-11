from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any

from trix.models import Agent, AgentReport, Event, TrixSession


class Store:
    """Small SQLite repository whose JSON records remain portable to a richer ORM later."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        with self.connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY, data TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agents (
                    id TEXT PRIMARY KEY, session_id TEXT NOT NULL, parent_id TEXT,
                    data TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS agents_session ON agents(session_id);
                CREATE TABLE IF NOT EXISTS reports (
                    id TEXT PRIMARY KEY, agent_id TEXT NOT NULL, data TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS reports_agent ON reports(agent_id);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL,
                    agent_id TEXT, event_type TEXT NOT NULL, message TEXT NOT NULL,
                    raw_event TEXT, created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS events_session ON events(session_id, id);
                """
            )

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = sqlite3.connect(self.path, timeout=10)
            connection.row_factory = sqlite3.Row
            try:
                yield connection
                connection.commit()
            finally:
                connection.close()

    @staticmethod
    def _json(model: TrixSession | Agent | AgentReport) -> str:
        return model.model_dump_json()

    def save_session(self, session: TrixSession) -> TrixSession:
        with self.connection() as db:
            db.execute(
                "INSERT OR REPLACE INTO sessions VALUES (?, ?, ?)",
                (session.id, self._json(session), session.created_at.isoformat()),
            )
        return session

    def get_session(self, session_id: str) -> TrixSession | None:
        with self.connection() as db:
            row = db.execute("SELECT data FROM sessions WHERE id = ?", (session_id,)).fetchone()
        return TrixSession.model_validate_json(row["data"]) if row else None

    def list_sessions(self) -> list[TrixSession]:
        with self.connection() as db:
            rows = db.execute("SELECT data FROM sessions ORDER BY created_at DESC").fetchall()
        return [TrixSession.model_validate_json(row["data"]) for row in rows]

    def save_agent(self, agent: Agent) -> Agent:
        with self.connection() as db:
            db.execute(
                "INSERT OR REPLACE INTO agents VALUES (?, ?, ?, ?, ?)",
                (
                    agent.id,
                    agent.session_id,
                    agent.parent_id,
                    self._json(agent),
                    agent.created_at.isoformat(),
                ),
            )
        return agent

    def get_agent(self, agent_id: str) -> Agent | None:
        with self.connection() as db:
            row = db.execute("SELECT data FROM agents WHERE id = ?", (agent_id,)).fetchone()
        return Agent.model_validate_json(row["data"]) if row else None

    def list_agents(self, session_id: str) -> list[Agent]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT data FROM agents WHERE session_id = ? ORDER BY created_at", (session_id,)
            ).fetchall()
        return [Agent.model_validate_json(row["data"]) for row in rows]

    def save_report(self, report: AgentReport) -> AgentReport:
        with self.connection() as db:
            db.execute(
                "INSERT OR REPLACE INTO reports VALUES (?, ?, ?, ?)",
                (report.id, report.agent_id, self._json(report), report.created_at.isoformat()),
            )
        return report

    def get_report(self, report_id: str) -> AgentReport | None:
        with self.connection() as db:
            row = db.execute("SELECT data FROM reports WHERE id = ?", (report_id,)).fetchone()
        return AgentReport.model_validate_json(row["data"]) if row else None

    def reports_for_agent(self, agent_id: str) -> list[AgentReport]:
        with self.connection() as db:
            rows = db.execute(
                "SELECT data FROM reports WHERE agent_id = ? ORDER BY created_at", (agent_id,)
            ).fetchall()
        return [AgentReport.model_validate_json(row["data"]) for row in rows]

    def add_event(self, event: Event) -> Event:
        raw = json.dumps(event.raw_event) if event.raw_event is not None else None
        with self.connection() as db:
            cursor = db.execute(
                """INSERT INTO events
                (session_id, agent_id, event_type, message, raw_event, created_at)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    event.session_id,
                    event.agent_id,
                    event.event_type,
                    event.message,
                    raw,
                    event.created_at.isoformat(),
                ),
            )
            event.id = cursor.lastrowid
        return event

    def list_events(self, session_id: str, after: int = 0, limit: int = 500) -> list[Event]:
        with self.connection() as db:
            rows = db.execute(
                """SELECT * FROM events WHERE session_id = ? AND id > ?
                ORDER BY id LIMIT ?""",
                (session_id, after, min(limit, 1000)),
            ).fetchall()
        return [self._event(row) for row in rows]

    @staticmethod
    def _event(row: sqlite3.Row) -> Event:
        data: dict[str, Any] = dict(row)
        data["raw_event"] = json.loads(data["raw_event"]) if data["raw_event"] else None
        return Event.model_validate(data)
