from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


def new_id() -> str:
    return str(uuid4())


def utc_now() -> datetime:
    return datetime.now(UTC)


class SessionStatus(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AgentStatus(StrEnum):
    QUEUED = "queued"
    STARTING = "starting"
    PLANNING = "planning"
    WORKING = "working"
    WAITING_FOR_CHILDREN = "waiting_for_children"
    VERIFYING = "verifying"
    REPORTING = "reporting"
    AWAITING_VERIFICATION = "awaiting_verification"
    IDLE = "idle"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_AGENT_STATUSES = frozenset(
    {AgentStatus.COMPLETED, AgentStatus.FAILED, AgentStatus.CANCELLED}
)

ACTIVE_AGENT_STATUSES = frozenset(
    {
        AgentStatus.QUEUED,
        AgentStatus.STARTING,
        AgentStatus.PLANNING,
        AgentStatus.WORKING,
        AgentStatus.WAITING_FOR_CHILDREN,
        AgentStatus.VERIFYING,
        AgentStatus.REPORTING,
        AgentStatus.AWAITING_VERIFICATION,
        AgentStatus.IDLE,
    }
)

#: States in which an agent is expected to be driving a Codex turn of its own.
RUNNING_AGENT_STATUSES = frozenset(
    {
        AgentStatus.STARTING,
        AgentStatus.PLANNING,
        AgentStatus.WORKING,
        AgentStatus.VERIFYING,
        AgentStatus.REPORTING,
    }
)


class ReportStatus(StrEnum):
    SUBMITTED = "submitted"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class TrixSession(BaseModel):
    id: str = Field(default_factory=new_id)
    title: str
    user_prompt: str
    repository_path: str
    status: SessionStatus = SessionStatus.CREATED
    root_agent_id: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None


class Agent(BaseModel):
    id: str = Field(default_factory=new_id)
    session_id: str
    parent_id: str | None = None
    depth: int = Field(ge=0)
    name: str
    role: str
    task: str
    codex_thread_id: str | None = None
    current_turn_id: str | None = None
    status: AgentStatus = AgentStatus.QUEUED
    current_activity: str = "Queued"
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class AgentReport(BaseModel):
    id: str = Field(default_factory=new_id)
    agent_id: str
    status: ReportStatus = ReportStatus.SUBMITTED
    summary: str = Field(min_length=1)
    requirements_completed: list[str] = Field(default_factory=list)
    files_changed: list[str] = Field(default_factory=list)
    commands_run: list[str] = Field(default_factory=list)
    verification_results: dict[str, Any] = Field(default_factory=dict)
    known_issues: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)
    recommended_parent_checks: list[str] = Field(default_factory=list)
    parent_feedback: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    reviewed_at: datetime | None = None


class Event(BaseModel):
    id: int | None = None
    session_id: str
    agent_id: str | None = None
    event_type: str
    message: str
    raw_event: dict[str, Any] | None = None
    created_at: datetime = Field(default_factory=utc_now)


class CreateSession(BaseModel):
    title: str = Field(min_length=1, max_length=160)
    prompt: str = Field(min_length=1, max_length=100_000)
    repository_path: str = Field(min_length=1, max_length=4096)


class SpawnAgent(BaseModel):
    parent_id: str
    name: str = Field(min_length=1, max_length=80)
    role: str = Field(min_length=1, max_length=80)
    task: str = Field(min_length=1, max_length=50_000)


class ReviewReport(BaseModel):
    accepted: bool
    feedback: str = Field(default="", max_length=20_000)


class Instruction(BaseModel):
    message: str = Field(min_length=1, max_length=50_000)
