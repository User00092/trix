# Trix — Project Overview

## Overview

Trix is a Strix-inspired, open-source AI development orchestration platform designed to coordinate Codex agents and subagents as a structured software-engineering team.

Strix provides a useful reference for how active AI work can be visualized: agents operate independently, their activity is visible in real time, and the user can inspect what each agent is doing. Trix should provide a similar experience, but focus on general software development rather than penetration testing.

Trix should not attempt to replace Codex. Codex remains responsible for coding, repository interaction, reasoning, command execution, testing, and agent work. Trix acts as the orchestration, policy, state-management, and visualization layer around Codex.

The core concept is a hierarchical agent tree where a primary Codex session operates as the Manager. The Manager decomposes the user's request, delegates independent work to subagents, waits for their reports, inspects their work, performs integration verification, and decides whether the overall task is complete.

---

# Primary Goals

Trix should:

* Accept a development task from the user.
* Start a primary Codex Manager session.
* Allow the Manager to analyze and decompose the task.
* Allow agents to dynamically delegate appropriate work.
* Enforce a maximum hierarchy depth.
* Enforce a maximum of two active direct children per spawning agent.
* Track the relationship between every parent and child agent.
* Stream Codex activity to the frontend in real time.
* Display the entire agent hierarchy visually.
* Allow the user to inspect individual agent activity.
* Collect structured completion reports from agents.
* Require parent agents to verify child work.
* Require the Manager to perform final integration verification.
* Clearly distinguish between work being performed, work awaiting verification, and accepted work.
* Persist enough state to recover and inspect previous Trix sessions.

---

# Agent Hierarchy

Trix supports three logical levels.

```text
Trix Manager                     Depth 0
│
├── Agent A                      Depth 1
│   ├── Agent A1                 Depth 2
│   └── Agent A2                 Depth 2
│
└── Agent B                      Depth 1
    ├── Agent B1                 Depth 2
    └── Agent B2                 Depth 2
```

## Depth 0 — Manager

The Manager is the root Codex session.

Responsibilities:

* Understand the user's request.
* Inspect the repository and current project state.
* Develop an execution plan.
* Decide which work should be delegated.
* Spawn up to two direct child agents at once.
* Monitor child progress.
* Receive reports from direct children.
* Inspect and verify their changes.
* Resolve integration problems.
* Run final project-level verification.
* Determine whether the user's request has actually been completed.
* Report the final result to the user.

The Manager must not treat a child agent reporting "complete" as proof that the task is complete.

---

# Depth 1 — Worker Agents

Depth-1 agents perform substantial delegated portions of the task.

Examples:

```text
Backend Agent
Frontend Agent
Testing Agent
Security Agent
Migration Agent
Refactoring Agent
Research Agent
```

A Depth-1 agent:

* Receives a clearly bounded task from the Manager.
* Owns that task until it reports back.
* May spawn up to two Depth-2 agents.
* Must wait for relevant child agents before reporting completion.
* Must inspect child work.
* Must integrate child contributions.
* Must independently verify its assigned task.
* Reports a consolidated result to the Manager.

A Depth-1 agent remains responsible for all work performed by its children.

---

# Depth 2 — Leaf Agents

Depth-2 agents are the lowest level.

They:

* Perform bounded work assigned by their Depth-1 parent.
* Cannot spawn additional agents.
* Report directly to the agent that spawned them.
* Do not independently report completion to the Manager.
* Provide structured evidence of the work performed.

Example:

```text
Manager
└── Backend Agent
    ├── Database Agent
    └── API Agent
```

The Database Agent and API Agent report to the Backend Agent.

The Backend Agent reviews and integrates their work before reporting to the Manager.

---

# Delegation Rules

Trix must enforce delegation rules programmatically rather than depending entirely on prompt instructions.

Constants should initially resemble:

```python
MAX_TREE_DEPTH = 2
MAX_ACTIVE_CHILDREN_PER_AGENT = 2
```

Depth is zero-indexed:

```text
0 = Manager
1 = Delegating Worker
2 = Leaf Worker
```

An agent may spawn another agent only when:

```python
agent.depth < MAX_TREE_DEPTH
```

An agent may have no more than two directly active children.

"Active" should include states such as:

```text
queued
starting
planning
working
waiting
verifying
```

Completed, failed, and cancelled children should not consume an active slot.

Delegation should be optional.

Agents must never spawn children simply because capacity exists.

Delegation is appropriate when work is:

* Independently executable.
* Clearly bounded.
* Parallelizable.
* Large enough to justify another context.
* Better handled by a specialist.
* Likely to benefit from independent verification.

---

# Parent/Child Reporting

Reports always travel upward through the hierarchy.

```text
Leaf Agent
    ↓
Depth-1 Parent
    ↓
Manager
    ↓
User
```

A child must report to its direct parent.

The Manager should generally receive consolidated information from Depth-1 agents rather than being flooded with every leaf agent's internal reasoning and execution history.

The UI may still expose all agent activity to the user.

---

# Agent Completion Reports

Every agent should produce a structured completion report.

Example schema:

```json
{
  "status": "completed",
  "summary": "Implemented JWT refresh-token rotation.",
  "requirements_completed": [
    "Rotate refresh tokens after successful use",
    "Reject expired refresh tokens"
  ],
  "files_changed": [
    "backend/auth/service.py",
    "backend/auth/routes.py"
  ],
  "commands_run": [
    "pytest tests/auth -q"
  ],
  "verification": {
    "tests_passed": 41,
    "tests_failed": 0
  },
  "known_issues": [],
  "risks": [],
  "recommended_parent_checks": [
    "Verify frontend behavior when access tokens expire"
  ]
}
```

The exact schema may evolve, but reports must contain evidence rather than simply stating that the task is done.

---

# Verification Model

Verification is a core Trix concept.

Trix should distinguish between:

```text
Work completed by an agent

and

Work accepted by its parent
```

These are not equivalent.

## Leaf Verification

A leaf agent should verify its own implementation before reporting.

For example:

```text
Implement code
↓
Run relevant tests
↓
Inspect diff
↓
Check requirements
↓
Report
```

## Depth-1 Verification

A Depth-1 parent should inspect the actual work performed by its children.

It should:

* Review relevant diffs.
* Review files changed.
* Run appropriate tests.
* Check interactions between child contributions.
* Detect conflicting implementations.
* Resolve integration problems.
* Confirm requirements were satisfied.

Only after this should it send its consolidated report to the Manager.

## Manager Verification

The Manager performs final integration verification.

Typical checks may include:

```text
Full test suite
Build
Lint
Type checking
Security-sensitive review
Integration testing
Repository diff review
Requirement-by-requirement checklist
```

The Manager determines final completion.

---

# Agent Lifecycle

Agents should have explicit lifecycle states.

Suggested states:

```text
queued
starting
planning
working
waiting_for_children
verifying
reporting
completed
failed
cancelled
```

Example lifecycle:

```text
QUEUED
  ↓
STARTING
  ↓
PLANNING
  ↓
WORKING
  ↓
WAITING_FOR_CHILDREN
  ↓
VERIFYING
  ↓
REPORTING
  ↓
COMPLETED
```

Not every agent will enter every state.

For example, a leaf agent will never need `waiting_for_children`.

---

# Codex Integration

Trix should use Codex as the underlying execution engine.

Prefer Codex App Server for the primary integration rather than building the system around terminal scraping.

The Trix backend should be responsible for:

* Creating Codex sessions/threads.
* Associating Codex threads with Trix agents.
* Sending tasks and follow-up instructions.
* Receiving streamed Codex events.
* Translating Codex events into Trix events.
* Tracking agent state.
* Enforcing hierarchy rules.
* Persisting session history.
* Delivering live updates to the frontend.

Do not reimplement Codex's coding capabilities inside Trix.

---

# Trix Backend

Use Python.

Recommended initial stack:

```text
Python
FastAPI
Pydantic
AsyncIO
WebSockets
SQLite
SQLAlchemy or SQLModel
Codex App Server integration
```

The backend should be designed so SQLite can eventually be replaced with PostgreSQL without major architectural changes.

Suggested package structure:

```text
backend/
├── main.py
├── api/
│   ├── sessions.py
│   ├── agents.py
│   ├── tasks.py
│   └── websocket.py
├── codex/
│   ├── client.py
│   ├── events.py
│   ├── mapper.py
│   └── session.py
├── orchestration/
│   ├── manager.py
│   ├── scheduler.py
│   ├── delegation.py
│   ├── verification.py
│   └── policies.py
├── models/
│   ├── session.py
│   ├── agent.py
│   ├── task.py
│   ├── report.py
│   └── event.py
├── services/
│   ├── agent_service.py
│   ├── task_service.py
│   └── event_service.py
└── database/
    ├── database.py
    └── migrations/
```

Do not blindly follow this structure if the implementation suggests a cleaner design.

---

# Core Data Models

## TrixSession

Represents one user-requested Trix run.

Suggested fields:

```python
id
title
user_prompt
status
root_agent_id
repository_path
created_at
started_at
completed_at
```

---

# Agent

Suggested fields:

```python
id
session_id

parent_id
depth

name
role

codex_thread_id

status

task_id

created_at
started_at
completed_at

error
```

---

# Task

Suggested fields:

```python
id
session_id
agent_id

title
description

status

dependencies

created_at
started_at
completed_at
```

---

# AgentReport

Suggested fields:

```python
id
agent_id
task_id

status
summary

requirements_completed
files_changed
commands_run

verification_results

known_issues
risks
recommended_parent_checks

created_at
```

---

# Event

Every meaningful action should be represented as an event.

Suggested fields:

```python
id
session_id
agent_id

event_type
message

raw_event

created_at
```

Possible event types:

```text
agent_created
agent_started
agent_message
agent_spawned
agent_waiting
file_read
file_changed
command_started
command_completed
tests_started
tests_completed
verification_started
verification_completed
report_submitted
report_accepted
report_rejected
agent_completed
agent_failed
```

---

# Event Normalization

Raw Codex events should be translated into human-readable semantic events.

Instead of exposing only:

```text
command_execution
tool_call
file_change
```

Trix should display entries such as:

```text
Reading backend/auth/service.py
Inspecting authentication middleware
Modifying refresh-token handling
Running authentication tests
41 tests passed
Spawned Database Agent
Waiting for API Agent
Reviewing Database Agent report
Verifying backend integration
Reported results to Manager
```

Store the original Codex event where practical for debugging.

---

# Frontend

Build a modern Strix-inspired interface, while maintaining Trix's own identity.

Recommended stack:

```text
Next.js
React
TypeScript
Tailwind CSS
WebSockets
```

The primary screen should focus on:

```text
Agent hierarchy
Current execution
Live activity
Task status
Verification status
Agent details
Changes
Reports
```

---

# Main UI Layout

Example:

```text
┌──────────────────────────────────────────────────────────────┐
│ TRIX                                           ● Running     │
├──────────────────────┬───────────────────────────────────────┤
│ AGENT TREE           │ CURRENT AGENT / SESSION               │
│                      │                                       │
│ ● Manager            │ Implement authentication system       │
│ ├─ ● Backend         │                                       │
│ │  ├─ ✓ Database     │ Current phase: Verification           │
│ │  └─ ● API          │                                       │
│ │                    │                                       │
│ └─ ● Frontend        │                                       │
│    ├─ ✓ UI           │                                       │
│    └─ ● Tests        │                                       │
├──────────────────────┼───────────────────────────────────────┤
│ SESSION              │ LIVE ACTIVITY                         │
│                      │                                       │
│ Agents: 7            │ Backend is reviewing Database report  │
│ Running: 3           │ API modified auth/routes.py           │
│ Completed: 2         │ Frontend is running npm test          │
│ Failed: 0            │ Manager waiting on Backend            │
└──────────────────────┴───────────────────────────────────────┘
```

---

# Agent Tree

The agent tree is a primary Trix feature.

Each node should show:

```text
Agent name
Role
Current status
Depth
Child count
Current activity
```

Suggested status indicators:

```text
○ queued
◌ starting
● working
◐ waiting
◆ verifying
✓ completed
✕ failed
```

Selecting an agent should display that agent's details.

---

# Agent Detail View

Display:

```text
Name
Role
Parent
Depth
Status
Task
Current activity
Children
Files changed
Commands executed
Verification
Completion report
Errors
Timeline
```

Users should be able to inspect the work of every agent, including leaf agents.

---

# Live Activity

Trix should feel active while work is occurring.

The UI should update over WebSockets without requiring page refreshes.

Example feed:

```text
17:42:03  Manager     Inspecting project structure
17:42:08  Manager     Spawned Backend Agent
17:42:09  Manager     Spawned Frontend Agent
17:42:17  Backend     Spawned Database Agent
17:42:20  Backend     Spawned API Agent
17:42:31  API         Modified backend/api/auth.py
17:42:45  Database    Running migration tests
17:42:53  Database    14 tests passed
17:42:58  Database    Reported completion to Backend
17:43:02  Backend     Reviewing Database Agent changes
```

---

# User Controls

Initial controls should include:

```text
Start
Stop
Cancel
Send instruction to Manager
Open agent
View changes
View report
```

Possible later functionality:

```text
Pause agent
Resume agent
Steer individual agent
Retry failed agent
Reject report
Force verification
Spawn manual agent
Change concurrency
```

Do not prioritize advanced controls until basic orchestration is stable.

---

# Manager Instructions

The root Manager should receive dedicated instructions establishing its role.

Important concepts:

```text
You are the Trix Manager.

You are responsible for the entire requested task.

Delegate independent work when doing so improves execution.

You may have at most two active direct children.

Your children may each create up to two children.

Agents at Depth 2 cannot delegate.

Do not assume work is correct because an agent reports success.

Inspect actual changes.

Run appropriate verification.

Resolve integration issues.

Do not mark the user's task complete until the repository itself demonstrates that the requested work is complete.
```

The exact prompt should be developed separately and tested carefully.

---

# Worker Instructions

Every worker should know:

```text
Its Trix agent ID
Its parent agent
Its depth
Its task
Maximum tree depth
Maximum children
```

Workers should be explicitly instructed that they remain responsible for their child agents.

Agents at Depth 2 should have delegation tools disabled where technically possible, not merely be instructed not to delegate.

Policy should be enforced in code.

---

# Concurrency

Separate hierarchy rules from global execution limits.

For example:

```python
MAX_TREE_DEPTH = 2
MAX_ACTIVE_CHILDREN_PER_AGENT = 2

MAX_GLOBAL_ACTIVE_AGENTS = 6
```

The first two define the logical Trix hierarchy.

The global limit controls resource usage.

These should not be treated as the same concept.

---

# Failure Handling

Agents will fail.

Trix should support:

```text
Codex thread failure
Tool failure
Command failure
Agent timeout
Invalid report
Agent crash
Repository conflict
Cancelled task
Failed verification
```

Failure should propagate intelligently.

A child failure should not automatically terminate the entire Trix session.

The parent should be informed and allowed to:

```text
Retry
Take over the work
Delegate to another child
Modify the approach
Report the blocker upward
```

---

# Report Rejection

Parent verification can reject a child report.

Example:

```text
API Agent
    ↓
"Complete"
    ↓
Backend Agent verifies
    ↓
Tests fail
    ↓
REPORT REJECTED
    ↓
API Agent receives:
"Refresh-token reuse test fails. Fix before reporting completion."
```

This is an important distinction from simple fire-and-forget subagents.

---

# Repository Safety

Multiple Codex agents may modify the same project.

Trix therefore needs to consider concurrency conflicts carefully.

Initially:

* Prefer agents working on clearly separated portions of the project.
* Avoid assigning the same files to multiple write-heavy agents simultaneously.
* Allow read-only research agents more freely.
* Track changed files per agent.
* Warn parents about overlapping changes.
* Require integration verification after parallel work.

More advanced isolation using Git worktrees may be considered later.

---

# Security

Treat all user-controlled and Codex-generated data as untrusted.

At minimum:

* Validate API input.
* Validate WebSocket messages.
* Never expose arbitrary filesystem access through the frontend.
* Restrict repository paths.
* Avoid shell=True unless absolutely necessary.
* Never interpolate frontend-controlled values directly into shell commands.
* Protect secrets and Codex authentication material.
* Do not expose internal stack traces through production APIs.
* Add request size limits.
* Add WebSocket message size limits.
* Add appropriate rate limiting.
* Use structured logging.
* Keep dangerous debugging endpoints disabled outside development.

Security should be considered throughout development rather than added afterward.

---

# Logging

Trix should use structured logs.

Every log should include useful correlation identifiers where possible:

```text
session_id
agent_id
parent_agent_id
task_id
codex_thread_id
```

The UI event timeline and backend application logs should remain separate concepts.

---

# Initial MVP

Do not attempt to implement everything immediately.

The first working milestone should prove the core architecture.

## MVP Requirements

1. Start the backend.
2. Connect to Codex.
3. Create a Trix session.
4. Start one Manager agent.
5. Stream Manager activity to the frontend.
6. Allow the Manager to create a child agent.
7. Associate the child with its parent.
8. Display both agents in the UI.
9. Stream both agents' activity independently.
10. Receive a child completion report.
11. Deliver that result to the parent.
12. Let the parent verify the result.
13. Persist basic session state.

Once this works reliably, expand to the full hierarchy.

---

# Milestone 2 — Hierarchical Delegation

Implement:

```text
Depth enforcement
Two-child enforcement
Depth-1 delegation
Depth-2 delegation prohibition
Waiting for children
Structured completion reports
Parent verification
Report acceptance/rejection
```

Test with:

```text
Manager
├── Backend
│   ├── API
│   └── Database
└── Frontend
    ├── UI
    └── Tests
```

---

# Milestone 3 — Strix-Inspired UI

Add:

```text
Interactive agent graph/tree
Agent status animations
Live semantic event stream
Agent detail panel
Current command
Files changed
Reports
Verification results
Session statistics
```

The interface should make it immediately obvious:

```text
Who is working
What they are doing
Who they report to
Who is waiting
Who is verifying
What completed
What failed
```

---

# Milestone 4 — Reliability

Add:

```text
Session recovery
Agent retry
Failure propagation
Report rejection
Cancellation
Codex reconnect handling
Database migrations
Conflict awareness
Full logging
Integration tests
```

---

# Milestone 5 — Advanced Capabilities

Potential future work:

```text
Git worktree isolation
Agent specialization
Reusable agent profiles
Per-project Trix configuration
Token/cost tracking
Agent performance statistics
Manual steering
Human approval checkpoints
PR creation
GitHub integration
Remote workers
Multiple repositories
Project memory
Reusable workflows
Task dependency graphs
Agent replay
```

---

# Development Principles

When implementing Trix:

1. Do not create fake orchestration where the backend itself pretends to be multiple agents.

2. Codex should remain the actual agent runtime.

3. Do not parse terminal text when structured Codex events are available.

4. Enforce important hierarchy and concurrency policies in code.

5. Treat agent completion reports as claims requiring verification.

6. Preserve parent-child ownership.

7. Avoid unnecessary agents.

8. Prefer bounded tasks with clear ownership.

9. Keep orchestration state separate from Codex conversation state.

10. Make execution observable.

11. Store enough information to understand why a session succeeded or failed.

12. Favor reliability over impressive-looking parallelism.

13. Start with the smallest functional end-to-end implementation.

---

# Definition of Done

The initial Trix project should ultimately allow a user to submit something like:

```text
Add authentication using JWT access and refresh tokens,
including a login UI and tests.
```

Trix should then visibly produce something resembling:

```text
Manager
│
├── Backend Agent
│   ├── Auth API Agent
│   └── Database Agent
│
└── Frontend Agent
    ├── Login UI Agent
    └── Frontend Test Agent
```

Each agent should execute independently through Codex.

Leaf agents report to their direct parents.

Depth-1 agents inspect, integrate, and verify child work.

Depth-1 agents report consolidated results to the Manager.

The Manager performs final repository-level verification.

The user can observe this entire process from the Trix interface.

Only after final verification passes should Trix report the user's task as completed.
