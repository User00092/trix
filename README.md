# Trix

Trix is an open-source AI development orchestration tool that coordinates Codex agents as a
structured software-engineering team. Codex remains responsible for repository work, commands,
reasoning, implementation, and testing. Trix supplies the hierarchy, policy, persistent state,
verification gates, and live interface around that work.

## What works in the MVP

- A root Manager and independently streamed worker agents, each backed by a Codex App Server thread.
- A zero-indexed, three-level hierarchy with a maximum of two active direct children.
- A separate global concurrency limit.
- Persistent SQLite sessions, agents, structured reports, and normalized activity events.
- Explicit separation between `report submitted` and `work accepted`.
- Report rejection with feedback delivered back to the worker's Codex thread.
- Native `trix.*` dynamic tools that let Managers and workers delegate from inside Codex turns.
- A strict read-only Manager that delegates all execution and only directs, observes, and verifies.
- REST controls and a WebSocket-powered agent tree, activity feed, and detail view.
- A required repository path that becomes the canonical per-session filesystem authority.

See [strix-to-trix.md](strix-to-trix.md) for the product direction and later milestones.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- A working, authenticated `codex` CLI with App Server support

## Run locally

```bash
uv sync
uv run trix --reload
```

Open <http://127.0.0.1:8787>, provide an existing repository directory, create a session, and start
its Manager. Trix canonicalizes the required path and automatically grants that session access to it.

Configuration:

```bash
export TRIX_DATABASE=/absolute/path/to/trix.db
export TRIX_CODEX_EXECUTABLE=codex
```

The OpenAPI document is available at `/docs`. Important endpoints include:

```text
POST /api/sessions
POST /api/sessions/{id}/start
POST /api/agents
POST /api/agents/{id}/instructions
POST /api/agents/{id}/reports
POST /api/reports/{id}/review
WS   /api/sessions/{id}/events
```

Every Codex thread receives depth-appropriate Trix tools. Managers and depth-1 workers can spawn up
to two direct children, steer them, inspect state and changes, and accept or reject reports. Leaf
workers cannot delegate. The Manager receives a read-only repository sandbox and can complete the
session only after every delegated agent has reached accepted completion.

## Development

```bash
make dev-install
make test
make check-all
```

The old `strix/` source is retained temporarily as migration reference but is no longer packaged or
exposed as a console command. New implementation belongs in `trix/`.

## Architecture

```text
Browser ── REST/WebSocket ── FastAPI
                              │
                     Trix Orchestrator
                       │            │
                    SQLite      Codex App Server
                                      │
                         Manager/worker threads
```

Trix is licensed under Apache-2.0. It began as a fork of
[usestrix/strix](https://github.com/usestrix/strix); preserved upstream code retains its original
copyright and license notices.
