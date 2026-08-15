from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from trix.codex import CodexAppServer
from trix.models import (
    Agent,
    AgentReport,
    CreateSession,
    Event,
    Instruction,
    ReviewReport,
    SpawnAgent,
)
from trix.orchestrator import Orchestrator
from trix.policies import PolicyViolation
from trix.store import Store

PACKAGE_ROOT = Path(__file__).parent
STATIC_ROOT = PACKAGE_ROOT / "static"
DATA_PATH = Path(os.environ.get("TRIX_DATABASE", ".trix/trix.db")).resolve()

store = Store(DATA_PATH)
codex = CodexAppServer(os.environ.get("TRIX_CODEX_EXECUTABLE", "codex"))
orchestrator = Orchestrator(store, codex)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await orchestrator.reconcile_orphaned_sessions()
    yield
    await orchestrator.aclose()
    await codex.close()


app = FastAPI(title="Trix", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def prevent_stale_frontend(request: Any, call_next: Any) -> Any:
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/assets/"):
        response.headers["Cache-Control"] = "no-store"
    return response


def not_found(error: KeyError) -> HTTPException:
    return HTTPException(status_code=404, detail=f"Resource not found: {error.args[0]}")


def agent_payload(agent: Agent) -> dict[str, Any]:
    return {
        **agent.model_dump(mode="json"),
        "reports": [report.model_dump(mode="json") for report in store.reports_for_agent(agent.id)],
    }


@app.get("/api/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "trix"}


@app.get("/api/sessions")
async def sessions() -> list[dict[str, Any]]:
    return [item.model_dump(mode="json") for item in store.list_sessions()]


@app.post("/api/sessions", status_code=201)
async def create_session(payload: CreateSession) -> dict[str, Any]:
    try:
        session = await orchestrator.create_session(
            payload.title, payload.prompt, payload.repository_path
        )
    except PolicyViolation as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    return session.model_dump(mode="json")


@app.get("/api/sessions/{session_id}")
async def session_detail(session_id: str) -> dict[str, Any]:
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    agents = store.list_agents(session_id)
    return {
        "session": session.model_dump(mode="json"),
        "agents": [agent_payload(agent) for agent in agents],
        "events": [event.model_dump(mode="json") for event in store.list_events(session_id)],
    }


@app.post("/api/sessions/{session_id}/start")
async def start_session(session_id: str) -> dict[str, Any]:
    try:
        return (await orchestrator.start_session(session_id)).model_dump(mode="json")
    except KeyError as error:
        raise not_found(error) from error
    except PolicyViolation as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/api/sessions/{session_id}/cancel")
async def cancel_session(session_id: str) -> dict[str, Any]:
    try:
        return (await orchestrator.cancel_session(session_id)).model_dump(mode="json")
    except KeyError as error:
        raise not_found(error) from error


@app.post("/api/agents", status_code=201)
async def spawn_agent(payload: SpawnAgent) -> dict[str, Any]:
    try:
        agent = await orchestrator.spawn(
            payload.parent_id, payload.name, payload.role, payload.task
        )
    except KeyError as error:
        raise not_found(error) from error
    except PolicyViolation as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return agent.model_dump(mode="json")


@app.post("/api/agents/{agent_id}/instructions")
async def instruct_agent(agent_id: str, payload: Instruction) -> dict[str, Any]:
    try:
        return (await orchestrator.instruct(agent_id, payload.message)).model_dump(mode="json")
    except KeyError as error:
        raise not_found(error) from error
    except PolicyViolation as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/api/agents/{agent_id}/reports", status_code=201)
async def submit_report(agent_id: str, report: AgentReport) -> dict[str, Any]:
    try:
        return (await orchestrator.submit_report(agent_id, report)).model_dump(mode="json")
    except KeyError as error:
        raise not_found(error) from error
    except PolicyViolation as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@app.post("/api/reports/{report_id}/review")
async def review_report(report_id: str, payload: ReviewReport) -> dict[str, Any]:
    try:
        report = await orchestrator.review_report(report_id, payload.accepted, payload.feedback)
    except KeyError as error:
        raise not_found(error) from error
    return report.model_dump(mode="json")


@app.websocket("/api/sessions/{session_id}/events")
async def events_socket(websocket: WebSocket, session_id: str) -> None:
    if store.get_session(session_id) is None:
        await websocket.close(code=4404)
        return
    await websocket.accept()

    async def send(event: Event) -> None:
        session = store.get_session(session_id)
        agent = store.get_agent(event.agent_id) if event.agent_id else None
        await websocket.send_json(
            {
                "type": "event",
                "event": event.model_dump(mode="json"),
                "session": session.model_dump(mode="json") if session else None,
                "agent": agent_payload(agent) if agent else None,
            }
        )

    orchestrator.subscribe(session_id, send)
    try:
        session = store.get_session(session_id)
        agents = store.list_agents(session_id)
        await websocket.send_json(
            {
                "type": "snapshot",
                "session": session.model_dump(mode="json") if session else None,
                "agents": [agent_payload(agent) for agent in agents],
                "events": [
                    event.model_dump(mode="json") for event in store.list_events(session_id)
                ],
            }
        )
        while True:
            message = await websocket.receive_text()
            if len(message) > 64_000:
                await websocket.close(code=1009)
                return
            if message == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        orchestrator.unsubscribe(session_id, send)


app.mount("/assets", StaticFiles(directory=STATIC_ROOT), name="assets")


@app.get("/{path:path}", include_in_schema=False)
async def frontend(path: str) -> FileResponse:
    return FileResponse(STATIC_ROOT / "index.html")
