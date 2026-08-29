from __future__ import annotations

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
import websockets
from fastapi import (
    Cookie,
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from pydantic import BaseModel, Field
from websockets.exceptions import WebSocketException
from websockets.typing import Subprotocol

from .postgres_store import ConflictError, PostgresStore, PostgresStoreConfig
from .security import create_ticket, verify_ticket
from .settings import settings


class SessionCreate(BaseModel):
    title: str | None = Field(default=None, max_length=200)
    metadata: dict[str, Any] = Field(default_factory=dict)


class MessageCreate(BaseModel):
    role: str = Field(default="user", pattern="^(user|assistant|system)$")
    content: str = Field(min_length=1, max_length=100_000)


class RunCreate(BaseModel):
    prompt: str | None = Field(default=None, min_length=1, max_length=100_000)
    request_id: str | None = Field(default=None, max_length=200)
    model: str | None = Field(default=None, max_length=200)


class RuntimeEvent(BaseModel):
    event_type: str = Field(min_length=1, max_length=100)
    run_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class RuntimeFinish(BaseModel):
    status: str = Field(pattern="^(completed|failed|cancelled|interrupted)$")
    lease_version: int = Field(ge=1)
    result: dict[str, Any] = Field(default_factory=dict)
    error_text: str | None = None


class EventHub:
    def __init__(self) -> None:
        self._conditions: dict[str, asyncio.Condition] = {}

    async def notify(self, session_id: str) -> None:
        condition = self._conditions.setdefault(session_id, asyncio.Condition())
        async with condition:
            condition.notify_all()

    async def wait(self, session_id: str, timeout: float = 2.0) -> None:
        condition = self._conditions.setdefault(session_id, asyncio.Condition())
        try:
            async with condition:
                await asyncio.wait_for(condition.wait(), timeout=timeout)
        except TimeoutError:
            pass


store = PostgresStore(
    PostgresStoreConfig(
        database_url=settings.database_url,
        session_ttl_seconds=settings.session_ttl_seconds,
    )
)
hub = EventHub()
background_tasks: set[asyncio.Task[Any]] = set()


async def _controller(method: str, path: str, **kwargs: Any) -> httpx.Response:
    headers = {"X-Internal-Token": settings.internal_token}
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.request(
            method, f"{settings.controller_url}{path}", headers=headers, **kwargs
        )
    response.raise_for_status()
    return response


async def _append_event(
    session_id: str,
    event_type: str,
    payload: dict[str, Any],
    *,
    run_id: str | None = None,
    runtime_id: str | None = None,
) -> dict[str, Any]:
    event = await asyncio.to_thread(
        store.append_event,
        session_id,
        event_type,
        payload,
        run_id=run_id,
        runtime_id=runtime_id,
    )
    await hub.notify(session_id)
    return event


async def _reaper() -> None:
    while True:
        await asyncio.sleep(30)
        interrupted = await asyncio.to_thread(store.interrupt_expired_runs)
        for run in interrupted:
            await _append_event(
                run["session_id"],
                "run.interrupted",
                {"run_id": run["id"], "reason": "worker_lease_expired"},
                run_id=run["id"],
            )
        sessions = await asyncio.to_thread(store.list_expired_sessions)
        for session in sessions:
            try:
                await _controller("DELETE", f"/runtimes/{session['id']}")
                await asyncio.to_thread(store.terminate_session, session["id"])
            except Exception:
                continue


@asynccontextmanager
async def lifespan(_: FastAPI):
    for attempt in range(30):
        try:
            await asyncio.to_thread(store.ping)
            break
        except Exception:
            if attempt == 29:
                raise
            await asyncio.sleep(1)
    task = asyncio.create_task(_reaper())
    background_tasks.add(task)
    yield
    for running in list(background_tasks):
        running.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)


app = FastAPI(
    title="Computer Use Session Control Plane",
    description="Isolated, durable and replayable computer-use sessions.",
    version="2.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)


def _runtime_auth(session_id: str, token: str | None) -> dict[str, Any]:
    if token is None or not store.authenticate_runtime(session_id, token):
        raise HTTPException(status_code=401, detail="invalid runtime credential")
    session = store.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session not found")
    return session


def _public_session(session: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in session.items()
        if key not in {"runtime_token_hash", "runtime_host"}
    }


def _desktop_url(session_id: str) -> str:
    return (
        f"/desktop/{session_id}/vnc.html?autoconnect=1&resize=scale&reconnect=1"
        f"&path=desktop/{session_id}/websockify"
    )


def _desktop_launch_url(session_id: str) -> str:
    """Open a fresh, session-bound desktop ticket before redirecting to noVNC."""
    return f"/desktop/{session_id}/launch"


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
async def ready() -> dict[str, str]:
    await asyncio.to_thread(store.ping)
    await _controller("GET", "/health")
    return {"status": "ready"}


@app.get("/")
async def home() -> FileResponse:
    return FileResponse(Path(__file__).with_name("static") / "index.html")


@app.get("/api/sessions")
async def list_sessions(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    sessions = await asyncio.to_thread(store.list_sessions, limit=limit)
    return {"items": [_public_session(session) for session in sessions]}


@app.post("/api/sessions")
async def create_session(payload: SessionCreate) -> JSONResponse:
    session, runtime_token = await asyncio.to_thread(
        store.create_session, title=payload.title, metadata=payload.metadata
    )
    await _append_event(session["id"], "session.provisioning", {})
    try:
        runtime_response = await _controller(
            "POST",
            "/runtimes",
            json={"session_id": session["id"], "runtime_token": runtime_token},
        )
        runtime = runtime_response.json()
        session = await asyncio.to_thread(
            store.attach_runtime,
            session["id"],
            runtime["runtime_id"],
            runtime["runtime_host"],
        )
        await _append_event(
            session["id"],
            "session.ready",
            {"runtime_id": runtime["runtime_id"]},
            runtime_id=runtime["runtime_id"],
        )
    except Exception as exc:
        await _append_event(session["id"], "session.provisioning_failed", {"error": str(exc)})
        try:
            await _controller("DELETE", f"/runtimes/{session['id']}")
        finally:
            await asyncio.to_thread(store.fail_session, session["id"])
        raise HTTPException(status_code=503, detail="runtime provisioning failed") from exc
    return JSONResponse(_public_session(session), status_code=201)


@app.get("/api/sessions/{session_id}")
async def get_session(session_id: str) -> dict[str, Any]:
    snapshot = await asyncio.to_thread(store.get_session_snapshot, session_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="session not found")
    return {
        **_public_session(snapshot["session"]),
        "active_run": snapshot["active_run"],
        "messages": snapshot["messages"],
        "events": snapshot["events"],
        "desktop_url": _desktop_url(session_id),
    }


@app.delete("/api/sessions/{session_id}", status_code=204)
async def delete_session(session_id: str) -> Response:
    try:
        requested = await asyncio.to_thread(store.request_termination, session_id)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not requested:
        raise HTTPException(status_code=404, detail="session not found")
    await _controller("DELETE", f"/runtimes/{session_id}")
    await asyncio.to_thread(store.terminate_session, session_id)
    return Response(status_code=204)


@app.post("/api/sessions/{session_id}/messages")
async def create_message(session_id: str, payload: MessageCreate) -> JSONResponse:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    message = await asyncio.to_thread(
        store.create_message, session_id, role=payload.role, content=payload.content
    )
    await _append_event(
        session_id, "message.created", {"message_id": message["id"], "role": payload.role}
    )
    return JSONResponse(message, status_code=201)


@app.get("/api/sessions/{session_id}/messages")
async def list_messages(session_id: str) -> dict[str, Any]:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    return {"items": await asyncio.to_thread(store.list_messages, session_id)}


@app.post("/api/sessions/{session_id}/runs")
async def create_run(session_id: str, request: Request, payload: RunCreate) -> JSONResponse:
    request_id = (
        payload.request_id
        or request.headers.get("Idempotency-Key")
        or request.headers.get("X-Request-Id")
        or f"req_{uuid.uuid4().hex}"
    )
    prompt = payload.prompt
    if not prompt:
        messages = await asyncio.to_thread(store.list_messages, session_id)
        prompt = next(
            (str(message["content"]) for message in reversed(messages) if message["role"] == "user"),
            None,
        )
    if not prompt:
        raise HTTPException(status_code=422, detail="prompt or prior user message is required")
    try:
        run, created = await asyncio.to_thread(
            store.create_run,
            session_id=session_id,
            idempotency_key=request_id,
            prompt=prompt,
            model=payload.model,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="session not found") from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if created:
        await _append_event(
            session_id, "run.queued", {"run_id": run["id"]}, run_id=run["id"]
        )
    return JSONResponse(run, status_code=201 if created else 200)


@app.get("/api/sessions/{session_id}/events")
async def replay_events(
    session_id: str, after: int = Query(default=0, ge=0)
) -> dict[str, Any]:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    return {
        "items": await asyncio.to_thread(store.list_events, session_id, after_sequence=after)
    }


@app.get("/api/sessions/{session_id}/stream")
async def stream_events(
    session_id: str, request: Request, after: int = Query(default=0, ge=0)
) -> StreamingResponse:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    try:
        after = max(after, int(request.headers.get("last-event-id", "0")))
    except ValueError:
        pass

    async def event_stream():
        cursor = after
        yield ": connected\n\n"
        while not await request.is_disconnected():
            events = await asyncio.to_thread(
                store.list_events, session_id, after_sequence=cursor, limit=100
            )
            if not events:
                await hub.wait(session_id)
                yield ": keepalive\n\n"
                continue
            for event in events:
                cursor = event["sequence"]
                yield f"id: {cursor}\ndata: {json.dumps(event)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/sessions/{session_id}/desktop-ticket")
async def desktop_ticket(session_id: str, response: Response) -> dict[str, str]:
    session = await asyncio.to_thread(store.get_session, session_id)
    if session is None or session["status"] not in ("ready", "running"):
        raise HTTPException(status_code=409, detail="desktop is not ready")
    ticket = create_ticket(session_id, settings.ticket_secret)
    response.set_cookie(
        "desktop_ticket",
        ticket,
        max_age=120,
        httponly=True,
        samesite="strict",
        secure=False,
        path=f"/desktop/{session_id}",
    )
    return {"url": _desktop_url(session_id), "launch_url": _desktop_launch_url(session_id)}


@app.get("/desktop/{session_id}/launch")
async def launch_desktop(session_id: str) -> Response:
    """Issue a new short-lived ticket for a top-level noVNC window."""
    session = await asyncio.to_thread(store.get_session, session_id)
    if session is None or session["status"] not in ("ready", "running"):
        raise HTTPException(status_code=409, detail="desktop is not ready")
    response = RedirectResponse(_desktop_url(session_id), status_code=302)
    response.set_cookie(
        "desktop_ticket",
        create_ticket(session_id, settings.ticket_secret),
        max_age=120,
        httponly=True,
        samesite="strict",
        secure=False,
        path=f"/desktop/{session_id}",
    )
    return response


def _desktop_session(session_id: str, ticket: str | None) -> dict[str, Any]:
    if not ticket or not verify_ticket(ticket, settings.ticket_secret, session_id):
        raise HTTPException(status_code=401, detail="invalid desktop ticket")
    session = store.get_session(session_id)
    if session is None or not session.get("runtime_host"):
        raise HTTPException(status_code=404, detail="desktop not found")
    return session


@app.api_route("/desktop/{session_id}/{path:path}", methods=["GET", "HEAD"])
async def desktop_http_proxy(
    session_id: str,
    path: str,
    request: Request,
    desktop_ticket: str | None = Cookie(default=None),
) -> Response:
    session = await asyncio.to_thread(_desktop_session, session_id, desktop_ticket)
    target = f"http://{session['runtime_host']}:6080/{path}"
    async with httpx.AsyncClient(timeout=30) as client:
        upstream = await client.request(request.method, target, params=request.query_params)
    headers = {}
    for name in ("content-type", "cache-control", "etag", "last-modified"):
        if name in upstream.headers:
            headers[name] = upstream.headers[name]
    headers["Cache-Control"] = "no-cache"
    return Response(upstream.content, status_code=upstream.status_code, headers=headers)


@app.websocket("/desktop/{session_id}/{path:path}")
async def desktop_websocket_proxy(session_id: str, path: str, websocket: WebSocket) -> None:
    ticket = websocket.cookies.get("desktop_ticket")
    try:
        session = await asyncio.to_thread(_desktop_session, session_id, ticket)
    except HTTPException:
        await websocket.close(code=4401)
        return
    query = f"?{websocket.url.query}" if websocket.url.query else ""
    target = f"ws://{session['runtime_host']}:6080/{path}{query}"
    await websocket.accept(subprotocol=websocket.headers.get("sec-websocket-protocol"))
    try:
        async with websockets.connect(
            target,
            subprotocols=[Subprotocol("binary")],
            max_size=None,
        ) as upstream:
            async def browser_to_vnc() -> None:
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        return
                    data = message.get("bytes")
                    await upstream.send(data if data is not None else message.get("text", ""))

            async def vnc_to_browser() -> None:
                async for message in upstream:
                    if isinstance(message, bytes):
                        await websocket.send_bytes(message)
                    else:
                        await websocket.send_text(message)

            first, second = await asyncio.wait(
                {
                    asyncio.create_task(browser_to_vnc()),
                    asyncio.create_task(vnc_to_browser()),
                },
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in first | second:
                task.cancel()
            await asyncio.gather(*(first | second), return_exceptions=True)
    except (WebSocketDisconnect, OSError, WebSocketException):
        pass


@app.get("/internal/desktop-auth")
async def desktop_auth(
    x_forwarded_uri: str | None = Header(default=None),
    desktop_ticket: str | None = Cookie(default=None),
) -> Response:
    parts = (x_forwarded_uri or "").split("/")
    session_id = parts[2] if len(parts) > 2 and parts[1] == "desktop" else ""
    if not desktop_ticket or not verify_ticket(
        desktop_ticket, settings.ticket_secret, session_id
    ):
        raise HTTPException(status_code=401, detail="invalid desktop ticket")
    return Response(status_code=204)


@app.post("/internal/runtime/{session_id}/events")
async def runtime_event(
    session_id: str,
    payload: RuntimeEvent,
    x_runtime_token: str | None = Header(default=None),
) -> dict[str, Any]:
    session = await asyncio.to_thread(_runtime_auth, session_id, x_runtime_token)
    return await _append_event(
        session_id,
        payload.event_type,
        payload.payload,
        run_id=payload.run_id,
        runtime_id=session["runtime_id"],
    )


@app.post("/internal/runtime/{session_id}/next-run")
async def runtime_next_run(
    session_id: str,
    x_runtime_token: str | None = Header(default=None),
    x_worker_id: str = Header(),
) -> Response:
    await asyncio.to_thread(_runtime_auth, session_id, x_runtime_token)
    run = await asyncio.to_thread(store.claim_next_run, session_id, x_worker_id)
    if run is None:
        return Response(status_code=204)
    await _append_event(session_id, "run.started", {"run_id": run["id"]}, run_id=run["id"])
    return JSONResponse(run)


@app.post("/internal/runtime/{session_id}/runs/{run_id}/heartbeat")
async def runtime_heartbeat(
    session_id: str,
    run_id: str,
    x_runtime_token: str | None = Header(default=None),
    x_worker_id: str = Header(),
    x_lease_version: int = Header(),
) -> dict[str, bool]:
    await asyncio.to_thread(_runtime_auth, session_id, x_runtime_token)
    renewed = await asyncio.to_thread(
        store.heartbeat_run, session_id, run_id, x_worker_id, x_lease_version
    )
    return {"renewed": renewed}


@app.post("/internal/runtime/{session_id}/runs/{run_id}/finish")
async def runtime_finish(
    session_id: str,
    run_id: str,
    payload: RuntimeFinish,
    x_runtime_token: str | None = Header(default=None),
    x_worker_id: str = Header(),
) -> dict[str, Any]:
    await asyncio.to_thread(_runtime_auth, session_id, x_runtime_token)
    run = await asyncio.to_thread(
        store.finish_run,
        run_id,
        status=payload.status,
        result=payload.result,
        error_text=payload.error_text,
        worker_id=x_worker_id,
        lease_version=payload.lease_version,
    )
    if run is None or run["session_id"] != session_id:
        raise HTTPException(status_code=409, detail="run lease is no longer valid")
    if payload.result.get("assistant_text"):
        await asyncio.to_thread(
            store.create_message,
            session_id,
            role="assistant",
            content=payload.result["assistant_text"],
            run_id=run_id,
        )
    await _append_event(
        session_id,
        "run.finished",
        {"run_id": run_id, "status": payload.status},
        run_id=run_id,
    )
    return run


# ── Gokul Dubbaka ──────────────────────────────────────────────────────────
