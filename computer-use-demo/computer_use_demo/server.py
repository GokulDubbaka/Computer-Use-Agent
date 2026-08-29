from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .loop import APIProvider, sampling_loop
from .session_store import SessionStore, SessionStoreConfig
from .tools import ToolResult


def _db_path() -> Path:
    configured = os.getenv("SESSION_DB_PATH")
    return Path(configured).expanduser() if configured else Path.home() / ".anthropic" / "sessions.sqlite3"


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


class RunFinish(BaseModel):
    status: str = Field(default="completed", pattern="^(completed|failed|cancelled)$")
    result: dict[str, Any] = Field(default_factory=dict)
    error_text: str | None = None


class EventHub:
    def __init__(self) -> None:
        self._conditions: dict[str, asyncio.Condition] = {}

    def _condition(self, session_id: str) -> asyncio.Condition:
        return self._conditions.setdefault(session_id, asyncio.Condition())

    async def notify(self, session_id: str) -> None:
        condition = self._condition(session_id)
        async with condition:
            condition.notify_all()

    async def wait(self, session_id: str, timeout: float = 10.0) -> None:
        try:
            condition = self._condition(session_id)
            async with condition:
                await asyncio.wait_for(condition.wait(), timeout=timeout)
        except TimeoutError:
            pass


store = SessionStore(SessionStoreConfig(db_path=_db_path()))
hub = EventHub()
runtime_lock = asyncio.Lock()
background_tasks: set[asyncio.Task[Any]] = set()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await asyncio.to_thread(store.recover_incomplete_runs)
    yield
    for task in list(background_tasks):
        task.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)


app = FastAPI(
    title="Computer Use Session Manager",
    description="Durable, event-streamed session control plane for Anthropic computer use.",
    version="1.0.0",
    lifespan=lifespan,
)


def _spawn(coro: Any) -> None:
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)


async def _append_event(session_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    event = await asyncio.to_thread(store.append_event, session_id, event_type, payload)
    await hub.notify(session_id)
    return event


def _latest_user_prompt(session_id: str) -> str | None:
    for message in reversed(store.list_messages(session_id)):
        if message["role"] == "user":
            return str(message["content"])
    return None


def _block_payload(block: Any) -> dict[str, Any]:
    if isinstance(block, dict):
        return block
    if hasattr(block, "model_dump"):
        return block.model_dump()
    return {"value": str(block)}


async def _execute_agent(session_id: str, run_id: str, prompt: str, model: str | None) -> None:
    loop = asyncio.get_running_loop()
    assistant_text: list[str] = []

    def emit(event_type: str, payload: dict[str, Any]) -> None:
        asyncio.run_coroutine_threadsafe(_append_event(session_id, event_type, payload), loop)

    def output_callback(block: Any) -> None:
        payload = _block_payload(block)
        if payload.get("type") == "text" and payload.get("text"):
            assistant_text.append(str(payload["text"]))
        emit("agent.output", {"run_id": run_id, "block": payload})

    def tool_output_callback(result: ToolResult, tool_use_id: str) -> None:
        emit(
            "agent.tool_result",
            {
                "run_id": run_id,
                "tool_use_id": tool_use_id,
                "output": result.output,
                "error": result.error,
                "has_image": bool(result.base64_image),
            },
        )

    def api_response_callback(request: Any, response: Any, error: Exception | None) -> None:
        emit(
            "agent.api_response",
            {
                "run_id": run_id,
                "status_code": getattr(response, "status_code", None),
                "error": str(error) if error else None,
            },
        )

    try:
        await asyncio.to_thread(store.claim_run, run_id, worker_id=f"local-{os.getpid()}")
        await _append_event(session_id, "run.started", {"run_id": run_id})
        provider = APIProvider(os.getenv("API_PROVIDER", "anthropic"))
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if provider == APIProvider.ANTHROPIC and not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is required to execute an agent run")

        async with runtime_lock:
            await _append_event(session_id, "runtime.acquired", {"run_id": run_id})

            def run_sampling_loop() -> Any:
                return asyncio.run(
                    sampling_loop(
                        model=model or os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
                        provider=provider,
                        system_prompt_suffix="",
                        messages=[{"role": "user", "content": prompt}],
                        output_callback=output_callback,
                        tool_output_callback=tool_output_callback,
                        api_response_callback=api_response_callback,
                        api_key=api_key,
                        only_n_most_recent_images=3,
                        max_tokens=16_384,
                        tool_version="computer_use_20251124",
                        thinking_mode="adaptive",
                        thinking_effort="medium",
                    )
                )

            await asyncio.to_thread(run_sampling_loop)

        if assistant_text:
            await asyncio.to_thread(
                store.create_message,
                session_id,
                role="assistant",
                content="\n".join(assistant_text),
            )
        run = await asyncio.to_thread(
            store.finish_run,
            run_id,
            status="completed",
            result={"assistant_text": "\n".join(assistant_text)},
        )
        assert run is not None
        await _append_event(session_id, "run.finished", {"run_id": run_id, "status": run["status"]})
    except asyncio.CancelledError:
        await asyncio.to_thread(store.finish_run, run_id, status="cancelled", error_text="server shutdown")
        raise
    except Exception as exc:
        await asyncio.to_thread(store.finish_run, run_id, status="failed", error_text=str(exc))
        await _append_event(session_id, "run.failed", {"run_id": run_id, "error": str(exc)})


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/")
async def home() -> FileResponse:
    return FileResponse(Path(__file__).with_name("static") / "index.html")


@app.get("/api/sessions")
async def list_sessions(limit: int = Query(default=50, ge=1, le=200)) -> dict[str, Any]:
    return {"items": await asyncio.to_thread(store.list_sessions, limit=limit)}


@app.post("/api/sessions")
async def create_session(payload: SessionCreate) -> JSONResponse:
    session = await asyncio.to_thread(store.create_session, title=payload.title, metadata=payload.metadata)
    await _append_event(session["id"], "session.created", {"title": session["title"]})
    return JSONResponse(session, status_code=201)


@app.get("/api/sessions/{session_id}")
async def get_session(session_id: str) -> dict[str, Any]:
    session = await asyncio.to_thread(store.get_session, session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session not found")
    return {
        **session,
        "active_run": await asyncio.to_thread(store.get_active_run, session_id),
        "messages": await asyncio.to_thread(store.list_messages, session_id),
        "events": await asyncio.to_thread(store.list_events, session_id, after_sequence=0, limit=200),
    }


@app.delete("/api/sessions/{session_id}", status_code=204)
async def delete_session(session_id: str) -> Response:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    if not await asyncio.to_thread(store.delete_session, session_id):
        raise HTTPException(status_code=409, detail="cannot delete a session with an active run")
    return Response(status_code=204)


@app.post("/api/sessions/{session_id}/messages")
async def create_message(session_id: str, payload: MessageCreate) -> JSONResponse:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    message = await asyncio.to_thread(store.create_message, session_id, role=payload.role, content=payload.content)
    await _append_event(session_id, "message.created", {"message_id": message["id"], "role": payload.role})
    return JSONResponse(message, status_code=201)


@app.get("/api/sessions/{session_id}/messages")
async def list_messages(session_id: str) -> dict[str, Any]:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    return {"items": await asyncio.to_thread(store.list_messages, session_id)}


@app.post("/api/sessions/{session_id}/runs")
async def create_run(session_id: str, request: Request, payload: RunCreate) -> JSONResponse:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    request_id = payload.request_id or request.headers.get("Idempotency-Key") or request.headers.get("X-Request-Id")
    request_id = request_id or f"req_{uuid.uuid4().hex}"
    prompt = payload.prompt or await asyncio.to_thread(_latest_user_prompt, session_id)
    if payload.prompt:
        await asyncio.to_thread(store.create_message, session_id, role="user", content=payload.prompt)

    try:
        run, created = await asyncio.to_thread(store.create_run, session_id=session_id, request_id=request_id)
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="session already has an active run") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    if created:
        await _append_event(session_id, "run.queued", {"run_id": run["id"], "request_id": request_id})
        if prompt:
            _spawn(_execute_agent(session_id, run["id"], prompt, payload.model))
    return JSONResponse(run, status_code=201 if created else 200)


@app.post("/api/sessions/{session_id}/runs/{run_id}/finish")
async def finish_run(session_id: str, run_id: str, payload: RunFinish) -> dict[str, Any]:
    run = await asyncio.to_thread(
        store.finish_run,
        run_id,
        status=payload.status,
        result=payload.result,
        error_text=payload.error_text,
    )
    if run is None or run["session_id"] != session_id:
        raise HTTPException(status_code=404, detail="run not found")
    await _append_event(session_id, "run.finished", {"run_id": run_id, "status": run["status"]})
    return run


@app.get("/api/sessions/{session_id}/events")
async def replay_events(session_id: str, after: int = Query(default=0, ge=0)) -> dict[str, Any]:
    if await asyncio.to_thread(store.get_session, session_id) is None:
        raise HTTPException(status_code=404, detail="session not found")
    return {"items": await asyncio.to_thread(store.list_events, session_id, after_sequence=after)}


@app.get("/api/sessions/{session_id}/stream")
async def stream_events(session_id: str, request: Request, after: int = Query(default=0, ge=0)) -> StreamingResponse:
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
            events = await asyncio.to_thread(store.list_events, session_id, after_sequence=cursor, limit=100)
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
