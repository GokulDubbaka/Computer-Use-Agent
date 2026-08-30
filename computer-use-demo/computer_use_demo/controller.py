from __future__ import annotations

import asyncio
import os
import re
from typing import Any

import docker
from docker.errors import APIError, NotFound
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from .settings import settings

app = FastAPI(title="Capsule Controller", docs_url=None, redoc_url=None)
client = docker.from_env()


class RuntimeCreate(BaseModel):
    session_id: str = Field(pattern=r"^sess_[a-f0-9]{32}$")
    runtime_token: str = Field(min_length=32)


def _authorize(value: str | None) -> None:
    if value != settings.internal_token:
        raise HTTPException(status_code=401, detail="invalid controller credential")


def _runtime_name(session_id: str) -> str:
    if not re.fullmatch(r"sess_[a-f0-9]{32}", session_id):
        raise ValueError("invalid session id")
    return f"capsule-{session_id}"


async def _wait_for_runtime(name: str, attempts: int = 20) -> Any:
    for attempt in range(attempts):
        try:
            return await asyncio.to_thread(client.containers.get, name)
        except NotFound:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(0.1)
    raise RuntimeError("unreachable")


async def _wait_for_desktop(name: str, attempts: int = 50) -> None:
    for attempt in range(attempts):
        try:
            _, writer = await asyncio.open_connection(name, 6080)
        except OSError:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(0.1)
            continue
        writer.close()
        await writer.wait_closed()
        return


@app.get("/health")
async def health() -> dict[str, str]:
    await asyncio.to_thread(client.ping)
    return {"status": "ok"}


@app.post("/runtimes", status_code=201)
async def create_runtime(
    payload: RuntimeCreate, x_internal_token: str | None = Header(default=None)
) -> dict[str, Any]:
    _authorize(x_internal_token)
    name = _runtime_name(payload.session_id)
    try:
        existing = await asyncio.to_thread(client.containers.get, name)
        if existing.status == "running":
            await _wait_for_desktop(name)
            return {"runtime_id": existing.id, "runtime_host": name, "created": False}
        await asyncio.to_thread(existing.remove, force=True, v=True)
    except NotFound:
        pass

    labels = {
        "computer-use.managed": "true",
        "computer-use.session-id": payload.session_id,
    }
    try:
        container = await asyncio.to_thread(
            client.containers.run,
            settings.capsule_image,
            name=name,
            detach=True,
            network=settings.capsule_network,
            labels=labels,
            environment={
                "SESSION_ID": payload.session_id,
                "RUNTIME_TOKEN": payload.runtime_token,
                "CONTROL_PLANE_URL": "http://api:8000",
                "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY", ""),
                "ANTHROPIC_WORKSPACE_ID": os.getenv("ANTHROPIC_WORKSPACE_ID", ""),
                "ANTHROPIC_MODEL": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
                "API_PROVIDER": os.getenv("API_PROVIDER", "anthropic"),
            },
            mem_limit=os.getenv("CAPSULE_MEMORY_LIMIT", "2g"),
            nano_cpus=int(float(os.getenv("CAPSULE_CPU_LIMIT", "1.5")) * 1_000_000_000),
            pids_limit=int(os.getenv("CAPSULE_PIDS_LIMIT", "1024")),
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            shm_size="512m",
        )
    except APIError as exc:
        if exc.status_code != 409:
            raise
        container = await _wait_for_runtime(name)
        await _wait_for_desktop(name)
        return {"runtime_id": container.id, "runtime_host": name, "created": False}
    await _wait_for_desktop(name)
    return {"runtime_id": container.id, "runtime_host": name, "created": True}


@app.delete("/runtimes/{session_id}", status_code=204)
async def destroy_runtime(
    session_id: str, x_internal_token: str | None = Header(default=None)
) -> None:
    _authorize(x_internal_token)
    try:
        container = await asyncio.to_thread(client.containers.get, _runtime_name(session_id))
    except NotFound:
        return
    await asyncio.to_thread(container.remove, force=True, v=True)
