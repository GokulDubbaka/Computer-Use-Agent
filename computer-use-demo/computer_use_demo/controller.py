from __future__ import annotations

import asyncio
import os
import re
from typing import Any

import docker
from docker.errors import NotFound
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
        return {"runtime_id": existing.id, "runtime_host": name, "created": False}
    except NotFound:
        pass

    labels = {
        "computer-use.managed": "true",
        "computer-use.session-id": payload.session_id,
    }
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
            "ANTHROPIC_MODEL": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
            "API_PROVIDER": os.getenv("API_PROVIDER", "anthropic"),
            "AGENT_MODE": os.getenv("AGENT_MODE", "mock"),
        },
        mem_limit=os.getenv("CAPSULE_MEMORY_LIMIT", "2g"),
        nano_cpus=int(float(os.getenv("CAPSULE_CPU_LIMIT", "1.5")) * 1_000_000_000),
        pids_limit=int(os.getenv("CAPSULE_PIDS_LIMIT", "512")),
        cap_drop=["ALL"],
        security_opt=["no-new-privileges:true"],
        shm_size="512m",
    )
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
