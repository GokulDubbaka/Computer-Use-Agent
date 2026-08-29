from __future__ import annotations

import asyncio
import os
import socket
import time
from pathlib import Path
from typing import Any

import httpx

from .loop import APIProvider, sampling_loop
from .tools import ToolResult

SESSION_ID = os.environ["SESSION_ID"]
RUNTIME_TOKEN = os.environ["RUNTIME_TOKEN"]
CONTROL_PLANE_URL = os.getenv("CONTROL_PLANE_URL", "http://api:8000")
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
HEADERS = {"X-Runtime-Token": RUNTIME_TOKEN, "X-Worker-Id": WORKER_ID}
AGENT_MODE = os.getenv("AGENT_MODE", "mock")


def _block_payload(block: Any) -> dict[str, Any]:
    if isinstance(block, dict):
        return block
    if hasattr(block, "model_dump"):
        return block.model_dump()
    return {"value": str(block)}


async def _post_event(
    client: httpx.AsyncClient,
    event_type: str,
    payload: dict[str, Any],
    run_id: str | None = None,
) -> None:
    for attempt in range(6):
        try:
            response = await client.post(
                f"/internal/runtime/{SESSION_ID}/events",
                headers=HEADERS,
                json={"event_type": event_type, "run_id": run_id, "payload": payload},
            )
            response.raise_for_status()
            return
        except (httpx.HTTPError, OSError):
            if attempt == 5:
                raise
            await asyncio.sleep(min(2**attempt, 10))


async def _heartbeat(client: httpx.AsyncClient, run_id: str, lease_version: int) -> None:
    while True:
        await asyncio.sleep(20)
        response = await client.post(
            f"/internal/runtime/{SESSION_ID}/runs/{run_id}/heartbeat",
            headers={**HEADERS, "X-Lease-Version": str(lease_version)},
        )
        response.raise_for_status()
        if not response.json()["renewed"]:
            return


async def _execute(client: httpx.AsyncClient, run: dict[str, Any]) -> None:
    run_id = run["id"]
    lease_version = int(run["lease_version"])
    assistant_text: list[str] = []
    api_failures: list[Exception] = []
    event_queue: asyncio.Queue[tuple[str, dict[str, Any]] | None] = asyncio.Queue()
    event_failures: list[Exception] = []

    async def publish_events() -> None:
        while True:
            item = await event_queue.get()
            try:
                if item is None:
                    return
                event_type, payload = item
                await _post_event(client, event_type, payload, run_id)
            except Exception as exc:
                event_failures.append(exc)
            finally:
                event_queue.task_done()

    def emit(event_type: str, payload: dict[str, Any]) -> None:
        event_queue.put_nowait((event_type, payload))

    def output_callback(block: Any) -> None:
        payload = _block_payload(block)
        if payload.get("type") == "text" and payload.get("text"):
            assistant_text.append(str(payload["text"]))
        emit("agent.output", {"block": payload})

    def tool_output_callback(result: ToolResult, tool_use_id: str) -> None:
        emit(
            "agent.tool_result",
            {
                "tool_use_id": tool_use_id,
                "output": result.output,
                "error": result.error,
                "has_image": bool(result.base64_image),
            },
        )

    def api_response_callback(request: Any, response: Any, error: Exception | None) -> None:
        if error is not None:
            api_failures.append(error)
        emit(
            "agent.api_response",
            {
                "status_code": getattr(response, "status_code", None),
                "error": str(error) if error else None,
            },
        )

    heartbeat = asyncio.create_task(_heartbeat(client, run_id, lease_version))
    publisher = asyncio.create_task(publish_events())
    try:
        if AGENT_MODE == "mock":
            await _post_event(
                client,
                "proof.execution_started",
                {"worker_id": WORKER_ID, "monotonic_ns": time.monotonic_ns()},
                run_id,
            )
            page = (
                f"<html><body style='font:32px sans-serif;background:#111;color:#eee'>"
                f"<h1>Isolated session</h1><p>{SESSION_ID}</p>"
                f"<p>{run['prompt']}</p></body></html>"
            )
            proof_page = Path.home() / "session-proof.html"
            proof_page.write_text(page, encoding="utf-8")
            process = await asyncio.create_subprocess_exec(
                "firefox-esr",
                "--no-remote",
                proof_page.as_uri(),
                env={**os.environ, "DISPLAY": ":1"},
            )
            await _post_event(client, "agent.output", {"block": {"type": "text", "text": "Mock task running"}}, run_id)
            await asyncio.sleep(float(os.getenv("MOCK_RUN_SECONDS", "5")))
            await _post_event(
                client,
                "proof.execution_finished",
                {"worker_id": WORKER_ID, "pid": process.pid, "monotonic_ns": time.monotonic_ns()},
                run_id,
            )
            response = await client.post(
                f"/internal/runtime/{SESSION_ID}/runs/{run_id}/finish",
                headers=HEADERS,
                json={
                    "status": "completed",
                    "lease_version": lease_version,
                    "result": {"assistant_text": f"Completed isolated mock task in {SESSION_ID}"},
                },
            )
            response.raise_for_status()
            return
        provider = APIProvider(os.getenv("API_PROVIDER", "anthropic"))
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        workspace_id = os.getenv("ANTHROPIC_WORKSPACE_ID") or None
        if provider == APIProvider.ANTHROPIC and not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is required for live execution")
        model = run.get("model") or os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        tool_version = (
            "computer_use_20250124"
            if "haiku-4-5" in model
            else "computer_use_20251124"
        )
        await sampling_loop(
            model=model,
            provider=provider,
            system_prompt_suffix=f" This desktop belongs only to session {SESSION_ID}.",
            messages=[{"role": "user", "content": run["prompt"]}],
            output_callback=output_callback,
            tool_output_callback=tool_output_callback,
            api_response_callback=api_response_callback,
            api_key=api_key,
            workspace_id=workspace_id,
            only_n_most_recent_images=3,
            max_tokens=16_384,
            tool_version=tool_version,
            thinking_mode="off",
        )
        await event_queue.join()
        if event_failures:
            raise event_failures[0]
        if api_failures:
            raise api_failures[0]
        response = await client.post(
            f"/internal/runtime/{SESSION_ID}/runs/{run_id}/finish",
            headers=HEADERS,
            json={
                "status": "completed",
                "lease_version": lease_version,
                "result": {"assistant_text": "\n".join(assistant_text)},
            },
        )
        response.raise_for_status()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        await event_queue.join()
        await _post_event(client, "run.failed", {"error": str(exc)}, run_id)
        response = await client.post(
            f"/internal/runtime/{SESSION_ID}/runs/{run_id}/finish",
            headers=HEADERS,
            json={
                "status": "failed",
                "lease_version": lease_version,
                "error_text": str(exc),
                "result": {},
            },
        )
        response.raise_for_status()
    finally:
        await event_queue.put(None)
        await event_queue.join()
        await asyncio.gather(publisher, return_exceptions=True)
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)


async def main() -> None:
    async with httpx.AsyncClient(base_url=CONTROL_PLANE_URL, timeout=60) as client:
        await _post_event(client, "runtime.worker_ready", {"worker_id": WORKER_ID})
        while True:
            try:
                response = await client.post(
                    f"/internal/runtime/{SESSION_ID}/next-run", headers=HEADERS
                )
                if response.status_code == 204:
                    await asyncio.sleep(1)
                    continue
                response.raise_for_status()
                await _execute(client, response.json())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                try:
                    await _post_event(client, "runtime.worker_error", {"error": str(exc)})
                except Exception:
                    pass
                await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
