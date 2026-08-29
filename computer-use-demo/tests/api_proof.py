from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

BASE_URL = "http://127.0.0.1:8080"


def call(method: str, path: str, body: dict | None = None, headers: dict | None = None):
    request = urllib.request.Request(
        BASE_URL + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", **(headers or {})},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read()
        return response.status, json.loads(payload) if payload else None


def resume_sse(path: str, last_event_id: int) -> dict:
    request = urllib.request.Request(
        BASE_URL + path,
        headers={"Accept": "text/event-stream", "Last-Event-ID": str(last_event_id)},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        for _ in range(30):
            line = response.readline().decode().strip()
            if line.startswith("data: "):
                return json.loads(line.removeprefix("data: "))
    raise RuntimeError("SSE did not replay an event")


def main() -> None:
    session_id = ""
    checks: dict[str, bool] = {}
    try:
        checks["health"] = call("GET", "/health")[1] == {"status": "ok"}
        checks["ready"] = call("GET", "/ready")[1] == {"status": "ready"}
        _, session = call("POST", "/api/sessions", {"title": "API proof"})
        session_id = session["id"]
        headers = {"Idempotency-Key": "stable-proof-key"}
        first_status, first = call(
            "POST",
            f"/api/sessions/{session_id}/runs",
            {"prompt": "idempotency marker"},
            headers,
        )
        second_status, second = call(
            "POST",
            f"/api/sessions/{session_id}/runs",
            {"prompt": "idempotency marker"},
            headers,
        )
        checks["idempotent_run"] = (
            first_status == 201 and second_status == 200 and first["id"] == second["id"]
        )
        try:
            call(
                "POST",
                f"/api/sessions/{session_id}/runs",
                {"prompt": "must conflict"},
                {"Idempotency-Key": "different-key"},
            )
            checks["one_active_run"] = False
        except urllib.error.HTTPError as exc:
            checks["one_active_run"] = exc.code == 409

        deadline = time.time() + 45
        detail = {}
        while time.time() < deadline:
            _, detail = call("GET", f"/api/sessions/{session_id}")
            if detail["active_run"] is None:
                break
            time.sleep(0.5)
        _, events = call("GET", f"/api/sessions/{session_id}/events?after=2")
        checks["event_replay"] = bool(events["items"]) and all(
            event["sequence"] > 2 for event in events["items"]
        )
        resumed = resume_sse(f"/api/sessions/{session_id}/stream", 2)
        checks["sse_last_event_id"] = resumed["sequence"] > 2
        checks["history_persisted"] = any(
            message["role"] == "assistant" for message in detail["messages"]
        )
        result = {"checks": checks, "passed": all(checks.values()), "session_id": session_id}
        print(json.dumps(result, indent=2))
        if not result["passed"]:
            raise SystemExit(1)
    finally:
        if session_id:
            try:
                call("DELETE", f"/api/sessions/{session_id}")
            except urllib.error.HTTPError:
                pass


if __name__ == "__main__":
    main()
