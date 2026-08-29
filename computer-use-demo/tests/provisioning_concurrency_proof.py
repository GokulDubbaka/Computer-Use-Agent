from __future__ import annotations

import concurrent.futures
import json
import time
import urllib.error
import urllib.request
from typing import Any

BASE_URL = "http://127.0.0.1:8080"
SESSION_COUNT = 2


def call(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    request = urllib.request.Request(
        BASE_URL + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def create_session(index: int) -> dict[str, Any]:
    return call(
        "POST",
        "/api/sessions",
        {"title": f"provisioning proof {index}", "metadata": {"marker": index}},
    )


def main() -> None:
    sessions: list[dict[str, Any]] = []
    checks: dict[str, bool] = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=SESSION_COUNT) as pool:
            sessions = list(pool.map(create_session, range(SESSION_COUNT)))

        checks["all_sessions_ready"] = all(
            session["status"] == "ready" for session in sessions
        )
        runtime_ids = {session["runtime_id"] for session in sessions}
        checks["unique_runtime_per_session"] = len(runtime_ids) == SESSION_COUNT

        deadline = time.time() + 30
        details: list[dict[str, Any]] = []
        while time.time() < deadline:
            details = [
                call("GET", f"/api/sessions/{session['id']}") for session in sessions
            ]
            if all(
                any(
                    event["event_type"] == "runtime.worker_ready"
                    for event in detail["events"]
                )
                for detail in details
            ):
                break
            time.sleep(0.25)

        worker_ids = {
            event["payload"]["worker_id"]
            for detail in details
            for event in detail["events"]
            if event["event_type"] == "runtime.worker_ready"
        }
        checks["unique_worker_per_session"] = len(worker_ids) == SESSION_COUNT
        checks["session_data_isolated"] = all(
            detail["metadata"] == {"marker": index}
            and detail["runtime_id"] == sessions[index]["runtime_id"]
            for index, detail in enumerate(details)
        )

        result = {
            "checks": checks,
            "sessions": [session["id"] for session in sessions],
            "runtimes": sorted(runtime_ids),
            "workers": sorted(worker_ids),
            "passed": all(checks.values()),
        }
        print(json.dumps(result, indent=2))
        if not result["passed"]:
            raise SystemExit(1)
    finally:
        for session in sessions:
            try:
                call("DELETE", f"/api/sessions/{session['id']}")
            except urllib.error.HTTPError:
                pass


if __name__ == "__main__":
    main()
