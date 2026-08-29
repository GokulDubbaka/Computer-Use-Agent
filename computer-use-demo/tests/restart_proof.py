from __future__ import annotations

import http.client
import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = "http://127.0.0.1:8080"
COMPOSE_FILE = Path(__file__).parents[1] / "compose.yaml"


def call(method: str, path: str, body: dict | None = None):
    request = urllib.request.Request(
        BASE_URL + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def main() -> None:
    session_id = ""
    checks: dict[str, bool] = {}
    try:
        session = call("POST", "/api/sessions", {"title": "restart proof"})
        session_id = session["id"]
        runtime_id = session["runtime_id"]
        message = call(
            "POST",
            f"/api/sessions/{session_id}/messages",
            {"role": "user", "content": "persist across API restart"},
        )
        restart = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "restart", "api"],
            capture_output=True,
            text=True,
            check=False,
        )
        checks["api_restarted"] = restart.returncode == 0
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                checks["ready_after_restart"] = call("GET", "/ready") == {
                    "status": "ready"
                }
                if checks["ready_after_restart"]:
                    break
            except (
                urllib.error.URLError,
                http.client.RemoteDisconnected,
                ConnectionError,
                TimeoutError,
            ):
                pass
            time.sleep(1)
        detail = call("GET", f"/api/sessions/{session_id}")
        checks["same_runtime"] = detail["runtime_id"] == runtime_id
        checks["message_persisted"] = any(
            item["id"] == message["id"] and item["content"] == "persist across API restart"
            for item in detail["messages"]
        )
        checks["events_persisted"] = bool(detail["events"])
        result = {"checks": checks, "passed": all(checks.values()), "session_id": session_id}
        print(json.dumps(result, indent=2))
        if not result["passed"]:
            raise SystemExit(1)
    finally:
        if session_id:
            try:
                call("DELETE", f"/api/sessions/{session_id}")
            except (
                urllib.error.HTTPError,
                urllib.error.URLError,
                http.client.RemoteDisconnected,
                ConnectionError,
            ):
                pass


if __name__ == "__main__":
    main()
