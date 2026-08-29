from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


def request(base_url: str, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if method == "POST":
        headers["Idempotency-Key"] = str(uuid.uuid4())
    req = urllib.request.Request(base_url + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=90) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--sessions", type=int, default=2)
    args = parser.parse_args()
    started = time.perf_counter()
    sessions: list[dict[str, Any]] = []
    report: dict[str, Any] = {"requested_sessions": args.sessions, "checks": {}}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.sessions) as pool:
            sessions = list(
                pool.map(
                    lambda index: request(
                        args.base_url,
                        "POST",
                        "/api/sessions",
                        {"title": f"proof-{index}", "metadata": {"marker": f"marker-{index}"}},
                    ),
                    range(args.sessions),
                )
            )
        report["sessions"] = [
            {"id": session["id"], "runtime_id": session["runtime_id"]}
            for session in sessions
        ]
        runtime_ids = {session["runtime_id"] for session in sessions}
        report["checks"]["unique_runtime_per_session"] = len(runtime_ids) == args.sessions

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.sessions) as pool:
            runs = list(
                pool.map(
                    lambda pair: request(
                        args.base_url,
                        "POST",
                        f"/api/sessions/{pair[1]['id']}/runs",
                        {"prompt": f"display marker-{pair[0]}"},
                    ),
                    enumerate(sessions),
                )
            )
        report["runs"] = [run["id"] for run in runs]

        deadline = time.time() + 45
        details: list[dict[str, Any]] = []
        while time.time() < deadline:
            details = [
                request(args.base_url, "GET", f"/api/sessions/{session['id']}")
                for session in sessions
            ]
            if all(detail["active_run"] is None for detail in details):
                break
            time.sleep(0.5)

        intervals: list[tuple[datetime, datetime]] = []
        workers: set[str] = set()
        ordered = True
        isolated = True
        for index, detail in enumerate(details):
            events = detail["events"]
            sequences = [event["sequence"] for event in events]
            ordered = ordered and sequences == list(range(1, len(sequences) + 1))
            begin = next(event for event in events if event["event_type"] == "proof.execution_started")
            end = next(event for event in events if event["event_type"] == "proof.execution_finished")
            intervals.append(
                (
                    datetime.fromisoformat(begin["created_at"]),
                    datetime.fromisoformat(end["created_at"]),
                )
            )
            workers.add(begin["payload"]["worker_id"])
            own_marker = f"marker-{index}"
            other_markers = {f"marker-{other}" for other in range(args.sessions) if other != index}
            serialized = json.dumps(detail)
            isolated = isolated and own_marker in serialized and not any(
                marker in serialized for marker in other_markers
            )

        overlap = all(
            start_a < end_b and start_b < end_a
            for pos, (start_a, end_a) in enumerate(intervals)
            for start_b, end_b in intervals[pos + 1 :]
        )
        report["checks"].update(
            {
                "different_workers": len(workers) == args.sessions,
                "strict_event_order": ordered,
                "parallel_interval_overlap": overlap,
                "session_data_isolation": isolated,
                "all_runs_completed": all(detail["status"] == "ready" for detail in details),
            }
        )
        report["workers"] = sorted(workers)
        report["intervals"] = [
            {"start": start.isoformat(), "end": end.isoformat()}
            for start, end in intervals
        ]
        report["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        artifact_dir = Path(__file__).parents[1] / "artifacts"
        artifact_dir.mkdir(exist_ok=True)
        screenshots: list[str] = []
        filesystem_isolated = True
        for index, session in enumerate(sessions):
            container = f"capsule-{session['id']}"
            page = subprocess.run(
                ["docker", "exec", container, "cat", "/home/computeruse/session-proof.html"],
                capture_output=True,
                text=True,
                check=False,
            )
            own_marker = f"marker-{index}"
            other_markers = {
                f"marker-{other}" for other in range(args.sessions) if other != index
            }
            filesystem_isolated = (
                filesystem_isolated
                and page.returncode == 0
                and own_marker in page.stdout
                and not any(marker in page.stdout for marker in other_markers)
            )
            remote = "/tmp/concurrency-proof.png"
            local = artifact_dir / f"desktop-{index + 1}.png"
            capture = subprocess.run(
                ["docker", "exec", container, "scrot", remote],
                capture_output=True,
                text=True,
                check=False,
            )
            copy = subprocess.run(
                ["docker", "cp", f"{container}:{remote}", str(local)],
                capture_output=True,
                text=True,
                check=False,
            )
            if capture.returncode == 0 and copy.returncode == 0 and local.exists():
                screenshots.append(local.name)
        report["screenshots"] = screenshots
        report["checks"]["desktop_screenshots"] = len(screenshots) == args.sessions
        report["checks"]["private_filesystem_markers"] = filesystem_isolated
        report["passed"] = all(report["checks"].values())
    finally:
        cleanup: dict[str, str] = {}
        for session in sessions:
            try:
                request(args.base_url, "DELETE", f"/api/sessions/{session['id']}")
                cleanup[session["id"]] = "deleted"
            except urllib.error.HTTPError as exc:
                cleanup[session["id"]] = f"HTTP {exc.code}"
        report["cleanup"] = cleanup
        artifact = Path(__file__).parents[1] / "artifacts" / "concurrency-proof.json"
        artifact.parent.mkdir(exist_ok=True)
        serialized = json.dumps(report, indent=2)
        artifact.write_text(serialized, encoding="utf-8")
        artifact.with_name(f"concurrency-proof-{args.sessions}.json").write_text(
            serialized, encoding="utf-8"
        )
        print(json.dumps(report, indent=2))
    if not report.get("passed"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
