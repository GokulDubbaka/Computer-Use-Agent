from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def _parse_json(value: str | None) -> Any:
    if value is None or value == "":
        return None
    return json.loads(value)


@dataclass(frozen=True)
class SessionStoreConfig:
    db_path: Path
    lease_seconds: int = 60


class SessionStore:
    def __init__(self, config: SessionStoreConfig):
        self.config = config
        self._init_lock = threading.Lock()
        self._initialized = False
        self._ensure_initialized()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.config.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            self.config.db_path.parent.mkdir(parents=True, exist_ok=True)
            with closing(self._connect()) as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS sessions (
                        id TEXT PRIMARY KEY,
                        title TEXT,
                        status TEXT NOT NULL,
                        metadata_json TEXT NOT NULL DEFAULT '{}',
                        active_run_id TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS runs (
                        id TEXT PRIMARY KEY,
                        session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                        request_id TEXT NOT NULL UNIQUE,
                        worker_id TEXT,
                        status TEXT NOT NULL,
                        lease_version INTEGER NOT NULL DEFAULT 0,
                        lease_expires_at TEXT,
                        result_json TEXT,
                        error_text TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active_per_session
                    ON runs(session_id)
                    WHERE status IN ('queued', 'running');

                    CREATE TABLE IF NOT EXISTS session_events (
                        session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                        sequence INTEGER NOT NULL,
                        event_type TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (session_id, sequence)
                    );

                    CREATE TABLE IF NOT EXISTS chat_messages (
                        id TEXT PRIMARY KEY,
                        session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                        role TEXT NOT NULL,
                        content TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS idempotency_keys (
                        scope TEXT NOT NULL,
                        key TEXT NOT NULL,
                        response_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (scope, key)
                    );
                    """
                )
            self._initialized = True

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_session(self, *, title: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        session_id = f"sess_{uuid.uuid4().hex}"
        now = _utc_now()
        metadata_json = _json(metadata or {})
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sessions (id, title, status, metadata_json, active_run_id, created_at, updated_at)
                VALUES (?, ?, 'idle', ?, NULL, ?, ?)
                """,
                (session_id, title, metadata_json, now, now),
            )
        return self.get_session(session_id)  # type: ignore[return-value]

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM sessions WHERE id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_session(row)

    def list_sessions(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM sessions
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_session(row) for row in rows]

    def delete_session(self, session_id: str) -> bool:
        with self.transaction() as conn:
            deleted = conn.execute(
                "DELETE FROM sessions WHERE id = ? AND active_run_id IS NULL",
                (session_id,),
            ).rowcount
        return deleted == 1

    def recover_incomplete_runs(self) -> int:
        """Fail work that cannot survive a restart of this local worker."""
        now = _utc_now()
        with self.transaction() as conn:
            active = conn.execute(
                "SELECT id, session_id FROM runs WHERE status IN ('queued', 'running')"
            ).fetchall()
            if not active:
                return 0
            conn.execute(
                """
                UPDATE runs
                SET status = 'failed', error_text = 'worker restarted', updated_at = ?
                WHERE status IN ('queued', 'running')
                """,
                (now,),
            )
            session_ids = {row["session_id"] for row in active}
            conn.executemany(
                """
                UPDATE sessions
                SET status = 'error', active_run_id = NULL, updated_at = ?
                WHERE id = ?
                """,
                [(now, session_id) for session_id in session_ids],
            )
            return len(active)

    def append_event(self, session_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = _utc_now()
        with self.transaction() as conn:
            next_sequence = conn.execute(
                """
                SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence
                FROM session_events
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()["next_sequence"]
            conn.execute(
                """
                INSERT INTO session_events (session_id, sequence, event_type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, next_sequence, event_type, _json(payload), now),
            )
            conn.execute(
                "UPDATE sessions SET updated_at = ? WHERE id = ?",
                (now, session_id),
            )
        return {
            "session_id": session_id,
            "sequence": next_sequence,
            "event_type": event_type,
            "payload": payload,
            "created_at": now,
        }

    def list_events(self, session_id: str, *, after_sequence: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM session_events
                WHERE session_id = ? AND sequence > ?
                ORDER BY sequence ASC
                LIMIT ?
                """,
                (session_id, after_sequence, limit),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def create_message(self, session_id: str, *, role: str, content: str) -> dict[str, Any]:
        message_id = f"msg_{uuid.uuid4().hex}"
        now = _utc_now()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO chat_messages (id, session_id, role, content, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (message_id, session_id, role, content, now),
            )
            conn.execute(
                "UPDATE sessions SET updated_at = ? WHERE id = ?",
                (now, session_id),
            )
        return {
            "id": message_id,
            "session_id": session_id,
            "role": role,
            "content": content,
            "created_at": now,
        }

    def list_messages(self, session_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM chat_messages
                WHERE session_id = ?
                ORDER BY created_at ASC
                """,
                (session_id,),
            ).fetchall()
        return [self._row_to_message(row) for row in rows]

    def create_run(
        self,
        *,
        session_id: str,
        request_id: str,
        worker_id: str | None = None,
        lease_seconds: int | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically create a run, or return the prior idempotent result."""
        now = _utc_now()
        lease_ttl = lease_seconds or self.config.lease_seconds
        lease_expires_at = datetime.now(UTC) + timedelta(seconds=lease_ttl)

        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM runs WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                if existing["session_id"] != session_id:
                    raise ValueError("idempotency key belongs to another session")
                return self._row_to_run(existing), False

            run_id = f"run_{uuid.uuid4().hex}"
            conn.execute(
                """
                INSERT INTO runs (
                    id, session_id, request_id, worker_id, status,
                    lease_version, lease_expires_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'queued', 0, ?, ?, ?)
                """,
                (
                    run_id,
                    session_id,
                    request_id,
                    worker_id,
                    lease_expires_at.isoformat(),
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                UPDATE sessions
                SET status = 'running', active_run_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (run_id, now, session_id),
            )
            created = conn.execute(
                "SELECT * FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            assert created is not None
            return self._row_to_run(created), True

    def claim_run(self, run_id: str, *, worker_id: str) -> dict[str, Any] | None:
        now = _utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if row is None or row["status"] != "queued":
                return None
            lease_version = int(row["lease_version"]) + 1
            lease_expires_at = datetime.now(UTC) + timedelta(seconds=self.config.lease_seconds)
            conn.execute(
                """
                UPDATE runs
                SET status = 'running',
                    worker_id = ?,
                    lease_version = ?,
                    lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    worker_id,
                    lease_version,
                    lease_expires_at.isoformat(),
                    now,
                    run_id,
                ),
            )
        return self.get_run(run_id)

    def finish_run(
        self,
        run_id: str,
        *,
        status: str = "completed",
        result: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> dict[str, Any] | None:
        now = _utc_now()
        with self.transaction() as conn:
            run = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
            if run is None:
                return None
            conn.execute(
                """
                UPDATE runs
                SET status = ?, result_json = ?, error_text = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, _json(result or {}), error_text, now, run_id),
            )
            conn.execute(
                """
                UPDATE sessions
                SET status = CASE WHEN ? = 'failed' THEN 'error' ELSE 'idle' END,
                    active_run_id = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (status, now, run["session_id"]),
            )
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_run(row)

    def get_active_run(self, session_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT *
                FROM runs
                WHERE session_id = ? AND status IN ('queued', 'running')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (session_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_run(row)

    def _row_to_session(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "title": row["title"],
            "status": row["status"],
            "metadata": _parse_json(row["metadata_json"]) or {},
            "active_run_id": row["active_run_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _row_to_run(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "session_id": row["session_id"],
            "request_id": row["request_id"],
            "worker_id": row["worker_id"],
            "status": row["status"],
            "lease_version": row["lease_version"],
            "lease_expires_at": row["lease_expires_at"],
            "result": _parse_json(row["result_json"]) or {},
            "error_text": row["error_text"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _row_to_event(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "session_id": row["session_id"],
            "sequence": row["sequence"],
            "event_type": row["event_type"],
            "payload": _parse_json(row["payload_json"]) or {},
            "created_at": row["created_at"],
        }

    def _row_to_message(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "session_id": row["session_id"],
            "role": row["role"],
            "content": row["content"],
            "created_at": row["created_at"],
        }
