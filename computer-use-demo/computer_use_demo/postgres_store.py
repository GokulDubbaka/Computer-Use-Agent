from __future__ import annotations

import hashlib
import json
import secrets
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, datetime) else value


def _row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: _iso(value) for key, value in row.items()}


@dataclass(frozen=True)
class PostgresStoreConfig:
    database_url: str
    lease_seconds: int = 60
    session_ttl_seconds: int = 3600


class ConflictError(RuntimeError):
    pass


class PostgresStore:
    """Synchronous transactional repository; FastAPI calls it through a thread."""

    def __init__(self, config: PostgresStoreConfig):
        self.config = config
        self._init_lock = threading.Lock()
        self._initialized = False

    def _connect(self) -> psycopg.Connection[dict[str, Any]]:
        return psycopg.connect(self.config.database_url, row_factory=dict_row)

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
            with self._connect() as conn:
                conn.execute(schema)
            self._initialized = True

    def ping(self) -> None:
        self.initialize()
        with self._connect() as conn:
            conn.execute("SELECT 1")

    def append_event(
        self,
        session_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        run_id: str | None = None,
        runtime_id: str | None = None,
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            sequence_row = conn.execute(
                """
                UPDATE sessions
                SET next_event_sequence = next_event_sequence + 1,
                    updated_at = now(), last_activity_at = now()
                WHERE id = %s
                RETURNING next_event_sequence - 1 AS sequence
                """,
                (session_id,),
            ).fetchone()
            if sequence_row is None:
                raise KeyError(session_id)
            event = conn.execute(
                """
                INSERT INTO events (
                    session_id, sequence, run_id, runtime_id, event_type, payload
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                RETURNING *
                """,
                (
                    session_id,
                    sequence_row["sequence"],
                    run_id,
                    runtime_id,
                    event_type,
                    _json(payload),
                ),
            ).fetchone()
            conn.execute("SELECT pg_notify('session_events', %s)", (session_id,))
        return _row(event) or {}

    def create_session(
        self, *, title: str | None = None, metadata: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], str]:
        self.initialize()
        session_id = _id("sess")
        runtime_token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(runtime_token.encode()).hexdigest()
        expires_at = datetime.now(UTC) + timedelta(seconds=self.config.session_ttl_seconds)
        with self._connect() as conn:
            session = conn.execute(
                """
                INSERT INTO sessions (
                    id, title, status, metadata, runtime_token_hash, expires_at
                ) VALUES (%s, %s, 'provisioning', %s::jsonb, %s, %s)
                RETURNING *
                """,
                (session_id, title, _json(metadata or {}), token_hash, expires_at),
            ).fetchone()
        return _row(session) or {}, runtime_token

    def authenticate_runtime(self, session_id: str, token: str) -> bool:
        self.initialize()
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        with self._connect() as conn:
            found = conn.execute(
                "SELECT 1 FROM sessions WHERE id = %s AND runtime_token_hash = %s",
                (session_id, token_hash),
            ).fetchone()
        return found is not None

    def attach_runtime(self, session_id: str, runtime_id: str, runtime_host: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            session = conn.execute(
                """
                UPDATE sessions
                SET runtime_id = %s, runtime_host = %s, status = 'ready', updated_at = now()
                WHERE id = %s AND status = 'provisioning'
                RETURNING *
                """,
                (runtime_id, runtime_host, session_id),
            ).fetchone()
        if session is None:
            raise ConflictError("session cannot accept a runtime")
        return _row(session) or {}

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM sessions WHERE id = %s AND status <> 'terminated'", (session_id,)
            ).fetchone()
        return _row(row)

    def list_sessions(self, *, limit: int = 50) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM sessions WHERE status <> 'terminated'
                ORDER BY created_at DESC LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [_row(row) or {} for row in rows]

    def create_message(
        self, session_id: str, *, role: str, content: Any, run_id: str | None = None
    ) -> dict[str, Any]:
        self.initialize()
        with self._connect() as conn:
            session = conn.execute(
                "SELECT id FROM sessions WHERE id = %s FOR UPDATE", (session_id,)
            ).fetchone()
            if session is None:
                raise KeyError(session_id)
            seq = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM messages WHERE session_id = %s",
                (session_id,),
            ).fetchone()["sequence"]
            message = conn.execute(
                """
                INSERT INTO messages (id, session_id, run_id, sequence, role, content)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                RETURNING *
                """,
                (_id("msg"), session_id, run_id, seq, role, _json(content)),
            ).fetchone()
            conn.execute(
                "UPDATE sessions SET updated_at = now(), last_activity_at = now() WHERE id = %s",
                (session_id,),
            )
        return _row(message) or {}

    def list_messages(self, session_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM messages WHERE session_id = %s ORDER BY sequence", (session_id,)
            ).fetchall()
        return [_row(row) or {} for row in rows]

    def create_run(
        self,
        *,
        session_id: str,
        idempotency_key: str,
        prompt: str,
        model: str | None,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically persist the prompt, run, command and events."""
        self.initialize()
        try:
            with self._connect() as conn:
                session = conn.execute(
                    "SELECT * FROM sessions WHERE id = %s FOR UPDATE", (session_id,)
                ).fetchone()
                if session is None:
                    raise KeyError(session_id)
                existing = conn.execute(
                    "SELECT * FROM runs WHERE session_id = %s AND idempotency_key = %s",
                    (session_id, idempotency_key),
                ).fetchone()
                if existing is not None:
                    return _row(existing) or {}, False
                if session["status"] not in ("ready", "running"):
                    raise ConflictError(f"session is {session['status']}")

                run_id = _id("run")
                run = conn.execute(
                    """
                    INSERT INTO runs (id, session_id, idempotency_key, prompt, model, status)
                    VALUES (%s, %s, %s, %s, %s, 'queued') RETURNING *
                    """,
                    (run_id, session_id, idempotency_key, prompt, model),
                ).fetchone()
                message_seq = conn.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM messages WHERE session_id = %s",
                    (session_id,),
                ).fetchone()["sequence"]
                conn.execute(
                    """
                    INSERT INTO messages (id, session_id, run_id, sequence, role, content)
                    VALUES (%s, %s, %s, %s, 'user', %s::jsonb)
                    """,
                    (_id("msg"), session_id, run_id, message_seq, _json(prompt)),
                )
                conn.execute(
                    """
                    INSERT INTO commands (id, session_id, run_id, command_type, payload)
                    VALUES (%s, %s, %s, 'execute_run', %s::jsonb)
                    """,
                    (_id("cmd"), session_id, run_id, _json({"run_id": run_id})),
                )
                conn.execute(
                    """
                    UPDATE sessions SET status = 'running', active_run_id = %s,
                        updated_at = now(), last_activity_at = now()
                    WHERE id = %s
                    """,
                    (run_id, session_id),
                )
            return _row(run) or {}, True
        except UniqueViolation as exc:
            raise ConflictError("session already has an active run") from exc

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id = %s", (run_id,)).fetchone()
        return _row(row)

    def get_active_run(self, session_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM runs WHERE session_id = %s AND status IN ('queued', 'running')
                ORDER BY created_at DESC LIMIT 1
                """,
                (session_id,),
            ).fetchone()
        return _row(row)

    def list_events(
        self, session_id: str, *, after_sequence: int = 0, limit: int = 100
    ) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM events WHERE session_id = %s AND sequence > %s
                ORDER BY sequence LIMIT %s
                """,
                (session_id, after_sequence, limit),
            ).fetchall()
        return [_row(row) or {} for row in rows]

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        result: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as conn:
            run = conn.execute("SELECT * FROM runs WHERE id = %s FOR UPDATE", (run_id,)).fetchone()
            if run is None:
                return None
            updated = conn.execute(
                """
                UPDATE runs SET status = %s, result = %s::jsonb, error_text = %s,
                    completed_at = now(), lease_expires_at = NULL, updated_at = now()
                WHERE id = %s RETURNING *
                """,
                (status, _json(result or {}), error_text, run_id),
            ).fetchone()
            session_status = "error" if status in ("failed", "interrupted") else "ready"
            conn.execute(
                """
                UPDATE sessions SET status = %s, active_run_id = NULL,
                    updated_at = now(), last_activity_at = now()
                WHERE id = %s AND active_run_id = %s
                """,
                (session_status, run["session_id"], run_id),
            )
        return _row(updated)

    def request_termination(self, session_id: str) -> bool:
        self.initialize()
        with self._connect() as conn:
            session = conn.execute(
                "SELECT * FROM sessions WHERE id = %s FOR UPDATE", (session_id,)
            ).fetchone()
            if session is None or session["status"] == "terminated":
                return False
            if session["active_run_id"]:
                raise ConflictError("cannot terminate a session with an active run")
            conn.execute(
                "UPDATE sessions SET status = 'terminating', updated_at = now() WHERE id = %s",
                (session_id,),
            )
            conn.execute(
                """
                INSERT INTO commands (id, session_id, command_type, payload)
                VALUES (%s, %s, 'destroy_runtime', '{}'::jsonb)
                """,
                (_id("cmd"), session_id),
            )
        return True
