CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    title TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'provisioning', 'ready', 'running', 'error', 'terminating', 'terminated'
    )),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    runtime_id TEXT UNIQUE,
    runtime_host TEXT,
    runtime_token_hash TEXT,
    active_run_id TEXT,
    next_event_sequence BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '1 hour'
);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    prompt TEXT NOT NULL,
    model TEXT,
    status TEXT NOT NULL CHECK (status IN (
        'queued', 'running', 'completed', 'failed', 'cancelled', 'interrupted'
    )),
    worker_id TEXT,
    lease_version BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, idempotency_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS runs_one_active_per_session
ON runs(session_id)
WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS runs_claim_queue
ON runs(status, created_at)
WHERE status = 'queued';

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,
    sequence BIGINT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, sequence)
);

CREATE TABLE IF NOT EXISTS events (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    run_id TEXT REFERENCES runs(id) ON DELETE SET NULL,
    runtime_id TEXT,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, sequence)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'sessions_active_run_fk'
    ) THEN
        ALTER TABLE sessions
            ADD CONSTRAINT sessions_active_run_fk
            FOREIGN KEY (active_run_id) REFERENCES runs(id) ON DELETE SET NULL;
    END IF;
END $$;
