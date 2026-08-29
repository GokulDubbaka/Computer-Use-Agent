Gokul Dubbaka

# Concurrent Computer-Use Agent Platform

A FastAPI application that runs independent Anthropic computer-use sessions concurrently. Each session owns its messages, event sequence, run state, Firefox desktop, VNC stream, and disposable runtime. A slow or failed session does not block another session from starting.

## What this demonstrates

- Dynamic one-capsule-per-session provisioning; no fixed worker or display count.
- Real parallel execution in separate Firefox desktops.
- PostgreSQL-backed sessions, messages, runs, leases, and ordered events.
- Live progress over Server-Sent Events (SSE).
- Browser-accessible noVNC desktops protected by short-lived session tickets.
- Idempotent run submission and fenced worker completion.
- Recovery of stopped capsules and expired worker leases.
- Live Anthropic computer-use execution.
- A basic HTML and JavaScript interface.

## Verified concurrency

Two fresh weather-search sessions produced the following evidence:

| Evidence | Tokyo | New York |
|---|---:|---:|
| Run started | 18:05:57.196806 UTC | 18:05:57.217459 UTC |
| Run finished | 18:06:35.905870 UTC | 18:07:26.823813 UTC |
| Runtime | Separate capsule | Separate capsule |

The runs started **0.021 seconds apart** and overlapped for **38.69 seconds**. The second request did not wait for the first. A separate warm-browser task completed in **16.33 seconds**.

Google may present a CAPTCHA when it detects automated traffic. A CAPTCHA is an external website response, not a platform concurrency, isolation, or execution failure.

## Architecture

```text
Browser UI
   |
   | HTTP + SSE + ticketed WebSocket
   v
FastAPI API ---------------- PostgreSQL
   |                         durable session state
   v
Runtime Controller -------- Docker Engine
                              |-- capsule(session A): worker + Firefox + VNC
                              |-- capsule(session B): worker + Firefox + VNC
                              `-- capsule(session N): worker + Firefox + VNC
```

The API is the control plane. PostgreSQL is the durable source of truth. The controller dynamically creates a uniquely named capsule for each session. Each capsule worker polls only its own session, renews its fenced lease, emits ordered events, and reports completion through authenticated internal endpoints.

## Session isolation

Every session receives:

- unique session, run, runtime, worker, message, and event identities;
- a separate container and process tree;
- a separate X11 namespace, Openbox instance, Firefox process, profile, VNC server, and noVNC endpoint;
- a session-scoped runtime token stored as a hash;
- a signed, short-lived desktop ticket bound to one session;
- a database-enforced active-run constraint and monotonic event sequence.

An enterprise deployment serving mutually untrusted tenants should strengthen the isolation boundary with gVisor, Kata Containers, or Firecracker.

## Concurrency correctness

### Idempotent admission

`POST /api/sessions/{session_id}/runs` accepts an `Idempotency-Key`. Repeating the same request returns the existing run. PostgreSQL serializes admission so one session cannot accidentally acquire two active runs.

### Ordered events

Events are appended transactionally with a per-session sequence. SSE consumers receive durable history followed by live updates, so reconnecting does not mix another session's events.

### Lease fencing

A worker claims a run with a lease version, renews it through heartbeats, and must present the same version when completing. If ownership expires, the old worker is rejected instead of being allowed to overwrite the current state. This is a race-condition safeguard, not a retry mechanism.

## Prerequisites

- Docker Desktop, Docker Engine, or another Docker-compatible Linux-container runtime
- Docker Compose v2
- At least 4 GB of available memory for a two-session demonstration
- An Anthropic API key
- Optional `ANTHROPIC_WORKSPACE_ID` for an identity-linked key

## Configuration

### macOS or Linux

These commands are provided for portability but have not been tested in this implementation environment.

```bash
cp computer-use-demo/.env.example computer-use-demo/.env
${EDITOR:-vi} computer-use-demo/.env
```

### Windows PowerShell

```powershell
Copy-Item "computer-use-demo\.env.example" "computer-use-demo\.env"
notepad "computer-use-demo\.env"
```

Set:

```dotenv
ANTHROPIC_API_KEY=your-key
ANTHROPIC_WORKSPACE_ID=your-workspace-id-if-required
ANTHROPIC_MODEL=claude-haiku-4-5-20251001
POSTGRES_PASSWORD=replace-this-local-password
INTERNAL_TOKEN=replace-this-with-a-long-random-value
TICKET_SECRET=replace-this-with-another-long-random-value
SESSION_TTL_SECONDS=3600
```

`.env` is excluded from source control. Never commit credentials.

## Run locally

The Docker Compose commands are identical on macOS, Linux, and Windows:

```bash
docker compose -f computer-use-demo/compose.yaml up --build -d
docker compose -f computer-use-demo/compose.yaml ps
```

Check readiness:

```bash
curl http://127.0.0.1:8080/ready
```

PowerShell equivalent:

```powershell
Invoke-RestMethod "http://127.0.0.1:8080/ready"
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080).

Stop the stack:

```bash
docker compose -f computer-use-demo/compose.yaml down
```

## Acceptance verification

The challenge is verified through two observable cases:

| Case | Action | Pass condition |
|---|---|---|
| Single session | Submit `Search the weather in Dubai` | Firefox performs the search, progress streams live, the answer is stored, and the session accepts another task |
| Concurrent sessions | Submit Tokyo and New York weather searches from two side-by-side windows | Both runs start before either finishes, both Firefox desktops are visible simultaneously, and all messages, events, and runtime identifiers remain session-scoped |

The controller derives capsule identity from the session and contains no fixed concurrency count or fixed display allocation.

## Automated validation

```bash
python -m ruff check computer-use-demo
python -m pyright computer-use-demo
python -m pytest \
  computer-use-demo/tests/controller_test.py \
  computer-use-demo/tests/loop_test.py \
  computer-use-demo/tests/security_test.py -q
```

Linux-specific inherited tool tests should run inside the Linux image because they use Unix process and path semantics.

## API surface

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Process liveness |
| `GET` | `/ready` | Database and controller readiness |
| `GET/POST` | `/api/sessions` | List or create sessions |
| `GET/DELETE` | `/api/sessions/{id}` | Inspect or end a session |
| `GET/POST` | `/api/sessions/{id}/messages` | Read or append messages |
| `POST` | `/api/sessions/{id}/runs` | Idempotently submit a run |
| `GET` | `/api/sessions/{id}/events` | Read ordered event history |
| `GET` | `/api/sessions/{id}/stream` | Stream events over SSE |
| `POST` | `/api/sessions/{id}/desktop-ticket` | Issue scoped VNC access |

Internal runtime endpoints require a session-specific token and are not public worker APIs.

## Recovery and self-healing

1. `/ready` verifies control-plane dependencies before work is accepted.
2. Runtime reconciliation reuses a healthy capsule, replaces an exited capsule, and resolves concurrent creation conflicts.
3. Supervisord starts and restarts TigerVNC, Openbox, noVNC, Firefox, and the capsule worker.
4. Lease fencing rejects stale completion after missed heartbeats.

```bash
docker compose -f computer-use-demo/compose.yaml ps
docker compose -f computer-use-demo/compose.yaml logs --tail 200 api controller postgres
docker ps --filter name=capsule-
```

## Production evolution

```text
                         Global load balancer + WAF
                                   |
                    Regional FastAPI control planes
                                   |
             HA PostgreSQL + outbox + Kafka/NATS + Temporal
                                   |
          Admission + fair queues + placement + warm worker pool
                                   |
                  KEDA scaling + Karpenter capacity
                                   |
          One gVisor/Kata/Firecracker sandbox per session
                                   |
       Object storage + workload identity + audit + OpenTelemetry
```

The production control plane admits work durably, applies tenant quotas and backpressure, and places each admitted session into an exclusive sandbox. Warm capacity absorbs bursts; KEDA scales from queue depth and oldest-message age, while Karpenter supplies nodes. Reconciliation repairs missing or unhealthy runtimes, circuit breakers isolate failing dependencies, and multi-region routing shifts traffic only to healthy regions.

# ── Gokul Dubbaka ──────────────────────────────────────────────────────────
