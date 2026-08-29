from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def create_ticket(session_id: str, secret: str, ttl_seconds: int = 120) -> str:
    payload = _b64(
        json.dumps(
            {"session_id": session_id, "expires_at": int(time.time()) + ttl_seconds},
            separators=(",", ":"),
        ).encode()
    )
    signature = _b64(hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{signature}"


def verify_ticket(ticket: str, secret: str, session_id: str) -> bool:
    try:
        payload, signature = ticket.split(".", 1)
        expected = _b64(hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(signature, expected):
            return False
        decoded: dict[str, Any] = json.loads(_unb64(payload))
        return decoded["session_id"] == session_id and int(decoded["expires_at"]) >= int(time.time())
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False

