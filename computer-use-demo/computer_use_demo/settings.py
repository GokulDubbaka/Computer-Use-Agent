from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://agent:agent@postgres:5432/agent"
    )
    controller_url: str = os.getenv("CONTROLLER_URL", "http://controller:9000")
    internal_token: str = os.getenv("INTERNAL_TOKEN", "local-development-only")
    ticket_secret: str = os.getenv("TICKET_SECRET", "local-development-ticket-secret")
    public_base_url: str = os.getenv("PUBLIC_BASE_URL", "http://localhost:8080")
    capsule_image: str = os.getenv("CAPSULE_IMAGE", "computer-use-capsule:local")
    capsule_network: str = os.getenv("CAPSULE_NETWORK", "computer-use-network")
    session_ttl_seconds: int = int(os.getenv("SESSION_TTL_SECONDS", "3600"))


settings = Settings()

