"""
API key authentication for the Telco Fallout Agent.

A request is authorized when it presents the `X-API-Key` header with a value
that matches one of the keys configured via the `TELCO_AGENT_API_KEYS`
environment variable (comma-separated).

Why a list of keys, not just one?
  - Lets you give different consumers their own key (e.g. each upstream BSS).
  - Rotation: add a new key, update consumers, remove the old one.

Loaded once at process start. The Secret backing this var is mounted as an
env from `telco-agent-secrets` (per-env in K8s, see deploy-aks.yml).
"""

import os
import secrets

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

API_KEY_HEADER = "X-API-Key"

_RAW = os.getenv("TELCO_AGENT_API_KEYS", "")
ALLOWED_KEYS: set[str] = {k.strip() for k in _RAW.split(",") if k.strip()}

_api_key_scheme = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def require_api_key(api_key: str | None = Security(_api_key_scheme)) -> str:
    """
    FastAPI dependency. Raises 401 if the request's API key is missing or
    invalid. Compared in constant time to thwart timing attacks (paranoid
    given the small key space, but free).
    """
    if not ALLOWED_KEYS:
        # Fail closed: refuse to serve protected endpoints if no keys are
        # configured, rather than silently letting everything through.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API key auth is not configured on the server.",
        )

    if api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Missing {API_KEY_HEADER} header.",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )

    if not any(secrets.compare_digest(api_key, valid) for valid in ALLOWED_KEYS):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )

    return api_key
