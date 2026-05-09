"""
API security for the Telco Fallout Agent.

Phase 1-14: X-API-Key header authentication (retained for backward compat).
Phase 15:   JWT (HS256) access tokens + per-operator sliding-window rate limiting.

Auth methods (both work in parallel):
  X-API-Key header  — existing machine-to-machine auth (BSS integrations)
  Bearer JWT        — operator/dashboard auth; issued via POST /auth/token

JWT claims:
  sub       — operator_id
  opco_id   — optional, restricts which opco data the operator can see
  role      — operator | supervisor | admin
  exp       — UTC expiry

Rate limiting:
  60 requests / 60 seconds per operator_id (configurable via env vars).
  Returns HTTP 429 with Retry-After header when limit is breached.
  WebSocket connections pass token as ?token=<JWT> query param.

Environment variables:
  TELCO_AGENT_API_KEYS          — comma-separated static API keys (legacy)
  JWT_SECRET_KEY                — HS256 signing secret (required in prod)
  JWT_ALGORITHM                 — default "HS256"
  ACCESS_TOKEN_EXPIRE_MINUTES   — default 60
  AUTH_DEV_MODE                 — "true" accepts any user with DEV_PASSWORD
  DEV_PASSWORD                  — default "synapse-dev"
  RATE_LIMIT_REQUESTS           — default 60
  RATE_LIMIT_WINDOW_SECONDS     — default 60
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security, WebSocket, status
from fastapi.security import APIKeyHeader, OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── Legacy API-key auth (Phase 1-14) ──────────────────────────────────────────

API_KEY_HEADER  = "X-API-Key"
_RAW            = os.getenv("TELCO_AGENT_API_KEYS", "")
ALLOWED_KEYS: set[str] = {k.strip() for k in _RAW.split(",") if k.strip()}
_api_key_scheme = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def require_api_key(api_key: str | None = Security(_api_key_scheme)) -> str:
    """FastAPI dependency — X-API-Key header (machine-to-machine)."""
    if not ALLOWED_KEYS:
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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key.")
    return api_key


# ── JWT configuration (Phase 15) ──────────────────────────────────────────────

JWT_SECRET_KEY   = os.getenv("JWT_SECRET_KEY", "change-me-in-production-use-32+-char-secret")
JWT_ALGORITHM    = os.getenv("JWT_ALGORITHM", "HS256")
TOKEN_EXPIRE_MIN = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

pwd_context   = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token", auto_error=False)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int = TOKEN_EXPIRE_MIN * 60


class OperatorInfo(BaseModel):
    operator_id: str
    opco_id: Optional[str] = None
    role: str = "operator"


# ── Rate limiter (sliding window, in-process) ──────────────────────────────────

RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "60"))
RATE_LIMIT_WINDOW   = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

_request_log: dict[str, collections.deque] = {}
_rate_lock = asyncio.Lock()


async def check_rate_limit(operator_id: str) -> None:
    """Sliding-window rate limiter. Raises HTTP 429 when limit is breached."""
    now    = time.monotonic()
    cutoff = now - RATE_LIMIT_WINDOW
    async with _rate_lock:
        dq = _request_log.setdefault(operator_id, collections.deque())
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= RATE_LIMIT_REQUESTS:
            retry_after = int(RATE_LIMIT_WINDOW - (now - dq[0])) + 1
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(retry_after)},
            )
        dq.append(now)


# ── JWT helpers ────────────────────────────────────────────────────────────────

def _create_access_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRE_MIN)
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid or expired token: {exc}",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ── FastAPI dependencies ───────────────────────────────────────────────────────

async def get_current_operator(
    token: Optional[str] = Depends(oauth2_scheme),
) -> OperatorInfo:
    """Dependency: validates Bearer JWT, returns OperatorInfo. Raises 401 on failure."""
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    payload = _decode_token(token)
    operator_id = payload.get("sub")
    if not operator_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")
    await check_rate_limit(operator_id)
    return OperatorInfo(
        operator_id=operator_id,
        opco_id=payload.get("opco_id"),
        role=payload.get("role", "operator"),
    )


async def get_ws_operator(
    websocket: WebSocket,
    token: Optional[str] = Query(None),
) -> OperatorInfo:
    """
    WebSocket dependency: token passed as ?token=<JWT>.
    Closes socket with code 1008 on auth failure.
    """
    if not token:
        await websocket.close(code=1008, reason="Missing auth token")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    try:
        payload = _decode_token(token)
    except HTTPException:
        await websocket.close(code=1008, reason="Invalid auth token")
        raise
    operator_id = payload.get("sub")
    if not operator_id:
        await websocket.close(code=1008, reason="Invalid token payload")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bad token")
    return OperatorInfo(
        operator_id=operator_id,
        opco_id=payload.get("opco_id"),
        role=payload.get("role", "operator"),
    )


# ── Auth router (mounted at /auth in api/main.py) ─────────────────────────────

auth_router = APIRouter(prefix="/auth", tags=["Auth"])


@auth_router.post("/token", response_model=TokenResponse, summary="Obtain a JWT access token")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    """
    Exchange operator credentials for a JWT.
    Dev mode (AUTH_DEV_MODE=true): any username + DEV_PASSWORD is accepted.
    Production: bcrypt check against operator_accounts MongoDB collection.
    """
    dev_mode = os.getenv("AUTH_DEV_MODE", "true").lower() == "true"
    if dev_mode:
        if form.password != os.getenv("DEV_PASSWORD", "synapse-dev"):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bad credentials")
        token = _create_access_token({"sub": form.username, "role": "operator"})
        logger.info("auth: dev-mode token issued for operator=%s", form.username)
        return TokenResponse(access_token=token)

    from motor.motor_asyncio import AsyncIOMotorClient
    client = AsyncIOMotorClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
    db = client[os.getenv("MONGODB_DB", "synapse")]
    account = await db.operator_accounts.find_one({"operator_id": form.username})
    if not account or not pwd_context.verify(form.password, account["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Bad credentials")

    token = _create_access_token({
        "sub":     account["operator_id"],
        "opco_id": account.get("opco_id"),
        "role":    account.get("role", "operator"),
    })
    logger.info("auth: token issued for operator=%s", form.username)
    return TokenResponse(access_token=token)


@auth_router.get("/refresh", response_model=TokenResponse, summary="Refresh a valid JWT")
async def refresh_token(operator: OperatorInfo = Depends(get_current_operator)):
    token = _create_access_token({
        "sub":     operator.operator_id,
        "opco_id": operator.opco_id,
        "role":    operator.role,
    })
    logger.info("auth: token refreshed for operator=%s", operator.operator_id)
    return TokenResponse(access_token=token)
