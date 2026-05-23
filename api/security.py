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
  SYNAPSE_API_KEYS          — comma-separated static API keys (legacy)
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
_RAW            = os.getenv("SYNAPSE_API_KEYS", "")
ALLOWED_KEYS: set[str] = {k.strip() for k in _RAW.split(",") if k.strip()}
_api_key_scheme = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


_DEV_FALLBACK_KEY = os.getenv("DEV_API_KEY", "synapse-dev-key")
# When ENV=development and SYNAPSE_API_KEYS is not set, accept the
# DEV_API_KEY so local curl testing works without editing the .env.
_IS_DEVELOPMENT = os.getenv("ENV", "development").lower() in ("development", "dev", "local")


def require_api_key(api_key: str | None = Security(_api_key_scheme)) -> str:
    """FastAPI dependency — X-API-Key header (machine-to-machine).

    In development (ENV=development, default) an empty SYNAPSE_API_KEYS
    is not fatal: the DEV_API_KEY env var (default 'synapse-dev-key') is
    accepted instead so local curl tests work without a full .env setup.

    Rejection reasons are logged at WARNING so they appear in stdout without
    enabling DEBUG mode.
    """
    # Dev fallback: no configured keys → accept the dev key
    effective_keys = ALLOWED_KEYS or ({_DEV_FALLBACK_KEY} if _IS_DEVELOPMENT else set())

    if not effective_keys:
        logger.warning("api-key: rejected — SYNAPSE_API_KEYS not configured and ENV != development")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "API key auth is not configured. "
                "Set SYNAPSE_API_KEYS in the environment."
            ),
        )
    if api_key is None:
        logger.warning("api-key: rejected — %s header missing", API_KEY_HEADER)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Missing {API_KEY_HEADER} header.",
            headers={"WWW-Authenticate": API_KEY_HEADER},
        )
    if not any(secrets.compare_digest(api_key, valid) for valid in effective_keys):
        # Log a truncated key prefix so you can identify typos without leaking the secret.
        logger.warning("api-key: rejected — key '%s...' not in allowed set", api_key[:6])
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key.")
    return api_key


# ── JWT configuration (Phase 15 / Phase 6 Identity Bridge) ────────────────────

JWT_SECRET_KEY        = os.getenv("JWT_SECRET_KEY", "change-me-in-production-use-32+-char-secret")
# Optional secondary secret issued by the Chatbot Gateway (Phase 6).
# Set CHATBOT_JWT_SECRET to the same value as JWT_SECRET in the chatbot .env.
CHATBOT_JWT_SECRET    = os.getenv("CHATBOT_JWT_SECRET", os.getenv("JWT_SECRET", ""))
JWT_ALGORITHM    = os.getenv("JWT_ALGORITHM", "HS256")
TOKEN_EXPIRE_MIN = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

# Chatbot uses uppercase role names; map them to Core's convention.
_CHATBOT_ROLE_MAP: dict[str, str] = {
    "MANAGER":  "manager",
    "OPERATOR": "operator",
    "CUSTOMER": "customer",
    "CHATBOT":  "customer",
}

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
    """Decode a JWT, trying the primary key then the chatbot gateway key.

    Logs the specific rejection reason at WARNING level so it's visible in
    stdout without enabling DEBUG:
      "jwt: rejected — token expired (sub=operator_uk)"
      "jwt: rejected — signature mismatch on all secrets"
      "jwt: rejected — missing 'sub' claim"
    """
    from jose import ExpiredSignatureError as _Expired

    last_error: str = "unknown"
    for secret in filter(None, [JWT_SECRET_KEY, CHATBOT_JWT_SECRET or None]):
        try:
            payload = jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])
            # Normalise chatbot uppercase role names to Core's lowercase convention.
            if "role" in payload:
                payload["role"] = _CHATBOT_ROLE_MAP.get(payload["role"], payload["role"])
            return payload
        except _Expired:
            sub_hint = "(unknown sub)"
            try:
                import base64, json as _json
                padded = token.split(".")[1] + "=="
                raw = _json.loads(base64.urlsafe_b64decode(padded))
                sub_hint = f"(sub={raw.get('sub', '?')})"
            except Exception:
                pass
            last_error = f"token expired {sub_hint}"
            logger.warning("jwt: rejected — %s", last_error)
            # Expired is definitive — no point trying other secrets.
            break
        except JWTError as exc:
            last_error = f"signature mismatch ({exc})"
            continue

    if "expired" in last_error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired — please obtain a new token via POST /auth/token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    logger.warning("jwt: rejected — %s", last_error)
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
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


# ── Phase 17: dual-auth dependencies ─────────────────────────────────────────

async def get_optional_operator(
    token: Optional[str] = Depends(oauth2_scheme),
) -> Optional[OperatorInfo]:
    """Returns OperatorInfo if a valid JWT Bearer is present, otherwise None."""
    if not token:
        return None
    try:
        payload = _decode_token(token)
        operator_id = payload.get("sub")
        if not operator_id:
            return None
        return OperatorInfo(
            operator_id=operator_id,
            opco_id=payload.get("opco_id"),
            role=payload.get("role", "operator"),
        )
    except HTTPException:
        return None


async def require_any_auth(
    api_key: str | None = Security(_api_key_scheme),
    token: Optional[str] = Depends(oauth2_scheme),
) -> Optional[OperatorInfo]:
    """
    Phase 17 — Dual authentication.

    Accepts Bearer JWT (dashboard) or X-API-Key (machine-to-machine).
    JWT returns an OperatorInfo for RBAC; API-key auth returns None.
    Raises 401 if neither is valid.
    """
    if token:
        try:
            payload = _decode_token(token)
            operator_id = payload.get("sub")
            if operator_id:
                await check_rate_limit(operator_id)
                return OperatorInfo(
                    operator_id=operator_id,
                    opco_id=payload.get("opco_id"),
                    role=payload.get("role", "operator"),
                )
        except HTTPException:
            pass  # fall through to API key

    if ALLOWED_KEYS and api_key and any(secrets.compare_digest(api_key, k) for k in ALLOWED_KEYS):
        return None  # API-key auth — no operator context, RBAC skipped

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required: provide X-API-Key or Bearer JWT",
        headers={"WWW-Authenticate": 'Bearer, APIKey realm="Synapse"'},
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
        expected_pw = os.getenv("DEV_PASSWORD", "synapse-dev")
        if form.password != expected_pw:
            logger.warning(
                "auth/token: dev-mode password mismatch for user=%s "
                "(hint: set DEV_PASSWORD env var, default is 'synapse-dev')",
                form.username,
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=(
                    "Bad credentials. Dev mode is active — "
                    f"use password '{expected_pw}' (or set DEV_PASSWORD env var)."
                ),
            )
        token = _create_access_token({
            "sub":     form.username,
            "role":    "operator",
            "opco_id": "UK_REGION",
        })
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
