"""
Per-OpCo AI Token Budget Manager — Phase 13.

Enforces monthly token quotas per OpCo so that a high-traffic tenant
(e.g. UK_REGION) cannot exhaust the shared Anthropic API budget and
leave other OpCos (e.g. DE_REGION) unable to process fallouts.

Architecture:
  • MongoDB `token_budgets` collection — one document per (opco_id, period_start)
  • Quotas configured via OPCO_TOKEN_QUOTAS env var (CSV of opco:quota pairs)
  • Billing periods reset on the 1st of each calendar month (UTC)
  • Each Claude invocation CHECKS quota before executing, DEBITS afterwards
  • On quota exhaustion: calling code routes the task to DLQ with QUOTA_EXCEEDED

Data model (one document per OpCo per billing period):
  {
    opco_id:         "UK_REGION",
    quota_tokens:    1_000_000,
    consumed_tokens: 437_821,
    period_start:    "2026-05-01T00:00:00+00:00",
    period_end:      "2026-06-01T00:00:00+00:00",
    created_at:      "2026-05-01T00:00:00+00:00",
  }

Environment
-----------
  OPCO_TOKEN_QUOTAS       CSV of opco_id:monthly_quota pairs, e.g.
                          UK_REGION:1000000,DE_REGION:500000,DEFAULT:100000
  DEFAULT_OPCO_TOKEN_QUOTA  fallback quota for any unspecified OpCo (default: 500000)
  TOKEN_BUDGET_ENABLED    "true" (default) | "false" to disable enforcement
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

TOKEN_BUDGET_ENABLED   = os.getenv("TOKEN_BUDGET_ENABLED", "true").lower() == "true"
_DEFAULT_QUOTA         = int(os.getenv("DEFAULT_OPCO_TOKEN_QUOTA", "500000"))


def _parse_quotas() -> dict[str, int]:
    """Parse OPCO_TOKEN_QUOTAS=UK_REGION:1000000,DE_REGION:500000 into a dict."""
    raw    = os.getenv("OPCO_TOKEN_QUOTAS", "")
    quotas: dict[str, int] = {}
    for item in raw.split(","):
        item = item.strip()
        if ":" in item:
            opco, quota = item.rsplit(":", 1)
            try:
                quotas[opco.strip()] = int(quota.strip())
            except ValueError:
                logger.warning("OPCO_TOKEN_QUOTAS: invalid entry %r", item)
    return quotas


_OPCO_QUOTAS: dict[str, int] = _parse_quotas()


def _current_period() -> tuple[datetime, datetime]:
    """Return (period_start, period_end) for the current calendar month (UTC)."""
    now   = datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if now.month == 12:
        end = start.replace(year=now.year + 1, month=1)
    else:
        end = start.replace(month=now.month + 1)
    return start, end


@dataclass
class QuotaStatus:
    opco_id:   str
    quota:     int
    consumed:  int
    remaining: int
    allowed:   bool
    period:    str


def _db():
    from db.ods import _async_motor_db  # noqa: PLC0415
    return _async_motor_db()


async def _ensure_budget_doc(opco_id: str) -> dict:
    """Return (or create) the current billing period document for an OpCo."""
    db           = _db()
    period_start, period_end = _current_period()
    quota        = _OPCO_QUOTAS.get(opco_id, _DEFAULT_QUOTA)
    period_key   = period_start.isoformat()

    doc = await db.token_budgets.find_one(
        {"opco_id": opco_id, "period_start": period_key},
        {"_id": 0},
    )
    if doc:
        return doc

    doc = {
        "opco_id":         opco_id,
        "quota_tokens":    quota,
        "consumed_tokens": 0,
        "period_start":    period_key,
        "period_end":      period_end.isoformat(),
        "created_at":      datetime.now(timezone.utc).isoformat(),
    }
    await db.token_budgets.insert_one(doc)
    doc.pop("_id", None)
    logger.info(
        "Token budget initialised opco=%s quota=%d period=%s",
        opco_id, quota, period_key,
    )
    return doc


async def check_quota(opco_id: str) -> QuotaStatus:
    """
    Check whether the OpCo has quota remaining for the current billing period.

    Always returns allowed=True when TOKEN_BUDGET_ENABLED is false (dev mode).
    Emits Prometheus metric and logs a warning on exhaustion.
    """
    if not TOKEN_BUDGET_ENABLED:
        return QuotaStatus(
            opco_id=opco_id,
            quota=_DEFAULT_QUOTA,
            consumed=0,
            remaining=_DEFAULT_QUOTA,
            allowed=True,
            period="disabled",
        )

    doc       = await _ensure_budget_doc(opco_id)
    quota     = doc["quota_tokens"]
    consumed  = doc["consumed_tokens"]
    remaining = max(0, quota - consumed)
    allowed   = remaining > 0

    from observability.metrics import token_budget_remaining_gauge
    token_budget_remaining_gauge.labels(opco_id=opco_id).set(remaining)

    if not allowed:
        logger.warning(
            "Token quota EXHAUSTED opco=%s consumed=%d quota=%d period=%s",
            opco_id, consumed, quota, doc["period_start"],
        )
        from observability.metrics import token_quota_exceeded_total
        token_quota_exceeded_total.labels(opco_id=opco_id).inc()

    return QuotaStatus(
        opco_id=opco_id,
        quota=quota,
        consumed=consumed,
        remaining=remaining,
        allowed=allowed,
        period=doc["period_start"],
    )


async def consume_tokens(opco_id: str, input_tokens: int, output_tokens: int) -> int:
    """
    Debit token usage from the OpCo's current billing period.
    Uses an atomic MongoDB $inc to prevent race conditions under concurrent calls.
    Returns the new cumulative consumed_tokens value.
    """
    if not TOKEN_BUDGET_ENABLED:
        return 0

    total        = input_tokens + output_tokens
    db           = _db()
    period_start, _ = _current_period()

    result = await db.token_budgets.find_one_and_update(
        {"opco_id": opco_id, "period_start": period_start.isoformat()},
        {"$inc": {"consumed_tokens": total}},
        return_document=True,
        projection={"_id": 0, "consumed_tokens": 1},
    )
    new_consumed = (result or {}).get("consumed_tokens", total)

    from observability.metrics import token_budget_consumed_total
    token_budget_consumed_total.labels(opco_id=opco_id).inc(total)

    logger.debug(
        "Token debit opco=%s in=%d out=%d total=%d cumulative=%d",
        opco_id, input_tokens, output_tokens, total, new_consumed,
    )
    return new_consumed


async def get_usage_report(opco_id: Optional[str] = None) -> list[dict]:
    """
    Return usage summaries for all OpCos (or one if opco_id is given)
    for the current billing period.
    """
    db           = _db()
    period_start, _ = _current_period()

    query: dict = {"period_start": period_start.isoformat()}
    if opco_id:
        query["opco_id"] = opco_id

    cursor = db.token_budgets.find(query, {"_id": 0})
    docs   = [doc async for doc in cursor]

    # Annotate each doc with utilisation_pct for dashboard convenience
    for doc in docs:
        quota    = doc.get("quota_tokens", 1)
        consumed = doc.get("consumed_tokens", 0)
        doc["utilisation_pct"] = round(consumed / quota * 100, 1) if quota else 0

    return docs


async def ensure_budget_indexes() -> None:
    """Create indexes on the token_budgets collection."""
    db = _db()
    await db.token_budgets.create_index(
        [("opco_id", 1), ("period_start", 1)],
        unique=True,
        name="opco_period_unique",
    )
    await db.token_budgets.create_index("period_start")
    logger.info("token_budgets indexes ensured")
