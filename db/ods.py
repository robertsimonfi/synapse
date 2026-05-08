"""
MongoDB Operational Data Store (ODS) — Phase 9.

Two collections:
  subscribers  — Golden JSON documents hydrated from Oracle by the Hydrator service.
                 Queried by agent/tools.py instead of Oracle for sub-ms read latency.
  outbox       — Saga write-back outbox. The Executor deposits approved actions here;
                 the Relay service lifts them into Kafka → Oracle Adapter.

Provides both sync (pymongo) and async (motor) interfaces because:
  - agent/tools.py runs in a thread pool (sync)
  - hydrator/relay/executor are async coroutines
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import pymongo
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, ReturnDocument

logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGODB_DB",  "synapse")

# ── Sync client (pymongo) — for thread-pool tool execution ────────────────────

_sync_client: Optional[MongoClient] = None


def _sync_db():
    global _sync_client
    if _sync_client is None:
        _sync_client = MongoClient(MONGO_URI)
    return _sync_client[MONGO_DB]


# ── Async client (motor) — for coroutine-based services ──────────────────────

_async_client: Optional[AsyncIOMotorClient] = None


def _async_motor_db():
    global _async_client
    if _async_client is None:
        _async_client = AsyncIOMotorClient(MONGO_URI)
    return _async_client[MONGO_DB]


# ── Index setup ───────────────────────────────────────────────────────────────

async def ensure_ods_indexes() -> None:
    db = _async_motor_db()
    # subscribers
    await db.subscribers.create_index("customer_id", unique=True)
    await db.subscribers.create_index("profile.email", sparse=True)
    await db.subscribers.create_index("hydrated_at")
    # outbox
    await db.outbox.create_index([("status", 1), ("created_at", 1)])
    await db.outbox.create_index("action_id", unique=True)
    await db.outbox.create_index("correlation_id")
    logger.info("ODS indexes ensured (subscribers, outbox)")


# ── Sync subscriber helpers (used by agent/tools.py) ─────────────────────────

def get_subscriber(customer_id: int) -> Optional[dict]:
    """Return the full subscriber document from the ODS, or None if not hydrated yet."""
    return _sync_db().subscribers.find_one(
        {"customer_id": customer_id}, {"_id": 0}
    )


def subscriber_exists(customer_id: int) -> bool:
    return _sync_db().subscribers.count_documents({"customer_id": customer_id}, limit=1) > 0


# ── Async subscriber helpers (used by hydrator) ───────────────────────────────

async def async_upsert_subscriber(doc: dict) -> None:
    """Upsert a hydrated subscriber document into the ODS."""
    db = _async_motor_db()
    await db.subscribers.update_one(
        {"customer_id": doc["customer_id"]},
        {"$set": {**doc, "hydrated_at": datetime.now(timezone.utc).isoformat()}},
        upsert=True,
    )


async def async_get_subscriber(customer_id: int) -> Optional[dict]:
    db = _async_motor_db()
    return await db.subscribers.find_one({"customer_id": customer_id}, {"_id": 0})


async def async_get_all_customer_ids() -> list[int]:
    """Return all hydrated customer_ids — used by hydrator on first-run bulk load."""
    db = _async_motor_db()
    cursor = db.subscribers.find({}, {"customer_id": 1, "_id": 0})
    return [doc["customer_id"] async for doc in cursor]


# ── Outbox helpers ─────────────────────────────────────────────────────────────
#
# Outbox status lifecycle:
#   PENDING → PUBLISHED → COMPLETED | FAILED
#
# The Relay service polls for PENDING, publishes to Kafka, marks PUBLISHED.
# The Oracle Adapter updates to COMPLETED or FAILED after Oracle execution.

async def create_outbox_entry(
    action_id: str,
    correlation_id: str,
    action_type: str,
    table_name: str,
    operation: str,
    where_clause: dict,
    set_values: dict,
    source_version_anchor: Optional[str] = None,
) -> str:
    """Insert a write-back command into the outbox. Returns the outbox document _id as str."""
    import uuid
    db = _async_motor_db()
    outbox_id = str(uuid.uuid4())
    doc = {
        "outbox_id":             outbox_id,
        "action_id":             action_id,
        "correlation_id":        correlation_id,
        "action_type":           action_type,
        "table_name":            table_name,
        "operation":             operation,
        "where_clause":          where_clause,
        "set_values":            set_values,
        "source_version_anchor": source_version_anchor,
        "status":                "PENDING",
        "retry_count":           0,
        "created_at":            datetime.now(timezone.utc),
        "published_at":          None,
        "completed_at":          None,
        "error":                 None,
    }
    await db.outbox.insert_one(doc)
    logger.info("Outbox entry created outbox_id=%s action_id=%s", outbox_id, action_id)
    return outbox_id


async def list_pending_outbox(limit: int = 50) -> list[dict]:
    """Return PENDING outbox entries ordered by created_at (oldest first)."""
    db = _async_motor_db()
    cursor = db.outbox.find(
        {"status": "PENDING"},
        {"_id": 0},
        sort=[("created_at", pymongo.ASCENDING)],
        limit=limit,
    )
    return [doc async for doc in cursor]


async def mark_outbox_published(outbox_id: str) -> None:
    db = _async_motor_db()
    await db.outbox.update_one(
        {"outbox_id": outbox_id},
        {"$set": {"status": "PUBLISHED", "published_at": datetime.now(timezone.utc)}},
    )


async def mark_outbox_completed(outbox_id: str) -> None:
    db = _async_motor_db()
    await db.outbox.update_one(
        {"outbox_id": outbox_id},
        {"$set": {"status": "COMPLETED", "completed_at": datetime.now(timezone.utc)}},
    )


async def mark_outbox_failed(outbox_id: str, error: str) -> None:
    db = _async_motor_db()
    await db.outbox.update_one(
        {"outbox_id": outbox_id},
        {
            "$set":  {"status": "FAILED", "error": error},
            "$inc":  {"retry_count": 1},
        },
    )


# ── Subscriber document builder (used by hydrator) ────────────────────────────

def _str(v: Any) -> Any:
    """Normalise Oracle types (CLOB, datetime) to JSON-safe primitives."""
    if v is None:
        return None
    if hasattr(v, "read"):          # Oracle CLOB
        text = v.read()
        return text[:500] if len(text) > 500 else text
    if isinstance(v, (int, float, bool, str)):
        return v
    return str(v)


def _clean(row: dict) -> dict:
    return {k: _str(v) for k, v in row.items()}


def build_subscriber_doc(customer_id: int) -> Optional[dict[str, Any]]:
    """
    Assemble the Golden JSON subscriber document from Oracle/SQLite.

    This is the single point where relational data is flattened into the
    document structure the AI agent reads at inference time. Keeping this
    function sync lets the hydrator call it from asyncio.to_thread().
    """
    from agent.db import query, query_one  # noqa: PLC0415

    # ── Customer profile + primary address ────────────────────────────────────
    customer = query_one(
        "SELECT c.*, a.street_line1, a.city, a.state, a.zip_code "
        "FROM CUSTOMER c LEFT JOIN CUSTOMER_ADDRESS a "
        "ON a.CUSTOMER_ID = c.CUSTOMER_ID AND a.IS_PRIMARY = 1 "
        "WHERE c.CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    if not customer:
        return None

    # ── Active subscriptions with product details ─────────────────────────────
    subscriptions = query(
        "SELECT s.subscription_id, s.status, s.msisdn, s.imsi, s.icc_id, "
        "       s.start_date, s.end_date, s.activation_date, s.suspend_reason, "
        "       p.product_id, p.product_code, p.product_name, p.product_type, "
        "       p.category, p.monthly_price, p.billing_cycle "
        "FROM SERVICE_SUBSCRIPTION s "
        "JOIN PRODUCT_CATALOG p ON p.PRODUCT_ID = s.PRODUCT_ID "
        "WHERE s.CUSTOMER_ID = :customer_id "
        "ORDER BY s.START_DATE DESC",
        {"customer_id": customer_id},
    )

    # ── Billing account ───────────────────────────────────────────────────────
    billing = query_one(
        "SELECT ba.*, "
        "(SELECT COUNT(*) FROM BILLING_INVOICE bi "
        " WHERE bi.BILLING_ACCOUNT_ID = ba.BILLING_ACCOUNT_ID "
        " AND bi.STATUS = 'OVERDUE') AS overdue_invoice_count "
        "FROM BILLING_ACCOUNT ba WHERE ba.CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )

    recent_invoices: list = []
    if billing and billing.get("billing_account_id"):
        recent_invoices = query(
            "SELECT invoice_number, billing_period_start, billing_period_end, "
            "       subtotal, tax_amount, total_amount, status, due_date, paid_date "
            "FROM BILLING_INVOICE "
            "WHERE BILLING_ACCOUNT_ID = :ba_id "
            "ORDER BY BILLING_PERIOD_START DESC "
            "FETCH FIRST 3 ROWS ONLY",
            {"ba_id": billing["billing_account_id"]},
        )

    # ── Recent orders (last 5) ─────────────────────────────────────────────────
    recent_orders = query(
        "SELECT order_id, order_number, order_type, status, channel, priority, "
        "       submitted_date, completed_date, failed_date "
        "FROM ORDER_HEADER "
        "WHERE CUSTOMER_ID = :customer_id "
        "ORDER BY SUBMITTED_DATE DESC "
        "FETCH FIRST 5 ROWS ONLY",
        {"customer_id": customer_id},
    )

    # ── Open fallouts ──────────────────────────────────────────────────────────
    open_fallouts = query(
        "SELECT f.fallout_id, f.error_code, f.error_message, f.domain, "
        "       f.severity, f.system_source, f.fallout_date, f.status, f.retry_count, "
        "       o.order_number "
        "FROM ORDER_FALLOUT f "
        "JOIN ORDER_HEADER o ON o.ORDER_ID = f.ORDER_ID "
        "WHERE o.CUSTOMER_ID = :customer_id "
        "  AND f.STATUS IN ('OPEN', 'IN_PROGRESS', 'ESCALATED') "
        "ORDER BY f.FALLOUT_DATE DESC "
        "FETCH FIRST 10 ROWS ONLY",
        {"customer_id": customer_id},
    )

    # ── Recent provisioning logs ───────────────────────────────────────────────
    recent_provisioning = query(
        "SELECT pl.log_id, pl.system_name, pl.action, pl.status, "
        "       pl.response_code, pl.error_detail, pl.attempt_number, "
        "       pl.duration_ms, pl.created_date, o.order_number "
        "FROM PROVISIONING_LOG pl "
        "JOIN ORDER_HEADER o ON o.ORDER_ID = pl.ORDER_ID "
        "WHERE o.CUSTOMER_ID = :customer_id "
        "ORDER BY pl.CREATED_DATE DESC "
        "FETCH FIRST 10 ROWS ONLY",
        {"customer_id": customer_id},
    )

    # ── Open incident tickets ─────────────────────────────────────────────────
    open_tickets = query(
        "SELECT ticket_number, category, priority, status, "
        "       assigned_team, assigned_to, created_date, title "
        "FROM INCIDENT_TICKET "
        "WHERE CUSTOMER_ID = :customer_id "
        "  AND STATUS IN ('OPEN', 'ASSIGNED', 'IN_PROGRESS', 'PENDING_CUSTOMER') "
        "ORDER BY CREATED_DATE DESC "
        "FETCH FIRST 10 ROWS ONLY",
        {"customer_id": customer_id},
    )

    # ── Usage summary (last 90 days, aggregated) ──────────────────────────────
    from datetime import datetime as _dt, timedelta
    cutoff = (_dt.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    usage_summary = query(
        "SELECT u.usage_type, u.unit, "
        "       ROUND(SUM(u.quantity), 3) AS total_quantity, "
        "       ROUND(SUM(u.rated_amount), 2) AS total_rated_amount, "
        "       SUM(CASE WHEN u.roaming_country IS NOT NULL AND u.roaming_country != '' "
        "                THEN 1 ELSE 0 END) AS roaming_sessions "
        "FROM USAGE_RECORD u "
        "JOIN SERVICE_SUBSCRIPTION s ON s.SUBSCRIPTION_ID = u.SUBSCRIPTION_ID "
        "WHERE s.CUSTOMER_ID = :customer_id AND u.START_TIME >= :cutoff "
        "GROUP BY u.usage_type, u.unit",
        {"customer_id": customer_id, "cutoff": cutoff},
    )

    # ── Source version anchor (most recent CUSTOMER.UPDATED_DATE) ─────────────
    updated_date = customer.get("updated_date") or customer.get("created_date", "")

    return {
        "customer_id":            customer_id,
        "source_version_anchor":  str(updated_date),
        "profile": {
            "first_name":    _str(customer.get("first_name")),
            "last_name":     _str(customer.get("last_name")),
            "email":         _str(customer.get("email")),
            "phone":         _str(customer.get("phone")),
            "date_of_birth": _str(customer.get("date_of_birth")),
            "account_type":  _str(customer.get("account_type")),
            "status":        _str(customer.get("status")),
            "credit_score":  customer.get("credit_score"),
            "segment":       _str(customer.get("segment")),
            "address": {
                "street_line1": _str(customer.get("street_line1")),
                "city":         _str(customer.get("city")),
                "state":        _str(customer.get("state")),
                "zip_code":     _str(customer.get("zip_code")),
            },
        },
        "subscriptions":       [_clean(s) for s in subscriptions],
        "billing": {
            **(_clean(billing) if billing else {}),
            "recent_invoices": [_clean(i) for i in recent_invoices],
        },
        "recent_orders":       [_clean(o) for o in recent_orders],
        "open_fallouts":       [_clean(f) for f in open_fallouts],
        "recent_provisioning": [_clean(p) for p in recent_provisioning],
        "open_tickets":        [_clean(t) for t in open_tickets],
        "usage_summary":       [_clean(u) for u in usage_summary],
    }
