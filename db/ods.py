"""
MongoDB Operational Data Store (ODS) — Phase 9 / Phase 12.

Two collections:
  subscribers  — Golden JSON documents hydrated from Oracle by the Hydrator service.
                 Queried by agent/tools.py instead of Oracle for sub-ms read latency.
  outbox       — Saga write-back outbox. The Executor deposits approved actions here;
                 the Relay service lifts them into Kafka → Oracle Adapter.

Provides both sync (pymongo) and async (motor) interfaces because:
  - agent/tools.py runs in a thread pool (sync)
  - hydrator/relay/executor are async coroutines

Phase 12 — Sharding Strategy:
  Shard key:  { opco_id: 1, customer_id: 1 }
  Rationale:  opco_id provides balanced cardinality across regional shards; customer_id
              as the sub-key ensures even distribution within each OpCo bucket.
              All hot-path queries filter on opco_id first, so chunk-level locality
              is preserved without scatter-gather fan-out.
  Admin:      Run infra/mongo_sharding.js against mongos when promoting to a sharded
              cluster.  The compound index created here doubles as the required
              supporting index for the shard key.
"""

from __future__ import annotations

import hashlib
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
    # subscribers — Phase 12: compound shard-key index first so it can be used
    # directly as the shard key without a second supporting index.
    await db.subscribers.create_index(
        [("opco_id", pymongo.ASCENDING), ("customer_id", pymongo.ASCENDING)],
        name="opco_customer_shard_key",
    )
    await db.subscribers.create_index("customer_id", unique=True)
    await db.subscribers.create_index("profile.email", sparse=True)
    await db.subscribers.create_index("hydrated_at")
    await db.subscribers.create_index(
        [("opco_id", pymongo.ASCENDING), ("hydrated_at", pymongo.DESCENDING)],
        name="opco_hydrated_at",
    )
    # prefetch cache index — Phase 12
    await db.subscribers.create_index(
        [("opco_id", pymongo.ASCENDING), ("_prefetch_requested_at", pymongo.DESCENDING)],
        name="opco_prefetch_ts",
        sparse=True,
    )
    # Phase 13: tiered storage — archiver queries on last_accessed_at + storage_tier
    await db.subscribers.create_index(
        [("storage_tier", pymongo.ASCENDING), ("last_accessed_at", pymongo.ASCENDING)],
        name="storage_tier_last_accessed",
        sparse=True,
    )
    await db.subscribers.create_index("storage_tier", sparse=True)
    # cold_subscribers — cold store index (MongoDB backend)
    await db.cold_subscribers.create_index(
        [("opco_id", pymongo.ASCENDING), ("customer_id", pymongo.ASCENDING)],
        unique=True,
        name="cold_opco_customer",
    )
    # token_budgets indexes are managed by billing/token_budget.py::ensure_budget_indexes()
    # outbox
    await db.outbox.create_index([("status", 1), ("created_at", 1)])
    await db.outbox.create_index("action_id", unique=True)
    await db.outbox.create_index("correlation_id")
    await db.outbox.create_index([("opco_id", 1), ("status", 1)])
    # dlq_events (Phase 10)
    await db.dlq_events.create_index([("status", 1), ("failed_at", -1)])
    await db.dlq_events.create_index("outbox_id", unique=True, sparse=True)
    await db.dlq_events.create_index("action_type")
    await db.dlq_events.create_index([("opco_id", 1), ("status", 1)])
    # canary_runs (Phase 12)
    await db.canary_runs.create_index([("model_version", 1), ("started_at", -1)])
    await db.canary_runs.create_index([("opco_id", 1), ("model_version", 1)])
    await db.canary_runs.create_index("run_id", unique=True)
    # force_sync_audit (Phase 12)
    await db.force_sync_audit.create_index([("customer_id", 1), ("synced_at", -1)])
    await db.force_sync_audit.create_index([("opco_id", 1), ("synced_at", -1)])
    logger.info("ODS indexes ensured (subscribers, outbox, dlq_events, canary_runs, force_sync_audit)")


# ── Sync subscriber helpers (used by agent/tools.py) ─────────────────────────

def get_subscriber(customer_id: int) -> Optional[dict]:
    """
    Return the full subscriber document from the ODS, or None if not hydrated yet.
    Phase 13: touches last_accessed_at to prevent premature archival.
    """
    db  = _sync_db()
    doc = db.subscribers.find_one({"customer_id": customer_id}, {"_id": 0})
    if doc and doc.get("storage_tier") != "cold":
        db.subscribers.update_one(
            {"customer_id": customer_id},
            {"$set": {"last_accessed_at": datetime.now(timezone.utc).isoformat()}},
        )
    return doc


def subscriber_exists(customer_id: int) -> bool:
    return _sync_db().subscribers.count_documents({"customer_id": customer_id}, limit=1) > 0


# ── Async subscriber helpers (used by hydrator) ───────────────────────────────

async def async_upsert_subscriber(doc: dict) -> None:
    """Upsert a hydrated subscriber document into the ODS."""
    db  = _async_motor_db()
    now = datetime.now(timezone.utc).isoformat()
    await db.subscribers.update_one(
        {"customer_id": doc["customer_id"]},
        {"$set": {
            **doc,
            "hydrated_at":      now,
            "last_accessed_at": now,
            "storage_tier":     "warm",
        }},
        upsert=True,
    )


async def async_get_subscriber(customer_id: int) -> Optional[dict]:
    """
    Async read path — also touches last_accessed_at for archiver TTL tracking.
    """
    db  = _async_motor_db()
    doc = await db.subscribers.find_one({"customer_id": customer_id}, {"_id": 0})
    if doc and doc.get("storage_tier") != "cold":
        await db.subscribers.update_one(
            {"customer_id": customer_id},
            {"$set": {"last_accessed_at": datetime.now(timezone.utc).isoformat()}},
        )
    return doc


async def async_get_all_customer_ids() -> list[int]:
    """Return all hydrated customer_ids — used by hydrator on first-run bulk load."""
    db = _async_motor_db()
    cursor = db.subscribers.find({}, {"customer_id": 1, "_id": 0})
    return [doc["customer_id"] async for doc in cursor]


async def async_get_customer_ids_by_opco(
    opco_id: str,
    limit: int = 5000,
    include_cold: bool = True,
) -> list[int]:
    """
    Return all customer_ids for a given OpCo — used by predictive prefetch.

    Phase 13: include_cold=True (default) returns both warm docs and cold stubs,
    so the prefetch fan-out list is complete and the Hydrator can promote cold
    subscribers before issuing their synthetic CDC events.
    """
    db    = _async_motor_db()
    query = {"opco_id": opco_id}
    if not include_cold:
        query["storage_tier"] = {"$ne": "cold"}

    cursor = db.subscribers.find(query, {"customer_id": 1, "_id": 0}, limit=limit)
    return [doc["customer_id"] async for doc in cursor]


async def promote_from_cold(customer_id: int, opco_id: str, reason: str = "prefetch") -> bool:
    """
    Phase 13 — Warm-up path: restore a cold document from the cold store to the hot ODS.

    Returns True if the document was found and promoted, False if it was not in cold store
    (caller should then fall back to Oracle source directly via build_subscriber_doc).

    Called by worker/prefetch.py when a HIGH_SIGNAL event touches a cold subscriber.
    The Predictive Hydrator is the warmth-tier arbiter: the act of prefetch IS the
    promotion event.
    """
    from db.cold_store import get_cold_store  # noqa: PLC0415

    cold = get_cold_store()
    doc  = await cold.retrieve(customer_id, opco_id)

    if doc is None:
        return False

    # Restore full document to hot ODS with warm tier
    await async_upsert_subscriber(doc)

    # Remove from cold store (it's now back in hot ODS)
    await cold.delete(customer_id, opco_id)

    from observability.metrics import cold_promotions_total  # noqa: PLC0415
    cold_promotions_total.labels(opco_id=opco_id, reason=reason).inc()

    logger.info(
        "Cold→warm promotion: customer_id=%s opco=%s reason=%s",
        customer_id, opco_id, reason,
    )
    return True


async def force_sync_customer(
    customer_id: int,
    opco_id: str,
    operator_id: str,
    justification: str,
) -> Optional[dict]:
    """
    Phase 12 — Force Sync: bypass Kafka and rebuild the subscriber document
    directly from Oracle, then immediately upsert to MongoDB ODS.

    This is the targeted per-customer override for when the Kafka circuit
    breaker is SYSTEM_STALE. The Kafka pipeline recovers independently;
    this call gives NOC operators fresh data for a specific subscriber
    without waiting for lag to drain.

    Returns the freshly upserted document, or None if the customer does not
    exist in Oracle.
    """
    import asyncio
    from db.ods import build_subscriber_doc, mask_subscriber_pii

    doc = await asyncio.to_thread(build_subscriber_doc, customer_id)
    if doc is None:
        return None

    doc["opco_id"] = opco_id
    doc, masked_count = mask_subscriber_pii(doc)
    doc["_force_synced_at"] = datetime.now(timezone.utc).isoformat()
    doc["_force_synced_by"] = operator_id

    await async_upsert_subscriber(doc)

    # Persist audit trail
    db = _async_motor_db()
    await db.force_sync_audit.insert_one({
        "customer_id":   customer_id,
        "opco_id":       opco_id,
        "operator_id":   operator_id,
        "justification": justification,
        "pii_masked":    masked_count,
        "synced_at":     datetime.now(timezone.utc),
    })

    logger.info(
        "Force-sync: customer_id=%s opco=%s by=%s pii_masked=%d",
        customer_id, opco_id, operator_id, masked_count,
    )
    return doc


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
    opco_id: Optional[str] = None,
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
        "opco_id":               opco_id,
        "status":                "PENDING",
        "retry_count":           0,
        "created_at":            datetime.now(timezone.utc),
        "published_at":          None,
        "completed_at":          None,
        "error":                 None,
    }
    await db.outbox.insert_one(doc)
    logger.info("Outbox entry created outbox_id=%s action_id=%s opco_id=%s", outbox_id, action_id, opco_id)
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


async def mark_outbox_sent(outbox_id: str) -> None:
    """
    Mark an outbox entry as SENT after the Relay successfully published it to Kafka.

    Preserves the entry in MongoDB with a sent_at timestamp so operators have a
    complete audit trail (PENDING → SENT → COMPLETED|FAILED) rather than entries
    disappearing from the queue after Kafka delivery.
    """
    db = _async_motor_db()
    await db.outbox.update_one(
        {"outbox_id": outbox_id},
        {"$set": {"status": "SENT", "sent_at": datetime.now(timezone.utc)}},
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


# ── Phase 10: DLQ event helpers ───────────────────────────────────────────────

async def list_dlq_events(status: str = "OPEN", limit: int = 100) -> list[dict]:
    """Return DLQ events ordered newest-first."""
    db = _async_motor_db()
    cursor = db.dlq_events.find(
        {"status": status} if status != "ALL" else {},
        {"_id": 0},
        sort=[("failed_at", pymongo.DESCENDING)],
        limit=limit,
    )
    return [doc async for doc in cursor]


async def acknowledge_dlq_event(outbox_id: str, operator_id: str) -> None:
    """Mark a DLQ event as ACKNOWLEDGED by an operator."""
    db = _async_motor_db()
    await db.dlq_events.update_one(
        {"outbox_id": outbox_id},
        {"$set": {
            "status": "ACKNOWLEDGED",
            "acknowledged_by": operator_id,
            "acknowledged_at": datetime.now(timezone.utc).isoformat(),
        }},
    )


# ── Phase 11: PII masking — Privacy by Design ─────────────────────────────────
#
# Deterministic SHA-256 tokenisation applied before any subscriber document
# is written to the MongoDB ODS.  Same input always produces the same token
# so cross-document correlation remains possible, but raw PII is never stored.
#
# Token format:  [PII:<CATEGORY>:<12-HEX>]

def _pii_token(value: str, category: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12].upper()
    return f"[PII:{category}:{digest}]"


def mask_subscriber_pii(doc: dict) -> tuple[dict, int]:
    """
    Return a copy of *doc* with PII fields in profile and subscriptions tokenised.
    Also returns the count of masked fields (for Prometheus).
    """
    masked_count = 0
    doc = dict(doc)  # shallow copy; we mutate profile and subscriptions below

    # ── Profile PII ───────────────────────────────────────────────────────────
    profile = dict(doc.get("profile") or {})
    for field, category in (("email", "EMAIL"), ("phone", "PHONE")):
        if profile.get(field):
            profile[field] = _pii_token(profile[field], category)
            masked_count += 1
    doc["profile"] = profile

    # ── Subscription PII (MSISDN, IMSI, ICC_ID) ──────────────────────────────
    masked_subs = []
    for sub in doc.get("subscriptions") or []:
        sub = dict(sub)
        for field, category in (("msisdn", "PHONE"), ("imsi", "IMSI"), ("icc_id", "ICC")):
            if sub.get(field):
                sub[field] = _pii_token(sub[field], category)
                masked_count += 1
        masked_subs.append(sub)
    doc["subscriptions"] = masked_subs

    return doc, masked_count


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
