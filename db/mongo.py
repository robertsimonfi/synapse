"""
MongoDB async state store for the Synapse action pipeline.

Collections:
  action_states       — One document per proposed action; status machine + optimistic locking.
  action_audit_trail  — Append-only audit log; one document per status transition.
  engagement_log      — Phase 7: post-action customer notifications and their responses.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument

from models.action import ActionProposal, ActionStatus, AuditEvent, VALID_TRANSITIONS
from models.engagement import CustomerResponse, EngagementLog, EngagementStatus

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient | None = None

_CUSTOMERS_COLLECTION = "customers"


def _get_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        _client = AsyncIOMotorClient(uri)
    return _client


def _db():
    return _get_client()[os.getenv("MONGODB_DB", "synapse")]


async def ensure_indexes() -> None:
    db = _db()
    await db.action_states.create_index(
        [("status", ASCENDING), ("created_at", DESCENDING)]
    )
    await db.action_states.create_index([("correlation_id", ASCENDING)])
    await db.action_states.create_index([("order_id", ASCENDING)])
    await db.action_states.create_index([("expires_at", ASCENDING)], sparse=True)
    await db.action_audit_trail.create_index(
        [("action_id", ASCENDING), ("timestamp", DESCENDING)]
    )
    await db[_CUSTOMERS_COLLECTION].create_index("customer_key", unique=True, background=True)
    await db[_CUSTOMERS_COLLECTION].create_index(
        [("segment", ASCENDING), ("churn_risk_score", DESCENDING)], background=True
    )
    # Phase 7 — engagement log indexes
    await db.engagement_log.create_index([("action_id", ASCENDING)])
    await db.engagement_log.create_index([("correlation_id", ASCENDING)])
    await db.engagement_log.create_index([("customer_id", ASCENDING)])
    await db.engagement_log.create_index([("created_at", DESCENDING)])
    logger.info("MongoDB indexes ensured")


# ── Customer document helpers ─────────────────────────────────────────────────


async def get_customer(customer_key: str) -> dict | None:
    """Fetch a full customer document by customer_key. Returns None if not found."""
    doc = await _db()[_CUSTOMERS_COLLECTION].find_one(
        {"customer_key": customer_key},
        projection={"_id": 0},
    )
    if doc is None:
        logger.warning("Customer not found: %s", customer_key)
    return doc


async def upsert_customer(customer_key: str, document: dict) -> None:
    """Insert or fully replace a customer document (used by seed scripts and tests)."""
    await _db()[_CUSTOMERS_COLLECTION].replace_one(
        {"customer_key": customer_key},
        document,
        upsert=True,
    )


async def list_customer_keys() -> list[str]:
    """Return all customer_key values in the collection."""
    cursor = _db()[_CUSTOMERS_COLLECTION].find({}, projection={"customer_key": 1, "_id": 0})
    docs = await cursor.to_list(length=None)
    return [d["customer_key"] for d in docs]


async def append_agent_history(customer_key: str, entry: dict) -> None:
    """Append an interaction record to the customer's agent_history array."""
    await _db()[_CUSTOMERS_COLLECTION].update_one(
        {"customer_key": customer_key},
        {"$push": {"agent_history": entry}},
    )


# ── CRUD ──────────────────────────────────────────────────────────────────────


async def create_action(proposal: ActionProposal) -> ActionProposal:
    db = _db()
    doc = _to_doc(proposal)
    await db.action_states.insert_one(doc)
    await _append_audit(
        proposal.id, None, ActionStatus.PENDING,
        "agent-thinker", {"correlation_id": proposal.correlation_id},
    )
    return proposal


async def get_action(action_id: str) -> ActionProposal | None:
    doc = await _db().action_states.find_one({"_id": action_id})
    return _from_doc(doc) if doc else None


async def list_actions(
    status: ActionStatus | None = None,
    limit: int = 100,
) -> list[ActionProposal]:
    filt: dict[str, Any] = {}
    if status:
        filt["status"] = status.value
    cursor = _db().action_states.find(filt).sort("created_at", DESCENDING).limit(limit)
    return [_from_doc(d) async for d in cursor]


async def transition_action(
    action_id: str,
    expected_version: int,
    new_status: ActionStatus,
    actor: str,
    extra_updates: dict[str, Any] | None = None,
) -> ActionProposal | None:
    """
    Atomic optimistic-lock status transition.

    Returns the updated ActionProposal on success.
    Returns None if the version does not match (concurrent writer detected).
    Raises KeyError if the action doesn't exist.
    Raises ValueError for invalid state-machine transitions.
    """
    current = await get_action(action_id)
    if not current:
        raise KeyError(f"Action {action_id} not found")

    allowed = VALID_TRANSITIONS.get(current.status, set())
    if new_status not in allowed:
        raise ValueError(
            f"Invalid transition for action {action_id}: "
            f"{current.status.value} → {new_status.value} "
            f"(allowed: {[s.value for s in allowed]})"
        )

    update_fields: dict[str, Any] = {
        "status": new_status.value,
        "version": expected_version + 1,
        "updated_at": datetime.utcnow(),
        **(extra_updates or {}),
    }

    result = await _db().action_states.find_one_and_update(
        {"_id": action_id, "version": expected_version},
        {"$set": update_fields},
        return_document=ReturnDocument.AFTER,
    )
    if result is None:
        # Version mismatch — another writer updated between our read and CAS write
        logger.warning(
            "Optimistic lock failed for action %s at version %d", action_id, expected_version
        )
        return None

    updated = _from_doc(result)
    await _append_audit(action_id, current.status, new_status, actor)
    return updated


async def get_audit_trail(action_id: str) -> list[AuditEvent]:
    cursor = _db().action_audit_trail.find(
        {"action_id": action_id}
    ).sort("timestamp", ASCENDING)
    return [_audit_from_doc(d) async for d in cursor]


async def get_queue_depths() -> dict[str, int]:
    """Return count of actions grouped by status."""
    pipeline = [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    depths: dict[str, int] = {s.value: 0 for s in ActionStatus}
    async for doc in _db().action_states.aggregate(pipeline):
        depths[doc["_id"]] = doc["count"]
    return depths


async def expire_stale_actions(max_age_hours: int = 4) -> list[str]:
    """
    Atomically transition PENDING actions older than max_age_hours to EXPIRED.
    Returns the IDs of every action that was expired.
    """
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    cursor = _db().action_states.find(
        {"status": ActionStatus.PENDING.value, "created_at": {"$lt": cutoff}}
    )
    expired_ids: list[str] = []
    async for doc in cursor:
        action_id = doc["_id"]
        result = await _db().action_states.find_one_and_update(
            {"_id": action_id, "status": ActionStatus.PENDING.value},
            {"$set": {
                "status": ActionStatus.EXPIRED.value,
                "version": doc["version"] + 1,
                "updated_at": datetime.utcnow(),
            }},
            return_document=ReturnDocument.AFTER,
        )
        if result:
            await _append_audit(
                action_id, ActionStatus.PENDING, ActionStatus.EXPIRED, "system-expiry-job"
            )
            expired_ids.append(action_id)
    return expired_ids


# ── Internal helpers ──────────────────────────────────────────────────────────


def _to_doc(proposal: ActionProposal) -> dict:
    d = proposal.model_dump()
    d["_id"] = d.pop("id")
    d["status"] = d["status"].value if hasattr(d["status"], "value") else d["status"]
    return d


def _from_doc(doc: dict) -> ActionProposal:
    d = dict(doc)
    d["id"] = d.pop("_id")
    return ActionProposal(**d)


def _audit_from_doc(doc: dict) -> AuditEvent:
    d = dict(doc)
    d["id"] = d.pop("_id")
    return AuditEvent(**d)


async def _append_audit(
    action_id: str,
    prev: ActionStatus | None,
    new: ActionStatus,
    actor: str,
    metadata: dict | None = None,
) -> None:
    event = AuditEvent(
        action_id=action_id,
        previous_status=prev,
        new_status=new,
        actor=actor,
        metadata=metadata or {},
    )
    doc = event.model_dump()
    doc["_id"] = doc.pop("id")
    doc["previous_status"] = prev.value if prev else None
    doc["new_status"] = new.value
    await _db().action_audit_trail.insert_one(doc)


# ── Phase 7: Engagement log helpers ──────────────────────────────────────────


def _eng_to_doc(log: EngagementLog) -> dict:
    d = log.model_dump()
    d["_id"] = d.pop("id")
    d["channel"]           = d["channel"].value           if hasattr(d["channel"], "value")           else d["channel"]
    d["status"]            = d["status"].value             if hasattr(d["status"], "value")             else d["status"]
    d["customer_response"] = d["customer_response"].value  if hasattr(d["customer_response"], "value")  else d["customer_response"]
    return d


def _eng_from_doc(doc: dict) -> EngagementLog:
    d = dict(doc)
    d["id"] = d.pop("_id")
    return EngagementLog(**d)


async def create_engagement(log: EngagementLog) -> EngagementLog:
    await _db().engagement_log.insert_one(_eng_to_doc(log))
    return log


async def get_engagement(engagement_id: str) -> EngagementLog | None:
    doc = await _db().engagement_log.find_one({"_id": engagement_id})
    return _eng_from_doc(doc) if doc else None


async def get_engagement_by_correlation(correlation_id: str) -> EngagementLog | None:
    """Return the most recent engagement for a given correlation_id."""
    doc = await _db().engagement_log.find_one(
        {"correlation_id": correlation_id},
        sort=[("created_at", DESCENDING)],
    )
    return _eng_from_doc(doc) if doc else None


async def list_engagements(
    limit: int = 100,
    action_id: str | None = None,
    customer_id: str | None = None,
) -> list[EngagementLog]:
    filt: dict[str, Any] = {}
    if action_id:
        filt["action_id"] = action_id
    if customer_id:
        filt["customer_id"] = customer_id
    cursor = _db().engagement_log.find(filt).sort("created_at", DESCENDING).limit(limit)
    return [_eng_from_doc(d) async for d in cursor]


async def record_customer_response(
    engagement_id: str,
    response_text: str,
    received_at: datetime,
) -> EngagementLog | None:
    """Mark an engagement as REPLIED and store the customer's response text."""
    result = await _db().engagement_log.find_one_and_update(
        {"_id": engagement_id},
        {"$set": {
            "customer_response":    CustomerResponse.REPLIED.value,
            "response_text":        response_text,
            "response_received_at": received_at,
        }},
        return_document=ReturnDocument.AFTER,
    )
    return _eng_from_doc(result) if result else None
