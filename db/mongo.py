"""
MongoDB async state store for the Synapse action pipeline.

Collections:
  action_states       — One document per proposed action; status machine + optimistic locking.
  action_audit_trail  — Append-only audit log; one document per status transition.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, ReturnDocument

from models.action import ActionProposal, ActionStatus, AuditEvent, VALID_TRANSITIONS

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient | None = None


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
    logger.info("MongoDB indexes ensured on collection 'action_states'")


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
