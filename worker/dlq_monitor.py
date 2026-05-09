"""
DLQ Monitor service — Phase 10 Dead Letter Queue consumer.

Consumes from `legacy.commands.dlq`, persists each failure to MongoDB
`dlq_events` collection, and emits structured NOC alerts via the logger.

Operator Portal surfaces DLQ events in the "Failed / DLQ" tab so a human
can choose to:
  • Re-queue the command (manual repair) after fixing the underlying data issue.
  • Acknowledge and close the event (deliberate discard).

Run as a standalone Docker service:
  python -m worker.dlq_monitor
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from kafka.consumer import create_consumer, extract_trace_context
from kafka.topics import GROUP_DLQ_MONITOR, TOPIC_DLQ

logger = logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGODB_DB",  "synapse")

_async_client = None


def _motor_db():
    global _async_client
    if _async_client is None:
        from motor.motor_asyncio import AsyncIOMotorClient
        _async_client = AsyncIOMotorClient(MONGO_URI)
    return _async_client[MONGO_DB]


async def _ensure_dlq_indexes() -> None:
    db = _motor_db()
    await db.dlq_events.create_index([("status", 1), ("failed_at", -1)])
    await db.dlq_events.create_index("outbox_id", unique=True, sparse=True)
    await db.dlq_events.create_index("action_type")
    logger.info("DLQ indexes ensured")


async def _persist_dlq_event(event: dict) -> None:
    """Upsert the DLQ event into MongoDB (idempotent on outbox_id)."""
    db = _motor_db()
    doc = {
        **event,
        "status":     "OPEN",           # OPEN → ACKNOWLEDGED | REQUEUED
        "received_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.dlq_events.update_one(
        {"outbox_id": event.get("outbox_id", event.get("dlq_id"))},
        {"$setOnInsert": doc},
        upsert=True,
    )


async def run_dlq_monitor() -> None:
    await _ensure_dlq_indexes()

    consumer = await create_consumer(
        TOPIC_DLQ,
        group_id=GROUP_DLQ_MONITOR,
    )
    logger.info("DLQ Monitor consuming from %s", TOPIC_DLQ)

    try:
        async for msg in consumer:
            extract_trace_context(msg)
            event = msg.value

            # NOC alert — structured so it can be scraped by log aggregators
            logger.error(
                "NOC_DLQ_ALERT outbox_id=%s action_type=%s table=%s attempts=%s "
                "correlation_id=%s trace_id=%s error=%s",
                event.get("outbox_id"),
                event.get("action_type"),
                event.get("table_name"),
                event.get("attempts"),
                event.get("correlation_id"),
                event.get("trace_id"),
                (event.get("error_context") or "")[:200],
            )

            try:
                await _persist_dlq_event(event)
            except Exception as exc:
                logger.error("DLQ Monitor: MongoDB persist failed: %s", exc)

            await consumer.commit()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [dlq_monitor] %(levelname)s %(message)s",
    )
    asyncio.run(run_dlq_monitor())
