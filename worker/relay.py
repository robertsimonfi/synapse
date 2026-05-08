"""
Relay service — Phase 9 Outbox Write-Back Relay.

Implements the Transactional Outbox Pattern for the Saga:

  MongoDB outbox (PENDING)
    → Relay polls → publishes WritebackCommand to `legacy.commands.writeback`
    → marks outbox entry PUBLISHED
    → Oracle Adapter consumes and executes the DML

This decouples the Executor (which decides WHAT to write) from Oracle
(which executes the write), protecting the mission-critical relational
system from direct AI-induced load.

Idempotency: The WritebackCommand carries source_version_anchor (Oracle SCN /
UPDATED_DATE). The Oracle Adapter uses this to guard against duplicate application
of the same write if the relay restarts and re-publishes.

Run as a standalone Docker service:
  python -m worker.relay
"""

import asyncio
import logging
import os

from db.ods import (
    ensure_ods_indexes,
    list_pending_outbox,
    mark_outbox_published,
)
from kafka.producer import create_producer, publish
from kafka.schemas import WritebackCommand
from kafka.topics import TOPIC_WRITEBACK

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("RELAY_POLL_INTERVAL", "5"))
BATCH_SIZE    = int(os.getenv("RELAY_BATCH_SIZE",    "50"))


async def _relay_batch(producer) -> int:
    """Lift one batch of PENDING outbox entries into Kafka. Returns the count published."""
    entries = await list_pending_outbox(limit=BATCH_SIZE)
    if not entries:
        return 0

    published = 0
    for entry in entries:
        try:
            cmd = WritebackCommand(
                command_id=entry["outbox_id"],
                action_id=entry["action_id"],
                correlation_id=entry["correlation_id"],
                action_type=entry["action_type"],
                table_name=entry["table_name"],
                operation=entry.get("operation", "UPDATE"),
                where_clause=entry.get("where_clause", {}),
                set_values=entry.get("set_values", {}),
                source_version_anchor=entry.get("source_version_anchor"),
                outbox_id=entry["outbox_id"],
            )
            await publish(
                producer,
                TOPIC_WRITEBACK,
                cmd.model_dump(),
                key=entry["action_id"],
            )
            await mark_outbox_published(entry["outbox_id"])
            published += 1
            logger.info(
                "Relay published outbox_id=%s action_type=%s → %s",
                entry["outbox_id"], entry["action_type"], TOPIC_WRITEBACK,
            )
        except Exception as exc:
            logger.error(
                "Relay failed for outbox_id=%s: %s", entry.get("outbox_id", "?"), exc
            )

    return published


async def run_relay() -> None:
    await ensure_ods_indexes()
    producer = await create_producer()
    logger.info(
        "Relay service started (poll_interval=%ds, batch=%d)", POLL_INTERVAL, BATCH_SIZE
    )

    try:
        while True:
            try:
                count = await _relay_batch(producer)
                if count:
                    logger.info("Relay batch: %d entries published", count)
            except Exception:
                logger.exception("Relay poll error")

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await producer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [relay] %(levelname)s %(message)s",
    )
    asyncio.run(run_relay())
