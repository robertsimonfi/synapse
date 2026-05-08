"""
Hydrator service — Phase 9 CDC Pipeline (Oracle → Kafka → MongoDB ODS).

Architecture:
  1. CDC Producer: polls Oracle for rows changed since the last checkpoint
     (simulating Debezium tailing the Oracle Redo Log). Publishes OracleCdcEvent
     messages to the `oracle.cdc.all_changes` Kafka topic.

  2. CDC Consumer / Hydrator: consumes from `oracle.cdc.all_changes`, identifies
     the affected customer_id(s), rebuilds the full Golden JSON subscriber document
     from Oracle, and upserts it into the MongoDB `subscribers` ODS collection.

The two halves run as concurrent asyncio tasks inside a single process, though
in production they could be split into separate deployments.

First-run bulk load: on startup the service checks MongoDB for existing documents
and schedules any customer_ids not yet present for immediate hydration (backfill).

Environment:
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  HYDRATOR_POLL_INTERVAL    seconds between CDC sweeps (default: 30)
  HYDRATOR_BATCH_SIZE       max customers processed per sweep (default: 200)
  DB_TYPE / SQLITE_PATH / ORA_* — inherited from agent.db

Run as a standalone Docker service:
  python -m worker.hydrator
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from aiokafka import AIOKafkaConsumer

from agent.db import query
from db.ods import (
    async_upsert_subscriber,
    build_subscriber_doc,
    ensure_ods_indexes,
)
from kafka.producer import create_producer, publish, current_trace_id
from kafka.consumer import create_consumer, extract_trace_context
from kafka.schemas import OracleCdcEvent
from kafka.topics import (
    GROUP_HYDRATOR,
    TOPIC_CDC_ALL_CHANGES,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("HYDRATOR_POLL_INTERVAL", "30"))
BATCH_SIZE    = int(os.getenv("HYDRATOR_BATCH_SIZE", "200"))

# ── CDC checkpoint (in-memory, persisted to a simple file for crash recovery) ──

_CHECKPOINT_FILE = os.getenv("HYDRATOR_CHECKPOINT_FILE", "/data/hydrator_checkpoint.json")


def _load_checkpoint() -> datetime:
    try:
        with open(_CHECKPOINT_FILE) as f:
            ts = json.load(f).get("last_seen_ts")
            return datetime.fromisoformat(ts) if ts else datetime.now(timezone.utc) - timedelta(days=1)
    except (FileNotFoundError, ValueError, KeyError):
        # On first run, load everything from the last 24 hours
        return datetime.now(timezone.utc) - timedelta(days=1)


def _save_checkpoint(ts: datetime) -> None:
    try:
        os.makedirs(os.path.dirname(_CHECKPOINT_FILE), exist_ok=True)
        with open(_CHECKPOINT_FILE, "w") as f:
            json.dump({"last_seen_ts": ts.isoformat()}, f)
    except Exception as exc:
        logger.warning("Checkpoint save failed: %s", exc)


# ── CDC producer — polls Oracle and publishes change events ───────────────────

# Tables that carry CUSTOMER_ID directly or indirectly
_DIRECT_CUSTOMER_TABLES = {
    "CUSTOMER":             "CUSTOMER_ID",
    "CUSTOMER_ADDRESS":     "CUSTOMER_ID",
    "SERVICE_SUBSCRIPTION": "CUSTOMER_ID",
    "BILLING_ACCOUNT":      "CUSTOMER_ID",
    "INCIDENT_TICKET":      "CUSTOMER_ID",
    "ORDER_HEADER":         "CUSTOMER_ID",
}

_INDIRECT_LOOKUP: dict[str, tuple[str, str, str]] = {
    # table_name → (join table, join_key, customer_col)
    "BILLING_INVOICE":  ("BILLING_ACCOUNT",      "BILLING_ACCOUNT_ID", "CUSTOMER_ID"),
    "ORDER_FALLOUT":    ("ORDER_HEADER",          "ORDER_ID",           "CUSTOMER_ID"),
    "PROVISIONING_LOG": ("ORDER_HEADER",          "ORDER_ID",           "CUSTOMER_ID"),
    "ORDER_LINE_ITEM":  ("ORDER_HEADER",          "ORDER_ID",           "CUSTOMER_ID"),
}


def _get_changed_customer_ids(since: datetime) -> list[int]:
    """
    Find all customer_ids whose Oracle rows have been touched since `since`.
    Simulates Debezium by polling UPDATED_DATE / CREATED_DATE columns.
    """
    cutoff = since.strftime("%Y-%m-%d %H:%M:%S")
    customer_ids: set[int] = set()

    for table, cid_col in _DIRECT_CUSTOMER_TABLES.items():
        try:
            rows = query(
                f"SELECT DISTINCT {cid_col} FROM {table} "
                f"WHERE COALESCE(UPDATED_DATE, CREATED_DATE) >= :cutoff",
                {"cutoff": cutoff},
            )
            customer_ids.update(r[cid_col.lower()] for r in rows if r.get(cid_col.lower()))
        except Exception as exc:
            logger.debug("CDC poll %s skipped: %s", table, exc)

    for table, (join_table, join_key, cid_col) in _INDIRECT_LOOKUP.items():
        try:
            rows = query(
                f"SELECT DISTINCT j.{cid_col} FROM {table} t "
                f"JOIN {join_table} j ON j.{join_key} = t.{join_key} "
                f"WHERE t.CREATED_DATE >= :cutoff",
                {"cutoff": cutoff},
            )
            customer_ids.update(r[cid_col.lower()] for r in rows if r.get(cid_col.lower()))
        except Exception as exc:
            logger.debug("CDC poll %s skipped: %s", table, exc)

    return list(customer_ids)


async def _run_cdc_producer(producer) -> None:
    """Continuously poll Oracle for changes and publish OracleCdcEvent messages."""
    checkpoint = _load_checkpoint()
    logger.info("CDC producer started — checkpoint=%s", checkpoint.isoformat())

    while True:
        sweep_start = datetime.now(timezone.utc)
        try:
            changed_ids = await asyncio.to_thread(_get_changed_customer_ids, checkpoint)
            if changed_ids:
                logger.info("CDC sweep found %d changed customer(s) since %s",
                            len(changed_ids), checkpoint.isoformat())
                for cid in changed_ids[:BATCH_SIZE]:
                    event = OracleCdcEvent(
                        op="UPDATE",
                        table_name="CUSTOMER",
                        primary_key={"CUSTOMER_ID": cid},
                        after={"CUSTOMER_ID": cid},
                    )
                    await publish(
                        producer,
                        TOPIC_CDC_ALL_CHANGES,
                        event.model_dump(),
                        key=str(cid),
                    )
            _save_checkpoint(sweep_start)
            checkpoint = sweep_start
        except Exception as exc:
            logger.exception("CDC producer sweep error: %s", exc)

        await asyncio.sleep(POLL_INTERVAL)


# ── CDC consumer / hydrator — builds Golden JSON and upserts to MongoDB ────────

async def _run_hydrator_consumer() -> None:
    """Consume CDC events and rebuild subscriber documents in MongoDB ODS."""
    consumer = await create_consumer(
        TOPIC_CDC_ALL_CHANGES,
        group_id=GROUP_HYDRATOR,
    )
    logger.info("Hydrator consumer started on %s", TOPIC_CDC_ALL_CHANGES)

    try:
        async for msg in consumer:
            # Phase 10: resume distributed trace from Kafka message headers
            extract_trace_context(msg)

            event = msg.value  # already a dict via consumer's deserializer
            customer_id = (
                event.get("primary_key", {}).get("CUSTOMER_ID")
                or event.get("after", {}).get("CUSTOMER_ID")
            )
            if not customer_id:
                await consumer.commit()
                continue

            try:
                doc = await asyncio.to_thread(build_subscriber_doc, int(customer_id))
                if doc:
                    # Phase 10: stamp the trace_id so operators can click-to-trace
                    tid = current_trace_id()
                    if tid:
                        doc["last_sync_trace_id"] = tid
                    await async_upsert_subscriber(doc)
                    logger.debug("Hydrated customer_id=%s trace_id=%s", customer_id, tid)
                else:
                    logger.warning("build_subscriber_doc returned None for customer_id=%s", customer_id)
            except Exception as exc:
                logger.error("Hydration failed for customer_id=%s: %s", customer_id, exc)
                # Commit anyway to avoid poison-pill loop; failed docs will be
                # re-hydrated on the next CDC sweep when UPDATED_DATE changes again.

            await consumer.commit()
    finally:
        await consumer.stop()


# ── Bulk backfill (first-run only) ─────────────────────────────────────────────

async def _bulk_backfill(producer) -> None:
    """
    On first run, find all customer_ids in Oracle that have no MongoDB document
    and emit synthetic CDC events so the consumer hydrates them.
    """
    from db.ods import async_get_all_customer_ids
    from agent.db import query as oracle_query

    existing = set(await async_get_all_customer_ids())
    all_ids_rows = await asyncio.to_thread(
        oracle_query,
        "SELECT CUSTOMER_ID FROM CUSTOMER ORDER BY CUSTOMER_ID",
        {},
    )
    all_ids = {r["customer_id"] for r in all_ids_rows}
    missing = all_ids - existing

    if not missing:
        logger.info("Bulk backfill: all %d customers already hydrated", len(all_ids))
        return

    logger.info("Bulk backfill: hydrating %d missing customer(s)", len(missing))
    for cid in missing:
        event = OracleCdcEvent(
            op="INSERT",
            table_name="CUSTOMER",
            primary_key={"CUSTOMER_ID": cid},
            after={"CUSTOMER_ID": cid},
        )
        await publish(producer, TOPIC_CDC_ALL_CHANGES, event.model_dump(), key=str(cid))

    logger.info("Bulk backfill: %d CDC events published", len(missing))


# ── Entry point ────────────────────────────────────────────────────────────────

async def run_hydrator() -> None:
    await ensure_ods_indexes()

    producer = await create_producer()
    try:
        # Backfill any customers not yet in ODS, then start the continuous loop
        await _bulk_backfill(producer)
        await asyncio.gather(
            _run_cdc_producer(producer),
            _run_hydrator_consumer(),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [hydrator] %(levelname)s %(message)s",
    )
    asyncio.run(run_hydrator())
