"""
Tiered Storage Archiver — Phase 13.

Nightly job that scans the hot ODS for subscriber documents inactive for more
than ARCHIVE_INACTIVE_DAYS (no query or event touch on `last_accessed_at`).
Inactive documents are:

  1. Written to the Cold Store (S3 or MongoDB cold_subscribers collection)
  2. Replaced in the hot ODS with a lightweight stub:
       { customer_id, opco_id, storage_tier: "cold", archived_at }

Why keep a stub?
  The stub preserves the opco_id shard key so that:
    • Shard locality is maintained (no chunk rebalancing needed)
    • async_get_customer_ids_by_opco() still returns cold subscribers,
      enabling the Predictive Hydrator to warm them during HIGH_SIGNAL events
    • Per-opco Prometheus accounting remains accurate

Re-warm path:
  worker/prefetch.py detects "cold" stubs during HIGH_SIGNAL fan-out and
  calls db.ods.promote_from_cold() before issuing the synthetic CDC event.

Architect's answer to the warmth-tier question:
  The Predictive Hydrator IS the warmth-tier decision-maker.  The presence
  of a storage_tier="cold" stub is the definitive signal.  A HIGH_SIGNAL
  prefetch promotes cold subscribers FIRST, then issues the CDC fan-out —
  so the act of prefetch is the promotion event itself.

Environment
-----------
  ARCHIVE_INACTIVE_DAYS    days before archival (default: 30)
  ARCHIVER_BATCH_SIZE      documents per pass (default: 500)
  ARCHIVER_CRON_HOUR       UTC hour to run nightly (default: 2)
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db.cold_store import get_cold_store
from db.ods import _async_motor_db, ensure_ods_indexes

logger = logging.getLogger(__name__)

ARCHIVE_INACTIVE_DAYS = int(os.getenv("ARCHIVE_INACTIVE_DAYS", "30"))
ARCHIVER_BATCH_SIZE   = int(os.getenv("ARCHIVER_BATCH_SIZE",   "500"))
ARCHIVER_CRON_HOUR    = int(os.getenv("ARCHIVER_CRON_HOUR",    "2"))


async def run_archival_pass() -> int:
    """
    Archive warm subscribers inactive for > ARCHIVE_INACTIVE_DAYS.

    Returns count of documents archived this pass.  Runs at most
    ARCHIVER_BATCH_SIZE documents per invocation to bound execution time.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=ARCHIVE_INACTIVE_DAYS)
    cold   = get_cold_store()
    db     = _async_motor_db()

    # Only consider documents that are warm (not already cold stubs)
    query = {
        "storage_tier": {"$ne": "cold"},
        "last_accessed_at": {"$lt": cutoff.isoformat(), "$exists": True},
    }

    archived = 0
    cursor   = db.subscribers.find(query, limit=ARCHIVER_BATCH_SIZE)

    async for doc in cursor:
        customer_id = doc.get("customer_id")
        opco_id     = doc.get("opco_id", "DEFAULT")
        doc.pop("_id", None)

        try:
            # 1. Write full document to cold store
            await cold.archive(doc)

            # 2. Replace hot document with a lightweight stub that keeps
            #    the shard key intact (opco_id + customer_id).
            stub = {
                "customer_id":  customer_id,
                "opco_id":      opco_id,
                "storage_tier": "cold",
                "archived_at":  datetime.now(timezone.utc).isoformat(),
            }
            await db.subscribers.replace_one({"customer_id": customer_id}, stub)

            archived += 1

            from observability.metrics import archived_documents_total
            archived_documents_total.labels(opco_id=opco_id).inc()

        except Exception as exc:
            logger.error("Archiver: failed customer_id=%s opco=%s: %s", customer_id, opco_id, exc)

    # Refresh cold-stub count gauge
    cold_count = await db.subscribers.count_documents({"storage_tier": "cold"})
    from observability.metrics import cold_store_documents_gauge
    cold_store_documents_gauge.set(cold_count)

    if archived:
        logger.info(
            "Archival pass: archived=%d cutoff=%s cold_total=%d",
            archived, cutoff.date().isoformat(), cold_count,
        )

    return archived


async def run_archiver() -> None:
    """
    Long-running archiver service with nightly APScheduler cron.
    Run as a standalone Docker service: python -m worker.archiver
    """
    await ensure_ods_indexes()
    logger.info(
        "Archiver started — nightly at %02d:00 UTC, inactive_threshold=%dd batch=%d",
        ARCHIVER_CRON_HOUR, ARCHIVE_INACTIVE_DAYS, ARCHIVER_BATCH_SIZE,
    )

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_archival_pass,
        CronTrigger(hour=ARCHIVER_CRON_HOUR, timezone="UTC"),
        name="tiered_storage_archival",
        max_instances=1,
        misfire_grace_time=3600,
    )
    scheduler.start()

    # Run one pass immediately on startup so stale docs don't wait 24 h
    await run_archival_pass()

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [archiver] %(levelname)s %(message)s",
    )
    asyncio.run(run_archiver())
