"""
Predictive Hydration Engine — Phase 12.

Goal: reduce first-call reasoning latency by proactively hydrating subscriber
documents *before* the first customer contacts the call centre.

Mechanism
---------
This service subscribes to the `synapse.system.events` topic and listens for
HIGH_SIGNAL events (outages, mass-fallout, CRITICAL severity spikes).  When one
is detected, it enumerates every subscriber in the affected OpCo and publishes
synthetic CDC events so the Hydrator consumer re-hydrates them immediately.

The net effect: by the time the first customer calls about the outage, their
Golden JSON document in MongoDB is already fresh — zero extra latency on the
reasoning path.

High-Signal event schema (published by CDC producer when anomalies are detected)
{
    "event_type":     "HIGH_SIGNAL",
    "signal_type":    "OUTAGE" | "MASS_FALLOUT" | "CRITICAL_SPIKE",
    "opco_id":        "VODAFONE_UK",
    "affected_count": 1420,          # estimated
    "sector":         "HLR-East",    # optional
    "ts":             "2026-05-08T12:00:00+00:00",
}

Environment
-----------
  PREFETCH_CONCURRENCY    max CDC events published per second (default: 50)
  PREFETCH_MAX_CUSTOMERS  cap on prefetch per signal (default: 2000)
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from kafka.consumer import create_consumer, extract_trace_context
from kafka.producer import create_producer, publish
from kafka.schemas import OracleCdcEvent
from kafka.topics import (
    GROUP_PREFETCH,
    TOPIC_CDC_ALL_CHANGES,
    TOPIC_SYSTEM_EVENTS,
)
from db.ods import async_get_customer_ids_by_opco, async_get_subscriber, promote_from_cold
from observability.metrics import (
    prefetch_signals_received_total,
    prefetch_customers_queued_total,
    cold_promotions_total,
)

logger = logging.getLogger(__name__)

PREFETCH_CONCURRENCY   = int(os.getenv("PREFETCH_CONCURRENCY",   "50"))
PREFETCH_MAX_CUSTOMERS = int(os.getenv("PREFETCH_MAX_CUSTOMERS", "2000"))

_HIGH_SIGNAL_TYPES = {"OUTAGE", "MASS_FALLOUT", "CRITICAL_SPIKE"}


async def _handle_high_signal(event: dict, producer) -> None:
    """Fan-out synthetic CDC events for every subscriber in the affected OpCo."""
    signal_type   = event.get("signal_type", "UNKNOWN")
    opco_id       = event.get("opco_id", "DEFAULT")
    sector        = event.get("sector")

    prefetch_signals_received_total.labels(
        signal_type=signal_type, opco_id=opco_id
    ).inc()

    logger.info(
        "Predictive prefetch triggered: signal=%s opco=%s sector=%s",
        signal_type, opco_id, sector,
    )

    customer_ids = await async_get_customer_ids_by_opco(
        opco_id, limit=PREFETCH_MAX_CUSTOMERS
    )
    if not customer_ids:
        logger.info("Prefetch: no hydrated customers found for opco=%s", opco_id)
        return

    queued    = 0
    promoted  = 0
    semaphore = asyncio.Semaphore(PREFETCH_CONCURRENCY)

    async def _publish_one(cid: int) -> None:
        nonlocal queued, promoted
        async with semaphore:
            # Phase 13: if this customer has a cold stub, promote to warm FIRST.
            # The Hydrator is the warmth-tier arbiter — the act of HIGH_SIGNAL
            # prefetch IS the promotion event for cold subscribers.
            stub = await async_get_subscriber(cid)
            if stub and stub.get("storage_tier") == "cold":
                warmed = await promote_from_cold(cid, opco_id, reason="high_signal_prefetch")
                if not warmed:
                    # Not in cold store — fall back to Oracle source (force-sync style)
                    import asyncio as _asyncio
                    from db.ods import build_subscriber_doc, async_upsert_subscriber
                    doc = await _asyncio.to_thread(build_subscriber_doc, cid)
                    if doc:
                        doc["opco_id"] = opco_id
                        await async_upsert_subscriber(doc)
                promoted += 1

            evt = OracleCdcEvent(
                op="UPDATE",
                table_name="CUSTOMER",
                primary_key={"CUSTOMER_ID": cid},
                after={"CUSTOMER_ID": cid},
                metadata={"prefetch_signal": signal_type, "opco_id": opco_id},
            )
            await publish(
                producer,
                TOPIC_CDC_ALL_CHANGES,
                evt.model_dump(),
                key=str(cid),
            )
            queued += 1

    await asyncio.gather(*[_publish_one(cid) for cid in customer_ids])

    prefetch_customers_queued_total.labels(
        signal_type=signal_type, opco_id=opco_id
    ).inc(queued)

    logger.info(
        "Predictive prefetch: queued=%d CDC events cold_promoted=%d opco=%s signal=%s",
        queued, promoted, opco_id, signal_type,
    )


async def run_prefetch_service() -> None:
    """
    Long-running coroutine: consume system events and trigger prefetch on HIGH_SIGNAL.
    Run as a separate Docker service (python -m worker.prefetch).
    """
    producer = await create_producer()
    consumer = await create_consumer(TOPIC_SYSTEM_EVENTS, group_id=GROUP_PREFETCH)
    logger.info("Prefetch service started — listening on %s", TOPIC_SYSTEM_EVENTS)

    try:
        async for msg in consumer:
            extract_trace_context(msg)
            event = msg.value  # dict via consumer's JSON deserializer

            event_type  = event.get("event_type", "")
            signal_type = event.get("signal_type", "")

            if event_type == "HIGH_SIGNAL" and signal_type in _HIGH_SIGNAL_TYPES:
                try:
                    await _handle_high_signal(event, producer)
                except Exception as exc:
                    logger.error("Prefetch handler error: %s", exc)

            await consumer.commit()
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [prefetch] %(levelname)s %(message)s",
    )
    asyncio.run(run_prefetch_service())
