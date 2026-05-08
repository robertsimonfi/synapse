"""
Async Kafka producer for Synapse Phase 9 / Phase 10.

Usage (fire-and-forget in async context):
    async with get_producer() as producer:
        await publish(producer, TOPIC_AI_ORCHESTRATION, task.model_dump())

Usage (singleton lifecycle managed by caller):
    producer = await create_producer()
    await publish(producer, TOPIC_WRITEBACK, cmd.model_dump())
    await producer.stop()

Phase 10 — Trace injection:
    Every message carries a W3C `traceparent` header injected from the current
    OTel span context. This lets a NOC engineer click "Trace" in the UI and see
    the exact millisecond an Oracle update reached the AI brain.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from kafka.topics import TOPIC_CONFIGS

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _build_trace_headers() -> list[tuple[str, bytes]]:
    """
    Inject the current OTel span context into W3C traceparent/tracestate headers.
    Returns an empty list when tracing is not configured (no-op safe).
    """
    try:
        from opentelemetry import propagate, trace
        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        return [(k, v.encode()) for k, v in carrier.items()]
    except Exception:
        return []


def current_trace_id() -> str | None:
    """Return the hex trace_id of the active OTel span, or None if not in a trace."""
    try:
        from opentelemetry import trace
        ctx = trace.get_current_span().get_span_context()
        if ctx.is_valid:
            return format(ctx.trace_id, "032x")
    except Exception:
        pass
    return None


async def _ensure_topics(bootstrap: str) -> None:
    """Create topics that don't yet exist. Safe to call concurrently — uses CREATE_TOPIC_IF_NOT_EXISTS semantics."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        await admin.start()
        existing = set(await admin.list_topics())
        new_topics = [
            NewTopic(
                name=name,
                num_partitions=cfg["num_partitions"],
                replication_factor=cfg["replication_factor"],
            )
            for name, cfg in TOPIC_CONFIGS.items()
            if name not in existing
        ]
        if new_topics:
            await admin.create_topics(new_topics)
            for t in new_topics:
                logger.info("Kafka topic created: %s", t.name)
    except Exception as exc:
        logger.warning("Topic creation skipped (%s) — broker may not be ready yet", exc)
    finally:
        try:
            await admin.close()
        except Exception:
            pass


async def create_producer(ensure_topics: bool = True) -> AIOKafkaProducer:
    """Start and return a long-lived AIOKafkaProducer instance."""
    if ensure_topics:
        await _ensure_topics(BOOTSTRAP_SERVERS)

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks="all",                  # wait for all in-sync replicas
        enable_idempotence=True,     # exactly-once producer semantics
        compression_type="lz4",
    )
    await producer.start()
    return producer


@asynccontextmanager
async def get_producer(ensure_topics: bool = True):
    """Context manager that yields a started producer and stops it on exit."""
    producer = await create_producer(ensure_topics=ensure_topics)
    try:
        yield producer
    finally:
        await producer.stop()


async def publish(
    producer: AIOKafkaProducer,
    topic: str,
    value: dict[str, Any],
    key: str | None = None,
) -> None:
    """Serialize and publish a dict payload to the given topic with OTel trace headers."""
    headers = _build_trace_headers()
    # Stamp trace_id into the payload itself so consumers can surface it in the UI
    # without requiring header parsing (useful for MongoDB storage).
    tid = current_trace_id()
    if tid and "trace_id" in value:
        value = {**value, "trace_id": tid}
    await producer.send_and_wait(topic, value=value, key=key, headers=headers)
    logger.debug("Published to %s key=%s trace_id=%s", topic, key, tid)
