"""
Async Kafka consumer helpers for Synapse Phase 9.

Each long-running consumer service (hydrator, thinker, oracle_adapter) calls
create_consumer() once at startup, then iterates over messages with:

    async for msg in consumer:
        payload = json.loads(msg.value)
        ...

Commit strategy: auto-commit is disabled. Services manually commit after
successful processing so that a crash mid-handler causes the message to be
re-delivered (at-least-once semantics).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


async def create_consumer(
    *topics: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
) -> AIOKafkaConsumer:
    """
    Start and return an AIOKafkaConsumer subscribed to the given topics.

    Manual commit is enabled (enable_auto_commit=False). Callers must call
    await consumer.commit() after successful message processing.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
    )
    await consumer.start()
    logger.info(
        "Kafka consumer started group=%s topics=%s", group_id, list(topics)
    )
    return consumer


def parse_message(msg) -> dict[str, Any]:
    """Return the deserialized message value (already a dict after our deserializer)."""
    return msg.value if isinstance(msg.value, dict) else json.loads(msg.value)


def extract_trace_context(msg) -> None:
    """
    Phase 10 — resume the OTel trace from a consumed Kafka message's headers.
    Call at the top of a message handler to link consumer spans to the producer's trace.
    No-op when tracing is not configured or headers are absent.
    """
    try:
        from opentelemetry import propagate, context as otel_context
        carrier = {
            k: v.decode() if isinstance(v, bytes) else v
            for k, v in (msg.headers or [])
        }
        if carrier:
            ctx = propagate.extract(carrier)
            otel_context.attach(ctx)
    except Exception:
        pass
