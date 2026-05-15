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

import asyncio
import json
import logging
import os
from typing import Any

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def _kafka_tls_kwargs() -> dict:
    """Return SSL kwargs for AIOKafkaConsumer when mTLS is configured (Phase 12)."""
    try:
        from infra.secrets import get_kafka_ssl_context
        import os as _os
        ctx = get_kafka_ssl_context()
        if ctx:
            return {
                "ssl_context":       ctx,
                "security_protocol": _os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
            }
    except Exception as exc:
        logger.debug("Kafka TLS config unavailable: %s", exc)
    return {}


async def create_consumer(
    *topics: str,
    group_id: str,
    auto_offset_reset: str = "earliest",
    max_retries: int = 8,
) -> AIOKafkaConsumer:
    """
    Start and return an AIOKafkaConsumer subscribed to the given topics.

    Manual commit is enabled (enable_auto_commit=False). Callers must call
    await consumer.commit() after successful message processing.

    Retries with exponential backoff (1s → 2s → 4s → … → 60s cap) so that
    workers launched before Kafka is ready don't die permanently on Errno 111.
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
        **_kafka_tls_kwargs(),
    )

    for attempt in range(1, max_retries + 1):
        try:
            await consumer.start()
            logger.info(
                "Kafka consumer started group=%s topics=%s", group_id, list(topics)
            )
            return consumer
        except Exception as exc:
            if attempt >= max_retries:
                logger.error(
                    "Kafka consumer failed after %d attempts — giving up: %s",
                    max_retries, exc,
                )
                raise
            backoff = min(2 ** (attempt - 1), 60)
            logger.warning(
                "Kafka consumer connect failed (attempt %d/%d): %s — retrying in %ds",
                attempt, max_retries, exc, backoff,
            )
            await asyncio.sleep(backoff)


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
