"""
Async Kafka producer for Synapse Phase 9.

Usage (fire-and-forget in async context):
    async with get_producer() as producer:
        await publish(producer, TOPIC_AI_ORCHESTRATION, task.model_dump())

Usage (singleton lifecycle managed by caller):
    producer = await create_producer()
    await publish(producer, TOPIC_WRITEBACK, cmd.model_dump())
    await producer.stop()
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
    """Serialize and publish a dict payload to the given topic."""
    await producer.send_and_wait(topic, value=value, key=key)
    logger.debug("Published to %s key=%s", topic, key)
