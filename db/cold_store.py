"""
Cold Storage adapter — Phase 13 Tiered Storage.

Hot ODS:   MongoDB `subscribers` collection (sharded, RAM-resident)
Cold Store: MongoDB `cold_subscribers` collection (dev) or S3 (production)

Documents migrate cold when a subscriber has had no event or query for
ARCHIVE_INACTIVE_DAYS (default: 30).  The archiver replaces the full Golden
JSON document with a lightweight stub (storage_tier="cold") that preserves
the shard key, so the opco fan-out list and shard locality both remain intact.

Warm-up path (driven by worker/prefetch.py):
  When the Predictive Hydrator sees a HIGH_SIGNAL event it reads the opco's
  full customer list (stubs included), detects cold stubs, and calls
  promote_from_cold() to restore the full document before issuing the
  synthetic CDC fan-out.  The Hydrator is therefore the warmth-tier arbiter:
  the act of a HIGH_SIGNAL prefetch IS the promotion event.

Environment
-----------
  COLD_STORE_BACKEND      "mongodb" | "s3"  (default: mongodb)
  COLD_STORE_S3_BUCKET    S3 bucket name    (required when backend=s3)
  COLD_STORE_S3_PREFIX    key prefix        (default: "cold/")
  COLD_STORE_S3_REGION    AWS region        (default: us-east-1)
  ARCHIVE_INACTIVE_DAYS   days before archival (default: 30)
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)

COLD_STORE_BACKEND    = os.getenv("COLD_STORE_BACKEND", "mongodb")
ARCHIVE_INACTIVE_DAYS = int(os.getenv("ARCHIVE_INACTIVE_DAYS", "30"))


class ColdStoreBackend(ABC):
    @abstractmethod
    async def archive(self, doc: dict) -> None: ...

    @abstractmethod
    async def retrieve(self, customer_id: int, opco_id: str) -> Optional[dict]: ...

    @abstractmethod
    async def delete(self, customer_id: int, opco_id: str) -> None: ...


# ── MongoDB cold store (dev / fallback) ────────────────────────────────────────

class MongoColdStore(ColdStoreBackend):
    """Dev-friendly cold store using a separate `cold_subscribers` collection."""

    @staticmethod
    def _db():
        from db.ods import _async_motor_db
        return _async_motor_db()

    async def archive(self, doc: dict) -> None:
        from datetime import datetime, timezone
        db = self._db()
        await db.cold_subscribers.replace_one(
            {"customer_id": doc["customer_id"]},
            {**doc, "archived_at": datetime.now(timezone.utc).isoformat()},
            upsert=True,
        )
        logger.debug("MongoColdStore: archived customer_id=%s", doc.get("customer_id"))

    async def retrieve(self, customer_id: int, opco_id: str) -> Optional[dict]:
        db = self._db()
        return await db.cold_subscribers.find_one(
            {"customer_id": customer_id, "opco_id": opco_id},
            {"_id": 0},
        )

    async def delete(self, customer_id: int, opco_id: str) -> None:
        db = self._db()
        await db.cold_subscribers.delete_one(
            {"customer_id": customer_id, "opco_id": opco_id}
        )


# ── S3 cold store (production) ─────────────────────────────────────────────────

class S3ColdStore(ColdStoreBackend):
    """
    Production cold store — gzip-compressed JSON objects in S3.

    Key format: {prefix}{opco_id}/{customer_id}.json.gz
    """

    def __init__(self) -> None:
        import boto3  # noqa: PLC0415
        self._bucket = os.environ["COLD_STORE_S3_BUCKET"]
        self._prefix = os.getenv("COLD_STORE_S3_PREFIX", "cold/")
        self._client = boto3.client(
            "s3",
            region_name=os.getenv("COLD_STORE_S3_REGION", "us-east-1"),
        )
        # Cache the NoSuchKey exception class so we don't look it up each call
        self._no_such_key = self._client.exceptions.NoSuchKey

    def _key(self, opco_id: str, customer_id: int) -> str:
        return f"{self._prefix}{opco_id}/{customer_id}.json.gz"

    async def archive(self, doc: dict) -> None:
        import asyncio
        key = self._key(doc["opco_id"], doc["customer_id"])

        def _put() -> None:
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                gz.write(json.dumps(doc, default=str).encode())
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=buf.getvalue(),
                ContentType="application/json",
                ContentEncoding="gzip",
            )

        await asyncio.to_thread(_put)
        logger.info("S3ColdStore: archived s3://%s/%s", self._bucket, key)

    async def retrieve(self, customer_id: int, opco_id: str) -> Optional[dict]:
        import asyncio
        key = self._key(opco_id, customer_id)

        def _get() -> Optional[dict]:
            try:
                resp = self._client.get_object(Bucket=self._bucket, Key=key)
                with gzip.GzipFile(fileobj=resp["Body"]) as gz:
                    return json.loads(gz.read())
            except self._no_such_key:
                return None
            except Exception as exc:
                logger.warning("S3ColdStore.retrieve failed key=%s: %s", key, exc)
                return None

        return await asyncio.to_thread(_get)

    async def delete(self, customer_id: int, opco_id: str) -> None:
        import asyncio
        key = self._key(opco_id, customer_id)
        await asyncio.to_thread(
            self._client.delete_object, Bucket=self._bucket, Key=key
        )
        logger.debug("S3ColdStore: deleted s3://%s/%s", self._bucket, key)


# ── Factory ────────────────────────────────────────────────────────────────────

_cold_store: Optional[ColdStoreBackend] = None


def get_cold_store() -> ColdStoreBackend:
    """Return the configured cold store singleton (lazy init)."""
    global _cold_store
    if _cold_store is None:
        if COLD_STORE_BACKEND == "s3":
            _cold_store = S3ColdStore()
            logger.info("Cold store backend: S3 bucket=%s", os.getenv("COLD_STORE_S3_BUCKET"))
        else:
            _cold_store = MongoColdStore()
            logger.info("Cold store backend: MongoDB cold_subscribers collection")
    return _cold_store
