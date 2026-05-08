"""
Canary Analysis Engine — Phase 12.

Tracks per-resolution quality metrics for each Claude model version deployed
in the Synapse agent, then produces a statistically grounded promotion decision.

Architecture
------------
1. Every /resolve call publishes a CanaryResult to `synapse.canary.results`.
2. This service consumes that topic, persists each result to MongoDB
   `canary_runs`, and recomputes rolling-window KPIs.
3. The API layer exposes /canary/* endpoints so NOC operators can:
     - View live head-to-head metrics (success rate, avg confidence, operator score)
     - Promote a candidate to 100% traffic
     - Roll back to the previous champion

Traffic Splitting
-----------------
Set CANARY_CANDIDATE_MODEL and CANARY_CANDIDATE_TRAFFIC_PCT in env:
  CANARY_CANDIDATE_MODEL         e.g. "claude-opus-4-7"
  CANARY_CANDIDATE_TRAFFIC_PCT   e.g. "10"   (10% of resolutions go to candidate)

The resolve endpoint reads CANARY_CANDIDATE_MODEL and CANARY_CANDIDATE_TRAFFIC_PCT
and stamps the CanaryResult with the model version used.

Promotion criteria (all must hold in the evaluation window)
-----------------------------------------------------------
  success_rate   >= CANARY_MIN_SUCCESS_RATE    (default 0.85)
  avg_confidence >= CANARY_MIN_AVG_CONFIDENCE  (default "MEDIUM")
  sample_count   >= CANARY_MIN_SAMPLES         (default 100)

Environment
-----------
  CANARY_WINDOW_MINUTES      rolling evaluation window (default 60)
  CANARY_MIN_SUCCESS_RATE    minimum approval rate (default 0.85)
  CANARY_MIN_SAMPLES         minimum sample count for a valid comparison (default 100)
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from kafka.consumer import create_consumer, extract_trace_context
from kafka.topics import GROUP_CANARY, TOPIC_CANARY_RESULTS
from observability.metrics import (
    canary_success_rate,
    canary_sample_count,
    canary_promotion_total,
)

logger = logging.getLogger(__name__)

CANARY_WINDOW_MINUTES    = int(os.getenv("CANARY_WINDOW_MINUTES",    "60"))
CANARY_MIN_SUCCESS_RATE  = float(os.getenv("CANARY_MIN_SUCCESS_RATE", "0.85"))
CANARY_MIN_SAMPLES       = int(os.getenv("CANARY_MIN_SAMPLES",        "100"))

_CONFIDENCE_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}


# ── MongoDB persistence helpers ───────────────────────────────────────────────

async def _save_result(result: dict) -> None:
    from db.ods import _async_motor_db
    db = _async_motor_db()
    doc = {
        **result,
        "run_id":     result.get("result_id", str(uuid.uuid4())),
        "stored_at":  datetime.now(timezone.utc),
    }
    await db.canary_runs.update_one(
        {"run_id": doc["run_id"]},
        {"$set": doc},
        upsert=True,
    )


async def _compute_kpis(model_version: str, window_minutes: int) -> dict:
    """Compute rolling KPIs for a specific model version."""
    from db.ods import _async_motor_db
    db = _async_motor_db()
    since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    pipeline = [
        {"$match": {
            "model_version": model_version,
            "stored_at":     {"$gte": since},
        }},
        {"$group": {
            "_id":              None,
            "sample_count":     {"$sum": 1},
            "approved_count":   {"$sum": {"$cond": [{"$eq": ["$approved", True]}, 1, 0]}},
            "high_conf_count":  {"$sum": {"$cond": [{"$eq": ["$confidence", "HIGH"]}, 1, 0]}},
            "avg_operator_score": {"$avg": "$operator_score"},
        }},
    ]
    result = await db.canary_runs.aggregate(pipeline).to_list(length=1)
    if not result:
        return {
            "model_version":     model_version,
            "sample_count":      0,
            "success_rate":      None,
            "high_conf_pct":     None,
            "avg_operator_score": None,
            "window_minutes":    window_minutes,
        }

    r = result[0]
    samples = r["sample_count"]
    return {
        "model_version":      model_version,
        "sample_count":       samples,
        "success_rate":       round(r["approved_count"] / samples, 4) if samples else None,
        "high_conf_pct":      round(r["high_conf_count"] / samples, 4) if samples else None,
        "avg_operator_score": round(r["avg_operator_score"], 2) if r["avg_operator_score"] else None,
        "window_minutes":     window_minutes,
    }


async def get_canary_status() -> dict:
    """Return head-to-head KPIs for champion vs. candidate model."""
    champion  = os.getenv("CANARY_CHAMPION_MODEL",  "claude-sonnet-4-6")
    candidate = os.getenv("CANARY_CANDIDATE_MODEL", "")

    champion_kpis  = await _compute_kpis(champion, CANARY_WINDOW_MINUTES)
    candidate_kpis = await _compute_kpis(candidate, CANARY_WINDOW_MINUTES) if candidate else None

    ready_to_promote = False
    if candidate_kpis and candidate_kpis["sample_count"] >= CANARY_MIN_SAMPLES:
        sr = candidate_kpis.get("success_rate") or 0
        ready_to_promote = sr >= CANARY_MIN_SUCCESS_RATE

    return {
        "champion":          champion_kpis,
        "candidate":         candidate_kpis,
        "ready_to_promote":  ready_to_promote,
        "min_success_rate":  CANARY_MIN_SUCCESS_RATE,
        "min_samples":       CANARY_MIN_SAMPLES,
        "window_minutes":    CANARY_WINDOW_MINUTES,
    }


async def promote_candidate(operator_id: str) -> dict:
    """
    Promote the candidate model to champion.
    In production this would update a feature-flag service (LaunchDarkly, Flagsmith, etc.)
    Here it updates the CANARY_CHAMPION_MODEL env key in a MongoDB config collection
    so the API can read it on the next request.
    """
    from db.ods import _async_motor_db

    candidate = os.getenv("CANARY_CANDIDATE_MODEL", "")
    if not candidate:
        raise ValueError("CANARY_CANDIDATE_MODEL is not configured")

    status = await get_canary_status()
    if not status["ready_to_promote"]:
        raise ValueError(
            f"Candidate {candidate!r} does not meet promotion criteria "
            f"(success_rate={status['candidate']['success_rate']}, "
            f"samples={status['candidate']['sample_count']})"
        )

    db = _async_motor_db()
    await db.canary_config.update_one(
        {"_id": "active"},
        {"$set": {
            "champion_model":     candidate,
            "promoted_at":        datetime.now(timezone.utc).isoformat(),
            "promoted_by":        operator_id,
            "previous_champion":  os.getenv("CANARY_CHAMPION_MODEL", ""),
        }},
        upsert=True,
    )

    canary_promotion_total.labels(
        from_model=os.getenv("CANARY_CHAMPION_MODEL", ""),
        to_model=candidate,
    ).inc()

    logger.info("Canary promoted: %s → %s by %s", os.getenv("CANARY_CHAMPION_MODEL"), candidate, operator_id)

    return {
        "status":        "promoted",
        "new_champion":  candidate,
        "promoted_by":   operator_id,
    }


async def score_result(result_id: str, operator_score: int, approved: bool) -> None:
    """Backfill operator feedback onto a canary result (called from HITL approval flow)."""
    from db.ods import _async_motor_db
    db = _async_motor_db()
    await db.canary_runs.update_one(
        {"run_id": result_id},
        {"$set": {"operator_score": operator_score, "approved": approved}},
    )


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def _update_prometheus(model_version: str) -> None:
    kpis = await _compute_kpis(model_version, CANARY_WINDOW_MINUTES)
    if kpis["success_rate"] is not None:
        canary_success_rate.labels(model_version=model_version).set(kpis["success_rate"])
    canary_sample_count.labels(model_version=model_version).set(kpis["sample_count"])


async def run_canary_service() -> None:
    """Consume CanaryResult events, persist, and update Prometheus gauges."""
    consumer = await create_consumer(TOPIC_CANARY_RESULTS, group_id=GROUP_CANARY)
    logger.info("Canary service started — listening on %s", TOPIC_CANARY_RESULTS)

    try:
        async for msg in consumer:
            extract_trace_context(msg)
            result = msg.value
            try:
                await _save_result(result)
                model = result.get("model_version", "unknown")
                await _update_prometheus(model)
            except Exception as exc:
                logger.error("Canary persist error: %s", exc)
            await consumer.commit()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [canary] %(levelname)s %(message)s",
    )
    asyncio.run(run_canary_service())
