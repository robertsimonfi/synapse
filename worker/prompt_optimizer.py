"""
Phase 15 Step 1 — Feedback-to-Prompt Optimizer.

Analyzes the operator_corrections MongoDB collection and identifies systematic
patterns where the AI consistently makes the same type of mistake. When a
pattern crosses the confidence threshold it persists a PromptSuggestion
document and publishes a suggestion_ready event to the NOC WebSocket so
operators can review and apply the change interactively.

Pattern detection logic:
  - Group corrections by (original_action_type, low quality_score) where score <= 2
  - If a group accumulates >= MIN_CORRECTIONS samples the cluster is "ripe"
  - Ask Claude to draft a targeted system-prompt addendum that would prevent the
    class of error, using the correction notes as training signal
  - Persist the suggestion; do NOT auto-apply (human must approve)

Scheduling: run every 4 hours via APScheduler (wired in worker/archiver.py startup).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any

import anthropic
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)

MONGODB_URI  = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB   = os.getenv("MONGODB_DB", "synapse")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL_OPTIMIZER", "claude-haiku-4-5-20251001")
MIN_CORRECTIONS = int(os.getenv("OPTIMIZER_MIN_CORRECTIONS", "5"))
MAX_SCORE_THRESHOLD = int(os.getenv("OPTIMIZER_MAX_SCORE", "2"))


async def _get_db():
    client = AsyncIOMotorClient(MONGODB_URI)
    return client[MONGODB_DB]


async def _fetch_low_score_corrections(db) -> list[dict[str, Any]]:
    cursor = db.operator_corrections.find(
        {"quality_score": {"$lte": MAX_SCORE_THRESHOLD}},
        {"_id": 0, "original_action_type": 1, "proposed_action": 1,
         "correct_action": 1, "feedback_notes": 1, "quality_score": 1,
         "opco_id": 1, "submitted_at": 1},
    ).sort("submitted_at", -1).limit(500)
    return await cursor.to_list(length=500)


def _cluster_corrections(corrections: list[dict]) -> dict[str, list[dict]]:
    clusters: dict[str, list[dict]] = defaultdict(list)
    for c in corrections:
        key = c.get("original_action_type", "UNKNOWN")
        clusters[key].append(c)
    return {k: v for k, v in clusters.items() if len(v) >= MIN_CORRECTIONS}


async def _draft_suggestion(action_type: str, samples: list[dict]) -> str:
    client = anthropic.AsyncAnthropic()

    sample_text = "\n".join(
        f"- Proposed: {s.get('proposed_action', '')} | "
        f"Correct: {s.get('correct_action', 'N/A')} | "
        f"Notes: {s.get('feedback_notes', '')}"
        for s in samples[:10]
    )

    message = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=512,
        system=(
            "You are a prompt-engineering assistant for a Telco CRM AI system. "
            "Given a set of operator corrections, draft a concise system-prompt "
            "addendum (≤150 words) that would guide the AI to avoid the documented "
            "mistakes. Write only the addendum text — no preamble."
        ),
        messages=[
            {
                "role": "user",
                "content": (
                    f"Action type: {action_type}\n"
                    f"Sample operator corrections (most recent first):\n{sample_text}\n\n"
                    "Draft the system-prompt addendum."
                ),
            }
        ],
    )
    return message.content[0].text.strip()


async def _already_suggested(db, action_type: str) -> bool:
    existing = await db.prompt_suggestions.find_one(
        {"action_type": action_type, "status": "PENDING"}
    )
    return existing is not None


async def run_optimizer() -> int:
    """Analyse corrections and write PromptSuggestion documents. Returns count of new suggestions."""
    db = await _get_db()
    corrections = await _fetch_low_score_corrections(db)
    if not corrections:
        logger.info("prompt_optimizer: no low-score corrections found")
        return 0

    clusters = _cluster_corrections(corrections)
    new_count = 0

    for action_type, samples in clusters.items():
        if await _already_suggested(db, action_type):
            logger.info("prompt_optimizer: pending suggestion already exists for %s", action_type)
            continue

        logger.info(
            "prompt_optimizer: %d corrections for %s — drafting suggestion",
            len(samples), action_type,
        )
        addendum = await _draft_suggestion(action_type, samples)

        suggestion = {
            "action_type":      action_type,
            "sample_count":     len(samples),
            "avg_score":        sum(s["quality_score"] for s in samples) / len(samples),
            "addendum":         addendum,
            "status":           "PENDING",   # PENDING → APPLIED | REJECTED
            "created_at":       datetime.utcnow().isoformat(),
            "applied_at":       None,
            "applied_by":       None,
        }
        await db.prompt_suggestions.insert_one(suggestion)
        new_count += 1
        logger.info("prompt_optimizer: suggestion created for action_type=%s", action_type)

    return new_count


async def apply_suggestion(suggestion_id: str, operator_id: str, db=None) -> bool:
    """
    Mark a pending suggestion as APPLIED. The addendum is appended to
    agent/prompt_addenda.json which build_system_prompt() reads at startup.
    Returns True on success.
    """
    from bson import ObjectId
    if db is None:
        db = await _get_db()

    doc = await db.prompt_suggestions.find_one({"_id": ObjectId(suggestion_id)})
    if not doc or doc["status"] != "PENDING":
        return False

    addenda_path = os.path.join(os.path.dirname(__file__), "..", "agent", "prompt_addenda.json")
    try:
        with open(addenda_path) as f:
            addenda: list[dict] = json.load(f)
    except FileNotFoundError:
        addenda = []

    addenda.append({
        "action_type": doc["action_type"],
        "addendum":    doc["addendum"],
        "applied_at":  datetime.utcnow().isoformat(),
        "applied_by":  operator_id,
    })
    with open(addenda_path, "w") as f:
        json.dump(addenda, f, indent=2)

    await db.prompt_suggestions.update_one(
        {"_id": ObjectId(suggestion_id)},
        {"$set": {"status": "APPLIED", "applied_at": datetime.utcnow().isoformat(), "applied_by": operator_id}},
    )
    logger.info("prompt_optimizer: suggestion %s applied by %s", suggestion_id, operator_id)
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = asyncio.run(run_optimizer())
    print(f"prompt_optimizer: {count} new suggestion(s) created")
