"""
Phase 14 — Sentiment Analysis Worker.

Consumes the synapse.customer.feedback Kafka topic, scores each message using a
rule-based keyword model (no external ML dependency), then:
  1. Persists the score + label back into the EngagementLog MongoDB document.
  2. Broadcasts a sentiment_alert NOC WebSocket event when the label is ANGRY.

Sentiment scale: -1.0 (furious) → 0.0 (neutral) → +1.0 (delighted)
Labels:
  POSITIVE  score >= 0.25
  NEUTRAL  -0.25 < score < 0.25
  NEGATIVE  score <= -0.25
  ANGRY     score <= -0.65  (triggers NOC toast in React dashboard)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Optional

from kafka.consumer import create_consumer
from kafka.schemas import CustomerFeedbackEvent
from kafka.topics import TOPIC_CUSTOMER_FEEDBACK, GROUP_SENTIMENT

logger = logging.getLogger(__name__)

# ── Rule-based lexicon ─────────────────────────────────────────────────────────

_POSITIVE_TERMS: list[tuple[str, float]] = [
    (r"\bthank(s| you)\b", 0.4),
    (r"\bgreat\b", 0.35),
    (r"\bexcellent\b", 0.4),
    (r"\bperfect\b", 0.4),
    (r"\bamazing\b", 0.4),
    (r"\bwonderful\b", 0.4),
    (r"\bhappy\b", 0.3),
    (r"\bsatisfied\b", 0.3),
    (r"\blove\b", 0.35),
    (r"\bawesome\b", 0.35),
    (r"\bfantastic\b", 0.4),
    (r"\bappreciate\b", 0.3),
    (r"\bhelped\b", 0.25),
    (r"\bquick(ly)?\b", 0.2),
    (r"\bfast\b", 0.15),
    (r"\bresolved\b", 0.25),
    (r"\bfixed\b", 0.2),
    (r"\bwork(s|ing)?\b", 0.1),
    (r"\bgood\b", 0.2),
]

_NEGATIVE_TERMS: list[tuple[str, float]] = [
    (r"\bterrible\b", -0.5),
    (r"\bawful\b", -0.5),
    (r"\bhorrible\b", -0.55),
    (r"\bdisgusted?\b", -0.55),
    (r"\bfurious\b", -0.7),
    (r"\blivid\b", -0.7),
    (r"\boutraged?\b", -0.65),
    (r"\banger\b", -0.6),
    (r"\bangr(y|ier)\b", -0.6),
    (r"\bfrustra(ted|tion)\b", -0.45),
    (r"\bunacceptable\b", -0.55),
    (r"\bstill broken\b", -0.4),
    (r"\bnot work(ing)?\b", -0.35),
    (r"\bdoes?n.?t work\b", -0.35),
    (r"\bnever work(s|ed)?\b", -0.5),
    (r"\bwaste of time\b", -0.5),
    (r"\bscam\b", -0.65),
    (r"\bbad\b", -0.3),
    (r"\bworst\b", -0.6),
    (r"\bhate\b", -0.55),
    (r"\bupset\b", -0.4),
    (r"\bdisappoint(ed|ing)?\b", -0.35),
    (r"\bcharging me\b", -0.3),
    (r"\boverchart?e\b", -0.45),
    (r"\bstill waiting\b", -0.3),
    (r"\bno response\b", -0.35),
    (r"\bunresolved\b", -0.4),
    (r"\bsick of\b", -0.5),
    (r"\btired of\b", -0.35),
]

# Intensity amplifiers (multiply next term weight by this factor)
_AMPLIFIERS: list[tuple[str, float]] = [
    (r"\bextremely\b", 1.5),
    (r"\babsolutely\b", 1.4),
    (r"\bcompletely\b", 1.3),
    (r"\bvery\b", 1.25),
    (r"\breally\b", 1.2),
    (r"\bso\b", 1.15),
    (r"\bincredibly\b", 1.4),
]


def _score_text(text: str) -> tuple[float, str]:
    """Return (raw_score, label) for a customer feedback string."""
    lowered = text.lower()
    raw = 0.0

    # Check for ALL-CAPS shouting (anger amplifier)
    caps_ratio = sum(1 for c in text if c.isupper()) / max(len(text), 1)
    amplifier = 1.3 if caps_ratio > 0.5 else 1.0

    for pattern, weight in _POSITIVE_TERMS:
        if re.search(pattern, lowered):
            raw += weight

    for pattern, weight in _NEGATIVE_TERMS:
        if re.search(pattern, lowered):
            raw += weight * amplifier   # amplify negative terms if shouting

    # Clamp to [-1, 1]
    score = max(-1.0, min(1.0, raw))

    if score <= -0.65:
        label = "ANGRY"
    elif score <= -0.25:
        label = "NEGATIVE"
    elif score >= 0.25:
        label = "POSITIVE"
    else:
        label = "NEUTRAL"

    return round(score, 3), label


# ── MongoDB helpers ────────────────────────────────────────────────────────────

_MONGO_ENABLED = bool(os.getenv("MONGODB_URI"))


async def _update_engagement_sentiment(
    engagement_id: str,
    score: float,
    label: str,
    correlation_id: Optional[str],
) -> None:
    if not _MONGO_ENABLED:
        return

    from db.mongo import get_db
    db = await get_db()

    update_filter: dict = {}
    if engagement_id:
        update_filter["id"] = engagement_id
    elif correlation_id:
        update_filter["correlation_id"] = correlation_id
    else:
        return

    result = await db.engagement_logs.update_one(
        update_filter,
        {"$set": {"sentiment_score": score, "sentiment_label": label}},
    )
    if result.modified_count:
        logger.info(
            "Sentiment: engagement_id=%s score=%.3f label=%s",
            engagement_id, score, label,
        )


async def _broadcast_angry_alert(
    feedback: CustomerFeedbackEvent,
    score: float,
) -> None:
    """Push a sentiment_alert WebSocket event to the NOC when a customer is ANGRY."""
    try:
        from api.websocket_manager import manager as ws_manager, noc_event
        await ws_manager.broadcast(
            feedback.opco_id or "ALL",
            noc_event(
                "sentiment_alert",
                feedback.opco_id or "DEFAULT",
                customer_id=feedback.customer_id,
                engagement_id=feedback.engagement_id,
                correlation_id=feedback.correlation_id,
                sentiment_score=score,
                sentiment_label="ANGRY",
                channel=feedback.channel,
            ),
        )
    except Exception as exc:
        logger.debug("WS broadcast skipped (not co-located with API): %s", exc)


# ── Main consumer loop ─────────────────────────────────────────────────────────

async def run() -> None:
    logger.info("Sentiment worker starting — consuming %s", TOPIC_CUSTOMER_FEEDBACK)
    consumer = await create_consumer(
        topics=[TOPIC_CUSTOMER_FEEDBACK],
        group_id=GROUP_SENTIMENT,
    )

    async with consumer:
        async for msg in consumer:
            try:
                raw = json.loads(msg.value.decode("utf-8"))
                feedback = CustomerFeedbackEvent(**raw)

                score, label = _score_text(feedback.raw_text)
                logger.info(
                    "Scored feedback_id=%s score=%.3f label=%s",
                    feedback.feedback_id, score, label,
                )

                await _update_engagement_sentiment(
                    feedback.engagement_id or "",
                    score,
                    label,
                    feedback.correlation_id,
                )

                if label == "ANGRY":
                    await _broadcast_angry_alert(feedback, score)

                await consumer.commit()

            except Exception:
                logger.exception("Error processing feedback message — skipping")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run())
