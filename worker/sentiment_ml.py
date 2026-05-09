"""
Phase 15 Step 3 — Transformer-based Multi-language Sentiment Engine.

Replaces the Phase 14 rule-based keyword scorer with a Small Language Model that
handles non-English feedback natively — critical for Vodafone DE/ES/IT markets.

Model selection (controlled by SENTIMENT_BACKEND env var):
  "distilbert"  — nlptown/bert-base-multilingual-uncased-sentiment (5-star, 102 langs)
                  ~670 MB, runs on CPU, no GPU required.
  "claude"      — Uses Claude Haiku via the Anthropic API for maximum accuracy.
                  Preferred when the deployment has API budget; no local GPU needed.
  "keyword"     — Phase 14 rule-based fallback (always available).

The public interface mirrors the Phase 14 scorer:
  score_text(text: str, language: str | None) -> SentimentScore

SentimentScore maps the 1-5 star range to Synapse's -1..+1 scale:
  5 stars → +1.0  POSITIVE
  4 stars → +0.50 POSITIVE
  3 stars →  0.0  NEUTRAL
  2 stars → -0.50 NEGATIVE
  1 star  → -1.0  ANGRY

Language detection uses langdetect (lightweight, offline).
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

SENTIMENT_BACKEND = os.getenv("SENTIMENT_BACKEND", "claude").lower()  # distilbert | claude | keyword
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL_SENTIMENT", "claude-haiku-4-5-20251001")

# Star → (score, label) mapping
_STAR_MAP: dict[int, tuple[float, str]] = {
    1: (-1.0,  "ANGRY"),
    2: (-0.50, "NEGATIVE"),
    3: ( 0.0,  "NEUTRAL"),
    4: ( 0.50, "POSITIVE"),
    5: ( 1.0,  "POSITIVE"),
}


@dataclass
class SentimentScore:
    score: float        # -1.0 … +1.0
    label: str          # ANGRY | NEGATIVE | NEUTRAL | POSITIVE
    language: str       # ISO 639-1 code detected, e.g. "en", "de", "es"
    backend: str        # which backend produced this result
    stars: Optional[int] = None   # 1-5 for distilbert/claude, None for keyword


# ── language detection ─────────────────────────────────────────────────────────

def _detect_language(text: str) -> str:
    try:
        from langdetect import detect
        return detect(text)
    except Exception:
        return "en"


# ── DistilBERT / mBERT backend ────────────────────────────────────────────────

_pipeline = None   # lazy-loaded

def _get_pipeline():
    global _pipeline
    if _pipeline is None:
        try:
            from transformers import pipeline as hf_pipeline
            _pipeline = hf_pipeline(
                "text-classification",
                model="nlptown/bert-base-multilingual-uncased-sentiment",
                top_k=1,
            )
            logger.info("sentiment_ml: loaded DistilBERT pipeline")
        except Exception as exc:
            logger.warning("sentiment_ml: cannot load DistilBERT — %s; falling back to keyword", exc)
            _pipeline = None
    return _pipeline


def _score_distilbert(text: str, language: str) -> SentimentScore:
    pipe = _get_pipeline()
    if pipe is None:
        return _score_keyword(text, language)

    # Truncate to 512 tokens (model limit)
    truncated = text[:1024]
    result = pipe(truncated)[0]
    label_raw: str = result["label"]   # e.g. "3 stars"
    stars = int(label_raw.split()[0])
    score, label = _STAR_MAP[stars]
    return SentimentScore(score=score, label=label, language=language, backend="distilbert", stars=stars)


# ── Claude Haiku backend ───────────────────────────────────────────────────────

_claude_client = None

def _get_claude():
    global _claude_client
    if _claude_client is None:
        import anthropic
        _claude_client = anthropic.Anthropic()
    return _claude_client


def _score_claude(text: str, language: str) -> SentimentScore:
    client = _get_claude()
    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=16,
            system=(
                "You are a sentiment classifier. Rate the customer message on a 1-5 star scale. "
                "Reply with a single integer between 1 and 5, nothing else."
            ),
            messages=[{"role": "user", "content": f"[Language: {language}]\n{text[:800]}"}],
        )
        stars = int(response.content[0].text.strip()[0])
        stars = max(1, min(5, stars))
        score, label = _STAR_MAP[stars]
        return SentimentScore(score=score, label=label, language=language, backend="claude", stars=stars)
    except Exception as exc:
        logger.warning("sentiment_ml: claude backend error — %s; falling back to keyword", exc)
        return _score_keyword(text, language)


# ── Rule-based keyword fallback (Phase 14) ────────────────────────────────────

import re

_POS = [
    (r"\bthank(s| you)\b", 0.4), (r"\bgreat\b", 0.35), (r"\bexcellent\b", 0.4),
    (r"\bperfect\b", 0.4),       (r"\bamazing\b", 0.4), (r"\bwonderful\b", 0.4),
    (r"\bhappy\b", 0.3),         (r"\bsatisfied\b", 0.3),(r"\blove\b", 0.35),
    (r"\bawesome\b", 0.35),      (r"\bresolved\b", 0.25),(r"\bfixed\b", 0.2),
    (r"\bgood\b", 0.2),
]
_NEG = [
    (r"\bterrible\b", -0.5),  (r"\bawful\b", -0.5),    (r"\bhorrible\b", -0.55),
    (r"\bdisgusted?\b", -0.55),(r"\bfurious\b", -0.7),  (r"\banger\b", -0.55),
    (r"\bangry\b", -0.65),    (r"\bunacceptable\b", -0.6),(r"\boutrage(ous|d)?\b", -0.6),
    (r"\bworst\b", -0.6),     (r"\bscam\b", -0.7),      (r"\bfraud\b", -0.7),
    (r"\bbroken\b", -0.4),    (r"\bstill.{0,15}not.{0,10}work\b", -0.45),
    (r"\bno.{0,10}service\b", -0.4),(r"\bno signal\b", -0.5),
    # Multilingual common terms
    (r"\bfurchtbar\b", -0.55),  # de: terrible
    (r"\bschrecklich\b", -0.55),(r"\bärgerlich\b", -0.5),
    (r"\bterrible\b", -0.5),    # es/fr/it overlap
    (r"\bhorrendo\b", -0.55),   # es
    (r"\bpessimo\b", -0.55),    # it
]


def _score_keyword(text: str, language: str) -> SentimentScore:
    low = text.lower()
    score = 0.0
    for pattern, weight in _POS:
        if re.search(pattern, low):
            score += weight
    for pattern, weight in _NEG:
        if re.search(pattern, low):
            score += weight
    score = max(-1.0, min(1.0, score))
    if score <= -0.65:
        label = "ANGRY"
    elif score <= -0.25:
        label = "NEGATIVE"
    elif score >= 0.25:
        label = "POSITIVE"
    else:
        label = "NEUTRAL"
    return SentimentScore(score=round(score, 3), label=label, language=language, backend="keyword")


# ── Public interface ───────────────────────────────────────────────────────────

def score_text(text: str, language: Optional[str] = None) -> SentimentScore:
    """
    Score a customer feedback string.

    Args:
        text: Raw customer message (any supported language).
        language: ISO 639-1 code. Auto-detected from text if None.

    Returns:
        SentimentScore with score in [-1, +1], label, language, and backend.
    """
    lang = language or _detect_language(text)

    if SENTIMENT_BACKEND == "distilbert":
        return _score_distilbert(text, lang)
    if SENTIMENT_BACKEND == "claude":
        return _score_claude(text, lang)
    return _score_keyword(text, lang)


async def score_text_async(text: str, language: Optional[str] = None) -> SentimentScore:
    """Async wrapper — runs the (potentially blocking) scorer in a thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, score_text, text, language)
