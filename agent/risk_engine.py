"""
Phase 3 — Weighted Risk Engine for chatbot-originated actions.

Point-accumulation model:
  base intent score  +  sentiment modifier  +  360 context bonus  +  domain boost  +  severity boost
  → total score compared against threshold of 50

Routing decisions:
  score < 50 AND confidence >= 0.90 AND not needs_discovery  → auto-approve (FORCE_APPROVED)
  score >= 50 AND not needs_discovery                         → HITL required → PENDING, NOC review
  needs_discovery (ANGRY + unknown intent)                    → ask clarifying question, no HITL yet

Sentiment modifiers:
  ANGRY    → +30 pts  (pushes billing disputes over the threshold)
  NEGATIVE → +10 pts  (minor boost, often keeps below threshold)

360 context bonus:
  has_360_context=True → +10 pts (verified customer data adds signal confidence)

Domain boost:
  BILLING / BSS → +10 pts (financial risk amplifier)

Severity boost:
  CRITICAL / HIGH → +15 pts
"""

from __future__ import annotations

from models.schemas import RiskLevel, RiskScore

# ── Thresholds ────────────────────────────────────────────────────────────────

_AUTO_APPROVE_MAX_SCORE  = 50    # score strictly below this → candidate for auto-approve (was 30)
_AUTO_APPROVE_MIN_CONF   = 0.90  # confidence must be at or above this to auto-approve
_HIGH_RISK_THRESHOLD     = 70    # score at or above this → HIGH band (was 60)
_CRITICAL_RISK_THRESHOLD = 90    # score at or above this → CRITICAL band (was 80)

# ── Sentiment modifiers ───────────────────────────────────────────────────────

_SENTIMENT_MODIFIERS: dict[str, int] = {
    "ANGRY":    30,
    "NEGATIVE": 10,
    "NEUTRAL":  0,
    "POSITIVE": 0,
}

# 360 context bonus — verified customer profile means more signal for routing.
_360_CONTEXT_BONUS = 10

# ── Score table ───────────────────────────────────────────────────────────────
#
# Base intent scores before sentiment / context / domain modifiers.
# 0          → SAFE: read-only query — answered inline, no action proposal.
# 1–49       → MEDIUM or below with modifiers: auto-approvable path.
# 50–69      → HIGH: HITL required.
# 70–89      → HIGH: HITL required + urgent NOC flag.
# 90–100     → CRITICAL: HITL required + immediate NOC escalation.

_SCORES: dict[str, int] = {
    # ── Read-only (SAFE) ──────────────────────────────────────────────────────
    "BILL_QUERY":            0,
    "DATA_USAGE":            0,
    "CHECK_BALANCE":         0,
    "GENERAL_INQUIRY":       0,
    "DISCOVERY":             0,
    "DISCOVERY_REQUEST":     0,

    # ── Low-risk self-serve (auto-approve candidates) ─────────────────────────
    "ADDRESS_CHANGE":        5,
    "CONTACT_UPDATE":        5,
    "NOTIFICATION_PREF":     5,
    "DATA_BOOST":            15,   # one-time top-up, low financial impact
    "SIM_SWAP":              20,   # same number, same customer

    # ── Technical issues (10 pts base per spec) ───────────────────────────────
    "PROVISIONING_ISSUE":    10,
    "NETWORK_OUTAGE":        10,
    "SERVICE_DEGRADATION":   10,

    # ── Medium-risk (below threshold solo; sentiment can push over 50) ────────
    "BILLING_DISPUTE":       20,   # per spec: 20 base; ANGRY +30 = 50 → HITL
    "CREDIT_ADJUSTMENT":     25,
    "LATE_FEE_WAIVER":       20,
    "ROAMING_ENABLE":        25,
    "INTERNATIONAL_UNLOCK":  30,
    "PLAN_CHANGE":           35,   # calm request is auto-approve; angry → HITL
    "PLAN_SWITCH":           35,
    "CONTRACT_EXTENSION":    40,

    # ── High-risk financial (HITL required regardless of sentiment) ───────────
    "REFUND":                55,
    "REFUND_REQUEST":        55,

    # ── Retention / churn (always HITL) ──────────────────────────────────────
    "CANCELLATION":          70,
    "CANCELLATION_REQUEST":  70,
    "CHURN_RISK":            60,
}

# Unknown intents default to 50 — exactly at threshold, HITL required.
_DEFAULT_SCORE = 50

# ── Discovery intents — ANGRY + one of these → Discovery Turn (ask first) ────
_DISCOVERY_INTENTS = frozenset({
    "GENERAL_INQUIRY", "DISCOVERY", "DISCOVERY_REQUEST", "GENERAL", "UNKNOWN", "",
})


# ── Public API ────────────────────────────────────────────────────────────────

def evaluate(
    intent_label: str,
    domain: str = "",
    severity: str = "MEDIUM",
    confidence: float = 1.0,
    sentiment_label: str = "NEUTRAL",
    has_360_context: bool = False,
) -> RiskScore:
    """Score an intent and return an immutable RiskScore routing decision.

    Parameters
    ----------
    intent_label : str
        The chatbot intent label (e.g. "Plan Change", "BILLING_DISPUTE").
    domain : str
        BSS/OSS domain hint from the IntentRouter.
    severity : str
        CRITICAL | HIGH | MEDIUM | LOW — operator/sentiment-assigned severity.
    confidence : float
        Agent confidence in the intent classification (0.0–1.0).
    sentiment_label : str
        ANGRY | NEGATIVE | NEUTRAL | POSITIVE from the sentiment engine.
    has_360_context : bool
        True when a verified ODS/SQLite 360-view was loaded for this customer.
    """
    key        = intent_label.upper().replace(" ", "_")
    base_score = _SCORES.get(key, _DEFAULT_SCORE)

    # Sentiment modifier — ANGRY pushes medium-risk items over the HITL threshold.
    sent           = sentiment_label.upper()
    sentiment_pts  = _SENTIMENT_MODIFIERS.get(sent, 0)

    # 360 context bonus — verified profile adds routing signal.
    context_pts = _360_CONTEXT_BONUS if has_360_context else 0

    # Domain boost: billing domain carries extra financial risk.
    domain_pts = 10 if domain.upper() in ("BILLING", "BSS") and base_score > 0 else 0

    # Severity boost for operator-flagged / sentiment-flagged high-impact events.
    severity_pts = 15 if severity.upper() in ("CRITICAL", "HIGH") else 0

    # Discovery Turn: ANGRY customer but intent is still unknown/general.
    # Don't escalate yet — ask a clarifying question first.
    needs_discovery = (sent == "ANGRY" and key in _DISCOVERY_INTENTS)

    # Accumulate score; cap the score below the HITL threshold for discovery turns
    # so they don't accidentally trip HITL before we know what the customer wants.
    score = base_score + sentiment_pts + context_pts + domain_pts + severity_pts
    if needs_discovery:
        score = min(score, _AUTO_APPROVE_MAX_SCORE - 1)
    score = min(100, score)

    # Determine qualitative band.
    if score == 0:
        level = RiskLevel.SAFE
    elif score < _AUTO_APPROVE_MAX_SCORE:
        level = RiskLevel.LOW
    elif score < _HIGH_RISK_THRESHOLD:
        level = RiskLevel.MEDIUM
    elif score < _CRITICAL_RISK_THRESHOLD:
        level = RiskLevel.HIGH
    else:
        level = RiskLevel.CRITICAL

    # Auto-approve: below threshold, confidence high, no discovery needed.
    auto_approve  = (
        score < _AUTO_APPROVE_MAX_SCORE
        and confidence >= _AUTO_APPROVE_MIN_CONF
        and not needs_discovery
    )
    # HITL: not auto-approve, not safe, and no discovery turn pending.
    hitl_required = not auto_approve and level != RiskLevel.SAFE and not needs_discovery

    # Build human-readable reason with full score breakdown.
    breakdown_parts = [f"base={base_score}"]
    if sentiment_pts:
        breakdown_parts.append(f"sentiment({sent})={sentiment_pts:+d}")
    if context_pts:
        breakdown_parts.append(f"360_context={context_pts:+d}")
    if domain_pts:
        breakdown_parts.append(f"domain={domain_pts:+d}")
    if severity_pts:
        breakdown_parts.append(f"severity={severity_pts:+d}")
    breakdown = " + ".join(breakdown_parts)

    verdict = (
        "read-only query"  if level == RiskLevel.SAFE else
        "discovery_turn"   if needs_discovery         else
        "auto-approve"     if auto_approve             else
        f"HITL required ({level.value})"
    )
    reason = (
        f"'{intent_label}' → {breakdown} = {score} pts "
        f"[confidence={confidence:.2f}] → {verdict}"
    )

    return RiskScore(
        score=score,
        level=level,
        auto_approve=auto_approve,
        hitl_required=hitl_required,
        reason=reason,
        needs_discovery=needs_discovery,
    )
