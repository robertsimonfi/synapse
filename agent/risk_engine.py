"""
Phase 2/3 — Risk Engine for chatbot-originated actions.

Assigns a numeric risk score (0–100) to a chatbot intent and returns a
routing decision:
  score < 30 AND confidence >= 0.90  → LOW risk → auto-approve (FORCE_APPROVED)
  score >= 30                         → HITL required → PENDING, NOC review

Score table keys are normalised to UPPER_SNAKE_CASE so both "Plan Change"
and "PLAN_CHANGE" resolve correctly.

Confidence is the float returned by the Synapse IntentRouter / agent
(1.0 for mock/explicit chatbot payloads, variable for live Claude output).
Unknown intents default to 50 (MEDIUM — HITL required).
"""

from __future__ import annotations

from models.schemas import RiskLevel, RiskScore

# ── Thresholds ────────────────────────────────────────────────────────────────

_AUTO_APPROVE_MAX_SCORE  = 30    # score strictly below this → candidate for auto-approve
_AUTO_APPROVE_MIN_CONF   = 0.90  # confidence must be at or above this to auto-approve
_HIGH_RISK_THRESHOLD     = 60    # score at or above this → HIGH/CRITICAL band
_CRITICAL_RISK_THRESHOLD = 80    # score at or above this → CRITICAL band

# ── Score table ───────────────────────────────────────────────────────────────
#
# 0          → SAFE: read-only query — no action proposal created in Core.
# 1–29       → LOW: auto-approvable if confidence is sufficient.
# 30–59      → MEDIUM: HITL required.
# 60–79      → HIGH: HITL required + urgent NOC flag.
# 80–100     → CRITICAL: HITL required + immediate NOC escalation.

_SCORES: dict[str, int] = {
    # ── Read-only (SAFE) ──────────────────────────────────────────────────────
    "BILL_QUERY":            0,
    "DATA_USAGE":            0,
    "CHECK_BALANCE":         0,
    "GENERAL_INQUIRY":       0,
    "DISCOVERY":             0,
    "DISCOVERY_REQUEST":     0,

    # ── Low-risk self-serve (AUTO-APPROVE candidates) ─────────────────────────
    "ADDRESS_CHANGE":        10,
    "CONTACT_UPDATE":        10,
    "NOTIFICATION_PREF":     10,
    "PROVISIONING_ISSUE":    25,
    "SIM_SWAP":              25,   # low-risk SIM swap (same number, same customer)
    "DATA_BOOST":            20,   # one-time data top-up, no recurring commitment

    # ── Medium-risk (HITL required) ───────────────────────────────────────────
    "PLAN_CHANGE":           40,
    "PLAN_SWITCH":           40,
    "CONTRACT_EXTENSION":    45,
    "ROAMING_ENABLE":        30,
    "INTERNATIONAL_UNLOCK":  35,

    # ── High-risk financial (HITL + high priority) ────────────────────────────
    "BILLING_DISPUTE":       60,
    "CREDIT_ADJUSTMENT":     65,
    "LATE_FEE_WAIVER":       60,
    "REFUND":                80,
    "REFUND_REQUEST":        80,

    # ── Retention / churn (HITL + immediate escalation) ──────────────────────
    "CANCELLATION":          80,
    "CANCELLATION_REQUEST":  80,
    "CHURN_RISK":            70,

    # ── Network fault escalations ─────────────────────────────────────────────
    "NETWORK_OUTAGE":        55,
    "SERVICE_DEGRADATION":   50,
}


# ── Public API ────────────────────────────────────────────────────────────────

def evaluate(
    intent_label: str,
    domain: str = "",
    severity: str = "MEDIUM",
    confidence: float = 1.0,
) -> RiskScore:
    """Score an intent and return an immutable RiskScore routing decision.

    Parameters
    ----------
    intent_label : str
        The chatbot intent label (e.g. "Plan Change", "REFUND_REQUEST").
    domain : str
        BSS/OSS domain hint from the IntentRouter.  Used to widen the
        score for billing domains where fraud risk is higher.
    severity : str
        CRITICAL | HIGH | MEDIUM | LOW — operator-assigned severity tag.
        CRITICAL or HIGH bumps the score by 15 pts (never above 100).
    confidence : float
        Agent confidence in the intent classification (0.0–1.0).
        Auto-approve is only granted when confidence >= _AUTO_APPROVE_MIN_CONF.
    """
    key   = intent_label.upper().replace(" ", "_")
    score = _SCORES.get(key, 50)   # default: MEDIUM risk, HITL required

    # Domain boost: billing actions carry extra financial risk.
    if domain.upper() in ("BILLING", "BSS") and score > 0:
        score = min(100, score + 10)

    # Severity boost for operator-flagged high-impact events.
    if severity.upper() in ("CRITICAL", "HIGH"):
        score = min(100, score + 15)

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

    # Auto-approve: LOW band AND confidence meets threshold.
    auto_approve  = (level == RiskLevel.LOW) and (confidence >= _AUTO_APPROVE_MIN_CONF)
    hitl_required = not auto_approve and level != RiskLevel.SAFE

    verdict = (
        "read-only query"  if level == RiskLevel.SAFE else
        "auto-approve"     if auto_approve             else
        f"HITL required ({level.value})"
    )
    reason = (
        f"'{intent_label}' → {score} pts "
        f"[confidence={confidence:.2f}] → {verdict}"
    )

    return RiskScore(
        score=score,
        level=level,
        auto_approve=auto_approve,
        hitl_required=hitl_required,
        reason=reason,
    )
