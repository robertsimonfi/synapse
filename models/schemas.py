"""
Shared Pydantic schemas and dataclasses used across the Synapse pipeline.

RiskScore — canonical risk assessment for a chatbot-originated action.
  Produced by agent/risk_engine.py; stored in the action_payload so the
  NOC dashboard can surface the score alongside the approval card.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class RiskLevel(str, Enum):
    """Qualitative risk band derived from the numeric score."""
    SAFE     = "SAFE"      # 0–9  → pure query, answered inline
    LOW      = "LOW"       # 10–29 → auto-approve if confidence ≥ 0.90
    MEDIUM   = "MEDIUM"    # 30–59 → HITL required
    HIGH     = "HIGH"      # 60–79 → HITL required, NOC priority
    CRITICAL = "CRITICAL"  # 80–100 → HITL required, immediate NOC escalation


@dataclass(frozen=True)
class RiskScore:
    """Immutable risk assessment for a single chatbot-originated action.

    score        0–100  numeric risk points
    level        qualitative band (SAFE / LOW / MEDIUM / HIGH / CRITICAL)
    auto_approve True when Core should FORCE_APPROVE without human review
    hitl_required True when a NOC operator must approve before execution
    reason       human-readable routing decision (for audit trail)
    """
    score:         int
    level:         RiskLevel
    auto_approve:  bool
    hitl_required: bool
    reason:        str

    def as_dict(self) -> dict:
        """Serialise for storage in MongoDB action_payload."""
        return {
            "risk_score":    self.score,
            "risk_level":    self.level.value,
            "auto_approve":  self.auto_approve,
            "hitl_required": self.hitl_required,
            "risk_reason":   self.reason,
        }
