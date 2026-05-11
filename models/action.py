"""
Action state machine models for the Synapse HITL pipeline.

State transitions:
  PENDING          → APPROVED | REJECTED | EXPIRED | STALE
  APPROVED         → EXECUTING | STALE
  EXECUTING        → EXECUTED | FAILED
  FAILED           → EXECUTING  (retry path)
  STALE            → ESCALATED_STALE  (Watchdog circuit breaker)
  ESCALATED_STALE  → FORCE_APPROVED   (Phase 10: operator manual override)
  FORCE_APPROVED   → EXECUTING        (drift check skipped)
  REJECTED, EXECUTED, EXPIRED are terminal
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
import uuid


class ActionStatus(str, Enum):
    PENDING          = "PENDING"
    APPROVED         = "APPROVED"
    REJECTED         = "REJECTED"
    EXECUTING        = "EXECUTING"
    EXECUTED         = "EXECUTED"
    FAILED           = "FAILED"
    EXPIRED          = "EXPIRED"
    STALE            = "STALE"            # Phase 8: CRM state drifted before execution
    ESCALATED_STALE  = "ESCALATED_STALE"  # Phase 8: max re-analyses exceeded → human review
    FORCE_APPROVED   = "FORCE_APPROVED"   # Phase 10: operator override bypasses drift check


VALID_TRANSITIONS: dict[ActionStatus, set[ActionStatus]] = {
    ActionStatus.PENDING:         {ActionStatus.APPROVED, ActionStatus.REJECTED,
                                   ActionStatus.EXPIRED,  ActionStatus.STALE,
                                   ActionStatus.FORCE_APPROVED},
    ActionStatus.APPROVED:        {ActionStatus.EXECUTING, ActionStatus.STALE},
    ActionStatus.EXECUTING:       {ActionStatus.EXECUTED, ActionStatus.FAILED},
    ActionStatus.FAILED:          {ActionStatus.EXECUTING},   # retry
    ActionStatus.REJECTED:        set(),
    ActionStatus.EXECUTED:        set(),
    ActionStatus.EXPIRED:         set(),
    ActionStatus.STALE:           {ActionStatus.ESCALATED_STALE},
    # Phase 10: ESCALATED_STALE can be force-approved by an operator with explicit reason
    ActionStatus.ESCALATED_STALE: {ActionStatus.FORCE_APPROVED},
    ActionStatus.FORCE_APPROVED:  {ActionStatus.EXECUTING},
}

EXPIRY_HOURS = 4

# ── Phase 8: State contract schemas ───────────────────────────────────────────

# Fields the agent MUST populate in predicted_before_state, keyed by action_type.
# _check_state_drift validates these exact keys against the live CRM state.
# Any key the agent omits is silently skipped (permissive — avoids false positives
# when the agent encounters missing data).
BEFORE_STATE_SCHEMA: dict[str, set[str]] = {
    "retry_provisioning":  {"customer_status", "subscription_status", "last_provisioning_result"},
    "update_billing":      {"account_status", "current_balance", "credit_limit"},
    "cancel_order":        {"order_status"},
    "escalate":            {"customer_status"},
    "notify_customer":     {"customer_status"},
    "update_subscription": {"customer_status", "subscription_status"},
    "create_ticket":       {"customer_status", "order_status"},
    "manual_review":       set(),   # unstructured — no drift check performed
}

# Absolute dollar tolerance for billing balance comparisons.
# Prevents STALE aborts on sub-cent tax rounding or in-flight payment clearing.
BILLING_BALANCE_DRIFT_TOLERANCE: float = 5.0  # USD

# Number of re-analysis cycles the Watchdog allows before escalating to ESCALATED_STALE.
# At 0 or 1 staleness: re-publish to resolve_jobs for a fresh agent analysis.
# At MAX_REANALYSIS_ATTEMPTS: transition to ESCALATED_STALE — human must intervene.
MAX_REANALYSIS_ATTEMPTS: int = 2


# ── Models ────────────────────────────────────────────────────────────────────

class ActionProposal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str
    order_id: str | None = None
    customer_id: str | None = None
    action_type: str
    action_description: str
    action_payload: dict[str, Any] = Field(default_factory=dict)
    predicted_before_state: dict[str, Any] = Field(default_factory=dict)
    predicted_after_state: dict[str, Any] = Field(default_factory=dict)
    status: ActionStatus = ActionStatus.PENDING
    version: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime = Field(
        default_factory=lambda: datetime.utcnow() + timedelta(hours=EXPIRY_HOURS)
    )
    approved_by: str | None = None
    rejection_reason: str | None = None
    retry_count: int = 0
    max_retries: int = 3
    executed_at: datetime | None = None
    execution_error: str | None = None
    # Phase 8 — Defensive Execution fields
    stale_reason:             str | None = None
    reanalysis_count:         int = 0
    predicted_state_at_check: dict[str, Any] | None = None  # snapshot before STALE transition
    current_state_at_check:   dict[str, Any] | None = None  # live state that caused drift

    # Execution failure detail
    handler_attempt_count: int | None = None
    compensation_type:     str | None = None
    compensation_result:   dict[str, Any] | None = None

    # Force Approve fields
    approval_method:       str | None = None   # "STANDARD" | "FORCE_APPROVED"
    force_approval_reason: str | None = None
    safety_checks_bypassed: bool = False

    # Phase 16: structured Chain-of-Thought reasoning monologue
    chain_of_thought: dict[str, str] | None = None


class AuditEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    action_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    previous_status: ActionStatus | None = None
    new_status: ActionStatus
    actor: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ApproveRequest(BaseModel):
    operator_id:           str = Field(..., description="Identity of the approving operator")
    approval_method:       str = Field(default="STANDARD", description="STANDARD or FORCE_APPROVED")
    force_approval_reason: str | None = Field(default=None, description="Required when approval_method is FORCE_APPROVED")

    model_config = {
        "json_schema_extra": {
            "example": {
                "operator_id": "operator-alice-42",
                "approval_method": "STANDARD",
                "force_approval_reason": None,
            }
        }
    }


class RejectRequest(BaseModel):
    operator_id: str = Field(..., description="Identity of the rejecting operator")
    reason: str = Field(..., min_length=5, description="Mandatory rejection reason")

    model_config = {
        "json_schema_extra": {
            "example": {
                "operator_id": "operator-alice-42",
                "reason": "Customer already has an active retry in progress via manual CRM.",
            }
        }
    }


class ForceApproveRequest(BaseModel):
    """
    Phase 10 — Manual override for ESCALATED_STALE actions.

    Bypasses the drift pre-flight check in the Executor. Requires a mandatory
    justification so every override is auditable.
    """
    operator_id:   str = Field(..., description="Identity of the operator forcing execution")
    justification: str = Field(..., min_length=10, description="Mandatory reason for override")

    model_config = {
        "json_schema_extra": {
            "example": {
                "operator_id": "operator-alice-42",
                "justification": "Confirmed with Network Ops team — stale state is a caching artefact. Safe to proceed.",
            }
        }
    }
