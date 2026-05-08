"""
Action state machine models for the Synapse HITL pipeline.

State transitions:
  PENDING  → APPROVED | REJECTED | EXPIRED | STALE
  APPROVED → EXECUTING | STALE
  EXECUTING → EXECUTED | FAILED
  FAILED → EXECUTING  (retry path)
  STALE → ESCALATED_STALE  (Watchdog circuit breaker)
  REJECTED, EXECUTED, EXPIRED, ESCALATED_STALE are terminal
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


VALID_TRANSITIONS: dict[ActionStatus, set[ActionStatus]] = {
    ActionStatus.PENDING:         {ActionStatus.APPROVED, ActionStatus.REJECTED,
                                   ActionStatus.EXPIRED,  ActionStatus.STALE},
    ActionStatus.APPROVED:        {ActionStatus.EXECUTING, ActionStatus.STALE},
    ActionStatus.EXECUTING:       {ActionStatus.EXECUTED, ActionStatus.FAILED},
    ActionStatus.FAILED:          {ActionStatus.EXECUTING},   # retry
    ActionStatus.REJECTED:        set(),
    ActionStatus.EXECUTED:        set(),
    ActionStatus.EXPIRED:         set(),
    ActionStatus.STALE:           {ActionStatus.ESCALATED_STALE},
    ActionStatus.ESCALATED_STALE: set(),
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
    stale_reason:     str | None = None  # reason recorded by drift check or Watchdog
    reanalysis_count: int = 0            # incremented each time Watchdog re-publishes


class AuditEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    action_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    previous_status: ActionStatus | None = None
    new_status: ActionStatus
    actor: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ApproveRequest(BaseModel):
    operator_id: str = Field(..., description="Identity of the approving operator")

    model_config = {
        "json_schema_extra": {"example": {"operator_id": "operator-alice-42"}}
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
