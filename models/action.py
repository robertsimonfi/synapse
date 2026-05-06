"""
Action state machine models for the Synapse HITL pipeline.

State transitions:
  PENDING → APPROVED | REJECTED | EXPIRED
  APPROVED → EXECUTING
  EXECUTING → EXECUTED | FAILED
  FAILED → EXECUTING  (retry path)
  REJECTED, EXECUTED, EXPIRED are terminal
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field
import uuid


class ActionStatus(str, Enum):
    PENDING   = "PENDING"
    APPROVED  = "APPROVED"
    REJECTED  = "REJECTED"
    EXECUTING = "EXECUTING"
    EXECUTED  = "EXECUTED"
    FAILED    = "FAILED"
    EXPIRED   = "EXPIRED"


VALID_TRANSITIONS: dict[ActionStatus, set[ActionStatus]] = {
    ActionStatus.PENDING:   {ActionStatus.APPROVED, ActionStatus.REJECTED, ActionStatus.EXPIRED},
    ActionStatus.APPROVED:  {ActionStatus.EXECUTING},
    ActionStatus.EXECUTING: {ActionStatus.EXECUTED, ActionStatus.FAILED},
    ActionStatus.FAILED:    {ActionStatus.EXECUTING},  # retry
    ActionStatus.REJECTED:  set(),
    ActionStatus.EXECUTED:  set(),
    ActionStatus.EXPIRED:   set(),
}

EXPIRY_HOURS = 4


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
