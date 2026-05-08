"""
Engagement tracking models for Phase 7 — Proactive Multi-Channel Engagement.

An EngagementLog is created whenever the Executor dispatches a post-action
notification to a customer. The record tracks the channel used, the template,
a masked version of the recipient address, and the eventual customer response.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field
import uuid


class EngagementChannel(str, Enum):
    SMS      = "SMS"
    EMAIL    = "EMAIL"
    WHATSAPP = "WHATSAPP"


class EngagementStatus(str, Enum):
    PENDING = "PENDING"
    SENT    = "SENT"
    FAILED  = "FAILED"


class CustomerResponse(str, Enum):
    PENDING  = "PENDING"
    ACCEPTED = "ACCEPTED"
    DECLINED = "DECLINED"
    REPLIED  = "REPLIED"


class EngagementLog(BaseModel):
    id:                   str = Field(default_factory=lambda: str(uuid.uuid4()))
    action_id:            str
    correlation_id:       str
    customer_id:          Optional[str] = None
    channel:              EngagementChannel
    template_used:        str
    message_body:         str
    # Recipient is masked at rest; the raw value is only held in-memory during dispatch.
    recipient_masked:     str
    sent_at:              Optional[datetime] = None
    status:               EngagementStatus  = EngagementStatus.PENDING
    customer_response:    CustomerResponse  = CustomerResponse.PENDING
    response_text:        Optional[str]     = None
    response_received_at: Optional[datetime] = None
    created_at:           datetime = Field(default_factory=datetime.utcnow)
