"""
Pydantic-based message schemas for the three Synapse Kafka topics.

These act as the "Schema Registry" contract — every producer validates its
message against the appropriate schema before publishing, and every consumer
deserialises into the schema on receipt. This ensures strict typing without
requiring a running Confluent Schema Registry in local dev.

Topics → Schema classes:
  oracle.cdc.all_changes    → OracleCdcEvent
  ai.orchestration.tasks    → OrchestrationTask
  legacy.commands.writeback → WritebackCommand
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field
import uuid


# ── oracle.cdc.all_changes ─────────────────────────────────────────────────────

class OracleCdcEvent(BaseModel):
    """A single row-level change event from Oracle, emitted by the Hydrator CDC producer."""

    event_id:      str      = Field(default_factory=lambda: str(uuid.uuid4()))
    op:            Literal["INSERT", "UPDATE", "DELETE"]
    table_name:    str      # e.g. CUSTOMER, SERVICE_SUBSCRIPTION
    primary_key:   dict[str, Any]       # e.g. {"CUSTOMER_ID": 42}
    before:        Optional[dict[str, Any]] = None  # NULL for INSERT
    after:         Optional[dict[str, Any]] = None  # NULL for DELETE
    scn:           Optional[int]        = None   # Oracle System Change Number (simulated)
    commit_ts:     str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    source_schema: str = "TELCO"

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── ai.orchestration.tasks ─────────────────────────────────────────────────────

class OrchestrationTask(BaseModel):
    """A fallout-resolution job published to the AI orchestration topic."""

    task_id:          str     = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id:   str     = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type:        Literal["RESOLVE_FALLOUT", "REANALYSIS"] = "RESOLVE_FALLOUT"

    # Fallout context
    order_id:         Optional[int]  = None
    order_number:     Optional[str]  = None
    customer_id:      Optional[int]  = None
    error_code:       str            = ""
    error_message:    str            = ""
    domain:           str            = ""
    severity:         str            = "MEDIUM"
    free_text:        str            = ""

    # Re-analysis fields (Watchdog → Thinker)
    reanalysis:       bool           = False
    reanalysis_count: int            = 0
    stale_action_id:  Optional[str]  = None

    published_at:     str = Field(default_factory=lambda: datetime.utcnow().isoformat())

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── legacy.commands.writeback ──────────────────────────────────────────────────

class WritebackCommand(BaseModel):
    """
    An approved write instruction for Oracle, produced by the Relay service
    from the MongoDB outbox and consumed by the Oracle Adapter.
    """

    command_id:     str     = Field(default_factory=lambda: str(uuid.uuid4()))
    action_id:      str                     # MongoDB ActionProposal ID
    correlation_id: str
    action_type:    str                     # retry_provisioning, update_billing, etc.
    table_name:     str                     # Primary Oracle table to write to
    operation:      Literal["UPDATE", "INSERT", "CALL_PROC"] = "UPDATE"

    # The WHERE clause key-value pairs for the UPDATE target row
    where_clause:   dict[str, Any] = Field(default_factory=dict)
    # The SET clause key-value pairs (or proc arguments for CALL_PROC)
    set_values:     dict[str, Any] = Field(default_factory=dict)

    # Source version anchor for Saga idempotency (Oracle SCN or row UPDATED_DATE)
    source_version_anchor: Optional[str] = None

    outbox_id:      str
    published_at:   str = Field(default_factory=lambda: datetime.utcnow().isoformat())

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
