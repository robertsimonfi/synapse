"""
Pydantic-based message schemas for the Synapse Kafka topics.

These act as the "Schema Registry" contract — every producer validates its
message against the appropriate schema before publishing, and every consumer
deserialises into the schema on receipt. This ensures strict typing without
requiring a running Confluent Schema Registry in local dev.

Topics → Schema classes:
  oracle.cdc.all_changes    → OracleCdcEvent
  ai.orchestration.tasks    → OrchestrationTask
  legacy.commands.writeback → WritebackCommand
  legacy.commands.dlq       → DlqEvent  (Phase 10)

Phase 10 — Schema Evolution:
  All message types carry an optional trace_id (W3C traceparent) so the full
  Oracle-CDC → Hydrator → AI Thinker → Executor → Oracle-Adapter path can be
  reconstructed in a single distributed trace.
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
    trace_id:      Optional[str] = None  # Phase 10: W3C traceparent
    opco_id:       Optional[str] = None  # Phase 11: operational company for multi-tenancy
    metadata:      dict[str, Any] = Field(default_factory=dict)  # Phase 12: prefetch/canary tags

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── synapse.system.events — HIGH_SIGNAL (Phase 12) ────────────────────────────

class HighSignalEvent(BaseModel):
    """Published by CDC producer or external alerting when a sector-wide event warrants prefetch."""

    event_id:       str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type:     str = "HIGH_SIGNAL"
    signal_type:    Literal["OUTAGE", "MASS_FALLOUT", "CRITICAL_SPIKE"]
    opco_id:        str
    sector:         Optional[str]  = None   # e.g. "HLR-East", "BSSv2-South"
    affected_count: Optional[int]  = None   # estimated impacted subscribers
    source:         str            = "cdc_producer"
    ts:             str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    trace_id:       Optional[str]  = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── synapse.canary.results (Phase 12) ─────────────────────────────────────────

class CanaryResult(BaseModel):
    """Published after each resolution; consumed by the Canary Analyzer."""

    result_id:      str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str
    model_version:  str                     # e.g. "claude-sonnet-4-6", "claude-opus-4-7"
    opco_id:        Optional[str]  = None
    confidence:     str                     # HIGH / MEDIUM / LOW
    action_count:   int            = 0
    domain:         str            = ""
    resolved_at:    str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    # Operator feedback (filled later via /canary/{result_id}/score)
    operator_score: Optional[int]  = None   # 1 (poor) – 5 (excellent)
    approved:       Optional[bool] = None   # True if at least one action was approved
    trace_id:       Optional[str]  = None

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
    trace_id:         Optional[str] = None  # Phase 10: W3C traceparent
    opco_id:          Optional[str] = None  # Phase 11: operational company for multi-tenancy

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
    trace_id:       Optional[str] = None  # Phase 10: W3C traceparent
    opco_id:        Optional[str] = None  # Phase 11: operational company for multi-tenancy

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── legacy.commands.dlq (Phase 10) ────────────────────────────────────────────

class DlqEvent(BaseModel):
    """
    A failed Oracle write-back command routed to the Dead Letter Queue.
    Published by oracle_adapter after MAX_RETRIES exhaustion.
    Consumed by dlq_monitor which surfaces the event for NOC repair.
    """

    dlq_id:         str     = Field(default_factory=lambda: str(uuid.uuid4()))
    source_topic:   str     = "legacy.commands.writeback"
    # Original command fields (copied verbatim for full auditability)
    command_id:     Optional[str] = None
    outbox_id:      str
    action_id:      str
    correlation_id: str
    action_type:    str
    table_name:     str
    where_clause:   dict[str, Any] = Field(default_factory=dict)
    set_values:     dict[str, Any] = Field(default_factory=dict)
    source_version_anchor: Optional[str] = None
    # Failure metadata
    error_context:  str             # Last exception message
    attempts:       int             # Number of retries attempted
    failed_at:      str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    trace_id:       Optional[str] = None  # W3C traceparent inherited from original command
    opco_id:        Optional[str] = None  # Phase 11: operational company for multi-tenancy

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── synapse.customer.feedback (Phase 14) ──────────────────────────────────────

class CustomerFeedbackEvent(BaseModel):
    """
    Inbound customer feedback from any channel (SMS reply, IVR, web form).
    Published by the webhook/engagement handler; consumed by the Sentiment worker.
    """

    feedback_id:    str = Field(default_factory=lambda: str(uuid.uuid4()))
    customer_id:    Optional[int]  = None
    engagement_id:  Optional[str]  = None   # links back to EngagementLog.id
    correlation_id: Optional[str]  = None
    channel:        str            = "UNKNOWN"   # SMS, EMAIL, WHATSAPP, IVR, WEB
    raw_text:       str                          # original customer message
    opco_id:        Optional[str]  = None
    received_at:    str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    trace_id:       Optional[str]  = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}


# ── synapse.operator.corrections (Phase 14) ───────────────────────────────────

class OperatorCorrectionEvent(BaseModel):
    """
    Human feedback from the NOC operator correcting or annotating an AI decision.
    Published via POST /actions/{id}/feedback; consumed by future training pipelines.
    """

    correction_id:        str = Field(default_factory=lambda: str(uuid.uuid4()))
    action_id:            str
    correlation_id:       str
    operator_id:          str
    original_action_type: str
    proposed_action:      str            # what the AI recommended
    correct_action:       Optional[str]  = None   # what the operator says should have happened
    quality_score:        int            = 3      # 1 (wrong) – 5 (excellent)
    feedback_notes:       Optional[str]  = None   # free-text operator rationale
    opco_id:              Optional[str]  = None
    submitted_at:         str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    trace_id:             Optional[str]  = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
