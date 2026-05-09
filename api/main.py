"""
Synapse — Telco Fallout Resolution Agent — FastAPI REST + WebSocket layer.

Endpoints:
  POST /resolve                    — Analyze a fallout; persist action proposals to MongoDB
  POST /customer-360               — Comprehensive customer profile (with opco_id)
  GET  /health                     — Health check
  GET  /system/status              — Circuit breaker state + WebSocket connections  (Phase 11)
  GET  /memory/stats               — Memory store statistics
  POST /memory/ingest              — Re-ingest runbooks into ChromaDB

HITL action management:
  GET  /actions                    — List actions (filter by ?status=PENDING)
  GET  /actions/depths             — Queue depth per status
  GET  /actions/{id}               — Single action detail
  POST /actions/{id}/approve       — Operator approves an action
  POST /actions/{id}/force-approve — Force-execute ESCALATED_STALE actions (Phase 10)
  POST /actions/{id}/reject        — Operator rejects an action (reason required)
  GET  /actions/{id}/audit         — Full audit trail for an action

Phase 7 — Proactive Engagement:
  GET  /engagements                — List engagement log entries
  POST /webhooks/customer-reply    — Inbound customer reply from SMS/Email gateway

Phase 10 — DLQ:
  GET  /dlq                        — List Dead Letter Queue events
  POST /dlq/{outbox_id}/acknowledge — Mark DLQ event as acknowledged

Phase 11 — Real-time NOC Dashboard:
  WS   /ws/noc/{opco_id}           — Live event stream per OpCo (or "ALL")

Phase 12 — Advanced Reliability:
  POST /customer/{customer_id}/force-sync — Bypass Kafka; rebuild ODS doc from Oracle
  GET  /canary/status              — Head-to-head champion vs. candidate model KPIs
  POST /canary/promote             — Promote candidate model to 100% traffic
  POST /canary/{result_id}/score   — Backfill operator quality score onto a canary result

Phase 14 — GA Sprint (Sentiment + React Dashboard):
  GET  /system/trace/{trace_id}    — Chronological event timeline for a distributed trace
  POST /actions/{id}/feedback      — Operator correction published to synapse.operator.corrections

Observability:
  GET  /metrics                    — Prometheus metrics (scraped by Prometheus)
"""

import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Security, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from pydantic import BaseModel, Field, field_validator

from agent.agent import (
    Customer360Input,
    FalloutInput,
    run_agent,
    run_customer_360,
)
from agent.memory import memory_stats
from api.security import require_api_key
from api.websocket_manager import manager as ws_manager, noc_event, VALID_OPCO_IDS
from api.canary_routes import router as canary_router  # Phase 12
from models.action import ApproveRequest, ForceApproveRequest, RejectRequest
from rag.knowledge_base import ingest_runbooks

logger = logging.getLogger(__name__)

_mongo_enabled = bool(os.getenv("MONGODB_URI"))


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    if _mongo_enabled:
        from db.mongo import ensure_indexes
        await ensure_indexes()
        logger.info("MongoDB indexes ready")

    from jobs.expiry import setup_expiry_scheduler
    scheduler = setup_expiry_scheduler()
    if _mongo_enabled:
        scheduler.start()
        logger.info("Expiry scheduler started")

    from observability.tracing import setup_tracing
    setup_tracing("synapse-api")

    yield

    # Shutdown
    if _mongo_enabled and scheduler.running:
        scheduler.shutdown(wait=False)


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Synapse — Telco Fallout Resolution Agent",
    description=(
        "AI-powered BSS/OSS fallout detection, root cause analysis, HITL action management, "
        "and resolution. Powered by Claude."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics at /metrics
app.mount("/metrics", make_asgi_app())

# Phase 12: canary analysis routes
app.include_router(canary_router)


# ── Resolve schemas ────────────────────────────────────────────────────────────

class ResolveRequest(BaseModel):
    order_id:      Optional[int]   = Field(None, description="Order ID from ORDER_HEADER")
    order_number:  Optional[str]   = Field(None, description="e.g. ORD-2024001")
    customer_id:   Optional[int]   = Field(None, description="Customer ID from CUSTOMER")
    error_code:    str             = Field("",   description="e.g. PROV_001, BILL_003")
    error_message: str             = Field("",   description="Human-readable error message")
    domain:        str             = Field("",   description="BSS, OSS, BILLING, NETWORK, etc.")
    severity:      str             = Field("MEDIUM", description="CRITICAL, HIGH, MEDIUM, LOW")
    free_text:     Optional[str]   = Field(None, description="Any additional operator context")
    opco_id:       str             = Field("DEFAULT", description="OpCo for multi-tenancy isolation")

    @field_validator("opco_id")
    @classmethod
    def validate_opco(cls, v: str) -> str:
        if v not in VALID_OPCO_IDS:
            raise ValueError(f"opco_id must be one of: {sorted(VALID_OPCO_IDS)}")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "order_id": 42,
                "order_number": "ORD-2024042",
                "customer_id": 7,
                "error_code": "PROV_001",
                "error_message": "HLR activation timeout after 30s",
                "domain": "PROVISIONING",
                "severity": "HIGH",
                "free_text": "Third retry attempt — still failing on HLR-East cluster",
                "opco_id": "VODAFONE_UK",
            }
        }
    }


class ClassificationOut(BaseModel):
    domain:     str
    issue_type: str
    severity:   str


class ResolveResponse(BaseModel):
    correlation_id:      str
    classification:      ClassificationOut
    root_cause:          str
    recommended_actions: list[str]
    runbook_references:  list[str]
    confidence:          str
    action_proposal_ids: list[str] = Field(
        default_factory=list,
        description="MongoDB IDs of PENDING action proposals created for human review",
    )
    critique_quality:    Optional[str] = None
    critique_lesson:     Optional[str] = None
    # Phase 6 metadata
    routing_domain:      Optional[str] = None
    routing_confidence:  Optional[float] = None
    critic_violations:   Optional[list[str]] = None
    policy_violations:   Optional[list[str]] = None


class HealthResponse(BaseModel):
    status:  str
    version: str


class MemoryStatsResponse(BaseModel):
    total_memories: int
    collection:     str


class Customer360Request(BaseModel):
    customer_id: int           = Field(..., description="Customer ID to build 360 profile for")
    query:       Optional[str] = Field(None, description="Specific question e.g. 'Why is my bill high?'")
    opco_id:     str           = Field("DEFAULT", description="OpCo for multi-tenancy isolation")

    @field_validator("opco_id")
    @classmethod
    def validate_opco(cls, v: str) -> str:
        if v not in VALID_OPCO_IDS:
            raise ValueError(f"opco_id must be one of: {sorted(VALID_OPCO_IDS)}")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "customer_id": 7,
                "query": "Why did my bill increase last month?",
                "opco_id": "VODAFONE_UK",
            }
        }
    }


class Customer360Response(BaseModel):
    customer_id:               int
    profile_summary:           dict
    active_services:           list
    billing_health:            dict
    usage_analysis:            dict
    open_issues:               dict
    proactive_recommendations: list


# ── HITL action schemas ────────────────────────────────────────────────────────

class ActionOut(BaseModel):
    id:                     str
    correlation_id:         str
    order_id:               Optional[str]
    customer_id:            Optional[str]
    action_type:            str
    action_description:     str
    action_payload:         dict[str, Any]
    predicted_before_state: dict[str, Any]
    predicted_after_state:  dict[str, Any]
    status:                 str
    version:                int
    created_at:             str
    updated_at:             str
    expires_at:             str
    approved_by:            Optional[str]
    rejection_reason:       Optional[str]
    retry_count:            int
    executed_at:            Optional[str]
    execution_error:        Optional[str]


class AuditOut(BaseModel):
    id:              str
    action_id:       str
    timestamp:       str
    previous_status: Optional[str]
    new_status:      str
    actor:           str
    metadata:        dict[str, Any]


def _action_to_out(a) -> ActionOut:
    return ActionOut(
        id=a.id,
        correlation_id=a.correlation_id,
        order_id=a.order_id,
        customer_id=a.customer_id,
        action_type=a.action_type,
        action_description=a.action_description,
        action_payload=a.action_payload,
        predicted_before_state=a.predicted_before_state,
        predicted_after_state=a.predicted_after_state,
        status=a.status.value,
        version=a.version,
        created_at=a.created_at.isoformat(),
        updated_at=a.updated_at.isoformat(),
        expires_at=a.expires_at.isoformat(),
        approved_by=a.approved_by,
        rejection_reason=a.rejection_reason,
        retry_count=a.retry_count,
        executed_at=a.executed_at.isoformat() if a.executed_at else None,
        execution_error=a.execution_error,
    )


def _audit_to_out(e) -> AuditOut:
    return AuditOut(
        id=e.id,
        action_id=e.action_id,
        timestamp=e.timestamp.isoformat(),
        previous_status=e.previous_status.value if e.previous_status else None,
        new_status=e.new_status.value,
        actor=e.actor,
        metadata=e.metadata,
    )


def _require_mongo():
    if not _mongo_enabled:
        raise HTTPException(
            status_code=503,
            detail="MongoDB not configured. Set MONGODB_URI to enable action management.",
        )


# ── System endpoints ───────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/system/status", tags=["System"])
def system_status():
    """NOC dashboard: WebSocket connections, circuit breaker state per OpCo."""
    from observability.metrics import kafka_circuit_breaker_stale, kafka_consumer_lag_seconds
    # Read the current Prometheus gauge values
    try:
        stale = kafka_circuit_breaker_stale.labels(topic="oracle.cdc.all_changes")._value.get()
        lag   = kafka_consumer_lag_seconds.labels(
            topic="oracle.cdc.all_changes", group_id="synapse-hydrator"
        )._value.get()
    except Exception:
        stale, lag = None, None

    return {
        "circuit_breaker": {
            "status":      "SYSTEM_STALE" if stale else "HEALTHY",
            "lag_seconds": round(lag, 1) if lag is not None else None,
            "threshold":   int(os.getenv("HYDRATOR_LAG_THRESHOLD_SECONDS", "300")),
        },
        "websocket_connections": ws_manager.connection_count(),
        "connected_opcos":       ws_manager.connected_opcos(),
        "version": "2.0.0",
    }


@app.get("/memory/stats", response_model=MemoryStatsResponse, tags=["Memory"])
def get_memory_stats(_: str = Security(require_api_key)):
    return memory_stats()


@app.post("/memory/ingest", tags=["Memory"])
def ingest_knowledge_base(force: bool = False, _: str = Security(require_api_key)):
    count = ingest_runbooks(force=force)
    return {"status": "ok", "chunks_ingested": count}


# ── Agent endpoint ─────────────────────────────────────────────────────────────

@app.post("/resolve", response_model=ResolveResponse, tags=["Agent"])
async def resolve_fallout(request: ResolveRequest, _: str = Security(require_api_key)):
    """
    Submit a Telco fallout for AI-powered root cause analysis.

    The agent investigates using database tools, identifies the root cause, and
    proposes structured actions. Each action is stored in MongoDB as PENDING,
    awaiting human approval in the Operator Portal before execution.
    """
    if not request.order_id and not request.order_number and not request.error_code:
        raise HTTPException(
            status_code=422,
            detail="Provide at least one of: order_id, order_number, or error_code",
        )

    correlation_id = str(uuid.uuid4())

    fallout_input = FalloutInput(
        order_id=request.order_id,
        order_number=request.order_number,
        customer_id=request.customer_id,
        error_code=request.error_code,
        error_message=request.error_message,
        domain=request.domain,
        severity=request.severity,
        free_text=request.free_text or "",
    )

    try:
        resolution = await asyncio.to_thread(run_agent, fallout_input, correlation_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {e}")

    # Persist action proposals to MongoDB for HITL review
    proposal_ids: list[str] = []
    if _mongo_enabled and resolution.action_proposals:
        from db.mongo import create_action
        from models.action import ActionProposal
        from observability.metrics import actions_pending_total

        for raw in resolution.action_proposals:
            proposal = ActionProposal(
                correlation_id=correlation_id,
                order_id=str(request.order_id) if request.order_id else None,
                customer_id=str(request.customer_id) if request.customer_id else None,
                action_type=raw.get("action_type", "unknown"),
                action_description=raw.get("action_description", ""),
                action_payload=raw.get("action_payload", {}),
                predicted_before_state=raw.get("predicted_before_state", {}),
                predicted_after_state=raw.get("predicted_after_state", {}),
            )
            await create_action(proposal)
            proposal_ids.append(proposal.id)
            actions_pending_total.labels(action_type=proposal.action_type).inc()

    classification = resolution.classification or {}
    critique       = resolution.critique or {}

    # Phase 11: broadcast live event to NOC WebSocket subscribers
    await ws_manager.broadcast(
        request.opco_id,
        noc_event(
            "resolution_proposed",
            request.opco_id,
            correlation_id=correlation_id,
            error_code=request.error_code,
            domain=classification.get("domain", request.domain),
            severity=classification.get("severity", request.severity),
            confidence=resolution.confidence,
            action_count=len(proposal_ids),
        ),
    )

    return ResolveResponse(
        correlation_id=correlation_id,
        classification=ClassificationOut(
            domain=classification.get("domain", request.domain or "UNKNOWN"),
            issue_type=classification.get("issue_type", "Unknown"),
            severity=classification.get("severity", request.severity),
        ),
        root_cause=resolution.root_cause,
        recommended_actions=resolution.recommended_actions,
        runbook_references=resolution.runbook_references,
        confidence=resolution.confidence,
        action_proposal_ids=proposal_ids,
        critique_quality=critique.get("quality"),
        critique_lesson=critique.get("lesson"),
        routing_domain=resolution.routing_domain,
        routing_confidence=resolution.routing_confidence,
        critic_violations=resolution.critic_violations or None,
        policy_violations=resolution.policy_violations or None,
    )


@app.post("/customer-360", response_model=Customer360Response, tags=["Agent"])
async def customer_360(request: Customer360Request, _: str = Security(require_api_key)):
    """Build a comprehensive Customer 360 profile for a given customer ID."""
    try:
        summary = await asyncio.to_thread(
            run_customer_360,
            Customer360Input(customer_id=request.customer_id, query=request.query or ""),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {e}")

    return Customer360Response(
        customer_id=summary.customer_id,
        profile_summary=summary.profile_summary,
        active_services=summary.active_services,
        billing_health=summary.billing_health,
        usage_analysis=summary.usage_analysis,
        open_issues=summary.open_issues,
        proactive_recommendations=summary.proactive_recommendations,
    )


# ── HITL action management endpoints ──────────────────────────────────────────

@app.get("/actions/depths", tags=["HITL"])
async def get_queue_depths(_: str = Security(require_api_key)):
    """Return the count of actions in each status (for dashboard metrics)."""
    _require_mongo()
    from db.mongo import get_queue_depths
    return await get_queue_depths()


@app.get("/actions", response_model=list[ActionOut], tags=["HITL"])
async def list_actions_endpoint(
    status: Optional[str] = None,
    limit: int = 100,
    _: str = Security(require_api_key),
):
    """
    List action proposals. Filter by ?status=PENDING|APPROVED|REJECTED|EXECUTING|EXECUTED|FAILED|EXPIRED.
    """
    _require_mongo()
    from db.mongo import list_actions
    from models.action import ActionStatus

    status_filter = None
    if status:
        try:
            status_filter = ActionStatus(status.upper())
        except ValueError:
            raise HTTPException(status_code=422, detail=f"Unknown status: {status}")

    actions = await list_actions(status=status_filter, limit=min(limit, 500))
    return [_action_to_out(a) for a in actions]


@app.get("/actions/{action_id}", response_model=ActionOut, tags=["HITL"])
async def get_action_endpoint(action_id: str, _: str = Security(require_api_key)):
    """Retrieve a single action proposal with its full state and diff payload."""
    _require_mongo()
    from db.mongo import get_action

    action = await get_action(action_id)
    if not action:
        raise HTTPException(status_code=404, detail=f"Action {action_id} not found")
    return _action_to_out(action)


@app.post("/actions/{action_id}/approve", response_model=ActionOut, tags=["HITL"])
async def approve_action(
    action_id: str,
    body: ApproveRequest,
    _: str = Security(require_api_key),
):
    """
    Operator approves a PENDING action.

    Records the operator's identity in the audit trail and transitions the
    action to APPROVED. The executor service will pick it up for execution.
    """
    _require_mongo()
    from db.mongo import get_action, transition_action
    from models.action import ActionStatus
    from observability.metrics import actions_approved_total

    action = await get_action(action_id)
    if not action:
        raise HTTPException(status_code=404, detail=f"Action {action_id} not found")

    try:
        updated = await transition_action(
            action_id, action.version, ActionStatus.APPROVED,
            actor=body.operator_id,
            extra_updates={"approved_by": body.operator_id},
        )
    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=409, detail=str(e))

    if updated is None:
        raise HTTPException(
            status_code=409,
            detail="Concurrent update detected — please refresh and try again.",
        )

    actions_approved_total.labels(action_type=action.action_type).inc()
    logger.info(
        "Action %s APPROVED by %s (correlation=%s)",
        action_id, body.operator_id, action.correlation_id,
    )
    # Phase 11: broadcast to all NOC subscribers (action doesn't carry opco_id yet)
    await ws_manager.broadcast(
        "ALL",
        noc_event(
            "action_approved",
            "ALL",
            action_id=action_id,
            action_type=action.action_type,
            operator_id=body.operator_id,
            correlation_id=action.correlation_id,
        ),
    )
    return _action_to_out(updated)


@app.post("/actions/{action_id}/force-approve", response_model=ActionOut, tags=["HITL"])
async def force_approve_action(
    action_id: str,
    body: ForceApproveRequest,
    _: str = Security(require_api_key),
):
    """
    Phase 10 — Operator manual override for ESCALATED_STALE actions.

    Transitions the action to FORCE_APPROVED, which the Executor picks up and
    executes immediately, skipping the pre-flight drift check. Requires a
    mandatory justification for the audit trail.
    """
    _require_mongo()
    from db.mongo import get_action, transition_action
    from models.action import ActionStatus

    action = await get_action(action_id)
    if not action:
        raise HTTPException(status_code=404, detail=f"Action {action_id} not found")

    if action.status != ActionStatus.ESCALATED_STALE:
        raise HTTPException(
            status_code=409,
            detail=f"Force-approve only applies to ESCALATED_STALE actions (current: {action.status})",
        )

    try:
        updated = await transition_action(
            action_id, action.version, ActionStatus.FORCE_APPROVED,
            actor=body.operator_id,
            extra_updates={"force_justification": body.justification},
        )
    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=409, detail=str(e))

    if updated is None:
        raise HTTPException(
            status_code=409,
            detail="Concurrent update detected — please refresh and try again.",
        )

    logger.warning(
        "Action %s FORCE_APPROVED by %s — justification: %s",
        action_id, body.operator_id, body.justification,
    )
    # Phase 11: broadcast — force-approve is a significant NOC event
    await ws_manager.broadcast(
        "ALL",
        noc_event(
            "force_approved",
            "ALL",
            action_id=action_id,
            action_type=action.action_type,
            operator_id=body.operator_id,
            justification=body.justification,
            correlation_id=action.correlation_id,
        ),
    )
    return _action_to_out(updated)


@app.post("/actions/{action_id}/reject", response_model=ActionOut, tags=["HITL"])
async def reject_action(
    action_id: str,
    body: RejectRequest,
    _: str = Security(require_api_key),
):
    """
    Operator rejects a PENDING action with a mandatory reason.

    The rejection reason is stored and can be used to fine-tune the agent's
    logic and understand systematic over-recommendations.
    """
    _require_mongo()
    from db.mongo import get_action, transition_action
    from models.action import ActionStatus
    from observability.metrics import actions_rejected_total

    action = await get_action(action_id)
    if not action:
        raise HTTPException(status_code=404, detail=f"Action {action_id} not found")

    try:
        updated = await transition_action(
            action_id, action.version, ActionStatus.REJECTED,
            actor=body.operator_id,
            extra_updates={"rejection_reason": body.reason},
        )
    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=409, detail=str(e))

    if updated is None:
        raise HTTPException(
            status_code=409,
            detail="Concurrent update detected — please refresh and try again.",
        )

    actions_rejected_total.labels(action_type=action.action_type).inc()
    logger.info(
        "Action %s REJECTED by %s — reason: %s",
        action_id, body.operator_id, body.reason,
    )
    return _action_to_out(updated)


@app.get("/actions/{action_id}/audit", response_model=list[AuditOut], tags=["HITL"])
async def get_audit_trail_endpoint(action_id: str, _: str = Security(require_api_key)):
    """Return the full chronological audit trail for a single action."""
    _require_mongo()
    from db.mongo import get_audit_trail

    events = await get_audit_trail(action_id)
    return [_audit_to_out(e) for e in events]


# ── Phase 7: Engagement endpoints ─────────────────────────────────────────────


class EngagementOut(BaseModel):
    id:                   str
    action_id:            str
    correlation_id:       str
    customer_id:          Optional[str]
    channel:              str
    template_used:        str
    message_body:         str
    recipient_masked:     str
    sent_at:              Optional[str]
    status:               str
    customer_response:    str
    response_text:        Optional[str]
    response_received_at: Optional[str]
    created_at:           str


class CustomerReplyRequest(BaseModel):
    correlation_id: str  = Field(..., description="Links reply to the original engagement")
    reply_text:     str  = Field(..., description="Raw text of the customer's reply")
    channel:        str  = Field("SMS", description="SMS | EMAIL | WHATSAPP")
    from_address:   Optional[str] = Field(None, description="Phone/email of sender (for audit)")


def _engagement_to_out(e) -> EngagementOut:
    return EngagementOut(
        id=e.id,
        action_id=e.action_id,
        correlation_id=e.correlation_id,
        customer_id=e.customer_id,
        channel=e.channel.value,
        template_used=e.template_used,
        message_body=e.message_body,
        recipient_masked=e.recipient_masked,
        sent_at=e.sent_at.isoformat() if e.sent_at else None,
        status=e.status.value,
        customer_response=e.customer_response.value,
        response_text=e.response_text,
        response_received_at=e.response_received_at.isoformat() if e.response_received_at else None,
        created_at=e.created_at.isoformat(),
    )


@app.get("/engagements", response_model=list[EngagementOut], tags=["Engagement"])
async def list_engagements_endpoint(
    action_id:   Optional[str] = None,
    customer_id: Optional[str] = None,
    limit:       int = 100,
    _: str = Security(require_api_key),
):
    """List engagement log entries, optionally filtered by action or customer."""
    _require_mongo()
    from db.mongo import list_engagements
    logs = await list_engagements(limit=min(limit, 500), action_id=action_id, customer_id=customer_id)
    return [_engagement_to_out(e) for e in logs]


@app.post("/webhooks/customer-reply", tags=["Engagement"])
async def customer_reply_webhook(request: CustomerReplyRequest):
    """
    Inbound customer reply hook — called by SMS/Email gateways.

    Looks up the engagement by correlation_id, records the reply, and publishes
    a CUSTOMER_FEEDBACK event to RabbitMQ so the Thinker can decide next steps.
    No API key required so that gateway services can POST without credentials.
    """
    _require_mongo()
    from datetime import timezone as _tz
    from db.mongo import get_engagement_by_correlation, record_customer_response

    engagement = await get_engagement_by_correlation(request.correlation_id)
    if not engagement:
        raise HTTPException(
            status_code=404,
            detail=f"No engagement found for correlation_id={request.correlation_id}",
        )

    received_at = datetime.now(_tz.utc)
    updated = await record_customer_response(engagement.id, request.reply_text, received_at)

    # Phase 14: publish CustomerFeedbackEvent to Kafka for sentiment scoring
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_servers:
        try:
            from kafka.producer import create_producer, publish
            from kafka.schemas import CustomerFeedbackEvent
            from kafka.topics import TOPIC_CUSTOMER_FEEDBACK

            feedback_evt = CustomerFeedbackEvent(
                customer_id=int(engagement.customer_id) if engagement.customer_id else None,
                engagement_id=engagement.id,
                correlation_id=request.correlation_id,
                channel=request.channel,
                raw_text=request.reply_text,
            )
            producer = await create_producer()
            async with producer:
                await publish(
                    producer,
                    TOPIC_CUSTOMER_FEEDBACK,
                    feedback_evt.model_dump(),
                    key=engagement.id,
                )
            logger.info(
                "CUSTOMER_FEEDBACK published to Kafka correlation=%s engagement=%s",
                request.correlation_id, engagement.id,
            )
        except Exception as exc:
            logger.warning("Kafka publish failed for customer feedback: %s", exc)

    return {
        "status":         "recorded",
        "engagement_id":  updated.id if updated else engagement.id,
        "customer_response": updated.customer_response.value if updated else "REPLIED",
    }


# ── Phase 10: DLQ endpoints ────────────────────────────────────────────────────

@app.get("/dlq", tags=["DLQ"])
async def list_dlq_events_endpoint(
    status: str = "OPEN",
    limit: int = 100,
    _: str = Security(require_api_key),
):
    """
    List Dead Letter Queue events.
    status: OPEN | ACKNOWLEDGED | ALL
    """
    _require_mongo()
    from db.ods import list_dlq_events
    return await list_dlq_events(status=status, limit=min(limit, 500))


@app.post("/dlq/{outbox_id}/acknowledge", tags=["DLQ"])
async def acknowledge_dlq_event_endpoint(
    outbox_id: str,
    body: ApproveRequest,
    _: str = Security(require_api_key),
):
    """Mark a DLQ event as acknowledged by an operator."""
    _require_mongo()
    from db.ods import acknowledge_dlq_event
    await acknowledge_dlq_event(outbox_id, body.operator_id)
    return {"status": "acknowledged", "outbox_id": outbox_id, "by": body.operator_id}


# ── Phase 14: Timeline API ────────────────────────────────────────────────────


@app.get("/system/trace/{trace_id}", tags=["System"])
async def get_trace_timeline(
    trace_id: str,
    _: str = Security(require_api_key),
):
    """
    Phase 14 — Distributed Trace Timeline.

    Aggregates all events that share the given trace_id (W3C traceparent) into a
    chronological JSON array.  Each event carries a `node_type` tag used by the
    React Timeline component to colour-code the node:
      blue  — AI_REASONING  (resolution proposed, action created, reanalysis)
      green — ORACLE_WRITE  (write-back completed, force-sync)
      red   — DLQ_FAILURE   (dead-letter queue event)
      amber — OPERATOR_HITL (approval, rejection, force-approve)
      grey  — ENGAGEMENT    (customer notification, reply)

    Falls back to correlation_id lookup when trace_id is a plain UUID.
    """
    _require_mongo()
    from db.mongo import get_db

    db = await get_db()

    # Primary key for the trace is the W3C traceparent; many documents store
    # the correlation_id *as* their trace_id, so we search both fields.
    q = {"$or": [{"trace_id": trace_id}, {"correlation_id": trace_id}]}

    events: list[dict[str, Any]] = []

    # ── Action proposals ──────────────────────────────────────────────────
    async for doc in db.action_proposals.find(q, {"_id": 0}):
        ts = doc.get("created_at", "")
        correlation_id = doc.get("correlation_id", "")
        events.append({
            "event_id":   str(doc.get("id", uuid.uuid4())),
            "trace_id":   trace_id,
            "node_type":  "AI_REASONING",
            "event_type": "ACTION_CREATED",
            "timestamp":  ts,
            "label":      f"AI proposed: {doc.get('action_type', '?')}",
            "detail": {
                "action_id":     doc.get("id"),
                "action_type":   doc.get("action_type"),
                "description":   doc.get("action_description"),
                "confidence":    doc.get("action_payload", {}).get("confidence"),
                "correlation_id": correlation_id,
            },
        })
        # Status transition events from the same action
        for status, field, evt, node in [
            ("APPROVED",  "updated_at", "ACTION_APPROVED",  "OPERATOR_HITL"),
            ("REJECTED",  "updated_at", "ACTION_REJECTED",  "OPERATOR_HITL"),
            ("EXECUTED",  "executed_at", "ORACLE_WRITE_COMPLETED", "ORACLE_WRITE"),
            ("FAILED",    "updated_at", "ACTION_FAILED",    "DLQ_FAILURE"),
            ("ESCALATED_STALE", "updated_at", "ACTION_ESCALATED", "OPERATOR_HITL"),
        ]:
            if doc.get("status") == status and doc.get(field):
                events.append({
                    "event_id":   str(uuid.uuid4()),
                    "trace_id":   trace_id,
                    "node_type":  node,
                    "event_type": evt,
                    "timestamp":  doc.get(field, ""),
                    "label":      f"{evt.replace('_', ' ').title()}: {doc.get('action_type', '?')}",
                    "detail": {
                        "action_id":       doc.get("id"),
                        "operator":        doc.get("approved_by"),
                        "rejection_reason": doc.get("rejection_reason"),
                        "execution_error": doc.get("execution_error"),
                    },
                })

    # ── DLQ events ────────────────────────────────────────────────────────
    async for doc in db.dlq_events.find(q, {"_id": 0}):
        events.append({
            "event_id":   str(doc.get("dlq_id", uuid.uuid4())),
            "trace_id":   trace_id,
            "node_type":  "DLQ_FAILURE",
            "event_type": "DLQ_FAILURE",
            "timestamp":  doc.get("failed_at", ""),
            "label":      f"DLQ: {doc.get('action_type', '?')} — {doc.get('table_name', '?')}",
            "detail": {
                "outbox_id":     doc.get("outbox_id"),
                "error_context": doc.get("error_context"),
                "attempts":      doc.get("attempts"),
                "table_name":    doc.get("table_name"),
            },
        })

    # ── Engagement / customer interaction events ───────────────────────────
    async for doc in db.engagement_logs.find(q, {"_id": 0}):
        events.append({
            "event_id":   str(doc.get("id", uuid.uuid4())),
            "trace_id":   trace_id,
            "node_type":  "ENGAGEMENT",
            "event_type": "ENGAGEMENT_SENT",
            "timestamp":  doc.get("sent_at") or doc.get("created_at", ""),
            "label":      f"Customer notified via {doc.get('channel', '?')}",
            "detail": {
                "channel":          doc.get("channel"),
                "template":         doc.get("template_used"),
                "status":           doc.get("status"),
                "customer_response": doc.get("customer_response"),
                "sentiment_score":  doc.get("sentiment_score"),
                "sentiment_label":  doc.get("sentiment_label"),
            },
        })
        if doc.get("response_received_at"):
            events.append({
                "event_id":   str(uuid.uuid4()),
                "trace_id":   trace_id,
                "node_type":  "ENGAGEMENT",
                "event_type": "CUSTOMER_REPLIED",
                "timestamp":  doc.get("response_received_at", ""),
                "label":      f"Customer replied ({doc.get('sentiment_label', 'UNKNOWN')})",
                "detail": {
                    "response":        doc.get("customer_response"),
                    "sentiment_score": doc.get("sentiment_score"),
                    "sentiment_label": doc.get("sentiment_label"),
                },
            })

    # ── Audit trail entries ───────────────────────────────────────────────
    async for doc in db.audit_events.find(
        {"$or": [{"trace_id": trace_id}, {"metadata.correlation_id": trace_id}]},
        {"_id": 0},
    ):
        if doc.get("new_status") in ("APPROVED", "REJECTED", "FORCE_APPROVED"):
            events.append({
                "event_id":   str(doc.get("id", uuid.uuid4())),
                "trace_id":   trace_id,
                "node_type":  "OPERATOR_HITL",
                "event_type": f"AUDIT_{doc.get('new_status', 'TRANSITION')}",
                "timestamp":  doc.get("timestamp", ""),
                "label":      f"Operator: {doc.get('new_status', '?')} by {doc.get('actor', '?')}",
                "detail":     doc.get("metadata", {}),
            })

    if not events:
        raise HTTPException(
            status_code=404,
            detail=f"No trace events found for trace_id={trace_id}",
        )

    # Sort chronologically; fall back to empty string for missing timestamps
    events.sort(key=lambda e: e.get("timestamp") or "")
    return {"trace_id": trace_id, "event_count": len(events), "events": events}


# ── Phase 14: Human Feedback endpoint ─────────────────────────────────────────


class HumanFeedbackRequest(BaseModel):
    operator_id:    str            = Field(..., description="Operator submitting the correction")
    quality_score:  int            = Field(..., ge=1, le=5, description="1 (wrong) – 5 (excellent)")
    correct_action: Optional[str]  = Field(None, description="What should the AI have proposed?")
    feedback_notes: Optional[str]  = Field(None, description="Free-text rationale for the correction")

    model_config = {
        "json_schema_extra": {
            "example": {
                "operator_id":    "noc-lead@vodafone.com",
                "quality_score":  2,
                "correct_action": "escalate",
                "feedback_notes": "Customer has VIP flag — should always escalate, not offer credit",
            }
        }
    }


@app.post("/actions/{action_id}/feedback", tags=["HITL"])
async def submit_action_feedback(
    action_id: str,
    body: HumanFeedbackRequest,
    _: str = Security(require_api_key),
):
    """
    Phase 14 — Human Feedback / Operator Correction.

    Publishes an OperatorCorrectionEvent to the synapse.operator.corrections Kafka
    topic so future fine-tuning pipelines can learn from NOC operator expertise.

    The feedback is also stored in MongoDB for offline analysis.
    """
    _require_mongo()
    from db.mongo import get_db

    db = await get_db()
    action_doc = await db.action_proposals.find_one({"id": action_id}, {"_id": 0})
    if not action_doc:
        raise HTTPException(status_code=404, detail=f"Action {action_id} not found")

    from kafka.schemas import OperatorCorrectionEvent
    from kafka.topics import TOPIC_OPERATOR_CORRECTIONS

    correction = OperatorCorrectionEvent(
        action_id=action_id,
        correlation_id=action_doc.get("correlation_id", ""),
        operator_id=body.operator_id,
        original_action_type=action_doc.get("action_type", ""),
        proposed_action=action_doc.get("action_description", ""),
        correct_action=body.correct_action,
        quality_score=body.quality_score,
        feedback_notes=body.feedback_notes,
        opco_id=action_doc.get("opco_id"),
        trace_id=action_doc.get("trace_id"),
    )

    # Persist to MongoDB for offline analytics
    await db.operator_corrections.insert_one(correction.model_dump())

    # Publish to Kafka (best-effort; non-fatal if Kafka unavailable)
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_servers:
        try:
            from kafka.producer import create_producer, publish
            producer = await create_producer()
            async with producer:
                await publish(
                    producer,
                    TOPIC_OPERATOR_CORRECTIONS,
                    correction.model_dump(),
                    key=action_id,
                )
        except Exception as exc:
            logger.warning("Could not publish operator correction to Kafka: %s", exc)

    logger.info(
        "Operator feedback: action=%s score=%d operator=%s",
        action_id, body.quality_score, body.operator_id,
    )
    return {
        "status":        "feedback_recorded",
        "correction_id": correction.correction_id,
        "action_id":     action_id,
        "quality_score": body.quality_score,
    }


# ── Phase 12: Force Sync — per-customer Oracle bypass for SYSTEM_STALE state ──
#
# Design rationale (architect-to-architect):
#   When the Kafka circuit breaker trips (SYSTEM_STALE), the NOC has two options:
#     A) Wait for the pipeline to drain naturally — correct for bulk traffic.
#     B) Force-sync a specific customer — correct when an agent is on the call NOW.
#
#   This endpoint implements (B): a point-lookup override that fetches ONE
#   subscriber directly from Oracle, tokenises PII, and writes to MongoDB ODS,
#   bypassing Kafka entirely.  The Kafka pipeline continues recovering in
#   parallel — this does NOT reset the consumer offset or flush the queue.
#
#   Force-sync requires an operator justification and produces an audit entry
#   so the action is fully traceable, identical to force-approve on actions.


class ForceSyncRequest(BaseModel):
    opco_id:       str = Field("DEFAULT", description="OpCo for multi-tenancy isolation")
    operator_id:   str = Field(..., description="Operator triggering the override")
    justification: str = Field(..., description="Reason for bypassing the Kafka pipeline")

    model_config = {
        "json_schema_extra": {
            "example": {
                "opco_id":       "VODAFONE_UK",
                "operator_id":   "noc-lead@vodafone.com",
                "justification": "Customer on live call — Kafka lag at 480s, need fresh ODS now",
            }
        }
    }


@app.post("/customer/{customer_id}/force-sync", tags=["System"])
async def force_sync_customer_endpoint(
    customer_id: int,
    body: ForceSyncRequest,
    _: str = Security(require_api_key),
):
    """
    Phase 12 — Force Sync: rebuild a subscriber's ODS document directly from Oracle.

    Use this when the Kafka circuit breaker is SYSTEM_STALE and a specific customer
    is on a live call.  The Kafka pipeline recovery is NOT affected; this is a
    targeted point-lookup override for a single subscriber.

    Requires a mandatory justification for the audit trail.
    Returns the freshly hydrated (PII-masked) subscriber document.
    """
    _require_mongo()
    from db.ods import force_sync_customer
    from observability.metrics import force_sync_total

    doc = await force_sync_customer(
        customer_id=customer_id,
        opco_id=body.opco_id,
        operator_id=body.operator_id,
        justification=body.justification,
    )

    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=f"Customer {customer_id} not found in Oracle",
        )

    force_sync_total.labels(opco_id=body.opco_id).inc()

    # Broadcast to NOC so other operators see the override
    await ws_manager.broadcast(
        body.opco_id,
        noc_event(
            "force_sync",
            body.opco_id,
            customer_id=customer_id,
            operator_id=body.operator_id,
            hydrated_at=doc.get("_force_synced_at"),
        ),
    )

    logger.warning(
        "Force-sync: customer_id=%s opco=%s by=%s reason=%s",
        customer_id, body.opco_id, body.operator_id, body.justification,
    )

    return {
        "status":          "force_synced",
        "customer_id":     customer_id,
        "opco_id":         body.opco_id,
        "hydrated_at":     doc.get("_force_synced_at"),
        "pii_fields_masked": sum(
            1 for v in str(doc).split("[PII:") if ":" in v
        ),
    }


# ── Phase 11: Real-time NOC WebSocket dashboard ────────────────────────────────

@app.websocket("/ws/noc/{opco_id}")
async def noc_websocket(websocket: WebSocket, opco_id: str):
    """
    Real-time NOC event stream — Phase 11.

    Connect with your OpCo ID to receive live events scoped to your boundary.
    Use "ALL" for a super-user view across all OpCos.

    Valid opco_id values: VODAFONE_UK, VODAFONE_DE, VODAFONE_ES, VODAFONE_IT,
                          DEFAULT, ALL

    Event types pushed:
      resolution_proposed  — agent completed analysis, actions awaiting review
      action_approved      — operator approved an action
      force_approved       — operator force-approved an ESCALATED_STALE action
      dlq_acknowledged     — DLQ event acknowledged by NOC
      system_stale         — Kafka consumer lag exceeded threshold
      heartbeat            — keep-alive every 30 s
    """
    if opco_id not in VALID_OPCO_IDS and opco_id != "ALL":
        await websocket.close(code=4008, reason=f"Unknown opco_id: {opco_id}")
        return

    await ws_manager.connect(websocket, opco_id)
    try:
        while True:
            await asyncio.sleep(30)
            await websocket.send_json(
                noc_event(
                    "heartbeat",
                    opco_id,
                    connections=ws_manager.connection_count(opco_id),
                )
            )
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket, opco_id)
    except Exception:
        ws_manager.disconnect(websocket, opco_id)


# ── Dev runner ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
