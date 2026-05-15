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

Phase 13 — Resilience & Cost:
  GET  /billing/usage              — All OpCo token usage for the current billing period
  GET  /billing/usage/{opco_id}    — Single OpCo token usage + quota status
  GET  /system/oracle-ha           — Oracle primary/standby circuit breaker state

Phase 14 — GA Sprint (Sentiment + React Dashboard):
  GET  /system/trace/{trace_id}    — Chronological event timeline for a distributed trace
  POST /actions/{id}/feedback      — Operator correction published to synapse.operator.corrections

Phase 15 — Autonomous Intelligence:
  POST /auth/token                  — Obtain a JWT access token (operator login)
  GET  /auth/refresh                — Refresh a valid JWT
  GET  /system/rca/latest           — Latest predictive Root Cause Analysis result
  POST /system/rca/run              — Trigger an on-demand RCA scan
  GET  /intelligence/prompt-suggestions      — List pending prompt-optimizer suggestions
  POST /intelligence/prompt-suggestions/{id}/apply  — Apply a suggestion to the system prompt
  POST /intelligence/prompt-suggestions/{id}/reject — Reject a suggestion

Observability:
  GET  /metrics                    — Prometheus metrics (scraped by Prometheus)

WebSocket events pushed to /ws/noc/{opco_id}:
  resolution_proposed  — agent completed analysis, actions awaiting review
  action_approved      — operator approved an action
  force_approved       — operator force-approved an ESCALATED_STALE action
  dlq_acknowledged     — DLQ event acknowledged by NOC
  system_stale         — Kafka consumer lag exceeded threshold
  force_sync           — NOC operator triggered a per-customer Kafka bypass
  heartbeat            — keep-alive every 30 s
"""

import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Security, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
import observability.metrics as _metrics  # noqa: F401 -- registers all synapse_* counters in the Prometheus registry at API startup
from pydantic import BaseModel, Field, field_validator

from agent.agent import (
    Customer360Input,
    FalloutInput,
    run_agent,
    run_customer_360,
)
from agent.memory import memory_stats
from api.security import require_api_key, require_any_auth, auth_router, get_current_operator, OperatorInfo
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
        from db.mongo import ensure_indexes, seed_roles_permissions
        from db.ods import ensure_ods_indexes
        from billing.token_budget import ensure_budget_indexes
        await ensure_indexes()
        await ensure_ods_indexes()
        await ensure_budget_indexes()
        await seed_roles_permissions()
        logger.info("MongoDB indexes ready (actions, ODS, token_budgets, roles_permissions)")

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

_OPENAPI_TAGS = [
    {"name": "Agent",      "description": "AI fallout analysis and Customer 360 profiles"},
    {"name": "HITL",       "description": "Human-in-the-loop action review and approval workflow"},
    {"name": "System",     "description": "Health, circuit breaker status, and NOC controls"},
    {"name": "Engagement", "description": "Proactive customer engagement via SMS / Email / WhatsApp"},
    {"name": "DLQ",        "description": "Dead Letter Queue — inspect and acknowledge failed commands"},
    {"name": "Canary",     "description": "A/B model traffic splitting and quality-gate promotion"},
    {"name": "Memory",     "description": "ChromaDB RAG knowledge base management"},
    {
        "name": "Billing",
        "description": (
            "Phase 13 — Per-OpCo AI token quota management.  "
            "Enforces monthly token budgets so no single OpCo can exhaust the shared "
            "Anthropic API allowance.  Quotas are configured via OPCO_TOKEN_QUOTAS env var."
        ),
    },
    {
        "name": "Auth",
        "description": (
            "Phase 15 — JWT operator authentication.  "
            "POST /auth/token to obtain a Bearer token.  All dashboard endpoints accept "
            "Bearer tokens in addition to the legacy X-API-Key header."
        ),
    },
    {
        "name": "Intelligence",
        "description": (
            "Phase 15 — Autonomous intelligence: predictive RCA cluster detection and "
            "AI prompt self-optimization driven by operator correction feedback."
        ),
    },
]

app = FastAPI(
    title="Synapse — Telco Fallout Resolution Agent",
    description=(
        "AI-powered BSS/OSS fallout detection, root cause analysis, HITL action management, "
        "and resolution.  Powered by Claude.\n\n"
        "**Multi-tenant** — each request is scoped to an `opco_id` "
        "(e.g. `UK_REGION`, `DE_REGION`) for data residency and quota isolation.\n\n"
        "**WebSocket NOC dashboard** — connect to `WS /ws/noc/{opco_id}` for live events."
    ),
    version="3.0.0",
    openapi_tags=_OPENAPI_TAGS,
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

# Phase 15: JWT auth routes
app.include_router(auth_router)


# ── Resolve schemas ────────────────────────────────────────────────────────────

class ResolveRequest(BaseModel):
    order_id:              Optional[int]   = Field(None, description="Order ID from ORDER_HEADER")
    order_number:          Optional[str]   = Field(None, description="e.g. ORD-2024001")
    customer_id:           Optional[int]   = Field(None, description="Customer ID from CUSTOMER")
    # String customer ID passed by the Chatbot Gateway for WebSocket session routing.
    # May differ from customer_id (which is coerced to int and can be None for demo IDs).
    chatbot_customer_id:   Optional[str]   = Field(None, description="Chatbot session ID for HITL callback")
    error_code:            str             = Field("",   description="e.g. PROV_001, BILL_003")
    error_message:         str             = Field("",   description="Human-readable error message")
    domain:                str             = Field("",   description="BSS, OSS, BILLING, NETWORK, etc.")
    severity:              str             = Field("MEDIUM", description="CRITICAL, HIGH, MEDIUM, LOW")
    free_text:             Optional[str]   = Field(None, description="Any additional operator context")
    opco_id:               str             = Field("DEFAULT", description="OpCo for multi-tenancy isolation")

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
                "opco_id": "UK_REGION",
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
                "opco_id": "UK_REGION",
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
    # Defensive Execution / Force Approve fields
    stale_reason:             Optional[str]
    predicted_state_at_check: Optional[dict[str, Any]]
    current_state_at_check:   Optional[dict[str, Any]]
    handler_attempt_count:    Optional[int]
    compensation_type:        Optional[str]
    compensation_result:      Optional[dict[str, Any]]
    approval_method:          Optional[str]
    force_approval_reason:    Optional[str]
    safety_checks_bypassed:   bool
    chain_of_thought:         Optional[dict[str, str]] = None
    opco_id:                  str = "DEFAULT"


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
        stale_reason=getattr(a, "stale_reason", None),
        predicted_state_at_check=getattr(a, "predicted_state_at_check", None),
        current_state_at_check=getattr(a, "current_state_at_check", None),
        handler_attempt_count=getattr(a, "handler_attempt_count", None),
        compensation_type=getattr(a, "compensation_type", None),
        compensation_result=getattr(a, "compensation_result", None),
        approval_method=getattr(a, "approval_method", None),
        force_approval_reason=getattr(a, "force_approval_reason", None),
        safety_checks_bypassed=getattr(a, "safety_checks_bypassed", False),
        chain_of_thought=getattr(a, "chain_of_thought", None),
        opco_id=getattr(a, "opco_id", "DEFAULT"),
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

        _PROV_TYPES = {"REPROVISION", "reprovision", "retry_provisioning"}
        for raw in resolution.action_proposals:
            # Inject chatbot_customer_id into payload so approve_action can callback.
            merged_payload = dict(raw.get("action_payload", {}))
            if request.chatbot_customer_id:
                merged_payload["chatbot_customer_id"] = request.chatbot_customer_id

            # Enrich MSISDN from SQLite for provisioning-type actions so that
            # the outbox PROV_RESYNC command carries a real phone number.
            if raw.get("action_type") in _PROV_TYPES:
                msisdn = merged_payload.get("msisdn")
                if (not msisdn or msisdn == "unknown") and request.customer_id:
                    try:
                        from worker.drift import get_primary_msisdn
                        resolved = await asyncio.to_thread(
                            get_primary_msisdn, request.customer_id
                        )
                        if resolved:
                            merged_payload["msisdn"] = resolved
                    except Exception:
                        pass

            proposal = ActionProposal(
                correlation_id=correlation_id,
                order_id=str(request.order_id) if request.order_id else None,
                customer_id=str(request.customer_id) if request.customer_id else None,
                action_type=raw.get("action_type", "unknown"),
                action_description=raw.get("action_description", ""),
                action_payload=merged_payload,
                predicted_before_state=raw.get("predicted_before_state", {}),
                predicted_after_state=raw.get("predicted_after_state", {}),
                chain_of_thought=resolution.chain_of_thought,
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


# ── Customer lookup (Phase 6 Identity Bridge) ─────────────────────────────────

# Lightweight mock profile store — keyed by the chatbot's string customer IDs.
# In production this would query the ODS / CRM.
_CUSTOMER_PROFILES: dict[str, dict] = {
    "demo-customer-1":  {"name": "Jane Customer",  "segment": "General", "plan": "Flex 50",     "opco_id": "UK_REGION"},
    "vvip-customer-1":  {"name": "Robert VIP",     "segment": "VVIP",    "plan": "Premium Plus", "opco_id": "UK_REGION"},
    "es-customer-1":    {"name": "Carlos García",  "segment": "General", "plan": "Flex 30",     "opco_id": "ES_REGION"},
    "mgr-001":          {"name": "Alex Manager",   "segment": "Staff",   "plan": None,           "opco_id": "UK_REGION"},
    "ops-de-001":       {"name": "Dieter Operator","segment": "Staff",   "plan": None,           "opco_id": "DE_REGION"},
}


@app.get("/customers/{customer_id}", tags=["Customers"])
async def get_customer(
    customer_id: str,
    operator: Optional[OperatorInfo] = Depends(require_any_auth),
):
    """
    Phase 6 — Return a customer's profile for the chatbot greeting and proxy dashboard.

    Accepts X-API-Key (machine-to-machine) or a Bearer JWT (chatbot gateway).
    Tries the MongoDB ODS first; falls back to the in-process mock profile store.
    """
    # Try MongoDB ODS if available
    if _mongo_enabled:
        try:
            from db.ods import async_get_subscriber
            cid_int = int(customer_id) if customer_id.isdigit() else None
            ods = await async_get_subscriber(cid_int) if cid_int else None
            if ods:
                profile = ods.get("profile") or {}
                first   = profile.get("first_name") or ""
                last    = profile.get("last_name")  or ""
                return {
                    "customer_id": customer_id,
                    "name":        f"{first} {last}".strip() or f"Customer {customer_id}",
                    "segment":     profile.get("segment") or ods.get("segment", "General"),
                    "plan":        (ods.get("subscriptions") or [{}])[0].get("product_name"),
                    "opco_id":     ods.get("opco_id", "DEFAULT"),
                }
        except Exception:
            pass  # fall through to mock

    profile = _CUSTOMER_PROFILES.get(customer_id)
    if profile:
        return {"customer_id": customer_id, **profile}

    # Unknown customer — return minimal record so the chatbot can still proceed.
    return {"customer_id": customer_id, "name": None, "segment": "General", "plan": None, "opco_id": "DEFAULT"}


@app.get("/customer/{customer_id}/context", tags=["Customers"])
async def get_customer_context(
    customer_id: str,
    operator: Optional[OperatorInfo] = Depends(require_any_auth),
):
    """
    Phase 9 — Return a merged customer context for AI agent resolution.

    Cross-checks the MongoDB ODS (hydrated from Oracle CDC) against the SQLite
    baseline (telco_dev.db) so the agent always has the freshest available view.

    Priority:
      1. MongoDB ODS — full golden-record JSON (subscriptions, billing, fallouts)
      2. SQLite baseline — CUSTOMER + SERVICE_SUBSCRIPTION tables (VVIP flag, status)
      3. In-process mock profiles — for chatbot demo IDs (e.g. "vvip-customer-1")
    """
    import sqlite3 as _sqlite

    context: dict = {"customer_id": customer_id, "sources": []}

    # 1 — MongoDB ODS
    if _mongo_enabled:
        try:
            from db.ods import async_get_subscriber
            cid_int = int(customer_id) if customer_id.isdigit() else None
            ods = await async_get_subscriber(cid_int) if cid_int else None
            if ods:
                context["ods"] = {
                    "profile":          ods.get("profile"),
                    "subscriptions":    ods.get("subscriptions"),
                    "billing":          ods.get("billing"),
                    "open_fallouts":    ods.get("open_fallouts"),
                    "recent_orders":    ods.get("recent_orders"),
                    "open_tickets":     ods.get("open_tickets"),
                    "hydrated_at":      ods.get("hydrated_at"),
                    "opco_id":          ods.get("opco_id", "DEFAULT"),
                }
                context["sources"].append("mongodb_ods")
        except Exception as exc:
            logger.debug("ODS context lookup failed for %s: %s", customer_id, exc)

    # 2 — SQLite baseline (VVIP flag, live account status)
    sqlite_path = os.getenv("SQLITE_PATH", "telco_dev.db")
    if os.path.exists(sqlite_path):
        try:
            cid_int = int(customer_id) if customer_id.isdigit() else None
            if cid_int:
                def _sqlite_lookup(cid: int) -> dict:
                    conn = _sqlite.connect(sqlite_path)
                    conn.row_factory = _sqlite.Row
                    try:
                        cust = conn.execute(
                            "SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, STATUS, SEGMENT, "
                            "       ACCOUNT_TYPE, CREDIT_SCORE "
                            "FROM CUSTOMER WHERE CUSTOMER_ID = ?", (cid,)
                        ).fetchone()
                        if not cust:
                            return {}
                        subs = conn.execute(
                            "SELECT SUBSCRIPTION_ID, STATUS, MSISDN "
                            "FROM SERVICE_SUBSCRIPTION WHERE CUSTOMER_ID = ? "
                            "ORDER BY CREATED_DATE DESC LIMIT 3", (cid,)
                        ).fetchall()
                        billing = conn.execute(
                            "SELECT STATUS, CURRENT_BALANCE, CREDIT_LIMIT "
                            "FROM BILLING_ACCOUNT WHERE CUSTOMER_ID = ?", (cid,)
                        ).fetchone()
                        return {
                            "customer":    dict(cust),
                            "subscriptions": [dict(s) for s in subs],
                            "billing":     dict(billing) if billing else {},
                        }
                    finally:
                        conn.close()

                baseline = await asyncio.to_thread(_sqlite_lookup, cid_int)
                if baseline:
                    context["baseline"] = baseline
                    context["sources"].append("sqlite_baseline")
        except Exception as exc:
            logger.debug("SQLite context lookup failed for %s: %s", customer_id, exc)

    # 3 — Mock profile fallback for chatbot demo IDs
    mock = _CUSTOMER_PROFILES.get(customer_id)
    if mock:
        context.setdefault("mock_profile", mock)
        context["sources"].append("mock_profile")

    if not context["sources"]:
        raise HTTPException(
            status_code=404,
            detail=f"No context found for customer_id={customer_id}",
        )

    return context


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
    operator: Optional[OperatorInfo] = Depends(require_any_auth),
):
    """
    Operator approves a PENDING action.
    Phase 17: RBAC — OPERATOR role may only approve actions in their assigned region.

    approval_method="STANDARD" (default): transitions to APPROVED; executor runs the
    pre-flight drift check before execution.

    approval_method="FORCE_APPROVED": transitions directly to FORCE_APPROVED; executor
    skips the pre-flight drift check. force_approval_reason is required in this case.
    """
    _require_mongo()
    from db.mongo import get_action, transition_action
    from models.action import ActionStatus
    from observability.metrics import actions_approved_total

    force_mode = body.approval_method == "FORCE_APPROVED"

    if force_mode and not body.force_approval_reason:
        raise HTTPException(
            status_code=422,
            detail="force_approval_reason is required when approval_method is FORCE_APPROVED",
        )

    action = await get_action(action_id)
    if not action:
        raise HTTPException(status_code=404, detail=f"Action {action_id} not found")

    # Phase 17 RBAC: operators are restricted to their assigned region
    if operator and operator.role == "operator" and operator.opco_id:
        action_opco = getattr(action, "opco_id", "DEFAULT")
        if action_opco and action_opco not in ("DEFAULT", operator.opco_id):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Permission denied: action belongs to region '{action_opco}' "
                    f"but your token is restricted to '{operator.opco_id}'"
                ),
            )

    # Correction intercept: if the operator previously submitted a correct_action
    # override for this action, mutate action_type in action_states before the
    # Executor picks it up — ensures the pipeline runs the human's intended action.
    from db.mongo import get_db as _get_approve_db
    _adb = await _get_approve_db()
    _correction = await _adb.operator_corrections.find_one(
        {"action_id": action_id, "correct_action": {"$ne": None, "$exists": True}},
        sort=[("submitted_at", -1)],
    )
    if _correction and _correction.get("correct_action"):
        _new_type = _correction["correct_action"]
        await _adb.action_states.update_one(
            {"_id": action_id},
            {"$set": {"action_type": _new_type}},
        )
        # Reload so transition_action & downstream use the corrected type
        action = await get_action(action_id) or action
        logger.info(
            "Correction intercept: action=%s type overridden → %s by %s",
            action_id, _new_type, body.operator_id,
        )

    target_status = ActionStatus.FORCE_APPROVED if force_mode else ActionStatus.APPROVED
    extra: dict = {
        "approved_by": body.operator_id,
        "approval_method": body.approval_method,
    }
    if force_mode:
        extra["force_approval_reason"] = body.force_approval_reason
        extra["safety_checks_bypassed"] = True

    try:
        updated = await transition_action(
            action_id, action.version, target_status,
            actor=body.operator_id,
            extra_updates=extra,
        )
    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=409, detail=str(e))

    if updated is None:
        raise HTTPException(
            status_code=409,
            detail="Concurrent update detected — please refresh and try again.",
        )

    actions_approved_total.labels(action_type=action.action_type).inc()
    if force_mode:
        logger.warning(
            "Action %s FORCE_APPROVED by %s — reason: %s (correlation=%s)",
            action_id, body.operator_id, body.force_approval_reason, action.correlation_id,
        )
    else:
        logger.info(
            "Action %s APPROVED by %s (correlation=%s)",
            action_id, body.operator_id, action.correlation_id,
        )
    # Phase 11: broadcast to all NOC subscribers
    await ws_manager.broadcast(
        "ALL",
        noc_event(
            "force_approved" if force_mode else "action_approved",
            "ALL",
            action_id=action_id,
            action_type=action.action_type,
            operator_id=body.operator_id,
            correlation_id=action.correlation_id,
        ),
    )

    # Phase 5.1: notify chatbot gateway so the customer UI resolves from HITL wait.
    # Guard: only attempt if CHATBOT_GATEWAY_ENABLED=true (avoids [Errno -2] DNS
    # errors in local dev where synapse-chatbot-gateway is not running).
    chatbot_cid     = (action.action_payload or {}).get("chatbot_customer_id")
    chatbot_enabled = os.getenv("CHATBOT_GATEWAY_ENABLED", "false").lower() == "true"
    if chatbot_cid and chatbot_enabled:
        chatbot_url  = os.getenv("CHATBOT_GATEWAY_URL", "http://synapse-chatbot-gateway:8001")
        action_label = action.action_type.replace("_", " ").title()
        try:
            import httpx
            async with httpx.AsyncClient(timeout=3.0) as _client:
                await _client.post(
                    f"{chatbot_url}/internal/action-update",
                    json={
                        "customer_id": chatbot_cid,
                        "action_id":   action_id,
                        "status":      "APPROVED",
                        "summary":     (
                            f"Your {action_label} request has been reviewed and approved "
                            "by our specialist team."
                        ),
                    },
                    headers={"X-Internal-Service-Key": os.getenv("INTERNAL_SERVICE_KEY", "")},
                )
            logger.info("Chatbot notified for session %s action=%s", chatbot_cid, action_id)
        except Exception as _exc:
            logger.warning("Chatbot notification failed for session %s: %s", chatbot_cid, _exc)
    elif chatbot_cid and not chatbot_enabled:
        logger.info(
            "[MOCK-CHATBOT] action=%s session=%s status=APPROVED "
            "(set CHATBOT_GATEWAY_ENABLED=true to enable live callbacks)",
            action_id, chatbot_cid,
        )

    return _action_to_out(updated)


@app.post("/actions/{action_id}/force-approve", response_model=ActionOut, tags=["HITL"])
async def force_approve_action(
    action_id: str,
    body: ForceApproveRequest,
    operator: Optional[OperatorInfo] = Depends(require_any_auth),
):
    """
    Phase 10 — Operator manual override for ESCALATED_STALE actions.
    Phase 17: RBAC — OPERATOR role may only force-approve actions in their assigned region.

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

    # Phase 17 RBAC: operators restricted to their assigned region
    if operator and operator.role == "operator" and operator.opco_id:
        action_opco = getattr(action, "opco_id", "DEFAULT")
        if action_opco and action_opco not in ("DEFAULT", operator.opco_id):
            raise HTTPException(
                status_code=403,
                detail=(
                    f"Permission denied: action belongs to region '{action_opco}' "
                    f"but your token is restricted to '{operator.opco_id}'"
                ),
            )

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
    operator: Optional[OperatorInfo] = Depends(require_any_auth),
):
    """
    Phase 14 — Distributed Trace Timeline.
    Phase 17 — Dual auth (Bearer JWT or X-API-Key) + UUID/ObjectId-safe query.

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

    # Build a list of all ID variants to query (string, UUID canonical form,
    # and ObjectId) so that any storage format resolves correctly.
    id_variants: list = [trace_id]
    try:
        import uuid as _uuid
        canonical = str(_uuid.UUID(trace_id))
        if canonical != trace_id:
            id_variants.append(canonical)
    except (ValueError, AttributeError):
        pass
    try:
        from bson import ObjectId
        id_variants.append(ObjectId(trace_id))
    except Exception:
        pass

    q = {"$or": [
        *[{"trace_id": v} for v in id_variants],
        *[{"correlation_id": v} for v in id_variants],
    ]}

    events: list[dict[str, Any]] = []
    _action_ids: list[str] = []  # collected to join with audit trail

    def _safe_str(v: Any) -> str:
        return str(v) if v is not None else ""

    # ── Action proposals (collection: action_states) ──────────────────────
    try:
        async for doc in db.action_states.find(q):
            action_id_val  = str(doc.get("_id") or uuid.uuid4())
            _action_ids.append(action_id_val)
            ts             = _safe_str(doc.get("created_at", ""))
            correlation_id = _safe_str(doc.get("correlation_id", ""))
            payload        = doc.get("action_payload") or {}
            events.append({
                "event_id":   action_id_val,
                "trace_id":   trace_id,
                "node_type":  "AI_REASONING",
                "event_type": "ACTION_CREATED",
                "timestamp":  ts,
                "label":      f"AI proposed: {doc.get('action_type') or '?'}",
                "detail": {
                    "action_id":      action_id_val,
                    "action_type":    doc.get("action_type"),
                    "description":    doc.get("action_description"),
                    "confidence":     payload.get("confidence") if isinstance(payload, dict) else None,
                    "correlation_id": correlation_id,
                    "chain_of_thought": doc.get("chain_of_thought"),
                },
            })
            for status_val, ts_field, evt, node in [
                ("APPROVED",       "updated_at",  "ACTION_APPROVED",      "OPERATOR_HITL"),
                ("REJECTED",       "updated_at",  "ACTION_REJECTED",      "OPERATOR_HITL"),
                ("EXECUTED",       "executed_at", "ORACLE_WRITE_COMPLETED","ORACLE_WRITE"),
                ("FAILED",         "updated_at",  "ACTION_FAILED",        "DLQ_FAILURE"),
                ("ESCALATED_STALE","updated_at",  "ACTION_ESCALATED",     "OPERATOR_HITL"),
            ]:
                if doc.get("status") == status_val and doc.get(ts_field):
                    events.append({
                        "event_id":   str(uuid.uuid4()),
                        "trace_id":   trace_id,
                        "node_type":  node,
                        "event_type": evt,
                        "timestamp":  _safe_str(doc.get(ts_field, "")),
                        "label":      f"{evt.replace('_', ' ').title()}: {doc.get('action_type') or '?'}",
                        "detail": {
                            "action_id":        action_id_val,
                            "operator":         doc.get("approved_by"),
                            "rejection_reason": doc.get("rejection_reason"),
                            "execution_error":  doc.get("execution_error"),
                        },
                    })
    except Exception as exc:
        logger.warning("trace: action_states query failed for trace_id=%s — %s", trace_id, exc)

    # ── DLQ events ────────────────────────────────────────────────────────
    try:
        async for doc in db.dlq_events.find(q, {"_id": 0}):
            events.append({
                "event_id":   str(doc.get("dlq_id") or uuid.uuid4()),
                "trace_id":   trace_id,
                "node_type":  "DLQ_FAILURE",
                "event_type": "DLQ_FAILURE",
                "timestamp":  _safe_str(doc.get("failed_at", "")),
                "label":      f"DLQ: {doc.get('action_type') or '?'} — {doc.get('table_name') or '?'}",
                "detail": {
                    "outbox_id":     doc.get("outbox_id"),
                    "error_context": doc.get("error_context"),
                    "attempts":      doc.get("attempts"),
                    "table_name":    doc.get("table_name"),
                },
            })
    except Exception as exc:
        logger.warning("trace: dlq_events query failed for trace_id=%s — %s", trace_id, exc)

    # ── Engagement / customer interaction events ───────────────────────────
    try:
        async for doc in db.engagement_log.find(q, {"_id": 0}):
            events.append({
                "event_id":   str(doc.get("id") or uuid.uuid4()),
                "trace_id":   trace_id,
                "node_type":  "ENGAGEMENT",
                "event_type": "ENGAGEMENT_SENT",
                "timestamp":  _safe_str(doc.get("sent_at") or doc.get("created_at", "")),
                "label":      f"Customer notified via {doc.get('channel') or '?'}",
                "detail": {
                    "channel":           doc.get("channel"),
                    "template":          doc.get("template_used"),
                    "status":            doc.get("status"),
                    "customer_response": doc.get("customer_response"),
                    "sentiment_score":   doc.get("sentiment_score"),
                    "sentiment_label":   doc.get("sentiment_label"),
                },
            })
            if doc.get("response_received_at"):
                events.append({
                    "event_id":   str(uuid.uuid4()),
                    "trace_id":   trace_id,
                    "node_type":  "ENGAGEMENT",
                    "event_type": "CUSTOMER_REPLIED",
                    "timestamp":  _safe_str(doc.get("response_received_at", "")),
                    "label":      f"Customer replied ({doc.get('sentiment_label') or 'UNKNOWN'})",
                    "detail": {
                        "response":        doc.get("customer_response"),
                        "sentiment_score": doc.get("sentiment_score"),
                        "sentiment_label": doc.get("sentiment_label"),
                    },
                })
    except Exception as exc:
        logger.warning("trace: engagement_log query failed for trace_id=%s — %s", trace_id, exc)

    # ── Audit trail entries ───────────────────────────────────────────────
    try:
        _audit_q: dict[str, Any] = (
            {"$or": [
                {"action_id": {"$in": _action_ids}},
                {"metadata.correlation_id": trace_id},
            ]}
            if _action_ids
            else {"metadata.correlation_id": trace_id}
        )
        async for doc in db.action_audit_trail.find(_audit_q, {"_id": 0}):
            new_status = doc.get("new_status")
            if new_status in ("APPROVED", "REJECTED", "FORCE_APPROVED"):
                events.append({
                    "event_id":   str(doc.get("id") or uuid.uuid4()),
                    "trace_id":   trace_id,
                    "node_type":  "OPERATOR_HITL",
                    "event_type": f"AUDIT_{new_status}",
                    "timestamp":  _safe_str(doc.get("timestamp", "")),
                    "label":      f"Operator: {new_status or '?'} by {doc.get('actor') or '?'}",
                    "detail":     doc.get("metadata") or {},
                })
            elif new_status == "OPERATOR_FEEDBACK":
                meta = doc.get("metadata") or {}
                events.append({
                    "event_id":   str(doc.get("id") or uuid.uuid4()),
                    "trace_id":   trace_id,
                    "node_type":  "OPERATOR_FEEDBACK",
                    "event_type": "OPERATOR_FEEDBACK_SUBMITTED",
                    "timestamp":  _safe_str(doc.get("timestamp", "")),
                    "label":      f"Feedback: score={meta.get('quality_score')} by {doc.get('actor') or '?'}",
                    "detail":     meta,
                })
    except Exception as exc:
        logger.warning("trace: audit_events query failed for trace_id=%s — %s", trace_id, exc)

    # ── Operator corrections — direct query for OPERATOR_FEEDBACK nodes ───────
    try:
        _feedback_q: dict[str, Any] = (
            {"action_id": {"$in": _action_ids}}
            if _action_ids
            else {"correlation_id": trace_id}
        )
        _seen_corrections: set[str] = set()
        async for doc in db.operator_corrections.find(_feedback_q, {"_id": 0}):
            cid = str(doc.get("correction_id") or "")
            if cid in _seen_corrections:
                continue
            _seen_corrections.add(cid)
            events.append({
                "event_id":   cid or str(uuid.uuid4()),
                "trace_id":   trace_id,
                "node_type":  "OPERATOR_FEEDBACK",
                "event_type": "OPERATOR_FEEDBACK_SUBMITTED",
                "timestamp":  _safe_str(doc.get("submitted_at", "")),
                "label":      f"Feedback: score={doc.get('quality_score')} by {doc.get('operator_id') or '?'}",
                "detail": {
                    "quality_score":  doc.get("quality_score"),
                    "correct_action": doc.get("correct_action"),
                    "feedback_notes": doc.get("feedback_notes"),
                    "training_ready": doc.get("training_ready"),
                    "correction_id":  cid,
                },
            })
    except Exception as exc:
        logger.warning("trace: operator_corrections query failed for trace_id=%s — %s", trace_id, exc)

    # ── Outbox FAILED entries — catch rowcount=0 / silent write failures ─────
    try:
        outbox_q: dict[str, Any] = {
            "$or": [
                *[{"correlation_id": v} for v in id_variants],
                *[{"action_id": aid} for aid in _action_ids],
            ],
            "status": "FAILED",
        }
        async for doc in db.outbox.find(outbox_q, {"_id": 0}):
            events.append({
                "event_id":   str(doc.get("outbox_id") or uuid.uuid4()),
                "trace_id":   trace_id,
                "node_type":  "DLQ_FAILURE",
                "event_type": "OUTBOX_WRITE_FAILED",
                "timestamp":  _safe_str(doc.get("failed_at") or doc.get("updated_at") or doc.get("created_at", "")),
                "label":      f"Write-back failed: {doc.get('action_type') or '?'} → {doc.get('table_name') or 'DB'}",
                "detail": {
                    "outbox_id":    doc.get("outbox_id"),
                    "action_type":  doc.get("action_type"),
                    "table_name":   doc.get("table_name"),
                    "error":        doc.get("error"),
                    "attempt_count": doc.get("attempt_count"),
                    "rows_affected": doc.get("rows_affected"),
                },
            })
    except Exception as exc:
        logger.warning("trace: outbox FAILED query failed for trace_id=%s — %s", trace_id, exc)

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
    action_doc = await db.action_states.find_one({"_id": action_id})
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

    # Persist to MongoDB for offline analytics.
    # TRAINING_READY=True flags corrections for prompt-optimizer ingestion:
    # triggered when the AI made a clear mistake (score ≤ 2) or the operator
    # provided an explicit correct_action override.
    training_ready = body.quality_score <= 2 or bool(body.correct_action)
    correction_doc = correction.model_dump()
    correction_doc["training_ready"] = training_ready
    await db.operator_corrections.insert_one(correction_doc)

    # Write OPERATOR_FEEDBACK audit event so it surfaces in the Trace timeline.
    import datetime as _dt
    audit_feedback_doc = {
        "id":              str(uuid.uuid4()),
        "action_id":       action_id,
        "previous_status": action_doc.get("status"),
        "new_status":      "OPERATOR_FEEDBACK",
        "actor":           body.operator_id,
        "timestamp":       _dt.datetime.utcnow().isoformat(),
        "metadata": {
            "quality_score":  body.quality_score,
            "correct_action": body.correct_action,
            "feedback_notes": body.feedback_notes,
            "correlation_id": action_doc.get("correlation_id", ""),
            "training_ready": training_ready,
            "correction_id":  correction.correction_id,
        },
    }
    await db.action_audit_trail.insert_one(audit_feedback_doc)

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
        "status":         "feedback_recorded",
        "correction_id":  correction.correction_id,
        "action_id":      action_id,
        "quality_score":  body.quality_score,
        "training_ready": training_ready,
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
                "opco_id":       "UK_REGION",
                "operator_id":   "noc-lead@synapse.io",
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

    Valid opco_id values: UK_REGION, DE_REGION, ES_REGION, IT_REGION,
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


# ── Phase 13: Billing — Per-OpCo AI Token Budget ──────────────────────────────


@app.get("/billing/usage", tags=["Billing"])
async def list_token_usage(_: str = Security(require_api_key)):
    """
    Phase 13 — List AI token usage for **all** OpCos in the current billing period.

    Returns one document per OpCo showing quota, consumed tokens, remaining tokens,
    and utilisation percentage.  Use this to monitor budget burn rate and catch
    runaway OpCos before they exhaust the shared Anthropic API allowance.
    """
    _require_mongo()
    from billing.token_budget import get_usage_report
    return await get_usage_report()


@app.get("/billing/usage/{opco_id}", tags=["Billing"])
async def get_opco_token_usage(opco_id: str, _: str = Security(require_api_key)):
    """
    Phase 13 — AI token usage and quota status for a **single** OpCo.

    Returns current period consumed/remaining tokens and whether the OpCo is
    within quota.  Useful for per-tenant dashboards and automated alerts.
    """
    _require_mongo()
    from billing.token_budget import check_quota, get_usage_report

    quota  = await check_quota(opco_id)
    report = await get_usage_report(opco_id)

    return {
        "opco_id":       quota.opco_id,
        "quota_tokens":  quota.quota,
        "consumed":      quota.consumed,
        "remaining":     quota.remaining,
        "utilisation_pct": round(quota.consumed / quota.quota * 100, 1) if quota.quota else 0,
        "allowed":       quota.allowed,
        "period":        quota.period,
        "detail":        report[0] if report else {},
    }


# ── Phase 13: Oracle HA Circuit Breaker status ─────────────────────────────────


@app.get("/system/oracle-ha", tags=["System"])
def oracle_ha_status(_: str = Security(require_api_key)):
    """
    Phase 13 — Oracle High-Availability circuit breaker state.

    Reports whether the primary Oracle is CLOSED (healthy), OPEN (failed over
    to standby), or HALF_OPEN (probing recovery).  When OPEN, writes are queued
    to Kafka and reads are served from the standby replica.
    """
    from db.oracle_ha import OracleHAPool
    pool = OracleHAPool.instance()
    return {
        "state":           pool.state.value,
        "failure_count":   pool.failure_count,
        "standby_configured": bool(os.getenv("ORA_STANDBY_HOST")),
        "failure_threshold": int(os.getenv("ORACLE_HA_FAILURE_THRESHOLD", "3")),
        "half_open_probe_seconds": int(os.getenv("ORACLE_HA_HALF_OPEN_PROBE_SECONDS", "60")),
    }


# ── Phase 15: Predictive RCA ───────────────────────────────────────────────────


@app.get("/system/rca/latest", tags=["Intelligence"])
async def get_latest_rca(
    opco_id: Optional[str] = None,
    _: str = Security(require_api_key),
):
    """
    Phase 15 — Return the most recent RCA result for the given OpCo (or all OpCos).

    If 80%+ of the last 100 events share the same postal code or network sector,
    the RCA engine will have generated a network ticket + mass SMS proposal here.
    """
    _require_mongo()
    from db.mongo import get_db
    db = await get_db()
    query: dict[str, Any] = {}
    if opco_id:
        query["cluster.opco_id"] = opco_id
    doc = await db.rca_results.find_one(query, {"_id": 0}, sort=[("run_at", -1)])
    if not doc:
        return {"status": "no_rca_results", "opco_id": opco_id}
    return doc


@app.post("/system/rca/run", tags=["Intelligence"])
async def trigger_rca(
    opco_id: Optional[str] = None,
    operator: OperatorInfo = Depends(get_current_operator),
):
    """
    Phase 15 — On-demand RCA scan (requires JWT Bearer auth).

    Scans the last 100 action proposals for spatial/temporal clusters. If a ripe
    cluster is detected (≥60% same postal code or network sector) it creates a
    network repair ticket + mass SMS proposal and returns the RCA result.
    Requires JWT Bearer token — the dashboard calls this from the NOC UI.
    """
    _require_mongo()
    from agent.rca import run_rca
    result = await run_rca(opco_id)
    if result is None:
        return {"status": "no_cluster_detected", "opco_id": opco_id}

    from dataclasses import asdict
    return {"status": "rca_complete", "result": asdict(result)}


# ── Phase 15: Prompt-Optimizer suggestions ─────────────────────────────────────


@app.get("/intelligence/prompt-suggestions", tags=["Intelligence"])
async def list_prompt_suggestions(
    status_filter: str = "PENDING",
    _: str = Security(require_api_key),
):
    """
    Phase 15 — List prompt-optimizer suggestions derived from operator corrections.

    Each suggestion targets a specific action_type where the AI was systematically
    wrong. Status values: PENDING | APPLIED | REJECTED.
    """
    _require_mongo()
    from db.mongo import get_db
    db = await get_db()
    cursor = db.prompt_suggestions.find(
        {"status": status_filter},
        {"_id": 1, "action_type": 1, "sample_count": 1, "avg_score": 1,
         "addendum": 1, "status": 1, "created_at": 1, "applied_at": 1, "applied_by": 1},
    ).sort("created_at", -1).limit(50)
    docs = await cursor.to_list(length=50)
    for d in docs:
        d["id"] = str(d.pop("_id"))
    return {"suggestions": docs, "count": len(docs)}


class SuggestionActionRequest(BaseModel):
    operator_id: str = Field(..., description="Operator approving or rejecting the suggestion")


@app.post("/intelligence/prompt-suggestions/{suggestion_id}/apply", tags=["Intelligence"])
async def apply_prompt_suggestion(
    suggestion_id: str,
    body: SuggestionActionRequest,
    operator: OperatorInfo = Depends(get_current_operator),
):
    """
    Phase 15 — Apply a PENDING prompt suggestion to the live system prompt.

    The addendum is appended to agent/prompt_addenda.json which the agent reads
    on next startup. Status moves PENDING → APPLIED. Requires JWT Bearer auth.
    """
    _require_mongo()
    from worker.prompt_optimizer import apply_suggestion
    success = await apply_suggestion(suggestion_id, body.operator_id)
    if not success:
        raise HTTPException(status_code=404, detail="Suggestion not found or not in PENDING state")

    await noc_event("ALL", {
        "event":        "prompt_suggestion_applied",
        "suggestion_id": suggestion_id,
        "operator_id":  body.operator_id,
        "ts":           datetime.utcnow().isoformat(),
    })
    return {"status": "applied", "suggestion_id": suggestion_id}


@app.post("/intelligence/prompt-suggestions/{suggestion_id}/reject", tags=["Intelligence"])
async def reject_prompt_suggestion(
    suggestion_id: str,
    body: SuggestionActionRequest,
    operator: OperatorInfo = Depends(get_current_operator),
):
    """
    Phase 15 — Reject a PENDING prompt suggestion (it will not affect the system prompt).
    Requires JWT Bearer auth.
    """
    _require_mongo()
    from db.mongo import get_db
    from bson import ObjectId
    db = await get_db()
    result = await db.prompt_suggestions.update_one(
        {"_id": ObjectId(suggestion_id), "status": "PENDING"},
        {"$set": {"status": "REJECTED", "applied_by": body.operator_id,
                  "applied_at": datetime.utcnow().isoformat()}},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Suggestion not found or not in PENDING state")
    return {"status": "rejected", "suggestion_id": suggestion_id}


# ── Dev runner ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
