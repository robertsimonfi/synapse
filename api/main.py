"""
Telco Fallout Resolution Agent — FastAPI REST layer.

Endpoints:
  POST /resolve          — Analyze and resolve a fallout
  GET  /health           — Health check
  GET  /memory/stats     — Memory store statistics
  POST /memory/ingest    — Re-ingest runbooks into ChromaDB
"""

from fastapi import FastAPI, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional
import uvicorn

from agent.agent import (
    FalloutInput, run_agent, resolution_to_dict,
    Customer360Input, run_customer_360,
)
from agent.memory import memory_stats
from api.security import require_api_key
from rag.knowledge_base import ingest_runbooks

app = FastAPI(
    title="Telco Fallout Resolution Agent",
    description="AI-powered BSS/OSS fallout detection, root cause analysis, and resolution.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response schemas ─────────────────────────────────────────────────

class ResolveRequest(BaseModel):
    order_id:      Optional[int]   = Field(None, description="Order ID from ORDER_HEADER")
    order_number:  Optional[str]   = Field(None, description="e.g. ORD-2024001")
    customer_id:   Optional[int]   = Field(None, description="Customer ID from CUSTOMER")
    error_code:    str             = Field("",   description="e.g. PROV_001, BILL_003")
    error_message: str             = Field("",   description="Human-readable error message")
    domain:        str             = Field("",   description="BSS, OSS, BILLING, NETWORK, etc.")
    severity:      str             = Field("MEDIUM", description="CRITICAL, HIGH, MEDIUM, LOW")
    free_text:     Optional[str]   = Field(None, description="Any additional operator context")

    class Config:
        json_schema_extra = {
            "example": {
                "order_id": 42,
                "order_number": "ORD-2024042",
                "customer_id": 7,
                "error_code": "PROV_001",
                "error_message": "HLR activation timeout after 30s",
                "domain": "PROVISIONING",
                "severity": "HIGH",
                "free_text": "Third retry attempt — still failing on HLR-East cluster",
            }
        }


class ClassificationOut(BaseModel):
    domain:     str
    issue_type: str
    severity:   str


class ResolveResponse(BaseModel):
    classification:      ClassificationOut
    root_cause:          str
    recommended_actions: list[str]
    runbook_references:  list[str]
    confidence:          str
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
    customer_id: int            = Field(..., description="Customer ID to build 360 profile for")
    query:       Optional[str]  = Field(None, description="Specific question e.g. 'Why is my bill high?'")

    class Config:
        json_schema_extra = {
            "example": {
                "customer_id": 7,
                "query": "Why did my bill increase last month?",
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


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    return {"status": "ok", "version": "1.0.0"}


@app.get("/memory/stats", response_model=MemoryStatsResponse, tags=["Memory"])
def get_memory_stats(_: str = Security(require_api_key)):
    """Return statistics about the agent's self-critique memory store."""
    return memory_stats()


@app.post("/memory/ingest", tags=["Memory"])
def ingest_knowledge_base(force: bool = False, _: str = Security(require_api_key)):
    """Re-ingest all runbooks into the RAG ChromaDB collection."""
    count = ingest_runbooks(force=force)
    return {"status": "ok", "chunks_ingested": count}


@app.post("/resolve", response_model=ResolveResponse, tags=["Agent"])
def resolve_fallout(request: ResolveRequest, _: str = Security(require_api_key)):
    """
    Submit a Telco fallout for AI-powered root cause analysis and resolution.

    The agent will:
    1. Retrieve relevant runbook context via RAG
    2. Query the DB using tool_use to correlate data across all 15 tables
    3. Identify root cause and recommend actions
    4. Self-critique and store lessons for future improvement
    """
    if not request.order_id and not request.order_number and not request.error_code:
        raise HTTPException(
            status_code=422,
            detail="Provide at least one of: order_id, order_number, or error_code",
        )

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
        resolution = run_agent(fallout_input)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {str(e)}")

    classification = resolution.classification or {}
    critique       = resolution.critique or {}

    return ResolveResponse(
        classification=ClassificationOut(
            domain=classification.get("domain", request.domain or "UNKNOWN"),
            issue_type=classification.get("issue_type", "Unknown"),
            severity=classification.get("severity", request.severity),
        ),
        root_cause=resolution.root_cause,
        recommended_actions=resolution.recommended_actions,
        runbook_references=resolution.runbook_references,
        confidence=resolution.confidence,
        critique_quality=critique.get("quality"),
        critique_lesson=critique.get("lesson"),
        routing_domain=resolution.routing_domain,
        routing_confidence=resolution.routing_confidence,
        critic_violations=resolution.critic_violations or None,
        policy_violations=resolution.policy_violations or None,
    )


@app.post("/customer-360", response_model=Customer360Response, tags=["Agent"])
def customer_360(request: Customer360Request, _: str = Security(require_api_key)):
    """
    Build a comprehensive Customer 360 profile for a given customer ID.

    The agent will:
    1. Gather customer profile, active subscriptions, and billing account details
    2. Retrieve recent invoices and detect billing trends or anomalies
    3. Analyse monthly usage (DATA/VOICE/SMS) and flag roaming activity
    4. Check open incident tickets to assess churn risk
    5. Return structured insights and proactive recommendations
    """
    try:
        summary = run_customer_360(Customer360Input(
            customer_id=request.customer_id,
            query=request.query or "",
        ))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Agent error: {str(e)}")

    return Customer360Response(
        customer_id=summary.customer_id,
        profile_summary=summary.profile_summary,
        active_services=summary.active_services,
        billing_health=summary.billing_health,
        usage_analysis=summary.usage_analysis,
        open_issues=summary.open_issues,
        proactive_recommendations=summary.proactive_recommendations,
    )


# ── Dev runner ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
