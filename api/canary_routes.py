"""
Canary Analysis API routes — Phase 12.

Mounted into the main FastAPI app under the /canary prefix.

Endpoints
---------
  GET  /canary/status               — Head-to-head KPIs: champion vs. candidate
  POST /canary/promote              — Promote candidate to champion
  POST /canary/{result_id}/score    — Backfill operator quality score onto a result

Traffic Splitting
-----------------
  The agent (agent/agent.py) reads CANARY_CANDIDATE_MODEL and
  CANARY_CANDIDATE_TRAFFIC_PCT at resolve time to decide which model handles
  the request.  The chosen model is stamped on the CanaryResult published to
  synapse.canary.results.

  Override the active champion at runtime by calling POST /canary/promote.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Security
from pydantic import BaseModel, Field

from api.security import require_api_key

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/canary", tags=["Canary"])


# ── Schemas ───────────────────────────────────────────────────────────────────

class PromoteRequest(BaseModel):
    operator_id: str = Field(..., description="Operator authorising the promotion")
    justification: str = Field("", description="Optional rationale for audit trail")

    model_config = {
        "json_schema_extra": {
            "example": {
                "operator_id":  "noc-lead@vodafone.com",
                "justification": "Candidate beat champion success_rate by 4pp over 48h",
            }
        }
    }


class ScoreRequest(BaseModel):
    operator_score: int = Field(..., ge=1, le=5, description="1 (poor) to 5 (excellent)")
    approved: bool      = Field(..., description="True if at least one action was approved")

    model_config = {
        "json_schema_extra": {
            "example": {"operator_score": 4, "approved": True}
        }
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/status")
async def get_canary_status_endpoint(_: str = Security(require_api_key)):
    """
    Return live head-to-head comparison between champion and candidate models.

    KPIs computed over the rolling CANARY_WINDOW_MINUTES window:
      - success_rate   : fraction of resolutions where an action was approved
      - high_conf_pct  : fraction of resolutions where confidence == HIGH
      - avg_operator_score : mean 1-5 operator quality score (null until scored)
      - sample_count   : total resolutions in the window

    `ready_to_promote` is true when the candidate meets all thresholds.
    """
    from worker.canary import get_canary_status
    return await get_canary_status()


@router.post("/promote")
async def promote_candidate_endpoint(
    body: PromoteRequest,
    _: str = Security(require_api_key),
):
    """
    Promote the CANARY_CANDIDATE_MODEL to champion after passing quality gates.

    Will return 409 if the candidate does not yet meet:
      - success_rate >= CANARY_MIN_SUCCESS_RATE  (default 0.85)
      - sample_count >= CANARY_MIN_SAMPLES       (default 100)

    In production, connect this to your feature-flag service (LaunchDarkly,
    Flagsmith, etc.) to immediately shift 100% of traffic to the new champion.
    """
    from worker.canary import promote_candidate

    try:
        result = await promote_candidate(body.operator_id)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    logger.info(
        "Canary promoted by %s — justification: %s",
        body.operator_id, body.justification or "(none)",
    )
    return result


@router.post("/{result_id}/score")
async def score_canary_result(
    result_id: str,
    body: ScoreRequest,
    _: str = Security(require_api_key),
):
    """
    Backfill an operator quality score onto a canary result document.

    Call this from the HITL approval flow immediately after an operator
    approves or rejects an action, to close the feedback loop.
    """
    from worker.canary import score_result

    await score_result(result_id, body.operator_score, body.approved)
    return {"status": "scored", "result_id": result_id}
