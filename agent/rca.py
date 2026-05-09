"""
Phase 15 Step 2 — Predictive Event Correlation / Root Cause Analysis engine.

Instead of resolving one customer at a time, the RCA engine periodically scans
the last N events (default 100) and looks for spatial/temporal clusters. When
≥CLUSTER_THRESHOLD% of events share the same postal_code or network_sector, it
escalates to a proactive Network Repair Ticket + Mass Customer SMS rather than
generating 80 individual HITL proposals.

Outputs:
  • RcaResult document persisted to MongoDB rca_results collection
  • Two ActionProposal documents: CREATE_NETWORK_TICKET + MASS_CUSTOMER_SMS
  • Publishes rca_alert to the NOC WebSocket for immediate operator awareness

Scheduling: called by the Thinker before processing each batch of tasks. Also
exposed as GET /system/rca/latest for the NOC dashboard.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import Counter
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Optional

import anthropic
from motor.motor_asyncio import AsyncIOMotorClient

logger = logging.getLogger(__name__)

MONGODB_URI       = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB        = os.getenv("MONGODB_DB", "synapse")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL_RCA", "claude-haiku-4-5-20251001")
RCA_WINDOW        = int(os.getenv("RCA_WINDOW", "100"))
CLUSTER_THRESHOLD = float(os.getenv("RCA_CLUSTER_THRESHOLD", "0.60"))   # 60 %


@dataclass
class RcaCluster:
    dimension: str           # "postal_code" or "network_sector"
    value: str               # e.g. "SE1 7PB" or "HLR-East"
    count: int
    total_events: int
    pct: float
    opco_id: Optional[str] = None
    sample_customer_ids: list[int] = field(default_factory=list)

    @property
    def is_ripe(self) -> bool:
        return self.pct >= CLUSTER_THRESHOLD


@dataclass
class RcaResult:
    run_at: str
    window: int
    cluster: Optional[RcaCluster]
    narrative: str
    actions: list[dict]
    status: str = "PENDING"   # PENDING | ESCALATED | DISMISSED


async def _get_db():
    client = AsyncIOMotorClient(MONGODB_URI)
    return client[MONGODB_DB]


async def _fetch_recent_events(db, opco_id: Optional[str] = None) -> list[dict[str, Any]]:
    query: dict[str, Any] = {}
    if opco_id:
        query["opco_id"] = opco_id
    cursor = (
        db.action_proposals
        .find(query, {"_id": 0, "customer_id": 1, "opco_id": 1,
                      "action_payload": 1, "created_at": 1})
        .sort("created_at", -1)
        .limit(RCA_WINDOW)
    )
    return await cursor.to_list(length=RCA_WINDOW)


def _detect_cluster(events: list[dict]) -> Optional[RcaCluster]:
    postal_counts: Counter = Counter()
    sector_counts: Counter = Counter()
    customer_ids_by_postal: dict[str, list[int]] = {}
    customer_ids_by_sector: dict[str, list[int]] = {}
    opco_ids: list[str] = []

    for ev in events:
        payload = ev.get("action_payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        postal = payload.get("postal_code") or payload.get("postcode") or payload.get("zip")
        sector = payload.get("network_sector") or payload.get("sector")
        cid    = ev.get("customer_id")
        opco   = ev.get("opco_id", "")
        if opco:
            opco_ids.append(opco)

        if postal:
            postal_counts[postal] += 1
            customer_ids_by_postal.setdefault(postal, [])
            if cid:
                customer_ids_by_postal[postal].append(cid)
        if sector:
            sector_counts[sector] += 1
            customer_ids_by_sector.setdefault(sector, [])
            if cid:
                customer_ids_by_sector[sector].append(cid)

    total = len(events)
    dominant_opco = Counter(opco_ids).most_common(1)[0][0] if opco_ids else None

    for dim, counter, cid_map in [
        ("postal_code", postal_counts, customer_ids_by_postal),
        ("network_sector", sector_counts, customer_ids_by_sector),
    ]:
        if not counter:
            continue
        top_val, top_count = counter.most_common(1)[0]
        pct = top_count / total if total else 0
        if pct >= CLUSTER_THRESHOLD:
            return RcaCluster(
                dimension=dim,
                value=top_val,
                count=top_count,
                total_events=total,
                pct=round(pct, 3),
                opco_id=dominant_opco,
                sample_customer_ids=list(set(cid_map.get(top_val, [])))[:20],
            )
    return None


async def _generate_narrative(cluster: RcaCluster) -> str:
    client = anthropic.AsyncAnthropic()
    msg = await client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=300,
        system=(
            "You are a Telco NOC AI. Write a concise (≤120 words) incident narrative "
            "for a network cluster event. Be factual and actionable."
        ),
        messages=[
            {
                "role": "user",
                "content": (
                    f"Cluster detected: {cluster.count} of {cluster.total_events} recent events "
                    f"share {cluster.dimension}={cluster.value} ({cluster.pct*100:.0f}%). "
                    f"OpCo: {cluster.opco_id or 'unknown'}. "
                    f"Affected customer IDs (sample): {cluster.sample_customer_ids[:5]}. "
                    "Draft the incident narrative."
                ),
            }
        ],
    )
    return msg.content[0].text.strip()


def _build_actions(cluster: RcaCluster, narrative: str) -> list[dict]:
    return [
        {
            "action_type":        "create_network_ticket",
            "action_description": (
                f"Open a P1 Network Repair Ticket for {cluster.dimension}={cluster.value} "
                f"affecting {cluster.count} subscribers ({cluster.pct*100:.0f}% of window)."
            ),
            "action_payload": {
                "dimension":         cluster.dimension,
                "cluster_value":     cluster.value,
                "affected_count":    cluster.count,
                "opco_id":           cluster.opco_id,
                "priority":          "P1",
                "narrative":         narrative,
            },
            "predicted_before_state": {"ticket_status": "NONE"},
            "predicted_after_state":  {"ticket_status": "OPEN"},
        },
        {
            "action_type":        "mass_customer_sms",
            "action_description": (
                f"Send a proactive service-disruption SMS to all customers in "
                f"{cluster.dimension}={cluster.value} ({cluster.count} recipients)."
            ),
            "action_payload": {
                "dimension":     cluster.dimension,
                "cluster_value": cluster.value,
                "customer_ids":  cluster.sample_customer_ids,
                "opco_id":       cluster.opco_id,
                "template":      "SERVICE_DISRUPTION_PROACTIVE",
            },
            "predicted_before_state": {"sms_sent": False},
            "predicted_after_state":  {"sms_sent": True},
        },
    ]


async def run_rca(opco_id: Optional[str] = None) -> Optional[RcaResult]:
    """
    Scan recent events for a cluster. If ripe, persist and return RcaResult.
    Returns None when no actionable cluster is found.
    """
    db = await _get_db()
    events = await _fetch_recent_events(db, opco_id)

    if not events:
        logger.info("rca: no recent events found for opco=%s", opco_id)
        return None

    cluster = _detect_cluster(events)
    if not cluster or not cluster.is_ripe:
        logger.info("rca: no dominant cluster detected (window=%d)", len(events))
        return None

    logger.info(
        "rca: cluster %s=%s pct=%.0f%% count=%d — generating narrative",
        cluster.dimension, cluster.value, cluster.pct * 100, cluster.count,
    )

    narrative = await _generate_narrative(cluster)
    actions   = _build_actions(cluster, narrative)

    result = RcaResult(
        run_at=datetime.utcnow().isoformat(),
        window=len(events),
        cluster=cluster,
        narrative=narrative,
        actions=actions,
    )

    doc = {
        **asdict(result),
        "cluster": asdict(cluster),
    }
    inserted = await db.rca_results.insert_one(doc)
    logger.info("rca: RcaResult persisted id=%s", inserted.inserted_id)

    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    opco = sys.argv[1] if len(sys.argv) > 1 else None
    result = asyncio.run(run_rca(opco))
    if result:
        print(json.dumps(asdict(result), indent=2, default=str))
    else:
        print("No ripe cluster found.")
