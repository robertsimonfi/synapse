"""
Thinker service — Phase 9 Kafka-based Agent Consumer.

Consumes OrchestrationTask messages from the `ai.orchestration.tasks` Kafka
topic, runs the Claude AI agent, then persists structured ActionProposals to
MongoDB for human review in the Operator Portal.

Replaces the Phase 3–8 RabbitMQ (aio-pika) implementation.

Run as a standalone Docker service:
  python -m worker.thinker
"""

import asyncio
import json
import logging
import os
import uuid

from agent.agent import FalloutInput, run_agent
from db.mongo import create_action, ensure_indexes
from kafka.consumer import create_consumer
from kafka.topics import GROUP_THINKER, TOPIC_AI_ORCHESTRATION
from models.action import ActionProposal
from observability.metrics import actions_pending_total
from observability.tracing import traced_span

logger = logging.getLogger(__name__)


# ── Proposal builder ──────────────────────────────────────────────────────────

def _build_proposals(
    resolution_raw: dict,
    correlation_id: str,
    order_id: str | None,
    customer_id: str | None,
    reanalysis_count: int = 0,
) -> list[ActionProposal]:
    """
    Convert agent output into ActionProposal documents.

    Prefers the structured 'action_proposals' list from the resolution JSON.
    Falls back to creating one 'manual_review' proposal per recommended_action string.
    """
    structured: list[dict] = resolution_raw.get("action_proposals", [])

    if not structured:
        for i, action_str in enumerate(resolution_raw.get("recommended_actions", [])):
            structured.append({
                "action_type":        "manual_review",
                "action_description": action_str,
                "action_payload":     {"priority": i + 1},
                "predicted_before_state": {},
                "predicted_after_state":  {},
            })

    proposals = []
    for raw in structured:
        proposals.append(ActionProposal(
            correlation_id=correlation_id,
            order_id=str(order_id) if order_id else None,
            customer_id=str(customer_id) if customer_id else None,
            action_type=raw.get("action_type", "unknown"),
            action_description=raw.get("action_description", ""),
            action_payload=raw.get("action_payload", {}),
            predicted_before_state=raw.get("predicted_before_state", {}),
            predicted_after_state=raw.get("predicted_after_state",  {}),
            reanalysis_count=reanalysis_count,
        ))
    return proposals


# ── Message handler ───────────────────────────────────────────────────────────

async def _handle_task(msg_value: dict) -> None:
    correlation_id: str = msg_value.get("correlation_id", str(uuid.uuid4()))

    with traced_span("thinker.resolve_job", correlation_id=correlation_id):
        logger.info("Thinker received task correlation_id=%s", correlation_id)

        fallout_input = FalloutInput(
            order_id=msg_value.get("order_id"),
            order_number=msg_value.get("order_number"),
            customer_id=msg_value.get("customer_id"),
            error_code=msg_value.get("error_code", ""),
            error_message=msg_value.get("error_message", ""),
            domain=msg_value.get("domain", ""),
            severity=msg_value.get("severity", "MEDIUM"),
            free_text=msg_value.get("free_text", ""),
        )

        # Agent is sync/CPU-bound — run in thread pool to free the event loop
        resolution = await asyncio.to_thread(run_agent, fallout_input, correlation_id)

        reanalysis_count = msg_value.get("reanalysis_count", 0)

        proposals = _build_proposals(
            {
                "recommended_actions": resolution.recommended_actions,
                "action_proposals":    resolution.action_proposals,
            },
            correlation_id=correlation_id,
            order_id=fallout_input.order_id,
            customer_id=fallout_input.customer_id,
            reanalysis_count=reanalysis_count,
        )

        for proposal in proposals:
            await create_action(proposal)
            actions_pending_total.labels(action_type=proposal.action_type).inc()
            logger.info(
                "Action proposal created id=%s type=%s correlation=%s",
                proposal.id, proposal.action_type, correlation_id,
            )


# ── Entry point ────────────────────────────────────────────────────────────────

async def run_thinker() -> None:
    await ensure_indexes()

    consumer = await create_consumer(
        TOPIC_AI_ORCHESTRATION,
        group_id=GROUP_THINKER,
    )
    logger.info("Thinker listening on Kafka topic '%s'", TOPIC_AI_ORCHESTRATION)

    try:
        async for msg in consumer:
            task = msg.value
            try:
                await _handle_task(task)
            except Exception:
                logger.exception(
                    "Thinker handler failed for correlation_id=%s",
                    task.get("correlation_id", "?"),
                )
            finally:
                # Always commit so a poison-pill doesn't stall the consumer
                await consumer.commit()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [thinker] %(levelname)s %(message)s",
    )
    asyncio.run(run_thinker())
