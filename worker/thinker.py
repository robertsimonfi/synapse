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
from billing.token_budget import check_quota, consume_tokens
from db.mongo import create_action, ensure_indexes
from kafka.consumer import create_consumer
from kafka.producer import create_producer, publish
from kafka.schemas import DlqEvent
from kafka.topics import GROUP_THINKER, TOPIC_AI_ORCHESTRATION, TOPIC_DLQ
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

_dlq_producer = None


async def _get_dlq_producer():
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = await create_producer()
    return _dlq_producer


async def _handle_task(msg_value: dict) -> None:
    correlation_id: str = msg_value.get("correlation_id", str(uuid.uuid4()))
    opco_id: str        = msg_value.get("opco_id") or "DEFAULT"

    with traced_span("thinker.resolve_job", correlation_id=correlation_id):
        logger.info(
            "Thinker received task correlation_id=%s opco=%s", correlation_id, opco_id
        )

        # Phase 13: check per-OpCo token quota before running the (expensive) agent
        quota = await check_quota(opco_id)
        if not quota.allowed:
            logger.warning(
                "Token quota EXHAUSTED opco=%s — routing task correlation=%s to DLQ",
                opco_id, correlation_id,
            )
            dlq_event = DlqEvent(
                source_topic=TOPIC_AI_ORCHESTRATION,
                outbox_id=correlation_id,
                action_id="N/A",
                correlation_id=correlation_id,
                action_type="resolve_fallout",
                table_name="N/A",
                error_context=(
                    f"QUOTA_EXCEEDED: opco={opco_id} consumed={quota.consumed} "
                    f"quota={quota.quota} period={quota.period}"
                ),
                attempts=1,
                opco_id=opco_id,
            )
            producer = await _get_dlq_producer()
            await publish(producer, TOPIC_DLQ, dlq_event.model_dump(), key=correlation_id)
            return

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

        # Phase 13: debit token usage from the OpCo's monthly budget
        usage = resolution.token_usage
        if usage.get("input_tokens") or usage.get("output_tokens"):
            await consume_tokens(
                opco_id,
                input_tokens=usage.get("input_tokens", 0),
                output_tokens=usage.get("output_tokens", 0),
            )

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

_rca_check_counter = 0
_RCA_INTERVAL = int(os.getenv("RCA_CHECK_INTERVAL_MESSAGES", "50"))


async def _maybe_run_rca(opco_id: str | None) -> None:
    """Phase 15: periodic RCA check every RCA_INTERVAL messages."""
    global _rca_check_counter
    _rca_check_counter += 1
    if _rca_check_counter % _RCA_INTERVAL != 0:
        return
    try:
        from agent.rca import run_rca
        result = await run_rca(opco_id)
        if result and result.cluster:
            logger.info(
                "rca: cluster detected %s=%s pct=%.0f%% — proposals created",
                result.cluster.dimension, result.cluster.value, result.cluster.pct * 100,
            )
    except Exception as exc:
        logger.warning("rca: check failed — %s", exc)


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
                await _maybe_run_rca(task.get("opco_id"))
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
