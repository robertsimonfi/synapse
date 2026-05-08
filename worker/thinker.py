"""
Thinker service — The Agent half of the Thinker/Doer split.

Consumes fallout-resolve jobs from the RabbitMQ "resolve_jobs" queue,
runs the Claude AI agent, then persists structured ActionProposals to MongoDB
so human operators can review them in the Operator Portal.

Run as a standalone Docker service:
  python -m worker.thinker
"""

import asyncio
import json
import logging
import os
import uuid

import aio_pika

from agent.agent import FalloutInput, run_agent
from db.mongo import create_action, ensure_indexes
from models.action import ActionProposal
from observability.metrics import actions_pending_total
from observability.tracing import traced_span

logger = logging.getLogger(__name__)

QUEUE_NAME    = os.getenv("THINKER_QUEUE", "resolve_jobs")
PREFETCH      = int(os.getenv("THINKER_PREFETCH", "5"))


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

    reanalysis_count is forwarded from Watchdog re-analysis messages (Phase 8)
    so the circuit breaker can count correctly across analysis cycles.
    """
    structured: list[dict] = resolution_raw.get("action_proposals", [])

    if not structured:
        for i, action_str in enumerate(resolution_raw.get("recommended_actions", [])):
            structured.append({
                "action_type": "manual_review",
                "action_description": action_str,
                "action_payload": {"priority": i + 1},
                "predicted_before_state": {},
                "predicted_after_state": {},
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
            predicted_after_state=raw.get("predicted_after_state", {}),
            reanalysis_count=reanalysis_count,
        ))
    return proposals


# ── Message handler ───────────────────────────────────────────────────────────


async def _handle_resolve_job(message: aio_pika.IncomingMessage) -> None:
    async with message.process(requeue_on_timeout=True):
        body = json.loads(message.body.decode())
        correlation_id: str = body.get("correlation_id", str(uuid.uuid4()))

        with traced_span("thinker.resolve_job", correlation_id=correlation_id):
            logger.info("Thinker received job correlation_id=%s", correlation_id)

            fallout_input = FalloutInput(**body.get("fallout_input", {}))

            # Agent is sync/CPU-bound — run in thread pool to free the event loop
            resolution = await asyncio.to_thread(run_agent, fallout_input, correlation_id)

            # Phase 8: carry forward reanalysis_count from Watchdog re-analysis messages
            reanalysis_count = body.get("reanalysis_count", 0)

            proposals = _build_proposals(
                {
                    "recommended_actions": resolution.recommended_actions,
                    "action_proposals": resolution.action_proposals,
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

    rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
    connection = await aio_pika.connect_robust(
        rabbitmq_url,
        reconnect_interval=5,
    )

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        logger.info("Thinker listening on queue '%s' (prefetch=%d)", QUEUE_NAME, PREFETCH)
        await queue.consume(_handle_resolve_job)
        await asyncio.Future()  # block forever


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [thinker] %(levelname)s %(message)s",
    )
    asyncio.run(run_thinker())
