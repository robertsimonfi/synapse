"""
Watchdog service — Phase 8 Defensive Execution.

Polls MongoDB every WATCHDOG_POLL_INTERVAL seconds for PENDING and APPROVED
ActionProposals and validates each against the live CRM state (SQLite/Oracle).

If the underlying CRM state has drifted from the action's predicted_before_state,
the action is transitioned to STALE and a fresh analysis job is re-published to
RabbitMQ (resolve_jobs queue) so the Thinker can re-analyse with current state.

Circuit breaker — prevents "agentic seizures" on volatile accounts:
  - Attempt 1 or 2 stale: mark STALE, re-publish to resolve_jobs.
  - Attempt >= MAX_REANALYSIS_ATTEMPTS: mark ESCALATED_STALE, do NOT re-publish.
    An ESCALATED_STALE action surfaces in the Operator Portal for human review.

The re-analysis message carries reanalysis_count + 1 so the Thinker stamps it
onto the new ActionProposal, allowing the Watchdog to count correctly on the
next cycle.

Run as a standalone Docker service:
  python -m worker.watchdog

Env vars:
  WATCHDOG_POLL_INTERVAL   seconds between full sweeps (default: 30)
  RABBITMQ_URL             amqp://... (default: amqp://guest:guest@localhost/)
  THINKER_QUEUE            queue name for re-analysis jobs (default: resolve_jobs)
  MONGODB_URI / MONGODB_DB standard Synapse mongo config
  SQLITE_PATH              path to telco SQLite DB (default: telco_dev.db)
"""

import asyncio
import json
import logging
import os
import uuid

import aio_pika

from db.mongo import ensure_indexes, list_actions, transition_action
from models.action import ActionStatus, MAX_REANALYSIS_ATTEMPTS
from worker.drift import check_state_drift, read_live_crm_state

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("WATCHDOG_POLL_INTERVAL", "30"))
RABBITMQ_URL  = os.getenv("RABBITMQ_URL",  "amqp://guest:guest@localhost/")
RESOLVE_QUEUE = os.getenv("THINKER_QUEUE", "resolve_jobs")
ACTOR         = "watchdog-service"


# ── Re-publish helper ──────────────────────────────────────────────────────────

async def _republish_for_reanalysis(
    channel: aio_pika.Channel,
    action,
    stale_reason: str,
) -> None:
    """
    Publish a fresh resolve_jobs message so the Thinker re-analyses the
    fallout with current CRM state. Carries reanalysis_count + 1 so the
    new ActionProposal inherits the circuit-breaker counter.
    """
    payload = {
        "correlation_id":   str(uuid.uuid4()),   # fresh correlation for new analysis
        "reanalysis":       True,
        "stale_action_id":  action.id,
        "reanalysis_count": action.reanalysis_count + 1,
        "fallout_input": {
            "order_id":     int(action.order_id)     if action.order_id     else None,
            "customer_id":  int(action.customer_id)  if action.customer_id  else None,
            "error_code":   "WATCHDOG_STALE",
            "error_message": f"Action marked stale: {stale_reason}",
            "domain":       "WATCHDOG",
            "severity":     "HIGH",
            "free_text": (
                f"Re-analysis triggered by Watchdog (attempt "
                f"{action.reanalysis_count + 1}/{MAX_REANALYSIS_ATTEMPTS}). "
                f"Original action: {action.id}. Reason: {stale_reason}"
            ),
        },
    }
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=RESOLVE_QUEUE,
    )
    logger.info(
        "WATCHDOG_REPUBLISHED action=%s attempt=%d/%d queue=%s",
        action.id, action.reanalysis_count + 1, MAX_REANALYSIS_ATTEMPTS, RESOLVE_QUEUE,
    )


# ── Watchdog sweep ─────────────────────────────────────────────────────────────

async def _sweep(mq_channel: aio_pika.Channel | None) -> None:
    """One full sweep: check every PENDING and APPROVED action for state drift."""
    pending  = await list_actions(status=ActionStatus.PENDING,  limit=500)
    approved = await list_actions(status=ActionStatus.APPROVED, limit=500)
    candidates = pending + approved

    if not candidates:
        return

    logger.debug("Watchdog sweep: %d candidate action(s)", len(candidates))

    for action in candidates:
        # Skip actions with no identifiers — nothing to look up
        if not action.customer_id and not action.order_id:
            continue

        try:
            live_state = await asyncio.to_thread(read_live_crm_state, action)
        except Exception as exc:
            logger.error("Watchdog CRM read failed for action %s: %s", action.id, exc)
            continue

        if live_state is None:
            continue  # SQLite unavailable — skip gracefully

        drift_ok, drift_reason = check_state_drift(action, live_state)
        if drift_ok:
            continue

        # ── State has drifted ──────────────────────────────────────────────────

        if action.reanalysis_count >= MAX_REANALYSIS_ATTEMPTS:
            # Circuit breaker tripped — escalate to human operator
            result = await transition_action(
                action.id, action.version, ActionStatus.ESCALATED_STALE,
                actor=ACTOR,
                extra_updates={
                    "stale_reason": (
                        f"max_reanalysis_exceeded ({action.reanalysis_count}/"
                        f"{MAX_REANALYSIS_ATTEMPTS}): {drift_reason}"
                    ),
                },
            )
            if result is not None:
                logger.warning(
                    "WATCHDOG_ESCALATED action=%s type=%s attempts=%d reason=%s",
                    action.id, action.action_type,
                    action.reanalysis_count, drift_reason,
                )
        else:
            # Mark STALE and re-publish for a fresh agent analysis
            result = await transition_action(
                action.id, action.version, ActionStatus.STALE,
                actor=ACTOR,
                extra_updates={
                    "stale_reason":     drift_reason,
                    "reanalysis_count": action.reanalysis_count + 1,
                },
            )
            if result is not None:
                logger.warning(
                    "WATCHDOG_STALE action=%s type=%s attempt=%d/%d reason=%s",
                    action.id, action.action_type,
                    action.reanalysis_count + 1, MAX_REANALYSIS_ATTEMPTS, drift_reason,
                )
                if mq_channel:
                    try:
                        await _republish_for_reanalysis(mq_channel, action, drift_reason)
                    except Exception as pub_exc:
                        logger.error(
                            "Watchdog republish failed for action %s: %s", action.id, pub_exc
                        )
            else:
                # Version conflict — another replica or the executor beat us to it
                logger.debug(
                    "Watchdog CAS miss on action %s (already transitioned)", action.id
                )


# ── Poll loop ──────────────────────────────────────────────────────────────────

async def _poll_loop() -> None:
    mq_channel: aio_pika.Channel | None = None

    try:
        connection = await aio_pika.connect_robust(
            RABBITMQ_URL,
            reconnect_interval=10,
        )
        mq_channel = await connection.channel()
        await mq_channel.declare_queue(RESOLVE_QUEUE, durable=True)
        logger.info("Watchdog connected to RabbitMQ queue '%s'", RESOLVE_QUEUE)
    except Exception as exc:
        logger.warning(
            "Watchdog cannot reach RabbitMQ (%s) — will mark STALE but skip re-publish", exc
        )

    while True:
        try:
            await _sweep(mq_channel)
        except Exception:
            logger.exception("Watchdog sweep error")

        await asyncio.sleep(POLL_INTERVAL)


async def run_watchdog() -> None:
    await ensure_indexes()
    logger.info(
        "Watchdog started (poll_interval=%ds, max_reanalysis=%d)",
        POLL_INTERVAL, MAX_REANALYSIS_ATTEMPTS,
    )
    await _poll_loop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [watchdog] %(levelname)s %(message)s",
    )
    asyncio.run(run_watchdog())
