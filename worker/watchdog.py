"""
Watchdog service — Phase 9 Kafka-based Defensive Execution.

Polls MongoDB every WATCHDOG_POLL_INTERVAL seconds for PENDING and APPROVED
ActionProposals and validates each against the live CRM state (Oracle/SQLite).

If the underlying CRM state has drifted from the action's predicted_before_state,
the action is transitioned to STALE and a fresh OrchestrationTask is published to
the `ai.orchestration.tasks` Kafka topic so the Thinker can re-analyse with
current state. Replaces the Phase 8 RabbitMQ (aio-pika) implementation.

Circuit breaker — prevents "agentic seizures" on volatile accounts:
  - Attempt 1 or 2 stale: mark STALE, publish re-analysis task to Kafka.
  - Attempt >= MAX_REANALYSIS_ATTEMPTS: mark ESCALATED_STALE, do NOT re-publish.
    An ESCALATED_STALE action surfaces in the Operator Portal for human review.

Run as a standalone Docker service:
  python -m worker.watchdog
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from db.mongo import ensure_indexes, list_actions, transition_action
from kafka.producer import create_producer, publish
from kafka.schemas import OrchestrationTask
from kafka.topics import TOPIC_AI_ORCHESTRATION
from models.action import ActionStatus, MAX_REANALYSIS_ATTEMPTS
from worker.drift import check_state_drift, read_live_crm_state

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("WATCHDOG_POLL_INTERVAL", "30"))
ACTOR         = "watchdog-service"


# ── Re-publish helper ──────────────────────────────────────────────────────────

async def _republish_for_reanalysis(producer, action, stale_reason: str) -> None:
    """
    Publish a fresh OrchestrationTask to Kafka so the Thinker re-analyses the
    fallout with current CRM state. Carries reanalysis_count + 1 so the
    new ActionProposal inherits the circuit-breaker counter.
    """
    task = OrchestrationTask(
        correlation_id=str(uuid.uuid4()),
        task_type="REANALYSIS",
        order_id=int(action.order_id)    if action.order_id    else None,
        customer_id=int(action.customer_id) if action.customer_id else None,
        error_code="WATCHDOG_STALE",
        error_message=f"Action marked stale: {stale_reason}",
        domain="WATCHDOG",
        severity="HIGH",
        free_text=(
            f"Re-analysis triggered by Watchdog (attempt "
            f"{action.reanalysis_count + 1}/{MAX_REANALYSIS_ATTEMPTS}). "
            f"Original action: {action.id}. Reason: {stale_reason}"
        ),
        reanalysis=True,
        reanalysis_count=action.reanalysis_count + 1,
        stale_action_id=action.id,
    )
    await publish(
        producer,
        TOPIC_AI_ORCHESTRATION,
        task.model_dump(),
        key=str(action.customer_id or action.order_id or action.id),
    )
    logger.info(
        "WATCHDOG_REPUBLISHED action=%s attempt=%d/%d topic=%s",
        action.id, action.reanalysis_count + 1, MAX_REANALYSIS_ATTEMPTS,
        TOPIC_AI_ORCHESTRATION,
    )


# ── Watchdog sweep ─────────────────────────────────────────────────────────────

async def _sweep(producer) -> None:
    """One full sweep: check every PENDING and APPROVED action for state drift."""
    pending  = await list_actions(status=ActionStatus.PENDING,  limit=500)
    approved = await list_actions(status=ActionStatus.APPROVED, limit=500)
    candidates = pending + approved

    if not candidates:
        return

    logger.debug("Watchdog sweep: %d candidate action(s)", len(candidates))

    for action in candidates:
        if not action.customer_id and not action.order_id:
            continue

        try:
            live_state = await asyncio.to_thread(read_live_crm_state, action)
        except Exception as exc:
            logger.error("Watchdog CRM read failed for action %s: %s", action.id, exc)
            continue

        if live_state is None:
            continue

        drift_ok, drift_reason = check_state_drift(action, live_state)
        if drift_ok:
            continue

        # ── State has drifted ──────────────────────────────────────────────────

        if action.reanalysis_count >= MAX_REANALYSIS_ATTEMPTS:
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
                if producer:
                    try:
                        await _republish_for_reanalysis(producer, action, drift_reason)
                    except Exception as pub_exc:
                        logger.error(
                            "Watchdog republish failed for action %s: %s", action.id, pub_exc
                        )
            else:
                logger.debug(
                    "Watchdog CAS miss on action %s (already transitioned)", action.id
                )


# ── Post-execution verification sweep ─────────────────────────────────────────

_VERIFY_WINDOW_SECONDS = int(os.getenv("WATCHDOG_VERIFY_WINDOW_SECONDS", "300"))


async def _sweep_executed(producer) -> None:
    """
    Deferred after-state verification for recently EXECUTED actions.

    The Executor's post-write verify runs immediately after the Oracle write, but
    propagation lag means the SQLite sim may not reflect the new state yet.
    This sweep re-checks actions executed within the last WATCHDOG_VERIFY_WINDOW_SECONDS
    and triggers REANALYSIS if predicted_after_state still doesn't match live CRM state.

    Actions without a predicted_after_state, or where SQLite is unavailable, are skipped.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=_VERIFY_WINDOW_SECONDS)

    executed = await list_actions(status=ActionStatus.EXECUTED, limit=200)
    candidates = [
        a for a in executed
        if a.predicted_after_state
        and a.executed_at
        and a.executed_at.replace(tzinfo=timezone.utc) >= cutoff
    ]

    if not candidates:
        return

    logger.debug("Watchdog post-exec verify: %d recently-executed action(s)", len(candidates))

    for action in candidates:
        if not action.customer_id and not action.order_id:
            continue

        try:
            live_state = await asyncio.to_thread(read_live_crm_state, action)
        except Exception as exc:
            logger.error(
                "Watchdog post-exec CRM read failed for action %s: %s", action.id, exc
            )
            continue

        if live_state is None:
            continue

        # Compare predicted_after_state keys against current live state.
        # Any mismatch on a field known to the drift checker signals state drift.
        from worker.drift import _EXACT_MATCH_FIELDS, _NUMERIC_TOLERANT_FIELDS

        drift_reason: str | None = None
        for key, expected_val in action.predicted_after_state.items():
            live_val = live_state.get(key)
            if expected_val is None or live_val is None:
                continue
            if key in _EXACT_MATCH_FIELDS:
                if str(expected_val).upper() != str(live_val).upper():
                    drift_reason = (
                        f"post-exec: after-state mismatch on {key}: "
                        f"expected={expected_val!r} live={live_val!r}"
                    )
                    break
            elif key in _NUMERIC_TOLERANT_FIELDS:
                tol = _NUMERIC_TOLERANT_FIELDS[key]
                try:
                    if abs(float(expected_val) - float(live_val)) > tol:
                        drift_reason = (
                            f"post-exec: {key} expected≈{expected_val} live={live_val} "
                            f"(tolerance={tol})"
                        )
                        break
                except (TypeError, ValueError):
                    pass

        if drift_reason is None:
            continue

        # After-state not reached — trigger REANALYSIS so Thinker can re-propose.
        logger.warning(
            "WATCHDOG_POSTEXEC_DRIFT action=%s type=%s reason=%s",
            action.id, action.action_type, drift_reason,
        )

        if producer:
            try:
                task = OrchestrationTask(
                    correlation_id=str(uuid.uuid4()),
                    task_type="REANALYSIS",
                    order_id=int(action.order_id)      if action.order_id    else None,
                    customer_id=int(action.customer_id) if action.customer_id else None,
                    error_code="WATCHDOG_POSTEXEC_DRIFT",
                    error_message=drift_reason,
                    domain="WATCHDOG",
                    severity="HIGH",
                    free_text=(
                        f"Post-execution after-state verification failed. "
                        f"Original action: {action.id}. {drift_reason}"
                    ),
                    reanalysis=True,
                    reanalysis_count=action.reanalysis_count + 1,
                    stale_action_id=action.id,
                )
                await publish(
                    producer,
                    TOPIC_AI_ORCHESTRATION,
                    task.model_dump(),
                    key=str(action.customer_id or action.order_id or action.id),
                )
                logger.info(
                    "WATCHDOG_POSTEXEC_REANALYSIS published action=%s attempt=%d/%d",
                    action.id, action.reanalysis_count + 1, MAX_REANALYSIS_ATTEMPTS,
                )
            except Exception as pub_exc:
                logger.error(
                    "Watchdog post-exec republish failed for action %s: %s",
                    action.id, pub_exc,
                )


# ── Poll loop ──────────────────────────────────────────────────────────────────

async def _poll_loop() -> None:
    producer = None
    try:
        from kafka.producer import create_producer
        producer = await create_producer()
        logger.info(
            "Watchdog connected to Kafka topic '%s'", TOPIC_AI_ORCHESTRATION
        )
    except Exception as exc:
        logger.warning(
            "Watchdog cannot reach Kafka (%s) — will mark STALE but skip re-publish", exc
        )

    try:
        while True:
            try:
                await _sweep(producer)
            except Exception:
                logger.exception("Watchdog sweep error")

            try:
                await _sweep_executed(producer)
            except Exception:
                logger.exception("Watchdog post-exec sweep error")

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        if producer:
            await producer.stop()


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
