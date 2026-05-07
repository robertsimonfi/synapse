"""
Executor service — The Doer half of the Thinker/Doer split.

Polls MongoDB every EXECUTOR_POLL_INTERVAL seconds for APPROVED actions,
acquires an optimistic lock (APPROVED → EXECUTING), calls the typed handler,
then transitions to EXECUTED or FAILED.

Retry strategy: exponential backoff up to max_retries; after exhaustion the
action is marked FAILED and a structured log is emitted for the DLQ pattern.

Run as a standalone Docker service:
  python -m worker.executor
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Awaitable, Callable

from db.mongo import ensure_indexes, list_actions, transition_action
from models.action import ActionProposal, ActionStatus
from observability.metrics import (
    actions_failed_total,
    execution_latency_seconds,
    pending_queue_depth,
)
from observability.tracing import traced_span

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("EXECUTOR_POLL_INTERVAL", "10"))
ACTOR         = "executor-service"

Handler = Callable[[ActionProposal], Awaitable[dict]]


# ── Action handlers ────────────────────────────────────────────────────────────
# Replace the simulate calls below with real CRM/BSS/OSS API calls in production.


async def _h_retry_provisioning(a: ActionProposal) -> dict:
    system = a.action_payload.get("system", "HLR")
    msisdn = a.action_payload.get("msisdn", "unknown")
    logger.info("Retrying provisioning on %s for MSISDN %s", system, msisdn)
    await asyncio.sleep(0.5)  # simulate async CRM call
    return {"result": "provisioning_retried", "system": system, "msisdn": msisdn}


async def _h_update_billing(a: ActionProposal) -> dict:
    logger.info("Updating billing for action %s", a.id)
    await asyncio.sleep(0.3)
    return {"result": "billing_updated", "account": a.action_payload.get("account_id")}


async def _h_create_ticket(a: ActionProposal) -> dict:
    ticket_id = f"INC-{a.id[:8].upper()}"
    logger.info("Creating incident ticket %s", ticket_id)
    await asyncio.sleep(0.2)
    return {"result": "ticket_created", "ticket_id": ticket_id}


async def _h_escalate(a: ActionProposal) -> dict:
    team = a.action_payload.get("team", "engineering")
    logger.info("Escalating to %s for action %s", team, a.id)
    await asyncio.sleep(0.1)
    return {"result": "escalated", "team": team}


async def _h_notify_customer(a: ActionProposal) -> dict:
    channel = a.action_payload.get("channel", "SMS")
    logger.info("Notifying customer via %s for action %s", channel, a.id)
    await asyncio.sleep(0.2)
    return {"result": "customer_notified", "channel": channel}


async def _h_update_subscription(a: ActionProposal) -> dict:
    logger.info("Updating subscription for action %s", a.id)
    await asyncio.sleep(0.4)
    return {"result": "subscription_updated", "subscription_id": a.action_payload.get("subscription_id")}


async def _h_cancel_order(a: ActionProposal) -> dict:
    logger.info("Cancelling order for action %s", a.id)
    await asyncio.sleep(0.3)
    return {"result": "order_cancelled", "order_id": a.order_id}


async def _h_default(a: ActionProposal) -> dict:
    logger.warning("No handler registered for action_type=%s — marking executed", a.action_type)
    return {"result": "no_handler", "action_type": a.action_type}


_HANDLERS: dict[str, Handler] = {
    "retry_provisioning":  _h_retry_provisioning,
    "update_billing":      _h_update_billing,
    "create_ticket":       _h_create_ticket,
    "escalate":            _h_escalate,
    "notify_customer":     _h_notify_customer,
    "update_subscription": _h_update_subscription,
    "cancel_order":        _h_cancel_order,
    "manual_review":       _h_default,
}


# ── Execution logic ────────────────────────────────────────────────────────────


async def _execute_action(action: ActionProposal) -> None:
    with traced_span("executor.execute_action", correlation_id=action.correlation_id,
                     attributes={"action.id": action.id, "action.type": action.action_type}):

        # Acquire execution lock via optimistic CAS: APPROVED → EXECUTING
        locked = await transition_action(
            action.id, action.version, ActionStatus.EXECUTING, actor=ACTOR
        )
        if locked is None:
            # Another executor replica grabbed this action first — skip it
            logger.debug("Lock contention on action %s — skipping", action.id)
            return

        approval_ts = action.updated_at.replace(tzinfo=timezone.utc)
        handler = _HANDLERS.get(action.action_type, _h_default)
        current_version = locked.version
        last_error: str | None = None

        for attempt in range(1, action.max_retries + 1):
            try:
                result = await handler(action)
                now = datetime.now(timezone.utc)
                latency = (now - approval_ts).total_seconds()

                await transition_action(
                    action.id, current_version, ActionStatus.EXECUTED,
                    actor=ACTOR,
                    extra_updates={"executed_at": now, "execution_result": result},
                )
                execution_latency_seconds.labels(action_type=action.action_type).observe(latency)
                logger.info(
                    "HITL_APPROVED_EXECUTION action=%s type=%s latency=%.2fs",
                    action.id, action.action_type, latency,
                )

                # Phase 7 — post-action engagement dispatch (non-blocking; failure is soft)
                try:
                    from src.actions.notifications import EngagementDispatcher
                    await EngagementDispatcher.dispatch(action, result)
                except Exception as eng_exc:
                    logger.warning(
                        "Engagement dispatch skipped for action %s: %s", action.id, eng_exc
                    )

                return

            except Exception as exc:
                last_error = str(exc)
                if attempt < action.max_retries:
                    backoff = 2 ** (attempt - 1)
                    logger.warning(
                        "Action %s attempt %d/%d failed: %s — retrying in %ds",
                        action.id, attempt, action.max_retries, last_error, backoff,
                    )
                    await asyncio.sleep(backoff)

        # All retries exhausted → FAILED (DLQ signal)
        await transition_action(
            action.id, current_version, ActionStatus.FAILED,
            actor=ACTOR,
            extra_updates={"execution_error": last_error, "retry_count": action.max_retries},
        )
        actions_failed_total.labels(action_type=action.action_type).inc()
        logger.error(
            "HITL_EXECUTION_FAILED action=%s type=%s error=%s retries=%d",
            action.id, action.action_type, last_error, action.max_retries,
        )


# ── Poll loop ──────────────────────────────────────────────────────────────────


async def _poll_loop() -> None:
    while True:
        try:
            approved = await list_actions(status=ActionStatus.APPROVED, limit=20)
            pending  = await list_actions(status=ActionStatus.PENDING,  limit=1000)
            pending_queue_depth.set(len(pending))

            if approved:
                logger.info("Executor: %d APPROVED action(s) to execute", len(approved))
                await asyncio.gather(*[_execute_action(a) for a in approved])

        except Exception:
            logger.exception("Executor poll loop error")

        await asyncio.sleep(POLL_INTERVAL)


async def run_executor() -> None:
    await ensure_indexes()
    logger.info("Executor started (poll_interval=%ds)", POLL_INTERVAL)
    await _poll_loop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [executor] %(levelname)s %(message)s",
    )
    asyncio.run(run_executor())
