"""
Executor service — The Doer half of the Thinker/Doer split.

Polls MongoDB every EXECUTOR_POLL_INTERVAL seconds for APPROVED actions,
acquires an optimistic lock (APPROVED → EXECUTING), calls the typed handler,
then transitions to EXECUTED or FAILED.

Phase 8 additions — Defensive Execution:
  1. Pre-flight drift check (APPROVED → STALE if CRM state changed since analysis)
  2. Post-write state verification (EXECUTING → FAILED + compensate if after-state
     doesn't match prediction)
  3. Compensation stubs per action type (documented as partial-completion stubs;
     a full Saga step-registry is a Phase 9 concern)

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
from db.ods import create_outbox_entry
from models.action import ActionProposal, ActionStatus
from observability.metrics import (
    actions_failed_total,
    drift_detections_total,
    execution_latency_seconds,
    force_approved_total,
    handler_failures_total,
    pending_queue_depth,
)
from observability.tracing import traced_span
from worker.drift import check_state_drift, read_live_crm_state

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("EXECUTOR_POLL_INTERVAL", "10"))
ACTOR         = "executor-service"

Handler = Callable[[ActionProposal], Awaitable[dict]]


# ── Action handlers ────────────────────────────────────────────────────────────

async def _h_retry_provisioning(a: ActionProposal) -> dict:
    system = a.action_payload.get("system", "HLR")
    msisdn = a.action_payload.get("msisdn", "unknown")
    logger.info("Retrying provisioning on %s for MSISDN %s", system, msisdn)
    await asyncio.sleep(0.5)
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
    return {"result": "subscription_updated",
            "subscription_id": a.action_payload.get("subscription_id")}


async def _h_cancel_order(a: ActionProposal) -> dict:
    logger.info("Cancelling order for action %s", a.id)
    await asyncio.sleep(0.3)
    return {"result": "order_cancelled", "order_id": a.order_id}


async def _h_reprovision(a: ActionProposal) -> dict:
    system     = a.action_payload.get("system", "HLR")
    msisdn     = a.action_payload.get("msisdn") or "unknown"
    fallout_id = a.action_payload.get("fallout_id")

    # Last-chance MSISDN resolution: if the payload still has "unknown" here,
    # do one final SQLite lookup and write the result back into the payload so
    # _outbox_params (called immediately after this handler returns) picks it up.
    if (not msisdn or msisdn == "unknown") and a.customer_id:
        try:
            from worker.drift import get_primary_msisdn
            resolved = await asyncio.to_thread(get_primary_msisdn, int(a.customer_id))
            if resolved:
                a.action_payload["msisdn"] = resolved
                msisdn = resolved
                logger.info(
                    "Executor: MSISDN resolved at handler time "
                    "action=%s customer_id=%s msisdn=%s",
                    a.id, a.customer_id, msisdn,
                )
        except Exception as exc:
            logger.debug("Executor: MSISDN last-chance resolution failed: %s", exc)

    logger.info(
        "REPROVISION: Issuing PROV_RESYNC command for MSISDN %s on %s (fallout_id=%s)",
        msisdn, system, fallout_id,
    )
    await asyncio.sleep(0.5)
    return {
        "result":     "prov_resync_queued",
        "command":    "PROV_RESYNC",
        "system":     system,
        "msisdn":     msisdn,
        "fallout_id": fallout_id,
    }


async def _h_default(a: ActionProposal) -> dict:
    logger.warning("No handler for action_type=%s — marking executed", a.action_type)
    return {"result": "no_handler", "action_type": a.action_type}


_HANDLERS: dict[str, Handler] = {
    "retry_provisioning":  _h_retry_provisioning,
    "REPROVISION":         _h_reprovision,
    "reprovision":         _h_reprovision,
    "update_billing":      _h_update_billing,
    "create_ticket":       _h_create_ticket,
    "escalate":            _h_escalate,
    "notify_customer":     _h_notify_customer,
    "update_subscription": _h_update_subscription,
    "cancel_order":        _h_cancel_order,
    "manual_review":       _h_default,
}


# ── Phase 8: Compensation stubs ────────────────────────────────────────────────
# IMPORTANT: These are flat, single-step stubs. They do NOT handle partial-completion
# scenarios (e.g., billing updated but network activation failed mid-way). A full
# Saga step-registry with per-step rollback is a Phase 9 concern. Each stub here is
# idempotent by design — safe to call multiple times without compounding damage.

async def _compensate_provisioning(a: ActionProposal) -> dict:
    logger.error(
        "SAGA_COMPENSATE retry_provisioning action=%s — "
        "marking provisioning attempt as failed, alerting NOC", a.id
    )
    # Real impl: POST to NOC alerting endpoint; set provisioning log entry to COMPENSATION_FAILED
    return {"compensated": True, "action": "noc_alerted_provisioning_partial_failure"}


async def _compensate_billing(a: ActionProposal) -> dict:
    logger.error(
        "SAGA_COMPENSATE update_billing action=%s — "
        "reversing credit memo, creating audit ticket", a.id
    )
    # Real impl: reverse the credit memo via Billing API; create INC ticket for billing ops
    return {"compensated": True, "action": "credit_memo_reversal_queued"}


async def _compensate_cancel_order(a: ActionProposal) -> dict:
    logger.error(
        "SAGA_COMPENSATE cancel_order action=%s — "
        "order cancel in unknown state, operator review required", a.id
    )
    # Real impl: re-read order status, notify operator if status is ambiguous
    return {"compensated": True, "action": "operator_review_flagged"}


async def _compensate_subscription(a: ActionProposal) -> dict:
    logger.error(
        "SAGA_COMPENSATE update_subscription action=%s — "
        "subscription in unknown state, reverting to previous plan", a.id
    )
    return {"compensated": True, "action": "subscription_revert_queued"}


async def _compensate_reprovision(a: ActionProposal) -> dict:
    logger.error(
        "SAGA_COMPENSATE REPROVISION action=%s — "
        "PROV_RESYNC in unknown state, alerting NOC for manual HLR check", a.id
    )
    return {"compensated": True, "action": "noc_alerted_prov_resync_partial_failure"}


_COMPENSATIONS: dict[str, Handler] = {
    "retry_provisioning":  _compensate_provisioning,
    "REPROVISION":         _compensate_reprovision,
    "reprovision":         _compensate_reprovision,
    "update_billing":      _compensate_billing,
    "cancel_order":        _compensate_cancel_order,
    "update_subscription": _compensate_subscription,
}


# ── Phase 9: Outbox write-back builder ────────────────────────────────────────

def _outbox_params(action: ActionProposal) -> tuple[str, str, dict, dict] | None:
    """
    Map an ActionProposal to (table_name, operation, where_clause, set_values) for the
    Saga write-back outbox. Returns None for action types with no Oracle target.
    """
    p = action.action_payload

    if action.action_type == "update_billing":
        return (
            "BILLING_ACCOUNT", "UPDATE",
            {"billing_account_id": p.get("account_id") or p.get("billing_account_id")},
            {k: p[k] for k in ("current_balance", "status", "credit_limit") if k in p},
        )
    elif action.action_type == "update_subscription":
        return (
            "SERVICE_SUBSCRIPTION", "UPDATE",
            {"subscription_id": p.get("subscription_id")},
            {k: p[k] for k in ("status", "suspend_reason") if k in p},
        )
    elif action.action_type == "cancel_order":
        return (
            "ORDER_HEADER", "UPDATE",
            {"order_id": int(action.order_id) if action.order_id else p.get("order_id")},
            {"status": "CANCELLED"},
        )
    elif action.action_type == "retry_provisioning":
        return (
            "ORDER_FALLOUT", "UPDATE",
            {"fallout_id": p.get("fallout_id")},
            {"status": "IN_PROGRESS"},
        )
    elif action.action_type in ("REPROVISION", "reprovision"):
        return (
            "ORDER_FALLOUT", "UPDATE",
            {"fallout_id": p.get("fallout_id") or p.get("order_id")},
            {
                "status":      "IN_PROGRESS",
                "command":     "PROV_RESYNC",
                "system":      p.get("system", "HLR"),
                "msisdn":      p.get("msisdn"),
                "customer_id": action.customer_id,  # fallback for subscription lookup
            },
        )
    elif action.action_type == "create_ticket":
        return (
            "INCIDENT_TICKET", "INSERT",
            {
                "customer_id": int(action.customer_id) if action.customer_id else None,
                "order_id":    int(action.order_id)    if action.order_id    else None,
                "fallout_id":  p.get("fallout_id"),
            },
            {
                "title":         p.get("title", action.action_description[:200]),
                "category":      p.get("category", "ORDER_FALLOUT"),
                "priority":      p.get("priority", "P3"),
                "assigned_team": p.get("assigned_team", "BSS_SUPPORT"),
            },
        )

    return None  # escalate, notify_customer, manual_review — no Oracle write


# ── Phase 8: Pre-flight drift check ───────────────────────────────────────────

async def _pre_flight_check(action: ActionProposal) -> tuple[bool, str, dict | None]:
    """
    Read live CRM state and compare against predicted_before_state.
    Returns (safe, reason, live_state). live_state is None when CRM is unavailable.
    Runs before acquiring the EXECUTING lock so APPROVED → STALE is a valid transition.
    """
    logger.info("Executor: [action_id: %s] Running pre-flight drift check...", action.id)
    logger.info("Executor: Predicted state: %s", action.predicted_before_state)

    try:
        live_state = await asyncio.to_thread(read_live_crm_state, action)
    except Exception as exc:
        logger.warning("Pre-flight CRM read failed for action %s: %s — proceeding", action.id, exc)
        return True, "crm_read_error_skipped", None

    if live_state is None:
        return True, "sqlite_unavailable", None

    logger.info("Executor: Current state: %s", live_state)
    ok, reason = check_state_drift(action, live_state)
    if not ok:
        logger.warning("Executor: ⚠️  STATE DRIFT DETECTED — marking action STALE")
    return ok, reason, live_state


# ── Phase 8: Post-write state verification ────────────────────────────────────

async def _post_write_verify(action: ActionProposal) -> tuple[bool, str]:
    """
    After the CRM write, re-read the live state and confirm predicted_after_state
    was reached. This catches silent failures (CRM returns 200 but data unchanged).

    Returns (True, "ok") if the after-state matches or cannot be verified.
    Returns (False, reason) if the state demonstrably did not change as expected.
    """
    if not action.predicted_after_state:
        return True, "no_after_state_defined"

    try:
        live_state = await asyncio.to_thread(read_live_crm_state, action)
    except Exception as exc:
        logger.warning("Post-write verify read failed for action %s: %s — assuming success",
                       action.id, exc)
        return True, "crm_read_error_skipped"

    if live_state is None:
        return True, "sqlite_unavailable"

    # Compare predicted_after_state keys against live state.
    # Uses the same exact-match logic as the drift check for status fields.
    from worker.drift import _EXACT_MATCH_FIELDS, _NUMERIC_TOLERANT_FIELDS
    for key, predicted_val in action.predicted_after_state.items():
        live_val = live_state.get(key)
        if live_val is None:
            continue  # field not in live state — cannot verify
        if key in _EXACT_MATCH_FIELDS:
            if str(predicted_val).upper() != str(live_val).upper():
                return (
                    False,
                    f"post-write: {key} expected={predicted_val!r} got={live_val!r} "
                    f"— possible silent CRM failure",
                )
        elif key in _NUMERIC_TOLERANT_FIELDS:
            try:
                if abs(float(predicted_val) - float(live_val)) > _NUMERIC_TOLERANT_FIELDS[key]:
                    return (
                        False,
                        f"post-write: {key} expected≈{predicted_val} got={live_val}",
                    )
            except (TypeError, ValueError):
                pass

    return True, "ok"


# ── Execution logic ────────────────────────────────────────────────────────────

async def _execute_action(action: ActionProposal) -> None:
    with traced_span("executor.execute_action", correlation_id=action.correlation_id,
                     attributes={"action.id": action.id, "action.type": action.action_type}):

        force_mode = action.status == ActionStatus.FORCE_APPROVED

        if not force_mode:
            # ── Phase 8: Pre-flight check (before acquiring the lock) ─────────
            # Runs APPROVED → STALE if CRM state has drifted since the agent's analysis.
            # Only one executor replica will win the CAS; others will see the version
            # mismatch and skip cleanly (same path as the EXECUTING lock contention).
            drift_ok, drift_reason, live_state = await _pre_flight_check(action)
            if not drift_ok:
                opco_id = action.action_payload.get("opco_id", "unknown")
                drift_detections_total.labels(
                    opco_id=opco_id, action_type=action.action_type
                ).inc()
                stale = await transition_action(
                    action.id, action.version, ActionStatus.STALE,
                    actor=ACTOR,
                    extra_updates={
                        "stale_reason": drift_reason,
                        "predicted_state_at_check": action.predicted_before_state,
                        "current_state_at_check": live_state,
                    },
                )
                if stale is not None:
                    logger.warning(
                        "PRE_FLIGHT_STALE action=%s type=%s reason=%s",
                        action.id, action.action_type, drift_reason,
                    )
                return
        else:
            logger.warning(
                "Executor: ⚠️  FORCE_APPROVED: Skipping pre-flight drift check per operator request"
            )
            logger.warning(
                "Executor: ⚠️  WARNING: Safety checks bypassed. Operator: %s, action_id=%s",
                action.approved_by, action.id,
            )
            force_approved_total.labels(
                operator=action.approved_by or "unknown",
                action_type=action.action_type,
            ).inc()

        # Acquire execution lock via optimistic CAS: APPROVED|FORCE_APPROVED → EXECUTING
        locked = await transition_action(
            action.id, action.version, ActionStatus.EXECUTING, actor=ACTOR
        )
        if locked is None:
            logger.debug("Lock contention on action %s — skipping", action.id)
            return

        approval_ts     = action.updated_at.replace(tzinfo=timezone.utc)
        handler         = _HANDLERS.get(action.action_type, _h_default)
        current_version = locked.version
        last_error: str | None = None

        for attempt in range(1, action.max_retries + 1):
            try:
                result = await handler(action)
                now     = datetime.now(timezone.utc)
                latency = (now - approval_ts).total_seconds()

                # ── Phase 8: Post-write verify ─────────────────────────────────
                verify_ok, verify_reason = await _post_write_verify(action)
                if not verify_ok:
                    logger.warning(
                        "POST_WRITE_VERIFY_FAIL action=%s reason=%s — triggering compensation",
                        action.id, verify_reason,
                    )
                    compensation = _COMPENSATIONS.get(action.action_type)
                    if compensation:
                        try:
                            await compensation(action)
                        except Exception as comp_exc:
                            logger.error("COMPENSATION_FAILED action=%s: %s", action.id, comp_exc)
                    raise RuntimeError(f"Post-write verification failed: {verify_reason}")

                # ── Phase 9: Write to outbox → Relay → Kafka → Oracle Adapter ────────
                outbox_p = _outbox_params(action)
                if outbox_p:
                    table_name, operation, where_clause, set_values = outbox_p
                    try:
                        await create_outbox_entry(
                            action_id=action.id,
                            correlation_id=action.correlation_id,
                            action_type=action.action_type,
                            table_name=table_name,
                            operation=operation,
                            where_clause=where_clause,
                            set_values=set_values,
                            source_version_anchor=action.predicted_before_state.get(
                                "updated_date"
                            ),
                        )
                    except Exception as outbox_exc:
                        logger.error(
                            "Outbox write failed for action %s: %s — execution marked EXECUTED anyway",
                            action.id, outbox_exc,
                        )

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

                # Phase 7 — post-action engagement dispatch (non-blocking)
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
                    logger.warning("Executor: Handler threw: %s", type(exc).__name__)
                    logger.info(
                        "Executor: Retrying... (attempt %d/%d)",
                        attempt, action.max_retries,
                    )
                    logger.warning(
                        "Action %s attempt %d/%d failed: %s — retrying in %ds",
                        action.id, attempt, action.max_retries, last_error, backoff,
                    )
                    await asyncio.sleep(backoff)

        # All retries exhausted → run compensation then FAILED (DLQ signal)
        compensation = _COMPENSATIONS.get(action.action_type)
        comp_type: str | None = None
        comp_result: dict | None = None
        if compensation:
            comp_type = action.action_type
            try:
                comp_result = await compensation(action)
            except Exception as comp_exc:
                logger.error(
                    "COMPENSATION_FAILED action=%s after exhausted retries: %s",
                    action.id, comp_exc,
                )

        logger.error("Executor: ❌ HANDLER FAILED after %d attempts", action.max_retries)
        handler_failures_total.labels(action_type=action.action_type).inc()

        await transition_action(
            action.id, current_version, ActionStatus.FAILED,
            actor=ACTOR,
            extra_updates={
                "execution_error": last_error,
                "retry_count": action.max_retries,
                "handler_attempt_count": action.max_retries,
                "compensation_type": comp_type,
                "compensation_result": comp_result,
            },
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
            approved       = await list_actions(status=ActionStatus.APPROVED,       limit=20)
            force_approved = await list_actions(status=ActionStatus.FORCE_APPROVED, limit=20)
            pending        = await list_actions(status=ActionStatus.PENDING,         limit=1000)
            pending_queue_depth.set(len(pending))

            to_execute = approved + force_approved
            if to_execute:
                logger.info(
                    "Executor: %d APPROVED + %d FORCE_APPROVED action(s) to execute",
                    len(approved), len(force_approved),
                )
                await asyncio.gather(*[_execute_action(a) for a in to_execute])

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
