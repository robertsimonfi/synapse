"""
Phase 8 — State Drift Detection.

Shared by both the Executor (pre-flight guard) and the Watchdog (async poll).
Reads the live CRM state from SQLite for the fields relevant to each action
type, then compares against the ActionProposal's predicted_before_state.

Design notes:
  - SQLite reads are synchronous; callers wrap with asyncio.to_thread.
  - Status field comparisons are case-insensitive exact matches — no tolerance.
  - Numeric fields (current_balance, credit_limit) use a configurable tolerance
    to avoid false positives from in-flight tax rounding or payment clearing.
  - Missing fields in either the predicted state or the live state are silently
    skipped (permissive default). Absence of data ≠ a detected drift.
  - If SQLite is unreachable, returns (True, "sqlite_unavailable") — the pre-
    flight guard treats unavailability as "no objection" to avoid blocking
    execution in degraded environments.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

from models.action import (
    ActionProposal,
    BEFORE_STATE_SCHEMA,
    BILLING_BALANCE_DRIFT_TOLERANCE,
)

logger = logging.getLogger(__name__)

_SQLITE_PATH = os.getenv("SQLITE_PATH", "telco_dev.db")

# Status fields are strings where ANY change is a hard abort — no tolerance.
_EXACT_MATCH_FIELDS: set[str] = {
    "customer_status",
    "subscription_status",
    "account_status",
    "order_status",
    "ticket_status",
    "last_provisioning_result",
}

# Numeric fields that allow a configurable tolerance band.
_NUMERIC_TOLERANT_FIELDS: dict[str, float] = {
    "current_balance": BILLING_BALANCE_DRIFT_TOLERANCE,
    "credit_limit":    BILLING_BALANCE_DRIFT_TOLERANCE,
}


# ── SQLite readers (synchronous — wrap with asyncio.to_thread) ─────────────────

def _open() -> sqlite3.Connection:
    conn = sqlite3.connect(_SQLITE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _read_customer_status(customer_id: str) -> Optional[str]:
    try:
        with _open() as conn:
            row = conn.execute(
                "SELECT STATUS FROM CUSTOMER WHERE CUSTOMER_ID = ?", (int(customer_id),)
            ).fetchone()
        return row["STATUS"] if row else None
    except Exception as exc:
        logger.debug("customer_status lookup failed for %s: %s", customer_id, exc)
        return None


def _read_subscription_state(customer_id: str, msisdn: Optional[str] = None) -> dict:
    try:
        with _open() as conn:
            if msisdn:
                row = conn.execute(
                    "SELECT STATUS FROM SERVICE_SUBSCRIPTION WHERE MSISDN = ?", (msisdn,)
                ).fetchone()
            else:
                row = conn.execute(
                    """SELECT STATUS FROM SERVICE_SUBSCRIPTION
                       WHERE CUSTOMER_ID = ?
                       ORDER BY CREATED_DATE DESC LIMIT 1""",
                    (int(customer_id),),
                ).fetchone()
        return {"subscription_status": row["STATUS"]} if row else {}
    except Exception as exc:
        logger.debug("subscription_state lookup failed for %s: %s", customer_id, exc)
        return {}


def _read_billing_state(customer_id: str) -> dict:
    try:
        with _open() as conn:
            row = conn.execute(
                """SELECT STATUS, CURRENT_BALANCE, CREDIT_LIMIT
                   FROM BILLING_ACCOUNT WHERE CUSTOMER_ID = ?""",
                (int(customer_id),),
            ).fetchone()
        if row:
            return {
                "account_status":  row["STATUS"],
                "current_balance": float(row["CURRENT_BALANCE"]),
                "credit_limit":    float(row["CREDIT_LIMIT"]),
            }
        return {}
    except Exception as exc:
        logger.debug("billing_state lookup failed for %s: %s", customer_id, exc)
        return {}


def _read_order_state(order_id: str) -> dict:
    try:
        with _open() as conn:
            row = conn.execute(
                "SELECT STATUS FROM ORDER_HEADER WHERE ORDER_ID = ?", (int(order_id),)
            ).fetchone()
        return {"order_status": row["STATUS"]} if row else {}
    except Exception as exc:
        logger.debug("order_state lookup failed for %s: %s", order_id, exc)
        return {}


def _read_provisioning_state(order_id: str) -> dict:
    try:
        with _open() as conn:
            row = conn.execute(
                """SELECT STATUS FROM PROVISIONING_LOG
                   WHERE ORDER_ID = ?
                   ORDER BY CREATED_DATE DESC LIMIT 1""",
                (int(order_id),),
            ).fetchone()
        return {"last_provisioning_result": row["STATUS"]} if row else {}
    except Exception as exc:
        logger.debug("provisioning_state lookup failed for %s: %s", order_id, exc)
        return {}


# ── Live state assembler ───────────────────────────────────────────────────────

def read_live_crm_state(action: ActionProposal) -> Optional[dict]:
    """
    Read the current live CRM state fields relevant to this action type.

    Returns a dict of field → current_value, or None if SQLite is unavailable.
    This function is synchronous; the caller must use asyncio.to_thread.
    """
    if not os.path.exists(_SQLITE_PATH):
        logger.debug("SQLite not found at %s — skipping drift check", _SQLITE_PATH)
        return None

    cid = action.customer_id
    oid = action.order_id
    msisdn = action.action_payload.get("msisdn")

    atype = action.action_type
    state: dict = {}

    if atype == "retry_provisioning":
        if cid:
            state["customer_status"] = _read_customer_status(cid)
            state.update(_read_subscription_state(cid, msisdn))
        if oid:
            state.update(_read_provisioning_state(oid))

    elif atype == "update_billing":
        if cid:
            state["customer_status"] = _read_customer_status(cid)
            state.update(_read_billing_state(cid))

    elif atype == "cancel_order":
        if oid:
            state.update(_read_order_state(oid))

    elif atype in ("escalate", "notify_customer"):
        if cid:
            state["customer_status"] = _read_customer_status(cid)

    elif atype == "update_subscription":
        if cid:
            state["customer_status"] = _read_customer_status(cid)
            state.update(_read_subscription_state(cid, msisdn))

    elif atype == "create_ticket":
        if cid:
            state["customer_status"] = _read_customer_status(cid)
        if oid:
            state.update(_read_order_state(oid))

    # manual_review and unknown types: return empty dict (no check performed)
    return {k: v for k, v in state.items() if v is not None}


# ── Drift comparator ──────────────────────────────────────────────────────────

def check_state_drift(action: ActionProposal, live_state: dict) -> tuple[bool, str]:
    """
    Compare predicted_before_state against live_state for the action's typed schema.

    Returns (True, "ok") if no significant drift is detected.
    Returns (False, reason) if a drift exceeds tolerance — caller should abort.

    Tolerance rules:
      Status fields  — exact match, case-insensitive. Any change = abort.
      current_balance, credit_limit — allow ±BILLING_BALANCE_DRIFT_TOLERANCE USD.
      Missing fields — silently skipped (permissive).
    """
    predicted = action.predicted_before_state
    schema_keys = BEFORE_STATE_SCHEMA.get(action.action_type, set())

    if not schema_keys:
        return True, "ok"  # no schema defined — no check performed

    for key in schema_keys:
        predicted_val = predicted.get(key)
        live_val      = live_state.get(key)

        # If either side is absent, skip — we don't fail on missing data
        if predicted_val is None or live_val is None:
            continue

        if key in _EXACT_MATCH_FIELDS:
            if str(predicted_val).upper() != str(live_val).upper():
                return (
                    False,
                    f"{key} changed: agent_saw={predicted_val!r} live_now={live_val!r}",
                )

        elif key in _NUMERIC_TOLERANT_FIELDS:
            tolerance = _NUMERIC_TOLERANT_FIELDS[key]
            try:
                diff = abs(float(predicted_val) - float(live_val))
                if diff > tolerance:
                    return (
                        False,
                        f"{key} drifted by ${diff:.2f} (tolerance=${tolerance:.2f}): "
                        f"agent_saw={predicted_val} live_now={live_val}",
                    )
            except (TypeError, ValueError):
                pass  # non-numeric value — fall through, no check

    return True, "ok"
