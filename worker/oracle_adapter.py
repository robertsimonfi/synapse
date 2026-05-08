"""
Oracle Adapter service — Phase 9 Write-Back Consumer.

Consumes WritebackCommand messages from the `legacy.commands.writeback` Kafka topic
and executes precise UPDATE or stored-procedure calls against Oracle.

Safety guarantees:
  1. Idempotency guard: before executing, the adapter re-reads the row's
     UPDATED_DATE and compares it with source_version_anchor. If the row has
     already been updated by a newer operation, the command is skipped (stale
     write guard — prevents duplicate saga application on replay).

  2. SCN anchor: the simulated source_version_anchor corresponds to the Oracle
     UPDATED_DATE at the time the AI analysed the subscriber document. Any
     divergence means the underlying state has changed and the write-back must
     not proceed blindly.

  3. Outbox status feedback: after each Oracle commit the adapter publishes
     back to the MongoDB outbox (COMPLETED / FAILED) so the audit trail is
     complete.

  4. DLQ pattern: after MAX_RETRIES the command is logged as FAILED and the
     outbox is marked accordingly — the operator portal surfaces these for
     human resolution.

Run as a standalone Docker service:
  python -m worker.oracle_adapter
"""

import asyncio
import logging
import os
from typing import Any

from db.ods import ensure_ods_indexes, mark_outbox_completed, mark_outbox_failed
from kafka.consumer import create_consumer
from kafka.topics import GROUP_ORACLE_ADAPTER, TOPIC_WRITEBACK

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.getenv("ORACLE_ADAPTER_MAX_RETRIES", "3"))


# ── Action-type → Oracle DML mapping ──────────────────────────────────────────

def _build_dml(action_type: str, where_clause: dict, set_values: dict) -> tuple[str, dict] | None:
    """
    Map an action_type to an Oracle UPDATE statement.

    Returns (sql, params) or None if the action has no Oracle target.
    Designed for exact parameterised bind variables — no string interpolation
    of user-supplied values to prevent SQL injection.
    """

    if action_type == "update_billing":
        # Apply a credit adjustment to the billing account
        ba_id = where_clause.get("billing_account_id")
        if ba_id is None:
            return None
        updates = []
        params: dict[str, Any] = {"ba_id": ba_id, "ts": "SYSTIMESTAMP"}
        if "current_balance" in set_values:
            updates.append("CURRENT_BALANCE = :current_balance")
            params["current_balance"] = set_values["current_balance"]
        if "status" in set_values:
            updates.append("STATUS = :status")
            params["status"] = set_values["status"]
        if "credit_limit" in set_values:
            updates.append("CREDIT_LIMIT = :credit_limit")
            params["credit_limit"] = set_values["credit_limit"]
        if not updates:
            return None
        sql = (
            f"UPDATE BILLING_ACCOUNT "
            f"SET {', '.join(updates)}, UPDATED_DATE = SYSTIMESTAMP "
            f"WHERE BILLING_ACCOUNT_ID = :ba_id"
        )
        return sql, params

    elif action_type == "update_subscription":
        sub_id = where_clause.get("subscription_id")
        if sub_id is None:
            return None
        updates = []
        params = {"sub_id": sub_id}
        for col in ("status", "suspend_reason"):
            if col in set_values:
                updates.append(f"{col.upper()} = :{col}")
                params[col] = set_values[col]
        if not updates:
            return None
        sql = (
            f"UPDATE SERVICE_SUBSCRIPTION "
            f"SET {', '.join(updates)}, UPDATED_DATE = SYSTIMESTAMP "
            f"WHERE SUBSCRIPTION_ID = :sub_id"
        )
        return sql, params

    elif action_type == "cancel_order":
        order_id = where_clause.get("order_id")
        if order_id is None:
            return None
        sql = (
            "UPDATE ORDER_HEADER "
            "SET STATUS = 'CANCELLED', UPDATED_DATE = SYSTIMESTAMP "
            "WHERE ORDER_ID = :order_id AND STATUS NOT IN ('COMPLETED','CANCELLED')"
        )
        return sql, {"order_id": order_id}

    elif action_type == "retry_provisioning":
        # Reset the fallout for re-processing
        fallout_id = where_clause.get("fallout_id")
        if fallout_id is None:
            return None
        sql = (
            "UPDATE ORDER_FALLOUT "
            "SET STATUS = 'IN_PROGRESS', RETRY_COUNT = RETRY_COUNT + 1 "
            "WHERE FALLOUT_ID = :fallout_id AND STATUS IN ('OPEN','IN_PROGRESS','ESCALATED')"
        )
        return sql, {"fallout_id": fallout_id}

    elif action_type == "create_ticket":
        # INSERT a new incident ticket
        ticket_number = set_values.get("ticket_number", f"TKT-AUTO-{where_clause.get('order_id','?')}")
        customer_id   = where_clause.get("customer_id")
        order_id      = where_clause.get("order_id")
        fallout_id    = where_clause.get("fallout_id")
        sql = (
            "INSERT INTO INCIDENT_TICKET "
            "(TICKET_NUMBER, CUSTOMER_ID, ORDER_ID, FALLOUT_ID, TITLE, CATEGORY, "
            " PRIORITY, STATUS, ASSIGNED_TEAM) "
            "VALUES (:ticket_number, :customer_id, :order_id, :fallout_id, :title, "
            "        :category, :priority, 'OPEN', :assigned_team)"
        )
        params = {
            "ticket_number":  ticket_number,
            "customer_id":    customer_id,
            "order_id":       order_id,
            "fallout_id":     fallout_id,
            "title":          set_values.get("title", "Auto-generated by Synapse AI"),
            "category":       set_values.get("category", "ORDER_FALLOUT"),
            "priority":       set_values.get("priority", "P3"),
            "assigned_team":  set_values.get("assigned_team", "BSS_SUPPORT"),
        }
        return sql, params

    # Other action types (escalate, notify_customer) have no Oracle target
    return None


async def _execute_oracle_dml(
    sql: str,
    params: dict,
    source_version_anchor: str | None,
    table_name: str,
    where_clause: dict,
) -> bool:
    """
    Execute a parameterised DML statement against Oracle.

    Returns True on success. Raises on failure.
    The source_version_anchor guard is applied for UPDATE statements — if the
    target row's UPDATED_DATE is newer than the anchor, the write is skipped.
    """
    from agent.db import get_connection

    def _do_execute():
        conn = get_connection()
        cur  = conn.cursor()
        try:
            # Stale-write guard for UPDATE operations
            if source_version_anchor and table_name in (
                "BILLING_ACCOUNT", "SERVICE_SUBSCRIPTION", "ORDER_HEADER", "ORDER_FALLOUT"
            ):
                pk_col = next(
                    (v for k, v in {
                        "billing_account_id": "BILLING_ACCOUNT_ID",
                        "subscription_id":    "SUBSCRIPTION_ID",
                        "order_id":           "ORDER_ID",
                        "fallout_id":         "FALLOUT_ID",
                    }.items() if k in where_clause),
                    None,
                )
                if pk_col:
                    pk_val = where_clause[pk_col.lower()]
                    cur.execute(
                        f"SELECT TO_CHAR(UPDATED_DATE, 'YYYY-MM-DD HH24:MI:SS') AS updated_date "
                        f"FROM {table_name} WHERE {pk_col} = :pk",
                        {"pk": pk_val},
                    )
                    row = cur.fetchone()
                    if row and row[0] and row[0] > source_version_anchor:
                        logger.warning(
                            "Stale-write guard: %s row updated_date=%s > anchor=%s — skipping",
                            table_name, row[0], source_version_anchor,
                        )
                        return True  # Idempotent skip

            cur.execute(sql, params)
            conn.commit()
            logger.info("Oracle DML executed: %s rows_affected=%d", sql[:80], cur.rowcount)
            return True
        finally:
            cur.close()
            conn.close()

    return await asyncio.to_thread(_do_execute)


async def _handle_command(cmd: dict) -> bool:
    """
    Process a single WritebackCommand. Returns True on success.
    Handles both real Oracle and SQLite (dev) — SQLite DML is a no-op stub
    since the write-back path only makes sense with a live Oracle instance.
    """
    import os as _os
    db_type = _os.getenv("DB_TYPE", "sqlite").lower()

    outbox_id              = cmd.get("outbox_id", "unknown")
    action_type            = cmd.get("action_type", "")
    table_name             = cmd.get("table_name", "")
    where_clause           = cmd.get("where_clause", {})
    set_values             = cmd.get("set_values", {})
    source_version_anchor  = cmd.get("source_version_anchor")

    dml = _build_dml(action_type, where_clause, set_values)

    if dml is None:
        # Action has no Oracle write target — acknowledge cleanly
        logger.info("No Oracle DML for action_type=%s — marking completed", action_type)
        await mark_outbox_completed(outbox_id)
        return True

    sql, params = dml

    if db_type == "sqlite":
        logger.info(
            "[SQLite mode] Would execute: %s — marking outbox completed (dev no-op)", sql[:80]
        )
        await mark_outbox_completed(outbox_id)
        return True

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await _execute_oracle_dml(sql, params, source_version_anchor, table_name, where_clause)
            await mark_outbox_completed(outbox_id)
            return True
        except Exception as exc:
            logger.error(
                "Oracle DML attempt %d/%d failed outbox_id=%s: %s",
                attempt, MAX_RETRIES, outbox_id, exc,
            )
            if attempt == MAX_RETRIES:
                await mark_outbox_failed(outbox_id, str(exc))
                return False
            await asyncio.sleep(2 ** attempt)

    return False


# ── Entry point ────────────────────────────────────────────────────────────────

async def run_oracle_adapter() -> None:
    await ensure_ods_indexes()

    consumer = await create_consumer(
        TOPIC_WRITEBACK,
        group_id=GROUP_ORACLE_ADAPTER,
    )
    logger.info("Oracle Adapter consuming from %s", TOPIC_WRITEBACK)

    try:
        async for msg in consumer:
            cmd = msg.value
            outbox_id = cmd.get("outbox_id", "?")
            logger.info(
                "Oracle Adapter received command outbox_id=%s action_type=%s",
                outbox_id, cmd.get("action_type"),
            )
            try:
                await _handle_command(cmd)
            except Exception:
                logger.exception("Oracle Adapter unhandled error for outbox_id=%s", outbox_id)
            finally:
                await consumer.commit()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [oracle_adapter] %(levelname)s %(message)s",
    )
    asyncio.run(run_oracle_adapter())
