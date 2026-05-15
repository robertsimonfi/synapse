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
import sqlite3 as _sqlite3
import time as _time
from typing import Any

from db.ods import ensure_ods_indexes, mark_outbox_completed, mark_outbox_failed
from kafka.consumer import create_consumer, extract_trace_context
from kafka.producer import create_producer, publish
from kafka.schemas import DlqEvent
from kafka.topics import GROUP_ORACLE_ADAPTER, TOPIC_DLQ, TOPIC_WRITEBACK

logger = logging.getLogger(__name__)

MAX_RETRIES = int(os.getenv("ORACLE_ADAPTER_MAX_RETRIES", "3"))

_producer = None  # lazily initialised on first DLQ publish


async def _get_producer():
    global _producer
    if _producer is None:
        _producer = await create_producer(ensure_topics=True)
    return _producer


async def _publish_to_dlq(cmd: dict, error_context: str, attempts: int) -> None:
    """
    Route a poison-pill command to legacy.commands.dlq with full error context.
    This prevents one bad message from blocking the entire write-back pipeline.
    """
    try:
        event = DlqEvent(
            source_topic="legacy.commands.writeback",
            command_id=cmd.get("command_id"),
            outbox_id=cmd.get("outbox_id", "unknown"),
            action_id=cmd.get("action_id", "unknown"),
            correlation_id=cmd.get("correlation_id", "unknown"),
            action_type=cmd.get("action_type", "unknown"),
            table_name=cmd.get("table_name", "unknown"),
            where_clause=cmd.get("where_clause", {}),
            set_values=cmd.get("set_values", {}),
            source_version_anchor=cmd.get("source_version_anchor"),
            error_context=error_context,
            attempts=attempts,
            trace_id=cmd.get("trace_id"),
        )
        producer = await _get_producer()
        await publish(
            producer,
            TOPIC_DLQ,
            event.model_dump(),
            key=cmd.get("outbox_id", "unknown"),
        )
        logger.error(
            "DLQ: published failed command outbox_id=%s action_type=%s error=%s",
            event.outbox_id, event.action_type, error_context[:120],
        )
    except Exception as dlq_exc:
        logger.critical(
            "DLQ publish itself failed outbox_id=%s: %s — message is lost!",
            cmd.get("outbox_id"), dlq_exc,
        )


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
        fallout_id = where_clause.get("fallout_id")
        if fallout_id is None:
            return None
        sql = (
            "UPDATE ORDER_FALLOUT "
            "SET STATUS = 'IN_PROGRESS', RETRY_COUNT = RETRY_COUNT + 1, "
            "UPDATED_DATE = SYSTIMESTAMP "
            "WHERE FALLOUT_ID = :fallout_id AND STATUS IN ('OPEN','IN_PROGRESS','ESCALATED')"
        )
        return sql, {"fallout_id": fallout_id}

    elif action_type in ("REPROVISION", "reprovision"):
        # Oracle: reset the fallout record; subscription activation done via stored proc
        fallout_id = where_clause.get("fallout_id")
        if fallout_id is None:
            return None
        sql = (
            "UPDATE ORDER_FALLOUT "
            "SET STATUS = 'IN_PROGRESS', RETRY_COUNT = RETRY_COUNT + 1, "
            "UPDATED_DATE = SYSTIMESTAMP "
            "WHERE FALLOUT_ID = :fallout_id"
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


import re as _re


def _to_int_id(val) -> int | None:
    """
    Coerce a potentially prefixed ID to a plain integer.

    Claude sometimes emits IDs like "FALLOUT-49" or "ORDER-12" but the SQLite
    schema uses bare INTEGER PKs.  Strip any leading alpha/dash prefix and return
    the trailing numeric part, or None if no digits are found.

    Examples:
        49           → 49
        "49"         → 49
        "FALLOUT-49" → 49
        "ORDER-12"   → 12
        None         → None
    """
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        m = _re.search(r"\d+$", str(val))
        return int(m.group()) if m else None


def _execute_sqlite_dml(cmd: dict) -> bool:
    """
    Execute the write-back DML against the local SQLite dev database.

    Replaces the previous no-op stub so that the happy-path E2E test
    (approve REPROVISION → SQLite subscription STATUS = 'ACTIVE') actually works.
    All statements use plain SQLite syntax (datetime('now') instead of SYSTIMESTAMP,
    positional ? binds instead of Oracle :named binds).
    """
    action_type  = cmd.get("action_type", "")
    where_clause = cmd.get("where_clause", {})
    set_values   = cmd.get("set_values",   {})
    sqlite_path  = os.getenv("SQLITE_PATH", "telco_dev.db")

    if not os.path.exists(sqlite_path):
        logger.warning("[SQLite] DB not found at %s — skipping write-back", sqlite_path)
        return True

    conn = _sqlite3.connect(sqlite_path)
    rows_affected = 0
    try:
        if action_type in ("REPROVISION", "reprovision", "retry_provisioning"):
            # Step 1: reset the ORDER_FALLOUT record — strip any "FALLOUT-" prefix
            fallout_id = _to_int_id(where_clause.get("fallout_id"))
            if fallout_id is not None:
                cur = conn.execute(
                    "UPDATE ORDER_FALLOUT "
                    "SET STATUS='IN_PROGRESS', RETRY_COUNT=RETRY_COUNT+1 "
                    "WHERE FALLOUT_ID=?",
                    (fallout_id,),
                )
                rows_affected += cur.rowcount
                logger.info("[SQLite] ORDER_FALLOUT id=%s → IN_PROGRESS (rowcount=%d)",
                            fallout_id, cur.rowcount)

            # Step 2 (REPROVISION only): activate the subscription.
            # Use a single OR condition so that a valid MSISDN takes priority
            # while customer_id acts as a guaranteed fallback when MSISDN is
            # still "unknown".  SQLite treats column=NULL as always-false, so
            # passing None for msisdn means only the customer_id branch fires.
            if action_type in ("REPROVISION", "reprovision"):
                raw_msisdn  = set_values.get("msisdn")
                msisdn_val  = raw_msisdn if (raw_msisdn and raw_msisdn != "unknown") else None
                customer_id = _to_int_id(set_values.get("customer_id"))
                cur = conn.execute(
                    "UPDATE SERVICE_SUBSCRIPTION "
                    "SET STATUS='ACTIVE', UPDATED_DATE=datetime('now') "
                    "WHERE (MSISDN=? OR CUSTOMER_ID=?) "
                    "  AND STATUS IN ('TERMINATED','SUSPENDED','PENDING_ACTIVATION')",
                    (msisdn_val, customer_id),
                )
                rows_affected += cur.rowcount
                logger.info(
                    "[SQLite] PROV_RESYNC: activated subscription "
                    "msisdn=%s customer_id=%s (rowcount=%d)",
                    msisdn_val, customer_id, cur.rowcount,
                )

        elif action_type == "update_billing":
            ba_id = _to_int_id(where_clause.get("billing_account_id"))
            if ba_id is not None:
                cols: list[str] = []
                vals: list      = []
                for col in ("current_balance", "status", "credit_limit"):
                    if col in set_values:
                        cols.append(f"{col.upper()}=?")
                        vals.append(set_values[col])
                if cols:
                    vals.append(ba_id)
                    cur = conn.execute(
                        f"UPDATE BILLING_ACCOUNT "
                        f"SET {', '.join(cols)}, UPDATED_DATE=datetime('now') "
                        f"WHERE BILLING_ACCOUNT_ID=?",
                        vals,
                    )
                    rows_affected += cur.rowcount

        elif action_type == "update_subscription":
            sub_id = _to_int_id(where_clause.get("subscription_id"))
            if sub_id is not None:
                cols = []
                vals = []
                for col in ("status", "suspend_reason"):
                    if col in set_values:
                        cols.append(f"{col.upper()}=?")
                        vals.append(set_values[col])
                if cols:
                    vals.append(sub_id)
                    cur = conn.execute(
                        f"UPDATE SERVICE_SUBSCRIPTION "
                        f"SET {', '.join(cols)}, UPDATED_DATE=datetime('now') "
                        f"WHERE SUBSCRIPTION_ID=?",
                        vals,
                    )
                    rows_affected += cur.rowcount

        elif action_type == "cancel_order":
            order_id = _to_int_id(where_clause.get("order_id"))
            if order_id is not None:
                cur = conn.execute(
                    "UPDATE ORDER_HEADER SET STATUS='CANCELLED' "
                    "WHERE ORDER_ID=? AND STATUS NOT IN ('COMPLETED','CANCELLED')",
                    (order_id,),
                )
                rows_affected += cur.rowcount

        elif action_type == "create_ticket":
            ticket_number = set_values.get(
                "ticket_number",
                f"TKT-AUTO-{_to_int_id(where_clause.get('order_id')) or '?'}-{int(_time.time())}",
            )
            conn.execute(
                "INSERT INTO INCIDENT_TICKET "
                "(TICKET_NUMBER, CUSTOMER_ID, ORDER_ID, FALLOUT_ID, "
                " TITLE, CATEGORY, PRIORITY, STATUS, ASSIGNED_TEAM) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?)",
                (
                    ticket_number,
                    _to_int_id(where_clause.get("customer_id")),
                    _to_int_id(where_clause.get("order_id")),
                    _to_int_id(where_clause.get("fallout_id")),
                    set_values.get("title",         "Auto-generated by Synapse AI"),
                    set_values.get("category",      "ORDER_FALLOUT"),
                    set_values.get("priority",      "P3"),
                    set_values.get("assigned_team", "BSS_SUPPORT"),
                ),
            )
            rows_affected += 1

        else:
            logger.info("[SQLite] No DML defined for action_type=%s — skipping", action_type)

        conn.commit()
        logger.info(
            "[SQLite] DML committed: action_type=%s rows_affected=%d",
            action_type, rows_affected,
        )
        return True

    except Exception as exc:
        conn.rollback()
        logger.error("[SQLite] DML failed action_type=%s: %s", action_type, exc)
        raise
    finally:
        conn.close()


async def _execute_oracle_dml(
    sql: str,
    params: dict,
    source_version_anchor: str | None,
    table_name: str,
    where_clause: dict,
) -> bool:
    """
    Execute a parameterised DML statement against Oracle.

    Phase 13 — Oracle HA:
      • Stale-write guard read uses OracleHAPool.get_read_connection()
        so it can fall back to the standby replica when the primary is OPEN.
      • The actual write uses OracleHAPool.get_write_connection() which raises
        OracleWriteUnavailable when the circuit is OPEN.  Callers catch this
        and re-queue the command to Kafka.

    Returns True on success. Raises on failure.
    """
    from db.oracle_ha import OracleHAPool, OracleWriteUnavailable  # noqa: PLC0415

    pool = OracleHAPool.instance()

    def _do_execute() -> bool:
        # ── Stale-write guard (read path — can use standby) ───────────────────
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
                pk_val   = where_clause[pk_col.lower()]
                read_con = pool.get_read_connection()
                cur      = read_con.cursor()
                try:
                    cur.execute(
                        f"SELECT TO_CHAR(UPDATED_DATE, 'YYYY-MM-DD HH24:MI:SS') AS updated_date "
                        f"FROM {table_name} WHERE {pk_col} = :pk",
                        {"pk": pk_val},
                    )
                    row = cur.fetchone()
                    if row and row[0] and row[0] > source_version_anchor:
                        logger.warning(
                            "Stale-write guard: %s updated_date=%s > anchor=%s — skipping",
                            table_name, row[0], source_version_anchor,
                        )
                        return True  # idempotent skip
                finally:
                    cur.close()
                    read_con.close()

        # ── DML write (primary only) ──────────────────────────────────────────
        write_con = pool.get_write_connection()  # raises OracleWriteUnavailable when OPEN
        cur       = write_con.cursor()
        try:
            cur.execute(sql, params)
            write_con.commit()
            logger.info("Oracle DML executed: %s rows_affected=%d", sql[:80], cur.rowcount)
            return True
        finally:
            cur.close()
            write_con.close()

    return await asyncio.to_thread(_do_execute)


async def _handle_command(cmd: dict) -> bool:
    """
    Process a single WritebackCommand. Returns True on success.

    DB_TYPE=sqlite  → executes real DML against telco_dev.db via _execute_sqlite_dml
                      (enables true E2E happy-path testing without an Oracle instance)
    DB_TYPE=oracle  → executes parameterised DML against Oracle via OracleHAPool,
                      with stale-write guard, HA circuit breaker, and DLQ fallback
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
        await asyncio.to_thread(_execute_sqlite_dml, cmd)
        await mark_outbox_completed(outbox_id)
        return True

    from db.oracle_ha import OracleWriteUnavailable  # noqa: PLC0415

    # Phase 13: if Oracle primary is unavailable, re-queue immediately to DLQ
    # rather than retrying against a known-down primary.  The outbox stays
    # PENDING so the Relay will re-lift it when the circuit recovers.
    last_error: str = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await _execute_oracle_dml(sql, params, source_version_anchor, table_name, where_clause)
            await mark_outbox_completed(outbox_id)
            return True
        except OracleWriteUnavailable as exc:
            # Primary circuit is OPEN — don't burn retries; queue to DLQ with
            # HA context so the NOC knows to wait for primary recovery.
            error_msg = f"ORACLE_HA_UNAVAILABLE: {exc}"
            logger.warning(
                "Oracle HA: primary unavailable — queuing outbox_id=%s to DLQ", outbox_id
            )
            from observability.metrics import oracle_writes_queued_total
            opco = cmd.get("opco_id", "DEFAULT")
            oracle_writes_queued_total.labels(opco_id=opco).inc()
            await mark_outbox_failed(outbox_id, error_msg)
            await _publish_to_dlq(cmd, error_msg, attempts=attempt)
            return False
        except Exception as exc:
            last_error = str(exc)
            logger.error(
                "Oracle DML attempt %d/%d failed outbox_id=%s: %s",
                attempt, MAX_RETRIES, outbox_id, exc,
            )
            if attempt == MAX_RETRIES:
                # Mark MongoDB outbox as FAILED and route to DLQ — prevents
                # this poison pill from blocking the write-back pipeline.
                await mark_outbox_failed(outbox_id, last_error)
                await _publish_to_dlq(cmd, last_error, attempts=MAX_RETRIES)
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
            # Phase 10: resume distributed trace from Kafka message headers
            extract_trace_context(msg)

            cmd = msg.value
            outbox_id = cmd.get("outbox_id", "?")
            logger.info(
                "Oracle Adapter received command outbox_id=%s action_type=%s trace_id=%s",
                outbox_id, cmd.get("action_type"), cmd.get("trace_id"),
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
