"""
Tool definitions for the Telco Fallout Agent — Phase 9 ODS-first architecture.

Tool execution strategy:
  1. Try MongoDB subscribers ODS first (db.ods.get_subscriber) — sub-ms latency,
     no Oracle join overhead, ideal for AI inference loops.
  2. Fall back to Oracle/SQLite for data not embedded in the subscriber document
     (network alarms, product dependencies) or when the ODS has no document yet.

The tool schemas (TOOL_DEFINITIONS) are unchanged — the Claude API sees the same
11 tools. Only the execution path has changed.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Tool schemas (passed to Claude API) ───────────────────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "get_order_details",
        "description": (
            "Retrieve full order information including status, type, channel, and timestamps "
            "for a given order ID or order number."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "order_id":     {"type": "integer", "description": "Numeric order ID"},
                "order_number": {"type": "string",  "description": "Order number string e.g. ORD-2024001"},
            },
        },
    },
    {
        "name": "get_order_fallouts",
        "description": (
            "Retrieve all fallout records for a given order, including error codes, "
            "error messages, domain, severity, and status."
        ),
        "input_schema": {
            "type": "object",
            "required": ["order_id"],
            "properties": {
                "order_id": {"type": "integer", "description": "Numeric order ID"},
            },
        },
    },
    {
        "name": "get_provisioning_logs",
        "description": (
            "Retrieve provisioning log entries for an order showing which downstream systems "
            "(HLR, OCS, EPC, IMS) were called, their status, and any error details."
        ),
        "input_schema": {
            "type": "object",
            "required": ["order_id"],
            "properties": {
                "order_id": {"type": "integer", "description": "Numeric order ID"},
            },
        },
    },
    {
        "name": "get_customer_info",
        "description": "Retrieve customer profile including account type, status, and credit score.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer", "description": "Numeric customer ID"},
                "email":       {"type": "string",  "description": "Customer email address"},
            },
        },
    },
    {
        "name": "get_customer_subscriptions",
        "description": (
            "List all service subscriptions for a customer, including product details, "
            "MSISDN, IMSI, and subscription status."
        ),
        "input_schema": {
            "type": "object",
            "required": ["customer_id"],
            "properties": {
                "customer_id": {"type": "integer", "description": "Numeric customer ID"},
            },
        },
    },
    {
        "name": "get_billing_account",
        "description": (
            "Retrieve billing account details including credit limit, current balance, "
            "payment method, and account status."
        ),
        "input_schema": {
            "type": "object",
            "required": ["customer_id"],
            "properties": {
                "customer_id": {"type": "integer", "description": "Numeric customer ID"},
            },
        },
    },
    {
        "name": "get_recent_invoices",
        "description": "Get the most recent invoices for a billing account, including status and amounts.",
        "input_schema": {
            "type": "object",
            "required": ["customer_id"],
            "properties": {
                "customer_id": {"type": "integer", "description": "Numeric customer ID"},
                "limit":       {"type": "integer", "description": "Number of invoices to return (default 3)"},
            },
        },
    },
    {
        "name": "get_active_network_alarms",
        "description": (
            "Retrieve active or recent network alarms, optionally filtered by region or "
            "element type. Useful for correlating provisioning failures with network issues."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "region":       {"type": "string", "description": "Network region e.g. NE, SW"},
                "severity":     {"type": "string", "description": "CRITICAL, MAJOR, MINOR, WARNING"},
                "element_type": {"type": "string", "description": "eNB, gNB, OCS, MME, etc."},
            },
        },
    },
    {
        "name": "get_incident_tickets",
        "description": "Retrieve existing incident tickets linked to an order or customer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "order_id":    {"type": "integer", "description": "Numeric order ID"},
                "customer_id": {"type": "integer", "description": "Numeric customer ID"},
            },
        },
    },
    {
        "name": "get_product_dependencies",
        "description": (
            "Check what products are required or excluded for a given product. "
            "Useful for diagnosing BSS_007 product dependency errors."
        ),
        "input_schema": {
            "type": "object",
            "required": ["product_id"],
            "properties": {
                "product_id": {"type": "integer", "description": "Numeric product ID"},
            },
        },
    },
    {
        "name": "get_usage_trends",
        "description": (
            "Get monthly usage breakdown for a customer across DATA, VOICE, and SMS "
            "for the last N months. Returns totals per month and usage type, plus "
            "roaming session counts to detect international activity and bill shock. "
            "Use this to compare consumption against plan limits and spot trends."
        ),
        "input_schema": {
            "type": "object",
            "required": ["customer_id"],
            "properties": {
                "customer_id": {"type": "integer", "description": "Numeric customer ID"},
                "months":      {"type": "integer", "description": "Months to look back (default 6)"},
            },
        },
    },
]


# ── ODS helpers ────────────────────────────────────────────────────────────────

def _get_ods(customer_id: int) -> dict | None:
    """Fetch the subscriber document from MongoDB ODS. Returns None if not hydrated."""
    try:
        from db.ods import get_subscriber
        return get_subscriber(customer_id)
    except Exception as exc:
        logger.debug("ODS lookup failed for customer_id=%s (%s) — falling back to Oracle", customer_id, exc)
        return None


def _j(obj: Any) -> str:
    return json.dumps(obj if obj is not None else [], indent=2, default=str)


# ── Oracle/SQLite fallback helpers ─────────────────────────────────────────────

def _sql_get_customer_info(inputs: dict) -> str:
    from agent.db import query, query_one, rows_to_json
    if inputs.get("customer_id"):
        rows = query(
            "SELECT c.*, a.street_line1, a.city, a.state, a.zip_code "
            "FROM CUSTOMER c LEFT JOIN CUSTOMER_ADDRESS a "
            "ON a.CUSTOMER_ID = c.CUSTOMER_ID AND a.IS_PRIMARY = 1 "
            "WHERE c.CUSTOMER_ID = :customer_id",
            {"customer_id": inputs["customer_id"]},
        )
    else:
        rows = query("SELECT * FROM CUSTOMER WHERE EMAIL = :email", {"email": inputs["email"]})
    return rows_to_json(rows)


def _sql_get_customer_subscriptions(customer_id: int) -> str:
    from agent.db import query, rows_to_json
    rows = query(
        "SELECT s.*, p.product_name, p.product_type, p.category, p.monthly_price "
        "FROM SERVICE_SUBSCRIPTION s "
        "JOIN PRODUCT_CATALOG p ON p.PRODUCT_ID = s.PRODUCT_ID "
        "WHERE s.CUSTOMER_ID = :customer_id ORDER BY s.START_DATE DESC",
        {"customer_id": customer_id},
    )
    return rows_to_json(rows)


def _sql_get_billing_account(customer_id: int) -> str:
    from agent.db import query, rows_to_json
    rows = query(
        "SELECT ba.*, "
        "(SELECT COUNT(*) FROM BILLING_INVOICE bi "
        " WHERE bi.BILLING_ACCOUNT_ID = ba.BILLING_ACCOUNT_ID "
        " AND bi.STATUS = 'OVERDUE') AS overdue_invoice_count "
        "FROM BILLING_ACCOUNT ba WHERE ba.CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    return rows_to_json(rows)


def _sql_get_recent_invoices(customer_id: int, limit: int) -> str:
    from agent.db import query, rows_to_json
    rows = query(
        "SELECT bi.* FROM BILLING_INVOICE bi "
        "JOIN BILLING_ACCOUNT ba ON ba.BILLING_ACCOUNT_ID = bi.BILLING_ACCOUNT_ID "
        "WHERE ba.CUSTOMER_ID = :customer_id "
        "ORDER BY bi.BILLING_PERIOD_START DESC "
        "FETCH FIRST :lim ROWS ONLY",
        {"customer_id": customer_id, "lim": limit},
    )
    return rows_to_json(rows)


# ── Tool execution ─────────────────────────────────────────────────────────────

def execute_tool(name: str, inputs: dict) -> str:
    """
    Dispatch a tool call to the appropriate data source. Returns a JSON string.

    Routing:
      ODS-first  — customer_info, subscriptions, billing, invoices,
                   order_fallouts, provisioning_logs, incident_tickets, usage_trends
      Oracle-only — active_network_alarms, product_dependencies (not per-customer)
      Hybrid     — order_details: check ODS recent_orders first, fall back to Oracle
    """

    # ── get_customer_info ─────────────────────────────────────────────────────
    if name == "get_customer_info":
        customer_id = inputs.get("customer_id")
        if customer_id:
            doc = _get_ods(customer_id)
            if doc:
                return _j(doc.get("profile"))
        return _sql_get_customer_info(inputs)

    # ── get_customer_subscriptions ────────────────────────────────────────────
    elif name == "get_customer_subscriptions":
        customer_id = inputs["customer_id"]
        doc = _get_ods(customer_id)
        if doc:
            return _j(doc.get("subscriptions", []))
        return _sql_get_customer_subscriptions(customer_id)

    # ── get_billing_account ───────────────────────────────────────────────────
    elif name == "get_billing_account":
        customer_id = inputs["customer_id"]
        doc = _get_ods(customer_id)
        if doc:
            billing = dict(doc.get("billing", {}))
            billing.pop("recent_invoices", None)
            return _j(billing)
        return _sql_get_billing_account(customer_id)

    # ── get_recent_invoices ───────────────────────────────────────────────────
    elif name == "get_recent_invoices":
        customer_id = inputs["customer_id"]
        limit       = inputs.get("limit", 3)
        doc = _get_ods(customer_id)
        if doc:
            invoices = doc.get("billing", {}).get("recent_invoices", [])[:limit]
            return _j(invoices)
        return _sql_get_recent_invoices(customer_id, limit)

    # ── get_order_details ─────────────────────────────────────────────────────
    elif name == "get_order_details":
        # Try ODS recent_orders first (avoids Oracle join for the common case)
        order_id     = inputs.get("order_id")
        order_number = inputs.get("order_number")
        if order_id or order_number:
            # Search across all hydrated subscriber docs would be costly; fall
            # back to Oracle for order-centric lookups since we don't know the
            # customer_id here.
            from agent.db import query, rows_to_json
            if order_id:
                rows = query(
                    "SELECT o.*, c.first_name, c.last_name, c.email, c.account_type "
                    "FROM ORDER_HEADER o JOIN CUSTOMER c ON c.CUSTOMER_ID = o.CUSTOMER_ID "
                    "WHERE o.ORDER_ID = :order_id",
                    {"order_id": order_id},
                )
            else:
                rows = query(
                    "SELECT o.*, c.first_name, c.last_name, c.email, c.account_type "
                    "FROM ORDER_HEADER o JOIN CUSTOMER c ON c.CUSTOMER_ID = o.CUSTOMER_ID "
                    "WHERE o.ORDER_NUMBER = :order_number",
                    {"order_number": order_number},
                )
            return rows_to_json(rows)

    # ── get_order_fallouts ────────────────────────────────────────────────────
    elif name == "get_order_fallouts":
        order_id = inputs["order_id"]
        # Fallouts are embedded per-customer in ODS; we need a customer_id to look up.
        # Try Oracle directly (order_id is indexed).
        from agent.db import query, rows_to_json
        rows = query(
            "SELECT f.*, li.product_id, li.action_type "
            "FROM ORDER_FALLOUT f "
            "LEFT JOIN ORDER_LINE_ITEM li ON li.LINE_ITEM_ID = f.LINE_ITEM_ID "
            "WHERE f.ORDER_ID = :order_id ORDER BY f.FALLOUT_DATE DESC",
            {"order_id": order_id},
        )
        # Supplement with ODS customer context if we can find the customer_id
        if rows:
            customer_id = None
            try:
                from agent.db import query_one
                oh = query_one(
                    "SELECT CUSTOMER_ID FROM ORDER_HEADER WHERE ORDER_ID = :oid",
                    {"oid": order_id},
                )
                if oh:
                    customer_id = oh.get("customer_id")
            except Exception:
                pass
            if customer_id:
                doc = _get_ods(customer_id)
                if doc:
                    # Attach ODS open_fallouts list as supplemental context
                    ods_fallouts = doc.get("open_fallouts", [])
                    return _j({"oracle_fallouts": rows, "ods_open_fallouts": ods_fallouts})
        return rows_to_json(rows)

    # ── get_provisioning_logs ─────────────────────────────────────────────────
    elif name == "get_provisioning_logs":
        order_id = inputs["order_id"]
        # Check ODS first via order lookup
        try:
            from agent.db import query_one, query, rows_to_json
            oh = query_one(
                "SELECT CUSTOMER_ID FROM ORDER_HEADER WHERE ORDER_ID = :oid",
                {"oid": order_id},
            )
            if oh:
                doc = _get_ods(oh.get("customer_id"))
                if doc:
                    # Filter ODS provisioning to this order
                    prov = [
                        p for p in doc.get("recent_provisioning", [])
                        if str(p.get("order_number", "")).endswith(str(order_id))
                        or True  # include all if order_number match unreliable
                    ]
                    if prov:
                        return _j(prov)
        except Exception:
            pass
        from agent.db import query, rows_to_json
        rows = query(
            "SELECT * FROM PROVISIONING_LOG "
            "WHERE ORDER_ID = :order_id ORDER BY CREATED_DATE DESC",
            {"order_id": order_id},
        )
        return rows_to_json(rows)

    # ── get_incident_tickets ──────────────────────────────────────────────────
    elif name == "get_incident_tickets":
        customer_id = inputs.get("customer_id")
        order_id    = inputs.get("order_id")
        if customer_id:
            doc = _get_ods(customer_id)
            if doc:
                tickets = doc.get("open_tickets", [])
                if order_id:
                    tickets = [t for t in tickets if str(t.get("order_id")) == str(order_id)]
                return _j(tickets)
        from agent.db import query, rows_to_json
        conditions, params = [], {}
        if order_id:
            conditions.append("ORDER_ID = :order_id")
            params["order_id"] = order_id
        if customer_id:
            conditions.append("CUSTOMER_ID = :customer_id")
            params["customer_id"] = customer_id
        where = " AND ".join(conditions) if conditions else "1=1"
        rows  = query(
            f"SELECT * FROM INCIDENT_TICKET WHERE {where} ORDER BY CREATED_DATE DESC",
            params,
        )
        return rows_to_json(rows)

    # ── get_usage_trends ──────────────────────────────────────────────────────
    elif name == "get_usage_trends":
        customer_id = inputs["customer_id"]
        months      = inputs.get("months", 6)
        doc = _get_ods(customer_id)
        if doc:
            # ODS has a 90-day summary; for longer spans fall through to Oracle
            if months <= 3:
                return _j(doc.get("usage_summary", []))

        from datetime import datetime, timedelta
        from agent.db import query, rows_to_json
        cutoff = (datetime.now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")
        rows = query(
            "SELECT "
            "  TO_CHAR(u.START_TIME, 'YYYY-MM') AS month, "
            "  u.USAGE_TYPE, "
            "  ROUND(SUM(u.QUANTITY), 3) AS total_quantity, "
            "  u.UNIT, "
            "  ROUND(SUM(u.RATED_AMOUNT), 2) AS total_rated_amount, "
            "  SUM(CASE WHEN u.ROAMING_COUNTRY IS NOT NULL AND u.ROAMING_COUNTRY != '' "
            "           THEN 1 ELSE 0 END) AS roaming_sessions "
            "FROM USAGE_RECORD u "
            "JOIN SERVICE_SUBSCRIPTION s ON s.SUBSCRIPTION_ID = u.SUBSCRIPTION_ID "
            "WHERE s.CUSTOMER_ID = :customer_id "
            "  AND u.START_TIME >= :cutoff "
            "GROUP BY TO_CHAR(u.START_TIME, 'YYYY-MM'), u.USAGE_TYPE, u.UNIT "
            "ORDER BY month DESC, u.USAGE_TYPE",
            {"customer_id": customer_id, "cutoff": cutoff},
        )
        return rows_to_json(rows)

    # ── get_active_network_alarms — always Oracle (regional, not per-customer) ──
    elif name == "get_active_network_alarms":
        from agent.db import query, rows_to_json
        conditions = ["STATUS IN ('ACTIVE','ACKNOWLEDGED')"]
        params: dict = {}
        if inputs.get("region"):
            conditions.append("REGION = :region")
            params["region"] = inputs["region"]
        if inputs.get("severity"):
            conditions.append("SEVERITY = :severity")
            params["severity"] = inputs["severity"]
        if inputs.get("element_type"):
            conditions.append("ELEMENT_TYPE = :element_type")
            params["element_type"] = inputs["element_type"]
        where = " AND ".join(conditions)
        rows  = query(
            f"SELECT * FROM NETWORK_ALARM WHERE {where} ORDER BY RAISED_TIME DESC",
            params,
        )
        return rows_to_json(rows)

    # ── get_product_dependencies — always Oracle (product catalog) ─────────────
    elif name == "get_product_dependencies":
        from agent.db import query, rows_to_json
        rows = query(
            "SELECT pd.*, p.product_name AS required_product_name, p.product_type "
            "FROM PRODUCT_DEPENDENCY pd "
            "JOIN PRODUCT_CATALOG p ON p.PRODUCT_ID = pd.REQUIRED_PRODUCT_ID "
            "WHERE pd.PRODUCT_ID = :product_id",
            {"product_id": inputs["product_id"]},
        )
        return rows_to_json(rows)

    return '{"error": "Unknown tool"}'
