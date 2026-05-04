"""
Tool definitions for the Telco Fallout Agent.
Each tool maps to a Claude tool_use function that queries the Oracle/SQLite DB.
"""

from agent.db import query, rows_to_json

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
]

# ── Tool execution ─────────────────────────────────────────────────────────────

def execute_tool(name: str, inputs: dict) -> str:
    """Dispatch a tool call to the appropriate DB query. Returns JSON string."""

    if name == "get_order_details":
        if inputs.get("order_id"):
            rows = query(
                "SELECT o.*, c.first_name, c.last_name, c.email, c.account_type "
                "FROM ORDER_HEADER o JOIN CUSTOMER c ON c.CUSTOMER_ID = o.CUSTOMER_ID "
                "WHERE o.ORDER_ID = :order_id",
                {"order_id": inputs["order_id"]},
            )
        else:
            rows = query(
                "SELECT o.*, c.first_name, c.last_name, c.email, c.account_type "
                "FROM ORDER_HEADER o JOIN CUSTOMER c ON c.CUSTOMER_ID = o.CUSTOMER_ID "
                "WHERE o.ORDER_NUMBER = :order_number",
                {"order_number": inputs["order_number"]},
            )
        return rows_to_json(rows)

    elif name == "get_order_fallouts":
        rows = query(
            "SELECT f.*, li.product_id, li.action_type "
            "FROM ORDER_FALLOUT f "
            "LEFT JOIN ORDER_LINE_ITEM li ON li.LINE_ITEM_ID = f.LINE_ITEM_ID "
            "WHERE f.ORDER_ID = :order_id "
            "ORDER BY f.FALLOUT_DATE DESC",
            {"order_id": inputs["order_id"]},
        )
        return rows_to_json(rows)

    elif name == "get_provisioning_logs":
        rows = query(
            "SELECT * FROM PROVISIONING_LOG "
            "WHERE ORDER_ID = :order_id "
            "ORDER BY CREATED_DATE DESC",
            {"order_id": inputs["order_id"]},
        )
        return rows_to_json(rows)

    elif name == "get_customer_info":
        if inputs.get("customer_id"):
            rows = query(
                "SELECT c.*, a.street_line1, a.city, a.state, a.zip_code "
                "FROM CUSTOMER c LEFT JOIN CUSTOMER_ADDRESS a "
                "ON a.CUSTOMER_ID = c.CUSTOMER_ID AND a.IS_PRIMARY = 1 "
                "WHERE c.CUSTOMER_ID = :customer_id",
                {"customer_id": inputs["customer_id"]},
            )
        else:
            rows = query(
                "SELECT * FROM CUSTOMER WHERE EMAIL = :email",
                {"email": inputs["email"]},
            )
        return rows_to_json(rows)

    elif name == "get_customer_subscriptions":
        rows = query(
            "SELECT s.*, p.product_name, p.product_type, p.category, p.monthly_price "
            "FROM SERVICE_SUBSCRIPTION s "
            "JOIN PRODUCT_CATALOG p ON p.PRODUCT_ID = s.PRODUCT_ID "
            "WHERE s.CUSTOMER_ID = :customer_id "
            "ORDER BY s.START_DATE DESC",
            {"customer_id": inputs["customer_id"]},
        )
        return rows_to_json(rows)

    elif name == "get_billing_account":
        rows = query(
            "SELECT ba.*, "
            "(SELECT COUNT(*) FROM BILLING_INVOICE bi "
            " WHERE bi.BILLING_ACCOUNT_ID = ba.BILLING_ACCOUNT_ID "
            " AND bi.STATUS = 'OVERDUE') AS overdue_invoice_count "
            "FROM BILLING_ACCOUNT ba "
            "WHERE ba.CUSTOMER_ID = :customer_id",
            {"customer_id": inputs["customer_id"]},
        )
        return rows_to_json(rows)

    elif name == "get_recent_invoices":
        limit = inputs.get("limit", 3)
        rows  = query(
            "SELECT bi.* FROM BILLING_INVOICE bi "
            "JOIN BILLING_ACCOUNT ba ON ba.BILLING_ACCOUNT_ID = bi.BILLING_ACCOUNT_ID "
            "WHERE ba.CUSTOMER_ID = :customer_id "
            "ORDER BY bi.BILLING_PERIOD_START DESC "
            "FETCH FIRST :lim ROWS ONLY",
            {"customer_id": inputs["customer_id"], "lim": limit},
        )
        return rows_to_json(rows)

    elif name == "get_active_network_alarms":
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

    elif name == "get_incident_tickets":
        conditions, params = [], {}
        if inputs.get("order_id"):
            conditions.append("ORDER_ID = :order_id")
            params["order_id"] = inputs["order_id"]
        if inputs.get("customer_id"):
            conditions.append("CUSTOMER_ID = :customer_id")
            params["customer_id"] = inputs["customer_id"]
        where = " AND ".join(conditions) if conditions else "1=1"
        rows  = query(
            f"SELECT * FROM INCIDENT_TICKET WHERE {where} ORDER BY CREATED_DATE DESC",
            params,
        )
        return rows_to_json(rows)

    elif name == "get_product_dependencies":
        rows = query(
            "SELECT pd.*, p.product_name AS required_product_name, p.product_type "
            "FROM PRODUCT_DEPENDENCY pd "
            "JOIN PRODUCT_CATALOG p ON p.PRODUCT_ID = pd.REQUIRED_PRODUCT_ID "
            "WHERE pd.PRODUCT_ID = :product_id",
            {"product_id": inputs["product_id"]},
        )
        return rows_to_json(rows)

    return '{"error": "Unknown tool"}'
