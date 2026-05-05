"""
Telco Fallout Resolution Agent — main orchestrator.

Flow:
  1. Receive an error input (order_id, error_code, description, etc.)
  2. Retrieve relevant runbook context via RAG (ChromaDB)
  3. Retrieve past lessons via memory loop (ChromaDB)
  4. Run Claude with tool_use to investigate the DB
  5. Return structured resolution: classification, root cause, recommended actions
  6. Run self-critique and store lesson if quality can be improved
"""

import json
import os
from dataclasses import dataclass, asdict

import anthropic

from agent.tools import TOOL_DEFINITIONS, execute_tool
from agent.memory import retrieve_memories, format_memories, self_critique_and_store
from rag.knowledge_base import retrieve as rag_retrieve, format_context, ingest_runbooks

# Ensure runbooks are indexed on first run
ingest_runbooks()

MODEL     = os.getenv("CLAUDE_MODEL",     "claude-opus-4-6")
MODEL_C360 = os.getenv("CLAUDE_MODEL_C360", "claude-sonnet-4-6")


@dataclass
class FalloutInput:
    """Input payload for the agent."""
    order_id:    int | None = None
    order_number: str | None = None
    customer_id: int | None = None
    error_code:  str = ""
    error_message: str = ""
    domain:      str = ""        # BSS, OSS, BILLING, etc.
    severity:    str = "MEDIUM"
    free_text:   str = ""        # any additional context from the operator


@dataclass
class FalloutResolution:
    """Structured resolution output."""
    classification: dict       # domain, severity, issue_type
    root_cause:     str
    recommended_actions: list[str]
    runbook_references:  list[str]
    confidence:     str        # HIGH, MEDIUM, LOW
    raw_reasoning:  str
    critique:       dict | None = None


def build_system_prompt(rag_context: str, memory_context: str) -> str:
    return f"""You are a Telco Fallout Resolution Agent for a major US carrier (T-Mobile style BSS/OSS).

Your job is to:
1. CLASSIFY the issue: determine the domain (BSS/OSS/BILLING/PROVISIONING/NETWORK/CRM),
   issue type, and severity.
2. INVESTIGATE using the available database tools to correlate data across orders,
   provisioning logs, billing, network alarms, and customer records.
3. IDENTIFY the root cause with supporting evidence from the data.
4. RECOMMEND concrete resolution actions in priority order.

{rag_context}

{memory_context}

Guidelines:
- Always call at minimum 2-3 tools to build a complete picture before concluding.
- Correlate provisioning failures with active network alarms when relevant.
- Check billing account health for any order-related issues.
- Be specific: cite data values (order IDs, error codes, system names) in your reasoning.
- At the end, respond with a JSON block wrapped in <resolution>...</resolution> tags.

Resolution JSON format:
{{
  "classification": {{
    "domain": "BSS|OSS|BILLING|PROVISIONING|NETWORK|CRM",
    "issue_type": "short label",
    "severity": "CRITICAL|HIGH|MEDIUM|LOW"
  }},
  "root_cause": "clear narrative explanation of what went wrong and why",
  "recommended_actions": [
    "Action 1 — specific and actionable",
    "Action 2 — ...",
    "Action 3 — ..."
  ],
  "runbook_references": ["order_fallout.txt", "provisioning_failure.txt"],
  "confidence": "HIGH|MEDIUM|LOW"
}}"""


def run_agent(fallout_input: FalloutInput) -> FalloutResolution:
    """
    Main agent entry point. Returns a FalloutResolution.
    """
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Build the issue description for retrieval
    issue_desc = (
        f"Error: {fallout_input.error_code} — {fallout_input.error_message}. "
        f"Domain: {fallout_input.domain}. "
        f"{fallout_input.free_text}"
    ).strip()

    # 1. RAG — retrieve runbook context
    rag_chunks  = rag_retrieve(issue_desc, top_k=4)
    rag_context = format_context(rag_chunks)
    runbook_refs = list({c["source"] for c in rag_chunks})

    # 2. Memory — retrieve past lessons
    memories       = retrieve_memories(issue_desc, top_k=3)
    memory_context = format_memories(memories)

    # 3. Build system prompt
    system = build_system_prompt(rag_context, memory_context)

    # 4. Build user message
    user_message = f"""Please investigate and resolve the following Telco fallout:

Error Code:    {fallout_input.error_code or 'Unknown'}
Error Message: {fallout_input.error_message or 'Not provided'}
Domain:        {fallout_input.domain or 'Unknown'}
Severity:      {fallout_input.severity}
Order ID:      {fallout_input.order_id or 'Not provided'}
Order Number:  {fallout_input.order_number or 'Not provided'}
Customer ID:   {fallout_input.customer_id or 'Not provided'}
Additional Context: {fallout_input.free_text or 'None'}

Use the available tools to investigate, then provide your structured resolution."""

    messages = [{"role": "user", "content": user_message}]

    # 5. Agentic tool-use loop
    raw_reasoning_parts = []
    max_iterations      = 10

    for _ in range(max_iterations):
        response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=system,
            tools=TOOL_DEFINITIONS,
            messages=messages,
        )

        # Collect text content
        for block in response.content:
            if hasattr(block, "text"):
                raw_reasoning_parts.append(block.text)

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason == "tool_use":
            # Process all tool calls
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     result,
                    })

            # Append assistant turn + tool results
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})
        else:
            break

    raw_reasoning = "\n".join(raw_reasoning_parts)

    # 6. Parse resolution JSON from <resolution>...</resolution> tags
    resolution_data = _parse_resolution(raw_reasoning)

    # 7. Self-critique and store lesson
    critique = self_critique_and_store(
        anthropic_client=client,
        issue_description=issue_desc,
        error_code=fallout_input.error_code,
        domain=fallout_input.domain,
        agent_output=raw_reasoning,
    )

    return FalloutResolution(
        classification=resolution_data.get("classification", {}),
        root_cause=resolution_data.get("root_cause", "Unable to determine root cause."),
        recommended_actions=resolution_data.get("recommended_actions", []),
        runbook_references=resolution_data.get("runbook_references", runbook_refs),
        confidence=resolution_data.get("confidence", "LOW"),
        raw_reasoning=raw_reasoning,
        critique=critique,
    )


def _parse_resolution(text: str) -> dict:
    """Extract JSON from <resolution>...</resolution> tags in agent output."""
    import re
    match = re.search(r"<resolution>(.*?)</resolution>", text, re.DOTALL)
    if not match:
        # Fallback: try to find a raw JSON block
        match = re.search(r"\{[\s\S]*\"root_cause\"[\s\S]*\}", text)
        if not match:
            return {}
        raw = match.group(0)
    else:
        raw = match.group(1).strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def resolution_to_dict(r: FalloutResolution) -> dict:
    return asdict(r)


# ── Customer 360 ───────────────────────────────────────────────────────────────

_C360_TOOL_NAMES = {
    "get_customer_info",
    "get_customer_subscriptions",
    "get_billing_account",
    "get_recent_invoices",
    "get_incident_tickets",
    "get_usage_trends",
}

C360_TOOL_DEFINITIONS = [t for t in TOOL_DEFINITIONS if t["name"] in _C360_TOOL_NAMES]


@dataclass
class Customer360Input:
    customer_id: int
    query: str = ""   # optional free-text question from the support rep or customer


@dataclass
class Customer360Summary:
    customer_id:              int
    profile_summary:          dict
    active_services:          list
    billing_health:           dict
    usage_analysis:           dict
    open_issues:              dict
    proactive_recommendations: list
    raw_reasoning:            str


def build_customer360_prompt(optional_query: str) -> str:
    query_section = (
        f"\nThe support rep has a specific question: {optional_query}\n"
        "Make sure your recommendations directly address this question."
        if optional_query else ""
    )
    return f"""You are a Telco Customer 360 Relationship Manager for a major US carrier (T-Mobile style).

Your persona: You are a proactive, empathetic support expert who builds a complete, \
data-driven picture of a customer's relationship with the carrier. You focus on customer \
satisfaction, churn prevention, and identifying the right moments to improve the experience.
{query_section}
Your job is to:
1. PROFILE the customer: account type, segment, tenure, credit health.
2. CATALOG active services: what plans and add-ons they have, at what price.
3. ASSESS billing health: current balance, overdue invoices, invoice trend across months.
4. ANALYZE usage vs. plan: detect if the customer is near data limits, has roaming charges, \
   or shows a changing consumption trend.
5. IDENTIFY open issues: incident tickets that signal frustration or churn risk.
6. GENERATE proactive recommendations with clear supporting evidence.

Guidelines:
- Always call get_customer_info, get_customer_subscriptions, get_billing_account, \
get_recent_invoices, and get_usage_trends before concluding.
- Call get_incident_tickets to detect churn risk signals (open/escalated tickets = high risk).
- Compare invoice totals across months to detect cost spikes.
- Roaming sessions in usage trends indicate international activity that causes bill shock.
- Be specific: cite real numbers (data GB used, invoice amounts, ticket counts, price changes).
- Respond with a JSON block wrapped in <summary>...</summary> tags.

Summary JSON format:
{{
  "profile_summary": {{
    "name": "First Last",
    "segment": "PREMIUM|STANDARD|VALUE",
    "account_status": "ACTIVE|SUSPENDED|PENDING",
    "account_type": "...",
    "tenure_years": 0.0,
    "credit_score": 0
  }},
  "active_services": [
    {{
      "product_name": "...",
      "product_type": "...",
      "status": "ACTIVE|SUSPENDED",
      "monthly_price": 0.0,
      "msisdn": "..."
    }}
  ],
  "billing_health": {{
    "current_balance": 0.0,
    "credit_limit": 0.0,
    "overdue_invoices": 0,
    "last_invoice_amount": 0.0,
    "avg_monthly_bill": 0.0,
    "billing_trend": "STABLE|INCREASING|DECREASING",
    "anomalies": ["describe any unexpected charges or spikes"]
  }},
  "usage_analysis": {{
    "data_gb_last_month": 0.0,
    "voice_minutes_last_month": 0,
    "sms_count_last_month": 0,
    "roaming_detected": false,
    "roaming_countries": [],
    "usage_trend": "STABLE|INCREASING|DECREASING"
  }},
  "open_issues": {{
    "ticket_count": 0,
    "churn_risk": "HIGH|MEDIUM|LOW|NONE",
    "ticket_titles": []
  }},
  "proactive_recommendations": [
    {{
      "type": "UPSELL|CHURN_RISK|BILLING_ISSUE|SERVICE_ISSUE|SAVINGS",
      "priority": "HIGH|MEDIUM|LOW",
      "recommendation": "One clear, actionable sentence.",
      "supporting_data": "The specific numbers that justify this recommendation."
    }}
  ]
}}"""


def run_customer_360(c360_input: Customer360Input) -> Customer360Summary:
    """Build a comprehensive Customer 360 summary for a given customer."""
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    system  = build_customer360_prompt(c360_input.query)
    user_msg = (
        f"Please build a complete Customer 360 profile for customer ID {c360_input.customer_id}."
        + (f" Specific question: {c360_input.query}" if c360_input.query else "")
    )
    messages = [{"role": "user", "content": user_msg}]

    raw_parts   = []
    max_iters   = 8

    for _ in range(max_iters):
        response = client.messages.create(
            model=MODEL_C360,
            max_tokens=4096,
            system=system,
            tools=C360_TOOL_DEFINITIONS,
            messages=messages,
        )

        for block in response.content:
            if hasattr(block, "text"):
                raw_parts.append(block.text)

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     result,
                    })
            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user",      "content": tool_results})
        else:
            break

    raw_reasoning = "\n".join(raw_parts)
    data          = _parse_summary(raw_reasoning)

    return Customer360Summary(
        customer_id=c360_input.customer_id,
        profile_summary=data.get("profile_summary", {}),
        active_services=data.get("active_services", []),
        billing_health=data.get("billing_health", {}),
        usage_analysis=data.get("usage_analysis", {}),
        open_issues=data.get("open_issues", {}),
        proactive_recommendations=data.get("proactive_recommendations", []),
        raw_reasoning=raw_reasoning,
    )


def _parse_summary(text: str) -> dict:
    """Extract JSON from <summary>...</summary> tags in agent output."""
    import re
    match = re.search(r"<summary>(.*?)</summary>", text, re.DOTALL)
    if not match:
        match = re.search(r"\{[\s\S]*\"profile_summary\"[\s\S]*\}", text)
        if not match:
            return {}
        raw = match.group(0)
    else:
        raw = match.group(1).strip()
        # Strip markdown code fence that Claude sometimes wraps around JSON
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def summary_to_dict(s: Customer360Summary) -> dict:
    return asdict(s)
