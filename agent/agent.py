"""
Telco Fallout Resolution Agent — main orchestrator.

Flow:
  1. Receive an error input (order_id, error_code, description, etc.)
  2. Route to the correct Expert domain via IntentRouter (Haiku, fast)
  3. Retrieve relevant runbook context via RAG (ChromaDB)
  4. Retrieve past lessons via memory loop (ChromaDB)
  5. Run Claude with tool_use to investigate the DB (expert tool subset)
  6. Internal Critic reviews the draft — triggers one revision pass if needed
  7. PolicyEngine validates any credit/discount actions against hard rules
  8. Return structured resolution with routing, critic, and policy metadata
  9. Run self-critique and store lesson if quality can be improved
"""

import json
import os
from dataclasses import dataclass, field, asdict
from datetime import date, datetime

import anthropic

from agent.tools import TOOL_DEFINITIONS, execute_tool
from agent.memory import retrieve_memories, format_memories, self_critique_and_store
from agent.router import IntentRouter, ExpertDomain, EXPERT_CONFIGS
from agent.critic import InternalCritic
from agent.policy import PolicyEngine, PolicyContext, extract_actions_from_text
from rag.knowledge_base import retrieve as rag_retrieve, format_context, ingest_runbooks

# Ensure runbooks are indexed on first run
ingest_runbooks()

MODEL      = os.getenv("CLAUDE_MODEL",      "claude-opus-4-6")
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
    classification:      dict       # domain, severity, issue_type
    root_cause:          str
    recommended_actions: list[str]
    runbook_references:  list[str]
    confidence:          str        # HIGH, MEDIUM, LOW
    raw_reasoning:       str
    action_proposals:    list[dict] = None   # structured HITL proposals for MongoDB
    critique:            dict | None = None
    # Phase 6 metadata
    routing_domain:      str = "GENERAL"
    routing_confidence:  float = 0.0
    critic_violations:   list[str] = field(default_factory=list)
    policy_violations:   list[str] = field(default_factory=list)
    # Phase 13: cumulative token usage across all API calls in this resolution
    token_usage:         dict = field(default_factory=lambda: {"input_tokens": 0, "output_tokens": 0})
    # Phase 16: structured Chain-of-Thought (Observation/Constraint/Strategy/Recommendation)
    chain_of_thought:    dict | None = None
    # Phase 3 (Contextual Smart Escalation): 1-sentence NOC card summary for the HITL approval card.
    # Example: "Angry Gold Tier customer disputing $40 data overage; ODS shows roaming active in France."
    manager_brief:       str | None = None
    # Originating channel tag — set to "CHATBOT_ESCALATION" when routed from the chatbot.
    origin:              str | None = None

    def __post_init__(self):
        if self.action_proposals is None:
            self.action_proposals = []


def _load_prompt_addenda() -> str:
    """Phase 15: load operator-approved prompt addenda from prompt_optimizer."""
    import json as _json
    addenda_path = os.path.join(os.path.dirname(__file__), "prompt_addenda.json")
    try:
        with open(addenda_path) as f:
            items: list[dict] = _json.load(f)
        if not items:
            return ""
        lines = ["Operator-approved AI behaviour guidelines (derived from correction analysis):"]
        for item in items:
            lines.append(f"- [{item.get('action_type', 'ALL')}] {item.get('addendum', '')}")
        return "\n".join(lines) + "\n"
    except FileNotFoundError:
        return ""


def build_system_prompt(
    rag_context: str, memory_context: str, expert_addendum: str = ""
) -> str:
    addenda = _load_prompt_addenda()
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
{addenda}{expert_addendum}
Guidelines:
- Always call at minimum 2-3 tools to build a complete picture before concluding.
- Correlate provisioning failures with active network alarms when relevant.
- Check billing account health for any order-related issues.
- Be specific: cite data values (order IDs, error codes, system names) in your reasoning.
- Before the <resolution> block, emit a <cot> block containing your full reasoning monologue in
  this exact JSON format so that NOC operators can audit every step of your thinking:

<cot>
{{
  "observation":              "What I see in the data — facts from tool outputs, no inference yet.",
  "constraint_check":         "Applicable regional policies, account flags, or business rules that constrain the options.",
  "strategy":                 "Which protocol or framework I am applying and why.",
  "recommendation":           "The concrete action I will propose and the expected outcome.",
  "natural_language_summary": "A 2-3 sentence human-readable explanation of your full reasoning. Include specific customer context (e.g. tenure, loyalty tier, account status) that shaped this decision. Write as if briefing a senior NOC manager."
}}
</cot>

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
  "confidence": "HIGH|MEDIUM|LOW",
  "manager_brief": "1-sentence NOC card summary for the HITL approval queue. Include: customer tier/segment, the core issue, and one key data point from your tool investigation. Example: 'Angry Gold Tier customer disputing £40 data overage; ODS shows roaming active in France with 3 GB consumed.'",
  "action_proposals": [
    {{
      "action_type": "retry_provisioning|update_billing|create_ticket|escalate|notify_customer|update_subscription|cancel_order",
      "action_description": "One concise sentence describing what this action does and why.",
      "action_payload": {{"key": "value pairs an executor needs to carry out this action (e.g. system, msisdn, order_id)"}},
      "predicted_before_state": {{
        // Populate ONLY the keys listed for your action_type. Use exact values
        // you observed from the database tools — do not guess or infer.
        // retry_provisioning  → customer_status, subscription_status, last_provisioning_result
        // update_billing       → account_status, current_balance, credit_limit
        // cancel_order         → order_status
        // escalate             → customer_status
        // notify_customer      → customer_status
        // update_subscription  → customer_status, subscription_status
        // create_ticket        → customer_status, order_status
        "key": "exact value you read from a tool result"
      }},
      "predicted_after_state": {{
        // Same keys as predicted_before_state, but with the values you expect
        // after the action executes successfully.
        "key": "expected value post-execution"
      }}
    }}
  ]
}}"""


def _mock_resolution(fallout_input: FalloutInput) -> FalloutResolution:
    """
    Return a realistic-looking hardcoded resolution without calling the Anthropic API.
    Enabled by setting CLAUDE_MOCK=true in the environment.

    The mock always proposes one concrete action so the full HITL pipeline fires:
      MongoDB (PENDING) → Operator Portal → Executor → Relay → Oracle Adapter → SQLite
    """
    error_code  = fallout_input.error_code or "UNKNOWN"
    customer_id = fallout_input.customer_id if fallout_input.customer_id is not None else "N/A"
    order_id    = fallout_input.order_id or 99999

    return FalloutResolution(
        classification={
            "domain":      fallout_input.domain or "PROVISIONING",
            "issue_type":  "SubscriptionActivationFailure",
            "severity":    fallout_input.severity or "HIGH",
        },
        root_cause=(
            f"[MOCK] Subscriber {customer_id} has a TERMINATED data subscription that was "
            f"not re-activated after plan upgrade. The HLR shows the MSISDN is registered "
            f"but the EPC bearer is missing, causing {error_code}. "
            "Root cause: provisioning state machine left the subscription in a partial "
            "ACTIVE/TERMINATED split across OCS and HSS."
        ),
        recommended_actions=[
            f"Re-trigger provisioning workflow for customer {customer_id} / order {order_id}",
            "Sync OCS and HSS subscription state via PROV_RESYNC command",
            "Issue compensatory data credit of 5 GB for service disruption",
        ],
        runbook_references=["provisioning_failure.txt", "order_fallout.txt"],
        confidence="HIGH",
        raw_reasoning="[MOCK MODE — no Claude API call was made]",
        action_proposals=[
            {
                # "retry_provisioning" matches the executor handler registry AND
                # _outbox_params() so the full path fires:
                #   Executor -> MongoDB outbox -> Relay -> Kafka legacy.commands.writeback
                #   -> Oracle Adapter -> SQLite ORDER_FALLOUT update
                "action_type":        "retry_provisioning",
                "action_description": (
                    f"Re-trigger provisioning workflow for customer {customer_id} "
                    f"to resolve {error_code} activation failure"
                ),
                "action_payload": {
                    "customer_id": customer_id,
                    "order_id":    order_id,
                    "error_code":  error_code,
                    "command":     "PROV_RESYNC",
                    "target_system": "HLR",
                    # fallout_id is used by _outbox_params for the WHERE clause
                    "fallout_id":  f"FALLOUT-{order_id}",
                    "msisdn":      "unknown",
                    "system":      "HLR",
                },
                "predicted_before_state": {"subscription_status": "TERMINATED"},
                "predicted_after_state":  {"subscription_status": "ACTIVE"},
            }
        ],
        critique={"quality": "good", "issues": [], "lesson": None},
        routing_domain=fallout_input.domain or "PROVISIONING",
        routing_confidence=0.99,
        critic_violations=[],
        policy_violations=[],
        token_usage={"input_tokens": 0, "output_tokens": 0},
        manager_brief=(
            f"[MOCK] Customer {customer_id} experiencing activation failure ({error_code}); "
            "HLR shows MSISDN registered but EPC bearer missing — PROV_RESYNC recommended."
        ),
        chain_of_thought={
            "observation":      (
                f"Subscriber {customer_id} has a TERMINATED data subscription. "
                f"HLR shows the MSISDN is registered but the EPC bearer is missing. "
                f"Error code: {error_code}."
            ),
            "constraint_check": (
                f"Customer is in region {fallout_input.domain or 'DEFAULT'}. "
                "Regional policy permits a one-time grace reactivation for TERMINATED subscriptions "
                "within 30 days of the last successful payment."
            ),
            "strategy": (
                "Applying the 'Graceful Recovery' protocol (Runbook: provisioning_failure.txt). "
                "Re-trigger the provisioning workflow with a PROV_RESYNC command to re-align "
                "OCS and HSS subscription state."
            ),
            "recommendation": (
                f"Retry provisioning for customer {customer_id} / order {order_id} "
                "with the PROV_RESYNC override flag. "
                "Expected outcome: subscription_status transitions from TERMINATED → ACTIVE."
            ),
            "natural_language_summary": (
                f"Customer {customer_id} is experiencing a provisioning split-state caused by "
                f"error {error_code}: the MSISDN is registered on the HLR but the EPC bearer is "
                "absent, leaving the subscription in a TERMINATED/ACTIVE deadlock. "
                "Regional grace policy allows a one-time PROV_RESYNC to re-align OCS and HSS "
                "without requiring a new order, making this the lowest-risk resolution path. "
                "I am recommending an automated retry to restore service immediately while "
                "preserving the existing order record."
            ),
        },
    )


def run_agent(fallout_input: FalloutInput, correlation_id: str | None = None) -> FalloutResolution:
    """
    Main agent entry point. Returns a FalloutResolution.

    Phase 6 additions:
      - IntentRouter classifies intent → selects Expert tool subset + prompt addendum
      - InternalCritic reviews draft → triggers one revision pass on violations
      - PolicyEngine validates credit/discount actions against hard business rules

    Set CLAUDE_MOCK=true in the environment to skip all Anthropic API calls and
    return a realistic hardcoded resolution instead (useful for KT / demo runs).
    """
    if os.getenv("CLAUDE_MOCK", "false").lower() == "true":
        return _mock_resolution(fallout_input)

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Build the issue description for retrieval
    issue_desc = (
        f"Error: {fallout_input.error_code} — {fallout_input.error_message}. "
        f"Domain: {fallout_input.domain}. "
        f"{fallout_input.free_text}"
    ).strip()

    # 1. Route — classify intent to pick the right Expert
    routing = IntentRouter(client).classify(
        error_code=fallout_input.error_code,
        domain=fallout_input.domain,
        error_message=fallout_input.error_message,
        free_text=fallout_input.free_text,
    )
    expert_cfg = EXPERT_CONFIGS[routing.domain]
    active_tool_names = expert_cfg["tools"]  # None = all tools
    active_tools = (
        TOOL_DEFINITIONS
        if active_tool_names is None
        else [t for t in TOOL_DEFINITIONS if t["name"] in active_tool_names]
    )

    # 2. RAG — retrieve runbook context
    rag_chunks   = rag_retrieve(issue_desc, top_k=4)
    rag_context  = format_context(rag_chunks)
    runbook_refs = list({c["source"] for c in rag_chunks})

    # 3. Memory — retrieve past lessons
    memories       = retrieve_memories(issue_desc, top_k=3)
    memory_context = format_memories(memories)

    # 4. Build system prompt with expert addendum
    system = build_system_prompt(rag_context, memory_context, expert_cfg["system_addendum"])

    # 5. Build user message
    user_message = (
        f"Please investigate and resolve the following Telco fallout:\n\n"
        f"Error Code:    {fallout_input.error_code or 'Unknown'}\n"
        f"Error Message: {fallout_input.error_message or 'Not provided'}\n"
        f"Domain:        {fallout_input.domain or 'Unknown'}\n"
        f"Severity:      {fallout_input.severity}\n"
        f"Order ID:      {fallout_input.order_id or 'Not provided'}\n"
        f"Order Number:  {fallout_input.order_number or 'Not provided'}\n"
        f"Customer ID:   {fallout_input.customer_id or 'Not provided'}\n"
        f"Additional Context: {fallout_input.free_text or 'None'}\n\n"
        "Use the available tools to investigate, then provide your structured resolution."
    )
    messages = [{"role": "user", "content": user_message}]

    # 6. Agentic tool-use loop
    raw_reasoning_parts: list[str] = []
    last_response = None
    max_iterations = 10
    # Phase 13: accumulate token usage across all API calls for budget tracking
    _token_usage: dict = {"input_tokens": 0, "output_tokens": 0}
    # Track whether last_response has already been appended to messages.
    # If the loop exhausts max_iterations on a tool_use turn, the response is
    # already in the history; the critic section must not append it again or it
    # creates a dangling tool_use block with no following tool_result.
    _last_response_in_messages = False

    for _ in range(max_iterations):
        last_response = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=system,
            tools=active_tools,
            messages=messages,
        )
        if hasattr(last_response, "usage") and last_response.usage:
            _token_usage["input_tokens"]  += getattr(last_response.usage, "input_tokens",  0)
            _token_usage["output_tokens"] += getattr(last_response.usage, "output_tokens", 0)

        for block in last_response.content:
            if hasattr(block, "text"):
                raw_reasoning_parts.append(block.text)

        if last_response.stop_reason == "end_turn":
            _last_response_in_messages = False
            break

        if last_response.stop_reason == "tool_use":
            tool_results = []
            for block in last_response.content:
                if block.type == "tool_use":
                    result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     result,
                    })
            messages.append({"role": "assistant", "content": last_response.content})
            messages.append({"role": "user",      "content": tool_results})
            _last_response_in_messages = True
        else:
            _last_response_in_messages = False
            break

    raw_reasoning   = "\n".join(raw_reasoning_parts)
    resolution_data = _parse_resolution(raw_reasoning)

    # 7. Internal Critic — review draft before HITL submission
    critic = InternalCritic(client)
    critic_violations = critic.review(
        root_cause=resolution_data.get("root_cause", ""),
        recommended_actions=resolution_data.get("recommended_actions", []),
        confidence=resolution_data.get("confidence", "LOW"),
        reasoning_excerpt=raw_reasoning,
        issue_context=issue_desc,
    )

    if critic_violations and last_response is not None:
        # Only append the final assistant turn if it isn't already in messages.
        # (It's already there when the main loop exhausted max_iterations on a
        # tool_use turn; appending it again would create a dangling tool_use.)
        if not _last_response_in_messages:
            messages.append({"role": "assistant", "content": last_response.content})
        messages.append({"role": "user", "content": critic.format_feedback(critic_violations)})

        for _ in range(5):
            last_response = client.messages.create(
                model=MODEL,
                max_tokens=2048,
                system=system,
                tools=active_tools,
                messages=messages,
            )
            if hasattr(last_response, "usage") and last_response.usage:
                _token_usage["input_tokens"]  += getattr(last_response.usage, "input_tokens",  0)
                _token_usage["output_tokens"] += getattr(last_response.usage, "output_tokens", 0)

            for block in last_response.content:
                if hasattr(block, "text"):
                    raw_reasoning_parts.append(block.text)

            if last_response.stop_reason == "end_turn":
                break
            if last_response.stop_reason == "tool_use":
                tool_results = []
                for block in last_response.content:
                    if block.type == "tool_use":
                        result = execute_tool(block.name, block.input)
                        tool_results.append({
                            "type":        "tool_result",
                            "tool_use_id": block.id,
                            "content":     result,
                        })
                messages.append({"role": "assistant", "content": last_response.content})
                messages.append({"role": "user",      "content": tool_results})
            else:
                break

        raw_reasoning   = "\n".join(raw_reasoning_parts)
        resolution_data = _parse_resolution(raw_reasoning)

    # 8. Policy Engine — validate credit/discount actions against hard rules
    policy_violations: list[str] = []
    if fallout_input.customer_id:
        policy_context = _build_policy_context(fallout_input.customer_id)
        if policy_context:
            proposed = extract_actions_from_text(
                resolution_data.get("recommended_actions", [])
            )
            policy_violations = PolicyEngine().validate_all(proposed, policy_context)

    # 9. Self-critique and store lesson
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
        action_proposals=resolution_data.get("action_proposals", []),
        critique=critique,
        routing_domain=routing.domain.value,
        routing_confidence=routing.confidence,
        critic_violations=critic_violations,
        policy_violations=policy_violations,
        token_usage=_token_usage,
        chain_of_thought=_parse_cot(raw_reasoning),
        manager_brief=resolution_data.get("manager_brief"),
    )


def _build_policy_context(customer_id: int) -> PolicyContext | None:
    """Fetch the minimal customer+billing data needed for PolicyEngine validation."""
    from agent.db import query_one

    row = query_one(
        "SELECT c.ACCOUNT_TYPE, c.CREATED_DATE, c.CREDIT_SCORE, "
        "ba.CURRENT_BALANCE, ba.CREDIT_LIMIT "
        "FROM CUSTOMER c "
        "LEFT JOIN BILLING_ACCOUNT ba ON ba.CUSTOMER_ID = c.CUSTOMER_ID "
        "WHERE c.CUSTOMER_ID = :customer_id",
        {"customer_id": customer_id},
    )
    if not row:
        return None

    created = row.get("created_date")
    if isinstance(created, str):
        try:
            created = datetime.fromisoformat(created).date()
        except ValueError:
            created = date.today()
    elif isinstance(created, datetime):
        created = created.date()

    tenure_days = (date.today() - created).days if isinstance(created, date) else 0

    return PolicyContext(
        customer_tenure_days=tenure_days,
        account_type=str(row.get("account_type") or "POSTPAID"),
        current_balance=float(row.get("current_balance") or 0.0),
        credit_limit=float(row.get("credit_limit") or 0.0),
        credit_score=row.get("credit_score"),
    )


def _parse_resolution(text: str) -> dict:
    """Extract JSON from <resolution>...</resolution> tags in agent output."""
    import re
    match = re.search(r"<resolution>(.*?)</resolution>", text, re.DOTALL)
    if not match:
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


def _parse_cot(text: str) -> dict[str, str] | None:
    """Extract structured Chain-of-Thought from <cot>...</cot> tags."""
    import re
    match = re.search(r"<cot>(.*?)</cot>", text, re.DOTALL)
    if not match:
        return None
    raw = match.group(1).strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return {k: str(v) for k, v in data.items()}
    except json.JSONDecodeError:
        pass
    return None


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
