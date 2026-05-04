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

MODEL = os.getenv("CLAUDE_MODEL", "claude-opus-4-6")


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
