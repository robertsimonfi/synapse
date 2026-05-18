"""
Intent Router — lightweight classifier that dispatches a FalloutInput to the
right Expert domain before the main agent loop runs.

The router calls Claude Haiku (fast + cheap) and returns a RoutingDecision
that the main agent uses to:
  - Filter the active tool set to the expert's relevant subset
  - Inject a domain-specific system-prompt addendum

Compound-sentence handling: if the free text contains signals from multiple
domains the Hard Overwrite Hierarchy is applied both in the prompt and by a
Python-level post-processor:

    BILLING > PROVISIONING > RETENTION > GENERAL
"""

import json
import os
from dataclasses import dataclass
from enum import Enum

import anthropic


class ExpertDomain(str, Enum):
    BILLING = "BILLING"
    PROVISIONING = "PROVISIONING"
    RETENTION = "RETENTION"
    GENERAL = "GENERAL"


@dataclass
class RoutingDecision:
    domain: ExpertDomain
    confidence: float
    reasoning: str


# Hard Overwrite Hierarchy — highest priority first.
_DOMAIN_PRIORITY: list[ExpertDomain] = [
    ExpertDomain.BILLING,
    ExpertDomain.PROVISIONING,
    ExpertDomain.RETENTION,
    ExpertDomain.GENERAL,
]

# Keyword signals used by the Python-level hierarchy enforcer.
# Intentionally broad substring matches (no word boundaries needed — we lower-case
# the whole text before checking).
_DOMAIN_SIGNALS: dict[ExpertDomain, list[str]] = {
    ExpertDomain.BILLING: [
        "overcharg", "bill", "invoice", "refund", "credit", "payment",
        "charg", "fee", "cost", "£", "$", "€", "money", "debit",
        "overdue", "balance", "receipt", "transaction",
    ],
    ExpertDomain.PROVISIONING: [
        "connect", "network", "signal", "activat", "provision",
        "sim", "internet", "data", "coverage", "outage", "hlr",
        "ocs", "epc", "ims", "service down", "no service",
    ],
    ExpertDomain.RETENTION: [
        "cancel", "leave", "quit", "churn", "discount", "loyal",
        "better deal", "competitor", "switch provider",
    ],
}


# Per-expert configuration: which tools and what system-prompt addendum.
# tools=None means the expert gets access to all tools (GENERAL fallback).
EXPERT_CONFIGS: dict[ExpertDomain, dict] = {
    ExpertDomain.BILLING: {
        "tools": {
            "get_billing_account",
            "get_recent_invoices",
            "get_customer_info",
            "get_customer_subscriptions",
            "get_incident_tickets",
        },
        "system_addendum": (
            "\n## Expert Mode: Billing Specialist\n"
            "You are a Billing Expert. Prioritise ledger accuracy, credit adjustments, "
            "tax logic, payment plan options, and overdue account resolution. "
            "Always verify the exact overcharge amount before recommending a credit — "
            "the credit must equal the verified overcharge, not exceed it."
        ),
    },
    ExpertDomain.PROVISIONING: {
        "tools": {
            "get_order_details",
            "get_order_fallouts",
            "get_provisioning_logs",
            "get_customer_subscriptions",
            "get_active_network_alarms",
            "get_incident_tickets",
            "get_product_dependencies",
        },
        "system_addendum": (
            "\n## Expert Mode: Technical / Provisioning Specialist\n"
            "You are a Provisioning Expert. Focus on system integration failures, "
            "HLR/OCS/EPC/IMS provisioning logs, IMEI/SIM issues, and network resets. "
            "Always correlate provisioning failures with active network alarms before "
            "attributing the fault solely to a provisioning system."
        ),
    },
    ExpertDomain.RETENTION: {
        "tools": {
            "get_customer_info",
            "get_customer_subscriptions",
            "get_billing_account",
            "get_recent_invoices",
            "get_usage_trends",
            "get_incident_tickets",
        },
        "system_addendum": (
            "\n## Expert Mode: Retention / Commercial Specialist\n"
            "You are a Retention Expert. Focus on customer lifetime value, churn prevention, "
            "and commercial resolution. Identify the right discount or plan change to retain "
            "the customer, grounded in their actual usage and billing history."
        ),
    },
    ExpertDomain.GENERAL: {
        "tools": None,
        "system_addendum": "",
    },
}


class IntentRouter:
    """
    Classifies a FalloutInput into one of four ExpertDomains using a fast model.
    Adds no latency on cache hits; single Haiku call otherwise.
    """

    def __init__(self, client: anthropic.Anthropic):
        self._client = client
        self._model = os.getenv("CLAUDE_MODEL_ROUTER", "claude-haiku-4-5-20251001")

    def classify(
        self,
        error_code: str,
        domain: str,
        error_message: str,
        free_text: str,
    ) -> RoutingDecision:
        """Return a RoutingDecision for the given fallout context.

        For compound sentences the Hard Overwrite Hierarchy is applied both
        inside the LLM prompt and by _apply_domain_hierarchy() afterwards.
        """
        prompt = (
            "Classify this telco support request into exactly one domain.\n\n"
            f"Error Code: {error_code or 'N/A'}\n"
            f"Domain hint: {domain or 'N/A'}\n"
            f"Error Message: {error_message or 'N/A'}\n"
            f"Free Text: {free_text or 'N/A'}\n\n"
            "Respond with JSON only — no markdown fences:\n"
            '{"domain": "BILLING|PROVISIONING|RETENTION|GENERAL", '
            '"confidence": 0.0-1.0, "reasoning": "brief reason"}\n\n'
            "Classification rules:\n"
            "- BILLING: payment failures, invoice disputes, credit limits, charge adjustments, refunds\n"
            "- PROVISIONING: activation failures, SIM issues, HLR/OCS/EPC/IMS errors, network provisioning\n"
            "- RETENTION: churn signals, discount requests, plan changes, customer satisfaction complaints\n"
            "- GENERAL: clearly multi-domain or insufficient signal\n\n"
            "COMPOUND SENTENCE RULE — Hard Overwrite Hierarchy:\n"
            "If the message contains signals from multiple domains apply this strict priority:\n"
            "  BILLING > PROVISIONING > RETENTION > GENERAL\n"
            "Examples:\n"
            "  'Update my address but I was overcharged £60' → BILLING (financial dispute wins)\n"
            "  'Cancel my plan, my internet is also down'   → PROVISIONING (service failure > churn)\n"
            "  'I want a better deal, my bill is too high'  → BILLING (monetary dispute wins)\n"
            "Never return GENERAL if a more specific domain signal is present."
        )
        response = self._client.messages.create(
            model=self._model,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        decision = self._parse(response.content[0].text)
        # Second layer: Python-level hierarchy enforcement on raw text signals.
        return self._apply_domain_hierarchy(free_text or "", decision)

    def _apply_domain_hierarchy(
        self, text: str, decision: RoutingDecision
    ) -> RoutingDecision:
        """Enforce BILLING > PROVISIONING > RETENTION > GENERAL on raw text.

        Upgrades the domain when a higher-priority signal is detected in the
        free text, regardless of what the LLM returned.  Never downgrades.
        """
        text_lower = text.lower()
        current_rank = _DOMAIN_PRIORITY.index(decision.domain)

        for candidate in _DOMAIN_PRIORITY[:current_rank]:
            signals = _DOMAIN_SIGNALS.get(candidate, [])
            if any(sig in text_lower for sig in signals):
                return RoutingDecision(
                    domain=candidate,
                    confidence=decision.confidence,
                    reasoning=(
                        f"[HIERARCHY_OVERRIDE] {candidate.value} signals detected "
                        f"in compound sentence — overrides {decision.domain.value}. "
                        f"Original reasoning: {decision.reasoning}"
                    ),
                )

        # No higher-priority signals found — return unchanged.
        return decision

    def _parse(self, text: str) -> RoutingDecision:
        try:
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(text[start:end])
                domain = ExpertDomain(data.get("domain", "GENERAL"))
                return RoutingDecision(
                    domain=domain,
                    confidence=float(data.get("confidence", 0.5)),
                    reasoning=data.get("reasoning", ""),
                )
        except Exception:
            pass
        return RoutingDecision(
            domain=ExpertDomain.GENERAL,
            confidence=0.0,
            reasoning="classification failed — falling back to GENERAL",
        )
