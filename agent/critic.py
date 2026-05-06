"""
Internal Critic — a fast pre-submission self-correction step.

After the main agent loop produces a resolution, the Critic (Claude Haiku)
reviews it against the BEST_PRACTICES document.  If violations are found,
the caller can inject the feedback back into the conversation and ask for
one revision pass before the ticket ever reaches the HITL queue.

Design intent:
  Draft  → "I recommend a $60 credit because the customer was overcharged."
  Critic → "The overcharge was only $40. A $60 credit violates PROPORTIONALITY."
  Final  → "I recommend a $40 credit and a formal apology."
"""

import json
import os

import anthropic


BEST_PRACTICES = """\
TELCO AGENT BEST PRACTICES
===========================
1. PROPORTIONALITY: Credit/refund amounts must match the verified overcharge or service
   failure value — never exceed it.
2. EVIDENCE: Every recommended action must cite at least one specific data point from
   the investigation tools (IDs, amounts, timestamps).
3. ROOT CAUSE FIRST: Never recommend a workaround without first identifying the root cause.
4. ESCALATION PATHS: P1/CRITICAL and P2/HIGH severity incidents must always include
   an escalation path in the recommended actions.
5. BILLING ARITHMETIC: Double-check dollar amounts — credit must equal the verified
   overcharge, not the customer's requested amount.
6. NETWORK CORRELATION: Check active network alarms before attributing a failure solely
   to a provisioning system.
7. NO SPECULATION: If data is insufficient, say so explicitly. Never invent facts not
   supported by tool results.
8. CUSTOMER TENURE: Long-tenured customers deserve priority consideration for goodwill
   credits and expedited handling.
"""


class InternalCritic:
    """
    Reviews a proposed resolution against BEST_PRACTICES using a fast model.

    Usage:
        critic = InternalCritic(client)
        violations = critic.review(root_cause, actions, confidence, reasoning, issue_ctx)
        if violations:
            # inject feedback into conversation and run one revision pass
    """

    def __init__(self, client: anthropic.Anthropic) -> None:
        self._client = client
        self._model = os.getenv("CLAUDE_MODEL_CRITIC", "claude-haiku-4-5-20251001")

    def review(
        self,
        root_cause: str,
        recommended_actions: list[str],
        confidence: str,
        reasoning_excerpt: str,
        issue_context: str,
    ) -> list[str]:
        """
        Returns a list of violation descriptions.
        An empty list means the resolution passed all best-practice checks.
        """
        prompt = self._build_prompt(
            root_cause, recommended_actions, confidence, reasoning_excerpt, issue_context
        )
        response = self._client.messages.create(
            model=self._model,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        return self._parse(response.content[0].text)

    def format_feedback(self, violations: list[str]) -> str:
        """Format violations as a user message for the revision pass."""
        lines = "\n".join(f"- {v}" for v in violations)
        return (
            "Your resolution was reviewed by an internal quality checker and the following "
            "issues were identified:\n\n"
            f"{lines}\n\n"
            "Please revise your resolution to address these issues. "
            "Provide an updated <resolution>...</resolution> block with corrected "
            "root cause, recommended actions, and confidence."
        )

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _build_prompt(
        self,
        root_cause: str,
        recommended_actions: list[str],
        confidence: str,
        reasoning_excerpt: str,
        issue_context: str,
    ) -> str:
        actions_text = "\n".join(f"  - {a}" for a in recommended_actions)
        excerpt = (reasoning_excerpt or "N/A")[:800]
        return (
            "You are an internal quality reviewer for a telco AI agent.\n\n"
            f"BEST PRACTICES:\n{BEST_PRACTICES}\n"
            f"ISSUE CONTEXT:\n{issue_context}\n\n"
            "PROPOSED RESOLUTION:\n"
            f"Root Cause: {root_cause}\n"
            f"Confidence: {confidence}\n"
            f"Recommended Actions:\n{actions_text}\n"
            f"Reasoning Excerpt: {excerpt}\n\n"
            "Review this resolution against the best practices above.\n"
            "Respond with JSON only — no markdown fences:\n"
            '{"violations": ["specific violation with detail", ...], '
            '"verdict": "APPROVED|NEEDS_REVISION", "summary": "one sentence"}\n\n'
            "Only flag genuine violations with specific detail — not stylistic preferences. "
            "If the resolution is sound, return an empty violations array."
        )

    def _parse(self, text: str) -> list[str]:
        try:
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(text[start:end])
                return data.get("violations", [])
        except Exception:
            pass
        return []
