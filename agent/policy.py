"""
Policy Engine — hard-coded guardrails that intercept a proposed resolution
before it reaches the HITL queue.

These rules are NON-LLM and cannot be hallucinated away.  The engine validates
each ProposedAction extracted from the agent's recommended_actions list and
returns a (possibly empty) list of violation strings.

Rule inventory:
  TENURE_CREDIT_LIMIT    — caps credit value for new customers
  PROPORTIONALITY_CAP    — credit must not exceed 150 % of monthly bill
  BALANCE_FLOOR          — credit must not push balance past credit limit
  ENTERPRISE_ESCALATION  — large credits on enterprise accounts need approval
"""

import re
from dataclasses import dataclass


class PolicyViolationError(Exception):
    """Raised when a single ProposedAction violates a hard rule."""

    def __init__(self, rule: str, message: str) -> None:
        self.rule = rule
        self.message = message
        super().__init__(f"[{rule}] {message}")


@dataclass
class PolicyContext:
    """Runtime context fetched from the DB for the affected customer."""

    customer_tenure_days: int
    account_type: str           # PREPAID | POSTPAID | ENTERPRISE
    current_balance: float
    credit_limit: float
    credit_score: int | None = None


@dataclass
class ProposedAction:
    """A single normalised action extracted from recommended_actions strings."""

    action_type: str    # CREDIT_ADJUSTMENT | DISCOUNT | OTHER
    value: float        # dollar amount (or 0 if not applicable)
    description: str


_CREDIT_KEYWORDS = {"credit", "refund", "adjustment", "waive", "reimburse", "compensat"}
_DISCOUNT_KEYWORDS = {"discount", "promo", "offer", "deal"}


def extract_actions_from_text(recommended_actions: list[str]) -> list[ProposedAction]:
    """
    Parse free-text recommended_actions into typed ProposedAction objects.
    Dollar amounts are detected via regex; action type is inferred from keywords.
    """
    actions: list[ProposedAction] = []
    for text in recommended_actions:
        lower = text.lower()
        amounts = re.findall(r"\$\s*([\d,]+(?:\.\d{1,2})?)", text)
        value = float(amounts[0].replace(",", "")) if amounts else 0.0

        if any(k in lower for k in _CREDIT_KEYWORDS) and value > 0:
            action_type = "CREDIT_ADJUSTMENT"
        elif any(k in lower for k in _DISCOUNT_KEYWORDS):
            action_type = "DISCOUNT"
        else:
            action_type = "OTHER"

        actions.append(ProposedAction(action_type=action_type, value=value, description=text))
    return actions


class PolicyEngine:
    """
    Validates a list of ProposedActions against PolicyContext.
    Returns a (possibly empty) list of violation strings so the caller can
    decide how to surface them — either blocking or advisory.
    """

    def validate(self, action: ProposedAction, context: PolicyContext) -> None:
        """Raise PolicyViolationError if action breaks any hard rule."""
        self._tenure_credit_limit(action, context)
        self._proportionality_cap(action, context)
        self._balance_floor(action, context)
        self._enterprise_escalation(action, context)

    def validate_all(
        self, actions: list[ProposedAction], context: PolicyContext
    ) -> list[str]:
        """Return all violation strings; empty list means all actions are clean."""
        violations: list[str] = []
        for action in actions:
            try:
                self.validate(action, context)
            except PolicyViolationError as exc:
                violations.append(str(exc))
        return violations

    # ── Rules ──────────────────────────────────────────────────────────────────

    def _tenure_credit_limit(
        self, action: ProposedAction, context: PolicyContext
    ) -> None:
        if action.action_type != "CREDIT_ADJUSTMENT":
            return
        if context.customer_tenure_days < 180 and action.value > 50:
            raise PolicyViolationError(
                "TENURE_CREDIT_LIMIT",
                f"Credit ${action.value:.2f} exceeds the $50 ceiling for customers "
                f"with <6 months tenure ({context.customer_tenure_days} days).",
            )
        if context.customer_tenure_days < 365 and action.value > 100:
            raise PolicyViolationError(
                "TENURE_CREDIT_LIMIT",
                f"Credit ${action.value:.2f} exceeds the $100 ceiling for customers "
                f"with <12 months tenure ({context.customer_tenure_days} days).",
            )

    def _proportionality_cap(
        self, action: ProposedAction, context: PolicyContext
    ) -> None:
        if action.action_type != "CREDIT_ADJUSTMENT":
            return
        monthly_est = context.credit_limit / 12 if context.credit_limit > 0 else 0
        if monthly_est > 0 and action.value > monthly_est * 1.5:
            raise PolicyViolationError(
                "PROPORTIONALITY_CAP",
                f"Credit ${action.value:.2f} exceeds 150% of estimated monthly bill "
                f"(${monthly_est:.2f}). Verify the actual overcharge amount.",
            )

    def _balance_floor(self, action: ProposedAction, context: PolicyContext) -> None:
        if action.action_type != "CREDIT_ADJUSTMENT":
            return
        resulting = context.current_balance - action.value
        if context.credit_limit > 0 and resulting < -context.credit_limit:
            raise PolicyViolationError(
                "BALANCE_FLOOR",
                f"Adjustment would push balance to ${resulting:.2f}, "
                f"exceeding credit limit of ${context.credit_limit:.2f}.",
            )

    def _enterprise_escalation(
        self, action: ProposedAction, context: PolicyContext
    ) -> None:
        if context.account_type.upper() != "ENTERPRISE":
            return
        if action.action_type == "CREDIT_ADJUSTMENT" and action.value > 500:
            raise PolicyViolationError(
                "ENTERPRISE_ESCALATION",
                f"Credits >$500 on ENTERPRISE accounts require senior approval. "
                f"Proposed: ${action.value:.2f}.",
            )
