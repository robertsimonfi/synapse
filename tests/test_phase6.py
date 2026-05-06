"""
Unit tests for Phase 6 components: router, policy, and critic.
No external API calls or DB connections required.
"""

import pytest
from unittest.mock import MagicMock, patch

# ── Router ─────────────────────────────────────────────────────────────────────

from agent.router import IntentRouter, ExpertDomain, EXPERT_CONFIGS, RoutingDecision


def _make_router(response_json: str) -> IntentRouter:
    client = MagicMock()
    msg = MagicMock()
    msg.content = [MagicMock(text=response_json)]
    client.messages.create.return_value = msg
    return IntentRouter(client)


def test_router_billing():
    router = _make_router('{"domain": "BILLING", "confidence": 0.9, "reasoning": "invoice error"}')
    decision = router.classify("BILL_003", "BILLING", "Invoice mismatch", "")
    assert decision.domain == ExpertDomain.BILLING
    assert decision.confidence == pytest.approx(0.9)


def test_router_provisioning():
    router = _make_router('{"domain": "PROVISIONING", "confidence": 0.85, "reasoning": "HLR fail"}')
    decision = router.classify("PROV_001", "OSS", "HLR timeout", "")
    assert decision.domain == ExpertDomain.PROVISIONING


def test_router_retention():
    router = _make_router('{"domain": "RETENTION", "confidence": 0.7, "reasoning": "churn signal"}')
    decision = router.classify("", "", "Customer wants to cancel", "unhappy with service")
    assert decision.domain == ExpertDomain.RETENTION


def test_router_fallback_on_bad_json():
    router = _make_router("not valid json at all")
    decision = router.classify("", "", "", "")
    assert decision.domain == ExpertDomain.GENERAL
    assert decision.confidence == 0.0


def test_expert_configs_tool_subsets():
    billing_tools = EXPERT_CONFIGS[ExpertDomain.BILLING]["tools"]
    assert "get_billing_account" in billing_tools
    assert "get_order_fallouts" not in billing_tools

    prov_tools = EXPERT_CONFIGS[ExpertDomain.PROVISIONING]["tools"]
    assert "get_provisioning_logs" in prov_tools
    assert "get_usage_trends" not in prov_tools

    assert EXPERT_CONFIGS[ExpertDomain.GENERAL]["tools"] is None


# ── Policy Engine ──────────────────────────────────────────────────────────────

from agent.policy import (
    PolicyEngine, PolicyContext, ProposedAction,
    PolicyViolationError, extract_actions_from_text,
)

STANDARD_CTX = PolicyContext(
    customer_tenure_days=400,
    account_type="POSTPAID",
    current_balance=50.0,
    credit_limit=500.0,
)


def test_policy_clean_credit():
    engine = PolicyEngine()
    action = ProposedAction("CREDIT_ADJUSTMENT", 30.0, "Apply $30 credit")
    engine.validate(action, STANDARD_CTX)  # should not raise


def test_policy_tenure_violation_6m():
    engine = PolicyEngine()
    ctx = PolicyContext(customer_tenure_days=90, account_type="POSTPAID",
                        current_balance=0.0, credit_limit=500.0)
    action = ProposedAction("CREDIT_ADJUSTMENT", 75.0, "Apply $75 credit")
    with pytest.raises(PolicyViolationError) as exc_info:
        engine.validate(action, ctx)
    assert "TENURE_CREDIT_LIMIT" in str(exc_info.value)


def test_policy_tenure_violation_12m():
    engine = PolicyEngine()
    ctx = PolicyContext(customer_tenure_days=200, account_type="POSTPAID",
                        current_balance=0.0, credit_limit=500.0)
    action = ProposedAction("CREDIT_ADJUSTMENT", 150.0, "Apply $150 credit")
    with pytest.raises(PolicyViolationError) as exc_info:
        engine.validate(action, ctx)
    assert "TENURE_CREDIT_LIMIT" in str(exc_info.value)


def test_policy_proportionality_cap():
    engine = PolicyEngine()
    # credit_limit=120, monthly_est=10, cap=15
    ctx = PolicyContext(customer_tenure_days=400, account_type="POSTPAID",
                        current_balance=0.0, credit_limit=120.0)
    action = ProposedAction("CREDIT_ADJUSTMENT", 20.0, "Apply $20 credit")
    with pytest.raises(PolicyViolationError) as exc_info:
        engine.validate(action, ctx)
    assert "PROPORTIONALITY_CAP" in str(exc_info.value)


def test_policy_balance_floor():
    engine = PolicyEngine()
    # current_balance=-750 (customer already holds a large credit), credit_limit=800
    # monthly_est=800/12≈66.67, 150% cap=100 — value=80 is under the proportionality cap
    # but -750 - 80 = -830 < -800, so BALANCE_FLOOR fires
    ctx = PolicyContext(customer_tenure_days=400, account_type="POSTPAID",
                        current_balance=-750.0, credit_limit=800.0)
    action = ProposedAction("CREDIT_ADJUSTMENT", 80.0, "Apply $80 credit")
    with pytest.raises(PolicyViolationError) as exc_info:
        engine.validate(action, ctx)
    assert "BALANCE_FLOOR" in str(exc_info.value)


def test_policy_enterprise_escalation():
    engine = PolicyEngine()
    ctx = PolicyContext(customer_tenure_days=1000, account_type="ENTERPRISE",
                        current_balance=0.0, credit_limit=10000.0)
    action = ProposedAction("CREDIT_ADJUSTMENT", 600.0, "Apply $600 credit")
    with pytest.raises(PolicyViolationError) as exc_info:
        engine.validate(action, ctx)
    assert "ENTERPRISE_ESCALATION" in str(exc_info.value)


def test_policy_validate_all_returns_violations():
    engine = PolicyEngine()
    ctx = PolicyContext(customer_tenure_days=90, account_type="POSTPAID",
                        current_balance=0.0, credit_limit=500.0)
    actions = [
        ProposedAction("CREDIT_ADJUSTMENT", 75.0, "Apply $75 credit"),
        ProposedAction("OTHER", 0.0, "Schedule technician visit"),
    ]
    violations = engine.validate_all(actions, ctx)
    assert len(violations) == 1
    assert "TENURE_CREDIT_LIMIT" in violations[0]


def test_extract_actions_from_text():
    texts = [
        "Issue a $45.00 credit to the customer's account",
        "Offer a 20% discount on next month's bill",
        "Reset the HLR provisioning flag",
    ]
    actions = extract_actions_from_text(texts)
    assert actions[0].action_type == "CREDIT_ADJUSTMENT"
    assert actions[0].value == pytest.approx(45.0)
    assert actions[1].action_type == "DISCOUNT"
    assert actions[2].action_type == "OTHER"
    assert actions[2].value == pytest.approx(0.0)


# ── Internal Critic ────────────────────────────────────────────────────────────

from agent.critic import InternalCritic


def _make_critic(response_json: str) -> InternalCritic:
    client = MagicMock()
    msg = MagicMock()
    msg.content = [MagicMock(text=response_json)]
    client.messages.create.return_value = msg
    return InternalCritic(client)


def test_critic_approved():
    critic = _make_critic('{"violations": [], "verdict": "APPROVED", "summary": "Looks good"}')
    violations = critic.review(
        root_cause="HLR timeout caused by network alarm.",
        recommended_actions=["Reset HLR flag — supported by provisioning log PROV-101"],
        confidence="HIGH",
        reasoning_excerpt="Tool results confirm HLR failure code 504.",
        issue_context="PROV_001 on order 42",
    )
    assert violations == []


def test_critic_violations():
    critic = _make_critic(
        '{"violations": ["Credit $60 exceeds verified overcharge of $40 (PROPORTIONALITY)"], '
        '"verdict": "NEEDS_REVISION", "summary": "Overcredit"}'
    )
    violations = critic.review(
        root_cause="Customer was overcharged $40.",
        recommended_actions=["Issue a $60 credit"],
        confidence="HIGH",
        reasoning_excerpt="Invoice shows $40 overcharge.",
        issue_context="BILL_003 on customer 7",
    )
    assert len(violations) == 1
    assert "PROPORTIONALITY" in violations[0]


def test_critic_bad_json_returns_empty():
    critic = _make_critic("I think the resolution looks fine actually")
    violations = critic.review("root", ["action"], "LOW", "excerpt", "ctx")
    assert violations == []


def test_critic_format_feedback_contains_violations():
    critic = _make_critic("{}")
    feedback = critic.format_feedback(["Violation A", "Violation B"])
    assert "Violation A" in feedback
    assert "Violation B" in feedback
    assert "<resolution>" in feedback
