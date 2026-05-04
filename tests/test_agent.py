"""
Basic smoke tests for the Telco Fallout Resolution Agent.
Run with: pytest tests/
"""

import pytest
from unittest.mock import patch, MagicMock
from agent.agent import FalloutInput, _parse_resolution


def test_parse_resolution_with_tags():
    text = """
Some reasoning here.
<resolution>
{
  "classification": {"domain": "PROVISIONING", "issue_type": "HLR Timeout", "severity": "HIGH"},
  "root_cause": "HLR is unreachable due to network alarm.",
  "recommended_actions": ["Check NOC dashboard", "Retry after HLR recovery"],
  "runbook_references": ["provisioning_failure.txt"],
  "confidence": "HIGH"
}
</resolution>
"""
    result = _parse_resolution(text)
    assert result["classification"]["domain"] == "PROVISIONING"
    assert result["confidence"] == "HIGH"
    assert len(result["recommended_actions"]) == 2


def test_parse_resolution_no_tags_fallback():
    text = """
{
  "classification": {"domain": "BILLING", "issue_type": "Credit Exceeded", "severity": "MEDIUM"},
  "root_cause": "Customer over credit limit.",
  "recommended_actions": ["Review billing account"],
  "runbook_references": ["billing_issues.txt"],
  "confidence": "MEDIUM"
}
"""
    result = _parse_resolution(text)
    assert result["classification"]["domain"] == "BILLING"


def test_parse_resolution_empty():
    result = _parse_resolution("No JSON here at all.")
    assert result == {}


def test_fallout_input_defaults():
    fi = FalloutInput()
    assert fi.severity == "MEDIUM"
    assert fi.error_code == ""
    assert fi.domain == ""


@pytest.mark.parametrize("error_code,domain", [
    ("PROV_001", "PROVISIONING"),
    ("BILL_003", "BILLING"),
    ("NET_004",  "NETWORK"),
    ("BSS_007",  "BSS"),
])
def test_fallout_input_fields(error_code, domain):
    fi = FalloutInput(error_code=error_code, domain=domain, severity="HIGH")
    assert fi.error_code == error_code
    assert fi.domain == domain
