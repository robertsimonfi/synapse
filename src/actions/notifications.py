"""
Phase 7 — Notification Provider Layer & Engagement Dispatcher.

Architecture:
  NotificationProvider (ABC)
    ├── MockProvider        — logs only; default for local/test environments
    ├── TwilioProvider      — SMS/WhatsApp via Twilio REST API (stub)
    └── SendGridProvider    — Email via SendGrid API (stub)

  EngagementDispatcher.dispatch(action, result)
    • Resolves customer contact details from SQLite (by customer_id)
    • Selects channel & template based on action_type
    • Injects action context (root_cause, description) into the message
    • Masks PII before writing to MongoDB — raw contact only lives in-memory
    • Records an EngagementLog to MongoDB; failure is soft (logged, not raised)

Switch provider via env var:
  NOTIFICATION_PROVIDER=mock | twilio | sendgrid  (default: mock)

Twilio env vars:  TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER
SendGrid env var: SENDGRID_API_KEY, SENDGRID_FROM_EMAIL
"""

from __future__ import annotations

import abc
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from models.action import ActionProposal
from models.engagement import EngagementChannel, EngagementLog, EngagementStatus

logger = logging.getLogger(__name__)

_SQLITE_PATH = os.getenv("SQLITE_PATH", "telco_dev.db")


# ── Per-action-type notification templates ─────────────────────────────────────

_TEMPLATES: dict[str, dict[str, str]] = {
    "retry_provisioning": {
        "SMS":   "Hi {first_name}, great news — we've resolved a provisioning issue on your account. Your service is now fully active. — Synapse",
        "EMAIL": "Hi {first_name},\n\nWe wanted to let you know that a provisioning issue affecting your account has been resolved. Your service is now active.\n\nSynapse Support Team",
    },
    "update_billing": {
        "SMS":   "Hi {first_name}, we've adjusted your bill and applied the relevant credit. No action needed from you. — Synapse",
        "EMAIL": "Hi {first_name},\n\nA billing adjustment has been applied to your account. You can review the details in your next statement.\n\nSynapse Billing Team",
    },
    "create_ticket": {
        "SMS":   "Hi {first_name}, we've raised an incident ticket for your service issue. Our team is actively working on it. — Synapse",
        "EMAIL": "Hi {first_name},\n\nAn incident ticket has been opened for the issue affecting your account. You will be notified when it is resolved.\n\nSynapse Support Team",
    },
    "escalate": {
        "SMS":   "Hi {first_name}, your issue has been escalated to our specialist team for urgent review. We'll be in touch soon. — Synapse",
        "EMAIL": "Hi {first_name},\n\nYour case has been escalated to our specialist team. Expect a follow-up within 24 hours.\n\nSynapse Support Team",
    },
    "notify_customer": {
        "SMS":   "Hi {first_name}, a service update: {action_description} — Synapse",
        "EMAIL": "Hi {first_name},\n\nService update: {action_description}\n\nSynapse Support Team",
    },
    "update_subscription": {
        "SMS":   "Hi {first_name}, your subscription has been updated as requested. — Synapse",
        "EMAIL": "Hi {first_name},\n\nYour subscription has been successfully updated. Changes are effective immediately.\n\nSynapse Team",
    },
    "cancel_order": {
        "SMS":   "Hi {first_name}, your order has been cancelled as requested. Any charges will be refunded within 3-5 days. — Synapse",
        "EMAIL": "Hi {first_name},\n\nYour order has been cancelled. Applicable refunds will be processed within 3-5 business days.\n\nSynapse Team",
    },
    "_default": {
        "SMS":   "Hi {first_name}, we've taken action on your account: {action_description} — Synapse",
        "EMAIL": "Hi {first_name},\n\nAn action has been taken on your account: {action_description}\n\nSynapse Support Team",
    },
}

# Action types that prefer email over SMS (less urgent, informational)
_EMAIL_PREFERRED = {"update_billing", "update_subscription", "cancel_order"}


# ── PII masking helpers ────────────────────────────────────────────────────────

def _mask_phone(phone: str) -> str:
    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) >= 4:
        return f"+***-***-{digits[-4:]}"
    return "***"


def _mask_email(email: str) -> str:
    if "@" in email:
        local, domain = email.split("@", 1)
        return f"{local[:1]}***@{domain}"
    return "***"


# ── Customer contact lookup ────────────────────────────────────────────────────

def _lookup_customer(customer_id: str) -> Optional[dict]:
    """Query SQLite for customer contact details. Returns None if unavailable."""
    if not customer_id or not os.path.exists(_SQLITE_PATH):
        return None
    try:
        conn = sqlite3.connect(_SQLITE_PATH)
        row = conn.execute(
            "SELECT FIRST_NAME, LAST_NAME, EMAIL, PHONE FROM CUSTOMER WHERE CUSTOMER_ID = ?",
            (int(customer_id),),
        ).fetchone()
        conn.close()
        if row:
            return {"first_name": row[0], "last_name": row[1], "email": row[2], "phone": row[3]}
    except Exception as exc:
        logger.debug("SQLite customer lookup failed for %s: %s", customer_id, exc)
    return None


# ── Provider ABC ───────────────────────────────────────────────────────────────

class NotificationProvider(abc.ABC):
    @abc.abstractmethod
    async def send_sms(self, to: str, body: str) -> bool: ...

    @abc.abstractmethod
    async def send_email(self, to: str, subject: str, body: str) -> bool: ...


# ── Mock provider ──────────────────────────────────────────────────────────────

class MockProvider(NotificationProvider):
    async def send_sms(self, to: str, body: str) -> bool:
        logger.info("[MOCK-SMS] to=%s | %s", to, body[:120])
        return True

    async def send_email(self, to: str, subject: str, body: str) -> bool:
        logger.info("[MOCK-EMAIL] to=%s | subject=%s | %s", to, subject, body[:120])
        return True


# ── Twilio provider (stub — wire up credentials to activate) ──────────────────

class TwilioProvider(NotificationProvider):
    def __init__(self) -> None:
        self._sid    = os.environ["TWILIO_ACCOUNT_SID"]
        self._token  = os.environ["TWILIO_AUTH_TOKEN"]
        self._from   = os.environ["TWILIO_FROM_NUMBER"]

    async def send_sms(self, to: str, body: str) -> bool:
        import aiohttp, base64
        auth = base64.b64encode(f"{self._sid}:{self._token}".encode()).decode()
        url  = f"https://api.twilio.com/2010-04-01/Accounts/{self._sid}/Messages.json"
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                url,
                headers={"Authorization": f"Basic {auth}"},
                data={"To": to, "From": self._from, "Body": body},
            )
            ok = resp.status in (200, 201)
            if not ok:
                logger.error("Twilio SMS failed: %s %s", resp.status, await resp.text())
            return ok

    async def send_email(self, to: str, subject: str, body: str) -> bool:
        logger.warning("TwilioProvider.send_email not implemented — use SendGridProvider")
        return False


# ── SendGrid provider (stub — wire up API key to activate) ────────────────────

class SendGridProvider(NotificationProvider):
    def __init__(self) -> None:
        self._api_key   = os.environ["SENDGRID_API_KEY"]
        self._from_email = os.getenv("SENDGRID_FROM_EMAIL", "no-reply@synapse.io")

    async def send_sms(self, to: str, body: str) -> bool:
        logger.warning("SendGridProvider.send_sms not implemented — use TwilioProvider")
        return False

    async def send_email(self, to: str, subject: str, body: str) -> bool:
        import aiohttp
        payload = {
            "personalizations": [{"to": [{"email": to}]}],
            "from": {"email": self._from_email},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                "https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"},
                json=payload,
            )
            ok = resp.status == 202
            if not ok:
                logger.error("SendGrid email failed: %s %s", resp.status, await resp.text())
            return ok


# ── Provider factory ───────────────────────────────────────────────────────────

def _get_provider() -> NotificationProvider:
    name = os.getenv("NOTIFICATION_PROVIDER", "mock").lower()
    if name == "twilio":
        return TwilioProvider()
    if name == "sendgrid":
        return SendGridProvider()
    return MockProvider()


# ── Engagement Dispatcher ──────────────────────────────────────────────────────

class EngagementDispatcher:
    """
    Called by the Executor immediately after a successful CRM action.
    Resolves contact, selects channel & template, dispatches, and logs.
    """

    @staticmethod
    async def dispatch(action: ActionProposal, execution_result: dict) -> Optional[EngagementLog]:
        """
        Dispatch a post-action notification for the given action.
        Returns the EngagementLog entry, or None if dispatch is skipped/failed.
        """
        # Resolve customer context
        customer = _lookup_customer(action.customer_id) if action.customer_id else None
        first_name   = customer["first_name"] if customer else "Valued Customer"
        phone        = customer["phone"]       if customer else action.action_payload.get("msisdn", "")
        email        = customer["email"]       if customer else action.action_payload.get("customer_email", "")

        if not phone and not email:
            logger.debug(
                "Skipping engagement for action %s — no contact info available", action.id
            )
            return None

        # Select channel
        use_email = action.action_type in _EMAIL_PREFERRED and bool(email)
        channel   = EngagementChannel.EMAIL if use_email else EngagementChannel.SMS
        contact   = email if use_email else phone

        if not contact:
            logger.debug("Skipping engagement for action %s — preferred contact unavailable", action.id)
            return None

        # Resolve template
        template_key  = action.action_type if action.action_type in _TEMPLATES else "_default"
        template_str  = _TEMPLATES[template_key].get(channel.value, _TEMPLATES["_default"]["SMS"])
        message_body  = template_str.format(
            first_name=first_name,
            action_description=action.action_description[:120],
        )

        # Mask PII for storage
        recipient_masked = _mask_email(contact) if channel == EngagementChannel.EMAIL else _mask_phone(contact)

        log = EngagementLog(
            action_id=action.id,
            correlation_id=action.correlation_id,
            customer_id=action.customer_id,
            channel=channel,
            template_used=template_key,
            message_body=message_body,
            recipient_masked=recipient_masked,
        )

        # Dispatch — raw contact is only used here and immediately discarded
        try:
            provider = _get_provider()
            if channel == EngagementChannel.EMAIL:
                subject = f"Update on your Synapse account — {action.action_type.replace('_', ' ').title()}"
                ok = await provider.send_email(contact, subject, message_body)
            else:
                ok = await provider.send_sms(contact, message_body)

            log.status  = EngagementStatus.SENT if ok else EngagementStatus.FAILED
            log.sent_at = datetime.now(timezone.utc) if ok else None

        except Exception as exc:
            logger.error("Notification dispatch error for action %s: %s", action.id, exc)
            log.status = EngagementStatus.FAILED

        # Persist to MongoDB (non-blocking; import deferred to avoid circular deps at module load)
        try:
            from db.mongo import create_engagement
            await create_engagement(log)
        except Exception as exc:
            logger.warning("Failed to persist EngagementLog for action %s: %s", action.id, exc)

        logger.info(
            "ENGAGEMENT action=%s channel=%s status=%s masked_recipient=%s",
            action.id, channel.value, log.status.value, recipient_masked,
        )
        return log
