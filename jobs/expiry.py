"""
Auto-expiry job: transitions stale PENDING actions to EXPIRED.

Runs every EXPIRY_CHECK_INTERVAL_MINUTES (default 15 min) via APScheduler.
Any action that has been PENDING for longer than ACTION_EXPIRY_HOURS (default 4h)
is expired automatically, and a Slack alert is posted if SLACK_WEBHOOK_URL is set.

Telco retention offers have a shelf-life — an offer that sits in a queue for
4+ hours is worthless because the customer may have already churned.
"""

import logging
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db.mongo import expire_stale_actions
from observability.metrics import actions_expired_total

logger = logging.getLogger(__name__)

EXPIRY_HOURS           = int(os.getenv("ACTION_EXPIRY_HOURS", "4"))
CHECK_INTERVAL_MINUTES = int(os.getenv("EXPIRY_CHECK_INTERVAL_MINUTES", "15"))


async def _expiry_job() -> None:
    try:
        expired_ids = await expire_stale_actions(max_age_hours=EXPIRY_HOURS)
        if expired_ids:
            actions_expired_total.inc(len(expired_ids))
            logger.warning(
                "Auto-expiry: %d stale action(s) expired after %dh: %s",
                len(expired_ids), EXPIRY_HOURS, expired_ids,
            )
            await _notify_slack(expired_ids)
        else:
            logger.debug("Auto-expiry check: no stale actions found")
    except Exception:
        logger.exception("Error in auto-expiry job")


async def _notify_slack(expired_ids: list[str]) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    sample = expired_ids[:10]
    overflow = len(expired_ids) - len(sample)
    id_list = "`, `".join(sample) + (f"` (+{overflow} more)" if overflow else "`")

    text = (
        f":hourglass_flowing_sand: *Synapse Auto-Expiry Alert*\n"
        f"{len(expired_ids)} PENDING action(s) exceeded the *{EXPIRY_HOURS}h* review "
        f"window and were automatically expired.\n"
        f"Action IDs: `{id_list}\n"
        f"These customers may still require manual outreach."
    )

    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            resp = await session.post(webhook_url, json={"text": text}, timeout=aiohttp.ClientTimeout(total=5))
            if resp.status != 200:
                logger.warning("Slack webhook returned HTTP %d", resp.status)
    except Exception:
        logger.exception("Failed to send Slack notification")


def setup_expiry_scheduler() -> AsyncIOScheduler:
    """Create and return a configured APScheduler (not yet started)."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _expiry_job,
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        id="action_auto_expiry",
        name=f"Action auto-expiry (>{EXPIRY_HOURS}h PENDING)",
        replace_existing=True,
    )
    logger.info(
        "Expiry scheduler configured: check every %d min, expire after %dh",
        CHECK_INTERVAL_MINUTES, EXPIRY_HOURS,
    )
    return scheduler
