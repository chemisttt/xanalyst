"""
Spec 004 Task 7: Daily cost + promise-resolution summary to System Health Mirror topic 6896.

Reads:
- Redis key `llm_cost:ct_digest:YYYY-MM` (cents accumulator)
- Redis key `llm_cost:ct_digest_feedback:YYYY-MM` (feedback parser cost separately)
- ct_promises aggregate за last 14d → resolution rate

Runs daily в 23:55 MSK (cron `55 20 * * *` UTC).
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.redis_client import init_redis, close_redis, get_redis
from shared import notifier

log = logging.getLogger("ct_digest.cost_summary")

SYSTEM_HEALTH_MIRROR_TOPIC = 6896  # из CLAUDE.md
RESOLUTION_ALERT_THRESHOLD = 0.08  # <8% triggers ⚠️


async def _get_cost_cents(redis_key: str) -> float:
    r = get_redis()
    val = await r.get(redis_key)
    try:
        return float(val) if val else 0.0
    except (ValueError, TypeError):
        return 0.0


async def _get_resolution_stats(pool) -> dict:
    """
    Denominator excludes 'announced' and 'deferred_check' (per spec predicted_outcomes
    rationale — these haven't been polled yet).
    """
    row = await pool.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE status IN ('live','missed','unverifiable')) AS resolved,
            COUNT(*) FILTER (WHERE status = 'live')                            AS live_count,
            COUNT(*) FILTER (WHERE status = 'missed')                          AS missed_count,
            COUNT(*) FILTER (WHERE status = 'unverifiable')                    AS unverifiable_count,
            COUNT(*) FILTER (WHERE status IN ('announced','deferred_check'))   AS pending,
            COUNT(*)                                                            AS total
        FROM ct_promises
        WHERE created_at > NOW() - INTERVAL '14 days'
        """
    )
    if not row or row["resolved"] == 0:
        return {
            "resolved": 0, "live": 0, "missed": 0, "unverifiable": 0,
            "pending": (row["pending"] if row else 0),
            "total": (row["total"] if row else 0),
            "rate": 0.0,
        }
    return {
        "resolved": row["resolved"],
        "live": row["live_count"],
        "missed": row["missed_count"],
        "unverifiable": row["unverifiable_count"],
        "pending": row["pending"],
        "total": row["total"],
        "rate": (row["live_count"] + row["missed_count"]) / row["resolved"],
    }


def _project_monthly(today_cents: float, day_of_month: int, days_in_month: int) -> float:
    if day_of_month == 0:
        return 0.0
    return today_cents * days_in_month / day_of_month


async def build_summary() -> str:
    """Compose summary string + return."""
    now = datetime.utcnow()
    ym = now.strftime("%Y-%m")
    digest_cost = await _get_cost_cents(f"llm_cost:ct_digest:{ym}")
    feedback_cost = await _get_cost_cents(f"llm_cost:ct_digest_feedback:{ym}")
    total_cents = digest_cost + feedback_cost
    total_dollars = total_cents / 100.0

    import calendar
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    projected_monthly = _project_monthly(total_cents, now.day, days_in_month) / 100.0

    # 2026-06-09: promise tracker retired (verify +19d FAIL — 5.1% resolution, 167/176
    # unverifiable; not fixing in V1). promise_cron cron disabled, so resolution stats
    # больше не обновляются — убрали блок из daily summary, оставили cost-отчёт.
    text = (
        f"📊 <b>CT Digest cost summary</b> · {now.strftime('%Y-%m-%d')}\n\n"
        f"💰 MTD spend: <b>${total_dollars:.2f}</b> "
        f"(digest ${digest_cost/100:.2f} + feedback ${feedback_cost/100:.2f})\n"
        f"📈 Projected month: <b>${projected_monthly:.2f}</b>"
    )
    return text


async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    await init_db()
    await init_redis()
    try:
        summary = await build_summary()
        log.info("summary:\n%s", summary)
        result = await asyncio.to_thread(
            notifier.send_to_topic, summary, SYSTEM_HEALTH_MIRROR_TOPIC,
            "HTML", True, None, None,
        )
        if result:
            log.info("posted to topic %s (msg_id=%s)", SYSTEM_HEALTH_MIRROR_TOPIC, result)
        else:
            log.warning("send_to_topic returned None")
    finally:
        await close_redis()
        await close_db()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
