"""
Spec 004 Task 4: Promise tracker — 30-min cron entry.

Resolves ct_promises rows: announced→upcoming→[live|missed|unverifiable];
deferred_check→[live|missed|unverifiable] (post-fact, no pre-snapshot baseline).

CatchMint integration via async context manager (per ADR 0002 R-5).
Kill switch + startup assertion mirrored from ct_alpha_digest.py main (per R-6).

Run: python -m services.ct_digest.promise_cron
Systemd: deploy/systemd/xanalyst-ct-digest-promise-cron.service (oneshot)
Cron: */30 * * * *
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional

import asyncpg

from shared.catchmint_client import CatchmintClient, RateLimited
from shared.config import settings
from shared.db import init_db, close_db, get_pool

log = logging.getLogger("ct_digest.promise_cron")

NAME_SEARCH_WINDOW_SEC = 86400  # 24h catchmint /overview for collection name fallback


async def _snapshot_total_supply(
    client: CatchmintClient,
    address: str,
) -> Optional[int]:
    """
    Single snapshot of totalSupply via catchmint get_contract_detail.

    Returns int OR None if 404 / network failure / missing field.
    Catches RateLimited → propagate (caller does backoff).
    """
    try:
        data = await client.get_contract_detail(address)
        # NOTE: field is `totalSupply` (per catchmint_radar.py:94),
        # NOT `totalMinted` (spec rev-1 was wrong).
        ts = data.get("totalSupply")
        if ts is None:
            return None
        return int(ts)
    except RateLimited:
        raise
    except Exception as e:
        log.debug("snapshot failed for %s: %s", address, e)
        return None


async def _resolve_name_to_address(
    client: CatchmintClient,
    collection_name: str,
) -> Optional[str]:
    """Fallback: substring match collection_name в get_overview list, return that addr."""
    try:
        rows = await client.get_overview(window_sec=NAME_SEARCH_WINDOW_SEC)
    except Exception as e:
        log.warning("name resolve get_overview failed: %s", e)
        return None
    needle = collection_name.lower()
    for r in rows:
        name = (r.get("name") or "").lower()
        if needle in name:
            addr = r.get("address")
            if addr:
                log.info("name-search hit for %r → %s", collection_name, addr)
                return addr
    return None


async def _resolve_announced_to_upcoming(
    pool: asyncpg.Pool, client: CatchmintClient,
) -> int:
    """
    Path 1: announced → upcoming.
    Pre-snapshot totalSupply at T-30min..now-30min window relative to promised_ts.
    """
    rows = await pool.fetch(
        """
        SELECT id, item_id, contract_address, collection_name, promised_ts
        FROM ct_promises
        WHERE status = 'announced'
          AND NOW() >= promised_ts - INTERVAL '60 minutes'
          AND NOW() < promised_ts - INTERVAL '30 minutes' + INTERVAL '60 minutes'
        """
    )
    handled = 0
    for r in rows:
        addr = r["contract_address"]
        if not addr and r["collection_name"]:
            addr = await _resolve_name_to_address(client, r["collection_name"])
            if addr:
                # Update for future use
                await pool.execute(
                    "UPDATE ct_promises SET contract_address = $1 WHERE id = $2",
                    addr, r["id"],
                )
        if not addr:
            await pool.execute(
                "UPDATE ct_promises SET status='unverifiable', "
                "resolved_at=NOW(), ground_truth_source='no_address_no_match' "
                "WHERE id=$1", r["id"],
            )
            handled += 1
            continue

        pre = await _snapshot_total_supply(client, addr)
        if pre is None:
            await pool.execute(
                "UPDATE ct_promises SET status='unverifiable', "
                "resolved_at=NOW(), ground_truth_source='no_signal_pre' "
                "WHERE id=$1", r["id"],
            )
        else:
            await pool.execute(
                "UPDATE ct_promises SET status='upcoming', total_supply_pre=$1 "
                "WHERE id=$2", pre, r["id"],
            )
        handled += 1
    return handled


async def _resolve_upcoming(
    pool: asyncpg.Pool, client: CatchmintClient,
) -> int:
    """Path 2: upcoming → [live | missed | unverifiable] post-T+30min."""
    rows = await pool.fetch(
        """
        SELECT id, item_id, contract_address, total_supply_pre
        FROM ct_promises
        WHERE status = 'upcoming'
          AND NOW() > promised_ts + INTERVAL '30 minutes'
        """
    )
    handled = 0
    for r in rows:
        addr = r["contract_address"]
        if not addr:
            await pool.execute(
                "UPDATE ct_promises SET status='unverifiable', "
                "resolved_at=NOW(), ground_truth_source='no_address_resolution' "
                "WHERE id=$1", r["id"],
            )
            handled += 1
            continue

        post = await _snapshot_total_supply(client, addr)
        pre = r["total_supply_pre"] or 0
        if post is None:
            await pool.execute(
                "UPDATE ct_promises SET status='unverifiable', "
                "resolved_at=NOW(), ground_truth_source='catchmint_gone' "
                "WHERE id=$1", r["id"],
            )
        elif post > pre:
            await pool.execute(
                "UPDATE ct_promises SET status='live', total_supply_post=$1, "
                "resolved_at=NOW(), ground_truth_source='catchmint_delta' "
                "WHERE id=$2", post, r["id"],
            )
        else:
            await pool.execute(
                "UPDATE ct_promises SET status='missed', total_supply_post=$1, "
                "resolved_at=NOW(), ground_truth_source='catchmint_delta' "
                "WHERE id=$2", post, r["id"],
            )
        handled += 1
    return handled


async def _resolve_deferred(
    pool: asyncpg.Pool, client: CatchmintClient,
) -> int:
    """
    Path 3: deferred_check items с promised_ts + 30min < now.
    No pre-snapshot baseline → use post-snapshot only; if any > 0 → 'live' heuristic;
    if exactly 0 or 404 → 'unverifiable' (we don't know if mint happened earlier).

    ALSO catches **stale `announced` rows** whose T-30min..T+30min pre-snapshot window
    was missed (e.g., cron didn't run during that window because of first-deploy gap or
    transient outage). Without this fallback, such promises stick at `announced` forever.
    Treat them с same post-only heuristic as deferred_check.
    """
    rows = await pool.fetch(
        """
        SELECT id, item_id, contract_address, collection_name, status
        FROM ct_promises
        WHERE (status = 'deferred_check' OR status = 'announced')
          AND NOW() > promised_ts + INTERVAL '30 minutes'
        """
    )
    handled = 0
    for r in rows:
        addr = r["contract_address"]
        if not addr and r["collection_name"]:
            addr = await _resolve_name_to_address(client, r["collection_name"])

        if not addr:
            await pool.execute(
                "UPDATE ct_promises SET status='unverifiable', "
                "resolved_at=NOW(), ground_truth_source='no_baseline' "
                "WHERE id=$1", r["id"],
            )
            handled += 1
            continue

        post = await _snapshot_total_supply(client, addr)
        if post and post > 0:
            await pool.execute(
                "UPDATE ct_promises SET status='live', total_supply_post=$1, "
                "resolved_at=NOW(), ground_truth_source='deferred_post_only' "
                "WHERE id=$2", post, r["id"],
            )
        else:
            await pool.execute(
                "UPDATE ct_promises SET status='unverifiable', "
                "resolved_at=NOW(), ground_truth_source='no_baseline' "
                "WHERE id=$1", r["id"],
            )
        handled += 1
    return handled


async def _update_credibility(pool: asyncpg.Pool) -> int:
    """
    Recompute ct_dev_credibility per handle over last 90d.
    Returns rows touched.
    """
    await pool.execute(
        """
        INSERT INTO ct_dev_credibility (handle, completed, missed, unverifiable, score, last_updated)
        SELECT
            i.author_handle AS handle,
            COUNT(*) FILTER (WHERE p.status='live')         AS completed,
            COUNT(*) FILTER (WHERE p.status='missed')       AS missed,
            COUNT(*) FILTER (WHERE p.status='unverifiable') AS unverifiable,
            CASE WHEN COUNT(*) FILTER (WHERE p.status IN ('live','missed')) >= 3
                 THEN COUNT(*) FILTER (WHERE p.status='live')::REAL
                      / NULLIF(COUNT(*) FILTER (WHERE p.status IN ('live','missed')), 0)
                 ELSE NULL
            END AS score,
            NOW() AS last_updated
        FROM ct_promises p
        JOIN ct_digest_items i ON i.id = p.item_id
        WHERE p.resolved_at > NOW() - INTERVAL '90 days'
        GROUP BY i.author_handle
        ON CONFLICT (handle) DO UPDATE SET
            completed    = EXCLUDED.completed,
            missed       = EXCLUDED.missed,
            unverifiable = EXCLUDED.unverifiable,
            score        = EXCLUDED.score,
            last_updated = EXCLUDED.last_updated
        """
    )
    cnt = await pool.fetchval("SELECT COUNT(*) FROM ct_dev_credibility")
    return cnt or 0


async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")

    # Kill switch (per ADR R-6 — mirrored from ct_alpha_digest.py main)
    if settings.ct_digest_paused:
        log.info("CT_DIGEST_PAUSED=1 — promise_cron skipping")
        return 0

    # If ct_digest_topic_id is set, anthropic key must also be set
    # (some downstream credibility analysis may use LLM in future; defensive check).
    # promise_cron itself doesn't call LLM в V1, но соответствие safety policy.
    if settings.ct_digest_topic_id and not settings.anthropic_api_key:
        log.error("CT_DIGEST_TOPIC_ID set but ANTHROPIC_API_KEY missing — abort")
        return 2

    await init_db()
    pool = get_pool()
    try:
        async with CatchmintClient(
            settings.catchmint_api_base,
            getattr(settings, "catchmint_user_agent", "xanalyst-ct-digest/1.0"),
        ) as client:
            try:
                up = await _resolve_announced_to_upcoming(pool, client)
                log.info("Path 1 (announced→upcoming): %d handled", up)

                res = await _resolve_upcoming(pool, client)
                log.info("Path 2 (upcoming resolution): %d handled", res)

                deferred = await _resolve_deferred(pool, client)
                log.info("Path 3 (deferred resolution): %d handled", deferred)
            except RateLimited as e:
                log.warning("RateLimited, retry next tick: retry_after=%ds", e.retry_after)
                return 1

        if res + deferred > 0:
            cred_count = await _update_credibility(pool)
            log.info("credibility table refreshed: %d handles", cred_count)

    finally:
        await close_db()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
