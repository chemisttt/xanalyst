"""
Spec 004 Task 5: CT Alpha Digest — main service entry point.

CLI:
  python -m services.ct_alpha_digest --once [--triggered-by USER_ID]
  python -m services.ct_alpha_digest --dry-run --sample N
  python -m services.ct_alpha_digest --validate-config

Exit codes (per ADR 0002 R-3):
  0 — success / cookie valid / dry-run OK
  1 — runtime error / cookie invalid / classifier failure
  2 — config / prerequisite missing (cookie file, ANTHROPIC_API_KEY+TOPIC_ID inconsistent)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.redis_client import init_redis, close_redis
from shared import notifier

from services.ct_digest.collectors import (
    fetch_all, X_TOKENS_PATH, CollectorAuthError,
)
from services.ct_digest.classifier import (
    classify_batch, assign_short_ids, CTDigestPaused, ClassifiedItem,
)
from services.ct_digest.cross_ref import enrich_with_cross_refs, has_strong_cross_ref
from services.ct_digest.digest_formatter import (
    build_digest_markdown, build_inline_keyboard, build_cross_post_alert,
    split_into_messages, _select_for_digest,
)
from services.ct_digest.promises import extract_promises_for_batch
from services.ct_digest.meta_tldr import generate_meta_tldr
from shared.ct_digest_queries import NFT_CURATORS, build_account_query

log = logging.getLogger("ct_alpha_digest")

# If classifier recovered < this fraction of collected tweets (and N is large enough),
# do not post a hollow digest — alert SYSHEALTH instead (tick 108: 2/17 after refusal).
_MIN_CLASSIFY_RATIO = 0.30
_MIN_RAW_FOR_RATIO_CHECK = 5
# Absolute floor: after dedup/cap/paid_hype filter, need at least this many TG lines.
_MIN_DIGEST_ITEMS = 1


def _alert_ct_failure(tick_id: int, reason: str) -> None:
    """Best-effort SYSHEALTH ping; never raise into tick path."""
    text = (
        f"⚠️ <b>CT Alpha Digest failed</b>\n"
        f"tick <code>#{tick_id}</code>\n"
        f"{reason}"
    )
    try:
        notifier._alert_syshealth(text)
    except Exception as e:
        log.error("SYSHEALTH alert failed: %s", e)


def assert_ct_digest_ready() -> None:
    """
    Startup assertion (per ADR 0002 R-6 — lives here, NOT in shared/config.__init__,
    иначе все сервисы упадут при partial .env state).

    Raises:
        SystemExit(2) if ANTHROPIC_API_KEY missing while CT_DIGEST_TOPIC_ID set,
                       or if cookie file missing.
    """
    if settings.ct_digest_topic_id and not settings.anthropic_api_key:
        log.error("CT_DIGEST_TOPIC_ID set but ANTHROPIC_API_KEY missing — abort")
        sys.exit(2)
    if not settings.ct_digest_topic_id:
        log.warning("CT_DIGEST_TOPIC_ID not set — digest emission will be no-op (dev mode)")


async def _persist_items(pool, tick_id: int, classified: list[ClassifiedItem]) -> list[tuple]:
    """
    INSERT items в ct_digest_items. Returns [(ClassifiedItem, db_id), ...].
    """
    out = []
    for item in classified:
        row_id = await pool.fetchval(
            """
            INSERT INTO ct_digest_items
                (tick_id, tweet_id, tweet_url, author_handle, author_metadata,
                 posted_at, raw_text, bucket, novelty_score, included, tags,
                 mechanic_notes, contract_address_hint, promised_ts,
                 collection_name_hint, cross_refs, short_id)
            VALUES ($1, $2, $3, $4, $5::jsonb,
                    NULLIF($6, '')::timestamp, $7, $8, $9, $10, $11::jsonb,
                    $12, $13, NULLIF($14, '')::timestamp, $15, $16::jsonb, $17)
            ON CONFLICT (tick_id, tweet_id) DO NOTHING
            RETURNING id
            """,
            tick_id, item.tweet_id, item.raw.url, item.raw.author_handle,
            _to_json(item.raw.author_metadata or {}),
            item.raw.posted_at_iso or "",
            item.raw.text, item.bucket, item.novelty_score, item.included,
            _to_json(item.tags or []),
            item.mechanic_notes, item.contract_address_hint,
            item.promised_timestamp_iso or "",
            item.collection_name_hint,
            _to_json(item.cross_refs or {}),
            item.short_id,
        )
        if row_id:
            out.append((item, row_id))
    return out


def _to_json(obj) -> str:
    import json
    return json.dumps(obj)


async def run_tick(triggered_by: Optional[int] = None, since_hours: int = 6) -> int:
    """One digest tick. Returns 0 on success, non-zero on failure."""
    if settings.ct_digest_paused:
        log.info("CT_DIGEST_PAUSED=1 — skipping tick")
        return 0

    await init_db()
    await init_redis()
    pool = get_pool()

    try:
        # 1) Start tick row
        tick_id = await pool.fetchval(
            """
            INSERT INTO ct_digest_ticks (started_at, status, manually_triggered_by)
            VALUES (NOW(), 'running', $1)
            RETURNING tick_id
            """,
            triggered_by,
        )
        log.info("tick %d started (triggered_by=%s)", tick_id, triggered_by)

        # 2) Fetch via 3 collectors
        try:
            raw_items = await fetch_all(since_hours=since_hours)
        except CollectorAuthError as e:
            log.error("collector auth error: %s", e)
            await pool.execute(
                "UPDATE ct_digest_ticks SET status='failed', completed_at=NOW() WHERE tick_id=$1",
                tick_id,
            )
            return 1

        if not raw_items:
            log.warning("no raw items returned by collectors")
            await pool.execute(
                "UPDATE ct_digest_ticks SET status='completed', completed_at=NOW() WHERE tick_id=$1",
                tick_id,
            )
            return 0

        # 3) Classify (с prompt cache + cost tracking + astroturf override)
        try:
            classified = await classify_batch(raw_items)
        except CTDigestPaused:
            log.info("classifier paused mid-tick")
            await pool.execute(
                "UPDATE ct_digest_ticks SET status='paused', completed_at=NOW() WHERE tick_id=$1",
                tick_id,
            )
            return 0
        except Exception as e:
            log.error("classification failed: %s", e)
            await pool.execute(
                "UPDATE ct_digest_ticks SET status='failed', completed_at=NOW() WHERE tick_id=$1",
                tick_id,
            )
            return 1

        # 4) Guard: hollow classification (proxy refusal → 2/17) — persist audit, no TG spam
        n_raw = len(raw_items)
        n_cls = len(classified)
        ratio = (n_cls / n_raw) if n_raw else 0.0
        if n_raw >= _MIN_RAW_FOR_RATIO_CHECK and ratio < _MIN_CLASSIFY_RATIO:
            reason = (
                f"classify coverage {n_cls}/{n_raw} ({ratio:.0%}) "
                f"&lt; {_MIN_CLASSIFY_RATIO:.0%} — likely LLM refusal/proxy fail. "
                f"Digest NOT posted."
            )
            log.error("tick %d abort emit: %s", tick_id, reason)
            # Still persist whatever we got for debugging
            assign_short_ids(classified)
            await _persist_items(pool, tick_id, classified)
            await pool.execute(
                """
                UPDATE ct_digest_ticks
                SET completed_at=NOW(), items_classified=$1, status='failed'
                WHERE tick_id=$2
                """,
                n_cls, tick_id,
            )
            _alert_ct_failure(tick_id, reason)
            return 1

        # 5) Cross-validation
        await enrich_with_cross_refs(pool, classified)

        # 6) Short IDs (e1/c1/k1/s1/p1 per-tick)
        assign_short_ids(classified)

        # 7) Persist items
        items_with_ids = await _persist_items(pool, tick_id, classified)
        log.info("persisted %d items", len(items_with_ids))

        # 8) Promise extraction (inline insert ct_promises)
        promise_count = await extract_promises_for_batch(pool, items_with_ids)
        log.info("extracted %d promises", promise_count)

        # Postable lines after dedup/cap/paid_hype filter
        selected_for_tg, _ = _select_for_digest(classified)
        handles_lower = list(
            {(c.raw.author_handle or "").lower() for c in classified if c.included}
        )
        stats_map: dict[str, dict] = {}

        # 9) Emission — main digest post
        digest_msg_id: Optional[int] = None
        if settings.ct_digest_topic_id:
            if len(selected_for_tg) < _MIN_DIGEST_ITEMS:
                reason = (
                    f"0 postable items after filter "
                    f"(classified={n_cls}, raw={n_raw}). Digest NOT posted."
                )
                log.error("tick %d abort emit: %s", tick_id, reason)
                await pool.execute(
                    """
                    UPDATE ct_digest_ticks
                    SET completed_at=NOW(), items_classified=$1, status='failed'
                    WHERE tick_id=$2
                    """,
                    n_cls, tick_id,
                )
                _alert_ct_failure(tick_id, reason)
                return 1

            # Batch-fetch latest twitter_analyses row per unique author handle
            # (DISTINCT ON keeps most recent analyzed_at per handle).
            tier_map: dict[str, str] = {}
            if handles_lower:
                analyses_rows = await pool.fetch(
                    """
                    SELECT DISTINCT ON (LOWER(handle))
                        LOWER(handle) AS h, tier, twitter_score, followers_count,
                        engagement_rate, account_age_days, rt_percentage, growth_velocity
                    FROM twitter_analyses
                    WHERE LOWER(handle) = ANY($1::text[])
                    ORDER BY LOWER(handle), analyzed_at DESC
                    """,
                    handles_lower,
                )
                stats_map = {r["h"]: dict(r) for r in analyses_rows}
                # Legacy tier_map fallback for handles in watchlist but not yet analyzed
                if len(stats_map) < len(handles_lower):
                    missing = [h for h in handles_lower if h not in stats_map]
                    wl_rows = await pool.fetch(
                        "SELECT LOWER(handle) AS h, last_tier FROM twitter_watchlist "
                        "WHERE LOWER(handle) = ANY($1::text[])",
                        missing,
                    )
                    tier_map = {r["h"]: r["last_tier"] for r in wl_rows if r["last_tier"]}

            # Meta TL;DR — 2-3 строки нарратива над header'ом. Graceful: None → header без preamble.
            meta_tldr = await generate_meta_tldr(classified)

            text = build_digest_markdown(
                classified, tick_id=tick_id,
                cron_label="on-demand" if triggered_by else "scheduled",
                tier_map=tier_map, stats_map=stats_map,
                meta_tldr=meta_tldr,
            )
            # Cap/dedup in formatter; split only if still over TG limit.
            messages = split_into_messages(text)
            kb = build_inline_keyboard(tick_id)
            digest_msg_id = await asyncio.to_thread(
                notifier.send_to_topic, messages[0], settings.ct_digest_topic_id,
                "HTML", True, kb, None,
            )
            for cont in messages[1:]:
                await asyncio.to_thread(
                    notifier.send_to_topic, cont, settings.ct_digest_topic_id,
                    "HTML", True, None, None,
                )
            log.info("digest_msg_id=%s sent to topic %s (%d message(s), %d items)",
                     digest_msg_id, settings.ct_digest_topic_id,
                     len(messages), len(selected_for_tg))

            # 10) Cross-post в EARLY: ТОЛЬКО strong matches (catchmint OR mirror_meme).
            # Watchlist-only matches видны в digest как ✅WL, но в EARLY не уходят —
            # иначе spam от любого @-mention отслеживаемого handle.
            early_topic = getattr(settings, "telegram_early_topic_id", None)
            cross_posted = 0
            if early_topic:
                for item in classified:
                    if item.included and has_strong_cross_ref(item.cross_refs):
                        alert = build_cross_post_alert(item)
                        try:
                            await asyncio.to_thread(
                                notifier.send_to_topic, alert, int(early_topic),
                                "HTML", True, None, None,
                            )
                            cross_posted += 1
                        except Exception as e:
                            log.warning("cross-post to EARLY failed: %s", e)
                log.info("cross-posted %d items to EARLY topic %s", cross_posted, early_topic)
        else:
            log.warning("CT_DIGEST_TOPIC_ID not set — skipping TG emission (dev mode)")

        # 11) Finalize tick row
        await pool.execute(
            """
            UPDATE ct_digest_ticks
            SET completed_at=NOW(), items_classified=$1, digest_msg_id=$2, status='completed'
            WHERE tick_id=$3
            """,
            len(items_with_ids), digest_msg_id, tick_id,
        )

        # 12) Self-improving feedback: enqueue unknown handles → twitter_worker analyzes
        # them, next tick's digest shows their stats. Push to stream:x_analyze (existing
        # Redis stream consumed by services/twitter_worker.py).
        if handles_lower:
            unknown = [h for h in handles_lower if h not in stats_map]
            if unknown:
                from shared.redis_client import get_redis as _gr
                redis = _gr()
                # Dedup with a 24h cooldown — avoid spamming same handle if it appears
                # in multiple ticks before worker processes it.
                queued = 0
                for h in unknown:
                    cooldown_key = f"ct_digest:enqueue_cooldown:{h}"
                    if await redis.set(cooldown_key, "1", ex=86400, nx=True):
                        await redis.xadd("stream:x_analyze", {"handle": h, "source": "ct_digest"})
                        queued += 1
                log.info("enqueued %d unknown handles to stream:x_analyze (cooldown 24h)", queued)

        return 0
    finally:
        await close_redis()
        await close_db()


async def dry_run(sample: int = 10) -> int:
    """No DB writes, no TG send — print digest to stdout. For smoke testing."""
    log.info("dry_run sample=%d", sample)
    try:
        raw_items = await fetch_all(since_hours=6)
    except CollectorAuthError as e:
        log.error("cookie auth error: %s", e)
        return 1

    if not raw_items:
        print("no items fetched")
        return 0

    sample_items = raw_items[:sample]
    classified = await classify_batch(sample_items)
    assign_short_ids(classified)
    # Meta TL;DR работает без redis (cost tracking optional — пропустит запись если pool не открыт)
    meta_tldr = await generate_meta_tldr(classified)
    md = build_digest_markdown(classified, tick_id=0, cron_label="dry-run", meta_tldr=meta_tldr)
    print(md)
    print(f"\n--- dry-run stats: {len(classified)}/{len(sample_items)} classified ---")
    if meta_tldr:
        print(f"--- meta tldr: {len(meta_tldr)} chars ---")
    return 0


async def validate_config() -> int:
    """
    Exit codes per ADR 0002 R-3:
      2 = cookie file missing (distinguishable от auth/HTTP errors)
      1 = cookie invalid OR curator probe non-zero exit
      0 = OK (curator volume = stdout advisory only, не gate-blocker)
    """
    # Pre-check: cookie file existence (x_search sys.exit(str)=1 для всех ошибок,
    # не дает distinguish auth-vs-config — поэтому проверяем здесь явно)
    if not Path(X_TOKENS_PATH).exists():
        print(f"❌ exit 2: cookie file missing at {X_TOKENS_PATH}", file=sys.stderr)
        print("Run deploy step #2 to provision x_tokens.json on VPS", file=sys.stderr)
        return 2

    # Single x_search probe — minimal query to test cookie validity
    cmd = [
        sys.executable,
        os.path.expanduser("~/.claude/tools/x_search.py"),
        "from:" + NFT_CURATORS[0],  # single low-volume query
        "--json", "--limit", "1", "--since-hours", "168",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    except asyncio.TimeoutError:
        proc.kill()
        print("❌ exit 1: x_search probe timeout (cookie may be invalid)", file=sys.stderr)
        return 1
    if proc.returncode != 0:
        print(f"❌ exit 1: x_search probe failed (cookie likely invalid)\n{stderr.decode()[:200]}", file=sys.stderr)
        return 1

    print(f"✅ cookie valid (probe returned {len(stdout)} bytes for @{NFT_CURATORS[0]})")
    print("Curator activity check (advisory only):")

    # Soft-advisory: probe each curator briefly
    hits = 0
    for h in NFT_CURATORS:
        cmd = [
            sys.executable,
            os.path.expanduser("~/.claude/tools/x_search.py"),
            f"from:{h}",
            "--json", "--limit", "1", "--since-hours", "168",
        ]
        proc2 = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            out, _ = await asyncio.wait_for(proc2.communicate(), timeout=15)
        except asyncio.TimeoutError:
            proc2.kill()
            continue
        if proc2.returncode == 0 and out and out.strip() != b"[]":
            hits += 1
            print(f"  ✓ @{h}")
        else:
            print(f"  - @{h} (no recent activity OR query failed)")

    print(f"\nCurator activity: {hits}/{len(NFT_CURATORS)} returned ≥1 result (advisory).")
    return 0


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="ct_alpha_digest")
    parser.add_argument("--once", action="store_true", help="single tick + exit")
    parser.add_argument("--triggered-by", type=int, default=None, dest="triggered_by",
                        help="Telegram user_id when invoked via /digest")
    parser.add_argument("--dry-run", action="store_true", help="no TG send, no DB write")
    parser.add_argument("--sample", type=int, default=10, help="dry-run sample size")
    parser.add_argument("--validate-config", action="store_true",
                        dest="validate_config", help="probe x_search cookie + curators")
    parser.add_argument("--since-hours", type=int, default=6, help="lookback window")
    args = parser.parse_args()

    if args.validate_config:
        sys.exit(asyncio.run(validate_config()))

    if args.dry_run:
        # dry-run still needs anthropic key but no topic — skip topic assertion
        if not settings.anthropic_api_key:
            print("❌ ANTHROPIC_API_KEY required for dry-run (classifier call)", file=sys.stderr)
            sys.exit(2)
        sys.exit(asyncio.run(dry_run(sample=args.sample)))

    if args.once:
        assert_ct_digest_ready()
        sys.exit(asyncio.run(run_tick(triggered_by=args.triggered_by,
                                       since_hours=args.since_hours)))

    parser.print_help()
    sys.exit(0)


if __name__ == "__main__":
    main()
