"""8 statistical signals для Theme Burst Detector (spec 002 V1.5).

Each signal — async function operating on PG + Redis:
  - S1 msg_rate_z       (gated by z_active)
  - S2 rate_ratio       (gated by z_active)
  - S3 unique_author_z  (gated by z_active; uses real-author SQL)
  - S4 new_ticker_burst (HARD-FIRE; uses real-author SQL)
  - S5 new_ca_seen      (HARD-FIRE)
  - S6 rare_authors     (uses real-author SQL)
  - S7 — DROPPED rev 2 (Telegram forum reply_to API collision)
  - S8 url_domain_burst (uses real-author SQL)
  - S9 cooldown gate    (Redis SETNX-first atomic — `acquire_cooldown_atomic`)

Combine: hybrid weighted-OR — `combine_score()`.

Parameter provenance (CRITICAL): all thresholds in `shared/config.py` с
"first-choice, not swept against historical data" comment. Re-tune AFTER replay
backtest (scripts/replay_theme_burst.py) AND 7d shadow review. Heuristic transfer
from onchain-radar 014 — domain-specific failure modes possible.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from shared.author_parser import extract_real_author_sql
from shared.config import settings


log = logging.getLogger("theme_signals")

CACHE_PREFIX = "v15c"  # bump 2026-08-12: multi-only soft_fire + higher floors — invalidate baselines


@dataclass
class SignalResult:
    """Per-signal result returned by compute functions."""
    value: float | int | None = None      # raw signal value (z-score, ratio, count, etc)
    fired: bool = False                    # did signal threshold trigger
    contributes: float = 0.0               # contribution to composite score (0 if not fired)
    hard_fire: bool = False                # S4/S5 only — Stage A bypass
    extras: dict[str, Any] = field(default_factory=dict)  # signal-specific extras


@dataclass
class CombineResult:
    """Output of combine_score for fire decision."""
    hard_fire: bool = False
    soft_fire: bool = False
    score: float | None = None
    n_distinct_signals: int = 0
    reason: str = ""


# =============================================================================
# Bucket eligibility gate (rev 2 C2 fix — avoid cold-start trap)
# =============================================================================

async def is_z_signal_active(
    pool, chat_id: int, topic_id: int | None, label: str, now_ts: datetime
) -> bool:
    """True если bucket имеет:
       - ≥theme_burst_min_msgs_7d_for_z msgs/7d
       - ≥theme_burst_min_slots_7d_for_z non-empty 30-min slots/7d
       - non-zero stddev on slot counts

    Иначе hard-fire-only mode (S4/S5 still active).

    Legacy NULL handling: pre-Task-2 rows имеют telegram_topic_id=NULL.
    Fallback на channel_name string match.
    """
    row = await pool.fetchrow("""
        WITH slots AS (
          SELECT date_trunc('hour', message_date) +
                 (EXTRACT(MINUTE FROM message_date)::int / 30) * INTERVAL '30 min' AS slot,
                 COUNT(*) AS c
          FROM channel_messages
          WHERE source_account='private_mirror'
            AND channel_id=$1
            AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
            AND message_date BETWEEN $4::timestamp - INTERVAL '7 days'
                                 AND $4::timestamp - INTERVAL '30 minutes'
          GROUP BY 1
        )
        SELECT COUNT(*) AS n_slots,
               COALESCE(SUM(c), 0) AS n_msgs,
               COALESCE(STDDEV(c), 0) AS sd
        FROM slots
    """, chat_id, topic_id, label, now_ts)

    if row is None:
        return False
    return (
        row['n_msgs'] >= settings.theme_burst_min_msgs_7d_for_z
        and row['n_slots'] >= settings.theme_burst_min_slots_7d_for_z
        and float(row['sd']) > 0
    )


# =============================================================================
# S1 — msg_rate z-score
# =============================================================================

async def compute_s1_msg_rate_z(
    pool, redis, chat_id, topic_id, label, now_ts, z_active: bool
) -> SignalResult:
    """z = (count_W30 - μ_30m_slots_7d) / σ. Cached baseline 6h TTL."""
    if not z_active:
        return SignalResult()

    count_w30 = await pool.fetchval("""
        SELECT COUNT(*) FROM channel_messages
        WHERE source_account='private_mirror' AND channel_id=$1
          AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
          AND message_date > $4::timestamp - INTERVAL '30 minutes'
    """, chat_id, topic_id, label, now_ts)

    # Floor 4 (was 8): current discussion chats rarely hit 8/30m; 4 still
    # filters pure idle noise but lets moderate bursts enter z-path.
    # 2026-08-12: floor 4→6 — chat rate noise (games/flood) at 4–5 msgs/30m
    if count_w30 < 6:
        return SignalResult(value=count_w30, fired=False)

    cache_key = f"{CACHE_PREFIX}:baseline:s1:{chat_id}:{topic_id or 'none'}:{now_ts.weekday()}_{now_ts.hour}"
    cached = await redis.get(cache_key)
    if cached:
        try:
            mean, std = map(float, cached.split(','))
        except Exception:
            mean, std = 0.0, 0.0
    else:
        row = await pool.fetchrow("""
            WITH slots AS (
              SELECT date_trunc('hour', message_date) +
                     (EXTRACT(MINUTE FROM message_date)::int / 30) * INTERVAL '30 min' AS slot,
                     COUNT(*) AS c
              FROM channel_messages
              WHERE source_account='private_mirror' AND channel_id=$1
                AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
                AND message_date BETWEEN $4::timestamp - INTERVAL '7 days'
                                     AND $4::timestamp - INTERVAL '30 minutes'
              GROUP BY 1
            )
            SELECT COALESCE(AVG(c), 0) AS mean, COALESCE(STDDEV(c), 0) AS std FROM slots
        """, chat_id, topic_id, label, now_ts)
        mean = float(row['mean'] or 0)
        std = float(row['std'] or 0)
        await redis.set(cache_key, f"{mean},{std}", ex=21600)

    if std == 0:
        return SignalResult(value=None, fired=False)

    z = (count_w30 - mean) / std
    fired = z >= settings.theme_burst_s1_z_threshold
    return SignalResult(value=z, fired=fired, contributes=z if fired else 0.0,
                        extras={"count_w30": count_w30, "mean": mean, "std": std})


# =============================================================================
# S2 — rate_ratio (low-σ-friendly companion of S1)
# =============================================================================

async def compute_s2_rate_ratio(
    pool, redis, chat_id, topic_id, label, now_ts, z_active: bool
) -> SignalResult:
    """ratio = count_W30 / max(mean_30m, 1). Reuses S1 cache key for mean."""
    if not z_active:
        return SignalResult()

    import math
    count_w30 = await pool.fetchval("""
        SELECT COUNT(*) FROM channel_messages
        WHERE source_account='private_mirror' AND channel_id=$1
          AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
          AND message_date > $4::timestamp - INTERVAL '30 minutes'
    """, chat_id, topic_id, label, now_ts)

    cache_key = f"{CACHE_PREFIX}:baseline:s1:{chat_id}:{topic_id or 'none'}:{now_ts.weekday()}_{now_ts.hour}"
    cached = await redis.get(cache_key)
    if cached:
        try:
            mean = float(cached.split(',')[0])
        except Exception:
            mean = 0.0
    else:
        mean = 0.0  # будет populated S1's next run

    if mean < 0.5:
        return SignalResult(value=None, fired=False)

    ratio = count_w30 / max(mean, 1)
    fired = ratio >= settings.theme_burst_s2_ratio_threshold
    return SignalResult(value=ratio, fired=fired,
                        contributes=math.log2(max(ratio, 1)) if fired else 0.0)


# =============================================================================
# S3 — unique_author z-score (uses real-author SQL helper)
# =============================================================================

async def compute_s3_unique_author_z(
    pool, redis, chat_id, topic_id, label, now_ts, z_active: bool
) -> SignalResult:
    """z = (unique_real_authors_W30 - μ_authors_30m_7d) / σ.

    Uses extract_real_author_sql() for chat-aware author identification.
    """
    if not z_active:
        return SignalResult()

    author_expr = extract_real_author_sql()
    count_w30 = await pool.fetchval(f"""
        SELECT COUNT(DISTINCT {author_expr}) FROM channel_messages
        WHERE source_account='private_mirror' AND channel_id=$1
          AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
          AND message_date > $4::timestamp - INTERVAL '30 minutes'
    """, chat_id, topic_id, label, now_ts)

    if (count_w30 or 0) < 6:
        return SignalResult(value=count_w30, fired=False)

    cache_key = f"{CACHE_PREFIX}:baseline:s3:{chat_id}:{topic_id or 'none'}:{now_ts.weekday()}_{now_ts.hour}"
    cached = await redis.get(cache_key)
    if cached:
        try:
            mean, std = map(float, cached.split(','))
        except Exception:
            mean, std = 0.0, 0.0
    else:
        row = await pool.fetchrow(f"""
            WITH slots AS (
              SELECT date_trunc('hour', message_date) +
                     (EXTRACT(MINUTE FROM message_date)::int / 30) * INTERVAL '30 min' AS slot,
                     COUNT(DISTINCT {author_expr}) AS c
              FROM channel_messages
              WHERE source_account='private_mirror' AND channel_id=$1
                AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
                AND message_date BETWEEN $4::timestamp - INTERVAL '7 days'
                                     AND $4::timestamp - INTERVAL '30 minutes'
              GROUP BY 1
            )
            SELECT COALESCE(AVG(c), 0) AS mean, COALESCE(STDDEV(c), 0) AS std FROM slots
        """, chat_id, topic_id, label, now_ts)
        mean = float(row['mean'] or 0)
        std = float(row['std'] or 0)
        await redis.set(cache_key, f"{mean},{std}", ex=21600)

    if std == 0:
        return SignalResult(value=None, fired=False)

    z = (count_w30 - mean) / std
    fired = z >= settings.theme_burst_s3_author_z_threshold
    return SignalResult(value=z, fired=fired, contributes=z if fired else 0.0,
                        extras={"unique_authors_w30": count_w30})


# =============================================================================
# S4 — new_ticker_burst (HARD-FIRE) — rewritten as NOT EXISTS (rev 2 M-N5 fix)
# =============================================================================

async def compute_s4_new_ticker(
    pool, chat_id, topic_id, label, now_ts
) -> SignalResult:
    """Tickers first-seen-24h AND ≥3 mentions in W30 AND ≥2 unique real-authors.

    Hard-fire bypass: any ticker с n_authors >= theme_burst_s4_min_authors_hard.

    Uses extract_real_author_sql() inline для author counting.
    """
    author_expr = extract_real_author_sql()
    rows = await pool.fetch(f"""
        WITH w30 AS (
          SELECT unnest(extracted_tickers) AS ticker, text
          FROM channel_messages
          WHERE source_account='private_mirror' AND channel_id=$1
            AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
            AND message_date > $4::timestamp - INTERVAL '30 minutes'
            AND array_length(extracted_tickers, 1) > 0
        ),
        agg AS (
          SELECT ticker,
                 COUNT(*) AS n_mentions,
                 COUNT(DISTINCT {author_expr}) AS n_authors
          FROM w30
          GROUP BY ticker
          HAVING COUNT(*) >= $5
             AND COUNT(DISTINCT {author_expr}) >= $6
        )
        SELECT ticker, n_mentions, n_authors FROM agg
        WHERE NOT EXISTS (
          SELECT 1 FROM channel_messages cm2
          WHERE cm2.source_account='private_mirror' AND cm2.channel_id=$1
            AND (cm2.telegram_topic_id=$2 OR (cm2.telegram_topic_id IS NULL AND cm2.channel_name=$3))
            AND cm2.message_date BETWEEN $4::timestamp - INTERVAL '7 days'
                                     AND $4::timestamp - INTERVAL '24 hours'
            AND agg.ticker = ANY(cm2.extracted_tickers)
        )
    """, chat_id, topic_id, label, now_ts,
        settings.theme_burst_s4_min_mentions,
        settings.theme_burst_s5_min_authors)  # min 2 distinct authors required

    n_qualifying = len(rows)
    hard_fire = any(r['n_authors'] >= settings.theme_burst_s4_min_authors_hard for r in rows)
    return SignalResult(
        value=n_qualifying,
        fired=n_qualifying > 0,
        contributes=float(n_qualifying) if n_qualifying > 0 else 0.0,
        hard_fire=hard_fire,
        extras={"tickers": [r['ticker'] for r in rows],
                "max_n_authors": max((r['n_authors'] for r in rows), default=0)},
    )


# =============================================================================
# S5 — new_ca_seen (HARD-FIRE, lower bar than S4 — CA inherently rarer)
# =============================================================================

async def compute_s5_new_ca(
    pool, chat_id, topic_id, label, now_ts
) -> SignalResult:
    """CAs first-seen-24h AND ≥2 unique real-authors. Hard-fire on any qualifying CA."""
    author_expr = extract_real_author_sql()
    rows = await pool.fetch(f"""
        WITH w30 AS (
          SELECT unnest(extracted_cas) AS ca, text
          FROM channel_messages
          WHERE source_account='private_mirror' AND channel_id=$1
            AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
            AND message_date > $4::timestamp - INTERVAL '30 minutes'
            AND array_length(extracted_cas, 1) > 0
        ),
        agg AS (
          SELECT ca, COUNT(DISTINCT {author_expr}) AS n_authors
          FROM w30 GROUP BY ca
          HAVING COUNT(DISTINCT {author_expr}) >= $5
        )
        SELECT ca, n_authors FROM agg
        WHERE NOT EXISTS (
          SELECT 1 FROM channel_messages cm2
          WHERE cm2.source_account='private_mirror' AND cm2.channel_id=$1
            AND (cm2.telegram_topic_id=$2 OR (cm2.telegram_topic_id IS NULL AND cm2.channel_name=$3))
            AND cm2.message_date BETWEEN $4::timestamp - INTERVAL '7 days'
                                     AND $4::timestamp - INTERVAL '24 hours'
            AND agg.ca = ANY(cm2.extracted_cas)
        )
    """, chat_id, topic_id, label, now_ts, settings.theme_burst_s5_min_authors)

    n = len(rows)
    return SignalResult(
        value=n,
        fired=n > 0,
        contributes=float(n) if n > 0 else 0.0,
        hard_fire=n > 0,  # S5 hard-fires on ANY qualifying CA (rarer than S4's tickers)
        extras={"cas": [r['ca'] for r in rows]},
    )


# =============================================================================
# S6 — rare_authors active (uses real-author + source_account filter — rev 2 fix M-N1)
# =============================================================================

async def compute_s6_rare_authors(
    pool, chat_id, topic_id, label, now_ts, z_active: bool
) -> SignalResult:
    """≥2 distinct real-authors в W30 чьё 30d msg-count ≤ 3.

    z_active gate applies (composite signal, not hard-fire).
    """
    if not z_active:
        return SignalResult()

    author_expr = extract_real_author_sql()
    n = await pool.fetchval(f"""
        WITH author_counts AS (
          SELECT {author_expr} AS author, COUNT(*) AS c
          FROM channel_messages
          WHERE source_account='private_mirror'
            AND channel_id=$1
            AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
            AND message_date > $4::timestamp - INTERVAL '30 days'
          GROUP BY author HAVING {author_expr} IS NOT NULL
        ),
        recent_authors AS (
          SELECT DISTINCT {author_expr} AS author
          FROM channel_messages
          WHERE source_account='private_mirror'
            AND channel_id=$1
            AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
            AND message_date > $4::timestamp - INTERVAL '30 minutes'
        )
        SELECT COUNT(*) FROM recent_authors r
        JOIN author_counts a USING(author)
        WHERE a.c <= $5 AND r.author IS NOT NULL
    """, chat_id, topic_id, label, now_ts, settings.theme_burst_s6_rare_max_msgs)

    n = n or 0
    fired = n >= 2
    return SignalResult(value=n, fired=fired, contributes=float(n) if fired else 0.0)


# =============================================================================
# S8 — url_domain_burst (rev 2 M-N14 fix — explicit author predicate)
# =============================================================================

async def compute_s8_url_domain_burst(
    pool, chat_id, topic_id, label, now_ts, z_active: bool
) -> SignalResult:
    """Domains seen ≥2 real-authors в W30, NOT seen в last 7d в этом bucket."""
    if not z_active:
        return SignalResult()

    author_expr = extract_real_author_sql()
    rows = await pool.fetch(f"""
        WITH w30_domains AS (
          SELECT
            substring(unnest(urls) from 'https?://([^/]+)') AS domain,
            {author_expr} AS author
          FROM channel_messages
          WHERE source_account='private_mirror' AND channel_id=$1
            AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
            AND message_date > $4::timestamp - INTERVAL '30 minutes'
            AND array_length(urls, 1) > 0
        ),
        agg AS (
          SELECT domain, COUNT(DISTINCT author) AS n_authors
          FROM w30_domains
          WHERE domain IS NOT NULL AND author IS NOT NULL
          GROUP BY domain
          HAVING COUNT(DISTINCT author) >= 2
        )
        SELECT agg.domain, agg.n_authors FROM agg
        WHERE NOT EXISTS (
          SELECT 1 FROM channel_messages cm2
          WHERE cm2.source_account='private_mirror' AND cm2.channel_id=$1
            AND cm2.message_date BETWEEN $4::timestamp - INTERVAL '7 days'
                                     AND $4::timestamp - INTERVAL '30 minutes'
            AND EXISTS (SELECT 1 FROM unnest(cm2.urls) u WHERE u LIKE '%' || agg.domain || '%')
        )
    """, chat_id, topic_id, label, now_ts)

    n = len(rows)
    return SignalResult(
        value=n, fired=n > 0, contributes=float(n) if n > 0 else 0.0,
        extras={"domains": [r['domain'] for r in rows]},
    )


# =============================================================================
# S9 — cooldown gate (Redis SETNX-first atomic; rev 2 C7 fix)
# =============================================================================

async def acquire_cooldown_atomic(redis, chat_id, topic_id, cycle_ts) -> bool:
    """Atomic SETNX. Returns True if acquired (proceed to emit), False otherwise.

    Redis-down fail mode (rev 3 M-N8): explicit fail-closed.
    """
    key = f"{CACHE_PREFIX}:cooldown:{chat_id}:{topic_id or 'none'}"
    try:
        result = await redis.set(
            key,
            str(int(cycle_ts.timestamp())),
            ex=settings.theme_burst_cooldown_sec,
            nx=True,
        )
        return bool(result)
    except Exception as e:
        log.warning(f"Redis-down on cooldown acquire {chat_id}/{topic_id}: {e}")
        return False  # fail-closed


async def release_cooldown(redis, chat_id, topic_id) -> None:
    """Release cooldown explicitly (LLM noise=true OR worker crash cleanup)."""
    key = f"{CACHE_PREFIX}:cooldown:{chat_id}:{topic_id or 'none'}"
    try:
        await redis.delete(key)
    except Exception as e:
        log.warning(f"Cooldown release failed {chat_id}/{topic_id}: {e}")


# =============================================================================
# Combine — weighted-OR с Stage A (hard-fire) + Stage B (composite)
# =============================================================================

def combine_score(
    s1: SignalResult, s2: SignalResult, s3: SignalResult,
    s4: SignalResult, s5: SignalResult, s6: SignalResult,
    s8: SignalResult, z_active: bool,
) -> CombineResult:
    """Hybrid weighted-OR. Stage A hard-fire (S5 OR S4+3authors) bypasses z_active gate."""

    # Stage A — hard-fire
    if s5.hard_fire:
        return CombineResult(hard_fire=True, soft_fire=False, score=None,
                            reason="s5_new_ca")
    if s4.hard_fire:
        return CombineResult(hard_fire=True, soft_fire=False, score=None,
                            reason="s4_new_ticker_3+authors")

    # Stage B — composite (requires z_active)
    if not z_active:
        return CombineResult(hard_fire=False, soft_fire=False, score=None,
                            reason="z_inactive")

    w = settings.theme_burst_weights  # SimpleNamespace
    score = (
        w.w1 * s1.contributes +
        w.w2 * s2.contributes +
        w.w3 * s3.contributes +
        w.w4 * s4.contributes +
        w.w6 * s6.contributes +
        w.w8 * s8.contributes
    )
    n_distinct = sum(1 for s in (s1, s2, s3, s4, s6, s8) if s.fired)

    # 2026-08-12 calibration: drop s1_alone/s2_alone.
    # 14d: 35/99 fires were n_distinct<=1 (chat rate spikes — games/flood, high user_noise).
    # Soft fire only when 2+ independent signals + composite threshold.
    multi = score >= settings.theme_burst_composite_fire and n_distinct >= 2
    soft_fire = multi

    if multi:
        reason = "soft_fire_composite"
    else:
        reason = "below_threshold"

    return CombineResult(
        hard_fire=False, soft_fire=soft_fire, score=score,
        n_distinct_signals=n_distinct,
        reason=reason,
    )
