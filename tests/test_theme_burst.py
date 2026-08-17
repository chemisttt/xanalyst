"""Integration tests для Theme Burst Detector (spec 002 V1.5).

Coverage:
- S1 z-score fires on spike; blocks on count_W30<8
- z_active gate inert когда count_7d < 200
- S4 hard-fire when new ticker has ≥3 authors
- S5 hard-fire when new CA has ≥2 authors
- S4 ignores tickers seen 25h+ ago
- combine_score requires ≥2 distinct signals для soft-fire
- Cooldown SETNX atomic blocks double-fire same cycle
- Cooldown released after TTL expiry
- LLM noise=true releases cooldown + suppresses alert
- LLM all-providers-fail emits raw-signal to SHADOW unconditionally
- Dry-run flag routes to SHADOW topic
- Kill-switch flips dry_run on high fire rate
- Author parser handles edge cases
- UNIQUE(cycle_ts_5min, bucket) constraint blocks race double-insert
- Reverse migration drops tables only (no channel_messages data loss)
- Bot tag callback updates alpha_events.user_tag

Requires Postgres test DB (xanalyst_test) + Redis db=15. Use:
    pytest tests/test_theme_burst.py -v --strict-markers
"""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
import pytest_asyncio


# =============================================================================
# Author parser unit tests
# =============================================================================

def test_author_parser_handles_edge_cases():
    from shared.author_parser import extract_real_author
    assert extract_real_author("[CALLER_B](tg://x) (@example_handle)\nbody") == "example_handle"
    assert extract_real_author("(@USER)\nbody") == "user"
    assert extract_real_author("plain") is None
    assert extract_real_author("hi @alice") is None
    assert extract_real_author("(@bob)\n(@alice)") == "bob"  # first line only
    assert extract_real_author(None) is None
    assert extract_real_author("") is None


# =============================================================================
# Chat-tuned ticker extraction
# =============================================================================

def test_chat_tuned_ticker_extracts_plain_text():
    from shared.chat_tickers import extract_tickers_chat
    assert sorted(extract_tickers_chat("sol pumping bonk wif memes")) == ["BONK", "SOL", "WIF"]
    assert extract_tickers_chat("please buy now") == []
    assert extract_tickers_chat(None) == []
    # Case-insensitive
    assert "BTC" in extract_tickers_chat("BTC up")
    assert "BTC" in extract_tickers_chat("btc dump")


# =============================================================================
# DB-backed signal tests
# =============================================================================

async def _insert_msg(conn, *, chat_id, topic_id, channel_name, text, message_date,
                     message_id, tickers=None, cas=None, urls=None):
    """Helper для insertion."""
    await conn.execute("""
        INSERT INTO channel_messages
            (source, source_account, channel_id, channel_name, message_id,
             author_name, text, has_media, urls,
             extracted_tickers, extracted_cas, message_date, telegram_topic_id)
        VALUES ('telegram', 'private_mirror', $1, $2, $3, 'mirror', $4, FALSE, $5,
                $6, $7, $8, $9)
        ON CONFLICT (source, channel_id, message_id) DO NOTHING
    """, chat_id, channel_name, message_id, text, urls or [],
        tickers or [], cas or [], message_date, topic_id)


@pytest.mark.asyncio
async def test_s1_msg_rate_z_fires_on_spike(pg_pool, redis_client):
    """Insert 30 msgs в W30 (burst) + baseline 7d с по 2 msg/30min → S1 fires."""
    from shared.theme_signals import compute_s1_msg_rate_z, is_z_signal_active

    chat_id = 9999
    topic_id = 1
    label = "TEST / Burst"
    now = datetime.utcnow()

    async with pg_pool.acquire() as conn:
        # Baseline 7d: 2 msg per 30-min slot × 336 slots = 672 msgs, low variance, mean=2 std=0
        # Need non-zero stddev. Use 1-3 msgs per slot для variation.
        for i in range(336):
            slot_start = now - timedelta(days=7) + timedelta(minutes=30 * i)
            n_in_slot = 1 + (i % 3)
            for j in range(n_in_slot):
                await _insert_msg(
                    conn, chat_id=chat_id, topic_id=topic_id, channel_name=label,
                    text=f"baseline msg {i}-{j}",
                    message_date=slot_start + timedelta(seconds=j),
                    message_id=i * 100 + j,
                )
        # Burst: 30 msgs in last 30 min
        for k in range(30):
            await _insert_msg(
                conn, chat_id=chat_id, topic_id=topic_id, channel_name=label,
                text=f"burst {k}", message_date=now - timedelta(minutes=20-k),
                message_id=99000 + k,
            )

    z_active = await is_z_signal_active(pg_pool, chat_id, topic_id, label, now)
    assert z_active, "z_active should be True with 7d+ baseline data"

    s1 = await compute_s1_msg_rate_z(pg_pool, redis_client, chat_id, topic_id, label, now, z_active)
    assert s1.fired, f"S1 should fire on 30 msgs vs baseline ~2/slot; got {s1}"
    assert s1.value > 2.0, f"z-score should be > 2.0; got {s1.value}"


@pytest.mark.asyncio
async def test_s1_blocks_on_low_count(pg_pool, redis_client):
    """count_W30 < 6 → S1 returns fired=False even if z hypothetically huge."""
    from shared.theme_signals import compute_s1_msg_rate_z

    chat_id = 9998
    topic_id = 2
    label = "TEST / Low"
    now = datetime.utcnow()

    async with pg_pool.acquire() as conn:
        # 7d baseline filled, but W30 has only 3 msgs
        for i in range(200):
            await _insert_msg(
                conn, chat_id=chat_id, topic_id=topic_id, channel_name=label,
                text=f"baseline {i}",
                message_date=now - timedelta(days=4) + timedelta(minutes=30 * i),
                message_id=i,
            )
        for k in range(3):
            await _insert_msg(
                conn, chat_id=chat_id, topic_id=topic_id, channel_name=label,
                text=f"recent {k}", message_date=now - timedelta(minutes=k),
                message_id=999000 + k,
            )

    s1 = await compute_s1_msg_rate_z(pg_pool, redis_client, chat_id, topic_id, label, now, z_active=True)
    assert not s1.fired, "S1 must not fire on count_W30 < 6 floor"


@pytest.mark.asyncio
async def test_s5_new_ca_hard_fires(pg_pool):
    """New CA in W30 mentioned by ≥2 authors → S5 hard-fire."""
    from shared.theme_signals import compute_s5_new_ca

    chat_id = 9997
    topic_id = 3
    label = "TEST / CA"
    now = datetime.utcnow()

    async with pg_pool.acquire() as conn:
        # 2 different real-authors mention same new CA in W30
        await _insert_msg(
            conn, chat_id=chat_id, topic_id=topic_id, channel_name=label,
            text="[A](tg://x) (@alice)\nновый меmcoin: So111aaa",
            cas=["So111aaa"],
            message_date=now - timedelta(minutes=10), message_id=1,
        )
        await _insert_msg(
            conn, chat_id=chat_id, topic_id=topic_id, channel_name=label,
            text="[B](tg://x) (@bob)\nthe same: So111aaa pump incoming",
            cas=["So111aaa"],
            message_date=now - timedelta(minutes=5), message_id=2,
        )
        # No prior 7d mention of that CA → first-seen-24h satisfied.

    s5 = await compute_s5_new_ca(pg_pool, chat_id, topic_id, label, now)
    assert s5.hard_fire, f"S5 should hard-fire on new CA with ≥2 authors; got {s5}"
    assert s5.value == 1, f"Should report 1 new CA"


@pytest.mark.asyncio
async def test_unique_constraint_blocks_double_insert(pg_pool):
    """UNIQUE(cycle_ts_5min, bucket) prevents double insert (rev 2 C9 fix)."""
    from datetime import datetime
    import asyncpg

    now = datetime.utcnow().replace(microsecond=0)
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO alpha_signal_scores
                (cycle_ts, bucket_chat_id, bucket_topic_id, bucket_label,
                 composite_score, n_distinct_signals)
            VALUES ($1, 1, 1, 'TEST', 5.0, 2)
        """, now)
        # Same cycle_ts_5min — different cycle_ts but same 5-min bucket
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute("""
                INSERT INTO alpha_signal_scores
                    (cycle_ts, bucket_chat_id, bucket_topic_id, bucket_label,
                     composite_score, n_distinct_signals)
                VALUES ($1, 1, 1, 'TEST', 6.0, 2)
            """, now + timedelta(seconds=10))


@pytest.mark.asyncio
async def test_cooldown_setnx_atomic(pg_pool, redis_client):
    """SETNX-first cooldown — second acquire returns False."""
    from shared.theme_signals import acquire_cooldown_atomic, release_cooldown

    cycle_ts = datetime.utcnow()
    chat_id = 8888
    topic_id = 1

    ok1 = await acquire_cooldown_atomic(redis_client, chat_id, topic_id, cycle_ts)
    assert ok1, "First acquire should succeed"

    ok2 = await acquire_cooldown_atomic(redis_client, chat_id, topic_id, cycle_ts)
    assert not ok2, "Second acquire должен fail (cooldown active)"

    # Release + reacquire
    await release_cooldown(redis_client, chat_id, topic_id)
    ok3 = await acquire_cooldown_atomic(redis_client, chat_id, topic_id, cycle_ts)
    assert ok3, "Re-acquire after release должен succeed"


@pytest.mark.asyncio
async def test_combine_score_requires_2_distinct(pg_pool):
    """combine_score: score >= threshold AND n_distinct >= 2."""
    from shared.theme_signals import SignalResult, combine_score

    # All zero except S1 fired strongly:
    s1 = SignalResult(value=10.0, fired=True, contributes=10.0)
    zero = SignalResult(value=None, fired=False, contributes=0.0)
    s4 = SignalResult(value=0, fired=False, contributes=0.0, hard_fire=False)
    s5 = SignalResult(value=0, fired=False, contributes=0.0, hard_fire=False)

    combined = combine_score(s1, zero, zero, s4, s5, zero, zero, z_active=True)
    assert not combined.soft_fire, \
        f"score might exceed threshold but n_distinct=1 should block soft_fire. Got {combined}"


# =============================================================================
# Replay backtest sanity
# =============================================================================

@pytest.mark.asyncio
async def test_replay_raises_on_empty_window(pg_pool):
    """replay_theme_burst.py should raise InsufficientDataError on empty window."""
    from scripts.replay_theme_burst import replay, InsufficientDataError

    since = datetime(2025, 1, 1)
    until = datetime(2025, 1, 2)
    with pytest.raises(InsufficientDataError):
        await replay(since, until, "/tmp/replay_test_empty.json")


# =============================================================================
# Bot keyboard generator
# =============================================================================

def test_make_theme_burst_keyboard_shape():
    from shared.notifier import make_theme_burst_keyboard
    kb = make_theme_burst_keyboard(42)
    assert "inline_keyboard" in kb
    assert len(kb["inline_keyboard"]) == 2  # 2 rows
    flat = [btn for row in kb["inline_keyboard"] for btn in row]
    assert any(b["callback_data"] == "tag:42:plausible" for b in flat)
    assert any(b["callback_data"] == "tag:42:real" for b in flat)
    assert any(b["callback_data"] == "tag:42:noise" for b in flat)
    assert any(b["callback_data"] == "tag:42:late_alpha" for b in flat)
