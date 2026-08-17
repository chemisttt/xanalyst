"""
Integration tests для Private Mirror Monitor V1 (spec 001 validation gate).

7 тестов покрывают core-логику пайплайна:
  1. test_ingestion_writes_with_source_account — ingester пишет в PG с правильным source_account
  2. test_named_caller_fast_path — CALLER_A событие сразу публикуется в TG, без дедупа
  3. test_dedup_threshold_emits_once — 3 одинаковых spread'а за окно → 1 emission
  4. test_dedup_below_threshold_no_emit — 1 одиночный сигнал → НЕТ emission в category
  5. test_window_close_locks_message — после window close: emitted_locked=TRUE + final edit
  6. test_late_echo_no_edit — поздний source → late_echo_count++ в БД, без edit
  7. test_source_account_filter_in_digest — digest читает ТОЛЬКО private_mirror

Запуск:
  createdb xanalyst_test   # один раз
  pytest tests/test_private_mirror_v1.py -v
"""

import asyncio
import json
from datetime import datetime, timedelta

import pytest
import pytest_asyncio


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Test 1 — ingester behavior
# ---------------------------------------------------------------------------

async def test_ingestion_writes_with_source_account(pg_pool):
    """Прямой INSERT по схеме ingester'а: source_account='private_mirror' + extracted поля."""
    from services.private_mirror_monitor import extract_tickers, extract_cas

    # Сценарий — текст с тикером и CA
    text = "buy $WIF at 0x1234567890aBcDeF1234567890aBcDeF12345678 dump"
    tickers = extract_tickers(text)
    cas = extract_cas(text)

    assert tickers == ["WIF"]
    assert cas == ["0x1234567890abcdef1234567890abcdef12345678"]  # lowercase

    async with pg_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO channel_messages
                (source, source_account, channel_id, channel_name, message_id,
                 author_name, text, has_media, urls, extracted_tickers,
                 extracted_cas, message_date)
            VALUES ('telegram', 'private_mirror', $1, $2, $3, $4, $5, FALSE,
                    $6, $7, $8, $9)
            """,
            123456, "Test Privatka", 1, "@tester", text,
            [], tickers, cas, datetime.utcnow(),
        )

        row = await conn.fetchrow(
            "SELECT * FROM channel_messages WHERE channel_id = $1",
            123456,
        )

    assert row["source_account"] == "private_mirror"
    assert row["extracted_tickers"] == ["WIF"]
    assert row["extracted_cas"] == ["0x1234567890abcdef1234567890abcdef12345678"]
    assert row["source"] == "telegram"  # backward compat


# ---------------------------------------------------------------------------
# Test 2 — named caller fast-path
# ---------------------------------------------------------------------------

async def test_named_caller_fast_path(redis_client, mock_bot_api):
    """Сообщение в mirror_routed_events → handler публикует в CALLER_A топик."""
    from services.private_mirror_dedup import handle_named_caller

    data = {
        "named_caller": "CALLER_A",
        "source_label": "Caller A",
        "text": "Тестовый колл: $LAB Bitget short / Binance long 1.8%",
        "sender": "CALLER_A",
        "tickers": json.dumps(["LAB"]),
        "cas": json.dumps([]),
    }

    await handle_named_caller(data)

    # Один send в CALLER_A topic (100)
    assert len(mock_bot_api.sent) == 1
    sent = mock_bot_api.sent[0]
    assert sent["payload"]["message_thread_id"] == 100
    assert "Caller A" in sent["payload"]["text"]
    assert "$LAB" in sent["payload"]["text"]


# ---------------------------------------------------------------------------
# Test 3 — dedup threshold emits once
# ---------------------------------------------------------------------------

async def test_dedup_threshold_emits_once(pg_pool, redis_client, mock_bot_api):
    """3 одинаковых ARB сигнала за окно → один send (при n=threshold=2 для arb)."""
    from services.private_mirror_dedup import update_bucket

    now = int(datetime.utcnow().timestamp())

    # 3 разных source'а с одним и тем же fingerprint (arb:WIF)
    sources = ["Example Source/Low", "Example Source/Funding", "Example Source/FLASH FUTURES"]
    for i, src in enumerate(sources):
        await update_bucket(
            fingerprint="arb:WIF",
            category="ARB",
            asset="WIF",
            asset_chain="cex",
            source_label=src,
            event_ts=now + i * 10,
            threshold=2,
            window_sec=90,
        )

    # Должен быть ровно 1 send (на n=2) + 1 edit (на n=3)
    # send_to_topic вызывается через _bot_api → один в mock.sent
    assert len(mock_bot_api.sent) == 1
    sent_payload = mock_bot_api.sent[0]["payload"]
    assert sent_payload["message_thread_id"] == 110  # ARB topic
    assert "$WIF" in sent_payload["text"]
    # Третий source должен пройти edit (через requests.post мок)
    assert len(mock_bot_api.edits) == 1

    # Проверим PG row создан
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT n_sources, leader_source, asset FROM mirror_merged_signals WHERE fingerprint = $1",
            "arb:WIF",
        )
    assert row is not None
    assert row["asset"] == "WIF"
    assert row["leader_source"] == "Example Source/Low"
    assert row["n_sources"] == 3


# ---------------------------------------------------------------------------
# Test 4 — below threshold: no emission
# ---------------------------------------------------------------------------

async def test_dedup_below_threshold_no_emit(pg_pool, redis_client, mock_bot_api):
    """1 spread с threshold=2 → НЕТ emit, PG row не создан, telegram_msg_id=None."""
    from services.private_mirror_dedup import update_bucket

    now = int(datetime.utcnow().timestamp())
    await update_bucket(
        fingerprint="arb:SOLO",
        category="ARB",
        asset="SOLO",
        asset_chain="cex",
        source_label="Example Source/Low",
        event_ts=now,
        threshold=2,
        window_sec=90,
    )

    assert mock_bot_api.sent == []
    assert mock_bot_api.edits == []

    # PG row отсутствует (row создаётся только при первой эмиссии)
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM mirror_merged_signals WHERE fingerprint = $1",
            "arb:SOLO",
        )
    assert row is None


# ---------------------------------------------------------------------------
# Test 5 — window close locks the message
# ---------------------------------------------------------------------------

async def test_window_close_locks_message(pg_pool, redis_client, mock_bot_api):
    """После close_window: emitted_locked=TRUE в PG, последний edit с ✅."""
    from services.private_mirror_dedup import update_bucket, close_window

    now = int(datetime.utcnow().timestamp())
    # Достигаем threshold
    await update_bucket("arb:LOCK", "ARB", "LOCK", "cex", "Example Source/Low",     now,      2, 90)
    await update_bucket("arb:LOCK", "ARB", "LOCK", "cex", "Example Source",    now + 5,  2, 90)

    # Сейчас в PG row уже есть, emitted_locked=FALSE
    async with pg_pool.acquire() as conn:
        before = await conn.fetchrow(
            "SELECT emitted_locked, window_closed_at FROM mirror_merged_signals WHERE fingerprint = $1",
            "arb:LOCK",
        )
    assert before["emitted_locked"] is False
    assert before["window_closed_at"] is None

    # Принудительно закрываем окно
    await close_window("arb:LOCK")

    # После close: PG обновлён
    async with pg_pool.acquire() as conn:
        after = await conn.fetchrow(
            "SELECT emitted_locked, window_closed_at, n_sources FROM mirror_merged_signals WHERE fingerprint = $1",
            "arb:LOCK",
        )
    assert after["emitted_locked"] is True
    assert after["window_closed_at"] is not None

    # Mock edits должен содержать ✅ в финальном тексте
    assert len(mock_bot_api.edits) >= 1
    last_edit_text = mock_bot_api.edits[-1]["payload"]["text"]
    assert "✅" in last_edit_text


# ---------------------------------------------------------------------------
# Test 6 — late echo doesn't edit
# ---------------------------------------------------------------------------

async def test_late_echo_no_edit(pg_pool, redis_client, mock_bot_api):
    """После lock — новый source инкрементит late_echo_count, в TG ничего."""
    from services.private_mirror_dedup import update_bucket, close_window

    now = int(datetime.utcnow().timestamp())
    # Set up: 2 sources → emitted
    await update_bucket("arb:LATE", "ARB", "LATE", "cex", "Example Source/Low",    now,     2, 90)
    await update_bucket("arb:LATE", "ARB", "LATE", "cex", "Example Source",   now + 5, 2, 90)

    # Lock window
    await close_window("arb:LATE")

    edits_before_late = len(mock_bot_api.edits)

    # Late echo (новый source ПОСЛЕ lock)
    await update_bucket("arb:LATE", "ARB", "LATE", "cex", "Example Source/FLASH",  now + 200, 2, 90)

    # В TG не должно быть новых edits после lock
    edits_after_late = len(mock_bot_api.edits)
    assert edits_after_late == edits_before_late, "Late echo не должно вызывать edit"

    # В PG late_echo_count должен инкрементироваться
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT late_echo_count FROM mirror_merged_signals WHERE fingerprint = $1",
            "arb:LATE",
        )
    assert row["late_echo_count"] == 1


# ---------------------------------------------------------------------------
# Test 7 — source_account filter in digest
# ---------------------------------------------------------------------------

async def test_source_account_filter_in_digest(pg_pool):
    """digest читает channel_messages WHERE source_account='private_mirror' только."""
    from services.private_mirror_digest import get_data_for_period

    now = datetime.utcnow()
    start = now - timedelta(hours=1)
    end = now + timedelta(hours=1)

    async with pg_pool.acquire() as conn:
        # Вставляем 2 row'а: один main (от telegram_monitor), один private_mirror
        await conn.execute(
            """
            INSERT INTO channel_messages
                (source, source_account, channel_id, channel_name, message_id,
                 author_name, text, has_media, urls, extracted_tickers, extracted_cas, message_date)
            VALUES
                ('telegram', 'main',           1, 'Public channel',  100, '@public', 'Public msg $BTC',
                 FALSE, ARRAY[]::text[], ARRAY['BTC']::text[], ARRAY[]::text[], $1),
                ('telegram', 'private_mirror', 2, 'Mirror Privatka', 101, '@mirror', 'Mirror msg $LAB',
                 FALSE, ARRAY[]::text[], ARRAY['LAB']::text[], ARRAY[]::text[], $1)
            """,
            now,
        )

    data = await get_data_for_period(start, end)

    # Должен быть ровно 1 raw_msg (private_mirror), не 2
    assert len(data["raw_msgs"]) == 1
    assert data["raw_msgs"][0]["channel_name"] == "Mirror Privatka"
    assert "LAB" in data["raw_msgs"][0]["extracted_tickers"]

    # Не должно быть упоминания $BTC ни в одном raw_msg
    for r in data["raw_msgs"]:
        assert "BTC" not in (r.get("extracted_tickers") or [])
