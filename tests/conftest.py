"""
pytest fixtures для xanalyst integration tests.

Стратегия:
- Postgres: DATABASE_TEST_URL env-var (default postgresql://xanalyst:password@localhost:5432/xanalyst_test).
  Создание DB вручную через `createdb xanalyst_test` либо pytest-postgresql если установлен.
  Schema загружается из scripts/init_db.sql в начале сессии.
- Redis: db=15 (изолированный keyspace), flushdb в начале и в конце каждого теста.
- Bot API mock: monkeypatch shared.notifier._bot_api для inspection без реальных HTTP-запросов.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

# Path setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Force test DB url BEFORE importing shared.config
os.environ.setdefault(
    "DATABASE_URL",
    os.getenv("DATABASE_TEST_URL", "postgresql://xanalyst:password@localhost:5432/xanalyst_test"),
)
os.environ.setdefault("REDIS_URL", os.getenv("REDIS_TEST_URL", "redis://localhost:6379/15"))

# pytest-asyncio mode
pytest_plugins = []


# ---------------------------------------------------------------------------
# DB schema setup once per session
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def _db_schema():
    """Применяет init_db.sql к test-DB в начале сессии."""
    import asyncpg

    from shared.config import settings

    schema_sql = (PROJECT_ROOT / "scripts" / "init_db.sql").read_text()
    try:
        conn = await asyncpg.connect(settings.database_url)
    except Exception as e:
        pytest.skip(f"Не могу подключиться к test DB ({settings.database_url}): {e}. "
                    "Создай локально: `createdb xanalyst_test`")
        return
    try:
        await conn.execute(schema_sql)
    finally:
        await conn.close()
    yield


@pytest_asyncio.fixture
async def pg_pool(_db_schema):
    """asyncpg pool для теста + truncate всех таблиц до/после."""
    from shared.db import init_db, close_db, get_pool

    await init_db()
    pool = get_pool()

    # Truncate before
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE channel_messages, mirror_merged_signals, mirror_digests, "
            "twitter_analyses, twitter_watchlist, "
            "alpha_signal_scores, alpha_events, "
            # spec 004 ct_digest tables (per ADR 0002 R-T1 — must include for cross_ref tests)
            "ct_feedback, ct_promises, ct_digest_items, ct_digest_ticks, ct_dev_credibility, "
            "catchmint_alerts "
            "RESTART IDENTITY"
        )
    yield pool
    # Truncate after
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "TRUNCATE channel_messages, mirror_merged_signals, mirror_digests, "
                "twitter_analyses, twitter_watchlist, "
                "ct_feedback, ct_promises, ct_digest_items, ct_digest_ticks, ct_dev_credibility, "
                "catchmint_alerts "
                "RESTART IDENTITY"
            )
    finally:
        await close_db()


# ---------------------------------------------------------------------------
# Redis fixture
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def redis_client():
    """Redis на db=15 с flushdb до/после теста."""
    from shared.redis_client import init_redis, close_redis, get_redis

    await init_redis()
    r = get_redis()
    await r.flushdb()
    yield r
    try:
        await r.flushdb()
    finally:
        await close_redis()


# ---------------------------------------------------------------------------
# Notifier mock — capture sent/edited messages without real HTTP
# ---------------------------------------------------------------------------

class MockBotAPI:
    """Подменяет _bot_api: захватывает все вызовы для assert'ов."""

    def __init__(self):
        self.sent: list[dict] = []
        self.edits: list[dict] = []
        self.next_message_id = 1000  # monotonic incrementing

    def __call__(self, method: str, payload: dict) -> dict | None:
        if method == "sendMessage":
            msg_id = self.next_message_id
            self.next_message_id += 1
            self.sent.append({"method": method, "payload": payload, "message_id": msg_id})
            return {"message_id": msg_id}
        return None


@pytest.fixture
def mock_bot_api(monkeypatch):
    """Подменяет shared.notifier._bot_api на MockBotAPI."""
    from shared import notifier as nmod

    mock = MockBotAPI()
    monkeypatch.setattr(nmod, "_bot_api", mock)

    # Также подменим editMessageText через requests
    def fake_requests_post(url, json=None, timeout=None, **kwargs):
        class FakeResp:
            def json(self_):
                if "editMessageText" in url:
                    mock.edits.append({"url": url, "payload": json})
                    return {"ok": True, "result": {"message_id": json.get("message_id")}}
                return {"ok": False}
        return FakeResp()

    monkeypatch.setattr("shared.notifier.requests.post", fake_requests_post)
    return mock


# ---------------------------------------------------------------------------
# Bot token shim — нужен для notifier чтобы не skip-ать по missing token
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _bot_token_shim(monkeypatch):
    """Гарантируем, что settings.telegram_bot_token и chat_id заданы для тестов."""
    from shared import config

    monkeypatch.setattr(config.settings, "telegram_bot_token", "test_token", raising=False)
    monkeypatch.setattr(config.settings, "telegram_notify_chat_id", "-1001234567890", raising=False)
    # Mirror topic IDs (positive int values for resolution)
    monkeypatch.setattr(config.settings, "telegram_mirror_caller_a_topic_id",     "100", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_caller_b_topic_id",     "101", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_community_topic_id", "102", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_arb_topic_id",      "110", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_pump_topic_id",     "111", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_meme_topic_id",     "112", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_whale_topic_id",    "113", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_digest_topic_id",   "120", raising=False)
    monkeypatch.setattr(config.settings, "telegram_mirror_syshealth_topic_id","199", raising=False)
