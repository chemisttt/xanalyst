"""
Integration tests для CatchMint Mint Radar (spec 003 validation gate).

14 tests covering:
  - core burst detection (test_*_signal* via shared.catchmint_signals)
  - safety/red-flag scorer (test_safety_*)
  - radar service orchestration with mocked HTTP

Запуск:
  createdb xanalyst_test    # once
  pip install pytest pytest-asyncio aioresponses asyncpg redis
  pytest tests/test_catchmint_radar.py -v
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio


pytestmark = pytest.mark.asyncio


# Default cfg for safety tests (no I/O needed)
def _cfg(**overrides):
    base = dict(
        catchmint_skip_on_honeypot=True,
        catchmint_skip_on_drain=True,
        catchmint_skip_on_scam=False,
        catchmint_skip_on_notable_flag=False,
        catchmint_skip_on_hide_ratio=0.10,
        catchmint_warn_on_fresh_deploy_minutes=30,
        catchmint_warn_on_proxy=True,
        catchmint_min_mints_in_window=15,
        catchmint_window_sec=600,
        catchmint_require_verified=True,
        catchmint_require_simulation_pass=False,   # после Gate E v2 default
        catchmint_max_supply_fraction=0.95,
        catchmint_enrich_enabled=True,
        catchmint_topic_id=7936,
        catchmint_overview_poll_sec=60,
        catchmint_cooldown_hours=4,
        catchmint_api_base="https://api.catchmint.xyz",
        catchmint_user_agent="test-ua",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


_NOW = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)


def _overview_row(addr="0x" + "a" * 40, name="Test", total_counts=200,
                  verified=True, sim=True, max_s=10000, total_s=500):
    """After Gate E v2: catchmint endpoint ?window=<sec> → totalCounts = mints за окно.
    Spec 003 v2 не использует counts[] для gate logic, только totalCounts."""
    return {
        "address": addr,
        "name": name,
        "chain": "Ethereum",
        "counts": [0, 0, 0, 0, total_counts, 0],   # cosmetic (catchmint ВСЕГДА 6 buckets)
        "totalCounts": total_counts,
        "totalSupply": total_s,
        "maxSupply": max_s,
        "isVerified": verified,
        "simulationPassed": sim,
        "imageUrl": "",
    }


def _detail_clean(addr="0x" + "a" * 40):
    return {
        "address": addr,
        "deployedAt": "2026-05-16T00:00:00.000000Z",  # 36h ago - не fresh
        "deployer": "0x" + "d" * 40,
        "isProxy": False,
        "flagCount": 0,
        "notableFlagCount": 0,
        "hideCount": 0,
        "uniqueWallets": 100,
        "twitterUrl": "",
        "discordUrl": "",
        "websiteUrl": "",
        "firstMint": "2026-05-16T00:10:00.000000Z",
        "implementationAddress": "",
    }


# ─────────────────── Burst signal tests (no PG) ───────────────────


def test_signal_no_data():
    from shared.catchmint_signals import evaluate_collection
    cfg = _cfg()
    d = evaluate_collection({}, cfg)   # отсутствует totalCounts
    assert d.fires is False and d.reason == "no_data"


def test_signal_below_min():
    from shared.catchmint_signals import evaluate_collection
    cfg = _cfg()   # min_mints_in_window=15 по дефолту
    row = _overview_row(total_counts=10)
    d = evaluate_collection(row, cfg)
    assert d.fires is False and d.reason == "below_min"


def test_signal_above_min_fires():
    """totalCounts >= min → fires (если verified + simulation + not sold-out)."""
    from shared.catchmint_signals import evaluate_collection
    cfg = _cfg()
    row = _overview_row(total_counts=200)
    d = evaluate_collection(row, cfg)
    assert d.fires is True and d.reason == "ok"
    assert d.mints_in_window == 200


def test_signal_not_verified():
    from shared.catchmint_signals import evaluate_collection
    cfg = _cfg()
    row = _overview_row(verified=False)
    d = evaluate_collection(row, cfg)
    assert d.fires is False and d.reason == "not_verified"


def test_signal_simulation_failed():
    """simulationPassed=False blocks ONLY when require_simulation_pass=True."""
    from shared.catchmint_signals import evaluate_collection
    cfg = _cfg(catchmint_require_simulation_pass=True)
    row = _overview_row(sim=False)
    d = evaluate_collection(row, cfg)
    assert d.fires is False and d.reason == "simulation_failed"
    # Default (require=False) → fires
    d2 = evaluate_collection(row, _cfg())
    assert d2.fires is True


def test_signal_sold_out():
    from shared.catchmint_signals import evaluate_collection
    cfg = _cfg()
    row = _overview_row(total_s=9999, max_s=10000)
    d = evaluate_collection(row, cfg)
    assert d.fires is False and d.reason == "near_sold_out"


# ─────────────────── Safety scorer tests ───────────────────


def test_safety_clean():
    from shared.catchmint_safety import evaluate_safety
    v = evaluate_safety(_detail_clean(), [], _cfg(), _NOW)
    assert v.severity == "safe" and not v.skip and not v.badges


def test_safety_honeypot_skip():
    from shared.catchmint_safety import evaluate_safety
    v = evaluate_safety(_detail_clean(), [{"label": "Honeypot", "count": 1}], _cfg(), _NOW)
    assert v.severity == "danger" and v.skip


def test_safety_drain_skip():
    from shared.catchmint_safety import evaluate_safety
    v = evaluate_safety(_detail_clean(), [{"label": "Drain", "count": 2}], _cfg(), _NOW)
    assert v.severity == "danger" and v.skip


def test_safety_scam_warn_default():
    """SCAM default: warn-badge, NOT skip."""
    from shared.catchmint_safety import evaluate_safety
    v = evaluate_safety(_detail_clean(), [{"label": "Scam", "count": 3}], _cfg(), _NOW)
    assert v.severity == "warn" and not v.skip
    assert any("SCAM" in b for b in v.badges)


def test_safety_scam_skip_when_enabled():
    from shared.catchmint_safety import evaluate_safety
    v = evaluate_safety(_detail_clean(), [{"label": "Scam", "count": 3}],
                        _cfg(catchmint_skip_on_scam=True), _NOW)
    assert v.skip and v.severity == "danger"


def test_safety_notable_flag_warn_default():
    """M3 fix: notable_flag default warn, not skip."""
    from shared.catchmint_safety import evaluate_safety
    d = _detail_clean(); d["notableFlagCount"] = 1
    v = evaluate_safety(d, [], _cfg(), _NOW)
    assert v.severity == "warn" and not v.skip
    assert any("notable" in b.lower() for b in v.badges)


def test_safety_hide_ratio_skip():
    from shared.catchmint_safety import evaluate_safety
    d = _detail_clean(); d["hideCount"] = 15; d["uniqueWallets"] = 100  # 15%
    v = evaluate_safety(d, [], _cfg(), _NOW)
    assert v.skip


def test_safety_hide_ratio_low_sample():
    """uniqueWallets<50 → hide ratio check disabled (статистика недостаточная)."""
    from shared.catchmint_safety import evaluate_safety
    d = _detail_clean(); d["hideCount"] = 15; d["uniqueWallets"] = 30
    v = evaluate_safety(d, [], _cfg(), _NOW)
    assert not v.skip


def test_safety_fresh_deploy_warn():
    from shared.catchmint_safety import evaluate_safety
    d = _detail_clean(); d["deployedAt"] = "2026-05-17T11:50:00.000000Z"  # 10 min ago
    v = evaluate_safety(d, [], _cfg(), _NOW)
    assert v.severity == "warn"
    assert any("fresh" in b.lower() for b in v.badges)


def test_safety_proxy_warn():
    from shared.catchmint_safety import evaluate_safety
    d = _detail_clean(); d["isProxy"] = True
    v = evaluate_safety(d, [], _cfg(), _NOW)
    assert v.severity == "warn"
    assert any("proxy" in b.lower() for b in v.badges)


def test_safety_unknown_label_ignored():
    """Unknown flag label (не в FLAG_LABELS) — ignored, не падает."""
    from shared.catchmint_safety import evaluate_safety
    v = evaluate_safety(_detail_clean(), [{"label": "WeirdNew", "count": 1}], _cfg(), _NOW)
    assert v.severity == "safe" and not v.skip


def test_safety_combined_severity():
    """proxy + fresh deploy + SCAM → warn (HONEYPOT escalates to danger+skip)."""
    from shared.catchmint_safety import evaluate_safety
    d = _detail_clean()
    d["isProxy"] = True; d["deployedAt"] = "2026-05-17T11:50:00.000000Z"
    v = evaluate_safety(d, [{"label": "Scam", "count": 2}], _cfg(), _NOW)
    assert v.severity == "warn" and not v.skip
    v2 = evaluate_safety(d, [{"label": "Scam", "count": 2}, {"label": "Honeypot", "count": 1}], _cfg(), _NOW)
    assert v2.severity == "danger" and v2.skip


# ─────────────────── Radar service integration ───────────────────


@pytest_asyncio.fixture
async def catchmint_schema(pg_pool):
    """Ensure catchmint_alerts table exists + truncate."""
    async with pg_pool.acquire() as conn:
        await conn.execute("TRUNCATE catchmint_alerts RESTART IDENTITY")
    return pg_pool


class _StubClient:
    """Mock CatchmintClient — returns canned responses, tracks calls."""
    def __init__(self, overview=(), detail=None, flags=None,
                 detail_exc=None, flags_exc=None):
        self._overview = list(overview)
        self._detail = detail or {}
        self._flags = flags or []
        self._detail_exc = detail_exc
        self._flags_exc = flags_exc
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        pass

    async def get_overview(self):
        self.calls.append("overview")
        return list(self._overview)

    async def get_contract_detail(self, addr):
        self.calls.append(f"detail:{addr}")
        if self._detail_exc:
            raise self._detail_exc
        return dict(self._detail)

    async def get_contract_flags(self, addr):
        self.calls.append(f"flags:{addr}")
        if self._flags_exc:
            raise self._flags_exc
        return list(self._flags)


async def test_overview_to_alert_happy_path(catchmint_schema, redis_client, mock_bot_api, monkeypatch):
    """1. feed с 1 burst коллекцией → 1 INSERT, 1 send_to_topic via asyncio.to_thread."""
    from services.catchmint_radar import process_overview_cycle
    from shared import config as cfg_mod

    monkeypatch.setattr(cfg_mod.settings, "catchmint_topic_id", 7936, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_min_mints_in_window", 15, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_window_sec", 600, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_require_verified", True, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_require_simulation_pass", True, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_max_supply_fraction", 0.95, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_enrich_enabled", True, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_cooldown_hours", 4, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_skip_on_honeypot", True, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_skip_on_drain", True, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_skip_on_scam", False, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_skip_on_notable_flag", False, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_skip_on_hide_ratio", 0.10, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_warn_on_fresh_deploy_minutes", 30, raising=False)
    monkeypatch.setattr(cfg_mod.settings, "catchmint_warn_on_proxy", True, raising=False)

    row = _overview_row(addr="0x" + "1" * 40, total_counts=200)
    client = _StubClient(overview=[row], detail=_detail_clean("0x" + "1" * 40), flags=[])
    active = {}

    await process_overview_cycle(client, catchmint_schema, redis_client, active)

    # PG row created
    async with catchmint_schema.acquire() as conn:
        rows = await conn.fetch("SELECT address, severity, telegram_msg_id FROM catchmint_alerts")
    assert len(rows) == 1
    assert rows[0]["address"] == "0x" + "1" * 40
    assert rows[0]["severity"] == "safe"
    assert rows[0]["telegram_msg_id"] is not None

    # TG send called
    assert len(mock_bot_api.sent) == 1
    payload = mock_bot_api.sent[0]["payload"]
    assert payload["message_thread_id"] == 7936
    assert "Mint Burst" in payload["text"]

    # ACTIVE populated
    assert "0x" + "1" * 40 in active


async def test_skipped_row_no_cooldown(catchmint_schema, redis_client, mock_bot_api, monkeypatch):
    """M7-e + C3: HONEYPOT flag → skip → INSERT с emitted_locked=TRUE, msg_id IS NULL, БЕЗ cooldown."""
    from services.catchmint_radar import process_overview_cycle
    from shared import config as cfg_mod
    for k, v in dict(catchmint_topic_id=7936, catchmint_min_mints_in_window=15,
                     catchmint_window_sec=600, catchmint_require_verified=True,
                     catchmint_require_simulation_pass=True, catchmint_max_supply_fraction=0.95,
                     catchmint_enrich_enabled=True, catchmint_cooldown_hours=4,
                     catchmint_skip_on_honeypot=True, catchmint_skip_on_drain=True,
                     catchmint_skip_on_scam=False, catchmint_skip_on_notable_flag=False,
                     catchmint_skip_on_hide_ratio=0.10, catchmint_warn_on_fresh_deploy_minutes=30,
                     catchmint_warn_on_proxy=True).items():
        monkeypatch.setattr(cfg_mod.settings, k, v, raising=False)

    addr = "0x" + "2" * 40
    row = _overview_row(addr=addr, total_counts=300)
    client = _StubClient(overview=[row], detail=_detail_clean(addr),
                         flags=[{"label": "Honeypot", "count": 1}])
    active = {}

    await process_overview_cycle(client, catchmint_schema, redis_client, active)

    async with catchmint_schema.acquire() as conn:
        r = await conn.fetchrow("SELECT severity, emitted_locked, telegram_msg_id, closed_at, skip_reasons FROM catchmint_alerts WHERE address = $1", addr)
    assert r["severity"] == "danger"
    assert r["emitted_locked"] is True
    assert r["telegram_msg_id"] is None
    assert r["closed_at"] is not None
    assert "HONEYPOT" in (r["skip_reasons"] or "")
    # No cooldown set
    assert await redis_client.get(f"catchmint:cooldown:{addr}") is None
    # No TG message sent
    assert len(mock_bot_api.sent) == 0
    assert addr not in active


async def test_enrichment_fail_open(catchmint_schema, redis_client, mock_bot_api, monkeypatch):
    """M7-a: detail+flags оба raise → severity='safe', fire всё равно происходит. severity NOT NULL."""
    from services.catchmint_radar import process_overview_cycle
    from shared import config as cfg_mod
    for k, v in dict(catchmint_topic_id=7936, catchmint_min_mints_in_window=15,
                     catchmint_window_sec=600, catchmint_require_verified=True,
                     catchmint_require_simulation_pass=True, catchmint_max_supply_fraction=0.95,
                     catchmint_enrich_enabled=True, catchmint_cooldown_hours=4,
                     catchmint_skip_on_honeypot=True, catchmint_skip_on_drain=True,
                     catchmint_skip_on_scam=False, catchmint_skip_on_notable_flag=False,
                     catchmint_skip_on_hide_ratio=0.10, catchmint_warn_on_fresh_deploy_minutes=30,
                     catchmint_warn_on_proxy=True).items():
        monkeypatch.setattr(cfg_mod.settings, k, v, raising=False)

    addr = "0x" + "3" * 40
    row = _overview_row(addr=addr, total_counts=200)
    client = _StubClient(overview=[row],
                         detail_exc=RuntimeError("502 from detail"),
                         flags_exc=RuntimeError("502 from flags"))
    active = {}

    await process_overview_cycle(client, catchmint_schema, redis_client, active)

    async with catchmint_schema.acquire() as conn:
        r = await conn.fetchrow("SELECT severity, telegram_msg_id FROM catchmint_alerts WHERE address = $1", addr)
    assert r is not None
    assert r["severity"] == "safe"  # M4: NOT NULL DEFAULT, fail-open keeps safe
    assert r["telegram_msg_id"] is not None
    assert len(mock_bot_api.sent) == 1


async def test_cooldown_blocks_re_emit(catchmint_schema, redis_client, mock_bot_api, monkeypatch):
    """cycle 1: fire + SETNX. Закрыть окно (manually set closed_at). cycle 2: cooldown ещё active → no re-emit."""
    from services.catchmint_radar import process_overview_cycle
    from shared import config as cfg_mod
    for k, v in dict(catchmint_topic_id=7936, catchmint_min_mints_in_window=15,
                     catchmint_window_sec=600, catchmint_require_verified=True,
                     catchmint_require_simulation_pass=True, catchmint_max_supply_fraction=0.95,
                     catchmint_enrich_enabled=True, catchmint_cooldown_hours=4,
                     catchmint_skip_on_honeypot=True, catchmint_skip_on_drain=True,
                     catchmint_skip_on_scam=False, catchmint_skip_on_notable_flag=False,
                     catchmint_skip_on_hide_ratio=0.10, catchmint_warn_on_fresh_deploy_minutes=30,
                     catchmint_warn_on_proxy=True).items():
        monkeypatch.setattr(cfg_mod.settings, k, v, raising=False)

    addr = "0x" + "4" * 40
    row = _overview_row(addr=addr, total_counts=200)
    client = _StubClient(overview=[row], detail=_detail_clean(addr), flags=[])
    active = {}

    await process_overview_cycle(client, catchmint_schema, redis_client, active)
    assert len(mock_bot_api.sent) == 1

    # Simulate window-close manually
    async with catchmint_schema.acquire() as conn:
        await conn.execute("UPDATE catchmint_alerts SET closed_at = NOW(), emitted_locked = TRUE WHERE address = $1", addr)
    active.clear()  # как будто перезапустились после close

    # New burst on same addr — cooldown still active
    await process_overview_cycle(client, catchmint_schema, redis_client, active)
    # Only the original send, no second
    assert len(mock_bot_api.sent) == 1
    # No new alerts inserted
    async with catchmint_schema.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM catchmint_alerts WHERE address = $1", addr)
    assert count == 1


async def test_window_close_with_3_miss_grace(catchmint_schema, redis_client, mock_bot_api, monkeypatch):
    """M2: 1-2 misses → НЕ closed. 3 misses → closed."""
    from services.catchmint_radar import process_overview_cycle
    from shared import config as cfg_mod
    for k, v in dict(catchmint_topic_id=7936, catchmint_min_mints_in_window=15,
                     catchmint_window_sec=600, catchmint_require_verified=True,
                     catchmint_require_simulation_pass=True, catchmint_max_supply_fraction=0.95,
                     catchmint_enrich_enabled=True, catchmint_cooldown_hours=4,
                     catchmint_skip_on_honeypot=True, catchmint_skip_on_drain=True,
                     catchmint_skip_on_scam=False, catchmint_skip_on_notable_flag=False,
                     catchmint_skip_on_hide_ratio=0.10, catchmint_warn_on_fresh_deploy_minutes=30,
                     catchmint_warn_on_proxy=True).items():
        monkeypatch.setattr(cfg_mod.settings, k, v, raising=False)

    addr = "0x" + "5" * 40
    row = _overview_row(addr=addr, total_counts=200)
    other = _overview_row(addr="0x" + "9" * 40, total_counts=0)  # below_min, doesn't fire
    client_with = _StubClient(overview=[row], detail=_detail_clean(addr), flags=[])
    client_without = _StubClient(overview=[other])
    active = {}

    # cycle 1: fire
    await process_overview_cycle(client_with, catchmint_schema, redis_client, active)
    assert addr in active
    assert active[addr]["miss_count"] == 0

    # cycle 2: addr not in overview → miss=1, NOT closed
    await process_overview_cycle(client_without, catchmint_schema, redis_client, active)
    assert addr in active and active[addr]["miss_count"] == 1
    async with catchmint_schema.acquire() as conn:
        ca = await conn.fetchval("SELECT closed_at FROM catchmint_alerts WHERE address = $1", addr)
    assert ca is None

    # cycle 3: miss=2, NOT closed
    await process_overview_cycle(client_without, catchmint_schema, redis_client, active)
    assert active[addr]["miss_count"] == 2
    async with catchmint_schema.acquire() as conn:
        ca = await conn.fetchval("SELECT closed_at FROM catchmint_alerts WHERE address = $1", addr)
    assert ca is None

    # cycle 4: miss=3 → CLOSE
    await process_overview_cycle(client_without, catchmint_schema, redis_client, active)
    assert addr not in active
    async with catchmint_schema.acquire() as conn:
        ca = await conn.fetchval("SELECT closed_at FROM catchmint_alerts WHERE address = $1", addr)
    assert ca is not None


async def test_orphan_recovery_on_boot(catchmint_schema, redis_client, monkeypatch):
    """M7-c: PG has 2 closed_at IS NULL rows. hydrate_active_state → ACTIVE содержит оба."""
    from services.catchmint_radar import hydrate_active_state

    addr1 = "0x" + "a" * 40
    addr2 = "0x" + "b" * 40
    async with catchmint_schema.acquire() as conn:
        for a, mid in [(addr1, 42), (addr2, 43)]:
            await conn.execute(
                """
                INSERT INTO catchmint_alerts (
                    address, chain, name, first_bucket_count, first_total_counts,
                    is_verified, simulation_passed, peak_bucket_count,
                    last_bucket_count, last_total_counts, severity,
                    telegram_msg_id, first_seen, last_updated
                ) VALUES ($1, 'Ethereum', 'Test', 100, 200, TRUE, TRUE, 100, 100, 200, 'safe', $2, NOW(), NOW())
                """,
                a, mid,
            )

    active = await hydrate_active_state(catchmint_schema)
    assert addr1 in active and addr2 in active
    assert active[addr1]["msg_id"] == 42
    assert active[addr2]["msg_id"] == 43
    assert active[addr1]["miss_count"] == 0


async def test_unknown_label_in_flags_doesnt_crash(catchmint_schema, redis_client, mock_bot_api, monkeypatch):
    """Catchmint adds new flag label → не падаем."""
    from services.catchmint_radar import process_overview_cycle
    from shared import config as cfg_mod
    for k, v in dict(catchmint_topic_id=7936, catchmint_min_mints_in_window=15,
                     catchmint_window_sec=600, catchmint_require_verified=True,
                     catchmint_require_simulation_pass=True, catchmint_max_supply_fraction=0.95,
                     catchmint_enrich_enabled=True, catchmint_cooldown_hours=4,
                     catchmint_skip_on_honeypot=True, catchmint_skip_on_drain=True,
                     catchmint_skip_on_scam=False, catchmint_skip_on_notable_flag=False,
                     catchmint_skip_on_hide_ratio=0.10, catchmint_warn_on_fresh_deploy_minutes=30,
                     catchmint_warn_on_proxy=True).items():
        monkeypatch.setattr(cfg_mod.settings, k, v, raising=False)

    addr = "0x" + "c" * 40
    row = _overview_row(addr=addr, total_counts=200)
    client = _StubClient(overview=[row], detail=_detail_clean(addr),
                         flags=[{"label": "MysteryNew", "count": 7}])
    active = {}

    await process_overview_cycle(client, catchmint_schema, redis_client, active)
    # Should have fired (unknown labels ignored)
    assert len(mock_bot_api.sent) == 1
