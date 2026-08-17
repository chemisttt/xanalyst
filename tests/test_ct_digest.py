"""
Spec 004 Task 8: unit + integration tests для CT Alpha Digest V1.

8 test classes covering rev-2 + rev-3 falsify concerns:
1. TestClassifierMock — mock AnthropicClient, schema validity, cost_redis_key kwarg
2. TestPromiseExtractor — regex + EIP-55 checksum + deferred_check threshold
3. TestFeedbackParser — short_id regex + LLM mock
4. TestCrossRefQueries — PG fixtures с real asset format (WIF no $)
5. TestAstroturfJaccard — 4 boundary cases
6. TestPromiseCronDeferred — deferred_check resolution path (ADR R-T critical)
7. TestCollectorAsync — mock create_subprocess_exec + JSON parsing
8. TestCallbackParse — handle_ct_digest_callback malformed payload guard
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

pytestmark = pytest.mark.asyncio

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "ct_digest"


# ============================================================
# TestClassifierMock — schema + cost_redis_key assertion
# ============================================================

class TestClassifierMock:
    async def test_classify_batch_returns_valid_schema(self, monkeypatch):
        from services.ct_digest import classifier
        from services.ct_digest.collectors import RawItem

        # Make sure settings.ct_digest_paused=False, anthropic key set
        monkeypatch.setattr("shared.config.settings.ct_digest_paused", False, raising=False)
        monkeypatch.setattr("shared.config.settings.anthropic_api_key", "test-key", raising=False)

        raw_items = [
            RawItem(
                tweet_id="t1", url="http://x.com/a/1", author_handle="a",
                author_metadata={}, posted_at_iso="2026-05-19T12:00:00Z",
                text="public mint live now @testproject 0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84",
                source_type="x_vocab", source_query="public mint",
            ),
        ]

        mock_response = json.dumps([{
            "tweet_id": "t1",
            "bucket": "calendar_24h",
            "novelty_score": 0.6,
            "included": True,
            "tags": ["mint"],
            "mechanic_notes": "public mint announced",
            "contract_address_hint": "0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84",
            "promised_timestamp_iso": "2026-05-20T18:00:00Z",
            "collection_name_hint": "testproject",
        }])

        # Patch AnthropicClient.call to return canned response
        call_kwargs_captured = {}

        async def fake_call(self, system, messages, max_tokens=None, cost_redis_key=None):
            call_kwargs_captured["cost_redis_key"] = cost_redis_key
            call_kwargs_captured["system_starts"] = system[:50]
            return mock_response, {"cost_cents": 0.5, "usage": {}}

        monkeypatch.setattr(
            "shared.llm_client.AnthropicClient.call", fake_call,
        )
        # Prevent real AsyncAnthropic init
        monkeypatch.setattr(
            "shared.llm_client.AnthropicClient.__init__",
            lambda self, api_key, model="claude-sonnet-4-6", max_tokens=4096: None,
        )

        results = await classifier.classify_batch(raw_items)
        assert len(results) == 1
        assert results[0].bucket == "calendar_24h"
        assert 0 <= results[0].novelty_score <= 1
        assert results[0].included is True

        # ADR check: cost_redis_key must be passed and start с llm_cost:ct_digest:
        assert call_kwargs_captured.get("cost_redis_key", "").startswith("llm_cost:ct_digest:")

    async def test_paused_raises(self, monkeypatch):
        from services.ct_digest import classifier
        from services.ct_digest.collectors import RawItem

        monkeypatch.setattr("shared.config.settings.ct_digest_paused", True, raising=False)
        with pytest.raises(classifier.CTDigestPaused):
            await classifier.classify_batch([RawItem(
                tweet_id="t1", url="", author_handle="", author_metadata={},
                posted_at_iso="", text="x", source_type="x_vocab", source_query="",
            )])

    async def test_empty_items_returns_empty(self, monkeypatch):
        from services.ct_digest import classifier
        monkeypatch.setattr("shared.config.settings.ct_digest_paused", False, raising=False)
        result = await classifier.classify_batch([])
        assert result == []


# ============================================================
# TestPromiseExtractor — regex + EIP-55 + deferred_check
# ============================================================

class TestPromiseExtractor:
    def test_extract_lowercase_address_normalized_to_checksum(self):
        from services.ct_digest.promises import extract_contract_address

        # Real Lido address — known checksum form
        lowercase = "mint at 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 tomorrow"
        result = extract_contract_address(lowercase)
        assert result is not None
        # EIP-55 mixed-case (NOT all lowercase)
        assert result != result.lower()
        assert result.lower() == "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"

    def test_extract_invalid_hex_returns_none(self):
        from services.ct_digest.promises import extract_contract_address
        # Length-correct but invalid format edge — actually all 0-9a-f IS valid hex,
        # so we test the case of address not present
        assert extract_contract_address("no address here") is None
        # Address malformed (39 chars — 1 too few)
        short = "0xae7ab96520de3a18e5e111b5eaab095312d7fe8"
        assert extract_contract_address(short) is None

    def test_parse_promised_ts_iso_with_z(self):
        from services.ct_digest.promises import parse_promised_ts
        result = parse_promised_ts("2026-05-20T18:00:00Z")
        assert result is not None
        assert result.year == 2026 and result.month == 5 and result.day == 20

    def test_parse_promised_ts_invalid_returns_none(self):
        from services.ct_digest.promises import parse_promised_ts
        assert parse_promised_ts(None) is None
        assert parse_promised_ts("not-a-date") is None

    async def test_insert_promise_deferred_check_threshold(self, pg_pool, monkeypatch):
        """promised_ts > now + 48h → status='deferred_check'"""
        from services.ct_digest.promises import insert_promise
        from services.ct_digest.classifier import ClassifiedItem
        from services.ct_digest.collectors import RawItem

        # Insert parent item first
        tick_id = await pg_pool.fetchval(
            "INSERT INTO ct_digest_ticks (started_at, status) VALUES (NOW(), 'running') RETURNING tick_id"
        )
        item_id = await pg_pool.fetchval(
            """
            INSERT INTO ct_digest_items (tick_id, tweet_id, raw_text, included)
            VALUES ($1, 't1', '0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84 mint', true)
            RETURNING id
            """,
            tick_id,
        )

        # Far-future promised_ts → deferred_check
        far_future = (datetime.utcnow() + timedelta(days=5)).isoformat() + "Z"
        item_deferred = ClassifiedItem(
            tweet_id="t1", bucket="calendar_7d", novelty_score=0.5, included=True,
            tags=[], mechanic_notes="",
            contract_address_hint="0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84",
            promised_timestamp_iso=far_future, collection_name_hint=None,
            raw=RawItem(tweet_id="t1", url="", author_handle="a", author_metadata={},
                        posted_at_iso="", text="0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84 mint",
                        source_type="x_vocab", source_query=""),
        )
        promise_id = await insert_promise(pg_pool, item_deferred, item_id)
        assert promise_id is not None
        status = await pg_pool.fetchval("SELECT status FROM ct_promises WHERE id=$1", promise_id)
        assert status == "deferred_check"

        # Near-future promised_ts → announced
        item_id2 = await pg_pool.fetchval(
            """INSERT INTO ct_digest_items (tick_id, tweet_id, raw_text, included)
               VALUES ($1, 't2', 'mint 0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84', true)
               RETURNING id""",
            tick_id,
        )
        soon = (datetime.utcnow() + timedelta(hours=12)).isoformat() + "Z"
        item_soon = ClassifiedItem(
            tweet_id="t2", bucket="calendar_24h", novelty_score=0.5, included=True,
            tags=[], mechanic_notes="",
            contract_address_hint="0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84",
            promised_timestamp_iso=soon, collection_name_hint=None,
            raw=item_deferred.raw,
        )
        # Replace tweet_id для raw consistency
        item_soon.raw.text = "mint 0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84"
        pid2 = await insert_promise(pg_pool, item_soon, item_id2)
        s2 = await pg_pool.fetchval("SELECT status FROM ct_promises WHERE id=$1", pid2)
        assert s2 == "announced"


# ============================================================
# TestFeedbackParser
# ============================================================

class TestFeedbackParser:
    def test_short_id_regex_extracts_refs(self):
        from services.ct_digest.feedback import _extract_short_ids_regex
        ids = _extract_short_ids_regex("e3 был хорош, c1 пропусти, [k2] спам")
        assert "e3" in ids
        assert "c1" in ids
        assert "k2" in ids

    def test_short_id_regex_empty_text(self):
        from services.ct_digest.feedback import _extract_short_ids_regex
        assert _extract_short_ids_regex("") == []
        assert _extract_short_ids_regex("no refs here") == []

    def test_format_parsed_summary(self):
        from services.ct_digest.feedback import format_parsed_summary
        out = format_parsed_summary({"mechanic_pref": "bonding_curve"}, ["e1", "c2"])
        assert "refs=e1,c2" in out
        assert "mechanic_pref=bonding_curve" in out


# ============================================================
# TestCrossRefQueries — actual schema column verification
# ============================================================

class TestCrossRefQueries:
    async def test_twitter_watchlist_handle_match(self, pg_pool):
        from services.ct_digest.cross_ref import _build_cross_refs_for_item
        from services.ct_digest.classifier import ClassifiedItem
        from services.ct_digest.collectors import RawItem

        await pg_pool.execute(
            "INSERT INTO twitter_watchlist (handle, notes) VALUES ('wh7nft', 'curator')"
        )
        item = ClassifiedItem(
            tweet_id="t1", bucket="early_signals", novelty_score=0.5, included=True,
            tags=[], mechanic_notes="",
            contract_address_hint=None, promised_timestamp_iso=None,
            collection_name_hint=None,
            raw=RawItem(
                tweet_id="t1", url="", author_handle="other",
                author_metadata={}, posted_at_iso="",
                text="@wh7nft check this drop",
                source_type="x_vocab", source_query="",
            ),
        )
        refs = await _build_cross_refs_for_item(pg_pool, item)
        assert "twitter_watchlist" in refs
        assert "wh7nft" in refs["twitter_watchlist"]

    async def test_mirror_meme_asset_ticker_without_dollar(self, pg_pool):
        """ADR 0002 R-2: asset stored as 'WIF' not '$WIF' (per private_mirror_dedup.py:51-52)."""
        from services.ct_digest.cross_ref import _build_cross_refs_for_item
        from services.ct_digest.classifier import ClassifiedItem
        from services.ct_digest.collectors import RawItem

        # Insert mirror row WITH ticker без $ (как реально хранится)
        await pg_pool.execute(
            """
            INSERT INTO mirror_merged_signals
                (category, fingerprint, first_seen, last_seen, asset, n_sources)
            VALUES ('meme', 'fp_test_1', NOW(), NOW(), 'TESTCOIN', 3)
            """
        )

        item = ClassifiedItem(
            tweet_id="t1", bucket="early_signals", novelty_score=0.5, included=True,
            tags=[], mechanic_notes="",
            contract_address_hint=None, promised_timestamp_iso=None,
            collection_name_hint=None,
            raw=RawItem(
                tweet_id="t1", url="", author_handle="x",
                author_metadata={}, posted_at_iso="",
                # Tweet uses $-prefix, code должен strip перед join
                text="watching $TESTCOIN closely",
                source_type="x_vocab", source_query="",
            ),
        )
        refs = await _build_cross_refs_for_item(pg_pool, item)
        assert "mirror_meme" in refs, f"expected mirror_meme hit, got {refs}"
        assert any(r["asset"] == "TESTCOIN" for r in refs["mirror_meme"])

    async def test_catchmint_alerts_name_column(self, pg_pool):
        """ADR R-2: column is 'name' (NOT 'collection_name')."""
        from services.ct_digest.cross_ref import _build_cross_refs_for_item
        from services.ct_digest.classifier import ClassifiedItem
        from services.ct_digest.collectors import RawItem

        # Minimal catchmint_alerts row — full schema has many cols, fill required ones
        await pg_pool.execute(
            """
            INSERT INTO catchmint_alerts (
                address, chain, name, first_bucket_count, first_total_counts,
                is_verified, simulation_passed, peak_bucket_count,
                last_bucket_count, last_total_counts, severity,
                first_seen, last_updated, telegram_msg_id
            )
            VALUES ('0xabc...', 'ethereum', 'TestCollection',
                    10, 50, false, true, 15, 12, 80, 'safe',
                    NOW(), NOW(), 99999)
            """
        )
        item = ClassifiedItem(
            tweet_id="t1", bucket="calendar_24h", novelty_score=0.5, included=True,
            tags=[], mechanic_notes="",
            contract_address_hint=None, promised_timestamp_iso=None,
            collection_name_hint="TestCollection",
            raw=RawItem(tweet_id="t1", url="", author_handle="", author_metadata={},
                        posted_at_iso="", text="", source_type="x_vocab", source_query=""),
        )
        refs = await _build_cross_refs_for_item(pg_pool, item)
        assert "catchmint" in refs
        assert refs["catchmint"][0]["name"] == "TestCollection"
        assert refs["catchmint"][0]["telegram_msg_id"] == 99999


# ============================================================
# TestAstroturfJaccard — 4 boundary cases per spec
# ============================================================

class TestAstroturfJaccard:
    def _make_items(self, texts: list[str], authors: list[str] | None = None):
        from services.ct_digest.collectors import RawItem
        authors = authors or [f"a{i}" for i in range(len(texts))]
        return [
            RawItem(tweet_id=f"t{i}", url="", author_handle=authors[i],
                    author_metadata={}, posted_at_iso="", text=t,
                    source_type="x_vocab", source_query="")
            for i, t in enumerate(texts)
        ]

    def test_3_identical_posts_3_authors_flagged(self):
        from services.ct_digest.classifier import detect_astroturf_clusters
        items = self._make_items([
            "Awesome new mint @Project drops at 18:00 UTC don't miss it",
            "Awesome new mint @Project drops at 18:00 UTC don't miss it",
            "Awesome new mint @Project drops at 18:00 UTC don't miss it",
        ], authors=["a1", "a2", "a3"])
        flagged = detect_astroturf_clusters(items)
        assert len(flagged) == 3

    def test_3_dissimilar_posts_not_flagged(self):
        from services.ct_digest.classifier import detect_astroturf_clusters
        items = self._make_items([
            "Hey check this NFT mint",
            "Different project completely separate launch news",
            "Random crypto tweet about market conditions",
        ], authors=["a1", "a2", "a3"])
        flagged = detect_astroturf_clusters(items)
        assert len(flagged) == 0

    def test_2_authors_only_not_flagged(self):
        """Threshold = 3 distinct authors даже при Jaccard 1.0."""
        from services.ct_digest.classifier import detect_astroturf_clusters
        items = self._make_items([
            "Identical text about a mint exactly the same words",
            "Identical text about a mint exactly the same words",
        ], authors=["a1", "a2"])
        flagged = detect_astroturf_clusters(items)
        assert len(flagged) == 0

    def test_4_similar_posts_4_authors_flagged(self):
        from services.ct_digest.classifier import detect_astroturf_clusters
        base = "Big NFT drop happening soon don't miss the public mint window today"
        items = self._make_items([
            base,
            base + " extra",
            "Big NFT drop happening soon don't miss the mint window today!",
            base + "!!",
        ], authors=["a1", "a2", "a3", "a4"])
        flagged = detect_astroturf_clusters(items)
        assert len(flagged) >= 3


# ============================================================
# TestPromiseCronDeferred — deferred_check resolution path
# ============================================================

class TestPromiseCronDeferred:
    async def test_deferred_unverifiable_when_no_address(self, pg_pool, monkeypatch):
        from services.ct_digest import promise_cron

        # Seed: deferred_check row с promised_ts уже прошедшим (need resolve)
        tick_id = await pg_pool.fetchval(
            "INSERT INTO ct_digest_ticks (started_at, status) VALUES (NOW(), 'running') RETURNING tick_id"
        )
        item_id = await pg_pool.fetchval(
            "INSERT INTO ct_digest_items (tick_id, tweet_id) VALUES ($1, 't1') RETURNING id",
            tick_id,
        )
        past = datetime.utcnow() - timedelta(hours=1)
        promise_id = await pg_pool.fetchval(
            """INSERT INTO ct_promises (item_id, promised_ts, status, collection_name)
               VALUES ($1, $2, 'deferred_check', 'unknown_collection_xyz') RETURNING id""",
            item_id, past,
        )

        # Mock CatchmintClient — name search returns empty, snapshot returns None
        mock_client = MagicMock()
        mock_client.get_overview = AsyncMock(return_value=[])
        mock_client.get_contract_detail = AsyncMock(return_value={})

        count = await promise_cron._resolve_deferred(pg_pool, mock_client)
        assert count == 1
        new_status, source = await pg_pool.fetchrow(
            "SELECT status, ground_truth_source FROM ct_promises WHERE id=$1", promise_id,
        )
        assert new_status == "unverifiable"
        assert source == "no_baseline"

    async def test_upcoming_live_when_delta_positive(self, pg_pool):
        from services.ct_digest import promise_cron

        tick_id = await pg_pool.fetchval(
            "INSERT INTO ct_digest_ticks (started_at, status) VALUES (NOW(), 'running') RETURNING tick_id"
        )
        item_id = await pg_pool.fetchval(
            "INSERT INTO ct_digest_items (tick_id, tweet_id) VALUES ($1, 't1') RETURNING id",
            tick_id,
        )
        past = datetime.utcnow() - timedelta(hours=1)
        pid = await pg_pool.fetchval(
            """INSERT INTO ct_promises (item_id, promised_ts, status, contract_address, total_supply_pre)
               VALUES ($1, $2, 'upcoming', '0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84', 100)
               RETURNING id""",
            item_id, past,
        )
        mock_client = MagicMock()
        mock_client.get_contract_detail = AsyncMock(return_value={"totalSupply": 105})

        await promise_cron._resolve_upcoming(pg_pool, mock_client)
        row = await pg_pool.fetchrow(
            "SELECT status, total_supply_post, ground_truth_source FROM ct_promises WHERE id=$1", pid,
        )
        assert row["status"] == "live"
        assert row["total_supply_post"] == 105
        assert row["ground_truth_source"] == "catchmint_delta"


# ============================================================
# TestCollectorAsync — behavioral mock of subprocess
# ============================================================

class TestCollectorAsync:
    async def test_vocabulary_collector_parses_json(self, monkeypatch):
        from services.ct_digest import collectors
        from pathlib import Path as _Path

        # Force cookie pre-check pass
        monkeypatch.setattr(collectors, "X_TOKENS_PATH", __file__)  # any existing file

        # Mock create_subprocess_exec to return canned JSON from fixture
        sample = (FIXTURE_DIR / "x_search_sample.json").read_text()

        async def fake_create_subprocess_exec(*args, **kwargs):
            proc = MagicMock()

            async def fake_comm():
                return (sample.encode("utf-8"), b"")
            proc.communicate = fake_comm
            proc.returncode = 0
            proc.kill = lambda: None
            return proc

        monkeypatch.setattr(
            "asyncio.create_subprocess_exec", fake_create_subprocess_exec,
        )

        col = collectors.VocabularyCollector(queries=['"public mint" today'])
        result = await col.fetch(since_hours=6)
        assert len(result) == 3
        assert result[0].tweet_id == "1800000000000000001"
        assert result[0].author_handle == "wh7nft"
        assert "public mint" in result[0].text


# ============================================================
# TestCallbackParse — handle_ct_digest_callback malformed payload
# ============================================================

class TestCallbackParse:
    async def test_malformed_callback_no_tick_id_answers_bad_format(self):
        from services.telegram_notifier import handle_ct_digest_callback

        # Build minimal mock Update with callback_query
        query = MagicMock()
        query.data = "ct_digest_v1:thumbs_up"  # missing tick_id (only 2 parts)
        query.answer = AsyncMock()
        query.from_user = MagicMock(id=12345)
        update = MagicMock(callback_query=query)
        context = MagicMock()

        await handle_ct_digest_callback(update, context)
        query.answer.assert_called_once_with("Bad callback format")

    async def test_malformed_callback_bad_action(self):
        from services.telegram_notifier import handle_ct_digest_callback

        query = MagicMock()
        query.data = "ct_digest_v1:weird_action:42"
        query.answer = AsyncMock()
        query.from_user = MagicMock(id=12345)
        update = MagicMock(callback_query=query)
        context = MagicMock()

        await handle_ct_digest_callback(update, context)
        # First call OR (one of) — assertion that "Bad action" was passed
        calls = [c.args[0] if c.args else c.kwargs.get("text", "") for c in query.answer.call_args_list]
        assert any("Bad action" in str(c) for c in calls)

    async def test_malformed_callback_bad_tick_id_int_parse(self):
        from services.telegram_notifier import handle_ct_digest_callback

        query = MagicMock()
        query.data = "ct_digest_v1:thumbs_up:not_a_number"
        query.answer = AsyncMock()
        query.from_user = MagicMock(id=12345)
        update = MagicMock(callback_query=query)
        context = MagicMock()

        await handle_ct_digest_callback(update, context)
        calls = [str(c) for c in query.answer.call_args_list]
        assert any("Bad tick_id" in s for s in calls)
