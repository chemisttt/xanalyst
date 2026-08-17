"""
Spec 004 Task 5: Cross-validation joins.

3 queries against existing xanalyst tables — ALL column names verified против
scripts/init_db.sql per ADR 0002 R-2:
- twitter_watchlist.handle (NOT 'ticker')
- catchmint_alerts.name, .telegram_msg_id (NOT 'collection_name'/'alert_msg_id')
- mirror_merged_signals.asset (single col, ticker WITHOUT $ — see private_mirror_dedup.py:51-52),
  category enum LOWERCASE 'meme'
"""

from __future__ import annotations

import logging
import re
from typing import Any

import asyncpg

from services.ct_digest.classifier import ClassifiedItem
from services.ct_digest.promises import extract_contract_address

log = logging.getLogger("ct_digest.cross_ref")

HANDLE_MENTION_RE = re.compile(r"@(\w{1,15})")
TICKER_RE = re.compile(r"\$([A-Z][A-Z0-9]{1,9})\b")


def _extract_mentioned_handles(text: str) -> list[str]:
    """All @handles от tweet text (1-15 char alphanum_)."""
    return list({m.group(1) for m in HANDLE_MENTION_RE.finditer(text)})


def _extract_tickers_no_dollar(text: str) -> list[str]:
    """
    $WIF style — capture group strips $. Match private_mirror_dedup.py:51-52.
    Returns list без $-префикса (e.g. ['WIF', 'PEPE']).
    """
    return list({m.group(1) for m in TICKER_RE.finditer(text)})


async def enrich_with_cross_refs(
    pool: asyncpg.Pool,
    items: list[ClassifiedItem],
) -> None:
    """
    Mutate items in place — set item.cross_refs = {twitter_watchlist:[], catchmint:[], mirror_meme:[]}.

    Skips items where included=False.
    """
    for item in items:
        if not item.included:
            continue
        item.cross_refs = await _build_cross_refs_for_item(pool, item)


async def _build_cross_refs_for_item(
    pool: asyncpg.Pool, item: ClassifiedItem,
) -> dict[str, Any]:
    refs: dict[str, Any] = {}
    text = item.raw.text

    # 1) Twitter watchlist — @handle mentions
    handles = _extract_mentioned_handles(text)
    if handles:
        rows = await pool.fetch(
            "SELECT handle FROM twitter_watchlist WHERE handle = ANY($1)",
            handles,
        )
        if rows:
            refs["twitter_watchlist"] = [r["handle"] for r in rows]

    # 2) CatchMint alerts — collection name substring
    if item.collection_name_hint:
        pattern = f"%{item.collection_name_hint}%"
        rows = await pool.fetch(
            """
            SELECT name, telegram_msg_id FROM catchmint_alerts
            WHERE name ILIKE $1 AND created_at > NOW() - INTERVAL '24 hours'
            ORDER BY created_at DESC LIMIT 5
            """,
            pattern,
        )
        if rows:
            refs["catchmint"] = [
                {"name": r["name"], "telegram_msg_id": r["telegram_msg_id"]}
                for r in rows
            ]

    # 3) Mirror Meme Signals — asset format = WIF (no $) OR checksum CA OR lowercase CA
    candidates: list[str] = []
    tickers = _extract_tickers_no_dollar(text)
    candidates.extend(tickers)

    addr_checksum = item.contract_address_hint
    if not addr_checksum:
        # Try extracting from raw text (item.contract_address_hint may be unset)
        addr_checksum = extract_contract_address(text)
    if addr_checksum:
        candidates.append(addr_checksum)
        candidates.append(addr_checksum.lower())  # legacy mirror entries may store lowercase

    if candidates:
        rows = await pool.fetch(
            """
            SELECT asset, telegram_msg_id FROM mirror_merged_signals
            WHERE category = 'meme'
              AND asset = ANY($1)
              AND created_at > NOW() - INTERVAL '24 hours'
            ORDER BY created_at DESC LIMIT 5
            """,
            candidates,
        )
        if rows:
            refs["mirror_meme"] = [
                {"asset": r["asset"], "telegram_msg_id": r["telegram_msg_id"]}
                for r in rows
            ]

    return refs


def has_any_cross_ref(refs: dict[str, Any]) -> bool:
    """True если хотя бы одна из 3 списков non-empty. Used for ✅XR badge в digest."""
    return any(refs.get(k) for k in ("twitter_watchlist", "catchmint", "mirror_meme"))


def has_strong_cross_ref(refs: dict[str, Any]) -> bool:
    """True ТОЛЬКО при catchmint OR mirror_meme match (strong signals).
    Used для EARLY cross-post trigger — watchlist alone слишком noisy (любое
    @-mention отслеживаемого handle хитит и спамит EARLY)."""
    return bool(refs.get("catchmint")) or bool(refs.get("mirror_meme"))
