"""
Spec 004 Task 4: Promise tracker — extraction logic.

Called inline после classify_batch в main service flow. Extracts contract address
(regex + EIP-55 checksum normalize) или collection name fallback, INSERTs into
ct_promises с статусом announced или deferred_check (по >48h future).

Resolution path в `promise_cron.py` (separate cron entry).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Optional

import asyncpg
from eth_utils import to_checksum_address  # noqa: F401  (re-export for tests)

from services.ct_digest.classifier import ClassifiedItem

log = logging.getLogger("ct_digest.promises")

ADDRESS_REGEX = re.compile(r"0x[a-fA-F0-9]{40}")
DEFER_THRESHOLD_HOURS = 48


def extract_contract_address(text: str) -> Optional[str]:
    """
    Regex match 0x[40] hex + EIP-55 checksum normalize.

    Returns checksum form (e.g. '0xae7AB96520DE3A18E5e111B5EaAb095312D7fE84')
    or None if no valid hex match. **MUST be checksum** — CatchMint API
    case-SENSITIVE, lowercase → 404 (CLAUDE.md spec 003).
    """
    match = ADDRESS_REGEX.search(text)
    if not match:
        return None
    raw = match.group(0)
    try:
        return to_checksum_address(raw)
    except (ValueError, Exception) as e:  # eth_utils может raise разные
        log.debug("invalid hex address %s: %s", raw, e)
        return None


def parse_promised_ts(iso_value: Optional[str]) -> Optional[datetime]:
    """Tolerant ISO parser; strips tz (PG schema is naive)."""
    if not iso_value:
        return None
    try:
        # Tolerant — drop 'Z', accept either '+00:00' or naive
        s = iso_value.rstrip("Z")
        if "+" in s[10:]:  # tz offset present after the date part
            s = s.split("+")[0]
        return datetime.fromisoformat(s)
    except (ValueError, TypeError) as e:
        log.debug("can't parse promised_ts %r: %s", iso_value, e)
        return None


async def insert_promise(
    pool: asyncpg.Pool,
    item: ClassifiedItem,
    item_db_id: int,
) -> Optional[int]:
    """
    INSERT one ct_promises row если item has promised_timestamp_iso.

    - status='announced' если promised_ts ≤ now + 48h (will resolve in normal cron path)
    - status='deferred_check' если promised_ts > now + 48h (re-poll at promise date)

    Returns row id or None (no promised_ts → no promise tracked).
    """
    promised = parse_promised_ts(item.promised_timestamp_iso)
    if not promised:
        return None

    address = extract_contract_address(item.raw.text)
    # Fallback ID — collection name из classifier hint
    collection = item.collection_name_hint

    if not address and not collection:
        log.debug("no address or collection — skip promise for tweet %s", item.tweet_id)
        return None

    now = datetime.utcnow()
    is_deferred = promised > now + timedelta(hours=DEFER_THRESHOLD_HOURS)
    status = "deferred_check" if is_deferred else "announced"

    row_id = await pool.fetchval(
        """
        INSERT INTO ct_promises (item_id, contract_address, collection_name,
                                 promised_ts, status, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        RETURNING id
        """,
        item_db_id, address, collection, promised, status,
    )
    log.info("promise %d inserted: status=%s addr=%s collection=%s promised=%s",
             row_id, status, address, collection, promised.isoformat())
    return row_id


async def extract_promises_for_batch(
    pool: asyncpg.Pool,
    items: list[tuple[ClassifiedItem, int]],
) -> int:
    """Iterate (classified_item, db_id) tuples, INSERT promises. Returns count."""
    count = 0
    for item, db_id in items:
        try:
            pid = await insert_promise(pool, item, db_id)
            if pid:
                count += 1
        except Exception as e:
            log.error("failed inserting promise for tweet %s: %s", item.tweet_id, e)
    return count
