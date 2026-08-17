"""
Spec 004 Task 2: Collector abstraction + 3 NFT collector implementations.

ABC `Collector` exposed для Phase 2 Reddit drop-in. RawItem — dataclass возвращаемый
collector'ами; classifier dedup'aет по `tweet_id` уровнем выше.

Subprocess discipline (load-bearing):
- ОБЯЗАТЕЛЬНО asyncio.create_subprocess_exec (НЕ sync subprocess.run — блокирует event loop)
- timeout=30s per query (raise CollectorTimeout)

x_search cookie auth prerequisite: ~/.claude/secrets/x_tokens.json должен существовать
на VPS botuser home (Task 7 deploy step #2 provisions).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from shared.ct_digest_queries import (
    NFT_CURATORS,
    NFT_PROJECT_CLUSTERS,
    NFT_VOCAB_QUERIES,
    build_account_query,
    build_project_cluster_query,
)

log = logging.getLogger("ct_digest.collectors")

# x_search global CLI tool (cookie-auth, см. ~/.claude/tools/x_search.py)
# Real file is x_search.py — symlinks /opt/homebrew/bin/x_search and ~/.local/bin/x_search
# point at it (convenience for shell). Use .py directly here to avoid PATH dependency
# in systemd / subprocess context.
X_SEARCH_PATH = os.path.expanduser("~/.claude/tools/x_search.py")
X_TOKENS_PATH = os.path.expanduser("~/.claude/secrets/x_tokens.json")

DEFAULT_TIMEOUT_SEC = 30
PER_QUERY_DELAY_SEC = 2  # rate-limit между sequential queries в одном collector'е


class CollectorTimeout(Exception):
    """x_search subprocess exceeded DEFAULT_TIMEOUT_SEC."""


class CollectorAuthError(Exception):
    """x_tokens.json missing or x_search exited with auth failure."""


@dataclass
class RawItem:
    """Сырой tweet из x_search до classification."""
    tweet_id: str
    url: str
    author_handle: str
    author_metadata: dict  # whatever x_search returns about author (verified, followers, etc.)
    posted_at_iso: str
    text: str
    source_type: str  # 'x_vocab' | 'x_account' | 'x_project'
    source_query: str  # фактический query который вернул этот item
    # Engagement metrics from x_search (None if unavailable)
    likes: int | None = None
    rts: int | None = None
    replies: int | None = None
    views: int | None = None


async def _run_x_search(query: str, since_hours: int, limit: int = 50) -> list[dict]:
    """
    Один subprocess call к x_search --json.

    Raises:
        CollectorTimeout — на истечение DEFAULT_TIMEOUT_SEC
        CollectorAuthError — если x_tokens.json отсутствует ИЛИ x_search exit с auth failure

    Returns: list[dict] — raw x_search JSON output (one dict per tweet).
    """
    if not Path(X_TOKENS_PATH).exists():
        raise CollectorAuthError(f"x_tokens.json not found at {X_TOKENS_PATH}")

    # Invoke via current python interpreter (sys.executable = venv python где наши deps
    # включая curl_cffi установлены). x_search.py shebang #!/usr/bin/env python3 hits
    # system python который не имеет curl_cffi на VPS — поэтому всегда даём sys.executable.
    cmd = [
        sys.executable,
        X_SEARCH_PATH,
        query,
        "--json",
        "--limit", str(limit),
        "--since-hours", str(since_hours),
    ]
    log.debug("x_search: %s (since %dh, limit %d)", query[:80], since_hours, limit)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=DEFAULT_TIMEOUT_SEC
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        raise CollectorTimeout(f"x_search timeout {DEFAULT_TIMEOUT_SEC}s on query={query[:60]!r}")

    if proc.returncode != 0:
        err = stderr.decode("utf-8", "replace")[:400]
        # x_search uses sys.exit(str) — any non-zero exit is opaque (auth failure looks
        # same as HTTP error). Bubble up as CollectorAuthError; caller decides if cookie
        # might need refresh.
        if "ct0" in err.lower() or "auth_token" in err.lower() or "401" in err:
            raise CollectorAuthError(f"x_search auth error: {err}")
        log.warning("x_search non-zero exit on %s: %s", query[:60], err)
        return []

    try:
        return json.loads(stdout.decode("utf-8", "replace"))
    except json.JSONDecodeError as e:
        log.warning("x_search returned non-JSON for query=%s: %s", query[:60], e)
        return []


def _parse_iso_ts(value: str | None) -> str:
    """x_search returns ISO already; just normalize None → empty."""
    return value or ""


def _as_int(v) -> int | None:
    """x_search returns engagement как varied types (twitter views = str, others = int).
    Coerce to int, None on failure/missing."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _normalize_raw(record: dict, source_type: str, source_query: str) -> RawItem | None:
    """Convert one x_search JSON dict → RawItem. Returns None if mandatory field missing."""
    try:
        tweet_id = str(record["id"])
        author = record.get("author") or {}
        author_handle = author if isinstance(author, str) else (author.get("screen_name") or author.get("handle") or "")
        text = record.get("text") or ""
        url = record.get("url") or f"https://x.com/{author_handle}/status/{tweet_id}"
        # x_search may expose author meta as nested dict; if just handle string, leave empty
        meta = author if isinstance(author, dict) else {}
        posted_at = _parse_iso_ts(record.get("created_at"))
        return RawItem(
            tweet_id=tweet_id,
            url=url,
            author_handle=author_handle,
            author_metadata=meta,
            posted_at_iso=posted_at,
            text=text,
            source_type=source_type,
            source_query=source_query,
            likes=_as_int(record.get("likes")),
            rts=_as_int(record.get("rts")),
            replies=_as_int(record.get("replies")),
            views=_as_int(record.get("views")),
        )
    except (KeyError, TypeError) as e:
        log.debug("skip malformed x_search record (%s): %s", e, str(record)[:120])
        return None


class Collector(ABC):
    """Abstract collector. Phase 2 adds Reddit/other implementations."""

    source_type: str = "abstract"

    @abstractmethod
    async def fetch(self, since_hours: int) -> list[RawItem]:
        """Returns deduped (within this collector) list of RawItems."""
        ...


class VocabularyCollector(Collector):
    """7 NFT vocab queries, sequential с 2s rate-limit между запросами."""

    source_type = "x_vocab"

    def __init__(self, queries: list[str] | None = None):
        self.queries = queries or NFT_VOCAB_QUERIES

    async def fetch(self, since_hours: int) -> list[RawItem]:
        seen: dict[str, RawItem] = {}
        for i, q in enumerate(self.queries):
            if i > 0:
                await asyncio.sleep(PER_QUERY_DELAY_SEC)
            try:
                records = await _run_x_search(q, since_hours=since_hours)
            except CollectorTimeout as e:
                log.warning("vocab collector skip query (timeout): %s", e)
                continue
            for rec in records:
                item = _normalize_raw(rec, self.source_type, q)
                if item and item.tweet_id not in seen:
                    seen[item.tweet_id] = item
        log.info("VocabularyCollector: %d unique items across %d queries", len(seen), len(self.queries))
        return list(seen.values())


class AccountFeedCollector(Collector):
    """Single OR'd query from:wh7nft OR from:0xvaidhik OR ... — concentrated alpha."""

    source_type = "x_account"

    def __init__(self, handles: list[str] | None = None):
        self.handles = handles or NFT_CURATORS

    async def fetch(self, since_hours: int) -> list[RawItem]:
        query = build_account_query(self.handles)
        try:
            records = await _run_x_search(query, since_hours=since_hours, limit=100)
        except CollectorTimeout as e:
            log.warning("account collector timeout: %s", e)
            return []
        items = [_normalize_raw(r, self.source_type, query) for r in records]
        items = [i for i in items if i is not None]
        # Dedup внутри collector'a (тот же tweet может попасть в overall feed дважды если bug).
        seen: dict[str, RawItem] = {}
        for i in items:
            seen.setdefault(i.tweet_id, i)
        log.info("AccountFeedCollector: %d unique items from %d curators", len(seen), len(self.handles))
        return list(seen.values())


class ProjectClusterCollector(Collector):
    """(@abnormalmfers OR @depunksClub OR ...) (mint OR drop OR live OR launch)."""

    source_type = "x_project"

    def __init__(self, handles: list[str] | None = None):
        self.handles = handles or NFT_PROJECT_CLUSTERS

    async def fetch(self, since_hours: int) -> list[RawItem]:
        query = build_project_cluster_query(self.handles)
        try:
            records = await _run_x_search(query, since_hours=since_hours, limit=100)
        except CollectorTimeout as e:
            log.warning("project cluster collector timeout: %s", e)
            return []
        items = [_normalize_raw(r, self.source_type, query) for r in records]
        items = [i for i in items if i is not None]
        seen: dict[str, RawItem] = {}
        for i in items:
            seen.setdefault(i.tweet_id, i)
        log.info("ProjectClusterCollector: %d unique items from %d projects", len(seen), len(self.handles))
        return list(seen.values())


async def fetch_all(since_hours: int = 6) -> list[RawItem]:
    """
    Run all 3 collectors in parallel, dedup acros collectors by tweet_id.

    `since_hours=6` matches the 4-tick/day cadence (overlap with prior tick caught
    by dedup at classifier level on tick boundary).
    """
    collectors: list[Collector] = [
        VocabularyCollector(),
        AccountFeedCollector(),
        ProjectClusterCollector(),
    ]
    results = await asyncio.gather(*(c.fetch(since_hours) for c in collectors), return_exceptions=True)

    merged: dict[str, RawItem] = {}
    for c, res in zip(collectors, results):
        if isinstance(res, Exception):
            log.error("Collector %s raised: %s", c.source_type, res)
            continue
        for item in res:
            # First wins (prefer earlier collector — vocab > account > project for tagging fairness).
            merged.setdefault(item.tweet_id, item)

    log.info("fetch_all: %d unique items across 3 collectors (since_hours=%d)", len(merged), since_hours)
    return list(merged.values())
