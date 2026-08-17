"""
Spec 004 Task 3: Classifier — Sonnet 4.6 с prompt cache + anti-pattern few-shots.

Flow:
  1. Pre-LLM astroturf detection (Jaccard на text-shingles, mark candidates)
  2. Batch ~10-20 items в одном Sonnet call (prompt cache hits на system)
  3. Parse JSON output → ClassifiedItem dataclass
  4. Cost tracked per call в Redis (TTL до end-of-month UTC)

Kill switch: config.ct_digest_paused → CTDigestPaused exception (caught в main entry).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable

from shared.config import settings
from shared.llm_client import AnthropicClient
from services.ct_digest.collectors import RawItem

log = logging.getLogger("ct_digest.classifier")

DEFAULT_BATCH_SIZE = 15
# vibecode proxy sometimes returns safety refusal instead of JSON (tick 108: "I can't discuss that.")
MAX_BATCH_RETRIES = 3
RETRY_BASE_DELAY_SEC = 1.5
ASTROTURF_JACCARD_THRESHOLD = 0.8
ASTROTURF_MIN_AUTHORS = 3

VALID_BUCKETS = {
    "early_signals", "emerging_clusters",
    "calendar_24h", "calendar_3d", "calendar_7d",
    "state_reconcile", "paid_hype",
}

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"


class CTDigestPaused(Exception):
    """Raised when config.ct_digest_paused=True — caught by main entry."""


class ClassifierError(Exception):
    """LLM returned malformed output that can't be parsed."""


@dataclass
class ClassifiedItem:
    tweet_id: str
    bucket: str
    novelty_score: float
    included: bool
    tags: list[str]
    mechanic_notes: str
    contract_address_hint: str | None
    promised_timestamp_iso: str | None
    collection_name_hint: str | None
    raw: RawItem
    pre_llm_astroturf: bool = False  # set by pre-LLM Jaccard pass
    cross_refs: dict = field(default_factory=dict)  # filled by cross_ref step
    short_id: str | None = None


def _load_prompt() -> str:
    """system prompt = ct_digest_system.md + anti_patterns.md concatenated."""
    system = (PROMPTS_DIR / "ct_digest_system.md").read_text("utf-8")
    anti = (PROMPTS_DIR / "ct_digest_anti_patterns.md").read_text("utf-8")
    return f"{system}\n\n---\n\n# Anti-pattern reference (few-shots)\n\n{anti}"


def _shingles(text: str, n: int = 3) -> set[str]:
    """Character n-grams для Jaccard similarity."""
    text = re.sub(r"\s+", " ", text.lower().strip())
    if len(text) < n:
        return {text}
    return {text[i:i + n] for i in range(len(text) - n + 1)}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def detect_astroturf_clusters(items: list[RawItem]) -> set[str]:
    """
    Pre-LLM Jaccard на pairwise text shingles. Returns set of tweet_ids flagged as
    members of astroturf cluster (≥ASTROTURF_MIN_AUTHORS distinct authors с pairwise
    Jaccard ≥ ASTROTURF_JACCARD_THRESHOLD).
    """
    flagged: set[str] = set()
    if len(items) < ASTROTURF_MIN_AUTHORS:
        return flagged

    shingles_by_id = {it.tweet_id: _shingles(it.text) for it in items}
    by_author = {it.tweet_id: it.author_handle for it in items}

    # Build clusters: greedy — for each item, find similar items
    for i, it in enumerate(items):
        cluster = {it.tweet_id}
        cluster_authors = {it.author_handle}
        for j, other in enumerate(items):
            if i == j:
                continue
            sim = _jaccard(shingles_by_id[it.tweet_id], shingles_by_id[other.tweet_id])
            if sim >= ASTROTURF_JACCARD_THRESHOLD:
                cluster.add(other.tweet_id)
                cluster_authors.add(other.author_handle)
        if len(cluster_authors) >= ASTROTURF_MIN_AUTHORS:
            flagged.update(cluster)

    if flagged:
        log.info("astroturf pre-LLM flagged %d items across %d authors",
                 len(flagged), len({by_author[t] for t in flagged}))
    return flagged


def _build_user_message(items: list[RawItem]) -> str:
    """Wrap items в XML для structured input."""
    blocks = []
    for it in items:
        # Trim text to 800 chars (CT tweets ≤280 normally, but quote-tweets могут быть длиннее)
        text = it.text[:800].replace("</tweet>", "&lt;/tweet&gt;")
        blocks.append(
            f'<tweet id="{it.tweet_id}" author="{it.author_handle}" '
            f'posted_at="{it.posted_at_iso}" source="{it.source_type}">\n'
            f"{text}\n"
            f"</tweet>"
        )
    return "<items>\n" + "\n".join(blocks) + "\n</items>"


def _is_llm_refusal(text: str) -> bool:
    """True if proxy/model returned a safety refusal instead of JSON array."""
    s = (text or "").strip()
    if not s:
        return True
    low = s.lower()
    # Known vibecode / Claude refusal shapes (tick 108: "I can't discuss that.")
    refusal_markers = (
        "i can't discuss",
        "i cannot discuss",
        "i can't help with",
        "i cannot help with",
        "i'm not able to",
        "i am not able to",
        "can't assist with that",
        "cannot assist with that",
        "as an ai",
        "i must refuse",
    )
    if any(m in low for m in refusal_markers):
        # Allow if it still looks like a JSON array after the prose (rare)
        if "[" in s and "]" in s and s.find("[") < 80:
            return False
        return True
    # Pure prose without any JSON structure
    if not s.lstrip().startswith(("[", "{", "```")):
        return True
    return False


def _parse_llm_output(text: str) -> list[dict]:
    """Strip markdown fences if present, parse JSON array."""
    if _is_llm_refusal(text):
        raise ClassifierError(
            f"LLM refusal / non-JSON body: first 200 chars: {(text or '')[:200]!r}"
        )
    s = text.strip()
    if s.startswith("```"):
        # Strip markdown fence
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```\s*$", "", s)
    try:
        data = json.loads(s)
    except json.JSONDecodeError as e:
        raise ClassifierError(f"LLM returned non-JSON: {e}; first 200 chars: {s[:200]}")
    if not isinstance(data, list):
        raise ClassifierError(f"LLM returned non-list: {type(data).__name__}")
    return data


async def classify_batch(items: list[RawItem]) -> list[ClassifiedItem]:
    """
    Один Sonnet call на batch (~10-20 items). Pre-LLM astroturf detection runs first.
    Retries batch on refusal / parse error (proxy soft-fails with HTTP 200 + prose).

    Raises:
        CTDigestPaused — if config.ct_digest_paused=True
        ClassifierError — malformed LLM output
        ValueError — anthropic_api_key not configured
    """
    if settings.ct_digest_paused:
        raise CTDigestPaused("CT_DIGEST_PAUSED=1 — skipping classify_batch")
    if not items:
        return []

    api_key = getattr(settings, "anthropic_api_key", None)
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set in env — cannot classify")

    astroturf_ids = detect_astroturf_clusters(items)

    client = AnthropicClient(api_key=api_key)
    system_prompt = _load_prompt()
    cost_key = f"llm_cost:ct_digest:{datetime.utcnow():%Y-%m}"

    results: list[ClassifiedItem] = []
    stats = {"calls": 0, "failed_leaves": 0}

    async def _call_parse(
        batch: list[RawItem], max_retries: int = MAX_BATCH_RETRIES,
    ) -> list[dict] | None:
        """Retry LLM+JSON parse. Returns parsed list or None."""
        user_msg = _build_user_message(batch)
        for attempt in range(1, max_retries + 1):
            stats["calls"] += 1
            try:
                text, meta = await client.call(
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_msg}],
                    cost_redis_key=cost_key,
                )
            except Exception as e:
                log.error(
                    "Sonnet call failed n=%d attempt %d/%d: %s",
                    len(batch), attempt, max_retries, e,
                )
                if attempt < max_retries:
                    await asyncio.sleep(RETRY_BASE_DELAY_SEC * attempt)
                    continue
                return None

            try:
                parsed = _parse_llm_output(text)
                if attempt > 1:
                    log.info("batch n=%d recovered on attempt %d", len(batch), attempt)
                return parsed
            except ClassifierError as e:
                log.warning(
                    "batch n=%d attempt %d/%d parse/refusal: %s",
                    len(batch), attempt, max_retries, e,
                )
                if attempt < max_retries:
                    await asyncio.sleep(RETRY_BASE_DELAY_SEC * attempt)
        return None

    def _items_from_parsed(batch: list[RawItem], parsed: list[dict]) -> list[ClassifiedItem]:
        out: list[ClassifiedItem] = []
        parsed_by_id = {p.get("tweet_id"): p for p in parsed if isinstance(p, dict)}
        for raw in batch:
            p = parsed_by_id.get(raw.tweet_id)
            if not p:
                log.debug("LLM dropped tweet_id=%s", raw.tweet_id)
                continue
            bucket = p.get("bucket")
            if bucket not in VALID_BUCKETS:
                log.warning(
                    "invalid bucket %r for tweet %s — defaulting to state_reconcile",
                    bucket, raw.tweet_id,
                )
                bucket = "state_reconcile"

            is_astroturf = raw.tweet_id in astroturf_ids
            if is_astroturf and bucket != "paid_hype":
                log.info(
                    "astroturf override: tweet %s bucket %s → paid_hype",
                    raw.tweet_id, bucket,
                )
                bucket = "paid_hype"

            tags = p.get("tags") or []
            if is_astroturf and "astroturf_cluster" not in tags:
                tags = list(tags) + ["astroturf_cluster"]

            out.append(ClassifiedItem(
                tweet_id=raw.tweet_id,
                bucket=bucket,
                novelty_score=float(p.get("novelty_score") or 0),
                included=bool(p.get("included", True)),
                tags=tags,
                mechanic_notes=p.get("mechanic_notes") or "",
                contract_address_hint=p.get("contract_address_hint"),
                promised_timestamp_iso=p.get("promised_timestamp_iso"),
                collection_name_hint=p.get("collection_name_hint"),
                raw=raw,
                pre_llm_astroturf=is_astroturf,
            ))
        return out

    async def _classify_recursive(batch: list[RawItem], depth: int = 0) -> list[ClassifiedItem]:
        """On persistent refusal, bisect the batch so one toxic tweet can't kill 15 others."""
        if not batch:
            return []
        # Full retries on root chunk; fewer after bisect (cost control)
        retries = MAX_BATCH_RETRIES if depth == 0 else 2
        parsed = await _call_parse(batch, max_retries=retries)
        if parsed is not None:
            return _items_from_parsed(batch, parsed)

        if len(batch) == 1:
            stats["failed_leaves"] += 1
            log.error(
                "single-item refusal tweet_id=%s author=@%s — drop",
                batch[0].tweet_id, batch[0].author_handle,
            )
            return []

        mid = max(1, len(batch) // 2)
        log.warning(
            "batch n=%d refused — bisect depth=%d into %d+%d",
            len(batch), depth, mid, len(batch) - mid,
        )
        left = await _classify_recursive(batch[:mid], depth + 1)
        right = await _classify_recursive(batch[mid:], depth + 1)
        return left + right

    # Process в chunks of DEFAULT_BATCH_SIZE, then bisect on refusal
    for start in range(0, len(items), DEFAULT_BATCH_SIZE):
        chunk = items[start:start + DEFAULT_BATCH_SIZE]
        results.extend(await _classify_recursive(chunk))

    log.info(
        "classify_batch: %d/%d items classified (llm_calls=%d, single_drops=%d)",
        len(results), len(items), stats["calls"], stats["failed_leaves"],
    )
    return results


def assign_short_ids(items: list[ClassifiedItem]) -> None:
    """Mutate items in place — assign per-tick short_ids (e1/c2/k1/s1/p1)."""
    prefix_for_bucket = {
        "early_signals": "e",
        "emerging_clusters": "c",
        "calendar_24h": "k",
        "calendar_3d": "k",
        "calendar_7d": "k",
        "state_reconcile": "s",
        "paid_hype": "p",
    }
    counters: dict[str, int] = {}
    for it in items:
        if not it.included:
            continue
        p = prefix_for_bucket.get(it.bucket, "x")
        counters[p] = counters.get(p, 0) + 1
        it.short_id = f"{p}{counters[p]}"
