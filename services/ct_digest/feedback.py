"""
Spec 004 Task 6: Feedback parser — second-pass LLM on freeform reply notes.

User replies на digest post free-form text. Parser extracts:
- item refs (по `[e3]` short_id ИЛИ по mention имени проекта)
- signal (+ / - / knew)
- multi-dim prefs (mechanic, chain, risk, dev, timing)

Сохраняет в ct_feedback с action='reply_note'.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Optional

import asyncpg

from shared.config import settings
from shared.llm_client import AnthropicClient

log = logging.getLogger("ct_digest.feedback")

SHORT_ID_RE = re.compile(r"\[([a-z]\d+)\]|(?<![a-zA-Z])([eckspx]\d+)(?![a-zA-Z0-9])")

FEEDBACK_SYSTEM_PROMPT = """You are parsing a freeform user note replying to a CT digest post.

Extract:
1. item_refs: list of {short_id, signal} where signal ∈ "+" | "-" | "knew" | "neutral"
   - short_ids look like "e3", "c1", "k2", "s1", "p1"
   - If user mentions a project name and no short_id, leave short_id empty: {"short_id": "", "signal": "+", "project_hint": "name"}
2. multi_dim_prefs: dict with optional keys:
   - mechanic_pref: e.g. "bonding_curve", "wl_only", "free_mint" (single value or null)
   - chain_pref: e.g. "ethereum", "base", "solana" (single value or null)
   - risk_tolerance: "high" | "medium" | "low" | null
   - dev_pref: "named" | "anon" | null
   - timing_pref: "now" | "24h" | "longer" | null

Output JSON only:
{"item_refs": [...], "multi_dim_prefs": {...}}

If text is too vague to extract anything specific, return {"item_refs": [], "multi_dim_prefs": {}}.
"""


def _extract_short_ids_regex(text: str) -> list[str]:
    """Pre-LLM cheap extraction для item refs (LLM can override)."""
    matches = SHORT_ID_RE.findall(text)
    return list({(m[0] or m[1]) for m in matches if (m[0] or m[1])})


async def parse_reply_note(
    pool: asyncpg.Pool,
    text: str,
    tick_id: int,
    digest_msg_id: int,
    user_id: Optional[int],
) -> Optional[int]:
    """
    Second-pass LLM extraction + INSERT ct_feedback row.

    Returns ct_feedback.id или None on failure.
    """
    if not settings.anthropic_api_key:
        log.error("anthropic_api_key not set — can't parse reply note")
        return None

    if not text.strip():
        return None

    client = AnthropicClient(api_key=settings.anthropic_api_key, max_tokens=512)
    cost_key = f"llm_cost:ct_digest_feedback:{datetime.utcnow():%Y-%m}"

    # Pre-LLM regex hint (LLM may use или ignore)
    regex_hints = _extract_short_ids_regex(text)
    user_msg = f"User reply text:\n```\n{text}\n```\n\nRegex-detected short_ids (hint): {regex_hints}"

    try:
        out_text, meta = await client.call(
            system=FEEDBACK_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
            max_tokens=512,
            cost_redis_key=cost_key,
        )
    except Exception as e:
        log.error("feedback LLM call failed: %s", e)
        return None

    # Strip fence if present
    stripped = out_text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped)
        stripped = re.sub(r"\s*```\s*$", "", stripped)

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        log.warning("feedback LLM returned non-JSON: %s", out_text[:200])
        parsed = {"item_refs": [], "multi_dim_prefs": {}}

    item_refs = parsed.get("item_refs") or []
    short_ids = [r.get("short_id") for r in item_refs if r.get("short_id")]
    prefs = parsed.get("multi_dim_prefs") or {}

    row_id = await pool.fetchval(
        """
        INSERT INTO ct_feedback (digest_msg_id, tick_id, action, item_short_ids,
                                 note_text, parsed_prefs, user_id, created_at)
        VALUES ($1, $2, 'reply_note', $3::text[], $4, $5::jsonb, $6, NOW())
        RETURNING id
        """,
        digest_msg_id, tick_id, short_ids or None, text,
        json.dumps(prefs), user_id,
    )
    log.info("feedback %d saved: tick=%s short_ids=%s cost=%.4f¢",
             row_id, tick_id, short_ids, meta["cost_cents"])
    return row_id


def format_parsed_summary(prefs: dict, short_ids: list[str]) -> str:
    """Short summary для reply-back к user."""
    parts = []
    if short_ids:
        parts.append(f"refs={','.join(short_ids)}")
    for k, v in (prefs or {}).items():
        if v:
            parts.append(f"{k}={v}")
    return ", ".join(parts) if parts else "—"
