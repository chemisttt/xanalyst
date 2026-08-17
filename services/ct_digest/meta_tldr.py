"""
Spec 004 follow-up: Meta TL;DR — 2-3 строки в начале каждого digest tick.

Берёт ClassifiedItem'ы за тик, шлёт компактный JSON в Sonnet (cache_control на
system prompt — одинаковый на всех тиках, prompt cache работает по нашему
обычному pricing). Возвращает короткий RU-параграф, который рендерится перед
header'ом в build_digest_markdown.

Graceful degradation: ЛЮБАЯ ошибка → None, digest отправляется без preamble.
Не хотим, чтобы новая фича блокировала весь pipeline.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from shared.config import settings
from shared.llm_client import AnthropicClient
from services.ct_digest.classifier import ClassifiedItem

log = logging.getLogger("ct_digest.meta_tldr")

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"

# Сколько items максимум подсовываем модели. Достаточно для нарратива, не раздувает input.
MAX_ITEMS_IN_INPUT = 30
# Cap длины mechanic_notes в input — иначе ~30 × 300 chars = 9K input tokens только на notes.
NOTES_CAP = 160
# Cap длины финального вывода — strip всё что больше, защита от LLM-runaway.
MAX_OUTPUT_CHARS = 400


def _load_system_prompt() -> str:
    return (PROMPTS_DIR / "ct_digest_meta_tldr.md").read_text("utf-8")


def _build_input_payload(items: list[ClassifiedItem]) -> dict:
    """Compress classified items в минимальный JSON для LLM."""
    included = [it for it in items if it.included][:MAX_ITEMS_IN_INPUT]
    bucket_counts: dict[str, int] = {}
    compressed = []
    for it in included:
        bucket_counts[it.bucket] = bucket_counts.get(it.bucket, 0) + 1
        compressed.append({
            "bucket": it.bucket,
            "tags": it.tags or [],
            "notes": (it.mechanic_notes or "")[:NOTES_CAP],
            "author": f"@{it.raw.author_handle}" if it.raw.author_handle else "?",
            "promised": it.promised_timestamp_iso or "",
            "collection": it.collection_name_hint or "",
            "cross_refs": list((it.cross_refs or {}).keys()),
        })
    return {
        "tick_window_hours": 6,
        "total_included": len(included),
        "bucket_counts": bucket_counts,
        "items": compressed,
    }


async def generate_meta_tldr(items: list[ClassifiedItem]) -> str | None:
    """
    Returns 2-3 строки meta-narrative, или None если items пуст/ошибка.

    Cost: один Sonnet call, ~$0.003 per tick (4 tick/день × 30 дней ≈ $0.36/мес).
    Cost учитывается в том же Redis key что digest classifier — попадёт в
    daily cost_summary автоматически.
    """
    if not items:
        return None
    included = [it for it in items if it.included]
    if not included:
        return None
    if not settings.anthropic_api_key:
        log.warning("ANTHROPIC_API_KEY missing — skipping meta tldr")
        return None

    try:
        system_prompt = _load_system_prompt()
        payload = _build_input_payload(items)
        user_content = (
            "Below is the compressed snapshot of this tick. Write the meta TL;DR per spec.\n\n"
            f"```json\n{json.dumps(payload, ensure_ascii=False, indent=1)}\n```"
        )

        client = AnthropicClient(api_key=settings.anthropic_api_key, max_tokens=400)
        month_key = datetime.utcnow().strftime("%Y-%m")
        text, meta = await client.call(
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
            max_tokens=400,
            cost_redis_key=f"llm_cost:ct_digest:{month_key}",
        )

        text = (text or "").strip()
        if not text:
            return None
        # Hard cap output — защита от runaway вывода
        if len(text) > MAX_OUTPUT_CHARS:
            text = text[:MAX_OUTPUT_CHARS].rstrip() + "…"
        log.info("meta tldr generated: %d chars, cost=%.4f¢",
                 len(text), meta.get("cost_cents", 0))
        return text
    except Exception as e:
        # Graceful — не валим тик из-за meta-фичи
        log.warning("meta tldr generation failed: %s", e)
        return None
