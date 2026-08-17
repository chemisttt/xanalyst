"""
Private Mirror Daily Digest (spec 001 task 6).

Cron-driven (НЕ daemon). Запускается 2 раза в день:
  - 09:00 MSK (06:00 UTC) — morning slot, период «прошлый вечер + ночь»
  - 21:00 MSK (18:00 UTC) — evening slot, период «утро + день»

Источник данных:
  - channel_messages WHERE source_account='private_mirror' AND message_date >= cutoff
  - mirror_merged_signals WHERE first_seen >= cutoff

Promo принципы (отличный от существующего daily_summary.py для main NEWS):
  - НЕ агрегирует "что произошло" в общем виде
  - Структурирует по 4 категориям + named callers + COMMUNITY
  - Использует уже-аггрегированные merged signals (не сырые сообщения)
  - LLM нужен в основном для прозы между числами, не для извлечения сигналов

LLM fallback chain (паттерн как в services/daily_summary.py):
  Perplexity (primary) → Gemini (fallback 1) → Groq (fallback 2)
  Если все три фейлятся: log + alert в SYSHEALTH topic + status=failed в mirror_digests

Запуск:
    python -m services.private_mirror_digest             # period выводится из current_time
    python -m services.private_mirror_digest morning     # принудительно morning slot
    python -m services.private_mirror_digest evening     # принудительно evening slot
"""

import asyncio
import json
import logging
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

import requests
from openai import OpenAI

# Path setup
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.notifier import (
    markdown_to_telegram_html,
    send_to_topic,
    resolve_mirror_topic_id,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("private_mirror_digest")


SYSTEM_PROMPT = """You write a compact crypto signal digest from several example chats.
This is a signal overview, not a news recap. Be concrete. Russian output.
Do not invent data. Categories: ARB, PUMP_DUMP, MEME, WHALE.
Manual callers: Caller A, Caller B. Community board for airdrop notes."""


def determine_slot(now: datetime | None = None) -> str:
    """Определяет slot ('morning'/'evening') по времени UTC."""
    if now is None:
        now = datetime.utcnow()
    # Morning slot: ~06:00 UTC = 09:00 MSK
    # Evening slot: ~18:00 UTC = 21:00 MSK
    return "morning" if now.hour < 12 else "evening"


def compute_period(slot: str, now: datetime | None = None) -> tuple[datetime, datetime, str]:
    """
    Возвращает (period_start, period_end, period_label) в UTC.
    - morning slot (~06:00 UTC): покрывает 18:00 предыдущего дня → 06:00 текущего
    - evening slot (~18:00 UTC): покрывает 06:00 текущего → 18:00 текущего
    """
    if now is None:
        now = datetime.utcnow()

    if slot == "morning":
        # End ≈ now (06:00 UTC), start = 12h earlier
        period_end = now.replace(minute=0, second=0, microsecond=0)
        period_start = period_end - timedelta(hours=12)
        period_label = "Утренний (ночь)"
    else:  # evening
        period_end = now.replace(minute=0, second=0, microsecond=0)
        period_start = period_end - timedelta(hours=12)
        period_label = "Вечерний (день)"

    return period_start, period_end, period_label


# =============================================================================
# Data gathering
# =============================================================================

async def get_data_for_period(period_start: datetime, period_end: datetime) -> dict:
    """Собрать все данные из PG для LLM-промпта (фильтр по source_account)."""
    pool = get_pool()

    async with pool.acquire() as conn:
        # 1. Все приваточные сообщения за период (только private_mirror!)
        raw_msgs = await conn.fetch(
            """
            SELECT channel_name, author_name, text, message_date,
                   extracted_tickers, extracted_cas
            FROM channel_messages
            WHERE source_account = 'private_mirror'
              AND message_date >= $1 AND message_date < $2
              AND text IS NOT NULL AND text != ''
            ORDER BY message_date
            """,
            period_start, period_end,
        )

        # 2. Merged signals за период
        merged = await conn.fetch(
            """
            SELECT id, category, asset, asset_chain, n_sources,
                   leader_source, sources, time_to_consensus_sec,
                   first_seen, last_seen, emitted_locked, late_echo_count
            FROM mirror_merged_signals
            WHERE first_seen >= $1 AND first_seen < $2
            ORDER BY n_sources DESC, first_seen DESC
            LIMIT 50
            """,
            period_start, period_end,
        )

    return {
        "raw_msgs": [dict(r) for r in raw_msgs],
        "merged": [dict(r) for r in merged],
    }


def format_data_for_prompt(data: dict, period_label: str, period_start: datetime, period_end: datetime) -> str:
    """Структурированный input для LLM."""
    raw = data["raw_msgs"]
    merged = data["merged"]

    lines = [
        f"=== Период: {period_start.strftime('%Y-%m-%d %H:%M')} → {period_end.strftime('%Y-%m-%d %H:%M')} UTC ({period_label}) ===",
        f"",
        f"=== TOP MERGED SIGNALS ({len(merged)} штук, отсортированы по n_sources) ===",
    ]
    if not merged:
        lines.append("(пусто)")
    else:
        for m in merged[:20]:
            sources_list = []
            if m.get("sources"):
                try:
                    s_data = m["sources"] if isinstance(m["sources"], list) else json.loads(m["sources"])
                    sources_list = [s.get("source", "?") for s in s_data][:4]
                except Exception:
                    sources_list = []
            srcs = ", ".join(sources_list)
            lines.append(
                f"  [{m['category']}] {m['asset']!r} chain={m.get('asset_chain') or '?'}  "
                f"n_sources={m['n_sources']} first_by={m['leader_source']}  "
                f"consensus_in={m['time_to_consensus_sec'] or '?'}s  "
                f"locked={'Y' if m['emitted_locked'] else 'N'}  "
                f"late_echoes={m['late_echo_count']}  "
                f"sources=[{srcs}]"
            )

    # По категориям — счётчики
    cat_counts = {}
    for m in merged:
        cat_counts[m["category"]] = cat_counts.get(m["category"], 0) + 1
    lines.append(f"")
    lines.append(f"=== СЧЁТЧИКИ ПО КАТЕГОРИЯМ ===")
    for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        lines.append(f"  {cat}: {cnt}")

    # Top tickers/CAs
    ticker_count: dict[str, int] = {}
    ca_count: dict[str, int] = {}
    for r in raw:
        for t in (r.get("extracted_tickers") or []):
            ticker_count[t] = ticker_count.get(t, 0) + 1
        for c in (r.get("extracted_cas") or []):
            ca_count[c] = ca_count.get(c, 0) + 1
    lines.append(f"")
    lines.append(f"=== TOP TICKERS (по упоминаниям в сырых сообщениях) ===")
    for t, c in sorted(ticker_count.items(), key=lambda x: -x[1])[:15]:
        lines.append(f"  ${t}: {c}")
    lines.append(f"")
    lines.append(f"=== TOP CAs (по упоминаниям) ===")
    for ca, c in sorted(ca_count.items(), key=lambda x: -x[1])[:10]:
        chain = "eth" if ca.startswith("0x") else "sol"
        lines.append(f"  {ca[:8]}...{ca[-6:]} ({chain}): {c}")

    # Named callers preview
    CALLER_A = [r for r in raw if r["channel_name"] and r["channel_name"].startswith("Caller A")]
    CALLER_B = [r for r in raw if r["channel_name"] and "Caller B topic" in (r["channel_name"] or "")]
    Community = [r for r in raw if r["channel_name"] and r["channel_name"].startswith("Community")]

    lines.append(f"")
    lines.append(f"=== РУЧНЫЕ КОЛЛЕРЫ ===")
    lines.append(f"  Caller A: {len(CALLER_A)} msgs")
    for r in CALLER_A[:3]:
        lines.append(f"    > {(r['text'] or '')[:200]}")
    lines.append(f"  CALLER_B (ForumB/коллит): {len(CALLER_B)} msgs")
    for r in CALLER_B[:3]:
        lines.append(f"    > {(r['text'] or '')[:200]}")
    lines.append(f"  Community (COMMUNITY community): {len(Community)} msgs")
    for r in Community[:3]:
        lines.append(f"    > {(r['text'] or '')[:200]}")

    lines.append(f"")
    lines.append(f"=== Всего сырых сообщений за период: {len(raw)} ===")

    return "\n".join(lines)


# =============================================================================
# LLM provider chain
# =============================================================================

def call_llm(system_prompt: str, user_content: str) -> tuple[str, str | None]:
    """
    Пытается Perplexity → Gemini → Groq.
    Возвращает (content, provider_used) или (error_msg, None) если все упали.
    """
    chain = [
        ("perplexity", settings.perplexity_api_key, "https://api.perplexity.ai", "sonar"),
        ("gemini", settings.gemini_api_key, "https://generativelanguage.googleapis.com/v1beta/openai/", "gemini-2.0-flash"),
        ("groq", settings.groq_api_key, "https://api.groq.com/openai/v1", "llama-3.3-70b-versatile"),
    ]
    errors = []
    for provider_name, key, base_url, model in chain:
        if not key:
            errors.append(f"{provider_name}: no API key")
            continue
        try:
            client = OpenAI(api_key=key, base_url=base_url, timeout=60)
            log.info(f"Пытаюсь {provider_name} ({model})...")
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
            )
            content = resp.choices[0].message.content
            log.info(f"  {provider_name}: OK ({len(content)} chars)")
            return content, provider_name
        except Exception as e:
            err = f"{provider_name}: {type(e).__name__}: {str(e)[:200]}"
            log.warning(err)
            errors.append(err)
            continue

    return "all providers failed: " + " | ".join(errors), None


# =============================================================================
# Persistence + publication
# =============================================================================

async def save_digest_row(
    period_start: datetime,
    period_end: datetime,
    slot: str,
    provider: str | None,
    status: str,
    error: str | None,
    content: str | None,
    telegram_msg_id: int | None,
):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO mirror_digests
                (period_start, period_end, slot, provider, status, error, content, telegram_msg_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            period_start, period_end, slot, provider, status, error, content, telegram_msg_id,
        )


def alert_digest_failure(period_start: datetime, period_end: datetime, error: str):
    """Уведомить SYSHEALTH топик о провале digest'а."""
    text = (
        f"⚠️ <b>Mirror digest failed</b>\n"
        f"Период: {period_start.strftime('%Y-%m-%d %H:%M')} → "
        f"{period_end.strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"Все LLM-провайдеры недоступны или вернули ошибку.\n\n"
        f"<i>Подробности:</i>\n<code>{(error or '')[:1500]}</code>"
    )
    syshealth_id = resolve_mirror_topic_id("SYSHEALTH")
    if syshealth_id:
        send_to_topic(text, syshealth_id)
    else:
        log.critical(f"[SYSHEALTH not configured] {text}")


# =============================================================================
# Main
# =============================================================================

async def run_digest(slot: str | None = None) -> int:
    """Главная функция. Возвращает 0 если digest или 1 если все failed."""
    await init_db()
    try:
        slot = slot or determine_slot()
        period_start, period_end, period_label = compute_period(slot)
        log.info(f"Digest slot={slot} период={period_start} → {period_end}")

        data = await get_data_for_period(period_start, period_end)
        log.info(f"Данные: {len(data['raw_msgs'])} raw msgs, {len(data['merged'])} merged signals")

        if not data["raw_msgs"] and not data["merged"]:
            log.info("За период нет данных, digest пропущен (но всё равно фиксируется в PG)")
            await save_digest_row(
                period_start, period_end, slot, None, "ok", None,
                f"За период {period_label.lower()} ({period_start.strftime('%H:%M')}-{period_end.strftime('%H:%M')} UTC) "
                f"нет данных из приваток. Скорее всего сервис только что стартовал или приватки молчали.",
                None,
            )
            return 0

        user_content = format_data_for_prompt(data, period_label, period_start, period_end)
        system_prompt = SYSTEM_PROMPT.format(
            period_label=period_label,
            period_str=f"{period_start.strftime('%H:%M')} → {period_end.strftime('%H:%M')} UTC",
        )

        # Cap user content (большие LLM имеют лимиты)
        if len(user_content) > 60000:
            user_content = user_content[:60000] + "\n\n... (обрезано из-за объёма)"

        content, provider = call_llm(system_prompt, user_content)

        if provider is None:
            # All providers failed
            log.error(f"All LLM providers failed: {content}")
            await save_digest_row(
                period_start, period_end, slot, None, "failed", content[:5000], None, None,
            )
            alert_digest_failure(period_start, period_end, content)
            return 1

        # Success — publish to digest topic
        digest_topic = resolve_mirror_topic_id("DIGEST")
        msg_id = None
        if digest_topic is None:
            log.warning("TELEGRAM_MIRROR_DIGEST_TOPIC_ID не задан, digest НЕ публикуется в TG (но сохранён в PG)")
        else:
            # Конвертация MD → HTML для Telegram
            telegram_text = markdown_to_telegram_html(content)
            if len(telegram_text) > 4000:
                # Telegram limit 4096 chars per message — обрезаем
                telegram_text = telegram_text[:4000] + "\n\n... (обрезано)"
            msg_id = send_to_topic(telegram_text, digest_topic)
            if msg_id:
                log.info(f"Digest опубликован в DIGEST topic, msg_id={msg_id}")
            else:
                log.warning("Digest publish to TG failed")

        await save_digest_row(
            period_start, period_end, slot, provider, "ok", None, content, msg_id,
        )
        return 0

    finally:
        await close_db()


async def main():
    # CLI: ["morning"|"evening"] | none = auto
    slot_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if slot_arg and slot_arg not in ("morning", "evening"):
        log.error(f"Invalid slot: {slot_arg!r}. Use 'morning' or 'evening'.")
        sys.exit(1)
    code = await run_digest(slot=slot_arg)
    sys.exit(code)


if __name__ == "__main__":
    asyncio.run(main())
