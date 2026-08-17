"""
Caller Digest — понятная сводка ЖИВЫХ human-caller каналов.

Не путать с private_mirror_digest (merged bot signals) и CT digest (twitter).

Источники — явный whitelist CALLER_DIGEST_SOURCES (не весь NAMED_CALLER_ROUTES):
  туда НЕ входят bot-топики вроде Mexc Kids «Коллы» (это tracker ставок
  esports/Polymarket, не человек с крипто-коллом).

Публикация: TELEGRAM_MIRROR_CALLER_DIGEST_TOPIC_ID или fallback DIGEST (6894).

Cron UTC: 30 5 / 30 17  (08:30 / 20:30 MSK)
  python -m services.caller_digest [morning|evening]
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from openai import OpenAI

sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config import settings
from shared.db import close_db, get_pool, init_db
from shared.notifier import markdown_to_telegram_html, resolve_mirror_topic_id, send_to_topic
from shared.source_routing import INGEST_WHITELIST

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("caller_digest")

# (chat_id, topic_id|None, short_label)
# topic_id=None → весь канал (broadcast) ИЛИ все топики чата с этим channel_id
# (для forum-каналов лучше указывать topic_id явно).
CALLER_DIGEST_SOURCES: list[tuple[int, int | None, str]] = [
    (1000000004, None, "Caller A"),
    (1000000005, 101, "Caller B"),
    (1000000006, None, "Community"),
    (1000000003, 201, "Research"),
]

MAX_MSGS_PER_SOURCE = 25
MAX_TEXT_CHARS = 400
MAX_USER_CONTENT = 45000
MIN_MSGS_TO_PUBLISH = 1

# Шаблоны bot-спама, которые иногда просачиваются в human-каналы
_BOT_NOISE_RE = re.compile(
    r"(Growing position|Trader:\s*#|Trade Median|MEGABET|Account:\s*#LARGE|"
    r"Volume UP by|Обнаружен Funding|New wall\s|Max Spread:|"
    r"AGG_BOT|@aggregator)",
    re.I,
)

SYSTEM_PROMPT = """Ты пишешь короткую записку трейдеру на русском — что говорили
ЖИВЫЕ коллеры в приватках за период. Не аналитический отчёт, не JSON.

Формат (строго):

👤 <b>Коллеры</b> · {period_label}
<i>{period_str}</i>

Если есть реальные крипто-коллы / сделки / мнения по рынкам:
• <b>Имя</b> — 1–2 строки своими словами: что сделал/сказал
  (тикер/цена/действие если есть). Без слов long/short/conviction.

Если коллер писал, но это треп/мем без trade-идеи:
• <b>Имя</b> — треп, без колла

В конце одна строка:
→ <b>Итого:</b> ...

Жёсткие правила:
1. Это крипто/NFT/airdrop контекст. Ставки на Dota/CS/политику/Polymarket-
   esports НЕ являются крипто-коллами — если такое попало во вход, игнорируй.
2. Не выдумывай тикеры и цены. Нет в тексте — не пиши.
3. Молчащих коллеров не перечисляй длинным списком (можно «остальные молчали»).
4. Максимум ~15 строк. Без markdown-таблиц, без английских заголовков.
5. Если за период реально нечего сказать — одна фраза:
   «За период живые коллеры почти молчали / без ясных крипто-коллов.»
"""


def determine_slot(now: datetime | None = None) -> str:
    if now is None:
        now = datetime.utcnow()
    return "morning" if now.hour < 12 else "evening"


def compute_period(slot: str, now: datetime | None = None) -> tuple[datetime, datetime, str]:
    if now is None:
        now = datetime.utcnow()
    period_end = now.replace(minute=0, second=0, microsecond=0)
    period_start = period_end - timedelta(hours=12)
    # Человекочитаемо в MSK (UTC+3) — так удобнее читать в TG
    def msk(dt: datetime) -> str:
        return (dt + timedelta(hours=3)).strftime("%d.%m %H:%M")

    label = "утро" if slot == "morning" else "вечер"
    period_str = f"{msk(period_start)} → {msk(period_end)} МСК"
    return period_start, period_end, label


def _is_noise(text: str) -> bool:
    if not text or len(text.strip()) < 3:
        return True
    if _BOT_NOISE_RE.search(text):
        return True
    return False


async def fetch_caller_messages(
    period_start: datetime, period_end: datetime
) -> dict[str, list[dict]]:
    pool = get_pool()
    by_label: dict[str, list[dict]] = {}

    async with pool.acquire() as conn:
        for chat_id, topic_id, label in CALLER_DIGEST_SOURCES:
            if topic_id is None:
                rows = await conn.fetch(
                    """
                    SELECT channel_name, author_name, text, message_date,
                           extracted_tickers, extracted_cas
                    FROM channel_messages
                    WHERE source_account = 'private_mirror'
                      AND channel_id = $1
                      AND message_date >= $2 AND message_date < $3
                      AND text IS NOT NULL AND text != ''
                    ORDER BY message_date DESC
                    LIMIT $4
                    """,
                    chat_id, period_start, period_end, MAX_MSGS_PER_SOURCE * 2,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT channel_name, author_name, text, message_date,
                           extracted_tickers, extracted_cas
                    FROM channel_messages
                    WHERE source_account = 'private_mirror'
                      AND channel_id = $1
                      AND telegram_topic_id = $2
                      AND message_date >= $3 AND message_date < $4
                      AND text IS NOT NULL AND text != ''
                    ORDER BY message_date DESC
                    LIMIT $5
                    """,
                    chat_id, topic_id, period_start, period_end, MAX_MSGS_PER_SOURCE * 2,
                )

            msgs = []
            for r in reversed(list(rows)):
                text = (r["text"] or "").strip()
                if _is_noise(text):
                    continue
                # Убираем зеркальный хвост @AGG_BOT
                text = re.sub(r"\s*@AGG_BOT\s*$", "", text, flags=re.I)
                text = text[:MAX_TEXT_CHARS]
                msgs.append({
                    "text": text,
                    "ts": (r["message_date"] + timedelta(hours=3)).strftime("%H:%M"),
                    "tickers": list(r["extracted_tickers"] or []),
                    "cas": list(r["extracted_cas"] or [])[:2],
                })
                if len(msgs) >= MAX_MSGS_PER_SOURCE:
                    break

            if msgs:
                by_label[label] = msgs

    return by_label


def format_prompt(
    by_label: dict[str, list[dict]],
    period_label: str,
    period_str: str,
) -> str:
    lines = [
        f"Период: {period_str} ({period_label})",
        f"Активных источников: {len(by_label)}",
        "Ниже — только сообщения людей. Сделай записку по правилам system.",
        "",
    ]
    for label, msgs in sorted(by_label.items(), key=lambda x: -len(x[1])):
        lines.append(f"### {label} ({len(msgs)} сообщ.)")
        for m in msgs:
            extra = ""
            if m["tickers"]:
                extra += f" [тикеры: {', '.join('$' + t for t in m['tickers'][:5])}]"
            if m["cas"]:
                short = [c[:8] + "…" for c in m["cas"]]
                extra += f" [CA: {', '.join(short)}]"
            lines.append(f"  {m['ts']} | {m['text']}{extra}")
        lines.append("")
    return "\n".join(lines)


def call_llm(system_prompt: str, user_content: str) -> tuple[str, str | None]:
    chain = [
        ("perplexity", settings.perplexity_api_key, "https://api.perplexity.ai", "sonar"),
        (
            "gemini",
            settings.gemini_api_key,
            "https://generativelanguage.googleapis.com/v1beta/openai/",
            "gemini-2.0-flash",
        ),
        ("groq", settings.groq_api_key, "https://api.groq.com/openai/v1", "llama-3.3-70b-versatile"),
    ]
    errors = []
    for name, key, base_url, model in chain:
        if not key:
            errors.append(f"{name}: no key")
            continue
        try:
            client = OpenAI(api_key=key, base_url=base_url, timeout=60)
            log.info(f"LLM try {name} ({model})")
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
            )
            content = resp.choices[0].message.content
            log.info(f"  {name}: OK ({len(content)} chars)")
            return content, name
        except Exception as e:
            err = f"{name}: {type(e).__name__}: {str(e)[:200]}"
            log.warning(err)
            errors.append(err)
    return "all failed: " + " | ".join(errors), None


def _resolve_topic() -> int | None:
    raw = getattr(settings, "telegram_mirror_caller_digest_topic_id", None)
    if raw:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    return resolve_mirror_topic_id("DIGEST")


def _fallback_plain(by_label: dict[str, list[dict]], period_label: str, period_str: str) -> str:
    """Без LLM: сырой но читаемый дайджест (если все провайдеры упали)."""
    lines = [
        f"👤 <b>Коллеры</b> · {period_label}",
        f"<i>{period_str}</i>",
        "",
    ]
    for label, msgs in sorted(by_label.items(), key=lambda x: -len(x[1])):
        sample = msgs[-1]["text"][:200].replace("<", "&lt;")
        lines.append(f"• <b>{label}</b> ({len(msgs)}) — {sample}")
    lines.append("")
    lines.append("→ <b>Итого:</b> LLM недоступен, сырой список выше.")
    return "\n".join(lines)


async def run_digest(slot: str | None = None) -> int:
    await init_db()
    try:
        slot = slot or determine_slot()
        period_start, period_end, period_label = compute_period(slot)

        def msk(dt: datetime) -> str:
            return (dt + timedelta(hours=3)).strftime("%d.%m %H:%M")

        period_str = f"{msk(period_start)} → {msk(period_end)} МСК"
        log.info(f"Caller digest slot={slot} {period_start} → {period_end} ({period_str})")

        by_label = await fetch_caller_messages(period_start, period_end)
        n_msgs = sum(len(v) for v in by_label.values())
        log.info(f"Собрано {n_msgs} msgs из {len(by_label)} источников (после noise-filter)")

        if n_msgs < MIN_MSGS_TO_PUBLISH:
            log.info("Пусто после фильтров — skip publish")
            return 0

        user_content = format_prompt(by_label, period_label, period_str)
        if len(user_content) > MAX_USER_CONTENT:
            user_content = user_content[:MAX_USER_CONTENT] + "\n\n... (обрезано)"

        system_prompt = SYSTEM_PROMPT.format(
            period_label=period_label,
            period_str=period_str,
        )
        content, provider = call_llm(system_prompt, user_content)
        if provider is None:
            log.error(f"LLM fail: {content[:300]}")
            content = _fallback_plain(by_label, period_label, period_str)
            html = content
        else:
            # LLM мог вернуть уже HTML или markdown — нормализуем
            if "<b>" in content or "<i>" in content:
                html = content
            else:
                html = markdown_to_telegram_html(content)

        if len(html) > 4000:
            html = html[:4000] + "\n\n…"

        topic_id = _resolve_topic()
        if topic_id is None:
            log.warning("Нет topic_id — print only")
            log.info(html[:1500])
            return 0

        msg_id = send_to_topic(html, topic_id)
        log.info(
            f"Published topic={topic_id} msg_id={msg_id} provider={provider} "
            f"sources={list(by_label.keys())}"
        )
        return 0 if msg_id else 1
    finally:
        await close_db()


async def main():
    slot_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if slot_arg and slot_arg not in ("morning", "evening"):
        log.error(f"Invalid slot {slot_arg!r}")
        sys.exit(2)
    rc = await run_digest(slot_arg)
    sys.exit(rc)


if __name__ == "__main__":
    asyncio.run(main())
