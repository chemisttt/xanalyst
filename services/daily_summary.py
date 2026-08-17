"""
AI Daily Summary — сводка дня по Telegram каналам.

Собирает все сообщения за день из PostgreSQL, группирует по каналам,
отправляет в Perplexity API (sonar) для суммаризации.

Perplexity хорош тем, что может дополнить сводку актуальной инфой из интернета
(проверить упомянутые проекты, дедлайны, новости).

Запуск:
    python -m services.daily_summary           # сводка за сегодня
    python -m services.daily_summary 2026-02-05  # сводка за конкретную дату
"""

import asyncio
import sys
import requests
from datetime import date, datetime, timedelta
from pathlib import Path

from openai import OpenAI

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.notifier import markdown_to_telegram_html


def send_to_telegram(text: str) -> bool:
    """Отправить сообщение в Telegram топик NEWS через бота."""
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        print("[!] Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_NOTIFY_CHAT_ID")
        return False

    base = f"https://api.telegram.org/bot{settings.telegram_bot_token}"

    # Конвертируем Markdown от Perplexity в HTML для Telegram
    text = markdown_to_telegram_html(text)

    # Telegram ограничивает сообщения до 4096 символов
    # Разбиваем на части если нужно
    chunks = []
    while text:
        if len(text) <= 4096:
            chunks.append(text)
            break
        # Ищем последний перенос строки до лимита
        cut = text[:4096].rfind("\n")
        if cut == -1:
            cut = 4096
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")

    for chunk in chunks:
        payload = {
            "chat_id": int(settings.telegram_notify_chat_id),
            "text": chunk,
            "parse_mode": "HTML",
        }
        # Если указан topic_id — отправляем в конкретный топик форума
        if settings.telegram_notify_topic_id:
            payload["message_thread_id"] = int(settings.telegram_notify_topic_id)

        resp = requests.post(f"{base}/sendMessage", json=payload)
        if not resp.json().get("ok"):
            print(f"[!] Ошибка отправки в Telegram: {resp.json()}")
            return False

    return True


# --- Промпт для Perplexity ---
SUMMARY_SYSTEM_PROMPT = """Ты — персональный крипто-аналитик. Твоя задача — сделать краткую,
полезную сводку дня по сообщениям из Telegram каналов.

Формат сводки:

📋 СВОДКА ЗА {date}

🔥 ГЛАВНОЕ (2-5 пунктов)
• Самые важные события и новости дня

⏰ ДЕДЛАЙНЫ И АКТИВНОСТИ
• Минты, клеймы, аирдропы, сейлы — с датами если указаны
• Что нужно сделать и когда

📊 ПРОЕКТЫ И ТРЕНДЫ
• Какие проекты упоминались чаще всего
• Новые проекты, на которые стоит обратить внимание

🐦 TWITTER-АНАЛИЗЫ ДНЯ (если есть)
• Результаты анализа X-профилей из Discord
• Tier, score, ключевые метрики
• Предупреждения о reused name / renamed

💡 ЗАМЕТКИ
• Интересные наблюдения, советы из каналов

Правила:
- Пиши кратко и по делу, без воды
- Если информации мало — не выдумывай, напиши что было
- Если упоминается проект — можешь дополнить актуальной инфой из интернета
- Дедлайны выделяй жирным
- Пиши на русском языке"""


async def get_messages_for_date(target_date: date) -> dict[str, list[dict]]:
    """Получить все сообщения за дату, сгруппированные по каналам."""
    pool = get_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT channel_name, author_name, text, message_date, urls
            FROM channel_messages
            WHERE message_date::date = $1
              AND source_account = 'main'
              AND text IS NOT NULL
              AND text != ''
            ORDER BY channel_name, message_date
            """,
            target_date,
        )

    # Группируем по каналам
    channels: dict[str, list[dict]] = {}
    for row in rows:
        ch = row["channel_name"] or "Unknown"
        if ch not in channels:
            channels[ch] = []
        channels[ch].append({
            "author": row["author_name"],
            "text": row["text"],
            "time": row["message_date"].strftime("%H:%M"),
            "urls": row["urls"] or [],
        })

    return channels


async def get_twitter_results_for_date(target_date: date) -> list[dict]:
    """Получить результаты Twitter-анализов за дату."""
    pool = get_pool()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT handle, display_name, followers_count, following_count,
                   engagement_rate, bot_percentage, twitter_score, tier,
                   reused_name, renamed, source,
                   rt_percentage, growth_velocity, account_age_days
            FROM twitter_analyses
            WHERE analyzed_date = $1
            ORDER BY twitter_score DESC
            """,
            target_date,
        )

    return [dict(row) for row in rows]


def format_messages_for_prompt(channels: dict[str, list[dict]], twitter_results: list[dict] = None) -> str:
    """Форматировать сообщения и Twitter-результаты в текст для промпта."""
    parts = []
    total = 0

    for ch_name, messages in channels.items():
        parts.append(f"\n=== Канал: {ch_name} ({len(messages)} сообщений) ===")
        for msg in messages:
            line = f"[{msg['time']}] {msg['author']}: {msg['text']}"
            if msg["urls"]:
                line += f" | ссылки: {', '.join(msg['urls'])}"
            parts.append(line)
            total += 1

    header = f"Всего: {total} сообщений из {len(channels)} каналов\n"

    # Добавляем результаты Twitter-анализов если есть
    if twitter_results:
        parts.append(f"\n\n=== Twitter-анализы за день ({len(twitter_results)} профилей) ===")
        for r in twitter_results:
            tier = r.get("tier", "?")
            score = r.get("twitter_score", 0)
            handle = r.get("handle", "?")
            name = r.get("display_name", "")
            followers = r.get("followers_count", 0)
            engagement = r.get("engagement_rate", 0)
            rt_pct = r.get("rt_percentage") or 0
            growth = r.get("growth_velocity") or 0
            age_days = r.get("account_age_days") or 0

            # "EARLY" лейбл для маленьких проектов
            early_tag = "EARLY " if followers < 2000 else ""
            line = f"  @{handle} ({name}) — {early_tag}{tier}-tier, score={score}, "
            line += f"followers={followers:,}, engagement={engagement}%, "
            line += f"RT={rt_pct}%, growth={growth} fol/day, age={age_days}d"
            if r.get("reused_name"):
                line += " ⚠️ REUSED NAME"
            if r.get("renamed"):
                line += " ⚠️ RENAMED"
            if rt_pct >= 80:
                line += " ⚠️ RT-ONLY"
            parts.append(line)

    return header + "\n".join(parts)


def generate_summary(messages_text: str, target_date: date) -> str:
    """Отправить сообщения в AI API и получить сводку.
    Поддерживает Gemini (бесплатный) и Perplexity (платный, fallback).
    """
    # Приоритет: Perplexity (умный + поиск) → Gemini (бесплатный) → Groq (бесплатный)
    if settings.perplexity_api_key:
        client = OpenAI(
            api_key=settings.perplexity_api_key,
            base_url="https://api.perplexity.ai",
        )
        model = "sonar"
        print("Использую Perplexity API...")
    elif settings.gemini_api_key:
        client = OpenAI(
            api_key=settings.gemini_api_key,
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        )
        model = "gemini-2.0-flash"
        print("Использую Gemini API...")
    elif settings.groq_api_key:
        client = OpenAI(
            api_key=settings.groq_api_key,
            base_url="https://api.groq.com/openai/v1",
        )
        model = "llama-3.3-70b-versatile"
        print("Использую Groq API (Llama 3.3 70B)...")
    else:
        raise RuntimeError("Нужен PERPLEXITY_API_KEY, GEMINI_API_KEY или GROQ_API_KEY в .env")

    system_prompt = SUMMARY_SYSTEM_PROMPT.replace("{date}", target_date.strftime("%d.%m.%Y"))

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Вот сообщения из крипто-каналов за день:\n\n{messages_text}"},
        ],
    )

    return response.choices[0].message.content


async def make_daily_summary(target_date: date | None = None) -> str:
    """Главная функция — собрать сообщения и сгенерировать сводку."""
    if target_date is None:
        target_date = date.today()

    print(f"Генерирую сводку за {target_date}...")

    # Подключаемся к БД
    await init_db()

    try:
        # Получаем сообщения из Telegram
        channels = await get_messages_for_date(target_date)

        # Получаем Twitter-анализы за день
        twitter_results = await get_twitter_results_for_date(target_date)

        if not channels and not twitter_results:
            return f"За {target_date.strftime('%d.%m.%Y')} нет данных в базе."

        total_msgs = sum(len(msgs) for msgs in channels.values())
        print(f"Найдено {total_msgs} сообщений из {len(channels)} каналов")
        if twitter_results:
            print(f"Найдено {len(twitter_results)} Twitter-анализов")

        # Форматируем для промпта (TG сообщения + Twitter результаты)
        messages_text = format_messages_for_prompt(channels, twitter_results)

        # Ограничиваем размер (Perplexity имеет лимит контекста)
        if len(messages_text) > 80000:
            messages_text = messages_text[:80000] + "\n\n... (обрезано из-за объёма)"
            print("Сообщения обрезаны до 80k символов")

        # Генерируем сводку через Perplexity
        print("Отправляю в Perplexity API...")
        summary = generate_summary(messages_text, target_date)

        print(f"Сводка готова ({len(summary)} символов)")
        return summary

    finally:
        await close_db()


async def main():
    """
    CLI:
        python -m services.daily_summary                  # сводка за сегодня → Telegram
        python -m services.daily_summary 2026-02-05       # за дату → Telegram
        python -m services.daily_summary --no-send        # только в консоль
    """
    # Проверяем API key (Gemini или Perplexity)
    if not settings.gemini_api_key and not settings.groq_api_key and not settings.perplexity_api_key:
        print("Ошибка: задай GEMINI_API_KEY, GROQ_API_KEY или PERPLEXITY_API_KEY в .env")
        return

    # Парсим аргументы
    args = sys.argv[1:]
    no_send = "--no-send" in args
    args = [a for a in args if a != "--no-send"]

    target_date = None
    if args:
        try:
            target_date = date.fromisoformat(args[0])
        except ValueError:
            print(f"Неверный формат даты: {args[0]} (нужен YYYY-MM-DD)")
            return

    # Генерируем сводку
    summary = await make_daily_summary(target_date)

    # Выводим в консоль
    print("\n" + "=" * 60)
    print(summary)
    print("=" * 60)

    # Отправляем в Telegram (если не --no-send)
    if not no_send:
        print("\nОтправляю в Telegram...")
        if send_to_telegram(summary):
            print("Сводка отправлена в топик NEWS!")
        else:
            print("Не удалось отправить в Telegram")


if __name__ == "__main__":
    asyncio.run(main())
