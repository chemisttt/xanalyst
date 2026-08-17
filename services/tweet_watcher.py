"""
Tweet Watcher — мониторинг твитов watchlist-аккаунтов по keywords.

Использует Twikit search_tweet() (SearchTimeline API) — один запрос
на всю watchlist с серверной фильтрацией по keywords.
При обнаружении нового твита с ключевым словом — уведомление в WATCHLIST топик.

Запуск (cron каждый час):
    python -m services.tweet_watcher
"""

import asyncio
import logging
import sys
from pathlib import Path

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.redis_client import init_redis, close_redis, get_redis
from shared.twitter_client import TwitterClient
from shared.notifier import send_keyword_tweet_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tweet_watcher")

# Максимум handle в одном поисковом запросе (ограничение длины query)
BATCH_SIZE = 15

# TTL для Redis SET — 48 часов (не алертить дубли)
SEEN_TTL = 48 * 60 * 60

# Redis ключ для SET уже виденных tweet_id
SEEN_KEY = "tweet_watcher:seen"


def build_search_query(handles: list[str], keywords: list[str]) -> str:
    """
    Собрать поисковый запрос Twitter.

    Формат: (from:h1 OR from:h2 OR ...) (kw1 OR kw2 OR kw3)
    """
    from_part = " OR ".join(f"from:{h}" for h in handles)
    kw_part = " OR ".join(keywords)
    return f"({from_part}) ({kw_part})"


def find_matched_keywords(text: str, keywords: list[str]) -> list[str]:
    """Найти какие keywords сработали в тексте твита."""
    text_lower = text.lower()
    return [kw for kw in keywords if kw.lower() in text_lower]


async def main():
    log.info("Tweet Watcher: старт")

    keywords = settings.watchlist_keywords
    log.info(f"Keywords: {keywords}")

    await init_db()
    await init_redis()

    tc = TwitterClient()
    if not await tc.init():
        log.error("Twitter клиент не инициализирован, выход")
        await close_db()
        await close_redis()
        return

    try:
        pool = get_pool()
        r = get_redis()

        # Получаем все handle из watchlist
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT handle FROM twitter_watchlist ORDER BY added_at")

        handles = [row["handle"] for row in rows]

        if not handles:
            log.info("Watchlist пуст, нечего искать")
            return

        log.info(f"Watchlist: {len(handles)} handle")

        # Разбиваем на батчи по BATCH_SIZE
        batches = [handles[i:i + BATCH_SIZE] for i in range(0, len(handles), BATCH_SIZE)]
        log.info(f"Батчей: {len(batches)}")

        total_found = 0
        total_new = 0

        for batch_idx, batch in enumerate(batches):
            query = build_search_query(batch, keywords)
            log.info(f"Батч {batch_idx + 1}/{len(batches)}: query={query[:120]}...")

            tweets = await tc.search_tweets(query, count=20)
            log.info(f"  Найдено твитов: {len(tweets)}")
            total_found += len(tweets)

            for tweet in tweets:
                # Проверяем дубликат через Redis SET
                is_seen = await r.sismember(SEEN_KEY, tweet.tweet_id)
                if is_seen:
                    continue

                # Новый твит — сохраняем в SET
                await r.sadd(SEEN_KEY, tweet.tweet_id)

                # Определяем какие keywords сработали
                matched = find_matched_keywords(tweet.text, keywords)
                if not matched:
                    # Twitter мог вернуть твит по другим причинам
                    log.debug(f"  Твит {tweet.tweet_id} без совпадений по keywords, пропускаю")
                    continue

                handle = tweet.handle or "unknown"
                total_new += 1

                log.info(f"  НОВЫЙ: @{handle} [{', '.join(matched)}] — {tweet.text[:80]}...")

                # Отправляем алерт
                send_keyword_tweet_alert(
                    handle=handle,
                    tweet_id=tweet.tweet_id,
                    tweet_text=tweet.text,
                    matched_keywords=matched,
                )

            # Пауза между батчами (если несколько)
            if batch_idx < len(batches) - 1:
                await asyncio.sleep(3)

        # Устанавливаем TTL на весь SET (обновляем при каждом запуске)
        await r.expire(SEEN_KEY, SEEN_TTL)

        log.info(f"Итого: найдено {total_found}, новых алертов {total_new}")

    finally:
        await close_db()
        await close_redis()
        log.info("Tweet Watcher: завершён")


if __name__ == "__main__":
    asyncio.run(main())
