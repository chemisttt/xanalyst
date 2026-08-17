"""
Watchlist Monitor — ре-анализ профилей из watchlist.

Читает все handle из twitter_watchlist, удаляет cooldown и
отправляет в stream:x_analyze с source=watchlist.
twitter_worker подхватит и обработает.

Запуск (cron каждые 12 часов):
    python -m services.watchlist_monitor
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("watchlist_monitor")


async def main():
    log.info("Watchlist Monitor: старт")

    await init_db()
    await init_redis()

    try:
        pool = get_pool()
        r = get_redis()

        # Получаем все handle из watchlist
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT handle FROM twitter_watchlist ORDER BY added_at")

        if not rows:
            log.info("Watchlist пуст, нечего анализировать")
            return

        log.info(f"Найдено {len(rows)} handle в watchlist")

        queued = 0
        for row in rows:
            handle = row["handle"]

            # Удаляем cooldown чтобы twitter_worker не пропустил
            await r.delete(f"cooldown:{handle}")

            # Отправляем в stream с source=watchlist
            await r.xadd("stream:x_analyze", {"handle": handle, "source": "watchlist"})
            queued += 1
            log.info(f"  В очередь: @{handle}")

            # Пауза между добавлениями (чтобы не забить stream)
            await asyncio.sleep(1)

        log.info(f"Готово: {queued} handle поставлено в очередь на ре-анализ")

    finally:
        await close_db()
        await close_redis()
        log.info("Watchlist Monitor: завершён")


if __name__ == "__main__":
    asyncio.run(main())
