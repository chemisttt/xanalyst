"""
Подключение к Redis.

Использование:
    from shared.redis_client import init_redis, close_redis, get_redis

    # При старте сервиса
    await init_redis()

    # Отправить событие в stream
    r = get_redis()
    await r.xadd("stream:x_analyze", {"handle": "elonmusk", "source": "discord"})

    # Прочитать события из stream
    events = await r.xread({"stream:x_analyze": "0"}, count=10, block=5000)

    # При завершении
    await close_redis()
"""

import redis.asyncio as aioredis
from shared.config import settings

# Глобальное подключение к Redis
_redis: aioredis.Redis | None = None


async def init_redis() -> aioredis.Redis:
    """Создать подключение к Redis."""
    global _redis
    if _redis is not None:
        return _redis

    _redis = aioredis.from_url(
        settings.redis_url,
        decode_responses=True,  # автоматически декодировать bytes → str
    )
    # Проверяем соединение
    await _redis.ping()
    print("[Redis] Подключение установлено")
    return _redis


async def close_redis():
    """Закрыть подключение к Redis."""
    global _redis
    if _redis:
        await _redis.close()
        _redis = None
        print("[Redis] Подключение закрыто")


def get_redis() -> aioredis.Redis:
    """Получить клиент Redis. Вызывать после init_redis()."""
    if _redis is None:
        raise RuntimeError("Redis не инициализирован! Вызови await init_redis() сначала.")
    return _redis
