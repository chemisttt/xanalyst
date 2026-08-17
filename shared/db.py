"""
Подключение к PostgreSQL через asyncpg.

Использование:
    from shared.db import init_db, close_db, get_pool

    # При старте сервиса
    await init_db()

    # Выполнить запрос
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM settings")

    # При завершении
    await close_db()
"""

import asyncpg
from shared.config import settings

# Глобальный пул соединений (создаётся один раз при старте)
_pool: asyncpg.Pool | None = None


async def init_db() -> asyncpg.Pool:
    """Создать пул соединений к PostgreSQL."""
    global _pool
    if _pool is not None:
        return _pool

    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,   # минимум соединений в пуле
        max_size=10,  # максимум соединений
    )
    print("[DB] Подключение к PostgreSQL установлено")
    return _pool


async def close_db():
    """Закрыть пул соединений."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        print("[DB] Пул соединений закрыт")


def get_pool() -> asyncpg.Pool:
    """Получить пул соединений. Вызывать после init_db()."""
    if _pool is None:
        raise RuntimeError("База данных не инициализирована! Вызови await init_db() сначала.")
    return _pool
