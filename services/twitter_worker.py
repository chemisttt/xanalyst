"""
Twitter Worker — анализирует X-профили из Redis stream.

Читает handle из stream:x_analyze (отправленные Discord Monitor'ом),
анализирует через Twikit, считает метрики, проверяет reused name,
сохраняет в PostgreSQL и отправляет уведомления для S/A tier.

Запуск:
    python -m services.twitter_worker
"""

import asyncio
import logging
from datetime import datetime, timezone

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.redis_client import init_redis, close_redis, get_redis
from shared.twitter_client import TwitterClient
from shared.metrics import analyze_profile, parse_account_created_at
from shared.notifier import send_twitter_alert, send_watchlist_change_alert
from shared.alphagate import get_username_history

# --- Логи ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("twitter_worker")

# Эмодзи для тиров
TIER_EMOJI = {"S": "🔥", "A": "⭐", "B": "💎", "C": "📈", "D": "🚫"}


async def check_cooldown(handle: str) -> bool:
    """Проверить, не анализировали ли этот handle недавно."""
    r = get_redis()
    key = f"cooldown:{handle}"
    exists = await r.exists(key)
    return bool(exists)


async def set_cooldown(handle: str):
    """Установить cooldown для handle."""
    r = get_redis()
    key = f"cooldown:{handle}"
    ttl = settings.twitter_cooldown_hours * 3600
    await r.set(key, "1", ex=ttl)


async def check_reused_name(handle: str, user_id: int) -> dict:
    """
    Проверить reused name / renamed.

    Возвращает:
    - reused_name: True если этот handle ранее принадлежал другому user_id
    - renamed: True если этот user_id ранее использовал другой handle
    - old_handle: предыдущий handle (если renamed)
    - old_user_id: предыдущий user_id (если reused)
    """
    pool = get_pool()
    result = {"reused_name": False, "renamed": False, "old_handle": None, "old_user_id": None}

    async with pool.acquire() as conn:
        # Проверяем: этот handle принадлежал другому user_id?
        prev_owner = await conn.fetchrow(
            "SELECT user_id, display_name FROM twitter_snapshots "
            "WHERE handle = $1 AND user_id != $2",
            handle, user_id,
        )
        if prev_owner:
            result["reused_name"] = True
            result["old_user_id"] = prev_owner["user_id"]
            log.warning(f"⚠️ REUSED NAME: @{handle} ранее принадлежал user_id={prev_owner['user_id']}")

        # Проверяем: этот user_id использовал другой handle?
        prev_handle = await conn.fetchrow(
            "SELECT handle, display_name FROM twitter_snapshots "
            "WHERE user_id = $1 AND handle != $2",
            user_id, handle,
        )
        if prev_handle:
            result["renamed"] = True
            result["old_handle"] = prev_handle["handle"]
            log.warning(f"⚠️ RENAMED: user_id={user_id} ранее был @{prev_handle['handle']}")

    return result


async def save_analysis(handle: str, profile, metrics: dict, reused: dict, source: str):
    """Сохранить результат анализа в PostgreSQL."""
    pool = get_pool()

    # Парсим дату создания аккаунта (убираем timezone — колонка TIMESTAMP без tz)
    account_created = parse_account_created_at(profile.created_at)
    if account_created:
        account_created = account_created.replace(tzinfo=None)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO twitter_analyses
                (handle, user_id, display_name, bio,
                 followers_count, following_count, tweets_count,
                 account_created_at, verified,
                 engagement_rate, bot_percentage, quality_score,
                 twitter_score, tier,
                 reused_name, renamed, source,
                 rt_percentage, growth_velocity, account_age_days,
                 prev_usernames)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
            ON CONFLICT (handle, analyzed_date) DO UPDATE SET
                twitter_score = $13,
                tier = $14,
                rt_percentage = $18,
                growth_velocity = $19,
                account_age_days = $20,
                prev_usernames = $21,
                analyzed_at = NOW()
            """,
            handle,
            profile.user_id,
            profile.display_name,
            profile.bio[:500] if profile.bio else "",
            profile.followers_count,
            profile.following_count,
            profile.tweets_count,
            account_created,
            profile.verified,
            metrics["engagement_rate"],
            metrics["bot_percentage"],
            metrics["quality_score"],
            metrics["twitter_score"],
            metrics["tier"],
            reused["reused_name"],
            reused["renamed"],
            source,
            metrics["rt_percentage"],
            metrics["growth_velocity"],
            metrics["account_age_days"],
            reused.get("prev_usernames") or [],
        )


def send_alert(handle: str, profile, metrics: dict, reused: dict):
    """Отправить уведомление с inline keyboard в Telegram."""
    profile_data = {
        "display_name": profile.display_name,
        "followers_count": profile.followers_count,
        "following_count": profile.following_count,
        "bio": profile.bio,
        "verified": profile.verified,
        "is_blue_verified": profile.is_blue_verified,
        "location": profile.location,
    }
    send_twitter_alert(handle, profile_data, metrics, reused)


async def _handle_watchlist_alert(handle: str, profile, metrics: dict):
    """Сравнить score/tier с watchlist и отправить алерт при изменении."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_score, last_tier FROM twitter_watchlist WHERE LOWER(handle) = LOWER($1)",
            handle,
        )
        if not row:
            log.info(f"  @{handle} не найден в watchlist (возможно удалён)")
            return

        old_score = row["last_score"]
        old_tier = row["last_tier"]
        new_score = metrics["twitter_score"]
        new_tier = metrics["tier"]

        # Достаём предыдущий анализ для детального сравнения
        prev = await conn.fetchrow(
            """SELECT followers_count, following_count, engagement_rate,
                      rt_percentage, growth_velocity
               FROM twitter_analyses
               WHERE LOWER(handle) = LOWER($1)
               ORDER BY analyzed_at DESC LIMIT 1 OFFSET 1""",
            handle,
        )
        old_metrics = dict(prev) if prev else None

        # Обновляем last_score/last_tier в watchlist
        await conn.execute(
            "UPDATE twitter_watchlist SET last_score = $1, last_tier = $2 WHERE LOWER(handle) = LOWER($3)",
            new_score, new_tier, handle,
        )

    # Алерт только если score изменился
    if old_score is not None and old_score == new_score and old_tier == new_tier:
        log.info(f"  Watchlist @{handle}: score не изменился ({new_score}/{new_tier})")
        return

    profile_data = {
        "display_name": profile.display_name,
        "followers_count": profile.followers_count,
        "following_count": profile.following_count,
        "bio": profile.bio,
        "verified": profile.verified,
        "is_blue_verified": profile.is_blue_verified,
        "location": profile.location,
    }
    send_watchlist_change_alert(handle, profile_data, metrics, old_score, old_tier, old_metrics)


async def process_handle(tc: TwitterClient, handle: str, source: str):
    """Полный цикл анализа одного handle."""
    log.info(f"Анализирую @{handle}...")

    # 1. Получаем профиль
    profile = await tc.get_profile(handle)
    if not profile:
        log.warning(f"Не удалось получить профиль @{handle}")
        return

    # 2. Получаем твиты (для engagement rate)
    await asyncio.sleep(settings.twitter_scrape_delay)
    tweets = await tc.get_recent_tweets(handle, count=20)

    # 3. Получаем предыдущий snapshot для growth velocity
    prev_followers = None
    days_since_prev = None
    pool = get_pool()
    async with pool.acquire() as conn:
        prev = await conn.fetchrow(
            """
            SELECT followers_count, analyzed_at FROM twitter_analyses
            WHERE LOWER(handle) = LOWER($1) AND followers_count IS NOT NULL
            ORDER BY analyzed_at DESC LIMIT 1
            """,
            handle,
        )
        if prev and prev["followers_count"]:
            prev_followers = prev["followers_count"]
            days_since_prev = max((datetime.now(timezone.utc) - prev["analyzed_at"].replace(tzinfo=timezone.utc)).days, 1)

    # Считаем метрики (без скрейпинга фолловеров — слишком рискованно)
    metrics = analyze_profile(
        profile, tweets, followers_sample=None,
        prev_followers=prev_followers, days_since_prev=days_since_prev,
    )

    # 4. Проверяем reused name (snapshot-based)
    reused = await check_reused_name(handle, profile.user_id)

    # 4b. Alphagate: история юзернеймов (graceful fallback)
    prev_usernames = await get_username_history(profile.user_id, handle)
    reused["prev_usernames"] = prev_usernames or []

    # Если Alphagate нашёл прошлые имена — это тоже reused/renamed
    if prev_usernames:
        reused["reused_name"] = True

    # Обновляем snapshot с реальными данными
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO twitter_snapshots (handle, user_id, display_name, followers_count)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (handle, user_id) DO UPDATE SET
                display_name = $3,
                followers_count = $4,
                last_seen = NOW()
            """,
            handle, profile.user_id, profile.display_name, profile.followers_count,
        )

    # 5. Сохраняем в БД
    await save_analysis(handle, profile, metrics, reused, source)

    tier = metrics["tier"]
    emoji = TIER_EMOJI.get(tier, "")
    early_tag = " [EARLY]" if metrics.get("is_early") else ""
    log.info(
        f"  {emoji} @{handle}{early_tag}: score={metrics['twitter_score']}, "
        f"tier={tier}, engagement={metrics['engagement_rate']}%, "
        f"RT={metrics['rt_percentage']}%, "
        f"followers={profile.followers_count:,}"
    )

    # 6. Уведомление
    if source == "watchlist":
        # Watchlist: сравниваем score с предыдущим, шлём алерт при изменении
        await _handle_watchlist_alert(handle, profile, metrics)
    elif source == "ct_digest":
        # Spec 004: ct_digest enqueued этот handle для stats enrichment в digest'е.
        # НЕ шлём EARLY-алерт — иначе self-improving loop спамит EARLY 20+ сообщений
        # за tick. Analysis сохранён в twitter_analyses, digest подхватит на след. tick.
        log.info(f"  📊 ct_digest source — analysis saved silently, no EARLY alert")
    else:
        # Обычный алерт в EARLY топик (Discord-link analyses)
        send_alert(handle, profile, metrics, reused)


async def main():
    """Главный цикл — слушаем Redis stream и анализируем."""
    log.info("Запуск Twitter Worker...")

    # Инициализация
    await init_db()
    await init_redis()

    tc = TwitterClient()
    if not await tc.init():
        log.error("Не удалось инициализировать Twitter клиент")
        await close_db()
        await close_redis()
        return

    log.info("Twitter Worker готов. Жду задачи из stream:x_analyze...\n")

    r = get_redis()

    # Восстанавливаем last_id из Redis (чтобы при рестарте не перечитывать всё)
    saved_id = await r.get("twitter_worker:last_id")
    if saved_id:
        last_id = saved_id if isinstance(saved_id, str) else saved_id.decode()
        log.info(f"Восстановлен last_id из Redis: {last_id}")
    else:
        # Первый запуск или ключ потерян — читаем только новые сообщения
        last_id = "$"
        log.info("Нет сохранённого last_id, читаю только новые сообщения")

    try:
        while True:
            # Health check: если circuit breaker открыт — ждём, не читаем стрим
            if not tc.is_healthy:
                log.warning("Circuit breaker открыт, жду 60с...")
                await asyncio.sleep(60)
                continue

            # Читаем новые события из Redis stream (блокируемся на 5 сек)
            events = await r.xread({"stream:x_analyze": last_id}, count=5, block=5000)

            if not events:
                continue

            for stream_name, messages in events:
                for msg_id, data in messages:
                    last_id = msg_id
                    # Сохраняем позицию в Redis после каждого сообщения
                    await r.set("twitter_worker:last_id", msg_id)
                    handle = data.get("handle", "")
                    source = data.get("source", "discord")

                    if not handle:
                        continue

                    # Проверяем cooldown
                    if await check_cooldown(handle):
                        log.info(f"  Пропуск @{handle} (cooldown)")
                        continue

                    # Анализируем
                    try:
                        await process_handle(tc, handle, source)
                        await set_cooldown(handle)
                    except Exception as e:
                        log.error(f"Ошибка анализа @{handle}: {e}")

                    # Пауза между анализами
                    await asyncio.sleep(settings.twitter_scrape_delay)

    except KeyboardInterrupt:
        log.info("Остановка по Ctrl+C")
    finally:
        await close_db()
        await close_redis()
        log.info("Twitter Worker остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
