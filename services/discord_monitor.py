"""
Discord Monitor — парсинг X/Twitter ссылок из Discord каналов.

Подключается через user token (self-bot) и слушает указанные каналы.
При обнаружении ссылок на x.com / twitter.com — извлекает handle
и отправляет в Redis stream для анализа Twitter Worker'ом.

Запуск:
    python -m services.discord_monitor
"""

import re
import time
import logging
import asyncio

import discord
import redis.asyncio as aioredis

from shared.config import settings
from shared.redis_client import init_redis, close_redis, get_redis

# --- Логи ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("discord_monitor")

# --- Регулярка для X/Twitter ссылок ---
# Ловим: https://x.com/username, https://twitter.com/username/status/123 и т.д.
X_LINK_PATTERN = re.compile(
    r'https?://(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,15})(?=[/\s?#]|$)',
)

# Системные "аккаунты" Twitter — не анализируем
IGNORE_HANDLES = {
    "home", "explore", "search", "notifications", "messages",
    "settings", "i", "intent", "hashtag", "share",
}


def extract_twitter_handles(text: str) -> list[str]:
    """Извлечь уникальные Twitter handles из текста."""
    if not text:
        return []
    matches = X_LINK_PATTERN.findall(text)
    # Фильтруем системные и дедуплицируем, приводим к lowercase
    handles = []
    seen = set()
    for handle in matches:
        h = handle.lower()
        if h not in IGNORE_HANDLES and h not in seen:
            handles.append(h)
            seen.add(h)
    return handles


class DiscordMonitor(discord.Client):
    """Self-bot клиент для мониторинга Discord каналов."""

    def __init__(self, channel_ids: list[int]):
        # Self-bot не использует intents как обычный бот
        super().__init__()
        self.channel_ids = set(channel_ids)
        # Метка последней записи heartbeat в Redis (rate-limit на 30 сек)
        self._last_heartbeat_write = 0.0

    async def on_socket_event_type(self, event_type: str):
        # Диспатчится при каждом Gateway-событии (READY, MESSAGE_CREATE, TYPING_START,
        # PRESENCE_UPDATE и т.д.) — доказательство что WS активно качает данные.
        # NB: socket_raw_receive работает только в DEBUG-логе, в INFO он pass.
        # Пишем в Redis с rate-limit 30 сек, health_check.py читает.
        now = time.time()
        if now - self._last_heartbeat_write < 30:
            return
        self._last_heartbeat_write = now
        try:
            r = get_redis()
            await r.set("discord_monitor:heartbeat", str(int(now)), ex=300)
        except Exception:
            # Redis hiccup не должен валить мониторинг
            pass

    async def on_ready(self):
        log.info(f"Discord: залогинен как {self.user} (id={self.user.id})")

        # Проверяем доступ к каналам
        found = 0
        for ch_id in self.channel_ids:
            ch = self.get_channel(ch_id)
            if ch:
                guild = getattr(ch, "guild", None)
                server_name = guild.name if guild else "DM"
                log.info(f"  Слушаю: #{ch.name} ({server_name})")
                found += 1
            else:
                log.warning(f"  Канал {ch_id} не найден или нет доступа")

        log.info(f"Мониторю {found}/{len(self.channel_ids)} каналов. Жду X-ссылки...\n")

    async def on_message(self, message: discord.Message):
        # Только из наших каналов
        if message.channel.id not in self.channel_ids:
            return

        # Извлекаем Twitter handles
        handles = extract_twitter_handles(message.content)

        # Также проверяем embeds (Discord автоматически создаёт превью ссылок)
        for embed in message.embeds:
            if embed.url:
                handles.extend(extract_twitter_handles(embed.url))

        if not handles:
            return

        # Дедуплицируем
        handles = list(dict.fromkeys(handles))

        r = get_redis()
        for handle in handles:
            # Отправляем в Redis stream для Twitter Worker
            await r.xadd(
                "stream:x_analyze",
                {
                    "handle": handle,
                    "source": "discord",
                    "channel_id": str(message.channel.id),
                    "channel_name": message.channel.name,
                    "message_url": message.jump_url,
                },
            )
            log.info(
                f"[#{message.channel.name}] Найден X-профиль: @{handle} "
                f"(от {message.author.name})"
            )


async def main():
    """Запуск Discord Monitor."""
    if not settings.discord_user_token:
        log.error("Задай DISCORD_USER_TOKEN в .env")
        return

    if not settings.discord_channel_ids:
        log.error("Задай DISCORD_CHANNEL_IDS в .env")
        return

    # Парсим channel IDs
    channel_ids = [int(cid) for cid in settings.discord_channel_ids]

    log.info("Запуск Discord Monitor...")

    # Подключаемся к Redis
    await init_redis()

    # Создаём и запускаем клиент
    client = DiscordMonitor(channel_ids)

    try:
        await client.start(settings.discord_user_token)
    except discord.LoginFailure:
        log.error("Не удалось залогиниться в Discord — проверь DISCORD_USER_TOKEN")
    except KeyboardInterrupt:
        log.info("Остановка по Ctrl+C")
    finally:
        await close_redis()
        log.info("Discord Monitor остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
