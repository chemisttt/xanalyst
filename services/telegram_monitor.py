"""
Telegram Monitor — сбор сообщений из каналов и чатов.

Использует Telethon (userbot) для подключения через реальный аккаунт.
Слушает новые сообщения в указанных каналах и сохраняет их в PostgreSQL.

Запуск:
    python -m services.telegram_monitor
"""

import asyncio
import re
import logging
from datetime import datetime

from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, User, Message
from telethon.tl import functions, types

from shared.config import settings
from shared.db import init_db, close_db, get_pool

# --- Настройка логов ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("tg_monitor")


# --- Регулярка для извлечения ссылок из текста ---
URL_PATTERN = re.compile(r'https?://\S+')


def extract_urls(text: str | None) -> list[str]:
    """Извлечь все URL из текста сообщения."""
    if not text:
        return []
    return URL_PATTERN.findall(text)


async def save_message(msg: Message, channel_name: str):
    """Сохранить сообщение в базу данных."""
    pool = get_pool()

    # Определяем автора
    sender = await msg.get_sender()
    if isinstance(sender, User):
        author = sender.first_name or ""
        if sender.last_name:
            author += f" {sender.last_name}"
        if sender.username:
            author += f" (@{sender.username})"
    elif isinstance(sender, (Channel, Chat)):
        author = sender.title or "Channel"
    else:
        author = "Unknown"

    # Извлекаем ссылки
    urls = extract_urls(msg.text)

    # Проверяем наличие медиа
    has_media = msg.media is not None

    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO channel_messages
                    (source, channel_id, channel_name, message_id,
                     author_name, text, has_media, urls, message_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (source, channel_id, message_id) DO NOTHING
                """,
                "telegram",                              # source
                msg.chat_id,                             # channel_id
                channel_name,                            # channel_name
                msg.id,                                  # message_id
                author,                                  # author_name
                msg.text or "",                          # text
                has_media,                               # has_media
                urls,                                    # urls (text[])
                msg.date.replace(tzinfo=None),           # message_date
            )
        log.info(f"[{channel_name}] {author}: {(msg.text or '(медиа)')[:80]}")
    except Exception as e:
        log.error(f"Ошибка сохранения сообщения: {e}")


async def main():
    """Главная функция — запуск мониторинга."""

    # Проверяем конфиг
    if not settings.check_telegram_monitor():
        return

    log.info("Запуск Telegram Monitor...")

    # Подключаемся к БД
    await init_db()

    # Создаём Telethon клиент
    # session файл сохраняется как 'xanalyst.session' в корне проекта
    client = TelegramClient(
        "xanalyst",
        int(settings.telegram_api_id),
        settings.telegram_api_hash,
    )

    await client.start(phone=settings.telegram_phone)
    me = await client.get_me()
    log.info(f"Авторизован как: {me.first_name} (@{me.username})")

    # Находим каналы/чаты для мониторинга
    monitored_chats = {}  # chat_id → название
    forum_chats = set()   # chat_id форумов (для определения топиков)
    # Кэш топиков: chat_id → {topic_id → topic_name} (заполняется лениво)
    forum_topics = {}

    for channel_ref in settings.telegram_channels:
        try:
            # Numeric id (for example -1001234567890) or username (example_channel)
            if channel_ref.lstrip("-").isdigit():
                ref = int(channel_ref)
            else:
                ref = channel_ref

            entity = await client.get_entity(ref)
            chat_id = entity.id
            name = getattr(entity, "title", None) or getattr(entity, "username", channel_ref)
            monitored_chats[chat_id] = name

            # Помечаем форумы
            is_forum = getattr(entity, "forum", False)
            if is_forum:
                forum_chats.add(chat_id)
                forum_topics[chat_id] = {1: "General"}
                log.info(f"  Мониторинг: {name} (id={chat_id}) [форум]")
            else:
                log.info(f"  Мониторинг: {name} (id={chat_id})")

        except Exception as e:
            log.warning(f"  Не удалось найти канал '{channel_ref}': {e}")

    if not monitored_chats:
        log.error("Нет каналов для мониторинга! Проверь TELEGRAM_CHANNELS в .env")
        await close_db()
        return

    # Логируем исключённые топики
    if settings.telegram_exclude_topics:
        for exc_chat, exc_topics in settings.telegram_exclude_topics.items():
            name = monitored_chats.get(exc_chat, exc_chat)
            log.info(f"  Исключены топики в {name}: {exc_topics}")

    log.info(f"Мониторю {len(monitored_chats)} каналов. Жду новые сообщения...\n")

    # Обработчик новых сообщений
    @client.on(events.NewMessage(chats=list(monitored_chats.keys())))
    async def handler(event: events.NewMessage.Event):
        msg = event.message
        # chat_id может быть отрицательным (-100...), а в словаре — положительный
        chat_id = event.chat_id
        pos_id = int(str(chat_id).replace("-100", "")) if chat_id < 0 else chat_id
        base_name = (
            monitored_chats.get(chat_id)
            or monitored_chats.get(-chat_id)
            or monitored_chats.get(pos_id)
            or "Unknown"
        )

        # Определяем название топика для форумов
        channel_name = base_name
        is_forum = pos_id in forum_chats or chat_id in forum_chats
        if is_forum:
            topic_id = None
            if msg.reply_to:
                # Для вложенных ответов: reply_to_top_id = ID корневого сообщения топика
                topic_id = getattr(msg.reply_to, "reply_to_top_id", None)
                # Для top-level сообщений в топике: reply_to_msg_id = ID корневого сообщения
                if topic_id is None:
                    topic_id = getattr(msg.reply_to, "reply_to_msg_id", None)
            else:
                # Если reply_to отсутствует — General (topic_id=1)
                log.debug(f"  [forum] msg.reply_to is None for msg {msg.id}")
                topic_id = 1

            # Проверяем исключённые топики
            if topic_id:
                excluded = settings.telegram_exclude_topics.get(pos_id, set())
                if topic_id in excluded:
                    return  # пропускаем сообщение из исключённого топика
                # Ищем название в кэше
                topics = forum_topics.get(pos_id, {})
                if topic_id in topics:
                    channel_name = f"{base_name} / {topics[topic_id]}"
                else:
                    # Лениво загружаем: запрашиваем сообщение-создатель топика
                    try:
                        topic_msg = await client.get_messages(pos_id, ids=topic_id)
                        if topic_msg and hasattr(topic_msg, "action"):
                            title = getattr(topic_msg.action, "title", None)
                            if title:
                                if pos_id not in forum_topics:
                                    forum_topics[pos_id] = {1: "General"}
                                forum_topics[pos_id][topic_id] = title
                                channel_name = f"{base_name} / {title}"
                                log.info(f"  Новый топик: {title} (id={topic_id})")
                            else:
                                channel_name = f"{base_name} / Topic {topic_id}"
                        else:
                            channel_name = f"{base_name} / Topic {topic_id}"
                    except Exception:
                        channel_name = f"{base_name} / Topic {topic_id}"

        await save_message(msg, channel_name)

    # Бесконечный цикл — слушаем сообщения
    try:
        await client.run_until_disconnected()
    except KeyboardInterrupt:
        log.info("Остановка по Ctrl+C")
    finally:
        await close_db()
        log.info("Telegram Monitor остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
