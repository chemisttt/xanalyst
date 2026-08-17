"""
Private Mirror Monitor (spec 001 task 2).

Telethon-userbot на втором TG-аккаунте `example_user` (+70000000000).
Слушает 25 aggregator-приваток из INGEST_WHITELIST, извлекает $TICKER и CA,
сохраняет в `channel_messages` с `source_account='private_mirror'`,
публикует events в Redis streams для downstream дедупа/digest.

**Inbound mode only** — никаких send/join/leave (см. threat model в spec 001).
Lint guard: `grep -E "client\\.(send|join|leave|delete)" services/private_mirror_monitor.py` должен возвращать 0.

Запуск:
    python -m services.private_mirror_monitor

systemd unit: `xanalyst-private-mirror-monitor.service` (MemoryMax=150M).
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat, User, Message, MessageMediaPhoto

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.redis_client import init_redis, close_redis, get_redis
from shared.source_routing import classify, INGEST_WHITELIST

# --- Логи ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("private_mirror_monitor")


# --- Regex для извлечения ---
URL_PATTERN = re.compile(r'https?://\S+')

# Тикеры: 4 паттерна для разных форматов aggregator-ботов (hotfix после canary discovery)
# Обнаружено что только 2% сообщений используют классический $TICKER —
# большинство ботов используют backticks (`TICKER`), Markdown bold (**TICKER**),
# или field-name синтаксис (Coin: TICKER, Монета: TICKER).
TICKER_PATTERNS = [
    re.compile(r'\$([A-Z][A-Z0-9]{1,9})\b'),                                    # $TICKER
    re.compile(r'`([A-Z][A-Z0-9]{1,10})`'),                                     # `TICKER` (любой uppercase в бэктиках)
    re.compile(r'\*\*([A-Z][A-Z0-9]{1,10})\*\*'),                               # **TICKER** (любой uppercase в bold)
    re.compile(r'(?:Coin|Token|Symbol|Монета|Тикер):\s*[`*]?([A-Z][A-Z0-9]{1,10})'),  # Coin: TICKER
]

# Suffix'ы которые надо отрезать с конца (LABUSDT → LAB, ETHBTC → ETH)
TICKER_QUOTE_SUFFIXES = ("USDT", "_USDT", "/USDT", "BUSD", "USDC", "BTC", "ETH", "BNB")

# Stopwords — типичные ложные срабатывания (uppercase слова которые не тикеры)
TICKER_STOPWORDS = {
    # Стейблы / валюты
    "USDT", "USDC", "DAI", "BUSD", "USD", "EUR", "RUB",
    # Технические термины
    "API", "URL", "DEX", "CEX", "AMM", "LP", "ATH", "ATL", "MC", "FDV", "TVL", "OI",
    "PNL", "ROI", "BE", "MCAP", "VOL", "TWAP", "VWAP",
    # Действия
    "GO", "BUY", "SELL", "LONG", "SHORT", "STOP", "TP", "SL", "DCA",
    "OPEN", "CLOSE", "ENTRY", "EXIT",
    # Общие
    "ID", "IP", "OK", "NO", "YES", "RT", "PM", "DM", "UTC", "GMT", "MSK",
    # Crypto slang
    "NFA", "DYOR", "WAGMI", "GM", "GN", "NGMI", "FOMO", "FUD", "LFG",
    # Major coins (оставлять отдельно — упоминаются как бенчмарки, не как сигналы)
    "ETH", "BTC", "SOL", "BNB", "TON", "AVAX",
    # Биржи / DEX (часто в **MEXC**, `OKX`, etc.)
    "MEXC", "OKX", "HTX", "BITGET", "BYBIT", "BINANCE", "KUCOIN", "GATEIO", "GATE",
    "ASTER", "RAYDIUM", "ORCA", "UNISWAP", "JUPITER", "DRIFT", "HYPERLIQUID",
    "PHANTOM", "TROJAN", "BANANA", "BONKBOT",
    # Chain'ы (могут быть в **BSC**, `ETH`, etc., но уже в major coins)
    "BSC", "ARB", "OP", "BASE", "POLYGON", "AVAX",
    # Прочее
    "PM2", "WS",
}

SOL_CA_PATTERN = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')
EVM_CA_PATTERN = re.compile(r'\b0x[a-fA-F0-9]{40}\b')


# --- Redis streams ---
STREAM_EVENTS = "mirror_events"           # все события от ingestor'а в dedup engine
STREAM_ROUTED = "mirror_routed_events"    # named-caller fast-path в notifier
HEARTBEAT_KEY = "health:ingestor:private_mirror"
HEARTBEAT_INTERVAL_SEC = 30

# --- Named-caller photo passthrough ---
# Скачиваем фото от named-caller'ов (CALLER_A/CALLER_B/COMMUNITY) во временный каталог,
# dedup читает path из Redis stream и заливает в bot-топик через send_photo_to_topic.
# Cleanup: hourly cron `find /tmp/xanalyst_mirror_photos -mmin +60 -type f -delete`
# на случай если dedup упадёт до unlink.
PHOTO_DIR = "/tmp/xanalyst_mirror_photos"


def extract_urls(text: str | None) -> list[str]:
    if not text:
        return []
    return URL_PATTERN.findall(text)


def _strip_quote_suffix(ticker: str) -> str:
    """LABUSDT → LAB, BONKBTC → BONK. Не трогает короткие токены."""
    for suffix in TICKER_QUOTE_SUFFIXES:
        if ticker.endswith(suffix) and len(ticker) > len(suffix) + 1:
            return ticker[:-len(suffix)]
    return ticker


def extract_tickers(text: str | None) -> list[str]:
    """Извлечь тикеры из разных форматов aggregator-ботов. Stopwords отсеиваются."""
    if not text:
        return []
    found: set[str] = set()
    for pat in TICKER_PATTERNS:
        for m in pat.findall(text):
            t = _strip_quote_suffix(m.upper())
            if t in TICKER_STOPWORDS:
                continue
            if len(t) < 2:
                continue
            found.add(t)
    return sorted(found)


def extract_cas(text: str | None) -> list[str]:
    """SOL base58 + EVM 0x (EVM lowercase). De-duped."""
    if not text:
        return []
    found: set[str] = set()
    for m in EVM_CA_PATTERN.findall(text):
        found.add(m.lower())
    for m in SOL_CA_PATTERN.findall(text):
        # base58-эвристика: должен содержать и цифры, и буквы (не чистые цифры/буквы)
        if any(c.isdigit() for c in m) and any(c.isalpha() for c in m) and not m.startswith("0x"):
            found.add(m)
    return sorted(found)


async def save_message(
    msg: Message,
    chat_id: int,
    topic_id: int | None,
    channel_name: str,
) -> int | None:
    """Сохранить в channel_messages с source_account='private_mirror'. Вернуть id row'а."""
    pool = get_pool()
    text = msg.text or ""

    # Автор
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

    urls = extract_urls(text)
    tickers = extract_tickers(text)
    cas = extract_cas(text)

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO channel_messages
                    (source, source_account, channel_id, channel_name, message_id,
                     author_name, text, has_media, urls,
                     extracted_tickers, extracted_cas, message_date,
                     telegram_topic_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (source, channel_id, message_id) DO NOTHING
                RETURNING id
                """,
                "telegram",                              # source
                "private_mirror",                        # source_account
                chat_id,                                 # channel_id
                channel_name,                            # channel_name
                msg.id,                                  # message_id
                author,                                  # author_name
                text,                                    # text
                msg.media is not None,                   # has_media
                urls,                                    # urls
                tickers,                                 # extracted_tickers
                cas,                                     # extracted_cas
                msg.date.replace(tzinfo=None),           # message_date
                topic_id,                                # telegram_topic_id (spec 002 V1.5)
            )
        if row is None:
            log.debug(f"[{channel_name}] msg {msg.id} duplicate, skip")
            return None
        log.info(
            f"[{channel_name}] {author}: {text[:80]!r} "
            f"tickers={tickers} cas={len(cas)} -> pg_id={row['id']}"
        )
        return row["id"]
    except Exception as e:
        log.error(f"PG insert error: {e}")
        return None


async def resolve_topic_id(client: TelegramClient, chat_id: int, msg: Message) -> int | None:
    """Извлечь topic_id из msg.reply_to для forum chats. None если не форум или General."""
    if not msg.reply_to:
        return None
    # Вложенный ответ: reply_to_top_id = ID корневого сообщения топика
    top_id = getattr(msg.reply_to, "reply_to_top_id", None)
    if top_id is None:
        # Top-level в топике: reply_to_msg_id = ID корневого
        top_id = getattr(msg.reply_to, "reply_to_msg_id", None)
    return top_id


def resolve_reply_parent(msg: Message, topic_id: int | None) -> int | None:
    """Реальный source реплая (не корень форум-топика).

    Telethon `msg.reply_to.reply_to_msg_id` для форумных чатов всегда указывает
    либо на корень топика (top-level пост в топике, НЕ настоящий реплай),
    либо на родителя в треде. `reply_to_top_id` если выставлен = корень топика.

    Returns: msg_id настоящего родителя, либо None если top-level/не реплай.
    """
    if not msg.reply_to:
        return None
    parent = getattr(msg.reply_to, "reply_to_msg_id", None)
    if parent is None:
        return None
    top = getattr(msg.reply_to, "reply_to_top_id", None)
    # Форум: nested reply. parent == top → реплай на корень топика, не настоящий
    if top is not None and parent == top:
        return None
    # Форум top-level (top is None): parent == topic root id → не настоящий
    if topic_id and top is None and parent == topic_id:
        return None
    return parent


async def download_named_caller_photo(msg: Message, chat_id: int) -> str | None:
    """Скачать photo для named-caller passthrough. Только MessageMediaPhoto.

    sticker/gif/video/audio/document — пропускаем (per UX prefs 2026-05-15).
    Возвращает path к файлу или None.
    """
    if not isinstance(msg.media, MessageMediaPhoto):
        return None
    try:
        os.makedirs(PHOTO_DIR, exist_ok=True)
        path = os.path.join(PHOTO_DIR, f"{chat_id}_{msg.id}.jpg")
        result = await msg.download_media(file=path)
        if result and os.path.exists(path):
            return path
        return None
    except Exception as e:
        log.warning(f"download_media failed for msg {msg.id} chat {chat_id}: {e}")
        return None


async def publish_event(
    pg_id: int,
    chat_id: int,
    topic_id: int | None,
    msg: Message,
    text: str,
    tickers: list[str],
    cas: list[str],
    routing: dict,
    media_path: str | None = None,
    reply_to_msg_id: int | None = None,
) -> None:
    """Опубликовать в Redis streams для downstream."""
    r = get_redis()
    payload = {
        "pg_id": str(pg_id),
        "chat_id": str(chat_id),
        "topic_id": str(topic_id) if topic_id is not None else "",
        "message_id": str(msg.id),
        "ts": str(int(msg.date.timestamp())),
        "text": text[:2000],  # cap для Redis stream size
        "tickers": json.dumps(tickers),
        "cas": json.dumps(cas),
        "source_label": routing.get("source_label") or "",
        "named_caller": routing.get("named_caller") or "",
        "category": routing.get("category") or "",
        "media_path": media_path or "",
        "reply_to_msg_id": str(reply_to_msg_id) if reply_to_msg_id else "",
    }
    # Главный stream — все события
    await r.xadd(STREAM_EVENTS, payload, maxlen=10000, approximate=True)
    # Routed stream — fast-path для named-callers (CALLER_A/CALLER_B/COMMUNITY)
    if routing.get("named_caller"):
        await r.xadd(STREAM_ROUTED, payload, maxlen=2000, approximate=True)


async def heartbeat_task():
    """Раз в HEARTBEAT_INTERVAL_SEC обновляет ts в Redis для watchdog."""
    while True:
        try:
            r = get_redis()
            await r.set(HEARTBEAT_KEY, str(int(time.time())), ex=HEARTBEAT_INTERVAL_SEC * 3)
        except Exception as e:
            log.warning(f"Heartbeat write failed: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)


async def main():
    if not settings.telegram_api_id or not settings.telegram_api_hash:
        log.error("TELEGRAM_API_ID или TELEGRAM_API_HASH не заданы")
        return
    if not settings.telegram_private_phone:
        log.error("TELEGRAM_PRIVATE_PHONE не задан — сервис не стартует (см. CLAUDE.md)")
        return

    log.info(f"Старт Private Mirror Monitor (phone={settings.telegram_private_phone})")

    await init_db()
    await init_redis()

    # Кэш названий топиков: (chat_id, topic_id) → human-readable title (для логов)
    topic_titles_cache: dict[tuple[int, int], str] = {}

    project_root = Path(__file__).resolve().parent.parent
    session_path = str(project_root / settings.telegram_private_session_name)

    client = TelegramClient(
        session_path,
        int(settings.telegram_api_id),
        settings.telegram_api_hash,
    )
    await client.connect()
    if not await client.is_user_authorized():
        log.error(
            f"Сессия {session_path}.session не авторизована. "
            f"Запусти scripts/analyze_private_chats.py локально, потом scp на VPS."
        )
        await close_db()
        await close_redis()
        return

    me = await client.get_me()
    log.info(f"Авторизован как: {me.first_name} (@{me.username or '-'}) id={me.id}")

    # Принудительно резолвим entities для всех чатов из whitelist (warm-up cache)
    monitored_chat_ids = list(INGEST_WHITELIST.keys())
    log.info(f"Whitelist: {len(monitored_chat_ids)} приваток для ingestion")

    # Heartbeat task в фоне
    asyncio.create_task(heartbeat_task())

    @client.on(events.NewMessage(chats=monitored_chat_ids))
    async def handler(event):
        msg = event.message
        chat_id = event.chat_id
        # Telethon возвращает chat_id с префиксом -100 для каналов; нормализуем
        pos_id = int(str(chat_id).replace("-100", "")) if chat_id < 0 else chat_id

        label = INGEST_WHITELIST.get(chat_id) or INGEST_WHITELIST.get(pos_id) or "Unknown"

        # Topic_id для форумов
        topic_id = await resolve_topic_id(client, pos_id, msg)

        # Routing decision
        routing = classify(pos_id, topic_id)
        if not routing["ingest"]:
            return  # chat_id не в whitelist (защита от race condition между chats=)

        # Channel name для PG + логов
        if topic_id:
            # Получим title топика для читабельности (lazy cache)
            cache_key = (pos_id, topic_id)
            title = topic_titles_cache.get(cache_key)
            if title is None:
                try:
                    topic_msg = await client.get_messages(pos_id, ids=topic_id)
                    if topic_msg and hasattr(topic_msg, "action"):
                        title = getattr(topic_msg.action, "title", None) or f"Topic {topic_id}"
                    else:
                        title = f"Topic {topic_id}"
                except Exception:
                    title = f"Topic {topic_id}"
                topic_titles_cache[cache_key] = title
            channel_name = f"{label} / {title}"
        else:
            channel_name = label

        # 1. Записать в PG
        pg_id = await save_message(msg, pos_id, topic_id, channel_name)
        if pg_id is None:
            return

        # 2. Скачать photo если named-caller (CALLER_A/CALLER_B/COMMUNITY) — для passthrough в их топики
        media_path = None
        if routing.get("named_caller") and msg.media is not None:
            media_path = await download_named_caller_photo(msg, pos_id)

        # 3. Опубликовать в Redis streams
        text = msg.text or ""
        reply_parent = resolve_reply_parent(msg, topic_id)
        await publish_event(
            pg_id=pg_id,
            chat_id=pos_id,
            topic_id=topic_id,
            msg=msg,
            text=text,
            tickers=extract_tickers(text),
            cas=extract_cas(text),
            routing=routing,
            media_path=media_path,
            reply_to_msg_id=reply_parent,
        )

    log.info("Жду новые сообщения (inbound only, no send/join/leave)...")

    try:
        await client.run_until_disconnected()
    except KeyboardInterrupt:
        log.info("Остановка по Ctrl+C")
    finally:
        await close_db()
        await close_redis()
        log.info("Private Mirror Monitor остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
