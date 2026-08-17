"""
Private Mirror Dedup Engine (spec 001 task 5).

Consumes Redis streams:
  - mirror_routed_events  → named-caller fast-path (CALLER_A/CALLER_B/COMMUNITY)
  - mirror_events         → merged-feed pipeline с дедупом + Variant C emission

**Variant C emission**: одно editable сообщение в окне.
  t=0 (первое упоминание) → store в Redis, ничего в TG
  t=X (n_sources достиг threshold) → publish, сохранить telegram_msg_id
  t<window_end (новые подтверждения) → edit_message_in_topic в место (silent updates)
  t=window_end → финальный edit с ✅ префиксом + lock
  late echoes → инкремент late_echo_count в PG, в TG ничего

Запуск:
    python -m services.private_mirror_dedup

systemd unit: `xanalyst-private-mirror-dedup.service` (MemoryMax=150M).
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.redis_client import init_redis, close_redis, get_redis
from shared.notifier import (
    send_to_topic,
    send_photo_to_topic,
    edit_message_in_topic,
    resolve_mirror_topic_id,
    markdown_to_telegram_html,
)
from shared.source_routing import CATEGORY_PARAMS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("private_mirror_dedup")

# --- Redis keys / streams ---
STREAM_EVENTS = "mirror_events"
STREAM_ROUTED = "mirror_routed_events"
CONSUMER_GROUP_EVENTS = "dedup"
CONSUMER_GROUP_ROUTED = "fastpath"
CONSUMER_NAME = "worker"

KEY_BUCKET = "mirror:bucket:{fingerprint}"
KEY_SOURCES = "mirror:bucket:sources:{fingerprint}"
KEY_ACTIVE = "mirror:bucket:active"      # ZSET, score=window_close_deadline_ts
KEY_STARTED_AT = "mirror:service_started_at"

# Reply threading: source message → our TG message id mapping (named-caller path).
# Хранится 48h — реплай-треды в named-caller чатах редко длиннее.
KEY_MSG_MAP = "mirror:msg_map:{chat_id}:{source_msg_id}"
MSG_MAP_TTL_SEC = 48 * 3600

WINDOW_CLOSE_POLL_SEC = 2


# =============================================================================
# Helpers — fingerprint, formatting
# =============================================================================

def make_fingerprint(category: str, tickers: list[str], cas: list[str]) -> str | None:
    """
    V1 simplified fingerprint:
      ARB / PUMP_DUMP → use first ticker as key (symbol-only granularity)
      MEME → use first CA (canonical)
      WHALE → first asset (ticker or CA) + 5-min time bucket
    Returns None if no extractable asset (event won't be merged).
    """
    if category in ("ARB", "PUMP_DUMP"):
        if not tickers:
            return None
        sym = tickers[0]
        prefix = "arb" if category == "ARB" else "pd"
        return f"{prefix}:{sym}"

    if category == "MEME":
        if not cas:
            # fallback на тикер если CA не выделена
            if not tickers:
                return None
            return f"mem:ticker:{tickers[0]}"
        ca = cas[0]
        chain = "eth" if ca.startswith("0x") else "sol"
        return f"mem:{chain}:{ca}"

    if category == "WHALE":
        asset = (tickers[0] if tickers else (cas[0] if cas else None))
        if not asset:
            return None
        # 30-минутный bucket: события китов реже, нужно ловить распределённый поток
        # одного и того же актива от разных трекеров (Arkham, HL, on-chain парсеры).
        bucket = int(time.time()) // 1800
        return f"whale:{asset}:{bucket}"

    return None


def detect_chain(ca: str) -> str:
    """Грубо: 0x... → eth, base58 → sol."""
    return "eth" if ca.startswith("0x") else "sol"


# =============================================================================
# Извлечение направления/спреда/бирж из текста (best-effort, для UX)
# =============================================================================

# Pump-направление: ▲/🚀/UP/PUMP/+%/▼/🔻/🟥/DOWN/DUMP/-%
DIRECTION_PATTERNS = {
    "UP":   re.compile(r'[▲🚀🟢⬆️]|\+\d+(?:[.,]\d+)?\s*%|\b(?:PUMP|UP|LONG|BUY)\b', re.IGNORECASE),
    "DOWN": re.compile(r'[▼🔻🟥⬇️]|-\d+(?:[.,]\d+)?\s*%|\b(?:DUMP|DOWN|SHORT|SELL)\b', re.IGNORECASE),
}

# Процент движения: ±N.NN%
PCT_MOVE_PATTERN = re.compile(r'([+-]?\d+(?:[.,]\d+)?)\s*%')

# Спред: "Spread: X.XX%" или просто "X.XX%" в контексте спред-сигнала
SPREAD_PATTERN = re.compile(r'(?:Spread|Спред|spread):\s*([+-]?\d+(?:[.,]\d+)?)\s*%', re.IGNORECASE)

# Биржевые пары: "MEXC → Gate" / "Bitget short / Binance long" / "Dex → Mexc" / "Mexc-Gate"
EXCHANGE_PAIR_PATTERN = re.compile(
    r'\b(MEXC|Mexc|mexc|Gate|gateio|GateIo|Gate\.io|Binance|binance|Bitget|bitget|Bybit|bybit|OKX|okx|HTX|htx|KuCoin|kucoin|Raydium|raydium|Uniswap|uniswap|Aster|aster|Hyperliquid|hyperliquid|Bingx|bingx|LBank|lbank|DEX|Dex|dex|CEX|cex|Spot|Fut|Futures)\b'
)

# Биржа в скобках или после "на": "ENMUSDT на MEXC", "на Gate", "(Binance)"
EXCHANGE_ON_PATTERN = re.compile(
    r'\b(?:на|on|@)\s+(MEXC|Gate|Binance|Bitget|Bybit|OKX|HTX|KuCoin|Raydium|Uniswap|Aster|Hyperliquid|Bingx|LBank)\b',
    re.IGNORECASE,
)

# Whale-specific patterns
WALLET_PATTERN = re.compile(r'0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44}')
WALLET_SHORT_PATTERN = re.compile(r'\b(0x[a-fA-F0-9]{4,8}[…\.]{1,3}[a-fA-F0-9]{4,8})\b')  # 0xab...cd shortened
USD_AMOUNT_PATTERN = re.compile(
    r'(?:\$|USD\s*)?(\d+(?:[.,]\d+)?)\s*(?:[kKкКmMмМbBбБ]|тыс|млн|млрд|thousand|million|billion)?\s*(?:\$|USD|долларов)?',
)
USD_AMOUNT_STRICT = re.compile(
    r'(?:\$\s*|USD\s*|стоимост[ьи]\s*|на сумму\s*)?'
    r'(\d{1,4}(?:[.,]\d+)?)\s*'
    r'([kKкКmMмМbBбБ]|тыс\b|млн\b|млрд\b|thousand|million|billion)\b',
    re.IGNORECASE,
)
WHALE_DIRECTION = {
    "INFLOW":  re.compile(r'\b(?:depositing|inflow|deposit|in(?:\s|$)|from)\b', re.IGNORECASE),
    "OUTFLOW": re.compile(r'\b(?:withdraw|outflow|out(?:\s|$)|to(?:\s|$))\b', re.IGNORECASE),
    "TRANSFER": re.compile(r'\b(?:transfer|перевод|move)\b', re.IGNORECASE),
}


def extract_wallet_short(text: str) -> str | None:
    """Найти кошелёк в тексте (полный или сокращённый формат)."""
    m = WALLET_SHORT_PATTERN.search(text)
    if m:
        return m.group(1)
    m = WALLET_PATTERN.search(text)
    if m:
        addr = m.group(0)
        if len(addr) > 16:
            return f"{addr[:6]}…{addr[-4:]}"
        return addr
    return None


def extract_usd_amount(text: str) -> str | None:
    """Найти первое значимое $-amount в тексте: $4.2M, 14M$, 661K и т.д."""
    for m in USD_AMOUNT_STRICT.finditer(text):
        num_str = m.group(1).replace(",", ".")
        unit = m.group(2).lower()
        try:
            val = float(num_str)
        except ValueError:
            continue
        # Multipliers
        if unit in ("k", "к", "тыс", "thousand"):
            val *= 1_000
        elif unit in ("m", "м", "млн", "million"):
            val *= 1_000_000
        elif unit in ("b", "б", "млрд", "billion"):
            val *= 1_000_000_000
        # Только значимые суммы (>=$50k для whale)
        if val < 50_000:
            continue
        # Format back
        if val >= 1_000_000_000:
            return f"${val / 1_000_000_000:.2f}B"
        if val >= 1_000_000:
            return f"${val / 1_000_000:.2f}M"
        if val >= 1_000:
            return f"${val / 1_000:.0f}K"
        return f"${val:.0f}"
    return None


def extract_direction(text: str) -> str | None:
    """Pump (UP) или Dump (DOWN) из текста, None если не определено."""
    up = bool(DIRECTION_PATTERNS["UP"].search(text))
    down = bool(DIRECTION_PATTERNS["DOWN"].search(text))
    if up and not down:
        return "↑ PUMP"
    if down and not up:
        return "↓ DUMP"
    if up and down:
        return None  # ambiguous (например, исторический контекст)
    return None


def extract_pct(text: str, max_abs: float = 99999) -> str | None:
    """Первый разумный процент из текста."""
    for m in PCT_MOVE_PATTERN.findall(text):
        try:
            val = float(m.replace(",", "."))
            if abs(val) <= max_abs:
                sign = "+" if val > 0 else ""
                return f"{sign}{val:g}%"
        except ValueError:
            continue
    return None


def extract_spread_pct(text: str) -> str | None:
    """Спред % с явным labelом 'Spread:'/'Спред:', если есть."""
    m = SPREAD_PATTERN.search(text)
    if m:
        try:
            val = float(m.group(1).replace(",", "."))
            return f"{val:g}%"
        except ValueError:
            pass
    return None


def extract_exchanges(text: str) -> tuple[str | None, str | None]:
    """Пара бирж: (from, to) если можно определить. Иначе (None, None)."""
    # Сначала пробуем явный → или ->
    arrow_pat = re.compile(
        r'\b([A-Za-z]{3,12})\s*(?:→|->|-|\\)\s*([A-Za-z]{3,12})\b'
    )
    m = arrow_pat.search(text)
    if m:
        return m.group(1).capitalize(), m.group(2).capitalize()

    # Иначе пытаемся: "X short / Y long" pattern
    pat2 = re.compile(
        r'\b([A-Za-z]{3,12})\s+(?:short|long|шорт|лонг)\s*/\s*([A-Za-z]{3,12})\s+(?:short|long|шорт|лонг)',
        re.IGNORECASE,
    )
    m = pat2.search(text)
    if m:
        return m.group(1).capitalize(), m.group(2).capitalize()

    # Просто "на BIRZHA" / "(BIRZHA)"
    m = EXCHANGE_ON_PATTERN.search(text)
    if m:
        return m.group(1).capitalize(), None

    return None, None


def format_first_text_preview(text: str, max_chars: int = 3500) -> str:
    """
    Превью сообщения с конвертацией Telegram-Markdown в HTML.
    [text](url) → <a href="url">text</a>
    **bold**   → <b>bold</b>
    `code`     → <code>code</code>

    Cut на ГРАНИЦАХ СТРОК. Лимита по числу строк нет — таблицы ARB/PUMP
    с 10+ биржами должны цитироваться целиком. Ограничение только по
    char-капу (с запасом под TG message cap 4096 + HTML expansion).
    """
    if not text:
        return ""
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if not lines:
        return ""

    # Жадно добавляем строки пока укладываемся в max_chars
    selected: list[str] = []
    total_len = 0
    for line in lines:
        if selected and total_len + len(line) + 1 > max_chars:
            break
        selected.append(line)
        total_len += len(line) + 1

    truncated_flag = len(selected) < len(lines)

    result = "\n".join(selected)
    html = markdown_to_telegram_html(result)
    if truncated_flag:
        html += "\n<i>… (свернуто)</i>"
    return html


# Major crypto tickers (whitelist для WHALE asset detection).
# В WHALE messages обычно фигурирует один из них.
WHALE_KNOWN_TOKENS = re.compile(
    r'\b('
    r'BTC|ETH|SOL|USDC|USDT|USD|BUSD|DAI|TUSD|'
    r'BNB|AVAX|MATIC|POL|XRP|LINK|ADA|DOGE|SHIB|TRX|DOT|ATOM|TON|'
    r'HYPE|WIF|PEPE|BONK|FLOKI|MOG|TRUMP|WLD|JTO|JUP|RAY|'
    r'ARB|OP|BASE|MANTLE|STRK|TIA|SEI|INJ|NEAR|FIL'
    r')\b'
)


def extract_whale_token(text: str) -> str | None:
    """Найти конкретный токен в whale-сообщении (Arkham/HL/Hyperliquid/on-chain).
    Возвращает first match из whitelist'а major-tokens (для дедупа).
    """
    m = WHALE_KNOWN_TOKENS.search(text)
    return m.group(1) if m else None


def format_emission_text(
    category: str,
    asset: str,
    n_sources: int,
    sources: list[str],
    first_seen_ts: int,
    leader_source: str,
    first_text: str | None = None,
    locked: bool = False,
) -> str:
    """
    Compact формат для merged-feed (spec 001 task 5).
    locked=True → префикс ✅ (после закрытия окна).
    Включает первое сообщение от leader-источника для контекста (UX hotfix).
    """
    cat_label = {
        "ARB":       "ARB",
        "PUMP_DUMP": "PUMP/DUMP",
        "MEME":      "MEME",
        "WHALE":     "WHALE",
    }.get(category, category)

    prefix = "✅" if locked else "📊"
    age_sec = int(time.time()) - first_seen_ts

    # Каноничный asset вид
    is_long_asset = asset.startswith("0x") or len(asset) >= 30
    asset_label = f"<code>{asset[:8]}...{asset[-6:]}</code>" if is_long_asset else f"${asset}"

    # Header с извлечёнными мета-данными
    header_extras: list[str] = []
    if first_text:
        if category == "PUMP_DUMP":
            direction = extract_direction(first_text)
            pct = extract_pct(first_text, max_abs=300)
            if direction:
                header_extras.append(direction)
            if pct:
                header_extras.append(pct)
            fr, to = extract_exchanges(first_text)
            if fr:
                header_extras.append(f"on {fr}")
        elif category == "ARB":
            spread = extract_spread_pct(first_text) or extract_pct(first_text, max_abs=80)
            fr, to = extract_exchanges(first_text)
            if fr and to:
                header_extras.append(f"{fr}→{to}")
            elif fr:
                header_extras.append(fr)
            if spread:
                header_extras.append(f"spread {spread}")
        elif category == "WHALE":
            amount = extract_usd_amount(first_text)
            wallet = extract_wallet_short(first_text)
            fr, to = extract_exchanges(first_text)
            if amount:
                header_extras.append(amount)
            if fr and to:
                header_extras.append(f"{fr}→{to}")
            elif fr:
                header_extras.append(f"on {fr}")
            if wallet:
                header_extras.append(f"wallet {wallet}")

    extras_str = f"  ·  {' · '.join(header_extras)}" if header_extras else ""

    lines = [
        f"{prefix} <b>{cat_label}</b> · {asset_label}{extras_str}",
        f"{n_sources}× sources in {age_sec}s · first: <i>{leader_source}</i>",
    ]

    # Sample от leader источника
    if first_text:
        preview = format_first_text_preview(first_text)
        if preview:
            lines.append(f"\n<blockquote>{preview}</blockquote>")

    # Список других источников
    if sources:
        others = [s for s in sources if s != leader_source]
        if others:
            srcs = ", ".join(others[:6])
            if len(others) > 6:
                srcs += f", +{len(others) - 6} more"
            lines.append(f"Also: {srcs}")

    return "\n".join(lines)


def format_named_caller_text(source_label: str, sender: str, text: str) -> str:
    """Compact формат для named-caller (CALLER_A/CALLER_B/COMMUNITY) — пересылаемое сообщение."""
    # Обрезаем длинные сообщения, escape HTML
    safe_text = (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )
    if len(safe_text) > 2500:
        safe_text = safe_text[:2500] + "..."
    return f"<b>{source_label}</b>\n\n{safe_text}"


# =============================================================================
# Named-caller fast-path consumer
# =============================================================================

async def named_caller_loop():
    """Читает mirror_routed_events stream → публикация в личный TG-топик."""
    r = get_redis()

    # Ensure consumer group exists
    try:
        await r.xgroup_create(STREAM_ROUTED, CONSUMER_GROUP_ROUTED, id="$", mkstream=True)
        log.info(f"Создана consumer group {CONSUMER_GROUP_ROUTED} на {STREAM_ROUTED}")
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create routed: {e}")

    log.info("Named-caller fast-path loop started")

    while True:
        try:
            events = await r.xreadgroup(
                CONSUMER_GROUP_ROUTED, CONSUMER_NAME,
                {STREAM_ROUTED: ">"},
                count=10, block=5000,
            )
        except Exception as e:
            log.error(f"xreadgroup routed: {e}")
            await asyncio.sleep(1)
            continue

        for stream_name, items in events:
            for stream_id, data in items:
                try:
                    await handle_named_caller(data)
                    await r.xack(STREAM_ROUTED, CONSUMER_GROUP_ROUTED, stream_id)
                except Exception as e:
                    log.error(f"named-caller handler error: {e} data={data}")


async def handle_named_caller(data: dict):
    target = (data.get("named_caller") or "").upper()
    if not target:
        return
    topic_id = resolve_mirror_topic_id(target)
    if topic_id is None:
        log.info(f"TELEGRAM_MIRROR_{target}_TOPIC_ID не задан — skip publication, msg остаётся в БД")
        return

    source_label = data.get("source_label") or target
    text = data.get("text") or ""
    sender = data.get("sender") or ""
    media_path = data.get("media_path") or ""
    chat_id_str = data.get("chat_id") or ""
    source_msg_id_str = data.get("message_id") or ""
    reply_to_msg_id_str = data.get("reply_to_msg_id") or ""

    # Lookup TG msg_id родителя (если это реплай и родитель уже эмитили).
    # allow_sending_without_reply=True на нашей стороне = если родитель пропал из map
    # или его не было — TG пошлёт без реплая, не упадёт.
    parent_tg_msg_id: int | None = None
    if reply_to_msg_id_str and chat_id_str:
        try:
            r = get_redis()
            val = await r.get(KEY_MSG_MAP.format(
                chat_id=chat_id_str, source_msg_id=reply_to_msg_id_str,
            ))
            if val:
                parent_tg_msg_id = int(val)
        except Exception as e:
            log.debug(f"msg_map lookup failed: {e}")

    async def _store_mapping(tg_msg_id: int) -> None:
        """Сохранить (chat_id, source_msg_id) → tg_msg_id для будущих реплаев."""
        if not (chat_id_str and source_msg_id_str):
            return
        try:
            r = get_redis()
            await r.set(
                KEY_MSG_MAP.format(chat_id=chat_id_str, source_msg_id=source_msg_id_str),
                str(tg_msg_id),
                ex=MSG_MAP_TTL_SEC,
            )
        except Exception as e:
            log.debug(f"msg_map store failed: {e}")

    # Photo passthrough path (только для named-caller, только photo per UX prefs)
    if media_path and os.path.exists(media_path):
        safe_text = (
            text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
        )
        # TG sendPhoto caption жёстко 1024 chars. Резерв ~50 на header `📷 <b>...</b>\n\n`.
        # Если оригинал длиннее — кидаем хвост отдельным send_to_topic.
        body_limit = 970
        body = safe_text[:body_limit]
        overflow = safe_text[body_limit:]
        caption = f"📷 <b>{source_label}</b>\n\n{body}"
        message_id = send_photo_to_topic(
            media_path, caption, topic_id,
            reply_to_message_id=parent_tg_msg_id,
        )
        try:
            os.unlink(media_path)
        except OSError as e:
            log.debug(f"unlink {media_path} failed: {e}")
        if message_id:
            log.info(f"[NAMED:{target}] published PHOTO msg_id={message_id} src={source_label}")
            await _store_mapping(message_id)
            if overflow:
                # Overflow реплаим к нашему фото-сообщению — чтобы тред оставался цельным
                send_to_topic(
                    f"<i>(продолжение)</i>\n{overflow}",
                    topic_id,
                    reply_to_message_id=message_id,
                )
            return
        # sendPhoto не прошёл — fallback на текст
        log.warning(f"[NAMED:{target}] photo publication failed, fallback к text-only")

    msg_text = format_named_caller_text(source_label, sender, text)
    message_id = send_to_topic(
        msg_text, topic_id,
        reply_to_message_id=parent_tg_msg_id,
    )
    if message_id:
        log.info(f"[NAMED:{target}] published msg_id={message_id} src={source_label}")
        await _store_mapping(message_id)
    else:
        log.warning(f"[NAMED:{target}] publication failed")


# =============================================================================
# Merged-feed consumer (Variant C emission)
# =============================================================================

async def merged_feed_loop():
    """Читает mirror_events, обновляет buckets, эмиссия по threshold."""
    r = get_redis()

    try:
        await r.xgroup_create(STREAM_EVENTS, CONSUMER_GROUP_EVENTS, id="$", mkstream=True)
        log.info(f"Создана consumer group {CONSUMER_GROUP_EVENTS} на {STREAM_EVENTS}")
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            log.warning(f"xgroup_create events: {e}")

    log.info("Merged-feed loop started")

    while True:
        try:
            events = await r.xreadgroup(
                CONSUMER_GROUP_EVENTS, CONSUMER_NAME,
                {STREAM_EVENTS: ">"},
                count=20, block=5000,
            )
        except Exception as e:
            log.error(f"xreadgroup events: {e}")
            await asyncio.sleep(1)
            continue

        for stream_name, items in events:
            for stream_id, data in items:
                try:
                    await handle_merged_event(data)
                    await r.xack(STREAM_EVENTS, CONSUMER_GROUP_EVENTS, stream_id)
                except Exception as e:
                    log.error(f"merged handler error: {e} data={data}")


async def handle_merged_event(data: dict):
    category = (data.get("category") or "").upper()
    if not category or category not in CATEGORY_PARAMS:
        # Не категоризовано — solo, не в merged
        return

    tickers = json.loads(data.get("tickers") or "[]")
    cas = json.loads(data.get("cas") or "[]")
    source_label = data.get("source_label") or "unknown"
    event_ts = int(data.get("ts") or time.time())
    event_text = data.get("text") or ""

    # WHALE: попытаемся найти конкретный токен в тексте (ETH/BTC/SOL/...)
    # Этот токен становится preferred asset для фингерпринта и display'я.
    if category == "WHALE":
        whale_token = extract_whale_token(event_text)
        if whale_token:
            # Вставляем впереди tickers чтобы make_fingerprint его использовал
            tickers = [whale_token] + [t for t in tickers if t != whale_token]

    fingerprint = make_fingerprint(category, tickers, cas)
    if not fingerprint:
        # Нет извлечённого asset'а — skip
        return

    params = CATEGORY_PARAMS[category]
    threshold = params["threshold"]
    window_sec = params["window_sec"]

    asset = tickers[0] if tickers else cas[0]
    asset_chain = detect_chain(cas[0]) if cas else (None if not tickers else "cex")

    await update_bucket(
        fingerprint=fingerprint,
        category=category,
        asset=asset,
        asset_chain=asset_chain,
        source_label=source_label,
        event_ts=event_ts,
        event_text=event_text,
        threshold=threshold,
        window_sec=window_sec,
    )


async def update_bucket(
    fingerprint: str,
    category: str,
    asset: str,
    asset_chain: str | None,
    source_label: str,
    event_ts: int,
    threshold: int,
    window_sec: int,
    event_text: str = "",
):
    r = get_redis()
    key = KEY_BUCKET.format(fingerprint=fingerprint)
    sources_key = KEY_SOURCES.format(fingerprint=fingerprint)

    # Уникальные источники (SET)
    added_new = await r.sadd(sources_key, source_label)
    n_sources = await r.scard(sources_key)
    await r.expire(sources_key, window_sec * 3)

    # Bucket HASH
    pipe = r.pipeline()
    pipe.hsetnx(key, "first_seen", event_ts)
    pipe.hsetnx(key, "leader_source", source_label)
    pipe.hsetnx(key, "category", category)
    pipe.hsetnx(key, "asset", asset)
    pipe.hsetnx(key, "fingerprint", fingerprint)
    pipe.hsetnx(key, "window_sec", window_sec)
    pipe.hsetnx(key, "threshold", threshold)
    # first_text — сохраняем только первое сообщение (leader), не перезаписываем
    if event_text:
        pipe.hsetnx(key, "first_text", event_text[:3000])
    pipe.hset(key, mapping={
        "last_seen": event_ts,
        "n_sources": n_sources,
    })
    if asset_chain:
        pipe.hsetnx(key, "asset_chain", asset_chain)
    pipe.expire(key, window_sec * 3)
    await pipe.execute()

    # Регистрируем в active ZSET для window close worker
    first_seen = int((await r.hget(key, "first_seen")) or event_ts)
    deadline = first_seen + window_sec
    await r.zadd(KEY_ACTIVE, {fingerprint: deadline})

    # Получим текущий state
    state = await r.hgetall(key)
    emitted_locked = state.get("emitted_locked") == "1"
    telegram_msg_id = state.get("telegram_msg_id")
    leader_source = state.get("leader_source") or source_label

    # Не редактируем после lock (только late_echo)
    if emitted_locked:
        # Late echo — увеличить counter
        pipe = r.pipeline()
        pipe.hincrby(key, "late_echo_count", 1)
        await pipe.execute()
        # Также инкрементировать в PG если есть merged row
        await pg_increment_late_echo(fingerprint)
        log.info(f"[LATE-ECHO] {fingerprint} n_sources={n_sources}")
        return

    sources_list = sorted(await r.smembers(sources_key))
    first_text = state.get("first_text")

    if telegram_msg_id is None and n_sources >= threshold:
        # Первое попадание в TG
        topic_id = resolve_mirror_topic_id(category)
        if topic_id is None:
            log.info(f"TELEGRAM_MIRROR_{category}_TOPIC_ID не задан — skip emission, фиксируем в PG только")
        else:
            text = format_emission_text(
                category=category,
                asset=asset,
                n_sources=n_sources,
                sources=sources_list,
                first_seen_ts=first_seen,
                leader_source=leader_source,
                first_text=first_text,
                locked=False,
            )
            msg_id = send_to_topic(text, topic_id)
            if msg_id:
                await r.hset(key, mapping={
                    "telegram_msg_id": msg_id,
                    "telegram_topic_id": topic_id,
                })
                # Записать row в PG (single source of truth)
                await pg_create_merged_signal(
                    fingerprint=fingerprint,
                    category=category,
                    asset=asset,
                    asset_chain=asset_chain,
                    first_seen=first_seen,
                    last_seen=event_ts,
                    n_sources=n_sources,
                    sources_list=sources_list,
                    leader_source=leader_source,
                    time_to_consensus_sec=event_ts - first_seen,
                    telegram_msg_id=msg_id,
                    telegram_topic_id=topic_id,
                )
                log.info(f"[EMIT] {fingerprint} threshold={threshold} reached, msg_id={msg_id}")
            else:
                log.warning(f"[EMIT-FAIL] {fingerprint} send_to_topic returned None")

    elif telegram_msg_id is not None:
        # Уже эмитировано в этом окне — edit с обновлёнными данными
        topic_id = int(state.get("telegram_topic_id") or 0) or resolve_mirror_topic_id(category)
        if topic_id is None:
            return
        text = format_emission_text(
            category=category,
            asset=asset,
            n_sources=n_sources,
            sources=sources_list,
            first_seen_ts=first_seen,
            leader_source=leader_source,
            first_text=first_text,
            locked=False,
        )
        ok = edit_message_in_topic(text, int(telegram_msg_id), topic_id)
        if not ok:
            # Edit fail — увеличить counter, проверить "не найдено" → NULL telegram_msg_id для re-emit
            await r.hincrby(key, "edit_failed_count", 1)
            await pg_increment_edit_failed(fingerprint)
            log.warning(f"[EDIT-FAIL] {fingerprint} msg_id={telegram_msg_id}")
        else:
            # Обновить PG row
            await pg_update_merged_signal(
                fingerprint=fingerprint,
                last_seen=event_ts,
                n_sources=n_sources,
                sources_list=sources_list,
            )
            log.info(f"[EDIT] {fingerprint} n_sources={n_sources}")


# =============================================================================
# Window close worker — закрывает истёкшие окна, финальный edit + lock
# =============================================================================

async def window_close_loop():
    log.info("Window close loop started")
    r = get_redis()
    while True:
        try:
            now = int(time.time())
            expired = await r.zrangebyscore(KEY_ACTIVE, 0, now)
            for fingerprint in expired:
                try:
                    await close_window(fingerprint)
                except Exception as e:
                    log.error(f"close_window {fingerprint} error: {e}")
                finally:
                    await r.zrem(KEY_ACTIVE, fingerprint)
        except Exception as e:
            log.error(f"window_close_loop iter: {e}")
        await asyncio.sleep(WINDOW_CLOSE_POLL_SEC)


async def close_window(fingerprint: str):
    r = get_redis()
    key = KEY_BUCKET.format(fingerprint=fingerprint)
    sources_key = KEY_SOURCES.format(fingerprint=fingerprint)

    state = await r.hgetall(key)
    if not state:
        return

    if state.get("emitted_locked") == "1":
        return  # уже закрыто

    telegram_msg_id = state.get("telegram_msg_id")
    if telegram_msg_id is None:
        # Solo — не достигли threshold за окно. Просто помечаем закрытым в PG если нужно (не в TG)
        log.debug(f"[SOLO-CLOSE] {fingerprint} below threshold, no emission")
        await r.hset(key, "emitted_locked", "1")
        return

    # Финальный edit с ✅
    topic_id = int(state.get("telegram_topic_id") or 0)
    category = state.get("category", "")
    asset = state.get("asset", "?")
    first_seen = int(state.get("first_seen") or time.time())
    n_sources = int(state.get("n_sources") or 1)
    leader_source = state.get("leader_source") or "?"
    first_text = state.get("first_text")
    sources_list = sorted(await r.smembers(sources_key))

    text = format_emission_text(
        category=category,
        asset=asset,
        n_sources=n_sources,
        sources=sources_list,
        first_seen_ts=first_seen,
        leader_source=leader_source,
        first_text=first_text,
        locked=True,
    )
    ok = edit_message_in_topic(text, int(telegram_msg_id), topic_id)
    if ok:
        log.info(f"[LOCK] {fingerprint} n_sources={n_sources} locked")
    else:
        log.warning(f"[LOCK-FAIL] {fingerprint} edit on close failed (msg deleted?)")

    # Lock в Redis + PG
    await r.hset(key, mapping={
        "emitted_locked": "1",
        "window_closed_at": int(time.time()),
    })
    await pg_lock_merged_signal(
        fingerprint=fingerprint,
        window_closed_at=datetime.utcnow(),
        n_sources=n_sources,
        sources_list=sources_list,
    )


# =============================================================================
# PG helpers — single source of truth для crash recovery
# =============================================================================

async def pg_create_merged_signal(
    fingerprint: str,
    category: str,
    asset: str,
    asset_chain: str | None,
    first_seen: int,
    last_seen: int,
    n_sources: int,
    sources_list: list[str],
    leader_source: str,
    time_to_consensus_sec: int,
    telegram_msg_id: int,
    telegram_topic_id: int,
):
    pool = get_pool()
    sources_jsonb = json.dumps([{"source": s} for s in sources_list])
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO mirror_merged_signals
                (category, fingerprint, first_seen, last_seen, n_sources,
                 sources, leader_source, time_to_consensus_sec, asset,
                 asset_chain, telegram_msg_id, telegram_topic_id)
            VALUES ($1, $2, to_timestamp($3), to_timestamp($4), $5,
                    $6::jsonb, $7, $8, $9, $10, $11, $12)
            """,
            category.lower(), fingerprint, first_seen, last_seen, n_sources,
            sources_jsonb, leader_source, time_to_consensus_sec, asset,
            asset_chain, telegram_msg_id, telegram_topic_id,
        )


async def pg_update_merged_signal(
    fingerprint: str,
    last_seen: int,
    n_sources: int,
    sources_list: list[str],
):
    pool = get_pool()
    sources_jsonb = json.dumps([{"source": s} for s in sources_list])
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE mirror_merged_signals
            SET last_seen = to_timestamp($2),
                n_sources = $3,
                sources = $4::jsonb
            WHERE fingerprint = $1 AND emitted_locked = FALSE
            """,
            fingerprint, last_seen, n_sources, sources_jsonb,
        )


async def pg_lock_merged_signal(
    fingerprint: str,
    window_closed_at: datetime,
    n_sources: int,
    sources_list: list[str],
):
    pool = get_pool()
    sources_jsonb = json.dumps([{"source": s} for s in sources_list])
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE mirror_merged_signals
            SET window_closed_at = $2,
                emitted_locked = TRUE,
                n_sources = $3,
                sources = $4::jsonb
            WHERE fingerprint = $1
            """,
            fingerprint, window_closed_at, n_sources, sources_jsonb,
        )


async def pg_increment_late_echo(fingerprint: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE mirror_merged_signals
            SET late_echo_count = late_echo_count + 1
            WHERE fingerprint = $1
            """,
            fingerprint,
        )


async def pg_increment_edit_failed(fingerprint: str):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE mirror_merged_signals
            SET edit_failed_count = edit_failed_count + 1
            WHERE fingerprint = $1 AND emitted_locked = FALSE
            """,
            fingerprint,
        )


# =============================================================================
# Recovery — orphan handling при старте
# =============================================================================

async def recover_orphans():
    """
    При старте: брошенные открытые окна (>1 час) форсированно закрываем в PG.
    Если telegram_msg_id есть — сообщение в TG останется как было.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            """
            UPDATE mirror_merged_signals
            SET emitted_locked = TRUE,
                window_closed_at = NOW()
            WHERE window_closed_at IS NULL
              AND created_at < NOW() - INTERVAL '1 hour'
            RETURNING (SELECT count(*) FROM mirror_merged_signals
                       WHERE window_closed_at = NOW())
            """
        ) or 0
        if n:
            log.warning(f"Recovery: force-closed {n} orphan merged signals (>1h open)")


# =============================================================================
# Main
# =============================================================================

async def main():
    log.info("Старт Private Mirror Dedup Engine")
    await init_db()
    await init_redis()
    r = get_redis()

    await r.set(KEY_STARTED_AT, str(int(time.time())))
    await recover_orphans()

    # 3 параллельные корутины:
    tasks = [
        asyncio.create_task(named_caller_loop(), name="named_caller_loop"),
        asyncio.create_task(merged_feed_loop(), name="merged_feed_loop"),
        asyncio.create_task(window_close_loop(), name="window_close_loop"),
    ]
    log.info("Все loops запущены, жду события...")

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info("Остановка по Ctrl+C")
        for t in tasks:
            t.cancel()
    finally:
        await close_db()
        await close_redis()
        log.info("Private Mirror Dedup остановлен.")


if __name__ == "__main__":
    asyncio.run(main())
