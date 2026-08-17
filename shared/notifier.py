"""
Telegram Notifier — отправка сообщений с inline keyboard.

Используется twitter_worker'ом для алертов и daily_summary для сводок.
Работает через raw Telegram Bot API (requests), чтобы не зависеть от
запущенного бота — алерты могут отправляться из любого сервиса.
"""

import json
import logging
import os
import re
import requests

from shared.config import settings

log = logging.getLogger("notifier")


def markdown_to_telegram_html(text: str) -> str:
    """
    Конвертировать Markdown от Perplexity в HTML для Telegram.

    Perplexity возвращает стандартный Markdown (**bold**, [ссылки], сноски [1][2]),
    а Telegram принимает свой HTML-формат (<b>, <a href> и т.д.).
    """
    # 1. Экранируем HTML-спецсимволы (до конвертации!)
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")

    # 2. Убираем сноски Perplexity: [1], [2], [1][2], [IDO research] и т.д.
    #    Обычно стоят в конце предложения, без пробела перед ними
    text = re.sub(r'\[(\d+)\]', '', text)

    # 3. Конвертируем Markdown-ссылки [text](url) → HTML <a>
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'<a href="\2">\1</a>', text)

    # 4. Конвертируем **bold** → <b>bold</b>
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)

    # 5. Конвертируем `code` → <code>code</code>
    text = re.sub(r'`(.+?)`', r'<code>\1</code>', text)

    return text


# Эмодзи для тиров
TIER_EMOJI = {"S": "🔥", "A": "⭐", "B": "💎", "C": "📈", "D": "🚫"}


def _bot_api(method: str, payload: dict) -> dict | None:
    """Вызвать метод Telegram Bot API."""
    if not settings.telegram_bot_token:
        log.warning("TELEGRAM_BOT_TOKEN не задан")
        return None

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=15)
        data = resp.json()
        if not data.get("ok"):
            log.error(f"Telegram API ошибка: {data}")
            return None
        return data.get("result")
    except Exception as e:
        log.error(f"Ошибка отправки в Telegram: {e}")
        return None


def _base_payload(topic: str = "news") -> dict:
    """
    Базовые параметры для отправки в наш чат/топик.

    topic="news"      → TELEGRAM_NOTIFY_TOPIC_ID (сводки, дайджесты)
    topic="early"     → TELEGRAM_EARLY_TOPIC_ID (Twitter-алерты, early проекты)
    topic="watchlist" → TELEGRAM_WATCHLIST_TOPIC_ID (watchlist изменения)
    """
    payload = {"chat_id": int(settings.telegram_notify_chat_id)}
    if topic == "watchlist" and settings.telegram_watchlist_topic_id:
        payload["message_thread_id"] = int(settings.telegram_watchlist_topic_id)
    elif topic == "early" and settings.telegram_early_topic_id:
        payload["message_thread_id"] = int(settings.telegram_early_topic_id)
    elif settings.telegram_notify_topic_id:
        payload["message_thread_id"] = int(settings.telegram_notify_topic_id)
    return payload


def send_twitter_alert(handle: str, profile_data: dict, metrics: dict, reused: dict) -> bool:
    """
    Отправить алерт о Twitter-анализе с inline keyboard.

    profile_data — словарь с полями: display_name, followers_count,
                   following_count, bio, verified, is_blue_verified, location
    """
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        return False

    tier = metrics["tier"]
    emoji = TIER_EMOJI.get(tier, "")
    is_early = metrics.get("is_early", False)

    # Лейбл: "EARLY S-TIER" для early projects
    tier_label = f"EARLY {tier}-TIER" if is_early else f"{tier}-TIER"

    # Verified badge
    verified = profile_data.get("verified", False)
    is_blue = profile_data.get("is_blue_verified", False)
    if verified and not is_blue:
        badge = " ✅"  # legacy verified (организации и т.д.)
    elif is_blue:
        badge = " 🔵"  # Twitter Blue
    else:
        badge = ""

    # Формируем текст сообщения
    lines = [
        f"{emoji} <b>{tier_label}: @{handle}</b>{badge}",
        f"",
        f"👤 {profile_data.get('display_name', handle)}",
        f"👥 {profile_data.get('followers_count', 0):,} followers / "
        f"{profile_data.get('following_count', 0):,} following",
        f"📊 Score: <b>{metrics['twitter_score']}/100</b>",
        f"📈 Engagement: {metrics['engagement_rate']}%",
    ]

    # Возраст аккаунта
    age_human = metrics.get("account_age_human", "")
    if age_human and age_human != "?":
        lines.append(f"🕐 Аккаунт: {age_human}")

    # RT%
    rt_pct = metrics.get("rt_percentage", 0)
    lines.append(f"🔄 RT: {rt_pct}%")

    # Growth velocity
    growth = metrics.get("growth_velocity", 0)
    if growth > 0:
        lines.append(f"📈 Рост: +{growth} fol/day")

    # Location (если есть)
    location = profile_data.get("location", "")
    if location:
        lines.append(f"📍 {location}")

    # Warning: RT >= 80%
    if rt_pct >= 80:
        lines.append(f"")
        lines.append(f"⚠️ <b>RT-ONLY</b> — {rt_pct}% контента это ретвиты")

    # Bio (обрезаем до 200 символов)
    bio = profile_data.get("bio", "")
    if bio:
        bio_short = bio[:200] + "..." if len(bio) > 200 else bio
        lines.append(f"")
        lines.append(f"📝 {bio_short}")

    # История юзернеймов (Alphagate) — показываем цепочку переименований
    prev_usernames = reused.get("prev_usernames", [])
    if prev_usernames:
        chain = " → ".join(f"@{name}" for name in prev_usernames)
        lines.append(f"")
        lines.append(f"🔄 <b>Ранее:</b> {chain} → <b>@{handle}</b>")
    elif reused.get("reused_name"):
        # Fallback: snapshot-based detection (если Alphagate недоступен)
        lines.append(f"")
        lines.append(f"⚠️ <b>REUSED NAME</b> — handle ранее принадлежал другому проекту!")
    if reused.get("renamed") and not prev_usernames:
        old = reused.get("old_handle", "?")
        lines.append(f"⚠️ <b>RENAMED</b> — ранее был @{old}")

    text = "\n".join(lines)

    # Inline keyboard: ссылка на профиль + кнопка деталей + re-analyze + observe
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "🔗 Профиль", "url": f"https://x.com/{handle}"},
                {"text": "📊 Подробнее", "callback_data": f"details:{handle}"},
            ],
            [
                {"text": "🔄 Пере-анализ", "callback_data": f"reanalyze:{handle}"},
                {"text": "⭐ В watchlist", "callback_data": f"watch:{handle}"},
            ],
            [
                {"text": "🔍 Observe", "callback_data": f"observe:{handle}"},
            ],
        ]
    }

    # Twitter-алерты идут в топик EARLY
    payload = _base_payload(topic="early")
    payload["text"] = text
    payload["parse_mode"] = "HTML"
    payload["reply_markup"] = json.dumps(keyboard)

    result = _bot_api("sendMessage", payload)
    if result:
        log.info(f"Алерт отправлен в EARLY: {tier_label} @{handle}")
        return True
    return False


def _fmt_delta(old_val, new_val, suffix: str = "", is_int: bool = False) -> str:
    """Форматировать дельту: +5 или -3. Пустая строка если нет old."""
    if old_val is None:
        return ""
    diff = new_val - old_val
    if diff == 0:
        return ""
    if is_int:
        return f" ({'+' if diff > 0 else ''}{int(diff)}{suffix})"
    return f" ({'+' if diff > 0 else ''}{diff:.1f}{suffix})"


def send_watchlist_change_alert(
    handle: str, profile_data: dict, metrics: dict,
    old_score: int | None, old_tier: str | None,
    old_metrics: dict | None = None,
) -> bool:
    """
    Отправить алерт об изменении score/tier для watchlist-профиля.

    old_metrics — предыдущий анализ из twitter_analyses (followers, engagement и т.д.)
    Отправляется в топик WATCHLIST (313).
    """
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        return False

    new_score = metrics["twitter_score"]
    new_tier = metrics["tier"]
    om = old_metrics or {}

    # Стрелки для score
    if old_score is not None:
        diff = new_score - old_score
        if diff > 0:
            score_arrow = f"⬆️ (+{diff})"
        elif diff < 0:
            score_arrow = f"⬇️ ({diff})"
        else:
            score_arrow = "➡️ (0)"
        score_line = f"📊 Score: {old_score} → <b>{new_score}</b> {score_arrow}"
    else:
        score_line = f"📊 Score: <b>{new_score}</b> (первый анализ)"

    # Стрелки для tier
    tier_order = {"S": 5, "A": 4, "B": 3, "C": 2, "D": 1}
    if old_tier:
        old_rank = tier_order.get(old_tier, 0)
        new_rank = tier_order.get(new_tier, 0)
        if new_rank > old_rank:
            tier_arrow = "⬆️"
        elif new_rank < old_rank:
            tier_arrow = "⬇️"
        else:
            tier_arrow = "➡️"
        tier_line = f"🏷 Tier: {old_tier} → <b>{new_tier}</b> {tier_arrow}"
    else:
        tier_line = f"🏷 Tier: <b>{new_tier}</b>"

    emoji = TIER_EMOJI.get(new_tier, "📊")

    # Дельты по конкретным метрикам
    fol_now = profile_data.get("followers_count", 0)
    fol_delta = _fmt_delta(om.get("followers_count"), fol_now, suffix="", is_int=True)

    eng_now = metrics.get("engagement_rate", 0)
    eng_delta = _fmt_delta(om.get("engagement_rate"), float(eng_now), suffix="%")

    rt_now = metrics.get("rt_percentage", 0)
    rt_delta = _fmt_delta(om.get("rt_percentage"), float(rt_now), suffix="%")

    lines = [
        f"{emoji} <b>Watchlist: @{handle}</b>",
        f"",
        score_line,
        tier_line,
        f"👥 {fol_now:,} followers{fol_delta}",
        f"📈 Engagement: {eng_now}%{eng_delta}",
        f"🔄 RT: {rt_now}%{rt_delta}",
    ]

    # Рост фолловеров (growth velocity)
    growth_now = metrics.get("growth_velocity", 0)
    if growth_now or om.get("growth_velocity"):
        growth_delta = _fmt_delta(om.get("growth_velocity"), float(growth_now or 0), suffix=" fol/d")
        lines.append(f"📈 Рост: {growth_now} fol/day{growth_delta}")

    text = "\n".join(lines)

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "🔗 Профиль", "url": f"https://x.com/{handle}"},
                {"text": "📊 Подробнее", "callback_data": f"details:{handle}"},
            ],
            [
                {"text": "🔄 Пере-анализ", "callback_data": f"reanalyze:{handle}"},
                {"text": "🗑 Убрать из watchlist", "callback_data": f"unwatch:{handle}"},
            ],
        ]
    }

    payload = _base_payload(topic="watchlist")
    payload["text"] = text
    payload["parse_mode"] = "HTML"
    payload["reply_markup"] = json.dumps(keyboard)

    result = _bot_api("sendMessage", payload)
    if result:
        log.info(f"Watchlist алерт: @{handle} score {old_score}→{new_score}")
        return True
    return False


def send_keyword_tweet_alert(
    handle: str, tweet_id: str, tweet_text: str, matched_keywords: list[str],
) -> bool:
    """
    Алерт о новом твите watchlist-аккаунта с ключевым словом.

    Отправляется в топик WATCHLIST (313).
    """
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        return False

    # Обрезаем текст твита до 280 символов
    text_short = tweet_text[:280] + "..." if len(tweet_text) > 280 else tweet_text

    keywords_str = ", ".join(matched_keywords)

    lines = [
        f"🔔 <b>Новый твит: @{handle}</b>",
        f"",
        f"«{text_short}»",
        f"",
        f"🔑 Ключевое слово: {keywords_str}",
    ]

    text = "\n".join(lines)

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "🔗 Твит", "url": f"https://x.com/{handle}/status/{tweet_id}"},
                {"text": "👤 Профиль", "url": f"https://x.com/{handle}"},
            ],
            [
                {"text": "🗑 Убрать из watchlist", "callback_data": f"unwatch:{handle}"},
            ],
        ]
    }

    payload = _base_payload(topic="watchlist")
    payload["text"] = text
    payload["parse_mode"] = "HTML"
    payload["reply_markup"] = json.dumps(keyboard)

    result = _bot_api("sendMessage", payload)
    if result:
        log.info(f"Keyword tweet alert: @{handle} [{keywords_str}]")
        return True
    return False


def send_admin_alert(text: str, urgent: bool = False) -> bool:
    """
    Отправить админ-алерт в EARLY топик.

    urgent=True → префикс "🚨 КРИТИЧНО"
    Используется twitter_client.py для уведомлений о проблемах с авторизацией,
    circuit breaker и т.д.
    """
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        return False

    prefix = "🚨 <b>КРИТИЧНО</b>\n\n" if urgent else "⚠️ <b>ADMIN ALERT</b>\n\n"
    message = prefix + text

    payload = _base_payload(topic="early")
    payload["text"] = message
    payload["parse_mode"] = "HTML"

    result = _bot_api("sendMessage", payload)
    if result:
        log.info(f"Admin alert отправлен (urgent={urgent})")
        return True
    return False


def send_text(text: str, parse_mode: str = "HTML") -> bool:
    """Отправить простое текстовое сообщение в наш чат/топик."""
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        return False

    # Разбиваем на чанки по 4096 символов
    chunks = []
    while text:
        if len(text) <= 4096:
            chunks.append(text)
            break
        cut = text[:4096].rfind("\n")
        if cut == -1:
            cut = 4096
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")

    ok = True
    for chunk in chunks:
        payload = _base_payload()
        payload["text"] = chunk
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if not _bot_api("sendMessage", payload):
            ok = False

    return ok


def answer_callback(callback_query_id: str, text: str = "") -> bool:
    """Ответить на callback_query (убрать 'часики' на кнопке)."""
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = False
    return _bot_api("answerCallbackQuery", payload) is not None


# =============================================================================
# Private Mirror Monitor V1 — generic topic send/edit primitives (spec 001 task 4)
# =============================================================================

# Маппинг target-метки на settings-атрибут (упрощает caller'у)
_MIRROR_TOPIC_ATTRS = {
    # V1 (spec 001)
    "CALLER_A":      "telegram_mirror_caller_a_topic_id",
    "CALLER_B":      "telegram_mirror_caller_b_topic_id",
    "OTHERS":    "telegram_mirror_others_topic_id",
    "COMMUNITY":  "telegram_mirror_community_topic_id",
    "ARB":       "telegram_mirror_arb_topic_id",
    "PUMP_DUMP": "telegram_mirror_pump_topic_id",
    "WHALE":     "telegram_mirror_whale_topic_id",
    "MEME":      "telegram_mirror_meme_topic_id",
    "DIGEST":    "telegram_mirror_digest_topic_id",
    "RAW":       "telegram_mirror_raw_topic_id",
    "SYSHEALTH": "telegram_mirror_syshealth_topic_id",
    # V1.5 (spec 002)
    "THEME_BURST":        "telegram_mirror_theme_burst_topic_id",
    "THEME_BURST_SHADOW": "telegram_mirror_theme_burst_shadow_topic_id",
}


def resolve_mirror_topic_id(target: str) -> int | None:
    """
    Преобразовать строковую метку ('CALLER_A', 'ARB', 'SYSHEALTH', ...) в topic_id из settings.
    Возвращает None если env-var не задан (caller должен залогировать и skip publication).
    """
    attr = _MIRROR_TOPIC_ATTRS.get(target.upper())
    if not attr:
        log.error(f"Неизвестный mirror-топик: {target}")
        return None
    raw = getattr(settings, attr, None)
    if not raw:
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        log.error(f"Некорректный topic_id для {target}: {raw!r}")
        return None


def _alert_syshealth(text: str) -> None:
    """Послать CRITICAL alert в SYSHEALTH топик. Тихий fallback если env-var не задан."""
    syshealth_id = resolve_mirror_topic_id("SYSHEALTH")
    if syshealth_id is None:
        log.critical(f"[SYSHEALTH not configured] {text}")
        return
    payload = {
        "chat_id": int(settings.telegram_notify_chat_id),
        "message_thread_id": syshealth_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    _bot_api("sendMessage", payload)


def send_to_topic(
    text: str,
    topic_id: int,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
    reply_markup: dict | None = None,
    reply_to_message_id: int | None = None,
) -> int | None:
    """
    Отправить новое сообщение в конкретный forum topic.

    Возвращает message_id успешного сообщения (для будущих edit'ов), либо None.

    Использует тот же raw HTTP паттерн что и существующий _bot_api().

    reply_markup: optional inline keyboard для interactive callbacks (spec 002 V1.5).
        Example: {"inline_keyboard": [[{"text": "OK", "callback_data": "tag:1:plausible"}]]}
    reply_to_message_id: тред-реплай к существующему сообщению в этом же чате.
        allow_sending_without_reply=True — если родитель удалён/не найден, шлём отдельно.
    """
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        log.warning("TELEGRAM_BOT_TOKEN или TELEGRAM_NOTIFY_CHAT_ID не задан — не отправляю")
        return None
    if not topic_id:
        log.warning("topic_id не задан — не отправляю")
        return None

    payload = {
        "chat_id": int(settings.telegram_notify_chat_id),
        "message_thread_id": int(topic_id),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup is not None:
        import json as _json
        payload["reply_markup"] = _json.dumps(reply_markup)
    if reply_to_message_id:
        payload["reply_to_message_id"] = int(reply_to_message_id)
        payload["allow_sending_without_reply"] = True
    result = _bot_api("sendMessage", payload)
    if not result:
        return None
    return result.get("message_id")


def send_photo_to_topic(
    photo_path: str,
    caption: str,
    topic_id: int,
    parse_mode: str = "HTML",
    reply_to_message_id: int | None = None,
) -> int | None:
    """
    Отправить photo из локального файла в forum topic через Bot API multipart.

    Caption жёстко обрезается до 1024 символов (TG limit для sendPhoto).
    Если caller хочет показать длинный текст полностью — пусть отправит остаток
    отдельным send_to_topic.

    Возвращает message_id успешного сообщения, либо None при ошибке.
    """
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        log.warning("TELEGRAM_BOT_TOKEN или TELEGRAM_NOTIFY_CHAT_ID не задан — не отправляю photo")
        return None
    if not topic_id:
        log.warning("topic_id не задан — не отправляю photo")
        return None
    if not os.path.exists(photo_path):
        log.error(f"send_photo_to_topic: файл {photo_path} не существует")
        return None

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendPhoto"
    data = {
        "chat_id": int(settings.telegram_notify_chat_id),
        "message_thread_id": int(topic_id),
        "caption": caption[:1024],
        "parse_mode": parse_mode,
    }
    if reply_to_message_id:
        data["reply_to_message_id"] = int(reply_to_message_id)
        data["allow_sending_without_reply"] = True
    try:
        with open(photo_path, "rb") as fh:
            resp = requests.post(url, data=data, files={"photo": fh}, timeout=30)
        result = resp.json()
        if not result.get("ok"):
            log.error(f"sendPhoto ошибка: {result}")
            return None
        return (result.get("result") or {}).get("message_id")
    except Exception as e:
        log.error(f"sendPhoto exception: {e}")
        return None


def make_theme_burst_keyboard(event_id: int) -> dict:
    """Inline keyboard для tagging alpha_events (spec 002 V1.5 Task 7.5).

    callback_data формат: `tag:{event_id}:{value}` (max 64 bytes per Telegram API).
    Handler сидит в services/telegram_notifier.py (handle_burst_tag_callback).
    """
    return {
        "inline_keyboard": [
            [
                {"text": "✅ Plausible", "callback_data": f"tag:{event_id}:plausible"},
                {"text": "🎯 Real Alpha", "callback_data": f"tag:{event_id}:real"},
            ],
            [
                {"text": "🗑️ Noise", "callback_data": f"tag:{event_id}:noise"},
                {"text": "⏰ Late", "callback_data": f"tag:{event_id}:late_alpha"},
            ],
        ]
    }


def edit_message_in_topic(
    text: str,
    message_id: int,
    topic_id: int,
    parse_mode: str = "HTML",
    disable_web_page_preview: bool = True,
) -> bool:
    """
    Отредактировать существующее сообщение в форум-топике (Variant C emission в dedup).

    Полная error matrix согласно spec 001 task 4:
    - success                     -> True
    - "message is not modified"   -> True (текст не изменился, не считается ошибкой)
    - "message to edit not found" -> False (caller должен NULL telegram_msg_id для re-emit)
    - 429 Too Many Requests       -> sleep(retry_after) + retry один раз
    - "chat not found"/Forbidden  -> CRITICAL: log + alert в SYSHEALTH, False
    - network timeout/error       -> retry один раз с backoff(1s)
    - other                       -> log ERROR с full response, False

    Telegram Bot API:
        editMessageText: https://core.telegram.org/bots/api#editmessagetext
        Не требует message_thread_id (берёт из самого сообщения по message_id)
    """
    if not settings.telegram_bot_token or not settings.telegram_notify_chat_id:
        log.warning("TELEGRAM_BOT_TOKEN или TELEGRAM_NOTIFY_CHAT_ID не задан — не редактирую")
        return False
    if not message_id or not topic_id:
        log.warning(f"message_id/topic_id не заданы: msg={message_id} topic={topic_id}")
        return False

    payload = {
        "chat_id": int(settings.telegram_notify_chat_id),
        "message_id": int(message_id),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }

    return _edit_with_retries(payload, retries_remaining=1)


def _edit_with_retries(payload: dict, retries_remaining: int = 1) -> bool:
    """Внутренний helper: один вызов editMessageText + обработка ошибок + 1 retry."""
    if not settings.telegram_bot_token:
        return False

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/editMessageText"

    try:
        resp = requests.post(url, json=payload, timeout=15)
    except requests.exceptions.Timeout:
        log.warning("editMessageText timeout")
        if retries_remaining > 0:
            import time
            time.sleep(1)
            return _edit_with_retries(payload, retries_remaining - 1)
        return False
    except Exception as e:
        log.error(f"editMessageText network error: {e}")
        if retries_remaining > 0:
            import time
            time.sleep(1)
            return _edit_with_retries(payload, retries_remaining - 1)
        return False

    try:
        data = resp.json()
    except Exception:
        log.error(f"editMessageText non-JSON response: status={resp.status_code} body={resp.text[:200]}")
        return False

    # Успех
    if data.get("ok"):
        return True

    description = (data.get("description") or "").lower()
    error_code = data.get("error_code")

    # "message is not modified" — текст совпадает, считаем успехом
    if "not modified" in description:
        log.debug(f"editMessageText: not modified (msg_id={payload.get('message_id')})")
        return True

    # "message to edit not found" — сообщение удалили; caller должен NULL telegram_msg_id
    if "message to edit not found" in description or "message_id_invalid" in description:
        log.warning(f"editMessageText: msg {payload.get('message_id')} not found, caller should reset")
        return False

    # Rate limit 429 — respect retry_after, retry один раз
    if error_code == 429:
        retry_after = (data.get("parameters") or {}).get("retry_after", 1)
        log.warning(f"editMessageText rate-limited, retry_after={retry_after}s")
        if retries_remaining > 0:
            import time
            time.sleep(retry_after + 1)
            return _edit_with_retries(payload, retries_remaining - 1)
        return False

    # Catastrophic: bot не в чате, kicked, forbidden
    catastrophic_phrases = [
        "chat not found", "bot was kicked", "forbidden",
        "bot is not a member", "have no rights",
    ]
    if any(p in description for p in catastrophic_phrases):
        log.critical(f"editMessageText catastrophic: {data}")
        _alert_syshealth(
            f"⚠️ <b>Mirror notifier catastrophic edit error</b>\n"
            f"msg_id: {payload.get('message_id')}\n"
            f"error: {data.get('description', '?')}"
        )
        return False

    # Другие ошибки
    log.error(f"editMessageText error: {data}")
    return False
