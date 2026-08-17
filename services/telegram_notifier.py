"""
Telegram Notifier Bot — команды, inline keyboard, объединённая сводка.

Бот слушает команды от пользователя и обрабатывает нажатия inline кнопок
(Details, Re-analyze, Watchlist) из алертов Twitter Worker'а.

Запуск:
    python -m services.telegram_notifier

Команды:
    /start, /help  — справка
    /settings       — текущие настройки
    /set <key> <val> — изменить настройку
    /analyze <handle> — ручной анализ Twitter-профиля
    /summary [дата]  — сгенерировать сводку дня
    /watch <handle>  — добавить в watchlist
    /unwatch <handle> — убрать из watchlist
    /watchlist       — показать watchlist
    /myid            — узнать свой Telegram user ID
"""

import asyncio
import logging
import sys
from datetime import date
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.redis_client import init_redis, close_redis, get_redis

# --- Логи ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("notifier_bot")

# Эмодзи для тиров
TIER_EMOJI = {"S": "🔥", "A": "⭐", "B": "💎", "C": "📈", "D": "🚫"}


# ==================== Проверка доступа ====================

def is_authorized(update: Update) -> bool:
    """Проверить, что сообщение от админа или из нашего чата."""
    user_id = update.effective_user.id if update.effective_user else None
    chat_id = update.effective_chat.id if update.effective_chat else None

    # Если задан TELEGRAM_ADMIN_ID — проверяем по нему
    if settings.telegram_admin_id:
        return str(user_id) == str(settings.telegram_admin_id)

    # Иначе — разрешаем из нашего чата
    if settings.telegram_notify_chat_id:
        return str(chat_id) == str(settings.telegram_notify_chat_id)

    return False


# ==================== Команды ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start и /help."""
    text = (
        "🤖 <b>xanalyst — крипто-ассистент</b>\n\n"
        "Доступные команды:\n"
        "/settings — текущие настройки\n"
        "/set &lt;key&gt; &lt;value&gt; — изменить настройку\n"
        "/analyze &lt;handle&gt; — анализ Twitter-профиля\n"
        "/summary [YYYY-MM-DD] — сводка дня\n"
        "/watch &lt;handle&gt; [заметка] — добавить в watchlist\n"
        "/unwatch &lt;handle&gt; — убрать из watchlist\n"
        "/watchlist — показать watchlist\n"
        "/myid — узнать свой Telegram ID\n"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /myid — показать Telegram user ID."""
    user = update.effective_user
    await update.message.reply_text(
        f"Твой Telegram ID: <code>{user.id}</code>\n"
        f"Chat ID: <code>{update.effective_chat.id}</code>",
        parse_mode="HTML",
    )


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /settings — показать текущие настройки из БД."""
    if not is_authorized(update):
        return

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM settings ORDER BY key")

    if not rows:
        await update.message.reply_text("Настройки пусты.")
        return

    lines = ["⚙️ <b>Настройки:</b>\n"]
    for row in rows:
        lines.append(f"  <code>{row['key']}</code> = {row['value']}")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /set <key> <value> — изменить настройку."""
    if not is_authorized(update):
        return

    args = context.args
    if not args or len(args) < 2:
        await update.message.reply_text(
            "Использование: /set <key> <value>\n"
            "Пример: /set min_twitter_score 40"
        )
        return

    key = args[0]
    value = " ".join(args[1:])

    # Допустимые ключи
    allowed_keys = {
        "min_twitter_score", "min_followers", "max_bot_percentage",
        "daily_summary_hour", "notify_tiers",
    }
    if key not in allowed_keys:
        await update.message.reply_text(
            f"Неизвестный ключ: {key}\n"
            f"Доступные: {', '.join(sorted(allowed_keys))}"
        )
        return

    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO settings (key, value, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()
            """,
            key, value,
        )

    await update.message.reply_text(f"✅ {key} = {value}")
    log.info(f"Настройка изменена: {key} = {value}")


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /analyze <handle> — ручной анализ Twitter-профиля."""
    if not is_authorized(update):
        return

    args = context.args
    if not args:
        await update.message.reply_text("Использование: /analyze <handle>\nПример: /analyze uniswap")
        return

    handle = args[0].lstrip("@").lower()
    await update.message.reply_text(f"🔍 Ставлю @{handle} в очередь анализа...")

    # Отправляем в Redis stream (как это делает discord_monitor)
    r = get_redis()
    await r.xadd(
        "stream:x_analyze",
        {
            "handle": handle,
            "source": "manual",
            "channel_id": "0",
            "channel_name": "telegram_bot",
            "message_url": "",
        },
    )
    log.info(f"Ручной анализ: @{handle} добавлен в очередь")


async def cmd_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /summary [дата] — сгенерировать сводку дня."""
    if not is_authorized(update):
        return

    args = context.args
    target_date = date.today()
    if args:
        try:
            target_date = date.fromisoformat(args[0])
        except ValueError:
            await update.message.reply_text("Формат даты: YYYY-MM-DD\nПример: /summary 2026-02-06")
            return

    await update.message.reply_text(f"⏳ Генерирую сводку за {target_date}...")

    # Импортируем daily_summary здесь чтобы избежать circular imports
    from services.daily_summary import make_daily_summary

    try:
        summary = await make_daily_summary(target_date)
        # Конвертируем Markdown от Perplexity в HTML и отправляем
        from shared.notifier import send_text, markdown_to_telegram_html
        send_text(markdown_to_telegram_html(summary))
        await update.message.reply_text("✅ Сводка отправлена в топик NEWS!")
    except Exception as e:
        log.error(f"Ошибка генерации сводки: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /watch <handle> [заметка] — добавить в watchlist."""
    if not is_authorized(update):
        return

    args = context.args
    if not args:
        await update.message.reply_text("Использование: /watch <handle> [заметка]\nПример: /watch uniswap DEX лидер")
        return

    handle = args[0].lstrip("@").lower()
    notes = " ".join(args[1:]) if len(args) > 1 else ""

    pool = get_pool()
    async with pool.acquire() as conn:
        # Пробуем получить последний анализ для score/tier
        last = await conn.fetchrow(
            "SELECT twitter_score, tier FROM twitter_analyses "
            "WHERE LOWER(handle) = LOWER($1) ORDER BY analyzed_at DESC LIMIT 1",
            handle,
        )
        score = last["twitter_score"] if last else None
        tier = last["tier"] if last else None

        await conn.execute(
            """
            INSERT INTO twitter_watchlist (handle, notes, last_score, last_tier)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (handle) DO UPDATE SET
                notes = $2, last_score = COALESCE($3, twitter_watchlist.last_score),
                last_tier = COALESCE($4, twitter_watchlist.last_tier)
            """,
            handle, notes, score, tier,
        )

    emoji = TIER_EMOJI.get(tier, "❓") if tier else "❓"
    info = f" ({emoji} {tier}, score={score})" if tier else ""
    await update.message.reply_text(f"⭐ @{handle} добавлен в watchlist{info}")
    log.info(f"Watchlist: добавлен @{handle}")


async def cmd_unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /unwatch <handle> — убрать из watchlist."""
    if not is_authorized(update):
        return

    args = context.args
    if not args:
        await update.message.reply_text("Использование: /unwatch <handle>")
        return

    handle = args[0].lstrip("@").lower()

    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM twitter_watchlist WHERE handle = $1", handle,
        )

    if "DELETE 1" in result:
        await update.message.reply_text(f"🗑 @{handle} убран из watchlist")
    else:
        await update.message.reply_text(f"@{handle} не найден в watchlist")


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /watchlist — показать watchlist."""
    if not is_authorized(update):
        return

    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT handle, notes, last_score, last_tier, added_at "
            "FROM twitter_watchlist ORDER BY added_at DESC"
        )

    if not rows:
        await update.message.reply_text("Watchlist пуст. Добавь: /watch <handle>")
        return

    lines = ["⭐ <b>Watchlist:</b>\n"]
    for row in rows:
        tier = row["last_tier"] or "?"
        score = row["last_score"] or "?"
        emoji = TIER_EMOJI.get(tier, "❓")
        handle = row["handle"]
        notes = row["notes"]
        line = f"  {emoji} @{handle} — {tier} ({score})"
        if notes:
            line += f" | {notes}"
        lines.append(line)

    lines.append(f"\nВсего: {len(rows)} профилей")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


# ==================== Callback-обработчики ====================

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий inline кнопок."""
    query = update.callback_query
    data = query.data  # формат: "action:handle" или "tag:event_id:value" (spec 002 V1.5)

    if ":" not in data:
        await query.answer("Неизвестная команда")
        return

    # Spec 002 V1.5 — Theme Burst tagging
    if data.startswith("tag:"):
        await handle_burst_tag_callback(update, context)
        return

    # Spec 004 — CT Alpha Digest feedback (3 callback buttons на digest post)
    if data.startswith("ct_digest_v1:"):
        await handle_ct_digest_callback(update, context)
        return

    action, handle = data.split(":", 1)

    if action == "details":
        await callback_details(query, handle)
    elif action == "reanalyze":
        await callback_reanalyze(query, handle)
    elif action == "watch":
        await callback_watch(query, handle)
    elif action == "observe":
        await callback_observe(query, handle)
    elif action == "unwatch":
        await callback_unwatch(query, handle)
    else:
        await query.answer("Неизвестное действие")


async def handle_burst_tag_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Spec 002 V1.5 Task 7.5 — handle tag:{event_id}:{value} callbacks.

    Updates alpha_events.user_tag column для machine-verifiable validation gate.
    Edit message text to confirm tag (no spam, append confirmation line).
    """
    query = update.callback_query
    parts = (query.data or "").split(":", 2)
    if len(parts) != 3:
        await query.answer("Bad callback format")
        return

    _, event_id_str, value = parts
    if value not in ("plausible", "real", "noise", "late_alpha"):
        await query.answer(f"Bad tag value: {value}")
        return
    try:
        event_id = int(event_id_str)
    except ValueError:
        await query.answer(f"Bad event id: {event_id_str}")
        return

    user_id = update.effective_user.id if update.effective_user else None
    tagged_by = str(user_id) if user_id else "anonymous"

    pool = await get_pool_or_none()
    if pool is None:
        await query.answer("DB unavailable")
        return

    async with pool.acquire() as conn:
        # Idempotent — re-tagging allowed (override)
        await conn.execute("""
            UPDATE alpha_events
            SET user_tag=$1, user_tagged_at=NOW(), user_tagged_by=$2
            WHERE id=$3
        """, value, tagged_by, event_id)

    await query.answer(f"Tagged: {value}")
    # Append confirmation line (avoid full edit — original alert preserved)
    try:
        current_text = query.message.text_html or query.message.text or ""
        if "✅ Tagged:" not in current_text:
            new_text = current_text + f"\n\n✅ Tagged: <b>{value}</b> by user {tagged_by}"
            await query.edit_message_text(
                text=new_text, parse_mode="HTML", disable_web_page_preview=True,
            )
    except Exception as e:
        log.warning(f"edit_message_text failed (non-fatal): {e}")


async def get_pool_or_none():
    """Helper для callback path — может вызываться вне основного pool init."""
    from shared.db import get_pool
    try:
        return get_pool()
    except Exception as e:
        log.error(f"DB pool unavailable in callback: {e}")
        return None


# ==================== Spec 004 — CT Alpha Digest handlers ====================

async def handle_ct_digest_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Spec 004 — feedback callback dispatcher.

    callback_data format: ct_digest_v1:<action>:<tick_id>
      action ∈ {thumbs_up, thumbs_down, knew}

    Per ADR 0002 critical#8: mandatory len(parts) != 3 guard mirrored from
    handle_burst_tag_callback pattern.
    """
    query = update.callback_query
    parts = (query.data or "").split(":", 2)
    if len(parts) != 3:
        await query.answer("Bad callback format")
        return

    _, action, tick_id_str = parts
    if action not in ("thumbs_up", "thumbs_down", "knew"):
        await query.answer(f"Bad action: {action}")
        return
    try:
        tick_id = int(tick_id_str)
    except ValueError:
        await query.answer(f"Bad tick_id: {tick_id_str}")
        return

    user_id = query.from_user.id if query.from_user else None
    pool = await get_pool_or_none()
    if not pool:
        await query.answer("DB unavailable")
        return

    try:
        await pool.execute(
            """
            INSERT INTO ct_feedback (digest_msg_id, tick_id, action, user_id, created_at)
            SELECT digest_msg_id, $1, $2, $3, NOW()
            FROM ct_digest_ticks WHERE tick_id = $1
            """,
            tick_id, action, user_id,
        )
    except Exception as e:
        log.error("ct_digest callback insert failed: %s", e)
        await query.answer("DB error")
        return

    replies = {"thumbs_up": "👍 noted", "thumbs_down": "👎 noted", "knew": "✋ already knew"}
    await query.answer(replies.get(action, "noted"))


async def handle_ct_digest_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Spec 004 — freeform reply на CT digest post → second-pass LLM parse.

    Triggers только если reply_to_message.message_id принадлежит ct_digest_ticks
    (иначе ignore — это reply на другое сообщение, не наша тема).
    """
    msg = update.message
    if not msg or not msg.reply_to_message:
        return
    reply_to_id = msg.reply_to_message.message_id
    text = msg.text or ""

    pool = await get_pool_or_none()
    if not pool:
        return

    # Check if reply_to belongs to a ct_digest tick
    tick_row = await pool.fetchrow(
        "SELECT tick_id FROM ct_digest_ticks WHERE digest_msg_id = $1",
        reply_to_id,
    )
    if not tick_row:
        return  # not a digest reply, ignore

    user_id = msg.from_user.id if msg.from_user else None

    try:
        from services.ct_digest.feedback import parse_reply_note, format_parsed_summary
        fb_id = await parse_reply_note(
            pool=pool,
            text=text,
            tick_id=tick_row["tick_id"],
            digest_msg_id=reply_to_id,
            user_id=user_id,
        )
        if fb_id:
            # Re-fetch для summary reply
            row = await pool.fetchrow(
                "SELECT item_short_ids, parsed_prefs FROM ct_feedback WHERE id = $1", fb_id,
            )
            prefs = row["parsed_prefs"] if row else {}
            import json as _json
            if isinstance(prefs, str):
                try:
                    prefs = _json.loads(prefs)
                except Exception:
                    prefs = {}
            summary = format_parsed_summary(prefs, list(row["item_short_ids"] or []))
            await msg.reply_text(f"📝 заметка записана: {summary}", quote=True)
        else:
            await msg.reply_text("📝 заметка не распарсилась — но сохранил raw", quote=True)
    except Exception as e:
        log.error("ct_digest reply handler failed: %s", e)


async def cmd_digest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Spec 004 — /digest команда: on-demand digest tick.

    Admin-only (per ADR 0002 R-7: use is_authorized, not int(config.telegram_admin_id)).
    Rate-limit 1/hour per user via Redis SETNX.
    Subprocess через asyncio.create_subprocess_exec с 300s timeout + error reply.
    """
    if not is_authorized(update):
        await update.message.reply_text("🚫 команда только для администратора")
        return

    user_id = update.effective_user.id
    redis = get_redis()
    rate_key = f"ct_digest:on_demand:{user_id}"
    # SETNX returns True если key не existed → permit; False если уже set → block
    permit = await redis.set(rate_key, "1", ex=3600, nx=True)
    if not permit:
        ttl = await redis.ttl(rate_key)
        mins = max(1, ttl // 60)
        await update.message.reply_text(
            f"⏱ /digest уже запускался недавно, доступно через ~{mins} мин"
        )
        return

    await update.message.reply_text(
        "🔭 digest tick queued, post появится в течение 1-2 минут"
    )

    cmd = [
        sys.executable, "-m", "services.ct_alpha_digest",
        "--once", "--triggered-by", str(user_id),
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
        if proc.returncode != 0:
            err = stderr.decode("utf-8", "replace")[:200]
            await update.message.reply_text(
                f"❌ tick failed (exit {proc.returncode}): {err}"
            )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await update.message.reply_text("⏱ tick timeout 300s — see logs")
    except Exception as e:
        log.error("cmd_digest subprocess error: %s", e)
        await update.message.reply_text(f"❌ subprocess error: {e}")


async def callback_details(query, handle: str):
    """Кнопка 'Подробнее' — показать полный анализ."""
    await query.answer("Загружаю данные...")

    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT handle, display_name, bio, followers_count, following_count,
                   tweets_count, engagement_rate, bot_percentage, quality_score,
                   twitter_score, tier, reused_name, renamed, analyzed_at,
                   rt_percentage, growth_velocity, account_age_days
            FROM twitter_analyses
            WHERE LOWER(handle) = LOWER($1)
            ORDER BY analyzed_at DESC
            LIMIT 1
            """,
            handle,
        )

    if not row:
        await query.message.reply_text(f"Нет данных для @{handle}")
        return

    tier = row["tier"]
    emoji = TIER_EMOJI.get(tier, "")

    # Определяем early по количеству followers
    is_early = (row["followers_count"] or 0) < 2000
    tier_label = f"EARLY {tier}" if is_early else tier

    # Форматируем возраст аккаунта
    from shared.metrics import format_account_age
    age_days = row["account_age_days"] or 0
    age_human = format_account_age(age_days)

    lines = [
        f"{emoji} <b>Подробный анализ @{handle}</b>",
        f"",
        f"👤 {row['display_name']}",
        f"📝 {(row['bio'] or '')[:300]}",
        f"",
        f"<b>Метрики:</b>",
        f"  👥 Followers: {row['followers_count']:,}",
        f"  👤 Following: {row['following_count']:,}",
        f"  📝 Tweets: {row['tweets_count']:,}",
        f"  📈 Engagement: {row['engagement_rate']}%",
        f"  🔄 RT: {row['rt_percentage'] or 0}%",
        f"  🕐 Аккаунт: {age_human}",
    ]

    # Growth velocity
    growth = row["growth_velocity"]
    if growth and growth > 0:
        lines.append(f"  📈 Рост: +{growth} fol/day")

    lines.extend([
        f"",
        f"<b>Итог:</b>",
        f"  📊 Twitter Score: <b>{row['twitter_score']}/100</b>",
        f"  🏆 Tier: <b>{tier_label}</b>",
    ])

    # RT warning
    rt_pct = row["rt_percentage"] or 0
    if rt_pct >= 80:
        lines.append(f"  ⚠️ RT-ONLY — {rt_pct}% контента это ретвиты")

    if row["reused_name"]:
        lines.append(f"  ⚠️ REUSED NAME — handle ранее принадлежал другому проекту!")
    if row["renamed"]:
        lines.append(f"  ⚠️ RENAMED — user_id менял handle")

    analyzed = row["analyzed_at"].strftime("%d.%m.%Y %H:%M")
    lines.append(f"\n🕐 Анализ: {analyzed}")

    # Кнопки
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔗 Профиль", url=f"https://x.com/{handle}"),
            InlineKeyboardButton("🔄 Пере-анализ", callback_data=f"reanalyze:{handle}"),
        ],
    ])

    await query.message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=keyboard,
    )


async def callback_reanalyze(query, handle: str):
    """Кнопка 'Пере-анализ' — сбросить cooldown и добавить в очередь."""
    await query.answer(f"Ставлю @{handle} в очередь...")

    r = get_redis()

    # Сбрасываем cooldown
    await r.delete(f"cooldown:{handle}")

    # Добавляем в очередь
    await r.xadd(
        "stream:x_analyze",
        {
            "handle": handle,
            "source": "manual",
            "channel_id": "0",
            "channel_name": "reanalyze_button",
            "message_url": "",
        },
    )

    await query.message.reply_text(f"🔄 @{handle} добавлен в очередь пере-анализа")
    log.info(f"Пере-анализ: @{handle}")


async def callback_watch(query, handle: str):
    """Кнопка 'В watchlist' — добавить handle в watchlist."""
    pool = get_pool()
    async with pool.acquire() as conn:
        last = await conn.fetchrow(
            "SELECT twitter_score, tier FROM twitter_analyses "
            "WHERE LOWER(handle) = LOWER($1) ORDER BY analyzed_at DESC LIMIT 1",
            handle,
        )
        score = last["twitter_score"] if last else None
        tier = last["tier"] if last else None

        await conn.execute(
            """
            INSERT INTO twitter_watchlist (handle, last_score, last_tier)
            VALUES ($1, $2, $3)
            ON CONFLICT (handle) DO UPDATE SET
                last_score = COALESCE($2, twitter_watchlist.last_score),
                last_tier = COALESCE($3, twitter_watchlist.last_tier)
            """,
            handle, score, tier,
        )

    await query.answer(f"⭐ @{handle} в watchlist!")
    log.info(f"Watchlist (кнопка): @{handle}")


async def callback_unwatch(query, handle: str):
    """Кнопка 'Убрать из watchlist' — удалить handle из watchlist."""
    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM twitter_watchlist WHERE LOWER(handle) = LOWER($1)", handle,
        )
    if "DELETE 1" in result:
        await query.answer(f"🗑 @{handle} удалён из watchlist")
        log.info(f"Unwatch (кнопка): @{handle}")
    else:
        await query.answer(f"@{handle} не найден в watchlist")


async def callback_observe(query, handle: str):
    """Кнопка 'Observe' — placeholder для будущей интеграции с Concord."""
    await query.answer("🔍 Функция в разработке")


# ==================== Инициализация ====================

async def post_init(application):
    """Вызывается после старта бота — подключаем БД и Redis."""
    await init_db()
    await init_redis()
    log.info("БД и Redis подключены")


async def post_shutdown(application):
    """Вызывается при остановке."""
    await close_db()
    await close_redis()
    log.info("БД и Redis отключены")


def main():
    """Запуск бота."""
    if not settings.telegram_bot_token:
        log.error("Задай TELEGRAM_BOT_TOKEN в .env")
        return

    log.info("Запуск Telegram Notifier Bot...")

    # Создаём приложение
    app = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Регистрируем команды
    app.add_handler(CommandHandler(["start", "help"], cmd_start))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("set", cmd_set))
    app.add_handler(CommandHandler("analyze", cmd_analyze))
    app.add_handler(CommandHandler("summary", cmd_summary))
    app.add_handler(CommandHandler("watch", cmd_watch))
    app.add_handler(CommandHandler("unwatch", cmd_unwatch))
    app.add_handler(CommandHandler("watchlist", cmd_watchlist))

    # Spec 004 — CT Alpha Digest
    app.add_handler(CommandHandler("digest", cmd_digest))
    # Reply-on-digest captures freeform notes (filters.REPLY ловит reply messages)
    app.add_handler(MessageHandler(filters.REPLY & filters.TEXT, handle_ct_digest_reply))

    # Регистрируем callback-обработчик для inline кнопок
    app.add_handler(CallbackQueryHandler(handle_callback))

    log.info("Бот запущен. Жду команды...\n")

    # Запускаем polling
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
