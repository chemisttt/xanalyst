"""
Health Check — следит что критичные сервисы xanalyst живые.

1) Discord Monitor — Redis heartbeat `discord_monitor:heartbeat`
2) CatchMint radar — process + свежесть catchmint_alerts / log mtime
3) CT Alpha Digest — вчерашний/сегодняшний tick completed, не orphan
4) Theme Burst — timer cycles пишут alpha_signal_scores, s1 не «вечный ноль»

Дебаунс: один алерт на check-type в час (Redis).
При выздоровлении дебаунс сбрасывается.

Запуск через cron:
    */5 * * * * cd /opt/xanalyst && venv/bin/python -m services.health_check >> logs/health_check.log 2>&1
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from shared.db import close_db, get_pool, init_db
from shared.notifier import resolve_mirror_topic_id, send_admin_alert, send_to_topic
from shared.redis_client import close_redis, get_redis, init_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("health_check")

HEARTBEAT_KEY = "discord_monitor:heartbeat"
ALERT_SENT_KEY = "discord_monitor:alert_sent_at"
STALE_THRESHOLD = 30 * 60  # 30 минут
ALERT_DEBOUNCE = 60 * 60

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATCHMINT_LOG = PROJECT_ROOT / "logs" / "catchmint_radar.log"
# CatchMint poll = 60s; если process умер — log mtime > 15 min = bad
CATCHMINT_LOG_STALE_SEC = 15 * 60
# CT digests: 1×/день ~21:05 MSK; orphan/missing > 36h = bad
CT_TICK_MAX_AGE_HOURS = 36
# Theme burst: cycles every 5 min; no rows 30 min = timer dead
THEME_SCORE_MAX_AGE_MIN = 30
# Theme: s1 forever zero while z_active (post-UTC-fix should recover)
THEME_S1_ZERO_STREAK_HOURS = 6


def _debounce_key(name: str) -> str:
    return f"health_check:alert_sent:{name}"


async def _should_alert(r, name: str) -> bool:
    raw = await r.get(_debounce_key(name))
    if raw is None:
        return True
    try:
        last = int(raw)
    except ValueError:
        return True
    return (int(time.time()) - last) >= ALERT_DEBOUNCE


async def _mark_alerted(r, name: str) -> None:
    await r.set(_debounce_key(name), str(int(time.time())), ex=86400)


async def _clear_debounce(r, name: str) -> None:
    await r.delete(_debounce_key(name))


def _emit_alert(text: str, urgent: bool = True) -> bool:
    """Admin DM + SYSHEALTH topic (best-effort)."""
    ok = send_admin_alert(text, urgent=urgent)
    sys_id = resolve_mirror_topic_id("SYSHEALTH")
    if sys_id:
        send_to_topic(text, sys_id)
    return ok


async def check_discord_monitor() -> None:
    r = get_redis()
    now = int(time.time())
    heartbeat_raw = await r.get(HEARTBEAT_KEY)

    if heartbeat_raw is None:
        age = None
        is_stale = True
        reason = "heartbeat key отсутствует (TTL 300s истёк или сервис не запускался)"
    else:
        try:
            heartbeat_ts = int(heartbeat_raw)
        except ValueError:
            log.error(f"heartbeat value не int: {heartbeat_raw!r}")
            return
        age = now - heartbeat_ts
        is_stale = age > STALE_THRESHOLD
        reason = f"heartbeat возрастом {age // 60} мин (порог {STALE_THRESHOLD // 60} мин)"

    if not is_stale:
        log.info(f"OK: discord_monitor heartbeat возрастом {age}с")
        if await r.get(ALERT_SENT_KEY) is not None:
            await r.delete(ALERT_SENT_KEY)
            log.info("Дебаунс discord алерта сброшен")
        await _clear_debounce(r, "discord")
        return

    if not await _should_alert(r, "discord"):
        log.warning(f"СБОЙ discord: {reason}. Дебаунс — пропуск.")
        return

    msg = (
        f"⚠️ <b>Discord Monitor offline</b>\n\n"
        f"Причина: {reason}\n\n"
        f"Фикс: <code>sudo systemctl restart xanalyst-discord-monitor</code>"
    )
    log.error(f"СБОЙ discord: {reason}")
    if _emit_alert(msg):
        await _mark_alerted(r, "discord")
        await r.set(ALERT_SENT_KEY, str(now), ex=86400)


def _catchmint_process_alive() -> bool:
    """pgrep without killing anything — read /proc."""
    try:
        for pid in Path("/proc").iterdir():
            if not pid.name.isdigit():
                continue
            try:
                cmdline = (pid / "cmdline").read_bytes().replace(b"\x00", b" ").decode(errors="ignore")
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
            if "services.catchmint_radar" in cmdline or "services/catchmint_radar" in cmdline:
                return True
    except Exception as e:
        log.warning(f"process scan failed: {e}")
    return False


async def check_catchmint() -> None:
    """CatchMint product SHUT DOWN mid-2026 (api.catchmint.xyz NXDOMAIN).

    Default: skip check (retired). Set CATCHMINT_HEALTH_CHECK=1 to re-enable
    if a replacement source is ever wired.
    """
    r = get_redis()
    enabled = os.getenv("CATCHMINT_HEALTH_CHECK", "0").strip() in ("1", "true", "TRUE", "yes")
    if not enabled:
        log.info("OK: catchmint check skipped (product retired; CATCHMINT_HEALTH_CHECK!=1)")
        await _clear_debounce(r, "catchmint")
        return

    alive = _catchmint_process_alive()
    log_age = None
    if CATCHMINT_LOG.exists():
        log_age = time.time() - CATCHMINT_LOG.stat().st_mtime

    stale_log = log_age is None or log_age > CATCHMINT_LOG_STALE_SEC
    bad = (not alive) or stale_log

    if not bad:
        log.info(f"OK: catchmint process alive, log age {int(log_age or 0)}s")
        await _clear_debounce(r, "catchmint")
        return

    if not await _should_alert(r, "catchmint"):
        log.warning("СБОЙ catchmint (дебаунс)")
        return

    parts = []
    if not alive:
        parts.append("process НЕ найден (systemd unit / nohup умер)")
    if stale_log:
        parts.append(f"log stale: age={int(log_age) if log_age is not None else 'missing'}s")
    msg = (
        f"⚠️ <b>CatchMint radar offline</b>\n\n"
        + "\n".join(f"• {p}" for p in parts)
        + "\n\nФикс: <code>sudo systemctl start xanalyst-catchmint-radar</code>"
        + "\n(или nohup fallback — см. CLAUDE.md)"
    )
    log.error(f"СБОЙ catchmint: {parts}")
    if _emit_alert(msg):
        await _mark_alerted(r, "catchmint")


async def check_ct_digest() -> None:
    r = get_redis()
    pool = get_pool()
    row = await pool.fetchrow(
        """
        SELECT started_at, completed_at, status, items_classified
        FROM ct_digest_ticks
        ORDER BY started_at DESC
        LIMIT 1
        """
    )
    if row is None:
        reason = "ct_digest_ticks пуста — ticks никогда не бежали"
        bad = True
    else:
        started = row["started_at"]
        age_h = (datetime.utcnow() - started).total_seconds() / 3600
        orphan = row["completed_at"] is None and age_h > 1
        too_old = age_h > CT_TICK_MAX_AGE_HOURS
        bad = orphan or too_old
        reason = (
            f"last tick started={started} status={row['status']} "
            f"items={row['items_classified']} age_h={age_h:.1f}"
            + (" ORPHAN" if orphan else "")
            + (" STALE" if too_old else "")
        )

    if not bad:
        log.info(f"OK: ct_digest {reason}")
        await _clear_debounce(r, "ct_digest")
        return

    if not await _should_alert(r, "ct_digest"):
        log.warning(f"СБОЙ ct_digest (дебаунс): {reason}")
        return

    msg = (
        f"⚠️ <b>CT Alpha Digest stale</b>\n\n"
        f"{reason}\n\n"
        f"Проверь cron <code>5 21 * * *</code> и proxy vibecode (503 = пустой digest)."
    )
    log.error(f"СБОЙ ct_digest: {reason}")
    if _emit_alert(msg, urgent=False):
        await _mark_alerted(r, "ct_digest")


async def check_theme_burst() -> None:
    r = get_redis()
    pool = get_pool()
    # Prefer latest cycle_ts that is not in the future vs UTC (legacy MSK-labeled rows
    # sit ~+3h ahead and would hide a dead timer).
    utc_now = datetime.utcnow()
    last = await pool.fetchval(
        """
        SELECT MAX(cycle_ts) FROM alpha_signal_scores
        WHERE cycle_ts <= $1::timestamp + INTERVAL '2 minutes'
        """,
        utc_now,
    )
    if last is None:
        bad_timer = True
        timer_reason = "alpha_signal_scores: нет UTC-циклов (timer мёртв или только legacy MSK rows)"
    else:
        age_min = (utc_now - last).total_seconds() / 60
        bad_timer = age_min > THEME_SCORE_MAX_AGE_MIN
        timer_reason = f"last cycle_ts={last} age_min={age_min:.1f}"

    # s1-zero streak: z_active rows with s1=0 continuously (last 6h UTC wall)
    cutoff = datetime.utcnow() - timedelta(hours=THEME_S1_ZERO_STREAK_HOURS)
    streak = await pool.fetchrow(
        """
        SELECT
          COUNT(*) FILTER (
            WHERE z_active
              AND cycle_ts > $1
              AND (s1_msg_rate_z IS NULL OR s1_msg_rate_z = 0)
          ) AS zero_n,
          COUNT(*) FILTER (
            WHERE z_active AND cycle_ts > $1
          ) AS z_n,
          COUNT(*) FILTER (
            WHERE z_active
              AND cycle_ts > $1
              AND s1_msg_rate_z IS NOT NULL AND s1_msg_rate_z <> 0
          ) AS nonzero_n
        FROM alpha_signal_scores
        """,
        cutoff,
    )
    zero_n = int(streak["zero_n"] or 0)
    z_n = int(streak["z_n"] or 0)
    nonzero_n = int(streak["nonzero_n"] or 0)
    # s1=0 is normal when discussion chats quiet. Flag only if there was
    # actual traffic in a chat-topic bucket during the window but s1 never moved
    # (classic MSK/UTC skew regression).
    recent_chat_msgs = await pool.fetchval(
        """
        SELECT COUNT(*) FROM channel_messages
        WHERE source_account = 'private_mirror'
          AND message_date > $1
          AND (
            (channel_id = 1000000001 AND telegram_topic_id = 501)
            OR (channel_id = 1000000001 AND telegram_topic_id = 501)
            OR (channel_id = 1000000001 AND telegram_topic_id = 501)
            OR (channel_id = 1000000001 AND telegram_topic_id = 501)
          )
        """,
        cutoff,
    )
    bad_s1 = (
        z_n >= 50
        and nonzero_n == 0
        and (recent_chat_msgs or 0) >= 80  # real traffic but signal never left 0
    )

    if not bad_timer and not bad_s1:
        log.info(f"OK: theme_burst {timer_reason}; s1 nonzero={nonzero_n}/{z_n}")
        await _clear_debounce(r, "theme_burst")
        return

    if not await _should_alert(r, "theme_burst"):
        log.warning("СБОЙ theme_burst (дебаунс)")
        return

    parts = []
    if bad_timer:
        parts.append(f"timer: {timer_reason}")
    if bad_s1:
        parts.append(
            f"s1 forever zero on z_active ({zero_n}/{z_n} in 6h) — "
            f"проверь UTC cycle_ts vs message_date (MSK skew bug)"
        )
    msg = (
        f"⚠️ <b>Theme Burst unhealthy</b>\n\n"
        + "\n".join(f"• {p}" for p in parts)
        + "\n\nФикс: <code>sudo systemctl start xanalyst-theme-burst-worker.timer</code>"
    )
    log.error(f"СБОЙ theme_burst: {parts}")
    if _emit_alert(msg, urgent=False):
        await _mark_alerted(r, "theme_burst")


async def main() -> None:
    await init_redis()
    await init_db()
    try:
        await check_discord_monitor()
        await check_catchmint()
        await check_ct_digest()
        await check_theme_burst()
    finally:
        await close_redis()
        await close_db()


if __name__ == "__main__":
    asyncio.run(main())
