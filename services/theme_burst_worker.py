"""theme_burst_worker — Spec 002 V1.5 Anomaly-First Theme + Burst Detector.

systemd-timer driven (OnUnitInactiveSec=5min), Type=oneshot. Single tick per invocation.

Algorithm per tick:
  Step 0:   Kill-switch guard (fires/hour, noise rate, raw-signal rate)
  Step 0.5: Per-day budget check → daily-digest mode
  Step 0.6: Auto-pause if ≥N user_tag='noise' / 24h
  Step 1-3: Per-bucket parallel signal compute (all 11 chat-topics)
  Step 4:   INSERT alpha_signal_scores с UNIQUE(cycle_ts_5min, bucket) race protection
  Step 5:   For each fired bucket:
            (a) SETNX cooldown FIRST (atomic, fail-closed on Redis-down)
            (b) LLM judge (Gemini-first chain, 15s timeout per provider)
            (c) Route: noise → drop+release_cooldown; LLM-fail → raw-signal to SHADOW; ok → emit
            (d) INSERT alpha_events + inline keyboard for tagging

Try/finally cleans up pending cooldowns on cycle crash (rev 2 M-N7 fix).

Usage:
  python -m services.theme_burst_worker
"""

import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from html import escape

import asyncpg
import redis.asyncio as aioredis

from shared.author_parser import extract_real_author
from shared.config import settings
from shared.llm_client import (
    AllProvidersFailedError,
    LLMResponseInvalidError,
    call_llm_async_gemini_first,
)
from shared.notifier import (
    make_theme_burst_keyboard,
    resolve_mirror_topic_id,
    send_to_topic,
)
from shared.source_routing import get_bucket_label, iter_chat_topic_buckets
from shared.theme_signals import (
    SignalResult,
    acquire_cooldown_atomic,
    combine_score,
    compute_s1_msg_rate_z,
    compute_s2_rate_ratio,
    compute_s3_unique_author_z,
    compute_s4_new_ticker,
    compute_s5_new_ca,
    compute_s6_rare_authors,
    compute_s8_url_domain_burst,
    is_z_signal_active,
    release_cooldown,
)

# TG hard limit 4096; оставляем запас под keyboard metadata
_ALERT_MAX_CHARS = 3800


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("theme_burst_worker")


# =============================================================================
# Per-bucket result aggregator
# =============================================================================

@dataclass
class BucketResult:
    chat_id: int
    topic_id: int | None
    label: str
    cycle_ts: datetime
    z_active: bool
    s1: SignalResult
    s2: SignalResult
    s3: SignalResult
    s4: SignalResult
    s5: SignalResult
    s6: SignalResult
    s8: SignalResult
    composite_score: float | None
    n_distinct_signals: int
    hard_fire: bool
    soft_fire: bool


async def compute_bucket(pool, redis, chat_id, topic_id, label, cycle_ts) -> BucketResult:
    """Compute all 8 signals для одного bucket + combine."""
    z_active = await is_z_signal_active(pool, chat_id, topic_id, label, cycle_ts)

    s1, s2, s3, s6, s8 = await asyncio.gather(
        compute_s1_msg_rate_z(pool, redis, chat_id, topic_id, label, cycle_ts, z_active),
        compute_s2_rate_ratio(pool, redis, chat_id, topic_id, label, cycle_ts, z_active),
        compute_s3_unique_author_z(pool, redis, chat_id, topic_id, label, cycle_ts, z_active),
        compute_s6_rare_authors(pool, chat_id, topic_id, label, cycle_ts, z_active),
        compute_s8_url_domain_burst(pool, chat_id, topic_id, label, cycle_ts, z_active),
    )
    s4 = await compute_s4_new_ticker(pool, chat_id, topic_id, label, cycle_ts)
    s5 = await compute_s5_new_ca(pool, chat_id, topic_id, label, cycle_ts)

    combined = combine_score(s1, s2, s3, s4, s5, s6, s8, z_active)

    return BucketResult(
        chat_id=chat_id, topic_id=topic_id, label=label, cycle_ts=cycle_ts,
        z_active=z_active,
        s1=s1, s2=s2, s3=s3, s4=s4, s5=s5, s6=s6, s8=s8,
        composite_score=combined.score,
        n_distinct_signals=combined.n_distinct_signals,
        hard_fire=combined.hard_fire,
        soft_fire=combined.soft_fire,
    )


# =============================================================================
# Kill-switch / anti-habituation guards (Step 0)
# =============================================================================

@dataclass
class KillSwitchResult:
    tripped: bool
    reason: str = ""


async def check_kill_switch(pool, cycle_ts: datetime) -> KillSwitchResult:
    """Query alpha_events last 1h: fires/h, noise rate, raw-signal rate.

    Shadow-mode aware (rev 2 M-N4 fix): different min_noise_pct floor для shadow vs prod.
    """
    row = await pool.fetchrow("""
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE llm_noise=TRUE) AS noise,
          COUNT(*) FILTER (WHERE was_raw_signal=TRUE) AS raw_signal,
          COUNT(*) FILTER (WHERE shadow_mode=TRUE) AS shadow_count
        FROM alpha_events
        WHERE fired_at > $1::timestamp - INTERVAL '1 hour'
    """, cycle_ts)

    total = row['total'] or 0
    noise = row['noise'] or 0
    raw_signal = row['raw_signal'] or 0
    shadow_count = row['shadow_count'] or 0

    # Fire rate ceiling
    if total > settings.theme_burst_kill_max_fires_per_hour:
        return KillSwitchResult(tripped=True,
            reason=f"fires_per_hour={total} > {settings.theme_burst_kill_max_fires_per_hour}")

    # Noise band — mode-aware
    if total >= 5:
        noise_rate = noise / total
        is_shadow_majority = shadow_count > total / 2
        min_noise_pct = (settings.theme_burst_kill_min_noise_pct_shadow if is_shadow_majority
                        else settings.theme_burst_kill_min_noise_pct_prod)
        if noise_rate < min_noise_pct:
            return KillSwitchResult(tripped=True,
                reason=f"noise_rate={noise_rate:.2%} < min={min_noise_pct:.2%} (suspicious; LLM may be degraded)")
        if noise_rate > settings.theme_burst_kill_max_noise_pct:
            return KillSwitchResult(tripped=True,
                reason=f"noise_rate={noise_rate:.2%} > max={settings.theme_burst_kill_max_noise_pct:.2%}")

    # Raw-signal floor (rev 3 NEW M-N4)
    if total >= 5:
        raw_rate = raw_signal / total
        if raw_rate > settings.theme_burst_kill_max_raw_signal_pct:
            return KillSwitchResult(tripped=True,
                reason=f"raw_signal_rate={raw_rate:.2%} > max — LLM providers degraded")

    return KillSwitchResult(tripped=False)


async def check_per_day_budget(pool, cycle_ts: datetime) -> bool:
    """True если budget exhausted и нужно switch to daily-digest mode."""
    fires_today = await pool.fetchval("""
        SELECT COUNT(*) FROM alpha_events
        WHERE fired_at > $1::timestamp - INTERVAL '24 hours'
    """, cycle_ts)
    return (fires_today or 0) >= settings.theme_burst_per_day_budget


async def check_noise_pause(pool, redis, cycle_ts: datetime) -> bool:
    """True если ≥N noise tags / 24h AND pause not yet set, OR existing pause still active."""
    # Check existing pause
    paused_until = await redis.get("v15:noise_pause_until")
    if paused_until and float(paused_until) > cycle_ts.timestamp():
        return True

    # Check if we should set a new pause
    noise_24h = await pool.fetchval("""
        SELECT COUNT(*) FROM alpha_events
        WHERE user_tag='noise' AND fired_at > $1::timestamp - INTERVAL '24 hours'
    """, cycle_ts)

    if (noise_24h or 0) >= settings.theme_burst_noise_pause_threshold:
        await redis.set(
            "v15:noise_pause_until",
            str(cycle_ts.timestamp() + settings.theme_burst_noise_pause_duration_sec),
            ex=settings.theme_burst_noise_pause_duration_sec,
        )
        log.warning(f"Auto-pause set: {noise_24h} noise tags in 24h")
        await _send_syshealth_alert(
            f"⚠️ Theme burst auto-paused 24h (noise tags = {noise_24h}). "
            f"Resume after pause expires."
        )
        return True

    return False


# =============================================================================
# Emit helpers
# =============================================================================

async def _fetch_w30_msgs(pool, result: BucketResult, limit: int | None = None) -> list:
    """Сообщения окна W30 (новые сверху) для LLM + цитат в алерте."""
    lim = limit if limit is not None else settings.theme_burst_llm_judge_max_msgs
    return await pool.fetch(
        """
        SELECT message_date, text, author_name FROM channel_messages
        WHERE source_account='private_mirror' AND channel_id=$1
          AND (telegram_topic_id=$2 OR (telegram_topic_id IS NULL AND channel_name=$3))
          AND message_date > $4::timestamp - INTERVAL '30 minutes'
          AND message_date <= $4::timestamp + INTERVAL '1 minute'
        ORDER BY message_date DESC
        LIMIT $5
        """,
        result.chat_id, result.topic_id, result.label, result.cycle_ts, lim,
    )


def _msg_body_plain(text: str | None) -> str:
    """Убрать aggregator-префикс `[Name](tg://...) (@user)` и хвост @AGG_BOT."""
    if not text:
        return ""
    lines = text.strip().split("\n")
    if lines:
        first = lines[0]
        if "tg://user" in first or (first.startswith("[") and "](" in first):
            lines = lines[1:]
        elif first.startswith("(@") or first.startswith("@"):
            # rare bare author line
            if len(lines) > 1:
                lines = lines[1:]
    body = "\n".join(lines).strip()
    body = re.sub(r"\s*@AGG_BOT\s*$", "", body, flags=re.I)
    body = re.sub(r"\s*@AGG_BOT\s*$", "", body, flags=re.I)
    return body.strip()


def _format_quotes_html(rows: list, *, chronological: bool = True) -> str:
    """Блок цитат для TG. rows — DESC (новые сверху); в алерте показываем хронологию."""
    if not rows:
        return ""
    n = settings.theme_burst_alert_quote_msgs
    max_chars = settings.theme_burst_alert_quote_chars
    # Берём последние n сообщений окна (самые свежие), в порядке времени
    sample = list(rows[:n])
    if chronological:
        sample = list(reversed(sample))

    lines = [f"<b>Сообщения</b> <i>(последние {len(sample)} из окна 30м)</i>:"]
    for r in sample:
        ts = r["message_date"].strftime("%H:%M")
        author = extract_real_author(r["text"]) or (r.get("author_name") or "?")
        if isinstance(author, str) and author.startswith("@"):
            author = author[1:]
        body = _msg_body_plain(r["text"])
        if not body:
            # photo-only / sticker / empty after strip — keep short marker + raw head
            raw = (r["text"] or "").replace("\n", " ").strip()
            body = (raw[:80] + "…") if len(raw) > 80 else (raw or "∅ media/empty")
        body = " ".join(body.split())  # collapse whitespace for compact quote
        if len(body) > max_chars:
            body = body[: max_chars - 1] + "…"
        lines.append(
            f"<blockquote>{escape(ts)} · @{escape(str(author))}\n"
            f"{escape(body)}</blockquote>"
        )
    return "\n".join(lines)


def _cap_alert(html: str) -> str:
    if len(html) <= _ALERT_MAX_CHARS:
        return html
    return html[: _ALERT_MAX_CHARS - 20] + "\n\n… <i>(обрезано)</i>"


def _format_alert_html(
    result: BucketResult, llm_response: dict, quote_rows: list | None = None,
) -> str:
    """HTML alert: summary + signals + исходные сообщения чата."""
    bucket = escape(result.label)
    summary = escape(llm_response.get("summary_ru", "") or "")
    topic = escape(llm_response.get("topic", "") or "")
    stance = escape(str(llm_response.get("stance", "?") or "?"))
    urgency = escape(str(llm_response.get("urgency", "?") or "?"))
    tickers = llm_response.get("tickers", []) or []
    cas = llm_response.get("cas", []) or []

    signal_lines = []
    if result.s2.fired and result.s2.value:
        signal_lines.append(
            f"• msg_rate <b>×{result.s2.value:.1f}</b>"
            + (f" (z={result.s1.value:.1f})" if result.s1.value else "")
        )
    elif result.s1.fired and result.s1.value:
        signal_lines.append(f"• msg_rate z=<b>{result.s1.value:.1f}</b>")
    if result.s3.fired:
        signal_lines.append(
            f"• {result.s3.extras.get('unique_authors_w30', '?')} authors"
            f" (z={result.s3.value:.1f})"
        )
    if result.s4.fired:
        tk = ", ".join(result.s4.extras.get("tickers", [])[:5])
        signal_lines.append(f"• <b>{result.s4.value}</b> new tickers: {escape(tk)}")
    if result.s5.fired:
        signal_lines.append(f"• <b>{result.s5.value}</b> new CAs")
    if result.s6.fired:
        signal_lines.append(f"• {result.s6.value} rare authors active")
    if result.s8.fired:
        signal_lines.append(f"• {result.s8.value} new url domains")

    score_str = (
        f"score=<b>{result.composite_score:.2f}</b>"
        if result.composite_score is not None
        else "hard_fire"
    )

    extra_bits = []
    if tickers:
        extra_bits.append("тикеры: " + ", ".join(f"${escape(str(t))}" for t in tickers[:6]))
    if cas:
        extra_bits.append(
            "CA: " + ", ".join(f"<code>{escape(str(c)[:12])}…</code>" for c in cas[:3])
        )

    # cycle_ts = UTC naive; показываем UTC явно (не путать с MSK wall-clock)
    ts_label = result.cycle_ts.strftime("%H:%M UTC · %Y-%m-%d")

    body_parts = [
        f"🔥 <b>Burst: {bucket}</b>",
        f"<i>{ts_label}</i>",
        "",
        f"<b>О чём:</b> {topic}" if topic else "",
        f"{summary}" if summary else "",
        (" · ".join(extra_bits) if extra_bits else ""),
        "",
        "<b>Сигналы:</b>",
        *signal_lines,
        f"{score_str} / {result.n_distinct_signals} distinct",
        f"Stance: {stance} · Urgency: {urgency}",
    ]

    quotes = _format_quotes_html(quote_rows or [])
    if quotes:
        body_parts.extend(["", quotes])

    return _cap_alert("\n".join(p for p in body_parts if p is not None and p != ""))


def _format_raw_signal_alert_html(
    result: BucketResult, quote_rows: list | None = None,
) -> str:
    """Raw-signal alert when LLM all-providers-fail (rev 3 M8 fix)."""
    bucket = escape(result.label)
    signal_lines = []
    if result.s4.fired:
        tk = ", ".join(result.s4.extras.get("tickers", [])[:5])
        signal_lines.append(f"• {result.s4.value} new tickers: {escape(tk)}")
    if result.s5.fired:
        signal_lines.append(f"• {result.s5.value} new CAs")
    if result.s2.fired and result.s2.value:
        signal_lines.append(f"• msg_rate ×{result.s2.value:.1f}")
    if result.s1.fired and result.s1.value:
        signal_lines.append(f"• msg_rate z={result.s1.value:.1f}")

    parts = [
        f"⚠️ <b>RAW SIGNAL (LLM unavailable)</b> · {bucket}",
        f"<i>{result.cycle_ts.strftime('%H:%M UTC')}</i>",
        "",
        "<b>Сигналы:</b>",
        *signal_lines,
        f"<i>{result.hard_fire and 'hard_fire' or 'soft_fire'}</i>",
    ]
    quotes = _format_quotes_html(quote_rows or [])
    if quotes:
        parts.extend(["", quotes])
    else:
        parts.append("")
        parts.append("<i>Manual review — LLM judge unavailable.</i>")
    return _cap_alert("\n".join(p for p in parts if p is not None and p != ""))


def _build_llm_prompt_system() -> str:
    return (
        "Ты — alpha-аналитик. На входе — burst-сообщений из приватного крипто-чата. "
        "В сообщениях фигурируют РЕАЛЬНЫЕ авторы (формат `(@username)`), не зеркало-боты.\n\n"
        "Верни СТРОГО JSON, без markdown:\n"
        "{\n"
        '  "topic": "<10-15 слов: о чём говорят>",\n'
        '  "tickers": ["BTC","XYZ"],\n'
        '  "cas": ["So111..."],\n'
        '  "stance": "bull|bear|info|degen",\n'
        '  "urgency": "low|med|high",\n'
        '  "summary_ru": "<2-3 предложения по делу>",\n'
        '  "noise": true|false\n'
        "}\n\n"
        "ВАЖНО: noise=true означает «болтовня без edge'а» (gm/gn, цена-сейчас, повседневный треп).\n"
        "noise=false — есть substantive обсуждение (план, инсайд, аналитика, новая coin/event/listing)."
    )


def _build_llm_prompt_user(result: BucketResult, rows: list) -> str:
    """Format already-fetched W30 msgs for LLM judge."""
    msg_lines = [
        f"[{r['message_date'].strftime('%H:%M')}] "
        f"@{extract_real_author(r['text']) or '?'} | {_msg_body_plain(r['text'])[:300]}"
        for r in rows
    ]
    s2_val = result.s2.value if result.s2.value else 0
    s1_val = result.s1.value if result.s1.value else 0
    s3_val = result.s3.value if result.s3.value else 0

    sig_summary = (
        f"msg_rate ×{s2_val:.1f} (z={s1_val:.1f}), "
        f"authors_z={s3_val:.1f}, "
        f"new tickers: {result.s4.extras.get('tickers', [])}, "
        f"new CAs: {result.s5.extras.get('cas', [])}, "
        f"rare authors: {result.s6.value if result.s6.value else 0}"
    )
    return (
        f"=== Burst в чате \"{result.label}\" ===\n"
        f"Сигналы: {sig_summary}\n\n"
        f"=== Сообщения last 30 min (новые сверху) ===\n"
        + "\n".join(msg_lines)
        + f"\n\n(всего {len(rows)} сообщений в выборке)"
    )


async def _insert_alpha_event(
    pool, result: BucketResult, llm_response: dict | None = None,
    llm_noise: bool | None = None, llm_provider: str | None = None,
    llm_raw_response: str | None = None, tg_msg_id: int | None = None,
    tg_topic_id: int | None = None, was_raw_signal: bool = False,
    shadow_mode: bool | None = None,
) -> int:
    """INSERT alpha_events row, return id."""
    cooldown_until = result.cycle_ts.replace(microsecond=0)
    cooldown_until_ts = cooldown_until.timestamp() + settings.theme_burst_cooldown_sec
    cooldown_until_dt = datetime.fromtimestamp(cooldown_until_ts)

    signals_json = {
        "z_active": result.z_active,
        "s1": result.s1.value, "s2": result.s2.value, "s3": result.s3.value,
        "s4": result.s4.value, "s5": result.s5.value, "s6": result.s6.value,
        "s8": result.s8.value,
        "composite_score": result.composite_score,
        "n_distinct": result.n_distinct_signals,
        "hard_fire": result.hard_fire,
        "soft_fire": result.soft_fire,
    }

    row = await pool.fetchrow("""
        INSERT INTO alpha_events
            (bucket_chat_id, bucket_topic_id, bucket_label, fired_at,
             signals_json, raw_msg_ids,
             llm_topic, llm_summary, llm_tickers, llm_cas, llm_stance, llm_urgency,
             llm_noise, llm_provider, llm_raw_response,
             tg_msg_id, tg_topic_id, cooldown_until,
             shadow_mode, was_raw_signal)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
        RETURNING id
    """,
        result.chat_id, result.topic_id, result.label, result.cycle_ts,
        json.dumps(signals_json), [],  # raw_msg_ids — empty (could populate from W30 query if needed)
        (llm_response or {}).get("topic"),
        (llm_response or {}).get("summary_ru"),
        (llm_response or {}).get("tickers"),
        (llm_response or {}).get("cas"),
        (llm_response or {}).get("stance"),
        (llm_response or {}).get("urgency"),
        llm_noise, llm_provider, llm_raw_response,
        tg_msg_id, tg_topic_id, cooldown_until_dt,
        (settings.theme_burst_dry_run if shadow_mode is None else shadow_mode),
        was_raw_signal,
    )
    return row['id']


def _send_syshealth_alert_sync(text: str) -> None:
    """Sync helper for syshealth alerts (called from sync paths)."""
    topic_id = resolve_mirror_topic_id("SYSHEALTH")
    if topic_id:
        send_to_topic(text, topic_id)


async def _send_syshealth_alert(text: str) -> None:
    """Async wrapper для SYSHEALTH alerts."""
    await asyncio.to_thread(_send_syshealth_alert_sync, text)


# =============================================================================
# Main cycle
# =============================================================================

async def run_cycle():
    # message_date в channel_messages = Telethon UTC naive (msg.date.replace(tzinfo=None)).
    # datetime.now() на VPS = MSK → окно «last 30 min» уезжает на +3h и всегда пустое
    # (s1/s2/s4 = 0 forever). Держим UTC как catchmint_radar / private_mirror_digest.
    cycle_ts = datetime.utcnow()
    log.info(f"Cycle start at {cycle_ts.isoformat()}Z (UTC)")

    pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    redis_url = os.getenv("REDIS_URL") or settings.redis_url
    redis = aioredis.from_url(redis_url, decode_responses=True)

    pending_cooldowns_to_release: list[tuple[int, int | None]] = []

    try:
        # Step 0: kill-switch — НЕ валим весь cycle (раньше return → 0 scores + 0 emits
        # на час после 5× LLM-noise; ночной ForumB/игры выглядели как «theme burst упал»).
        # Вместо этого: force shadow routing + debounce SYSHEALTH.
        force_shadow = False
        kill = await check_kill_switch(pool, cycle_ts)
        if kill.tripped:
            force_shadow = True
            await redis.set("v15:dry_run_override", "true", ex=3600)  # 1h, not 24h
            # debounce alert: max 1 / hour
            if await redis.set("v15:kill_alert_sent", "1", nx=True, ex=3600):
                await _send_syshealth_alert(
                    f"⚠️ Theme burst kill-switch: {kill.reason}\n"
                    f"Циклы продолжают считать; emits → <b>SHADOW</b> 1h."
                )
            log.warning(f"Kill-switch: {kill.reason} → force_shadow=1 (cycle continues)")
        else:
            # clear override when healthy again
            if await redis.get("v15:dry_run_override"):
                await redis.delete("v15:dry_run_override")
                log.info("Kill-switch clear: dry_run_override removed")

        # Also honor leftover override from previous deploys
        if (await redis.get("v15:dry_run_override")) == "true":
            force_shadow = True

        # Step 0.5: daily budget
        if await check_per_day_budget(pool, cycle_ts):
            log.info("Per-day budget exhausted; switching to digest mode (no fires this cycle)")
            return

        # Step 0.6: noise pause (user_tag noise from TG keyboard — not LLM noise)
        if await check_noise_pause(pool, redis, cycle_ts):
            log.info("Theme burst paused due to noise tags; skipping cycle")
            return

        # Steps 1-3: parallel compute per bucket
        buckets = list(iter_chat_topic_buckets())
        log.info(f"Computing {len(buckets)} buckets in parallel")

        results = await asyncio.gather(*[
            compute_bucket(pool, redis, chat_id, topic_id, label, cycle_ts)
            for chat_id, topic_id, label in buckets
        ], return_exceptions=True)

        # Filter exceptions
        clean_results: list[BucketResult] = []
        for r in results:
            if isinstance(r, Exception):
                log.error(f"Bucket compute exception: {r}")
                continue
            clean_results.append(r)

        # Step 4: INSERT alpha_signal_scores (always — audit trail)
        for r in clean_results:
            try:
                await pool.execute("""
                    INSERT INTO alpha_signal_scores
                        (cycle_ts, bucket_chat_id, bucket_topic_id, bucket_label,
                         s1_msg_rate_z, s2_rate_ratio, s3_unique_author_z,
                         s4_new_ticker_count, s5_new_ca_count, s6_rare_authors,
                         s8_url_domain_burst, composite_score, n_distinct_signals,
                         hard_fire, soft_fire, fired, z_active)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                """,
                    r.cycle_ts, r.chat_id, r.topic_id, r.label,
                    r.s1.value, r.s2.value, r.s3.value,
                    r.s4.value, r.s5.value, r.s6.value, r.s8.value,
                    r.composite_score, r.n_distinct_signals,
                    r.hard_fire, r.soft_fire, r.hard_fire or r.soft_fire,
                    r.z_active,
                )
            except asyncpg.UniqueViolationError:
                log.debug(f"Race: {r.label} @ cycle_ts_5min already written")
                continue

        # Step 5: fire path
        fired_buckets = [r for r in clean_results if r.hard_fire or r.soft_fire]
        log.info(f"Fired buckets: {len(fired_buckets)}")

        for r in fired_buckets:
            # 5a: SETNX cooldown first (atomic)
            acquired = await acquire_cooldown_atomic(redis, r.chat_id, r.topic_id, r.cycle_ts)
            if not acquired:
                log.info(f"  {r.label}: cooldown active, skip emit")
                continue
            pending_cooldowns_to_release.append((r.chat_id, r.topic_id))

            # 5b: fetch W30 msgs once → LLM + цитаты в алерте
            w30_rows = await _fetch_w30_msgs(pool, r)
            system_prompt = _build_llm_prompt_system()
            user_content = _build_llm_prompt_user(r, w30_rows)

            # Determine target topic — shadow vs prod (kill-switch force_shadow wins)
            use_shadow = bool(settings.theme_burst_dry_run or force_shadow)
            target_label = "THEME_BURST_SHADOW" if use_shadow else "THEME_BURST"
            target_topic_id = resolve_mirror_topic_id(target_label)
            if target_topic_id is None:
                log.warning(f"  {r.label}: target topic {target_label} not configured, fallback SYSHEALTH")
                target_label = "SYSHEALTH"
                target_topic_id = resolve_mirror_topic_id("SYSHEALTH")
                if target_topic_id is None:
                    log.error(f"  {r.label}: no topic configured at all, skip emit")
                    pending_cooldowns_to_release.remove((r.chat_id, r.topic_id))
                    continue

            # 5c: LLM judge
            llm_response = None
            llm_provider = None
            llm_raw_response = None
            try:
                llm_response, llm_provider = await call_llm_async_gemini_first(
                    system_prompt, user_content,
                    response_format="json",
                    timeout=settings.theme_burst_llm_timeout_sec,
                )
                llm_raw_response = json.dumps(llm_response, ensure_ascii=False)
            except (AllProvidersFailedError, LLMResponseInvalidError, asyncio.TimeoutError) as e:
                log.warning(f"  {r.label}: LLM failed ({type(e).__name__}: {e}); raw-signal fallback")
                # Raw-signal fallback to SHADOW unconditionally (rev 3 fix M-N7)
                shadow_id = resolve_mirror_topic_id("THEME_BURST_SHADOW") or target_topic_id
                raw_html = _format_raw_signal_alert_html(r, w30_rows)
                msg_id = await asyncio.to_thread(send_to_topic, raw_html, shadow_id)
                await _insert_alpha_event(
                    pool, r, was_raw_signal=True,
                    tg_msg_id=msg_id, tg_topic_id=shadow_id, shadow_mode=True,
                )
                pending_cooldowns_to_release.remove((r.chat_id, r.topic_id))
                continue

            # 5d: LLM noise — не глотаем молча: шлём в SHADOW с цитатами,
            # чтобы было видно «что отфильтровали» (ночные CS/покер и т.п.).
            # Prod не спамим. Cooldown держим (не release), иначе 5×noise/25мин
            # → kill-switch noise_rate=100%.
            if llm_response.get("noise"):
                shadow_id = resolve_mirror_topic_id("THEME_BURST_SHADOW") or target_topic_id
                noise_html = _format_alert_html(r, llm_response, w30_rows)
                noise_html = (
                    f"🗑 <b>LLM noise</b> (не в prod)\n" + noise_html
                )
                msg_id = await asyncio.to_thread(send_to_topic, noise_html, shadow_id)
                await _insert_alpha_event(
                    pool, r, llm_response=llm_response, llm_noise=True,
                    llm_provider=llm_provider, llm_raw_response=llm_raw_response,
                    tg_msg_id=msg_id, tg_topic_id=shadow_id, shadow_mode=True,
                )
                log.info(
                    f"  {r.label}: LLM noise → shadow msg_id={msg_id} "
                    f"(cooldown kept)"
                )
                pending_cooldowns_to_release.remove((r.chat_id, r.topic_id))
                continue

            # 5e: normal emit with inline keyboard + quote block
            event_id = await _insert_alpha_event(
                pool, r, llm_response=llm_response, llm_noise=False,
                llm_provider=llm_provider, llm_raw_response=llm_raw_response,
                tg_topic_id=target_topic_id, shadow_mode=use_shadow,
            )
            keyboard = make_theme_burst_keyboard(event_id)
            html = _format_alert_html(r, llm_response, w30_rows)
            msg_id = await asyncio.to_thread(
                send_to_topic, html, target_topic_id,
                "HTML", True, keyboard,
            )
            if msg_id:
                await pool.execute("UPDATE alpha_events SET tg_msg_id=$1 WHERE id=$2", msg_id, event_id)
                log.info(f"  {r.label}: emitted to {target_label} msg_id={msg_id} event_id={event_id}")
            else:
                log.warning(f"  {r.label}: send_to_topic returned None")
            pending_cooldowns_to_release.remove((r.chat_id, r.topic_id))

    except Exception:
        log.exception("Cycle crashed; releasing pending cooldowns")
        # Release any cooldowns that were acquired but emit didn't complete
        for chat_id, topic_id in pending_cooldowns_to_release:
            await release_cooldown(redis, chat_id, topic_id)
        raise

    finally:
        await redis.aclose()
        await pool.close()
        log.info(f"Cycle end at {datetime.utcnow().isoformat()}Z (UTC)")


def main():
    try:
        asyncio.run(run_cycle())
    except KeyboardInterrupt:
        log.info("Interrupted; exiting")
        sys.exit(130)


if __name__ == "__main__":
    main()
