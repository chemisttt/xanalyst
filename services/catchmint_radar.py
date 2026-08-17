"""
spec 003: CatchMint Mint Radar — main service.

Async loop поллит /timeseries/mints/overview/ каждые `catchmint_overview_poll_sec` секунд,
для каждой коллекции с burst-сигналом эмитит editable card в TG forum topic + per-fire
red-flag enrichment.

Variant C emission (как в private_mirror_dedup):
- t=0: first burst → SETNX cooldown + INSERT row + send TG → ACTIVE[addr]
- t+poll: если ещё в overview → edit message с обновлёнными counts
- t+enrichment_refresh: re-fetch detail/flags, edit на severity escalation
- адрес выпал из top-50 на 3 consecutive polls → final edit ✅ + closed_at, lock

Run: python -m services.catchmint_radar
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

from shared.catchmint_client import CatchmintClient, RateLimited
from shared.catchmint_safety import evaluate_safety, SafetyVerdict
from shared.catchmint_signals import evaluate_collection, BurstDecision
from shared.config import settings
from shared.db import init_db, close_db, get_pool
from shared.notifier import send_to_topic, edit_message_in_topic
from shared.redis_client import init_redis, close_redis, get_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("catchmint_radar")

WINDOW_CLOSE_MISSES = 3   # M2: сколько подряд misses до window close
ENRICH_REFRESH_SEC = 3600  # raz в час обновляем флаги для active alerts
COOLDOWN_KEY = "catchmint:cooldown:{addr}"


# ─────────────────────────── helpers ────────────────────────────


def _human_age(deployed_iso: str | None, now: datetime) -> str | None:
    """'8m ago' / '2h ago' / '3d ago' или None если не парсится.
    `now` ожидается naive UTC (как и всё в catchmint_radar после datetime-fix)."""
    if not deployed_iso or not isinstance(deployed_iso, str):
        return None
    try:
        dt = datetime.fromisoformat(deployed_iso.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None
    delta = now - dt
    secs = delta.total_seconds()
    if secs < 0:
        return None
    if secs < 3600:
        return f"{int(secs / 60)}m ago"
    if secs < 86400:
        return f"{int(secs / 3600)}h ago"
    return f"{int(secs / 86400)}d ago"


def _format_msg(
    row: dict,
    burst: BurstDecision,
    detail: dict,
    verdict: SafetyVerdict,
    closed: bool = False,
    escalated: bool = False,
    window_label: str = "10мин",   # human-readable label for the window
    peak_in_window: int | None = None,   # для closed: показать peak за окно
) -> str:
    """
    Compact HTML message для TG. Single source of truth для rate = burst.mints_in_window
    (это totalCounts из catchmint endpoint с ?window=<sec>).

    Header:
      🪙 <Name> · +N за 10мин · 👥 W mint
      📦 X/Y (Z%) · 🕐 age
      🔗 catchmint · 0x… [social · image]
    """
    addr = row["address"]
    name = row.get("name") or "Unknown"
    mints = burst.mints_in_window
    total_supply = row.get("totalSupply") or 0
    max_supply = row.get("maxSupply")
    if isinstance(max_supply, list):
        max_supply = max_supply[0].get("supply") if max_supply else None

    head_emoji = {"danger": "🚨", "warn": "⚠️", "safe": "🪙"}.get(verdict.severity, "🪙")

    # Supply: "626/777 (80%)" if known max, else "626/∞"
    if max_supply and isinstance(max_supply, int) and max_supply > 0:
        pct = int(total_supply / max_supply * 100)
        supply_str = f"{total_supply:,}/{max_supply:,} ({pct}%)"
    else:
        supply_str = f"{total_supply:,}/∞"

    wallets = (detail.get("uniqueWallets") if detail else None) or 0
    wallets_part = f"👥 {wallets:,} mint" if wallets else ""

    now = datetime.utcnow()
    age = _human_age(detail.get("deployedAt"), now) if detail else None

    if closed:
        peak = peak_in_window if peak_in_window is not None else mints
        head = f"✅ {head_emoji} <b>{name}</b> · closed · peak +{peak:,} за {window_label}"
    else:
        head = f"{head_emoji} <b>{name}</b> · +{mints:,} за {window_label}"
    if wallets_part:
        head += f" · {wallets_part}"
    if escalated and not closed:
        head = f"⚠ [escalated] {head}"

    # Badges line (warn flags)
    extra_badges = list(verdict.badges)
    if not row.get("isVerified"):
        extra_badges.append("⚠ unverified")
    badges_line = ""
    if extra_badges:
        badges_line = "\n🚩 " + " · ".join(extra_badges)

    # Meta: supply · age (wallets уже на header'е)
    meta_parts = [f"📦 {supply_str}"]
    if age:
        meta_parts.append(f"🕐 {age}")
    meta_line = "\n" + " · ".join(meta_parts)

    # Social links (только если есть)
    social_parts = []
    if detail:
        if detail.get("twitterUrl"):
            social_parts.append(f"<a href=\"{detail['twitterUrl']}\">🐦</a>")
        if detail.get("websiteUrl"):
            social_parts.append(f"<a href=\"{detail['websiteUrl']}\">🌐</a>")
        if detail.get("discordUrl"):
            social_parts.append(f"<a href=\"{detail['discordUrl']}\">💬</a>")
    social_inline = ""
    if social_parts:
        social_inline = " · " + " ".join(social_parts)

    img = row.get("imageUrl")
    img_inline = ""
    if img and isinstance(img, str) and img.startswith("http"):
        img_inline = f" · <a href=\"{img}\">🖼</a>"

    addr_short = f"{addr[:6]}…{addr[-4:]}"

    closed_line = ""
    if closed:
        closed_line = f"\n🔒 closed at {now.strftime('%H:%M UTC')}"

    return (
        f"{head}"
        f"{badges_line}"
        f"{meta_line}"
        f"{closed_line}\n"
        f"\n"
        f"🔗 <a href=\"https://catchmint.xyz/collection/ethereum/{addr}\">catchmint</a>"
        f" · <a href=\"https://etherscan.io/address/{addr}\">{addr_short}</a>"
        f"{social_inline}"
        f"{img_inline}"
    )


# ─────────────────────────── enrichment ────────────────────────────


async def fetch_enrichment(
    client: CatchmintClient, addr: str
) -> tuple[dict, list[dict]]:
    """
    Параллельный fetch detail + flags. Fail-open: если оба упали → ({}, []).
    Используется до cooldown SETNX (C3 fix).
    """
    if not settings.catchmint_enrich_enabled:
        return {}, []
    detail_task = asyncio.create_task(client.get_contract_detail(addr))
    flags_task = asyncio.create_task(client.get_contract_flags(addr))
    detail_res, flags_res = await asyncio.gather(detail_task, flags_task, return_exceptions=True)
    detail = detail_res if not isinstance(detail_res, Exception) else {}
    flags = flags_res if not isinstance(flags_res, Exception) else []
    if isinstance(detail_res, Exception):
        log.warning(f"enrich detail fail for {addr}: {detail_res!r}")
    if isinstance(flags_res, Exception):
        log.warning(f"enrich flags fail for {addr}: {flags_res!r}")
    return detail, flags


# ─────────────────────────── PG operations ────────────────────────────


async def hydrate_active_state(pool) -> dict[str, dict]:
    """
    m7 fix: boot — загрузить незакрытые alerts из PG, восстановить in-memory active set.
    Возвращает {address: {msg_id, peak_count, last_enrich_at, last_severity}}.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT address, telegram_msg_id, severity, peak_bucket_count,
                   last_updated, last_enrich_at
            FROM catchmint_alerts
            WHERE closed_at IS NULL AND emitted_locked = FALSE
            """
        )
    active = {}
    for r in rows:
        active[r["address"]] = {
            "msg_id": r["telegram_msg_id"],
            "peak_count": r["peak_bucket_count"],
            "last_enrich_at": r["last_enrich_at"],
            "last_severity": r["severity"],
            "miss_count": 0,
        }
    log.info(f"hydrated {len(active)} active alerts from PG")
    return active


async def insert_alert(
    pool,
    row: dict,
    burst: BurstDecision,
    detail: dict,
    flags: list[dict],
    verdict: SafetyVerdict,
    msg_id: int | None,
    skip: bool,
    now: datetime,
):
    """INSERT в catchmint_alerts. skip=True → emitted_locked + closed_at, без msg_id."""
    max_supply = row.get("maxSupply")
    if isinstance(max_supply, list):
        max_supply = max_supply[0].get("supply") if max_supply else None

    closed_at = now if skip else None
    emitted_locked = skip

    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO catchmint_alerts (
                    address, chain, name, image_url,
                    first_bucket_count, first_total_counts, first_total_supply,
                    max_supply, is_verified, simulation_passed,
                    peak_bucket_count, last_bucket_count, last_total_counts,
                    flag_labels, flag_count, notable_flag_count, hide_count,
                    severity, deployer, deployed_at, first_mint_at,
                    is_proxy, implementation_address, unique_wallets,
                    twitter_url, discord_url, website_url,
                    telegram_msg_id, telegram_topic_id, emitted_locked,
                    first_seen, last_updated, closed_at, last_enrich_at,
                    skip_reasons
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7,
                    $8, $9, $10,
                    $11, $12, $13,
                    $14, $15, $16, $17,
                    $18, $19, $20, $21,
                    $22, $23, $24,
                    $25, $26, $27,
                    $28, $29, $30,
                    $31, $32, $33, $34,
                    $35
                )
                """,
                row["address"], row.get("chain") or "Ethereum",
                row.get("name") or "Unknown", row.get("imageUrl"),
                burst.mints_in_window, row.get("totalCounts") or 0,
                row.get("totalSupply") or 0,
                max_supply if isinstance(max_supply, int) else None,
                bool(row.get("isVerified")), bool(row.get("simulationPassed")),
                burst.mints_in_window, burst.mints_in_window, row.get("totalCounts") or 0,
                json.dumps(flags or []),
                (detail.get("flagCount") if detail else 0) or 0,
                (detail.get("notableFlagCount") if detail else 0) or 0,
                (detail.get("hideCount") if detail else 0) or 0,
                verdict.severity,
                (detail.get("deployer") if detail else None),
                _parse_dt(detail.get("deployedAt")) if detail else None,
                _parse_dt(detail.get("firstMint")) if detail else None,
                bool(detail.get("isProxy")) if detail else None,
                (detail.get("implementationAddress") if detail else None),
                (detail.get("uniqueWallets") if detail else None),
                (detail.get("twitterUrl") if detail else None),
                (detail.get("discordUrl") if detail else None),
                (detail.get("websiteUrl") if detail else None),
                msg_id, settings.catchmint_topic_id, emitted_locked,
                now, now, closed_at, now if detail else None,
                ", ".join(verdict.reasons) if verdict.reasons else None,
            )
        except Exception as e:
            # m2 unique constraint violation — конкурентный insert на тот же addr
            log.warning(f"insert_alert failed for {row['address']}: {e!r}")
            raise


def _parse_dt(s: str | None) -> datetime | None:
    """Parse ISO-8601 → naive UTC datetime (без tzinfo).
    Catchmint возвращает '...Z' (UTC), а наша PG-схема использует TIMESTAMP без tz,
    как и весь codebase (datetime.utcnow в other services). Strip tz после parse."""
    if not s or not isinstance(s, str):
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except (ValueError, TypeError):
        return None


async def update_alert_counts(
    pool, addr: str, burst: BurstDecision, total_counts: int, now: datetime
):
    """UPDATE peak/last counts при edit-cycle."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE catchmint_alerts
            SET peak_bucket_count = GREATEST(peak_bucket_count, $2),
                last_bucket_count = $2,
                last_total_counts = $3,
                last_updated = $4
            WHERE address = $1 AND closed_at IS NULL
            """,
            addr, burst.mints_in_window, total_counts, now,
        )


async def update_alert_severity(
    pool, addr: str, severity: str, flags: list[dict], detail: dict, now: datetime
):
    """UPDATE severity + flag snapshot после enrichment refresh."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE catchmint_alerts
            SET severity = $2,
                flag_labels = $3,
                flag_count = $4,
                notable_flag_count = $5,
                hide_count = $6,
                last_enrich_at = $7
            WHERE address = $1 AND closed_at IS NULL
            """,
            addr, severity, json.dumps(flags or []),
            (detail.get("flagCount") if detail else 0) or 0,
            (detail.get("notableFlagCount") if detail else 0) or 0,
            (detail.get("hideCount") if detail else 0) or 0,
            now,
        )


async def close_alert(pool, addr: str, now: datetime):
    """Финальный close — set closed_at, lock."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE catchmint_alerts
            SET closed_at = $2,
                emitted_locked = TRUE,
                last_updated = $2
            WHERE address = $1 AND closed_at IS NULL
            """,
            addr, now,
        )


async def incr_edit_failed(pool, addr: str):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE catchmint_alerts SET edit_failed_count = edit_failed_count + 1 "
            "WHERE address = $1 AND closed_at IS NULL",
            addr,
        )


# ─────────────────────────── main loop ────────────────────────────


def _window_label(window_sec: int) -> str:
    """human-readable: 600 → '10мин', 3600 → '1ч'."""
    if window_sec < 60:
        return f"{window_sec}с"
    mins = window_sec // 60
    if mins < 60:
        return f"{mins}мин"
    hours = mins // 60
    return f"{hours}ч"


async def process_overview_cycle(
    client: CatchmintClient,
    pool,
    redis,
    active: dict[str, dict],
):
    """Один проход по overview/. Mutates `active`."""
    window_sec = settings.catchmint_window_sec
    window_label = _window_label(window_sec)
    overview = await client.get_overview(window_sec=window_sec)
    current_top = {r["address"] for r in overview if "address" in r}
    now = datetime.utcnow()  # naive UTC для PG TIMESTAMP columns

    # Обработка каждой коллекции
    for row in overview:
        addr = row.get("address")
        if not addr:
            continue
        burst = evaluate_collection(row, settings)
        if not burst.fires:
            continue

        if addr in active:
            # ── Active edit-path ──
            active[addr]["miss_count"] = 0
            await update_alert_counts(pool, addr, burst, burst.mints_in_window, now)
            # Track peak (для closed message)
            active[addr]["peak_count"] = max(
                active[addr].get("peak_count", 0), burst.mints_in_window
            )

            # Enrichment refresh раз в час
            last_enrich = active[addr].get("last_enrich_at")
            need_refresh = (
                last_enrich is None
                or (now - last_enrich).total_seconds() > ENRICH_REFRESH_SEC
            )
            verdict_now = None
            detail = {}
            if need_refresh and settings.catchmint_enrich_enabled:
                detail, flags = await fetch_enrichment(client, addr)
                if detail or flags:
                    verdict_now = evaluate_safety(detail, flags, settings, now)
                    active[addr]["last_enrich_at"] = now
                    await update_alert_severity(
                        pool, addr, verdict_now.severity, flags, detail, now
                    )
                if detail:
                    active[addr]["last_detail"] = detail

            if not detail and "last_detail" in active[addr]:
                detail = active[addr]["last_detail"]

            old_severity = active[addr].get("last_severity", "safe")
            escalated = (
                verdict_now is not None
                and {"safe": 0, "warn": 1, "danger": 2}[verdict_now.severity]
                > {"safe": 0, "warn": 1, "danger": 2}[old_severity]
            )
            if verdict_now:
                active[addr]["last_severity"] = verdict_now.severity
            else:
                verdict_now = SafetyVerdict(severity=old_severity, skip=False)

            text = _format_msg(
                row, burst, detail, verdict_now,
                closed=False, escalated=escalated,
                window_label=window_label,
            )
            msg_id = active[addr].get("msg_id")
            if msg_id:
                ok = await asyncio.to_thread(
                    edit_message_in_topic,
                    text, msg_id, settings.catchmint_topic_id,
                )
                if not ok:
                    log.warning(f"edit failed for {addr} msg_id={msg_id}")
                    await incr_edit_failed(pool, addr)
            log.info(
                f"edit active {addr} ({row.get('name','?')}) "
                f"+{burst.mints_in_window} за {window_label} sev={verdict_now.severity}"
            )

        else:
            # ── New burst path ──
            # C3 fix: enrichment BEFORE cooldown SETNX
            detail, flags = await fetch_enrichment(client, addr)
            verdict = evaluate_safety(detail, flags, settings, now)

            if verdict.skip:
                try:
                    await insert_alert(pool, row, burst, detail, flags, verdict, msg_id=None, skip=True, now=now)
                    log.info(
                        f"SKIPPED {addr} ({row.get('name','?')}): reasons={verdict.reasons}"
                    )
                except Exception:
                    pass
                continue

            cooldown_key = COOLDOWN_KEY.format(addr=addr)
            ttl = settings.catchmint_cooldown_hours * 3600
            new_set = await redis.set(cooldown_key, "1", nx=True, ex=ttl)
            if not new_set:
                log.info(f"cooldown active for {addr} ({row.get('name','?')}), skipping fire")
                continue

            text = _format_msg(
                row, burst, detail, verdict, closed=False,
                window_label=window_label,
            )
            msg_id = await asyncio.to_thread(
                send_to_topic, text, settings.catchmint_topic_id,
            )
            try:
                await insert_alert(
                    pool, row, burst, detail, flags, verdict,
                    msg_id=msg_id, skip=False, now=now,
                )
            except Exception:
                log.warning(f"new-burst insert race for {addr}")
                continue

            active[addr] = {
                "msg_id": msg_id,
                "peak_count": burst.mints_in_window,
                "last_enrich_at": now,
                "last_severity": verdict.severity,
                "miss_count": 0,
                "last_detail": detail or {},
            }
            log.info(
                f"FIRE {addr} ({row.get('name','?')}) +{burst.mints_in_window} за {window_label} "
                f"sev={verdict.severity} msg_id={msg_id}"
            )

    # ── Window close (M2: 3-miss grace) ──
    for addr in list(active.keys()):
        if addr in current_top:
            continue  # уже handled выше
        active[addr]["miss_count"] += 1
        if active[addr]["miss_count"] < WINDOW_CLOSE_MISSES:
            continue
        # Close
        msg_id = active[addr].get("msg_id")
        # Используем последние известные данные — но row уже нет. Construct minimal closing msg.
        async with pool.acquire() as conn:
            r = await conn.fetchrow(
                """
                SELECT name, chain, image_url, peak_bucket_count, last_bucket_count,
                       last_total_counts, is_verified, simulation_passed, max_supply,
                       severity, flag_labels, deployer, deployed_at, twitter_url,
                       discord_url, website_url, unique_wallets
                FROM catchmint_alerts WHERE address = $1 AND closed_at IS NULL
                """, addr,
            )
        if r:
            fake_row = {
                "address": addr,
                "name": r["name"], "chain": r["chain"], "imageUrl": r["image_url"],
                "totalCounts": r["last_total_counts"],
                "totalSupply": 0,
                "maxSupply": r["max_supply"],
                "isVerified": r["is_verified"],
                "simulationPassed": r["simulation_passed"],
            }
            fake_burst = BurstDecision(
                fires=True, reason="closed",
                mints_in_window=r["last_bucket_count"],
            )
            fake_detail = {
                "deployer": r["deployer"],
                "deployedAt": r["deployed_at"].isoformat() if r["deployed_at"] else None,
                "twitterUrl": r["twitter_url"],
                "discordUrl": r["discord_url"],
                "websiteUrl": r["website_url"],
                "uniqueWallets": r["unique_wallets"],
            }
            verdict = SafetyVerdict(severity=r["severity"], skip=False)
            text = _format_msg(
                fake_row, fake_burst, fake_detail, verdict, closed=True,
                window_label=window_label,
                peak_in_window=r["peak_bucket_count"],
            )
            if msg_id:
                ok = await asyncio.to_thread(
                    edit_message_in_topic, text, msg_id, settings.catchmint_topic_id,
                )
                if not ok:
                    await incr_edit_failed(pool, addr)
        await close_alert(pool, addr, now)
        log.info(f"CLOSED {addr} (3 misses)")
        del active[addr]


# ─────────────────────────── runner ────────────────────────────


async def main_loop():
    if not settings.catchmint_enabled:
        log.info("catchmint disabled in config (CATCHMINT_ENABLED=false), exiting")
        return
    if not settings.catchmint_topic_id:
        log.error("CATCHMINT_TOPIC_ID не задан в .env — не запускаюсь")
        return

    await init_db()
    await init_redis()
    pool = get_pool()
    redis = get_redis()

    active = await hydrate_active_state(pool)

    stop = asyncio.Event()

    def _on_signal(*_a):
        log.info("SIGTERM/SIGINT received, draining cycle then exit")
        stop.set()
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(s, _on_signal)
        except NotImplementedError:
            pass

    log.info(
        f"catchmint_radar started: poll={settings.catchmint_overview_poll_sec}s, "
        f"topic={settings.catchmint_topic_id}, "
        f"window={settings.catchmint_window_sec}s ({_window_label(settings.catchmint_window_sec)}), "
        f"min_mints={settings.catchmint_min_mints_in_window}"
    )

    cycle = 0
    while not stop.is_set():
        cycle += 1
        try:
            async with CatchmintClient(
                settings.catchmint_api_base, settings.catchmint_user_agent
            ) as client:
                await asyncio.shield(
                    process_overview_cycle(client, pool, redis, active)
                )
        except RateLimited as e:
            wait = e.retry_after + random.uniform(0, 5)
            log.warning(f"rate limited, sleeping {wait:.0f}s")
            try:
                await asyncio.wait_for(stop.wait(), timeout=wait)
                break
            except asyncio.TimeoutError:
                continue
        except Exception as e:
            log.error(f"cycle {cycle} failed: {e!r}", exc_info=True)

        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.catchmint_overview_poll_sec)
            break
        except asyncio.TimeoutError:
            pass

    log.info("draining: closing connections")
    await close_redis()
    await close_db()
    log.info("catchmint_radar stopped cleanly")


if __name__ == "__main__":
    asyncio.run(main_loop())
