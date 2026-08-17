"""
Spec 004 Task 5: Digest formatter — markdown sections + inline keyboard.

Per-tick scope only (V1 — no cross-tick edits). Inline keyboard = 3 callback buttons
для feedback (👍/👎/✋knew). Each item gets short_id prefix (e1/c2/k1/s1/p1)
для freeform reply note refs.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from html import escape

from services.ct_digest.classifier import ClassifiedItem

RU_MONTHS = ["", "янв", "фев", "мар", "апр", "мая", "июн",
             "июл", "авг", "сен", "окт", "ноя", "дек"]

BUCKET_HEADERS = {
    "early_signals":      "🔭 <b>Ранние сигналы</b>",
    "emerging_clusters":  "🌐 <b>Кластеры</b>",
    "calendar_24h":       "📅 <b>Календарь — 24ч</b>",
    "calendar_3d":        "📅 <b>Календарь — 3д</b>",
    "calendar_7d":        "📅 <b>Календарь — 7д</b>",
    "state_reconcile":    "🔁 <b>Апдейты</b>",
    "paid_hype":          "⚠️ <b>Paid / hype</b>",
}
BUCKET_ORDER = list(BUCKET_HEADERS.keys())

# Telegram: 1 читаемое сообщение > «всё + 3 продолжения».
# 2026-08-13: tick 107 = 38 included → 2–3 TG msgs + visual dups. Cap restored.
MAX_ITEMS_PER_BUCKET = 4
MAX_TOTAL_ITEMS = 12
MAX_NOTES_CHARS = 90
MAX_TEXT_PREVIEW = 60

# Telegram hard limit на одно сообщение = 4096. Берём запас под HTML-теги/префиксы.
TG_MSG_LIMIT = 3900

# Buckets excluded from TG digest body (still classified/persisted in PG).
_SKIP_BUCKETS_IN_DIGEST = frozenset({"paid_hype"})

# @handles that are platforms / infra — not project identity for dedup keys.
_HANDLE_NOISE = frozenset({
    "opensea", "blur_io", "blur", "magiceden", "twitter", "x", "circle",
    "binance", "coinbase", "ethereum", "solana", "base", "arbitrum",
    "robinhood", "aggregator", "aggregator_bot", "uniswap", "uniswapfoundation",
    "aave", "curve", "curvefinance", "metamask", "discord", "telegram",
})

# Prefer actionable buckets when applying global cap.
_BUCKET_PRIORITY = {
    "early_signals": 0,
    "calendar_24h": 0,
    "emerging_clusters": 1,
    "calendar_3d": 1,
    "calendar_7d": 2,
    "state_reconcile": 3,
    "paid_hype": 4,
}


def _rank(it: ClassifiedItem) -> tuple:
    """Ранжирование: cross-validated первыми, потом novelty."""
    xref_boost = 1 if it.cross_refs else 0
    return (-xref_boost, -(it.novelty_score or 0), it.tweet_id)


def _global_rank(it: ClassifiedItem) -> tuple:
    """Global rank for cap: bucket priority, then cross-ref, then novelty."""
    bprio = _BUCKET_PRIORITY.get(it.bucket, 5)
    xref_boost = 1 if it.cross_refs else 0
    return (bprio, -xref_boost, -(it.novelty_score or 0), it.tweet_id)


def _norm_name(s: str | None) -> str:
    """Lowercase + strip spaces/punct for project identity."""
    if not s:
        return ""
    return re.sub(r"[\s\W_]+", "", s.lower(), flags=re.UNICODE)


def _mentions_from_text(text: str | None) -> list[str]:
    """@handles from tweet body, filtered noise, longest first (project-ish)."""
    if not text:
        return []
    found = re.findall(r"@([A-Za-z0-9_]{3,30})", text)
    out: list[str] = []
    seen: set[str] = set()
    for h in found:
        hl = h.lower()
        if hl in _HANDLE_NOISE or hl in seen:
            continue
        seen.add(hl)
        out.append(hl)
    # Prefer longer handles as project keys (depunksClub > gtd)
    out.sort(key=len, reverse=True)
    return out


def _project_key(it: ClassifiedItem) -> str | None:
    """Ключ дедупа «1 проект = 1 запись».

    Priority: CA → collection_name_hint → first @project mention in tweet.
    None → no identity → cannot merge (shown as «Без названия» until cap drops them).
    """
    addr = (it.contract_address_hint or "").strip().lower()
    if addr.startswith("0x") and len(addr) == 42:
        return f"addr:{addr}"

    name = _norm_name(it.collection_name_hint)
    if len(name) >= 2:
        return f"name:{name}"

    mentions = _mentions_from_text(getattr(it.raw, "text", None) or "")
    if mentions:
        return f"handle:{mentions[0]}"
    return None


def _canonical_name_key(name_keys: list[str]) -> dict[str, str]:
    """Map each name:X key → canonical key (shortest in prefix cluster)."""
    parent = {k: k for k in name_keys}

    def root(k: str) -> str:
        while parent[k] != k:
            parent[k] = parent[parent[k]]
            k = parent[k]
        return k

    for i, a in enumerate(name_keys):
        na = a[5:]
        for b in name_keys[i + 1:]:
            nb = b[5:]
            if not na or not nb:
                continue
            short, long_ = (na, nb) if len(na) <= len(nb) else (nb, na)
            # prefix match only if short is meaningful (avoid "go" ⊂ "goofies")
            if len(short) >= 4 and long_.startswith(short):
                ra, rb = root(a), root(b)
                # prefer shorter key as canonical
                if len(ra[5:]) <= len(rb[5:]):
                    parent[rb] = ra
                else:
                    parent[ra] = rb
    return {k: root(k) for k in name_keys}


def _merge_cross_refs(items: list[ClassifiedItem]) -> dict:
    """Union cross_refs по группе (если разные авторы дали разные подтверждения)."""
    out: dict[str, list] = {}
    for it in items:
        for key, vals in (it.cross_refs or {}).items():
            if not vals:
                continue
            bucket = out.setdefault(key, [])
            for v in vals:
                if v not in bucket:
                    bucket.append(v)
    return out


def _dedup_by_project(items: list[ClassifiedItem]) -> tuple[list[ClassifiedItem], dict]:
    """Collapse айтемы одного проекта → 1 representative (+ 👥×N meta).

    Returns (deduped_items, meta) где meta[tweet_id] = (n_mentions, [other_handles]).
    """
    raw_groups: dict[str, list[ClassifiedItem]] = {}
    singles: list[ClassifiedItem] = []
    for it in items:
        k = _project_key(it)
        if k is None:
            singles.append(it)
            continue
        raw_groups.setdefault(k, []).append(it)

    # Merge near-name keys (biwls / biwlsxyz)
    name_keys = [k for k in raw_groups if k.startswith("name:")]
    canon = _canonical_name_key(name_keys) if len(name_keys) >= 2 else {}

    groups: dict[str, list[ClassifiedItem]] = {}
    for k, grp in raw_groups.items():
        ck = canon.get(k, k)
        groups.setdefault(ck, []).extend(grp)

    deduped: list[ClassifiedItem] = []
    meta: dict[str, tuple[int, list[str]]] = {}
    # Stable-ish order: by best rank in group
    for k in sorted(groups.keys(), key=lambda kk: _rank(sorted(groups[kk], key=_rank)[0])):
        grp = sorted(groups[k], key=_rank)
        rep = grp[0]
        rep.cross_refs = _merge_cross_refs(grp)
        if len(grp) > 1:
            others = [g.raw.author_handle for g in grp[1:] if g.raw.author_handle]
            meta[rep.tweet_id] = (len(grp), others)
        deduped.append(rep)
    deduped.extend(singles)
    return deduped, meta


def _select_for_digest(
    items: list[ClassifiedItem],
    max_total: int = MAX_TOTAL_ITEMS,
) -> tuple[list[ClassifiedItem], dict]:
    """Dedup → drop paid_hype → global top-N. Returns (selected, dup_meta)."""
    included = [i for i in items if i.included and i.bucket not in _SKIP_BUCKETS_IN_DIGEST]
    deduped, dup_meta = _dedup_by_project(included)
    named = [i for i in deduped if _project_key(i) is not None]
    anon = [i for i in deduped if _project_key(i) is None]
    selected = sorted(named, key=_global_rank)[:max_total]
    if len(selected) < max_total and anon:
        room = max_total - len(selected)
        selected.extend(sorted(anon, key=_global_rank)[:room])
    return selected, dup_meta


def split_into_messages(text: str, limit: int = TG_MSG_LIMIT) -> list[str]:
    """Разбить готовый дайджест на сообщения ≤limit по границам айтемов (\\n\\n).
    Никогда не рвёт один айтем. Continuations получают '(продолжение k/n)' header."""
    paras = text.split("\n\n")
    chunks: list[str] = []
    cur = ""
    for p in paras:
        piece = p if not cur else cur + "\n\n" + p
        if len(piece) <= limit:
            cur = piece
            continue
        if cur:
            chunks.append(cur)
            cur = ""
        while len(p) > limit:  # одиночный супер-длинный абзац (редко) — хард-рез
            chunks.append(p[:limit])
            p = p[limit:]
        cur = p
    if cur:
        chunks.append(cur)
    if len(chunks) <= 1:
        return chunks or [text]
    n = len(chunks)
    out = [chunks[0]]
    for i in range(1, n):
        out.append(f"🔭 <b>CT Digest</b> — продолжение {i + 1}/{n}\n\n{chunks[i]}")
    return out


def build_inline_keyboard(tick_id: int) -> dict:
    """3 callback buttons. Pattern: ct_digest_v1:<action>:<tick_id>."""
    return {
        "inline_keyboard": [[
            {"text": "👍", "callback_data": f"ct_digest_v1:thumbs_up:{tick_id}"},
            {"text": "👎", "callback_data": f"ct_digest_v1:thumbs_down:{tick_id}"},
            {"text": "✋ knew", "callback_data": f"ct_digest_v1:knew:{tick_id}"},
        ]]
    }


def _humanize(n: int | None) -> str:
    """Compact int formatting: 142 → '142', 1500 → '1.5K', 25000 → '25K', 1.2M."""
    if n is None or n <= 0:
        return ""
    if n < 1000:
        return str(n)
    if n < 10000:
        return f"{n/1000:.1f}K".replace(".0K", "K")
    if n < 1_000_000:
        return f"{n//1000}K"
    return f"{n/1_000_000:.1f}M".replace(".0M", "M")


def _format_engagement(item: ClassifiedItem) -> str:
    """Compact engagement line: 👍142 🔁23 👁5K (skip zero/None)."""
    parts = []
    if item.raw.likes:
        parts.append(f"👍{_humanize(item.raw.likes)}")
    if item.raw.rts:
        parts.append(f"🔁{_humanize(item.raw.rts)}")
    if item.raw.views:
        parts.append(f"👁{_humanize(item.raw.views)}")
    return " ".join(parts)


def _format_promised_ru(iso: str | None) -> str:
    """ISO → 'сегодня 19:23 UTC' / 'завтра 12:00 UTC' / '20 мая 12:00 UTC'."""
    if not iso:
        return ""
    try:
        s = iso.rstrip("Z").split("+")[0]
        dt = datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return ""
    today = datetime.utcnow().date()
    if dt.date() == today:
        return f"сегодня {dt:%H:%M} UTC"
    if dt.date() == today + timedelta(days=1):
        return f"завтра {dt:%H:%M} UTC"
    return f"{dt.day} {RU_MONTHS[dt.month]} {dt:%H:%M} UTC"


def _format_author_stats(stats: dict | None) -> str:
    """Compact author block с emoji prefixes для clarity.

    Example: '🏆S·84 · 👥5.2K · 📅4y · 🔄16% · 📈+136/d'
    Skip None / zero fields. tier+score merged into single 🏆 block.
    """
    if not stats:
        return ""
    parts: list[str] = []
    tier = stats.get("tier")
    score = stats.get("twitter_score")
    if tier and score:
        parts.append(f"🏆<b>{escape(tier)}</b>·{int(score)}")
    elif tier:
        parts.append(f"🏆<b>{escape(tier)}</b>")
    elif score:
        parts.append(f"🏆{int(score)}")
    fol = stats.get("followers_count")
    if fol:
        parts.append(f"👥{_humanize(int(fol))}")
    age_d = stats.get("account_age_days")
    if age_d:
        if age_d >= 365:
            parts.append(f"📅{age_d//365}y")
        elif age_d >= 30:
            parts.append(f"📅{age_d//30}mo")
        else:
            parts.append(f"📅{age_d}d")
    rt = stats.get("rt_percentage")
    if rt is not None and rt > 0:
        parts.append(f"🔄{int(round(float(rt)))}%")
    grow = stats.get("growth_velocity")
    if grow is not None:
        g = float(grow)
        if g >= 1 or g <= -1:
            sign = "+" if g >= 0 else ""
            parts.append(f"📈{sign}{int(round(g))}/d")
    return " · ".join(parts)


def _project_title(item: ClassifiedItem) -> str:
    """Human-readable project title for TG header.

    Priority: collection name → @mention from text → short CA → «Без названия».
    Ticker/CA optional — many NFT/CT posts have only a name.
    """
    name = (item.collection_name_hint or "").strip()
    if name:
        return name[:80]
    mentions = _mentions_from_text(getattr(item.raw, "text", None) or "")
    if mentions:
        return f"@{mentions[0]}"[:80]
    ca = (item.contract_address_hint or "").strip()
    if ca.startswith("0x") and len(ca) >= 10:
        return f"{ca[:6]}…{ca[-4:]}"
    return "Без названия"


def _format_item_line(
    item: ClassifiedItem,
    tier_map: dict[str, str | None] | None = None,  # legacy — kept for back-compat
    stats_map: dict[str, dict] | None = None,
    dup: tuple[int, list[str]] | None = None,
) -> str:
    """Compact item: <b>Project</b> · @handle · meta / stats / date / notes.

    Project name first (readable scan). short_id intentionally NOT in UI —
    was e1/k12 noise; feedback eval deferred.

    dup = (n_mentions, [other_handles]) если проект упомянут несколькими авторами
    (дедуп «1 проект = 1 запись») → рендерим 👥×N badge.
    """
    title = _project_title(item)
    handle = item.raw.author_handle or "?"
    notes = (item.mechanic_notes or "")[:MAX_NOTES_CHARS]
    url = item.raw.url or ""

    # Cross-ref badge
    xref = ""
    if item.cross_refs:
        labels = []
        if item.cross_refs.get("twitter_watchlist"):
            labels.append("WL")
        if item.cross_refs.get("catchmint"):
            labels.append("CM")
        if item.cross_refs.get("mirror_meme"):
            labels.append("MM")
        if labels:
            xref = f" ✅{'/'.join(labels)}"

    # Tweet engagement (per-tweet likes/rts/views)
    eng = _format_engagement(item)
    eng_str = f" · {eng}" if eng else ""

    # Dedup badge: проект упомянут N авторами (1 проект = 1 запись)
    dup_badge = ""
    if dup and dup[0] > 1:
        dup_badge = f" 👥×{dup[0]}"

    # Single tag inline (top-priority hint)
    tag = f" #{escape(item.tags[0])}" if item.tags else ""

    # Author stats (from twitter_analyses if handle ever profiled)
    stats = (stats_map or {}).get(handle.lower())
    stats_str = _format_author_stats(stats)
    stats_line = f"   {stats_str}\n" if stats_str else ""

    # Fallback: legacy tier_map (только tier) если stats_map нет
    if not stats_str and tier_map:
        t = tier_map.get(handle.lower())
        if t:
            stats_line = f"   <b>{escape(t)}</b>-tier\n"

    # Optional promised_ts line in Russian
    promised_line = ""
    promised_text = _format_promised_ru(item.promised_timestamp_iso)
    if promised_text:
        promised_line = f"🕐 <b>{escape(promised_text)}</b>\n"

    # Title first — scan by project, not by short_id/handle
    return (
        f"<b>{escape(title)}</b> · @{escape(handle)}{eng_str}{xref}{dup_badge}{tag} · "
        f"<a href=\"{escape(url)}\">↗</a>\n"
        f"{stats_line}"
        f"{promised_line}"
        f"<i>{escape(notes)}</i>"
    )


def build_digest_markdown(
    items: list[ClassifiedItem],
    tick_id: int,
    cron_label: str = "scheduled",
    tier_map: dict[str, str | None] | None = None,
    stats_map: dict[str, dict] | None = None,
    meta_tldr: str | None = None,
) -> str:
    """
    Compose full digest HTML (parse_mode=HTML in Telegram).

    cron_label: 'scheduled' for cron-tick, 'on-demand' for /digest invocations.
    meta_tldr: optional 2-3 sentence RU preamble (see services/ct_digest/meta_tldr.py).
    """
    # Dedup by project + hard cap (target: 1 TG message). paid_hype out of body.
    selected, dup_meta = _select_for_digest(items, max_total=MAX_TOTAL_ITEMS)
    bucketed: dict[str, list[ClassifiedItem]] = {b: [] for b in BUCKET_ORDER}
    for it in selected:
        if it.bucket in bucketed:
            bucketed[it.bucket].append(it)
    for bucket in BUCKET_ORDER:
        bucketed[bucket] = sorted(bucketed[bucket], key=_rank)

    final_count = sum(len(v) for v in bucketed.values())
    header = (
        f"🪙 <b>CT Alpha Digest</b> · tick #{tick_id} · {cron_label}\n"
        f"{final_count} items"
    )

    sections: list[str] = []
    if meta_tldr:
        sections.append(f"<blockquote>🎯 <b>Meta now:</b> {escape(meta_tldr)}</blockquote>")
    sections.append(header)
    for bucket in BUCKET_ORDER:
        if not bucketed[bucket]:
            continue
        section_header = BUCKET_HEADERS[bucket]
        lines = [
            _format_item_line(it, tier_map=tier_map, stats_map=stats_map,
                              dup=dup_meta.get(it.tweet_id))
            for it in bucketed[bucket]
        ]
        sections.append(f"\n{section_header}\n" + "\n\n".join(lines))

    sections.append(
        "\n💬 reply на этот пост = заметка (LLM парсит). Кнопки = aggregate signal."
    )
    return "\n".join(sections)


def build_cross_post_alert(item: ClassifiedItem) -> str:
    """Small thin alert для EARLY topic — только cross-validated items."""
    handle = item.raw.author_handle or "?"
    notes = (item.mechanic_notes or "")[:160]
    url = item.raw.url or ""

    refs = []
    cr = item.cross_refs or {}
    if cr.get("twitter_watchlist"):
        refs.append("Twitter watchlist")
    if cr.get("catchmint"):
        refs.append("CatchMint alerts")
    if cr.get("mirror_meme"):
        refs.append("Mirror meme")

    return (
        f"🔭 <b>CT cross-validated</b> · @{escape(handle)}\n"
        f"<i>{escape(notes)}</i>\n"
        f"Matches: {escape(', '.join(refs))}\n"
        f"<a href=\"{escape(url)}\">tweet</a>"
    )
