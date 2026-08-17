"""
Red-flag severity scorer для catchmint enrichment (spec 003 Task 2.5).

Чистая функция — берёт detail (из GET /contracts/{addr}/) + flags (из GET /contracts/{addr}/flags/)
и cfg, возвращает SafetyVerdict.

Flag enum (из JS bundle): SCAM, DRAIN, COPY, HONEYPOT, OTHER.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

FLAG_LABELS = {"SCAM", "DRAIN", "COPY", "HONEYPOT", "OTHER"}


@dataclass
class SafetyVerdict:
    severity: str                  # 'safe' | 'warn' | 'danger'
    skip: bool                     # True → не emit'ить алерт вообще
    reasons: list[str] = field(default_factory=list)  # ['HONEYPOT_flag_3', 'fresh_deploy_8m', ...]
    badges: list[str] = field(default_factory=list)   # ['⚠ SCAM flag (2)', '🆕 fresh 8m', ...]


def _escalate(current: str, new: str) -> str:
    """Severity progression: safe → warn → danger (одностороннее)."""
    order = {"safe": 0, "warn": 1, "danger": 2}
    return new if order[new] > order[current] else current


def evaluate_safety(
    detail: dict,
    flags: list[dict],
    cfg,
    now: Optional[datetime] = None,
) -> SafetyVerdict:
    """
    detail — payload из GET /contracts/{addr}/
    flags  — payload из GET /contracts/{addr}/flags/, list[{"label": str, "count": int}]
    cfg    — Settings-like объект с catchmint_skip_on_* и catchmint_warn_on_* атрибутами
    now    — для testability; default datetime.now(UTC)
    """
    now = now or datetime.now(timezone.utc)
    verdict = SafetyVerdict(severity="safe", skip=False)

    # 1. Flag labels — bucket by label (uppercase)
    flag_map: dict[str, int] = {}
    for f in flags or []:
        if not isinstance(f, dict):
            continue
        lbl = (f.get("label") or "").upper()
        cnt = f.get("count") or 0
        if lbl in FLAG_LABELS:
            flag_map[lbl] = cnt
        # unknown labels — игнорируются (не падаем)

    if "HONEYPOT" in flag_map and cfg.catchmint_skip_on_honeypot:
        verdict.skip = True
        verdict.reasons.append(f"HONEYPOT_flag_{flag_map['HONEYPOT']}")
        verdict.severity = _escalate(verdict.severity, "danger")

    if "DRAIN" in flag_map and cfg.catchmint_skip_on_drain:
        verdict.skip = True
        verdict.reasons.append(f"DRAIN_flag_{flag_map['DRAIN']}")
        verdict.severity = _escalate(verdict.severity, "danger")

    if "SCAM" in flag_map:
        cnt = flag_map["SCAM"]
        if cfg.catchmint_skip_on_scam:
            verdict.skip = True
            verdict.reasons.append(f"SCAM_flag_{cnt}")
            verdict.severity = _escalate(verdict.severity, "danger")
        else:
            verdict.badges.append(f"⚠ SCAM flag ({cnt})")
            verdict.severity = _escalate(verdict.severity, "warn")

    if "COPY" in flag_map:
        verdict.badges.append(f"⚠ COPY/stolen ({flag_map['COPY']})")
        verdict.severity = _escalate(verdict.severity, "warn")

    if "OTHER" in flag_map:
        verdict.badges.append(f"⚠ flagged ({flag_map['OTHER']})")
        verdict.severity = _escalate(verdict.severity, "warn")

    # 2. Notable flags — high-rep accounts
    nf = detail.get("notableFlagCount") or 0
    if nf > 0:
        if cfg.catchmint_skip_on_notable_flag:
            verdict.skip = True
            verdict.reasons.append(f"notable_flag_{nf}")
            verdict.severity = _escalate(verdict.severity, "danger")
        else:
            verdict.badges.append(f"🚩 notable flag ({nf})")
            verdict.severity = _escalate(verdict.severity, "warn")

    # 3. Hide ratio — community downvoting
    hc = detail.get("hideCount") or 0
    uw = detail.get("uniqueWallets") or 0
    if uw >= 50 and hc / max(uw, 1) > cfg.catchmint_skip_on_hide_ratio:
        verdict.skip = True
        verdict.reasons.append(f"hide_ratio_{hc}/{uw}")
        verdict.severity = _escalate(verdict.severity, "warn")

    # 4. Fresh deploy → warn (не skip)
    deployed = detail.get("deployedAt")
    if deployed and isinstance(deployed, str):
        try:
            dt = datetime.fromisoformat(deployed.replace("Z", "+00:00"))
            # Normalize to same kind as `now` (tz-aware OR tz-naive). Avoid subtract-mismatch.
            if (now.tzinfo is None) and (dt.tzinfo is not None):
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            elif (now.tzinfo is not None) and (dt.tzinfo is None):
                dt = dt.replace(tzinfo=timezone.utc)
            mins = (now - dt).total_seconds() / 60
            if mins < cfg.catchmint_warn_on_fresh_deploy_minutes:
                verdict.badges.append(f"🆕 fresh {int(mins)}m")
                verdict.severity = _escalate(verdict.severity, "warn")
        except (ValueError, TypeError):
            pass

    # 5. Proxy → warn (upgradeable, deployer может поменять логику)
    if cfg.catchmint_warn_on_proxy and detail.get("isProxy"):
        verdict.badges.append("🔄 proxy")
        verdict.severity = _escalate(verdict.severity, "warn")

    return verdict
