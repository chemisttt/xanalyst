"""
Burst detection logic для catchmint /timeseries/mints/overview/?window=<sec> (spec 003).

После Gate E v2 (2026-05-17): catchmint endpoint принимает `?window=<sec>` и возвращает
totalCounts = mints за указанный window напрямую. Никакого median/ratio не нужно —
просто totalCounts >= порог.

Чистая функция без I/O.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BurstDecision:
    fires: bool
    reason: str       # 'ok' | 'no_data' | 'below_min' | 'not_verified'
                      # | 'simulation_failed' | 'near_sold_out'
    mints_in_window: int    # totalCounts = mints за catchmint_window_sec (default 600s = 10мин)


def evaluate_collection(item: dict, cfg) -> BurstDecision:
    """
    item — одна row из overview/?window=<sec>. cfg — Settings-like объект.
    Использует item['totalCounts'] напрямую — это и есть mints за окно (catchmint считает).
    """
    total = item.get("totalCounts")
    if total is None:
        return BurstDecision(False, "no_data", 0)
    total = int(total)

    if total < cfg.catchmint_min_mints_in_window:
        return BurstDecision(False, "below_min", total)

    if cfg.catchmint_require_verified and not item.get("isVerified"):
        return BurstDecision(False, "not_verified", total)

    if cfg.catchmint_require_simulation_pass and not item.get("simulationPassed"):
        return BurstDecision(False, "simulation_failed", total)

    # C2: overview maxSupply scalar (defensive vs detail array)
    max_s = item.get("maxSupply")
    if isinstance(max_s, list):
        max_s = max_s[0].get("supply") if max_s else None
    tot_s = item.get("totalSupply") or 0
    if (
        max_s
        and isinstance(max_s, int)
        and max_s > 0
        and (tot_s / max_s) > cfg.catchmint_max_supply_fraction
    ):
        return BurstDecision(False, "near_sold_out", total)

    return BurstDecision(True, "ok", total)
