from __future__ import annotations

from datetime import date
from typing import Any


def action_factors(action: dict[str, Any]) -> tuple[float, float]:
    numerator = float(action.get("ratio_numerator") or 1)
    denominator = float(action.get("ratio_denominator") or 1)
    if numerator <= 0 or denominator <= 0:
        return 1.0, 1.0
    action_type = str(action.get("action_type") or "").lower()
    if action_type in {"split", "bonus"}:
        price_factor = denominator / numerator
        volume_factor = numerator / denominator
        return price_factor, volume_factor
    if action_type in {"reverse_split", "consolidation"}:
        price_factor = numerator / denominator
        volume_factor = denominator / numerator
        return price_factor, volume_factor
    return float(action.get("price_adjustment_factor") or 1), float(action.get("volume_adjustment_factor") or 1)


def adjust_ohlcv_for_actions(bars: list[dict[str, Any]], actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return split/bonus adjusted historical bars.

    For an action on ex_date, bars before that date are adjusted. This prevents false
    80 percent gap alerts and broken moving averages after splits/bonuses.
    """
    if not bars or not actions:
        return bars
    parsed_actions: list[tuple[date, float, float]] = []
    for action in actions:
        raw_date = action.get("ex_date")
        try:
            ex_date = raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            continue
        price_factor, volume_factor = action_factors(action)
        parsed_actions.append((ex_date, price_factor, volume_factor))
    if not parsed_actions:
        return bars
    adjusted: list[dict[str, Any]] = []
    for bar in bars:
        try:
            bar_date = date.fromisoformat(str(bar.get("datetime") or bar.get("date"))[:10])
        except ValueError:
            adjusted.append(dict(bar))
            continue
        price_factor = 1.0
        volume_factor = 1.0
        for ex_date, pf, vf in parsed_actions:
            if bar_date < ex_date:
                price_factor *= pf
                volume_factor *= vf
        clean = dict(bar)
        for key in ("open", "high", "low", "close"):
            if clean.get(key) is not None:
                clean[key] = round(float(clean[key]) * price_factor, 4)
        if clean.get("volume") is not None:
            clean["volume"] = int(float(clean["volume"]) * volume_factor)
        adjusted.append(clean)
    return adjusted
