from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import mean, median, stdev
from typing import Any

from portfolio_manager import portfolio_risk_audit
from regime_filter import regime_from_breadth

try:
    import numpy as np
except ImportError:  # pragma: no cover - optional in zero-install local checks
    np = None


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def score_between(value: float | None, poor: float, excellent: float) -> float:
    if value is None:
        return 50.0
    if excellent == poor:
        return 50.0
    score = (value - poor) / (excellent - poor) * 100
    return clamp(score)


def score_inverse(value: float | None, excellent: float, poor: float) -> float:
    if value is None:
        return 50.0
    if poor == excellent:
        return 50.0
    score = (poor - value) / (poor - excellent) * 100
    return clamp(score)


def to_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def sector_relative_score(value: float | None, sector_median: float | None, higher_is_better: bool = True) -> float:
    if value is None or sector_median in {None, 0}:
        return 50.0
    ratio = float(value) / max(float(sector_median), 0.0001)
    if higher_is_better:
        return clamp(50 + (ratio - 1) * 45)
    return clamp(50 + (1 - ratio) * 45)


def sector_kind(fundamentals: dict[str, Any]) -> str:
    label = f"{fundamentals.get('sector', '')} {fundamentals.get('industry', '')}".lower()
    if any(word in label for word in ["bank", "nbfc", "financial", "finance", "insurance", "asset management"]):
        return "financial"
    if any(word in label for word in ["power", "infra", "cement", "steel", "metal", "utility", "capital goods", "manufacturing"]):
        return "capital_intensive"
    if any(word in label for word in ["technology", "it", "software", "consumer", "fmcg", "pharma"]):
        return "asset_light"
    return "general"


def promoter_pledge_score(pledge_percent: float) -> float:
    if pledge_percent <= 0:
        return 100
    if pledge_percent <= 10:
        return 92 - pledge_percent * 0.8
    if pledge_percent <= 30:
        return 82 - (pledge_percent - 10) * 1.6
    return max(20, 50 - (pledge_percent - 30) * 1.2)


def capital_efficiency_score(fundamentals: dict[str, Any], roce_score: float, roe_score: float) -> dict[str, Any]:
    kind = sector_kind(fundamentals)
    roa = to_float(fundamentals.get("roa"))
    nim = to_float(fundamentals.get("nim"))
    roa_score = score_between(roa, 0.6, 2.0) if roa is not None else 50
    nim_score = score_between(nim, 2.5, 5.5) if nim is not None else 50
    if kind == "financial":
        score = roe_score * 0.45 + roa_score * 0.35 + nim_score * 0.20
        method = "financial_roe_roa_nim"
    elif kind == "capital_intensive":
        score = roce_score * 0.72 + roe_score * 0.28
        method = "capital_intensive_roce_heavy"
    elif kind == "asset_light":
        score = roce_score * 0.58 + roe_score * 0.42
        method = "asset_light_balanced"
    else:
        score = roce_score * 0.65 + roe_score * 0.35
        method = "general_roce_preferred"
    return {"score": clamp(score), "method": method, "sector_kind": kind, "roa_score": round(roa_score, 2), "nim_score": round(nim_score, 2)}


def valuation_sanity_score(fundamentals: dict[str, Any], medians: dict[str, Any]) -> float:
    sector = sector_kind(fundamentals)
    pe = to_float(fundamentals.get("forward_pe") or fundamentals.get("pe"))
    growth = to_float(fundamentals.get("forward_profit_growth") or fundamentals.get("profit_cagr"), 0) or 0
    if sector == "financial" and fundamentals.get("pb") is not None:
        pb = to_float(fundamentals.get("pb"))
        return sector_relative_score(pb, medians.get("pb"), False) * 0.65 + score_inverse(pb, 1.0, 4.0) * 0.35
    if pe is None or growth <= 0:
        return 50
    peg = pe / max(growth, 1)
    ideal = 2.2 if growth >= 30 else 1.8 if growth >= 20 else 1.4 if growth >= 12 else 1.1
    peg_score = clamp(100 - abs(peg - ideal) * 22)
    sector_score = sector_relative_score(pe, medians.get("forward_pe") or medians.get("pe"), False)
    high_pe_penalty = max(0, pe - 80) * 0.35
    return clamp(peg_score * 0.60 + sector_score * 0.40 - high_pe_penalty)


def weighted_score(parts: dict[str, tuple[float, float]]) -> tuple[int, dict[str, dict[str, float]]]:
    total = 0.0
    max_total = 0.0
    breakdown: dict[str, dict[str, float]] = {}
    for key, (score, weight) in parts.items():
        clean = clamp(score)
        contribution = clean / 100 * weight
        total += contribution
        max_total += weight
        breakdown[key] = {
            "score": round(clean, 2),
            "weight": weight,
            "points": round(contribution, 2),
        }
    final = round(total / max_total * 100) if max_total else 0
    return int(clamp(final)), breakdown


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    multiplier = 2 / (period + 1)
    result = [values[0]]
    for value in values[1:]:
        result.append((value - result[-1]) * multiplier + result[-1])
    return result


def sma(values: list[float], period: int) -> list[float]:
    result: list[float] = []
    for idx in range(len(values)):
        start = max(0, idx - period + 1)
        result.append(mean(values[start : idx + 1]))
    return result


def rsi(values: list[float], period: int = 14) -> float:
    if len(values) < max(period * 2, 28):
        return 50.0
    gains: list[float] = []
    losses: list[float] = []
    for prev, current in zip(values[:-1], values[1:]):
        diff = current - prev
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))

    avg_gain = mean(gains[:period])
    avg_loss = mean(losses[:period])
    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs_value = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs_value)), 2)


def macd(values: list[float]) -> dict[str, float | str]:
    if len(values) < 35:
        return {"macd": 0.0, "signal": 0.0, "histogram": 0.0, "state": "Neutral"}
    fast = ema(values, 12)
    slow = ema(values, 26)
    macd_line = [a - b for a, b in zip(fast[-len(slow) :], slow)]
    signal_line = ema(macd_line, 9)
    hist = macd_line[-1] - signal_line[-1]
    state = "Bullish" if hist > 0 and macd_line[-1] > 0 else "Bearish" if hist < 0 else "Neutral"
    return {
        "macd": round(macd_line[-1], 2),
        "signal": round(signal_line[-1], 2),
        "histogram": round(hist, 2),
        "state": state,
    }


def stochastic(bars: list[dict[str, Any]], period: int = 14, smooth: int = 3) -> dict[str, float | str]:
    if len(bars) < period + smooth:
        return {"k": 50.0, "d": 50.0, "state": "Neutral"}
    k_values: list[float] = []
    for idx in range(period - 1, len(bars)):
        window = bars[idx - period + 1 : idx + 1]
        low = min(float(bar["low"]) for bar in window)
        high = max(float(bar["high"]) for bar in window)
        close = float(bars[idx]["close"])
        k_values.append(50.0 if high == low else (close - low) / (high - low) * 100)
    k = k_values[-1]
    d = mean(k_values[-smooth:]) if len(k_values) >= smooth else k
    state = "Bullish" if k > d and 35 <= k <= 85 else "Overbought" if k > 85 else "Weak" if k < 25 else "Neutral"
    return {"k": round(k, 2), "d": round(d, 2), "state": state}


def atr(bars: list[dict[str, Any]], period: int = 14) -> float:
    if len(bars) < 2:
        return 0.0
    ranges: list[float] = []
    for prev, bar in zip(bars[:-1], bars[1:]):
        high = float(bar["high"])
        low = float(bar["low"])
        prev_close = float(prev["close"])
        ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    if not ranges:
        return 0.0
    return round(mean(ranges[-period:]), 2)


def obv(values: list[float], volumes: list[float]) -> list[float]:
    if not values or not volumes:
        return []
    result = [0.0]
    for prev, current, volume in zip(values[:-1], values[1:], volumes[1:]):
        if current > prev:
            result.append(result[-1] + volume)
        elif current < prev:
            result.append(result[-1] - volume)
        else:
            result.append(result[-1])
    return result


def obv_slope_score(values: list[float], volumes: list[float], lookback: int = 20) -> dict[str, Any]:
    line = obv(values, volumes)
    if len(line) < lookback + 1:
        return {"score": 50, "state": "Insufficient history", "slope_pct": 0}
    start = line[-lookback - 1]
    end = line[-1]
    volume_scale = mean([abs(item) for item in volumes[-lookback:]]) * lookback if volumes[-lookback:] else 1
    denom = max(volume_scale, 1)
    slope_pct = (end - start) / denom * 100
    score = clamp(50 + slope_pct * 2.5)
    state = "Accumulation" if score >= 62 else "Distribution" if score <= 38 else "Neutral"
    return {"score": round(score, 2), "state": state, "slope_pct": round(slope_pct, 2)}


def latest_close(bars: list[dict[str, Any]]) -> float:
    return float(bars[-1]["close"]) if bars else 0.0


def pct_distance(value: float, base: float) -> float:
    if base == 0:
        return 0.0
    return (value - base) / base * 100


def calculate_rubber_band_penalty(current_price: float, sma_200: float) -> dict[str, Any]:
    if sma_200 <= 0 or current_price <= 0:
        return {"status": "ERROR_NO_DATA", "multiplier": 0.0, "ratio": 0.0, "extension_pct": None}
    ratio = current_price / sma_200
    extension_pct = (ratio - 1) * 100
    if ratio >= 1.50:
        status = "REJECTED_OVEREXTENDED"
        multiplier = 0.0
        message = "Price is more than 50 percent above the 200 DMA; mean-reversion risk is too high."
    elif ratio >= 1.35:
        status = "CAUTION_LATE_STAGE"
        multiplier = 0.5
        message = "Price is 35-50 percent above the 200 DMA; position size is cut."
    elif ratio >= 1.05:
        status = "IDEAL_POCKET"
        multiplier = 1.0
        message = "Trend is established without severe overextension."
    else:
        status = "EARLY_STAGE_CAUTION"
        multiplier = 0.8
        message = "Price is near or below the 200 DMA; momentum is still proving itself."
    return {
        "status": status,
        "multiplier": multiplier,
        "ratio": round(ratio, 3),
        "extension_pct": round(extension_pct, 2),
        "message": message,
    }


def parse_bar_datetime(value: Any) -> datetime:
    raw = str(value)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        parsed = datetime.strptime(raw[:10], "%Y-%m-%d")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def remove_weekend_bars(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clean: list[dict[str, Any]] = []
    for bar in bars:
        try:
            if parse_bar_datetime(bar["datetime"]).weekday() < 5:
                clean.append(bar)
        except (KeyError, ValueError):
            clean.append(bar)
    return clean or bars


def resample_to_weekly(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    ordered = sorted(remove_weekend_bars(bars), key=lambda bar: parse_bar_datetime(bar["datetime"]))
    for bar in ordered:
        stamp = parse_bar_datetime(bar["datetime"])
        iso_year, iso_week, _ = stamp.isocalendar()
        grouped.setdefault((iso_year, iso_week), []).append(bar)

    weekly: list[dict[str, Any]] = []
    for _, week_bars in sorted(grouped.items()):
        weekly.append(
            {
                "datetime": week_bars[-1]["datetime"],
                "open": float(week_bars[0]["open"]),
                "high": max(float(bar["high"]) for bar in week_bars),
                "low": min(float(bar["low"]) for bar in week_bars),
                "close": float(week_bars[-1]["close"]),
                "volume": int(sum(float(bar.get("volume", 0)) for bar in week_bars)),
            }
        )
    return weekly


def business_data_quality(fundamentals: dict[str, Any]) -> dict[str, Any]:
    required = [
        "sales_cagr",
        "profit_cagr",
        "roce",
        "roe",
        "debt_equity",
        "cfo_pat",
        "fcf_trend",
        "promoter_holding_trend",
        "pledge_percent",
        "margin_trend_bps",
        "pe",
    ]
    missing = [field for field in required if fundamentals.get(field) is None]
    completeness = round((len(required) - len(missing)) / len(required) * 100, 1)
    return {
        "completeness_pct": completeness,
        "missing_fields": missing,
        "warning": "Data incomplete; neutral defaults were used" if missing else None,
    }


def days_until_date(raw: Any) -> int | None:
    if not raw:
        return None
    try:
        parsed = parse_bar_datetime(raw)
    except (ValueError, TypeError):
        return None
    return (parsed.date() - datetime.now(timezone.utc).date()).days


def altman_z_score(fundamentals: dict[str, Any]) -> float | None:
    direct = to_float(fundamentals.get("altman_z_score"))
    if direct is not None:
        return direct
    total_assets = to_float(fundamentals.get("total_assets"))
    total_liabilities = to_float(fundamentals.get("total_liabilities"))
    working_capital = to_float(fundamentals.get("working_capital"))
    retained_earnings = to_float(fundamentals.get("retained_earnings"))
    ebit = to_float(fundamentals.get("ebit"))
    market_value_equity = to_float(fundamentals.get("market_value_equity"))
    sales = to_float(fundamentals.get("sales"))
    if not all(value is not None for value in [total_assets, total_liabilities, working_capital, retained_earnings, ebit, market_value_equity, sales]):
        return None
    if not total_assets or not total_liabilities:
        return None
    return round(
        1.2 * working_capital / total_assets
        + 1.4 * retained_earnings / total_assets
        + 3.3 * ebit / total_assets
        + 0.6 * market_value_equity / total_liabilities
        + 1.0 * sales / total_assets,
        3,
    )


def piotroski_f_score(fundamentals: dict[str, Any]) -> int | None:
    direct = fundamentals.get("piotroski_f_score")
    if direct is not None:
        return int(to_float(direct, 0) or 0)
    fields = {
        "roa": to_float(fundamentals.get("roa")),
        "cfo": to_float(fundamentals.get("cfo")),
        "delta_roa": to_float(fundamentals.get("delta_roa")),
        "accrual_quality": to_float(fundamentals.get("cfo_roa_spread")),
        "delta_leverage": to_float(fundamentals.get("delta_leverage")),
        "delta_current_ratio": to_float(fundamentals.get("delta_current_ratio")),
        "shares_issued": fundamentals.get("shares_issued"),
        "delta_gross_margin": to_float(fundamentals.get("delta_gross_margin")),
        "delta_asset_turnover": to_float(fundamentals.get("delta_asset_turnover")),
    }
    if sum(value is not None for value in fields.values()) < 6:
        return None
    score = 0
    score += int((fields["roa"] or 0) > 0)
    score += int((fields["cfo"] or 0) > 0)
    score += int((fields["delta_roa"] or 0) > 0)
    score += int((fields["accrual_quality"] or 0) > 0)
    score += int((fields["delta_leverage"] or 0) < 0)
    score += int((fields["delta_current_ratio"] or 0) > 0)
    score += int(not bool(fields["shares_issued"]))
    score += int((fields["delta_gross_margin"] or 0) > 0)
    score += int((fields["delta_asset_turnover"] or 0) > 0)
    return score


def forensic_earnings_quality(fundamentals: dict[str, Any]) -> dict[str, Any]:
    net_income = to_float(fundamentals.get("net_income") or fundamentals.get("pat"))
    cfo = to_float(fundamentals.get("operating_cash_flow") or fundamentals.get("cfo"))
    cfi = to_float(fundamentals.get("cash_flow_investing") or fundamentals.get("cfi"), 0)
    avg_assets = to_float(fundamentals.get("average_total_assets"))
    sloan_ratio = None
    if net_income is not None and cfo is not None and cfi is not None and avg_assets:
        sloan_ratio = (net_income - cfo - cfi) / avg_assets

    ebitda = to_float(fundamentals.get("ebitda"))
    cfo_ebitda = None if cfo is None or not ebitda else cfo / ebitda
    cfo_ebitda_history_raw = fundamentals.get("cfo_ebitda_history") or fundamentals.get("ocf_ebitda_history") or []
    if isinstance(cfo_ebitda_history_raw, str):
        cfo_ebitda_history = [to_float(item.strip()) for item in cfo_ebitda_history_raw.split(",")]
    elif isinstance(cfo_ebitda_history_raw, (list, tuple)):
        cfo_ebitda_history = [to_float(item) for item in cfo_ebitda_history_raw]
    else:
        cfo_ebitda_history = []
    cfo_ebitda_history = [item for item in cfo_ebitda_history if item is not None]
    if not cfo_ebitda_history:
        prior_ratio = to_float(fundamentals.get("previous_cfo_ebitda") or fundamentals.get("cfo_ebitda_previous_year"))
        latest_ratio = to_float(fundamentals.get("cfo_ebitda") or cfo_ebitda)
        cfo_ebitda_history = [item for item in [prior_ratio, latest_ratio] if item is not None]
    two_year_cfo_ebitda_fail = len(cfo_ebitda_history) >= 2 and all(item < 0.60 for item in cfo_ebitda_history[-2:])

    revenue_growth = to_float(fundamentals.get("revenue_growth") or fundamentals.get("sales_cagr"))
    receivables_growth = to_float(fundamentals.get("receivables_growth"))
    receivable_divergence = None if revenue_growth is None or receivables_growth is None else receivables_growth - revenue_growth

    ccc = to_float(fundamentals.get("cash_conversion_cycle_days") or fundamentals.get("ccc_days"))
    prior_ccc = to_float(fundamentals.get("previous_cash_conversion_cycle_days") or fundamentals.get("previous_ccc_days"))
    ccc_change = None if ccc is None or prior_ccc is None else ccc - prior_ccc

    altman = altman_z_score(fundamentals)
    piotroski = piotroski_f_score(fundamentals)
    beneish = to_float(fundamentals.get("beneish_m_score"))

    metric_scores = {
        "sloan_ratio": score_inverse(sloan_ratio, -0.05, 0.18) if sloan_ratio is not None else 50,
        "cfo_to_ebitda": score_between(cfo_ebitda, 0.55, 1.05) if cfo_ebitda is not None else 50,
        "working_capital_divergence": score_inverse(receivable_divergence, 0, 35) if receivable_divergence is not None else 50,
        "cash_conversion_cycle": score_inverse(ccc_change, -8, 35) if ccc_change is not None else 50,
        "altman_z": score_between(altman, 1.8, 4.0) if altman is not None else 50,
        "piotroski_f": score_between(piotroski, 3, 8) if piotroski is not None else 50,
        "beneish_m": score_inverse(beneish, -2.4, -1.2) if beneish is not None else 50,
    }
    total, breakdown = weighted_score(
        {
            "sloan_ratio": (metric_scores["sloan_ratio"], 18),
            "cfo_to_ebitda": (metric_scores["cfo_to_ebitda"], 16),
            "working_capital_divergence": (metric_scores["working_capital_divergence"], 16),
            "cash_conversion_cycle": (metric_scores["cash_conversion_cycle"], 10),
            "altman_z": (metric_scores["altman_z"], 14),
            "piotroski_f": (metric_scores["piotroski_f"], 16),
            "beneish_m": (metric_scores["beneish_m"], 10),
        }
    )
    flags: list[str] = []
    hard_fail = False
    if sloan_ratio is not None and sloan_ratio > 0.12:
        flags.append("high_sloan_accrual_ratio")
        hard_fail = True
    if cfo_ebitda is not None and cfo_ebitda < 0.8:
        flags.append("cfo_below_80pct_of_ebitda")
    if two_year_cfo_ebitda_fail:
        flags.append("cfo_ebitda_below_60pct_two_years")
    if receivable_divergence is not None and receivable_divergence > 20:
        flags.append("receivables_growing_much_faster_than_sales")
    if ccc_change is not None and ccc_change > 20:
        flags.append("cash_conversion_cycle_deteriorating")
    if altman is not None and altman < 1.8:
        flags.append("altman_distress_zone")
        hard_fail = True
    if piotroski is not None and piotroski < 7:
        flags.append("piotroski_below_7")
    if piotroski is not None and piotroski < 4:
        flags.append("piotroski_below_4")
        hard_fail = True
    if beneish is not None and beneish > -1.78:
        flags.append("beneish_manipulation_risk")
        hard_fail = True
    return {
        "score": total,
        "breakdown": breakdown,
        "flags": flags,
        "hard_fail": hard_fail,
        "metrics": {
            "sloan_ratio": None if sloan_ratio is None else round(sloan_ratio, 4),
            "cfo_to_ebitda": None if cfo_ebitda is None else round(cfo_ebitda, 3),
            "cfo_ebitda_history": [round(item, 3) for item in cfo_ebitda_history],
            "two_year_cfo_ebitda_fail": two_year_cfo_ebitda_fail,
            "receivable_growth_minus_revenue_growth": None if receivable_divergence is None else round(receivable_divergence, 2),
            "cash_conversion_cycle_change_days": None if ccc_change is None else round(ccc_change, 2),
            "altman_z_score": altman,
            "piotroski_f_score": piotroski,
            "beneish_m_score": beneish,
        },
    }


def business_quality_score(fundamentals: dict[str, Any]) -> dict[str, Any]:
    medians = fundamentals.get("sector_medians") or {}
    revenue_abs = score_between(fundamentals.get("sales_cagr"), 0, 22)
    profit_abs = score_between(fundamentals.get("profit_cagr"), 0, 28)
    roce_abs = score_between(fundamentals.get("roce"), 8, 28)
    roe_abs = score_between(fundamentals.get("roe"), 8, 25)
    debt_abs = score_inverse(fundamentals.get("debt_equity"), 0.0, 1.8)
    revenue_score = revenue_abs * 0.65 + sector_relative_score(to_float(fundamentals.get("sales_cagr")), medians.get("sales_cagr"), True) * 0.35
    profit_score = profit_abs * 0.65 + sector_relative_score(to_float(fundamentals.get("profit_cagr")), medians.get("profit_cagr"), True) * 0.35
    roce_score = roce_abs * 0.6 + sector_relative_score(to_float(fundamentals.get("roce")), medians.get("roce"), True) * 0.4
    roe_score = roe_abs * 0.6 + sector_relative_score(to_float(fundamentals.get("roe")), medians.get("roe"), True) * 0.4
    debt_score = debt_abs * 0.7 + sector_relative_score(to_float(fundamentals.get("debt_equity")), medians.get("debt_equity"), False) * 0.3
    cfo_score = score_between(fundamentals.get("cfo_pat"), 0.45, 1.05)
    fcf_map = {"positive": 100, "improving": 82, "volatile": 55, "negative": 20}
    fcf_score = fcf_map.get(str(fundamentals.get("fcf_trend", "")).lower(), 45)
    promoter_trend = str(fundamentals.get("promoter_holding_trend", "stable")).lower()
    promoter_score = {"rising": 100, "stable": 85, "flat": 78, "falling": 35}.get(promoter_trend, 65)
    promoter_score = min(promoter_score, promoter_pledge_score(float(fundamentals.get("pledge_percent", 0) or 0)))
    if fundamentals.get("dilution_flag"):
        promoter_score -= 20
    margin_score = score_between(fundamentals.get("margin_trend_bps"), -250, 350)
    capital_efficiency = capital_efficiency_score(fundamentals, roce_score, roe_score)
    valuation_score = valuation_sanity_score(fundamentals, medians)
    forensic = forensic_earnings_quality(fundamentals)

    total, breakdown = weighted_score(
        {
            "revenue_growth": (revenue_score, 12),
            "profit_growth": (profit_score, 12),
            "capital_efficiency": (capital_efficiency["score"], 13),
            "debt_profile": (debt_score, 9),
            "cash_flow_quality": ((cfo_score * 0.65) + (fcf_score * 0.35), 12),
            "forensic_earnings_quality": (forensic["score"], 15),
            "promoter_pledge_dilution": (promoter_score, 12),
            "margin_trend": (margin_score, 10),
            "valuation_sanity": (valuation_score, 5),
        }
    )
    statutory_audit_penalty = 0.0
    if "cfo_ebitda_below_60pct_two_years" in forensic.get("flags", []):
        statutory_audit_penalty = 0.50
        total = int(clamp(total * (1 - statutory_audit_penalty)))
    return {
        "score": total,
        "breakdown": breakdown,
        "statutory_audit_penalty": statutory_audit_penalty,
        "valuation_score": int(round(valuation_score)),
        "data_quality": business_data_quality(fundamentals),
        "forensic_quality": forensic,
        "capital_efficiency": capital_efficiency,
        "sector_medians": medians,
    }


def sector_tailwind_score(tailwind: dict[str, Any]) -> dict[str, Any]:
    as_of_raw = tailwind.get("tailwind_as_of") or tailwind.get("as_of") or tailwind.get("updated_at")
    age_days = None
    staleness_factor = 1.0
    if as_of_raw:
        try:
            age_days = max(0, (datetime.now(timezone.utc) - parse_bar_datetime(as_of_raw)).days)
            if age_days > 90:
                staleness_factor = max(0.55, math.exp(-(age_days - 90) / 180))
        except (ValueError, TypeError):
            age_days = None
    sector_momentum = float(tailwind.get("sector_momentum", 50) or 50)
    rotation_score = tailwind.get("sector_rotation_score")
    if rotation_score is not None:
        sector_momentum = sector_momentum * 0.55 + float(rotation_score) * 0.45
    def decay(value: Any) -> float:
        raw = float(value if value is not None else 50)
        return 50 + (raw - 50) * staleness_factor
    total, breakdown = weighted_score(
        {
            "demand_trend": (decay(tailwind.get("demand_trend", 50)), 30),
            "policy_support": (decay(tailwind.get("policy_support", 50)), 20),
            "cost_environment": (decay(tailwind.get("cost_environment", 50)), 15),
            "order_visibility": (decay(tailwind.get("order_visibility", 50)), 20),
            "sector_momentum": (decay(sector_momentum), 15),
        }
    )
    return {
        "score": total,
        "breakdown": breakdown,
        "sector_rotation_score": None if rotation_score is None else round(float(rotation_score), 2),
        "tailwind_as_of": as_of_raw,
        "tailwind_age_days": age_days,
        "staleness_factor": round(staleness_factor, 3),
        "stale_warning": "Tailwind inputs are stale and decayed toward neutral" if staleness_factor < 1 else None,
    }


SOURCE_RELIABILITY = {
    "exchange_filing": 1.0,
    "promoter_buying": 0.9,
    "company_ir": 0.92,
    "earnings_transcript": 0.88,
    "credit_rating": 0.84,
    "credible_news": 0.64,
    "sector_report": 0.58,
    "social": 0.25,
    "rumor": 0.15,
}


def parse_event_time(event: dict[str, Any], now: datetime) -> int:
    if event.get("days_old") is not None:
        return int(event["days_old"])
    raw = event.get("timestamp")
    if not raw:
        return 30
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0, (now - parsed).days)
    except ValueError:
        return 30


def event_freshness(days_old: int, timeframe: str) -> float:
    if timeframe == "monthly":
        if days_old <= 60:
            return 1.0
        return clamp(math.exp(-(days_old - 60) / 75), 0.25, 1.0)
    if days_old <= 7:
        return 1.0
    return clamp(math.exp(-(days_old - 7) / 21), 0.18, 1.0)


def score_single_event(event: dict[str, Any], now: datetime | None = None, timeframe: str = "weekly") -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    category = str(event.get("category", "")).lower()
    sentiment = float(event.get("sentiment", 0))
    importance_default = 80 if category == "promoter_buying" else 50
    importance = clamp(float(event.get("importance", importance_default)), 0, 100)
    days_old = parse_event_time(event, now)
    freshness = event_freshness(days_old, timeframe)
    source_type = "promoter_buying" if category == "promoter_buying" else str(event.get("source_type", "credible_news"))
    reliability = SOURCE_RELIABILITY.get(source_type, 0.45)
    if category == "promoter_buying":
        sentiment = max(sentiment, 0.68)
    impact = sentiment * freshness * reliability * importance
    return {
        **event,
        "days_old": days_old,
        "freshness": round(freshness, 3),
        "reliability": reliability,
        "net_score": round(impact, 2),
    }


def weighted_median(values: list[tuple[float, float]]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values, key=lambda item: item[0])
    total_weight = sum(max(weight, 0) for _, weight in ordered)
    if total_weight <= 0:
        return median([value for value, _ in ordered])
    running = 0.0
    for value, weight in ordered:
        running += max(weight, 0)
        if running >= total_weight / 2:
            return value
    return ordered[-1][0]


def event_strength_score(events: list[dict[str, Any]], timeframe: str = "weekly") -> dict[str, Any]:
    scored = [score_single_event(event, timeframe=timeframe) for event in events]
    if not scored:
        return {"score": 50, "events": [], "negative_governance": False, "timeframe": timeframe}
    sentiment_weights = [
        (
            float(event.get("sentiment", 0)),
            float(event.get("freshness", 0)) * float(event.get("reliability", 0)) * math.sqrt(max(float(event.get("importance", 50)), 1)),
        )
        for event in scored
    ]
    median_sentiment = weighted_median(sentiment_weights)
    damped_sum = math.tanh(sum(float(event.get("net_score", 0)) for event in scored) / 180)
    extreme_event = max(scored, key=lambda event: abs(float(event.get("net_score", 0))), default={})
    extreme_component = clamp(float(extreme_event.get("net_score", 0)) / 100, -0.45, 0.45)
    normalized = median_sentiment * 0.55 + damped_sum * 0.30 + extreme_component * 0.15
    score = int(round(clamp(50 + normalized * 45)))
    negative_governance = any(
        float(e.get("sentiment", 0)) < -0.5
        and str(e.get("category", "")).lower() in {"governance", "auditor", "pledge", "fraud", "management_exit"}
        for e in scored
    )
    return {"score": score, "events": scored, "negative_governance": negative_governance, "timeframe": timeframe}


def relative_strength(
    stock_bars: list[dict[str, Any]],
    benchmark_bars: list[dict[str, Any]] | None,
    lookback: int = 55,
) -> dict[str, Any]:
    if not benchmark_bars or len(stock_bars) < lookback or len(benchmark_bars) < lookback:
        return {"pct": 0.0, "state": "Unknown"}
    stock_return = pct_distance(float(stock_bars[-1]["close"]), float(stock_bars[-lookback]["close"]))
    benchmark_return = pct_distance(float(benchmark_bars[-1]["close"]), float(benchmark_bars[-lookback]["close"]))
    diff = stock_return - benchmark_return
    state = "Leadership" if diff >= 8 else "Positive" if diff >= 2 else "Neutral" if diff >= -2 else "Weak"
    return {"pct": round(diff, 2), "state": state}


def daily_returns(bars: list[dict[str, Any]]) -> list[float]:
    closes = [float(bar["close"]) for bar in bars if to_float(bar.get("close")) is not None]
    returns: list[float] = []
    for prev, current in zip(closes[:-1], closes[1:]):
        returns.append((current / prev - 1) if prev else 0.0)
    return returns


def idiosyncratic_momentum(
    stock_bars: list[dict[str, Any]],
    benchmark_bars: list[dict[str, Any]] | None,
    lookback: int = 252,
    annualization: int = 252,
) -> dict[str, Any]:
    if not benchmark_bars:
        return {"score": 0.0, "state": "Unknown", "alpha_annual": 0.0, "beta": None, "residual_vol": None}
    stock_returns = daily_returns(stock_bars)
    market_returns = daily_returns(benchmark_bars)
    count = min(len(stock_returns), len(market_returns), lookback)
    min_required = 40 if annualization >= 200 else 20
    if count < min_required:
        return {"score": 0.0, "state": "Insufficient history", "alpha_annual": 0.0, "beta": None, "residual_vol": None}
    y = stock_returns[-count:]
    x = market_returns[-count:]
    if np is not None:
        x_arr = np.asarray(x, dtype=float)
        y_arr = np.asarray(y, dtype=float)
        matrix = np.vstack([np.ones_like(x_arr), x_arr]).T
        alpha, beta = np.linalg.lstsq(matrix, y_arr, rcond=None)[0]
        residuals = y_arr - (alpha + beta * x_arr)
        residual_vol = float(np.std(residuals, ddof=1)) if len(residuals) > 1 else 0.0
    else:
        x_mean = mean(x)
        y_mean = mean(y)
        variance = sum((item - x_mean) ** 2 for item in x)
        beta = 0.0 if variance == 0 else sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y)) / variance
        alpha = y_mean - beta * x_mean
        residuals = [yi - (alpha + beta * xi) for xi, yi in zip(x, y)]
        residual_vol = stdev(residuals) if len(residuals) > 1 else 0.0
    score = 0.0 if residual_vol == 0 else (alpha * annualization) / (math.sqrt(annualization) * residual_vol)
    state = "Independent leadership" if score > 0.8 else "Positive" if score > 0.5 else "Market beta driven" if score < 0 else "Neutral"
    return {
        "score": round(score, 3),
        "state": state,
        "alpha_annual": round(alpha * annualization * 100, 2),
        "beta": round(beta, 3),
        "residual_vol": round(residual_vol * math.sqrt(annualization) * 100, 2),
        "lookback": count,
    }


def vpt(values: list[float], volumes: list[float]) -> list[float]:
    if not values or not volumes:
        return []
    result = [0.0]
    for prev, current, volume in zip(values[:-1], values[1:], volumes[1:]):
        result.append(result[-1] + (volume * ((current - prev) / prev if prev else 0.0)))
    return result


def vpt_slope_score(values: list[float], volumes: list[float], lookback: int = 20) -> dict[str, Any]:
    line = vpt(values, volumes)
    if len(line) < lookback + 1:
        return {"score": 50, "state": "Insufficient history", "slope_pct": 0}
    start = line[-lookback - 1]
    end = line[-1]
    denom = max(abs(start), mean([abs(item) for item in line[-lookback:]]) or 1)
    slope_pct = (end - start) / denom * 100
    score = clamp(50 + slope_pct * 3)
    state = "Accumulation" if score >= 62 else "Distribution" if score <= 38 else "Neutral"
    return {"score": round(score, 2), "state": state, "slope_pct": round(slope_pct, 2)}


def base_quality_score(bars: list[dict[str, Any]], lookback: int, current: float) -> dict[str, Any]:
    window = bars[-lookback - 1 : -1] if len(bars) > lookback else bars[:-1]
    if len(window) < 10:
        return {
            "score": 45,
            "days_in_base": len(window),
            "tightness_pct": None,
            "range_pct": None,
            "last_5_tight": False,
            "near_high": False,
        }
    closes = [float(bar["close"]) for bar in window]
    highs = [float(bar["high"]) for bar in window]
    lows = [float(bar["low"]) for bar in window]
    close_mean = mean(closes)
    tightness_pct = stdev(closes) / close_mean * 100 if len(closes) > 1 and close_mean else 12.0
    range_pct = (max(highs) - min(lows)) / max(current, 1) * 100
    recent = closes[-5:]
    last_5_tight = max(recent) / max(min(recent), 1) <= 1.03 if len(recent) >= 5 else False
    near_high = current >= max(highs) * 0.94
    score, breakdown = weighted_score(
        {
            "base_duration": (score_between(len(window), 15, lookback), 25),
            "base_tightness": (score_inverse(tightness_pct, 2.2, 10.5), 30),
            "base_range_control": (score_inverse(range_pct, 8, 34), 20),
            "near_base_high": (100 if near_high else 35, 15),
            "last_5_tight": (100 if last_5_tight else 45, 10),
        }
    )
    return {
        "score": score,
        "days_in_base": len(window),
        "tightness_pct": round(tightness_pct, 2),
        "range_pct": round(range_pct, 2),
        "last_5_tight": last_5_tight,
        "near_high": near_high,
        "breakdown": breakdown,
    }


def vcp_pattern_score(bars: list[dict[str, Any]], lookback: int = 55) -> dict[str, Any]:
    window = bars[-lookback:] if len(bars) >= lookback else bars
    if len(window) < 24:
        return {"score": 45, "state": "Insufficient history", "contractions": [], "volume_dryup": None}
    segment_count = 3
    segment_size = max(6, len(window) // segment_count)
    contractions: list[float] = []
    segment_volumes: list[float] = []
    for idx in range(segment_count):
        segment = window[idx * segment_size : (idx + 1) * segment_size] if idx < segment_count - 1 else window[idx * segment_size :]
        if not segment:
            continue
        high = max(float(bar["high"]) for bar in segment)
        low = min(float(bar["low"]) for bar in segment)
        base = max(float(segment[-1]["close"]), 1)
        contractions.append((high - low) / base * 100)
        segment_volumes.append(mean([float(bar.get("volume", 0) or 0) for bar in segment]))
    decreasing_ranges = all(left > right for left, right in zip(contractions[:-1], contractions[1:])) if len(contractions) >= 3 else False
    volume_dryup = segment_volumes[-1] / max(segment_volumes[0], 1) if segment_volumes else None
    final_tight = contractions[-1] <= 6 if contractions else False
    score = 45
    if decreasing_ranges:
        score += 25
    if volume_dryup is not None and volume_dryup <= 0.65:
        score += 20
    if final_tight:
        score += 10
    state = "VCP" if score >= 75 else "Constructive" if score >= 60 else "Loose"
    return {
        "score": int(clamp(score)),
        "state": state,
        "contractions": [round(item, 2) for item in contractions],
        "decreasing_ranges": decreasing_ranges,
        "volume_dryup": None if volume_dryup is None else round(volume_dryup, 2),
        "final_tight": final_tight,
    }


def event_volume_context(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {"fresh_official_event": False, "fresh_event_title": None}
    fresh_official = [
        event
        for event in events
        if int(event.get("days_old", 99) or 99) <= 2
        and str(event.get("source_type", "")).lower() in {"exchange_filing", "company_ir", "earnings_transcript", "promoter_buying"}
    ]
    if not fresh_official:
        return {"fresh_official_event": False, "fresh_event_title": None}
    top = max(fresh_official, key=lambda item: float(item.get("importance", 0) or 0))
    return {"fresh_official_event": True, "fresh_event_title": top.get("title")}


def technical_profile(timeframe: str) -> dict[str, Any]:
    if timeframe == "monthly":
        return {
            "timeframe": "monthly",
            "bar_unit": "week",
            "min_bars": 35,
            "fast_period": 10,
            "medium_period": 20,
            "long_period": 40,
            "rsi_period": 14,
            "atr_period": 10,
            "volume_lookback": 10,
            "breakout_fast": 10,
            "breakout_slow": 26,
            "rs_lookback": 13,
            "stretch_pct": 18,
            "fast_label": "10-week EMA",
            "medium_label": "20-week EMA",
            "long_label": "40-week MA",
            "breakout_fast_label": "10-week high breakout",
            "breakout_slow_label": "26-week high breakout",
        }
    return {
        "timeframe": "weekly",
        "bar_unit": "day",
        "min_bars": 60,
        "fast_period": 20,
        "medium_period": 50,
        "long_period": 200,
        "rsi_period": 14,
        "atr_period": 14,
        "volume_lookback": 20,
        "breakout_fast": 20,
        "breakout_slow": 55,
        "rs_lookback": 55,
        "stretch_pct": 12,
        "fast_label": "20 EMA",
        "medium_label": "50 EMA",
        "long_label": "200 DMA",
        "breakout_fast_label": "20-day high breakout",
        "breakout_slow_label": "55-day high breakout",
    }


def technical_strength_score(
    bars: list[dict[str, Any]],
    benchmark_bars: list[dict[str, Any]] | None = None,
    market_regime: str = "Neutral",
    negative_event: bool = False,
    timeframe: str = "weekly",
    account_size: float = 1_000_000,
    risk_fraction: float = 0.01,
    event_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = technical_profile(timeframe)
    working_bars = resample_to_weekly(bars) if timeframe == "monthly" else remove_weekend_bars(bars)
    working_benchmark = resample_to_weekly(benchmark_bars or []) if timeframe == "monthly" and benchmark_bars else benchmark_bars
    if len(working_bars) < profile["min_bars"]:
        raise ValueError(f"At least {profile['min_bars']} {profile['bar_unit']} bars are required for {timeframe} technical scoring")

    closes = [float(bar["close"]) for bar in working_bars]
    volumes = [float(bar["volume"]) for bar in working_bars]
    current = closes[-1]
    fast_ma = ema(closes, profile["fast_period"])[-1]
    medium_ma = ema(closes, profile["medium_period"])[-1]
    long_ma = sma(closes, profile["long_period"])[-1]
    rubber_band = calculate_rubber_band_penalty(current, long_ma)
    rsi14 = rsi(closes, profile["rsi_period"])
    macd_info = macd(closes)
    stochastic_info = stochastic(working_bars)
    atr14 = atr(working_bars, profile["atr_period"])
    vol_lb = profile["volume_lookback"]
    avg_vol = mean(volumes[-vol_lb - 1 : -1]) if len(volumes) >= vol_lb + 1 else mean(volumes)
    volume_ratio = volumes[-1] / avg_vol if avg_vol else 1.0
    prior_volume_window = volumes[-min(6, len(volumes)) - 1 : -1]
    volume_dryup_ratio = (mean(prior_volume_window) / avg_vol) if avg_vol and prior_volume_window else 1.0
    relative_volume_confirmed = volume_ratio >= 1.5
    obv_info = obv_slope_score(closes, volumes, min(20, max(5, len(closes) // 4)))
    vpt_info = vpt_slope_score(closes, volumes, min(20, max(5, len(closes) // 4)))
    fast_breakout = max(float(bar["high"]) for bar in working_bars[-profile["breakout_fast"] - 1 : -1])
    slow_breakout = max(float(bar["high"]) for bar in working_bars[-profile["breakout_slow"] - 1 : -1])
    breakout_fast = current > fast_breakout
    breakout_slow = current > slow_breakout
    rs = relative_strength(working_bars, working_benchmark, profile["rs_lookback"])
    rs_rating = event_context.get("rs_rating") if event_context else None
    base_quality = base_quality_score(working_bars, profile["breakout_slow"], current)
    vcp = vcp_pattern_score(working_bars, profile["breakout_slow"])
    keltner_upper = fast_ma + 2 * atr14
    atr_breakout = current > keltner_upper
    last_bar = working_bars[-1]
    day_range = max(float(last_bar["high"]) - float(last_bar["low"]), 0.0001)
    close_position = (current - float(last_bar["low"])) / day_range
    idio = idiosyncratic_momentum(
        working_bars,
        working_benchmark,
        lookback=52 if timeframe == "monthly" else 252,
        annualization=52 if timeframe == "monthly" else 252,
    )
    rsi_low, rsi_high = (50, 78) if market_regime == "Risk-on" else (40, 68) if market_regime == "Risk-off" else (48, 76)
    idio_pass = idio["state"] == "Insufficient history" or float(idio["score"]) > 0.5
    fresh_official_event = bool((event_context or {}).get("fresh_official_event"))

    checks = {
        "price_above_fast_ma": current > fast_ma,
        "price_above_medium_ma": current > medium_ma,
        "price_above_long_ma": current > long_ma,
        "medium_ma_above_long_ma": medium_ma > long_ma,
        "fast_breakout": breakout_fast,
        "slow_breakout": breakout_slow,
        "atr_breakout": atr_breakout,
        "volume_confirmed": relative_volume_confirmed,
        "vpt_accumulation": vpt_info["score"] >= 60,
        "obv_accumulation": obv_info["score"] >= 55,
        "volume_dryup_before_breakout": (not (breakout_fast or breakout_slow)) or volume_dryup_ratio <= 0.85,
        "base_quality": base_quality["score"] >= 62,
        "vcp_pattern": vcp["score"] >= 65,
        "tight_near_high": bool(base_quality["last_5_tight"] and base_quality["near_high"]),
        "strong_close": (not (breakout_fast or breakout_slow or atr_breakout)) or close_position >= 0.7,
        "rsi_healthy": rsi_low <= rsi14 <= rsi_high,
        "macd_bullish": macd_info["state"] == "Bullish",
        "idiosyncratic_momentum_positive": idio_pass,
        "relative_strength_positive": rs["state"] in {"Positive", "Leadership"},
        "rs_rating_leadership": rs_rating is None or int(rs_rating) >= 70,
        "rubber_band_not_overextended": rubber_band["multiplier"] > 0,
    }
    weights = {
        "price_above_fast_ma": 8,
        "price_above_medium_ma": 10,
        "price_above_long_ma": 10,
        "medium_ma_above_long_ma": 10,
        "fast_breakout": 8,
        "slow_breakout": 10,
        "atr_breakout": 8,
        "volume_confirmed": 8,
        "vpt_accumulation": 8,
        "obv_accumulation": 4,
        "volume_dryup_before_breakout": 5,
        "base_quality": 8,
        "vcp_pattern": 8,
        "tight_near_high": 6,
        "strong_close": 7,
        "rsi_healthy": 10,
        "macd_bullish": 8,
        "idiosyncratic_momentum_positive": 12,
        "relative_strength_positive": 6,
        "rs_rating_leadership": 7,
        "rubber_band_not_overextended": 12,
    }
    max_weight = sum(weights.values())
    score = sum(weights[key] for key, ok in checks.items() if ok) / max_weight * 100
    extended_from_fast = pct_distance(current, fast_ma)
    fake_breakout_flags: list[str] = []
    if (breakout_fast or breakout_slow) and volume_ratio < 1.3:
        fake_breakout_flags.append("Breakout lacks volume confirmation")
        score -= 8
    if (breakout_fast or breakout_slow) and obv_info["score"] < 45:
        fake_breakout_flags.append("OBV does not confirm accumulation")
        score -= 6
    if (breakout_fast or breakout_slow or atr_breakout) and vpt_info["score"] < 50:
        fake_breakout_flags.append("VPT does not confirm price-volume accumulation")
        score -= 7
    if (breakout_fast or breakout_slow or atr_breakout) and idio["state"] != "Insufficient history" and float(idio["score"]) < 0.5:
        fake_breakout_flags.append("Breakout appears market-beta driven, not idiosyncratic")
        score -= 10
    if (breakout_fast or breakout_slow or atr_breakout) and close_position < 0.3 and volume_ratio >= 1.5:
        fake_breakout_flags.append("High-volume breakout closed in the lower 30 percent of the range")
        score -= 10
    if (breakout_fast or breakout_slow) and volume_dryup_ratio > 1.15:
        fake_breakout_flags.append("No clear volume dry-up before breakout")
        score -= 4
    if (breakout_fast or breakout_slow) and volume_ratio >= 2.2 and fresh_official_event:
        fake_breakout_flags.append("Volume spike may be event-day reaction, not organic accumulation")
        score -= 4
    if (breakout_fast or breakout_slow) and base_quality["score"] < 50:
        fake_breakout_flags.append("Breakout is from a loose or shallow base")
        score -= 8
    if extended_from_fast > profile["stretch_pct"]:
        fake_breakout_flags.append(f"Price is stretched more than {profile['stretch_pct']} percent above {profile['fast_label']}")
        score -= 10
    if rubber_band["status"] == "REJECTED_OVEREXTENDED":
        fake_breakout_flags.append("Rubber-band limit breached: price is too far above 200 DMA")
        score -= 18
    elif rubber_band["status"] == "CAUTION_LATE_STAGE":
        fake_breakout_flags.append("Late-stage extension: position size should be cut")
        score -= 6
    if market_regime == "Risk-off" and (breakout_fast or breakout_slow):
        fake_breakout_flags.append("Broader market is risk-off")
        score -= 8
    if negative_event:
        fake_breakout_flags.append("Fresh negative event conflicts with price strength")
        score -= 12
    if rsi14 > 78:
        fake_breakout_flags.append("RSI is overheated")
        score -= 8

    breakout_level = slow_breakout if breakout_slow else fast_breakout
    recent_low_lookback = min(10, len(working_bars))
    recent_low = min(float(bar["low"]) for bar in working_bars[-recent_low_lookback:])
    stop_by_atr = current - atr14 * 1.7
    stop_candidates = [value for value in (recent_low, stop_by_atr) if 0 < value < current]
    stop = max(stop_candidates) if stop_candidates else max(current - atr14 * 1.7, current * 0.92)
    invalidation = min(stop, medium_ma * 0.985)
    aggressive_low = max(current, breakout_level * 1.001)
    aggressive_high = aggressive_low + max(atr14 * 0.7, current * 0.012)
    pullback_low = max(min(fast_ma, breakout_level) - atr14 * 0.25, 0)
    pullback_high = min(max(fast_ma, breakout_level) + atr14 * 0.25, current)
    risk_per_share = max(aggressive_low - max(stop, 0), 0)
    regime_position_multiplier = float((event_context or {}).get("regime_position_multiplier", 1.0))
    risk_capital = account_size * risk_fraction * regime_position_multiplier * float(rubber_band.get("multiplier", 1.0) or 0.0)
    fixed_risk_units = int(risk_capital / risk_per_share) if risk_per_share > 0 else 0
    atr_pct = atr14 / current if current else 0
    inverse_vol_capital = min(account_size * 0.25, risk_capital / max(atr_pct, 0.005))
    inverse_vol_units = int(inverse_vol_capital / aggressive_low) if aggressive_low > 0 else 0
    adv20 = avg_vol
    liquidity_cap_pct = float((event_context or {}).get("liquidity_cap_pct", 0.015))
    adv20_value = adv20 * current
    liquidity_cap_value = adv20_value * liquidity_cap_pct
    liquidity_cap_units = int(liquidity_cap_value / aggressive_low) if adv20 and aggressive_low > 0 else fixed_risk_units
    units = max(0, min(unit for unit in [fixed_risk_units, inverse_vol_units, liquidity_cap_units] if unit is not None))

    return {
        "score": int(clamp(round(score))),
        "raw_score": round(score, 2),
        "timeframe": timeframe,
        "bar_unit": profile["bar_unit"],
        "indicators": {
            "close": round(current, 2),
            "ema20": round(fast_ma, 2),
            "ema50": round(medium_ma, 2),
            "dma200": round(long_ma, 2),
            "fast_ma": round(fast_ma, 2),
            "medium_ma": round(medium_ma, 2),
            "long_ma": round(long_ma, 2),
            "moving_average_labels": {
                "fast": profile["fast_label"],
                "medium": profile["medium_label"],
                "long": profile["long_label"],
            },
            "rsi14": rsi14,
            "rsi_healthy_range": [rsi_low, rsi_high],
            "macd": macd_info,
            "stochastic": stochastic_info,
            "atr14": atr14,
            "atr_pct": round(atr_pct * 100, 2),
            "keltner_upper": round(keltner_upper, 2),
            "atr_breakout": atr_breakout,
            "close_position_in_range": round(close_position, 3),
            "volume_ratio": round(volume_ratio, 2),
            "relative_volume": round(volume_ratio, 2),
            "volume_dryup_ratio": round(volume_dryup_ratio, 2),
            "avg_volume_20": round(avg_vol, 2),
            "rubber_band": rubber_band,
            "extension_ratio": rubber_band["ratio"],
            "extension_pct_above_200dma": rubber_band["extension_pct"],
            "vpt": vpt_info,
            "obv": obv_info,
            "breakout_20d": breakout_fast,
            "breakout_55d": breakout_slow,
            "breakout_fast_label": profile["breakout_fast_label"],
            "breakout_slow_label": profile["breakout_slow_label"],
            "breakout_level": round(breakout_level, 2),
            "relative_strength": rs,
            "idiosyncratic_momentum": idio,
            "rs_rating": rs_rating,
            "extended_from_20ema_pct": round(extended_from_fast, 2),
            "base_quality": base_quality,
            "vcp_pattern": vcp,
            "event_volume_context": {
                "fresh_official_event": fresh_official_event,
                "fresh_event_title": (event_context or {}).get("fresh_event_title"),
            },
        },
        "checks": checks,
        "fake_breakout_flags": fake_breakout_flags,
        "entry": {
            "breakout_level": round(breakout_level, 2),
            "aggressive": [round(aggressive_low, 2), round(aggressive_high, 2)] if score >= 60 else None,
            "pullback": [round(pullback_low, 2), round(pullback_high, 2)],
            "stop": round(max(stop, 0), 2),
            "invalidation": f"Close below {round(max(invalidation, 0), 2)}",
            "atr_stop_note": "Use 1.5x to 2.0x ATR below entry depending on position horizon",
            "position_sizing": {
                "account_size": round(account_size, 2),
                "risk_fraction": risk_fraction,
                "risk_capital": round(risk_capital, 2),
                "base_risk_capital": round(account_size * risk_fraction, 2),
                "regime_position_multiplier": regime_position_multiplier,
                "rubber_band_multiplier": rubber_band.get("multiplier", 1.0),
                "entry_reference": round(aggressive_low, 2),
                "risk_per_share": round(risk_per_share, 2),
                "fixed_risk_quantity": fixed_risk_units,
                "inverse_volatility_quantity": inverse_vol_units,
                "liquidity_cap_quantity": liquidity_cap_units,
                "liquidity_cap_pct_of_adv": liquidity_cap_pct,
                "average_daily_volume_20": round(adv20, 2),
                "average_daily_value_20": round(adv20_value, 2),
                "liquidity_cap_value": round(liquidity_cap_value, 2),
                "atr_pct": round(atr_pct * 100, 2),
                "suggested_quantity": units,
                "approx_position_value": round(units * aggressive_low, 2),
                "sizing_method": "min(fixed risk, inverse volatility, liquidity cap) after market-regime and rubber-band multipliers",
            },
        },
        "exit_rules": {
            "price_stop": f"Close below {round(max(stop, 0), 2)}",
            "swing_trend_exit": "Two closes below 20 EMA",
            "positional_trend_exit": "Close below 50 EMA",
            "event_exit": "Reduce or exit on auditor resignation, pledge jump, guidance collapse, fraud, or governance issue",
        },
    }


def market_support_score(market: dict[str, Any]) -> dict[str, Any]:
    bars = market.get("nifty_bars", [])
    closes = [float(bar["close"]) for bar in bars]
    if len(closes) >= 200:
        nifty_close = closes[-1]
        ema50 = ema(closes, 50)[-1]
        dma200 = sma(closes, 200)[-1]
    else:
        nifty_close = float(market.get("nifty_close", 0))
        ema50 = float(market.get("nifty_ema50", nifty_close))
        dma200 = float(market.get("nifty_dma200", nifty_close))
    count_breadth = float(market.get("breadth_above_50dma", market.get("advance_decline_breadth_pct", 50)))
    nifty500_breadth_pct = market.get("nifty500_breadth_pct", market.get("nifty500_above_50dma_pct", count_breadth))
    master_switch = regime_from_breadth(
        to_float(nifty500_breadth_pct),
        measured_count=market.get("nifty500_total_measured"),
        above_count=market.get("nifty500_above_50dma_count"),
        source=market.get("breadth_source") or "breadth_above_50dma",
    )
    adv_volume = to_float(market.get("advancing_volume"))
    dec_volume = to_float(market.get("declining_volume"))
    volume_breadth = None
    if adv_volume is not None and dec_volume is not None and adv_volume + dec_volume > 0:
        volume_breadth = adv_volume / (adv_volume + dec_volume) * 100
    breadth = count_breadth if volume_breadth is None else count_breadth * 0.60 + volume_breadth * 0.40
    breadth_history = market.get("breadth_history") or []
    zweig_thrust = None
    if len(breadth_history) >= 10:
        recent = [float(item) for item in breadth_history[-10:]]
        zweig_thrust = mean(recent)
        if min(recent[:5]) < 42 and max(recent[-5:]) > 61.5:
            breadth = min(100, breadth + 8)
    sector_strength = float(market.get("sector_strength", 50))
    vix = float(market.get("vix", 15))
    vix_score = score_inverse(vix, 14, 30)
    total, breakdown = weighted_score(
        {
            "nifty_above_50ema": (100 if nifty_close > ema50 else 20, 20),
            "nifty_above_200dma": (100 if nifty_close > dma200 else 15, 25),
            "breadth": (breadth, 20),
            "sector_index_strength": (sector_strength, 20),
            "vix_regime": (vix_score, 15),
        }
    )
    regime = "Risk-on" if total >= 70 else "Neutral" if total >= 45 else "Risk-off"
    updated_at = market.get("updated_at")
    data_age_days = None
    if updated_at:
        try:
            data_age_days = max(0, (datetime.now(timezone.utc) - parse_bar_datetime(updated_at)).days)
        except ValueError:
            data_age_days = None
    is_stale = data_age_days is None or data_age_days > 2
    stale_penalty = 0 if not is_stale else min(24, 4 + max((data_age_days or 7) - 2, 0) * 1.5)
    adjusted_total = int(clamp(total - stale_penalty))
    adjusted_regime = "Risk-on" if adjusted_total >= 70 else "Neutral" if adjusted_total >= 45 else "Risk-off"
    return {
        "score": adjusted_total,
        "raw_score": total,
        "regime": adjusted_regime,
        "regime_status": master_switch["regime_status"],
        "position_multiplier": master_switch["position_multiplier"],
        "can_buy": master_switch["can_buy"],
        "status_message": master_switch["description"],
        "master_switch": master_switch,
        "raw_regime": regime,
        "nifty_close": round(nifty_close, 2),
        "nifty_ema50": round(ema50, 2),
        "nifty_dma200": round(dma200, 2),
        "breadth_above_50dma": round(breadth, 2),
        "advance_decline_breadth_pct": count_breadth,
        "volume_breadth_pct": None if volume_breadth is None else round(volume_breadth, 2),
        "zweig_breadth_thrust_10d": None if zweig_thrust is None else round(zweig_thrust, 2),
        "advance_decline_ratio": market.get("advance_decline_ratio"),
        "advancers": market.get("advancers"),
        "decliners": market.get("decliners"),
        "sector_strength": sector_strength,
        "sector_rotation": market.get("sector_rotation", {}),
        "vix": vix,
        "source": market.get("source", "seed_or_manual"),
        "breadth_source": market.get("breadth_source"),
        "updated_at": updated_at,
        "data_age_days": data_age_days,
        "is_stale": is_stale,
        "stale_penalty": round(stale_penalty, 2),
        "breakdown": breakdown,
        "rules": {
            "Risk-on": "Allow breakouts, normal stops, normal candidate count",
            "Neutral": "Prefer pullbacks and leaders, keep risk moderate",
            "Risk-off": "Reduce breakout score, tighten stops, cut candidate count, raise quality threshold",
        },
    }


def risk_penalty(
    fundamentals: dict[str, Any],
    event_result: dict[str, Any],
    technical_result: dict[str, Any],
    market_result: dict[str, Any],
    forensic_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    penalties: dict[str, float] = {}
    debt = float(fundamentals.get("debt_equity", 0) or 0)
    pledge = float(fundamentals.get("pledge_percent", 0) or 0)
    cfo_pat = fundamentals.get("cfo_pat")
    forensic = forensic_result or forensic_earnings_quality(fundamentals)
    if sector_kind(fundamentals) != "financial" and debt > 1.2:
        penalties["high_debt"] = min((debt - 1.2) * 8, 14)
    if pledge > 0:
        if pledge <= 10:
            penalties["promoter_pledge_minor"] = 1.5
        elif pledge <= 30:
            penalties["promoter_pledge_moderate"] = 6 + (pledge - 10) * 0.25
        else:
            penalties["promoter_pledge_high"] = min(14 + (pledge - 30) * 0.18, 22)
    if cfo_pat is not None and float(cfo_pat) < 0.65:
        penalties["weak_cash_conversion"] = min((0.65 - float(cfo_pat)) * 35, 12)
    earnings_days = days_until_date(fundamentals.get("next_earnings_date"))
    if earnings_days is not None and 0 <= earnings_days <= 7:
        penalties["earnings_within_7_days"] = 12 if earnings_days <= 2 else 8
    for flag in forensic.get("flags", []):
        if flag == "piotroski_below_4":
            penalties["piotroski_below_4"] = 18
        elif flag == "piotroski_below_7":
            penalties["piotroski_below_7"] = 7
        elif flag == "high_sloan_accrual_ratio":
            penalties["high_sloan_accrual_ratio"] = 16
        elif flag == "beneish_manipulation_risk":
            penalties["beneish_manipulation_risk"] = 16
        elif flag == "altman_distress_zone":
            penalties["altman_distress_zone"] = 14
        elif flag == "cfo_below_80pct_of_ebitda":
            penalties["cfo_below_80pct_of_ebitda"] = 8
        elif flag == "cfo_ebitda_below_60pct_two_years":
            penalties["cfo_ebitda_below_60pct_two_years"] = 18
        elif flag in {"receivables_growing_much_faster_than_sales", "cash_conversion_cycle_deteriorating"}:
            penalties[flag] = 7
    if fundamentals.get("dilution_flag"):
        penalties["dilution"] = 8
    if event_result.get("negative_governance"):
        penalties["negative_governance_event"] = 18
    indicators = technical_result.get("indicators", {})
    if float(indicators.get("extended_from_20ema_pct", 0)) > 12:
        penalties["stretched_price"] = 8
    if float(indicators.get("rsi14", 50)) > 78:
        penalties["overheated_rsi"] = 6
    if market_result.get("regime") == "Risk-off":
        penalties["risk_off_market"] = 10
    raw_total = sum(penalties.values())
    effective_total = 60 * (1 - math.exp(-raw_total / 45)) if raw_total > 0 else 0
    return {
        "score": int(clamp(round(effective_total), 0, 60)),
        "raw_score": round(raw_total, 2),
        "breakdown": {k: round(v, 2) for k, v in penalties.items()},
        "technical_breakout_flags_diagnostic": technical_result.get("fake_breakout_flags", []),
    }


def portfolio_concentration_check(company: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    holdings = context.get("holdings") or []
    max_sector_positions = int(context.get("max_sector_positions", 3))
    max_industry_positions = int(context.get("max_industry_positions", 2))
    sector = company.get("sector")
    industry = company.get("industry")
    symbol = company.get("symbol")
    sector_count = 0
    industry_count = 0
    already_held = False
    for holding in holdings:
        if str(holding.get("symbol", "")).upper() == str(symbol).upper():
            already_held = True
        if holding.get("sector") == sector:
            sector_count += 1
        if industry and holding.get("industry") == industry:
            industry_count += 1
    if not already_held and sector_count >= max_sector_positions:
        return {
            "pass": False,
            "reason": f"portfolio_sector_limit_{sector}",
            "sector_count": sector_count,
            "industry_count": industry_count,
            "max_sector_positions": max_sector_positions,
            "max_industry_positions": max_industry_positions,
        }
    if not already_held and industry_count >= max_industry_positions:
        return {
            "pass": False,
            "reason": f"portfolio_industry_limit_{industry}",
            "sector_count": sector_count,
            "industry_count": industry_count,
            "max_sector_positions": max_sector_positions,
            "max_industry_positions": max_industry_positions,
        }
    return {
        "pass": True,
        "reason": "portfolio concentration ok",
        "sector_count": sector_count,
        "industry_count": industry_count,
        "max_sector_positions": max_sector_positions,
        "max_industry_positions": max_industry_positions,
    }


def confidence_interval(weekly: int, monthly: int, weekly_raw: float, monthly_raw: float) -> dict[str, Any]:
    weekly_margin = round(weekly_raw - 75, 2)
    monthly_margin = round(monthly_raw - 75, 2)
    min_margin = round(min(weekly_margin, monthly_margin), 2)
    if min_margin >= 10:
        label = "Wide"
    elif min_margin >= 0:
        label = "Thin"
    elif weekly >= 60 or monthly >= 60:
        label = "Watch"
    else:
        label = "Weak"
    return {
        "label": label,
        "type": "score_margin_band",
        "not_statistical_confidence_interval": True,
        "weekly_margin_above_high": weekly_margin,
        "monthly_margin_above_high": monthly_margin,
        "min_margin_above_high": min_margin,
    }


def apply_portfolio_position_adjustment(entry: dict[str, Any], portfolio_check: dict[str, Any]) -> dict[str, Any]:
    sizing = dict(entry.get("position_sizing") or {})
    if not sizing:
        return entry
    pre_qty = int(sizing.get("suggested_quantity") or 0)
    entry_ref = float(sizing.get("entry_reference") or 0)
    multiplier = float(portfolio_check.get("position_multiplier", 1.0) or 0.0)
    sector_headroom = portfolio_check.get("sector_headroom") or {}
    available_capital = sector_headroom.get("available_capital")
    headroom_qty = pre_qty
    if available_capital is not None and entry_ref > 0:
        headroom_qty = int(float(available_capital) / entry_ref)
    adjusted_qty = max(0, min(int(pre_qty * multiplier), headroom_qty))
    sizing.update(
        {
            "pre_portfolio_quantity": pre_qty,
            "portfolio_multiplier": multiplier,
            "sector_headroom_cap_quantity": headroom_qty,
            "portfolio_adjusted_quantity": adjusted_qty,
            "suggested_quantity": adjusted_qty,
            "approx_position_value": round(adjusted_qty * entry_ref, 2),
            "sector_headroom": sector_headroom,
            "correlation_check": portfolio_check.get("correlation"),
            "sizing_method": f"{sizing.get('sizing_method', 'base sizing')} + sector/correlation matrix",
        }
    )
    return {**entry, "position_sizing": sizing}


def final_decision(
    company: dict[str, Any],
    bars: list[dict[str, Any]],
    benchmark_bars: list[dict[str, Any]],
    market_result: dict[str, Any],
    account_size: float = 1_000_000,
    portfolio_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fundamentals = {**company["fundamentals"], "sector": company.get("sector"), "industry": company.get("industry", "")}
    master_switch = market_result.get("master_switch") or {
        "can_buy": market_result.get("regime") != "Risk-off",
        "position_multiplier": 0.5 if market_result.get("regime") == "Neutral" else 1.0,
        "regime_status": "YELLOW" if market_result.get("regime") == "Neutral" else "GREEN",
    }
    regime_position_multiplier = float(master_switch.get("position_multiplier", 1.0) or 0.0)
    business = business_quality_score(fundamentals)
    tailwind = sector_tailwind_score(company["tailwind"])
    event_result = event_strength_score(company.get("events", []), timeframe="weekly")
    monthly_event_result = event_strength_score(company.get("events", []), timeframe="monthly")
    technical = technical_strength_score(
        bars,
        benchmark_bars,
        market_result.get("regime", "Neutral"),
        bool(event_result.get("negative_governance")),
        timeframe="weekly",
        account_size=account_size,
        event_context={
            **event_volume_context(event_result.get("events", [])),
            "rs_rating": company.get("rs_rating"),
            "liquidity_cap_pct": company.get("liquidity_cap_pct", 0.015),
            "regime_position_multiplier": regime_position_multiplier,
        },
    )
    monthly_technical = technical_strength_score(
        bars,
        benchmark_bars,
        market_result.get("regime", "Neutral"),
        bool(monthly_event_result.get("negative_governance")),
        timeframe="monthly",
        account_size=account_size,
        event_context={
            **event_volume_context(monthly_event_result.get("events", [])),
            "rs_rating": company.get("rs_rating"),
            "liquidity_cap_pct": company.get("liquidity_cap_pct", 0.015),
            "regime_position_multiplier": regime_position_multiplier,
        },
    )
    risk = risk_penalty(fundamentals, event_result, technical, market_result, business.get("forensic_quality"))
    valuation = business["valuation_score"]
    weekly_weights = {
        "technical": 0.28,
        "events": 0.15,
        "tailwind": 0.17,
        "business": 0.20,
        "market": 0.20,
    }
    monthly_weights = {
        "business": 0.28,
        "tailwind": 0.18,
        "events": 0.17,
        "technical": 0.20,
        "market": 0.10,
        "valuation": 0.07,
    }
    weekly_raw = (
        technical["score"] * weekly_weights["technical"]
        + event_result["score"] * weekly_weights["events"]
        + tailwind["score"] * weekly_weights["tailwind"]
        + business["score"] * weekly_weights["business"]
        + market_result["score"] * weekly_weights["market"]
        - risk["score"]
    )
    monthly_raw = (
        business["score"] * monthly_weights["business"]
        + tailwind["score"] * monthly_weights["tailwind"]
        + monthly_event_result["score"] * monthly_weights["events"]
        + monthly_technical["score"] * monthly_weights["technical"]
        + market_result["score"] * monthly_weights["market"]
        + valuation * monthly_weights["valuation"]
        - risk["score"]
    )
    weekly = int(clamp(round(weekly_raw)))
    monthly = int(clamp(round(monthly_raw)))
    five_questions = {
        "good_business": business["score"] >= 65,
        "sector_helping_now": tailwind["score"] >= 60,
        "fresh_trigger": event_result["score"] >= 55,
        "chart_confirming": technical["score"] >= 60,
        "where_am_i_wrong_defined": risk["score"] <= 35 and bool(technical.get("entry")),
    }
    data_quality = business.get("data_quality", {})
    data_complete_enough = float(data_quality.get("completeness_pct", 0) or 0) >= 40
    forensic_clean = not bool(business.get("forensic_quality", {}).get("hard_fail"))
    tech_indicators = technical.get("indicators", {})
    idio_score = float(tech_indicators.get("idiosyncratic_momentum", {}).get("score", 0) or 0)
    breakout_active = bool(tech_indicators.get("breakout_20d") or tech_indicators.get("breakout_55d") or tech_indicators.get("atr_breakout"))
    idio_state = tech_indicators.get("idiosyncratic_momentum", {}).get("state")
    idiosyncratic_ok = (not breakout_active) or idio_state == "Insufficient history" or idio_score > 0.5
    portfolio_count_check = portfolio_concentration_check(company, portfolio_context or {})
    portfolio_matrix = portfolio_risk_audit(company, bars, portfolio_context or {}, account_size)
    portfolio_check = {
        **portfolio_count_check,
        "count_gate_pass": portfolio_count_check["pass"],
        "portfolio_matrix_pass": portfolio_matrix["pass"],
        "sector_headroom": portfolio_matrix["sector_headroom"],
        "correlation": portfolio_matrix["correlation"],
        "position_multiplier": portfolio_matrix["position_multiplier"],
        "matrix_reason": portfolio_matrix["reason"],
        "pass": portfolio_count_check["pass"] and portfolio_matrix["pass"],
    }
    rubber_band = tech_indicators.get("rubber_band", {})
    rubber_band_ok = float(rubber_band.get("multiplier", 1.0) or 0.0) > 0
    master_switch_ok = bool(master_switch.get("can_buy", True))
    candidate = (
        all(five_questions.values())
        and market_result["score"] >= 45
        and master_switch_ok
        and rubber_band_ok
        and data_complete_enough
        and forensic_clean
        and idiosyncratic_ok
    )
    risk_off_quality_gate = True
    if market_result.get("regime") == "Risk-off":
        risk_off_quality_gate = business["score"] >= 78 and risk["score"] <= 20
        candidate = candidate and risk_off_quality_gate
    failed_gates = [key for key, passed in five_questions.items() if not passed]
    if market_result["score"] < 45:
        failed_gates.append("market_support")
    if not master_switch_ok:
        failed_gates.append("market_breadth_master_switch_red")
    if not rubber_band_ok:
        failed_gates.append("rubber_band_overextension_limit")
    if not data_complete_enough:
        failed_gates.append("data_completeness_below_40pct")
    if not forensic_clean:
        failed_gates.append("forensic_earnings_quality_fail")
    if not idiosyncratic_ok:
        failed_gates.append("idiosyncratic_momentum_below_0_5")
    if not risk_off_quality_gate:
        failed_gates.append("risk_off_quality_gate")
    if not portfolio_check["pass"]:
        candidate = False
        failed_gates.append(portfolio_check.get("reason") or portfolio_check.get("matrix_reason") or "portfolio_risk_matrix_failed")
    entry = {**technical["entry"]}
    entry = apply_portfolio_position_adjustment(entry, portfolio_check)
    if not candidate:
        entry["aggressive"] = None
        entry["candidate_gate"] = "Blocked: " + ", ".join(failed_gates or ["candidate quality threshold"])
    else:
        entry["candidate_gate"] = "Pass"
    conviction = (
        "Insufficient Data"
        if not data_complete_enough
        else "Hard Avoid"
        if weekly_raw < 0 or monthly_raw < 0
        else "High"
        if candidate and weekly >= 75 and monthly >= 75
        else "Watchlist"
        if weekly >= 60 or monthly >= 60
        else "Avoid"
    )
    confidence = confidence_interval(weekly, monthly, weekly_raw, monthly_raw)
    trade_state = derive_trade_state(candidate, {**technical, "entry": entry})
    explanation = {
        "thesis": build_thesis(company, business, tailwind, event_result, technical, market_result, risk),
        "five_questions": five_questions,
        "risk_flags": list(risk["breakdown"].keys()) + technical.get("fake_breakout_flags", []),
    }
    return {
        "symbol": company["symbol"],
        "name": company["name"],
        "sector": company["sector"],
        "industry": company.get("industry", ""),
        "market_cap_cr": company.get("market_cap_cr"),
        "price": technical["indicators"]["close"],
        "change_pct": round(pct_distance(technical["indicators"]["close"], float(bars[-2]["close"])), 2) if len(bars) >= 2 else 0,
        "weekly_score": weekly,
        "monthly_score": monthly,
        "weekly_raw_score": round(weekly_raw, 2),
        "monthly_raw_score": round(monthly_raw, 2),
        "score_diagnostics": {
            "weekly_raw": round(weekly_raw, 2),
            "weekly_clamped": weekly,
            "monthly_raw": round(monthly_raw, 2),
            "monthly_clamped": monthly,
            "hard_avoid": weekly_raw < 0 or monthly_raw < 0,
            "weekly_weights": weekly_weights,
            "monthly_weights": monthly_weights,
        },
        "confidence_interval": confidence,
        "business_quality": business,
        "sector_tailwind": tailwind,
        "event_strength": event_result,
        "monthly_event_strength": monthly_event_result,
        "technical_strength": technical,
        "monthly_technical_strength": monthly_technical,
        "market_support": market_result,
        "risk_penalty": risk,
        "candidate": candidate,
        "conviction": conviction,
        "data_quality_gate": {
            "pass": data_complete_enough,
            "min_completeness_pct": 40,
            "actual_completeness_pct": data_quality.get("completeness_pct"),
            "missing_fields": data_quality.get("missing_fields", []),
            "warning": "Blocked from candidate status because fundamentals are less than 40 percent complete" if not data_complete_enough else None,
        },
        "forensic_gate": {
            "pass": forensic_clean,
            "flags": business.get("forensic_quality", {}).get("flags", []),
            "hard_fail": business.get("forensic_quality", {}).get("hard_fail", False),
        },
        "trade_state": trade_state,
        "entry": entry,
        "portfolio_check": portfolio_check,
        "execution_audit": {
            "market_master_switch": master_switch,
            "rubber_band": rubber_band,
            "portfolio_matrix": {
                "sector_headroom": portfolio_check.get("sector_headroom"),
                "correlation": portfolio_check.get("correlation"),
                "position_multiplier": portfolio_check.get("position_multiplier"),
                "pass": portfolio_check.get("pass"),
            },
            "overnight_batch_note": "Heavy math is designed for EOD batching; live ticks should only reconcile LTP against stored trigger levels.",
        },
        "exit_rules": technical["exit_rules"],
        "explanation_json": explanation,
        "fundamentals": fundamentals,
        "tailwind_factors": company.get("tailwind_factors", []),
        "bars": bars[-260:],
        "benchmark_bars": (benchmark_bars or [])[-260:],
    }


def derive_trade_state(candidate: bool, technical: dict[str, Any]) -> dict[str, Any]:
    entry = technical.get("entry", {})
    indicators = technical.get("indicators", {})
    price = float(indicators.get("close", 0) or 0)
    breakout_level = float(entry.get("breakout_level", 0) or 0)
    stop = float(entry.get("stop", 0) or 0)
    if price and stop and price <= stop:
        state = "Exited"
        reason = "Price is at or below stop"
    elif candidate and breakout_level and price >= breakout_level:
        state = "Triggered"
        reason = "Candidate is trading through the stored breakout level"
    elif candidate:
        state = "Watchlist"
        reason = "Candidate passed the five-question gate but has not triggered"
    else:
        state = "Screened"
        reason = "Screened, but not a complete candidate"
    return {
        "state": state,
        "reason": reason,
        "breakout_level": round(breakout_level, 2),
        "last_price": round(price, 2),
        "stop": round(stop, 2),
    }


def build_thesis(
    company: dict[str, Any],
    business: dict[str, Any],
    tailwind: dict[str, Any],
    event_result: dict[str, Any],
    technical: dict[str, Any],
    market_result: dict[str, Any],
    risk: dict[str, Any],
) -> list[str]:
    top_tailwind = (company.get("tailwind_factors") or ["No specific tailwind factor recorded"])[0]
    top_event = "No fresh event recorded"
    if event_result.get("events"):
        top_event = max(event_result["events"], key=lambda event: abs(float(event.get("net_score", 0)))).get("title", top_event)
    fundamentals = company.get("fundamentals", {})
    quality_reason = f"ROCE {fundamentals.get('roce', 'NA')}%, CFO/PAT {fundamentals.get('cfo_pat', 'NA')}, debt/equity {fundamentals.get('debt_equity', 'NA')}"
    lines = [
        f"{company['symbol']} business quality is {business['score']}/100 because {quality_reason}.",
        f"Sector tailwind is {tailwind['score']}/100; the lead factor is: {top_tailwind}.",
        f"Event layer scores {event_result['score']}/100; top event: {top_event}.",
        f"Technical score is {technical['score']}/100 with breakout level {technical['entry']['breakout_level']} and stop {technical['entry']['stop']}.",
    ]
    if risk["score"] > 0:
        lines.append(f"Risk penalty is {risk['score']}/100 from {', '.join(risk['breakdown'].keys()) or 'general risk checks'}.")
    else:
        lines.append("No major rule-based risk penalty is active.")
    return lines
