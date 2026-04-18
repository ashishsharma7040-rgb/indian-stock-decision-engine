from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Any


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def score_between(value: float | None, poor: float, excellent: float) -> float:
    if value is None:
        return 45.0
    if excellent == poor:
        return 50.0
    score = (value - poor) / (excellent - poor) * 100
    return clamp(score)


def score_inverse(value: float | None, excellent: float, poor: float) -> float:
    if value is None:
        return 45.0
    if poor == excellent:
        return 50.0
    score = (poor - value) / (poor - excellent) * 100
    return clamp(score)


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
    denom = max(abs(start), mean([abs(item) for item in line[-lookback:]]) or 1)
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


def business_quality_score(fundamentals: dict[str, Any]) -> dict[str, Any]:
    revenue_score = score_between(fundamentals.get("sales_cagr"), 0, 22)
    profit_score = score_between(fundamentals.get("profit_cagr"), 0, 28)
    roce_score = score_between(fundamentals.get("roce"), 8, 28)
    roe_score = score_between(fundamentals.get("roe"), 8, 25)
    debt_score = score_inverse(fundamentals.get("debt_equity"), 0.0, 1.8)
    cfo_score = score_between(fundamentals.get("cfo_pat"), 0.45, 1.05)
    fcf_map = {"positive": 100, "improving": 82, "volatile": 55, "negative": 20}
    fcf_score = fcf_map.get(str(fundamentals.get("fcf_trend", "")).lower(), 45)
    promoter_trend = str(fundamentals.get("promoter_holding_trend", "stable")).lower()
    promoter_score = {"rising": 100, "stable": 85, "flat": 78, "falling": 35}.get(promoter_trend, 65)
    promoter_score -= min(float(fundamentals.get("pledge_percent", 0) or 0) * 5, 50)
    if fundamentals.get("dilution_flag"):
        promoter_score -= 20
    margin_score = score_between(fundamentals.get("margin_trend_bps"), -250, 350)
    pe = fundamentals.get("pe")
    growth = fundamentals.get("profit_cagr") or 0
    if pe is None or growth <= 0:
        valuation_score = 45
    else:
        peg = pe / max(growth, 1)
        valuation_score = 100 - abs(peg - 1.2) * 28
        if pe > 80:
            valuation_score -= 20
        valuation_score = clamp(valuation_score)

    total, breakdown = weighted_score(
        {
            "revenue_growth": (revenue_score, 15),
            "profit_growth": (profit_score, 15),
            "roce_roe": ((roce_score + roe_score) / 2, 15),
            "debt_profile": (debt_score, 10),
            "cash_flow_quality": ((cfo_score * 0.65) + (fcf_score * 0.35), 15),
            "promoter_pledge_dilution": (promoter_score, 15),
            "margin_trend": (margin_score, 10),
            "valuation_sanity": (valuation_score, 5),
        }
    )
    return {
        "score": total,
        "breakdown": breakdown,
        "valuation_score": int(round(valuation_score)),
        "data_quality": business_data_quality(fundamentals),
    }


def sector_tailwind_score(tailwind: dict[str, Any]) -> dict[str, Any]:
    sector_momentum = float(tailwind.get("sector_momentum", 50) or 50)
    rotation_score = tailwind.get("sector_rotation_score")
    if rotation_score is not None:
        sector_momentum = sector_momentum * 0.55 + float(rotation_score) * 0.45
    total, breakdown = weighted_score(
        {
            "demand_trend": (tailwind.get("demand_trend", 50), 30),
            "policy_support": (tailwind.get("policy_support", 50), 20),
            "cost_environment": (tailwind.get("cost_environment", 50), 15),
            "order_visibility": (tailwind.get("order_visibility", 50), 20),
            "sector_momentum": (sector_momentum, 15),
        }
    )
    return {
        "score": total,
        "breakdown": breakdown,
        "sector_rotation_score": None if rotation_score is None else round(float(rotation_score), 2),
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


def event_strength_score(events: list[dict[str, Any]], timeframe: str = "weekly") -> dict[str, Any]:
    scored = [score_single_event(event, timeframe=timeframe) for event in events]
    if not scored:
        return {"score": 50, "events": [], "negative_governance": False, "timeframe": timeframe}
    denominator = sum(abs(float(e["freshness"]) * float(e["reliability"]) * float(e.get("importance", 50))) for e in scored)
    normalized = 0.0 if denominator == 0 else sum(float(e["net_score"]) for e in scored) / denominator
    score = int(round(clamp(50 + normalized * 50)))
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
    fast_breakout = max(float(bar["high"]) for bar in working_bars[-profile["breakout_fast"] - 1 : -1])
    slow_breakout = max(float(bar["high"]) for bar in working_bars[-profile["breakout_slow"] - 1 : -1])
    breakout_fast = current > fast_breakout
    breakout_slow = current > slow_breakout
    rs = relative_strength(working_bars, working_benchmark, profile["rs_lookback"])
    rs_rating = event_context.get("rs_rating") if event_context else None
    base_quality = base_quality_score(working_bars, profile["breakout_slow"], current)
    fresh_official_event = bool((event_context or {}).get("fresh_official_event"))

    checks = {
        "price_above_fast_ma": current > fast_ma,
        "price_above_medium_ma": current > medium_ma,
        "price_above_long_ma": current > long_ma,
        "medium_ma_above_long_ma": medium_ma > long_ma,
        "fast_breakout": breakout_fast,
        "slow_breakout": breakout_slow,
        "volume_confirmed": relative_volume_confirmed,
        "obv_accumulation": obv_info["score"] >= 60,
        "volume_dryup_before_breakout": (not (breakout_fast or breakout_slow)) or volume_dryup_ratio <= 0.85,
        "base_quality": base_quality["score"] >= 62,
        "tight_near_high": bool(base_quality["last_5_tight"] and base_quality["near_high"]),
        "rsi_healthy": 55 <= rsi14 <= 72,
        "macd_bullish": macd_info["state"] == "Bullish",
        "relative_strength_positive": rs["state"] in {"Positive", "Leadership"},
        "rs_rating_leadership": rs_rating is None or int(rs_rating) >= 70,
    }
    weights = {
        "price_above_fast_ma": 8,
        "price_above_medium_ma": 10,
        "price_above_long_ma": 10,
        "medium_ma_above_long_ma": 10,
        "fast_breakout": 8,
        "slow_breakout": 12,
        "volume_confirmed": 10,
        "obv_accumulation": 7,
        "volume_dryup_before_breakout": 5,
        "base_quality": 10,
        "tight_near_high": 6,
        "rsi_healthy": 10,
        "macd_bullish": 8,
        "relative_strength_positive": 12,
        "rs_rating_leadership": 8,
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
    stop = min(recent_low, stop_by_atr)
    invalidation = min(stop, medium_ma * 0.985)
    aggressive_low = max(current, breakout_level * 1.001)
    aggressive_high = aggressive_low + max(atr14 * 0.7, current * 0.012)
    pullback_low = max(min(fast_ma, breakout_level) - atr14 * 0.25, 0)
    pullback_high = min(max(fast_ma, breakout_level) + atr14 * 0.25, current)
    risk_per_share = max(aggressive_low - max(stop, 0), 0)
    risk_capital = account_size * risk_fraction
    units = int(risk_capital / risk_per_share) if risk_per_share > 0 else 0

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
            "macd": macd_info,
            "stochastic": stochastic_info,
            "atr14": atr14,
            "volume_ratio": round(volume_ratio, 2),
            "relative_volume": round(volume_ratio, 2),
            "volume_dryup_ratio": round(volume_dryup_ratio, 2),
            "obv": obv_info,
            "breakout_20d": breakout_fast,
            "breakout_55d": breakout_slow,
            "breakout_fast_label": profile["breakout_fast_label"],
            "breakout_slow_label": profile["breakout_slow_label"],
            "breakout_level": round(breakout_level, 2),
            "relative_strength": rs,
            "rs_rating": rs_rating,
            "extended_from_20ema_pct": round(extended_from_fast, 2),
            "base_quality": base_quality,
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
                "entry_reference": round(aggressive_low, 2),
                "risk_per_share": round(risk_per_share, 2),
                "suggested_quantity": units,
                "approx_position_value": round(units * aggressive_low, 2),
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
    breadth = float(market.get("breadth_above_50dma", market.get("advance_decline_breadth_pct", 50)))
    sector_strength = float(market.get("sector_strength", 50))
    vix = float(market.get("vix", 15))
    vix_score = score_inverse(vix, 11, 24)
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
    adjusted_total = int(clamp(total - (8 if is_stale else 0)))
    adjusted_regime = "Risk-on" if adjusted_total >= 70 else "Neutral" if adjusted_total >= 45 else "Risk-off"
    return {
        "score": adjusted_total,
        "raw_score": total,
        "regime": adjusted_regime,
        "raw_regime": regime,
        "nifty_close": round(nifty_close, 2),
        "nifty_ema50": round(ema50, 2),
        "nifty_dma200": round(dma200, 2),
        "breadth_above_50dma": breadth,
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
        "stale_penalty": 8 if is_stale else 0,
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
) -> dict[str, Any]:
    penalties: dict[str, float] = {}
    debt = float(fundamentals.get("debt_equity", 0) or 0)
    pledge = float(fundamentals.get("pledge_percent", 0) or 0)
    cfo_pat = fundamentals.get("cfo_pat")
    if debt > 1.2:
        penalties["high_debt"] = min((debt - 1.2) * 8, 14)
    if pledge > 0:
        penalties["promoter_pledge"] = min(pledge * 0.8, 18)
    if cfo_pat is not None and float(cfo_pat) < 0.65:
        penalties["weak_cash_conversion"] = min((0.65 - float(cfo_pat)) * 35, 12)
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
    if technical_result.get("fake_breakout_flags"):
        penalties["fake_breakout_risk"] = min(len(technical_result["fake_breakout_flags"]) * 4, 12)
    total = int(round(sum(penalties.values())))
    return {"score": int(clamp(total)), "breakdown": {k: round(v, 2) for k, v in penalties.items()}}


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
        "weekly_margin_above_high": weekly_margin,
        "monthly_margin_above_high": monthly_margin,
        "min_margin_above_high": min_margin,
    }


def final_decision(
    company: dict[str, Any],
    bars: list[dict[str, Any]],
    benchmark_bars: list[dict[str, Any]],
    market_result: dict[str, Any],
    account_size: float = 1_000_000,
    portfolio_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    business = business_quality_score(company["fundamentals"])
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
        event_context={**event_volume_context(event_result.get("events", [])), "rs_rating": company.get("rs_rating")},
    )
    monthly_technical = technical_strength_score(
        bars,
        benchmark_bars,
        market_result.get("regime", "Neutral"),
        bool(monthly_event_result.get("negative_governance")),
        timeframe="monthly",
        account_size=account_size,
        event_context={**event_volume_context(monthly_event_result.get("events", [])), "rs_rating": company.get("rs_rating")},
    )
    risk = risk_penalty(company["fundamentals"], event_result, technical, market_result)
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
    portfolio_check = portfolio_concentration_check(company, portfolio_context or {})
    candidate = all(five_questions.values()) and market_result["score"] >= 45 and data_complete_enough
    risk_off_quality_gate = True
    if market_result.get("regime") == "Risk-off":
        risk_off_quality_gate = business["score"] >= 78 and risk["score"] <= 20
        candidate = candidate and risk_off_quality_gate
    failed_gates = [key for key, passed in five_questions.items() if not passed]
    if market_result["score"] < 45:
        failed_gates.append("market_support")
    if not data_complete_enough:
        failed_gates.append("data_completeness_below_40pct")
    if not risk_off_quality_gate:
        failed_gates.append("risk_off_quality_gate")
    if not portfolio_check["pass"]:
        candidate = False
        failed_gates.append(portfolio_check["reason"])
    entry = {**technical["entry"]}
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
        "trade_state": trade_state,
        "entry": entry,
        "portfolio_check": portfolio_check,
        "exit_rules": technical["exit_rules"],
        "explanation_json": explanation,
        "fundamentals": company["fundamentals"],
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
