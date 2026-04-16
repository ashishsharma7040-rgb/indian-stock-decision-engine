from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import mean
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
    if len(values) <= period:
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


def latest_close(bars: list[dict[str, Any]]) -> float:
    return float(bars[-1]["close"]) if bars else 0.0


def pct_distance(value: float, base: float) -> float:
    if base == 0:
        return 0.0
    return (value - base) / base * 100


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
    return {"score": total, "breakdown": breakdown, "valuation_score": int(round(valuation_score))}


def sector_tailwind_score(tailwind: dict[str, Any]) -> dict[str, Any]:
    total, breakdown = weighted_score(
        {
            "demand_trend": (tailwind.get("demand_trend", 50), 30),
            "policy_support": (tailwind.get("policy_support", 50), 20),
            "cost_environment": (tailwind.get("cost_environment", 50), 15),
            "order_visibility": (tailwind.get("order_visibility", 50), 20),
            "sector_momentum": (tailwind.get("sector_momentum", 50), 15),
        }
    )
    return {"score": total, "breakdown": breakdown}


SOURCE_RELIABILITY = {
    "exchange_filing": 1.0,
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


def score_single_event(event: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    sentiment = float(event.get("sentiment", 0))
    importance = clamp(float(event.get("importance", 50)), 0, 100)
    days_old = parse_event_time(event, now)
    freshness = clamp(math.exp(-days_old / 30), 0.18, 1.0)
    source_type = str(event.get("source_type", "credible_news"))
    reliability = SOURCE_RELIABILITY.get(source_type, 0.45)
    impact = sentiment * freshness * reliability * importance
    return {
        **event,
        "days_old": days_old,
        "freshness": round(freshness, 3),
        "reliability": reliability,
        "net_score": round(impact, 2),
    }


def event_strength_score(events: list[dict[str, Any]]) -> dict[str, Any]:
    scored = [score_single_event(event) for event in events]
    if not scored:
        return {"score": 50, "events": [], "negative_governance": False}
    denominator = sum(abs(float(e["freshness"]) * float(e["reliability"]) * float(e.get("importance", 50))) for e in scored)
    normalized = 0.0 if denominator == 0 else sum(float(e["net_score"]) for e in scored) / denominator
    score = int(round(clamp(50 + normalized * 50)))
    negative_governance = any(
        float(e.get("sentiment", 0)) < -0.5
        and str(e.get("category", "")).lower() in {"governance", "auditor", "pledge", "fraud", "management_exit"}
        for e in scored
    )
    return {"score": score, "events": scored, "negative_governance": negative_governance}


def relative_strength(stock_bars: list[dict[str, Any]], benchmark_bars: list[dict[str, Any]] | None) -> dict[str, Any]:
    if not benchmark_bars or len(stock_bars) < 55 or len(benchmark_bars) < 55:
        return {"pct": 0.0, "state": "Unknown"}
    stock_return = pct_distance(float(stock_bars[-1]["close"]), float(stock_bars[-55]["close"]))
    benchmark_return = pct_distance(float(benchmark_bars[-1]["close"]), float(benchmark_bars[-55]["close"]))
    diff = stock_return - benchmark_return
    state = "Leadership" if diff >= 8 else "Positive" if diff >= 2 else "Neutral" if diff >= -2 else "Weak"
    return {"pct": round(diff, 2), "state": state}


def technical_strength_score(
    bars: list[dict[str, Any]],
    benchmark_bars: list[dict[str, Any]] | None = None,
    market_regime: str = "Neutral",
    negative_event: bool = False,
) -> dict[str, Any]:
    if len(bars) < 60:
        raise ValueError("At least 60 bars are required for technical scoring")
    closes = [float(bar["close"]) for bar in bars]
    volumes = [float(bar["volume"]) for bar in bars]
    current = closes[-1]
    ema20 = ema(closes, 20)[-1]
    ema50 = ema(closes, 50)[-1]
    dma200 = sma(closes, 200)[-1]
    rsi14 = rsi(closes, 14)
    macd_info = macd(closes)
    atr14 = atr(bars, 14)
    avg_vol = mean(volumes[-21:-1]) if len(volumes) >= 22 else mean(volumes)
    volume_ratio = volumes[-1] / avg_vol if avg_vol else 1.0
    prior_20_high = max(float(bar["high"]) for bar in bars[-21:-1])
    prior_55_high = max(float(bar["high"]) for bar in bars[-56:-1])
    breakout_20 = current > prior_20_high
    breakout_55 = current > prior_55_high
    rs = relative_strength(bars, benchmark_bars)

    checks = {
        "price_above_20ema": current > ema20,
        "price_above_50ema": current > ema50,
        "price_above_200dma": current > dma200,
        "ema50_above_200dma": ema50 > dma200,
        "twenty_day_breakout": breakout_20,
        "fiftyfive_day_breakout": breakout_55,
        "volume_confirmed": volume_ratio >= 1.8,
        "rsi_healthy": 55 <= rsi14 <= 72,
        "macd_bullish": macd_info["state"] == "Bullish",
        "relative_strength_positive": rs["state"] in {"Positive", "Leadership"},
    }
    weights = {
        "price_above_20ema": 8,
        "price_above_50ema": 10,
        "price_above_200dma": 10,
        "ema50_above_200dma": 10,
        "twenty_day_breakout": 8,
        "fiftyfive_day_breakout": 12,
        "volume_confirmed": 12,
        "rsi_healthy": 10,
        "macd_bullish": 8,
        "relative_strength_positive": 12,
    }
    score = sum(weights[key] for key, ok in checks.items() if ok)
    extended_from_20 = pct_distance(current, ema20)
    fake_breakout_flags: list[str] = []
    if (breakout_20 or breakout_55) and volume_ratio < 1.3:
        fake_breakout_flags.append("Breakout lacks volume confirmation")
        score -= 8
    if extended_from_20 > 12:
        fake_breakout_flags.append("Price is stretched more than 12 percent above 20 EMA")
        score -= 10
    if market_regime == "Risk-off" and (breakout_20 or breakout_55):
        fake_breakout_flags.append("Broader market is risk-off")
        score -= 8
    if negative_event:
        fake_breakout_flags.append("Fresh negative event conflicts with price strength")
        score -= 12
    if rsi14 > 78:
        fake_breakout_flags.append("RSI is overheated")
        score -= 8

    breakout_level = prior_55_high if breakout_55 else prior_20_high
    recent_low = min(float(bar["low"]) for bar in bars[-10:])
    stop_by_atr = current - atr14 * 1.7
    stop = min(recent_low, stop_by_atr)
    invalidation = min(stop, ema50 * 0.985)
    aggressive_low = max(current, breakout_level * 1.001)
    aggressive_high = aggressive_low + max(atr14 * 0.7, current * 0.012)
    pullback_low = max(min(ema20, breakout_level) - atr14 * 0.25, 0)
    pullback_high = min(max(ema20, breakout_level) + atr14 * 0.25, current)

    return {
        "score": int(clamp(round(score))),
        "indicators": {
            "close": round(current, 2),
            "ema20": round(ema20, 2),
            "ema50": round(ema50, 2),
            "dma200": round(dma200, 2),
            "rsi14": rsi14,
            "macd": macd_info,
            "atr14": atr14,
            "volume_ratio": round(volume_ratio, 2),
            "breakout_20d": breakout_20,
            "breakout_55d": breakout_55,
            "breakout_level": round(breakout_level, 2),
            "relative_strength": rs,
            "extended_from_20ema_pct": round(extended_from_20, 2),
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
    breadth = float(market.get("breadth_above_50dma", 50))
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
    return {
        "score": total,
        "regime": regime,
        "nifty_close": round(nifty_close, 2),
        "nifty_ema50": round(ema50, 2),
        "nifty_dma200": round(dma200, 2),
        "breadth_above_50dma": breadth,
        "sector_strength": sector_strength,
        "vix": vix,
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


def final_decision(
    company: dict[str, Any],
    bars: list[dict[str, Any]],
    benchmark_bars: list[dict[str, Any]],
    market_result: dict[str, Any],
) -> dict[str, Any]:
    business = business_quality_score(company["fundamentals"])
    tailwind = sector_tailwind_score(company["tailwind"])
    event_result = event_strength_score(company.get("events", []))
    technical = technical_strength_score(
        bars,
        benchmark_bars,
        market_result.get("regime", "Neutral"),
        bool(event_result.get("negative_governance")),
    )
    risk = risk_penalty(company["fundamentals"], event_result, technical, market_result)
    valuation = business["valuation_score"]
    weekly = (
        technical["score"] * 0.35
        + event_result["score"] * 0.20
        + tailwind["score"] * 0.15
        + business["score"] * 0.10
        + market_result["score"] * 0.20
        - risk["score"]
    )
    monthly = (
        business["score"] * 0.30
        + tailwind["score"] * 0.20
        + event_result["score"] * 0.20
        + technical["score"] * 0.20
        + valuation * 0.10
        - risk["score"]
    )
    five_questions = {
        "good_business": business["score"] >= 65,
        "sector_helping_now": tailwind["score"] >= 60,
        "fresh_trigger": event_result["score"] >= 55,
        "chart_confirming": technical["score"] >= 60,
        "where_am_i_wrong_defined": risk["score"] <= 35 and bool(technical.get("entry")),
    }
    candidate = all(five_questions.values()) and market_result["score"] >= 45
    if market_result.get("regime") == "Risk-off":
        candidate = candidate and business["score"] >= 78 and risk["score"] <= 20
    conviction = "High" if candidate and weekly >= 75 and monthly >= 75 else "Watchlist" if weekly >= 60 or monthly >= 60 else "Avoid"
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
        "change_pct": round(pct_distance(technical["indicators"]["close"], float(bars[-2]["close"])), 2),
        "weekly_score": int(clamp(round(weekly))),
        "monthly_score": int(clamp(round(monthly))),
        "business_quality": business,
        "sector_tailwind": tailwind,
        "event_strength": event_result,
        "technical_strength": technical,
        "market_support": market_result,
        "risk_penalty": risk,
        "candidate": candidate,
        "conviction": conviction,
        "entry": technical["entry"],
        "exit_rules": technical["exit_rules"],
        "explanation_json": explanation,
        "fundamentals": company["fundamentals"],
        "tailwind_factors": company.get("tailwind_factors", []),
        "bars": bars[-120:],
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
    lines = [
        f"{company['symbol']} business quality is {business['score']}/100 with cash flow and promoter checks included.",
        f"Sector tailwind is {tailwind['score']}/100 and market regime is {market_result['regime']}.",
        f"Event layer scores {event_result['score']}/100 after source reliability and freshness weighting.",
        f"Technical score is {technical['score']}/100 with breakout level {technical['entry']['breakout_level']}.",
    ]
    if risk["score"] > 0:
        lines.append(f"Risk penalty is {risk['score']}/100 from {', '.join(risk['breakdown'].keys()) or 'general risk checks'}.")
    else:
        lines.append("No major rule-based risk penalty is active.")
    return lines
