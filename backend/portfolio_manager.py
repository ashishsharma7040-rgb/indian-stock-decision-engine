from __future__ import annotations

import math
from statistics import mean, stdev
from typing import Any


def _daily_returns_from_bars(bars: list[dict[str, Any]], lookback: int = 60) -> list[float]:
    closes = []
    for bar in bars or []:
        try:
            closes.append(float(bar["close"]))
        except (KeyError, TypeError, ValueError):
            continue
    returns = [(current / prev - 1) if prev else 0.0 for prev, current in zip(closes[:-1], closes[1:])]
    return returns[-lookback:]


def _pearson(left: list[float], right: list[float]) -> float | None:
    count = min(len(left), len(right))
    if count < 20:
        return None
    x = left[-count:]
    y = right[-count:]
    sx = stdev(x) if count > 1 else 0.0
    sy = stdev(y) if count > 1 else 0.0
    if sx == 0 or sy == 0:
        return None
    mx = mean(x)
    my = mean(y)
    cov = sum((a - mx) * (b - my) for a, b in zip(x, y)) / (count - 1)
    return cov / (sx * sy)


class SectorCorrelationMatrix:
    def __init__(self, max_sector_exposure: float = 0.25) -> None:
        self.max_sector_exposure = max_sector_exposure

    def calculate_sector_headroom(
        self,
        target_sector: str,
        current_portfolio: list[dict[str, Any]],
        total_account_value: float,
    ) -> dict[str, Any]:
        if total_account_value <= 0:
            return {"status": "ERROR", "available_capital": 0.0, "current_exposure_pct": 0.0}
        exposure = 0.0
        for position in current_portfolio:
            if position.get("sector") == target_sector:
                value = position.get("current_value")
                if value is None:
                    value = float(position.get("shares_held", 0) or 0) * float(position.get("last_price", 0) or position.get("price", 0) or 0)
                exposure += float(value or 0)
        current_pct = exposure / total_account_value
        max_allowed = total_account_value * self.max_sector_exposure
        headroom = max(0.0, max_allowed - exposure)
        if current_pct >= self.max_sector_exposure:
            return {
                "status": "CAPPED",
                "available_capital": 0.0,
                "current_exposure_pct": round(current_pct * 100, 2),
                "reason": f"{target_sector} exposure at {round(current_pct * 100, 1)}% exceeds {round(self.max_sector_exposure * 100, 1)}% cap.",
            }
        return {
            "status": "OPEN",
            "available_capital": round(headroom, 2),
            "current_exposure_pct": round(current_pct * 100, 2),
            "max_sector_exposure_pct": round(self.max_sector_exposure * 100, 2),
        }


class CorrelationRiskEngine:
    def __init__(self, max_correlation: float = 0.65, caution_correlation: float = 0.40) -> None:
        self.max_correlation = max_correlation
        self.caution_correlation = caution_correlation

    def check_portfolio_correlation(
        self,
        target_returns: list[float],
        portfolio_holdings: list[dict[str, Any]],
        historical_bars_by_symbol: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        holdings = [item for item in portfolio_holdings if str(item.get("symbol", "")).upper() in historical_bars_by_symbol]
        if not holdings:
            return {"status": "APPROVED", "correlation": 0.0, "multiplier": 1.0, "reason": "No comparable active holdings."}
        values = []
        for item in holdings:
            value = item.get("current_value")
            if value is None:
                value = float(item.get("shares_held", 0) or 0) * float(item.get("last_price", 0) or item.get("price", 0) or 0)
            values.append(max(float(value or 0), 0.0))
        total_value = sum(values)
        if total_value <= 0:
            return {"status": "APPROVED", "correlation": 0.0, "multiplier": 1.0, "reason": "Portfolio values unavailable."}

        target_len = len(target_returns)
        if target_len < 20:
            return {"status": "UNKNOWN", "correlation": None, "multiplier": 0.8, "reason": "Insufficient target return history."}
        aggregate = [0.0] * target_len
        used = 0
        for item, value in zip(holdings, values):
            weight = value / total_value
            returns = _daily_returns_from_bars(historical_bars_by_symbol[str(item["symbol"]).upper()], target_len + 1)
            count = min(len(returns), target_len)
            if count < 20:
                continue
            offset = target_len - count
            for idx, ret in enumerate(returns[-count:]):
                aggregate[offset + idx] += ret * weight
            used += 1
        if used == 0:
            return {"status": "UNKNOWN", "correlation": None, "multiplier": 0.8, "reason": "No usable holding return history."}
        corr = _pearson(target_returns, aggregate)
        if corr is None or math.isnan(corr):
            return {"status": "UNKNOWN", "correlation": None, "multiplier": 0.8, "reason": "Correlation could not be calculated."}
        if corr >= self.max_correlation:
            return {
                "status": "REJECTED_HIGH_CORRELATION",
                "correlation": round(corr, 3),
                "multiplier": 0.0,
                "reason": f"Target moves too closely with current portfolio (r={round(corr, 2)}).",
            }
        if corr >= self.caution_correlation:
            return {
                "status": "CAUTION_CORRELATED",
                "correlation": round(corr, 3),
                "multiplier": 0.5,
                "reason": f"Target has moderate portfolio correlation (r={round(corr, 2)}); size is cut.",
            }
        return {
            "status": "APPROVED",
            "correlation": round(corr, 3),
            "multiplier": 1.0,
            "reason": "Statistically diversified alpha.",
        }


def portfolio_risk_audit(
    company: dict[str, Any],
    bars: list[dict[str, Any]],
    context: dict[str, Any],
    account_size: float,
) -> dict[str, Any]:
    holdings = context.get("holdings") or []
    total_equity = float(context.get("total_account_value") or context.get("total_equity") or account_size)
    sector_matrix = SectorCorrelationMatrix(float(context.get("max_sector_exposure", 0.25)))
    headroom = sector_matrix.calculate_sector_headroom(str(company.get("sector") or "Unclassified"), holdings, total_equity)
    target_returns = _daily_returns_from_bars(bars, int(context.get("correlation_lookback", 60)))
    corr = CorrelationRiskEngine(
        float(context.get("max_correlation", 0.65)),
        float(context.get("caution_correlation", 0.40)),
    ).check_portfolio_correlation(target_returns, holdings, context.get("historical_bars_by_symbol") or {})
    pass_gate = headroom.get("status") != "CAPPED" and corr.get("status") != "REJECTED_HIGH_CORRELATION"
    multiplier = min(float(corr.get("multiplier", 1.0) or 0.0), 1.0)
    return {
        "pass": pass_gate,
        "sector_headroom": headroom,
        "correlation": corr,
        "position_multiplier": multiplier,
        "reason": headroom.get("reason") or corr.get("reason") or "Portfolio risk matrix passed.",
    }
