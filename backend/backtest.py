from __future__ import annotations

from statistics import mean
from typing import Any

from scoring_engine import final_decision, market_support_score, pct_distance


def _date_key(value: Any) -> str:
    return str(value or "")[:10]


def point_in_time_fundamentals(dataset: dict[str, Any], symbol: str, signal_date: str) -> dict[str, Any] | None:
    snapshots = dataset.get("fundamentals_snapshots") or dataset.get("fundamentals_snapshot") or []
    valid: list[dict[str, Any]] = []
    for row in snapshots:
        if str(row.get("symbol", "")).upper() != symbol.upper():
            continue
        knowledge_date = _date_key(row.get("knowledge_date") or row.get("as_of"))
        if knowledge_date and knowledge_date <= signal_date:
            valid.append(row)
    if not valid:
        return None
    latest = sorted(valid, key=lambda row: (_date_key(row.get("knowledge_date") or row.get("as_of")), _date_key(row.get("effective_date") or row.get("as_of"))))[-1]
    return {key: value for key, value in latest.items() if key not in {"symbol", "knowledge_date", "effective_date", "as_of"}}


def evaluate_signal(
    entry_price: float,
    stop_price: float,
    future_bars: list[dict[str, Any]],
    target_pct: float,
) -> dict[str, Any]:
    target_price = entry_price * (1 + target_pct / 100)
    for day, bar in enumerate(future_bars, start=1):
        low = float(bar["low"])
        high = float(bar["high"])
        if stop_price > 0 and low <= stop_price:
            return {
                "outcome": "stopped",
                "days_held": day,
                "exit_price": round(stop_price, 2),
                "return_pct": round(pct_distance(stop_price, entry_price), 2),
            }
        if high >= target_price:
            return {
                "outcome": "target_hit",
                "days_held": day,
                "exit_price": round(target_price, 2),
                "return_pct": round(target_pct, 2),
            }
    final_close = float(future_bars[-1]["close"]) if future_bars else entry_price
    return {
        "outcome": "horizon_close",
        "days_held": len(future_bars),
        "exit_price": round(final_close, 2),
        "return_pct": round(pct_distance(final_close, entry_price), 2),
    }


def run_backtest(
    dataset: dict[str, Any],
    horizon_days: int = 20,
    target_pct: float = 8.0,
    min_history_days: int = 220,
    signal_threshold: int = 70,
) -> dict[str, Any]:
    """Simple rolling validation harness.

    This is intentionally small and deterministic. It is meant to be fed real
    historical bars later; on seed data the output is only a smoke test.
    """
    is_seed_data = dataset.get("data_mode") == "seed_generated_demo" or dataset.get("is_seed_data") is True
    if is_seed_data:
        return {
            "config": {
                "horizon_days": horizon_days,
                "target_pct": target_pct,
                "min_history_days": min_history_days,
                "signal_threshold": signal_threshold,
            },
            "summary": {
                "signals": None,
                "wins": None,
                "stopped": None,
                "win_rate_pct": None,
                "stop_rate_pct": None,
                "average_return_pct": None,
            },
            "signals": [],
            "refused": True,
            "warning": "Backtest statistics are disabled for seed/generated bars. Upload real NSE historical OHLCV before computing win rate, stop rate, or average return.",
        }
    market_result = market_support_score(dataset["market"])
    benchmark = dataset.get("benchmark_bars") or []
    signals: list[dict[str, Any]] = []
    pit_enabled = bool(dataset.get("fundamentals_snapshots") or dataset.get("fundamentals_snapshot"))

    for company in dataset.get("companies", []):
        symbol = company["symbol"]
        bars = dataset.get("bars", {}).get(symbol, [])
        idx = min_history_days
        while idx < len(bars) - horizon_days:
            history = bars[: idx + 1]
            benchmark_history = benchmark[: idx + 1] if benchmark else []
            signal_date = _date_key(history[-1].get("datetime"))
            scoped_company = {**company, "fundamentals": dict(company.get("fundamentals", {}))}
            pit_fundamentals = point_in_time_fundamentals(dataset, symbol, signal_date)
            if pit_fundamentals:
                scoped_company["fundamentals"].update(pit_fundamentals)
            try:
                scored = final_decision(scoped_company, history, benchmark_history, market_result)
            except (KeyError, ValueError, ZeroDivisionError):
                idx += 1
                continue

            qualifies = (
                scored.get("candidate")
                and scored.get("weekly_score", 0) >= signal_threshold
                and scored.get("conviction") in {"High", "Watchlist"}
            )
            if not qualifies:
                idx += 1
                continue

            entry_price = float(scored["price"])
            stop_price = float(scored.get("entry", {}).get("stop") or 0)
            result = evaluate_signal(entry_price, stop_price, bars[idx + 1 : idx + 1 + horizon_days], target_pct)
            signals.append(
                {
                    "symbol": symbol,
                    "signal_date": history[-1]["datetime"],
                    "point_in_time_fundamentals": bool(pit_fundamentals),
                    "weekly_score": scored["weekly_score"],
                    "monthly_score": scored["monthly_score"],
                    "conviction": scored["conviction"],
                    "entry_price": round(entry_price, 2),
                    "stop_price": round(stop_price, 2),
                    **result,
                }
            )
            idx += horizon_days

    wins = [signal for signal in signals if signal["outcome"] == "target_hit"]
    stopped = [signal for signal in signals if signal["outcome"] == "stopped"]
    returns = [float(signal["return_pct"]) for signal in signals]
    return {
        "config": {
            "horizon_days": horizon_days,
            "target_pct": target_pct,
            "min_history_days": min_history_days,
            "signal_threshold": signal_threshold,
        },
        "summary": {
            "signals": len(signals),
            "wins": len(wins),
            "stopped": len(stopped),
            "win_rate_pct": round(len(wins) / len(signals) * 100, 2) if signals else 0,
            "stop_rate_pct": round(len(stopped) / len(signals) * 100, 2) if signals else 0,
            "average_return_pct": round(mean(returns), 2) if returns else 0,
        },
        "signals": signals[:200],
        "warning": (
            "Point-in-time fundamentals were used."
            if pit_enabled
            else "No point-in-time fundamentals supplied; backtest uses current company fundamentals and can contain look-ahead bias."
        ),
    }
