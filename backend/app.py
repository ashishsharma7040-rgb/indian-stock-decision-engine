from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from data_sources import (
    OFFICIAL_FILING_LINKS,
    fetch_alpha_vantage_daily,
    fetch_no_key_news,
    fetch_newsapi,
    fetch_yahoo_chart,
    fetch_yahoo_intraday,
    recommended_api_stack,
)
from scoring_engine import final_decision, market_support_score
from seed_data import build_demo_dataset


APP_NAME = "Indian Stock Decision Engine"
DATASET = build_demo_dataset()
TRADE_STATES: dict[str, dict[str, Any]] = {}

app = FastAPI(
    title=APP_NAME,
    version="1.0.0",
    description="Broker-free, AI-assisted, rules-first stock research engine for Indian equities.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def build_scored_universe() -> list[dict[str, Any]]:
    market = market_support_score(DATASET["market"])
    scored: list[dict[str, Any]] = []
    for company in DATASET["companies"]:
        bars = DATASET["bars"][company["symbol"]]
        scored.append(final_decision(company, bars, DATASET["benchmark_bars"], market))
    return scored


def find_company(symbol: str) -> dict[str, Any] | None:
    symbol = symbol.upper()
    return next((company for company in DATASET["companies"] if company["symbol"] == symbol), None)


def advance_trade_state(scored: dict[str, Any]) -> dict[str, Any]:
    symbol = scored["symbol"]
    now = datetime.now(timezone.utc).isoformat()
    computed = scored.get("trade_state", {})
    state = TRADE_STATES.get(symbol)
    if state is None:
        state = {
            "symbol": symbol,
            "state": computed.get("state", "Screened"),
            "created_at": now,
            "updated_at": now,
            "breakout_level": computed.get("breakout_level"),
            "entry_price": None,
            "stop": computed.get("stop"),
            "last_price": computed.get("last_price"),
            "history": [{"timestamp": now, "state": computed.get("state", "Screened"), "reason": computed.get("reason")}],
        }
        TRADE_STATES[symbol] = state
    else:
        previous = state["state"]
        price = float(computed.get("last_price", 0) or 0)
        breakout = float(state.get("breakout_level") or computed.get("breakout_level") or 0)
        stop = float(state.get("stop") or computed.get("stop") or 0)
        if previous in {"Screened", "Watchlist"} and computed.get("state") in {"Watchlist", "Triggered"}:
            state["state"] = "Triggered" if breakout and price >= breakout else computed.get("state", previous)
        if previous == "Triggered" and state["entry_price"] is None:
            state["entry_price"] = price
            state["state"] = "In Trade"
        if previous == "In Trade" and stop and price <= stop:
            state["state"] = "Exited"
        state["updated_at"] = now
        state["last_price"] = price
        state["stop"] = stop
        if state["state"] != previous:
            state.setdefault("history", []).append({"timestamp": now, "state": state["state"], "reason": computed.get("reason")})
    return state


def stock_summary(stock: dict[str, Any]) -> dict[str, Any]:
    technical = stock["technical_strength"]
    business = stock["business_quality"]
    tailwind = stock["sector_tailwind"]
    events = stock["event_strength"]
    risk = stock["risk_penalty"]
    return {
        "symbol": stock["symbol"],
        "name": stock["name"],
        "sector": stock["sector"],
        "industry": stock["industry"],
        "price": stock["price"],
        "change_pct": stock["change_pct"],
        "weekly_score": stock["weekly_score"],
        "monthly_score": stock["monthly_score"],
        "weekly_raw_score": stock.get("weekly_raw_score"),
        "monthly_raw_score": stock.get("monthly_raw_score"),
        "business_score": business["score"],
        "tailwind_score": tailwind["score"],
        "event_score": events["score"],
        "technical_score": technical["score"],
        "monthly_technical_score": stock.get("monthly_technical_strength", {}).get("score"),
        "market_score": stock["market_support"]["score"],
        "risk_score": risk["score"],
        "conviction": stock["conviction"],
        "candidate": stock["candidate"],
        "trade_state": stock.get("trade_state"),
        "entry": stock["entry"],
        "risk_flags": stock["explanation_json"]["risk_flags"],
    }


def sector_summary(stocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for stock in stocks:
        grouped.setdefault(stock["sector"], []).append(stock)
    rows = []
    for sector, names in grouped.items():
        rows.append(
            {
                "sector": sector,
                "avg_weekly_score": round(sum(s["weekly_score"] for s in names) / len(names), 1),
                "avg_tailwind_score": round(sum(s["sector_tailwind"]["score"] for s in names) / len(names), 1),
                "leader": max(names, key=lambda s: s["weekly_score"])["symbol"],
                "count": len(names),
            }
        )
    return sorted(rows, key=lambda row: row["avg_weekly_score"], reverse=True)


def critical_events(stocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = []
    for stock in stocks:
        for event in stock["event_strength"]["events"]:
            if abs(float(event.get("sentiment", 0))) >= 0.35 or float(event.get("importance", 0)) >= 70:
                events.append(
                    {
                        "symbol": stock["symbol"],
                        "title": event["title"],
                        "source": event.get("source"),
                        "source_type": event.get("source_type"),
                        "days_old": event.get("days_old"),
                        "sentiment": event.get("sentiment"),
                        "importance": event.get("importance"),
                        "net_score": event.get("net_score"),
                    }
                )
    return sorted(events, key=lambda item: abs(float(item["net_score"])), reverse=True)[:12]


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "app": APP_NAME,
        "status": "ok",
        "frontend": "Open ../frontend/index.html or serve it with python -m http.server",
        "docs": "/docs",
    }


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data_mode": "demo_seed_plus_optional_live_refresh",
    }


@app.get("/api/dashboard")
def dashboard() -> dict[str, Any]:
    stocks = build_scored_universe()
    market = market_support_score(DATASET["market"])
    top_weekly = sorted(stocks, key=lambda stock: stock["weekly_score"], reverse=True)[:3]
    top_monthly = sorted(stocks, key=lambda stock: stock["monthly_score"], reverse=True)[:3]
    avoid = [
        stock
        for stock in sorted(stocks, key=lambda item: (item["risk_penalty"]["score"], -item["weekly_score"]), reverse=True)
        if stock["risk_penalty"]["score"] >= 18 or stock["conviction"] == "Avoid"
    ][:5]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_regime": market,
        "top_weekly_candidates": [stock_summary(stock) for stock in top_weekly],
        "top_monthly_candidates": [stock_summary(stock) for stock in top_monthly],
        "top_sectors": sector_summary(stocks),
        "avoid_list": [stock_summary(stock) for stock in avoid],
        "latest_critical_events": critical_events(stocks),
        "stocks": [stock_summary(stock) for stock in sorted(stocks, key=lambda stock: stock["weekly_score"], reverse=True)],
        "latency_strategy": {
            "prices": "Refresh on demand through no-key Yahoo Finance 5-minute bars; use a licensed feed for production trading.",
            "events": "Refresh on demand through no-key Yahoo Finance RSS, Google News RSS, and GDELT; optional NewsAPI key supported.",
            "fundamentals": "Refresh daily after exchange/results updates.",
            "scores": "Refresh daily and immediately after major official event.",
        },
        "disclaimer": "Research workflow only. This app does not provide investment advice or guaranteed predictions.",
    }


@app.get("/api/stocks")
def list_stocks() -> dict[str, Any]:
    return {"stocks": [stock_summary(stock) for stock in build_scored_universe()]}


@app.get("/api/stocks/{symbol}")
def stock_detail(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    stocks = build_scored_universe()
    for stock in stocks:
        if stock["symbol"] == symbol:
            stock["trade_state"] = advance_trade_state(stock)
            return stock
    raise HTTPException(status_code=404, detail=f"{symbol} is not in the current universe")


@app.get("/api/trade-state/{symbol}")
def get_trade_state(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    detail = stock_detail(symbol)
    return {"symbol": symbol, "trade_state": detail["trade_state"]}


@app.post("/api/trade-state/{symbol}")
def update_trade_state(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    symbol = symbol.upper()
    if not find_company(symbol):
        raise HTTPException(status_code=404, detail=f"{symbol} is not in the current universe")
    now = datetime.now(timezone.utc).isoformat()
    state = TRADE_STATES.get(symbol, {"symbol": symbol, "created_at": now, "history": []})
    for key in ("state", "entry_price", "stop", "breakout_level", "last_price", "notes"):
        if key in payload:
            state[key] = payload[key]
    state["updated_at"] = now
    state.setdefault("history", []).append({"timestamp": now, "state": state.get("state"), "reason": payload.get("reason", "Manual update")})
    TRADE_STATES[symbol] = state
    return {"symbol": symbol, "trade_state": state}


@app.post("/api/fundamentals/{symbol}")
def update_fundamentals(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    symbol = symbol.upper()
    company = find_company(symbol)
    if not company:
        raise HTTPException(status_code=404, detail=f"{symbol} is not in the current universe")
    allowed = {
        "sales_cagr",
        "profit_cagr",
        "roce",
        "roe",
        "debt_equity",
        "cfo_pat",
        "fcf_trend",
        "promoter_holding_trend",
        "pledge_percent",
        "dilution_flag",
        "margin_trend_bps",
        "pe",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="No supported fundamental fields supplied")
    company["fundamentals"].update(updates)
    market = market_support_score(DATASET["market"])
    bars = DATASET["bars"][symbol]
    scored = final_decision(company, bars, DATASET["benchmark_bars"], market)
    scored["trade_state"] = advance_trade_state(scored)
    return {"symbol": symbol, "updated_fields": sorted(updates), "stock": scored}


@app.post("/api/refresh/{symbol}")
def refresh_symbol(symbol: str) -> dict[str, Any]:
    """Best-effort live refresh.

    It keeps deterministic scoring, but tries no-key Yahoo Finance/news connectors first.
    If a connector fails, the endpoint falls back to seeded data.
    """
    symbol = symbol.upper()
    company = next((deepcopy(item) for item in DATASET["companies"] if item["symbol"] == symbol), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"{symbol} is not in the current universe")

    connector_notes: list[str] = []
    bars = []
    try:
        bars = fetch_yahoo_chart(symbol)
        if len(bars) >= 60:
            connector_notes.append("Yahoo Finance daily history loaded without API key")
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"Yahoo Finance daily history failed: {exc}")
    try:
        intraday_bars = fetch_yahoo_intraday(symbol)
        if bars and intraday_bars:
            latest = intraday_bars[-1]
            bars[-1]["close"] = latest["close"]
            bars[-1]["high"] = max(float(bars[-1]["high"]), float(latest["high"]))
            bars[-1]["low"] = min(float(bars[-1]["low"]), float(latest["low"]))
            bars[-1]["volume"] = max(int(bars[-1]["volume"]), int(latest.get("volume", 0) or 0))
            connector_notes.append("Latest Yahoo Finance 5-minute price merged into daily history")
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"Yahoo Finance intraday quote failed: {exc}")
    if len(bars) < 60:
        connector_notes.append("Yahoo Finance unavailable or insufficient bars")
        try:
            bars = fetch_alpha_vantage_daily(symbol)
            if len(bars) >= 60:
                connector_notes.append("Alpha Vantage daily bars loaded")
        except Exception as exc:  # pragma: no cover - network dependent
            connector_notes.append(f"Alpha Vantage fallback failed: {exc}")
            bars = []
    if len(bars) < 60:
        connector_notes.append("Using seeded OHLCV bars")
        bars = DATASET["bars"][symbol]

    news_events: list[dict[str, Any]] = []
    try:
        no_key_news = fetch_no_key_news(symbol, company["name"], max_records=8)
        news_events.extend(no_key_news)
        if no_key_news:
            connector_notes.append("No-key news loaded from Yahoo Finance RSS, Google News RSS, or GDELT")
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"No-key news failed: {exc}")
    try:
        news_events.extend(fetch_newsapi(f"{company['name']} India stock", max_records=5))
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"NewsAPI failed: {exc}")
    company["events"] = company.get("events", []) + news_events[:10]

    market = market_support_score(DATASET["market"])
    scored = final_decision(company, bars, DATASET["benchmark_bars"], market)
    scored["trade_state"] = advance_trade_state(scored)
    scored["connector_notes"] = connector_notes or ["Live refresh succeeded"]
    return scored


@app.post("/api/score")
def score_custom_stock(payload: dict[str, Any]) -> dict[str, Any]:
    required = {"company", "bars"}
    missing = required - set(payload)
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing fields: {', '.join(sorted(missing))}")
    market = market_support_score(payload.get("market") or DATASET["market"])
    benchmark = payload.get("benchmark_bars") or DATASET["benchmark_bars"]
    return final_decision(payload["company"], payload["bars"], benchmark, market)


@app.get("/api/apis")
def api_stack() -> dict[str, Any]:
    return {
        "recommended_stack": recommended_api_stack(),
        "official_filing_links": OFFICIAL_FILING_LINKS,
        "environment_variables": [
            "ALPHA_VANTAGE_API_KEY optional fallback",
            "NEWSAPI_API_KEY optional fallback",
            "ALPHA_VANTAGE_SYMBOL_<SYMBOL> for custom symbol mapping",
        ],
    }
