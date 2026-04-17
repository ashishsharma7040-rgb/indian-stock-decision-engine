from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backtest import run_backtest
from data_sources import (
    OFFICIAL_FILING_LINKS,
    fetch_alpha_vantage_daily,
    fetch_nse_advance_decline,
    fetch_no_key_news,
    fetch_newsapi,
    fetch_yahoo_chart,
    fetch_yahoo_intraday,
    recommended_api_stack,
)
from fundamental_import import parse_fundamentals_csv
from live_feed import live_feed
from scoring_engine import final_decision, market_support_score, pct_distance
from seed_data import build_demo_dataset


APP_NAME = "Indian Stock Decision Engine"
DATASET = build_demo_dataset()
TRADE_STATES: dict[str, dict[str, Any]] = {}
PORTFOLIO_HOLDINGS: list[dict[str, Any]] = []

SECTOR_PROXY_SYMBOLS = {
    "Technology": "^CNXIT",
    "Financials": "^NSEBANK",
    "Power": "^CNXENERGY",
    "Renewable Energy": "^CNXENERGY",
    "Industrials": "^CNXINFRA",
    "Manufacturing": "^CNXAUTO",
    "Diversified": "^NSEI",
}

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


def universe_symbols() -> list[str]:
    return [company["symbol"] for company in DATASET["companies"]]


def build_scored_universe() -> list[dict[str, Any]]:
    market = market_support_score(DATASET["market"])
    scored: list[dict[str, Any]] = []
    portfolio_context = {
        "holdings": PORTFOLIO_HOLDINGS,
        "max_sector_positions": 3,
        "max_industry_positions": 2,
    }
    for company in DATASET["companies"]:
        bars = DATASET["bars"][company["symbol"]]
        scored.append(final_decision(company, bars, DATASET["benchmark_bars"], market, portfolio_context=portfolio_context))
    return scored


def find_company(symbol: str) -> dict[str, Any] | None:
    symbol = symbol.upper()
    return next((company for company in DATASET["companies"] if company["symbol"] == symbol), None)


def apply_sector_rotation_scores(rotation: dict[str, dict[str, Any]]) -> None:
    for company in DATASET["companies"]:
        sector_score = rotation.get(company["sector"], {})
        if sector_score:
            company.setdefault("tailwind", {})["sector_rotation_score"] = sector_score["score"]


def compute_sector_rotation(nifty_bars: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rotation: dict[str, dict[str, Any]] = {}
    if len(nifty_bars) < 65:
        return rotation
    nifty_13w = pct_distance(float(nifty_bars[-1]["close"]), float(nifty_bars[-65]["close"]))
    for sector, yahoo_symbol in SECTOR_PROXY_SYMBOLS.items():
        try:
            bars = fetch_yahoo_chart(yahoo_symbol, range_value="1y", interval="1d")
        except Exception:
            continue
        if len(bars) < 85:
            continue
        sector_13w = pct_distance(float(bars[-1]["close"]), float(bars[-65]["close"]))
        sector_17w = pct_distance(float(bars[-20]["close"]), float(bars[-85]["close"]))
        rs = sector_13w - nifty_13w
        rs_delta = sector_13w - sector_17w
        score = max(0, min(100, 50 + rs * 3 + rs_delta * 2))
        rotation[sector] = {
            "proxy": yahoo_symbol,
            "score": round(score, 2),
            "rs_13w_vs_nifty": round(rs, 2),
            "rs_delta_4w": round(rs_delta, 2),
        }
    return rotation


def refresh_market_data() -> dict[str, Any]:
    notes: list[str] = []
    updated: dict[str, Any] = {}
    live_updates = 0
    try:
        nifty_bars = fetch_yahoo_chart("NIFTY", range_value="1y", interval="1d")
        if len(nifty_bars) >= 200:
            DATASET["benchmark_bars"] = nifty_bars
            updated["nifty_bars"] = nifty_bars
            updated["nifty_close"] = float(nifty_bars[-1]["close"])
            updated["source"] = "Yahoo Finance ^NSEI"
            live_updates += 1
            notes.append("Nifty 50 daily bars refreshed from Yahoo Finance")
            rotation = compute_sector_rotation(nifty_bars)
            if rotation:
                updated["sector_rotation"] = rotation
                updated["sector_strength"] = round(sum(item["score"] for item in rotation.values()) / len(rotation), 2)
                apply_sector_rotation_scores(rotation)
                notes.append("Sector rotation proxies refreshed")
    except Exception as exc:  # pragma: no cover - network dependent
        notes.append(f"Nifty refresh failed: {exc}")
    try:
        breadth = fetch_nse_advance_decline("NIFTY 50")
        breadth_source = breadth.pop("source", "NSE breadth")
        updated.update(breadth)
        updated["breadth_source"] = breadth_source
        updated["breadth_above_50dma"] = breadth["advance_decline_breadth_pct"]
        live_updates += 1
        notes.append("NSE advance/decline breadth refreshed")
    except Exception as exc:  # pragma: no cover - network dependent
        notes.append(f"NSE breadth refresh failed: {exc}")
    if live_updates:
        updated["updated_at"] = datetime.now(timezone.utc).isoformat()
        DATASET["market"].update(updated)
    else:
        notes.append("No live market connector succeeded; keeping previous market data")
    return {"market": market_support_score(DATASET["market"]), "notes": notes}


def scan_trade_alerts(refresh_market: bool = False) -> dict[str, Any]:
    if refresh_market:
        refresh_market_data()
    alerts: list[dict[str, Any]] = []
    for stock in build_scored_universe():
        previous = TRADE_STATES.get(stock["symbol"], {}).get("state")
        new_state = advance_trade_state(stock)
        if previous == "Watchlist" and new_state.get("state") in {"Triggered", "In Trade"}:
            alerts.append(
                {
                    "symbol": stock["symbol"],
                    "name": stock["name"],
                    "state": new_state["state"],
                    "price": stock["price"],
                    "breakout_level": stock["entry"].get("breakout_level"),
                    "stop": stock["entry"].get("stop"),
                    "message": f"{stock['symbol']} moved from Watchlist to {new_state['state']} near {stock['price']}",
                }
            )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "alerts": alerts,
        "telegram_ready": bool(alerts),
        "delivery_note": "Wire this response to Telegram/email from a scheduled job or Supabase Edge Function.",
    }


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
        "confidence_interval": stock.get("confidence_interval"),
        "portfolio_check": stock.get("portfolio_check"),
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
        "live_feed": live_feed.status(),
        "latency_strategy": {
            "live_feed": "Shoonya WebSocket pushes real-time ticks to /ws/live-prices when backend credentials are configured.",
            "market": "Call /api/market/refresh or /api/scheduled/daily after market close to refresh Nifty, breadth, and sector rotation.",
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


@app.get("/api/live/status")
def live_status() -> dict[str, Any]:
    if live_feed.configured:
        live_feed.start()
    return live_feed.status()


@app.post("/api/live/subscribe")
def live_subscribe(payload: dict[str, Any]) -> dict[str, Any]:
    symbols = payload.get("symbols") or universe_symbols()
    if not isinstance(symbols, list):
        raise HTTPException(status_code=400, detail="symbols must be a list")
    return live_feed.subscribe([str(symbol) for symbol in symbols])


@app.get("/api/live/snapshot")
def live_snapshot(symbols: str | None = None) -> dict[str, Any]:
    wanted = [item.strip().upper() for item in symbols.split(",")] if symbols else universe_symbols()
    return {"status": live_feed.status(), "ticks": live_feed.snapshot(wanted)}


@app.websocket("/ws/live-prices")
async def live_prices_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    raw_symbols = websocket.query_params.get("symbols", "")
    symbols = [item.strip().upper() for item in raw_symbols.split(",") if item.strip()] or universe_symbols()
    status = live_feed.subscribe(symbols)
    await websocket.send_json({"type": "status", "status": status, "symbols": symbols})
    last_sent: dict[str, tuple[Any, Any, Any]] = {}
    try:
        while True:
            ticks = live_feed.snapshot(symbols)
            updates = []
            for symbol, tick in ticks.items():
                key = (tick.get("timestamp"), tick.get("ltp"), tick.get("volume"))
                if last_sent.get(symbol) != key:
                    updates.append(tick)
                    last_sent[symbol] = key
            if updates:
                await websocket.send_json({"type": "ticks", "ticks": updates})
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return


@app.post("/api/market/refresh")
def refresh_market() -> dict[str, Any]:
    return refresh_market_data()


@app.get("/api/market")
def market_detail() -> dict[str, Any]:
    return market_support_score(DATASET["market"])


@app.post("/api/scan/alerts")
def scan_alerts(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    return scan_trade_alerts(refresh_market=bool(payload.get("refresh_market", False)))


@app.get("/api/scheduled/daily")
def scheduled_daily_scan() -> dict[str, Any]:
    market = refresh_market_data()
    alerts = scan_trade_alerts(refresh_market=False)
    return {"market_refresh": market, "alert_scan": alerts}


@app.get("/api/portfolio")
def get_portfolio() -> dict[str, Any]:
    return {"holdings": PORTFOLIO_HOLDINGS, "max_sector_positions": 3, "max_industry_positions": 2}


@app.post("/api/portfolio")
def update_portfolio(payload: dict[str, Any]) -> dict[str, Any]:
    holdings = payload.get("holdings", [])
    if not isinstance(holdings, list):
        raise HTTPException(status_code=400, detail="holdings must be a list")
    PORTFOLIO_HOLDINGS.clear()
    for holding in holdings:
        symbol = str(holding.get("symbol", "")).upper()
        company = find_company(symbol)
        if company:
            PORTFOLIO_HOLDINGS.append(
                {
                    "symbol": symbol,
                    "sector": company["sector"],
                    "industry": company.get("industry", ""),
                    "quantity": holding.get("quantity"),
                    "entry_price": holding.get("entry_price"),
                }
            )
    return get_portfolio()


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


@app.post("/api/fundamentals/{symbol}/screener-csv")
def import_screener_csv(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    csv_text = str(payload.get("csv_text") or payload.get("text") or "")
    if not csv_text.strip():
        raise HTTPException(status_code=400, detail="Send CSV text as csv_text")
    updates = parse_fundamentals_csv(csv_text)
    if not updates:
        raise HTTPException(status_code=400, detail="No supported fundamental fields were detected in the CSV")
    return update_fundamentals(symbol, updates)


@app.post("/api/tailwind/{symbol}")
def update_tailwind(symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
    symbol = symbol.upper()
    company = find_company(symbol)
    if not company:
        raise HTTPException(status_code=404, detail=f"{symbol} is not in the current universe")
    allowed = {
        "demand_trend",
        "policy_support",
        "cost_environment",
        "order_visibility",
        "sector_momentum",
    }
    updates = {key: value for key, value in payload.items() if key in allowed}
    if updates:
        company.setdefault("tailwind", {}).update(updates)
    if "tailwind_factors" in payload:
        if not isinstance(payload["tailwind_factors"], list):
            raise HTTPException(status_code=400, detail="tailwind_factors must be a list of strings")
        company["tailwind_factors"] = [str(item) for item in payload["tailwind_factors"]]
        updates["tailwind_factors"] = company["tailwind_factors"]
    if not updates:
        raise HTTPException(status_code=400, detail="No supported tailwind fields supplied")
    market = market_support_score(DATASET["market"])
    bars = DATASET["bars"][symbol]
    scored = final_decision(company, bars, DATASET["benchmark_bars"], market)
    scored["trade_state"] = advance_trade_state(scored)
    return {"symbol": symbol, "updated_fields": sorted(updates), "stock": scored}


@app.get("/api/backtest/demo")
def demo_backtest() -> dict[str, Any]:
    return run_backtest(DATASET)


@app.post("/api/backtest")
def custom_backtest(payload: dict[str, Any]) -> dict[str, Any]:
    dataset = {
        "companies": payload.get("companies") or DATASET["companies"],
        "bars": payload.get("bars") or DATASET["bars"],
        "benchmark_bars": payload.get("benchmark_bars") or DATASET["benchmark_bars"],
        "market": payload.get("market") or DATASET["market"],
    }
    return run_backtest(
        dataset,
        horizon_days=int(payload.get("horizon_days", 20)),
        target_pct=float(payload.get("target_pct", 8.0)),
        min_history_days=int(payload.get("min_history_days", 220)),
        signal_threshold=int(payload.get("signal_threshold", 70)),
    )


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
