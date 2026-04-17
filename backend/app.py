from __future__ import annotations

import asyncio
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backtest import run_backtest
from data_sources import (
    OFFICIAL_FILING_LINKS,
    fetch_alpha_vantage_daily,
    fetch_latest_nse_bhavcopy,
    fetch_nse_advance_decline,
    fetch_nse_equity_master,
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
from universe_store import UNIVERSE_STORE

try:
    from bhavcopy_loader import database_counts, run_eod_update
    from database import DATABASE_ENABLED, database_status, init_db
except Exception as exc:  # pragma: no cover - optional DB dependencies
    DATABASE_ENABLED = False
    DB_IMPORT_ERROR = str(exc)
    database_counts = None  # type: ignore
    run_eod_update = None  # type: ignore
    init_db = None  # type: ignore

    def database_status() -> dict[str, Any]:  # type: ignore
        return {"enabled": False, "url_configured": bool(os.getenv("DATABASE_URL")), "import_error": DB_IMPORT_ERROR}


APP_NAME = "Indian Stock Decision Engine"
DATASET = build_demo_dataset()
UNIVERSE_STORE.load_seed_companies(DATASET["companies"])
TRADE_STATES: dict[str, dict[str, Any]] = {}
PORTFOLIO_HOLDINGS: list[dict[str, Any]] = []
EOD_TASK_STATUS: dict[str, Any] = {"status": "idle", "updated_at": None, "result": None, "error": None}

NEUTRAL_FUNDAMENTALS = {
    "sales_cagr": None,
    "profit_cagr": None,
    "roce": None,
    "roe": None,
    "debt_equity": None,
    "cfo_pat": None,
    "fcf_trend": None,
    "promoter_holding_trend": "stable",
    "pledge_percent": 0,
    "dilution_flag": False,
    "margin_trend_bps": None,
    "pe": None,
}

NEUTRAL_TAILWIND = {
    "demand_trend": 50,
    "policy_support": 50,
    "cost_environment": 50,
    "order_visibility": 50,
    "sector_momentum": 50,
}

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


def refresh_nse_universe() -> dict[str, Any]:
    notes: list[str] = []
    refreshed: dict[str, Any] = {}
    try:
        master = fetch_nse_equity_master()
        refreshed["equity_master_count"] = UNIVERSE_STORE.refresh_equity_master(master)
        notes.append(f"NSE equity master loaded {refreshed['equity_master_count']} symbols")
    except Exception as exc:  # pragma: no cover - network dependent
        notes.append(f"NSE equity master refresh failed: {exc}")
        UNIVERSE_STORE.set_error(str(exc))
    try:
        bhavcopy = fetch_latest_nse_bhavcopy()
        refreshed["bhavcopy_count"] = UNIVERSE_STORE.refresh_bhavcopy(bhavcopy)
        refreshed["bhavcopy_as_of"] = bhavcopy.get("as_of")
        notes.append(f"NSE bhavcopy loaded {refreshed['bhavcopy_count']} EOD rows for {bhavcopy.get('as_of')}")
    except Exception as exc:  # pragma: no cover - network dependent
        notes.append(f"NSE bhavcopy refresh failed: {exc}")
        UNIVERSE_STORE.set_error(str(exc))
    return {"universe": UNIVERSE_STORE.meta(), "notes": notes, "refreshed": refreshed}


def ensure_universe_loaded() -> None:
    if UNIVERSE_STORE.count() <= len(DATASET["companies"]) + 5:
        try:
            master = fetch_nse_equity_master()
            UNIVERSE_STORE.refresh_equity_master(master)
        except Exception as exc:
            UNIVERSE_STORE.set_error(str(exc))
            return


def dashboard_focus(stocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    focus = [
        stock
        for stock in stocks
        if stock.get("candidate")
        or int(stock.get("weekly_score", 0)) >= 70
        or int(stock.get("monthly_score", 0)) >= 70
        or stock.get("conviction") == "High"
    ]
    if len(focus) < 5:
        focus = sorted(stocks, key=lambda stock: max(stock["weekly_score"], stock["monthly_score"]), reverse=True)[:8]
    return sorted(focus, key=lambda stock: (stock["candidate"], stock["weekly_score"], stock["monthly_score"]), reverse=True)


async def eod_background_task() -> None:
    EOD_TASK_STATUS.update({"status": "running", "updated_at": datetime.now(timezone.utc).isoformat(), "result": None, "error": None})
    try:
        if run_eod_update is None:
            raise RuntimeError("Database EOD loader is unavailable")
        result = await run_eod_update()
        EOD_TASK_STATUS.update({"status": "complete", "updated_at": datetime.now(timezone.utc).isoformat(), "result": result, "error": None})
    except Exception as exc:  # pragma: no cover - network/db dependent
        EOD_TASK_STATUS.update({"status": "error", "updated_at": datetime.now(timezone.utc).isoformat(), "result": None, "error": str(exc)})


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
        scored.append(apply_live_tick(final_decision(company, bars, DATASET["benchmark_bars"], market, portfolio_context=portfolio_context)))
    return scored


def find_company(symbol: str) -> dict[str, Any] | None:
    symbol = symbol.upper()
    return next((company for company in DATASET["companies"] if company["symbol"] == symbol), None)


def seed_row_for_symbol(symbol: str) -> dict[str, Any] | None:
    symbol = symbol.upper()
    company = find_company(symbol)
    if not company:
        return None
    return {
        "symbol": symbol,
        "name": company.get("name", symbol),
        "sector": company.get("sector", "Researched"),
        "industry": company.get("industry", "Researched universe"),
        "market_cap_cr": company.get("market_cap_cr"),
        "research_covered": True,
    }


def company_from_universe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": str(row.get("symbol", "")).upper(),
        "name": row.get("name") or row.get("symbol"),
        "sector": row.get("sector") or "Unclassified",
        "industry": row.get("industry") or "NSE Equity",
        "market_cap_cr": row.get("market_cap_cr"),
        "fundamentals": deepcopy(NEUTRAL_FUNDAMENTALS),
        "tailwind": deepcopy(NEUTRAL_TAILWIND),
        "tailwind_factors": [
            "This stock is loaded from the NSE searchable universe.",
            "Fundamental and sector tailwind scores are neutral until you add research coverage.",
        ],
        "events": [],
    }


def dynamic_universe_stock_detail(symbol: str) -> dict[str, Any]:
    ensure_universe_loaded()
    row = UNIVERSE_STORE.get(symbol) or seed_row_for_symbol(symbol)
    if not row:
        raise HTTPException(status_code=404, detail=f"{symbol.upper()} was not found in the NSE universe cache")
    company = company_from_universe_row(row)
    connector_notes: list[str] = []
    try:
        bars = fetch_yahoo_chart(symbol, range_value="2y", interval="1d")
        if len(bars) >= 210:
            connector_notes.append("Yahoo Finance 2-year daily history loaded for searched NSE symbol")
        else:
            connector_notes.append(f"Yahoo Finance returned only {len(bars)} daily bars")
    except Exception as exc:  # pragma: no cover - network dependent
        bars = []
        connector_notes.append(f"Yahoo Finance history failed: {exc}")
    if len(bars) < 210:
        raise HTTPException(
            status_code=503,
            detail=f"{symbol.upper()} is in NSE search, but not enough Yahoo history is available to score it yet. Notes: {' | '.join(connector_notes)}",
        )
    try:
        news_events = fetch_no_key_news(symbol, company["name"], max_records=8)
        company["events"] = news_events
        if news_events:
            connector_notes.append("No-key news loaded for searched NSE symbol")
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"No-key news failed: {exc}")
    market = market_support_score(DATASET["market"])
    scored = apply_live_tick(final_decision(company, bars, DATASET["benchmark_bars"], market))
    scored["trade_state"] = advance_trade_state(scored)
    scored["connector_notes"] = connector_notes
    scored["universe_row"] = row
    scored["data_mode"] = "nse_search_yahoo_scored"
    return scored


def apply_live_tick(stock: dict[str, Any]) -> dict[str, Any]:
    tick = live_feed.snapshot([stock["symbol"]]).get(stock["symbol"])
    ltp = tick.get("ltp") if tick else None
    if ltp is None:
        return stock
    previous_price = float(stock.get("price") or 0)
    stock["price"] = float(ltp)
    if tick.get("change_pct") is not None:
        stock["change_pct"] = tick["change_pct"]
    elif previous_price:
        stock["change_pct"] = round((float(ltp) - previous_price) / previous_price * 100, 2)
    stock["live_tick"] = tick
    entry = stock.get("entry", {})
    breakout = float(entry.get("breakout_level") or 0)
    stop = float(entry.get("stop") or 0)
    if stop and float(ltp) <= stop:
        stock["trade_state"] = {
            **stock.get("trade_state", {}),
            "state": "Exited",
            "reason": "Live Shoonya tick is at or below stop",
            "last_price": round(float(ltp), 2),
        }
    elif stock.get("candidate") and breakout and float(ltp) >= breakout:
        stock["trade_state"] = {
            **stock.get("trade_state", {}),
            "state": "Triggered",
            "reason": "Live Shoonya tick crossed breakout level",
            "last_price": round(float(ltp), 2),
        }
    return stock


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


async def startup_refresh_and_subscribe() -> None:
    await asyncio.sleep(8)
    try:
        universe_result = await asyncio.to_thread(refresh_nse_universe)
        print(f"[startup] NSE universe: {' | '.join(universe_result.get('notes', []))}")
    except Exception as exc:  # pragma: no cover - startup/network dependent
        print(f"[startup] NSE universe refresh failed: {exc}")

    try:
        result = await asyncio.to_thread(refresh_market_data)
        print(f"[startup] Market refresh: {' | '.join(result.get('notes', []))}")
    except Exception as exc:  # pragma: no cover - startup/network dependent
        print(f"[startup] Market refresh failed: {exc}")

    if live_feed.configured:
        try:
            live_feed.start()
            await asyncio.sleep(15)
            await asyncio.to_thread(live_feed.subscribe, universe_symbols())
            print(f"[startup] Shoonya subscribe attempted for {len(universe_symbols())} symbols")
        except Exception as exc:  # pragma: no cover - broker/network dependent
            print(f"[startup] Shoonya startup failed: {exc}")


@app.on_event("startup")
async def on_startup() -> None:
    if DATABASE_ENABLED and init_db is not None:
        try:
            db_init = await init_db()
            print(f"[startup] Database: {db_init}")
        except Exception as exc:  # pragma: no cover - db dependent
            print(f"[startup] Database init failed: {exc}")
    asyncio.create_task(startup_refresh_and_subscribe())


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
        "sparkline": [float(bar["close"]) for bar in (stock.get("bars") or [])[-18:] if bar.get("close") is not None],
        "live_tick": stock.get("live_tick"),
        "data_mode": stock.get("data_mode", "research_scored"),
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
        "data_mode": "focused_research_plus_nse_search_cache",
        "universe": UNIVERSE_STORE.meta(),
        "database": database_status(),
    }


@app.get("/api/dashboard")
def dashboard() -> dict[str, Any]:
    stocks = build_scored_universe()
    focus = dashboard_focus(stocks)
    market = market_support_score(DATASET["market"])
    top_weekly = sorted(focus, key=lambda stock: stock["weekly_score"], reverse=True)[:3]
    top_monthly = sorted(focus, key=lambda stock: stock["monthly_score"], reverse=True)[:3]
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
        "stocks": [stock_summary(stock) for stock in sorted(focus, key=lambda stock: stock["weekly_score"], reverse=True)],
        "scored_research_total": len(stocks),
        "dashboard_mode": "focus_list",
        "focus_criteria": "candidate true OR weekly_score >= 70 OR monthly_score >= 70, with top-ranked fallback",
        "nse_universe": UNIVERSE_STORE.meta(),
        "database": database_status(),
        "live_feed": live_feed.status(),
        "latency_strategy": {
            "live_feed": "Shoonya WebSocket pushes real-time ticks to /ws/live-prices when backend credentials are configured.",
            "universe": "NSE equity master + latest bhavcopy create a searchable all-NSE EOD cache; live ticks stay limited to focus names.",
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
    stocks = build_scored_universe()
    return {"stocks": [stock_summary(stock) for stock in dashboard_focus(stocks)], "mode": "focus_list"}


@app.get("/api/universe/status")
def universe_status() -> dict[str, Any]:
    return UNIVERSE_STORE.meta()


@app.post("/api/universe/refresh")
def refresh_universe() -> dict[str, Any]:
    return refresh_nse_universe()


@app.get("/api/universe/search")
def search_universe(q: str = "", limit: int = 50) -> dict[str, Any]:
    ensure_universe_loaded()
    rows = UNIVERSE_STORE.search(q, limit=limit)
    return {
        "query": q,
        "count": len(rows),
        "rows": rows,
        "meta": UNIVERSE_STORE.meta(),
        "note": "Search uses NSE equity master and bhavcopy EOD cache. Click a result to score it through Yahoo history and current decision rules.",
    }


@app.get("/api/search")
def command_search(q: str = "", limit: int = 30) -> list[dict[str, Any]]:
    ensure_universe_loaded()
    return UNIVERSE_STORE.search(q, limit=limit)


@app.get("/api/database/status")
async def get_database_status() -> dict[str, Any]:
    payload = database_status()
    if DATABASE_ENABLED and database_counts is not None:
        try:
            payload["counts"] = await database_counts()
        except Exception as exc:  # pragma: no cover - db dependent
            payload["count_error"] = str(exc)
    payload["eod_task"] = EOD_TASK_STATUS
    return payload


@app.post("/api/admin/run-eod")
async def trigger_eod(background_tasks: BackgroundTasks, secret: str) -> dict[str, Any]:
    expected = os.getenv("ADMIN_SECRET")
    if not expected:
        raise HTTPException(status_code=403, detail="ADMIN_SECRET is not configured on the backend")
    if secret != expected:
        raise HTTPException(status_code=403, detail="Invalid admin secret")
    if not DATABASE_ENABLED or run_eod_update is None:
        raise HTTPException(status_code=400, detail="DATABASE_URL is not configured or database loader is unavailable")
    background_tasks.add_task(eod_background_task)
    return {"status": "EOD update queued", "database": database_status()}


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


@app.post("/api/live/twofa")
def live_twofa(payload: dict[str, Any]) -> dict[str, Any]:
    twofa = str(payload.get("twofa") or payload.get("otp") or "").strip()
    if not twofa:
        raise HTTPException(status_code=400, detail="Send current Shoonya OTP/TOTP as twofa")
    try:
        return live_feed.set_runtime_twofa(twofa)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
    universe = refresh_nse_universe()
    market = refresh_market_data()
    alerts = scan_trade_alerts(refresh_market=False)
    return {"universe_refresh": universe, "market_refresh": market, "alert_scan": alerts}


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
    return dynamic_universe_stock_detail(symbol)


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
        return dynamic_universe_stock_detail(symbol)

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
