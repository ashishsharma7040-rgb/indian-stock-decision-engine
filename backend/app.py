from __future__ import annotations

import asyncio
import json
import os
import threading
from copy import deepcopy
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backtest import run_backtest
from data_sources import (
    OFFICIAL_FILING_LINKS,
    env_first,
    fetch_alpha_vantage_daily,
    fetch_corporate_actions_for_symbol,
    fetch_free_fundamentals,
    fetch_latest_nse_bhavcopy,
    fetch_market_news,
    fetch_market_quote,
    fetch_nifty500_breadth,
    fetch_nse_advance_decline,
    fetch_nse_equity_master,
    fetch_no_key_news,
    fetch_official_ohlcv,
    fetch_newsapi,
    fetch_yahoo_chart,
    fetch_yahoo_intraday,
    recommended_api_stack,
)
from corporate_actions import adjust_ohlcv_for_actions
from fundamental_import import parse_fundamentals_csv
from live_feed import live_feed
from llm_thesis import generate_premium_thesis
import redis_state
from scoring_engine import event_strength_score, final_decision, market_support_score, pct_distance
from seed_data import WATCH_SYMBOLS, build_demo_dataset
from universe_store import UNIVERSE_STORE

try:
    from bhavcopy_loader import database_counts, run_eod_update
    from database import AsyncSessionLocal, DATABASE_ENABLED, database_status, init_db
    from models import ActivePortfolio, TradeState as PersistentTradeState
    from sqlalchemy import delete, select
except Exception as exc:  # pragma: no cover - optional DB dependencies
    DATABASE_ENABLED = False
    DB_IMPORT_ERROR = str(exc)
    AsyncSessionLocal = None  # type: ignore
    ActivePortfolio = None  # type: ignore
    PersistentTradeState = None  # type: ignore
    delete = None  # type: ignore
    select = None  # type: ignore
    database_counts = None  # type: ignore
    run_eod_update = None  # type: ignore
    init_db = None  # type: ignore

    def database_status() -> dict[str, Any]:  # type: ignore
        return {"enabled": False, "url_configured": bool(os.getenv("DATABASE_URL")), "import_error": DB_IMPORT_ERROR}


APP_NAME = "Indian Stock Decision Engine"
BASE_DIR = Path(__file__).resolve().parent
DATASET = build_demo_dataset()
DATASET_LOCK = threading.RLock()
UNIVERSE_STORE.load_seed_companies(DATASET["companies"])
TRADE_STATES: dict[str, dict[str, Any]] = {}
PORTFOLIO_HOLDINGS: list[dict[str, Any]] = []
EOD_TASK_STATUS: dict[str, Any] = {"status": "idle", "updated_at": None, "result": None, "error": None}
PRICE_REFRESH_STATUS: dict[str, Any] = {"updated_at": None, "watch_updated_at": None, "last_error": None}
LATEST_QUOTES: dict[str, dict[str, Any]] = {}
_event_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_bars_cache: dict[str, tuple[float, dict[str, Any]]] = {}
SCORE_HISTORY: dict[str, list[dict[str, Any]]] = {}
WEEKLY_SCAN_STATUS: dict[str, Any] = {"status": "idle", "updated_at": None, "results": [], "error": None}
MASTER_UNIVERSE_CACHE: dict[str, Any] = {"generated_at": None, "payload": None}
FOCUS_DASHBOARD_CACHE: dict[str, Any] = {"generated_at": None, "payload": None}
FOCUS_DASHBOARD_TTL_SECONDS = 300

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


def now_ist() -> datetime:
    return datetime.now(ZoneInfo("Asia/Kolkata"))


def is_market_hours() -> bool:
    stamp = now_ist()
    if stamp.weekday() >= 5:
        return False
    return dt_time(9, 15) <= stamp.time() <= dt_time(15, 30)


def update_symbol_quote(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    tick = live_feed.snapshot([symbol]).get(symbol)
    if tick and tick.get("ltp") is not None:
        quote = {
            "symbol": symbol,
            "price": round(float(tick["ltp"]), 2),
            "change_pct": tick.get("change_pct"),
            "open": tick.get("open"),
            "high": tick.get("high"),
            "low": tick.get("low"),
            "close": round(float(tick["ltp"]), 2),
            "volume": tick.get("volume"),
            "timestamp": tick.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            "source": "Shoonya live tick",
        }
    else:
        quote = fetch_market_quote(symbol)
    LATEST_QUOTES[symbol] = quote
    with DATASET_LOCK:
        bars = DATASET["bars"].get(symbol)
        if bars:
            last = bars[-1]
            last["close"] = quote["price"]
            last["high"] = max(float(last.get("high", quote["price"])), float(quote.get("high") or quote["price"]))
            last["low"] = min(float(last.get("low", quote["price"])), float(quote.get("low") or quote["price"]))
            if quote.get("volume") is not None:
                last["volume"] = max(int(last.get("volume", 0) or 0), int(quote["volume"] or 0))
    return quote


def merge_quote_into_bars(symbol: str, bars: list[dict[str, Any]], connector_notes: list[str] | None = None) -> list[dict[str, Any]]:
    if not bars:
        return bars
    try:
        quote = fetch_market_quote(symbol)
        LATEST_QUOTES[symbol.upper()] = quote
        last = bars[-1]
        last["close"] = quote["price"]
        last["high"] = max(float(last.get("high", quote["price"])), float(quote.get("high") or quote["price"]))
        last["low"] = min(float(last.get("low", quote["price"])), float(quote.get("low") or quote["price"]))
        if quote.get("volume") is not None:
            last["volume"] = max(int(last.get("volume", 0) or 0), int(quote["volume"] or 0))
        if connector_notes is not None:
            connector_notes.append(f"Latest quote merged from {quote.get('source', 'market quote')}")
    except Exception as exc:  # pragma: no cover - network/provider dependent
        if connector_notes is not None:
            connector_notes.append(f"Latest quote merge failed: {exc}")
    return bars


def ensure_watch_quotes_current(max_age_seconds: int = 75) -> None:
    stamp = PRICE_REFRESH_STATUS.get("watch_updated_at")
    if stamp:
        try:
            parsed = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
            if datetime.now(timezone.utc).timestamp() - parsed.timestamp() < max_age_seconds:
                return
        except Exception:
            pass
    refresh_seed_prices(prioritise_watchlist=True)


def refresh_seed_prices(prioritise_watchlist: bool = False) -> dict[str, Any]:
    symbols = WATCH_SYMBOLS if prioritise_watchlist else universe_symbols()
    updated: list[str] = []
    errors: list[str] = []
    for symbol in symbols:
        try:
            update_symbol_quote(symbol)
            updated.append(symbol)
        except Exception as exc:  # pragma: no cover - network dependent
            errors.append(f"{symbol}: {exc}")
    stamp = datetime.now(timezone.utc).isoformat()
    PRICE_REFRESH_STATUS["watch_updated_at" if prioritise_watchlist else "updated_at"] = stamp
    PRICE_REFRESH_STATUS["last_error"] = errors[:5] or None
    return {"updated": updated, "errors": errors[:5], "updated_at": stamp}


async def background_price_refresh() -> None:
    await asyncio.sleep(20)
    while True:
        try:
            if is_market_hours():
                await asyncio.to_thread(refresh_seed_prices, True)
                await asyncio.sleep(300)
                await asyncio.to_thread(refresh_seed_prices, False)
                await asyncio.sleep(900)
            else:
                await asyncio.sleep(900)
        except Exception as exc:  # pragma: no cover - network dependent
            PRICE_REFRESH_STATUS["last_error"] = str(exc)
            await asyncio.sleep(900)


async def background_market_refresh() -> None:
    """Refresh Nifty, breadth, and sector proxy momentum daily without manual clicks."""
    await asyncio.sleep(40)
    last_refresh_date: str | None = None
    while True:
        try:
            stamp = now_ist()
            should_refresh = stamp.weekday() < 5 and stamp.time() >= dt_time(16, 10)
            today_key = stamp.date().isoformat()
            if should_refresh and last_refresh_date != today_key:
                await asyncio.to_thread(refresh_market_data)
                last_refresh_date = today_key
            await asyncio.sleep(1800)
        except Exception as exc:  # pragma: no cover - network/provider dependent
            PRICE_REFRESH_STATUS["last_error"] = f"market_refresh: {exc}"
            await asyncio.sleep(1800)


async def background_weekly_scan() -> None:
    """Optional weekly scan over the filtered liquid universe.

    Disabled unless ENABLE_WEEKLY_SCAN=true because broad free-provider scans can hit rate limits.
    """
    if str(os.getenv("ENABLE_WEEKLY_SCAN", "")).lower() not in {"1", "true", "yes"}:
        return
    await asyncio.sleep(90)
    last_scan_week: str | None = None
    while True:
        try:
            stamp = now_ist()
            week_key = f"{stamp.isocalendar().year}-{stamp.isocalendar().week}"
            if stamp.weekday() == 4 and stamp.time() >= dt_time(16, 20) and last_scan_week != week_key:
                await asyncio.to_thread(run_weekly_scan, int(os.getenv("WEEKLY_SCAN_LIMIT", "35")))
                last_scan_week = week_key
            await asyncio.sleep(3600)
        except Exception as exc:  # pragma: no cover - provider dependent
            WEEKLY_SCAN_STATUS.update({"status": "error", "updated_at": datetime.now(timezone.utc).isoformat(), "error": str(exc)})
            await asyncio.sleep(3600)


def sector_map(stocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    current = sector_summary(stocks)
    return [
        {
            "sector": row["sector"],
            "avg_weekly": row["avg_weekly_score"],
            "avg_weekly_score": row["avg_weekly_score"],
            "avg_monthly": round(
                sum(s["monthly_score"] for s in stocks if s["sector"] == row["sector"]) / max(row["count"], 1),
                1,
            ),
            "leader": row["leader"],
            "count": row["count"],
            "trend": 0,
        }
        for row in current
    ]


def record_score_history(stocks: list[dict[str, Any]]) -> None:
    stamp = datetime.now(timezone.utc).isoformat()
    for stock in stocks:
        history = SCORE_HISTORY.setdefault(stock["symbol"], [])
        last = history[-1] if history else None
        if last and last.get("weekly_score") == stock["weekly_score"] and last.get("monthly_score") == stock["monthly_score"]:
            continue
        history.append({"ts": stamp, "weekly_score": stock["weekly_score"], "monthly_score": stock["monthly_score"]})
        del history[:-30]


def period_to_yahoo(period: str) -> tuple[str, str]:
    mapping = {
        "1w": ("5d", "5m"),
        "1m": ("1mo", "1d"),
        "3m": ("3mo", "1d"),
        "6m": ("6mo", "1d"),
        "1y": ("1y", "1d"),
    }
    return mapping.get(period.lower(), ("3mo", "1d"))


def _run_async_sync(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    result: dict[str, Any] = {}

    def runner() -> None:
        try:
            result["value"] = asyncio.run(coro)
        except Exception as exc:  # pragma: no cover - defensive bridge
            result["error"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join(timeout=20)
    if "error" in result:
        raise result["error"]
    if "value" not in result:
        raise RuntimeError("Async provider call timed out")
    return result["value"]


def maybe_adjust_for_corporate_actions(symbol: str, bars: list[dict[str, Any]], notes: list[str]) -> tuple[list[dict[str, Any]], bool]:
    if not bars or str(os.getenv("ENABLE_CORPORATE_ACTIONS_ADJUSTMENT", "true")).lower() not in {"1", "true", "yes"}:
        return bars, False
    try:
        actions = _run_async_sync(fetch_corporate_actions_for_symbol(symbol))
        if actions:
            notes.append(f"Applied {len(actions)} NSE corporate-action adjustment(s)")
            return adjust_ohlcv_for_actions(bars, actions), True
        notes.append("No NSE split/bonus adjustment required from corporate-action scan")
        return bars, True
    except Exception as exc:  # pragma: no cover - optional provider dependent
        notes.append(f"Corporate-action scan unavailable: {exc}")
        return bars, False


def merge_free_fundamentals(symbol: str, company: dict[str, Any], notes: list[str]) -> None:
    try:
        fundamentals = _run_async_sync(fetch_free_fundamentals(symbol))
        if fundamentals:
            company.setdefault("fundamentals", {}).update({key: value for key, value in fundamentals.items() if value not in (None, "")})
            notes.append("Free NSE fundamentals merged from niftyterminal")
    except Exception as exc:  # pragma: no cover - optional provider dependent
        notes.append(f"Free NSE fundamentals unavailable: {exc}")


def load_research_bars(symbol: str, connector_notes: list[str], min_bars: int = 210) -> tuple[list[dict[str, Any]], bool]:
    bars = fetch_official_ohlcv(symbol, days=260)
    corporate_actions_applied = False
    if len(bars) >= min_bars:
        connector_notes.append("Official NSE bhavcopy history loaded via nsefin")
        bars, corporate_actions_applied = maybe_adjust_for_corporate_actions(symbol, bars, connector_notes)
        return bars, corporate_actions_applied
    if bars:
        connector_notes.append(f"NSE bhavcopy history returned only {len(bars)} bars; falling back to chart providers")
    try:
        bars = fetch_yahoo_chart(symbol, range_value="2y", interval="1d")
        if len(bars) >= min_bars:
            connector_notes.append("Yahoo Finance 2-year daily history loaded as fallback")
        else:
            connector_notes.append(f"Yahoo Finance returned only {len(bars)} daily bars")
    except Exception as exc:  # pragma: no cover - network dependent
        bars = []
        connector_notes.append(f"Yahoo Finance history failed: {exc}")
    if bars:
        bars, corporate_actions_applied = maybe_adjust_for_corporate_actions(symbol, bars, connector_notes)
    return bars, corporate_actions_applied


def bars_payload(symbol: str, period: str, fresh: bool = False) -> dict[str, Any]:
    key = f"{symbol.upper()}:{period.lower()}"
    now_ts = datetime.now(timezone.utc).timestamp()
    hit = _bars_cache.get(key)
    if hit and not fresh and now_ts - hit[0] < 900:
        return hit[1]
    range_value, interval = period_to_yahoo(period)
    source = "Yahoo Finance chart endpoint, independent of scoring payload"
    bars: list[dict[str, Any]] = []
    if interval == "1d":
        official_days = {"1m": 35, "3m": 90, "6m": 140, "1y": 260}.get(period.lower(), 90)
        bars = fetch_official_ohlcv(symbol, days=official_days)
        if len(bars) >= max(20, official_days // 3):
            source = "Official NSE bhavcopy via nsefin, independent of scoring payload"
        else:
            bars = []
    if not bars:
        bars = fetch_yahoo_chart(symbol, range_value=range_value, interval=interval)
    benchmark = fetch_yahoo_chart("NIFTY", range_value=range_value, interval=interval)
    normalized = [
        {
            "time": str(bar.get("datetime", ""))[:10],
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar.get("volume", 0),
        }
        for bar in bars
    ]
    bench_norm = [
        {
            "time": str(bar.get("datetime", ""))[:10],
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar.get("volume", 0),
        }
        for bar in benchmark[-len(normalized):]
    ]
    payload = {
        "symbol": symbol.upper(),
        "period": period.lower(),
        "bars": normalized,
        "benchmark": bench_norm,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
    }
    _bars_cache[key] = (now_ts, payload)
    return payload


def get_symbol_events(symbol: str, fresh: bool = False) -> dict[str, Any]:
    symbol = symbol.upper()
    company = find_company(symbol) or company_from_universe_row(UNIVERSE_STORE.get(symbol) or {"symbol": symbol, "name": symbol})
    now_ts = datetime.now(timezone.utc).timestamp()
    hit = _event_cache.get(symbol)
    if hit and not fresh and now_ts - hit[0] < 300:
        events = hit[1]
    else:
        events = fetch_market_news(symbol, company["name"], max_records=18, bypass_cache=fresh)
        _event_cache[symbol] = (now_ts, events)
    scored = event_strength_score(events, timeframe="weekly")
    sorted_events = sorted(scored["events"], key=lambda event: abs(float(event.get("net_score", 0))), reverse=True)
    return {"symbol": symbol, "score": scored["score"], "updated_at": datetime.now(timezone.utc).isoformat(), "events": sorted_events}


def dashboard_events_for(company: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    """Return provider-backed events for dashboard scoring, never only baked seed events."""
    symbol = company["symbol"]
    try:
        payload = get_symbol_events(symbol, fresh=False)
        events = payload.get("events", [])
        if events:
            return events[:12], "provider_cache"
    except Exception:
        pass
    return list(company.get("events", []))[:8], "seed_fallback"


def compute_rs_ratings(companies: list[dict[str, Any]], bars_by_symbol: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    performances: list[tuple[str, float]] = []
    for company in companies:
        symbol = company["symbol"]
        bars = bars_by_symbol.get(symbol, [])
        if len(bars) < 65:
            continue
        lookback = 126 if len(bars) > 126 else 65
        start = float(bars[-lookback]["close"])
        end = float(bars[-1]["close"])
        if start > 0:
            performances.append((symbol, pct_distance(end, start)))
    if not performances:
        return {}
    ordered = sorted(performances, key=lambda item: item[1])
    total = max(len(ordered) - 1, 1)
    return {symbol: max(1, min(99, round(index / total * 98 + 1))) for index, (symbol, _) in enumerate(ordered)}


def load_static_sector_medians() -> dict[str, dict[str, float]]:
    try:
        return json.loads((BASE_DIR / "sector_medians.json").read_text(encoding="utf-8"))
    except Exception:
        return {}


def compute_sector_medians(companies: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    fields = ["sales_cagr", "profit_cagr", "roce", "roe", "debt_equity", "pe"]
    grouped: dict[str, dict[str, list[float]]] = {}
    for company in companies:
        sector = company.get("sector") or "Unclassified"
        grouped.setdefault(sector, {field: [] for field in fields})
        fundamentals = company.get("fundamentals", {})
        for field in fields:
            value = fundamentals.get(field)
            try:
                if value is not None:
                    grouped[sector][field].append(float(value))
            except (TypeError, ValueError):
                continue
    medians: dict[str, dict[str, float]] = load_static_sector_medians()
    for sector, values in grouped.items():
        medians.setdefault(sector, {})
        for field, rows in values.items():
            if rows:
                ordered = sorted(rows)
                mid = len(ordered) // 2
                medians[sector][field] = ordered[mid] if len(ordered) % 2 else (ordered[mid - 1] + ordered[mid]) / 2
    return medians


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
    with DATASET_LOCK:
        companies = deepcopy(DATASET["companies"])
        bars_by_symbol = deepcopy(DATASET["bars"])
        benchmark_bars = deepcopy(DATASET["benchmark_bars"])
        market_source = deepcopy(DATASET["market"])
    market = market_support_score(market_source)
    scored: list[dict[str, Any]] = []
    portfolio_holdings = _run_async_sync(load_portfolio_from_db()) if DATABASE_ENABLED else PORTFOLIO_HOLDINGS
    portfolio_context = {
        "holdings": portfolio_holdings,
        "historical_bars_by_symbol": bars_by_symbol,
        "total_account_value": float(os.getenv("ACCOUNT_SIZE", "1000000")),
        "max_sector_exposure": float(os.getenv("MAX_SECTOR_EXPOSURE", "0.25")),
        "max_correlation": float(os.getenv("MAX_PORTFOLIO_CORRELATION", "0.65")),
        "caution_correlation": float(os.getenv("CAUTION_PORTFOLIO_CORRELATION", "0.40")),
        "max_sector_positions": 3,
        "max_industry_positions": 2,
    }
    rs_ratings = compute_rs_ratings(companies, bars_by_symbol)
    sector_medians = compute_sector_medians(companies)
    for company in companies:
        bars = bars_by_symbol[company["symbol"]]
        company.setdefault("fundamentals", {})["sector_medians"] = sector_medians.get(company.get("sector") or "Unclassified", {})
        company["events"], company["event_data_mode"] = dashboard_events_for(company)
        company["rs_rating"] = rs_ratings.get(company["symbol"])
        scored.append(apply_live_tick(final_decision(company, bars, benchmark_bars, market, portfolio_context=portfolio_context)))
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
        "delivery_pct": row.get("delivery_pct"),
        "delivery_qty": row.get("delivery_qty"),
        "avg_volume_50d": row.get("avg_volume_50d"),
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
    merge_free_fundamentals(symbol, company, connector_notes)
    bars, corporate_actions_applied = load_research_bars(symbol, connector_notes, min_bars=210)
    if len(bars) < 210:
        raise HTTPException(
            status_code=503,
            detail=f"{symbol.upper()} is in NSE search, but not enough official/free history is available to score it yet. Notes: {' | '.join(connector_notes)}",
        )
    company["corporate_actions_applied"] = corporate_actions_applied
    bars = merge_quote_into_bars(symbol, bars, connector_notes)
    try:
        news_events = fetch_market_news(symbol, company["name"], max_records=10)
        company["events"] = news_events
        if news_events:
            connector_notes.append("News loaded from configured NewsAPI plus no-key fallbacks")
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"News refresh failed: {exc}")
    market = market_support_score(DATASET["market"])
    scored = apply_live_tick(final_decision(company, bars, DATASET["benchmark_bars"], market))
    scored["trade_state"] = advance_trade_state(scored)
    scored["connector_notes"] = connector_notes
    scored["universe_row"] = row
    scored["data_mode"] = "nse_search_official_first_scored"
    scored.setdefault("data_quality_gate", {})["corporate_actions_applied"] = corporate_actions_applied
    return scored


def run_weekly_scan(limit: int = 35) -> dict[str, Any]:
    ensure_universe_loaded()
    rows = UNIVERSE_STORE.top_liquid(limit=max(5, min(limit, 80)))
    results: list[dict[str, Any]] = []
    errors: list[str] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        try:
            scored = dynamic_universe_stock_detail(symbol)
            if scored.get("data_quality_gate", {}).get("pass") and int(scored.get("weekly_score", 0)) >= 60:
                results.append(stock_summary(scored) | {
                    "candidate": scored.get("candidate"),
                    "data_quality_gate": scored.get("data_quality_gate"),
                    "connector_notes": scored.get("connector_notes", []),
                    "investable_filter": row.get("investable_filter"),
                })
        except Exception as exc:  # pragma: no cover - provider dependent
            errors.append(f"{symbol}: {exc}")
    results.sort(key=lambda item: (bool(item.get("candidate")), int(item.get("weekly_score", 0))), reverse=True)
    payload = {
        "status": "complete",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "scanned": len(rows),
        "returned": len(results[:25]),
        "results": results[:25],
        "errors": errors[:10],
        "universe_filter": "series EQ, turnover >= Rs 1 crore, market cap >= Rs 500 crore when market cap is available",
        "warning": "Free providers may throttle broad scans. Keep scan limits modest unless you add a licensed data feed.",
    }
    WEEKLY_SCAN_STATUS.update(payload | {"error": None})
    return payload


def apply_live_tick(stock: dict[str, Any]) -> dict[str, Any]:
    tick = live_feed.snapshot([stock["symbol"]]).get(stock["symbol"])
    ltp = tick.get("ltp") if tick else None
    if ltp is None:
        quote = LATEST_QUOTES.get(stock["symbol"])
        if quote:
            stock["price"] = quote.get("price", stock.get("price"))
            stock["change_pct"] = quote.get("change_pct", stock.get("change_pct"))
            stock["price_source"] = quote.get("source")
            stock["price_as_of"] = quote.get("timestamp")
        return stock
    previous_price = float(stock.get("price") or 0)
    stock["price"] = float(ltp)
    if tick.get("change_pct") is not None:
        stock["change_pct"] = tick["change_pct"]
    elif previous_price:
        stock["change_pct"] = round((float(ltp) - previous_price) / previous_price * 100, 2)
    stock["live_tick"] = tick
    stock["price_source"] = "Shoonya live tick"
    stock["price_as_of"] = tick.get("timestamp") or datetime.now(timezone.utc).isoformat()
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
    with DATASET_LOCK:
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


def compute_smallcap_relative_regime(nifty_bars: list[dict[str, Any]]) -> dict[str, Any] | None:
    if len(nifty_bars) < 25:
        return None
    symbols = [
        os.getenv("SMALLCAP_INDEX_SYMBOL", "").strip(),
        "^CNXSC",
        "^CNXSMALLCAP",
        "NIFTYSMLCAP250.NS",
    ]
    smallcap_bars: list[dict[str, Any]] = []
    used_symbol = ""
    for symbol in [item for item in symbols if item]:
        try:
            smallcap_bars = fetch_yahoo_chart(symbol, range_value="6mo", interval="1d")
            if len(smallcap_bars) >= 25:
                used_symbol = symbol
                break
        except Exception:
            continue
    if len(smallcap_bars) < 25:
        return None
    count = min(len(nifty_bars), len(smallcap_bars))
    nifty_tail = nifty_bars[-count:]
    small_tail = smallcap_bars[-count:]
    ratio = [
        float(small["close"]) / float(nifty["close"])
        for small, nifty in zip(small_tail, nifty_tail)
        if float(nifty.get("close") or 0) > 0
    ]
    if len(ratio) < 25:
        return None
    rs20 = sum(ratio[-20:]) / 20
    rs5 = sum(ratio[-5:]) / 5
    trend = "UP" if ratio[-1] >= rs20 and rs5 >= rs20 else "DOWN"
    return {
        "symbol": used_symbol,
        "ratio": round(ratio[-1], 6),
        "ratio_sma20": round(rs20, 6),
        "ratio_5dma": round(rs5, 6),
        "trend": trend,
        "smallcap_entries_restricted": trend == "DOWN",
        "source": f"{used_symbol} vs NIFTY 50 relative strength",
    }


def refresh_market_data() -> dict[str, Any]:
    notes: list[str] = []
    updated: dict[str, Any] = {}
    live_updates = 0
    try:
        nifty_bars = fetch_yahoo_chart("NIFTY", range_value="1y", interval="1d")
        if len(nifty_bars) >= 200:
            with DATASET_LOCK:
                DATASET["benchmark_bars"] = nifty_bars
            updated["nifty_bars"] = nifty_bars
            updated["nifty_close"] = float(nifty_bars[-1]["close"])
            updated["source"] = "Yahoo Finance ^NSEI"
            live_updates += 1
            notes.append("Nifty 50 daily bars refreshed from Yahoo Finance")
            smallcap_rs = compute_smallcap_relative_regime(nifty_bars)
            if smallcap_rs:
                updated["smallcap_relative_strength"] = smallcap_rs
                notes.append(f"Smallcap relative regime refreshed: {smallcap_rs['trend']}")
            rotation = compute_sector_rotation(nifty_bars)
            if rotation:
                updated["sector_rotation"] = rotation
                updated["sector_strength"] = round(sum(item["score"] for item in rotation.values()) / len(rotation), 2)
                apply_sector_rotation_scores(rotation)
                notes.append("Sector rotation proxies refreshed")
    except Exception as exc:  # pragma: no cover - network dependent
        notes.append(f"Nifty refresh failed: {exc}")
    try:
        broad_breadth = fetch_nifty500_breadth()
        updated["nifty500_breadth_pct"] = broad_breadth.get("breadth_pct")
        updated["nifty500_total_measured"] = broad_breadth.get("total_stocks")
        updated["nifty500_above_50dma_count"] = broad_breadth.get("above_50dma")
        updated["breadth_source"] = broad_breadth.get("source")
        updated["breadth_above_50dma"] = broad_breadth.get("breadth_pct")
        updated["breadth_as_of"] = broad_breadth.get("as_of")
        live_updates += 1
        notes.append(
            f"Nifty 500/liquid-universe breadth refreshed: {broad_breadth.get('breadth_pct')}% above 50 DMA"
        )
    except Exception as exc:  # pragma: no cover - optional provider dependent
        notes.append(f"Nifty 500 breadth refresh failed: {exc}")
    try:
        breadth = fetch_nse_advance_decline("NIFTY 50")
        breadth_source = breadth.pop("source", "NSE breadth")
        updated.update(breadth)
        updated.setdefault("breadth_source", breadth_source)
        updated.setdefault("breadth_above_50dma", breadth["advance_decline_breadth_pct"])
        live_updates += 1
        notes.append("NSE advance/decline breadth refreshed")
    except Exception as exc:  # pragma: no cover - network dependent
        notes.append(f"NSE breadth refresh failed: {exc}")
    if live_updates:
        updated["updated_at"] = datetime.now(timezone.utc).isoformat()
        with DATASET_LOCK:
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

    in_app_feed = str(os.getenv("SHOONYA_IN_APP_FEED", "true")).lower() in {"1", "true", "yes"}
    if live_feed.configured and in_app_feed:
        try:
            live_feed.start()
            await asyncio.sleep(15)
            await asyncio.to_thread(live_feed.subscribe, universe_symbols())
            print(f"[startup] Shoonya subscribe attempted for {len(universe_symbols())} symbols")
        except Exception as exc:  # pragma: no cover - broker/network dependent
            print(f"[startup] Shoonya startup failed: {exc}")
    elif live_feed.configured:
        print("[startup] Shoonya app feed disabled; expecting feed_worker.py + Redis")


@app.on_event("startup")
async def on_startup() -> None:
    if DATABASE_ENABLED and init_db is not None:
        try:
            db_init = await init_db()
            print(f"[startup] Database: {db_init}")
        except Exception as exc:  # pragma: no cover - db dependent
            print(f"[startup] Database init failed: {exc}")
    asyncio.create_task(startup_refresh_and_subscribe())
    asyncio.create_task(background_price_refresh())
    asyncio.create_task(background_market_refresh())
    asyncio.create_task(background_weekly_scan())


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
        "data_quality_gate": stock.get("data_quality_gate"),
        "forensic_gate": stock.get("forensic_gate"),
        "rs_rating": technical.get("indicators", {}).get("rs_rating"),
        "price_source": stock.get("price_source"),
        "price_as_of": stock.get("price_as_of"),
        "confidence_interval": stock.get("confidence_interval"),
        "portfolio_check": stock.get("portfolio_check"),
        "execution_audit": stock.get("execution_audit"),
        "extension_ratio": technical.get("indicators", {}).get("extension_ratio"),
        "rubber_band": technical.get("indicators", {}).get("rubber_band"),
        "risk_flags": stock["explanation_json"]["risk_flags"],
        "sparkline": [float(bar["close"]) for bar in (stock.get("bars") or [])[-18:] if bar.get("close") is not None],
        "live_tick": stock.get("live_tick"),
        "data_mode": stock.get("data_mode", "research_scored"),
    }


def focus_cache_valid() -> bool:
    generated_at = FOCUS_DASHBOARD_CACHE.get("generated_at")
    if not generated_at or not FOCUS_DASHBOARD_CACHE.get("payload"):
        return False
    try:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
        return age.total_seconds() < FOCUS_DASHBOARD_TTL_SECONDS
    except Exception:
        return False


def score_focus_universe_row(row: dict[str, Any], market_result: dict[str, Any]) -> dict[str, Any] | None:
    symbol = str(row.get("symbol", "")).upper()
    if not symbol:
        return None
    notes: list[str] = []
    try:
        company = company_from_universe_row(row)
        merge_free_fundamentals(symbol, company, notes)
        bars, corporate_actions_applied = load_research_bars(symbol, notes, min_bars=60)
        if len(bars) < 60:
            return None
        company["corporate_actions_applied"] = corporate_actions_applied
        bars = merge_quote_into_bars(symbol, bars, notes)
        try:
            company["events"] = get_symbol_events(symbol, fresh=False).get("events", [])[:8]
        except Exception:
            company["events"] = []
        scored = apply_live_tick(final_decision(company, bars, DATASET["benchmark_bars"], market_result))
        scored["trade_state"] = advance_trade_state(scored)
        scored["connector_notes"] = notes
        scored["universe_row"] = row
        scored["data_mode"] = "nse_dynamic_focus"
        return scored
    except Exception:
        return None


def build_focus_dashboard_payload(force: bool = False, scan_limit: int = 60) -> dict[str, Any]:
    if not force and focus_cache_valid():
        return FOCUS_DASHBOARD_CACHE["payload"]
    scan_limit = max(20, min(int(scan_limit or 60), 120))
    try:
        ensure_watch_quotes_current()
    except Exception as exc:
        PRICE_REFRESH_STATUS["last_error"] = str(exc)
    market = market_support_score(DATASET["market"])
    seed_scored = build_scored_universe()
    seed_symbols = {stock["symbol"] for stock in seed_scored}
    ensure_universe_loaded()
    dynamic_scored: list[dict[str, Any]] = []
    for row in UNIVERSE_STORE.top_liquid(limit=scan_limit):
        symbol = str(row.get("symbol") or "").upper()
        if not symbol or symbol in seed_symbols:
            continue
        scored = score_focus_universe_row(row, market)
        if scored:
            dynamic_scored.append(scored)
    all_scored = seed_scored + dynamic_scored
    triggered: list[dict[str, Any]] = []
    stalking: list[dict[str, Any]] = []
    watchlist_bucket: list[dict[str, Any]] = []
    avoid: list[dict[str, Any]] = []
    can_buy = bool(market.get("can_buy", True))
    for stock in all_scored:
        price = float(stock.get("price") or 0)
        breakout = float((stock.get("entry") or {}).get("breakout_level") or 0)
        monthly = int(stock.get("monthly_score") or 0)
        weekly = int(stock.get("weekly_score") or 0)
        conviction = stock.get("conviction", "Avoid")
        risk_score = int(stock.get("risk_penalty", {}).get("score", 0) or stock.get("risk_score", 0))
        row = stock.get("universe_row") or {}
        at_upper = bool(row.get("at_upper_circuit"))
        if conviction == "Avoid" or risk_score >= 22:
            avoid.append(stock)
            continue
        if stock.get("candidate") and breakout > 0 and price >= breakout and can_buy and not at_upper:
            stock["_bucket"] = "triggered"
            triggered.append(stock)
            continue
        if stock.get("candidate") and breakout > 0 and price > 0:
            gap_pct = (breakout - price) / price * 100
            if 0 < gap_pct <= 3.0 and monthly >= 65:
                stock["_bucket"] = "stalking"
                stock["_breakout_gap_pct"] = round(gap_pct, 2)
                stalking.append(stock)
                continue
        if stock.get("candidate") and conviction in {"High", "Watchlist"} and (monthly >= 60 or weekly >= 65):
            stock["_bucket"] = "watchlist"
            watchlist_bucket.append(stock)
    triggered.sort(key=lambda item: item.get("weekly_score", 0), reverse=True)
    stalking.sort(key=lambda item: item.get("monthly_score", 0) * 0.6 + item.get("weekly_score", 0) * 0.4 - item.get("_breakout_gap_pct", 3) * 5, reverse=True)
    watchlist_bucket.sort(key=lambda item: item.get("monthly_score", 0) * 0.6 + item.get("weekly_score", 0) * 0.4, reverse=True)
    avoid.sort(key=lambda item: item.get("risk_penalty", {}).get("score", 0), reverse=True)

    def clean(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for stock in rows[:limit]:
            summary = stock_summary(stock)
            summary["bucket"] = stock.get("_bucket", "")
            summary["breakout_gap_pct"] = stock.get("_breakout_gap_pct")
            summary["data_source"] = stock.get("data_mode", "")
            summary["at_upper_circuit"] = bool((stock.get("universe_row") or {}).get("at_upper_circuit"))
            output.append(summary)
        return output

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_health": {
            "regime": market.get("regime_status", market.get("regime", "YELLOW")),
            "breadth_pct": market.get("breadth_above_50dma"),
            "position_multiplier": market.get("position_multiplier", 1.0),
            "can_buy": can_buy,
            "status_message": market.get("status_message", ""),
            "score": market.get("score"),
            "raw": market,
        },
        "focus": {
            "triggered": clean(triggered, 10),
            "stalking": clean(stalking, 15),
            "watchlist": clean(watchlist_bucket, 20),
            "avoid": clean(avoid, 5),
        },
        "sector_map": sector_map(all_scored),
        "scan_meta": {
            "seed_scored": len(seed_scored),
            "dynamic_scored": len(dynamic_scored),
            "total_scored": len(all_scored),
            "scan_limit_used": scan_limit,
            "universe_size": UNIVERSE_STORE.count(),
            "can_buy_regime": can_buy,
            "min_turnover_rs": 100_000_000,
            "min_delivery_pct": 40,
        },
        "disclaimer": "Research workflow only. Not investment advice.",
    }
    FOCUS_DASHBOARD_CACHE.update({"generated_at": payload["generated_at"], "payload": payload})
    return payload


async def load_portfolio_from_db() -> list[dict[str, Any]]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None or ActivePortfolio is None or select is None:
        return list(PORTFOLIO_HOLDINGS)
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(ActivePortfolio))
        rows = result.scalars().all()
    return [
        {
            "symbol": row.symbol,
            "sector": row.sector,
            "industry": row.industry or "",
            "shares_held": row.shares_held,
            "quantity": row.shares_held,
            "entry_price": row.entry_price,
            "last_price": row.last_price,
            "current_value": row.current_value,
        }
        for row in rows
    ]


async def save_portfolio_to_db(holdings: list[dict[str, Any]]) -> None:
    if not DATABASE_ENABLED or AsyncSessionLocal is None or ActivePortfolio is None or delete is None:
        return
    async with AsyncSessionLocal() as session:
        await session.execute(delete(ActivePortfolio))
        for holding in holdings:
            shares = int(float(holding.get("shares_held") or holding.get("quantity") or 0))
            current_value = float(holding.get("current_value") or 0)
            if not current_value and holding.get("last_price") and shares:
                current_value = shares * float(holding.get("last_price") or 0)
            session.add(
                ActivePortfolio(
                    symbol=holding["symbol"],
                    sector=holding.get("sector") or "Unclassified",
                    industry=holding.get("industry") or "",
                    shares_held=shares,
                    entry_price=holding.get("entry_price"),
                    last_price=holding.get("last_price"),
                    current_value=current_value,
                )
            )
        await session.commit()


async def load_trade_state_from_db(symbol: str) -> dict[str, Any] | None:
    if not DATABASE_ENABLED or AsyncSessionLocal is None or PersistentTradeState is None:
        return None
    async with AsyncSessionLocal() as session:
        row = await session.get(PersistentTradeState, symbol.upper())
    if row is None:
        return None
    return {
        "symbol": row.symbol,
        "state": row.state,
        "breakout_level": row.breakout_level,
        "stop": row.stop,
        "entry_price": row.entry_price,
        "last_price": row.last_price,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        "history": row.history or [],
    }


async def save_trade_state_to_db(symbol: str, state: dict[str, Any]) -> None:
    if not DATABASE_ENABLED or AsyncSessionLocal is None or PersistentTradeState is None:
        return
    async with AsyncSessionLocal() as session:
        row = await session.get(PersistentTradeState, symbol.upper())
        if row is None:
            row = PersistentTradeState(symbol=symbol.upper())
            session.add(row)
        row.state = state.get("state", row.state or "Screened")
        row.breakout_level = state.get("breakout_level")
        row.stop = state.get("stop")
        row.entry_price = state.get("entry_price")
        row.last_price = state.get("last_price")
        row.history = state.get("history", row.history or [])
        await session.commit()


def screener_master_row(stock: dict[str, Any]) -> dict[str, Any]:
    technical = stock.get("technical_strength", {})
    indicators = technical.get("indicators", {})
    entry = stock.get("entry", {})
    sizing = entry.get("position_sizing", {})
    forensic = stock.get("business_quality", {}).get("forensic_quality", {})
    metrics = forensic.get("metrics", {})
    audit = stock.get("execution_audit", {})
    risk_flags = stock.get("explanation_json", {}).get("risk_flags", [])
    candles = [
        {
            "time": str(bar.get("time") or bar.get("datetime", ""))[:10],
            "open": float(bar.get("open", bar.get("close", 0)) or 0),
            "high": float(bar.get("high", bar.get("close", 0)) or 0),
            "low": float(bar.get("low", bar.get("close", 0)) or 0),
            "close": float(bar.get("close", 0) or 0),
            "volume": float(bar.get("volume", 0) or 0),
        }
        for bar in (stock.get("bars") or [])[-160:]
    ]
    return {
        "symbol": stock["symbol"],
        "name": stock["name"],
        "sector": stock["sector"],
        "industry": stock.get("industry"),
        "close_price": stock["price"],
        "trigger_level": entry.get("breakout_level"),
        "stop_loss": entry.get("stop"),
        "status": stock.get("trade_state", {}).get("state"),
        "conviction": stock.get("conviction"),
        "weekly_score": stock.get("weekly_score"),
        "monthly_score": stock.get("monthly_score"),
        "idio_momentum_score": indicators.get("idiosyncratic_momentum", {}).get("score"),
        "rs_rating": indicators.get("rs_rating"),
        "atr": indicators.get("atr14"),
        "sma_200": indicators.get("dma200") or indicators.get("long_ma"),
        "extension_ratio": indicators.get("extension_ratio"),
        "extension_pct_above_200dma": indicators.get("extension_pct_above_200dma"),
        "rubber_band": indicators.get("rubber_band"),
        "cfo_ebitda_ratio": metrics.get("cfo_to_ebitda"),
        "sloan_ratio": metrics.get("sloan_ratio"),
        "piotroski_f_score": metrics.get("piotroski_f_score"),
        "altman_z_score": metrics.get("altman_z_score"),
        "liquidity_cap_shares": sizing.get("liquidity_cap_quantity"),
        "suggested_quantity": sizing.get("suggested_quantity"),
        "portfolio_audit": audit.get("portfolio_matrix"),
        "market_master_switch": audit.get("market_master_switch"),
        "bear_case": risk_flags[:3] or ["No active rule-based bear-case flag."],
        "historical_candles": candles,
    }


def master_universe_payload(force: bool = False) -> dict[str, Any]:
    generated_at = MASTER_UNIVERSE_CACHE.get("generated_at")
    if not force and generated_at and MASTER_UNIVERSE_CACHE.get("payload"):
        try:
            age = datetime.now(timezone.utc) - datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
            if age.total_seconds() < 300:
                return MASTER_UNIVERSE_CACHE["payload"]
        except ValueError:
            pass
    stocks = build_scored_universe()
    record_score_history(stocks)
    market = market_support_score(DATASET["market"])
    focus = sorted(dashboard_focus(stocks), key=lambda item: (item["weekly_score"], item["monthly_score"]), reverse=True)
    data = {stock["symbol"]: screener_master_row(stock) for stock in focus[:80]}
    master_switch = market.get("master_switch", {})
    payload = {
        "status": "success",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_health": {
            "regime": master_switch.get("regime_status", market.get("regime")),
            "breadth_pct": master_switch.get("breadth_pct", market.get("breadth_above_50dma")),
            "position_multiplier": master_switch.get("position_multiplier", market.get("position_multiplier")),
            "can_buy": master_switch.get("can_buy", market.get("can_buy")),
            "status_message": master_switch.get("description", market.get("status_message")),
            "raw_market": market,
        },
        "data": data,
        "data_shape": "overnight_audit_payload",
        "live_contract": {"websocket": "/ws/live", "purpose": "lightweight live ledger; compare LTP to cached trigger levels only"},
    }
    MASTER_UNIVERSE_CACHE.update({"generated_at": payload["generated_at"], "payload": payload})
    return payload


def live_ledger_payload(tick: dict[str, Any], trigger_map: dict[str, float]) -> dict[str, Any] | None:
    symbol = str(tick.get("symbol") or "").upper()
    ltp = tick.get("ltp") or tick.get("price")
    if not symbol or ltp is None:
        return None
    ltp_float = float(ltp)
    trigger = trigger_map.get(symbol)
    status = "BREAKOUT" if trigger is not None and ltp_float >= trigger else "PRICE_UPDATE"
    return {
        "symbol": symbol,
        "ltp": ltp_float,
        "status": status,
        "trigger_level": trigger,
        "timestamp": tick.get("timestamp") or tick.get("redis_updated_at") or datetime.now(timezone.utc).isoformat(),
        "volume": tick.get("volume"),
        "source": tick.get("source") or tick.get("provider") or "live_feed",
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


@app.get("/api/providers/status")
def provider_status() -> dict[str, Any]:
    price_provider = env_first("PRICE_DATA_PROVIDER", "FINANCIAL_DATA_PROVIDER") or "auto"
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price_provider": price_provider,
        "price_keys": {
            "twelve_data": bool(env_first("TWELVE_DATA_API_KEY", "TWELVEDATA_API_KEY")),
            "finnhub": bool(env_first("FINNHUB_API_KEY", "FINNHUB_TOKEN")),
            "fmp": bool(env_first("FMP_API_KEY", "FINANCIAL_MODELING_PREP_API_KEY")),
            "alpha_vantage": bool(env_first("ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY")),
            "shoonya_live": bool(live_feed.status().get("configured")),
        },
        "news_keys": {
            "newsapi": bool(env_first("NEWSAPI_API_KEY", "NEWS_API_KEY", "NEWSAPI_KEY", "NEWS_API_ORG_KEY")),
        },
        "accepted_env_names": {
            "newsapi": ["NEWSAPI_API_KEY", "NEWS_API_KEY", "NEWSAPI_KEY", "NEWS_API_ORG_KEY"],
            "price_provider": ["PRICE_DATA_PROVIDER", "FINANCIAL_DATA_PROVIDER"],
            "twelve_data": ["TWELVE_DATA_API_KEY", "TWELVEDATA_API_KEY"],
            "finnhub": ["FINNHUB_API_KEY", "FINNHUB_TOKEN"],
            "fmp": ["FMP_API_KEY", "FINANCIAL_MODELING_PREP_API_KEY"],
            "alpha_vantage": ["ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY"],
        },
        "last_price_refresh": PRICE_REFRESH_STATUS,
        "redis": redis_state.status(),
        "latest_quote_sources": {
            symbol: {"source": quote.get("source"), "timestamp": quote.get("timestamp"), "price": quote.get("price")}
            for symbol, quote in list(LATEST_QUOTES.items())[:20]
        },
    }


@app.get("/api/redis/status")
def redis_backend_status() -> dict[str, Any]:
    return redis_state.status()


@app.get("/api/dashboard")
def dashboard() -> dict[str, Any]:
    try:
        ensure_watch_quotes_current()
    except Exception as exc:  # pragma: no cover - network/provider dependent
        PRICE_REFRESH_STATUS["last_error"] = str(exc)
    stocks = build_scored_universe()
    record_score_history(stocks)
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
        "prices_as_of": PRICE_REFRESH_STATUS.get("watch_updated_at") or PRICE_REFRESH_STATUS.get("updated_at") or datetime.now(timezone.utc).isoformat(),
        "price_refresh": PRICE_REFRESH_STATUS,
        "sector_map": sector_map(stocks),
        "dashboard_mode": "focus_list",
        "focus_criteria": "candidate true OR weekly_score >= 70 OR monthly_score >= 70, with top-ranked fallback",
        "nse_universe": UNIVERSE_STORE.meta(),
        "weekly_scan": {
            "status": WEEKLY_SCAN_STATUS.get("status"),
            "updated_at": WEEKLY_SCAN_STATUS.get("updated_at"),
            "result_count": len(WEEKLY_SCAN_STATUS.get("results") or []),
            "enabled": str(os.getenv("ENABLE_WEEKLY_SCAN", "")).lower() in {"1", "true", "yes"},
        },
        "database": database_status(),
        "live_feed": live_feed.status(),
        "latency_strategy": {
            "live_feed": "Optional Shoonya WebSocket or Redis worker can push ticks to /ws/live-prices, but broker APIs are not required.",
            "universe": "NSE equity master + official bhavcopy create a searchable all-NSE EOD cache; live/free quotes stay limited to focus names.",
            "market": "Call /api/market/refresh or /api/scheduled/daily after market close to refresh Nifty, breadth, and sector rotation.",
            "prices": "Dashboard quotes use NSE public wrappers first when installed, then configured price keys, then Yahoo Finance fallback.",
            "events": "Events use configured NewsAPI keys first, then Yahoo Finance RSS, Google News RSS, and GDELT fallbacks.",
            "fundamentals": "Refresh daily after exchange/results updates.",
            "scores": "Refresh daily and immediately after major official event.",
        },
        "disclaimer": "Research workflow only. This app does not provide investment advice or guaranteed predictions.",
    }


@app.get("/api/v1/screener/master")
def screener_master(force: bool = False) -> dict[str, Any]:
    return master_universe_payload(force=force)


@app.get("/api/v1/dashboard/focus")
def focus_dashboard(force: bool = False, scan_limit: int = 60) -> dict[str, Any]:
    return build_focus_dashboard_payload(force=force, scan_limit=scan_limit)


@app.post("/api/admin/run-overnight-batch")
def run_overnight_batch(secret: str | None = None) -> dict[str, Any]:
    expected = os.getenv("ADMIN_SECRET")
    if expected and secret != expected:
        raise HTTPException(status_code=403, detail="Invalid admin secret")
    try:
        refresh_market_data()
    except Exception as exc:  # pragma: no cover - network/provider dependent
        PRICE_REFRESH_STATUS["last_error"] = str(exc)
    payload = master_universe_payload(force=True)
    return {
        "status": "overnight_batch_complete",
        "generated_at": payload["generated_at"],
        "symbols": len(payload.get("data", {})),
        "market_health": payload.get("market_health"),
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


@app.get("/api/universe/investable")
def investable_universe(limit: int = 100) -> dict[str, Any]:
    ensure_universe_loaded()
    rows = UNIVERSE_STORE.top_liquid(limit=max(1, min(limit, 300)))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(rows),
        "filter": "series EQ, turnover >= Rs 1 crore, market cap >= Rs 500 crore when market cap is available",
        "rows": rows,
        "universe": UNIVERSE_STORE.meta(),
    }


@app.post("/api/scan/weekly")
def weekly_scan(limit: int = 35) -> dict[str, Any]:
    try:
        return run_weekly_scan(limit=limit)
    except Exception as exc:
        WEEKLY_SCAN_STATUS.update({"status": "error", "updated_at": datetime.now(timezone.utc).isoformat(), "error": str(exc)})
        raise HTTPException(status_code=502, detail=f"Weekly scan failed: {exc}") from exc


@app.get("/api/scan/weekly")
def weekly_scan_status() -> dict[str, Any]:
    return WEEKLY_SCAN_STATUS


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


@app.get("/api/events/{symbol}")
def events_detail(symbol: str, fresh: int = 0) -> dict[str, Any]:
    try:
        return get_symbol_events(symbol, fresh=bool(fresh))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Event refresh failed: {exc}") from exc


@app.get("/api/bars/{symbol}")
def bars_detail(symbol: str, period: str = "3m", fresh: int = 0) -> dict[str, Any]:
    try:
        return bars_payload(symbol, period, fresh=bool(fresh))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Bar refresh failed: {exc}") from exc


@app.get("/api/chart/{symbol}")
def chart_detail(symbol: str, range: str = "3m", fresh: int = 1) -> dict[str, Any]:
    """Fresh chart bars, intentionally decoupled from scoring JSON and seed bars."""
    try:
        return bars_payload(symbol, range, fresh=bool(fresh))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Chart refresh failed: {exc}") from exc


@app.post("/api/prices/refresh")
def refresh_prices() -> dict[str, Any]:
    return refresh_seed_prices(prioritise_watchlist=False)


@app.websocket("/ws/live-prices")
async def live_prices_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    raw_symbols = websocket.query_params.get("symbols", "")
    symbols = [item.strip().upper() for item in raw_symbols.split(",") if item.strip()] or universe_symbols()
    status = live_feed.subscribe(symbols)
    await websocket.send_json({"type": "status", "status": status, "symbols": symbols})
    last_sent: dict[str, tuple[Any, Any, Any]] = {}
    redis_pubsub = None
    if redis_state.enabled():
        redis_client = redis_state.client()
        if redis_client is not None:
            redis_pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            redis_pubsub.subscribe(redis_state.TICK_CHANNEL)
    try:
        while True:
            ticks = live_feed.snapshot(symbols)
            if redis_pubsub is not None:
                message = await asyncio.to_thread(redis_pubsub.get_message, True, 1.0)
                if message and message.get("type") == "message":
                    try:
                        tick = json.loads(message.get("data") or "{}")
                        if not symbols or str(tick.get("symbol", "")).upper() in set(symbols):
                            ticks[str(tick.get("symbol")).upper()] = tick
                    except Exception:
                        pass
            updates = []
            for symbol, tick in ticks.items():
                key = (tick.get("timestamp"), tick.get("ltp"), tick.get("volume"))
                if last_sent.get(symbol) != key:
                    updates.append(tick)
                    last_sent[symbol] = key
            if updates:
                await websocket.send_json({"type": "ticks", "ticks": updates})
            await asyncio.sleep(0.25 if redis_pubsub is not None else 1)
    except WebSocketDisconnect:
        if redis_pubsub is not None:
            redis_pubsub.close()
        return


@app.websocket("/ws/live")
async def live_ledger_ws(websocket: WebSocket) -> None:
    await websocket.accept()
    master = master_universe_payload(force=False)
    trigger_map = {
        symbol: float(row["trigger_level"])
        for symbol, row in (master.get("data") or {}).items()
        if row.get("trigger_level") is not None
    }
    symbols = list(trigger_map) or universe_symbols()
    live_feed.subscribe(symbols)
    await websocket.send_json({"type": "status", "status": "connected", "symbols": symbols, "contract": "live_ledger"})
    redis_pubsub = None
    if redis_state.enabled():
        redis_client = redis_state.client()
        if redis_client is not None:
            redis_pubsub = redis_client.pubsub(ignore_subscribe_messages=True)
            redis_pubsub.subscribe(redis_state.TICK_CHANNEL)
    last_sent: dict[str, tuple[Any, Any]] = {}
    try:
        while True:
            raw_ticks: list[dict[str, Any]] = []
            if redis_pubsub is not None:
                message = await asyncio.to_thread(redis_pubsub.get_message, True, 1.0)
                if message and message.get("type") == "message":
                    try:
                        raw_ticks.append(json.loads(message.get("data") or "{}"))
                    except Exception:
                        pass
            else:
                raw_ticks.extend(live_feed.snapshot(symbols).values())
                await asyncio.sleep(1)
            for tick in raw_ticks:
                payload = live_ledger_payload(tick, trigger_map)
                if payload is None:
                    continue
                key = (payload.get("ltp"), payload.get("status"))
                if last_sent.get(payload["symbol"]) == key:
                    continue
                last_sent[payload["symbol"]] = key
                await websocket.send_json(payload)
            await asyncio.sleep(0.02 if redis_pubsub is not None else 0)
    except WebSocketDisconnect:
        if redis_pubsub is not None:
            redis_pubsub.close()
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
    master = master_universe_payload(force=True)
    focus = build_focus_dashboard_payload(force=True, scan_limit=int(os.getenv("DAILY_FOCUS_SCAN_LIMIT", "60")))
    return {
        "universe_refresh": universe,
        "market_refresh": market,
        "alert_scan": alerts,
        "overnight_audit": {"generated_at": master["generated_at"], "symbols": len(master.get("data", {}))},
        "focus_dashboard": {"generated_at": focus["generated_at"], "symbols": focus.get("scan_meta", {}).get("total_scored")},
    }


@app.get("/api/portfolio")
def get_portfolio() -> dict[str, Any]:
    holdings = _run_async_sync(load_portfolio_from_db()) if DATABASE_ENABLED else PORTFOLIO_HOLDINGS
    return {
        "holdings": holdings,
        "max_sector_positions": 3,
        "max_industry_positions": 2,
        "max_sector_exposure": float(os.getenv("MAX_SECTOR_EXPOSURE", "0.25")),
        "max_correlation": float(os.getenv("MAX_PORTFOLIO_CORRELATION", "0.65")),
        "total_account_value": float(os.getenv("ACCOUNT_SIZE", "1000000")),
        "persistence": "database" if DATABASE_ENABLED else "memory",
    }


@app.post("/api/portfolio")
def update_portfolio(payload: dict[str, Any]) -> dict[str, Any]:
    holdings = payload.get("holdings", [])
    if not isinstance(holdings, list):
        raise HTTPException(status_code=400, detail="holdings must be a list")
    PORTFOLIO_HOLDINGS.clear()
    cleaned_holdings: list[dict[str, Any]] = []
    for holding in holdings:
        symbol = str(holding.get("symbol", "")).upper()
        company = find_company(symbol)
        if company:
            item = {
                "symbol": symbol,
                "sector": company["sector"],
                "industry": company.get("industry", ""),
                "quantity": holding.get("quantity"),
                "shares_held": holding.get("shares_held", holding.get("quantity")),
                "entry_price": holding.get("entry_price"),
                "last_price": holding.get("last_price", holding.get("price")),
                "current_value": holding.get("current_value"),
            }
            PORTFOLIO_HOLDINGS.append(item)
            cleaned_holdings.append(item)
    if DATABASE_ENABLED:
        _run_async_sync(save_portfolio_to_db(cleaned_holdings))
    MASTER_UNIVERSE_CACHE.update({"generated_at": None, "payload": None})
    FOCUS_DASHBOARD_CACHE.update({"generated_at": None, "payload": None})
    return get_portfolio()


@app.get("/api/stocks/{symbol}")
def stock_detail(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    stocks = build_scored_universe()
    for stock in stocks:
        if stock["symbol"] == symbol:
            stock["trade_state"] = advance_trade_state(stock)
            stock["score_history"] = SCORE_HISTORY.get(symbol, [])
            return stock
    detail = dynamic_universe_stock_detail(symbol)
    detail["score_history"] = SCORE_HISTORY.get(symbol, [])
    return detail


@app.get("/api/thesis/{symbol}/premium")
def premium_thesis(symbol: str) -> dict[str, Any]:
    stock = stock_detail(symbol)
    try:
        return {"symbol": symbol.upper(), **generate_premium_thesis(stock)}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Premium thesis generation failed: {exc}") from exc


@app.get("/api/trade-state/{symbol}")
def get_trade_state(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    persisted = _run_async_sync(load_trade_state_from_db(symbol)) if DATABASE_ENABLED else None
    if persisted:
        return {"symbol": symbol, "trade_state": persisted, "persistence": "database"}
    detail = stock_detail(symbol)
    return {"symbol": symbol, "trade_state": detail["trade_state"], "persistence": "memory"}


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
    if DATABASE_ENABLED:
        _run_async_sync(save_trade_state_to_db(symbol, state))
    return {"symbol": symbol, "trade_state": state, "persistence": "database" if DATABASE_ENABLED else "memory"}


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
        "net_income",
        "operating_cash_flow",
        "cash_flow_investing",
        "average_total_assets",
        "ebitda",
        "receivables_growth",
        "revenue_growth",
        "cash_conversion_cycle_days",
        "previous_cash_conversion_cycle_days",
        "altman_z_score",
        "piotroski_f_score",
        "beneish_m_score",
        "next_earnings_date",
        "forward_pe",
        "forward_profit_growth",
        "pb",
        "roa",
        "nim",
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
        "tailwind_as_of",
        "as_of",
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
        "fundamentals_snapshots": payload.get("fundamentals_snapshots") or payload.get("fundamentals_snapshot") or [],
        "allow_lookahead_backtest": bool(payload.get("allow_lookahead_backtest", False)),
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
    merge_free_fundamentals(symbol, company, connector_notes)
    bars, corporate_actions_applied = load_research_bars(symbol, connector_notes, min_bars=60)
    try:
        intraday_bars = fetch_yahoo_intraday(symbol)
        if bars and intraday_bars:
            latest = intraday_bars[-1]
            bars[-1]["close"] = latest["close"]
            bars[-1]["high"] = max(float(bars[-1]["high"]), float(latest["high"]))
            bars[-1]["low"] = min(float(bars[-1]["low"]), float(latest["low"]))
            bars[-1]["volume"] = max(int(bars[-1]["volume"]), int(latest.get("volume", 0) or 0))
            connector_notes.append("Latest 5-minute price merged into daily history")
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"Intraday quote merge failed: {exc}")
    if bars:
        bars = merge_quote_into_bars(symbol, bars, connector_notes)
    if len(bars) < 60:
        connector_notes.append("Yahoo Finance unavailable or insufficient bars")
        try:
            bars = fetch_alpha_vantage_daily(symbol)
            if len(bars) >= 60:
                connector_notes.append("Alpha Vantage daily bars loaded")
                bars = merge_quote_into_bars(symbol, bars, connector_notes)
        except Exception as exc:  # pragma: no cover - network dependent
            connector_notes.append(f"Alpha Vantage fallback failed: {exc}")
            bars = []
    if len(bars) < 60:
        connector_notes.append("Using seeded OHLCV bars")
        bars = DATASET["bars"][symbol]
    company["corporate_actions_applied"] = corporate_actions_applied

    news_events: list[dict[str, Any]] = []
    try:
        provider_news = fetch_market_news(symbol, company["name"], max_records=12, bypass_cache=True)
        news_events.extend(provider_news)
        if provider_news:
            connector_notes.append("News loaded from configured NewsAPI plus no-key fallbacks")
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"News refresh failed: {exc}")
    company["events"] = company.get("events", []) + news_events[:10]

    market = market_support_score(DATASET["market"])
    scored = final_decision(company, bars, DATASET["benchmark_bars"], market)
    scored["trade_state"] = advance_trade_state(scored)
    scored["connector_notes"] = connector_notes or ["Live refresh succeeded"]
    scored.setdefault("data_quality_gate", {})["corporate_actions_applied"] = corporate_actions_applied
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
            "PRICE_DATA_PROVIDER optional: auto, twelve_data, finnhub, fmp, alpha_vantage, yahoo",
            "DATA_SOURCE_PREFERENCE optional: nsefin, auto, or yahoo. nsefin prioritises official/free NSE wrappers.",
            "ENABLE_CORPORATE_ACTIONS_ADJUSTMENT=true applies split/bonus adjustments when nsemine returns actions.",
            "NSE_BHAVCOPY_CACHE_HOURS optional cache horizon for official bhavcopy refresh jobs.",
            "NSE_FUNDAMENTALS_REFRESH_DAYS optional cadence for free NSE fundamentals refresh jobs.",
            "TWELVE_DATA_API_KEY optional quote provider",
            "FINNHUB_API_KEY optional quote provider",
            "FMP_API_KEY optional quote provider",
            "ALPHA_VANTAGE_API_KEY or ALPHAVANTAGE_API_KEY optional quote/daily fallback",
            "NEWSAPI_API_KEY or NEWS_API_KEY optional news provider",
            "ALPHA_VANTAGE_SYMBOL_<SYMBOL> for custom symbol mapping",
            "TWELVE_DATA_SYMBOL_<SYMBOL>, FINNHUB_SYMBOL_<SYMBOL>, FMP_SYMBOL_<SYMBOL>, or PRICE_SYMBOL_<SYMBOL> for custom provider symbols",
            "REDIS_URL optional live tick state/pubsub store",
            "SHOONYA_IN_APP_FEED=false when running feed_worker.py as a separate worker",
            "ENABLE_WEEKLY_SCAN=true to run the scheduled liquid-universe scan",
        ],
    }
