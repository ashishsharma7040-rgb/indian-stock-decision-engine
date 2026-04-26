from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import time
import threading
from copy import deepcopy
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any

from fastapi import BackgroundTasks, Body, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

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
from scoring_engine import event_strength_score, final_decision, market_support_score, pct_distance, validate_scan_history
from seed_data import WATCH_SYMBOLS, build_demo_dataset
from universe_store import UNIVERSE_STORE

try:
    from bhavcopy_loader import (
        database_counts,
        ensure_recent_bhavcopy_in_db,
        historical_bars_from_db,
        latest_bhavcopy_rows_from_db,
        latest_fundamentals_from_db,
        run_eod_update,
    )
    from database import AsyncSessionLocal, DATABASE_ENABLED, database_status, dispose_database_engine, init_db
    from models import ActivePortfolio, ScanResult, ScanRun, TradeState as PersistentTradeState
    from sqlalchemy import delete, func, select, text
except Exception as exc:  # pragma: no cover - optional DB dependencies
    DATABASE_ENABLED = False
    DB_IMPORT_ERROR = str(exc)
    AsyncSessionLocal = None  # type: ignore
    ActivePortfolio = None  # type: ignore
    ScanResult = None  # type: ignore
    ScanRun = None  # type: ignore
    PersistentTradeState = None  # type: ignore
    delete = None  # type: ignore
    func = None  # type: ignore
    select = None  # type: ignore
    text = None  # type: ignore
    database_counts = None  # type: ignore
    ensure_recent_bhavcopy_in_db = None  # type: ignore
    historical_bars_from_db = None  # type: ignore
    latest_bhavcopy_rows_from_db = None  # type: ignore
    latest_fundamentals_from_db = None  # type: ignore
    run_eod_update = None  # type: ignore
    init_db = None  # type: ignore
    dispose_database_engine = None  # type: ignore

    def database_status() -> dict[str, Any]:  # type: ignore
        return {"enabled": False, "url_configured": bool(os.getenv("DATABASE_URL")), "import_error": DB_IMPORT_ERROR}

ENRICH_IMPORT_ERROR: str | None = None
try:
    from enrichment_worker import run_enrichment_pipeline
except Exception as exc:  # pragma: no cover - optional DB/network dependencies
    ENRICH_IMPORT_ERROR = str(exc)
    run_enrichment_pipeline = None  # type: ignore


APP_NAME = "Indian Stock Decision Engine"
BASE_DIR = Path(__file__).resolve().parent
DATASET = build_demo_dataset()
DATASET_LOCK = threading.RLock()
UNIVERSE_STORE.load_seed_companies(DATASET["companies"])
TRADE_STATES: dict[str, dict[str, Any]] = {}
PORTFOLIO_HOLDINGS: list[dict[str, Any]] = []
EOD_TASK_STATUS: dict[str, Any] = {"status": "idle", "updated_at": None, "result": None, "error": None}
YAHOO_ENRICH_STATUS: dict[str, Any] = {
    "status": "idle",
    "progress": 0,
    "message": "Yahoo enrichment has not run yet.",
    "started_at": None,
    "updated_at": None,
    "finished_at": None,
    "result": None,
    "error": None,
}
YAHOO_ENRICH_LOCK = threading.RLock()
YAHOO_ENRICH_TASK: asyncio.Task | None = None
YAHOO_ENRICH_STATUS_PATH = Path(os.getenv("YAHOO_ENRICH_STATUS_PATH", "/tmp/yahoo_enrich_status.json" if os.name != "nt" else str(BASE_DIR / ".yahoo_enrich_status.json")))
PRICE_REFRESH_STATUS: dict[str, Any] = {"updated_at": None, "watch_updated_at": None, "last_error": None}
LATEST_QUOTES: dict[str, dict[str, Any]] = {}
_event_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_bars_cache: dict[str, tuple[float, dict[str, Any]]] = {}
SCORE_HISTORY: dict[str, list[dict[str, Any]]] = {}
WEEKLY_SCAN_STATUS: dict[str, Any] = {"status": "idle", "updated_at": None, "results": [], "error": None}
MASTER_UNIVERSE_CACHE: dict[str, Any] = {"generated_at": None, "payload": None}
FOCUS_DASHBOARD_CACHE: dict[str, Any] = {"generated_at": None, "payload": None}
FOCUS_DASHBOARD_TTL_SECONDS = 300
FULL_NSE_SCAN_STATUS: dict[str, Any] = {
    "status": "idle",
    "started_at": None,
    "updated_at": None,
    "finished_at": None,
    "error": None,
    "progress": 0,
    "message": "No full NSE scan has run yet.",
    "scan_meta": {},
}
FULL_NSE_SCAN_CACHE: dict[str, Any] = {"generated_at": None, "payload": None}
FULL_NSE_SCAN_LOCK = threading.RLock()
FULL_NSE_SCAN_THREAD: threading.Thread | None = None
FULL_NSE_SCAN_STATUS_PATH = Path(os.getenv("FULL_NSE_SCAN_STATUS_PATH", "/tmp/full_nse_scan_status.json" if os.name != "nt" else str(BASE_DIR / ".full_nse_scan_status.json")))
INDIVIDUAL_STOCK_CACHE: dict[str, dict[str, Any]] = {}
INDIVIDUAL_STOCK_CACHE_LOCK = threading.RLock()
INDIVIDUAL_STOCK_MARKET_TTL_SECONDS = max(15, int(os.getenv("INDIVIDUAL_STOCK_MARKET_TTL_SECONDS", "45")))
INDIVIDUAL_STOCK_OFF_MARKET_TTL_SECONDS = max(
    INDIVIDUAL_STOCK_MARKET_TTL_SECONDS,
    int(os.getenv("INDIVIDUAL_STOCK_OFF_MARKET_TTL_SECONDS", "300")),
)
INDIVIDUAL_STOCK_ASYNC_TIMEOUT_SECONDS = max(
    3.0,
    float(os.getenv("INDIVIDUAL_STOCK_ASYNC_TIMEOUT_SECONDS", "6")),
)
INDIVIDUAL_STOCK_PROVIDER_TIMEOUT_SECONDS = max(
    3.0,
    float(os.getenv("INDIVIDUAL_STOCK_PROVIDER_TIMEOUT_SECONDS", "6")),
)

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


def require_admin_secret(secret: str | None) -> None:
    expected = os.getenv("ADMIN_SECRET", "").strip()
    if not expected:
        raise HTTPException(status_code=403, detail="ADMIN_SECRET is not configured on the backend.")
    if secret != expected:
        raise HTTPException(status_code=403, detail="Invalid admin secret")


def is_market_hours() -> bool:
    stamp = now_ist()
    if stamp.weekday() >= 5:
        return False
    return dt_time(9, 15) <= stamp.time() <= dt_time(15, 30)


def live_debug_payload() -> dict[str, Any]:
    status = live_feed.status()
    missing = status.get("missing_credentials") or []
    configured = bool(status.get("configured"))
    feed_open = bool(status.get("feed_open"))
    sdk_available = status.get("sdk_available")
    last_error = status.get("last_error")

    if not configured:
        next_step = "Set the missing Shoonya Render environment variables, then restart the service."
    elif sdk_available is False:
        next_step = "Install or verify the Shoonya SDK package on Render, then redeploy."
    elif last_error:
        next_step = "Review Render logs, submit a fresh OTP if needed, and confirm Render can reach Shoonya."
    elif not feed_open:
        next_step = "Submit a fresh Shoonya OTP/TOTP or wait for auto reconnect, then retry the live feed."
    else:
        next_step = "Live feed is healthy. Keep subscriptions limited to focus, selected, and watchlist symbols."

    return {
        "configured": configured,
        "status": status.get("status"),
        "feed_open": feed_open,
        "missing_credentials": missing,
        "last_error": last_error,
        "sdk_available": sdk_available,
        "trading_enabled": bool(status.get("trading_enabled")),
        "subscribed_symbols": status.get("subscribed_symbols", []),
        "subscribed_count": status.get("subscribed_count", 0),
        "resolved_tokens_count": status.get("resolved_tokens_count", 0),
        "runtime_twofa_set_at": status.get("runtime_twofa_set_at"),
        "accepted_env_names": status.get("accepted_env_names", {}),
        "config_message": status.get("config_message"),
        "next_step": next_step,
    }


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


def merge_quote_into_bars(
    symbol: str,
    bars: list[dict[str, Any]],
    connector_notes: list[str] | None = None,
    timeout_seconds: float | None = None,
) -> list[dict[str, Any]]:
    if not bars:
        return bars
    try:
        quote = _run_sync_with_timeout(
            fetch_market_quote,
            symbol,
            timeout_seconds=timeout_seconds or INDIVIDUAL_STOCK_PROVIDER_TIMEOUT_SECONDS,
        )
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
    """Refresh bhavcopy, Nifty, breadth, and sector proxy momentum daily without manual clicks."""
    await asyncio.sleep(40)
    last_refresh_date: str | None = None
    while True:
        try:
            stamp = now_ist()
            should_refresh = stamp.weekday() < 5 and stamp.time() >= dt_time(16, 10)
            today_key = stamp.date().isoformat()
            if should_refresh and last_refresh_date != today_key:
                await asyncio.to_thread(refresh_nse_universe)
                if DATABASE_ENABLED and ensure_recent_bhavcopy_in_db is not None:
                    await ensure_recent_bhavcopy_in_db(force=True)
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


def _run_async_sync(coro: Any, timeout_seconds: float | None = None) -> Any:
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
    thread.join(timeout=timeout_seconds or float(os.getenv("ASYNC_SYNC_TIMEOUT_SECONDS", "90")))
    if "error" in result:
        raise result["error"]
    if "value" not in result:
        raise RuntimeError("Async provider call timed out")
    return result["value"]


def _run_sync_with_timeout(func: Any, *args: Any, timeout_seconds: float | None = None, **kwargs: Any) -> Any:
    result: dict[str, Any] = {}

    def runner() -> None:
        try:
            result["value"] = func(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - defensive bridge
            result["error"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds or INDIVIDUAL_STOCK_PROVIDER_TIMEOUT_SECONDS)
    if "error" in result:
        raise result["error"]
    if "value" not in result:
        raise RuntimeError(f"{getattr(func, '__name__', 'provider')} timed out")
    return result["value"]


def append_debug_stage(stages: list[dict[str, Any]] | None, name: str, started_at: float, **extra: Any) -> None:
    if stages is None:
        return
    stages.append({"stage": name, "ms": round((time.perf_counter() - started_at) * 1000, 1), **extra})


def maybe_adjust_for_corporate_actions(
    symbol: str,
    bars: list[dict[str, Any]],
    notes: list[str],
    timeout_seconds: float | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    if not bars or str(os.getenv("ENABLE_CORPORATE_ACTIONS_ADJUSTMENT", "true")).lower() not in {"1", "true", "yes"}:
        return bars, False
    try:
        actions = _run_async_sync(
            fetch_corporate_actions_for_symbol(symbol),
            timeout_seconds=timeout_seconds or INDIVIDUAL_STOCK_ASYNC_TIMEOUT_SECONDS,
        )
        if actions:
            notes.append(f"Applied {len(actions)} NSE corporate-action adjustment(s)")
            return adjust_ohlcv_for_actions(bars, actions), True
        notes.append("No NSE split/bonus adjustment required from corporate-action scan")
        return bars, True
    except Exception as exc:  # pragma: no cover - optional provider dependent
        if "timed out" in str(exc).lower():
            notes.append("Corporate-action scan timed out; using unadjusted bars for now")
        else:
            notes.append(f"Corporate-action scan unavailable: {exc}")
        return bars, False


def merge_free_fundamentals(
    symbol: str,
    company: dict[str, Any],
    notes: list[str],
    timeout_seconds: float | None = None,
) -> None:
    try:
        fundamentals = _run_async_sync(
            fetch_free_fundamentals(symbol),
            timeout_seconds=timeout_seconds or INDIVIDUAL_STOCK_ASYNC_TIMEOUT_SECONDS,
        )
        if fundamentals:
            company.setdefault("fundamentals", {}).update({key: value for key, value in fundamentals.items() if value not in (None, "")})
            notes.append("Free NSE fundamentals merged from niftyterminal")
    except Exception as exc:  # pragma: no cover - optional provider dependent
        if "timed out" in str(exc).lower():
            notes.append("Free NSE fundamentals timed out")
        else:
            notes.append(f"Free NSE fundamentals unavailable: {exc}")


def load_research_bars(
    symbol: str,
    connector_notes: list[str],
    min_bars: int = 210,
    timeout_seconds: float | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    timeout = timeout_seconds or INDIVIDUAL_STOCK_PROVIDER_TIMEOUT_SECONDS
    try:
        bars = _run_sync_with_timeout(fetch_official_ohlcv, symbol, days=260, timeout_seconds=timeout)
    except Exception as exc:
        bars = []
        connector_notes.append(f"Official NSE bhavcopy history failed: {exc}")
    corporate_actions_applied = False
    if len(bars) >= min_bars:
        connector_notes.append("Official NSE bhavcopy history loaded via nsefin")
        bars, corporate_actions_applied = maybe_adjust_for_corporate_actions(symbol, bars, connector_notes, timeout_seconds=timeout)
        return bars, corporate_actions_applied
    if bars:
        connector_notes.append(f"NSE bhavcopy history returned only {len(bars)} bars; falling back to chart providers")
    try:
        bars = _run_sync_with_timeout(
            fetch_yahoo_chart,
            symbol,
            range_value="2y",
            interval="1d",
            timeout_seconds=timeout,
        )
        if len(bars) >= min_bars:
            connector_notes.append("Yahoo Finance 2-year daily history loaded as fallback")
        else:
            connector_notes.append(f"Yahoo Finance returned only {len(bars)} daily bars")
    except Exception as exc:  # pragma: no cover - network dependent
        bars = []
        connector_notes.append(f"Yahoo Finance history failed: {exc}")
    if bars:
        bars, corporate_actions_applied = maybe_adjust_for_corporate_actions(symbol, bars, connector_notes, timeout_seconds=timeout)
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
        min_chart_bars = max(20, official_days // 3)
        if DATABASE_ENABLED and historical_bars_from_db is not None:
            try:
                payload = _run_async_sync(historical_bars_from_db([symbol], days=official_days, min_bars=min_chart_bars))
                bars = (payload.get("bars_by_symbol") or {}).get(symbol.upper()) or []
                if bars:
                    source = "Supabase Yahoo-enriched daily history, independent of scoring payload"
            except Exception:
                bars = []
        if not bars:
            bars = fetch_official_ohlcv(symbol, days=official_days)
        if len(bars) >= max(20, official_days // 3):
            if source == "Yahoo Finance chart endpoint, independent of scoring payload":
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


STATIC_SECTOR_MEDIANS = load_static_sector_medians()


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
    medians: dict[str, dict[str, float]] = deepcopy(STATIC_SECTOR_MEDIANS)
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
    sector = row.get("sector") or "Unclassified"
    fundamentals = deepcopy(NEUTRAL_FUNDAMENTALS)
    fundamentals["sector_medians"] = deepcopy(STATIC_SECTOR_MEDIANS.get(sector) or STATIC_SECTOR_MEDIANS.get("Unclassified") or {})
    return {
        "symbol": str(row.get("symbol", "")).upper(),
        "name": row.get("name") or row.get("symbol"),
        "sector": sector,
        "industry": row.get("industry") or "NSE Equity",
        "market_cap_cr": row.get("market_cap_cr"),
        "delivery_pct": row.get("delivery_pct"),
        "delivery_qty": row.get("delivery_qty"),
        "avg_volume_50d": row.get("avg_volume_50d"),
        "fundamentals": fundamentals,
        "tailwind": deepcopy(NEUTRAL_TAILWIND),
        "tailwind_factors": [
            "This stock is loaded from the NSE searchable universe.",
            "Missing core business fields fall back to sector medians until you add direct quarterly research coverage.",
        ],
        "events": [],
    }


NON_EQUITY_SCAN_PATTERNS = tuple(
    token.strip().upper()
    for token in os.getenv(
        "NON_EQUITY_SCAN_PATTERNS",
        "ETF,BEES,LIQUID,GILT,MAFANG,MAKEINDIA,ABSPSE,MON100,NIFTY,SENSEX,PSUBNK",
    ).split(",")
    if token.strip()
)


def is_non_equity_scan_instrument(row: dict[str, Any]) -> bool:
    symbol = str(row.get("symbol", "")).upper()
    name = str(row.get("name", "")).upper()
    series = str(row.get("series") or "EQ").upper()
    if series and series != "EQ":
        return True
    return any(token in symbol or token in name for token in NON_EQUITY_SCAN_PATTERNS)


def build_scan_proxy_bars_from_row(row: dict[str, Any], days: int = 260) -> list[dict[str, Any]]:
    """Build a conservative EOD scan proxy when full history is unavailable.

    This is not used for backtests. It prevents one failed history provider from making
    the full NSE scan empty. The final bar is the actual NSE bhavcopy OHLCV row; prior
    bars are deterministic context so the existing engine can still apply its liquidity,
    base, ATR, and breakout mechanics with a clear data-quality warning.
    """
    close = float(row.get("price") or row.get("close") or 0)
    if close <= 0:
        return []
    symbol = str(row.get("symbol") or "NSE")
    seed = sum(ord(char) for char in symbol)
    turnover = float(row.get("turnover") or 0)
    volume = int(float(row.get("volume") or 0) or max(turnover / max(close, 1), 100_000))
    drift = ((seed % 17) - 6) / 10_000
    cycle = ((seed % 11) + 5) / 10_000
    current = close * (0.82 + (seed % 9) / 100)
    bars: list[dict[str, Any]] = []
    start = datetime.now(timezone.utc).date() - timedelta(days=days * 7 // 5 + 20)
    day = start
    while len(bars) < days:
        if day.weekday() >= 5:
            day += timedelta(days=1)
            continue
        index = len(bars)
        progress = index / max(days - 1, 1)
        target_pull = (close / max(current, 0.01)) ** (1 / max(days - index, 1))
        wave = 1 + drift + cycle * (1 if (index // 7) % 2 == 0 else -1)
        current *= max(0.985, min(1.025, target_pull * wave))
        if index == days - 1:
            open_price = float(row.get("open") or current)
            high = float(row.get("high") or max(open_price, close) * 1.01)
            low = float(row.get("low") or min(open_price, close) * 0.99)
            current = close
        else:
            open_price = current * (1 + (((seed + index) % 9) - 4) / 1000)
            range_pct = 0.008 + ((seed + index) % 8) / 1000
            high = max(open_price, current) * (1 + range_pct)
            low = min(open_price, current) * (1 - range_pct)
        vol_scale = 0.75 + ((seed + index) % 13) / 25
        bars.append(
            {
                "datetime": day.isoformat(),
                "open": round(open_price, 2),
                "high": round(max(high, open_price, current), 2),
                "low": round(min(low, open_price, current), 2),
                "close": round(current, 2),
                "volume": max(50_000, int(volume * vol_scale)),
                "source": "NSE EOD scan proxy from latest bhavcopy",
            }
        )
        day += timedelta(days=1)
    return bars


def merge_scan_row_into_history(bars: list[dict[str, Any]], row: dict[str, Any]) -> list[dict[str, Any]]:
    """Overlay the latest bhavcopy row onto enriched history for scan-time price consistency."""
    clean = [dict(bar) for bar in bars if bar.get("close") is not None]
    as_of = str(row.get("as_of") or "")[:10]
    close = row.get("price") if row.get("price") is not None else row.get("close")
    if not as_of or close is None:
        return clean[-260:]
    latest_bar = {
        "datetime": as_of,
        "open": float(row.get("open") or close),
        "high": float(row.get("high") or max(float(row.get("open") or close), float(close))),
        "low": float(row.get("low") or min(float(row.get("open") or close), float(close))),
        "close": float(close),
        "volume": int(float(row.get("volume") or 0)),
        "source": row.get("source") or "Supabase latest bhavcopy overlay",
        "data_source": row.get("source") or "Supabase latest bhavcopy overlay",
        "is_adjusted": False,
    }
    if clean and str(clean[-1].get("datetime") or "")[:10] == as_of:
        clean[-1].update(latest_bar)
    elif not clean or str(clean[-1].get("datetime") or "")[:10] < as_of:
        clean.append(latest_bar)
    clean.sort(key=lambda item: str(item.get("datetime") or "")[:10])
    return clean[-260:]


def merge_database_fundamentals(
    symbol: str,
    company: dict[str, Any],
    connector_notes: list[str],
    timeout_seconds: float | None = None,
) -> None:
    if not DATABASE_ENABLED or latest_fundamentals_from_db is None:
        return
    try:
        payload = _run_async_sync(
            latest_fundamentals_from_db([symbol]),
            timeout_seconds=timeout_seconds or INDIVIDUAL_STOCK_ASYNC_TIMEOUT_SECONDS,
        )
        fundamentals = (payload.get("fundamentals_by_symbol") or {}).get(symbol.upper()) or {}
        if fundamentals:
            company.setdefault("fundamentals", {}).update({key: value for key, value in fundamentals.items() if value not in (None, "")})
            connector_notes.append(f"Supabase fundamentals merged from latest snapshot ({fundamentals.get('fundamentals_as_of', 'unknown date')})")
    except Exception as exc:
        if "timed out" in str(exc).lower():
            connector_notes.append("Supabase fundamentals lookup timed out")
        else:
            connector_notes.append(f"Supabase fundamentals unavailable: {exc}")


def load_database_research_bars(
    symbol: str,
    row: dict[str, Any],
    connector_notes: list[str],
    min_bars: int = 210,
    timeout_seconds: float | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    if not DATABASE_ENABLED or historical_bars_from_db is None:
        return [], False
    try:
        payload = _run_async_sync(
            historical_bars_from_db([symbol], days=260, min_bars=min_bars),
            timeout_seconds=timeout_seconds or INDIVIDUAL_STOCK_ASYNC_TIMEOUT_SECONDS,
        )
        bars = (payload.get("bars_by_symbol") or {}).get(symbol.upper()) or []
        if len(bars) >= min_bars:
            bars = merge_scan_row_into_history(bars, row)
            adjusted = any(bool(bar.get("is_adjusted")) for bar in bars)
            connector_notes.append(
                f"Supabase Yahoo-enriched history loaded ({len(bars)} bars; adjusted={str(adjusted).lower()})"
            )
            return bars, adjusted
        connector_notes.append(
            f"Supabase history not ready for {symbol.upper()}: "
            f"{payload.get('symbols_with_history', 0)}/{payload.get('symbols_requested', 1)} symbols met {min_bars}+ bars"
        )
    except Exception as exc:
        if "timed out" in str(exc).lower():
            connector_notes.append("Supabase history lookup timed out")
        else:
            connector_notes.append(f"Supabase history unavailable: {exc}")
    return [], False


def individual_stock_cache_ttl_seconds(market_open: bool | None = None) -> int:
    current_market_open = is_market_hours() if market_open is None else market_open
    return INDIVIDUAL_STOCK_MARKET_TTL_SECONDS if current_market_open else INDIVIDUAL_STOCK_OFF_MARKET_TTL_SECONDS


def cached_individual_stock(symbol: str) -> dict[str, Any] | None:
    target = symbol.upper()
    now_ts = time.time()
    with INDIVIDUAL_STOCK_CACHE_LOCK:
        entry = INDIVIDUAL_STOCK_CACHE.get(target)
        if not entry:
            return None
        if entry.get("expires_at", 0) <= now_ts:
            INDIVIDUAL_STOCK_CACHE.pop(target, None)
            return None
        payload = deepcopy(entry.get("payload") or {})
    if payload:
        payload["_individual_cache"] = True
    return payload


def store_individual_stock_cache(symbol: str, payload: dict[str, Any], market_open: bool | None = None) -> None:
    target = symbol.upper()
    expires_at = time.time() + individual_stock_cache_ttl_seconds(market_open)
    with INDIVIDUAL_STOCK_CACHE_LOCK:
        INDIVIDUAL_STOCK_CACHE[target] = {"expires_at": expires_at, "payload": deepcopy(payload)}


def cached_stock_from_payload(payload: dict[str, Any] | None, symbol: str) -> dict[str, Any] | None:
    if not payload:
        return None
    target = symbol.upper()
    for key in ("stocks", "top_weekly", "top_monthly", "top_weekly_candidates", "top_monthly_candidates", "avoid_list"):
        rows = payload.get(key) or []
        for row in rows:
            if str(row.get("symbol") or "").upper() == target:
                hit = deepcopy(row)
                hit["_cache_collection"] = key
                return hit
    return None


def cached_stock_sources(symbol: str) -> tuple[dict[str, Any] | None, str | None]:
    target = symbol.upper()
    individual = cached_individual_stock(target)
    if individual:
        return individual, "individual_cache"
    full_scan_payload = FULL_NSE_SCAN_CACHE.get("payload") or restore_full_scan_cache_from_db()
    full_scan_hit = cached_stock_from_payload(full_scan_payload, target)
    if full_scan_hit:
        full_scan_hit.setdefault("connector_notes", []).append("Served from persisted full-scan cache. Use fresh=1 for a live deep fetch.")
        return full_scan_hit, "full_scan_cache"
    focus_payload = FOCUS_DASHBOARD_CACHE.get("payload")
    focus_hit = cached_stock_from_payload(focus_payload, target)
    if focus_hit:
        focus_hit.setdefault("connector_notes", []).append("Served from focus dashboard cache. Use fresh=1 for a live deep fetch.")
        return focus_hit, "focus_dashboard_cache"
    return None, None


def wait_data_stock_detail(
    symbol: str,
    row: dict[str, Any],
    connector_notes: list[str],
    reason: str,
    bars_available: int = 0,
) -> dict[str, Any]:
    current_price = row.get("price") if row.get("price") is not None else row.get("close")
    market = market_support_score(DATASET["market"])
    return {
        "symbol": str(row.get("symbol") or symbol).upper(),
        "name": row.get("name") or symbol.upper(),
        "sector": row.get("sector") or "Unclassified",
        "industry": row.get("industry") or "NSE Equity",
        "price": current_price,
        "change_pct": row.get("change_pct"),
        "weekly_score": 0,
        "monthly_score": 0,
        "weekly_raw_score": 0,
        "monthly_raw_score": 0,
        "business_quality": {"score": 0, "breakdown": {}, "flags": []},
        "sector_tailwind": {"score": 0, "breakdown": {}},
        "event_strength": {"score": 0, "events": []},
        "technical_strength": {"score": 0, "indicators": {}, "checks": {}, "fake_breakout_flags": []},
        "monthly_technical_strength": {"score": 0, "indicators": {}, "checks": {}},
        "market_support": market,
        "risk_penalty": {"score": 0, "breakdown": {}},
        "conviction": "Insufficient Data",
        "verdict": "WAIT_DATA - more valid price history is required before this stock can be actioned.",
        "candidate": False,
        "trade_state": {
            "state": "Waiting Data",
            "reason": reason,
            "last_price": current_price,
        },
        "entry": {
            "candidate_gate": "Blocked until real history and data quality pass",
            "buy_stop_trigger": None,
            "breakout_level": None,
            "pullback": None,
            "aggressive": None,
            "stop": None,
            "target_1": None,
            "target_2": None,
            "support_levels": {},
            "resistance_levels": {},
            "pivot_levels": {},
            "fib_levels": {},
        },
        "action_plan": {
            "action": "WAIT_DATA",
            "confidence": "low",
            "reason_summary": reason,
            "summary": reason,
            "trigger_price": None,
            "pullback_zone": None,
            "aggressive_zone": None,
            "stop": None,
            "target_1": None,
            "target_2": None,
            "passed_gates": [],
            "failed_gates": ["Data quality gate"],
        },
        "data_quality_gate": {
            "pass": False,
            "warning": reason,
            "reason": reason,
            "bars_available": bars_available,
            "actual_completeness_pct": 0,
            "price_data_quality": {
                "issues": [],
                "warnings": [reason],
            },
        },
        "explanation_json": {
            "five_questions": {},
            "thesis": [reason],
            "risk_flags": ["Do not rank/action until data-quality gate passes."],
        },
        "engine_scores": {"final_score": 0, "technical_strength": 0, "fundamental_forensic": 0},
        "premium_tags": [],
        "forensic_audit": {"status": "WAIT_DATA", "warnings": [], "metrics": {}},
        "connector_notes": connector_notes + [reason],
        "universe_row": row,
        "data_mode": "wait_data_detail",
        "history_bars_used": bars_available,
        "ranking_eligible": False,
        "scan_confidence": "low",
        "bars": [],
        "benchmark_bars": [],
    }


def dynamic_universe_stock_detail(
    symbol: str,
    provider_timeout_seconds: float | None = None,
    allow_proxy: bool = False,
    debug_stages: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ensure_universe_loaded()
    row = UNIVERSE_STORE.get(symbol) or seed_row_for_symbol(symbol)
    if not row:
        raise HTTPException(status_code=404, detail=f"{symbol.upper()} was not found in the NSE universe cache")
    company = company_from_universe_row(row)
    connector_notes: list[str] = []
    provider_timeout = provider_timeout_seconds or INDIVIDUAL_STOCK_PROVIDER_TIMEOUT_SECONDS
    async_timeout = max(4.0, min(provider_timeout, INDIVIDUAL_STOCK_ASYNC_TIMEOUT_SECONDS))
    started = time.perf_counter()
    merge_free_fundamentals(symbol, company, connector_notes, timeout_seconds=async_timeout)
    append_debug_stage(debug_stages, "free_fundamentals", started, notes=connector_notes[-1:] if connector_notes else [])
    started = time.perf_counter()
    merge_database_fundamentals(symbol, company, connector_notes, timeout_seconds=async_timeout)
    append_debug_stage(debug_stages, "database_fundamentals", started, notes=connector_notes[-1:] if connector_notes else [])
    started = time.perf_counter()
    bars, corporate_actions_applied = load_database_research_bars(
        symbol,
        row,
        connector_notes,
        min_bars=210,
        timeout_seconds=async_timeout,
    )
    append_debug_stage(debug_stages, "database_history", started, bars=len(bars), source="database")
    data_mode = "supabase_yahoo_history_detail_scored" if bars else "nse_search_official_first_scored"
    if not bars:
        started = time.perf_counter()
        bars, corporate_actions_applied = load_research_bars(
            symbol,
            connector_notes,
            min_bars=210,
            timeout_seconds=provider_timeout,
        )
        append_debug_stage(debug_stages, "provider_history", started, bars=len(bars), source="official_then_yahoo")
    if len(bars) < 210:
        reason = f"Need 210+ valid daily bars before this stock can be scored reliably. Only {len(bars)} bars are available right now."
        if not allow_proxy:
            append_debug_stage(debug_stages, "wait_data", time.perf_counter(), reason=reason, bars=len(bars))
            return wait_data_stock_detail(symbol, row, connector_notes, reason, bars_available=len(bars))
        proxy_bars = build_scan_proxy_bars_from_row(row, days=240)
        if len(proxy_bars) < 200:
            raise HTTPException(
                status_code=503,
                detail=f"{symbol.upper()} is in NSE search, but not enough official/free history is available to score it yet. Notes: {' | '.join(connector_notes)}",
            )
        connector_notes.append(
            "Full historical provider data is unavailable; using NSE EOD proxy context from the latest universe/bhavcopy row for an indicative decision."
        )
        bars = proxy_bars
        corporate_actions_applied = False
        data_mode = "nse_eod_proxy_detail_scored"
    company["corporate_actions_applied"] = corporate_actions_applied
    started = time.perf_counter()
    bars = merge_quote_into_bars(symbol, bars, connector_notes, timeout_seconds=provider_timeout)
    append_debug_stage(debug_stages, "quote_overlay", started, bars=len(bars))
    try:
        started = time.perf_counter()
        news_events = _run_sync_with_timeout(
            fetch_market_news,
            symbol,
            company["name"],
            max_records=10,
            timeout_seconds=provider_timeout,
        )
        company["events"] = news_events
        if news_events:
            connector_notes.append("News loaded from configured NewsAPI plus no-key fallbacks")
        append_debug_stage(debug_stages, "news", started, events=len(news_events))
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"News refresh failed: {exc}")
    market = market_support_score(DATASET["market"])
    started = time.perf_counter()
    scored = apply_live_tick(final_decision(company, bars, DATASET["benchmark_bars"], market))
    append_debug_stage(debug_stages, "final_decision", started)
    scored["trade_state"] = advance_trade_state(scored)
    scored["connector_notes"] = connector_notes
    scored["universe_row"] = row
    scored["data_mode"] = data_mode
    if data_mode == "nse_eod_proxy_detail_scored":
        scored["price_source"] = row.get("source") or "NSE EOD proxy"
        scored["price_as_of"] = row.get("as_of")
        scored.setdefault("data_quality_gate", {}).setdefault("price_data_quality", {}).setdefault("warnings", []).append(
            "Indicative proxy detail: full 200+ day official/licensed history was unavailable for this symbol."
        )
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

    if DATABASE_ENABLED and ensure_recent_bhavcopy_in_db is not None and str(os.getenv("AUTO_EOD_DB_SYNC", "true")).lower() in {"1", "true", "yes"}:
        try:
            eod_result = await ensure_recent_bhavcopy_in_db(force=False)
            EOD_TASK_STATUS.update({"status": eod_result.get("status"), "updated_at": datetime.now(timezone.utc).isoformat(), "result": eod_result, "error": None})
            print(f"[startup] Supabase bhavcopy sync: {eod_result.get('status')}")
        except Exception as exc:  # pragma: no cover - db/network dependent
            EOD_TASK_STATUS.update({"status": "error", "updated_at": datetime.now(timezone.utc).isoformat(), "result": None, "error": str(exc)})
            print(f"[startup] Supabase bhavcopy sync failed: {exc}")

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
        "verdict": stock.get("verdict"),
        "candidate": stock["candidate"],
        "trade_state": stock.get("trade_state"),
        "entry": stock["entry"],
        "data_quality_gate": stock.get("data_quality_gate"),
        "forensic_gate": stock.get("forensic_gate"),
        "rs_rating": technical.get("indicators", {}).get("rs_rating"),
        "price_source": stock.get("price_source"),
        "price_as_of": stock.get("price_as_of"),
        "confidence_interval": stock.get("confidence_interval"),
        "engine_scores": stock.get("engine_scores"),
        "premium_tags": stock.get("premium_tags", []),
        "forensic_audit": stock.get("forensic_audit"),
        "action_plan": stock.get("action_plan"),
        "history_bars_used": stock.get("history_bars_used"),
        "ranking_eligible": stock.get("ranking_eligible", stock.get("data_mode") in {"supabase_yahoo_history_scored", "real_nse_scored", "nse_dynamic_real"}),
        "scan_confidence": stock.get("scan_confidence", "medium"),
        "portfolio_check": stock.get("portfolio_check"),
        "execution_audit": stock.get("execution_audit"),
        "latest_events": events.get("events", [])[:5],
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
        merge_database_fundamentals(symbol, company, notes)
        bars, corporate_actions_applied = load_database_research_bars(symbol, row, notes, min_bars=60)
        data_mode = "supabase_yahoo_history_scored" if bars else "real_nse_scored"
        if not bars:
            bars, corporate_actions_applied = load_research_bars(symbol, notes, min_bars=60)
        if len(bars) < 60:
            proxy_bars = build_scan_proxy_bars_from_row(row)
            if len(proxy_bars) < 60:
                return None
            bars = proxy_bars
            corporate_actions_applied = False
            notes.append("Historical provider unavailable; using NSE EOD scan proxy from latest bhavcopy row")
            data_mode = "nse_eod_proxy_scored"
        company["corporate_actions_applied"] = corporate_actions_applied
        try:
            bars = merge_quote_into_bars(symbol, bars, notes)
        except Exception as exc:
            notes.append(f"Live quote merge skipped: {exc}")
        try:
            company["events"] = get_symbol_events(symbol, fresh=False).get("events", [])[:8]
        except Exception:
            company["events"] = []
        benchmark_bars = market_result.get("_benchmark_bars") or DATASET["benchmark_bars"]
        scored = apply_live_tick(final_decision(company, bars, benchmark_bars, market_result))
        scored["trade_state"] = advance_trade_state(scored)
        scored["connector_notes"] = notes
        scored["universe_row"] = row
        scored["data_mode"] = data_mode
        scored["is_seed_demo"] = str(row.get("source", "")).lower() == "seed_research" and not row.get("as_of")
        return scored
    except Exception:
        return None


def score_full_scan_row_fast(
    row: dict[str, Any],
    market_result: dict[str, Any],
    db_history: list[dict[str, Any]] | None = None,
    db_fundamentals: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Fast batch scorer for full NSE scans.

    The full scan must not do per-symbol network work. It uses the refreshed NSE
    universe/bhavcopy row, builds deterministic EOD context bars, and runs the
    existing scoring engine. Expensive live history/news/fundamental refreshes
    remain available when a user opens an individual stock.
    """
    symbol = str(row.get("symbol", "")).upper()
    if not symbol:
        return None
    if is_non_equity_scan_instrument(row):
        return None
    try:
        company = company_from_universe_row(row)
        if db_fundamentals:
            company.setdefault("fundamentals", {}).update({key: value for key, value in db_fundamentals.items() if value not in (None, "")})
        latest_eod_date = str(row.get("as_of") or "")[:10] or None
        company["corporate_actions_applied"] = bool(db_history and any(bar.get("is_adjusted") for bar in db_history))
        company["tailwind_factors"] = [
            "Full NSE scan uses the latest NSE EOD universe row for fast batch ranking.",
            "If Yahoo enrichment exists in Supabase, the scan uses that stored long history and overlays the latest official bhavcopy bar.",
        ]
        price_source = "NSE bhavcopy EOD overlay + Supabase price_history_daily"
        notes = ["Batch full-scan mode: no per-symbol network calls were made."]
        history_rows = list(db_history or [])
        if history_rows:
            bars = merge_scan_row_into_history(history_rows, row)
            notes.append(f"Used {len(bars)} stored historical bars from Supabase price_history_daily.")
        else:
            bars = []
            notes.append("No stored long-history bars found in Supabase price_history_daily.")
        history_validation = validate_scan_history(symbol, bars, latest_eod_date=latest_eod_date)
        if not history_validation.get("pass"):
            reason = str(history_validation.get("reason") or "Need 220+ valid real daily bars before this stock can be ranked.")
            result = build_scan_wait_data_result(
                row,
                market_result,
                reason=reason,
                bars_available=int(history_validation.get("bars_available") or len(bars)),
                latest_bar_date=history_validation.get("latest_bar_date"),
                latest_eod_date=latest_eod_date,
                checks=history_validation.get("checks") or {},
                data_mode="insufficient_history" if int(history_validation.get("bars_available") or 0) < 220 else "data_quality_failed",
            )
            result["connector_notes"] = notes + [reason]
            result["price_source"] = price_source
            result["price_as_of"] = row.get("as_of")
            return result
        benchmark_bars = market_result.get("_benchmark_bars") or DATASET["benchmark_bars"]
        scored = final_decision(company, bars, benchmark_bars, market_result)
        scored["price"] = float(row.get("price") or row.get("close") or scored.get("price") or 0)
        scored["change_pct"] = row.get("change_pct", scored.get("change_pct"))
        scored["trade_state"] = advance_trade_state(scored)
        scored["connector_notes"] = notes
        scored["universe_row"] = row
        scored["data_mode"] = "supabase_yahoo_history_scored"
        scored["price_source"] = price_source
        scored["price_as_of"] = row.get("as_of")
        scored["history_bars_used"] = len(bars)
        scored["ranking_eligible"] = bool(scored.get("data_quality_gate", {}).get("pass"))
        scored["scan_confidence"] = "high"
        scored.setdefault("data_quality_gate", {})
        scored["data_quality_gate"]["scan_history_validation"] = history_validation
        scored["is_seed_demo"] = False
        return scored
    except Exception:
        return None


def compact_full_scan_result(stock: dict[str, Any]) -> dict[str, Any]:
    """Keep only what ranking/UI needs after scoring to avoid RAM blow-ups."""
    compact = dict(stock)
    spark_bars = [
        {"close": float(bar["close"])}
        for bar in (stock.get("bars") or [])[-18:]
        if bar.get("close") is not None
    ]
    compact["bars"] = spark_bars
    compact["benchmark_bars"] = []
    return compact


def prefetch_full_scan_db_batch(
    symbols: list[str],
    *,
    history_days: int = 260,
    history_min_bars: int = 220,
) -> dict[str, Any]:
    history_by_symbol: dict[str, list[dict[str, Any]]] = {}
    fundamentals_by_symbol: dict[str, dict[str, Any]] = {}
    scan_timeout = float(os.getenv("SCAN_DB_TIMEOUT_SECONDS", os.getenv("ASYNC_SYNC_TIMEOUT_SECONDS", "90")))
    history_chunk = max(20, int(os.getenv("SCAN_DB_PREFETCH_CHUNK", "100")))
    fundamentals_chunk = max(50, int(os.getenv("SCAN_FUNDAMENTALS_CHUNK", "200")))
    history_meta: dict[str, Any] = {
        "enabled": bool(DATABASE_ENABLED and historical_bars_from_db is not None),
        "status": "not_used",
        "chunk_size": history_chunk,
        "symbols_requested": len(symbols),
    }
    fundamentals_meta: dict[str, Any] = {
        "enabled": bool(DATABASE_ENABLED and latest_fundamentals_from_db is not None),
        "status": "not_used",
        "chunk_size": fundamentals_chunk,
        "symbols_requested": len(symbols),
    }
    if DATABASE_ENABLED and historical_bars_from_db is not None and symbols:
        try:
            history_payload = _run_async_sync(
                historical_bars_from_db(
                    symbols,
                    days=history_days,
                    min_bars=history_min_bars,
                    chunk_size=history_chunk,
                ),
                timeout_seconds=scan_timeout,
            )
            history_by_symbol = dict(history_payload.get("bars_by_symbol") or {})
            history_meta = {key: value for key, value in history_payload.items() if key != "bars_by_symbol"}
            history_meta["chunk_size"] = history_chunk
        except Exception as exc:
            history_meta = {
                "enabled": True,
                "status": "error",
                "error": str(exc),
                "chunk_size": history_chunk,
                "symbols_requested": len(symbols),
            }
    if DATABASE_ENABLED and latest_fundamentals_from_db is not None and symbols:
        try:
            fundamentals_payload = _run_async_sync(
                latest_fundamentals_from_db(symbols, chunk_size=fundamentals_chunk),
                timeout_seconds=scan_timeout,
            )
            fundamentals_by_symbol = dict(fundamentals_payload.get("fundamentals_by_symbol") or {})
            fundamentals_meta = {
                key: value for key, value in fundamentals_payload.items() if key != "fundamentals_by_symbol"
            }
            fundamentals_meta["chunk_size"] = fundamentals_chunk
        except Exception as exc:
            fundamentals_meta = {
                "enabled": True,
                "status": "error",
                "error": str(exc),
                "chunk_size": fundamentals_chunk,
                "symbols_requested": len(symbols),
            }
    return {
        "history_by_symbol": history_by_symbol,
        "fundamentals_by_symbol": fundamentals_by_symbol,
        "history_meta": history_meta,
        "fundamentals_meta": fundamentals_meta,
    }


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
        "latest_critical_events": critical_events(all_scored),
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


def full_scan_cache_valid(max_age_seconds: int = 1800) -> bool:
    generated_at = FULL_NSE_SCAN_CACHE.get("generated_at")
    payload = FULL_NSE_SCAN_CACHE.get("payload")
    if not generated_at or not payload:
        return False
    meta = payload.get("scan_meta") or {}
    if payload.get("dashboard_mode") != "full_nse_scan":
        return False
    if int(meta.get("total_scored") or 0) <= 0:
        return False
    if not payload.get("stocks"):
        return False
    try:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
        return age.total_seconds() < max_age_seconds
    except Exception:
        return False


def set_full_scan_status(status: str, progress: int, message: str, **extra: Any) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with FULL_NSE_SCAN_LOCK:
        FULL_NSE_SCAN_STATUS.update(
            {
                "status": status,
                "progress": max(0, min(100, int(progress))),
                "message": message,
                "updated_at": now,
                **extra,
            }
        )
        try:
            FULL_NSE_SCAN_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
            FULL_NSE_SCAN_STATUS_PATH.write_text(json.dumps(FULL_NSE_SCAN_STATUS, default=str), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - status persistence is best effort
            logger.debug("Could not persist full scan status: %s", exc)


def persisted_full_scan_status() -> dict[str, Any] | None:
    try:
        if not FULL_NSE_SCAN_STATUS_PATH.exists():
            return None
        payload = json.loads(FULL_NSE_SCAN_STATUS_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def full_scan_worker_stopped_status(status: dict[str, Any], worker_alive: bool) -> dict[str, Any]:
    if worker_alive or str(status.get("status") or "") not in {"queued", "running"}:
        return status
    failed = dict(status)
    failed.update(
        {
            "status": "error",
            "error": "scan_worker_not_alive",
            "message": (
                "Full NSE scan worker stopped before completion. The backend may have restarted, "
                "or the worker crashed. Check Render logs and rerun after the previous cause is fixed."
            ),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return failed


def build_scan_wait_data_result(
    row: dict[str, Any],
    market_result: dict[str, Any],
    *,
    reason: str,
    bars_available: int,
    latest_bar_date: str | None = None,
    latest_eod_date: str | None = None,
    checks: dict[str, Any] | None = None,
    data_mode: str = "insufficient_history",
) -> dict[str, Any]:
    symbol = str(row.get("symbol", "")).upper()
    price = float(row.get("price") or row.get("close") or 0)
    action_plan = {
        "action": "WAIT_DATA",
        "confidence": "low",
        "reason_summary": reason,
        "summary": reason,
        "verdict": reason,
    }
    return {
        "symbol": symbol,
        "name": row.get("name") or symbol,
        "sector": row.get("sector") or "Unclassified",
        "industry": row.get("industry") or "NSE Equity",
        "market_cap_cr": row.get("market_cap_cr"),
        "price": price,
        "change_pct": row.get("change_pct", 0),
        "weekly_score": 0,
        "monthly_score": 0,
        "weekly_raw_score": None,
        "monthly_raw_score": None,
        "engine_scores": {
            "final_score": 0,
            "weekly_score": 0,
            "monthly_score": 0,
            "technical_strength": 0,
            "fundamental_forensic": None,
            "business_quality": 0,
            "risk_penalty": 0,
        },
        "confidence_interval": {"label": "Weak", "width": 0, "numeric": 0},
        "business_quality": {"score": 0, "breakdown": {}, "data_quality": {"completeness_pct": 0, "missing_fields": []}},
        "sector_tailwind": {"score": 50, "breakdown": {}},
        "event_strength": {"score": 0, "events": []},
        "monthly_event_strength": {"score": 0, "events": []},
        "technical_strength": {"score": 0, "entry": {}, "indicators": {"close": price}},
        "monthly_technical_strength": {"score": 0},
        "market_support": market_result,
        "risk_penalty": {"score": 0, "breakdown": {}},
        "candidate": False,
        "conviction": "Insufficient Data",
        "verdict": reason,
        "premium_tags": [],
        "data_quality_gate": {
            "pass": False,
            "reason": reason,
            "bars_available": bars_available,
            "latest_bar_date": latest_bar_date,
            "latest_eod_date": latest_eod_date,
            "checks": checks or {},
            "warning": reason,
            "price_data_quality": {"pass": False, "issues": [reason], "warnings": []},
        },
        "forensic_gate": {"pass": True, "flags": [], "hard_fail": False},
        "forensic_audit": {"status": "PASS", "score": None, "hard_fails": [], "warnings": []},
        "trade_state": {"state": "Screened", "reason": reason, "breakout_level": None, "last_price": round(price, 2), "stop": None},
        "entry": {"candidate_gate": reason, "position_sizing": {}},
        "action_plan": action_plan,
        "portfolio_check": {"pass": True},
        "execution_audit": {},
        "exit_rules": [],
        "explanation_json": {"thesis": [reason], "five_questions": {}, "risk_flags": []},
        "fundamentals": {"sector_medians": deepcopy(STATIC_SECTOR_MEDIANS.get(row.get("sector") or "Unclassified") or {})},
        "tailwind_factors": [],
        "bars": [],
        "benchmark_bars": [],
        "connector_notes": [reason],
        "universe_row": row,
        "data_mode": data_mode,
        "price_source": "Supabase official latest bhavcopy",
        "price_as_of": row.get("as_of"),
        "history_bars_used": bars_available,
        "ranking_eligible": False,
        "scan_confidence": "low",
        "is_seed_demo": False,
    }


def _parse_iso_datetime(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


async def persist_full_scan_payload_to_db(payload: dict[str, Any]) -> int | None:
    if not DATABASE_ENABLED or AsyncSessionLocal is None or ScanRun is None or ScanResult is None:
        return None
    scan_meta = dict(payload.get("scan_meta") or {})
    generated_at = _parse_iso_datetime(payload.get("generated_at")) or datetime.now(timezone.utc)
    started_at = _parse_iso_datetime(scan_meta.get("scan_started_at")) or generated_at
    finished_at = _parse_iso_datetime(scan_meta.get("scan_finished_at")) or generated_at
    top_weekly = list(payload.get("top_weekly") or [])
    top_monthly = list(payload.get("top_monthly") or [])
    weekly_ranks = {str(item.get("symbol")).upper(): index + 1 for index, item in enumerate(top_weekly) if item.get("symbol")}
    monthly_ranks = {str(item.get("symbol")).upper(): index + 1 for index, item in enumerate(top_monthly) if item.get("symbol")}
    meta_payload = {
        "generated_at": payload.get("generated_at"),
        "scan_meta": scan_meta,
        "market_regime": payload.get("market_regime"),
        "latest_critical_events": payload.get("latest_critical_events"),
        "sector_map": payload.get("sector_map"),
        "nse_universe": payload.get("nse_universe"),
        "prices_as_of": payload.get("prices_as_of"),
        "disclaimer": payload.get("disclaimer"),
        "dashboard_mode": payload.get("dashboard_mode"),
    }
    async with AsyncSessionLocal() as session:
        run = ScanRun(
            started_at=started_at,
            finished_at=finished_at,
            status="complete",
            total_symbols=int(scan_meta.get("total_symbols_seen") or scan_meta.get("passed_liquidity") or 0),
            ranking_eligible_count=int(scan_meta.get("ranking_eligible_count") or 0),
            insufficient_history_count=int(scan_meta.get("insufficient_history_count") or 0),
            data_quality_failed_count=int(scan_meta.get("data_quality_failed_count") or 0),
            error=scan_meta.get("error"),
            meta=meta_payload,
        )
        session.add(run)
        await session.flush()
        for stock in payload.get("stocks") or []:
            symbol = str(stock.get("symbol") or "").upper()
            if not symbol:
                continue
            session.add(
                ScanResult(
                    scan_run_id=run.id,
                    symbol=symbol,
                    rank_weekly=weekly_ranks.get(symbol),
                    rank_monthly=monthly_ranks.get(symbol),
                    weekly_score=int(stock.get("weekly_score") or 0),
                    monthly_score=int(stock.get("monthly_score") or 0),
                    conviction=stock.get("conviction"),
                    ranking_eligible=bool(stock.get("ranking_eligible")),
                    data_mode=stock.get("data_mode"),
                    data_quality_gate=stock.get("data_quality_gate") or {},
                    action_plan=stock.get("action_plan") or {},
                    raw_payload=stock,
                )
            )
        await session.commit()
        return int(run.id)


async def load_latest_completed_full_scan_from_db() -> dict[str, Any] | None:
    if not DATABASE_ENABLED or AsyncSessionLocal is None or ScanRun is None or ScanResult is None or select is None:
        return None
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ScanRun)
            .where(ScanRun.status == "complete")
            .order_by(ScanRun.finished_at.desc().nullslast(), ScanRun.id.desc())
            .limit(1)
        )
        run = result.scalars().first()
        if run is None:
            return None
        rows_result = await session.execute(
            select(ScanResult)
            .where(ScanResult.scan_run_id == run.id)
            .order_by(ScanResult.ranking_eligible.desc(), ScanResult.rank_weekly.asc().nullslast(), ScanResult.weekly_score.desc())
        )
        rows = list(rows_result.scalars().all())
    meta = dict(run.meta or {})
    stocks = [dict(row.raw_payload or {}) for row in rows if isinstance(row.raw_payload, dict)]
    top_weekly = [dict(row.raw_payload or {}) for row in sorted((item for item in rows if item.rank_weekly is not None), key=lambda item: int(item.rank_weekly or 0))]
    top_monthly = [dict(row.raw_payload or {}) for row in sorted((item for item in rows if item.rank_monthly is not None), key=lambda item: int(item.rank_monthly or 0))]
    generated_at = meta.get("generated_at") or (run.finished_at.isoformat() if run.finished_at else run.started_at.isoformat() if run.started_at else datetime.now(timezone.utc).isoformat())
    return {
        "status": "success",
        "generated_at": generated_at,
        "dashboard_mode": meta.get("dashboard_mode") or "full_nse_scan",
        "market_regime": meta.get("market_regime") or {},
        "top_weekly": top_weekly,
        "top_monthly": top_monthly,
        "top_weekly_candidates": top_weekly,
        "top_monthly_candidates": top_monthly,
        "latest_critical_events": meta.get("latest_critical_events") or [],
        "sector_map": meta.get("sector_map") or [],
        "stocks": stocks,
        "scan_meta": meta.get("scan_meta") or {},
        "nse_universe": meta.get("nse_universe") or {},
        "prices_as_of": meta.get("prices_as_of") or generated_at,
        "disclaimer": meta.get("disclaimer") or "Research workflow only. Full scan uses free/EOD data and is not investment advice.",
    }


def restore_full_scan_cache_from_db() -> dict[str, Any] | None:
    if FULL_NSE_SCAN_CACHE.get("payload") or not DATABASE_ENABLED or AsyncSessionLocal is None or ScanRun is None:
        return FULL_NSE_SCAN_CACHE.get("payload")
    try:
        payload = _run_async_sync(load_latest_completed_full_scan_from_db(), timeout_seconds=120)
    except Exception:
        return None
    if payload:
        FULL_NSE_SCAN_CACHE.update({"generated_at": payload.get("generated_at"), "payload": payload})
    return payload


def set_yahoo_enrich_status(status: str, progress: int, message: str, **extra: Any) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with YAHOO_ENRICH_LOCK:
        YAHOO_ENRICH_STATUS.update(
            {
                "status": status,
                "progress": max(0, min(100, int(progress))),
                "message": message,
                "updated_at": now,
                **extra,
            }
        )
        try:
            YAHOO_ENRICH_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
            YAHOO_ENRICH_STATUS_PATH.write_text(json.dumps(YAHOO_ENRICH_STATUS, default=str), encoding="utf-8")
        except Exception as exc:  # pragma: no cover - status persistence is best effort
            logger.debug("Yahoo enrichment status persistence failed: %s", exc)


def persisted_yahoo_enrich_status() -> dict[str, Any] | None:
    try:
        if not YAHOO_ENRICH_STATUS_PATH.exists():
            return None
        payload = json.loads(YAHOO_ENRICH_STATUS_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def yahoo_enrich_worker_stopped_status(status: dict[str, Any], worker_alive: bool) -> dict[str, Any]:
    if worker_alive or str(status.get("status") or "") not in {"queued", "running"}:
        return status
    stopped = dict(status)
    stopped.update(
        {
            "status": "interrupted",
            "error": "yahoo_worker_not_alive_resume_available",
            "message": (
                "Yahoo enrichment was interrupted before completion. Start Sync Yahoo data again with force=false; "
                "the backend will skip symbols that already have enough stored history."
            ),
            "resume_available": True,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return stopped


def yahoo_enrich_status_payload() -> dict[str, Any]:
    with YAHOO_ENRICH_LOCK:
        payload = dict(YAHOO_ENRICH_STATUS)
    worker_alive = bool(YAHOO_ENRICH_TASK and not YAHOO_ENRICH_TASK.done())
    if payload.get("status") == "idle":
        persisted = persisted_yahoo_enrich_status()
        if persisted:
            payload = persisted
    payload = yahoo_enrich_worker_stopped_status(payload, worker_alive)
    payload["worker_alive"] = worker_alive
    return payload


def build_full_nse_scan_payload(scan_limit: Any = "all", force: bool = False) -> dict[str, Any]:
    """Score the real investable NSE universe through the existing engine.

    Full-scan picks intentionally exclude unverified seed/demo scores. A seed symbol can
    still qualify only if it appears in the refreshed NSE universe with real bhavcopy
    liquidity data and passes the same investability gates as every other symbol.
    """
    max_workers = max(1, min(int(os.getenv("SCAN_WORKERS", "2")), 6))
    max_limit = int(os.getenv("FULL_NSE_SCAN_MAX", "3000"))
    requested_scan_limit = scan_limit
    if not force and full_scan_cache_valid(max_age_seconds=1800):
        cached = FULL_NSE_SCAN_CACHE["payload"]
        set_full_scan_status("complete", 100, "Serving cached full NSE scan.", scan_meta=cached.get("scan_meta", {}))
        return cached

    with FULL_NSE_SCAN_LOCK:
        if FULL_NSE_SCAN_STATUS.get("status") == "running":
            cached = FULL_NSE_SCAN_CACHE.get("payload")
            if cached:
                return {**cached, "status": "running_cached", "scan_status": dict(FULL_NSE_SCAN_STATUS)}
    set_full_scan_status(
        "running",
        1,
        "Starting real NSE full scan.",
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=None,
        error=None,
        scan_meta={},
    )

    try:
        scan_started_at = datetime.now(timezone.utc).isoformat()
        set_full_scan_status("running", 5, "Refreshing NSE universe and bhavcopy.")
        if force or not UNIVERSE_STORE.count():
            refresh_nse_universe()
        else:
            ensure_universe_loaded()
        universe_meta = UNIVERSE_STORE.meta()
        db_scan_source: dict[str, Any] = {"enabled": False, "status": "not_used"}
        if DATABASE_ENABLED and ensure_recent_bhavcopy_in_db is not None:
            try:
                db_scan_source["sync"] = _run_async_sync(ensure_recent_bhavcopy_in_db(force=False))
            except Exception as exc:
                db_scan_source["sync_error"] = str(exc)
        db_available_rows = 0
        if DATABASE_ENABLED and database_counts is not None:
            try:
                db_counts = _run_async_sync(database_counts())
                db_available_rows = int(db_counts.get("daily_ohlcv") or 0)
                db_scan_source["counts"] = db_counts
            except Exception as exc:
                db_scan_source["count_error"] = str(exc)
        if DATABASE_ENABLED and latest_bhavcopy_rows_from_db is not None:
            try:
                db_probe = _run_async_sync(latest_bhavcopy_rows_from_db(limit=1))
                if db_probe.get("rows") and not db_available_rows:
                    db_available_rows = 1
                db_scan_source.update({key: value for key, value in db_probe.items() if key != "rows"})
            except Exception as exc:
                db_scan_source["probe_error"] = str(exc)
        investable_count = int(universe_meta.get("investable") or 0) or db_available_rows
        limit_value = requested_scan_limit or os.getenv("FULL_NSE_SCAN_LIMIT", "all")
        if str(limit_value).lower() == "all":
            scan_limit = max(20, min(investable_count or max_limit, max_limit))
        else:
            scan_limit = max(20, min(int(limit_value), max_limit))

        set_full_scan_status("running", 10, "Refreshing market regime and breadth.")
        try:
            market_refreshed = refresh_market_data()
            market = market_refreshed.get("market") or market_support_score(DATASET["market"])
        except Exception:
            market = market_support_score(DATASET["market"])
        try:
            benchmark_bars = fetch_yahoo_chart("NIFTY", range_value="2y", interval="1d")
            if len(benchmark_bars) >= 60:
                market["_benchmark_bars"] = benchmark_bars
        except Exception:
            pass

        rows_to_score: list[dict[str, Any]] = []
        scan_filter_mode = "unresolved"
        if DATABASE_ENABLED and latest_bhavcopy_rows_from_db is not None:
            try:
                db_rows_payload = _run_async_sync(latest_bhavcopy_rows_from_db(limit=scan_limit))
                db_scan_source.update({key: value for key, value in db_rows_payload.items() if key != "rows"})
                rows_to_score = list(db_rows_payload.get("rows") or [])
                if rows_to_score:
                    scan_filter_mode = "supabase_bhavcopy_latest"
            except Exception as exc:
                db_scan_source["read_error"] = str(exc)
        if not rows_to_score:
            rows_to_score, scan_filter_mode = UNIVERSE_STORE.scan_candidates(limit=scan_limit, strict=True)
        # Guard only against unpriced/seed-only rows. Turnover and delivery can be
        # missing in some public NSE files, so they must not wipe the full scan.
        excluded_non_equity_rows = [row for row in rows_to_score if is_non_equity_scan_instrument(row)]
        rows_to_score = [row for row in rows_to_score if row.get("price") is not None and not is_non_equity_scan_instrument(row)]
        if not rows_to_score:
            set_full_scan_status("running", 12, "No priced rows in cache; forcing NSE bhavcopy refresh before scanning.")
            refresh_nse_universe()
            rows_to_score, scan_filter_mode = UNIVERSE_STORE.scan_candidates(limit=scan_limit, strict=False)
            excluded_non_equity_rows = [row for row in rows_to_score if is_non_equity_scan_instrument(row)]
            rows_to_score = [row for row in rows_to_score if row.get("price") is not None and not is_non_equity_scan_instrument(row)]
        total_rows = len(rows_to_score)
        latest_eod_date = str(rows_to_score[0].get("as_of") or "")[:10] if rows_to_score else None
        if total_rows == 0:
            raise RuntimeError("Full scan could not find any priced NSE rows. Refresh bhavcopy/universe before scanning.")
        history_meta: dict[str, Any] = {
            "enabled": bool(DATABASE_ENABLED and historical_bars_from_db is not None),
            "status": "batched_prefetch" if DATABASE_ENABLED and historical_bars_from_db is not None else "not_used",
            "symbols_requested": total_rows,
            "symbols_with_history": 0,
            "symbols_with_any_history": 0,
            "batches_loaded": 0,
            "chunk_size": max(20, int(os.getenv("SCAN_DB_PREFETCH_CHUNK", "100"))),
        }
        fundamentals_meta: dict[str, Any] = {
            "enabled": bool(DATABASE_ENABLED and latest_fundamentals_from_db is not None),
            "status": "batched_prefetch" if DATABASE_ENABLED and latest_fundamentals_from_db is not None else "not_used",
            "symbols_requested": total_rows,
            "symbols_with_fundamentals": 0,
            "batches_loaded": 0,
            "chunk_size": max(50, int(os.getenv("SCAN_FUNDAMENTALS_CHUNK", "200"))),
        }
        set_full_scan_status(
            "running",
            15,
            f"Scoring {total_rows} NSE names with {max_workers} workers ({scan_filter_mode}, batched Supabase prefetch).",
            scan_meta={
                "universe_size": UNIVERSE_STORE.count(),
                "passed_liquidity": total_rows,
                "scored_so_far": 0,
                "skipped_so_far": 0,
                "proxy_so_far": 0,
                "insufficient_history_count": 0,
                "data_quality_failed_count": 0,
                "ranked_candidates": 0,
                "excluded_non_equity": len(excluded_non_equity_rows),
                "scan_filter_mode": scan_filter_mode,
                "scan_engine": "fast_eod_batch",
                "db_scan_source": db_scan_source,
                "db_history": history_meta,
                "db_fundamentals": fundamentals_meta,
                "data_mode": "real_nse_only",
                "latest_eod_date": latest_eod_date,
                "scan_started_at": scan_started_at,
            },
        )

        all_scored: list[dict[str, Any]] = []
        skipped = 0
        done_count = 0
        proxy_scored = 0
        enriched_history_scored = 0
        insufficient_history_count = 0
        data_quality_failed_count = 0
        status_lock = threading.Lock()
        scan_batch_size = max(max_workers, int(os.getenv("SCAN_BATCH_SIZE", "150")))

        deep_mode = str(os.getenv("FULL_NSE_SCAN_DEEP_MODE", "")).lower() in {"1", "true", "yes"}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for batch_start in range(0, total_rows, scan_batch_size):
                batch_rows = rows_to_score[batch_start:batch_start + scan_batch_size]
                batch_history_by_symbol: dict[str, list[dict[str, Any]]] = {}
                batch_fundamentals_by_symbol: dict[str, dict[str, Any]] = {}
                if not deep_mode:
                    batch_symbols = [str(row.get("symbol", "")).upper() for row in batch_rows if row.get("symbol")]
                    set_full_scan_status(
                        "running",
                        max(15, 15 + int((batch_start / max(total_rows, 1)) * 70)),
                        f"Prefetching Supabase history for scan batch {batch_start + 1}-{min(batch_start + len(batch_rows), total_rows)}.",
                        scan_meta={
                            "universe_size": UNIVERSE_STORE.count(),
                            "passed_liquidity": total_rows,
                            "scored_so_far": len(all_scored),
                            "skipped_so_far": skipped,
                            "proxy_so_far": proxy_scored,
                            "excluded_non_equity": len(excluded_non_equity_rows),
                            "scan_engine": "fast_eod_batch",
                            "scan_batch_size": scan_batch_size,
                            "db_scan_source": db_scan_source,
                            "db_history": history_meta,
                            "db_fundamentals": fundamentals_meta,
                            "data_mode": "real_nse_only",
                        },
                    )
                    batch_prefetch = prefetch_full_scan_db_batch(batch_symbols, history_days=260, history_min_bars=220)
                    batch_history_by_symbol = dict(batch_prefetch.get("history_by_symbol") or {})
                    batch_fundamentals_by_symbol = dict(batch_prefetch.get("fundamentals_by_symbol") or {})
                    batch_history_meta = dict(batch_prefetch.get("history_meta") or {})
                    batch_fundamentals_meta = dict(batch_prefetch.get("fundamentals_meta") or {})
                    history_meta["batches_loaded"] = int(history_meta.get("batches_loaded") or 0) + 1
                    history_meta["symbols_with_history"] = int(history_meta.get("symbols_with_history") or 0) + int(batch_history_meta.get("symbols_with_history") or 0)
                    history_meta["symbols_with_any_history"] = int(history_meta.get("symbols_with_any_history") or 0) + int(batch_history_meta.get("symbols_with_any_history") or len(batch_history_by_symbol))
                    if batch_history_meta.get("status") == "error":
                        errors = list(history_meta.get("errors") or [])
                        errors.append(batch_history_meta.get("error"))
                        history_meta["errors"] = errors[-10:]
                    fundamentals_meta["batches_loaded"] = int(fundamentals_meta.get("batches_loaded") or 0) + 1
                    fundamentals_meta["symbols_with_fundamentals"] = int(fundamentals_meta.get("symbols_with_fundamentals") or 0) + len(batch_fundamentals_by_symbol)
                    if batch_fundamentals_meta.get("status") == "error":
                        errors = list(fundamentals_meta.get("errors") or [])
                        errors.append(batch_fundamentals_meta.get("error"))
                        fundamentals_meta["errors"] = errors[-10:]

                def score_one(row: dict[str, Any]) -> dict[str, Any] | None:
                    if deep_mode:
                        return score_focus_universe_row(dict(row), market)
                    symbol = str(row.get("symbol", "")).upper()
                    return score_full_scan_row_fast(
                        dict(row),
                        market,
                        batch_history_by_symbol.get(symbol),
                        batch_fundamentals_by_symbol.get(symbol),
                    )

                futures = {executor.submit(score_one, row): row for row in batch_rows}
                for future in concurrent.futures.as_completed(futures):
                    result = None
                    try:
                        result = future.result()
                    except Exception as exc:
                        row = futures.get(future) or {}
                        logger.warning("Full NSE scan failed for %s: %s", row.get("symbol", "unknown"), exc)
                        result = None
                    with status_lock:
                        done_count += 1
                        if result is not None and not result.get("is_seed_demo"):
                            compact = compact_full_scan_result(result)
                            all_scored.append(compact)
                            data_mode = compact.get("data_mode")
                            if data_mode == "supabase_yahoo_history_scored":
                                enriched_history_scored += 1
                            elif data_mode == "insufficient_history":
                                insufficient_history_count += 1
                            elif data_mode == "data_quality_failed":
                                data_quality_failed_count += 1
                        else:
                            skipped += 1
                        if done_count % 5 == 0 or done_count == total_rows:
                            pct = 15 + int((done_count / max(total_rows, 1)) * 75)
                            set_full_scan_status(
                                "running",
                                pct,
                                f"Scored {done_count}/{total_rows}: {len(all_scored)} valid, {skipped} skipped.",
                                scan_meta={
                                    "universe_size": UNIVERSE_STORE.count(),
                                    "passed_liquidity": total_rows,
                                    "scored_so_far": len(all_scored),
                                    "skipped_so_far": skipped,
                                    "proxy_so_far": proxy_scored,
                                    "enriched_history_scored": enriched_history_scored,
                                    "insufficient_history_count": insufficient_history_count,
                                    "data_quality_failed_count": data_quality_failed_count,
                                    "excluded_non_equity": len(excluded_non_equity_rows),
                                    "scan_engine": "deep_provider" if deep_mode else "fast_eod_batch",
                                    "scan_batch_size": scan_batch_size,
                                    "db_scan_source": db_scan_source,
                                    "db_history": history_meta,
                                    "db_fundamentals": fundamentals_meta,
                                    "data_mode": "real_nse_only",
                                },
                            )
                            time.sleep(0.02)
                time.sleep(0.05)

        if not all_scored:
            raise RuntimeError(
                f"Full scan completed the row loop but scored 0/{total_rows}. "
                "The cached NSE bhavcopy rows may be malformed or the scoring engine rejected every row."
            )

        set_full_scan_status("running", 92, "Ranking real full-scan candidates.")
        summaries = [stock_summary(stock) for stock in all_scored]
        enriched_count = enriched_history_scored
        insufficient_history = [stock for stock in summaries if stock.get("data_mode") == "insufficient_history"]
        data_quality_failed = [stock for stock in summaries if stock.get("data_mode") == "data_quality_failed"]
        real_history_modes = {"supabase_yahoo_history_scored"}
        rankable_real = [
            stock
            for stock in summaries
            if stock.get("ranking_eligible") is True
            and bool((stock.get("data_quality_gate") or {}).get("pass"))
            and stock.get("data_mode") in real_history_modes
        ]
        strict_ranked = [
            stock
            for stock in rankable_real
            if int(stock.get("risk_score") or 0) < 25
            and stock.get("conviction") not in {"Avoid", "Hard Avoid"}
            and int(stock.get("weekly_score") or 0) >= 45
        ]
        ranked = strict_ranked
        rank_basis = "strict_qualified"
        if not ranked and rankable_real:
            ranked = [
                stock
                for stock in rankable_real
                if int(stock.get("risk_score") or 0) < 45
                and stock.get("conviction") != "Hard Avoid"
                and int(stock.get("weekly_score") or 0) >= 30
            ]
            rank_basis = "watch_only_relaxed" if ranked else "none"
        ranking_ready = bool(rankable_real)
        if not ranked and not ranking_ready:
            rank_basis = "await_history_enrichment"
        def rank_key(stock: dict[str, Any], score_key: str) -> tuple[int, int]:
            candidate_bonus = 1 if stock.get("candidate") else 0
            return (candidate_bonus, int(stock.get(score_key) or 0))

        top_weekly = sorted(ranked, key=lambda stock: rank_key(stock, "weekly_score"), reverse=True)[:3]
        top_monthly = sorted(ranked, key=lambda stock: rank_key(stock, "monthly_score"), reverse=True)[:3]
        generated_at = datetime.now(timezone.utc).isoformat()
        insufficient_examples = [
            {
                "symbol": stock.get("symbol"),
                "name": stock.get("name"),
                "bars_available": (stock.get("data_quality_gate") or {}).get("bars_available"),
                "reason": (stock.get("data_quality_gate") or {}).get("reason") or stock.get("conviction"),
            }
            for stock in insufficient_history[:12]
        ]
        data_quality_failed_examples = [
            {
                "symbol": stock.get("symbol"),
                "name": stock.get("name"),
                "bars_available": (stock.get("data_quality_gate") or {}).get("bars_available"),
                "reason": (stock.get("data_quality_gate") or {}).get("reason") or stock.get("conviction"),
            }
            for stock in data_quality_failed[:12]
        ]
        scan_meta = {
            "universe_size": UNIVERSE_STORE.count(),
            "passed_liquidity": total_rows,
            "total_symbols_seen": total_rows,
            "seed_scored": 0,
            "dynamic_scored": len(all_scored),
            "enriched_history_scored": enriched_count,
            "skipped_insufficient_data": skipped,
            "total_scored": len(all_scored),
            "ranked_candidates": len(ranked),
            "strict_candidates": len(strict_ranked),
            "ranking_ready": ranking_ready,
            "real_history_rankable": len(rankable_real),
            "symbols_with_history": int(history_meta.get("symbols_with_any_history") or history_meta.get("symbols_with_history") or 0),
            "ranking_eligible_count": len(rankable_real),
            "insufficient_history_count": len(insufficient_history),
            "data_quality_failed_count": len(data_quality_failed),
            "excluded_non_equity": len(excluded_non_equity_rows),
            "excluded_non_equity_symbols": [str(row.get("symbol", "")).upper() for row in excluded_non_equity_rows[:12]],
            "insufficient_history_examples": insufficient_examples,
            "data_quality_failed_examples": data_quality_failed_examples,
            "rank_basis": rank_basis,
            "scan_limit_used": scan_limit,
            "max_scan_limit": max_limit,
            "requested_scan_limit": requested_scan_limit,
            "workers_used": max_workers,
            "scan_filter_mode": scan_filter_mode,
            "scan_engine": "deep_provider" if deep_mode else "fast_eod_batch",
            "db_scan_source": db_scan_source,
            "db_history": history_meta,
            "db_fundamentals": fundamentals_meta,
            "data_mode": "real_nse_only",
            "latest_eod_date": latest_eod_date,
            "scan_started_at": scan_started_at,
            "scan_finished_at": generated_at,
            "source_summary": {
                "official_eod_table": "daily_ohlcv",
                "long_history_table": "price_history_daily",
                "latest_eod_date": latest_eod_date,
                "history_reader": "Supabase price_history_daily with bhavcopy overlay",
            },
            "filter": "EQ, turnover >= Rs 10 crore, delivery >= 40% when available, volume consistency, no circuit/operator junk.",
            "ranking_warning": None if ranking_ready else "Top picks stay blank until enough symbols have 220+ validated stored daily bars. Proxy/demo bars are not used for real ranking.",
        }
        payload = {
            "status": "success",
            "generated_at": generated_at,
            "dashboard_mode": "full_nse_scan",
            "market_regime": market,
            "top_weekly": top_weekly,
            "top_monthly": top_monthly,
            "top_weekly_candidates": top_weekly,
            "top_monthly_candidates": top_monthly,
            "insufficient_history": insufficient_history,
            "data_quality_failed": data_quality_failed,
            "latest_critical_events": critical_events(all_scored),
            "sector_map": sector_map(all_scored),
            "stocks": sorted(
                summaries,
                key=lambda stock: (
                    1 if stock.get("ranking_eligible") else 0,
                    1 if stock.get("candidate") else 0,
                    int(stock.get("weekly_score") or 0),
                ),
                reverse=True,
            ),
            "scan_meta": scan_meta,
            "nse_universe": UNIVERSE_STORE.meta(),
            "prices_as_of": PRICE_REFRESH_STATUS.get("watch_updated_at") or PRICE_REFRESH_STATUS.get("updated_at") or generated_at,
            "disclaimer": "Research workflow only. Full scan uses free/EOD data and is not investment advice.",
        }
        if DATABASE_ENABLED and AsyncSessionLocal is not None and ScanRun is not None:
            try:
                scan_run_id = _run_async_sync(persist_full_scan_payload_to_db(payload), timeout_seconds=180)
                if scan_run_id:
                    scan_meta["scan_run_id"] = scan_run_id
                    payload["scan_meta"] = scan_meta
            except Exception as exc:
                scan_meta["persistence_error"] = str(exc)
        FULL_NSE_SCAN_CACHE.update({"generated_at": generated_at, "payload": payload})
        FOCUS_DASHBOARD_CACHE.update({"generated_at": generated_at, "payload": dashboard_payload_from_full_scan(payload)})
        set_full_scan_status(
            "complete",
            100,
            f"Full NSE scan complete. {len(all_scored)} real NSE stocks scored, {len(ranked)} ranked.",
            finished_at=generated_at,
            scan_meta=scan_meta,
        )
        return payload
    except Exception as exc:
        logger.exception("Full NSE scan failed")
        set_full_scan_status("error", 0, f"Full NSE scan failed: {exc}", error=str(exc), finished_at=datetime.now(timezone.utc).isoformat())
        return {
            "status": "error",
            "dashboard_mode": "full_nse_scan",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stocks": [],
            "scan_meta": {"error": str(exc), "data_mode": "real_nse_only"},
        }


def dashboard_payload_from_full_scan(payload: dict[str, Any]) -> dict[str, Any]:
    stocks = payload.get("stocks") or []
    return {
        "generated_at": payload.get("generated_at"),
        "market_health": {
            "regime": payload.get("market_regime", {}).get("regime_status", payload.get("market_regime", {}).get("regime")),
            "breadth_pct": payload.get("market_regime", {}).get("breadth_above_50dma"),
            "position_multiplier": payload.get("market_regime", {}).get("position_multiplier", 1.0),
            "can_buy": payload.get("market_regime", {}).get("can_buy", True),
            "score": payload.get("market_regime", {}).get("score"),
            "raw": payload.get("market_regime", {}),
        },
        "focus": {
            "triggered": [
                stock
                for stock in stocks
                if (stock.get("trade_state") or {}).get("state") == "Triggered"
                or (
                    float(stock.get("price") or 0)
                    >= float((stock.get("entry") or {}).get("breakout_level") or 10**18)
                )
            ][:10],
            "stalking": [
                stock
                for stock in stocks
                if 0
                < (
                    (float((stock.get("entry") or {}).get("breakout_level") or 0) - float(stock.get("price") or 0))
                    / max(float(stock.get("price") or 0), 1)
                    * 100
                )
                <= 3
            ][:15],
            "watchlist": stocks[:25],
            "avoid": [stock for stock in stocks if stock.get("conviction") in {"Avoid", "Hard Avoid"} or int(stock.get("risk_score") or 0) >= 25][:5],
        },
        "sector_map": payload.get("sector_map") or [],
        "latest_critical_events": payload.get("latest_critical_events") or [],
        "scan_meta": payload.get("scan_meta") or {},
        "database": database_status(),
        "live_feed": live_feed.status(),
        "disclaimer": payload.get("disclaimer"),
    }


async def run_yahoo_enrichment_job(limit: int | None, force: bool) -> None:
    def publish(update: dict[str, Any]) -> None:
        status_name = str(update.get("status") or "running")
        progress = int(update.get("progress") or 0)
        message = str(update.get("message") or "Yahoo enrichment running.")
        extra = {key: value for key, value in update.items() if key not in {"status", "progress", "message"}}
        set_yahoo_enrich_status(status_name, progress, message, result=update if status_name == "complete" else None, **extra)

    started_at = datetime.now(timezone.utc).isoformat()
    set_yahoo_enrich_status(
        "running",
        1,
        "Preparing Supabase and latest bhavcopy before Yahoo enrichment.",
        started_at=started_at,
        finished_at=None,
        error=None,
        result=None,
        limit=limit or "all",
        force=force,
    )
    try:
        if not DATABASE_ENABLED:
            raise RuntimeError("DATABASE_URL is not configured; Yahoo enrichment needs Supabase/Postgres storage.")
        if init_db is not None:
            await init_db()
        if ensure_recent_bhavcopy_in_db is not None:
            set_yahoo_enrich_status("running", 2, "Checking latest bhavcopy rows in Supabase.", started_at=started_at)
            try:
                await ensure_recent_bhavcopy_in_db(force=False)
            except Exception as exc:
                # Yahoo enrichment can still run from the companies table if
                # bhavcopy sync is temporarily unavailable. Do not waste a long
                # manual enrichment attempt because the NSE archive blinked.
                set_yahoo_enrich_status(
                    "running",
                    2,
                    f"Bhavcopy pre-check failed, continuing with stored symbols if available: {exc}",
                    started_at=started_at,
                    precheck_warning=str(exc),
                )
        if run_enrichment_pipeline is None:
            raise RuntimeError(f"Yahoo enrichment module is unavailable: {ENRICH_IMPORT_ERROR or 'unknown import error'}")
        result = await run_enrichment_pipeline(limit=limit, force=force, status_callback=publish)
        set_yahoo_enrich_status(
            "complete",
            100,
            result.get("message") or "Yahoo enrichment complete.",
            finished_at=datetime.now(timezone.utc).isoformat(),
            result=result,
            error=None,
        )
    except Exception as exc:
        set_yahoo_enrich_status(
            "error",
            0,
            f"Yahoo enrichment failed: {exc}",
            error=str(exc),
            finished_at=datetime.now(timezone.utc).isoformat(),
        )


def run_full_scan_worker_safe(scan_limit: Any, force: bool) -> None:
    try:
        build_full_nse_scan_payload(scan_limit, force)
    except Exception as exc:  # pragma: no cover - last-resort worker protection
        logger.exception("Full NSE scan worker crashed")
        set_full_scan_status(
            "error",
            0,
            f"Full NSE scan worker crashed: {exc}",
            error=str(exc),
            finished_at=datetime.now(timezone.utc).isoformat(),
        )


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


@app.post("/api/admin/run-full-nse-scan")
def run_full_nse_scan(
    payload: dict[str, Any] | None = Body(default=None),
) -> dict[str, Any]:
    global FULL_NSE_SCAN_THREAD
    payload = payload or {}
    if str(os.getenv("FULL_SCAN_REQUIRE_SECRET", "")).lower() in {"1", "true", "yes"}:
        if payload.get("secret") != os.getenv("ADMIN_SECRET"):
            raise HTTPException(status_code=403, detail="Invalid full-scan secret")
    scan_limit = payload.get("scan_limit") or os.getenv("FULL_NSE_SCAN_LIMIT", "all")
    force = bool(payload.get("force", False))
    thread_alive = bool(FULL_NSE_SCAN_THREAD and FULL_NSE_SCAN_THREAD.is_alive())
    status_name = str(FULL_NSE_SCAN_STATUS.get("status") or "")
    if status_name in {"queued", "running"} and not (force and not thread_alive):
        return {
            "status": "already_running",
            "message": "A full NSE scan is already running. Poll /api/admin/full-nse-scan/status.",
            "poll_url": "/api/admin/full-nse-scan/status",
            "scan_status": FULL_NSE_SCAN_STATUS,
            "worker_alive": thread_alive,
        }
    universe_meta = UNIVERSE_STORE.meta()
    queued_meta = {
        "universe_size": universe_meta.get("total") or UNIVERSE_STORE.count(),
        "passed_liquidity": universe_meta.get("investable") or 0,
        "scored_so_far": 0,
        "skipped_so_far": 0,
        "proxy_so_far": 0,
        "total_scored": 0,
        "ranked_candidates": 0,
        "requested_scan_limit": scan_limit,
        "scan_engine": "fast_eod_batch",
        "data_mode": "real_nse_only",
    }
    set_full_scan_status(
        "queued",
        1,
        f"Full NSE scan queued with limit={scan_limit}. Worker will start shortly.",
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=None,
        error=None,
        scan_meta=queued_meta,
    )
    FULL_NSE_SCAN_THREAD = threading.Thread(
        target=run_full_scan_worker_safe,
        args=(scan_limit, force),
        name="full-nse-scan-worker",
        daemon=True,
    )
    FULL_NSE_SCAN_THREAD.start()
    return {
        "status": "started",
        "message": f"Full NSE scan started with limit={scan_limit}. Poll /api/admin/full-nse-scan/status.",
        "poll_url": "/api/admin/full-nse-scan/status",
        "result_source": "full_scan_cache",
        "scan_limit": scan_limit,
        "scan_meta": queued_meta,
    }


@app.get("/api/admin/full-nse-scan/status")
def full_nse_scan_status() -> dict[str, Any]:
    payload = FULL_NSE_SCAN_CACHE.get("payload") or restore_full_scan_cache_from_db()
    status = dict(FULL_NSE_SCAN_STATUS)
    worker_alive = bool(FULL_NSE_SCAN_THREAD and FULL_NSE_SCAN_THREAD.is_alive())
    if status.get("status") == "idle":
        persisted = persisted_full_scan_status()
        if persisted and persisted.get("status") in {"queued", "running", "error", "complete"}:
            status = persisted
        elif payload:
            status.update(
                {
                    "status": "complete",
                    "progress": 100,
                    "message": "Loaded latest completed full NSE scan from database cache.",
                    "started_at": (payload.get("scan_meta") or {}).get("scan_started_at"),
                    "finished_at": (payload.get("scan_meta") or {}).get("scan_finished_at") or payload.get("generated_at"),
                }
            )
    status = full_scan_worker_stopped_status(status, worker_alive)
    if payload and (not status.get("scan_meta") or status.get("status") == "complete"):
        status["scan_meta"] = payload.get("scan_meta", status.get("scan_meta") or {})
    return {
        **status,
        "has_cached_result": bool(payload),
        "cached_generated_at": FULL_NSE_SCAN_CACHE.get("generated_at"),
        "worker_alive": worker_alive,
    }


@app.get("/api/admin/full-nse-scan/result")
def full_nse_scan_result() -> dict[str, Any]:
    payload = FULL_NSE_SCAN_CACHE.get("payload") or restore_full_scan_cache_from_db()
    if not payload:
        raise HTTPException(status_code=404, detail="No full NSE scan result cached yet")
    return payload


@app.post("/api/admin/enrich-yahoo")
async def trigger_yahoo_enrichment(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    global YAHOO_ENRICH_TASK
    payload = payload or {}
    require_admin_secret(payload.get("secret"))
    if YAHOO_ENRICH_TASK and not YAHOO_ENRICH_TASK.done():
        return {
            "status": "already_running",
            "message": "Yahoo enrichment is already running. Poll /api/admin/enrich-yahoo/status.",
            "poll_url": "/api/admin/enrich-yahoo/status",
            "enrichment_status": yahoo_enrich_status_payload(),
        }
    limit_raw = payload.get("limit", None)
    limit: int | None
    if limit_raw in (None, "", "all"):
        limit = None
    else:
        try:
            limit = max(1, int(limit_raw))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="limit must be a positive integer or 'all'")
    force = bool(payload.get("force", False))
    set_yahoo_enrich_status(
        "queued",
        0,
        "Yahoo enrichment queued.",
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=None,
        error=None,
        result=None,
        limit=limit or "all",
        force=force,
    )
    YAHOO_ENRICH_TASK = asyncio.create_task(run_yahoo_enrichment_job(limit=limit, force=force))
    return {
        "status": "started",
        "message": "Yahoo enrichment started in the background.",
        "poll_url": "/api/admin/enrich-yahoo/status",
        "limit": limit or "all",
        "force": force,
    }


@app.get("/api/admin/enrich-yahoo/status")
def yahoo_enrichment_status() -> dict[str, Any]:
    return yahoo_enrich_status_payload()


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
        "filter": "series EQ, turnover >= Rs 10 crore, delivery >= 40% when available, volume >= 80% of 50-day average when available, market cap >= Rs 500 crore when market cap is available",
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
    if DATABASE_ENABLED and AsyncSessionLocal is not None and text is not None:
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("SELECT 1"))
            payload["connection_test"] = "ok"
        except Exception as exc:  # pragma: no cover - db dependent
            payload["connection_test"] = "failed"
            payload["connection_error"] = str(exc)
    else:
        payload["connection_test"] = "disabled"
    if DATABASE_ENABLED and database_counts is not None:
        try:
            payload["counts"] = await database_counts()
        except Exception as exc:  # pragma: no cover - db dependent
            payload["count_error"] = str(exc)
    payload["eod_task"] = EOD_TASK_STATUS
    return payload


@app.get("/api/database/bhavcopy-dates")
async def database_bhavcopy_dates() -> dict[str, Any]:
    status = await get_database_status()
    counts = status.get("counts") or {}
    return {
        "enabled": status.get("enabled"),
        "connection_test": status.get("connection_test"),
        "dates": counts.get("bhavcopy_dates") or [],
        "latest": counts.get("latest_bhavcopy_date"),
        "latest_age_days": counts.get("latest_bhavcopy_age_days"),
        "retention_dates": counts.get("bhavcopy_retention_dates"),
        "stale": counts.get("bhavcopy_stale"),
        "raw_rows": counts.get("daily_ohlcv"),
        "enriched_symbols": counts.get("enriched_symbols"),
        "enriched_rows": counts.get("enriched_ohlcv"),
        "error": status.get("connection_error") or status.get("count_error") or status.get("error"),
    }


@app.post("/api/database/sync-bhavcopy")
async def sync_database_bhavcopy(payload: dict[str, Any] | None = Body(default=None)) -> dict[str, Any]:
    payload = payload or {}
    if not DATABASE_ENABLED or ensure_recent_bhavcopy_in_db is None:
        raise HTTPException(status_code=400, detail=f"DATABASE_URL is not configured or database loader is unavailable. Status: {database_status()}")
    if str(os.getenv("BHAVCOPY_SYNC_REQUIRE_SECRET", "false")).lower() in {"1", "true", "yes"}:
        if payload.get("secret") != os.getenv("ADMIN_SECRET"):
            raise HTTPException(status_code=403, detail="Invalid bhavcopy sync secret")
    force = bool(payload.get("force", True))
    EOD_TASK_STATUS.update({"status": "running", "updated_at": datetime.now(timezone.utc).isoformat(), "result": None, "error": None})
    try:
        if init_db is not None:
            await init_db()
    except Exception as exc:
        status = database_status()
        message = str(exc)
        if "[Errno -2]" in message or "Name or service not known" in message:
            detail = (
                f"Supabase/Postgres hostname could not be resolved before bhavcopy download. "
                f"Parsed host={status.get('host')!r}, port={status.get('port')!r}. "
                "Fix Render DATABASE_URL: use Supabase Transaction pooler URI, paste only the postgresql:// value, "
                "and URL-encode/reset any password containing #, /, ?, %, spaces, or :. "
                f"Original error: {message}"
            )
        else:
            detail = f"Supabase/Postgres connection failed before bhavcopy download. Status={status}. Original error: {message}"
        EOD_TASK_STATUS.update({"status": "error", "updated_at": datetime.now(timezone.utc).isoformat(), "result": None, "error": detail})
        raise HTTPException(status_code=503, detail=detail) from exc
    try:
        try:
            result = await ensure_recent_bhavcopy_in_db(force=force)
        except Exception as loop_exc:
            if "Event loop is closed" not in str(loop_exc) or dispose_database_engine is None:
                raise
            await dispose_database_engine()
            result = await ensure_recent_bhavcopy_in_db(force=force)
        EOD_TASK_STATUS.update({"status": result.get("status"), "updated_at": datetime.now(timezone.utc).isoformat(), "result": result, "error": None})
        return {"status": "ok", "result": result, "database": await get_database_status()}
    except Exception as exc:
        status = database_status()
        message = str(exc)
        if "[Errno -2]" in message or "Name or service not known" in message:
            detail = (
                f"Bhavcopy sync failed due to a hostname/DNS error. If parsed host={status.get('host')!r} "
                f"is your Supabase host, fix Render DATABASE_URL. If the database is connected, the NSE archive "
                f"host may be temporarily unreachable from Render. Original error: {message}"
            )
            code = 503
        else:
            detail = f"Bhavcopy sync failed: {message}"
            code = 502
        EOD_TASK_STATUS.update({"status": "error", "updated_at": datetime.now(timezone.utc).isoformat(), "result": None, "error": detail})
        raise HTTPException(status_code=code, detail=detail) from exc


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


@app.get("/api/live/debug")
def live_debug() -> dict[str, Any]:
    if live_feed.configured:
        live_feed.start()
    return live_debug_payload()


@app.post("/api/live/subscribe")
def live_subscribe(payload: dict[str, Any]) -> dict[str, Any]:
    symbols = payload.get("symbols") or WATCH_SYMBOLS
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
def chart_detail(symbol: str, range: str = "3m", fresh: int = 0) -> dict[str, Any]:
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
    symbols = [item.strip().upper() for item in raw_symbols.split(",") if item.strip()] or WATCH_SYMBOLS
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
    eod = _run_async_sync(run_eod_update()) if DATABASE_ENABLED and run_eod_update is not None else None
    market = refresh_market_data()
    alerts = scan_trade_alerts(refresh_market=False)
    master = master_universe_payload(force=True)
    focus = build_focus_dashboard_payload(force=True, scan_limit=int(os.getenv("DAILY_FOCUS_SCAN_LIMIT", "60")))
    full_scan = None
    if str(os.getenv("ENABLE_DAILY_FULL_SCAN", "")).lower() in {"1", "true", "yes"}:
        full_scan = build_full_nse_scan_payload(scan_limit=os.getenv("FULL_NSE_SCAN_LIMIT", "all"), force=True)
    return {
        "universe_refresh": universe,
        "eod_bhavcopy_sync": eod,
        "market_refresh": market,
        "alert_scan": alerts,
        "overnight_audit": {"generated_at": master["generated_at"], "symbols": len(master.get("data", {}))},
        "focus_dashboard": {"generated_at": focus["generated_at"], "symbols": focus.get("scan_meta", {}).get("total_scored")},
        "full_nse_scan": None
        if full_scan is None
        else {"generated_at": full_scan["generated_at"], "symbols": full_scan.get("scan_meta", {}).get("total_scored")},
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
def stock_detail(symbol: str, fresh: bool = False, debug: bool = False) -> dict[str, Any]:
    symbol = symbol.upper()
    market_open = is_market_hours()
    started_total = time.perf_counter()
    stages: list[dict[str, Any]] = []

    if not fresh:
        started = time.perf_counter()
        cached, source = cached_stock_sources(symbol)
        append_debug_stage(stages, "cache_lookup", started, source=source or "miss", cache_hit=bool(cached))
        if cached:
            cached["trade_state"] = advance_trade_state(cached)
            cached["score_history"] = SCORE_HISTORY.get(symbol, [])
            cached["market_is_open"] = market_open
            if debug:
                cached["debug_timing"] = {
                    "cache_hit": True,
                    "source": source,
                    "total_ms": round((time.perf_counter() - started_total) * 1000, 1),
                    "stages": stages,
                }
            return cached
    else:
        stages.append({"stage": "cache_lookup", "ms": 0.0, "source": "bypassed", "cache_hit": False})

    try:
        detail = dynamic_universe_stock_detail(
            symbol,
            provider_timeout_seconds=INDIVIDUAL_STOCK_PROVIDER_TIMEOUT_SECONDS,
            allow_proxy=False,
            debug_stages=stages,
        )
        source = "dynamic_fetch"
    except HTTPException as exc:
        if exc.status_code == 404:
            raise
        row = UNIVERSE_STORE.get(symbol) or seed_row_for_symbol(symbol)
        if not row:
            raise
        reason = str(exc.detail or f"{symbol} detail fetch did not complete.")
        detail = wait_data_stock_detail(symbol, row, [reason], reason, bars_available=0)
        source = "dynamic_partial"
    except Exception as exc:
        row = UNIVERSE_STORE.get(symbol) or seed_row_for_symbol(symbol)
        if not row:
            raise HTTPException(status_code=502, detail=f"{symbol} detail fetch failed: {exc}") from exc
        reason = f"Provider response was slow or incomplete: {exc}"
        detail = wait_data_stock_detail(symbol, row, [reason], reason, bars_available=0)
        source = "dynamic_partial"

    detail["score_history"] = SCORE_HISTORY.get(symbol, [])
    detail["market_is_open"] = market_open
    if "business_quality" in detail:
        store_individual_stock_cache(symbol, detail, market_open)
    if debug:
        detail["debug_timing"] = {
            "cache_hit": False,
            "source": source,
            "total_ms": round((time.perf_counter() - started_total) * 1000, 1),
            "stages": stages,
        }
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
    merge_database_fundamentals(symbol, company, connector_notes)
    row = UNIVERSE_STORE.get(symbol) or seed_row_for_symbol(symbol) or {"symbol": symbol, "name": company.get("name", symbol)}
    bars, corporate_actions_applied = load_database_research_bars(symbol, row, connector_notes, min_bars=60)
    if not bars:
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
    if any("Supabase Yahoo-enriched history loaded" in note for note in connector_notes):
        scored["data_mode"] = "supabase_yahoo_history_detail_scored"
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
            "DATABASE_URL enables Supabase/Postgres storage for bhavcopy, Yahoo-enriched history, fundamentals, portfolio, and trade state",
            "DB_USE_NULL_POOL=true avoids asyncpg event-loop reuse errors on Render worker threads",
            "YAHOO_ENRICH_HISTORY_TIMEOUT and YAHOO_ENRICH_FUNDAMENTAL_TIMEOUT cap each Yahoo symbol fetch",
            "YAHOO_ENRICH_SKIP_PATTERNS skips ETF/index-like NSE symbols that Yahoo frequently rejects",
            "REDIS_URL optional live tick state/pubsub store",
            "SHOONYA_IN_APP_FEED=false when running feed_worker.py as a separate worker",
            "ENABLE_WEEKLY_SCAN=true to run the scheduled liquid-universe scan",
        ],
    }
