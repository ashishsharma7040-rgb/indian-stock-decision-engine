from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any

from data_sources import fetch_yahoo_chart, fetch_yahoo_fundamentals
from database import AsyncSessionLocal, DATABASE_ENABLED, db_retry, init_db
from models import Company, DailyOHLCV, Fundamental

try:
    from sqlalchemy import func, select
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except Exception:  # pragma: no cover - dependencies are installed on Render
    func = None  # type: ignore
    select = None  # type: ignore
    pg_insert = None  # type: ignore


StatusCallback = Callable[[dict[str, Any]], None]

DEFAULT_SKIP_PATTERNS = (
    "BEES",
    "ETF",
    "GOLDBEES",
    "SILVERBEES",
    "LIQUIDBEES",
    "JUNIORBEES",
    "NIFTYBEES",
    "BANKBEES",
    "PSUBNKBEES",
)


def _skip_patterns() -> tuple[str, ...]:
    raw = os.getenv("YAHOO_ENRICH_SKIP_PATTERNS", "")
    if not raw.strip():
        return DEFAULT_SKIP_PATTERNS
    return tuple(item.strip().upper() for item in raw.split(",") if item.strip())


def should_skip_symbol(symbol: str) -> bool:
    upper = str(symbol or "").upper()
    return any(pattern in upper for pattern in _skip_patterns())


def _clean_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _bar_date(row: dict[str, Any]) -> date | None:
    raw = str(row.get("datetime") or row.get("date") or "")[:10]
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


@db_retry(max_retries=3, delay=1.0)
async def _load_enrichment_symbols(limit: int | None = None) -> list[str]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None or select is None or func is None:
        return []
    async with AsyncSessionLocal() as session:
        latest_bhavcopy_date = await session.scalar(
            select(func.max(DailyOHLCV.date)).where(DailyOHLCV.is_adjusted.is_(False))
        )
        if latest_bhavcopy_date is not None:
            result = await session.execute(
                select(DailyOHLCV.symbol)
                .outerjoin(Company, Company.symbol == DailyOHLCV.symbol)
                .where(
                    DailyOHLCV.date == latest_bhavcopy_date,
                    DailyOHLCV.is_adjusted.is_(False),
                    (Company.series.is_(None)) | (Company.series == "EQ"),
                )
                .order_by((DailyOHLCV.turnover).desc().nullslast(), (DailyOHLCV.close * DailyOHLCV.volume).desc().nullslast())
                .limit(limit or 10000)
            )
            symbols = [str(item).upper() for item in result.scalars().all() if item]
            if symbols:
                return symbols
        result = await session.execute(
            select(Company.symbol)
            .where((Company.series.is_(None)) | (Company.series == "EQ"))
            .order_by(Company.symbol)
            .limit(limit or 10000)
        )
        return [str(item).upper() for item in result.scalars().all() if item]


@db_retry(max_retries=3, delay=1.0)
async def _existing_adjusted_history_counts(symbols: list[str]) -> dict[str, int]:
    if not symbols or not DATABASE_ENABLED or AsyncSessionLocal is None or select is None or func is None:
        return {}
    counts: dict[str, int] = {}
    chunk_size = max(50, int(os.getenv("YAHOO_ENRICH_DB_CHUNK", "500")))
    async with AsyncSessionLocal() as session:
        for index in range(0, len(symbols), chunk_size):
            batch = symbols[index:index + chunk_size]
            result = await session.execute(
                select(DailyOHLCV.symbol, func.count(DailyOHLCV.id))
                .where(DailyOHLCV.symbol.in_(batch), DailyOHLCV.is_adjusted.is_(True))
                .group_by(DailyOHLCV.symbol)
            )
            for symbol, count in result.all():
                counts[str(symbol).upper()] = int(count or 0)
    return counts


async def _upsert_historical_ohlcv(session: Any, symbol: str, bars: list[dict[str, Any]]) -> int:
    if not bars or pg_insert is None:
        return 0
    values: list[dict[str, Any]] = []
    for row in bars:
        bar_date = _bar_date(row)
        if bar_date is None:
            continue
        close = _clean_float(row.get("close"))
        open_price = _clean_float(row.get("open"))
        high = _clean_float(row.get("high"))
        low = _clean_float(row.get("low"))
        if close is None or open_price is None or high is None or low is None:
            continue
        values.append(
            {
                "symbol": symbol,
                "date": bar_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": int(float(row.get("volume") or 0)),
                "turnover": close * float(row.get("volume") or 0) if row.get("volume") else None,
                "data_source": "Yahoo Finance 2y daily enrichment",
                "is_adjusted": True,
            }
        )
    if not values:
        return 0
    stmt = pg_insert(DailyOHLCV).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["symbol", "date"],
        set_={
            "open": stmt.excluded.open,
            "high": stmt.excluded.high,
            "low": stmt.excluded.low,
            "close": stmt.excluded.close,
            "volume": stmt.excluded.volume,
            "turnover": stmt.excluded.turnover,
            "data_source": stmt.excluded.data_source,
            "is_adjusted": stmt.excluded.is_adjusted,
        },
    )
    result = await session.execute(stmt)
    return int(getattr(result, "rowcount", 0) or len(values))


async def _upsert_fundamentals(session: Any, symbol: str, fundamentals: dict[str, Any]) -> bool:
    if not fundamentals or pg_insert is None:
        return False
    as_of = datetime.now(timezone.utc).date()
    company = await session.get(Company, symbol)
    if company is None:
        company = Company(symbol=symbol, name=symbol, series="EQ", sector="Unclassified", industry="NSE Equity", source="Yahoo enrichment")
        session.add(company)
    if fundamentals.get("market_cap_cr") is not None:
        company.market_cap_cr = _clean_float(fundamentals.get("market_cap_cr"))

    payload = {
        "symbol": symbol,
        "as_of": as_of,
        "effective_date": as_of,
        "knowledge_date": as_of,
        "roe": _clean_float(fundamentals.get("roe")),
        "debt_equity": _clean_float(fundamentals.get("debt_equity")),
        "pe": _clean_float(fundamentals.get("pe")),
        "forward_pe": _clean_float(fundamentals.get("forward_pe")),
        "pb": _clean_float(fundamentals.get("pb")),
        "ebitda": _clean_float(fundamentals.get("ebitda")),
        "operating_cash_flow": _clean_float(fundamentals.get("operating_cash_flow")),
        "fcf_trend": fundamentals.get("fcf_trend"),
    }
    payload = {key: value for key, value in payload.items() if value is not None}
    if len(payload) <= 4:
        return False
    stmt = pg_insert(Fundamental).values(payload)
    update_values = {key: getattr(stmt.excluded, key) for key in payload if key not in {"symbol", "as_of"}}
    stmt = stmt.on_conflict_do_update(index_elements=["symbol", "as_of"], set_=update_values)
    await session.execute(stmt)
    return True


async def _fetch_history(symbol: str) -> list[dict[str, Any]]:
    timeout = max(3.0, float(os.getenv("YAHOO_ENRICH_HISTORY_TIMEOUT", "15")))
    return await asyncio.wait_for(
        asyncio.to_thread(fetch_yahoo_chart, symbol, "2y", "1d"),
        timeout=timeout,
    )


async def _fetch_fundamentals(symbol: str) -> dict[str, Any]:
    timeout = max(3.0, float(os.getenv("YAHOO_ENRICH_FUNDAMENTAL_TIMEOUT", "10")))
    return await asyncio.wait_for(
        asyncio.to_thread(fetch_yahoo_fundamentals, symbol),
        timeout=timeout,
    )


@db_retry(max_retries=3, delay=1.5)
async def _write_symbol_payload(symbol: str, bars: list[dict[str, Any]], fundamentals: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"symbol": symbol, "history_rows": 0, "fundamentals": False}
    async with AsyncSessionLocal() as session:
        with session.no_autoflush:
            result["history_rows"] = await _upsert_historical_ohlcv(session, symbol, bars)
            result["fundamentals"] = await _upsert_fundamentals(session, symbol, fundamentals)
        await session.commit()
    return result


async def enrich_symbol(symbol: str) -> dict[str, Any]:
    symbol = str(symbol or "").upper()
    if should_skip_symbol(symbol):
        return {"symbol": symbol, "history_rows": 0, "fundamentals": False, "skipped": True, "skip_reason": "unsupported_etf_or_index_like_symbol"}
    result: dict[str, Any] = {"symbol": symbol, "history_rows": 0, "fundamentals": False}
    try:
        bars = await _fetch_history(symbol)
    except asyncio.TimeoutError:
        return {**result, "warning": f"Yahoo history timed out after {os.getenv('YAHOO_ENRICH_HISTORY_TIMEOUT', '15')}s"}
    fundamentals: dict[str, Any] = {}
    try:
        fundamentals = await _fetch_fundamentals(symbol)
    except asyncio.TimeoutError:
        result["fundamentals_warning"] = f"Yahoo fundamentals timed out after {os.getenv('YAHOO_ENRICH_FUNDAMENTAL_TIMEOUT', '10')}s"
    except Exception as exc:
        result["fundamentals_warning"] = str(exc)
    result.update(await _write_symbol_payload(symbol, bars, fundamentals))
    if not result["history_rows"]:
        result["warning"] = "Yahoo returned no usable historical bars"
    return result


async def run_enrichment_pipeline(
    limit: int | None = None,
    force: bool = False,
    status_callback: StatusCallback | None = None,
) -> dict[str, Any]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled", "error": "DATABASE_URL is not configured"}
    await init_db()
    requested_symbols = await _load_enrichment_symbols(limit=limit)
    unsupported_symbols = [symbol for symbol in requested_symbols if should_skip_symbol(symbol)]
    enrichable_symbols = [symbol for symbol in requested_symbols if not should_skip_symbol(symbol)]
    min_existing_bars = max(60, int(os.getenv("YAHOO_ENRICH_MIN_BARS", "220")))
    counts = await _existing_adjusted_history_counts(enrichable_symbols)
    symbols = enrichable_symbols if force else [symbol for symbol in enrichable_symbols if counts.get(symbol, 0) < min_existing_bars]
    total = len(symbols)
    workers = max(1, min(int(os.getenv("YAHOO_ENRICH_WORKERS", "2")), 5))
    request_delay = max(0.0, float(os.getenv("YAHOO_ENRICH_REQUEST_DELAY", "0.6")))
    status: dict[str, Any] = {
        "enabled": True,
        "status": "running",
        "progress": 3,
        "message": f"Preparing Yahoo enrichment for {total} symbols.",
        "total": total,
        "requested_symbols": len(requested_symbols),
        "skipped_existing": len(enrichable_symbols) - total,
        "skipped_unsupported": len(unsupported_symbols),
        "processed": 0,
        "enriched": 0,
        "history_rows": 0,
        "fundamentals": 0,
        "errors": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "workers": workers,
        "force": force,
    }

    def publish(extra: dict[str, Any] | None = None) -> None:
        if extra:
            status.update(extra)
        if status_callback:
            status_callback(dict(status))

    publish()
    if total == 0:
        status.update(
            {
                "status": "complete",
                "progress": 100,
                "message": (
                    "All selected equity symbols already have sufficient Yahoo history."
                    if enrichable_symbols else
                    "No enrichable equity symbols were found after filtering ETF/index-like instruments."
                ),
            }
        )
        publish()
        return status

    queue: asyncio.Queue[str] = asyncio.Queue()
    for symbol in symbols:
        queue.put_nowait(symbol)
    lock = asyncio.Lock()

    async def worker(worker_id: int) -> None:
        nonlocal status
        while True:
            try:
                symbol = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                if request_delay:
                    await asyncio.sleep(request_delay * max(worker_id, 1))
                row = await enrich_symbol(symbol)
                async with lock:
                    status["processed"] += 1
                    if row.get("skipped"):
                        status["skipped_unsupported"] = int(status.get("skipped_unsupported") or 0) + 1
                    elif int(row.get("history_rows") or 0) > 0:
                        status["enriched"] += 1
                    if row.get("fundamentals"):
                        status["fundamentals"] += 1
                    status["history_rows"] += int(row.get("history_rows") or 0)
                    status["current_symbol"] = symbol
                    status["progress"] = min(99, 5 + int((status["processed"] / max(total, 1)) * 92))
                    status["message"] = f"Yahoo enriched {status['processed']}/{total}: {symbol}"
                    if row.get("warning"):
                        errors = list(status.get("errors") or [])
                        errors.append({"symbol": symbol, "warning": row.get("warning")})
                        status["errors"] = errors[-25:]
                    publish()
            except Exception as exc:
                async with lock:
                    status["processed"] += 1
                    errors = list(status.get("errors") or [])
                    errors.append({"symbol": symbol, "error": str(exc)})
                    status["errors"] = errors[-25:]
                    status["current_symbol"] = symbol
                    status["progress"] = min(99, 5 + int((status["processed"] / max(total, 1)) * 92))
                    status["message"] = f"Yahoo enrichment skipped {symbol}: {exc}"
                    publish()
            finally:
                queue.task_done()

    await asyncio.gather(*(worker(worker_id + 1) for worker_id in range(workers)))
    status.update(
        {
            "status": "complete",
            "progress": 100,
            "message": f"Yahoo enrichment complete. {status['enriched']}/{total} symbols received usable history.",
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    publish()
    return status
