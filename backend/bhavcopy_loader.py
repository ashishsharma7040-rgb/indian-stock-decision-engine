from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from data_sources import fetch_latest_nse_bhavcopy, fetch_nse_bhavcopy_for_date, fetch_nse_equity_master
from database import AsyncSessionLocal, DATABASE_ENABLED, init_db
from models import Company, DailyOHLCV, Fundamental

try:
    from sqlalchemy import delete, func, select
except Exception:  # pragma: no cover - dependencies are installed on Render
    delete = None  # type: ignore
    func = None  # type: ignore
    select = None  # type: ignore


BHAVCOPY_RETENTION_DATES = max(1, int(os.getenv("BHAVCOPY_RETENTION_DATES", "2")))
BHAVCOPY_MAX_AGE_DAYS = max(1, int(os.getenv("BHAVCOPY_MAX_AGE_DAYS", "2")))


def latest_trading_day(today: date | None = None) -> date:
    day = today or datetime.now(timezone.utc).astimezone().date()
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


async def upsert_equity_master() -> int:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return 0
    rows = await asyncio.to_thread(fetch_nse_equity_master)
    async with AsyncSessionLocal() as session:
        count = 0
        for row in rows:
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            company = await session.get(Company, symbol)
            if company is None:
                company = Company(symbol=symbol, name=row.get("name") or symbol)
                session.add(company)
            company.name = row.get("name") or company.name or symbol
            company.series = row.get("series") or company.series or "EQ"
            company.isin = row.get("isin") or company.isin
            company.sector = company.sector or "Unclassified"
            company.industry = company.industry or "NSE Equity"
            company.source = "NSE equity master"
            count += 1
        await session.commit()
        return count


async def prune_bhavcopy_retention(session: Any, keep_dates: int | None = None) -> dict[str, Any]:
    """Keep only the most recent N bhavcopy dates in Supabase.

    This prevents Supabase from becoming sluggish because each bhavcopy contains
    thousands of rows. The default keeps the latest 2 trading dates only.
    """
    if select is None or delete is None:
        return {"enabled": False, "deleted_rows": 0, "kept_dates": []}
    keep_count = max(1, int(keep_dates or BHAVCOPY_RETENTION_DATES))
    result = await session.execute(
        select(DailyOHLCV.date)
        .where(DailyOHLCV.is_adjusted.is_(False))
        .distinct()
        .order_by(DailyOHLCV.date.desc())
    )
    dates = list(result.scalars().all())
    kept = dates[:keep_count]
    if not kept:
        return {"enabled": True, "deleted_rows": 0, "kept_dates": []}
    delete_result = await session.execute(
        delete(DailyOHLCV).where(
            DailyOHLCV.date.not_in(kept),
            DailyOHLCV.is_adjusted.is_(False),
        )
    )
    return {
        "enabled": True,
        "deleted_rows": int(getattr(delete_result, "rowcount", 0) or 0),
        "kept_dates": [item.isoformat() for item in kept],
        "retention_dates": keep_count,
    }


async def update_database_with_bhavcopy(for_date: date | None = None) -> dict[str, Any]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled", "reason": "DATABASE_URL is not configured"}
    await init_db()
    payload = await asyncio.to_thread(fetch_nse_bhavcopy_for_date, for_date) if for_date else await asyncio.to_thread(fetch_latest_nse_bhavcopy)
    as_of = date.fromisoformat(payload["as_of"])
    rows = payload.get("rows", [])
    async with AsyncSessionLocal() as session:
        await session.execute(delete(DailyOHLCV).where(DailyOHLCV.date == as_of))
        symbols = sorted({str(row.get("symbol", "")).upper() for row in rows if row.get("symbol")})
        existing_symbols: set[str] = set()
        if symbols:
            existing_result = await session.execute(select(Company.symbol).where(Company.symbol.in_(symbols)))
            existing_symbols = {str(symbol).upper() for symbol in existing_result.scalars().all()}

        inserted = 0
        seen_new_companies: set[str] = set()
        batch_size = max(100, int(os.getenv("BHAVCOPY_INSERT_BATCH_SIZE", "750")))
        with session.no_autoflush:
            for row in rows:
                symbol = str(row.get("symbol", "")).upper()
                if not symbol:
                    continue
                if symbol not in existing_symbols and symbol not in seen_new_companies:
                    session.add(
                        Company(
                            symbol=symbol,
                            name=symbol,
                            sector="Unclassified",
                            industry="NSE Equity",
                            series=row.get("series") or "EQ",
                            isin=row.get("isin") or None,
                            source="NSE bhavcopy",
                        )
                    )
                    seen_new_companies.add(symbol)
                session.add(
                    DailyOHLCV(
                        symbol=symbol,
                        date=as_of,
                        open=row.get("open"),
                        high=row.get("high"),
                        low=row.get("low"),
                        close=row.get("close"),
                        volume=row.get("volume"),
                        turnover=row.get("turnover"),
                        data_source=row.get("data_source") or payload.get("source") or "NSE bhavcopy",
                        is_adjusted=False,
                    )
                )
                inserted += 1
                if inserted % batch_size == 0:
                    await session.flush()
        await session.flush()
        retention = await prune_bhavcopy_retention(session)
        await session.commit()
    return {
        "enabled": True,
        "status": "ok",
        "as_of": as_of.isoformat(),
        "rows": inserted,
        "source": payload.get("source"),
        "retention": retention,
    }


async def run_eod_update() -> dict[str, Any]:
    if not DATABASE_ENABLED:
        return {"enabled": False, "status": "disabled", "reason": "DATABASE_URL is not configured"}
    await init_db()
    master_count = 0
    master_error = None
    try:
        master_count = await upsert_equity_master()
    except Exception as exc:  # pragma: no cover - public NSE endpoint dependent
        # Equity master improves company names/ISINs, but it must never block
        # the official bhavcopy from being stored for scans.
        master_error = str(exc)
    # Use the latest available NSE bhavcopy with lookback. This handles exchange
    # holidays better than only checking the last weekday.
    bhavcopy = await update_database_with_bhavcopy(None)
    return {
        "enabled": True,
        "status": "ok",
        "equity_master_count": master_count,
        "equity_master_error": master_error,
        "bhavcopy": bhavcopy,
    }


async def database_counts() -> dict[str, Any]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled"}
    async with AsyncSessionLocal() as session:
        companies = await session.scalar(select(func.count()).select_from(Company))
        bars = await session.scalar(select(func.count()).select_from(DailyOHLCV))
        enriched_bars = await session.scalar(
            select(func.count()).select_from(DailyOHLCV).where(DailyOHLCV.is_adjusted.is_(True))
        )
        enriched_symbols = await session.scalar(
            select(func.count(func.distinct(DailyOHLCV.symbol))).where(DailyOHLCV.is_adjusted.is_(True))
        )
        latest_dates_result = await session.execute(
            select(DailyOHLCV.date)
            .where(DailyOHLCV.is_adjusted.is_(False))
            .distinct()
            .order_by(DailyOHLCV.date.desc())
            .limit(BHAVCOPY_RETENTION_DATES)
        )
        latest_dates = list(latest_dates_result.scalars().all())
    latest_date = latest_dates[0] if latest_dates else None
    age_days = (datetime.now(timezone.utc).date() - latest_date).days if latest_date else None
    return {
        "enabled": True,
        "companies": companies or 0,
        "daily_ohlcv": bars or 0,
        "enriched_ohlcv": enriched_bars or 0,
        "enriched_symbols": enriched_symbols or 0,
        "bhavcopy_dates": [item.isoformat() for item in latest_dates],
        "latest_bhavcopy_date": latest_date.isoformat() if latest_date else None,
        "latest_bhavcopy_age_days": age_days,
        "bhavcopy_retention_dates": BHAVCOPY_RETENTION_DATES,
        "bhavcopy_stale": age_days is None or age_days > BHAVCOPY_MAX_AGE_DAYS,
    }


async def latest_bhavcopy_rows_from_db(limit: int = 3000, max_age_days: int | None = None) -> dict[str, Any]:
    """Read the latest retained bhavcopy from Supabase for full-scan calculations."""
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled", "rows": [], "reason": "DATABASE_URL is not configured"}
    max_age = BHAVCOPY_MAX_AGE_DAYS if max_age_days is None else max(0, int(max_age_days))
    async with AsyncSessionLocal() as session:
        latest_date = await session.scalar(
            select(func.max(DailyOHLCV.date)).where(DailyOHLCV.is_adjusted.is_(False))
        )
        if latest_date is None:
            return {"enabled": True, "status": "empty", "rows": [], "reason": "No bhavcopy rows in daily_ohlcv"}
        age_days = (datetime.now(timezone.utc).date() - latest_date).days
        if age_days > max_age:
            return {
                "enabled": True,
                "status": "stale",
                "rows": [],
                "latest_bhavcopy_date": latest_date.isoformat(),
                "age_days": age_days,
                "max_age_days": max_age,
            }
        result = await session.execute(
            select(DailyOHLCV, Company)
            .outerjoin(Company, Company.symbol == DailyOHLCV.symbol)
            .where(DailyOHLCV.date == latest_date, DailyOHLCV.is_adjusted.is_(False))
        )
        pairs = list(result.all())
        pairs.sort(
            key=lambda item: float(item[0].turnover or 0) or float(item[0].close or 0) * float(item[0].volume or 0),
            reverse=True,
        )
        pairs = pairs[: max(1, int(limit or 3000))]
    rows: list[dict[str, Any]] = []
    for bar, company in pairs:
        rows.append(
            {
                "symbol": bar.symbol,
                "name": company.name if company else bar.symbol,
                "sector": company.sector if company else "Unclassified",
                "industry": company.industry if company else "NSE Equity",
                "series": company.series if company else "EQ",
                "isin": company.isin if company else None,
                "price": bar.close,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "turnover": bar.turnover,
                "as_of": latest_date.isoformat(),
                "source": "Supabase daily_ohlcv latest bhavcopy",
                "data_mode": "supabase_bhavcopy_eod",
                "investable": True,
            }
        )
    return {
        "enabled": True,
        "status": "ok",
        "rows": rows,
        "latest_bhavcopy_date": latest_date.isoformat(),
        "age_days": age_days,
        "source": "Supabase daily_ohlcv",
    }


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index:index + size] for index in range(0, len(items), size)]


async def historical_bars_from_db(
    symbols: list[str],
    days: int = 260,
    min_bars: int = 120,
    chunk_size: int = 250,
) -> dict[str, Any]:
    """Load enriched historical OHLCV from Supabase for batch scoring.

    Bhavcopy retention intentionally keeps only a couple of raw exchange dates,
    so a useful full scan needs the longer adjusted history written by the
    Yahoo enrichment job. This reader accepts all rows but prefers adjusted
    history when available.
    """
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled", "bars_by_symbol": {}, "reason": "DATABASE_URL is not configured"}
    wanted = sorted({str(symbol).upper() for symbol in symbols if symbol})
    if not wanted:
        return {"enabled": True, "status": "empty", "bars_by_symbol": {}, "symbols_requested": 0}
    start_date = datetime.now(timezone.utc).date() - timedelta(days=max(days * 2, 400))
    bars_by_symbol: dict[str, list[dict[str, Any]]] = {}
    async with AsyncSessionLocal() as session:
        for batch in _chunks(wanted, max(20, int(chunk_size or 250))):
            result = await session.execute(
                select(DailyOHLCV)
                .where(DailyOHLCV.symbol.in_(batch), DailyOHLCV.date >= start_date)
                .order_by(DailyOHLCV.symbol, DailyOHLCV.date)
            )
            for bar in result.scalars().all():
                bars_by_symbol.setdefault(str(bar.symbol).upper(), []).append(
                    {
                        "datetime": bar.date.isoformat(),
                        "open": float(bar.open or 0),
                        "high": float(bar.high or 0),
                        "low": float(bar.low or 0),
                        "close": float(bar.close or 0),
                        "volume": int(bar.volume or 0),
                        "turnover": float(bar.turnover or 0) if bar.turnover is not None else None,
                        "source": bar.data_source or "Supabase daily_ohlcv",
                        "data_source": bar.data_source or "Supabase daily_ohlcv",
                        "is_adjusted": bool(bar.is_adjusted),
                    }
                )
    trimmed: dict[str, list[dict[str, Any]]] = {}
    adjusted_symbols = 0
    for symbol, rows in bars_by_symbol.items():
        clean = [
            row for row in rows
            if row.get("open") and row.get("high") and row.get("low") and row.get("close")
        ]
        if not clean:
            continue
        adjusted = [row for row in clean if row.get("is_adjusted")]
        chosen = adjusted if len(adjusted) >= min_bars else clean
        if adjusted:
            adjusted_symbols += 1
        trimmed[symbol] = chosen[-days:]
    usable = {symbol: rows for symbol, rows in trimmed.items() if len(rows) >= min_bars}
    return {
        "enabled": True,
        "status": "ok",
        "bars_by_symbol": usable,
        "symbols_requested": len(wanted),
        "symbols_with_history": len(usable),
        "symbols_with_adjusted_history": adjusted_symbols,
        "min_bars": min_bars,
        "days": days,
    }


async def latest_fundamentals_from_db(symbols: list[str], chunk_size: int = 500) -> dict[str, Any]:
    """Return the latest stored fundamental snapshot per symbol."""
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled", "fundamentals_by_symbol": {}}
    wanted = sorted({str(symbol).upper() for symbol in symbols if symbol})
    if not wanted:
        return {"enabled": True, "status": "empty", "fundamentals_by_symbol": {}}
    latest: dict[str, tuple[date, dict[str, Any]]] = {}
    fields = [
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
        "forward_pe",
        "forward_profit_growth",
        "pb",
        "roa",
        "nim",
        "next_earnings_date",
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
    ]
    async with AsyncSessionLocal() as session:
        for batch in _chunks(wanted, max(50, int(chunk_size or 500))):
            result = await session.execute(
                select(Fundamental)
                .where(Fundamental.symbol.in_(batch))
                .order_by(Fundamental.symbol, Fundamental.as_of.desc())
            )
            for row in result.scalars().all():
                symbol = str(row.symbol).upper()
                as_of = row.as_of or row.knowledge_date or row.effective_date or date.min
                current = latest.get(symbol)
                if current and current[0] >= as_of:
                    continue
                values: dict[str, Any] = {}
                for field in fields:
                    value = getattr(row, field, None)
                    if value not in (None, ""):
                        values[field] = value.isoformat() if hasattr(value, "isoformat") else value
                values["fundamentals_as_of"] = as_of.isoformat() if hasattr(as_of, "isoformat") else str(as_of)
                latest[symbol] = (as_of, values)
    return {
        "enabled": True,
        "status": "ok",
        "fundamentals_by_symbol": {symbol: values for symbol, (_, values) in latest.items() if values},
        "symbols_requested": len(wanted),
        "symbols_with_fundamentals": len(latest),
    }


async def ensure_recent_bhavcopy_in_db(force: bool = False) -> dict[str, Any]:
    """Auto-fill Supabase if bhavcopy is missing or older than the configured max age."""
    if not DATABASE_ENABLED:
        return {"enabled": False, "status": "disabled", "reason": "DATABASE_URL is not configured"}
    await init_db()
    state = await database_counts()
    latest_date = date.fromisoformat(state["latest_bhavcopy_date"]) if state.get("latest_bhavcopy_date") else None
    today = datetime.now(timezone.utc).astimezone().date()
    weekend_latest_ok = bool(today.weekday() >= 5 and latest_date == latest_trading_day(today))
    stale = bool(state.get("bhavcopy_stale")) and not weekend_latest_ok
    if force or stale or int(state.get("daily_ohlcv") or 0) == 0:
        update = await run_eod_update()
        return {"enabled": True, "status": "updated", "before": state, "update": update}
    return {"enabled": True, "status": "fresh", "state": state, "weekend_latest_ok": weekend_latest_ok}
