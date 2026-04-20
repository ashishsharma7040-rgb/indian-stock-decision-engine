from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from data_sources import fetch_latest_nse_bhavcopy, fetch_nse_bhavcopy_for_date, fetch_nse_equity_master
from database import AsyncSessionLocal, DATABASE_ENABLED, init_db
from models import Company, DailyOHLCV

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
        select(DailyOHLCV.date).distinct().order_by(DailyOHLCV.date.desc())
    )
    dates = list(result.scalars().all())
    kept = dates[:keep_count]
    if not kept:
        return {"enabled": True, "deleted_rows": 0, "kept_dates": []}
    delete_result = await session.execute(delete(DailyOHLCV).where(~DailyOHLCV.date.in_(kept)))
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
        inserted = 0
        for row in rows:
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            company = await session.get(Company, symbol)
            if company is None:
                company = Company(
                    symbol=symbol,
                    name=symbol,
                    sector="Unclassified",
                    industry="NSE Equity",
                    series=row.get("series") or "EQ",
                    isin=row.get("isin") or None,
                    source="NSE bhavcopy",
                )
                session.add(company)
            else:
                company.series = row.get("series") or company.series
                company.isin = row.get("isin") or company.isin
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
    master_count = await upsert_equity_master()
    # Use the latest available NSE bhavcopy with lookback. This handles exchange
    # holidays better than only checking the last weekday.
    bhavcopy = await update_database_with_bhavcopy(None)
    return {"enabled": True, "status": "ok", "equity_master_count": master_count, "bhavcopy": bhavcopy}


async def database_counts() -> dict[str, Any]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled"}
    async with AsyncSessionLocal() as session:
        companies = await session.scalar(select(func.count()).select_from(Company))
        bars = await session.scalar(select(func.count()).select_from(DailyOHLCV))
        latest_dates_result = await session.execute(
            select(DailyOHLCV.date).distinct().order_by(DailyOHLCV.date.desc()).limit(BHAVCOPY_RETENTION_DATES)
        )
        latest_dates = list(latest_dates_result.scalars().all())
    latest_date = latest_dates[0] if latest_dates else None
    age_days = (datetime.now(timezone.utc).date() - latest_date).days if latest_date else None
    return {
        "enabled": True,
        "companies": companies or 0,
        "daily_ohlcv": bars or 0,
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
        latest_date = await session.scalar(select(func.max(DailyOHLCV.date)))
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
        result = await session.execute(select(DailyOHLCV).where(DailyOHLCV.date == latest_date))
        bars = list(result.scalars().all())
        bars.sort(key=lambda item: float(item.turnover or 0) or float(item.close or 0) * float(item.volume or 0), reverse=True)
        bars = bars[: max(1, int(limit or 3000))]
        symbols = [bar.symbol for bar in bars if bar.symbol]
        companies_by_symbol: dict[str, Company] = {}
        if symbols:
            company_result = await session.execute(select(Company).where(Company.symbol.in_(symbols)))
            companies_by_symbol = {row.symbol: row for row in company_result.scalars().all()}
    rows: list[dict[str, Any]] = []
    for bar in bars:
        company = companies_by_symbol.get(bar.symbol)
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


async def ensure_recent_bhavcopy_in_db(force: bool = False) -> dict[str, Any]:
    """Auto-fill Supabase if bhavcopy is missing or older than the configured max age."""
    if not DATABASE_ENABLED:
        return {"enabled": False, "status": "disabled", "reason": "DATABASE_URL is not configured"}
    state = await database_counts()
    if force or state.get("bhavcopy_stale") or int(state.get("daily_ohlcv") or 0) == 0:
        update = await run_eod_update()
        return {"enabled": True, "status": "updated", "before": state, "update": update}
    return {"enabled": True, "status": "fresh", "state": state}
