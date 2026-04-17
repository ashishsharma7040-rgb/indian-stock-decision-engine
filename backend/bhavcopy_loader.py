from __future__ import annotations

import asyncio
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
                )
            )
            inserted += 1
        await session.commit()
    return {"enabled": True, "status": "ok", "as_of": as_of.isoformat(), "rows": inserted, "source": payload.get("source")}


async def run_eod_update() -> dict[str, Any]:
    if not DATABASE_ENABLED:
        return {"enabled": False, "status": "disabled", "reason": "DATABASE_URL is not configured"}
    await init_db()
    master_count = await upsert_equity_master()
    bhavcopy = await update_database_with_bhavcopy(latest_trading_day())
    return {"enabled": True, "status": "ok", "equity_master_count": master_count, "bhavcopy": bhavcopy}


async def database_counts() -> dict[str, Any]:
    if not DATABASE_ENABLED or AsyncSessionLocal is None:
        return {"enabled": False, "status": "disabled"}
    async with AsyncSessionLocal() as session:
        companies = await session.scalar(select(func.count()).select_from(Company))
        bars = await session.scalar(select(func.count()).select_from(DailyOHLCV))
    return {"enabled": True, "companies": companies or 0, "daily_ohlcv": bars or 0}
