from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from data_sources import (
    OFFICIAL_FILING_LINKS,
    fetch_alpha_vantage_daily,
    fetch_gdelt_news,
    fetch_newsapi,
    fetch_yahoo_chart,
    recommended_api_stack,
)
from scoring_engine import final_decision, market_support_score
from seed_data import build_demo_dataset


APP_NAME = "Indian Stock Decision Engine"
DATASET = build_demo_dataset()

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
        "business_score": business["score"],
        "tailwind_score": tailwind["score"],
        "event_score": events["score"],
        "technical_score": technical["score"],
        "market_score": stock["market_support"]["score"],
        "risk_score": risk["score"],
        "conviction": stock["conviction"],
        "candidate": stock["candidate"],
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
            "prices": "Refresh every 5 minutes for a licensed intraday feed; demo seed is daily.",
            "events": "Refresh official filings and news every 15 minutes.",
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
            return stock
    raise HTTPException(status_code=404, detail=f"{symbol} is not in the current universe")


@app.post("/api/refresh/{symbol}")
def refresh_symbol(symbol: str) -> dict[str, Any]:
    """Best-effort live refresh.

    It keeps deterministic scoring, but tries optional price/news connectors first.
    If a connector has no key or fails, the endpoint falls back to seeded data.
    """
    symbol = symbol.upper()
    company = next((deepcopy(item) for item in DATASET["companies"] if item["symbol"] == symbol), None)
    if not company:
        raise HTTPException(status_code=404, detail=f"{symbol} is not in the current universe")

    connector_notes: list[str] = []
    bars = fetch_alpha_vantage_daily(symbol)
    if len(bars) < 60:
        connector_notes.append("Alpha Vantage unavailable or insufficient bars")
        try:
            bars = fetch_yahoo_chart(symbol)
        except Exception as exc:  # pragma: no cover - network dependent
            connector_notes.append(f"Yahoo chart fallback failed: {exc}")
            bars = []
    if len(bars) < 60:
        connector_notes.append("Using seeded OHLCV bars")
        bars = DATASET["bars"][symbol]

    news_events: list[dict[str, Any]] = []
    try:
        news_events.extend(fetch_newsapi(f"{company['name']} India stock", max_records=5))
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"NewsAPI failed: {exc}")
    try:
        news_events.extend(fetch_gdelt_news(f'"{company["name"]}" stock India', max_records=5))
    except Exception as exc:  # pragma: no cover - network dependent
        connector_notes.append(f"GDELT failed: {exc}")
    company["events"] = company.get("events", []) + news_events[:6]

    market = market_support_score(DATASET["market"])
    scored = final_decision(company, bars, DATASET["benchmark_bars"], market)
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
            "ALPHA_VANTAGE_API_KEY",
            "NEWSAPI_API_KEY",
            "ALPHA_VANTAGE_SYMBOL_<SYMBOL> for custom symbol mapping",
        ],
    }
