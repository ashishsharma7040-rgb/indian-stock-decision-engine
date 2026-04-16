from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


USER_AGENT = "StockDecisionEngine/1.0 personal-research-app"


def request_json(url: str, timeout: int = 12) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def yahoo_chart_symbol(symbol: str) -> str:
    if symbol.upper() == "NIFTY":
        return "^NSEI"
    if symbol.startswith("^"):
        return symbol
    if "." in symbol:
        return symbol
    return f"{symbol}.NS"


def fetch_yahoo_chart(symbol: str, range_value: str = "1y", interval: str = "1d") -> list[dict[str, Any]]:
    """Unofficial fallback for personal research; replace with a licensed feed for production."""
    query_symbol = urllib.parse.quote(yahoo_chart_symbol(symbol), safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{query_symbol}?range={range_value}&interval={interval}"
    payload = request_json(url)
    result = payload.get("chart", {}).get("result", [])
    if not result:
        return []
    data = result[0]
    timestamps = data.get("timestamp", [])
    quote = data.get("indicators", {}).get("quote", [{}])[0]
    bars: list[dict[str, Any]] = []
    for idx, ts in enumerate(timestamps):
        try:
            close = quote["close"][idx]
            if close is None:
                continue
            bars.append(
                {
                    "datetime": datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat(),
                    "open": round(float(quote["open"][idx]), 2),
                    "high": round(float(quote["high"][idx]), 2),
                    "low": round(float(quote["low"][idx]), 2),
                    "close": round(float(close), 2),
                    "volume": int(quote["volume"][idx] or 0),
                }
            )
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    return bars


def fetch_alpha_vantage_daily(symbol: str) -> list[dict[str, Any]]:
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        return []
    query_symbol = os.getenv(f"ALPHA_VANTAGE_SYMBOL_{symbol.upper()}", symbol)
    params = urllib.parse.urlencode(
        {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": query_symbol,
            "apikey": api_key,
            "outputsize": "compact",
        }
    )
    payload = request_json(f"https://www.alphavantage.co/query?{params}")
    series = payload.get("Time Series (Daily)", {})
    bars: list[dict[str, Any]] = []
    for day, row in sorted(series.items()):
        try:
            bars.append(
                {
                    "datetime": day,
                    "open": round(float(row["1. open"]), 2),
                    "high": round(float(row["2. high"]), 2),
                    "low": round(float(row["3. low"]), 2),
                    "close": round(float(row["4. close"]), 2),
                    "volume": int(float(row["6. volume"])),
                }
            )
        except (KeyError, ValueError):
            continue
    return bars


def fetch_gdelt_news(query: str, max_records: int = 10) -> list[dict[str, Any]]:
    encoded = urllib.parse.urlencode(
        {
            "query": query,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": max_records,
            "sort": "HybridRel",
        }
    )
    payload = request_json(f"https://api.gdeltproject.org/api/v2/doc/doc?{encoded}")
    articles = payload.get("articles", [])
    events: list[dict[str, Any]] = []
    for article in articles:
        title = article.get("title") or article.get("seendate") or "News mention"
        tone = float(article.get("tone", 0) or 0)
        sentiment = max(-0.65, min(0.65, tone / 10))
        events.append(
            {
                "title": title,
                "source": article.get("domain", "GDELT"),
                "source_type": "credible_news",
                "sentiment": sentiment,
                "importance": 40,
                "timestamp": article.get("seendate"),
                "url": article.get("url"),
                "category": "news",
            }
        )
    return events


def fetch_newsapi(query: str, max_records: int = 10) -> list[dict[str, Any]]:
    api_key = os.getenv("NEWSAPI_API_KEY")
    if not api_key:
        return []
    params = urllib.parse.urlencode(
        {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": max_records,
            "apiKey": api_key,
        }
    )
    payload = request_json(f"https://newsapi.org/v2/everything?{params}")
    events: list[dict[str, Any]] = []
    for article in payload.get("articles", []):
        events.append(
            {
                "title": article.get("title") or "News mention",
                "source": article.get("source", {}).get("name", "NewsAPI"),
                "source_type": "credible_news",
                "sentiment": 0,
                "importance": 42,
                "timestamp": article.get("publishedAt"),
                "url": article.get("url"),
                "category": "news",
            }
        )
    return events


def recommended_api_stack() -> list[dict[str, str]]:
    return [
        {
            "layer": "Price OHLCV",
            "primary": "Yahoo Finance chart endpoint or yfinance for personal prototyping",
            "free_tier": "No key, unofficial, fragile; use only for personal research",
            "production_upgrade": "Licensed Indian feed such as Global Datafeeds, TrueData, or broker market data API",
        },
        {
            "layer": "Daily technicals and global OHLCV",
            "primary": "Alpha Vantage",
            "free_tier": "25 requests per day on free key",
            "production_upgrade": "Paid plan or licensed exchange data",
        },
        {
            "layer": "News",
            "primary": "GDELT DOC 2.0 and NewsAPI",
            "free_tier": "GDELT is open; NewsAPI developer plan is useful for local development",
            "production_upgrade": "Licensed news feed with redistribution rights",
        },
        {
            "layer": "Official filings",
            "primary": "NSE and BSE corporate filings pages",
            "free_tier": "Public pages; automate carefully and respect terms/rate limits",
            "production_upgrade": "Paid corporate actions/announcement feed",
        },
        {
            "layer": "Fundamentals",
            "primary": "Company annual reports, exchange filings, screener exports, or paid fundamentals feed",
            "free_tier": "Manual import/CSV is safest for personal use",
            "production_upgrade": "Licensed provider with Indian small/mid-cap coverage",
        },
    ]


OFFICIAL_FILING_LINKS = {
    "nse_announcements": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "bse_announcements": "https://www.bseindia.com/corporates/ann.html",
    "bse_results": "https://www.bseindia.com/corporates/Forth_Results.html",
}
