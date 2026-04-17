from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 StockDecisionEngine/1.0"
_CACHE: dict[str, tuple[float, Any]] = {}


def cached(key: str, ttl_seconds: int, loader: Any) -> Any:
    now = time.time()
    hit = _CACHE.get(key)
    if hit and now - hit[0] < ttl_seconds:
        return hit[1]
    value = loader()
    _CACHE[key] = (now, value)
    return value


def request_text(url: str, timeout: int = 12) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/xml,application/rss+xml,text/html;q=0.9,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def request_json(url: str, timeout: int = 12) -> dict[str, Any]:
    return json.loads(request_text(url, timeout=timeout))


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
    yahoo_symbol = yahoo_chart_symbol(symbol)
    query_symbol = urllib.parse.quote(yahoo_symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{query_symbol}?range={range_value}&interval={interval}"
    payload = cached(f"yahoo-chart:{yahoo_symbol}:{range_value}:{interval}", 45, lambda: request_json(url))
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
            stamp = datetime.fromtimestamp(ts, tz=timezone.utc)
            bars.append(
                {
                    "datetime": stamp.isoformat() if interval.endswith("m") else stamp.date().isoformat(),
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


def fetch_yahoo_intraday(symbol: str) -> list[dict[str, Any]]:
    """Fast no-key 5-minute bars from Yahoo Finance's chart endpoint.

    This endpoint is unofficial. It is useful for a personal research app, but it should not
    be treated as a licensed market-data feed for production trading automation.
    """
    return fetch_yahoo_chart(symbol, range_value="5d", interval="5m")


def parse_rss_datetime(raw: str | None) -> str | None:
    if not raw:
        return None
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        return None


def rss_items_to_events(xml_text: str, source_name: str, max_records: int = 10) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    events: list[dict[str, Any]] = []
    for item in root.findall(".//item")[:max_records]:
        title = unescape(item.findtext("title") or "News mention")
        link = item.findtext("link")
        published = parse_rss_datetime(item.findtext("pubDate"))
        description = unescape(item.findtext("description") or "")
        lowered = f"{title} {description}".lower()
        sentiment = 0.0
        if any(word in lowered for word in ["order", "wins", "profit", "growth", "approval", "upgrade", "surge", "record"]):
            sentiment += 0.25
        if any(word in lowered for word in ["resigns", "fraud", "downgrade", "loss", "probe", "pledge", "default", "falls"]):
            sentiment -= 0.35
        events.append(
            {
                "title": title,
                "source": source_name,
                "source_type": "credible_news",
                "sentiment": sentiment,
                "importance": 44,
                "timestamp": published,
                "url": link,
                "category": "news",
            }
        )
    return events


def fetch_yahoo_finance_news(symbol: str, max_records: int = 8) -> list[dict[str, Any]]:
    yahoo_symbol = yahoo_chart_symbol(symbol)
    params = urllib.parse.urlencode({"s": yahoo_symbol, "region": "IN", "lang": "en-IN"})
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?{params}"
    xml_text = cached(f"yahoo-news:{yahoo_symbol}", 300, lambda: request_text(url))
    return rss_items_to_events(xml_text, "Yahoo Finance RSS", max_records=max_records)


def fetch_google_news_rss(query: str, max_records: int = 8) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"q": query, "hl": "en-IN", "gl": "IN", "ceid": "IN:en"})
    url = f"https://news.google.com/rss/search?{params}"
    xml_text = cached(f"google-news:{query}", 300, lambda: request_text(url))
    return rss_items_to_events(xml_text, "Google News RSS", max_records=max_records)


def fetch_no_key_news(symbol: str, company_name: str, max_records: int = 12) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    for loader in (
        lambda: fetch_yahoo_finance_news(symbol, max_records=5),
        lambda: fetch_google_news_rss(f'"{company_name}" stock India OR NSE OR BSE', max_records=5),
        lambda: fetch_gdelt_news(f'"{company_name}" stock India', max_records=5),
    ):
        try:
            events.extend(loader())
        except Exception as exc:  # pragma: no cover - network dependent
            errors.append(str(exc))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        key = (event.get("title") or "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(event)
    return deduped[:max_records]


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
            "primary": "Yahoo Finance chart endpoint for no-key 5-minute and daily bars",
            "free_tier": "No key, unofficial, cached by the app; use only for personal research",
            "production_upgrade": "Licensed Indian feed such as Global Datafeeds, TrueData, or broker market data API",
        },
        {
            "layer": "Optional daily technicals and global OHLCV",
            "primary": "Alpha Vantage",
            "free_tier": "Optional fallback if you later get a free key",
            "production_upgrade": "Paid plan or licensed exchange data",
        },
        {
            "layer": "News",
            "primary": "Yahoo Finance RSS, Google News RSS, and GDELT DOC 2.0",
            "free_tier": "No key required; NewsAPI remains an optional fallback",
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
