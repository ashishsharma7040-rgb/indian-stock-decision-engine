from __future__ import annotations

import csv
import io
import json
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.cookiejar import CookieJar
from html import unescape
from typing import Any

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError:  # pragma: no cover - optional dependency during local zero-install use
    SentimentIntensityAnalyzer = None


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 StockDecisionEngine/1.0"
_CACHE: dict[str, tuple[float, Any]] = {}
_PRICE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_SENTIMENT_ANALYZER = SentimentIntensityAnalyzer() if SentimentIntensityAnalyzer else None


def env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return ""


def provider_symbol(provider: str, symbol: str, default: str) -> str:
    clean = symbol.upper().replace("-", "_")
    provider_key = provider.upper().replace("-", "_")
    return (
        os.getenv(f"{provider_key}_SYMBOL_{clean}")
        or os.getenv(f"PRICE_SYMBOL_{clean}")
        or os.getenv(f"SYMBOL_{clean}")
        or default
    )


def cached(key: str, ttl_seconds: int, loader: Any, bypass: bool = False) -> Any:
    now = time.time()
    hit = _CACHE.get(key)
    if not bypass and hit and now - hit[0] < ttl_seconds:
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


def request_bytes(url: str, timeout: int = 18) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,application/zip,application/octet-stream,*/*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def request_json(url: str, timeout: int = 12) -> dict[str, Any]:
    return json.loads(request_text(url, timeout=timeout))


def request_nse_json(api_path: str, timeout: int = 12) -> dict[str, Any]:
    cookie_jar = CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    }
    opener.open(urllib.request.Request("https://www.nseindia.com", headers=headers), timeout=timeout)
    request = urllib.request.Request(f"https://www.nseindia.com{api_path}", headers=headers)
    with opener.open(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


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


def fetch_yahoo_quote(symbol: str, ttl_seconds: int = 60, bypass_cache: bool = False) -> dict[str, Any]:
    now = time.time()
    clean = symbol.upper()
    hit = _PRICE_CACHE.get(clean)
    if not bypass_cache and hit and now - hit[0] < ttl_seconds:
        return hit[1]
    yahoo_symbol = yahoo_chart_symbol(clean)
    query_symbol = urllib.parse.quote(yahoo_symbol, safe="")
    payload = request_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{query_symbol}?range=5d&interval=5m")
    result = payload.get("chart", {}).get("result", [])
    meta = result[0].get("meta", {}) if result else {}
    bars = fetch_yahoo_chart(clean, range_value="5d", interval="5m")
    if not bars:
        bars = fetch_yahoo_chart(clean, range_value="5d", interval="1d")
    if not bars:
        raise RuntimeError(f"No Yahoo quote bars for {clean}")
    latest = bars[-1]
    close = float(meta.get("regularMarketPrice") or latest["close"])
    prev_close = float(meta.get("previousClose") or meta.get("chartPreviousClose") or 0)
    if not prev_close:
        daily_bars = fetch_yahoo_chart(clean, range_value="5d", interval="1d")
        previous_daily = next((bar for bar in reversed(daily_bars[:-1]) if float(bar.get("close", 0) or 0) > 0), None)
        prev_close = float(previous_daily["close"]) if previous_daily else close
    regular_time = meta.get("regularMarketTime")
    timestamp = (
        datetime.fromtimestamp(int(regular_time), tz=timezone.utc).isoformat()
        if regular_time
        else latest.get("datetime")
    )
    quote = {
        "symbol": clean,
        "price": round(close, 2),
        "change_pct": round((close - prev_close) / prev_close * 100, 2) if prev_close else 0,
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        "previous_close": round(prev_close, 2) if prev_close else None,
        "timestamp": timestamp,
        "source": "Yahoo Finance chart",
    }
    _PRICE_CACHE[clean] = (now, quote)
    return quote


def fetch_alpha_vantage_quote(symbol: str) -> dict[str, Any]:
    api_key = env_first("ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY is not configured")
    clean = symbol.upper()
    query_symbol = provider_symbol("ALPHA_VANTAGE", clean, clean)
    params = urllib.parse.urlencode({"function": "GLOBAL_QUOTE", "symbol": query_symbol, "apikey": api_key})
    payload = request_json(f"https://www.alphavantage.co/query?{params}")
    row = payload.get("Global Quote", {})
    price = float(row.get("05. price") or 0)
    if not price:
        raise RuntimeError(payload.get("Note") or payload.get("Information") or f"Alpha Vantage quote unavailable for {query_symbol}")
    prev_close = float(row.get("08. previous close") or price)
    return {
        "symbol": clean,
        "price": round(price, 2),
        "change_pct": round((price - prev_close) / prev_close * 100, 2) if prev_close else 0,
        "open": _clean_float(row.get("02. open")),
        "high": _clean_float(row.get("03. high")),
        "low": _clean_float(row.get("04. low")),
        "close": round(price, 2),
        "volume": int(float(row.get("06. volume") or 0)),
        "previous_close": round(prev_close, 2) if prev_close else None,
        "timestamp": row.get("07. latest trading day"),
        "source": f"Alpha Vantage ({query_symbol})",
    }


def fetch_twelve_data_quote(symbol: str) -> dict[str, Any]:
    api_key = env_first("TWELVE_DATA_API_KEY", "TWELVEDATA_API_KEY")
    if not api_key:
        raise RuntimeError("TWELVE_DATA_API_KEY is not configured")
    clean = symbol.upper()
    query_symbol = provider_symbol("TWELVE_DATA", clean, f"{clean}:NSE")
    params = urllib.parse.urlencode({"symbol": query_symbol, "apikey": api_key})
    payload = request_json(f"https://api.twelvedata.com/quote?{params}")
    if payload.get("status") == "error":
        raise RuntimeError(payload.get("message") or f"Twelve Data quote unavailable for {query_symbol}")
    price = float(payload.get("close") or payload.get("price") or 0)
    if not price:
        raise RuntimeError(f"Twelve Data quote unavailable for {query_symbol}")
    prev_close = float(payload.get("previous_close") or price)
    return {
        "symbol": clean,
        "price": round(price, 2),
        "change_pct": round(float(payload.get("percent_change") or ((price - prev_close) / prev_close * 100 if prev_close else 0)), 2),
        "open": _clean_float(payload.get("open")),
        "high": _clean_float(payload.get("high")),
        "low": _clean_float(payload.get("low")),
        "close": round(price, 2),
        "volume": int(float(payload.get("volume") or 0)),
        "previous_close": round(prev_close, 2) if prev_close else None,
        "timestamp": payload.get("datetime"),
        "source": f"Twelve Data ({query_symbol})",
    }


def fetch_finnhub_quote(symbol: str) -> dict[str, Any]:
    api_key = env_first("FINNHUB_API_KEY", "FINNHUB_TOKEN")
    if not api_key:
        raise RuntimeError("FINNHUB_API_KEY is not configured")
    clean = symbol.upper()
    query_symbol = provider_symbol("FINNHUB", clean, f"{clean}.NS")
    params = urllib.parse.urlencode({"symbol": query_symbol, "token": api_key})
    payload = request_json(f"https://finnhub.io/api/v1/quote?{params}")
    price = float(payload.get("c") or 0)
    if not price:
        raise RuntimeError(f"Finnhub quote unavailable for {query_symbol}")
    prev_close = float(payload.get("pc") or price)
    timestamp = payload.get("t")
    return {
        "symbol": clean,
        "price": round(price, 2),
        "change_pct": round(float(payload.get("dp") or ((price - prev_close) / prev_close * 100 if prev_close else 0)), 2),
        "open": _clean_float(payload.get("o")),
        "high": _clean_float(payload.get("h")),
        "low": _clean_float(payload.get("l")),
        "close": round(price, 2),
        "volume": None,
        "previous_close": round(prev_close, 2) if prev_close else None,
        "timestamp": datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat() if timestamp else None,
        "source": f"Finnhub ({query_symbol})",
    }


def fetch_fmp_quote(symbol: str) -> dict[str, Any]:
    api_key = env_first("FMP_API_KEY", "FINANCIAL_MODELING_PREP_API_KEY")
    if not api_key:
        raise RuntimeError("FMP_API_KEY is not configured")
    clean = symbol.upper()
    query_symbol = provider_symbol("FMP", clean, f"{clean}.NS")
    payload = request_json(f"https://financialmodelingprep.com/api/v3/quote/{urllib.parse.quote(query_symbol, safe='')}?apikey={urllib.parse.quote(api_key)}")
    if not payload:
        raise RuntimeError(f"FMP quote unavailable for {query_symbol}")
    row = payload[0]
    price = float(row.get("price") or 0)
    if not price:
        raise RuntimeError(f"FMP quote unavailable for {query_symbol}")
    prev_close = float(row.get("previousClose") or price)
    return {
        "symbol": clean,
        "price": round(price, 2),
        "change_pct": round(float(row.get("changesPercentage") or ((price - prev_close) / prev_close * 100 if prev_close else 0)), 2),
        "open": _clean_float(row.get("open")),
        "high": _clean_float(row.get("dayHigh")),
        "low": _clean_float(row.get("dayLow")),
        "close": round(price, 2),
        "volume": int(float(row.get("volume") or 0)),
        "previous_close": round(prev_close, 2) if prev_close else None,
        "timestamp": datetime.fromtimestamp(int(row["timestamp"]), tz=timezone.utc).isoformat() if row.get("timestamp") else None,
        "source": f"FMP ({query_symbol})",
    }


def fetch_market_quote(symbol: str, ttl_seconds: int = 60, bypass_cache: bool = False) -> dict[str, Any]:
    clean = symbol.upper()
    provider_pref = env_first("PRICE_DATA_PROVIDER", "FINANCIAL_DATA_PROVIDER").lower()
    cache_key = f"best:{provider_pref or 'auto'}:{clean}"
    now = time.time()
    hit = _PRICE_CACHE.get(cache_key)
    if not bypass_cache and hit and now - hit[0] < ttl_seconds:
        return hit[1]
    providers = {
        "twelve_data": fetch_twelve_data_quote,
        "twelvedata": fetch_twelve_data_quote,
        "finnhub": fetch_finnhub_quote,
        "fmp": fetch_fmp_quote,
        "alpha_vantage": fetch_alpha_vantage_quote,
        "alphavantage": fetch_alpha_vantage_quote,
        "yahoo": lambda item: fetch_yahoo_quote(item, ttl_seconds=ttl_seconds, bypass_cache=bypass_cache),
    }
    if provider_pref in providers:
        order = [providers[provider_pref], lambda item: fetch_yahoo_quote(item, ttl_seconds=ttl_seconds, bypass_cache=bypass_cache)]
    else:
        order = [
            fetch_twelve_data_quote,
            fetch_finnhub_quote,
            fetch_fmp_quote,
            fetch_alpha_vantage_quote,
            lambda item: fetch_yahoo_quote(item, ttl_seconds=ttl_seconds, bypass_cache=bypass_cache),
        ]
    errors: list[str] = []
    for loader in order:
        try:
            quote = loader(clean)
            _PRICE_CACHE[cache_key] = (now, quote)
            return quote
        except Exception as exc:  # pragma: no cover - network/provider dependent
            errors.append(str(exc))
    raise RuntimeError("; ".join(errors[-4:]) or f"No quote provider returned data for {clean}")


def _normalise_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _pick(row: dict[str, Any], names: list[str]) -> Any:
    lookup = {_normalise_header(key): value for key, value in row.items()}
    for name in names:
        key = _normalise_header(name)
        if key in lookup and lookup[key] not in (None, ""):
            return lookup[key]
    return None


def _clean_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(str(value).replace(",", "").strip())
    except ValueError:
        return default


def _read_csv_rows(raw_text: str) -> list[dict[str, Any]]:
    cleaned = raw_text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(cleaned))
    return [dict(row) for row in reader if row]


def fetch_nse_equity_master() -> list[dict[str, Any]]:
    """Download the NSE-listed equity master for all-symbol search.

    This file gives the full NSE symbol/name universe. It is not a live price feed;
    bhavcopy or broker ticks should be layered on top for pricing.
    """
    urls = [
        "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
        "https://www.nseindia.com/content/equities/EQUITY_L.csv",
    ]
    last_error: Exception | None = None
    raw_text = ""
    for url in urls:
        try:
            raw_text = cached(f"nse-equity-master:{url}", 86400, lambda url=url: request_text(url, timeout=18))
            break
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = exc
    if not raw_text:
        raise RuntimeError(f"NSE equity master download failed: {last_error}")

    rows: list[dict[str, Any]] = []
    allowed_series = {"EQ", "BE", "BZ", "SM", "ST", "SZ"}
    for row in _read_csv_rows(raw_text):
        symbol = str(_pick(row, ["SYMBOL", "Symbol"]) or "").strip().upper()
        if not symbol:
            continue
        series = str(_pick(row, ["SERIES", "Series"]) or "").strip().upper()
        if series and series not in allowed_series:
            continue
        rows.append(
            {
                "symbol": symbol,
                "name": str(_pick(row, ["NAME OF COMPANY", "Company Name", "NAME"]) or symbol).strip(),
                "series": series or "EQ",
                "isin": str(_pick(row, ["ISIN NUMBER", "ISIN"]) or "").strip(),
                "date_of_listing": str(_pick(row, ["DATE OF LISTING", "Date of Listing"]) or "").strip(),
                "face_value": _clean_float(_pick(row, ["FACE VALUE", "Face Value"])),
                "source": "NSE equity master",
            }
        )
    return rows


def _nse_bhavcopy_urls(day: date) -> list[tuple[str, bool]]:
    year = day.strftime("%Y")
    mon = day.strftime("%b").upper()
    ddmonyyyy = day.strftime("%d%b%Y").upper()
    yyyymmdd = day.strftime("%Y%m%d")
    ddmmyyyy = day.strftime("%d%m%Y")
    return [
        (f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip", True),
        (f"https://archives.nseindia.com/content/historical/EQUITIES/{year}/{mon}/cm{ddmonyyyy}bhav.csv.zip", True),
        (f"https://nsearchives.nseindia.com/content/historical/EQUITIES/{year}/{mon}/cm{ddmonyyyy}bhav.csv.zip", True),
        (f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv", False),
        (f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{ddmmyyyy}.csv", False),
    ]


def _decode_csv_payload(payload: bytes, zipped: bool) -> str:
    if not zipped:
        return payload.decode("utf-8", errors="replace")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("NSE bhavcopy zip did not contain a CSV file")
        return archive.read(csv_names[0]).decode("utf-8", errors="replace")


def _parse_bhavcopy_csv(raw_text: str, as_of: date, source: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    allowed_series = {"EQ", "BE", "BZ", "SM", "ST", "SZ"}
    for row in _read_csv_rows(raw_text):
        symbol = str(_pick(row, ["SYMBOL", "TckrSymb", "TICKER_SYMBOL", "TICKER"]) or "").strip().upper()
        if not symbol:
            continue
        series = str(_pick(row, ["SERIES", "SctySrs", "SECURITY_SERIES"]) or "EQ").strip().upper()
        if series and series not in allowed_series:
            continue
        close = _clean_float(_pick(row, ["CLOSE", "CLOSE_PRICE", "ClsPric", "ClosePrice"]))
        if close is None:
            continue
        previous = _clean_float(_pick(row, ["PREVCLOSE", "PREV_CLOSE", "PrvsClsgPric", "PreviousClose"]))
        volume = _clean_float(_pick(row, ["TOTTRDQTY", "TTL_TRD_QNTY", "TtlTradgVol", "TOTAL_TRADED_QUANTITY"]), 0) or 0
        turnover = _clean_float(_pick(row, ["TOTTRDVAL", "TURNOVER_LACS", "TtlTrfVal", "TOTAL_TRADED_VALUE"]), 0) or 0
        if "sec_bhavdata_full" in source and turnover:
            turnover *= 100000
        change_pct = round((close - previous) / previous * 100, 2) if previous else None
        rows.append(
            {
                "symbol": symbol,
                "series": series or "EQ",
                "open": _clean_float(_pick(row, ["OPEN", "OPEN_PRICE", "OpnPric", "OpenPrice"])),
                "high": _clean_float(_pick(row, ["HIGH", "HIGH_PRICE", "HghPric", "HighPrice"])),
                "low": _clean_float(_pick(row, ["LOW", "LOW_PRICE", "LwPric", "LowPrice"])),
                "close": close,
                "last": _clean_float(_pick(row, ["LAST", "LAST_PRICE", "LastPric"])),
                "previous_close": previous,
                "change_pct": change_pct,
                "volume": int(volume),
                "turnover": round(float(turnover), 2),
                "isin": str(_pick(row, ["ISIN", "ISIN NUMBER"]) or "").strip(),
                "as_of": as_of.isoformat(),
                "source": source,
            }
        )
    return rows


def fetch_nse_bhavcopy_for_date(day: date) -> dict[str, Any]:
    last_error: Exception | None = None
    for url, zipped in _nse_bhavcopy_urls(day):
        try:
            payload = cached(f"nse-bhavcopy:{day.isoformat()}:{url}", 86400, lambda url=url: request_bytes(url, timeout=22))
            raw_text = _decode_csv_payload(payload, zipped=zipped)
            rows = _parse_bhavcopy_csv(raw_text, day, url)
            if rows:
                return {"as_of": day.isoformat(), "source": url, "rows": rows}
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = exc
            continue
    raise RuntimeError(f"NSE bhavcopy unavailable for {day.isoformat()}: {last_error}")


def fetch_latest_nse_bhavcopy(max_lookback_days: int = 10) -> dict[str, Any]:
    today = datetime.now(timezone.utc).astimezone().date()
    last_error: Exception | None = None
    for offset in range(max_lookback_days + 1):
        day = today - timedelta(days=offset)
        if day.weekday() >= 5:
            continue
        try:
            return fetch_nse_bhavcopy_for_date(day)
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = exc
    raise RuntimeError(f"No NSE bhavcopy found in last {max_lookback_days} days: {last_error}")


def fetch_nse_advance_decline(index_name: str = "NIFTY 50") -> dict[str, Any]:
    encoded_index = urllib.parse.quote(index_name, safe="")
    payload = cached(
        f"nse-advdec:{index_name}",
        900,
        lambda: request_nse_json(f"/api/equity-stockIndices?index={encoded_index}"),
    )
    advancers = 0
    decliners = 0
    unchanged = 0
    for row in payload.get("data", []):
        symbol = str(row.get("symbol", "")).upper()
        if symbol in {index_name.upper(), "NIFTY 50"}:
            continue
        try:
            change_pct = float(row.get("pChange", 0) or 0)
        except (TypeError, ValueError):
            continue
        if change_pct > 0:
            advancers += 1
        elif change_pct < 0:
            decliners += 1
        else:
            unchanged += 1
    total = advancers + decliners + unchanged
    breadth_pct = round(advancers / total * 100, 2) if total else 50.0
    return {
        "index": index_name,
        "advancers": advancers,
        "decliners": decliners,
        "unchanged": unchanged,
        "advance_decline_ratio": round(advancers / max(decliners, 1), 2),
        "advance_decline_breadth_pct": breadth_pct,
        "source": "NSE equity-stockIndices",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


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


FILING_BOILERPLATE_PATTERNS = [
    r"this is for your information and records?.*",
    r"kindly take the same on record.*",
    r"pursuant to regulation \d+.*?",
    r"sebi \(listing obligations and disclosure requirements\).*?",
    r"we wish to inform you that",
    r"please find enclosed.*?",
]


def clean_filing_text(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", text or "")
    cleaned = unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    for pattern in FILING_BOILERPLATE_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cleaned).strip()


def estimate_news_sentiment(text: str, source_type: str = "", category: str = "") -> float:
    cleaned = clean_filing_text(text) if source_type in {"exchange_filing", "company_ir", "earnings_transcript"} else text
    if category == "promoter_buying":
        return 0.68
    if _SENTIMENT_ANALYZER:
        return max(-0.7, min(0.7, _SENTIMENT_ANALYZER.polarity_scores(cleaned)["compound"]))
    lowered = cleaned.lower()
    sentiment = 0.0
    if any(word in lowered for word in ["order", "wins", "profit", "growth", "approval", "upgrade", "surge", "record", "promoter purchase", "open market purchase"]):
        sentiment += 0.25
    if any(word in lowered for word in ["resigns", "fraud", "downgrade", "loss", "probe", "pledge", "default", "falls"]):
        sentiment -= 0.35
    return max(-0.7, min(0.7, sentiment))


def rss_items_to_events(xml_text: str, source_name: str, max_records: int = 10) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    events: list[dict[str, Any]] = []
    for item in root.findall(".//item")[:max_records]:
        title = unescape(item.findtext("title") or "News mention")
        link = item.findtext("link")
        published = parse_rss_datetime(item.findtext("pubDate"))
        description = unescape(item.findtext("description") or "")
        sentiment = estimate_news_sentiment(f"{title}. {description}", source_type="credible_news", category="news")
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


def fetch_yahoo_finance_news(symbol: str, max_records: int = 8, bypass_cache: bool = False) -> list[dict[str, Any]]:
    yahoo_symbol = yahoo_chart_symbol(symbol)
    params = urllib.parse.urlencode({"s": yahoo_symbol, "region": "IN", "lang": "en-IN"})
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?{params}"
    xml_text = cached(f"yahoo-news:{yahoo_symbol}", 300, lambda: request_text(url), bypass=bypass_cache)
    return rss_items_to_events(xml_text, "Yahoo Finance RSS", max_records=max_records)


def fetch_google_news_rss(query: str, max_records: int = 8, bypass_cache: bool = False) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"q": query, "hl": "en-IN", "gl": "IN", "ceid": "IN:en"})
    url = f"https://news.google.com/rss/search?{params}"
    xml_text = cached(f"google-news:{query}", 300, lambda: request_text(url), bypass=bypass_cache)
    return rss_items_to_events(xml_text, "Google News RSS", max_records=max_records)


def fetch_no_key_news(symbol: str, company_name: str, max_records: int = 12, bypass_cache: bool = False) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    for loader in (
        lambda: fetch_yahoo_finance_news(symbol, max_records=5, bypass_cache=bypass_cache),
        lambda: fetch_google_news_rss(f'"{company_name}" stock India OR NSE OR BSE', max_records=5, bypass_cache=bypass_cache),
        lambda: fetch_gdelt_news(f'"{company_name}" stock India', max_records=5),
    ):
        try:
            events.extend(loader())
        except Exception as exc:  # pragma: no cover - network dependent
            errors.append(str(exc))
    return dedupe_events(events)[:max_records]


def fetch_alpha_vantage_daily(symbol: str) -> list[dict[str, Any]]:
    api_key = env_first("ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY")
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
        sentiment = max(-0.35, min(0.35, tone / 25))
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


def fetch_newsapi(query: str, max_records: int = 10, bypass_cache: bool = False) -> list[dict[str, Any]]:
    api_key = env_first("NEWSAPI_API_KEY", "NEWS_API_KEY", "NEWSAPI_KEY", "NEWS_API_ORG_KEY")
    if not api_key:
        return []
    from_date = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
    params = urllib.parse.urlencode(
        {
            "q": query,
            "language": "en",
            "from": from_date,
            "sortBy": "publishedAt",
            "pageSize": max_records,
            "apiKey": api_key,
        }
    )
    url = f"https://newsapi.org/v2/everything?{params}"
    payload = cached(f"newsapi:{query}:{max_records}", 300, lambda: request_json(url), bypass=bypass_cache)
    events: list[dict[str, Any]] = []
    for article in payload.get("articles", []):
        title = article.get("title") or "News mention"
        description = article.get("description") or ""
        events.append(
            {
                "title": title,
                "source": article.get("source", {}).get("name", "NewsAPI"),
                "source_type": "credible_news",
                "sentiment": estimate_news_sentiment(f"{title}. {description}", source_type="credible_news", category="news"),
                "importance": 42,
                "timestamp": article.get("publishedAt"),
                "url": article.get("url"),
                "category": "news",
            }
        )
    return events


def _event_timestamp_value(event: dict[str, Any]) -> float:
    raw = event.get("timestamp")
    if not raw:
        return 0
    try:
        parsed = parsedate_to_datetime(str(raw))
    except Exception:
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except Exception:
            return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        key = (event.get("url") or event.get("title") or "").strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(event)
    return sorted(deduped, key=_event_timestamp_value, reverse=True)


def fetch_market_news(symbol: str, company_name: str, max_records: int = 12, bypass_cache: bool = False) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    news_queries = [
        f'"{company_name}" India stock',
        f"{symbol.upper()} NSE BSE result order deal stock",
    ]
    for query in news_queries:
        try:
            events.extend(fetch_newsapi(query, max_records=6, bypass_cache=bypass_cache))
        except Exception as exc:  # pragma: no cover - network/provider dependent
            errors.append(f"NewsAPI: {exc}")
    try:
        events.extend(fetch_no_key_news(symbol, company_name, max_records=10, bypass_cache=bypass_cache))
    except Exception as exc:  # pragma: no cover - network/provider dependent
        errors.append(f"No-key news: {exc}")
    deduped = dedupe_events(events)[:max_records]
    if errors and deduped:
        deduped[0]["provider_warnings"] = errors[:3]
    return deduped


def recommended_api_stack() -> list[dict[str, str]]:
    return [
        {
            "layer": "All-NSE searchable universe",
            "primary": "NSE equity master plus daily bhavcopy EOD cache",
            "free_tier": "No key; one daily file gives symbol, close, volume, and change for the listed universe",
            "production_upgrade": "Licensed NSE data vendor for guaranteed uptime and redistribution rights",
        },
        {
            "layer": "Price OHLCV",
            "primary": "Yahoo Finance chart endpoint for no-key 5-minute and daily bars, including ^NSEI for Nifty",
            "free_tier": "No key, unofficial, cached by the app; use only for personal research",
            "production_upgrade": "Licensed Indian feed such as Global Datafeeds, TrueData, or broker market data API",
        },
        {
            "layer": "Market breadth",
            "primary": "NSE public Nifty 50 advance/decline proxy",
            "free_tier": "No key; cached and best-effort because NSE may rate-limit direct calls",
            "production_upgrade": "Licensed breadth feed or an end-of-day bhavcopy pipeline",
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
