from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

try:
    import pandas as pd
except ImportError:  # pragma: no cover - pandas is in requirements, but keep boot safe
    pd = None  # type: ignore

try:
    import nsefin  # type: ignore
except ImportError:  # pragma: no cover - optional provider
    nsefin = None  # type: ignore


logger = logging.getLogger(__name__)


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _clean_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _clean_int(value: Any, default: int = 0) -> int:
    clean = _clean_float(value)
    return int(clean) if clean is not None else default


def _normalise_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _pick(row: dict[str, Any], names: list[str]) -> Any:
    lookup = {_normalise_key(key): value for key, value in row.items()}
    for name in names:
        key = _normalise_key(name)
        if key in lookup and lookup[key] not in (None, ""):
            return lookup[key]
    return None


class NSEDataFetcher:
    """Best-effort official-NSE data adapter.

    The open-source NSE wrappers have changed APIs across releases. This class keeps
    those moving parts behind a single optional layer so the app still boots when a
    package is absent or when NSE blocks a particular public endpoint.
    """

    def __init__(self) -> None:
        self._nse_client: Any | None = None
        self._bhav_cache: dict[str, Any] = {}
        self._corp_actions_cache: dict[str, list[dict[str, Any]]] = {}

    @property
    def available(self) -> bool:
        return nsefin is not None and pd is not None

    def status(self) -> dict[str, Any]:
        return {
            "nsefin_available": nsefin is not None,
            "pandas_available": pd is not None,
            "corporate_actions_adjustment": _is_truthy(os.getenv("ENABLE_CORPORATE_ACTIONS_ADJUSTMENT", "true")),
            "preference": os.getenv("DATA_SOURCE_PREFERENCE", "auto"),
        }

    def _client(self) -> Any:
        if nsefin is None:
            raise RuntimeError("nsefin is not installed")
        if self._nse_client is not None:
            return self._nse_client
        for name in ("NSEClient", "NseClient", "NSE", "NSEFinance"):
            cls = getattr(nsefin, name, None)
            if cls:
                try:
                    self._nse_client = cls()
                    return self._nse_client
                except Exception:
                    continue
        self._nse_client = nsefin
        return self._nse_client

    def _candidate_callables(self, names: list[str]) -> list[Callable[..., Any]]:
        client = self._client()
        callables: list[Callable[..., Any]] = []
        for target in (client, nsefin):
            if target is None:
                continue
            for name in names:
                candidate = getattr(target, name, None)
                if callable(candidate) and candidate not in callables:
                    callables.append(candidate)
        return callables

    def _as_dataframe(self, raw: Any) -> Any:
        if pd is None:
            raise RuntimeError("pandas is not installed")
        if raw is None:
            raise RuntimeError("empty NSE response")
        if hasattr(raw, "empty"):
            df = raw.copy()
        elif isinstance(raw, dict):
            rows = raw.get("data") or raw.get("rows") or raw.get("records") or raw
            df = pd.DataFrame(rows)
        else:
            df = pd.DataFrame(raw)
        if df.empty:
            raise RuntimeError("empty NSE dataframe")
        df.columns = [str(col).upper().strip() for col in df.columns]
        return df

    def fetch_bhavcopy(self, target_date: date | None = None, max_lookback_days: int = 4) -> Any:
        """Fetch official NSE equity bhavcopy via nsefin when available."""
        if not self.available:
            raise RuntimeError("nsefin/pandas is unavailable")
        requested = target_date or date.today()
        last_error: Exception | None = None
        methods = self._candidate_callables(
            [
                "get_equity_bhav_copy",
                "get_equity_bhavcopy",
                "equity_bhav_copy",
                "equity_bhavcopy",
                "get_bhavcopy",
                "bhavcopy",
            ]
        )
        if not methods:
            raise RuntimeError("nsefin does not expose a recognised bhavcopy method")

        for offset in range(max_lookback_days + 1):
            day = requested - timedelta(days=offset)
            if day.weekday() >= 5:
                continue
            cache_key = day.isoformat()
            if cache_key in self._bhav_cache:
                return self._bhav_cache[cache_key]
            for method in methods:
                arg_sets = [(day,), (day.strftime("%d-%m-%Y"),), (day.strftime("%Y-%m-%d"),)]
                if offset == 0:
                    arg_sets.append(())
                for args in arg_sets:
                    try:
                        raw = method(*args)
                        df = self._as_dataframe(raw)
                        self._bhav_cache[cache_key] = df
                        return df
                    except Exception as exc:  # pragma: no cover - provider dependent
                        last_error = exc
                        continue
        raise RuntimeError(f"nsefin bhavcopy failed near {requested.isoformat()}: {last_error}")

    def bhavcopy_payload(self, target_date: date | None = None) -> dict[str, Any]:
        day = target_date or date.today()
        df = self.fetch_bhavcopy(day, max_lookback_days=0 if target_date else 4)
        rows: list[dict[str, Any]] = []
        for raw_row in df.to_dict("records"):
            symbol = str(_pick(raw_row, ["SYMBOL", "TckrSymb", "TICKER_SYMBOL"]) or "").strip().upper()
            if not symbol:
                continue
            series = str(_pick(raw_row, ["SERIES", "SctySrs", "SECURITY_SERIES"]) or "EQ").strip().upper()
            if series and series not in {"EQ", "BE", "BZ", "SM", "ST", "SZ"}:
                continue
            close = _clean_float(_pick(raw_row, ["CLOSE", "CLOSE_PRICE", "ClsPric", "ClosePrice"]))
            if close is None:
                continue
            previous = _clean_float(_pick(raw_row, ["PREVCLOSE", "PREV_CLOSE", "PrvsClsgPric", "PreviousClose"]))
            turnover = _clean_float(_pick(raw_row, ["TOTTRDVAL", "TURNOVER_LACS", "TtlTrfVal", "TOTAL_TRADED_VALUE"]), 0) or 0
            volume = _clean_int(_pick(raw_row, ["TOTTRDQTY", "TTL_TRD_QNTY", "TtlTradgVol", "TOTAL_TRADED_QUANTITY", "VOLUME"]))
            upper_circuit = _clean_float(_pick(raw_row, ["UPPER_CIRCUIT", "UPPER_PRICE_BAND", "upperPriceBand", "UPPERBAND"]))
            lower_circuit = _clean_float(_pick(raw_row, ["LOWER_CIRCUIT", "LOWER_PRICE_BAND", "lowerPriceBand", "LOWERBAND"]))
            circuit_limit_pct = None
            if upper_circuit and lower_circuit and close:
                circuit_limit_pct = round(min(abs(upper_circuit - close), abs(close - lower_circuit)) / close * 100, 2)
            rows.append(
                {
                    "symbol": symbol,
                    "series": series or "EQ",
                    "open": _clean_float(_pick(raw_row, ["OPEN", "OPEN_PRICE", "OpnPric", "OpenPrice"])),
                    "high": _clean_float(_pick(raw_row, ["HIGH", "HIGH_PRICE", "HghPric", "HighPrice"])),
                    "low": _clean_float(_pick(raw_row, ["LOW", "LOW_PRICE", "LwPric", "LowPrice"])),
                    "close": close,
                    "last": _clean_float(_pick(raw_row, ["LAST", "LAST_PRICE", "LastPric"])),
                    "previous_close": previous,
                    "change_pct": round((close - previous) / previous * 100, 2) if previous else None,
                    "volume": volume,
                    "turnover": round(float(turnover), 2),
                    "upper_circuit": upper_circuit,
                    "lower_circuit": lower_circuit,
                    "circuit_limit_pct": circuit_limit_pct,
                    "at_upper_circuit": bool(upper_circuit and close >= upper_circuit * 0.999),
                    "at_lower_circuit": bool(lower_circuit and close <= lower_circuit * 1.001),
                    "isin": str(_pick(raw_row, ["ISIN", "ISIN NUMBER"]) or "").strip(),
                    "as_of": day.isoformat(),
                    "source": "NSE Bhavcopy via nsefin",
                    "data_source": "nsefin",
                }
            )
        return {"as_of": day.isoformat(), "source": "NSE Bhavcopy via nsefin", "rows": rows}

    def get_ohlcv_for_symbol(self, symbol: str, days: int = 260) -> list[dict[str, Any]]:
        """Build historical bars from daily bhavcopies. Slow but official and cacheable."""
        clean = symbol.upper()
        end_date = date.today()
        start_date = end_date - timedelta(days=days + 90)
        bars: list[dict[str, Any]] = []
        current = end_date
        while current >= start_date and len(bars) < days:
            if current.weekday() < 5:
                try:
                    payload = self.bhavcopy_payload(current)
                    row = next((item for item in payload["rows"] if item["symbol"] == clean), None)
                    if row:
                        bars.append(
                            {
                                "datetime": row["as_of"],
                                "open": row["open"],
                                "high": row["high"],
                                "low": row["low"],
                                "close": row["close"],
                                "volume": row["volume"],
                                "source": "NSE Bhavcopy via nsefin",
                                "data_source": "nsefin",
                            }
                        )
                except Exception as exc:  # pragma: no cover - provider dependent
                    logger.debug("No nsefin bhavcopy for %s: %s", current, exc)
            current -= timedelta(days=1)
        return sorted(bars, key=lambda item: str(item["datetime"]))

    async def fetch_corporate_actions(self, symbol: str) -> list[dict[str, Any]]:
        clean = symbol.upper()
        if clean in self._corp_actions_cache:
            return self._corp_actions_cache[clean]
        try:
            import nsemine  # type: ignore
        except ImportError:
            return []

        def load() -> Any:
            for name in ("get_corporate_actions", "corporate_actions", "get_actions"):
                method = getattr(nsemine, name, None)
                if callable(method):
                    return method(clean)
            raise RuntimeError("nsemine does not expose a recognised corporate action method")

        try:
            raw_actions = await asyncio.to_thread(load)
            records = raw_actions.to_dict("records") if hasattr(raw_actions, "to_dict") else (raw_actions or [])
            parsed = [self._normalise_action(item) for item in records]
            actions = [item for item in parsed if item]
            self._corp_actions_cache[clean] = actions
            return actions
        except Exception as exc:  # pragma: no cover - provider dependent
            logger.warning("Corporate actions fetch failed for %s: %s", clean, exc)
            return []

    def _normalise_action(self, action: Any) -> dict[str, Any] | None:
        try:
            row = dict(action) if isinstance(action, dict) else getattr(action, "__dict__", {})
        except Exception:
            return None
        raw_type = str(_pick(row, ["purpose", "subject", "action_type", "type"]) or "").lower()
        ex_date = _pick(row, ["ex_date", "exDate", "exdate", "date"])
        ratio_num, ratio_den = self._parse_action_ratio(raw_type)
        if ratio_num == 1.0 and ratio_den == 1.0:
            return None
        action_type = "bonus" if "bonus" in raw_type else "split" if any(word in raw_type for word in ["split", "sub division", "subdivision"]) else "corporate_action"
        return {
            "ex_date": str(ex_date)[:10] if ex_date else None,
            "action_type": action_type,
            "ratio_numerator": ratio_num,
            "ratio_denominator": ratio_den,
            "source": "NSE Corporate Actions via nsemine",
            "raw_json": row,
        }

    def _parse_action_ratio(self, purpose: str) -> tuple[float, float]:
        split = re.search(r"from\s*rs\.?\s*(\d+(?:\.\d+)?).*?to\s*rs\.?\s*(\d+(?:\.\d+)?)", purpose)
        if split:
            return float(split.group(1)), float(split.group(2))
        ratio = re.search(r"(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", purpose)
        if ratio and "bonus" in purpose:
            bonus = float(ratio.group(1))
            existing = float(ratio.group(2))
            return existing + bonus, existing
        if ratio:
            return float(ratio.group(1)), float(ratio.group(2))
        return 1.0, 1.0

    async def fetch_fundamentals(self, symbol: str) -> dict[str, Any]:
        """Fetch free NSE fundamentals if niftyterminal is installed.

        This stays conservative because niftyterminal response shapes vary. Missing
        fields are better than invented fundamentals.
        """
        try:
            from niftyterminal import NSE  # type: ignore
        except ImportError:
            return {}

        try:
            async with NSE() as nse:
                financials = await nse.get_stock_financials(symbol, period="Annual")
                balance = await nse.get_stock_balance_sheet(symbol)
                cashflow = await nse.get_stock_cash_flow(symbol)
            return self._transform_fundamentals(financials, balance, cashflow)
        except Exception as exc:  # pragma: no cover - provider dependent
            logger.warning("Fundamentals fetch failed for %s: %s", symbol, exc)
            return {}

    def _transform_fundamentals(self, financials: Any, balance: Any, cashflow: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if pd is None:
            return result
        frames = {
            "financials": financials if hasattr(financials, "iloc") else pd.DataFrame(financials or []),
            "balance": balance if hasattr(balance, "iloc") else pd.DataFrame(balance or []),
            "cashflow": cashflow if hasattr(cashflow, "iloc") else pd.DataFrame(cashflow or []),
        }
        for key, df in frames.items():
            if hasattr(df, "columns"):
                df.columns = [str(col).lower().strip() for col in df.columns]
            frames[key] = df
        sales_cagr = self._cagr_from_frame(frames["financials"], ["sales", "revenue", "income from operations"])
        profit_cagr = self._cagr_from_frame(frames["financials"], ["pat", "net profit", "profit after tax"])
        if sales_cagr is not None:
            result["sales_cagr"] = sales_cagr
        if profit_cagr is not None:
            result["profit_cagr"] = profit_cagr
        pat = self._latest_from_frame(frames["financials"], ["pat", "net profit", "profit after tax"])
        cfo = self._latest_from_frame(frames["cashflow"], ["cash from operating", "operating cash", "cfo"])
        if pat and cfo is not None:
            result["cfo_pat"] = round(cfo / pat, 2) if pat else None
        total_debt = self._latest_from_frame(frames["balance"], ["borrowings", "debt"])
        equity = self._latest_from_frame(frames["balance"], ["equity", "net worth", "shareholder"])
        if total_debt is not None and equity:
            result["debt_equity"] = round(total_debt / equity, 2)
        result["source"] = "niftyterminal"
        result["fundamentals_as_of"] = datetime.now(timezone.utc).date().isoformat()
        return result

    def _matching_column(self, df: Any, keywords: list[str]) -> str | None:
        if not hasattr(df, "columns"):
            return None
        for column in df.columns:
            label = str(column).lower()
            if any(keyword in label for keyword in keywords):
                return column
        return None

    def _numeric_values(self, df: Any, keywords: list[str]) -> list[float]:
        column = self._matching_column(df, keywords)
        if column is None or not hasattr(df, "__getitem__"):
            return []
        values: list[float] = []
        for value in list(df[column]):
            clean = _clean_float(value)
            if clean is not None and clean != 0:
                values.append(clean)
        return values

    def _latest_from_frame(self, df: Any, keywords: list[str]) -> float | None:
        values = self._numeric_values(df, keywords)
        return values[-1] if values else None

    def _cagr_from_frame(self, df: Any, keywords: list[str]) -> float | None:
        values = self._numeric_values(df, keywords)
        if len(values) < 4:
            return None
        start, end = values[-4], values[-1]
        if start <= 0 or end <= 0:
            return None
        return round(((end / start) ** (1 / 3) - 1) * 100, 2)

    def get_live_quote(self, symbol: str) -> dict[str, Any]:
        clean = symbol.upper()
        methods = self._candidate_callables(["get_quote", "quote", "get_equity_quote", "equity_quote"])
        last_error: Exception | None = None
        for method in methods:
            try:
                raw = method(clean)
                if isinstance(raw, dict):
                    quote = raw.get("data") or raw.get("quote") or raw
                    return self._normalise_quote(clean, quote, "NSE Live via nsefin")
            except Exception as exc:  # pragma: no cover - provider dependent
                last_error = exc
        try:
            import nseazy  # type: ignore

            raw = nseazy.show_data(clean, {"LTP": True, "Info": True})
            if raw:
                return self._normalise_quote(clean, raw, "NSE Live via nseazy")
        except Exception as exc:  # pragma: no cover - provider dependent
            last_error = exc
        raise RuntimeError(f"NSE live quote unavailable for {clean}: {last_error}")

    def _normalise_quote(self, symbol: str, raw: dict[str, Any], source: str) -> dict[str, Any]:
        price = _clean_float(_pick(raw, ["lastPrice", "ltp", "LTP", "price", "last"]), 0) or 0
        if price <= 0:
            raise RuntimeError("quote has no LTP")
        previous = _clean_float(_pick(raw, ["previousClose", "prevClose", "close", "previous_close"]), price) or price
        return {
            "symbol": symbol,
            "price": round(price, 2),
            "change_pct": round(_clean_float(_pick(raw, ["pChange", "percentChange", "net_change"]), ((price - previous) / previous * 100 if previous else 0)) or 0, 2),
            "open": _clean_float(_pick(raw, ["open", "dayOpen"])),
            "high": _clean_float(_pick(raw, ["dayHigh", "high"])),
            "low": _clean_float(_pick(raw, ["dayLow", "low"])),
            "close": round(price, 2),
            "previous_close": round(previous, 2) if previous else None,
            "volume": _clean_int(_pick(raw, ["totalTradedVolume", "volume", "totalTradedQty"])),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": source,
        }

    def compute_nifty500_breadth(self, symbols: list[str] | None = None, days: int = 70) -> dict[str, Any]:
        """Compute above-50-DMA breadth from bhavcopy closes.

        If a true Nifty 500 constituent list is not supplied, use the most liquid EQ
        names in the latest bhavcopy. This remains a broader and more honest breadth
        proxy than Nifty 50 advance/decline alone.
        """
        latest = self.bhavcopy_payload()
        liquid_rows = [row for row in latest["rows"] if row.get("series") == "EQ" and row.get("close")]
        if symbols:
            universe = [sym.upper() for sym in symbols]
        else:
            liquid_rows.sort(key=lambda row: float(row.get("turnover") or 0), reverse=True)
            universe = [row["symbol"] for row in liquid_rows[:500]]
        history: dict[str, list[float]] = {sym: [] for sym in universe}
        current = date.fromisoformat(str(latest["as_of"])[:10])
        scanned = 0
        while scanned < days + 40 and any(len(values) < 50 for values in history.values()):
            if current.weekday() < 5:
                try:
                    payload = self.bhavcopy_payload(current)
                    by_symbol = {row["symbol"]: row for row in payload["rows"]}
                    for sym in universe:
                        row = by_symbol.get(sym)
                        if row and row.get("close") is not None and len(history[sym]) < 50:
                            history[sym].append(float(row["close"]))
                except Exception:
                    pass
            scanned += 1
            current -= timedelta(days=1)
        total = 0
        above = 0
        for sym, closes_desc in history.items():
            if len(closes_desc) < 50:
                continue
            closes = list(reversed(closes_desc[:50]))
            sma50 = sum(closes) / len(closes)
            total += 1
            if closes[-1] > sma50:
                above += 1
        if total == 0:
            raise RuntimeError("No symbols had enough bhavcopy history for breadth")
        breadth = round(above / total * 100, 2)
        return {
            "breadth_pct": breadth,
            "total_stocks": total,
            "above_50dma": above,
            "source": "Nifty 500/liquid-universe breadth via NSE bhavcopy",
            "as_of": latest["as_of"],
        }


nse_fetcher = NSEDataFetcher()
