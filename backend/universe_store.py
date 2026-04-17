from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any


class NSEUniverseStore:
    """Small in-memory NSE universe cache.

    It keeps the frontend searchable without asking Yahoo or Shoonya for thousands
    of symbols. Render may restart, so the backend refreshes this cache on startup
    and on demand through /api/universe/refresh.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rows: dict[str, dict[str, Any]] = {}
        self._meta: dict[str, Any] = {
            "total": 0,
            "priced": 0,
            "equity_master_loaded_at": None,
            "bhavcopy_loaded_at": None,
            "bhavcopy_as_of": None,
            "source": "seed_research",
            "last_error": None,
        }

    def load_seed_companies(self, companies: list[dict[str, Any]]) -> None:
        with self._lock:
            for company in companies:
                symbol = str(company.get("symbol", "")).upper()
                if not symbol:
                    continue
                row = self._rows.setdefault(symbol, {"symbol": symbol})
                row.update(
                    {
                        "symbol": symbol,
                        "name": company.get("name", symbol),
                        "sector": company.get("sector", "Researched"),
                        "industry": company.get("industry", "Researched universe"),
                        "market_cap_cr": company.get("market_cap_cr"),
                        "research_covered": True,
                        "source": "seed_research",
                    }
                )
            self._refresh_meta_locked()

    def refresh_equity_master(self, rows: list[dict[str, Any]]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            for item in rows:
                symbol = str(item.get("symbol", "")).upper()
                if not symbol:
                    continue
                row = self._rows.setdefault(symbol, {"symbol": symbol})
                row.update(
                    {
                        "symbol": symbol,
                        "name": item.get("name") or row.get("name") or symbol,
                        "series": item.get("series") or row.get("series") or "EQ",
                        "isin": item.get("isin") or row.get("isin"),
                        "date_of_listing": item.get("date_of_listing") or row.get("date_of_listing"),
                        "face_value": item.get("face_value") if item.get("face_value") is not None else row.get("face_value"),
                        "source": "NSE equity master",
                    }
                )
                row.setdefault("sector", "Unclassified")
                row.setdefault("industry", "NSE Equity")
            self._meta["equity_master_loaded_at"] = now
            self._meta["last_error"] = None
            self._refresh_meta_locked()
            return len(rows)

    def refresh_bhavcopy(self, payload: dict[str, Any]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        count = 0
        with self._lock:
            for item in payload.get("rows", []):
                symbol = str(item.get("symbol", "")).upper()
                if not symbol:
                    continue
                row = self._rows.setdefault(symbol, {"symbol": symbol})
                row.update(
                    {
                        "symbol": symbol,
                        "name": row.get("name") or symbol,
                        "series": item.get("series") or row.get("series") or "EQ",
                        "price": item.get("close"),
                        "open": item.get("open"),
                        "high": item.get("high"),
                        "low": item.get("low"),
                        "previous_close": item.get("previous_close"),
                        "change_pct": item.get("change_pct"),
                        "volume": item.get("volume"),
                        "turnover": item.get("turnover"),
                        "as_of": item.get("as_of") or payload.get("as_of"),
                        "isin": item.get("isin") or row.get("isin"),
                        "source": "NSE bhavcopy EOD",
                    }
                )
                row.setdefault("sector", "Unclassified")
                row.setdefault("industry", "NSE Equity")
                count += 1
            self._meta["bhavcopy_loaded_at"] = now
            self._meta["bhavcopy_as_of"] = payload.get("as_of")
            self._meta["bhavcopy_source"] = payload.get("source")
            self._meta["source"] = "NSE equity master + NSE bhavcopy EOD"
            self._meta["last_error"] = None
            self._refresh_meta_locked()
        return count

    def set_error(self, error: str) -> None:
        with self._lock:
            self._meta["last_error"] = error

    def meta(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_meta_locked()
            return dict(self._meta)

    def count(self) -> int:
        with self._lock:
            return len(self._rows)

    def priced_count(self) -> int:
        with self._lock:
            return sum(1 for row in self._rows.values() if row.get("price") is not None)

    def get(self, symbol: str) -> dict[str, Any] | None:
        symbol = symbol.upper()
        with self._lock:
            row = self._rows.get(symbol)
            return dict(row) if row else None

    def search(self, query: str = "", limit: int = 40) -> list[dict[str, Any]]:
        query = (query or "").strip().lower()
        with self._lock:
            rows = list(self._rows.values())
        if query:
            terms = [term for term in query.split() if term]

            def matches(row: dict[str, Any]) -> bool:
                haystack = f"{row.get('symbol','')} {row.get('name','')} {row.get('sector','')} {row.get('industry','')}".lower()
                return all(term in haystack for term in terms)

            rows = [row for row in rows if matches(row)]

        def rank(row: dict[str, Any]) -> tuple[int, int, float, str]:
            symbol = str(row.get("symbol", "")).lower()
            name = str(row.get("name", "")).lower()
            if query and symbol == query:
                match_rank = 0
            elif query and symbol.startswith(query):
                match_rank = 1
            elif query and name.startswith(query):
                match_rank = 2
            elif query:
                match_rank = 3
            else:
                match_rank = 4
            research_bonus = 0 if row.get("research_covered") else 1
            liquidity = float(row.get("turnover") or row.get("volume") or 0)
            return (match_rank, research_bonus, -liquidity, symbol)

        return [self._public_row(row) for row in sorted(rows, key=rank)[: max(1, min(limit, 200))]]

    def top_liquid(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._rows.values())
        rows = [row for row in rows if row.get("price") is not None]
        rows.sort(key=lambda row: float(row.get("turnover") or row.get("volume") or 0), reverse=True)
        return [self._public_row(row) for row in rows[:limit]]

    def _refresh_meta_locked(self) -> None:
        self._meta["total"] = len(self._rows)
        self._meta["priced"] = sum(1 for row in self._rows.values() if row.get("price") is not None)

    @staticmethod
    def _public_row(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": row.get("symbol"),
            "name": row.get("name") or row.get("symbol"),
            "sector": row.get("sector") or "Unclassified",
            "industry": row.get("industry") or "NSE Equity",
            "series": row.get("series"),
            "price": row.get("price"),
            "change_pct": row.get("change_pct"),
            "volume": row.get("volume"),
            "turnover": row.get("turnover"),
            "as_of": row.get("as_of"),
            "isin": row.get("isin"),
            "research_covered": bool(row.get("research_covered")),
            "source": row.get("source") or "NSE universe",
            "data_mode": "research_scored" if row.get("research_covered") else "nse_eod_search",
        }


UNIVERSE_STORE = NSEUniverseStore()
