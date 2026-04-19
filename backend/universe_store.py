from __future__ import annotations

import json
import os
from pathlib import Path
import threading
from datetime import datetime, timezone
from typing import Any


MIN_INVESTABLE_MARKET_CAP_CR = float(os.getenv("MIN_INVESTABLE_MARKET_CAP_CR", "500"))
MIN_INVESTABLE_TURNOVER = float(os.getenv("MIN_INVESTABLE_TURNOVER_RS", "100000000"))
MIN_DELIVERY_PCT = float(os.getenv("MIN_DELIVERY_PCT", "40"))
MIN_VOLUME_CONSISTENCY_RATIO = float(os.getenv("MIN_VOLUME_CONSISTENCY_RATIO", "0.8"))


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
        self._cache_path = Path(os.getenv("UNIVERSE_CACHE_PATH", Path(__file__).resolve().parent / ".cache" / "nse_universe.json"))
        self._load_persisted()

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
            self._persist_locked()
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
                        "delivery_qty": item.get("delivery_qty"),
                        "delivery_pct": item.get("delivery_pct"),
                        "avg_volume_50d": item.get("avg_volume_50d") or row.get("avg_volume_50d"),
                        "upper_circuit": item.get("upper_circuit"),
                        "lower_circuit": item.get("lower_circuit"),
                        "circuit_limit_pct": item.get("circuit_limit_pct"),
                        "at_upper_circuit": item.get("at_upper_circuit"),
                        "at_lower_circuit": item.get("at_lower_circuit"),
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
            self._persist_locked()
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
        rows = [row for row in rows if self._is_investable(row)]
        rows.sort(key=lambda row: float(row.get("turnover") or row.get("volume") or 0), reverse=True)
        return [self._public_row(row) for row in rows[:limit]]

    def scan_candidates(self, limit: int = 100, strict: bool = True) -> tuple[list[dict[str, Any]], str]:
        """Return rows for the full scanner.

        Strict mode uses the institutional liquidity gate. If public NSE data is
        missing turnover/delivery fields and the strict set becomes empty, fall
        back to priced EQ rows so the scan can still produce a research ranking
        instead of an empty dashboard. The fallback is labeled for the UI/meta.
        """
        with self._lock:
            rows = list(self._rows.values())
        if strict:
            strict_rows = [row for row in rows if self._is_investable(row)]
            if strict_rows:
                strict_rows.sort(key=lambda row: float(row.get("turnover") or row.get("volume") or 0), reverse=True)
                return [self._public_row(row) for row in strict_rows[:limit]], "strict_investable"
        relaxed_rows = [
            row
            for row in rows
            if str(row.get("series") or "EQ").upper() == "EQ"
            and row.get("price") is not None
            and (row.get("as_of") or row.get("turnover") or row.get("volume"))
        ]
        relaxed_rows.sort(key=lambda row: float(row.get("turnover") or row.get("volume") or 0), reverse=True)
        return [self._public_row(row) for row in relaxed_rows[:limit]], "relaxed_priced_eq"

    def _refresh_meta_locked(self) -> None:
        self._meta["total"] = len(self._rows)
        self._meta["priced"] = sum(1 for row in self._rows.values() if row.get("price") is not None)
        self._meta["investable"] = sum(1 for row in self._rows.values() if self._is_investable(row))

    def _load_persisted(self) -> None:
        try:
            if not self._cache_path.exists():
                return
            payload = json.loads(self._cache_path.read_text(encoding="utf-8"))
            self._rows = {str(row.get("symbol", "")).upper(): row for row in payload.get("rows", []) if row.get("symbol")}
            self._meta.update(payload.get("meta", {}))
            self._meta["source"] = f"{self._meta.get('source', 'NSE universe')} + local cache"
            self._refresh_meta_locked()
        except Exception as exc:
            self._meta["last_error"] = f"Universe cache load failed: {exc}"

    def _persist_locked(self) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"meta": self._meta, "rows": list(self._rows.values()), "saved_at": datetime.now(timezone.utc).isoformat()}
            self._cache_path.write_text(json.dumps(payload, separators=(",", ":"), default=str), encoding="utf-8")
        except Exception as exc:
            self._meta["last_error"] = f"Universe cache persist failed: {exc}"

    @staticmethod
    def _is_investable(row: dict[str, Any]) -> bool:
        series = str(row.get("series") or "EQ").upper()
        if series != "EQ":
            return False
        turnover = float(row.get("turnover") or 0)
        if turnover < MIN_INVESTABLE_TURNOVER:
            return False
        delivery_pct = row.get("delivery_pct")
        if delivery_pct is not None and float(delivery_pct or 0) < MIN_DELIVERY_PCT:
            return False
        avg_volume_50d = float(row.get("avg_volume_50d") or 0)
        current_volume = float(row.get("volume") or 0)
        if avg_volume_50d > 0 and current_volume < avg_volume_50d * MIN_VOLUME_CONSISTENCY_RATIO:
            return False
        market_cap = row.get("market_cap_cr")
        if market_cap is not None and float(market_cap or 0) < MIN_INVESTABLE_MARKET_CAP_CR:
            return False
        circuit = row.get("circuit_limit_pct")
        if circuit is not None and float(circuit or 0) <= 5:
            return False
        if row.get("at_upper_circuit") or row.get("at_lower_circuit"):
            return False
        return row.get("price") is not None

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
            "delivery_qty": row.get("delivery_qty"),
            "delivery_pct": row.get("delivery_pct"),
            "avg_volume_50d": row.get("avg_volume_50d"),
            "upper_circuit": row.get("upper_circuit"),
            "lower_circuit": row.get("lower_circuit"),
            "circuit_limit_pct": row.get("circuit_limit_pct"),
            "at_upper_circuit": row.get("at_upper_circuit"),
            "at_lower_circuit": row.get("at_lower_circuit"),
            "as_of": row.get("as_of"),
            "isin": row.get("isin"),
            "research_covered": bool(row.get("research_covered")),
            "investable": NSEUniverseStore._is_investable(row),
            "investable_filter": {
                "series_eq": str(row.get("series") or "EQ").upper() == "EQ",
                "min_turnover_rs": MIN_INVESTABLE_TURNOVER,
                "turnover_pass": float(row.get("turnover") or 0) >= MIN_INVESTABLE_TURNOVER,
                "min_delivery_pct": MIN_DELIVERY_PCT,
                "delivery_pct": row.get("delivery_pct"),
                "delivery_pass": row.get("delivery_pct") is None or float(row.get("delivery_pct") or 0) >= MIN_DELIVERY_PCT,
                "min_volume_consistency_ratio": MIN_VOLUME_CONSISTENCY_RATIO,
                "avg_volume_50d": row.get("avg_volume_50d"),
                "volume_consistency_pass": not row.get("avg_volume_50d")
                or float(row.get("volume") or 0) >= float(row.get("avg_volume_50d") or 0) * MIN_VOLUME_CONSISTENCY_RATIO,
                "min_market_cap_cr": MIN_INVESTABLE_MARKET_CAP_CR,
                "market_cap_pass": row.get("market_cap_cr") is None or float(row.get("market_cap_cr") or 0) >= MIN_INVESTABLE_MARKET_CAP_CR,
                "circuit_pass": not (
                    (row.get("circuit_limit_pct") is not None and float(row.get("circuit_limit_pct") or 0) <= 5)
                    or row.get("at_upper_circuit")
                    or row.get("at_lower_circuit")
                ),
            },
            "source": row.get("source") or "NSE universe",
            "data_mode": "research_scored" if row.get("research_covered") else "nse_eod_search",
        }


UNIVERSE_STORE = NSEUniverseStore()
