from __future__ import annotations

import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from scoring_engine import final_decision, market_support_score
from seed_data import build_demo_dataset


DATASET = build_demo_dataset()


def scored_universe() -> list[dict[str, Any]]:
    market = market_support_score(DATASET["market"])
    return [
        final_decision(company, DATASET["bars"][company["symbol"]], DATASET["benchmark_bars"], market)
        for company in DATASET["companies"]
    ]


def summary(stock: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": stock["symbol"],
        "name": stock["name"],
        "sector": stock["sector"],
        "industry": stock["industry"],
        "price": stock["price"],
        "change_pct": stock["change_pct"],
        "weekly_score": stock["weekly_score"],
        "monthly_score": stock["monthly_score"],
        "business_score": stock["business_quality"]["score"],
        "tailwind_score": stock["sector_tailwind"]["score"],
        "event_score": stock["event_strength"]["score"],
        "technical_score": stock["technical_strength"]["score"],
        "market_score": stock["market_support"]["score"],
        "risk_score": stock["risk_penalty"]["score"],
        "conviction": stock["conviction"],
        "candidate": stock["candidate"],
        "entry": stock["entry"],
        "risk_flags": stock["explanation_json"]["risk_flags"],
    }


def dashboard() -> dict[str, Any]:
    stocks = scored_universe()
    market = market_support_score(DATASET["market"])
    top_weekly = sorted(stocks, key=lambda stock: stock["weekly_score"], reverse=True)[:3]
    top_monthly = sorted(stocks, key=lambda stock: stock["monthly_score"], reverse=True)[:3]
    sectors: dict[str, list[dict[str, Any]]] = {}
    for stock in stocks:
        sectors.setdefault(stock["sector"], []).append(stock)
    top_sectors = []
    for sector, rows in sectors.items():
        top_sectors.append(
            {
                "sector": sector,
                "avg_weekly_score": round(sum(row["weekly_score"] for row in rows) / len(rows), 1),
                "avg_tailwind_score": round(sum(row["sector_tailwind"]["score"] for row in rows) / len(rows), 1),
                "leader": max(rows, key=lambda row: row["weekly_score"])["symbol"],
                "count": len(rows),
            }
        )
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
    avoid = [
        stock for stock in sorted(stocks, key=lambda row: row["risk_penalty"]["score"], reverse=True)
        if stock["risk_penalty"]["score"] >= 18 or stock["conviction"] == "Avoid"
    ][:5]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "market_regime": market,
        "top_weekly_candidates": [summary(stock) for stock in top_weekly],
        "top_monthly_candidates": [summary(stock) for stock in top_monthly],
        "top_sectors": sorted(top_sectors, key=lambda row: row["avg_weekly_score"], reverse=True),
        "avoid_list": [summary(stock) for stock in avoid],
        "latest_critical_events": sorted(events, key=lambda item: abs(float(item["net_score"])), reverse=True)[:12],
        "stocks": [summary(stock) for stock in sorted(stocks, key=lambda stock: stock["weekly_score"], reverse=True)],
        "disclaimer": "Research workflow only. This app does not provide investment advice or guaranteed predictions.",
    }


class Handler(BaseHTTPRequestHandler):
    def send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_json({"ok": True})

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in {"/", "/api/health"}:
            self.send_json({"status": "ok", "server": "stdlib", "docs": "Use FastAPI app.py for OpenAPI docs"})
            return
        if path == "/api/dashboard":
            self.send_json(dashboard())
            return
        if path == "/api/stocks":
            self.send_json({"stocks": [summary(stock) for stock in scored_universe()]})
            return
        if path.startswith("/api/stocks/"):
            symbol = path.rsplit("/", 1)[-1].upper()
            for stock in scored_universe():
                if stock["symbol"] == symbol:
                    self.send_json(stock)
                    return
            self.send_json({"detail": f"{symbol} not found"}, 404)
            return
        if path == "/api/apis":
            self.send_json(
                {
                    "recommended_stack": [
                        {"layer": "Price OHLCV", "primary": "Yahoo/yfinance prototype", "free_tier": "Unofficial personal use"},
                        {"layer": "News", "primary": "GDELT + NewsAPI", "free_tier": "Open/developer tiers"},
                        {"layer": "Filings", "primary": "NSE/BSE corporate filings", "free_tier": "Public pages"},
                    ]
                }
            )
            return
        self.send_json({"detail": "Not found"}, 404)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[local-api] {self.address_string()} - {fmt % args}")


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 8000), Handler)
    print("Local API server running at http://127.0.0.1:8000")
    print("Open frontend/index.html in your browser.")
    server.serve_forever()
