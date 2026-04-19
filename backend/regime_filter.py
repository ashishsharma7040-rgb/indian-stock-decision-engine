from __future__ import annotations

from typing import Any


class RegimeFilter:
    """Daily market-breadth master switch used by the overnight batch."""

    GREEN_THRESHOLD = 50.0
    RED_THRESHOLD = 30.0

    def calculate_daily_regime(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        measured = [
            row
            for row in rows
            if row.get("close") is not None and (row.get("sma_50") is not None or row.get("dma50") is not None)
        ]
        if not measured:
            return self.from_breadth_pct(None, measured_count=0, source="missing_nifty500_breadth")
        above = sum(1 for row in measured if float(row["close"]) > float(row.get("sma_50") or row.get("dma50")))
        breadth = above / len(measured) * 100
        return self.from_breadth_pct(breadth, measured_count=len(measured), above_count=above, source="nifty500_50dma")

    def from_breadth_pct(
        self,
        breadth_pct: float | None,
        measured_count: int | None = None,
        above_count: int | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        if breadth_pct is None:
            return {
                "regime_status": "YELLOW",
                "breadth_pct": None,
                "breadth_percentage": None,
                "position_multiplier": 0.5,
                "can_buy": True,
                "description": "Breadth unavailable. Engine falls back to cautious half sizing.",
                "total_stocks_measured": measured_count or 0,
                "stocks_above_50dma": above_count,
                "source": source or "unknown",
            }
        if breadth_pct >= self.GREEN_THRESHOLD:
            regime = "GREEN"
            multiplier = 1.0
            can_buy = True
            description = "Market breadth is healthy. Full position sizing allowed."
        elif breadth_pct < self.RED_THRESHOLD:
            regime = "RED"
            multiplier = 0.0
            can_buy = False
            description = "Systemic weakness detected. New entries are locked; manage existing risk only."
        else:
            regime = "YELLOW"
            multiplier = 0.5
            can_buy = True
            description = "Market breadth is deteriorating. New entries allowed at half size."
        return {
            "regime_status": regime,
            "breadth_pct": round(float(breadth_pct), 2),
            "breadth_percentage": round(float(breadth_pct), 2),
            "position_multiplier": multiplier,
            "can_buy": can_buy,
            "description": description,
            "total_stocks_measured": measured_count,
            "stocks_above_50dma": above_count,
            "source": source or "breadth_above_50dma",
        }


def regime_from_breadth(breadth_pct: float | None, **kwargs: Any) -> dict[str, Any]:
    return RegimeFilter().from_breadth_pct(breadth_pct, **kwargs)
