from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any


def generate_bars(
    symbol: str,
    last_close: float,
    annual_trend: float,
    volatility: float,
    days: int = 260,
    breakout: bool = False,
    volume_ratio: float = 1.2,
) -> list[dict[str, Any]]:
    rng = random.Random(symbol)
    close = 100.0
    raw: list[dict[str, Any]] = []
    start = date.today() - timedelta(days=days)
    daily_trend = annual_trend / 252
    daily_noise = volatility / 100
    volume_base = rng.randint(650_000, 4_500_000)
    for idx in range(days):
        close *= 1 + daily_trend + rng.uniform(-daily_noise, daily_noise)
        high = close * (1 + rng.uniform(0.001, 0.025))
        low = close * (1 - rng.uniform(0.001, 0.025))
        open_price = close * (1 + rng.uniform(-0.012, 0.012))
        volume = int(volume_base * (1 + rng.uniform(-0.35, 0.55)))
        raw.append(
            {
                "datetime": str(start + timedelta(days=idx)),
                "open": round(open_price, 2),
                "high": round(max(high, open_price, close), 2),
                "low": round(min(low, open_price, close), 2),
                "close": round(close, 2),
                "volume": max(volume, 50_000),
            }
        )
    scale = last_close / float(raw[-1]["close"])
    for bar in raw:
        for key in ("open", "high", "low", "close"):
            bar[key] = round(float(bar[key]) * scale, 2)
    if breakout:
        for bar in raw[-56:-1]:
            bar["high"] = round(min(float(bar["high"]), last_close * 0.985), 2)
            bar["close"] = round(min(float(bar["close"]), last_close * 0.975), 2)
        raw[-1]["open"] = round(last_close * 0.992, 2)
        raw[-1]["high"] = round(last_close * 1.014, 2)
        raw[-1]["low"] = round(last_close * 0.982, 2)
        raw[-1]["close"] = round(last_close, 2)
    raw[-1]["volume"] = int(sum(float(bar["volume"]) for bar in raw[-21:-1]) / 20 * volume_ratio)
    return raw


UNIVERSE: list[dict[str, Any]] = [
    {
        "symbol": "POLYCAB",
        "name": "Polycab India",
        "sector": "Industrials",
        "industry": "Cables and Wires",
        "market_cap_cr": 103000,
        "last_close": 6850,
        "annual_trend": 0.34,
        "volatility": 1.25,
        "breakout": True,
        "volume_ratio": 2.2,
        "fundamentals": {
            "sales_cagr": 22.4,
            "profit_cagr": 28.1,
            "roce": 28.7,
            "roe": 23.4,
            "debt_equity": 0.08,
            "cfo_pat": 0.91,
            "fcf_trend": "positive",
            "promoter_holding_trend": "stable",
            "pledge_percent": 0.0,
            "dilution_flag": False,
            "margin_trend_bps": 190,
            "pe": 47,
        },
        "tailwind": {
            "demand_trend": 88,
            "policy_support": 82,
            "cost_environment": 66,
            "order_visibility": 90,
            "sector_momentum": 86,
        },
        "tailwind_factors": [
            "Transmission and distribution capex is rising",
            "Housing wiring demand remains healthy",
            "Data centre and EV charging cable demand is structural",
            "Copper cost spikes are the main margin watch item",
        ],
        "events": [
            {
                "title": "Large power transmission order announced",
                "source": "BSE filing",
                "source_type": "exchange_filing",
                "sentiment": 0.88,
                "importance": 82,
                "days_old": 2,
                "category": "order_win",
            },
            {
                "title": "Export volumes accelerated in recent update",
                "source": "Company investor presentation",
                "source_type": "company_ir",
                "sentiment": 0.62,
                "importance": 60,
                "days_old": 9,
                "category": "demand",
            },
        ],
    },
    {
        "symbol": "TATAPOWER",
        "name": "Tata Power",
        "sector": "Power",
        "industry": "Utilities and Renewables",
        "market_cap_cr": 131000,
        "last_close": 412,
        "annual_trend": 0.28,
        "volatility": 1.45,
        "breakout": True,
        "volume_ratio": 1.8,
        "fundamentals": {
            "sales_cagr": 18.4,
            "profit_cagr": 31.2,
            "roce": 10.8,
            "roe": 12.2,
            "debt_equity": 1.2,
            "cfo_pat": 0.78,
            "fcf_trend": "improving",
            "promoter_holding_trend": "stable",
            "pledge_percent": 0.0,
            "dilution_flag": False,
            "margin_trend_bps": 80,
            "pe": 36,
        },
        "tailwind": {
            "demand_trend": 86,
            "policy_support": 90,
            "cost_environment": 62,
            "order_visibility": 78,
            "sector_momentum": 88,
        },
        "tailwind_factors": [
            "Power demand growth remains a strong macro tailwind",
            "Renewable capacity addition keeps visibility high",
            "Distribution reforms and smart metering can add triggers",
            "Debt and regulated return caps need monitoring",
        ],
        "events": [
            {
                "title": "Solar project commissioned ahead of schedule",
                "source": "Exchange disclosure",
                "source_type": "exchange_filing",
                "sentiment": 0.72,
                "importance": 70,
                "days_old": 4,
                "category": "execution",
            }
        ],
    },
    {
        "symbol": "SUZLON",
        "name": "Suzlon Energy",
        "sector": "Renewable Energy",
        "industry": "Wind Equipment",
        "market_cap_cr": 83500,
        "last_close": 61.4,
        "annual_trend": 0.52,
        "volatility": 2.35,
        "breakout": True,
        "volume_ratio": 2.1,
        "fundamentals": {
            "sales_cagr": 24.1,
            "profit_cagr": 80.0,
            "roce": 18.7,
            "roe": 21.5,
            "debt_equity": 0.12,
            "cfo_pat": 0.88,
            "fcf_trend": "improving",
            "promoter_holding_trend": "stable",
            "pledge_percent": 8.2,
            "dilution_flag": False,
            "margin_trend_bps": 260,
            "pe": 55,
        },
        "tailwind": {
            "demand_trend": 90,
            "policy_support": 92,
            "cost_environment": 58,
            "order_visibility": 86,
            "sector_momentum": 84,
        },
        "tailwind_factors": [
            "Renewable capacity target supports order inflow",
            "Corporate green power purchase agreements are rising",
            "Execution discipline matters after balance sheet repair",
            "Pledge level needs continuous monitoring",
        ],
        "events": [
            {
                "title": "New wind turbine order added to order book",
                "source": "NSE filing",
                "source_type": "exchange_filing",
                "sentiment": 0.86,
                "importance": 78,
                "days_old": 3,
                "category": "order_win",
            },
            {
                "title": "Promoter pledge still visible in shareholding data",
                "source": "Exchange shareholding data",
                "source_type": "exchange_filing",
                "sentiment": -0.38,
                "importance": 62,
                "days_old": 15,
                "category": "pledge",
            },
        ],
    },
    {
        "symbol": "DIXON",
        "name": "Dixon Technologies",
        "sector": "Manufacturing",
        "industry": "Electronics Manufacturing",
        "market_cap_cr": 88500,
        "last_close": 14800,
        "annual_trend": 0.38,
        "volatility": 1.65,
        "breakout": False,
        "volume_ratio": 1.35,
        "fundamentals": {
            "sales_cagr": 71.0,
            "profit_cagr": 82.0,
            "roce": 22.4,
            "roe": 21.1,
            "debt_equity": 0.18,
            "cfo_pat": 0.74,
            "fcf_trend": "volatile",
            "promoter_holding_trend": "stable",
            "pledge_percent": 0.0,
            "dilution_flag": False,
            "margin_trend_bps": 110,
            "pe": 98,
        },
        "tailwind": {
            "demand_trend": 86,
            "policy_support": 90,
            "cost_environment": 58,
            "order_visibility": 82,
            "sector_momentum": 79,
        },
        "tailwind_factors": [
            "PLI and China plus one tailwind remain powerful",
            "Customer concentration and valuation are the key risks",
            "Margin expansion depends on scale and component costs",
        ],
        "events": [
            {
                "title": "New electronics assembly mandate discussed by management",
                "source": "Earnings transcript",
                "source_type": "earnings_transcript",
                "sentiment": 0.58,
                "importance": 66,
                "days_old": 6,
                "category": "order_visibility",
            }
        ],
    },
    {
        "symbol": "KPITTECH",
        "name": "KPIT Technologies",
        "sector": "Technology",
        "industry": "Auto Software",
        "market_cap_cr": 45500,
        "last_close": 1680,
        "annual_trend": 0.32,
        "volatility": 1.55,
        "breakout": False,
        "volume_ratio": 1.3,
        "fundamentals": {
            "sales_cagr": 38.0,
            "profit_cagr": 44.0,
            "roce": 32.1,
            "roe": 27.9,
            "debt_equity": 0.01,
            "cfo_pat": 0.88,
            "fcf_trend": "positive",
            "promoter_holding_trend": "stable",
            "pledge_percent": 0.0,
            "dilution_flag": False,
            "margin_trend_bps": 180,
            "pe": 61,
        },
        "tailwind": {
            "demand_trend": 82,
            "policy_support": 72,
            "cost_environment": 70,
            "order_visibility": 80,
            "sector_momentum": 76,
        },
        "tailwind_factors": [
            "Software-defined vehicle transition is a durable theme",
            "Large auto OEM deal ramp-up is the main trigger to track",
            "Global auto capex moderation can delay growth",
        ],
        "events": [
            {
                "title": "Strategic auto OEM contract extension announced",
                "source": "Exchange filing",
                "source_type": "exchange_filing",
                "sentiment": 0.7,
                "importance": 72,
                "days_old": 5,
                "category": "contract",
            }
        ],
    },
    {
        "symbol": "HDFCBANK",
        "name": "HDFC Bank",
        "sector": "Financials",
        "industry": "Private Bank",
        "market_cap_cr": 1300000,
        "last_close": 1720,
        "annual_trend": 0.10,
        "volatility": 0.95,
        "breakout": False,
        "volume_ratio": 1.05,
        "fundamentals": {
            "sales_cagr": 16.0,
            "profit_cagr": 22.0,
            "roce": 17.2,
            "roe": 16.4,
            "debt_equity": 0.0,
            "cfo_pat": 0.95,
            "fcf_trend": "positive",
            "promoter_holding_trend": "stable",
            "pledge_percent": 0.0,
            "dilution_flag": False,
            "margin_trend_bps": -40,
            "pe": 19,
        },
        "tailwind": {
            "demand_trend": 68,
            "policy_support": 62,
            "cost_environment": 58,
            "order_visibility": 70,
            "sector_momentum": 57,
        },
        "tailwind_factors": [
            "Credit growth is supportive",
            "Deposit growth and cost of funds remain the watch items",
            "Large cap bank chart needs stronger relative strength",
        ],
        "events": [
            {
                "title": "Management said merger integration remains on track",
                "source": "Earnings transcript",
                "source_type": "earnings_transcript",
                "sentiment": 0.2,
                "importance": 52,
                "days_old": 20,
                "category": "management_commentary",
            },
            {
                "title": "Deposit growth lagged loan growth in sector data",
                "source": "Credible news",
                "source_type": "credible_news",
                "sentiment": -0.42,
                "importance": 58,
                "days_old": 7,
                "category": "macro",
            },
        ],
    },
    {
        "symbol": "LTIM",
        "name": "LTIMindtree",
        "sector": "Technology",
        "industry": "IT Services",
        "market_cap_cr": 160000,
        "last_close": 5420,
        "annual_trend": 0.04,
        "volatility": 1.05,
        "breakout": False,
        "volume_ratio": 0.9,
        "fundamentals": {
            "sales_cagr": 7.1,
            "profit_cagr": 9.4,
            "roce": 28.1,
            "roe": 23.6,
            "debt_equity": 0.02,
            "cfo_pat": 0.96,
            "fcf_trend": "positive",
            "promoter_holding_trend": "stable",
            "pledge_percent": 0.0,
            "dilution_flag": False,
            "margin_trend_bps": -130,
            "pe": 32,
        },
        "tailwind": {
            "demand_trend": 48,
            "policy_support": 45,
            "cost_environment": 60,
            "order_visibility": 52,
            "sector_momentum": 50,
        },
        "tailwind_factors": [
            "IT spending recovery is still selective",
            "Large deal conversion needs improvement",
            "Margins are stable but growth trigger is not strong yet",
        ],
        "events": [
            {
                "title": "Cautious demand commentary after recent results",
                "source": "Earnings transcript",
                "source_type": "earnings_transcript",
                "sentiment": -0.48,
                "importance": 68,
                "days_old": 12,
                "category": "guidance",
            }
        ],
    },
    {
        "symbol": "RELIANCE",
        "name": "Reliance Industries",
        "sector": "Diversified",
        "industry": "Energy, Retail and Telecom",
        "market_cap_cr": 1920000,
        "last_close": 2847,
        "annual_trend": 0.16,
        "volatility": 1.05,
        "breakout": False,
        "volume_ratio": 1.45,
        "fundamentals": {
            "sales_cagr": 8.2,
            "profit_cagr": 11.4,
            "roce": 13.2,
            "roe": 9.6,
            "debt_equity": 0.44,
            "cfo_pat": 0.92,
            "fcf_trend": "improving",
            "promoter_holding_trend": "stable",
            "pledge_percent": 0.0,
            "dilution_flag": False,
            "margin_trend_bps": 70,
            "pe": 28,
        },
        "tailwind": {
            "demand_trend": 70,
            "policy_support": 68,
            "cost_environment": 52,
            "order_visibility": 74,
            "sector_momentum": 65,
        },
        "tailwind_factors": [
            "Telecom ARPU and retail scale are the clean triggers",
            "Oil-to-chemicals spreads can dilute near-term momentum",
            "Green energy capex is a longer-cycle optionality",
        ],
        "events": [
            {
                "title": "Retail and telecom margin commentary improved",
                "source": "Company investor presentation",
                "source_type": "company_ir",
                "sentiment": 0.45,
                "importance": 62,
                "days_old": 5,
                "category": "earnings",
            },
            {
                "title": "Crude volatility remains a segment headwind",
                "source": "Credible news",
                "source_type": "credible_news",
                "sentiment": -0.18,
                "importance": 48,
                "days_old": 1,
                "category": "commodity",
            },
        ],
    },
]

WATCH_SYMBOLS = [
    "POLYCAB",
    "TATAPOWER",
    "SUZLON",
    "DIXON",
    "KPITTECH",
    "HDFCBANK",
    "RELIANCE",
]


def build_demo_dataset() -> dict[str, Any]:
    benchmark_bars = generate_bars("^NSEI", 22680, 0.13, 0.8, breakout=False, volume_ratio=1.0)
    market = {
        "nifty_bars": benchmark_bars,
        "breadth_above_50dma": 61,
        "sector_strength": 67,
        "vix": 13.6,
    }
    companies = []
    bars_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for item in UNIVERSE:
        bars = generate_bars(
            item["symbol"],
            item["last_close"],
            item["annual_trend"],
            item["volatility"],
            breakout=item["breakout"],
            volume_ratio=item["volume_ratio"],
        )
        clean_item = {k: v for k, v in item.items() if k not in {"annual_trend", "volatility", "breakout", "volume_ratio"}}
        companies.append(clean_item)
        bars_by_symbol[item["symbol"]] = bars
    return {"market": market, "companies": companies, "bars": bars_by_symbol, "benchmark_bars": benchmark_bars}
