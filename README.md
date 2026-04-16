# Indian Stock Decision Engine

Personal research app for Indian small and mid-cap screening. It is a rules-first decision engine, not a prediction app.

Core formula:

```text
Final Score = Business Quality + Sector Tailwind + Event Strength + Technical Strength + Market Support - Risk Penalties
```

## What is included

- Frontend dashboard: `frontend/index.html`
- FastAPI backend: `backend/app.py`
- Deterministic scoring engine: `backend/scoring_engine.py`
- Seed Indian equity universe: `backend/seed_data.py`
- Optional live connectors: `backend/data_sources.py`
- PostgreSQL-ready schema: `backend/schema.sql`
- Browser local-storage thesis tracker

## Run locally

Zero-install demo backend:

```powershell
cd "C:\Users\user\Documents\New project\stock-intelligence-platform\backend"
python local_api_server.py
```

FastAPI backend:

```powershell
cd "C:\Users\user\Documents\New project\stock-intelligence-platform\backend"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
uvicorn app:app --reload --host 127.0.0.1 --port 8000
```

Then open:

```text
C:\Users\user\Documents\New project\stock-intelligence-platform\frontend\index.html
```

Backend docs:

```text
http://127.0.0.1:8000/docs
```

## API keys

Copy `.env.example` to `.env` and add keys if you want optional live refresh:

```text
ALPHA_VANTAGE_API_KEY=
NEWSAPI_API_KEY=
```

The app works without keys using seeded data and browser demo mode. Live intraday Indian market data should eventually come from a licensed feed.

## Deploy Backend On Render

The repo includes `render.yaml`, so Render can create the backend service from a blueprint.

Recommended Render settings if you create the service manually:

```text
Service type: Web Service
Runtime: Python
Root Directory: backend
Build Command: pip install --upgrade pip && pip install -r requirements.txt
Start Command: uvicorn app:app --host 0.0.0.0 --port $PORT
Health Check Path: /api/health
```

The intended Render service URL is:

```text
https://indian-stock-decision-engine-api.onrender.com
```

If Render gives you a different URL, update:

```text
frontend/config.js
```

and set:

```javascript
window.STOCK_ENGINE_API_BASE = "https://your-render-service-url.onrender.com";
```

Optional Render environment variables:

```text
ALPHA_VANTAGE_API_KEY
NEWSAPI_API_KEY
```

The app works without those keys by using seeded research data.

## Deploy Frontend On Vercel

Use the same GitHub repo and set:

```text
Framework Preset: Other
Root Directory: frontend
Build Command: leave blank
Output Directory: .
Install Command: leave blank
```

For local file opening, the frontend uses `http://127.0.0.1:8000`.
On Vercel, it uses `window.STOCK_ENGINE_API_BASE` from `frontend/config.js`.

## Free and practical data stack

| Layer | Best free/practical option | Notes |
| --- | --- | --- |
| Price OHLCV | Yahoo chart endpoint / yfinance for personal prototyping | Free but unofficial and fragile; not suitable for production trading automation. |
| Daily data and technicals | Alpha Vantage | Free tier is very limited, useful for prototype checks. |
| News | GDELT + NewsAPI developer plan | GDELT is open; NewsAPI developer tier is for development/testing. |
| Official filings | NSE/BSE corporate filings pages | Best reliability layer; automate respectfully and cache aggressively. |
| Fundamentals | Company reports, exchange filings, CSV imports | Safest free route for personal use; paid feeds are better for scale. |

## Backend endpoints

- `GET /api/dashboard`
- `GET /api/stocks`
- `GET /api/stocks/{symbol}`
- `POST /api/refresh/{symbol}`
- `POST /api/score`
- `GET /api/apis`

## Scoring flow

1. Business quality: sales CAGR, profit CAGR, ROCE, ROE, debt, CFO/PAT, FCF trend, promoter/pledge/dilution, margins, valuation sanity.
2. Sector tailwind: demand, policy, cost environment, order visibility, sector momentum.
3. Event strength: sentiment x freshness x source reliability x importance.
4. Technical strength: 20 EMA, 50 EMA, 200 DMA, RSI, MACD, ATR, volume ratio, breakout, relative strength.
5. Market support: Nifty trend, breadth, sector strength, VIX.
6. Risk penalties: high debt, pledge, weak cash conversion, dilution, governance events, stretched price, overheated RSI, risk-off market, fake breakout flags.

## Important

This is a research workflow and journal. It is not investment advice, not a recommendation engine, and not a guarantee of returns.
