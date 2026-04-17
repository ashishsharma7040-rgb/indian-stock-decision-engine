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
- Shoonya live WebSocket bridge: `backend/live_feed.py`
- Optional Supabase alert function: `supabase/functions/daily-alert-scan/index.ts`
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

Live refresh works without broker keys by using:

```text
Yahoo Finance chart endpoint for 5-minute and daily bars
Yahoo Finance ^NSEI for Nifty regime
NSE public Nifty 50 advance/decline proxy
Yahoo Finance RSS
Google News RSS
GDELT DOC 2.0
```

Copy `.env.example` to `.env` only if you want optional fallback providers:

```text
ALPHA_VANTAGE_API_KEY=
NEWSAPI_API_KEY=
```

The app works without keys using Yahoo/no-key news plus seeded fallback data. For free real-time ticks, configure Shoonya on the backend only.

## Shoonya Real-Time Live Feed

Shoonya is optional. When configured, the FastAPI backend logs in to Shoonya and opens a live WebSocket. The browser never receives your Shoonya credentials; it connects only to your backend:

```text
WS /ws/live-prices
```

Safety mode:

```text
BROKER_MODE=market_data_only
ENABLE_TRADING=false
```

The live bridge disables common order methods and this app does not expose buy/sell/order endpoints.

Add these only in Render backend environment variables:

```text
LIVE_DATA_PROVIDER=shoonya
BROKER_MODE=market_data_only
ENABLE_TRADING=false
SHOONYA_USER_ID=your_client_id
SHOONYA_PASSWORD=your_password
SHOONYA_VENDOR_CODE=your_vendor_code
SHOONYA_API_KEY=your_api_key
SHOONYA_IMEI=your_imei_or_device_id
SHOONYA_TOTP_SECRET=your_totp_secret
```

Use `SHOONYA_TOTP_SECRET` for automatic reconnects. You can use `SHOONYA_TWOFA` instead for a short-lived manual OTP, but it will expire. Never put these values in GitHub, Vercel, frontend files, screenshots, or chat messages.

After Render redeploys, test:

```text
https://indian-stock-decision-engine-api.onrender.com/api/live/status
```

If `configured` is false, check the `missing_credentials` array in that response. It will list which Render variables are still missing without exposing your secret values.

Expected when live:

```text
configured: true
trading_enabled: false
status: live
feed_open: true
```

The frontend auto-connects and updates the universe stocks on the page without pressing Refresh.

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
LIVE_DATA_PROVIDER
BROKER_MODE
ENABLE_TRADING
SHOONYA_USER_ID
SHOONYA_PASSWORD
SHOONYA_VENDOR_CODE
SHOONYA_API_KEY
SHOONYA_IMEI
SHOONYA_TOTP_SECRET
SHOONYA_TWOFA
```

The app works without those keys by using Yahoo Finance chart data, no-key RSS/GDELT news, and seeded fallback data.

## Deploy Frontend On Vercel

Use the same GitHub repo and set:

```text
Framework Preset: Other
Root Directory: frontend
Build Command: leave blank
Output Directory: .
Install Command: leave blank
```

The frontend folder includes `frontend/vercel.json` for static serving.
The repo root also includes `index.html` and `vercel.json` as a fallback if Vercel is accidentally pointed at the repo root. The preferred setting is still:

```text
Root Directory: frontend
```

For local file opening, the frontend uses `http://127.0.0.1:8000`.
On Vercel, it uses `window.STOCK_ENGINE_API_BASE` from `frontend/config.js`.

## Free and practical data stack

| Layer | Best free/practical option | Notes |
| --- | --- | --- |
| Real-time price feed | Shoonya WebSocket | Free for account holders; backend-only credentials; market-data-only bridge. |
| Price OHLCV | Yahoo Finance chart endpoint | No key; useful for 5-minute and daily bars in a personal prototype. |
| Optional daily data fallback | Alpha Vantage | Only needed if you later get a key. |
| News | Yahoo Finance RSS + Google News RSS + GDELT | No key; useful for broad event discovery. |
| Official filings | NSE/BSE corporate filings pages | Best reliability layer; automate respectfully and cache aggressively. |
| Fundamentals | Company reports, exchange filings, CSV imports | Safest free route for personal use; paid feeds are better for scale. |

## Backend endpoints

- `GET /api/dashboard`
- `GET /api/stocks`
- `GET /api/stocks/{symbol}`
- `GET /api/live/status`
- `POST /api/live/subscribe`
- `GET /api/live/snapshot`
- `WS /ws/live-prices`
- `GET /api/market`
- `POST /api/market/refresh`
- `POST /api/scan/alerts`
- `GET /api/scheduled/daily`
- `GET /api/portfolio`
- `POST /api/portfolio`
- `POST /api/refresh/{symbol}`
- `POST /api/score`
- `GET /api/trade-state/{symbol}`
- `POST /api/trade-state/{symbol}`
- `POST /api/fundamentals/{symbol}`
- `POST /api/fundamentals/{symbol}/screener-csv`
- `POST /api/tailwind/{symbol}`
- `GET /api/backtest/demo`
- `POST /api/backtest`
- `GET /api/apis`

## Scoring flow

1. Business quality: sales CAGR, profit CAGR, ROCE, ROE, debt, CFO/PAT, FCF trend, promoter/pledge/dilution, margins, valuation sanity.
2. Sector tailwind: demand, policy, cost environment, order visibility, sector momentum.
3. Event strength: sentiment x freshness x source reliability x importance. Weekly scoring uses faster freshness decay; monthly scoring keeps relevant events alive longer. RSS and NewsAPI events use VADER sentiment when dependencies are installed, with filing boilerplate stripped before scoring.
4. Technical strength: weekly swing score uses daily bars; monthly positional score resamples daily data to weekly OHLCV and uses 10-week EMA, 20-week EMA, 40-week MA, weekly RSI, base quality, tight-pattern checks, and weekly breakout structure.
5. Market support: Nifty trend, NSE advance/decline breadth proxy, sector rotation proxies, VIX.
6. Risk penalties: high debt, pledge, weak cash conversion, dilution, governance events, stretched price, overheated RSI, risk-off market, fake breakout flags.

## Current Score Weights

Weekly swing score:

```text
Technical 28 + Business 20 + Market 20 + Tailwind 17 + Events 15 - Risk
```

Monthly positional score:

```text
Business 28 + Technical 20 + Tailwind 18 + Events 17 + Market 10 + Valuation 7 - Risk
```

Aggressive entry is hidden unless the full five-question gate passes. Pullback levels, stops, invalidation, and position sizing remain visible so a blocked setup can still be watched intelligently.

## Swing Trading Additions

- Weekly score and monthly score now use different technical timeframes.
- Raw and clamped scores are returned so hard-avoid names are not hidden as a simple zero.
- Entry output includes ATR-based position sizing using a default Rs 10,00,000 account and 1% risk per trade.
- Trade state is exposed as Screened, Watchlist, Triggered, In Trade, or Exited.
- `POST /api/scan/alerts` and `GET /api/scheduled/daily` detect Watchlist -> Triggered transitions for Telegram/email workflows.
- `POST /api/fundamentals/{symbol}` supports partial fundamental updates from refreshed Screener/filing data.
- `POST /api/fundamentals/{symbol}/screener-csv` accepts pasted/exported CSV text and maps common Screener-style labels into engine fields.
- `POST /api/tailwind/{symbol}` supports quarterly/manual sector tailwind refreshes.
- `GET /api/backtest/demo` and `POST /api/backtest` provide a validation harness for real historical bars. The demo backtest uses generated seed data only and must not be treated as evidence.
- `POST /api/market/refresh` refreshes Nifty 50 daily OHLCV, NSE advance/decline breadth, and sector rotation proxy scores.
- `POST /api/portfolio` lets the candidate gate block excess sector/industry concentration.
- Business scoring now returns a data-quality warning when important fields are missing.
- Thesis text includes specific business, sector, event, entry, and risk context instead of only repeating scores.
- The frontend stock list now shows conviction, trade state, weekly/monthly scores, sector, and risk at a glance.
- The chart plots candlesticks, 20 EMA, 200 DMA, colored volume, RS vs Nifty, 52-week high, breakout, and stop lines.

## Alert Workflow

The backend has an alert-ready daily scan:

```text
GET /api/scheduled/daily
```

It refreshes market regime, rescans watchlist state transitions, and returns alerts when a stock moves from Watchlist to Triggered.

Optional Supabase Edge Function:

```text
supabase/functions/daily-alert-scan/index.ts
```

Set these Supabase secrets:

```text
STOCK_ENGINE_BACKEND_URL=https://indian-stock-decision-engine-api.onrender.com
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

Schedule it after market close. It calls the backend and sends Telegram messages only when there are triggered watchlist alerts.

## Important

This is a research workflow and journal. It is not investment advice, not a recommendation engine, and not a guarantee of returns.
