# Nifty Options Ladder Trader (FastAPI + Dhan)

FastAPI web app (not a desktop app) for running a 2-sided NIFTY options ladder strategy using the **Dhan** Python SDK.

## What’s included
- Web dashboard to set `client_id`, `access_token`, strategy params, and enable/disable the engine
- Dhan MarketFeed WebSocket subscription to **NIFTY spot** (security id `13`)
- Background 1-minute candle aggregator (spot ticks → 1m OHLC)
- Breakout entry logic (2 green / 2 red candles), then ladder management on trailing SL / target (manual decision after stop by default; toggle **Full automation** to auto-flip)
- Instrument master downloader + weekly option selector from Dhan scrip-master CSV
- Strike selection: BUY prefers ITM (strict ITM on exact strikes), SELL prefers one-step OTM (strict OTM on exact strikes)
- Optional `instant_start` toggle to bypass breakout and start the first ladder immediately

## Safety / reality checks
- “Microseconds” end-to-end latency is not realistic in Python/Windows; this is **event-driven async** and avoids sleeps, but network + broker latency dominate.
- Orders are sent as **INTRADAY**. You must review quantities, risk limits, and broker rules before live trading.

## Setup
```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

## Run
```powershell
.\.venv\Scripts\Activate.ps1
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open: `http://127.0.0.1:8000/`

If you prefer `uvicorn main:app`, this repo includes a top-level `main.py` shim.

## First-time steps (important)
1. Click **Refresh instruments** (downloads Dhan scrip-master CSV to `data/dhan_scrip_master.csv`).
2. Enter `client_id` + `access_token`, set params, click **Save**.
3. Click **Start engine**.

## Notes
- The app downloads Dhan’s scrip master CSV on demand from within the UI (used to map weekly options to `security_id`).
- NIFTY spot feed uses Dhan MarketFeed exchange segment `IDX` with security id `"13"`.
- If you choose `LIMIT` order type, the app needs option LTP ticks (it auto-subscribes the active option), but the first order may still fail until the first option tick is received.
- Runtime files are stored in your user data folder (Windows: `%LOCALAPPDATA%\\niftyalgo\\`) so `uvicorn --reload` won’t restart when instruments are refreshed.
- `config.json` contains your `access_token`; keep your Windows user profile secure.
