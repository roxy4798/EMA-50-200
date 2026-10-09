# NEXORA EMA CROSS

Configurable EMA Golden Cross monitoring for **Binance USDⓈ-M Futures**, with a 1-hour (1H) signal timeframe, a Telegram alert pipeline, a SQLite signal history, and a read-only web dashboard.

NEXORA is an **alerting and market-monitoring system only**. It does not place trades or manage positions.

## What it does

- Monitors selected active USDT perpetual futures symbols, or the active USDT perpetual universe when enabled.
- Calculates configurable fast and slow EMAs (the fast period must be lower than the slow period).
- Detects **Golden Cross only**: on a closed 1H candle, the fast EMA crosses from at-or-below the slow EMA to above it.
- Uses Binance USDⓈ-M Futures REST and WebSocket market data.
- Stores signals and candle cache in SQLite, with duplicate-signal protection.
- Sends live signal notifications to Telegram and attaches a generated chart when chart rendering succeeds.
- Provides a terminal dashboard and a React/TypeScript web dashboard with interactive market charts and signal history.
- Separates live alerts from historical signal scans. Historical scan results are recorded as history and are not sent as live Telegram alerts.

## Strategy rules

| Setting | Behavior |
|---|---|
| Market | Binance USDⓈ-M Futures |
| Signal timeframe | **1H only** |
| Signal type | **Golden Cross only** |
| EMA periods | Configurable via `EMA_FAST` and `EMA_SLOW`; defaults in code are 50 and 200 |
| Candle confirmation | Closed candles only; no signal on an open/in-progress candle |
| Warm-up history | At least 1,000 closed 1H candles are required by the signal engine |
| Short/death-cross signals | Not implemented |
| Order execution | Not implemented |

Golden Cross condition:

```text
previous_fast_ema <= previous_slow_ema
AND
current_fast_ema > current_slow_ema
AND
current candle is closed
```

EMA calculation is shared between signal detection and chart data, using `alpha = 2 / (period + 1)` with `adjust=False`.

### EMA 50/500 configuration

The periods are not hardcoded to 50/200. For example, to use EMA 50/500, configure:

```ini
TIMEFRAME=1h
EMA_FAST=50
EMA_SLOW=500
CANDLE_LIMIT=1000
```

The configuration loader enforces a minimum effective `CANDLE_LIMIT` of 1,000 even if an older `.env` file still contains `CANDLE_LIMIT=250`. The fast EMA must be positive and strictly lower than the slow EMA. NEXORA rejects timeframes other than 1H.

## Architecture

```text
Binance USDⓈ-M Futures REST + WebSocket
                  |
                  v
      Closed 1H candle processing
                  |
                  v
       Configurable EMA engine
                  |
                  v
      Golden Cross signal detection
                  |
           +------+------+
           |             |
           v             v
     SQLite history   Async alert queue
                           |
                    +------+------+
                    |             |
                    v             v
              Chart renderer   Telegram alert
                    |
                    v
             PNG chart attachment

The same signal/candle data also powers the terminal dashboard and web dashboard.
```

## Features

### Market data and signal engine
- Binance USDⓈ-M Futures REST client and WebSocket kline streams.
- Closed-candle validation and 1H-only signal processing.
- Historical candle initialization, cache persistence, and recovery/retry handling.
- Duplicate prevention for signals.
- Configurable fast/slow EMA periods used across signal detection, charts, and notification labels.

### Alerts and charts
- Asynchronous alert queue so chart rendering is separated from signal processing.
- Telegram messages for live Golden Cross events, with a generated 1600×900 PNG chart when available.
- Chart rendering errors are logged; they should not stop the main signal-processing loop.
- Historical scan results are stored separately from live alerts.

### Dashboards
- **Terminal dashboard:** market-data connectivity, WebSocket health, database and Telegram status, monitored symbols, and recent signal information.
- **Web dashboard:** interactive candlestick chart, EMA overlays, Golden Cross markers, symbol selection, and recent signal history.
- Web dashboard/API default port: `8080`.

### Telegram commands

The bot registers these read-only commands:

| Command | Purpose |
|---|---|
| `/start` | Welcome screen and navigation |
| `/dashboard` | System status, symbol count, signal counts, and uptime |
| `/market` | Market scanner and WebSocket status |
| `/alerts` | Recent recorded Golden Cross signals |
| `/symbol BTCUSDT` | Inspect a symbol's EMA structure and view its chart |
| `/history` | Historical signal counts and lookback filters |
| `/status` | Technical health and connection/error metrics |
| `/help` | Command guide |

Telegram notifications require valid bot credentials and an enabled configuration. Keep credentials private; do not commit `.env` to Git.

## Project structure

```text
.
├── app/
│   ├── api/server.py                 # FastAPI API and dashboard integration
│   ├── charts/
│   │   ├── chart_data.py             # Chart data and candle cache access
│   │   ├── chart_renderer.py         # PNG chart generation
│   │   └── chart_theme.py            # Chart styling and price formatting
│   ├── engine/
│   │   ├── alert_queue.py            # Asynchronous alert pipeline
│   │   └── signal_engine.py          # Closed 1H Golden Cross detection
│   ├── exchange/
│   │   ├── binance_client.py         # Binance USDⓈ-M Futures REST client
│   │   └── websocket_manager.py      # Kline WebSocket management
│   ├── indicators/ema.py             # EMA calculation and cross detection
│   ├── monitoring/terminal_dashboard.py
│   ├── notifications/
│   │   ├── telegram_bot.py           # Telegram notifier and command listener
│   │   └── telegram_commands.py      # Telegram command views
│   ├── persistence/
│   │   ├── database.py               # SQLite persistence and candle cache
│   │   └── models.py                 # Signal/candle data structures
│   └── config.py                     # Environment settings and validation
├── dashboard/                        # React + TypeScript + Vite frontend
├── tests/                            # Unit and regression tests
├── data/                             # SQLite database directory
├── charts/                           # Generated chart images
├── logs/                             # Application logs
├── .env.example                      # Example environment configuration
├── requirements.txt                  # Python dependencies
├── run.py                            # Application entry point
└── verify_system.py                  # System verification helper
```

## Quick start

### Requirements

- Python 3.10 or newer
- Node.js and npm (to build the dashboard)
- Network access to Binance USDⓈ-M Futures endpoints
- Telegram bot credentials if Telegram alerts are required

### 1. Install Python dependencies

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

On Windows, activate the environment with:

```powershell
.venv\Scripts\Activate.ps1
```

### 2. Build the web dashboard

```bash
cd dashboard
npm install
npm run build
cd ..
```

### 3. Configure environment

Copy the example file and edit it:

```bash
cp .env.example .env
```

Example configuration for EMA 50/500:

```ini
BINANCE_FUTURES_BASE_URL=https://fapi.binance.com
BINANCE_WS_BASE_URL=wss://fstream.binance.com
TIMEFRAME=1h
EMA_FAST=50
EMA_SLOW=500
CANDLE_LIMIT=1000

# Comma-separated symbols to monitor
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT
ALL_SYMBOLS=false

# Telegram (leave disabled if not configured)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_ENABLED=false

HOST=0.0.0.0
PORT=8080
DB_PATH=data/nexora.db
CHARTS_DIR=charts
CHART_WIDTH=1600
CHART_HEIGHT=900
CHART_DPI=100
CACHE_TTL_SECONDS=300
```

Configuration notes:

- `EMA_FAST` and `EMA_SLOW` are configurable; `EMA_FAST` must be smaller than `EMA_SLOW`.
- `TIMEFRAME` must be `1h`.
- At least 1,000 closed candles are required. The effective `CANDLE_LIMIT` is clamped to at least 1,000.
- Set `ALL_SYMBOLS=true` or pass `--all-symbols` to monitor the active USDT perpetual universe. Otherwise, NEXORA uses the comma-separated `SYMBOLS` list.
- Set `TELEGRAM_ENABLED=true` and provide the bot token and chat ID to enable Telegram delivery.
- Do not commit `.env`, API credentials, Telegram tokens, or production databases.

### 4. Run NEXORA

```bash
python run.py
```

Optional arguments:

```bash
python run.py --port 8080
python run.py --no-terminal
python run.py --all-symbols
```

Open `http://localhost:8080` from the host running NEXORA. If the dashboard is reachable from another machine, restrict access with firewall/network rules or a properly configured reverse proxy; do not expose an unprotected service to the public internet.

On startup, NEXORA initializes historical candles, starts market-data streams, and performs a bounded historical signal scan. Initializing symbols may take time and Binance rate limits can affect how quickly all symbols become ready. Symbols without enough valid closed-candle history must not be treated as signal-ready.

## Tests and verification

Run the complete Python test suite:

```bash
python -m pytest -v
```

Run the EMA configuration and Golden Cross regression tests specifically:

```bash
python -m pytest tests/test_ema_generalization.py -v
```

Build the frontend:

```bash
cd dashboard
npm run build
```

The tests cover configurable EMA periods, Golden Cross detection, closed-candle requirements, minimum history validation, alert queue behavior, Telegram labels/commands, WebSocket handling, and other regression cases. Test totals can vary if the local checkout or environment contains a different set of tests.

## Data and operational notes

- SQLite data is stored at `data/nexora.db` by default; generated PNG charts are stored under `charts/`, and application logs under `logs/`.
- Back up the SQLite database before performing manual schema or data changes.
- Binance market data is subject to API availability and rate limits. A successful process start does not by itself mean every symbol has initialized; inspect the logs and dashboard health indicators.
- The dashboard and Telegram views are informational. **NEXORA does not submit orders or execute trades.**
- Keep production configuration and secrets out of source control.

## License

No license is specified in this repository. All rights and permissions remain with the repository owner unless a license is added.
