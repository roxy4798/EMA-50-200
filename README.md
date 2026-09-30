# NEXORA EMA CROSS — MODERN MARKET CHART SYSTEM

Institutional market-data visualization and real-time monitoring engine for **Binance USD-M Futures 1H Golden Crosses**.

---

## Architecture Overview

```
                        BINANCE USD-M FUTURES
                                  │
                          (REST & WebSocket)
                                  ▼
                         MARKET DATA ENGINE
                                  │
                                  ▼
                             EMA ENGINE
                         (EMA 50 & EMA 200)
                                  │
                                  ▼
                         GOLDEN CROSS ENGINE
             (prev_ema50 <= prev_ema200 && curr_ema50 > curr_ema200)
                                  │
        ┌─────────────────────────┴─────────────────────────┐
        ▼                                                   ▼
 MODERN TERMINAL DASHBOARD                    INTERACTIVE WEB DASHBOARD
     (Rich Live Terminal)                     (TradingView Lightweight Charts)
        │                                                   │
        └─────────────────────────┬─────────────────────────┘
                                  ▼
                             ALERT QUEUE
                        (Async Non-Blocking)
                                  │
                                  ▼
                            CHART RENDERER
                     (1600x900 Institutional PNG)
                                  │
                                  ▼
                         TELEGRAM ALERT ENGINE
                     (Text Alert + Attached Chart)
```

---

## Key Features

1. **Two Professional User Interfaces**:
   - **Modern Interactive Web Chart** (`http://localhost:8080`): Built with React, TypeScript, and **TradingView Lightweight Charts**. Features real-time candle inspection, crosshair tooltips, EMA50 & EMA200 curves, exact Golden Cross markers, symbol selector, and a recent Golden Cross table with click-to-center navigation.
   - **Modern Terminal Dashboard**: Built with **Rich**. Displays live Binance connectivity, WebSocket health, database status, telegram delivery state, monitored universe metrics, and recent Golden Cross detections.

2. **Real Binance Market Data**:
   - Streams live 1H closed candles from Binance USD-M Futures (`wss://fstream.binance.com/ws` and `https://fapi.binance.com`).
   - Strict adherence to closed 1H candles for signal confirmation (no intra-candle false triggers).

3. **Shared EMA Logic**:
   - The Signal Engine and Chart Visualizer use the exact same calculation formula (`alpha = 2 / (period + 1)` with `adjust=False`).

4. **Dedicated NEXORA Chart Theme & Renderer**:
   - Generates crisp **1600x900 PNG** market charts with deep dark background (`#080B11`), vivid candles (`#00E676` bullish, `#FF3366` bearish), cyan EMA50 (`#00E5FF`), gold EMA200 (`#FFA000`), and dedicated `● GOLDEN CROSS` callout annotations.
   - Intelligent price axis formatting (e.g. BTC `83,170.00`, XRP `1.5450`, small-cap `0.001234`).

5. **Non-Blocking Alert Pipeline & Resilient Queue**:
   - Golden Cross detection is decoupled from chart generation via an asynchronous `AlertQueue`.
   - If chart rendering ever fails, the application logs `CHART_RENDER_ERROR`, preserves the signal in SQLite, and continues delivering the Telegram alert without interruption.

6. **Strictly Informational (Zero Trading Features)**:
   - Contains **NO** Entry, Stop Loss, Take Profit, Leverage, Margin, Position, or PnL metrics. Exclusively dedicated to quantitative Golden Cross market monitoring.

---

## Project Structure

```
nexora-ema-cross/
├── app/
│   ├── engine/
│   │   ├── alert_queue.py         # Async non-blocking alert queue & chart worker
│   │   └── signal_engine.py       # 1H candle Golden Cross detection engine
│   ├── exchange/
│   │   ├── binance_client.py      # Binance USD-M Futures REST client
│   │   └── websocket_manager.py   # Kline 1H WebSocket manager with auto-reconnect
│   ├── indicators/
│   │   └── ema.py                 # Shared EMA 50 & 200 formula & cross detector
│   ├── charts/
│   │   ├── chart_theme.py         # NEXORA dark visual identity tokens & price formatter
│   │   ├── chart_data.py          # Data provider with SQLite caching & window slicing
│   │   └── chart_renderer.py      # High-res 1600x900 Matplotlib PNG chart generator
│   ├── notifications/
│   │   └── telegram_bot.py        # Telegram alert formatter & photo uploader
│   ├── persistence/
│   │   ├── database.py            # SQLite database with aiosqlite
│   │   └── models.py              # Signal & candle data structures
│   ├── monitoring/
│   │   └── terminal_dashboard.py  # Rich live terminal dashboard
│   ├── api/
│   │   └── server.py              # FastAPI server serving API & React dashboard
│   ├── config.py                  # Pydantic configuration & environment settings
│   └── __init__.py
├── dashboard/                     # Modern React + TypeScript + TradingView Charts
│   ├── src/
│   │   ├── components/
│   │   │   ├── MarketChart.tsx    # Lightweight Charts instance & inspector
│   │   │   ├── SymbolSelector.tsx # Asset selector & search filter
│   │   │   ├── SystemStatusCard.tsx# Live system status metrics (Section 25)
│   │   │   ├── CrossHistory.tsx   # Recent Golden Crosses with click-to-center
│   │   │   └── Header.tsx         # Header banner & live pulse indicator
│   │   ├── api.ts                 # Typed API client
│   │   ├── App.tsx                # Master dashboard layout
│   │   └── index.css              # NEXORA institutional CSS design system
│   ├── package.json
│   ├── tsconfig.json
│   └── vite.config.ts
├── tests/
│   ├── test_indicators.py         # Tests for EMA and Golden Cross condition
│   ├── test_chart_data.py         # Tests for price formatting and data provider
│   ├── test_chart_renderer.py     # Tests for 1600x900 PNG generation & error safety
│   └── test_alert_queue.py        # Tests for async queue and API endpoints
├── data/                          # SQLite database location (nexora.db)
├── logs/                          # System logs
├── charts/                        # Generated PNG chart images (1600x900)
├── .env.example                   # Environment configuration template
├── requirements.txt               # Python package dependencies
├── run.py                         # Unified system entry point
└── verify_system.py               # End-to-end verification script
```

---

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
cd dashboard && npm install && npm run build && cd ..
```

### 2. Configure Environment (`.env`)
```ini
BINANCE_FUTURES_BASE_URL=https://fapi.binance.com
BINANCE_WS_BASE_URL=wss://fstream.binance.com/ws
TIMEFRAME=1h
EMA_FAST=50
EMA_SLOW=200
CANDLE_LIMIT=150
SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,NEARUSDT
PORT=8080
```

### 3. Run the System
```bash
python run.py
```
- Open `http://localhost:8080` in your browser to interact with the modern market chart.
- Monitor real-time status in the live terminal dashboard.

### 4. Run Automated Tests
```bash
python -m pytest tests/ -v
```
All 10 test suites validate indicator calculations, exact Golden Cross triggers, 1600x900 PNG image rendering, async alert queue processing, and API endpoints.
