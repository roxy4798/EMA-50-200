"""NEXORA FastAPI Server & Web Endpoints with complete health metrics."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.engine.signal_engine import SignalEngine
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.exchange.binance_client import BinanceFuturesClient
from app.notifications.telegram_bot import TelegramNotifier
from app.persistence.database import Database

logger = logging.getLogger("nexora.api")


def create_app(
    signal_engine: SignalEngine,
    chart_data_provider: ChartDataProvider,
    chart_renderer: ChartRenderer,
    ws_manager: BinanceWebSocketManager,
    binance_client: BinanceFuturesClient,
    telegram_notifier: TelegramNotifier,
    database: Database,
) -> FastAPI:
    app = FastAPI(title="NEXORA EMA CROSS API", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    Path("charts").mkdir(parents=True, exist_ok=True)
    app.mount("/charts", StaticFiles(directory="charts"), name="charts")

    @app.get("/api/status")
    async def get_system_status() -> Dict[str, Any]:
        """Provides real-time system status indicators matching terminal & web dashboard specs."""
        now = time.time()
        uptime_sec = int(now - signal_engine.start_time)
        hours, remainder = divmod(uptime_sec, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        total_signals = await database.get_total_signals_count()
        live_signals = await database.get_live_signals_count()
        historical_signals = await database.get_historical_signals_count()
        last_sig = await database.get_last_signal() or signal_engine.last_signal

        last_closed_str = "-"
        if signal_engine.last_closed_candle_time:
            dt = datetime.fromtimestamp(signal_engine.last_closed_candle_time / 1000.0, tz=timezone.utc)
            last_closed_str = dt.strftime("%d %b %Y • %H:%M UTC")

        telegram_status = "ONLINE" if telegram_notifier.is_configured else "TELEGRAM CONFIGURATION MISSING"
        market_data_health = ws_manager.get_market_data_health()

        return {
            "mode": "LONG ONLY",
            "signal": f"EMA{signal_engine.fast_period} CROSS ABOVE EMA{signal_engine.slow_period}",
            "binance": "ONLINE",
            "websocket": "CONNECTED" if ws_manager.is_connected else "RECONNECTING",
            "market_data": market_data_health["status"],
            "last_kline_received_at": market_data_health["last_kline_received_at"],
            "last_closed_1h_candle": last_closed_str,
            "database": "ONLINE",
            "telegram": telegram_status,
            "symbols_total": len(signal_engine.symbols),
            "symbols_initialized": len(signal_engine.initialized_symbols),
            "golden_crosses_total": total_signals,
            "live_signals_count": live_signals,
            "historical_crosses_count": historical_signals,
            "candles_closed_count": ws_manager.candles_closed_count,
            "last_signal": last_sig,
            "last_closed_candle": last_closed_str,
            "reconnect_count": ws_manager.reconnect_count,
            "rate_limit_429_count": binance_client.rate_limit_429_count,
            "ip_ban_418_count": binance_client.ip_ban_418_count,
            "uptime": uptime_str,
            "timeframe": signal_engine.timeframe.upper(),
            "ema_fast": signal_engine.fast_period,
            "ema_slow": signal_engine.slow_period,
        }

    @app.get("/api/symbols")
    async def get_symbols() -> Dict[str, Any]:
        """List monitored symbols with metadata."""
        symbols_info = []
        for s in signal_engine.symbols:
            is_init = s in signal_engine.initialized_symbols
            history = signal_engine.candles_history.get(s, [])
            last_close = history[-1]["close"] if history else None
            symbols_info.append({
                "symbol": s,
                "initialized": is_init,
                "last_close": last_close,
                "timeframe": signal_engine.timeframe.upper(),
            })
        return {"symbols": symbols_info}

    @app.get("/api/chart/{symbol}")
    async def get_chart_data(
        symbol: str,
        timeframe: str = "1h",
        limit: int = Query(default=150, ge=30, le=500),
        target_timestamp: Optional[int] = None,
        force_fresh: bool = False,
    ) -> Dict[str, Any]:
        """Returns structured candlestick, EMA50, EMA200 and Golden Cross markers."""
        symbol = symbol.upper()
        data = await chart_data_provider.get_chart_data(
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
            target_timestamp=target_timestamp,
            force_fresh=force_fresh,
        )

        if not data.get("candles"):
            raise HTTPException(status_code=404, detail=f"No candle data available for symbol {symbol}")

        resp_data = {
            "symbol": data["symbol"],
            "timeframe": data["timeframe"],
            "candles": data["candles"],
            "ema50": data.get("ema50"),
            "ema200": data.get("ema200"),
            "ema_fast": data.get("ema_fast", data.get("ema50")),
            "ema_slow": data.get("ema_slow", data.get("ema200")),
            "fast_period": data.get("fast_period", signal_engine.fast_period),
            "slow_period": data.get("slow_period", signal_engine.slow_period),
            "cross_markers": data["cross_markers"],
            "latest": data["latest"],
        }
        return resp_data

    @app.get("/api/signals/recent")
    async def get_recent_signals(limit: int = Query(default=30, ge=1, le=100)) -> List[Dict[str, Any]]:
        """Fetch recent Golden Cross signals."""
        return await database.get_recent_signals(limit=limit)

    @app.get("/api/chart-image/{symbol}")
    async def generate_chart_image(
        symbol: str,
        target_timestamp: Optional[int] = None,
    ) -> Response:
        """Generates or fetches PNG market chart for the symbol."""
        symbol = symbol.upper()
        data = await chart_data_provider.get_chart_data(
            symbol=symbol,
            timeframe=signal_engine.timeframe,
            limit=150,
            target_timestamp=target_timestamp,
            force_fresh=False,
        )

        if not data.get("candles"):
            raise HTTPException(status_code=404, detail=f"Cannot generate chart: No candle data for {symbol}")

        png_path = chart_renderer.render_golden_cross_chart(
            chart_data=data,
            target_timestamp=target_timestamp,
        )

        if not png_path or not os.path.isfile(png_path):
            raise HTTPException(status_code=500, detail="Failed to render chart image")

        return FileResponse(png_path, media_type="image/png", filename=os.path.basename(png_path))

    dashboard_dist = Path("dashboard/dist")
    if dashboard_dist.is_dir():
        app.mount("/", StaticFiles(directory="dashboard/dist", html=True), name="dashboard")
    else:
        @app.get("/")
        async def fallback_home():
            return JSONResponse({
                "message": "NEXORA EMA CROSS API is running.",
                "endpoints": {
                    "status": "/api/status",
                    "symbols": "/api/symbols",
                    "recent_signals": "/api/signals/recent",
                    "chart_data": "/api/chart/BTCUSDT",
                    "chart_image": "/api/chart-image/BTCUSDT",
                },
            })

    return app
