"""NEXORA EMA CROSS — Main Application Runner.

Runs both the Terminal Dashboard and the Web Dashboard/Chart Server.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from typing import Optional
import uvicorn

from app.config import settings
from app.persistence.database import Database
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.notifications.telegram_bot import TelegramNotifier
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.monitoring.terminal_dashboard import TerminalDashboard
from app.api.server import create_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/nexora.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("nexora.main")


async def main() -> None:
    parser = argparse.ArgumentParser(description="NEXORA EMA CROSS System")
    parser.add_argument("--port", type=int, default=settings.port, help="Web dashboard port")
    parser.add_argument("--no-terminal", action="store_true", help="Disable Rich terminal dashboard")
    parser.add_argument("--all-symbols", action="store_true", help="Monitor all active USDT perpetuals")
    args = parser.parse_args()

    port = args.port

    print("============================================================")
    print("NEXORA EMA CROSS — MODERN MARKET CHART SYSTEM")
    print("Binance USD-M Futures • 1H Golden Cross Engine")
    print(f"Web Dashboard: http://localhost:{port}")
    print("============================================================")

    # 1. Initialize Database
    db = Database(db_path=settings.db_path)
    await db.init()

    # 2. Initialize Binance REST Client & WebSocket Manager
    binance_client = BinanceFuturesClient(base_url=settings.binance_futures_base_url)
    ws_manager = BinanceWebSocketManager(
        base_ws_url=settings.binance_ws_base_url,
        timeframe=settings.timeframe,
    )

    # 3. Initialize Chart Services
    chart_data = ChartDataProvider(
        binance_client=binance_client,
        database=db,
        cache_ttl_seconds=settings.cache_ttl_seconds,
        fast_period=settings.ema_fast,
        slow_period=settings.ema_slow,
    )
    chart_renderer = ChartRenderer(
        output_dir=settings.charts_dir,
        width_px=settings.chart_width,
        height_px=settings.chart_height,
        dpi=settings.chart_dpi,
        fast_period=settings.ema_fast,
        slow_period=settings.ema_slow,
    )

    # 4. Initialize Telegram Notifier
    telegram_notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
        enabled=settings.telegram_enabled,
        fast_period=settings.ema_fast,
        slow_period=settings.ema_slow,
    )

    # 5. Initialize Alert Queue
    alert_queue = AlertQueue(
        chart_data_provider=chart_data,
        chart_renderer=chart_renderer,
        telegram_notifier=telegram_notifier,
        database=db,
        fast_period=settings.ema_fast,
        slow_period=settings.ema_slow,
    )
    alert_queue.start()

    # 6. Initialize Signal Engine
    signal_engine = SignalEngine(
        binance_client=binance_client,
        database=db,
        alert_queue=alert_queue,
        timeframe=settings.timeframe,
        fast_period=settings.ema_fast,
        slow_period=settings.ema_slow,
        candle_limit=settings.candle_limit,
    )

    ws_manager.on_candle_closed = signal_engine.handle_closed_candle

    # Determine symbols
    if args.all_symbols or settings.all_symbols:
        active_symbols = await binance_client.get_active_usdt_symbols()
        symbols = active_symbols if active_symbols else settings.symbol_list
    else:
        symbols = settings.symbol_list

    signal_engine.set_symbols(symbols)
    ws_manager.set_symbols(symbols)

    # Wire services to Telegram Notifier & start command listener
    telegram_notifier.set_services(
        signal_engine=signal_engine,
        ws_manager=ws_manager,
        database=db,
        binance_client=binance_client,
        alert_queue=alert_queue,
        chart_data_provider=chart_data,
        chart_renderer=chart_renderer,
    )
    await telegram_notifier.start()

    # 7. Start FastAPI Web Server in asyncio task
    api_app = create_app(
        signal_engine=signal_engine,
        chart_data_provider=chart_data,
        chart_renderer=chart_renderer,
        ws_manager=ws_manager,
        binance_client=binance_client,
        telegram_notifier=telegram_notifier,
        database=db,
    )

    server_config = uvicorn.Config(
        app=api_app,
        host=settings.host,
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(server_config)
    server_task = asyncio.create_task(server.serve())

    # 8. Start Background Symbol Initialization & Historical Scan
    init_task = asyncio.create_task(
        signal_engine.initialize_symbols(max_concurrency=3, pacing_delay_ms=200.0)
    )

    # 9. Start WebSocket Manager
    await ws_manager.start()

    # 10. Start Terminal Dashboard (if not disabled)
    terminal_dashboard: Optional[TerminalDashboard] = None
    if not args.no_terminal:
        terminal_dashboard = TerminalDashboard(
            signal_engine=signal_engine,
            ws_manager=ws_manager,
            binance_client=binance_client,
            telegram_notifier=telegram_notifier,
            database=db,
            port=port,
        )
        terminal_dashboard.start()

    # Post initialization: scan all symbols for historical crosses
    # Cap at 50 for fast startup; remaining symbols get full live detection coverage
    HISTORICAL_SCAN_LIMIT = 50
    async def post_init():
        await init_task
        scan_targets = symbols[:HISTORICAL_SCAN_LIMIT]
        await signal_engine.scan_historical_symbols(
            scan_targets,
            max_concurrency=5,
            total_symbols_count=len(symbols),
            log=logger,
        )

    asyncio.create_task(post_init())

    # Periodic low-noise health summary task (runs every 60 seconds)
    async def periodic_health_summary():
        from datetime import datetime, timezone
        while True:
            await asyncio.sleep(60)
            try:
                mkt_health = ws_manager.get_market_data_health()
                status = mkt_health["status"]
                active_conn = mkt_health.get("active_connections", 0)
                total_workers = mkt_health.get("total_workers", 0)
                tot_msgs = mkt_health.get("total_messages_received", 0)
                tot_klines = mkt_health.get("total_klines_received", 0)
                closed_count = mkt_health.get("candles_closed_count", 0)
                reconnects = mkt_health.get("reconnect_count", 0)
                last_kline_ts = mkt_health.get("last_kline_received_at")
                last_kline_str = (
                    datetime.fromtimestamp(last_kline_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    if last_kline_ts else "NEVER"
                )
                ready_syms = len(signal_engine.initialized_symbols)
                total_syms = len(signal_engine.symbols)
                waiting_syms = signal_engine.get_state_count("WAITING_FOR_HISTORY")
                retry_syms = signal_engine.get_state_count("RETRY_PENDING")
                c429 = binance_client.rate_limit_429_count
                c418 = binance_client.ip_ban_418_count

                if mkt_health["is_healthy"]:
                    logger.info(
                        f"[HEALTH_SUMMARY] Feed: HEALTHY | WS: {active_conn}/{total_workers} active | "
                        f"Msgs: {tot_msgs} | Klines: {tot_klines} (last: {last_kline_str}) | "
                        f"Closed: {closed_count} | Ready: {ready_syms}/{total_syms} (Waiting: {waiting_syms}, RetryPending: {retry_syms}) | "
                        f"REST 429/418: {c429}/{c418} | Reconnects: {reconnects}"
                    )
                else:
                    logger.warning(
                        f"[HEALTH_WARNING] Feed: {status} | WS: {active_conn}/{total_workers} active | "
                        f"Msgs: {tot_msgs} | Klines: {tot_klines} (last: {last_kline_str}) | "
                        f"Closed: {closed_count} | Ready: {ready_syms}/{total_syms} (Waiting: {waiting_syms}, RetryPending: {retry_syms}) | "
                        f"REST 429/418: {c429}/{c418} | Reconnects: {reconnects}"
                    )
            except Exception as e:
                logger.error(f"Error in periodic health summary: {e}")

    health_task = asyncio.create_task(periodic_health_summary())

    # Periodic background retry for unready symbols (runs every 5 minutes)
    async def periodic_unready_retry():
        while True:
            await asyncio.sleep(300)
            try:
                unready_count = len(signal_engine.symbols) - len(signal_engine.initialized_symbols)
                if unready_count > 0:
                    await signal_engine.retry_unready_symbols(max_concurrency=2, pacing_delay_ms=250.0)
            except Exception as e:
                logger.error(f"Error in periodic unready symbols retry: {e}")

    retry_task = asyncio.create_task(periodic_unready_retry())

    # Periodic symbol refresh task (runs every 60 minutes)
    async def periodic_refresh():
        while True:
            await asyncio.sleep(3600)
            try:
                await signal_engine.refresh_symbol_universe()
            except Exception as e:
                logger.error(f"Error in periodic symbol refresh: {e}")

    refresh_task = asyncio.create_task(periodic_refresh())

    # Graceful shutdown handler
    stop_event = asyncio.Event()

    def handle_signal():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    try:
        while not stop_event.is_set():
            await asyncio.sleep(1.0)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        print("\nShutting down NEXORA EMA CROSS...")
        health_task.cancel()
        retry_task.cancel()
        refresh_task.cancel()
        if terminal_dashboard:
            await terminal_dashboard.stop()
        await ws_manager.stop()
        await alert_queue.stop()
        await binance_client.close()
        await telegram_notifier.close()
        server.should_exit = True
        await server_task
        print("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
