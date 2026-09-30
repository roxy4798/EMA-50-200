"""Unit and integration tests for AlertQueue and API Server."""

import asyncio
import pytest
from httpx import AsyncClient, ASGITransport

from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.indicators.ema import GoldenCrossSignal
from app.notifications.telegram_bot import TelegramNotifier
from app.persistence.database import Database
from app.api.server import create_app


@pytest.mark.asyncio
async def test_alert_queue_processing(tmp_path):
    db_path = str(tmp_path / "test.db")
    db = Database(db_path=db_path)
    await db.init()

    binance_client = BinanceFuturesClient()
    chart_data = ChartDataProvider(binance_client=binance_client, database=db)
    chart_renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)

    queue = AlertQueue(
        chart_data_provider=chart_data,
        chart_renderer=chart_renderer,
        telegram_notifier=telegram,
        database=db,
    )
    queue.start()

    signal = GoldenCrossSignal(
        symbol="BTCUSDT",
        timeframe="1H",
        candle_timestamp=1700000000000,
        signal_time_utc="30 Sep 2026 • 10:00 UTC",
        ema50=65000.0,
        ema200=64000.0,
        close_price=65200.0,
        previous_ema50=63900.0,
        previous_ema200=64100.0,
    )

    # Save to db
    from app.persistence.models import SignalRecord
    sig_id = await db.save_signal(SignalRecord(
        id=None,
        symbol=signal.symbol,
        timeframe=signal.timeframe,
        candle_timestamp=signal.candle_timestamp,
        signal_time_utc=signal.signal_time_utc,
        ema50=signal.ema50,
        ema200=signal.ema200,
        close_price=signal.close_price,
    ))

    # Seed cached candles so worker does not have to call live Binance
    base_ts = 1700000000000
    seed_candles = [
        {
            "timestamp": base_ts + i * 3600000,
            "open": 64000.0,
            "high": 65500.0,
            "low": 63900.0,
            "close": 65000.0,
            "volume": 100.0,
            "ema_50": 64500.0,
            "ema_200": 64000.0,
        }
        for i in range(120)
    ]
    await db.cache_candles(seed_candles, "BTCUSDT", "1h")

    # Enqueue
    await queue.enqueue(sig_id, signal)
    # Wait for queue to be processed
    for _ in range(20):
        if queue.total_processed >= 1:
            break
        await asyncio.sleep(0.1)

    assert queue.total_processed >= 1
    await queue.stop()
    await binance_client.close()


@pytest.mark.asyncio
async def test_api_status_and_symbols(tmp_path):
    db_path = str(tmp_path / "api_test.db")
    db = Database(db_path=db_path)
    await db.init()

    binance_client = BinanceFuturesClient()
    chart_data = ChartDataProvider(binance_client=binance_client, database=db)
    chart_renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    queue = AlertQueue(chart_data, chart_renderer, telegram, db)
    engine = SignalEngine(binance_client, db, queue)
    engine.set_symbols(["BTCUSDT", "ETHUSDT"])
    ws_manager = BinanceWebSocketManager()

    app = create_app(
        signal_engine=engine,
        chart_data_provider=chart_data,
        chart_renderer=chart_renderer,
        ws_manager=ws_manager,
        binance_client=binance_client,
        telegram_notifier=telegram,
        database=db,
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Test /api/status
        resp = await client.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["binance"] == "ONLINE"
        assert data["symbols_total"] == 2
        assert "uptime" in data

        # 2. Test /api/symbols
        resp_sym = await client.get("/api/symbols")
        assert resp_sym.status_code == 200
        sym_data = resp_sym.json()
        assert len(sym_data["symbols"]) == 2
        assert sym_data["symbols"][0]["symbol"] == "BTCUSDT"

        # 3. Test /api/signals/recent
        resp_sig = await client.get("/api/signals/recent")
        assert resp_sig.status_code == 200
        assert isinstance(resp_sig.json(), list)

    await binance_client.close()
