"""Full pipeline verification script for NEXORA EMA CROSS."""

import asyncio
import os
import sys
from app.config import settings
from app.persistence.database import Database
from app.exchange.binance_client import BinanceFuturesClient
from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.notifications.telegram_bot import TelegramNotifier


async def run_verification():
    print("\n--- 1. Testing Database & Persistence ---")
    db = Database(db_path="data/test_verify.db")
    await db.init()
    print("Database initialized successfully.")

    print("\n--- 2. Testing Binance USD-M Futures REST Client ---")
    client = BinanceFuturesClient()
    connected = await client.check_connectivity()
    print(f"Binance Futures Ping: {'OK' if connected else 'FAILED'}")

    active_symbols = await client.get_active_usdt_symbols()
    print(f"Total Active USDT Perpetual Symbols on Binance: {len(active_symbols)}")
    assert len(active_symbols) > 100, "Should have over 100 active symbols"
    print(f"Sample Symbols: {active_symbols[:5]}")

    print("\n--- 3. Testing 1H Closed Candles Fetching ---")
    symbol = "BTCUSDT"
    candles = await client.get_klines(symbol, interval="1h", limit=150, only_closed=True)
    print(f"Fetched {len(candles)} closed 1H candles for {symbol}")
    assert len(candles) >= 100, "Should fetch at least 100 candles"
    print(f"Latest 1H Candle for {symbol}: Time={candles[-1]['timestamp']}, Close={candles[-1]['close']}")

    print("\n--- 4. Testing ChartDataProvider & EMA Calculations ---")
    chart_data = ChartDataProvider(binance_client=client, database=db)
    data = await chart_data.get_chart_data(symbol, timeframe="1h", limit=150, force_fresh=True)
    print(f"Chart data generated for {symbol}:")
    print(f"  Total candles: {len(data['candles'])}")
    print(f"  Total EMA50 points: {len(data['ema50'])}")
    print(f"  Total EMA200 points: {len(data['ema200'])}")
    print(f"  Detected Golden Crosses: {len(data['cross_markers'])}")
    print(f"  Latest Close: {data['latest']['close']}")
    print(f"  Latest EMA50: {data['latest']['ema50']}")
    print(f"  Latest EMA200: {data['latest']['ema200']}")

    print("\n--- 5. Testing High-Res 1600x900 Chart Image Renderer ---")
    renderer = ChartRenderer(output_dir="charts", width_px=1600, height_px=900, dpi=100)
    img_path = renderer.render_golden_cross_chart(chart_data=data)
    print(f"Rendered chart image at: {img_path}")
    assert img_path is not None and os.path.isfile(img_path), "Chart image should exist on disk"
    img_size = os.path.getsize(img_path)
    print(f"Chart image file size: {img_size:,} bytes")

    print("\n--- 6. Testing SignalEngine Historical Cross Scan ---")
    telegram = TelegramNotifier(enabled=False)
    alert_queue = AlertQueue(chart_data, renderer, telegram, db)
    alert_queue.start()

    engine = SignalEngine(client, db, alert_queue)
    engine.set_symbols(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    await engine.initialize_symbols()
    crosses = await engine.scan_and_record_historical_crosses("BTCUSDT")
    print(f"Historical Golden Crosses found in BTCUSDT dataset: {len(crosses)}")
    for c in crosses:
        print(f"  * Cross at {c.signal_time_utc} | Price: {c.close_price} | EMA50: {c.ema50:.2f} | EMA200: {c.ema200:.2f}")

    print("\n--- 7. Testing Alert Queue & Non-Blocking Worker ---")
    if crosses:
        test_signal = crosses[0]
        from app.persistence.models import SignalRecord
        sig_id = await db.save_signal(SignalRecord(
            id=None,
            symbol=test_signal.symbol,
            timeframe=test_signal.timeframe,
            candle_timestamp=test_signal.candle_timestamp,
            signal_time_utc=test_signal.signal_time_utc,
            ema50=test_signal.ema50,
            ema200=test_signal.ema200,
            close_price=test_signal.close_price,
        ))
        await alert_queue.enqueue(sig_id, test_signal)
        await asyncio.sleep(1.0)
        print(f"Alert Queue total processed: {alert_queue.total_processed}")

    await alert_queue.stop()
    await client.close()
    print("\n============================================================")
    print("ALL VERIFICATION CHECKS PASSED SUCCESSFULLY!")
    print("============================================================")


if __name__ == "__main__":
    asyncio.run(run_verification())
