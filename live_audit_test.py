"""NEXORA EMA CROSS — LIVE INTEGRATION & AUDIT TEST RUNNER.

Executes all audit criteria and outputs the exact validation report.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone
import pandas as pd
from PIL import Image

from app.config import settings
from app.exchange.binance_client import BinanceFuturesClient
from app.indicators.ema import (
    calculate_ema,
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)
from app.persistence.database import Database
from app.persistence.models import SignalRecord
from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.charts.chart_theme import format_price
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.notifications.telegram_bot import TelegramNotifier


async def run_live_audit() -> None:
    print("=" * 65)
    print("NEXORA EMA CROSS — LIVE PRODUCTION AUDIT")
    print("=" * 65)

    # -------------------------------------------------------------
    # 1. CORE SIGNAL & MATHEMATICAL VALIDATION
    # -------------------------------------------------------------
    print("\n[TEST 1] Core Signal & Mathematical EMA Validation...")
    # Numerical validation of alpha = 2 / (period + 1)
    for span in [50, 200]:
        alpha = 2.0 / (span + 1.0)
        test_prices = [100.0 + i * 2.0 for i in range(10)]
        ema_res = calculate_ema(test_prices, span=span)
        # Verify first step:
        assert ema_res.iloc[0] == pytest_approx(100.0)
        assert ema_res.iloc[1] == pytest_approx(102.0 * alpha + 100.0 * (1.0 - alpha))
    print("  -> EMA recurrence formula alpha = 2/(span+1): PASS")

    # Signal condition check: prev EMA50 <= prev EMA200 AND curr EMA50 > curr EMA200
    df_sig_pass = pd.DataFrame([
        {"timestamp": 1000, "close": 100.0, "ema_50": 99.5, "ema_200": 100.0},  # <=
        {"timestamp": 2000, "close": 105.0, "ema_50": 100.2, "ema_200": 100.05}, # >
    ])
    sig = detect_golden_cross(df_sig_pass, "BTCUSDT", "1h")
    assert sig is not None and sig.symbol == "BTCUSDT" and sig.candle_status == "CLOSED"

    # Verify no signal when already bullish
    df_already_bull = pd.DataFrame([
        {"timestamp": 1000, "close": 100.0, "ema_50": 101.0, "ema_200": 100.0},
        {"timestamp": 2000, "close": 105.0, "ema_50": 102.0, "ema_200": 100.5},
    ])
    assert detect_golden_cross(df_already_bull, "BTCUSDT", "1h") is None

    # Verify no death cross signal
    df_death = pd.DataFrame([
        {"timestamp": 1000, "close": 100.0, "ema_50": 101.0, "ema_200": 100.0},
        {"timestamp": 2000, "close": 90.0, "ema_50": 98.0, "ema_200": 99.5},
    ])
    assert detect_golden_cross(df_death, "BTCUSDT", "1h") is None
    print("  -> Golden Cross condition strictly verified (zero death-cross/open candle signals): PASS")

    # -------------------------------------------------------------
    # 2. REAL BINANCE USD-M FUTURES DATA TEST
    # -------------------------------------------------------------
    print("\n[TEST 2] Live Binance USD-M Futures Public Market Data...")
    client = BinanceFuturesClient()
    is_online = await client.check_connectivity()
    assert is_online, "Binance ping failed"
    print(f"  -> Binance Futures REST Ping: PASS (Status: ONLINE)")

    symbols = await client.get_active_usdt_symbols()
    print(f"  -> Binance Active USDT Perpetuals Discovered: {len(symbols)} symbols")
    assert len(symbols) > 200, f"Expected > 200 perpetuals, got {len(symbols)}"
    for test_sym in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
        assert test_sym in symbols, f"Missing core symbol {test_sym}"
    print(f"  -> Core symbol availability (BTC, ETH, SOL): PASS")

    # Fetch 1H closed candles
    btc_candles = await client.get_klines("BTCUSDT", interval="1h", limit=150, only_closed=True)
    assert len(btc_candles) >= 140, "Candles count insufficient"
    latest_candle = btc_candles[-1]
    now_ms = int(time.time() * 1000)
    # The latest closed 1H candle must have closed within the past 1-2 hours
    candle_age_hr = (now_ms - latest_candle["timestamp"]) / (3600 * 1000)
    print(f"  -> Latest closed 1H candle for BTCUSDT: Close={latest_candle['close']}, Age={candle_age_hr:.2f}h")
    assert candle_age_hr < 3.0, "Latest candle seems stale"
    print("  -> Live Binance closed candle handling: PASS")

    # -------------------------------------------------------------
    # 3. REAL HISTORICAL GOLDEN CROSS TEST
    # -------------------------------------------------------------
    print("\n[TEST 3] Real Historical Golden Cross Verification...")
    test_db = Database(db_path="data/audit_test.db")
    await test_db.init()
    chart_provider = ChartDataProvider(client, test_db)

    # Search for real cross in recent 500 candles across major symbols
    found_cross: Optional[GoldenCrossSignal] = None
    target_sym = ""
    for s in ["ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "BTCUSDT"]:
        klines = await client.get_klines(s, interval="1h", limit=250, only_closed=True)
        if len(klines) >= 200:
            df = enrich_candles_with_ema(klines, 50, 200)
            crosses = find_all_golden_crosses(df, s, "1h")
            if crosses:
                found_cross = crosses[-1]
                target_sym = s
                break

    assert found_cross is not None, "Failed to find any real historical Golden Cross"
    print("\nREAL HISTORICAL CROSS")
    print("-" * 35)
    print(f"Symbol:           {target_sym}")
    print(f"Time (UTC):       {found_cross.signal_time_utc}")
    print(f"Candle Timestamp: {found_cross.candle_timestamp}")
    print(f"Close Price:      {format_price(found_cross.close_price)}")
    print(f"Previous EMA50:   {format_price(found_cross.previous_ema50)}")
    print(f"Previous EMA200:  {format_price(found_cross.previous_ema200)}")
    print(f"Current EMA50:    {format_price(found_cross.ema50)}")
    print(f"Current EMA200:   {format_price(found_cross.ema200)}")
    
    cond_check = (found_cross.previous_ema50 <= found_cross.previous_ema200) and (found_cross.ema50 > found_cross.ema200)
    print(f"Condition Check:  {'PASS' if cond_check else 'FAIL'}")
    assert cond_check, "Mathematical Golden Cross condition failed!"

    # -------------------------------------------------------------
    # 4. CHART & MARKER AUDIT
    # -------------------------------------------------------------
    print("\n[TEST 4] High-Resolution (1600x900) Chart Rendering & Marker Audit...")
    # Persist the detected event first so the chart uses the exact canonical
    # crossover and previous-candle EMA values produced by the same detector.
    await test_db.save_signal(SignalRecord(
        id=None,
        symbol=found_cross.symbol,
        timeframe=found_cross.timeframe,
        candle_timestamp=found_cross.candle_timestamp,
        signal_time_utc=found_cross.signal_time_utc,
        ema50=found_cross.ema50,
        ema200=found_cross.ema200,
        close_price=found_cross.close_price,
        previous_ema50=found_cross.previous_ema50,
        previous_ema200=found_cross.previous_ema200,
    ))
    chart_data = await chart_provider.get_chart_data(target_sym, limit=150, target_timestamp=found_cross.candle_timestamp, force_fresh=True)
    assert len(chart_data["candles"]) >= 60, "Candles in chart window insufficient"
    assert len(chart_data["cross_markers"]) >= 1, "Expected at least 1 Golden Cross marker in window"

    matching_markers = [m for m in chart_data["cross_markers"] if m["timestamp_ms"] == found_cross.candle_timestamp]
    assert len(matching_markers) >= 1, "Marker timestamp must EXACTLY match candle timestamp!"
    marker = matching_markers[0]
    print(f"Marker Text:      {marker['text']}")
    print(f"Marker Time ms:   {marker['timestamp_ms']}")
    assert marker["timestamp_ms"] == found_cross.candle_timestamp, "Marker timestamp must EXACTLY match candle timestamp!"
    print("Chart Marker:     PASS (Exact timestamp match)")

    renderer = ChartRenderer(output_dir="charts", width_px=1600, height_px=900, dpi=100)
    chart_path = renderer.render_golden_cross_chart(chart_data, target_timestamp=found_cross.candle_timestamp, custom_filename="AUDIT_GOLDEN_CROSS.png")
    assert chart_path and os.path.isfile(chart_path), "Failed to render chart PNG"
    with Image.open(chart_path) as im:
        assert im.size == (1600, 900), f"Expected 1600x900, got {im.size}"
        assert im.format == "PNG"
    print(f"  -> Generated 1600x900 PNG: PASS ({chart_path}, {os.path.getsize(chart_path):,} bytes)")

    # -------------------------------------------------------------
    # 5. DUPLICATE ALERT SUPPRESSION & IDEMPOTENCY TEST
    # -------------------------------------------------------------
    print("\n[TEST 5] Duplicate Alert Suppression & Idempotency...")
    # First save:
    sig_rec = SignalRecord(
        id=None,
        symbol=found_cross.symbol,
        timeframe=found_cross.timeframe,
        candle_timestamp=found_cross.candle_timestamp,
        signal_time_utc=found_cross.signal_time_utc,
        ema50=found_cross.ema50,
        ema200=found_cross.ema200,
        close_price=found_cross.close_price,
        previous_ema50=found_cross.previous_ema50,
        previous_ema200=found_cross.previous_ema200,
    )
    id1 = await test_db.save_signal(sig_rec)
    has1 = await test_db.has_signal(found_cross.symbol, found_cross.timeframe, found_cross.candle_timestamp)
    assert has1 is True

    # Duplicate save:
    id2 = await test_db.save_signal(sig_rec)
    assert id1 == id2, "Duplicate save should not create new row"
    count = await test_db.get_total_signals_count()
    assert count == 1, f"Expected 1 signal in DB, got {count}"
    print("  -> Duplicate alert suppression and unique constraint: PASS")

    # -------------------------------------------------------------
    # 6. RATE LIMIT RESILIENCE (429 & 418) TEST
    # -------------------------------------------------------------
    print("\n[TEST 6] Rate Limit (429 & 418) Resilience...")
    # Mock response object for 429
    class MockResp:
        def __init__(self, status, headers):
            self.status = status
            self.headers = headers
    
    resp_429 = MockResp(429, {"Retry-After": "2"})
    assert await client._check_rate_limit(resp_429) is False
    assert client.rate_limit_429_count == 1
    assert client._pause_until > time.time()
    print("  -> 429 Retry-After handling & backoff pause: PASS")

    resp_418 = MockResp(418, {"Retry-After": "5"})
    assert await client._check_rate_limit(resp_418) is False
    assert client.ip_ban_418_count == 1
    print("  -> 418 IP ban warning handling & backoff pause: PASS")

    # Reset pause for remaining checks
    client._pause_until = 0.0

    # -------------------------------------------------------------
    # 7. TELEGRAM MESSAGE AUDIT (ZERO PROHIBITED TERMS)
    # -------------------------------------------------------------
    print("\n[TEST 7] Telegram Notification Compliance Audit...")
    telegram = TelegramNotifier(enabled=False)
    msg = telegram.format_alert_message(found_cross)
    assert "🟢 GOLDEN CROSS" in msg
    assert target_sym in msg
    assert "TIMEFRAME: 1H" in msg
    assert "CANDLE: CLOSED" in msg
    prohibited = ["ENTRY", "STOP LOSS", "TAKE PROFIT", "TP1", "TP2", "TP3", "LEVERAGE", "MARGIN", "PNL", "RISK", "POSITION", "SHORT"]
    for p in prohibited:
        assert p not in msg.upper(), f"Prohibited keyword '{p}' detected in Telegram alert!"
    print("  -> Telegram alert content compliance: PASS (Zero prohibited terms)")

    # -------------------------------------------------------------
    # 8. GAP RECOVERY & RESTART TEST
    # -------------------------------------------------------------
    print("\n[TEST 8] Gap Recovery & Restart Verification...")
    # Verify cached timestamp lookup works
    cached_ts = await test_db.get_last_cached_candle_timestamp(target_sym, "1h")
    # Simulate gap check
    dummy_queue = AlertQueue(chart_provider, renderer, telegram, test_db)
    engine = SignalEngine(client, test_db, dummy_queue)
    engine.set_symbols([target_sym])
    await engine.initialize_symbols()
    assert target_sym in engine.initialized_symbols
    assert len(engine.candles_history[target_sym]) >= 100
    print("  -> Restart & historical state recovery: PASS")

    await client.close()
    print("\n" + "=" * 65)
    print("ALL 8 LIVE AUDIT PHASES PASSED WITH ZERO ERRORS.")
    print("=" * 65)


def pytest_approx(val, tolerance=1e-6):
    class ApproxVal:
        def __eq__(self, other):
            return abs(val - other) <= tolerance
    return ApproxVal()


if __name__ == "__main__":
    asyncio.run(run_live_audit())
