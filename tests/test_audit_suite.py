"""Comprehensive production audit and hardening test suite for NEXORA EMA CROSS."""

import asyncio
import os
import pytest
import pandas as pd
import numpy as np

from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.indicators.ema import (
    calculate_ema,
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)
from app.notifications.telegram_bot import TelegramNotifier
from app.persistence.database import Database
from app.persistence.models import SignalRecord


def test_ema_mathematical_precision():
    """Validates that EMA50 and EMA200 strictly use alpha = 2/(period+1) with adjust=False."""
    prices = [100.0, 102.0, 101.0, 103.0, 105.0]
    
    # Span 3: alpha = 2 / (3 + 1) = 0.5
    # EMA_0 = 100.0
    # EMA_1 = 102.0 * 0.5 + 100.0 * 0.5 = 101.0
    # EMA_2 = 101.0 * 0.5 + 101.0 * 0.5 = 101.0
    # EMA_3 = 103.0 * 0.5 + 101.0 * 0.5 = 102.0
    # EMA_4 = 105.0 * 0.5 + 102.0 * 0.5 = 103.5
    ema_span3 = calculate_ema(prices, span=3)
    expected_span3 = [100.0, 101.0, 101.0, 102.0, 103.5]
    for actual, exp in zip(ema_span3, expected_span3):
        assert actual == pytest.approx(exp, abs=1e-6)

    # Verify formula equivalence: EMA_t = Close_t * alpha + EMA_{t-1} * (1 - alpha)
    for span in [50, 200]:
        alpha = 2.0 / (span + 1.0)
        long_series = [100.0 + i * 0.5 for i in range(250)]
        res = calculate_ema(long_series, span=span)
        # Check recurrence from index 1 to 249
        for i in range(1, len(long_series)):
            step_calc = long_series[i] * alpha + res.iloc[i - 1] * (1.0 - alpha)
            assert res.iloc[i] == pytest.approx(step_calc, abs=1e-6)


@pytest.mark.asyncio
async def test_duplicate_alert_prevention(tmp_path):
    """Verifies that duplicate Golden Cross signals for the same candle are suppressed (Req 11)."""
    db_path = str(tmp_path / "test_dup.db")
    db = Database(db_path=db_path)
    await db.init()

    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    queue = AlertQueue(chart_data, renderer, telegram, db)
    queue.start()

    engine = SignalEngine(client, db, queue)
    engine.set_symbols(["BTCUSDT"])

    # Simulate candles leading to a Golden Cross on candle 2
    ts_cross = 1790000000000
    candle1 = {"timestamp": ts_cross - 3600000, "open": 100.0, "high": 105.0, "low": 98.0, "close": 99.0, "volume": 10.0}
    candle2 = {"timestamp": ts_cross, "open": 99.0, "high": 120.0, "low": 99.0, "close": 118.0, "volume": 50.0}

    # Seed history such that candle2 triggers Golden Cross
    engine.candles_history["BTCUSDT"] = [
        {"timestamp": ts_cross - 7200000, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0, "ema_50": 98.0, "ema_200": 99.0},
        {"timestamp": ts_cross - 3600000, "open": 99.0, "high": 100.0, "low": 97.0, "close": 98.0, "volume": 10.0, "ema_50": 98.5, "ema_200": 99.0},  # fast <= slow
    ]

    # First event trigger
    # Note: to create a clear cross, we can feed enriched rows directly or let handle_closed_candle enrich
    sig_record = SignalRecord(
        id=None,
        symbol="BTCUSDT",
        timeframe="1H",
        candle_timestamp=ts_cross,
        signal_time_utc="30 Sep 2026 • 12:00 UTC",
        ema50=100.5,
        ema200=99.8,
        close_price=118.0,
    )
    sig_id1 = await db.save_signal(sig_record)
    assert sig_id1 > 0

    # Second insert with exact same (symbol, timeframe, candle_timestamp)
    sig_id2 = await db.save_signal(sig_record)
    # Should update existing and return same id, not duplicate
    assert sig_id2 == sig_id1

    # Check total signals count in database
    total = await db.get_total_signals_count()
    assert total == 1

    # Check has_signal
    assert await db.has_signal("BTCUSDT", "1H", ts_cross) is True
    assert await db.has_signal("BTCUSDT", "1H", ts_cross + 3600000) is False

    await queue.stop()
    await client.close()


@pytest.mark.asyncio
async def test_restart_persistence(tmp_path):
    """Verifies that SQLite data and alert history survive application restart (Req 12)."""
    db_path = str(tmp_path / "test_restart.db")
    
    # 1. First run: save candles and signal
    db1 = Database(db_path=db_path)
    await db1.init()

    sig = SignalRecord(
        id=None,
        symbol="ETHUSDT",
        timeframe="1H",
        candle_timestamp=1780000000000,
        signal_time_utc="25 Sep 2026 • 10:00 UTC",
        ema50=2650.0,
        ema200=2600.0,
        close_price=2680.0,
    )
    await db1.save_signal(sig)
    
    # Cache some candles
    seed_candles = [
        {"timestamp": 1780000000000 + i * 3600000, "open": 2600.0, "high": 2700.0, "low": 2550.0, "close": 2650.0, "volume": 100.0, "ema_50": 2620.0, "ema_200": 2600.0}
        for i in range(20)
    ]
    await db1.cache_candles(seed_candles, "ETHUSDT", "1h")

    # 2. Simulate shutdown and second run with fresh Database instance
    db2 = Database(db_path=db_path)
    await db2.init()

    # Verify signal persisted
    signals = await db2.get_recent_signals(limit=10)
    assert len(signals) == 1
    assert signals[0]["symbol"] == "ETHUSDT"
    assert signals[0]["candle_timestamp"] == 1780000000000

    # Verify candle cache persisted
    cached = await db2.get_cached_candles("ETHUSDT", "1h", limit=50)
    assert len(cached) == 20
    assert cached[0]["timestamp"] == 1780000000000


def test_telegram_alert_formatting_and_no_prohibited_terms():
    """Verifies that Telegram alerts contain ONLY Golden Cross info and ZERO prohibited trading terms (Req 6 & 10)."""
    notifier = TelegramNotifier(enabled=False)
    signal = GoldenCrossSignal(
        symbol="SOLUSDT",
        timeframe="1H",
        candle_timestamp=1790000000000,
        signal_time_utc="30 Sep 2026 • 14:00 UTC",
        ema50=154.50,
        ema200=152.10,
        close_price=155.00,
        previous_ema50=151.90,
        previous_ema200=152.00,
        candle_status="CLOSED",
    )

    msg = notifier.format_alert_message(signal)
    
    # Must contain exact required fields
    assert "NEXORA EMA CROSS" in msg
    assert "🟢 GOLDEN CROSS" in msg
    assert "SYMBOL: SOLUSDT" in msg
    assert "TIMEFRAME: 1H" in msg
    assert "CANDLE: CLOSED" in msg
    assert "EMA50" in msg
    assert "EMA200" in msg
    assert "154.50" in msg

    # Must NOT contain prohibited terms
    prohibited_terms = [
        "ENTRY", "STOP LOSS", "TAKE PROFIT", "TP1", "TP2", "TP3",
        "LEVERAGE", "MARGIN", "PNL", "RISK", "POSITION", "SHORT",
        "WIN RATE", "BUY LIMIT", "ORDER", "RESEARCH", "OUTCOME",
    ]
    msg_upper = msg.upper()
    for term in prohibited_terms:
        assert term not in msg_upper, f"Prohibited trading term '{term}' found in Telegram alert!"


def test_chart_marker_exact_alignment():
    """Verifies that the Golden Cross marker timestamp exactly matches the signal candle timestamp (Req 29)."""
    candles = [
        {"timestamp": 1700000000000, "close": 100.0, "open": 100.0, "high": 101.0, "low": 99.0, "volume": 10.0, "ema_50": 98.0, "ema_200": 99.0},
        {"timestamp": 1700003600000, "close": 105.0, "open": 100.0, "high": 106.0, "low": 99.0, "volume": 15.0, "ema_50": 99.5, "ema_200": 99.2}, # Cross!
    ]
    df = pd.DataFrame(candles)
    crosses = find_all_golden_crosses(df, symbol="BTCUSDT")
    assert len(crosses) == 1
    # Marker timestamp must be EXACTLY candle timestamp
    assert crosses[0].candle_timestamp == 1700003600000


@pytest.mark.asyncio
async def test_live_vs_historical_signals_separation(tmp_path):
    """Verifies that historical scans do NOT trigger live alerts or spam Telegram."""
    db_path = str(tmp_path / "test_hist_vs_live.db")
    db = Database(db_path=db_path)
    await db.init()

    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    queue = AlertQueue(chart_data, renderer, telegram, db)
    queue.start()

    engine = SignalEngine(client, db, queue)
    engine.set_symbols(["BTCUSDT"])

    # Historical cross data
    ts_old = 1780000000000
    engine.candles_history["BTCUSDT"] = [
        {"timestamp": ts_old - 3600000, "close": 100.0, "open": 100.0, "high": 101.0, "low": 99.0, "volume": 10.0, "ema_50": 98.0, "ema_200": 99.0},
        {"timestamp": ts_old, "close": 105.0, "open": 100.0, "high": 106.0, "low": 99.0, "volume": 15.0, "ema_50": 99.5, "ema_200": 99.2},
    ]

    # Run historical scan
    crosses = await engine.scan_and_record_historical_crosses("BTCUSDT")
    assert len(crosses) == 1
    # Alert queue should NOT have received this historical signal
    assert queue._queue.qsize() == 0
    assert engine.live_signals_count == 0
    assert await db.get_live_signals_count() == 0
    assert await db.get_historical_signals_count() == 1

    # Now simulate a LIVE closed candle cross
    ts_live = 1790000000000
    live_candle = {
        "timestamp": ts_live,
        "open": 105.0,
        "high": 120.0,
        "low": 104.0,
        "close": 118.0,
        "volume": 50.0,
    }
    # Reset EMA so it crosses on this candle
    engine.candles_history["BTCUSDT"] = [
        {"timestamp": ts_live - 3600000, "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "volume": 10.0, "ema_50": 98.0, "ema_200": 99.0},
    ]
    # Feed closed candle
    live_sig = await engine.handle_closed_candle("BTCUSDT", live_candle)
    assert live_sig is not None
    assert engine.live_signals_count == 1
    assert await db.get_live_signals_count() == 1
    assert await db.get_total_signals_count() == 2

    await queue.stop()
    await client.close()


@pytest.mark.asyncio
async def test_telegram_safe_connectivity():
    """Verifies that missing Telegram configuration is safely detected without throwing or leaking."""
    notifier_missing = TelegramNotifier(bot_token="", chat_id="", enabled=False)
    res = await notifier_missing.verify_connectivity()
    assert res["status"] == "TELEGRAM CONFIGURATION MISSING"
    assert res["configured"] is False
    assert res["connected"] is False
    await notifier_missing.close()
