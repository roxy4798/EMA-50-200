"""Regression tests for NEXORA EMA CROSS historical scan optimization."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch
import pytest

from app.exchange.binance_client import BinanceFuturesClient
from app.indicators.ema import (
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)
from app.persistence.database import Database
from app.persistence.models import SignalRecord
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.notifications.telegram_bot import TelegramNotifier


def _generate_golden_cross_candles(ts_start: int = 1700000000000):
    """Generates synthetic candles that produce a single Golden Cross."""
    candles = []
    # 200 candles below
    for i in range(200):
        candles.append({
            "timestamp": ts_start + i * 3600000,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 10.0,
            "ema_50": 98.0,
            "ema_200": 99.0,
        })
    # Candle crossing over
    cross_ts = ts_start + 200 * 3600000
    candles.append({
        "timestamp": cross_ts,
        "open": 100.0,
        "high": 115.0,
        "low": 100.0,
        "close": 110.0,
        "volume": 25.0,
        "ema_50": 100.5,
        "ema_200": 99.8,
    })
    return candles


@pytest.mark.asyncio
async def test_historical_scan_uses_memory_only_and_no_binance_rest(tmp_path):
    """Verifies Requirement A & B: Historical scan uses ONLY in-memory candle history and makes ZERO Binance REST calls."""
    db = Database(db_path=str(tmp_path / "test_ab.db"))
    await db.init()

    mock_client = BinanceFuturesClient()
    mock_client.get_klines = AsyncMock()  # If called, test fails

    notifier = TelegramNotifier(enabled=False)
    chart_data = ChartDataProvider(mock_client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    alert_queue = AlertQueue(chart_data, renderer, notifier, db)

    engine = SignalEngine(mock_client, db, alert_queue)
    engine.candles_history["BTCUSDT"] = _generate_golden_cross_candles()

    crosses = await engine.scan_and_record_historical_crosses("BTCUSDT")

    assert len(crosses) >= 1
    # Verify ZERO calls to Binance REST client during historical scan
    mock_client.get_klines.assert_not_called()


@pytest.mark.asyncio
async def test_historical_scan_does_not_enqueue_alert_queue_and_is_live_zero(tmp_path):
    """Verifies Requirement C & D: Historical scan does NOT enqueue to AlertQueue and saves records with is_live=0."""
    db = Database(db_path=str(tmp_path / "test_cd.db"))
    await db.init()

    mock_client = BinanceFuturesClient()
    notifier = TelegramNotifier(enabled=False)
    chart_data = ChartDataProvider(mock_client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    alert_queue = AlertQueue(chart_data, renderer, notifier, db)
    alert_queue.start()

    engine = SignalEngine(mock_client, db, alert_queue)
    engine.candles_history["ETHUSDT"] = _generate_golden_cross_candles()

    crosses = await engine.scan_and_record_historical_crosses("ETHUSDT")
    assert len(crosses) >= 1

    # AlertQueue must have 0 items
    assert alert_queue._queue.qsize() == 0
    assert engine.live_signals_count == 0

    # Database records must have is_live = 0
    recent = await db.get_recent_signals(limit=10)
    assert len(recent) == len(crosses)
    for r in recent:
        assert r["is_live"] == 0
        assert r["telegram_sent"] == 0

    assert await db.get_live_signals_count() == 0
    assert await db.get_historical_signals_count() == len(crosses)

    await alert_queue.stop()


@pytest.mark.asyncio
async def test_duplicate_historical_signals_suppressed_by_unique_constraint(tmp_path):
    """Verifies Requirement E: Duplicate historical signals are suppressed by UNIQUE(symbol, timeframe, candle_timestamp)."""
    db = Database(db_path=str(tmp_path / "test_e.db"))
    await db.init()

    mock_client = BinanceFuturesClient()
    notifier = TelegramNotifier(enabled=False)
    chart_data = ChartDataProvider(mock_client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    alert_queue = AlertQueue(chart_data, renderer, notifier, db)

    engine = SignalEngine(mock_client, db, alert_queue)
    engine.candles_history["SOLUSDT"] = _generate_golden_cross_candles()

    # Pass 1
    crosses_1 = await engine.scan_and_record_historical_crosses("SOLUSDT")
    count_1 = await db.get_total_signals_count()
    assert count_1 == len(crosses_1)

    # Pass 2 - Idempotent re-scan
    crosses_2 = await engine.scan_and_record_historical_crosses("SOLUSDT")
    count_2 = await db.get_total_signals_count()
    assert count_2 == count_1, "Duplicate historical crosses must NOT insert additional DB rows"


@pytest.mark.asyncio
async def test_bounded_concurrency_scanning(tmp_path):
    """Verifies Requirement F: Multiple historical symbols are scanned concurrently with bounded concurrency."""
    db = Database(db_path=str(tmp_path / "test_f.db"))
    await db.init()

    mock_client = BinanceFuturesClient()
    notifier = TelegramNotifier(enabled=False)
    chart_data = ChartDataProvider(mock_client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    alert_queue = AlertQueue(chart_data, renderer, notifier, db)

    engine = SignalEngine(mock_client, db, alert_queue)
    symbols = [f"SYM{i}USDT" for i in range(12)]
    for s in symbols:
        engine.candles_history[s] = _generate_golden_cross_candles(ts_start=1700000000000)

    # Track concurrent executions
    current_concurrent = 0
    max_observed_concurrent = 0
    lock = asyncio.Lock()

    original_scan = engine.scan_and_record_historical_crosses

    async def monitored_scan(sym: str):
        nonlocal current_concurrent, max_observed_concurrent
        async with lock:
            current_concurrent += 1
            if current_concurrent > max_observed_concurrent:
                max_observed_concurrent = current_concurrent
        try:
            await asyncio.sleep(0.01)  # tiny yield to let concurrency build
            return await original_scan(sym)
        finally:
            async with lock:
                current_concurrent -= 1

    with patch.object(engine, "scan_and_record_historical_crosses", side_effect=monitored_scan):
        results = await engine.scan_historical_symbols(symbols, max_concurrency=4)

    assert len(results) == 12
    assert max_observed_concurrent <= 4, f"Observed concurrency {max_observed_concurrent} exceeded limit 4"
    assert max_observed_concurrent >= 2, "Expected concurrent execution"


@pytest.mark.asyncio
async def test_failed_historical_symbol_does_not_abort_others(tmp_path):
    """Verifies Requirement G: Failure in one historical symbol does NOT abort remaining symbols."""
    db = Database(db_path=str(tmp_path / "test_g.db"))
    await db.init()

    mock_client = BinanceFuturesClient()
    notifier = TelegramNotifier(enabled=False)
    chart_data = ChartDataProvider(mock_client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    alert_queue = AlertQueue(chart_data, renderer, notifier, db)

    engine = SignalEngine(mock_client, db, alert_queue)
    symbols = [f"SYM{i}USDT" for i in range(10)]
    for s in symbols:
        engine.candles_history[s] = _generate_golden_cross_candles()

    original_scan = engine.scan_and_record_historical_crosses

    async def faulty_scan(sym: str):
        if sym == "SYM3USDT":
            raise RuntimeError("Simulated network or corrupt data error for SYM3USDT")
        return await original_scan(sym)

    with patch.object(engine, "scan_and_record_historical_crosses", side_effect=faulty_scan):
        results = await engine.scan_historical_symbols(symbols, max_concurrency=5)

    # 10 symbols were attempted
    assert len(results) == 10
    # Faulty symbol returned empty list
    assert results["SYM3USDT"] == []
    # All other 9 symbols succeeded
    for i in [0, 1, 2, 4, 5, 6, 7, 8, 9]:
        assert len(results[f"SYM{i}USDT"]) >= 1


@pytest.mark.asyncio
async def test_live_signal_processing_remains_unchanged(tmp_path):
    """Verifies Requirement H: Live signal processing (is_live=1, AlertQueue, Telegram) remains completely functional."""
    db = Database(db_path=str(tmp_path / "test_h.db"))
    await db.init()

    mock_client = BinanceFuturesClient()
    notifier = TelegramNotifier(enabled=False)
    chart_data = ChartDataProvider(mock_client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    alert_queue = AlertQueue(chart_data, renderer, notifier, db)
    alert_queue.start()

    engine = SignalEngine(mock_client, db, alert_queue)
    # Populate historical baseline just before cross
    ts_base = 1750000000000
    engine.candles_history["BTCUSDT"] = [
        {"timestamp": ts_base, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10.0, "ema_50": 98.0, "ema_200": 99.0},
    ]

    # Incoming live closed candle crossing EMA50 above EMA200
    live_candle = {
        "timestamp": ts_base + 3600000,
        "open": 100.0,
        "high": 120.0,
        "low": 100.0,
        "close": 115.0,
        "volume": 50.0,
    }

    sig = await engine.handle_closed_candle("BTCUSDT", live_candle)
    assert sig is not None
    assert sig.symbol == "BTCUSDT"

    # Enqueued into alert queue
    assert alert_queue._queue.qsize() == 1
    assert engine.live_signals_count == 1

    # In database as is_live = 1
    recent = await db.get_recent_signals(limit=5)
    assert len(recent) == 1
    assert recent[0]["is_live"] == 1
    assert await db.get_live_signals_count() == 1

    await alert_queue.stop()


def test_golden_cross_detection_logic_invariance():
    """Verifies Requirement I: Golden Cross formula remains strictly prev_fast <= prev_slow and curr_fast > curr_slow."""
    import pandas as pd

    # 1. Valid Golden Cross
    df_valid = pd.DataFrame([
        {"timestamp": 1000, "close": 100.0, "ema_50": 99.0, "ema_200": 100.0},
        {"timestamp": 2000, "close": 105.0, "ema_50": 101.0, "ema_200": 100.5},
    ])
    assert detect_golden_cross(df_valid, "BTCUSDT") is not None

    # 2. Equal previous (still Golden Cross since <=)
    df_equal_prev = pd.DataFrame([
        {"timestamp": 1000, "close": 100.0, "ema_50": 100.0, "ema_200": 100.0},
        {"timestamp": 2000, "close": 105.0, "ema_50": 101.0, "ema_200": 100.5},
    ])
    assert detect_golden_cross(df_equal_prev, "BTCUSDT") is not None

    # 3. Already bullish (NOT a Golden Cross)
    df_already_bull = pd.DataFrame([
        {"timestamp": 1000, "close": 100.0, "ema_50": 101.0, "ema_200": 100.0},
        {"timestamp": 2000, "close": 105.0, "ema_50": 102.0, "ema_200": 100.5},
    ])
    assert detect_golden_cross(df_already_bull, "BTCUSDT") is None

    # 4. Bearish cross / Death cross (NOT a Golden Cross)
    df_death = pd.DataFrame([
        {"timestamp": 1000, "close": 100.0, "ema_50": 101.0, "ema_200": 100.0},
        {"timestamp": 2000, "close": 90.0, "ema_50": 98.0, "ema_200": 99.0},
    ])
    assert detect_golden_cross(df_death, "BTCUSDT") is None
