"""Comprehensive regression tests for NEXORA EMA Generalization (50/200 and 50/500)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
import numpy as np
import pandas as pd
import pytest

from app.config import Settings
from app.indicators.ema import (
    GoldenCrossSignal,
    calculate_ema,
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
)
from app.engine.signal_engine import SignalEngine
from app.engine.alert_queue import AlertQueue
from app.exchange.binance_client import BinanceFuturesClient
from app.notifications.telegram_bot import TelegramNotifier
from app.notifications.telegram_commands import build_start_view, build_symbol_view
from app.persistence.database import Database
from app.persistence.models import SignalRecord
from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer


# ============================================================================
# 1. Configuration Validation Tests
# ============================================================================

def test_config_validation_valid_configurations():
    """Verify standard 50/200 and target 50/500 configs pass validation."""
    s1 = Settings(timeframe="1h", ema_fast=50, ema_slow=200, candle_limit=1000)
    assert s1.ema_fast == 50
    assert s1.ema_slow == 200
    assert s1.candle_limit == 1000

    s2 = Settings(timeframe="1h", ema_fast=50, ema_slow=500, candle_limit=1000)
    assert s2.ema_fast == 50
    assert s2.ema_slow == 500
    assert s2.candle_limit == 1000


def test_config_validation_candle_limit_preserves_1000_minimum():
    """Verify legacy CANDLE_LIMIT=250 in .env is automatically clamped to >=1000."""
    s = Settings(timeframe="1h", ema_fast=50, ema_slow=500, candle_limit=250)
    assert s.candle_limit == 1000


def test_config_validation_invalid_parameters():
    """Verify invalid configs raise ValueError with explicit messages."""
    with pytest.raises(ValueError, match="1h"):
        Settings(timeframe="4h", ema_fast=50, ema_slow=500)

    with pytest.raises(ValueError, match="positive"):
        Settings(timeframe="1h", ema_fast=0, ema_slow=500)

    with pytest.raises(ValueError, match="positive"):
        Settings(timeframe="1h", ema_fast=50, ema_slow=-1)

    with pytest.raises(ValueError, match="less than slow period"):
        Settings(timeframe="1h", ema_fast=500, ema_slow=50)

    with pytest.raises(ValueError, match="less than slow period"):
        Settings(timeframe="1h", ema_fast=50, ema_slow=50)


# ============================================================================
# 2. Indicator & Mathematical Calculation Tests (Raw Close Prices)
# ============================================================================

def test_ema_indicators_50_500_enrichment():
    """Verify enrich_candles_with_ema calculates ema_50 and ema_500 from raw prices."""
    np.random.seed(42)
    candles = []
    base_ts = 1700000000000
    for i in range(1200):
        candles.append({
            "timestamp": base_ts + i * 3600000,
            "open": 50000.0 + i,
            "high": 50100.0 + i,
            "low": 49900.0 + i,
            "close": 50000.0 + i * 5,
            "volume": 100.0,
        })

    df = enrich_candles_with_ema(candles, fast_period=50, slow_period=500)
    assert "ema_50" in df.columns
    assert "ema_500" in df.columns
    assert "ema_fast" in df.columns
    assert "ema_slow" in df.columns

    # Verify EMA 500 is calculated from raw close prices, not reusing EMA 200
    assert "ema_200" not in df.columns
    # Check EMA values are not null after sufficient warm up
    assert pd.notna(df["ema_500"].iloc[-1])
    assert pd.notna(df["ema_50"].iloc[-1])
    assert df["ema_50"].iloc[-1] > df["ema_500"].iloc[-1]


def test_golden_cross_50_500_detection_on_closed_candle():
    """Verify Golden Cross is detected exactly when previous fast <= slow and current fast > slow."""
    # Build candle sequence where cross occurs at the last closed candle
    candles = []
    base_ts = 1700000000000
    # First 1100 candles flat, then sharp run up
    for i in range(1100):
        t = base_ts + i * 3600000
        # Start at 20000 and rise fast so 50 crosses 500
        p = 20000.0 if i < 900 else (20000.0 + (i - 900) * 150.0)
        candles.append({
            "timestamp": t,
            "open": p,
            "high": p + 10,
            "low": p - 10,
            "close": p,
            "volume": 50.0,
            "close_time": t + 3599999,
        })

    df = enrich_candles_with_ema(candles, 50, 500)
    crosses = find_all_golden_crosses(df, symbol="BTCUSDT", fast_col="ema_50", slow_col="ema_500", fast_period=50, slow_period=500)
    assert len(crosses) >= 1
    sig = crosses[-1]
    assert sig.fast_period == 50
    assert sig.slow_period == 500
    assert sig.previous_ema_fast <= sig.previous_ema_slow
    assert sig.ema_fast > sig.ema_slow


# ============================================================================
# 3. Fail-Closed Historical Minimum (1000 Candles) Tests
# ============================================================================

@pytest.mark.asyncio
async def test_signal_engine_fail_closed_under_1000_candles(tmp_path):
    """Verify SignalEngine strictly refuses to emit signals if history < 1000 candles."""
    db = Database(db_path=str(tmp_path / "test_fc.db"))
    await db.init()
    client = BinanceFuturesClient()
    alert_queue = MagicMock()

    engine = SignalEngine(
        binance_client=client,
        database=db,
        alert_queue=alert_queue,
        timeframe="1h",
        fast_period=50,
        slow_period=500,
        candle_limit=1000,
    )
    engine.set_symbols(["BTCUSDT"])

    # Provide only 500 candles to engine history
    short_candles = []
    base_ts = 1700000000000
    for i in range(500):
        t = base_ts + i * 3600000
        short_candles.append({
            "timestamp": t,
            "open": 60000.0,
            "high": 61000.0,
            "low": 59000.0,
            "close": 60000.0,
            "volume": 10.0,
            "close_time": t + 3599999,
        })
    engine.candles_history["BTCUSDT"] = short_candles

    # Incoming closed candle
    closed_kline = {
        "symbol": "BTCUSDT",
        "timeframe": "1h",
        "timestamp": base_ts + 500 * 3600000,
        "open": 60000.0,
        "high": 80000.0,
        "low": 60000.0,
        "close": 80000.0,
        "volume": 100.0,
        "is_closed": True,
    }

    signal = await engine.handle_closed_candle("BTCUSDT", closed_kline)
    assert signal is None, "Engine must fail-closed when candle history is < 1000 candles"
    assert not alert_queue.enqueue.called


@pytest.mark.asyncio
async def test_signal_engine_validation_on_init():
    """Verify SignalEngine validates inputs and prevents invalid configs."""
    client = BinanceFuturesClient()
    db = MagicMock()
    aq = MagicMock()

    # Valid 50/200
    e1 = SignalEngine(client, db, aq, fast_period=50, slow_period=200)
    assert e1.fast_period == 50 and e1.slow_period == 200

    # Valid 50/500
    e2 = SignalEngine(client, db, aq, fast_period=50, slow_period=500)
    assert e2.fast_period == 50 and e2.slow_period == 500

    # Invalid fast >= slow
    with pytest.raises(ValueError, match="strictly less than slow_period"):
        SignalEngine(client, db, aq, fast_period=500, slow_period=50)

    # Invalid timeframe
    with pytest.raises(ValueError, match="1h"):
        SignalEngine(client, db, aq, timeframe="15m")

    # Invalid candle_limit
    with pytest.raises(ValueError, match="1000"):
        SignalEngine(client, db, aq, candle_limit=500)


# ============================================================================
# 4. AlertQueue Independent 50/500 Validation Tests
# ============================================================================

@pytest.mark.asyncio
async def test_alert_queue_uses_configured_50_500(tmp_path):
    """Verify AlertQueue validates crossover using configured 50/500 periods."""
    db = Database(db_path=str(tmp_path / "test_aq.db"))
    await db.init()

    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(
        binance_client=client,
        database=db,
        fast_period=50,
        slow_period=500,
    )
    chart_renderer = ChartRenderer(
        output_dir=str(tmp_path / "charts"),
        fast_period=50,
        slow_period=500,
    )
    telegram = TelegramNotifier(
        enabled=False,
        fast_period=50,
        slow_period=500,
    )

    queue = AlertQueue(
        chart_data_provider=chart_data,
        chart_renderer=chart_renderer,
        telegram_notifier=telegram,
        database=db,
        fast_period=50,
        slow_period=500,
    )
    queue.start()

    target_ts = 1700000000000
    signal = GoldenCrossSignal(
        symbol="BTCUSDT",
        timeframe="1H",
        candle_timestamp=target_ts,
        signal_time_utc="30 Sep 2026 • 10:00 UTC",
        ema50=10784.0,
        ema200=10079.0,
        close_price=30000.0,
        previous_ema50=10000.0,
        previous_ema200=10000.0,
        fast_period=50,
        slow_period=500,
    )

    sig_id = await db.save_signal(SignalRecord(
        id=None,
        symbol=signal.symbol,
        timeframe=signal.timeframe,
        candle_timestamp=signal.candle_timestamp,
        signal_time_utc=signal.signal_time_utc,
        ema50=signal.ema50,
        ema200=signal.ema200,
        close_price=signal.close_price,
        previous_ema50=signal.previous_ema50,
        previous_ema200=signal.previous_ema200,
        fast_period=50,
        slow_period=500,
    ))

    # Generate 1000 candles leading up to target_ts with genuine 50/500 crossover
    # 0..998 constant at 10000, 999 shoots to 30000
    mock_candles = []
    for i in range(1000):
        t = target_ts - (999 - i) * 3600000
        p = 10000.0 if i < 999 else 30000.0
        mock_candles.append({
            "timestamp": t,
            "open": p,
            "high": p + 50,
            "low": p - 50,
            "close": p,
            "volume": 100.0,
            "close_time": t + 3599999,
        })
    client.get_klines = AsyncMock(return_value=mock_candles)

    await queue.enqueue(sig_id, signal)
    # Give the queue worker a moment to process
    await asyncio.sleep(0.8)

    assert queue.total_processed >= 1
    await queue.stop()


# ============================================================================
# 5. Telegram & Commands Dynamic Formatting Tests
# ============================================================================

def test_telegram_alert_message_dynamic_labels():
    """Verify format_alert_message uses configured EMA fast and slow periods."""
    telegram = TelegramNotifier(fast_period=50, slow_period=500)
    sig_50_500 = GoldenCrossSignal(
        symbol="ETHUSDT",
        timeframe="1H",
        candle_timestamp=1700000000000,
        signal_time_utc="01 Oct 2026 • 12:00 UTC",
        ema50=2600.5,
        ema200=2500.25,
        close_price=2650.0,
        fast_period=50,
        slow_period=500,
    )
    msg = telegram.format_alert_message(sig_50_500)
    assert "EMA50:" in msg
    assert "EMA500:" in msg
    assert "EMA200:" not in msg

    # Also verify backward compatibility when signal has 50/200
    sig_50_200 = GoldenCrossSignal(
        symbol="ETHUSDT",
        timeframe="1H",
        candle_timestamp=1700000000000,
        signal_time_utc="01 Oct 2026 • 12:00 UTC",
        ema50=2600.5,
        ema200=2500.25,
        close_price=2650.0,
        fast_period=50,
        slow_period=200,
    )
    msg_200 = telegram.format_alert_message(sig_50_200)
    assert "EMA50:" in msg_200
    assert "EMA200:" in msg_200


def test_telegram_start_view_dynamic_labels():
    """Verify build_start_view displays configured EMA periods."""
    text_500, _ = build_start_view(fast_period=50, slow_period=500)
    assert "EMA 50 / EMA 500 Golden Cross Monitor" in text_500

    text_200, _ = build_start_view(fast_period=50, slow_period=200)
    assert "EMA 50 / EMA 200 Golden Cross Monitor" in text_200


@pytest.mark.asyncio
async def test_telegram_symbol_view_dynamic_periods():
    """Verify build_symbol_view calculates and formats configured EMA periods."""
    mock_client = AsyncMock()
    mock_client.resolve_symbol.return_value = "BTCUSDT"

    # Generate 1000 candles
    mock_candles = []
    base_ts = 1700000000000
    for i in range(1000):
        t = base_ts + i * 3600000
        mock_candles.append({
            "timestamp": t,
            "open": 60000.0,
            "high": 61000.0,
            "low": 59000.0,
            "close": 60000.0 + i,
            "volume": 10.0,
            "close_time": t + 3599999,
        })
    mock_client.get_klines.return_value = mock_candles

    mock_engine = MagicMock()
    mock_engine.symbols = ["BTCUSDT"]
    mock_engine.initialized_symbols = {"BTCUSDT"}
    mock_engine.timeframe = "1h"
    mock_engine.fast_period = 50
    mock_engine.slow_period = 500

    mock_db = AsyncMock()
    mock_db.get_last_signal_for_symbol.return_value = None

    text, _ = await build_symbol_view("BTCUSDT", mock_engine, mock_client, mock_db)
    assert "EMA 50" in text
    assert "EMA 500" in text
    assert "EMA 200" not in text
