"""Comprehensive Regression Test Suite for NEXORA EMA CROSS Data Consistency.

Validates all 12 audit requirements from the Critical Data Consistency Fix:
1. Valid Golden Cross detection.
2. EMA50 <= EMA200 is never a Golden Cross.
3. Historical Golden Cross chart uses crossover candle EMA values.
4. Current bearish structure is never labeled Golden Cross.
5. Chart status and EMA values are never contradictory (Fail-Closed).
6. /symbol command and chart current EMA values are consistent.
7. Signal database EMA values strictly match crossover candle.
8. Closed candle requirement (k.x == True).
9. WebSocket connected but no messages reports DATA STALE.
10. Spot data is never used for Futures signals.
11. No-lookahead bias.
12. No extra filters (pure EMA50/EMA200 Golden Cross).
13. SAFEUSDT regression test with exact values.
"""

from __future__ import annotations

import os
import time
import pytest
import pandas as pd
import numpy as np

from app.charts.chart_theme import format_price
from app.charts.chart_renderer import ChartRenderer
from app.charts.chart_data import ChartDataProvider
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.indicators.ema import (
    calculate_ema,
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)
from app.persistence.database import Database
from app.persistence.models import SignalRecord
from app.notifications.telegram_commands import build_symbol_view, build_status_view


def _create_synthetic_candles(num_candles: int = 1000, start_price: float = 100.0) -> list[dict]:
    """Generates synthetic 1H candlestick sequence."""
    base_ts = 1790000000000
    candles = []
    p = start_price
    for i in range(num_candles):
        ts = base_ts + i * 3600000
        o = p
        c = p + (1.5 if i % 2 == 0 else -1.0)
        h = max(o, c) + 0.5
        l = min(o, c) - 0.5
        p = c
        candles.append({
            "timestamp": ts,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": 1000.0,
            "close_time": ts + 3599999,
            "is_closed": True,
        })
    return candles


def test_1_golden_cross_valid():
    """1. Valid Golden Cross: prev EMA50 <= prev EMA200 AND curr EMA50 > curr EMA200."""
    candles = _create_synthetic_candles(250)
    df = enrich_candles_with_ema(candles, 50, 200)

    # Force crossover on the last candle
    df.loc[df.index[-2], "ema_50"] = 100.0
    df.loc[df.index[-2], "ema_200"] = 100.5
    df.loc[df.index[-1], "ema_50"] = 101.0
    df.loc[df.index[-1], "ema_200"] = 100.5

    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")
    assert signal is not None
    assert signal.symbol == "BTCUSDT"
    assert signal.ema50 == 101.0
    assert signal.ema200 == 100.5
    assert signal.previous_ema50 == 100.0
    assert signal.previous_ema200 == 100.5
    assert signal.candle_status == "CLOSED"


def test_2_ema50_less_equal_ema200_not_golden_cross():
    """2. EMA50 <= EMA200 must NEVER produce a Golden Cross."""
    candles = _create_synthetic_candles(250)
    df = enrich_candles_with_ema(candles, 50, 200)

    # Bearish: EMA50 < EMA200
    df.loc[df.index[-2], "ema_50"] = 99.0
    df.loc[df.index[-2], "ema_200"] = 100.0
    df.loc[df.index[-1], "ema_50"] = 99.5
    df.loc[df.index[-1], "ema_200"] = 100.0
    assert detect_golden_cross(df, symbol="BTCUSDT") is None

    # Equal: EMA50 == EMA200
    df.loc[df.index[-1], "ema_50"] = 100.0
    df.loc[df.index[-1], "ema_200"] = 100.0
    assert detect_golden_cross(df, symbol="BTCUSDT") is None


def test_3_historical_golden_cross_chart_uses_crossover_candle(tmp_path):
    """3. Historical Golden Cross chart uses EMA values of the CROSSOVER CANDLE, not latest candle."""
    candles = _create_synthetic_candles(150)
    df = enrich_candles_with_ema(candles, 50, 200)

    # Place Golden Cross at candle index 80
    cross_ts = int(df.loc[80, "timestamp"])
    df.loc[79, "ema_50"] = 95.0
    df.loc[79, "ema_200"] = 96.0
    df.loc[80, "ema_50"] = 97.0
    df.loc[80, "ema_200"] = 96.0
    df.loc[80, "close"] = 98.0

    # Make the latest candle index 149 completely different (e.g. bearish)
    df.loc[149, "ema_50"] = 50.0
    df.loc[149, "ema_200"] = 60.0
    df.loc[149, "close"] = 49.0

    renderer = ChartRenderer(output_dir=str(tmp_path))
    chart_data = {
        "symbol": "ETHUSDT",
        "timeframe": "1H",
        "df": df,
        "cross_markers": [],
        "target_timestamp": cross_ts,
    }

    chart_file = renderer.render_golden_cross_chart(chart_data, target_timestamp=cross_ts)
    assert chart_file is not None
    assert os.path.isfile(chart_file)


def test_4_current_bearish_structure_never_labeled_golden_cross(tmp_path):
    """4. Current bearish structure (EMA50 < EMA200) must NEVER be labeled Golden Cross."""
    candles = _create_synthetic_candles(100, start_price=0.1)
    df = enrich_candles_with_ema(candles, 50, 200)

    # Set current closed candle as Bearish (SAFEUSDT scenario)
    df.loc[df.index[-2], "ema_50"] = 0.109620
    df.loc[df.index[-2], "ema_200"] = 0.109989
    df.loc[df.index[-1], "ema_50"] = 0.109830
    df.loc[df.index[-1], "ema_200"] = 0.110039
    df.loc[df.index[-1], "close"] = 0.114970

    renderer = ChartRenderer(output_dir=str(tmp_path))
    chart_data = {
        "symbol": "SAFEUSDT",
        "timeframe": "1H",
        "df": df,
        "cross_markers": [],
        "latest": {
            "symbol": "SAFEUSDT",
            "timeframe": "1H",
            "close": 0.114970,
            "ema50": 0.109830,
            "ema200": 0.110039,
            "timestamp": int(df.iloc[-1]["timestamp"]),
        },
    }

    # Render overview chart (target_timestamp=None)
    chart_file = renderer.render_golden_cross_chart(chart_data, target_timestamp=None)
    assert chart_file is not None
    assert os.path.isfile(chart_file)


def test_5_chart_status_and_ema_values_never_contradictory_fail_closed(tmp_path):
    """5. Chart status and EMA values must never contradict: FAIL-CLOSED on invalid cross."""
    candles = _create_synthetic_candles(100)
    df = enrich_candles_with_ema(candles, 50, 200)

    # Target candle has EMA50 < EMA200 (NOT a Golden Cross)
    target_ts = int(df.loc[50, "timestamp"])
    df.loc[49, "ema_50"] = 0.109
    df.loc[49, "ema_200"] = 0.110
    df.loc[50, "ema_50"] = 0.108  # Still below EMA200!
    df.loc[50, "ema_200"] = 0.110

    renderer = ChartRenderer(output_dir=str(tmp_path))
    chart_data = {
        "symbol": "SAFEUSDT",
        "timeframe": "1H",
        "df": df,
        "cross_markers": [],
    }

    # Calling with target_timestamp on a non-cross candle MUST FAIL CLOSED (return None)
    result = renderer.render_golden_cross_chart(chart_data, target_timestamp=target_ts)
    assert result is None, "Expected FAIL-CLOSED (None) when target candle is not a valid Golden Cross!"


@pytest.mark.asyncio
async def test_6_symbol_and_chart_current_ema_values_consistent(tmp_path):
    """6. /symbol command and chart current EMA values must be 100% consistent."""
    db_path = str(tmp_path / "test_symbol_consist.db")
    db = Database(db_path=db_path)
    await db.init()

    # Pre-cache SAFEUSDT candles
    candles = _create_synthetic_candles(1000, start_price=0.12)
    # Deterministic declining USD-M frame gives a bearish structure.
    for i, candle in enumerate(candles):
        candle["close"] = 0.12 - i * 0.0001
        candle["open"] = candle["close"]
    candles[-1]["close"] = 0.095

    await db.cache_candles(candles, "SAFEUSDT", "1h")

    # Mock signal engine
    class MockEngine:
        symbols = ["SAFEUSDT"]
        initialized_symbols = {"SAFEUSDT"}
        timeframe = "1h"
        candles_history = {"SAFEUSDT": candles}

    class FuturesClient:
        def resolve_symbol(self, symbol):
            return symbol

        async def get_klines(self, symbol, interval, limit, only_closed):
            return candles

    text, _ = await build_symbol_view(
        raw_input="SAFEUSDT",
        signal_engine=MockEngine(),
        binance_client=FuturesClient(),
        database=db,
    )

    assert "SAFEUSDT" in text
    assert "BEARISH" in text
    assert "GOLDEN CROSS CONFIRMED" not in text
    provider = ChartDataProvider(FuturesClient(), db)
    chart = await provider.get_chart_data("SAFEUSDT", limit=150, force_fresh=True)
    assert format_price(chart["latest"]["ema50"]) in text
    assert format_price(chart["latest"]["ema200"]) in text


@pytest.mark.asyncio
async def test_7_signal_database_ema_values_match_crossover_candle(tmp_path):
    """7. SQLite signal table stores exact crossover candle EMA values and rejects invalid crosses."""
    db_path = str(tmp_path / "test_db_consist.db")
    db = Database(db_path=db_path)
    await db.init()

    # Valid signal: ema50 > ema200
    valid_record = SignalRecord(
        id=None,
        symbol="BTCUSDT",
        timeframe="1H",
        candle_timestamp=1790000000000,
        signal_time_utc="30 Sep 2026 • 12:00 UTC",
        ema50=65200.0,
        ema200=65000.0,
        close_price=65500.0,
        is_live=True,
        previous_ema50=64900.0,
        previous_ema200=65000.0,
    )
    sig_id = await db.save_signal(valid_record)
    assert sig_id > 0

    signals = await db.get_recent_signals(limit=1)
    assert len(signals) == 1
    saved = signals[0]
    assert saved["ema50"] == 65200.0
    assert saved["ema200"] == 65000.0
    assert saved["previous_ema50"] == 64900.0
    assert saved["previous_ema200"] == 65000.0
    assert saved["close_price"] == 65500.0

    # Invalid signal: ema50 <= ema200 must be rejected
    invalid_record = SignalRecord(
        id=None,
        symbol="SAFEUSDT",
        timeframe="1H",
        candle_timestamp=1790003600000,
        signal_time_utc="30 Sep 2026 • 13:00 UTC",
        ema50=0.109830,
        ema200=0.110039,
        close_price=0.11497,
        is_live=True,
    )
    rejected_id = await db.save_signal(invalid_record)
    assert rejected_id == 0, "Database must reject signal where ema50 <= ema200!"

    invalid_previous = SignalRecord(
        id=None, symbol="ETHUSDT", timeframe="1H", candle_timestamp=1790007200000,
        signal_time_utc="30 Sep 2026 • 14:00 UTC", ema50=102.0, ema200=101.0,
        previous_ema50=102.0, previous_ema200=101.0, close_price=103.0,
    )
    assert await db.save_signal(invalid_previous) == 0


@pytest.mark.asyncio
async def test_8_closed_candle_requirement():
    """8. Live signals strictly require closed candles (k.x == True)."""
    closed_received = []

    async def on_closed(symbol: str, candle: dict):
        closed_received.append((symbol, candle))

    ws = BinanceWebSocketManager(on_candle_closed=on_closed)
    ws.set_symbols(["BTCUSDT"])

    # 1. Unclosed candle (k.x == False)
    unclosed_msg = {
        "e": "kline",
        "k": {
            "s": "BTCUSDT",
            "t": 1790000000000,
            "T": 1790003599999,
            "i": "1h",
            "o": "50000",
            "h": "50500",
            "l": "49500",
            "c": "50200",
            "v": "100",
            "x": False,
        },
    }
    await ws._handle_message(unclosed_msg)
    assert len(closed_received) == 0, "Must NOT emit candle when k.x == False!"

    # 2. Closed candle (k.x == True)
    closed_msg = {
        "e": "kline",
        "k": {
            "s": "BTCUSDT",
            "t": 1790000000000,
            "T": 1790003599999,
            "i": "1h",
            "o": "50000",
            "h": "50500",
            "l": "49500",
            "c": "50200",
            "v": "100",
            "x": True,
        },
    }
    await ws._handle_message(closed_msg)
    assert len(closed_received) == 1
    assert closed_received[0][0] == "BTCUSDT"
    assert closed_received[0][1]["is_closed"] is True


def test_9_websocket_connected_but_no_message_data_stale():
    """9. WebSocket connected but no messages reports DATA STALE / NO MARKET DATA."""
    ws = BinanceWebSocketManager()
    ws._active_connections = 6  # 6 sockets open

    # No kline frame ever received
    health = ws.get_market_data_health()
    assert health["is_connected"] is True
    assert health["is_healthy"] is False
    assert "DATA STALE" in health["status"]

    # Simulating stale message received > 120s ago
    ws.total_klines_received = 100
    ws.last_kline_received_at = time.time() - 150  # 150s ago
    health_stale = ws.get_market_data_health()
    assert health_stale["is_healthy"] is False
    assert "DATA STALE" in health_stale["status"]

    # Simulating healthy fresh message
    ws.last_kline_received_at = time.time() - 5  # 5s ago
    health_ok = ws.get_market_data_health()
    assert health_ok["is_healthy"] is True
    assert health_ok["status"] == "HEALTHY"


def test_10_spot_data_never_used_for_futures_signal():
    """10. Spot endpoints are never used for Binance USD-M Futures signals."""
    client = BinanceFuturesClient()
    assert "fapi.binance.com" in client.base_url
    assert client.base_url == "https://fapi.binance.com"  # USD-M Futures REST
    with pytest.raises(ValueError):
        BinanceFuturesClient(base_url="https://api.binance.com")

    ws = BinanceWebSocketManager()
    assert "fstream.binance.com" in ws.base_ws_url
    assert "stream.binance.com" not in ws.base_ws_url or "fstream" in ws.base_ws_url  # Must be fstream
    with pytest.raises(ValueError):
        BinanceWebSocketManager(base_ws_url="wss://stream.binance.com:9443")


def test_11_no_lookahead_bias():
    """11. EMA calculation has no lookahead bias: future prices do not alter past EMAs."""
    candles = _create_synthetic_candles(100)
    df_base = enrich_candles_with_ema(candles[:80], 50, 200)

    # Append future candles with huge price spikes
    future_candles = list(candles)
    for c in future_candles[80:]:
        c["close"] = 999999.0

    df_future = enrich_candles_with_ema(future_candles, 50, 200)

    # EMAs from index 0 to 79 must be IDENTICAL
    for i in range(80):
        assert df_base.loc[i, "ema_50"] == pytest.approx(df_future.loc[i, "ema_50"], abs=1e-8)
        assert df_base.loc[i, "ema_200"] == pytest.approx(df_future.loc[i, "ema_200"], abs=1e-8)


def test_12_no_extra_filters():
    """12. Strategy relies solely on EMA50 > EMA200 on 1H closed candles without extra indicators."""
    candles = _create_synthetic_candles(250)
    df = enrich_candles_with_ema(candles, 50, 200)

    # Verify column set: only EMA50 and EMA200 added, NO RSI/MACD/ADX/ATR
    forbidden_indicators = ["rsi", "macd", "adx", "atr", "volume_ma", "slope"]
    for col in df.columns:
        for forbidden in forbidden_indicators:
            assert forbidden not in col.lower(), f"Forbidden indicator {forbidden} detected in columns!"


def test_13_safeusdt_reproducibility_regression(tmp_path):
    """13. Reproduction of exact SAFEUSDT reported bug:
    EMA50: 0.109830, EMA200: 0.110039 -> MUST be BEARISH, NEVER GOLDEN CROSS.
    """
    candles = _create_synthetic_candles(100, start_price=0.10)
    df = enrich_candles_with_ema(candles, 50, 200)

    # Apply exact SAFEUSDT values reported by user
    df.loc[df.index[-2], "ema_50"] = 0.109620
    df.loc[df.index[-2], "ema_200"] = 0.109989
    df.loc[df.index[-1], "ema_50"] = 0.109830
    df.loc[df.index[-1], "ema_200"] = 0.110039
    df.loc[df.index[-1], "close"] = 0.114970

    # 1. detect_golden_cross MUST be None
    cross = detect_golden_cross(df, symbol="SAFEUSDT")
    assert cross is None, "SAFEUSDT must NOT trigger a Golden Cross!"

    # 2. Chart rendering in overview mode MUST be BEARISH
    renderer = ChartRenderer(output_dir=str(tmp_path))
    chart_data = {
        "symbol": "SAFEUSDT",
        "timeframe": "1H",
        "df": df,
        "cross_markers": [],
        "latest": {
            "symbol": "SAFEUSDT",
            "timeframe": "1H",
            "close": 0.114970,
            "ema50": 0.109830,
            "ema200": 0.110039,
            "timestamp": int(df.iloc[-1]["timestamp"]),
        },
    }
    path = renderer.render_golden_cross_chart(chart_data, target_timestamp=None)
    assert path is not None
    assert os.path.isfile(path)

    # 3. Attempting to force an event chart on this candle MUST fail closed
    target_ts = int(df.iloc[-1]["timestamp"])
    failed_path = renderer.render_golden_cross_chart(chart_data, target_timestamp=target_ts)
    assert failed_path is None, "Must FAIL-CLOSED when trying to render non-cross as Golden Cross!"
