"""Unit tests for EMA indicator and Golden Cross detection."""

import pytest
import pandas as pd
import numpy as np

from app.indicators.ema import (
    calculate_ema,
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)


def test_calculate_ema():
    # Test on a known sequence
    prices = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
    ema = calculate_ema(prices, span=3)
    assert len(ema) == len(prices)
    # EMA formula: alpha = 2/(span+1) = 2/4 = 0.5
    # ema[0] = 10.0
    # ema[1] = 11*0.5 + 10*0.5 = 10.5
    # ema[2] = 12*0.5 + 10.5*0.5 = 11.25
    assert ema.iloc[0] == pytest.approx(10.0)
    assert ema.iloc[1] == pytest.approx(10.5)
    assert ema.iloc[2] == pytest.approx(11.25)


def test_enrich_candles_with_ema():
    raw_candles = [
        {"timestamp": 1000 + i * 3600000, "open": 100.0 + i, "high": 105.0 + i, "low": 95.0 + i, "close": 100.0 + i, "volume": 50.0}
        for i in range(10)
    ]
    df = enrich_candles_with_ema(raw_candles, fast_period=3, slow_period=5)
    assert "ema_3" in df.columns
    assert "ema_5" in df.columns
    assert len(df) == 10
    assert not df["ema_3"].isna().all()


def test_detect_golden_cross_confirmed():
    # Construct a DataFrame where previous ema_50 <= ema_200 and current ema_50 > ema_200
    df = pd.DataFrame([
        {
            "timestamp": 1700000000000,
            "open": 98.0,
            "high": 101.0,
            "low": 97.0,
            "close": 100.0,
            "volume": 100.0,
            "ema_50": 99.5,
            "ema_200": 100.0,  # fast <= slow
        },
        {
            "timestamp": 1700003600000,
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 104.0,
            "volume": 150.0,
            "ema_50": 101.2,
            "ema_200": 100.5,  # fast > slow (Golden Cross!)
        },
    ])

    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")
    assert signal is not None
    assert isinstance(signal, GoldenCrossSignal)
    assert signal.symbol == "BTCUSDT"
    assert signal.timeframe == "1H"
    assert signal.candle_timestamp == 1700003600000
    assert signal.ema50 == 101.2
    assert signal.ema200 == 100.5
    assert signal.close_price == 104.0
    assert signal.candle_status == "CLOSED"


def test_detect_golden_cross_not_triggered_when_already_bullish():
    # Previous was already fast > slow
    df = pd.DataFrame([
        {
            "timestamp": 1700000000000,
            "open": 100.0, "high": 105.0, "low": 99.0, "close": 102.0, "volume": 100.0,
            "ema_50": 102.0, "ema_200": 100.0,
        },
        {
            "timestamp": 1700003600000,
            "open": 102.0, "high": 108.0, "low": 101.0, "close": 106.0, "volume": 120.0,
            "ema_50": 103.5, "ema_200": 100.8,
        },
    ])
    signal = detect_golden_cross(df, symbol="ETHUSDT", timeframe="1h")
    assert signal is None


def test_find_all_golden_crosses():
    candles = [
        {"timestamp": 1000, "close": 10.0, "ema_50": 9.0, "ema_200": 10.0},
        {"timestamp": 2000, "close": 12.0, "ema_50": 10.5, "ema_200": 10.2}, # Cross 1
        {"timestamp": 3000, "close": 11.0, "ema_50": 10.4, "ema_200": 10.3},
        {"timestamp": 4000, "close": 8.0, "ema_50": 9.5, "ema_200": 9.8},   # Bearish drop
        {"timestamp": 5000, "close": 14.0, "ema_50": 10.2, "ema_200": 9.9},  # Cross 2
    ]
    df = pd.DataFrame(candles)
    crosses = find_all_golden_crosses(df, symbol="SOLUSDT")
    assert len(crosses) == 2
    assert crosses[0].candle_timestamp == 2000
    assert crosses[1].candle_timestamp == 5000
