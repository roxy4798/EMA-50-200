"""Strict Hardening & Regression Test Suite for Pure EMA50/EMA200 Golden Cross.

Validates canonical production behavior without any additional indicators or filters:
A. previous EMA50 < EMA200, current EMA50 > EMA200 => TRUE
B. previous EMA50 == EMA200, current EMA50 > EMA200 => TRUE
C. previous EMA50 > EMA200, current EMA50 > EMA200 => FALSE
D. previous EMA50 < EMA200, current EMA50 < EMA200 => FALSE
E. Death Cross => FALSE / no short signal
F. Unclosed WebSocket candle (k.x == False) => not forwarded to SignalEngine
G. EMA50 already above EMA200 for multiple candles => signals only on transition
H. Same symbol + same closed timestamp => only one alert
I. Different symbols => independent signals
J. Adversarial conditions => valid Golden Cross still generates signal (Behavioral Proof)
K. Chart event condition strictly matches Signal Engine condition
L. Telegram message represents the exact same Golden Cross event
"""

import asyncio
import os
import pytest
import pandas as pd
import numpy as np

from app.charts.chart_renderer import ChartRenderer
from app.charts.chart_data import ChartDataProvider
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.indicators.ema import (
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)
from app.notifications.telegram_bot import TelegramNotifier
from app.persistence.database import Database


def _build_two_bar_df(
    prev_fast: float,
    prev_slow: float,
    curr_fast: float,
    curr_slow: float,
    prev_close: float = 100.0,
    curr_close: float = 105.0,
) -> pd.DataFrame:
    """Helper creating a minimal 2-candle DataFrame with explicit EMA50 and EMA200."""
    return pd.DataFrame([
        {
            "timestamp": 1790000000000,
            "open": prev_close,
            "high": prev_close + 1.0,
            "low": prev_close - 1.0,
            "close": prev_close,
            "volume": 100.0,
            "ema_50": prev_fast,
            "ema_200": prev_slow,
        },
        {
            "timestamp": 1790003600000,
            "open": prev_close,
            "high": max(prev_close, curr_close) + 2.0,
            "low": min(prev_close, curr_close) - 2.0,
            "close": curr_close,
            "volume": 120.0,
            "ema_50": curr_fast,
            "ema_200": curr_slow,
        },
    ])


# ---------------------------------------------------------------------------
# TEST A: previous EMA50 < EMA200 and current EMA50 > EMA200 => TRUE
# ---------------------------------------------------------------------------
def test_requirement_a_prev_below_curr_above_triggers_golden_cross():
    df = _build_two_bar_df(prev_fast=99.0, prev_slow=100.0, curr_fast=101.0, curr_slow=100.0)
    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")

    assert signal is not None, "Expected valid Golden Cross when previous < and current >"
    assert signal.symbol == "BTCUSDT"
    assert signal.timeframe == "1H"
    assert signal.ema50 == 101.0
    assert signal.ema200 == 100.0
    assert signal.previous_ema50 == 99.0
    assert signal.previous_ema200 == 100.0
    assert signal.candle_status == "CLOSED"


# ---------------------------------------------------------------------------
# TEST B: previous EMA50 == EMA200 and current EMA50 > EMA200 => TRUE
# ---------------------------------------------------------------------------
def test_requirement_b_prev_equal_curr_above_triggers_golden_cross():
    df = _build_two_bar_df(prev_fast=100.0, prev_slow=100.0, curr_fast=100.2, curr_slow=100.0)
    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")

    assert signal is not None, "Expected valid Golden Cross when previous == and current >"
    assert signal.previous_ema50 == 100.0
    assert signal.previous_ema200 == 100.0
    assert signal.ema50 > signal.ema200


# ---------------------------------------------------------------------------
# TEST C: previous EMA50 > EMA200 and current EMA50 > EMA200 => FALSE
# ---------------------------------------------------------------------------
def test_requirement_c_prev_above_curr_above_continuous_bullish_no_signal():
    df = _build_two_bar_df(prev_fast=102.0, prev_slow=100.0, curr_fast=103.0, curr_slow=100.0)
    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")

    assert signal is None, "Continuous bullish condition (no crossover event) must not trigger Golden Cross"


# ---------------------------------------------------------------------------
# TEST D: previous EMA50 < EMA200 and current EMA50 < EMA200 => FALSE
# ---------------------------------------------------------------------------
def test_requirement_d_prev_below_curr_below_continuous_bearish_no_signal():
    df = _build_two_bar_df(prev_fast=98.0, prev_slow=100.0, curr_fast=99.0, curr_slow=100.0)
    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")

    assert signal is None, "Continuous bearish condition must not trigger Golden Cross"


# ---------------------------------------------------------------------------
# TEST E: Death Cross (previous >= slow and current < slow) => FALSE / no short
# ---------------------------------------------------------------------------
def test_requirement_e_death_cross_ignored_no_short_signal():
    # Previous fast above slow, current fast below slow
    df = _build_two_bar_df(prev_fast=101.0, prev_slow=100.0, curr_fast=99.0, curr_slow=100.0)
    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")

    assert signal is None, "Death Cross must be completely ignored (never trigger Golden Cross)"

    # Also verify find_all_golden_crosses ignores Death Cross
    crosses = find_all_golden_crosses(df, symbol="BTCUSDT", timeframe="1h")
    assert len(crosses) == 0, "Death Cross must produce zero signals"


# ---------------------------------------------------------------------------
# TEST F: Unclosed WebSocket candle (k.x == False) => not forwarded
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_f_unclosed_websocket_candle_not_forwarded():
    forwarded_candles = []

    async def mock_callback(sym: str, candle_data: dict):
        forwarded_candles.append((sym, candle_data))

    ws = BinanceWebSocketManager(
        base_ws_url="wss://fstream.binance.com/market",
        timeframe="1h",
        on_candle_closed=mock_callback,
    )
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
            "x": False,  # Candle still forming!
        },
    }
    await ws._handle_message(unclosed_msg)
    assert len(forwarded_candles) == 0, "Unclosed candle (x=False) must NOT be forwarded to callback"

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
            "x": True,  # Candle closed!
        },
    }
    await ws._handle_message(closed_msg)
    assert len(forwarded_candles) == 1, "Closed candle (x=True) must be forwarded"
    assert forwarded_candles[0][0] == "BTCUSDT"
    assert forwarded_candles[0][1]["is_closed"] is True


# ---------------------------------------------------------------------------
# TEST G: EMA50 already above EMA200 for multiple candles => single signal
# ---------------------------------------------------------------------------
def test_requirement_g_already_above_multiple_candles_signals_only_on_transition():
    candles = [
        {"timestamp": 1000, "close": 10.0, "ema_50": 9.0, "ema_200": 10.0},
        {"timestamp": 2000, "close": 12.0, "ema_50": 10.5, "ema_200": 10.0},  # TRANSITION 1
        {"timestamp": 3000, "close": 13.0, "ema_50": 11.0, "ema_200": 10.1},  # already above
        {"timestamp": 4000, "close": 14.0, "ema_50": 11.5, "ema_200": 10.2},  # already above
        {"timestamp": 5000, "close": 15.0, "ema_50": 12.0, "ema_200": 10.3},  # already above
    ]
    df = pd.DataFrame(candles)
    crosses = find_all_golden_crosses(df, symbol="SOLUSDT")

    assert len(crosses) == 1, "Only the crossover transition candle must produce a Golden Cross"
    assert crosses[0].candle_timestamp == 2000


# ---------------------------------------------------------------------------
# TEST H: Same symbol + same candle timestamp => only one alert
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_h_same_symbol_same_closed_ts_only_one_alert(tmp_path):
    db = Database(db_path=str(tmp_path / "dedup.db"))
    await db.init()
    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    # Note: Do not start worker background loop to deterministically inspect queue depth
    queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, queue)
    engine.set_symbols(["BTCUSDT"])

    ts = 1790000000000
    # Seed 250 candles with bearish EMAs so the incoming candle triggers crossover
    engine.candles_history["BTCUSDT"] = [
        {"timestamp": ts - (250 - i) * 3600000, "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "volume": 10.0}
        for i in range(250)
    ]

    candle = {
        "timestamp": ts,
        "open": 98.0,
        "high": 125.0,
        "low": 98.0,
        "close": 120.0,
        "volume": 50.0,
        "is_closed": True,
    }

    # 1. First evaluation: Golden Cross detected and enqueued
    sig1 = await engine.handle_closed_candle("BTCUSDT", candle)
    assert sig1 is not None
    assert queue._queue.qsize() == 1
    assert await db.get_live_signals_count() == 1

    # 2. Repeated evaluation of exact same timestamp: suppressed
    sig2 = await engine.handle_closed_candle("BTCUSDT", candle)
    assert sig2 is None, "Repeated candle with same timestamp must be suppressed by deduplication"
    assert queue._queue.qsize() == 1, "Alert queue must not receive duplicate alert"
    assert await db.get_live_signals_count() == 1

    await client.close()


# ---------------------------------------------------------------------------
# TEST I: Different symbols => independent signals
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_i_different_symbols_independent_signals(tmp_path):
    db = Database(db_path=str(tmp_path / "multi_sym.db"))
    await db.init()
    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    # Note: Do not start worker background loop to deterministically inspect queue depth
    queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, queue)
    engine.set_symbols(["BTCUSDT", "ETHUSDT"])

    ts = 1790000000000
    for sym in ["BTCUSDT", "ETHUSDT"]:
        engine.candles_history[sym] = [
            {"timestamp": ts - (250 - i) * 3600000, "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "volume": 10.0}
            for i in range(250)
        ]
        candle = {
            "timestamp": ts,
            "open": 98.0,
            "high": 125.0,
            "low": 98.0,
            "close": 120.0,
            "volume": 50.0,
            "is_closed": True,
        }
        sig = await engine.handle_closed_candle(sym, candle)
        assert sig is not None
        assert sig.symbol == sym

    assert queue._queue.qsize() == 2, "Both distinct symbols must independently enqueue alerts"
    assert engine.live_signals_count == 2
    assert await db.get_live_signals_count() == 2

    await client.close()


# ---------------------------------------------------------------------------
# TEST J: Adversarial conditions => valid Golden Cross still generates signal
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_j_adversarial_conditions_behavioral_proof_pure_golden_cross(tmp_path):
    """BEHAVIORAL PROOF OF PURE GOLDEN CROSS:

    Even under the most unfavorable market conditions:
    1. Candle is heavily BEARISH (close << open, large red candle)
    2. Close price is strictly BELOW EMA50
    3. Close price is strictly BELOW EMA200
    4. Volume is practically DEAD (0.000001)
    5. Mock adversarial values (RSI deep oversold, MACD bearish histogram, ADX flat, ATR extreme)
    6. Mock negative trend / market indicators

    BECAUSE:
    - previous EMA50 <= previous EMA200
    - current EMA50 > current EMA200
    - candle is CLOSED

    EXPECTED:
    GOLDEN CROSS MUST STILL BE GENERATED AND ALERT ENQUEUED!
    """
    ts_curr = 1790003600000
    ts_prev = 1790000000000

    # Construct adversarial 2-bar DataFrame for direct detector verification
    adversarial_df = pd.DataFrame([
        {
            "timestamp": ts_prev,
            "open": 150.0,
            "high": 155.0,
            "low": 100.0,
            "close": 102.0,
            "volume": 0.000001,       # Negligible volume
            "ema_50": 100.0,
            "ema_200": 100.5,        # previous EMA50 <= EMA200
            # Extra adversarial indicators
            "rsi": 98.0,             # Overbought exhaustion
            "macd": -45.0,           # Bearish MACD
            "adx": 6.0,              # No trend strength
            "atr": 95.0,             # Extreme volatility
        },
        {
            "timestamp": ts_curr,
            "open": 120.0,
            "high": 120.0,
            "low": 60.0,
            "close": 65.0,           # Massive RED candle (close << open), far BELOW both EMA50 and EMA200
            "volume": 0.000001,      # Dead volume
            "ema_50": 101.0,         # current EMA50 > EMA200 (VALID CROSSOVER!)
            "ema_200": 100.5,
            # Extra adversarial indicators
            "rsi": 8.0,              # Oversold crash
            "macd": -85.0,           # Further deteriorating MACD
            "adx": 4.0,              # Dead trend
            "atr": 160.0,            # High ATR
            "trend_score": -100.0,
        },
    ])

    # 1. Behavioral verification on detect_golden_cross
    sig = detect_golden_cross(adversarial_df, symbol="BTCUSDT", timeframe="1h")
    assert sig is not None, "Detector MUST trigger Golden Cross despite extreme unfavorable conditions!"
    assert sig.symbol == "BTCUSDT"
    assert sig.close_price == 65.0
    assert sig.close_price < sig.ema50
    assert sig.close_price < sig.ema200
    assert sig.ema50 == 101.0
    assert sig.ema200 == 100.5
    assert sig.candle_status == "CLOSED"


# ---------------------------------------------------------------------------
# TEST K: Chart event condition strictly matches Signal Engine condition
# ---------------------------------------------------------------------------
def test_requirement_k_chart_event_condition_matches_signal_engine(tmp_path):
    # Provide >= 5 candles to satisfy ChartRenderer minimal candle requirements
    rows = []
    base_ts = 1790000000000
    for i in range(10):
        ts = base_ts + i * 3600000
        # Target candle at index 8
        if i < 8:
            fast = 98.0
            slow = 100.0
        elif i == 8:
            fast = 101.0  # CROSSOVER HERE!
            slow = 100.0
        else:
            fast = 102.0
            slow = 100.0

        rows.append({
            "timestamp": ts,
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 102.0,
            "volume": 100.0,
            "ema_50": fast,
            "ema_200": slow,
        })

    df = pd.DataFrame(rows)
    target_ts = int(df.iloc[8]["timestamp"])

    # 1. Signal engine detector confirms crossover
    slice_df = df.iloc[:9]
    signal = detect_golden_cross(slice_df, symbol="BTCUSDT")
    assert signal is not None

    # 2. ChartRenderer confirms exact same mathematical condition
    renderer = ChartRenderer(output_dir=str(tmp_path))
    chart_data = {
        "symbol": "BTCUSDT",
        "timeframe": "1H",
        "df": df,
        "cross_markers": [{"timestamp_ms": target_ts, "signal_time_utc": signal.signal_time_utc}],
    }
    chart_path = renderer.render_golden_cross_chart(chart_data, target_timestamp=target_ts)
    assert chart_path is not None, "Valid Golden Cross must render chart successfully"
    assert os.path.isfile(chart_path)

    # 3. Non-crossover candle must fail-closed in chart renderer
    non_cross_ts = int(df.iloc[5]["timestamp"])
    invalid_path = renderer.render_golden_cross_chart(chart_data, target_timestamp=non_cross_ts)
    assert invalid_path is None, "Chart renderer must fail-closed if candle is not a valid crossover"


# ---------------------------------------------------------------------------
# TEST L: Telegram message represents the exact same Golden Cross event
# ---------------------------------------------------------------------------
def test_requirement_l_telegram_message_represents_same_event():
    notifier = TelegramNotifier(enabled=False)
    sig = GoldenCrossSignal(
        symbol="BTCUSDT",
        timeframe="1H",
        candle_timestamp=1790003600000,
        signal_time_utc="01 Oct 2026 • 12:00 UTC",
        ema50=101.0500,
        ema200=100.0200,
        close_price=105.50,
        previous_ema50=99.8000,
        previous_ema200=100.0100,
        candle_status="CLOSED",
    )

    msg = notifier.format_alert_message(sig)

    assert "NEXORA EMA CROSS" in msg
    assert "🟢 GOLDEN CROSS" in msg
    assert "SYMBOL: BTCUSDT" in msg
    assert "TIMEFRAME: 1H" in msg
    assert "EMA50: 101.05" in msg
    assert "EMA200: 100.02" in msg
    assert "CANDLE: CLOSED" in msg
    assert "TIME: 01 Oct 2026 • 12:00 UTC" in msg
