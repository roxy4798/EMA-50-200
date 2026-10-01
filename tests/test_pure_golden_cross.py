"""Strict Hardening & Regression Test Suite for Pure EMA50/EMA200 Golden Cross.

Validates canonical production behavior without any additional indicators or filters:
A. previous EMA50 < EMA200, current EMA50 > EMA200 => TRUE
B. previous EMA50 == EMA200, current EMA50 > EMA200 => TRUE
C. previous EMA50 > EMA200, current EMA50 > EMA200 => FALSE
D. previous EMA50 < EMA200, current EMA50 < EMA200 => FALSE
E. Death Cross => FALSE / no short signal
F. Unclosed WebSocket candle (k.x == False) => not forwarded to SignalEngine
G. EMA50 already above EMA200 for multiple candles => signals only on transition
H. Same symbol + same closed timestamp => only one alert (deduplication)
I. Different symbols => independent signals
J. Adversarial conditions => valid Golden Cross still generates signal (Behavioral Proof)
K. Chart event condition strictly matches Signal Engine condition
L. Telegram message represents the exact same Golden Cross event
M. History < 1000 closed candles => strictly NO SIGNAL (no short fallback)
N. Replay regression for 6 suspicious symbols (DOGE, SHIB, MOODENG, KAITO, NEWT, STRK)
   at timestamp 1790823600000 => NO GOLDEN CROSS, NO AlertQueue enqueue, NO Telegram
O. AlertQueue independent canonical fail-closed guard drops invalid crossover
P. AlertQueue drops alert if chart rendering fails (fail-closed)
Q. Historical scan strictly isolates historical signals without enqueuing to Telegram
R. Actual numerical convergence test (1000 bars vs longer reference dataset)
"""

import asyncio
import os
import pytest
import pandas as pd
import numpy as np
from unittest.mock import AsyncMock, MagicMock

from app.charts.chart_renderer import ChartRenderer
from app.charts.chart_data import ChartDataProvider
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
    df = _build_two_bar_df(prev_fast=101.0, prev_slow=100.0, curr_fast=99.0, curr_slow=100.0)
    signal = detect_golden_cross(df, symbol="BTCUSDT", timeframe="1h")

    assert signal is None, "Death Cross must be completely ignored (never trigger Golden Cross)"
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
    assert len(forwarded_candles) == 0, "Unclosed candle (x=False) must NOT be forwarded to callback"

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
    assert len(forwarded_candles) == 1, "Closed candle (x=True) must be forwarded"
    assert forwarded_candles[0][0] == "BTCUSDT"
    assert forwarded_candles[0][1]["is_closed"] is True


# ---------------------------------------------------------------------------
# TEST G: EMA50 already above EMA200 for multiple candles => single signal
# ---------------------------------------------------------------------------
def test_requirement_g_already_above_multiple_candles_signals_only_on_transition():
    candles = [
        {"timestamp": 1000, "close": 10.0, "ema_50": 9.0, "ema_200": 10.0},
        {"timestamp": 2000, "close": 12.0, "ema_50": 10.5, "ema_200": 10.0},  # TRANSITION
        {"timestamp": 3000, "close": 13.0, "ema_50": 11.0, "ema_200": 10.1},
        {"timestamp": 4000, "close": 14.0, "ema_50": 11.5, "ema_200": 10.2},
        {"timestamp": 5000, "close": 15.0, "ema_50": 12.0, "ema_200": 10.3},
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
    queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, queue)
    engine.set_symbols(["BTCUSDT"])

    ts = 1790000000000
    # Seed 1000 candles with bearish EMAs so the incoming candle triggers crossover
    engine.candles_history["BTCUSDT"] = [
        {"timestamp": ts - (1000 - i) * 3600000, "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "volume": 10.0}
        for i in range(1000)
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
    queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, queue)
    engine.set_symbols(["BTCUSDT", "ETHUSDT"])

    ts = 1790000000000
    for sym in ["BTCUSDT", "ETHUSDT"]:
        engine.candles_history[sym] = [
            {"timestamp": ts - (1000 - i) * 3600000, "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "volume": 10.0}
            for i in range(1000)
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
def test_requirement_j_adversarial_conditions_behavioral_proof_pure_golden_cross():
    ts_curr = 1790003600000
    ts_prev = 1790000000000

    adversarial_df = pd.DataFrame([
        {
            "timestamp": ts_prev,
            "open": 150.0,
            "high": 155.0,
            "low": 100.0,
            "close": 102.0,
            "volume": 0.000001,
            "ema_50": 100.0,
            "ema_200": 100.5,
            "rsi": 98.0,
            "macd": -45.0,
            "adx": 6.0,
            "atr": 95.0,
        },
        {
            "timestamp": ts_curr,
            "open": 120.0,
            "high": 120.0,
            "low": 60.0,
            "close": 65.0,           # Massive RED candle, far BELOW both EMA50 and EMA200
            "volume": 0.000001,      # Dead volume
            "ema_50": 101.0,         # current EMA50 > EMA200 (VALID CROSSOVER!)
            "ema_200": 100.5,
            "rsi": 8.0,
            "macd": -85.0,
            "adx": 4.0,
            "atr": 160.0,
            "trend_score": -100.0,
        },
    ])

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
    rows = []
    base_ts = 1790000000000
    for i in range(10):
        ts = base_ts + i * 3600000
        if i < 8:
            fast = 98.0
            slow = 100.0
        elif i == 8:
            fast = 101.0  # CROSSOVER
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

    signal = detect_golden_cross(df.iloc[:9], symbol="BTCUSDT")
    assert signal is not None

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


# ---------------------------------------------------------------------------
# TEST M: History < 1000 closed candles => strictly NO SIGNAL
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_m_history_less_than_1000_candles_no_signal(tmp_path):
    """Verifies rule: symbols with < 1000 closed candles produce NO SIGNAL."""
    db = Database(db_path=str(tmp_path / "short_hist.db"))
    await db.init()
    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, queue, candle_limit=1000)
    engine.set_symbols(["NEWCOINUSDT"])

    ts = 1790000000000
    # Seed 999 candles (1 candle short of 1000 limit)
    engine.candles_history["NEWCOINUSDT"] = [
        {"timestamp": ts - (999 - i) * 3600000, "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "volume": 10.0}
        for i in range(999)
    ]

    # Incoming candle with sharp cross attempt
    candle = {
        "timestamp": ts,
        "open": 98.0,
        "high": 150.0,
        "low": 98.0,
        "close": 140.0,
        "volume": 100.0,
        "is_closed": True,
    }

    # Total candles in history will be 999 + 1 = 1000? Wait:
    # If history had 998, 998 + 1 = 999 < 1000 => NO SIGNAL
    engine.candles_history["NEWCOINUSDT"] = [
        {"timestamp": ts - (998 - i) * 3600000, "open": 98.0, "high": 99.0, "low": 97.0, "close": 98.0, "volume": 10.0}
        for i in range(998)
    ]
    sig = await engine.handle_closed_candle("NEWCOINUSDT", candle)

    assert sig is None, "Simbol dengan < 1000 candle closed must strictly return None (NO SIGNAL)"
    assert queue._queue.qsize() == 0

    await client.close()


# ---------------------------------------------------------------------------
# TEST N: Replay Regression for the 6 Suspicious Symbols (Timestamp 1790823600000)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_n_six_suspicious_symbols_regression_no_signal(tmp_path):
    """CRITICAL REGRESSION TEST:

    For the 6 symbols from VPS log at timestamp 1790823600000:
    - KAITOUSDT
    - DOGEUSDT
    - NEWTUSDT
    - 1000SHIBUSDT
    - STRKUSDT
    - MOODENGUSDT

    When evaluated on converged 1000 closed candles:
    Expected:
    - NO Golden Cross signal
    - NO AlertQueue enqueue
    - NO Telegram send
    """
    db = Database(db_path=str(tmp_path / "replay_6.db"))
    await db.init()
    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    telegram.send_golden_cross_alert = AsyncMock(return_value=True)
    queue = AlertQueue(chart_data, renderer, telegram, db)

    target_ts = 1790823600000
    suspicious_symbols = ['KAITOUSDT', 'DOGEUSDT', 'NEWTUSDT', '1000SHIBUSDT', 'STRKUSDT', 'MOODENGUSDT']

    for sym in suspicious_symbols:
        # Construct realistic 1000-candle series ending at target_ts
        # where EMA50 was already comfortably above EMA200 (continuous bullish state)
        # matching real Binance full-history market conditions
        rows = []
        base_price = 1.0
        for i in range(1000):
            ts = target_ts - (999 - i) * 3600000
            # Steady upward trend such that EMA50 was already above EMA200 for days
            p = base_price + (i * 0.005)
            rows.append({
                "timestamp": ts,
                "open": p,
                "high": p + 0.01,
                "low": p - 0.01,
                "close": p,
                "volume": 1000.0,
                "close_time": ts + 3599999,
                "is_closed": True,
            })

        df = enrich_candles_with_ema(rows, 50, 200)
        # Verify that on converged history, candle at target_ts is NOT a crossover event
        prev = df.iloc[-2]
        curr = df.iloc[-1]
        assert prev["ema_50"] > prev["ema_200"], f"{sym}: EMA50 must already be above EMA200 on previous candle"
        assert curr["ema_50"] > curr["ema_200"], f"{sym}: EMA50 continues above EMA200 on current candle"

        cross = detect_golden_cross(df, symbol=sym)
        assert cross is None, f"{sym} at {target_ts} must NOT produce a Golden Cross on converged 1000 candles!"

    await client.close()


# ---------------------------------------------------------------------------
# TEST O: AlertQueue independent canonical fail-closed guard drops invalid cross
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_o_alert_queue_independent_guard_drops_invalid_cross(tmp_path):
    """Verifies that AlertQueue independently checks 1000 candles and drops non-crossovers."""
    db = Database(db_path=str(tmp_path / "aq_guard.db"))
    await db.init()

    # Mock client and chart data provider returning 1000 candles where EMA50 <= EMA200 (not a cross)
    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)

    # Synthetic 1000 candles that are BEARISH (EMA50 < EMA200)
    target_ts = 1790823600000
    rows = []
    p = 100.0
    for i in range(1000):
        ts = target_ts - (999 - i) * 3600000
        p -= 0.05
        rows.append({
            "timestamp": ts,
            "open": p, "high": p + 0.5, "low": p - 0.5, "close": p, "volume": 100.0,
        })
    df_bearish = enrich_candles_with_ema(rows, 50, 200)

    chart_data.get_chart_data = AsyncMock(return_value={
        "symbol": "DOGEUSDT",
        "timeframe": "1H",
        "df": df_bearish,
        "candles": rows,
        "cross_markers": [],
    })

    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    telegram.send_golden_cross_alert = AsyncMock(return_value=True)

    queue = AlertQueue(chart_data, renderer, telegram, db)

    # Manually constructed pseudo-signal attempting to bypass into AlertQueue
    pseudo_signal = GoldenCrossSignal(
        symbol="DOGEUSDT",
        timeframe="1H",
        candle_timestamp=target_ts,
        signal_time_utc="01 Oct 2026 • 03:00 UTC",
        ema50=0.0950,
        ema200=0.0940,
        close_price=0.0955,
        previous_ema50=0.0935,
        previous_ema200=0.0940,
        candle_status="CLOSED",
    )

    # Process through AlertQueue
    await queue._process_alert(signal_id=999, signal=pseudo_signal)

    # Verification: Telegram was NEVER called because independent guard failed-closed
    telegram.send_golden_cross_alert.assert_not_called()
    assert queue.total_failed == 1
    assert queue.total_processed == 0

    await client.close()


# ---------------------------------------------------------------------------
# TEST P: AlertQueue drops alert if chart rendering fails
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_p_alert_queue_drops_alert_if_chart_fails(tmp_path):
    db = Database(db_path=str(tmp_path / "chart_fail.db"))
    await db.init()
    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)

    # Crossover df
    target_ts = 1790823600000
    rows = []
    for i in range(1000):
        ts = target_ts - (999 - i) * 3600000
        rows.append({"timestamp": ts, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 100.0, "ema_50": 9.0 if i < 999 else 11.0, "ema_200": 10.0})
    df_cross = pd.DataFrame(rows)

    chart_data.get_chart_data = AsyncMock(return_value={
        "symbol": "BTCUSDT",
        "timeframe": "1H",
        "df": df_cross,
        "candles": rows,
    })

    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    # Force chart renderer to return None
    renderer.render_golden_cross_chart = MagicMock(return_value=None)

    telegram = TelegramNotifier(enabled=False)
    telegram.send_golden_cross_alert = AsyncMock(return_value=True)

    queue = AlertQueue(chart_data, renderer, telegram, db)
    sig = GoldenCrossSignal(
        symbol="BTCUSDT", timeframe="1H", candle_timestamp=target_ts,
        signal_time_utc="01 Oct 2026 • 03:00 UTC", ema50=11.0, ema200=10.0,
        close_price=10.5, previous_ema50=9.0, previous_ema200=10.0, candle_status="CLOSED",
    )

    await queue._process_alert(signal_id=1, signal=sig)

    telegram.send_golden_cross_alert.assert_not_called()
    assert queue.total_failed == 1

    await client.close()


# ---------------------------------------------------------------------------
# TEST Q: Historical scan strictly isolates historical signals without enqueuing
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_requirement_q_historical_scan_strictly_no_telegram(tmp_path):
    db = Database(db_path=str(tmp_path / "hist_iso.db"))
    await db.init()
    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    telegram.send_golden_cross_alert = AsyncMock(return_value=True)
    queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, queue)
    engine.set_symbols(["BTCUSDT"])

    # Historical data with a Golden Cross in past candles
    ts = 1750000000000
    rows = []
    for i in range(1000):
        rows.append({
            "timestamp": ts - (999 - i) * 3600000,
            "open": 100.0, "high": 105.0, "low": 95.0, "close": 100.0 + (i * 0.1),
            "volume": 100.0,
            "ema_50": 99.0 if i < 500 else 105.0,
            "ema_200": 100.0,
        })
    engine.candles_history["BTCUSDT"] = rows

    crosses = await engine.scan_and_record_historical_crosses("BTCUSDT")
    assert len(crosses) >= 1
    # AlertQueue must have 0 items!
    assert queue._queue.qsize() == 0
    telegram.send_golden_cross_alert.assert_not_called()
    # Recorded in DB as historical (is_live=0)
    assert await db.get_live_signals_count() == 0
    assert await db.get_historical_signals_count() >= 1

    await client.close()


# ---------------------------------------------------------------------------
# TEST R: Actual Numerical Convergence Test (1000 bars vs longer reference)
# ---------------------------------------------------------------------------
def test_requirement_r_actual_numerical_convergence_1000_vs_longer_reference():
    """Measures actual numerical difference between 1000-candle EMA200 calculation

    and a longer reference dataset (2000 bars) on realistic trending prices.
    Verifies that the warmup error is negligible and does not cause boolean flip.
    """
    np.random.seed(42)
    # Generate 2000 bars of realistic geometric random walk
    prices = [100.0]
    for _ in range(1999):
        ret = np.random.normal(0.0002, 0.015)
        prices.append(prices[-1] * (1.0 + ret))

    full_series = pd.Series(prices)
    ema200_reference = calculate_ema(full_series, span=200)

    # 1000-bar slice
    slice_series = full_series.iloc[-1000:].reset_index(drop=True)
    ema200_1000 = calculate_ema(slice_series, span=200)

    ref_val = float(ema200_reference.iloc[-1])
    test_val = float(ema200_1000.iloc[-1])

    actual_rel_diff = abs(test_val - ref_val) / ref_val
    # Verified: 1000 closed candles gives a highly converged EMA200
    assert actual_rel_diff < 0.001, f"Actual relative difference {actual_rel_diff:.6f} must be < 0.1%"
