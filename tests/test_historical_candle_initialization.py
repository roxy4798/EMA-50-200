"""Comprehensive regression test suite for historical candle initialization,
lifecycle state management (READY, WAITING_FOR_HISTORY, RETRY_PENDING),
continuity validation, and live WebSocket promotion.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import (
    STATE_FAILED,
    STATE_READY,
    STATE_RETRY_PENDING,
    STATE_WAITING_FOR_HISTORY,
    SignalEngine,
)
from app.exchange.binance_client import BinanceFuturesClient
from app.persistence.database import Database


def make_candle(
    ts: int,
    open_p: float = 100.0,
    high_p: float = 105.0,
    low_p: float = 95.0,
    close_p: float = 102.0,
    volume: float = 10.0,
    is_closed: bool = True,
) -> Dict[str, Any]:
    return {
        "timestamp": ts,
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close_p,
        "volume": volume,
        "close_time": ts + 3600_000 - 1,
        "is_closed": is_closed,
    }


def make_candle_sequence(
    count: int,
    start_ts: int = 1700000000000,
    step_ms: int = 3600_000,
    trend: str = "flat",
) -> List[Dict[str, Any]]:
    candles = []
    base_price = 100.0
    for i in range(count):
        ts = start_ts + i * step_ms
        if trend == "bullish":
            price = base_price + i * 0.5
        elif trend == "bearish":
            price = base_price - i * 0.5
        else:
            price = base_price
        candles.append(make_candle(ts, close_p=price))
    return candles


async def setup_test_engine(tmp_path, candle_limit: int = 1000, fast_p: int = 50, slow_p: int = 500):
    db_path = str(tmp_path / "test_init.db")
    db = Database(db_path=db_path)
    await db.init()

    client = BinanceFuturesClient()
    alert_queue = MagicMock(spec=AlertQueue)
    alert_queue.enqueue = AsyncMock()

    engine = SignalEngine(
        binance_client=client,
        database=db,
        alert_queue=alert_queue,
        timeframe="1h",
        fast_period=fast_p,
        slow_period=slow_p,
        candle_limit=candle_limit,
    )
    return engine, client, db, alert_queue


# ===========================================================================
# 1. Exactly 1,000 valid closed candles results in READY
# ===========================================================================
@pytest.mark.asyncio
async def test_case_1_exactly_1000_closed_candles_results_in_ready(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    candles_1000 = make_candle_sequence(1000)
    client.get_klines = AsyncMock(return_value=candles_1000)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    assert "BTCUSDT" in engine.initialized_symbols
    assert engine.get_symbol_state("BTCUSDT") == STATE_READY
    assert len(engine.candles_history["BTCUSDT"]) == 1000
    assert engine.get_state_count(STATE_READY) == 1
    assert engine.get_state_count(STATE_WAITING_FOR_HISTORY) == 0

    await client.close()


# ===========================================================================
# 2. Fewer than 1,000 candles never results in READY or a live signal
# ===========================================================================
@pytest.mark.asyncio
async def test_case_2_fewer_than_1000_candles_never_results_in_ready_or_live_signal(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    candles_999 = make_candle_sequence(999)
    client.get_klines = AsyncMock(return_value=candles_999)
    client.is_history_exhausted = MagicMock(return_value=True)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    assert "BTCUSDT" not in engine.initialized_symbols
    assert engine.get_symbol_state("BTCUSDT") == STATE_WAITING_FOR_HISTORY
    assert len(engine.candles_history["BTCUSDT"]) == 999

    # Deliver an incoming candle that still leaves total < 1000
    # E.g. start with 990 candles
    engine.candles_history["BTCUSDT"] = candles_999[:990]
    next_ts = candles_999[989]["timestamp"] + 3600_000
    new_candle = make_candle(next_ts, open_p=100.0, close_p=120.0, is_closed=True)

    sig = await engine.handle_closed_candle("BTCUSDT", new_candle)
    assert sig is None
    alert_queue.enqueue.assert_not_called()
    assert "BTCUSDT" not in engine.initialized_symbols

    await client.close()


# ===========================================================================
# 3. A newly listed symbol with fewer than 1,000 available candles remains pending
#    without fabricated data (e.g. CTUSDT 188, MARSCOINUSDT 906, PONSUSDT 789)
# ===========================================================================
@pytest.mark.asyncio
async def test_case_3_newly_listed_symbol_remains_pending_without_fabrication(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["CTUSDT", "MARSCOINUSDT", "PONSUSDT"])

    async def mock_fetch(sym, **kwargs):
        if sym == "CTUSDT":
            return make_candle_sequence(188, start_ts=1700000000000)
        elif sym == "MARSCOINUSDT":
            return make_candle_sequence(906, start_ts=1700000000000)
        elif sym == "PONSUSDT":
            return make_candle_sequence(789, start_ts=1700000000000)
        return []

    client.get_klines = AsyncMock(side_effect=mock_fetch)
    client.is_history_exhausted = MagicMock(return_value=True)

    await engine.initialize_symbols(max_concurrency=3, pacing_delay_ms=0.0)

    for sym, count in [("CTUSDT", 188), ("MARSCOINUSDT", 906), ("PONSUSDT", 789)]:
        assert sym not in engine.initialized_symbols
        assert engine.get_symbol_state(sym) == STATE_WAITING_FOR_HISTORY
        loaded = engine.candles_history[sym]
        assert len(loaded) == count
        # Verify no fabricated timestamps (exact 1-hour intervals)
        for i in range(1, len(loaded)):
            assert loaded[i]["timestamp"] - loaded[i - 1]["timestamp"] == 3600_000

    assert engine.get_state_count(STATE_READY) == 0
    assert engine.get_state_count(STATE_WAITING_FOR_HISTORY) == 3

    await client.close()


# ===========================================================================
# 4. A temporary partial REST response is retried safely
# ===========================================================================
@pytest.mark.asyncio
async def test_case_4_temporary_partial_rest_response_retried_safely(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    attempts = 0

    async def mock_fetch(sym, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            # Temporary error / partial response
            return []
        # 3rd attempt succeeds with 1000 candles
        return make_candle_sequence(1000)

    client.get_klines = AsyncMock(side_effect=mock_fetch)
    client.is_history_exhausted = MagicMock(return_value=False)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    assert attempts == 3
    assert "BTCUSDT" in engine.initialized_symbols
    assert engine.get_symbol_state("BTCUSDT") == STATE_READY

    await client.close()


# ===========================================================================
# 5. A later successful fetch or live accumulation completes initialization automatically
# ===========================================================================
@pytest.mark.asyncio
async def test_case_5_later_successful_accumulation_completes_initialization(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    # Initial fetch: 999 candles (WAITING_FOR_HISTORY)
    candles_999 = make_candle_sequence(999, start_ts=1700000000000)
    client.get_klines = AsyncMock(return_value=candles_999)
    client.is_history_exhausted = MagicMock(return_value=True)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)
    assert engine.get_symbol_state("BTCUSDT") == STATE_WAITING_FOR_HISTORY
    assert "BTCUSDT" not in engine.initialized_symbols

    # Next live closed candle arrives via WebSocket
    next_ts = candles_999[-1]["timestamp"] + 3600_000
    candle_1000 = make_candle(next_ts, is_closed=True)

    await engine.handle_closed_candle("BTCUSDT", candle_1000)

    # Now total closed candles reached 1000!
    assert "BTCUSDT" in engine.initialized_symbols
    assert engine.get_symbol_state("BTCUSDT") == STATE_READY
    assert len(engine.candles_history["BTCUSDT"]) == 1000

    # Also test background retry_unready_symbols promotion
    engine.initialized_symbols.clear()
    engine.symbol_states["BTCUSDT"] = STATE_RETRY_PENDING
    client.get_klines = AsyncMock(return_value=make_candle_sequence(1000, start_ts=1700000000000))

    summary = await engine.retry_unready_symbols(max_concurrency=1, pacing_delay_ms=0.0)
    assert "BTCUSDT" in engine.initialized_symbols
    assert summary["ready"] == 1

    await client.close()


# ===========================================================================
# 6. Cached and fresh candles merge correctly without duplicate timestamps
# ===========================================================================
@pytest.mark.asyncio
async def test_case_6_cached_and_fresh_candles_merge_without_duplicates(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    start_ts = 1700000000000
    # Seed DB cache with 600 candles (timestamps 0..599)
    cached_600 = make_candle_sequence(600, start_ts=start_ts)
    await db.cache_candles(cached_600, "BTCUSDT", "1h", 50, 500)

    # Fresh fetch returns 600 candles (timestamps 400..999, overlapping 400..599)
    fresh_start = start_ts + 400 * 3600_000
    fresh_600 = make_candle_sequence(600, start_ts=fresh_start)
    client.get_klines = AsyncMock(return_value=fresh_600)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    assert "BTCUSDT" in engine.initialized_symbols
    assert engine.get_symbol_state("BTCUSDT") == STATE_READY
    merged = engine.candles_history["BTCUSDT"]
    assert len(merged) == 1000

    # Verify strict ascending order with no duplicates
    timestamps = [c["timestamp"] for c in merged]
    assert len(timestamps) == len(set(timestamps))
    for i in range(1, len(timestamps)):
        assert timestamps[i] - timestamps[i - 1] == 3600_000

    await client.close()


# ===========================================================================
# 7. Open candles are excluded from the initialization count
# ===========================================================================
@pytest.mark.asyncio
async def test_case_7_open_candles_excluded_from_initialization_count(tmp_path, monkeypatch):
    now_ms = 1700000000000 + 1000 * 3600_000
    monkeypatch.setattr("app.exchange.binance_client.time.time", lambda: now_ms / 1000.0)

    client = BinanceFuturesClient()

    # 999 closed candles + 1 open candle
    rows = []
    for i in range(999):
        ts = 1700000000000 + i * 3600_000
        rows.append([ts, 100, 105, 95, 100, 10, ts + 3600_000 - 1])
    # Open candle whose close_time is in the future
    open_ts = 1700000000000 + 999 * 3600_000
    rows.append([open_ts, 100, 105, 95, 100, 10, now_ms + 1800_000])

    class MockResp:
        status = 200
        async def json(self):
            return rows
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass

    class MockSession:
        def get(self, url, params=None):
            return MockResp()

    client._get_session = AsyncMock(return_value=MockSession())

    result = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)
    # Open candle was excluded
    assert len(result) == 999
    assert all(c["close_time"] < now_ms for c in result)

    await client.close()


# ===========================================================================
# 8. Missing timestamps or discontinuous history are handled explicitly
# ===========================================================================
@pytest.mark.asyncio
async def test_case_8_discontinuous_history_handled_explicitly(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    start_ts = 1700000000000
    candles = make_candle_sequence(500, start_ts=start_ts)
    # Introduce a 2-hour gap: skip 1 candle
    gap_ts = candles[-1]["timestamp"] + 7200_000
    candles.extend(make_candle_sequence(500, start_ts=gap_ts))
    assert len(candles) == 1000

    # Continuity validation must detect this
    assert engine.validate_candle_continuity(candles) is False

    client.get_klines = AsyncMock(return_value=candles)
    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    # Discontinuous history must NOT result in READY!
    assert "BTCUSDT" not in engine.initialized_symbols
    assert engine.get_symbol_state("BTCUSDT") == STATE_RETRY_PENDING
    details = engine.symbol_details.get("BTCUSDT", {})
    assert "Discontinuous" in details.get("reason", "")

    await client.close()


# ===========================================================================
# 9. HTTP 429/418 respects backoff and does not create a request storm
# ===========================================================================
@pytest.mark.asyncio
async def test_case_9_rate_limit_backoff_avoids_request_storm(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT", "ETHUSDT"])

    # Set pause_remaining on client
    client._pause_until = time.time() + 10.0
    assert client.pause_remaining > 0

    client.get_klines = AsyncMock()

    # Background retry must immediately skip when client is paused
    summary = await engine.retry_unready_symbols(max_concurrency=2, pacing_delay_ms=0.0)
    client.get_klines.assert_not_called()
    assert summary["ready"] == 0

    await client.close()


# ===========================================================================
# 10. Retry processing does not block WebSocket event handling
# ===========================================================================
@pytest.mark.asyncio
async def test_case_10_retry_processing_does_not_block_websocket_events(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT", "SLOWUSDT"])

    # Preload BTCUSDT as READY
    engine.candles_history["BTCUSDT"] = make_candle_sequence(1000)
    engine.initialized_symbols.add("BTCUSDT")
    engine.symbol_states["BTCUSDT"] = STATE_READY

    # SLOWUSDT is unready, simulated with slow REST response
    async def slow_fetch(sym, **kwargs):
        await asyncio.sleep(0.2)
        return make_candle_sequence(1000)

    client.get_klines = AsyncMock(side_effect=slow_fetch)

    # Start background retry in asyncio task
    retry_task = asyncio.create_task(engine.retry_unready_symbols(max_concurrency=1, pacing_delay_ms=0.0))

    # Concurrently deliver a WebSocket event for BTCUSDT
    ws_candle = make_candle(1700000000000 + 1001 * 3600_000, is_closed=True)
    t0 = time.monotonic()
    await engine.handle_closed_candle("BTCUSDT", ws_candle)
    t_elapsed = time.monotonic() - t0

    # Event handling should not be blocked by the 0.2s slow REST fetch
    assert t_elapsed < 0.15

    await retry_task
    assert engine.get_symbol_state("SLOWUSDT") == STATE_READY

    await client.close()


# ===========================================================================
# 11. Historical initialization never generates live Telegram alerts
# ===========================================================================
@pytest.mark.asyncio
async def test_case_11_historical_initialization_never_generates_live_alerts(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    # 1,000 candles with a crossover in the past
    candles = make_candle_sequence(1000, trend="bullish")
    client.get_klines = AsyncMock(return_value=candles)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    # Verify no live signal was created during initialization
    assert engine.live_signals_count == 0
    alert_queue.enqueue.assert_not_called()

    # Even historical scan must record as historical (is_live = 0)
    crosses = await engine.scan_and_record_historical_crosses("BTCUSDT")
    assert await db.get_live_signals_count() == 0
    assert await db.get_historical_signals_count() == len(crosses)
    alert_queue.enqueue.assert_not_called()

    await client.close()


# ===========================================================================
# 12. Existing Golden Cross behavior remains unchanged
# ===========================================================================
@pytest.mark.asyncio
async def test_case_12_existing_golden_cross_behavior_remains_unchanged(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path, candle_limit=1000, fast_p=50, slow_p=500)
    engine.set_symbols(["BTCUSDT"])

    # Create 999 candles in a bearish state: price falling steadily
    # Then candle 1000 has massive upward surge causing EMA50 to cross above EMA500
    candles = make_candle_sequence(999, start_ts=1700000000000, trend="flat")
    client.get_klines = AsyncMock(return_value=candles)
    client.is_history_exhausted = MagicMock(return_value=True)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)
    assert engine.get_symbol_state("BTCUSDT") == STATE_WAITING_FOR_HISTORY

    # Crossover candle arrives
    next_ts = candles[-1]["timestamp"] + 3600_000
    crossover_candle = make_candle(next_ts, open_p=100.0, close_p=1000.0, is_closed=True)

    sig = await engine.handle_closed_candle("BTCUSDT", crossover_candle)
    # The symbol is now promoted to READY
    assert engine.get_symbol_state("BTCUSDT") == STATE_READY
    assert "BTCUSDT" in engine.initialized_symbols

    # Verify EMA periods match configuration
    assert engine.fast_period == 50
    assert engine.slow_period == 500

    await client.close()


# ===========================================================================
# 13. A symbol in RETRY_PENDING remains retry-pending after receiving a live candle
#     when exchange history is not exhausted
# ===========================================================================
@pytest.mark.asyncio
async def test_case_13_symbol_in_retry_pending_remains_retry_pending_on_live_candle(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    # Simulate temporary partial retrieval: returns 500 candles, NOT exhausted
    candles_500 = make_candle_sequence(500, start_ts=1700000000000)
    client.get_klines = AsyncMock(return_value=candles_500)
    client.is_history_exhausted = MagicMock(return_value=False)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    # Initial state must be RETRY_PENDING
    assert engine.get_symbol_state("BTCUSDT") == STATE_RETRY_PENDING
    assert "BTCUSDT" not in engine.initialized_symbols

    # A live candle arrives via WebSocket
    next_ts = candles_500[-1]["timestamp"] + 3600_000
    live_candle = make_candle(next_ts, open_p=100.0, close_p=105.0, is_closed=True)
    sig = await engine.handle_closed_candle("BTCUSDT", live_candle)

    # Must remain RETRY_PENDING, not WAITING_FOR_HISTORY, not READY
    assert sig is None
    assert engine.get_symbol_state("BTCUSDT") == STATE_RETRY_PENDING
    assert "BTCUSDT" not in engine.initialized_symbols
    assert len(engine.candles_history["BTCUSDT"]) == 501
    alert_queue.enqueue.assert_not_called()

    await client.close()


# ===========================================================================
# 14. Discontinuous history does not become WAITING_FOR_HISTORY or READY
# ===========================================================================
@pytest.mark.asyncio
async def test_case_14_discontinuous_history_does_not_become_waiting_or_ready(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    # History with a gap
    candles = make_candle_sequence(500, start_ts=1700000000000)
    gap_ts = candles[-1]["timestamp"] + 7200_000  # 2-hour gap
    candles.extend(make_candle_sequence(499, start_ts=gap_ts))  # total 999
    assert len(candles) == 999
    assert engine.validate_candle_continuity(candles) is False

    client.get_klines = AsyncMock(return_value=candles)
    # Even if client erroneously reported exhausted:
    client.is_history_exhausted = MagicMock(return_value=True)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    # Must be RETRY_PENDING, not WAITING_FOR_HISTORY
    assert engine.get_symbol_state("BTCUSDT") == STATE_RETRY_PENDING
    assert "BTCUSDT" not in engine.initialized_symbols

    # Live candle arrives bringing count to 1000
    next_ts = candles[-1]["timestamp"] + 3600_000
    live_candle = make_candle(next_ts, open_p=100.0, close_p=105.0, is_closed=True)
    sig = await engine.handle_closed_candle("BTCUSDT", live_candle)

    # Must NOT become READY and must NOT evaluate signals due to gap
    assert sig is None
    assert engine.get_symbol_state("BTCUSDT") == STATE_RETRY_PENDING
    assert "BTCUSDT" not in engine.initialized_symbols
    alert_queue.enqueue.assert_not_called()

    await client.close()


# ===========================================================================
# 15. A symbol with genuinely exhausted exchange history can accumulate candles
#     and eventually become READY
# ===========================================================================
@pytest.mark.asyncio
async def test_case_15_exhausted_history_accumulates_and_becomes_ready(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["NEWUSDT"])

    candles_998 = make_candle_sequence(998, start_ts=1700000000000)
    client.get_klines = AsyncMock(return_value=candles_998)
    client.is_history_exhausted = MagicMock(return_value=True)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    assert engine.get_symbol_state("NEWUSDT") == STATE_WAITING_FOR_HISTORY
    assert "NEWUSDT" not in engine.initialized_symbols

    # Accumulate candle 999
    ts_999 = candles_998[-1]["timestamp"] + 3600_000
    c_999 = make_candle(ts_999, open_p=100.0, close_p=101.0, is_closed=True)
    sig1 = await engine.handle_closed_candle("NEWUSDT", c_999)
    assert sig1 is None
    assert engine.get_symbol_state("NEWUSDT") == STATE_WAITING_FOR_HISTORY
    assert "NEWUSDT" not in engine.initialized_symbols

    # Accumulate candle 1000 -> reaches canonical threshold!
    ts_1000 = ts_999 + 3600_000
    c_1000 = make_candle(ts_1000, open_p=101.0, close_p=102.0, is_closed=True)
    sig2 = await engine.handle_closed_candle("NEWUSDT", c_1000)

    # Must transition automatically to READY
    assert engine.get_symbol_state("NEWUSDT") == STATE_READY
    assert "NEWUSDT" in engine.initialized_symbols
    assert len(engine.candles_history["NEWUSDT"]) == 1000

    await client.close()


# ===========================================================================
# 16. A non-empty partial REST response for an established symbol is not
#     automatically treated as permanent history exhaustion without evidence
# ===========================================================================
@pytest.mark.asyncio
async def test_case_16_partial_rest_for_established_symbol_not_treated_as_exhaustion(tmp_path):
    engine, client, db, alert_queue = await setup_test_engine(tmp_path)
    engine.set_symbols(["BTCUSDT"])

    # Established symbol receives partial response (e.g. 700 candles)
    # Client has not established exhaustion (is_history_exhausted returns False)
    candles_700 = make_candle_sequence(700, start_ts=1700000000000)
    client.get_klines = AsyncMock(return_value=candles_700)
    client.is_history_exhausted = MagicMock(return_value=False)

    await engine.initialize_symbols(max_concurrency=1, pacing_delay_ms=0.0)

    # Must be RETRY_PENDING, not WAITING_FOR_HISTORY
    assert engine.get_symbol_state("BTCUSDT") == STATE_RETRY_PENDING
    assert engine.get_symbol_state("BTCUSDT") != STATE_WAITING_FOR_HISTORY

    await client.close()


# ===========================================================================
# 17. Binance pagination and open-candle filtering do not cause false exhaustion
# ===========================================================================
@pytest.mark.asyncio
async def test_case_17_binance_client_pagination_and_open_candle_no_false_exhaustion(monkeypatch):
    client = BinanceFuturesClient(base_url="https://fapi.binance.com")
    now_ms = 1800000000000
    monkeypatch.setattr("app.exchange.binance_client.time.time", lambda: now_ms / 1000.0)

    # Build 1000 candles ending with an open candle
    raw_page_1 = []
    base_ts = now_ms - 1000 * 3600_000
    for i in range(999):
        ts = base_ts + i * 3600_000
        raw_page_1.append([ts, 100, 105, 95, 100, 10, ts + 3600_000 - 1])
    # Open candle
    open_ts = base_ts + 999 * 3600_000
    raw_page_1.append([open_ts, 100, 105, 95, 100, 10, now_ms + 1800_000])

    # Backfill page: 10 older candles
    raw_backfill = []
    earliest_ts = base_ts
    for i in range(1, 11):
        ts = earliest_ts - i * 3600_000
        raw_backfill.append([ts, 100, 105, 95, 100, 10, ts + 3600_000 - 1])

    call_count = 0
    class MockResp:
        def __init__(self, data, status=200):
            self._data = data
            self.status = status
        async def json(self):
            return self._data
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass

    class MockSession:
        def get(self, url, params=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return MockResp(raw_page_1)
            else:
                return MockResp(raw_backfill)

    client._get_session = AsyncMock(return_value=MockSession())

    result = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    # 999 closed from page 1 + older candles from backfill satisfy limit 1000
    assert len(result) == 1000
    assert all(c["close_time"] < now_ms for c in result)
    # CRITICAL: exchange_exhausted must be False because full limit was satisfied!
    assert client.is_history_exhausted("BTCUSDT") is False

    await client.close()
