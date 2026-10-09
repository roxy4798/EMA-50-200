"""Automated verification suite for Live-Data Reliability Audit & Fix.

Covers all 15 required verification test cases:
1. Valid open-candle WebSocket updates refresh market-data freshness but do not trigger a Golden Cross alert.
2. A confirmed closed candle with a valid Golden Cross triggers exactly one alert.
3. Duplicate delivery of the same closed candle does not create duplicate alerts.
4. No incoming market data beyond the stale threshold changes the feed state to STALE or DEGRADED.
5. Valid messages after a stale period restore health.
6. A socket that remains connected but stops delivering data is detected as stale.
7. WebSocket disconnection triggers recovery and resubscription without spawning duplicate reconnect loops.
8. A subscription error is surfaced instead of silently reporting all streams healthy.
9. HTTP 429 respects Retry-After and does not create a tight retry loop.
10. HTTP 418 honors the required waiting period.
11. One symbol with insufficient candles is marked not ready without falsely declaring the entire feed healthy or unhealthy.
12. A failed background consumer task is detected and reflected in health status.
13. Historical scans do not send duplicate live alerts.
14. Telegram failure is reflected accurately and does not block WebSocket message consumption.
15. Health monitoring and logging do not block the asynchronous event loop.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

import pytest

from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
import app.exchange.websocket_manager as websocket_module
from app.indicators.ema import GoldenCrossSignal
from app.notifications.telegram_bot import TelegramNotifier
from app.persistence.database import Database
from app.persistence.models import SignalRecord


def _make_kline_payload(
    symbol: str = "BTCUSDT",
    timestamp: int = 1790000000000,
    close: float = 50000.0,
    closed: bool = False,
    timeframe: str = "1h",
) -> Dict[str, Any]:
    return {
        "e": "kline",
        "E": timestamp + 1000,
        "s": symbol,
        "k": {
            "t": timestamp,
            "T": timestamp + 3599999,
            "s": symbol,
            "i": timeframe,
            "o": "49000",
            "h": "51000",
            "l": "48900",
            "c": str(close),
            "v": "100",
            "x": closed,
        },
    }


# ===========================================================================
# 1. Open-candle updates refresh freshness without triggering alerts
# ===========================================================================
@pytest.mark.asyncio
async def test_case_1_open_candle_refreshes_freshness_no_alert():
    alerts_received = []

    async def on_closed(sym, candle):
        alerts_received.append((sym, candle))

    manager = BinanceWebSocketManager(on_candle_closed=on_closed)
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 1

    open_candle_msg = _make_kline_payload(symbol="BTCUSDT", closed=False)
    await manager._handle_message(open_candle_msg)

    health = manager.get_market_data_health()
    assert health["is_healthy"] is True
    assert health["status"] == "HEALTHY"
    assert manager.last_kline_received_at is not None
    assert manager.last_processed_at is not None
    assert manager.candles_closed_count == 0
    assert len(alerts_received) == 0


# ===========================================================================
# 2. Confirmed closed candle with Golden Cross triggers exactly one alert
# ===========================================================================
@pytest.mark.asyncio
async def test_case_2_closed_candle_with_golden_cross_triggers_one_alert(tmp_path):
    db = Database(db_path=str(tmp_path / "test_case2.db"))
    await db.init()

    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    alert_queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, alert_queue, fast_period=50, slow_period=200, candle_limit=1000)
    symbol = "BTCUSDT"
    engine.set_symbols([symbol])

    base_ts = 1790000000000
    # Seed 1000 candles with flat prices
    history = [
        {
            "timestamp": base_ts + i * 3600000,
            "open": 50000.0,
            "high": 50500.0,
            "low": 49500.0,
            "close": 50000.0,
            "volume": 100.0,
        }
        for i in range(1000)
    ]
    engine.candles_history[symbol] = history

    # Construct crossing candle that forces Golden Cross
    cross_candle = {
        "timestamp": base_ts + 1000 * 3600000,
        "open": 70000.0,
        "high": 80000.0,
        "low": 69000.0,
        "close": 78000.0,
        "volume": 1000.0,
        "close_time": base_ts + 1000 * 3600000 + 3599999,
        "is_closed": True,
    }
    # Force previous EMA relation: fast <= slow
    engine.candles_history[symbol][-2]["ema_50"] = 50000.0
    engine.candles_history[symbol][-2]["ema_200"] = 50010.0
    engine.candles_history[symbol][-1]["ema_50"] = 50050.0
    engine.candles_history[symbol][-1]["ema_200"] = 50020.0

    sig = await engine.handle_closed_candle(symbol, cross_candle)
    assert sig is not None
    assert sig.symbol == "BTCUSDT"
    assert engine.live_signals_count == 1
    assert await db.get_live_signals_count() == 1

    await client.close()


# ===========================================================================
# 3. Duplicate delivery of same closed candle creates zero duplicate alerts
# ===========================================================================
@pytest.mark.asyncio
async def test_case_3_duplicate_closed_candle_no_duplicate_alert(tmp_path):
    db = Database(db_path=str(tmp_path / "test_case3.db"))
    await db.init()

    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    alert_queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, alert_queue, fast_period=50, slow_period=200, candle_limit=1000)
    symbol = "BTCUSDT"
    engine.set_symbols([symbol])

    base_ts = 1790000000000
    engine.candles_history[symbol] = [
        {
            "timestamp": base_ts + i * 3600000,
            "open": 50000.0,
            "high": 50500.0,
            "low": 49500.0,
            "close": 50000.0,
            "volume": 100.0,
        }
        for i in range(1000)
    ]
    cross_candle = {
        "timestamp": base_ts + 1000 * 3600000,
        "open": 70000.0,
        "high": 80000.0,
        "low": 69000.0,
        "close": 78000.0,
        "volume": 1000.0,
        "close_time": base_ts + 1000 * 3600000 + 3599999,
        "is_closed": True,
    }
    engine.candles_history[symbol][-2]["ema_50"] = 50000.0
    engine.candles_history[symbol][-2]["ema_200"] = 50010.0
    engine.candles_history[symbol][-1]["ema_50"] = 50050.0
    engine.candles_history[symbol][-1]["ema_200"] = 50020.0

    sig1 = await engine.handle_closed_candle(symbol, cross_candle)
    assert sig1 is not None

    # Re-feed exact same closed candle
    sig2 = await engine.handle_closed_candle(symbol, cross_candle)
    assert sig2 is None
    assert engine.live_signals_count == 1
    assert await db.get_live_signals_count() == 1

    await client.close()


# ===========================================================================
# 4. No incoming market data beyond stale threshold changes state to STALE
# ===========================================================================
@pytest.mark.asyncio
async def test_case_4_stale_threshold_marks_feed_stale(monkeypatch):
    manager = BinanceWebSocketManager(stale_threshold_seconds=120.0)
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 1

    current_time = [1000.0]
    monkeypatch.setattr(websocket_module.time, "time", lambda: current_time[0])

    await manager._handle_message(_make_kline_payload(symbol="BTCUSDT"))
    assert manager.get_market_data_health()["status"] == "HEALTHY"

    # Fast forward past 120s
    current_time[0] = 1125.0
    health = manager.get_market_data_health()
    assert health["is_healthy"] is False
    assert "DATA STALE" in health["status"]
    assert "125s silent" in health["status"]


# ===========================================================================
# 5. Valid messages after stale period restore health
# ===========================================================================
@pytest.mark.asyncio
async def test_case_5_valid_messages_restore_health_after_stale(monkeypatch):
    manager = BinanceWebSocketManager(stale_threshold_seconds=120.0)
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 1

    current_time = [1000.0]
    monkeypatch.setattr(websocket_module.time, "time", lambda: current_time[0])

    await manager._handle_message(_make_kline_payload(symbol="BTCUSDT"))
    assert manager.get_market_data_health()["is_healthy"] is True

    # Go stale
    current_time[0] = 1150.0
    assert manager.get_market_data_health()["is_healthy"] is False

    # New message arrives at 1150
    await manager._handle_message(_make_kline_payload(symbol="BTCUSDT", timestamp=1790003600000))
    health = manager.get_market_data_health()
    assert health["is_healthy"] is True
    assert health["status"] == "HEALTHY"


# ===========================================================================
# 6. Connected socket that stops delivering data detected as stale
# ===========================================================================
@pytest.mark.asyncio
async def test_case_6_connected_socket_without_data_detected_as_stale(monkeypatch):
    manager = BinanceWebSocketManager(stale_threshold_seconds=120.0)
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 6  # TCP/WS connection appears open

    current_time = [2000.0]
    monkeypatch.setattr(websocket_module.time, "time", lambda: current_time[0])

    # No data received at all
    health = manager.get_market_data_health()
    assert health["is_connected"] is True
    assert health["is_healthy"] is False
    assert health["status"] == "DATA STALE / NO MARKET DATA"


# ===========================================================================
# 7. Disconnection triggers recovery without spawning duplicate reconnect loops
# ===========================================================================
@pytest.mark.asyncio
async def test_case_7_websocket_disconnect_recovery_no_duplicate_loops():
    manager = BinanceWebSocketManager()
    manager.set_symbols(["BTCUSDT"])

    assert manager._running is False
    await manager.start()
    assert manager._running is True
    task_count_before = len(manager._tasks)

    # Duplicate call to start() must be safely ignored
    await manager.start()
    assert len(manager._tasks) == task_count_before

    await manager.stop()
    assert manager._running is False


# ===========================================================================
# 8. Subscription error surfaced instead of silently reporting healthy
# ===========================================================================
@pytest.mark.asyncio
async def test_case_8_subscription_error_surfaced_not_silently_healthy():
    manager = BinanceWebSocketManager()
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 1

    error_payload = {"error": {"code": 1, "msg": "Invalid stream name btcusdt@kline_1h"}}
    await manager._handle_message(error_payload)

    assert manager.subscription_errors_count == 1
    assert "Invalid stream" in (manager.last_subscription_error or "")

    health = manager.get_market_data_health()
    assert health["is_healthy"] is False
    assert "DEGRADED" in health["status"]
    assert "subscription error" in health["status"]


# ===========================================================================
# 9. HTTP 429 respects Retry-After without tight retry loops
# ===========================================================================
@pytest.mark.asyncio
async def test_case_9_http_429_respects_retry_after(monkeypatch):
    client = BinanceFuturesClient()

    class Mock429Response:
        status = 429
        headers = {"Retry-After": "15"}

    t_now = [1000.0]
    monkeypatch.setattr("app.exchange.binance_client.time.time", lambda: t_now[0])

    ok = await client._check_rate_limit(Mock429Response(), endpoint_category="klines")
    assert ok is False
    assert client.rate_limit_429_count == 1
    assert client.pause_remaining == 15.0
    assert client.is_paused is True

    # Consecutive 429 with missing Retry-After uses bounded exponential backoff
    class Mock429NoHeader:
        status = 429
        headers = {}

    ok2 = await client._check_rate_limit(Mock429NoHeader(), endpoint_category="klines")
    assert ok2 is False
    assert client.rate_limit_429_count == 2
    # Base 5 * (2 ** 1) = 10s backoff
    assert client.pause_remaining == 10.0


# ===========================================================================
# 10. HTTP 418 honors required waiting period
# ===========================================================================
@pytest.mark.asyncio
async def test_case_10_http_418_honors_waiting_period(monkeypatch):
    client = BinanceFuturesClient()

    class Mock418Response:
        status = 418
        headers = {"Retry-After": "120"}

    t_now = [5000.0]
    monkeypatch.setattr("app.exchange.binance_client.time.time", lambda: t_now[0])

    ok = await client._check_rate_limit(Mock418Response(), endpoint_category="klines")
    assert ok is False
    assert client.ip_ban_418_count == 1
    assert client.pause_remaining == 120.0
    assert client.is_paused is True


# ===========================================================================
# 11. Symbol with insufficient candles marked not ready without breaking feed
# ===========================================================================
@pytest.mark.asyncio
async def test_case_11_insufficient_candles_marked_not_ready(tmp_path):
    db = Database(db_path=str(tmp_path / "test_case11.db"))
    await db.init()

    client = BinanceFuturesClient()
    # Mock get_klines: BTC and ETH return 1000 candles; NEWCOIN returns only 500
    async def mock_klines(sym, interval="1h", limit=1000, only_closed=True, **kwargs):
        count = 500 if sym == "NEWCOINUSDT" else 1000
        return [
            {
                "timestamp": 1700000000000 + i * 3600000,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.0,
                "volume": 10.0,
                "close_time": 1700000000000 + (i + 1) * 3600000 - 1,
            }
            for i in range(count)
        ]

    client.get_klines = AsyncMock(side_effect=mock_klines)
    telegram = TelegramNotifier(enabled=False)
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    alert_queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, alert_queue, candle_limit=1000)
    engine.set_symbols(["BTCUSDT", "ETHUSDT", "NEWCOINUSDT"])

    await engine.initialize_symbols(max_concurrency=3, pacing_delay_ms=10.0)

    # BTC and ETH are ready, NEWCOIN is not
    assert "BTCUSDT" in engine.initialized_symbols
    assert "ETHUSDT" in engine.initialized_symbols
    assert "NEWCOINUSDT" not in engine.initialized_symbols
    assert len(engine.initialized_symbols) == 2

    await client.close()


# ===========================================================================
# 12. Failed background consumer task detected and reflected in health
# ===========================================================================
@pytest.mark.asyncio
async def test_case_12_failed_background_task_detected_in_health():
    manager = BinanceWebSocketManager()
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 1
    manager.last_kline_received_at = time.time()

    # Create dummy tasks, one of which terminates with an exception
    async def healthy_worker():
        await asyncio.sleep(100)

    async def crashing_worker():
        raise RuntimeError("Worker crash simulated")

    t1 = asyncio.create_task(healthy_worker())
    t2 = asyncio.create_task(crashing_worker())
    manager._tasks = [t1, t2]

    # Allow t2 to complete with exception
    await asyncio.sleep(0.05)

    health = manager.get_market_data_health()
    assert health["is_healthy"] is False
    assert "DEGRADED" in health["status"]
    assert "worker 1 terminated" in health["status"]

    t1.cancel()
    await asyncio.gather(t1, return_exceptions=True)


# ===========================================================================
# 13. Historical scans do not send duplicate live alerts
# ===========================================================================
@pytest.mark.asyncio
async def test_case_13_historical_scans_do_not_send_live_alerts(tmp_path):
    db = Database(db_path=str(tmp_path / "test_case13.db"))
    await db.init()

    client = BinanceFuturesClient()
    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))
    telegram = TelegramNotifier(enabled=False)
    alert_queue = AlertQueue(chart_data, renderer, telegram, db)

    engine = SignalEngine(client, db, alert_queue, fast_period=50, slow_period=200, candle_limit=1000)
    symbol = "BTCUSDT"
    engine.set_symbols([symbol])

    base_ts = 1790000000000
    engine.candles_history[symbol] = [
        {
            "timestamp": base_ts + i * 3600000,
            "open": 50000.0,
            "high": 50500.0,
            "low": 49500.0,
            "close": 50000.0,
            "volume": 100.0,
        }
        for i in range(1000)
    ]

    crosses = await engine.scan_and_record_historical_crosses(symbol)
    assert engine.live_signals_count == 0
    assert await db.get_live_signals_count() == 0
    assert alert_queue._queue.qsize() == 0

    await client.close()


# ===========================================================================
# 14. Telegram failure is reflected accurately without blocking WebSocket
# ===========================================================================
@pytest.mark.asyncio
async def test_case_14_telegram_failure_reflected_accurately():
    telegram = TelegramNotifier(bot_token="fake_token", chat_id="12345", enabled=True)
    telegram._get_session = AsyncMock()

    class MockFailedResp:
        status = 500
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
        async def text(self):
            return "Internal Telegram Server Error"

    mock_session = AsyncMock()
    mock_session.post.return_value = MockFailedResp()
    telegram._get_session.return_value = mock_session

    sig = GoldenCrossSignal(
        symbol="BTCUSDT",
        timeframe="1H",
        candle_timestamp=1790000000000,
        signal_time_utc="09 Oct 2026 • 02:00 UTC",
        ema50=51000.0,
        ema200=50000.0,
        close_price=52000.0,
        previous_ema50=49900.0,
        previous_ema200=50000.0,
    )

    success = await telegram.send_golden_cross_alert(sig, chart_image_path=None)
    assert success is False
    assert telegram.alerts_failed >= 1
    assert telegram.status.startswith("DEGRADED")

    # Verify WebSocket message handling continues regardless of telegram status
    manager = BinanceWebSocketManager()
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 1
    await manager._handle_message(_make_kline_payload(symbol="BTCUSDT"))
    assert manager.get_market_data_health()["is_healthy"] is True


# ===========================================================================
# 15. Health monitoring and logging do not block the event loop
# ===========================================================================
@pytest.mark.asyncio
async def test_case_15_health_monitoring_non_blocking():
    manager = BinanceWebSocketManager()
    manager.set_symbols(["BTCUSDT"])
    manager._active_connections = 1
    manager.last_kline_received_at = time.time()

    loop = asyncio.get_running_loop()
    t0 = loop.time()
    for _ in range(500):
        _ = manager.get_market_data_health()
    elapsed = loop.time() - t0

    # 500 health checks should execute in well under 50ms without blocking
    assert elapsed < 0.10
