"""Regression test suite for BinanceWebSocketManager stream URL construction."""

from __future__ import annotations

import pytest
import asyncio
from unittest.mock import AsyncMock
from app.exchange.websocket_manager import BinanceWebSocketManager
import app.exchange.websocket_manager as websocket_module


def test_websocket_url_normalization_standard_base():
    """Verifies that base URL 'wss://fstream.binance.com' generates correct combined stream URL."""
    ws = BinanceWebSocketManager(base_ws_url="wss://fstream.binance.com", timeframe="1h")
    symbols = ["btcusdt", "ethusdt"]
    url = ws.get_stream_url(symbols)
    
    assert url == "wss://fstream.binance.com/stream?streams=btcusdt@kline_1h/ethusdt@kline_1h"
    assert "/ws/stream" not in url


def test_websocket_url_normalization_with_ws_suffix():
    """Verifies that base URL 'wss://fstream.binance.com/ws' is normalized to valid stream URL."""
    ws = BinanceWebSocketManager(base_ws_url="wss://fstream.binance.com/ws", timeframe="1h")
    symbols = ["btcusdt", "ethusdt"]
    url = ws.get_stream_url(symbols)
    
    assert url == "wss://fstream.binance.com/stream?streams=btcusdt@kline_1h/ethusdt@kline_1h"
    assert "/ws/stream" not in url
    assert url.startswith("wss://fstream.binance.com/stream?streams=")


def test_websocket_url_normalization_with_trailing_slashes():
    """Verifies that trailing slashes are safely stripped from both formats."""
    ws1 = BinanceWebSocketManager(base_ws_url="wss://fstream.binance.com/", timeframe="1h")
    url1 = ws1.get_stream_url(["btcusdt"])
    assert url1 == "wss://fstream.binance.com/stream?streams=btcusdt@kline_1h"

    ws2 = BinanceWebSocketManager(base_ws_url="wss://fstream.binance.com/ws/", timeframe="1h")
    url2 = ws2.get_stream_url(["btcusdt"])
    assert url2 == "wss://fstream.binance.com/stream?streams=btcusdt@kline_1h"


def test_websocket_batching_stream_urls():
    """Verifies that 527 symbols generate 6 batches, all with properly constructed URLs."""
    ws = BinanceWebSocketManager(base_ws_url="wss://fstream.binance.com/ws", timeframe="1h")
    test_symbols = [f"sym{i}usdt" for i in range(527)]
    ws.symbols = test_symbols

    batch_size = 100
    batches = [ws.symbols[i : i + batch_size] for i in range(0, len(ws.symbols), batch_size)]
    assert len(batches) == 6
    assert len(batches[0]) == 100
    assert len(batches[5]) == 27

    for b in batches:
        url = ws.get_stream_url(b)
        assert url.startswith("wss://fstream.binance.com/stream?streams=")
        assert "/ws/stream" not in url
        # Verify streams parameter contains expected number of @kline_1h entries
        stream_param = url.split("streams=")[1]
        streams = stream_param.split("/")
        assert len(streams) == len(b)
        assert all(s.endswith("@kline_1h") for s in streams)


def _kline_message(symbol="BTCUSDT", closed=False):
    return {
        "e": "kline",
        "E": 1790003600000,
        "s": symbol,
        "k": {
            "t": 1790000000000,
            "T": 1790003599999,
            "s": symbol,
            "i": "1h",
            "o": "50000",
            "h": "50500",
            "l": "49500",
            "c": "50200",
            "v": "100",
            "x": closed,
        },
    }


@pytest.mark.asyncio
async def test_single_futures_raw_stream_kline_reaches_callback():
    received = []

    async def on_closed(symbol, candle):
        received.append((symbol, candle))

    manager = BinanceWebSocketManager(on_candle_closed=on_closed)
    manager.set_symbols(["BTCUSDT"])
    await manager._handle_message(_kline_message(closed=True))

    assert received[0][0] == "BTCUSDT"
    assert received[0][1]["is_closed"] is True
    assert manager.last_message_received_at is not None
    assert manager.last_kline_received_at is not None
    assert manager.last_closed_candle_received_at is not None
    assert manager.last_symbol_closed == "BTCUSDT"


@pytest.mark.asyncio
async def test_combined_stream_wrapper_and_open_candle_metrics():
    received = []

    async def on_closed(symbol, candle):
        received.append((symbol, candle))

    manager = BinanceWebSocketManager(on_candle_closed=on_closed)
    manager.set_symbols(["BTCUSDT"])
    msg = {"stream": "btcusdt@kline_1h", "data": _kline_message(closed=False)}
    await manager._handle_message(msg)

    assert manager.total_messages_received == 1
    assert manager.total_klines_received == 1
    assert manager.last_kline_received_at is not None
    assert manager.candles_closed_count == 0
    assert manager.last_closed_candle_received_at is None
    assert received == []


@pytest.mark.asyncio
async def test_combined_stream_closed_candle_reaches_callback():
    received = []

    async def on_closed(symbol, candle):
        received.append((symbol, candle))

    manager = BinanceWebSocketManager(on_candle_closed=on_closed)
    manager.set_symbols(["BTCUSDT"])
    msg = {"stream": "btcusdt@kline_1h", "data": _kline_message(closed=True)}
    await manager._handle_message(msg)

    assert received[0][0] == "BTCUSDT"
    assert received[0][1]["is_closed"] is True
    assert manager.total_messages_received == 1
    assert manager.candles_closed_count == 1


def test_partition_and_deduplicate_full_symbol_universe():
    manager = BinanceWebSocketManager()
    symbols = [f"sym{i}usdt" for i in range(527)]
    symbols.extend(["sym1usdt", "SYM1USDT"])
    manager.set_symbols(symbols)
    batches = manager.partition_symbols(manager.symbols, 100)

    assert len(manager.symbols) == 527
    assert len(batches) == 6
    assert [len(batch) for batch in batches] == [100, 100, 100, 100, 100, 27]
    assigned = [symbol for batch in batches for symbol in batch]
    assert len(assigned) == len(set(assigned))
    assert set(assigned) == set(manager.symbols)


@pytest.mark.asyncio
async def test_message_timeout_reconnects_silent_socket(monkeypatch):
    manager = BinanceWebSocketManager(message_timeout_seconds=0.01)
    manager._running = True
    connects = []
    sleep_calls = []

    class SilentSocket:
        async def recv(self):
            await asyncio.Future()

    class Connection:
        async def __aenter__(self):
            return SilentSocket()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def connect(*args, **kwargs):
        connects.append(args[0])
        return Connection()

    async def fast_sleep(_delay):
        sleep_calls.append(_delay)
        if len(connects) >= 2:
            manager._running = False

    monkeypatch.setattr(websocket_module.websockets, "connect", connect)
    monkeypatch.setattr(websocket_module.asyncio, "sleep", fast_sleep)
    await manager._run_batch_loop(0, ["btcusdt"])

    assert len(connects) == 2
    assert manager.reconnect_count == 2
    assert sleep_calls
