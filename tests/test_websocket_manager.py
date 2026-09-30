"""Regression test suite for BinanceWebSocketManager stream URL construction."""

from __future__ import annotations

import pytest
from app.exchange.websocket_manager import BinanceWebSocketManager


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
