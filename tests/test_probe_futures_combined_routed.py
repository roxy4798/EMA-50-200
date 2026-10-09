"""Unit tests for probe_futures_combined_routed.py."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest
import sys
from pathlib import Path

repo_root = str(Path(__file__).resolve().parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from probe_futures_combined_routed import probe_combined_endpoint


@pytest.mark.asyncio
async def test_probe_combined_active(monkeypatch):
    class MockWS:
        def __init__(self):
            self.state = MagicMock()
            self.state.name = "OPEN"
            self.close_code = None
            self.close_reason = None

        async def ping(self):
            fut = asyncio.Future()
            fut.set_result(None)
            return fut

        async def recv(self):
            return '{"stream":"btcusdt@kline_1h","data":{"e":"kline","s":"BTCUSDT","k":{"s":"BTCUSDT","x":false,"c":"61000"}}}'

        async def close(self):
            pass

    monkeypatch.setattr("probe_futures_combined_routed.socket.getaddrinfo", lambda *a, **kw: [(2, 1, 6, "", ("1.2.3.4", 443))])
    monkeypatch.setattr("probe_futures_combined_routed.socket.socket.connect", lambda self, addr: None)
    monkeypatch.setattr("probe_futures_combined_routed.ssl.SSLContext.wrap_socket", lambda self, sock, server_hostname=None: MagicMock(cipher=lambda: ("TLS_AES_128_GCM_SHA256",)))
    monkeypatch.setattr("probe_futures_combined_routed.websockets.connect", AsyncMock(return_value=MockWS()))

    res = await probe_combined_endpoint("TEST_COMBINED", "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1h", listen_seconds=0.1, ping_count=1)
    assert res["final_state"] == "ACTIVE_DATA_STREAM"
    assert "btcusdt@kline_1h" in res["streams_received"]
    assert "BTCUSDT" in res["symbols_received"]


@pytest.mark.asyncio
async def test_probe_combined_silent(monkeypatch):
    class MockSilentWS:
        def __init__(self):
            self.state = MagicMock()
            self.state.name = "OPEN"
            self.close_code = None
            self.close_reason = None

        async def ping(self):
            fut = asyncio.Future()
            fut.set_result(None)
            return fut

        async def recv(self):
            await asyncio.sleep(1.0)
            return "msg"

        async def close(self):
            pass

    monkeypatch.setattr("probe_futures_combined_routed.socket.getaddrinfo", lambda *a, **kw: [(2, 1, 6, "", ("1.2.3.4", 443))])
    monkeypatch.setattr("probe_futures_combined_routed.socket.socket.connect", lambda self, addr: None)
    monkeypatch.setattr("probe_futures_combined_routed.ssl.SSLContext.wrap_socket", lambda self, sock, server_hostname=None: MagicMock(cipher=lambda: ("TLS_AES_128_GCM_SHA256",)))
    monkeypatch.setattr("probe_futures_combined_routed.websockets.connect", AsyncMock(return_value=MockSilentWS()))

    res = await probe_combined_endpoint("TEST_COMBINED_SILENT", "wss://fstream.binance.com/stream?streams=btcusdt@kline_1h", listen_seconds=0.05, ping_count=1)
    assert res["final_state"] == "SILENT_OPEN_SOCKET"
    assert res["app_frames_count"] == 0
