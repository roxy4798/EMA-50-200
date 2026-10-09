"""Unit tests for probe_futures_routed_endpoint.py."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest
import sys
from pathlib import Path

repo_root = str(Path(__file__).resolve().parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from probe_futures_routed_endpoint import probe_single_endpoint, run_comparison


@pytest.mark.asyncio
async def test_probe_single_endpoint_active_mock(monkeypatch):
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
            return '{"e":"kline","s":"BTCUSDT","k":{"x":false,"s":"BTCUSDT","c":"60000"}}'

        async def close(self):
            pass

    monkeypatch.setattr("probe_futures_routed_endpoint.socket.getaddrinfo", lambda *a, **kw: [(2, 1, 6, "", ("1.2.3.4", 443))])
    monkeypatch.setattr("probe_futures_routed_endpoint.socket.socket.connect", lambda self, addr: None)
    monkeypatch.setattr("probe_futures_routed_endpoint.ssl.SSLContext.wrap_socket", lambda self, sock, server_hostname=None: MagicMock(cipher=lambda: ("TLS_AES_128_GCM_SHA256",)))
    monkeypatch.setattr("probe_futures_routed_endpoint.websockets.connect", AsyncMock(return_value=MockWS()))

    res = await probe_single_endpoint("TEST_ACTIVE", "wss://fstream.binance.com/market/ws/btcusdt@kline_1h", listen_seconds=0.1, ping_count=1)
    assert res["final_state"] == "ACTIVE_DATA_STREAM"
    assert res["app_frames_count"] >= 1


@pytest.mark.asyncio
async def test_probe_single_endpoint_silent_mock(monkeypatch):
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

    monkeypatch.setattr("probe_futures_routed_endpoint.socket.getaddrinfo", lambda *a, **kw: [(2, 1, 6, "", ("1.2.3.4", 443))])
    monkeypatch.setattr("probe_futures_routed_endpoint.socket.socket.connect", lambda self, addr: None)
    monkeypatch.setattr("probe_futures_routed_endpoint.ssl.SSLContext.wrap_socket", lambda self, sock, server_hostname=None: MagicMock(cipher=lambda: ("TLS_AES_128_GCM_SHA256",)))
    monkeypatch.setattr("probe_futures_routed_endpoint.websockets.connect", AsyncMock(return_value=MockSilentWS()))

    res = await probe_single_endpoint("TEST_SILENT", "wss://fstream.binance.com/ws/btcusdt@kline_1h", listen_seconds=0.05, ping_count=1)
    assert res["final_state"] == "SILENT_OPEN_SOCKET"
    assert res["app_frames_count"] == 0
