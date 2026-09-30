"""Unit tests for probe_futures_data_plane.py."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

import sys
from pathlib import Path

repo_root = str(Path(__file__).resolve().parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from probe_futures_data_plane import WebSocketDataPlaneProbe


@pytest.mark.asyncio
async def test_probe_active_stream(monkeypatch):
    """Verifies probe correctly classifies active data stream when app frames arrive."""
    probe = WebSocketDataPlaneProbe(
        url="wss://fstream.binance.com/ws/btcusdt@kline_1h",
        listen_seconds=0.1,
        ping_count=1,
    )

    class MockWS:
        def __init__(self):
            self.state = MagicMock(name="OPEN")
            self.state.name = "OPEN"
            self.close_code = None
            self.close_reason = None
            self.closed = False

        async def ping(self):
            fut = asyncio.Future()
            fut.set_result(None)
            return fut

        async def recv(self):
            return '{"e":"kline","s":"BTCUSDT","k":{"x":false,"s":"BTCUSDT"}}'

        async def close(self):
            self.closed = True

    monkeypatch.setattr("probe_futures_data_plane.socket.getaddrinfo", lambda *a, **kw: [(2, 1, 6, "", ("1.2.3.4", 443))])
    monkeypatch.setattr("probe_futures_data_plane.socket.socket.connect", lambda self, addr: None)
    monkeypatch.setattr("probe_futures_data_plane.ssl.SSLContext.wrap_socket", lambda self, sock, server_hostname=None: MagicMock(version=lambda: "TLSv1.3", cipher=lambda: ("TLS_AES_128_GCM_SHA256",)))
    monkeypatch.setattr("probe_futures_data_plane.websockets.connect", AsyncMock(return_value=MockWS()))

    code = await probe.run()
    assert code == 0


@pytest.mark.asyncio
async def test_probe_silent_open_socket(monkeypatch):
    """Verifies probe correctly classifies silent socket when pings pass but no app frames arrive."""
    probe = WebSocketDataPlaneProbe(
        url="wss://fstream.binance.com/ws/btcusdt@kline_1h",
        listen_seconds=0.05,
        ping_count=2,
    )

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

    monkeypatch.setattr("probe_futures_data_plane.socket.getaddrinfo", lambda *a, **kw: [(2, 1, 6, "", ("1.2.3.4", 443))])
    monkeypatch.setattr("probe_futures_data_plane.socket.socket.connect", lambda self, addr: None)
    monkeypatch.setattr("probe_futures_data_plane.ssl.SSLContext.wrap_socket", lambda self, sock, server_hostname=None: MagicMock(version=lambda: "TLSv1.3", cipher=lambda: ("TLS_AES_128_GCM_SHA256",)))
    monkeypatch.setattr("probe_futures_data_plane.websockets.connect", AsyncMock(return_value=MockSilentWS()))

    code = await probe.run()
    assert code == 1
