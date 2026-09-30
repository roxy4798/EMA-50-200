"""Isolated read-only Data Plane WebSocket probe for Binance USD-M Futures.

Target: wss://fstream.binance.com/ws/btcusdt@kline_1h

Strictly diagnostic:
- No database access
- No Telegram delivery
- No credentials or .env access
- No trading strategy or SignalEngine modification
- No Spot endpoints or REST fallbacks
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import platform
import socket
import ssl
import sys
import time
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import websockets


class WebSocketDataPlaneProbe:
    def __init__(
        self,
        url: str = "wss://fstream.binance.com/ws/btcusdt@kline_1h",
        listen_seconds: float = 35.0,
        ping_count: int = 3,
        ping_timeout: float = 5.0,
    ) -> None:
        self.url = url
        self.listen_seconds = listen_seconds
        self.ping_count = ping_count
        self.ping_timeout = ping_timeout
        self.results: Dict[str, Any] = {}

    def log(self, tag: str, message: str) -> None:
        elapsed = time.monotonic() - self.start_time if hasattr(self, "start_time") else 0.0
        print(f"[{elapsed:06.2f}s] {tag}: {message}", flush=True)

    async def run(self) -> int:
        self.start_time = time.monotonic()
        print("=" * 65, flush=True)
        print("NEXORA DATA PLANE WEBSOCKET PROBE", flush=True)
        print("=" * 65, flush=True)
        self.log("TARGET_URL", self.url)
        self.log("PYTHON_VER", f"{platform.python_version()} ({platform.system()} {platform.machine()})")
        self.log("WEBSOCKETS", getattr(websockets, "__version__", "unknown"))

        parsed = urlparse(self.url)
        host = parsed.hostname or "fstream.binance.com"
        port = parsed.port or 443

        # 1. DNS Resolution
        try:
            t0 = time.monotonic()
            addr_info = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            dns_time = (time.monotonic() - t0) * 1000
            ipv4_addrs = [ai[4][0] for ai in addr_info if ai[0] == socket.AF_INET]
            self.log("DNS_RESOLVE", f"PASS ({dns_time:.1f}ms) IPv4={','.join(set(ipv4_addrs)) or 'NONE'}")
        except Exception as exc:
            self.log("DNS_RESOLVE", f"FAIL {type(exc).__name__}: {exc}")
            return 1

        # 2. TCP & TLS SNI Check
        target_ip = ipv4_addrs[0] if ipv4_addrs else host
        try:
            t0 = time.monotonic()
            raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_sock.settimeout(5.0)
            raw_sock.connect((target_ip, port))
            tcp_time = (time.monotonic() - t0) * 1000
            self.log("TCP_CONNECT", f"PASS ({tcp_time:.1f}ms) to {target_ip}:{port}")

            t0 = time.monotonic()
            ssl_ctx = ssl.create_default_context()
            with ssl_ctx.wrap_socket(raw_sock, server_hostname=host) as tls_sock:
                tls_time = (time.monotonic() - t0) * 1000
                cipher = tls_sock.cipher()
                self.log(
                    "TLS_HANDSHAKE",
                    f"PASS ({tls_time:.1f}ms) version={tls_sock.version()} cipher={cipher[0] if cipher else 'NONE'}",
                )
        except Exception as exc:
            self.log("TCP_TLS", f"FAIL {type(exc).__name__}: {exc}")
            return 1

        # 3. WebSocket Connect & Handshake
        ws = None
        state_result = "UNKNOWN"
        app_messages_count = 0
        pings_passed = 0
        close_code: Optional[int] = None
        close_reason: Optional[str] = None

        try:
            t0 = time.monotonic()
            self.log("WS_CONNECT", f"Initiating handshake to {self.url}...")
            ws = await websockets.connect(
                self.url,
                open_timeout=10.0,
                close_timeout=3.0,
                ping_interval=None,  # Disabled automatic keepalive to measure explicit pings
            )
            hs_time = (time.monotonic() - t0) * 1000
            self.log("WS_HANDSHAKE", f"PASS ({hs_time:.1f}ms) HTTP 101 Switching Protocols")

            # 4. Explicit Ping/Pong Control Frame Audit
            self.log("CONTROL_FRAMES", f"Sending {self.ping_count} explicit RFC 6455 PING frames to server...")
            for i in range(self.ping_count):
                pt0 = time.monotonic()
                try:
                    waiter = await ws.ping()
                    await asyncio.wait_for(waiter, timeout=self.ping_timeout)
                    prtt = (time.monotonic() - pt0) * 1000
                    pings_passed += 1
                    self.log("PING_PONG", f"[{i+1}/{self.ping_count}] PASS rtt={prtt:.2f}ms")
                except asyncio.TimeoutError:
                    self.log("PING_PONG", f"[{i+1}/{self.ping_count}] FAIL (Pong timeout > {self.ping_timeout}s)")
                except Exception as exc:
                    self.log("PING_PONG", f"[{i+1}/{self.ping_count}] ERROR {type(exc).__name__}: {exc}")
                if i < self.ping_count - 1:
                    await asyncio.sleep(1.0)

            # 5. Passive Application Frame Reception Audit
            self.log("APP_FRAMES", f"Listening for application messages (timeout={self.listen_seconds}s)...")
            listen_deadline = time.monotonic() + self.listen_seconds
            while time.monotonic() < listen_deadline:
                remaining = max(0.01, listen_deadline - time.monotonic())
                try:
                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    app_messages_count += 1
                    preview = raw_msg[:120].replace("\n", "")
                    # Inspect parsed payload
                    try:
                        parsed_json = json.loads(raw_msg)
                        event = parsed_json.get("e")
                        symbol = parsed_json.get("s") or parsed_json.get("k", {}).get("s")
                        kline_closed = parsed_json.get("k", {}).get("x")
                        self.log(
                            "APP_FRAME_RECEIVED",
                            f"msg #{app_messages_count} len={len(raw_msg)} event={event} "
                            f"symbol={symbol} closed={kline_closed} preview={preview}",
                        )
                    except Exception:
                        self.log("APP_FRAME_RECEIVED", f"msg #{app_messages_count} len={len(raw_msg)} preview={preview}")
                except asyncio.TimeoutError:
                    break
                except websockets.exceptions.ConnectionClosed as cc:
                    close_code = cc.rcvd.code if hasattr(cc, "rcvd") and cc.rcvd else cc.code
                    close_reason = cc.rcvd.reason if hasattr(cc, "rcvd") and cc.rcvd else cc.reason
                    self.log("SERVER_CLOSED", f"Connection terminated by peer: code={close_code} reason={close_reason}")
                    break

            # 6. Socket State Determination
            # Check final state safely across websockets 14-17
            state_name = getattr(ws.state, "name", str(ws.state))
            close_code = getattr(ws, "close_code", close_code)
            close_reason = getattr(ws, "close_reason", close_reason)
            self.log("SOCKET_STATE", f"state={state_name} close_code={close_code} close_reason={close_reason}")

            if app_messages_count > 0:
                state_result = "ACTIVE_DATA_STREAM"
            elif close_code is not None:
                state_result = "SERVER_DISCONNECTED"
            elif pings_passed == self.ping_count:
                state_result = "SILENT_OPEN_SOCKET"
            elif pings_passed == 0:
                state_result = "FROZEN_SOCKET"
            else:
                state_result = "UNSTABLE_SOCKET"

        except Exception as exc:
            self.log("TRANSPORT_ERROR", f"{type(exc).__name__}: {exc}")
            state_result = "TRANSPORT_FAILED"
        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass

        # 7. Summary Report
        print("=" * 65, flush=True)
        print("DATA PLANE DIAGNOSTIC SUMMARY", flush=True)
        print("=" * 65, flush=True)
        self.log("CLASSIFICATION", state_result)
        self.log("APP_FRAMES_COUNT", str(app_messages_count))
        self.log("PING_PONG_SUCCESS", f"{pings_passed}/{self.ping_count}")
        self.log("CLOSE_CODE", str(close_code))
        self.log("CLOSE_REASON", str(close_reason))

        if state_result == "SILENT_OPEN_SOCKET":
            print(
                "\n[ANALYSIS] TCP, TLS, and WebSocket handshakes succeed. Binance answers control\n"
                "pings immediately (sub-second RTT). However, Binance Futures sends 0 application\n"
                "frames on '/ws/btcusdt@kline_1h'. The socket is 100% healthy at the transport\n"
                "layer, but completely silent at the application streaming layer.\n",
                flush=True,
            )
        elif state_result == "ACTIVE_DATA_STREAM":
            print(
                f"\n[ANALYSIS] Success. Received {app_messages_count} application frames from Binance Futures.\n",
                flush=True,
            )
        elif state_result == "SERVER_DISCONNECTED":
            print(
                f"\n[ANALYSIS] The server actively closed the connection with code {close_code}.\n",
                flush=True,
            )
        return 0 if state_result == "ACTIVE_DATA_STREAM" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Isolated Binance Futures Data Plane WebSocket Probe")
    parser.add_argument("--listen-seconds", type=float, default=20.0, help="Seconds to listen for app frames (default: 20)")
    parser.add_argument("--pings", type=int, default=3, help="Number of test pings (default: 3)")
    args = parser.parse_args()

    probe = WebSocketDataPlaneProbe(
        url="wss://fstream.binance.com/ws/btcusdt@kline_1h",
        listen_seconds=args.listen_seconds,
        ping_count=args.pings,
    )
    return asyncio.run(probe.run())


if __name__ == "__main__":
    sys.exit(main())
