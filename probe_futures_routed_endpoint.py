"""Isolated comparative diagnostic: Unrouted (OLD) vs Routed /market/ (NEW) Binance Futures WebSocket.

Endpoints tested:
1. OLD: wss://fstream.binance.com/ws/btcusdt@kline_1h
2. NEW: wss://fstream.binance.com/market/ws/btcusdt@kline_1h

Strictly read-only and zero-dependency:
- No REST
- No Spot
- No database
- No Telegram
- No SignalEngine
- No secrets or credentials
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import socket
import ssl
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import websockets


async def probe_single_endpoint(
    name: str,
    url: str,
    listen_seconds: float = 15.0,
    ping_count: int = 2,
    ping_timeout: float = 5.0,
) -> Dict[str, Any]:
    print(f"\n{'=' * 65}", flush=True)
    print(f"PROBING [{name}]: {url}", flush=True)
    print(f"{'=' * 65}", flush=True)

    result: Dict[str, Any] = {
        "name": name,
        "url": url,
        "dns_pass": False,
        "tcp_pass": False,
        "tls_pass": False,
        "ws_handshake_pass": False,
        "pings_success": 0,
        "ping_rtts_ms": [],
        "app_frames_count": 0,
        "sample_events": [],
        "close_code": None,
        "close_reason": None,
        "final_state": "UNKNOWN",
        "duration_sec": 0.0,
    }

    parsed = urlparse(url)
    host = parsed.hostname or "fstream.binance.com"
    port = parsed.port or 443

    t_start = time.monotonic()

    # 1. DNS
    try:
        t0 = time.monotonic()
        addrs = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        ipv4s = [a[4][0] for a in addrs if a[0] == socket.AF_INET]
        target_ip = ipv4s[0] if ipv4s else host
        dns_ms = (time.monotonic() - t0) * 1000
        result["dns_pass"] = True
        print(f"[DNS] PASS ({dns_ms:.1f}ms) -> {target_ip}", flush=True)
    except Exception as exc:
        print(f"[DNS] FAIL: {exc}", flush=True)
        result["final_state"] = "DNS_FAIL"
        return result

    # 2. TCP + TLS
    try:
        t0 = time.monotonic()
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_sock.settimeout(5.0)
        raw_sock.connect((target_ip, port))
        tcp_ms = (time.monotonic() - t0) * 1000
        result["tcp_pass"] = True

        ssl_ctx = ssl.create_default_context()
        with ssl_ctx.wrap_socket(raw_sock, server_hostname=host) as tls_sock:
            tls_ms = (time.monotonic() - t0) * 1000 - tcp_ms
            result["tls_pass"] = True
            cipher = tls_sock.cipher()
            print(f"[TCP+TLS] PASS (tcp={tcp_ms:.1f}ms, tls={tls_ms:.1f}ms) cipher={cipher[0] if cipher else 'N/A'}", flush=True)
    except Exception as exc:
        print(f"[TCP+TLS] FAIL: {exc}", flush=True)
        result["final_state"] = "TCP_TLS_FAIL"
        return result

    # 3. WebSocket Handshake & Frames
    ws = None
    try:
        t0 = time.monotonic()
        ws = await websockets.connect(
            url,
            open_timeout=10.0,
            close_timeout=3.0,
            ping_interval=None,
        )
        hs_ms = (time.monotonic() - t0) * 1000
        result["ws_handshake_pass"] = True
        print(f"[WS_HANDSHAKE] PASS ({hs_ms:.1f}ms) HTTP 101 Switching Protocols", flush=True)

        # 4. Explicit Pings
        for i in range(ping_count):
            pt0 = time.monotonic()
            try:
                waiter = await ws.ping()
                await asyncio.wait_for(waiter, timeout=ping_timeout)
                prtt = (time.monotonic() - pt0) * 1000
                result["pings_success"] += 1
                result["ping_rtts_ms"].append(round(prtt, 2))
                print(f"[PING] [{i+1}/{ping_count}] PASS rtt={prtt:.2f}ms", flush=True)
            except Exception as pe:
                print(f"[PING] [{i+1}/{ping_count}] FAIL: {pe}", flush=True)
            if i < ping_count - 1:
                await asyncio.sleep(0.5)

        # 5. Listen for Application Frames
        print(f"[APP_LISTEN] Listening for application frames ({listen_seconds}s timeout)...", flush=True)
        deadline = time.monotonic() + listen_seconds
        while time.monotonic() < deadline:
            remaining = max(0.01, deadline - time.monotonic())
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
                result["app_frames_count"] += 1
                try:
                    payload = json.loads(msg)
                    event = payload.get("e")
                    symbol = payload.get("s") or payload.get("k", {}).get("s")
                    kline_closed = payload.get("k", {}).get("x")
                    close_price = payload.get("k", {}).get("c")
                    info = f"event={event} symbol={symbol} closed={kline_closed} close={close_price}"
                    if len(result["sample_events"]) < 5:
                        result["sample_events"].append(info)
                    print(f"  -> APP_FRAME #{result['app_frames_count']}: {info} len={len(msg)}", flush=True)
                except Exception:
                    print(f"  -> APP_FRAME #{result['app_frames_count']}: (non-json) len={len(msg)}", flush=True)
            except asyncio.TimeoutError:
                break
            except websockets.exceptions.ConnectionClosed as cc:
                result["close_code"] = cc.rcvd.code if hasattr(cc, "rcvd") and cc.rcvd else cc.code
                result["close_reason"] = cc.rcvd.reason if hasattr(cc, "rcvd") and cc.rcvd else cc.reason
                print(f"[CLOSED] Peer closed connection: code={result['close_code']} reason={result['close_reason']}", flush=True)
                break

        state_name = getattr(ws.state, "name", str(ws.state))
        result["close_code"] = getattr(ws, "close_code", result["close_code"])
        result["close_reason"] = getattr(ws, "close_reason", result["close_reason"])

        if result["app_frames_count"] > 0:
            result["final_state"] = "ACTIVE_DATA_STREAM"
        elif result["close_code"] is not None:
            result["final_state"] = "SERVER_CLOSED"
        elif result["pings_success"] > 0:
            result["final_state"] = "SILENT_OPEN_SOCKET"
        else:
            result["final_state"] = "FROZEN_SOCKET"

    except Exception as exc:
        print(f"[ERROR] {type(exc).__name__}: {exc}", flush=True)
        result["final_state"] = f"ERROR_{type(exc).__name__}"
    finally:
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    result["duration_sec"] = round(time.monotonic() - t_start, 2)
    print(f"[RESULT] {name}: {result['final_state']} (frames={result['app_frames_count']}, pings={result['pings_success']}/{ping_count})", flush=True)
    return result


async def run_comparison(listen_seconds: float = 15.0) -> int:
    print("=" * 65, flush=True)
    print("BINANCE FUTURES WEBSOCKET ROUTING COMPARISON PROBE", flush=True)
    print(f"Python: {platform.python_version()} | websockets: {getattr(websockets, '__version__', 'unknown')}", flush=True)
    print("=" * 65, flush=True)

    endpoints = [
        ("OLD_UNROUTED", "wss://fstream.binance.com/ws/btcusdt@kline_1h"),
        ("NEW_ROUTED_MARKET", "wss://fstream.binance.com/market/ws/btcusdt@kline_1h"),
    ]

    results: List[Dict[str, Any]] = []
    for name, url in endpoints:
        res = await probe_single_endpoint(name, url, listen_seconds=listen_seconds)
        results.append(res)
        await asyncio.sleep(1.0)

    print("\n" + "=" * 65, flush=True)
    print("COMPARATIVE SUMMARY", flush=True)
    print("=" * 65, flush=True)
    print(f"{'Endpoint':<20} | {'Handshake':<10} | {'Pings':<8} | {'Frames':<8} | {'Classification'}", flush=True)
    print("-" * 65, flush=True)
    for r in results:
        hs = "PASS" if r["ws_handshake_pass"] else "FAIL"
        pings = f"{r['pings_success']}/2"
        frames = str(r["app_frames_count"])
        print(f"{r['name']:<20} | {hs:<10} | {pings:<8} | {frames:<8} | {r['final_state']}", flush=True)
    print("-" * 65, flush=True)

    old_res = results[0]
    new_res = results[1]

    if new_res["final_state"] == "ACTIVE_DATA_STREAM" and old_res["final_state"] == "SILENT_OPEN_SOCKET":
        print("\n[VERDICT] PROVEN:", flush=True)
        print("  - The OLD unrouted endpoint ('/ws/') connects and pongs, but sends ZERO market data frames.")
        print("  - The NEW routed endpoint ('/market/ws/') successfully receives real-time kline application data.")
        print("  - The silence on VPS was caused by Binance's Base URL routing migration to '/market'.")
        return 0
    elif new_res["final_state"] == "ACTIVE_DATA_STREAM" and old_res["final_state"] == "ACTIVE_DATA_STREAM":
        print("\n[VERDICT] Both endpoints received application data.", flush=True)
        return 0
    else:
        print(f"\n[VERDICT] Inconclusive or different pattern: OLD={old_res['final_state']}, NEW={new_res['final_state']}", flush=True)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare Unrouted vs Routed Binance Futures WebSocket")
    parser.add_argument("--listen-seconds", type=float, default=15.0, help="Listen duration in seconds per endpoint (default: 15.0)")
    args = parser.parse_args()
    return asyncio.run(run_comparison(listen_seconds=args.listen_seconds))


if __name__ == "__main__":
    sys.exit(main())
