"""Read-only Binance USD-M Futures WebSocket diagnostic for the NEXORA host.

Run on the VPS from the project directory:
    python diagnose_live_websocket.py

No API keys, Telegram credentials, production database, or signal delivery are used.
The script stops before combined/full-universe probes if the BTCUSDT single stream
does not deliver kline data and a closed candle to the real SignalEngine callback.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import socket
import ssl
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.request import getproxies

import websockets

from app.engine.signal_engine import SignalEngine
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.indicators.ema import enrich_candles_with_ema
from app.persistence.database import Database


def network_diagnostics(host: str = "fstream.binance.com") -> None:
    print(f"HOST={host}")
    print(f"PYTHON={platform.python_version()} WEBSOCKETS={websockets.__version__}")
    proxy_names = sorted(name for name in getproxies() if name.lower() in {"http", "https", "all", "no"})
    print(f"PROXY_CONFIGURED={bool(proxy_names)} PROXY_TYPES={','.join(proxy_names) or 'none'}")

    try:
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError as exc:
        print(f"DNS=FAIL error={type(exc).__name__}: {exc}")
        return

    unique = {}
    for family, socktype, proto, _canonname, sockaddr in addresses:
        unique.setdefault(family, sockaddr)
    ipv4 = ",".join(addr[0] for fam, addr in unique.items() if fam == socket.AF_INET)
    ipv6 = ",".join(addr[0] for fam, addr in unique.items() if fam == socket.AF_INET6)
    print(f"DNS_IPV4={ipv4 or 'NONE'}")
    print(f"DNS_IPV6={ipv6 or 'NO_AAAA'}")

    context = ssl.create_default_context()
    for family, sockaddr in unique.items():
        label = "IPv4" if family == socket.AF_INET else "IPv6"
        raw_sock = socket.socket(family, socket.SOCK_STREAM)
        raw_sock.settimeout(4)
        try:
            raw_sock.connect(sockaddr)
            print(f"TCP_443_{label}=PASS remote={sockaddr[0]}")
            with context.wrap_socket(raw_sock, server_hostname=host) as tls_sock:
                print(f"TLS_SNI_{label}=PASS version={tls_sock.version()} cipher={tls_sock.cipher()[0]}")
            raw_sock = None  # type: ignore[assignment]
        except OSError as exc:
            print(f"TCP_TLS_{label}=FAIL error={type(exc).__name__}: {exc}")
        finally:
            if raw_sock is not None:
                raw_sock.close()


async def prepare_engine(symbols: List[str]):
    """Warm real SignalEngine instances from Futures REST into a temporary DB."""
    temp_dir = tempfile.TemporaryDirectory(prefix="nexora-ws-diagnostic-")
    database = Database(str(Path(temp_dir.name) / "diagnostic.sqlite"))
    await database.init()
    client = BinanceFuturesClient()

    class DiscardQueue:
        async def enqueue(self, _signal_id: int, _signal: Any) -> None:
            return None

    engine = SignalEngine(client, database, DiscardQueue())
    engine.set_symbols(symbols)
    for symbol in symbols:
        candles = await client.get_klines(symbol, interval="1h", limit=250, only_closed=True)
        if len(candles) < 250:
            raise RuntimeError(f"Only {len(candles)} closed Futures 1H candles for {symbol}; need 250 to warm the canonical frame")
        engine.candles_history[symbol] = enrich_candles_with_ema(candles, 50, 200).to_dict(orient="records")
        engine.initialized_symbols.add(symbol)
        await asyncio.sleep(0.2)
    return temp_dir, database, client, engine


class LiveProbe:
    def __init__(self, manager: BinanceWebSocketManager, engine: SignalEngine) -> None:
        self.manager = manager
        self.engine = engine
        self.callback_count = 0
        self.callback_symbols: List[str] = []
        self.detected_signals = 0

    async def on_candle_closed(self, symbol: str, candle: Dict[str, Any]) -> None:
        self.callback_count += 1
        self.callback_symbols.append(symbol)
        signal = await self.engine.handle_closed_candle(symbol, candle)
        if signal:
            self.detected_signals += 1
        print(
            f"ENGINE_CALLBACK=REACHED symbol={symbol} interval=1h closed={candle['is_closed']} "
            f"timestamp={candle['timestamp']} close={candle['close']} "
            f"golden_cross={bool(signal)}"
        )

    async def receive_until(self, ws: Any, timeout: float, *, wait_closed: bool = False) -> bool:
        deadline = time.monotonic() + timeout
        received_kline = False
        callback_start = self.callback_count
        while time.monotonic() < deadline:
            remaining = max(0.01, deadline - time.monotonic())
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            message = json.loads(raw)
            payload = message.get("data", message) if isinstance(message, dict) else {}
            event = payload.get("e") if isinstance(payload, dict) else None
            kline = payload.get("k", {}) if isinstance(payload, dict) else {}
            print(
                f"APP_MESSAGE=RECEIVED event={event} symbol={kline.get('s')} "
                f"interval={kline.get('i')} closed={kline.get('x')}"
            )
            await self.manager._handle_message(message)
            if event == "kline":
                received_kline = True
            if wait_closed and self.callback_count > callback_start:
                return True
            if not wait_closed and received_kline:
                return True
        return False


async def probe_single(manager: BinanceWebSocketManager, probe: LiveProbe, timeout: float, closed_timeout: float) -> bool:
    url = "wss://fstream.binance.com/ws/btcusdt@kline_1h"
    print(f"\nSINGLE_FUTURES_URL={url}")
    try:
        async with websockets.connect(url, open_timeout=10, close_timeout=3, ping_interval=20) as ws:
            print("SINGLE_WS_HANDSHAKE=PASS")
            if not await probe.receive_until(ws, timeout):
                print("SINGLE_KLINE=FAIL no application kline received")
                return False
            print("SINGLE_KLINE=PASS")
            if closed_timeout <= 0:
                print("SINGLE_CLOSED_CALLBACK=NOT_WAITED")
                return False
            closed = await probe.receive_until(ws, closed_timeout, wait_closed=True)
            print(f"SINGLE_CLOSED_CALLBACK={'PASS' if closed else 'TIMEOUT'} callbacks={probe.callback_count}")
            return closed
    except Exception as exc:
        print(f"SINGLE_TRANSPORT=FAIL {type(exc).__name__}: {exc}")
        return False


async def probe_combined_five(manager: BinanceWebSocketManager, probe: LiveProbe, timeout: float) -> bool:
    streams = "/".join(f"{s.lower()}@kline_1h" for s in ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"))
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    print(f"\nCOMBINED_FIVE_URL={url}")
    seen = set()
    deadline = time.monotonic() + timeout
    try:
        async with websockets.connect(url, open_timeout=10, close_timeout=3, ping_interval=20) as ws:
            print("COMBINED_FIVE_WS_HANDSHAKE=PASS")
            while time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.01, deadline - time.monotonic()))
                except asyncio.TimeoutError:
                    break
                message = json.loads(raw)
                payload = message.get("data", message) if isinstance(message, dict) else {}
                kline = payload.get("k", {}) if isinstance(payload, dict) else {}
                if payload.get("e") == "kline":
                    seen.add(kline.get("s", "").upper())
                    await manager._handle_message(message)
                if len(seen) == 5:
                    break
            print(f"COMBINED_FIVE_SYMBOLS_RECEIVED={','.join(sorted(seen)) or 'NONE'}")
            return bool(seen)
    except Exception as exc:
        print(f"COMBINED_FIVE_TRANSPORT=FAIL {type(exc).__name__}: {exc}")
        return False


async def probe_full_universe(symbols: List[str], timeout: float) -> bool:
    manager = BinanceWebSocketManager(message_timeout_seconds=90)
    manager.set_symbols(symbols)
    batches = manager.partition_symbols(manager.symbols, 100)
    assigned = [symbol for batch in batches for symbol in batch]
    print(
        f"\nFULL_UNIVERSE={len(manager.symbols)} workers={len(batches)} "
        f"worker_sizes={[len(batch) for batch in batches]} unique={len(assigned)==len(set(assigned))} "
        f"complete={set(assigned)==set(manager.symbols)}"
    )
    await manager.start()
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if len(manager._workers_with_data) == len(batches):
                break
            await asyncio.sleep(0.25)
        print(
            f"FULL_WORKERS_CONNECTED={manager._active_connections}/{len(batches)} "
            f"WORKERS_WITH_DATA={len(manager._workers_with_data)}/{len(batches)} "
            f"KLINES={manager.total_klines_received} CLOSED={manager.candles_closed_count} "
            f"RECONNECTS={manager.reconnect_count}"
        )
        return len(manager._workers_with_data) == len(batches)
    finally:
        await manager.stop()


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--first-message-timeout", type=float, default=12.0)
    parser.add_argument("--closed-candle-timeout", type=float, default=3900.0)
    parser.add_argument("--combined-timeout", type=float, default=12.0)
    parser.add_argument("--full-timeout", type=float, default=20.0)
    args = parser.parse_args()

    network_diagnostics()
    client = BinanceFuturesClient()
    try:
        symbols = await client.get_active_usdt_symbols()
        if len(symbols) != 527:
            print(f"FUTURES_USDT_PERPETUALS={len(symbols)} (does not match the reported 527; stop before full test)")
            return 2
    finally:
        await client.close()

    symbols_to_seed = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    temp_dir, database, rest_client, engine = await prepare_engine(symbols_to_seed)
    manager = BinanceWebSocketManager(on_candle_closed=None, message_timeout_seconds=90)
    manager.set_symbols(symbols_to_seed)
    probe = LiveProbe(manager, engine)
    manager.on_candle_closed = probe.on_candle_closed
    try:
        single_ok = await probe_single(
            manager, probe, args.first_message_timeout, args.closed_candle_timeout
        )
        if not single_ok:
            print("SKIP_COMBINED_AND_527: single stream did not prove a closed candle reached SignalEngine")
            return 1
        combined_ok = await probe_combined_five(manager, probe, args.combined_timeout)
        if not combined_ok:
            print("SKIP_527: combined 5-symbol stream did not deliver a kline")
            return 1
        universe_client = BinanceFuturesClient()
        try:
            full_symbols = await universe_client.get_active_usdt_symbols()
        finally:
            await universe_client.close()
        if len(full_symbols) != 527:
            print(f"FULL_UNIVERSE_SKIPPED: REST returned {len(full_symbols)} perpetuals, expected 527")
            return 1
        full_ok = await probe_full_universe(full_symbols, args.full_timeout)
        return 0 if full_ok else 1
    finally:
        await rest_client.close()
        temp_dir.cleanup()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
