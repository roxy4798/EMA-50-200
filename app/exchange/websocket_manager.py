"""Binance USD-M Futures WebSocket manager with multi-stream batching and resilient reconnection."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from urllib.parse import urlparse
from typing import Any, Callable, Coroutine, Dict, List, Optional
import websockets

logger = logging.getLogger("nexora.websocket")


class BinanceWebSocketManager:
    def __init__(
        self,
        base_ws_url: str = "wss://fstream.binance.com",
        timeframe: str = "1h",
        on_candle_closed: Optional[Callable[[str, Dict], Coroutine]] = None,
    ) -> None:
        self.base_ws_url = base_ws_url.rstrip("/").removesuffix("/ws")
        if urlparse(self.base_ws_url).hostname != "fstream.binance.com":
            raise ValueError("Live market data must use Binance USD-M Futures (fstream.binance.com)")
        if timeframe.lower() != "1h":
            raise ValueError("NEXORA live scanner supports the 1h timeframe only")
        self.timeframe = timeframe
        self.on_candle_closed = on_candle_closed
        self.symbols: List[str] = []
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self.candles_closed_count = 0
        self.total_klines_received = 0
        self.reconnect_count = 0
        self.last_kline_received_at: Optional[float] = None
        self.last_candle_time: Optional[int] = None
        self.last_closed_candle_received_at: Optional[float] = None
        self.last_symbol_closed: Optional[str] = None
        self._active_connections = 0

    @property
    def is_connected(self) -> bool:
        return self._active_connections > 0

    def get_market_data_health(self) -> Dict[str, Any]:
        """Provides fine-grained health metrics separating connection from data reception (Section 8)."""
        now = time.time()
        is_conn = self.is_connected
        last_rcv = self.last_kline_received_at

        if not is_conn:
            status = "DISCONNECTED"
            is_healthy = False
        elif last_rcv is None:
            status = "DATA STALE / NO MARKET DATA"
            is_healthy = False
        else:
            elapsed = now - last_rcv
            if elapsed > 120.0:
                status = f"DATA STALE / NO MARKET DATA ({int(elapsed)}s silent)"
                is_healthy = False
            else:
                status = "HEALTHY"
                is_healthy = True

        return {
            "status": status,
            "is_healthy": is_healthy,
            "is_connected": is_conn,
            "active_connections": self._active_connections,
            "total_klines_received": self.total_klines_received,
            "candles_closed_count": self.candles_closed_count,
            "last_kline_received_at": last_rcv,
            "last_closed_candle_time": self.last_candle_time,
            "last_closed_candle_received_at": self.last_closed_candle_received_at,
            "reconnect_count": self.reconnect_count,
        }

    def set_symbols(self, symbols: List[str]) -> None:
        from app.exchange.binance_client import BinanceFuturesClient
        client = BinanceFuturesClient()
        self.symbols = [client.resolve_symbol(s).lower() for s in symbols]

    async def start(self) -> None:
        self._running = True
        # Chunk symbols into groups of max 100 streams per connection
        batch_size = 100
        if not self.symbols:
            logger.warning("No symbols provided to WebSocketManager.")
            return

        batches = [self.symbols[i : i + batch_size] for i in range(0, len(self.symbols), batch_size)]
        logger.info(f"Starting {len(batches)} WebSocket connection(s) for {len(self.symbols)} symbols...")

        for idx, batch in enumerate(batches):
            task = asyncio.create_task(self._run_batch_loop(idx, batch))
            self._tasks.append(task)

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._active_connections = 0

    def get_stream_url(self, symbols: List[str]) -> str:
        base_url = self.base_ws_url.rstrip("/").removesuffix("/ws")
        stream_names = [f"{s}@kline_{self.timeframe}" for s in symbols]
        stream_param = "/".join(stream_names)
        return f"{base_url}/stream?streams={stream_param}"

    async def _run_batch_loop(self, batch_idx: int, batch_symbols: List[str]) -> None:
        backoff = 2
        ws_url = self.get_stream_url(batch_symbols)

        while self._running:
            try:
                logger.info(f"[WS Worker {batch_idx}] Connecting ({len(batch_symbols)} streams)...")
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=15,
                    close_timeout=5,
                ) as ws:
                    self._active_connections += 1
                    backoff = 2
                    logger.info(f"[WS Worker {batch_idx}] Connected successfully.")

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            await self._handle_message(msg)
                        except Exception as e:
                            logger.error(f"[WS Worker {batch_idx}] Message parsing error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.reconnect_count += 1
                logger.warning(
                    f"[WS Worker {batch_idx}] Disconnected ({e}). Reconnecting in {backoff}s... (Total reconnects: {self.reconnect_count})"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            finally:
                if self._active_connections > 0:
                    self._active_connections -= 1

    async def _handle_message(self, data: Dict) -> None:
        payload = data.get("data", data)
        event_type = payload.get("e")

        if event_type == "kline":
            k = payload.get("k", {})
            is_closed = k.get("x", False)
            symbol = k.get("s", "").upper()
            interval = k.get("i")

            # Strictly require closed candle (k.x == True)
            if interval != self.timeframe:
                logger.warning("Ignoring non-%s Futures kline for %s", self.timeframe, symbol or "UNKNOWN")
                return
            if symbol.lower() not in self.symbols:
                logger.warning("Ignoring kline for unsubscribed Futures symbol %s", symbol or "UNKNOWN")
                return
            try:
                timestamp = int(k["t"])
                values = [float(k[key]) for key in ("o", "h", "l", "c", "v")]
                if timestamp <= 0 or any(v <= 0 for v in values[:4]):
                    raise ValueError("invalid candle fields")
            except (KeyError, TypeError, ValueError):
                logger.error("Ignoring malformed Futures kline for %s", symbol)
                return
            self.total_klines_received += 1
            self.last_kline_received_at = time.time()
            if is_closed and symbol:
                self.candles_closed_count += 1
                self.last_candle_time = timestamp
                self.last_closed_candle_received_at = time.time()
                self.last_symbol_closed = symbol

                candle_data = {
                    "timestamp": timestamp,
                    "open": values[0],
                    "high": values[1],
                    "low": values[2],
                    "close": values[3],
                    "volume": values[4],
                    "close_time": int(k.get("T", 0)),
                    "is_closed": True,
                }

                logger.info(f"1H CLOSED candle for {symbol} at {candle_data['close']}")
                if self.on_candle_closed:
                    try:
                        await self.on_candle_closed(symbol, candle_data)
                    except Exception as e:
                        logger.error(f"Error executing candle closed callback for {symbol}: {e}")
