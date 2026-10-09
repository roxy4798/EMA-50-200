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
        message_timeout_seconds: float = 90.0,
        stale_threshold_seconds: float = 120.0,
    ) -> None:
        self.base_ws_url = self._normalize_base_ws_url(base_ws_url)
        if urlparse(self.base_ws_url).hostname != "fstream.binance.com":
            raise ValueError("Live market data must use Binance USD-M Futures (fstream.binance.com)")
        if timeframe.lower() != "1h":
            raise ValueError("NEXORA live scanner supports the 1h timeframe only")
        self.timeframe = timeframe
        self.on_candle_closed = on_candle_closed
        self.message_timeout_seconds = message_timeout_seconds
        self.stale_threshold_seconds = stale_threshold_seconds
        self.symbols: List[str] = []
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self.candles_closed_count = 0
        self.total_klines_received = 0
        self.total_messages_received = 0
        self.reconnect_count = 0
        self.last_message_received_at: Optional[float] = None
        self.last_kline_received_at: Optional[float] = None
        self.last_processed_at: Optional[float] = None
        self.last_candle_time: Optional[int] = None
        self.last_closed_candle_received_at: Optional[float] = None
        self.last_symbol_closed: Optional[str] = None
        self.subscription_errors_count = 0
        self.last_subscription_error: Optional[str] = None
        self._active_connections = 0
        self._workers_with_data: set[int] = set()
        self._workers_with_kline: set[int] = set()

    @property
    def is_connected(self) -> bool:
        return self._active_connections > 0

    @property
    def ws_connected(self) -> bool:
        return self.is_connected

    def get_market_data_health(self) -> Dict[str, Any]:
        """Provides fine-grained health metrics separating connection from data reception."""
        now = time.time()
        is_conn = self.is_connected
        last_rcv = self.last_kline_received_at
        last_proc = self.last_processed_at

        # Check if any worker tasks terminated unexpectedly
        dead_workers = [
            i for i, t in enumerate(self._tasks)
            if t.done() and not t.cancelled()
        ]

        if not is_conn:
            status = "DISCONNECTED"
            is_healthy = False
        elif dead_workers:
            status = f"DEGRADED (worker {dead_workers[0]} terminated)"
            is_healthy = False
        elif self.subscription_errors_count > 0 and self.total_klines_received == 0:
            status = f"DEGRADED (subscription error: {self.last_subscription_error})"
            is_healthy = False
        elif last_rcv is None:
            status = "DATA STALE / NO MARKET DATA"
            is_healthy = False
        else:
            elapsed = now - last_rcv
            if elapsed > self.stale_threshold_seconds:
                status = f"DATA STALE / NO MARKET DATA ({int(elapsed)}s silent)"
                is_healthy = False
            else:
                status = "HEALTHY"
                is_healthy = True

        return {
            "status": status,
            "is_healthy": is_healthy,
            "is_connected": is_conn,
            "ws_connected": is_conn,
            "active_connections": self._active_connections,
            "total_workers": len(self._tasks),
            "dead_workers_count": len(dead_workers),
            "total_streams": len(self.symbols),
            "total_messages_received": self.total_messages_received,
            "total_klines_received": self.total_klines_received,
            "candles_closed_count": self.candles_closed_count,
            "last_message_received_at": self.last_message_received_at,
            "last_kline_received_at": last_rcv,
            "last_processed_at": last_proc,
            "last_closed_candle_time": self.last_candle_time,
            "last_closed_candle_received_at": self.last_closed_candle_received_at,
            "last_symbol_closed": self.last_symbol_closed,
            "reconnect_count": self.reconnect_count,
            "stale_threshold_seconds": self.stale_threshold_seconds,
            "subscription_errors_count": self.subscription_errors_count,
            "last_subscription_error": self.last_subscription_error,
        }

    def set_symbols(self, symbols: List[str]) -> None:
        from app.exchange.binance_client import BinanceFuturesClient
        client = BinanceFuturesClient()
        self.symbols = list(dict.fromkeys(client.resolve_symbol(s).lower() for s in symbols))

    @staticmethod
    def partition_symbols(symbols: List[str], batch_size: int = 100) -> List[List[str]]:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        return [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]

    async def start(self) -> None:
        if self._running:
            logger.warning("WebSocketManager already running; ignoring duplicate start call.")
            return
        self._running = True
        # Chunk symbols into groups of max 100 streams per connection
        batch_size = 100
        if not self.symbols:
            logger.warning("No symbols provided to WebSocketManager.")
            return

        batches = self.partition_symbols(self.symbols, batch_size)
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

    @staticmethod
    def _normalize_base_ws_url(url: str) -> str:
        cleaned = url.strip().rstrip("/")
        parsed = urlparse(cleaned)
        if parsed.hostname != "fstream.binance.com":
            raise ValueError("Live market data must use Binance USD-M Futures (fstream.binance.com)")
        scheme = parsed.scheme or "wss"
        netloc = parsed.netloc or "fstream.binance.com"
        path = parsed.path.rstrip("/")
        while path.endswith(("/ws", "/stream", "/market")):
            if path.endswith("/ws"):
                path = path[:-3].rstrip("/")
            elif path.endswith("/stream"):
                path = path[:-7].rstrip("/")
            elif path.endswith("/market"):
                path = path[:-7].rstrip("/")
        return f"{scheme}://{netloc}/market"

    def get_raw_stream_url(self, symbol: str) -> str:
        clean_symbol = symbol.strip().lower()
        return f"{self.base_ws_url}/ws/{clean_symbol}@kline_{self.timeframe}"

    def get_stream_url(self, symbols: List[str]) -> str:
        stream_names = [f"{s.strip().lower()}@kline_{self.timeframe}" for s in symbols]
        stream_param = "/".join(stream_names)
        return f"{self.base_ws_url}/stream?streams={stream_param}"

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
                    self._workers_with_data.discard(batch_idx)
                    self._workers_with_kline.discard(batch_idx)
                    logger.info(f"WS CONNECTED [worker {batch_idx}] streams={len(batch_symbols)}")

                    while self._running:
                        raw_msg = await asyncio.wait_for(
                            ws.recv(), timeout=self.message_timeout_seconds
                        )
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            if batch_idx not in self._workers_with_data:
                                self._workers_with_data.add(batch_idx)
                                logger.info("WS DATA RECEIVED [worker %s]", batch_idx)
                            await self._handle_message(msg, batch_idx=batch_idx)
                        except Exception as e:
                            logger.error(f"[WS Worker {batch_idx}] Message parsing error: {e}")

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                self.reconnect_count += 1
                logger.warning(
                    "WS DATA TIMEOUT [worker %s]: no application message for %.0fs; "
                    "reconnecting (count=%s)",
                    batch_idx, self.message_timeout_seconds, self.reconnect_count,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
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

    async def _handle_message(self, data: Dict, batch_idx: Optional[int] = None) -> None:
        if not isinstance(data, dict):
            logger.warning("Ignoring non-object WebSocket application message")
            return
        self.total_messages_received += 1
        self.last_message_received_at = time.time()
        payload = data.get("data", data)
        if not isinstance(payload, dict):
            logger.warning("Ignoring malformed WebSocket payload")
            return

        # Check for subscription errors or acknowledgements
        if "error" in data or "error" in payload:
            err = data.get("error") or payload.get("error")
            self.subscription_errors_count += 1
            self.last_subscription_error = str(err)
            logger.error("WS_SUBSCRIPTION_ERROR: Received subscription error from Binance: %s", err)
            self.last_processed_at = time.time()
            return

        if "result" in data and "id" in data:
            logger.debug("WS_SUBSCRIPTION_ACK: id=%s result=%s", data.get("id"), data.get("result"))
            self.last_processed_at = time.time()
            return

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

            now = time.time()
            # If feed was previously stale, log recovery
            if self.last_kline_received_at and (now - self.last_kline_received_at > self.stale_threshold_seconds):
                logger.info("WS_FEED_RECOVERED: Market data feed resumed after stale period (symbol=%s)", symbol)

            self.total_klines_received += 1
            self.last_kline_received_at = now
            if batch_idx is not None and batch_idx not in self._workers_with_kline:
                self._workers_with_kline.add(batch_idx)
                logger.info("KLINE RECEIVED [worker %s] symbol=%s interval=%s", batch_idx, symbol, interval)
            if is_closed and symbol:
                self.candles_closed_count += 1
                self.last_candle_time = timestamp
                self.last_closed_candle_received_at = now
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
            self.last_processed_at = time.time()
