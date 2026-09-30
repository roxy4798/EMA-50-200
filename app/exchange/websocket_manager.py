"""Binance USD-M Futures WebSocket manager with multi-stream batching and resilient reconnection."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, Coroutine, Dict, List, Optional
import websockets

logger = logging.getLogger("nexora.websocket")


class BinanceWebSocketManager:
    def __init__(
        self,
        base_ws_url: str = "wss://fstream.binance.com",
        timeframe: str = "1h",
        on_candle_closed: Optional[Callable[[str, Dict], Coroutine]] = None,
    ) -> None:
        self.base_ws_url = base_ws_url.rstrip("/")
        self.timeframe = timeframe
        self.on_candle_closed = on_candle_closed
        self.symbols: List[str] = []
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self.candles_closed_count = 0
        self.reconnect_count = 0
        self.last_candle_time: Optional[int] = None
        self.last_symbol_closed: Optional[str] = None
        self._active_connections = 0

    @property
    def is_connected(self) -> bool:
        return self._active_connections > 0

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

    async def _run_batch_loop(self, batch_idx: int, batch_symbols: List[str]) -> None:
        backoff = 2
        stream_names = [f"{s}@kline_{self.timeframe}" for s in batch_symbols]
        stream_param = "/".join(stream_names)
        ws_url = f"{self.base_ws_url}/stream?streams={stream_param}"

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

            # Strictly require closed candle (k.x == True)
            if is_closed and symbol:
                self.candles_closed_count += 1
                self.last_candle_time = int(k.get("t", 0))
                self.last_symbol_closed = symbol

                candle_data = {
                    "timestamp": int(k.get("t", 0)),
                    "open": float(k.get("o", 0.0)),
                    "high": float(k.get("h", 0.0)),
                    "low": float(k.get("l", 0.0)),
                    "close": float(k.get("c", 0.0)),
                    "volume": float(k.get("v", 0.0)),
                    "close_time": int(k.get("T", 0)),
                    "is_closed": True,
                }

                logger.info(f"1H CLOSED candle for {symbol} at {candle_data['close']}")
                if self.on_candle_closed:
                    try:
                        await self.on_candle_closed(symbol, candle_data)
                    except Exception as e:
                        logger.error(f"Error executing candle closed callback for {symbol}: {e}")
