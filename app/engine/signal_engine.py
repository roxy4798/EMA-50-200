"""NEXORA Signal Engine with duplicate prevention, gap recovery, and dynamic symbol discovery."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set

from app.engine.alert_queue import AlertQueue
from app.exchange.binance_client import BinanceFuturesClient
from app.indicators.ema import (
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)
from app.persistence.database import Database
from app.persistence.models import SignalRecord

logger = logging.getLogger("nexora.engine.signal")


class SignalEngine:
    def __init__(
        self,
        binance_client: BinanceFuturesClient,
        database: Database,
        alert_queue: AlertQueue,
        timeframe: str = "1h",
        fast_period: int = 50,
        slow_period: int = 200,
        candle_limit: int = 250,
    ) -> None:
        self.binance_client = binance_client
        self.database = database
        self.alert_queue = alert_queue
        self.timeframe = timeframe.lower()
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.candle_limit = candle_limit

        self.symbols: List[str] = []
        self.candles_history: Dict[str, List[Dict[str, Any]]] = {}
        self.initialized_symbols: Set[str] = set()
        self.total_signals_detected = 0
        self.live_signals_count = 0
        self.historical_crosses_count = 0
        self.last_signal: Optional[Dict[str, Any]] = None
        self.last_closed_candle_time: Optional[int] = None
        self._lock = asyncio.Lock()
        self.start_time = time.time()

    def set_symbols(self, symbols: List[str]) -> None:
        self.symbols = [s.upper() for s in symbols]

    async def initialize_symbols(
        self,
        max_concurrency: int = 5,
        pacing_delay_ms: float = 160.0,
    ) -> None:
        """Fetch historical candles for all monitored symbols with safe retry/backoff and rate-limit pacing."""
        logger.info(
            f"Initializing {len(self.symbols)} symbols with historical 1H candles "
            f"(limit={self.candle_limit}, pacing={pacing_delay_ms}ms)..."
        )
        semaphore = asyncio.Semaphore(max_concurrency)
        pacer_lock = asyncio.Lock()
        last_request_time = 0.0

        async def paced_get_klines(sym: str, limit: int, **kwargs) -> List[Dict[str, Any]]:
            nonlocal last_request_time
            async with pacer_lock:
                now = time.monotonic()
                elapsed_ms = (now - last_request_time) * 1000.0
                if elapsed_ms < pacing_delay_ms:
                    await asyncio.sleep((pacing_delay_ms - elapsed_ms) / 1000.0)
                last_request_time = time.monotonic()
                return await self.binance_client.get_klines(
                    sym, interval=self.timeframe, limit=limit, only_closed=True, **kwargs
                )

        async def init_single(raw_sym: str) -> None:
            sym = self.binance_client.resolve_symbol(raw_sym)
            async with semaphore:
                for attempt in range(1, 4):
                    try:
                        # 1. Check for gap recovery if candles already exist in local DB
                        last_cached_ts = await self.database.get_last_cached_candle_timestamp(sym, self.timeframe)
                        candles: List[Dict[str, Any]] = []

                        if last_cached_ts:
                            cached = await self.database.get_cached_candles(sym, self.timeframe, limit=self.candle_limit)
                            now_ms = int(time.time() * 1000)
                            if now_ms - last_cached_ts > 3600_000 * 2:
                                missing = await paced_get_klines(
                                    sym,
                                    limit=100,
                                    start_time=last_cached_ts + 3600_000,
                                )
                                seen_ts = {c["timestamp"] for c in cached}
                                combined = list(cached)
                                for m in missing:
                                    if m["timestamp"] not in seen_ts:
                                        combined.append(m)
                                        seen_ts.add(m["timestamp"])
                                candles = combined
                            else:
                                candles = cached

                        if not candles or len(candles) < self.slow_period:
                            candles = await paced_get_klines(
                                sym, limit=self.candle_limit
                            )

                        if candles and len(candles) >= self.fast_period:
                            df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                            enriched_candles = df.to_dict(orient="records")
                            async with self._lock:
                                self.candles_history[sym] = enriched_candles
                                self.initialized_symbols.add(sym)
                                if raw_sym != sym:
                                    self.initialized_symbols.add(raw_sym)
                                if enriched_candles:
                                    self.last_closed_candle_time = max(
                                        self.last_closed_candle_time or 0,
                                        int(enriched_candles[-1]["timestamp"]),
                                    )
                            # Cache into DB
                            await self.database.cache_candles(enriched_candles, sym, self.timeframe)
                            break
                        else:
                            if attempt < 3:
                                await asyncio.sleep(attempt * 1.5)
                            else:
                                logger.warning(f"No sufficient candles returned for {sym} (got {len(candles) if candles else 0})")
                    except Exception as e:
                        if attempt < 3:
                            await asyncio.sleep(attempt * 1.5)
                        else:
                            logger.error(f"Failed to initialize {sym} after {attempt} attempts: {e}")

        tasks = [init_single(s) for s in self.symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"Initialization complete: {len(self.initialized_symbols)}/{len(self.symbols)} ready.")

    async def handle_closed_candle(self, symbol: str, candle: Dict[str, Any]) -> Optional[GoldenCrossSignal]:
        """Called when a 1H candle closes for a symbol."""
        symbol = symbol.upper()
        self.last_closed_candle_time = max(self.last_closed_candle_time or 0, int(candle["timestamp"]))

        async with self._lock:
            history = self.candles_history.get(symbol, [])
            if history and history[-1]["timestamp"] == candle["timestamp"]:
                history[-1] = candle
            else:
                history.append(candle)

            if len(history) > 300:
                history = history[-300:]

            # Enrich with EMA50 and EMA200
            df = enrich_candles_with_ema(history, self.fast_period, self.slow_period)
            self.candles_history[symbol] = df.to_dict(orient="records")

            # Detect Golden Cross on the last closed candle
            signal = detect_golden_cross(
                df,
                symbol=symbol,
                timeframe=self.timeframe,
                fast_col=f"ema_{self.fast_period}",
                slow_col=f"ema_{self.slow_period}",
            )

        if signal:
            # Duplicate prevention check (Req 11)
            already_exists = await self.database.has_signal(
                signal.symbol, signal.timeframe, signal.candle_timestamp
            )
            if already_exists:
                logger.info(f"DUPLICATE_ALERT_SUPPRESSED: Golden Cross for {symbol} at {signal.candle_timestamp} already alerted.")
                return None

            logger.info(f"🔥 LIVE GOLDEN CROSS CONFIRMED for {symbol} at {signal.signal_time_utc} (Price: {signal.close_price})")
            self.total_signals_detected += 1
            self.live_signals_count += 1
            self.last_signal = {
                "symbol": signal.symbol,
                "timeframe": signal.timeframe,
                "candle_timestamp": signal.candle_timestamp,
                "signal_time_utc": signal.signal_time_utc,
                "close_price": signal.close_price,
                "ema50": signal.ema50,
                "ema200": signal.ema200,
                "is_live": True,
            }

            # 1. Persist to database as LIVE signal
            record = SignalRecord(
                id=None,
                symbol=signal.symbol,
                timeframe=signal.timeframe,
                candle_timestamp=signal.candle_timestamp,
                signal_time_utc=signal.signal_time_utc,
                ema50=signal.ema50,
                ema200=signal.ema200,
                close_price=signal.close_price,
                is_live=True,
            )
            signal_id = await self.database.save_signal(record)

            # 2. Enqueue into alert queue (ONLY LIVE SIGNALS TRIGGER TELEGRAM)
            await self.alert_queue.enqueue(signal_id, signal)

        return signal

    async def scan_and_record_historical_crosses(self, symbol: str) -> List[GoldenCrossSignal]:
        """Scans loaded history for existing Golden Crosses and records them in DB idempotently without triggering alerts."""
        symbol = symbol.upper()
        async with self._lock:
            history = list(self.candles_history.get(symbol, []))
        if not history:
            return []

        df = enrich_candles_with_ema(history, self.fast_period, self.slow_period)
        crosses = find_all_golden_crosses(df, symbol=symbol, timeframe=self.timeframe)

        if crosses:
            records = [
                SignalRecord(
                    id=None,
                    symbol=c.symbol,
                    timeframe=c.timeframe,
                    candle_timestamp=c.candle_timestamp,
                    signal_time_utc=c.signal_time_utc,
                    ema50=c.ema50,
                    ema200=c.ema200,
                    close_price=c.close_price,
                    is_live=False,
                )
                for c in crosses
            ]
            await self.database.save_historical_signals_batch(records)

            async with self._lock:
                self.historical_crosses_count += len(crosses)
                self.total_signals_detected = max(self.total_signals_detected, len(crosses))
                if not self.last_signal:
                    self.last_signal = {
                        "symbol": crosses[-1].symbol,
                        "timeframe": crosses[-1].timeframe,
                        "candle_timestamp": crosses[-1].candle_timestamp,
                        "signal_time_utc": crosses[-1].signal_time_utc,
                        "close_price": crosses[-1].close_price,
                        "ema50": crosses[-1].ema50,
                        "ema200": crosses[-1].ema200,
                        "is_live": False,
                    }

        return crosses

    async def scan_historical_symbols(
        self,
        symbols: List[str],
        max_concurrency: int = 5,
        total_symbols_count: Optional[int] = None,
        log: Optional[logging.Logger] = None,
    ) -> Dict[str, List[GoldenCrossSignal]]:
        """Scans loaded history for multiple symbols with bounded concurrency."""
        active_logger = log or logger
        total_targets = len(symbols)
        universe_total = total_symbols_count if total_symbols_count is not None else total_targets
        active_logger.info(
            f"Historical scan started: {total_targets}/{universe_total} symbols, concurrency={max_concurrency}"
        )
        semaphore = asyncio.Semaphore(max_concurrency)
        completed = 0
        total_crosses = 0
        progress_lock = asyncio.Lock()
        start_time = time.monotonic()
        results: Dict[str, List[GoldenCrossSignal]] = {}

        async def scan_one(sym: str) -> None:
            nonlocal completed, total_crosses
            async with semaphore:
                try:
                    crosses = await self.scan_and_record_historical_crosses(sym)
                    n_crosses = len(crosses)
                    results[sym] = crosses
                except Exception as e:
                    active_logger.error(f"Historical scan failed for {sym}: {e}")
                    n_crosses = 0
                    results[sym] = []

                async with progress_lock:
                    completed += 1
                    total_crosses += n_crosses
                    if completed % 5 == 0 or completed == total_targets:
                        active_logger.info(f"Historical scan progress: {completed}/{total_targets}")

        await asyncio.gather(*(scan_one(s) for s in symbols), return_exceptions=False)
        duration = time.monotonic() - start_time
        active_logger.info(
            f"Historical scan complete: {completed}/{total_targets} symbols, "
            f"{total_crosses} historical Golden Crosses found, duration={duration:.1f}s"
        )
        return results

    async def refresh_symbol_universe(self) -> List[str]:
        """Discovers new symbols from Binance without destroying existing valid EMA state (Req 23)."""
        try:
            active_symbols = await self.binance_client.get_active_usdt_symbols()
            if not active_symbols:
                return self.symbols

            new_symbols = [s for s in active_symbols if s not in self.symbols]
            if new_symbols:
                logger.info(f"Discovered {len(new_symbols)} new USDT perpetual symbols from Binance.")
                self.symbols.extend(new_symbols)
                # Warm up only new symbols
                semaphore = asyncio.Semaphore(5)

                async def warm_new(sym: str):
                    async with semaphore:
                        try:
                            candles = await self.binance_client.get_klines(sym, interval=self.timeframe, limit=self.candle_limit, only_closed=True)
                            if candles:
                                df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                                async with self._lock:
                                    self.candles_history[sym] = df.to_dict(orient="records")
                                    self.initialized_symbols.add(sym)
                        except Exception as e:
                            logger.error(f"Error warming new symbol {sym}: {e}")

                await asyncio.gather(*[warm_new(s) for s in new_symbols], return_exceptions=True)

            return self.symbols
        except Exception as e:
            logger.error(f"Error during symbol universe refresh: {e}")
            return self.symbols
