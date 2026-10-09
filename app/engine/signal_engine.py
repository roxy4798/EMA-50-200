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


# Canonical initialization lifecycle states
STATE_READY = "READY"
STATE_WAITING_FOR_HISTORY = "WAITING_FOR_HISTORY"
STATE_RETRY_PENDING = "RETRY_PENDING"
STATE_FAILED = "FAILED"


class SignalEngine:
    def __init__(
        self,
        binance_client: BinanceFuturesClient,
        database: Database,
        alert_queue: AlertQueue,
        timeframe: str = "1h",
        fast_period: int = 50,
        slow_period: int = 200,
        candle_limit: int = 1000,
    ) -> None:
        self.binance_client = binance_client
        self.database = database
        self.alert_queue = alert_queue
        self.timeframe = timeframe.lower()
        if self.timeframe != "1h":
            raise ValueError("NEXORA SignalEngine supports the 1h timeframe only")
        if fast_period <= 0 or slow_period <= 0 or fast_period >= slow_period:
            raise ValueError(
                f"Invalid EMA configuration: fast_period={fast_period}, slow_period={slow_period}. "
                "fast_period must be positive and strictly less than slow_period."
            )
        if candle_limit < 1000:
            raise ValueError(
                f"NEXORA canonical frame requires at least 1000 closed 1H candles for convergence (got {candle_limit})"
            )
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.candle_limit = candle_limit

        self.symbols: List[str] = []
        self.candles_history: Dict[str, List[Dict[str, Any]]] = {}
        self.initialized_symbols: Set[str] = set()
        self.symbol_states: Dict[str, str] = {}
        self.symbol_details: Dict[str, Dict[str, Any]] = {}
        self.total_signals_detected = 0
        self.live_signals_count = 0
        self.historical_crosses_count = 0
        self.last_signal: Optional[Dict[str, Any]] = None
        self.last_closed_candle_time: Optional[int] = None
        self._lock = asyncio.Lock()
        self.start_time = time.time()

    @staticmethod
    def validate_candle_continuity(candles: List[Dict[str, Any]], interval_ms: int = 3600_000) -> bool:
        """Validates that consecutive closed candles have exact timestamp progression without gaps."""
        if not candles or len(candles) < 2:
            return True
        for i in range(1, len(candles)):
            prev_ts = int(candles[i - 1]["timestamp"])
            curr_ts = int(candles[i]["timestamp"])
            if curr_ts - prev_ts != interval_ms:
                return False
        return True

    def get_symbol_state(self, symbol: str) -> str:
        """Returns the canonical initialization lifecycle state of a symbol."""
        s = symbol.upper()
        resolved = self.binance_client.resolve_symbol(s) if hasattr(self.binance_client, "resolve_symbol") else s
        if resolved in self.symbol_states:
            return self.symbol_states[resolved]
        if s in self.symbol_states:
            return self.symbol_states[s]
        if resolved in self.initialized_symbols or s in self.initialized_symbols:
            return STATE_READY
        return STATE_RETRY_PENDING

    def get_state_count(self, state: str) -> int:
        """Returns the count of monitored symbols currently in the given state."""
        return sum(1 for s in self.symbols if self.get_symbol_state(s) == state)

    def get_initialization_summary(self) -> Dict[str, int]:
        """Provides an aggregate summary of symbol initialization lifecycle states."""
        return {
            "ready": len(self.initialized_symbols),
            "waiting_for_history": self.get_state_count(STATE_WAITING_FOR_HISTORY),
            "retry_pending": self.get_state_count(STATE_RETRY_PENDING),
            "failed": self.get_state_count(STATE_FAILED),
            "total": len(self.symbols),
        }

    def set_symbols(self, symbols: List[str]) -> None:
        self.symbols = [s.upper() for s in symbols]
        for s in self.symbols:
            resolved = self.binance_client.resolve_symbol(s) if hasattr(self.binance_client, "resolve_symbol") else s
            if s not in self.symbol_states:
                self.symbol_states[s] = STATE_RETRY_PENDING
            if resolved not in self.symbol_states:
                self.symbol_states[resolved] = STATE_RETRY_PENDING

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
            sym = self.binance_client.resolve_symbol(raw_sym) if hasattr(self.binance_client, "resolve_symbol") else raw_sym
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
                                seen_ts = {int(c["timestamp"]) for c in cached}
                                combined = list(cached)
                                for m in missing:
                                    if int(m["timestamp"]) not in seen_ts:
                                        combined.append(m)
                                        seen_ts.add(int(m["timestamp"]))
                                candles = combined
                            else:
                                candles = cached

                        fresh = await paced_get_klines(sym, limit=self.candle_limit)
                        if fresh:
                            by_timestamp = {int(c["timestamp"]): c for c in candles}
                            by_timestamp.update({int(c["timestamp"]): c for c in fresh})
                            candles = sorted(by_timestamp.values(), key=lambda c: int(c["timestamp"]))[-self.candle_limit:]

                        is_continuous = self.validate_candle_continuity(candles)

                        if candles and len(candles) >= self.candle_limit and is_continuous:
                            df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                            enriched_candles = df.to_dict(orient="records")
                            async with self._lock:
                                self.candles_history[sym] = enriched_candles
                                self.initialized_symbols.add(sym)
                                if raw_sym != sym:
                                    self.initialized_symbols.add(raw_sym)
                                self.symbol_states[sym] = STATE_READY
                                if raw_sym != sym:
                                    self.symbol_states[raw_sym] = STATE_READY
                                self.symbol_details[sym] = {
                                    "state": STATE_READY,
                                    "candles": len(candles),
                                    "reason": "Sufficient history loaded",
                                }
                                if enriched_candles:
                                    self.last_closed_candle_time = max(
                                        self.last_closed_candle_time or 0,
                                        int(enriched_candles[-1]["timestamp"]),
                                    )
                            # Cache into DB
                            await self.database.cache_candles(
                                enriched_candles, sym, self.timeframe,
                                fast_period=self.fast_period, slow_period=self.slow_period
                            )
                            logger.info(
                                "[INIT_STATE] symbol=%s timeframe=%s state=READY candles=%d/%d EMA=(%d/%d) status='Initialized successfully'",
                                sym, self.timeframe, len(candles), self.candle_limit, self.fast_period, self.slow_period,
                            )
                            break
                        else:
                            # Fewer than candle_limit OR discontinuous
                            is_exhausted = False
                            if hasattr(self.binance_client, "is_history_exhausted"):
                                is_exhausted = bool(self.binance_client.is_history_exhausted(sym))
                            elif is_continuous and candles and len(candles) < self.candle_limit:
                                pause_rem = getattr(self.binance_client, "pause_remaining", 0.0)
                                is_exhausted = (pause_rem <= 0.0)

                            if not is_continuous and candles:
                                new_state = STATE_RETRY_PENDING
                                reason = "Discontinuous candle history (gap detected in timestamp sequence)"
                            elif is_exhausted:
                                new_state = STATE_WAITING_FOR_HISTORY
                                reason = f"Limited exchange history ({len(candles) if candles else 0}/{self.candle_limit} closed 1H candles available)"
                            else:
                                new_state = STATE_RETRY_PENDING
                                reason = f"Temporary REST failure, partial retrieval, or rate limit ({len(candles) if candles else 0}/{self.candle_limit} candles)"

                            # Save available valid candles so live WebSocket can accumulate from them
                            if candles and is_continuous:
                                df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                                enriched_candles = df.to_dict(orient="records")
                                async with self._lock:
                                    self.candles_history[sym] = enriched_candles
                                await self.database.cache_candles(
                                    enriched_candles, sym, self.timeframe,
                                    fast_period=self.fast_period, slow_period=self.slow_period
                                )

                            async with self._lock:
                                self.symbol_states[sym] = new_state
                                if raw_sym != sym:
                                    self.symbol_states[raw_sym] = new_state
                                self.symbol_details[sym] = {
                                    "state": new_state,
                                    "candles": len(candles) if candles else 0,
                                    "reason": reason,
                                }
                                self.initialized_symbols.discard(sym)
                                self.initialized_symbols.discard(raw_sym)

                            if new_state == STATE_WAITING_FOR_HISTORY:
                                logger.info(
                                    "[INIT_STATE] symbol=%s timeframe=%s state=WAITING_FOR_HISTORY candles=%d/%d reason='%s' action='Accumulating live candles via WebSocket'",
                                    sym, self.timeframe, len(candles) if candles else 0, self.candle_limit, reason,
                                )
                                # For newly listed contracts, break startup retry loop to preserve rate limits
                                break
                            else:
                                # RETRY_PENDING
                                pause_rem = getattr(self.binance_client, "pause_remaining", 0.0)
                                retry_interval = max(attempt * 1.5, pause_rem + 0.5)
                                if attempt < 3:
                                    logger.warning(
                                        "[INIT_STATE] symbol=%s timeframe=%s state=RETRY_PENDING candles=%d/%d reason='%s' next_retry_in=%.1fs (attempt %d/3)",
                                        sym, self.timeframe, len(candles) if candles else 0, self.candle_limit, reason, retry_interval, attempt,
                                    )
                                    await asyncio.sleep(retry_interval)
                                else:
                                    logger.warning(
                                        "[INIT_STATE] symbol=%s timeframe=%s state=RETRY_PENDING candles=%d/%d reason='%s' (startup retries exhausted, will retry in background)",
                                        sym, self.timeframe, len(candles) if candles else 0, self.candle_limit, reason,
                                    )
                    except Exception as e:
                        pause_rem = getattr(self.binance_client, "pause_remaining", 0.0)
                        retry_interval = max(attempt * 1.5, pause_rem + 0.5)
                        async with self._lock:
                            self.symbol_states[sym] = STATE_RETRY_PENDING
                            if raw_sym != sym:
                                self.symbol_states[raw_sym] = STATE_RETRY_PENDING
                            self.symbol_details[sym] = {
                                "state": STATE_RETRY_PENDING,
                                "candles": 0,
                                "reason": f"Exception during fetch: {e}",
                            }
                            self.initialized_symbols.discard(sym)
                            self.initialized_symbols.discard(raw_sym)
                        if attempt < 3:
                            logger.warning(
                                "[INIT_STATE] symbol=%s timeframe=%s state=RETRY_PENDING error='%s' next_retry_in=%.1fs (attempt %d/3)",
                                sym, self.timeframe, str(e), retry_interval, attempt,
                            )
                            await asyncio.sleep(retry_interval)
                        else:
                            logger.error(
                                "[INIT_STATE] symbol=%s timeframe=%s state=RETRY_PENDING error='%s' (startup retries exhausted, will retry in background)",
                                sym, self.timeframe, str(e),
                            )

        tasks = [init_single(s) for s in self.symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        ready_count = len(self.initialized_symbols)
        waiting_count = self.get_state_count(STATE_WAITING_FOR_HISTORY)
        retry_count = self.get_state_count(STATE_RETRY_PENDING)
        logger.info(
            f"Initialization complete: {ready_count}/{len(self.symbols)} ready "
            f"(Waiting for history: {waiting_count}, Retry pending: {retry_count})."
        )

    async def retry_unready_symbols(
        self,
        max_concurrency: int = 2,
        pacing_delay_ms: float = 200.0,
    ) -> Dict[str, int]:
        """Retries symbols that are not yet READY without blocking live WebSocket processing."""
        unready = [s for s in self.symbols if self.get_symbol_state(s) != STATE_READY]
        if not unready:
            return self.get_initialization_summary()

        pause_rem = getattr(self.binance_client, "pause_remaining", 0.0)
        if pause_rem > 0:
            logger.info(f"Skipping retry of {len(unready)} unready symbols; Binance REST is paused for {pause_rem:.1f}s.")
            return self.get_initialization_summary()

        semaphore = asyncio.Semaphore(max_concurrency)
        pacer_lock = asyncio.Lock()
        last_req_time = 0.0

        async def paced_fetch(sym: str, limit: int) -> List[Dict[str, Any]]:
            nonlocal last_req_time
            async with pacer_lock:
                now = time.monotonic()
                elapsed_ms = (now - last_req_time) * 1000.0
                if elapsed_ms < pacing_delay_ms:
                    await asyncio.sleep((pacing_delay_ms - elapsed_ms) / 1000.0)
                last_req_time = time.monotonic()
                return await self.binance_client.get_klines(
                    sym, interval=self.timeframe, limit=limit, only_closed=True
                )

        async def retry_single(raw_sym: str) -> None:
            sym = self.binance_client.resolve_symbol(raw_sym) if hasattr(self.binance_client, "resolve_symbol") else raw_sym
            async with semaphore:
                try:
                    existing = self.candles_history.get(sym, [])
                    if not existing:
                        existing = await self.database.get_cached_candles(sym, self.timeframe, limit=self.candle_limit)
                    fresh = await paced_fetch(sym, limit=self.candle_limit)
                    candles = []
                    if fresh or existing:
                        by_timestamp = {int(c["timestamp"]): c for c in existing}
                        by_timestamp.update({int(c["timestamp"]): c for c in fresh})
                        candles = sorted(by_timestamp.values(), key=lambda c: int(c["timestamp"]))[-self.candle_limit:]

                    is_continuous = self.validate_candle_continuity(candles)

                    if candles and len(candles) >= self.candle_limit and is_continuous:
                        df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                        enriched = df.to_dict(orient="records")
                        async with self._lock:
                            self.candles_history[sym] = enriched
                            self.initialized_symbols.add(sym)
                            if raw_sym != sym:
                                self.initialized_symbols.add(raw_sym)
                            self.symbol_states[sym] = STATE_READY
                            if raw_sym != sym:
                                self.symbol_states[raw_sym] = STATE_READY
                            self.symbol_details[sym] = {
                                "state": STATE_READY,
                                "candles": len(candles),
                                "reason": "Background retry completed successfully",
                            }
                        await self.database.cache_candles(
                            enriched, sym, self.timeframe,
                            fast_period=self.fast_period, slow_period=self.slow_period
                        )
                        logger.info(
                            "[SYMBOL_PROMOTED_TO_READY] %s is now READY via background retry (%d/%d candles)",
                            sym, len(candles), self.candle_limit,
                        )
                    else:
                        is_exhausted = False
                        if hasattr(self.binance_client, "is_history_exhausted"):
                            is_exhausted = bool(self.binance_client.is_history_exhausted(sym))
                        elif candles and len(candles) < self.candle_limit:
                            pause_rem = getattr(self.binance_client, "pause_remaining", 0.0)
                            is_exhausted = (pause_rem <= 0.0)

                        new_state = STATE_WAITING_FOR_HISTORY if (is_exhausted and is_continuous) else STATE_RETRY_PENDING
                        if candles and is_continuous:
                            df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                            enriched = df.to_dict(orient="records")
                            async with self._lock:
                                self.candles_history[sym] = enriched
                            await self.database.cache_candles(
                                enriched, sym, self.timeframe,
                                fast_period=self.fast_period, slow_period=self.slow_period
                            )
                        async with self._lock:
                            self.symbol_states[sym] = new_state
                            if raw_sym != sym:
                                self.symbol_states[raw_sym] = new_state
                            self.symbol_details[sym] = {
                                "state": new_state,
                                "candles": len(candles) if candles else 0,
                                "reason": f"{new_state} after background retry ({len(candles) if candles else 0}/{self.candle_limit})",
                            }
                except Exception as e:
                    logger.debug(f"Background retry error for {sym}: {e}")

        await asyncio.gather(*[retry_single(s) for s in unready], return_exceptions=True)
        return self.get_initialization_summary()

    async def handle_closed_candle(self, symbol: str, candle: Dict[str, Any]) -> Optional[GoldenCrossSignal]:
        """Called when a 1H candle closes for a symbol."""
        if self.timeframe != "1h" or not candle.get("is_closed", False):
            logger.warning("LIVE_SIGNAL_REJECTED: only closed Binance USD-M Futures 1H candles are eligible")
            return None
        try:
            timestamp = int(candle["timestamp"])
            if timestamp <= 0:
                raise ValueError("invalid 1H open timestamp")
        except (KeyError, TypeError, ValueError):
            logger.error("LIVE_SIGNAL_REJECTED: invalid 1H candle timestamp for %s", symbol)
            return None
        symbol = symbol.upper()
        resolved_sym = self.binance_client.resolve_symbol(symbol) if hasattr(self.binance_client, "resolve_symbol") else symbol
        if self.symbols and symbol not in self.symbols and resolved_sym not in self.symbols:
            logger.error("LIVE_SIGNAL_REJECTED: unmonitored symbol %s", symbol)
            return None
        self.last_closed_candle_time = max(self.last_closed_candle_time or 0, timestamp)

        async with self._lock:
            history = self.candles_history.get(symbol, [])
            if not history and resolved_sym != symbol:
                history = self.candles_history.get(resolved_sym, [])

            if history and timestamp < int(history[-1]["timestamp"]):
                logger.warning(
                    "LIVE_SIGNAL_REJECTED: out-of-order candle for %s timestamp=%s latest=%s",
                    symbol, timestamp, history[-1]["timestamp"],
                )
                return None
            if history and int(history[-1]["timestamp"]) == timestamp:
                history[-1] = candle
            else:
                history.append(candle)

            if len(history) > self.candle_limit:
                history = history[-self.candle_limit:]

            # Enrich with EMA
            df = enrich_candles_with_ema(history, self.fast_period, self.slow_period)
            self.candles_history[symbol] = df.to_dict(orient="records")
            if resolved_sym != symbol:
                self.candles_history[resolved_sym] = self.candles_history[symbol]

            is_continuous = self.validate_candle_continuity(history)
            signal = None

            if len(df) >= self.candle_limit and is_continuous:
                if symbol not in self.initialized_symbols:
                    self.initialized_symbols.add(symbol)
                    if resolved_sym != symbol:
                        self.initialized_symbols.add(resolved_sym)
                    self.symbol_states[symbol] = STATE_READY
                    if resolved_sym != symbol:
                        self.symbol_states[resolved_sym] = STATE_READY
                    self.symbol_details[symbol] = {
                        "state": STATE_READY,
                        "candles": len(df),
                        "reason": "Accumulated sufficient closed candles via WebSocket",
                    }
                    logger.info(
                        "[SYMBOL_PROMOTED_TO_READY] %s has accumulated %d/%d closed 1H candles. Transitioned to READY.",
                        symbol, len(df), self.candle_limit,
                    )

                signal = detect_golden_cross(
                    df,
                    symbol=symbol,
                    timeframe=self.timeframe,
                    fast_col=f"ema_{self.fast_period}",
                    slow_col=f"ema_{self.slow_period}",
                    fast_period=self.fast_period,
                    slow_period=self.slow_period,
                )
            else:
                # Still under candle_limit or discontinuous: fail-closed, NO signal
                curr_state = self.get_symbol_state(symbol)
                if curr_state != STATE_READY:
                    self.symbol_states[symbol] = STATE_WAITING_FOR_HISTORY
                    if resolved_sym != symbol:
                        self.symbol_states[resolved_sym] = STATE_WAITING_FOR_HISTORY
                    self.symbol_details[symbol] = {
                        "state": STATE_WAITING_FOR_HISTORY,
                        "candles": len(df),
                        "reason": f"Accumulating live candles ({len(df)}/{self.candle_limit})",
                    }
                logger.debug(
                    "INSUFFICIENT_HISTORY: %s has %d/%d closed candles. No signal evaluated.",
                    symbol, len(df), self.candle_limit,
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
                "ema_fast": signal.ema_fast,
                "ema_slow": signal.ema_slow,
                "fast_period": self.fast_period,
                "slow_period": self.slow_period,
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
                previous_ema50=signal.previous_ema50,
                previous_ema200=signal.previous_ema200,
                fast_period=self.fast_period,
                slow_period=self.slow_period,
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
        crosses = find_all_golden_crosses(
            df,
            symbol=symbol,
            timeframe=self.timeframe,
            fast_col=f"ema_{self.fast_period}",
            slow_col=f"ema_{self.slow_period}",
            fast_period=self.fast_period,
            slow_period=self.slow_period,
        )

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
                    previous_ema50=c.previous_ema50,
                    previous_ema200=c.previous_ema200,
                    fast_period=self.fast_period,
                    slow_period=self.slow_period,
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
                        "ema_fast": crosses[-1].ema_fast,
                        "ema_slow": crosses[-1].ema_slow,
                        "fast_period": self.fast_period,
                        "slow_period": self.slow_period,
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

                async def warm_new(raw_sym: str):
                    sym = self.binance_client.resolve_symbol(raw_sym) if hasattr(self.binance_client, "resolve_symbol") else raw_sym
                    async with semaphore:
                        try:
                            candles = await self.binance_client.get_klines(
                                sym, interval=self.timeframe, limit=self.candle_limit, only_closed=True
                            )
                            is_continuous = self.validate_candle_continuity(candles)
                            if candles and len(candles) >= self.candle_limit and is_continuous:
                                df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                                enriched = df.to_dict(orient="records")
                                async with self._lock:
                                    self.candles_history[sym] = enriched
                                    self.initialized_symbols.add(sym)
                                    if raw_sym != sym:
                                        self.initialized_symbols.add(raw_sym)
                                    self.symbol_states[sym] = STATE_READY
                                    if raw_sym != sym:
                                        self.symbol_states[raw_sym] = STATE_READY
                                    self.symbol_details[sym] = {
                                        "state": STATE_READY,
                                        "candles": len(candles),
                                        "reason": "Initialized during universe refresh",
                                    }
                                await self.database.cache_candles(
                                    enriched, sym, self.timeframe,
                                    fast_period=self.fast_period, slow_period=self.slow_period
                                )
                            else:
                                is_exhausted = hasattr(self.binance_client, "is_history_exhausted") and self.binance_client.is_history_exhausted(sym)
                                state = STATE_WAITING_FOR_HISTORY if (is_exhausted and is_continuous) else STATE_RETRY_PENDING
                                if candles and is_continuous:
                                    df = enrich_candles_with_ema(candles, self.fast_period, self.slow_period)
                                    enriched = df.to_dict(orient="records")
                                    async with self._lock:
                                        self.candles_history[sym] = enriched
                                    await self.database.cache_candles(
                                        enriched, sym, self.timeframe,
                                        fast_period=self.fast_period, slow_period=self.slow_period
                                    )
                                async with self._lock:
                                    self.symbol_states[sym] = state
                                    if raw_sym != sym:
                                        self.symbol_states[raw_sym] = state
                                    self.symbol_details[sym] = {
                                        "state": state,
                                        "candles": len(candles) if candles else 0,
                                        "reason": f"Discovered during refresh ({len(candles) if candles else 0}/{self.candle_limit} candles)",
                                    }
                        except Exception as e:
                            logger.error(f"Error warming new symbol {sym}: {e}")

                await asyncio.gather(*[warm_new(s) for s in new_symbols], return_exceptions=True)

            return self.symbols
        except Exception as e:
            logger.error(f"Error during symbol universe refresh: {e}")
            return self.symbols
