"""Asynchronous Non-blocking Alert Queue & Chart Worker."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional, Tuple
from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.indicators.ema import GoldenCrossSignal
from app.notifications.telegram_bot import TelegramNotifier
from app.persistence.database import Database

logger = logging.getLogger("nexora.engine.alert_queue")


class AlertQueue:
    def __init__(
        self,
        chart_data_provider: ChartDataProvider,
        chart_renderer: ChartRenderer,
        telegram_notifier: TelegramNotifier,
        database: Database,
        fast_period: int = 50,
        slow_period: int = 200,
    ) -> None:
        self.chart_data_provider = chart_data_provider
        self.chart_renderer = chart_renderer
        self.telegram_notifier = telegram_notifier
        self.database = database
        self.fast_period = fast_period
        self.slow_period = slow_period

        self._queue: asyncio.Queue[Tuple[int, GoldenCrossSignal]] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self.total_processed = 0
        self.total_failed = 0

    def start(self) -> None:
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._worker_loop())
            logger.info("AlertQueue worker started.")

    async def stop(self) -> None:
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None
            logger.info("AlertQueue worker stopped.")

    async def enqueue(self, signal_id: int, signal: GoldenCrossSignal) -> None:
        """Enqueues a signal without blocking the caller."""
        await self._queue.put((signal_id, signal))
        logger.info(f"Signal for {signal.symbol} enqueued (queue depth: {self._queue.qsize()})")

    async def _worker_loop(self) -> None:
        while self._running:
            try:
                signal_id, signal = await self._queue.get()
                await self._process_alert(signal_id, signal)
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in alert worker loop: {e}", exc_info=True)
                await asyncio.sleep(0.5)

    async def _process_alert(self, signal_id: int, signal: GoldenCrossSignal) -> None:
        """Processes a single alert: validates canonical cross on 1000 candles, renders chart, sends telegram, updates database."""
        chart_path: Optional[str] = None
        try:
            fast_p = getattr(signal, "fast_period", self.fast_period)
            slow_p = getattr(signal, "slow_period", self.slow_period)

            # 1. Fetch centered chart data using 1000 closed candles from REST
            chart_data = await self.chart_data_provider.get_chart_data(
                symbol=signal.symbol,
                timeframe=signal.timeframe.lower(),
                limit=150,
                target_timestamp=signal.candle_timestamp,
                force_fresh=True,
                fast_period=fast_p,
                slow_period=slow_p,
            )

            # 2. Independent Canonical Fail-Closed Validation Guard:
            # Verify the crossover event strictly holds on the converged 1000-candle dataset.
            df = chart_data.get("df_full")
            if df is None:
                df = chart_data.get("df")
            if df is None or len(df) < 1000:
                logger.error(
                    f"ALERT_ABORTED_FAIL_CLOSED: Insufficient candle data for {signal.symbol} "
                    f"({0 if df is None else len(df)} < 1000). Alert dropped."
                )
                self.total_failed += 1
                return

            df = df.reset_index(drop=True)
            target_ts = signal.candle_timestamp
            match_indices = df.index[df["timestamp"] == target_ts].tolist()
            if not match_indices or match_indices[0] < 1:
                logger.error(f"ALERT_ABORTED_FAIL_CLOSED: Target timestamp {target_ts} not found or lacks prior candle for {signal.symbol}. Alert dropped.")
                self.total_failed += 1
                return

            target_pos = match_indices[0]
            curr_candle = df.iloc[target_pos]
            prev_candle = df.iloc[target_pos - 1]

            fast_col = f"ema_{fast_p}"
            slow_col = f"ema_{slow_p}"

            curr_fast = float(curr_candle.get(fast_col, curr_candle.get("ema_fast", curr_candle.get("ema_50", 0.0))))
            curr_slow = float(curr_candle.get(slow_col, curr_candle.get("ema_slow", curr_candle.get("ema_200", 0.0))))
            prev_fast = float(prev_candle.get(fast_col, prev_candle.get("ema_fast", prev_candle.get("ema_50", 0.0))))
            prev_slow = float(prev_candle.get(slow_col, prev_candle.get("ema_slow", prev_candle.get("ema_200", 0.0))))

            is_valid_canonical_cross = (prev_fast <= prev_slow) and (curr_fast > curr_slow)
            if not is_valid_canonical_cross:
                logger.error(
                    f"ALERT_ABORTED_FAIL_CLOSED: {signal.symbol} at {target_ts} failed independent canonical "
                    f"1000-candle validation (prev: {prev_fast:.6f} vs {prev_slow:.6f}, "
                    f"curr: {curr_fast:.6f} vs {curr_slow:.6f}). Alert dropped."
                )
                self.total_failed += 1
                return

            # 3. Render chart image in thread executor
            loop = asyncio.get_running_loop()
            chart_path = await loop.run_in_executor(
                None,
                self.chart_renderer.render_golden_cross_chart,
                chart_data,
                signal.candle_timestamp,
            )
            if not chart_path or not os.path.isfile(chart_path):
                logger.error(f"ALERT_ABORTED_FAIL_CLOSED: Chart generation failed for {signal.symbol}. Alert dropped.")
                self.total_failed += 1
                return

        except Exception as e:
            logger.error(f"ALERT_PROCESSING_ERROR: Failed validating/rendering for {signal.symbol}: {e}", exc_info=True)
            self.total_failed += 1
            return

        # 4. Deliver via Telegram only after all fail-closed guards passed
        telegram_sent = False
        try:
            telegram_sent = await self.telegram_notifier.send_golden_cross_alert(
                signal=signal,
                chart_image_path=chart_path,
            )
        except Exception as e:
            logger.error(f"Failed delivering Telegram alert for {signal.symbol}: {e}")

        # 5. Update database delivery state
        try:
            await self.database.update_signal_delivery(
                signal_id=signal_id,
                chart_image_path=chart_path,
                telegram_sent=telegram_sent,
            )
        except Exception as e:
            logger.error(f"Failed updating delivery record for signal {signal_id}: {e}")

        self.total_processed += 1
        logger.info(f"Alert processing complete for {signal.symbol} (Chart: OK, Telegram: {telegram_sent})")
