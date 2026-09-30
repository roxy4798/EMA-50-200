"""Asynchronous Non-blocking Alert Queue & Chart Worker."""

from __future__ import annotations

import asyncio
import logging
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
    ) -> None:
        self.chart_data_provider = chart_data_provider
        self.chart_renderer = chart_renderer
        self.telegram_notifier = telegram_notifier
        self.database = database

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
        """Processes a single alert: renders chart, sends telegram, updates database."""
        chart_path: Optional[str] = None
        try:
            # 1. Fetch centered chart data
            chart_data = await self.chart_data_provider.get_chart_data(
                symbol=signal.symbol,
                timeframe=signal.timeframe.lower(),
                limit=150,
                target_timestamp=signal.candle_timestamp,
                force_fresh=True,
            )

            # 2. Render chart image in thread executor to prevent event-loop blocking
            loop = asyncio.get_running_loop()
            chart_path = await loop.run_in_executor(
                None,
                self.chart_renderer.render_golden_cross_chart,
                chart_data,
                signal.candle_timestamp,
            )
        except Exception as e:
            logger.error(f"CHART_RENDER_ERROR: Worker failed rendering for {signal.symbol}: {e}")
            chart_path = None

        # 3. Deliver via Telegram (even if chart rendering returned None)
        telegram_sent = False
        try:
            telegram_sent = await self.telegram_notifier.send_golden_cross_alert(
                signal=signal,
                chart_image_path=chart_path,
            )
        except Exception as e:
            logger.error(f"Failed delivering Telegram alert for {signal.symbol}: {e}")

        # 4. Update database delivery state
        try:
            await self.database.update_signal_delivery(
                signal_id=signal_id,
                chart_image_path=chart_path,
                telegram_sent=telegram_sent,
            )
        except Exception as e:
            logger.error(f"Failed updating delivery record for signal {signal_id}: {e}")

        self.total_processed += 1
        logger.info(f"Alert processing complete for {signal.symbol} (Chart: {'OK' if chart_path else 'FAILED'}, Telegram: {telegram_sent})")
