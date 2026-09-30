"""NEXORA Terminal Dashboard strictly adhering to Section 24 audit requirements."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from app.charts.chart_theme import format_price
from app.engine.signal_engine import SignalEngine
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.exchange.binance_client import BinanceFuturesClient
from app.notifications.telegram_bot import TelegramNotifier
from app.persistence.database import Database


class TerminalDashboard:
    def __init__(
        self,
        signal_engine: SignalEngine,
        ws_manager: BinanceWebSocketManager,
        binance_client: BinanceFuturesClient,
        telegram_notifier: TelegramNotifier,
        database: Database,
        port: int = 8080,
    ) -> None:
        self.signal_engine = signal_engine
        self.ws_manager = ws_manager
        self.binance_client = binance_client
        self.telegram_notifier = telegram_notifier
        self.database = database
        self.port = port
        self.console = Console()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        await asyncio.sleep(1.0)
        with Live(self.generate_display_group(), console=self.console, refresh_per_second=2, screen=False) as live:
            while self._running:
                try:
                    display = await self.build_dashboard_group()
                    live.update(display)
                    await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    break
                except Exception:
                    await asyncio.sleep(1.0)

    def generate_display_group(self) -> Group:
        header = Panel(
            Text("⚡ NEXORA EMA CROSS — INITIALIZING SYSTEM...", style="bold cyan"),
            border_style="cyan",
        )
        return Group(header)

    async def build_dashboard_group(self) -> Group:
        now = time.time()
        uptime_sec = int(now - self.signal_engine.start_time)
        hours, rem = divmod(uptime_sec, 3600)
        mins, secs = divmod(rem, 60)
        uptime_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

        # 1. Header Banner (Section 24)
        header_text = Text()
        header_text.append("  NEXORA EMA CROSS  ", style="bold white on dark_blue")
        header_text.append(" • BINANCE USD-M FUTURES\n", style="bold cyan")
        header_text.append("  MODE: LONG ONLY  |  SIGNAL: EMA50 CROSS ABOVE EMA200  |  TIMEFRAME: 1H\n", style="bold yellow")
        header_text.append(f"  WEB DASHBOARD & CHART: http://localhost:{self.port}", style="green")

        header_panel = Panel(header_text, border_style="cyan", padding=(0, 1))

        # 2. System Status Metrics Table (Strictly Req 24)
        status_table = Table(box=None, expand=True, show_header=False)
        status_table.add_column("Key1", style="bold white", width=22)
        status_table.add_column("Val1", style="bold", width=24)
        status_table.add_column("Key2", style="bold white", width=22)
        status_table.add_column("Val2", style="bold", width=24)

        ws_style = "bold green" if self.ws_manager.is_connected else "bold yellow"
        ws_status = "CONNECTED" if self.ws_manager.is_connected else "DISCONNECTED"

        tg_style = "bold green" if self.telegram_notifier.is_configured else "bold yellow"
        tg_status = "ONLINE" if self.telegram_notifier.is_configured else "TELEGRAM CONFIGURATION MISSING"

        init_count = len(self.signal_engine.initialized_symbols)
        total_syms = len(self.signal_engine.symbols)
        init_style = "green" if init_count >= total_syms and total_syms > 0 else "yellow"

        total_signals = await self.database.get_total_signals_count()
        live_signals = await self.database.get_live_signals_count()
        historical_signals = await self.database.get_historical_signals_count()
        last_sig = await self.database.get_last_signal() or self.signal_engine.last_signal
        last_sig_str = f"{last_sig['symbol']} ({last_sig.get('signal_time_utc', '')})" if last_sig else "NONE"

        last_closed_candle_str = "-"
        if self.signal_engine.last_closed_candle_time:
            dt = datetime.fromtimestamp(self.signal_engine.last_closed_candle_time / 1000.0, tz=timezone.utc)
            last_closed_candle_str = dt.strftime("%d %b %H:%M UTC")

        # Row 1: Core Connectivity
        status_table.add_row(
            "BINANCE:", Text("ONLINE", style="bold green"),
            "SYMBOLS:", Text(f"{total_syms}", style="white"),
        )
        # Row 2: Stream & Init
        status_table.add_row(
            "WEBSOCKET:", Text(ws_status, style=ws_style),
            "INITIALIZED:", Text(f"{init_count} / {total_syms}", style=init_style),
        )
        # Row 3: Telegram & Database
        status_table.add_row(
            "TELEGRAM:", Text(tg_status, style=tg_style),
            "DATABASE:", Text("ONLINE", style="bold green"),
        )
        # Row 4: Live Alerts vs Historical Crosses
        status_table.add_row(
            "LIVE ALERTS (TG):", Text(f"{live_signals}", style="bold green"),
            "HISTORICAL CROSSES:", Text(f"{historical_signals}", style="bold cyan"),
        )
        # Row 5: Last Signal & Uptime
        status_table.add_row(
            "LAST CLOSED CANDLE:", Text(last_closed_candle_str, style="white"),
            "UPTIME:", Text(uptime_str, style="white"),
        )
        # Row 6: Last Golden Cross & Reconnects
        status_table.add_row(
            "LAST GOLDEN CROSS:", Text(last_sig_str, style="bold cyan"),
            "RECONNECT COUNT:", Text(f"{self.ws_manager.reconnect_count}", style="white"),
        )
        # Row 7: Rate Limit Counters
        c429_style = "white" if self.binance_client.rate_limit_429_count == 0 else "bold yellow"
        c418_style = "white" if self.binance_client.ip_ban_418_count == 0 else "bold red"
        status_table.add_row(
            "429 / 418 COUNT:", Text(f"{self.binance_client.rate_limit_429_count} / {self.binance_client.ip_ban_418_count}", style=c429_style),
            "TOTAL SIGNALS:", Text(f"{total_signals}", style="bold cyan"),
        )

        status_panel = Panel(status_table, title="[bold cyan]● LIVE SYSTEM STATUS[/bold cyan]", border_style="blue")

        # 3. Recent Signals Table
        recent_signals = await self.database.get_recent_signals(limit=5)
        signals_table = Table(box=None, expand=True)
        signals_table.add_column("SYMBOL", style="bold white", width=12)
        signals_table.add_column("TIME (UTC)", style="cyan", width=22)
        signals_table.add_column("CLOSE PRICE", style="white", justify="right", width=16)
        signals_table.add_column("EMA 50", style="bright_cyan", justify="right", width=16)
        signals_table.add_column("EMA 200", style="bright_yellow", justify="right", width=16)
        signals_table.add_column("SIGNAL STATUS", style="bold green", width=24)

        if recent_signals:
            for s in recent_signals:
                signals_table.add_row(
                    s["symbol"],
                    s["signal_time_utc"],
                    format_price(s["close_price"]),
                    format_price(s["ema50"]),
                    format_price(s["ema200"]),
                    "● GOLDEN CROSS CONFIRMED",
                )
        else:
            signals_table.add_row("-", "Scanning closed 1H candles...", "-", "-", "-", "MONITORING...")

        signals_panel = Panel(signals_table, title="[bold green]● RECENT GOLDEN CROSS DETECTIONS[/bold green]", border_style="green")

        # 4. Monitored Universe Snapshot
        sym_table = Table(box=None, expand=True)
        sym_table.add_column("SYMBOL", style="bold white", width=12)
        sym_table.add_column("LAST 1H CLOSE", style="white", justify="right", width=16)
        sym_table.add_column("EMA 50", style="bright_cyan", justify="right", width=16)
        sym_table.add_column("EMA 200", style="bright_yellow", justify="right", width=16)
        sym_table.add_column("SPREAD", style="cyan", justify="right", width=16)

        for sym in self.signal_engine.symbols[:6]:
            history = self.signal_engine.candles_history.get(sym, [])
            if history:
                last_c = history[-1]
                close_p = last_c.get("close", 0.0)
                e50 = last_c.get("ema_50", 0.0)
                e200 = last_c.get("ema_200", 0.0)
                spread = ((e50 - e200) / e200 * 100) if e200 and e50 else 0.0
                sym_table.add_row(
                    sym,
                    format_price(close_p),
                    format_price(e50) if e50 else "-",
                    format_price(e200) if e200 else "-",
                    f"{spread:+.2f}%",
                )
            else:
                sym_table.add_row(sym, "Loading...", "-", "-", "-")

        universe_panel = Panel(sym_table, title="[bold white]● MONITORED MARKET OVERVIEW[/bold white]", border_style="dim")

        return Group(header_panel, status_panel, signals_panel, universe_panel)
