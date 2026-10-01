"""NEXORA Telegram Command Views and Formatting."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.charts.chart_theme import format_price
from app.indicators.ema import enrich_candles_with_ema

logger = logging.getLogger("nexora.telegram.commands")

COMMANDS_MENU = [
    {"command": "start", "description": "Launch NEXORA"},
    {"command": "dashboard", "description": "System overview"},
    {"command": "market", "description": "Market monitor"},
    {"command": "alerts", "description": "Recent Golden Cross alerts"},
    {"command": "symbol", "description": "Inspect a symbol"},
    {"command": "history", "description": "Signal history"},
    {"command": "status", "description": "Technical system status"},
    {"command": "help", "description": "Command guide"},
]


def build_start_view() -> Tuple[str, Dict[str, Any]]:
    """Builds /start welcome screen with inline navigation."""
    text = (
        "NEXORA EMA CROSS\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Real-time Binance Futures\n"
        "EMA 50 / EMA 200 Golden Cross Monitor\n\n"
        "TIMEFRAME   1H\n"
        "SIGNAL      LONG ONLY\n"
        "UNIVERSE    USDT PERPETUALS\n\n"
        "● Monitoring active\n\n"
        "Use /dashboard to view system status."
    )
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
                {"text": "ALERTS", "callback_data": "btn:alerts:1"},
            ],
            [
                {"text": "MARKET", "callback_data": "btn:market"},
                {"text": "HELP", "callback_data": "btn:help"},
            ],
        ]
    }
    return text, keyboard


async def build_dashboard_view(
    signal_engine: Any,
    ws_manager: Any,
    database: Any,
    binance_client: Any,
    telegram_notifier: Any,
) -> Tuple[str, Dict[str, Any]]:
    """Builds /dashboard system overview using live application state."""
    binance_status = "ONLINE" if (binance_client and await binance_client.check_connectivity()) else "ONLINE"
    ws_status = "CONNECTED" if (ws_manager and ws_manager.is_connected) else "DISCONNECTED"
    market_data_status = (
        ws_manager.get_market_data_health().get("status", "OFFLINE") if ws_manager else "OFFLINE"
    )
    telegram_status = "ONLINE" if (telegram_notifier and telegram_notifier.is_configured) else "OFFLINE"

    db_status = "ONLINE"
    try:
        if database:
            await database.get_total_signals_count()
        else:
            db_status = "OFFLINE"
    except Exception:
        db_status = "OFFLINE"

    symbols_count = len(signal_engine.symbols) if signal_engine else 0
    tf_str = signal_engine.timeframe.upper() if signal_engine else "1H"
    fast_p = signal_engine.fast_period if signal_engine else 50
    slow_p = signal_engine.slow_period if signal_engine else 200

    hist_count = await database.get_historical_signals_count() if database else 0
    live_today = await database.get_live_signals_today_count() if database else 0
    last_sig = (await database.get_last_signal()) if database else None
    if not last_sig and signal_engine:
        last_sig = signal_engine.last_signal
    last_sig_str = last_sig["symbol"] if (last_sig and "symbol" in last_sig) else "NONE"

    uptime_str = "00h 00m"
    if signal_engine and hasattr(signal_engine, "start_time"):
        elapsed = int(time.time() - signal_engine.start_time)
        hrs, rem = divmod(elapsed, 3600)
        mins, _ = divmod(rem, 60)
        uptime_str = f"{hrs:02d}h {mins:02d}m"

    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")

    text = (
        "NEXORA DASHBOARD\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "SYSTEM\n"
        f"● Binance        {binance_status}\n"
        f"● WebSocket      {ws_status}\n"
        f"● Market Data    {market_data_status}\n"
        f"● Telegram       {telegram_status}\n"
        f"● Database       {db_status}\n\n"
        "MARKET\n"
        f"Symbols          {symbols_count}\n"
        f"Timeframe        {tf_str}\n"
        f"Strategy         EMA {fast_p} / EMA {slow_p}\n\n"
        "SIGNALS\n"
        f"Historical       {hist_count}\n"
        f"Live Today       {live_today}\n"
        f"Last Signal      {last_sig_str}\n\n"
        "UPTIME\n"
        f"{uptime_str}\n\n"
        f"Updated {now_utc}"
    )

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "MARKET", "callback_data": "btn:market"},
                {"text": "ALERTS", "callback_data": "btn:alerts:1"},
            ],
            [
                {"text": "HISTORY", "callback_data": "btn:history:all"},
                {"text": "STATUS", "callback_data": "btn:status"},
            ],
            [
                {"text": "REFRESH", "callback_data": "btn:dashboard"},
            ],
        ]
    }
    return text, keyboard


async def build_market_view(signal_engine: Any, ws_manager: Any) -> Tuple[str, Dict[str, Any]]:
    """Builds /market market monitor view using live WebSocket & scanner state."""
    symbols_count = len(signal_engine.symbols) if signal_engine else 0
    tf_str = signal_engine.timeframe.upper() if signal_engine else "1H"
    connections = len(ws_manager._tasks) if (ws_manager and ws_manager._tasks) else ((symbols_count + 99) // 100 if symbols_count else 0)
    streams = len(ws_manager.symbols) if (ws_manager and ws_manager.symbols) else symbols_count

    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
    health = ws_manager.get_market_data_health() if ws_manager else {"status": "OFFLINE", "is_healthy": False}
    scanner_status = health["status"]

    text = (
        "MARKET MONITOR\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Universe\n"
        f"{symbols_count} USDT Perpetuals\n\n"
        "Scanner\n"
        f"● {scanner_status}\n\n"
        "WebSocket\n"
        f"{connections} connections\n"
        f"{streams} streams\n\n"
        "Timeframe\n"
        f"{tf_str}\n\n"
        "Candle Status\n"
        "WAITING FOR CLOSE\n\n"
        f"Updated {now_utc}"
    )

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
                {"text": "ALERTS", "callback_data": "btn:alerts:1"},
            ],
            [
                {"text": "REFRESH", "callback_data": "btn:market"},
            ],
        ]
    }
    return text, keyboard


async def build_alerts_view(database: Any, page: int = 1) -> Tuple[str, Dict[str, Any]]:
    """Builds /alerts view with pagination."""
    page_size = 10
    signals, total = (await database.get_signals_page(page=page, page_size=page_size)) if database else ([], 0)
    total_pages = max(1, (total + page_size - 1) // page_size) if total > 0 else 1
    safe_page = min(max(1, page), total_pages)

    lines = [
        "RECENT GOLDEN CROSSES",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]
    if not signals:
        lines.append("No Golden Cross signals recorded yet.")
    else:
        for s in signals:
            lines.append(f"{s['symbol']}")
            lines.append(f"{s.get('signal_time_utc', '')}")
            lines.append("")

    text = "\n".join(lines).strip()

    nav_row = []
    if safe_page > 1:
        nav_row.append({"text": "←", "callback_data": f"btn:alerts:{safe_page - 1}"})
    nav_row.append({"text": f"{safe_page} / {total_pages}", "callback_data": "noop"})
    if safe_page < total_pages:
        nav_row.append({"text": "→", "callback_data": f"btn:alerts:{safe_page + 1}"})

    keyboard = {
        "inline_keyboard": [
            nav_row,
            [
                {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
                {"text": "REFRESH", "callback_data": f"btn:alerts:{safe_page}"},
            ],
        ]
    }
    return text, keyboard


async def build_symbol_view(
    raw_input: str,
    signal_engine: Any,
    binance_client: Any,
    database: Any,
) -> Tuple[str, Dict[str, Any]]:
    """Builds /symbol inspect screen with validation and aliases."""
    sym_clean = raw_input.strip().upper()
    if not sym_clean:
        text = "Usage:\n/symbol BTCUSDT"
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
                    {"text": "ALERTS", "callback_data": "btn:alerts:1"},
                ]
            ]
        }
        return text, keyboard

    # Resolve alias
    resolved = binance_client.resolve_symbol(sym_clean) if binance_client else sym_clean

    # Verify if symbol in monitored universe
    all_syms = set(signal_engine.symbols) if (signal_engine and signal_engine.symbols) else set()
    is_in_universe = (resolved in all_syms) or (sym_clean in all_syms)

    if not is_in_universe:
        text = (
            "SYMBOL NOT FOUND\n\n"
            f"{sym_clean} is not currently available in the NEXORA monitored universe."
        )
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
                    {"text": "ALERTS", "callback_data": "btn:alerts:1"},
                ]
            ]
        }
        return text, keyboard

    # Status
    is_init = (resolved in signal_engine.initialized_symbols) if signal_engine else False
    status_str = "● MONITORING" if is_init else "● INITIALIZING"

    # Current structure always comes from the newest closed USD-M Futures REST
    # frame. Do not silently present cached/in-memory values as current when REST
    # is unavailable.
    candles = []
    if binance_client:
        klines = await binance_client.get_klines(resolved, interval="1h", limit=1000, only_closed=True)
        if klines:
            df = enrich_candles_with_ema(klines, 50, 200)
            candles = df.to_dict(orient="records")

    ema_section = ""
    if candles and len(candles) >= 2:
        last_c = candles[-1]
        prev_c = candles[-2]
        ema50 = last_c.get("ema_50")
        ema200 = last_c.get("ema_200")
        prev_ema50 = prev_c.get("ema_50")
        prev_ema200 = prev_c.get("ema_200")
        close_price = last_c.get("close", 0.0)
        ts = int(last_c.get("timestamp", 0))
        dt_str = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).strftime("%d %b %Y • %H:%M UTC")

        if ema50 is not None and ema200 is not None and not (isinstance(ema50, float) and ema50 != ema50):
            if prev_ema50 is not None and prev_ema200 is not None and prev_ema50 <= prev_ema200 and ema50 > ema200:
                signal_val = "GOLDEN CROSS CONFIRMED"
            elif ema50 > ema200:
                signal_val = "BULLISH"
            elif ema50 < ema200:
                signal_val = "BEARISH"
            else:
                signal_val = "NEUTRAL"

            ema_section = (
                "EMA STRUCTURE (1H CLOSED CANDLE)\n"
                f"Time      {dt_str}\n"
                f"Close     {format_price(close_price)}\n"
                f"EMA 50    {format_price(ema50)}\n"
                f"EMA 200   {format_price(ema200)}\n\n"
                f"SIGNAL\n{signal_val}\n\n"
            )
        else:
            ema_section = "EMA STRUCTURE\nEMA data unavailable.\n\n"
    else:
        ema_section = "CURRENT STRUCTURE (1H CLOSED CANDLE)\nBinance USD-M Futures market data unavailable.\n\n"
        status_str = "● DATA STALE / NO MARKET DATA"

    # Last golden cross from DB
    last_cross = (await database.get_last_signal_for_symbol(resolved)) if database else None
    if last_cross and last_cross.get("signal_time_utc"):
        last_cross_str = last_cross["signal_time_utc"].replace(" • ", "\n")
    else:
        last_cross_str = "None recorded"

    tf_str = signal_engine.timeframe.upper() if signal_engine else "1H"

    text = (
        f"{resolved}\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "STATUS\n"
        f"{status_str}\n\n"
        "TIMEFRAME\n"
        f"{tf_str}\n\n"
        f"{ema_section}"
        "LAST GOLDEN CROSS\n"
        f"{last_cross_str}"
    )

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "VIEW CHART", "callback_data": f"chart:{resolved}"},
            ],
            [
                {"text": "ALERTS", "callback_data": "btn:alerts:1"},
                {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
            ],
        ]
    }
    return text, keyboard


async def build_history_view(database: Any, window_filter: str = "all") -> Tuple[str, Dict[str, Any]]:
    """Builds /history statistics and window filters."""
    stats = (await database.get_history_stats()) if database else {
        "total": 0, "count_24h": 0, "count_7d": 0, "count_30d": 0, "latest": None
    }

    if window_filter in ("24h", "7d", "30d"):
        hours = 24 if window_filter == "24h" else (7 * 24 if window_filter == "7d" else 30 * 24)
        signals = (await database.get_signals_in_window(window_hours=hours, limit=10)) if database else []
        title_tag = window_filter.upper()
        count_val = len(signals)

        lines = [
            f"SIGNAL HISTORY ({title_tag})",
            "━━━━━━━━━━━━━━━━━━",
            "",
            f"COUNT: {count_val}",
            "",
        ]
        if not signals:
            lines.append(f"No Golden Cross signals in the last {title_tag}.")
        else:
            for s in signals:
                lines.append(f"{s['symbol']}")
                lines.append(f"{s.get('signal_time_utc', '')}")
                lines.append("")

        text = "\n".join(lines).strip()
    else:
        latest = stats.get("latest")
        if latest and "symbol" in latest:
            latest_str = f"{latest['symbol']}\n{latest.get('signal_time_utc', '')}"
        else:
            latest_str = "None recorded"

        text = (
            "SIGNAL HISTORY\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "TOTAL\n"
            f"{stats.get('total', 0)}\n\n"
            "LAST 24H\n"
            f"{stats.get('count_24h', 0)}\n\n"
            "LAST 7D\n"
            f"{stats.get('count_7d', 0)}\n\n"
            "LAST 30D\n"
            f"{stats.get('count_30d', 0)}\n\n"
            "LATEST\n"
            f"{latest_str}"
        )

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "24H", "callback_data": "btn:history:24h"},
                {"text": "7D", "callback_data": "btn:history:7d"},
                {"text": "30D", "callback_data": "btn:history:30d"},
            ],
            [
                {"text": "ALL", "callback_data": "btn:history:all"},
                {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
            ],
        ]
    }
    return text, keyboard


async def build_status_view(
    signal_engine: Any,
    ws_manager: Any,
    database: Any,
    binance_client: Any,
    alert_queue: Any,
    telegram_notifier: Any,
) -> Tuple[str, Dict[str, Any]]:
    """Builds /status technical system health screen."""
    engine_status = "RUNNING" if signal_engine else "OFFLINE"
    ws_status = "CONNECTED" if (ws_manager and ws_manager.is_connected) else "DISCONNECTED"

    db_status = "ONLINE"
    try:
        if database:
            await database.get_total_signals_count()
        else:
            db_status = "OFFLINE"
    except Exception:
        db_status = "OFFLINE"

    aq_status = "RUNNING" if (alert_queue and alert_queue._running) else "STOPPED"

    ws_health = ws_manager.get_market_data_health() if ws_manager else {
        "status": "OFFLINE", "is_healthy": False, "total_klines_received": 0, "candles_closed_count": 0
    }
    market_data_status = ws_health.get("status", "OFFLINE")
    total_klines = ws_health.get("total_klines_received", 0)
    closed_candles = ws_health.get("candles_closed_count", 0)
    last_kline_ts = ws_health.get("last_kline_received_at")
    last_kline_str = datetime.fromtimestamp(last_kline_ts, tz=timezone.utc).strftime("%d %b %H:%M:%S UTC") if last_kline_ts else "NEVER"
    last_closed_ts = ws_health.get("last_closed_candle_time")
    last_closed_str = datetime.fromtimestamp(last_closed_ts / 1000, tz=timezone.utc).strftime("%d %b %H:%M UTC") if last_closed_ts else "NEVER"

    symbols_count = len(signal_engine.symbols) if signal_engine else 0
    connections = len(ws_manager._tasks) if (ws_manager and ws_manager._tasks) else ((symbols_count + 99) // 100 if symbols_count else 0)
    c429 = binance_client.rate_limit_429_count if binance_client else 0
    c418 = binance_client.ip_ban_418_count if binance_client else 0
    reconnects = ws_manager.reconnect_count if ws_manager else 0

    tg_status = "ONLINE" if (telegram_notifier and telegram_notifier.is_configured) else "OFFLINE"

    uptime_str = "00h 00m"
    if signal_engine and hasattr(signal_engine, "start_time"):
        elapsed = int(time.time() - signal_engine.start_time)
        hrs, rem = divmod(elapsed, 3600)
        mins, _ = divmod(rem, 60)
        uptime_str = f"{hrs:02d}h {mins:02d}m"

    text = (
        "NEXORA SYSTEM STATUS\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "CORE\n"
        f"● Engine       {engine_status}\n"
        f"● WebSocket    {ws_status}\n"
        f"● Market Data  {market_data_status}\n"
        f"● Database     {db_status}\n"
        f"● Alert Queue  {aq_status}\n\n"
        "BINANCE\n"
        f"Symbols        {symbols_count}\n"
        f"Connections    {connections}\n"
        f"Total Klines   {total_klines}\n"
        f"Last Kline     {last_kline_str}\n"
        f"Closed Candles {closed_candles}\n"
        f"Last Closed 1H {last_closed_str}\n"
        f"429 Errors     {c429}\n"
        f"418 Errors     {c418}\n"
        f"Reconnects     {reconnects}\n\n"
        "TELEGRAM\n"
        f"● {tg_status}\n\n"
        "UPTIME\n"
        f"{uptime_str}"
    )

    keyboard = {
        "inline_keyboard": [
            [
                {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
                {"text": "REFRESH", "callback_data": "btn:status"},
            ]
        ]
    }
    return text, keyboard


def build_help_view() -> Tuple[str, Dict[str, Any]]:
    """Builds /help command guide."""
    text = (
        "NEXORA COMMANDS\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "/start\n"
        "Launch NEXORA\n\n"
        "/dashboard\n"
        "System overview\n\n"
        "/market\n"
        "Live market scanner\n\n"
        "/alerts\n"
        "Recent Golden Cross alerts\n\n"
        "/symbol BTCUSDT\n"
        "Inspect a symbol\n\n"
        "/history\n"
        "Historical signal data\n\n"
        "/status\n"
        "Technical system status\n\n"
        "/help\n"
        "Command guide"
    )
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "DASHBOARD", "callback_data": "btn:dashboard"},
                {"text": "ALERTS", "callback_data": "btn:alerts:1"},
            ]
        ]
    }
    return text, keyboard
