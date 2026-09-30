"""Comprehensive test suite for NEXORA EMA CROSS Telegram Command System."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from app.charts.chart_data import ChartDataProvider
from app.charts.chart_renderer import ChartRenderer
from app.engine.alert_queue import AlertQueue
from app.engine.signal_engine import SignalEngine
from app.exchange.binance_client import BinanceFuturesClient
from app.exchange.websocket_manager import BinanceWebSocketManager
from app.notifications.telegram_bot import TelegramNotifier
from app.notifications.telegram_commands import COMMANDS_MENU
from app.persistence.database import Database
from app.persistence.models import SignalRecord


async def get_test_env(tmp_path):
    """Sets up a realistic test environment with database, engine, and Telegram notifier."""
    db_path = str(tmp_path / "test_tg.db")
    db = Database(db_path=db_path)
    await db.init()

    # Pre-populate database with some historical and live signals
    now_ms = int(time.time() * 1000)
    for i in range(15):
        is_live = (i < 3)
        ts = now_ms - (i * 3600 * 1000 * 2)  # spaced out
        rec = SignalRecord(
            id=None,
            symbol="BTCUSDT" if i % 2 == 0 else "ETHUSDT",
            timeframe="1H",
            candle_timestamp=ts,
            signal_time_utc=f"30 Sep 2026 • {12 - i:02d}:00 UTC",
            ema50=100.0 + i,
            ema200=98.0 + i,
            close_price=101.0 + i,
            is_live=is_live,
        )
        await db.save_signal(rec)

    client = BinanceFuturesClient()
    client.get_klines = AsyncMock(return_value=[
        {"timestamp": now_ms - 3 * 3600_000, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 8.0},
        {"timestamp": now_ms - 2 * 3600_000, "open": 100.0, "high": 106.0, "low": 99.0, "close": 105.0, "volume": 10.0},
        {"timestamp": now_ms - 3600_000, "open": 105.0, "high": 111.0, "low": 104.0, "close": 110.0, "volume": 12.0},
    ])
    ws_manager = BinanceWebSocketManager(timeframe="1h")
    ws_manager.set_symbols(["BTCUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT"])
    ws_manager._active_connections = 1

    chart_data = ChartDataProvider(client, db)
    renderer = ChartRenderer(output_dir=str(tmp_path / "charts"))

    notifier = TelegramNotifier(
        bot_token="TEST_BOT_TOKEN_12345",
        chat_id="999888777",
        enabled=True,
    )

    alert_queue = AlertQueue(chart_data, renderer, notifier, db)
    alert_queue.start()

    engine = SignalEngine(client, db, alert_queue)
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT"]
    engine.set_symbols(symbols)
    engine.initialized_symbols.add("BTCUSDT")
    engine.initialized_symbols.add("ETHUSDT")

    # Add mock candle history for BTCUSDT
    engine.candles_history["BTCUSDT"] = [
        {"timestamp": now_ms - 3600000, "close": 105.0, "ema_50": 102.0, "ema_200": 100.0},
        {"timestamp": now_ms, "close": 110.0, "ema_50": 104.5, "ema_200": 101.2},
    ]

    notifier.set_services(
        signal_engine=engine,
        ws_manager=ws_manager,
        database=db,
        binance_client=client,
        alert_queue=alert_queue,
        chart_data_provider=chart_data,
        chart_renderer=renderer,
    )

    return {
        "db": db,
        "client": client,
        "ws_manager": ws_manager,
        "engine": engine,
        "alert_queue": alert_queue,
        "notifier": notifier,
        "chart_data": chart_data,
        "renderer": renderer,
        "chat_id": "999888777",
    }


@pytest.mark.asyncio
async def test_start_command(tmp_path):
    """Test A: /start returns professional welcome screen with inline navigation."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, kb = await notifier.handle_command("/start", chat_id=chat_id)

    assert "NEXORA EMA CROSS" in text
    assert "Real-time Binance Futures" in text
    assert "TIMEFRAME   1H" in text
    assert "SIGNAL      LONG ONLY" in text
    assert "● Monitoring active" in text
    assert "Use /dashboard to view system status" in text

    # Verify buttons
    buttons = [btn["text"] for row in kb["inline_keyboard"] for btn in row]
    assert "DASHBOARD" in buttons
    assert "ALERTS" in buttons
    assert "MARKET" in buttons


@pytest.mark.asyncio
async def test_dashboard_command(tmp_path):
    """Test B: /dashboard displays live system state without hardcoded values."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, kb = await notifier.handle_command("/dashboard", chat_id=chat_id)

    assert "NEXORA DASHBOARD" in text
    assert "● Binance        ONLINE" in text
    assert "● WebSocket      CONNECTED" in text
    assert "● Telegram       ONLINE" in text
    assert "● Database       ONLINE" in text
    assert "Symbols          4" in text
    assert "Timeframe        1H" in text
    assert "Strategy         EMA 50 / EMA 200" in text
    assert "Historical" in text
    assert "Live Today" in text
    assert "UPTIME" in text
    assert "Updated" in text


@pytest.mark.asyncio
async def test_market_command(tmp_path):
    """Test C: /market displays live market and WebSocket connection status."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, kb = await notifier.handle_command("/market", chat_id=chat_id)

    assert "MARKET MONITOR" in text
    assert "4 USDT Perpetuals" in text
    assert "● DATA STALE / NO MARKET DATA" in text
    assert "WebSocket" in text
    assert "connections" in text
    assert "Timeframe\n1H" in text
    assert "Candle Status\nWAITING FOR CLOSE" in text


@pytest.mark.asyncio
async def test_alerts_command(tmp_path):
    """Test D: /alerts retrieves real signals from database."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, kb = await notifier.handle_command("/alerts", chat_id=chat_id)

    assert "RECENT GOLDEN CROSSES" in text
    assert "BTCUSDT" in text or "ETHUSDT" in text
    # Verify pagination button exists
    buttons = [btn["text"] for row in kb["inline_keyboard"] for btn in row]
    assert "DASHBOARD" in buttons
    assert any("/" in b for b in buttons)


@pytest.mark.asyncio
async def test_symbol_command_valid(tmp_path):
    """Test E: /symbol BTCUSDT inspects real in-memory EMA and DB state."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, kb = await notifier.handle_command("/symbol BTCUSDT", chat_id=chat_id)

    assert "BTCUSDT" in text
    assert "● MONITORING" in text
    assert "TIMEFRAME\n1H" in text
    assert "EMA STRUCTURE" in text
    assert "EMA 50" in text and "EMA 200" in text
    assert "SIGNAL\nBULLISH" in text
    assert "LAST GOLDEN CROSS" in text

    buttons = [btn["text"] for row in kb["inline_keyboard"] for btn in row]
    assert "VIEW CHART" in buttons


@pytest.mark.asyncio
async def test_symbol_lowercase_and_alias_normalization(tmp_path):
    """Test F: Lowercase symbols and Binance aliases normalize cleanly."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    # Lowercase BTCUSDT
    text_btc, _ = await notifier.handle_command("/symbol btcusdt", chat_id=chat_id)
    assert "BTCUSDT" in text_btc

    # Alias PEPEUSDT -> 1000PEPEUSDT
    text_pepe, _ = await notifier.handle_command("/symbol pepeusdt", chat_id=chat_id)
    assert "1000PEPEUSDT" in text_pepe
    assert "TIMEFRAME\n1H" in text_pepe


@pytest.mark.asyncio
async def test_symbol_invalid_handled_safely(tmp_path):
    """Test G: Invalid symbol handled safely with clean message and no stack trace."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, _ = await notifier.handle_command("/symbol FAKESYMBOLXYZ", chat_id=chat_id)

    assert "SYMBOL NOT FOUND" in text
    assert "FAKESYMBOLXYZ is not currently available" in text
    assert "Traceback" not in text


@pytest.mark.asyncio
async def test_symbol_without_argument(tmp_path):
    """Test H: /symbol without argument prompts with usage without crashing."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, _ = await notifier.handle_command("/symbol", chat_id=chat_id)

    assert "Usage:\n/symbol BTCUSDT" in text


@pytest.mark.asyncio
async def test_history_command_and_filters(tmp_path):
    """Test I: /history displays database aggregated metrics and supports 24H/7D/30D."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    # Overall history
    text, kb = await notifier.handle_command("/history", chat_id=chat_id)
    assert "SIGNAL HISTORY" in text
    assert "TOTAL\n15" in text
    assert "LAST 24H" in text
    assert "LAST 7D" in text
    assert "LAST 30D" in text
    assert "LATEST" in text

    # Filtered 24H
    text_24, _ = await notifier.handle_command("/history 24h", chat_id=chat_id)
    assert "SIGNAL HISTORY (24H)" in text_24
    assert "COUNT:" in text_24


@pytest.mark.asyncio
async def test_status_command(tmp_path):
    """Test J: /status technical health screen displays live subsystem metrics."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, _ = await notifier.handle_command("/status", chat_id=chat_id)

    assert "NEXORA SYSTEM STATUS" in text
    assert "CORE" in text
    assert "● Engine       RUNNING" in text
    assert "● WebSocket    CONNECTED" in text
    assert "● Database     ONLINE" in text
    assert "● Alert Queue  RUNNING" in text
    assert "BINANCE" in text
    assert "Symbols        4" in text
    assert "429 Errors     0" in text
    assert "418 Errors     0" in text
    assert "Reconnects     0" in text
    assert "TELEGRAM" in text
    assert "UPTIME" in text


@pytest.mark.asyncio
async def test_help_command(tmp_path):
    """Test K: /help lists all standard commands concisely."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    text, _ = await notifier.handle_command("/help", chat_id=chat_id)

    assert "NEXORA COMMANDS" in text
    assert "/start" in text
    assert "/dashboard" in text
    assert "/market" in text
    assert "/alerts" in text
    assert "/symbol BTCUSDT" in text
    assert "/history" in text
    assert "/status" in text
    assert "/help" in text


@pytest.mark.asyncio
async def test_command_registration(tmp_path):
    """Test L: Telegram command registration calls setMyCommands with exact menu."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]

    with patch.object(notifier, "_get_session") as mock_get_sess:
        mock_session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post.return_value = mock_cm
        mock_get_sess.return_value = mock_session

        res = await notifier.register_commands()
        assert res is True
        assert notifier._commands_registered is True
        # Check payload
        call_args = mock_session.post.call_args
        assert call_args[1]["json"]["commands"] == COMMANDS_MENU


@pytest.mark.asyncio
async def test_inline_buttons_handling(tmp_path):
    """Test M: Inline buttons trigger in-place message edits without spam."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    with patch.object(notifier, "edit_message", new_callable=AsyncMock) as mock_edit:
        mock_edit.return_value = True

        # Click Dashboard button
        await notifier.handle_callback("btn:dashboard", chat_id=chat_id, message_id=101)
        mock_edit.assert_called_once()
        args = mock_edit.call_args[0]
        assert args[0] == chat_id
        assert args[1] == 101
        assert "NEXORA DASHBOARD" in args[2]

        mock_edit.reset_mock()
        # Click Market button
        await notifier.handle_callback("btn:market", chat_id=chat_id, message_id=102)
        mock_edit.assert_called_once()
        assert "MARKET MONITOR" in mock_edit.call_args[0][2]


@pytest.mark.asyncio
async def test_alerts_pagination(tmp_path):
    """Test N: Pagination in /alerts updates to next/previous pages."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    # Page 1
    text_p1, kb_p1 = await notifier.handle_command("/alerts 1", chat_id=chat_id)
    # Page 2
    text_p2, kb_p2 = await notifier.handle_command("/alerts 2", chat_id=chat_id)

    assert "RECENT GOLDEN CROSSES" in text_p1
    assert "RECENT GOLDEN CROSSES" in text_p2
    # Verify different signals appear on page 1 vs page 2
    assert text_p1 != text_p2


@pytest.mark.asyncio
async def test_unauthorized_access_restriction(tmp_path):
    """Test O: Unauthorized chat IDs are blocked with clean restricted notice and zero data."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    unauthorized_chat_id = "111222333"  # Not 999888777

    with patch.object(notifier, "send_message", new_callable=AsyncMock) as mock_send:
        # Simulate update from unauthorized chat
        update = {
            "message": {
                "chat": {"id": unauthorized_chat_id},
                "text": "/dashboard",
            }
        }
        await notifier.handle_update(update)

        mock_send.assert_called_once()
        sent_text = mock_send.call_args[1]["text"]
        assert "Access Restricted" in sent_text
        assert "NEXORA DASHBOARD" not in sent_text
        assert "TEST_BOT_TOKEN" not in sent_text


@pytest.mark.asyncio
async def test_telegram_exceptions_do_not_crash_engine(tmp_path):
    """Test P: Exceptions during command execution are caught and return user-friendly error."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]

    # Simulate a database failure during /dashboard
    with patch.object(env["db"], "get_historical_signals_count", side_effect=RuntimeError("Database lock failed")):
        text, _ = await notifier.handle_command("/dashboard", chat_id=chat_id)
        # Should gracefully return error screen without crashing
        assert "Unable to retrieve system status" in text
        assert "RuntimeError" not in text
        assert "Database lock failed" not in text


@pytest.mark.asyncio
async def test_no_secrets_appear_in_responses(tmp_path):
    """Test Q: No bot tokens, credentials, or file paths appear in Telegram views."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]
    token = env["notifier"].bot_token

    commands = ["/start", "/dashboard", "/market", "/alerts", "/symbol BTCUSDT", "/history", "/status", "/help"]
    for cmd in commands:
        text, _ = await notifier.handle_command(cmd, chat_id=chat_id)
        assert token not in text, f"Secret token leaked in response for {cmd}!"
        assert "sqlite" not in text.lower()
        assert "c:\\" not in text.lower()
        assert "/home" not in text.lower()


@pytest.mark.asyncio
async def test_commands_do_not_enqueue_alerts_or_create_signals(tmp_path):
    """Test R, S, T: Running commands does NOT enqueue alerts, insert duplicate DB rows, or alter strategy."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    chat_id = env["chat_id"]
    queue = env["alert_queue"]
    db = env["db"]

    init_qsize = queue._queue.qsize()
    init_db_count = await db.get_total_signals_count()

    commands = ["/start", "/dashboard", "/market", "/alerts", "/symbol BTCUSDT", "/history", "/status", "/help"]
    for cmd in commands:
        await notifier.handle_command(cmd, chat_id=chat_id)

    # Queue depth must remain 0
    assert queue._queue.qsize() == init_qsize
    # DB row count must remain unchanged
    final_db_count = await db.get_total_signals_count()
    assert final_db_count == init_db_count


@pytest.mark.asyncio
async def test_live_alert_pipeline_remains_functional(tmp_path):
    """Test U: TelegramNotifier.send_golden_cross_alert still delivers live alerts perfectly."""
    env = await get_test_env(tmp_path)
    notifier = env["notifier"]
    from app.indicators.ema import GoldenCrossSignal

    signal = GoldenCrossSignal(
        symbol="BTCUSDT",
        timeframe="1H",
        candle_timestamp=1790000000000,
        signal_time_utc="30 Sep 2026 • 15:00 UTC",
        ema50=104.50,
        ema200=101.20,
        close_price=105.00,
        previous_ema50=100.90,
        previous_ema200=101.00,
        candle_status="CLOSED",
    )

    with patch.object(notifier, "_get_session") as mock_sess:
        mock_session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"result": {"message_id": 1}})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session.post.return_value = mock_cm
        mock_sess.return_value = mock_session

        res = await notifier.send_golden_cross_alert(signal)
        assert res is True
        # Verify alert formatting
        payload = mock_session.post.call_args[1]["json"]
        assert "🟢 GOLDEN CROSS" in payload["text"]
        assert "BTCUSDT" in payload["text"]
        assert "TIMEFRAME: 1H" in payload["text"]
