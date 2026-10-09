"""NEXORA Telegram Notification & Interactive Command System."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple
import aiohttp

from app.charts.chart_theme import format_price
from app.indicators.ema import GoldenCrossSignal
from app.notifications.telegram_commands import (
    COMMANDS_MENU,
    build_start_view,
    build_dashboard_view,
    build_market_view,
    build_alerts_view,
    build_symbol_view,
    build_history_view,
    build_status_view,
    build_help_view,
)

logger = logging.getLogger("nexora.telegram")


class TelegramNotifier:
    def __init__(
        self,
        bot_token: str = "",
        chat_id: str = "",
        enabled: bool = False,
        signal_engine: Optional[Any] = None,
        ws_manager: Optional[Any] = None,
        database: Optional[Any] = None,
        binance_client: Optional[Any] = None,
        alert_queue: Optional[Any] = None,
        chart_data_provider: Optional[Any] = None,
        chart_renderer: Optional[Any] = None,
        fast_period: int = 50,
        slow_period: int = 200,
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.enabled = enabled and bool(self.bot_token) and bool(self.chat_id)
        self.fast_period = fast_period
        self.slow_period = slow_period
        self._session: Optional[aiohttp.ClientSession] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._commands_registered: bool = False
        self._last_update_id: int = 0
        self._running: bool = False

        # Alert reliability and telemetry metrics
        self.alerts_attempted: int = 0
        self.alerts_successful: int = 0
        self.alerts_failed: int = 0
        self.last_api_status: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_successful_send_at: Optional[float] = None

        # Service references for single source of truth
        self.signal_engine = signal_engine
        self.ws_manager = ws_manager
        self.database = database
        self.binance_client = binance_client
        self.alert_queue = alert_queue
        self.chart_data_provider = chart_data_provider
        self.chart_renderer = chart_renderer

    def set_services(
        self,
        signal_engine: Optional[Any] = None,
        ws_manager: Optional[Any] = None,
        database: Optional[Any] = None,
        binance_client: Optional[Any] = None,
        alert_queue: Optional[Any] = None,
        chart_data_provider: Optional[Any] = None,
        chart_renderer: Optional[Any] = None,
        fast_period: Optional[int] = None,
        slow_period: Optional[int] = None,
    ) -> None:
        """Inject runtime services for dynamic state inspection."""
        if signal_engine is not None:
            self.signal_engine = signal_engine
            if hasattr(signal_engine, "fast_period"):
                self.fast_period = signal_engine.fast_period
            if hasattr(signal_engine, "slow_period"):
                self.slow_period = signal_engine.slow_period
        if ws_manager is not None:
            self.ws_manager = ws_manager
        if database is not None:
            self.database = database
        if binance_client is not None:
            self.binance_client = binance_client
        if alert_queue is not None:
            self.alert_queue = alert_queue
        if chart_data_provider is not None:
            self.chart_data_provider = chart_data_provider
        if chart_renderer is not None:
            self.chart_renderer = chart_renderer
        if fast_period is not None:
            self.fast_period = fast_period
        if slow_period is not None:
            self.slow_period = slow_period

    @property
    def is_configured(self) -> bool:
        return self.enabled and bool(self.bot_token) and bool(self.chat_id)

    @property
    def status(self) -> str:
        if not self.is_configured:
            return "TELEGRAM CONFIGURATION MISSING"
        if self.alerts_failed > 0 and self.alerts_successful == 0:
            return f"DEGRADED ({self.last_api_status or 'ERROR'})"
        return "ONLINE"

    def is_authorized(self, chat_id: Any) -> bool:
        """Verify chat authorization against configured chat ID."""
        if not self.chat_id:
            return True
        return str(chat_id).strip() == str(self.chat_id).strip()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=35))
        return self._session

    async def start(self) -> None:
        """Starts Telegram command registration and polling listener."""
        if not self.enabled:
            logger.info("Telegram command listener is disabled (running in simulated mode).")
            return

        self._running = True
        # 1. Register command menu once
        if not self._commands_registered:
            await self.register_commands()

        # 2. Start listener loop
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._command_listener_loop())
            logger.info("Telegram command listener started.")

    async def close(self) -> None:
        """Stops listener and cleans up network sessions."""
        self._running = False
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None

        if self._session and not self._session.closed:
            await self._session.close()

    async def register_commands(self) -> bool:
        """Registers the standard command menu with Telegram Bot API."""
        if not self.bot_token:
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/setMyCommands"
        session = await self._get_session()
        try:
            payload = {"commands": COMMANDS_MENU}
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    self._commands_registered = True
                    logger.info("Telegram command menu registered successfully.")
                    return True
                else:
                    err = await resp.text()
                    logger.warning(f"Failed to register Telegram commands: HTTP {resp.status} - {err}")
                    return False
        except Exception as e:
            logger.error(f"Error registering Telegram command menu: {e}")
            return False

    async def verify_connectivity(self) -> Dict[str, Any]:
        """Safely verify Telegram connectivity using getMe without sending any messages."""
        if not self.bot_token:
            return {
                "configured": False,
                "connected": False,
                "status": "TELEGRAM CONFIGURATION MISSING",
                "reason": "TELEGRAM_BOT_TOKEN is empty or not set",
            }
        if not self.chat_id:
            return {
                "configured": False,
                "connected": False,
                "status": "TELEGRAM CONFIGURATION MISSING",
                "reason": "TELEGRAM_CHAT_ID is empty or not set",
            }

        url = f"https://api.telegram.org/bot{self.bot_token}/getMe"
        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    bot_user = data.get("result", {}).get("username", "UnknownBot")
                    return {
                        "configured": True,
                        "connected": True,
                        "status": "ONLINE",
                        "bot_username": bot_user,
                    }
                else:
                    return {
                        "configured": True,
                        "connected": False,
                        "status": "CONNECTION_FAILED",
                        "http_status": resp.status,
                    }
        except Exception as e:
            return {
                "configured": True,
                "connected": False,
                "status": "NETWORK_ERROR",
                "error": str(e),
            }

    def format_alert_message(self, signal: GoldenCrossSignal) -> str:
        """Formats the official Golden Cross alert message matching production specifications."""
        fast_p = getattr(signal, "fast_period", getattr(self, "fast_period", 50))
        slow_p = getattr(signal, "slow_period", getattr(self, "slow_period", 200))
        fast_val = getattr(signal, "ema_fast", getattr(signal, "ema50", 0.0))
        slow_val = getattr(signal, "ema_slow", getattr(signal, "ema200", 0.0))
        return (
            "NEXORA EMA CROSS\n"
            "🟢 GOLDEN CROSS\n\n"
            f"SYMBOL: {signal.symbol}\n"
            "TIMEFRAME: 1H\n"
            f"EMA{fast_p}: {format_price(fast_val)}\n"
            f"EMA{slow_p}: {format_price(slow_val)}\n"
            f"CANDLE: {signal.candle_status}\n"
            f"TIME: {signal.signal_time_utc}"
        )

    async def send_golden_cross_alert(
        self,
        signal: GoldenCrossSignal,
        chart_image_path: Optional[str] = None,
    ) -> bool:
        """Sends the Golden Cross message and optional chart image."""
        self.alerts_attempted += 1
        message_text = self.format_alert_message(signal)

        if not self.enabled:
            logger.info(
                f"[TELEGRAM SIMULATED ALERT - {signal.symbol}]:\n{message_text}\n"
                f"[Attached Chart]: {chart_image_path or 'None'}"
            )
            self.alerts_successful += 1
            self.last_api_status = "SIMULATED"
            self.last_successful_send_at = time.time()
            return True

        session = await self._get_session()

        # If chart image exists and is readable, send as sendPhoto
        if chart_image_path and os.path.isfile(chart_image_path):
            url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
            try:
                with open(chart_image_path, "rb") as photo_file:
                    form_data = aiohttp.FormData()
                    form_data.add_field("chat_id", self.chat_id)
                    form_data.add_field("caption", message_text)
                    form_data.add_field(
                        "photo",
                        photo_file.read(),
                        filename=os.path.basename(chart_image_path),
                        content_type="image/png",
                    )
                    async with session.post(url, data=form_data) as resp:
                        if resp.status == 200:
                            logger.info(f"Telegram photo alert sent for {signal.symbol}")
                            self.alerts_successful += 1
                            self.last_api_status = "OK"
                            self.last_successful_send_at = time.time()
                            return True
                        elif resp.status == 429:
                            await self._handle_rate_limit(resp)
                            self.alerts_failed += 1
                            self.last_api_status = "RATE_LIMITED_429"
                        else:
                            resp_text = await resp.text()
                            logger.error(f"Failed to send Telegram photo: HTTP {resp.status} - {resp_text}")
                            self.alerts_failed += 1
                            self.last_api_status = f"HTTP_{resp.status}"
            except Exception as e:
                logger.error(f"Error sending Telegram photo alert: {e}")
                self.alerts_failed += 1
                self.last_api_status = type(e).__name__

        # Fallback to text message
        fallback_ok = await self.send_text_message(message_text)
        if fallback_ok:
            self.alerts_successful += 1
            self.last_api_status = "OK"
            self.last_successful_send_at = time.time()
            return True
        else:
            self.alerts_failed += 1
            return False

    async def send_text_message(self, text: str) -> bool:
        """Legacy text message sender."""
        res = await self.send_message(self.chat_id, text)
        return res is not None

    async def send_message(
        self,
        chat_id: str,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        """Sends a text message with optional inline keyboard. Returns message_id."""
        if not self.enabled:
            logger.info(f"[TELEGRAM SIMULATED SEND to {chat_id}]:\n{text}")
            return 1

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        session = await self._get_session()
        payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("result", {}).get("message_id")
                elif resp.status == 429:
                    await self._handle_rate_limit(resp)
                    return None
                else:
                    err = await resp.text()
                    logger.error(f"Telegram sendMessage error: HTTP {resp.status} - {err}")
                    return None
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return None

    async def edit_message(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Edits an existing text message and keyboard in place."""
        if not self.enabled:
            logger.info(f"[TELEGRAM SIMULATED EDIT {message_id}]:\n{text}")
            return True

        url = f"https://api.telegram.org/bot{self.bot_token}/editMessageText"
        session = await self._get_session()
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup

        try:
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    return True
                elif resp.status == 429:
                    await self._handle_rate_limit(resp)
                    return False
                else:
                    err = await resp.text()
                    if "message is not modified" in err.lower():
                        return True
                    logger.warning(f"Telegram editMessage error: HTTP {resp.status} - {err}")
                    return False
        except Exception as e:
            logger.error(f"Error editing Telegram message: {e}")
            return False

    async def answer_callback_query(self, callback_query_id: str, text: Optional[str] = None) -> bool:
        """Acknowledges inline button click."""
        if not self.enabled:
            return True

        url = f"https://api.telegram.org/bot{self.bot_token}/answerCallbackQuery"
        session = await self._get_session()
        payload: Dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text

        try:
            async with session.post(url, json=payload) as resp:
                return resp.status == 200
        except Exception as e:
            logger.warning(f"Error answering callback query: {e}")
            return False

    async def send_photo(
        self,
        chat_id: str,
        photo_path: str,
        caption: Optional[str] = None,
    ) -> bool:
        """Sends photo to chat."""
        if not self.enabled:
            logger.info(f"[TELEGRAM SIMULATED PHOTO {photo_path} to {chat_id}] Caption: {caption}")
            return True

        if not os.path.isfile(photo_path):
            return False

        url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
        session = await self._get_session()
        try:
            with open(photo_path, "rb") as photo_file:
                form_data = aiohttp.FormData()
                form_data.add_field("chat_id", str(chat_id))
                if caption:
                    form_data.add_field("caption", caption)
                form_data.add_field(
                    "photo",
                    photo_file.read(),
                    filename=os.path.basename(photo_path),
                    content_type="image/png",
                )
                async with session.post(url, data=form_data) as resp:
                    if resp.status == 200:
                        return True
                    elif resp.status == 429:
                        await self._handle_rate_limit(resp)
                        return False
                    else:
                        err = await resp.text()
                        logger.error(f"sendPhoto error: HTTP {resp.status} - {err}")
                        return False
        except Exception as e:
            logger.error(f"Error sending photo to Telegram: {e}")
            return False

    async def _handle_rate_limit(self, resp: aiohttp.ClientResponse) -> None:
        """Respects Telegram rate-limits safely."""
        retry_sec = 3.0
        try:
            data = await resp.json()
            retry_sec = float(data.get("parameters", {}).get("retry_after", 3.0))
        except Exception:
            retry_after_hdr = resp.headers.get("Retry-After")
            if retry_after_hdr and retry_after_hdr.isdigit():
                retry_sec = float(retry_after_hdr)

        safe_wait = min(retry_sec, 5.0)
        logger.warning(f"Telegram API rate-limited (HTTP 429). Backing off for {safe_wait}s...")
        await asyncio.sleep(safe_wait)

    async def _command_listener_loop(self) -> None:
        """Background worker polling for incoming Telegram commands and button callbacks."""
        logger.info("Telegram command listener worker running.")
        session = await self._get_session()

        while self._running:
            try:
                url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
                payload = {
                    "offset": self._last_update_id + 1,
                    "timeout": 20,
                    "allowed_updates": ["message", "callback_query"],
                }

                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        updates = data.get("result", [])
                        for update in updates:
                            self._last_update_id = max(self._last_update_id, update.get("update_id", 0))
                            await self.handle_update(update)
                    elif resp.status == 429:
                        await self._handle_rate_limit(resp)
                    else:
                        await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Telegram listener loop: {e}")
                await asyncio.sleep(3.0)

    async def handle_update(self, update: Dict[str, Any]) -> None:
        """Dispatches an incoming update to command or callback handlers."""
        # 1. Message update
        if "message" in update:
            msg = update["message"]
            chat_id = msg.get("chat", {}).get("id")
            text = msg.get("text", "")
            if not chat_id or not text:
                return

            if not self.is_authorized(chat_id):
                logger.warning(f"Unauthorized Telegram access attempt from chat_id={chat_id}")
                await self.send_message(
                    chat_id=str(chat_id),
                    text="NEXORA\n━━━━━━━━━━━━━━━━━━\nAccess Restricted.\nThis bot instance is restricted to authorized chats.",
                )
                return

            reply_text, keyboard = await self.handle_command(text, chat_id=str(chat_id))
            await self.send_message(chat_id=str(chat_id), text=reply_text, reply_markup=keyboard)

        # 2. Callback query update (inline button)
        elif "callback_query" in update:
            cb = update["callback_query"]
            cb_id = cb.get("id")
            chat_id = cb.get("message", {}).get("chat", {}).get("id")
            msg_id = cb.get("message", {}).get("message_id")
            data = cb.get("data", "")

            if cb_id:
                await self.answer_callback_query(cb_id)

            if not chat_id or not msg_id or not data:
                return

            if not self.is_authorized(chat_id):
                logger.warning(f"Unauthorized Telegram callback attempt from chat_id={chat_id}")
                return

            await self.handle_callback(data, chat_id=str(chat_id), message_id=msg_id)

    async def handle_command(self, command_text: str, chat_id: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """Processes commands cleanly and returns view text and inline keyboard."""
        raw = command_text.strip()
        parts = raw.split(maxsplit=1)
        cmd = parts[0].lower() if parts else ""
        arg = parts[1].strip() if len(parts) > 1 else ""

        # Remove bot username suffix if present (e.g., /start@NexoraBot)
        if "@" in cmd:
            cmd = cmd.split("@")[0]

        try:
            if cmd == "/start":
                fast_p = getattr(self.signal_engine, "fast_period", self.fast_period)
                slow_p = getattr(self.signal_engine, "slow_period", self.slow_period)
                return build_start_view(fast_period=fast_p, slow_period=slow_p)

            elif cmd == "/dashboard":
                return await build_dashboard_view(
                    self.signal_engine,
                    self.ws_manager,
                    self.database,
                    self.binance_client,
                    self,
                )

            elif cmd == "/market":
                return await build_market_view(self.signal_engine, self.ws_manager)

            elif cmd == "/alerts":
                page = int(arg) if arg.isdigit() else 1
                return await build_alerts_view(self.database, page=page)

            elif cmd == "/symbol":
                return await build_symbol_view(
                    arg,
                    self.signal_engine,
                    self.binance_client,
                    self.database,
                )

            elif cmd == "/history":
                filter_val = arg.lower() if arg.lower() in ("24h", "7d", "30d") else "all"
                return await build_history_view(self.database, window_filter=filter_val)

            elif cmd == "/status":
                return await build_status_view(
                    self.signal_engine,
                    self.ws_manager,
                    self.database,
                    self.binance_client,
                    self.alert_queue,
                    self,
                )

            elif cmd == "/help":
                return build_help_view()

            else:
                # Default unknown command
                return (
                    "NEXORA\n━━━━━━━━━━━━━━━━━━\n\nUnknown command. Use /help to see available commands.",
                    {"inline_keyboard": [[{"text": "HELP", "callback_data": "btn:help"}, {"text": "DASHBOARD", "callback_data": "btn:dashboard"}]]},
                )
        except Exception as e:
            logger.error(f"Error handling Telegram command '{raw}': {e}", exc_info=True)
            return (
                "NEXORA\n━━━━━━━━━━━━━━━━━━\n\nUnable to retrieve system status.\nPlease try again.",
                {"inline_keyboard": [[{"text": "DASHBOARD", "callback_data": "btn:dashboard"}]]},
            )

    async def handle_callback(self, data: str, chat_id: str, message_id: int) -> None:
        """Handles inline button clicks by editing the existing message in place."""
        try:
            if data == "noop":
                return

            if data == "btn:dashboard":
                text, kb = await build_dashboard_view(
                    self.signal_engine,
                    self.ws_manager,
                    self.database,
                    self.binance_client,
                    self,
                )
                await self.edit_message(chat_id, message_id, text, reply_markup=kb)

            elif data == "btn:market":
                text, kb = await build_market_view(self.signal_engine, self.ws_manager)
                await self.edit_message(chat_id, message_id, text, reply_markup=kb)

            elif data.startswith("btn:alerts:"):
                page_str = data.split(":")[-1]
                page = int(page_str) if page_str.isdigit() else 1
                text, kb = await build_alerts_view(self.database, page=page)
                await self.edit_message(chat_id, message_id, text, reply_markup=kb)

            elif data.startswith("btn:history:"):
                filter_val = data.split(":")[-1]
                text, kb = await build_history_view(self.database, window_filter=filter_val)
                await self.edit_message(chat_id, message_id, text, reply_markup=kb)

            elif data == "btn:status":
                text, kb = await build_status_view(
                    self.signal_engine,
                    self.ws_manager,
                    self.database,
                    self.binance_client,
                    self.alert_queue,
                    self,
                )
                await self.edit_message(chat_id, message_id, text, reply_markup=kb)

            elif data == "btn:help":
                text, kb = build_help_view()
                await self.edit_message(chat_id, message_id, text, reply_markup=kb)

            elif data.startswith("chart:"):
                symbol = data.split(":", 1)[1].upper()
                await self._handle_chart_request(symbol, chat_id)

        except Exception as e:
            logger.error(f"Error handling Telegram callback '{data}': {e}", exc_info=True)

    async def _handle_chart_request(self, symbol: str, chat_id: str) -> None:
        """Safely generates and sends chart for the requested symbol without blocking event loop."""
        if not self.chart_data_provider or not self.chart_renderer:
            await self.send_message(chat_id, "Chart unavailable.")
            return

        try:
            # 1. Fetch chart data
            chart_data = await self.chart_data_provider.get_chart_data(
                symbol=symbol,
                timeframe="1h",
                limit=150,
                force_fresh=True,
            )

            # 2. Render chart image in thread executor to prevent event-loop blocking (Overview Mode: target_timestamp=None)
            loop = asyncio.get_running_loop()
            chart_path = await loop.run_in_executor(
                None,
                self.chart_renderer.render_golden_cross_chart,
                chart_data,
                None,
            )

            if chart_path and os.path.isfile(chart_path):
                latest = chart_data.get("latest", {})
                e_fast = latest.get("ema_fast", latest.get("ema50"))
                e_slow = latest.get("ema_slow", latest.get("ema200"))
                if e_fast is not None and e_slow is not None:
                    if e_fast > e_slow:
                        struct = "BULLISH"
                    elif e_fast < e_slow:
                        struct = "BEARISH"
                    else:
                        struct = "NEUTRAL"
                else:
                    struct = "1H"
                caption = f"{symbol} • 1H Market Overview ({struct})"
                sent = await self.send_photo(chat_id, chart_path, caption=caption)
                if not sent:
                    await self.send_message(chat_id, f"Chart generated for {symbol} but failed to deliver image.")
            else:
                await self.send_message(chat_id, "Chart unavailable.")
        except Exception as e:
            logger.error(f"Error generating chart for symbol {symbol}: {e}")
            await self.send_message(chat_id, "Chart unavailable.")
