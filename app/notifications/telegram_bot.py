"""NEXORA Telegram Notification Service."""

from __future__ import annotations

import logging
import os
from typing import Optional
import aiohttp

from app.charts.chart_theme import format_price
from app.indicators.ema import GoldenCrossSignal

logger = logging.getLogger("nexora.telegram")


class TelegramNotifier:
    def __init__(
        self,
        bot_token: str = "",
        chat_id: str = "",
        enabled: bool = False,
    ) -> None:
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.enabled = enabled and bool(self.bot_token) and bool(self.chat_id)
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def is_configured(self) -> bool:
        return self.enabled and bool(self.bot_token) and bool(self.chat_id)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

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
        """Formats the official Golden Cross alert message matching production specifications.
        
        Strictly contains ONLY:
        NEXORA EMA CROSS
        GOLDEN CROSS
        Symbol
        Timeframe 1H
        EMA50
        EMA200
        Candle CLOSED
        UTC timestamp
        """
        return (
            "NEXORA EMA CROSS\n"
            "🟢 GOLDEN CROSS\n\n"
            f"SYMBOL: {signal.symbol}\n"
            "TIMEFRAME: 1H\n"
            f"EMA50: {format_price(signal.ema50)}\n"
            f"EMA200: {format_price(signal.ema200)}\n"
            f"CANDLE: {signal.candle_status}\n"
            f"TIME: {signal.signal_time_utc}"
        )

    async def send_golden_cross_alert(
        self,
        signal: GoldenCrossSignal,
        chart_image_path: Optional[str] = None,
    ) -> bool:
        """Sends the Golden Cross message and optional chart image.

        If chart image is missing or invalid, falls back to text-only message.
        """
        message_text = self.format_alert_message(signal)

        if not self.enabled:
            logger.info(
                f"[TELEGRAM SIMULATED ALERT - {signal.symbol}]:\n{message_text}\n"
                f"[Attached Chart]: {chart_image_path or 'None'}"
            )
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
                            return True
                        else:
                            resp_text = await resp.text()
                            logger.error(f"Failed to send Telegram photo: HTTP {resp.status} - {resp_text}")
            except Exception as e:
                logger.error(f"Error sending Telegram photo alert: {e}")

        # Fallback to text message
        return await self.send_text_message(message_text)

    async def send_text_message(self, text: str) -> bool:
        if not self.enabled:
            logger.info(f"[TELEGRAM SIMULATED TEXT]:\n{text}")
            return True

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        session = await self._get_session()
        try:
            payload = {"chat_id": self.chat_id, "text": text}
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    logger.info("Telegram text message sent successfully.")
                    return True
                else:
                    resp_text = await resp.text()
                    logger.error(f"Telegram sendMessage error: HTTP {resp.status} - {resp_text}")
                    return False
        except Exception as e:
            logger.error(f"Error sending Telegram message: {e}")
            return False
