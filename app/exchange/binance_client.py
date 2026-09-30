"""Binance USD-M Futures REST client with 429/418 rate-limit handling and backoff."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
import aiohttp

logger = logging.getLogger("nexora.exchange")


class BinanceFuturesClient:
    COMMON_ALIASES = {
        "PEPEUSDT": "1000PEPEUSDT",
        "SHIBUSDT": "1000SHIBUSDT",
        "BONKUSDT": "1000BONKUSDT",
        "FLOKIUSDT": "1000FLOKIUSDT",
        "LUNCUSDT": "1000LUNCUSDT",
        "RATSUSDT": "1000RATSUSDT",
        "SATSUSDT": "1000SATSUSDT",
        "CATUSDT": "1000CATUSDT",
        "MOGUSDT": "1000000MOGUSDT",
        "CHEEMSUSDT": "1000CHEEMSUSDT",
    }

    def __init__(self, base_url: str = "https://fapi.binance.com") -> None:
        self.base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None
        self.rate_limit_429_count = 0
        self.ip_ban_418_count = 0
        self._pause_until = 0.0

    def resolve_symbol(self, symbol: str) -> str:
        s = symbol.upper()
        return self.COMMON_ALIASES.get(s, s)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _check_rate_limit(self, resp: aiohttp.ClientResponse) -> bool:
        """Inspects response for rate limit (429) or IP ban (418) codes and updates backoff."""
        if resp.status == 429:
            self.rate_limit_429_count += 1
            retry_after = resp.headers.get("Retry-After")
            wait_sec = int(retry_after) if retry_after and retry_after.isdigit() else 5
            self._pause_until = time.time() + wait_sec
            logger.warning(f"REST_429_LIMIT: Binance returned 429. Backing off for {wait_sec}s (Total 429s: {self.rate_limit_429_count})")
            return False

        if resp.status == 418:
            self.ip_ban_418_count += 1
            retry_after = resp.headers.get("Retry-After")
            wait_sec = int(retry_after) if retry_after and retry_after.isdigit() else 60
            self._pause_until = time.time() + wait_sec
            logger.error(f"REST_418_BAN: Binance returned 418 IP ban warning! Pausing REST requests for {wait_sec}s (Total 418s: {self.ip_ban_418_count})")
            return False

        return True

    async def get_active_usdt_symbols(self) -> List[str]:
        """Fetch all active USDT perpetual symbols from Binance USD-M Futures."""
        if time.time() < self._pause_until:
            wait_time = self._pause_until - time.time()
            logger.warning(f"REST requests paused due to rate-limit. Waiting {wait_time:.1f}s...")
            return []

        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                if not await self._check_rate_limit(resp):
                    return []
                if resp.status != 200:
                    logger.error(f"Failed to fetch exchangeInfo: HTTP {resp.status}")
                    return []
                data = await resp.json()
                symbols = []
                for s in data.get("symbols", []):
                    if (
                        s.get("status") == "TRADING"
                        and s.get("quoteAsset") == "USDT"
                        and s.get("contractType") == "PERPETUAL"
                    ):
                        symbols.append(s["symbol"])
                return sorted(symbols)
        except Exception as e:
            logger.error(f"Error fetching active symbols: {e}")
            return []

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1h",
        limit: int = 150,
        only_closed: bool = True,
        end_time: Optional[int] = None,
        start_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch historical klines/candlesticks for a symbol from Binance USD-M Futures."""
        symbol = self.resolve_symbol(symbol)
        if time.time() < self._pause_until:
            wait_time = self._pause_until - time.time()
            logger.warning(f"REST requests paused due to rate-limit. Waiting {wait_time:.1f}s...")
            return []

        req_limit = min(limit + (2 if only_closed else 0), 1000)
        url = f"{self.base_url}/fapi/v1/klines"
        params: Dict[str, Any] = {"symbol": symbol.upper(), "interval": interval, "limit": req_limit}
        if end_time:
            params["endTime"] = end_time
        if start_time:
            params["startTime"] = start_time

        session = await self._get_session()
        try:
            async with session.get(url, params=params) as resp:
                if not await self._check_rate_limit(resp):
                    return []
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"Failed to fetch klines for {symbol}: HTTP {resp.status} - {text}")
                    return []
                raw_data = await resp.json()
                if not isinstance(raw_data, list):
                    return []

                candles: List[Dict[str, Any]] = []
                for item in raw_data:
                    open_time = int(item[0])
                    close_time = int(item[6])
                    candle = {
                        "timestamp": open_time,
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                        "close_time": close_time,
                    }
                    candles.append(candle)

                if only_closed and candles:
                    # Drop the active candle if it is still open
                    candles = candles[:-1]

                return candles[-limit:]
        except Exception as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
            return []

    async def check_connectivity(self) -> bool:
        """Check if Binance USD-M Futures REST API is reachable."""
        url = f"{self.base_url}/fapi/v1/ping"
        session = await self._get_session()
        try:
            async with session.get(url) as resp:
                await self._check_rate_limit(resp)
                return resp.status == 200
        except Exception:
            return False
