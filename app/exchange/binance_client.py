"""Binance USD-M Futures REST client with 429/418 rate-limit handling and backoff."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
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
        parsed = urlparse(self.base_url)
        if parsed.scheme != "https" or parsed.hostname != "fapi.binance.com":
            raise ValueError("NEXORA REST market data must use Binance USD-M Futures (fapi.binance.com)")
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
        if interval != "1h":
            raise ValueError("NEXORA only requests Binance USD-M Futures 1h candles")
        symbol = self.resolve_symbol(symbol)
        if time.time() < self._pause_until:
            wait_time = self._pause_until - time.time()
            logger.warning(f"REST requests paused due to rate-limit. Waiting {wait_time:.1f}s...")
            return []

        req_limit = min(limit + (2 if only_closed else 0), 1000)
        url = f"{self.base_url}/fapi/v1/klines"
        params: Dict[str, Any] = {"symbol": symbol.upper(), "interval": interval, "limit": req_limit}
        if end_time is not None:
            params["endTime"] = end_time
        if start_time is not None:
            params["startTime"] = start_time

        session = await self._get_session()
        try:
            async def fetch_page(page_params: Dict[str, Any]) -> Optional[List[Any]]:
                async with session.get(url, params=page_params) as resp:
                    if not await self._check_rate_limit(resp):
                        return None
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(f"Failed to fetch klines for {symbol}: HTTP {resp.status} - {text}")
                        return None
                    raw_data = await resp.json()
                    return raw_data if isinstance(raw_data, list) else None

            raw_data = await fetch_page(params)
            if raw_data is None:
                return []

            def parse_candles(rows: List[Any]) -> List[Dict[str, Any]]:
                parsed: List[Dict[str, Any]] = []
                for item in rows:
                    parsed.append({
                        "timestamp": int(item[0]),
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5]),
                        "close_time": int(item[6]),
                    })
                return parsed

            candles = parse_candles(raw_data)
            if not only_closed:
                return candles[-limit:]

            # Filter to closed candles only and collect into a timestamp-keyed
            # dict for automatic deduplication.
            now_ms = int(time.time() * 1000)
            by_timestamp: Dict[int, Dict[str, Any]] = {
                int(c["timestamp"]): c
                for c in candles
                if int(c["close_time"]) < now_ms
            }

            # Paginate backward to fill the deficit.  The initial Binance
            # response is capped at 1000 rows.  When it contains the current
            # open candle we lose one closed row, but the real issue is that
            # 1000 rows is the hard Binance cap -- if a caller needs 1000
            # *closed* candles the open candle eats one slot.  We paginate
            # backward (using endTime) to collect the shortfall.
            #
            # Stop conditions:
            #   - collected >= limit closed candles
            #   - explicit start_time was provided (no backfill across caller bounds)
            #   - last page returned < 1000 rows (Binance history exhausted)
            #   - safety cap of MAX_BACKFILL_PAGES reached
            #
            # Rate-limit handling:
            #   Each backfill page is retried up to MAX_RETRIES_PER_PAGE times
            #   if a 429/418 is received.  Before each retry we sleep for the
            #   Retry-After period (capped at MAX_RATE_LIMIT_WAIT_S).  If all
            #   retries are exhausted the loop breaks and returns partial
            #   results (fail-closed: SignalEngine rejects <1000).
            MAX_BACKFILL_PAGES = 3
            MAX_RETRIES_PER_PAGE = 2
            MAX_RATE_LIMIT_WAIT_S = 30
            last_page_size = len(raw_data)

            for _ in range(MAX_BACKFILL_PAGES):
                if len(by_timestamp) >= limit:
                    break
                if start_time is not None:
                    break
                if last_page_size < 1000:
                    # Binance returned fewer than 1000 rows -- no older data.
                    break

                earliest_timestamp = min(by_timestamp) if by_timestamp else (
                    min(int(c["timestamp"]) for c in candles) if candles else None
                )
                if earliest_timestamp is None:
                    break

                backfill_params = {
                    "symbol": symbol.upper(),
                    "interval": interval,
                    "limit": 1000,
                    "endTime": earliest_timestamp - 1,
                }

                # Fetch with bounded rate-limit retry.
                backfill_raw = None
                for _attempt in range(MAX_RETRIES_PER_PAGE):
                    pause_remaining = self._pause_until - time.time()
                    if pause_remaining > 0:
                        if pause_remaining > MAX_RATE_LIMIT_WAIT_S:
                            logger.warning(
                                f"Backfill pause {pause_remaining:.0f}s exceeds "
                                f"{MAX_RATE_LIMIT_WAIT_S}s cap for {symbol} — giving up"
                            )
                            break
                        await asyncio.sleep(pause_remaining)
                    backfill_raw = await fetch_page(backfill_params)
                    if backfill_raw is not None:
                        break
                    # fetch_page returned None → rate-limited or HTTP error.
                    # _check_rate_limit already updated _pause_until with
                    # Retry-After; the next iteration will honour the wait.

                if backfill_raw is None:
                    # All retries exhausted — return what we have (fail-closed).
                    break
                last_page_size = len(backfill_raw)
                if last_page_size == 0:
                    break
                for candle in parse_candles(backfill_raw):
                    if int(candle["close_time"]) < now_ms:
                        by_timestamp.setdefault(int(candle["timestamp"]), candle)

            return sorted(by_timestamp.values(), key=lambda c: int(c["timestamp"]))[-limit:]
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
