"""NEXORA Chart Data Provider with local SQLite caching."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional
import pandas as pd

from app.exchange.binance_client import BinanceFuturesClient
from app.indicators.ema import enrich_candles_with_ema, find_all_golden_crosses, GoldenCrossSignal
from app.persistence.database import Database

logger = logging.getLogger("nexora.charts.data")


class ChartDataProvider:
    def __init__(
        self,
        binance_client: BinanceFuturesClient,
        database: Database,
        cache_ttl_seconds: int = 300,
        fast_period: int = 50,
        slow_period: int = 200,
    ) -> None:
        self.binance_client = binance_client
        self.database = database
        self.cache_ttl_seconds = cache_ttl_seconds
        self.fast_period = fast_period
        self.slow_period = slow_period
        # Cache is written for downstream consumers, but current charts always
        # read the canonical closed-candle frame from Futures REST.
        self._last_fetch_time: Dict[str, float] = {}

    async def get_chart_data(
        self,
        symbol: str,
        timeframe: str = "1h",
        limit: int = 150,
        target_timestamp: Optional[int] = None,
        force_fresh: bool = False,
        fast_period: Optional[int] = None,
        slow_period: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Provides full candlestick, EMA fast, EMA slow and Golden Cross marker data.

        Returns data structure ready for both Matplotlib PNG rendering and
        TradingView Lightweight Charts.
        """
        symbol = symbol.upper()
        if timeframe.lower() != "1h":
            raise ValueError("NEXORA charts support the 1h timeframe only")
        timeframe = "1h"
        fp = fast_period or self.fast_period
        sp = slow_period or self.slow_period
        fast_col = f"ema_{fp}"
        slow_col = f"ema_{sp}"
        candles: List[Dict[str, Any]] = []

        # Fetch converged dataset of 1000 closed candles
        fetch_limit = 1000
        if target_timestamp:
            # Fetch 1000 candles ending ~35 candles after target_timestamp for converged EMA warmup
            end_req_time = target_timestamp + (35 * 3600 * 1000)
            candles = await self.binance_client.get_klines(
                symbol, interval=timeframe, limit=fetch_limit, only_closed=True, end_time=end_req_time
            )
        else:
            candles = await self.binance_client.get_klines(symbol, interval=timeframe, limit=fetch_limit, only_closed=True)
            if candles:
                df = enrich_candles_with_ema(candles, fast_period=fp, slow_period=sp)
                candles_to_cache = df.to_dict(orient="records")
                await self.database.cache_candles(
                    candles_to_cache, symbol, timeframe, fast_period=fp, slow_period=sp
                )
                candles = candles_to_cache

        if not candles:
            return {
                "symbol": symbol,
                "timeframe": timeframe.upper(),
                "candles": [],
                "ema50": [],
                "ema200": [],
                "ema_fast": [],
                "ema_slow": [],
                "fast_period": fp,
                "slow_period": sp,
                "cross_markers": [],
                "latest": {},
            }

        # Calculate EMA across the converged 1000-candle dataset
        df = enrich_candles_with_ema(candles, fast_period=fp, slow_period=sp)

        # Retrieve stored signal record for metadata reference without overriding
        # the objectively calculated EMA values from the converged candle dataset.
        target_signal = None
        if target_timestamp:
            target_signal = await self.database.get_signal_for_candle(symbol, timeframe, target_timestamp)

        # Detect all historical Golden Crosses in the dataset
        golden_crosses = find_all_golden_crosses(
            df,
            symbol=symbol,
            timeframe=timeframe,
            fast_col=fast_col,
            slow_col=slow_col,
            fast_period=fp,
            slow_period=sp,
        )

        # Slice data if target_timestamp is specified (centered around target)
        if target_timestamp:
            # Find the row index closest to target_timestamp
            ts_series = df["timestamp"]
            matches = df[ts_series == target_timestamp]
            if not matches.empty:
                idx = matches.index[0]
                start_idx = max(0, idx - 100)
                end_idx = min(len(df), idx + 50)
                df_sliced = df.iloc[start_idx:end_idx].copy()
            else:
                df_sliced = df.tail(limit).copy()
        else:
            df_sliced = df.tail(limit).copy()

        # Build formatted arrays for Lightweight Charts and Renderer
        formatted_candles = []
        formatted_ema_fast = []
        formatted_ema_slow = []

        for _, row in df_sliced.iterrows():
            ts_sec = int(row["timestamp"] // 1000)
            formatted_candles.append({
                "time": ts_sec,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0.0)),
            })
            f_val = float(row[fast_col]) if (fast_col in row and pd.notna(row[fast_col])) else (float(row["ema_50"]) if "ema_50" in row and pd.notna(row["ema_50"]) else None)
            s_val = float(row[slow_col]) if (slow_col in row and pd.notna(row[slow_col])) else (float(row["ema_200"]) if "ema_200" in row and pd.notna(row["ema_200"]) else None)
            if f_val is not None:
                formatted_ema_fast.append({
                    "time": ts_sec,
                    "value": f_val,
                })
            if s_val is not None:
                formatted_ema_slow.append({
                    "time": ts_sec,
                    "value": s_val,
                })

        # Filter cross markers that fall within the sliced window
        sliced_min_ts = df_sliced["timestamp"].min()
        sliced_max_ts = df_sliced["timestamp"].max()

        markers = []
        for gx in golden_crosses:
            if sliced_min_ts <= gx.candle_timestamp <= sliced_max_ts:
                markers.append({
                    "time": int(gx.candle_timestamp // 1000),
                    "timestamp_ms": gx.candle_timestamp,
                    "price": gx.close_price,
                    "ema50": gx.ema50,
                    "ema200": gx.ema200,
                    "ema_fast": gx.ema_fast,
                    "ema_slow": gx.ema_slow,
                    "fast_period": fp,
                    "slow_period": sp,
                    "signal_time_utc": gx.signal_time_utc,
                    "text": "GOLDEN CROSS",
                    "position": "belowBar",
                    "shape": "arrowUp",
                    "color": "#00E5FF",
                })

        last_row = df_sliced.iloc[-1]
        last_fast = float(last_row[fast_col]) if (fast_col in last_row and pd.notna(last_row[fast_col])) else (float(last_row["ema_50"]) if "ema_50" in last_row and pd.notna(last_row["ema_50"]) else None)
        last_slow = float(last_row[slow_col]) if (slow_col in last_row and pd.notna(last_row[slow_col])) else (float(last_row["ema_200"]) if "ema_200" in last_row and pd.notna(last_row["ema_200"]) else None)
        latest_info = {
            "symbol": symbol,
            "timeframe": timeframe.upper(),
            "close": float(last_row["close"]),
            "open": float(last_row["open"]),
            "high": float(last_row["high"]),
            "low": float(last_row["low"]),
            "volume": float(last_row.get("volume", 0.0)),
            "ema50": last_fast,
            "ema200": last_slow,
            "ema_fast": last_fast,
            "ema_slow": last_slow,
            "fast_period": fp,
            "slow_period": sp,
            "timestamp": int(last_row["timestamp"]),
            "total_candles": len(df_sliced),
        }

        crossover_info = None
        if target_timestamp:
            target_matches = df[df["timestamp"] == target_timestamp]
            if not target_matches.empty:
                t_row = target_matches.iloc[0]
                t_fast = float(t_row[fast_col]) if (fast_col in t_row and pd.notna(t_row[fast_col])) else (float(t_row["ema_50"]) if "ema_50" in t_row and pd.notna(t_row["ema_50"]) else None)
                t_slow = float(t_row[slow_col]) if (slow_col in t_row and pd.notna(t_row[slow_col])) else (float(t_row["ema_200"]) if "ema_200" in t_row and pd.notna(t_row["ema_200"]) else None)
                crossover_info = {
                    "timestamp": int(t_row["timestamp"]),
                    "close": float(t_row["close"]),
                    "open": float(t_row["open"]),
                    "high": float(t_row["high"]),
                    "low": float(t_row["low"]),
                    "volume": float(t_row.get("volume", 0.0)),
                    "ema50": t_fast,
                    "ema200": t_slow,
                    "ema_fast": t_fast,
                    "ema_slow": t_slow,
                    "fast_period": fp,
                    "slow_period": sp,
                }

        return {
            "symbol": symbol,
            "timeframe": timeframe.upper(),
            "candles": formatted_candles,
            "ema50": formatted_ema_fast,
            "ema200": formatted_ema_slow,
            "ema_fast": formatted_ema_fast,
            "ema_slow": formatted_ema_slow,
            "fast_period": fp,
            "slow_period": sp,
            "cross_markers": markers,
            "latest": latest_info,
            "target_timestamp": target_timestamp,
            "crossover_candle": crossover_info,
            "target_signal": target_signal,
            "df": df_sliced,  # Retain DataFrame for high-performance Matplotlib rendering
            "df_full": df,    # Converged 1000-candle dataset for independent canonical validation
        }
