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
    ) -> None:
        self.binance_client = binance_client
        self.database = database
        self.cache_ttl_seconds = cache_ttl_seconds
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
    ) -> Dict[str, Any]:
        """Provides full candlestick, EMA50, EMA200 and Golden Cross marker data.

        Returns data structure ready for both Matplotlib PNG rendering and
        TradingView Lightweight Charts.
        """
        symbol = symbol.upper()
        if timeframe.lower() != "1h":
            raise ValueError("NEXORA charts support the 1h timeframe only")
        timeframe = "1h"
        candles: List[Dict[str, Any]] = []

        # If target_timestamp is specified, fetch historical window around target
        if target_timestamp:
            # Fetch 300 candles ending ~35 candles after target_timestamp to give full context
            end_req_time = target_timestamp + (35 * 3600 * 1000)
            candles = await self.binance_client.get_klines(
                symbol, interval=timeframe, limit=300, only_closed=True, end_time=end_req_time
            )
        else:
            fetch_limit = 250
            candles = await self.binance_client.get_klines(symbol, interval=timeframe, limit=fetch_limit, only_closed=True)
            if candles:
                df = enrich_candles_with_ema(candles, fast_period=50, slow_period=200)
                candles_to_cache = df.to_dict(orient="records")
                await self.database.cache_candles(candles_to_cache, symbol, timeframe)
                candles = candles_to_cache

        if not candles:
            return {
                "symbol": symbol,
                "timeframe": timeframe.upper(),
                "candles": [],
                "ema50": [],
                "ema200": [],
                "cross_markers": [],
                "latest": {},
            }

        # Ensure EMA is calculated across the full dataset
        df = enrich_candles_with_ema(candles, fast_period=50, slow_period=200)

        # Reuse the exact stored EMA pair for recorded historical signal charts.
        # This prevents a different REST window/EMA seed from changing the event
        # values displayed in its panel.
        target_signal = None
        if target_timestamp:
            target_signal = await self.database.get_signal_for_candle(symbol, timeframe, target_timestamp)
            if target_signal:
                matches = df.index[df["timestamp"] == target_timestamp].tolist()
                if matches:
                    pos = matches[0]
                    df.at[pos, "ema_50"] = float(target_signal["ema50"])
                    df.at[pos, "ema_200"] = float(target_signal["ema200"])
                    if pos > 0 and target_signal.get("previous_ema50") is not None and target_signal.get("previous_ema200") is not None:
                        df.at[pos - 1, "ema_50"] = float(target_signal["previous_ema50"])
                        df.at[pos - 1, "ema_200"] = float(target_signal["previous_ema200"])

        # Detect all historical Golden Crosses in the dataset
        golden_crosses = find_all_golden_crosses(df, symbol=symbol, timeframe=timeframe)

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
        formatted_ema50 = []
        formatted_ema200 = []

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
            if pd.notna(row["ema_50"]):
                formatted_ema50.append({
                    "time": ts_sec,
                    "value": float(row["ema_50"]),
                })
            if pd.notna(row["ema_200"]):
                formatted_ema200.append({
                    "time": ts_sec,
                    "value": float(row["ema_200"]),
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
                    "signal_time_utc": gx.signal_time_utc,
                    "text": "GOLDEN CROSS",
                    "position": "belowBar",
                    "shape": "arrowUp",
                    "color": "#00E5FF",
                })

        last_row = df_sliced.iloc[-1]
        latest_info = {
            "symbol": symbol,
            "timeframe": timeframe.upper(),
            "close": float(last_row["close"]),
            "open": float(last_row["open"]),
            "high": float(last_row["high"]),
            "low": float(last_row["low"]),
            "volume": float(last_row.get("volume", 0.0)),
            "ema50": float(last_row["ema_50"]) if pd.notna(last_row["ema_50"]) else None,
            "ema200": float(last_row["ema_200"]) if pd.notna(last_row["ema_200"]) else None,
            "timestamp": int(last_row["timestamp"]),
            "total_candles": len(df_sliced),
        }

        crossover_info = None
        if target_timestamp:
            target_matches = df[df["timestamp"] == target_timestamp]
            if not target_matches.empty:
                t_row = target_matches.iloc[0]
                crossover_info = {
                    "timestamp": int(t_row["timestamp"]),
                    "close": float(t_row["close"]),
                    "open": float(t_row["open"]),
                    "high": float(t_row["high"]),
                    "low": float(t_row["low"]),
                    "volume": float(t_row.get("volume", 0.0)),
                    "ema50": float(t_row["ema_50"]) if pd.notna(t_row["ema_50"]) else None,
                    "ema200": float(t_row["ema_200"]) if pd.notna(t_row["ema_200"]) else None,
                }

        return {
            "symbol": symbol,
            "timeframe": timeframe.upper(),
            "candles": formatted_candles,
            "ema50": formatted_ema50,
            "ema200": formatted_ema200,
            "cross_markers": markers,
            "latest": latest_info,
            "target_timestamp": target_timestamp,
            "crossover_candle": crossover_info,
            "target_signal": target_signal,
            "df": df_sliced,  # Retain DataFrame for high-performance Matplotlib rendering
        }
