"""NEXORA EMA Indicator and Golden Cross Detection Engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np


@dataclass
class GoldenCrossSignal:
    symbol: str
    timeframe: str
    candle_timestamp: int  # ms timestamp
    signal_time_utc: str
    ema50: float
    ema200: float
    close_price: float
    previous_ema50: float
    previous_ema200: float
    candle_status: str = "CLOSED"


def calculate_ema(series: pd.Series | List[float], span: int) -> pd.Series:
    """Calculate standard Exponential Moving Average with smoothing alpha = 2 / (span + 1).

    Uses adjust=False which corresponds to the standard recursive formula:
    EMA_today = Price_today * (2/(span+1)) + EMA_yesterday * (1 - (2/(span+1)))
    """
    if not isinstance(series, pd.Series):
        series = pd.Series(series, dtype=float)
    return series.ewm(span=span, adjust=False).mean()


def enrich_candles_with_ema(
    candles: List[Dict[str, Any]],
    fast_period: int = 50,
    slow_period: int = 200,
) -> pd.DataFrame:
    """Takes a list of candle dicts and returns a DataFrame with calculated EMA fast & slow.

    Expected candle keys:
    - timestamp (ms open or close)
    - open
    - high
    - low
    - close
    - volume
    """
    if not candles:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", f"ema_{fast_period}", f"ema_{slow_period}"])

    df = pd.DataFrame(candles)
    # Ensure float types
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    if "close" in df.columns and len(df) > 0:
        df[f"ema_{fast_period}"] = calculate_ema(df["close"], fast_period)
        df[f"ema_{slow_period}"] = calculate_ema(df["close"], slow_period)
    else:
        df[f"ema_{fast_period}"] = np.nan
        df[f"ema_{slow_period}"] = np.nan

    return df


def detect_golden_cross(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str = "1h",
    fast_col: str = "ema_50",
    slow_col: str = "ema_200",
) -> Optional[GoldenCrossSignal]:
    """Detects if a Golden Cross occurred at the last closed candle.

    Definition:
    previous EMA50 <= previous EMA200
    AND
    current EMA50 > current EMA200
    """
    if df is None or len(df) < 2:
        return None

    if fast_col not in df.columns or slow_col not in df.columns:
        return None

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    prev_fast = float(prev_row[fast_col])
    prev_slow = float(prev_row[slow_col])
    curr_fast = float(curr_row[fast_col])
    curr_slow = float(curr_row[slow_col])

    # Check for NaN
    if np.isnan(prev_fast) or np.isnan(prev_slow) or np.isnan(curr_fast) or np.isnan(curr_slow):
        return None

    # Condition: previous fast <= previous slow and current fast > current slow
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        ts = int(curr_row["timestamp"])
        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        time_str = dt.strftime("%d %b %Y • %H:%M UTC")

        return GoldenCrossSignal(
            symbol=symbol,
            timeframe=timeframe.upper(),
            candle_timestamp=ts,
            signal_time_utc=time_str,
            ema50=curr_fast,
            ema200=curr_slow,
            close_price=float(curr_row["close"]),
            previous_ema50=prev_fast,
            previous_ema200=prev_slow,
            candle_status="CLOSED",
        )

    return None


def find_all_golden_crosses(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str = "1h",
    fast_col: str = "ema_50",
    slow_col: str = "ema_200",
) -> List[GoldenCrossSignal]:
    """Scans historical candles in the DataFrame and finds all Golden Cross points."""
    crosses: List[GoldenCrossSignal] = []
    if df is None or len(df) < 2:
        return crosses

    if fast_col not in df.columns or slow_col not in df.columns:
        return crosses

    for i in range(1, len(df)):
        prev_row = df.iloc[i - 1]
        curr_row = df.iloc[i]

        prev_fast = float(prev_row[fast_col])
        prev_slow = float(prev_row[slow_col])
        curr_fast = float(curr_row[fast_col])
        curr_slow = float(curr_row[slow_col])

        if np.isnan(prev_fast) or np.isnan(prev_slow) or np.isnan(curr_fast) or np.isnan(curr_slow):
            continue

        if prev_fast <= prev_slow and curr_fast > curr_slow:
            ts = int(curr_row["timestamp"])
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            time_str = dt.strftime("%d %b %Y • %H:%M UTC")
            crosses.append(
                GoldenCrossSignal(
                    symbol=symbol,
                    timeframe=timeframe.upper(),
                    candle_timestamp=ts,
                    signal_time_utc=time_str,
                    ema50=curr_fast,
                    ema200=curr_slow,
                    close_price=float(curr_row["close"]),
                    previous_ema50=prev_fast,
                    previous_ema200=prev_slow,
                    candle_status="CLOSED",
                )
            )

    return crosses
