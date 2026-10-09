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
    ema50: float  # Fast EMA value (ema50 kept for backward compatibility)
    ema200: float # Slow EMA value (ema200 kept for backward compatibility)
    close_price: float
    previous_ema50: float = 0.0
    previous_ema200: float = 0.0
    candle_status: str = "CLOSED"
    fast_period: int = 50
    slow_period: int = 200

    @property
    def ema_fast(self) -> float:
        return self.ema50

    @property
    def ema_slow(self) -> float:
        return self.ema200

    @property
    def previous_ema_fast(self) -> float:
        return self.previous_ema50

    @property
    def previous_ema_slow(self) -> float:
        return self.previous_ema200


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
    cols = ["timestamp", "open", "high", "low", "close", "volume", f"ema_{fast_period}", f"ema_{slow_period}", "ema_fast", "ema_slow"]
    if not candles:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(candles)
    # Ensure float types
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    if "close" in df.columns and len(df) > 0:
        fast_ema = calculate_ema(df["close"], fast_period)
        slow_ema = calculate_ema(df["close"], slow_period)
        df[f"ema_{fast_period}"] = fast_ema
        df[f"ema_{slow_period}"] = slow_ema
        df["ema_fast"] = fast_ema
        df["ema_slow"] = slow_ema
    else:
        df[f"ema_{fast_period}"] = np.nan
        df[f"ema_{slow_period}"] = np.nan
        df["ema_fast"] = np.nan
        df["ema_slow"] = np.nan

    return df


def _resolve_ema_columns(
    df: pd.DataFrame,
    fast_col: Optional[str] = None,
    slow_col: Optional[str] = None,
    fast_period: int = 50,
    slow_period: int = 200,
) -> tuple[Optional[str], Optional[str], int, int]:
    """Helper resolving fast and slow EMA column names and period integers."""
    fc = fast_col
    fp = fast_period
    if fc is None or fc not in df.columns:
        if f"ema_{fast_period}" in df.columns:
            fc = f"ema_{fast_period}"
            fp = fast_period
        elif "ema_fast" in df.columns:
            fc = "ema_fast"
        elif "ema_50" in df.columns:
            fc = "ema_50"
            fp = 50
    elif fc.startswith("ema_") and fc[4:].isdigit():
        fp = int(fc[4:])

    sc = slow_col
    sp = slow_period
    if sc is None or sc not in df.columns:
        if f"ema_{slow_period}" in df.columns:
            sc = f"ema_{slow_period}"
            sp = slow_period
        elif "ema_slow" in df.columns:
            sc = "ema_slow"
        elif "ema_200" in df.columns:
            sc = "ema_200"
            sp = 200
        else:
            # Look for any other ema_* column that isn't fc
            candidate_cols = [c for c in df.columns if c.startswith("ema_") and c != fc and c not in ("ema_fast", "ema_slow")]
            if candidate_cols:
                sc = candidate_cols[-1]
                if sc[4:].isdigit():
                    sp = int(sc[4:])
    elif sc.startswith("ema_") and sc[4:].isdigit():
        sp = int(sc[4:])

    return fc, sc, fp, sp


def detect_golden_cross(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str = "1h",
    fast_col: Optional[str] = "ema_50",
    slow_col: Optional[str] = "ema_200",
    fast_period: int = 50,
    slow_period: int = 200,
) -> Optional[GoldenCrossSignal]:
    """Detects if a Golden Cross occurred at the last closed candle.

    Definition:
    previous EMA fast <= previous EMA slow
    AND
    current EMA fast > current EMA slow
    """
    if df is None or len(df) < 2:
        return None

    resolved_fast, resolved_slow, fp, sp = _resolve_ema_columns(
        df, fast_col, slow_col, fast_period, slow_period
    )
    if not resolved_fast or not resolved_slow or resolved_fast not in df.columns or resolved_slow not in df.columns:
        return None

    prev_row = df.iloc[-2]
    curr_row = df.iloc[-1]

    prev_fast = float(prev_row[resolved_fast])
    prev_slow = float(prev_row[resolved_slow])
    curr_fast = float(curr_row[resolved_fast])
    curr_slow = float(curr_row[resolved_slow])

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
            fast_period=fp,
            slow_period=sp,
        )

    return None


def find_all_golden_crosses(
    df: pd.DataFrame,
    symbol: str,
    timeframe: str = "1h",
    fast_col: Optional[str] = "ema_50",
    slow_col: Optional[str] = "ema_200",
    fast_period: int = 50,
    slow_period: int = 200,
) -> List[GoldenCrossSignal]:
    """Scans historical candles in the DataFrame and finds all Golden Cross points."""
    crosses: List[GoldenCrossSignal] = []
    if df is None or len(df) < 2:
        return crosses

    resolved_fast, resolved_slow, fp, sp = _resolve_ema_columns(
        df, fast_col, slow_col, fast_period, slow_period
    )
    if not resolved_fast or not resolved_slow or resolved_fast not in df.columns or resolved_slow not in df.columns:
        return crosses

    for i in range(1, len(df)):
        prev_row = df.iloc[i - 1]
        curr_row = df.iloc[i]

        prev_fast = float(prev_row[resolved_fast])
        prev_slow = float(prev_row[resolved_slow])
        curr_fast = float(curr_row[resolved_fast])
        curr_slow = float(curr_row[resolved_slow])

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
                    fast_period=fp,
                    slow_period=sp,
                )
            )

    return crosses
