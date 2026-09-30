"""Database models and data structures."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class SignalRecord:
    id: Optional[int]
    symbol: str
    timeframe: str
    candle_timestamp: int
    signal_time_utc: str
    ema50: float
    ema200: float
    close_price: float
    chart_image_path: Optional[str] = None
    telegram_sent: bool = False
    is_live: bool = False
    created_at: Optional[str] = None


@dataclass
class CachedCandle:
    symbol: str
    timeframe: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    ema50: Optional[float] = None
    ema200: Optional[float] = None
