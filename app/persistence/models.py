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
    ema50: float  # Fast EMA value (ema50 kept for backward compatibility)
    ema200: float # Slow EMA value (ema200 kept for backward compatibility)
    close_price: float
    chart_image_path: Optional[str] = None
    telegram_sent: bool = False
    is_live: bool = False
    created_at: Optional[str] = None
    previous_ema50: Optional[float] = None
    previous_ema200: Optional[float] = None
    fast_period: int = 50
    slow_period: int = 200

    @property
    def ema_fast(self) -> float:
        return self.ema50

    @property
    def ema_slow(self) -> float:
        return self.ema200

    @property
    def previous_ema_fast(self) -> Optional[float]:
        return self.previous_ema50

    @property
    def previous_ema_slow(self) -> Optional[float]:
        return self.previous_ema200


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
    fast_period: int = 50
    slow_period: int = 200

    @property
    def ema_fast(self) -> Optional[float]:
        return self.ema50

    @property
    def ema_slow(self) -> Optional[float]:
        return self.ema200
