"""Indicators package."""

from app.indicators.ema import (
    calculate_ema,
    enrich_candles_with_ema,
    detect_golden_cross,
    find_all_golden_crosses,
    GoldenCrossSignal,
)

__all__ = [
    "calculate_ema",
    "enrich_candles_with_ema",
    "detect_golden_cross",
    "find_all_golden_crosses",
    "GoldenCrossSignal",
]
