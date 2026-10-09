"""NEXORA Chart Theme & Visual Identity Tokens."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class NexoraChartTheme:
    # Canvas & Panels
    bg_canvas: str = "#080B11"
    bg_plot: str = "#0D121D"
    bg_panel: str = "#121826"
    border_color: str = "#1E293B"

    # Grid
    grid_color: str = "#1E293B"
    grid_alpha: float = 0.45
    grid_linestyle: str = "--"

    # Candles
    candle_up: str = "#00E676"        # Vivid emerald green
    candle_down: str = "#FF3366"      # Clean high-contrast crimson
    wick_up: str = "#00E676"
    wick_down: str = "#FF3366"

    # Moving Averages
    ema50_color: str = "#00E5FF"      # Cyan neon accent
    ema50_width: float = 2.0
    ema200_color: str = "#FFA000"     # Rich amber gold accent
    ema200_width: float = 2.0

    @property
    def ema_fast_color(self) -> str:
        return self.ema50_color

    @property
    def ema_slow_color(self) -> str:
        return self.ema200_color

    @property
    def ema_fast_width(self) -> float:
        return self.ema50_width

    @property
    def ema_slow_width(self) -> float:
        return self.ema200_width

    # Cross Marker
    cross_marker_color: str = "#00E5FF"
    cross_badge_bg: str = "#00363A"
    cross_text_color: str = "#E0F7FA"

    # Typography
    font_family: str = "sans-serif"
    title_color: str = "#FFFFFF"
    subtitle_color: str = "#94A3B8"
    accent_color: str = "#38BDF8"
    text_muted: str = "#64748B"
    axis_text_color: str = "#94A3B8"

    # Status / Indicators
    status_confirmed: str = "#00E676"


THEME = NexoraChartTheme()


def format_price(price: float) -> str:
    """Intelligently format asset price based on magnitude."""
    if price >= 1000:
        return f"{price:,.2f}"
    elif price >= 1:
        return f"{price:,.4f}"
    elif price >= 0.001:
        return f"{price:,.6f}"
    else:
        return f"{price:,.8f}"
