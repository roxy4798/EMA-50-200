"""High-quality Chart Image Renderer for Telegram & Exports."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
import matplotlib
# Use non-interactive Agg backend for headless server rendering
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as patches
from matplotlib.lines import Line2D
import pandas as pd
import numpy as np

from app.charts.chart_theme import THEME, format_price

logger = logging.getLogger("nexora.charts.renderer")


class ChartRenderer:
    def __init__(
        self,
        output_dir: str = "charts",
        width_px: int = 1600,
        height_px: int = 900,
        dpi: int = 100,
    ) -> None:
        self.output_dir = output_dir
        self.width_px = width_px
        self.height_px = height_px
        self.dpi = dpi
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

    def render_golden_cross_chart(
        self,
        chart_data: Dict[str, Any],
        target_timestamp: Optional[int] = None,
        custom_filename: Optional[str] = None,
    ) -> Optional[str]:
        """Renders a 1600x900 modern market chart for a Golden Cross event.

        Saves to PNG and returns the absolute file path.
        Returns None and logs CHART_RENDER_ERROR if rendering fails.
        """
        try:
            symbol = chart_data.get("symbol", "UNKNOWN")
            timeframe = chart_data.get("timeframe", "1H")
            df = chart_data.get("df")

            if df is None or len(df) < 5:
                logger.error(f"CHART_RENDER_ERROR: Insufficient candle data for {symbol} (count: {len(df) if df is not None else 0})")
                return None

            # Prepare data
            df = df.copy().reset_index(drop=True)
            # Create datetime objects from timestamps
            df["dt"] = [datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc) for ts in df["timestamp"]]
            # Also keep sequential integer index for clean candle spacing without weekend/gap distortion
            df["idx"] = range(len(df))

            # Set up Figure
            fig_w = self.width_px / self.dpi
            fig_h = self.height_px / self.dpi
            fig = plt.figure(figsize=(fig_w, fig_h), dpi=self.dpi, facecolor=THEME.bg_canvas)

            # Create layout: Top Header banner (grid or custom axes)
            # Main chart rect: [left, bottom, width, height]
            ax_main = fig.add_axes([0.05, 0.09, 0.90, 0.72], facecolor=THEME.bg_plot)

            # Configure spines / borders
            for spine in ax_main.spines.values():
                spine.set_color(THEME.border_color)
                spine.set_linewidth(1.2)

            # Draw Candlesticks manually for pixel-perfect control
            candle_width = 0.65
            wick_width = 1.2

            up_mask = df["close"] >= df["open"]
            down_mask = df["close"] < df["open"]

            # Bullish candles
            if up_mask.any():
                df_up = df[up_mask]
                # Wicks
                ax_main.vlines(
                    df_up["idx"],
                    df_up["low"],
                    df_up["high"],
                    color=THEME.wick_up,
                    linewidth=wick_width,
                    zorder=2,
                )
                # Bodies
                for _, r in df_up.iterrows():
                    rect_bottom = r["open"]
                    rect_height = max(r["close"] - r["open"], (r["high"] - r["low"]) * 0.01)
                    rect = patches.Rectangle(
                        (r["idx"] - candle_width / 2, rect_bottom),
                        candle_width,
                        rect_height,
                        facecolor=THEME.candle_up,
                        edgecolor=THEME.candle_up,
                        linewidth=0.8,
                        zorder=3,
                    )
                    ax_main.add_patch(rect)

            # Bearish candles
            if down_mask.any():
                df_down = df[down_mask]
                # Wicks
                ax_main.vlines(
                    df_down["idx"],
                    df_down["low"],
                    df_down["high"],
                    color=THEME.wick_down,
                    linewidth=wick_width,
                    zorder=2,
                )
                # Bodies
                for _, r in df_down.iterrows():
                    rect_bottom = r["close"]
                    rect_height = max(r["open"] - r["close"], (r["high"] - r["low"]) * 0.01)
                    rect = patches.Rectangle(
                        (r["idx"] - candle_width / 2, rect_bottom),
                        candle_width,
                        rect_height,
                        facecolor=THEME.candle_down,
                        edgecolor=THEME.candle_down,
                        linewidth=0.8,
                        zorder=3,
                    )
                    ax_main.add_patch(rect)

            # Draw EMA Lines
            if "ema_50" in df.columns:
                valid_ema50 = df["ema_50"].dropna()
                ax_main.plot(
                    df.loc[valid_ema50.index, "idx"],
                    valid_ema50,
                    color=THEME.ema50_color,
                    linewidth=THEME.ema50_width,
                    label="EMA 50",
                    zorder=4,
                )

            if "ema_200" in df.columns:
                valid_ema200 = df["ema_200"].dropna()
                ax_main.plot(
                    df.loc[valid_ema200.index, "idx"],
                    valid_ema200,
                    color=THEME.ema200_color,
                    linewidth=THEME.ema200_width,
                    label="EMA 200",
                    zorder=4,
                )

            # Draw Golden Cross markers
            cross_markers = chart_data.get("cross_markers", [])
            signal_time_str = "CURRENT"

            for m in cross_markers:
                marker_ts = m.get("timestamp_ms")
                match_row = df[df["timestamp"] == marker_ts]
                if not match_row.empty:
                    m_idx = match_row["idx"].values[0]
                    m_close = match_row["close"].values[0]
                    m_low = match_row["low"].values[0]
                    m_ema50 = match_row["ema_50"].values[0] if "ema_50" in match_row and pd.notna(match_row["ema_50"].values[0]) else m_close
                    signal_time_str = m.get("signal_time_utc", "")

                    # Circle marker at cross point
                    ax_main.scatter(
                        [m_idx],
                        [m_ema50],
                        color="#00E5FF",
                        edgecolor="#FFFFFF",
                        s=180,
                        linewidth=2.5,
                        zorder=6,
                    )

                    # Elegant callout annotation box below the candle
                    y_range = max(df["high"].max() - df["low"].min(), 1.0)
                    offset_y = y_range * 0.07
                    annot_y = m_low - offset_y

                    ax_main.annotate(
                        "● GOLDEN CROSS",
                        xy=(m_idx, m_ema50),
                        xytext=(m_idx, annot_y),
                        arrowprops=dict(
                            arrowstyle="->",
                            color="#00E5FF",
                            lw=2.0,
                            shrinkA=4,
                            shrinkB=4,
                        ),
                        bbox=dict(
                            boxstyle="round,pad=0.5,rounding_size=0.3",
                            facecolor="#002b36",
                            edgecolor="#00E5FF",
                            linewidth=1.8,
                            alpha=0.95,
                        ),
                        color="#FFFFFF",
                        fontsize=11,
                        fontweight="bold",
                        ha="center",
                        va="top",
                        zorder=7,
                    )

            # Y-Axis Formatting (Price)
            ax_main.yaxis.tick_right()
            ax_main.yaxis.set_label_position("right")
            ax_main.tick_params(axis="y", colors=THEME.axis_text_color, labelsize=10.5, length=5)
            ax_main.yaxis.set_major_formatter(plt.FuncFormatter(lambda val, pos: format_price(val)))

            # X-Axis Formatting (Readable Timestamps)
            # Pick ~8-10 equidistant tick indices
            step = max(1, len(df) // 8)
            tick_indices = list(range(0, len(df), step))
            if tick_indices[-1] != len(df) - 1:
                tick_indices.append(len(df) - 1)

            tick_labels = [df.loc[i, "dt"].strftime("%d %b\n%H:%M") for i in tick_indices]
            ax_main.set_xticks(tick_indices)
            ax_main.set_xticklabels(tick_labels, color=THEME.axis_text_color, fontsize=10, ha="center")
            ax_main.set_xlim(-1, len(df))

            # Y limits with breathing room
            p_min = df["low"].min()
            p_max = df["high"].max()
            margin = (p_max - p_min) * 0.12 if p_max > p_min else 1.0
            ax_main.set_ylim(p_min - margin, p_max + margin * 0.6)

            # Gridlines
            ax_main.grid(True, linestyle=THEME.grid_linestyle, color=THEME.grid_color, alpha=THEME.grid_alpha)

            # Legend
            legend_elements = [
                Line2D([0], [0], color=THEME.ema50_color, lw=2.5, label="EMA 50"),
                Line2D([0], [0], color=THEME.ema200_color, lw=2.5, label="EMA 200"),
                Line2D([0], [0], marker="o", color="w", markerfacecolor=THEME.candle_up, markersize=8, label="Bullish 1H"),
                Line2D([0], [0], marker="o", color="w", markerfacecolor=THEME.candle_down, markersize=8, label="Bearish 1H"),
            ]
            leg = ax_main.legend(
                handles=legend_elements,
                loc="upper left",
                frameon=True,
                facecolor=THEME.bg_panel,
                edgecolor=THEME.border_color,
                fontsize=10,
                labelcolor="#E2E8F0",
            )
            leg.get_frame().set_alpha(0.85)

            # -------------------------------------------------------------
            # HEADER & INFORMATION PANEL (NEXORA Terminal UI)
            # -------------------------------------------------------------
            latest = chart_data.get("latest", {})
            last_close = latest.get("close", df.iloc[-1]["close"])
            ema50_val = latest.get("ema50", df.iloc[-1].get("ema_50", 0.0))
            ema200_val = latest.get("ema200", df.iloc[-1].get("ema_200", 0.0))

            # Title & Subtitle (Left top)
            fig.text(0.05, 0.955, "NEXORA EMA CROSS", color="#FFFFFF", fontsize=18, fontweight="heavy", fontfamily="sans-serif")
            fig.text(
                0.05,
                0.925,
                f"{symbol} • BINANCE USD-M FUTURES • TIMEFRAME {timeframe}",
                color=THEME.subtitle_color,
                fontsize=11.5,
                fontfamily="sans-serif",
            )

            # Signal & Status Badges (Left mid)
            fig.text(
                0.05,
                0.895,
                "SIGNAL: EMA50 CROSS ABOVE EMA200   |   STATUS: GOLDEN CROSS CONFIRMED",
                color="#00E676",
                fontsize=11,
                fontweight="bold",
            )

            # Compact Info Panel Box (Right top)
            # Position: x=0.62, y=0.88, width=0.33, height=0.09
            ax_info = fig.add_axes([0.62, 0.885, 0.33, 0.095], facecolor=THEME.bg_panel)
            for spine in ax_info.spines.values():
                spine.set_color(THEME.border_color)
                spine.set_linewidth(1.0)
            ax_info.set_xticks([])
            ax_info.set_yticks([])

            # Info panel text rows
            # Left column: Symbol, Close, Candle
            ax_info.text(0.04, 0.72, f"SYMBOL: {symbol}", color="#FFFFFF", fontsize=10, fontweight="bold")
            ax_info.text(0.04, 0.40, f"CLOSE: {format_price(last_close)}", color="#E2E8F0", fontsize=10)
            ax_info.text(0.04, 0.12, "CANDLE: CLOSED", color="#00E676", fontsize=9.5, fontweight="bold")

            # Mid column: EMA50 & EMA200
            ax_info.text(0.42, 0.72, f"EMA 50: {format_price(ema50_val)}", color=THEME.ema50_color, fontsize=10, fontweight="bold")
            ax_info.text(0.42, 0.40, f"EMA 200: {format_price(ema200_val)}", color=THEME.ema200_color, fontsize=10, fontweight="bold")

            # Right column: Signal Time
            ax_info.text(0.42, 0.12, f"TIME: {signal_time_str}", color=THEME.subtitle_color, fontsize=9)

            # Save PNG
            timestamp_ms = target_timestamp or int(latest.get("timestamp", int(time.time() * 1000)))
            if not custom_filename:
                filename = f"{symbol}_{timeframe}_{timestamp_ms}.png"
            else:
                filename = custom_filename

            filepath = os.path.abspath(os.path.join(self.output_dir, filename))
            plt.savefig(filepath, dpi=self.dpi, facecolor=THEME.bg_canvas, edgecolor="none", format="png")
            plt.close(fig)

            logger.info(f"Generated market chart for {symbol}: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"CHART_RENDER_ERROR: Failed to render chart for {chart_data.get('symbol')}: {e}", exc_info=True)
            if "fig" in locals():
                plt.close(fig)
            return None
