"""Tests for Chart Data Provider and Chart Renderer."""

import os
import pytest
import pandas as pd
from PIL import Image

from app.charts.chart_theme import THEME, format_price
from app.charts.chart_renderer import ChartRenderer


def test_format_price():
    assert format_price(65432.10) == "65,432.10"
    assert format_price(154.25) == "154.2500"
    assert format_price(0.004567) == "0.004567"
    assert format_price(0.00001234) == "0.00001234"


def test_chart_renderer_generates_valid_1600x900_png(tmp_path):
    renderer = ChartRenderer(
        output_dir=str(tmp_path),
        width_px=1600,
        height_px=900,
        dpi=100,
    )

    # Generate synthetic 100 candles
    base_ts = 1700000000000
    rows = []
    price = 50000.0
    for i in range(100):
        ts = base_ts + i * 3600000
        o = price
        c = price + (50 if i % 2 == 0 else -40)
        h = max(o, c) + 30
        l = min(o, c) - 30
        price = c
        rows.append({
            "timestamp": ts,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": 1000.0,
            "ema_50": price - 20,
            "ema_200": price - 50,
        })

    df = pd.DataFrame(rows)
    cross_ts = base_ts + 80 * 3600000
    chart_data = {
        "symbol": "BTCUSDT",
        "timeframe": "1H",
        "df": df,
        "cross_markers": [
            {
                "time": int(cross_ts // 1000),
                "timestamp_ms": cross_ts,
                "price": 50500.0,
                "signal_time_utc": "30 Sep 2026 • 10:00 UTC",
                "text": "GOLDEN CROSS",
            }
        ],
        "latest": {
            "symbol": "BTCUSDT",
            "timeframe": "1H",
            "close": 51200.0,
            "ema50": 51000.0,
            "ema200": 50400.0,
            "timestamp": base_ts + 99 * 3600000,
        },
    }

    output_path = renderer.render_golden_cross_chart(
        chart_data=chart_data,
        target_timestamp=cross_ts,
        custom_filename="test_chart.png",
    )

    assert output_path is not None
    assert os.path.isfile(output_path)

    # Verify PNG dimensions
    with Image.open(output_path) as img:
        assert img.size == (1600, 900)
        assert img.format == "PNG"


def test_chart_renderer_handles_empty_data_gracefully(tmp_path):
    renderer = ChartRenderer(output_dir=str(tmp_path))
    result = renderer.render_golden_cross_chart({"symbol": "BTCUSDT", "df": None})
    # Must log error and return None without raising exception
    assert result is None
