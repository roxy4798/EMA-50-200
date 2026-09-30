"""NEXORA EMA CROSS System Configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import List
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # Binance USD-M Futures
    binance_futures_base_url: str = Field(default="https://fapi.binance.com")
    binance_ws_base_url: str = Field(default="wss://fstream.binance.com/ws")
    timeframe: str = Field(default="1h")
    ema_fast: int = Field(default=50)
    ema_slow: int = Field(default=200)
    candle_limit: int = Field(default=250)

    # Symbol filtering
    symbols: str = Field(default="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,NEARUSDT,SUIUSDT,APTUSDT,ARBUSDT,OPUSDT,1000PEPEUSDT")
    all_symbols: bool = Field(default=False)

    # Telegram
    telegram_bot_token: str = Field(default="")
    telegram_chat_id: str = Field(default="")
    telegram_enabled: bool = Field(default=False)

    # Server & Dashboard
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080)

    # Paths & rendering
    db_path: str = Field(default="data/nexora.db")
    charts_dir: str = Field(default="charts")
    chart_width: int = Field(default=1600)
    chart_height: int = Field(default=900)
    chart_dpi: int = Field(default=100)
    cache_ttl_seconds: int = Field(default=300)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

    @property
    def symbol_list(self) -> List[str]:
        if not self.symbols:
            return ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        return [s.strip().upper() for s in self.symbols.split(",") if s.strip()]

    def ensure_directories(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.charts_dir).mkdir(parents=True, exist_ok=True)
        Path("logs").mkdir(parents=True, exist_ok=True)


settings = Settings()
settings.ensure_directories()
