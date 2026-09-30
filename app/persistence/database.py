"""SQLite persistence engine for NEXORA EMA CROSS with idempotency and gap recovery support."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import aiosqlite

from app.persistence.models import SignalRecord, CachedCandle


class Database:
    def __init__(self, db_path: str = "data/nexora.db") -> None:
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def init(self) -> None:
        """Initialize database tables and indexes."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    candle_timestamp INTEGER NOT NULL,
                    signal_time_utc TEXT NOT NULL,
                    ema50 REAL NOT NULL,
                    ema200 REAL NOT NULL,
                    close_price REAL NOT NULL,
                    chart_image_path TEXT,
                    telegram_sent INTEGER NOT NULL DEFAULT 0,
                    is_live INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, timeframe, candle_timestamp)
                )
                """
            )

            # Migration for existing database
            try:
                await db.execute("ALTER TABLE signals ADD COLUMN is_live INTEGER NOT NULL DEFAULT 0")
            except Exception:
                pass  # column already exists

            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS candle_cache (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    ema50 REAL,
                    ema200 REAL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(symbol, timeframe, timestamp)
                )
                """
            )

            await db.execute("CREATE INDEX IF NOT EXISTS idx_signals_sym_ts ON signals(symbol, timeframe, candle_timestamp DESC)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at DESC)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_signals_live ON signals(is_live)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_candles_lookup ON candle_cache(symbol, timeframe, timestamp ASC)")

            await db.commit()

    async def has_signal(self, symbol: str, timeframe: str, candle_timestamp: int) -> bool:
        """Check if a signal has already been recorded for this exact closed candle."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM signals WHERE symbol = ? AND timeframe = ? AND candle_timestamp = ?",
                (symbol.upper(), timeframe.upper(), candle_timestamp),
            )
            row = await cursor.fetchone()
            return row is not None

    async def save_signal(self, signal: SignalRecord) -> int:
        """Inserts a new golden cross signal. Returns the signal id."""
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    """
                    INSERT INTO signals (
                        symbol, timeframe, candle_timestamp, signal_time_utc,
                        ema50, ema200, close_price, chart_image_path, telegram_sent, is_live
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, timeframe, candle_timestamp) DO UPDATE SET
                        chart_image_path = coalesce(excluded.chart_image_path, signals.chart_image_path),
                        telegram_sent = max(signals.telegram_sent, excluded.telegram_sent),
                        is_live = max(signals.is_live, excluded.is_live)
                    RETURNING id
                    """,
                    (
                        signal.symbol.upper(),
                        signal.timeframe.upper(),
                        signal.candle_timestamp,
                        signal.signal_time_utc,
                        signal.ema50,
                        signal.ema200,
                        signal.close_price,
                        signal.chart_image_path,
                        1 if signal.telegram_sent else 0,
                        1 if signal.is_live else 0,
                    ),
                )
                row = await cursor.fetchone()
                await db.commit()
                return row[0] if row else 0

    async def save_historical_signals_batch(self, signals: List[SignalRecord]) -> int:
        """Batch inserts historical golden cross signals idempotently in a single transaction.

        Uses INSERT ... ON CONFLICT(symbol, timeframe, candle_timestamp) DO NOTHING
        to preserve idempotency and avoid overwriting live signals or re-inserting
        existing records.
        Returns the number of new rows inserted.
        """
        if not signals:
            return 0

        records = [
            (
                s.symbol.upper(),
                s.timeframe.upper(),
                s.candle_timestamp,
                s.signal_time_utc,
                s.ema50,
                s.ema200,
                s.close_price,
                s.chart_image_path,
                1 if s.telegram_sent else 0,
                0,  # is_live = 0 strictly for historical signals
            )
            for s in signals
        ]

        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                total_before = db.total_changes
                await db.executemany(
                    """
                    INSERT INTO signals (
                        symbol, timeframe, candle_timestamp, signal_time_utc,
                        ema50, ema200, close_price, chart_image_path, telegram_sent, is_live
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, timeframe, candle_timestamp) DO NOTHING
                    """,
                    records,
                )
                await db.commit()
                return db.total_changes - total_before

    async def update_signal_delivery(self, signal_id: int, chart_image_path: Optional[str], telegram_sent: bool) -> None:
        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    """
                    UPDATE signals
                    SET chart_image_path = coalesce(?, chart_image_path),
                        telegram_sent = ?
                    WHERE id = ?
                    """,
                    (chart_image_path, 1 if telegram_sent else 0, signal_id),
                )
                await db.commit()

    async def get_recent_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves recent Golden Cross signals."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT id, symbol, timeframe, candle_timestamp, signal_time_utc,
                       ema50, ema200, close_price, chart_image_path, telegram_sent, is_live, created_at
                FROM signals
                ORDER BY candle_timestamp DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_total_signals_count(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM signals")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_live_signals_count(self) -> int:
        """Counts exclusively live detected Golden Cross alerts."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM signals WHERE is_live = 1")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_historical_signals_count(self) -> int:
        """Counts historical Golden Crosses discovered in initial scan."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM signals WHERE is_live = 0")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_last_signal(self) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT symbol, signal_time_utc, candle_timestamp, close_price
                FROM signals
                ORDER BY candle_timestamp DESC, id DESC
                LIMIT 1
                """
            )
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def cache_candles(self, candles: List[Dict[str, Any]], symbol: str, timeframe: str = "1h") -> None:
        """Batch upsert candles into cache."""
        if not candles:
            return

        records = []
        for c in candles:
            records.append((
                symbol.upper(),
                timeframe.upper(),
                int(c["timestamp"]),
                float(c["open"]),
                float(c["high"]),
                float(c["low"]),
                float(c["close"]),
                float(c.get("volume", 0.0)),
                float(c["ema_50"]) if "ema_50" in c and c["ema_50"] is not None and not (isinstance(c["ema_50"], float) and c["ema_50"] != c["ema_50"]) else None,
                float(c["ema_200"]) if "ema_200" in c and c["ema_200"] is not None and not (isinstance(c["ema_200"], float) and c["ema_200"] != c["ema_200"]) else None,
            ))

        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                await db.executemany(
                    """
                    INSERT INTO candle_cache (
                        symbol, timeframe, timestamp, open, high, low, close, volume, ema50, ema200, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(symbol, timeframe, timestamp) DO UPDATE SET
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        ema50 = coalesce(excluded.ema50, candle_cache.ema50),
                        ema200 = coalesce(excluded.ema200, candle_cache.ema200),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    records,
                )
                await db.commit()

    async def get_cached_candles(self, symbol: str, timeframe: str = "1h", limit: int = 150) -> List[Dict[str, Any]]:
        """Fetch cached candles for symbol sorted ascending by timestamp."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT timestamp, open, high, low, close, volume, ema50 as ema_50, ema200 as ema_200
                FROM candle_cache
                WHERE symbol = ? AND timeframe = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (symbol.upper(), timeframe.upper(), limit),
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in reversed(rows)]

    async def get_last_cached_candle_timestamp(self, symbol: str, timeframe: str = "1h") -> Optional[int]:
        """Fetch the timestamp of the latest cached candle for gap detection."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT timestamp FROM candle_cache
                WHERE symbol = ? AND timeframe = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol.upper(), timeframe.upper()),
            )
            row = await cursor.fetchone()
            return int(row[0]) if row else None
