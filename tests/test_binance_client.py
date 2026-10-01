from __future__ import annotations

from collections import deque

import pytest

from app.exchange.binance_client import BinanceFuturesClient


HOUR_MS = 3_600_000
NOW_MS = 2_000_000_000_000


def _row(timestamp: int, *, open_candle: bool = False) -> list:
    close_time = timestamp + HOUR_MS - 1
    if open_candle:
        close_time = NOW_MS + 1
    return [timestamp, "1", "2", "0.5", "1.5", "10", close_time]


class _Response:
    status = 200
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def json(self):
        return self.payload

    async def text(self):
        return ""


class _Session:
    def __init__(self, payloads):
        self.payloads = deque(payloads)
        self.calls = []

    def get(self, url, *, params):
        self.calls.append((url, dict(params)))
        return _Response(self.payloads.popleft())


def _client_with_pages(monkeypatch, pages):
    monkeypatch.setattr("app.exchange.binance_client.time.time", lambda: NOW_MS / 1000)
    client = BinanceFuturesClient()
    session = _Session(pages)

    async def get_session():
        return session

    monkeypatch.setattr(client, "_get_session", get_session)
    return client, session


@pytest.mark.asyncio
async def test_1000_rows_with_open_candle_backfills_one_closed_row(monkeypatch):
    start = NOW_MS - 1001 * HOUR_MS
    first_page = [_row(start + i * HOUR_MS) for i in range(999)]
    first_page.append(_row(start + 999 * HOUR_MS, open_candle=True))
    client, session = _client_with_pages(
        monkeypatch,
        [first_page, [_row(start - HOUR_MS)]],
    )

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len({c["timestamp"] for c in candles}) == 1000
    assert [c["timestamp"] for c in candles] == sorted(c["timestamp"] for c in candles)
    assert all(c["close_time"] < NOW_MS for c in candles)
    assert [call[1]["limit"] for call in session.calls] == [1000, 1]
    assert session.calls[1][1]["endTime"] == start - 1


@pytest.mark.asyncio
async def test_exactly_1000_closed_rows_do_not_trigger_backfill(monkeypatch):
    start = NOW_MS - 1001 * HOUR_MS
    client, session = _client_with_pages(
        monkeypatch,
        [[_row(start + i * HOUR_MS) for i in range(1000)]],
    )

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 1
    assert session.calls[0][1]["limit"] == 1000


@pytest.mark.asyncio
async def test_backfill_deduplicates_and_sorts_by_timestamp(monkeypatch):
    start = NOW_MS - 1001 * HOUR_MS
    first_page = [_row(start + i * HOUR_MS) for i in range(999)]
    first_page.append(_row(start + 999 * HOUR_MS, open_candle=True))
    # Simulate an overlapping page: ignore its duplicate and retain the older row.
    client, _session = _client_with_pages(
        monkeypatch,
        [first_page, [_row(start), _row(start - HOUR_MS)]],
    )

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    timestamps = [c["timestamp"] for c in candles]
    assert len(candles) == 1000
    assert len(timestamps) == len(set(timestamps))
    assert timestamps == sorted(timestamps)


@pytest.mark.asyncio
async def test_insufficient_history_stays_short_after_backfill(monkeypatch):
    start = NOW_MS - 1000 * HOUR_MS
    client, session = _client_with_pages(
        monkeypatch,
        [[_row(start + i * HOUR_MS) for i in range(999)], []],
    )

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 999
    assert len(session.calls) == 2
    assert all(c["close_time"] < NOW_MS for c in candles)


@pytest.mark.asyncio
async def test_only_closed_false_keeps_open_row_and_does_not_backfill(monkeypatch):
    rows = [_row(NOW_MS - 3 * HOUR_MS), _row(NOW_MS - 2 * HOUR_MS), _row(NOW_MS - HOUR_MS, open_candle=True)]
    client, session = _client_with_pages(monkeypatch, [rows])

    candles = await client.get_klines("BTCUSDT", limit=3, only_closed=False)

    assert len(candles) == 3
    assert candles[-1]["close_time"] >= NOW_MS
    assert len(session.calls) == 1
    assert session.calls[0][1]["limit"] == 3


@pytest.mark.asyncio
async def test_start_and_end_time_bounds_are_preserved_without_backfill(monkeypatch):
    start_time = NOW_MS - 3 * HOUR_MS
    end_time = NOW_MS
    rows = [_row(start_time), _row(start_time + HOUR_MS), _row(start_time + 2 * HOUR_MS, open_candle=True)]
    client, session = _client_with_pages(monkeypatch, [rows])

    candles = await client.get_klines(
        "BTCUSDT", limit=3, only_closed=True, start_time=start_time, end_time=end_time
    )

    params = session.calls[0][1]
    assert params["startTime"] == start_time
    assert params["endTime"] == end_time
    assert len(session.calls) == 1
    assert len(candles) == 2
    assert all(start_time <= c["timestamp"] <= end_time for c in candles)
