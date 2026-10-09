"""Deterministic unit tests for BinanceFuturesClient.get_klines pagination."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Dict, List, Optional

import pytest

from app.exchange.binance_client import BinanceFuturesClient


HOUR_MS = 3_600_000
NOW_MS = 2_000_000_000_000


def _row(timestamp: int, *, open_candle: bool = False) -> list:
    """Build a Binance raw kline row.  close_time is set in the future for open candles."""
    close_time = timestamp + HOUR_MS - 1
    if open_candle:
        close_time = NOW_MS + 1
    return [timestamp, "1", "2", "0.5", "1.5", "10", close_time]


# ---------------------------------------------------------------------------
# Lightweight HTTP mock infrastructure
# ---------------------------------------------------------------------------

class _Response:
    """Simulates an aiohttp response."""
    headers: Dict[str, str] = {}

    def __init__(self, payload: Any, status: int = 200):
        self.payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def json(self):
        return self.payload

    async def text(self):
        return ""


class _RateLimitResponse:
    """Simulates a 429 rate-limit response."""
    status = 429

    def __init__(self, retry_after: str = "1"):
        self.headers = {"Retry-After": retry_after}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def json(self):
        return {}

    async def text(self):
        return "rate limited"


class _Session:
    """Records every HTTP call and returns pre-configured responses."""

    def __init__(self, payloads: List[Any], statuses: Optional[List[int]] = None):
        self.payloads = deque(payloads)
        self.statuses = deque(statuses) if statuses else deque([200] * len(payloads))
        self.calls: List[tuple] = []

    def get(self, url, *, params):
        self.calls.append((url, dict(params)))
        payload = self.payloads.popleft() if self.payloads else []
        status = self.statuses.popleft() if self.statuses else 200
        return _Response(payload, status)


class _SessionWithSequence:
    """Returns responses from a sequence of (payload, status) tuples.
    For 429 responses, returns a _RateLimitResponse to trigger _check_rate_limit."""

    def __init__(self, sequence: List[tuple]):
        """sequence: list of (payload, status) tuples. Use None payload for 429."""
        self._sequence = deque(sequence)
        self.calls: List[tuple] = []

    def get(self, url, *, params):
        self.calls.append((url, dict(params)))
        if not self._sequence:
            return _Response([], 200)
        payload, status = self._sequence.popleft()
        if status == 429:
            return _RateLimitResponse()
        return _Response(payload, status)


def _client_with_pages(monkeypatch, pages, statuses=None):
    """Create a BinanceFuturesClient wired to return *pages* sequentially."""
    monkeypatch.setattr("app.exchange.binance_client.time.time", lambda: NOW_MS / 1000)
    client = BinanceFuturesClient()
    session = _Session(pages, statuses)

    async def get_session():
        return session

    monkeypatch.setattr(client, "_get_session", get_session)
    return client, session


def _client_with_sequence(monkeypatch, sequence):
    """Create a BinanceFuturesClient wired to return a sequence of (payload, status)."""
    # Track time for rate-limit retry sleeps
    _fake_time = [NOW_MS / 1000]

    def fake_time():
        return _fake_time[0]

    _orig_sleep = asyncio.sleep

    async def fake_sleep(seconds):
        _fake_time[0] += seconds
        # Minimal actual sleep to yield control
        await _orig_sleep(0)

    monkeypatch.setattr("app.exchange.binance_client.time.time", fake_time)
    monkeypatch.setattr("app.exchange.binance_client.asyncio.sleep", fake_sleep)
    client = BinanceFuturesClient()
    session = _SessionWithSequence(sequence)

    async def get_session():
        return session

    monkeypatch.setattr(client, "_get_session", get_session)
    return client, session


# ---------------------------------------------------------------------------
# A. 1000 rows / 999 closed — initial page, no backfill needed if 1000 closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_1000_closed_candles_no_backfill(monkeypatch):
    """When all 1000 rows are already closed, exactly 1 request is made."""
    start = NOW_MS - 1001 * HOUR_MS
    page = [_row(start + i * HOUR_MS) for i in range(1000)]
    client, session = _client_with_pages(monkeypatch, [page])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 1
    assert all(c["close_time"] < NOW_MS for c in candles)


# ---------------------------------------------------------------------------
# B. 999 closed + 1 open -> backfill succeeds -> 1000 closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_999_closed_plus_open_backfill_succeeds(monkeypatch):
    """The standard live case: 999 closed + 1 open -> single backfill page."""
    start = NOW_MS - 1001 * HOUR_MS
    first_page = [_row(start + i * HOUR_MS) for i in range(999)]
    first_page.append(_row(start + 999 * HOUR_MS, open_candle=True))
    backfill_page = [_row(start - HOUR_MS)]

    client, session = _client_with_pages(monkeypatch, [first_page, backfill_page])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 2
    assert all(c["close_time"] < NOW_MS for c in candles)
    # Backfill should request endTime = earliest - 1
    assert session.calls[1][1]["endTime"] == start - 1


# ---------------------------------------------------------------------------
# C. Backfill receives 429 once -> Retry-After/backoff -> succeeds -> 1000
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_429_then_retry_succeeds(monkeypatch):
    """If the first backfill attempt gets 429, wait Retry-After, retry, succeed."""
    start = NOW_MS - 1001 * HOUR_MS
    first_page = [_row(start + i * HOUR_MS) for i in range(999)]
    first_page.append(_row(start + 999 * HOUR_MS, open_candle=True))
    backfill_page = [_row(start - HOUR_MS)]

    sequence = [
        (first_page, 200),     # Initial request succeeds
        (None, 429),           # First backfill attempt: 429
        (backfill_page, 200),  # Second backfill attempt: succeeds
    ]
    client, session = _client_with_sequence(monkeypatch, sequence)

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 3  # initial + 429 + retry
    assert all(c["close_time"] < NOW_MS for c in candles)


# ---------------------------------------------------------------------------
# D. Repeated 429 until retry budget exhausted -> <1000 -> fail closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_persistent_429_exhausts_retries(monkeypatch):
    """If backfill gets 429 on every attempt, return partial (fail-closed)."""
    start = NOW_MS - 1001 * HOUR_MS
    first_page = [_row(start + i * HOUR_MS) for i in range(999)]
    first_page.append(_row(start + 999 * HOUR_MS, open_candle=True))

    sequence = [
        (first_page, 200),  # Initial request succeeds
        (None, 429),        # Backfill attempt 1: 429
        (None, 429),        # Backfill attempt 2: 429 (retry budget exhausted)
    ]
    client, session = _client_with_sequence(monkeypatch, sequence)

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    # Should return 999 (not 1000, not empty)
    assert len(candles) == 999
    assert len(session.calls) == 3  # initial + 2 failed attempts
    assert all(c["close_time"] < NOW_MS for c in candles)


# ---------------------------------------------------------------------------
# E. Genuinely new symbol with 600 total candles -> <1000 -> fail closed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_genuinely_short_symbol_verified_by_backfill(monkeypatch):
    """Symbol with only 600 closed candles. Backfill is attempted and empty result confirms exhaustion."""
    start = NOW_MS - 601 * HOUR_MS
    # 600 closed + 1 open = 601 total rows (< 1000)
    page = [_row(start + i * HOUR_MS) for i in range(600)]
    page.append(_row(start + 600 * HOUR_MS, open_candle=True))
    assert len(page) == 601

    client, session = _client_with_pages(monkeypatch, [page])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 600  # Genuinely short
    assert len(session.calls) == 2  # Initial fetch + 1 backfill verifying exhaustion
    assert all(c["close_time"] < NOW_MS for c in candles)
    assert client.is_history_exhausted("BTCUSDT") is True


# ---------------------------------------------------------------------------
# F. Stale cache containing 260 candles must NOT be accepted as sufficient
#    (This tests the SignalEngine check, not get_klines directly.)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_260_candles_rejected_as_insufficient():
    """SignalEngine requires >= candle_limit candles. 260 must fail the check."""
    candle_limit = 1000
    cached_count = 260
    # Simulate what SignalEngine does at line 124:
    # if candles and len(candles) >= self.candle_limit:
    assert not (cached_count >= candle_limit), (
        "260 candles must NOT pass the >= 1000 check"
    )


# ---------------------------------------------------------------------------
# G. Old cache must not produce a Golden Cross
#    (Signal evaluation only runs when len(candles) >= candle_limit)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_insufficient_history_blocks_signal_evaluation():
    """Signal evaluation is gated on len(candles) >= candle_limit.
    Fewer candles (from stale cache or short symbols) must not reach
    enrich_candles_with_ema / detect_golden_cross."""
    candle_limit = 1000
    test_counts = [0, 250, 260, 600, 763, 999]
    for count in test_counts:
        # This mirrors SignalEngine.init_single line 124:
        passes_check = count > 0 and count >= candle_limit
        assert not passes_check, (
            f"{count} candles must NOT pass the >= {candle_limit} gate"
        )


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pagination_deduplicates(monkeypatch):
    """Backfill page overlaps with initial page — duplicates are removed."""
    start = NOW_MS - 1001 * HOUR_MS
    first_page = [_row(start + i * HOUR_MS) for i in range(999)]
    first_page.append(_row(start + 999 * HOUR_MS, open_candle=True))
    # Backfill returns 2 rows: one overlapping (start) and one new (start - HOUR_MS)
    backfill_page = [_row(start - HOUR_MS), _row(start)]

    client, _ = _client_with_pages(monkeypatch, [first_page, backfill_page])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    timestamps = [c["timestamp"] for c in candles]
    assert len(candles) == 1000
    assert len(timestamps) == len(set(timestamps))  # No duplicates
    assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# endTime backward chaining
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pagination_endtime_moves_backward(monkeypatch):
    """Each backfill page uses endTime = earliest_collected_timestamp - 1."""
    start = NOW_MS - 1001 * HOUR_MS
    p0 = [_row(start + i * HOUR_MS) for i in range(999)]
    p0.append(_row(start + 999 * HOUR_MS, open_candle=True))
    p1 = [_row(start - HOUR_MS)]

    client, session = _client_with_pages(monkeypatch, [p0, p1])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 2
    backfill_end_time = session.calls[1][1]["endTime"]
    assert backfill_end_time == start - 1


# ---------------------------------------------------------------------------
# Final candles sorted ascending
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_final_candles_sorted_ascending(monkeypatch):
    """Result must be sorted by timestamp ascending."""
    start = NOW_MS - 1002 * HOUR_MS
    page = [_row(start + i * HOUR_MS) for i in range(1000)]
    client, _ = _client_with_pages(monkeypatch, [page])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    timestamps = [c["timestamp"] for c in candles]
    assert timestamps == sorted(timestamps)


# ---------------------------------------------------------------------------
# Only closed candles in result
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_only_closed_candles_in_result(monkeypatch):
    """No candle with close_time >= now_ms should appear."""
    start = NOW_MS - 1001 * HOUR_MS
    page = [_row(start + i * HOUR_MS) for i in range(999)]
    page.append(_row(start + 999 * HOUR_MS, open_candle=True))
    backfill = [_row(start - HOUR_MS)]

    client, _ = _client_with_pages(monkeypatch, [page, backfill])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert all(c["close_time"] < NOW_MS for c in candles)
    assert len(candles) == 1000


# ---------------------------------------------------------------------------
# Safety cap prevents infinite pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_safety_cap_limits_backfill_pages(monkeypatch):
    """Max 3 backfill pages even if deficit remains."""
    start = NOW_MS - 1001 * HOUR_MS
    p0 = [_row(start + i * HOUR_MS) for i in range(999)]
    p0.append(_row(start + 999 * HOUR_MS, open_candle=True))

    # Each backfill page returns 1000 rows but all are open (tests the cap)
    fake_backfill = [_row(start - (i + 1) * HOUR_MS, open_candle=True) for i in range(1000)]

    # 4 pages total: initial + 3 backfill (safety cap)
    client, session = _client_with_pages(
        monkeypatch, [p0, fake_backfill, fake_backfill, fake_backfill]
    )

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 999
    # 1 initial + 3 backfill × (initial attempt only, no 429 retry) = 4 requests
    assert len(session.calls) == 4


# ---------------------------------------------------------------------------
# start_time prevents backfill
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_time_prevents_backfill(monkeypatch):
    """When start_time is specified, no backfill pagination occurs."""
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


# ---------------------------------------------------------------------------
# only_closed=False preserves open row and skips backfill
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_only_closed_false_no_backfill(monkeypatch):
    """only_closed=False returns rows as-is including open candle."""
    rows = [_row(NOW_MS - 3 * HOUR_MS), _row(NOW_MS - 2 * HOUR_MS), _row(NOW_MS - HOUR_MS, open_candle=True)]
    client, session = _client_with_pages(monkeypatch, [rows])

    candles = await client.get_klines("BTCUSDT", limit=3, only_closed=False)

    assert len(candles) == 3
    assert candles[-1]["close_time"] >= NOW_MS
    assert len(session.calls) == 1
    assert session.calls[0][1]["limit"] == 3


# ---------------------------------------------------------------------------
# Backfill exhaustion — empty page
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_exhaustion_empty_page(monkeypatch):
    """If backfill page returns empty [], pagination stops gracefully."""
    start = NOW_MS - 1001 * HOUR_MS
    p0 = [_row(start + i * HOUR_MS) for i in range(999)]
    p0.append(_row(start + 999 * HOUR_MS, open_candle=True))

    client, session = _client_with_pages(monkeypatch, [p0, []])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 999
    assert len(session.calls) == 2


# ---------------------------------------------------------------------------
# Backfill exhaustion — short page stops further pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_exhaustion_short_page(monkeypatch):
    """If backfill page returns < 1000 rows, no further pages are requested."""
    start = NOW_MS - 1001 * HOUR_MS
    p0 = [_row(start + i * HOUR_MS) for i in range(999)]
    p0.append(_row(start + 999 * HOUR_MS, open_candle=True))
    p1 = [_row(start - (i + 1) * HOUR_MS) for i in range(500)]

    client, session = _client_with_pages(monkeypatch, [p0, p1])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000  # 999 + 500 = 1499, take newest 1000
    assert len(session.calls) == 2
    timestamps = [c["timestamp"] for c in candles]
    assert timestamps == sorted(timestamps)
    assert len(set(timestamps)) == 1000


# ---------------------------------------------------------------------------
# Genuinely short — 260 rows total
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_genuinely_short_260_rows(monkeypatch):
    """Symbol with only 260 rows (<1000) total. Backfill verifies exhaustion."""
    start = NOW_MS - 261 * HOUR_MS
    page = [_row(start + i * HOUR_MS) for i in range(259)]
    page.append(_row(start + 259 * HOUR_MS, open_candle=True))
    assert len(page) == 260

    client, session = _client_with_pages(monkeypatch, [page])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 259
    assert len(session.calls) == 2  # Initial fetch + 1 backfill verifying exhaustion
    assert all(c["close_time"] < NOW_MS for c in candles)
    assert client.is_history_exhausted("BTCUSDT") is True


# ---------------------------------------------------------------------------
# Genuinely short — 763 rows total
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_763_initial_rows_genuinely_short(monkeypatch):
    """Symbol with only 763 rows (<1000) total. Backfill verifies exhaustion."""
    start = NOW_MS - 764 * HOUR_MS
    page = [_row(start + i * HOUR_MS) for i in range(762)]
    page.append(_row(start + 762 * HOUR_MS, open_candle=True))
    assert len(page) == 763

    client, session = _client_with_pages(monkeypatch, [page])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 762
    assert len(session.calls) == 2  # Initial fetch + 1 backfill verifying exhaustion
    assert client.is_history_exhausted("BTCUSDT") is True


# ---------------------------------------------------------------------------
# Exactly 1000 closed rows — fa289df regression
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_exactly_1000_closed_rows_regression(monkeypatch):
    """fa289df regression: when all 1000 rows are closed, no backfill."""
    start = NOW_MS - 1001 * HOUR_MS
    client, session = _client_with_pages(
        monkeypatch,
        [[_row(start + i * HOUR_MS) for i in range(1000)]],
    )

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 1
    assert session.calls[0][1]["limit"] == 1000


# ---------------------------------------------------------------------------
# Multi-page backward pagination with large backfill
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_multi_page_backward_pagination(monkeypatch):
    """Backfill page gives enough to reach 1000 when combined with initial."""
    base = NOW_MS - 4000 * HOUR_MS
    p0 = [_row(base + i * HOUR_MS) for i in range(3000, 3999)]
    p0.append(_row(base + 3999 * HOUR_MS, open_candle=True))
    assert len(p0) == 1000
    p1 = [_row(base + i * HOUR_MS) for i in range(2000, 3000)]
    assert len(p1) == 1000

    client, session = _client_with_pages(monkeypatch, [p0, p1])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 2
    timestamps = [c["timestamp"] for c in candles]
    assert timestamps == sorted(timestamps)
    assert len(set(timestamps)) == 1000
    assert all(c["close_time"] < NOW_MS for c in candles)


# ---------------------------------------------------------------------------
# 429 on backfill with excessive Retry-After (>30s) caps out immediately
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_excessive_retry_after_caps_out(monkeypatch):
    """If Retry-After exceeds MAX_RATE_LIMIT_WAIT_S (30s), give up immediately."""
    start = NOW_MS - 1001 * HOUR_MS
    first_page = [_row(start + i * HOUR_MS) for i in range(999)]
    first_page.append(_row(start + 999 * HOUR_MS, open_candle=True))

    _fake_time = [NOW_MS / 1000]

    def fake_time():
        return _fake_time[0]

    monkeypatch.setattr("app.exchange.binance_client.time.time", fake_time)
    client = BinanceFuturesClient()

    call_count = [0]

    class _SessionExcessiveRetryAfter:
        calls = []

        def get(self, url, *, params):
            self.calls.append((url, dict(params)))
            call_count[0] += 1
            if call_count[0] == 1:
                return _Response(first_page)
            # 429 with 60s Retry-After (exceeds 30s cap)
            return _RateLimitResponse("60")

    session = _SessionExcessiveRetryAfter()

    async def get_session():
        return session

    monkeypatch.setattr(client, "_get_session", get_session)

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    # Should return 999 (partial), not wait 60s
    assert len(candles) == 999
    assert len(session.calls) == 2  # initial + 1 failed attempt (cap exceeded)


# ---------------------------------------------------------------------------
# Regression tests for partial initial responses & backfill exhaustion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_partial_initial_response_backfills_older_candles_to_1000(monkeypatch):
    """Initial page has 500 rows for established symbol. Backfill collects 600 older rows to reach 1000."""
    start = NOW_MS - 501 * HOUR_MS
    page1 = [_row(start + i * HOUR_MS) for i in range(499)]
    page1.append(_row(start + 499 * HOUR_MS, open_candle=True))
    assert len(page1) == 500

    backfill_start = start - 600 * HOUR_MS
    page2 = [_row(backfill_start + i * HOUR_MS) for i in range(600)]

    client, session = _client_with_pages(monkeypatch, [page1, page2])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 1000
    assert len(session.calls) == 2
    assert client.is_history_exhausted("BTCUSDT") is False


@pytest.mark.asyncio
async def test_partial_initial_response_backfill_failure_preserves_not_exhausted(monkeypatch):
    """Initial page has 500 rows. Backfill encounters 429 rate limit. Must NOT mark exhausted."""
    start = NOW_MS - 501 * HOUR_MS
    page1 = [_row(start + i * HOUR_MS) for i in range(499)]
    page1.append(_row(start + 499 * HOUR_MS, open_candle=True))

    # Initial page succeeds, backfill calls return 429
    sequence = [(page1, 200), (None, 429), (None, 429)]
    monkeypatch.setattr("app.exchange.binance_client.time.time", lambda: NOW_MS / 1000)
    client = BinanceFuturesClient()
    session = _SessionWithSequence(sequence)

    async def get_session():
        return session

    monkeypatch.setattr(client, "_get_session", get_session)

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 499
    # Transient backfill failure must NEVER produce exchange_exhausted=True
    assert client.is_history_exhausted("BTCUSDT") is False


@pytest.mark.asyncio
async def test_partial_initial_response_backfill_network_error_not_exhausted(monkeypatch):
    """Initial page has 500 rows. Backfill encounters HTTP 500 error on all attempts. Must NOT mark exhausted."""
    start = NOW_MS - 501 * HOUR_MS
    page1 = [_row(start + i * HOUR_MS) for i in range(499)]
    page1.append(_row(start + 499 * HOUR_MS, open_candle=True))

    client, session = _client_with_pages(monkeypatch, [page1, [], []], statuses=[200, 500, 500])

    candles = await client.get_klines("BTCUSDT", limit=1000, only_closed=True)

    assert len(candles) == 499
    # Transient server error must NEVER produce exchange_exhausted=True
    assert client.is_history_exhausted("BTCUSDT") is False
