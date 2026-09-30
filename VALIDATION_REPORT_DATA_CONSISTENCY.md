# NEXORA EMA Data Consistency Validation

Date: 2026-09-30

## Root cause

The chart renderer used a static “GOLDEN CROSS CONFIRMED” banner, even for a current market overview whose displayed EMA50 was below EMA200. `/symbol` could read a different in-memory or cached candle frame from the chart's fresh Binance request. The engine also recalculated from a growing 300-candle window while current views used 250 candles. Historical signal rows did not retain the previous-candle EMA pair, so a later chart could not verify the original crossover using the same event data.

## Logic changed

- Current `/symbol` and chart views fetch closed candles from Binance USD-M Futures REST and use the same 250-candle EMA50/EMA200 frame. They show market data unavailable instead of presenting cached data as current when REST has no data.
- Historical event charts use the signal row's crossover-candle EMA values and previous-candle EMA values. The renderer verifies `previous EMA50 <= previous EMA200` and `current EMA50 > current EMA200`; invalid or unverifiable stored events fail closed and log `CHART_DATA_INCONSISTENCY` with symbol, timeframe, timestamp, and EMA values.
- SQLite now stores the previous EMA pair for new signals and migrates existing tables. Signal creation remains the same EMA50/EMA200 Golden Cross definition; database writes reject invalid timeframe, current EMA relationship, or a supplied previous pair that did not meet the crossing condition.
- REST and WebSocket clients are constrained to Binance USD-M Futures 1H. REST closed-candle selection checks Binance candle close time. WebSocket signal input requires a valid subscribed symbol, `1h`, and `k.x == true`; stale/out-of-order or insufficiently warmed engine frames cannot generate live signals.
- Health reports separate connection state, last valid kline receipt, and last closed 1H candle. Connected streams with no messages report `DATA STALE / NO MARKET DATA`. No Spot fallback was added.
- No RSI, MACD, ADX, ATR, volume, slope, cooldown, or other strategy filter was added.

## SAFEUSDT reproduction

Source: Binance USD-M Futures REST `/fapi/v1/klines`, SAFEUSDT, 1H, 250 closed candles, processed with `app.indicators.ema.enrich_candles_with_ema`.

| Candle | UTC open time | OHLC | EMA50 | EMA200 |
|---|---|---|---:|---:|
| Previous | 2026-09-30 11:00 | 0.11152 / 0.11199 / 0.11096 / 0.11142 | 0.1096199601 | 0.1099894574 |
| Latest closed | 2026-09-30 12:00 | 0.11137 / 0.11557 / 0.11089 / 0.11497 | 0.1098297656 | 0.1100390151 |

Golden Cross result: **false**. Previous EMA50 was below EMA200 and latest EMA50 remains below EMA200, so SAFEUSDT current structure is **BEARISH**. The old chart values match the Futures calculation but its static status label was wrong. The fixed current chart and `/symbol` display the latest closed candle's bearish relationship; a historical event chart uses that event candle's stored EMA pair.

## WebSocket result

Regression test result: when a socket is marked connected and no kline has arrived, health is `DATA STALE / NO MARKET DATA`; a fresh valid kline changes health to `HEALTHY`. Spot REST and Spot WebSocket hosts are rejected. VPS connectivity was not exercised, and nothing was deployed.

## Validation

- `pytest -q`: **62 passed** (15 warnings, including existing dependency deprecations and unclosed test client-session warnings).
- `python -m compileall -q app tests run.py verify_system.py live_audit_test.py`: **PASS**.
- `python verify_system.py`: **PASS** (Binance Futures REST, closed candles, chart, historical scan, alert queue).
- `python live_audit_test.py`: **PASS**, all 8 audit phases (live Futures REST, historical event/chart marker, duplicate handling, rate-limit checks, Telegram formatting, recovery).
- Golden Cross strategy remains Binance USD-M Futures USDT perpetuals, 1H, EMA50/EMA200, closed candle only, with the original previous-`<=` and current-`>` definition.

## Commit

Implementation and regression tests: `4bf32bd` (`fix: enforce Binance Futures EMA data consistency`).
