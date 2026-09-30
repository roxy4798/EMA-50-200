# NEXORA live WebSocket diagnostic

## Result

The production VPS has not been accessed in this task. The live WebSocket issue is therefore **not accepted as fixed**, and deployment is not verified as safe. No deployment, process restart, `.env` edit, production database access, or Telegram delivery was performed.

The local workstation reproduces the key symptom: DNS, TCP 443, TLS SNI, and the raw Binance USD-M Futures WebSocket handshake pass, but no application kline arrives. This does not identify whether the cause is the VPS network, an upstream route or Binance edge, or another environment-specific condition.

## Local evidence (not VPS evidence)

Diagnostic run on 2026-09-30:

```text
HOST=fstream.binance.com
PYTHON=3.13.12 WEBSOCKETS=17.0.1
PROXY_CONFIGURED=False PROXY_TYPES=none
DNS_IPV4=52.195.24.86
DNS_IPV6=NO_AAAA
TCP_443_IPv4=PASS
TLS_SNI_IPv4=PASS version=TLSv1.3 cipher=TLS_AES_128_GCM_SHA256
SINGLE_FUTURES_URL=wss://fstream.binance.com/ws/btcusdt@kline_1h
SINGLE_WS_HANDSHAKE=PASS
SINGLE_KLINE=FAIL no application kline received
SKIP_COMBINED_AND_527: single stream did not prove a closed candle reached SignalEngine
```

Separate local probes previously established WebSocket handshakes for the combined Futures 5-symbol URL and a diagnostic 527-symbol combined URL, with no application message in the short probe window. Production manager configuration partitions 527 symbols into six sockets sized `100, 100, 100, 100, 100, 27`. The six-worker live test and closed callback acceptance could not be proven from this host because the single stream is silent. Spot delivered a kline locally, but Spot was only a diagnostic comparison and was not used for signal data.

## Changes prepared

- Added receive-timeout recovery so a connected but silent socket is logged and reconnected.
- Added message receipt timestamps/counters and first-message/first-kline logs, plus `ws_connected` health visibility.
- Preserved combined-stream wrapper parsing, 1H-only filtering, closed-candle gating, and the existing SignalEngine callback/strategy.
- Added regression coverage for raw and combined closed candles, open-candle rejection, six-batch partitioning, and silent-socket reconnects.
- Added [`diagnose_live_websocket.py`](diagnose_live_websocket.py), a read-only staged probe intended to run on the VPS. It uses a temporary SQLite database, warms the real SignalEngine from Futures REST, and stops before the combined/full-universe stages unless a closed BTCUSDT candle reaches the actual engine callback.

## Validation

- `python -m pytest -q`: **67 passed**, 15 existing dependency deprecation warnings.
- `python -m compileall -q app tests run.py verify_system.py live_audit_test.py diagnose_live_websocket.py`: passed.
- `git diff --check`: passed (Git reported only its LF-to-CRLF working-copy notice).

## VPS acceptance still required

Run from the deployed project directory on the VPS:

```powershell
python diagnose_live_websocket.py
```

The default run can wait up to 65 minutes for the next hourly close. It reports runtime/library versions, proxy presence without values, DNS, IPv4/IPv6 TCP/TLS, raw single-stream data, a closed-candle `SignalEngine` callback, combined five-symbol data, and all six production-sized workers. Share its complete stdout to identify the VPS-side cause and determine whether deployment is safe. Do not use a Spot endpoint for signals. No REST fallback has been implemented; its design and request-weight budget must be evaluated only if VPS evidence shows the Futures stream is unavailable.

## Endpoint reference

The official Binance USDⓈ-M Futures stream documentation describes raw `/ws/` and combined `/stream?streams=` connections and the combined payload wrapper: [Live subscribing/unsubscribing to streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams). No alternate official production market-stream endpoint was established by the probes in this task.
