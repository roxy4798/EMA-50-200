export interface SystemStatus {
  binance: string;
  websocket: string;
  database: string;
  telegram: string;
  symbols_total: number;
  symbols_initialized: number;
  golden_crosses_total: number;
  live_signals_count?: number;
  historical_crosses_count?: number;
  candles_closed_count: number;
  last_signal: {
    symbol: string;
    signal_time_utc: string;
    candle_timestamp: number;
    close_price: number;
    ema50?: number;
    ema200?: number;
  } | null;
  uptime: string;
  timeframe: string;
  ema_fast: number;
  ema_slow: number;
}

export interface SymbolItem {
  symbol: string;
  initialized: boolean;
  last_close: number | null;
  timeframe: string;
}

export interface CandleData {
  time: number; // Unix seconds
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface LineData {
  time: number;
  value: number;
}

export interface CrossMarker {
  time: number;
  timestamp_ms: number;
  price: number;
  ema50?: number;
  ema200?: number;
  signal_time_utc?: string;
  text: string;
  position: 'aboveBar' | 'belowBar' | 'inBar';
  shape: 'circle' | 'square' | 'arrowUp' | 'arrowDown';
  color: string;
}

export interface ChartResponse {
  symbol: string;
  timeframe: string;
  candles: CandleData[];
  ema50: LineData[];
  ema200: LineData[];
  cross_markers: CrossMarker[];
  latest: {
    symbol: string;
    timeframe: string;
    close: number;
    open: number;
    high: number;
    low: number;
    volume: number;
    ema50: number | null;
    ema200: number | null;
    timestamp: number;
    total_candles: number;
  };
}

export interface RecentSignal {
  id: number;
  symbol: string;
  timeframe: string;
  candle_timestamp: number;
  signal_time_utc: string;
  ema50: number;
  ema200: number;
  close_price: number;
  chart_image_path: string | null;
  telegram_sent: number;
}

const API_BASE = '/api';

export async function fetchSystemStatus(): Promise<SystemStatus> {
  const res = await fetch(`${API_BASE}/status`);
  if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
  return res.json();
}

export async function fetchSymbols(): Promise<SymbolItem[]> {
  const res = await fetch(`${API_BASE}/symbols`);
  if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
  const data = await res.json();
  return data.symbols || [];
}

export async function fetchChartData(
  symbol: string,
  timeframe = '1h',
  targetTimestamp?: number
): Promise<ChartResponse> {
  let url = `${API_BASE}/chart/${symbol}?timeframe=${timeframe}&limit=150`;
  if (targetTimestamp) {
    url += `&target_timestamp=${targetTimestamp}`;
  }
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to load chart data for ${symbol}`);
  return res.json();
}

export async function fetchRecentSignals(): Promise<RecentSignal[]> {
  const res = await fetch(`${API_BASE}/signals/recent?limit=25`);
  if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
  return res.json();
}

export function formatPrice(val: number | null | undefined): string {
  if (val === null || val === undefined || isNaN(val)) return '-';
  if (val >= 1000) {
    return val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  } else if (val >= 1) {
    return val.toLocaleString('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 4 });
  } else {
    return val.toLocaleString('en-US', { minimumFractionDigits: 6, maximumFractionDigits: 6 });
  }
}
