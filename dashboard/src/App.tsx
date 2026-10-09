import React, { useEffect, useState, useCallback } from 'react';
import { Header } from './components/Header';
import { SystemStatusCard } from './components/SystemStatusCard';
import { SymbolSelector } from './components/SymbolSelector';
import { MarketChart } from './components/MarketChart';
import { CrossHistory } from './components/CrossHistory';
import {
  fetchSystemStatus,
  fetchSymbols,
  fetchChartData,
  fetchRecentSignals,
  SystemStatus,
  SymbolItem,
  ChartResponse,
  RecentSignal,
} from './api';

export const App: React.FC = () => {
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [symbols, setSymbols] = useState<SymbolItem[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState<string>('BTCUSDT');
  const [chartData, setChartData] = useState<ChartResponse | null>(null);
  const [recentSignals, setRecentSignals] = useState<RecentSignal[]>([]);
  const [selectedTimestamp, setSelectedTimestamp] = useState<number | undefined>(undefined);
  const [isLoadingChart, setIsLoadingChart] = useState<boolean>(false);
  const [isRefreshing, setIsRefreshing] = useState<boolean>(false);

  // Load Status and Signals periodically
  const loadGlobalMeta = useCallback(async () => {
    try {
      const [statusRes, symbolsRes, signalsRes] = await Promise.all([
        fetchSystemStatus(),
        fetchSymbols(),
        fetchRecentSignals(),
      ]);
      setStatus(statusRes);
      setSymbols(symbolsRes);
      setRecentSignals(signalsRes);
    } catch (err) {
      console.error('Error fetching global metadata:', err);
    }
  }, []);

  // Load Chart Data for Selected Symbol
  const loadChart = useCallback(
    async (sym: string, targetTs?: number) => {
      setIsLoadingChart(true);
      try {
        const data = await fetchChartData(sym, '1h', targetTs);
        setChartData(data);
      } catch (err) {
        console.error(`Failed to load chart for ${sym}:`, err);
      } finally {
        setIsLoadingChart(false);
      }
    },
    []
  );

  // Initial load
  useEffect(() => {
    loadGlobalMeta();
    loadChart(selectedSymbol);

    // Auto-refresh interval (every 10 seconds)
    const interval = setInterval(() => {
      loadGlobalMeta();
    }, 10000);

    return () => clearInterval(interval);
  }, [loadGlobalMeta, loadChart]);

  // When selected symbol changes
  const handleSelectSymbol = (sym: string) => {
    setSelectedSymbol(sym);
    setSelectedTimestamp(undefined);
    loadChart(sym);
  };

  // When user clicks a Golden Cross event in history table
  const handleSelectEvent = (sym: string, timestamp: number) => {
    setSelectedSymbol(sym);
    setSelectedTimestamp(timestamp);
    loadChart(sym, timestamp);
  };

  const handleManualRefresh = async () => {
    setIsRefreshing(true);
    await Promise.all([loadGlobalMeta(), loadChart(selectedSymbol, selectedTimestamp)]);
    setIsRefreshing(false);
  };

  return (
    <div style={{ minHeight: '100vh', display: 'flex', flexDirection: 'column', backgroundColor: 'var(--bg-app)' }}>
      {/* Top Header */}
      <Header status={status} onRefresh={handleManualRefresh} isRefreshing={isRefreshing} />

      {/* Main Workspace */}
      <main style={{ flex: 1, padding: '16px 24px', display: 'flex', flexDirection: 'column', gap: '16px' }}>
        {/* Main Grid: Sidebar + Center Chart */}
        <div style={{
          display: 'grid',
          gridTemplateColumns: 'minmax(280px, 320px) 1fr',
          gap: '16px',
          alignItems: 'start',
        }}>
          {/* Left Column: Live Status & Symbol Selector */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            <SystemStatusCard status={status} />
            <SymbolSelector
              symbols={symbols}
              selectedSymbol={selectedSymbol}
              onSelect={handleSelectSymbol}
            />
          </div>

          {/* Right Column: Interactive TradingView Chart */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            <MarketChart
              chartData={chartData}
              isLoading={isLoadingChart}
              selectedTimestamp={selectedTimestamp}
            />
          </div>
        </div>

        {/* Bottom Section: Recent Golden Crosses Table */}
        <CrossHistory
          signals={recentSignals}
          onSelectEvent={handleSelectEvent}
          fastPeriod={status?.ema_fast}
          slowPeriod={status?.ema_slow}
        />
      </main>

      {/* Institutional Footer */}
      <footer style={{
        padding: '12px 24px',
        borderTop: '1px solid var(--border-subtle)',
        fontSize: '11px',
        color: 'var(--text-dim)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        background: 'rgba(6, 8, 14, 0.95)',
      }}>
        <div>
          NEXORA EMA CROSS v1.0.0 • PROPRIETARY QUANTITATIVE MARKET MONITORING
        </div>
        <div style={{ display: 'flex', gap: '16px' }}>
          <span>PORT: 8080</span>
          <span>DATA: BINANCE USD-M FUTURES</span>
          <span>TIMEFRAME: 1H CLOSED</span>
        </div>
      </footer>
    </div>
  );
};
