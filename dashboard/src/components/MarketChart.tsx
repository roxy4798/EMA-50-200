import React, { useEffect, useRef, useState } from 'react';
import {
  createChart,
  IChartApi,
  ISeriesApi,
  ColorType,
  CrosshairMode,
  Time,
} from 'lightweight-charts';
import { Download, Maximize2, CheckCircle2 } from 'lucide-react';
import { ChartResponse, formatPrice } from '../api';

interface MarketChartProps {
  chartData: ChartResponse | null;
  isLoading: boolean;
  selectedTimestamp?: number;
}

export const MarketChart: React.FC<MarketChartProps> = ({
  chartData,
  isLoading,
  selectedTimestamp,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const ema50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ema200SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  // Inspector state when hovering crosshair
  const [hoveredData, setHoveredData] = useState<{
    time: string;
    open: number;
    high: number;
    low: number;
    close: number;
    ema50?: number | null;
    ema200?: number | null;
  } | null>(null);

  // Initialize and mount chart
  useEffect(() => {
    if (!containerRef.current) return;

    // Create TradingView Lightweight Charts instance
    const chart = createChart(containerRef.current, {
      width: containerRef.current.clientWidth,
      height: 520,
      layout: {
        background: { type: ColorType.Solid, color: '#090D15' },
        textColor: '#94A3B8',
        fontSize: 12,
        fontFamily: "'JetBrains Mono', 'Inter', monospace",
      },
      grid: {
        vertLines: { color: 'rgba(30, 41, 59, 0.4)', style: 2 },
        horzLines: { color: 'rgba(30, 41, 59, 0.4)', style: 2 },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: '#38BDF8',
          width: 1,
          style: 3,
          labelBackgroundColor: '#0E1726',
        },
        horzLine: {
          color: '#38BDF8',
          width: 1,
          style: 3,
          labelBackgroundColor: '#0E1726',
        },
      },
      rightPriceScale: {
        borderColor: '#1E293B',
        scaleMargins: { top: 0.12, bottom: 0.12 },
        autoScale: true,
      },
      timeScale: {
        borderColor: '#1E293B',
        timeVisible: true,
        secondsVisible: false,
      },
    });

    // Add Candlestick Series
    const candleSeries = chart.addCandlestickSeries({
      upColor: '#00E676',
      downColor: '#FF3366',
      borderVisible: true,
      borderUpColor: '#00E676',
      borderDownColor: '#FF3366',
      wickUpColor: '#00E676',
      wickDownColor: '#FF3366',
    });

    // Add EMA 50 Line Series
    const ema50Series = chart.addLineSeries({
      color: '#00E5FF',
      lineWidth: 2,
      priceLineVisible: false,
      title: 'EMA 50',
    });

    // Add EMA 200 Line Series
    const ema200Series = chart.addLineSeries({
      color: '#FFA000',
      lineWidth: 2,
      priceLineVisible: false,
      title: 'EMA 200',
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    ema50SeriesRef.current = ema50Series;
    ema200SeriesRef.current = ema200Series;

    // Crosshair hover inspection
    chart.subscribeCrosshairMove((param) => {
      if (!param || !param.time || !param.seriesData) {
        setHoveredData(null);
        return;
      }

      const candleVal = param.seriesData.get(candleSeries) as any;
      const ema50Val = param.seriesData.get(ema50Series) as any;
      const ema200Val = param.seriesData.get(ema200Series) as any;

      if (candleVal) {
        const dateObj = new Date((param.time as number) * 1000);
        const timeStr = dateObj.toUTCString().replace('GMT', 'UTC');
        setHoveredData({
          time: timeStr,
          open: candleVal.open,
          high: candleVal.high,
          low: candleVal.low,
          close: candleVal.close,
          ema50: ema50Val ? ema50Val.value : null,
          ema200: ema200Val ? ema200Val.value : null,
        });
      }
    });

    // Responsive resize observer
    const handleResize = () => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: containerRef.current.clientWidth,
        });
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
      chartRef.current = null;
    };
  }, []);

  // Update data when chartData changes
  useEffect(() => {
    if (!chartData || !chartRef.current || !candleSeriesRef.current) return;

    const candleSeries = candleSeriesRef.current;
    const ema50Series = ema50SeriesRef.current;
    const ema200Series = ema200SeriesRef.current;

    const fastPeriod = chartData?.fast_period ?? 50;
    const slowPeriod = chartData?.slow_period ?? 200;

    // Format for Lightweight charts (time must be strictly ascending)
    const candles = chartData.candles.map((c) => ({
      time: c.time as Time,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
    }));
    candleSeries.setData(candles);

    const fastData = chartData.ema_fast || chartData.ema50;
    if (ema50Series && fastData) {
      ema50Series.applyOptions({ title: `EMA ${fastPeriod}` });
      const emaFast = fastData.map((e) => ({
        time: e.time as Time,
        value: e.value,
      }));
      ema50Series.setData(emaFast);
    }

    const slowData = chartData.ema_slow || chartData.ema200;
    if (ema200Series && slowData) {
      ema200Series.applyOptions({ title: `EMA ${slowPeriod}` });
      const emaSlow = slowData.map((e) => ({
        time: e.time as Time,
        value: e.value,
      }));
      ema200Series.setData(emaSlow);
    }

    // Set Golden Cross Markers
    if (chartData.cross_markers && chartData.cross_markers.length > 0) {
      const markers = chartData.cross_markers.map((m) => ({
        time: m.time as Time,
        position: 'belowBar' as const,
        color: '#00E5FF',
        shape: 'arrowUp' as const,
        text: '● GOLDEN CROSS',
      }));
      candleSeries.setMarkers(markers);
    } else {
      candleSeries.setMarkers([]);
    }

    // Fit content or center on selected timestamp
    if (selectedTimestamp) {
      const sec = Math.floor(selectedTimestamp / 1000);
      chartRef.current.timeScale().setVisibleRange({
        from: (sec - 80 * 3600) as Time,
        to: (sec + 25 * 3600) as Time,
      });
    } else {
      chartRef.current.timeScale().fitContent();
    }
  }, [chartData, selectedTimestamp]);

  const fastPeriod = chartData?.fast_period ?? 50;
  const slowPeriod = chartData?.slow_period ?? 200;
  const latest = chartData?.latest;
  const activeEmaFast = hoveredData?.ema50 ?? (latest?.ema_fast ?? latest?.ema50);
  const activeEmaSlow = hoveredData?.ema200 ?? (latest?.ema_slow ?? latest?.ema200);
  const activeClose = hoveredData?.close ?? latest?.close;

  const handleDownloadPNG = () => {
    if (!chartData?.symbol) return;
    const url = `/api/chart-image/${chartData.symbol}${selectedTimestamp ? `?target_timestamp=${selectedTimestamp}` : ''}`;
    window.open(url, '_blank');
  };

  return (
    <div className="glass-panel" style={{ borderRadius: '8px', padding: '18px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
      {/* 1. CHART HEADER (Exact Spec from Section 6) */}
      <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px', borderBottom: '1px solid var(--border-subtle)', paddingBottom: '14px' }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <h2 style={{ fontSize: '18px', fontWeight: 800, color: '#FFFFFF', letterSpacing: '0.03em' }}>
              NEXORA EMA CROSS
            </h2>
            <span className="badge-cyan" style={{ fontSize: '11px', fontWeight: 700, padding: '2px 8px', borderRadius: '4px' }}>
              {chartData?.symbol || 'SELECT SYMBOL'} • BINANCE USD-M FUTURES
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginTop: '6px', fontSize: '12px' }}>
            <span style={{ color: 'var(--text-muted)' }}>
              TIMEFRAME: <strong style={{ color: '#FFFFFF' }}>1H</strong>
            </span>
            <span style={{ color: 'var(--text-dim)' }}>|</span>
            <span style={{ color: 'var(--text-muted)' }}>
              SIGNAL: <strong style={{ color: 'var(--color-cyan)' }}>EMA{fastPeriod} CROSS ABOVE EMA{slowPeriod}</strong>
            </span>
            <span style={{ color: 'var(--text-dim)' }}>|</span>
            <span style={{ color: 'var(--color-green)', display: 'flex', alignItems: 'center', gap: '4px', fontWeight: 700 }}>
              <CheckCircle2 size={13} /> STATUS: GOLDEN CROSS CONFIRMED
            </span>
          </div>
        </div>

        {/* Action Controls */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <button
            onClick={() => chartRef.current?.timeScale().fitContent()}
            className="btn-nexora"
            title="Reset Chart Zoom"
          >
            <Maximize2 size={13} />
            <span>RESET</span>
          </button>
          <button
            onClick={handleDownloadPNG}
            className="btn-nexora btn-nexora-primary"
            title="Download 1600x900 Telegram Chart Image"
          >
            <Download size={13} />
            <span>EXPORT HD PNG</span>
          </button>
        </div>
      </div>

      {/* 2. COMPACT INFORMATION PANEL (Exact Spec from Section 7) */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))',
        gap: '10px',
        background: 'rgba(255,255,255,0.015)',
        border: '1px solid var(--border-subtle)',
        borderRadius: '6px',
        padding: '10px 14px',
      }}>
        <div>
          <div style={{ fontSize: '10px', color: 'var(--text-dim)', fontWeight: 600 }}>SYMBOL</div>
          <div style={{ fontSize: '13px', fontWeight: 700, color: '#FFFFFF', fontFamily: 'JetBrains Mono' }}>
            {chartData?.symbol || '-'}
          </div>
        </div>

        <div>
          <div style={{ fontSize: '10px', color: 'var(--text-dim)', fontWeight: 600 }}>TIMEFRAME</div>
          <div style={{ fontSize: '13px', fontWeight: 700, color: '#FFFFFF', fontFamily: 'JetBrains Mono' }}>
            1H CLOSED
          </div>
        </div>

        <div>
          <div style={{ fontSize: '10px', color: 'var(--text-dim)', fontWeight: 600 }}>CLOSE PRICE</div>
          <div style={{ fontSize: '13px', fontWeight: 700, color: '#FFFFFF', fontFamily: 'JetBrains Mono' }}>
            {formatPrice(activeClose)}
          </div>
        </div>

        <div>
          <div style={{ fontSize: '10px', color: 'var(--color-cyan)', fontWeight: 600 }}>EMA {fastPeriod}</div>
          <div style={{ fontSize: '13px', fontWeight: 700, color: 'var(--color-cyan)', fontFamily: 'JetBrains Mono' }}>
            {formatPrice(activeEmaFast)}
          </div>
        </div>

        <div>
          <div style={{ fontSize: '10px', color: 'var(--color-gold)', fontWeight: 600 }}>EMA {slowPeriod}</div>
          <div style={{ fontSize: '13px', fontWeight: 700, color: 'var(--color-gold)', fontFamily: 'JetBrains Mono' }}>
            {formatPrice(activeEmaSlow)}
          </div>
        </div>

        <div>
          <div style={{ fontSize: '10px', color: 'var(--color-green)', fontWeight: 600 }}>CANDLE STATUS</div>
          <div style={{ fontSize: '13px', fontWeight: 700, color: 'var(--color-green)' }}>
            CLOSED
          </div>
        </div>
      </div>

      {/* 3. CANDLESTICK & EMA INSPECTION BAR */}
      {hoveredData && (
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '16px',
          fontSize: '11px',
          fontFamily: 'JetBrains Mono',
          color: 'var(--text-muted)',
          background: 'rgba(0, 229, 255, 0.04)',
          padding: '6px 12px',
          borderRadius: '4px',
          border: '1px solid rgba(0, 229, 255, 0.15)',
        }}>
          <span style={{ color: '#FFFFFF' }}>TIME: {hoveredData.time}</span>
          <span>O: <strong style={{ color: '#FFFFFF' }}>{formatPrice(hoveredData.open)}</strong></span>
          <span>H: <strong style={{ color: '#00E676' }}>{formatPrice(hoveredData.high)}</strong></span>
          <span>L: <strong style={{ color: '#FF3366' }}>{formatPrice(hoveredData.low)}</strong></span>
          <span>C: <strong style={{ color: '#FFFFFF' }}>{formatPrice(hoveredData.close)}</strong></span>
          {activeEmaFast && (
            <span>EMA{fastPeriod}: <strong style={{ color: 'var(--color-cyan)' }}>{formatPrice(activeEmaFast)}</strong></span>
          )}
          {activeEmaSlow && (
            <span>EMA{slowPeriod}: <strong style={{ color: 'var(--color-gold)' }}>{formatPrice(activeEmaSlow)}</strong></span>
          )}
        </div>
      )}

      {/* 4. INTERACTIVE CHART CANVAS */}
      <div style={{ position: 'relative', width: '100%', minHeight: '520px' }}>
        {isLoading && (
          <div style={{
            position: 'absolute',
            inset: 0,
            background: 'rgba(8, 11, 17, 0.8)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 10,
            backdropFilter: 'blur(4px)',
          }}>
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '10px' }}>
              <div className="pulse-indicator" style={{ width: '14px', height: '14px' }}></div>
              <span style={{ fontSize: '13px', fontWeight: 600, color: 'var(--color-cyan)', fontFamily: 'JetBrains Mono' }}>
                FETCHING BINANCE 1H REAL MARKET DATA...
              </span>
            </div>
          </div>
        )}

        <div ref={containerRef} style={{ width: '100%', height: '520px', borderRadius: '6px', overflow: 'hidden' }} />
      </div>

      {/* 5. LEGEND & COMPLIANCE FOOTER */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: '11px', color: 'var(--text-dim)', borderTop: '1px solid var(--border-subtle)', paddingTop: '10px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ width: '12px', height: '3px', background: 'var(--color-cyan)', display: 'inline-block' }}></span>
            <span style={{ color: 'var(--text-muted)' }}>EMA {fastPeriod} (Fast)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ width: '12px', height: '3px', background: 'var(--color-gold)', display: 'inline-block' }}></span>
            <span style={{ color: 'var(--text-muted)' }}>EMA {slowPeriod} (Slow)</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ color: 'var(--color-cyan)' }}>▲</span>
            <span style={{ color: 'var(--text-muted)' }}>Golden Cross Point</span>
          </div>
        </div>

        <div style={{ fontStyle: 'italic' }}>
          * Informational Only • Closed 1H Candles • Strict Zero Trading Execution Info
        </div>
      </div>
    </div>
  );
};
