import React from 'react';
import { History, ExternalLink } from 'lucide-react';
import { RecentSignal, formatPrice } from '../api';

interface CrossHistoryProps {
  signals: RecentSignal[];
  onSelectEvent: (symbol: string, timestamp: number) => void;
  selectedSignalId?: number;
}

export const CrossHistory: React.FC<CrossHistoryProps> = ({
  signals,
  onSelectEvent,
  selectedSignalId,
}) => {
  return (
    <div className="glass-panel" style={{ borderRadius: '8px', padding: '16px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-subtle)', paddingBottom: '10px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <History size={16} color="var(--color-green)" />
          <span style={{ fontSize: '12px', fontWeight: 700, letterSpacing: '0.06em', color: '#FFFFFF', textTransform: 'uppercase' }}>
            Recent Golden Cross Events
          </span>
        </div>
        <span style={{ fontSize: '11px', color: 'var(--text-dim)', fontFamily: 'JetBrains Mono' }}>
          CLICK TO CENTER CHART
        </span>
      </div>

      <div style={{ overflowX: 'auto', maxHeight: '240px' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px', textAlign: 'left' }}>
          <thead>
            <tr style={{ color: 'var(--text-dim)', borderBottom: '1px solid var(--border-subtle)', fontSize: '11px' }}>
              <th style={{ padding: '8px 10px', fontWeight: 600 }}>SYMBOL</th>
              <th style={{ padding: '8px 10px', fontWeight: 600 }}>TIMEFRAME</th>
              <th style={{ padding: '8px 10px', fontWeight: 600 }}>EVENT TIME (UTC)</th>
              <th style={{ padding: '8px 10px', fontWeight: 600, textAlign: 'right' }}>CLOSE PRICE</th>
              <th style={{ padding: '8px 10px', fontWeight: 600, textAlign: 'right' }}>EMA 50</th>
              <th style={{ padding: '8px 10px', fontWeight: 600, textAlign: 'right' }}>EMA 200</th>
              <th style={{ padding: '8px 10px', fontWeight: 600, textAlign: 'center' }}>CHART</th>
            </tr>
          </thead>
          <tbody>
            {signals.map((sig) => {
              const isSelected = sig.id === selectedSignalId;
              return (
                <tr
                  key={sig.id}
                  onClick={() => onSelectEvent(sig.symbol, sig.candle_timestamp)}
                  style={{
                    borderBottom: '1px solid rgba(255,255,255,0.02)',
                    cursor: 'pointer',
                    background: isSelected ? 'rgba(0, 229, 255, 0.08)' : 'transparent',
                    transition: 'background 0.15s ease',
                  }}
                  onMouseEnter={(e) => {
                    if (!isSelected) e.currentTarget.style.background = 'rgba(255,255,255,0.02)';
                  }}
                  onMouseLeave={(e) => {
                    if (!isSelected) e.currentTarget.style.background = 'transparent';
                  }}
                >
                  <td style={{ padding: '9px 10px', fontWeight: 700, color: 'var(--color-cyan)', fontFamily: 'JetBrains Mono' }}>
                    {sig.symbol}
                  </td>
                  <td style={{ padding: '9px 10px', color: 'var(--text-muted)' }}>
                    1H CLOSED
                  </td>
                  <td style={{ padding: '9px 10px', color: '#FFFFFF', fontFamily: 'JetBrains Mono' }}>
                    {sig.signal_time_utc}
                  </td>
                  <td style={{ padding: '9px 10px', textAlign: 'right', color: '#FFFFFF', fontFamily: 'JetBrains Mono', fontWeight: 600 }}>
                    {formatPrice(sig.close_price)}
                  </td>
                  <td style={{ padding: '9px 10px', textAlign: 'right', color: 'var(--color-cyan)', fontFamily: 'JetBrains Mono' }}>
                    {formatPrice(sig.ema50)}
                  </td>
                  <td style={{ padding: '9px 10px', textAlign: 'right', color: 'var(--color-gold)', fontFamily: 'JetBrains Mono' }}>
                    {formatPrice(sig.ema200)}
                  </td>
                  <td style={{ padding: '9px 10px', textAlign: 'center' }}>
                    <span className="badge-cyan" style={{ fontSize: '10.5px', padding: '3px 8px', borderRadius: '4px', display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
                      <ExternalLink size={11} /> VIEW
                    </span>
                  </td>
                </tr>
              );
            })}

            {signals.length === 0 && (
              <tr>
                <td colSpan={7} style={{ padding: '24px', textAlign: 'center', color: 'var(--text-dim)' }}>
                  Awaiting Golden Cross events on closed 1H candles...
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
