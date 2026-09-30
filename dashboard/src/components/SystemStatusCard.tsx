import React from 'react';
import { Server, Zap } from 'lucide-react';
import { SystemStatus } from '../api';

interface SystemStatusCardProps {
  status: SystemStatus | null;
}

export const SystemStatusCard: React.FC<SystemStatusCardProps> = ({ status }) => {
  return (
    <div className="glass-panel" style={{ borderRadius: '8px', padding: '16px', display: 'flex', flexDirection: 'column', gap: '14px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-subtle)', paddingBottom: '10px' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Server size={16} color="var(--color-cyan)" />
          <span style={{ fontSize: '12px', fontWeight: 700, letterSpacing: '0.06em', color: '#FFFFFF', textTransform: 'uppercase' }}>
            Live System Status
          </span>
        </div>
        <span style={{ fontSize: '11px', color: 'var(--text-dim)', fontFamily: 'JetBrains Mono' }}>
          UPTIME: {status?.uptime || '00:00:00'}
        </span>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '10px' }}>
        {/* BINANCE */}
        <div style={{ background: 'rgba(255,255,255,0.02)', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
          <div style={{ fontSize: '10.5px', color: 'var(--text-dim)', fontWeight: 600 }}>BINANCE</div>
          <div style={{ fontSize: '12.5px', fontWeight: 700, color: 'var(--color-green)', display: 'flex', alignItems: 'center', gap: '6px', marginTop: '4px' }}>
            <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--color-green)' }}></span>
            {status?.binance || 'ONLINE'}
          </div>
        </div>

        {/* WEBSOCKET */}
        <div style={{ background: 'rgba(255,255,255,0.02)', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
          <div style={{ fontSize: '10.5px', color: 'var(--text-dim)', fontWeight: 600 }}>WEBSOCKET</div>
          <div style={{
            fontSize: '12.5px',
            fontWeight: 700,
            color: status?.websocket === 'CONNECTED' ? 'var(--color-green)' : 'var(--color-gold)',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            marginTop: '4px',
          }}>
            <span style={{
              width: '6px',
              height: '6px',
              borderRadius: '50%',
              background: status?.websocket === 'CONNECTED' ? 'var(--color-green)' : 'var(--color-gold)',
            }}></span>
            {status?.websocket || 'CONNECTING'}
          </div>
        </div>

        {/* DATABASE */}
        <div style={{ background: 'rgba(255,255,255,0.02)', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
          <div style={{ fontSize: '10.5px', color: 'var(--text-dim)', fontWeight: 600 }}>DATABASE</div>
          <div style={{ fontSize: '12.5px', fontWeight: 700, color: 'var(--color-green)', display: 'flex', alignItems: 'center', gap: '6px', marginTop: '4px' }}>
            <span style={{ width: '6px', height: '6px', borderRadius: '50%', background: 'var(--color-green)' }}></span>
            {status?.database || 'ONLINE'}
          </div>
        </div>

        {/* TELEGRAM */}
        <div style={{ background: 'rgba(255,255,255,0.02)', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
          <div style={{ fontSize: '10.5px', color: 'var(--text-dim)', fontWeight: 600 }}>TELEGRAM</div>
          <div style={{
            fontSize: '11.5px',
            fontWeight: 700,
            color: status?.telegram === 'ONLINE' ? 'var(--color-green)' : 'var(--color-gold)',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            marginTop: '4px',
          }}>
            <span style={{
              width: '6px',
              height: '6px',
              borderRadius: '50%',
              background: status?.telegram === 'ONLINE' ? 'var(--color-green)' : 'var(--color-gold)',
            }}></span>
            {status?.telegram === 'ONLINE' ? 'ONLINE' : 'CONFIG MISSING'}
          </div>
        </div>
      </div>

      {/* Numerical Metrics: Clearly separating Live Alerts from Historical Crosses */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '6px', borderTop: '1px solid var(--border-subtle)', paddingTop: '10px' }}>
        <div>
          <span style={{ fontSize: '9.5px', color: 'var(--text-dim)', display: 'block' }}>SYMBOLS</span>
          <span style={{ fontSize: '13px', fontWeight: 700, color: '#FFFFFF', fontFamily: 'JetBrains Mono' }}>
            {status?.symbols_total ?? 0}
          </span>
        </div>

        <div>
          <span style={{ fontSize: '9.5px', color: 'var(--text-dim)', display: 'block' }}>INIT</span>
          <span style={{ fontSize: '13px', fontWeight: 700, color: 'var(--color-green)', fontFamily: 'JetBrains Mono' }}>
            {status?.symbols_initialized ?? 0}/{status?.symbols_total ?? 0}
          </span>
        </div>

        <div>
          <span style={{ fontSize: '9.5px', color: 'var(--color-green)', display: 'block' }}>LIVE ALERTS</span>
          <span style={{ fontSize: '13px', fontWeight: 700, color: 'var(--color-green)', fontFamily: 'JetBrains Mono' }}>
            {status?.live_signals_count ?? 0}
          </span>
        </div>

        <div>
          <span style={{ fontSize: '9.5px', color: 'var(--color-cyan)', display: 'block' }}>HISTORICAL</span>
          <span style={{ fontSize: '13px', fontWeight: 700, color: 'var(--color-cyan)', fontFamily: 'JetBrains Mono' }}>
            {status?.historical_crosses_count ?? status?.golden_crosses_total ?? 0}
          </span>
        </div>
      </div>

      {/* Last Signal Banner */}
      <div style={{
        background: 'rgba(0, 229, 255, 0.05)',
        border: '1px solid rgba(0, 229, 255, 0.2)',
        borderRadius: '6px',
        padding: '10px 12px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Zap size={15} color="var(--color-cyan)" />
          <div>
            <div style={{ fontSize: '10px', color: 'var(--text-dim)', fontWeight: 600 }}>LAST SIGNAL</div>
            <div style={{ fontSize: '12px', fontWeight: 700, color: '#FFFFFF', fontFamily: 'JetBrains Mono' }}>
              {status?.last_signal?.symbol ? `${status.last_signal.symbol} (1H)` : 'AWAITING EVENT'}
            </div>
          </div>
        </div>
        {status?.last_signal?.signal_time_utc && (
          <span style={{ fontSize: '10.5px', color: 'var(--color-cyan)', fontFamily: 'JetBrains Mono' }}>
            {status.last_signal.signal_time_utc}
          </span>
        )}
      </div>
    </div>
  );
};
