import React from 'react';
import { Activity, Radio, ShieldCheck, RefreshCw, Layers } from 'lucide-react';
import { SystemStatus } from '../api';

interface HeaderProps {
  status: SystemStatus | null;
  onRefresh: () => void;
  isRefreshing: boolean;
}

export const Header: React.FC<HeaderProps> = ({ status, onRefresh, isRefreshing }) => {
  return (
    <header className="glass-panel" style={{ borderBottom: '1px solid var(--border-subtle)', padding: '12px 24px' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '16px' }}>
        {/* Brand & Subtitle */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '14px' }}>
          <div style={{
            width: '38px',
            height: '38px',
            borderRadius: '8px',
            background: 'linear-gradient(135deg, #00E5FF 0%, #007799 100%)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 0 16px rgba(0,229,255,0.3)',
          }}>
            <Activity size={22} color="#080C14" strokeWidth={2.5} />
          </div>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <h1 style={{ fontSize: '18px', fontWeight: 800, letterSpacing: '0.04em', color: '#FFFFFF' }}>
                NEXORA <span style={{ color: 'var(--color-cyan)' }}>EMA CROSS</span>
              </h1>
              <span className="badge-green" style={{ fontSize: '11px', fontWeight: 700, padding: '2px 8px', borderRadius: '4px', display: 'flex', alignItems: 'center', gap: '5px' }}>
                <span className="pulse-indicator"></span> SYSTEM LIVE
              </span>
            </div>
            <p style={{ fontSize: '11.5px', color: 'var(--text-muted)', letterSpacing: '0.02em', marginTop: '2px' }}>
              BINANCE USD-M FUTURES • 1H GOLDEN CROSS VISUALIZATION ENGINE
            </p>
          </div>
        </div>

        {/* Global Live Badges */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '12px', color: 'var(--text-muted)', background: 'rgba(255,255,255,0.02)', padding: '6px 12px', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
            <Radio size={14} color={status?.websocket === 'CONNECTED' ? 'var(--color-green)' : 'var(--color-gold)'} />
            <span>STREAM:</span>
            <span style={{ fontWeight: 600, color: status?.websocket === 'CONNECTED' ? 'var(--color-green)' : 'var(--color-gold)' }}>
              {status?.websocket || 'CONNECTING'}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '12px', color: 'var(--text-muted)', background: 'rgba(255,255,255,0.02)', padding: '6px 12px', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
            <Layers size={14} color="var(--color-cyan)" />
            <span>SYMBOLS:</span>
            <span style={{ fontWeight: 600, color: '#FFFFFF' }}>
              {status?.symbols_initialized ?? 0} / {status?.symbols_total ?? 0}
            </span>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '12px', color: 'var(--text-muted)', background: 'rgba(255,255,255,0.02)', padding: '6px 12px', borderRadius: '6px', border: '1px solid var(--border-subtle)' }}>
            <ShieldCheck size={14} color="var(--color-cyan)" />
            <span>CROSSES:</span>
            <span style={{ fontWeight: 700, color: 'var(--color-cyan)' }}>
              {status?.golden_crosses_total ?? 0}
            </span>
          </div>

          <button
            onClick={onRefresh}
            className="btn-nexora"
            title="Refresh Market Data"
            disabled={isRefreshing}
          >
            <RefreshCw size={14} className={isRefreshing ? 'animate-spin' : ''} />
            <span>SYNC</span>
          </button>
        </div>
      </div>
    </header>
  );
};
