import React, { useState } from 'react';
import { Search, ChevronRight, Check } from 'lucide-react';
import { SymbolItem, formatPrice } from '../api';

interface SymbolSelectorProps {
  symbols: SymbolItem[];
  selectedSymbol: string;
  onSelect: (symbol: string) => void;
}

export const SymbolSelector: React.FC<SymbolSelectorProps> = ({
  symbols,
  selectedSymbol,
  onSelect,
}) => {
  const [search, setSearch] = useState('');

  const filtered = symbols.filter((s) =>
    s.symbol.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="glass-panel" style={{ borderRadius: '8px', padding: '16px', display: 'flex', flexDirection: 'column', gap: '12px', height: '100%' }}>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ fontSize: '12px', fontWeight: 700, letterSpacing: '0.06em', color: '#FFFFFF', textTransform: 'uppercase' }}>
          Monitored Universe
        </span>
        <span style={{ fontSize: '11px', color: 'var(--text-dim)', fontFamily: 'JetBrains Mono' }}>
          {symbols.length} ASSETS
        </span>
      </div>

      {/* Search Input */}
      <div style={{ position: 'relative' }}>
        <Search
          size={14}
          color="var(--text-dim)"
          style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)' }}
        />
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Filter symbol (e.g. BTC)..."
          style={{
            width: '100%',
            background: 'rgba(255,255,255,0.03)',
            border: '1px solid var(--border-subtle)',
            borderRadius: '6px',
            padding: '7px 10px 7px 32px',
            color: '#FFFFFF',
            fontSize: '12px',
            outline: 'none',
          }}
        />
      </div>

      {/* Symbol List */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', overflowY: 'auto', maxHeight: '380px', paddingRight: '4px' }}>
        {filtered.map((item) => {
          const isSelected = item.symbol === selectedSymbol;
          return (
            <button
              key={item.symbol}
              onClick={() => onSelect(item.symbol)}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: '9px 12px',
                borderRadius: '6px',
                background: isSelected ? 'rgba(0, 229, 255, 0.12)' : 'rgba(255,255,255,0.015)',
                border: isSelected ? '1px solid var(--color-cyan)' : '1px solid transparent',
                cursor: 'pointer',
                textAlign: 'left',
                transition: 'all 0.15s ease',
              }}
            >
              <div>
                <div style={{
                  fontSize: '12.5px',
                  fontWeight: isSelected ? 700 : 600,
                  color: isSelected ? 'var(--color-cyan)' : '#FFFFFF',
                  fontFamily: 'JetBrains Mono',
                }}>
                  {item.symbol}
                </div>
                <div style={{ fontSize: '10px', color: 'var(--text-dim)' }}>
                  BINANCE 1H PERP
                </div>
              </div>

              <div style={{ textAlign: 'right', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ fontSize: '12px', color: isSelected ? '#FFFFFF' : 'var(--text-muted)', fontFamily: 'JetBrains Mono' }}>
                  {formatPrice(item.last_close)}
                </span>
                {isSelected ? (
                  <Check size={14} color="var(--color-cyan)" />
                ) : (
                  <ChevronRight size={14} color="var(--text-dim)" />
                )}
              </div>
            </button>
          );
        })}

        {filtered.length === 0 && (
          <div style={{ padding: '24px 0', textAlign: 'center', color: 'var(--text-dim)', fontSize: '12px' }}>
            No matching symbols found.
          </div>
        )}
      </div>
    </div>
  );
};
