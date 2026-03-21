import { useState } from 'react';
import Inventory from './Inventory';
import FBAShipments from './FBAShipments';
import ItemPlanning from './ItemPlanning';
import ItemMaster from './ItemMaster';

const B = {
  b1:'#1B4F8A', b2:'#2E6FBB', b3:'#5B9FD4',
  o2:'#E8821E', t2:'#1AA392', gold:'#F5B731',
  brd:'#1a2f4a',
};

const SUB_TABS = [
  { key: 'fba-inventory',  label: 'FBA Inventory',  accent: B.t2 },
  { key: 'fba-shipments',  label: 'FBA Shipments',  accent: B.b2 },
  { key: 'forecasting',    label: 'Forecasting',    accent: B.gold },
  { key: 'item-master',    label: 'Item Master',    accent: B.b3 },
];

export default function FBACombined({ filters = {} }) {
  const [active, setActive] = useState('fba-inventory');
  const activeTab = SUB_TABS.find(t => t.key === active) || SUB_TABS[0];

  return (
    <div>
      {/* ── Sub-tab bar ── */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: 2,
        marginBottom: 16,
        borderBottom: '1px solid var(--brd)',
        paddingBottom: 0,
        overflowX: 'auto',
      }}>
        {SUB_TABS.map((tab, i) => {
          const isActive = active === tab.key;
          return (
            <button
              key={tab.key}
              onClick={() => setActive(tab.key)}
              style={{
                padding: '8px 18px',
                fontSize: 11,
                fontWeight: isActive ? 700 : 500,
                fontFamily: "'Space Grotesk', monospace",
                letterSpacing: '.04em',
                border: 'none',
                borderBottom: isActive ? `3px solid ${tab.accent}` : '3px solid transparent',
                borderRadius: 0,
                background: 'transparent',
                color: isActive ? 'var(--txt)' : 'var(--txt3)',
                cursor: 'pointer',
                transition: 'all .15s',
                whiteSpace: 'nowrap',
                marginBottom: -1,
              }}
            >
              {tab.label}
            </button>
          );
        })}
        {/* accent indicator */}
        <div style={{
          marginLeft: 'auto',
          fontSize: 9,
          fontWeight: 700,
          textTransform: 'uppercase',
          letterSpacing: '.1em',
          padding: '3px 10px',
          borderRadius: 99,
          background: `${activeTab.accent}22`,
          color: activeTab.accent,
          border: `1px solid ${activeTab.accent}44`,
          whiteSpace: 'nowrap',
          flexShrink: 0,
        }}>
          {activeTab.label}
        </div>
      </div>

      {/* ── Sub-tab content ── */}
      {active === 'fba-inventory' && <Inventory filters={filters} />}
      {active === 'fba-shipments' && <FBAShipments filters={filters} />}
      {active === 'forecasting'   && <ItemPlanning />}
      {active === 'item-master'   && <ItemMaster />}
    </div>
  );
}
