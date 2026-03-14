import { useState, useEffect, useRef, useMemo } from "react";
import { api } from "../lib/api";

// ── Status helpers ─────────────────────────────────────────────────────────────
const SC = {
  delivered: { col: '#2ECFAA', label: 'Delivered',  bg: 'rgba(46,207,170,.14)'  },
  transit:   { col: '#E87830', label: 'In Transit', bg: 'rgba(232,120,48,.14)'  },
  booked:    { col: 'var(--acc3)', label: 'Booked', bg: 'rgba(62,101,140,.18)'  },
  open:      { col: 'var(--txt3)', label: 'Open',   bg: 'rgba(141,174,200,.10)' },
};

function normStatus(s) {
  if (!s || s === '—') return 'open';
  const l = s.toLowerCase();
  if (l === 'delivered') return 'delivered';
  if (l.includes('transit')) return 'transit';
  if (l === 'booked') return 'booked';
  return 'open';
}

function fmtNum(n) {
  if (n == null || n === '—') return '—';
  return Number(n).toLocaleString();
}

function fmtDate(iso) {
  if (!iso || iso === '—') return '—';
  try {
    const parts = iso.split('-');
    if (parts.length === 3) {
      return `${parseInt(parts[1])}/${parseInt(parts[2])}/${parts[0].slice(2)}`;
    }
    // Already MM/DD/YY or similar
    return iso;
  } catch { return iso; }
}

function countdown(etaISO, status) {
  if (status === 'delivered') return { text: '✓ Del.', color: '#2ECFAA' };
  if (status === 'open')      return { text: 'Open',   color: 'var(--txt3)' };
  if (!etaISO || etaISO === '—') return null;
  try {
    const d = Math.round((new Date(etaISO + 'T00:00:00') - new Date()) / 864e5);
    if (d < 0)   return { text: `${Math.abs(d)}d late`, color: '#f87171' };
    if (d === 0) return { text: 'Today',               color: '#f87171' };
    if (d <= 7)  return { text: `${d}d`,               color: '#f87171' };
    if (d <= 14) return { text: `${d}d`,               color: '#fb923c' };
    return       { text: `${d}d`,                      color: '#2ECFAA' };
  } catch { return null; }
}

function StatusBadge({ status }) {
  const s = SC[status] || SC.open;
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 5,
      padding: '2px 8px', borderRadius: 99, fontSize: 10, fontWeight: 700,
      whiteSpace: 'nowrap', fontFamily: "'Space Grotesk',monospace",
      background: s.bg, color: s.col,
    }}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: s.col, flexShrink: 0 }} />
      {s.label}
    </span>
  );
}

function CdBadge({ text, color }) {
  const bg = color === '#2ECFAA' ? 'rgba(46,207,170,.12)'
           : color === '#fb923c' ? 'rgba(251,146,60,.15)'
           : 'rgba(248,113,113,.15)';
  return (
    <span style={{
      fontSize: 9, fontWeight: 700, padding: '1px 6px', borderRadius: 99,
      fontFamily: "'Space Grotesk',monospace", color, background: bg,
    }}>{text}</span>
  );
}

function StatusCell({ eta, status }) {
  const s   = SC[status] || SC.open;
  const cd  = countdown(eta, status);
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5, whiteSpace: 'nowrap' }}>
      <span style={{ width: 7, height: 7, borderRadius: '50%', background: s.col, flexShrink: 0 }} />
      <span style={{ fontSize: 10, fontWeight: 700, color: s.col }}>{fmtDate(eta)}</span>
      {cd && cd.text !== 'Open' && <CdBadge text={cd.text} color={cd.color} />}
    </div>
  );
}

// ── Sort ───────────────────────────────────────────────────────────────────────
const DATE_COLS = new Set(['container_eta','etd','actual_delivery','eta_delivery','eta']);

function sortRows(rows, key, dir) {
  return [...rows].sort((a, b) => {
    let av = a[key], bv = b[key];
    if (DATE_COLS.has(key)) {
      const toT = v => (v && v !== '—') ? new Date(v + 'T00:00:00').getTime() : 9e12;
      return dir * (toT(av) - toT(bv));
    }
    if (typeof av === 'number' && typeof bv === 'number') return dir * (av - bv);
    return dir * String(av || '').toLowerCase().localeCompare(String(bv || '').toLowerCase());
  });
}

function SortTh({ label, col, view, sortState, onSort, align, minW }) {
  const { key, dir } = sortState[view] || {};
  const active = key === col;
  return (
    <th onClick={() => onSort(view, col)} style={{
      fontFamily: "'Space Grotesk',monospace", fontSize: 9, fontWeight: 700,
      textTransform: 'uppercase', letterSpacing: '.08em',
      color: active ? 'var(--acc3)' : 'var(--txt3)',
      padding: '8px 12px', textAlign: align || 'left',
      borderBottom: '1px solid var(--brd)', whiteSpace: 'nowrap',
      background: 'var(--card2)', cursor: 'pointer', userSelect: 'none',
      minWidth: minW,
    }}>
      {label}{active ? (dir === 1 ? ' ▲' : ' ▼') : ''}
    </th>
  );
}

const TH0 = { // static th style
  fontFamily: "'Space Grotesk',monospace", fontSize: 9, fontWeight: 700,
  textTransform: 'uppercase', letterSpacing: '.08em', color: 'var(--txt3)',
  padding: '8px 12px', borderBottom: '1px solid var(--brd)',
  background: 'var(--card2)', whiteSpace: 'nowrap',
};
const TD0 = { // static td style
  fontFamily: "'Sora',sans-serif", fontSize: 10, color: 'var(--txt2)',
  padding: '6px 8px', borderBottom: '1px solid var(--brd)',
  whiteSpace: 'nowrap', verticalAlign: 'middle',
};

// ── Gantt ──────────────────────────────────────────────────────────────────────
const JAN_T = new Date('2026-01-01T00:00:00').getTime();
const JUN_T = new Date('2026-06-30T00:00:00').getTime();
const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun'];

function datePct(iso) {
  if (!iso || iso === '—') return null;
  try {
    const t = new Date(iso + 'T00:00:00').getTime();
    return Math.max(0, Math.min(1, (t - JAN_T) / (JUN_T - JAN_T)));
  } catch { return null; }
}

function GanttChart({ containers }) {
  const W = 1060, LW = 130, SW = 150, CW = W - LW - SW;
  const RH = 30, PT = 24;
  const rows = containers.filter(c => c.container_number && c.container_number !== '—').slice(0, 10);
  const H = PT + rows.length * RH + 16;
  const cx = r => LW + r * CW;
  const todayR = Math.max(0, Math.min(1, (Date.now() - JAN_T) / (JUN_T - JAN_T)));

  return (
    <div style={{ background: 'var(--surf)', border: '1px solid var(--brd)', borderRadius: 14, padding: 16, marginBottom: 14 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
        <span style={{ fontFamily: "'Sora',sans-serif", fontSize: 13, fontWeight: 700, color: 'var(--txt)' }}>
          Container Timeline — Jan to Jun 2026
        </span>
        <div style={{ display: 'flex', gap: 14 }}>
          {[['Delivered','#2ECFAA'],['In Transit','#E87830'],['Open','var(--txt3)']].map(([l,c]) => (
            <div key={l} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: 'var(--txt3)', fontFamily: "'Sora',sans-serif" }}>
              <div style={{ width: 8, height: 8, borderRadius: '50%', background: c }} />{l}
            </div>
          ))}
        </div>
      </div>
      <div style={{ overflowX: 'auto' }}>
        <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ display: 'block', overflow: 'visible' }}>
          <defs>
            {rows.map((_, i) => (
              <filter key={i} id={`scg${i}`} x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="2.5" result="b"/>
                <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
              </filter>
            ))}
          </defs>
          {/* Header */}
          <rect x={0} y={0} width={W} height={PT} fill="var(--card2)" />
          <text x={10} y={PT-8} fontSize={9} fontWeight={700} fill="var(--txt3)" letterSpacing="0.08em">CONTAINER #</text>
          {MONTHS.map((m, i) => {
            const midX = cx(i/6) + (cx((i+1)/6) - cx(i/6)) / 2;
            return <text key={m} x={midX.toFixed(1)} y={PT-8} textAnchor="middle" fontSize={9} fontWeight={700} fill="var(--txt3)" letterSpacing="0.08em">{m.toUpperCase()}</text>;
          })}
          <text x={(W-SW+10).toFixed(1)} y={PT-8} fontSize={9} fontWeight={700} fill="var(--txt3)" letterSpacing="0.08em">STATUS</text>
          {/* Vertical separators */}
          {[0,1,2,3,4,5,6].map(i => (
            <line key={i} x1={cx(i/6).toFixed(1)} y1={PT} x2={cx(i/6).toFixed(1)} y2={H-16} stroke="var(--brd)" strokeWidth={0.5}/>
          ))}
          <line x1={LW} y1={0} x2={LW} y2={H} stroke="var(--brd2)" strokeWidth={0.75}/>
          <line x1={W-SW} y1={0} x2={W-SW} y2={H} stroke="var(--brd2)" strokeWidth={0.75}/>
          {/* Today line */}
          <line x1={cx(todayR).toFixed(1)} y1={PT} x2={cx(todayR).toFixed(1)} y2={H-16} stroke="#E87830" strokeWidth={1.5} strokeDasharray="3 3" opacity={0.5}/>
          <text x={cx(todayR).toFixed(1)} y={PT-2} textAnchor="middle" fontSize={8} fill="#E87830" opacity={0.65}>▼</text>
          {/* Rows */}
          {rows.map((c, i) => {
            const status  = normStatus(c.status);
            const col     = SC[status]?.col || 'var(--txt3)';
            const y       = PT + i * RH;
            const dotY    = (y + RH/2).toFixed(1);
            const ep      = datePct(c.etd);
            const ap      = datePct(c.container_eta);
            const dp      = c.actual_delivery ? datePct(c.actual_delivery) : null;
            const cid     = c.container_number.slice(0, 11);
            const dispD   = c.actual_delivery && c.actual_delivery !== '—' ? fmtDate(c.actual_delivery) : fmtDate(c.container_eta);
            const cd      = countdown(c.container_eta, status);
            const cdColor = cd ? cd.color : 'var(--txt3)';
            const barW    = ep !== null && ap !== null ? Math.max(4, cx(ap) - cx(ep)) : 0;
            return (
              <g key={i}>
                <rect x={0} y={y} width={W} height={RH} fill={i%2===0 ? 'var(--card2)' : 'var(--bg)'} opacity={0.55}/>
                <line x1={0} y1={y+RH} x2={W} y2={y+RH} stroke="var(--brd)" strokeWidth={0.5}/>
                <circle cx={10} cy={dotY} r={4} fill={col} filter={`url(#scg${i})`} opacity={0.9}/>
                <text x={22} y={(y+RH/2+3.5).toFixed(1)} fontSize={11} fontWeight={600} fill="var(--txt2)" fontFamily="'Space Grotesk',monospace">{cid}</text>
                {ep !== null && ap !== null ? (
                  <>
                    <rect x={cx(ep).toFixed(1)} y={(y+4).toFixed(1)} width={barW.toFixed(1)} height={(RH-8).toFixed(1)} rx={3} fill={col} opacity={0.82}/>
                    {barW > 36 && (
                      <text x={(cx(ep)+8).toFixed(1)} y={(y+RH/2+3.5).toFixed(1)} fontSize={9} fontWeight={700} fill="white" opacity={0.92}>
                        {(c.vessel || '').split(' ').slice(-1)[0]}
                      </text>
                    )}
                  </>
                ) : (
                  <line x1={(LW+6).toFixed(1)} y1={dotY} x2={(W-SW-6).toFixed(1)} y2={dotY} stroke={col} strokeWidth={1} strokeDasharray="5 5" opacity={0.25}/>
                )}
                {dp !== null && <rect x={(cx(dp)-1.25).toFixed(1)} y={(y+3).toFixed(1)} width={2.5} height={(RH-6).toFixed(1)} rx={1} fill="#fff" opacity={0.45}/>}
                <circle cx={W-SW+14} cy={dotY} r={7} fill={col} opacity={0.15}/>
                <circle cx={W-SW+14} cy={dotY} r={4.5} fill={col} opacity={0.9}/>
                <text x={W-SW+26} y={(y+RH/2+3.5).toFixed(1)} fontSize={11} fontWeight={700} fontFamily="-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
                  <tspan fill={col}>{dispD}</tspan>
                  {cd && <tspan fill={cdColor} fontSize={10} fontWeight={700} dx={6}>{cd.text}</tspan>}
                </text>
              </g>
            );
          })}
        </svg>
      </div>
      <div style={{ fontSize: 9, color: 'var(--txt3)', marginTop: 8, fontFamily: "'Sora',sans-serif" }}>
        Bar = ETD → ETA &nbsp;·&nbsp; White tick = actual delivery &nbsp;·&nbsp; ▼ = today
      </div>
    </div>
  );
}

// ── Shared card wrapper ────────────────────────────────────────────────────────
function Card({ title, stats, children }) {
  return (
    <div style={{ background: 'var(--surf)', border: '1px solid var(--brd)', borderRadius: 14, overflow: 'hidden', marginBottom: 14 }}>
      <div style={{ padding: '13px 16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid var(--brd)', flexWrap: 'wrap', gap: 8 }}>
        <span style={{ fontFamily: "'Sora',sans-serif", fontSize: 13, fontWeight: 700, color: 'var(--txt)' }}>{title}</span>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
          {stats.map((s, i) => (
            <span key={i} style={{ fontSize: 11, color: 'var(--txt3)', fontFamily: "'Sora',sans-serif" }}>
              <span style={{ color: 'var(--txt)', fontWeight: 600 }}>{s.val}</span> {s.label}
            </span>
          ))}
        </div>
      </div>
      <div style={{ overflowX: 'auto' }}>{children}</div>
    </div>
  );
}

// ── ContainerToggleBtn ─────────────────────────────────────────────────────────
function ToggleBtn({ label, open, onClick }) {
  return (
    <button onClick={onClick} style={{
      display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px',
      borderRadius: 6, fontSize: 10, fontWeight: 700, cursor: 'pointer',
      border: `1px solid ${open ? 'var(--acc3)' : 'var(--brd2)'}`,
      background: open ? 'rgba(62,101,140,.1)' : 'transparent',
      color: open ? 'var(--acc3)' : 'var(--txt3)',
      fontFamily: "'Space Grotesk',monospace", whiteSpace: 'nowrap',
    }}>
      {label} {open ? '▴' : '▾'}
    </button>
  );
}

function ContainerCell({ c, open, onToggle }) {
  const status = normStatus(c.status);
  const col    = SC[status]?.col || 'var(--txt3)';
  const label  = (c.container_number && c.container_number !== '—')
    ? c.container_number.slice(0, 12) : c.po_number;
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: col, flexShrink: 0, display: 'inline-block' }}/>
      <ToggleBtn label={label} open={open} onClick={onToggle} />
    </div>
  );
}

// ── Expand inner panel ─────────────────────────────────────────────────────────
function ExpandPanel({ title, children }) {
  return (
    <tr>
      <td colSpan={99} style={{ padding: 0, borderBottom: '1px solid var(--brd)', background: 'var(--bg)' }}>
        <div style={{ margin: '6px 8px 8px 8px', background: 'var(--card2)', borderRadius: 8, border: '1px solid var(--brd)', overflow: 'hidden' }}>
          <div style={{ padding: '6px 12px', background: 'var(--card)', borderBottom: '1px solid var(--brd)', display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.08em', color: 'var(--acc3)' }}>{title}</span>
          </div>
          {children}
        </div>
      </td>
    </tr>
  );
}

function ExpandTabs({ tabs, active, onSelect }) {
  return (
    <div style={{ display: 'flex', gap: 2, padding: 2, background: 'var(--surf)', borderRadius: 7 }}>
      {tabs.map(t => (
        <button key={t.key} onClick={() => onSelect(t.key)} style={{
          padding: '3px 10px', borderRadius: 5, fontSize: 10, fontWeight: 600,
          border: 'none', cursor: 'pointer', fontFamily: "'Sora',sans-serif",
          background: active === t.key ? 'var(--atab)' : 'transparent',
          color:      active === t.key ? '#fff'        : 'var(--txt3)',
        }}>{t.label}</button>
      ))}
    </div>
  );
}

function MiniTable({ headers, rows: dataRows }) {
  return (
    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
      <thead>
        <tr>
          {headers.map(h => (
            <th key={h} style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.08em', color: 'var(--txt3)', padding: '6px 10px', background: 'var(--card)', borderBottom: '1px solid var(--brd)', textAlign: 'left' }}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {dataRows.map((r, i) => (
          <tr key={i}>
            {r.map((cell, j) => (
              <td key={j} style={{ fontFamily: "'Sora',sans-serif", fontSize: 10, color: 'var(--txt2)', padding: '7px 10px', borderBottom: '1px solid var(--brd)', ...(typeof cell === 'object' && cell?.style ? cell.style : {}) }}>
                {typeof cell === 'object' && cell?.content !== undefined ? cell.content : cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ── Row hover helper (inline styles only) ─────────────────────────────────────
function DataRow({ children, onClick }) {
  const [hov, setHov] = useState(false);
  return (
    <tr
      style={{ background: hov ? 'rgba(62,101,140,.05)' : 'transparent', cursor: onClick ? 'pointer' : 'default' }}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      onClick={onClick}
    >{children}</tr>
  );
}

// ── TotalRow ──────────────────────────────────────────────────────────────────
function TotalRow({ label, units, cbm, colSpan = 10 }) {
  return (
    <tr style={{ background: 'var(--card2)', fontFamily: "'Space Grotesk',monospace", fontSize: 11, fontWeight: 800, color: 'var(--txt)' }}>
      <td colSpan={colSpan} style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)' }}>{label}</td>
      <td style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)', textAlign: 'right' }}>{fmtNum(units)}</td>
      <td style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)', textAlign: 'right' }}>{typeof cbm === 'number' ? cbm.toFixed(1) : cbm}</td>
      <td style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)' }}/>
    </tr>
  );
}

// ── FILTER helpers ────────────────────────────────────────────────────────────
function applyFilter(rows, statusFilter, search) {
  let r = rows;
  if (statusFilter !== 'all') r = r.filter(c => normStatus(c.status) === statusFilter);
  if (search) {
    const q = search.toLowerCase();
    r = r.filter(c => JSON.stringify(c).toLowerCase().includes(q));
  }
  return r;
}

// ─────────────────────────────────────────────────────────────────────────────
// VIEW 1: SUMMARY
// ─────────────────────────────────────────────────────────────────────────────
function SummaryView({ containers, itemsByContainer, expandedRows, setExpandedRows, sortState, onSort, statusFilter, search }) {
  const filtered = useMemo(() => {
    const f = applyFilter(containers, statusFilter, search);
    return sortRows(f, sortState.summary?.key || 'container_eta', sortState.summary?.dir || 1);
  }, [containers, statusFilter, search, sortState]);

  const tu = filtered.reduce((s, r) => s + (r.units || 0), 0);
  const tc = filtered.reduce((s, r) => s + (r.cbm || 0), 0);

  return (
    <>
      <GanttChart containers={filtered} />
      <Card
        title="All Shipments"
        stats={[
          { val: filtered.length, label: 'shipments' },
          { val: fmtNum(tu),      label: 'units'     },
          { val: tc.toFixed(1),   label: 'CBM'       },
          { val: '▾ click container to expand', label: '' },
        ]}
      >
        <table style={{ width: '100%', borderCollapse: 'collapse', tableLayout: 'auto' }}>
          <thead>
            <tr>
              <th style={{ ...TH0, minWidth: 130 }}>Container #</th>
              <SortTh label="PO #"      col="po_number"      view="summary" sortState={sortState} onSort={onSort} />
              <th style={TH0}>Invoice #</th>
              <th style={TH0}>Type</th>
              <SortTh label="ETD"       col="etd"            view="summary" sortState={sortState} onSort={onSort} />
              <th style={TH0}>ETD Port</th>
              <SortTh label="ETA"       col="container_eta"  view="summary" sortState={sortState} onSort={onSort} />
              <th style={TH0}>ETA Port</th>
              <th style={TH0}>ETA Dest.</th>
              <th style={TH0}>Destination</th>
              <SortTh label="Units" col="units" view="summary" sortState={sortState} onSort={onSort} align="right" />
              <th style={{ ...TH0, textAlign: 'right' }}>CBM</th>
              <th style={TH0}>Status</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((c, i) => {
              const key    = 'sum-' + i;
              const isOpen = !!expandedRows[key];
              const status = normStatus(c.status);
              return (
                <>
                  <DataRow key={key}>
                    <td style={{ ...TD0, minWidth: 130 }}>
                      <ContainerCell c={c} open={isOpen} onToggle={() => setExpandedRows(p => ({ ...p, [key]: isOpen ? null : 'open' }))} />
                    </td>
                    <td style={{ ...TD0, fontWeight: 700, color: 'var(--txt)' }}>{c.po_number || '—'}</td>
                    <td style={{ ...TD0, fontFamily: "'Space Grotesk',monospace" }}>{c.invoice_number || '—'}</td>
                    <td style={{ ...TD0, color: 'var(--txt3)' }}>{c.container_type || '—'}</td>
                    <td style={{ ...TD0, color: 'var(--txt3)' }}>{fmtDate(c.etd)}</td>
                    <td style={{ ...TD0, color: 'var(--txt3)' }}>{(c.etd_port || '—').split(',')[0]}</td>
                    <td style={{ ...TD0, fontWeight: 700, color: 'var(--txt)' }}>{fmtDate(c.container_eta)}</td>
                    <td style={{ ...TD0, color: 'var(--txt2)' }}>{(c.eta_port || '—').split(',')[0]}</td>
                    <td style={{ ...TD0, color: 'var(--txt3)' }}>{fmtDate(c.eta_delivery)}</td>
                    <td style={{ ...TD0, color: 'var(--txt2)' }}>{c.delivery_location || '—'}</td>
                    <td style={{ ...TD0, fontWeight: 700, color: 'var(--txt)', textAlign: 'right' }}>{fmtNum(c.units)}</td>
                    <td style={{ ...TD0, color: 'var(--txt3)', textAlign: 'right' }}>{(c.cbm || 0).toFixed(1)}</td>
                    <td style={TD0}><StatusCell eta={c.container_eta} status={status} /></td>
                  </DataRow>
                  {isOpen && (
                    <ExpandPanel key={key+'-exp'} title={`${c.container_number !== '—' ? c.container_number : c.po_number} — Shipment Details`}>
                      <MiniTable
                        headers={['HBL #','Vessel','Shipper','ETD Port','ETA Port','Comments']}
                        rows={[[
                          { content: c.hb_number || '—', style: { fontFamily: "'Space Grotesk',monospace", fontWeight: 700, color: 'var(--acc3)' } },
                          { content: c.vessel || '—', style: { color: 'var(--txt)' } },
                          c.shipper || '—',
                          c.etd_port || '—',
                          c.eta_port || '—',
                          { content: '—', style: { color: 'var(--txt3)', fontStyle: 'italic' } },
                        ]]}
                      />
                      {/* Item breakdown if available */}
                      {(itemsByContainer[c.container_number] || []).length > 0 && (
                        <div style={{ borderTop: '1px solid var(--brd)' }}>
                          <div style={{ padding: '4px 12px 2px', fontFamily: "'Space Grotesk',monospace", fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.08em', color: 'var(--txt3)', background: 'var(--card)' }}>Item Breakdown</div>
                          <MiniTable
                            headers={['SKU','Description','Units']}
                            rows={(itemsByContainer[c.container_number] || []).map(item => [
                              { content: item.sku, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                              item.description,
                              { content: fmtNum(item.units), style: { textAlign: 'right', fontWeight: 700, color: 'var(--txt)' } },
                            ])}
                          />
                        </div>
                      )}
                    </ExpandPanel>
                  )}
                </>
              );
            })}
            <TotalRow label={`TOTAL — ${filtered.length} shipments`} units={tu} cbm={tc} colSpan={10} />
          </tbody>
        </table>
      </Card>
    </>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// VIEW 2: BY CONTAINER
// ─────────────────────────────────────────────────────────────────────────────
function ContainerView({ containers, itemsByContainer, expandedRows, setExpandedRows, sortState, onSort, statusFilter, search }) {
  const filtered = useMemo(() => {
    const f = applyFilter(containers, statusFilter, search);
    return sortRows(f, sortState.container?.key || 'container_eta', sortState.container?.dir || 1);
  }, [containers, statusFilter, search, sortState]);

  const tu = filtered.reduce((s, r) => s + (r.units || 0), 0);
  const tc = filtered.reduce((s, r) => s + (r.cbm || 0), 0);

  return (
    <Card
      title="By Container"
      stats={[{ val: filtered.length, label: 'containers' }, { val: fmtNum(tu), label: 'units' }, { val: tc.toFixed(1), label: 'CBM' }]}
    >
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <SortTh label="Container #" col="container_number" view="container" sortState={sortState} onSort={onSort} minW={130} />
            <SortTh label="PO #"        col="po_number"        view="container" sortState={sortState} onSort={onSort} />
            <th style={TH0}>Invoice #</th>
            <th style={TH0}>Shipper</th>
            <th style={TH0}>HB #</th>
            <th style={TH0}>Vessel</th>
            <th style={TH0}>Type</th>
            <SortTh label="ETD"      col="etd"           view="container" sortState={sortState} onSort={onSort} />
            <SortTh label="ETA"      col="container_eta" view="container" sortState={sortState} onSort={onSort} />
            <th style={TH0}>ETA Port</th>
            <SortTh label="Delivered" col="actual_delivery" view="container" sortState={sortState} onSort={onSort} />
            <SortTh label="Units" col="units" view="container" sortState={sortState} onSort={onSort} align="right" />
            <th style={{ ...TH0, textAlign: 'right' }}>CBM</th>
            <th style={TH0}>Status</th>
            <th style={TH0}></th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((c, idx) => {
            const key    = 'ctr-' + idx;
            const exp    = expandedRows[key];
            const items  = itemsByContainer[c.container_number] || [];
            const status = normStatus(c.status);
            return (
              <>
                <DataRow key={key}>
                  <td style={{ ...TD0, fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' }}>{c.container_number}</td>
                  <td style={{ ...TD0, fontWeight: 700, color: 'var(--txt)' }}>{c.po_number || '—'}</td>
                  <td style={{ ...TD0, fontFamily: "'Space Grotesk',monospace" }}>{c.invoice_number || '—'}</td>
                  <td style={{ ...TD0, color: 'var(--txt2)' }}>{c.shipper || '—'}</td>
                  <td style={{ ...TD0, fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--txt3)' }}>{c.hb_number || '—'}</td>
                  <td style={{ ...TD0, color: 'var(--txt2)' }}>{c.vessel || '—'}</td>
                  <td style={TD0}>{c.container_type || '—'}</td>
                  <td style={{ ...TD0, color: 'var(--txt3)' }}>{fmtDate(c.etd)}</td>
                  <td style={{ ...TD0, fontWeight: 600, color: 'var(--txt)' }}>{fmtDate(c.container_eta)}</td>
                  <td style={{ ...TD0, color: 'var(--txt3)' }}>{(c.eta_port || '—').split(',')[0]}</td>
                  <td style={{ ...TD0, color: c.actual_delivery && c.actual_delivery !== '—' ? '#2ECFAA' : 'var(--txt3)' }}>{fmtDate(c.actual_delivery)}</td>
                  <td style={{ ...TD0, fontWeight: 700, textAlign: 'right' }}>{fmtNum(c.units)}</td>
                  <td style={{ ...TD0, color: 'var(--txt3)', textAlign: 'right' }}>{(c.cbm || 0).toFixed(1)}</td>
                  <td style={TD0}><StatusBadge status={status} /></td>
                  <td style={TD0}>
                    {items.length > 0 && (
                      <ToggleBtn
                        label={`${items.length} items`}
                        open={!!exp}
                        onClick={() => setExpandedRows(p => ({ ...p, [key]: exp ? null : 'items' }))}
                      />
                    )}
                  </td>
                </DataRow>
                {exp && items.length > 0 && (
                  <ExpandPanel key={key+'-exp'} title={`Item Breakdown — ${c.container_number}`}>
                    <MiniTable
                      headers={['PO #','Invoice #','SKU','Description','Units','Status']}
                      rows={items.map(item => [
                        { content: c.po_number || '—', style: { fontWeight: 700, color: 'var(--txt)' } },
                        { content: c.invoice_number || '—', style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--txt2)' } },
                        { content: item.sku, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                        { content: item.description, style: { maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis' } },
                        { content: fmtNum(item.units), style: { textAlign: 'right', fontWeight: 700, color: 'var(--txt)' } },
                        { content: <StatusBadge status={normStatus(c.status)} />, style: {} },
                      ])}
                    />
                  </ExpandPanel>
                )}
              </>
            );
          })}
          <TotalRow label="TOTAL" units={tu} cbm={tc} colSpan={11} />
        </tbody>
      </table>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// VIEW 3: BY PO
// ─────────────────────────────────────────────────────────────────────────────
function POView({ containers, itemsByContainer, expandedRows, setExpandedRows, sortState, onSort, statusFilter, search }) {
  // Build PO groups client-side from container rows
  const poGroups = useMemo(() => {
    const map = {};
    containers.forEach(c => {
      const po = c.po_number || '—';
      if (!map[po]) map[po] = { po, containers: [], eta: c.container_eta, status: c.status };
      map[po].containers.push(c);
      // Use earliest future ETA as PO ETA
      if (c.container_eta && c.container_eta !== '—') {
        if (!map[po].eta || map[po].eta === '—' || c.container_eta < map[po].eta) {
          map[po].eta = c.container_eta;
        }
      }
    });
    return Object.values(map).map(g => ({
      ...g,
      units: g.containers.reduce((s, c) => s + (c.units || 0), 0),
      cbm:   g.containers.reduce((s, c) => s + (c.cbm   || 0), 0),
      status: g.containers.every(c => normStatus(c.status) === 'delivered') ? 'delivered'
            : g.containers.some(c  => normStatus(c.status) === 'transit')   ? 'transit'
            : g.containers.every(c => normStatus(c.status) === 'open')      ? 'open'
            : 'transit',
      invoices: [...new Set(g.containers.map(c => c.invoice_number).filter(i => i && i !== '—'))],
    }));
  }, [containers]);

  const filtered = useMemo(() => {
    const f = applyFilter(poGroups.map(p => ({ ...p, container_eta: p.eta })), statusFilter, search);
    return sortRows(f, sortState.po?.key || 'eta', sortState.po?.dir || 1);
  }, [poGroups, statusFilter, search, sortState]);

  const tu = filtered.reduce((s, r) => s + (r.units || 0), 0);
  const tc = filtered.reduce((s, r) => s + (r.cbm   || 0), 0);

  return (
    <Card
      title="By PO"
      stats={[{ val: filtered.length, label: 'POs' }, { val: fmtNum(tu), label: 'units' }, { val: tc.toFixed(1), label: 'CBM' }]}
    >
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <SortTh label="PO #"    col="po"  view="po" sortState={sortState} onSort={onSort} />
            <SortTh label="ETA"     col="eta" view="po" sortState={sortState} onSort={onSort} />
            <th style={TH0}>Containers</th>
            <th style={TH0}>Invoices</th>
            <SortTh label="Units"  col="units" view="po" sortState={sortState} onSort={onSort} align="right" />
            <th style={{ ...TH0, textAlign: 'right' }}>CBM</th>
            <th style={TH0}>Status</th>
            <th style={TH0}>Details</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((p, idx) => {
            const key    = 'po-' + idx;
            const exp    = expandedRows[key];
            const status = p.status;
            const expTab = exp || 'container';
            return (
              <>
                <DataRow key={key}>
                  <td style={{ ...TD0, fontWeight: 700, fontSize: 13, color: 'var(--acc3)' }}>{p.po}</td>
                  <td style={{ ...TD0, fontWeight: 600, color: 'var(--txt)' }}>{fmtDate(p.eta)}</td>
                  <td style={{ ...TD0, color: 'var(--txt2)' }}>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3 }}>
                      {p.containers.map(c => (
                        <span key={c.container_number} style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--acc3)', marginRight: 2 }}>
                          {c.container_number !== '—' ? c.container_number.slice(0,11) : '(unbooked)'}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td style={{ ...TD0, color: 'var(--txt2)' }}>{p.invoices.length ? p.invoices.join(', ') : '—'}</td>
                  <td style={{ ...TD0, fontWeight: 700, textAlign: 'right' }}>{fmtNum(p.units)}</td>
                  <td style={{ ...TD0, color: 'var(--txt3)', textAlign: 'right' }}>{p.cbm.toFixed(1)}</td>
                  <td style={TD0}><StatusBadge status={status} /></td>
                  <td style={TD0}>
                    <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap' }}>
                      {[['container','Containers'],['invoice','Invoices'],['items','Items']].map(([tab, label]) => (
                        <ToggleBtn
                          key={tab}
                          label={label}
                          open={exp === tab}
                          onClick={() => setExpandedRows(p2 => ({ ...p2, [key]: exp === tab ? null : tab }))}
                        />
                      ))}
                    </div>
                  </td>
                </DataRow>
                {exp && (
                  <ExpandPanel key={key+'-exp'} title={`PO ${p.po}`}>
                    <div style={{ padding: '6px 12px 4px', background: 'var(--card)', borderBottom: '1px solid var(--brd)', display: 'flex', alignItems: 'center', gap: 8 }}>
                      <ExpandTabs
                        tabs={[{ key:'container', label:'Containers' }, { key:'invoice', label:'Invoices' }, { key:'items', label:'Items' }]}
                        active={expTab}
                        onSelect={tab => setExpandedRows(p2 => ({ ...p2, [key]: tab }))}
                      />
                    </div>
                    {expTab === 'container' && (
                      <MiniTable
                        headers={['Container #','Invoice #','Vessel','ETD','ETA','Port','Status','Units','CBM']}
                        rows={p.containers.map(c => [
                          { content: c.container_number, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                          { content: c.invoice_number || '—', style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--txt2)' } },
                          { content: c.vessel || '—', style: { color: 'var(--txt2)' } },
                          fmtDate(c.etd),
                          { content: fmtDate(c.container_eta), style: { fontWeight: 700, color: 'var(--txt)' } },
                          (c.eta_port || '—').split(',')[0],
                          { content: <StatusBadge status={normStatus(c.status)} />, style: {} },
                          { content: fmtNum(c.units), style: { textAlign: 'right', fontWeight: 700 } },
                          { content: (c.cbm||0).toFixed(1), style: { textAlign: 'right' } },
                        ])}
                      />
                    )}
                    {expTab === 'invoice' && (
                      <MiniTable
                        headers={['Invoice #','Container','ETA','Status','Units','CBM']}
                        rows={p.invoices.length ? p.containers.filter(c => c.invoice_number && c.invoice_number !== '—').map(c => [
                          { content: c.invoice_number, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--txt)' } },
                          { content: c.container_number, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)', fontSize: 9 } },
                          fmtDate(c.container_eta),
                          { content: <StatusBadge status={normStatus(c.status)} />, style: {} },
                          { content: fmtNum(c.units), style: { textAlign: 'right', fontWeight: 700 } },
                          { content: (c.cbm||0).toFixed(1), style: { textAlign: 'right' } },
                        ]) : [[{ content: 'No invoices yet', style: { color: 'var(--txt3)', fontStyle: 'italic' } }]]}
                      />
                    )}
                    {expTab === 'items' && (() => {
                      const allItems = p.containers.flatMap(c => (itemsByContainer[c.container_number] || []).map(it => ({ ...it, container: c.container_number })));
                      const bysku = {};
                      allItems.forEach(it => {
                        if (!bysku[it.sku]) bysku[it.sku] = { sku: it.sku, desc: it.description, units: 0 };
                        bysku[it.sku].units += it.units;
                      });
                      const rows2 = Object.values(bysku);
                      return rows2.length ? (
                        <MiniTable
                          headers={['SKU','Description','Units']}
                          rows={rows2.map(r => [
                            { content: r.sku, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                            r.desc,
                            { content: fmtNum(r.units), style: { textAlign: 'right', fontWeight: 700 } },
                          ])}
                        />
                      ) : <div style={{ padding: '10px 12px', color: 'var(--txt3)', fontStyle: 'italic', fontSize: 11 }}>No item detail available. Upload PO data to see SKU breakdown.</div>;
                    })()}
                  </ExpandPanel>
                )}
              </>
            );
          })}
          <TotalRow label={`TOTAL — ${filtered.length} POs`} units={tu} cbm={tc} colSpan={5} />
        </tbody>
      </table>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// VIEW 4: BY INVOICE
// ─────────────────────────────────────────────────────────────────────────────
function InvoiceView({ containers, itemsByContainer, expandedRows, setExpandedRows, sortState, onSort, statusFilter, search }) {
  const invGroups = useMemo(() => {
    const map = {};
    containers.forEach(c => {
      const inv = c.invoice_number || '—';
      if (!map[inv]) map[inv] = { invoice: inv, containers: [] };
      map[inv].containers.push(c);
    });
    return Object.values(map).map(g => ({
      ...g,
      po:     [...new Set(g.containers.map(c => c.po_number).filter(Boolean))].join(', '),
      eta:    g.containers.reduce((best, c) => (!best || !c.container_eta || c.container_eta === '—') ? best : (!best ? c.container_eta : (c.container_eta < best ? c.container_eta : best)), null) || '—',
      units:  g.containers.reduce((s, c) => s + (c.units || 0), 0),
      cbm:    g.containers.reduce((s, c) => s + (c.cbm   || 0), 0),
      status: g.containers.every(c => normStatus(c.status) === 'delivered') ? 'delivered'
            : g.containers.some(c  => normStatus(c.status) === 'transit')   ? 'transit'
            : 'open',
    }));
  }, [containers]);

  const filtered = useMemo(() => {
    const f = applyFilter(invGroups.map(g => ({ ...g, container_eta: g.eta })), statusFilter, search);
    return sortRows(f, sortState.invoice?.key || 'eta', sortState.invoice?.dir || 1);
  }, [invGroups, statusFilter, search, sortState]);

  const tu = filtered.reduce((s, r) => s + (r.units || 0), 0);
  const tc = filtered.reduce((s, r) => s + (r.cbm   || 0), 0);

  return (
    <Card
      title="By Invoice"
      stats={[{ val: filtered.length, label: 'invoices' }, { val: fmtNum(tu), label: 'units' }, { val: tc.toFixed(1), label: 'CBM' }]}
    >
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <SortTh label="Invoice #"   col="invoice" view="invoice" sortState={sortState} onSort={onSort} />
            <SortTh label="PO #"        col="po"      view="invoice" sortState={sortState} onSort={onSort} />
            <th style={TH0}>Container(s)</th>
            <SortTh label="ETA"         col="eta"     view="invoice" sortState={sortState} onSort={onSort} />
            <SortTh label="Units"       col="units"   view="invoice" sortState={sortState} onSort={onSort} align="right" />
            <th style={{ ...TH0, textAlign: 'right' }}>CBM</th>
            <th style={TH0}>Status</th>
            <th style={TH0}>Details</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((g, idx) => {
            const key    = 'inv-' + idx;
            const exp    = expandedRows[key];
            const expTab = exp || 'container';
            return (
              <>
                <DataRow key={key}>
                  <td style={{ ...TD0, fontFamily: "'Space Grotesk',monospace", fontWeight: 700, color: 'var(--txt)' }}>{g.invoice}</td>
                  <td style={{ ...TD0, color: 'var(--acc3)', fontWeight: 600 }}>{g.po}</td>
                  <td style={{ ...TD0, fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--txt3)' }}>
                    {g.containers.map(c => c.container_number === '—' ? '(unbooked)' : c.container_number.slice(0,11)).join(', ')}
                  </td>
                  <td style={{ ...TD0, fontWeight: 600, color: 'var(--txt)' }}>{fmtDate(g.eta)}</td>
                  <td style={{ ...TD0, fontWeight: 700, textAlign: 'right' }}>{fmtNum(g.units)}</td>
                  <td style={{ ...TD0, color: 'var(--txt3)', textAlign: 'right' }}>{g.cbm.toFixed(1)}</td>
                  <td style={TD0}><StatusBadge status={g.status} /></td>
                  <td style={TD0}>
                    <div style={{ display: 'flex', gap: 4 }}>
                      {[['container','Containers'],['po','By PO'],['items','Items']].map(([tab, label]) => (
                        <ToggleBtn
                          key={tab}
                          label={label}
                          open={exp === tab}
                          onClick={() => setExpandedRows(p => ({ ...p, [key]: exp === tab ? null : tab }))}
                        />
                      ))}
                    </div>
                  </td>
                </DataRow>
                {exp && (
                  <ExpandPanel key={key+'-exp'} title={`Invoice ${g.invoice}`}>
                    <div style={{ padding: '6px 12px 4px', background: 'var(--card)', borderBottom: '1px solid var(--brd)', display: 'flex', alignItems: 'center', gap: 8 }}>
                      <ExpandTabs
                        tabs={[{ key:'container', label:'Containers' }, { key:'po', label:'By PO' }, { key:'items', label:'Items' }]}
                        active={expTab}
                        onSelect={tab => setExpandedRows(p => ({ ...p, [key]: tab }))}
                      />
                    </div>
                    {expTab === 'container' && (
                      <MiniTable
                        headers={['Container #','Type','ETD','ETA','ETA Port','Status','Units','CBM']}
                        rows={g.containers.map(c => [
                          { content: c.container_number, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                          c.container_type || '—',
                          fmtDate(c.etd),
                          { content: fmtDate(c.container_eta), style: { fontWeight: 700, color: 'var(--txt)' } },
                          (c.eta_port || '—').split(',')[0],
                          { content: <StatusBadge status={normStatus(c.status)} />, style: {} },
                          { content: fmtNum(c.units), style: { textAlign: 'right', fontWeight: 700 } },
                          { content: (c.cbm||0).toFixed(1), style: { textAlign: 'right' } },
                        ])}
                      />
                    )}
                    {expTab === 'po' && (
                      <MiniTable
                        headers={['PO #','Container','ETA','Status','Units','CBM']}
                        rows={g.containers.map(c => [
                          { content: c.po_number || '—', style: { fontWeight: 700, color: 'var(--acc3)' } },
                          { content: c.container_number, style: { fontFamily: "'Space Grotesk',monospace", fontSize: 9 } },
                          fmtDate(c.container_eta),
                          { content: <StatusBadge status={normStatus(c.status)} />, style: {} },
                          { content: fmtNum(c.units), style: { textAlign: 'right', fontWeight: 700 } },
                          { content: (c.cbm||0).toFixed(1), style: { textAlign: 'right' } },
                        ])}
                      />
                    )}
                    {expTab === 'items' && (() => {
                      const allItems = g.containers.flatMap(c => (itemsByContainer[c.container_number] || []));
                      const bysku = {};
                      allItems.forEach(it => {
                        if (!bysku[it.sku]) bysku[it.sku] = { sku: it.sku, desc: it.description, units: 0 };
                        bysku[it.sku].units += it.units;
                      });
                      const rows2 = Object.values(bysku);
                      return rows2.length ? (
                        <MiniTable
                          headers={['SKU','Description','Units']}
                          rows={rows2.map(r => [
                            { content: r.sku, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                            r.desc,
                            { content: fmtNum(r.units), style: { textAlign: 'right', fontWeight: 700 } },
                          ])}
                        />
                      ) : <div style={{ padding: '10px 12px', color: 'var(--txt3)', fontStyle: 'italic', fontSize: 11 }}>No item detail available.</div>;
                    })()}
                  </ExpandPanel>
                )}
              </>
            );
          })}
          <TotalRow label={`TOTAL — ${filtered.length} invoices`} units={tu} cbm={tc} colSpan={5} />
        </tbody>
      </table>
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// VIEW 5: ITEMS
// ─────────────────────────────────────────────────────────────────────────────
function ItemsView({ itemPivot, containers, expandedRows, setExpandedRows, sortState, onSort, statusFilter, search }) {
  // Build flat item list from itemPivot (SKU-level aggregates)
  const items = useMemo(() => {
    if (!itemPivot?.length) return [];
    return itemPivot.map(item => {
      const ctrEntries = Object.entries(item.containers || {});
      // Determine overall status from matching containers
      const ctrObjects = ctrEntries.map(([cid]) => containers.find(c => c.container_number === cid)).filter(Boolean);
      const status = ctrObjects.length === 0 ? 'open'
                   : ctrObjects.every(c => normStatus(c.status) === 'delivered') ? 'delivered'
                   : ctrObjects.some(c  => normStatus(c.status) === 'transit')   ? 'transit'
                   : 'open';
      const eta = ctrObjects.reduce((best, c) => {
        if (!c.container_eta || c.container_eta === '—') return best;
        return !best ? c.container_eta : (c.container_eta < best ? c.container_eta : best);
      }, null) || '—';
      // Estimate CBM proportionally from container-level CBM
      const cbm = Math.round(ctrEntries.reduce((sum, [cid, skuUnits]) => {
        const c = containers.find(ct => ct.container_number === cid);
        if (!c || !c.units || !c.cbm) return sum;
        return sum + (skuUnits / c.units) * c.cbm;
      }, 0) * 10) / 10;
      return { ...item, status, eta, cbm };
    });
  }, [itemPivot, containers]);

  const filtered = useMemo(() => {
    let r = items;
    if (statusFilter && statusFilter !== 'all') r = r.filter(item => item.status === statusFilter);
    if (search) {
      const q = search.toLowerCase();
      r = r.filter(item => `${item.sku} ${item.description}`.toLowerCase().includes(q));
    }
    return sortRows(r, sortState.item?.key || 'sku', sortState.item?.dir || 1);
  }, [items, statusFilter, search, sortState]);

  const tu = filtered.reduce((s, r) => s + (r.total || 0), 0);

  return (
    <Card
      title="All Items — Aggregated by SKU"
      stats={[{ val: filtered.length, label: 'SKUs' }, { val: fmtNum(tu), label: 'total units' }]}
    >
      {filtered.length === 0 ? (
        <div style={{ padding: '24px 16px', color: 'var(--txt3)', fontStyle: 'italic', fontSize: 12, fontFamily: "'Sora',sans-serif" }}>
          No item data. Upload a PO file to see item breakdown.
        </div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <SortTh label="SKU"         col="sku"   view="item" sortState={sortState} onSort={onSort} />
              <SortTh label="Description" col="description" view="item" sortState={sortState} onSort={onSort} />
              <th style={TH0}>Status</th>
              <SortTh label="Total Units" col="total" view="item" sortState={sortState} onSort={onSort} align="right" />
              <th style={{ ...TH0, textAlign: 'right' }}>Total CBM</th>
              <th style={TH0}>Details</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((item, idx) => {
              const key = 'item-' + idx;
              const exp = expandedRows[key];
              const expTab = exp || 'po';
              const ctrList = Object.entries(item.containers || {});
              const ctrObjects = ctrList.map(([cid]) => containers.find(c => c.container_number === cid)).filter(Boolean);
              // Group by PO, Container, Invoice
              const byPO = {}, byCtr2 = {}, byInv2 = {};
              ctrList.forEach(([cid, units]) => {
                const c = containers.find(ct => ct.container_number === cid);
                if (!c) return;
                const po = c.po_number || '—';
                const inv = c.invoice_number || '—';
                const st = normStatus(c.status);
                // byPO
                if (!byPO[po]) byPO[po] = { po, units: 0, containers: [], invoices: [], status: st };
                byPO[po].units += units;
                if (!byPO[po].containers.includes(cid)) byPO[po].containers.push(cid);
                if (inv !== '—' && !byPO[po].invoices.includes(inv)) byPO[po].invoices.push(inv);
                if (st === 'transit') byPO[po].status = 'transit';
                else if (st === 'open' && byPO[po].status === 'delivered') byPO[po].status = 'open';
                // byCtr
                if (!byCtr2[cid]) byCtr2[cid] = { cid, units: 0, pos: [], invoices: [], status: st, eta: c.container_eta };
                byCtr2[cid].units += units;
                if (!byCtr2[cid].pos.includes(po)) byCtr2[cid].pos.push(po);
                if (inv !== '—' && !byCtr2[cid].invoices.includes(inv)) byCtr2[cid].invoices.push(inv);
                // byInv
                if (!byInv2[inv]) byInv2[inv] = { inv, units: 0, pos: [], containers: [], status: st, eta: c.container_eta };
                byInv2[inv].units += units;
                if (!byInv2[inv].pos.includes(po)) byInv2[inv].pos.push(po);
                if (!byInv2[inv].containers.includes(cid)) byInv2[inv].containers.push(cid);
              });
              return (
                <>
                  <DataRow key={key}>
                    <td style={{ ...TD0, fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)', fontWeight: 700 }}>{item.sku}</td>
                    <td style={{ ...TD0, maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis' }}>{item.description}</td>
                    <td style={TD0}><StatusBadge status={item.status} /></td>
                    <td style={{ ...TD0, fontWeight: 800, fontSize: 13, color: 'var(--txt)', textAlign: 'right' }}>{fmtNum(item.total)}</td>
                    <td style={{ ...TD0, textAlign: 'right', color: 'var(--txt2)' }}>{item.cbm > 0 ? item.cbm.toFixed(1) : '—'}</td>
                    <td style={TD0}>
                      <div style={{ display: 'flex', gap: 4 }}>
                        {[['po','By PO'],['container','By Container'],['invoice','By Invoice']].map(([tab, label]) => (
                          <ToggleBtn
                            key={tab}
                            label={label}
                            open={exp === tab}
                            onClick={() => setExpandedRows(p => ({ ...p, [key]: exp === tab ? null : tab }))}
                          />
                        ))}
                      </div>
                    </td>
                  </DataRow>
                  {exp && (
                    <ExpandPanel key={key+'-exp'} title={`${item.sku} — ${item.description}`}>
                      <div style={{ padding: '6px 12px 4px', background: 'var(--card)', borderBottom: '1px solid var(--brd)', display: 'flex', alignItems: 'center', gap: 8 }}>
                        <ExpandTabs
                          tabs={[{ key:'po', label:'By PO' }, { key:'container', label:'By Container' }, { key:'invoice', label:'By Invoice' }]}
                          active={expTab}
                          onSelect={tab => setExpandedRows(p => ({ ...p, [key]: tab }))}
                        />
                      </div>
                      {expTab === 'po' && (
                        <MiniTable
                          headers={['PO #','Containers','Invoices','Status','Units']}
                          rows={Object.values(byPO).map(p => [
                            { content: p.po, style: { fontWeight: 700, color: 'var(--acc3)' } },
                            { content: p.containers.map(c => c === '—' ? '(unbooked)' : c.slice(0,11)).join(', '), style: { fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--txt3)' } },
                            { content: p.invoices.length ? p.invoices.join(', ') : '—', style: { fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--txt2)' } },
                            { content: <StatusBadge status={p.status} />, style: {} },
                            { content: fmtNum(p.units), style: { textAlign: 'right', fontWeight: 700, color: 'var(--txt)' } },
                          ])}
                        />
                      )}
                      {expTab === 'container' && (
                        <MiniTable
                          headers={['Container #','PO(s)','Invoice(s)','ETA','Status','Units']}
                          rows={Object.entries(byCtr2).map(([cid, d]) => [
                            { content: cid === '—' ? '(unbooked)' : cid, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                            { content: d.pos.join(', '), style: { fontWeight: 700, color: 'var(--txt)' } },
                            { content: d.invoices.length ? d.invoices.join(', ') : '—', style: { fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--txt2)' } },
                            { content: fmtDate(d.eta), style: { fontWeight: 600, color: 'var(--txt)' } },
                            { content: <StatusBadge status={d.status} />, style: {} },
                            { content: fmtNum(d.units), style: { textAlign: 'right', fontWeight: 700, color: 'var(--txt)' } },
                          ])}
                        />
                      )}
                      {expTab === 'invoice' && (
                        <MiniTable
                          headers={['Invoice #','PO(s)','Container(s)','ETA','Status','Units']}
                          rows={Object.entries(byInv2).map(([inv, d]) => [
                            { content: inv, style: { fontFamily: "'Space Grotesk',monospace", color: 'var(--acc3)' } },
                            { content: d.pos.join(', '), style: { fontWeight: 700, color: 'var(--txt)' } },
                            { content: d.containers.map(c => c === '—' ? '(unbooked)' : c.slice(0,11)).join(', '), style: { fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--txt3)' } },
                            { content: fmtDate(d.eta), style: { fontWeight: 600, color: 'var(--txt)' } },
                            { content: <StatusBadge status={d.status} />, style: {} },
                            { content: fmtNum(d.units), style: { textAlign: 'right', fontWeight: 700, color: 'var(--txt)' } },
                          ])}
                        />
                      )}
                    </ExpandPanel>
                  )}
                </>
              );
            })}
            <tr style={{ background: 'var(--card2)', fontFamily: "'Space Grotesk',monospace", fontSize: 11, fontWeight: 800, color: 'var(--txt)' }}>
              <td colSpan={3} style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)' }}>TOTAL — {filtered.length} SKUs</td>
              <td style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)', textAlign: 'right' }}>{fmtNum(tu)}</td>
              <td style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)' }}/>
              <td style={{ padding: '10px 12px', borderTop: '1px solid var(--brd2)' }}/>
            </tr>
          </tbody>
        </table>
      )}
    </Card>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// UPLOAD MODAL
// ─────────────────────────────────────────────────────────────────────────────
function UploadModal({ open, onClose, onUpload, uploading, uploadMsg }) {
  const [tab, setTab] = useState('po');
  const poRef   = useRef();

  if (!open) return null;
  return (
    <div
      onClick={e => { if (e.target === e.currentTarget) onClose(); }}
      style={{
        position: 'fixed', inset: 0, background: 'rgba(0,0,0,.72)', zIndex: 200,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}
    >
      <div style={{
        background: 'var(--card2)', border: '1px solid var(--brd2)', borderRadius: 16,
        padding: 24, width: 520, maxWidth: '95vw',
      }}>
        <div style={{ fontFamily: "'DM Serif Display',Georgia,serif", fontSize: 17, fontWeight: 400, marginBottom: 6, color: 'var(--txt)' }}>
          Upload Supply Chain Data
        </div>
        <div style={{ fontSize: 12, color: 'var(--txt3)', marginBottom: 16, fontFamily: "'Sora',sans-serif" }}>
          Upload your Excel file. Matching POs/Containers will be updated automatically.
        </div>
        {/* Modal tabs */}
        <div style={{ display: 'flex', gap: 4, marginBottom: 16 }}>
          {[['po','Tab 1 — PO & Items'],['ship','Tab 2 — Shipments']].map(([k,l]) => (
            <button key={k} onClick={() => setTab(k)} style={{
              padding: '6px 16px', borderRadius: '7px 7px 0 0', fontSize: 12, fontWeight: 600,
              border: '1px solid var(--brd)', borderBottom: tab === k ? '1px solid var(--card2)' : '1px solid var(--brd)',
              background: tab === k ? 'var(--card2)' : 'var(--surf)',
              color: tab === k ? 'var(--txt)' : 'var(--txt3)', cursor: 'pointer',
              fontFamily: "'Sora',sans-serif",
            }}>{l}</button>
          ))}
        </div>
        {tab === 'po' && (
          <>
            <label style={{ display: 'block', border: '2px dashed var(--brd2)', borderRadius: 12, padding: 28, textAlign: 'center', cursor: 'pointer' }}>
              <div style={{ fontSize: 28, marginBottom: 8 }}>📂</div>
              <div style={{ fontSize: 12, color: 'var(--txt3)', fontFamily: "'Sora',sans-serif" }}>
                <strong style={{ color: 'var(--acc3)' }}>Click to choose</strong> or drag &amp; drop
              </div>
              <div style={{ marginTop: 4, fontSize: 11, color: 'var(--txt3)', fontFamily: "'Sora',sans-serif" }}>PO_Update_YYYY-MM-DD.xlsx</div>
              <input ref={poRef} type="file" accept=".xlsx,.xls" hidden onChange={e => onUpload(e.target.files?.[0])} />
            </label>
            <div style={{ fontSize: 10, color: 'var(--txt3)', marginTop: 10, lineHeight: 1.6, fontFamily: "'Sora',sans-serif" }}>
              <strong style={{ color: 'var(--acc3)' }}>Required columns:</strong><br/>
              PO# · Factory · Invoice# · Container# · SKU · Description · Units · CBM · Est. Arrival · Status<br/><br/>
              One row per SKU × PO × Container. Blank Invoice/Container = Open (not yet shipped).
            </div>
          </>
        )}
        {tab === 'ship' && (
          <>
            <label style={{ display: 'block', border: '2px dashed var(--brd2)', borderRadius: 12, padding: 28, textAlign: 'center', cursor: 'pointer' }}>
              <div style={{ fontSize: 28, marginBottom: 8 }}>📂</div>
              <div style={{ fontSize: 12, color: 'var(--txt3)', fontFamily: "'Sora',sans-serif" }}>
                <strong style={{ color: 'var(--acc3)' }}>Click to choose</strong> or drag &amp; drop
              </div>
              <div style={{ marginTop: 4, fontSize: 11, color: 'var(--txt3)', fontFamily: "'Sora',sans-serif" }}>Shipment_Update_YYYY-MM-DD.xlsx</div>
              <input type="file" accept=".xlsx,.xls" hidden onChange={e => onUpload(e.target.files?.[0])} />
            </label>
            <div style={{ fontSize: 10, color: 'var(--txt3)', marginTop: 10, lineHeight: 1.6, fontFamily: "'Sora',sans-serif" }}>
              <strong style={{ color: 'var(--acc3)' }}>Required columns:</strong><br/>
              Container# · PO# · Shipper · HB# · Type · Vessel · ETD · ETD Port · ETA · ETA Port · ETA Dest. · Destination · Act. Delivery · Comments<br/><br/>
              One row per PO × Container.
            </div>
          </>
        )}
        {uploadMsg && (
          <div style={{
            marginTop: 12, padding: '8px 12px', borderRadius: 8, fontSize: 12,
            background: uploadMsg.toLowerCase().includes('error') || uploadMsg.toLowerCase().includes('fail')
              ? 'rgba(248,113,113,.12)' : 'rgba(46,207,170,.12)',
            color: uploadMsg.toLowerCase().includes('error') || uploadMsg.toLowerCase().includes('fail')
              ? '#f87171' : '#2ECFAA',
          }}>{uploadMsg}</div>
        )}
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 20 }}>
          <button onClick={onClose} style={{
            padding: '8px 18px', borderRadius: 8, fontSize: 13, fontWeight: 600,
            border: '1px solid var(--brd)', background: 'transparent', color: 'var(--txt3)',
            cursor: 'pointer', fontFamily: "'Sora',sans-serif",
          }}>Cancel</button>
          <button disabled={uploading} style={{
            padding: '8px 18px', borderRadius: 8, fontSize: 13, fontWeight: 700,
            border: '1px solid var(--acc2)', background: 'rgba(232,120,48,.1)', color: 'var(--acc2)',
            cursor: 'pointer', fontFamily: "'Sora',sans-serif", opacity: uploading ? 0.6 : 1,
          }}>{uploading ? 'Uploading…' : 'Upload & Process'}</button>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN COMPONENT
// ─────────────────────────────────────────────────────────────────────────────
export default function SupplyChain() {
  const [view,          setView]          = useState('summary');
  const [statusFilter,  setStatusFilter]  = useState('all');
  const [search,        setSearch]        = useState('');
  const [expandedRows,  setExpandedRows]  = useState({});
  const [sortState,     setSortState]     = useState({
    summary:   { key: 'container_eta', dir: 1 },
    container: { key: 'container_eta', dir: 1 },
    po:        { key: 'eta',           dir: 1 },
    invoice:   { key: 'eta',           dir: 1 },
    item:      { key: 'sku',           dir: 1 },
  });
  const [modalOpen,   setModalOpen]   = useState(false);
  const [containers,  setContainers]  = useState([]);
  const [itemPivot,   setItemPivot]   = useState([]);
  const [kpis,        setKpis]        = useState(null);
  const [lastUpload,  setLastUpload]  = useState(null);
  const [loading,     setLoading]     = useState(true);
  const [loadErr,     setLoadErr]     = useState('');
  const [uploading,   setUploading]   = useState(false);
  const [uploadMsg,   setUploadMsg]   = useState('');

  // Build items-by-container lookup from itemPivot
  const itemsByContainer = useMemo(() => {
    const map = {};
    (itemPivot || []).forEach(item => {
      Object.entries(item.containers || {}).forEach(([cid, units]) => {
        if (!map[cid]) map[cid] = [];
        map[cid].push({ sku: item.sku, description: item.description, units });
      });
    });
    return map;
  }, [itemPivot]);

  const load = async () => {
    setLoading(true);
    setLoadErr('');
    try {
      const data = await api.supplyChainOTW();
      const rows = data?.rows || [];
      setContainers(rows);
      setItemPivot(data?.itemPivot || []);
      setLastUpload(data?.lastUpload || null);
      // Compute KPIs from rows
      const delivered = rows.filter(r => normStatus(r.status) === 'delivered');
      const transit   = rows.filter(r => normStatus(r.status) === 'transit');
      const open      = rows.filter(r => normStatus(r.status) === 'open');
      setKpis({
        units_on_order:  rows.reduce((s, r) => s + (r.units || 0), 0),
        units_transit:   transit.reduce((s, r) => s + (r.units || 0), 0),
        units_delivered: delivered.reduce((s, r) => s + (r.units || 0), 0),
        units_open:      open.reduce((s, r) => s + (r.units || 0), 0),
        active_pos:      new Set(rows.map(r => r.po_number).filter(Boolean)).size,
        total_cbm:       Math.round(rows.reduce((s, r) => s + (r.cbm || 0), 0)),
        cnt_transit:     transit.length,
      });
    } catch (e) {
      setLoadErr('Failed to load supply chain data: ' + e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const handleSort = (view, col) => {
    setSortState(prev => ({
      ...prev,
      [view]: prev[view]?.key === col
        ? { key: col, dir: -(prev[view].dir) }
        : { key: col, dir: 1 },
    }));
  };

  const handleViewChange = (v) => {
    setView(v);
    setExpandedRows({});
  };

  const handleUpload = async (file) => {
    if (!file) return;
    setUploading(true);
    setUploadMsg('');
    try {
      const res = await api.uploadSupplyChainV2(file);
      if (res.status === 'ok') {
        setUploadMsg(`Uploaded ${res.records_parsed || 0} records (${res.total_records || 0} total).`);
        setModalOpen(false);
        await load();
      } else {
        setUploadMsg('Upload error: ' + (res.message || 'Unknown error'));
      }
    } catch (e) {
      setUploadMsg('Upload failed: ' + e.message);
    } finally {
      setUploading(false);
    }
  };

  const KPI_CONFIG = [
    { label: 'Units On Order',     val: fmtNum(kpis?.units_on_order),  sub: 'All POs',                               col: 'var(--acc3)' },
    { label: 'In Transit',         val: fmtNum(kpis?.units_transit),   sub: `${kpis?.cnt_transit || 0} containers`,  col: '#E87830'     },
    { label: 'Delivered',          val: fmtNum(kpis?.units_delivered), sub: 'This season',                           col: '#2ECFAA'     },
    { label: 'Open / Not Shipped', val: fmtNum(kpis?.units_open),      sub: 'Pending booking',                       col: 'var(--txt3)' },
    { label: 'Active POs',         val: kpis?.active_pos ?? '—',       sub: 'All factories',                         col: 'var(--acc3)' },
    { label: 'Total CBM',          val: fmtNum(kpis?.total_cbm),       sub: 'All containers',                        col: 'var(--acc3)' },
  ];

  const VIEW_TABS = [
    { key: 'summary',   label: 'Summary'      },
    { key: 'container', label: 'By Container' },
    { key: 'po',        label: 'By PO'        },
    { key: 'invoice',   label: 'By Invoice'   },
    { key: 'item',      label: 'Items'        },
  ];

  const FILTER_PILLS = [
    { key: 'all',       label: 'All'         },
    { key: 'delivered', label: 'Delivered'   },
    { key: 'transit',   label: 'In Transit'  },
    { key: 'open',      label: 'Open'        },
  ];

  if (loading) {
    return <div style={{ display: 'flex', alignItems: 'center', gap: 10, color: 'var(--txt3)', padding: 40, fontFamily: "'Sora',sans-serif" }}>
      <div style={{ width: 18, height: 18, border: '2px solid var(--brd)', borderTopColor: 'var(--acc3)', borderRadius: '50%', animation: 'spin 0.8s linear infinite' }}/>
      Loading supply chain…
    </div>;
  }

  return (
    <div style={{ fontFamily: "'Sora',sans-serif" }}>
      {/* ── Page header ─────────────────────────────────────────────── */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', flexWrap: 'wrap', gap: 12, marginBottom: 20 }}>
        <div>
          <h2 style={{ fontFamily: "'DM Serif Display',Georgia,serif", fontSize: 22, fontWeight: 400, color: 'var(--txt)', margin: 0 }}>
            Supply Chain
          </h2>
          <div style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 10, color: 'var(--txt3)', marginTop: 4, letterSpacing: '.04em' }}>
            Golf Division · On The Water &amp; Open Orders
            {lastUpload && <span> &nbsp;·&nbsp; Updated {new Date(lastUpload).toLocaleDateString()}</span>}
          </div>
        </div>
        <button
          onClick={() => { setUploadMsg(''); setModalOpen(true); }}
          style={{
            display: 'flex', alignItems: 'center', gap: 6, padding: '6px 14px',
            borderRadius: 9, fontSize: 12, fontWeight: 700,
            border: '1px solid var(--acc2)', background: 'rgba(232,120,48,.1)', color: 'var(--acc2)',
            cursor: 'pointer', fontFamily: "'Sora',sans-serif",
          }}
        >
          <svg width={13} height={13} viewBox="0 0 14 14" fill="none">
            <path d="M7 1v8M3.5 4.5L7 1l3.5 3.5M1 10.5v1.5a1 1 0 001 1h10a1 1 0 001-1v-1.5" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
          Upload Data
        </button>
      </div>

      {loadErr && (
        <div style={{ marginBottom: 14, padding: '8px 14px', borderRadius: 8, background: 'rgba(248,113,113,.12)', color: '#f87171', fontSize: 12, border: '1px solid rgba(248,113,113,.2)' }}>
          ⚠ {loadErr}
        </div>
      )}

      {/* ── KPI cards ───────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 20, overflowX: 'auto', paddingBottom: 2 }}>
        {KPI_CONFIG.map(k => (
          <div key={k.label} style={{
            flex: '1 1 0', minWidth: 130,
            background: 'linear-gradient(145deg,var(--card),var(--card2))',
            borderRadius: 12, padding: '11px 10px', border: '1px solid var(--brd)',
          }}>
            <div style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 9, color: 'var(--txt3)', textTransform: 'uppercase', letterSpacing: '.07em', marginBottom: 6, whiteSpace: 'nowrap' }}>
              {k.label}
            </div>
            <div style={{ fontFamily: "'DM Serif Display',Georgia,serif", fontSize: 22, lineHeight: 1, color: k.col }}>
              {k.val}
            </div>
            <div style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 10, color: 'var(--txt3)', marginTop: 4 }}>
              {k.sub}
            </div>
          </div>
        ))}
      </div>

      {/* ── View tabs ───────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: 2, padding: 4, borderRadius: 12, background: 'var(--surf)', border: '1px solid var(--brd)', width: 'fit-content', marginBottom: 20 }}>
        {VIEW_TABS.map(t => (
          <button
            key={t.key}
            onClick={() => handleViewChange(t.key)}
            style={{
              padding: '6px 20px', borderRadius: 8, fontSize: 13, fontWeight: 600,
              border: 'none', cursor: 'pointer', fontFamily: "'Sora',sans-serif",
              background: view === t.key ? 'var(--atab)' : 'transparent',
              color:      view === t.key ? '#fff'        : 'var(--txt3)',
              transition: 'all .15s',
            }}
          >{t.label}</button>
        ))}
      </div>

      {/* ── Toolbar ─────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', gap: 8, marginBottom: 14, flexWrap: 'wrap', alignItems: 'center' }}>
        <span style={{ fontFamily: "'Space Grotesk',monospace", fontSize: 10, color: 'var(--txt3)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '.07em' }}>Status:</span>
        {FILTER_PILLS.map(p => (
          <button
            key={p.key}
            onClick={() => setStatusFilter(p.key)}
            style={{
              padding: '4px 12px', borderRadius: 99, fontSize: 11, fontWeight: 600,
              cursor: 'pointer', fontFamily: "'Sora',sans-serif", transition: 'all .15s',
              border: `1px solid ${statusFilter === p.key ? 'var(--acc3)' : 'var(--brd)'}`,
              background: statusFilter === p.key ? 'rgba(62,101,140,.18)' : 'transparent',
              color:      statusFilter === p.key ? 'var(--acc3)'          : 'var(--txt3)',
            }}
          >{p.label}</button>
        ))}
        <div style={{ flex: 1 }} />
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search PO, Container, SKU…"
          style={{
            padding: '5px 12px', borderRadius: 8, border: '1px solid var(--brd2)',
            background: 'var(--ibg)', color: 'var(--txt)', fontSize: 12, outline: 'none',
            width: 200, fontFamily: "'Sora',sans-serif",
          }}
        />
      </div>

      {/* ── Active view ─────────────────────────────────────────────── */}
      {view === 'summary' && (
        <SummaryView
          containers={containers}
          itemsByContainer={itemsByContainer}
          expandedRows={expandedRows}
          setExpandedRows={setExpandedRows}
          sortState={sortState}
          onSort={handleSort}
          statusFilter={statusFilter}
          search={search}
        />
      )}
      {view === 'container' && (
        <ContainerView
          containers={containers}
          itemsByContainer={itemsByContainer}
          expandedRows={expandedRows}
          setExpandedRows={setExpandedRows}
          sortState={sortState}
          onSort={handleSort}
          statusFilter={statusFilter}
          search={search}
        />
      )}
      {view === 'po' && (
        <POView
          containers={containers}
          itemsByContainer={itemsByContainer}
          expandedRows={expandedRows}
          setExpandedRows={setExpandedRows}
          sortState={sortState}
          onSort={handleSort}
          statusFilter={statusFilter}
          search={search}
        />
      )}
      {view === 'invoice' && (
        <InvoiceView
          containers={containers}
          itemsByContainer={itemsByContainer}
          expandedRows={expandedRows}
          setExpandedRows={setExpandedRows}
          sortState={sortState}
          onSort={handleSort}
          statusFilter={statusFilter}
          search={search}
        />
      )}
      {view === 'item' && (
        <ItemsView
          itemPivot={itemPivot}
          containers={containers}
          expandedRows={expandedRows}
          setExpandedRows={setExpandedRows}
          sortState={sortState}
          onSort={handleSort}
          statusFilter={statusFilter}
          search={search}
        />
      )}

      {/* ── Upload modal ─────────────────────────────────────────────── */}
      <UploadModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onUpload={handleUpload}
        uploading={uploading}
        uploadMsg={uploadMsg}
      />
    </div>
  );
}
