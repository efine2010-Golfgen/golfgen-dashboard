import { useState, useEffect, useCallback } from 'react';
import { api } from '../lib/api';

/* ═══════════════════════════════════════════════════════════════════════════
   SALES PLANNING  —  Plan vs Goal  ·  Monthly Revenue  ·  Item-Level Targets
   ═══════════════════════════════════════════════════════════════════════════ */

const B = {
  b1:'#1B4F8A', b2:'#2E6FBB', b3:'#5B9FD4',
  o1:'#D4600A', o2:'#E8821E', o3:'#F5A54A',
  t1:'#0F7A6E', t2:'#1AA392', t3:'#4DC5B8',
};

const MONTHS     = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MONTH_KEYS = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
const PLAN_YEAR  = 2026;
const SETTINGS_KEY = `sales_planning_${PLAN_YEAR}`;

// ── Formatters ────────────────────────────────────────────────
const f$  = v => v == null || isNaN(v) ? '—' : v >= 1e6 ? `$${(v/1e6).toFixed(2)}M` : v >= 1000 ? `$${(v/1000).toFixed(1)}K` : `$${Number(v).toFixed(0)}`;
const fN  = v => v == null || isNaN(v) || v === 0 ? '—' : v >= 1000 ? `${(v/1000).toFixed(1)}K` : Math.round(v).toLocaleString();
const avg = (obj, keys) => { const vals = keys.map(k => Number(obj?.[k])||0).filter(v=>v>0); return vals.length ? vals.reduce((s,v)=>s+v,0)/vals.length : 0; };

// ── Empty state factories ──────────────────────────────────────
const emptyMonthly = () => Object.fromEntries(MONTH_KEYS.map(k => [k, '']));
const emptyKPIs    = () => ({ return_rate:'', roas:'', tacos:'', conversion:'', add_to_cart:'', wos_amazon:'', wos_warehouse:'' });
const emptyPlan    = () => ({ monthly_revenue: emptyMonthly(), monthly_aur: emptyMonthly(), kpis: emptyKPIs(), items: {} });

// ── Shared table cell styles ───────────────────────────────────
const thS   = { padding:'6px 8px', background:'rgba(12,26,46,.6)', fontSize:8, fontWeight:700, textTransform:'uppercase', letterSpacing:'.07em', color:'var(--txt3)', borderBottom:'1px solid var(--brd)', textAlign:'right', whiteSpace:'nowrap' };
const tdS   = { padding:'5px 6px', textAlign:'right', verticalAlign:'middle', borderBottom:'1px solid rgba(26,47,74,.4)' };
const tdLbl = { padding:'7px 10px', fontSize:10, fontWeight:600, color:'var(--txt2)', textAlign:'left', whiteSpace:'nowrap', borderBottom:'1px solid rgba(26,47,74,.4)', minWidth:110 };

function CellInput({ value, onChange, placeholder='0', width=68 }) {
  return (
    <input
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      style={{
        width, fontSize:10, fontWeight:600, padding:'3px 5px', borderRadius:4,
        border:'1px solid rgba(46,111,187,.25)', background:'rgba(27,79,138,.12)',
        color:'var(--txt)', textAlign:'right', outline:'none',
        fontFamily:"'Space Grotesk',monospace",
      }}
    />
  );
}

function Section({ title, accent, children }) {
  return (
    <div style={{ background:'var(--card)', border:'1px solid var(--brd)', borderTop:`3px solid ${accent||'#2E6FBB'}`, borderRadius:12, padding:'14px 16px 16px', marginBottom:16 }}>
      <div style={{ fontSize:9, fontWeight:700, textTransform:'uppercase', letterSpacing:'.1em', color:'var(--txt3)', marginBottom:14 }}>{title}</div>
      {children}
    </div>
  );
}

const KPI_DEFS = [
  { key:'return_rate',   label:'Return Rate',      unit:'%',    tip:'Returns as % of gross revenue for the year' },
  { key:'roas',          label:'ROAS',             unit:'x',    tip:'Revenue generated per $1 of ad spend' },
  { key:'tacos',         label:'TACOS',            unit:'%',    tip:'Total ad spend divided by total revenue' },
  { key:'conversion',    label:'Conversion Rate',  unit:'%',    tip:'% of sessions that result in a purchase' },
  { key:'add_to_cart',   label:'Add to Cart',      unit:'%',    tip:'% of page views that add to cart' },
  { key:'wos_amazon',    label:'WoS at Amazon',    unit:' wks', tip:'Target weeks of supply at FBA warehouse' },
  { key:'wos_warehouse', label:'WoS at Warehouse', unit:' wks', tip:'Target weeks of supply at your warehouse' },
];

// ═══════════════════════════════════════════════════════════════
export default function SalesPlanning({ filters = {} }) {
  const [version,  setVersion]  = useState('plan');
  const [planData, setPlanData] = useState({ plan: emptyPlan(), goal: emptyPlan() });
  const [items,    setItems]    = useState([]);
  const [loading,  setLoading]  = useState(true);
  const [saving,   setSaving]   = useState(false);
  const [savedMsg, setSavedMsg] = useState('');
  const [search,   setSearch]   = useState('');

  // ── Load ──────────────────────────────────────────────────────
  useEffect(() => {
    const load = async () => {
      try {
        const [itemData, settings] = await Promise.all([
          api('/api/item-plan'),
          api('/api/dashboard-settings'),
        ]);

        const skus = (itemData?.skus || []).filter(s => s.product_name || s.asin);
        setItems(skus);

        const raw = settings?.[SETTINGS_KEY];
        if (raw) {
          try {
            const parsed = JSON.parse(raw);
            const merge = saved => ({
              monthly_revenue: { ...emptyMonthly(), ...(saved?.monthly_revenue || {}) },
              monthly_aur:     { ...emptyMonthly(), ...(saved?.monthly_aur     || {}) },
              kpis:            { ...emptyKPIs(),    ...(saved?.kpis            || {}) },
              items:           saved?.items || {},
            });
            setPlanData({ plan: merge(parsed?.plan), goal: merge(parsed?.goal) });
            return;
          } catch { /* fall through */ }
        }

        // Pre-populate from existing item-plan data
        const preItems = {};
        for (const sku of skus) {
          const id      = sku.asin || sku.sku;
          const units   = sku.ty_plan_annual || 0;
          const aurAvg  = avg(sku.ty_aur || sku.ly_aur || {}, MONTH_KEYS);
          preItems[id] = {
            units: units > 0   ? String(units)              : '',
            aur:   aurAvg > 0  ? aurAvg.toFixed(2)          : '',
          };
        }
        setPlanData({
          plan: { ...emptyPlan(), items: preItems },
          goal: { ...emptyPlan(), items: JSON.parse(JSON.stringify(preItems)) },
        });
      } catch (e) {
        console.error('SalesPlanning load:', e);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  // ── Mutators ──────────────────────────────────────────────────
  const setMonthRev  = useCallback((k, v) => setPlanData(p => ({ ...p, [version]: { ...p[version], monthly_revenue: { ...p[version].monthly_revenue, [k]: v } } })), [version]);
  const setMonthAUR  = useCallback((k, v) => setPlanData(p => ({ ...p, [version]: { ...p[version], monthly_aur:     { ...p[version].monthly_aur,     [k]: v } } })), [version]);
  const setKPI       = useCallback((k, v) => setPlanData(p => ({ ...p, [version]: { ...p[version], kpis: { ...p[version].kpis, [k]: v } } })), [version]);
  const setItem      = useCallback((id, field, v) => setPlanData(p => ({
    ...p, [version]: { ...p[version], items: { ...p[version].items, [id]: { ...(p[version].items[id] || {}), [field]: v } } },
  })), [version]);

  // ── Save ──────────────────────────────────────────────────────
  const save = async () => {
    setSaving(true);
    try {
      await api('/api/dashboard-settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: SETTINGS_KEY, value: JSON.stringify(planData) }),
      });
      setSavedMsg('✓ Saved');
      setTimeout(() => setSavedMsg(''), 2500);
    } catch {
      setSavedMsg('❌ Failed');
      setTimeout(() => setSavedMsg(''), 3000);
    } finally { setSaving(false); }
  };

  // ── Derived numbers ───────────────────────────────────────────
  const cur = planData[version];

  const monthRevs  = MONTH_KEYS.map(k => Number(cur.monthly_revenue[k]) || 0);
  const monthAURs  = MONTH_KEYS.map(k => Number(cur.monthly_aur[k])     || 0);
  const monthUnits = monthAURs.map((aur, i) => aur > 0 ? Math.round(monthRevs[i] / aur) : 0);
  const totalRev   = monthRevs.reduce((s,v)=>s+v, 0);
  const totalUnits = monthUnits.reduce((s,v)=>s+v, 0);
  const blendedAUR = totalUnits > 0 ? totalRev / totalUnits : 0;

  const retRate   = (Number(cur.kpis.return_rate) || 0) / 100;
  const monthRets = monthRevs.map(r => Math.round(r * retRate));
  const totalRets = monthRets.reduce((s,v)=>s+v, 0);

  const filteredItems = items.filter(s => {
    if (!search) return true;
    const q = search.toLowerCase();
    return (s.product_name||'').toLowerCase().includes(q) || (s.asin||'').toLowerCase().includes(q) || (s.sku||'').toLowerCase().includes(q);
  });

  const itemRows = filteredItems.map(s => {
    const id  = s.asin || s.sku;
    const ip  = cur.items[id] || {};
    const u   = Number(ip.units) || 0;
    const aur = Number(ip.aur)   || 0;
    const rev = Math.round(u * aur);
    return { id, name: s.product_name || s.productName || id, asin: s.asin, sku: s.sku, u, aur, rev };
  });

  const totItemUnits = itemRows.reduce((s,r)=>s+r.u,   0);
  const totItemRev   = itemRows.reduce((s,r)=>s+r.rev, 0);
  const totItemAUR   = totItemUnits > 0 ? totItemRev / totItemUnits : 0;

  if (loading) return (
    <div style={{ display:'flex', alignItems:'center', justifyContent:'center', height:300, color:'var(--txt3)', fontSize:13 }}>
      Loading plan data…
    </div>
  );

  const accentColor = version === 'goal' ? B.o2 : B.b2;

  return (
    <div style={{ fontFamily:"'Sora',-apple-system,sans-serif", color:'var(--txt)', paddingBottom:60 }}>

      {/* ── Header ────────────────────────────────────────── */}
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:18, paddingTop:4, flexWrap:'wrap', gap:10 }}>
        <div>
          <h2 style={{ margin:'0 0 3px', fontSize:18, fontWeight:800, letterSpacing:'-.02em' }}>{PLAN_YEAR} Sales Planning</h2>
          <div style={{ fontSize:10, color:'var(--txt3)' }}>Revenue targets, KPI goals, and item-level plans for Plan and Goal scenarios</div>
        </div>
        <div style={{ display:'flex', alignItems:'center', gap:10 }}>
          <div style={{ display:'flex', background:'var(--surf)', borderRadius:8, padding:3, border:'1px solid var(--brd)' }}>
            {[['plan','📋 Plan'],['goal','🎯 Goal']].map(([v,lbl]) => (
              <button key={v} onClick={()=>setVersion(v)} style={{
                padding:'5px 18px', borderRadius:6, border:'none', cursor:'pointer',
                fontWeight:700, fontSize:11, letterSpacing:'.04em', textTransform:'uppercase',
                fontFamily:"'Space Grotesk',monospace", transition:'all .18s',
                background: version===v ? (v==='goal' ? B.o2 : B.b2) : 'transparent',
                color: version===v ? '#fff' : 'var(--txt3)',
              }}>{lbl}</button>
            ))}
          </div>
          <button onClick={save} disabled={saving} style={{
            padding:'6px 18px', borderRadius:8, border:'none', cursor:'pointer',
            fontWeight:700, fontSize:11, fontFamily:"'Space Grotesk',monospace",
            background: savedMsg.startsWith('✓') ? B.t2 : accentColor, color:'#fff', transition:'all .2s',
          }}>
            {saving ? '⏳ Saving…' : savedMsg || '💾 Save Plan'}
          </button>
        </div>
      </div>

      {/* context banner */}
      <div style={{
        background:`linear-gradient(135deg,${accentColor}12,${accentColor}06)`,
        border:`1px solid ${accentColor}38`, borderLeft:`3px solid ${accentColor}`,
        borderRadius:8, padding:'8px 14px', marginBottom:18, fontSize:11, color:'var(--txt2)',
      }}>
        {version==='plan'
          ? '📋 Plan — Baseline targets for the year. Your primary operating plan.'
          : '🎯 Goal — Stretch targets. Aspirational numbers that drive team outperformance.'}
      </div>

      {/* ── KPI Targets ───────────────────────────────────── */}
      <Section title="Annual KPI Targets" accent={B.b2}>
        <div style={{ display:'flex', flexWrap:'wrap', gap:10 }}>
          {KPI_DEFS.map(({ key, label, unit, tip }) => (
            <div key={key} title={tip} style={{
              background:'var(--surf)', border:'1px solid var(--brd)', borderRadius:10,
              padding:'10px 14px', flex:'1 1 130px', maxWidth:190,
            }}>
              <div style={{ fontSize:8, color:'var(--txt3)', textTransform:'uppercase', letterSpacing:'.08em', marginBottom:6 }}>{label} ⓘ</div>
              <div style={{ display:'flex', alignItems:'center', gap:4 }}>
                <CellInput value={cur.kpis[key]} onChange={v=>setKPI(key,v)} placeholder={key==='roas'?'0.0':'0'} width={62} />
                <span style={{ fontSize:10, color:'var(--txt3)', fontWeight:600 }}>{unit}</span>
              </div>
            </div>
          ))}
        </div>
      </Section>

      {/* ── Monthly Revenue Plan ──────────────────────────── */}
      <Section title="Monthly Revenue Plan" accent={B.o2}>
        <div style={{ overflowX:'auto' }}>
          <table style={{ borderCollapse:'collapse', width:'100%', minWidth:920, fontSize:10 }}>
            <thead>
              <tr>
                <th style={{ ...thS, textAlign:'left', width:120 }}>Metric</th>
                {MONTHS.map(m => <th key={m} style={thS}>{m}</th>)}
                <th style={{ ...thS, background:'rgba(46,111,187,.1)', color:B.b3 }}>Full Year</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style={tdLbl}>Revenue $</td>
                {MONTH_KEYS.map(k => (
                  <td key={k} style={tdS}><CellInput value={cur.monthly_revenue[k]} onChange={v=>setMonthRev(k,v)} /></td>
                ))}
                <td style={{ ...tdS, fontWeight:800, color:B.b3, background:'rgba(46,111,187,.06)', fontSize:11 }}>{f$(totalRev)}</td>
              </tr>
              <tr>
                <td style={tdLbl}>AUR $</td>
                {MONTH_KEYS.map(k => (
                  <td key={k} style={tdS}><CellInput value={cur.monthly_aur[k]} onChange={v=>setMonthAUR(k,v)} placeholder="0.00" /></td>
                ))}
                <td style={{ ...tdS, fontWeight:700, color:'var(--txt2)', background:'rgba(46,111,187,.06)' }}>
                  {blendedAUR > 0 ? `$${blendedAUR.toFixed(2)}` : '—'}
                </td>
              </tr>
              <tr>
                <td style={{ ...tdLbl, color:B.t2 }}>Revenue Units <span style={{ fontSize:8,opacity:.6 }}>(Rev÷AUR)</span></td>
                {monthUnits.map((u,i) => <td key={i} style={{ ...tdS, color:B.t3, fontWeight:600 }}>{u>0?fN(u):'—'}</td>)}
                <td style={{ ...tdS, fontWeight:800, color:B.t2, background:'rgba(46,111,187,.06)', fontSize:11 }}>{totalUnits>0?fN(totalUnits):'—'}</td>
              </tr>
              <tr>
                <td style={{ ...tdLbl, color:'#fb923c' }}>Returns $ <span style={{ fontSize:8,opacity:.6 }}>{retRate>0?`(${(retRate*100).toFixed(1)}%)`:''}</span></td>
                {monthRets.map((r,i) => <td key={i} style={{ ...tdS, color:'rgba(251,146,60,.65)', fontSize:9 }}>{r>0?f$(r):'—'}</td>)}
                <td style={{ ...tdS, fontWeight:700, color:'#fb923c', background:'rgba(46,111,187,.06)' }}>{totalRets>0?f$(totalRets):'—'}</td>
              </tr>
              <tr style={{ borderTop:'1px solid rgba(46,111,187,.2)' }}>
                <td style={{ ...tdLbl, color:'#4ade80', fontWeight:700 }}>Net Revenue</td>
                {MONTH_KEYS.map((k,i) => {
                  const net = (Number(cur.monthly_revenue[k])||0) - monthRets[i];
                  return <td key={k} style={{ ...tdS, color:'rgba(74,222,128,.75)', fontWeight:600, fontSize:9 }}>{net>0?f$(net):'—'}</td>;
                })}
                <td style={{ ...tdS, fontWeight:800, color:'#4ade80', background:'rgba(46,111,187,.06)', fontSize:11 }}>{(totalRev-totalRets)>0?f$(totalRev-totalRets):'—'}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div style={{ fontSize:8.5, color:'var(--txt3)', marginTop:8 }}>
          ⓘ Revenue Units = Revenue $ ÷ AUR &nbsp;·&nbsp; Returns = Revenue $ × Return Rate% (set above) &nbsp;·&nbsp; Net = Revenue − Returns
        </div>
      </Section>

      {/* ── Item Annual Plan ──────────────────────────────── */}
      <Section title={`Item Annual Plan  ·  ${items.length} items`} accent={B.t2}>
        <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:10, flexWrap:'wrap', gap:8 }}>
          <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search items…" style={{
            width:220, fontSize:11, padding:'5px 10px', borderRadius:6,
            border:'1px solid var(--brd)', background:'var(--surf)', color:'var(--txt)', outline:'none',
          }}/>
          <div style={{ display:'flex', gap:18, fontSize:10, color:'var(--txt3)' }}>
            <span>Units: <strong style={{color:B.b3}}>{totItemUnits>0?fN(totItemUnits):'—'}</strong></span>
            <span>Avg AUR: <strong style={{color:'var(--txt2)'}}>{totItemAUR>0?`$${totItemAUR.toFixed(2)}`:'—'}</strong></span>
            <span>Revenue: <strong style={{color:B.o2}}>{totItemRev>0?f$(totItemRev):'—'}</strong></span>
          </div>
        </div>

        {items.length === 0 ? (
          <div style={{ color:'var(--txt3)', fontSize:11, padding:'20px 0' }}>No items in item master.</div>
        ) : (
          <div style={{ overflowX:'auto', maxHeight:540, overflowY:'auto' }}>
            <table style={{ borderCollapse:'collapse', width:'100%', fontSize:10 }}>
              <thead style={{ position:'sticky', top:0, zIndex:2 }}>
                <tr>
                  <th style={{ ...thS, textAlign:'left', minWidth:200, width:280 }}>Item</th>
                  <th style={{ ...thS, textAlign:'left', width:96 }}>ASIN / SKU</th>
                  <th style={{ ...thS, width:92 }}>Annual Units</th>
                  <th style={{ ...thS, width:82 }}>AUR $</th>
                  <th style={{ ...thS, width:88, color:B.b3 }}>Revenue $</th>
                  <th style={{ ...thS, width:88, color:B.t3 }}>Rev Units</th>
                </tr>
              </thead>
              <tbody>
                {itemRows.map(row => {
                  const revUnits = row.aur > 0 ? Math.round(row.rev / row.aur) : row.u;
                  return (
                    <tr key={row.id}>
                      <td style={{ ...tdS, textAlign:'left', fontWeight:600, color:'var(--txt)', maxWidth:280, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                        {row.name}
                      </td>
                      <td style={{ ...tdS, textAlign:'left', fontSize:8.5, color:'var(--txt3)', fontFamily:"'Space Grotesk',monospace" }}>
                        {row.asin || row.sku}
                      </td>
                      <td style={tdS}>
                        <CellInput value={cur.items[row.id]?.units??''} onChange={v=>setItem(row.id,'units',v)} width={78} />
                      </td>
                      <td style={tdS}>
                        <CellInput value={cur.items[row.id]?.aur??''} onChange={v=>setItem(row.id,'aur',v)} placeholder="0.00" width={72} />
                      </td>
                      <td style={{ ...tdS, fontWeight:700, color:row.rev>0?B.b3:'var(--txt3)' }}>{row.rev>0?f$(row.rev):'—'}</td>
                      <td style={{ ...tdS, color:revUnits>0?B.t3:'var(--txt3)' }}>{revUnits>0?fN(revUnits):'—'}</td>
                    </tr>
                  );
                })}
              </tbody>
              <tfoot>
                <tr style={{ borderTop:'2px solid rgba(46,111,187,.35)', background:'rgba(27,79,138,.12)' }}>
                  <td colSpan={2} style={{ ...tdS, textAlign:'left', fontWeight:700, fontSize:11 }}>
                    TOTAL ({itemRows.length} items)
                  </td>
                  <td style={{ ...tdS, fontWeight:800, color:B.b3, fontSize:11 }}>{totItemUnits>0?fN(totItemUnits):'—'}</td>
                  <td style={{ ...tdS, color:'var(--txt3)', fontSize:9 }}>{totItemAUR>0?`$${totItemAUR.toFixed(2)} avg`:'—'}</td>
                  <td style={{ ...tdS, fontWeight:800, color:B.o2, fontSize:12 }}>{totItemRev>0?f$(totItemRev):'—'}</td>
                  <td style={{ ...tdS, fontWeight:700, color:B.t2, fontSize:11 }}>{totItemUnits>0?fN(totItemUnits):'—'}</td>
                </tr>
              </tfoot>
            </table>
          </div>
        )}
      </Section>

    </div>
  );
}
