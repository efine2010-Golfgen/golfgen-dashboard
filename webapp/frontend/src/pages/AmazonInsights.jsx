import { useState, useEffect } from 'react';

/* ── Palette (matches Sales.jsx B object) ── */
const B = {
  b1:'#1B4F8A', b2:'#2E6FBB', b3:'#5B9FD4',
  o1:'#D4600A', o2:'#E8821E', o3:'#F5A54A',
  t1:'#0F7A6E', t2:'#1AA392', t3:'#4DC5B8',
  gold:'#F5B731',
  brd:'#1a2f4a', surf:'#0c1a2e', card:'#122138',
  sub:'#5b7fa0', dim:'#374f66',
};

const f$ = v => v == null ? '—' : '$' + Number(v).toLocaleString('en-US', {minimumFractionDigits:0, maximumFractionDigits:0});
const fP = v => v == null ? '—' : `${(v*100).toFixed(1)}%`;
const fX = v => v == null ? '—' : `${Number(v).toFixed(2)}x`;
const fN = v => v == null ? '—' : v >= 1000 ? `${(v/1000).toFixed(1)}K` : Math.round(v).toLocaleString();
const dp = (a,b) => b ? ((a-b)/b*100) : null;

async function apiFetch(path, params = {}) {
  const q = new URLSearchParams(Object.fromEntries(
    Object.entries(params).filter(([,v]) => v != null && v !== '' && v !== 'all')
  )).toString();
  const res = await fetch(`/api/sales/${path}${q ? '?' + q : ''}`);
  if (!res.ok) throw new Error(`${res.status}`);
  const data = await res.json();
  if (data?.error) throw new Error(data.error);
  return data;
}

function Spinner() {
  return <div style={{display:'flex',alignItems:'center',justifyContent:'center',padding:'32px 0'}}>
    <div style={{width:22,height:22,border:`3px solid ${B.brd}`,borderTopColor:B.b2,borderRadius:'50%',animation:'spin .7s linear infinite'}}/>
    <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
  </div>;
}

/* ── Score gauge arc ── */
function Gauge({ score, size=110 }) {
  const col = score >= 75 ? B.t2 : score >= 55 ? B.gold : '#ef4444';
  const r = 42, cx = size/2, cy = size/2;
  const circ = Math.PI * r; // half circle
  const offset = circ - (score/100) * circ;
  return (
    <svg width={size} height={size/2+14} viewBox={`0 0 ${size} ${size/2+14}`}>
      <path d={`M ${cx-r} ${cy} A ${r} ${r} 0 0 1 ${cx+r} ${cy}`}
        fill="none" stroke={B.brd} strokeWidth={9} strokeLinecap="round"/>
      <path d={`M ${cx-r} ${cy} A ${r} ${r} 0 0 1 ${cx+r} ${cy}`}
        fill="none" stroke={col} strokeWidth={9} strokeLinecap="round"
        strokeDasharray={circ} strokeDashoffset={offset}
        style={{transition:'stroke-dashoffset .8s ease'}}/>
      <text x={cx} y={cy+2} textAnchor="middle"
        style={{fontFamily:"'DM Serif Display',Georgia,serif",fontSize:22,fill:col,fontWeight:700}}>{score}</text>
      <text x={cx} y={cy+14} textAnchor="middle"
        style={{fontFamily:"'Space Grotesk',monospace",fontSize:8,fill:B.sub,textTransform:'uppercase',letterSpacing:'.08em'}}>/ 100</text>
    </svg>
  );
}

/* ── Score bar ── */
function ScoreBar({ label, score, color }) {
  return (
    <div style={{marginBottom:8}}>
      <div style={{display:'flex',justifyContent:'space-between',marginBottom:3}}>
        <span style={{fontSize:10,color:'var(--txt3)'}}>{label}</span>
        <span style={{fontSize:10,fontWeight:700,color}}>{score}</span>
      </div>
      <div style={{height:5,background:'var(--brd)',borderRadius:3,overflow:'hidden'}}>
        <div style={{height:'100%',width:`${score}%`,background:color,borderRadius:3,transition:'width .7s ease'}}/>
      </div>
    </div>
  );
}

/* ── Insight card ── */
function InsightCard({ type, title, desc, action }) {
  const cfg = {
    ok:    { border:B.t2,   bg:'rgba(26,163,146,.08)',  icon:'✅', badge:'Positive',  badgeCol:B.t2 },
    warn:  { border:B.gold, bg:'rgba(245,183,49,.07)',  icon:'⚠️',  badge:'Watch',     badgeCol:B.gold },
    alert: { border:'#ef4444', bg:'rgba(239,68,68,.07)', icon:'🚨', badge:'Action',   badgeCol:'#f87171' },
    info:  { border:B.b2,   bg:'rgba(46,111,187,.07)',  icon:'ℹ️', badge:'Info',      badgeCol:B.b3 },
  }[type] || { border:B.sub, bg:'transparent', icon:'•', badge:'', badgeCol:B.sub };
  return (
    <div style={{background:cfg.bg, border:`1px solid ${cfg.border}`, borderLeft:`3px solid ${cfg.border}`,
      borderRadius:10, padding:'12px 14px', marginBottom:8}}>
      <div style={{display:'flex',alignItems:'flex-start',gap:10}}>
        <span style={{fontSize:16,flexShrink:0,marginTop:1}}>{cfg.icon}</span>
        <div style={{flex:1}}>
          <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:3,flexWrap:'wrap'}}>
            <span style={{fontSize:8,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',
              padding:'2px 7px',borderRadius:99,background:`${cfg.border}22`,color:cfg.badgeCol}}>
              {cfg.badge}
            </span>
          </div>
          <div style={{fontSize:12,fontWeight:600,color:'var(--txt)',marginBottom:3,lineHeight:1.4}}>{title}</div>
          <div style={{fontSize:11,color:'var(--txt3)',lineHeight:1.5}}>{desc}</div>
        </div>
      </div>
    </div>
  );
}

/* ── KPI tile ── */
function KpiTile({ label, value, sub, delta, accent }) {
  const pos = delta == null ? null : delta >= 0;
  return (
    <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`3px solid ${accent}`,
      borderRadius:12,padding:'14px 16px',flex:'1 1 0',minWidth:130}}>
      <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:6}}>{label}</div>
      <div style={{fontSize:22,fontWeight:800,color:'var(--txt)',lineHeight:1,marginBottom:4}}>{value}</div>
      {sub && <div style={{fontSize:10,color:'var(--txt3)'}}>{sub}</div>}
      {delta != null && (
        <span style={{fontSize:10,fontWeight:700,color:pos?B.t2:'#f87171'}}>
          {pos?'▲':'▼'} {Math.abs(delta).toFixed(1)}% vs LY
        </span>
      )}
    </div>
  );
}

/* ── Section header ── */
function SectionHdr({ label, accent }) {
  return (
    <div style={{display:'flex',alignItems:'center',gap:10,margin:'22px 0 10px'}}>
      <div style={{fontSize:9,fontWeight:800,textTransform:'uppercase',letterSpacing:'.12em',
        padding:'3px 10px',borderRadius:99,background:`${accent}22`,color:accent,
        border:`1px solid ${accent}44`,whiteSpace:'nowrap'}}>{label}</div>
      <div style={{flex:1,height:1,background:'var(--brd)'}}/>
    </div>
  );
}

export default function AmazonInsights({ filters = {} }) {
  const [metrics, setMetrics]   = useState(null);
  const [periodCols, setPeriodCols] = useState(null);
  const [tod, setTod]           = useState(null);
  const [loading, setLoading]   = useState(true);
  const [lastRefresh, setLastRefresh] = useState(null);

  const mp = filters.marketplace || 'US';
  const baseParams = { marketplace: mp, division: filters.division || undefined, customer: filters.customer || undefined };

  useEffect(() => {
    setLoading(true);
    const p = { ...baseParams };
    Promise.all([
      apiFetch('summary', p).catch(() => null),
      apiFetch('period-comparison', { ...p, view: 'mtd' }).catch(() => null),
    ]).then(([m, pc]) => {
      setMetrics(m);
      setPeriodCols(pc);
      setTod(pc?.columns?.find?.(c => c.period === 'Today') || pc?.Today || null);
      setLastRefresh(new Date().toLocaleTimeString('en-US', {hour:'2-digit',minute:'2-digit'}));
      setLoading(false);
    });
  }, [mp, filters.division, filters.customer]);

  if (loading) return (
    <div style={{padding:'40px 0'}}>
      <Spinner/>
      <p style={{textAlign:'center',color:B.sub,fontSize:12,marginTop:8}}>Loading insights…</p>
    </div>
  );

  /* ── Derive metrics ── */
  const m   = metrics?.summary || metrics || {};
  const pc  = periodCols || {};

  // Period helpers — handles both array and object response shapes
  const getPeriod = (key) => {
    if (pc[key]) return pc[key];
    if (Array.isArray(pc.columns)) return pc.columns.find(c => c.period === key) || {};
    return {};
  };

  const mtd = getPeriod('MTD');
  const ytd = getPeriod('YTD');
  const today = getPeriod('Today');

  const rev    = mtd.sales      || m.sales      || 0;
  const lyRev  = mtd.ly_sales   || m.ly_sales   || 0;
  const units  = mtd.units      || m.units      || 0;
  const lyUnits= mtd.ly_units   || m.ly_units   || 0;
  const ytdRev = ytd.sales      || 0;
  const lyYtd  = ytd.ly_sales   || 0;

  const retRaw  = mtd.returns_amount || m.returns_amount || 0;
  const retRate = rev > 0 ? retRaw / rev : 0;
  const adSpend = m.ad_spend || 0;
  const roas    = m.roas || 0;
  const tacos   = m.tacos || 0;
  const dos     = m.dos || 0;
  const conv    = m.conversion || m.cvr || 0;
  const atcRate = m.atc_rate || m.add_to_cart_rate || 0;

  const mtdChg = dp(rev, lyRev);
  const ytdChg = dp(ytdRev, lyYtd);
  const unitChg = dp(units, lyUnits);

  /* ── Health scores ── */
  const paceScore = rev > 0 && lyRev > 0 ? Math.min(100, Math.round((rev / lyRev) * 50 + 50)) : 65;
  const convScore = conv > 0 ? Math.min(100, Math.round((conv / 0.14) * 85)) : 65;
  const roasScore = roas > 0 ? Math.min(100, Math.round((roas / 3.5) * 100)) : 60;
  const atcScore  = atcRate > 0 ? Math.min(100, Math.round((atcRate / 0.22) * 80)) : 68;
  const retScore  = Math.min(100, Math.max(0, Math.round((1 - retRate / 0.05) * 100)));
  const dosScore  = dos > 0 ? Math.min(100, Math.round(Math.min(dos, 90) / 90 * 100)) : 55;
  const overall   = Math.round((paceScore + convScore + roasScore + atcScore + retScore + dosScore) / 6);

  const scoreColor = overall >= 75 ? B.t2 : overall >= 55 ? B.gold : '#ef4444';

  /* ── Build action items ── */
  const insights = [];
  if (atcRate > 0 && atcRate < 0.18) insights.push({ type:'alert', title:`🛒 ATC Rate at ${fP(atcRate)} — below 22% benchmark`, desc:'Add-to-cart rate is below benchmark. Review top ASINs for pricing gaps, competitor undercutting, or missing A+ content.' });
  if (tacos > 0.16) insights.push({ type:'warn', title:`📣 TACOS at ${fP(tacos)} — near 16% ceiling`, desc:'Ad spend as % of revenue is elevated. Consider pausing underperforming campaigns or reducing bids on low-ROAS keywords.' });
  if (dos > 0 && dos < 30) insights.push({ type:'alert', title:`📦 Days of Supply at ${dos}d — reorder needed`, desc:`Stock is running low. Create an FBA shipment plan now to avoid stockouts and BSR rank drop.` });
  if (mtdChg !== null && mtdChg >= 5) insights.push({ type:'ok', title:`✅ MTD revenue ▲${mtdChg.toFixed(1)}% vs LY — strong pace`, desc:'Revenue is tracking well above last year. Consider increasing ad budget to capture share while TACOS is healthy.' });
  else if (mtdChg !== null && mtdChg < -3) insights.push({ type:'warn', title:`📉 MTD revenue ▼${Math.abs(mtdChg).toFixed(1)}% vs LY`, desc:'Revenue trailing last year. Check for suppressed listings, lost Buy Box, or gaps in ad coverage.' });
  if (retRate > 0 && retRate < 0.04) insights.push({ type:'ok', title:`✅ Return rate ${fP(retRate)} — below 4% target`, desc:'Returns are well-controlled. Maintain current listing accuracy and A+ content.' });
  else if (retRate > 0.07) insights.push({ type:'alert', title:`⚠ Return rate ${fP(retRate)} — above 7% threshold`, desc:'High returns may indicate listing inaccuracy, product issues, or sizing confusion. Review top return reasons in Seller Central.' });
  if (roas > 0 && roas < 2.5) insights.push({ type:'warn', title:`📊 ROAS at ${fX(roas)} — below 2.5× floor`, desc:'Ad return on spend is low. Review campaign structure, negative keywords, and bid strategy.' });
  if (roas >= 3.5) insights.push({ type:'ok', title:`📊 ROAS at ${fX(roas)} — above 3.5× target`, desc:'Ad efficiency is strong. Reinvest a portion of returns into top-performing campaigns to scale revenue.' });
  if (dos >= 60) insights.push({ type:'info', title:`📦 Inventory healthy at ${dos}d supply`, desc:'Stock levels are comfortable. Monitor velocity weekly and plan next replenishment order.' });
  if (insights.length === 0) insights.push({ type:'info', title:'✅ All metrics within healthy ranges', desc:'No urgent actions detected. Review the detailed tabs for optimization opportunities in advertising, inventory, and pricing.' });

  const positives = insights.filter(i => i.type === 'ok');
  const warnings  = insights.filter(i => i.type === 'warn' || i.type === 'alert');
  const infoItems = insights.filter(i => i.type === 'info');

  return (
    <div style={{paddingTop:8}}>

      {/* ── Page header ── */}
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:16,flexWrap:'wrap',gap:8}}>
        <div>
          <div style={{fontSize:18,fontWeight:800,color:'var(--txt)',fontFamily:"'DM Serif Display',Georgia,serif",letterSpacing:'-.01em'}}>
            Business Intelligence Hub
          </div>
          <div style={{fontSize:11,color:'var(--txt3)',marginTop:2}}>
            AI-generated insights across revenue, advertising, inventory, and conversion
          </div>
        </div>
        {lastRefresh && (
          <div style={{fontSize:9,color:B.sub,background:'var(--card)',border:'1px solid var(--brd)',
            borderRadius:6,padding:'4px 10px',fontFamily:"'Space Grotesk',monospace"}}>
            Refreshed {lastRefresh}
          </div>
        )}
      </div>

      {/* ── KPI Row ── */}
      <div style={{display:'flex',gap:10,marginBottom:4,flexWrap:'wrap'}}>
        <KpiTile label="MTD Revenue" value={f$(rev)} sub={`LY: ${f$(lyRev)}`} delta={mtdChg} accent={B.o2}/>
        <KpiTile label="YTD Revenue" value={f$(ytdRev)} sub={`LY: ${f$(lyYtd)}`} delta={ytdChg} accent={B.gold}/>
        <KpiTile label="MTD Units" value={fN(units)} sub={`LY: ${fN(lyUnits)}`} delta={unitChg} accent={B.b2}/>
        <KpiTile label="ROAS" value={fX(roas)} sub={roas >= 2.5 ? '✓ Above 2.5× floor' : '⚠ Below floor'} delta={null} accent={roas >= 2.5 ? B.t2 : '#ef4444'}/>
        <KpiTile label="TACOS" value={fP(tacos)} sub={tacos <= 0.16 ? '✓ Under 16% ceiling' : '⚠ Near ceiling'} delta={null} accent={tacos <= 0.14 ? B.t2 : B.gold}/>
        <KpiTile label="Days of Supply" value={dos > 0 ? `${dos}d` : '—'} sub={dos < 30 ? '⚠ Reorder soon' : dos < 60 ? '✓ Healthy' : '📦 Surplus'} delta={null} accent={dos < 30 ? '#ef4444' : dos < 60 ? B.t2 : B.b3}/>
      </div>

      {/* ── Two-column layout: Health + Insights ── */}
      <div style={{display:'grid',gridTemplateColumns:'280px 1fr',gap:14,marginTop:16,alignItems:'start'}}>

        {/* LEFT: Health Scorecard */}
        <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderTop:`3px solid ${scoreColor}`,borderRadius:14,padding:16}}>
          <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',marginBottom:12}}>
            Business Health Score
          </div>
          <div style={{display:'flex',justifyContent:'center',marginBottom:12}}>
            <Gauge score={overall}/>
          </div>
          <div style={{textAlign:'center',marginBottom:14}}>
            <div style={{fontSize:11,fontWeight:700,color:scoreColor}}>
              {overall >= 75 ? '🟢 Healthy' : overall >= 55 ? '🟡 Needs Attention' : '🔴 Action Required'}
            </div>
            <div style={{fontSize:9,color:B.sub,marginTop:2}}>Based on 6 core performance signals</div>
          </div>
          <div style={{borderTop:'1px solid var(--brd)',paddingTop:12}}>
            <ScoreBar label="Revenue Pace" score={paceScore} color={B.o2}/>
            <ScoreBar label="Conversion Rate" score={convScore} color={B.t2}/>
            <ScoreBar label="ROAS / Ad Eff." score={roasScore} color={B.b3}/>
            <ScoreBar label="ATC / Funnel" score={atcScore} color={B.gold}/>
            <ScoreBar label="Return Rate" score={retScore} color={B.t2}/>
            <ScoreBar label="Inventory DOS" score={dosScore} color={B.b3}/>
          </div>
        </div>

        {/* RIGHT: Insights feed */}
        <div>
          {warnings.length > 0 && (
            <>
              <SectionHdr label={`⚠ ${warnings.length} Action Item${warnings.length > 1 ? 's' : ''}`} accent="#ef4444"/>
              {warnings.map((ins, i) => <InsightCard key={i} {...ins}/>)}
            </>
          )}
          {positives.length > 0 && (
            <>
              <SectionHdr label={`✅ ${positives.length} Positive Signal${positives.length > 1 ? 's' : ''}`} accent={B.t2}/>
              {positives.map((ins, i) => <InsightCard key={i} {...ins}/>)}
            </>
          )}
          {infoItems.length > 0 && (
            <>
              <SectionHdr label="ℹ️ Status" accent={B.b2}/>
              {infoItems.map((ins, i) => <InsightCard key={i} {...ins}/>)}
            </>
          )}
        </div>
      </div>

      {/* ── Ad Performance Summary ── */}
      <SectionHdr label="📣 Advertising Snapshot" accent={B.gold}/>
      <div style={{display:'flex',gap:10,flexWrap:'wrap'}}>
        {[
          { label:'Ad Spend', value:f$(adSpend), accent:B.gold },
          { label:'ROAS', value:fX(roas), accent: roas >= 2.5 ? B.t2 : '#ef4444' },
          { label:'TACOS', value:fP(tacos), accent: tacos <= 0.16 ? B.t2 : B.gold },
          { label:'Conversion Rate', value:fP(conv), accent:B.t2 },
          { label:'Return Rate', value:fP(retRate), accent: retRate <= 0.04 ? B.t2 : retRate > 0.07 ? '#ef4444' : B.gold },
        ].map(k => (
          <div key={k.label} style={{flex:'1 1 0',minWidth:110,background:'var(--card)',border:'1px solid var(--brd)',
            borderTop:`3px solid ${k.accent}`,borderRadius:10,padding:'10px 12px'}}>
            <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:4}}>{k.label}</div>
            <div style={{fontSize:18,fontWeight:800,color:'var(--txt)',lineHeight:1}}>{k.value}</div>
          </div>
        ))}
      </div>

      {/* ── Ask Claude prompt ── */}
      <div style={{marginTop:20,background:'rgba(46,111,187,.06)',border:'1px solid rgba(46,111,187,.2)',
        borderLeft:`3px solid ${B.b2}`,borderRadius:12,padding:'14px 16px'}}>
        <div style={{fontSize:11,fontWeight:700,color:B.b3,marginBottom:4}}>💬 Ask Claude about your business</div>
        <div style={{fontSize:11,color:'var(--txt3)',lineHeight:1.6}}>
          Use the <strong style={{color:'var(--txt)'}}>Ask Claude</strong> bar above to ask natural language questions about your data —
          e.g. <em style={{color:B.b3}}>"Why is my conversion rate dropping this month?"</em> or{' '}
          <em style={{color:B.b3}}>"What should I do to improve ROAS before Q4?"</em>
        </div>
      </div>

    </div>
  );
}
