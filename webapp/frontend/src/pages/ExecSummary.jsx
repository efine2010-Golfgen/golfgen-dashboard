import { useState, useEffect } from 'react';

const B = {
  b2:'#2E6FBB', o2:'#E8821E', o3:'#F5A54A', t2:'#1AA392', t3:'#4DC5B8',
  sub:'#5b7fa0'
};
const f$ = v => v == null ? '—' : v >= 1e6 ? `$${(v/1e6).toFixed(2)}M` : v >= 1000 ? `$${(v/1000).toFixed(1)}K` : `$${Number(v).toFixed(0)}`;

function stackedBarSVG(data, W=1100, H=160) {
  if (!data || data.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No channel data</div>';
  const CC = {amazon:B.b2, walmart_marketplace:B.o2, walmart_stores:B.o3, shopify:B.t2, store_other:B.t3};
  const keys = ['amazon','walmart_marketplace','walmart_stores','shopify','store_other'];
  const labels = {amazon:'Amazon',walmart_marketplace:'Walmart Mkt',walmart_stores:'Walmart Stores',shopify:'Shopify',store_other:'Stores'};
  const pad = {t:28,r:16,b:24,l:54};
  const maxT = Math.max(...data.map(d => keys.reduce((s,k)=>s+(d[k]||0),0)));
  const iw = W-pad.l-pad.r, ih = H-pad.t-pad.b;
  const bw = Math.max(8, (iw/data.length)*.65);
  const x = i => pad.l + ((i+.5)/data.length)*iw;
  const step = Math.max(1, Math.floor(data.length/7));
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  for (let i=0;i<=3;i++) { const v=maxT*(i/3); s+=`<line x1="${pad.l}" y1="${(pad.t+ih*(1-i/3)).toFixed(1)}" x2="${W-pad.r}" y2="${(pad.t+ih*(1-i/3)).toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/><text x="${pad.l-5}" y="${(pad.t+ih*(1-i/3)+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#374f66">${f$(v)}</text>`; }
  data.forEach((d,i) => {
    let base = 0;
    [...keys].reverse().forEach(k => {
      const h = ((d[k]||0)/maxT)*ih;
      const yy = pad.t+ih-base-h; base += h;
      s += `<rect x="${(x(i)-bw/2).toFixed(1)}" y="${yy.toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.max(0,h).toFixed(1)}" fill="${CC[k]}" rx="2" opacity=".88"/>`;
    });
    if (i%step===0) s += `<text x="${x(i).toFixed(1)}" y="${H-4}" text-anchor="middle" font-size="8" fill="#374f66">${d.week||d.week_start||''}</text>`;
  });
  const legs = keys.filter(k => data.some(d => (d[k]||0) > 0));
  legs.forEach((k,i) => s += `<g transform="translate(${pad.l+i*100},${pad.t-16})"><rect width="8" height="8" y="-1" rx="2" fill="${CC[k]}"/><text x="11" y="7" font-size="9" fill="${B.sub}">${labels[k]}</text></g>`);
  return s + '</svg>';
}

const SG = (sz = 12, wt = 400) => ({
  fontFamily: "'Space Grotesk',monospace", fontSize: sz, fontWeight: wt,
});
const DM = (sz = 22) => ({
  fontFamily: "'DM Serif Display',Georgia,serif", fontSize: sz,
});

export default function ExecSummary({ filters = {} }) {
  const [channel, setChannel] = useState(null);
  const [loading, setLoading] = useState(true);
  const [period, setPeriod] = useState('last_30d');

  useEffect(() => {
    setLoading(true);
    const params = new URLSearchParams({ period });
    if (filters.division) params.append('division', filters.division);
    fetch(`/api/sales/by-channel?${params}`)
      .then(r => r.json())
      .then(d => { setChannel(Array.isArray(d) ? d : []); setLoading(false); })
      .catch(() => setLoading(false));
  }, [period, filters.division]);

  const PERIODS = [
    { label: '7D', value: 'last_7d' },
    { label: '30D', value: 'last_30d' },
    { label: '90D', value: 'last_90d' },
    { label: '180D', value: 'last_180d' },
  ];

  return (
    <div style={{ padding: "0 24px 40px" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <div>
          <h1 style={{ ...DM(22), margin: 0, color: "var(--txt)" }}>Executive Summary</h1>
          <p style={{ ...SG(11), color: "var(--txt3)", margin: "2px 0 0" }}>
            Cross-channel overview · {filters.division || "All Divisions"}
          </p>
        </div>
      </div>

      {/* Revenue by Channel */}
      <div style={{ background: "var(--card)", border: "1px solid var(--brd)", borderRadius: 12, padding: 0, marginBottom: 16 }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 16px", borderBottom: "1px solid var(--brd)" }}>
          <span style={{ ...SG(12, 700), color: "var(--txt)" }}>Revenue by Channel</span>
          <div style={{ display: "flex", gap: 4 }}>
            {PERIODS.map(p => (
              <button key={p.value} onClick={() => setPeriod(p.value)} style={{
                ...SG(10, period === p.value ? 700 : 500),
                padding: "3px 10px", borderRadius: 6,
                border: "1px solid " + (period === p.value ? "var(--acc1)" : "var(--brd)"),
                background: period === p.value ? "rgba(46,207,170,.12)" : "transparent",
                color: period === p.value ? "var(--acc1)" : "var(--txt3)",
                cursor: "pointer",
              }}>{p.label}</button>
            ))}
          </div>
        </div>
        <div style={{ padding: "14px 16px" }}>
          {loading ? (
            <div style={{ textAlign: "center", padding: 30, color: "var(--txt3)", ...SG(11) }}>Loading...</div>
          ) : (
            <div dangerouslySetInnerHTML={{ __html: stackedBarSVG(channel || []) }} />
          )}
        </div>
        <div style={{ display: "flex", gap: 14, padding: "0 16px 12px", flexWrap: "wrap" }}>
          {[['Amazon',B.b2],['Walmart Mkt',B.o2],['Walmart Stores',B.o3],['Shopify',B.t2],['Stores',B.t3]].map(([l,c]) => (
            <div key={l} style={{ display: "flex", alignItems: "center", gap: 5 }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, background: c, flexShrink: 0 }} />
              <span style={{ ...SG(9, 600), color: "var(--txt3)" }}>{l}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Placeholder for more cross-channel content */}
      <div style={{ padding: "40px 20px", textAlign: "center", color: "var(--txt3)", border: "1px dashed var(--brd)", borderRadius: 8, background: "var(--card)" }}>
        <p style={{ ...SG(11), color: "var(--txt3)" }}>Additional cross-channel metrics, P&L rollup, and KPI comparisons coming soon.</p>
      </div>
    </div>
  );
}
