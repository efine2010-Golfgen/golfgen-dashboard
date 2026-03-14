import { useState, useEffect, useCallback, useRef } from 'react';

// ── BRAND COLORS ─────────────────────────────────────────────────
const B = {
  b1:'#1B4F8A', b2:'#2E6FBB', b3:'#5B9FD4',
  o1:'#D4600A', o2:'#E8821E', o3:'#F5A54A',
  t1:'#0F7A6E', t2:'#1AA392', t3:'#4DC5B8',
  brd:'#1a2f4a', surf:'#0c1a2e', card:'#122138',
  sub:'#5b7fa0', dim:'#374f66'
};

// ── CONSTANTS ─────────────────────────────────────────────────────
const PERIODS = {
  Exec:    ['Today','Yesterday','WTD','MTD','YTD'],
  Daily:   ['Today','Yesterday','2 Days Ago','3 Days Ago','4 Days Ago','5 Days Ago','6 Days Ago'],
  Weekly:  ['WTD','Last Week','4 Weeks','8 Weeks','13 Weeks','26 Weeks'],
  Monthly: ['MTD','Last Month','2 Months Ago','3 Months Ago','Last 12 Months'],
  Yearly:  ['2026 YTD','2025 YTD (Same Period)','2025 Full Year','2024 Full Year'],
};
const PERIOD_API_MAP = {
  'Today':'today','Yesterday':'yesterday','WTD':'wtd','MTD':'mtd','YTD':'ytd',
  '2 Days Ago':'2_days_ago','3 Days Ago':'3_days_ago','4 Days Ago':'4_days_ago',
  '5 Days Ago':'5_days_ago','6 Days Ago':'6_days_ago',
  'Last Week':'last_week','4 Weeks':'last_4w','8 Weeks':'last_8w',
  '13 Weeks':'last_13w','26 Weeks':'last_26w',
  'Last Month':'last_month','2 Months Ago':'2_months_ago','3 Months Ago':'3_months_ago',
  'Last 12 Months':'last_12m',
  '2026 YTD':'2026_ytd','2025 YTD (Same Period)':'2025_ytd',
  '2025 Full Year':'2025_full','2024 Full Year':'2024_full',
};
const GOLF_CUSTOMERS    = ['All Channels','Amazon','Walmart','Shopify','First Tee'];
const HW_CUSTOMERS      = ['All Channels','Belk',"Albertson's",'Family Dollar','Hobby Lobby'];
const CHART_PERIODS     = ['7D','30D','60D','90D','180D'];
const CHART_PERIOD_API  = {'7D':'last_7d','30D':'last_30d','60D':'last_60d','90D':'last_90d','180D':'last_180d'};
const VIEW_TABS         = ['Exec','Daily','Weekly','Monthly','Yearly','Custom'];
const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

// ── FORMATTERS ─────────────────────────────────────────────────────
const f$ = v => v == null ? '—' : v >= 1e6 ? `$${(v/1e6).toFixed(2)}M` : v >= 1000 ? `$${(v/1000).toFixed(1)}K` : `$${Number(v).toFixed(0)}`;
const fN = v => v == null ? '—' : v >= 1000 ? `${(v/1000).toFixed(1)}K` : Math.round(v).toLocaleString();
const fP = v => v == null ? '—' : `${(v*100).toFixed(1)}%`;
const fX = v => v == null ? '—' : `${Number(v).toFixed(2)}x`;
const dp = (a,b) => b ? ((a-b)/b*100) : null;

// ── API HELPER ─────────────────────────────────────────────────────
async function apiFetch(path, params = {}) {
  const q = new URLSearchParams(Object.fromEntries(
    Object.entries(params).filter(([,v]) => v != null && v !== '' && v !== 'all' && v !== 'All Channels')
  )).toString();
  const url = `/api/sales/${path}${q ? '?' + q : ''}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  const data = await res.json();
  if (data && data.error) throw new Error(data.error);
  return data;
}
const toArr = v => Array.isArray(v) ? v : [];

// ── SVG CHART BUILDERS ─────────────────────────────────────────────
function dualLineSVG(data, k1, k2, c1, c2, fmtFn, W=1100, H=130) {
  if (!data || data.length < 2) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No data for this period</div>';
  const pad = {t:14,r:16,b:24,l:54};
  const vals1 = data.map(d => d[k1]).filter(v => v != null);
  const vals2 = data.map(d => d[k2]).filter(v => v != null);
  const all = [...vals1, ...vals2];
  const mn = Math.min(...all), mx = Math.max(...all), rng = mx - mn || 1;
  const iw = W-pad.l-pad.r, ih = H-pad.t-pad.b;
  const x = i => pad.l + (i/(data.length-1||1))*iw;
  const y = v => pad.t + ih - ((v-mn)/rng)*ih;
  const pts1 = data.map((d,i) => d[k1]!=null ? `${x(i).toFixed(1)},${y(d[k1]).toFixed(1)}` : null).filter(Boolean).join(' ');
  const pts2 = data.map((d,i) => d[k2]!=null ? `${x(i).toFixed(1)},${y(d[k2]).toFixed(1)}` : null).filter(Boolean).join(' ');
  const area = `M${x(0)},${y(data[0][k1]||mn)} ${data.map((d,i)=>`L${x(i)},${y(d[k1]||mn)}`).join(' ')} L${x(data.length-1)},${pad.t+ih} L${x(0)},${pad.t+ih} Z`;
  const step = Math.max(1, Math.floor(data.length/7));
  const uid = `g${k1}${W}${Math.random().toString(36).slice(2,6)}`;
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  s += `<defs><linearGradient id="${uid}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${c1}" stop-opacity=".2"/><stop offset="100%" stop-color="${c1}" stop-opacity="0"/></linearGradient></defs>`;
  for (let i=0; i<=4; i++) { const v=mn+(rng/4)*i; s+=`<line x1="${pad.l}" y1="${y(v).toFixed(1)}" x2="${W-pad.r}" y2="${y(v).toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/><text x="${pad.l-5}" y="${(y(v)+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#374f66">${fmtFn(v)}</text>`; }
  data.filter((_,i) => i%step===0||i===data.length-1).forEach((d) => {
    const i = data.indexOf(d);
    const label = d.date ? d.date.slice(5) : d.d || '';
    s += `<text x="${x(i).toFixed(1)}" y="${H-4}" text-anchor="middle" font-size="8" fill="#374f66">${label}</text>`;
  });
  s += `<path d="${area}" fill="url(#${uid})"/>`;
  s += `<polyline points="${pts1}" fill="none" stroke="${c1}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>`;
  if (pts2) s += `<polyline points="${pts2}" fill="none" stroke="${c2}" stroke-width="1.5" stroke-dasharray="4 3" stroke-linecap="round" stroke-linejoin="round"/>`;
  return s + '</svg>';
}

function stackedBarSVG(data, W=1100, H=130) {
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

function yoyBarSVG(data, W=1100, H=180) {
  if (!data || data.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No YOY data</div>';
  const CC = {y2024:B.dim, y2025:B.b2, y2026:B.o2};
  const pad = {t:28,r:16,b:26,l:58};
  const maxV = Math.max(...data.flatMap(d => [d.y2024||0, d.y2025||0, d.y2026||0]), 1);
  const iw = W-pad.l-pad.r, ih = H-pad.t-pad.b;
  const slotW = iw / data.length;
  // Group occupies 62% of the slot — remaining 38% becomes inter-month breathing room
  const groupW = slotW * 0.62;
  const innerGap = Math.max(2, groupW * 0.05); // ~5% of group as inner gap
  const bw = Math.max((groupW - innerGap * 2) / 3, 3);
  const x = (mi,yi) => pad.l + mi * slotW + (slotW - groupW) / 2 + yi * (bw + innerGap);
  const h = v => Math.max((v/maxV)*ih, v > 0 ? 4 : 0);
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  for (let i=0;i<=3;i++) { const v=maxV*(i/3); s+=`<line x1="${pad.l}" y1="${(pad.t+ih*(1-i/3)).toFixed(1)}" x2="${W-pad.r}" y2="${(pad.t+ih*(1-i/3)).toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/><text x="${pad.l-5}" y="${(pad.t+ih*(1-i/3)+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#374f66">${f$(v)}</text>`; }
  data.forEach((d,mi) => {
    [[d.y2024,'y2024',0],[d.y2025,'y2025',1],[d.y2026,'y2026',2]].forEach(([v,k,yi]) => {
      if (v == null) return;
      const hh = h(v);
      const bx = x(mi,yi);
      s += `<rect x="${bx.toFixed(1)}" y="${(pad.t+ih-hh).toFixed(1)}" width="${bw.toFixed(1)}" height="${hh.toFixed(1)}" fill="${CC[k]}" rx="2" opacity="${k==='y2026'?1:.82}"/>`;
      if (v > 0 && k === 'y2026') s += `<text x="${(bx+bw/2).toFixed(1)}" y="${(pad.t+ih-hh-3).toFixed(1)}" text-anchor="middle" font-size="7" fill="${B.o3}">${f$(v)}</text>`;
    });
    s += `<text x="${(pad.l+(mi+.5)/data.length*iw).toFixed(1)}" y="${H-6}" text-anchor="middle" font-size="9" fill="#374f66">${d.month}</text>`;
  });
  [['2024',CC.y2024],['2025',CC.y2025],['2026 (YTD)',CC.y2026]].forEach(([l,c],i) =>
    s += `<g transform="translate(${pad.l+i*92},${pad.t-16})"><rect width="8" height="8" y="-1" rx="2" fill="${c}"/><text x="11" y="7" font-size="9" fill="${B.sub}">${l}</text></g>`);
  return s + '</svg>';
}

function heatmapSVG(data, W=1100) {
  if (!data || data.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No heatmap data</div>';
  const DAYS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  const WEEKS = 26;
  const mx = Math.max(...data.map(d => d.units||0));
  const padL=36,padT=18,padR=8,padB=8,cellH=26;
  const cellW = Math.floor((W-padL-padR)/WEEKS);
  const svgW = padL+WEEKS*cellW+padR, svgH = padT+7*cellH+padB;
  let s = `<svg width="100%" viewBox="0 0 ${svgW} ${svgH}" style="overflow:visible;display:block">`;
  Array.from({length:WEEKS},(_,w) => { if(w%4===0||w===WEEKS-1) s+=`<text x="${padL+(w+.5)*cellW}" y="${padT-5}" text-anchor="middle" font-size="8" fill="${B.sub}">W${WEEKS-w}</text>`; });
  DAYS.forEach((d,i) => s+=`<text x="${padL-3}" y="${padT+(i+.5)*cellH+4}" text-anchor="end" font-size="9" fill="${B.sub}">${d}</text>`);
  data.forEach(({week,day,units}) => {
    const alpha = mx > 0 ? Math.max(0.07, Math.min(0.9, (units||0)/mx)).toFixed(2) : '0.07';
    const c = day >= 5 ? B.o2 : B.b2;
    s += `<rect x="${(padL+week*cellW+2).toFixed(1)}" y="${(padT+day*cellH+2).toFixed(1)}" width="${(cellW-4).toFixed(1)}" height="${(cellH-4).toFixed(1)}" rx="3" fill="${c}" fill-opacity="${alpha}"/>`;
    if ((units||0)/mx > 0.68) s += `<text x="${(padL+(week+.5)*cellW).toFixed(1)}" y="${(padT+(day+.5)*cellH+3.5).toFixed(1)}" text-anchor="middle" font-size="7" fill="white" opacity=".85">${units}</text>`;
  });
  return s + '</svg>';
}

function funnelSVG(data) {
  if (!data || data.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No funnel data</div>';
  const n = data.length;

  // Layout constants — funnel LEFT, comparison table RIGHT
  const W = 1010;
  const rowH = 56;
  const padT = 38;        // space above first row (header row)
  const padB = 44;        // legend below
  const derivedRowH = 28;
  const nDerived = 2;     // ATC%, Conv%
  const H = padT + n * rowH + 14 + nDerived * derivedRowH + padB;

  // Funnel geometry (left side)
  const funnelCX = 280;
  const maxFW = 480;

  // Table column x-positions (right side)
  const labelX = 560;
  const tyX    = 720;   // right-anchored
  const lyX    = 850;   // right-anchored
  const chgX   = 980;  // right-anchored

  // Pre-compute derived metrics
  const sessIdx = data.findIndex(d => d.label === 'Sessions');
  const atcIdx  = data.findIndex(d => d.label === 'Add to Cart');
  const ordIdx  = data.findIndex(d => d.label === 'Orders');
  const sess_ty = sessIdx >= 0 ? data[sessIdx].ty : 0;
  const sess_ly = sessIdx >= 0 ? data[sessIdx].ly : 0;
  const atc_ty  = atcIdx  >= 0 ? data[atcIdx].ty  : 0;
  const atc_ly  = atcIdx  >= 0 ? data[atcIdx].ly  : 0;
  const ord_ty  = ordIdx  >= 0 ? data[ordIdx].ty  : 0;
  const ord_ly  = ordIdx  >= 0 ? data[ordIdx].ly  : 0;
  const atcPct_ty = sess_ty > 0 ? (atc_ty / sess_ty * 100).toFixed(1) : null;
  const atcPct_ly = sess_ly > 0 ? (atc_ly / sess_ly * 100).toFixed(1) : null;
  const conv_ty   = sess_ty > 0 ? (ord_ty / sess_ty * 100).toFixed(2) : null;
  const conv_ly   = sess_ly > 0 ? (ord_ly / sess_ly * 100).toFixed(2) : null;

  // TY bar width: normalized to first stage TY — drives the funnel shape
  const maxVal = data[0].ty || 1;
  const minW = maxFW * 0.10;
  const tyW_fn  = val => Math.max(minW, (val / maxVal) * maxFW);
  // LY bar width: proportional to TY width *within that row* so visual ratio is always correct
  // e.g. TY=320, LY=240 → LY bar = 75% of TY bar regardless of absolute scale
  const lyW_fn  = (ty, ly) => ty > 0 ? (ly / ty) * tyW_fn(ty) : tyW_fn(ly);

  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  s += `<defs>${data.map((_,i) => `<linearGradient id="fg${i}" x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stop-color="${B.b1}" stop-opacity=".92"/><stop offset="100%" stop-color="${B.b3}" stop-opacity=".92"/></linearGradient>`).join('')}</defs>`;

  // ── Table header row ──────────────────────────────────────
  s += `<text x="${labelX}" y="${padT - 10}" text-anchor="start" font-size="9" font-weight="700" fill="${B.sub}" letter-spacing="1">METRIC</text>`;
  s += `<text x="${tyX}"    y="${padT - 10}" text-anchor="end"   font-size="9" font-weight="700" fill="${B.sub}" letter-spacing="1">TY</text>`;
  s += `<text x="${lyX}"    y="${padT - 10}" text-anchor="end"   font-size="9" font-weight="700" fill="${B.sub}" letter-spacing="1">LY</text>`;
  s += `<text x="${chgX}"   y="${padT - 10}" text-anchor="end"   font-size="9" font-weight="700" fill="${B.sub}" letter-spacing="1">% CHG</text>`;
  s += `<line x1="${labelX}" y1="${padT - 4}" x2="${W - 8}" y2="${padT - 4}" stroke="${B.brd}" stroke-width="0.5" opacity=".9"/>`;

  // ── Funnel + table rows ───────────────────────────────────
  data.forEach((f, i) => {
    const tyW  = tyW_fn(f.ty);
    const lyW  = lyW_fn(f.ty, f.ly);
    const nextF    = i < n - 1 ? data[i+1] : null;
    const nextTyW  = nextF ? tyW_fn(nextF.ty)            : tyW  * 0.55;
    const nextLyW  = nextF ? lyW_fn(nextF.ty, nextF.ly)  : lyW  * 0.55;
    const rowY  = padT + i * rowH;
    const segH  = rowH - 4;
    const rowMid = rowY + rowH / 2;

    // Funnel trapezoid
    s += `<path d="M${funnelCX - tyW/2},${rowY + 2} L${funnelCX + tyW/2},${rowY + 2} L${funnelCX + nextTyW/2},${rowY + segH - 1} L${funnelCX - nextTyW/2},${rowY + segH - 1} Z" fill="url(#fg${i})" opacity=".92"/>`;
    s += `<path d="M${funnelCX - lyW/2},${rowY} L${funnelCX + lyW/2},${rowY} L${funnelCX + nextLyW/2},${rowY + segH} L${funnelCX - nextLyW/2},${rowY + segH} Z" fill="none" stroke="${B.sub}" stroke-width="1.5" stroke-dasharray="5 3" opacity=".75"/>`;
    // TY value on funnel bar
    s += `<text x="${funnelCX}" y="${rowMid + 4}" text-anchor="middle" font-size="12" font-weight="700" fill="#fff">${fN(f.ty)}</text>`;

    // Table row
    const lyDelta = f.ly > 0 ? dp(f.ty, f.ly) : null;
    const deltaColor = lyDelta != null ? (lyDelta >= 0 ? '#4ade80' : '#fb923c') : B.sub;

    s += `<text x="${labelX}" y="${rowMid + 4}" text-anchor="start" font-size="11" font-weight="600" fill="#e2e8f0">${f.label}</text>`;
    s += `<text x="${tyX}"    y="${rowMid + 4}" text-anchor="end"   font-size="12" font-weight="700" fill="#e2e8f0">${fN(f.ty)}</text>`;
    s += `<text x="${lyX}"    y="${rowMid + 4}" text-anchor="end"   font-size="11"                   fill="${B.sub}">${fN(f.ly)}</text>`;
    if (lyDelta != null) {
      s += `<text x="${chgX}" y="${rowMid + 4}" text-anchor="end" font-size="11" font-weight="700" fill="${deltaColor}">${lyDelta >= 0 ? '\u25B2' : '\u25BC'} ${Math.abs(lyDelta).toFixed(1)}%</text>`;
    } else {
      s += `<text x="${chgX}" y="${rowMid + 4}" text-anchor="end" font-size="11" fill="${B.sub}">\u2014</text>`;
    }

    // Row divider (not after last)
    if (i < n - 1) {
      s += `<line x1="${labelX}" y1="${rowY + rowH}" x2="${W - 8}" y2="${rowY + rowH}" stroke="${B.brd}" stroke-width="0.4" opacity=".5"/>`;
    }
  });

  // ── Derived rows: ATC% and Conv% ──────────────────────────
  const divY = padT + n * rowH + 8;
  s += `<line x1="${labelX}" y1="${divY}" x2="${W - 8}" y2="${divY}" stroke="${B.brd}" stroke-width="0.6" opacity=".8"/>`;

  const derivedRows = [];
  if (atcPct_ty != null) {
    const atcChg = atcPct_ly != null ? dp(parseFloat(atcPct_ty), parseFloat(atcPct_ly)) : null;
    derivedRows.push({ label: 'ATC %', ty: atcPct_ty + '%', ly: atcPct_ly != null ? atcPct_ly + '%' : '\u2014', chg: atcChg });
  }
  if (conv_ty != null) {
    const convChg = conv_ly != null ? dp(parseFloat(conv_ty), parseFloat(conv_ly)) : null;
    derivedRows.push({ label: 'Conversion %', ty: conv_ty + '%', ly: conv_ly != null ? conv_ly + '%' : '\u2014', chg: convChg });
  }

  derivedRows.forEach((dm, i) => {
    const ry = divY + 20 + i * derivedRowH;
    const chgColor = dm.chg != null ? (dm.chg >= 0 ? '#4ade80' : '#fb923c') : B.sub;
    s += `<text x="${labelX}" y="${ry}" text-anchor="start" font-size="11" font-weight="600" fill="#e2e8f0">${dm.label}</text>`;
    s += `<text x="${tyX}"    y="${ry}" text-anchor="end"   font-size="12" font-weight="700" fill="#e2e8f0">${dm.ty}</text>`;
    s += `<text x="${lyX}"    y="${ry}" text-anchor="end"   font-size="11"               fill="${B.sub}">${dm.ly}</text>`;
    if (dm.chg != null) {
      s += `<text x="${chgX}" y="${ry}" text-anchor="end" font-size="11" font-weight="700" fill="${chgColor}">${dm.chg >= 0 ? '\u25B2' : '\u25BC'} ${Math.abs(dm.chg).toFixed(1)}%</text>`;
    } else {
      s += `<text x="${chgX}" y="${ry}" text-anchor="end" font-size="11" fill="${B.sub}">\u2014</text>`;
    }
  });

  // ── Legend (bottom-left, under funnel) ────────────────────
  const legY = H - padB + 18;
  s += `<g transform="translate(16,${legY})">`;
  s += `<rect width="10" height="10" y="-1" rx="2" fill="${B.b2}" opacity=".9"/><text x="14" y="8" font-size="9" fill="${B.sub}">This Year</text>`;
  s += `<rect x="90" width="10" height="8" y="0" rx="2" fill="none" stroke="${B.sub}" stroke-width="1" stroke-dasharray="4 2"/><text x="104" y="8" font-size="9" fill="${B.sub}">Last Year</text>`;
  s += `</g>`;

  return s + '</svg>';
}

function adQuadrantSVG(data) {
  if (!data || !data.ty) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No ad efficiency data</div>';
  const W=1100, H=340, pad={t:50,r:120,b:60,l:80};
  const iw=W-pad.l-pad.r, ih=H-pad.t-pad.b;
  const acosMin=0, acosMax=0.40, roasMin=0, roasMax=12;
  const {acos:tyAcos, roas:tyRoas} = data.ty;
  const {acos:lyAcos, roas:lyRoas} = data.ly;
  const px = v => pad.l+((v-acosMin)/(acosMax-acosMin))*iw;
  const py = v => pad.t+ih-((v-roasMin)/(roasMax-roasMin))*ih;
  const lyx=px(lyAcos), lyy=py(lyRoas), tyx=px(tyAcos), tyy=py(tyRoas);
  const midX=lyx, midY=lyy;
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  // Quadrant backgrounds
  s+=`<rect x="${pad.l}" y="${pad.t}" width="${midX-pad.l}" height="${midY-pad.t}" fill="${B.t1}" opacity=".12" rx="4"/>`;
  s+=`<rect x="${midX}" y="${pad.t}" width="${pad.l+iw-midX}" height="${midY-pad.t}" fill="${B.o2}" opacity=".08" rx="4"/>`;
  s+=`<rect x="${pad.l}" y="${midY}" width="${midX-pad.l}" height="${pad.t+ih-midY}" fill="${B.b1}" opacity=".08" rx="4"/>`;
  s+=`<rect x="${midX}" y="${midY}" width="${pad.l+iw-midX}" height="${pad.t+ih-midY}" fill="${B.o1}" opacity=".14" rx="4"/>`;
  // Quadrant labels
  s+=`<text x="${(pad.l+midX)/2}" y="${(pad.t+midY)/2-8}" text-anchor="middle" font-size="11" font-weight="600" fill="${B.t2}" opacity=".7">IDEAL</text>`;
  s+=`<text x="${(pad.l+midX)/2}" y="${(pad.t+midY)/2+7}" text-anchor="middle" font-size="9" fill="${B.t2}" opacity=".5">Low ACOS \u00B7 High ROAS</text>`;
  s+=`<text x="${(midX+pad.l+iw)/2}" y="${(pad.t+midY)/2-8}" text-anchor="middle" font-size="11" font-weight="600" fill="${B.o2}" opacity=".7">HIGH SPEND</text>`;
  s+=`<text x="${(midX+pad.l+iw)/2}" y="${(pad.t+midY)/2+7}" text-anchor="middle" font-size="9" fill="${B.o2}" opacity=".5">Optimize bids</text>`;
  s+=`<text x="${(pad.l+midX)/2}" y="${(midY+pad.t+ih)/2-2}" text-anchor="middle" font-size="11" font-weight="600" fill="${B.sub}" opacity=".5">UNDERINVESTING</text>`;
  s+=`<text x="${(midX+pad.l+iw)/2}" y="${(midY+pad.t+ih)/2-2}" text-anchor="middle" font-size="11" font-weight="600" fill="${B.o1}" opacity=".7">INEFFICIENT</text>`;
  // Grid
  for(let i=0;i<=4;i++){const xv=acosMin+(acosMax-acosMin)*(i/4),xp=px(xv);s+=`<line x1="${xp}" y1="${pad.t}" x2="${xp}" y2="${pad.t+ih}" stroke="#1a2f4a" stroke-width="0.5"/><text x="${xp}" y="${pad.t+ih+14}" text-anchor="middle" font-size="9" fill="#374f66">${(xv*100).toFixed(0)}%</text>`;const yv=roasMin+(roasMax-roasMin)*(i/4),yp=py(yv);s+=`<line x1="${pad.l}" y1="${yp}" x2="${pad.l+iw}" y2="${yp}" stroke="#1a2f4a" stroke-width="0.5"/><text x="${pad.l-8}" y="${yp+4}" text-anchor="end" font-size="9" fill="#374f66">${yv.toFixed(1)}x</text>`;}
  // LY crosshair
  s+=`<line x1="${lyx}" y1="${pad.t}" x2="${lyx}" y2="${pad.t+ih}" stroke="${B.sub}" stroke-width="1" stroke-dasharray="5 4" opacity=".5"/>`;
  s+=`<line x1="${pad.l}" y1="${lyy}" x2="${pad.l+iw}" y2="${lyy}" stroke="${B.sub}" stroke-width="1" stroke-dasharray="5 4" opacity=".5"/>`;
  s+=`<text x="${lyx+4}" y="${pad.t-6}" font-size="9" fill="${B.sub}">LY avg (ACOS ${(lyAcos*100).toFixed(0)}%)</text>`;
  // Trajectory arrow
  const angle=Math.atan2(tyy-lyy,tyx-lyx),dist=Math.sqrt((tyx-lyx)**2+(tyy-lyy)**2),shorten=18;
  const ex=lyx+Math.cos(angle)*(dist-shorten),ey=lyy+Math.sin(angle)*(dist-shorten);
  s+=`<line x1="${lyx}" y1="${lyy}" x2="${ex}" y2="${ey}" stroke="${B.t2}" stroke-width="1.5" stroke-dasharray="4 3" opacity=".7"/>`;
  const aw=8,ah=5,ax1=ex-aw*Math.cos(angle)+ah*Math.sin(angle),ay1=ey-aw*Math.sin(angle)-ah*Math.cos(angle),ax2=ex-aw*Math.cos(angle)-ah*Math.sin(angle),ay2=ey-aw*Math.sin(angle)+ah*Math.cos(angle);
  s+=`<path d="M${ex},${ey} L${ax1},${ay1} L${ax2},${ay2} Z" fill="${B.t2}" opacity=".7"/>`;
  // Dots
  s+=`<circle cx="${lyx}" cy="${lyy}" r="8" fill="${B.dim}" opacity=".8" stroke="#0c1a2e" stroke-width="2"/><text x="${lyx}" y="${lyy+4}" text-anchor="middle" font-size="8" font-weight="700" fill="#fff">LY</text>`;
  s+=`<circle cx="${tyx}" cy="${tyy}" r="16" fill="${B.b2}" opacity=".18"/><circle cx="${tyx}" cy="${tyy}" r="11" fill="${B.b2}" stroke="#0c1a2e" stroke-width="2"/><text x="${tyx}" y="${tyy+4}" text-anchor="middle" font-size="9" font-weight="700" fill="#fff">TY</text>`;
  // Callout
  s+=`<rect x="${tyx+18}" y="${tyy-30}" width="155" height="56" rx="6" fill="#122138" stroke="#1a2f4a" stroke-width="1"/>`;
  s+=`<text x="${tyx+26}" y="${tyy-14}" font-size="10" font-weight="700" fill="#f1f5f9">ACOS: ${(tyAcos*100).toFixed(1)}%  ROAS: ${tyRoas.toFixed(2)}x</text>`;
  const acosChg = lyAcos > 0 ? ((1-tyAcos/lyAcos)*100).toFixed(1) : '0.0';
  const roasChg = lyRoas > 0 ? ((tyRoas/lyRoas-1)*100).toFixed(1) : '0.0';
  s+=`<text x="${tyx+26}" y="${tyy+2}" font-size="9" fill="${B.t2}">\u25B2 ${acosChg}% ACOS improvement</text>`;
  s+=`<text x="${tyx+26}" y="${tyy+18}" font-size="9" fill="${B.t2}">\u25B2 ${roasChg}% ROAS improvement</text>`;
  // Axis labels
  s+=`<text x="${pad.l+iw/2}" y="${H-4}" text-anchor="middle" font-size="11" fill="${B.sub}">ACOS \u2192  (lower is better)</text>`;
  s+=`<text x="${pad.l-55}" y="${pad.t+ih/2}" text-anchor="middle" font-size="11" fill="${B.sub}" transform="rotate(-90,${pad.l-55},${pad.t+ih/2})">ROAS \u2192  (higher is better)</text>`;
  return s + '</svg>';
}

// ── SUB-COMPONENTS ─────────────────────────────────────────────────
function Spinner() {
  return (
    <div style={{display:'flex',justifyContent:'center',padding:'32px',color:'var(--txt3)',fontSize:12}}>
      Loading...
    </div>
  );
}

function ChartCard({ title, badge, children, noMargin, error }) {
  return (
    <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:16,marginBottom:noMargin?0:12,transition:'background .3s'}}>
      <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:14}}>
        <span style={{fontSize:13,fontWeight:700,color:'var(--txt)'}}>{title}</span>
        {badge && <span style={{fontSize:10,padding:'2px 9px',borderRadius:99,background:'rgba(46,111,187,.15)',color:B.b3,border:'1px solid rgba(46,111,187,.2)'}}>{badge}</span>}
      </div>
      {error
        ? <div style={{padding:'12px 14px',color:'#fb923c',fontSize:11,background:'rgba(251,146,60,.08)',border:'1px solid rgba(251,146,60,.18)',borderRadius:8}}>⚠ {error}</div>
        : children
      }
    </div>
  );
}

function Legend({ items }) {
  return (
    <div style={{display:'flex',gap:12,flexWrap:'wrap',marginTop:8}}>
      {items.map(([label, color, dashed]) => (
        <div key={label} style={{display:'flex',alignItems:'center',gap:5,fontSize:10,color:'var(--txt3)'}}>
          <div style={{width:8,height:8,borderRadius:2,background:color,opacity:dashed?.45:1,flexShrink:0}}/>
          <span>{label}</span>
        </div>
      ))}
    </div>
  );
}

function MetricCard({ label, value, ly, delta, expandContent, invert }) {
  const [expanded, setExpanded] = useState(false);
  const isPos = invert ? delta < 0 : delta > 0;
  const deltaEl = delta != null ? (
    <span style={{fontSize:9,fontWeight:700,padding:'2px 5px',borderRadius:6,
      color:isPos?'#4ade80':'#fb923c',
      background:isPos?'rgba(74,222,128,.1)':'rgba(251,146,60,.12)',whiteSpace:'nowrap',flexShrink:0}}>
      {delta > 0 ? '\u25B2' : '\u25BC'}&nbsp;{Math.abs(delta).toFixed(1)}%
    </span>
  ) : null;
  return (
    <div style={{flex:'1 1 0',minWidth:155,background:'linear-gradient(145deg,var(--card),var(--card2))',borderRadius:12,padding:'10px 12px 9px',border:'1px solid var(--brd)',transition:'background .3s'}}>
      {/* Label */}
      <div style={{fontSize:9,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.07em',marginBottom:5,whiteSpace:'nowrap'}}>{label}</div>
      {/* Single row: TY value | LY | delta badge */}
      <div style={{display:'flex',alignItems:'center',gap:7,flexWrap:'nowrap',overflow:'hidden'}}>
        <span style={{fontSize:16,fontWeight:800,letterSpacing:'-.02em',color:'var(--txt)',lineHeight:1,flexShrink:0}}>{value}</span>
        {ly && (
          <span style={{fontSize:10,color:'var(--txt3)',whiteSpace:'nowrap',letterSpacing:'-.01em',flexShrink:0,borderLeft:'1px solid var(--brd2)',paddingLeft:7}}>
            LY&nbsp;<span style={{color:'var(--txt2)'}}>{ly}</span>
          </span>
        )}
        {deltaEl}
      </div>
      {expandContent && (
        <div style={{marginTop:7}}>
          <button onClick={() => setExpanded(e => !e)}
            style={{fontSize:9,padding:'2px 7px',borderRadius:99,border:'1px solid var(--brd)',background:'transparent',color:'var(--txt3)',cursor:'pointer',fontWeight:600,whiteSpace:'nowrap'}}>
            {expanded ? '\u25B2 hide' : '\u25BC detail'}
          </button>
        </div>
      )}
      {expanded && expandContent && (
        <div style={{marginTop:10,paddingTop:10,borderTop:'1px solid var(--brd)'}}>
          {expandContent}
        </div>
      )}
    </div>
  );
}

function SectionDivider({ label }) {
  return (
    <div style={{display:'flex',alignItems:'center',gap:10,margin:'24px 0 14px'}}>
      <div style={{width:12,height:1,background:'var(--brd)'}}/>
      <span style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.14em',color:'var(--txt3)',whiteSpace:'nowrap'}}>{label}</span>
      <div style={{flex:1,height:1,background:'var(--brd)'}}/>
    </div>
  );
}

function PeriodBar({ value, onChange }) {
  return (
    <div style={{display:'flex',gap:2,padding:4,borderRadius:10,background:'var(--surf)',border:'1px solid var(--brd2)'}}>
      {CHART_PERIODS.map(p => (
        <button key={p} onClick={() => onChange(p)}
          style={{padding:'4px 13px',borderRadius:7,fontSize:12,fontWeight:600,border:'none',cursor:'pointer',
            background:value===p?B.b1:'transparent',color:value===p?'#fff':'var(--txt3)',transition:'all .15s'}}>
          {p}
        </button>
      ))}
    </div>
  );
}

// ── MAIN COMPONENT ─────────────────────────────────────────────────
export default function Sales({ filters = {} }) {
  // Division/customer come from the global filter bar (App.jsx HierarchyFilter)
  const divRaw  = filters.division  || '';   // 'golf' | 'housewares' | ''
  const custRaw = filters.customer  || '';   // 'amazon' | '' etc.

  const [viewTab,     setViewTab]     = useState('Exec');
  const [activePeriod,setActivePeriod]= useState('MTD');
  const [cpSales,     setCpSales]     = useState('30D');
  const [cpTraffic,   setCpTraffic]   = useState('30D');
  const [customStart, setCustomStart] = useState('');
  const [customEnd,   setCustomEnd]   = useState('');

  // Data state
  const [metrics,     setMetrics]     = useState(null);
  const [periodCols,  setPeriodCols]  = useState(null);
  const [trend,       setTrend]       = useState(null);
  const [yoy,         setYoy]         = useState(null);
  const [channel,     setChannel]     = useState(null);
  const [rolling,     setRolling]     = useState(null);
  const [heatmap,     setHeatmap]     = useState(null);
  const [trendTraffic, setTrendTraffic] = useState(null);
  const [funnel,      setFunnel]      = useState(null);
  const [adEff,       setAdEff]       = useState(null);
  const [feeBreak,    setFeeBreak]    = useState(null);
  const [adBreak,     setAdBreak]     = useState(null);
  const [loading,     setLoading]     = useState({});
  const [errors,      setErrors]      = useState({});

  const periodApiKey = PERIOD_API_MAP[activePeriod] || 'last_30d';
  const chartSalesApi = CHART_PERIOD_API[cpSales];
  const chartTrafficApi = CHART_PERIOD_API[cpTraffic];

  const baseParams = {
    division: divRaw  || null,
    customer: custRaw || null,
  };

  const setLoad = (key, val) => setLoading(l => ({...l, [key]: val}));
  const setErr  = (key, val) => setErrors(e  => ({...e, [key]: val}));

  async function load(key, setter, path, params) {
    setLoad(key, true); setErr(key, null);
    try {
      const data = await apiFetch(path, params);
      setter(data);
    } catch (e) {
      setErr(key, e.message);
    } finally {
      setLoad(key, false);
    }
  }

  // Fetch metrics when period/division/customer changes
  useEffect(() => {
    const params = {...baseParams, period: periodApiKey};
    if (viewTab === 'Custom' && customStart && customEnd) {
      params.period = 'custom'; params.start = customStart; params.end = customEnd;
    }
    load('metrics', setMetrics, 'summary', params);
    load('periodCols', setPeriodCols, 'period-comparison', {...baseParams, view: viewTab.toLowerCase()});
    load('feeBreak', setFeeBreak, 'fee-breakdown', params);
    load('adBreak',  setAdBreak,  'ad-breakdown',  params);
    load('funnel',   setFunnel,   'funnel',         params);
    load('adEff',    setAdEff,    'ad-efficiency',  params);
  }, [divRaw, custRaw, activePeriod, viewTab, customStart, customEnd]);

  // Fetch sales chart data when cpSales changes
  useEffect(() => {
    const params = {...baseParams, period: chartSalesApi};
    load('trend',   setTrend,   'trend',     params);
    load('rolling', setRolling, 'rolling',   params);
    load('channel', setChannel, 'by-channel',params);
    load('heatmap', setHeatmap, 'heatmap',   baseParams);
    load('yoy',     setYoy,     'monthly-yoy',baseParams);
  }, [divRaw, custRaw, cpSales]);

  // Fetch traffic chart data when cpTraffic changes
  useEffect(() => {
    const params = {...baseParams, period: chartTrafficApi};
    load('trendTraffic', setTrendTraffic, 'trend', params);
  }, [divRaw, custRaw, cpTraffic]);

  const handleViewTab = v => { setViewTab(v); setActivePeriod(PERIODS[v]?.[0] || ''); };

  const m = metrics || {};
  const fellBack = m.fell_back;
  const ly = k => m[`ly_${k}`];
  const svgChart = html => <div dangerouslySetInnerHTML={{__html: html}}/>;
  const periods = viewTab === 'Custom' ? [] : PERIODS[viewTab] || [];

  return (
    <div style={{fontFamily:"'Sora',-apple-system,BlinkMacSystemFont,sans-serif",color:'var(--txt)'}}>

      {/* HEADER */}
      <div style={{marginBottom:20}}>
        <h2 style={{fontFamily:"'DM Serif Display',Georgia,serif",fontSize:22,fontWeight:400,margin:0,color:'var(--txt)'}}>Performance Snapshot</h2>
        <div style={{fontSize:12,color:'var(--txt3)',marginTop:3,fontFamily:"'Space Grotesk',monospace"}}>
          {!divRaw ? 'All Divisions' : divRaw === 'golf' ? 'Golf (PGAT)' : 'Housewares'}
          {custRaw ? ` \u00B7 ${custRaw.replace(/_/g,' ').replace(/\b\w/g,l=>l.toUpperCase())}` : ''}
          {fellBack && <span style={{marginLeft:8,color:B.o3,fontStyle:'italic'}}>{m.period_label}</span>}
        </div>
      </div>

      {/* VIEW TABS */}
      <div className="ptab-bar" style={{marginBottom:18}}>
        {VIEW_TABS.map(t => (
          <button key={t} className={`ptab${viewTab===t?' active':''}`} onClick={() => handleViewTab(t)}>
            {t}
          </button>
        ))}
      </div>

      {/* CUSTOM DATE PICKER */}
      {viewTab === 'Custom' && (
        <div style={{background:'var(--card)',border:'1px solid var(--brd2)',borderRadius:14,padding:16,marginBottom:18,transition:'background .3s'}}>
          <div style={{fontSize:13,fontWeight:700,marginBottom:12,color:'var(--txt)'}}>Custom Date Range</div>
          <div style={{display:'flex',gap:10,alignItems:'flex-end',flexWrap:'wrap'}}>
            {[['Start',customStart,setCustomStart],['End',customEnd,setCustomEnd]].map(([l,v,s]) => (
              <div key={l}>
                <label style={{display:'block',fontSize:10,color:'var(--txt3)',marginBottom:4}}>{l} Date</label>
                <input type="date" value={v} onChange={e => s(e.target.value)}
                  style={{padding:'6px 12px',borderRadius:8,border:'1px solid var(--brd2)',background:'var(--surf)',color:'var(--txt)',fontSize:12,outline:'none',colorScheme:'dark'}}/>
              </div>
            ))}
            {[7,30,90].map(d => (
              <button key={d} onClick={() => { const e=new Date(),s=new Date();s.setDate(s.getDate()-d);setCustomStart(s.toISOString().split('T')[0]);setCustomEnd(e.toISOString().split('T')[0]); }}
                style={{padding:'5px 11px',borderRadius:8,border:'1px solid var(--brd)',background:'transparent',color:'var(--txt3)',fontSize:11,cursor:'pointer'}}>
                Last {d}d
              </button>
            ))}
          </div>
        </div>
      )}

      {/* PERIOD COMPARISON COLUMNS */}
      {viewTab !== 'Custom' && periodCols && (
        <div style={{display:'flex',gap:8,overflowX:'auto',paddingBottom:6,marginBottom:24}}>
          {periods.slice(0,5).map(p => {
            const d = periodCols[p] || {};
            const pct = (ty, lyv) => (!lyv || !ty) ? null : ((ty - lyv) / lyv * 100);
            const pctEl = (delta, inv, key) => {
              if (delta == null) return <span key={key} style={{fontSize:9,color:'var(--txt3)',textAlign:'right'}}>—</span>;
              const pos = inv ? delta < 0 : delta > 0;
              return (
                <span key={key} style={{fontSize:9,fontWeight:700,color:pos?'#4ade80':'#fb923c',whiteSpace:'nowrap',textAlign:'right'}}>
                  {delta>0?'▲':'▼'} {Math.abs(delta).toFixed(1)}%
                </span>
              );
            };
            const rows = [
              ['Sales $',      f$(d.sales),         f$(d.ly_sales),         pct(d.sales, d.ly_sales),         false],
              ['Units',        fN(d.units),          fN(d.ly_units),         pct(d.units, d.ly_units),          false],
              ['AUR',          f$(d.aur),            f$(d.ly_aur),           pct(d.aur, d.ly_aur),              false],
              ['Orders',       fN(d.orders),         fN(d.ly_orders),        pct(d.orders, d.ly_orders),        false],
              ['AOV',          f$(d.aov),            f$(d.ly_aov),           pct(d.aov, d.ly_aov),              false],
              ['Sessions',     fN(d.sessions),       fN(d.ly_sessions),      pct(d.sessions, d.ly_sessions),    false],
              ['Glance Views', fN(d.glance_views),   fN(d.ly_glance_views),  pct(d.glance_views, d.ly_glance_views), false],
              ['Conv %',       fP(d.conversion),     fP(d.ly_conversion),    pct(d.conversion, d.ly_conversion), false],
            ];
            return (
              <div key={p} style={{flex:'1 1 185px',minWidth:185,background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:12,padding:'12px 14px',transition:'background .3s'}}>
                <div style={{fontSize:10,fontWeight:700,textTransform:'uppercase',letterSpacing:'.12em',color:B.b2,paddingBottom:9,borderBottom:'1px solid var(--brd)',marginBottom:9}}>{p}</div>
                {/* Single flat grid — all header + data cells share the same column tracks */}
                <div style={{display:'grid',gridTemplateColumns:'auto 1fr 1fr auto',columnGap:7,rowGap:6,alignItems:'center'}}>
                  {/* header row */}
                  <span/>
                  <span style={{fontSize:9,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.06em'}}>TY</span>
                  <span style={{fontSize:9,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.06em'}}>LY</span>
                  <span style={{fontSize:9,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.06em',textAlign:'right'}}>Chg</span>
                  {/* data rows — flat children so all share same column sizing */}
                  {rows.flatMap(([l, ty, lyv, delta, inv]) => [
                    <span key={l+'-l'} style={{fontSize:10,color:'var(--txt3)',whiteSpace:'nowrap',paddingRight:3}}>{l}</span>,
                    <span key={l+'-ty'} style={{fontSize:12,fontWeight:700,color:'var(--txt)'}}>{ty}</span>,
                    <span key={l+'-ly'} style={{fontSize:10,color:'var(--txt2)'}}>{lyv}</span>,
                    pctEl(delta, inv, l+'-chg'),
                  ])}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* PERIOD PILLS — directly above Sales Overview KPIs */}
      {viewTab !== 'Custom' && (
        <div className="ppill-bar" style={{marginBottom:14}}>
          {periods.map(p => (
            <button key={p} className={`ppill${activePeriod===p?' active':''}`} onClick={() => setActivePeriod(p)}>
              {p}
            </button>
          ))}
        </div>
      )}

      {/* ══ SALES OVERVIEW ══════════════════════════════════════════ */}
      <SectionDivider label="Sales Overview"/>
      <div style={{display:'flex',gap:8,marginBottom:14,overflowX:'auto',paddingBottom:2}}>
        {loading.metrics ? <Spinner/> : <>
          <MetricCard label="Sales $"       value={f$(m.sales)}         ly={f$(ly('sales'))}         delta={dp(m.sales,ly('sales'))}/>
          <MetricCard label="Unit Sales"    value={fN(m.unit_sales)}     ly={fN(ly('unit_sales'))}    delta={dp(m.unit_sales,ly('unit_sales'))}/>
          <MetricCard label="AUR"           value={f$(m.aur)}            ly={f$(ly('aur'))}           delta={dp(m.aur,ly('aur'))}/>
          <MetricCard label="COGS"          value={f$(m.cogs)}           ly={f$(ly('cogs'))}          delta={dp(m.cogs,ly('cogs'))}/>
          <MetricCard label="Amazon Fees"   value={f$(m.amazon_fees)}    ly={f$(ly('amazon_fees'))}   delta={dp(m.amazon_fees,ly('amazon_fees'))}
            expandContent={feeBreak && Array.isArray(feeBreak) && <div>{feeBreak.map(f=><div key={f.type} style={{display:'flex',justifyContent:'space-between',fontSize:10,marginBottom:5}}><span style={{color:'var(--txt3)'}}>{f.type}</span><span style={{fontWeight:600,color:'var(--txt)'}}>{f$(f.amount)}</span></div>)}</div>}/>
          <MetricCard label="Gross Margin $"  value={f$(m.gross_margin)}     ly={f$(ly('gross_margin'))}     delta={dp(m.gross_margin,ly('gross_margin'))}/>
          <MetricCard label="Gross Margin %"  value={fP(m.gross_margin_pct)} ly={fP(ly('gross_margin_pct'))} delta={dp(m.gross_margin_pct,ly('gross_margin_pct'))}/>
        </>}
      </div>

      {/* Sales Charts period bar */}
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',flexWrap:'wrap',gap:8,margin:'6px 0 10px'}}>
        <div style={{fontSize:10,color:'var(--txt3)',fontWeight:600,textTransform:'uppercase',letterSpacing:'.08em'}}>Charts</div>
        <PeriodBar value={cpSales} onChange={setCpSales}/>
      </div>

      {/* Monthly YOY */}
      <ChartCard title="Monthly Revenue — 2024 / 2025 / 2026" badge="Year over Year" error={errors.yoy}>
        {loading.yoy ? <Spinner/> : svgChart(yoyBarSVG(toArr(yoy)))}
      </ChartCard>

      {/* Revenue by Channel (All Divisions only) */}
      {!divRaw && (
        <ChartCard title="Revenue by Channel" badge={cpSales} error={errors.channel}>
          {loading.channel ? <Spinner/> : <>
            {svgChart(stackedBarSVG(toArr(channel)))}
            <Legend items={[['Amazon',B.b2],['Walmart Mkt',B.o2],['Walmart Stores',B.o3],['Shopify',B.t2],['Stores',B.t3]]}/>
          </>}
        </ChartCard>
      )}

      {/* Sales $ Trend */}
      <ChartCard title="Sales $ Trend" badge={cpSales} error={errors.trend}>
        {loading.trend ? <Spinner/> : <>
          {svgChart(dualLineSVG(toArr(trend),'ty_sales','ly_sales',B.b2,B.sub,f$))}
          <Legend items={[['This Year',B.b2],['Last Year',B.sub,true]]}/>
        </>}
      </ChartCard>

      {/* AUR Trend */}
      <ChartCard title="AUR Trend" badge={cpSales} error={errors.trend}>
        {loading.trend ? <Spinner/> : <>
          {svgChart(dualLineSVG(toArr(trend),'ty_aur','ly_aur',B.t2,B.sub,f$))}
          <Legend items={[['AUR TY',B.t2],['AUR LY',B.sub,true]]}/>
        </>}
      </ChartCard>

      {/* Rolling 4-Week */}
      <ChartCard title="Rolling 4-Week Revenue vs LY" badge={cpSales} error={errors.rolling}>
        {loading.rolling ? <Spinner/> : <>
          {svgChart(dualLineSVG(toArr(rolling),'ty_rolling','ly_rolling',B.b3,B.sub,f$))}
          <Legend items={[['This Year',B.b3],['Last Year',B.sub,true]]}/>
        </>}
      </ChartCard>

      {/* Units Heatmap */}
      <ChartCard title="Units Sold — 26 Weeks × Day of Week" noMargin error={errors.heatmap}>
        {loading.heatmap ? <Spinner/> : svgChart(heatmapSVG(toArr(heatmap)))}
      </ChartCard>

      {/* ══ TRAFFIC & CONVERSION ════════════════════════════════════ */}
      <SectionDivider label="Traffic & Conversion"/>
      <div style={{display:'flex',gap:8,marginBottom:14,overflowX:'auto',paddingBottom:2}}>
        {loading.metrics ? <Spinner/> : <>
          <MetricCard label="Sessions"     value={fN(m.sessions)}    ly={fN(ly('sessions'))}    delta={dp(m.sessions,ly('sessions'))}/>
          <MetricCard label="Glance Views" value={fN(m.glance_views)} ly={fN(ly('glance_views'))} delta={dp(m.glance_views,ly('glance_views'))}/>
          <MetricCard label="Click Through" value={fP(m.ctr)}         ly={fP(ly('ctr'))}         delta={dp(m.ctr,ly('ctr'))}/>
          <MetricCard label="Conversion"   value={fP(m.conversion)}  ly={fP(ly('conversion'))}  delta={dp(m.conversion,ly('conversion'))}/>
        </>}
      </div>
      <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',flexWrap:'wrap',gap:8,margin:'6px 0 10px'}}>
        <div style={{fontSize:10,color:'var(--txt3)',fontWeight:600,textTransform:'uppercase',letterSpacing:'.08em'}}>Charts</div>
        <PeriodBar value={cpTraffic} onChange={setCpTraffic}/>
      </div>
      <ChartCard title="Traffic & Conversion" badge={cpTraffic} error={errors.trendTraffic}>
        {loading.trendTraffic ? <Spinner/> : <>
          {svgChart(dualLineSVG(toArr(trendTraffic),'ty_sessions','ly_sessions',B.o2,B.sub,fN))}
          <Legend items={[['Sessions TY',B.o2],['Sessions LY',B.sub,true]]}/>
        </>}
      </ChartCard>
      <ChartCard title="Conversion Funnel vs LY" noMargin error={errors.funnel}>
        {loading.funnel ? <Spinner/> : svgChart(funnelSVG(toArr(funnel)))}
      </ChartCard>

      {/* ══ ADVERTISING ═════════════════════════════════════════════ */}
      <SectionDivider label="Advertising"/>
      <div style={{display:'flex',gap:8,marginBottom:14,overflowX:'auto',paddingBottom:2}}>
        {loading.metrics ? <Spinner/> : <>
          <MetricCard label="Ad Spend $" value={f$(m.ad_spend)} ly={f$(ly('ad_spend'))} delta={dp(m.ad_spend,ly('ad_spend'))} invert
            expandContent={adBreak && Array.isArray(adBreak) && <div>{adBreak.map(a=><div key={a.type} style={{marginBottom:6}}><div style={{display:'flex',justifyContent:'space-between',fontSize:10}}><span style={{color:'var(--txt3)'}}>{a.type}</span><span style={{fontWeight:600,color:'var(--txt)'}}>{f$(a.spend)}</span></div><div style={{display:'flex',justifyContent:'space-between',fontSize:10,marginTop:2}}><span style={{color:'var(--txt3)',paddingLeft:8}}>ROAS</span><span style={{fontWeight:600,color:'#4ade80'}}>{fX(a.roas)}</span></div></div>)}</div>}/>
          <MetricCard label="ROAS"  value={fX(m.roas)}  ly={fX(ly('roas'))}  delta={dp(m.roas,ly('roas'))}/>
          <MetricCard label="TACOS" value={fP(m.tacos)} ly={fP(ly('tacos'))} delta={dp(m.tacos,ly('tacos'))} invert/>
        </>}
      </div>
      <ChartCard title="Ad Efficiency — ACOS vs ROAS Quadrant" badge="TY vs LY" noMargin error={errors.adEff}>
        {loading.adEff ? <Spinner/> : svgChart(adQuadrantSVG(adEff))}
      </ChartCard>

      {/* ══ INVENTORY HEALTH ════════════════════════════════════════ */}
      <SectionDivider label="Inventory Health"/>
      <ChartCard title="Days of Supply Trend" noMargin>
        <div style={{display:'flex',gap:24,marginBottom:10}}>
          {[['Current DOS',`${m.dos || 0}d`,B.t2],['Units on Hand',fN(m.stock_units),B.b3],['LY DOS','42d',B.sub]].map(([l,v,c])=>(
            <div key={l}><div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.08em',color:B.sub}}>{l}</div><div style={{fontSize:18,fontWeight:700,color:c,marginTop:2}}>{v}</div></div>
          ))}
        </div>
        <div style={{height:6,borderRadius:3,background:'var(--card)',overflow:'hidden',marginBottom:14}}>
          <div style={{height:'100%',width:`${Math.min(100,((m.dos||0)/90)*100)}%`,background:`linear-gradient(90deg,${B.o1},${B.o2},${B.t2})`,borderRadius:3}}/>
        </div>
        <Legend items={[['DOS This Year',B.t2],['DOS Last Year',B.sub,true]]}/>
        <div style={{display:'flex',alignItems:'center',gap:6,marginTop:8}}>
          <div style={{width:22,height:1.5,background:B.o1,borderRadius:1}}/>
          <span style={{fontSize:9,color:B.o2,fontWeight:700}}>30-day reorder threshold</span>
        </div>
      </ChartCard>

    </div>
  );
}
