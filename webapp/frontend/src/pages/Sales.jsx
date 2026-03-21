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
  'Sales Summary': ['Today','Yesterday','WTD','MTD','YTD'],
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
  // Chart-period keys (used by Executive tab period buttons → also drive 8 KPI boxes)
  '7D':'last_7d','30D':'last_30d','60D':'last_60d','90D':'last_90d',
  '120D':'last_120d','180D':'last_180d','1Y':'last_1y',
};
const GOLF_CUSTOMERS    = ['All Channels','Amazon','Walmart','Shopify','First Tee'];
const HW_CUSTOMERS      = ['All Channels','Belk',"Albertson's",'Family Dollar','Hobby Lobby'];
const CHART_PERIODS     = ['7D','30D','60D','90D','120D','180D','1Y'];
const CHART_PERIOD_API  = {'7D':'last_7d','30D':'last_30d','60D':'last_60d','90D':'last_90d','120D':'last_120d','180D':'last_180d','1Y':'last_1y'};
const EXEC_PERIODS_LIST = ['L7','L30','L60','L90','L180','L365'];
const EXEC_PERIOD_MAP   = {'L7':'7D','L30':'30D','L60':'60D','L90':'90D','L180':'180D','L365':'1Y'};
const HM_WEEKS_MAP      = {'13W':13,'26W':26,'52W':52};
// 4 × 13-week buckets within 52W data
// Backend: week=0 = THIS week, week=N = N weeks ago
// LY proxy: week≈52−N means "what happened N weeks from now, last year"
const HM_WINDOWS_DEF = {
  B: { label: 'Past 13w',    range: 'past',   indices: Array.from({length:13},(_,i)=>i) },      // 0-12:  most recent 13w
  A: { label: 'Past 14-26w', range: 'past',   indices: Array.from({length:13},(_,i)=>13+i) },   // 13-25: 13-25 weeks ago
  D: { label: '+14-26w LY',  range: 'future', indices: Array.from({length:13},(_,i)=>26+i) },   // 26-38: LY proxy 14-26w ahead
  C: { label: '+1-13w LY',   range: 'future', indices: Array.from({length:13},(_,i)=>39+i) },   // 39-51: LY proxy 1-13w ahead
};
const VIEW_TABS         = ['Executive','Sales Summary','Daily','Weekly','Monthly','Yearly','Custom'];
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

// ── Dual-axis chart: Revenue $ (or Conv %) on left Y-axis, AUR on right Y-axis ──
function salesAurSVG(data, W=1100, H=165, leftMetric='revenue') {
  if (!data || data.length < 2) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No data for this period</div>';
  const pad = {t:22, r:68, b:24, l:58};
  const iw = W-pad.l-pad.r, ih = H-pad.t-pad.b;
  const n = data.length;
  const x = i => pad.l + (i/(n-1||1))*iw;
  const leftIsConv = leftMetric === 'conversion';
  // Left axis — Revenue $ or Conversion %
  const lv = leftIsConv
    ? data.flatMap(d => [d.conversion, d.ly_conversion]).filter(v => v != null && v > 0)
    : data.flatMap(d => [d.ty_sales, d.ly_sales]).filter(v => v != null && v >= 0);
  const lmx = lv.length ? (leftIsConv ? Math.max(...lv)*1.2 : Math.max(...lv)) : 1;
  const yL  = v => pad.t + ih - Math.min(1, Math.max(0, (v||0)/(lmx||1))) * ih;
  const leftFmt     = leftIsConv ? fP : f$;
  const leftColor   = leftIsConv ? '#f97316' : '#2E6FBB';
  const leftColorLY = leftIsConv ? '#fb923c' : '#5B9FD4';
  const leftLabel   = leftIsConv ? '\u2190 Conv %' : '\u2190 Rev $';
  const leftTickFill= leftIsConv ? '#fb923c88' : '#5b7fa0';
  // AUR axis (right) — natural range with padding
  const av = data.flatMap(d => [d.ty_aur, d.ly_aur]).filter(v => v != null && v > 0);
  const amn = av.length ? Math.max(0, Math.min(...av)*0.88) : 0;
  const amx = av.length ? Math.max(...av)*1.12 : 1;
  const yA = v => pad.t + ih - Math.min(1, Math.max(0, (v-amn)/(amx-amn||1))) * ih;
  const uid = `sa${Math.random().toString(36).slice(2,6)}`;
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  s += `<defs><linearGradient id="${uid}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${leftColor}" stop-opacity=".18"/><stop offset="100%" stop-color="${leftColor}" stop-opacity="0"/></linearGradient></defs>`;
  // Axis labels — clear of the value ticks
  s += `<text x="${pad.l-5}" y="${pad.t-7}" text-anchor="end" font-size="8" fill="${leftIsConv?'#f9731699':'#5b7fa099'}" font-weight="600">${leftLabel}</text>`;
  s += `<text x="${W-pad.r+5}" y="${pad.t-7}" text-anchor="start" font-size="8" fill="#22c55e99" font-weight="600">AUR \u2192</text>`;
  // Grid lines + left axis tick labels
  for (let i=0; i<=4; i++) {
    const lval = lmx/4*i, ya = yL(lval);
    s += `<line x1="${pad.l}" y1="${ya.toFixed(1)}" x2="${W-pad.r}" y2="${ya.toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/>`;
    s += `<text x="${pad.l-5}" y="${(ya+4).toFixed(1)}" text-anchor="end" font-size="9" fill="${leftTickFill}">${leftFmt(lval)}</text>`;
  }
  // Right (AUR) tick labels
  for (let i=0; i<=4; i++) {
    const av2 = amn+(amx-amn)/4*i, ya = yA(av2);
    s += `<text x="${W-pad.r+5}" y="${(ya+4).toFixed(1)}" text-anchor="start" font-size="9" fill="#22c55e88">${f$(av2)}</text>`;
  }
  // X axis labels
  const step = Math.max(1, Math.floor(n/7));
  data.forEach((d,i) => {
    if (i%step===0 || i===n-1) {
      const lbl = d.date ? d.date.slice(5) : d.d || '';
      s += `<text x="${x(i).toFixed(1)}" y="${H-4}" text-anchor="middle" font-size="8" fill="#374f66">${lbl}</text>`;
    }
  });
  // Left metric — area fill (revenue only) + TY/LY lines
  const tyKey = leftIsConv ? 'conversion' : 'ty_sales';
  const lyKey = leftIsConv ? 'ly_conversion' : 'ly_sales';
  if (!leftIsConv) {
    const area = `M${x(0)},${yL(data[0].ty_sales||0)} ${data.map((d,i)=>`L${x(i)},${yL(d.ty_sales||0)}`).join(' ')} L${x(n-1)},${pad.t+ih} L${x(0)},${pad.t+ih} Z`;
    s += `<path d="${area}" fill="url(#${uid})"/>`;
  }
  const ptsTY = data.map((d,i)=>d[tyKey]!=null?`${x(i).toFixed(1)},${yL(d[tyKey]||0).toFixed(1)}`:null).filter(Boolean).join(' ');
  if (ptsTY) s += `<polyline points="${ptsTY}" fill="none" stroke="${leftColor}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>`;
  const ptsLY = data.map((d,i)=>d[lyKey]!=null?`${x(i).toFixed(1)},${yL(d[lyKey]||0).toFixed(1)}`:null).filter(Boolean).join(' ');
  if (ptsLY) s += `<polyline points="${ptsLY}" fill="none" stroke="${leftColorLY}" stroke-width="1.5" stroke-dasharray="4 3" stroke-linecap="round" stroke-linejoin="round"/>`;
  // AUR TY (solid green, right axis)
  const ptsAty = data.map((d,i)=>d.ty_aur!=null&&d.ty_aur>0?`${x(i).toFixed(1)},${yA(d.ty_aur).toFixed(1)}`:null).filter(Boolean).join(' ');
  if (ptsAty) s += `<polyline points="${ptsAty}" fill="none" stroke="#22c55e" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>`;
  // AUR LY (dashed darker green, right axis)
  const ptsAly = data.map((d,i)=>d.ly_aur!=null&&d.ly_aur>0?`${x(i).toFixed(1)},${yA(d.ly_aur).toFixed(1)}`:null).filter(Boolean).join(' ');
  if (ptsAly) s += `<polyline points="${ptsAly}" fill="none" stroke="#16a34a" stroke-width="1.5" stroke-dasharray="4 3" stroke-linecap="round" stroke-linejoin="round"/>`
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

function yoyBarSVG(data, forecast={}, yearVis={y2024:true,y2025:true,y2026:true}, W=1100, H=195, currentMonth=null) {
  if (!data || data.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No YOY data</div>';
  const CC = {y2024:B.dim, y2025:B.b2, y2026:B.o2, forecast:'#f59e0b'};
  // Build active bar definitions (only visible years + forecast when y2026 visible)
  const barDefs = [
    yearVis.y2024 && {key:'y2024',   color:CC.y2024,  lc:'#8899aa', lbl:"'24 Act"},
    yearVis.y2025 && {key:'y2025',   color:CC.y2025,  lc:B.b3,      lbl:"'25 Act"},
    yearVis.y2026 && {key:'y2026',   color:CC.y2026,  lc:B.o3,      lbl:"'26 Act"},
    yearVis.y2026 && {key:'forecast',color:CC.forecast,lc:'#fbbf24', lbl:"'26 Proj"},
  ].filter(Boolean);
  const nBars = Math.max(barDefs.length, 1);
  const hasForecast = Object.keys(forecast).length > 0;

  const pad = {t:30,r:16,b:26,l:58};
  // Compute max value across all visible bars including forecast
  const allVals = data.flatMap(d => barDefs.map(b =>
    b.key === 'forecast' ? (forecast[d.month_num]||0) : (d[b.key]||0)
  ));
  const maxV = Math.max(...allVals, 1);
  const iw = W-pad.l-pad.r, ih = H-pad.t-pad.b;
  const slotW = iw / data.length;
  const groupW  = slotW * 0.72;
  const innerGap = Math.max(1.5, groupW * 0.04);
  const bw = Math.max((groupW - innerGap * (nBars-1)) / nBars, 3);
  const groupX = mi => pad.l + mi*slotW + (slotW-groupW)/2;
  const barX   = (mi,bi) => groupX(mi) + bi*(bw+innerGap);
  const barH   = v => Math.max(((v||0)/maxV)*ih, (v||0) > 0 ? 3 : 0);
  const fmtTip = v => v>=1e6?`$${(v/1e6).toFixed(2)}M`:v>=1000?`$${(v/1000).toFixed(1)}k`:`$${Math.round(v)}`;
  const fmtLbl = v => v>=1e6?`$${(v/1e6).toFixed(1)}M`:v>=1000?`$${Math.round(v/1000)}k`:`$${Math.round(v)}`;

  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;

  // Grid lines
  for (let i=0;i<=3;i++) {
    const v=maxV*(i/3);
    s+=`<line x1="${pad.l}" y1="${(pad.t+ih*(1-i/3)).toFixed(1)}" x2="${W-pad.r}" y2="${(pad.t+ih*(1-i/3)).toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/>`;
    s+=`<text x="${pad.l-5}" y="${(pad.t+ih*(1-i/3)+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#374f66">${fmtLbl(v)}</text>`;
  }

  // Bars + dotted placeholders
  // Tooltip pattern: <g><title>text</title><rect.../></g> — most reliable SVG hover across browsers
  data.forEach((d,mi) => {
    const mn = d.month_num;
    const isCurrent = currentMonth!=null && mn===currentMonth;
    barDefs.forEach((bar,bi) => {
      const v = bar.key==='forecast' ? (forecast[mn]||null) : d[bar.key];
      const isFcst = bar.key==='forecast';

      // Dotted placeholder: y2026 slot is empty but forecast exists → dashed outline rect
      if (bar.key==='y2026' && (v==null||v<=0) && forecast[mn]>0) {
        const fh = barH(forecast[mn]);
        const bx = barX(mi, bi);
        const tip = `${d.month} '26 Act: (not yet actualized)`;
        s+=`<g data-tip="${tip}"><rect x="${bx.toFixed(1)}" y="${(pad.t+ih-fh).toFixed(1)}" width="${bw.toFixed(1)}" height="${fh.toFixed(1)}" rx="2" fill="none" stroke="${CC.y2026}" stroke-width="1.2" stroke-dasharray="3,2" opacity="0.45"/></g>`;
        return;
      }

      if (v==null||v<=0) return;
      const hh = barH(v);
      const bx = barX(mi,bi);

      // Build tooltip label — uses data-tip on <g> (SVG <title> unreliable in Chrome via innerHTML)
      const fv = fmtTip(v);
      let ttip='';
      if (bar.key==='y2024')      ttip=`${d.month} '24 Act: ${fv}`;
      else if (bar.key==='y2025') ttip=`${d.month} '25 Act: ${fv}`;
      else if (bar.key==='y2026') ttip=isCurrent?`${d.month} '26 MTD: ${fv}`:`${d.month} '26 Act: ${fv}`;
      else                        ttip=`${d.month} '26 Proj: ${fv}`;

      s+=`<g data-tip="${ttip}">`;
      s+=`<rect x="${bx.toFixed(1)}" y="${(pad.t+ih-hh).toFixed(1)}" width="${bw.toFixed(1)}" height="${hh.toFixed(1)}" fill="${bar.color}" rx="2" opacity="${isFcst?0.7:bar.key==='y2026'?1:0.82}"${isFcst?' stroke-dasharray="3,2" stroke="#f59e0b" stroke-width="0.8"':''}/>`;
      s+=`</g>`;
      if (v>0) s+=`<text x="${(bx+bw/2).toFixed(1)}" y="${(pad.t+ih-hh-3).toFixed(1)}" text-anchor="middle" font-size="7" font-weight="600" fill="${bar.lc}">${fmtLbl(v)}</text>`;
    });
    s+=`<text x="${(groupX(mi)+groupW/2).toFixed(1)}" y="${H-6}" text-anchor="middle" font-size="9" fill="#374f66">${d.month}</text>`;
  });

  // Legend (skip forecast if no data; add dashed-outline entry for future months placeholder)
  const legItems = barDefs.filter(b=>b.key!=='forecast'||hasForecast);
  legItems.forEach((b,i)=>{
    const lx=pad.l+i*100;
    const rectFill = b.color;
    const isFC = b.key==='forecast';
    s+=`<g transform="translate(${lx},${pad.t-18})">`;
    s+=`<rect width="8" height="8" y="-1" rx="2" fill="${isFC?'none':rectFill}" opacity="${isFC?1:0.85}"${isFC?` stroke="${b.color}" stroke-width="0.8" stroke-dasharray="2,1.5"`:''}/>`;
    s+=`<text x="11" y="7" font-size="9" fill="${B.sub}">${b.lbl}</text></g>`;
  });
  // Placeholder legend entry
  if (yearVis.y2026 && hasForecast) {
    const lx=pad.l+legItems.length*100;
    s+=`<g transform="translate(${lx},${pad.t-18})">`;
    s+=`<rect width="8" height="8" y="-1" rx="2" fill="none" stroke="${CC.y2026}" stroke-width="1.2" stroke-dasharray="3,2" opacity="0.55"/>`;
    s+=`<text x="11" y="7" font-size="9" fill="${B.sub}">Future</text></g>`;
  }

  return s+'</svg>';
}

// futureSvgCols: Set of SVG column indices (0=leftmost) that contain LY-proxy forecast data
// weekStartDates: array[w] → "Mon DD" string for each SVG column (may be empty '')
function heatmapSVG(data, W=1100, weeks=26, metricKey='units', futureSvgCols=new Set(), weekStartDates=[]) {
  if (!data || data.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No heatmap data</div>';
  const DAYS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
  const WEEKS = weeks;
  const mx = Math.max(...data.map(d => d[metricKey]||0), 1);
  // Extra top padding: W-number row (y=padT-20) + date row (y=padT-9) + gap
  const padL=36,padT=34,padR=8,padB=8,cellH=26;
  const cellW = Math.max(12, Math.floor((W-padL-padR)/WEEKS));
  const svgW = padL+WEEKS*cellW+padR, svgH = padT+7*cellH+padB;
  // Build lookup: "week,day" → metricKey value
  const lookup = {};
  data.forEach(d => { lookup[`${d.week},${d.day}`] = d[metricKey]||0; });
  let s = `<svg width="100%" viewBox="0 0 ${svgW} ${svgH}" style="overflow:visible;display:block">`;
  // Week labels: raised W-number + date below
  Array.from({length:WEEKS},(_,w) => {
    const showLabel = w%4===0 || w===WEEKS-1;
    if (!showLabel) return;
    const cx = (padL+(w+.5)*cellW).toFixed(1);
    const isFut = futureSvgCols.has(w);
    const wLblColor = isFut ? '#4ade80' : B.sub;
    s += `<text x="${cx}" y="${padT-20}" text-anchor="middle" font-size="8" font-weight="600" fill="${wLblColor}">W${WEEKS-w}</text>`;
    const dt = weekStartDates[w] || '';
    if (dt) s += `<text x="${cx}" y="${padT-9}" text-anchor="middle" font-size="7" fill="${isFut ? '#4ade80' : '#374f66'}" opacity="0.85">${dt}</text>`;
  });
  DAYS.forEach((d,i) => s+=`<text x="${padL-3}" y="${padT+(i+.5)*cellH+4}" text-anchor="end" font-size="9" fill="${B.sub}">${d}</text>`);
  // Full-spectrum heatmap: cool→warm color gradient based on value intensity
  const stops = [
    [21,37,62],[15,82,115],[13,115,119],[34,139,34],[218,165,32],[230,101,30],[214,40,57],
  ];
  const heatColor = (t) => {
    if (t <= 0) return 'rgb(21,37,62)';
    if (t >= 1) return 'rgb(214,40,57)';
    const seg = t * (stops.length - 1);
    const i = Math.floor(seg), f = seg - i;
    const a = stops[i], b = stops[Math.min(i+1, stops.length-1)];
    return `rgb(${Math.round(a[0]+(b[0]-a[0])*f)},${Math.round(a[1]+(b[1]-a[1])*f)},${Math.round(a[2]+(b[2]-a[2])*f)})`;
  };
  const sqrtMax = Math.sqrt(mx || 1);
  const isSales = metricKey === 'sales';
  // Draw column background for forecast (LY-proxy) columns
  if (futureSvgCols.size > 0) {
    for (const w of futureSvgCols) {
      s += `<rect x="${(padL+w*cellW).toFixed(1)}" y="${padT}" width="${cellW.toFixed(1)}" height="${(7*cellH).toFixed(1)}" fill="#0a2a1a" opacity="0.55" rx="2"/>`;
    }
  }
  for (let w=0; w<WEEKS; w++) {
    for (let d=0; d<7; d++) {
      const val = lookup[`${WEEKS-1-w},${d}`] || 0;
      const isFuture = futureSvgCols.has(w);
      const pct = val > 0 ? Math.sqrt(val) / sqrtMax : 0;
      const fill = val > 0 ? heatColor(pct) : isFuture ? 'rgb(10,42,26)' : 'rgb(21,37,62)';
      const opacity = val > 0 ? (0.55 + pct * 0.45).toFixed(2) : isFuture ? '0.45' : '0.18';
      s += `<rect x="${(padL+w*cellW+2).toFixed(1)}" y="${(padT+d*cellH+2).toFixed(1)}" width="${Math.max(4,cellW-4).toFixed(1)}" height="${(cellH-4).toFixed(1)}" rx="3" fill="${fill}" opacity="${opacity}"${isFuture && val===0 ? ' stroke="#1a4a2a" stroke-width="0.5"':''}/>`;
      if (val > 0 && cellW > 14) {
        const dispVal = isSales ? (val>=1000?`$${(val/1000).toFixed(0)}k`:`$${Math.round(val)}`) : val;
        const fs = String(dispVal).length > 4 ? 5 : 7;
        const txtFill = pct > 0.5 ? '#fff' : isFuture ? '#86efac' : '#c8d6e5';
        s += `<text x="${(padL+(w+.5)*cellW).toFixed(1)}" y="${(padT+(d+.5)*cellH+3.5).toFixed(1)}" text-anchor="middle" font-size="${fs}" font-weight="600" fill="${txtFill}">${dispVal}</text>`;
      }
    }
  }
  // Separator line + "LY Proxy" label between past and forecast sections
  if (futureSvgCols.size > 0 && futureSvgCols.size < WEEKS) {
    const futureCols = [...futureSvgCols].sort((a,b)=>a-b);
    const boundaries = new Set();
    for (const fc of futureCols) {
      if (!futureSvgCols.has(fc+1) && (fc+1)<WEEKS) boundaries.add(fc+1);
      if (!futureSvgCols.has(fc-1) && (fc-1)>=0) boundaries.add(fc);
    }
    for (const bw of boundaries) {
      const lx = (padL + bw*cellW - 1).toFixed(1);
      s += `<line x1="${lx}" y1="${padT-4}" x2="${lx}" y2="${padT+7*cellH+2}" stroke="#22c55e" stroke-width="1" stroke-dasharray="3 2" opacity="0.55"/>`;
    }
    const fcMidX = ((padL + Math.min(...futureCols)*cellW + padL + (Math.max(...futureCols)+1)*cellW)/2).toFixed(1);
    s += `<text x="${fcMidX}" y="${padT-24}" text-anchor="middle" font-size="7" fill="#22c55e" font-weight="700" opacity="0.8">── LY PROXY ──</text>`;
  }
  return s + '</svg>';
}

function funnelSVG(data) {
  if (!data || data.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No funnel data</div>';
  const n = data.length;
  const W = 1060, rowH = 64, padT = 44, padB = 48;
  const derivedRowH = 32;
  const H = padT + n * rowH + 16 + 2 * derivedRowH + padB;

  const funnelCX = 256, maxFW = 436;
  const labelX = 530, tyX = 706, lyX = 840, chgX = 956, stepX = 1052;

  const stepRates = data.map((d, i) =>
    i === 0 ? null : data[i-1].ty > 0 ? (d.ty / data[i-1].ty * 100) : null);

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

  const maxVal = data[0].ty || 1;
  const minW = maxFW * 0.08;
  const tyW_fn = val => Math.max(minW, (val / maxVal) * maxFW);
  const lyW_fn = (ty, ly) => ty > 0 ? (ly / ty) * tyW_fn(ty) : tyW_fn(ly);

  const stageColors = ['#1B4F8A','#1e5fa8','#1a6ba0','#186e8a','#0e7dad'];

  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  s += `<defs>`;
  data.forEach((_, i) => {
    const c = stageColors[i % stageColors.length];
    s += `<linearGradient id="fg${i}" x1="0" y1="0" x2="1" y2="0"><stop offset="0%" stop-color="${c}" stop-opacity=".98"/><stop offset="100%" stop-color="${c}" stop-opacity=".72"/></linearGradient>`;
  });
  s += `</defs>`;

  // Dark panel behind table
  s += `<rect x="${labelX - 14}" y="${padT - 36}" width="${W - labelX + 14}" height="${n * rowH + 44}" rx="8" fill="#0a1929" opacity=".65"/>`;

  // Column headers
  s += `<text x="${labelX}"  y="${padT - 15}" text-anchor="start" font-size="8" font-weight="700" fill="${B.sub}" letter-spacing="1.2">STAGE</text>`;
  s += `<text x="${tyX}"     y="${padT - 15}" text-anchor="end"   font-size="8" font-weight="700" fill="${B.t3}"  letter-spacing="1.2">TY</text>`;
  s += `<text x="${lyX}"     y="${padT - 15}" text-anchor="end"   font-size="8" font-weight="700" fill="${B.sub}" letter-spacing="1.2">LY</text>`;
  s += `<text x="${chgX}"    y="${padT - 15}" text-anchor="end"   font-size="8" font-weight="700" fill="${B.sub}" letter-spacing="1.2">VS LY</text>`;
  s += `<text x="${stepX}"   y="${padT - 15}" text-anchor="end"   font-size="8" font-weight="700" fill="${B.sub}" letter-spacing="1.2">STEP %</text>`;
  s += `<line x1="${labelX - 14}" y1="${padT - 7}" x2="${W - 6}" y2="${padT - 7}" stroke="${B.t2}" stroke-width="0.6" opacity=".45"/>`;

  data.forEach((f, i) => {
    const tyW = tyW_fn(f.ty);
    const lyW = lyW_fn(f.ty, f.ly);
    const nextF   = i < n - 1 ? data[i+1] : null;
    const nextTyW = nextF ? tyW_fn(nextF.ty) : tyW * 0.52;
    const nextLyW = nextF ? lyW_fn(nextF.ty, nextF.ly) : lyW * 0.52;
    const rowY = padT + i * rowH, segH = rowH - 5, rowMid = rowY + rowH / 2;

    s += `<path d="M${funnelCX - tyW/2},${rowY+2} L${funnelCX + tyW/2},${rowY+2} L${funnelCX + nextTyW/2},${rowY+segH} L${funnelCX - nextTyW/2},${rowY+segH} Z" fill="url(#fg${i})" opacity=".93"/>`;
    s += `<path d="M${funnelCX - lyW/2},${rowY} L${funnelCX + lyW/2},${rowY} L${funnelCX + nextLyW/2},${rowY+segH+2} L${funnelCX - nextLyW/2},${rowY+segH+2} Z" fill="none" stroke="#f59e0b" stroke-width="1.6" stroke-dasharray="5 3" opacity=".82"/>`;
    s += `<text x="${funnelCX}" y="${rowMid+5}" text-anchor="middle" font-size="13" font-weight="800" fill="#ffffff">${fN(f.ty)}</text>`;

    const lyDelta = f.ly > 0 ? dp(f.ty, f.ly) : null;
    const dCol = lyDelta != null ? (lyDelta >= 0 ? '#4ade80' : '#fb923c') : B.sub;
    const sr   = stepRates[i];
    const srCol = sr == null ? B.sub : sr >= 50 ? '#4ade80' : sr >= 20 ? B.t2 : '#f59e0b';

    if (i % 2 === 0) s += `<rect x="${labelX-14}" y="${rowY}" width="${W - labelX + 14}" height="${rowH}" fill="white" opacity=".02" rx="2"/>`;

    s += `<text x="${labelX}" y="${rowMid+5}" text-anchor="start" font-size="12" font-weight="600" fill="#dde8f5">${f.label}</text>`;
    s += `<text x="${tyX}"    y="${rowMid+5}" text-anchor="end"   font-size="14" font-weight="800" fill="#f0f8ff">${fN(f.ty)}</text>`;
    s += `<text x="${lyX}"    y="${rowMid+5}" text-anchor="end"   font-size="11"                   fill="${B.sub}">${fN(f.ly)}</text>`;
    s += lyDelta != null
      ? `<text x="${chgX}" y="${rowMid+5}" text-anchor="end" font-size="11" font-weight="700" fill="${dCol}">${lyDelta>=0?'\u25B2':'\u25BC'} ${Math.abs(lyDelta).toFixed(1)}%</text>`
      : `<text x="${chgX}" y="${rowMid+5}" text-anchor="end" font-size="11" fill="${B.sub}">\u2014</text>`;
    s += sr != null
      ? `<text x="${stepX}" y="${rowMid+5}" text-anchor="end" font-size="12" font-weight="700" fill="${srCol}">${sr.toFixed(0)}%</text>`
      : `<text x="${stepX}" y="${rowMid+5}" text-anchor="end" font-size="9"  font-weight="600" fill="${B.sub}">ENTRY</text>`;

    if (i < n-1) s += `<line x1="${labelX-14}" y1="${rowY+rowH}" x2="${W-6}" y2="${rowY+rowH}" stroke="${B.brd}" stroke-width="0.4" opacity=".4"/>`;
  });

  const divY = padT + n * rowH + 12;
  s += `<line x1="${labelX-14}" y1="${divY}" x2="${W-6}" y2="${divY}" stroke="${B.t2}" stroke-width="0.6" opacity=".35"/>`;
  const derivedRows = [];
  if (atcPct_ty != null) {
    const atcChg = atcPct_ly != null ? dp(parseFloat(atcPct_ty), parseFloat(atcPct_ly)) : null;
    derivedRows.push({ label:'Add-to-Cart %', ty:atcPct_ty+'%', ly:atcPct_ly!=null?atcPct_ly+'%':'\u2014', chg:atcChg });
  }
  if (conv_ty != null) {
    const convChg = conv_ly != null ? dp(parseFloat(conv_ty), parseFloat(conv_ly)) : null;
    derivedRows.push({ label:'Conversion %', ty:conv_ty+'%', ly:conv_ly!=null?conv_ly+'%':'\u2014', chg:convChg });
  }
  derivedRows.forEach((dm, i) => {
    const ry = divY + 24 + i * derivedRowH;
    const cCol = dm.chg != null ? (dm.chg>=0?'#4ade80':'#fb923c') : B.sub;
    s += `<rect x="${labelX-14}" y="${ry-18}" width="${W-labelX+14}" height="${derivedRowH}" fill="${B.t1}" opacity=".18" rx="3"/>`;
    s += `<text x="${labelX}" y="${ry+2}" text-anchor="start" font-size="11" font-weight="600" fill="${B.t3}">${dm.label}</text>`;
    s += `<text x="${tyX}"    y="${ry+2}" text-anchor="end"   font-size="14" font-weight="800" fill="${B.t2}">${dm.ty}</text>`;
    s += `<text x="${lyX}"    y="${ry+2}" text-anchor="end"   font-size="11"                   fill="${B.sub}">${dm.ly}</text>`;
    if (dm.chg != null) s += `<text x="${chgX}" y="${ry+2}" text-anchor="end" font-size="11" font-weight="700" fill="${cCol}">${dm.chg>=0?'\u25B2':'\u25BC'} ${Math.abs(dm.chg).toFixed(1)}%</text>`;
  });

  const legY = H - padB + 18;
  s += `<g transform="translate(16,${legY})">`;
  s += `<rect width="12" height="12" y="-2" rx="2" fill="${B.b2}" opacity=".9"/><text x="16" y="9" font-size="9" fill="${B.sub}">This Year (TY)</text>`;
  s += `<rect x="130" width="12" height="9" y="0" rx="2" fill="none" stroke="#f59e0b" stroke-width="1.5" stroke-dasharray="4 2"/><text x="146" y="9" font-size="9" fill="${B.sub}">Last Year outline</text>`;
  s += `<text x="300" y="9" font-size="9" fill="${B.sub}">STEP% \u2014 % of visitors passing through from prior stage</text>`;
  s += `</g>`;
  return s + '</svg>';
}


function adQuadrantSVG(data) {
  if (!data || !data.ty) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No ad efficiency data</div>';
  const W=1100, H=340, pad={t:52,r:130,b:62,l:82};
  const iw=W-pad.l-pad.r, ih=H-pad.t-pad.b;
  const acosMin=0, acosMax=0.40, roasMin=0, roasMax=12;
  const {acos:tyAcos, roas:tyRoas} = data.ty;
  const {acos:lyAcos, roas:lyRoas} = data.ly || {acos:0,roas:0};
  const px = v => pad.l+((v-acosMin)/(acosMax-acosMin))*iw;
  const py = v => pad.t+ih-((v-roasMin)/(roasMax-roasMin))*ih;
  const lyx=px(lyAcos), lyy=py(lyRoas), tyx=px(tyAcos), tyy=py(tyRoas);
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;

  // Quadrant backgrounds — use LY as crosshair origin
  const qX = lyx, qY = lyy;
  s+=`<rect x="${pad.l}" y="${pad.t}"  width="${qX-pad.l}"     height="${qY-pad.t}"      fill="${B.t1}" opacity=".10" rx="3"/>`;
  s+=`<rect x="${qX}"    y="${pad.t}"  width="${pad.l+iw-qX}"  height="${qY-pad.t}"      fill="${B.o2}" opacity=".07" rx="3"/>`;
  s+=`<rect x="${pad.l}" y="${qY}"     width="${qX-pad.l}"     height="${pad.t+ih-qY}"   fill="${B.b1}" opacity=".07" rx="3"/>`;
  s+=`<rect x="${qX}"    y="${qY}"     width="${pad.l+iw-qX}"  height="${pad.t+ih-qY}"   fill="${B.o1}" opacity=".12" rx="3"/>`;

  // Quadrant labels
  s+=`<text x="${(pad.l+qX)/2}"      y="${(pad.t+qY)/2-6}"   text-anchor="middle" font-size="11" font-weight="700" fill="${B.t2}"  opacity=".75">IDEAL</text>`;
  s+=`<text x="${(pad.l+qX)/2}"      y="${(pad.t+qY)/2+9}"   text-anchor="middle" font-size="9"                    fill="${B.t2}"  opacity=".5">Low ACOS \u00B7 High ROAS</text>`;
  s+=`<text x="${(qX+pad.l+iw)/2}"   y="${(pad.t+qY)/2-6}"   text-anchor="middle" font-size="11" font-weight="700" fill="${B.o2}"  opacity=".7">HIGH SPEND</text>`;
  s+=`<text x="${(qX+pad.l+iw)/2}"   y="${(pad.t+qY)/2+9}"   text-anchor="middle" font-size="9"                    fill="${B.o2}"  opacity=".5">Optimize bids</text>`;
  s+=`<text x="${(pad.l+qX)/2}"      y="${(qY+pad.t+ih)/2}"  text-anchor="middle" font-size="10" font-weight="600" fill="${B.sub}" opacity=".5">UNDERINVESTING</text>`;
  s+=`<text x="${(qX+pad.l+iw)/2}"   y="${(qY+pad.t+ih)/2}"  text-anchor="middle" font-size="11" font-weight="700" fill="${B.o1}"  opacity=".7">INEFFICIENT</text>`;

  // Grid lines + axis labels
  for(let i=0;i<=5;i++){
    const xv=acosMin+(acosMax-acosMin)*(i/5), xp=px(xv);
    s+=`<line x1="${xp}" y1="${pad.t}" x2="${xp}" y2="${pad.t+ih}" stroke="#1a2f4a" stroke-width="0.6"/>`;
    s+=`<text x="${xp}" y="${pad.t+ih+14}" text-anchor="middle" font-size="9" fill="#4a6a88">${(xv*100).toFixed(0)}%</text>`;
    const yv=roasMin+(roasMax-roasMin)*(i/5), yp=py(yv);
    s+=`<line x1="${pad.l}" y1="${yp}" x2="${pad.l+iw}" y2="${yp}" stroke="#1a2f4a" stroke-width="0.6"/>`;
    s+=`<text x="${pad.l-8}" y="${yp+4}" text-anchor="end" font-size="9" fill="#4a6a88">${yv.toFixed(1)}x</text>`;
  }

  // LY crosshair dashes
  s+=`<line x1="${lyx}" y1="${pad.t}" x2="${lyx}" y2="${pad.t+ih}" stroke="${B.sub}" stroke-width="1.2" stroke-dasharray="5 4" opacity=".45"/>`;
  s+=`<line x1="${pad.l}" y1="${lyy}" x2="${pad.l+iw}" y2="${lyy}" stroke="${B.sub}" stroke-width="1.2" stroke-dasharray="5 4" opacity=".45"/>`;
  s+=`<text x="${lyx+4}" y="${pad.t-8}" font-size="9" fill="${B.sub}" opacity=".8">LY (ACOS ${(lyAcos*100).toFixed(0)}%)</text>`;

  // Trajectory arrow LY → TY
  const angle=Math.atan2(tyy-lyy,tyx-lyx);
  const dist=Math.sqrt((tyx-lyx)**2+(tyy-lyy)**2);
  if(dist>20){
    const shorten=19;
    const ex=lyx+Math.cos(angle)*(dist-shorten), ey=lyy+Math.sin(angle)*(dist-shorten);
    s+=`<line x1="${lyx}" y1="${lyy}" x2="${ex}" y2="${ey}" stroke="${B.t2}" stroke-width="1.5" stroke-dasharray="4 3" opacity=".7"/>`;
    const aw=8,ah=5;
    const ax1=ex-aw*Math.cos(angle)+ah*Math.sin(angle), ay1=ey-aw*Math.sin(angle)-ah*Math.cos(angle);
    const ax2=ex-aw*Math.cos(angle)-ah*Math.sin(angle), ay2=ey-aw*Math.sin(angle)+ah*Math.cos(angle);
    s+=`<path d="M${ex},${ey} L${ax1},${ay1} L${ax2},${ay2} Z" fill="${B.t2}" opacity=".7"/>`;
  }

  // Dots
  s+=`<circle cx="${lyx}" cy="${lyy}" r="9"  fill="${B.dim}" opacity=".85" stroke="#0c1a2e" stroke-width="2"/>`;
  s+=`<text   x="${lyx}"  y="${lyy+4}" text-anchor="middle" font-size="8" font-weight="700" fill="#fff">LY</text>`;
  s+=`<circle cx="${tyx}" cy="${tyy}" r="18" fill="${B.b2}" opacity=".15"/>`;
  s+=`<circle cx="${tyx}" cy="${tyy}" r="12" fill="${B.b2}" stroke="#0c1a2e" stroke-width="2"/>`;
  s+=`<text   x="${tyx}"  y="${tyy+4}" text-anchor="middle" font-size="9" font-weight="700" fill="#fff">TY</text>`;

  // Callout box — flip left if TY dot is near right edge
  const goLeft = tyx > W * 0.60;
  const boxW = 168, boxH = 60;
  const bx = goLeft ? tyx - 22 - boxW : tyx + 22;
  const by = Math.min(pad.t, tyy - 35);
  s+=`<rect x="${bx}" y="${by}" width="${boxW}" height="${boxH}" rx="7" fill="#0f1f35" stroke="#1e3a5a" stroke-width="1.2"/>`;
  s+=`<text x="${bx+12}" y="${by+18}" font-size="11" font-weight="700" fill="#e8f4fd">ACOS: ${(tyAcos*100).toFixed(1)}%   ROAS: ${tyRoas.toFixed(2)}x</text>`;
  const acosChg = lyAcos > 0 ? ((1-tyAcos/lyAcos)*100).toFixed(1) : '0.0';
  const roasChg = lyRoas > 0 ? ((tyRoas/lyRoas-1)*100).toFixed(1) : '0.0';
  const acosUp = parseFloat(acosChg) >= 0;
  const roasUp = parseFloat(roasChg) >= 0;
  s+=`<text x="${bx+12}" y="${by+35}" font-size="9" fill="${acosUp?'#4ade80':'#fb923c'}">${acosUp?'\u25B2':'\u25BC'} ${Math.abs(acosChg)}% ACOS ${acosUp?'improvement':'increase'}</text>`;
  s+=`<text x="${bx+12}" y="${by+51}" font-size="9" fill="${roasUp?'#4ade80':'#fb923c'}">${roasUp?'\u25B2':'\u25BC'} ${Math.abs(roasChg)}% ROAS ${roasUp?'improvement':'decline'}</text>`;

  // Axis labels
  s+=`<text x="${pad.l+iw/2}" y="${H-6}" text-anchor="middle" font-size="11" fill="${B.sub}">ACOS \u2192  (lower is better)</text>`;
  s+=`<text x="${pad.l-60}" y="${pad.t+ih/2}" text-anchor="middle" font-size="11" fill="${B.sub}" transform="rotate(-90,${pad.l-60},${pad.t+ih/2})">ROAS \u2192  (higher is better)</text>`;
  return s + '</svg>';
}


function hourlySVG(tyData, lyData, currentHour, W=1100, H=180) {
  if (!tyData || tyData.length === 0) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No hourly data</div>';
  const pad = {t:20, r:18, b:36, l:58};
  const iw = W - pad.l - pad.r, ih = H - pad.t - pad.b;
  const nHours = 24;
  const barW = Math.max(12, (iw / nHours) * 0.58);
  const tyMap = {}, lyMap = {};
  tyData.forEach(d => { tyMap[d.hour] = d.sales || 0; });
  lyData.forEach(d => { lyMap[d.hour] = d.sales || 0; });
  const allVals = [...tyData.map(d => d.sales||0), ...lyData.map(d => d.sales||0)];
  const maxV = Math.max(...allVals, 1);
  const xH = h => pad.l + ((h + 0.5) / nHours) * iw;
  const yV = v => pad.t + ih - (v / maxV) * ih;
  const fmtHr = h => h === 0 ? '12a' : h < 12 ? `${h}a` : h === 12 ? '12p' : `${h-12}p`;
  const uid = `hr${Math.random().toString(36).slice(2,6)}`;
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  s += `<defs>`;
  s += `<linearGradient id="${uid}ty" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#2E6FBB" stop-opacity=".92"/><stop offset="100%" stop-color="#2E6FBB" stop-opacity=".35"/></linearGradient>`;
  s += `<linearGradient id="${uid}now" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#4DC5B8" stop-opacity=".95"/><stop offset="100%" stop-color="#4DC5B8" stop-opacity=".4"/></linearGradient>`;
  s += `</defs>`;

  // Grid lines + Y-axis labels
  for (let i = 0; i <= 4; i++) {
    const v = maxV * (i/4);
    const y = yV(v).toFixed(1);
    s += `<line x1="${pad.l}" y1="${y}" x2="${W-pad.r}" y2="${y}" stroke="#1a2f4a" stroke-width="${i===0?'0.8':'0.5'}"/>`;
    s += `<text x="${pad.l-6}" y="${(parseFloat(y)+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#4a6a88">${f$(v)}</text>`;
  }

  // Hour bands for readability (alternating light shading)
  for (let h = 0; h < 24; h+=2) {
    const bx = xH(h) - barW * 1.4;
    const bw = barW * 2.8;
    s += `<rect x="${bx.toFixed(1)}" y="${pad.t}" width="${bw.toFixed(1)}" height="${ih}" fill="white" opacity=".012"/>`;
  }

  // TY bars
  for (let h = 0; h < 24; h++) {
    const v = tyMap[h] || 0;
    if (v <= 0) continue;
    const bh = (v / maxV) * ih;
    const isCurrent = currentHour != null && h === currentHour;
    const isFuture  = currentHour != null && h > currentHour;
    s += `<rect x="${(xH(h) - barW/2).toFixed(1)}" y="${yV(v).toFixed(1)}" width="${barW.toFixed(1)}" height="${bh.toFixed(1)}" fill="${isCurrent ? `url(#${uid}now)` : `url(#${uid}ty)`}" rx="2" opacity="${isFuture ? 0.22 : 0.92}"/>`;
  }

  // LY line
  const lyPts = Array.from({length:24}, (_,h) => `${xH(h).toFixed(1)},${yV(lyMap[h]||0).toFixed(1)}`).join(' ');
  s += `<polyline points="${lyPts}" fill="none" stroke="#f59e0b" stroke-width="2" stroke-dasharray="5 3" stroke-linejoin="round" opacity=".85"/>`;
  for (let h = 0; h < 24; h++) {
    if ((lyMap[h]||0) > 0) s += `<circle cx="${xH(h).toFixed(1)}" cy="${yV(lyMap[h]).toFixed(1)}" r="2.5" fill="#f59e0b" opacity=".75"/>`;
  }

  // "Now" marker
  if (currentHour != null && currentHour >= 0 && currentHour <= 23) {
    const mx = xH(currentHour).toFixed(1);
    s += `<line x1="${mx}" y1="${pad.t-4}" x2="${mx}" y2="${pad.t+ih}" stroke="#4DC5B8" stroke-width="1.4" stroke-dasharray="3 3" opacity=".9"/>`;
    s += `<text x="${parseFloat(mx)+4}" y="${pad.t+2}" font-size="8" font-weight="700" fill="#4DC5B8">now</text>`;
  }

  // X-axis hour labels — every 3 hours
  [0,3,6,9,12,15,18,21,23].forEach(h => {
    s += `<text x="${xH(h).toFixed(1)}" y="${H-10}" text-anchor="middle" font-size="9" fill="#4a6a88">${fmtHr(h)}</text>`;
    s += `<line x1="${xH(h).toFixed(1)}" y1="${pad.t+ih}" x2="${xH(h).toFixed(1)}" y2="${pad.t+ih+4}" stroke="#1a2f4a" stroke-width="0.8"/>`;
  });
  s += `<line x1="${pad.l}" y1="${pad.t+ih}" x2="${W-pad.r}" y2="${pad.t+ih}" stroke="#1a2f4a" stroke-width="0.8"/>`;

  return s + '</svg>';
}


// ── Conversion Rate Arc Gauge ──────────────────────────────────────────────
// Semi-circular gauge: left=0%, right=max. TY arc colored by delta vs LY.
// LY position marked by a dot on the arc track.
function convGaugeSVG(tyRate, lyRate, W=300, H=155) {
  const maxRate = 0.10; // 10% scale covers typical Amazon range (1-5%)
  const cx = W/2, cy = H - 18, r = Math.min(cx - 12, H - 30);
  const f  = tyRate != null ? Math.min(Math.max(tyRate / maxRate, 0), 1) : null;
  const fLy = lyRate != null ? Math.min(Math.max(lyRate / maxRate, 0), 1) : null;
  // Point on arc at fraction t (0=left, 1=right, arches upward through top)
  const arcPt = t => {
    const angle = Math.PI * (1 - t); // π→0 as t→0→1
    return [cx + r * Math.cos(angle), cy - r * Math.sin(angle)];
  };
  // SVG arc path from left (t=0) to fraction t
  const arcPath = t => {
    if (t <= 0) return '';
    const [ex, ey] = arcPt(t);
    const largeArc = t > 0.5 ? 1 : 0;
    return `M${(cx-r).toFixed(1)},${cy.toFixed(1)} A${r},${r} 0 ${largeArc} 1 ${ex.toFixed(1)},${ey.toFixed(1)}`;
  };
  const delta = (tyRate != null && lyRate != null && lyRate > 0)
    ? (tyRate - lyRate) / lyRate * 100 : null;
  const fillColor = delta == null ? '#2E6FBB' : delta >= 0 ? '#1AA392' : '#E8821E';
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  // Background track
  s += `<path d="${arcPath(1)}" fill="none" stroke="#1a2f4a" stroke-width="16" stroke-linecap="round"/>`;
  // Zone tints: red 0-2%, yellow 2-4%, green 4%+
  [['#c0392b',0,0.20],['#d68910',0.20,0.40],['#1AA392',0.40,1.00]].forEach(([col,t0,t1])=>{
    const p0 = arcPath(t0===0?0.001:t0), p1 = arcPath(t1);
    if (p0 && p1) {
      // Draw zone as a short arc segment using stroke on a clipped path (approximate with opacity fill)
      const [x0,y0]=arcPt(t0===0?0.001:t0),[x1,y1]=arcPt(t1);
      const la = (t1-t0) > 0.5 ? 1 : 0;
      s += `<path d="M${x0.toFixed(1)},${y0.toFixed(1)} A${r},${r} 0 ${la} 1 ${x1.toFixed(1)},${y1.toFixed(1)}" fill="none" stroke="${col}" stroke-width="16" stroke-linecap="butt" opacity=".18"/>`;
    }
  });
  // TY fill arc
  if (f != null && f > 0) {
    s += `<path d="${arcPath(f)}" fill="none" stroke="${fillColor}" stroke-width="16" stroke-linecap="round" opacity=".95"/>`;
  }
  // LY dot marker
  if (fLy != null) {
    const [ldx,ldy] = arcPt(fLy);
    s += `<circle cx="${ldx.toFixed(1)}" cy="${ldy.toFixed(1)}" r="7" fill="#5b7fa0" stroke="#0c1a2e" stroke-width="2.5"/>`;
    s += `<text x="${ldx.toFixed(1)}" y="${(ldy+3.5).toFixed(1)}" text-anchor="middle" font-size="6" font-weight="700" fill="#fff">LY</text>`;
  }
  // Scale tick labels: 0, 2, 4, 6, 8, 10%
  [0,0.02,0.04,0.06,0.08,0.10].forEach(v => {
    const tf = v/maxRate;
    const angle = Math.PI*(1-tf);
    const tx = cx + (r+14)*Math.cos(angle), ty2 = cy - (r+14)*Math.sin(angle);
    s += `<text x="${tx.toFixed(1)}" y="${ty2.toFixed(1)}" text-anchor="middle" dominant-baseline="middle" font-size="7" fill="#374f66">${(v*100).toFixed(0)}%</text>`;
  });
  // Big TY number
  if (tyRate != null) {
    s += `<text x="${cx}" y="${cy-22}" text-anchor="middle" font-size="26" font-weight="800" fill="${fillColor}" letter-spacing="-1">${(tyRate*100).toFixed(2)}%</text>`;
    s += `<text x="${cx}" y="${cy-4}" text-anchor="middle" font-size="8" font-weight="700" fill="#5b7fa0" letter-spacing=".08em">CONVERSION TY</text>`;
  } else {
    s += `<text x="${cx}" y="${cy-12}" text-anchor="middle" font-size="12" fill="#374f66">—</text>`;
  }
  // Delta badge
  if (delta != null) {
    const dColor = delta >= 0 ? '#4ade80' : '#fb923c';
    s += `<text x="${cx}" y="${cy+14}" text-anchor="middle" font-size="10" font-weight="700" fill="${dColor}">${delta>=0?'▲':'▼'} ${Math.abs(delta).toFixed(1)}% vs LY</text>`;
  }
  return s + '</svg>';
}

// ── Sessions + Conversion Rate (dual Y-axis) ──────────────────────────────
// Left Y: sessions count (orange lines). Right Y: conversion % (teal bars TY/LY).
function sessionsConvSVG(data, W=1100, H=165) {
  if (!data || data.length < 2) return '<div style="color:#374f66;padding:20px;text-align:center;font-size:12px">No data for this period</div>';
  const pad = {t:14, r:64, b:24, l:58};
  const iw = W-pad.l-pad.r, ih = H-pad.t-pad.b;
  const n = data.length;
  // Bar-center x (for bars); line x (for lines, edge-to-edge)
  const xB = i => pad.l + ((i+0.5)/n)*iw;
  const xL = i => pad.l + (i/(n-1||1))*iw;
  // Sessions axis (left)
  const sessVals = data.flatMap(d => [d.ty_sessions, d.ly_sessions]).filter(v => v != null && v >= 0);
  const smx = Math.max(...sessVals, 1);
  const yS = v => pad.t + ih - Math.min(1,Math.max(0,(v||0)/smx))*ih;
  // Conversion axis (right) — use natural max with 20% headroom
  const convVals = data.flatMap(d => [d.ty_conv, d.ly_conv]).filter(v => v != null && v > 0);
  const cmx = convVals.length ? Math.max(...convVals)*1.25 : 0.08;
  const yC = v => pad.t + ih - Math.min(1,Math.max(0,(v||0)/cmx))*ih;
  const bw = Math.max(3, Math.floor((iw/n)*0.28)); // half-bar width
  const uid = `sc${Math.random().toString(36).slice(2,6)}`;
  let s = `<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
  s += `<defs><linearGradient id="${uid}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${B.o2}" stop-opacity=".16"/><stop offset="100%" stop-color="${B.o2}" stop-opacity="0"/></linearGradient></defs>`;
  // Grid lines + left axis (sessions)
  for (let i=0; i<=4; i++) {
    const sv=smx/4*i, ya=yS(sv);
    s += `<line x1="${pad.l}" y1="${ya.toFixed(1)}" x2="${W-pad.r}" y2="${ya.toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/>`;
    s += `<text x="${pad.l-5}" y="${(ya+4).toFixed(1)}" text-anchor="end" font-size="9" fill="${B.sub}">${fN(sv)}</text>`;
  }
  // Right axis (conversion %)
  for (let i=0; i<=4; i++) {
    const cv=cmx/4*i, ya=yC(cv);
    s += `<text x="${W-pad.r+5}" y="${(ya+4).toFixed(1)}" text-anchor="start" font-size="9" fill="#1aa39270">${(cv*100).toFixed(1)}%</text>`;
  }
  s += `<text x="${W-pad.r+5}" y="${pad.t-2}" text-anchor="start" font-size="8" fill="#1AA392" font-weight="600">Conv% →</text>`;
  // X axis labels
  const step = Math.max(1, Math.floor(n/7));
  data.forEach((d,i) => {
    if (i%step===0||i===n-1) {
      const lbl = d.date ? d.date.slice(5) : d.d||'';
      s += `<text x="${xL(i).toFixed(1)}" y="${H-4}" text-anchor="middle" font-size="8" fill="#374f66">${lbl}</text>`;
    }
  });
  // Conversion bars (drawn first, behind lines)
  data.forEach((d,i) => {
    const cx = xB(i);
    if ((d.ty_conv||0) > 0) {
      const bh = Math.max(2, ((d.ty_conv||0)/cmx)*ih);
      s += `<rect x="${(cx-bw*1.15).toFixed(1)}" y="${yC(d.ty_conv).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="#1AA392" opacity="0.62" rx="2"/>`;
    }
    if ((d.ly_conv||0) > 0) {
      const bh = Math.max(2, ((d.ly_conv||0)/cmx)*ih);
      s += `<rect x="${(cx+0.15).toFixed(1)}" y="${yC(d.ly_conv).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="none" stroke="#1AA392" stroke-width="1" stroke-dasharray="3 2" opacity="0.55" rx="2"/>`;
    }
  });
  // Sessions TY area fill + line
  const area = `M${xL(0)},${yS(data[0].ty_sessions||0)} ${data.map((d,i)=>`L${xL(i)},${yS(d.ty_sessions||0)}`).join(' ')} L${xL(n-1)},${pad.t+ih} L${xL(0)},${pad.t+ih} Z`;
  s += `<path d="${area}" fill="url(#${uid})"/>`;
  const ptsTy = data.map((d,i)=>d.ty_sessions!=null?`${xL(i).toFixed(1)},${yS(d.ty_sessions).toFixed(1)}`:null).filter(Boolean).join(' ');
  if (ptsTy) s += `<polyline points="${ptsTy}" fill="none" stroke="${B.o2}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>`;
  // Sessions LY dashed line
  const ptsLy = data.map((d,i)=>d.ly_sessions!=null?`${xL(i).toFixed(1)},${yS(d.ly_sessions).toFixed(1)}`:null).filter(Boolean).join(' ');
  if (ptsLy) s += `<polyline points="${ptsLy}" fill="none" stroke="${B.sub}" stroke-width="1.5" stroke-dasharray="4 3" stroke-linecap="round" stroke-linejoin="round"/>`;
  return s + '</svg>';
}

// ── SVGTip: wraps dangerouslySetInnerHTML SVG and shows floating tooltip ──
// Uses data-tip attributes on <g> elements — reliable in all browsers unlike SVG <title>
function SVGTip({ html }) {
  const [tip, setTip] = useState(null);
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const onMove = e => {
    const el = e.target && e.target.closest ? e.target.closest('[data-tip]') : null;
    if (el) {
      setTip(el.getAttribute('data-tip'));
      setPos({ x: e.clientX, y: e.clientY });
    } else {
      setTip(null);
    }
  };
  return (
    <div onMouseMove={onMove} onMouseLeave={() => setTip(null)}>
      <div dangerouslySetInnerHTML={{ __html: html }} />
      {tip && (
        <div style={{
          position: 'fixed',
          left: pos.x + 14,
          top: pos.y - 38,
          background: '#0d1f35',
          border: '1px solid #2a4a6a',
          borderRadius: 6,
          padding: '5px 11px',
          fontSize: 11,
          color: '#ddeeff',
          fontWeight: 600,
          pointerEvents: 'none',
          zIndex: 9999,
          whiteSpace: 'nowrap',
          boxShadow: '0 3px 10px rgba(0,0,0,.5)',
        }}>{tip}</div>
      )}
    </div>
  );
}

// ── SUB-COMPONENTS ─────────────────────────────────────────────────
function Spinner() {
  return (
    <div style={{display:'flex',justifyContent:'center',padding:'32px',color:'var(--txt3)',fontSize:12}}>
      Loading...
    </div>
  );
}

function ChartCard({ title, badge, children, noMargin, error, headerRight }) {
  return (
    <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:16,marginBottom:noMargin?0:12,transition:'background .3s'}}>
      <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:14}}>
        <span style={{fontSize:13,fontWeight:700,color:'var(--txt)'}}>{title}</span>
        <div style={{display:'flex',alignItems:'center',gap:6}}>
          {headerRight}
          {badge && <span style={{fontSize:10,padding:'2px 9px',borderRadius:99,background:'rgba(46,111,187,.15)',color:B.b3,border:'1px solid rgba(46,111,187,.2)'}}>{badge}</span>}
        </div>
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

function MetricCard({ label, value, ly, delta, expandContent, invert, goal, goalLabel }) {
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
      {/* Goal row — shown if goal prop provided */}
      {goal != null && (
        <div style={{marginTop:5,fontSize:9,color:'var(--txt3)'}}>
          <span style={{marginRight:3}}>{goalLabel||'Proj EOM:'}</span>
          <span style={{fontWeight:700,color:'#7ac2e0'}}>{goal}</span>
        </div>
      )}
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
  const mpRaw   = filters.marketplace || 'US'; // 'US' | 'CA'

  const [viewTab,     setViewTab]     = useState('Sales Summary');
  const [activePeriod,setActivePeriod]= useState('MTD');
  const [yearVis,     setYearVis]     = useState({y2024:true,y2025:true,y2026:true});
  const [cpSales,     setCpSales]     = useState('30D');
  const [showExecCharts, setShowExecCharts] = useState(false);
  const [cpTraffic,   setCpTraffic]   = useState('30D');
  const [customStart, setCustomStart] = useState('');
  const [customEnd,   setCustomEnd]   = useState('');
  const [hmMetric,    setHmMetric]    = useState('$');        // '$' | 'units' (hourly heatmap)
  const [hideNight,   setHideNight]   = useState(false);      // hide hours 0-5
  const [hideLate,    setHideLate]    = useState(false);      // hide hours 18-23
  const [hmWindows,   setHmWindows]   = useState(['A','B']);   // up to 2 of ['A','B','C','D'] (weekly heatmap windows)
  const [hmMetric2,   setHmMetric2]   = useState('units');     // 'units'|'sales'|'returns' (weekly heatmap)
  const [execPeriod,  setExecPeriod]  = useState('L30');       // executive tab chart period
  const [showDailyCharts, setShowDailyCharts] = useState(false); // daily tab heatmap toggle
  const [expandedPeriods, setExpandedPeriods] = useState({}); // period card row expand
  const [dismissedDailyInsights, setDismissedDailyInsights] = useState([]); // dismissed insight keys
  const [snoozedDailyInsights,  setSnoozedDailyInsights]  = useState({}); // {key: snoozeUntilMs}
  const [dailyAnomalyDismissed, setDailyAnomalyDismissed] = useState(false); // anomaly banner
  const [pricingCache,          setPricingCache]          = useState(null); // amazon pricing data

  // ── Per-chart period overrides (Daily tab) ──
  const [cpTrendChart,      setCpTrendChart]     = useState(null); // null = follow cpSales
  const [trendChart,        setTrendChart]       = useState(null);
  const [cpTrafficChart,    setCpTrafficChart]   = useState(null); // null = follow cpTraffic
  const [trendTrafficChart, setTrendTrafficChart]= useState(null);
  const [unitsChartMetric,  setUnitsChartMetric]  = useState('units'); // 'units'|'sales'|'returns'
  const [revChartMetric,    setRevChartMetric]    = useState('revenue'); // 'revenue'|'conversion'

  // Data state
  const [metrics,     setMetrics]     = useState(null);
  const [periodCols,  setPeriodCols]  = useState(null);
  const [trend,       setTrend]       = useState(null);
  const [yoy,         setYoy]         = useState(null);
  const [channel,     setChannel]     = useState(null);
  const [rolling,     setRolling]     = useState(null);
  const [heatmap,     setHeatmap]     = useState(null);
  const [trendTraffic,   setTrendTraffic]   = useState(null);
  const [metricsTraffic, setMetricsTraffic] = useState(null); // period-aware traffic KPIs
  const [funnel,      setFunnel]      = useState(null);
  const [execTrend,         setExecTrend]         = useState(null);
  const [execTrendTraffic,  setExecTrendTraffic]  = useState(null);
  const [execFunnel,        setExecFunnel]        = useState(null);
  const [adEff,       setAdEff]       = useState(null);
  const [feeBreak,    setFeeBreak]    = useState(null);
  const [adBreak,     setAdBreak]     = useState(null);
  const [hmData,      setHmData]      = useState(null);       // 30-day hourly heatmap
  const [loading,     setLoading]     = useState({});
  const [errors,      setErrors]      = useState({});

  const periodApiKey = PERIOD_API_MAP[activePeriod] || 'last_30d';
  const chartSalesApi = CHART_PERIOD_API[cpSales];
  const chartTrafficApi = CHART_PERIOD_API[cpTraffic];

  const baseParams = {
    division: divRaw  || null,
    customer: custRaw || null,
    marketplace: mpRaw || null,
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
    // Daily tab uses same exec-style periods (Today/Yesterday/WTD/MTD/YTD)
    const pcView = viewTab === 'Daily' ? 'exec' : viewTab.toLowerCase();
    load('periodCols', setPeriodCols, 'period-comparison', {...baseParams, view: pcView});
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
    load('yoy',     setYoy,     'monthly-yoy',baseParams);
  }, [divRaw, custRaw, cpSales]);

  // Fetch weekly heatmap — always 52W so any window selection works client-side
  useEffect(() => {
    load('heatmap', setHeatmap, 'heatmap', {...baseParams, weeks: 52});
  }, [divRaw, custRaw]);

  // Fetch traffic chart data + period-aware traffic KPIs when cpTraffic changes
  useEffect(() => {
    const params = {...baseParams, period: chartTrafficApi};
    load('trendTraffic',   setTrendTraffic,   'trend',   params);
    load('metricsTraffic', setMetricsTraffic, 'summary', params);
  }, [divRaw, custRaw, cpTraffic]);

  // Fetch executive tab chart data — tied to cpSales (same period as Sales Summary)
  useEffect(() => {
    if (viewTab !== 'Executive') return;
    const params = {...baseParams, period: chartSalesApi};
    load('execTrend',        setExecTrend,        'trend',  params);
    load('execTrendTraffic', setExecTrendTraffic, 'trend',  params);
    load('execFunnel',       setExecFunnel,       'funnel', params);
  }, [divRaw, custRaw, cpSales, viewTab]);

  // Fetch 30-day hourly heatmap data — reload on filter changes only
  useEffect(() => {
    load('hmData', setHmData, 'hourly-heatmap', {...baseParams, days: 30});
  }, [divRaw, custRaw]);

  // Fetch pricing cache for Competitor Price Watch (Daily tab)
  useEffect(() => {
    if (viewTab !== 'Daily') return;
    fetch('/api/profitability/amazon-pricing')
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d && !d.error) setPricingCache(d); })
      .catch(() => {});
  }, [viewTab]);


  // ── Per-chart overrides: reset when global period changes ──
  useEffect(() => { setCpTrendChart(null); setTrendChart(null); }, [cpSales]);
  useEffect(() => { setCpTrafficChart(null); setTrendTrafficChart(null); }, [cpTraffic]);
  useEffect(() => {
    if (!cpTrendChart || cpTrendChart === cpSales) { setTrendChart(null); return; }
    const api = CHART_PERIOD_API[cpTrendChart];
    if (api) load('trendChart', setTrendChart, 'trend', {...baseParams, period: api});
  }, [divRaw, custRaw, cpTrendChart]); // eslint-disable-line
  useEffect(() => {
    if (!cpTrafficChart || cpTrafficChart === cpTraffic) { setTrendTrafficChart(null); return; }
    const api = CHART_PERIOD_API[cpTrafficChart];
    if (api) load('trendTrafficChart', setTrendTrafficChart, 'trend', {...baseParams, period: api});
  }, [divRaw, custRaw, cpTrafficChart]); // eslint-disable-line

  const handleViewTab = v => { setViewTab(v); setActivePeriod(PERIODS[v]?.[0] || ''); };

  const m = metrics || {};
  const fellBack = m.fell_back;
  const ly = k => m[`ly_${k}`];
  const svgChart = html => <div dangerouslySetInnerHTML={{__html: html}}/>;
  const periods = viewTab === 'Custom' ? [] : PERIODS[viewTab] || [];

  // ── Extract YOY months + meta from new {months, meta} response ──
  const yoyMonths = yoy?.months ?? (Array.isArray(yoy) ? yoy : []);
  const yoyMeta   = yoy?.meta   ?? null;

  // ── Forecast computation ──
  const forecastMap = {};
  if (yoyMeta && yoyMonths.length > 0) {
    const { current_month, ly_mtd } = yoyMeta;
    const lyYearTotal = yoyMonths.reduce((s, m2) => s + (m2.y2025 || 0), 0);
    const curData     = yoyMonths.find(m2 => m2.month_num === current_month);
    const tyMtd       = curData?.y2026 || 0;
    const lyFullMonth = curData?.y2025 || 0;
    const lyPct       = (ly_mtd > 0 && lyFullMonth > 0) ? ly_mtd / lyFullMonth : 0;
    const curForecast = (lyPct > 0.01 && tyMtd > 0) ? tyMtd / lyPct : 0;
    if (curForecast > 0) forecastMap[current_month] = Math.round(curForecast);

    if (curForecast > 0 && lyYearTotal > 0 && lyFullMonth > 0) {
      const curLyShare  = lyFullMonth / lyYearTotal;
      const impliedM1   = curForecast / curLyShare;
      const ytdActs     = yoyMonths.filter(m2 => m2.month_num < current_month).reduce((s,m2)=>s+(m2.y2026||0),0);
      const lyYtdShare  = yoyMonths.filter(m2 => m2.month_num <= current_month).reduce((s,m2)=>s+(m2.y2025||0),0) / lyYearTotal;
      const impliedM2   = lyYtdShare > 0 ? (ytdActs + curForecast) / lyYtdShare : impliedM1;
      yoyMonths.forEach(m2 => {
        if (m2.month_num <= current_month || m2.y2026 != null) return;
        const moShare = (m2.y2025 || 0) / lyYearTotal;
        const avg = ((impliedM1 + impliedM2) / 2) * moShare;
        if (avg > 0) forecastMap[m2.month_num] = Math.round(avg);
      });
    }
  }

  // ── FY summary totals for Monthly Revenue card header ──
  const fy24Total = yoyMonths.reduce((s,m2)=>s+(m2.y2024||0),0);
  const fy25Total = yoyMonths.reduce((s,m2)=>s+(m2.y2025||0),0);
  const ytd26     = yoyMonths.reduce((s,m2)=>s+(m2.y2026||0),0);
  const proj26    = ytd26 + Object.values(forecastMap).reduce((s,v)=>s+v,0);

  return (
    <div style={{fontFamily:"'Sora',-apple-system,BlinkMacSystemFont,sans-serif",color:'var(--txt)'}}>

      {/* HEADER */}
      <div style={{marginBottom:20}}>
        <h2 style={{fontFamily:"'DM Serif Display',Georgia,serif",fontSize:22,fontWeight:400,margin:0,color:'var(--txt)'}}>{viewTab === 'Sales Summary' ? 'Sales Summary' : viewTab === 'Daily' ? 'Daily Sales' : viewTab === 'Weekly' ? 'Weekly Sales' : viewTab === 'Monthly' ? 'Monthly Sales' : viewTab === 'Yearly' ? 'Year-over-Year' : 'Sales Analytics'}</h2>
        <div style={{fontSize:12,color:'var(--txt3)',marginTop:3,fontFamily:"'Space Grotesk',monospace"}}>
          {mpRaw === 'CA' ? 'Amazon.ca (Canada)' : 'Amazon.com (US)'}
          {' \u00B7 '}
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

      {/* ══ EXECUTIVE DASHBOARD ══════════════════════════════════ */}
      {viewTab === 'Executive' && (() => {
        const tod = periodCols?.['Today'] || {};
        // TY NOW
        const tyS   = tod.sales  || 0;
        const tyU   = Math.max(0, tod.units  || 0);
        const tyO   = tod.orders || 0;
        const tyAur = tyU > 0 ? tyS / tyU : 0;
        // LY NOW (same time last year)
        const lyNowS   = tod.ly_same_time_sales  || 0;
        const lyNowU   = tod.ly_same_time_units  || 0;
        const lyNowO   = tod.ly_same_time_orders || 0;
        const lyNowAur = lyNowU > 0 ? lyNowS / lyNowU : 0;
        // TY EOD Forecast
        const tyFcstS   = tod.ty_forecast        || 0;
        const tyFcstU   = Math.max(0, tod.ty_units_forecast  || 0);
        const tyFcstAur = tyFcstU > 0 ? tyFcstS / tyFcstU : 0;
        // LY EOD Actual
        const lyEodS   = tod.ly_eod_sales  ?? tod.ly_sales  ?? 0;
        const lyEodU   = tod.ly_eod_units  ?? tod.ly_units  ?? 0;
        const lyEodO   = tod.ly_orders || 0;
        const lyEodAur = lyEodU > 0 ? lyEodS / lyEodU : 0;

        const dp2 = (a, b) => (a && b) ? (a - b) / b * 100 : null;
        const chgSpan = (delta, inv = false) => {
          if (delta == null) return <span style={{color:'var(--txt3)',fontSize:9}}>—</span>;
          const pos = inv ? delta < 0 : delta > 0;
          return <span style={{color: pos ? '#4ade80' : '#fb923c', fontWeight:700, fontSize:9, whiteSpace:'nowrap'}}>
            {delta > 0 ? '▲' : '▼'}{Math.abs(delta).toFixed(1)}%
          </span>;
        };

        const heroRows = (rows) => rows.map(([lbl, val]) => (
          <div key={lbl} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'4px 0',borderBottom:'1px solid rgba(26,47,74,.5)',fontSize:11}}>
            <span style={{color:'var(--txt3)'}}>{lbl}</span>
            <span style={{fontWeight:600,color:'var(--txt)'}}>{val}</span>
          </div>
        ));

        const EXEC_PERIODS = ['Today','Yesterday','WTD','MTD','YTD'];
        const ACCENTS      = [B.o2, 'var(--brd2)', 'var(--brd2)', B.b2, B.t2];

        return (
          <>
            {/* 4-column Today Hero */}
            {loading.periodCols
              ? <div style={{height:180,display:'flex',alignItems:'center',justifyContent:'center'}}><Spinner/></div>
              : (
              <div style={{display:'flex',background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:12,overflow:'hidden',marginBottom:14}}>
                {[
                  { lbl:'🟠 TY NOW', sub:`thru ${tod.snapshot_time||'today'} · ${fN(tyO)} orders`, val:f$(tyS), color:B.o2,
                    rows:[['Units', fN(tyU)], ['AUR', f$(tyAur)], ['Fees', f$(tod.amazon_fees||0)], ['Returns', f$(tod.returns_amount||0)], ['Conv %', fP(tod.conversion)]] },
                  { lbl:'🔵 LY NOW', sub:'same time last year', val:f$(lyNowS), color:B.b3,
                    rows:[['Units', fN(lyNowU)], ['AUR', f$(lyNowAur)], ['Fees', '—'], ['Returns', '—'], ['Conv %', fP(tod.ly_conversion)]] },
                  { lbl:'🟢 TY EOD FCST', sub:`projected full day · pacing ${tyFcstS > 0 && lyEodS > 0 ? ((tyFcstS/lyEodS-1)*100).toFixed(1)+'% vs LY EOD' : '—'}`, val:f$(tyFcstS), color:B.t2,
                    rows:[['Units', tyFcstU > 0 ? `~${fN(tyFcstU)}` : '—'], ['AUR', tyFcstAur > 0 ? f$(tyFcstAur) : '—'], ['Fees', '—'], ['Returns', '—'], ['Conv %', '—']] },
                  { lbl:'⬜ LY EOD ACTUAL', sub:'full day last year', val:f$(lyEodS), color:'var(--txt3)',
                    rows:[['Units', fN(lyEodU)], ['AUR', f$(lyEodAur)], ['Fees', f$(tod.ly_amazon_fees||0)], ['Returns', f$(tod.ly_returns_amount||0)], ['Conv %', fP(tod.ly_conversion)]] },
                ].map((col, i, arr) => (
                  <div key={col.lbl} style={{
                    flex:'1 1 0', padding:'14px 16px', minWidth:0,
                    borderRight: i < arr.length-1 ? '1px solid var(--brd)' : 'none',
                    background: i===0 ? 'rgba(27,79,138,.1)' : 'transparent',
                  }}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:col.color,marginBottom:4}}>{col.lbl}</div>
                    <div style={{fontSize:28,fontWeight:800,color:col.color,lineHeight:1,marginBottom:3}}>{col.val}</div>
                    <div style={{fontSize:10,color:'var(--txt3)',marginBottom:10}}>{col.sub}</div>
                    {heroRows(col.rows)}
                    {/* sparkline from trend data */}
                    {(() => {
                      const tArr = toArr(trend);
                      if (!tArr || tArr.length < 3) return null;
                      const last7 = tArr.slice(-7);
                      const isLY = col.lbl.includes('LY');
                      const vals = last7.map(d => isLY ? (d.ly_sales||0) : (d.ty_sales||0));
                      const maxV = Math.max(...vals, 1);
                      if (vals.every(v=>v===0)) return null;
                      const W=200, H=32, n=vals.length;
                      const pts=vals.map((v,i)=>`${(i/(n-1))*W},${H-(v/maxV)*(H-6)-3}`).join(' ');
                      const col2=isLY?'rgba(91,159,212,.5)':col.color;
                      return (
                        <div style={{height:32,margin:'8px 0 2px',position:'relative',opacity:.85}}>
                          <svg width="100%" height="32" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" style={{display:'block'}}>
                            <polyline points={pts} fill="none" stroke={col2} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round"/>
                            <circle cx={(n-1)/(n-1)*W} cy={H-(vals[n-1]/maxV)*(H-6)-3} r="3" fill={col2}/>
                          </svg>
                        </div>
                      );
                    })()}
                    {/* pace bar for TY NOW col only */}
                    {col.lbl.includes('TY') && !col.lbl.includes('EOD') && periodCols?.['MTD'] && (() => {
                      const mtdD = periodCols['MTD'];
                      const now = new Date();
                      const daysInMonth = new Date(now.getFullYear(), now.getMonth()+1, 0).getDate();
                      const dom = now.getDate();
                      const pct = Math.round((dom / daysInMonth) * 100);
                      const mtdSales = mtdD.sales || 0;
                      const projMo = dom > 0 ? (mtdSales / dom) * daysInMonth : 0;
                      const onPaceColor = mtdSales > 0 ? `linear-gradient(90deg,${B.t1},${B.t2})` : `linear-gradient(90deg,${B.o1},${B.o2})`;
                      return (
                        <div style={{marginTop:8,paddingTop:8,borderTop:'1px solid var(--brd)'}}>
                          <div style={{display:'flex',justifyContent:'space-between',fontSize:9,color:'var(--txt3)',marginBottom:4}}>
                            <span>MTD Progress</span>
                            <strong style={{color:'var(--txt)'}}>{dom}d / {daysInMonth}d</strong>
                          </div>
                          <div style={{height:5,background:'rgba(255,255,255,.06)',borderRadius:3,overflow:'hidden'}}>
                            <div style={{height:'100%',width:`${pct}%`,background:onPaceColor,borderRadius:3,transition:'width .6s ease'}}/>
                          </div>
                          <div style={{fontSize:9,marginTop:3,color:B.t3}}>
                            {pct}% elapsed · proj {f$(projMo)}/mo
                          </div>
                        </div>
                      );
                    })()}
                  </div>
                ))}
              </div>
            )}

            {/* Period Summary — 5 compact cards */}
            {periodCols && (
              <div style={{display:'grid',gridTemplateColumns:'repeat(5,1fr)',gap:10,marginBottom:20}}>
                {EXEC_PERIODS.map((lbl, idx) => {
                  const d  = periodCols[lbl] || {};
                  const isToday = lbl === 'Today';
                  const rows = [
                    { k:'Sales $', ty: isToday ? tyS   : d.sales,         ly: isToday ? lyNowS   : d.ly_sales,   fmt:f$,  inv:false },
                    { k:'Units',   ty: isToday ? tyU   : d.units,          ly: isToday ? lyNowU   : d.ly_units,   fmt:fN,  inv:false },
                    { k:'AUR',     ty: isToday ? tyAur : d.aur,            ly: isToday ? lyNowAur : d.ly_aur,     fmt:f$,  inv:false },
                    { k:'Orders',  ty: isToday ? tyO   : d.orders,         ly: isToday ? lyNowO   : d.ly_orders,  fmt:fN,  inv:false },
                    { k:'Fees',    ty: d.amazon_fees,                       ly: d.ly_amazon_fees,                  fmt:f$,  inv:true  },
                    { k:'Returns', ty: d.returns_amount||0,                 ly: d.ly_returns_amount||0,            fmt:f$,  inv:true  },
                    { k:'Conv %',  ty: d.conversion,                        ly: isToday ? tod.ly_conversion : d.ly_conversion, fmt:fP, inv:false },
                  ];
                  return (
                    <div key={lbl} style={{background:'var(--card2)',border:'1px solid var(--brd)',borderTop:`2px solid ${ACCENTS[idx]}`,borderRadius:10,padding:'10px 12px'}}>
                      <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:ACCENTS[idx],paddingBottom:5,marginBottom:6,borderBottom:'1px solid var(--brd)',display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                        <span>{lbl}</span>
                        {isToday && tod.snapshot_time && <span style={{fontSize:8,color:'var(--txt3)',fontWeight:400,textTransform:'none'}}>thru {tod.snapshot_time}</span>}
                      </div>
                      {rows.map(r => (
                        <div key={r.k} style={{display:'grid',gridTemplateColumns:'1.1fr 1fr 1fr 0.6fr',gap:2,alignItems:'center',padding:'3px 0',borderBottom:'1px solid rgba(26,47,74,.4)',fontSize:11}}>
                          <span style={{fontSize:10,color:'var(--txt3)',overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{r.k}</span>
                          <span style={{fontWeight:600,color:'var(--txt)'}}>{r.ty != null ? r.fmt(r.ty) : '—'}</span>
                          <span style={{color:'var(--txt3)',fontSize:10}}>{r.ly != null ? r.fmt(r.ly) : '—'}</span>
                          {chgSpan(dp2(r.ty, r.ly), r.inv)}
                        </div>
                      ))}
                    </div>
                  );
                })}
              </div>
            )}

            {/* ── Heatmap Charts Toggle ── */}
            <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:showExecCharts?10:14,padding:'8px 12px',
              background:showExecCharts?'rgba(30,58,92,.4)':'var(--card)',
              border:`1px solid ${showExecCharts?B.b2+'44':'var(--brd)'}`,borderRadius:9,cursor:'pointer'}}
              onClick={()=>setShowExecCharts(v=>!v)}>
              <div style={{display:'flex',alignItems:'center',gap:8}}>
                <span style={{fontSize:14}}>{showExecCharts ? '📉' : '📊'}</span>
                <span style={{fontSize:11,fontWeight:700,color:showExecCharts?B.b3:'var(--txt2)'}}>Heatmap Charts</span>
                <span style={{fontSize:9,color:B.sub}}>Hourly · 26-Week Daily</span>
              </div>
              <span style={{fontSize:11,color:showExecCharts?B.b3:B.sub,fontWeight:700,userSelect:'none'}}>
                {showExecCharts ? '▲ Hide' : '▼ Show'}
              </span>
            </div>

            {showExecCharts && <>
              {/* ── Hourly Heatmap (exec) ── */}
              {(()=>{
                const hmStops=[[21,37,62],[15,82,115],[13,115,119],[34,139,34],[218,165,32],[230,101,30],[214,40,57]];
                const hmColor=(t)=>{if(t<=0)return 'rgb(21,37,62)';if(t>=1)return 'rgb(214,40,57)';const seg=t*(hmStops.length-1);const i=Math.floor(seg),f=seg-i;const a=hmStops[i],b2=hmStops[Math.min(i+1,hmStops.length-1)];return `rgb(${Math.round(a[0]+(b2[0]-a[0])*f)},${Math.round(a[1]+(b2[1]-a[1])*f)},${Math.round(a[2]+(b2[2]-a[2])*f)})`};
                const HOUR_LABELS=['12am','1am','2am','3am','4am','5am','6am','7am','8am','9am','10am','11am','12pm','1pm','2pm','3pm','4pm','5pm','6pm','7pm','8pm','9pm','10pm','11pm'];
                const HCOL_W=26,DLW=72,DRH=28;
                const hmDays=hmData?.days||[];
                const maxVal=hmMetric==='$'?(hmData?.maxSales||0):(hmData?.maxUnits||0);
                const sqrtMax=Math.sqrt(maxVal||1);
                const visHours=Array.from({length:24},(_,i)=>i).filter(h=>!(hideNight&&h<6)).filter(h=>!(hideLate&&h>=18));
                const fCell=(v)=>{if(v==null||v===0)return '';if(hmMetric==='$'){if(v>=1000)return `$${(v/1000).toFixed(1)}k`;return `$${Math.round(v)}`;}if(v>=1000)return `${(v/1000).toFixed(1)}k`;return String(v)};
                const pillBtn=(label,active,onClick)=>(<button key={label} onClick={e=>{e.stopPropagation();onClick();}} style={{padding:'3px 10px',borderRadius:6,fontSize:10,fontWeight:600,cursor:'pointer',transition:'all .15s',border:`1px solid ${active?B.b2:'var(--brd)'}`,background:active?`${B.b1}33`:'transparent',color:active?B.b3:'var(--txt3)'}}>{label}</button>);
                const last7=hmDays.slice(-7);
                return (
                  <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:'14px 16px',marginBottom:10}}>
                    <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:10,flexWrap:'wrap',gap:8}}>
                      <span style={{fontSize:13,fontWeight:700,color:'var(--txt)'}}>Hourly Sales — Last 7 Days</span>
                      <div style={{display:'flex',gap:5,alignItems:'center',flexWrap:'wrap'}}>
                        {pillBtn('$',hmMetric==='$',()=>setHmMetric('$'))}
                        {pillBtn('Units',hmMetric==='units',()=>setHmMetric('units'))}
                        <div style={{width:1,height:16,background:'var(--brd)',margin:'0 2px'}}/>
                        {pillBtn(hideNight?'Show 12–5am':'Hide 12–5am',hideNight,()=>setHideNight(v=>!v))}
                        {pillBtn(hideLate?'Show 6–11pm':'Hide 6–11pm',hideLate,()=>setHideLate(v=>!v))}
                      </div>
                    </div>
                    {loading.hmData?<Spinner/>:last7.length===0?(
                      <div style={{padding:'24px',textAlign:'center',color:B.sub,fontSize:12}}>No hourly data yet</div>
                    ):(
                      <div style={{overflowX:'auto'}}>
                        {last7.map(d=>(
                          <div key={d.date} style={{display:'flex',alignItems:'center',marginBottom:2}}>
                            <div style={{width:DLW,flexShrink:0,paddingRight:8,textAlign:'right'}}>
                              <div style={{fontSize:9,fontWeight:d.isToday?700:500,color:d.isToday?B.b3:'var(--txt2)',lineHeight:'1.2',whiteSpace:'nowrap'}}>{d.dayOfWeek}</div>
                              <div style={{fontSize:8,color:d.isToday?B.b2:B.sub,lineHeight:'1.2',whiteSpace:'nowrap'}}>{d.label}</div>
                            </div>
                            {visHours.map(h=>{
                              const cell=(d.hours||{})[h]||{};
                              const val=hmMetric==='$'?cell.sales:cell.units;
                              const isFuture=(val===null||val===undefined);
                              const numVal=isFuture?0:(Number(val)||0);
                              const pct=(numVal>0)?Math.sqrt(numVal)/sqrtMax:0;
                              const bgColor=isFuture?'rgb(21,37,62)':(numVal>0?hmColor(pct):'rgb(21,37,62)');
                              const opacity=isFuture?0.06:(numVal>0?0.55+pct*0.45:0.16);
                              const txtColor=pct>0.5?'#ffffff':'#7a9bbf';
                              return (<div key={h} title={isFuture?`${d.label} ${HOUR_LABELS[h]}: future`:`${d.label} ${HOUR_LABELS[h]}: ${hmMetric==='$'?f$(numVal):`${numVal} units`}`} style={{width:HCOL_W-2,height:DRH,marginRight:2,background:bgColor,opacity,borderRadius:3,flexShrink:0,display:'flex',alignItems:'center',justifyContent:'center',fontSize:7,color:txtColor,fontWeight:600,overflow:'hidden',cursor:'default',border:d.isToday?`1px solid ${B.b2}44`:'none'}}>{!isFuture&&numVal>0&&fCell(numVal)}</div>);
                            })}
                          </div>
                        ))}
                        <div style={{display:'flex',marginLeft:DLW,marginTop:4}}>
                          {visHours.map(h=>(<div key={h} style={{width:HCOL_W-2,marginRight:2,flexShrink:0,textAlign:'center',fontSize:7,color:B.sub,lineHeight:'1.2'}}>{HOUR_LABELS[h]}</div>))}
                        </div>
                        <div style={{display:'flex',justifyContent:'space-between',marginTop:8,marginLeft:DLW}}>
                          <div style={{display:'flex',alignItems:'center',gap:3}}>
                            <span style={{fontSize:8,color:B.sub}}>Low</span>
                            {[0,.17,.33,.5,.67,.83,1].map(t=>(<div key={t} style={{width:10,height:10,borderRadius:2,background:t===0?'rgb(21,37,62)':hmColor(t),opacity:0.55+t*0.45}}/>))}
                            <span style={{fontSize:8,color:B.sub}}>High</span>
                          </div>
                          <span style={{fontSize:8,color:B.sub}}>{hmData?.lastUpdated?hmData.lastUpdated.replace('T',' '):'—'}</span>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })()}

            </>}

            {/* ── Sales Overview KPIs ── */}
            <div style={{display:'flex',gap:8,marginBottom:10,overflowX:'auto',paddingBottom:2}}>
              {loading.metrics ? <Spinner/> : <>
                <MetricCard label="Sales $"       value={f$(m.sales)}         ly={f$(ly('sales'))}         delta={dp(m.sales,ly('sales'))}/>
                <MetricCard label="Unit Sales"    value={fN(m.unit_sales)}     ly={fN(ly('unit_sales'))}    delta={dp(m.unit_sales,ly('unit_sales'))}/>
                <MetricCard label="AUR"           value={f$(m.aur)}            ly={f$(ly('aur'))}           delta={dp(m.aur,ly('aur'))}/>
                <MetricCard label="COGS"          value={f$(m.cogs)}           ly={f$(ly('cogs'))}          delta={dp(m.cogs,ly('cogs'))}/>
                <MetricCard label="Amazon Fees"   value={f$(m.amazon_fees)}    ly={f$(ly('amazon_fees'))}   delta={dp(m.amazon_fees,ly('amazon_fees'))}/>
                <MetricCard label="Returns" value={`${fN(m.returns)} · ${f$(m.returns_amount)}`} ly={`${fN(ly('returns'))} · ${f$(ly('returns_amount'))}`} delta={dp(m.returns,ly('returns'))} invert/>
                <MetricCard label="Gross Margin $"  value={f$(m.gross_margin)}     ly={f$(ly('gross_margin'))}     delta={dp(m.gross_margin,ly('gross_margin'))}/>
                <MetricCard label="Gross Margin %"  value={fP(m.gross_margin_pct)} ly={fP(ly('gross_margin_pct'))} delta={dp(m.gross_margin_pct,ly('gross_margin_pct'))}/>
              </>}
            </div>

            {/* ── Period selector — controls KPIs, charts, and pipeline below ── */}
            <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:14,paddingBottom:10,borderBottom:'1px solid var(--brd)'}}>
              <span style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',whiteSpace:'nowrap'}}>Period</span>
              {EXEC_PERIODS_LIST.map(p => (
                <button key={p} onClick={() => { const cp = EXEC_PERIOD_MAP[p]; setCpSales(cp); setActivePeriod(cp); }} style={{
                  fontSize:9,fontWeight:700,padding:'3px 9px',borderRadius:5,cursor:'pointer',
                  border:`1px solid ${cpSales===EXEC_PERIOD_MAP[p] ? B.b2 : 'var(--brd)'}`,
                  background: cpSales===EXEC_PERIOD_MAP[p] ? `${B.b2}22` : 'transparent',
                  color: cpSales===EXEC_PERIOD_MAP[p] ? B.b2 : 'var(--txt3)',
                  transition:'all .15s',
                }}>{p}</button>
              ))}
              {/* date range badge */}
              {(() => {
                const now2 = new Date();
                const activeP = EXEC_PERIODS_LIST.find(p => EXEC_PERIOD_MAP[p] === cpSales) || 'L30';
                const days2 = parseInt(activeP.replace('L','')) || 30;
                const end2 = new Date(now2); end2.setDate(end2.getDate()-1);
                const start2 = new Date(end2); start2.setDate(start2.getDate()-(days2-1));
                const fmt2 = d => d.toLocaleDateString('en-US',{month:'short',day:'numeric'});
                return (
                  <span style={{fontSize:10,color:B.b3,background:'rgba(46,111,187,.1)',border:'1px solid rgba(46,111,187,.2)',borderRadius:6,padding:'4px 10px',fontWeight:600,whiteSpace:'nowrap',marginLeft:4}}>
                    {fmt2(start2)} – {fmt2(end2)}, {end2.getFullYear()} &nbsp;·&nbsp; {days2} days
                  </span>
                );
              })()}
            </div>

            {/* ── Revenue & AUR Trend + Units Sold ── */}
            {(() => {
              return cpSales === '7D' ? (
              <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:12,marginBottom:12}}>
                <ChartCard title="Revenue & AUR Trend" error={errors.execTrend}>
                  {loading.execTrend ? <Spinner/> : <>
                    {svgChart(salesAurSVG(toArr(execTrend)))}
                    <Legend items={[['Sales TY','#2E6FBB'],['Sales LY','#5B9FD4',true],['AUR TY','#22c55e'],['AUR LY','#16a34a',true]]}/>
                  </>}
                </ChartCard>
                <ChartCard title="Units Sold — TY vs LY" error={errors.execTrend}>
                  {loading.execTrend ? <Spinner/> : (() => {
                    const tArr = toArr(execTrend);
                    if (!tArr || tArr.length < 2) return <div style={{color:'var(--txt3)',padding:20,textAlign:'center',fontSize:12}}>No data</div>;
                    const W=1100,H=165,pad={t:14,r:20,b:24,l:54};
                    const iw=W-pad.l-pad.r, ih=H-pad.t-pad.b, n=tArr.length;
                    const xB=i=>pad.l+((i+0.5)/n)*iw;
                    const uvArr=tArr.flatMap(d=>[d.ty_units,d.ly_units]).filter(v=>v!=null&&v>=0);
                    const umx=Math.max(...uvArr,1);
                    const yU=v=>pad.t+ih-Math.min(1,Math.max(0,(v||0)/umx))*ih;
                    const bw=Math.max(3,Math.floor((iw/n)*0.35));
                    let s=`<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
                    for(let i=0;i<=4;i++){const uval=umx/4*i,ya=yU(uval);s+=`<line x1="${pad.l}" y1="${ya.toFixed(1)}" x2="${W-pad.r}" y2="${ya.toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/><text x="${pad.l-5}" y="${(ya+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#5b7fa0">${fN(uval)}</text>`;}
                    const step=Math.max(1,Math.floor(n/7));
                    tArr.forEach((d,i)=>{if(i%step===0||i===n-1)s+=`<text x="${xB(i).toFixed(1)}" y="${H-4}" text-anchor="middle" font-size="8" fill="#374f66">${d.date?d.date.slice(5):''}</text>`;});
                    tArr.forEach((d,i)=>{if((d.ly_units||0)>0){const bh=Math.max(2,(d.ly_units/umx)*ih);s+=`<rect x="${xB(i).toFixed(1)}" y="${yU(d.ly_units).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="rgba(91,159,212,.35)" rx="2"/>`;}});
                    tArr.forEach((d,i)=>{if((d.ty_units||0)>0){const bh=Math.max(2,(d.ty_units/umx)*ih);s+=`<rect x="${(xB(i)-bw).toFixed(1)}" y="${yU(d.ty_units).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="${B.b1}" rx="2"/>`;}});
                    return <>{svgChart(s+'</svg>')}<Legend items={[['TY Units',B.b1],['LY Units','#5B9FD4',true]]}/></>;
                  })()}
                </ChartCard>
              </div>
            ) : (
              <div style={{display:'flex',flexDirection:'column',gap:12,marginBottom:12}}>
                <ChartCard title="Revenue & AUR Trend" error={errors.execTrend}>
                  {loading.execTrend ? <Spinner/> : <>
                    {svgChart(salesAurSVG(toArr(execTrend)))}
                    <Legend items={[['Sales TY','#2E6FBB'],['Sales LY','#5B9FD4',true],['AUR TY','#22c55e'],['AUR LY','#16a34a',true]]}/>
                  </>}
                </ChartCard>
                <ChartCard title="Units Sold — TY vs LY" error={errors.execTrend}>
                  {loading.execTrend ? <Spinner/> : (() => {
                    const tArr = toArr(execTrend);
                    if (!tArr || tArr.length < 2) return <div style={{color:'var(--txt3)',padding:20,textAlign:'center',fontSize:12}}>No data</div>;
                    const W=1100,H=190,pad={t:14,r:20,b:24,l:54};
                    const iw=W-pad.l-pad.r, ih=H-pad.t-pad.b, n=tArr.length;
                    const xB=i=>pad.l+((i+0.5)/n)*iw;
                    const uvArr=tArr.flatMap(d=>[d.ty_units,d.ly_units]).filter(v=>v!=null&&v>=0);
                    const umx=Math.max(...uvArr,1);
                    const yU=v=>pad.t+ih-Math.min(1,Math.max(0,(v||0)/umx))*ih;
                    const bw=Math.max(3,Math.floor((iw/n)*0.35));
                    let s=`<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
                    for(let i=0;i<=4;i++){const uval=umx/4*i,ya=yU(uval);s+=`<line x1="${pad.l}" y1="${ya.toFixed(1)}" x2="${W-pad.r}" y2="${ya.toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/><text x="${pad.l-5}" y="${(ya+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#5b7fa0">${fN(uval)}</text>`;}
                    const step=Math.max(1,Math.floor(n/7));
                    tArr.forEach((d,i)=>{if(i%step===0||i===n-1)s+=`<text x="${xB(i).toFixed(1)}" y="${H-4}" text-anchor="middle" font-size="8" fill="#374f66">${d.date?d.date.slice(5):''}</text>`;});
                    tArr.forEach((d,i)=>{if((d.ly_units||0)>0){const bh=Math.max(2,(d.ly_units/umx)*ih);s+=`<rect x="${xB(i).toFixed(1)}" y="${yU(d.ly_units).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="rgba(91,159,212,.35)" rx="2"/>`;}});
                    tArr.forEach((d,i)=>{if((d.ty_units||0)>0){const bh=Math.max(2,(d.ty_units/umx)*ih);s+=`<rect x="${(xB(i)-bw).toFixed(1)}" y="${yU(d.ty_units).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="${B.b1}" rx="2"/>`;}});
                    return <>{svgChart(s+'</svg>')}<Legend items={[['TY Units',B.b1],['LY Units','#5B9FD4',true]]}/></>;
                  })()}
                </ChartCard>
              </div>
            );})()}

            {/* ── Traffic & Conversion Pipeline (exec period-controlled) ── */}
            <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,marginBottom:12}}>
              <div style={{display:'flex',alignItems:'center',gap:10,marginBottom:12}}>
                <div style={{flex:1,height:1,background:'var(--brd)'}}/>
                <span style={{fontSize:10,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',whiteSpace:'nowrap'}}>Traffic &amp; Conversion Pipeline</span>
                <div style={{flex:1,height:1,background:'var(--brd)'}}/>
              </div>
              {Array.isArray(execFunnel) && execFunnel.length > 0 && (() => {
                const maxV=Math.max(...execFunnel.map(s2=>s2.ty||0),1);
                const sColors=[B.b3,B.b2,B.o2,B.t2];
                const rates=execFunnel.map((s2,i)=>i===0?null:execFunnel[i-1].ty>0?((s2.ty||0)/execFunnel[i-1].ty*100).toFixed(1)+'%':null);
                return (
                  <div style={{display:'grid',gridTemplateColumns:`repeat(${execFunnel.length},1fr)`,gap:0,border:'1px solid var(--brd)',borderRadius:8,overflow:'hidden',marginBottom:12}}>
                    {execFunnel.map((s2,i)=>{
                      const lyDelta=s2.ly>0?((s2.ty-s2.ly)/s2.ly*100):null;
                      const col=sColors[i%sColors.length];
                      return (
                        <div key={s2.label} style={{padding:'10px 14px',borderRight:i<execFunnel.length-1?'1px solid var(--brd)':'none',position:'relative'}}>
                          <div style={{fontSize:10,color:'var(--txt3)',fontWeight:600,marginBottom:4,whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{s2.label}</div>
                          <div style={{fontSize:20,fontWeight:800,color:col}}>{(s2.ty||0).toLocaleString()}</div>
                          <div style={{height:4,background:'var(--brd)',borderRadius:2,overflow:'hidden',margin:'6px 0 4px'}}>
                            <div style={{height:'100%',width:`${Math.max(4,(s2.ty/maxV)*100)}%`,background:col,borderRadius:2}}/>
                          </div>
                          {rates[i] && <div style={{fontSize:9,color:'var(--txt3)'}}>{rates[i]} step-through</div>}
                          {lyDelta!==null && <div style={{fontSize:10,fontWeight:700,color:lyDelta>=0?'#4ade80':'#fb923c'}}>{lyDelta>=0?'▲':'▼'}{Math.abs(lyDelta).toFixed(1)}% vs LY</div>}
                          {i<execFunnel.length-1 && <div style={{position:'absolute',right:-9,top:'50%',transform:'translateY(-50%)',fontSize:14,color:'var(--brd2)',zIndex:1}}>›</div>}
                        </div>
                      );
                    })}
                  </div>
                );
              })()}
              <div style={{display:'grid',gridTemplateColumns:'3fr 1fr',gap:12}}>
                <ChartCard title="Sessions & Conversion Rate" badge={execPeriod} error={errors.execTrendTraffic} noMargin>
                  {loading.execTrendTraffic ? <Spinner/> : <>
                    {svgChart(sessionsConvSVG(toArr(execTrendTraffic)))}
                    <Legend items={[['Sessions TY',B.o2],['Sessions LY',B.sub,true],['Conv% TY',B.t2]]}/>
                  </>}
                </ChartCard>
                <div style={{display:'flex',flexDirection:'column',gap:10}}>
                  <div style={{background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:10,padding:'12px 14px'}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:8}}>Traffic KPIs · MTD</div>
                    {metricsTraffic && [
                      ['Page Views', fN(metricsTraffic.glance_views||0), metricsTraffic.glance_views||0, metricsTraffic.ly_glance_views||0],
                      ['Sessions',   fN(metricsTraffic.sessions||0),     metricsTraffic.sessions||0,     metricsTraffic.ly_sessions||0],
                      ['Conv %',     fP(metricsTraffic.conversion||0),   metricsTraffic.conversion||0,   metricsTraffic.ly_conversion||0],
                      ['Buy Box %',  fP(metricsTraffic.buy_box||0),      metricsTraffic.buy_box||0,      metricsTraffic.ly_buy_box||0],
                    ].map(([lbl,val,ty,ly2])=>{
                      const delta=ly2>0?((ty-ly2)/ly2*100):null;
                      return (
                        <div key={lbl} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'4px 0',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                          <span style={{fontSize:11,color:'var(--txt3)'}}>{lbl}</span>
                          <div style={{display:'flex',alignItems:'center',gap:5}}>
                            <span style={{fontSize:13,fontWeight:700,color:'var(--txt)'}}>{val}</span>
                            {delta!==null && <span style={{fontSize:9,fontWeight:700,padding:'1px 5px',borderRadius:4,background:delta>=0?'rgba(34,197,94,.12)':'rgba(239,68,68,.12)',color:delta>=0?'#4ade80':'#f87171'}}>{delta>=0?'▲':'▼'}{Math.abs(delta).toFixed(1)}%</span>}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>{/* closes right-side flex col */}
              </div>{/* closes 3fr/1fr grid */}
            </div>{/* closes Traffic & Conversion outer card */}

            {/* ── Section 6: Business Health + Actions ── */}
            <div style={{display:'grid',gridTemplateColumns:'1fr 2fr',gap:12,marginBottom:12}}>
              {(() => {
                const tod2=periodCols?.['Today']||{};
                const mtdH=periodCols?.['MTD']||{};
                const paceScore=tod2.ty_forecast>0&&tod2.ly_eod_sales>0?Math.min(100,Math.round((tod2.ty_forecast/(tod2.ly_eod_sales||1))*50+50)):72;
                const convScore=metricsTraffic?.conversion>0?Math.min(100,Math.round((metricsTraffic.conversion/0.14)*85)):70;
                const roasScore=m.roas>0?Math.min(100,Math.round((m.roas/3.5)*100)):65;
                const atcS=Array.isArray(funnel)&&funnel.length>2?(funnel[2]?.ty||0)/(funnel[0]?.ty||1):0;
                const atcScore=Math.min(100,Math.round((atcS/0.22)*80));
                const retRatioH=(mtdH.returns_amount||0)/(mtdH.sales||1);
                const retScore=Math.min(100,Math.max(0,Math.round((1-retRatioH/0.05)*100)));
                const dosScore=m.dos>0?Math.min(100,Math.round(Math.min(m.dos,90)/90*100)):60;
                const overall=Math.round((paceScore+convScore+roasScore+atcScore+retScore+dosScore)/6);
                const scoreColor=overall>=80?'#22c55e':overall>=65?B.t2:overall>=50?'#f59e0b':'#ef4444';
                const scoreLabel=overall>=80?'Excellent':overall>=65?'Good':overall>=50?'Fair':'Needs Attention';
                const circ=2*Math.PI*38;
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,display:'flex',flexDirection:'column',alignItems:'center',gap:14}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',width:'100%',textAlign:'center'}}>Business Health Score</div>
                    <div style={{display:'flex',alignItems:'center',gap:16,width:'100%'}}>
                      <div style={{position:'relative',width:88,height:88,flexShrink:0}}>
                        <svg viewBox="0 0 88 88" width="88" height="88" style={{transform:'rotate(-90deg)'}}>
                          <circle cx="44" cy="44" r="38" fill="none" stroke="var(--brd)" strokeWidth="8"/>
                          <circle cx="44" cy="44" r="38" fill="none" stroke={scoreColor} strokeWidth="8"
                            strokeDasharray={`${(overall/100)*circ} ${circ}`} strokeLinecap="round"/>
                        </svg>
                        <div style={{position:'absolute',inset:0,display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center'}}>
                          <div style={{fontSize:22,fontWeight:800,lineHeight:1,color:scoreColor}}>{overall}</div>
                          <div style={{fontSize:8,fontWeight:700,textTransform:'uppercase',letterSpacing:'.06em',color:'var(--txt3)',marginTop:1}}>/ 100</div>
                        </div>
                      </div>
                      <div style={{flex:1}}>
                        <div style={{fontSize:14,fontWeight:700,color:scoreColor,marginBottom:3}}>{scoreLabel}</div>
                        <div style={{fontSize:10,color:'var(--txt3)',lineHeight:1.5}}>Revenue pace, conversion, ROAS, funnel efficiency, returns &amp; inventory.</div>
                      </div>
                    </div>
                    <div style={{width:'100%',display:'flex',flexDirection:'column',gap:5}}>
                      {[['Revenue Pace',paceScore,'#22c55e'],['Conversion Rate',convScore,B.t2],['ROAS / Ad Eff.',roasScore,B.b3],['ATC / Funnel',atcScore,'#f59e0b'],['Return Rate',retScore,'#22c55e'],['Inventory DOS',dosScore,B.b3]].map(([lbl,sc,col])=>(
                        <div key={lbl} style={{display:'flex',alignItems:'center',gap:8,fontSize:10}}>
                          <span style={{width:96,color:'var(--txt3)',whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{lbl}</span>
                          <div style={{flex:1,height:5,background:'var(--brd)',borderRadius:3,overflow:'hidden'}}>
                            <div style={{width:`${sc}%`,height:'100%',background:col,borderRadius:3}}/>
                          </div>
                          <span style={{width:26,textAlign:'right',fontWeight:700,color:col}}>{sc}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })()}
              {(() => {
                const mtdA=periodCols?.['MTD']||{};
                const actions=[];
                const atcIdx=Array.isArray(funnel)?funnel.findIndex(s2=>s2.label?.toLowerCase().includes('cart')):-1;
                const sessIdx=Array.isArray(funnel)?funnel.findIndex(s2=>s2.label?.toLowerCase().includes('sess')):-1;
                const atcRate=atcIdx>=0&&sessIdx>=0&&(funnel[sessIdx]?.ty||0)>0?(funnel[atcIdx].ty/funnel[sessIdx].ty):0;
                if(atcRate>0&&atcRate<0.18) actions.push({type:'alert',title:`🛒 ATC Rate low at ${(atcRate*100).toFixed(1)}% — benchmark ~22%`,desc:'Add-to-cart rate is below the 90-day benchmark. Review top ASINs for pricing gaps, competitor undercutting, or missing A+ content.'});
                if((m.tacos||0)>0.16) actions.push({type:'warn',title:`📣 TACOS at ${fP(m.tacos)} — approaching 16% threshold`,desc:'Ad spend as a % of revenue is elevated. Consider pausing underperforming campaigns or reducing bids on low-ROAS keywords.'});
                if((m.dos||0)>0&&(m.dos||0)<30) actions.push({type:'alert',title:`📦 Days of Supply at ${m.dos}d — reorder approaching`,desc:`With ${m.dos} days of supply, FBA stock is running low. Create a shipment plan now to avoid stockouts and BSR drop.`});
                const mtdChg=(mtdA.ly_sales||0)>0?((mtdA.sales||0)-(mtdA.ly_sales||0))/(mtdA.ly_sales||1):null;
                if(mtdChg!==null&&mtdChg>=0.05) actions.push({type:'ok',title:`✅ MTD pacing ▲${(mtdChg*100).toFixed(1)}% vs LY`,desc:'Revenue is tracking above last year. Consider increasing ad budget to capture more share while TACOS is healthy.'});
                else if(mtdChg!==null&&mtdChg<-0.02) actions.push({type:'warn',title:`📉 MTD revenue ▼${(Math.abs(mtdChg)*100).toFixed(1)}% vs LY`,desc:'Revenue is trailing last year MTD. Check for suppressed listings, lost Buy Box, or gaps in ad coverage.'});
                const retR=(mtdA.returns_amount||0)/(mtdA.sales||1);
                if(retR>0&&retR<0.04) actions.push({type:'ok',title:`✅ Return rate ${(retR*100).toFixed(1)}% — below 4% target`,desc:'Returns are well-controlled. Maintain current listing accuracy and A+ content that is driving this improvement.'});
                else if(retR>0.07) actions.push({type:'alert',title:`⚠ Return rate ${(retR*100).toFixed(1)}% — above 7% threshold`,desc:'High returns may indicate listing inaccuracy, product issues, or sizing confusion. Review top return reasons in Seller Central.'});
                if((m.roas||0)>0&&(m.roas||0)<2.5) actions.push({type:'warn',title:`📊 ROAS at ${fX(m.roas)} — below 2.5x floor`,desc:'Ad return on spend is low. Review campaign structure, negative keywords, and bid strategy. Consider pausing bottom-decile ASINs.'});
                if(actions.length===0) actions.push({type:'info',title:'✅ All metrics within healthy ranges',desc:'No urgent actions detected. Review the detailed tabs for optimization opportunities in advertising, inventory, and pricing.'});
                const tStyle={alert:{border:'rgba(239,68,68,.2)',bg:'rgba(239,68,68,.05)',dot:'#ef4444'},warn:{border:'rgba(245,158,11,.2)',bg:'rgba(245,158,11,.05)',dot:'#f59e0b'},ok:{border:'rgba(34,197,94,.2)',bg:'rgba(34,197,94,.05)',dot:'#22c55e'},info:{border:'rgba(91,159,212,.2)',bg:'rgba(91,159,212,.04)',dot:B.b3}};
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',marginBottom:12}}>📋 Actions to Take Today</div>
                    <div style={{display:'flex',flexDirection:'column',gap:8}}>
                      {actions.slice(0,6).map((a,i)=>{
                        const st=tStyle[a.type]||tStyle.info;
                        return (
                          <div key={i} style={{display:'flex',gap:10,alignItems:'flex-start',padding:'10px 12px',borderRadius:8,border:`1px solid ${st.border}`,background:st.bg}}>
                            <div style={{width:8,height:8,borderRadius:'50%',background:st.dot,flexShrink:0,marginTop:4}}/>
                            <div>
                              <div style={{fontSize:12,fontWeight:600,color:'var(--txt)',marginBottom:2}}>{a.title}</div>
                              <div style={{fontSize:10,color:'var(--txt3)',lineHeight:1.5}}>{a.desc}</div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })()}
            </div>

            {/* ── Section 7: Profitability Snapshot ── */}
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:12,marginBottom:12}}>
              {(() => {
                const mtdP=periodCols?.['MTD']||{};
                const rev=mtdP.sales||0;
                const ret=mtdP.returns_amount||0;
                const netRev=rev-ret;
                const fees=Math.abs(mtdP.amazon_fees||0);
                const adSpend=m.ad_spend||0;
                const cogs=netRev*0.35;
                const cm=netRev-fees-adSpend-cogs;
                const cmPct=netRev>0?cm/netRev*100:0;
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:10}}>P&amp;L Summary · MTD</div>
                    {[['Gross Revenue',rev,B.t2,false],['− Returns &amp; Refunds',-ret,'#ef4444',true],['= Net Revenue',netRev,'var(--txt)',false],['− Amazon Fees',-fees,'#ef4444',true],['− Ad Spend',-adSpend,'#ef4444',true],['− COGS est. 35%',-cogs,'#ef4444',true]].map(([lbl,val,col,neg],idx)=>(
                      <div key={idx} style={{display:'flex',justifyContent:'space-between',padding:'6px 0',borderBottom:lbl.startsWith('=')?'1px solid var(--brd)':'1px solid rgba(26,47,74,.4)',alignItems:'center'}}>
                        <span style={{fontSize:11,color:neg?'var(--txt3)':'var(--txt2)'}}>{lbl}</span>
                        <span style={{fontSize:12,fontWeight:700,color:col}}>{neg?'−':''}{f$(Math.abs(val))}</span>
                      </div>
                    ))}
                    <div style={{display:'flex',justifyContent:'space-between',padding:'8px 0 2px',alignItems:'center'}}>
                      <span style={{fontSize:12,fontWeight:700,color:'var(--txt)'}}>Contribution Margin</span>
                      <span style={{fontSize:16,fontWeight:800,color:B.t2}}>{f$(cm)}</span>
                    </div>
                    <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                      <span style={{fontSize:10,color:'var(--txt3)'}}>Margin %</span>
                      <span style={{fontSize:12,fontWeight:700,color:B.t3}}>{cmPct.toFixed(1)}%</span>
                    </div>
                  </div>
                );
              })()}
              <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>
                <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:10}}>Amazon Fee Breakdown · MTD</div>
                {feeBreak && (feeBreak.items||Array.isArray(feeBreak)) ? (() => {
                  const items=feeBreak.items||feeBreak;
                  const total=items.reduce((s2,f2)=>s2+(f2.amount||0),0);
                  const fColors=['#E87830',B.b2,B.t2,B.sub,'#f59e0b'];
                  return <>
                    {items.slice(0,5).map((f2,i)=>{
                      const pct=total>0?((f2.amount||0)/total*100).toFixed(1):'0';
                      return (
                        <div key={f2.type} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'5px 0',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                          <div style={{display:'flex',alignItems:'center',gap:6}}>
                            <div style={{width:10,height:10,borderRadius:2,background:fColors[i%fColors.length],flexShrink:0}}/>
                            <span style={{fontSize:11,color:'var(--txt3)'}}>{f2.type}</span>
                          </div>
                          <div style={{textAlign:'right'}}>
                            <div style={{fontSize:12,fontWeight:700,color:fColors[i%fColors.length]}}>{f$(f2.amount||0)}</div>
                            <div style={{fontSize:9,color:'var(--txt3)'}}>{pct}%</div>
                          </div>
                        </div>
                      );
                    })}
                    <div style={{display:'flex',justifyContent:'space-between',padding:'6px 0 0',alignItems:'center',borderTop:'1px solid var(--brd)',marginTop:2}}>
                      <span style={{fontSize:11,fontWeight:700,color:'var(--txt)'}}>Total Fees</span>
                      <span style={{fontSize:14,fontWeight:800,color:'#ef4444'}}>{f$(total)}</span>
                    </div>
                  </>;
                })() : <div style={{color:'var(--txt3)',fontSize:11,textAlign:'center',padding:'20px 0'}}>Fee data loading…</div>}
              </div>
              {(() => {
                const mtdM=periodCols?.['MTD']||{};
                const rev5=mtdM.sales||0, lyRev5=mtdM.ly_sales||0;
                const fees5=Math.abs(mtdM.amazon_fees||0), lyFees5=Math.abs(mtdM.ly_amazon_fees||0);
                const ret5=mtdM.returns_amount||0, lyRet5=mtdM.ly_returns_amount||0;
                const adS5=m.ad_spend||0, lyAd5=m.ly_ad_spend||0;
                const cm5=rev5-ret5-fees5-adS5-(rev5-ret5)*0.35;
                const lyCm5=lyRev5-lyRet5-lyFees5-lyAd5-(lyRev5-lyRet5)*0.35;
                const cmPct5=rev5>0?cm5/rev5*100:0, lyCmPct5=lyRev5>0?lyCm5/lyRev5*100:0;
                const cmChg=lyCmPct5>0?cmPct5-lyCmPct5:null;
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`2px solid ${B.t2}`,borderRadius:12,padding:16}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:10}}>Margin &amp; AUR · MTD</div>
                    <div style={{display:'flex',gap:14,alignItems:'flex-end',marginBottom:12,flexWrap:'wrap'}}>
                      <div><div style={{fontSize:9,color:'var(--txt3)',marginBottom:2}}>TY CM%</div><div style={{fontSize:26,fontWeight:800,color:B.t2,lineHeight:1}}>{cmPct5.toFixed(1)}%</div></div>
                      <div><div style={{fontSize:9,color:'var(--txt3)',marginBottom:2}}>LY CM%</div><div style={{fontSize:26,fontWeight:800,color:B.b3,lineHeight:1}}>{lyCmPct5.toFixed(1)}%</div></div>
                      {cmChg!==null && <span style={{fontSize:11,fontWeight:700,padding:'3px 8px',borderRadius:6,background:cmChg>=0?'rgba(34,197,94,.12)':'rgba(239,68,68,.12)',color:cmChg>=0?'#4ade80':'#f87171',marginBottom:4}}>{cmChg>=0?'▲':'▼'}{Math.abs(cmChg).toFixed(1)}pt</span>}
                    </div>
                    <div style={{height:1,background:'var(--brd)',marginBottom:10}}/>
                    {[
                      ['ROAS',  fX(m.roas||0),  B.t2],
                      ['TACOS', fP(m.tacos||0), (m.tacos||0)>0.16?'#f59e0b':B.t3],
                      ['AUR TY',f$(mtdM.aur||(rev5/(mtdM.units||1))), 'var(--txt)'],
                      ['AUR LY',f$(mtdM.ly_aur||(lyRev5/(mtdM.ly_units||1))), B.sub],
                    ].map(([lbl,val,col])=>(
                      <div key={lbl} style={{display:'flex',justifyContent:'space-between',padding:'5px 0',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                        <span style={{fontSize:11,color:'var(--txt3)'}}>{lbl}</span>
                        <span style={{fontSize:13,fontWeight:700,color:col}}>{val}</span>
                      </div>
                    ))}
                  </div>
                );
              })()}
            </div>
          </>
        );
      })()}


      {/* ── DAILY TAB ─────────────────────────────────────────────────────── */}
      {viewTab === 'Daily' && (() => {
        const tod = periodCols?.['Today'] || {};
        const tyS   = tod.sales  || 0;
        const tyU   = Math.max(0, tod.units  || 0);
        const tyO   = tod.orders || 0;
        const tyAur = tyU > 0 ? tyS / tyU : 0;
        const lyNowS   = tod.ly_same_time_sales  || 0;
        const lyNowU   = tod.ly_same_time_units  || 0;
        const lyNowO   = tod.ly_same_time_orders || 0;
        const lyNowAur = lyNowU > 0 ? lyNowS / lyNowU : 0;
        const tyFcstS   = tod.ty_forecast       || 0;
        const tyFcstU   = Math.max(0, tod.ty_units_forecast || 0);
        const tyFcstAur = tyFcstU > 0 ? tyFcstS / tyFcstU : 0;
        const lyEodS   = tod.ly_eod_sales  ?? tod.ly_sales  ?? 0;
        const lyEodU   = tod.ly_eod_units  ?? tod.ly_units  ?? 0;
        const lyEodAur = lyEodU > 0 ? lyEodS / lyEodU : 0;

        const dp2 = (a, b) => (a && b) ? (a - b) / b * 100 : null;
        const chgSpan = (delta, inv = false) => {
          if (delta == null) return <span style={{color:'var(--txt3)',fontSize:9}}>—</span>;
          const pos = inv ? delta < 0 : delta > 0;
          return <span style={{color: pos ? '#4ade80' : '#fb923c', fontWeight:700, fontSize:9, whiteSpace:'nowrap'}}>
            {delta > 0 ? '▲' : '▼'}{Math.abs(delta).toFixed(1)}%
          </span>;
        };
        const heroRows = (rows) => rows.map(([lbl, val]) => (
          <div key={lbl} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'4px 0',borderBottom:'1px solid rgba(26,47,74,.5)',fontSize:11}}>
            <span style={{color:'var(--txt3)'}}>{lbl}</span>
            <span style={{fontWeight:600,color:'var(--txt)'}}>{val}</span>
          </div>
        ));
        const EpochLabel = ({type, label, right}) => {
          const st = type==='today'
            ? {bg:'rgba(26,163,146,.15)',color:B.t3,border:'rgba(26,163,146,.25)'}
            : type==='trend'
            ? {bg:'rgba(232,130,30,.12)',color:B.o3,border:'rgba(232,130,30,.2)'}
            : {bg:'rgba(46,111,187,.15)',color:B.b3,border:'rgba(46,111,187,.25)'};
          return (
            <div style={{display:'flex',alignItems:'center',gap:10,margin:'14px 0 8px'}}>
              <div style={{fontSize:9,fontWeight:800,textTransform:'uppercase',letterSpacing:'.12em',padding:'3px 9px',borderRadius:99,background:st.bg,color:st.color,border:`1px solid ${st.border}`,whiteSpace:'nowrap'}}>{label}</div>
              <div style={{flex:1,height:1,background:'var(--brd)'}}/>
              {right && <span style={{fontSize:9,color:B.sub}}>{right}</span>}
            </div>
          );
        };

        const DAILY_PERIODS = ['Today','Yesterday','WTD','MTD','YTD'];
        const ACCENTS = [B.o2, 'var(--brd2)', 'var(--brd2)', B.b2, B.t2];

        return (
          <>
            {/* ── ANOMALY BANNER ── */}
            {!dailyAnomalyDismissed && periodCols?.['Today'] && (() => {
              const tod2b = periodCols['Today'];
              const tyNow = tod2b.sales || 0;
              const lyNow = tod2b.ly_same_time_sales || 0;
              if (!tyNow || !lyNow) return null;
              const pctVsLY = (tyNow - lyNow) / lyNow * 100;
              const dayName = new Date().toLocaleDateString('en-US',{weekday:'long'});
              const isBelow = pctVsLY < -8;
              const isAbove = pctVsLY > 15;
              if (!isBelow && !isAbove) return null;
              const bgCol = isBelow ? 'rgba(239,68,68,.08)' : 'rgba(34,197,94,.07)';
              const bdCol = isBelow ? 'rgba(239,68,68,.25)' : 'rgba(34,197,94,.2)';
              const txtCol = isBelow ? '#fca5a5' : '#86efac';
              const subCol = isBelow ? '#ef4444' : '#22c55e';
              const icon = isBelow ? '⚠️' : '🚀';
              const msg = isBelow
                ? `Today tracking ${Math.abs(pctVsLY).toFixed(0)}% below same time last year`
                : `Today tracking ${pctVsLY.toFixed(0)}% above same time last year`;
              const sub = isBelow
                ? `LY same-time: ${f$(lyNow)} · Current: ${f$(tyNow)} · ${tod2b.snapshot_time||''}`
                : `LY same-time: ${f$(lyNow)} · Current: ${f$(tyNow)} — strong day in progress`;
              return (
                <div style={{margin:'0 0 12px',background:bgCol,border:`1px solid ${bdCol}`,borderRadius:10,padding:'10px 14px',display:'flex',alignItems:'center',justifyContent:'space-between',gap:12}}>
                  <div style={{display:'flex',alignItems:'flex-start',gap:10,flex:1}}>
                    <span style={{fontSize:16,flexShrink:0}}>{icon}</span>
                    <div>
                      <div style={{fontSize:11,fontWeight:700,color:txtCol}}>{msg}</div>
                      <div style={{fontSize:10,color:subCol,marginTop:2}}>{sub}</div>
                    </div>
                  </div>
                  <button onClick={()=>setDailyAnomalyDismissed(true)} style={{background:'none',border:'none',color:'var(--txt3)',cursor:'pointer',fontSize:15,padding:'2px 6px',borderRadius:4,flexShrink:0,lineHeight:1}}>✕</button>
                </div>
              );
            })()}

            {/* ── TODAY SNAPSHOT ── */}
            <EpochLabel type="today" label="📍 Today Snapshot" right={tod.snapshot_time ? `thru ${tod.snapshot_time}` : 'live data'}/>
            {loading.periodCols
              ? <div style={{height:180,display:'flex',alignItems:'center',justifyContent:'center'}}><Spinner/></div>
              : (
              <div style={{display:'flex',background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:12,overflow:'hidden',marginBottom:14}}>
                {[
                  { lbl:'🟠 TY NOW', sub:`thru ${tod.snapshot_time||'today'} · ${fN(tyO)} orders`, val:f$(tyS), color:B.o2,
                    rows:[['Units', fN(tyU)], ['AUR', f$(tyAur)], ['Fees', f$(tod.amazon_fees||0)], ['Returns', f$(tod.returns_amount||0)], ['Conv %', fP(tod.conversion)]] },
                  { lbl:'🔵 LY NOW', sub:'same time last year', val:f$(lyNowS), color:B.b3,
                    rows:[['Units', fN(lyNowU)], ['AUR', f$(lyNowAur)], ['Fees', '—'], ['Returns', '—'], ['Conv %', fP(tod.ly_conversion)]] },
                  { lbl:'🟢 TY EOD FCST', sub:`projected full day · pacing ${tyFcstS > 0 && lyEodS > 0 ? ((tyFcstS/lyEodS-1)*100).toFixed(1)+'% vs LY EOD' : '—'}`, val:f$(tyFcstS), color:B.t2,
                    rows:[['Units', tyFcstU > 0 ? `~${fN(tyFcstU)}` : '—'], ['AUR', tyFcstAur > 0 ? f$(tyFcstAur) : '—'], ['Fees', '—'], ['Returns', '—'], ['Conv %', '—']] },
                  { lbl:'⬜ LY EOD ACTUAL', sub:'full day last year', val:f$(lyEodS), color:'var(--txt3)',
                    rows:[['Units', fN(lyEodU)], ['AUR', f$(lyEodAur)], ['Fees', f$(tod.ly_amazon_fees||0)], ['Returns', f$(tod.ly_returns_amount||0)], ['Conv %', fP(tod.ly_conversion)]] },
                ].map((col, i, arr) => (
                  <div key={col.lbl} style={{
                    flex:'1 1 0', padding:'14px 16px', minWidth:0,
                    borderRight: i < arr.length-1 ? '1px solid var(--brd)' : 'none',
                    background: i===0 ? 'rgba(27,79,138,.1)' : 'transparent',
                  }}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:col.color,marginBottom:4}}>{col.lbl}</div>
                    <div style={{fontSize:28,fontWeight:800,color:col.color,lineHeight:1,marginBottom:3}}>{col.val}</div>
                    <div style={{fontSize:10,color:'var(--txt3)',marginBottom:10}}>{col.sub}</div>
                    {heroRows(col.rows)}
                    {/* sparkline */}
                    {(() => {
                      const tArr = toArr(trend);
                      if (!tArr || tArr.length < 3) return null;
                      const last7 = tArr.slice(-7);
                      const isLY = col.lbl.includes('LY');
                      const vals = last7.map(d => isLY ? (d.ly_sales||0) : (d.ty_sales||0));
                      const maxV = Math.max(...vals, 1);
                      if (vals.every(v=>v===0)) return null;
                      const W=200, H=32, n=vals.length;
                      const pts=vals.map((v,idx2)=>`${(idx2/(n-1))*W},${H-(v/maxV)*(H-6)-3}`).join(' ');
                      const sparkCol=isLY?'rgba(91,159,212,.5)':col.color;
                      return (
                        <div style={{height:32,margin:'8px 0 2px',opacity:.85}}>
                          <svg width='100%' height='32' viewBox={`0 0 ${W} ${H}`} preserveAspectRatio='none' style={{display:'block'}}>
                            <polyline points={pts} fill='none' stroke={sparkCol} strokeWidth='2' strokeLinejoin='round' strokeLinecap='round'/>
                            <circle cx={W} cy={H-(vals[n-1]/maxV)*(H-6)-3} r='3' fill={sparkCol}/>
                          </svg>
                        </div>
                      );
                    })()}
                    {/* pace bar — TY NOW only: MTD vs LY pace */}
                    {col.lbl.includes('TY') && !col.lbl.includes('EOD') && periodCols?.['MTD'] && (() => {
                      const mtdD = periodCols['MTD'];
                      const nowD = new Date();
                      const daysInMo = new Date(nowD.getFullYear(), nowD.getMonth()+1, 0).getDate();
                      const dom = nowD.getDate();
                      const pctMo = Math.round((dom / daysInMo) * 100);
                      const mtdSales = mtdD.sales || 0;
                      const mtdLySales = mtdD.ly_sales || 0;
                      const projMo = dom > 0 ? (mtdSales / dom) * daysInMo : 0;
                      // Pace vs LY: if TY MTD > LY MTD same period → on pace
                      const vsLyPct = mtdLySales > 0 ? ((mtdSales - mtdLySales) / mtdLySales * 100) : null;
                      const onPace = vsLyPct == null ? null : vsLyPct >= 0;
                      const paceColor = onPace === null ? `linear-gradient(90deg,${B.t1},${B.t2})` : onPace ? `linear-gradient(90deg,${B.t1},${B.t2})` : `linear-gradient(90deg,${B.o1},${B.o2})`;
                      const paceNote = onPace === null
                        ? `${pctMo}% of month elapsed · proj ${f$(projMo)}/mo`
                        : onPace
                          ? `✓ ${vsLyPct.toFixed(1)}% vs LY MTD · proj ${f$(projMo)}/mo`
                          : `⚠ ${Math.abs(vsLyPct).toFixed(1)}% behind LY MTD · proj ${f$(projMo)}/mo`;
                      const paceNoteColor = onPace === null ? B.t3 : onPace ? B.t3 : B.o3;
                      return (
                        <div style={{marginTop:8,paddingTop:8,borderTop:'1px solid var(--brd)'}}>
                          <div style={{display:'flex',justifyContent:'space-between',fontSize:9,color:'var(--txt3)',marginBottom:4}}>
                            <span>MTD vs LY Pace</span>
                            <strong style={{color:'var(--txt)'}}>{f$(mtdSales)} / {f$(mtdLySales||projMo)}</strong>
                          </div>
                          <div style={{height:5,background:'rgba(255,255,255,.06)',borderRadius:3,overflow:'hidden'}}>
                            <div style={{height:'100%',width:`${Math.min(pctMo,100)}%`,background:paceColor,borderRadius:3}}/>
                          </div>
                          <div style={{fontSize:9,marginTop:3,color:paceNoteColor}}>{paceNote}</div>
                        </div>
                      );
                    })()}
                  </div>
                ))}
              </div>
            )}

            {/* ── PERFORMANCE TO PLAN ── */}
            {periodCols?.['MTD'] && (() => {
              const mtd = periodCols['MTD'] || {};
              const wtd = periodCols['WTD'] || {};
              const ytd = periodCols['YTD'] || {};
              const nowD = new Date();
              const dom = nowD.getDate();
              const daysInMo = new Date(nowD.getFullYear(), nowD.getMonth()+1, 0).getDate();
              const pctMo = Math.round((dom / daysInMo) * 100);
              const projMo = dom > 0 && mtd.sales > 0 ? (mtd.sales / dom) * daysInMo : 0;
              const rows3 = [
                { label: 'MTD Revenue', ty: mtd.sales, ly: mtd.ly_sales, proj: projMo, fmt: f$ },
                { label: 'MTD Units',   ty: mtd.units, ly: mtd.ly_units, proj: dom>0&&mtd.units>0?(mtd.units/dom)*daysInMo:0, fmt: fN },
                { label: 'WTD Revenue', ty: wtd.sales, ly: wtd.ly_sales, proj: null, fmt: f$ },
                { label: 'YTD Revenue', ty: ytd.sales, ly: ytd.ly_sales, proj: null, fmt: f$ },
              ];
              return (
                <div style={{background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:12,padding:'12px 14px',marginBottom:14}}>
                  <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:10}}>
                    <span style={{fontSize:11,fontWeight:700,color:'var(--txt)'}}>📋 Performance to Plan</span>
                    <span style={{fontSize:9,color:B.sub}}>Month {pctMo}% elapsed ({dom}d / {daysInMo}d)</span>
                  </div>
                  <div style={{display:'grid',gridTemplateColumns:'repeat(4,1fr)',gap:8}}>
                    {rows3.map(r => {
                      const chg = (r.ly||0) > 0 ? ((r.ty||0)-(r.ly||0))/(r.ly) : null;
                      const onPace = chg == null ? null : chg >= 0;
                      return (
                        <div key={r.label} style={{background:'var(--surf)',borderRadius:8,padding:'8px 10px',border:`1px solid ${onPace===false?B.o1+'44':'var(--brd)'}`}}>
                          <div style={{fontSize:9,color:'var(--txt3)',marginBottom:3}}>{r.label}</div>
                          <div style={{fontSize:15,fontWeight:800,color:'var(--txt)',lineHeight:1,marginBottom:2}}>{r.ty != null ? r.fmt(r.ty) : '—'}</div>
                          <div style={{fontSize:9,color:'var(--txt3)'}}>LY: <span style={{color:'var(--txt2)'}}>{r.ly != null ? r.fmt(r.ly) : '—'}</span>
                            {chg != null && <span style={{marginLeft:5,fontWeight:700,color:onPace?'#4ade80':'#fb923c'}}>{chg>=0?'▲':'▼'}{Math.abs(chg*100).toFixed(1)}%</span>}
                          </div>
                          {r.proj > 0 && <div style={{fontSize:9,marginTop:3,color:B.sub}}>Proj EOM: <span style={{fontWeight:600,color:B.b3}}>{r.fmt(r.proj)}</span></div>}
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })()}

            {/* ── PERIOD SUMMARY ── */}
            <EpochLabel type="period" label="📅 Period Summary" right="5 comparison windows"/>
            {periodCols && (
              <div style={{display:'grid',gridTemplateColumns:'repeat(5,1fr)',gap:10,marginBottom:20}}>
                {DAILY_PERIODS.map((lbl, idx) => {
                  const d = periodCols[lbl] || {};
                  const isToday = lbl === 'Today';
                  const rows2 = [
                    { k:'Sales $', ty: isToday ? tyS   : d.sales,         ly: isToday ? lyNowS   : d.ly_sales,   fmt:f$,  inv:false },
                    { k:'Units',   ty: isToday ? tyU   : d.units,          ly: isToday ? lyNowU   : d.ly_units,   fmt:fN,  inv:false },
                    { k:'AUR',     ty: isToday ? tyAur : d.aur,            ly: isToday ? lyNowAur : d.ly_aur,     fmt:f$,  inv:false },
                    { k:'Orders',  ty: isToday ? tyO   : d.orders,         ly: isToday ? lyNowO   : d.ly_orders,  fmt:fN,  inv:false },
                    { k:'Fees',    ty: d.amazon_fees,                       ly: d.ly_amazon_fees,                  fmt:f$,  inv:true  },
                    { k:'Returns', ty: d.returns_amount||0,                 ly: d.ly_returns_amount||0,            fmt:f$,  inv:true  },
                    { k:'Conv %',  ty: d.conversion,                        ly: isToday ? tod.ly_conversion : d.ly_conversion, fmt:fP, inv:false },
                  ];
                  const isExpPeriod = !!expandedPeriods[lbl];
                  const alwaysRows2 = rows2.slice(0,3);
                  const extraRows2  = rows2.slice(3);
                  const pRow2 = (r) => (
                    <div key={r.k} style={{display:'grid',gridTemplateColumns:'1.1fr 1fr 1fr 0.6fr',gap:2,alignItems:'center',padding:'3px 0',borderBottom:'1px solid rgba(26,47,74,.4)',fontSize:11}}>
                      <span style={{fontSize:10,color:'var(--txt3)',overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{r.k}</span>
                      <span style={{fontWeight:600,color:'var(--txt)'}}>{r.ty != null ? r.fmt(r.ty) : '—'}</span>
                      <span style={{color:'var(--txt3)',fontSize:10}}>{r.ly != null ? r.fmt(r.ly) : '—'}</span>
                      {chgSpan(dp2(r.ty, r.ly), r.inv)}
                    </div>
                  );
                  return (
                    <div key={lbl} style={{background:'var(--card2)',border:'1px solid var(--brd)',borderTop:`2px solid ${ACCENTS[idx]}`,borderRadius:10,padding:'10px 12px'}}>
                      <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:ACCENTS[idx],paddingBottom:5,marginBottom:6,borderBottom:'1px solid var(--brd)',display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                        <span>{lbl}</span>
                        {isToday && tod.snapshot_time && <span style={{fontSize:8,color:'var(--txt3)',fontWeight:400,textTransform:'none'}}>thru {tod.snapshot_time}</span>}
                      </div>
                      {alwaysRows2.map(pRow2)}
                      {isExpPeriod && extraRows2.map(pRow2)}
                      <button onClick={()=>setExpandedPeriods(p=>({...p,[lbl]:!p[lbl]}))}
                        style={{fontSize:9,color:'var(--b3)',background:'none',border:'none',cursor:'pointer',padding:'4px 0 0',width:'100%',textAlign:'left'}}>
                        {isExpPeriod ? '− Show less' : `+ ${extraRows2.length} more rows`}
                      </button>
                    </div>
                  );
                })}
              </div>
            )}

            {/* ── Heatmap Charts Toggle ── */}
            <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:showDailyCharts?10:14,padding:'8px 12px',
              background:showDailyCharts?'rgba(30,58,92,.4)':'var(--card)',
              border:`1px solid ${showDailyCharts?B.b2+'44':'var(--brd)'}`,borderRadius:9,cursor:'pointer'}}
              onClick={()=>setShowDailyCharts(v=>!v)}>
              <div style={{display:'flex',alignItems:'center',gap:8}}>
                <span style={{fontSize:14}}>{showDailyCharts ? '📉' : '📊'}</span>
                <span style={{fontSize:11,fontWeight:700,color:showDailyCharts?B.b3:'var(--txt2)'}}>Heatmap Charts</span>
                <span style={{fontSize:9,color:B.sub}}>Hourly · 26-Week Daily</span>
              </div>
              <span style={{fontSize:11,color:showDailyCharts?B.b3:B.sub,fontWeight:700,userSelect:'none'}}>
                {showDailyCharts ? '▲ Hide' : '▼ Show'}
              </span>
            </div>

            {showDailyCharts && <>
              {(()=>{
                const hmStops=[[21,37,62],[15,82,115],[13,115,119],[34,139,34],[218,165,32],[230,101,30],[214,40,57]];
                const hmColor=(t)=>{if(t<=0)return 'rgb(21,37,62)';if(t>=1)return 'rgb(214,40,57)';const seg=t*(hmStops.length-1);const i=Math.floor(seg),f=seg-i;const a=hmStops[i],b2=hmStops[Math.min(i+1,hmStops.length-1)];return `rgb(${Math.round(a[0]+(b2[0]-a[0])*f)},${Math.round(a[1]+(b2[1]-a[1])*f)},${Math.round(a[2]+(b2[2]-a[2])*f)})`};
                const HOUR_LABELS=['12am','1am','2am','3am','4am','5am','6am','7am','8am','9am','10am','11am','12pm','1pm','2pm','3pm','4pm','5pm','6pm','7pm','8pm','9pm','10pm','11pm'];
                const HCOL_W=26,DLW=72,DRH=28;
                const hmDays=hmData?.days||[];
                const maxVal=hmMetric==='$'?(hmData?.maxSales||0):(hmData?.maxUnits||0);
                const sqrtMax=Math.sqrt(maxVal||1);
                const visHours=Array.from({length:24},(_,i)=>i).filter(h=>!(hideNight&&h<6)).filter(h=>!(hideLate&&h>=18));
                const fCell=(v)=>{if(v==null||v===0)return '';if(hmMetric==='$'){if(v>=1000)return `$${(v/1000).toFixed(1)}k`;return `$${Math.round(v)}`;}if(v>=1000)return `${(v/1000).toFixed(1)}k`;return String(v)};
                const pillBtn=(label,active,onClick2)=>(<button key={label} onClick={e=>{e.stopPropagation();onClick2();}} style={{padding:'3px 10px',borderRadius:6,fontSize:10,fontWeight:600,cursor:'pointer',transition:'all .15s',border:`1px solid ${active?B.b2:'var(--brd)'}`,background:active?`${B.b1}33`:'transparent',color:active?B.b3:'var(--txt3)'}}>{label}</button>);
                const last7=hmDays.slice(-7);
                return (
                  <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:'14px 16px',marginBottom:10}}>
                    <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:10,flexWrap:'wrap',gap:8}}>
                      <span style={{fontSize:13,fontWeight:700,color:'var(--txt)'}}>Hourly Sales — Last 7 Days</span>
                      <div style={{display:'flex',gap:5,alignItems:'center',flexWrap:'wrap'}}>
                        {pillBtn('$',hmMetric==='$',()=>setHmMetric('$'))}
                        {pillBtn('Units',hmMetric==='units',()=>setHmMetric('units'))}
                        <div style={{width:1,height:16,background:'var(--brd)',margin:'0 2px'}}/>
                        {pillBtn(hideNight?'Show 12–5am':'Hide 12–5am',hideNight,()=>setHideNight(v=>!v))}
                        {pillBtn(hideLate?'Show 6–11pm':'Hide 6–11pm',hideLate,()=>setHideLate(v=>!v))}
                      </div>
                    </div>
                    {loading.hmData?<Spinner/>:last7.length===0?(
                      <div style={{padding:'24px',textAlign:'center',color:B.sub,fontSize:12}}>No hourly data yet</div>
                    ):(
                      <div style={{overflowX:'auto'}}>
                        {last7.map(d=>(
                          <div key={d.date} style={{display:'flex',alignItems:'center',marginBottom:2}}>
                            <div style={{width:DLW,flexShrink:0,paddingRight:8,textAlign:'right'}}>
                              <div style={{fontSize:9,fontWeight:d.isToday?700:500,color:d.isToday?B.b3:'var(--txt2)',lineHeight:'1.2',whiteSpace:'nowrap'}}>{d.dayOfWeek}</div>
                              <div style={{fontSize:8,color:d.isToday?B.b2:B.sub,lineHeight:'1.2',whiteSpace:'nowrap'}}>{d.label}</div>
                            </div>
                            {visHours.map(h=>{
                              const cell=(d.hours||{})[h]||{};
                              const val=hmMetric==='$'?cell.sales:cell.units;
                              const isFuture=(val===null||val===undefined);
                              const numVal=isFuture?0:(Number(val)||0);
                              const pct=(numVal>0)?Math.sqrt(numVal)/sqrtMax:0;
                              const bgColor=isFuture?'rgb(21,37,62)':(numVal>0?hmColor(pct):'rgb(21,37,62)');
                              const opacity=isFuture?0.06:(numVal>0?0.55+pct*0.45:0.16);
                              const txtColor=pct>0.5?'#ffffff':'#7a9bbf';
                              return (<div key={h} title={isFuture?`${d.label} ${HOUR_LABELS[h]}: future`:`${d.label} ${HOUR_LABELS[h]}: ${hmMetric==='$'?f$(numVal):`${numVal} units`}`} style={{width:HCOL_W-2,height:DRH,marginRight:2,background:bgColor,opacity,borderRadius:3,flexShrink:0,display:'flex',alignItems:'center',justifyContent:'center',fontSize:7,color:txtColor,fontWeight:600,overflow:'hidden',cursor:'default',border:d.isToday?`1px solid ${B.b2}44`:'none'}}>{!isFuture&&numVal>0&&fCell(numVal)}</div>);
                            })}
                          </div>
                        ))}
                        <div style={{display:'flex',marginLeft:DLW,marginTop:4}}>
                          {visHours.map(h=>(<div key={h} style={{width:HCOL_W-2,marginRight:2,flexShrink:0,textAlign:'center',fontSize:7,color:B.sub,lineHeight:'1.2'}}>{HOUR_LABELS[h]}</div>))}
                        </div>
                        <div style={{display:'flex',justifyContent:'space-between',marginTop:8,marginLeft:DLW}}>
                          <div style={{display:'flex',alignItems:'center',gap:3}}>
                            <span style={{fontSize:8,color:B.sub}}>Low</span>
                            {[0,.17,.33,.5,.67,.83,1].map(t=>(<div key={t} style={{width:10,height:10,borderRadius:2,background:t===0?'rgb(21,37,62)':hmColor(t),opacity:0.55+t*0.45}}/>))}
                            <span style={{fontSize:8,color:B.sub}}>High</span>
                          </div>
                          <span style={{fontSize:8,color:B.sub}}>{hmData?.lastUpdated?hmData.lastUpdated.replace('T',' '):'—'}</span>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })()}

            {/* ── Weekly 26-Week Heatmap (Units/Sales/Returns × Day of Week) ── */}
            {(()=>{
              // Toggle a window in/out; enforce max 2 active
              const toggleWindow = (w) => {
                setHmWindows(prev => {
                  if (prev.includes(w)) {
                    return prev.length > 1 ? prev.filter(x => x !== w) : prev;
                  }
                  if (prev.length >= 2) return [prev[1], w]; // drop oldest, add new
                  return [...prev, w];
                });
              };
              const pillBtn2 = (key, label, range) => {
                const active = hmWindows.includes(key);
                const isFuture = range === 'future';
                return (
                  <button key={key} onClick={() => toggleWindow(key)} style={{
                    padding:'3px 10px', borderRadius:6, fontSize:10, fontWeight:600, cursor:'pointer', transition:'all .15s',
                    border:`1px solid ${active ? (isFuture ? '#22c55e' : B.b2) : 'var(--brd)'}`,
                    background: active ? (isFuture ? 'rgba(34,197,94,.12)' : `${B.b1}33`) : 'transparent',
                    color: active ? (isFuture ? '#4ade80' : B.b3) : 'var(--txt3)',
                  }}>
                    {isFuture ? '🌿 ' : ''}{label}
                  </button>
                );
              };
              // Build filtered + remapped heatmap data for selected windows
              const allSelIdx = hmWindows.flatMap(w => HM_WINDOWS_DEF[w].indices);
              const sortedIdx = [...allSelIdx].sort((a,b) => a-b); // ascending = oldest index first
              const indexMap  = Object.fromEntries(sortedIdx.map((idx, pos) => [idx, pos]));
              const filteredHm = toArr(heatmap)
                .filter(d => allSelIdx.includes(d.week))
                .map(d => ({...d, week: indexMap[d.week]}));
              const totalWks = sortedIdx.length || 26;

              // Compute which SVG columns are "future" (LY proxy) — SVG col w shows position TOTAL-1-w
              const futureOrigIdx = new Set(['C','D'].flatMap(w => hmWindows.includes(w) ? HM_WINDOWS_DEF[w].indices : []));
              const futureSvgCols = new Set(
                sortedIdx.map((idx, pos) => futureOrigIdx.has(idx) ? (totalWks-1-pos) : -1).filter(p => p >= 0)
              );

              // Compute week-start dates for each SVG column (0=leftmost)
              // week=N in backend means N weeks ago from today (Mon–Sun)
              const todayJs = new Date();
              const dow = todayJs.getDay(); // 0=Sun
              const daysToMon = dow === 0 ? 6 : dow - 1;
              const thisMonday = new Date(todayJs);
              thisMonday.setDate(todayJs.getDate() - daysToMon);
              const weekStartDates = Array.from({length:totalWks}, (_, w) => {
                const pos = totalWks - 1 - w;           // position in sorted array (oldest first)
                if (pos < 0 || pos >= sortedIdx.length) return '';
                const weeksAgo = sortedIdx[pos];         // original backend index = weeks ago
                const d = new Date(thisMonday);
                d.setDate(thisMonday.getDate() - weeksAgo * 7);
                return `${MONTHS[d.getMonth()]} ${d.getDate()}`;
              });

              // ── Day-of-week averages (recalculated for currently shown data) ──
              const DAY_NAMES = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
              // Per-window breakdowns for comparison
              const winProfiles = hmWindows.map(wKey => {
                const wIdxSet = new Set(HM_WINDOWS_DEF[wKey].indices);
                const dayAvgs = DAY_NAMES.map((_, d) => {
                  const rows = toArr(heatmap).filter(r => wIdxSet.has(r.week) && r.day === d);
                  const vals = rows.map(r => r[hmMetric2] || 0);
                  return vals.length ? vals.reduce((s,v)=>s+v,0)/vals.length : 0;
                });
                return { key: wKey, label: HM_WINDOWS_DEF[wKey].label, range: HM_WINDOWS_DEF[wKey].range, dayAvgs };
              });
              // Combined across all selected data
              const combinedDayAvgs = DAY_NAMES.map((_, d) => {
                const vals = filteredHm.filter(r => r.day === d).map(r => r[hmMetric2] || 0);
                return vals.length ? vals.reduce((s,v)=>s+v,0)/vals.length : 0;
              });
              const combinedMax = Math.max(...combinedDayAvgs, 1);
              const overallAvg = combinedDayAvgs.reduce((s,v)=>s+v,0) / 7;
              // Rank days by combined avg
              const dayRanks = combinedDayAvgs.map((v, d) => ({d, v})).sort((a,b)=>b.v-a.v);
              const bestDay = dayRanks[0];
              const worstDay = dayRanks[6];
              const weekendAvg = ((combinedDayAvgs[5]||0)+(combinedDayAvgs[6]||0))/2;
              const weekdayAvg = combinedDayAvgs.slice(0,5).reduce((s,v)=>s+v,0)/5;

              const metricFmt = hmMetric2==='sales' ? f$ : fN;
              const metricLabel = hmMetric2==='units' ? 'Units' : hmMetric2==='sales' ? 'Sales $' : 'Returns';
              const windowLabel = hmWindows.map(w => HM_WINDOWS_DEF[w].label).join(' + ');

              return (
                <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:16,marginBottom:12,transition:'background .3s'}}>
                  {/* Controls row */}
                  <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:8,flexWrap:'wrap',gap:8}}>
                    <span style={{fontSize:13,fontWeight:700,color:'var(--txt)'}}>
                      {metricLabel} — {windowLabel} × Day of Week
                    </span>
                    <div style={{display:'flex',gap:4,alignItems:'center',flexWrap:'wrap'}}>
                      {['D','C','A','B'].map(w => pillBtn2(w, HM_WINDOWS_DEF[w].label, HM_WINDOWS_DEF[w].range))}
                      <div style={{width:1,height:16,background:'var(--brd)',margin:'0 4px'}}/>
                      {[['Units','units'],['Sales $','sales'],['Returns','returns']].map(([lbl,key]) => (
                        <button key={key} onClick={() => setHmMetric2(key)} style={{
                          padding:'3px 10px',borderRadius:6,fontSize:10,fontWeight:600,cursor:'pointer',transition:'all .15s',
                          border:`1px solid ${hmMetric2===key ? B.b2 : 'var(--brd)'}`,
                          background: hmMetric2===key ? `${B.b1}33` : 'transparent',
                          color: hmMetric2===key ? B.b3 : 'var(--txt3)',
                        }}>{lbl}</button>
                      ))}
                    </div>
                  </div>

                  {errors.heatmap && <div style={{padding:'10px 14px',color:'#fb923c',fontSize:11,background:'rgba(251,146,60,.08)',border:'1px solid rgba(251,146,60,.18)',borderRadius:8,marginBottom:8}}>⚠ {errors.heatmap}</div>}
                  {loading.heatmap ? <Spinner/> : svgChart(heatmapSVG(filteredHm, 1100, totalWks, hmMetric2, futureSvgCols, weekStartDates))}

                  {/* ── Day-of-Week Averages panel ── */}
                  {!loading.heatmap && filteredHm.length > 0 && (
                    <div style={{marginTop:12,padding:'12px 14px',background:'var(--card)',borderRadius:10,border:'1px solid var(--brd)'}}>
                      <div style={{display:'flex',justifyContent:'space-between',alignItems:'flex-start',gap:16,flexWrap:'wrap'}}>
                        {/* Left: bar chart of day averages */}
                        <div style={{flex:'1 1 420px'}}>
                          <div style={{fontSize:10,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.08em',marginBottom:10}}>
                            Day Strength — Avg {metricLabel} per Week
                          </div>
                          <div style={{display:'flex',flexDirection:'column',gap:6}}>
                            {dayRanks.map(({d, v}, rank) => {
                              const barPct = Math.min(100, combinedMax > 0 ? (v/combinedMax)*100 : 0);
                              const vsAvg = overallAvg > 0 ? ((v-overallAvg)/overallAvg*100) : 0;
                              const isBest = rank === 0;
                              const isWeakest = rank === 6;
                              const isWeekend = d >= 5;
                              const barColor = isBest ? B.o2 : isWeakest ? '#475569' : isWeekend ? B.t2 : B.b2;
                              const rankEmoji = isBest ? '🥇' : rank === 1 ? '🥈' : rank === 2 ? '🥉' : null;
                              const w0avg = winProfiles[0]?.dayAvgs[d] ?? 0;
                              const w1avg = winProfiles[1]?.dayAvgs[d] ?? null;
                              const w0pct = Math.min(100, combinedMax > 0 ? (w0avg/combinedMax)*100 : 0);
                              const w1pct = Math.min(100, w1avg != null && combinedMax > 0 ? (w1avg/combinedMax)*100 : 0);
                              return (
                                <div key={d} style={{display:'grid',gridTemplateColumns:'52px 1fr 76px 70px',gap:8,alignItems:'center',padding:'3px 0'}}>
                                  {/* Day label + medal */}
                                  <div style={{display:'flex',alignItems:'center',gap:5}}>
                                    {rankEmoji
                                      ? <span style={{fontSize:16,lineHeight:1,flexShrink:0}}>{rankEmoji}</span>
                                      : <span style={{
                                          width:18,height:18,borderRadius:'50%',flexShrink:0,
                                          display:'inline-flex',alignItems:'center',justifyContent:'center',
                                          fontSize:9,fontWeight:700,
                                          background: isWeakest ? 'rgba(71,85,105,.25)' : 'rgba(91,159,212,.12)',
                                          color: isWeakest ? '#64748b' : B.sub,
                                        }}>{rank+1}</span>
                                    }
                                    <span style={{fontSize:13,fontWeight:700,color: isWeekend ? B.t3 : isBest ? B.o2 : 'var(--txt)'}}>{DAY_NAMES[d]}</span>
                                  </div>
                                  {/* Bar track */}
                                  <div style={{position:'relative',height:22,borderRadius:5,background:'rgba(255,255,255,.04)'}}>
                                    {winProfiles.length === 2 ? (
                                      <>
                                        <div style={{position:'absolute',left:0,top:2,height:8,borderRadius:3,width:`${w0pct.toFixed(1)}%`,background:winProfiles[0].range==='future'?'#22c55e':B.b2,opacity:.9}}/>
                                        <div style={{position:'absolute',left:0,top:12,height:8,borderRadius:3,width:`${w1pct.toFixed(1)}%`,background:winProfiles[1].range==='future'?'#22c55e':B.b3,opacity:.8}}/>
                                      </>
                                    ) : (
                                      <div style={{position:'absolute',left:0,top:3,height:16,borderRadius:4,width:`${barPct.toFixed(1)}%`,background:barColor,opacity:.85,transition:'width .3s'}}/>
                                    )}
                                  </div>
                                  {/* Value */}
                                  <div style={{fontSize:13,fontWeight:700,color: isBest ? B.o2 : 'var(--txt)',textAlign:'right'}}>{metricFmt(v)}</div>
                                  {/* vs avg */}
                                  <div style={{
                                    fontSize:11,textAlign:'right',fontWeight: Math.abs(vsAvg) > 10 ? 700 : 500,
                                    color: vsAvg > 10 ? '#4ade80' : vsAvg < -10 ? '#fb923c' : 'var(--txt3)',
                                  }}>
                                    {vsAvg >= 0 ? '+' : ''}{vsAvg.toFixed(0)}% avg
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                          {/* Legend for 2-window comparison */}
                          {winProfiles.length === 2 && (
                            <div style={{display:'flex',gap:14,marginTop:10,flexWrap:'wrap'}}>
                              {winProfiles.map((wp, i) => (
                                <div key={wp.key} style={{display:'flex',alignItems:'center',gap:5,fontSize:9,color:'var(--txt3)'}}>
                                  <div style={{width:16,height:5,borderRadius:2,background:wp.range==='future'?'#22c55e':(i===0?B.b2:B.b3)}}/>
                                  {wp.label}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>

                        {/* Right: insight callouts */}
                        <div style={{flex:'0 0 220px',display:'flex',flexDirection:'column',gap:8}}>
                          <div style={{fontSize:10,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.08em',marginBottom:2}}>
                            Key Insights
                          </div>
                          <div style={{background:'rgba(234,179,8,.08)',border:'1px solid rgba(234,179,8,.2)',borderRadius:8,padding:'8px 10px'}}>
                            <div style={{fontSize:9,color:'#fbbf24',fontWeight:700,textTransform:'uppercase',marginBottom:3}}>🏆 Best Day</div>
                            <div style={{fontSize:14,fontWeight:800,color:'var(--txt)'}}>{DAY_NAMES[bestDay.d]}</div>
                            <div style={{fontSize:10,color:'var(--txt2)'}}>{metricFmt(bestDay.v)} avg · {bestDay.v>0&&overallAvg>0 ? `${((bestDay.v/overallAvg-1)*100).toFixed(0)}% above avg` : ''}</div>
                          </div>
                          <div style={{background:'rgba(100,116,139,.08)',border:'1px solid rgba(100,116,139,.2)',borderRadius:8,padding:'8px 10px'}}>
                            <div style={{fontSize:9,color:'var(--txt3)',fontWeight:700,textTransform:'uppercase',marginBottom:3}}>Wknd vs Wkday</div>
                            <div style={{display:'flex',gap:12,alignItems:'baseline'}}>
                              <div>
                                <div style={{fontSize:10,color:B.t3,fontWeight:700}}>Sa–Su</div>
                                <div style={{fontSize:13,fontWeight:800,color:'var(--txt)'}}>{metricFmt(weekendAvg)}</div>
                              </div>
                              <div style={{fontSize:11,color:'var(--txt3)'}}>vs</div>
                              <div>
                                <div style={{fontSize:10,color:B.b3,fontWeight:700}}>M–F</div>
                                <div style={{fontSize:13,fontWeight:800,color:'var(--txt)'}}>{metricFmt(weekdayAvg)}</div>
                              </div>
                            </div>
                            {weekdayAvg > 0 && <div style={{fontSize:9,color:'var(--txt3)',marginTop:4}}>
                              Weekend is {weekendAvg > weekdayAvg
                                ? <span style={{color:'#4ade80',fontWeight:700}}>{((weekendAvg/weekdayAvg-1)*100).toFixed(0)}% stronger</span>
                                : <span style={{color:'#fb923c',fontWeight:700}}>{((weekdayAvg/weekendAvg-1)*100).toFixed(0)}% weaker</span>} than weekdays
                            </div>}
                          </div>
                          <div style={{background:'rgba(59,130,246,.06)',border:'1px solid rgba(59,130,246,.15)',borderRadius:8,padding:'8px 10px'}}>
                            <div style={{fontSize:9,color:B.b3,fontWeight:700,textTransform:'uppercase',marginBottom:4}}>Day Spread</div>
                            <div style={{display:'flex',gap:6,flexWrap:'wrap'}}>
                              {dayRanks.slice(0,3).map(({d},i) => (
                                <span key={d} style={{fontSize:9,padding:'2px 6px',borderRadius:99,
                                  background: i===0?`${B.o2}22`:i===1?`${B.b2}22`:`${B.t2}22`,
                                  color: i===0?B.o2:i===1?B.b3:B.t3,fontWeight:700}}>
                                  #{i+1} {DAY_NAMES[d]}
                                </span>
                              ))}
                            </div>
                            <div style={{fontSize:9,color:'var(--txt3)',marginTop:5}}>
                              Weakest: <span style={{color:'#fb923c',fontWeight:700}}>{DAY_NAMES[worstDay.d]}</span> ({metricFmt(worstDay.v)})
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              );
            })()}
</>}

            {/* ── TRENDS ── */}
            <EpochLabel type="trend" label="📈 Trends" right="Period-controlled"/>

            {/* ── Period selector (card-wrapped, matches period-bar-wrap) ── */}
            <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',gap:8,padding:'10px 14px',background:'var(--card)',border:'1px solid var(--brd)',borderRadius:9,marginBottom:12}}>
              <div style={{display:'flex',alignItems:'center',gap:6,flexWrap:'wrap'}}>
                <span style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',whiteSpace:'nowrap'}}>Period</span>
                {EXEC_PERIODS_LIST.map(p => (
                  <button key={p} onClick={() => { const cp = EXEC_PERIOD_MAP[p]; setCpSales(cp); setActivePeriod(cp); }} style={{
                    fontSize:10,fontWeight:700,padding:'4px 10px',borderRadius:5,cursor:'pointer',
                    border:`1px solid ${cpSales===EXEC_PERIOD_MAP[p] ? B.b2 : 'var(--brd)'}`,
                    background: cpSales===EXEC_PERIOD_MAP[p] ? 'rgba(46,111,187,.15)' : 'transparent',
                    color: cpSales===EXEC_PERIOD_MAP[p] ? B.b3 : 'var(--txt3)',
                    transition:'all .15s',
                  }}>{p}</button>
                ))}
              </div>
              {/* date range badge */}
              {(() => {
                const now2 = new Date();
                const activeP2 = EXEC_PERIODS_LIST.find(p => EXEC_PERIOD_MAP[p] === cpSales) || 'L30';
                const days2 = parseInt(activeP2.replace('L','')) || 30;
                const end2 = new Date(now2); end2.setDate(end2.getDate()-1);
                const start2 = new Date(end2); start2.setDate(start2.getDate()-(days2-1));
                const fmt2 = d => d.toLocaleDateString('en-US',{month:'short',day:'numeric'});
                return (
                  <span style={{fontSize:10,color:B.b3,background:'rgba(46,111,187,.1)',border:'1px solid rgba(46,111,187,.2)',borderRadius:6,padding:'4px 10px',fontWeight:600,whiteSpace:'nowrap',flexShrink:0}}>
                    {fmt2(start2)} – {fmt2(end2)}, {end2.getFullYear()} &nbsp;·&nbsp; {days2} days
                  </span>
                );
              })()}
            </div>

            {/* ── Sales Overview KPIs ── */}
            {(() => {
              // Compute projected EOM from MTD data for goal context on each card
              const mtdKpi = periodCols?.['MTD'] || {};
              const nowKpi = new Date();
              const domKpi = nowKpi.getDate();
              const daysInMoKpi = new Date(nowKpi.getFullYear(), nowKpi.getMonth()+1, 0).getDate();
              const projSales = domKpi > 0 && mtdKpi.sales  > 0 ? f$((mtdKpi.sales  / domKpi) * daysInMoKpi) : null;
              const projUnits = domKpi > 0 && mtdKpi.units  > 0 ? fN((mtdKpi.units  / domKpi) * daysInMoKpi) : null;
              const estBadge = <span style={{fontSize:8,background:'rgba(245,158,11,.15)',color:B.o3,border:'1px solid rgba(245,158,11,.2)',borderRadius:3,padding:'1px 4px',marginLeft:4,verticalAlign:'middle'}}>⚠ Est.</span>;
              const retRatePct = (mtdKpi.sales||0) > 0 ? ((mtdKpi.returns_amount||0)/(mtdKpi.sales||1)*100).toFixed(1) : null;
              const feesPct = (mtdKpi.sales||0) > 0 ? ((Math.abs(mtdKpi.amazon_fees||0))/(mtdKpi.sales||1)*100).toFixed(1) : null;
              return (
                <div style={{display:'flex',gap:8,marginBottom:10,overflowX:'auto',paddingBottom:2}}>
                  {loading.metrics ? <Spinner/> : <>
                    <MetricCard label="Sales $"       value={f$(m.sales)}         ly={f$(ly('sales'))}         delta={dp(m.sales,ly('sales'))}         goal={projSales}  goalLabel="Proj EOM:"/>
                    <MetricCard label="Unit Sales"    value={fN(m.unit_sales)}     ly={fN(ly('unit_sales'))}    delta={dp(m.unit_sales,ly('unit_sales'))} goal={projUnits}  goalLabel="Proj EOM:"/>
                    <MetricCard label="AUR"           value={f$(m.aur)}            ly={f$(ly('aur'))}           delta={dp(m.aur,ly('aur'))}/>
                    <MetricCard label={<>COGS{estBadge}</>}          value={f$(m.cogs)}           ly={f$(ly('cogs'))}          delta={dp(m.cogs,ly('cogs'))}          goal="35% fallback" goalLabel="Rate:"/>
                    <MetricCard label="Amazon Fees"   value={f$(m.amazon_fees)}    ly={f$(ly('amazon_fees'))}   delta={dp(m.amazon_fees,ly('amazon_fees'))} goal={feesPct ? `${feesPct}% of rev` : null} goalLabel=""/>
                    <MetricCard label="Returns" value={`${fN(m.returns)} · ${f$(m.returns_amount)}`} ly={`${fN(ly('returns'))} · ${f$(ly('returns_amount'))}`} delta={dp(m.returns,ly('returns'))} invert goal={retRatePct ? `${retRatePct}%` : null} goalLabel="Return rate:"/>
                    <MetricCard label={<>Gross Margin ${estBadge}</>}  value={f$(m.gross_margin)}     ly={f$(ly('gross_margin'))}     delta={dp(m.gross_margin,ly('gross_margin'))}/>
                    <MetricCard label={<>GM %{estBadge}</>}            value={fP(m.gross_margin_pct)} ly={fP(ly('gross_margin_pct'))} delta={dp(m.gross_margin_pct,ly('gross_margin_pct'))} goal="35% COGS est." goalLabel="Based on:"/>
                  </>}
                </div>
              );
            })()}

            {/* ── Revenue & AUR Trend + Units ── */}
            {(() => {
              const effTrend  = trendChart || trend;
              const effPeriod = cpTrendChart || cpSales;

              // ── Compact period pills (shared by both charts) ──
              const pSelector = (
                <div style={{display:'flex',alignItems:'center',gap:2}}>
                  {EXEC_PERIODS_LIST.map(p => {
                    const pv = EXEC_PERIOD_MAP[p];
                    const isAct = effPeriod === pv;
                    return (
                      <button key={p} onClick={()=>setCpTrendChart(pv)} style={{
                        fontSize:8,fontWeight:700,padding:'1px 5px',borderRadius:3,cursor:'pointer',
                        transition:'all .12s',
                        border:`1px solid ${isAct?B.b2:'var(--brd)'}`,
                        background:isAct?`${B.b2}22`:'transparent',
                        color:isAct?B.b2:'var(--txt3)',
                      }}>{p}</button>
                    );
                  })}
                  {cpTrendChart && cpTrendChart !== cpSales && (
                    <button onClick={()=>setCpTrendChart(null)} title="Sync to global period" style={{
                      fontSize:8,fontWeight:600,padding:'1px 5px',borderRadius:3,cursor:'pointer',
                      border:'1px solid var(--brd)',background:'transparent',color:'var(--txt3)',
                    }}>↩</button>
                  )}
                </div>
              );

              // ── Revenue chart: metric toggle (Revenue/Conversion) + period pills ──
              const revRight = (
                <div style={{display:'flex',alignItems:'center',gap:4}}>
                  <div style={{display:'flex',alignItems:'center',gap:2}}>
                    {[['Revenue','revenue'],['Conversion','conversion']].map(([lbl,key]) => (
                      <button key={key} onClick={()=>setRevChartMetric(key)} style={{
                        fontSize:8,fontWeight:700,padding:'1px 5px',borderRadius:3,cursor:'pointer',
                        transition:'all .12s',
                        border:`1px solid ${revChartMetric===key?(key==='conversion'?'#f97316':B.b2):'var(--brd)'}`,
                        background:revChartMetric===key?(key==='conversion'?'rgba(249,115,22,.15)':`${B.b2}22`):'transparent',
                        color:revChartMetric===key?(key==='conversion'?'#f97316':B.b2):'var(--txt3)',
                      }}>{lbl}</button>
                    ))}
                  </div>
                  <div style={{width:1,height:12,background:'var(--brd)'}}/>
                  {pSelector}
                </div>
              );

              // ── Metric toggle + period pills for Units chart ──
              const unitsRight = (
                <div style={{display:'flex',alignItems:'center',gap:4}}>
                  <div style={{display:'flex',alignItems:'center',gap:2}}>
                    {[['Units','units'],['$ Sold','sales'],['Return U','returns']].map(([lbl,key]) => (
                      <button key={key} onClick={()=>setUnitsChartMetric(key)} style={{
                        fontSize:8,fontWeight:700,padding:'1px 5px',borderRadius:3,cursor:'pointer',
                        transition:'all .12s',
                        border:`1px solid ${unitsChartMetric===key?B.b2:'var(--brd)'}`,
                        background:unitsChartMetric===key?`${B.b2}22`:'transparent',
                        color:unitsChartMetric===key?B.b2:'var(--txt3)',
                      }}>{lbl}</button>
                    ))}
                  </div>
                  <div style={{width:1,height:12,background:'var(--brd)'}}/>
                  {pSelector}
                </div>
              );

              // ── Shared bar chart renderer (H adapts to layout) ──
              const unitsBars = (H) => {
                const tArr = toArr(effTrend);
                if (!tArr || tArr.length < 2) return <div style={{color:'var(--txt3)',padding:20,textAlign:'center',fontSize:12}}>No data</div>;
                const isRet   = unitsChartMetric === 'returns';
                const isSales = unitsChartMetric === 'sales';
                const tyK     = isSales ? 'ty_sales'  : isRet ? 'returns'    : 'ty_units';
                const lyK     = isSales ? 'ly_sales'  : isRet ? 'ly_returns' : 'ly_units';
                const fmt     = isSales ? f$ : fN;
                const tyColor = isRet ? 'rgba(251,146,60,.85)'  : B.b1;
                const lyColor = isRet ? 'rgba(251,146,60,.30)'  : 'rgba(91,159,212,.35)';
                const tyLabel = isSales ? 'TY $'       : isRet ? 'Returns TY' : 'TY Units';
                const lyLabel = isSales ? 'LY $'       : isRet ? 'Returns LY' : 'LY Units';
                const lyLegColor = isRet ? '#fb923c'   : '#5B9FD4';
                const W=1100, pad={t:14,r:20,b:24,l:54};
                const iw=W-pad.l-pad.r, ih=H-pad.t-pad.b, n=tArr.length;
                const xB=i=>pad.l+((i+0.5)/n)*iw;
                const uvArr=tArr.flatMap(d=>[d[tyK],d[lyK]]).filter(v=>v!=null&&v>=0);
                const umx=Math.max(...uvArr,1);
                const yU=v=>pad.t+ih-Math.min(1,Math.max(0,(v||0)/umx))*ih;
                const bw=Math.max(3,Math.floor((iw/n)*0.35));
                let s=`<svg width="100%" viewBox="0 0 ${W} ${H}" style="overflow:visible;display:block">`;
                for(let i=0;i<=4;i++){
                  const uval=umx/4*i, ya=yU(uval);
                  s+=`<line x1="${pad.l}" y1="${ya.toFixed(1)}" x2="${W-pad.r}" y2="${ya.toFixed(1)}" stroke="#1a2f4a" stroke-width="0.5"/>`;
                  s+=`<text x="${pad.l-5}" y="${(ya+4).toFixed(1)}" text-anchor="end" font-size="9" fill="#5b7fa0">${fmt(uval)}</text>`;
                }
                const step=Math.max(1,Math.floor(n/7));
                tArr.forEach((d,i)=>{if(i%step===0||i===n-1)s+=`<text x="${xB(i).toFixed(1)}" y="${H-4}" text-anchor="middle" font-size="8" fill="#374f66">${d.date?d.date.slice(5):''}</text>`;});
                tArr.forEach((d,i)=>{if((d[lyK]||0)>0){const bh=Math.max(2,(d[lyK]/umx)*ih);s+=`<rect x="${xB(i).toFixed(1)}" y="${yU(d[lyK]).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="${lyColor}" rx="2"/>`;}});
                tArr.forEach((d,i)=>{if((d[tyK]||0)>0){const bh=Math.max(2,(d[tyK]/umx)*ih);s+=`<rect x="${(xB(i)-bw).toFixed(1)}" y="${yU(d[tyK]).toFixed(1)}" width="${bw.toFixed(1)}" height="${bh.toFixed(1)}" fill="${tyColor}" rx="2"/>`;}});
                return <>{svgChart(s+'</svg>')}<Legend items={[[tyLabel, isRet?'#fb923c':B.b1],[lyLabel, lyLegColor, true]]}/></>;
              };

              const unitsTitle = unitsChartMetric==='sales'   ? '$ Sales — TY vs LY'
                               : unitsChartMetric==='returns' ? 'Return Units — TY vs LY'
                               : 'Units Sold — TY vs LY';

              return effPeriod === '7D' ? (
                <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:12,marginBottom:12}}>
                  <ChartCard title={revChartMetric==='conversion'?'Conversion & AUR Trend':'Revenue & AUR Trend'} error={errors.trend} headerRight={revRight}>
                    {(loading.trendChart || loading.trend) ? <Spinner/> : <>
                      {svgChart(salesAurSVG(toArr(effTrend), 1100, 165, revChartMetric))}
                      <Legend items={revChartMetric==='conversion'
                        ? [['Conv TY','#f97316'],['Conv LY','#fb923c',true],['AUR TY','#22c55e'],['AUR LY','#16a34a',true]]
                        : [['Sales TY','#2E6FBB'],['Sales LY','#5B9FD4',true],['AUR TY','#22c55e'],['AUR LY','#16a34a',true]]}/>
                    </>}
                  </ChartCard>
                  <ChartCard title={unitsTitle} error={errors.trend} headerRight={unitsRight}>
                    {(loading.trendChart || loading.trend) ? <Spinner/> : unitsBars(165)}
                  </ChartCard>
                </div>
              ) : (
                <div style={{display:'flex',flexDirection:'column',gap:12,marginBottom:12}}>
                  <ChartCard title={revChartMetric==='conversion'?'Conversion & AUR Trend':'Revenue & AUR Trend'} error={errors.trend} headerRight={revRight}>
                    {(loading.trendChart || loading.trend) ? <Spinner/> : <>
                      {svgChart(salesAurSVG(toArr(effTrend), 1100, 165, revChartMetric))}
                      <Legend items={revChartMetric==='conversion'
                        ? [['Conv TY','#f97316'],['Conv LY','#fb923c',true],['AUR TY','#22c55e'],['AUR LY','#16a34a',true]]
                        : [['Sales TY','#2E6FBB'],['Sales LY','#5B9FD4',true],['AUR TY','#22c55e'],['AUR LY','#16a34a',true]]}/>
                    </>}
                  </ChartCard>
                  <ChartCard title={unitsTitle} error={errors.trend} headerRight={unitsRight}>
                    {(loading.trendChart || loading.trend) ? <Spinner/> : unitsBars(190)}
                  </ChartCard>
                </div>
              );
            })()}
            {/* ── P&L Waterfall ── */}
            {(() => {
              const mtdW = periodCols?.['MTD'] || {};
              const rev = mtdW.sales || 0;
              if (rev <= 0) return null;
              const ret  = Math.abs(mtdW.returns_amount || 0);
              const fees = Math.abs(mtdW.amazon_fees || 0);
              const adSp = m.ad_spend || 0;
              const cogs = rev * 0.35;
              const netM = rev - cogs - fees - adSp - ret;
              const netPct = (netM / rev * 100);
              const wBar = val => Math.max(1, (val / rev) * 100);
              const wfRows = [
                {lbl:'Gross Revenue', val:rev,  col:B.b3,      bg:`linear-gradient(90deg,${B.b1},${B.b2})`, neg:false},
                {lbl:'− COGS (est.)', val:cogs, col:'#f87171', bg:'rgba(239,68,68,.35)',                    neg:true},
                {lbl:'− Amazon Fees', val:fees, col:'#fb923c', bg:'rgba(251,146,60,.35)',                   neg:true},
                {lbl:'− Ad Spend',    val:adSp, col:'#fbbf24', bg:'rgba(251,191,36,.3)',                    neg:true},
                {lbl:'− Returns',     val:ret,  col:'#94a3b8', bg:'rgba(148,163,184,.25)',                  neg:true},
              ];
              return (
                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,marginBottom:12}}>
                  <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:12}}>
                    <span style={{fontSize:10,fontWeight:700,color:'var(--txt2)'}}>P&L Waterfall — MTD</span>
                    <span style={{fontSize:9,fontWeight:700,color:'var(--txt3)',background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:5,padding:'2px 7px'}}>Estimated COGS</span>
                  </div>
                  <div style={{display:'flex',flexDirection:'column',gap:6,padding:'4px 0'}}>
                    {wfRows.map(({lbl,val,col,bg,neg})=>{
                      const w = wBar(val);
                      const ml = neg ? (100-w) : 0;
                      return (
                        <div key={lbl} style={{display:'grid',gridTemplateColumns:'130px 1fr 80px',gap:8,alignItems:'center'}}>
                          <div style={{fontSize:11,color:col,textAlign:'right',fontWeight:neg?500:700}}>{lbl}</div>
                          <div style={{position:'relative',height:22,borderRadius:4,background:'rgba(255,255,255,.03)'}}>
                            <div style={{position:'absolute',left:`${ml}%`,width:`${w}%`,height:'100%',borderRadius:4,background:bg,display:'flex',alignItems:'center',paddingLeft:8,fontSize:10,fontWeight:700,color:'rgba(255,255,255,.9)',whiteSpace:'nowrap',overflow:'hidden',transition:'width .5s ease'}}>
                              {neg?'−':''}{f$(Math.abs(val))}
                            </div>
                          </div>
                          <div style={{fontSize:12,fontWeight:700,textAlign:'right',color:col}}>{neg?'−':''}{f$(Math.abs(val))}</div>
                        </div>
                      );
                    })}
                    <div style={{height:1,background:'var(--brd)',margin:'4px 0'}}/>
                    <div style={{background:'var(--card2)',borderRadius:8,padding:'8px 12px',display:'grid',gridTemplateColumns:'130px 1fr 80px',gap:8,alignItems:'center'}}>
                      <div style={{fontSize:13,fontWeight:800,color:B.t3,textAlign:'right'}}>= Net Margin</div>
                      <div style={{background:'linear-gradient(90deg,rgba(26,163,146,.25),rgba(26,163,146,.08))',height:28,borderRadius:6,display:'flex',alignItems:'center',paddingLeft:12,fontSize:13,fontWeight:800,color:B.t3}}>
                        {f$(netM)}
                      </div>
                      <div style={{fontSize:15,fontWeight:800,textAlign:'right',color:B.t3}}>{netPct.toFixed(1)}<span style={{fontSize:10,color:'var(--txt3)'}}>%</span></div>
                    </div>
                  </div>
                  <div style={{fontSize:9,color:'var(--txt3)',marginTop:8}}>⚠ COGS estimated at 35% — upload actual COGS CSV for accurate net margin</div>
                </div>
              );
            })()}

            {/* ── Traffic & Conversion Pipeline ── */}
            <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,marginBottom:12}}>
              <SectionDivider label="Traffic & Conversion"/>
              {(() => {
                // Build 5-stage pipeline: Impressions, Clicks, Sessions, Add-to-Cart, Orders
                const adsImpressions = m?.impressions || 0;
                const adsClicks      = m?.clicks || 0;
                const ctr = adsImpressions > 0 ? (adsClicks / adsImpressions * 100).toFixed(1) + '%' : null;
                const isToday = activePeriod === 'Today';
                const synth = [
                  {label:'Impressions', ty:adsImpressions, ly:0, col:B.b3, step:null},
                  {label:'Clicks',      ty:adsClicks,      ly:0, col:B.b2, step: ctr ? `CTR: ${ctr}` : null},
                ];
                const funnelStages = Array.isArray(funnel) ? funnel : [];
                const allStages = [...synth, ...funnelStages];
                const maxV = Math.max(...allStages.map(s2=>s2.ty||0), 1);
                const sColors = [B.b3, B.b2, B.o2, B.t2, B.t3];
                return (
                  <div style={{display:'grid',gridTemplateColumns:`repeat(${allStages.length},1fr)`,gap:0,border:'1px solid var(--brd)',borderRadius:8,overflow:'hidden',marginBottom:12}}>
                    {allStages.map((s2,i)=>{
                      const lyDelta = s2.ly>0 ? ((s2.ty-s2.ly)/s2.ly*100) : null;
                      const col = sColors[i % sColors.length];
                      const isSessions = s2.label?.toLowerCase().includes('sess');
                      const prevTy = i>0 ? allStages[i-1].ty : 0;
                      const stepPct = i>0 && prevTy>0 && !isSessions ? ((s2.ty||0)/prevTy*100).toFixed(1)+'%' : null;
                      return (
                        <div key={s2.label} style={{padding:'10px 14px',borderRight:i<allStages.length-1?'1px solid var(--brd)':'none',position:'relative'}}>
                          <div style={{fontSize:10,color:'var(--txt3)',fontWeight:600,marginBottom:4,whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{s2.label}</div>
                          {isSessions && isToday ? (
                            <>
                              <div style={{fontSize:20,fontWeight:800,color:'var(--txt3)'}}>—</div>
                              <div style={{fontSize:9,color:B.sub,marginTop:4,display:'flex',alignItems:'center',gap:3}} title="Sessions data has a 24-hour processing lag from Amazon — will populate tomorrow">
                                <span style={{fontSize:10}}>ℹ</span> 24hr lag
                              </div>
                              {funnelStages[0]?.ly>0 && <div style={{fontSize:9,color:'var(--txt3)',fontStyle:'italic',marginTop:2}}>L30: {fN(funnelStages[0]?.ly||0)}</div>}
                            </>
                          ) : (
                            <>
                              <div style={{fontSize:20,fontWeight:800,color:col}}>{(s2.ty||0).toLocaleString()}</div>
                              <div style={{height:4,background:'var(--brd)',borderRadius:2,overflow:'hidden',margin:'6px 0 4px'}}>
                                <div style={{height:'100%',width:`${Math.max(4,(s2.ty/maxV)*100)}%`,background:col,borderRadius:2}}/>
                              </div>
                              {(s2.step||stepPct) && <div style={{fontSize:9,color:'var(--txt3)'}}>{s2.step || (stepPct+' step-through')}</div>}
                              {lyDelta!==null && <div style={{fontSize:10,fontWeight:700,color:lyDelta>=0?'#4ade80':'#fb923c'}}>{lyDelta>=0?'▲':'▼'}{Math.abs(lyDelta).toFixed(1)}% vs LY</div>}
                            </>
                          )}
                          {i<allStages.length-1 && <div style={{position:'absolute',right:-9,top:'50%',transform:'translateY(-50%)',fontSize:14,color:'var(--brd2)',zIndex:1}}>›</div>}
                        </div>
                      );
                    })}
                  </div>
                );
              })()}
              <div style={{display:'grid',gridTemplateColumns:'3fr 1fr',gap:12}}>
                <ChartCard title="Sessions & Conversion Rate" badge={cpSales} error={errors.trendTraffic} noMargin>
                  {(loading.trendTrafficChart || loading.trendTraffic) ? <Spinner/> : <>
                    {svgChart(sessionsConvSVG(toArr(trendTrafficChart || trendTraffic)))}
                    <Legend items={[['Sessions TY',B.o2],['Sessions LY',B.sub,true],['Conv% TY',B.t2]]}/>
                  </>}
                </ChartCard>

                {/* ── Per-chart traffic period override ── */}
                {(()=>{
                  const effTP = cpTrafficChart || cpSales;
                  return (
                    <div style={{display:'flex',alignItems:'center',gap:4,marginBottom:10,padding:'4px 12px',background:'var(--card)',border:'1px solid var(--brd)',borderRadius:8,flexWrap:'wrap'}}>
                      <span style={{fontSize:9,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.06em',marginRight:2,whiteSpace:'nowrap',flexShrink:0}}>Chart Period:</span>
                      {EXEC_PERIODS_LIST.map(p => {
                        const pv = EXEC_PERIOD_MAP[p];
                        const isActive = effTP === pv;
                        return (<button key={p} onClick={()=>setCpTrafficChart(pv)} style={{fontSize:9,fontWeight:700,padding:'2px 8px',borderRadius:4,cursor:'pointer',transition:'all .12s',border:`1px solid ${isActive?B.b2:'var(--brd)'}`,background:isActive?`${B.b2}22`:'transparent',color:isActive?B.b2:'var(--txt3)'}}>{p}</button>);
                      })}
                      {cpTrafficChart && cpTrafficChart !== cpSales && (
                        <button onClick={()=>setCpTrafficChart(null)} style={{fontSize:9,fontWeight:600,padding:'2px 8px',borderRadius:4,cursor:'pointer',border:'1px solid var(--brd)',background:'transparent',color:'var(--txt3)',marginLeft:4}}>↩ sync</button>
                      )}
                    </div>
                  );
                })()}
                <div style={{display:'flex',flexDirection:'column',gap:10}}>
                  <div style={{background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:10,padding:'12px 14px'}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:8}}>Traffic KPIs · MTD</div>
                    {metricsTraffic && [
                      ['Page Views', fN(metricsTraffic.glance_views||0), metricsTraffic.glance_views||0, metricsTraffic.ly_glance_views||0],
                      ['Sessions',   fN(metricsTraffic.sessions||0),     metricsTraffic.sessions||0,     metricsTraffic.ly_sessions||0],
                      ['Conv %',     fP(metricsTraffic.conversion||0),   metricsTraffic.conversion||0,   metricsTraffic.ly_conversion||0],
                      ['Buy Box %',  fP(metricsTraffic.buy_box||0),      metricsTraffic.buy_box||0,      metricsTraffic.ly_buy_box||0],
                    ].map(([lbl,val,ty,ly2])=>{
                      const delta=ly2>0?((ty-ly2)/ly2*100):null;
                      return (
                        <div key={lbl} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'4px 0',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                          <span style={{fontSize:10,color:'var(--txt3)'}}>{lbl}</span>
                          <div style={{textAlign:'right'}}>
                            <div style={{fontSize:12,fontWeight:700,color:'var(--txt)'}}>{val}</div>
                            {delta!==null && <div style={{fontSize:9,fontWeight:700,color:delta>=0?'#4ade80':'#fb923c'}}>{delta>=0?'▲':'▼'}{Math.abs(delta).toFixed(1)}% vs LY</div>}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            </div>

            {/* ── Business Health + Actions ── */}
            <div style={{display:'grid',gridTemplateColumns:'1fr 2fr',gap:12,marginBottom:12}}>
              {(() => {
                const tod2=periodCols?.['Today']||{};
                const mtdH=periodCols?.['MTD']||{};
                const paceScore=tod2.ty_forecast>0&&tod2.ly_eod_sales>0?Math.min(100,Math.round((tod2.ty_forecast/(tod2.ly_eod_sales||1))*50+50)):72;
                const convScore=metricsTraffic?.conversion>0?Math.min(100,Math.round((metricsTraffic.conversion/0.14)*85)):70;
                const roasScore=m.roas>0?Math.min(100,Math.round((m.roas/3.5)*100)):65;
                const atcS=Array.isArray(funnel)&&funnel.length>2?(funnel[2]?.ty||0)/(funnel[0]?.ty||1):0;
                const atcScore=Math.min(100,Math.round((atcS/0.22)*80));
                const retRatioH=(mtdH.returns_amount||0)/(mtdH.sales||1);
                const retScore=Math.min(100,Math.max(0,Math.round((1-retRatioH/0.05)*100)));
                const dosScore=m.dos>0?Math.min(100,Math.round(Math.min(m.dos,90)/90*100)):60;
                const overall=Math.round((paceScore+convScore+roasScore+atcScore+retScore+dosScore)/6);
                const scoreColor=overall>=80?'#22c55e':overall>=65?B.t2:overall>=50?'#f59e0b':'#ef4444';
                const scoreLabel=overall>=80?'Excellent':overall>=65?'Good':overall>=50?'Fair':'Needs Attention';
                const circ=2*Math.PI*38;
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,display:'flex',flexDirection:'column',alignItems:'center',gap:14}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',width:'100%',textAlign:'center'}}>Business Health Score</div>
                    <div style={{display:'flex',alignItems:'center',gap:16,width:'100%'}}>
                      <div style={{position:'relative',width:88,height:88,flexShrink:0}}>
                        <svg viewBox="0 0 88 88" width="88" height="88" style={{transform:'rotate(-90deg)'}}>
                          <circle cx="44" cy="44" r="38" fill="none" stroke="var(--brd)" strokeWidth="8"/>
                          <circle cx="44" cy="44" r="38" fill="none" stroke={scoreColor} strokeWidth="8"
                            strokeDasharray={`${(overall/100)*circ} ${circ}`} strokeLinecap="round"/>
                        </svg>
                        <div style={{position:'absolute',inset:0,display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center'}}>
                          <div style={{fontSize:22,fontWeight:800,lineHeight:1,color:scoreColor}}>{overall}</div>
                          <div style={{fontSize:8,fontWeight:700,textTransform:'uppercase',letterSpacing:'.06em',color:'var(--txt3)',marginTop:1}}>/ 100</div>
                        </div>
                      </div>
                      <div style={{flex:1}}>
                        <div style={{fontSize:14,fontWeight:700,color:scoreColor,marginBottom:3}}>{scoreLabel}</div>
                        <div style={{fontSize:10,color:'var(--txt3)',lineHeight:1.5}}>Revenue pace, conversion, ROAS, funnel efficiency, returns & inventory.</div>
                      </div>
                    </div>
                    <div style={{width:'100%',display:'flex',flexDirection:'column',gap:5}}>
                      {[['Revenue Pace',paceScore,'#22c55e'],['Conversion Rate',convScore,B.t2],['ROAS / Ad Eff.',roasScore,B.b3],['ATC / Funnel',atcScore,'#f59e0b'],['Return Rate',retScore,'#22c55e'],['Inventory DOS',dosScore,B.b3]].map(([lbl,sc,col])=>(
                        <div key={lbl} style={{display:'flex',alignItems:'center',gap:8,fontSize:10}}>
                          <span style={{width:96,color:'var(--txt3)',whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{lbl}</span>
                          <div style={{flex:1,height:5,background:'var(--brd)',borderRadius:3,overflow:'hidden'}}>
                            <div style={{width:`${sc}%`,height:'100%',background:col,borderRadius:3}}/>
                          </div>
                          <span style={{width:26,textAlign:'right',fontWeight:700,color:col}}>{sc}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })()}
              {(() => {
                const mtdA=periodCols?.['MTD']||{};
                const actions=[];
                const atcIdx=Array.isArray(funnel)?funnel.findIndex(s2=>s2.label?.toLowerCase().includes('cart')):-1;
                const sessIdx=Array.isArray(funnel)?funnel.findIndex(s2=>s2.label?.toLowerCase().includes('sess')):-1;
                const atcRate=atcIdx>=0&&sessIdx>=0&&(funnel[sessIdx]?.ty||0)>0?(funnel[atcIdx].ty/funnel[sessIdx].ty):0;
                if(atcRate>0&&atcRate<0.18) actions.push({type:'alert',title:`🛒 ATC Rate low at ${(atcRate*100).toFixed(1)}% — benchmark ~22%`,desc:'Add-to-cart rate is below the 90-day benchmark. Review top ASINs for pricing gaps, competitor undercutting, or missing A+ content.'});
                if((m.tacos||0)>0.16) actions.push({type:'warn',title:`📣 TACOS at ${fP(m.tacos)} — approaching 16% threshold`,desc:'Ad spend as a % of revenue is elevated. Consider pausing underperforming campaigns or reducing bids on low-ROAS keywords.'});
                if((m.dos||0)>0&&(m.dos||0)<30) actions.push({type:'alert',title:`📦 Days of Supply at ${m.dos}d — reorder approaching`,desc:`With ${m.dos} days of supply, FBA stock is running low. Create a shipment plan now to avoid stockouts and BSR drop.`});
                const mtdChg=(mtdA.ly_sales||0)>0?((mtdA.sales||0)-(mtdA.ly_sales||0))/(mtdA.ly_sales||1):null;
                if(mtdChg!==null&&mtdChg>=0.05) actions.push({type:'ok',title:`✅ MTD pacing ▲${(mtdChg*100).toFixed(1)}% vs LY`,desc:'Revenue is tracking above last year. Consider increasing ad budget to capture more share while TACOS is healthy.'});
                else if(mtdChg!==null&&mtdChg<-0.02) actions.push({type:'warn',title:`📉 MTD revenue ▼${(Math.abs(mtdChg)*100).toFixed(1)}% vs LY`,desc:'Revenue is trailing last year MTD. Check for suppressed listings, lost Buy Box, or gaps in ad coverage.'});
                const retR=(mtdA.returns_amount||0)/(mtdA.sales||1);
                if(retR>0&&retR<0.04) actions.push({type:'ok',title:`✅ Return rate ${(retR*100).toFixed(1)}% — below 4% target`,desc:'Returns are well-controlled. Maintain current listing accuracy and A+ content that is driving this improvement.'});
                else if(retR>0.07) actions.push({type:'alert',title:`⚠ Return rate ${(retR*100).toFixed(1)}% — above 7% threshold`,desc:'High returns may indicate listing inaccuracy, product issues, or sizing confusion. Review top return reasons in Seller Central.'});
                if((m.roas||0)>0&&(m.roas||0)<2.5) actions.push({type:'warn',title:`📊 ROAS at ${fX(m.roas)} — below 2.5x floor`,desc:'Ad return on spend is low. Review campaign structure, negative keywords, and bid strategy. Consider pausing bottom-decile ASINs.'});
                if(actions.length===0) actions.push({type:'info',title:'✅ All metrics within healthy ranges',desc:'No urgent actions detected. Review the detailed tabs for optimization opportunities in advertising, inventory, and pricing.'});
                const tStyle={alert:{border:'rgba(239,68,68,.2)',bg:'rgba(239,68,68,.05)',dot:'#ef4444'},warn:{border:'rgba(245,158,11,.2)',bg:'rgba(245,158,11,.05)',dot:'#f59e0b'},ok:{border:'rgba(34,197,94,.2)',bg:'rgba(34,197,94,.05)',dot:'#22c55e'},info:{border:'rgba(91,159,212,.2)',bg:'rgba(91,159,212,.04)',dot:B.b3}};
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',marginBottom:12}}>📋 Actions to Take Today</div>
                    <div style={{display:'flex',flexDirection:'column',gap:8}}>
                      {actions.slice(0,6).map((a,i)=>{
                        const st=tStyle[a.type]||tStyle.info;
                        return (
                          <div key={i} style={{display:'flex',gap:10,alignItems:'flex-start',padding:'10px 12px',borderRadius:8,border:`1px solid ${st.border}`,background:st.bg}}>
                            <div style={{width:8,height:8,borderRadius:'50%',background:st.dot,flexShrink:0,marginTop:4}}/>
                            <div>
                              <div style={{fontSize:12,fontWeight:600,color:'var(--txt)',marginBottom:2}}>{a.title}</div>
                              <div style={{fontSize:10,color:'var(--txt3)',lineHeight:1.5}}>{a.desc}</div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })()}
            </div>

            {/* ── DAY-OF-WEEK INTELLIGENCE ── */}
            {(() => {
              const tArr = toArr(trend);
              if (!tArr || tArr.length < 7) return null;
              // group by DOW
              const dowMap = {};
              tArr.forEach(d => {
                if (!d.date || !d.ty_sales) return;
                const dow = new Date(d.date+'T12:00:00').getDay(); // 0=Sun..6=Sat
                if (!dowMap[dow]) dowMap[dow] = {sales:[],units:[]};
                dowMap[dow].sales.push(d.ty_sales||0);
                dowMap[dow].units.push(d.ty_units||0);
              });
              const DOW_NAMES = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
              const avgs = Object.entries(dowMap).map(([dow,v]) => ({
                dow: parseInt(dow),
                name: DOW_NAMES[parseInt(dow)],
                avgSales: v.sales.reduce((a,b)=>a+b,0)/v.sales.length,
                avgUnits: v.units.reduce((a,b)=>a+b,0)/v.units.length,
              })).filter(d => d.avgSales > 0);
              if (avgs.length < 3) return null;
              const overallAvg = avgs.reduce((a,b)=>a+b.avgSales,0)/avgs.length;
              const best = avgs.reduce((a,b)=>a.avgSales>b.avgSales?a:b);
              const worst = avgs.reduce((a,b)=>a.avgSales<b.avgSales?a:b);
              const bestVsAvg = overallAvg > 0 ? ((best.avgSales - overallAvg)/overallAvg*100).toFixed(0) : 0;
              const worstVsAvg = overallAvg > 0 ? ((overallAvg - worst.avgSales)/overallAvg*100).toFixed(0) : 0;
              return (
                <div style={{marginBottom:12}}>
                  <div style={{display:'flex',alignItems:'center',gap:10,margin:'4px 0 10px'}}>
                    <div style={{flex:1,height:1,background:'var(--brd)'}}/>
                    <span style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.12em',color:'var(--txt3)',whiteSpace:'nowrap'}}>Day-of-Week Intelligence</span>
                    <div style={{flex:1,height:1,background:'var(--brd)'}}/>
                  </div>
                  <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:10}}>
                    <div style={{background:'rgba(232,130,30,.04)',border:'1px solid rgba(232,130,30,.3)',borderRadius:10,padding:'12px 14px',display:'flex',alignItems:'center',gap:14}}>
                      <span style={{fontSize:32,flexShrink:0}}>🥇</span>
                      <div style={{flex:1}}>
                        <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.09em',color:'var(--txt3)',marginBottom:3}}>This Period's Best Day</div>
                        <div style={{fontSize:20,fontWeight:800,color:B.o3}}>{best.name}</div>
                        <div style={{fontSize:11,color:'var(--txt3)',marginTop:2}}>{f$(best.avgSales)} avg · {Math.round(best.avgUnits)} units avg</div>
                        <div style={{fontSize:11,fontWeight:700,color:'#4ade80',marginTop:3}}>▲ {bestVsAvg}% above daily avg &nbsp;·&nbsp; Weekend premium {parseInt(bestVsAvg)>30?'confirmed':'noted'}</div>
                      </div>
                    </div>
                    <div style={{background:'var(--card)',border:'1px solid rgba(100,116,139,.25)',borderRadius:10,padding:'12px 14px',display:'flex',alignItems:'center',gap:14}}>
                      <span style={{fontSize:32,flexShrink:0}}>📉</span>
                      <div style={{flex:1}}>
                        <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.09em',color:'var(--txt3)',marginBottom:3}}>Softest Day</div>
                        <div style={{fontSize:20,fontWeight:800,color:'var(--txt3)'}}>{worst.name}</div>
                        <div style={{fontSize:11,color:'var(--txt3)',marginTop:2}}>{f$(worst.avgSales)} avg · {Math.round(worst.avgUnits)} units avg</div>
                        <div style={{fontSize:11,fontWeight:700,color:'#fb923c',marginTop:3}}>▼ {worstVsAvg}% below daily avg &nbsp;·&nbsp; Consider {worst.name.slice(0,3)} ad boost</div>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })()}

            {/* ── DAYS OF SUPPLY GAUGE ── */}
            {m.dos > 0 && (() => {
              const dos = m.dos;
              const unitsOH = m.fba_units || 0;
              const maxDos = 90;
              const pctPos = Math.min(100, (dos / maxDos) * 100);
              const dosColor = dos < 14 ? '#ef4444' : dos < 30 ? '#f59e0b' : dos < 60 ? '#22c55e' : '#3b82f6';
              const dosLabel = dos < 14 ? '🔴 Critical' : dos < 30 ? '🟡 Reorder Soon' : dos < 60 ? '🟢 Healthy' : '🔵 Surplus';
              const reorderDate = new Date(); reorderDate.setDate(reorderDate.getDate() + Math.max(0, dos - 30));
              return (
                <div style={{marginBottom:12}}>
                  <div style={{display:'flex',alignItems:'center',gap:10,margin:'4px 0 10px'}}>
                    <div style={{flex:1,height:1,background:'var(--brd)'}}/>
                    <span style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.12em',color:'var(--txt3)',whiteSpace:'nowrap'}}>Inventory Health</span>
                    <div style={{flex:1,height:1,background:'var(--brd)'}}/>
                  </div>
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>
                    <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:12,flexWrap:'wrap',gap:10}}>
                      <div>
                        <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:4}}>Current DOS</div>
                        <div style={{fontSize:24,fontWeight:800,lineHeight:1,color:dosColor,display:'flex',alignItems:'center',gap:10}}>
                          {dos} <span style={{fontSize:14,fontWeight:600}}>days</span>
                          <span style={{fontSize:11,fontWeight:700,padding:'3px 10px',borderRadius:10,background:`${dosColor}22`,color:dosColor,border:`1px solid ${dosColor}44`}}>{dosLabel}</span>
                        </div>
                      </div>
                      {unitsOH > 0 && <div style={{borderLeft:'1px solid var(--brd)',paddingLeft:16}}>
                        <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:4}}>Units on Hand</div>
                        <div style={{fontSize:20,fontWeight:800,color:B.b3}}>{fN(unitsOH)}</div>
                      </div>}
                      <div style={{borderLeft:'1px solid var(--brd)',paddingLeft:16}}>
                        <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:4}}>Reorder By</div>
                        <div style={{fontSize:18,fontWeight:800,color:B.o2}}>{reorderDate.toLocaleDateString('en-US',{month:'short',day:'numeric'})}</div>
                      </div>
                    </div>
                    {/* DoS zone bar */}
                    <div style={{display:'flex',height:14,borderRadius:7,overflow:'hidden',marginBottom:5,position:'relative'}}>
                      <div style={{flex:'0 0 15.5%',background:'rgba(239,68,68,.4)'}}/>
                      <div style={{flex:'0 0 17.8%',background:'rgba(245,158,11,.4)'}}/>
                      <div style={{flex:'0 0 33.3%',background:'rgba(34,197,94,.4)'}}/>
                      <div style={{flex:1,background:'rgba(59,130,246,.4)'}}/>
                      <div style={{position:'absolute',top:'50%',left:`${pctPos}%`,transform:'translate(-50%,-50%)',width:4,height:20,background:dosColor,borderRadius:2,boxShadow:`0 0 6px ${dosColor}`}}/>
                    </div>
                    <div style={{display:'flex',justifyContent:'space-between',fontSize:8,marginBottom:10}}>
                      <span style={{color:'#ef4444',fontWeight:600}}>0–14d Critical</span>
                      <span style={{color:'#f59e0b',fontWeight:600}}>15–30d Reorder</span>
                      <span style={{color:'#22c55e',fontWeight:600}}>31–60d Healthy</span>
                      <span style={{color:'#3b82f6',fontWeight:600}}>61d+ Surplus</span>
                    </div>
                    <div style={{display:'flex',alignItems:'center',gap:7,background:`${dosColor}12`,border:`1px solid ${dosColor}30`,borderRadius:7,padding:'6px 10px',fontSize:10,color:dos<30?'#fca5a5':'#86efac',fontWeight:600}}>
                      {dos<14?'🔴 Critical stock level — create FBA shipment immediately':dos<30?`⚠ Reorder soon — target shipment by ${reorderDate.toLocaleDateString('en-US',{month:'short',day:'numeric'})}`:dos<60?`✅ Inventory healthy — monitor weekly · Next reorder ~${reorderDate.toLocaleDateString('en-US',{month:'short',day:'numeric'})}`:'📦 Surplus inventory — consider pausing replenishment'}
                    </div>
                  </div>
                </div>
              );
            })()}

            {/* ── BUSINESS INSIGHTS ── */}
            {(() => {
              const mtdBI = periodCols?.['MTD'] || {};
              const allInsights = [];
              const atcIdxBI = Array.isArray(funnel) ? funnel.findIndex(s=>s.label?.toLowerCase().includes('cart')) : -1;
              const sessIdxBI = Array.isArray(funnel) ? funnel.findIndex(s=>s.label?.toLowerCase().includes('sess')) : -1;
              const atcRateBI = atcIdxBI>=0 && sessIdxBI>=0 && (funnel[sessIdxBI]?.ty||0)>0 ? (funnel[atcIdxBI].ty/funnel[sessIdxBI].ty) : 0;
              if (atcRateBI>0 && atcRateBI<0.18) allInsights.push({key:'atc',type:'alert',title:`🛒 ATC Rate at ${(atcRateBI*100).toFixed(1)}% — below 22% benchmark`,desc:'Add-to-cart rate is below the benchmark. Review top ASINs for pricing gaps, competitor undercutting, or missing A+ content.',action:'Review Listings'});
              if ((m.tacos||0)>0.16) allInsights.push({key:'tacos',type:'warn',title:`📣 TACOS at ${fP(m.tacos)} — near 16% threshold`,desc:'Ad spend as a % of revenue is elevated. Consider pausing underperforming campaigns or reducing bids on low-ROAS keywords.',action:'Review Campaigns'});
              if ((m.dos||0)>0 && (m.dos||0)<30) allInsights.push({key:'dos',type:'alert',title:`📦 Days of Supply at ${m.dos}d — reorder needed`,desc:`Stock is running low. Create a shipment plan now to avoid stockouts and BSR drop.`,action:'Create Shipment'});
              const mtdChgBI = (mtdBI.ly_sales||0)>0 ? ((mtdBI.sales||0)-(mtdBI.ly_sales||0))/(mtdBI.ly_sales||1) : null;
              if (mtdChgBI!==null && mtdChgBI>=0.05) allInsights.push({key:'mtdup',type:'ok',title:`✅ MTD pacing ▲${(mtdChgBI*100).toFixed(1)}% vs LY — strong month`,desc:'Revenue is tracking well above last year. Consider increasing ad budget while TACOS is still healthy.'});
              else if (mtdChgBI!==null && mtdChgBI<-0.03) allInsights.push({key:'mtddn',type:'warn',title:`📉 MTD revenue ▼${(Math.abs(mtdChgBI)*100).toFixed(1)}% vs LY`,desc:'Revenue trailing last year. Check for suppressed listings, lost Buy Box, or gaps in ad coverage.'});
              const retRBI = (mtdBI.returns_amount||0)/(mtdBI.sales||1);
              if (retRBI>0 && retRBI<0.04) allInsights.push({key:'ret',type:'ok',title:`✅ Return rate ${(retRBI*100).toFixed(1)}% — below 4% target`,desc:'Returns are well-controlled. Maintain current listing accuracy and A+ content.'});
              else if (retRBI>0.07) allInsights.push({key:'rethigh',type:'alert',title:`⚠ Return rate ${(retRBI*100).toFixed(1)}% — above 7% threshold`,desc:'High returns may indicate listing inaccuracy or product issues. Review top return reasons in Seller Central.'});
              if ((m.roas||0)>0 && (m.roas||0)<2.5) allInsights.push({key:'roas',type:'warn',title:`📊 ROAS at ${fX(m.roas)} — below 2.5× floor`,desc:'Ad return on spend is low. Review campaign structure, negative keywords, and bid strategy.'});
              const now = Date.now();
              const visible = allInsights.filter(ins =>
                !dismissedDailyInsights.includes(ins.key) &&
                !(snoozedDailyInsights[ins.key] && snoozedDailyInsights[ins.key] > now)
              );
              if (visible.length === 0) return null;
              const tStyle2 = {alert:{border:'rgba(239,68,68,.2)',bg:'rgba(239,68,68,.05)',dot:'#ef4444'},warn:{border:'rgba(245,158,11,.2)',bg:'rgba(245,158,11,.05)',dot:'#f59e0b'},ok:{border:'rgba(34,197,94,.2)',bg:'rgba(34,197,94,.05)',dot:'#22c55e'}};
              const snoozeIns = key => setSnoozedDailyInsights(p=>({...p,[key]: now + 7*24*60*60*1000}));
              const dismissIns = key => setDismissedDailyInsights(p=>[...p,key]);
              return (
                <div style={{marginBottom:12}}>
                  <SectionDivider label="Business Insights"/>
                  <div style={{display:'flex',flexDirection:'column',gap:8}}>
                    {visible.map(ins => {
                      const st2 = tStyle2[ins.type] || tStyle2.ok;
                      return (
                        <div key={ins.key} style={{display:'flex',alignItems:'flex-start',gap:10,padding:'10px 12px',borderRadius:9,border:`1px solid ${st2.border}`,background:st2.bg}}>
                          <div style={{width:8,height:8,borderRadius:'50%',background:st2.dot,flexShrink:0,marginTop:4}}/>
                          <div style={{flex:1}}>
                            <div style={{fontSize:11,fontWeight:700,color:'var(--txt)',marginBottom:3}}>{ins.title}</div>
                            <div style={{fontSize:10,color:'var(--txt3)',lineHeight:1.5}}>{ins.desc}</div>
                            <div style={{display:'flex',gap:6,marginTop:6,alignItems:'center'}}>
                              {ins.action && <button style={{fontSize:9,padding:'2px 8px',borderRadius:5,border:'1px solid var(--brd)',color:'var(--txt3)',background:'transparent',cursor:'pointer',fontWeight:600}}>{ins.action} →</button>}
                              <button onClick={()=>snoozeIns(ins.key)} style={{fontSize:9,padding:'2px 8px',borderRadius:5,border:'1px solid var(--brd)',color:'var(--txt3)',background:'transparent',cursor:'pointer',fontWeight:600}}>⏰ Snooze 7 days</button>
                              <button onClick={()=>dismissIns(ins.key)} style={{fontSize:9,color:B.sub,background:'transparent',border:'none',cursor:'pointer',padding:'2px 6px',borderRadius:4}}>✕ Dismiss</button>
                            </div>
                          </div>
                          <button onClick={()=>dismissIns(ins.key)} style={{background:'none',border:'none',color:'var(--txt3)',cursor:'pointer',fontSize:14,padding:'2px 5px',borderRadius:4,lineHeight:1,flexShrink:0}}>✕</button>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })()}

            {/* ── ADVERTISING ── */}
            <SectionDivider label="Advertising"/>
            <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:10}}>
              <span style={{fontSize:9,color:B.sub,fontWeight:700,textTransform:'uppercase',letterSpacing:'.07em'}}>Period:</span>
              <span style={{fontSize:10,color:B.b3,fontWeight:700,background:'rgba(46,111,187,.12)',border:'1px solid rgba(46,111,187,.25)',borderRadius:5,padding:'2px 8px'}}>{activePeriod}</span>
              <span style={{fontSize:9,color:B.sub}}>— same as period selector above</span>
            </div>
            {loading.metrics ? <Spinner/> : (
              <div style={{display:'flex',gap:8,marginBottom:12,overflowX:'auto',paddingBottom:4}}>
                {/* Ad Spend */}
                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:10,padding:'10px 12px',minWidth:130,flex:1}}>
                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:6}}>Ad Spend $</div>
                  <div style={{fontSize:18,fontWeight:800,lineHeight:1.1,color:'var(--txt)'}}>{f$(m.ad_spend||0)}</div>
                  {ly('ad_spend')>0 && <div style={{fontSize:10,color:'var(--txt3)',marginTop:2}}>LY: {f$(ly('ad_spend'))} &nbsp;<span style={{color:(m.ad_spend||0)>(ly('ad_spend')||0)?'#f87171':'#4ade80',fontWeight:700}}>{(m.ad_spend||0)>(ly('ad_spend')||0)?'▲':'▼'}{Math.abs(dp(m.ad_spend,ly('ad_spend'))||0).toFixed(1)}%</span></div>}
                  <div style={{fontSize:10,marginTop:3,color:B.sub}}>{(m.tacos||0)>0?(m.tacos*100).toFixed(1)+'% TACOS':null}</div>
                </div>
                {/* ROAS */}
                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:10,padding:'10px 12px',minWidth:130,flex:1}}>
                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:6}}>ROAS</div>
                  <div style={{fontSize:18,fontWeight:800,lineHeight:1.1,color:(m.roas||0)>=2.5?'#4ade80':'#f87171'}}>{fX(m.roas||0)}</div>
                  {ly('roas')>0 && <div style={{fontSize:10,color:'var(--txt3)',marginTop:2}}>LY: {fX(ly('roas'))} &nbsp;<span style={{color:(m.roas||0)>=(ly('roas')||0)?'#4ade80':'#f87171',fontWeight:700}}>{(m.roas||0)>=(ly('roas')||0)?'▲':'▼'}{Math.abs(dp(m.roas,ly('roas'))||0).toFixed(0)}%</span></div>}
                  <div style={{fontSize:10,marginTop:3}}>{(m.roas||0)>=2.5?<span style={{color:'#4ade80'}}>✓ Above 2.5× floor</span>:<span style={{color:'#f87171'}}>⚠ Below 2.5× floor</span>}</div>
                </div>
                {/* TACOS */}
                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:10,padding:'10px 12px',minWidth:130,flex:1}}>
                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:6}}>TACOS</div>
                  <div style={{fontSize:18,fontWeight:800,lineHeight:1.1,color:(m.tacos||0)>0.14?B.o3:'var(--txt)'}}>{fP(m.tacos||0)}</div>
                  {ly('tacos')>0 && <div style={{fontSize:10,color:'var(--txt3)',marginTop:2}}>LY: {fP(ly('tacos'))} &nbsp;<span style={{color:(m.tacos||0)<=(ly('tacos')||0)?'#4ade80':'#f87171',fontWeight:700}}>{(m.tacos||0)<=(ly('tacos')||0)?'▼':'▲'}{Math.abs(dp(m.tacos,ly('tacos'))||0).toFixed(0)}%</span></div>}
                  <div style={{fontSize:10,marginTop:3}}>{(m.tacos||0)>0.16?<span style={{color:'#f87171'}}>⚠ Near 16% ceiling</span>:<span style={{color:'#4ade80'}}>✓ Under 16% ceiling</span>}</div>
                </div>
              </div>
            )}
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:12,marginBottom:12}}>
              <ChartCard title="Ad Efficiency — ACOS vs ROAS" badge={`${activePeriod} · TY vs LY`} error={errors.adEff}>
                {loading.adEff ? <Spinner/> : svgChart(adQuadrantSVG(adEff))}
              </ChartCard>
              <ChartCard title="Competitor Price Watch" badge="Live · SP-API">
                {(() => {
                  const pricing = pricingCache?.amazonPricing;
                  if (!pricing || Object.keys(pricing).length === 0) {
                    return <div style={{color:'var(--txt3)',fontSize:11,textAlign:'center',padding:20}}>No pricing data — sync pending</div>;
                  }
                  const rows = Object.entries(pricing).slice(0, 6).map(([asin, p]) => {
                    const yourPrice = p.listPrice || p.landedPrice || 0;
                    const bbPrice   = p.buyBoxPrice || yourPrice;
                    const delta     = bbPrice - yourPrice;
                    const wonBB     = Math.abs(delta) < 0.05 || bbPrice >= yourPrice * 0.99;
                    return {asin, name: p.name || asin.slice(-8), yourPrice, bbPrice, delta, wonBB};
                  });
                  return (
                    <>
                      <table style={{width:'100%',borderCollapse:'collapse',fontSize:11}}>
                        <thead>
                          <tr>{['ASIN / SKU','Your Price','Buy Box','Δ','Status'].map(h=>(
                            <th key={h} style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',padding:'4px 8px',borderBottom:'1px solid var(--brd)',textAlign:'left'}}>{h}</th>
                          ))}</tr>
                        </thead>
                        <tbody>
                          {rows.map(r=>(
                            <tr key={r.asin}>
                              <td style={{padding:'6px 8px',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                                <div style={{fontWeight:600,color:'var(--txt)'}}>{r.name}</div>
                                <div style={{fontSize:9,color:B.sub}}>{r.asin}</div>
                              </td>
                              <td style={{padding:'6px 8px',borderBottom:'1px solid rgba(26,47,74,.4)',color:B.b3,fontWeight:600}}>{f$(r.yourPrice)}</td>
                              <td style={{padding:'6px 8px',borderBottom:'1px solid rgba(26,47,74,.4)',color:'var(--txt2)'}}>{f$(r.bbPrice)}</td>
                              <td style={{padding:'6px 8px',borderBottom:'1px solid rgba(26,47,74,.4)',fontWeight:700,
                                color:Math.abs(r.delta)<0.05?B.o3:r.wonBB?'#4ade80':'#f87171'}}>
                                {Math.abs(r.delta)<0.05?'= Tied':r.wonBB?`▲ ${f$(r.delta)}`:`▼ ${f$(Math.abs(r.delta))}`}
                              </td>
                              <td style={{padding:'6px 8px',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                                <span style={{fontSize:9,padding:'1px 5px',borderRadius:3,fontWeight:700,
                                  background:r.wonBB?'rgba(34,197,94,.15)':'rgba(239,68,68,.15)',
                                  color:r.wonBB?'#4ade80':'#f87171',
                                  border:`1px solid ${r.wonBB?'rgba(34,197,94,.3)':'rgba(239,68,68,.3)'}`}}>
                                  {r.wonBB?'Won ✓':'Lost'}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      {pricingCache?.lastSync && <div style={{fontSize:9,color:B.sub,marginTop:8}}>Last sync: {new Date(pricingCache.lastSync).toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',hour12:true})} CT · From pricing_sync.json cache</div>}
                    </>
                  );
                })()}
              </ChartCard>
            </div>

            {/* ── PROFITABILITY ── */}
            <EpochLabel type="period" label="💰 Profitability" right="MTD"/>
            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:12,marginBottom:12}}>
              {(() => {
                const mtdP=periodCols?.['MTD']||{};
                const rev=mtdP.sales||0;
                const ret=mtdP.returns_amount||0;
                const netRev=rev-ret;
                const fees=Math.abs(mtdP.amazon_fees||0);
                const adSpend=m.ad_spend||0;
                const cogs=netRev*0.35;
                const cm=netRev-fees-adSpend-cogs;
                const cmPct=netRev>0?cm/netRev*100:0;
                const wfRows=[
                  {lbl:'Gross Revenue', val:rev,      col:B.t2,    neg:false, eq:false},
                  {lbl:'- Returns',     val:-ret,     col:'#ef4444',neg:true, eq:false},
                  {lbl:'= Net Revenue', val:netRev,   col:'var(--txt)',neg:false,eq:true},
                  {lbl:'- Amazon Fees', val:-fees,    col:'#ef4444',neg:true, eq:false},
                  {lbl:'- Ad Spend',    val:-adSpend, col:'#ef4444',neg:true, eq:false},
                  {lbl:'- COGS (est.)', val:-cogs,    col:'#ef4444',neg:true, eq:false},
                ];
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:10}}>P&L Summary · MTD</div>
                    {wfRows.map((r,idx)=>(
                      <div key={idx} style={{display:'flex',justifyContent:'space-between',padding:'6px 0',borderBottom:r.eq?'1px solid var(--brd)':'1px solid rgba(26,47,74,.4)',alignItems:'center'}}>
                        <span style={{fontSize:11,color:r.neg?'var(--txt3)':'var(--txt2)'}}>{r.lbl}</span>
                        <span style={{fontSize:12,fontWeight:700,color:r.col}}>{r.neg?'−':''}{f$(Math.abs(r.val))}</span>
                      </div>
                    ))}
                    <div style={{display:'flex',justifyContent:'space-between',padding:'8px 0 2px',alignItems:'center'}}>
                      <span style={{fontSize:12,fontWeight:700,color:'var(--txt)'}}>Contribution Margin</span>
                      <span style={{fontSize:16,fontWeight:800,color:B.t2}}>{f$(cm)}</span>
                    </div>
                    <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                      <span style={{fontSize:10,color:'var(--txt3)'}}>Margin %</span>
                      <span style={{fontSize:12,fontWeight:700,color:B.t3}}>{cmPct.toFixed(1)}%</span>
                    </div>
                  </div>
                );
              })()}
              <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>
                <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:10}}>Amazon Fee Breakdown · MTD</div>
                {feeBreak && (feeBreak.items||Array.isArray(feeBreak)) ? (() => {
                  const items=feeBreak.items||feeBreak;
                  const total=items.reduce((s2,f2)=>s2+(f2.amount||0),0);
                  const fColors=['#E87830',B.b2,B.t2,B.sub,'#f59e0b'];
                  return <>
                    {items.slice(0,5).map((f2,i)=>{
                      const pct=total>0?((f2.amount||0)/total*100).toFixed(1):'0';
                      return (
                        <div key={f2.type} style={{display:'flex',justifyContent:'space-between',alignItems:'center',padding:'5px 0',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                          <div style={{display:'flex',alignItems:'center',gap:6}}>
                            <div style={{width:10,height:10,borderRadius:2,background:fColors[i%fColors.length],flexShrink:0}}/>
                            <span style={{fontSize:11,color:'var(--txt3)'}}>{f2.type}</span>
                          </div>
                          <div style={{textAlign:'right'}}>
                            <div style={{fontSize:12,fontWeight:700,color:fColors[i%fColors.length]}}>{f$(f2.amount||0)}</div>
                            <div style={{fontSize:9,color:'var(--txt3)'}}>{pct}%</div>
                          </div>
                        </div>
                      );
                    })}
                    <div style={{display:'flex',justifyContent:'space-between',padding:'6px 0 0',alignItems:'center',borderTop:'1px solid var(--brd)',marginTop:2}}>
                      <span style={{fontSize:11,fontWeight:700,color:'var(--txt)'}}>Total Fees</span>
                      <span style={{fontSize:14,fontWeight:800,color:'#ef4444'}}>{f$(total)}</span>
                    </div>
                  </>;
                })() : <div style={{color:'var(--txt3)',fontSize:11,textAlign:'center',padding:'20px 0'}}>Fee data loading…</div>}
              </div>
              {(() => {
                const mtdM=periodCols?.['MTD']||{};
                const rev5=mtdM.sales||0, lyRev5=mtdM.ly_sales||0;
                const fees5=Math.abs(mtdM.amazon_fees||0), lyFees5=Math.abs(mtdM.ly_amazon_fees||0);
                const ret5=mtdM.returns_amount||0, lyRet5=mtdM.ly_returns_amount||0;
                const adS5=m.ad_spend||0, lyAd5=m.ly_ad_spend||0;
                const cm5=rev5-ret5-fees5-adS5-(rev5-ret5)*0.35;
                const lyCm5=lyRev5-lyRet5-lyFees5-lyAd5-(lyRev5-lyRet5)*0.35;
                const cmPct5=rev5>0?cm5/rev5*100:0, lyCmPct5=lyRev5>0?lyCm5/lyRev5*100:0;
                const cmChg=lyCmPct5>0?cmPct5-lyCmPct5:null;
                return (
                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`2px solid ${B.t2}`,borderRadius:12,padding:16}}>
                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:10}}>Margin & AUR · MTD</div>
                    <div style={{display:'flex',gap:14,alignItems:'flex-end',marginBottom:12,flexWrap:'wrap'}}>
                      <div><div style={{fontSize:9,color:'var(--txt3)',marginBottom:2}}>TY CM%</div><div style={{fontSize:26,fontWeight:800,color:B.t2,lineHeight:1}}>{cmPct5.toFixed(1)}%</div></div>
                      <div><div style={{fontSize:9,color:'var(--txt3)',marginBottom:2}}>LY CM%</div><div style={{fontSize:26,fontWeight:800,color:B.b3,lineHeight:1}}>{lyCmPct5.toFixed(1)}%</div></div>
                      {cmChg!==null && <span style={{fontSize:11,fontWeight:700,padding:'3px 8px',borderRadius:6,background:cmChg>=0?'rgba(34,197,94,.12)':'rgba(239,68,68,.12)',color:cmChg>=0?'#4ade80':'#f87171',marginBottom:4}}>{cmChg>=0?'▲':'▼'}{Math.abs(cmChg).toFixed(1)}pt</span>}
                    </div>
                    <div style={{height:1,background:'var(--brd)',marginBottom:10}}/>
                    {[
                      ['ROAS',   fX(m.roas||0),  B.t2],
                      ['TACOS',  fP(m.tacos||0), (m.tacos||0)>0.16?'#f59e0b':B.t3],
                      ['AUR TY', f$(mtdM.aur||(rev5/(mtdM.units||1))), 'var(--txt)'],
                      ['AUR LY', f$(mtdM.ly_aur||(lyRev5/(mtdM.ly_units||1))), B.sub],
                    ].map(([lbl,val,col])=>(
                      <div key={lbl} style={{display:'flex',justifyContent:'space-between',padding:'5px 0',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                        <span style={{fontSize:11,color:'var(--txt3)'}}>{lbl}</span>
                        <span style={{fontSize:13,fontWeight:700,color:col}}>{val}</span>
                      </div>
                    ))}
                  </div>
                );
              })()}
            </div>
          </>
        );
      })()}

      {/* PERIOD COMPARISON COLUMNS */}
      {viewTab !== 'Custom' && viewTab !== 'Executive' && viewTab !== 'Daily' && periodCols && (
        <div style={{display:'grid',gridTemplateColumns:'minmax(380px,2.1fr) repeat(4,minmax(185px,1fr))',gap:8,overflowX:'auto',paddingBottom:6,marginBottom:24}}>
          {periods.slice(0,5).map(p => {
            const d = periodCols[p] || {};
            const pct = (ty, lyv) => (!lyv || !ty) ? null : ((ty - lyv) / lyv * 100);
            const pctEl = (delta, inv, key) => {
              if (delta == null) return <span key={key} style={{fontSize:9,color:'var(--txt3)',textAlign:'left'}}>—</span>;
              const pos = inv ? delta < 0 : delta > 0;
              return (
                <span key={key} style={{fontSize:9,fontWeight:700,color:pos?'#4ade80':'#fb923c',whiteSpace:'nowrap',textAlign:'left'}}>
                  {delta>0?'▲':'▼'} {Math.abs(delta).toFixed(1)}%
                </span>
              );
            };

            // ── TODAY: wider card — TY NOW | LY NOW | CHG | TY FCST | LY EOD | vs LY% ──
            if (p === 'Today') {
              const lyEod   = d.ly_eod_sales ?? d.ly_sales;
              const lyEodU  = d.ly_eod_units ?? d.ly_units;
              const lyNow   = d.ly_same_time_sales;
              const lyNowU  = d.ly_same_time_units;
              const lyNowO  = d.ly_same_time_orders || null;
              const lyNowAur  = lyNowU  > 0 ? lyNow  / lyNowU  : null;
              const lyEodAur  = lyEodU  > 0 ? lyEod  / lyEodU  : (d.ly_aur || null);
              const tyProjAur = (d.ty_units_forecast > 0 && d.ty_forecast > 0) ? d.ty_forecast / d.ty_units_forecast : null;
              // rows: [label, TY, LY-now, delta, invert, TY-FCST, LY-EOD]
              // Returns row uses objects {amt, units} for TY and LY-EOD so flatMap can render two-line
              const todayRows = [
                ['Sales $',    d.sales,            lyNow,        pct(d.sales, lyNow),           false, d.ty_forecast,         lyEod],
                ['Units',      d.units,             lyNowU,       pct(d.units, lyNowU),           false, d.ty_units_forecast,   lyEodU],
                ['AUR',        d.aur,               lyNowAur,     pct(d.aur, lyNowAur),           false, tyProjAur,             lyEodAur],
                ['Amzn Fees',  d.amazon_fees,       null,         null,                           true,  null,                  d.ly_amazon_fees],
                ['Returns',    {amt:d.returns_amount||0, units:d.returns||0},
                               null, null, true,
                               null,
                               {amt:d.ly_returns_amount||0, units:d.ly_returns||0}],
                ['Orders',     d.orders,            lyNowO,       pct(d.orders, lyNowO),          false, null,                  d.ly_orders],
                ['Sessions',   null,                null,         null,                           false, null,                  null],
                ['Conv %',     d.sessions > 0 ? d.conversion : null, null, null, false, null, d.ly_conversion],
              ];
              // Format helpers
              const fmt = (l, v) => {
                if (v == null || v === 0 && l === 'Sessions') return '—';
                if (l === 'Sales $' || l === 'Amzn Fees') return f$(v);
                if (l === 'AUR') return f$(v);
                if (l === 'Conv %') return fP(v);
                return fN(v);
              };
              return (
                <div key={p} style={{background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:12,padding:'10px 12px',transition:'background .3s'}}>
                  {/* Title row — fixed 30px to match regular cards */}
                  <div style={{height:30,display:'flex',alignItems:'center',gap:8,borderBottom:'1px solid var(--brd)',marginBottom:8}}>
                    <span style={{fontSize:10,fontWeight:700,textTransform:'uppercase',letterSpacing:'.12em',color:B.b2,whiteSpace:'nowrap'}}>Today</span>
                    <span style={{fontSize:9,color:'var(--txt3)',whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>
                      {d.snapshot_time ? `thru ${d.snapshot_time}` : 'so far'}{' · LY = same time'}
                    </span>
                  </div>
                  {/* Two side-by-side group boxes. Fixed row heights align with sibling cards. */}
                  {(() => {
                    const ROW_H = 26, RET_H = 34;
                    const HDR_H = 20;
                    const hdr = {height:HDR_H,display:'flex',alignItems:'center',fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.04em'};
                    const rowH   = (isRet) => ({height:isRet?RET_H:ROW_H,display:'flex',alignItems:'center',flexShrink:0});
                    const retH   = (isRet) => ({height:isRet?RET_H:ROW_H,display:'flex',flexDirection:'column',justifyContent:'center',gap:1,flexShrink:0});
                    const retCell = (l, val, baseStyle, amtStyle, unitsStyle) => {
                      const isRet = l === 'Returns';
                      return isRet
                        ? <div style={retH(true)}>
                            <span style={amtStyle}>{val.amt > 0 ? f$(val.amt) : '—'}</span>
                            {val.units > 0 && <span style={unitsStyle}>{val.units} units</span>}
                          </div>
                        : <span style={{...rowH(false),...baseStyle}}>{fmt(l, val)}</span>;
                    };
                    return (
                      <div style={{display:'flex',gap:8,alignItems:'flex-start'}}>

                        {/* ── GROUP 1: Label | TY NOW | LY NOW | CHG ── */}
                        <div style={{flex:'0 0 auto',background:'rgba(46,207,170,.05)',border:'1px solid rgba(46,207,170,.18)',borderRadius:8,padding:'5px 7px'}}>
                          <div style={{fontSize:8,fontWeight:700,color:'var(--acc1)',textTransform:'uppercase',letterSpacing:'.06em',textAlign:'center',marginBottom:4}}>TODAY LIVE</div>
                          <div style={{display:'grid',gridTemplateColumns:'60px 54px 50px 34px',columnGap:3,rowGap:0,alignItems:'start'}}>
                            {/* headers */}
                            <span style={{height:20}}/>
                            <span style={{...hdr,color:'var(--txt3)'}}>TY NOW</span>
                            <span style={{...hdr,color:B.b3}}>LY NOW</span>
                            <span style={{...hdr,color:'var(--txt3)'}}>CHG</span>
                            {/* data rows */}
                            {todayRows.map(([l, ty, lyNowV, delta, inv]) => {
                              const isRet = l === 'Returns';
                              return [
                                <span key={l+'-l'} style={{...rowH(isRet),fontSize:10,color:'var(--txt3)',overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{l}</span>,
                                retCell(l, ty,
                                  {fontSize:11,fontWeight:600,color:'var(--txt)'},
                                  {fontSize:11,fontWeight:600,color:'var(--txt)'},
                                  {fontSize:8,color:'var(--txt3)'}),
                                isRet
                                  ? <span key={l+'-ln'} style={rowH(true)}>—</span>
                                  : <span key={l+'-ln'} style={{...rowH(false),fontSize:10,color:B.b3}}>{lyNowV != null ? fmt(l, lyNowV) : '—'}</span>,
                                <span key={l+'-chg'} style={rowH(isRet)}>{pctEl(delta, inv, l+'-chg-inner')}</span>,
                              ];
                            })}
                          </div>
                        </div>

                        {/* ── GROUP 2: TY FCST | LY EOD | CHG ── */}
                        <div style={{flex:'0 0 auto',background:'rgba(245,158,11,.05)',border:'1px solid rgba(245,158,11,.18)',borderRadius:8,padding:'5px 7px'}}>
                          <div style={{fontSize:8,fontWeight:700,color:B.t2,textTransform:'uppercase',letterSpacing:'.06em',textAlign:'center',marginBottom:4}}>TODAY FORECAST</div>
                          <div style={{display:'grid',gridTemplateColumns:'54px 50px 34px',columnGap:3,rowGap:0,alignItems:'start'}}>
                            {/* headers */}
                            <span style={{...hdr,color:B.t2}}>TY FCST</span>
                            <span style={{...hdr,color:'var(--txt3)'}}>LY EOD</span>
                            <span style={{...hdr,color:'var(--txt3)'}}>CHG</span>
                            {/* data rows */}
                            {todayRows.map(([l, ty, , , inv, fcst, lyEodV]) => {
                              const isRet = l === 'Returns';
                              return [
                                isRet
                                  ? <span key={l+'-fc'} style={{...rowH(true),fontSize:11,fontWeight:700,color:B.t2}}>—</span>
                                  : <span key={l+'-fc'} style={{...rowH(false),fontSize:11,fontWeight:700,color:B.t2}}>{fcst != null ? fmt(l, fcst) : '—'}</span>,
                                retCell(l, lyEodV,
                                  {fontSize:10,color:'var(--txt2)'},
                                  {fontSize:10,color:'var(--txt2)'},
                                  {fontSize:8,color:'var(--txt3)'}),
                                <span key={l+'-vs'} style={rowH(isRet)}>{isRet ? pctEl(pct(ty.amt, lyEodV.amt), inv, l+'-vs-inner') : pctEl(pct(fcst, lyEodV), inv, l+'-vs-inner')}</span>,
                              ];
                            })}
                          </div>
                        </div>

                      </div>
                    );
                  })()}
                </div>
              );
            }

            // ── All other periods: standard 4-column layout ──
            // Returns row: {amt, units} object so flatMap can render two-line cell
            const rows = [
              ['Sales $',    d.sales,         d.ly_sales,         pct(d.sales, d.ly_sales),         false],
              ['Units',      d.units,          d.ly_units,         pct(d.units, d.ly_units),          false],
              ['AUR',        d.aur,            d.ly_aur,           pct(d.aur, d.ly_aur),              false],
              ['Amzn Fees',  d.amazon_fees,    d.ly_amazon_fees,   pct(d.amazon_fees, d.ly_amazon_fees), true],
              ['Returns',    {amt:d.returns_amount||0, units:d.returns||0},
                             {amt:d.ly_returns_amount||0, units:d.ly_returns||0},
                             pct(d.returns_amount||0, d.ly_returns_amount||0), true],
              ['Orders',     d.orders,         d.ly_orders,        pct(d.orders, d.ly_orders),        false],
              ['Sessions',   d.sessions,       d.ly_sessions,      pct(d.sessions, d.ly_sessions),    false],
              ['Conv %',     d.conversion,     d.ly_conversion,    pct(d.conversion, d.ly_conversion), false],
            ];
            const fmtP = (l, v) => {
              if (v == null) return '—';
              if (l === 'Sales $' || l === 'Amzn Fees') return f$(v);
              if (l === 'AUR') return f$(v);
              if (l === 'Conv %') return fP(v);
              if (l === 'Sessions' && v === 0) return '—';
              return fN(v);
            };
            return (
              <div key={p} style={{background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:12,padding:'10px 12px',transition:'background .3s'}}>
                {/* Title row — fixed 30px to match Today card */}
                <div style={{height:30,display:'flex',alignItems:'center',borderBottom:'1px solid var(--brd)',marginBottom:8}}>
                  <span style={{fontSize:10,fontWeight:700,textTransform:'uppercase',letterSpacing:'.12em',color:B.b2}}>{p}</span>
                </div>
                <div style={{display:'grid',gridTemplateColumns:'68px 52px 48px 44px',columnGap:3,rowGap:0,alignItems:'start'}}>
                  <span style={{height:20}}/>
                  <span style={{height:20,display:'flex',alignItems:'center',fontSize:9,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.06em'}}>TY</span>
                  <span style={{height:20,display:'flex',alignItems:'center',fontSize:9,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.06em'}}>LY</span>
                  <span style={{height:20,display:'flex',alignItems:'center',fontSize:9,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.06em'}}>Chg</span>
                  {rows.flatMap(([l, ty, lyv, delta, inv]) => {
                    const isRet = l === 'Returns';
                    const rowH = {height: isRet ? 34 : 26, flexShrink:0, display:'flex', alignItems:'center'};
                    const retH = {height: isRet ? 34 : 26, flexShrink:0, display:'flex', flexDirection:'column', justifyContent:'center', gap:1};
                    const retCell = (val, amtStyle, unitsStyle) => isRet
                      ? <div style={retH}>
                          <span style={amtStyle}>{val.amt > 0 ? f$(val.amt) : '—'}</span>
                          {val.units > 0 && <span style={unitsStyle}>{val.units} units</span>}
                        </div>
                      : <span style={{...rowH,...amtStyle}}>{fmtP(l, val)}</span>;
                    return [
                      <span key={l+'-l'} style={{...rowH,fontSize:10,color:'var(--txt3)',overflow:'hidden',textOverflow:'ellipsis',whiteSpace:'nowrap'}}>{l}</span>,
                      retCell(ty, {fontSize:11,fontWeight:400,color:'var(--txt)'}, {fontSize:8,color:'var(--txt3)'}),
                      retCell(lyv, {fontSize:10,color:'var(--txt2)'}, {fontSize:8,color:'var(--txt3)'}),
                      <span key={l+'-chg'} style={rowH}>{pctEl(delta, inv, l+'-chg-inner')}</span>,
                    ];
                  })}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* HOURLY SALES HEATMAP — 30 days × 24 hours */}
      {viewTab === 'Sales Summary' && (() => {
        // ── Color scale (same as 26-week heatmap) ──
        const hmStops = [
          [21,37,62],[15,82,115],[13,115,119],[34,139,34],
          [218,165,32],[230,101,30],[214,40,57],
        ];
        const hmColor = (t) => {
          if (t <= 0) return 'rgb(21,37,62)';
          if (t >= 1) return 'rgb(214,40,57)';
          const seg = t * (hmStops.length - 1);
          const i = Math.floor(seg), f = seg - i;
          const a = hmStops[i], b2 = hmStops[Math.min(i+1,hmStops.length-1)];
          return `rgb(${Math.round(a[0]+(b2[0]-a[0])*f)},${Math.round(a[1]+(b2[1]-a[1])*f)},${Math.round(a[2]+(b2[2]-a[2])*f)})`;
        };

        const HOUR_LABELS = [
          '12am','1am','2am','3am','4am','5am',
          '6am','7am','8am','9am','10am','11am',
          '12pm','1pm','2pm','3pm','4pm','5pm',
          '6pm','7pm','8pm','9pm','10pm','11pm',
        ];
        const CELL_W = 33; // px per day column
        const CELL_H = 21; // px per hour row
        const LABEL_W = 38; // px for hour label column

        const hmDays = hmData?.days || [];
        const maxVal  = hmMetric === '$' ? (hmData?.maxSales || 0) : (hmData?.maxUnits || 0);
        const sqrtMax = Math.sqrt(maxVal || 1);

        const visHours = Array.from({length:24},(_,i)=>i)
          .filter(h => !(hideNight && h < 6))
          .filter(h => !(hideLate  && h >= 18));

        const fCell = (v) => {
          if (v == null || v === 0) return '';
          if (hmMetric === '$') {
            if (v >= 1000) return `$${(v/1000).toFixed(1)}k`;
            if (v >= 100) return `$${Math.round(v)}`;
            return `$${Math.round(v)}`;
          }
          if (v >= 1000) return `${(v/1000).toFixed(1)}k`;
          return String(v);
        };

        const pillBtn = (label, active, onClick) => (
          <button key={label} onClick={onClick} style={{
            padding:'3px 10px',borderRadius:6,fontSize:10,fontWeight:600,cursor:'pointer',transition:'all .15s',
            border:`1px solid ${active ? B.b2 : 'var(--brd)'}`,
            background: active ? `${B.b1}33` : 'transparent',
            color: active ? B.b3 : 'var(--txt3)',
          }}>{label}</button>
        );

        // New layout: days = rows (last 7), hours = columns, labels at bottom
        const HCOL_W = 26;  // px per hour column (24 cols × 26 = 624 + 72 label = ~700px)
        const DLW    = 72;  // px for day-label column
        const DRH    = 28;  // px row height
        const last7  = hmDays.slice(-7);

        return (
          <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:'14px 16px',marginBottom:12}}>
            {/* Header */}
            <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:10,flexWrap:'wrap',gap:8}}>
              <span style={{fontSize:13,fontWeight:700,color:'var(--txt)'}}>Hourly Sales — Last 7 Days</span>
              <div style={{display:'flex',gap:5,alignItems:'center',flexWrap:'wrap'}}>
                {pillBtn('$', hmMetric==='$', ()=>setHmMetric('$'))}
                {pillBtn('Units', hmMetric==='units', ()=>setHmMetric('units'))}
                <div style={{width:1,height:16,background:'var(--brd)',margin:'0 2px'}}/>
                {pillBtn(hideNight?'Show 12–5am':'Hide 12–5am', hideNight, ()=>setHideNight(v=>!v))}
                {pillBtn(hideLate ?'Show 6–11pm':'Hide 6–11pm',  hideLate,  ()=>setHideLate(v=>!v))}
              </div>
            </div>

            {errors.hmData && <div style={{padding:'10px 14px',color:'#fb923c',fontSize:11,background:'rgba(251,146,60,.08)',border:'1px solid rgba(251,146,60,.18)',borderRadius:8,marginBottom:8}}>⚠ {errors.hmData}</div>}

            {loading.hmData ? <Spinner/> : last7.length === 0 ? (
              <div style={{padding:'24px',textAlign:'center',color:B.sub,fontSize:12}}>No hourly data yet — data accumulates over time</div>
            ) : (
              <div style={{overflowX:'auto',overflowY:'visible'}}>
                {/* Day rows */}
                {last7.map(d => (
                  <div key={d.date} style={{display:'flex',alignItems:'center',marginBottom:2}}>
                    {/* Day label */}
                    <div style={{
                      width:DLW, flexShrink:0, paddingRight:8, textAlign:'right',
                    }}>
                      <div style={{fontSize:9,fontWeight:d.isToday?700:500,color:d.isToday?B.b3:'var(--txt2)',lineHeight:'1.2',whiteSpace:'nowrap'}}>
                        {d.dayOfWeek}
                      </div>
                      <div style={{fontSize:8,color:d.isToday?B.b2:B.sub,lineHeight:'1.2',whiteSpace:'nowrap'}}>
                        {d.label}
                      </div>
                    </div>
                    {/* Hour cells */}
                    {visHours.map(h => {
                      const cell = (d.hours||{})[h] || {};
                      const val = hmMetric === '$' ? cell.sales : cell.units;
                      const isFuture = (val === null || val === undefined);
                      const numVal = isFuture ? 0 : (Number(val) || 0);
                      const pct = (numVal > 0) ? Math.sqrt(numVal) / sqrtMax : 0;
                      const bgColor = isFuture ? 'rgb(21,37,62)' : (numVal > 0 ? hmColor(pct) : 'rgb(21,37,62)');
                      const opacity = isFuture ? 0.06 : (numVal > 0 ? 0.55 + pct*0.45 : 0.16);
                      const txtColor = pct > 0.5 ? '#ffffff' : '#7a9bbf';
                      const tipLabel = `${d.label} ${HOUR_LABELS[h]}`;
                      const tipVal = hmMetric === '$' ? f$(numVal) : `${numVal} units`;
                      return (
                        <div key={h}
                          title={isFuture ? `${tipLabel}: future` : `${tipLabel}: ${tipVal}`}
                          style={{
                            width: HCOL_W-2, height: DRH, marginRight:2,
                            background: bgColor, opacity,
                            borderRadius:3, flexShrink:0,
                            display:'flex',alignItems:'center',justifyContent:'center',
                            fontSize:7, color:txtColor, fontWeight:600,
                            overflow:'hidden', cursor:'default',
                            border: d.isToday ? `1px solid ${B.b2}44` : 'none',
                          }}>
                          {!isFuture && numVal > 0 && fCell(numVal)}
                        </div>
                      );
                    })}
                  </div>
                ))}

                {/* Hour labels at bottom */}
                <div style={{display:'flex',marginLeft:DLW,marginTop:4}}>
                  {visHours.map(h => (
                    <div key={h} style={{
                      width:HCOL_W-2, marginRight:2, flexShrink:0,
                      textAlign:'center', fontSize:7, color:B.sub,
                      fontVariantNumeric:'tabular-nums', lineHeight:'1.2',
                    }}>
                      {HOUR_LABELS[h]}
                    </div>
                  ))}
                </div>

                {/* Footer */}
                <div style={{display:'flex',justifyContent:'space-between',marginTop:8,marginLeft:DLW}}>
                  <div style={{display:'flex',gap:10,alignItems:'center'}}>
                    <div style={{display:'flex',alignItems:'center',gap:3}}>
                      <span style={{fontSize:8,color:B.sub}}>Low</span>
                      {[0,.17,.33,.5,.67,.83,1].map(t => (
                        <div key={t} style={{width:10,height:10,borderRadius:2,background:t===0?'rgb(21,37,62)':hmColor(t),opacity:0.55+t*0.45}}/>
                      ))}
                      <span style={{fontSize:8,color:B.sub}}>High</span>
                    </div>
                    <span style={{fontSize:8,color:B.sub}}>· Today outlined in blue</span>
                  </div>
                  <span style={{fontSize:8,color:B.sub,alignSelf:'flex-end'}}>
                    Updated: {hmData?.lastUpdated ? hmData.lastUpdated.replace('T',' ') : '—'}
                  </span>
                </div>
              </div>
            )}
          </div>
        );
      })()}

      {/* PERIOD PILLS — directly above Sales Overview KPIs */}
      {viewTab !== 'Custom' && viewTab !== 'Daily' && (
        <div className="ppill-bar" style={{marginBottom:14}}>
          {periods.map(p => (
            <button key={p} className={`ppill${activePeriod===p?' active':''}`} onClick={() => setActivePeriod(p)}>
              {p}
            </button>
          ))}
        </div>
      )}

      {viewTab !== 'Daily' && <>
      {/* ══ SALES OVERVIEW ══════════════════════════════════════════ */}
      <SectionDivider label="Sales Overview"/>
      <div style={{display:'flex',gap:8,marginBottom:14,overflowX:'auto',paddingBottom:2}}>
        {loading.metrics ? <Spinner/> : <>
          <MetricCard label="Sales $"       value={f$(m.sales)}         ly={f$(ly('sales'))}         delta={dp(m.sales,ly('sales'))}/>
          <MetricCard label="Unit Sales"    value={fN(m.unit_sales)}     ly={fN(ly('unit_sales'))}    delta={dp(m.unit_sales,ly('unit_sales'))}/>
          <MetricCard label="AUR"           value={f$(m.aur)}            ly={f$(ly('aur'))}           delta={dp(m.aur,ly('aur'))}/>
          <MetricCard label="COGS"          value={f$(m.cogs)}           ly={f$(ly('cogs'))}          delta={dp(m.cogs,ly('cogs'))}/>
          <MetricCard label="Amazon Fees"   value={f$(m.amazon_fees)}    ly={f$(ly('amazon_fees'))}   delta={dp(m.amazon_fees,ly('amazon_fees'))}
            expandContent={feeBreak && (feeBreak.items || Array.isArray(feeBreak)) && <div>{(feeBreak.items || feeBreak).map(f=><div key={f.type} style={{display:'flex',justifyContent:'space-between',fontSize:10,marginBottom:5}}><span style={{color:'var(--txt3)'}}>{f.type}</span><span style={{fontWeight:600,color:'var(--txt)'}}>{f$(f.amount)}</span></div>)}{feeBreak.estimated && <div style={{fontSize:9,color:'#f59e0b',marginTop:4,fontStyle:'italic'}}>* Estimated (settlement data pending)</div>}</div>}/>
          <MetricCard label="Returns" value={`${fN(m.returns)} · ${f$(m.returns_amount)}`} ly={`${fN(ly('returns'))} · ${f$(ly('returns_amount'))}`} delta={dp(m.returns,ly('returns'))} invert/>
          <MetricCard label="Gross Margin $"  value={f$(m.gross_margin)}     ly={f$(ly('gross_margin'))}     delta={dp(m.gross_margin,ly('gross_margin'))}/>
          <MetricCard label="Gross Margin %"  value={fP(m.gross_margin_pct)} ly={fP(ly('gross_margin_pct'))} delta={dp(m.gross_margin_pct,ly('gross_margin_pct'))}/>
        </>}
      </div>

      {/* Monthly Revenue YOY — independent chart, NOT controlled by the Charts period bar below */}
      <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:16,marginBottom:12,transition:'background .3s'}}>
        <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:12,flexWrap:'wrap',gap:8}}>
          {/* Left: title + year toggles */}
          <div style={{display:'flex',gap:6,alignItems:'center',flexWrap:'wrap'}}>
            <span style={{fontSize:12,fontWeight:700,color:'var(--txt)',marginRight:4}}>Monthly Revenue</span>
            <div style={{width:1,height:14,background:'var(--brd)',margin:'0 2px'}}/>
            {[['2024','y2024',B.dim],['2025','y2025',B.b2],['2026','y2026',B.o2]].map(([lbl,key,col])=>(
              <button key={key} onClick={()=>setYearVis(v=>({...v,[key]:!v[key]}))} style={{
                fontSize:10,fontWeight:600,padding:'3px 10px',borderRadius:6,cursor:'pointer',
                border:`1px solid ${yearVis[key]?col:'var(--brd)'}`,
                background:yearVis[key]?`${col}22`:'transparent',
                color:yearVis[key]?col:'var(--txt3)',transition:'all .15s',
              }}>{lbl}</button>
            ))}
          </div>
          {/* Right: FY summary boxes — same row as year toggles */}
          <div style={{display:'flex',gap:5,alignItems:'center',flexWrap:'wrap'}}>
            {yearVis.y2024 && fy24Total>0 && (
              <div style={{background:'#0b1829',border:`1px solid ${B.dim}55`,borderRadius:7,padding:'3px 11px',textAlign:'center',minWidth:58}}>
                <div style={{fontSize:7,color:B.sub,lineHeight:'1.4',letterSpacing:'.05em'}}>FY 24</div>
                <div style={{fontSize:12,fontWeight:700,color:B.dim,lineHeight:'1.3'}}>{f$(fy24Total)}</div>
              </div>
            )}
            {yearVis.y2025 && fy25Total>0 && (
              <div style={{background:'#0b1829',border:`1px solid ${B.b2}55`,borderRadius:7,padding:'3px 11px',textAlign:'center',minWidth:58}}>
                <div style={{fontSize:7,color:B.sub,lineHeight:'1.4',letterSpacing:'.05em'}}>FY 25</div>
                <div style={{fontSize:12,fontWeight:700,color:B.b2,lineHeight:'1.3'}}>{f$(fy25Total)}</div>
              </div>
            )}
            {yearVis.y2026 && ytd26>0 && (
              <div style={{background:'#0b1829',border:`1px solid ${B.o2}55`,borderRadius:7,padding:'3px 11px',textAlign:'center',minWidth:58}}>
                <div style={{fontSize:7,color:B.sub,lineHeight:'1.4',letterSpacing:'.05em'}}>&apos;26 YTD</div>
                <div style={{fontSize:12,fontWeight:700,color:B.o2,lineHeight:'1.3'}}>{f$(ytd26)}</div>
              </div>
            )}
            {yearVis.y2026 && proj26>ytd26 && (
              <div style={{background:'#0b1829',border:'1px solid #f59e0b55',borderRadius:7,padding:'3px 11px',textAlign:'center',minWidth:58}}>
                <div style={{fontSize:7,color:B.sub,lineHeight:'1.4',letterSpacing:'.05em'}}>&apos;26 Proj</div>
                <div style={{fontSize:12,fontWeight:700,color:'#f59e0b',lineHeight:'1.3'}}>{f$(proj26)}</div>
              </div>
            )}
          </div>
        </div>
        {errors.yoy && <div style={{padding:'12px 14px',color:'#fb923c',fontSize:11,background:'rgba(251,146,60,.08)',border:'1px solid rgba(251,146,60,.18)',borderRadius:8}}>⚠ {errors.yoy}</div>}
        {loading.yoy ? <Spinner/> : <SVGTip html={yoyBarSVG(yoyMonths, forecastMap, yearVis, 1100, 195, yoyMeta?.current_month)}/>}
      </div>

      {/* Sales Charts period bar — controls trend/rolling charts only, NOT the monthly revenue chart above */}
      <div style={{display:'flex',alignItems:'center',gap:10,flexWrap:'wrap',margin:'6px 0 10px'}}>
        <PeriodBar value={cpSales} onChange={setCpSales}/>
        <div style={{fontSize:10,color:'var(--txt3)',fontWeight:600,textTransform:'uppercase',letterSpacing:'.08em'}}>Trend Charts</div>
      </div>

      {/* Sales $ + AUR Trend — dual Y-axis: Sales (left, blue) + AUR (right, green) */}
      {viewTab !== 'Executive' && <ChartCard title="Sales $ & AUR Trend" badge={cpSales} error={errors.trend}>
        {loading.trend ? <Spinner/> : <>
          {svgChart(salesAurSVG(toArr(trend)))}
          <Legend items={[['Sales TY','#2E6FBB'],['Sales LY','#5B9FD4',true],['AUR TY','#22c55e'],['AUR LY','#16a34a',true]]}/>
        </>}
      </ChartCard>}


      {/* ══ TRAFFIC & CONVERSION ════════════════════════════════════ */}
      {viewTab !== 'Executive' && <>
      <SectionDivider label="Traffic & Conversion"/>
      <div style={{display:'flex',gap:8,marginBottom:14,overflowX:'auto',paddingBottom:2}}>
        {loading.metricsTraffic ? <Spinner/> : (() => {
          const mt = metricsTraffic || {};
          const mlt = k => mt[`ly_${k}`]; // same pattern as main ly() helper
          return <>
            <MetricCard label="Sessions"     value={fN(mt.sessions)}    ly={fN(mlt('sessions'))}    delta={dp(mt.sessions,mlt('sessions'))}/>
            <MetricCard label="Glance Views" value={fN(mt.glance_views)} ly={fN(mlt('glance_views'))} delta={dp(mt.glance_views,mlt('glance_views'))}/>
            <MetricCard label="Click Through" value={fP(mt.ctr)}         ly={fP(mlt('ctr'))}         delta={dp(mt.ctr,mlt('ctr'))}/>
            <MetricCard label="Conversion"   value={fP(mt.conversion)}  ly={fP(mlt('conversion'))}  delta={dp(mt.conversion,mlt('conversion'))}/>
          </>;
        })()}
      </div>
      <div style={{display:'flex',alignItems:'center',gap:10,flexWrap:'wrap',margin:'6px 0 10px'}}>
        <PeriodBar value={cpTraffic} onChange={setCpTraffic}/>
        <div style={{fontSize:10,color:'var(--txt3)',fontWeight:600,textTransform:'uppercase',letterSpacing:'.08em'}}>Traffic Charts</div>
      </div>
      {/* Sessions trend + Conversion rate — dual Y-axis (sessions lines left, conv% bars right) */}
      <ChartCard title="Sessions & Conversion Rate" badge={cpTraffic} error={errors.trendTraffic}>
        {loading.trendTraffic ? <Spinner/> : <>
          {svgChart(sessionsConvSVG(toArr(trendTraffic)))}
          <Legend items={[['Sessions TY',B.o2],['Sessions LY',B.sub,true],['Conv% TY','#1AA392'],['Conv% LY','#1AA392',true]]}/>
        </>}
      </ChartCard>
      {/* Compact Conversion Pipeline summary */}
      {!loading.funnel && Array.isArray(funnel) && funnel.length > 0 && (() => {
        const stages = funnel;
        const colors = ['#5B9FD4','#2E6FBB','#1B4F8A','#E8821E','#1AA392'];
        const maxVal = Math.max(...stages.map(s => s.ty || 0), 1);
        // Compute step-through rates
        const rates = stages.map((s, i) => {
          if (i === 0) return null;
          const prev = stages[i-1].ty || 0;
          return prev > 0 ? ((s.ty || 0) / prev * 100).toFixed(1) : null;
        });
        // Find bottleneck (worst step-through rate)
        let minRate = Infinity, bottleneckI = -1;
        rates.forEach((r, i) => { if (r !== null && Number(r) < minRate) { minRate = Number(r); bottleneckI = i; } });
        return (
          <div style={{background:'var(--card)',border:'1px solid var(--border)',borderRadius:8,padding:'14px 18px',marginBottom:12}}>
            <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:12}}>
              <span style={{fontSize:11,fontWeight:700,color:'var(--txt3)',textTransform:'uppercase',letterSpacing:'.08em'}}>Conversion Pipeline</span>
              <a href="#advertising" style={{fontSize:10,color:B.b3,textDecoration:'none',fontWeight:600}} onClick={e=>{e.preventDefault();}}>Full Funnel →</a>
            </div>
            <div style={{display:'grid',gridTemplateColumns:`repeat(${stages.length},1fr)`,gap:10}}>
              {stages.map((s, i) => {
                const barW = maxVal > 0 ? Math.max(4, (s.ty / maxVal) * 100) : 0;
                const isBottleneck = i === bottleneckI;
                const stageColor = isBottleneck ? '#f59e0b' : colors[i] || B.b3;
                const lyDelta = s.ly > 0 ? ((s.ty - s.ly) / s.ly * 100).toFixed(1) : null;
                return (
                  <div key={s.label} style={{display:'flex',flexDirection:'column',gap:4}}>
                    <div style={{fontSize:10,color:isBottleneck?'#f59e0b':'var(--txt3)',fontWeight:isBottleneck?700:400,whiteSpace:'nowrap',overflow:'hidden',textOverflow:'ellipsis'}}>{isBottleneck ? '⚠ ' : ''}{s.label}</div>
                    <div style={{fontSize:16,fontWeight:700,color:stageColor}}>{(s.ty||0).toLocaleString()}</div>
                    {/* Bar */}
                    <div style={{height:5,background:'var(--surface)',borderRadius:3,overflow:'hidden'}}>
                      <div style={{width:`${barW}%`,height:'100%',background:`linear-gradient(90deg,${stageColor}99,${stageColor})`,borderRadius:3}}/>
                    </div>
                    {/* Step rate */}
                    {rates[i] !== null && (
                      <div style={{fontSize:10,color:isBottleneck?'#f59e0b':B.sub}}>{rates[i]}% step-through</div>
                    )}
                    {/* YoY delta */}
                    {lyDelta !== null && (
                      <div style={{fontSize:10,color:Number(lyDelta)>=0?'#4ade80':'#f87171',fontWeight:600}}>{Number(lyDelta)>=0?'+':''}{lyDelta}% vs LY</div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        );
      })()}

      <ChartCard title="Conversion Funnel vs LY" noMargin error={errors.funnel}>
        {loading.funnel ? <Spinner/> : svgChart(funnelSVG(toArr(funnel)))}
      </ChartCard>
      </>}

      {/* ══ ADVERTISING ═════════════════════════════════════════════ */}
      <SectionDivider label="Advertising"/>
      <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:10}}>
        <span style={{fontSize:9,color:B.sub,fontWeight:600,textTransform:'uppercase',letterSpacing:'.08em'}}>Period:</span>
        <span style={{fontSize:10,color:B.b3,fontWeight:700,background:`${B.b1}33`,border:`1px solid ${B.b2}44`,borderRadius:5,padding:'2px 8px'}}>{activePeriod}</span>
        <span style={{fontSize:9,color:B.sub}}>— same as KPI period above</span>
      </div>
      <div style={{display:'flex',gap:8,marginBottom:14,overflowX:'auto',paddingBottom:2}}>
        {loading.metrics ? <Spinner/> : <>
          <MetricCard label="Ad Spend $" value={f$(m.ad_spend)} ly={f$(ly('ad_spend'))} delta={dp(m.ad_spend,ly('ad_spend'))} invert
            expandContent={adBreak && Array.isArray(adBreak) && <div>{adBreak.map(a=><div key={a.type} style={{marginBottom:6}}><div style={{display:'flex',justifyContent:'space-between',fontSize:10}}><span style={{color:'var(--txt3)'}}>{a.type}</span><span style={{fontWeight:600,color:'var(--txt)'}}>{f$(a.spend)}</span></div><div style={{display:'flex',justifyContent:'space-between',fontSize:10,marginTop:2}}><span style={{color:'var(--txt3)',paddingLeft:8}}>ROAS</span><span style={{fontWeight:600,color:'#4ade80'}}>{fX(a.roas)}</span></div></div>)}</div>}/>
          <MetricCard label="ROAS"  value={fX(m.roas)}  ly={fX(ly('roas'))}  delta={dp(m.roas,ly('roas'))}/>
          <MetricCard label="TACOS" value={fP(m.tacos)} ly={fP(ly('tacos'))} delta={dp(m.tacos,ly('tacos'))} invert/>
        </>}
      </div>
      <ChartCard title="Ad Efficiency — ACOS vs ROAS Quadrant" badge={`${activePeriod} · TY vs LY`} noMargin error={errors.adEff}>
        {loading.adEff ? <Spinner/> : svgChart(adQuadrantSVG(adEff))}
      </ChartCard>

      {/* ══ INVENTORY HEALTH ════════════════════════════════════════ */}
      <SectionDivider label="Inventory Health"/>
      <ChartCard title="Days of Supply" noMargin>
        {(() => {
          const dos = m.dos || 0;
          const stockUnits = m.stock_units || 0;
          // Zone thresholds
          const zones = [
            { label:'Critical', max:14,  color:'#ef4444', bg:'rgba(239,68,68,.12)',   icon:'🔴' },
            { label:'Reorder',  max:30,  color:'#f59e0b', bg:'rgba(245,158,11,.12)',  icon:'🟡' },
            { label:'Healthy',  max:60,  color:'#22c55e', bg:'rgba(34,197,94,.12)',   icon:'🟢' },
            { label:'Surplus',  max:999, color:'#3b82f6', bg:'rgba(59,130,246,.12)',  icon:'🔵' },
          ];
          const zone = zones.find(z => dos <= z.max) || zones[3];
          // Track: 0-90 days, clamp pointer at 90
          const TRACK_MAX = 90;
          const pct = Math.min(100, (dos / TRACK_MAX) * 100);
          // Zone widths on track (14/30/60/90 → 15.5%/33.3%/66.7%/100%)
          const zonePcts = [14/TRACK_MAX*100, 30/TRACK_MAX*100, 60/TRACK_MAX*100, 100];
          const zoneColors = ['rgba(239,68,68,.35)','rgba(245,158,11,.35)','rgba(34,197,94,.35)','rgba(59,130,246,.35)'];
          return (
            <div>
              {/* KPI row */}
              <div style={{display:'flex',gap:16,marginBottom:14,flexWrap:'wrap'}}>
                <div>
                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.08em',color:B.sub,marginBottom:3}}>Current DOS</div>
                  <div style={{display:'flex',alignItems:'baseline',gap:5}}>
                    <span style={{fontSize:26,fontWeight:800,color:zone.color,lineHeight:1}}>{dos}</span>
                    <span style={{fontSize:13,fontWeight:600,color:zone.color}}>days</span>
                    <span style={{fontSize:10,fontWeight:700,padding:'2px 8px',borderRadius:10,
                      background:zone.bg,color:zone.color,border:`1px solid ${zone.color}44`,marginLeft:4}}>
                      {zone.icon} {zone.label}
                    </span>
                  </div>
                </div>
                <div style={{borderLeft:'1px solid var(--brd)',paddingLeft:16}}>
                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.08em',color:B.sub,marginBottom:3}}>Units on Hand</div>
                  <div style={{fontSize:22,fontWeight:700,color:B.b3,lineHeight:1}}>{fN(stockUnits)}</div>
                </div>
                <div style={{borderLeft:'1px solid var(--brd)',paddingLeft:16}}>
                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.08em',color:B.sub,marginBottom:3}}>Reorder Point</div>
                  <div style={{fontSize:22,fontWeight:700,color:B.o2,lineHeight:1}}>30d</div>
                </div>
              </div>

              {/* Color-coded track */}
              <div style={{position:'relative',height:18,borderRadius:9,overflow:'hidden',display:'flex',marginBottom:6}}>
                {zones.map((z,i)=>{
                  const w = i===0 ? zonePcts[0] : zonePcts[i]-zonePcts[i-1];
                  return <div key={i} style={{width:`${w}%`,height:'100%',background:zoneColors[i]}}/>;
                })}
                {/* Pointer */}
                <div style={{
                  position:'absolute',left:`${pct}%`,top:'50%',transform:'translate(-50%,-50%)',
                  width:4,height:22,background:zone.color,borderRadius:2,
                  boxShadow:`0 0 6px ${zone.color}`,zIndex:2,
                }}/>
              </div>

              {/* Zone labels below track */}
              <div style={{display:'flex',justifyContent:'space-between',marginBottom:12}}>
                <span style={{fontSize:8,color:'#ef4444',fontWeight:600}}>0 — 14d Critical</span>
                <span style={{fontSize:8,color:'#f59e0b',fontWeight:600}}>15 — 30d Reorder</span>
                <span style={{fontSize:8,color:'#22c55e',fontWeight:600}}>31 — 60d Healthy</span>
                <span style={{fontSize:8,color:'#3b82f6',fontWeight:600}}>61d+ Surplus</span>
              </div>

              {/* Threshold marker note */}
              <div style={{display:'flex',alignItems:'center',gap:6,background:'rgba(245,158,11,.07)',
                border:'1px solid rgba(245,158,11,.2)',borderRadius:7,padding:'5px 10px'}}>
                <span style={{fontSize:12}}>⚠️</span>
                <span style={{fontSize:10,color:B.o2,fontWeight:600}}>
                  {dos <= 14 ? 'Stock critically low — create shipment plan immediately'
                   : dos <= 30 ? 'Approaching reorder threshold — plan replenishment now'
                   : dos <= 60 ? 'Inventory healthy — monitor weekly'
                   : 'Overstocked — consider slowing replenishment'}
                </span>
              </div>
            </div>
          );
        })()}
      </ChartCard>
      </>}

    </div>
  );
}
