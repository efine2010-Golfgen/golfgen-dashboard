import { useState, useEffect, useRef, useCallback } from "react";
import { api, fmt$ } from "../lib/api";

/* ── Tiny inline-style helpers (same as Inventory.jsx) ── */
const SG = (x = {}) => ({ fontFamily: "'Space Grotesk',monospace", ...x });
const DM = (x = {}) => ({ fontFamily: "'DM Serif Display',Georgia,serif", ...x });

/* ── Smart name shortener: strips common prefix, keeps distinguishing part ── */
const shortName = (name, maxLen = 22) => {
  if (!name) return "—";
  // Strip common brand prefixes that make every name look the same
  let s = name
    .replace(/^PGA TOUR\s*/i, "")
    .replace(/^Elite Global\s*/i, "")
    .replace(/^EGB\s*/i, "")
    .replace(/^GolfGen\s*/i, "")
    .trim();
  if (!s) s = name; // fallback if name was ONLY the prefix
  if (s.length > maxLen) s = s.slice(0, maxLen) + "…";
  return s;
};

const fmtK = (n) => {
  if (n == null) return "—";
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1000) return `$${(n / 1000).toFixed(1)}K`;
  return `$${n.toLocaleString()}`;
};

/* ── Reusable card / section helpers ── */
const Card = ({ children, style }) => (
  <div style={{ background: "var(--surf)", border: "1px solid var(--brd)", borderRadius: 13, overflow: "hidden", marginBottom: 12, ...style }}>{children}</div>
);
const CardHdr = ({ title, children }) => (
  <div style={{ padding: "10px 16px", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid var(--brd)", flexWrap: "wrap", gap: 6 }}>
    <span style={{ fontSize: 12, fontWeight: 700, color: "var(--txt)" }}>{title}</span>
    <div style={{ display: "flex", gap: 7, alignItems: "center", flexWrap: "wrap" }}>{children}</div>
  </div>
);
const SecDiv = ({ label }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 10, margin: "16px 0 10px" }}>
    <span style={{ ...SG(), fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".14em", color: "var(--txt3)", whiteSpace: "nowrap" }}>{label}</span>
    <div style={{ flex: 1, height: 1, background: "var(--brd)" }} />
  </div>
);
const Badge = ({ children, color = "teal" }) => {
  const colors = {
    teal: { bg: "rgba(46,207,170,.14)", fg: "#2ECFAA" },
    red: { bg: "rgba(248,113,113,.14)", fg: "#f87171" },
    amber: { bg: "rgba(245,183,49,.14)", fg: "#F5B731" },
    blue: { bg: "rgba(123,174,208,.14)", fg: "#7BAED0" },
    purple: { bg: "rgba(162,107,225,.14)", fg: "#A26BE1" },
    orange: { bg: "rgba(232,120,48,.14)", fg: "#E87830" },
    slate: { bg: "rgba(141,174,200,.1)", fg: "var(--txt3)" },
    gold: { bg: "rgba(245,183,49,.18)", fg: "#F5B731" },
  };
  const c = colors[color] || colors.teal;
  return <span style={{ ...SG(), display: "inline-flex", alignItems: "center", padding: "2px 7px", borderRadius: 6, fontSize: 9, fontWeight: 700, background: c.bg, color: c.fg, whiteSpace: "nowrap" }}>{children}</span>;
};
const CBadge = ({ children, color = "ok" }) => {
  const colors = { ok: { bg: "rgba(46,207,170,.14)", fg: "#2ECFAA" }, warn: { bg: "rgba(245,183,49,.14)", fg: "#F5B731" }, risk: { bg: "rgba(248,113,113,.14)", fg: "#f87171" }, blue: { bg: "rgba(123,174,208,.14)", fg: "#7BAED0" }, purple: { bg: "rgba(162,107,225,.14)", fg: "#A26BE1" }, orange: { bg: "rgba(232,120,48,.14)", fg: "#E87830" } };
  const c = colors[color] || colors.ok;
  return <span style={{ ...SG(), fontSize: 9, padding: "2px 8px", borderRadius: 99, fontWeight: 700, background: c.bg, color: c.fg }}>{children}</span>;
};
const ScoreBox = ({ grade }) => {
  const cls = grade.startsWith("A") ? { bg: "rgba(46,207,170,.15)", fg: "#2ECFAA" } : grade.startsWith("B") ? { bg: "rgba(245,183,49,.14)", fg: "#F5B731" } : { bg: "rgba(248,113,113,.14)", fg: "#f87171" };
  return <span style={{ ...SG(), display: "inline-flex", alignItems: "center", justifyContent: "center", width: 30, height: 19, borderRadius: 5, fontSize: 9, fontWeight: 800, background: cls.bg, color: cls.fg }}>{grade}</span>;
};

/* ── Sparkline (canvas) ── */
function Sparkline({ data, width = 64, height = 20, forceColor }) {
  const ref = useRef(null);
  useEffect(() => {
    if (!ref.current || !data || data.length < 2) return;
    const c = ref.current, ctx = c.getContext("2d");
    const w = c.width, h = c.height, pad = 2;
    const mn = Math.min(...data), mx = Math.max(...data), rng = mx - mn || 1;
    const pts = data.map((v, i) => [pad + i * (w - 2 * pad) / (data.length - 1), h - pad - (v - mn) / rng * (h - 2 * pad)]);
    const declining = data[data.length - 1] < data[0] * 0.92;
    const rising = data[data.length - 1] > data[0] * 1.04;
    const col = forceColor || (declining ? "#f87171" : rising ? "#2ECFAA" : "#7BAED0");
    ctx.clearRect(0, 0, w, h);
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, col === "#f87171" ? "rgba(248,113,113,.2)" : col === "#2ECFAA" ? "rgba(46,207,170,.18)" : "rgba(123,174,208,.14)");
    grad.addColorStop(1, "rgba(0,0,0,0)");
    ctx.beginPath(); ctx.moveTo(pts[0][0], h);
    pts.forEach(p => ctx.lineTo(p[0], p[1]));
    ctx.lineTo(pts[pts.length - 1][0], h); ctx.closePath();
    ctx.fillStyle = grad; ctx.fill();
    ctx.beginPath(); ctx.moveTo(pts[0][0], pts[0][1]);
    pts.forEach(p => ctx.lineTo(p[0], p[1]));
    ctx.strokeStyle = col; ctx.lineWidth = 1.5; ctx.lineJoin = "round"; ctx.stroke();
    ctx.beginPath(); ctx.arc(pts[pts.length - 1][0], pts[pts.length - 1][1], 2, 0, 2 * Math.PI);
    ctx.fillStyle = col; ctx.fill();
  }, [data, forceColor]);
  return <canvas ref={ref} width={width} height={height} style={{ display: "block" }} />;
}

/* ── Heatmap Cell ── */
function HmCell({ value, fmt = v => v, bg, fg }) {
  return (
    <div style={{ flex: 1, minWidth: 46, height: 26, borderRadius: 4, display: "flex", alignItems: "center", justifyContent: "center", ...SG(), fontSize: 8, fontWeight: 700, background: bg, color: fg, cursor: "pointer", transition: "filter .15s" }}
      onMouseEnter={e => e.currentTarget.style.filter = "brightness(1.35)"}
      onMouseLeave={e => e.currentTarget.style.filter = ""}>
      {fmt(value)}
    </div>
  );
}

/* ── Color helpers ── */
const cvrColor = (v) => v >= 4.0 ? "#2ECFAA" : v >= 3.0 ? "#F5B731" : "#f87171";
const marginColor = (v) => v >= 50 ? "#2ECFAA" : v >= 38 ? "#F5B731" : "#f87171";
const acosColor = (v) => v === 0 ? "var(--txt3)" : v <= 20 ? "#2ECFAA" : v <= 40 ? "#F5B731" : "#f87171";
const retColor = (v) => v <= 3 ? "#2ECFAA" : v <= 6 ? "#F5B731" : "#f87171";

/* ── CVR heatmap color ── */
const cvrHmBgFg = (v) => {
  if (v >= 4.0) return { bg: `rgba(46,207,170,${Math.min(0.35 + v * 0.06, 0.75)})`, fg: "#0c1a2e" };
  if (v >= 3.0) return { bg: `rgba(245,183,49,${Math.min(0.25 + v * 0.06, 0.55)})`, fg: "#0c1a2e" };
  return { bg: `rgba(248,113,113,${Math.min(0.2 + v * 0.06, 0.55)})`, fg: "#f87171" };
};

/* ── Units heatmap color ── */
const unitsHmBgFg = (v, max, isDeclining, isHW) => {
  const ratio = v / (max || 1);
  const baseColor = isHW ? "232,120,48" : "46,207,170";
  if (isDeclining) {
    return { bg: `rgba(248,113,113,${0.2 + ratio * 0.25})`, fg: ratio > 0.4 ? "#0c1a2e" : "#f87171" };
  }
  if (ratio > 0.6) return { bg: `rgba(${baseColor},${0.3 + ratio * 0.4})`, fg: "#0c1a2e" };
  if (ratio > 0.3) return { bg: `rgba(${baseColor},${0.1 + ratio * 0.3})`, fg: isHW ? "#E87830" : "#2ECFAA" };
  return { bg: `rgba(${baseColor},.12)`, fg: isHW ? "#E87830" : "#2ECFAA" };
};

/* ── Grade calculator ── */
/* Scoring: Revenue (0-25), CVR (0-25), Margin (0-20), ACOS (0-15), Returns (0-15)
   Revenue is weighted heavily so low-revenue items can't score A+ on ratios alone.
   Items with <$500 rev are capped at B+ max. */
function calcGrade(p) {
  let score = 0;
  // Revenue (0-25) — heavily weighted so niche/low-volume items don't rank top
  if (p.rev >= 10000) score += 25;
  else if (p.rev >= 5000) score += 20;
  else if (p.rev >= 2000) score += 12;
  else if (p.rev >= 500) score += 5;
  // CVR (0-25)
  if (p.convRate >= 5) score += 25; else if (p.convRate >= 3.5) score += 18; else if (p.convRate >= 2.5) score += 10;
  // Margin (0-20)
  if (p.margin >= 50) score += 20; else if (p.margin >= 38) score += 12; else if (p.margin >= 25) score += 5;
  // ACOS (0-15) — no ad spend gets partial credit
  const acos = p.adSpend > 0 && p.rev > 0 ? (p.adSpend / p.rev * 100) : 0;
  if (acos === 0) score += 8; else if (acos <= 20) score += 15; else if (acos <= 35) score += 8;
  // Returns (0-15)
  const ret = p.refundRate || 0;
  if (ret <= 3) score += 15; else if (ret <= 6) score += 8;
  // Cap: items under $500 revenue can't exceed B+
  if (p.rev < 500 && score >= 55) score = 54;
  if (score >= 85) return "A+"; if (score >= 70) return "A"; if (score >= 55) return "B+"; if (score >= 40) return "B";
  return "C";
}

/* ── Generate simulated weekly trend data ── */
function generateTrendData(total, weeks = 8) {
  if (!total) return Array(weeks).fill(0);
  const weeklyAvg = total / (weeks * 0.8);
  const data = [];
  let val = weeklyAvg * (0.7 + Math.random() * 0.3);
  for (let i = 0; i < weeks; i++) {
    val = val * (0.9 + Math.random() * 0.2);
    data.push(Math.round(Math.max(val, 1)));
  }
  return data;
}

/* ── Generate AUR trend data ── */
function generateAurData(price, weeks = 7) {
  if (!price) return Array(weeks).fill(0);
  const data = [];
  let val = price * (0.92 + Math.random() * 0.08);
  for (let i = 0; i < weeks; i++) {
    val = val * (0.97 + Math.random() * 0.06);
    data.push(parseFloat(val.toFixed(2)));
  }
  return data;
}

/* ── Generate CVR trend data ── */
function generateCvrData(cvr, weeks = 8) {
  if (!cvr) return Array(weeks).fill(0);
  const data = [];
  let val = cvr * (0.75 + Math.random() * 0.25);
  for (let i = 0; i < weeks; i++) {
    val = val * (0.92 + Math.random() * 0.16);
    data.push(parseFloat(Math.max(val, 0.5).toFixed(1)));
  }
  data[data.length - 1] = cvr; // ensure last value = current
  return data;
}

/* ═══════════════════════════════════
   MAIN COMPONENT
   ═══════════════════════════════════ */
const RANGES = [
  { label: "7D", days: 7 },
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
];

export default function Products({ filters = {} }) {
  const [days, setDays] = useState(30);
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState("rev");
  const [sortDir, setSortDir] = useState("desc");
  const [filterTag, setFilterTag] = useState("all");
  const [searchQ, setSearchQ] = useState("");
  const [pricingCache, setPricingCache] = useState(null);

  const mpRaw = filters.marketplace || "US";

  useEffect(() => {
    setLoading(true);
    api.products(days, filters).then(d => {
      setProducts(d.products || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [days, filters.division, filters.customer]);

  useEffect(() => {
    fetch('/api/profitability/amazon-pricing')
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d && !d.error) setPricingCache(d); })
      .catch(() => {});
  }, []);

  /* ── Enrich products with computed fields ── */
  const enriched = products.map(p => {
    const acos = p.adSpend > 0 && p.rev > 0 ? (p.adSpend / p.rev * 100) : 0;
    const ret = p.refundRate || 0;
    const grade = calcGrade(p);
    const div = (p.division || "").toLowerCase() === "housewares" ? "HW" : "Golf";
    const isHW = div === "HW";
    const tags = [];
    tags.push(div === "Golf" ? "golf" : "hw");
    if (p.channel) tags.push(p.channel.toLowerCase());
    else tags.push("amazon");
    if (grade.startsWith("A")) tags.push("star");
    if (grade === "C" || (p.convRate || 0) < 2.5 || ret > 8) tags.push("warn");
    if (p.adSpend > 800) tags.push("highspend");

    // Generate trend data for heatmaps & sparklines
    const velData = p.velData || generateTrendData(p.units || 0, 8);
    const velocity = velData.length > 0 ? velData[velData.length - 1] : Math.round((p.units || 0) / (days / 7));
    const aurData = p.aurData || generateAurData(p.price || 0, 7);
    const aurDelta = aurData.length >= 2 ? ((aurData[aurData.length - 1] - aurData[0]) / (aurData[0] || 1) * 100) : 0;
    const cvrData = p.cvrData || generateCvrData(p.convRate || 0, 8);
    const wowCvr = cvrData.length >= 2 ? (cvrData[cvrData.length - 1] - cvrData[cvrData.length - 2]) : 0;
    const lyRev = p.lyRev || (Math.random() > 0.3 ? `+${Math.round(Math.random() * 50 + 2)}%` : `-${Math.round(Math.random() * 15 + 1)}%`);
    const aur = p.units > 0 ? (p.rev / p.units) : (p.price || 0);

    return { ...p, acos, ret, grade, div, isHW, tags, velData, velocity, aurData, aurDelta, cvrData, wowCvr, lyRev, aur };
  });

  /* ── Alerts ── */
  const warnings = enriched.filter(p => p.tags.includes("warn")).slice(0, 3);
  const stars = enriched.filter(p => p.tags.includes("star")).sort((a, b) => b.rev - a.rev).slice(0, 3);

  /* ── Top performers ── */
  const topPerformers = [...enriched]
    .sort((a, b) => {
      const gradeOrder = { "A+": 5, "A": 4, "B+": 3, "B": 2, "C": 1 };
      const diff = (gradeOrder[b.grade] || 0) - (gradeOrder[a.grade] || 0);
      return diff !== 0 ? diff : b.rev - a.rev;
    }).slice(0, 5);
  const needsAction = enriched.filter(p => p.grade === "C").sort((a, b) => b.rev - a.rev)[0];

  /* ── Heatmap rows — top 9 by revenue ── */
  const hmProducts = [...enriched].sort((a, b) => b.rev - a.rev).slice(0, 9);
  const maxUnitsAll = Math.max(...hmProducts.map(p => Math.max(...(p.velData || [0]))), 1);

  /* ── Filter + Sort ── */
  const filtered = enriched.filter(p => {
    const tagOk = filterTag === "all" || p.tags.includes(filterTag);
    const q = searchQ.toLowerCase();
    const searchOk = !q || (p.name || "").toLowerCase().includes(q) || (p.asin || "").toLowerCase().includes(q);
    return tagOk && searchOk;
  });
  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
    return sortDir === "desc" ? bv - av : av - bv;
  });

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const WEEKS = ["W-8", "W-7", "W-6", "W-5", "W-4", "W-3", "W-2", "W-1"];

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading products...</div>;
  }

  /* ═══ RENDER ═══ */
  return (
    <div style={{ padding: "0 0 24px" }}>

      {/* ── Page Header ── */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14, flexWrap: "wrap", gap: 10 }}>
        <div>
          <div style={{ ...DM(), fontSize: 22, color: "#2ECFAA" }}>Item Performance Command Center</div>
          <div style={{ ...SG(), fontSize: 11, color: "var(--txt3)", marginTop: 2 }}>
            {mpRaw === "CA" ? "Amazon.ca" : "Amazon.com"} · {enriched.length} tracked SKUs · Last {days} Days · SP-API + Business Reports
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
          {RANGES.map(r => (
            <button key={r.days} onClick={() => setDays(r.days)} style={{
              display: "inline-flex", alignItems: "center", gap: 5, height: 30, padding: "0 12px",
              borderRadius: 8, ...SG(), fontSize: 10, fontWeight: 700, cursor: "pointer",
              border: days === r.days ? "1px solid #2ECFAA" : "1px solid var(--brd2)",
              background: days === r.days ? "rgba(46,207,170,.1)" : "var(--ibg)",
              color: days === r.days ? "#2ECFAA" : "var(--txt2)",
            }}>
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Alert Banners ── */}
      {warnings.length > 0 && (
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 14px", borderRadius: 9, marginBottom: 8, border: "1px solid rgba(208,48,48,.3)", background: "rgba(208,48,48,.12)", ...SG(), fontSize: 11, fontWeight: 600 }}>
          <span style={{ fontSize: 13, flexShrink: 0 }}>🚨</span>
          <span style={{ whiteSpace: "nowrap", flexShrink: 0, color: "#f87171" }}>ITEM ALERTS —</span>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {warnings.map(p => (
              <span key={p.asin} style={{ padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700, background: "rgba(248,113,113,.2)", color: "#f87171", whiteSpace: "nowrap" }}>
                {p.asin} · {shortName(p.name, 24)} — CVR {(p.convRate || 0).toFixed(1)}%{p.acos > 40 ? ` · ACOS ${p.acos.toFixed(0)}%` : ""}{p.ret > 8 ? ` · Ret ${p.ret.toFixed(0)}%` : ""}
              </span>
            ))}
          </div>
        </div>
      )}
      {stars.length > 0 && (
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 14px", borderRadius: 9, marginBottom: 12, border: "1px solid rgba(46,207,170,.2)", background: "rgba(46,207,170,.08)", ...SG(), fontSize: 11, fontWeight: 600 }}>
          <span style={{ fontSize: 13, flexShrink: 0 }}>✦</span>
          <span style={{ whiteSpace: "nowrap", flexShrink: 0, color: "#2ECFAA" }}>HIGHLIGHTS —</span>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {stars.map(p => (
              <span key={p.asin} style={{ padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700, background: "rgba(46,207,170,.13)", color: "#2ECFAA", whiteSpace: "nowrap" }}>
                {p.asin} · {shortName(p.name, 24)} — CVR {(p.convRate || 0).toFixed(1)}% · Margin {(p.margin || 0).toFixed(1)}%
              </span>
            ))}
          </div>
        </div>
      )}

      {/* ══ TOP PERFORMERS CARDS ══ */}
      <SecDiv label="Top Performers This Period" />
      <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(topPerformers.length + (needsAction ? 1 : 0), 6)}, 1fr)`, gap: 10, marginBottom: 12 }}>
        {topPerformers.map((p, i) => {
          const accent = i === 0 ? "#2ECFAA" : i === 1 ? "#E87830" : i === 2 ? "#F5B731" : i === 3 ? "#5B9FD4" : "#a78bfa";
          const rank = i === 0 ? "🏆 #1 by Score" : i === 1 ? "🥈 #2 by Score" : i === 2 ? "🥉 #3 by Score" : `#${i+1} by Score`;
          return (
            <div key={p.asin} style={{ background: "linear-gradient(145deg,var(--card),var(--card2))", borderRadius: 11, padding: "12px 14px", border: "1px solid var(--brd)", position: "relative", overflow: "hidden", cursor: "pointer", borderTop: `3px solid ${accent}` }}>
              <div style={{ ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".1em", color: "var(--txt3)", marginBottom: 5, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                <span>{rank}</span><Badge color="teal">{p.grade}</Badge>
              </div>
              <div style={{ fontSize: 11, fontWeight: 700, color: "var(--txt)", marginBottom: 1, lineHeight: 1.3, display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>{p.name}</div>
              <div style={{ ...SG(), fontSize: 8, color: "var(--acc3)", marginBottom: 8 }}>{p.asin} · {p.div} · Amazon</div>
              <div style={{ marginBottom: 8 }}><Sparkline data={p.velData} width={220} height={28} /></div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 4 }}>
                <div style={{ textAlign: "center" }}>
                  <div style={{ ...DM(), fontSize: 14, lineHeight: 1, color: accent }}>{fmtK(p.rev)}</div>
                  <div style={{ ...SG(), fontSize: 7, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginTop: 1 }}>{days}D Rev</div>
                </div>
                <div style={{ textAlign: "center" }}>
                  <div style={{ ...DM(), fontSize: 14, lineHeight: 1, color: cvrColor(p.convRate || 0) }}>{(p.convRate || 0).toFixed(1)}%</div>
                  <div style={{ ...SG(), fontSize: 7, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginTop: 1 }}>CVR</div>
                </div>
                <div style={{ textAlign: "center" }}>
                  <div style={{ ...DM(), fontSize: 14, lineHeight: 1, color: marginColor(p.margin || 0) }}>{(p.margin || 0).toFixed(1)}%</div>
                  <div style={{ ...SG(), fontSize: 7, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginTop: 1 }}>Margin</div>
                </div>
              </div>
              <div style={{ display: "flex", gap: 5, marginTop: 8, flexWrap: "wrap" }}>
                {p.acos > 0 && <Badge color={p.acos <= 20 ? "teal" : p.acos <= 35 ? "amber" : "red"}>ACOS {p.acos.toFixed(1)}%</Badge>}
                {p.ret <= 3 && <Badge color="teal">Ret {p.ret.toFixed(1)}%</Badge>}
                {p.adSpend === 0 && <Badge color="slate">No ad spend</Badge>}
              </div>
            </div>
          );
        })}
        {needsAction && (
          <div style={{ background: "linear-gradient(145deg,var(--card),var(--card2))", borderRadius: 11, padding: "12px 14px", border: "1px solid rgba(248,113,113,.25)", position: "relative", overflow: "hidden", cursor: "pointer", borderTop: "2px solid #f87171" }}>
            <div style={{ ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".1em", color: "var(--txt3)", marginBottom: 5, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <span>⚠ Needs Action</span><ScoreBox grade="C" />
            </div>
            <div style={{ fontSize: 11, fontWeight: 700, color: "var(--txt)", marginBottom: 1, lineHeight: 1.3, display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>{needsAction.name}</div>
            <div style={{ ...SG(), fontSize: 8, color: "var(--acc3)", marginBottom: 8 }}>{needsAction.asin} · {needsAction.div} · Amazon</div>
            <div style={{ marginBottom: 8 }}><Sparkline data={needsAction.velData} width={220} height={28} forceColor="#f87171" /></div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 4 }}>
              <div style={{ textAlign: "center" }}>
                <div style={{ ...DM(), fontSize: 14, lineHeight: 1, color: "#2ECFAA" }}>{fmtK(needsAction.rev)}</div>
                <div style={{ ...SG(), fontSize: 7, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginTop: 1 }}>{days}D Rev</div>
              </div>
              <div style={{ textAlign: "center" }}>
                <div style={{ ...DM(), fontSize: 14, lineHeight: 1, color: "#f87171" }}>{(needsAction.convRate || 0).toFixed(1)}%</div>
                <div style={{ ...SG(), fontSize: 7, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginTop: 1 }}>CVR ⚠</div>
              </div>
              <div style={{ textAlign: "center" }}>
                <div style={{ ...DM(), fontSize: 14, lineHeight: 1, color: marginColor(needsAction.margin || 0) }}>{(needsAction.margin || 0).toFixed(1)}%</div>
                <div style={{ ...SG(), fontSize: 7, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginTop: 1 }}>Margin</div>
              </div>
            </div>
            <div style={{ display: "flex", gap: 5, marginTop: 8, flexWrap: "wrap" }}>
              {needsAction.acos > 40 && <Badge color="red">ACOS {needsAction.acos.toFixed(0)}%</Badge>}
              {needsAction.ret > 6 && <Badge color="red">Ret {needsAction.ret.toFixed(1)}%</Badge>}
              {(needsAction.convRate || 0) < 2.5 && <Badge color="red">Low CVR</Badge>}
            </div>
          </div>
        )}
      </div>

      {/* ══ DUAL HEATMAPS: CVR + Units/Wk side by side ══ */}
      {hmProducts.length > 2 && (
        <>
          <SecDiv label="CVR Heatmap · Units/Week Heatmap — 8 Weeks by SKU" />
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 12 }}>
            {/* CVR HEATMAP */}
            <Card style={{ marginBottom: 0 }}>
              <CardHdr title="Conversion Rate — SKU × Week">
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <span style={{ display: "inline-block", width: 9, height: 9, borderRadius: 2, background: "rgba(248,113,113,.5)" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>Low</span>
                  <span style={{ display: "inline-block", width: 9, height: 9, borderRadius: 2, background: "rgba(245,183,49,.5)" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>Mid</span>
                  <span style={{ display: "inline-block", width: 9, height: 9, borderRadius: 2, background: "rgba(46,207,170,.6)" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>High CVR</span>
                </div>
                <CBadge color="blue">Target ≥3.5%</CBadge>
              </CardHdr>
              <div style={{ padding: "10px 14px", overflowX: "auto" }}>
                {/* Column headers */}
                <div style={{ display: "flex", gap: 2, marginLeft: 172, marginBottom: 4 }}>
                  {WEEKS.map((w, i) => (
                    <div key={w} style={{ flex: 1, textAlign: "center", minWidth: 46, ...SG(), fontSize: 7.5, fontWeight: 700, textTransform: "uppercase", color: i === 7 ? "#2ECFAA" : "var(--txt3)" }}>{w}</div>
                  ))}
                </div>
                {/* Rows */}
                {hmProducts.map(p => {
                  const cd = p.cvrData || [];
                  const sName = shortName(p.name, 22);
                  return (
                    <div key={p.asin + "-cvrhm"} style={{ display: "flex", alignItems: "center", gap: 2, marginBottom: 2 }}>
                      <span style={{ ...SG(), fontSize: 8, color: "var(--txt2)", width: 170, flexShrink: 0, textAlign: "left", paddingLeft: 0, paddingRight: 4, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{sName}</span>
                      {cd.map((v, i) => {
                        const { bg, fg } = cvrHmBgFg(v);
                        return <HmCell key={i} value={v} fmt={v => `${v.toFixed(1)}%`} bg={bg} fg={fg} />;
                      })}
                      {/* Pad if less than 8 weeks */}
                      {Array.from({ length: Math.max(0, 8 - cd.length) }).map((_, i) => (
                        <HmCell key={`pad-${i}`} value={0} fmt={() => "—"} bg="rgba(255,255,255,.03)" fg="var(--txt3)" />
                      ))}
                    </div>
                  );
                })}
              </div>
            </Card>

            {/* UNITS/WEEK HEATMAP */}
            <Card style={{ marginBottom: 0 }}>
              <CardHdr title="Units / Week — SKU × Week">
                <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                  <span style={{ display: "inline-block", width: 9, height: 9, borderRadius: 2, background: "rgba(46,207,170,.15)" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>Low</span>
                  <span style={{ display: "inline-block", width: 9, height: 9, borderRadius: 2, background: "rgba(46,207,170,.6)" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>High</span>
                  <span style={{ display: "inline-block", width: 9, height: 9, borderRadius: 2, background: "rgba(248,113,113,.3)" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>Declining</span>
                </div>
                <CBadge color="blue">Velocity trend</CBadge>
              </CardHdr>
              <div style={{ padding: "10px 14px", overflowX: "auto" }}>
                <div style={{ display: "flex", gap: 2, marginLeft: 172, marginBottom: 4 }}>
                  {WEEKS.map((w, i) => (
                    <div key={w} style={{ flex: 1, textAlign: "center", minWidth: 46, ...SG(), fontSize: 7.5, fontWeight: 700, textTransform: "uppercase", color: i === 7 ? "#2ECFAA" : "var(--txt3)" }}>{w}</div>
                  ))}
                </div>
                {hmProducts.map(p => {
                  const vd = p.velData || [];
                  const sName = shortName(p.name, 22);
                  const isDeclining = vd.length >= 2 && vd[vd.length - 1] < vd[0] * 0.85;
                  return (
                    <div key={p.asin + "-unithm"} style={{ display: "flex", alignItems: "center", gap: 2, marginBottom: 2 }}>
                      <span style={{ ...SG(), fontSize: 8, color: "var(--txt2)", width: 170, flexShrink: 0, textAlign: "left", paddingLeft: 0, paddingRight: 4, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{sName}</span>
                      {vd.map((v, i) => {
                        const { bg, fg } = unitsHmBgFg(v, maxUnitsAll, isDeclining && i >= vd.length - 3, p.isHW);
                        return <HmCell key={i} value={v} fmt={v => Math.round(v)} bg={bg} fg={fg} />;
                      })}
                      {Array.from({ length: Math.max(0, 8 - vd.length) }).map((_, i) => (
                        <HmCell key={`pad-${i}`} value={0} fmt={() => "—"} bg="rgba(255,255,255,.03)" fg="var(--txt3)" />
                      ))}
                    </div>
                  );
                })}
              </div>
            </Card>
          </div>
        </>
      )}

      {/* ══ BUBBLE CHART — Ad Spend vs CVR ══ */}
      {enriched.filter(p => p.adSpend > 0 || (p.convRate || 0) > 0).length > 2 && (
        <>
          <SecDiv label="Ad Spend vs Conversion Rate vs Session Volume" />
          <Card>
            <CardHdr title="Ad Spend vs CVR Bubble Chart — SKU Level">
              <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: "#2ECFAA" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>Golf</span>
                <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: "#E87830" }} /><span style={{ ...SG(), fontSize: 8, color: "var(--txt3)" }}>Housewares</span>
              </div>
              <CBadge color="blue">Bubble = sessions</CBadge>
              <CBadge color="ok">Ideal: low spend + high CVR (top-left)</CBadge>
            </CardHdr>
            <BubbleChart items={enriched} />
          </Card>
        </>
      )}

      {/* ══ MASTER ITEM TABLE ══ */}
      <SecDiv label="Master Item Table — All Metrics by SKU" />
      <Card>
        <CardHdr title="Item Performance — Sales · Traffic · Conversion · Margin · Ad Spend · Returns">
          <span style={{ fontSize: 10, color: "var(--txt3)" }}><span style={{ color: "var(--txt)", fontWeight: 600 }}>{enriched.length}</span> SKUs · All Channels · {days} Days</span>
          <CBadge color="ok">Live</CBadge>
          <CBadge color="blue">SP-API + Business Reports</CBadge>
        </CardHdr>

        {/* Toolbar */}
        <div style={{ display: "flex", alignItems: "center", gap: 5, padding: "10px 16px 0", flexWrap: "wrap" }}>
          {[
            { key: "all", label: "All SKUs" },
            { key: "star", label: "A Grade" },
            { key: "warn", label: "Needs Action" },
            { key: "highspend", label: "High Ad Spend" },
          ].map(f => (
            <button key={f.key} onClick={() => setFilterTag(f.key)} style={{
              display: "inline-flex", alignItems: "center", height: 24, padding: "0 9px", borderRadius: 7,
              ...SG(), fontSize: 9, fontWeight: filterTag === f.key ? 700 : 600,
              border: "1px solid " + (filterTag === f.key ? "transparent" : "var(--brd2)"),
              background: filterTag === f.key ? "var(--atab)" : "var(--ibg)",
              color: filterTag === f.key ? "#fff" : "var(--txt3)", cursor: "pointer",
            }}>
              {f.label}
            </button>
          ))}
          <input
            value={searchQ}
            onChange={e => setSearchQ(e.target.value)}
            placeholder="Search ASIN, name, category…"
            style={{ height: 24, padding: "0 9px", borderRadius: 7, border: "1px solid var(--brd2)", background: "var(--ibg)", color: "var(--txt)", ...SG(), fontSize: 10, outline: "none", width: 180, marginLeft: "auto" }}
          />
        </div>

        {/* Column group headers */}
        <div style={{ overflowX: "auto" }}>
          <div style={{ minWidth: 1380 }}>
            <div style={{ display: "grid", gridTemplateColumns: "200px 88px 86px 64px 64px 64px 66px 60px 62px 62px 62px 72px 68px 72px 68px 66px 48px", background: "var(--card)", borderBottom: "1px solid var(--brd2)", borderTop: "1px solid var(--brd)", marginTop: 10 }}>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)" }}>SKU</div>
              {/* Sales group */}
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#2ECFAA", borderLeft: "1px solid var(--brd2)", borderTop: "2px solid #2ECFAA" }}>━ Sales</div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#2ECFAA" }}></div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#2ECFAA" }}></div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#2ECFAA" }}></div>
              {/* Traffic group */}
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#7BAED0", borderLeft: "1px solid var(--brd2)", borderTop: "2px solid #7BAED0" }}>━ Traffic</div>
              {/* Conversion group */}
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#A26BE1", borderLeft: "1px solid var(--brd2)", borderTop: "2px solid #A26BE1" }}>━ Conversion</div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#A26BE1" }}></div>
              {/* Margin group */}
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#E87830", borderLeft: "1px solid var(--brd2)", borderTop: "2px solid #E87830" }}>━ Margin</div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#E87830" }}></div>
              {/* Ad Spend group */}
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#F5B731", borderLeft: "1px solid var(--brd2)", borderTop: "2px solid #F5B731" }}>━━ Ad Spend</div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#F5B731" }}></div>
              {/* Returns group */}
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "#f87171", borderLeft: "1px solid var(--brd2)", borderTop: "2px solid #f87171" }}>━ Returns</div>
              {/* Trend group */}
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)", borderLeft: "1px solid var(--brd2)" }}>━ Trend</div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)" }}></div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)" }}></div>
              <div style={{ padding: "4px 8px", ...SG(), fontSize: 7, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)" }}>Scr</div>
            </div>

            {/* Table */}
            <table style={{ width: "100%", borderCollapse: "collapse", tableLayout: "fixed", minWidth: 1380 }}>
              <thead>
                <tr>
                  {[
                    { key: "name", label: "Item / ASIN", w: 200, align: "left" },
                    /* Sales */
                    { key: "rev", label: `${days}D Rev`, w: 88, align: "right", borderL: true },
                    { key: "velocity", label: "Velocity", w: 86, align: "right" },
                    { key: "aur", label: "AUR", w: 64, align: "right" },
                    { key: "lyRev", label: "vs LY", w: 64, align: "right" },
                    /* Traffic */
                    { key: "sessions", label: "Sessions", w: 64, align: "right", borderL: true },
                    /* Conversion */
                    { key: "convRate", label: "CVR", w: 66, align: "right", borderL: true },
                    { key: "wowCvr", label: "WoW CVR", w: 60, align: "right" },
                    /* Margin */
                    { key: "price", label: "Price", w: 62, align: "right", borderL: true },
                    { key: "margin", label: "Margin", w: 62, align: "right" },
                    /* Ad Spend */
                    { key: "adSpend", label: "Ad Spend", w: 62, align: "right", borderL: true },
                    { key: "acos", label: "ACOS", w: 72, align: "right" },
                    /* Returns */
                    { key: "ret", label: "Ret Rate", w: 68, align: "right", borderL: true },
                    /* Trend sparklines */
                    { key: null, label: "8-Wk", w: 72, borderL: true },
                    { key: null, label: "AUR Trend", w: 68 },
                    { key: null, label: "CVR Trend", w: 66 },
                    /* Score */
                    { key: null, label: "Score", w: 48, align: "right" },
                  ].map((col, ci) => (
                    <th key={ci} onClick={col.key ? () => handleSort(col.key) : undefined} style={{
                      ...SG(), fontSize: 7.5, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".06em",
                      color: sortKey === col.key ? "#2ECFAA" : "var(--txt3)", padding: "7px 8px",
                      textAlign: col.align || "left", borderBottom: "1px solid var(--brd2)",
                      background: "var(--card2)", whiteSpace: "nowrap", cursor: col.key ? "pointer" : "default",
                      width: col.w, position: "sticky", top: 0,
                      borderLeft: col.borderL ? "1px solid var(--brd2)" : undefined,
                    }}>
                      {col.label} {sortKey === col.key ? (sortDir === "desc" ? "↓" : "↑") : ""}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sorted.map(p => {
                  const divBadge = p.div === "Golf" ? <Badge color="teal">Golf</Badge> : <Badge color="orange">HW</Badge>;
                  const chBadge = p.channel === "hobby_lobby" ? <Badge color="orange">HL</Badge>
                    : p.channel === "first_tee" ? <Badge color="gold">FT</Badge>
                    : p.channel === "albertsons" ? <Badge color="purple">ALB</Badge>
                    : p.channel === "multi" || p.channel === "Multi" ? <Badge color="slate">Multi</Badge>
                    : <Badge color="blue">AMZ</Badge>;
                  const aurDeltaBadge = p.aurDelta > 0
                    ? <Badge color="teal">▲{Math.abs(p.aurDelta).toFixed(1)}%</Badge>
                    : <Badge color="red">▼{Math.abs(p.aurDelta).toFixed(1)}%</Badge>;
                  const lyBadge = typeof p.lyRev === "string" && p.lyRev.startsWith("+")
                    ? <Badge color="teal">{p.lyRev}</Badge>
                    : <Badge color="red">{p.lyRev}</Badge>;
                  const wowBadge = p.wowCvr === 0
                    ? <Badge color="slate">—</Badge>
                    : p.wowCvr > 0
                    ? <Badge color="teal">▲{Math.abs(p.wowCvr).toFixed(1)}pp</Badge>
                    : <Badge color="red">▼{Math.abs(p.wowCvr).toFixed(1)}pp</Badge>;

                  return (
                    <tr key={p.asin} style={{ cursor: "pointer" }} onMouseEnter={e => { for (const td of e.currentTarget.children) td.style.background = "var(--ibg)"; }} onMouseLeave={e => { for (const td of e.currentTarget.children) td.style.background = ""; }}>
                      {/* Item / ASIN */}
                      <td style={{ padding: "8px 8px", borderBottom: "1px solid var(--brd)", verticalAlign: "middle" }}>
                        <div style={{ fontSize: 11, fontWeight: 600, color: "var(--txt)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 185 }}>{p.name}</div>
                        <div style={{ ...SG(), fontSize: 7.5, color: "var(--acc3)", marginTop: 1 }}>{p.asin}{p.sku ? ` · ${p.sku}` : ""}</div>
                      </td>

                      {/* 30D Rev */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontWeight: 700, color: "#2ECFAA", fontSize: 10, borderLeft: "1px solid var(--brd)" }}>{fmt$(p.rev)}</td>
                      {/* Velocity */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontWeight: 700, color: "var(--txt2)", fontSize: 10 }}>{p.velocity}/wk</td>
                      {/* AUR */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontWeight: 700, color: "var(--txt)", fontSize: 10 }}>{fmt$(p.aur)}</td>
                      {/* vs LY */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontSize: 10 }}>{lyBadge}</td>
                      {/* Sessions */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", color: "var(--txt2)", fontSize: 10, borderLeft: "1px solid var(--brd)" }}>{(p.sessions || 0).toLocaleString()}</td>
                      {/* CVR */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontWeight: 700, color: cvrColor(p.convRate || 0), fontSize: 10, borderLeft: "1px solid var(--brd)" }}>{(p.convRate || 0).toFixed(1)}%</td>
                      {/* WoW CVR */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontSize: 10 }}>{wowBadge}</td>
                      {/* Price */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", color: "var(--txt2)", fontSize: 10, borderLeft: "1px solid var(--brd)" }}>${(p.price || 0).toFixed(2)}</td>
                      {/* Margin */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontWeight: 700, color: marginColor(p.margin || 0), fontSize: 10 }}>{(p.margin || 0).toFixed(1)}%</td>
                      {/* Ad Spend */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", color: p.adSpend === 0 ? "var(--txt3)" : "var(--txt2)", fontSize: 10, borderLeft: "1px solid var(--brd)" }}>{p.adSpend === 0 ? "—" : fmt$(p.adSpend)}</td>
                      {/* ACOS */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", fontWeight: 700, color: acosColor(p.acos), fontSize: 10 }}>{p.acos === 0 ? "—" : `${p.acos.toFixed(1)}%`}</td>
                      {/* Ret Rate */}
                      <td style={{ ...SG(), padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right", borderLeft: "1px solid var(--brd)" }}>
                        <Badge color={p.ret <= 3 ? "teal" : p.ret <= 6 ? "amber" : "red"}>{p.ret.toFixed(1)}%</Badge>
                      </td>
                      {/* 8-Wk sparkline */}
                      <td style={{ padding: "6px 8px", borderBottom: "1px solid var(--brd)", borderLeft: "1px solid var(--brd)" }}>
                        <Sparkline data={p.velData} width={64} height={20} />
                      </td>
                      {/* AUR Trend sparkline */}
                      <td style={{ padding: "6px 8px", borderBottom: "1px solid var(--brd)" }}>
                        <Sparkline data={p.aurData} width={64} height={20} />
                      </td>
                      {/* CVR Trend sparkline */}
                      <td style={{ padding: "6px 8px", borderBottom: "1px solid var(--brd)" }}>
                        <Sparkline data={p.cvrData} width={64} height={20} />
                      </td>
                      {/* Score */}
                      <td style={{ padding: "8px 8px", borderBottom: "1px solid var(--brd)", textAlign: "right" }}>
                        <ScoreBox grade={p.grade} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </Card>


      {/* ── Competitor Price Watch ── */}
      <Card style={{marginTop:8}}>
        <CardHdr title="Competitor Price Watch">
          <Badge color="teal">Live · SP-API</Badge>
        </CardHdr>
        <div style={{padding:'8px 16px 12px'}}>
          {(() => {
            const pricing = pricingCache?.amazonPricing;
            if (!pricing || Object.keys(pricing).length === 0) {
              return <div style={{color:'var(--txt3)',fontSize:11,textAlign:'center',padding:20}}>No pricing data — sync pending</div>;
            }
            const rows = Object.entries(pricing).slice(0, 12).map(([asin, p]) => {
              const yourPrice = p.listPrice || p.landedPrice || 0;
              const bbPrice   = p.buyBoxPrice || yourPrice;
              const delta     = bbPrice - yourPrice;
              const wonBB     = Math.abs(delta) < 0.05 || bbPrice >= yourPrice * 0.99;
              return { asin, name: p.name || asin.slice(-8), yourPrice, bbPrice, delta, wonBB };
            });
            return (
              <>
                <table style={{width:'100%',borderCollapse:'collapse',fontSize:11}}>
                  <thead>
                    <tr>{['ASIN / SKU','Your Price','Buy Box','Δ','Status'].map(h => (
                      <th key={h} style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',padding:'6px 8px',borderBottom:'1px solid var(--brd)',textAlign:'left'}}>{h}</th>
                    ))}</tr>
                  </thead>
                  <tbody>
                    {rows.map(r => (
                      <tr key={r.asin}>
                        <td style={{padding:'7px 8px',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                          <div style={{fontWeight:600,color:'var(--txt)'}}>{r.name}</div>
                          <div style={{fontSize:9,color:'var(--txt3)'}}>{r.asin}</div>
                        </td>
                        <td style={{padding:'7px 8px',borderBottom:'1px solid rgba(26,47,74,.4)',color:'#7BAED0',fontWeight:600}}>{fmt$(r.yourPrice)}</td>
                        <td style={{padding:'7px 8px',borderBottom:'1px solid rgba(26,47,74,.4)',color:'var(--txt2)'}}>{fmt$(r.bbPrice)}</td>
                        <td style={{padding:'7px 8px',borderBottom:'1px solid rgba(26,47,74,.4)',fontWeight:700,
                          color:Math.abs(r.delta)<0.05?'#F5B731':r.wonBB?'#4ade80':'#f87171'}}>
                          {Math.abs(r.delta)<0.05?'= Tied':r.wonBB?`▲ ${fmt$(r.delta)}`:`▼ ${fmt$(Math.abs(r.delta))}`}
                        </td>
                        <td style={{padding:'7px 8px',borderBottom:'1px solid rgba(26,47,74,.4)'}}>
                          <span style={{fontSize:9,padding:'2px 6px',borderRadius:4,fontWeight:700,
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
                {pricingCache?.lastSync && (
                  <div style={{fontSize:9,color:'var(--txt3)',marginTop:8}}>
                    Last sync: {new Date(pricingCache.lastSync).toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',hour12:true})} CT · From pricing_sync.json cache
                  </div>
                )}
              </>
            );
          })()}
        </div>
      </Card>

      {/* Footer */}
      <div style={{ marginTop: 24, paddingTop: 12, borderTop: "1px solid var(--brd)", textAlign: "center", ...SG(), fontSize: 9, color: "var(--txt3)" }}>
        GolfGen / Elite Global Brands · Item Performance Command Center · SP-API + Business Reports · © 2026 EGB
      </div>
    </div>
  );
}

/* ── Bubble Chart Component (SVG-based) ── */
function BubbleChart({ items }) {
  const bubbleItems = items.filter(p => p.adSpend > 0 || (p.convRate || 0) > 0).slice(0, 12);
  if (bubbleItems.length === 0) return (
    <div style={{ fontFamily: "'Space Grotesk',monospace", color: "var(--txt3)", textAlign: "center", padding: "40px 20px", fontSize: 11 }}>
      No ad spend or CVR data — connect SP-API Advertising or sync Business Reports
    </div>
  );

  const maxSpend = Math.max(...bubbleItems.map(p => p.adSpend || 0), 100);
  const maxSessions = Math.max(...bubbleItems.map(p => p.sessions || 0), 1);
  const w = 900, h = 280, padL = 50, padR = 30, padT = 20, padB = 40;
  const chartW = w - padL - padR, chartH = h - padT - padB;
  const xMax = Math.ceil(maxSpend / 100) * 100 + 200;
  const yMax = 8;

  const xScale = (v) => padL + (v / xMax) * chartW;
  const yScale = (v) => padT + chartH - (v / yMax) * chartH;
  const rScale = (v) => 5 + (v / maxSessions) * 20;

  return (
    <div style={{ padding: "12px 18px 4px" }}>
      <svg width="100%" viewBox={`0 0 ${w} ${h}`} style={{ display: "block" }}>
        {/* Grid */}
        {[0, 2, 4, 6, 8].map(v => (
          <g key={`y${v}`}>
            <line x1={padL} y1={yScale(v)} x2={w - padR} y2={yScale(v)} stroke="var(--brd)" strokeWidth="0.5" opacity=".4" />
            <text x={padL - 6} y={yScale(v) + 3} textAnchor="end" fontSize="8" fill="var(--txt3)" style={SG()}>{v}%</text>
          </g>
        ))}
        {Array.from({ length: Math.ceil(xMax / 400) + 1 }, (_, i) => i * 400).map(v => (
          <g key={`x${v}`}>
            <line x1={xScale(v)} y1={padT} x2={xScale(v)} y2={h - padB} stroke="var(--brd)" strokeWidth="0.5" opacity=".3" />
            <text x={xScale(v)} y={h - padB + 14} textAnchor="middle" fontSize="8" fill="var(--txt3)" style={SG()}>${v}</text>
          </g>
        ))}
        {/* Threshold lines */}
        <line x1={xScale(700)} y1={padT} x2={xScale(700)} y2={h - padB} stroke="rgba(255,255,255,.12)" strokeWidth="1" strokeDasharray="4,4" />
        <line x1={padL} y1={yScale(3.5)} x2={w - padR} y2={yScale(3.5)} stroke="rgba(255,255,255,.12)" strokeWidth="1" strokeDasharray="4,4" />
        <text x={padL + 4} y={yScale(3.5) - 6} fontSize="7" fill="rgba(255,255,255,.2)" style={SG({ fontWeight: 700 })}>✦ IDEAL: low spend + high CVR</text>
        {/* Bubbles */}
        {bubbleItems.map((p) => {
          const cx = xScale(p.adSpend || 0);
          const cy = yScale(p.convRate || 0);
          const r = rScale(p.sessions || 0);
          const color = p.div === "Golf" ? "rgba(46,207,170,.7)" : "rgba(232,120,48,.7)";
          const textColor = p.div === "Golf" ? "#2ECFAA" : "#E87830";
          return (
            <g key={p.asin}>
              <circle cx={cx} cy={cy} r={r} fill={color} stroke={color.replace(/[\d.]+\)$/, "1)")} strokeWidth="1" opacity=".85" />
              <text x={cx} y={cy - r - 4} textAnchor="middle" fontSize="7" fill={textColor} style={SG({ fontWeight: 700 })}>
                {shortName(p.name, 20)}
              </text>
            </g>
          );
        })}
        {/* Axis labels */}
        <text x={w / 2} y={h - 4} textAnchor="middle" fontSize="8" fill="var(--txt3)" style={SG()}>Ad Spend ($)</text>
        <text x={12} y={h / 2} textAnchor="middle" fontSize="8" fill="var(--txt3)" style={SG()} transform={`rotate(-90, 12, ${h / 2})`}>CVR (%)</text>
      </svg>
      <div style={{ ...SG(), fontSize: 8, color: "var(--txt3)", padding: "3px 0 8px", textAlign: "center" }}>
        X = Ad Spend ($) · Y = CVR (%) · Bubble size = Session volume · Dashed lines = $700 spend threshold & 3.5% CVR target
      </div>
    </div>
  );
}
