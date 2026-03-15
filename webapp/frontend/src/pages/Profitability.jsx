import { useState, useEffect, useMemo, useCallback } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, ComposedChart, Line,
  PieChart, Pie, ScatterChart, Scatter, ZAxis,
  Legend
} from "recharts";
import { api, fmt$ } from "../lib/api";
import { TOOLTIP_STYLE } from "../lib/constants";

/* ── Constants ─────────────────────────────────────────────── */
const SUB_TABS = [
  { key: "pnl", label: "P&L Overview" },
  { key: "fees", label: "Amazon Fee Detail" },
  { key: "items", label: "Item Profitability" },
  { key: "aur", label: "AUR Analysis" },
  { key: "pricing", label: "Pricing & Coupons" },
];

const DAY_OPTIONS = [
  { label: "7D", days: 7 },
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
];

const ITEM_FILTERS = [
  { key: "all", label: "All" },
  { key: "golf", label: "Golf" },
  { key: "housewares", label: "Housewares" },
  { key: "healthy", label: "Healthy ≥40%" },
  { key: "atrisk", label: "At Risk 20-39%" },
  { key: "danger", label: "Danger <20%" },
];

const ITEM_SORTS = [
  { key: "rev", label: "Revenue" },
  { key: "grossMargin", label: "Gross Margin" },
  { key: "netMargin", label: "Net Margin" },
  { key: "totalFeePct", label: "Fee Ratio" },
  { key: "cogsPct", label: "COGS%" },
];

const WF_COLORS = {
  revenue: "#2ECFAA", referral: "#f87171", fba: "#f87171",
  storage: "#F5B731", other: "#A26BE1", netRev: "#3E658C",
  cogs: "#E87830", shipping: "#7BAED0", coupon: "#8B5CF6",
  nop: "#2ECFAA", nopNeg: "#f87171",
};

const DONUT_COLORS = ["#f87171", "#E87830", "#F5B731", "#A26BE1"];

/* ── Helpers ───────────────────────────────────────────────── */
function round(n, d = 0) { const f = Math.pow(10, d); return Math.round(n * f) / f; }
function fmt$2(n) { if (n == null) return "$0"; return (n < 0 ? "-" : "") + "$" + Math.abs(Math.round(n)).toLocaleString(); }
function fmtPct(n) { if (n == null) return "0%"; return round(n, 1) + "%"; }
function fmtDate(d) {
  if (!d) return "—";
  try { return new Date(d).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "2-digit" }); }
  catch { return d; }
}
function fmtDateInput(d) {
  if (!d) return "";
  try { return new Date(d).toISOString().slice(0, 10); } catch { return ""; }
}
function scoreClass(s) { return s === "A" ? "sc-a" : s === "B" ? "sc-b" : "sc-c"; }
function statusClass(s) { return s === "active" ? "status-active" : s === "scheduled" ? "status-sched" : "status-ended"; }

/* ── Styles ────────────────────────────────────────────────── */
const SG = (sz, wt = 600, c) => ({ fontFamily: "'Space Grotesk', monospace", fontSize: sz, fontWeight: wt, ...(c ? { color: c } : {}) });
const DM = (sz, c) => ({ fontFamily: "'DM Serif Display', Georgia, serif", fontSize: sz, ...(c ? { color: c } : {}) });
const cellR = { padding: "8px 10px", textAlign: "right", ...SG(10, 700), color: "var(--txt2)", whiteSpace: "nowrap" };
const cellL = { ...cellR, textAlign: "left" };

/* ══════════════════════════════════════════════════════════════
   MAIN COMPONENT
   ══════════════════════════════════════════════════════════════ */
export default function Profitability({ filters = {} }) {
  const [tab, setTab] = useState("pnl");
  const [days, setDays] = useState(30);
  const [toast, setToast] = useState(null);

  const showToast = useCallback((msg) => {
    setToast(msg);
    setTimeout(() => setToast(null), 3000);
  }, []);

  return (
    <>
      {/* Sub-nav tabs */}
      <div className="inv-subnav">
        {SUB_TABS.map(t => (
          <button key={t.key} className={`inv-tab ${tab === t.key ? "active" : ""}`}
            onClick={() => setTab(t.key)}>{t.label}</button>
        ))}
      </div>

      {/* Page header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14, flexWrap: "wrap", gap: 10 }}>
        <div>
          <div style={{ ...DM(22), color: "var(--teal, #2ECFAA)" }}>Profitability Command Center</div>
          <div style={{ ...SG(11, 500), color: "var(--muted, #6B8090)", marginTop: 2 }}>
            {filters.division || "All Divisions"} · {filters.customer || "All Channels"} · Last {days} Days · Financial Events
          </div>
        </div>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          {DAY_OPTIONS.map(d => (
            <button key={d.days} className={`range-tab ${days === d.days ? "active" : ""}`}
              onClick={() => setDays(d.days)} style={{ ...SG(10, 700), padding: "4px 10px", borderRadius: 7, border: "1px solid var(--border)", background: days === d.days ? "var(--navy)" : "transparent", color: days === d.days ? "#fff" : "var(--muted)", cursor: "pointer" }}>
              {d.label}
            </button>
          ))}
        </div>
      </div>

      {/* Tab content */}
      {tab === "pnl" && <PnlOverview filters={filters} days={days} showToast={showToast} />}
      {tab === "fees" && <FeeDetail filters={filters} days={days} />}
      {tab === "items" && <ItemProfitability filters={filters} days={days} />}
      {tab === "aur" && <AurAnalysis filters={filters} days={days} />}
      {tab === "pricing" && <PricingCoupons filters={filters} showToast={showToast} />}

      {/* Toast */}
      {toast && (
        <div style={{
          position: "fixed", bottom: 24, right: 24, background: "var(--navy, #0E1F2D)",
          border: "1px solid var(--teal, #2ECFAA)", borderRadius: 10, padding: "12px 18px",
          ...SG(11, 700, "var(--teal, #2ECFAA)"),
          boxShadow: "0 8px 32px rgba(0,0,0,.5)", zIndex: 9999,
        }}>{toast}</div>
      )}
    </>
  );
}

/* ══════════════════════════════════════════════════════════════
   TAB 1 — P&L OVERVIEW
   ══════════════════════════════════════════════════════════════ */
function PnlOverview({ filters, days, showToast }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.profitabilityOverview(days, filters).then(d => { setData(d); setLoading(false); }).catch(() => setLoading(false));
  }, [days, filters.division, filters.customer, filters.marketplace]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading P&L overview...</div>;
  if (!data) return <div style={{ color: "var(--muted)", padding: 40, textAlign: "center" }}>No data available</div>;

  const kpis = data.kpis || {};
  const wf = data.waterfall || {};
  const marginTrend = data.marginTrend || [];
  const feeDonut = data.feeDonut || [];

  // Build waterfall rows from the waterfall object
  const grossRev0 = wf.sales || 0;
  const waterfall = [
    { label: "Gross Revenue", value: grossRev0 },
    { label: "Referral Fees", value: -(wf.referralFees || 0) },
    { label: "FBA Fulfillment", value: -(wf.fbaFees || 0) },
    { label: "Storage Fees", value: -(wf.storageFees || 0) },
    { label: "Other Fees", value: -((wf.otherFees || 0) - (wf.storageFees || 0)) },
    { label: "Net Revenue", value: grossRev0 - (wf.amazonFees || 0) },
    { label: "COGS", value: -(wf.cogs || 0) },
    { label: "Shipping", value: -(wf.shipping || 0) },
    { label: "Net Operating Profit", value: wf.netProfit || 0 },
  ].filter(w => Math.abs(w.value) > 0.01 || w.label === "Net Operating Profit" || w.label === "Gross Revenue");

  // Build waterfall chart data
  const wfChartData = waterfall.map(w => ({
    name: w.label,
    value: w.value,
    fill: w.label.includes("Gross Rev") ? WF_COLORS.revenue :
          w.label.includes("Referral") ? WF_COLORS.referral :
          w.label.includes("FBA") ? WF_COLORS.fba :
          w.label.includes("Storage") ? WF_COLORS.storage :
          w.label.includes("Other") ? WF_COLORS.other :
          w.label.includes("Net Rev") ? WF_COLORS.netRev :
          w.label.includes("COGS") ? WF_COLORS.cogs :
          w.label.includes("Ship") ? WF_COLORS.shipping :
          w.label.includes("Coupon") ? WF_COLORS.coupon :
          (w.value >= 0 ? WF_COLORS.nop : WF_COLORS.nopNeg),
  }));

  const grossRev = kpis.grossRevenue || 0;

  return (
    <>
      {/* KPI Row */}
      <div className="kpi-grid" style={{ marginBottom: 14 }}>
        <KpiCard label="Gross Revenue" value={fmt$2(kpis.grossRevenue)} color="#2ECFAA" sub="Before all fees" />
        <KpiCard label="Net Revenue" value={fmt$2(kpis.netRevenue)} color="#2ECFAA" sub="After Amazon fees" />
        <KpiCard label="Gross Margin" value={fmtPct(kpis.grossMargin)} color="#E87830" sub="After COGS + fees" />
        <KpiCard label="Total Amazon Fees" value={fmt$2(kpis.totalFees)} color="#f87171" sub={`${kpis.feePct || 0}% of gross rev`} />
        <KpiCard label="Total COGS" value={fmt$2(kpis.totalCogs)} color="#F5B731" sub={`${kpis.cogsPct || 0}% of gross rev`} />
        <KpiCard label="Net Operating Profit" value={fmt$2(kpis.netProfit)} color="#7BAED0" sub="After COGS + all fees" />
        <KpiCard label="Avg Unit Cost" value={`$${round(kpis.avgUnitCost || 0, 2)}`} color="#A26BE1" sub="Blended landed COGS" />
        <KpiCard label="Contribution / Unit" value={`$${round(kpis.contributionPerUnit || 0, 2)}`} color="#2ECFAA" sub="Rev less COGS + fees" />
      </div>

      {/* Section: P&L Waterfall */}
      <div className="sec-div"><span>P&L Waterfall — {days} Days · Gross to Net</span></div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 340px", gap: 12, marginBottom: 12 }}>
        {/* Waterfall chart + table */}
        <div className="table-card" style={{ padding: 0 }}>
          <div style={{ padding: "10px 16px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ ...SG(12, 700) }}>Revenue Waterfall — Gross to Net Operating Profit</span>
          </div>
          <div style={{ padding: "14px 18px" }}>
            <ResponsiveContainer width="100%" height={180}>
              <BarChart data={wfChartData} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
                <XAxis dataKey="name" tick={{ ...SG(8, 600), fill: "var(--muted, #6B8090)" }} interval={0} angle={-20} textAnchor="end" height={40} />
                <YAxis tick={{ ...SG(9, 600), fill: "var(--muted, #6B8090)" }} tickFormatter={v => `$${(Math.abs(v)/1000).toFixed(0)}k`} />
                <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt$2(v), ""]} />
                <Bar dataKey="value" radius={[4,4,0,0]}>
                  {wfChartData.map((e, i) => <Cell key={i} fill={e.fill} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          {/* Tabular waterfall rows */}
          {waterfall.map((w, i) => (
            <WfRow key={i} label={w.label} amount={w.value} pct={grossRev > 0 ? round(Math.abs(w.value) / grossRev * 100, 1) : 0}
              color={w.value >= 0 ? "#2ECFAA" : "#f87171"} maxVal={grossRev}
              isTotal={w.label.includes("Net Operating") || w.label.includes("Gross Rev")}
              isSub={w.label.includes("Referral") || w.label.includes("FBA") || w.label.includes("Storage") || w.label.includes("Other Fee")} />
          ))}
        </div>

        {/* Right side: Margin Trend + Fee Donut */}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {/* Margin Trend */}
          <div className="table-card" style={{ padding: 0 }}>
            <div style={{ padding: "10px 16px", borderBottom: "1px solid var(--border)" }}>
              <span style={SG(12, 700)}>8-Week Margin Trend</span>
            </div>
            <div style={{ padding: 14 }}>
              <ResponsiveContainer width="100%" height={160}>
                <ComposedChart data={marginTrend}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
                  <XAxis dataKey="label" tick={{ ...SG(8, 600), fill: "var(--muted)" }} />
                  <YAxis tick={{ ...SG(9, 600), fill: "var(--muted)" }} tickFormatter={v => `${v}%`} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [`${round(v, 1)}%`, ""]} />
                  <Line type="monotone" dataKey="grossMargin" name="Gross Margin" stroke="#2ECFAA" strokeWidth={2} dot={{ r: 3 }} />
                  <Line type="monotone" dataKey="netMargin" name="Net Margin" stroke="#7BAED0" strokeWidth={2} dot={{ r: 3 }} />
                  <Line type="monotone" dataKey="feeRatio" name="Fee Ratio" stroke="#f87171" strokeWidth={1.5} strokeDasharray="4 4" dot={{ r: 2 }} />
                  <Legend wrapperStyle={{ ...SG(9, 600) }} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Fee Donut */}
          <div className="table-card" style={{ padding: 0, flex: 1 }}>
            <div style={{ padding: "10px 16px", borderBottom: "1px solid var(--border)" }}>
              <span style={SG(12, 700)}>Fee Composition</span>
            </div>
            <div style={{ padding: 14, display: "flex", alignItems: "center", gap: 12 }}>
              <ResponsiveContainer width="50%" height={140}>
                <PieChart>
                  <Pie data={feeDonut} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={35} outerRadius={55}>
                    {feeDonut.map((_, i) => <Cell key={i} fill={DONUT_COLORS[i % DONUT_COLORS.length]} />)}
                  </Pie>
                  <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt$2(v), ""]} />
                </PieChart>
              </ResponsiveContainer>
              <div style={{ flex: 1 }}>
                {feeDonut.map((f, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "4px 0", borderBottom: "1px solid var(--border, rgba(14,31,45,0.06))" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <span style={{ width: 8, height: 8, borderRadius: 2, background: DONUT_COLORS[i % DONUT_COLORS.length], flexShrink: 0 }} />
                      <span style={SG(9, 600, "var(--muted)")}>{f.name}</span>
                    </div>
                    <span style={SG(10, 700)}>{fmt$2(f.value)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

/* ══════════════════════════════════════════════════════════════
   TAB 2 — AMAZON FEE DETAIL
   ══════════════════════════════════════════════════════════════ */
function FeeDetail({ filters, days }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.profitabilityFeeDetail(days, filters).then(d => { setData(d); setLoading(false); }).catch(() => setLoading(false));
  }, [days, filters.division, filters.customer, filters.marketplace]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading fee detail...</div>;
  if (!data) return <div style={{ color: "var(--muted)", padding: 40, textAlign: "center" }}>No fee data available</div>;

  const categories = data.categories || [];
  const totalFees = data.total_fees || 0;

  return (
    <>
      <div className="sec-div"><span>Amazon Fee Breakdown — {days} Days</span></div>
      <div className="table-card" style={{ padding: 0 }}>
        <div style={{ padding: "10px 16px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={SG(12, 700)}>Complete Fee Breakdown</span>
          <span style={SG(10, 700, "#f87171")}>Total: {fmt$2(totalFees)}</span>
        </div>
        {/* Header */}
        <div style={{ display: "grid", gridTemplateColumns: "220px 1fr 80px 70px 70px", gap: 8, padding: "7px 14px", borderBottom: "1px solid var(--border)", background: "var(--navy, #0E1F2D)" }}>
          <span style={SG(8, 700, "rgba(255,255,255,0.5)")}>FEE TYPE</span>
          <span style={SG(8, 700, "rgba(255,255,255,0.5)")}></span>
          <span style={{ ...SG(8, 700, "rgba(255,255,255,0.5)"), textAlign: "right" }}>AMOUNT</span>
          <span style={{ ...SG(8, 700, "rgba(255,255,255,0.5)"), textAlign: "right" }}>% OF REV</span>
          <span style={{ ...SG(8, 700, "rgba(255,255,255,0.5)"), textAlign: "right" }}>WoW Δ</span>
        </div>
        {categories.map((cat, ci) => (
          <div key={ci}>
            {/* Category header */}
            <div style={{ display: "grid", gridTemplateColumns: "220px 1fr 80px 70px 70px", gap: 8, padding: "8px 14px", borderBottom: "1px solid var(--border)", background: "rgba(14,31,45,0.03)" }}>
              <span style={SG(10, 700)}>{cat.name}</span>
              <div style={{ height: 6, background: "var(--border, rgba(14,31,45,0.08))", borderRadius: 3, overflow: "hidden", alignSelf: "center" }}>
                <div style={{ width: `${totalFees > 0 ? Math.abs(cat.total) / totalFees * 100 : 0}%`, height: "100%", borderRadius: 3, background: DONUT_COLORS[ci % DONUT_COLORS.length] }} />
              </div>
              <span style={{ ...SG(11, 700, "#f87171"), textAlign: "right" }}>{fmt$2(cat.total)}</span>
              <span style={{ ...SG(10, 700, "var(--muted)"), textAlign: "right" }}>{fmtPct(cat.pct_of_rev)}</span>
              <span style={{ textAlign: "right" }}></span>
            </div>
            {/* Sub-items */}
            {(cat.items || []).map((item, ii) => (
              <div key={ii} style={{ display: "grid", gridTemplateColumns: "220px 1fr 80px 70px 70px", gap: 8, padding: "6px 14px 6px 28px", borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))" }}>
                <span style={SG(9, 500, "var(--muted)")}>{item.name}</span>
                <div style={{ height: 4, background: "var(--border, rgba(14,31,45,0.06))", borderRadius: 2, overflow: "hidden", alignSelf: "center" }}>
                  <div style={{ width: `${totalFees > 0 ? Math.abs(item.amount) / totalFees * 100 : 0}%`, height: "100%", borderRadius: 2, background: DONUT_COLORS[ci % DONUT_COLORS.length], opacity: 0.6 }} />
                </div>
                <span style={{ ...SG(10, 600), textAlign: "right" }}>{fmt$2(item.amount)}</span>
                <span style={{ ...SG(9, 600, "var(--muted)"), textAlign: "right" }}>{fmtPct(item.pct_of_rev)}</span>
                <span style={{ textAlign: "right" }}></span>
              </div>
            ))}
          </div>
        ))}
      </div>
    </>
  );
}

/* ══════════════════════════════════════════════════════════════
   TAB 3 — ITEM PROFITABILITY
   ══════════════════════════════════════════════════════════════ */
function ItemProfitability({ filters, days }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState("all");
  const [sortKey, setSortKey] = useState("rev");
  const [search, setSearch] = useState("");

  useEffect(() => {
    setLoading(true);
    api.profitabilityItems(days, filters).then(d => { setItems(d.items || []); setLoading(false); }).catch(() => setLoading(false));
  }, [days, filters.division, filters.customer, filters.marketplace]);

  const filtered = useMemo(() => {
    let list = [...items];
    // Division / margin filter
    if (filter === "golf") list = list.filter(i => (i.division || "").toLowerCase() === "golf");
    else if (filter === "housewares") list = list.filter(i => (i.division || "").toLowerCase() === "housewares");
    else if (filter === "healthy") list = list.filter(i => (i.grossMargin || 0) >= 40);
    else if (filter === "atrisk") list = list.filter(i => (i.grossMargin || 0) >= 20 && (i.grossMargin || 0) < 40);
    else if (filter === "danger") list = list.filter(i => (i.grossMargin || 0) < 20);
    // Search
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(i => (i.name || "").toLowerCase().includes(q) || (i.asin || "").toLowerCase().includes(q) || (i.sku || "").toLowerCase().includes(q));
    }
    // Sort
    list.sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0));
    return list;
  }, [items, filter, sortKey, search]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading item data...</div>;

  return (
    <>
      <div className="sec-div"><span>Item-Level Profitability — {days} Days</span></div>
      <div className="table-card" style={{ padding: 0 }}>
        {/* Toolbar */}
        <div style={{ display: "flex", alignItems: "center", gap: 5, padding: "10px 16px 8px", flexWrap: "wrap" }}>
          {ITEM_FILTERS.map(f => (
            <button key={f.key}
              onClick={() => setFilter(f.key)}
              style={{
                ...SG(9, filter === f.key ? 700 : 600),
                height: 24, padding: "0 9px", borderRadius: 7,
                border: `1px solid ${filter === f.key ? "transparent" : "var(--border)"}`,
                background: filter === f.key ? "var(--navy, #0E1F2D)" : "transparent",
                color: filter === f.key ? "#fff" : "var(--muted)",
                cursor: "pointer",
              }}>{f.label}</button>
          ))}
          <select value={sortKey} onChange={e => setSortKey(e.target.value)}
            style={{ ...SG(9, 600), height: 24, padding: "0 8px", borderRadius: 7, border: "1px solid var(--border)", background: "transparent", color: "var(--muted)", cursor: "pointer", marginLeft: 8 }}>
            {ITEM_SORTS.map(s => <option key={s.key} value={s.key}>Sort: {s.label}</option>)}
          </select>
          <input type="text" placeholder="Search SKU / ASIN..." value={search} onChange={e => setSearch(e.target.value)}
            style={{ ...SG(10, 500), height: 24, padding: "0 9px", borderRadius: 7, border: "1px solid var(--border)", background: "transparent", color: "var(--body-text, #2A3D50)", width: 180, marginLeft: "auto", outline: "none" }} />
        </div>

        {/* Table header */}
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "var(--navy, #0E1F2D)" }}>
                {["SKU / ASIN", "30D Rev", "COGS/u", "COGS%", "Ref Fee", "FBA Fee", "Storage", "Other", "Total Fees", "Gross Margin", "Net Margin", "$/Unit Net", "Score"].map((h, i) => (
                  <th key={i} style={{ ...SG(8, 700), padding: "8px 10px", textAlign: i === 0 ? "left" : "right", color: "rgba(255,255,255,0.6)", textTransform: "uppercase", letterSpacing: "0.06em", whiteSpace: "nowrap" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map(item => {
                const gm = item.grossMargin || 0;
                const gmColor = gm >= 40 ? "#2ECFAA" : gm >= 20 ? "#F5B731" : "#f87171";
                return (
                  <tr key={item.asin} style={{ borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))" }}>
                    <td style={{ ...cellL, maxWidth: 200 }}>
                      <div style={{ fontWeight: 600, color: "var(--body-text, #2A3D50)", fontSize: 11, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.name || item.sku || item.asin}</div>
                      <div style={SG(7.5, 500, "var(--muted)")}>{item.asin}{item.sku ? ` · ${item.sku}` : ""}</div>
                    </td>
                    <td style={cellR}>{fmt$2(item.rev)}</td>
                    <td style={cellR}>${round(item.cogsPerUnit || 0, 2)}</td>
                    <td style={cellR}>{fmtPct(item.cogsPct)}</td>
                    <td style={cellR}>{fmt$2(item.referralTotal)}</td>
                    <td style={cellR}>{fmt$2(item.fbaTotal)}</td>
                    <td style={cellR}>{fmt$2(item.storageFees || 0)}</td>
                    <td style={cellR}>{fmt$2(item.otherFees || 0)}</td>
                    <td style={{ ...cellR, color: "#f87171", fontWeight: 700 }}>{fmt$2(item.totalFees || (item.referralTotal + item.fbaTotal + (item.storageFees || 0)))}</td>
                    <td style={{ ...cellR, color: gmColor, fontWeight: 700 }}>{fmtPct(gm)}</td>
                    <td style={{ ...cellR, color: (item.margin || 0) >= 0 ? "#2ECFAA" : "#f87171" }}>{fmtPct(item.margin)}</td>
                    <td style={{ ...cellR, color: (item.netPerUnit || 0) >= 0 ? "#2ECFAA" : "#f87171" }}>${round(item.netPerUnit || 0, 2)}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <span style={{
                        display: "inline-flex", alignItems: "center", justifyContent: "center",
                        width: 30, height: 19, borderRadius: 5, ...SG(9, 800),
                        background: item.score === "A" ? "rgba(46,207,170,.15)" : item.score === "B" ? "rgba(245,183,49,.14)" : "rgba(248,113,113,.14)",
                        color: item.score === "A" ? "#2ECFAA" : item.score === "B" ? "#F5B731" : "#f87171",
                      }}>{item.score || "—"}</span>
                    </td>
                  </tr>
                );
              })}
              {filtered.length === 0 && (
                <tr><td colSpan={13} style={{ padding: 30, textAlign: "center", color: "var(--muted)" }}>No items match your filters</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}

/* ══════════════════════════════════════════════════════════════
   TAB 4 — AUR ANALYSIS
   ══════════════════════════════════════════════════════════════ */
function AurAnalysis({ filters, days }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    api.profitabilityAur(days > 56 ? days : 56, filters).then(d => { setData(d); setLoading(false); }).catch(() => setLoading(false));
  }, [days, filters.division, filters.customer, filters.marketplace]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading AUR data...</div>;
  if (!data) return <div style={{ color: "var(--muted)", padding: 40, textAlign: "center" }}>No AUR data available</div>;

  const skus = data.skus || [];
  const weeks = ["Wk-8", "Wk-7", "Wk-6", "Wk-5", "Wk-4", "Wk-3", "Wk-2", "Wk-1"];
  const bubbles = skus.map(s => ({ name: s.name || s.asin, aur: s.aur || 0, net_margin: s.netMargin || 0, revenue: (s.aur || 0) * (s.totalUnits || 0) }));

  return (
    <>
      <div className="sec-div"><span>AUR Trend by SKU — 8-Week Window</span></div>

      {/* AUR Trend Table */}
      <div className="table-card" style={{ padding: 0, marginBottom: 12 }}>
        <div style={{ padding: "10px 16px", borderBottom: "1px solid var(--border)" }}>
          <span style={SG(12, 700)}>Average Unit Revenue (AUR) — Weekly Trend</span>
        </div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "var(--navy, #0E1F2D)" }}>
                <th style={{ ...SG(8, 700), padding: "8px 14px", textAlign: "left", color: "rgba(255,255,255,0.6)", textTransform: "uppercase" }}>SKU / ASIN</th>
                {weeks.map((w, i) => (
                  <th key={i} style={{ ...SG(8, 700), padding: "8px 6px", textAlign: "center", color: "rgba(255,255,255,0.6)" }}>{w}</th>
                ))}
                <th style={{ ...SG(8, 700), padding: "8px 10px", textAlign: "right", color: "rgba(255,255,255,0.6)" }}>AVG AUR</th>
                <th style={{ ...SG(8, 700), padding: "8px 10px", textAlign: "center", color: "rgba(255,255,255,0.6)" }}>TREND</th>
              </tr>
            </thead>
            <tbody>
              {skus.map((s, si) => {
                const aurs = (s.weeklyAur || []).map(a => a == null ? 0 : a);
                const avg = s.aur || 0;
                const trend = aurs.length >= 2 ? (aurs[aurs.length - 1] - aurs[0]) : 0;
                return (
                  <tr key={si} style={{ borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))" }}>
                    <td style={{ ...cellL, maxWidth: 190 }}>
                      <div style={{ fontWeight: 600, color: "var(--body-text)", fontSize: 11, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{s.name || s.sku || s.asin}</div>
                      <div style={SG(7.5, 500, "var(--muted)")}>{s.asin}</div>
                    </td>
                    {aurs.map((a, ai) => (
                      <td key={ai} style={{ ...cellR, textAlign: "center", fontSize: 9.5 }}>${round(a, 2)}</td>
                    ))}
                    {/* Pad empty cells if fewer than 8 weeks */}
                    {Array.from({ length: Math.max(0, weeks.length - aurs.length) }).map((_, pi) => (
                      <td key={`p${pi}`} style={cellR}>—</td>
                    ))}
                    <td style={{ ...cellR, fontWeight: 700 }}>${round(avg, 2)}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <span style={{ ...SG(9, 700), color: trend >= 0 ? "#2ECFAA" : "#f87171" }}>
                        {trend >= 0 ? "▲" : "▼"} ${Math.abs(round(trend, 2))}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Bubble chart: AUR vs Net Margin */}
      <div className="sec-div"><span>AUR vs Net Margin — Bubble Size = Revenue</span></div>
      <div className="table-card" style={{ padding: 14 }}>
        <ResponsiveContainer width="100%" height={320}>
          <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
            <XAxis type="number" dataKey="aur" name="AUR" tick={{ ...SG(9, 600), fill: "var(--muted)" }} tickFormatter={v => `$${v}`} label={{ value: "AUR ($)", position: "bottom", offset: -2, style: SG(9, 600) }} />
            <YAxis type="number" dataKey="net_margin" name="Net Margin" tick={{ ...SG(9, 600), fill: "var(--muted)" }} tickFormatter={v => `${v}%`} label={{ value: "Net Margin %", angle: -90, position: "insideLeft", style: SG(9, 600) }} />
            <ZAxis type="number" dataKey="revenue" range={[50, 800]} />
            <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v, name) => [name === "AUR" ? `$${round(v, 2)}` : name === "Net Margin" ? `${round(v, 1)}%` : fmt$2(v), name]} />
            <Scatter data={bubbles} fill="#2ECFAA" fillOpacity={0.6} stroke="#2ECFAA" strokeWidth={1} />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </>
  );
}

/* ══════════════════════════════════════════════════════════════
   TAB 5 — PRICING & COUPONS
   ══════════════════════════════════════════════════════════════ */
function PricingCoupons({ filters, showToast }) {
  const [prices, setPrices] = useState([]);
  const [coupons, setCoupons] = useState([]);
  const [products, setProducts] = useState([]);
  const [amazonPricing, setAmazonPricing] = useState([]);
  const [amazonCoupons, setAmazonCoupons] = useState([]);
  const [lastSync, setLastSync] = useState(null);
  const [loading, setLoading] = useState(true);

  // Forms
  const [showPriceForm, setShowPriceForm] = useState(false);
  const [showCouponForm, setShowCouponForm] = useState(false);
  const [editPrice, setEditPrice] = useState(null);
  const [editCoupon, setEditCoupon] = useState(null);

  const reload = useCallback(() => {
    setLoading(true);
    Promise.all([
      api.salePrices(filters),
      api.coupons(filters),
      api.profitabilityItems(365, filters),
      api.amazonPricing(),
    ]).then(([sp, cp, items, amz]) => {
      setPrices(sp.salePrices || []);
      setCoupons(cp.coupons || []);
      setProducts((items.items || []).map(i => ({ asin: i.asin, sku: i.sku, name: i.name })));
      setAmazonPricing(amz.amazonPricing || []);
      setAmazonCoupons(amz.amazonCoupons || []);
      setLastSync(amz.lastSync || null);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [filters.division, filters.customer, filters.marketplace]);

  useEffect(() => { reload(); }, [reload]);

  // Price CRUD
  const handleCreatePrice = async (data) => {
    await api.createSalePrice(data);
    showToast("Sale price created");
    setShowPriceForm(false);
    reload();
  };
  const handleUpdatePrice = async (id, data) => {
    await api.updateSalePrice(id, data);
    showToast("Sale price updated");
    setEditPrice(null);
    reload();
  };
  const handleDeletePrice = async (id) => {
    if (!confirm("Delete this sale price?")) return;
    await api.deleteSalePrice(id);
    showToast("Sale price deleted");
    reload();
  };
  const handlePushPrice = async (id) => {
    const res = await api.pushPrice(id);
    showToast(res.pushed ? "Pushed to Amazon!" : res.error || "Push not available yet");
    reload();
  };

  // Coupon CRUD
  const handleCreateCoupon = async (data) => {
    await api.createCoupon(data);
    showToast("Coupon created");
    setShowCouponForm(false);
    reload();
  };
  const handleUpdateCoupon = async (id, data) => {
    await api.updateCoupon(id, data);
    showToast("Coupon updated");
    setEditCoupon(null);
    reload();
  };
  const handleDeleteCoupon = async (id) => {
    if (!confirm("Delete this coupon?")) return;
    await api.deleteCoupon(id);
    showToast("Coupon deleted");
    reload();
  };

  if (loading) return <div className="loading"><div className="spinner" /> Loading pricing data...</div>;

  return (
    <>
      {/* Action buttons */}
      <div style={{ display: "flex", gap: 8, marginBottom: 14 }}>
        <button onClick={() => { setShowPriceForm(!showPriceForm); setShowCouponForm(false); }}
          style={{ ...SG(10, 700), height: 30, padding: "0 12px", borderRadius: 8, border: "1px solid #2ECFAA", background: "rgba(46,207,170,.1)", color: "#2ECFAA", cursor: "pointer" }}>
          + New Sale Price
        </button>
        <button onClick={() => { setShowCouponForm(!showCouponForm); setShowPriceForm(false); }}
          style={{ ...SG(10, 700), height: 30, padding: "0 12px", borderRadius: 8, border: "1px solid #E87830", background: "rgba(232,120,48,.1)", color: "#E87830", cursor: "pointer" }}>
          + New Coupon
        </button>
      </div>

      {/* Create Sale Price Form */}
      {showPriceForm && (
        <SalePriceForm products={products} onSubmit={handleCreatePrice} onCancel={() => setShowPriceForm(false)} />
      )}

      {/* Create Coupon Form */}
      {showCouponForm && (
        <CouponForm products={products} onSubmit={handleCreateCoupon} onCancel={() => setShowCouponForm(false)} />
      )}

      {/* Amazon Active Pricing — read-only from SP-API */}
      <div className="sec-div"><span>Amazon Active Pricing — Live from SP-API {lastSync ? `(synced ${fmtDate(lastSync)})` : ""}</span></div>
      <div className="table-card" style={{ padding: 0, marginBottom: 12 }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "var(--navy, #0E1F2D)" }}>
                {["SKU / ASIN", "Division", "List Price", "Buy Box", "Landed Price", "Sale Price", "Discount"].map((h, i) => (
                  <th key={i} style={{ ...SG(8, 700), padding: "8px 10px", textAlign: i === 0 ? "left" : "center", color: "rgba(255,255,255,0.6)", textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {amazonPricing.map(p => (
                <tr key={p.asin} style={{ borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))" }}>
                  <td style={{ ...cellL, maxWidth: 200 }}>
                    <div style={{ fontWeight: 600, fontSize: 11, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.productName || p.sku || p.asin}</div>
                    <div style={SG(7.5, 500, "var(--muted)")}>{p.asin}{p.sku ? ` · ${p.sku}` : ""}</div>
                  </td>
                  <td style={{ ...cellR, textAlign: "center" }}>
                    <span style={{ ...SG(8, 700), padding: "2px 7px", borderRadius: 5, background: p.division === "golf" ? "rgba(46,207,170,.1)" : "rgba(232,120,48,.1)", color: p.division === "golf" ? "#2ECFAA" : "#E87830" }}>{p.division}</span>
                  </td>
                  <td style={{ ...cellR, textAlign: "center" }}>{p.listPrice ? `$${round(p.listPrice, 2)}` : "—"}</td>
                  <td style={{ ...cellR, textAlign: "center", color: "#2ECFAA", fontWeight: 700 }}>{p.buyBoxPrice ? `$${round(p.buyBoxPrice, 2)}` : "—"}</td>
                  <td style={{ ...cellR, textAlign: "center" }}>{p.landedPrice ? `$${round(p.landedPrice, 2)}` : "—"}</td>
                  <td style={{ ...cellR, textAlign: "center", color: p.salePrice ? "#E87830" : "var(--muted)" }}>{p.salePrice ? `$${round(p.salePrice, 2)}` : "—"}</td>
                  <td style={{ ...cellR, textAlign: "center" }}>
                    {p.discountPct > 0 ? (
                      <span style={{ ...SG(9, 700), padding: "2px 7px", borderRadius: 6, background: "rgba(46,207,170,.13)", color: "#2ECFAA" }}>{round(p.discountPct, 0)}% off</span>
                    ) : <span style={{ color: "var(--muted)" }}>—</span>}
                  </td>
                </tr>
              ))}
              {amazonPricing.length === 0 && (
                <tr><td colSpan={7} style={{ padding: 20, textAlign: "center", color: "var(--muted)" }}>No Amazon pricing data cached — pricing sync runs every hour at :30</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Amazon Active Coupons — read-only from Ads API */}
      <div className="sec-div"><span>Amazon Active Coupons — Live from Ads API</span></div>
      <div className="table-card" style={{ padding: 0, marginBottom: 12 }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "var(--navy, #0E1F2D)" }}>
                {["Coupon ID", "Items", "Discount", "Budget", "Used", "Redemptions", "State", "Start", "End"].map((h, i) => (
                  <th key={i} style={{ ...SG(8, 700), padding: "8px 10px", textAlign: i <= 1 ? "left" : "center", color: "rgba(255,255,255,0.6)", textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {amazonCoupons.map(c => {
                const usedPct = c.budgetAmount > 0 ? round(c.budgetUsed / c.budgetAmount * 100, 0) : 0;
                return (
                  <tr key={c.couponId} style={{ borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))" }}>
                    <td style={{ ...cellL, maxWidth: 130 }}>
                      <div style={SG(9, 600)}>{c.couponId}</div>
                    </td>
                    <td style={{ ...cellL, maxWidth: 200 }}>
                      <div style={SG(8, 500, "var(--muted)")}>
                        {(c.asins || []).map(a => a.productName || a.sku || a.asin).join(", ") || "—"}
                      </div>
                    </td>
                    <td style={{ ...cellR, textAlign: "center", color: "#E87830", fontWeight: 700 }}>
                      {c.discountType === "%" ? `${c.discountValue}%` : `$${round(c.discountValue, 2)}`}
                    </td>
                    <td style={{ ...cellR, textAlign: "center" }}>${round(c.budgetAmount || 0, 0)}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 5, justifyContent: "center" }}>
                        <div style={{ width: 50, height: 5, borderRadius: 3, background: "var(--border, rgba(14,31,45,0.1))", overflow: "hidden" }}>
                          <div style={{ width: `${Math.min(usedPct, 100)}%`, height: "100%", borderRadius: 3, background: usedPct > 80 ? "#f87171" : "#2ECFAA" }} />
                        </div>
                        <span style={SG(9, 700)}>{usedPct}%</span>
                      </div>
                    </td>
                    <td style={{ ...cellR, textAlign: "center" }}>{c.redemptions || 0}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <span style={{
                        ...SG(8, 700), padding: "2px 7px", borderRadius: 6, display: "inline-flex", alignItems: "center", gap: 4,
                        background: c.state === "ENABLED" ? "rgba(46,207,170,.13)" : "rgba(123,174,208,.13)",
                        color: c.state === "ENABLED" ? "#2ECFAA" : "#7BAED0",
                      }}>
                        {c.state === "ENABLED" && <span style={{ width: 5, height: 5, borderRadius: "50%", background: "#2ECFAA" }} />}
                        {c.state || "UNKNOWN"}
                      </span>
                    </td>
                    <td style={{ ...cellR, textAlign: "center", fontSize: 10 }}>{fmtDate(c.startDate)}</td>
                    <td style={{ ...cellR, textAlign: "center", fontSize: 10 }}>{fmtDate(c.endDate)}</td>
                  </tr>
                );
              })}
              {amazonCoupons.length === 0 && (
                <tr><td colSpan={9} style={{ padding: 20, textAlign: "center", color: "var(--muted)" }}>No active Amazon coupons found — coupon sync runs hourly</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Managed Sale Prices — our CRUD */}
      <div className="sec-div"><span>Managed Sale Prices</span></div>
      <div className="table-card" style={{ padding: 0, marginBottom: 12 }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "var(--navy, #0E1F2D)" }}>
                {["SKU / ASIN", "Reg Price", "Sale Price", "Discount", "Start", "End", "Status", "Actions"].map((h, i) => (
                  <th key={i} style={{ ...SG(8, 700), padding: "8px 10px", textAlign: i === 0 ? "left" : "center", color: "rgba(255,255,255,0.6)", textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {prices.map(p => {
                const disc = p.regularPrice > 0 ? round((1 - p.salePrice / p.regularPrice) * 100, 0) : 0;
                return (
                  <tr key={p.id} style={{ borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))" }}>
                    <td style={{ ...cellL, maxWidth: 200 }}>
                      <div style={{ fontWeight: 600, fontSize: 11, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.productName || p.sku || p.asin}</div>
                      <div style={SG(7.5, 500, "var(--muted)")}>{p.asin}{p.sku ? ` · ${p.sku}` : ""}</div>
                    </td>
                    <td style={{ ...cellR, textAlign: "center" }}>${round(p.regularPrice, 2)}</td>
                    <td style={{ ...cellR, textAlign: "center", color: "#E87830", fontWeight: 700 }}>${round(p.salePrice, 2)}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <span style={{ ...SG(9, 700), padding: "2px 7px", borderRadius: 6, background: "rgba(46,207,170,.13)", color: "#2ECFAA" }}>{disc}% off</span>
                    </td>
                    <td style={{ ...cellR, textAlign: "center", fontSize: 10 }}>{fmtDate(p.startDate)}</td>
                    <td style={{ ...cellR, textAlign: "center", fontSize: 10 }}>{fmtDate(p.endDate)}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <StatusBadge status={p.status} />
                    </td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <div style={{ display: "flex", gap: 4, justifyContent: "center" }}>
                        <button onClick={() => setEditPrice(p)} style={editBtnStyle}>✏️</button>
                        <button onClick={() => handlePushPrice(p.id)} title="Push to Amazon" style={{ ...editBtnStyle, borderColor: "#2ECFAA", color: "#2ECFAA" }}>⬆</button>
                        <button onClick={() => handleDeletePrice(p.id)} style={delBtnStyle}>✕</button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {prices.length === 0 && (
                <tr><td colSpan={8} style={{ padding: 20, textAlign: "center", color: "var(--muted)" }}>No sale prices configured</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Active/Scheduled Coupons */}
      <div className="sec-div"><span>Active &amp; Scheduled Coupons</span></div>
      <div className="table-card" style={{ padding: 0 }}>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: "var(--navy, #0E1F2D)" }}>
                {["Title / Items", "Off", "Type", "Budget", "Used", "Start", "End", "Status", "Actions"].map((h, i) => (
                  <th key={i} style={{ ...SG(8, 700), padding: "8px 10px", textAlign: i === 0 ? "left" : "center", color: "rgba(255,255,255,0.6)", textTransform: "uppercase" }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {coupons.map(c => {
                const usedPct = c.budget > 0 ? round(c.budgetUsed / c.budget * 100, 0) : 0;
                return (
                  <tr key={c.id} style={{ borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))" }}>
                    <td style={{ ...cellL, maxWidth: 220 }}>
                      <div style={{ fontWeight: 600, fontSize: 11 }}>{c.title || "Untitled Coupon"}</div>
                      <div style={SG(7.5, 500, "var(--muted)")}>
                        {(c.items || []).map(it => it.sku || it.asin).join(", ") || "No items"}
                      </div>
                    </td>
                    <td style={{ ...cellR, textAlign: "center", color: "#E87830", fontWeight: 700 }}>
                      {c.couponType === "percentage" ? `${c.discountValue}%` : `$${round(c.discountValue, 2)}`}
                    </td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <span style={SG(9, 600, "var(--muted)")}>{c.couponType === "percentage" ? "%" : "$"}</span>
                    </td>
                    <td style={{ ...cellR, textAlign: "center" }}>${round(c.budget || 0, 0)}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 5, justifyContent: "center" }}>
                        <div style={{ width: 50, height: 5, borderRadius: 3, background: "var(--border, rgba(14,31,45,0.1))", overflow: "hidden" }}>
                          <div style={{ width: `${Math.min(usedPct, 100)}%`, height: "100%", borderRadius: 3, background: usedPct > 80 ? "#f87171" : "#2ECFAA" }} />
                        </div>
                        <span style={SG(9, 700)}>{usedPct}%</span>
                      </div>
                    </td>
                    <td style={{ ...cellR, textAlign: "center", fontSize: 10 }}>{fmtDate(c.startDate)}</td>
                    <td style={{ ...cellR, textAlign: "center", fontSize: 10 }}>{fmtDate(c.endDate)}</td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <StatusBadge status={c.status} />
                    </td>
                    <td style={{ ...cellR, textAlign: "center" }}>
                      <div style={{ display: "flex", gap: 4, justifyContent: "center" }}>
                        <button onClick={() => setEditCoupon(c)} style={editBtnStyle}>✏️</button>
                        <button onClick={() => handleDeleteCoupon(c.id)} style={delBtnStyle}>✕</button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {coupons.length === 0 && (
                <tr><td colSpan={9} style={{ padding: 20, textAlign: "center", color: "var(--muted)" }}>No coupons configured</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Edit modals */}
      {editPrice && (
        <Modal title="Edit Sale Price" onClose={() => setEditPrice(null)}>
          <SalePriceForm products={products} initial={editPrice}
            onSubmit={(data) => handleUpdatePrice(editPrice.id, data)}
            onCancel={() => setEditPrice(null)} isEdit />
        </Modal>
      )}
      {editCoupon && (
        <Modal title="Edit Coupon" onClose={() => setEditCoupon(null)}>
          <CouponForm products={products} initial={editCoupon}
            onSubmit={(data) => handleUpdateCoupon(editCoupon.id, data)}
            onCancel={() => setEditCoupon(null)} isEdit />
        </Modal>
      )}
    </>
  );
}

/* ══════════════════════════════════════════════════════════════
   SUB-COMPONENTS
   ══════════════════════════════════════════════════════════════ */

function KpiCard({ label, value, color, sub, delta }) {
  return (
    <div className="kpi-card" style={{ position: "relative", overflow: "hidden" }}>
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: color }} />
      <div style={SG(7, 700, "var(--muted)")}>{label.toUpperCase()}</div>
      <div style={{ ...DM(20), color, lineHeight: 1, margin: "4px 0 2px" }}>{value}</div>
      {delta && <span style={SG(8, 700, delta.startsWith("▲") ? "#2ECFAA" : "#f87171")}>{delta}</span>}
      {sub && <div style={SG(8, 500, "var(--muted)")}>{sub}</div>}
    </div>
  );
}

function WfRow({ label, amount, pct, color, maxVal, isTotal, isSub }) {
  const barW = maxVal > 0 ? Math.abs(amount) / maxVal * 100 : 0;
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 10, padding: "6px 18px",
      borderBottom: "1px solid var(--border, rgba(14,31,45,0.04))",
      ...(isTotal ? { background: "rgba(14,31,45,0.04)", borderTop: "2px solid #2ECFAA" } : {}),
    }}>
      <span style={{ ...SG(isSub ? 8 : 9, isTotal ? 700 : 600), color: isSub ? "var(--muted)" : undefined, width: 200, flexShrink: 0, paddingLeft: isSub ? 12 : 0 }}>{label}</span>
      <div style={{ flex: 1, height: 8, background: "var(--border, rgba(14,31,45,0.06))", borderRadius: 4, overflow: "hidden" }}>
        <div style={{ width: `${Math.min(barW, 100)}%`, height: "100%", borderRadius: 4, background: color, opacity: isSub ? 0.5 : 0.8, transition: "width 0.6s" }} />
      </div>
      <span style={{ ...DM(14, color), width: 80, textAlign: "right", flexShrink: 0 }}>
        {amount < 0 ? "−" : ""}{fmt$2(Math.abs(amount))}
      </span>
      <span style={{ ...SG(9, 700, color), width: 42, textAlign: "right", flexShrink: 0 }}>{pct}%</span>
    </div>
  );
}

function StatusBadge({ status }) {
  const s = (status || "").toLowerCase();
  if (s === "active") return <span style={{ ...SG(8, 700), padding: "2px 7px", borderRadius: 6, background: "rgba(46,207,170,.13)", color: "#2ECFAA", display: "inline-flex", alignItems: "center", gap: 4 }}><span style={{ width: 5, height: 5, borderRadius: "50%", background: "#2ECFAA" }} />Active</span>;
  if (s === "scheduled") return <span style={{ ...SG(8, 700), padding: "2px 7px", borderRadius: 6, background: "rgba(123,174,208,.13)", color: "#7BAED0" }}>Scheduled</span>;
  return <span style={{ ...SG(8, 700), padding: "2px 7px", borderRadius: 6, background: "rgba(141,174,200,.08)", color: "var(--muted)" }}>Ended</span>;
}

const editBtnStyle = { width: 28, height: 22, borderRadius: 5, border: "1px solid var(--border)", background: "transparent", color: "var(--muted)", fontSize: 10, cursor: "pointer", display: "inline-flex", alignItems: "center", justifyContent: "center" };
const delBtnStyle = { ...editBtnStyle, borderColor: "rgba(248,113,113,.2)", color: "rgba(248,113,113,.5)" };

function Modal({ title, onClose, children }) {
  return (
    <div style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,.6)", zIndex: 1000, display: "flex", alignItems: "center", justifyContent: "center" }}
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div style={{ background: "var(--card-bg, #fff)", border: "1px solid var(--border)", borderRadius: 14, padding: 24, width: 520, maxWidth: "95vw", boxShadow: "0 20px 60px rgba(0,0,0,.3)" }}>
        <div style={{ ...DM(18), color: "#2ECFAA", marginBottom: 4 }}>{title}</div>
        <div style={{ borderTop: "1px solid var(--border)", paddingTop: 16 }}>
          {children}
        </div>
      </div>
    </div>
  );
}

/* ── Sale Price Form ──────────────────────────────── */
function SalePriceForm({ products, onSubmit, onCancel, initial, isEdit }) {
  const [asin, setAsin] = useState(initial?.asin || "");
  const [regPrice, setRegPrice] = useState(initial?.regularPrice || "");
  const [salePrice, setSalePrice] = useState(initial?.salePrice || "");
  const [startDate, setStartDate] = useState(fmtDateInput(initial?.startDate) || "");
  const [endDate, setEndDate] = useState(fmtDateInput(initial?.endDate) || "");
  const [marketplace, setMarketplace] = useState(initial?.marketplace || "US");

  const disc = regPrice && salePrice && Number(regPrice) > 0
    ? round((1 - Number(salePrice) / Number(regPrice)) * 100, 0) : 0;

  const selectedProduct = products.find(p => p.asin === asin);

  const handleSubmit = () => {
    onSubmit({
      asin,
      sku: selectedProduct?.sku || "",
      product_name: selectedProduct?.name || "",
      regular_price: Number(regPrice),
      sale_price: Number(salePrice),
      start_date: startDate,
      end_date: endDate,
      marketplace,
    });
  };

  return (
    <div className="table-card" style={{ padding: 16, marginBottom: 12 }}>
      <div style={{ ...SG(9, 700, "var(--muted)"), textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 12 }}>
        {isEdit ? "Edit Sale Price" : "Create New Sale Price"}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <FormRow label="SKU / ASIN">
          <select value={asin} onChange={e => setAsin(e.target.value)} style={inputStyle}>
            <option value="">Select product...</option>
            {products.map(p => <option key={p.asin} value={p.asin}>{p.name || p.sku || p.asin}</option>)}
          </select>
        </FormRow>
        <FormRow label="Marketplace">
          <select value={marketplace} onChange={e => setMarketplace(e.target.value)} style={inputStyle}>
            <option value="US">US</option>
            <option value="CA">CA</option>
          </select>
        </FormRow>
        <FormRow label="Regular Price">
          <input type="number" step="0.01" value={regPrice} onChange={e => setRegPrice(e.target.value)} placeholder="$0.00" style={inputStyle} />
        </FormRow>
        <FormRow label="Sale Price">
          <input type="number" step="0.01" value={salePrice} onChange={e => setSalePrice(e.target.value)} placeholder="$0.00" style={inputStyle} />
        </FormRow>
        <FormRow label="Start Date">
          <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} style={inputStyle} />
        </FormRow>
        <FormRow label="End Date">
          <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} style={inputStyle} />
        </FormRow>
      </div>
      {disc > 0 && <div style={{ ...SG(10, 700, "#2ECFAA"), marginTop: 8 }}>Discount: {disc}% off</div>}
      <div style={{ display: "flex", gap: 8, marginTop: 14, justifyContent: "flex-end" }}>
        <button onClick={onCancel} style={{ ...SG(10, 700), height: 32, padding: "0 16px", borderRadius: 8, border: "1px solid var(--border)", background: "transparent", color: "var(--muted)", cursor: "pointer" }}>Cancel</button>
        <button onClick={handleSubmit} style={{ ...SG(10, 700), height: 32, padding: "0 16px", borderRadius: 8, border: "1px solid #2ECFAA", background: "rgba(46,207,170,.12)", color: "#2ECFAA", cursor: "pointer" }}>
          {isEdit ? "Update" : "Create"} Sale Price
        </button>
      </div>
    </div>
  );
}

/* ── Coupon Form (multi-item) ──────────────────────── */
function CouponForm({ products, onSubmit, onCancel, initial, isEdit }) {
  const [title, setTitle] = useState(initial?.title || "");
  const [couponType, setCouponType] = useState(initial?.couponType || "percentage");
  const [discountValue, setDiscountValue] = useState(initial?.discountValue || "");
  const [budget, setBudget] = useState(initial?.budget || "");
  const [startDate, setStartDate] = useState(fmtDateInput(initial?.startDate) || "");
  const [endDate, setEndDate] = useState(fmtDateInput(initial?.endDate) || "");
  const [marketplace, setMarketplace] = useState(initial?.marketplace || "US");
  const [selectedItems, setSelectedItems] = useState(
    (initial?.items || []).map(i => i.asin) || []
  );

  const toggleItem = (asin) => {
    setSelectedItems(prev =>
      prev.includes(asin) ? prev.filter(a => a !== asin) : [...prev, asin]
    );
  };

  const handleSubmit = () => {
    onSubmit({
      title,
      coupon_type: couponType,
      discount_value: Number(discountValue),
      budget: Number(budget),
      start_date: startDate,
      end_date: endDate,
      marketplace,
      items: selectedItems.map(asin => {
        const p = products.find(pp => pp.asin === asin);
        return { asin, sku: p?.sku || "", product_name: p?.name || "" };
      }),
    });
  };

  return (
    <div className="table-card" style={{ padding: 16, marginBottom: 12 }}>
      <div style={{ ...SG(9, 700, "var(--muted)"), textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 12 }}>
        {isEdit ? "Edit Coupon" : "Create New Coupon"}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <FormRow label="Title" span={2}>
          <input type="text" value={title} onChange={e => setTitle(e.target.value)} placeholder="Coupon title" style={inputStyle} />
        </FormRow>
        <FormRow label="Coupon Type">
          <select value={couponType} onChange={e => setCouponType(e.target.value)} style={inputStyle}>
            <option value="percentage">Percentage Off</option>
            <option value="fixed">Fixed Amount Off</option>
          </select>
        </FormRow>
        <FormRow label={couponType === "percentage" ? "Discount %" : "Discount $"}>
          <input type="number" step="0.01" value={discountValue} onChange={e => setDiscountValue(e.target.value)}
            placeholder={couponType === "percentage" ? "e.g. 10" : "e.g. 5.00"} style={inputStyle} />
        </FormRow>
        <FormRow label="Budget ($)">
          <input type="number" step="1" value={budget} onChange={e => setBudget(e.target.value)} placeholder="$500" style={inputStyle} />
        </FormRow>
        <FormRow label="Marketplace">
          <select value={marketplace} onChange={e => setMarketplace(e.target.value)} style={inputStyle}>
            <option value="US">US</option>
            <option value="CA">CA</option>
          </select>
        </FormRow>
        <FormRow label="Start Date">
          <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} style={inputStyle} />
        </FormRow>
        <FormRow label="End Date">
          <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} style={inputStyle} />
        </FormRow>
      </div>

      {/* Multi-item selector */}
      <div style={{ marginTop: 12 }}>
        <div style={{ ...SG(9, 700, "var(--muted)"), textTransform: "uppercase", marginBottom: 6 }}>
          Select Items ({selectedItems.length} selected)
        </div>
        <div style={{ maxHeight: 200, overflowY: "auto", border: "1px solid var(--border)", borderRadius: 8, padding: 4 }}>
          {products.map(p => (
            <label key={p.asin} style={{
              display: "flex", alignItems: "center", gap: 8, padding: "5px 8px", borderRadius: 6, cursor: "pointer",
              background: selectedItems.includes(p.asin) ? "rgba(46,207,170,.08)" : "transparent",
            }}>
              <input type="checkbox" checked={selectedItems.includes(p.asin)} onChange={() => toggleItem(p.asin)}
                style={{ accentColor: "#2ECFAA" }} />
              <div>
                <div style={{ fontSize: 11, fontWeight: 600 }}>{p.name || p.sku || p.asin}</div>
                <div style={SG(8, 500, "var(--muted)")}>{p.asin}{p.sku ? ` · ${p.sku}` : ""}</div>
              </div>
            </label>
          ))}
          {products.length === 0 && <div style={{ padding: 10, textAlign: "center", color: "var(--muted)", fontSize: 11 }}>No products found</div>}
        </div>
      </div>

      <div style={{ display: "flex", gap: 8, marginTop: 14, justifyContent: "flex-end" }}>
        <button onClick={onCancel} style={{ ...SG(10, 700), height: 32, padding: "0 16px", borderRadius: 8, border: "1px solid var(--border)", background: "transparent", color: "var(--muted)", cursor: "pointer" }}>Cancel</button>
        <button onClick={handleSubmit} style={{ ...SG(10, 700), height: 32, padding: "0 16px", borderRadius: 8, border: "1px solid #E87830", background: "rgba(232,120,48,.12)", color: "#E87830", cursor: "pointer" }}>
          {isEdit ? "Update" : "Create"} Coupon
        </button>
      </div>
    </div>
  );
}

function FormRow({ label, children, span }) {
  return (
    <div style={{ ...(span === 2 ? { gridColumn: "span 2" } : {}) }}>
      <div style={{ ...SG(9, 600, "var(--muted)"), marginBottom: 3 }}>{label}</div>
      {children}
    </div>
  );
}

const inputStyle = {
  width: "100%", height: 30, padding: "0 10px", borderRadius: 7,
  border: "1px solid var(--border)", background: "transparent",
  color: "var(--body-text, #2A3D50)", fontSize: 11,
  fontFamily: "'Sora', sans-serif", outline: "none",
  boxSizing: "border-box",
};
