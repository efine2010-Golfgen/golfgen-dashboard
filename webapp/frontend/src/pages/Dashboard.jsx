import { useState, useEffect } from "react";
import {
  AreaChart, Area, BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, PieChart, Pie, Cell, ComposedChart
} from "recharts";
import { api, fmt$ } from "../lib/api";

const RANGES = [
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
];

const COLORS = ["#2ECFAA", "#3E658C", "#E87830", "#F5B731", "#7BAED0", "#22A387", "#D03030", "#8B5CF6", "#94a3b8", "#8892b0"];
const TOOLTIP_STYLE = { background: "#0E1F2D", border: "1px solid rgba(46,207,170,0.2)", borderRadius: 8, color: "#e0e8f0" };

// Moving average helper
function movingAvg(data, key, window) {
  return data.map((_, i) => {
    const start = Math.max(0, i - window + 1);
    const slice = data.slice(start, i + 1);
    const sum = slice.reduce((s, d) => s + (d[key] || 0), 0);
    return Math.round(sum / slice.length);
  });
}

export default function Dashboard() {
  const [days, setDays] = useState(365);
  const [summary, setSummary] = useState(null);
  const [daily, setDaily] = useState([]);
  const [pnl, setPnl] = useState(null);
  const [comparison, setComparison] = useState([]);
  const [monthlyYoY, setMonthlyYoY] = useState({ years: [], data: [] });
  const [productMix, setProductMix] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.summary(days),
      api.daily(days, days <= 90 ? "daily" : "weekly"),
      api.pnl(days),
      api.comparison(),
      api.monthlyYoY(),
      api.productMix(days),
    ]).then(([s, d, p, comp, yoy, mix]) => {
      setSummary(s);
      setDaily(d.data);
      setPnl(p);
      setComparison(comp.periods || []);
      setMonthlyYoY(yoy);
      setProductMix(mix.products || []);
      setLoading(false);
    }).catch(err => {
      console.error("API error:", err);
      setLoading(false);
    });
  }, [days]);

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading dashboard...</div>;
  }

  // Build chart data with moving averages
  const dailyRaw = daily;
  const ma7 = movingAvg(dailyRaw, "revenue", 7);
  const ma30 = movingAvg(dailyRaw, "revenue", 30);
  const dailyWithMA = dailyRaw.map((d, i) => ({
    ...d,
    ma7: ma7[i],
    ma30: ma30[i],
  }));

  // P&L waterfall
  const waterfall = pnl ? [
    { name: "Revenue", value: pnl.revenue, fill: "#2ECFAA" },
    { name: "COGS", value: -pnl.cogs, fill: "#E87830" },
    { name: "FBA Fees", value: -pnl.fbaFees, fill: "#3E658C" },
    { name: "Referral", value: -pnl.referralFees, fill: "#7BAED0" },
    { name: "Ad Spend", value: -pnl.adSpend, fill: "#F5B731" },
    { name: "Net Profit", value: pnl.netProfit, fill: pnl.netProfit >= 0 ? "#2ECFAA" : "#D03030" },
  ] : [];

  // Monthly YoY chart data
  const yoyData = monthlyYoY.data || [];
  const yoyYears = monthlyYoY.years || [];
  const yoyColors = ["#3E658C", "#2ECFAA", "#E87830", "#F5B731"];

  return (
    <>
      <div className="page-header">
        <h1>Dashboard</h1>
        <p>Amazon FBA performance overview</p>
      </div>

      {/* ── Period Comparison Table ──────────────────────── */}
      {comparison.length > 0 && (
        <div className="table-card comp-table-card">
          <h3>Performance Snapshot</h3>
          <div style={{ overflowX: "auto" }}>
            <table className="comp-table">
              <thead>
                <tr>
                  <th>Metric</th>
                  {comparison.map(c => (
                    <th key={c.label}>{c.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                <CompRow label="Revenue" values={comparison} fmt={c => fmt$(c.revenue)} chg={c => c.revChg} cls="revenue-row" />
                <CompRow label="Units Sold" values={comparison} fmt={c => c.units.toLocaleString()} chg={c => c.unitChg} />
                <CompRow label="Orders" values={comparison} fmt={c => c.orders.toLocaleString()} chg={c => c.orderChg} />
                <CompRow label="AUR" values={comparison} fmt={c => `$${c.aur.toFixed(2)}`} cls="divider-row" />
                <CompRow label="Ad Spend" values={comparison} fmt={c => fmt$(c.adSpend)} chg={c => c.adChg} />
                <CompRow label="TACOS %" values={comparison} fmt={c => `${c.tacos}%`} />
                <CompRow label="Sessions" values={comparison} fmt={c => c.sessions.toLocaleString()} chg={c => c.sessChg} />
                <CompRow label="Conv Rate" values={comparison} fmt={c => `${c.convRate}%`} cls="divider-row" />
                <CompRow label="Net Profit" values={comparison} fmt={c => fmt$(c.netProfit)} cls="profit-row" isPnl />
                <CompRow label="Margin" values={comparison} fmt={c => `${c.margin}%`} isPnl />
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Range Tabs ──────────────────────────────────── */}
      <div className="range-tabs">
        {RANGES.map(r => (
          <button
            key={r.days}
            className={`range-tab ${days === r.days ? "active" : ""}`}
            onClick={() => setDays(r.days)}
          >
            {r.label}
          </button>
        ))}
      </div>

      {/* ── KPI Cards ──────────────────────────────────── */}
      {summary && (
        <div className="kpi-grid">
          <KPI label="Revenue" value={fmt$(summary.revenue)} className="teal" />
          <KPI label="Units Sold" value={summary.units.toLocaleString()} />
          <KPI label="Orders" value={summary.orders.toLocaleString()} />
          <KPI label="Sessions" value={summary.sessions.toLocaleString()} />
          <KPI label="Avg AUR" value={`$${summary.aur.toFixed(2)}`} />
          <KPI label="Conv Rate" value={`${summary.convRate}%`} />
          <KPI label="Net Profit" value={fmt$(summary.netProfit)} className={summary.netProfit >= 0 ? "pos" : "neg"} />
          <KPI label="Margin" value={`${summary.margin}%`} className={summary.margin >= 0 ? "pos" : "neg"} />
        </div>
      )}

      {/* ── Charts: Row 1 ──────────────────────────────── */}
      <div className="chart-grid">
        {/* 1. Daily Sales + Moving Averages */}
        <div className="chart-card">
          <h3>Daily Sales & Moving Averages</h3>
          <p className="sub">{days <= 90 ? "Daily" : "Weekly"} revenue with 7-day and 30-day MA</p>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={dailyWithMA}>
              <defs>
                <linearGradient id="revGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3E658C" stopOpacity={0.15} />
                  <stop offset="95%" stopColor="#3E658C" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis dataKey="date" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
              <YAxis tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [`$${v.toLocaleString()}`, ""]} labelFormatter={l => l} />
              <Legend />
              <Area type="monotone" dataKey="revenue" name="Revenue" stroke="#3E658C" strokeWidth={1.5} fill="url(#revGrad)" />
              <Line type="monotone" dataKey="ma7" name="7-Day MA" stroke="#E87830" strokeWidth={2.5} strokeDasharray="5 4" dot={false} />
              <Line type="monotone" dataKey="ma30" name="30-Day MA" stroke="#2ECFAA" strokeWidth={2.5} strokeDasharray="5 4" dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* 2. Ad Spend vs Revenue */}
        <div className="chart-card">
          <h3>Ad Spend vs Revenue</h3>
          <p className="sub">Weekly ad spend with TACOS trend</p>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={dailyRaw}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis dataKey="date" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
              <YAxis yAxisId="left" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `${v}%`} />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Legend />
              <Bar yAxisId="left" dataKey="revenue" name="Revenue" fill="#2ECFAA" radius={[3,3,0,0]} opacity={0.6} />
              <Line yAxisId="right" type="monotone" dataKey="convRate" name="Conv %" stroke="#F5B731" strokeWidth={2} dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ── Charts: Row 2 ──────────────────────────────── */}
      <div className="chart-grid">
        {/* 3. AUR & Unit Velocity */}
        <div className="chart-card">
          <h3>AUR & Unit Velocity</h3>
          <p className="sub">Average unit retail vs daily unit volume</p>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={dailyRaw}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis dataKey="date" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
              <YAxis yAxisId="left" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `$${v}`} label={{ value: "AUR $", angle: -90, position: "insideLeft", fill: "#8892b0", fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: "#8892b0", fontSize: 11 }} label={{ value: "Units", angle: 90, position: "insideRight", fill: "#8892b0", fontSize: 11 }} />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Legend />
              <Area yAxisId="left" type="monotone" dataKey="aur" name="AUR ($)" stroke="#3E658C" fill="rgba(62,101,140,0.08)" strokeWidth={2} />
              <Line yAxisId="right" type="monotone" dataKey="units" name="Units/Day" stroke="#E87830" strokeWidth={2} strokeDasharray="4 4" dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* 4. Traffic & Conversion Funnel */}
        <div className="chart-card">
          <h3>Traffic & Conversion Funnel</h3>
          <p className="sub">Sessions, orders, units with conversion rate overlay</p>
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={dailyRaw}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis dataKey="date" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
              <YAxis yAxisId="left" tick={{ fill: "#8892b0", fontSize: 11 }} label={{ value: "Count", angle: -90, position: "insideLeft", fill: "#8892b0", fontSize: 11 }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `${v}%`} domain={[0, 15]} label={{ value: "Conv %", angle: 90, position: "insideRight", fill: "#8892b0", fontSize: 11 }} />
              <Tooltip contentStyle={TOOLTIP_STYLE} />
              <Legend />
              <Area yAxisId="left" type="monotone" dataKey="sessions" name="Sessions" stroke="#3E658C" fill="rgba(62,101,140,0.08)" strokeWidth={2} />
              <Area yAxisId="left" type="monotone" dataKey="orders" name="Orders" stroke="#2ECFAA" fill="rgba(46,207,170,0.08)" strokeWidth={2} />
              <Area yAxisId="left" type="monotone" dataKey="units" name="Units" stroke="#E87830" fill="rgba(232,120,48,0.08)" strokeWidth={2} />
              <Line yAxisId="right" type="monotone" dataKey="convRate" name="Conv Rate %" stroke="#F5B731" strokeWidth={2.5} strokeDasharray="5 4" dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ── Charts: Row 3 ──────────────────────────────── */}
      <div className="chart-grid">
        {/* 5. Revenue by Product (Donut) */}
        <div className="chart-card">
          <h3>Revenue by Product (Top 10)</h3>
          <p className="sub">Product revenue distribution</p>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={productMix}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={110}
                paddingAngle={2}
                dataKey="value"
                nameKey="name"
              >
                {productMix.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt$(v), "Revenue"]} />
              <Legend
                layout="vertical"
                align="right"
                verticalAlign="middle"
                formatter={(value) => <span style={{ color: "#e0e8f0", fontSize: 11 }}>{value.length > 20 ? value.slice(0, 18) + "…" : value}</span>}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* 6. Monthly Revenue Year-over-Year */}
        <div className="chart-card">
          <h3>Monthly Revenue Year-over-Year</h3>
          <p className="sub">Revenue by month across years</p>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={yoyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis dataKey="month" tick={{ fill: "#8892b0", fontSize: 11 }} />
              <YAxis tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
              <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt$(v), "Revenue"]} />
              <Legend />
              {yoyYears.map((yr, i) => (
                <Bar key={yr} dataKey={String(yr)} name={String(yr)} fill={yoyColors[i % yoyColors.length]} radius={[3,3,0,0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ── Charts: Row 4 ──────────────────────────────── */}
      <div className="chart-grid">
        {/* 7. P&L Waterfall */}
        <div className="chart-card">
          <h3>Revenue → Net Profit Breakdown</h3>
          <p className="sub">P&L waterfall for selected period</p>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={waterfall}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis dataKey="name" tick={{ fill: "#8892b0", fontSize: 11 }} />
              <YAxis tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `$${(Math.abs(v)/1000).toFixed(0)}k`} />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                formatter={(v) => [fmt$(Math.abs(v)), v < 0 ? "Cost" : "Amount"]}
              />
              <Bar dataKey="value" radius={[4,4,0,0]}>
                {waterfall.map((entry, i) => (
                  <Cell key={i} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* 8. Conversion Rate Trend */}
        <div className="chart-card">
          <h3>Conversion Rate</h3>
          <p className="sub">{days <= 90 ? "Daily" : "Weekly"} session-to-order rate</p>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={dailyRaw}>
              <defs>
                <linearGradient id="convGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#E87830" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#E87830" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis dataKey="date" tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
              <YAxis tick={{ fill: "#8892b0", fontSize: 11 }} tickFormatter={v => `${v}%`} />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                formatter={(v) => [`${v}%`, "Conv Rate"]}
              />
              <Area type="monotone" dataKey="convRate" stroke="#E87830" strokeWidth={2} fill="url(#convGrad)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </>
  );
}

/* ── Sub-Components ──────────────────────────────────────── */

function KPI({ label, value, className = "" }) {
  return (
    <div className="kpi-card">
      <div className="kpi-label">{label}</div>
      <div className={`kpi-value ${className}`}>{value}</div>
    </div>
  );
}

function CompRow({ label, values, fmt, chg, cls = "", isPnl = false }) {
  return (
    <tr className={cls}>
      <td className="comp-metric">{label}</td>
      {values.map((c, i) => {
        const val = fmt(c);
        const change = chg ? chg(c) : null;
        const changeClass = change > 0 ? (isPnl ? "pos" : "pos") : change < 0 ? (isPnl ? "neg" : "neg") : "";
        return (
          <td key={i}>
            <span className="comp-value">{val}</span>
            {change !== null && change !== 0 && (
              <span className={`comp-chg ${changeClass}`}>
                {change > 0 ? "▲" : "▼"} {Math.abs(change)}% YoY
              </span>
            )}
          </td>
        );
      })}
    </tr>
  );
}
