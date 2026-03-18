import { Fragment, useState, useEffect } from "react";
import { api } from "../../lib/api";
import {
  SG,
  DM,
  Card,
  CardHdr,
  fN,
  f$,
  fPct,
  COLORS,
  ChartCanvas,
} from "./WalmartHelpers";

// ── Metric formatting helpers ─────────────────────────────
const NUMBER_1D_METRICS = [
  "unit turns", "warehouse turns", "retail turns",
  "weeks of supply", "weeks on hand", "store weeks of supply",
];
const DOLLAR_OVERRIDE_METRICS = ["store returns $", "returns $"];

const getMetricFormat = (name) => {
  const n = (name || "").toLowerCase();
  if (NUMBER_1D_METRICS.some((m) => n.includes(m))) return "number1d";
  if (DOLLAR_OVERRIDE_METRICS.some((m) => n.includes(m))) return "dollar";
  if (n.includes("returns") && !n.includes("%") && !n.includes("$")) return "dollar";
  if (n.includes("%") || n.includes("instock") || n.includes("gim") || n.includes("margin") || n.includes("aur")) return "pct";
  if (n.includes("dollar") || n.includes("sales") || n.includes("$") || n.includes("mumd dol") || n.includes("cost") || n.includes("retail") || n.includes("receipt") || n.includes("ships")) return "dollar";
  if (n.includes("unit") || n.includes("qty")) return "number";
  return "number";
};

const fMetric = (v, metricName) => {
  if (v == null) return "—";
  const fmt = getMetricFormat(metricName);
  if (fmt === "number1d") {
    const num = Number(v);
    if (Math.abs(num) < 2 && Math.abs(num) > 0) return (num * 1).toFixed(1);
    return num.toFixed(1);
  }
  if (fmt === "pct") {
    const num = Number(v);
    if (Math.abs(num) < 2) return (num * 100).toFixed(1) + "%";
    return num.toFixed(1) + "%";
  }
  if (fmt === "dollar") return f$(v);
  return fN(v);
};

const fDiff = (v) => {
  if (v == null || v === 0) return { text: "—", color: "var(--txt3)" };
  const pct = Number(v) * 100;
  const color = pct >= 0 ? COLORS.teal : COLORS.red;
  const arrow = pct >= 0 ? "▲" : "▼";
  return { text: `${arrow} ${Math.abs(pct).toFixed(1)}%`, color };
};

// ── Period definitions ────────────────────────────────────
// Year moved to end (after Q4)
const ALL_PERIODS = ["Last Week", "Current Month", "Last Month", "Q1", "Q2", "Q3", "Q4", "Year"];

const SHORT_PERIOD = {
  "Last Week": "LW", "Current Month": "Cur Mo", "Last Month": "Lst Mo",
  Year: "Year", Q1: "Q1", Q2: "Q2", Q3: "Q3", Q4: "Q4",
};

const PERIOD_VIEWS = [
  { key: "all", label: "All" },
  { key: "recent", label: "Recent" },
  { key: "quarterly", label: "Quarterly" },
];

const PERIOD_VIEW_MAP = {
  all: ALL_PERIODS,
  recent: ["Last Week", "Current Month", "Last Month", "Year"],
  quarterly: ["Q1", "Q2", "Q3", "Q4", "Year"],
};

const VENDOR_ORDER = [
  "All Vendors",
  "Vendor 77893 / IMPORT-ELITE SUPPLIER GROUP IN",
  "Vendor 79010 / ELITE SUPPLIER GROUP INC",
];

const GROUP_ORDER = ["Store Sales", "Store Inventory", "DC Inventory", "Margins and Markdowns"];

const PIE_COLORS = ["#2ecf99", "#f97316", "#3b82f6", "#a78bfa", "#ef4444", "#f59e0b", "#ec4899", "#14b8a6"];

// ── KPI Period buttons — Year after Q4 ───────────────────
const KPI_PERIOD_BTNS = [
  { key: "Last Week", label: "LW" },
  { key: "Current Month", label: "Cur Mo" },
  { key: "Last Month", label: "Lst Mo" },
  { key: "Q1", label: "Q1" },
  { key: "Q2", label: "Q2" },
  { key: "Q3", label: "Q3" },
  { key: "Q4", label: "Q4" },
  { key: "Year", label: "Year" },
];

// ── Bar chart view periods ────────────────────────────────
const BAR_ALL_PERIODS = ["Last Week", "Current Month", "Last Month", "Q1", "Q2", "Q3", "Q4", "Year"];
const BAR_RECENT_PERIODS = ["Last Week", "Current Month", "Last Month", "Year"];
const BAR_QUARTERLY_PERIODS = ["Q1", "Q2", "Q3", "Q4"];

// ── Doughnut % label plugin ───────────────────────────────
const pctLabelPlugin = {
  id: "pctLabels",
  afterDraw(chart) {
    if (chart.config.type !== "doughnut" && chart.config.type !== "pie") return;
    const ctx = chart.ctx;
    const ds = chart.data.datasets[0];
    if (!ds || !ds.data) return;
    const total = ds.data.reduce((a, b) => a + b, 0);
    if (total === 0) return;
    chart.getDatasetMeta(0).data.forEach((arc, i) => {
      const pct = (ds.data[i] / total) * 100;
      if (pct < 4) return;
      const pos = arc.tooltipPosition();
      ctx.save();
      ctx.fillStyle = "#fff";
      ctx.font = "bold 11px 'Space Grotesk', monospace";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(pct.toFixed(0) + "%", pos.x, pos.y);
      ctx.restore();
    });
  },
};

// ── Small tab button style helper ─────────────────────────
const smallTab = (active, onClick, label) => (
  <span
    onClick={onClick}
    style={{
      ...SG(8, 600),
      padding: "2px 8px",
      borderRadius: 3,
      cursor: "pointer",
      background: active ? "rgba(46,207,170,0.15)" : "rgba(255,255,255,0.04)",
      color: active ? COLORS.teal : "var(--txt3)",
      border: `1px solid ${active ? "rgba(46,207,170,0.3)" : "rgba(255,255,255,0.08)"}`,
      userSelect: "none",
      display: "inline-block",
    }}
  >
    {label}
  </span>
);

// ── KPI metrics (no GMROII) ──────────────────────────────
const KPI_METRICS = ["POS Sales $", "POS Units", "Comp Sales $", "Returns $", "Returns %", "GIM%", "Repl Instock %", "AUR"];

// ── Wide chart metrics: dollar + unit metrics for TY vs LY bar chart ──
const WIDE_CHART_METRICS = [
  { key: "POS Sales $", label: "POS Sales $", fmt: "dollar" },
  { key: "POS Units", label: "POS Units", fmt: "number" },
  { key: "Comp Sales $", label: "Comp Sales $", fmt: "dollar" },
  { key: "Returns $", label: "Returns $", fmt: "dollar" },
];

// ═══════════════════════════════════════════════════════════
export function WalmartScorecard({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [kpiPeriod, setKpiPeriod] = useState("Last Week");
  const [periodView, setPeriodView] = useState("recent");
  const [activeVendor, setActiveVendor] = useState("All Vendors");
  const [barView, setBarView] = useState("recent");
  const [freezeHeaders, setFreezeHeaders] = useState(false);
  const [hideZeros, setHideZeros] = useState(false);
  const [showComparison, setShowComparison] = useState(false);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartScorecard(filters);
        setData(result);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [filters.division, filters.customer]);

  if (loading) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>Loading scorecard...</p></Card>;
  if (error) return <Card><p style={{ ...SG(12), color: "#f87171" }}>Error: {error}</p></Card>;
  if (!data) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No data</p></Card>;

  const scorecard = data.scorecard || [];
  if (scorecard.length === 0)
    return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No scorecard data loaded. Upload a Scintilla scorecard report to get started.</p></Card>;

  const periodsInData = ALL_PERIODS.filter((p) => scorecard.some((row) => row.period === p));
  const activePeriods = (PERIOD_VIEW_MAP[periodView] || ALL_PERIODS).filter((p) => periodsInData.includes(p));

  const vendorsInData = VENDOR_ORDER.filter((v) => scorecard.some((row) => row.vendorSection === v));
  const extraVendors = [...new Set(scorecard.map((r) => r.vendorSection))].filter((v) => !VENDOR_ORDER.includes(v));
  const allVendors = [...vendorsInData, ...extraVendors];

  // ── Helpers ──
  const getVal = (vendor, metric, period, field = "valueTy") => {
    const row = scorecard.find((r) => r.vendorSection === vendor && r.metricName === metric && r.period === period);
    return row ? row[field] : null;
  };
  const getNum = (vendor, metric, period, field = "valueTy") => {
    const v = getVal(vendor, metric, period, field);
    return v != null ? Number(v) : 0;
  };
  const toMarginPct = (v) => {
    if (!v) return 0;
    const n = Number(v);
    return Math.abs(n) < 2 ? n * 100 : n;
  };

  // ── KPI data for selected period ──
  const displayKpis = [];
  KPI_METRICS.forEach((metric) => {
    const row = scorecard.find((r) => r.vendorSection === "All Vendors" && r.period === kpiPeriod && r.metricName === metric);
    if (row) {
      displayKpis.push({ label: metric, ty: row.valueTy, ly: row.valueLy, diff: row.valueDiff });
    }
  });

  // ── Section map builder ──
  const buildSectionMap = (vendor) => {
    const vendorRows = scorecard.filter((r) => r.vendorSection === vendor);
    const map = {};
    vendorRows.forEach((row) => {
      const group = row.metricGroup || "General";
      if (!map[group]) map[group] = new Set();
      map[group].add(row.metricName);
    });
    const sorted = {};
    GROUP_ORDER.forEach((g) => { if (map[g]) sorted[g] = [...map[g]]; });
    Object.keys(map).forEach((g) => { if (!sorted[g]) sorted[g] = [...map[g]]; });
    return sorted;
  };

  // ── Quarter periods that exist ──
  const quarterPeriods = ["Q1", "Q2", "Q3", "Q4"].filter((p) => periodsInData.includes(p));

  // ── Bar chart periods based on view ──
  const barPeriodsSource = barView === "all" ? BAR_ALL_PERIODS : barView === "recent" ? BAR_RECENT_PERIODS : BAR_QUARTERLY_PERIODS;
  const barPeriods = barPeriodsSource.filter((p) => periodsInData.includes(p));
  const barLabels = barPeriods.map((p) => SHORT_PERIOD[p] || p);

  // ── Check if a metric row has all zeros across displayed periods ──
  const isAllZero = (vendor, metric, periods) => {
    return periods.every((p) => {
      const ty = getVal(vendor, metric, p, "valueTy");
      const ly = getVal(vendor, metric, p, "valueLy");
      return (ty == null || Number(ty) === 0) && (ly == null || Number(ly) === 0);
    });
  };

  // ── Vendor short name helper ──
  const vendorShort = (v) =>
    v === "All Vendors" ? "All Vendors" : v.includes("77893") ? "Vendor 77893" : v.includes("79010") ? "Vendor 79010" : v;

  // ── Wide chart data: TY vs LY for key dollar/unit metrics at selected kpiPeriod ──
  const wideChartLabels = WIDE_CHART_METRICS.map((m) => m.label);
  const wideChartTY = WIDE_CHART_METRICS.map((m) => Math.abs(getNum("All Vendors", m.key, kpiPeriod)));
  const wideChartLY = WIDE_CHART_METRICS.map((m) => Math.abs(getNum("All Vendors", m.key, kpiPeriod, "valueLy")));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>

      {/* ═══ KPI Period Buttons ═══ */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
        <span style={{ ...SG(9, 600), color: "var(--txt3)", marginRight: 4 }}>Period:</span>
        {KPI_PERIOD_BTNS.filter((b) => periodsInData.includes(b.key)).map((b) => (
          <button
            key={b.key}
            onClick={() => setKpiPeriod(b.key)}
            style={{
              ...SG(9, kpiPeriod === b.key ? 700 : 500),
              padding: "3px 10px",
              borderRadius: 4,
              cursor: "pointer",
              background: kpiPeriod === b.key ? "var(--acc1)" : "var(--card)",
              color: kpiPeriod === b.key ? "#fff" : "var(--txt3)",
              border: kpiPeriod === b.key ? "none" : "1px solid var(--brd)",
            }}
          >
            {b.label}
          </button>
        ))}
      </div>

      {/* ═══ KPI Cards — all fit across screen ═══ */}
      {displayKpis.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: `repeat(${displayKpis.length}, 1fr)`, gap: 8 }}>
          {displayKpis.map((kpi) => {
            const d = fDiff(kpi.diff);
            return (
              <div
                key={kpi.label}
                style={{
                  background: "var(--card)",
                  borderRadius: 10,
                  border: "1px solid var(--brd)",
                  borderTop: `3px solid ${COLORS.teal}`,
                  padding: "10px 10px",
                  minWidth: 0,
                }}
              >
                <div style={{ ...SG(7, 700), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 6, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  {kpi.label}
                </div>
                <div style={{ display: "flex", alignItems: "baseline", gap: 6, flexWrap: "wrap" }}>
                  <span style={{ ...DM(16), color: "var(--txt)" }}>{fMetric(kpi.ty, kpi.label)}</span>
                  <span style={{ ...SG(8), color: "var(--txt3)" }}>LY {fMetric(kpi.ly, kpi.label)}</span>
                  <span style={{ ...SG(8, 600), color: d.color }}>{d.text}</span>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* ═══ Wide Summary Chart — TY vs LY for selected period ═══ */}
      <Card>
        <CardHdr title={`Performance Summary — ${SHORT_PERIOD[kpiPeriod] || kpiPeriod}`} />
        <ChartCanvas
          type="bar"
          labels={wideChartLabels}
          configKey={`sc-wide-${kpiPeriod}-${activeVendor}`}
          height={200}
          datasets={[
            {
              label: "This Year (TY)",
              data: wideChartTY,
              backgroundColor: COLORS.teal,
              borderColor: COLORS.teal,
              borderWidth: 1,
            },
            {
              label: "Last Year (LY)",
              data: wideChartLY,
              backgroundColor: "rgba(249,115,22,0.6)",
              borderColor: "#f97316",
              borderWidth: 1,
            },
          ]}
          options={{
            scales: {
              y: {
                beginAtZero: true,
                ticks: {
                  callback: (v) => v >= 1000 ? "$" + (v / 1000).toFixed(0) + "k" : "$" + v.toLocaleString(),
                  color: "rgba(255,255,255,0.7)",
                },
                grid: { color: "rgba(255,255,255,0.08)" },
              },
              x: {
                ticks: { color: "rgba(255,255,255,0.7)", font: { size: 11 } },
                grid: { display: false },
              },
            },
            plugins: {
              legend: { labels: { color: "rgba(255,255,255,0.7)", font: { size: 10 } } },
              tooltip: {
                callbacks: {
                  label: (ctx) => {
                    const v = ctx.raw || 0;
                    const metric = WIDE_CHART_METRICS[ctx.dataIndex];
                    const formatted = metric && metric.fmt === "number" ? v.toLocaleString() + " units" : "$" + v.toLocaleString();
                    return `${ctx.dataset.label}: ${formatted}`;
                  },
                },
              },
            },
          }}
        />
      </Card>

      {/* ═══ Pie Charts: Quarterly Breakdown ═══ */}
      {quarterPeriods.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16 }}>
          {[
            { title: "POS Sales $ by Quarter", metric: "POS Sales $", fmt: (v) => "$" + (v || 0).toLocaleString() },
            { title: "POS Units by Quarter", metric: "POS Units", fmt: (v) => (v || 0).toLocaleString() + " units" },
            {
              title: "Gross Margin $ by Quarter",
              metric: "Gross Margin $",
              fallback: (p) => {
                const gim = getNum(activeVendor, "GIM%", p);
                const sales = getNum(activeVendor, "POS Sales $", p);
                const gimPct = Math.abs(gim) < 2 ? gim : gim / 100;
                return sales * gimPct;
              },
              fmt: (v) => "$" + (v || 0).toLocaleString(),
            },
            {
              title: "Return $ by Quarter",
              metric: "Returns $",
              altMetric: "Store Returns $",
              fmt: (v) => "$" + (v || 0).toLocaleString(),
            },
          ].map((pie) => (
            <Card key={pie.title}>
              <CardHdr title={pie.title} />
              <ChartCanvas
                type="doughnut"
                labels={quarterPeriods}
                configKey={`sc-pie-${pie.metric}-${activeVendor}`}
                height={200}
                chartPlugins={[pctLabelPlugin]}
                datasets={[
                  {
                    data: quarterPeriods.map((p) => {
                      let v = Math.abs(getNum(activeVendor, pie.metric, p));
                      if (!v && pie.altMetric) v = Math.abs(getNum(activeVendor, pie.altMetric, p));
                      if (!v && pie.fallback) v = Math.abs(pie.fallback(p));
                      return v;
                    }),
                    backgroundColor: PIE_COLORS.slice(0, quarterPeriods.length),
                    borderColor: "rgba(0,0,0,0.2)",
                    borderWidth: 1,
                  },
                ]}
                options={{
                  plugins: {
                    legend: { display: true, position: "bottom", labels: { color: "rgba(255,255,255,0.7)", font: { size: 9 } } },
                    tooltip: {
                      callbacks: {
                        label: (ctx) => {
                          const total = ctx.dataset.data.reduce((a, b) => a + b, 0);
                          const pct = total > 0 ? ((ctx.raw / total) * 100).toFixed(1) : 0;
                          return `${ctx.label}: ${pie.fmt(ctx.raw)} (${pct}% of total)`;
                        },
                      },
                    },
                  },
                }}
              />
            </Card>
          ))}
        </div>
      )}

      {/* ═══ Bar Charts: Margin + Instock with View Toggle ═══ */}
      {barPeriods.length > 0 && (
        <>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ ...SG(9, 600), color: "var(--txt3)" }}>Chart View:</span>
            {smallTab(barView === "all", () => setBarView("all"), "All")}
            {smallTab(barView === "recent", () => setBarView("recent"), "Recent")}
            {smallTab(barView === "quarterly", () => setBarView("quarterly"), "Quarterly")}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            {/* Maintained Margin vs Gross Margin */}
            <Card>
              <CardHdr title={`Maintained Margin vs Gross Margin`} />
              <ChartCanvas
                type="bar"
                labels={barLabels}
                configKey={`sc-margin-comp-${activeVendor}-${barView}-${barPeriods.length}`}
                height={220}
                datasets={[
                  {
                    label: "Gross Margin (GIM%)",
                    data: barPeriods.map((p) => toMarginPct(getVal(activeVendor, "GIM%", p))),
                    backgroundColor: COLORS.teal,
                    borderColor: COLORS.teal,
                    borderWidth: 1,
                  },
                  {
                    label: "Maintained Margin%",
                    data: barPeriods.map((p) => toMarginPct(getVal(activeVendor, "Maintain Margin%", p))),
                    backgroundColor: "#3b82f6",
                    borderColor: "#3b82f6",
                    borderWidth: 1,
                  },
                ]}
                options={{
                  scales: {
                    y: {
                      beginAtZero: true, suggestedMax: 60,
                      ticks: { callback: (v) => v.toFixed(0) + "%", color: "rgba(255,255,255,0.7)", stepSize: 10 },
                      grid: { color: "rgba(255,255,255,0.08)" },
                      title: { display: true, text: "Margin %", color: "rgba(255,255,255,0.5)", font: { size: 10 } },
                    },
                    x: { ticks: { color: "rgba(255,255,255,0.7)" }, grid: { display: false } },
                  },
                  plugins: {
                    legend: { labels: { color: "rgba(255,255,255,0.7)", font: { size: 10 } } },
                    tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%` } },
                  },
                }}
              />
            </Card>

            {/* Replenishment In Stock % */}
            <Card>
              <CardHdr title={`Replenishment In Stock %`} />
              <ChartCanvas
                type="bar"
                labels={barLabels}
                configKey={`sc-repl-instock-${activeVendor}-${barView}-${barPeriods.length}`}
                height={220}
                datasets={[
                  {
                    label: "Repl Instock % TY",
                    data: barPeriods.map((p) => toMarginPct(getVal(activeVendor, "Repl Instock %", p))),
                    backgroundColor: COLORS.teal,
                    borderColor: COLORS.teal,
                    borderWidth: 1,
                  },
                  {
                    label: "Repl Instock % LY",
                    data: barPeriods.map((p) => toMarginPct(getVal(activeVendor, "Repl Instock %", p, "valueLy"))),
                    backgroundColor: "rgba(249,115,22,0.5)",
                    borderColor: "#f97316",
                    borderWidth: 1,
                  },
                ]}
                options={{
                  scales: {
                    y: {
                      min: 80, max: 100,
                      ticks: { callback: (v) => v.toFixed(0) + "%", color: "rgba(255,255,255,0.7)", stepSize: 5 },
                      grid: { color: "rgba(255,255,255,0.08)" },
                      title: { display: true, text: "In Stock %", color: "rgba(255,255,255,0.5)", font: { size: 10 } },
                    },
                    x: { ticks: { color: "rgba(255,255,255,0.7)" }, grid: { display: false } },
                  },
                  plugins: {
                    legend: { labels: { color: "rgba(255,255,255,0.7)", font: { size: 10 } } },
                    tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%` } },
                  },
                }}
              />
            </Card>
          </div>
        </>
      )}

      {/* ═══ Controls: Period View + Vendor Tabs + Table Controls ═══ */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          {PERIOD_VIEWS.map((pv) => (
            <button
              key={pv.key}
              onClick={() => setPeriodView(pv.key)}
              style={{
                ...SG(10, periodView === pv.key ? 700 : 500),
                padding: "4px 12px", borderRadius: 4, cursor: "pointer",
                background: periodView === pv.key ? "var(--acc1)" : "var(--card)",
                color: periodView === pv.key ? "#fff" : "var(--txt3)",
                border: periodView === pv.key ? "none" : "1px solid var(--brd)",
              }}
            >
              {pv.label}
            </button>
          ))}
          <span style={{ width: 1, height: 18, background: "var(--brd)", margin: "0 4px" }} />
          {smallTab(freezeHeaders, () => setFreezeHeaders((p) => !p), freezeHeaders ? "Unfreeze Headers" : "Freeze Headers")}
          {smallTab(hideZeros, () => setHideZeros((p) => !p), hideZeros ? "Show All" : "Hide $0")}
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          {allVendors.map((v) => (
            <button
              key={v}
              onClick={() => setActiveVendor(v)}
              style={{
                ...SG(10, activeVendor === v ? 700 : 500),
                padding: "4px 12px", borderRadius: 4, cursor: "pointer",
                background: activeVendor === v ? COLORS.teal : "var(--card)",
                color: activeVendor === v ? "#fff" : "var(--txt3)",
                border: activeVendor === v ? "none" : "1px solid var(--brd)",
              }}
            >
              {vendorShort(v)}
            </button>
          ))}
        </div>
      </div>

      {/* ═══ Scorecard Detail Table ═══ */}
      {(() => {
        const groupMap = buildSectionMap(activeVendor);
        if (Object.keys(groupMap).length === 0) return null;

        // Frozen: give the container its OWN vertical scroll context (overflowY: auto +
        // maxHeight).  This means position:sticky on the thead rows resolves against the
        // container — not the window — so top:0 / top:28 work exactly right regardless of
        // the app's nav-header height or whether Show Comparison is open.
        // The CardHdr ("All Vendors — Full Scorecard") lives ABOVE the container and is
        // always visible without needing to be sticky.
        // When not frozen: overflowX only, page scrolls freely.
        const containerStyle = freezeHeaders
          ? { overflowX: "auto", overflowY: "auto", maxHeight: "62vh" }
          : { overflowX: "auto" };

        // Row 1 thead: stick at top of scroll container
        // Row 2 thead: stick just below row 1 (~28px)
        const stickyR1 = freezeHeaders ? { position: "sticky", top: 0, zIndex: 10, background: "var(--card)" } : {};
        const stickyR2 = freezeHeaders ? { position: "sticky", top: 28, zIndex: 10, background: "var(--card)" } : {};
        // Corner cells also need sticky left so the metric label column stays pinned
        const stickyCornerR1 = freezeHeaders
          ? { position: "sticky", top: 0, left: 0, zIndex: 14, background: "var(--card)" }
          : { position: "sticky", left: 0, zIndex: 12, background: "var(--card)" };
        const stickyCornerR2 = freezeHeaders
          ? { position: "sticky", top: 28, left: 0, zIndex: 14, background: "var(--card)" }
          : { position: "sticky", left: 0, zIndex: 12, background: "var(--card)" };

        return (
          <Card>
            <CardHdr
              title={activeVendor === "All Vendors" ? "All Vendors — Full Scorecard" : activeVendor}
            />
            <div style={containerStyle}>
              <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
                <thead>
                  {/* Row 1: Time period names (green font, spans 3 cols each) */}
                  <tr style={{ background: "var(--card)" }}>
                    <th
                      style={{
                        padding: "6px 8px", textAlign: "left", ...SG(9, 700), color: "var(--txt3)",
                        whiteSpace: "nowrap", background: "var(--card)", minWidth: 160,
                        borderBottom: "none",
                        ...stickyCornerR1,
                      }}
                    />
                    {activePeriods.map((period) => (
                      <th
                        key={period}
                        colSpan={3}
                        style={{
                          padding: "6px 6px 2px", textAlign: "center",
                          ...SG(9, 700), color: COLORS.teal,
                          whiteSpace: "nowrap", borderLeft: "2px solid var(--brd)",
                          borderBottom: "none", background: "var(--card)",
                          ...stickyR1,
                        }}
                      >
                        {SHORT_PERIOD[period] || period}
                      </th>
                    ))}
                  </tr>
                  {/* Row 2: TY / LY / % Change labels — TY and LY same color */}
                  <tr style={{ borderBottom: "2px solid var(--brd)", background: "var(--card)" }}>
                    <th
                      style={{
                        padding: "2px 8px 6px", textAlign: "left", ...SG(8, 700), color: "var(--txt3)",
                        whiteSpace: "nowrap", background: "var(--card)", minWidth: 160,
                        ...stickyCornerR2,
                      }}
                    >
                      Metric
                    </th>
                    {activePeriods.map((period) => (
                      <Fragment key={period}>
                        <th style={{ padding: "2px 6px 6px", textAlign: "right", ...SG(8, 600), color: "var(--txt3)", whiteSpace: "nowrap", borderLeft: "2px solid var(--brd)", background: "var(--card)", ...stickyR2 }}>
                          TY
                        </th>
                        <th style={{ padding: "2px 6px 6px", textAlign: "right", ...SG(8, 600), color: "var(--txt3)", whiteSpace: "nowrap", background: "var(--card)", ...stickyR2 }}>
                          LY
                        </th>
                        <th style={{ padding: "2px 6px 6px", textAlign: "right", ...SG(8, 600), color: "var(--txt3)", whiteSpace: "nowrap", background: "var(--card)", ...stickyR2 }}>
                          % Chg
                        </th>
                      </Fragment>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(groupMap).map(([group, metrics]) => (
                    <Fragment key={group}>
                      <tr>
                        <td
                          colSpan={1 + activePeriods.length * 3}
                          style={{
                            padding: "8px 8px 4px", ...SG(10, 700), color: COLORS.teal,
                            borderTop: "2px solid var(--brd)", background: "rgba(46,207,170,0.05)",
                            position: "sticky", left: 0,
                          }}
                        >
                          {group}
                        </td>
                      </tr>
                      {metrics.map((metric, mi) => {
                        if (hideZeros && isAllZero(activeVendor, metric, activePeriods)) return null;
                        const bgColor = mi % 2 === 0 ? "transparent" : "rgba(255,255,255,0.02)";
                        return (
                          <tr key={metric} style={{ backgroundColor: bgColor, borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                            <td
                              style={{
                                padding: "5px 8px", textAlign: "left", color: "var(--txt)", ...SG(9, 500),
                                whiteSpace: "nowrap", position: "sticky", left: 0, background: "var(--card)", zIndex: 1,
                              }}
                            >
                              {metric}
                            </td>
                            {activePeriods.map((period) => {
                              const ty = getVal(activeVendor, metric, period, "valueTy");
                              const ly = getVal(activeVendor, metric, period, "valueLy");
                              const diff = getVal(activeVendor, metric, period, "valueDiff");
                              const d = fDiff(diff);
                              return (
                                <Fragment key={`${metric}-${period}`}>
                                  <td style={{ padding: "5px 6px", textAlign: "right", color: "var(--txt)", ...SG(9), borderLeft: "2px solid var(--brd)" }}>
                                    {fMetric(ty, metric)}
                                  </td>
                                  <td style={{ padding: "5px 6px", textAlign: "right", color: "var(--txt2)", ...SG(9) }}>
                                    {fMetric(ly, metric)}
                                  </td>
                                  <td style={{ padding: "5px 6px", textAlign: "right", color: d.color, ...SG(9, 600) }}>
                                    {d.text}
                                  </td>
                                </Fragment>
                              );
                            })}
                          </tr>
                        );
                      })}
                    </Fragment>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        );
      })()}

      {/* ═══ Vendor Comparison — small tab toggle ═══ */}
      {allVendors.length > 1 && (
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
            {smallTab(showComparison, () => setShowComparison((p) => !p), showComparison ? "Hide Comparison" : "Show Comparison")}
          </div>
          {showComparison && (
            <Card>
              <CardHdr title="Vendor Comparison — Year to Date" />
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
                  <thead>
                    <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                      <th style={{ padding: "6px 8px", textAlign: "left", ...SG(9, 700), color: "var(--txt3)", position: "sticky", left: 0, background: "var(--card)", zIndex: 2, minWidth: 160 }}>
                        Metric
                      </th>
                      {allVendors.map((v) => {
                        const sn = v === "All Vendors" ? "All Vendors" : v.includes("77893") ? "V-77893" : v.includes("79010") ? "V-79010" : v.substring(0, 20);
                        return (
                          <Fragment key={v}>
                            <th style={{ padding: "6px 6px", textAlign: "right", ...SG(8, 700), color: COLORS.teal, whiteSpace: "nowrap", borderLeft: "2px solid var(--brd)" }}>
                              {sn} TY
                            </th>
                            <th style={{ padding: "6px 6px", textAlign: "right", ...SG(8, 600), color: "var(--txt3)" }}>
                              Diff
                            </th>
                          </Fragment>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      "POS Sales $", "Comp Sales $", "POS Units", "AUR", "Returns $", "Returns %",
                      "GIM%", "Repl Instock %", "Weeks of Supply", "Unit Turns", "Cost On Order", "Ships At Cost WHSE",
                    ].map((metric, mi) => {
                      if (hideZeros && allVendors.every((v) => {
                        const ty = getVal(v, metric, "Year", "valueTy");
                        return ty == null || Number(ty) === 0;
                      })) return null;
                      return (
                        <tr key={metric} style={{ borderBottom: "1px solid rgba(255,255,255,0.05)", background: mi % 2 === 0 ? "transparent" : "rgba(255,255,255,0.02)" }}>
                          <td style={{ padding: "5px 8px", ...SG(9, 500), color: "var(--txt)", position: "sticky", left: 0, background: "var(--card)", zIndex: 1 }}>
                            {metric}
                          </td>
                          {allVendors.map((v) => {
                            const ty = getVal(v, metric, "Year", "valueTy");
                            const diff = getVal(v, metric, "Year", "valueDiff");
                            const d = fDiff(diff);
                            return (
                              <Fragment key={v}>
                                <td style={{ padding: "5px 6px", textAlign: "right", color: "var(--txt)", ...SG(9), borderLeft: "2px solid var(--brd)" }}>
                                  {fMetric(ty, metric)}
                                </td>
                                <td style={{ padding: "5px 6px", textAlign: "right", color: d.color, ...SG(9, 600) }}>
                                  {d.text}
                                </td>
                              </Fragment>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </Card>
          )}
        </div>
      )}
    </div>
  );
}
