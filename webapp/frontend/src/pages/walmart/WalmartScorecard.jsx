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
  KPICard,
  ChartCanvas,
} from "./WalmartHelpers";

// ── Metric formatting helpers ─────────────────────────────
// Metrics that should NEVER be formatted as percentages
const NUMBER_1D_METRICS = [
  "unit turns",
  "warehouse turns",
  "retail turns",
  "weeks of supply",
  "weeks on hand",
  "store weeks of supply",
];

const DOLLAR_OVERRIDE_METRICS = ["store returns $", "returns $"];

const getMetricFormat = (name) => {
  const n = (name || "").toLowerCase();

  // Check explicit number-with-1-decimal overrides first
  if (NUMBER_1D_METRICS.some((m) => n.includes(m))) return "number1d";

  // Check explicit dollar overrides (returns $ should be dollar, not pct)
  if (DOLLAR_OVERRIDE_METRICS.some((m) => n.includes(m))) return "dollar";

  // "Returns %" is a pct, but "Store Returns" without % is a dollar amount
  // Only match "returns" if it also has "%"
  if (n.includes("returns") && !n.includes("%") && !n.includes("$"))
    return "dollar";

  if (
    n.includes("%") ||
    n.includes("instock") ||
    n.includes("gim") ||
    n.includes("margin") ||
    n.includes("gmroii") ||
    n.includes("aur")
  )
    return "pct";
  if (
    n.includes("dollar") ||
    n.includes("sales") ||
    n.includes("$") ||
    n.includes("mumd dol") ||
    n.includes("cost") ||
    n.includes("retail") ||
    n.includes("receipt") ||
    n.includes("ships")
  )
    return "dollar";
  if (n.includes("unit") || n.includes("qty")) return "number";
  return "number";
};

const fMetric = (v, metricName) => {
  if (v == null) return "—";
  const fmt = getMetricFormat(metricName);
  if (fmt === "number1d") {
    const num = Number(v);
    // If stored as decimal < 2, it's likely a ratio — multiply
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
const ALL_PERIODS = [
  "Last Week",
  "Current Month",
  "Last Month",
  "Year",
  "Q1",
  "Q2",
  "Q3",
  "Q4",
];

const SHORT_PERIOD = {
  "Last Week": "Lst Wk",
  "Current Month": "Cur Mo",
  "Last Month": "Lst Mo",
  Year: "Year",
  Q1: "Q1",
  Q2: "Q2",
  Q3: "Q3",
  Q4: "Q4",
};

const PERIOD_VIEWS = [
  { key: "all", label: "All Periods" },
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

const GROUP_ORDER = [
  "Store Sales",
  "Store Inventory",
  "DC Inventory",
  "Margins and Markdowns",
];

// Pie chart color palettes
const PIE_COLORS = [
  "#2ecf99", "#f97316", "#3b82f6", "#a78bfa",
  "#ef4444", "#f59e0b", "#ec4899", "#14b8a6",
];

export function WalmartScorecard({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [periodView, setPeriodView] = useState("all");
  const [activeVendor, setActiveVendor] = useState("All Vendors");
  const [showComparison, setShowComparison] = useState(true);

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

  if (loading)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>Loading scorecard...</p>
      </Card>
    );
  if (error)
    return (
      <Card>
        <p style={{ ...SG(12), color: "#f87171" }}>Error: {error}</p>
      </Card>
    );
  if (!data)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>No data</p>
      </Card>
    );

  const scorecard = data.scorecard || [];
  if (scorecard.length === 0)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>
          No scorecard data loaded. Upload a Scintilla scorecard report to get started.
        </p>
      </Card>
    );

  // Determine which periods actually exist in the data
  const periodsInData = ALL_PERIODS.filter((p) =>
    scorecard.some((row) => row.period === p)
  );
  const activePeriods = (PERIOD_VIEW_MAP[periodView] || ALL_PERIODS).filter(
    (p) => periodsInData.includes(p)
  );

  // Available vendors
  const vendorsInData = VENDOR_ORDER.filter((v) =>
    scorecard.some((row) => row.vendorSection === v)
  );
  const extraVendors = [
    ...new Set(scorecard.map((r) => r.vendorSection)),
  ].filter((v) => !VENDOR_ORDER.includes(v));
  const allVendors = [...vendorsInData, ...extraVendors];

  // ── KPI Cards from "All Vendors" + "Last Week" ──
  const kpiMetrics = [
    "POS Sales $",
    "POS Units",
    "Comp Sales $",
    "Returns %",
    "GIM%",
    "GMROII",
    "Repl Instock %",
    "AUR",
  ];
  const displayKpis = [];
  kpiMetrics.forEach((metric) => {
    const row = scorecard.find(
      (r) =>
        r.vendorSection === "All Vendors" &&
        r.period === "Last Week" &&
        r.metricName === metric
    );
    if (row) {
      displayKpis.push({
        label: metric,
        value: fMetric(row.valueTy, metric),
        delta: row.valueDiff ? (row.valueDiff * 100).toFixed(1) : null,
      });
    }
  });

  // ── Helper: get metric value for a vendor + period ──
  const getVal = (vendor, metric, period, field = "valueTy") => {
    const row = scorecard.find(
      (r) =>
        r.vendorSection === vendor &&
        r.metricName === metric &&
        r.period === period
    );
    return row ? row[field] : null;
  };

  // Helper: get numeric value (handles null → 0)
  const getNum = (vendor, metric, period, field = "valueTy") => {
    const v = getVal(vendor, metric, period, field);
    return v != null ? Number(v) : 0;
  };

  // ── Build section map for tables ──
  const buildSectionMap = (vendor) => {
    const vendorRows = scorecard.filter((r) => r.vendorSection === vendor);
    const map = {};
    vendorRows.forEach((row) => {
      const group = row.metricGroup || "General";
      if (!map[group]) map[group] = new Set();
      map[group].add(row.metricName);
    });
    const sorted = {};
    GROUP_ORDER.forEach((g) => {
      if (map[g]) sorted[g] = [...map[g]];
    });
    Object.keys(map).forEach((g) => {
      if (!sorted[g]) sorted[g] = [...map[g]];
    });
    return sorted;
  };

  // ── Quarter periods that exist ──
  const quarterPeriods = ["Q1", "Q2", "Q3", "Q4"].filter((p) =>
    periodsInData.includes(p)
  );

  // ── Pie chart data builders ──
  // Each pie shows per-quarter breakdown for a given metric
  const buildPieData = (metricName) => {
    return {
      labels: quarterPeriods,
      datasets: [
        {
          data: quarterPeriods.map((p) =>
            Math.abs(getNum(activeVendor, metricName, p))
          ),
          backgroundColor: quarterPeriods.map((_, i) => PIE_COLORS[i % PIE_COLORS.length]),
          borderColor: "rgba(0,0,0,0.2)",
          borderWidth: 1,
        },
      ],
    };
  };

  // Helper: convert margin value to percentage display
  const toMarginPct = (v) => {
    if (!v) return 0;
    const n = Number(v);
    return Math.abs(n) < 2 ? n * 100 : n;
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      {/* ═══ KPI Cards ═══ */}
      {displayKpis.length > 0 && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: `repeat(${Math.min(8, displayKpis.length)}, 1fr)`,
            gap: 10,
          }}
        >
          {displayKpis.map((kpi) => (
            <KPICard
              key={kpi.label}
              label={kpi.label}
              value={kpi.value}
              delta={kpi.delta}
            />
          ))}
        </div>
      )}

      {/* ═══ Pie Charts: Quarterly Breakdown ═══ */}
      {quarterPeriods.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16 }}>
          {/* Pie 1: POS Sales $ */}
          <Card>
            <CardHdr title="POS Sales $ by Quarter" />
            <ChartCanvas
              type="doughnut"
              labels={quarterPeriods}
              configKey={`sc-pie-pos$-${activeVendor}`}
              height={200}
              datasets={[
                {
                  data: quarterPeriods.map((p) =>
                    Math.abs(getNum(activeVendor, "POS Sales $", p))
                  ),
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
                      label: (ctx) => `${ctx.label}: $${(ctx.raw || 0).toLocaleString()}`,
                    },
                  },
                },
              }}
            />
          </Card>

          {/* Pie 2: POS Units */}
          <Card>
            <CardHdr title="POS Units by Quarter" />
            <ChartCanvas
              type="doughnut"
              labels={quarterPeriods}
              configKey={`sc-pie-units-${activeVendor}`}
              height={200}
              datasets={[
                {
                  data: quarterPeriods.map((p) =>
                    Math.abs(getNum(activeVendor, "POS Units", p))
                  ),
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
                      label: (ctx) => `${ctx.label}: ${(ctx.raw || 0).toLocaleString()} units`,
                    },
                  },
                },
              }}
            />
          </Card>

          {/* Pie 3: Gross Margin $ */}
          <Card>
            <CardHdr title="Gross Margin $ by Quarter" />
            <ChartCanvas
              type="doughnut"
              labels={quarterPeriods}
              configKey={`sc-pie-gm$-${activeVendor}`}
              height={200}
              datasets={[
                {
                  data: quarterPeriods.map((p) => {
                    // Try "Gross Margin $" or calculate from GIM% * POS Sales $
                    let v = getNum(activeVendor, "Gross Margin $", p);
                    if (!v) {
                      const gim = getNum(activeVendor, "GIM%", p);
                      const sales = getNum(activeVendor, "POS Sales $", p);
                      const gimPct = Math.abs(gim) < 2 ? gim : gim / 100;
                      v = sales * gimPct;
                    }
                    return Math.abs(v);
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
                      label: (ctx) => `${ctx.label}: $${(ctx.raw || 0).toLocaleString()}`,
                    },
                  },
                },
              }}
            />
          </Card>

          {/* Pie 4: Return $ */}
          <Card>
            <CardHdr title="Return $ by Quarter" />
            <ChartCanvas
              type="doughnut"
              labels={quarterPeriods}
              configKey={`sc-pie-ret$-${activeVendor}`}
              height={200}
              datasets={[
                {
                  data: quarterPeriods.map((p) => {
                    let v = getNum(activeVendor, "Store Returns $", p);
                    if (!v) v = getNum(activeVendor, "Returns $", p);
                    return Math.abs(v);
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
                      label: (ctx) => `${ctx.label}: $${(ctx.raw || 0).toLocaleString()}`,
                    },
                  },
                },
              }}
            />
          </Card>
        </div>
      )}

      {/* ═══ Bar Charts: Margin Comparison + Repl Instock ═══ */}
      {quarterPeriods.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          {/* Maintained Margin vs Gross Margin by Quarter */}
          <Card>
            <CardHdr title="Maintained Margin vs Gross Margin — Quarterly" />
            <ChartCanvas
              type="bar"
              labels={quarterPeriods}
              configKey={`sc-margin-comp-${activeVendor}-${quarterPeriods.length}`}
              height={220}
              datasets={[
                {
                  label: "Gross Margin (GIM%)",
                  data: quarterPeriods.map((p) =>
                    toMarginPct(getVal(activeVendor, "GIM%", p))
                  ),
                  backgroundColor: COLORS.teal,
                  borderColor: COLORS.teal,
                  borderWidth: 1,
                },
                {
                  label: "Maintained Margin%",
                  data: quarterPeriods.map((p) =>
                    toMarginPct(getVal(activeVendor, "Maintain Margin%", p))
                  ),
                  backgroundColor: "#3b82f6",
                  borderColor: "#3b82f6",
                  borderWidth: 1,
                },
              ]}
              options={{
                scales: {
                  y: {
                    beginAtZero: true,
                    suggestedMax: 60,
                    ticks: {
                      callback: (v) => v.toFixed(0) + "%",
                      color: "rgba(255,255,255,0.7)",
                      stepSize: 10,
                    },
                    grid: { color: "rgba(255,255,255,0.08)" },
                    title: { display: true, text: "Margin %", color: "rgba(255,255,255,0.5)", font: { size: 10 } },
                  },
                  x: {
                    ticks: { color: "rgba(255,255,255,0.7)" },
                    grid: { display: false },
                  },
                },
                plugins: {
                  legend: { labels: { color: "rgba(255,255,255,0.7)", font: { size: 10 } } },
                  tooltip: {
                    callbacks: {
                      label: (ctx) => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`,
                    },
                  },
                },
              }}
            />
          </Card>

          {/* Replenishment In Stock % by Quarter */}
          <Card>
            <CardHdr title="Replenishment In Stock % — Quarterly" />
            <ChartCanvas
              type="bar"
              labels={quarterPeriods}
              configKey={`sc-repl-instock-${activeVendor}-${quarterPeriods.length}`}
              height={220}
              datasets={[
                {
                  label: "Repl Instock % TY",
                  data: quarterPeriods.map((p) =>
                    toMarginPct(getVal(activeVendor, "Repl Instock %", p))
                  ),
                  backgroundColor: COLORS.teal,
                  borderColor: COLORS.teal,
                  borderWidth: 1,
                },
                {
                  label: "Repl Instock % LY",
                  data: quarterPeriods.map((p) =>
                    toMarginPct(getVal(activeVendor, "Repl Instock %", p, "valueLy"))
                  ),
                  backgroundColor: "rgba(249,115,22,0.5)",
                  borderColor: "#f97316",
                  borderWidth: 1,
                },
              ]}
              options={{
                scales: {
                  y: {
                    min: 80,
                    max: 100,
                    ticks: {
                      callback: (v) => v.toFixed(0) + "%",
                      color: "rgba(255,255,255,0.7)",
                      stepSize: 5,
                    },
                    grid: { color: "rgba(255,255,255,0.08)" },
                    title: { display: true, text: "In Stock %", color: "rgba(255,255,255,0.5)", font: { size: 10 } },
                  },
                  x: {
                    ticks: { color: "rgba(255,255,255,0.7)" },
                    grid: { display: false },
                  },
                },
                plugins: {
                  legend: { labels: { color: "rgba(255,255,255,0.7)", font: { size: 10 } } },
                  tooltip: {
                    callbacks: {
                      label: (ctx) => `${ctx.dataset.label}: ${ctx.raw.toFixed(1)}%`,
                    },
                  },
                },
              }}
            />
          </Card>
        </div>
      )}

      {/* ═══ Period View Selector + Vendor Tabs ═══ */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 8 }}>
        <div style={{ display: "flex", gap: 6 }}>
          {PERIOD_VIEWS.map((pv) => (
            <button
              key={pv.key}
              onClick={() => setPeriodView(pv.key)}
              style={{
                ...SG(10, periodView === pv.key ? 700 : 500),
                padding: "4px 12px",
                borderRadius: 4,
                cursor: "pointer",
                background: periodView === pv.key ? "var(--acc1)" : "var(--card)",
                color: periodView === pv.key ? "#fff" : "var(--txt3)",
                border: periodView === pv.key ? "none" : "1px solid var(--brd)",
              }}
            >
              {pv.label}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          {allVendors.map((v) => {
            const shortName =
              v === "All Vendors"
                ? "All Vendors"
                : v.includes("77893")
                ? "Vendor 77893"
                : v.includes("79010")
                ? "Vendor 79010"
                : v;
            return (
              <button
                key={v}
                onClick={() => setActiveVendor(v)}
                style={{
                  ...SG(10, activeVendor === v ? 700 : 500),
                  padding: "4px 12px",
                  borderRadius: 4,
                  cursor: "pointer",
                  background: activeVendor === v ? COLORS.teal : "var(--card)",
                  color: activeVendor === v ? "#fff" : "var(--txt3)",
                  border: activeVendor === v ? "none" : "1px solid var(--brd)",
                }}
              >
                {shortName}
              </button>
            );
          })}
        </div>
      </div>

      {/* ═══ Scorecard Table for Active Vendor ═══ */}
      {(() => {
        const groupMap = buildSectionMap(activeVendor);
        if (Object.keys(groupMap).length === 0) return null;

        return (
          <Card>
            <CardHdr
              title={
                activeVendor === "All Vendors"
                  ? "All Vendors — Full Scorecard"
                  : activeVendor
              }
            />
            <div style={{ overflowX: "auto" }}>
              <table
                style={{
                  width: "100%",
                  borderCollapse: "collapse",
                  ...SG(10),
                }}
              >
                <thead>
                  <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                    <th
                      style={{
                        padding: "6px 8px",
                        textAlign: "left",
                        ...SG(9, 700),
                        color: "var(--txt3)",
                        whiteSpace: "nowrap",
                        position: "sticky",
                        left: 0,
                        background: "var(--card)",
                        zIndex: 2,
                        minWidth: 160,
                      }}
                    >
                      Metric
                    </th>
                    {activePeriods.map((period) => (
                      <Fragment key={period}>
                        <th
                          style={{
                            padding: "6px 6px",
                            textAlign: "right",
                            ...SG(8, 700),
                            color: COLORS.teal,
                            whiteSpace: "nowrap",
                            borderLeft: "2px solid var(--brd)",
                          }}
                        >
                          {SHORT_PERIOD[period] || period} TY
                        </th>
                        <th
                          style={{
                            padding: "6px 6px",
                            textAlign: "right",
                            ...SG(8, 600),
                            color: "var(--txt3)",
                            whiteSpace: "nowrap",
                          }}
                        >
                          LY
                        </th>
                        <th
                          style={{
                            padding: "6px 6px",
                            textAlign: "right",
                            ...SG(8, 600),
                            color: "var(--txt3)",
                            whiteSpace: "nowrap",
                          }}
                        >
                          Diff
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
                            padding: "8px 8px 4px",
                            ...SG(10, 700),
                            color: COLORS.teal,
                            borderTop: "2px solid var(--brd)",
                            background: "rgba(46,207,170,0.05)",
                            position: "sticky",
                            left: 0,
                          }}
                        >
                          {group}
                        </td>
                      </tr>
                      {metrics.map((metric, mi) => {
                        const bgColor =
                          mi % 2 === 0
                            ? "transparent"
                            : "rgba(255,255,255,0.02)";
                        return (
                          <tr
                            key={metric}
                            style={{
                              backgroundColor: bgColor,
                              borderBottom: "1px solid rgba(255,255,255,0.05)",
                            }}
                          >
                            <td
                              style={{
                                padding: "5px 8px",
                                textAlign: "left",
                                color: "var(--txt)",
                                ...SG(9, 500),
                                whiteSpace: "nowrap",
                                position: "sticky",
                                left: 0,
                                background: "var(--card)",
                                zIndex: 1,
                              }}
                            >
                              {metric}
                            </td>
                            {activePeriods.map((period) => {
                              const ty = getVal(
                                activeVendor,
                                metric,
                                period,
                                "valueTy"
                              );
                              const ly = getVal(
                                activeVendor,
                                metric,
                                period,
                                "valueLy"
                              );
                              const diff = getVal(
                                activeVendor,
                                metric,
                                period,
                                "valueDiff"
                              );
                              const d = fDiff(diff);
                              return (
                                <Fragment key={`${metric}-${period}`}>
                                  <td
                                    style={{
                                      padding: "5px 6px",
                                      textAlign: "right",
                                      color: "var(--txt)",
                                      ...SG(9),
                                      borderLeft: "2px solid var(--brd)",
                                    }}
                                  >
                                    {fMetric(ty, metric)}
                                  </td>
                                  <td
                                    style={{
                                      padding: "5px 6px",
                                      textAlign: "right",
                                      color: "var(--txt2)",
                                      ...SG(9),
                                    }}
                                  >
                                    {fMetric(ly, metric)}
                                  </td>
                                  <td
                                    style={{
                                      padding: "5px 6px",
                                      textAlign: "right",
                                      color: d.color,
                                      ...SG(9, 600),
                                    }}
                                  >
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

      {/* ═══ Vendor Comparison Toggle + Table ═══ */}
      {allVendors.length > 1 && (
        <div>
          <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 8 }}>
            <button
              onClick={() => setShowComparison((prev) => !prev)}
              style={{
                ...SG(10, 600),
                padding: "5px 14px",
                borderRadius: 4,
                cursor: "pointer",
                background: showComparison ? "var(--card)" : "var(--acc1)",
                color: showComparison ? "var(--txt3)" : "#fff",
                border: "1px solid var(--brd)",
              }}
            >
              {showComparison ? "Hide Vendor Comparison" : "Show Vendor Comparison"}
            </button>
          </div>
          {showComparison && (
            <Card>
              <CardHdr title="Vendor Comparison — Year to Date" />
              <div style={{ overflowX: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    ...SG(10),
                  }}
                >
                  <thead>
                    <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                      <th
                        style={{
                          padding: "6px 8px",
                          textAlign: "left",
                          ...SG(9, 700),
                          color: "var(--txt3)",
                          position: "sticky",
                          left: 0,
                          background: "var(--card)",
                          zIndex: 2,
                          minWidth: 160,
                        }}
                      >
                        Metric
                      </th>
                      {allVendors.map((v) => {
                        const shortName =
                          v === "All Vendors"
                            ? "All Vendors"
                            : v.includes("77893")
                            ? "V-77893"
                            : v.includes("79010")
                            ? "V-79010"
                            : v.substring(0, 20);
                        return (
                          <Fragment key={v}>
                            <th
                              style={{
                                padding: "6px 6px",
                                textAlign: "right",
                                ...SG(8, 700),
                                color: COLORS.teal,
                                whiteSpace: "nowrap",
                                borderLeft: "2px solid var(--brd)",
                              }}
                            >
                              {shortName} TY
                            </th>
                            <th
                              style={{
                                padding: "6px 6px",
                                textAlign: "right",
                                ...SG(8, 600),
                                color: "var(--txt3)",
                              }}
                            >
                              Diff
                            </th>
                          </Fragment>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {(() => {
                      const keyMetrics = [
                        "POS Sales $",
                        "Comp Sales $",
                        "POS Units",
                        "AUR",
                        "Returns %",
                        "GIM%",
                        "GMROII",
                        "Repl Instock %",
                        "Weeks of Supply",
                        "Unit Turns",
                        "Cost On Order",
                        "Ships At Cost WHSE",
                      ];
                      return keyMetrics.map((metric, mi) => (
                        <tr
                          key={metric}
                          style={{
                            borderBottom: "1px solid rgba(255,255,255,0.05)",
                            background:
                              mi % 2 === 0
                                ? "transparent"
                                : "rgba(255,255,255,0.02)",
                          }}
                        >
                          <td
                            style={{
                              padding: "5px 8px",
                              ...SG(9, 500),
                              color: "var(--txt)",
                              position: "sticky",
                              left: 0,
                              background: "var(--card)",
                              zIndex: 1,
                            }}
                          >
                            {metric}
                          </td>
                          {allVendors.map((v) => {
                            const ty = getVal(v, metric, "Year", "valueTy");
                            const diff = getVal(v, metric, "Year", "valueDiff");
                            const d = fDiff(diff);
                            return (
                              <Fragment key={v}>
                                <td
                                  style={{
                                    padding: "5px 6px",
                                    textAlign: "right",
                                    color: "var(--txt)",
                                    ...SG(9),
                                    borderLeft: "2px solid var(--brd)",
                                  }}
                                >
                                  {fMetric(ty, metric)}
                                </td>
                                <td
                                  style={{
                                    padding: "5px 6px",
                                    textAlign: "right",
                                    color: d.color,
                                    ...SG(9, 600),
                                  }}
                                >
                                  {d.text}
                                </td>
                              </Fragment>
                            );
                          })}
                        </tr>
                      ));
                    })()}
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
