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

// Detect metric formatting type from name
const getMetricFormat = (name) => {
  const n = (name || "").toLowerCase();
  if (
    n.includes("%") ||
    n.includes("instock") ||
    n.includes("gim") ||
    n.includes("margin") ||
    n.includes("gmroii") ||
    n.includes("turns") ||
    n.includes("aur")
  )
    return "pct";
  if (
    n.includes("dollar") ||
    n.includes("sales") ||
    n.includes("$") ||
    n.includes("mumd") ||
    n.includes("cost") ||
    n.includes("retail") ||
    n.includes("receipt")
  )
    return "dollar";
  return "number";
};

// Format value based on metric type
const fMetric = (v, metricName) => {
  if (v == null) return "—";
  const fmt = getMetricFormat(metricName);
  if (fmt === "pct") {
    const num = Number(v);
    if (Math.abs(num) < 2) return (num * 100).toFixed(1) + "%";
    return num.toFixed(1) + "%";
  }
  if (fmt === "dollar") return f$(v);
  return fN(v);
};

// Format diff value
const fDiff = (v) => {
  if (v == null || v === 0) return { text: "—", color: "var(--txt3)" };
  const pct = Number(v) * 100;
  const color = pct >= 0 ? COLORS.teal : COLORS.red;
  const arrow = pct >= 0 ? "▲" : "▼";
  return { text: `${arrow} ${Math.abs(pct).toFixed(1)}%`, color };
};

export function WalmartScorecard({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);

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
        <p style={{ ...SG(12), color: "var(--txt3)" }}>Loading...</p>
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

  // Extract KPIs from "All Vendors" + "Last Week" rows
  const displayKpis = [];
  if (scorecard.length > 0) {
    const lwRows = scorecard.filter(
      (r) => r.vendorSection === "All Vendors" && r.period === "Last Week"
    );
    const seen = new Set();
    lwRows.forEach((r) => {
      if (displayKpis.length >= 6 || seen.has(r.metricName)) return;
      seen.add(r.metricName);
      displayKpis.push({
        label: r.metricName,
        value: fMetric(r.valueTy, r.metricName),
        delta: r.valueDiff ? (r.valueDiff * 100).toFixed(1) : null,
      });
    });
  }

  // Extract unique periods and sort in preferred order
  const periodOrder = [
    "Last Week",
    "4-Week",
    "13-Week",
    "26-Week",
    "52-Week",
    "Year",
  ];
  const periodsInData = periodOrder.filter((p) =>
    scorecard.some((row) => row.period === p)
  );

  // Group scorecard by section, then by metric group
  const sectionMap = {};
  scorecard.forEach((row) => {
    const section = row.vendorSection || "Other";
    if (!sectionMap[section]) sectionMap[section] = {};
    const group = row.metricGroup || "General";
    if (!sectionMap[section][group]) sectionMap[section][group] = [];
    sectionMap[section][group].push(row);
  });

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {/* KPI Cards */}
      {displayKpis.length > 0 && (
        <div
          style={{
            display: "grid",
            gridTemplateColumns: `repeat(${Math.min(6, displayKpis.length)}, 1fr)`,
            gap: "12px",
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

      {/* ═══ Summary Charts — Key Metrics Across Periods ═══ */}
      {(() => {
        // Build chart data from "All Vendors" section across periods
        const allVendorRows = scorecard.filter(
          (r) => r.vendorSection === "All Vendors"
        );
        // Key chart metrics to visualize
        const chartMetrics = [
          { name: "POS Sales $", format: "dollar" },
          { name: "Cost On Order", format: "dollar" },
          { name: "Ships At Cost WHSE", format: "dollar" },
          { name: "GMROII", format: "pct" },
          { name: "Cost On Hand", format: "dollar" },
          { name: "Warehouse Turns", format: "pct" },
        ];
        // Filter to metrics that exist in data
        const availableMetrics = chartMetrics.filter((cm) =>
          allVendorRows.some(
            (r) => r.metricName === cm.name
          )
        );
        if (availableMetrics.length === 0) return null;

        // Build period-based chart for key dollar metrics
        const dollarMetrics = availableMetrics.filter(
          (m) => m.format === "dollar"
        );
        const pctMetrics = availableMetrics.filter(
          (m) => m.format === "pct"
        );

        const buildBarData = (metrics) => {
          const labels = periodsInData;
          const datasets = metrics.map((cm, idx) => {
            const colors = [COLORS.teal, COLORS.orange, "#60a5fa", "#a78bfa", COLORS.red, "#f59e0b"];
            return {
              label: cm.name,
              data: periodsInData.map((period) => {
                const row = allVendorRows.find(
                  (r) => r.metricName === cm.name && r.period === period
                );
                return row?.valueTy || 0;
              }),
              backgroundColor: colors[idx % colors.length],
              borderColor: colors[idx % colors.length],
              borderWidth: 1,
            };
          });
          return { labels, datasets };
        };

        return (
          <div
            style={{
              display: "grid",
              gridTemplateColumns: pctMetrics.length > 0 ? "1fr 1fr" : "1fr",
              gap: 16,
            }}
          >
            {dollarMetrics.length > 0 && (
              <Card>
                <CardHdr title="Key Dollar Metrics by Period (TY)" />
                <ChartCanvas
                  type="bar"
                  labels={periodsInData}
                  configKey={`scorecard-dollar-${periodsInData.length}`}
                  height={220}
                  datasets={buildBarData(dollarMetrics).datasets}
                />
              </Card>
            )}
            {pctMetrics.length > 0 && (
              <Card>
                <CardHdr title="Key % Metrics by Period (TY)" />
                <ChartCanvas
                  type="bar"
                  labels={periodsInData}
                  configKey={`scorecard-pct-${periodsInData.length}`}
                  height={220}
                  datasets={pctMetrics.map((cm, idx) => {
                    const colors = ["#a78bfa", COLORS.teal, COLORS.orange];
                    return {
                      label: cm.name,
                      data: periodsInData.map((period) => {
                        const row = allVendorRows.find(
                          (r) =>
                            r.metricName === cm.name && r.period === period
                        );
                        const v = row?.valueTy || 0;
                        // If stored as decimal, convert to readable %
                        return Math.abs(v) < 2 ? v * 100 : v;
                      }),
                      backgroundColor: colors[idx % colors.length],
                      borderColor: colors[idx % colors.length],
                      borderWidth: 1,
                    };
                  })}
                />
              </Card>
            )}
          </div>
        );
      })()}

      {/* Full Scorecard Matrix by Vendor Section */}
      {Object.entries(sectionMap).map(([section, groupMap]) => (
        <Card key={section}>
          <CardHdr title={section} />
          <div style={{ overflowX: "auto" }}>
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                ...SG(11),
              }}
            >
              <thead>
                <tr
                  style={{
                    backgroundColor: "var(--card)",
                    borderBottom: "2px solid var(--border)",
                  }}
                >
                  <th
                    style={{
                      padding: "10px",
                      textAlign: "left",
                      ...SG(10, 600),
                      color: "var(--txt3)",
                      whiteSpace: "nowrap",
                    }}
                  >
                    Metric
                  </th>
                  {periodsInData.map((period) => (
                    <Fragment key={period}>
                      <th
                        style={{
                          padding: "10px",
                          textAlign: "right",
                          ...SG(10, 600),
                          color: "var(--txt3)",
                          whiteSpace: "nowrap",
                          borderLeft: "1px solid var(--border)",
                        }}
                      >
                        {period} TY
                      </th>
                      <th
                        style={{
                          padding: "10px",
                          textAlign: "right",
                          ...SG(10, 600),
                          color: "var(--txt3)",
                          whiteSpace: "nowrap",
                        }}
                      >
                        LY
                      </th>
                      <th
                        style={{
                          padding: "10px",
                          textAlign: "right",
                          ...SG(10, 600),
                          color: "var(--txt3)",
                          whiteSpace: "nowrap",
                        }}
                      >
                        Diff %
                      </th>
                    </Fragment>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Object.entries(groupMap).map(([group, rows]) => {
                  const metricNames = [
                    ...new Set(rows.map((r) => r.metricName)),
                  ];
                  let metricIdx = 0;

                  return (
                    <Fragment key={group}>
                      {metricNames.map((metric) => {
                        const metricRows = rows.filter(
                          (r) => r.metricName === metric
                        );
                        const isFirstInGroup = metricIdx === 0;
                        metricIdx++;
                        const bgColor =
                          metricIdx % 2 === 1
                            ? "var(--card)"
                            : "rgba(0,0,0,0.02)";

                        return (
                          <tr
                            key={metric}
                            style={{
                              backgroundColor: bgColor,
                              borderBottom: "1px solid var(--border)",
                            }}
                          >
                            <td
                              style={{
                                padding: "8px 10px",
                                textAlign: "left",
                                color: "var(--txt)",
                                fontWeight: isFirstInGroup ? 500 : 400,
                                borderTop: isFirstInGroup
                                  ? "2px solid var(--border)"
                                  : "none",
                              }}
                            >
                              {metric}
                            </td>
                            {periodsInData.map((period, pIdx) => {
                              const row = metricRows.find(
                                (r) => r.period === period
                              );
                              const ty = row?.valueTy;
                              const ly = row?.valueLy;
                              const diff = row?.valueDiff;

                              return (
                                <Fragment key={`${metric}-${period}`}>
                                  <td
                                    style={{
                                      padding: "8px 10px",
                                      textAlign: "right",
                                      color: "var(--txt)",
                                      borderLeft:
                                        pIdx === 0
                                          ? "1px solid var(--border)"
                                          : "none",
                                      borderTop: isFirstInGroup
                                        ? "2px solid var(--border)"
                                        : "none",
                                    }}
                                  >
                                    {fMetric(ty, metric)}
                                  </td>
                                  <td
                                    style={{
                                      padding: "8px 10px",
                                      textAlign: "right",
                                      color: "var(--txt)",
                                      borderTop: isFirstInGroup
                                        ? "2px solid var(--border)"
                                        : "none",
                                    }}
                                  >
                                    {fMetric(ly, metric)}
                                  </td>
                                  <td
                                    style={{
                                      padding: "8px 10px",
                                      textAlign: "right",
                                      color: fDiff(diff).color,
                                      fontWeight: 500,
                                      borderTop: isFirstInGroup
                                        ? "2px solid var(--border)"
                                        : "none",
                                    }}
                                  >
                                    {fDiff(diff).text}
                                  </td>
                                </Fragment>
                              );
                            })}
                          </tr>
                        );
                      })}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>
      ))}
    </div>
  );
}
