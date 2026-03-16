import { Fragment } from "react";
import {
  SG,
  DM,
  Card,
  fN,
  COLORS,
  KPICard,
} from "./WalmartHelpers";

export function ScorecardPage({ data }) {
  if (!data?.scorecard)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>
          No scorecard data available.
        </p>
      </Card>
    );

  const scorecard = data.scorecard;

  // Group by vendor → metric group
  const vendors = {};
  scorecard.forEach((r) => {
    if (!vendors[r.vendorSection]) vendors[r.vendorSection] = {};
    if (!vendors[r.vendorSection][r.metricGroup])
      vendors[r.vendorSection][r.metricGroup] = [];
    vendors[r.vendorSection][r.metricGroup].push(r);
  });

  // Extract unique periods
  const periods = [...new Set(scorecard.map((r) => r.period))];

  // Helper: detect if metric is a percentage type
  const isPctMetric = (name) =>
    /(%|margin|turns|gmroii|instock|aur)/i.test(name);

  // Smart value formatter for scorecard
  const fVal = (v, metricName) => {
    if (v == null) return "—";
    if (isPctMetric(metricName)) {
      // Percentage metrics: value is already a decimal (0.291 = 29.1%)
      if (Math.abs(v) < 10) return (v * 100).toFixed(1) + "%";
      // If value looks like it's already a whole percent or dollar, show as number
      return fN(v);
    }
    return fN(v);
  };

  // Top 5 KPIs: pick distinct metrics from "All Vendors" Last Week period
  const priorityMetrics = [
    "POS Sales in dollars",
    "POS Sales in Units",
    "Comp Sales in dollars",
    "Repl Instock %",
    "Gross Initial Margin %",
  ];
  const topKpis = [];
  for (const pm of priorityMetrics) {
    const match = scorecard.find(
      (r) =>
        r.vendorSection === "All Vendors" &&
        r.period === "Last Week" &&
        r.metricName === pm
    );
    if (match) topKpis.push(match);
  }
  // Fall back to first 5 distinct metrics if no matches
  if (topKpis.length === 0) {
    const seen = new Set();
    for (const r of scorecard) {
      if (!seen.has(r.metricName) && r.period === "Last Week") {
        seen.add(r.metricName);
        topKpis.push(r);
        if (topKpis.length >= 5) break;
      }
    }
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Top KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: 12,
        }}
      >
        {topKpis.map((kpi, i) => (
          <KPICard
            key={i}
            label={kpi.metricName}
            value={fVal(kpi.valueTy, kpi.metricName)}
            delta={
              kpi.valueDiff != null
                ? (kpi.valueDiff * 100).toFixed(1)
                : null
            }
          />
        ))}
      </div>

      {/* Full Scorecard Matrix */}
      {Object.entries(vendors).map(([vendor, groups]) => (
        <Card key={vendor}>
          <div
            style={{
              ...SG(13, 700),
              color: "var(--txt)",
              marginBottom: 12,
            }}
          >
            {vendor}
          </div>
          {Object.entries(groups).map(([group, rows]) => (
            <div key={group} style={{ marginBottom: 16 }}>
              <div
                style={{
                  ...SG(11, 700),
                  color: COLORS.teal,
                  textTransform: "uppercase",
                  letterSpacing: ".08em",
                  marginBottom: 8,
                }}
              >
                {group}
              </div>
              <div style={{ overflowX: "auto" }}>
                <table
                  style={{
                    width: "100%",
                    borderCollapse: "collapse",
                    ...SG(9),
                  }}
                >
                  <thead>
                    <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                      <th
                        style={{
                          textAlign: "left",
                          padding: "4px 6px",
                          color: "var(--txt3)",
                        }}
                      >
                        Metric
                      </th>
                      {periods.map((p) => (
                        <th
                          key={p}
                          colSpan={3}
                          style={{
                            textAlign: "center",
                            padding: "4px 6px",
                            color: "var(--txt3)",
                          }}
                        >
                          {p}
                        </th>
                      ))}
                    </tr>
                    <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                      <th />
                      {periods.map((p) => (
                        <Fragment key={p}>
                          <th
                            style={{
                              textAlign: "right",
                              padding: "2px 4px",
                              color: "var(--txt3)",
                              fontSize: 8,
                            }}
                          >
                            TY
                          </th>
                          <th
                            style={{
                              textAlign: "right",
                              padding: "2px 4px",
                              color: "var(--txt3)",
                              fontSize: 8,
                            }}
                          >
                            LY
                          </th>
                          <th
                            style={{
                              textAlign: "right",
                              padding: "2px 4px",
                              color: "var(--txt3)",
                              fontSize: 8,
                            }}
                          >
                            Diff
                          </th>
                        </Fragment>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      ...new Set(
                        rows.map((r) => r.metricName)
                      ),
                    ].map((metric) => {
                      const mRows = rows.filter(
                        (r) => r.metricName === metric
                      );
                      return (
                        <tr
                          key={metric}
                          style={{
                            borderBottom: "1px solid var(--brd)",
                          }}
                        >
                          <td
                            style={{
                              padding: "4px 6px",
                              color: "var(--txt)",
                            }}
                          >
                            {metric}
                          </td>
                          {periods.map((p) => {
                            const r = mRows.find((x) => x.period === p);
                            return (
                              <Fragment key={p}>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: "var(--txt)",
                                  }}
                                >
                                  {r ? fVal(r.valueTy, metric) : "—"}
                                </td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: "var(--txt2)",
                                  }}
                                >
                                  {r ? fVal(r.valueLy, metric) : "—"}
                                </td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color:
                                      r && r.valueDiff >= 0
                                        ? COLORS.teal
                                        : COLORS.red,
                                  }}
                                >
                                  {r && r.valueDiff != null
                                    ? (r.valueDiff * 100).toFixed(1) +
                                      "%"
                                    : "—"}
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
            </div>
          ))}
        </Card>
      ))}
    </div>
  );
}
