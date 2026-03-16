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

  // Top 5 KPIs by first metric TY value
  const topKpis = scorecard.slice(0, 5);

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
            value={fN(kpi.valueTy)}
            delta={
              kpi.valueDiff
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
                                  {r ? fN(r.valueTy) : "—"}
                                </td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: "var(--txt2)",
                                  }}
                                >
                                  {r ? fN(r.valueLy) : "—"}
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
