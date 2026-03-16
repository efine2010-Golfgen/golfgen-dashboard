import { useState, useEffect } from "react";
import { api } from "../../lib/api";
import {
  SG,
  Card,
  CardHdr,
  KPICard,
  ChartCanvas,
  fN,
  fPct,
  COLORS,
} from "./WalmartHelpers";

// Smart instock formatter: handles both 0-1 decimal and 0-100 range
const fInstock = (v) => {
  if (v == null || v === 0) return "—";
  const num = Number(v);
  if (num > 1) return num.toFixed(1) + "%";
  return (num * 100).toFixed(1) + "%";
};

// Convert to 0-100 scale for charts
const toPercent = (v) => {
  if (v == null || v === 0) return 0;
  const num = Number(v);
  return num > 1 ? num : num * 100;
};

// Color for instock value (after converting to 0-100 scale)
const instockColor = (v) => {
  const pct = toPercent(v);
  if (pct >= 90) return COLORS.teal;
  if (pct >= 70) return COLORS.yellow;
  return COLORS.red;
};

// Line color palette for items
const ITEM_COLORS = [
  "#2ecf99", "#f97316", "#3b82f6", "#a78bfa",
  "#ef4444", "#f59e0b", "#ec4899", "#14b8a6",
];

export function WalmartInventory({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [weeklyData, setWeeklyData] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const [invResult, weeklyResult] = await Promise.all([
          api.walmartInventory(filters),
          api.walmartWeeklyTrend(filters),
        ]);
        setData(invResult);
        setWeeklyData(weeklyResult);
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

  const kpis = data.kpis || {};
  const instockTrend = data.instockTrend || {};
  const ohDistribution = data.ohDistribution || {};

  // Build instock trend chart data — only include items that have data across
  // at least 2 periods (filters out items that didn't exist historically)
  const itemNames = Object.keys(instockTrend);
  const periodKeys = ["lw", "l4w", "l13w", "l26w", "l52w"];
  const periodLabels = ["1W", "4W", "13W", "26W", "52W"];

  const validItems = itemNames.filter((item) => {
    const vals = periodKeys.map((p) => instockTrend[item]?.[p]);
    const nonZero = vals.filter((v) => v != null && v !== 0);
    return nonZero.length >= 2;
  });

  const instockChartLines = validItems.slice(0, 8).map((item, idx) => ({
    label: item.length > 25 ? item.substring(0, 25) + "…" : item,
    data: periodKeys.map((p) => {
      const v = instockTrend[item]?.[p];
      return v != null && v !== 0 ? toPercent(v) : null;
    }),
    borderColor: ITEM_COLORS[idx % ITEM_COLORS.length],
    fill: false,
    tension: 0.3,
    spanGaps: true,
  }));

  // Build OH distribution chart — include ALL sales types
  const ohLabels = Object.keys(ohDistribution).filter(
    (k) => ohDistribution[k] > 0
  );
  const ohValues = ohLabels.map((k) => ohDistribution[k]);
  const ohColors = [
    COLORS.teal,
    COLORS.red,
    COLORS.orange,
    COLORS.blue,
    COLORS.purple,
    COLORS.yellow,
  ];
  const ohChartData = {
    labels: ohLabels.length > 0 ? ohLabels : ["No Data"],
    datasets: [
      {
        label: "OH Units",
        data: ohValues.length > 0 ? ohValues : [0],
        backgroundColor: ohLabels.map((_, i) => ohColors[i % ohColors.length]),
      },
    ],
  };

  // Weekly trend data for instock bar + item lines chart
  const weeks = weeklyData?.weeks || [];
  const itemInstock = weeklyData?.itemInstock || {};
  const weekOrder = weeklyData?.weekOrder || weeks.map((w) => w.week);

  // Identify items that have per-week instock data
  const itemsWithWeeklyInstock = Object.keys(itemInstock).filter((item) => {
    const weekData = itemInstock[item] || {};
    const nonZero = Object.values(weekData).filter((v) => v != null && v > 0);
    return nonZero.length >= 2;
  });

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {/* KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(3, 1fr)",
          gap: "12px",
        }}
      >
        <KPICard label="Total OH Units" value={fN(kpis.totalOhUnits)} />
        <KPICard label="Avg Instock %" value={fInstock(kpis.avgInstockPct)} />
        <KPICard
          label="Weeks of Supply"
          value={
            kpis.weeksOfSupply != null
              ? Number(kpis.weeksOfSupply).toFixed(1)
              : "—"
          }
        />
      </div>

      {/* Weekly In Stock % — Bars = dept instock %, Lines = per-item instock % */}
      {weeks.length > 0 && (
        <Card>
          <CardHdr title="Weekly In Stock % & Item Trends" />
          <div style={{ ...SG(9), color: "var(--txt3)", marginBottom: 8 }}>
            Bars show total department in stock % (left axis). Colored lines
            show per-item in stock % by week.
          </div>
          <ChartCanvas
            type="bar"
            height={280}
            configKey={`inv-weekly-${weeks.length}-${itemsWithWeeklyInstock.length}`}
            labels={weeks.map((w) => {
              const s = String(w.week);
              return s.length >= 6 ? "Wk" + s.slice(4) : s;
            })}
            datasets={[
              {
                label: "Dept In Stock %",
                data: weeks.map((w) => toPercent(w.instockPct)),
                backgroundColor: "rgba(46,207,170,0.3)",
                borderColor: COLORS.teal,
                borderWidth: 1,
                order: 10,
                yAxisID: "y",
              },
              // Per-item instock lines
              ...itemsWithWeeklyInstock.slice(0, 6).map((item, idx) => ({
                label: item.length > 20 ? item.substring(0, 20) + "…" : item,
                type: "line",
                data: weekOrder.map((wk) => {
                  const v = itemInstock[item]?.[wk];
                  return v != null && v > 0 ? toPercent(v) : null;
                }),
                borderColor: ITEM_COLORS[idx % ITEM_COLORS.length],
                borderWidth: 2,
                pointRadius: 1,
                fill: false,
                tension: 0.3,
                spanGaps: true,
                order: idx + 1,
                yAxisID: "y",
              })),
            ]}
            options={{
              scales: {
                y: {
                  min: 0,
                  max: 100,
                  ticks: {
                    callback: (v) => v + "%",
                  },
                },
              },
            }}
          />
        </Card>
      )}

      {/* Instock % Trend by Item (period-based line chart) */}
      <Card>
        <CardHdr title="Instock % Trend by Item" />
        {instockChartLines.length > 0 ? (
          <div style={{ position: "relative", height: 300 }}>
            <ChartCanvas
              type="line"
              data={{
                labels: periodLabels,
                datasets: instockChartLines,
              }}
              height={300}
            />
          </div>
        ) : (
          <p style={{ ...SG(11), color: "var(--txt3)" }}>No trend data</p>
        )}
      </Card>

      {/* OH Distribution by Sales Type — full width, no doughnut */}
      <Card>
        <CardHdr title="OH Distribution by Type" />
        <div style={{ position: "relative", height: 250 }}>
          <ChartCanvas type="bar" data={ohChartData} height={250} />
        </div>
      </Card>

      {/* Instock Detail Table — only show items with valid data */}
      {validItems.length > 0 && (
        <Card>
          <CardHdr title="Instock % by Item & Period" />
          <div
            style={{
              overflowX: "auto",
              fontSize: "11px",
              lineHeight: "1.6",
            }}
          >
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--border)" }}>
                  <th
                    style={{
                      textAlign: "left",
                      padding: "8px",
                      ...SG(10, 600),
                    }}
                  >
                    Item
                  </th>
                  {periodLabels.map((lbl) => (
                    <th
                      key={lbl}
                      style={{
                        textAlign: "right",
                        padding: "8px",
                        ...SG(10, 600),
                      }}
                    >
                      {lbl}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {validItems.map((item, idx) => (
                  <tr
                    key={idx}
                    style={{
                      borderBottom: "1px solid var(--border)",
                      backgroundColor:
                        idx % 2 === 0 ? "var(--bg2)" : "transparent",
                    }}
                  >
                    <td style={{ padding: "8px", ...SG(11) }}>{item}</td>
                    {periodKeys.map((pk) => {
                      const val = instockTrend[item]?.[pk];
                      const hasData = val != null && val !== 0;
                      return (
                        <td
                          key={pk}
                          style={{
                            padding: "8px",
                            textAlign: "right",
                            ...SG(11),
                            color: hasData ? instockColor(val) : "var(--txt3)",
                          }}
                        >
                          {hasData ? fInstock(val) : "—"}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      )}
    </div>
  );
}
