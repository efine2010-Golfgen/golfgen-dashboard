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

  // Weekly trend data for instock bar + item lines chart
  const weeks = weeklyData?.weeks || [];
  const itemInstock = weeklyData?.itemInstock || {};
  const weekOrder = weeklyData?.weekOrder || weeks.map((w) => w.week);

  // Get last 52 weeks for dept chart, last 26 weeks for item chart/table
  const last52WeekOrder = weekOrder.slice(-52);
  const last52Weeks = weeks.filter((w) => last52WeekOrder.includes(w.week));
  const last26WeekOrder = weekOrder.slice(-26);
  const last26Weeks = weeks.filter((w) => last26WeekOrder.includes(w.week));

  // Identify items that have per-week instock data
  const itemsWithWeeklyInstock = Object.keys(itemInstock).filter((item) => {
    const weekData = itemInstock[item] || {};
    const nonZero = Object.values(weekData).filter((v) => v != null && v > 0);
    return nonZero.length >= 2;
  });

  // Format week label: "202501" → "Wk01", etc.
  const fmtWeek = (wk) => {
    const s = String(wk);
    return s.length >= 6 ? "Wk" + s.slice(4) : s;
  };

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

      {/* Weekly In Stock % Bar Chart — last 52 weeks */}
      {last52Weeks.length > 0 && (
        <Card>
          <CardHdr title="Weekly In Stock % — Last 52 Weeks" />
          <div style={{ ...SG(9), color: "var(--txt3)", marginBottom: 8 }}>
            Bars show total department in stock % by week.
          </div>
          <ChartCanvas
            type="bar"
            height={280}
            configKey={`inv-weekly-52wk-${last52Weeks.length}`}
            labels={last52Weeks.map((w) => fmtWeek(w.week))}
            datasets={[
              {
                label: "Dept In Stock %",
                data: last52Weeks.map((w) => toPercent(w.instockPct)),
                backgroundColor: "rgba(46,207,170,0.3)",
                borderColor: COLORS.teal,
                borderWidth: 1,
                yAxisID: "y",
              },
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
                x: {
                  ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 52 },
                },
              },
            }}
          />
        </Card>
      )}

      {/* Item-Level In Stock % — last 26 weeks */}
      {last26Weeks.length > 0 && itemsWithWeeklyInstock.length > 0 && (
        <Card>
          <CardHdr title="Item In Stock % — Last 26 Weeks" />
          <div style={{ ...SG(9), color: "var(--txt3)", marginBottom: 8 }}>
            Colored lines show per-item in stock % by week.
          </div>
          <ChartCanvas
            type="line"
            height={280}
            configKey={`inv-item-26wk-${last26Weeks.length}-${itemsWithWeeklyInstock.length}`}
            labels={last26Weeks.map((w) => fmtWeek(w.week))}
            datasets={
              itemsWithWeeklyInstock.slice(0, 8).map((item, idx) => ({
                label: item.length > 25 ? item.substring(0, 25) + "…" : item,
                data: last26WeekOrder.map((wk) => {
                  const v = itemInstock[item]?.[wk];
                  return v != null && v > 0 ? toPercent(v) : null;
                }),
                borderColor: ITEM_COLORS[idx % ITEM_COLORS.length],
                borderWidth: 2,
                pointRadius: 2,
                fill: false,
                tension: 0.3,
                spanGaps: true,
              }))
            }
            options={{
              scales: {
                y: {
                  min: 0,
                  max: 100,
                  ticks: {
                    callback: (v) => v + "%",
                  },
                },
                x: {
                  ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 13 },
                },
              },
            }}
          />
        </Card>
      )}

      {/* 26-Week Instock % Table by Item */}
      {last26Weeks.length > 0 && itemsWithWeeklyInstock.length > 0 && (
        <Card>
          <CardHdr title="In Stock % by Item — Last 26 Weeks" />
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
                      position: "sticky",
                      left: 0,
                      background: "var(--card)",
                      zIndex: 2,
                      minWidth: 160,
                    }}
                  >
                    Item
                  </th>
                  {last26WeekOrder.map((wk) => (
                    <th
                      key={wk}
                      style={{
                        textAlign: "right",
                        padding: "6px 4px",
                        ...SG(8, 600),
                        whiteSpace: "nowrap",
                      }}
                    >
                      {fmtWeek(wk)}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {itemsWithWeeklyInstock.map((item, idx) => (
                  <tr
                    key={idx}
                    style={{
                      borderBottom: "1px solid var(--border)",
                      backgroundColor:
                        idx % 2 === 0 ? "var(--bg2)" : "transparent",
                    }}
                  >
                    <td
                      style={{
                        padding: "6px 8px",
                        ...SG(10),
                        position: "sticky",
                        left: 0,
                        background: "var(--card)",
                        zIndex: 1,
                        whiteSpace: "nowrap",
                      }}
                    >
                      {item.length > 30 ? item.substring(0, 30) + "…" : item}
                    </td>
                    {last26WeekOrder.map((wk) => {
                      const val = itemInstock[item]?.[wk];
                      const hasData = val != null && val > 0;
                      return (
                        <td
                          key={wk}
                          style={{
                            padding: "6px 4px",
                            textAlign: "right",
                            ...SG(10),
                            color: hasData ? instockColor(val) : "var(--txt3)",
                            whiteSpace: "nowrap",
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
