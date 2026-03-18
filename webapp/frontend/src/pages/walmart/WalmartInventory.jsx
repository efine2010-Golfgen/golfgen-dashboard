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

// Period options for the instock chart
const PERIOD_OPTIONS = [
  { label: "LW",   weeks: 1  },
  { label: "L4W",  weeks: 4  },
  { label: "L8W",  weeks: 8  },
  { label: "L13W", weeks: 13 },
  { label: "L26W", weeks: 26 },
  { label: "L52W", weeks: 52 },
];

export function WalmartInventory({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [weeklyData, setWeeklyData] = useState(null);
  const [instockPeriod, setInstockPeriod] = useState(52);
  const [hiddenItems, setHiddenItems] = useState(new Set());

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

  // Active window driven by period selector
  const activeWeekOrder = weekOrder.slice(-instockPeriod);
  const activeWeeks = weeks.filter((w) => activeWeekOrder.includes(w.week));

  // Always keep full 52w for availability check; 26w for detail table
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

      {/* Combined In Stock % — period selector + item toggles + overlay chart */}
      {last52Weeks.length > 0 && (
        <Card>
          <CardHdr title="In Stock % — Weekly Trend" />

          {/* Period selector */}
          <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 10, flexWrap: "wrap" }}>
            {PERIOD_OPTIONS.map((p) => (
              <button
                key={p.weeks}
                onClick={() => setInstockPeriod(p.weeks)}
                style={{
                  ...SG(9, instockPeriod === p.weeks ? 700 : 500),
                  padding: "3px 10px", borderRadius: 6,
                  border: `1px solid ${instockPeriod === p.weeks ? "transparent" : "var(--brd)"}`,
                  background: instockPeriod === p.weeks ? "var(--card2)" : "transparent",
                  color: instockPeriod === p.weeks ? "#fff" : "var(--txt3)",
                  cursor: "pointer",
                }}
              >{p.label}</button>
            ))}
            <span style={{ ...SG(9), color: "var(--txt3)", marginLeft: 6 }}>
              Bars = dept &nbsp;·&nbsp; Lines = per item
            </span>
          </div>

          {/* Item toggle pills */}
          {itemsWithWeeklyInstock.length > 0 && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 5, marginBottom: 12 }}>
              {itemsWithWeeklyInstock.map((item, idx) => {
                const hidden = hiddenItems.has(item);
                const color = ITEM_COLORS[idx % ITEM_COLORS.length];
                const label = item.length > 32 ? item.substring(0, 32) + "…" : item;
                return (
                  <button
                    key={item}
                    onClick={() =>
                      setHiddenItems((prev) => {
                        const next = new Set(prev);
                        if (next.has(item)) next.delete(item); else next.add(item);
                        return next;
                      })
                    }
                    style={{
                      ...SG(9, 500),
                      display: "flex", alignItems: "center", gap: 5,
                      padding: "3px 9px", borderRadius: 12,
                      border: `1px solid ${hidden ? "var(--brd)" : color}`,
                      background: hidden ? "transparent" : `${color}1a`,
                      color: hidden ? "var(--txt3)" : color,
                      cursor: "pointer", opacity: hidden ? 0.45 : 1,
                      transition: "all 0.15s",
                    }}
                  >
                    <span style={{
                      width: 8, height: 8, borderRadius: "50%", flexShrink: 0,
                      background: hidden ? "var(--txt3)" : color,
                      display: "inline-block",
                    }} />
                    {label}
                  </button>
                );
              })}
            </div>
          )}

          <ChartCanvas
            type="bar"
            height={320}
            configKey={`inv-combined-${instockPeriod}-${activeWeeks.length}-${[...hiddenItems].sort().join("|")}`}
            labels={activeWeeks.map((w) => fmtWeek(w.week))}
            datasets={[
              {
                label: "Dept In Stock %",
                data: activeWeeks.map((w) => toPercent(w.instockPct)),
                backgroundColor: "rgba(46,207,170,0.18)",
                borderColor: "rgba(46,207,170,0.5)",
                borderWidth: 1,
                yAxisID: "y",
                order: 2,
              },
              ...itemsWithWeeklyInstock
                .filter((item) => !hiddenItems.has(item))
                .map((item) => {
                  const origIdx = itemsWithWeeklyInstock.indexOf(item);
                  return {
                    type: "line",
                    label: item.length > 28 ? item.substring(0, 28) + "…" : item,
                    data: activeWeekOrder.map((wk) => {
                      const v = itemInstock[item]?.[wk];
                      return v != null && v > 0 ? toPercent(v) : null;
                    }),
                    borderColor: ITEM_COLORS[origIdx % ITEM_COLORS.length],
                    backgroundColor: "transparent",
                    borderWidth: 2,
                    pointRadius: instockPeriod <= 8 ? 4 : 2,
                    fill: false,
                    tension: 0.3,
                    spanGaps: true,
                    yAxisID: "y",
                    order: 1,
                  };
                }),
            ]}
            options={{
              scales: {
                y: { min: 0, max: 100, ticks: { callback: (v) => v + "%" } },
                x: { ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: instockPeriod <= 13 ? instockPeriod : 26 } },
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
