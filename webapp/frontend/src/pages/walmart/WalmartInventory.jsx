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

// Store detail view modes
const STORE_VIEWS = [
  { key: "stores", label: "Store Counts" },
  { key: "units",  label: "Units"        },
];

// Metric definitions per view
const STORE_METRICS = [
  { key: "traitedStores",  label: "# Traited Stores"      },
  { key: "storesWithInv",  label: "Stores w/ Inv >0"      },
  { key: "traitedZeroInv", label: "Traited w/ 0 Inv"      },
  { key: "storesOneUnit",  label: "Stores w/ 1 Unit"      },
];
const UNIT_METRICS = [
  { key: "ohUnits",      label: "OH Units"       },
  { key: "onOrderUnits", label: "On Order Units" },
];

// Generic item-toggle pill component
function TogglePills({ items, hidden, onToggle, maxLabel = 32 }) {
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 5, marginBottom: 12 }}>
      {items.map((item, idx) => {
        const isHidden = hidden.has(item);
        const color = ITEM_COLORS[idx % ITEM_COLORS.length];
        const label = item.length > maxLabel ? item.substring(0, maxLabel) + "…" : item;
        return (
          <button
            key={item}
            onClick={() => onToggle(item)}
            style={{
              ...SG(9, 500),
              display: "flex", alignItems: "center", gap: 5,
              padding: "3px 9px", borderRadius: 12,
              border: `1px solid ${isHidden ? "var(--brd)" : color}`,
              background: isHidden ? "transparent" : `${color}1a`,
              color: isHidden ? "var(--txt3)" : color,
              cursor: "pointer", opacity: isHidden ? 0.45 : 1,
              transition: "all 0.15s",
            }}
          >
            <span style={{
              width: 8, height: 8, borderRadius: "50%", flexShrink: 0,
              background: isHidden ? "var(--txt3)" : color,
              display: "inline-block",
            }} />
            {label}
          </button>
        );
      })}
    </div>
  );
}

export function WalmartInventory({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [weeklyData, setWeeklyData] = useState(null);
  const [storeDetailData, setStoreDetailData] = useState(null);

  // Instock chart state
  const [instockPeriod, setInstockPeriod] = useState(52);
  const [hiddenItems, setHiddenItems] = useState(new Set());

  // Instock table state
  const [tablePeriod, setTablePeriod] = useState(26);
  const [hiddenTableItems, setHiddenTableItems] = useState(new Set());

  // Store detail chart state
  const [storeDetailView, setStoreDetailView] = useState("stores");
  const [hiddenStoreItems, setHiddenStoreItems] = useState(new Set());

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const [invResult, weeklyResult, storeResult] = await Promise.all([
          api.walmartInventory(filters),
          api.walmartWeeklyTrend(filters),
          api.walmartItemStoreDetail(filters),
        ]);
        setData(invResult);
        setWeeklyData(weeklyResult);
        setStoreDetailData(storeResult);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [filters.division, filters.customer]);

  // Helper: toggle an item in a Set state
  const toggleSetItem = (setter) => (item) =>
    setter((prev) => {
      const next = new Set(prev);
      if (next.has(item)) next.delete(item); else next.add(item);
      return next;
    });

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

  // ── Weekly trend data for instock chart ─────────────────────────────────
  const weeks = weeklyData?.weeks || [];
  const itemInstock = weeklyData?.itemInstock || {};
  const weekOrder = weeklyData?.weekOrder || weeks.map((w) => w.week);

  const activeWeekOrder = weekOrder.slice(-instockPeriod);
  const activeWeeks = weeks.filter((w) => activeWeekOrder.includes(w.week));
  const last52WeekOrder = weekOrder.slice(-52);
  const last52Weeks = weeks.filter((w) => last52WeekOrder.includes(w.week));
  const last26WeekOrder = weekOrder.slice(-26);
  const last26Weeks = weeks.filter((w) => last26WeekOrder.includes(w.week));

  const itemsWithWeeklyInstock = Object.keys(itemInstock).filter((item) => {
    const weekData = itemInstock[item] || {};
    const nonZero = Object.values(weekData).filter((v) => v != null && v > 0);
    return nonZero.length >= 2;
  });

  const fmtWeek = (wk) => {
    const s = String(wk);
    return s.length >= 6 ? "Wk" + s.slice(4) : s;
  };

  // ── Store detail chart data ──────────────────────────────────────────────
  const storeItems = storeDetailData?.items || [];
  const storeItemNames = storeItems.map((it) => it.itemName);

  const activeMetrics = storeDetailView === "stores" ? STORE_METRICS : UNIT_METRICS;
  const metricLabels = activeMetrics.map((m) => m.label);

  // One dataset per item (Chart.js grouped bar: X=metrics, each item is a series)
  const storeChartDatasets = storeItems
    .filter((it) => !hiddenStoreItems.has(it.itemName))
    .map((it, arrIdx) => {
      const origIdx = storeItemNames.indexOf(it.itemName);
      const color = ITEM_COLORS[origIdx % ITEM_COLORS.length];
      return {
        label: it.itemName.length > 28 ? it.itemName.substring(0, 28) + "…" : it.itemName,
        data: activeMetrics.map((m) => it[m.key] || 0),
        backgroundColor: color + "bb",
        borderColor: color,
        borderWidth: 1,
      };
    });

  const storeConfigKey = `store-detail-${storeDetailView}-${[...hiddenStoreItems].sort().join("|")}`;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>

      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: "12px" }}>
        <KPICard label="Total OH Units" value={fN(kpis.totalOhUnits)} />
        <KPICard label="Avg Instock %" value={fInstock(kpis.avgInstockPct)} />
        <KPICard
          label="Weeks of Supply"
          value={kpis.weeksOfSupply != null ? Number(kpis.weeksOfSupply).toFixed(1) : "—"}
        />
      </div>

      {/* ── Combined In Stock % — period selector + item toggles + overlay chart ── */}
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
            <TogglePills
              items={itemsWithWeeklyInstock}
              hidden={hiddenItems}
              onToggle={toggleSetItem(setHiddenItems)}
            />
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

      {/* ── In Stock % Table by Item — period selector + item toggles ── */}
      {last26Weeks.length > 0 && itemsWithWeeklyInstock.length > 0 && (
        <Card>
          <CardHdr title="In Stock % by Item" />

          {/* Period selector */}
          <div style={{ display: "flex", alignItems: "center", gap: 4, marginBottom: 10, flexWrap: "wrap" }}>
            {[{ label: "L8W", weeks: 8 }, { label: "L13W", weeks: 13 }, { label: "L26W", weeks: 26 }].map((p) => (
              <button
                key={p.weeks}
                onClick={() => setTablePeriod(p.weeks)}
                style={{
                  ...SG(9, tablePeriod === p.weeks ? 700 : 500),
                  padding: "3px 10px", borderRadius: 6,
                  border: `1px solid ${tablePeriod === p.weeks ? "transparent" : "var(--brd)"}`,
                  background: tablePeriod === p.weeks ? "var(--card2)" : "transparent",
                  color: tablePeriod === p.weeks ? "#fff" : "var(--txt3)",
                  cursor: "pointer",
                }}
              >{p.label}</button>
            ))}
          </div>

          {/* Item show/hide toggles */}
          <TogglePills
            items={itemsWithWeeklyInstock}
            hidden={hiddenTableItems}
            onToggle={toggleSetItem(setHiddenTableItems)}
          />

          {/* Table */}
          {(() => {
            const tblWeekOrder = weekOrder.slice(-tablePeriod);
            const visibleItems = itemsWithWeeklyInstock.filter((item) => !hiddenTableItems.has(item));
            return (
              <div style={{ overflowX: "auto", fontSize: "11px", lineHeight: "1.6" }}>
                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                      <th style={{
                        textAlign: "left", padding: "8px", ...SG(10, 600),
                        position: "sticky", left: 0, background: "var(--card)", zIndex: 2, minWidth: 160,
                      }}>Item</th>
                      {tblWeekOrder.map((wk) => (
                        <th key={wk} style={{ textAlign: "right", padding: "6px 4px", ...SG(8, 600), whiteSpace: "nowrap" }}>
                          {fmtWeek(wk)}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {visibleItems.map((item, idx) => (
                      <tr key={item} style={{ borderBottom: "1px solid var(--brd)", backgroundColor: idx % 2 === 0 ? "var(--bg2)" : "transparent" }}>
                        <td style={{
                          padding: "6px 8px", ...SG(10),
                          position: "sticky", left: 0, background: "var(--card)", zIndex: 1, whiteSpace: "nowrap",
                          color: ITEM_COLORS[itemsWithWeeklyInstock.indexOf(item) % ITEM_COLORS.length],
                          fontWeight: 600,
                        }}>
                          {item.length > 30 ? item.substring(0, 30) + "…" : item}
                        </td>
                        {tblWeekOrder.map((wk) => {
                          const val = itemInstock[item]?.[wk];
                          const hasData = val != null && val > 0;
                          return (
                            <td key={wk} style={{
                              padding: "6px 4px", textAlign: "right", ...SG(10),
                              color: hasData ? instockColor(val) : "var(--txt3)",
                              whiteSpace: "nowrap",
                            }}>
                              {hasData ? fInstock(val) : "—"}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                    {visibleItems.length === 0 && (
                      <tr>
                        <td colSpan={tblWeekOrder.length + 1} style={{ padding: 20, textAlign: "center", color: "var(--txt3)", ...SG(10) }}>
                          All items hidden — toggle items above to show
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            );
          })()}
        </Card>
      )}

      {/* ── 2026 Item Store Inventory Detail — grouped bar chart ─────────────── */}
      {storeItems.length > 0 && (
        <Card>
          <CardHdr title="2026 Item Store Inventory Detail" />

          {/* View toggle + legend note */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10, flexWrap: "wrap" }}>
            {STORE_VIEWS.map((v) => (
              <button
                key={v.key}
                onClick={() => setStoreDetailView(v.key)}
                style={{
                  ...SG(9, storeDetailView === v.key ? 700 : 500),
                  padding: "3px 10px", borderRadius: 6,
                  border: `1px solid ${storeDetailView === v.key ? "transparent" : "var(--brd)"}`,
                  background: storeDetailView === v.key ? "var(--card2)" : "transparent",
                  color: storeDetailView === v.key ? "#fff" : "var(--txt3)",
                  cursor: "pointer",
                }}
              >{v.label}</button>
            ))}
            <span style={{ ...SG(9), color: "var(--txt3)", marginLeft: 4 }}>
              {storeDetailView === "stores"
                ? "Stores w/ Inv estimated from Instock % × Traited Stores"
                : "OH = on-hand units across all traited stores"}
            </span>
          </div>

          {/* Item toggle pills */}
          <TogglePills
            items={storeItemNames}
            hidden={hiddenStoreItems}
            onToggle={toggleSetItem(setHiddenStoreItems)}
          />

          {storeChartDatasets.length > 0 ? (
            <ChartCanvas
              type="bar"
              height={300}
              configKey={storeConfigKey}
              labels={metricLabels}
              datasets={storeChartDatasets}
              options={{
                plugins: {
                  legend: { display: true, position: "bottom" },
                  tooltip: {
                    callbacks: {
                      label: (ctx) => {
                        const val = ctx.parsed.y;
                        return `${ctx.dataset.label}: ${Number(val).toLocaleString()}`;
                      },
                    },
                  },
                },
                scales: {
                  x: { ticks: { ...SG(9) } },
                  y: {
                    beginAtZero: true,
                    ticks: {
                      callback: (v) => Number(v).toLocaleString(),
                    },
                  },
                },
              }}
            />
          ) : (
            <p style={{ ...SG(11), color: "var(--txt3)", padding: "16px 0" }}>
              All items hidden — toggle items above to show
            </p>
          )}

          {/* Summary table */}
          <div style={{ overflowX: "auto", marginTop: 16 }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "11px" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                  <th style={{ textAlign: "left", padding: "6px 8px", ...SG(10, 600), minWidth: 160 }}>Item</th>
                  <th style={{ textAlign: "right", padding: "6px 8px", ...SG(10, 600) }}>OH Units</th>
                  <th style={{ textAlign: "right", padding: "6px 8px", ...SG(10, 600) }}>On Order</th>
                  <th style={{ textAlign: "right", padding: "6px 8px", ...SG(10, 600) }}># Traited</th>
                  <th style={{ textAlign: "right", padding: "6px 8px", ...SG(10, 600) }}>w/ Inv &gt;0</th>
                  <th style={{ textAlign: "right", padding: "6px 8px", ...SG(10, 600) }}>Traited 0 Inv</th>
                  <th style={{ textAlign: "right", padding: "6px 8px", ...SG(10, 600) }}>w/ 1 Unit</th>
                </tr>
              </thead>
              <tbody>
                {storeItems.map((it, idx) => {
                  const color = ITEM_COLORS[idx % ITEM_COLORS.length];
                  const isHidden = hiddenStoreItems.has(it.itemName);
                  return (
                    <tr
                      key={it.itemName}
                      style={{
                        borderBottom: "1px solid var(--brd)",
                        backgroundColor: idx % 2 === 0 ? "var(--bg2)" : "transparent",
                        opacity: isHidden ? 0.35 : 1,
                      }}
                    >
                      <td style={{ padding: "5px 8px", ...SG(10), color, fontWeight: 600, whiteSpace: "nowrap" }}>
                        {it.itemName.length > 34 ? it.itemName.substring(0, 34) + "…" : it.itemName}
                      </td>
                      <td style={{ padding: "5px 8px", textAlign: "right", ...SG(10), color: "var(--txt1)" }}>
                        {fN(it.ohUnits)}
                      </td>
                      <td style={{ padding: "5px 8px", textAlign: "right", ...SG(10), color: "var(--txt1)" }}>
                        {fN(it.onOrderUnits)}
                      </td>
                      <td style={{ padding: "5px 8px", textAlign: "right", ...SG(10), color: "var(--txt1)" }}>
                        {fN(it.traitedStores)}
                      </td>
                      <td style={{ padding: "5px 8px", textAlign: "right", ...SG(10), color: COLORS.teal }}>
                        {fN(it.storesWithInv)}
                      </td>
                      <td style={{ padding: "5px 8px", textAlign: "right", ...SG(10), color: it.traitedZeroInv > 0 ? COLORS.red : "var(--txt3)" }}>
                        {fN(it.traitedZeroInv)}
                      </td>
                      <td style={{ padding: "5px 8px", textAlign: "right", ...SG(10), color: "var(--txt3)" }}>
                        {it.storesOneUnit > 0 ? fN(it.storesOneUnit) : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </Card>
      )}

    </div>
  );
}
