import { useState } from "react";
import {
  SG,
  DM,
  Card,
  fN,
  f$,
  fPct,
  COLORS,
  PERIODS,
  KPICard,
  ChartCanvas,
} from "./WalmartHelpers";

export function InventoryPage({ data }) {
  const [selectedPeriod, setSelectedPeriod] = useState("lw");
  const [selectedItem, setSelectedItem] = useState(null);

  if (!data?.inventoryHealth)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>
          No inventory data available.
        </p>
      </Card>
    );

  const ih = data.inventoryHealth;
  const kpis = ih.kpis || {};
  const items = ih.instockTrend || {};
  const categories = ih.instockByType?.[selectedPeriod] || {};

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
          gap: 12,
        }}
      >
        <KPICard label="Total Store OH" value={fN(kpis.totalOhUnits)} />
        <KPICard
          label="Regular Instock %"
          value={fPct(kpis.regInstockPct)}
        />
        <KPICard
          label="Clearance Instock %"
          value={fPct(kpis.clrInstockPct)}
        />
        <KPICard label="CVP Instock %" value={fPct(kpis.cvpInstockPct)} />
        <KPICard
          label="Retail $ Inventory"
          value={f$(kpis.retailInvValue)}
        />
        <KPICard
          label="Weeks of Supply"
          value={kpis.wksOfSupply?.toFixed(1)}
        />
      </div>

      {/* Instock Trend Chart (all items) */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          Instock % Trend (90 Days)
        </div>
        <ChartCanvas
          type="line"
          labels={["L52W", "L26W", "L13W", "L4W", "LW"]}
        />
      </Card>

      {/* OH per Store Chart with Period Selector */}
      <Card>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 8,
          }}
        >
          <div style={{ ...SG(12, 700), color: "var(--txt)" }}>
            OH per Store Distribution
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            {PERIODS.map((p) => (
              <button
                key={p}
                onClick={() => setSelectedPeriod(p)}
                style={{
                  ...SG(
                    8,
                    selectedPeriod === p ? 700 : 500
                  ),
                  padding: "2px 6px",
                  borderRadius: 3,
                  cursor: "pointer",
                  background:
                    selectedPeriod === p
                      ? "var(--acc1)"
                      : "var(--card)",
                  color:
                    selectedPeriod === p
                      ? "#fff"
                      : "var(--txt3)",
                  border: "none",
                }}
              >
                {p.toUpperCase()}
              </button>
            ))}
          </div>
        </div>
        <ChartCanvas type="bar" />
      </Card>

      {/* Instock by Type Chart */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          Instock by Type
        </div>
        <ChartCanvas
          type="bar"
          labels={["Regular", "Clearance", "CVP"]}
          datasets={[
            {
              data: [
                categories.regular || 0,
                categories.clearance || 0,
                categories.cvp || 0,
              ],
              backgroundColor: [COLORS.teal, COLORS.orange, COLORS.blue],
            },
          ]}
        />
      </Card>

      {/* Item Instock Detail */}
      <Card>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 8,
          }}
        >
          <div style={{ ...SG(12, 700), color: "var(--txt)" }}>
            Item Instock Analysis
          </div>
          <select
            value={selectedItem || ""}
            onChange={(e) => setSelectedItem(e.target.value || null)}
            style={{
              ...SG(10),
              padding: "4px 8px",
              borderRadius: 4,
              border: "1px solid var(--brd)",
              background: "var(--card)",
              color: "var(--txt)",
            }}
          >
            <option value="">Select Item</option>
            {Object.keys(items).map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </div>
        {selectedItem && items[selectedItem] && (
          <div
            style={{
              padding: "12px",
              background: "rgba(255,255,255,.02)",
              borderRadius: 6,
            }}
          >
            <div style={{ display: "flex", gap: 24, ...SG(11) }}>
              {items[selectedItem].map((pct, idx) => (
                <div key={idx}>
                  <div
                    style={{
                      ...SG(9, 600),
                      color: "var(--txt3)",
                      marginBottom: 2,
                    }}
                  >
                    {"52w,26w,13w,4w,1w".split(",")[idx]}
                  </div>
                  <div style={{ color: "var(--txt)" }}>
                    {fPct(pct)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </Card>
    </div>
  );
}
