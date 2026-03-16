import { useState, useEffect } from "react";
import { api } from "../../lib/api";
import {
  SG,
  DM,
  Card,
  CardHdr,
  KPICard,
  ChartCanvas,
  DeltaBadge,
  fN,
  f$,
  fPct,
  delta,
  COLORS,
} from "./WalmartHelpers";

export function WalmartInventory({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);
  const [selectedItem, setSelectedItem] = useState(null);
  const [selectedPeriod, setSelectedPeriod] = useState("L4W");

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartInventory(filters);
        setData(result);
        if (result?.instockDetail?.length > 0) {
          setSelectedItem(result.instockDetail[0].itemDesc);
        }
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [filters.division, filters.customer]);

  if (loading) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>Loading...</p>
      </Card>
    );
  }
  if (error) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "#f87171" }}>Error: {error}</p>
      </Card>
    );
  }
  if (!data) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>No data</p>
      </Card>
    );
  }

  const kpis = data.kpis || {};
  const instockTrend = data.instockTrend || {};
  const ohDistribution = data.ohDistribution || {};
  const instockByType = data.instockByType || {};
  const instockDetail = data.instockDetail || [];
  const inventoryDetail = data.inventoryDetail || [];

  // Build instock trend chart data
  const itemNames = Object.keys(instockTrend);
  const periods = ["L52W", "L26W", "L13W", "L4W", "LW"];
  const periodLabels = ["52W", "26W", "13W", "4W", "1W"];
  const instockChartLines = itemNames.map((item, idx) => ({
    label: item,
    data: periods.map((p) => instockTrend[item]?.[periods.indexOf(p)] || 0),
    borderColor: Object.values(COLORS)[idx % 5],
  }));

  // Build OH distribution chart data for selected period
  const ohPeriod = selectedPeriod;
  const ohData = ohDistribution[ohPeriod] || {};
  const ohChartData = {
    labels: ohData.items || [],
    datasets: [
      {
        label: "TY",
        data: ohData.ty || [],
        backgroundColor: COLORS.teal,
      },
      {
        label: "LY",
        data: ohData.ly || [],
        backgroundColor: COLORS.orange,
      },
    ],
  };

  // Category sales (instock by type chart)
  const catPeriods = Object.keys(instockByType).slice(0, 4);
  const catChartData = {
    labels: catPeriods,
    datasets: [
      {
        label: "Regular",
        data: catPeriods.map((p) => instockByType[p]?.Regular || 0),
        backgroundColor: COLORS.teal,
      },
      {
        label: "Clearance",
        data: catPeriods.map((p) => instockByType[p]?.Clearance || 0),
        backgroundColor: COLORS.red,
      },
    ],
  };

  // Item instock detail chart
  const selectedItemData = instockDetail.find(
    (row) => row.itemDesc === selectedItem
  );
  const itemInstockChartData = selectedItemData
    ? {
        labels: ["Avg Instock %"],
        datasets: [
          {
            label: selectedItem,
            data: [selectedItemData.avgInstockPct * 100],
            backgroundColor: COLORS.blue,
          },
        ],
      }
    : null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {/* KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(6, 1fr)",
          gap: "12px",
        }}
      >
        <KPICard
          label="Total Store OH"
          value={fN(kpis.totalStoreOh)}
          delta={0}
        />
        <KPICard label="Avg Instock %" value={fPct(kpis.avgInstockPct)} />
        <KPICard label="Retail Inventory" value={f$(kpis.retailInventory)} />
        <KPICard label="Weeks of Supply" value={fN(kpis.weeksOfSupply)} />
        <KPICard
          label="Clearance Instock %"
          value={fPct(kpis.clearanceInstockPct)}
        />
        <KPICard label="CVP Instock %" value={fPct(kpis.cvpInstockPct)} />
      </div>

      {/* Instock % Trend */}
      <Card>
        <CardHdr title="Instock % Trend (52 weeks)" />
        {instockChartLines.length > 0 ? (
          <ChartCanvas
            type="line"
            data={{
              labels: periodLabels,
              datasets: instockChartLines,
            }}
            height={300}
          />
        ) : (
          <p style={{ ...SG(11), color: "var(--txt3)" }}>No trend data</p>
        )}
      </Card>

      {/* OH Distribution */}
      <Card>
        <CardHdr title="OH per Traited Store" />
        <div style={{ marginBottom: "12px" }}>
          <label style={{ ...SG(11, 500), marginRight: "12px" }}>
            Period:
          </label>
          <select
            value={selectedPeriod}
            onChange={(e) => setSelectedPeriod(e.target.value)}
            style={{
              padding: "6px 8px",
              ...SG(11),
              borderRadius: "4px",
              border: "1px solid var(--border)",
            }}
          >
            {Object.keys(ohDistribution).map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </div>
        <ChartCanvas
          type="bar"
          data={ohChartData}
          height={300}
        />
      </Card>

      {/* Two charts side by side */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
        <Card>
          <CardHdr title="Instock by Type" />
          <ChartCanvas type="bar" data={catChartData} height={250} />
        </Card>
        <Card>
          <CardHdr title="Category Sales" />
          <p style={{ ...SG(11), color: "var(--txt3)" }}>Chart placeholder</p>
        </Card>
      </div>

      {/* Item Instock Chart */}
      <Card>
        <CardHdr title="Item Instock Detail" />
        <div style={{ marginBottom: "12px" }}>
          <label style={{ ...SG(11, 500), marginRight: "12px" }}>
            Item:
          </label>
          <select
            value={selectedItem || ""}
            onChange={(e) => setSelectedItem(e.target.value)}
            style={{
              padding: "6px 8px",
              ...SG(11),
              borderRadius: "4px",
              border: "1px solid var(--border)",
            }}
          >
            {instockDetail.map((row) => (
              <option key={row.itemDesc} value={row.itemDesc}>
                {row.itemDesc}
              </option>
            ))}
          </select>
        </div>
        {itemInstockChartData && (
          <ChartCanvas type="bar" data={itemInstockChartData} height={250} />
        )}
      </Card>

      {/* Instock Detail Table */}
      <Card>
        <CardHdr title="Instock Detail" />
        <div
          style={{
            overflowX: "auto",
            fontSize: "11px",
            lineHeight: "1.6",
          }}
        >
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
            }}
          >
            <thead>
              <tr style={{ borderBottom: "1px solid var(--border)" }}>
                <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>Item Desc</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>Traited TY</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>Avg Instock %</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>On Order TY</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>Repl Instock %</th>
              </tr>
            </thead>
            <tbody>
              {instockDetail.map((row, idx) => (
                <tr
                  key={idx}
                  style={{
                    borderBottom: "1px solid var(--border)",
                    backgroundColor: idx % 2 === 0 ? "var(--bg2)" : "transparent",
                  }}
                >
                  <td style={{ padding: "8px", ...SG(11) }}>{row.itemDesc}</td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.traitedTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fPct(row.avgInstockPct)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.onOrderTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fPct(row.replInstockPct)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Inventory Detail Table */}
      <Card>
        <CardHdr title="Inventory Detail" />
        <div
          style={{
            overflowX: "auto",
            fontSize: "11px",
            lineHeight: "1.6",
          }}
        >
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
            }}
          >
            <thead>
              <tr style={{ borderBottom: "1px solid var(--border)" }}>
                <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>Item Desc</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>OH TY</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>OH LY</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>Traited TY</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>Traited LY</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>Instock TY</th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>Clearance OH TY</th>
              </tr>
            </thead>
            <tbody>
              {inventoryDetail.map((row, idx) => (
                <tr
                  key={idx}
                  style={{
                    borderBottom: "1px solid var(--border)",
                    backgroundColor: idx % 2 === 0 ? "var(--bg2)" : "transparent",
                  }}
                >
                  <td style={{ padding: "8px", ...SG(11) }}>{row.itemDesc}</td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.ohTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.ohLy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.traitedTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.traitedLy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.instockTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(row.clearanceOhTy)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
