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

export function WalmartInventory({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartInventory(filters);
        setData(result);
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

  // Build instock trend chart data
  // API returns: {itemName: {lw: val, l4w: val, l13w: val, l26w: val, l52w: val}}
  const itemNames = Object.keys(instockTrend);
  const periodKeys = ["lw", "l4w", "l13w", "l26w", "l52w"];
  const periodLabels = ["1W", "4W", "13W", "26W", "52W"];
  const colorValues = Object.values(COLORS);

  const instockChartLines = itemNames.slice(0, 8).map((item, idx) => ({
    label: item.length > 25 ? item.substring(0, 25) + "…" : item,
    data: periodKeys.map((p) => toPercent(instockTrend[item]?.[p])),
    borderColor: colorValues[idx % colorValues.length],
    fill: false,
    tension: 0.3,
  }));

  // Build OH distribution chart (by sales type)
  // API returns: {salesType: totalOh} e.g. {"Regular": 1234, "Clearance": 56}
  const ohLabels = Object.keys(ohDistribution);
  const ohValues = Object.values(ohDistribution);
  const ohChartData = {
    labels: ohLabels,
    datasets: [
      {
        label: "OH Units",
        data: ohValues,
        backgroundColor: [COLORS.teal, COLORS.red, COLORS.orange, COLORS.blue, COLORS.purple],
      },
    ],
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
        <KPICard label="Weeks of Supply" value={kpis.weeksOfSupply != null ? Number(kpis.weeksOfSupply).toFixed(1) : "—"} />
      </div>

      {/* Instock % Trend */}
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

      {/* OH Distribution by Sales Type */}
      {ohLabels.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
          <Card>
            <CardHdr title="OH Distribution by Type" />
            <div style={{ position: "relative", height: 250 }}>
              <ChartCanvas type="bar" data={ohChartData} height={250} />
            </div>
          </Card>
          <Card>
            <CardHdr title="OH Breakdown" />
            <div style={{ position: "relative", height: 250 }}>
              <ChartCanvas type="doughnut" data={ohChartData} height={250} />
            </div>
          </Card>
        </div>
      )}

      {/* Instock Detail Table */}
      {itemNames.length > 0 && (
        <Card>
          <CardHdr title="Instock % by Item & Period" />
          <div style={{ overflowX: "auto", fontSize: "11px", lineHeight: "1.6" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--border)" }}>
                  <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>
                    Item
                  </th>
                  {periodLabels.map((lbl) => (
                    <th
                      key={lbl}
                      style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}
                    >
                      {lbl}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {itemNames.map((item, idx) => (
                  <tr
                    key={idx}
                    style={{
                      borderBottom: "1px solid var(--border)",
                      backgroundColor: idx % 2 === 0 ? "var(--bg2)" : "transparent",
                    }}
                  >
                    <td style={{ padding: "8px", ...SG(11) }}>{item}</td>
                    {periodKeys.map((pk) => {
                      const val = instockTrend[item]?.[pk];
                      return (
                        <td
                          key={pk}
                          style={{
                            padding: "8px",
                            textAlign: "right",
                            ...SG(11),
                            color: instockColor(val),
                          }}
                        >
                          {fInstock(val)}
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
