import { useState, useEffect } from "react";
import { api } from "../../lib/api";
import {
  SG,
  Card,
  CardHdr,
  fN,
  COLORS,
  KPICard,
  ChartCanvas,
} from "./WalmartHelpers";

export function WalmartForecast({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartForecast(filters);
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
  const items = data.items || [];

  // Group items by DC type for chart
  const dcTypeCounts = {};
  items.forEach((item) => {
    const t = item.storeDcType || "Unknown";
    dcTypeCounts[t] = (dcTypeCounts[t] || 0) + 1;
  });

  const dcChartData = {
    labels: Object.keys(dcTypeCounts),
    datasets: [
      {
        label: "DCs by Type",
        data: Object.values(dcTypeCounts),
        backgroundColor: [COLORS.teal, COLORS.orange, COLORS.blue, COLORS.purple],
      },
    ],
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {/* KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(2, 1fr)",
          gap: "12px",
        }}
      >
        <KPICard label="DC Count" value={fN(kpis.dcCount)} />
        <KPICard
          label="Latest Snapshot"
          value={kpis.latestDate ? new Date(kpis.latestDate).toLocaleDateString() : "—"}
        />
      </div>

      {/* DC Distribution Chart */}
      {Object.keys(dcTypeCounts).length > 0 && (
        <Card>
          <CardHdr title="DCs by Type" />
          <div style={{ position: "relative", height: 250 }}>
            <ChartCanvas type="bar" data={dcChartData} height={250} />
          </div>
        </Card>
      )}

      {/* DC Details Table */}
      <Card>
        <CardHdr title="Order Forecast Details" />
        <div style={{ overflowX: "auto", fontSize: "11px", lineHeight: "1.6" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--border)" }}>
                <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>
                  Snapshot Date
                </th>
                <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>
                  Store DC #
                </th>
                <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>
                  DC Type
                </th>
                <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>
                  Vendor Dept #
                </th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, idx) => (
                <tr
                  key={idx}
                  style={{
                    borderBottom: "1px solid var(--border)",
                    backgroundColor: idx % 2 === 0 ? "var(--bg2)" : "transparent",
                  }}
                >
                  <td style={{ padding: "8px", ...SG(11) }}>
                    {item.snapshotDate
                      ? new Date(item.snapshotDate).toLocaleDateString()
                      : "—"}
                  </td>
                  <td style={{ padding: "8px", ...SG(11) }}>
                    {item.storeDcNbr || "—"}
                  </td>
                  <td style={{ padding: "8px", ...SG(11) }}>
                    {item.storeDcType || "—"}
                  </td>
                  <td style={{ padding: "8px", ...SG(11) }}>
                    {item.vendorDeptNumber || "—"}
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
