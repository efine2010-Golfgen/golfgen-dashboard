import { useState, useEffect } from "react";
import { api } from "../../lib/api";
import {
  SG,
  Card,
  CardHdr,
  fN,
  f$,
  delta,
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

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {/* KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(4, 1fr)",
          gap: "12px",
        }}
      >
        <KPICard
          label="DC Count"
          value={fN(kpis.dcCount)}
        />
        <KPICard
          label="Latest Date"
          value={kpis.latestDate ? new Date(kpis.latestDate).toLocaleDateString() : "—"}
        />
        <KPICard
          label="Cost on Order TY"
          value={f$(kpis.costOnOrderTy)}
          delta={delta(kpis.costOnOrderTy, kpis.costOnOrderLy)}
        />
        <KPICard
          label="Whse Cost on Order TY"
          value={f$(kpis.whseCostOnOrderTy)}
        />
      </div>

      {/* On-Order Trend Chart */}
      <Card>
        <CardHdr title="On-Order Trend (TY vs LY)" />
        <ChartCanvas
          type="line"
          data={{
            labels: ["Week 1", "Week 2", "Week 3", "Week 4"],
            datasets: [
              {
                label: "TY",
                data: [kpis.costOnOrderTy || 0, 0, 0, 0],
                borderColor: COLORS.teal,
                fill: false,
              },
              {
                label: "LY",
                data: [kpis.costOnOrderLy || 0, 0, 0, 0],
                borderColor: COLORS.orange,
                fill: false,
              },
            ],
          }}
          height={250}
        />
      </Card>

      {/* DC Details Table */}
      <Card>
        <CardHdr title="Order Details by DC" />
        <div style={{ overflowX: "auto", fontSize: "11px", lineHeight: "1.6" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
            }}
          >
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
                    {item.snapshotDate ? new Date(item.snapshotDate).toLocaleDateString() : "—"}
                  </td>
                  <td style={{ padding: "8px", ...SG(11) }}>
                    {item.storeDcNbr || "—"}
                  </td>
                  <td style={{ padding: "8px", ...SG(11) }}>
                    {item.storeDcType || "—"}
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
