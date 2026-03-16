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

export function WalmartEcomm({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartEcommerce(filters);
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
          gridTemplateColumns: "repeat(5, 1fr)",
          gap: "12px",
        }}
      >
        <KPICard
          label="Auth Sales TY"
          value={f$(kpis.authSalesTy)}
          delta={delta(kpis.authSalesTy, kpis.authSalesLy)}
        />
        <KPICard
          label="Auth Sales LY"
          value={f$(kpis.authSalesLy)}
        />
        <KPICard
          label="Shipped Sales TY"
          value={f$(kpis.shippedSalesTy)}
          delta={delta(kpis.shippedSalesTy, kpis.shippedSalesLy)}
        />
        <KPICard
          label="Shipped Sales LY"
          value={f$(kpis.shippedSalesLy)}
        />
        <KPICard
          label="Total Items"
          value={fN(kpis.totalItems)}
        />
      </div>

      {/* Auth vs Shipped Chart */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
        <Card>
          <CardHdr title="Auth vs Shipped Sales" />
          <ChartCanvas
            type="bar"
            data={{
              labels: ["Auth", "Shipped"],
              datasets: [
                {
                  label: "TY",
                  data: [kpis.authSalesTy || 0, kpis.shippedSalesTy || 0],
                  backgroundColor: COLORS.teal,
                },
                {
                  label: "LY",
                  data: [kpis.authSalesLy || 0, kpis.shippedSalesLy || 0],
                  backgroundColor: COLORS.orange,
                },
              ],
            }}
            height={250}
          />
        </Card>
        <Card>
          <CardHdr title="Top Items — Auth Sales TY vs LY" />
          <ChartCanvas
            type="bar"
            data={{
              labels: items.slice(0, 8).map((item) => {
                const n = item.productName || "";
                return n.length > 20 ? n.substring(0, 20) + "…" : n;
              }),
              datasets: [
                {
                  label: "TY",
                  data: items.slice(0, 8).map((item) => item.authSalesTy || 0),
                  backgroundColor: COLORS.teal,
                },
                {
                  label: "LY",
                  data: items.slice(0, 8).map((item) => item.authSalesLy || 0),
                  backgroundColor: COLORS.orange,
                },
              ],
            }}
            height={250}
          />
        </Card>
      </div>

      {/* Item Detail Table */}
      <Card>
        <CardHdr title="Item Detail" />
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
                  Product Name
                </th>
                <th style={{ textAlign: "left", padding: "8px", ...SG(10, 600) }}>
                  Fineline
                </th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>
                  Auth Sales TY
                </th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>
                  Auth Sales LY
                </th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>
                  Shipped Sales TY
                </th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>
                  Auth Qty TY
                </th>
                <th style={{ textAlign: "right", padding: "8px", ...SG(10, 600) }}>
                  Auth Qty LY
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
                  <td style={{ padding: "8px", ...SG(11) }}>{item.productName}</td>
                  <td style={{ padding: "8px", ...SG(11) }}>{item.fineline}</td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {f$(item.authSalesTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {f$(item.authSalesLy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {f$(item.shippedSalesTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(item.authQtyTy)}
                  </td>
                  <td style={{ padding: "8px", textAlign: "right", ...SG(11) }}>
                    {fN(item.authQtyLy)}
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
