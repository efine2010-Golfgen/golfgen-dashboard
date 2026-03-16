import { SG, Card, fN, f$, delta, COLORS, KPICard, ChartCanvas } from "./WalmartHelpers";

export function EcommPage({ data }) {
  if (!data?.ecommerce)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>
          No eComm data available.
        </p>
      </Card>
    );

  const ec = data.ecommerce;
  const kpis = ec.kpis || {};
  const items = ec.items || [];

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
          label="Item Count"
          value={fN(kpis.itemCount)}
        />
      </div>

      {/* Auth vs Shipped Chart */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          Auth vs Shipped
        </div>
        <ChartCanvas
          type="bar"
          labels={["Auth", "Shipped"]}
          datasets={[
            {
              label: "TY",
              data: [
                kpis.authSalesTy || 0,
                kpis.shippedSalesTy || 0,
              ],
              backgroundColor: COLORS.teal,
            },
            {
              label: "LY",
              data: [
                kpis.authSalesLy || 0,
                kpis.shippedSalesLy || 0,
              ],
              backgroundColor: COLORS.orange,
            },
          ]}
        />
      </Card>

      {/* Items Table */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          Item Detail
        </div>
        <div style={{ overflowX: "auto" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              ...SG(9),
            }}
          >
            <thead>
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th
                  style={{
                    textAlign: "left",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Item #
                </th>
                <th
                  style={{
                    textAlign: "left",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Product
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Auth $ TY
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Auth $ LY
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Shipped $ TY
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Auth Qty TY
                </th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                  <td
                    style={{
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {item.itemNumber}
                  </td>
                  <td
                    style={{
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {item.productName}
                  </td>
                  <td
                    style={{
                      textAlign: "right",
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {f$(item.authSalesTy)}
                  </td>
                  <td
                    style={{
                      textAlign: "right",
                      padding: "4px 8px",
                      color: "var(--txt2)",
                    }}
                  >
                    {f$(item.authSalesLy)}
                  </td>
                  <td
                    style={{
                      textAlign: "right",
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {f$(item.shippedSalesTy)}
                  </td>
                  <td
                    style={{
                      textAlign: "right",
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {fN(item.authQtyTy)}
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
