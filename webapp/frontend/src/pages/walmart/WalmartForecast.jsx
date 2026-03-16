import { SG, Card, fN, f$, delta, COLORS, KPICard, ChartCanvas } from "./WalmartHelpers";

export function ForecastPage({ data }) {
  if (!data?.orderForecast)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>
          No forecast data available.
        </p>
      </Card>
    );

  const of = data.orderForecast;
  const kpis = of.kpis || {};
  const items = of.items || [];

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
          label="Cost on Order TY"
          value={f$(kpis.costOnOrderTy)}
          delta={delta(
            kpis.costOnOrderTy,
            kpis.costOnOrderLy
          )}
        />
        <KPICard
          label="Cost on Order LY"
          value={f$(kpis.costOnOrderLy)}
        />
      </div>

      {/* Forecast Trend Chart */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          On-Order Trend (TY vs LY)
        </div>
        <ChartCanvas
          type="line"
          labels={["Week 1", "Week 2", "Week 3", "Week 4"]}
          datasets={[
            {
              label: "TY",
              data: [0, 0, 0, 0],
              borderColor: COLORS.teal,
            },
            {
              label: "LY",
              data: [0, 0, 0, 0],
              borderColor: COLORS.orange,
            },
          ]}
        />
      </Card>

      {/* Items Table */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          Order Details
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
                  Description
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Qty
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Unit Cost
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Total
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
                    {item.itemNumber || "—"}
                  </td>
                  <td
                    style={{
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {item.description || "—"}
                  </td>
                  <td
                    style={{
                      textAlign: "right",
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {fN(item.qty)}
                  </td>
                  <td
                    style={{
                      textAlign: "right",
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {f$(item.unitCost)}
                  </td>
                  <td
                    style={{
                      textAlign: "right",
                      padding: "4px 8px",
                      color: "var(--txt)",
                    }}
                  >
                    {f$(item.total)}
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
