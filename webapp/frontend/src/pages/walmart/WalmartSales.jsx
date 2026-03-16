import { useState, Fragment } from "react";
import {
  SG,
  DM,
  Card,
  fN,
  f$,
  fPct,
  delta,
  COLORS,
  PERIODS,
  PERIOD_LABELS,
  KPICard,
  ChartCanvas,
} from "./WalmartHelpers";

export function SalesPage({ data }) {
  const [selectedPeriod, setSelectedPeriod] = useState("lw");
  const [expandedItem, setExpandedItem] = useState(null);
  const [chartView, setChartView] = useState("sales"); // "sales" or "units"

  if (!data?.salesPerformance)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>
          No sales data available.
        </p>
      </Card>
    );

  const sp = data.salesPerformance;
  // Map backend period keys (L52W etc) to frontend lowercase keys
  const periodKeyMap = {
    lw: "LW",
    l4w: "L4W",
    l13w: "L13W",
    l26w: "L26W",
    l52w: "L52W",
  };
  const backendPeriod = periodKeyMap[selectedPeriod] || selectedPeriod;
  const kpis = sp.kpis?.[backendPeriod] || {};
  const categories = sp.categories || {};
  const items = sp.items || [];

  // Calculate Reg % of Total
  const regPct =
    kpis.regularSalesTy &&
    kpis.regularSalesTy + kpis.clearanceSalesTy > 0
      ? kpis.regularSalesTy / (kpis.regularSalesTy + kpis.clearanceSalesTy)
      : 0;

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
          label="POS Sales TY"
          value={f$(kpis.posSalesTy)}
          delta={delta(kpis.posSalesTy, kpis.posSalesLy)}
        />
        <KPICard
          label="POS Units TY"
          value={fN(kpis.posQtyTy)}
          delta={delta(kpis.posQtyTy, kpis.posQtyLy)}
        />
        <KPICard
          label="Regular Sales TY"
          value={f$(kpis.regularSalesTy)}
          delta={delta(kpis.regularSalesTy, kpis.regularSalesLy)}
        />
        <KPICard
          label="Clearance TY"
          value={f$(kpis.clearanceSalesTy)}
          delta={delta(kpis.clearanceSalesTy, kpis.clearanceSalesLy)}
        />
        <KPICard
          label="Returns TY"
          value={"—"}
          delta={null}
          color={COLORS.purple}
        />
        <KPICard
          label="Reg % of Total"
          value={fPct(regPct)}
          delta={null}
        />
      </div>

      {/* Period selector */}
      <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
        {PERIODS.map((p) => (
          <button
            key={p}
            onClick={() => setSelectedPeriod(p)}
            style={{
              ...SG(10, selectedPeriod === p ? 700 : 500),
              padding: "3px 8px",
              borderRadius: 4,
              cursor: "pointer",
              background:
                selectedPeriod === p ? "var(--acc1)" : "var(--card)",
              color: selectedPeriod === p ? "#fff" : "var(--txt3)",
              border:
                selectedPeriod === p ? "none" : "1px solid var(--brd)",
            }}
          >
            {PERIOD_LABELS[p]}
          </button>
        ))}
      </div>

      {/* Revenue & Units Charts */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Card>
          <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
            POS Revenue TY vs LY
          </div>
          <ChartCanvas
            type="bar"
            periods={PERIODS}
            getData={(p) => [kpis.posSalesTy || 0, kpis.posSalesLy || 0]}
          />
        </Card>
        <Card>
          <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
            POS Units TY vs LY
          </div>
          <ChartCanvas
            type="bar"
            periods={PERIODS}
            getData={(p) => [kpis.posQtyTy || 0, kpis.posQtyLy || 0]}
          />
        </Card>
      </div>

      {/* Sales by Type Table */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          Sales by Type
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
                  Type
                </th>
                {PERIODS.map((p) => (
                  <th
                    key={p}
                    colSpan={3}
                    style={{
                      textAlign: "center",
                      padding: "4px 6px",
                      color: "var(--txt3)",
                    }}
                  >
                    {PERIOD_LABELS[p]}
                  </th>
                ))}
              </tr>
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                <th />
                {PERIODS.map((p) => (
                  <Fragment key={p}>
                    <th
                      style={{
                        textAlign: "right",
                        padding: "2px 4px",
                        color: "var(--txt3)",
                        fontSize: 8,
                      }}
                    >
                      TY
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: "2px 4px",
                        color: "var(--txt3)",
                        fontSize: 8,
                      }}
                    >
                      LY
                    </th>
                    <th
                      style={{
                        textAlign: "right",
                        padding: "2px 4px",
                        color: "var(--txt3)",
                        fontSize: 8,
                      }}
                    >
                      %Δ
                    </th>
                  </Fragment>
                ))}
              </tr>
            </thead>
            <tbody>
              {(sp.salesByType?.[backendPeriod]
                ? Object.entries(sp.salesByType[backendPeriod])
                : []
              ).map(([type, vals]) => (
                <tr key={type} style={{ borderBottom: "1px solid var(--brd)" }}>
                  <td
                    style={{
                      padding: "4px 8px",
                      color: "var(--txt)",
                      ...SG(10, 600),
                    }}
                  >
                    {type}
                  </td>
                  {PERIODS.map((p) => {
                    const bk = periodKeyMap[p] || p;
                    const pVals =
                      sp.salesByType?.[bk]?.[type] ||
                      { salesTy: 0, salesLy: 0 };
                    const d = delta(pVals.salesTy, pVals.salesLy);
                    return (
                      <Fragment key={p}>
                        <td
                          style={{
                            textAlign: "right",
                            padding: "4px 6px",
                            color: "var(--txt)",
                          }}
                        >
                          {f$(pVals.salesTy)}
                        </td>
                        <td
                          style={{
                            textAlign: "right",
                            padding: "4px 6px",
                            color: "var(--txt2)",
                          }}
                        >
                          {f$(pVals.salesLy)}
                        </td>
                        <td
                          style={{
                            textAlign: "right",
                            padding: "4px 6px",
                            color:
                              d && parseFloat(d) >= 0
                                ? COLORS.teal
                                : COLORS.red,
                          }}
                        >
                          {d != null ? d + "%" : "—"}
                        </td>
                      </Fragment>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Category Chart */}
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
            Sales by Category
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            <button
              onClick={() => setChartView("sales")}
              style={{
                ...SG(9, chartView === "sales" ? 700 : 500),
                padding: "2px 6px",
                borderRadius: 3,
                cursor: "pointer",
                background:
                  chartView === "sales" ? "var(--acc1)" : "var(--card)",
                color: chartView === "sales" ? "#fff" : "var(--txt3)",
                border: "none",
              }}
            >
              $ Sales
            </button>
            <button
              onClick={() => setChartView("units")}
              style={{
                ...SG(9, chartView === "units" ? 700 : 500),
                padding: "2px 6px",
                borderRadius: 3,
                cursor: "pointer",
                background:
                  chartView === "units" ? "var(--acc1)" : "var(--card)",
                color: chartView === "units" ? "#fff" : "var(--txt3)",
                border: "none",
              }}
            >
              % Mix
            </button>
          </div>
        </div>
        <ChartCanvas
          type="doughnut"
          labels={Object.keys(categories)}
          datasets={[
            {
              data: Object.values(categories).map((c) => c.salesTy || 0),
              backgroundColor: [
                COLORS.teal,
                COLORS.orange,
                COLORS.blue,
                COLORS.purple,
                COLORS.yellow,
              ],
            },
          ]}
        />
      </Card>

      {/* Item Performance Table with Expandable Detail Rows */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
          Item Performance
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
                <th style={{ width: 20 }} />
                <th
                  style={{
                    textAlign: "left",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                  colSpan={2}
                >
                  Item Description
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  $ Sales TY
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  $ Sales LY
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                  }}
                >
                  Δ%
                </th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, i) => {
                const d = delta(item.posSalesTy, item.posSalesLy);
                return (
                  <Fragment key={i}>
                    <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                      <td
                        style={{
                          textAlign: "center",
                          padding: "4px 6px",
                          cursor: "pointer",
                        }}
                        onClick={() =>
                          setExpandedItem(expandedItem === i ? null : i)
                        }
                      >
                        <span style={{ color: "var(--txt3)" }}>
                          {expandedItem === i ? "▼" : "▶"}
                        </span>
                      </td>
                      <td
                        style={{
                          padding: "4px 8px",
                          color: "var(--txt)",
                        }}
                        colSpan={2}
                      >
                        {item.itemDesc}
                      </td>
                      <td
                        style={{
                          textAlign: "right",
                          padding: "4px 8px",
                          color: "var(--txt)",
                        }}
                      >
                        {f$(item.posSalesTy)}
                      </td>
                      <td
                        style={{
                          textAlign: "right",
                          padding: "4px 8px",
                          color: "var(--txt2)",
                        }}
                      >
                        {f$(item.posSalesLy)}
                      </td>
                      <td
                        style={{
                          textAlign: "right",
                          padding: "4px 8px",
                          color:
                            d && parseFloat(d) >= 0
                              ? COLORS.teal
                              : COLORS.red,
                        }}
                      >
                        {d != null ? d + "%" : "—"}
                      </td>
                    </tr>
                    {expandedItem === i && (
                      <tr
                        style={{
                          borderBottom: "1px solid var(--brd)",
                          background: "rgba(255,255,255,.02)",
                        }}
                      >
                        <td colSpan={6} style={{ padding: "8px" }}>
                          <div
                            style={{
                              display: "grid",
                              gridTemplateColumns: "repeat(5, 1fr)",
                              gap: 12,
                              ...SG(9),
                            }}
                          >
                            <div>
                              <div
                                style={{
                                  ...SG(8, 600),
                                  color: "var(--txt3)",
                                  marginBottom: 2,
                                }}
                              >
                                Units TY
                              </div>
                              <div style={{ color: "var(--txt)" }}>
                                {fN(item.posQtyTy)}
                              </div>
                            </div>
                            <div>
                              <div
                                style={{
                                  ...SG(8, 600),
                                  color: "var(--txt3)",
                                  marginBottom: 2,
                                }}
                              >
                                Units LY
                              </div>
                              <div style={{ color: "var(--txt)" }}>
                                {fN(item.posQtyLy)}
                              </div>
                            </div>
                            <div>
                              <div
                                style={{
                                  ...SG(8, 600),
                                  color: "var(--txt3)",
                                  marginBottom: 2,
                                }}
                              >
                                AUR
                              </div>
                              <div style={{ color: "var(--txt)" }}>
                                {f$(item.aur)}
                              </div>
                            </div>
                            <div>
                              <div
                                style={{
                                  ...SG(8, 600),
                                  color: "var(--txt3)",
                                  marginBottom: 2,
                                }}
                              >
                                OH Units
                              </div>
                              <div style={{ color: "var(--txt)" }}>
                                {fN(item.onHandTy)}
                              </div>
                            </div>
                            <div>
                              <div
                                style={{
                                  ...SG(8, 600),
                                  color: "var(--txt3)",
                                  marginBottom: 2,
                                }}
                              >
                                Instock %
                              </div>
                              <div style={{ color: "var(--txt)" }}>
                                {fPct(item.instockPct)}
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
