import { useState, useEffect, Fragment } from "react";
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
import { api } from "../../lib/api";

export function SalesPage({ filters }) {
  // Local state
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedPeriod, setSelectedPeriod] = useState("lw");
  const [expandedItem, setExpandedItem] = useState(null);

  // Fetch data on mount and when filters change
  useEffect(() => {
    async function fetchData() {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartSales(filters);
        setData(result);
      } catch (err) {
        setError(err.message || "Failed to load sales data");
      } finally {
        setLoading(false);
      }
    }
    fetchData();
  }, [JSON.stringify(filters)]);

  // Loading state
  if (loading) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>Loading sales data...</p>
      </Card>
    );
  }

  // Error state
  if (error) {
    return (
      <Card>
        <p style={{ ...SG(12), color: COLORS.red }}>Error: {error}</p>
      </Card>
    );
  }

  // No data state
  if (!data || !data.kpis) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>No sales data available.</p>
      </Card>
    );
  }

  // Map frontend lowercase period keys to backend uppercase keys
  const periodKeyMap = {
    lw: "LW",
    l4w: "L4W",
    l13w: "L13W",
    l26w: "L26W",
    l52w: "L52W",
  };

  const backendPeriod = periodKeyMap[selectedPeriod] || selectedPeriod;
  const kpis = data.kpis?.[backendPeriod] || {};
  const salesByType = data.salesByType || {};
  const items = data.items || [];
  const categories = data.categories || {};

  // Calculate Reg % of Total
  const regPct =
    kpis.regularSalesTy && kpis.regularSalesTy + kpis.clearanceSalesTy > 0
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
          value={f$(kpis.returnsTy || 0)}
          delta={delta(kpis.returnsTy, 0)}
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
            datasets={[
              {
                label: "TY",
                data: PERIODS.map((p) => {
                  const bk = periodKeyMap[p] || p;
                  return data.kpis?.[bk]?.posSalesTy || 0;
                }),
                backgroundColor: COLORS.teal,
              },
              {
                label: "LY",
                data: PERIODS.map((p) => {
                  const bk = periodKeyMap[p] || p;
                  return data.kpis?.[bk]?.posSalesLy || 0;
                }),
                backgroundColor: COLORS.orange,
              },
            ]}
          />
        </Card>
        <Card>
          <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
            POS Units TY vs LY
          </div>
          <ChartCanvas
            type="bar"
            periods={PERIODS}
            datasets={[
              {
                label: "TY",
                data: PERIODS.map((p) => {
                  const bk = periodKeyMap[p] || p;
                  return data.kpis?.[bk]?.posQtyTy || 0;
                }),
                backgroundColor: COLORS.teal,
              },
              {
                label: "LY",
                data: PERIODS.map((p) => {
                  const bk = periodKeyMap[p] || p;
                  return data.kpis?.[bk]?.posQtyLy || 0;
                }),
                backgroundColor: COLORS.orange,
              },
            ]}
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
              {Object.entries(salesByType).length > 0 ? (
                Object.entries(salesByType).map(([type, typeByPeriod]) => (
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
                      const pVals = typeByPeriod?.[bk] || {
                        salesTy: 0,
                        salesLy: 0,
                        qtyTy: 0,
                        qtyLy: 0,
                      };
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
                ))
              ) : (
                <tr>
                  <td colSpan={16} style={{ padding: "12px", textAlign: "center", color: "var(--txt3)" }}>
                    No sales by type data available.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Category Chart */}
      {Object.keys(categories).length > 0 && (
        <Card>
          <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
            Sales by Category
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
      )}

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
                >
                  Item Description
                </th>
                <th
                  style={{
                    textAlign: "left",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                    width: 80,
                  }}
                >
                  Brand
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
              {items.length > 0 ? (
                items.map((item, i) => {
                  const d = delta(item.lw?.posSalesTy, item.lw?.posSalesLy);
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
                        >
                          {item.name}
                        </td>
                        <td
                          style={{
                            padding: "4px 8px",
                            color: "var(--txt2)",
                            fontSize: 9,
                          }}
                        >
                          {item.brand}
                        </td>
                        <td
                          style={{
                            textAlign: "right",
                            padding: "4px 8px",
                            color: "var(--txt)",
                          }}
                        >
                          {f$(item.lw?.posSalesTy)}
                        </td>
                        <td
                          style={{
                            textAlign: "right",
                            padding: "4px 8px",
                            color: "var(--txt2)",
                          }}
                        >
                          {f$(item.lw?.posSalesLy)}
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
                          <td colSpan={6} style={{ padding: "12px 8px" }}>
                            <div
                              style={{
                                display: "grid",
                                gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
                                gap: 16,
                                ...SG(9),
                              }}
                            >
                              {/* Iterate through all periods */}
                              {PERIODS.map((p) => {
                                const bk = periodKeyMap[p];
                                const pData = item[p] || {};
                                return (
                                  <div key={p}>
                                    <div
                                      style={{
                                        ...SG(8, 600),
                                        color: "var(--txt3)",
                                        marginBottom: 8,
                                        textTransform: "uppercase",
                                      }}
                                    >
                                      {PERIOD_LABELS[p]}
                                    </div>
                                    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                                      <div>
                                        <div
                                          style={{
                                            ...SG(7),
                                            color: "var(--txt3)",
                                            marginBottom: 1,
                                          }}
                                        >
                                          POS Sales TY
                                        </div>
                                        <div style={{ color: "var(--txt)" }}>
                                          {f$(pData.posSalesTy)}
                                        </div>
                                      </div>
                                      <div>
                                        <div
                                          style={{
                                            ...SG(7),
                                            color: "var(--txt3)",
                                            marginBottom: 1,
                                          }}
                                        >
                                          POS Qty TY
                                        </div>
                                        <div style={{ color: "var(--txt)" }}>
                                          {fN(pData.posQtyTy)}
                                        </div>
                                      </div>
                                      <div>
                                        <div
                                          style={{
                                            ...SG(7),
                                            color: "var(--txt3)",
                                            marginBottom: 1,
                                          }}
                                        >
                                          Traited TY
                                        </div>
                                        <div style={{ color: "var(--txt)" }}>
                                          {fN(pData.traitedTy)}
                                        </div>
                                      </div>
                                      <div>
                                        <div
                                          style={{
                                            ...SG(7),
                                            color: "var(--txt3)",
                                            marginBottom: 1,
                                          }}
                                        >
                                          AUR
                                        </div>
                                        <div style={{ color: "var(--txt)" }}>
                                          {f$(pData.aurTy)}
                                        </div>
                                      </div>
                                      <div>
                                        <div
                                          style={{
                                            ...SG(7),
                                            color: "var(--txt3)",
                                            marginBottom: 1,
                                          }}
                                        >
                                          OH
                                        </div>
                                        <div style={{ color: "var(--txt)" }}>
                                          {fN(pData.ohTy)}
                                        </div>
                                      </div>
                                      <div>
                                        <div
                                          style={{
                                            ...SG(7),
                                            color: "var(--txt3)",
                                            marginBottom: 1,
                                          }}
                                        >
                                          Instock %
                                        </div>
                                        <div style={{ color: "var(--txt)" }}>
                                          {fPct(pData.instockPct)}
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })
              ) : (
                <tr>
                  <td colSpan={6} style={{ padding: "12px", textAlign: "center", color: "var(--txt3)" }}>
                    No item data available.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
