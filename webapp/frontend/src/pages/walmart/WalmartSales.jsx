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
  CardHdr,
} from "./WalmartHelpers";
import { api } from "../../lib/api";

export function SalesPage({ filters }) {
  // Local state
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedPeriod, setSelectedPeriod] = useState("l4w");
  const [chartMetric, setChartMetric] = useState("sales");

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

  // Extract Regular/Clearance from salesByType for selected period
  const typeData = salesByType[backendPeriod] || {};
  const regSalesTy = typeData["Regular"]?.salesTy || 0;
  const regSalesLy = typeData["Regular"]?.salesLy || 0;
  const clrSalesTy = typeData["Clearance"]?.salesTy || 0;
  const clrSalesLy = typeData["Clearance"]?.salesLy || 0;
  const regPct = regSalesTy + clrSalesTy > 0 ? regSalesTy / (regSalesTy + clrSalesTy) : 0;

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
          value={f$(regSalesTy)}
          delta={delta(regSalesTy, regSalesLy)}
        />
        <KPICard
          label="Clearance TY"
          value={f$(clrSalesTy)}
          delta={delta(clrSalesTy, clrSalesLy)}
        />
        <KPICard
          label="Returns TY"
          value={fN(kpis.returnsTy || 0)}
          delta={delta(kpis.returnsTy, kpis.returnsLy)}
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

      {/* POS Revenue/Units Chart — Full Width with Toggle */}
      <Card>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <div style={{ ...SG(12, 700), color: "var(--txt)" }}>
            {chartMetric === "sales" ? "POS Revenue TY vs LY" : "POS Units TY vs LY"}
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              onClick={() => setChartMetric("sales")}
              style={{
                ...SG(10, chartMetric === "sales" ? 700 : 500),
                padding: "4px 10px",
                borderRadius: 4,
                cursor: "pointer",
                background: chartMetric === "sales" ? "var(--acc1)" : "var(--card)",
                color: chartMetric === "sales" ? "#fff" : "var(--txt3)",
                border: chartMetric === "sales" ? "none" : "1px solid var(--brd)",
              }}
            >
              POS $
            </button>
            <button
              onClick={() => setChartMetric("qty")}
              style={{
                ...SG(10, chartMetric === "qty" ? 700 : 500),
                padding: "4px 10px",
                borderRadius: 4,
                cursor: "pointer",
                background: chartMetric === "qty" ? "var(--acc1)" : "var(--card)",
                color: chartMetric === "qty" ? "#fff" : "var(--txt3)",
                border: chartMetric === "qty" ? "none" : "1px solid var(--brd)",
              }}
            >
              POS Qty
            </button>
          </div>
        </div>
        <ChartCanvas
          type="bar"
          periods={PERIODS}
          datasets={[
            {
              label: "TY",
              data: PERIODS.map((p) => {
                const bk = periodKeyMap[p] || p;
                return chartMetric === "sales"
                  ? data.kpis?.[bk]?.posSalesTy || 0
                  : data.kpis?.[bk]?.posQtyTy || 0;
              }),
              backgroundColor: COLORS.teal,
              borderColor: COLORS.teal,
              order: 2,
            },
            {
              label: "LY",
              type: "line",
              data: PERIODS.map((p) => {
                const bk = periodKeyMap[p] || p;
                return chartMetric === "sales"
                  ? data.kpis?.[bk]?.posSalesLy || 0
                  : data.kpis?.[bk]?.posQtyLy || 0;
              }),
              borderColor: COLORS.orange,
              backgroundColor: "transparent",
              borderWidth: 2,
              pointRadius: 4,
              pointBackgroundColor: COLORS.orange,
              fill: false,
              order: 1,
            },
          ]}
        />
      </Card>

      {/* Sales by Type Table */}
      <Card>
        <CardHdr title="Sales by Type" />
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
                {PERIODS.map((p, pi) => (
                  <Fragment key={p}>
                    {pi > 0 && <th style={{ width: 12 }} />}
                    <th
                      colSpan={3}
                      style={{
                        textAlign: "center",
                        padding: "4px 2px",
                        color: "var(--txt3)",
                      }}
                    >
                      {PERIOD_LABELS[p]}
                    </th>
                  </Fragment>
                ))}
              </tr>
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                <th />
                {PERIODS.map((p, pi) => (
                  <Fragment key={p}>
                    {pi > 0 && <th style={{ width: 12 }} />}
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
              {(() => {
                const allTypes = new Set();
                Object.values(salesByType).forEach((types) =>
                  Object.keys(types).forEach((t) => allTypes.add(t))
                );
                const typeList = [...allTypes].sort();
                if (typeList.length === 0) {
                  return (
                    <tr>
                      <td colSpan={20} style={{ padding: "12px", textAlign: "center", color: "var(--txt3)" }}>
                        No sales by type data available.
                      </td>
                    </tr>
                  );
                }
                return typeList.map((type) => (
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
                    {PERIODS.map((p, pi) => {
                      const bk = periodKeyMap[p] || p;
                      const pVals = salesByType[bk]?.[type] || {
                        salesTy: 0,
                        salesLy: 0,
                      };
                      const d = delta(pVals.salesTy, pVals.salesLy);
                      return (
                        <Fragment key={p}>
                          {pi > 0 && <td style={{ width: 12 }} />}
                          <td
                            style={{
                              textAlign: "right",
                              padding: "4px 4px",
                              color: "var(--txt)",
                            }}
                          >
                            {f$(pVals.salesTy)}
                          </td>
                          <td
                            style={{
                              textAlign: "right",
                              padding: "4px 4px",
                              color: "var(--txt2)",
                            }}
                          >
                            {f$(pVals.salesLy)}
                          </td>
                          <td
                            style={{
                              textAlign: "right",
                              padding: "4px 4px",
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
                ));
              })()}
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

      {/* Item Performance Table — Complete Redesign */}
      <Card>
        <CardHdr title="Item Performance" />
        <div style={{ overflowX: "auto" }}>
          <table
            style={{
              width: "100%",
              borderCollapse: "collapse",
              ...SG(9),
            }}
          >
            <thead>
              {/* Period header row */}
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                <th
                  style={{
                    textAlign: "left",
                    padding: "6px 8px",
                    color: "var(--txt3)",
                    position: "sticky",
                    left: 0,
                    background: "var(--card)",
                    zIndex: 2,
                    minWidth: 180,
                  }}
                >
                  Item
                </th>
                <th
                  style={{
                    textAlign: "left",
                    padding: "6px 4px",
                    color: "var(--txt3)",
                    width: 70,
                  }}
                >
                  Metric
                </th>
                <th
                  style={{
                    textAlign: "right",
                    padding: "6px 4px",
                    color: "var(--txt3)",
                    width: 50,
                  }}
                >
                  % Total
                </th>
                {PERIODS.map((p, pi) => (
                  <Fragment key={p}>
                    {pi > 0 && <th style={{ width: 8 }} />}
                    <th
                      colSpan={3}
                      style={{
                        textAlign: "center",
                        padding: "4px 2px",
                        color: "var(--txt3)",
                        borderBottom: `2px solid ${COLORS.teal}`,
                      }}
                    >
                      {PERIOD_LABELS[p]}
                    </th>
                  </Fragment>
                ))}
              </tr>
              {/* TY/LY/%Chg sub-header */}
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th
                  style={{
                    position: "sticky",
                    left: 0,
                    background: "var(--card)",
                    zIndex: 2,
                  }}
                />
                <th />
                <th />
                {PERIODS.map((p, pi) => (
                  <Fragment key={p}>
                    {pi > 0 && <th style={{ width: 8 }} />}
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
                      %Chg
                    </th>
                  </Fragment>
                ))}
              </tr>
            </thead>
            <tbody>
              {(() => {
                if (items.length === 0) {
                  return (
                    <tr>
                      <td
                        colSpan={3 + PERIODS.length * 4}
                        style={{
                          padding: "12px",
                          textAlign: "center",
                          color: "var(--txt3)",
                        }}
                      >
                        No item data available.
                      </td>
                    </tr>
                  );
                }

                // Calculate totalSalesByPeriod ONCE
                const totalSalesByPeriod = {};
                PERIODS.forEach((p) => {
                  totalSalesByPeriod[p] = items.reduce(
                    (sum, it) => sum + (it[p]?.posTy || 0),
                    0
                  );
                });

                const metrics = [
                  { key: "sales", label: "POS Sales", tyField: "posTy", lyField: "posLy", fmt: f$, showPctTotal: true },
                  { key: "qty", label: "POS Qty", tyField: "qtyTy", lyField: "qtyLy", fmt: fN, showPctTotal: false },
                  { key: "oh", label: "OH Units", tyField: "ohTy", lyField: "ohLy", fmt: fN, showPctTotal: false },
                  { key: "instock", label: "In Stock %", tyField: "instockPct", lyField: null, fmt: null, showPctTotal: false },
                ];

                return (
                  <>
                    {items.map((item, i) => (
                      metrics.map((m, mi) => (
                        <tr
                          key={`${i}-${m.key}`}
                          style={{
                            borderBottom:
                              mi === metrics.length - 1
                                ? "2px solid var(--brd)"
                                : "1px solid rgba(255,255,255,0.03)",
                            background:
                              i % 2 === 0
                                ? "rgba(255,255,255,0.01)"
                                : "transparent",
                          }}
                        >
                          {/* Item name - only on first metric row */}
                          {mi === 0 ? (
                            <td
                              rowSpan={metrics.length}
                              style={{
                                padding: "4px 8px",
                                color: "var(--txt)",
                                ...SG(10, 600),
                                position: "sticky",
                                left: 0,
                                background:
                                  i % 2 === 0
                                    ? "var(--card)"
                                    : "var(--card)",
                                zIndex: 1,
                                verticalAlign: "top",
                                borderBottom: "2px solid var(--brd)",
                              }}
                            >
                              {item.name}
                              {item.brand && (
                                <div
                                  style={{
                                    ...SG(8),
                                    color: "var(--txt3)",
                                    marginTop: 2,
                                  }}
                                >
                                  {item.brand}
                                </div>
                              )}
                            </td>
                          ) : null}
                          {/* Metric label */}
                          <td
                            style={{
                              padding: "3px 4px",
                              color: "var(--txt3)",
                              ...SG(8, 600),
                            }}
                          >
                            {m.label}
                          </td>
                          {/* % of Total */}
                          <td
                            style={{
                              textAlign: "right",
                              padding: "3px 4px",
                              color: "var(--txt2)",
                              ...SG(8),
                            }}
                          >
                            {m.showPctTotal ? (() => {
                              const total =
                                totalSalesByPeriod[selectedPeriod] || 0;
                              const val =
                                item[selectedPeriod]?.posTy || 0;
                              return total > 0
                                ? ((val / total) * 100).toFixed(1) + "%"
                                : "—";
                            })() : ""}
                          </td>
                          {/* Period data */}
                          {PERIODS.map((p, pi) => {
                            const pd = item[p] || {};
                            if (m.key === "instock") {
                              const raw = pd.instockPct;
                              const formatted =
                                raw == null || raw === 0
                                  ? "—"
                                  : raw > 1
                                  ? raw.toFixed(1) + "%"
                                  : ((raw * 100).toFixed(1)) + "%";
                              return (
                                <Fragment key={p}>
                                  {pi > 0 && <td style={{ width: 8 }} />}
                                  <td
                                    style={{
                                      textAlign: "right",
                                      padding: "3px 4px",
                                      color: "var(--txt)",
                                    }}
                                  >
                                    {formatted}
                                  </td>
                                  <td
                                    style={{
                                      textAlign: "right",
                                      padding: "3px 4px",
                                      color: "var(--txt2)",
                                    }}
                                  >
                                    —
                                  </td>
                                  <td
                                    style={{
                                      textAlign: "right",
                                      padding: "3px 4px",
                                    }}
                                  />
                                </Fragment>
                              );
                            }
                            const ty = pd[m.tyField] || 0;
                            const ly = pd[m.lyField] || 0;
                            const d = delta(ty, ly);
                            return (
                              <Fragment key={p}>
                                {pi > 0 && <td style={{ width: 8 }} />}
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: "var(--txt)",
                                  }}
                                >
                                  {m.fmt(ty)}
                                </td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: "var(--txt2)",
                                  }}
                                >
                                  {m.fmt(ly)}
                                </td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
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
                    ))}
                    {/* Total row */}
                    {items.length > 0 && (() => {
                      const totals = {};
                      PERIODS.forEach((p) => {
                        totals[p] = items.reduce(
                          (acc, item) => {
                            const pd = item[p] || {};
                            return {
                              salesTy: acc.salesTy + (pd.posTy || 0),
                              salesLy: acc.salesLy + (pd.posLy || 0),
                              qtyTy: acc.qtyTy + (pd.qtyTy || 0),
                              qtyLy: acc.qtyLy + (pd.qtyLy || 0),
                            };
                          },
                          {
                            salesTy: 0,
                            salesLy: 0,
                            qtyTy: 0,
                            qtyLy: 0,
                          }
                        );
                      });

                      const totalMetrics = [
                        {
                          label: "POS Sales",
                          tyKey: "salesTy",
                          lyKey: "salesLy",
                          fmt: f$,
                        },
                        {
                          label: "POS Qty",
                          tyKey: "qtyTy",
                          lyKey: "qtyLy",
                          fmt: fN,
                        },
                      ];

                      return totalMetrics.map((m, mi) => (
                        <tr
                          key={`total-${m.label}`}
                          style={{
                            borderBottom:
                              mi === totalMetrics.length - 1
                                ? "2px solid var(--brd)"
                                : "none",
                            background: "rgba(46,207,170,0.05)",
                            fontWeight: 700,
                          }}
                        >
                          {mi === 0 && (
                            <td
                              rowSpan={totalMetrics.length}
                              style={{
                                padding: "4px 8px",
                                color: COLORS.teal,
                                ...SG(10, 700),
                                position: "sticky",
                                left: 0,
                                background: "var(--card)",
                                zIndex: 1,
                                borderBottom: "2px solid var(--brd)",
                              }}
                            >
                              TOTAL
                            </td>
                          )}
                          <td
                            style={{
                              padding: "3px 4px",
                              color: COLORS.teal,
                              ...SG(8, 700),
                            }}
                          >
                            {m.label}
                          </td>
                          <td
                            style={{
                              textAlign: "right",
                              padding: "3px 4px",
                              color: COLORS.teal,
                              ...SG(8, 700),
                            }}
                          >
                            {mi === 0 ? "100%" : ""}
                          </td>
                          {PERIODS.map((p, pi) => {
                            const t = totals[p];
                            const ty = t[m.tyKey];
                            const ly = t[m.lyKey];
                            const d = delta(ty, ly);
                            return (
                              <Fragment key={p}>
                                {pi > 0 && <td style={{ width: 8 }} />}
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: "var(--txt)",
                                  }}
                                >
                                  {m.fmt(ty)}
                                </td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: "var(--txt2)",
                                  }}
                                >
                                  {m.fmt(ly)}
                                </td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
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
                      ));
                    })()}
                  </>
                );
              })()}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}
