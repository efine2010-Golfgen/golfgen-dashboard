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
  const [data, setData] = useState(null);
  const [weeklyData, setWeeklyData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedPeriod, setSelectedPeriod] = useState("l4w");
  const [chartMetric, setChartMetric] = useState("sales");

  useEffect(() => {
    async function fetchData() {
      try {
        setLoading(true);
        setError(null);
        const [salesResult, weeklyResult] = await Promise.all([
          api.walmartSales(filters),
          api.walmartWeeklyTrend(filters),
        ]);
        setData(salesResult);
        setWeeklyData(weeklyResult);
      } catch (err) {
        setError(err.message || "Failed to load sales data");
      } finally {
        setLoading(false);
      }
    }
    fetchData();
  }, [JSON.stringify(filters)]);

  if (loading)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>Loading sales data...</p>
      </Card>
    );
  if (error)
    return (
      <Card>
        <p style={{ ...SG(12), color: COLORS.red }}>Error: {error}</p>
      </Card>
    );
  if (!data || !data.kpis)
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>No sales data available.</p>
      </Card>
    );

  const periodKeyMap = { lw: "LW", l4w: "L4W", l13w: "L13W", l26w: "L26W", l52w: "L52W" };
  const backendPeriod = periodKeyMap[selectedPeriod] || selectedPeriod;
  const kpis = data.kpis?.[backendPeriod] || {};
  const salesByType = data.salesByType || {};
  const items = data.items || [];

  const typeData = salesByType[backendPeriod] || {};
  const regSalesTy = typeData["Regular"]?.salesTy || 0;
  const regSalesLy = typeData["Regular"]?.salesLy || 0;
  const clrSalesTy = typeData["Clearance"]?.salesTy || 0;
  const clrSalesLy = typeData["Clearance"]?.salesLy || 0;
  const regPct = regSalesTy + clrSalesTy > 0 ? regSalesTy / (regSalesTy + clrSalesTy) : 0;

  // Weekly trend data for chart
  const weeks = weeklyData?.weeks || [];
  const weekLabels = weeks.map((w) => {
    // Format "202604" → "Wk04" or just show the raw week
    const s = String(w.week);
    return s.length >= 6 ? "Wk" + s.slice(4) : s;
  });

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
        <KPICard label="POS Sales TY" value={f$(kpis.posSalesTy)} delta={delta(kpis.posSalesTy, kpis.posSalesLy)} />
        <KPICard label="POS Units TY" value={fN(kpis.posQtyTy)} delta={delta(kpis.posQtyTy, kpis.posQtyLy)} />
        <KPICard label="Regular Sales TY" value={f$(regSalesTy)} delta={delta(regSalesTy, regSalesLy)} />
        <KPICard label="Clearance TY" value={f$(clrSalesTy)} delta={delta(clrSalesTy, clrSalesLy)} />
        <KPICard label="Returns TY" value={fN(kpis.returnsTy || 0)} delta={delta(kpis.returnsTy, kpis.returnsLy)} color={COLORS.purple} />
        <KPICard label="Reg % of Total" value={fPct(regPct)} delta={null} />
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
              background: selectedPeriod === p ? "var(--acc1)" : "var(--card)",
              color: selectedPeriod === p ? "#fff" : "var(--txt3)",
              border: selectedPeriod === p ? "none" : "1px solid var(--brd)",
            }}
          >
            {PERIOD_LABELS[p]}
          </button>
        ))}
      </div>

      {/* ═══════════ POS Revenue / Units Chart — 52 Weekly Bars + LY Line ═══════════ */}
      <Card>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <div style={{ ...SG(12, 700), color: "var(--txt)" }}>
            {chartMetric === "sales" ? "POS Revenue TY vs LY" : "POS Units TY vs LY"}
          </div>
          <div style={{ display: "flex", gap: 8 }}>
            {[
              { key: "sales", label: "POS $" },
              { key: "qty", label: "POS Qty" },
            ].map((btn) => (
              <button
                key={btn.key}
                onClick={() => setChartMetric(btn.key)}
                style={{
                  ...SG(10, chartMetric === btn.key ? 700 : 500),
                  padding: "4px 10px",
                  borderRadius: 4,
                  cursor: "pointer",
                  background: chartMetric === btn.key ? "var(--acc1)" : "var(--card)",
                  color: chartMetric === btn.key ? "#fff" : "var(--txt3)",
                  border: chartMetric === btn.key ? "none" : "1px solid var(--brd)",
                }}
              >
                {btn.label}
              </button>
            ))}
          </div>
        </div>
        {weeks.length > 0 ? (
          <ChartCanvas
            type="bar"
            labels={weekLabels}
            configKey={`weekly-${chartMetric}-${weeks.length}`}
            height={260}
            datasets={[
              {
                label: "TY",
                data: weeks.map((w) => (chartMetric === "sales" ? w.posSalesTy : w.posQtyTy)),
                backgroundColor: COLORS.teal,
                borderColor: COLORS.teal,
                borderWidth: 1,
                order: 2,
              },
              {
                label: "LY",
                type: "line",
                data: weeks.map((w) => (chartMetric === "sales" ? w.posSalesLy : w.posQtyLy)),
                borderColor: COLORS.orange,
                backgroundColor: "transparent",
                borderWidth: 2,
                pointRadius: 1.5,
                pointBackgroundColor: COLORS.orange,
                fill: false,
                tension: 0.3,
                order: 1,
              },
            ]}
          />
        ) : (
          <div style={{ ...SG(11), color: "var(--txt3)", padding: "40px 0", textAlign: "center" }}>
            No weekly trend data available. Store-level weekly data is needed for this chart.
          </div>
        )}
      </Card>

      {/* ═══════════ Sales by Type Table — Tight columns + Total + % Total ═══════════ */}
      <Card>
        <CardHdr title="Sales by Type" />
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th style={{ textAlign: "left", padding: "6px 6px", color: "var(--txt3)", minWidth: 80 }}>TYPE</th>
                {PERIODS.map((p, pi) => (
                  <Fragment key={p}>
                    {pi > 0 && <th style={{ width: 6, background: "rgba(30,50,72,.4)" }} />}
                    <th colSpan={4} style={{ textAlign: "center", padding: "4px 0", color: "var(--txt3)" }}>
                      {PERIOD_LABELS[p]}
                    </th>
                  </Fragment>
                ))}
              </tr>
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                <th />
                {PERIODS.map((p, pi) => (
                  <Fragment key={p}>
                    {pi > 0 && <th style={{ width: 6, background: "rgba(30,50,72,.4)" }} />}
                    {["TY", "LY", "%Δ", "% Tot"].map((h) => (
                      <th key={h} style={{ textAlign: "right", padding: "2px 3px", color: "var(--txt3)", fontSize: 8, whiteSpace: "nowrap" }}>
                        {h}
                      </th>
                    ))}
                  </Fragment>
                ))}
              </tr>
            </thead>
            <tbody>
              {(() => {
                const allTypes = new Set();
                Object.values(salesByType).forEach((types) => Object.keys(types).forEach((t) => allTypes.add(t)));
                const typeList = [...allTypes].sort();
                if (typeList.length === 0)
                  return (
                    <tr>
                      <td colSpan={30} style={{ padding: "12px", textAlign: "center", color: "var(--txt3)" }}>
                        No sales by type data available.
                      </td>
                    </tr>
                  );

                // Compute totals per period for % Total
                const periodTotals = {};
                PERIODS.forEach((p) => {
                  const bk = periodKeyMap[p] || p;
                  let total = 0;
                  typeList.forEach((type) => {
                    total += salesByType[bk]?.[type]?.salesTy || 0;
                  });
                  periodTotals[p] = total;
                });

                const typeRows = typeList.map((type) => (
                  <tr key={type} style={{ borderBottom: "1px solid var(--brd)" }}>
                    <td style={{ padding: "3px 6px", color: "var(--txt)", ...SG(10, 600) }}>{type}</td>
                    {PERIODS.map((p, pi) => {
                      const bk = periodKeyMap[p] || p;
                      const v = salesByType[bk]?.[type] || { salesTy: 0, salesLy: 0 };
                      const d = delta(v.salesTy, v.salesLy);
                      const pctTotal = periodTotals[p] > 0 ? ((v.salesTy / periodTotals[p]) * 100).toFixed(1) + "%" : "—";
                      return (
                        <Fragment key={p}>
                          {pi > 0 && <td style={{ width: 6, background: "rgba(30,50,72,.4)" }} />}
                          <td style={{ textAlign: "right", padding: "3px 3px", color: "var(--txt)" }}>{f$(v.salesTy)}</td>
                          <td style={{ textAlign: "right", padding: "3px 3px", color: "var(--txt2)" }}>{f$(v.salesLy)}</td>
                          <td style={{ textAlign: "right", padding: "3px 3px", color: d && parseFloat(d) >= 0 ? COLORS.teal : COLORS.red }}>
                            {d != null ? d + "%" : "—"}
                          </td>
                          <td style={{ textAlign: "right", padding: "3px 3px", color: "var(--txt3)", fontSize: 8 }}>{pctTotal}</td>
                        </Fragment>
                      );
                    })}
                  </tr>
                ));

                // Total row
                const totalRow = (
                  <tr key="__total__" style={{ borderTop: "2px solid var(--brd)", background: "rgba(46,207,170,0.05)", fontWeight: 700 }}>
                    <td style={{ padding: "4px 6px", color: COLORS.teal, ...SG(10, 700) }}>Total</td>
                    {PERIODS.map((p, pi) => {
                      const bk = periodKeyMap[p] || p;
                      let ty = 0, ly = 0;
                      typeList.forEach((type) => {
                        ty += salesByType[bk]?.[type]?.salesTy || 0;
                        ly += salesByType[bk]?.[type]?.salesLy || 0;
                      });
                      const d = delta(ty, ly);
                      return (
                        <Fragment key={p}>
                          {pi > 0 && <td style={{ width: 6, background: "rgba(30,50,72,.4)" }} />}
                          <td style={{ textAlign: "right", padding: "4px 3px", color: "var(--txt)" }}>{f$(ty)}</td>
                          <td style={{ textAlign: "right", padding: "4px 3px", color: "var(--txt2)" }}>{f$(ly)}</td>
                          <td style={{ textAlign: "right", padding: "4px 3px", color: d && parseFloat(d) >= 0 ? COLORS.teal : COLORS.red }}>
                            {d != null ? d + "%" : "—"}
                          </td>
                          <td style={{ textAlign: "right", padding: "4px 3px", color: COLORS.teal, ...SG(8, 700) }}>100%</td>
                        </Fragment>
                      );
                    })}
                  </tr>
                );

                return [...typeRows, totalRow];
              })()}
            </tbody>
          </table>
        </div>
      </Card>

      {/* ═══════════ Item Performance Table — NO % Total column ═══════════ */}
      <Card>
        <CardHdr title="Item Performance" />
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
            <thead>
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
                <th style={{ textAlign: "left", padding: "6px 4px", color: "var(--txt3)", width: 70 }}>Metric</th>
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
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th style={{ position: "sticky", left: 0, background: "var(--card)", zIndex: 2 }} />
                <th />
                {PERIODS.map((p, pi) => (
                  <Fragment key={p}>
                    {pi > 0 && <th style={{ width: 8 }} />}
                    {["TY", "LY", "%Chg"].map((h) => (
                      <th key={h} style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 8 }}>
                        {h}
                      </th>
                    ))}
                  </Fragment>
                ))}
              </tr>
            </thead>
            <tbody>
              {(() => {
                if (items.length === 0)
                  return (
                    <tr>
                      <td colSpan={2 + PERIODS.length * 4} style={{ padding: "12px", textAlign: "center", color: "var(--txt3)" }}>
                        No item data available.
                      </td>
                    </tr>
                  );

                const metrics = [
                  { key: "sales", label: "POS Sales", tyField: "posTy", lyField: "posLy", fmt: f$ },
                  { key: "qty", label: "POS Qty", tyField: "qtyTy", lyField: "qtyLy", fmt: fN },
                  { key: "oh", label: "OH Units", tyField: "ohTy", lyField: "ohLy", fmt: fN },
                  { key: "instock", label: "In Stock %", tyField: "instockPct", lyField: null, fmt: null },
                ];

                return (
                  <>
                    {items.map((item, i) =>
                      metrics.map((m, mi) => (
                        <tr
                          key={`${i}-${m.key}`}
                          style={{
                            borderBottom: mi === metrics.length - 1 ? "2px solid var(--brd)" : "1px solid rgba(255,255,255,0.03)",
                            background: i % 2 === 0 ? "rgba(255,255,255,0.01)" : "transparent",
                          }}
                        >
                          {mi === 0 ? (
                            <td
                              rowSpan={metrics.length}
                              style={{
                                padding: "4px 8px",
                                color: "var(--txt)",
                                ...SG(10, 600),
                                position: "sticky",
                                left: 0,
                                background: "var(--card)",
                                zIndex: 1,
                                verticalAlign: "top",
                                borderBottom: "2px solid var(--brd)",
                              }}
                            >
                              {item.name}
                              {item.brand && (
                                <div style={{ ...SG(8), color: "var(--txt3)", marginTop: 2 }}>{item.brand}</div>
                              )}
                            </td>
                          ) : null}
                          <td style={{ padding: "3px 4px", color: "var(--txt3)", ...SG(8, 600) }}>{m.label}</td>
                          {PERIODS.map((p, pi) => {
                            const pd = item[p] || {};
                            if (m.key === "instock") {
                              const raw = pd.instockPct;
                              const formatted =
                                raw == null || raw === 0 ? "—" : raw > 1 ? raw.toFixed(1) + "%" : (raw * 100).toFixed(1) + "%";
                              return (
                                <Fragment key={p}>
                                  {pi > 0 && <td style={{ width: 8 }} />}
                                  <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt)" }}>{formatted}</td>
                                  <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt2)" }}>—</td>
                                  <td style={{ textAlign: "right", padding: "3px 4px" }} />
                                </Fragment>
                              );
                            }
                            const ty = pd[m.tyField] || 0;
                            const ly = pd[m.lyField] || 0;
                            const d = delta(ty, ly);
                            return (
                              <Fragment key={p}>
                                {pi > 0 && <td style={{ width: 8 }} />}
                                <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt)" }}>{m.fmt(ty)}</td>
                                <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt2)" }}>{m.fmt(ly)}</td>
                                <td
                                  style={{
                                    textAlign: "right",
                                    padding: "3px 4px",
                                    color: d && parseFloat(d) >= 0 ? COLORS.teal : COLORS.red,
                                  }}
                                >
                                  {d != null ? d + "%" : "—"}
                                </td>
                              </Fragment>
                            );
                          })}
                        </tr>
                      ))
                    )}
                    {/* Total row */}
                    {items.length > 0 &&
                      (() => {
                        const totals = {};
                        PERIODS.forEach((p) => {
                          const n = items.length || 1;
                          totals[p] = items.reduce(
                            (acc, item) => {
                              const pd = item[p] || {};
                              const raw = pd.instockPct || 0;
                              const pct = raw > 1 ? raw : raw * 100;
                              return {
                                salesTy: acc.salesTy + (pd.posTy || 0),
                                salesLy: acc.salesLy + (pd.posLy || 0),
                                qtyTy: acc.qtyTy + (pd.qtyTy || 0),
                                qtyLy: acc.qtyLy + (pd.qtyLy || 0),
                                ohTy: acc.ohTy + (pd.ohTy || 0),
                                ohLy: acc.ohLy + (pd.ohLy || 0),
                                instockSum: acc.instockSum + pct,
                              };
                            },
                            { salesTy: 0, salesLy: 0, qtyTy: 0, qtyLy: 0, ohTy: 0, ohLy: 0, instockSum: 0 }
                          );
                          totals[p].instockAvg = totals[p].instockSum / n;
                        });
                        const totalMetrics = [
                          { label: "POS Sales", tyKey: "salesTy", lyKey: "salesLy", fmt: f$ },
                          { label: "POS Qty", tyKey: "qtyTy", lyKey: "qtyLy", fmt: fN },
                          { label: "OH Units", tyKey: "ohTy", lyKey: "ohLy", fmt: fN },
                          { label: "In Stock %", tyKey: "instockAvg", lyKey: null, fmt: (v) => v != null ? v.toFixed(1) + "%" : "—" },
                        ];
                        return totalMetrics.map((m, mi) => (
                          <tr
                            key={`total-${m.label}`}
                            style={{
                              borderBottom: mi === totalMetrics.length - 1 ? "2px solid var(--brd)" : "none",
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
                            <td style={{ padding: "3px 4px", color: COLORS.teal, ...SG(8, 700) }}>{m.label}</td>
                            {PERIODS.map((p, pi) => {
                              const t = totals[p];
                              const ty = t[m.tyKey];
                              const ly = m.lyKey ? t[m.lyKey] : null;
                              const d = ly != null ? delta(ty, ly) : null;
                              return (
                                <Fragment key={p}>
                                  {pi > 0 && <td style={{ width: 8 }} />}
                                  <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt)" }}>{m.fmt(ty)}</td>
                                  <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt2)" }}>{ly != null ? m.fmt(ly) : "—"}</td>
                                  <td
                                    style={{
                                      textAlign: "right",
                                      padding: "3px 4px",
                                      color: d && parseFloat(d) >= 0 ? COLORS.teal : COLORS.red,
                                    }}
                                  >
                                    {d != null ? d + "%" : ""}
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
