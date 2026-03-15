import { useState, useEffect, useRef, Fragment, useCallback } from "react";
import { api } from "../lib/api";

const SG = (sz, wt = 600, c) => ({ fontFamily: "'Space Grotesk', monospace", fontSize: sz, fontWeight: wt, ...(c ? { color: c } : {}) });
const DM = (sz, c) => ({ fontFamily: "'DM Serif Display', Georgia, serif", fontSize: sz, ...(c ? { color: c } : {}) });

const Card = ({ children, style }) => (
  <div style={{ background: "var(--card)", borderRadius: 12, border: "1px solid var(--brd)", padding: "18px 20px", ...style }}>{children}</div>
);

const fN = (v) => v == null ? "0" : Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
const f$ = (v) => v == null ? "$0" : "$" + Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
const fPct = (v) => v == null ? "—" : (Number(v) * 100).toFixed(1) + "%";
const delta = (ty, ly) => {
  if (!ly || ly === 0) return null;
  return ((ty - ly) / Math.abs(ly) * 100).toFixed(1);
};

const DeltaBadge = ({ ty, ly, invert }) => {
  const d = delta(ty, ly);
  if (d === null) return <span style={{ ...SG(10), color: "var(--txt3)" }}>—</span>;
  const pos = parseFloat(d) >= 0;
  const good = invert ? !pos : pos;
  return <span style={{ ...SG(10, 600), color: good ? "#2ECFAA" : "#f87171" }}>{pos ? "▲" : "▼"} {Math.abs(d)}%</span>;
};

// Color palette
const COLORS = {
  teal: "#2ECFAA",
  orange: "#E87830",
  blue: "#7BAED0",
  purple: "#A26BE1",
  red: "#f87171",
  yellow: "#F5B731"
};

const PERIODS = ["lw", "l4w", "l13w", "l26w", "l52w"];
const PERIOD_LABELS = { lw: "Last Week", l4w: "L4W", l13w: "L13W", l26w: "L26W", l52w: "L52W" };

export default function WalmartAnalytics({ filters = {} }) {
  const [page, setPage] = useState("sales");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState(null);
  const fileRef = useRef(null);

  const h = filters;

  useEffect(() => {
    setLoading(true);
    setError(null);
    api.walmartAnalytics(h)
      .then(d => { setData(d); setError(null); })
      .catch(e => { setData(null); setError(e.message); })
      .finally(() => setLoading(false));
  }, [filters.division, filters.customer]);

  const handleUpload = async () => {
    const file = fileRef.current?.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadResult(null);
    try {
      const res = await api.retailUpload(file);
      setUploadResult(res);
      // Reload data after successful upload
      const fresh = await api.walmartAnalytics(h);
      setData(fresh);
    } catch (e) {
      setUploadResult({ status: "error", error: e.message });
    }
    setUploading(false);
  };

  if (loading) return <div style={{ padding: "24px", ...SG(12), color: "var(--txt3)" }}>Loading...</div>;

  const isDataAvailable = data?.dataAvailable === true;

  return (
    <div style={{ padding: "0 24px 40px" }}>
      {/* ── Header ── */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <div>
          <h1 style={{ ...DM(22), margin: 0, color: "var(--txt)" }}>Walmart Analytics</h1>
          <p style={{ ...SG(11), color: "var(--txt3)", margin: "2px 0 0" }}>
            Sales performance, inventory health, and order forecasting
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <input type="file" ref={fileRef} accept=".xlsx,.xls" style={{ display: "none" }} onChange={handleUpload} />
          <button
            onClick={() => fileRef.current?.click()}
            disabled={uploading}
            style={{ ...SG(11, 600), background: "var(--acc1)", color: "#fff", border: "none", borderRadius: 6, padding: "6px 14px", cursor: "pointer", opacity: uploading ? 0.6 : 1 }}
          >
            {uploading ? "Uploading..." : "Upload Report"}
          </button>
          {uploadResult && (
            <span style={{ ...SG(10), color: uploadResult.status === "ok" ? "#2ECFAA" : "#f87171" }}>
              {uploadResult.status === "ok"
                ? `✓ ${uploadResult.rows_loaded} rows`
                : `✗ ${uploadResult.error || "Failed"}`}
            </span>
          )}
        </div>
      </div>

      {/* ── Page nav ── */}
      <div style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: "1px solid var(--brd)" }}>
        {[
          { key: "sales", label: "SALES PERFORMANCE" },
          { key: "inventory", label: "INVENTORY HEALTH" },
          { key: "scorecard", label: "VENDOR SCORECARD" },
          { key: "ecomm", label: "ECOMMERCE" },
          { key: "forecast", label: "ORDER FORECAST" }
        ].map(t => (
          <button key={t.key} onClick={() => setPage(t.key)}
            style={{
              ...SG(11, page === t.key ? 700 : 500),
              background: "none", border: "none", cursor: "pointer",
              padding: "8px 14px",
              color: page === t.key ? "var(--acc1)" : "var(--txt3)",
              borderBottom: page === t.key ? "2px solid var(--acc1)" : "2px solid transparent",
            }}>{t.label}</button>
        ))}
      </div>

      {/* ── No data state ── */}
      {!isDataAvailable && (
        <Card style={{ background: "rgba(165, 107, 225, 0.05)", border: "1px solid rgba(165, 107, 225, 0.2)", marginBottom: 16 }}>
          <div style={{ ...DM(16), color: "var(--txt)", marginBottom: 8 }}>No Data Available</div>
          <p style={{ ...SG(11), color: "var(--txt3)", margin: 0, lineHeight: "1.4" }}>
            Upload Walmart Scintilla reports to populate this dashboard. Use the Upload Report button above to get started.
          </p>
        </Card>
      )}

      {/* ── Page content ── */}
      {page === "sales" && <SalesPage data={data} />}
      {page === "inventory" && <InventoryPage data={data} />}
      {page === "scorecard" && <ScorecardPage data={data} />}
      {page === "ecomm" && <EcommPage data={data} />}
      {page === "forecast" && <ForecastPage data={data} />}
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  SALES PERFORMANCE PAGE                         */
/* ═══════════════════════════════════════════════ */
function SalesPage({ data }) {
  const [selectedPeriod, setSelectedPeriod] = useState("lw");
  const [expandedItem, setExpandedItem] = useState(null);
  const [chartView, setChartView] = useState("sales"); // "sales" or "units"

  if (!data?.salesPerformance) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No sales data available.</p></Card>;

  const sp = data.salesPerformance;
  // Map backend period keys (L52W etc) to frontend lowercase keys
  const periodKeyMap = { lw: "LW", l4w: "L4W", l13w: "L13W", l26w: "L26W", l52w: "L52W" };
  const backendPeriod = periodKeyMap[selectedPeriod] || selectedPeriod;
  const kpis = sp.kpis?.[backendPeriod] || {};
  const categories = sp.categories || {};
  const items = sp.items || [];

  // Calculate Reg % of Total
  const regPct = kpis.regularSalesTy && (kpis.regularSalesTy + kpis.clearanceSalesTy) > 0 ? kpis.regularSalesTy / (kpis.regularSalesTy + kpis.clearanceSalesTy) : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
        <KPICard label="POS Sales TY" value={f$(kpis.posSalesTy)} delta={delta(kpis.posSalesTy, kpis.posSalesLy)} />
        <KPICard label="POS Units TY" value={fN(kpis.posQtyTy)} delta={delta(kpis.posQtyTy, kpis.posQtyLy)} />
        <KPICard label="Regular Sales TY" value={f$(kpis.regularSalesTy)} delta={delta(kpis.regularSalesTy, kpis.regularSalesLy)} />
        <KPICard label="Clearance TY" value={f$(kpis.clearanceSalesTy)} delta={delta(kpis.clearanceSalesTy, kpis.clearanceSalesLy)} />
        <KPICard label="Returns TY" value={"—"} delta={null} color={COLORS.purple} />
        <KPICard label="Reg % of Total" value={fPct(regPct)} delta={null} />
      </div>

      {/* Period selector */}
      <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
        {PERIODS.map(p => (
          <button key={p} onClick={() => setSelectedPeriod(p)}
            style={{ ...SG(10, selectedPeriod === p ? 700 : 500), padding: "3px 8px", borderRadius: 4, cursor: "pointer",
              background: selectedPeriod === p ? "var(--acc1)" : "var(--card)",
              color: selectedPeriod === p ? "#fff" : "var(--txt3)",
              border: selectedPeriod === p ? "none" : "1px solid var(--brd)" }}>{PERIOD_LABELS[p]}</button>
        ))}
      </div>

      {/* Revenue & Units Charts */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <Card>
          <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>POS Revenue TY vs LY</div>
          <ChartCanvas type="bar" periods={PERIODS} getData={(p) => [kpis.posSalesTy || 0, kpis.posSalesLy || 0]} />
        </Card>
        <Card>
          <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>POS Units TY vs LY</div>
          <ChartCanvas type="bar" periods={PERIODS} getData={(p) => [kpis.posQtyTy || 0, kpis.posQtyLy || 0]} />
        </Card>
      </div>

      {/* Sales by Type Table */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Sales by Type</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--txt3)" }}>Type</th>
                {PERIODS.map(p => (
                  <th key={p} colSpan={3} style={{ textAlign: "center", padding: "4px 6px", color: "var(--txt3)" }}>{PERIOD_LABELS[p]}</th>
                ))}
              </tr>
              <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                <th />
                {PERIODS.map(p => (
                  <Fragment key={p}>
                    <th style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 8 }}>TY</th>
                    <th style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 8 }}>LY</th>
                    <th style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 8 }}>%Δ</th>
                  </Fragment>
                ))}
              </tr>
            </thead>
            <tbody>
              {(sp.salesByType?.[backendPeriod] ? Object.entries(sp.salesByType[backendPeriod]) : []).map(([type, vals]) => (
                <tr key={type} style={{ borderBottom: "1px solid var(--brd)" }}>
                  <td style={{ padding: "4px 8px", color: "var(--txt)", ...SG(10, 600) }}>{type}</td>
                  {PERIODS.map(p => {
                    const bk = periodKeyMap[p] || p;
                    const pVals = sp.salesByType?.[bk]?.[type] || { salesTy: 0, salesLy: 0 };
                    const d = delta(pVals.salesTy, pVals.salesLy);
                    return (
                      <Fragment key={p}>
                        <td style={{ textAlign: "right", padding: "4px 6px", color: "var(--txt)" }}>{f$(pVals.salesTy)}</td>
                        <td style={{ textAlign: "right", padding: "4px 6px", color: "var(--txt2)" }}>{f$(pVals.salesLy)}</td>
                        <td style={{ textAlign: "right", padding: "4px 6px", color: d && parseFloat(d) >= 0 ? COLORS.teal : COLORS.red }}>{d != null ? d + "%" : "—"}</td>
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
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
          <div style={{ ...SG(12, 700), color: "var(--txt)" }}>Sales by Category</div>
          <div style={{ display: "flex", gap: 6 }}>
            <button onClick={() => setChartView("sales")} style={{ ...SG(9, chartView === "sales" ? 700 : 500), padding: "2px 6px", borderRadius: 3, cursor: "pointer", background: chartView === "sales" ? "var(--acc1)" : "var(--card)", color: chartView === "sales" ? "#fff" : "var(--txt3)", border: "none" }}>$ Sales</button>
            <button onClick={() => setChartView("units")} style={{ ...SG(9, chartView === "units" ? 700 : 500), padding: "2px 6px", borderRadius: 3, cursor: "pointer", background: chartView === "units" ? "var(--acc1)" : "var(--card)", color: chartView === "units" ? "#fff" : "var(--txt3)", border: "none" }}>% Mix</button>
          </div>
        </div>
        <ChartCanvas type="doughnut" labels={Object.keys(categories)} datasets={[{ data: Object.values(categories).map(c => c.salesTy || 0), backgroundColor: [COLORS.teal, COLORS.orange, COLORS.blue, COLORS.purple, COLORS.yellow] }]} />
      </Card>

      {/* Item Performance Table with Expandable Detail Rows */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Item Performance</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th style={{ width: 20 }} />
                <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--txt3)" }} colSpan={2}>Item Description</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>$ Sales TY</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>$ Sales LY</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Δ%</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, i) => {
                const d = delta(item.posSalesTy, item.posSalesLy);
                return (
                  <Fragment key={i}>
                    <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                      <td style={{ textAlign: "center", padding: "4px 6px", cursor: "pointer" }} onClick={() => setExpandedItem(expandedItem === i ? null : i)}>
                        <span style={{ color: "var(--txt3)" }}>{expandedItem === i ? "▼" : "▶"}</span>
                      </td>
                      <td style={{ padding: "4px 8px", color: "var(--txt)" }} colSpan={2}>{item.itemDesc}</td>
                      <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt)" }}>{f$(item.posSalesTy)}</td>
                      <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt2)" }}>{f$(item.posSalesLy)}</td>
                      <td style={{ textAlign: "right", padding: "4px 8px", color: d && parseFloat(d) >= 0 ? COLORS.teal : COLORS.red }}>{d != null ? d + "%" : "—"}</td>
                    </tr>
                    {expandedItem === i && (
                      <tr style={{ borderBottom: "1px solid var(--brd)", background: "rgba(255,255,255,.02)" }}>
                        <td colSpan={6} style={{ padding: "8px" }}>
                          <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12, ...SG(9) }}>
                            <div>
                              <div style={{ ...SG(8, 600), color: "var(--txt3)", marginBottom: 2 }}>Units TY</div>
                              <div style={{ color: "var(--txt)" }}>{fN(item.posQtyTy)}</div>
                            </div>
                            <div>
                              <div style={{ ...SG(8, 600), color: "var(--txt3)", marginBottom: 2 }}>Units LY</div>
                              <div style={{ color: "var(--txt)" }}>{fN(item.posQtyLy)}</div>
                            </div>
                            <div>
                              <div style={{ ...SG(8, 600), color: "var(--txt3)", marginBottom: 2 }}>AUR</div>
                              <div style={{ color: "var(--txt)" }}>{f$(item.aur)}</div>
                            </div>
                            <div>
                              <div style={{ ...SG(8, 600), color: "var(--txt3)", marginBottom: 2 }}>OH Units</div>
                              <div style={{ color: "var(--txt)" }}>{fN(item.onHandTy)}</div>
                            </div>
                            <div>
                              <div style={{ ...SG(8, 600), color: "var(--txt3)", marginBottom: 2 }}>Instock %</div>
                              <div style={{ color: "var(--txt)" }}>{fPct(item.instockPct)}</div>
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

/* ═══════════════════════════════════════════════ */
/*  INVENTORY HEALTH PAGE                          */
/* ═══════════════════════════════════════════════ */
function InventoryPage({ data }) {
  const [selectedPeriod, setSelectedPeriod] = useState("lw");
  const [selectedItem, setSelectedItem] = useState(null);

  if (!data?.inventoryHealth) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No inventory data available.</p></Card>;

  const ih = data.inventoryHealth;
  const kpis = ih.kpis || {};
  const items = ih.instockTrend || {};
  const categories = ih.instockByType?.[selectedPeriod] || {};

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
        <KPICard label="Total Store OH" value={fN(kpis.totalOhUnits)} />
        <KPICard label="Regular Instock %" value={fPct(kpis.regInstockPct)} />
        <KPICard label="Clearance Instock %" value={fPct(kpis.clrInstockPct)} />
        <KPICard label="CVP Instock %" value={fPct(kpis.cvpInstockPct)} />
        <KPICard label="Retail $ Inventory" value={f$(kpis.retailInvValue)} />
        <KPICard label="Weeks of Supply" value={kpis.wksOfSupply?.toFixed(1)} />
      </div>

      {/* Instock Trend Chart (all items) */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Instock % Trend (90 Days)</div>
        <ChartCanvas type="line" labels={["L52W", "L26W", "L13W", "L4W", "LW"]} />
      </Card>

      {/* OH per Store Chart with Period Selector */}
      <Card>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
          <div style={{ ...SG(12, 700), color: "var(--txt)" }}>OH per Store Distribution</div>
          <div style={{ display: "flex", gap: 6 }}>
            {PERIODS.map(p => (
              <button key={p} onClick={() => setSelectedPeriod(p)} style={{ ...SG(8, selectedPeriod === p ? 700 : 500), padding: "2px 6px", borderRadius: 3, cursor: "pointer", background: selectedPeriod === p ? "var(--acc1)" : "var(--card)", color: selectedPeriod === p ? "#fff" : "var(--txt3)", border: "none" }}>{p.toUpperCase()}</button>
            ))}
          </div>
        </div>
        <ChartCanvas type="bar" />
      </Card>

      {/* Instock by Type Chart */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Instock by Type</div>
        <ChartCanvas type="bar" labels={["Regular", "Clearance", "CVP"]} datasets={[{ data: [categories.regular || 0, categories.clearance || 0, categories.cvp || 0], backgroundColor: [COLORS.teal, COLORS.orange, COLORS.blue] }]} />
      </Card>

      {/* Item Instock Detail */}
      <Card>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
          <div style={{ ...SG(12, 700), color: "var(--txt)" }}>Item Instock Analysis</div>
          <select value={selectedItem || ""} onChange={e => setSelectedItem(e.target.value || null)} style={{ ...SG(10), padding: "4px 8px", borderRadius: 4, border: "1px solid var(--brd)", background: "var(--card)", color: "var(--txt)" }}>
            <option value="">Select Item</option>
            {Object.keys(items).map(name => <option key={name} value={name}>{name}</option>)}
          </select>
        </div>
        {selectedItem && items[selectedItem] && (
          <div style={{ padding: "12px", background: "rgba(255,255,255,.02)", borderRadius: 6 }}>
            <div style={{ display: "flex", gap: 24, ...SG(11) }}>
              {items[selectedItem].map((pct, idx) => (
                <div key={idx}>
                  <div style={{ ...SG(9, 600), color: "var(--txt3)", marginBottom: 2 }}>{"52w,26w,13w,4w,1w".split(",")[idx]}</div>
                  <div style={{ color: "var(--txt)" }}>{fPct(pct)}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </Card>
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  VENDOR SCORECARD PAGE                          */
/* ═══════════════════════════════════════════════ */
function ScorecardPage({ data }) {
  if (!data?.scorecard) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No scorecard data available.</p></Card>;

  const scorecard = data.scorecard;

  // Group by vendor → metric group
  const vendors = {};
  scorecard.forEach(r => {
    if (!vendors[r.vendorSection]) vendors[r.vendorSection] = {};
    if (!vendors[r.vendorSection][r.metricGroup]) vendors[r.vendorSection][r.metricGroup] = [];
    vendors[r.vendorSection][r.metricGroup].push(r);
  });

  // Extract unique periods
  const periods = [...new Set(scorecard.map(r => r.period))];

  // Top 5 KPIs by first metric TY value
  const topKpis = scorecard.slice(0, 5);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* Top KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
        {topKpis.map((kpi, i) => (
          <KPICard key={i} label={kpi.metricName} value={fN(kpi.valueTy)} delta={kpi.valueDiff ? (kpi.valueDiff * 100).toFixed(1) : null} />
        ))}
      </div>

      {/* Full Scorecard Matrix */}
      {Object.entries(vendors).map(([vendor, groups]) => (
        <Card key={vendor}>
          <div style={{ ...SG(13, 700), color: "var(--txt)", marginBottom: 12 }}>{vendor}</div>
          {Object.entries(groups).map(([group, rows]) => (
            <div key={group} style={{ marginBottom: 16 }}>
              <div style={{ ...SG(11, 700), color: COLORS.teal, textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 8 }}>{group}</div>
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
                  <thead>
                    <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                      <th style={{ textAlign: "left", padding: "4px 6px", color: "var(--txt3)" }}>Metric</th>
                      {periods.map(p => (
                        <th key={p} colSpan={3} style={{ textAlign: "center", padding: "4px 6px", color: "var(--txt3)" }}>{p}</th>
                      ))}
                    </tr>
                    <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                      <th />
                      {periods.map(p => (
                        <Fragment key={p}>
                          <th style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 8 }}>TY</th>
                          <th style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 8 }}>LY</th>
                          <th style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 8 }}>Diff</th>
                        </Fragment>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {[...new Set(rows.map(r => r.metricName))].map(metric => {
                      const mRows = rows.filter(r => r.metricName === metric);
                      return (
                        <tr key={metric} style={{ borderBottom: "1px solid var(--brd)" }}>
                          <td style={{ padding: "4px 6px", color: "var(--txt)" }}>{metric}</td>
                          {periods.map(p => {
                            const r = mRows.find(x => x.period === p);
                            return (
                              <Fragment key={p}>
                                <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt)" }}>{r ? fN(r.valueTy) : "—"}</td>
                                <td style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt2)" }}>{r ? fN(r.valueLy) : "—"}</td>
                                <td style={{ textAlign: "right", padding: "3px 4px", color: r && r.valueDiff >= 0 ? COLORS.teal : COLORS.red }}>{r && r.valueDiff != null ? (r.valueDiff * 100).toFixed(1) + "%" : "—"}</td>
                              </Fragment>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </Card>
      ))}
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  ECOMMERCE PAGE                                 */
/* ═══════════════════════════════════════════════ */
function EcommPage({ data }) {
  if (!data?.ecommerce) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No eComm data available.</p></Card>;

  const ec = data.ecommerce;
  const kpis = ec.kpis || {};
  const items = ec.items || [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
        <KPICard label="Auth Sales TY" value={f$(kpis.authSalesTy)} delta={delta(kpis.authSalesTy, kpis.authSalesLy)} />
        <KPICard label="Auth Sales LY" value={f$(kpis.authSalesLy)} />
        <KPICard label="Shipped Sales TY" value={f$(kpis.shippedSalesTy)} delta={delta(kpis.shippedSalesTy, kpis.shippedSalesLy)} />
        <KPICard label="Shipped Sales LY" value={f$(kpis.shippedSalesLy)} />
        <KPICard label="Item Count" value={fN(kpis.itemCount)} />
      </div>

      {/* Auth vs Shipped Chart */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Auth vs Shipped</div>
        <ChartCanvas type="bar" labels={["Auth", "Shipped"]} datasets={[{ label: "TY", data: [kpis.authSalesTy || 0, kpis.shippedSalesTy || 0], backgroundColor: COLORS.teal }, { label: "LY", data: [kpis.authSalesLy || 0, kpis.shippedSalesLy || 0], backgroundColor: COLORS.orange }]} />
      </Card>

      {/* Items Table */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Item Detail</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--txt3)" }}>Item #</th>
                <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--txt3)" }}>Product</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Auth $ TY</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Auth $ LY</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Shipped $ TY</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Auth Qty TY</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                  <td style={{ padding: "4px 8px", color: "var(--txt)" }}>{item.itemNumber}</td>
                  <td style={{ padding: "4px 8px", color: "var(--txt)" }}>{item.productName}</td>
                  <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt)" }}>{f$(item.authSalesTy)}</td>
                  <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt2)" }}>{f$(item.authSalesLy)}</td>
                  <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt)" }}>{f$(item.shippedSalesTy)}</td>
                  <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt)" }}>{fN(item.authQtyTy)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  ORDER FORECAST PAGE                            */
/* ═══════════════════════════════════════════════ */
function ForecastPage({ data }) {
  if (!data?.orderForecast) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No forecast data available.</p></Card>;

  const of = data.orderForecast;
  const kpis = of.kpis || {};
  const items = of.items || [];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 12 }}>
        <KPICard label="Cost on Order TY" value={f$(kpis.costOnOrderTy)} delta={delta(kpis.costOnOrderTy, kpis.costOnOrderLy)} />
        <KPICard label="Cost on Order LY" value={f$(kpis.costOnOrderLy)} />
      </div>

      {/* Forecast Trend Chart */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>On-Order Trend (TY vs LY)</div>
        <ChartCanvas type="line" labels={["Week 1", "Week 2", "Week 3", "Week 4"]} datasets={[{ label: "TY", data: [0, 0, 0, 0], borderColor: COLORS.teal }, { label: "LY", data: [0, 0, 0, 0], borderColor: COLORS.orange }]} />
      </Card>

      {/* Items Table */}
      <Card>
        <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Order Details</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--txt3)" }}>Item #</th>
                <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--txt3)" }}>Description</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Qty</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Unit Cost</th>
                <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--txt3)" }}>Total</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                  <td style={{ padding: "4px 8px", color: "var(--txt)" }}>{item.itemNumber || "—"}</td>
                  <td style={{ padding: "4px 8px", color: "var(--txt)" }}>{item.description || "—"}</td>
                  <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt)" }}>{fN(item.qty)}</td>
                  <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt)" }}>{f$(item.unitCost)}</td>
                  <td style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt)" }}>{f$(item.total)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  HELPER COMPONENTS                              */
/* ═══════════════════════════════════════════════ */
function KPICard({ label, value, delta, color = COLORS.teal }) {
  return (
    <div style={{
      background: "var(--card)",
      borderRadius: 12,
      border: `1px solid var(--brd)`,
      borderTop: `3px solid ${color}`,
      padding: "16px 18px"
    }}>
      <div style={{ ...SG(9, 700), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 6 }}>{label}</div>
      <div style={{ ...DM(20), color: "var(--txt)", marginBottom: 4 }}>{value}</div>
      {delta != null && <span style={{ ...SG(10, 600), color: parseFloat(delta) >= 0 ? COLORS.teal : COLORS.red }}>{parseFloat(delta) >= 0 ? "▲" : "▼"} {Math.abs(delta)}%</span>}
    </div>
  );
}

function ChartCanvas({ type, labels, datasets, periods, configKey }) {
  const canvasRef = useRef(null);
  const chartRef = useRef(null);
  const dataRef = useRef({ labels, datasets, periods });
  dataRef.current = { labels, datasets, periods };

  // Use configKey or JSON.stringify a stable key to avoid infinite re-renders
  const stableKey = configKey || JSON.stringify({ labels, datasets: (datasets || []).map(d => d.data) });

  useEffect(() => {
    if (!canvasRef.current || !window.Chart) return;
    if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }

    const { labels: l, datasets: ds, periods: p } = dataRef.current;
    const isDoughnut = type === "doughnut" || type === "pie";
    const config = {
      type,
      data: {
        labels: l || (p ? p.map(pp => PERIOD_LABELS[pp]) : []),
        datasets: ds || []
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { font: { family: "'Space Grotesk', monospace", size: 10 }, color: "rgba(255,255,255,.5)" } }
        },
        ...(isDoughnut ? {} : {
          scales: {
            x: { grid: { color: "rgba(30,50,72,.5)" }, ticks: { font: { family: "'Space Grotesk', monospace", size: 9 }, color: "rgba(255,255,255,.4)" } },
            y: { grid: { color: "rgba(30,50,72,.5)" }, ticks: { font: { family: "'Space Grotesk', monospace", size: 9 }, color: "rgba(255,255,255,.4)" } }
          }
        })
      }
    };

    chartRef.current = new window.Chart(canvasRef.current, config);
    return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
  }, [type, stableKey]);

  return <canvas ref={canvasRef} style={{ height: 200 }} />;
}

