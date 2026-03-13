import { useState, useEffect } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Cell, ComposedChart, Line, Area,
  AreaChart, Legend
} from "recharts";
import { api, fmt$ } from "../lib/api";
import { TOOLTIP_STYLE } from "../lib/constants";

const COMP_VIEWS = [
  { key: "realtime", label: "Today / WTD / MTD / YTD" },
  { key: "weekly", label: "Weekly Rollups" },
  { key: "monthly", label: "Monthly Rollups" },
  { key: "yearly", label: "Year-over-Year" },
  { key: "monthly2026", label: new Date().getFullYear() + " by Month" },
];

const RANGES = [
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
];

const WATERFALL_COLORS = {
  sales: "#2ECFAA",
  promo: "#8B5CF6",
  adSpend: "#F5B731",
  shipping: "#7BAED0",
  refunds: "#D03030",
  amazonFees: "#3E658C",
  cogs: "#E87830",
  indirect: "#94a3b8",
  netProfit_pos: "#2ECFAA",
  netProfit_neg: "#D03030",
};

export default function Profitability() {
  const [view, setView] = useState("realtime");
  const [periods, setPeriods] = useState([]);
  const [loading, setLoading] = useState(true);
  const [itemDays, setItemDays] = useState(365);
  const [items, setItems] = useState([]);
  const [itemLoading, setItemLoading] = useState(true);
  const [sortCol, setSortCol] = useState("sales");
  const [sortDir, setSortDir] = useState("desc");

  // Load waterfall periods
  useEffect(() => {
    setLoading(true);
    api.profitability(view).then(data => {
      setPeriods(data.periods || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [view]);

  // Load item-level data
  useEffect(() => {
    setItemLoading(true);
    api.profitabilityItems(itemDays).then(data => {
      setItems(data.items || []);
      setItemLoading(false);
    }).catch(() => setItemLoading(false));
  }, [itemDays]);

  // Sort items
  const sortedItems = [...items].sort((a, b) => {
    const av = a[sortCol] ?? 0, bv = b[sortCol] ?? 0;
    return sortDir === "desc" ? bv - av : av - bv;
  });

  const handleSort = (col) => {
    if (sortCol === col) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortCol(col); setSortDir("desc"); }
  };

  // Pick the "primary" period for the big waterfall chart (MTD for realtime, or last item)
  const primaryIdx = view === "realtime" ? 2 : periods.length > 1 ? periods.length - 1 : 0;
  const primary = periods[primaryIdx] || null;

  // Build waterfall chart data from primary period
  const waterfallData = primary ? [
    { name: "Sales", value: primary.sales, fill: WATERFALL_COLORS.sales },
    ...(primary.promo > 0 ? [{ name: "Promo", value: -primary.promo, fill: WATERFALL_COLORS.promo }] : []),
    { name: "Ad Spend", value: -primary.adSpend, fill: WATERFALL_COLORS.adSpend },
    ...(primary.shipping > 0 ? [{ name: "Shipping", value: -primary.shipping, fill: WATERFALL_COLORS.shipping }] : []),
    ...(primary.refunds > 0 ? [{ name: "Refunds", value: -primary.refunds, fill: WATERFALL_COLORS.refunds }] : []),
    { name: "Amazon Fees", value: -primary.amazonFees, fill: WATERFALL_COLORS.amazonFees },
    { name: "COGS", value: -primary.cogs, fill: WATERFALL_COLORS.cogs },
    ...(primary.indirect > 0 ? [{ name: "Indirect", value: -primary.indirect, fill: WATERFALL_COLORS.indirect }] : []),
    { name: "Net Profit", value: primary.netProfit, fill: primary.netProfit >= 0 ? WATERFALL_COLORS.netProfit_pos : WATERFALL_COLORS.netProfit_neg },
  ] : [];

  // Build cost breakdown pie-like data for a horizontal stacked bar
  const costBreakdown = primary ? [
    { name: "COGS", value: primary.cogs, pct: primary.sales > 0 ? round(primary.cogs / primary.sales * 100, 1) : 0, fill: "#E87830" },
    { name: "Amazon Fees", value: primary.amazonFees, pct: primary.sales > 0 ? round(primary.amazonFees / primary.sales * 100, 1) : 0, fill: "#3E658C" },
    { name: "Ad Spend", value: primary.adSpend, pct: primary.sales > 0 ? round(primary.adSpend / primary.sales * 100, 1) : 0, fill: "#F5B731" },
    { name: "Net Profit", value: primary.netProfit, pct: primary.margin, fill: primary.netProfit >= 0 ? "#2ECFAA" : "#D03030" },
  ] : [];

  // Multi-period net profit trend
  const profitTrend = periods.map(p => ({
    label: p.label,
    sales: p.sales,
    netProfit: p.netProfit,
    margin: p.margin,
    adSpend: p.adSpend,
    cogs: p.cogs,
    amazonFees: p.amazonFees,
  }));

  return (
    <>
      <div className="page-header">
        <h1>Profitability</h1>
        <p>Sellerboard-style P&L waterfall &bull; Revenue → deductions → inventory cost → overhead</p>
      </div>

      {/* ── Period View Tabs ─────────────────────────────── */}
      <div className="comp-tabs" style={{ marginBottom: 20 }}>
        {COMP_VIEWS.map(v => (
          <button
            key={v.key}
            className={`comp-tab ${view === v.key ? "active" : ""}`}
            onClick={() => setView(v.key)}
          >
            {v.label}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="loading"><div className="spinner" /> Loading profitability...</div>
      ) : (
        <>
          {/* ── KPI Row ──────────────────────────────────── */}
          {primary && (
            <div className="kpi-grid" style={{ marginBottom: 24 }}>
              <KPI label="Net Revenue" value={fmt$(primary.sales)} className="teal" />
              <KPI label="Net Profit" value={fmt$(primary.netProfit)} className={primary.netProfit >= 0 ? "pos" : "neg"} />
              <KPI label="Margin" value={`${primary.margin}%`} className={primary.margin >= 0 ? "pos" : "neg"} />
              <KPI label="ROI" value={`${primary.roi}%`} className={primary.roi >= 0 ? "pos" : "neg"} />
              <KPI label="Ad Spend" value={fmt$(primary.adSpend)} />
              <KPI label="COGS" value={fmt$(primary.cogs)} />
              <KPI label="Amazon Fees" value={fmt$(primary.amazonFees)} />
              <KPI label="Returns" value={`${primary.refundUnits || 0} (${primary.returnPct || 0}%)`} className={(primary.refundUnits || 0) > 0 ? "neg" : ""} />
              <KPI label="Real ACOS" value={`${primary.realAcos}%`} />
            </div>
          )}

          {/* ── Waterfall Comparison Table ────────────────── */}
          {periods.length > 0 && (
            <div className="table-card" style={{ marginBottom: 24 }}>
              <h3>Profit Waterfall by Period</h3>
              <div style={{ overflowX: "auto" }}>
                <table className="comp-table">
                  <thead>
                    <tr>
                      <th style={{ textAlign: "left", color: "#fff" }}>Component</th>
                      {periods.map(p => (
                        <th key={p.label}>
                          {p.label}
                          {p.sub && <div style={{ fontSize: 10, fontWeight: 400, opacity: 0.7 }}>{p.sub}</div>}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    <WaterfallRow label="Sales" periods={periods} field="sales" cls="revenue-row" />
                    <WaterfallRow label="Promo" periods={periods} field="promo" negative />
                    <WaterfallRow label="Ad Spend" periods={periods} field="adSpend" negative />
                    <WaterfallRow label="Shipping" periods={periods} field="shipping" negative />
                    <WaterfallRow label="Refunds" periods={periods} field="refunds" negative />
                    <WaterfallRow label="Amazon Fees" periods={periods} field="amazonFees" negative bold />
                    <WaterfallRow label="  FBA Fees" periods={periods} field="fbaFees" negative sub />
                    <WaterfallRow label="  Referral Fees" periods={periods} field="referralFees" negative sub />
                    <WaterfallRow label="  Other Fees" periods={periods} field="otherFees" negative sub />
                    <WaterfallRow label="Refund Units" periods={periods} field="refundUnits" isUnit />
                    <WaterfallRow label="Return %" periods={periods} field="returnPct" isSuffix="%" />
                    <tr className="divider-row"><td colSpan={periods.length + 1} style={{ padding: 0 }} /></tr>
                    <WaterfallRow label="COGS" periods={periods} field="cogs" negative />
                    <WaterfallRow label="Indirect" periods={periods} field="indirect" negative />
                    <WaterfallRow label="Net Profit" periods={periods} field="netProfit" cls="profit-row" isPnl />
                    <WaterfallRow label="Margin %" periods={periods} field="margin" isSuffix="%" cls="profit-row" />
                    <WaterfallRow label="ROI %" periods={periods} field="roi" isSuffix="%" />
                    <WaterfallRow label="Real ACOS %" periods={periods} field="realAcos" isSuffix="%" />
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── Charts Row ───────────────────────────────── */}
          <div className="chart-grid">
            {/* Waterfall Bar Chart */}
            <div className="chart-card">
              <h3>Revenue → Net Profit Waterfall</h3>
              <p className="sub">{primary ? primary.label : ""} breakdown</p>
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={waterfallData} margin={{ top: 10, right: 10, bottom: 10, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
                  <XAxis dataKey="name" tick={{ fill: "#6B8090", fontSize: 11 }} />
                  <YAxis tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={v => `$${(Math.abs(v)/1000).toFixed(0)}k`} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [fmt$(Math.abs(v)), v < 0 ? "Cost" : "Amount"]} />
                  <Bar dataKey="value" radius={[4,4,0,0]}>
                    {waterfallData.map((entry, i) => (
                      <Cell key={i} fill={entry.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Cost Breakdown as % of Revenue */}
            <div className="chart-card">
              <h3>Cost Structure (% of Sales)</h3>
              <p className="sub">{primary ? primary.label : ""} — where the money goes</p>
              <div style={{ padding: "20px 0" }}>
                {costBreakdown.map(item => (
                  <div key={item.name} style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
                    <div style={{ width: 100, fontSize: 12, fontWeight: 600, color: "#2A3D50", flexShrink: 0 }}>{item.name}</div>
                    <div style={{ flex: 1, background: "rgba(14,31,45,0.06)", borderRadius: 6, height: 28, overflow: "hidden", position: "relative" }}>
                      <div style={{
                        width: `${Math.min(Math.abs(item.pct), 100)}%`,
                        height: "100%",
                        background: item.fill,
                        borderRadius: 6,
                        transition: "width 0.5s ease",
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "flex-end",
                        paddingRight: 8,
                      }}>
                        {Math.abs(item.pct) > 8 && (
                          <span style={{ color: "#fff", fontSize: 11, fontWeight: 700 }}>{item.pct}%</span>
                        )}
                      </div>
                      {Math.abs(item.pct) <= 8 && (
                        <span style={{ position: "absolute", left: `${Math.abs(item.pct) + 2}%`, top: "50%", transform: "translateY(-50%)", fontSize: 11, fontWeight: 600, color: "#6B8090" }}>{item.pct}%</span>
                      )}
                    </div>
                    <div style={{ width: 80, textAlign: "right", fontSize: 13, fontWeight: 600, fontFamily: "'Space Grotesk', sans-serif", color: "#2A3D50" }}>{fmt$(item.value)}</div>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* ── Multi-period Profit Trend ────────────────── */}
          {profitTrend.length > 1 && (
            <div className="chart-grid" style={{ marginTop: 20 }}>
              <div className="chart-card">
                <h3>Profit Trend Across Periods</h3>
                <p className="sub">Net profit and margin by period</p>
                <ResponsiveContainer width="100%" height={300}>
                  <ComposedChart data={profitTrend}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
                    <XAxis dataKey="label" tick={{ fill: "#6B8090", fontSize: 11 }} />
                    <YAxis yAxisId="left" tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
                    <YAxis yAxisId="right" orientation="right" tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={v => `${v}%`} />
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                    <Legend />
                    <Bar yAxisId="left" dataKey="sales" name="Sales" fill="#2ECFAA" opacity={0.3} radius={[3,3,0,0]} />
                    <Bar yAxisId="left" dataKey="netProfit" name="Net Profit" fill="#3E658C" radius={[3,3,0,0]} />
                    <Line yAxisId="right" type="monotone" dataKey="margin" name="Margin %" stroke="#E87830" strokeWidth={2.5} dot />
                  </ComposedChart>
                </ResponsiveContainer>
              </div>

              <div className="chart-card">
                <h3>Cost Stack by Period</h3>
                <p className="sub">Where revenue goes across periods</p>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={profitTrend}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
                    <XAxis dataKey="label" tick={{ fill: "#6B8090", fontSize: 11 }} />
                    <YAxis tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
                    <Tooltip contentStyle={TOOLTIP_STYLE} formatter={v => [fmt$(v), ""]} />
                    <Legend />
                    <Bar dataKey="cogs" name="COGS" stackId="costs" fill="#E87830" />
                    <Bar dataKey="amazonFees" name="Amazon Fees" stackId="costs" fill="#3E658C" />
                    <Bar dataKey="adSpend" name="Ad Spend" stackId="costs" fill="#F5B731" />
                    <Bar dataKey="netProfit" name="Net Profit" stackId="costs" fill="#2ECFAA" radius={[3,3,0,0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}
        </>
      )}

      {/* ── Item-Level Breakdown ──────────────────────────── */}
      <div style={{ marginTop: 32 }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
          <div>
            <h2 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: 18, fontWeight: 700, color: "var(--navy)", marginBottom: 4 }}>
              Item-Level Profitability
            </h2>
            <p style={{ color: "var(--muted)", fontSize: 13 }}>Per-ASIN profit breakdown &bull; Sellerboard-style metrics</p>
          </div>
          <div className="range-tabs">
            {RANGES.map(r => (
              <button
                key={r.days}
                className={`range-tab ${itemDays === r.days ? "active" : ""}`}
                onClick={() => setItemDays(r.days)}
              >
                {r.label}
              </button>
            ))}
          </div>
        </div>

        {itemLoading ? (
          <div className="loading"><div className="spinner" /> Loading item data...</div>
        ) : (
          <div className="table-card" style={{ padding: 0 }}>
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr style={{ background: "var(--navy)" }}>
                    <SortTh label="Product" col="name" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} align="left" first />
                    <th style={staticThStyle}>Coupon</th>
                    <th style={staticThStyle}>Coupon End</th>
                    <th style={staticThStyle}>Sale Price</th>
                    <th style={staticThStyle}>Sale Dates</th>
                    <SortTh label="Units" col="units" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="Sales" col="sales" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="Ad Spend" col="adSpend" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="Amazon Fees" col="amazonFees" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="COGS" col="cogs" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="Returns" col="refundUnits" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="Return %" col="returnPct" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="Net Profit" col="netProfit" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="Margin" col="margin" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                    <SortTh label="ROI" col="roi" sortCol={sortCol} sortDir={sortDir} onClick={handleSort} />
                  </tr>
                </thead>
                <tbody>
                  {sortedItems.map(item => (
                    <tr key={item.asin} style={{ borderBottom: "1px solid rgba(14,31,45,0.04)" }}>
                      <td style={{ padding: "10px 12px", maxWidth: 220 }}>
                        <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontWeight: 600, color: "var(--body-text)" }}>{item.name}</div>
                        <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 1, fontFamily: "'Space Grotesk', monospace" }}>
                          {item.asin}{item.sku ? ` · ${item.sku}` : ""}
                        </div>
                      </td>
                      <td style={{ ...cellStyle, textAlign: "center" }}>
                        <CouponPill status={item.couponStatus} discount={item.couponDiscount} type={item.couponType} />
                      </td>
                      <td style={{ ...cellStyle, textAlign: "center", fontSize: 11 }}>
                        {item.couponEndDate ? fmtDate(item.couponEndDate) : <span style={{ color: "var(--muted)" }}>—</span>}
                      </td>
                      <td style={{ ...cellStyle, textAlign: "center" }}>
                        {item.salePrice ? (
                          <span>
                            <span style={{ fontWeight: 600, color: "#E87830" }}>${item.salePrice.toFixed(2)}</span>
                            {item.listPrice && item.listPrice > item.salePrice && (
                              <span style={{ fontSize: 10, color: "var(--muted)", textDecoration: "line-through", marginLeft: 4 }}>
                                ${item.listPrice.toFixed(2)}
                              </span>
                            )}
                          </span>
                        ) : (
                          <span style={{ color: "var(--muted)" }}>—</span>
                        )}
                      </td>
                      <td style={{ ...cellStyle, textAlign: "center", fontSize: 10 }}>
                        {item.salePriceStartDate || item.salePriceEndDate ? (
                          <span>
                            {item.salePriceStartDate ? fmtDate(item.salePriceStartDate) : "—"}
                            <span style={{ color: "var(--muted)", margin: "0 2px" }}>→</span>
                            {item.salePriceEndDate ? fmtDate(item.salePriceEndDate) : "—"}
                          </span>
                        ) : (
                          <span style={{ color: "var(--muted)" }}>—</span>
                        )}
                      </td>
                      <td style={cellStyle}>{item.units.toLocaleString()}</td>
                      <td style={cellStyle}>{fmt$(item.sales)}</td>
                      <td style={cellStyle}>{fmt$(item.adSpend)}</td>
                      <td style={cellStyle}>{fmt$(item.amazonFees)}</td>
                      <td style={cellStyle}>{fmt$(item.cogs)}</td>
                      <td style={{ ...cellStyle, color: (item.refundUnits || 0) > 0 ? "var(--neg)" : undefined }}>{(item.refundUnits || 0).toLocaleString()}</td>
                      <td style={{ ...cellStyle, color: (item.returnPct || 0) > 0 ? "var(--neg)" : undefined }}>{(item.returnPct || 0)}%</td>
                      <td style={{ ...cellStyle, color: item.netProfit >= 0 ? "var(--pos)" : "var(--neg)", fontWeight: 700 }}>
                        {fmt$(item.netProfit)}
                      </td>
                      <td style={{ ...cellStyle, color: item.margin >= 0 ? "var(--pos)" : "var(--neg)" }}>
                        {item.margin}%
                      </td>
                      <td style={{ ...cellStyle, color: item.roi >= 0 ? "var(--pos)" : "var(--neg)" }}>
                        {item.roi}%
                      </td>
                    </tr>
                  ))}
                  {/* Totals row */}
                  {sortedItems.length > 0 && (() => {
                    const totals = sortedItems.reduce((acc, i) => ({
                      units: acc.units + i.units,
                      sales: acc.sales + i.sales,
                      adSpend: acc.adSpend + i.adSpend,
                      amazonFees: acc.amazonFees + i.amazonFees,
                      cogs: acc.cogs + i.cogs,
                      netProfit: acc.netProfit + i.netProfit,
                      refundUnits: acc.refundUnits + (i.refundUnits || 0),
                    }), { units: 0, sales: 0, adSpend: 0, amazonFees: 0, cogs: 0, netProfit: 0, refundUnits: 0 });
                    const totalMargin = totals.sales > 0 ? round(totals.netProfit / totals.sales * 100, 1) : 0;
                    const totalRoi = totals.cogs > 0 ? round(totals.netProfit / totals.cogs * 100, 1) : 0;
                    const totalReturnPct = totals.units > 0 ? round(totals.refundUnits / totals.units * 100, 1) : 0;
                    return (
                      <tr style={{ background: "rgba(14,31,45,0.04)", fontWeight: 700, borderTop: "2px solid var(--border)" }}>
                        <td style={{ padding: "12px 12px", color: "var(--navy)" }}>TOTAL ({sortedItems.length} products)</td>
                        <td style={cellStyle}></td>
                        <td style={cellStyle}></td>
                        <td style={cellStyle}></td>
                        <td style={cellStyle}>{totals.units.toLocaleString()}</td>
                        <td style={cellStyle}>{fmt$(totals.sales)}</td>
                        <td style={cellStyle}>{fmt$(totals.adSpend)}</td>
                        <td style={cellStyle}>{fmt$(totals.amazonFees)}</td>
                        <td style={cellStyle}>{fmt$(totals.cogs)}</td>
                        <td style={{ ...cellStyle, color: totals.refundUnits > 0 ? "var(--neg)" : undefined }}>{totals.refundUnits.toLocaleString()}</td>
                        <td style={{ ...cellStyle, color: totalReturnPct > 0 ? "var(--neg)" : undefined }}>{totalReturnPct}%</td>
                        <td style={{ ...cellStyle, color: totals.netProfit >= 0 ? "var(--pos)" : "var(--neg)" }}>{fmt$(totals.netProfit)}</td>
                        <td style={{ ...cellStyle, color: totalMargin >= 0 ? "var(--pos)" : "var(--neg)" }}>{totalMargin}%</td>
                        <td style={{ ...cellStyle, color: totalRoi >= 0 ? "var(--pos)" : "var(--neg)" }}>{totalRoi}%</td>
                      </tr>
                    );
                  })()}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </>
  );
}

/* ── Sub-Components ──────────────────────────────────────── */

const staticThStyle = {
  padding: "12px 12px",
  textAlign: "center",
  color: "rgba(255,255,255,0.7)",
  fontSize: 11,
  fontWeight: 700,
  textTransform: "uppercase",
  letterSpacing: "0.8px",
  whiteSpace: "nowrap",
  borderBottom: "none",
};

const COUPON_COLORS = {
  ACTIVE: { bg: "#ECFDF5", text: "#065F46", border: "#A7F3D0" },
  SCHEDULED: { bg: "#EFF6FF", text: "#1E40AF", border: "#BFDBFE" },
  PAUSED: { bg: "#FEF3C7", text: "#92400E", border: "#FDE68A" },
  EXPIRED: { bg: "#F3F4F6", text: "#6B7280", border: "#E5E7EB" },
};

function CouponPill({ status, discount, type }) {
  if (!status) return <span style={{ color: "var(--muted)", fontSize: 11 }}>—</span>;
  const colors = COUPON_COLORS[status] || COUPON_COLORS.EXPIRED;
  const discountLabel = discount
    ? (type === "PERCENTAGE" ? `${discount}%` : `$${discount}`)
    : "";
  return (
    <span style={{
      display: "inline-block",
      padding: "2px 8px",
      borderRadius: 12,
      fontSize: 10,
      fontWeight: 700,
      letterSpacing: "0.5px",
      background: colors.bg,
      color: colors.text,
      border: `1px solid ${colors.border}`,
      whiteSpace: "nowrap",
    }}>
      {status}{discountLabel ? ` ${discountLabel}` : ""}
    </span>
  );
}

function fmtDate(dateStr) {
  if (!dateStr) return null;
  try {
    const d = new Date(dateStr);
    if (isNaN(d.getTime())) return dateStr;
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch {
    return dateStr;
  }
}

const cellStyle = {
  padding: "10px 12px",
  textAlign: "right",
  whiteSpace: "nowrap",
  fontFamily: "'Space Grotesk', sans-serif",
  fontSize: 13,
  color: "var(--body-text)",
};

function round(n, d = 0) {
  const f = Math.pow(10, d);
  return Math.round(n * f) / f;
}

function KPI({ label, value, className = "" }) {
  return (
    <div className="kpi-card">
      <div className="kpi-label">{label}</div>
      <div className={`kpi-value ${className}`}>{value}</div>
    </div>
  );
}

function WaterfallRow({ label, periods, field, negative = false, cls = "", isPnl = false, isSuffix = "", bold = false, sub = false, isUnit = false }) {
  return (
    <tr className={cls} style={sub ? { background: "rgba(14,31,45,0.02)" } : undefined}>
      <td className="comp-metric" style={{
        fontWeight: bold ? 700 : sub ? 400 : 600,
        color: sub ? "var(--muted)" : undefined,
        fontSize: sub ? 12 : undefined,
        paddingLeft: sub ? 28 : undefined,
      }}>{label}</td>
      {periods.map((p, i) => {
        const val = p[field] ?? 0;
        const display = isUnit ? val.toLocaleString() : isSuffix ? `${val}${isSuffix}` : fmt$(Math.abs(val));
        const prefix = negative && val > 0 ? "-" : "";
        const color = isPnl ? (val >= 0 ? "var(--pos)" : "var(--neg)") :
                      isSuffix ? (field === "margin" || field === "roi" ? (val >= 0 ? "var(--pos)" : "var(--neg)") : "var(--body-text)") :
                      sub ? "var(--muted)" : undefined;
        return (
          <td key={i} style={{ textAlign: "center" }}>
            <span className="comp-value" style={{
              ...(color ? { color } : {}),
              ...(sub ? { fontSize: 12 } : {}),
              ...(bold ? { fontWeight: 700 } : {}),
            }}>
              {isUnit ? display : isSuffix ? display : `${prefix}${display}`}
            </span>
          </td>
        );
      })}
    </tr>
  );
}

function SortTh({ label, col, sortCol, sortDir, onClick, align = "right", first = false }) {
  const active = sortCol === col;
  const arrow = active ? (sortDir === "desc" ? " ▼" : " ▲") : "";
  return (
    <th
      onClick={() => onClick(col)}
      style={{
        padding: "12px 12px",
        textAlign: align,
        color: active ? "#fff" : "rgba(255,255,255,0.7)",
        fontSize: 11,
        fontWeight: 700,
        textTransform: "uppercase",
        letterSpacing: "0.8px",
        cursor: "pointer",
        whiteSpace: "nowrap",
        borderBottom: "none",
        ...(first ? { borderRadius: "8px 0 0 0" } : {}),
      }}
    >
      {label}{arrow}
    </th>
  );
}
