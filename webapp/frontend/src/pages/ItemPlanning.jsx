import { useState, useEffect, useRef, useMemo } from "react";
import { api, fmt$, fmtPct } from "../lib/api";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Legend, ComposedChart, Area
} from "recharts";

const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan27"];
const MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec","Jan '27"];
const SECTION_TABS = [
  { key: "salesUnits", label: "Sales Units" },
  { key: "revenue", label: "Revenue $" },
  { key: "grossProfit", label: "Gross Profit $" },
  { key: "shipments", label: "Shipments" },
  { key: "returns", label: "Returns" },
];
const CATEGORIES = ["GREEN","RED","BLUE","ORANGE","INDIVIDUAL CLUBS","ACCESSORIES"];
const CAT_COLORS = { GREEN: "#2ECFAA", RED: "#E87830", BLUE: "#3E658C", ORANGE: "#F5B731", "INDIVIDUAL CLUBS": "#8B5CF6", ACCESSORIES: "#64748B" };

function kpiIcon(metric) {
  if (metric.includes("Units")) return "📦";
  if (metric.includes("Revenue")) return "💰";
  if (metric.includes("Profit") && metric.includes("$")) return "📈";
  if (metric.includes("AUR")) return "🏷️";
  if (metric.includes("Shipment")) return "🚚";
  if (metric.includes("Return") && metric.includes("Rate")) return "📊";
  if (metric.includes("Return")) return "↩️";
  if (metric.includes("Margin")) return "📊";
  return "📊";
}

function formatKpiValue(metric, val) {
  if (metric.includes("Rate") || metric.includes("Margin")) return (val * 100).toFixed(1) + "%";
  if (metric.includes("$") || metric.includes("Revenue") || metric.includes("Profit") || metric.includes("AUR")) {
    if (metric.includes("AUR")) return "$" + val.toFixed(2);
    return "$" + Math.round(val).toLocaleString();
  }
  return Math.round(val).toLocaleString();
}

function formatPctChange(val) {
  if (val == null || val === 0) return "—";
  const pct = (val * 100).toFixed(1);
  return val > 0 ? `+${pct}%` : `${pct}%`;
}

// ═══════════════════════════════════════════════════════════
// EXECUTIVE SUMMARY COMPONENT
// ═══════════════════════════════════════════════════════════
function ExecutiveSummary({ kpis, sections }) {
  const [activeTab, setActiveTab] = useState("salesUnits");
  const section = sections?.[activeTab] || [];
  const grandTotal = section.find(i => i.type === "grandTotal");
  const categoryTotals = section.filter(i => i.type === "categoryTotal");

  // Chart data: monthly grand totals
  const chartData = MONTHS.map((m, i) => ({
    month: MONTH_LABELS[i],
    value: grandTotal?.monthly?.[m] || 0
  }));

  // Category breakdown for stacked chart
  const stackData = MONTHS.map((m, i) => {
    const row = { month: MONTH_LABELS[i] };
    categoryTotals.forEach(ct => {
      row[ct.category] = ct.monthly?.[m] || 0;
    });
    return row;
  });

  const isCurrency = activeTab === "revenue" || activeTab === "grossProfit";

  return (
    <div className="ip-exec-summary">
      {/* KPI Cards */}
      <div className="ip-kpi-grid">
        {kpis.map((k, i) => (
          <div key={i} className="ip-kpi-card">
            <div className="ip-kpi-icon">{kpiIcon(k.metric)}</div>
            <div className="ip-kpi-content">
              <div className="ip-kpi-label">{k.metric}</div>
              <div className="ip-kpi-value">{formatKpiValue(k.metric, k.tyPlan)}</div>
              <div className="ip-kpi-compare">
                <span className={k.vsLyPct > 0 ? "kpi-up" : k.vsLyPct < 0 ? "kpi-down" : ""}>
                  {formatPctChange(k.vsLyPct)} vs LY
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Section Tabs */}
      <div className="ip-section-tabs">
        {SECTION_TABS.map(t => (
          <button key={t.key} className={`ip-tab ${activeTab === t.key ? "active" : ""}`}
            onClick={() => setActiveTab(t.key)}>{t.label}</button>
        ))}
      </div>

      {/* Charts Row */}
      <div className="ip-charts-row">
        <div className="ip-chart-card">
          <h3>Monthly Trend — {SECTION_TABS.find(t=>t.key===activeTab)?.label}</h3>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" fontSize={11} />
              <YAxis fontSize={11} tickFormatter={v => isCurrency ? `$${(v/1000).toFixed(0)}k` : v.toLocaleString()} />
              <Tooltip formatter={v => isCurrency ? fmt$(v) : v.toLocaleString()} />
              <Bar dataKey="value" fill="#3E658C" radius={[4,4,0,0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="ip-chart-card">
          <h3>By Category</h3>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={stackData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" fontSize={11} />
              <YAxis fontSize={11} tickFormatter={v => isCurrency ? `$${(v/1000).toFixed(0)}k` : v.toLocaleString()} />
              <Tooltip formatter={v => isCurrency ? fmt$(v) : v.toLocaleString()} />
              {categoryTotals.map(ct => (
                <Bar key={ct.category} dataKey={ct.category} stackId="a"
                  fill={CAT_COLORS[ct.category] || "#94A3B8"} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Data Table */}
      <div className="ip-table-wrap">
        <table className="ip-table">
          <thead>
            <tr>
              <th className="sticky-col">Product</th>
              {MONTH_LABELS.map(m => <th key={m}>{m}</th>)}
              <th className="fy-total">FY Total</th>
            </tr>
          </thead>
          <tbody>
            {section.filter(i => i.type !== "grandTotal").map((item, idx) => (
              <tr key={idx} className={
                item.type === "categoryTotal" ? "ip-cat-total" : ""
              } style={item.type === "categoryTotal" ? {} : {}}>
                {item.type === "categoryTotal" ? (
                  <td className="sticky-col ip-cat-name">{item.name}</td>
                ) : (
                  <td className="sticky-col" title={item.sku}>
                    <span className="ip-cat-dot" style={{backgroundColor: CAT_COLORS[item.category] || "#94A3B8"}} />
                    {item.name?.replace("PGA TOUR ", "PGAT ").substring(0, 55)}
                  </td>
                )}
                {MONTHS.map(m => (
                  <td key={m} className="num">
                    {isCurrency ? fmt$(item.monthly?.[m]) : (item.monthly?.[m] || 0).toLocaleString()}
                  </td>
                ))}
                <td className="num fy-total">
                  <strong>{isCurrency ? fmt$(item.fyTotal) : (item.fyTotal || 0).toLocaleString()}</strong>
                </td>
              </tr>
            ))}
            {grandTotal && (
              <tr className="ip-grand-total">
                <td className="sticky-col"><strong>GRAND TOTAL</strong></td>
                {MONTHS.map(m => (
                  <td key={m} className="num">
                    <strong>{isCurrency ? fmt$(grandTotal.monthly?.[m]) : (grandTotal.monthly?.[m] || 0).toLocaleString()}</strong>
                  </td>
                ))}
                <td className="num fy-total">
                  <strong>{isCurrency ? fmt$(grandTotal.fyTotal) : (grandTotal.fyTotal || 0).toLocaleString()}</strong>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════
// ITEM PLAN DETAIL COMPONENT
// ═══════════════════════════════════════════════════════════
function ItemPlanDetail({ plan, overrides, onOverride }) {
  if (!plan) return <div className="ip-empty">Select an item above to view its plan.</div>;

  const sku = plan.sku;
  const ov = overrides?.[sku] || {};

  // Helper to get row data with override support
  function getRow(key, overrideKey) {
    const base = plan[key] || {};
    const override = ov[overrideKey];
    if (override) return { ...base, ...override };
    return base;
  }

  // Build display rows
  const sections = [
    {
      title: "SALES UNITS",
      rows: [
        { label: "TY Plan Units", data: plan.tyPlanUnits, cls: "" },
        { label: "TY Override Units", data: getRow("tyOverrideUnits", "overrideUnits"), cls: "override-row", editable: "overrideUnits" },
        { label: "LY Units", data: plan.lyUnits, cls: "ly-row" },
        { label: "LLY Units", data: plan.llyUnits, cls: "lly-row" },
      ]
    },
    {
      title: "REVENUE ($)",
      rows: [
        { label: "TY Plan Revenue", data: plan.tyPlanRevenue, cls: "", fmt: "$" },
        { label: "TY Projected Revenue", data: plan.tyProjectedRevenue, cls: "projected-row", fmt: "$" },
        { label: "LY Revenue", data: plan.lyRevenue, cls: "ly-row", fmt: "$" },
        { label: "LLY Revenue", data: plan.llyRevenue, cls: "lly-row", fmt: "$" },
      ]
    },
    {
      title: "AUR ($)",
      rows: [
        { label: "TY AUR", data: plan.tyAUR, cls: "", fmt: "$d" },
        { label: "TY AUR Override", data: getRow("tyAUROverride", "aurOverride"), cls: "override-row", editable: "aurOverride", fmt: "$d" },
        { label: "LY AUR", data: plan.lyAUR, cls: "ly-row", fmt: "$d" },
        { label: "LLY AUR", data: plan.llyAUR, cls: "lly-row", fmt: "$d" },
      ]
    },
    {
      title: "PROFITABILITY",
      rows: [
        { label: "TY Plan Gross Profit", data: plan.tyPlanGrossProfit, cls: "", fmt: "$" },
        { label: "LY Gross Profit", data: plan.lyGrossProfit, cls: "ly-row", fmt: "$" },
        { label: "LY Profit Margin %", data: plan.lyProfitMarginPct, cls: "ly-row", fmt: "%" },
      ]
    },
    {
      title: "RETURNS / REFUNDS",
      rows: [
        { label: "TY Plan Refund Units", data: plan.tyPlanRefundUnits, cls: "" },
        { label: "TY Override Refunds", data: getRow("tyOverrideRefundUnits", "refundOverride"), cls: "override-row", editable: "refundOverride" },
        { label: "TY Refund Rate % Override", data: getRow("tyRefundRateOverridePct", "refundRateOverride"), cls: "override-row pct-row", editable: "refundRateOverride", fmt: "%" },
        { label: "LY Refund Units", data: plan.lyRefundUnits, cls: "ly-row" },
        { label: "LY Refund Rate %", data: plan.lyRefundRatePct, cls: "ly-row", fmt: "%" },
      ]
    },
    {
      title: "FBA INVENTORY & SHIPMENTS",
      rows: [
        { label: "Beginning FBA Inventory", data: plan.beginFBAInventory, cls: "" },
        { label: "Recommended Shipment", data: plan.recommendedShipment, cls: "projected-row" },
        { label: "Shipment Override", data: getRow("shipmentOverride", "shipmentOverride"), cls: "override-row", editable: "shipmentOverride" },
        { label: "FBA Shipments In", data: plan.fbaShipmentsIn, cls: "" },
        { label: "Sales (Units Out)", data: plan.salesUnitsOut, cls: "" },
        { label: "Returns In", data: plan.returnsIn, cls: "" },
        { label: "Ending FBA Inventory", data: plan.endingFBAInventory, cls: "total-row" },
        { label: "Weeks of FBA Cover", data: plan.weeksOfFBACover, cls: "", fmt: "d1" },
      ]
    },
    {
      title: "WAREHOUSE INVENTORY",
      rows: [
        { label: "Beginning WH Inventory", data: plan.beginWHInventory, cls: "" },
        { label: "Factory Receipts In", data: plan.factoryReceiptsIn, cls: "" },
        { label: "Shipped to FBA", data: plan.shippedToFBA, cls: "" },
        { label: "Ending WH Inventory", data: plan.endingWHInventory, cls: "total-row" },
        { label: "WH Months of Supply", data: plan.whMonthsOfSupply, cls: "", fmt: "d1" },
      ]
    },
    {
      title: "EFFECTIVE PROJECTIONS",
      rows: [
        { label: "Effective Sales Units", data: plan.effectiveSalesUnits, cls: "total-row" },
        { label: "Effective AUR ($)", data: plan.effectiveAUR, cls: "", fmt: "$d" },
        { label: "Effective Refund Units", data: plan.effectiveRefundUnits, cls: "" },
        { label: "Forecasted Revenue $", data: plan.forecastedRevenue, cls: "total-row", fmt: "$" },
        { label: "Projected Gross Profit $", data: plan.projectedGrossProfit, cls: "", fmt: "$" },
      ]
    },
  ];

  function fmtCell(val, fmt) {
    if (val == null || val === 0) {
      if (fmt === "%" || fmt === "d1") return "—";
      if (fmt === "$" || fmt === "$d") return "$0";
      return "0";
    }
    if (fmt === "$") return "$" + Math.round(val).toLocaleString();
    if (fmt === "$d") return "$" + Number(val).toFixed(2);
    if (fmt === "%") return (val * 100).toFixed(1) + "%";
    if (fmt === "d1") return Number(val).toFixed(1);
    return Math.round(val).toLocaleString();
  }

  // Chart: Revenue + Units comparison
  const chartData = MONTHS.map((m, i) => ({
    month: MONTH_LABELS[i],
    tyUnits: plan.effectiveSalesUnits?.[m] || plan.tyOverrideUnits?.[m] || plan.tyPlanUnits?.[m] || 0,
    lyUnits: plan.lyUnits?.[m] || 0,
    tyRevenue: plan.forecastedRevenue?.[m] || plan.tyProjectedRevenue?.[m] || 0,
    lyRevenue: plan.lyRevenue?.[m] || 0,
  }));

  return (
    <div className="ip-item-detail">
      {/* Item Header */}
      <div className="ip-item-header">
        <div>
          <h2>{plan.productName}</h2>
          <div className="ip-item-meta">
            SKU: <strong>{plan.sku}</strong> &bull; ASIN: <strong>{plan.asin}</strong> &bull;
            Curve: <span className="ip-curve-badge">{plan.curveSelection}</span>
          </div>
        </div>
        <div className="ip-item-fy-summary">
          <div className="ip-fy-metric">
            <span className="ip-fy-label">FY Units</span>
            <span className="ip-fy-value">{(plan.effectiveSalesUnits?.fyTotal || 0).toLocaleString()}</span>
          </div>
          <div className="ip-fy-metric">
            <span className="ip-fy-label">FY Revenue</span>
            <span className="ip-fy-value">{fmt$(plan.forecastedRevenue?.fyTotal)}</span>
          </div>
          <div className="ip-fy-metric">
            <span className="ip-fy-label">Avg AUR</span>
            <span className="ip-fy-value">${(plan.effectiveAUR?.fyTotal || 0).toFixed(2)}</span>
          </div>
          <div className="ip-fy-metric">
            <span className="ip-fy-label">FY Profit</span>
            <span className="ip-fy-value">{fmt$(plan.projectedGrossProfit?.fyTotal)}</span>
          </div>
        </div>
      </div>

      {/* Charts */}
      <div className="ip-charts-row">
        <div className="ip-chart-card">
          <h3>Units: TY Plan vs LY</h3>
          <ResponsiveContainer width="100%" height={220}>
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" fontSize={11} />
              <YAxis fontSize={11} />
              <Tooltip />
              <Bar dataKey="tyUnits" fill="#3E658C" name="TY Units" radius={[3,3,0,0]} />
              <Line type="monotone" dataKey="lyUnits" stroke="#E87830" name="LY Units" strokeWidth={2} dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
        <div className="ip-chart-card">
          <h3>Revenue: TY Plan vs LY</h3>
          <ResponsiveContainer width="100%" height={220}>
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" fontSize={11} />
              <YAxis fontSize={11} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
              <Tooltip formatter={v => fmt$(v)} />
              <Area type="monotone" dataKey="tyRevenue" fill="#3E658C22" stroke="#3E658C" name="TY Revenue" strokeWidth={2} />
              <Line type="monotone" dataKey="lyRevenue" stroke="#E87830" name="LY Revenue" strokeWidth={2} dot={false} strokeDasharray="5 5" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Detail Table Sections */}
      <div className="ip-detail-sections">
        {sections.map((sec, si) => (
          <div key={si} className="ip-detail-section">
            <div className="ip-section-header">{sec.title}</div>
            <div className="ip-table-wrap">
              <table className="ip-table ip-detail-table">
                <thead>
                  <tr>
                    <th className="sticky-col metric-col">Metric</th>
                    <th>S1</th>
                    <th>S2</th>
                    <th className="fy-total">FY</th>
                    {MONTH_LABELS.map(m => <th key={m}>{m}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {sec.rows.map((row, ri) => {
                    if (!row.data) return null;
                    return (
                      <tr key={ri} className={row.cls}>
                        <td className="sticky-col metric-col">{row.label}</td>
                        <td className="num">{fmtCell(row.data?.s1Total, row.fmt)}</td>
                        <td className="num">{fmtCell(row.data?.s2Total, row.fmt)}</td>
                        <td className="num fy-total"><strong>{fmtCell(row.data?.fyTotal, row.fmt)}</strong></td>
                        {MONTHS.map(m => (
                          <td key={m} className="num">{fmtCell(row.data?.[m], row.fmt)}</td>
                        ))}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════
// MAIN ITEM PLANNING PAGE
// ═══════════════════════════════════════════════════════════
export default function ItemPlanning() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [view, setView] = useState("summary"); // summary | item
  const [selectedItem, setSelectedItem] = useState(null);
  const [uploading, setUploading] = useState(false);
  const fileRef = useRef();

  useEffect(() => {
    api.itemPlanning()
      .then(d => { setData(d); setLoading(false); })
      .catch(e => { setError(e.message); setLoading(false); });
  }, []);

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    try {
      await api.uploadItemPlanning(file);
      const fresh = await api.itemPlanning();
      setData(fresh);
    } catch (err) {
      alert("Upload failed: " + err.message);
    }
    setUploading(false);
    fileRef.current.value = "";
  };

  const handleOverride = async (sku, field, values) => {
    await api.itemPlanningOverride(sku, field, values);
    const fresh = await api.itemPlanning();
    setData(fresh);
  };

  // Build item selector options grouped by category
  const itemOptions = useMemo(() => {
    if (!data?.itemPlans) return [];
    const groups = {};
    data.itemPlans.forEach(p => {
      // Determine category from exec summary sections
      const salesItems = data.sections?.salesUnits || [];
      const match = salesItems.find(si => si.sku === p.sku);
      const cat = match?.category || "OTHER";
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push(p);
    });
    return groups;
  }, [data]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading Item Planning...</div>;
  if (error) return <div className="error-state">Error: {error}</div>;
  if (!data) return null;

  const currentPlan = selectedItem ? data.itemPlans.find(p => p.sku === selectedItem) : null;

  return (
    <div className="ip-page">
      {/* Page Header */}
      <div className="ip-page-header">
        <div>
          <h1>Item Planning</h1>
          <p className="ip-subtitle">FY 2026 Amazon Ladder Plan — Sales, Inventory & Profitability</p>
        </div>
        <div className="ip-header-actions">
          {data.lastUpload && (
            <span className="ip-upload-date">Last upload: {data.lastUpload}</span>
          )}
          <input type="file" ref={fileRef} accept=".xlsx,.xls" onChange={handleUpload} hidden />
          <button className="ip-upload-btn" onClick={() => fileRef.current.click()} disabled={uploading}>
            {uploading ? "Uploading..." : "📤 Upload Plan"}
          </button>
        </div>
      </div>

      {/* View Toggle */}
      <div className="ip-view-toggle">
        <button className={`ip-view-btn ${view === "summary" ? "active" : ""}`}
          onClick={() => setView("summary")}>
          📊 Executive Summary
        </button>
        <button className={`ip-view-btn ${view === "item" ? "active" : ""}`}
          onClick={() => setView("item")}>
          📋 Item Plans
        </button>
      </div>

      {view === "summary" && (
        <ExecutiveSummary kpis={data.kpis} sections={data.sections} />
      )}

      {view === "item" && (
        <div className="ip-item-view">
          {/* Item Selector */}
          <div className="ip-selector-bar">
            <label>Select Item:</label>
            <select className="ip-select" value={selectedItem || ""}
              onChange={e => setSelectedItem(e.target.value || null)}>
              <option value="">— Choose an item —</option>
              {Object.entries(itemOptions).map(([cat, items]) => (
                <optgroup key={cat} label={cat}>
                  {items.map(p => (
                    <option key={p.sku} value={p.sku}>
                      {p.productName?.replace("PGA TOUR ", "PGAT ").substring(0, 70)} ({p.sku})
                    </option>
                  ))}
                </optgroup>
              ))}
            </select>
            {data.itemPlans.length > 0 && (
              <span className="ip-item-count">{data.itemPlans.length} items</span>
            )}
          </div>

          <ItemPlanDetail plan={currentPlan} overrides={data.overrides} onOverride={handleOverride} />
        </div>
      )}
    </div>
  );
}
