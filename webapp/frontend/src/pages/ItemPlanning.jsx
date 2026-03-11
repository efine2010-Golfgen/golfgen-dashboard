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
  const categoryTotals = section.filter(i => i.type === "categoryTotal");
  // Compute grandTotal by summing category totals if not present in data
  const grandTotal = section.find(i => i.type === "grandTotal") || (() => {
    const monthly = {};
    MONTHS.forEach(m => {
      monthly[m] = categoryTotals.reduce((sum, ct) => sum + (Number(ct.monthly?.[m]) || 0), 0);
    });
    const fyTotal = MONTHS.reduce((s, m) => s + (monthly[m] || 0), 0);
    return { type: "grandTotal", monthly, fyTotal };
  })();

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
// Jan & Feb are actualized months; Mar onward are projected
const ACTUAL_MONTHS = new Set(["Jan", "Feb"]);
function isActual(m) { return ACTUAL_MONTHS.has(m); }

const Q1M = ["Jan","Feb","Mar"], Q2M = ["Apr","May","Jun"];
const Q3M = ["Jul","Aug","Sep"], Q4M = ["Oct","Nov","Dec"];
const S1M = [...Q1M,...Q2M], S2M = [...Q3M,...Q4M], FYM = [...S1M,...S2M];
const PERIOD_COLS = [
  { key:"q1", label:"Q1", months:Q1M, cls:"ip-th-quarter" },
  { key:"q2", label:"Q2", months:Q2M, cls:"ip-th-quarter" },
  { key:"q3", label:"Q3", months:Q3M, cls:"ip-th-quarter" },
  { key:"q4", label:"Q4", months:Q4M, cls:"ip-th-quarter" },
  { key:"s1", label:"S1", months:S1M, cls:"ip-th-half" },
  { key:"s2", label:"S2", months:S2M, cls:"ip-th-half" },
  { key:"fy", label:"FY", months:FYM, cls:"fy-total" },
];
// reduce modes: "sum" (default), "first" (beg inventory), "last" (end inventory), "avg" (rates/AUR)
function periodVal(data, months, reduce) {
  if (!data || !months.length) return 0;
  if (reduce === "first") return Number(data[months[0]]) || 0;
  if (reduce === "last") return Number(data[months[months.length - 1]]) || 0;
  if (reduce === "avg") {
    const s = months.reduce((a, m) => a + (Number(data[m]) || 0), 0);
    return months.length ? s / months.length : 0;
  }
  return months.reduce((a, m) => a + (Number(data[m]) || 0), 0);
}

function ItemPlanDetail({ plan, overrides, onOverride }) {
  if (!plan) return <div className="ip-empty">Select an item above to view its plan.</div>;

  const sku = plan.sku;
  const ov = overrides?.[sku] || {};

  // Local state for editable override fields
  const [localEdits, setLocalEdits] = useState({});
  const [saving, setSaving] = useState({});

  useEffect(() => { setLocalEdits({}); }, [sku]);

  // Helper to get row data with override support
  function getRow(key, overrideKey) {
    const base = plan[key] || {};
    const serverOv = ov[overrideKey];
    const local = localEdits[overrideKey];
    const merged = { ...base };
    if (serverOv) Object.assign(merged, serverOv);
    if (local) Object.assign(merged, local);
    return merged;
  }

  // ── Effective value helpers ──
  function getEffectiveUnits(m) {
    const ovUnits = getRow("tyOverrideUnits", "overrideUnits");
    const v = ovUnits[m];
    if (v != null && v !== "" && v !== 0) return Number(v);
    return plan.tyPlanUnits?.[m] || 0;
  }

  function getEffectiveAUR(m) {
    const ovAUR = getRow("tyAUROverride", "aurOverride");
    const v = ovAUR[m];
    if (v != null && v !== "" && v !== 0) return Number(v);
    return plan.tyAUR?.[m] || 0;
  }

  // Refund rate override drives refund units: if rate override set, refunds = units × rate
  function getEffectiveRefundRate(m) {
    const ovRate = getRow("tyRefundRateOverridePct", "refundRateOverride");
    const v = ovRate[m];
    if (v != null && v !== "" && v !== 0) return Number(v);
    return plan.lyRefundRatePct?.[m] || 0; // fallback to LY rate if no override
  }

  function getEffectiveRefundUnits(m, effectiveUnitsVal) {
    // Priority: direct refund unit override > rate-driven > plan refund units
    const ovRefunds = getRow("tyOverrideRefundUnits", "refundOverride");
    const directVal = ovRefunds[m];
    if (directVal != null && directVal !== "" && directVal !== 0) return Number(directVal);

    // If refund rate override is set, calculate from rate × effective units
    const ovRate = getRow("tyRefundRateOverridePct", "refundRateOverride");
    const rateVal = ovRate[m];
    if (rateVal != null && rateVal !== "" && rateVal !== 0) {
      return Math.round(effectiveUnitsVal * Number(rateVal));
    }

    return plan.tyPlanRefundUnits?.[m] || 0;
  }

  function getEffectiveShipments(m) {
    const ovShip = getRow("shipmentOverride", "shipmentOverride");
    const v = ovShip[m];
    if (v != null && v !== "" && v !== 0) return Number(v);
    return plan.fbaShipmentsIn?.[m] || plan.recommendedShipment?.[m] || 0;
  }

  // ── Full projection engine ──
  const projections = useMemo(() => {
    const effectiveUnits = {};
    const effectiveAUR = {};
    const effectiveRefunds = {};
    const effectiveRefundRate = {};
    const forecastedRevenue = {};
    const projectedProfit = {};

    // Projected FBA inventory (rolling balance)
    const projBegFBA = {};
    const projEndFBA = {};
    const projShipmentsIn = {};
    const projSalesOut = {};
    const projReturnsIn = {};
    const projWeeksCover = {};

    // COGS per unit estimate from plan
    const cogs = plan.tyPlanGrossProfit && plan.tyPlanRevenue
      ? (() => {
          const fyRev = plan.tyPlanRevenue?.fyTotal || 1;
          const fyProfit = plan.tyPlanGrossProfit?.fyTotal || 0;
          const fyUnits = plan.tyPlanUnits?.fyTotal || 1;
          return (fyRev - fyProfit) / fyUnits;
        })()
      : 0;

    let s1Units = 0, s2Units = 0, fyUnits = 0;
    let s1Rev = 0, s2Rev = 0, fyRev = 0;
    let s1Prof = 0, s2Prof = 0, fyProf = 0;
    let s1Ref = 0, s2Ref = 0, fyRef = 0;
    let s1AUR = 0, s2AUR = 0;
    let s1Cnt = 0, s2Cnt = 0;
    let q1Units=0,q2Units=0,q3Units=0,q4Units=0;
    let q1Rev=0,q2Rev=0,q3Rev=0,q4Rev=0;
    let q1Prof=0,q2Prof=0,q3Prof=0,q4Prof=0;
    let q1Ref=0,q2Ref=0,q3Ref=0,q4Ref=0;

    // Starting FBA for rolling calc: use plan's beginning FBA for Jan
    let prevEndFBA = plan.beginFBAInventory?.["Jan"] || 0;

    MONTHS.forEach((m, i) => {
      const units = getEffectiveUnits(m);
      const aur = getEffectiveAUR(m);
      const refunds = getEffectiveRefundUnits(m, units);
      const refundRate = units > 0 ? refunds / units : 0;
      const rev = units * aur;
      const profit = rev - (units * cogs);
      const shipmentsIn = getEffectiveShipments(m);

      effectiveUnits[m] = units;
      effectiveAUR[m] = aur;
      effectiveRefunds[m] = refunds;
      effectiveRefundRate[m] = refundRate;
      forecastedRevenue[m] = rev;
      projectedProfit[m] = profit;

      // FBA inventory rolling balance
      // For actual months (Jan, Feb), use plan data as-is
      if (isActual(m)) {
        projBegFBA[m] = plan.beginFBAInventory?.[m] || 0;
        projShipmentsIn[m] = plan.fbaShipmentsIn?.[m] || 0;
        projSalesOut[m] = plan.salesUnitsOut?.[m] || 0;
        projReturnsIn[m] = plan.returnsIn?.[m] || 0;
        projEndFBA[m] = plan.endingFBAInventory?.[m] || 0;
        prevEndFBA = projEndFBA[m];
      } else {
        // Projected months: rolling calc from overrides
        projBegFBA[m] = prevEndFBA;
        projShipmentsIn[m] = shipmentsIn;
        projSalesOut[m] = units;
        projReturnsIn[m] = refunds;
        projEndFBA[m] = projBegFBA[m] + shipmentsIn - units + refunds;
        prevEndFBA = projEndFBA[m];
      }

      // Weeks of FBA cover: ending inventory / (next month's projected weekly sales)
      // Approximate: weekly sales = units / 4.33
      const weeklySales = units / 4.33;
      projWeeksCover[m] = weeklySales > 0 ? projEndFBA[m] / weeklySales : 0;

      // Only accumulate Jan-Dec (exclude Jan27 at index 12) for period totals
      if (i < 12) {
        fyUnits += units; fyRev += rev; fyProf += profit; fyRef += refunds;
        if (i < 6) { s1Units += units; s1Rev += rev; s1Prof += profit; s1Ref += refunds; s1AUR += aur; s1Cnt++; }
        else { s2Units += units; s2Rev += rev; s2Prof += profit; s2Ref += refunds; s2AUR += aur; s2Cnt++; }
        if (i < 3) { q1Units += units; q1Rev += rev; q1Prof += profit; q1Ref += refunds; }
        else if (i < 6) { q2Units += units; q2Rev += rev; q2Prof += profit; q2Ref += refunds; }
        else if (i < 9) { q3Units += units; q3Rev += rev; q3Prof += profit; q3Ref += refunds; }
        else { q4Units += units; q4Rev += rev; q4Prof += profit; q4Ref += refunds; }
      }
    });

    // Totals (Jan-Dec only, excluding Jan27)
    effectiveUnits.s1Total = s1Units; effectiveUnits.s2Total = s2Units; effectiveUnits.fyTotal = fyUnits;
    effectiveUnits.q1Total = q1Units; effectiveUnits.q2Total = q2Units; effectiveUnits.q3Total = q3Units; effectiveUnits.q4Total = q4Units;
    effectiveAUR.s1Total = s1Cnt ? s1AUR / s1Cnt : 0; effectiveAUR.s2Total = s2Cnt ? s2AUR / s2Cnt : 0;
    effectiveAUR.fyTotal = (s1Cnt + s2Cnt) ? (s1AUR + s2AUR) / (s1Cnt + s2Cnt) : 0;
    effectiveRefunds.s1Total = s1Ref; effectiveRefunds.s2Total = s2Ref; effectiveRefunds.fyTotal = fyRef;
    effectiveRefunds.q1Total = q1Ref; effectiveRefunds.q2Total = q2Ref; effectiveRefunds.q3Total = q3Ref; effectiveRefunds.q4Total = q4Ref;
    forecastedRevenue.s1Total = s1Rev; forecastedRevenue.s2Total = s2Rev; forecastedRevenue.fyTotal = fyRev;
    forecastedRevenue.q1Total = q1Rev; forecastedRevenue.q2Total = q2Rev; forecastedRevenue.q3Total = q3Rev; forecastedRevenue.q4Total = q4Rev;
    projectedProfit.s1Total = s1Prof; projectedProfit.s2Total = s2Prof; projectedProfit.fyTotal = fyProf;
    projectedProfit.q1Total = q1Prof; projectedProfit.q2Total = q2Prof; projectedProfit.q3Total = q3Prof; projectedProfit.q4Total = q4Prof;

    // Sum helpers for FBA (exclude Jan27 from S2 and FY)
    const fySum = (obj) => { let t = 0; MONTHS.slice(0,12).forEach(m => t += (obj[m] || 0)); return t; };
    const s1Sum = (obj) => { let t = 0; MONTHS.slice(0, 6).forEach(m => t += (obj[m] || 0)); return t; };
    const s2Sum = (obj) => { let t = 0; MONTHS.slice(6, 12).forEach(m => t += (obj[m] || 0)); return t; };

    projBegFBA.s1Total = projBegFBA["Jan"] || 0; projBegFBA.s2Total = projBegFBA["Jul"] || 0; projBegFBA.fyTotal = projBegFBA["Jan"] || 0;
    projEndFBA.s1Total = projEndFBA["Jun"] || 0; projEndFBA.s2Total = projEndFBA["Dec"] || 0; projEndFBA.fyTotal = projEndFBA["Dec"] || 0;
    projShipmentsIn.s1Total = s1Sum(projShipmentsIn); projShipmentsIn.s2Total = s2Sum(projShipmentsIn); projShipmentsIn.fyTotal = fySum(projShipmentsIn);
    projSalesOut.s1Total = s1Sum(projSalesOut); projSalesOut.s2Total = s2Sum(projSalesOut); projSalesOut.fyTotal = fySum(projSalesOut);
    projReturnsIn.s1Total = s1Sum(projReturnsIn); projReturnsIn.s2Total = s2Sum(projReturnsIn); projReturnsIn.fyTotal = fySum(projReturnsIn);

    return {
      effectiveUnits, effectiveAUR, effectiveRefunds, effectiveRefundRate,
      forecastedRevenue, projectedProfit,
      projBegFBA, projEndFBA, projShipmentsIn, projSalesOut, projReturnsIn, projWeeksCover,
    };
  }, [plan, ov, localEdits]);

  // ── Edit handlers ──
  function handleCellEdit(overrideKey, month, rawValue) {
    setLocalEdits(prev => ({
      ...prev,
      [overrideKey]: { ...(prev[overrideKey] || {}), [month]: rawValue }
    }));
  }

  async function handleCellBlur(overrideKey, month) {
    const localVal = localEdits[overrideKey]?.[month];
    if (localVal === undefined) return;
    const serverVals = ov[overrideKey] || {};
    const localFieldEdits = localEdits[overrideKey] || {};
    const merged = { ...serverVals, ...localFieldEdits };
    const cleaned = {};
    MONTHS.forEach(m => {
      const v = merged[m];
      if (v != null && v !== "") cleaned[m] = Number(v);
    });
    setSaving(prev => ({ ...prev, [overrideKey + month]: true }));
    try {
      await onOverride(sku, overrideKey, cleaned);
      // Clear local edits for this field after successful save — server data is now authoritative
      setLocalEdits(prev => {
        const next = { ...prev };
        delete next[overrideKey];
        return next;
      });
    } catch (e) {
      console.error("Override save failed:", e);
    }
    setSaving(prev => ({ ...prev, [overrideKey + month]: false }));
  }

  // ── Section rows ──
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
        { label: "TY Projected Revenue", data: projections.forecastedRevenue, cls: "projected-row", fmt: "$" },
        { label: "LY Revenue", data: plan.lyRevenue, cls: "ly-row", fmt: "$" },
        { label: "LLY Revenue", data: plan.llyRevenue, cls: "lly-row", fmt: "$" },
      ]
    },
    {
      title: "AUR ($)",
      rows: [
        { label: "TY AUR Plan", data: plan.tyAUR, cls: "", fmt: "$d", reduce: "avg" },
        { label: "TY AUR Override", data: getRow("tyAUROverride", "aurOverride"), cls: "override-row", editable: "aurOverride", fmt: "$d", reduce: "avg" },
        { label: "LY AUR", data: plan.lyAUR, cls: "ly-row", fmt: "$d", reduce: "avg" },
        { label: "LLY AUR", data: plan.llyAUR, cls: "lly-row", fmt: "$d", reduce: "avg" },
      ]
    },
    {
      title: "PROFITABILITY",
      rows: [
        { label: "TY Plan Gross Profit", data: plan.tyPlanGrossProfit, cls: "", fmt: "$" },
        { label: "Projected Gross Profit", data: projections.projectedProfit, cls: "projected-row", fmt: "$" },
        { label: "LY Gross Profit", data: plan.lyGrossProfit, cls: "ly-row", fmt: "$" },
        { label: "LY Profit Margin %", data: plan.lyProfitMarginPct, cls: "ly-row", fmt: "%", reduce: "avg" },
      ]
    },
    {
      title: "RETURNS / REFUNDS",
      rows: [
        { label: "TY Plan Refund Units", data: plan.tyPlanRefundUnits, cls: "" },
        { label: "TY Override Refund Units", data: getRow("tyOverrideRefundUnits", "refundOverride"), cls: "override-row", editable: "refundOverride" },
        { label: "Refund Rate % Override", data: getRow("tyRefundRateOverridePct", "refundRateOverride"), cls: "override-row pct-row", editable: "refundRateOverride", fmt: "%", reduce: "avg" },
        { label: "Projected Refund Units", data: projections.effectiveRefunds, cls: "projected-row" },
        { label: "Effective Refund Rate %", data: projections.effectiveRefundRate, cls: "projected-row", fmt: "%", reduce: "avg" },
        { label: "LY Refund Units", data: plan.lyRefundUnits, cls: "ly-row" },
        { label: "LY Refund Rate %", data: plan.lyRefundRatePct, cls: "ly-row", fmt: "%", reduce: "avg" },
      ]
    },
    {
      title: "FBA INVENTORY & SHIPMENTS",
      rows: [
        { label: "Beg. FBA Inventory (Plan)", data: plan.beginFBAInventory, cls: "", reduce: "first" },
        { label: "Beg. FBA Inventory (Projected)", data: projections.projBegFBA, cls: "projected-row", reduce: "first" },
        { label: "Recommended Shipment", data: plan.recommendedShipment, cls: "" },
        { label: "Shipment Override", data: getRow("shipmentOverride", "shipmentOverride"), cls: "override-row", editable: "shipmentOverride" },
        { label: "FBA Shipments In (Plan)", data: plan.fbaShipmentsIn, cls: "" },
        { label: "FBA Shipments In (Projected)", data: projections.projShipmentsIn, cls: "projected-row" },
        { label: "Sales Units Out (Plan)", data: plan.salesUnitsOut, cls: "" },
        { label: "Sales Units Out (Projected)", data: projections.projSalesOut, cls: "projected-row" },
        { label: "Returns In (Plan)", data: plan.returnsIn, cls: "" },
        { label: "Returns In (Projected)", data: projections.projReturnsIn, cls: "projected-row" },
        { label: "End FBA Inventory (Plan)", data: plan.endingFBAInventory, cls: "total-row", reduce: "last" },
        { label: "End FBA Inventory (Projected)", data: projections.projEndFBA, cls: "projected-row total-row", reduce: "last" },
        { label: "Weeks of FBA Cover (Projected)", data: projections.projWeeksCover, cls: "projected-row", fmt: "d1", reduce: "last" },
      ]
    },
    {
      title: "WAREHOUSE INVENTORY",
      rows: [
        { label: "Beginning WH Inventory", data: plan.beginWHInventory, cls: "", reduce: "first" },
        { label: "Factory Receipts In", data: plan.factoryReceiptsIn, cls: "" },
        { label: "Shipped to FBA", data: plan.shippedToFBA, cls: "" },
        { label: "Ending WH Inventory", data: plan.endingWHInventory, cls: "total-row", reduce: "last" },
        { label: "WH Months of Supply", data: plan.whMonthsOfSupply, cls: "", fmt: "d1", reduce: "last" },
      ]
    },
    {
      title: "EFFECTIVE PROJECTIONS",
      rows: [
        { label: "Effective Sales Units", data: projections.effectiveUnits, cls: "total-row" },
        { label: "Effective AUR ($)", data: projections.effectiveAUR, cls: "", fmt: "$d", reduce: "avg" },
        { label: "Effective Refund Units", data: projections.effectiveRefunds, cls: "" },
        { label: "Forecasted Revenue $", data: projections.forecastedRevenue, cls: "total-row", fmt: "$" },
        { label: "Projected Gross Profit $", data: projections.projectedProfit, cls: "", fmt: "$" },
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

  // Chart data using projections
  const chartData = MONTHS.map((m, i) => ({
    month: MONTH_LABELS[i],
    tyUnits: projections.effectiveUnits[m] || 0,
    lyUnits: plan.lyUnits?.[m] || 0,
    tyRevenue: projections.forecastedRevenue[m] || 0,
    lyRevenue: plan.lyRevenue?.[m] || 0,
    isActual: isActual(m),
  }));

  return (
    <div className="ip-item-detail">
      {/* Actual/Projected legend */}
      <div className="ip-legend">
        <span className="ip-legend-item"><span className="ip-legend-dot ip-actual-dot" /> Actual (Jan–Feb)</span>
        <span className="ip-legend-item"><span className="ip-legend-dot ip-projected-dot" /> Projected (Mar+)</span>
        <span className="ip-legend-item"><span className="ip-legend-swatch ip-override-swatch" /> Override (editable)</span>
      </div>

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
            <span className="ip-fy-value">{(projections.effectiveUnits.fyTotal || 0).toLocaleString()}</span>
          </div>
          <div className="ip-fy-metric">
            <span className="ip-fy-label">FY Revenue</span>
            <span className="ip-fy-value">{fmt$(projections.forecastedRevenue.fyTotal)}</span>
          </div>
          <div className="ip-fy-metric">
            <span className="ip-fy-label">Avg AUR</span>
            <span className="ip-fy-value">${(projections.effectiveAUR.fyTotal || 0).toFixed(2)}</span>
          </div>
          <div className="ip-fy-metric">
            <span className="ip-fy-label">FY Profit</span>
            <span className="ip-fy-value">{fmt$(projections.projectedProfit.fyTotal)}</span>
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
              <Bar dataKey="tyUnits" name="TY Units" radius={[3,3,0,0]}
                fill="#3E658C"
                shape={(props) => {
                  const { x, y, width, height, payload } = props;
                  const fill = payload.isActual ? "#1A5276" : "#3E658C";
                  const strokeDash = payload.isActual ? "" : "3 2";
                  return <rect x={x} y={y} width={width} height={height} fill={fill} rx={3} ry={3}
                    stroke={payload.isActual ? "none" : "#3E658C55"} strokeDasharray={strokeDash} />;
                }}
              />
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
                    {MONTH_LABELS.map((ml, mi) => (
                      <th key={ml} className={isActual(MONTHS[mi]) ? "ip-th-actual" : "ip-th-projected"}>
                        {ml}
                        {isActual(MONTHS[mi]) && <span className="ip-actual-badge">A</span>}
                      </th>
                    ))}
                    {PERIOD_COLS.map(pc => (
                      <th key={pc.key} className={pc.cls}>{pc.label}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sec.rows.map((row, ri) => {
                    if (!row.data) return null;
                    const isEditable = !!row.editable;
                    return (
                      <tr key={ri} className={row.cls}>
                        <td className="sticky-col metric-col">
                          {row.label}
                          {isEditable && <span className="ip-edit-hint">✏️</span>}
                        </td>
                        {MONTHS.map(m => {
                          const actual = isActual(m);
                          const cellCls = `num${isEditable ? " ip-editable-cell" : ""}${actual ? " ip-actual-cell" : " ip-proj-cell"}`;
                          return (
                            <td key={m} className={cellCls}>
                              {isEditable ? (() => {
                                const isPct = row.fmt === "%";
                                const isDollar = row.fmt === "$d";
                                const rawVal = localEdits[row.editable]?.[m] !== undefined
                                  ? localEdits[row.editable][m]
                                  : (row.data?.[m] != null ? row.data[m] : "");
                                const displayVal = isPct && rawVal !== "" && rawVal != null
                                  ? (Number(rawVal) * 100).toFixed(1).replace(/\.0$/, "")
                                  : rawVal;
                                return (
                                  <div style={{ position: "relative" }}>
                                    {isDollar && <span style={{ position: "absolute", left: 4, top: "50%", transform: "translateY(-50%)", fontSize: 10, color: "#94a3b8", pointerEvents: "none" }}>$</span>}
                                    <input
                                      type="number"
                                      className={`ip-cell-input${actual ? " ip-input-actual" : ""}${isDollar ? " ip-input-dollar" : ""}`}
                                      value={displayVal}
                                      step={isDollar ? "0.01" : isPct ? "0.1" : "1"}
                                      onChange={e => {
                                        const v = e.target.value;
                                        if (isPct) {
                                          handleCellEdit(row.editable, m, v === "" ? "" : (Number(v) / 100));
                                        } else {
                                          handleCellEdit(row.editable, m, v);
                                        }
                                      }}
                                      onBlur={() => handleCellBlur(row.editable, m)}
                                      onKeyDown={e => { if (e.key === "Enter") e.target.blur(); }}
                                      placeholder={isPct ? "0%" : isDollar ? "0.00" : "0"}
                                    />
                                    {isPct && <span style={{ position: "absolute", right: 4, top: "50%", transform: "translateY(-50%)", fontSize: 10, color: "#94a3b8", pointerEvents: "none" }}>%</span>}
                                  </div>
                                );
                              })() : (
                                fmtCell(row.data?.[m], row.fmt)
                              )}
                            </td>
                          );
                        })}
                        {PERIOD_COLS.map(pc => {
                          const pv = periodVal(row.data, pc.months, row.reduce);
                          const isFY = pc.key === "fy";
                          const cellCls = `num ${pc.cls === "fy-total" ? "fy-total" : pc.cls === "ip-th-quarter" ? "ip-quarter-cell" : "ip-half-cell"}`;
                          return (
                            <td key={pc.key} className={cellCls}>
                              {isFY ? <strong>{fmtCell(pv, row.fmt)}</strong> : fmtCell(pv, row.fmt)}
                            </td>
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
