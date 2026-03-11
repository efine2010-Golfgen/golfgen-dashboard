import { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { api, fmt$ } from "../lib/api";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, ComposedChart, Area, Legend
} from "recharts";

/* ═══════════════════════════════════════════════════════════
   FY2026 ITEM PLANNING MODULE
   FY = Feb 2026 – Jan 2027, with Jan 2026 as Pre-FY column
   ═══════════════════════════════════════════════════════════ */

const FY_MONTHS = [
  { key: "jan", label: "Jan '26", fy: false },
  { key: "feb", label: "Feb", fy: true },
  { key: "mar", label: "Mar", fy: true },
  { key: "apr", label: "Apr", fy: true },
  { key: "may", label: "May", fy: true },
  { key: "jun", label: "Jun", fy: true },
  { key: "jul", label: "Jul", fy: true },
  { key: "aug", label: "Aug", fy: true },
  { key: "sep", label: "Sep", fy: true },
  { key: "oct", label: "Oct", fy: true },
  { key: "nov", label: "Nov", fy: true },
  { key: "dec", label: "Dec", fy: true },
  { key: "jan_next", label: "Jan '27", fy: true },
];

const FY_KEYS = FY_MONTHS.map(m => m.key);
const FY_ONLY = FY_MONTHS.filter(m => m.fy).map(m => m.key); // feb..jan_next
const S1_KEYS = ["feb","mar","apr","may","jun","jul"];
const S2_KEYS = ["aug","sep","oct","nov","dec","jan_next"];

const ACTUALIZED_THROUGH = 2; // Feb = month index 2 in FY_MONTHS (0=jan, 1=feb -> index 1 in FY_MONTHS)
// Actually: jan=0, feb=1 in FY_MONTHS array. actualized_through_month=2 means Feb is actualized.
// Map month key to whether it's actualized
function isActualized(monthKey) {
  const idx = FY_KEYS.indexOf(monthKey);
  // Jan '26 (idx 0) and Feb (idx 1) are actualized when actualized_through_month=2
  return idx <= 1; // jan and feb
}

/* ── Formatting helpers ── */
function fmtNum(v) { return v == null ? "0" : Math.round(v).toLocaleString(); }
function fmtDol(v) { return v == null ? "$0" : "$" + Math.round(v).toLocaleString(); }
function fmtDol2(v) { return v == null ? "$0.00" : "$" + Number(v).toFixed(2); }
function fmtPct(v) { return v == null ? "0.0%" : (v * 100).toFixed(1) + "%"; }
function fmtWks(v) { return v == null ? "—" : Number(v).toFixed(1); }

function sumKeys(obj, keys) {
  return keys.reduce((s, k) => s + (Number(obj?.[k]) || 0), 0);
}
function avgKeys(obj, keys) {
  const vals = keys.map(k => Number(obj?.[k]) || 0).filter(v => v !== 0);
  return vals.length ? vals.reduce((s,v) => s+v, 0) / vals.length : 0;
}

/* ═══════════════════════════════════════════════════════════
   CALCULATION ENGINE — runs entirely client-side
   ═══════════════════════════════════════════════════════════ */
function calcSKU(sku, overrides, curves, factoryOrders) {
  const ov = overrides?.[sku.sku] || {};
  const curveType = sku.curve || "LY";
  const curveData = curveType === "MASTER" ? curves?.master : (curves?.bySku?.[sku.sku] || curves?.master);

  const result = { sku: sku.sku, asin: sku.asin, product_name: sku.product_name, curve: curveType };

  // ── 1. SALES UNITS ──
  const tyPlanAnnual = sku.ty_plan_annual || sku.ty_plan_units?.fy_total || 0;
  const tyPlan = {};
  const tyOverride = {};
  const tyEffective = {};

  FY_KEYS.forEach(m => {
    // Curve proration: TY Plan = Annual × (LY month / LY total)
    const lyTotal = sumKeys(sku.ly_units, FY_ONLY);
    const lyMonth = Number(sku.ly_units?.[m]) || 0;
    const ratio = lyTotal > 0 ? lyMonth / lyTotal : 0;
    tyPlan[m] = curveData?.[m] != null ? tyPlanAnnual * (Number(curveData[m]) || 0) : Math.round(tyPlanAnnual * ratio);

    // Override
    const ovVal = ov.ty_override_units?.[m];
    tyOverride[m] = ovVal != null ? Number(ovVal) : null;
    tyEffective[m] = tyOverride[m] != null ? tyOverride[m] : tyPlan[m];
  });

  result.ty_plan_units = tyPlan;
  result.ty_override_units = tyOverride;
  result.ty_effective_units = tyEffective;
  result.ly_units = sku.ly_units || {};
  result.lly_units = sku.lly_units || {};

  // ── 2. REVENUE ──
  const tyAurPlan = {};
  const tyAurOverride = {};
  const tyAurEffective = {};
  const tyRevenue = {};
  const lyRevenue = sku.ly_revenue || {};
  const llyRevenue = sku.lly_revenue || {};

  FY_KEYS.forEach(m => {
    tyAurPlan[m] = Number(sku.ty_aur?.[m]) || 0;
    const aurOv = ov.ty_aur_override?.[m];
    tyAurOverride[m] = aurOv != null ? Number(aurOv) : null;
    tyAurEffective[m] = tyAurOverride[m] != null ? tyAurOverride[m] : tyAurPlan[m];
    tyRevenue[m] = tyEffective[m] * tyAurEffective[m];
  });

  result.ty_aur_plan = tyAurPlan;
  result.ty_aur_override = tyAurOverride;
  result.ty_aur_effective = tyAurEffective;
  result.ty_revenue = tyRevenue;
  result.ly_revenue = lyRevenue;
  result.lly_revenue = llyRevenue;
  result.ly_aur = sku.ly_aur || {};

  // ── 3. PROFITABILITY ──
  const lyGP = sku.ly_gross_profit || {};
  const llyGP = sku.lly_gross_profit || {};
  const lyMargin = {};
  const tyGP = {};

  // Estimate COGS per unit from LY data
  const lyRevTotal = sumKeys(lyRevenue, FY_ONLY);
  const lyGPTotal = sumKeys(lyGP, FY_ONLY);
  const lyUnitsTotal = sumKeys(sku.ly_units, FY_ONLY);
  const cogsPerUnit = lyUnitsTotal > 0 ? (lyRevTotal - lyGPTotal) / lyUnitsTotal : 0;

  FY_KEYS.forEach(m => {
    const lr = Number(lyRevenue[m]) || 0;
    const lg = Number(lyGP[m]) || 0;
    lyMargin[m] = lr > 0 ? lg / lr : 0;
    tyGP[m] = tyRevenue[m] - (tyEffective[m] * cogsPerUnit);
  });

  result.ty_gross_profit = tyGP;
  result.ly_gross_profit = lyGP;
  result.lly_gross_profit = llyGP;
  result.ly_profit_margin = lyMargin;

  // ── 4. REFUNDS ──
  const tyRefundOverride = {};
  const tyRefundRateOverride = {};
  const tyRefundEffective = {};
  const tyRefundRateEffective = {};
  const lyRefunds = sku.ly_refund_units || {};
  const llyRefunds = sku.lly_refund_units || {};

  FY_KEYS.forEach(m => {
    const lyRate = Number(sku.ly_refund_rate?.[m]) || 0;
    const refOv = ov.ty_override_refund_units?.[m];
    const rateOv = ov.ty_refund_rate_override?.[m];

    tyRefundOverride[m] = refOv != null ? Number(refOv) : null;
    tyRefundRateOverride[m] = rateOv != null ? Number(rateOv) : null;

    if (tyRefundOverride[m] != null) {
      tyRefundEffective[m] = tyRefundOverride[m];
    } else if (tyRefundRateOverride[m] != null) {
      tyRefundEffective[m] = Math.round(tyEffective[m] * tyRefundRateOverride[m]);
    } else {
      tyRefundEffective[m] = Math.round(tyEffective[m] * lyRate);
    }
    tyRefundRateEffective[m] = tyEffective[m] > 0 ? tyRefundEffective[m] / tyEffective[m] : 0;
  });

  result.ty_refund_override = tyRefundOverride;
  result.ty_refund_rate_override = tyRefundRateOverride;
  result.ty_refund_effective = tyRefundEffective;
  result.ty_refund_rate_effective = tyRefundRateEffective;
  result.ly_refund_units = lyRefunds;
  result.lly_refund_units = llyRefunds;
  result.ly_refund_rate = sku.ly_refund_rate || {};

  // ── 5. FBA INVENTORY (rolling balance) ──
  const fbaBeg = {};
  const fbaShipIn = {};
  const fbaSalesOut = {};
  const fbaReturnsIn = {};
  const fbaEnd = {};
  const fbaWeeksCover = {};

  let prevEnd = Number(sku.fba_beginning) || 0;
  FY_KEYS.forEach(m => {
    fbaBeg[m] = prevEnd;
    const shipOv = ov.shipment_override?.[m];
    fbaShipIn[m] = shipOv != null ? Number(shipOv) : 0;
    fbaSalesOut[m] = tyEffective[m];
    fbaReturnsIn[m] = tyRefundEffective[m];
    fbaEnd[m] = fbaBeg[m] + fbaShipIn[m] - fbaSalesOut[m] + fbaReturnsIn[m];
    const weeklySales = tyEffective[m] / 4.33;
    fbaWeeksCover[m] = weeklySales > 0 ? fbaEnd[m] / weeklySales : 0;
    prevEnd = fbaEnd[m];
  });

  result.fba_beg = fbaBeg;
  result.fba_ship_in = fbaShipIn;
  result.fba_sales_out = fbaSalesOut;
  result.fba_returns_in = fbaReturnsIn;
  result.fba_end = fbaEnd;
  result.fba_weeks_cover = fbaWeeksCover;
  result.shipment_override = ov.shipment_override || {};

  // ── 6. WAREHOUSE INVENTORY (rolling balance) ──
  const whBeg = {};
  const whFactoryIn = {};
  const whShipFBA = {};
  const whEnd = {};

  // Factory orders for this SKU
  const skuOrders = (factoryOrders || []).filter(o => o.sku === sku.sku);
  const factoryByMonth = {};
  FY_KEYS.forEach(m => { factoryByMonth[m] = 0; });
  skuOrders.forEach(o => {
    if (o.est_arrival_month && factoryByMonth[o.est_arrival_month] != null) {
      factoryByMonth[o.est_arrival_month] += (o.units || 0);
    }
  });

  let whPrev = Number(sku.wh_beginning) || 0;
  FY_KEYS.forEach(m => {
    whBeg[m] = whPrev;
    whFactoryIn[m] = Number(sku.factory_receipts?.[m]) || factoryByMonth[m] || 0;
    whShipFBA[m] = fbaShipIn[m];
    whEnd[m] = whBeg[m] + whFactoryIn[m] - whShipFBA[m];
    whPrev = whEnd[m];
  });

  result.wh_beg = whBeg;
  result.wh_factory_in = whFactoryIn;
  result.wh_ship_fba = whShipFBA;
  result.wh_end = whEnd;

  // ── Totals ──
  result.fy_units = sumKeys(tyEffective, FY_ONLY);
  result.fy_revenue = sumKeys(tyRevenue, FY_ONLY);
  result.fy_gp = sumKeys(tyGP, FY_ONLY);
  result.fy_aur = result.fy_units > 0 ? result.fy_revenue / result.fy_units : 0;
  result.ly_fy_units = sumKeys(sku.ly_units, FY_ONLY);
  result.ly_fy_revenue = sumKeys(lyRevenue, FY_ONLY);

  return result;
}

function calcPortfolio(skuResults) {
  const portfolio = {};
  FY_KEYS.forEach(m => {
    portfolio[m] = {
      units: 0, revenue: 0, gp: 0, ly_units: 0, ly_revenue: 0,
      refunds: 0, ly_refunds: 0,
    };
  });
  skuResults.forEach(r => {
    FY_KEYS.forEach(m => {
      portfolio[m].units += r.ty_effective_units[m] || 0;
      portfolio[m].revenue += r.ty_revenue[m] || 0;
      portfolio[m].gp += r.ty_gross_profit[m] || 0;
      portfolio[m].ly_units += Number(r.ly_units[m]) || 0;
      portfolio[m].ly_revenue += Number(r.ly_revenue[m]) || 0;
      portfolio[m].refunds += r.ty_refund_effective[m] || 0;
      portfolio[m].ly_refunds += Number(r.ly_refund_units[m]) || 0;
    });
  });
  const totalUnits = sumKeys(Object.fromEntries(FY_ONLY.map(k=>[k, portfolio[k].units])), FY_ONLY);
  const totalRev = sumKeys(Object.fromEntries(FY_ONLY.map(k=>[k, portfolio[k].revenue])), FY_ONLY);
  const totalGP = sumKeys(Object.fromEntries(FY_ONLY.map(k=>[k, portfolio[k].gp])), FY_ONLY);
  const lyUnits = sumKeys(Object.fromEntries(FY_ONLY.map(k=>[k, portfolio[k].ly_units])), FY_ONLY);
  const lyRev = sumKeys(Object.fromEntries(FY_ONLY.map(k=>[k, portfolio[k].ly_revenue])), FY_ONLY);
  return { monthly: portfolio, totalUnits, totalRev, totalGP, lyUnits, lyRev,
    aur: totalUnits > 0 ? totalRev / totalUnits : 0,
    margin: totalRev > 0 ? totalGP / totalRev : 0,
  };
}

/* ═══════════════════════════════════════════════════════════
   EDITABLE CELL COMPONENT
   ═══════════════════════════════════════════════════════════ */
function EditableCell({ value, overrideValue, onSave, onClear, disabled, fmt, monthKey }) {
  const [editing, setEditing] = useState(false);
  const [editVal, setEditVal] = useState("");
  const inputRef = useRef();
  const isOverridden = overrideValue != null;
  const actual = isActualized(monthKey);

  const displayValue = isOverridden ? overrideValue : value;

  const formatDisplay = (v) => {
    if (v == null || v === "") return "—";
    if (fmt === "$") return fmtDol(v);
    if (fmt === "$2") return fmtDol2(v);
    if (fmt === "%") return fmtPct(v);
    return fmtNum(v);
  };

  const startEdit = () => {
    if (disabled || actual) return;
    setEditing(true);
    const raw = isOverridden ? overrideValue : (value || "");
    if (fmt === "%") setEditVal(raw != null ? (Number(raw) * 100).toFixed(1) : "");
    else setEditVal(raw != null ? String(raw) : "");
    setTimeout(() => inputRef.current?.select(), 0);
  };

  const commitEdit = () => {
    setEditing(false);
    if (editVal === "" || editVal === null) {
      if (isOverridden) onClear();
      return;
    }
    let numVal = Number(editVal);
    if (fmt === "%") numVal = numVal / 100;
    if (isNaN(numVal)) return;
    onSave(numVal);
  };

  if (editing) {
    return (
      <input ref={inputRef} type="number" className="ip-cell-input"
        value={editVal}
        step={fmt === "$2" ? "0.01" : fmt === "%" ? "0.1" : "1"}
        onChange={e => setEditVal(e.target.value)}
        onBlur={commitEdit}
        onKeyDown={e => { if (e.key === "Enter") e.target.blur(); if (e.key === "Escape") setEditing(false); }}
        autoFocus
      />
    );
  }

  return (
    <div className={`ip-cell-display${isOverridden ? " ip-override" : ""}${actual ? " ip-actual" : ""}${!disabled && !actual ? " ip-editable" : ""}`}
      onDoubleClick={startEdit}>
      <span>{formatDisplay(displayValue)}</span>
      {isOverridden && !actual && (
        <button className="ip-clear-btn" onClick={e => { e.stopPropagation(); onClear(); }} title="Clear override">&times;</button>
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════
   SKU ACCORDION PANEL
   ═══════════════════════════════════════════════════════════ */
function SKUPanel({ skuResult, rawSku, overrides, onOverride, onClearOverride, onCurveChange, curves }) {
  const [open, setOpen] = useState(false);
  const [openSections, setOpenSections] = useState({ sales: true, revenue: false, aur: false, profit: false, refunds: false, fba: false, warehouse: false, effective: false });
  const r = skuResult;

  const toggleSection = (key) => setOpenSections(prev => ({ ...prev, [key]: !prev[key] }));

  const unitsDelta = r.ly_fy_units > 0 ? ((r.fy_units - r.ly_fy_units) / r.ly_fy_units * 100).toFixed(1) : "—";
  const revDelta = r.ly_fy_revenue > 0 ? ((r.fy_revenue - r.ly_fy_revenue) / r.ly_fy_revenue * 100).toFixed(1) : "—";

  // Row renderer for data sections
  const renderRow = (label, data, opts = {}) => {
    const { fmt: cellFmt, editable, overrideField, isProjected, isBold } = opts;
    return (
      <tr key={label} className={`${isProjected ? "ip-row-projected" : ""} ${isBold ? "ip-row-bold" : ""}`}>
        <td className="ip-metric-col">{label}</td>
        {FY_MONTHS.map(m => {
          const val = data?.[m.key];
          const fmtFn = cellFmt === "$" ? fmtDol : cellFmt === "$2" ? fmtDol2 : cellFmt === "%" ? fmtPct : cellFmt === "wk" ? fmtWks : fmtNum;

          if (editable) {
            const ov = overrides?.[r.sku]?.[overrideField];
            const ovVal = ov?.[m.key] ?? null;
            return (
              <td key={m.key} className={`ip-data-cell${isActualized(m.key) ? " ip-actual-bg" : ""}`}>
                <EditableCell
                  value={val} overrideValue={ovVal} fmt={cellFmt} monthKey={m.key}
                  disabled={false}
                  onSave={v => onOverride(r.sku, overrideField, m.key, v)}
                  onClear={() => onClearOverride(r.sku, overrideField, m.key)}
                />
              </td>
            );
          }

          return (
            <td key={m.key} className={`ip-data-cell${isActualized(m.key) ? " ip-actual-bg" : ""}`}>
              <div className={`ip-cell-display${isActualized(m.key) ? " ip-actual" : ""}`}>
                {fmtFn(val)}
              </div>
            </td>
          );
        })}
        <td className="ip-data-cell ip-total-cell">
          <strong>{
            cellFmt === "$" ? fmtDol(sumKeys(data, FY_ONLY)) :
            cellFmt === "$2" ? fmtDol2(avgKeys(data, FY_ONLY)) :
            cellFmt === "%" ? fmtPct(avgKeys(data, FY_ONLY)) :
            cellFmt === "wk" ? fmtWks(data?.[FY_ONLY[FY_ONLY.length-1]]) :
            fmtNum(sumKeys(data, FY_ONLY))
          }</strong>
        </td>
      </tr>
    );
  };

  const sections = [
    { key: "sales", title: "Sales Units", rows: [
      { label: "TY Plan Units", data: r.ty_plan_units },
      { label: "TY Override Units", data: r.ty_override_units, editable: true, overrideField: "ty_override_units" },
      { label: "TY Effective Units", data: r.ty_effective_units, isBold: true, isProjected: true },
      { label: "LY Units", data: r.ly_units },
      { label: "LLY Units", data: r.lly_units },
    ]},
    { key: "revenue", title: "Revenue ($)", rows: [
      { label: "TY Revenue", data: r.ty_revenue, fmt: "$", isProjected: true, isBold: true },
      { label: "LY Revenue", data: r.ly_revenue, fmt: "$" },
      { label: "LLY Revenue", data: r.lly_revenue, fmt: "$" },
    ]},
    { key: "aur", title: "AUR ($)", rows: [
      { label: "TY AUR Plan", data: r.ty_aur_plan, fmt: "$2" },
      { label: "TY AUR Override", data: r.ty_aur_override, fmt: "$2", editable: true, overrideField: "ty_aur_override" },
      { label: "TY AUR Effective", data: r.ty_aur_effective, fmt: "$2", isProjected: true, isBold: true },
      { label: "LY AUR", data: r.ly_aur, fmt: "$2" },
    ]},
    { key: "profit", title: "Profitability", rows: [
      { label: "TY Gross Profit", data: r.ty_gross_profit, fmt: "$", isProjected: true, isBold: true },
      { label: "LY Gross Profit", data: r.ly_gross_profit, fmt: "$" },
      { label: "LLY Gross Profit", data: r.lly_gross_profit, fmt: "$" },
      { label: "LY Profit Margin", data: r.ly_profit_margin, fmt: "%" },
    ]},
    { key: "refunds", title: "Refunds", rows: [
      { label: "Refund Units Override", data: r.ty_refund_override, editable: true, overrideField: "ty_override_refund_units" },
      { label: "Refund Rate Override", data: r.ty_refund_rate_override, fmt: "%", editable: true, overrideField: "ty_refund_rate_override" },
      { label: "Effective Refund Units", data: r.ty_refund_effective, isProjected: true, isBold: true },
      { label: "Effective Refund Rate", data: r.ty_refund_rate_effective, fmt: "%", isProjected: true },
      { label: "LY Refund Units", data: r.ly_refund_units },
      { label: "LY Refund Rate", data: r.ly_refund_rate, fmt: "%" },
    ]},
    { key: "fba", title: "FBA Inventory", rows: [
      { label: "Beginning FBA", data: r.fba_beg },
      { label: "Shipments In", data: r.fba_ship_in, editable: true, overrideField: "shipment_override" },
      { label: "Sales Out", data: r.fba_sales_out, isProjected: true },
      { label: "Returns In", data: r.fba_returns_in, isProjected: true },
      { label: "Ending FBA", data: r.fba_end, isBold: true, isProjected: true },
      { label: "Weeks of Cover", data: r.fba_weeks_cover, fmt: "wk", isProjected: true },
    ]},
    { key: "warehouse", title: "Warehouse Inventory", rows: [
      { label: "Beginning WH", data: r.wh_beg },
      { label: "Factory Receipts", data: r.wh_factory_in },
      { label: "Shipped to FBA", data: r.wh_ship_fba },
      { label: "Ending WH", data: r.wh_end, isBold: true },
    ]},
    { key: "effective", title: "Effective Projections", rows: [
      { label: "Effective Units", data: r.ty_effective_units, isBold: true, isProjected: true },
      { label: "Effective AUR", data: r.ty_aur_effective, fmt: "$2", isProjected: true },
      { label: "Effective Revenue", data: r.ty_revenue, fmt: "$", isBold: true, isProjected: true },
      { label: "Effective GP", data: r.ty_gross_profit, fmt: "$", isProjected: true },
      { label: "Effective Refunds", data: r.ty_refund_effective, isProjected: true },
    ]},
  ];

  return (
    <div className={`ip-sku-panel${open ? " ip-sku-open" : ""}`}>
      {/* Accordion header */}
      <div className="ip-sku-header" onClick={() => setOpen(!open)}>
        <div className="ip-sku-toggle">{open ? "▼" : "▶"}</div>
        <div className="ip-sku-name">{r.product_name}</div>
        <div className="ip-sku-meta">
          <span className="ip-sku-id">{r.sku}</span>
          <span className="ip-sku-kpi">{fmtNum(r.fy_units)} units</span>
          <span className="ip-sku-kpi">{fmtDol(r.fy_revenue)}</span>
          <span className={`ip-sku-delta ${Number(unitsDelta) > 0 ? "pos" : Number(unitsDelta) < 0 ? "neg" : ""}`}>
            {unitsDelta !== "—" ? `${Number(unitsDelta) > 0 ? "+" : ""}${unitsDelta}% vs LY` : "—"}
          </span>
        </div>
        <div className="ip-curve-toggle" onClick={e => e.stopPropagation()}>
          <label>Curve:</label>
          <select value={r.curve} onChange={e => onCurveChange(r.sku, e.target.value)}>
            <option value="LY">LY</option>
            <option value="MASTER">MASTER</option>
          </select>
        </div>
      </div>

      {/* Accordion body */}
      {open && (
        <div className="ip-sku-body">
          {sections.map(sec => (
            <div key={sec.key} className="ip-section-block">
              <div className="ip-section-header" onClick={() => toggleSection(sec.key)}>
                <span>{openSections[sec.key] ? "▾" : "▸"}</span> {sec.title}
              </div>
              {openSections[sec.key] && (
                <div className="ip-table-wrap">
                  <table className="ip-table">
                    <thead>
                      <tr>
                        <th className="ip-metric-col">Metric</th>
                        {FY_MONTHS.map(m => (
                          <th key={m.key} className={isActualized(m.key) ? "ip-th-actual" : ""}>
                            {m.label}
                            {isActualized(m.key) && <span className="ip-a-badge">A</span>}
                          </th>
                        ))}
                        <th className="ip-total-cell">FY Total</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sec.rows.map(row => renderRow(row.label, row.data, row))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════
   PORTFOLIO SUMMARY COMPONENT
   ═══════════════════════════════════════════════════════════ */
function PortfolioSummary({ portfolio, skuResults }) {
  const p = portfolio;
  const unitsPctVsLY = p.lyUnits > 0 ? ((p.totalUnits - p.lyUnits) / p.lyUnits * 100).toFixed(1) : "—";
  const revPctVsLY = p.lyRev > 0 ? ((p.totalRev - p.lyRev) / p.lyRev * 100).toFixed(1) : "—";

  const chartData = FY_ONLY.map(k => {
    const m = FY_MONTHS.find(fm => fm.key === k);
    return {
      month: m?.label || k,
      tyUnits: p.monthly[k]?.units || 0,
      lyUnits: p.monthly[k]?.ly_units || 0,
      tyRevenue: p.monthly[k]?.revenue || 0,
      lyRevenue: p.monthly[k]?.ly_revenue || 0,
      actual: isActualized(k),
    };
  });

  return (
    <div className="ip-portfolio">
      {/* KPI Cards */}
      <div className="ip-kpi-grid">
        <div className="ip-kpi-card">
          <div className="ip-kpi-label">FY Units</div>
          <div className="ip-kpi-value">{fmtNum(p.totalUnits)}</div>
          <div className={`ip-kpi-delta ${Number(unitsPctVsLY) > 0 ? "pos" : Number(unitsPctVsLY) < 0 ? "neg" : ""}`}>
            {unitsPctVsLY !== "—" ? `${Number(unitsPctVsLY) > 0 ? "+" : ""}${unitsPctVsLY}% vs LY` : "—"}
          </div>
        </div>
        <div className="ip-kpi-card">
          <div className="ip-kpi-label">FY Revenue</div>
          <div className="ip-kpi-value">{fmtDol(p.totalRev)}</div>
          <div className={`ip-kpi-delta ${Number(revPctVsLY) > 0 ? "pos" : Number(revPctVsLY) < 0 ? "neg" : ""}`}>
            {revPctVsLY !== "—" ? `${Number(revPctVsLY) > 0 ? "+" : ""}${revPctVsLY}% vs LY` : "—"}
          </div>
        </div>
        <div className="ip-kpi-card">
          <div className="ip-kpi-label">Avg AUR</div>
          <div className="ip-kpi-value">{fmtDol2(p.aur)}</div>
        </div>
        <div className="ip-kpi-card">
          <div className="ip-kpi-label">FY Gross Profit</div>
          <div className="ip-kpi-value">{fmtDol(p.totalGP)}</div>
          <div className="ip-kpi-delta">{fmtPct(p.margin)} margin</div>
        </div>
      </div>

      {/* Charts */}
      <div className="ip-charts-row">
        <div className="ip-chart-card">
          <h3>Monthly Units: TY vs LY</h3>
          <ResponsiveContainer width="100%" height={240}>
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" fontSize={11} />
              <YAxis fontSize={11} />
              <Tooltip />
              <Bar dataKey="tyUnits" name="TY Plan" fill="#3E658C" radius={[3,3,0,0]} />
              <Line type="monotone" dataKey="lyUnits" name="LY Actual" stroke="#E87830" strokeWidth={2} dot={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
        <div className="ip-chart-card">
          <h3>Monthly Revenue: TY vs LY</h3>
          <ResponsiveContainer width="100%" height={240}>
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis dataKey="month" fontSize={11} />
              <YAxis fontSize={11} tickFormatter={v => `$${(v/1000).toFixed(0)}k`} />
              <Tooltip formatter={v => fmtDol(v)} />
              <Area type="monotone" dataKey="tyRevenue" name="TY Plan" fill="#3E658C22" stroke="#3E658C" strokeWidth={2} />
              <Line type="monotone" dataKey="lyRevenue" name="LY Actual" stroke="#E87830" strokeWidth={2} dot={false} strokeDasharray="5 5" />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Summary table */}
      <div className="ip-table-wrap">
        <table className="ip-table">
          <thead>
            <tr>
              <th className="ip-metric-col">Metric</th>
              {FY_MONTHS.map(m => (
                <th key={m.key} className={isActualized(m.key) ? "ip-th-actual" : ""}>
                  {m.label}
                </th>
              ))}
              <th className="ip-total-cell">FY Total</th>
            </tr>
          </thead>
          <tbody>
            <tr className="ip-row-bold">
              <td className="ip-metric-col">TY Units</td>
              {FY_MONTHS.map(m => (
                <td key={m.key} className={`ip-data-cell${isActualized(m.key) ? " ip-actual-bg" : ""}`}>
                  {fmtNum(p.monthly[m.key]?.units)}
                </td>
              ))}
              <td className="ip-data-cell ip-total-cell"><strong>{fmtNum(p.totalUnits)}</strong></td>
            </tr>
            <tr>
              <td className="ip-metric-col">LY Units</td>
              {FY_MONTHS.map(m => (
                <td key={m.key} className="ip-data-cell">{fmtNum(p.monthly[m.key]?.ly_units)}</td>
              ))}
              <td className="ip-data-cell ip-total-cell">{fmtNum(p.lyUnits)}</td>
            </tr>
            <tr className="ip-row-bold">
              <td className="ip-metric-col">TY Revenue</td>
              {FY_MONTHS.map(m => (
                <td key={m.key} className={`ip-data-cell${isActualized(m.key) ? " ip-actual-bg" : ""}`}>
                  {fmtDol(p.monthly[m.key]?.revenue)}
                </td>
              ))}
              <td className="ip-data-cell ip-total-cell"><strong>{fmtDol(p.totalRev)}</strong></td>
            </tr>
            <tr>
              <td className="ip-metric-col">LY Revenue</td>
              {FY_MONTHS.map(m => (
                <td key={m.key} className="ip-data-cell">{fmtDol(p.monthly[m.key]?.ly_revenue)}</td>
              ))}
              <td className="ip-data-cell ip-total-cell">{fmtDol(p.lyRev)}</td>
            </tr>
            <tr className="ip-row-bold">
              <td className="ip-metric-col">TY Gross Profit</td>
              {FY_MONTHS.map(m => (
                <td key={m.key} className={`ip-data-cell${isActualized(m.key) ? " ip-actual-bg" : ""}`}>
                  {fmtDol(p.monthly[m.key]?.gp)}
                </td>
              ))}
              <td className="ip-data-cell ip-total-cell"><strong>{fmtDol(p.totalGP)}</strong></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════
   MAIN ITEM PLANNING PAGE
   ═══════════════════════════════════════════════════════════ */
export default function ItemPlanning() {
  const [data, setData] = useState(null);
  const [curves, setCurves] = useState(null);
  const [factoryOrders, setFactoryOrders] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState("");
  const [saveStatus, setSaveStatus] = useState(""); // "", "saving", "saved", "error"
  const saveTimer = useRef(null);
  const pendingOverrides = useRef({});

  // Load all data
  useEffect(() => {
    Promise.all([
      api.itemPlan(),
      api.itemPlanSalesCurves(),
      api.factoryOnOrder(),
    ]).then(([planData, curveData, foData]) => {
      setData(planData);
      setCurves(curveData);
      setFactoryOrders(foData.items || []);
      setLoading(false);
    }).catch(e => {
      setError(e.message);
      setLoading(false);
    });
  }, []);

  // Debounced save (500ms)
  const flushOverrides = useCallback(async () => {
    const pending = { ...pendingOverrides.current };
    pendingOverrides.current = {};
    const entries = Object.entries(pending);
    if (entries.length === 0) return;

    setSaveStatus("saving");
    try {
      for (const [key, { sku, field, month, value }] of entries) {
        await api.itemPlanOverride(sku, field, month, value);
      }
      // Refresh data
      const fresh = await api.itemPlan();
      setData(fresh);
      setSaveStatus("saved");
      setTimeout(() => setSaveStatus(""), 2000);
    } catch (e) {
      console.error("Save failed:", e);
      setSaveStatus("error");
      setTimeout(() => setSaveStatus(""), 3000);
    }
  }, []);

  const scheduleFlush = useCallback(() => {
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(flushOverrides, 500);
  }, [flushOverrides]);

  const handleOverride = useCallback((sku, field, month, value) => {
    // Optimistic local update
    setData(prev => {
      if (!prev) return prev;
      const newOv = { ...prev.overrides };
      if (!newOv[sku]) newOv[sku] = {};
      if (!newOv[sku][field]) newOv[sku][field] = {};
      newOv[sku][field][month] = value;
      return { ...prev, overrides: newOv };
    });
    // Queue for save
    const key = `${sku}:${field}:${month}`;
    pendingOverrides.current[key] = { sku, field, month, value };
    scheduleFlush();
  }, [scheduleFlush]);

  const handleClearOverride = useCallback((sku, field, month) => {
    // Optimistic local update
    setData(prev => {
      if (!prev) return prev;
      const newOv = { ...prev.overrides };
      if (newOv[sku]?.[field]) {
        const copy = { ...newOv[sku][field] };
        delete copy[month];
        newOv[sku] = { ...newOv[sku], [field]: copy };
      }
      return { ...prev, overrides: newOv };
    });
    // Save null to clear
    const key = `${sku}:${field}:${month}`;
    pendingOverrides.current[key] = { sku, field, month, value: null };
    scheduleFlush();
  }, [scheduleFlush]);

  const handleCurveChange = useCallback(async (sku, curveType) => {
    try {
      await api.itemPlanCurve(sku, curveType);
      const fresh = await api.itemPlan();
      setData(fresh);
    } catch (e) {
      console.error("Curve change failed:", e);
    }
  }, []);

  // Calculate all SKU results
  const { skuResults, portfolio } = useMemo(() => {
    if (!data?.skus) return { skuResults: [], portfolio: null };
    const results = data.skus.map(sku =>
      calcSKU(sku, data.overrides, curves, factoryOrders)
    );
    const port = calcPortfolio(results);
    return { skuResults: results, portfolio: port };
  }, [data, curves, factoryOrders]);

  // Filtered SKUs
  const filtered = useMemo(() => {
    if (!search) return skuResults;
    const q = search.toLowerCase();
    return skuResults.filter(r =>
      r.product_name?.toLowerCase().includes(q) || r.sku?.toLowerCase().includes(q) || r.asin?.toLowerCase().includes(q)
    );
  }, [skuResults, search]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading Item Planning...</div>;
  if (error) return <div className="section-card" style={{ padding: 32, textAlign: "center", color: "#c00" }}>Error: {error}</div>;
  if (!data) return null;

  return (
    <div className="ip-page">
      {/* Page Header */}
      <div className="ip-page-header">
        <div>
          <h1>Item Planning</h1>
          <p className="ip-subtitle">FY 2026 Amazon Ladder Plan (Feb '26 – Jan '27)</p>
        </div>
        <div className="ip-header-actions">
          {saveStatus && (
            <span className={`ip-save-status ip-save-${saveStatus}`}>
              {saveStatus === "saving" ? "Saving..." : saveStatus === "saved" ? "Saved" : "Save error"}
            </span>
          )}
          <span className="ip-sku-count">{data.skus?.length || 0} SKUs</span>
        </div>
      </div>

      {/* Portfolio Summary */}
      {portfolio && <PortfolioSummary portfolio={portfolio} skuResults={skuResults} />}

      {/* SKU Search & List */}
      <div className="ip-sku-section">
        <div className="ip-search-bar">
          <input type="text" className="ip-search-input" placeholder="Search SKUs by name, SKU, or ASIN..."
            value={search} onChange={e => setSearch(e.target.value)} />
          <span className="ip-search-count">{filtered.length} of {skuResults.length} items</span>
        </div>

        <div className="ip-legend-bar">
          <span className="ip-legend-item"><span className="ip-legend-dot" style={{background:"#1A5276"}} /> Actualized (Jan–Feb)</span>
          <span className="ip-legend-item"><span className="ip-legend-dot" style={{background:"#3E658C"}} /> Projected (Mar+)</span>
          <span className="ip-legend-item"><span className="ip-legend-swatch" style={{background:"#FFFBE6",border:"2px solid #F5B731"}} /> Override</span>
          <span className="ip-legend-item">Double-click to edit projected cells</span>
        </div>

        {filtered.map(r => (
          <SKUPanel key={r.sku} skuResult={r} rawSku={data.skus.find(s => s.sku === r.sku)}
            overrides={data.overrides} onOverride={handleOverride} onClearOverride={handleClearOverride}
            onCurveChange={handleCurveChange} curves={curves} />
        ))}
      </div>
    </div>
  );
}
