import { useState, useEffect, useMemo, useCallback } from "react";
import { api } from "../lib/api";

const INV_VIEWS = [
  { key: "golfgen-inventory", path: "/golfgen-inventory", label: "GolfGen Inventory" },
  { key: "inventory",         path: "/inventory",         label: "Amazon Inventory" },
  { key: "fba-shipments",     path: "/fba-shipments",     label: "Shipments to FBA" },
  { key: "stranded",          path: "/stranded",          label: "Stranded & Suppressed" },
  { key: "inventory-ledger",  path: "/inventory-ledger",  label: "Inventory Ledger" },
];

/* ── tiny inline-style helpers ── */
const SG = (extra = {}) => ({ fontFamily: "'Space Grotesk',monospace", ...extra });
const DM = (extra = {}) => ({ fontFamily: "'DM Serif Display',Georgia,serif", ...extra });

const fmtK = (n) => {
  if (n == null) return "—";
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1000) return `${(n / 1000).toFixed(1)}K`;
  return n.toLocaleString();
};
const fmtPct = (n) => (n == null ? "—" : `${n}%`);
const fmtNum = (n) => (n == null ? "—" : Number(n).toLocaleString());

/* ── Inline SVG chart builders ── */
function VelChart({ data }) {
  if (!data || data.length < 2) return <div style={{ padding: 20, color: "var(--txt3)", ...SG() }}>No velocity data yet</div>;
  const maxU = Math.max(...data.map(d => d.units), 1);
  const w = 800, h = 140, padB = 20, padL = 30, chartH = h - padB;
  const chartW = w - padL;
  const pts = data.map((d, i) => `${padL + (i / (data.length - 1)) * chartW},${chartH - (d.units / maxU) * (chartH - 10)}`).join(" ");
  const avg = data.reduce((s, d) => s + d.units, 0) / data.length;
  const avgY = chartH - (avg / maxU) * (chartH - 10);
  // Y-axis labels
  const yLabels = [0, Math.round(maxU / 2), Math.round(maxU)];
  // X-axis: show ~5 date labels evenly spaced
  const xStep = Math.max(1, Math.floor(data.length / 5));
  return (
    <div style={{ padding: "12px 16px" }}>
      <svg width="100%" viewBox={`0 0 ${w} ${h}`} style={{ display: "block" }}>
        {yLabels.map((v, i) => {
          const y = chartH - (v / maxU) * (chartH - 10);
          return <g key={i}><line x1={padL} y1={y} x2={w} y2={y} stroke="var(--brd)" strokeWidth="0.5" opacity=".4" /><text x={padL - 4} y={y + 3} textAnchor="end" fontSize="7" fill="var(--txt3)">{v}</text></g>;
        })}
        <polyline points={pts} fill="none" stroke="#2ECFAA" strokeWidth="2" />
        <line x1={padL} y1={avgY} x2={w} y2={avgY} stroke="#3E658C" strokeWidth="1" strokeDasharray="6,4" />
        <text x={w - 4} y={avgY - 4} textAnchor="end" fontSize="7" fill="#3E658C">avg {Math.round(avg)}/day</text>
        {data.filter((_, i) => i % xStep === 0 || i === data.length - 1).map((d, i) => {
          const origIdx = i * xStep >= data.length ? data.length - 1 : i * xStep;
          const x = padL + (origIdx / (data.length - 1)) * chartW;
          return <text key={i} x={x} y={h - 2} textAnchor="middle" fontSize="7" fill="var(--txt3)">{d.date?.slice(5)}</text>;
        })}
        <text x={padL - 2} y={6} textAnchor="end" fontSize="6" fill="var(--txt3)">units</text>
      </svg>
    </div>
  );
}

function BBChart({ data }) {
  if (!data || data.length < 2) return <div style={{ padding: 20, color: "var(--txt3)", ...SG() }}>Only {data?.length || 0} day(s) of buy box data — more appears after daily S&amp;T report syncs</div>;
  const w = 800, h = 140, minBB = 60, maxBB = 100, padL = 30, padB = 20;
  const chartW = w - padL, chartH = h - padB;
  const scale = (v) => chartH - ((Math.min(maxBB, Math.max(minBB, v)) - minBB) / (maxBB - minBB)) * (chartH - 10);
  const pts = data.map((d, i) => `${padL + (i / (data.length - 1)) * chartW},${scale(d.bb)}`).join(" ");
  const warningY = scale(80);
  const avgBB = data.reduce((s, d) => s + d.bb, 0) / data.length;
  // Y-axis labels
  const yVals = [60, 70, 80, 90, 100];
  return (
    <div style={{ padding: "12px 16px" }}>
      <svg width="100%" viewBox={`0 0 ${w} ${h}`} style={{ display: "block" }}>
        {yVals.map((v, i) => {
          const y = scale(v);
          return <g key={i}><line x1={padL} y1={y} x2={w} y2={y} stroke="var(--brd)" strokeWidth="0.5" opacity=".3" /><text x={padL - 4} y={y + 3} textAnchor="end" fontSize="7" fill="var(--txt3)">{v}%</text></g>;
        })}
        <polyline points={pts} fill="none" stroke="#2ECFAA" strokeWidth="2" />
        <line x1={padL} y1={warningY} x2={w} y2={warningY} stroke="#f87171" strokeWidth="1" strokeDasharray="4,4" />
        <text x={w - 4} y={warningY - 4} textAnchor="end" fontSize="7" fill="#f87171">80% threshold</text>
        <text x={w - 4} y={scale(avgBB) - 4} textAnchor="end" fontSize="7" fill="#2ECFAA">avg {avgBB.toFixed(1)}%</text>
        {data.map((d, i) => {
          const x = padL + (i / (data.length - 1)) * chartW;
          return <text key={i} x={x} y={h - 2} textAnchor="middle" fontSize="7" fill="var(--txt3)">{d.date?.slice(5)}</text>;
        })}
        <text x={padL - 2} y={6} textAnchor="end" fontSize="6" fill="var(--txt3)">BB%</text>
      </svg>
    </div>
  );
}

function InvTrendChart({ data, velTrend }) {
  if ((!data || data.length < 2) && (!velTrend || velTrend.length < 2))
    return <div style={{ padding: 20, color: "var(--txt3)", ...SG() }}>Building 90-day history — data appears after the first daily snapshot (11 PM nightly)</div>;
  const useVel = velTrend && velTrend.length >= 2;
  const w = 1200, h = 200, padL = 40, padB = 24;
  const chartW = w - padL, chartH = h - padB;
  if (!data || data.length < 2) {
    // Show velocity trend as placeholder with labels
    if (!useVel) return null;
    const maxV = Math.max(...velTrend.map(d => d.units), 1);
    const vPts = velTrend.map((d, i) => `${padL + (i / (velTrend.length - 1)) * chartW},${chartH - (d.units / maxV) * (chartH - 20)}`).join(" ");
    const yLabels = [0, Math.round(maxV / 2), Math.round(maxV)];
    const xStep = Math.max(1, Math.floor(velTrend.length / 6));
    return (
      <div style={{ padding: "12px 20px" }}>
        <div style={{ ...SG({ fontSize: 9, color: "var(--txt3)", marginBottom: 6, fontStyle: "italic" }) }}>Showing daily sales velocity (inventory snapshots building — available after nightly captures)</div>
        <svg width="100%" viewBox={`0 0 ${w} ${h}`} style={{ display: "block" }}>
          {yLabels.map((v, i) => {
            const y = chartH - (v / maxV) * (chartH - 20);
            return <g key={i}><line x1={padL} y1={y} x2={w} y2={y} stroke="var(--brd)" strokeWidth="0.5" opacity=".3" /><text x={padL - 4} y={y + 3} textAnchor="end" fontSize="7" fill="var(--txt3)">{v}</text></g>;
          })}
          <polyline points={vPts} fill="none" stroke="#2ECFAA" strokeWidth="2" />
          {velTrend.filter((_, i) => i % xStep === 0 || i === velTrend.length - 1).map((d, idx) => {
            const origI = idx * xStep >= velTrend.length ? velTrend.length - 1 : idx * xStep;
            const x = padL + (origI / (velTrend.length - 1)) * chartW;
            return <text key={idx} x={x} y={h - 2} textAnchor="middle" fontSize="7" fill="var(--txt3)">{d.date?.slice(5)}</text>;
          })}
          <text x={padL - 2} y={6} textAnchor="end" fontSize="6" fill="var(--txt3)">units/day</text>
        </svg>
      </div>
    );
  }
  const maxS = Math.max(...data.map(d => d.sellable), 1);
  const barW = w / data.length;
  return (
    <div style={{ padding: "12px 20px" }}>
      <svg width="100%" height={h} viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
        {data.map((d, i) => {
          const bh = (d.sellable / maxS) * (h - 20);
          return <rect key={i} x={i * barW + 1} y={h - bh} width={barW - 2} height={bh} fill="#1A2D42" stroke="#3E658C" strokeWidth="0.5" rx="1" />;
        })}
        {data.map((d, i) => {
          const ih = (d.inbound / maxS) * (h - 20);
          return <rect key={`ib-${i}`} x={i * barW + 1} y={h - (d.sellable / maxS) * (h - 20) - ih} width={barW - 2} height={ih} fill="rgba(248,113,113,.3)" rx="1" />;
        })}
      </svg>
      <div style={{ display: "flex", justifyContent: "space-between", ...SG({ fontSize: 8, color: "var(--txt3)", marginTop: 4 }) }}>
        <span>{data[0]?.date?.slice(5)}</span><span>{data[data.length - 1]?.date?.slice(5)}</span>
      </div>
    </div>
  );
}

/* ── Card shells ── */
const Card = ({ children, style }) => (
  <div style={{ background: "var(--surf)", border: "1px solid var(--brd)", borderRadius: 13, overflow: "hidden", marginBottom: 14, ...style }}>{children}</div>
);
const CardHdr = ({ title, children }) => (
  <div style={{ padding: "11px 16px", display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid var(--brd)", flexWrap: "wrap", gap: 6 }}>
    <span style={{ fontSize: 12, fontWeight: 700, color: "var(--txt)" }}>{title}</span>
    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>{children}</div>
  </div>
);
const SecDiv = ({ label }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 10, margin: "20px 0 12px" }}>
    <span style={{ ...SG({ fontSize: 9, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".14em", color: "var(--txt3)", whiteSpace: "nowrap" }) }}>{label}</span>
    <div style={{ flex: 1, height: 1, background: "var(--brd)" }} />
  </div>
);
const Badge = ({ type, children }) => {
  const bg = type === "ok" ? "rgba(46,207,170,.14)" : type === "warn" ? "rgba(245,183,49,.14)" : "rgba(248,113,113,.14)";
  const color = type === "ok" ? "#2ECFAA" : type === "warn" ? "#F5B731" : "#f87171";
  return <span style={{ ...SG({ fontSize: 9, padding: "2px 8px", borderRadius: 99, fontWeight: 700, background: bg, color }) }}>{children}</span>;
};
const LegItem = ({ color, label, dashed }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 5, ...SG({ fontSize: 9, color: "var(--txt3)" }) }}>
    <div style={{ width: 10, height: dashed ? 2 : 3, borderRadius: 2, background: color, borderTop: dashed ? `1px dashed ${color}` : "none" }} />
    {label}
  </div>
);

/* ══════════════════════════════════════════════════════════════════════════ */
/* ── Create Shipment Modal ────────────────────────────────────────────── */
const DEFAULT_SHIP_FROM = {
  Name: "GolfGen LLC",
  AddressLine1: "1201 S Walton Blvd",
  AddressLine2: "",
  City: "Bentonville",
  StateOrProvinceCode: "AR",
  PostalCode: "72712",
  CountryCode: "US",
};

function CreateShipmentModal({ onClose }) {
  const [step, setStep] = useState(1); // 1=select, 2=address, 3=review, 4=confirmed
  const [products, setProducts] = useState([]);
  const [loadingProducts, setLoadingProducts] = useState(true);
  const [selectedItems, setSelectedItems] = useState([]); // [{sku,asin,productName,quantity}]
  const [shipFrom, setShipFrom] = useState({ ...DEFAULT_SHIP_FROM });
  const [labelPref, setLabelPref] = useState("SELLER_LABEL");
  const [plans, setPlans] = useState(null);
  const [creating, setCreating] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);
  const [searchQ, setSearchQ] = useState("");
  const [sortCol, setSortCol] = useState("units30d");
  const [sortDir, setSortDir] = useState("desc");

  useEffect(() => {
    api.fbaShipmentProducts()
      .then(d => { setProducts(d.products || []); setLoadingProducts(false); })
      .catch(() => setLoadingProducts(false));
  }, []);

  const addItem = (prod) => {
    if (selectedItems.find(i => i.sku === prod.sku)) return;
    setSelectedItems(prev => [...prev, { sku: prod.sku, asin: prod.asin, productName: prod.productName, quantity: 1 }]);
  };
  const removeItem = (sku) => setSelectedItems(prev => prev.filter(i => i.sku !== sku));
  const updateQty = (sku, qty) => setSelectedItems(prev => prev.map(i => i.sku === sku ? { ...i, quantity: Math.max(1, parseInt(qty) || 1) } : i));

  const handleCreatePlan = async () => {
    setCreating(true); setError(null);
    try {
      const res = await api.fbaCreatePlan({
        shipFromAddress: shipFrom,
        items: selectedItems.map(i => ({ SellerSKU: i.sku, ASIN: i.asin, Quantity: i.quantity, Condition: "NewItem" })),
        labelPrepPreference: labelPref,
      });
      if (res.ok) { setPlans(res.plans); setStep(3); }
      else setError(res.error || "Failed to create plan");
    } catch (e) { setError(e.message); }
    setCreating(false);
  };

  const handleConfirmPlan = async (plan) => {
    setConfirming(true); setError(null);
    try {
      const res = await api.fbaConfirmPlan({
        shipmentId: plan.shipmentId,
        shipmentName: `GolfGen ${new Date().toLocaleDateString()}`,
        destinationFC: plan.destinationFC,
        items: plan.items.map(i => ({ SellerSKU: i.SellerSKU, Quantity: i.Quantity })),
        shipFromAddress: shipFrom,
        labelPrepPreference: labelPref,
      });
      if (res.ok) { setResult(res); setStep(4); }
      else setError(res.error || "Failed to confirm plan");
    } catch (e) { setError(e.message); }
    setConfirming(false);
  };

  const handleSort = (col) => {
    if (sortCol === col) setSortDir(d => d === "asc" ? "desc" : "asc");
    else { setSortCol(col); setSortDir("desc"); }
  };

  const filtered = useMemo(() => {
    let list = products;
    if (searchQ.trim()) {
      const q = searchQ.toLowerCase();
      list = list.filter(p => (p.productName + p.sku + p.asin).toLowerCase().includes(q));
    }
    list = [...list].sort((a, b) => {
      const av = a[sortCol] ?? 0, bv = b[sortCol] ?? 0;
      return sortDir === "asc" ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
    });
    return list;
  }, [products, searchQ, sortCol, sortDir]);

  const fmtD = (n) => n == null ? "—" : Number(n).toLocaleString();
  const fmtR = (n) => n == null ? "—" : `$${Number(n).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
  const stockColor = (stock, u30) => {
    if (!stock || stock <= 0) return "#f87171";
    if (u30 > 0 && stock / (u30 / 30) < 14) return "#F5B731";
    return "#2ECFAA";
  };

  const overlay = { position: "fixed", top: 0, left: 0, right: 0, bottom: 0, background: "rgba(0,0,0,.65)", zIndex: 9999, display: "flex", alignItems: "center", justifyContent: "center" };
  const modal = { background: "var(--card)", border: "1px solid var(--brd)", borderRadius: 16, width: "95vw", maxWidth: 1100, maxHeight: "90vh", overflow: "hidden", display: "flex", flexDirection: "column", boxShadow: "0 20px 60px rgba(0,0,0,.5)" };
  const hdr = { padding: "14px 20px", borderBottom: "1px solid var(--brd)", display: "flex", alignItems: "center", justifyContent: "space-between", background: "var(--card2, var(--card))" };
  const body = { padding: "16px 20px", overflowY: "auto", flex: 1 };
  const footer = { padding: "12px 20px", borderTop: "1px solid var(--brd)", display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8 };
  const btnP = { display: "inline-flex", alignItems: "center", gap: 5, height: 34, padding: "0 18px", borderRadius: 8, ...SG({ fontSize: 11, fontWeight: 700 }), cursor: "pointer", border: "none", background: "var(--acc1)", color: "#fff" };
  const btnS = { ...btnP, background: "transparent", border: "1px solid var(--brd2)", color: "var(--txt2)" };
  const inp = { height: 32, padding: "0 10px", borderRadius: 7, border: "1px solid var(--brd2)", background: "var(--ibg)", color: "var(--txt)", ...SG({ fontSize: 10 }), outline: "none", width: "100%" };
  const thS = { ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)", padding: "8px 6px", textAlign: "right", cursor: "pointer", whiteSpace: "nowrap", userSelect: "none" }) };
  const tdS = { ...SG({ fontSize: 10, padding: "7px 6px", textAlign: "right", color: "var(--txt2)" }) };
  const arrow = (col) => sortCol === col ? (sortDir === "asc" ? " ▲" : " ▼") : "";

  return (
    <div style={overlay} onClick={onClose}>
      <div style={modal} onClick={e => e.stopPropagation()}>
        {/* Header */}
        <div style={hdr}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700, color: "var(--txt)" }}>📦 Create FBA Shipment</div>
            <div style={{ ...SG({ fontSize: 9, color: "var(--txt3)", marginTop: 2 }) }}>
              Step {step} of 4 — {step === 1 ? "Select Products & Quantities" : step === 2 ? "Ship From Address" : step === 3 ? "Review Amazon Plan" : "Confirmed"}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {[1,2,3,4].map(s => (
              <div key={s} style={{ width: 28, height: 4, borderRadius: 2, background: s <= step ? "var(--acc1)" : "var(--brd)", transition: "background .3s" }} />
            ))}
            <button onClick={onClose} style={{ width: 28, height: 28, borderRadius: 6, border: "1px solid var(--brd2)", background: "transparent", color: "var(--txt3)", fontSize: 14, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", marginLeft: 8 }}>×</button>
          </div>
        </div>

        {/* Body */}
        <div style={body}>
          {error && (
            <div style={{ padding: "8px 12px", borderRadius: 8, background: "rgba(248,113,113,.12)", border: "1px solid rgba(248,113,113,.3)", marginBottom: 12, ...SG({ fontSize: 10, color: "#f87171" }) }}>
              {error}
            </div>
          )}

          {/* ── Step 1: Product Selection Table ── */}
          {step === 1 && (
            <>
              {/* Selected items summary */}
              {selectedItems.length > 0 && (
                <div style={{ marginBottom: 14, padding: "10px 14px", borderRadius: 10, background: "rgba(46,207,170,.06)", border: "1px solid rgba(46,207,170,.2)" }}>
                  <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".1em", color: "#2ECFAA", marginBottom: 6 }) }}>
                    Selected: {selectedItems.length} item{selectedItems.length !== 1 ? "s" : ""} · {selectedItems.reduce((s, i) => s + i.quantity, 0)} total units
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {selectedItems.map(item => (
                      <div key={item.sku} style={{ display: "flex", alignItems: "center", gap: 6, padding: "4px 8px", borderRadius: 6, background: "rgba(46,207,170,.1)", border: "1px solid rgba(46,207,170,.25)" }}>
                        <span style={{ ...SG({ fontSize: 9, fontWeight: 700, color: "var(--acc1)" }) }}>{item.asin}</span>
                        <input type="number" min="1" value={item.quantity} onChange={e => updateQty(item.sku, e.target.value)}
                          style={{ width: 50, height: 22, padding: "0 4px", borderRadius: 4, border: "1px solid var(--brd2)", background: "var(--ibg)", color: "var(--txt)", ...SG({ fontSize: 9 }), textAlign: "center", outline: "none" }} />
                        <button onClick={() => removeItem(item.sku)} style={{ width: 18, height: 18, borderRadius: 3, border: "1px solid rgba(248,113,113,.3)", background: "rgba(248,113,113,.08)", color: "#f87171", fontSize: 10, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", padding: 0 }}>×</button>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Search */}
              <input placeholder="Search by product name, SKU, or ASIN…" value={searchQ} onChange={e => setSearchQ(e.target.value)}
                style={{ ...inp, marginBottom: 10 }} />

              {loadingProducts ? (
                <div style={{ padding: 30, textAlign: "center", color: "var(--txt3)", fontSize: 12 }}>Loading products…</div>
              ) : (
                <div style={{ border: "1px solid var(--brd)", borderRadius: 10, overflow: "hidden" }}>
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 900 }}>
                      <thead>
                        <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                          <th style={{ ...thS, textAlign: "left", width: 40 }}></th>
                          <th style={{ ...thS, textAlign: "left", minWidth: 100 }}>ASIN</th>
                          <th style={{ ...thS, textAlign: "left", minWidth: 240 }}>Product Description</th>
                          <th style={{ ...thS, width: 90 }} onClick={() => handleSort("currentStock")}>FBA On Hand{arrow("currentStock")}</th>
                          <th style={{ ...thS, width: 80 }} onClick={() => handleSort("units30d")}>30D Units{arrow("units30d")}</th>
                          <th style={{ ...thS, width: 80 }} onClick={() => handleSort("rev30d")}>30D Rev{arrow("rev30d")}</th>
                          <th style={{ ...thS, width: 80 }} onClick={() => handleSort("units60d")}>60D Units{arrow("units60d")}</th>
                          <th style={{ ...thS, width: 80 }} onClick={() => handleSort("rev60d")}>60D Rev{arrow("rev60d")}</th>
                          <th style={{ ...thS, width: 80 }} onClick={() => handleSort("units90d")}>90D Units{arrow("units90d")}</th>
                          <th style={{ ...thS, width: 80 }} onClick={() => handleSort("rev90d")}>90D Rev{arrow("rev90d")}</th>
                          <th style={{ ...thS, width: 80 }} onClick={() => handleSort("daysOfStock")}>Days Left{arrow("daysOfStock")}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {filtered.length === 0 ? (
                          <tr><td colSpan={11} style={{ padding: 30, textAlign: "center", color: "var(--txt3)", ...SG({ fontSize: 11 }) }}>No products found</td></tr>
                        ) : filtered.map(p => {
                          const isAdded = selectedItems.find(i => i.sku === p.sku);
                          return (
                            <tr key={p.sku}
                              style={{ borderBottom: "1px solid var(--brd)", cursor: isAdded ? "default" : "pointer", opacity: isAdded ? 0.5 : 1, transition: "background .15s" }}
                              onClick={() => !isAdded && addItem(p)}
                              onMouseEnter={e => { if (!isAdded) e.currentTarget.style.background = "var(--ibg)"; }}
                              onMouseLeave={e => { e.currentTarget.style.background = ""; }}>
                              <td style={{ ...tdS, textAlign: "center" }}>
                                {isAdded
                                  ? <span style={{ color: "#2ECFAA", fontWeight: 700 }}>✓</span>
                                  : <span style={{ color: "var(--acc1)", fontWeight: 700 }}>+</span>}
                              </td>
                              <td style={{ ...tdS, textAlign: "left" }}>
                                <div style={{ fontWeight: 700, color: "var(--acc1)" }}>{p.asin}</div>
                                <div style={{ fontSize: 8, color: "var(--txt3)", marginTop: 1 }}>{p.sku}</div>
                              </td>
                              <td style={{ ...tdS, textAlign: "left", maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                <div style={{ color: "var(--txt)" }}>{p.productName || "—"}</div>
                                <div style={{ fontSize: 8, color: "var(--txt3)", marginTop: 1 }}>{p.division}</div>
                              </td>
                              <td style={{ ...tdS, fontWeight: 700, color: stockColor(p.currentStock, p.units30d) }}>{fmtD(p.currentStock)}</td>
                              <td style={{ ...tdS, fontWeight: 700, color: "var(--txt)" }}>{fmtD(p.units30d)}</td>
                              <td style={{ ...tdS, color: "var(--txt2)" }}>{fmtR(p.rev30d)}</td>
                              <td style={{ ...tdS }}>{fmtD(p.units60d)}</td>
                              <td style={{ ...tdS }}>{fmtR(p.rev60d)}</td>
                              <td style={{ ...tdS }}>{fmtD(p.units90d)}</td>
                              <td style={{ ...tdS }}>{fmtR(p.rev90d)}</td>
                              <td style={{ ...tdS, fontWeight: 700, color: p.daysOfStock < 14 ? "#f87171" : p.daysOfStock < 30 ? "#F5B731" : "var(--txt2)" }}>
                                {p.daysOfStock >= 999 ? "∞" : `${Math.round(p.daysOfStock)}d`}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}

          {/* ── Step 2: Ship From Address ── */}
          {step === 2 && (
            <div style={{ maxWidth: 480 }}>
              <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".1em", color: "var(--txt3)", marginBottom: 10 }) }}>Ship From Address</div>
              {[
                { key: "Name", label: "Company / Name" },
                { key: "AddressLine1", label: "Address Line 1" },
                { key: "AddressLine2", label: "Address Line 2 (optional)" },
                { key: "City", label: "City" },
                { key: "StateOrProvinceCode", label: "State" },
                { key: "PostalCode", label: "Zip Code" },
                { key: "CountryCode", label: "Country Code" },
              ].map(f => (
                <div key={f.key} style={{ marginBottom: 8 }}>
                  <label style={{ ...SG({ fontSize: 9, color: "var(--txt3)", display: "block", marginBottom: 3 }) }}>{f.label}</label>
                  <input value={shipFrom[f.key] || ""} onChange={e => setShipFrom(prev => ({ ...prev, [f.key]: e.target.value }))} style={inp} />
                </div>
              ))}
              <div style={{ marginTop: 12 }}>
                <label style={{ ...SG({ fontSize: 9, color: "var(--txt3)", display: "block", marginBottom: 3 }) }}>Label Prep Preference</label>
                <select value={labelPref} onChange={e => setLabelPref(e.target.value)} style={{ ...inp, appearance: "auto" }}>
                  <option value="SELLER_LABEL">Seller Label (you label items)</option>
                  <option value="AMAZON_LABEL_ONLY">Amazon Label Only</option>
                  <option value="AMAZON_LABEL_PREFERRED">Amazon Label Preferred</option>
                </select>
              </div>
            </div>
          )}

          {/* ── Step 3: Review Amazon Plan ── */}
          {step === 3 && plans && (
            <>
              <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".1em", color: "var(--txt3)", marginBottom: 10 }) }}>
                Amazon Proposed {plans.length} Shipment{plans.length !== 1 ? "s" : ""}
              </div>
              <div style={{ padding: "8px 12px", borderRadius: 8, background: "rgba(123,174,208,.1)", border: "1px solid rgba(123,174,208,.25)", marginBottom: 14, ...SG({ fontSize: 10, color: "#7BAED0" }) }}>
                Amazon splits your items across fulfillment centers for optimal placement. Each box below is a separate shipment to pack and ship.
              </div>
              {plans.map((plan, idx) => (
                <Card key={idx} style={{ marginBottom: 10 }}>
                  <div style={{ padding: "10px 14px", borderBottom: "1px solid var(--brd)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: "var(--txt)" }}>{plan.shipmentId}</div>
                      <div style={{ ...SG({ fontSize: 9, color: "var(--txt3)" }) }}>Destination: <span style={{ fontWeight: 700, color: "var(--acc1)" }}>{plan.destinationFC}</span> · {plan.items.length} item{plan.items.length !== 1 ? "s" : ""}</div>
                    </div>
                    <button onClick={() => handleConfirmPlan(plan)} disabled={confirming}
                      style={{ ...btnP, opacity: confirming ? 0.6 : 1, fontSize: 10, height: 30 }}>
                      {confirming ? "Creating…" : "Confirm & Create"}
                    </button>
                  </div>
                  <div style={{ padding: "8px 14px" }}>
                    {plan.items.map((pi, j) => (
                      <div key={j} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "5px 0", borderBottom: j < plan.items.length - 1 ? "1px solid var(--brd)" : "none" }}>
                        <div>
                          <span style={{ ...SG({ fontSize: 10, fontWeight: 700, color: "var(--txt)" }) }}>{pi.SellerSKU}</span>
                          {pi.FulfillmentNetworkSKU && <span style={{ ...SG({ fontSize: 8, color: "var(--txt3)", marginLeft: 6 }) }}>FNSKU: {pi.FulfillmentNetworkSKU}</span>}
                        </div>
                        <span style={{ ...SG({ fontSize: 11, fontWeight: 700, color: "var(--acc1)" }) }}>{pi.Quantity} units</span>
                      </div>
                    ))}
                  </div>
                </Card>
              ))}
            </>
          )}

          {/* ── Step 4: Confirmed ── */}
          {step === 4 && result && (
            <div style={{ textAlign: "center", padding: "30px 20px" }}>
              <div style={{ fontSize: 40, marginBottom: 10 }}>✅</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: "var(--acc1)", marginBottom: 6 }}>Shipment Created!</div>
              <div style={{ ...SG({ fontSize: 12, color: "var(--txt2)", marginBottom: 16 }) }}>{result.message}</div>
              <div style={{ ...SG({ fontSize: 10, fontWeight: 700, color: "var(--acc3, #3E658C)", marginBottom: 16 }) }}>
                Shipment ID: {result.shipmentId}
              </div>
              {result.sellerCentralUrl && (
                <a href={result.sellerCentralUrl} target="_blank" rel="noreferrer"
                  style={{ ...btnP, textDecoration: "none", display: "inline-flex" }}>
                  Open in Seller Central →
                </a>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        {step < 4 && (
          <div style={footer}>
            <div>
              {step > 1 && step < 3 && <button onClick={() => setStep(step - 1)} style={btnS}>← Back</button>}
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={onClose} style={btnS}>Cancel</button>
              {step === 1 && (
                <button onClick={() => setStep(2)} disabled={selectedItems.length === 0}
                  style={{ ...btnP, opacity: selectedItems.length === 0 ? 0.4 : 1 }}>
                  Next: Ship From → ({selectedItems.length} selected)
                </button>
              )}
              {step === 2 && (
                <button onClick={handleCreatePlan} disabled={creating}
                  style={{ ...btnP, opacity: creating ? 0.6 : 1 }}>
                  {creating ? "Submitting to Amazon…" : "Submit Plan to Amazon →"}
                </button>
              )}
            </div>
          </div>
        )}
        {step === 4 && (
          <div style={footer}>
            <div />
            <button onClick={onClose} style={btnP}>Done</button>
          </div>
        )}
      </div>
    </div>
  );
}

/* ══════════════════════════════════════════════════════════════════════════ */
export default function Inventory({ filters = {} }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [skuFilter, setSkuFilter] = useState("all");
  const [searchTerm, setSearchTerm] = useState("");
  const [showCreate, setShowCreate] = useState(false);

  const divRaw = (filters.division || "").toLowerCase();
  const divLabel = !divRaw ? "All Divisions" : divRaw === "golf" ? "Golf (PGAT)" : "Housewares";
  const custLabel = filters.customer ? ` · ${filters.customer.replace(/_/g, " ").replace(/\b\w/g, l => l.toUpperCase())}` : "";
  const currentPath = typeof window !== "undefined" ? window.location.pathname : "/inventory";

  useEffect(() => {
    setLoading(true);
    api.inventoryCommandCenter(filters)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [filters.division, filters.customer]);

  const filteredSkus = useMemo(() => {
    if (!data?.skus) return [];
    let list = data.skus;
    if (skuFilter !== "all") list = list.filter(s => s.risk === skuFilter);
    if (searchTerm) {
      const q = searchTerm.toLowerCase();
      list = list.filter(s => s.name.toLowerCase().includes(q) || s.asin.toLowerCase().includes(q) || (s.sku || "").toLowerCase().includes(q));
    }
    return list;
  }, [data?.skus, skuFilter, searchTerm]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading Amazon FBA Inventory...</div>;
  if (!data) return <div style={{ padding: 40, textAlign: "center", color: "var(--txt3)" }}>Failed to load inventory data.</div>;

  const k = data.kpis || {};
  const pipe = data.pipeline || {};
  const aging = data.aging || {};
  const health = data.health || {};
  const alerts = data.alerts || {};

  /* ── KPI card data ── */
  const deltas = k.deltas || {};
  const kpiCards = [
    { label: "FBA Units On Hand", value: fmtNum(k.totalUnits), color: "#2ECFAA", accent: "#2ECFAA", sub: "All sellable ASINs", delta: deltas.totalUnits },
    { label: "Sellable Now", value: fmtNum(k.sellable), color: "#7BAED0", accent: "#7BAED0", sub: "Fulfillable qty", delta: deltas.sellable },
    { label: "Inbound Pipeline", value: fmtNum(k.inbound), color: "#E87830", accent: "#E87830", sub: "In transit to FBA", delta: deltas.inbound },
    { label: "Avg Weeks Cover", value: k.weeksCover ?? "—", color: "#F5B731", accent: "#F5B731", sub: "Portfolio avg", delta: deltas.weeksCover },
    { label: "Aged >180 Days", value: fmtNum(k.aged180Plus), color: "#f87171", accent: "#f87171", sub: "LTSF fee risk", delta: deltas.aged180Plus },
    { label: "Sell-Through Rate", value: k.sellThrough != null ? `${k.sellThrough}×` : "—", color: "#3E658C", accent: "#3E658C", sub: "90d units / avg inv", delta: deltas.sellThrough },
    { label: "Inventory Value", value: k.inventoryValue > 0 ? `$${fmtK(k.inventoryValue)}` : "—", color: "#2ECFAA", accent: "#2ECFAA", sub: "At cost", delta: deltas.inventoryValue },
    { label: "Avg Buy Box %", value: fmtPct(k.avgBuyBox), color: "#2ECFAA", accent: "#2ECFAA", sub: "Weighted by sessions", delta: deltas.avgBuyBox },
  ];

  const pipeTotal = pipe.sellable + pipe.inbound + pipe.reserved + (pipe.fcTransfer || 0) + pipe.unfulfillable || 1;

  return (
    <div style={{ fontFamily: "'Sora',-apple-system,BlinkMacSystemFont,sans-serif", color: "var(--txt)" }}>

      {/* ══ HEADER — matches Exec Summary style ══ */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16, flexWrap: "wrap", gap: 10 }}>
        <div>
          <h2 style={{ ...DM({ fontSize: 22, fontWeight: 400, margin: 0, color: "var(--acc1, #2ECFAA)" }) }}>
            Amazon FBA Inventory
          </h2>
          <div style={{ ...SG({ fontSize: 12, color: "var(--txt3)", marginTop: 3 }) }}>
            {divLabel}{custLabel} · {k.totalAsins || 0} active ASINs
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
            <button style={{ display: "inline-flex", alignItems: "center", gap: 5, height: 30, padding: "0 12px", borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: "pointer", whiteSpace: "nowrap", border: "1px solid var(--acc1)", background: "rgba(46,207,170,.1)", color: "var(--acc1)" }}>⛳ Sync FBA ↑</button>
            <span style={{ ...SG({ fontSize: 8, color: "var(--txt3)", marginTop: 1, textAlign: "center" }) }}>Auto every 4hr</span>
          </div>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center" }}>
            <button onClick={() => setShowCreate(true)} style={{ display: "inline-flex", alignItems: "center", gap: 5, height: 30, padding: "0 12px", borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: "pointer", whiteSpace: "nowrap", border: "1px solid var(--acc2, #E87830)", background: "rgba(232,120,48,.1)", color: "var(--acc2, #E87830)" }}>📦 Create Shipment</button>
            <span style={{ ...SG({ fontSize: 8, color: "var(--txt3)", marginTop: 1, textAlign: "center" }) }}>Send to FBA</span>
          </div>
          <button style={{ display: "inline-flex", alignItems: "center", gap: 5, height: 30, padding: "0 12px", borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: "pointer", whiteSpace: "nowrap", border: "1px solid var(--brd2)", background: "var(--ibg, rgba(255,255,255,.05))", color: "var(--txt2)" }}>⬇ Export</button>
          <button style={{ display: "inline-flex", alignItems: "center", gap: 5, height: 30, padding: "0 12px", borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: "pointer", whiteSpace: "nowrap", border: "1px solid var(--brd2)", background: "var(--ibg, rgba(255,255,255,.05))", color: "var(--txt2)" }}>⚙ Alerts</button>
        </div>
      </div>

      {/* ══ Sub-nav tabs ══ */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 18, flexWrap: "wrap" }}>
        <div className="ptab-bar">
          {INV_VIEWS.map(v => (
            <a key={v.key} href={v.path} className={`ptab${currentPath === v.path ? " active" : ""}`} style={{ textDecoration: "none" }}>{v.label}</a>
          ))}
        </div>
      </div>

      {/* ══ ALERTS ══ */}
      {alerts.critical?.length > 0 && (
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 14px", borderRadius: 9, marginBottom: 10, border: "1px solid rgba(208,48,48,.3)", background: "rgba(208,48,48,.12)", ...SG({ fontSize: 11, fontWeight: 600, color: "#f87171" }) }}>
          <span style={{ fontSize: 13 }}>🚨</span>
          <span style={{ whiteSpace: "nowrap" }}>STOCKOUT RISK —</span>
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            {alerts.critical.slice(0, 5).map(a => (
              <span key={a.sku} style={{ padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700, background: "rgba(248,113,113,.2)", color: "#f87171" }}>
                {a.sku} · {Math.round(a.daysCover)}d cover
              </span>
            ))}
          </div>
        </div>
      )}
      {alerts.warn?.length > 0 && (
        <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 14px", borderRadius: 9, marginBottom: 14, border: "1px solid rgba(245,183,49,.25)", background: "rgba(245,183,49,.1)", ...SG({ fontSize: 11, fontWeight: 600, color: "#F5B731" }) }}>
          <span style={{ fontSize: 13 }}>⚠️</span>
          <span style={{ whiteSpace: "nowrap" }}>ATTENTION —</span>
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            {alerts.warn.slice(0, 5).map((a, i) => (
              <span key={i} style={{ padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700, background: "rgba(245,183,49,.15)", color: "#F5B731" }}>
                {a.type === "buybox" ? `Buy Box lost: ${a.sku} (${a.bb}%)` : a.type}
              </span>
            ))}
            {aging.aged_180_plus > 0 && <span style={{ padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700, background: "rgba(245,183,49,.15)", color: "#F5B731" }}>Aged &gt;180d: {fmtNum(aging.aged_180_plus)} units</span>}
            {health.strandedUnits > 0 && <span style={{ padding: "2px 8px", borderRadius: 99, fontSize: 10, fontWeight: 700, background: "rgba(245,183,49,.15)", color: "#F5B731" }}>Stranded: {health.strandedSkus} SKUs · {fmtNum(health.strandedUnits)} units</span>}
          </div>
        </div>
      )}

      {/* ══ 8 KPI CARDS ══ */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(8, 1fr)", gap: 8, marginBottom: 16 }}>
        {kpiCards.map((c, i) => (
          <div key={i} className="inv-kpi-card" style={{ background: "linear-gradient(145deg, var(--card, #112030), var(--card2, #1A2D42))", borderRadius: 11, padding: "11px 13px", border: "1px solid var(--brd)", position: "relative", overflow: "hidden" }}>
            <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: c.accent }} />
            <div style={{ ...SG({ fontSize: 7, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".09em", marginBottom: 4 }) }}>{c.label}</div>
            <div style={{ ...DM({ fontSize: 20, lineHeight: 1, marginBottom: 2, color: c.color }) }}>{c.value}</div>
            {c.delta?.label && <div style={{ ...SG({ display: "inline-flex", alignItems: "center", gap: 2, fontSize: 8, fontWeight: 700, color: c.delta.value > 0 ? "#2ECFAA" : c.delta.value < 0 ? "#f87171" : "var(--txt3)" }) }}>{c.delta.label}</div>}
            <div style={{ ...SG({ fontSize: 8, color: "var(--txt3)", marginTop: 1 }) }}>{c.sub}</div>
          </div>
        ))}
      </div>

      {/* ══ FBM SUMMARY ══ */}
      {data.fbm && data.fbm.totalUnits > 0 && (
        <div style={{ padding: "10px 16px", borderRadius: 9, marginBottom: 14, border: "1px solid rgba(62,101,140,.3)", background: "rgba(30,58,95,.25)" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12, ...SG({ fontSize: 11, color: "var(--txt2)" }) }}>
            <span style={{ fontWeight: 700, color: "#7BAED0" }}>FBM Inventory</span>
            <span style={{ padding: "2px 8px", borderRadius: 99, fontSize: 9, fontWeight: 700, background: "rgba(123,174,208,.15)", color: "#7BAED0" }}>{data.fbm.asinCount} ASINs · {fmtNum(data.fbm.totalUnits)} units</span>
            <span style={{ fontSize: 9, color: "var(--txt3)", marginLeft: "auto" }}>Not included in FBA metrics above</span>
          </div>
          {data.fbm.items?.length > 0 && (
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginTop: 6 }}>
              {data.fbm.items.slice(0, 6).map((item, i) => (
                <span key={i} style={{ ...SG({ fontSize: 9, color: "var(--txt3)" }) }}>{item.sku || item.asin}: <span style={{ color: "var(--txt2)", fontWeight: 600 }}>{fmtNum(item.units)}</span></span>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ══ 30-DAY TRENDS ══ */}
      <SecDiv label="30-Day Trends — Full Portfolio" />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 }}>
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Daily Sales Velocity — Units Sold (30 Days)">
            <div style={{ display: "flex", gap: 14, alignItems: "center" }}>
              <LegItem color="#2ECFAA" label="Units/day" />
              <LegItem color="#3E658C" label="30d avg" dashed />
            </div>
            <Badge type="ok">7d avg: {k.avgDaily7d}/d</Badge>
          </CardHdr>
          <VelChart data={data.velTrend} />
        </Card>
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Portfolio Buy Box % Trend (30 Days)">
            <div style={{ display: "flex", gap: 14, alignItems: "center" }}>
              <LegItem color="#2ECFAA" label="Weighted BB%" />
              <LegItem color="#f87171" label="Warning (80%)" />
            </div>
            {alerts.warn?.filter(a => a.type === "buybox").length > 0 && <Badge type="warn">{alerts.warn.filter(a => a.type === "buybox").length} ASIN below 80%</Badge>}
          </CardHdr>
          <BBChart data={data.bbTrend} />
        </Card>
      </div>

      {/* ══ 90-DAY INVENTORY vs SALES ══ */}
      <SecDiv label="Inventory vs. Sales Overlap — 90-Day View" />
      <Card>
        <CardHdr title="FBA Sellable Units vs. Daily Sales Velocity — 90 Days">
          <div style={{ display: "flex", gap: 14, flexWrap: "wrap", alignItems: "center" }}>
            <LegItem color="#1A2D42" label="Inventory level" />
            <LegItem color="#2ECFAA" label="Daily velocity" />
            <LegItem color="rgba(248,113,113,.3)" label="Inbound pipeline" />
          </div>
          <Badge type="ok">Inventory health: Good</Badge>
        </CardHdr>
        <InvTrendChart data={data.invTrend} velTrend={data.velTrend} />
      </Card>

      {/* ══ PIPELINE & AGING ══ */}
      <SecDiv label="Inventory Pipeline & Aging" />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 14 }}>
        {/* Pipeline */}
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Inventory Pipeline — All Locations">
            <span style={{ ...SG({ fontSize: 10, color: "var(--txt3)" }) }}>Total tracked: <span style={{ color: "var(--txt)", fontWeight: 600 }}>{fmtNum(pipeTotal)}</span> units</span>
            <Badge type="ok">Healthy</Badge>
          </CardHdr>
          <div style={{ display: "flex", gap: 0, padding: "14px 20px" }}>
            {[
              { icon: "✅", val: pipe.sellable, lbl: "Sellable", color: "#2ECFAA" },
              { icon: "📦", val: pipe.inbound, lbl: "Inbound", color: "#E87830" },
              { icon: "🔒", val: pipe.reserved, lbl: "Reserved", color: "#7BAED0" },
              { icon: "🔄", val: pipe.fcTransfer || 0, lbl: "FC Transfer", color: "#F5B731" },
              { icon: "⚠️", val: pipe.unfulfillable, lbl: "Unfulfillable", color: "#f87171" },
            ].map((s, i, arr) => (
              <div key={i} style={{ flex: 1, textAlign: "center", position: "relative" }}>
                <div style={{ fontSize: 20, marginBottom: 4 }}>{s.icon}</div>
                <div style={{ ...DM({ fontSize: 22, lineHeight: 1, marginBottom: 2, color: s.color }) }}>{fmtNum(s.val)}</div>
                <div style={{ ...SG({ fontSize: 8, textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)" }) }}>{s.lbl}</div>
                <div style={{ ...SG({ fontSize: 8, color: "var(--txt3)", marginTop: 2 }) }}>{pipeTotal > 0 ? `${Math.round(s.val / pipeTotal * 100)}%` : ""}</div>
                {i < arr.length - 1 && <span style={{ position: "absolute", right: -8, top: "50%", transform: "translateY(-50%)", color: "var(--txt3)", fontSize: 12 }}>→</span>}
              </div>
            ))}
          </div>
          <div style={{ padding: "0 20px 14px" }}>
            <div style={{ display: "flex", height: 10, borderRadius: 5, overflow: "hidden" }}>
              <div style={{ width: `${pipe.sellable / pipeTotal * 100}%`, background: "#2ECFAA", opacity: 0.85 }} />
              <div style={{ width: `${pipe.inbound / pipeTotal * 100}%`, background: "#E87830", opacity: 0.85 }} />
              <div style={{ width: `${pipe.reserved / pipeTotal * 100}%`, background: "#7BAED0", opacity: 0.85 }} />
              <div style={{ width: `${(pipe.fcTransfer || 0) / pipeTotal * 100}%`, background: "#F5B731", opacity: 0.85 }} />
              <div style={{ width: `${pipe.unfulfillable / pipeTotal * 100}%`, background: "#f87171", opacity: 0.85 }} />
            </div>
          </div>
          {/* Reserved Breakdown */}
          {pipe.reservedBreakdown && (
            <div style={{ borderTop: "1px solid var(--brd)", padding: "10px 20px" }}>
              <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)", marginBottom: 7 }) }}>Reserved Breakdown</div>
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                {[
                  { lbl: "Customer Orders", val: pipe.reservedBreakdown.customerOrders, color: "#7BAED0" },
                  { lbl: "FC Transfer", val: pipe.reservedBreakdown.fcTransfer, color: "#7BAED0" },
                  { lbl: "FC Processing", val: pipe.reservedBreakdown.fcProcessing, color: "#7BAED0" },
                ].map((b, i) => (
                  <div key={i} style={{ ...SG({ fontSize: 9, color: "var(--txt3)" }) }}>{b.lbl} <span style={{ color: b.color, fontWeight: 700 }}>{fmtNum(b.val)}</span></div>
                ))}
              </div>
            </div>
          )}
          {/* Unfulfillable Breakdown */}
          {pipe.unfulfillableBreakdown && (
            <div style={{ borderTop: "1px solid var(--brd)", padding: "10px 20px" }}>
              <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)", marginBottom: 7 }) }}>Unfulfillable Breakdown</div>
              <div style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
                {[
                  { lbl: "Customer Dmg", val: pipe.unfulfillableBreakdown.customerDamaged },
                  { lbl: "Warehouse Dmg", val: pipe.unfulfillableBreakdown.warehouseDamaged },
                  { lbl: "Defective", val: pipe.unfulfillableBreakdown.defective },
                  { lbl: "Carrier Dmg", val: pipe.unfulfillableBreakdown.carrierDamaged },
                ].map((b, i) => (
                  <div key={i} style={{ ...SG({ fontSize: 9, color: "var(--txt3)" }) }}>{b.lbl} <span style={{ color: "#f87171", fontWeight: 700 }}>{fmtNum(b.val)}</span></div>
                ))}
              </div>
            </div>
          )}
        </Card>

        {/* Aging */}
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Inventory Aging Distribution">
            {aging.total_ltsf > 0 && <span style={{ ...SG({ fontSize: 10, color: "var(--txt3)" }) }}>LTSF: <span style={{ color: "#f87171", fontWeight: 700 }}>${fmtNum(Math.round(aging.total_ltsf))}/mo</span></span>}
            {aging.aged_180_plus > 0 ? <Badge type="warn">Action recommended</Badge> : <Badge type="ok">Healthy</Badge>}
          </CardHdr>
          <div style={{ padding: "12px 0 4px" }}>
            {(aging.brackets || []).map((b, i) => {
              const maxUnits = Math.max(...(aging.brackets || []).map(x => x.units), 1);
              return (
                <div key={i} style={{ display: "grid", gridTemplateColumns: "90px 1fr 56px 52px", alignItems: "center", gap: 10, padding: "5px 20px" }}>
                  <span style={{ ...SG({ fontSize: 9, color: "var(--txt3)" }) }}>{b.label}</span>
                  <div style={{ height: 10, borderRadius: 5, background: "var(--brd)", overflow: "hidden" }}>
                    <div style={{ width: `${b.units / maxUnits * 100}%`, height: "100%", borderRadius: 5, background: b.color, transition: "width .8s" }} />
                  </div>
                  <span style={{ ...SG({ fontSize: 10, fontWeight: 700, textAlign: "right" }) }}>{fmtNum(b.units)}</span>
                  <span style={{ ...SG({ fontSize: 10, color: "var(--txt3)", textAlign: "right" }) }}>{b.pct}%</span>
                </div>
              );
            })}
            {(!aging.brackets || aging.brackets.length === 0) && (
              <div style={{ padding: "16px 20px", color: "var(--txt3)", ...SG({ fontSize: 10 }) }}>Aging data will appear after the next SP-API sync</div>
            )}
          </div>
          {/* Monthly Storage Fee Forecast */}
          {data.storageForecast?.length > 0 && (
            <div style={{ borderTop: "1px solid var(--brd)", padding: "12px 20px" }}>
              <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", color: "var(--txt3)", marginBottom: 8 }) }}>Monthly Storage Fee Forecast — Next 6 Months</div>
              <div style={{ display: "flex", alignItems: "flex-end", gap: 6, height: 70 }}>
                {data.storageForecast.map((m, i) => {
                  const maxFee = Math.max(...data.storageForecast.map(x => x.fee), 1);
                  const barH = Math.max(4, (m.fee / maxFee) * 56);
                  const col = m.fee > maxFee * 0.85 ? "#f87171" : m.fee > maxFee * 0.7 ? "#F5B731" : "#2ECFAA";
                  return (
                    <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center" }}>
                      <span style={{ ...SG({ fontSize: 7, color: col, fontWeight: 700, marginBottom: 2 }) }}>${m.fee >= 1000 ? `${(m.fee / 1000).toFixed(1)}K` : m.fee}</span>
                      <div style={{ width: "100%", height: barH, borderRadius: 2, background: col, opacity: 0.7 }} />
                      <span style={{ ...SG({ fontSize: 7, color: "var(--txt3)", marginTop: 3 }) }}>{m.month}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </Card>
      </div>

      {/* ══ FC DISTRIBUTION & HEALTH ══ */}
      <SecDiv label="FC Distribution & Health" />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 14, marginBottom: 14 }}>
        {/* FC Distribution */}
        <Card style={{ marginBottom: 0 }}>
          <div style={{ padding: "9px 14px", borderBottom: "1px solid var(--brd)", background: "var(--card2)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: "var(--txt)" }}>Amazon FC Distribution</span>
            <Badge type="ok">{data.fcDistribution?.length || 0} FCs</Badge>
          </div>
          {(data.fcDistribution || []).map((fc, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 16px", borderBottom: i < (data.fcDistribution?.length || 0) - 1 ? "1px solid var(--brd)" : "none" }}>
              <div style={{ width: 62, flexShrink: 0 }}>
                <span style={{ ...SG({ fontSize: 9, fontWeight: 700, color: "var(--txt2)", display: "block" }) }}>{fc.name}</span>
                {fc.location && <span style={{ ...SG({ fontSize: 7, color: "var(--txt3)", display: "block", marginTop: 1 }) }}>{fc.location}</span>}
              </div>
              <div style={{ flex: 1, height: 8, background: "var(--brd)", borderRadius: 4, overflow: "hidden" }}>
                <div style={{ height: "100%", width: `${fc.pct * 2}%`, borderRadius: 4, background: fc.color }} />
              </div>
              <span style={{ ...SG({ fontSize: 9, color: "var(--txt3)", width: 48, textAlign: "right", flexShrink: 0 }) }}>{fc.units >= 1000 ? `${(fc.units / 1000).toFixed(1)}K` : fc.units}</span>
              <span style={{ ...SG({ fontSize: 9, fontWeight: 700, width: 32, textAlign: "right", flexShrink: 0, color: fc.color }) }}>{fc.pct}%</span>
            </div>
          ))}
          {(!data.fcDistribution || data.fcDistribution.length === 0) && (
            <div style={{ padding: "16px 14px", color: "var(--txt3)", ...SG({ fontSize: 10 }) }}>FC data will appear after SP-API sync</div>
          )}
        </Card>

        {/* Return Rate by SKU */}
        <Card style={{ marginBottom: 0 }}>
          <div style={{ padding: "9px 14px", borderBottom: "1px solid var(--brd)", background: "var(--card2)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: "var(--txt)" }}>Return Rate by SKU (90 Days)</span>
            {data.returnRateTable?.length > 0 && <Badge type="ok">Avg {(data.returnRateTable.reduce((s, r) => s + r.rate, 0) / data.returnRateTable.length).toFixed(1)}%</Badge>}
          </div>
          {/* Column headers */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 44px 44px 54px", gap: 6, padding: "5px 16px", borderBottom: "1px solid var(--brd2)", background: "var(--card2)" }}>
            {["SKU", "Sold", "Returned", "Rate"].map((h, i) => (
              <span key={i} style={{ ...SG({ fontSize: 8, fontWeight: 700, color: "var(--txt3)", textAlign: i > 0 ? "right" : "left" }) }}>{h}</span>
            ))}
          </div>
          {(data.returnRateTable || []).slice(0, 8).map((r, i) => {
            const rateCol = r.rate > 4 ? "#f87171" : r.rate > 3 ? "#F5B731" : "#2ECFAA";
            return (
              <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 44px 44px 54px", gap: 6, alignItems: "center", padding: "6px 16px", borderBottom: "1px solid var(--brd)" }}>
                <span style={{ ...SG({ fontSize: 9, color: "#3E658C", fontWeight: 700 }) }}>{r.sku}</span>
                <span style={{ ...SG({ fontSize: 9, color: "var(--txt2)", textAlign: "right" }) }}>{fmtNum(r.sold)}</span>
                <span style={{ ...SG({ fontSize: 9, color: "var(--txt3)", textAlign: "right" }) }}>{r.returned}</span>
                <span style={{ ...SG({ fontSize: 10, fontWeight: 700, color: rateCol, textAlign: "right" }) }}>{r.rate}%</span>
              </div>
            );
          })}
          {(!data.returnRateTable || data.returnRateTable.length === 0) && (
            <div style={{ padding: "16px 14px", color: "var(--txt3)", ...SG({ fontSize: 10 }) }}>Return data pending</div>
          )}
        </Card>

        {/* Portfolio Health Metrics */}
        <Card style={{ marginBottom: 0 }}>
          <div style={{ padding: "9px 14px", borderBottom: "1px solid var(--brd)", background: "var(--card2)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <span style={{ fontSize: 11, fontWeight: 700, color: "var(--txt)" }}>Portfolio Health Metrics</span>
            <div>
              <svg viewBox="0 0 120 60" width="80" height="40" style={{ display: "inline-block", verticalAlign: "middle" }}>
                <path d="M10 55 A46 46 0 0 1 110 55" fill="none" stroke="var(--brd)" strokeWidth="9" strokeLinecap="round" />
                <path d="M10 55 A46 46 0 0 1 110 55" fill="none" stroke="#2ECFAA" strokeWidth="9" strokeLinecap="round" strokeDasharray="145" strokeDashoffset={145 - (health.score / 100 * 145)} opacity=".9" />
                <text x="60" y="52" textAnchor="middle" fontFamily="DM Serif Display,serif" fontSize="18" fill="#2ECFAA">{health.score}</text>
              </svg>
              <span style={{ ...SG({ fontSize: 8, color: "var(--txt3)", verticalAlign: "middle" }) }}>/100</span>
            </div>
          </div>
          {[
            { lbl: "Inventory Turnover", val: `${health.turnover}×`, color: "#7BAED0", extra: null, delta: true },
            { lbl: "Stranded Units", val: fmtNum(health.strandedUnits), color: health.strandedUnits > 0 ? "#F5B731" : "#2ECFAA", extra: health.strandedSkus > 0 ? `${health.strandedSkus} SKUs` : "" },
            { lbl: "Researching (Lost)", val: fmtNum(health.researching || 0), color: (health.researching || 0) > 0 ? "#f87171" : "var(--txt3)", extra: (health.researching || 0) > 0 ? "Investigate" : "" },
            { lbl: "Reserved %", val: `${health.reservedPct}%`, color: "var(--txt2)", extra: health.reservedPct < 20 ? "Normal" : "Elevated" },
            { lbl: "Order Defect Rate", val: `${health.orderDefectRate || 0}%`, color: (health.orderDefectRate || 0) < 1 ? "#2ECFAA" : "#f87171", extra: (health.orderDefectRate || 0) < 1 ? "Excellent" : "Action needed" },
            { lbl: "Cancellation Rate", val: `${health.cancellationRate || 0}%`, color: (health.cancellationRate || 0) < 2.5 ? "#2ECFAA" : "#F5B731", extra: (health.cancellationRate || 0) < 2.5 ? "Healthy" : "Elevated" },
          ].map((m, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 14px", borderBottom: i < 5 ? "1px solid var(--brd)" : "none" }}>
              <div>
                <div style={{ ...SG({ fontSize: 8, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".07em" }) }}>{m.lbl}</div>
                <div style={{ ...DM({ fontSize: 15, lineHeight: 1, color: m.color }) }}>{m.val}</div>
              </div>
              {m.extra && <span style={{ ...SG({ fontSize: 8, color: "var(--txt3)" }) }}>{m.extra}</span>}
            </div>
          ))}
        </Card>
      </div>

      {/* ══ REPLENISHMENT FORECAST ══ */}
      <SecDiv label="Replenishment Forecast & Risk Radar" />
      <Card>
        <CardHdr title="Reorder Planning — All SKUs by Risk">
          <span style={{ ...SG({ fontSize: 10, color: "var(--txt3)" }) }}>
            <span style={{ color: "#f87171" }}>{data.skus?.filter(s => s.risk === "critical").length || 0}</span> Critical ·{" "}
            <span style={{ color: "#F5B731" }}>{data.skus?.filter(s => s.risk === "watch").length || 0}</span> Watch ·{" "}
            <span style={{ color: "#2ECFAA" }}>{data.skus?.filter(s => s.risk === "low").length || 0}</span> Healthy
          </span>
          {data.skus?.some(s => s.risk === "critical") && <Badge type="warn">Reorder needed this week</Badge>}
        </CardHdr>
        {/* Header */}
        <div style={{ display: "grid", gridTemplateColumns: "16px 1fr 72px 72px 66px 66px 72px 72px 74px", gap: 8, padding: "7px 16px", borderBottom: "1px solid var(--brd2)", background: "var(--card2)" }}>
          {["", "SKU / Product", "On Hand", "Inbound", "Daily Vel.", "Days Cover", "Wks Cover", "Reorder Pt", "Risk"].map((h, i) => (
            <span key={i} style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)", textAlign: i >= 2 ? "right" : "left" }) }}>{h}</span>
          ))}
        </div>
        {(data.replForecast || []).slice(0, 12).map((s, i) => {
          const riskColor = s.risk === "critical" ? "#f87171" : s.risk === "watch" ? "#F5B731" : "#2ECFAA";
          const riskBg = s.risk === "critical" ? "rgba(248,113,113,.15)" : s.risk === "watch" ? "rgba(245,183,49,.14)" : "rgba(46,207,170,.12)";
          return (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "16px 1fr 72px 72px 66px 66px 72px 72px 74px", gap: 8, padding: "9px 16px", borderBottom: "1px solid var(--brd)", alignItems: "center" }}>
              <span style={{ width: 8, height: 8, borderRadius: "50%", background: riskColor, display: "inline-block" }} />
              <div>
                <div style={{ fontSize: 11, fontWeight: 600, color: "var(--txt)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{s.name}</div>
                <div style={{ ...SG({ fontSize: 8, color: "#3E658C" }) }}>{s.sku}</div>
              </div>
              <span style={{ ...SG({ fontSize: 10, textAlign: "right" }) }}>{fmtNum(s.onHand)}</span>
              <span style={{ ...SG({ fontSize: 10, textAlign: "right" }) }}>{fmtNum(s.inbound)}</span>
              <span style={{ ...SG({ fontSize: 10, textAlign: "right" }) }}>{s.dailyVel}</span>
              <span style={{ ...SG({ fontSize: 10, textAlign: "right", color: s.daysCover != null && s.daysCover < 30 ? "#f87171" : "var(--txt)" }) }}>
                {s.daysCover != null ? Math.round(s.daysCover) : "∞"}
              </span>
              <span style={{ ...SG({ fontSize: 10, textAlign: "right" }) }}>{s.weeksCover != null ? s.weeksCover : "∞"}</span>
              <span style={{ ...SG({ fontSize: 10, textAlign: "right", color: "var(--txt3)" }) }}>{s.reorderPt != null ? fmtNum(s.reorderPt) : "—"}</span>
              <span style={{ ...SG({ fontSize: 9, fontWeight: 700, padding: "2px 8px", borderRadius: 6, textAlign: "center", background: riskBg, color: riskColor, whiteSpace: "nowrap" }) }}>
                {s.risk === "critical" ? "CRITICAL" : s.risk === "watch" ? "WATCH" : "HEALTHY"}
              </span>
            </div>
          );
        })}
      </Card>

      {/* ══ REORDER TIMELINE ══ */}
      <SecDiv label="Reorder Timeline — When Each SKU Hits Reorder Point" />
      <Card>
        <CardHdr title="Days Until Reorder Point — Portfolio Timeline View">
          <div style={{ display: "flex", gap: 14, alignItems: "center" }}>
            <LegItem color="#2ECFAA" label="Days of cover remaining" />
            <LegItem color="#f87171" label="Reorder threshold" />
          </div>
          <span style={{ ...SG({ fontSize: 10, color: "var(--txt3)" }) }}>Lead time: <span style={{ color: "var(--txt)", fontWeight: 600 }}>45 days</span></span>
        </CardHdr>
        <div style={{ padding: "6px 0" }}>
          <div style={{ display: "grid", gridTemplateColumns: "130px 1fr 56px 70px", gap: 8, padding: "6px 16px", borderBottom: "1px solid var(--brd2)", background: "var(--card2)" }}>
            {["SKU", "Coverage bar (100d scale)", "Days", "Reorder by"].map((h, i) => (
              <span key={i} style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)", textAlign: i >= 2 ? "right" : "left" }) }}>{h}</span>
            ))}
          </div>
          {(data.reorderTimeline || []).slice(0, 10).map((r, i) => {
            const coverPct = Math.min(100, ((r.daysCover || 0) / 100) * 100);
            const reorderPct = Math.min(100, (45 / 100) * 100); // lead time threshold
            const urgColor = r.urgency === "critical" ? "#f87171" : r.urgency === "watch" ? "#F5B731" : "#2ECFAA";
            return (
              <div key={i} style={{ display: "grid", gridTemplateColumns: "130px 1fr 56px 70px", gap: 8, alignItems: "center", padding: "7px 16px", borderBottom: "1px solid var(--brd)" }}>
                <div>
                  <div style={{ ...SG({ fontSize: 9, fontWeight: 700, color: "#3E658C" }) }}>{r.sku}</div>
                  <div style={{ fontSize: 10, fontWeight: 600, color: "var(--txt)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{r.name}</div>
                </div>
                <div style={{ height: 14, background: "var(--brd)", borderRadius: 3, overflow: "hidden", position: "relative" }}>
                  <div style={{ height: "100%", width: `${coverPct}%`, borderRadius: 3, background: urgColor, opacity: 0.7, position: "absolute", top: 0, left: 0 }} />
                  <div style={{ position: "absolute", top: 0, height: "100%", width: 2, background: "#f87171", left: `${reorderPct}%`, zIndex: 2 }} />
                </div>
                <span style={{ ...SG({ fontSize: 9, textAlign: "right", color: urgColor, fontWeight: 700 }) }}>{Math.round(r.daysCover || 0)}</span>
                <span style={{ ...SG({ fontSize: 9, textAlign: "right", color: "var(--txt3)" }) }}>{r.reorderBy ? r.reorderBy.slice(5) : "—"}</span>
              </div>
            );
          })}
        </div>
      </Card>

      {/* ══ SKU COMMAND CENTER & STRANDED ══ */}
      <SecDiv label="SKU Command Center & Stranded Inventory" />
      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", gap: 14, marginBottom: 14 }}>
        {/* ── Main: SKU Table ── */}
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 5, marginBottom: 12, flexWrap: "wrap" }}>
            <span style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".1em", color: "var(--txt3)" }) }}>Risk:</span>
            {[
              { key: "all", label: "All" },
              { key: "critical", label: "Critical", color: "#f87171" },
              { key: "watch", label: "Watch", color: "#F5B731" },
              { key: "low", label: "Healthy", color: "#2ECFAA" },
            ].map(f => (
              <button key={f.key} onClick={() => setSkuFilter(f.key)}
                style={{
                  display: "inline-flex", alignItems: "center", height: 24, padding: "0 9px", borderRadius: 7,
                  ...SG({ fontSize: 9, fontWeight: skuFilter === f.key ? 700 : 600 }),
                  border: `1px solid ${skuFilter === f.key ? "transparent" : f.color ? `${f.color}40` : "var(--brd2)"}`,
                  background: skuFilter === f.key ? "var(--atab, #1a4060)" : "var(--ibg, rgba(255,255,255,.05))",
                  color: skuFilter === f.key ? "#fff" : f.color || "var(--txt3)",
                  cursor: "pointer", whiteSpace: "nowrap",
                }}>{f.label}</button>
            ))}
            <span style={{ ...SG({ fontSize: 9, color: "var(--txt3)" }) }}>{filteredSkus.length} of {data.skus?.length || 0}</span>
            <input
              placeholder="Search ASIN, SKU, title…"
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
              style={{
                height: 24, padding: "0 9px", borderRadius: 7, border: "1px solid var(--brd2)",
                background: "var(--ibg, rgba(255,255,255,.05))", color: "var(--txt)", fontSize: 10,
                outline: "none", width: 200, ...SG(), marginLeft: "auto",
              }}
            />
          </div>
          <Card style={{ overflow: "hidden", marginBottom: 0 }}>
            <div style={{ overflowX: "auto" }}>
              <table className="inv-sku-table" style={{ width: "100%", borderCollapse: "collapse", tableLayout: "fixed", minWidth: 960 }}>
                <thead>
                  <tr>
                    {[
                      { label: "Product", w: 180 },
                      { label: "On Hand", w: 62, r: true },
                      { label: "Inbound", w: 56, r: true },
                      { label: "Days Cvr", w: 56, r: true },
                      { label: "Wks Cover", w: 62, r: true },
                      { label: "Daily Vel", w: 56, r: true },
                      { label: "Buy Box", w: 84 },
                      { label: "Conv %", w: 52, r: true },
                      { label: "Sell-Thru", w: 56, r: true },
                      { label: "Aged 180+", w: 56, r: true },
                      { label: "Ret Rate", w: 56, r: true },
                      { label: "Risk", w: 60, center: true },
                    ].map((col, i) => (
                      <th key={i} style={{
                        ...SG({ fontSize: 8, fontWeight: 700, textTransform: "uppercase", letterSpacing: ".07em", color: "var(--txt3)", padding: "8px 10px", textAlign: col.r ? "right" : col.center ? "center" : "left", borderBottom: "1px solid var(--brd2)", background: "var(--card2)", whiteSpace: "nowrap", width: col.w }),
                      }}>{col.label}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filteredSkus.slice(0, 50).map((s, i) => {
                    const riskColor = s.risk === "critical" ? "#f87171" : s.risk === "watch" ? "#F5B731" : "#2ECFAA";
                    const riskBg = s.risk === "critical" ? "rgba(248,113,113,.15)" : s.risk === "watch" ? "rgba(245,183,49,.14)" : "rgba(46,207,170,.12)";
                    const bbColor = s.buyBox >= 90 ? "#2ECFAA" : s.buyBox >= 80 ? "#F5B731" : s.buyBox > 0 ? "#f87171" : "var(--txt3)";
                    const bbPct = Math.min(100, s.buyBox || 0);
                    return (
                      <tr key={s.asin} style={{ borderBottom: "1px solid var(--brd)" }}>
                        <td style={{ padding: "8px 10px", verticalAlign: "middle" }}>
                          <div style={{ fontWeight: 600, color: "var(--txt)", fontSize: 10, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 180 }}>{s.name}</div>
                          <div style={{ ...SG({ fontSize: 8, color: "#3E658C" }) }}>{s.sku || s.asin}</div>
                        </td>
                        <td style={{ ...SG({ fontSize: 10, color: "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{fmtNum(s.onHand)}</td>
                        <td style={{ ...SG({ fontSize: 10, color: "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{fmtNum(s.inbound)}</td>
                        <td style={{ ...SG({ fontSize: 10, color: s.daysCover != null && s.daysCover < 30 ? "#f87171" : "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>
                          {s.daysCover != null ? Math.round(s.daysCover) : "∞"}
                        </td>
                        <td style={{ ...SG({ fontSize: 10, color: "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{s.weeksCover != null ? s.weeksCover : "∞"}</td>
                        <td style={{ ...SG({ fontSize: 10, color: "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{s.dailyVel}</td>
                        <td style={{ padding: "8px 10px", verticalAlign: "middle" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: 5 }}>
                            <div style={{ width: 44, height: 5, borderRadius: 3, background: "var(--brd)", overflow: "hidden", flexShrink: 0 }}>
                              <div style={{ height: "100%", width: `${bbPct}%`, borderRadius: 3, background: bbColor }} />
                            </div>
                            <span style={{ ...SG({ fontSize: 10, fontWeight: 700, color: bbColor }) }}>{s.buyBox > 0 ? `${s.buyBox}%` : "—"}</span>
                          </div>
                        </td>
                        <td style={{ ...SG({ fontSize: 10, color: "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{s.convPct > 0 ? `${s.convPct}%` : "—"}</td>
                        <td style={{ ...SG({ fontSize: 10, color: "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{s.sellThru != null ? `${s.sellThru}×` : "—"}</td>
                        <td style={{ ...SG({ fontSize: 10, color: s.aged180 > 0 ? "#F5B731" : "var(--txt3)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{s.aged180 > 0 ? fmtNum(s.aged180) : "—"}</td>
                        <td style={{ ...SG({ fontSize: 10, color: "var(--txt2)", padding: "8px 10px", textAlign: "right", verticalAlign: "middle" }) }}>{s.returnRate != null ? `${s.returnRate}%` : "—"}</td>
                        <td style={{ padding: "8px 10px", textAlign: "center", verticalAlign: "middle" }}>
                          <span style={{ ...SG({ display: "inline-flex", alignItems: "center", justifyContent: "center", width: 32, height: 18, borderRadius: 5, fontSize: 9, fontWeight: 800, background: riskBg, color: riskColor }) }}>
                            {s.risk === "critical" ? "HI" : s.risk === "watch" ? "MED" : "LOW"}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Card>
        </div>

        {/* ── Side Panel: Stranded + Reimbursements ── */}
        <div>
          <Card style={{ marginBottom: 14 }}>
            <div style={{ padding: "9px 14px", borderBottom: "1px solid var(--brd)", background: "var(--card2)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: "var(--txt)" }}>Stranded Inventory</span>
              {health.strandedValue > 0 && <Badge type="warn">${fmtK(health.strandedValue)} at risk</Badge>}
            </div>
            {(data.stranded || []).length > 0 ? data.stranded.slice(0, 6).map((s, i) => (
              <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 54px 54px auto", gap: 6, alignItems: "center", padding: "7px 14px", borderBottom: "1px solid var(--brd)" }}>
                <div>
                  <div style={{ fontSize: 10, fontWeight: 600, color: "var(--txt)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{s.name || s.sku}</div>
                  <div style={{ ...SG({ fontSize: 8, color: "#3E658C" }) }}>{s.sku} · {s.qty} units</div>
                </div>
                <span style={{ ...SG({ fontSize: 10, textAlign: "right", fontWeight: 700, color: "#f87171" }) }}>{s.qty}</span>
                <span style={{ ...SG({ fontSize: 9, textAlign: "right", color: "var(--txt3)" }) }}>${s.value >= 1000 ? `${(s.value / 1000).toFixed(1)}K` : s.value}</span>
                <span style={{ ...SG({ fontSize: 8, fontWeight: 700, padding: "2px 6px", borderRadius: 4, background: s.reason?.includes("Listing") ? "rgba(208,48,48,.15)" : s.reason?.includes("Price") ? "rgba(245,183,49,.15)" : "rgba(123,174,208,.15)", color: s.reason?.includes("Listing") ? "#f87171" : s.reason?.includes("Price") ? "#F5B731" : "#7BAED0" }) }}>{s.reason}</span>
              </div>
            )) : (
              <div style={{ padding: "16px 14px", color: "var(--txt3)", ...SG({ fontSize: 10 }) }}>No stranded inventory detected</div>
            )}
          </Card>

          <Card style={{ marginBottom: 0 }}>
            <div style={{ padding: "9px 14px", borderBottom: "1px solid var(--brd)", background: "var(--card2)", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: "var(--txt)" }}>Recent Reimbursements</span>
              {data.reimbursements?.length > 0 && <Badge type="ok">+${data.reimbursements.reduce((s, r) => s + r.amount, 0).toFixed(0)}</Badge>}
            </div>
            {(data.reimbursements || []).length > 0 ? data.reimbursements.slice(0, 5).map((r, i) => (
              <div key={i} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "8px 14px", borderBottom: i < Math.min(data.reimbursements.length, 5) - 1 ? "1px solid var(--brd)" : "none" }}>
                <div>
                  <div style={{ ...SG({ fontSize: 8, color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".07em" }) }}>{r.reason}</div>
                  <div style={{ ...DM({ fontSize: 13, lineHeight: 1, color: "#2ECFAA" }) }}>${r.amount.toFixed(2)}</div>
                  <div style={{ ...SG({ fontSize: 8, color: "var(--txt3)" }) }}>{r.sku} · {r.qty} unit{r.qty !== 1 ? "s" : ""}</div>
                </div>
              </div>
            )) : (
              <div style={{ padding: "16px 14px", color: "var(--txt3)", ...SG({ fontSize: 10 }) }}>No reimbursements in the last 90 days</div>
            )}
          </Card>
        </div>
      </div>

      {/* Create Shipment Modal */}
      {showCreate && <CreateShipmentModal onClose={() => setShowCreate(false)} />}
    </div>
  );
}
