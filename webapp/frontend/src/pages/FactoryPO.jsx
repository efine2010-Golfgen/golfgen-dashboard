import { useState, useEffect, useRef, useMemo } from "react";
import { api } from "../lib/api";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

const fmtK = n => n == null ? "0" : n >= 1000 ? (n / 1000).toFixed(1) + "K" : n.toLocaleString();
const COLORS = ["#2ECFAA", "#3E658C", "#E87830", "#F5B731", "#7BAED0", "#D03030", "#22A387", "#9B59B6", "#1ABC9C", "#E74C3C", "#3498DB"];

// Known Amazon and Walmart SKU sets for channel classification
const WALMART_PREFIXES = ["GGWMSS", "GGRTFT"];
const AMAZON_PREFIXES = ["GGLFT", "GGLTN", "GGRTN"];

function classifySKU(sku) {
  if (!sku) return "Other";
  const s = sku.toUpperCase();
  const isWalmart = WALMART_PREFIXES.some(p => s.startsWith(p));
  const isAmazon = AMAZON_PREFIXES.some(p => s.startsWith(p));
  if (isWalmart && isAmazon) return "Both";
  if (isWalmart) return "Walmart";
  if (isAmazon) return "Amazon";
  if (s.startsWith("KIT")) return "Other";
  return "Other";
}

function classifyCategory(sku, desc) {
  if (!sku && !desc) return "Other";
  const combined = ((sku || "") + " " + (desc || "")).toUpperCase();
  if (combined.includes("CLUB SET") || combined.includes("SS2")) return "Club Sets";
  if (combined.includes("JUNIOR") || combined.includes("FT") && combined.includes("SET")) return "Junior Sets";
  if (combined.includes("NET") || combined.includes("NW")) return "Nets & Training";
  if (combined.includes("KIT")) return "Kits";
  if (combined.includes("IRON") || combined.includes("DRIVER") || combined.includes("PUTTER") || combined.includes("WEDGE")) return "Individual Clubs";
  return "Golf Accessories";
}

export default function FactoryPO() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [view, setView] = useState("summary"); // summary | byItem | arrival
  const fileRef = useRef();

  const load = () => {
    setLoading(true);
    api.factoryPO().then(d => { setData(d); setLoading(false); }).catch(() => setLoading(false));
  };
  useEffect(load, []);

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      const res = await api.uploadSupplyChain(file);
      if (res.factoryPO) load();
      else alert("No 'Factory PO Summary' sheet found in file.");
    } catch (err) { alert("Upload failed: " + err.message); }
    setUploading(false);
    if (fileRef.current) fileRef.current.value = "";
  };

  if (loading) return <div className="loading"><div className="spinner" /> Loading Factory PO data...</div>;
  if (!data) return <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>No Factory PO data. Upload an Excel file to begin.</div>;

  const pos = (data.purchaseOrders || []).filter(p => {
    // Filter out garbage rows from payment calendar
    const po = (p.poNumber || "").toString();
    if (po.includes(".") || po.length > 10) return false;
    if ((p.factory || "").startsWith("T-") || (p.factory || "").startsWith("TOTAL")) return false;
    return true;
  });
  const skus = (data.unitsByItem || []).filter(u => {
    const s = (u.sku || "").toUpperCase();
    return s && s !== "TOTAL" && s !== "TOTALS" && s !== "SKU";
  });
  const arrivals = (data.arrivalSchedule || []).filter(a => {
    const s = (a.sku || "").toUpperCase();
    return s && s !== "TOTAL" && s !== "TOTALS" && s !== "SKU";
  });

  // KPI calculations
  const totalUnits = pos.reduce((s, p) => s + (p.units || 0), 0);
  const totalCBM = pos.reduce((s, p) => s + (p.cbm || 0), 0);
  const activePOs = pos.filter(p => (p.units || 0) > 0).length;

  // Channel breakdown from unitsByItem
  const channelUnits = { Amazon: 0, Walmart: 0, Other: 0 };
  skus.forEach(u => {
    const ch = classifySKU(u.sku);
    const qty = u.total || u.quantity || 0;
    if (ch === "Both") { channelUnits.Amazon += qty; channelUnits.Walmart += qty; }
    else if (channelUnits[ch] !== undefined) channelUnits[ch] += qty;
    else channelUnits.Other += qty;
  });

  // Category breakdown
  const categoryMap = {};
  skus.forEach(u => {
    const cat = classifyCategory(u.sku, u.description);
    const qty = u.total || u.quantity || 0;
    if (!categoryMap[cat]) categoryMap[cat] = 0;
    categoryMap[cat] += qty;
  });
  const categoryPie = Object.entries(categoryMap).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value);

  // Factory breakdown for pie chart
  const factoryMap = {};
  pos.forEach(p => {
    if ((p.units || 0) <= 0) return;
    const name = p.factory.length > 20 ? p.factory.split(" ").slice(0, 3).join(" ") : p.factory;
    factoryMap[name] = (factoryMap[name] || 0) + p.units;
  });
  const factoryPie = Object.entries(factoryMap).map(([name, value]) => ({ name, value }));

  // Arrival timeline from arrivalSchedule data
  const monthTotals = {};
  arrivals.forEach(a => {
    const monthly = a.monthlyArrivals || {};
    Object.entries(monthly).forEach(([month, units]) => {
      if (units > 0) {
        monthTotals[month] = (monthTotals[month] || 0) + units;
      }
    });
  });
  const arrivalBar = Object.entries(monthTotals)
    .map(([month, units]) => ({ month, units }))
    .sort((a, b) => {
      // Sort by date order
      const months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
      const aIdx = months.findIndex(m => a.month.toLowerCase().startsWith(m));
      const bIdx = months.findIndex(m => b.month.toLowerCase().startsWith(m));
      return aIdx - bIdx;
    });

  // Get all unique month columns from arrival data
  const allMonthCols = useMemo(() => {
    const months = new Set();
    arrivals.forEach(a => {
      Object.keys(a.monthlyArrivals || {}).forEach(m => months.add(m));
    });
    const order = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
    return [...months].sort((a, b) => {
      const ai = order.findIndex(m => a.toLowerCase().startsWith(m));
      const bi = order.findIndex(m => b.toLowerCase().startsWith(m));
      return ai - bi;
    });
  }, [arrivals]);

  // Get all PO columns from unitsByItem
  const allPOCols = useMemo(() => {
    const poCols = new Set();
    skus.forEach(u => Object.keys(u.byPO || {}).forEach(k => poCols.add(k)));
    return [...poCols].sort();
  }, [skus]);

  return (
    <div>
      {/* Header with upload */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: 22, color: "var(--navy)", margin: 0 }}>
            Factory Purchase Order Summary
          </h2>
          <p style={{ fontSize: 12, color: "var(--muted)", marginTop: 4 }}>
            {data.lastUpload ? `Last updated: ${data.lastUpload}` : "No upload date"}{data.sourceFile ? ` • ${data.sourceFile}` : ""}
          </p>
        </div>
        <div>
          <input type="file" accept=".xlsx,.xls" ref={fileRef} onChange={handleUpload} style={{ display: "none" }} />
          <button onClick={() => fileRef.current?.click()} disabled={uploading}
            style={{ padding: "8px 20px", background: "var(--teal)", color: "#fff", border: "none", borderRadius: 8, fontWeight: 600, fontSize: 13, cursor: "pointer", opacity: uploading ? 0.6 : 1 }}>
            {uploading ? "Uploading..." : "📤 Upload Update"}
          </button>
        </div>
      </div>

      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(155px, 1fr))", gap: 12, marginBottom: 24 }}>
        {[
          { label: "Active POs", value: activePOs, color: "#3E658C" },
          { label: "Total Units On Order", value: fmtK(totalUnits), color: "#2ECFAA" },
          { label: "Amazon On Order", value: fmtK(channelUnits.Amazon), color: "#E87830" },
          { label: "Walmart On Order", value: fmtK(channelUnits.Walmart), color: "#F5B731" },
          { label: "Total CBM", value: totalCBM.toFixed(0), color: "#7BAED0" },
          { label: "SKUs Ordered", value: skus.length, color: "#22A387" },
        ].map((kpi, i) => (
          <div key={i} style={{ background: "#fff", borderRadius: 10, padding: "14px 16px", boxShadow: "var(--card-shadow)", borderLeft: `3px solid ${kpi.color}` }}>
            <div style={{ fontSize: 11, color: "var(--muted)", fontWeight: 600, letterSpacing: 0.5 }}>{kpi.label}</div>
            <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginTop: 2 }}>{kpi.value}</div>
            {kpi.sub && <div style={{ fontSize: 10, color: kpi.color, marginTop: 2 }}>{kpi.sub}</div>}
          </div>
        ))}
      </div>

      {/* Charts Row */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
        {/* Arrival Timeline */}
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "var(--card-shadow)" }}>
          <h3 style={{ fontSize: 14, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 16 }}>Estimated Arrival Timeline</h3>
          {arrivalBar.length > 0 ? (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={arrivalBar} margin={{ left: -10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis dataKey="month" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} tickFormatter={v => fmtK(v)} />
                <Tooltip formatter={v => [v.toLocaleString() + " units", "Units"]} />
                <Bar dataKey="units" fill="var(--teal)" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ height: 220, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--muted)", fontSize: 13 }}>
              No arrival data available. Re-upload Excel to populate.
            </div>
          )}
        </div>

        {/* Units by Category */}
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "var(--card-shadow)" }}>
          <h3 style={{ fontSize: 14, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 16 }}>Units by Product Category</h3>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={categoryPie} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={45} outerRadius={80}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`} labelLine={false} style={{ fontSize: 10 }}>
                {categoryPie.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip formatter={v => [v.toLocaleString() + " units", "Units"]} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* View Toggle */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {[["summary", "PO Summary"], ["byItem", "Units by Item"], ["arrival", "Arrival Schedule"]].map(([k, l]) => (
          <button key={k} onClick={() => setView(k)}
            style={{ padding: "6px 16px", borderRadius: 6, border: "1px solid " + (view === k ? "var(--teal)" : "var(--border)"),
              background: view === k ? "var(--teal)" : "#fff", color: view === k ? "#fff" : "var(--body-text)",
              fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
            {l}
          </button>
        ))}
      </div>

      {/* PO Summary Table */}
      {view === "summary" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "hidden", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>PO #</th><th style={th}>Factory</th>
                <th style={{...th, textAlign: "right"}}>Units</th>
                <th style={th}>Est. Arrival</th>
                <th style={{...th, textAlign: "right"}}>CBM</th>
              </tr>
            </thead>
            <tbody>
              {pos.filter(p => (p.units || 0) > 0).map((po, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                  <td style={{...td, fontWeight: 700, color: "var(--blue)"}}>{po.poNumber}</td>
                  <td style={{...td, fontSize: 11, maxWidth: 250, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{po.factory}</td>
                  <td style={{...td, textAlign: "right", fontWeight: 600}}>{(po.units || 0).toLocaleString()}</td>
                  <td style={{...td, fontSize: 11}}>{po.estArrival || "—"}</td>
                  <td style={{...td, textAlign: "right"}}>{(po.cbm || 0).toFixed(1)}</td>
                </tr>
              ))}
              <tr style={{ background: "var(--teal-bg)", fontWeight: 700 }}>
                <td style={td} colSpan={2}>TOTAL</td>
                <td style={{...td, textAlign: "right"}}>{totalUnits.toLocaleString()}</td>
                <td style={td}></td>
                <td style={{...td, textAlign: "right"}}>{totalCBM.toFixed(1)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

      {/* Units by Item Table */}
      {view === "byItem" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "auto", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>SKU</th><th style={th}>Description</th><th style={th}>Channel</th>
                {allPOCols.map(po => <th key={po} style={{...th, textAlign: "right", fontSize: 10}}>{po}</th>)}
                <th style={{...th, textAlign: "right"}}>Total</th>
              </tr>
            </thead>
            <tbody>
              {skus.map((row, i) => {
                const channel = classifySKU(row.sku);
                const total = row.total || row.quantity || 0;
                return (
                  <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                    <td style={{...td, fontWeight: 600, fontSize: 10, fontFamily: "monospace"}}>{row.sku}</td>
                    <td style={{...td, fontSize: 10, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{row.description}</td>
                    <td style={td}>
                      <span style={{ display: "inline-block", padding: "1px 8px", borderRadius: 10, fontSize: 9, fontWeight: 700,
                        background: channel === "Amazon" ? "#FF990022" : channel === "Walmart" ? "#0071CE22" : "#eee",
                        color: channel === "Amazon" ? "#E87830" : channel === "Walmart" ? "#0071CE" : "#999" }}>
                        {channel}
                      </span>
                    </td>
                    {allPOCols.map(po => {
                      const v = (row.byPO || {})[po] || 0;
                      return <td key={po} style={{...td, textAlign: "right", color: v > 0 ? "var(--body-text)" : "#ddd", fontSize: 10}}>{v > 0 ? v.toLocaleString() : "—"}</td>;
                    })}
                    <td style={{...td, textAlign: "right", fontWeight: 700}}>{total.toLocaleString()}</td>
                  </tr>
                );
              })}
              <tr style={{ background: "var(--teal-bg)", fontWeight: 700 }}>
                <td style={td} colSpan={3 + allPOCols.length}>TOTAL</td>
                <td style={{...td, textAlign: "right"}}>{skus.reduce((s, r) => s + (r.total || r.quantity || 0), 0).toLocaleString()}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

      {/* Arrival Schedule Table */}
      {view === "arrival" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "auto", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>SKU</th><th style={th}>Description</th><th style={th}>Channel</th>
                {allMonthCols.map(m => <th key={m} style={{...th, textAlign: "right", fontSize: 10}}>{m}</th>)}
                <th style={{...th, textAlign: "right"}}>Total</th>
              </tr>
            </thead>
            <tbody>
              {arrivals.map((row, i) => {
                const channel = classifySKU(row.sku);
                return (
                  <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                    <td style={{...td, fontWeight: 600, fontSize: 11, fontFamily: "monospace"}}>{row.sku}</td>
                    <td style={{...td, fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{row.description}</td>
                    <td style={td}>
                      <span style={{ display: "inline-block", padding: "1px 8px", borderRadius: 10, fontSize: 9, fontWeight: 700,
                        background: channel === "Amazon" ? "#FF990022" : channel === "Walmart" ? "#0071CE22" : "#eee",
                        color: channel === "Amazon" ? "#E87830" : channel === "Walmart" ? "#0071CE" : "#999" }}>
                        {channel}
                      </span>
                    </td>
                    {allMonthCols.map(m => {
                      const v = (row.monthlyArrivals || {})[m] || 0;
                      return <td key={m} style={{...td, textAlign: "right", color: v > 0 ? "var(--body-text)" : "#ddd"}}>{v > 0 ? v.toLocaleString() : "—"}</td>;
                    })}
                    <td style={{...td, textAlign: "right", fontWeight: 700}}>{(row.total || 0).toLocaleString()}</td>
                  </tr>
                );
              })}
              <tr style={{ background: "var(--teal-bg)", fontWeight: 700 }}>
                <td style={td} colSpan={3}>TOTAL</td>
                {allMonthCols.map(m => {
                  const colTotal = arrivals.reduce((s, r) => s + ((r.monthlyArrivals || {})[m] || 0), 0);
                  return <td key={m} style={{...td, textAlign: "right"}}>{colTotal > 0 ? colTotal.toLocaleString() : "—"}</td>;
                })}
                <td style={{...td, textAlign: "right"}}>{arrivals.reduce((s, r) => s + (r.total || 0), 0).toLocaleString()}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const th = { padding: "10px 12px", textAlign: "left", fontWeight: 600, fontSize: 11, letterSpacing: 0.3 };
const td = { padding: "8px 12px" };
