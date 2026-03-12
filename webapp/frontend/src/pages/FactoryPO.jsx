import { useState, useEffect, useRef, useMemo } from "react";
import { api } from "../lib/api";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

const fmtK = n => n == null ? "0" : n >= 1000 ? (n / 1000).toFixed(1) + "K" : n.toLocaleString();
const COLORS = ["#2ECFAA", "#3E658C", "#E87830", "#F5B731", "#7BAED0", "#D03030", "#22A387", "#9B59B6", "#1ABC9C", "#E74C3C", "#3498DB"];

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
  if (combined.includes("JUNIOR") || (combined.includes("FT") && combined.includes("SET"))) return "Junior Sets";
  if (combined.includes("NET") || combined.includes("NW")) return "Nets & Training";
  if (combined.includes("KIT")) return "Kits";
  if (combined.includes("IRON") || combined.includes("DRIVER") || combined.includes("PUTTER") || combined.includes("WEDGE")) return "Individual Clubs";
  return "Golf Accessories";
}

const STATUS_BADGE = (status) => {
  if (!status) return <span style={{ color: "#C4CDD0", fontSize: 10 }}>—</span>;
  const s = (status || "").toUpperCase();
  const bg = s.includes("DELIVER") ? "#22A387" : s.includes("TRANSIT") ? "#3E658C" : s.includes("PENDING") ? "#F5B731" : "#C4CDD0";
  return <span style={{ display: "inline-block", padding: "2px 10px", borderRadius: 12, background: bg, color: "#fff", fontSize: 10, fontWeight: 700, letterSpacing: 0.5 }}>{status}</span>;
};

export default function FactoryPO() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [view, setView] = useState("summary");
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

  // All derived data via useMemo — MUST be before any early returns
  const pos = useMemo(() => {
    return (data?.purchaseOrders || []).filter(p => {
      const po = (p.poNumber || "").toString();
      if (po.includes(".") || po.length > 10) return false;
      if ((p.factory || "").startsWith("T-") || (p.factory || "").startsWith("TOTAL")) return false;
      if ((p.units || 0) <= 0) return false;
      return true;
    });
  }, [data]);

  const skus = useMemo(() => {
    return (data?.unitsByItem || []).filter(u => {
      const s = (u.sku || "").toUpperCase();
      return s && s !== "TOTAL" && s !== "TOTALS" && s !== "SKU";
    });
  }, [data]);

  const arrivals = useMemo(() => {
    return (data?.arrivalSchedule || []).filter(a => {
      const s = (a.sku || "").toUpperCase();
      return s && s !== "TOTAL" && s !== "TOTALS" && s !== "SKU";
    });
  }, [data]);

  // Merge SKU data with arrival data for the combined view
  const combinedItems = useMemo(() => {
    const arrivalMap = {};
    arrivals.forEach(a => { arrivalMap[a.sku] = a; });
    // Start from skus list; add arrival info where available
    const items = skus.map(u => {
      const arr = arrivalMap[u.sku] || {};
      return { ...u, monthlyArrivals: arr.monthlyArrivals || {}, arrivalTotal: arr.total || 0 };
    });
    // Add any SKUs that appear only in arrivals but not in skus
    arrivals.forEach(a => {
      if (!skus.find(u => u.sku === a.sku)) {
        items.push({ sku: a.sku, description: a.description, byPO: {}, total: 0,
          monthlyArrivals: a.monthlyArrivals || {}, arrivalTotal: a.total || 0 });
      }
    });
    return items;
  }, [skus, arrivals]);

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

  const allPOCols = useMemo(() => {
    const poCols = new Set();
    skus.forEach(u => Object.keys(u.byPO || {}).forEach(k => poCols.add(k)));
    return [...poCols].sort();
  }, [skus]);

  const channelUnits = useMemo(() => {
    const ch = { Amazon: 0, Walmart: 0, Other: 0 };
    skus.forEach(u => {
      const c = classifySKU(u.sku);
      const qty = u.total || u.quantity || 0;
      if (c === "Both") { ch.Amazon += qty; ch.Walmart += qty; }
      else if (ch[c] !== undefined) ch[c] += qty;
      else ch.Other += qty;
    });
    return ch;
  }, [skus]);

  const categoryPie = useMemo(() => {
    const m = {};
    skus.forEach(u => {
      const cat = classifyCategory(u.sku, u.description);
      const qty = u.total || u.quantity || 0;
      m[cat] = (m[cat] || 0) + qty;
    });
    return Object.entries(m).map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value);
  }, [skus]);

  const arrivalBar = useMemo(() => {
    const monthTotals = {};
    arrivals.forEach(a => {
      const monthly = a.monthlyArrivals || {};
      Object.entries(monthly).forEach(([month, units]) => {
        if (units > 0) monthTotals[month] = (monthTotals[month] || 0) + units;
      });
    });
    const order = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
    return Object.entries(monthTotals)
      .map(([month, units]) => ({ month, units }))
      .sort((a, b) => {
        const ai = order.findIndex(m => a.month.toLowerCase().startsWith(m));
        const bi = order.findIndex(m => b.month.toLowerCase().startsWith(m));
        return ai - bi;
      });
  }, [arrivals]);

  // Now safe for early returns
  if (loading) return <div className="loading"><div className="spinner" /> Loading Factory PO data...</div>;
  if (!data) return <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>No Factory PO data. Upload an Excel file to begin.</div>;

  const totalUnits = pos.reduce((s, p) => s + (p.units || 0), 0);
  const totalCBM = pos.reduce((s, p) => s + (p.cbm || 0), 0);
  const activePOs = pos.length;

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
          { label: "Total CBM", value: (totalCBM || 0).toFixed(0), color: "#7BAED0" },
          { label: "SKUs Ordered", value: skus.length, color: "#22A387" },
        ].map((kpi, i) => (
          <div key={i} style={{ background: "#fff", borderRadius: 10, padding: "14px 16px", boxShadow: "var(--card-shadow)", borderLeft: `3px solid ${kpi.color}` }}>
            <div style={{ fontSize: 11, color: "var(--muted)", fontWeight: 600, letterSpacing: 0.5 }}>{kpi.label}</div>
            <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginTop: 2 }}>{kpi.value}</div>
          </div>
        ))}
      </div>

      {/* Charts Row */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
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
              No arrival data available
            </div>
          )}
        </div>

        <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "var(--card-shadow)" }}>
          <h3 style={{ fontSize: 14, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 16 }}>Units by Product Category</h3>
          {categoryPie.length > 0 ? (
            <ResponsiveContainer width="100%" height={220}>
              <PieChart>
                <Pie data={categoryPie} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={45} outerRadius={80}
                  label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`} labelLine={false} style={{ fontSize: 10 }}>
                  {categoryPie.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                </Pie>
                <Tooltip formatter={v => [v.toLocaleString() + " units", "Units"]} />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ height: 220, display: "flex", alignItems: "center", justifyContent: "center", color: "var(--muted)", fontSize: 13 }}>
              No SKU data available
            </div>
          )}
        </div>
      </div>

      {/* View Toggle — now 2 tabs: PO Summary + Units & Arrivals by Item */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {[["summary", "PO Summary"], ["byItem", "Units & Arrivals by Item"]].map(([k, l]) => (
          <button key={k} onClick={() => setView(k)}
            style={{ padding: "6px 16px", borderRadius: 6, border: "1px solid " + (view === k ? "var(--teal)" : "var(--border)"),
              background: view === k ? "var(--teal)" : "#fff", color: view === k ? "#fff" : "var(--body-text)",
              fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
            {l}
          </button>
        ))}
      </div>

      {/* PO Summary Table — with Container(s), Arrival Date, Status */}
      {view === "summary" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "auto", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>PO #</th>
                <th style={th}>Factory</th>
                <th style={{...th, textAlign: "right"}}>Units</th>
                <th style={th}>Container(s)</th>
                <th style={th}>Est. Arrival</th>
                <th style={th}>Status</th>
                <th style={{...th, textAlign: "right"}}>CBM</th>
              </tr>
            </thead>
            <tbody>
              {pos.map((po, i) => {
                const containers = po.containers || [];
                const containerStr = containers.length > 0
                  ? containers.map(c => c.containerNumber).join(", ")
                  : "—";
                return (
                  <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                    <td style={{...td, fontWeight: 700, color: "var(--blue)"}}>{po.poNumber}</td>
                    <td style={{...td, fontSize: 11, maxWidth: 250, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{po.factory}</td>
                    <td style={{...td, textAlign: "right", fontWeight: 600}}>{(po.units || 0).toLocaleString()}</td>
                    <td style={{...td, fontFamily: "monospace", fontSize: 10, maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{containerStr}</td>
                    <td style={{...td, fontSize: 11}}>{po.estArrival || "—"}</td>
                    <td style={td}>{STATUS_BADGE(po.containerStatus)}</td>
                    <td style={{...td, textAlign: "right"}}>{(po.cbm || 0).toFixed(1)}</td>
                  </tr>
                );
              })}
              <tr style={{ background: "var(--teal-bg)", fontWeight: 700 }}>
                <td style={td} colSpan={2}>TOTAL</td>
                <td style={{...td, textAlign: "right"}}>{totalUnits.toLocaleString()}</td>
                <td style={td}></td>
                <td style={td}></td>
                <td style={td}></td>
                <td style={{...td, textAlign: "right"}}>{(totalCBM || 0).toFixed(1)}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

      {/* Combined Units & Arrivals by Item Table */}
      {view === "byItem" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "auto", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, minWidth: 900 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>SKU</th>
                <th style={th}>Description</th>
                <th style={th}>Channel</th>
                <th style={th}>Container #</th>
                <th style={th}>Arrival Date</th>
                <th style={{...th, textAlign: "right", borderLeft: "2px solid rgba(255,255,255,0.15)"}}>Total Ordered</th>
                {allMonthCols.length > 0 && allMonthCols.map(m => (
                  <th key={m} style={{...th, textAlign: "right", fontSize: 10}}>{m}</th>
                ))}
                {allMonthCols.length > 0 && (
                  <th style={{...th, textAlign: "right", borderLeft: "2px solid rgba(255,255,255,0.15)"}}>Arrival Total</th>
                )}
              </tr>
            </thead>
            <tbody>
              {combinedItems.map((row, i) => {
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
                    <td style={{...td, fontFamily: "monospace", fontSize: 10}}>{row.containerNumber || "—"}</td>
                    <td style={{...td, fontSize: 10, color: row.arrivalDate ? "#22A387" : "var(--muted)"}}>{row.arrivalDate || "—"}</td>
                    <td style={{...td, textAlign: "right", fontWeight: 700, borderLeft: "2px solid var(--border)"}}>{total > 0 ? total.toLocaleString() : "—"}</td>
                    {allMonthCols.map(m => {
                      const v = (row.monthlyArrivals || {})[m] || 0;
                      return <td key={m} style={{...td, textAlign: "right", color: v > 0 ? "var(--body-text)" : "#ddd"}}>{v > 0 ? v.toLocaleString() : "—"}</td>;
                    })}
                    {allMonthCols.length > 0 && (
                      <td style={{...td, textAlign: "right", fontWeight: 700, borderLeft: "2px solid var(--border)"}}>{(row.arrivalTotal || 0) > 0 ? row.arrivalTotal.toLocaleString() : "—"}</td>
                    )}
                  </tr>
                );
              })}
              <tr style={{ background: "var(--teal-bg)", fontWeight: 700 }}>
                <td style={td} colSpan={5}>TOTAL</td>
                <td style={{...td, textAlign: "right", borderLeft: "2px solid var(--border)"}}>{combinedItems.reduce((s, r) => s + (r.total || r.quantity || 0), 0).toLocaleString()}</td>
                {allMonthCols.map(m => {
                  const colTotal = combinedItems.reduce((s, r) => s + ((r.monthlyArrivals || {})[m] || 0), 0);
                  return <td key={m} style={{...td, textAlign: "right"}}>{colTotal > 0 ? colTotal.toLocaleString() : "—"}</td>;
                })}
                {allMonthCols.length > 0 && (
                  <td style={{...td, textAlign: "right", borderLeft: "2px solid var(--border)"}}>{combinedItems.reduce((s, r) => s + (r.arrivalTotal || 0), 0).toLocaleString()}</td>
                )}
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
