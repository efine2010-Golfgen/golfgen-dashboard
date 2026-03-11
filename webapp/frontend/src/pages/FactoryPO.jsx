import { useState, useEffect, useRef } from "react";
import { api } from "../lib/api";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend } from "recharts";

const fmt$ = n => n == null ? "$0" : "$" + Math.round(n).toLocaleString();
const fmtK = n => n == null ? "0" : n >= 1000 ? (n / 1000).toFixed(1) + "K" : n.toLocaleString();
const COLORS = ["#2ECFAA", "#3E658C", "#E87830", "#F5B731", "#7BAED0", "#D03030", "#22A387", "#9B59B6", "#1ABC9C", "#E74C3C", "#3498DB"];

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
      await api.uploadFactoryPO(file);
      load();
    } catch (err) { alert("Upload failed: " + err.message); }
    setUploading(false);
    if (fileRef.current) fileRef.current.value = "";
  };

  if (loading) return <div className="loading"><div className="spinner" /> Loading Factory PO data...</div>;
  if (!data) return <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>No Factory PO data. Upload an Excel file to begin.</div>;

  const pos = data.purchaseOrders || [];
  const skus = data.unitsByItem || [];
  const arrivals = data.arrivalSchedule || [];

  // KPI calculations
  const totalUnits = pos.reduce((s, p) => s + (p.units || 0), 0);
  const totalCost = pos.reduce((s, p) => s + (p.totalCost || 0), 0);
  const totalLanded = pos.reduce((s, p) => s + (p.landedCost || 0), 0);
  const totalCBM = pos.reduce((s, p) => s + (p.cbm || 0), 0);
  const activePOs = pos.filter(p => p.units > 0 && p.paymentTerms !== "CANCELLED").length;
  const cancelledPOs = pos.filter(p => p.paymentTerms === "CANCELLED").length;
  const avgUnitCost = totalUnits > 0 ? totalCost / totalUnits : 0;
  const avgLandedCost = totalUnits > 0 ? totalLanded / totalUnits : 0;

  // Factory breakdown for pie chart
  const factoryMap = {};
  pos.forEach(p => {
    if (p.units <= 0) return;
    const name = p.factory.length > 20 ? p.factory.split(" ").slice(0, 3).join(" ") : p.factory;
    factoryMap[name] = (factoryMap[name] || 0) + p.units;
  });
  const factoryPie = Object.entries(factoryMap).map(([name, value]) => ({ name, value }));

  // Arrival timeline for bar chart
  const arrivalBar = [
    { month: "Jan", units: arrivals.reduce((s, a) => s + (a.jan2026 || 0), 0) },
    { month: "Feb", units: arrivals.reduce((s, a) => s + (a.feb2026 || 0), 0) },
    { month: "Mar", units: arrivals.reduce((s, a) => s + (a.mar2026 || 0), 0) },
    { month: "May", units: arrivals.reduce((s, a) => s + (a.may2026 || 0), 0) },
    { month: "Jun", units: arrivals.reduce((s, a) => s + (a.jun2026 || 0), 0) },
  ].filter(m => m.units > 0);

  // Top SKUs by total units
  const topSkus = [...skus].sort((a, b) => (b.total || 0) - (a.total || 0)).slice(0, 10);

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
        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          <input type="file" accept=".xlsx,.xls" ref={fileRef} onChange={handleUpload} style={{ display: "none" }} />
          <button onClick={() => fileRef.current?.click()} disabled={uploading}
            style={{ padding: "8px 20px", background: "var(--teal)", color: "#fff", border: "none", borderRadius: 8, fontWeight: 600, fontSize: 13, cursor: "pointer", opacity: uploading ? 0.6 : 1 }}>
            {uploading ? "Uploading..." : "📤 Upload Update"}
          </button>
        </div>
      </div>

      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12, marginBottom: 24 }}>
        {[
          { label: "Active POs", value: activePOs, sub: cancelledPOs > 0 ? `${cancelledPOs} cancelled` : null },
          { label: "Total Units", value: fmtK(totalUnits) },
          { label: "Total FOB Cost", value: fmt$(totalCost) },
          { label: "Total Landed", value: fmt$(totalLanded) },
          { label: "Avg Unit Cost", value: "$" + avgUnitCost.toFixed(2) },
          { label: "Avg Landed/Unit", value: "$" + avgLandedCost.toFixed(2) },
          { label: "Total CBM", value: totalCBM.toFixed(0) },
          { label: "SKUs Ordered", value: skus.length },
        ].map((kpi, i) => (
          <div key={i} style={{ background: "#fff", borderRadius: 10, padding: "14px 16px", boxShadow: "var(--card-shadow)", borderLeft: "3px solid " + COLORS[i % COLORS.length] }}>
            <div style={{ fontSize: 11, color: "var(--muted)", fontWeight: 600, letterSpacing: 0.5 }}>{kpi.label}</div>
            <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginTop: 2 }}>{kpi.value}</div>
            {kpi.sub && <div style={{ fontSize: 10, color: "var(--orange)", marginTop: 2 }}>{kpi.sub}</div>}
          </div>
        ))}
      </div>

      {/* Charts Row */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 24 }}>
        {/* Arrival Timeline */}
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "var(--card-shadow)" }}>
          <h3 style={{ fontSize: 14, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 16 }}>Estimated Arrival Timeline</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={arrivalBar} margin={{ left: -10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis dataKey="month" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 11 }} tickFormatter={v => fmtK(v)} />
              <Tooltip formatter={v => [v.toLocaleString() + " units", "Units"]} />
              <Bar dataKey="units" fill="var(--teal)" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Factory Mix */}
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "var(--card-shadow)" }}>
          <h3 style={{ fontSize: 14, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 16 }}>Units by Factory</h3>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={factoryPie} dataKey="value" nameKey="name" cx="50%" cy="50%" innerRadius={45} outerRadius={80}
                label={({ name, percent }) => `${name.split(" ")[0]} ${(percent * 100).toFixed(0)}%`} labelLine={false} style={{ fontSize: 10 }}>
                {factoryPie.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
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
                <th style={th}>PO #</th><th style={th}>Factory</th><th style={th}>Terms</th>
                <th style={{...th, textAlign: "right"}}>Units</th><th style={{...th, textAlign: "right"}}>FOB Cost</th>
                <th style={th}>FOB Date</th><th style={th}>Est. Arrival</th>
                <th style={{...th, textAlign: "right"}}>CBM</th><th style={{...th, textAlign: "right"}}>Landed Cost</th>
              </tr>
            </thead>
            <tbody>
              {pos.filter(p => p.paymentTerms !== "CANCELLED").map((po, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                  <td style={{...td, fontWeight: 700, color: "var(--blue)"}}>{po.poNumber}</td>
                  <td style={{...td, fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{po.factory}</td>
                  <td style={{...td, fontSize: 10, color: "var(--muted)"}}>{po.paymentTerms}</td>
                  <td style={{...td, textAlign: "right", fontWeight: 600}}>{(po.units || 0).toLocaleString()}</td>
                  <td style={{...td, textAlign: "right"}}>{fmt$(po.totalCost)}</td>
                  <td style={{...td, fontSize: 11}}>{po.fobDate || "—"}</td>
                  <td style={{...td, fontSize: 11}}>{po.estArrival || "—"}</td>
                  <td style={{...td, textAlign: "right"}}>{(po.cbm || 0).toFixed(1)}</td>
                  <td style={{...td, textAlign: "right", fontWeight: 600, color: "var(--teal-dark)"}}>{fmt$(po.landedCost)}</td>
                </tr>
              ))}
              <tr style={{ background: "var(--teal-bg)", fontWeight: 700 }}>
                <td style={td} colSpan={3}>TOTAL</td>
                <td style={{...td, textAlign: "right"}}>{totalUnits.toLocaleString()}</td>
                <td style={{...td, textAlign: "right"}}>{fmt$(totalCost)}</td>
                <td style={td}></td><td style={td}></td>
                <td style={{...td, textAlign: "right"}}>{totalCBM.toFixed(1)}</td>
                <td style={{...td, textAlign: "right", color: "var(--teal-dark)"}}>{fmt$(totalLanded)}</td>
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
                <th style={th}>SKU</th><th style={th}>Description</th>
                {Object.keys(skus[0]?.byPO || {}).map(po => <th key={po} style={{...th, textAlign: "right", fontSize: 10}}>{po}</th>)}
                <th style={{...th, textAlign: "right"}}>Total</th>
              </tr>
            </thead>
            <tbody>
              {skus.map((row, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                  <td style={{...td, fontWeight: 600, fontSize: 10, fontFamily: "monospace"}}>{row.sku}</td>
                  <td style={{...td, fontSize: 10, maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{row.description}</td>
                  {Object.values(row.byPO || {}).map((v, j) => (
                    <td key={j} style={{...td, textAlign: "right", color: v > 0 ? "var(--body-text)" : "#ddd", fontSize: 10}}>{v > 0 ? v.toLocaleString() : "—"}</td>
                  ))}
                  <td style={{...td, textAlign: "right", fontWeight: 700}}>{(row.total || 0).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Arrival Schedule Table */}
      {view === "arrival" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "hidden", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>SKU</th><th style={th}>Description</th>
                <th style={{...th, textAlign: "right"}}>Jan</th><th style={{...th, textAlign: "right"}}>Feb</th>
                <th style={{...th, textAlign: "right"}}>Mar</th><th style={{...th, textAlign: "right"}}>May</th>
                <th style={{...th, textAlign: "right"}}>Jun</th><th style={{...th, textAlign: "right"}}>Total</th>
              </tr>
            </thead>
            <tbody>
              {arrivals.map((row, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                  <td style={{...td, fontWeight: 600, fontSize: 11, fontFamily: "monospace"}}>{row.sku}</td>
                  <td style={{...td, fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{row.description}</td>
                  {["jan2026", "feb2026", "mar2026", "may2026", "jun2026"].map(m => (
                    <td key={m} style={{...td, textAlign: "right", color: (row[m] || 0) > 0 ? "var(--body-text)" : "#ddd"}}>{(row[m] || 0) > 0 ? (row[m]).toLocaleString() : "—"}</td>
                  ))}
                  <td style={{...td, textAlign: "right", fontWeight: 700}}>{(row.total || 0).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const th = { padding: "10px 12px", textAlign: "left", fontWeight: 600, fontSize: 11, letterSpacing: 0.3 };
const td = { padding: "8px 12px" };
