import { useState, useEffect, useRef, useMemo } from "react";
import { api } from "../lib/api";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

const fmtK = n => n == null ? "0" : n >= 1000 ? (n / 1000).toFixed(1) + "K" : n.toLocaleString();
const COLORS = ["#22A387", "#3E658C", "#E87830", "#F5B731", "#7BAED0", "#D03030", "#2ECFAA", "#9B59B6"];

const STATUS_COLORS = { "DELIVERED": "#22A387", "IN TRANSIT": "#3E658C", "PENDING": "#F5B731", "Pending Shipment": "#E87830" };
const STATUS_BADGE = (status) => {
  const s = (status || "").toUpperCase();
  const bg = s.includes("DELIVER") ? "#22A387" : s.includes("TRANSIT") ? "#3E658C" : s.includes("PENDING") ? "#F5B731" : "#C4CDD0";
  return <span style={{ display: "inline-block", padding: "2px 10px", borderRadius: 12, background: bg, color: "#fff", fontSize: 10, fontWeight: 700, letterSpacing: 0.5 }}>{status}</span>;
};

export default function LogisticsTracking() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [view, setView] = useState("shipments"); // shipments | containers
  const [statusFilter, setStatusFilter] = useState("ALL");
  const fileRef = useRef();

  const load = () => {
    setLoading(true);
    api.logistics().then(d => { setData(d); setLoading(false); }).catch(() => setLoading(false));
  };
  useEffect(load, []);

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    try {
      await api.uploadLogistics(file);
      load();
    } catch (err) { alert("Upload failed: " + err.message); }
    setUploading(false);
    if (fileRef.current) fileRef.current.value = "";
  };

  const shipments = useMemo(() => data?.shipments || [], [data]);
  const items = useMemo(() => data?.itemsByContainer || [], [data]);

  // Status breakdown
  const statusMap = useMemo(() => {
    const m = {};
    shipments.forEach(s => {
      const st = s.status || "Unknown";
      if (!m[st]) m[st] = { count: 0, units: 0, cbm: 0 };
      m[st].count++; m[st].units += s.units || 0; m[st].cbm += s.cbm || 0;
    });
    return m;
  }, [shipments]);

  // Port breakdown
  const portMap = useMemo(() => {
    const m = {};
    shipments.forEach(s => {
      const p = s.arrivalPort || "Unknown";
      if (!m[p]) m[p] = { count: 0, units: 0 };
      m[p].count++; m[p].units += s.units || 0;
    });
    return m;
  }, [shipments]);

  // Container type breakdown for chart
  const containerTypes = useMemo(() => {
    const m = {};
    shipments.forEach(s => {
      const t = s.containerType || "Unknown";
      m[t] = (m[t] || 0) + 1;
    });
    return Object.entries(m).map(([name, value]) => ({ name, value }));
  }, [shipments]);

  // Shipper breakdown
  const shipperMap = useMemo(() => {
    const m = {};
    shipments.forEach(s => {
      const name = (s.shipper || "Unknown").split(" ").slice(0, 2).join(" ");
      if (!m[name]) m[name] = { units: 0, shipments: 0 };
      m[name].units += s.units || 0; m[name].shipments++;
    });
    return Object.entries(m).map(([name, d]) => ({ name, units: d.units, shipments: d.shipments })).sort((a, b) => b.units - a.units);
  }, [shipments]);

  // Filtered shipments
  const filteredShipments = useMemo(() => {
    if (statusFilter === "ALL") return shipments;
    return shipments.filter(s => (s.status || "").toUpperCase().includes(statusFilter));
  }, [shipments, statusFilter]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading Logistics data...</div>;
  if (!data) return <div style={{ padding: 40, textAlign: "center", color: "var(--muted)" }}>No Logistics data. Upload an Excel file to begin.</div>;

  const totalUnits = shipments.reduce((s, sh) => s + (sh.units || 0), 0);
  const totalCBM = shipments.reduce((s, sh) => s + (sh.cbm || 0), 0);
  const deliveredUnits = Object.entries(statusMap).filter(([k]) => k.toUpperCase().includes("DELIVER")).reduce((s, [, v]) => s + v.units, 0);
  const inTransitUnits = Object.entries(statusMap).filter(([k]) => k.toUpperCase().includes("TRANSIT")).reduce((s, [, v]) => s + v.units, 0);
  const uniqueContainers = new Set(shipments.map(s => s.containerNumber).filter(Boolean)).size;

  // Port bar chart data
  const portBar = Object.entries(portMap).map(([port, d]) => ({ port, units: d.units })).sort((a, b) => b.units - a.units);

  return (
    <div>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: 22, color: "var(--navy)", margin: 0 }}>
            OTW / Logistics Tracking
          </h2>
          <p style={{ fontSize: 12, color: "var(--muted)", marginTop: 4 }}>
            {data.lastUpload ? `Last updated: ${data.lastUpload}` : "No upload date"}{data.sourceFile ? ` • ${data.sourceFile}` : ""}
          </p>
        </div>
        <div>
          <input type="file" accept=".xlsx,.xls" ref={fileRef} onChange={handleUpload} style={{ display: "none" }} />
          <button onClick={() => fileRef.current?.click()} disabled={uploading}
            style={{ padding: "8px 20px", background: "var(--blue)", color: "#fff", border: "none", borderRadius: 8, fontWeight: 600, fontSize: 13, cursor: "pointer", opacity: uploading ? 0.6 : 1 }}>
            {uploading ? "Uploading..." : "📤 Upload Update"}
          </button>
        </div>
      </div>

      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(155px, 1fr))", gap: 12, marginBottom: 24 }}>
        {[
          { label: "Total Shipments", value: shipments.length, color: "#3E658C" },
          { label: "Containers", value: uniqueContainers, color: "#7BAED0" },
          { label: "Total Units", value: fmtK(totalUnits), color: "#2ECFAA" },
          { label: "Delivered", value: fmtK(deliveredUnits), color: "#22A387", sub: totalUnits > 0 ? `${((deliveredUnits / totalUnits) * 100).toFixed(0)}% of total` : null },
          { label: "In Transit", value: fmtK(inTransitUnits), color: "#E87830", sub: totalUnits > 0 ? `${((inTransitUnits / totalUnits) * 100).toFixed(0)}% of total` : null },
          { label: "Total CBM", value: totalCBM.toFixed(0), color: "#F5B731" },
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
        {/* Units by Arrival Port */}
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "var(--card-shadow)" }}>
          <h3 style={{ fontSize: 14, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 16 }}>Units by Arrival Port</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={portBar} layout="vertical" margin={{ left: 30 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={fmtK} />
              <YAxis type="category" dataKey="port" tick={{ fontSize: 11 }} width={90} />
              <Tooltip formatter={v => [v.toLocaleString() + " units", "Units"]} />
              <Bar dataKey="units" fill="var(--blue)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Shipper Breakdown */}
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "var(--card-shadow)" }}>
          <h3 style={{ fontSize: 14, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 16 }}>Units by Shipper</h3>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={shipperMap} dataKey="units" nameKey="name" cx="50%" cy="50%" innerRadius={45} outerRadius={80}
                label={({ name, percent }) => `${name.split(" ")[0]} ${(percent * 100).toFixed(0)}%`} labelLine={false} style={{ fontSize: 10 }}>
                {shipperMap.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip formatter={v => [v.toLocaleString() + " units", "Units"]} />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* View Toggle + Status Filter */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap", alignItems: "center" }}>
        {[["shipments", "Shipment Log"], ["containers", "Items by Container"]].map(([k, l]) => (
          <button key={k} onClick={() => setView(k)}
            style={{ padding: "6px 16px", borderRadius: 6, border: "1px solid " + (view === k ? "var(--blue)" : "var(--border)"),
              background: view === k ? "var(--blue)" : "#fff", color: view === k ? "#fff" : "var(--body-text)",
              fontSize: 12, fontWeight: 600, cursor: "pointer" }}>
            {l}
          </button>
        ))}
        {view === "shipments" && (
          <div style={{ marginLeft: "auto", display: "flex", gap: 4 }}>
            {["ALL", "DELIVER", "TRANSIT"].map(f => (
              <button key={f} onClick={() => setStatusFilter(f)}
                style={{ padding: "4px 12px", borderRadius: 12, border: "none", fontSize: 10, fontWeight: 600, cursor: "pointer",
                  background: statusFilter === f ? (f === "DELIVER" ? "#22A387" : f === "TRANSIT" ? "#3E658C" : "var(--navy)") : "#eee",
                  color: statusFilter === f ? "#fff" : "var(--muted)" }}>
                {f === "ALL" ? "All" : f === "DELIVER" ? "Delivered" : "In Transit"}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Shipment Log Table */}
      {view === "shipments" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "auto", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, minWidth: 900 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>Status</th><th style={th}>PO #</th><th style={th}>Container</th>
                <th style={th}>Type</th><th style={th}>Vessel / Voyage</th>
                <th style={{...th, textAlign: "right"}}>Units</th><th style={{...th, textAlign: "right"}}>CBM</th>
                <th style={th}>Departure</th><th style={th}>ETD</th><th style={th}>ETA Port</th>
                <th style={th}>Port</th><th style={th}>Delivery</th>
              </tr>
            </thead>
            <tbody>
              {filteredShipments.map((s, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--border)", background: i % 2 ? "#FAFBFC" : "#fff" }}>
                  <td style={td}>{STATUS_BADGE(s.status)}</td>
                  <td style={{...td, fontWeight: 600, color: "var(--blue)", fontSize: 10}}>{s.poNumber}</td>
                  <td style={{...td, fontFamily: "monospace", fontSize: 10}}>{s.containerNumber}</td>
                  <td style={{...td, fontSize: 10, color: "var(--muted)"}}>{s.containerType}</td>
                  <td style={{...td, fontSize: 10, maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{s.vesselVoyage}</td>
                  <td style={{...td, textAlign: "right", fontWeight: 600}}>{(s.units || 0).toLocaleString()}</td>
                  <td style={{...td, textAlign: "right"}}>{(s.cbm || 0).toFixed(1)}</td>
                  <td style={{...td, fontSize: 10}}>{s.departurePort || "—"}</td>
                  <td style={{...td, fontSize: 10}}>{s.etdOrigin || "—"}</td>
                  <td style={{...td, fontSize: 10}}>{s.etaDischarge || "—"}</td>
                  <td style={{...td, fontSize: 10, fontWeight: 600}}>{s.arrivalPort || "—"}</td>
                  <td style={{...td, fontSize: 10}}>{s.deliveryDate || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ padding: "8px 16px", fontSize: 11, color: "var(--muted)", borderTop: "1px solid var(--border)" }}>
            Showing {filteredShipments.length} of {shipments.length} shipments
          </div>
        </div>
      )}

      {/* Items by Container Table */}
      {view === "containers" && (
        <div style={{ background: "#fff", borderRadius: 12, overflow: "auto", boxShadow: "var(--card-shadow)" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ background: "var(--navy)", color: "#fff" }}>
                <th style={th}>Container #</th><th style={th}>Invoice</th><th style={th}>ETA</th>
                <th style={th}>PO #</th><th style={th}>SKU</th><th style={th}>Description</th>
                <th style={{...th, textAlign: "right"}}>Qty</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item, i) => {
                const isTotal = item.description === "Container Total";
                return (
                  <tr key={i} style={{ borderBottom: "1px solid var(--border)",
                    background: isTotal ? "var(--teal-bg)" : (item.containerNumber ? "#F8FAFB" : "#fff"),
                    fontWeight: isTotal ? 700 : 400 }}>
                    <td style={{...td, fontFamily: "monospace", fontSize: 10, fontWeight: item.containerNumber ? 700 : 400, color: item.containerNumber ? "var(--blue)" : "transparent"}}>{item.containerNumber || ""}</td>
                    <td style={{...td, fontSize: 10}}>{item.invoice}</td>
                    <td style={{...td, fontSize: 10}}>{item.eta || ""}</td>
                    <td style={{...td, fontSize: 10, color: "var(--blue)"}}>{item.po}</td>
                    <td style={{...td, fontFamily: "monospace", fontSize: 10}}>{item.sku}</td>
                    <td style={{...td, fontSize: 10, maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap"}}>{item.description}</td>
                    <td style={{...td, textAlign: "right", fontWeight: isTotal ? 700 : 400}}>{(item.qty || 0).toLocaleString()}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

const th = { padding: "10px 12px", textAlign: "left", fontWeight: 600, fontSize: 11, letterSpacing: 0.3 };
const td = { padding: "7px 12px" };
