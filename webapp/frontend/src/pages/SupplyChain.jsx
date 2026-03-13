import { useState, useEffect, useMemo, useRef } from "react";
import { api } from "../lib/api";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
  PieChart, Pie, Cell,
} from "recharts";

/* ── Status colors matching app style ────────────────────────── */
const STATUS_COLORS = {
  Delivered: { bg: "#D4F5E9", text: "#0d6939", dot: "#22c55e" },
  "In Transit": { bg: "#FFF3CD", text: "#856404", dot: "#f59e0b" },
  Open: { bg: "#E2E8F0", text: "#475569", dot: "#94a3b8" },
};
const PIE_COLORS = ["#22c55e", "#f59e0b", "#94a3b8"];

function StatusBadge({ status }) {
  const s = STATUS_COLORS[status] || STATUS_COLORS.Open;
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 5,
      padding: "3px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600,
      background: s.bg, color: s.text,
    }}>
      <span style={{ width: 8, height: 8, borderRadius: "50%", background: s.dot, display: "inline-block" }} />
      {status || "—"}
    </span>
  );
}

function KPICard({ label, value, sub }) {
  return (
    <div className="metric-card" style={{ flex: "1 1 140px", minWidth: 120 }}>
      <div style={{ fontSize: 11, color: "var(--muted)", textTransform: "uppercase", letterSpacing: 1 }}>{label}</div>
      <div style={{ fontSize: 24, fontWeight: 700, margin: "4px 0" }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "var(--muted)" }}>{sub}</div>}
    </div>
  );
}

function fmtDate(d) {
  if (!d || d === "—") return "—";
  try {
    const dt = new Date(d + "T00:00:00");
    return dt.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "2-digit" });
  } catch { return d; }
}

function fmtNum(n) {
  if (n == null || n === "—") return "—";
  return Number(n).toLocaleString();
}

function SubTab({ label, active, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: "8px 20px", border: "none", borderRadius: "6px 6px 0 0",
        background: active ? "var(--navy, #0E1F2D)" : "#e2e8f0",
        color: active ? "#fff" : "#475569",
        fontWeight: active ? 700 : 500, fontSize: 13, cursor: "pointer",
        transition: "all 0.15s",
      }}
    >{label}</button>
  );
}

/* ── Status Pie Chart (shared) ───────────────────────────────── */
function StatusPieChart({ delivered = 0, inTransit = 0, open = 0 }) {
  const data = [
    { name: "Delivered", value: delivered },
    { name: "In Transit", value: inTransit },
    { name: "Open", value: open },
  ].filter(d => d.value > 0);
  if (data.length === 0) return null;
  return (
    <div style={{ width: 220, height: 200 }}>
      <ResponsiveContainer>
        <PieChart>
          <Pie data={data} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={70}
            label={({ name, value }) => `${name}: ${value}`} labelLine={false} fontSize={11}>
            {data.map((_, i) => <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />)}
          </Pie>
          <Tooltip />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

/* ── Units by ETA Month Bar Chart (shared) ───────────────────── */
function UnitsByMonthChart({ rows, unitsKey = "units" }) {
  const monthData = useMemo(() => {
    const map = {};
    rows.forEach(r => {
      const mo = r.eta_month || "—";
      if (mo === "—") return;
      if (!map[mo]) map[mo] = { month: mo, units: 0 };
      map[mo].units += r[unitsKey] || 0;
    });
    const arr = Object.values(map);
    // Sort by date
    arr.sort((a, b) => {
      try {
        return new Date("1 " + a.month) - new Date("1 " + b.month);
      } catch { return 0; }
    });
    return arr;
  }, [rows, unitsKey]);
  if (monthData.length === 0) return null;
  return (
    <div style={{ width: "100%", height: 220 }}>
      <ResponsiveContainer>
        <BarChart data={monthData} margin={{ top: 5, right: 20, bottom: 5, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="month" fontSize={11} />
          <YAxis fontSize={11} />
          <Tooltip formatter={(v) => fmtNum(v)} />
          <Bar dataKey="units" fill="#2ECFAA" radius={[4, 4, 0, 0]} name="Units" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════ */
export default function SupplyChain() {
  const [view, setView] = useState("otw");
  const [otwData, setOtwData] = useState(null);
  const [poData, setPoData] = useState(null);
  const [invData, setInvData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState("");
  const fileRef = useRef(null);

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      const [otw, po, inv] = await Promise.all([
        api.supplyChainOTW(),
        api.supplyChainPO(),
        api.supplyChainInvoices(),
      ]);
      setOtwData(otw);
      setPoData(po);
      setInvData(inv);
    } catch (e) {
      setError("Failed to load supply chain data: " + e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadMsg("");
    try {
      const res = await api.uploadSupplyChainV2(file);
      if (res.status === "ok") {
        setUploadMsg(`Uploaded ${res.records_parsed} records (${res.total_records} total).`);
        await load();
      } else {
        setUploadMsg("Upload error: " + (res.message || "Unknown error"));
      }
    } catch (e) {
      setUploadMsg("Upload failed: " + e.message);
    } finally {
      setUploading(false);
      if (fileRef.current) fileRef.current.value = "";
    }
  };

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading supply chain data...</div>;
  }

  const lastUpload = otwData?.lastUpload;
  const sourceFile = otwData?.sourceFile;

  return (
    <div>
      <div className="page-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 12 }}>
        <div>
          <h2>Supply Chain Report</h2>
          <p style={{ color: "var(--muted)", marginTop: 4, fontSize: 13 }}>
            OTW &bull; PO &bull; Invoice — Unified View
            {lastUpload && <span> &bull; Last upload: {new Date(lastUpload).toLocaleDateString()}</span>}
            {sourceFile && <span> ({sourceFile})</span>}
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <label className="upload-btn" style={{
            padding: "8px 16px", borderRadius: 6, background: "var(--teal, #2ECFAA)", color: "#fff",
            fontWeight: 600, fontSize: 13, cursor: "pointer", display: "inline-block",
          }}>
            {uploading ? "Uploading..." : "Upload Excel"}
            <input ref={fileRef} type="file" accept=".xlsx,.xls" onChange={handleUpload} hidden disabled={uploading} />
          </label>
        </div>
      </div>

      {uploadMsg && (
        <div style={{ margin: "0 0 12px", padding: "8px 14px", borderRadius: 6,
          background: uploadMsg.includes("error") || uploadMsg.includes("fail") ? "#FEE2E2" : "#D4F5E9",
          color: uploadMsg.includes("error") || uploadMsg.includes("fail") ? "#991b1b" : "#0d6939",
          fontSize: 13 }}>
          {uploadMsg}
        </div>
      )}

      {error && <div className="login-error" style={{ margin: "0 0 16px" }}>{error}</div>}

      <div style={{ display: "flex", gap: 4, marginBottom: 0, position: "sticky", top: 178, zIndex: 90, background: "#f1f5f9", paddingTop: 4, paddingBottom: 2 }}>
        <SubTab label="OTW Summary" active={view === "otw"} onClick={() => setView("otw")} />
        <SubTab label="PO Summary" active={view === "po"} onClick={() => setView("po")} />
        <SubTab label="Invoice Summary" active={view === "invoices"} onClick={() => setView("invoices")} />
      </div>

      {view === "otw" && otwData && <OTWSummaryView data={otwData} />}
      {view === "po" && poData && <POSummaryView data={poData} />}
      {view === "invoices" && invData && <InvoiceSummaryView data={invData} />}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════ */
/*  VIEW 1: OTW Summary                                          */
/* ═══════════════════════════════════════════════════════════════ */
function OTWSummaryView({ data }) {
  const { rows, summary, itemPivot, allContainers } = data;
  const [showItems, setShowItems] = useState(false);
  const [showCharts, setShowCharts] = useState(true);

  return (
    <div className="section-card" style={{ borderRadius: "0 8px 8px 8px" }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <KPICard label="Total Containers" value={summary.totalContainers} />
        <KPICard label="Total Units" value={fmtNum(summary.totalUnits)} />
        <KPICard label="Total CBM" value={summary.totalCBM} />
        <KPICard label="Delivered" value={summary.delivered} />
        <KPICard label="In Transit" value={summary.inTransit} />
        <KPICard label="Open" value={summary.open} />
      </div>

      {/* Charts */}
      {rows.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <button onClick={() => setShowCharts(!showCharts)} style={{
            background: "none", border: "1px solid var(--border, #ddd)", borderRadius: 6,
            padding: "6px 14px", cursor: "pointer", fontSize: 13, fontWeight: 600, marginBottom: 12,
          }}>
            {showCharts ? "▾ Hide" : "▸ Show"} Charts
          </button>
          {showCharts && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 24, alignItems: "flex-start" }}>
              <div>
                <div style={{ fontSize: 12, fontWeight: 600, color: "var(--muted)", marginBottom: 4 }}>Status Breakdown</div>
                <StatusPieChart delivered={summary.delivered} inTransit={summary.inTransit} open={summary.open} />
              </div>
              <div style={{ flex: 1, minWidth: 300 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: "var(--muted)", marginBottom: 4 }}>Units by ETA Month</div>
                <UnitsByMonthChart rows={rows} unitsKey="units" />
              </div>
            </div>
          )}
        </div>
      )}

      <div style={{ overflowX: "auto" }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>Container #</th>
              <th>HBL#</th>
              <th>Vessel</th>
              <th>ETD</th>
              <th>ETD Port</th>
              <th>ETA</th>
              <th>ETA Port</th>
              <th>ETA Dest.</th>
              <th>Destination</th>
              <th>Act. Delivery</th>
              <th style={{ textAlign: "right" }}>Units</th>
              <th style={{ textAlign: "right" }}>CBM</th>
              <th>PO #</th>
              <th>Invoice #</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#F2F7FB" }}>
                <td style={{ fontWeight: 600 }}>{r.container_number}</td>
                <td>{r.hb_number}</td>
                <td>{r.vessel}</td>
                <td>{fmtDate(r.etd)}</td>
                <td>{r.etd_port}</td>
                <td>{fmtDate(r.container_eta)}</td>
                <td>{r.eta_port}</td>
                <td>{fmtDate(r.eta_delivery)}</td>
                <td>{r.delivery_location}</td>
                <td>{fmtDate(r.actual_delivery)}</td>
                <td style={{ textAlign: "right" }}>{fmtNum(r.units)}</td>
                <td style={{ textAlign: "right" }}>{r.cbm}</td>
                <td>{r.po_number}</td>
                <td>{r.invoice_number}</td>
                <td><StatusBadge status={r.status} /></td>
              </tr>
            ))}
            {rows.length > 0 && (
              <tr style={{ fontWeight: 700, background: "#D6E4F1" }}>
                <td colSpan={10}>TOTAL</td>
                <td style={{ textAlign: "right" }}>{fmtNum(summary.totalUnits)}</td>
                <td style={{ textAlign: "right" }}>{summary.totalCBM}</td>
                <td colSpan={3}></td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {rows.length === 0 && (
        <div style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>
          No supply chain data yet. Upload an Excel file to get started.
        </div>
      )}

      {itemPivot && itemPivot.length > 0 && (
        <div style={{ marginTop: 24 }}>
          <button onClick={() => setShowItems(!showItems)} style={{
            background: "none", border: "1px solid var(--border, #ddd)", borderRadius: 6,
            padding: "6px 14px", cursor: "pointer", fontSize: 13, fontWeight: 600,
          }}>
            {showItems ? "▾ Hide" : "▸ Show"} Item Detail — Units by Container
          </button>
          {showItems && <ItemPivotTable items={itemPivot} columns={allContainers} columnLabel="Container" columnKey="containers" />}
        </div>
      )}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════ */
/*  VIEW 2: PO Summary                                           */
/* ═══════════════════════════════════════════════════════════════ */
function POSummaryView({ data }) {
  const { rows, summary, itemPivot, allPOs } = data;
  const [showItems, setShowItems] = useState(false);
  const [showCharts, setShowCharts] = useState(true);

  // Build units by month from rows for chart
  const poChartRows = useMemo(() => {
    return rows.map(r => ({
      ...r,
      units: r.units_shipped || 0,
    }));
  }, [rows]);

  return (
    <div className="section-card" style={{ borderRadius: "0 8px 8px 8px" }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <KPICard label="Total POs" value={summary.totalPOs} />
        <KPICard label="Units Ordered" value={fmtNum(summary.unitsOrdered)} />
        <KPICard label="Units Shipped" value={fmtNum(summary.unitsShipped)} />
        <KPICard label="Delivered" value={summary.delivered} />
        <KPICard label="In Transit" value={summary.inTransit} />
        <KPICard label="Open" value={summary.open} />
      </div>

      {rows.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <button onClick={() => setShowCharts(!showCharts)} style={{
            background: "none", border: "1px solid var(--border, #ddd)", borderRadius: 6,
            padding: "6px 14px", cursor: "pointer", fontSize: 13, fontWeight: 600, marginBottom: 12,
          }}>
            {showCharts ? "▾ Hide" : "▸ Show"} Charts
          </button>
          {showCharts && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 24, alignItems: "flex-start" }}>
              <div>
                <div style={{ fontSize: 12, fontWeight: 600, color: "var(--muted)", marginBottom: 4 }}>Status Breakdown</div>
                <StatusPieChart delivered={summary.delivered} inTransit={summary.inTransit} open={summary.open} />
              </div>
              <div style={{ flex: 1, minWidth: 300 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: "var(--muted)", marginBottom: 4 }}>Units Shipped by ETA Month</div>
                <UnitsByMonthChart rows={poChartRows} unitsKey="units" />
              </div>
            </div>
          )}
        </div>
      )}

      <div style={{ overflowX: "auto" }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>PO #</th>
              <th>Factory</th>
              <th style={{ textAlign: "right" }}>Units Ordered</th>
              <th style={{ textAlign: "right" }}>Units Shipped</th>
              <th style={{ textAlign: "right" }}>Difference</th>
              <th style={{ textAlign: "right" }}>CBM</th>
              <th>Container #</th>
              <th>Invoice #</th>
              <th>FOB Date</th>
              <th>ETD</th>
              <th>ETD Port</th>
              <th>ETA Month</th>
              <th>ETA</th>
              <th>ETA Port</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const diff = r.difference;
              const showDiff = r.units_ordered !== "—" && diff != null && diff !== "—";
              return (
                <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#F2F7FB" }}>
                  <td style={{ fontWeight: 600 }}>{r.po_number}</td>
                  <td style={{ fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.factory}</td>
                  <td style={{ textAlign: "right" }}>{r.units_ordered !== "—" ? fmtNum(r.units_ordered) : "—"}</td>
                  <td style={{ textAlign: "right" }}>{fmtNum(r.units_shipped)}</td>
                  <td style={{ textAlign: "right", color: showDiff && diff > 0 ? "#dc2626" : "inherit", fontWeight: showDiff && diff > 0 ? 600 : 400 }}>
                    {showDiff ? fmtNum(diff) : "—"}
                  </td>
                  <td style={{ textAlign: "right" }}>{r.cbm}</td>
                  <td>{r.container_number}</td>
                  <td>{r.invoice_number}</td>
                  <td>{fmtDate(r.x_factory_date)}</td>
                  <td>{fmtDate(r.etd)}</td>
                  <td>{r.etd_port}</td>
                  <td>{r.eta_month}</td>
                  <td>{fmtDate(r.container_eta)}</td>
                  <td>{r.eta_port}</td>
                  <td><StatusBadge status={r.status} /></td>
                </tr>
              );
            })}
            {rows.length > 0 && (
              <tr style={{ fontWeight: 700, background: "#D6E4F1" }}>
                <td colSpan={2}>TOTAL</td>
                <td style={{ textAlign: "right" }}>{fmtNum(summary.unitsOrdered)}</td>
                <td style={{ textAlign: "right" }}>{fmtNum(summary.unitsShipped)}</td>
                <td style={{ textAlign: "right", color: "#dc2626" }}>{fmtNum(summary.unitsOrdered - summary.unitsShipped)}</td>
                <td colSpan={10}></td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {rows.length === 0 && (
        <div style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>
          No PO data yet. Upload an Excel file to get started.
        </div>
      )}

      {itemPivot && itemPivot.length > 0 && (
        <div style={{ marginTop: 24 }}>
          <button onClick={() => setShowItems(!showItems)} style={{
            background: "none", border: "1px solid var(--border, #ddd)", borderRadius: 6,
            padding: "6px 14px", cursor: "pointer", fontSize: 13, fontWeight: 600,
          }}>
            {showItems ? "▾ Hide" : "▸ Show"} Item Detail — Units by PO
          </button>
          {showItems && <ItemPivotTable items={itemPivot} columns={allPOs} columnLabel="PO" columnKey="pos" />}
        </div>
      )}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════ */
/*  VIEW 3: Invoice Summary                                       */
/* ═══════════════════════════════════════════════════════════════ */
function InvoiceSummaryView({ data }) {
  const { rows, summary, itemPivot, allInvoices } = data;
  const [showItems, setShowItems] = useState(false);
  const [showCharts, setShowCharts] = useState(true);

  const invChartRows = useMemo(() => {
    return rows.map(r => ({
      ...r,
      units: r.total_units || 0,
    }));
  }, [rows]);

  return (
    <div className="section-card" style={{ borderRadius: "0 8px 8px 8px" }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <KPICard label="Total Invoices" value={summary.totalInvoices} />
        <KPICard label="Total Units" value={fmtNum(summary.totalUnits)} />
        <KPICard label="Total CBM" value={summary.totalCBM} />
        <KPICard label="Delivered" value={summary.delivered} />
        <KPICard label="In Transit" value={summary.inTransit} />
        <KPICard label="Open" value={summary.open} />
      </div>

      {rows.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <button onClick={() => setShowCharts(!showCharts)} style={{
            background: "none", border: "1px solid var(--border, #ddd)", borderRadius: 6,
            padding: "6px 14px", cursor: "pointer", fontSize: 13, fontWeight: 600, marginBottom: 12,
          }}>
            {showCharts ? "▾ Hide" : "▸ Show"} Charts
          </button>
          {showCharts && (
            <div style={{ display: "flex", flexWrap: "wrap", gap: 24, alignItems: "flex-start" }}>
              <div>
                <div style={{ fontSize: 12, fontWeight: 600, color: "var(--muted)", marginBottom: 4 }}>Status Breakdown</div>
                <StatusPieChart delivered={summary.delivered} inTransit={summary.inTransit} open={summary.open} />
              </div>
              <div style={{ flex: 1, minWidth: 300 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: "var(--muted)", marginBottom: 4 }}>Invoice Units by ETA Month</div>
                <UnitsByMonthChart rows={invChartRows} unitsKey="units" />
              </div>
            </div>
          )}
        </div>
      )}

      <div style={{ overflowX: "auto" }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>Invoice #</th>
              <th>Invoice Date</th>
              <th style={{ textAlign: "right" }}>Total Units</th>
              <th style={{ textAlign: "right" }}>CBM</th>
              <th>PO #</th>
              <th>Container #</th>
              <th>Est. ETA</th>
              <th>ETA Port</th>
              <th>Factory</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#F2F7FB" }}>
                <td style={{ fontWeight: 600 }}>{r.invoice_number}</td>
                <td>{fmtDate(r.invoice_date)}</td>
                <td style={{ textAlign: "right" }}>{fmtNum(r.total_units)}</td>
                <td style={{ textAlign: "right" }}>{r.cbm}</td>
                <td>{r.po_number}</td>
                <td>{r.container_number}</td>
                <td>{fmtDate(r.container_eta)}</td>
                <td>{r.eta_port}</td>
                <td style={{ fontSize: 11, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.factory}</td>
                <td><StatusBadge status={r.status} /></td>
              </tr>
            ))}
            {rows.length > 0 && (
              <tr style={{ fontWeight: 700, background: "#D6E4F1" }}>
                <td colSpan={2}>TOTAL</td>
                <td style={{ textAlign: "right" }}>{fmtNum(summary.totalUnits)}</td>
                <td style={{ textAlign: "right" }}>{summary.totalCBM}</td>
                <td colSpan={6}></td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {rows.length === 0 && (
        <div style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>
          No invoice data yet. Upload an Excel file to get started.
        </div>
      )}

      {itemPivot && itemPivot.length > 0 && (
        <div style={{ marginTop: 24 }}>
          <button onClick={() => setShowItems(!showItems)} style={{
            background: "none", border: "1px solid var(--border, #ddd)", borderRadius: 6,
            padding: "6px 14px", cursor: "pointer", fontSize: 13, fontWeight: 600,
          }}>
            {showItems ? "▾ Hide" : "▸ Show"} Item Detail — Units by Invoice
          </button>
          {showItems && <ItemPivotTable items={itemPivot} columns={allInvoices} columnLabel="Invoice" columnKey="invoices" />}
        </div>
      )}
    </div>
  );
}


/* ═══════════════════════════════════════════════════════════════ */
/*  Item Pivot Table (shared across all 3 views)                  */
/* ═══════════════════════════════════════════════════════════════ */
function ItemPivotTable({ items, columns, columnLabel, columnKey }) {
  return (
    <div style={{ overflowX: "auto", marginTop: 12 }}>
      <table className="data-table" style={{ fontSize: 12 }}>
        <thead>
          <tr>
            <th style={{ minWidth: 120 }}>Item #</th>
            <th style={{ minWidth: 200 }}>Item Description</th>
            {columns.map(col => (
              <th key={col} style={{ textAlign: "right", minWidth: 80, fontSize: 11, whiteSpace: "nowrap" }}>{col}</th>
            ))}
            <th style={{ textAlign: "right", fontWeight: 700 }}>TOTAL</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item, i) => (
            <tr key={item.sku} style={{ background: i % 2 === 0 ? "#fff" : "#F2F7FB" }}>
              <td style={{ fontWeight: 600, fontSize: 11 }}>{item.sku}</td>
              <td style={{ fontSize: 11 }}>{item.description}</td>
              {columns.map(col => {
                const val = (item[columnKey] || {})[col];
                return <td key={col} style={{ textAlign: "right" }}>{val ? fmtNum(val) : "—"}</td>;
              })}
              <td style={{ textAlign: "right", fontWeight: 700 }}>{fmtNum(item.total)}</td>
            </tr>
          ))}
          {items.length > 0 && (
            <tr style={{ fontWeight: 700, background: "#D6E4F1" }}>
              <td colSpan={2}>TOTAL</td>
              {columns.map(col => {
                const total = items.reduce((sum, item) => sum + ((item[columnKey] || {})[col] || 0), 0);
                return <td key={col} style={{ textAlign: "right" }}>{fmtNum(total)}</td>;
              })}
              <td style={{ textAlign: "right" }}>{fmtNum(items.reduce((s, item) => s + item.total, 0))}</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
