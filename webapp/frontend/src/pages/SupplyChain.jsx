import { useState, useEffect, useMemo, useRef } from "react";
import { api } from "../lib/api";

/* ── Status colors matching spec ─────────────────────────────── */
const STATUS_COLORS = {
  Delivered: { bg: "#D4F5E9", text: "#0d6939", dot: "#22c55e" },
  "In Transit": { bg: "#FFF3CD", text: "#856404", dot: "#f59e0b" },
  Open: { bg: "#E2E8F0", text: "#475569", dot: "#94a3b8" },
};

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

/* ── Sub-tab button component ─────────────────────────────────── */
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

/* ═══════════════════════════════════════════════════════════════ */
export default function SupplyChain() {
  const [view, setView] = useState("otw"); // otw | po | invoices
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
            PO &bull; OTW &bull; Invoice — Unified View
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
          background: uploadMsg.startsWith("Upload") && !uploadMsg.includes("error") && !uploadMsg.includes("fail") ? "#D4F5E9" : "#FEE2E2",
          color: uploadMsg.startsWith("Upload") && !uploadMsg.includes("error") && !uploadMsg.includes("fail") ? "#0d6939" : "#991b1b",
          fontSize: 13 }}>
          {uploadMsg}
        </div>
      )}

      {error && <div className="login-error" style={{ margin: "0 0 16px" }}>{error}</div>}

      {/* Sub-tab navigation */}
      <div style={{ display: "flex", gap: 4, marginBottom: 0 }}>
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

  return (
    <div className="section-card" style={{ borderRadius: "0 8px 8px 8px" }}>
      {/* KPI Cards */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <KPICard label="Total Containers" value={summary.totalContainers} />
        <KPICard label="Total Units" value={fmtNum(summary.totalUnits)} />
        <KPICard label="Total CBM" value={summary.totalCBM} />
        <KPICard label="Delivered" value={summary.delivered} />
        <KPICard label="In Transit" value={summary.inTransit} />
        <KPICard label="Open" value={summary.open} />
      </div>

      {/* Main table */}
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

      {/* Item Detail toggle */}
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

  return (
    <div className="section-card" style={{ borderRadius: "0 8px 8px 8px" }}>
      {/* KPI Cards */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <KPICard label="Total POs" value={summary.totalPOs} />
        <KPICard label="Units Ordered" value={fmtNum(summary.unitsOrdered)} />
        <KPICard label="Units Shipped" value={fmtNum(summary.unitsShipped)} />
        <KPICard label="Delivered" value={summary.delivered} />
        <KPICard label="In Transit" value={summary.inTransit} />
        <KPICard label="Open" value={summary.open} />
      </div>

      {/* Main table */}
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
                <td colSpan={9}></td>
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

  return (
    <div className="section-card" style={{ borderRadius: "0 8px 8px 8px" }}>
      {/* KPI Cards */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginBottom: 20 }}>
        <KPICard label="Total Invoices" value={summary.totalInvoices} />
        <KPICard label="Total Units" value={fmtNum(summary.totalUnits)} />
        <KPICard label="Total CBM" value={summary.totalCBM} />
        <KPICard label="Delivered" value={summary.delivered} />
        <KPICard label="In Transit" value={summary.inTransit} />
        <KPICard label="Open" value={summary.open} />
      </div>

      {/* Main table */}
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
