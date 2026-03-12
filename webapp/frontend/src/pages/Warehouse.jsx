import { useState, useEffect, useCallback } from "react";
import { api } from "../lib/api";
import { COLOR_BADGES } from "../lib/constants";

function Badge({ color }) {
  if (!color) return null;
  const style = COLOR_BADGES[color] || { bg: "#f3f4f6", color: "#6b7280" };
  return (
    <span style={{
      display: "inline-block", padding: "2px 10px", borderRadius: 12,
      fontSize: 11, fontWeight: 600, background: style.bg, color: style.color,
    }}>
      {color}
    </span>
  );
}

const SUFFIX_LABELS = {
  "/RB": "Rebox",
  "/DONATE": "Donate",
  "/RETD": "Returned",
  "/HOLD": "Hold",
  "/INBD": "Inbound",
  "/1": "Each",
};

function SuffixBadge({ suffix }) {
  // Try to match known suffixes
  let label = suffix;
  let bg = "#f3f4f6";
  let fg = "#6b7280";

  for (const [key, lbl] of Object.entries(SUFFIX_LABELS)) {
    if (suffix.includes(key)) {
      label = lbl;
      if (key === "/RB") { bg = "#fef3c7"; fg = "#92400e"; }
      else if (key === "/DONATE") { bg = "#dbeafe"; fg = "#1e40af"; }
      else if (key === "/RETD") { bg = "#fee2e2"; fg = "#991b1b"; }
      else if (key === "/HOLD") { bg = "#fce7f3"; fg = "#9d174d"; }
      else if (key === "/INBD") { bg = "#d1fae5"; fg = "#065f46"; }
      else if (key === "/1") { bg = "#ede9fe"; fg = "#5b21b6"; }
      break;
    }
  }

  return (
    <span style={{
      display: "inline-block", padding: "1px 8px", borderRadius: 10,
      fontSize: 10, fontWeight: 600, background: bg, color: fg,
    }}>
      {label}
    </span>
  );
}

const FILTERS = ["All", "Amazon SKUs", "Non-Amazon"];

export default function Warehouse() {
  const [masters, setMasters] = useState([]);
  const [loading, setLoading] = useState(true);
  const [expandedRef, setExpandedRef] = useState(null);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState("All");
  const [sortKey, setSortKey] = useState("totalOnHand");
  const [sortDir, setSortDir] = useState("desc");
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState(null);

  const load = useCallback(() => {
    setLoading(true);
    api.warehouse().then(d => {
      setMasters(d.masters || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    setUploadMsg(null);
    try {
      const { ok, data } = await api.uploadWarehouseExcel(file);
      if (ok) {
        const whRows = data.warehouse?.rows || 0;
        setUploadMsg({ type: "success", text: `Updated! ${whRows} warehouse items refreshed from ${file.name}` });
        load(); // Reload data
      } else {
        setUploadMsg({ type: "error", text: data.detail || "Upload failed" });
      }
    } catch (err) {
      setUploadMsg({ type: "error", text: err.message });
    } finally {
      setUploading(false);
      e.target.value = ""; // Reset file input
    }
  };

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const filtered = masters.filter(m => {
    if (filter === "Amazon SKUs" && !m.asin) return false;
    if (filter === "Non-Amazon" && m.asin) return false;
    if (search) {
      const q = search.toLowerCase();
      return (m.itemRef || "").toLowerCase().includes(q) ||
             (m.asin || "").toLowerCase().includes(q) ||
             (m.description || "").toLowerCase().includes(q) ||
             (m.whDescription || "").toLowerCase().includes(q);
    }
    return true;
  });

  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey] ?? "";
    const bv = b[sortKey] ?? "";
    if (typeof av === "number" && typeof bv === "number") return sortDir === "desc" ? bv - av : av - bv;
    return sortDir === "desc" ? String(bv).localeCompare(String(av)) : String(av).localeCompare(String(bv));
  });

  const SortHeader = ({ label, field, style: s }) => (
    <th onClick={() => handleSort(field)} style={{ cursor: "pointer", userSelect: "none", ...s }}>
      {label} {sortKey === field ? (sortDir === "desc" ? "▼" : "▲") : ""}
    </th>
  );

  if (loading) return <div className="loading"><div className="spinner" /> Loading warehouse data...</div>;

  // Summary stats
  const totalPcsOH = masters.reduce((s, m) => s + m.totalOnHand, 0);
  const totalAvail = masters.reduce((s, m) => s + m.totalAvailable, 0);
  const totalAlloc = masters.reduce((s, m) => s + m.totalAllocated, 0);
  const amazonItems = masters.filter(m => m.asin).length;
  const totalDamage = masters.reduce((s, m) => s + m.totalDamage, 0);

  return (
    <>
      <div className="page-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h1>GolfGen Warehouse</h1>
          <p>Moose 3PL Daily Inventory &middot; {masters.length} master items &middot; {totalPcsOH.toLocaleString()} total pcs on hand</p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <label style={{
            display: "inline-flex", alignItems: "center", gap: 6, padding: "6px 14px",
            background: "var(--teal)", color: "#fff", borderRadius: 8, cursor: "pointer",
            fontSize: 13, fontWeight: 600, opacity: uploading ? 0.6 : 1,
          }}>
            {uploading ? "Uploading..." : "Upload Excel"}
            <input type="file" accept=".xlsx,.xls" onChange={handleUpload}
              style={{ display: "none" }} disabled={uploading} />
          </label>
        </div>
      </div>
      {uploadMsg && (
        <div style={{
          padding: "8px 16px", marginBottom: 12, borderRadius: 8, fontSize: 13,
          background: uploadMsg.type === "success" ? "#dcfce7" : "#fee2e2",
          color: uploadMsg.type === "success" ? "#166534" : "#991b1b",
          display: "flex", justifyContent: "space-between", alignItems: "center",
        }}>
          <span>{uploadMsg.text}</span>
          <button onClick={() => setUploadMsg(null)} style={{
            background: "none", border: "none", cursor: "pointer", fontSize: 16,
            color: uploadMsg.type === "success" ? "#166534" : "#991b1b",
          }}>×</button>
        </div>
      )}

      {/* Summary Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 16, marginBottom: 24 }}>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 12, color: "var(--muted)" }}>Master Items</div>
          <div style={{ fontSize: 26, fontWeight: 700, color: "var(--navy)" }}>{masters.length}</div>
        </div>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 12, color: "var(--muted)" }}>Pcs On Hand</div>
          <div style={{ fontSize: 26, fontWeight: 700, color: "var(--teal)" }}>{totalPcsOH.toLocaleString()}</div>
        </div>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 12, color: "var(--muted)" }}>Available</div>
          <div style={{ fontSize: 26, fontWeight: 700, color: "#16a34a" }}>{totalAvail.toLocaleString()}</div>
        </div>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 12, color: "var(--muted)" }}>Allocated</div>
          <div style={{ fontSize: 26, fontWeight: 700, color: "var(--orange)" }}>{totalAlloc.toLocaleString()}</div>
        </div>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 12, color: "var(--muted)" }}>Amazon SKUs</div>
          <div style={{ fontSize: 26, fontWeight: 700, color: "#7BAED0" }}>{amazonItems}</div>
        </div>
        {totalDamage > 0 && (
          <div className="card" style={{ padding: 16, textAlign: "center" }}>
            <div style={{ fontSize: 12, color: "var(--muted)" }}>Damaged</div>
            <div style={{ fontSize: 26, fontWeight: 700, color: "#dc2626" }}>{totalDamage.toLocaleString()}</div>
          </div>
        )}
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>
        <div className="range-tabs">
          {FILTERS.map(f => (
            <button key={f} className={`range-tab ${filter === f ? "active" : ""}`} onClick={() => setFilter(f)}>
              {f}
            </button>
          ))}
        </div>
        <input
          type="text" placeholder="Search item #, ASIN, or description..."
          value={search} onChange={e => setSearch(e.target.value)}
          style={{
            padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8,
            fontSize: 13, width: 280, outline: "none",
          }}
        />
        <span style={{ fontSize: 13, color: "var(--muted)", marginLeft: "auto" }}>
          Showing {sorted.length} of {masters.length}
        </span>
      </div>

      {/* Main Table */}
      <div className="card" style={{ overflowX: "auto" }}>
        <table className="data-table" style={{ fontSize: 13 }}>
          <thead>
            <tr>
              <SortHeader label="Item Number" field="itemRef" style={{ minWidth: 160 }} />
              <th style={{ minWidth: 50 }}>ASIN</th>
              <SortHeader label="Description" field="description" style={{ minWidth: 240 }} />
              <SortHeader label="Pack" field="pack" />
              <SortHeader label="On Hand" field="totalOnHand" />
              <SortHeader label="Damage" field="totalDamage" />
              <SortHeader label="QC Hold" field="totalQcHold" />
              <SortHeader label="Allocated" field="totalAllocated" />
              <SortHeader label="Available" field="totalAvailable" />
              <th style={{ width: 30 }}></th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(m => (
              <>
                <tr key={m.itemRef} style={{
                  background: m.asin ? undefined : "rgba(0,0,0,0.015)",
                }}>
                  <td style={{ fontFamily: "monospace", fontSize: 12, fontWeight: 600 }}>{m.itemRef}</td>
                  <td style={{ fontFamily: "monospace", fontSize: 11, color: m.asin ? "var(--teal-dark)" : "var(--muted)" }}>
                    {m.asin || "—"}
                  </td>
                  <td style={{ maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                      title={m.description}>
                    {m.description}
                    {m.color && <> <Badge color={m.color} /></>}
                  </td>
                  <td style={{ textAlign: "right" }}>{m.pack}</td>
                  <td style={{ textAlign: "right", fontWeight: 600 }}>{m.totalOnHand.toLocaleString()}</td>
                  <td style={{ textAlign: "right", color: m.totalDamage > 0 ? "#dc2626" : "var(--muted)" }}>
                    {m.totalDamage > 0 ? m.totalDamage.toLocaleString() : "—"}
                  </td>
                  <td style={{ textAlign: "right", color: m.totalQcHold > 0 ? "#d97706" : "var(--muted)" }}>
                    {m.totalQcHold > 0 ? m.totalQcHold.toLocaleString() : "—"}
                  </td>
                  <td style={{ textAlign: "right", color: m.totalAllocated > 0 ? "var(--orange)" : "var(--muted)" }}>
                    {m.totalAllocated > 0 ? m.totalAllocated.toLocaleString() : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontWeight: 600, color: m.totalAvailable > 0 ? "#16a34a" : "#dc2626" }}>
                    {m.totalAvailable.toLocaleString()}
                  </td>
                  <td>
                    {m.subCount > 0 ? (
                      <button
                        onClick={() => setExpandedRef(expandedRef === m.itemRef ? null : m.itemRef)}
                        style={{
                          background: "none", border: "none", cursor: "pointer", fontSize: 14,
                          color: expandedRef === m.itemRef ? "var(--teal)" : "var(--muted)",
                          fontWeight: 600,
                        }}
                        title={`${m.subCount} sub-item${m.subCount > 1 ? "s" : ""}`}
                      >
                        {expandedRef === m.itemRef ? "▲" : "▼"} <span style={{ fontSize: 11 }}>{m.subCount}</span>
                      </button>
                    ) : (
                      <span style={{ color: "var(--muted)", fontSize: 11 }}>—</span>
                    )}
                  </td>
                </tr>
                {expandedRef === m.itemRef && m.subCount > 0 && (
                  <tr key={m.itemRef + "-subs"}>
                    <td colSpan={10} style={{ padding: 0 }}>
                      <div style={{
                        background: "rgba(14,31,45,0.03)", borderLeft: "3px solid var(--teal)",
                        padding: "8px 16px 8px 32px",
                      }}>
                        <div style={{ fontSize: 11, fontWeight: 600, color: "var(--muted)", marginBottom: 6 }}>
                          SUB-ITEMS — {m.subCount} variant{m.subCount > 1 ? "s" : ""} of {m.itemRef}
                        </div>
                        <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                          <thead>
                            <tr style={{ borderBottom: "1px solid rgba(14,31,45,0.1)" }}>
                              <th style={{ textAlign: "left", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Item Number</th>
                              <th style={{ textAlign: "left", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Type</th>
                              <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Pack</th>
                              <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>On Hand</th>
                              <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Damage</th>
                              <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>QC Hold</th>
                              <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Allocated</th>
                              <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Available</th>
                            </tr>
                          </thead>
                          <tbody>
                            {/* Show master row first */}
                            <tr style={{ borderBottom: "1px solid rgba(14,31,45,0.06)", background: "rgba(46,207,170,0.04)" }}>
                              <td style={{ padding: "4px 8px", fontFamily: "monospace", fontWeight: 600 }}>{m.itemNumber}</td>
                              <td style={{ padding: "4px 8px" }}>
                                <span style={{ display: "inline-block", padding: "1px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600, background: "#d1fae5", color: "#065f46" }}>Master</span>
                              </td>
                              <td style={{ textAlign: "right", padding: "4px 8px" }}>{m.pack}</td>
                              <td style={{ textAlign: "right", padding: "4px 8px", fontWeight: 600 }}>
                                {(m.totalOnHand - m.subs.reduce((s, sub) => s + sub.pcsOnHand, 0)).toLocaleString()}
                              </td>
                              <td style={{ textAlign: "right", padding: "4px 8px" }}>
                                {(m.totalDamage - m.subs.reduce((s, sub) => s + sub.damage, 0)) > 0
                                  ? (m.totalDamage - m.subs.reduce((s, sub) => s + sub.damage, 0)).toLocaleString() : "—"}
                              </td>
                              <td style={{ textAlign: "right", padding: "4px 8px" }}>
                                {(m.totalQcHold - m.subs.reduce((s, sub) => s + sub.qcHold, 0)) > 0
                                  ? (m.totalQcHold - m.subs.reduce((s, sub) => s + sub.qcHold, 0)).toLocaleString() : "—"}
                              </td>
                              <td style={{ textAlign: "right", padding: "4px 8px" }}>
                                {(m.totalAllocated - m.subs.reduce((s, sub) => s + sub.pcsAllocated, 0)) > 0
                                  ? (m.totalAllocated - m.subs.reduce((s, sub) => s + sub.pcsAllocated, 0)).toLocaleString() : "—"}
                              </td>
                              <td style={{ textAlign: "right", padding: "4px 8px", fontWeight: 600, color: "#16a34a" }}>
                                {(m.totalAvailable - m.subs.reduce((s, sub) => s + sub.pcsAvailable, 0)).toLocaleString()}
                              </td>
                            </tr>
                            {m.subs.map((sub, idx) => (
                              <tr key={sub.itemNumber} style={{
                                borderBottom: idx < m.subs.length - 1 ? "1px solid rgba(14,31,45,0.06)" : "none",
                              }}>
                                <td style={{ padding: "4px 8px", fontFamily: "monospace", fontSize: 11 }}>{sub.itemNumber}</td>
                                <td style={{ padding: "4px 8px" }}><SuffixBadge suffix={sub.suffix || ""} /></td>
                                <td style={{ textAlign: "right", padding: "4px 8px" }}>{sub.pack}</td>
                                <td style={{ textAlign: "right", padding: "4px 8px" }}>{sub.pcsOnHand.toLocaleString()}</td>
                                <td style={{ textAlign: "right", padding: "4px 8px", color: sub.damage > 0 ? "#dc2626" : "var(--muted)" }}>
                                  {sub.damage > 0 ? sub.damage.toLocaleString() : "—"}
                                </td>
                                <td style={{ textAlign: "right", padding: "4px 8px", color: sub.qcHold > 0 ? "#d97706" : "var(--muted)" }}>
                                  {sub.qcHold > 0 ? sub.qcHold.toLocaleString() : "—"}
                                </td>
                                <td style={{ textAlign: "right", padding: "4px 8px", color: sub.pcsAllocated > 0 ? "var(--orange)" : "var(--muted)" }}>
                                  {sub.pcsAllocated > 0 ? sub.pcsAllocated.toLocaleString() : "—"}
                                </td>
                                <td style={{ textAlign: "right", padding: "4px 8px", color: sub.pcsAvailable > 0 ? "#16a34a" : "var(--muted)" }}>
                                  {sub.pcsAvailable.toLocaleString()}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: 16, padding: 12, background: "rgba(14,31,45,0.03)", borderRadius: 8, fontSize: 12, color: "var(--muted)" }}>
        <strong>Note:</strong> All quantities are in eaches (units), not cartons. Items with an ASIN are sorted first.
        Click ▼ to expand sub-items (rebox, donate, returned, etc.). Suffix codes: /RB = Rebox, /DONATE = Donate, /RETD = Returned, /HOLD = Hold, /INBD = Inbound, /1 = Each.
      </div>
    </>
  );
}
