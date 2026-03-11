import { useState, useEffect } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { api } from "../lib/api";

const DIVISIONS = ["Golf", "Housewares"];
const GOLF_CHANNELS = ["All", "Amazon", "Walmart", "Walmart & Amazon", "Other"];
const COLORS = ["#2ECFAA", "#3E658C", "#E87830", "#F5B731", "#7BAED0", "#22A387", "#D03030", "#8B5CF6", "#94a3b8"];
const TOOLTIP_STYLE = { background: "#fff", border: "1px solid rgba(14,31,45,0.1)", borderRadius: 8, color: "#2A3D50", boxShadow: "0 4px 12px rgba(14,31,45,0.1)" };

const SUFFIX_COLORS = {
  Standard: "#2ECFAA",
  RB: "#F5B731",
  RETD: "#D03030",
  DONATE: "#3E658C",
  Damage: "#dc2626",
  FBM: "#8B5CF6",
  HOLD: "#ec4899",
  CUST: "#E87830",
  Transfer: "#7BAED0",
  Each: "#94a3b8",
  INBD: "#22A387",
};

function SuffixBadge({ suffix }) {
  const colors = {
    Standard: { bg: "#d1fae5", fg: "#065f46" },
    RB: { bg: "#fef3c7", fg: "#92400e" },
    RETD: { bg: "#fee2e2", fg: "#991b1b" },
    DONATE: { bg: "#dbeafe", fg: "#1e40af" },
    Damage: { bg: "#fee2e2", fg: "#991b1b" },
    FBM: { bg: "#ede9fe", fg: "#5b21b6" },
    HOLD: { bg: "#fce7f3", fg: "#9d174d" },
    CUST: { bg: "#ffedd5", fg: "#9a3412" },
    Transfer: { bg: "#e0f2fe", fg: "#0369a1" },
    Each: { bg: "#f3f4f6", fg: "#6b7280" },
    INBD: { bg: "#d1fae5", fg: "#065f46" },
  };
  const style = colors[suffix] || { bg: "#f3f4f6", fg: "#6b7280" };
  return (
    <span style={{
      display: "inline-block", padding: "1px 8px", borderRadius: 10,
      fontSize: 10, fontWeight: 600, background: style.bg, color: style.fg,
    }}>
      {suffix}
    </span>
  );
}

function ChannelBadge({ channel }) {
  const map = {
    Amazon: { color: "var(--orange)", bg: "rgba(232,120,48,0.1)" },
    Walmart: { color: "var(--blue-active)", bg: "rgba(62,101,140,0.1)" },
    "Walmart & Amazon": { color: "var(--sky-blue)", bg: "rgba(123,174,208,0.1)" },
    Other: { color: "var(--muted)", bg: "rgba(0,0,0,0.04)" },
  };
  const s = map[channel] || map.Other;
  return (
    <span style={{
      display: "inline-block", padding: "2px 10px", borderRadius: 12,
      fontSize: 11, fontWeight: 600, background: s.bg, color: s.color,
      border: `1px solid ${s.color}33`,
    }}>
      {channel || "—"}
    </span>
  );
}

export default function GolfGenInventory() {
  const [division, setDivision] = useState("Golf");
  const [channel, setChannel] = useState("All");
  const [data, setData] = useState(null);
  const [overviewData, setOverviewData] = useState(null); // both divisions summary
  const [loading, setLoading] = useState(true);
  const [expandedSku, setExpandedSku] = useState(null);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState("totalOnHand");
  const [sortDir, setSortDir] = useState("desc");
  const [uploading, setUploading] = useState(null); // "golf" or "housewares" or null
  const [uploadMsg, setUploadMsg] = useState(null);
  const [uploadMeta, setUploadMeta] = useState({});

  // Load overview summary (both divisions)
  useEffect(() => {
    api.warehouseSummary().then(d => setOverviewData(d)).catch(() => null);
    api.uploadMeta().then(d => setUploadMeta(d || {})).catch(() => {});
  }, []);

  // Load division data
  useEffect(() => {
    setLoading(true);
    setExpandedSku(null);
    const div = division === "Golf" ? "golf" : "housewares";
    api.warehouseUnified(div, division === "Golf" ? channel : null).then(d => {
      setData(d);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, [division, channel]);

  const handleUpload = async (e, div) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(div);
    setUploadMsg(null);
    try {
      const formData = new FormData();
      formData.append("file", file);
      const API_BASE = import.meta.env.VITE_API_URL || "";
      const res = await fetch(`${API_BASE}/api/upload/inventory-excel?division=${div}`, {
        method: "POST",
        body: formData,
        credentials: "include",
      });
      const result = await res.json();
      if (res.ok) {
        setUploadMsg({ type: "success", text: `${div === "golf" ? "Golf" : "Housewares"}: ${result.itemCount} items uploaded from ${file.name}` });
        // Refresh data and metadata
        api.uploadMeta().then(d => setUploadMeta(d || {})).catch(() => {});
        api.warehouseSummary().then(d => setOverviewData(d)).catch(() => null);
        // Reload current division data
        const curDiv = division === "Golf" ? "golf" : "housewares";
        api.warehouseUnified(curDiv, division === "Golf" ? channel : null).then(d => setData(d)).catch(() => {});
      } else {
        setUploadMsg({ type: "error", text: result.detail || "Upload failed" });
      }
    } catch (err) {
      setUploadMsg({ type: "error", text: err.message });
    } finally {
      setUploading(null);
      e.target.value = "";
    }
  };

  const formatDate = (iso) => {
    if (!iso) return "Never";
    const d = new Date(iso);
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  };

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  // Filter and sort
  const masters = data?.masters || [];
  const filtered = masters.filter(m => {
    if (!search) return true;
    const q = search.toLowerCase();
    return (m.baseSku || "").toLowerCase().includes(q) ||
           (m.description || "").toLowerCase().includes(q) ||
           (m.itemNumber || "").toLowerCase().includes(q);
  });

  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey] ?? "";
    const bv = b[sortKey] ?? "";
    if (typeof av === "number" && typeof bv === "number") return sortDir === "desc" ? bv - av : av - bv;
    return sortDir === "desc" ? String(bv).localeCompare(String(av)) : String(av).localeCompare(String(bv));
  });

  const summary = data?.summary || {};
  const suffixBreakdown = data?.suffixBreakdown || {};
  const channelBreakdown = data?.channelBreakdown || {};

  // Chart data
  const suffixChartData = Object.entries(suffixBreakdown)
    .map(([name, vals]) => ({ name, pcsOnHand: vals.pcsOnHand, skus: vals.skus }))
    .filter(d => d.pcsOnHand > 0)
    .sort((a, b) => b.pcsOnHand - a.pcsOnHand);

  const channelChartData = division === "Golf"
    ? Object.entries(channelBreakdown).map(([name, count]) => ({ name, value: count }))
    : [];

  return (
    <>
      <div className="page-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h1>GolfGen Inventory</h1>
          <p>3PL Warehouse Inventory &middot; Golf &amp; Housewares divisions</p>
        </div>
        <div style={{ display: "flex", gap: 12, alignItems: "flex-start" }}>
          {/* Golf Upload */}
          <div style={{ textAlign: "center" }}>
            <label style={{
              display: "inline-flex", alignItems: "center", gap: 6, padding: "6px 14px",
              background: "var(--teal)", color: "#fff", borderRadius: 8, cursor: "pointer",
              fontSize: 12, fontWeight: 600, opacity: uploading === "golf" ? 0.6 : 1,
              whiteSpace: "nowrap",
            }}>
              {uploading === "golf" ? "Uploading..." : "Upload Golf"}
              <input type="file" accept=".xlsx,.xls" onChange={e => handleUpload(e, "golf")}
                style={{ display: "none" }} disabled={uploading !== null} />
            </label>
            <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 3 }}>
              Last: {formatDate(uploadMeta.golf?.lastUpload)}
            </div>
          </div>
          {/* Housewares Upload */}
          <div style={{ textAlign: "center" }}>
            <label style={{
              display: "inline-flex", alignItems: "center", gap: 6, padding: "6px 14px",
              background: "var(--blue-active)", color: "#fff", borderRadius: 8, cursor: "pointer",
              fontSize: 12, fontWeight: 600, opacity: uploading === "housewares" ? 0.6 : 1,
              whiteSpace: "nowrap",
            }}>
              {uploading === "housewares" ? "Uploading..." : "Upload Housewares"}
              <input type="file" accept=".xlsx,.xls" onChange={e => handleUpload(e, "housewares")}
                style={{ display: "none" }} disabled={uploading !== null} />
            </label>
            <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 3 }}>
              Last: {formatDate(uploadMeta.housewares?.lastUpload)}
            </div>
          </div>
        </div>
      </div>

      {/* Upload Status Message */}
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

      {/* ── Overview Summary (both divisions) ── */}
      {overviewData && (
        <div className="kpi-grid" style={{ marginBottom: 24 }}>
          <div className="kpi-card">
            <div className="kpi-label">Golf — On Hand</div>
            <div className="kpi-value teal">{(overviewData.golf?.pcsOnHand || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Golf — Allocated</div>
            <div className="kpi-value" style={{ color: "var(--gold)" }}>{(overviewData.golf?.pcsAllocated || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Golf — Available</div>
            <div className="kpi-value pos">{(overviewData.golf?.pcsAvailable || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Housewares — On Hand</div>
            <div className="kpi-value teal">{(overviewData.housewares?.pcsOnHand || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Housewares — Allocated</div>
            <div className="kpi-value" style={{ color: "var(--gold)" }}>{(overviewData.housewares?.pcsAllocated || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Housewares — Available</div>
            <div className="kpi-value pos">{(overviewData.housewares?.pcsAvailable || 0).toLocaleString()}</div>
          </div>
        </div>
      )}

      {/* ── Suffix Breakdown Summary ── */}
      {Object.keys(suffixBreakdown).length > 0 && (
        <div className="table-card" style={{ marginBottom: 24, padding: 16 }}>
          <h3 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: 14, fontWeight: 600, color: "var(--navy)", marginBottom: 12 }}>
            {division} — Breakdown by Type
          </h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 10 }}>
            {Object.entries(suffixBreakdown)
              .sort((a, b) => b[1].pcsOnHand - a[1].pcsOnHand)
              .map(([suffix, vals]) => (
                <div key={suffix} style={{
                  padding: "10px 14px", borderRadius: 10,
                  background: "rgba(14,31,45,0.02)", border: "1px solid rgba(14,31,45,0.06)",
                }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 6 }}>
                    <SuffixBadge suffix={suffix} />
                    <span style={{ fontSize: 11, color: "var(--muted)" }}>{vals.skus} SKUs</span>
                  </div>
                  <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)" }}>
                    {vals.pcsOnHand.toLocaleString()}
                  </div>
                  <div style={{ fontSize: 11, color: "var(--muted)" }}>
                    Alloc: {vals.pcsAllocated.toLocaleString()} &middot; Avail: {vals.pcsAvailable.toLocaleString()}
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* ── Division Toggle + Channel Filter ── */}
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 20, flexWrap: "wrap" }}>
        <div className="range-tabs">
          {DIVISIONS.map(d => (
            <button key={d} className={`range-tab ${division === d ? "active" : ""}`}
              onClick={() => { setDivision(d); setChannel("All"); setSearch(""); }}>
              {d === "Golf" ? "⛳ Golf" : "🏠 Housewares"}
            </button>
          ))}
        </div>

        {division === "Golf" && (
          <div className="range-tabs">
            {GOLF_CHANNELS.map(ch => (
              <button key={ch} className={`range-tab ${channel === ch ? "active" : ""}`}
                onClick={() => setChannel(ch)}>
                {ch}
                {ch !== "All" && channelBreakdown[ch] != null && (
                  <span style={{ opacity: 0.7, marginLeft: 4, fontSize: 11 }}>({channelBreakdown[ch]})</span>
                )}
              </button>
            ))}
          </div>
        )}

        <input
          type="text" placeholder="Search SKU or description..."
          value={search} onChange={e => setSearch(e.target.value)}
          style={{
            padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8,
            fontSize: 13, width: 260, outline: "none",
          }}
        />
        <span style={{ fontSize: 13, color: "var(--muted)", marginLeft: "auto" }}>
          Showing {sorted.length} of {masters.length} items
        </span>
      </div>

      {/* ── Division KPIs ── */}
      {!loading && (
        <div className="kpi-grid" style={{ marginBottom: 24 }}>
          <div className="kpi-card">
            <div className="kpi-label">Master SKUs</div>
            <div className="kpi-value">{summary.totalSkus || 0}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Total Items</div>
            <div className="kpi-value">{summary.totalItems || 0}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Pcs On Hand</div>
            <div className="kpi-value teal">{(summary.totalOnHand || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Pcs Available</div>
            <div className="kpi-value pos">{(summary.totalAvailable || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Pcs Allocated</div>
            <div className="kpi-value" style={{ color: "var(--gold)" }}>{(summary.totalAllocated || 0).toLocaleString()}</div>
          </div>
          {(summary.totalDamage || 0) > 0 && (
            <div className="kpi-card">
              <div className="kpi-label">Damage</div>
              <div className="kpi-value neg">{(summary.totalDamage || 0).toLocaleString()}</div>
            </div>
          )}
        </div>
      )}

      {/* ── Charts Row ── */}
      {!loading && (
        <div style={{ display: "grid", gridTemplateColumns: channelChartData.length > 0 ? "1fr 1fr" : "1fr", gap: 20, marginBottom: 28 }}>
          {/* Suffix breakdown bar chart */}
          {suffixChartData.length > 0 && (
            <div className="chart-card">
              <h3>Inventory by Type</h3>
              <p className="sub">Pieces on hand by suffix type</p>
              <ResponsiveContainer width="100%" height={280}>
                <BarChart data={suffixChartData} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="rgba(14,31,45,0.06)" />
                  <XAxis dataKey="name" tick={{ fontSize: 11, fill: "#64748b" }} />
                  <YAxis tick={{ fontSize: 11, fill: "#64748b" }} tickFormatter={v => v >= 1000 ? `${(v/1000).toFixed(0)}k` : v} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(v) => [v.toLocaleString(), "Pcs On Hand"]} />
                  <Bar dataKey="pcsOnHand" radius={[6, 6, 0, 0]}>
                    {suffixChartData.map((entry, i) => (
                      <Cell key={i} fill={SUFFIX_COLORS[entry.name] || COLORS[i % COLORS.length]} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Channel pie chart (Golf only) */}
          {channelChartData.length > 0 && (
            <div className="chart-card">
              <h3>SKUs by Channel</h3>
              <p className="sub">Golf inventory distribution by sales channel</p>
              <ResponsiveContainer width="100%" height={280}>
                <PieChart>
                  <Pie data={channelChartData} cx="50%" cy="50%" outerRadius={100} dataKey="value"
                    label={({ name, value }) => `${name} (${value})`}>
                    {channelChartData.map((_, i) => (
                      <Cell key={i} fill={COLORS[i % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      )}

      {/* ── Loading State ── */}
      {loading && (
        <div className="loading"><div className="spinner" /> Loading {division.toLowerCase()} inventory...</div>
      )}

      {/* ── Main Table ── */}
      {!loading && (
        <div className="table-card">
          <table>
            <thead>
              <tr>
                <SortTH label="Product" field="description" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} style={{ minWidth: 280 }} />
                {division === "Golf" && <th>Channel</th>}
                <SortTH label="Pack" field="pack" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} />
                <SortTH label="On Hand" field="totalOnHand" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} />
                <SortTH label="Allocated" field="totalAllocated" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} />
                <SortTH label="Available" field="totalAvailable" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} />
                <SortTH label="Damage" field="totalDamage" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} />
                <th style={{ width: 50 }}></th>
              </tr>
            </thead>
            <tbody>
              {sorted.map(m => (
                <MasterRow
                  key={m.baseSku}
                  master={m}
                  showChannel={division === "Golf"}
                  expanded={expandedSku === m.baseSku}
                  onToggle={() => setExpandedSku(expandedSku === m.baseSku ? null : m.baseSku)}
                />
              ))}
            </tbody>
          </table>
          {sorted.length === 0 && (
            <div style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>
              No items found{search ? ` matching "${search}"` : ""}.
            </div>
          )}
        </div>
      )}

      <div style={{ marginTop: 16, padding: 12, background: "rgba(14,31,45,0.03)", borderRadius: 8, fontSize: 12, color: "var(--muted)" }}>
        <strong>Note:</strong> All quantities are in pieces (units). Click ▼ to expand sub-items (rebox, donate, returned, etc.).
        Suffix codes: RB = Rebox, RETD = Returned, DONATE = Donate, FBM = Fulfilled by Merchant, HOLD = Hold, CUST = Customer.
      </div>
    </>
  );
}

/* ── Master Row with Collapsible Subs ── */
function MasterRow({ master: m, showChannel, expanded, onToggle }) {
  const colSpan = showChannel ? 9 : 8;
  return (
    <>
      <tr>
        <td style={{ maxWidth: 320 }}>
          <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={m.description}>
            {m.description}
          </div>
          <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
            {m.baseSku}{m.asin ? ` · ${m.asin}` : ""}
          </div>
        </td>
        {showChannel && <td><ChannelBadge channel={m.channel} /></td>}
        <td>{m.pack}</td>
        <td style={{ fontWeight: 600 }}>{m.totalOnHand.toLocaleString()}</td>
        <td style={{ color: m.totalAllocated > 0 ? "var(--gold)" : "var(--muted)" }}>
          {m.totalAllocated > 0 ? m.totalAllocated.toLocaleString() : "—"}
        </td>
        <td style={{ fontWeight: 600, color: m.totalAvailable > 0 ? "#16a34a" : "#dc2626" }}>
          {m.totalAvailable.toLocaleString()}
        </td>
        <td style={{ color: m.totalDamage > 0 ? "#dc2626" : "var(--muted)" }}>
          {m.totalDamage > 0 ? m.totalDamage.toLocaleString() : "—"}
        </td>
        <td>
          {m.subCount > 0 ? (
            <button onClick={onToggle} style={{
              background: "none", border: "none", cursor: "pointer", fontSize: 14,
              color: expanded ? "var(--teal)" : "var(--muted)", fontWeight: 600,
            }}
              title={`${m.subCount} sub-item${m.subCount > 1 ? "s" : ""}`}>
              {expanded ? "▲" : "▼"} <span style={{ fontSize: 11 }}>{m.subCount}</span>
            </button>
          ) : (
            <span style={{ color: "var(--muted)", fontSize: 11 }}>—</span>
          )}
        </td>
      </tr>
      {expanded && m.subCount > 0 && (
        <tr>
          <td colSpan={colSpan} style={{ padding: 0 }}>
            <div style={{
              background: "rgba(14,31,45,0.03)", borderLeft: "3px solid var(--teal)",
              padding: "8px 16px 8px 32px",
            }}>
              <div style={{ fontSize: 11, fontWeight: 600, color: "var(--muted)", marginBottom: 6 }}>
                SUB-ITEMS — {m.subCount} variant{m.subCount > 1 ? "s" : ""} of {m.baseSku}
              </div>
              <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid rgba(14,31,45,0.1)" }}>
                    <th style={{ textAlign: "left", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Item Number</th>
                    <th style={{ textAlign: "left", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Type</th>
                    <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Pack</th>
                    <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>On Hand</th>
                    <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Allocated</th>
                    <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Available</th>
                    <th style={{ textAlign: "right", padding: "4px 8px", fontSize: 11, color: "var(--muted)" }}>Damage</th>
                  </tr>
                </thead>
                <tbody>
                  {/* Master's own row */}
                  <tr style={{ borderBottom: "1px solid rgba(14,31,45,0.06)", background: "rgba(46,207,170,0.04)" }}>
                    <td style={{ padding: "4px 8px", fontFamily: "monospace", fontWeight: 600 }}>{m.itemNumber}</td>
                    <td style={{ padding: "4px 8px" }}>
                      <span style={{ display: "inline-block", padding: "1px 8px", borderRadius: 10, fontSize: 10, fontWeight: 600, background: "#d1fae5", color: "#065f46" }}>Master</span>
                    </td>
                    <td style={{ textAlign: "right", padding: "4px 8px" }}>{m.pack}</td>
                    <td style={{ textAlign: "right", padding: "4px 8px", fontWeight: 600 }}>
                      {(m.totalOnHand - m.subs.reduce((s, sub) => s + (sub.pcsOnHand || 0), 0)).toLocaleString()}
                    </td>
                    <td style={{ textAlign: "right", padding: "4px 8px" }}>
                      {(() => { const v = m.totalAllocated - m.subs.reduce((s, sub) => s + (sub.pcsAllocated || 0), 0); return v > 0 ? v.toLocaleString() : "—"; })()}
                    </td>
                    <td style={{ textAlign: "right", padding: "4px 8px", fontWeight: 600, color: "#16a34a" }}>
                      {(m.totalAvailable - m.subs.reduce((s, sub) => s + (sub.pcsAvailable || 0), 0)).toLocaleString()}
                    </td>
                    <td style={{ textAlign: "right", padding: "4px 8px" }}>
                      {(() => { const v = m.totalDamage - m.subs.reduce((s, sub) => s + (sub.damage || 0), 0); return v > 0 ? v.toLocaleString() : "—"; })()}
                    </td>
                  </tr>
                  {m.subs.map((sub, idx) => (
                    <tr key={sub.itemNumber || idx} style={{
                      borderBottom: idx < m.subs.length - 1 ? "1px solid rgba(14,31,45,0.06)" : "none",
                    }}>
                      <td style={{ padding: "4px 8px", fontFamily: "monospace", fontSize: 11 }}>{sub.itemNumber}</td>
                      <td style={{ padding: "4px 8px" }}><SuffixBadge suffix={sub.suffix || "—"} /></td>
                      <td style={{ textAlign: "right", padding: "4px 8px" }}>{sub.pack}</td>
                      <td style={{ textAlign: "right", padding: "4px 8px" }}>{(sub.pcsOnHand || 0).toLocaleString()}</td>
                      <td style={{ textAlign: "right", padding: "4px 8px", color: (sub.pcsAllocated || 0) > 0 ? "var(--gold)" : "var(--muted)" }}>
                        {(sub.pcsAllocated || 0) > 0 ? sub.pcsAllocated.toLocaleString() : "—"}
                      </td>
                      <td style={{ textAlign: "right", padding: "4px 8px", color: (sub.pcsAvailable || 0) > 0 ? "#16a34a" : "var(--muted)" }}>
                        {(sub.pcsAvailable || 0).toLocaleString()}
                      </td>
                      <td style={{ textAlign: "right", padding: "4px 8px", color: (sub.damage || 0) > 0 ? "#dc2626" : "var(--muted)" }}>
                        {(sub.damage || 0) > 0 ? sub.damage.toLocaleString() : "—"}
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
  );
}

function SortTH({ label, field, sortKey, sortDir, onClick, style }) {
  const active = sortKey === field;
  return (
    <th onClick={() => onClick(field)} style={{ cursor: "pointer", userSelect: "none", ...(active ? { color: "var(--teal-dark)" } : {}), ...style }}>
      {label} {active ? (sortDir === "desc" ? "▼" : "▲") : ""}
    </th>
  );
}
