import { useState, useEffect } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { api } from "../lib/api";
import { CHART_COLORS as COLORS, TOOLTIP_STYLE } from "../lib/constants";

const DIVISIONS = ["Golf", "HW"];
const GOLF_CHANNELS = ["All", "Amazon", "Walmart"];

const SUFFIX_COLORS = {
  Standard: "#2ECFAA",
  RB: "#F5B731",
  RETD: "#D03030",
  DONATE: "#3E658C",
  Damage: "#dc2626",
  FBM: "#8B5CF6",
  HOLD: "#ec4899",
  CUST: "#E87830",
  "T-": "#7BAED0",
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
    "T-": { bg: "#e0f2fe", fg: "#0369a1" },
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

/* ── Mini KPI used in the executive summary card ── */
function MiniKPI({ label, value, color, sub }) {
  return (
    <div style={{ textAlign: "center", minWidth: 100 }}>
      <div style={{ fontSize: 11, color: "var(--muted)", fontWeight: 500, marginBottom: 2 }}>{label}</div>
      <div style={{
        fontSize: 22, fontWeight: 700, fontFamily: "'Space Grotesk', sans-serif",
        color: color || "var(--navy)", lineHeight: 1.1,
      }}>
        {typeof value === "number" ? value.toLocaleString() : value}
      </div>
      {sub && <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 2 }}>{sub}</div>}
    </div>
  );
}

export default function GolfGenInventory() {
  const [division, setDivision] = useState("Golf");
  const [channel, setChannel] = useState("All");
  const [data, setData] = useState(null);
  const [overviewData, setOverviewData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [expandedSku, setExpandedSku] = useState(null);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState("totalOnHand");
  const [sortDir, setSortDir] = useState("desc");
  const [uploading, setUploading] = useState(null);
  const [uploadMsg, setUploadMsg] = useState(null);
  const [uploadMeta, setUploadMeta] = useState({});

  useEffect(() => {
    api.warehouseSummary().then(d => setOverviewData(d)).catch(() => null);
    api.uploadMeta().then(d => setUploadMeta(d || {})).catch(() => {});
  }, []);

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
      const { ok, data: result } = await api.uploadInventoryExcel(file, div);
      if (ok) {
        setUploadMsg({ type: "success", text: `${div === "golf" ? "Golf" : "HW"}: ${result.itemCount} items uploaded from ${file.name}` });
        api.uploadMeta().then(d => setUploadMeta(d || {})).catch(() => {});
        api.warehouseSummary().then(d => setOverviewData(d)).catch(() => null);
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
    .map(([name, vals]) => ({ name, pcsOnHand: vals.pcsOnHand, pcsAvailable: vals.pcsAvailable, skus: vals.skus }))
    .filter(d => d.pcsOnHand > 0)
    .sort((a, b) => b.pcsOnHand - a.pcsOnHand);

  const channelChartData = division === "Golf"
    ? Object.entries(channelBreakdown).map(([name, count]) => ({ name, value: count }))
    : [];

  // Division label helper
  const divLabel = division === "Golf" ? "Golf" : "HW";

  return (
    <>
      <div className="page-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h1>GolfGen Inventory</h1>
          <p>3PL Warehouse Inventory &middot; Golf &amp; HW divisions</p>
        </div>
        <div style={{ display: "flex", gap: 12, alignItems: "flex-start" }}>
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
          <div style={{ textAlign: "center" }}>
            <label style={{
              display: "inline-flex", alignItems: "center", gap: 6, padding: "6px 14px",
              background: "var(--blue-active)", color: "#fff", borderRadius: 8, cursor: "pointer",
              fontSize: 12, fontWeight: 600, opacity: uploading === "housewares" ? 0.6 : 1,
              whiteSpace: "nowrap",
            }}>
              {uploading === "housewares" ? "Uploading..." : "Upload HW"}
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

      {/* ── Controls Bar: toggles + search + count — all on one line ── */}
      <div style={{
        display: "flex", gap: 10, alignItems: "center", marginBottom: 20, flexWrap: "wrap",
      }}>
        <div className="range-tabs">
          {DIVISIONS.map(d => (
            <button key={d} className={`range-tab ${division === d ? "active" : ""}`}
              onClick={() => { setDivision(d); setChannel("All"); setSearch(""); }}>
              {d === "Golf" ? "⛳ Golf" : "🏠 HW"}
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

        <div style={{ flex: 1 }} />

        <input
          type="text" placeholder="Search SKU or description..."
          value={search} onChange={e => setSearch(e.target.value)}
          style={{
            padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8,
            fontSize: 13, width: 240, outline: "none",
          }}
        />
        <span style={{ fontSize: 12, color: "var(--muted)", whiteSpace: "nowrap" }}>
          {sorted.length} of {masters.length}
        </span>
      </div>

      {/* ── Division KPIs (current filter) ── */}
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
            <div className="kpi-label">Pcs Allocated</div>
            <div className="kpi-value" style={{ color: "var(--gold)" }}>{(summary.totalAllocated || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Pcs Available</div>
            <div className="kpi-value pos">{(summary.totalAvailable || 0).toLocaleString()}</div>
          </div>
          {(summary.totalDamage || 0) > 0 && (
            <div className="kpi-card">
              <div className="kpi-label">Damage</div>
              <div className="kpi-value neg">{(summary.totalDamage || 0).toLocaleString()}</div>
            </div>
          )}
        </div>
      )}

      {/* ── Executive Summary — combined KPIs + breakdown chart ── */}
      {overviewData && !loading && (
        <div className="table-card" style={{ marginBottom: 24, padding: 0, overflow: "hidden" }}>
          {/* Top row: division KPI summaries side by side */}
          <div style={{
            display: "grid", gridTemplateColumns: "1fr 1fr",
            borderBottom: "1px solid rgba(14,31,45,0.08)",
          }}>
            {/* Golf summary */}
            <div style={{
              padding: "20px 24px",
              borderRight: "1px solid rgba(14,31,45,0.08)",
              background: division === "Golf" ? "rgba(46,207,170,0.04)" : "transparent",
            }}>
              <div style={{
                fontSize: 12, fontWeight: 600, color: "var(--teal)",
                textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 12,
              }}>
                ⛳ Golf
              </div>
              <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                <MiniKPI label="On Hand" value={overviewData.golf?.pcsOnHand || 0} color="var(--teal)" />
                <MiniKPI label="Allocated" value={overviewData.golf?.pcsAllocated || 0} color="var(--gold)" />
                <MiniKPI label="Available" value={overviewData.golf?.pcsAvailable || 0} color="#16a34a" />
                <MiniKPI label="SKUs" value={overviewData.golf?.skus || 0} />
              </div>
            </div>
            {/* HW summary */}
            <div style={{
              padding: "20px 24px",
              background: division === "HW" ? "rgba(62,101,140,0.04)" : "transparent",
            }}>
              <div style={{
                fontSize: 12, fontWeight: 600, color: "var(--blue-active)",
                textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 12,
              }}>
                🏠 HW
              </div>
              <div style={{ display: "flex", gap: 24, flexWrap: "wrap" }}>
                <MiniKPI label="On Hand" value={overviewData.housewares?.pcsOnHand || 0} color="var(--blue-active)" />
                <MiniKPI label="Allocated" value={overviewData.housewares?.pcsAllocated || 0} color="var(--gold)" />
                <MiniKPI label="Available" value={overviewData.housewares?.pcsAvailable || 0} color="#16a34a" />
                <MiniKPI label="SKUs" value={overviewData.housewares?.skus || 0} />
              </div>
            </div>
          </div>

          {/* Bottom row: breakdown chart + channel pie */}
          <div style={{
            display: "grid",
            gridTemplateColumns: channelChartData.length > 0 ? "1fr 320px" : "1fr",
            minHeight: 220,
          }}>
            {/* Horizontal bar chart — inventory by type */}
            {suffixChartData.length > 0 && (
              <div style={{ padding: "16px 20px" }}>
                <div style={{
                  fontSize: 12, fontWeight: 600, color: "var(--navy)", marginBottom: 8,
                  fontFamily: "'Space Grotesk', sans-serif",
                }}>
                  {divLabel} — Inventory by Type
                </div>
                <ResponsiveContainer width="100%" height={Math.max(180, suffixChartData.length * 34 + 20)}>
                  <BarChart data={suffixChartData} layout="vertical" margin={{ top: 0, right: 40, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="rgba(14,31,45,0.06)" />
                    <XAxis type="number" tick={{ fontSize: 10, fill: "#94a3b8" }}
                      tickFormatter={v => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} />
                    <YAxis type="category" dataKey="name" tick={{ fontSize: 11, fill: "#64748b" }} width={70} />
                    <Tooltip
                      contentStyle={TOOLTIP_STYLE}
                      formatter={(v, name) => [v.toLocaleString(), name === "pcsOnHand" ? "On Hand" : "Available"]}
                    />
                    <Bar dataKey="pcsOnHand" radius={[0, 4, 4, 0]} name="On Hand">
                      {suffixChartData.map((entry, i) => (
                        <Cell key={i} fill={SUFFIX_COLORS[entry.name] || COLORS[i % COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Channel pie (Golf only) */}
            {channelChartData.length > 0 && (
              <div style={{
                borderLeft: "1px solid rgba(14,31,45,0.08)",
                padding: "16px 20px",
                display: "flex", flexDirection: "column", alignItems: "center",
              }}>
                <div style={{
                  fontSize: 12, fontWeight: 600, color: "var(--navy)", marginBottom: 4,
                  fontFamily: "'Space Grotesk', sans-serif", alignSelf: "flex-start",
                }}>
                  SKUs by Channel
                </div>
                <ResponsiveContainer width="100%" height={210}>
                  <PieChart>
                    <Pie data={channelChartData} cx="50%" cy="45%" innerRadius={35} outerRadius={65} dataKey="value"
                      label={false}
                      labelLine={false}>
                      {channelChartData.map((_, i) => (
                        <Cell key={i} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                    <Legend
                      verticalAlign="bottom"
                      height={36}
                      iconType="circle"
                      iconSize={8}
                      formatter={(value, entry) => {
                        const item = channelChartData.find(d => d.name === value);
                        return <span style={{ fontSize: 11, color: "#64748b" }}>{value} ({item?.value || 0})</span>;
                      }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── Loading State ── */}
      {loading && (
        <div className="loading"><div className="spinner" /> Loading {divLabel.toLowerCase()} inventory...</div>
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
