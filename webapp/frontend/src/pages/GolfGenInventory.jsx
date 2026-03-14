import { useState, useEffect } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { api } from "../lib/api";
import { CHART_COLORS as COLORS, TOOLTIP_STYLE } from "../lib/constants";

const DIVISIONS    = ["Golf", "HW"];
const GOLF_CHANNELS = ["All", "Amazon", "Walmart"];

/* ── Suffix badge colors — semi-transparent, work on light & dark themes ── */
const SBCOLS = {
  Standard: { bg: "rgba(46,207,170,.15)",  fg: "#2ECFAA" },
  RB:       { bg: "rgba(245,183,49,.15)",  fg: "#F5B731" },
  RETD:     { bg: "rgba(208,48,48,.15)",   fg: "#D03030" },
  DONATE:   { bg: "rgba(62,101,140,.20)",  fg: "#7BAED0" },
  Damage:   { bg: "rgba(220,38,38,.15)",   fg: "#dc2626" },
  FBM:      { bg: "rgba(139,92,246,.15)",  fg: "#8B5CF6" },
  HOLD:     { bg: "rgba(236,72,153,.15)",  fg: "#ec4899" },
  CUST:     { bg: "rgba(232,120,48,.15)",  fg: "#E87830" },
  "T-":     { bg: "rgba(123,174,208,.15)", fg: "#7BAED0" },
  Each:     { bg: "rgba(148,163,184,.15)", fg: "var(--txt2)" },
  INBD:     { bg: "rgba(34,163,135,.15)",  fg: "#22A387" },
};

/* chart fill colors per suffix type */
const SUFFIX_COLORS = {
  Standard: "#2ECFAA", RB: "#F5B731", RETD: "#D03030",
  DONATE: "#3E658C", Damage: "#dc2626", FBM: "#8B5CF6",
  HOLD: "#ec4899", CUST: "#E87830", "T-": "#7BAED0", Each: "#94a3b8", INBD: "#22A387",
};

/* ── Sub-components ── */
function SuffixBadge({ suffix }) {
  const s = SBCOLS[suffix] || { bg: "rgba(148,163,184,.15)", fg: "var(--txt2)" };
  return (
    <span style={{
      display: "inline-block", padding: "1px 7px", borderRadius: 10,
      fontSize: 10, fontWeight: 600, background: s.bg, color: s.fg,
      whiteSpace: "nowrap",
    }}>
      {suffix}
    </span>
  );
}

function ChannelBadge({ channel }) {
  const map = {
    Amazon:            { color: "var(--orange)",     bg: "rgba(232,120,48,.12)" },
    Walmart:           { color: "var(--blue-active)", bg: "rgba(62,101,140,.12)" },
    "Walmart & Amazon":{ color: "var(--sky-blue)",   bg: "rgba(123,174,208,.12)" },
    Other:             { color: "var(--muted)",       bg: "rgba(148,163,184,.08)" },
  };
  const s = map[channel] || map.Other;
  return (
    <span style={{
      display: "inline-block", padding: "2px 9px", borderRadius: 12,
      fontSize: 11, fontWeight: 600, background: s.bg, color: s.color,
      border: `1px solid ${s.color}33`,
    }}>
      {channel || "—"}
    </span>
  );
}

function MiniKPI({ label, value, color, sub }) {
  return (
    <div style={{ textAlign: "center", minWidth: 88 }}>
      <div style={{ fontSize: 11, color: "var(--txt3)", fontWeight: 500, marginBottom: 2 }}>{label}</div>
      <div style={{
        fontSize: 20, fontWeight: 700, fontFamily: "'Space Grotesk',sans-serif",
        color: color || "var(--txt)", lineHeight: 1.1,
      }}>
        {typeof value === "number" ? value.toLocaleString() : value}
      </div>
      {sub && <div style={{ fontSize: 10, color: "var(--txt3)", marginTop: 2 }}>{sub}</div>}
    </div>
  );
}

function SortTH({ label, field, sortKey, sortDir, onClick, style }) {
  const active = sortKey === field;
  return (
    <th
      onClick={() => onClick(field)}
      style={{ cursor: "pointer", userSelect: "none", whiteSpace: "nowrap",
        ...(active ? { color: "var(--teal)" } : {}), ...style }}
    >
      {label} {active ? (sortDir === "desc" ? "▼" : "▲") : ""}
    </th>
  );
}

/* ── Main page component ── */
export default function GolfGenInventory({ filters = {} }) {
  const [division,     setDivision]    = useState("Golf");
  const [channel,      setChannel]     = useState("All");
  const [data,         setData]        = useState(null);
  const [overviewData, setOverviewData]= useState(null);
  const [loading,      setLoading]     = useState(true);
  const [expandedSku,  setExpandedSku] = useState(null);
  const [search,       setSearch]      = useState("");
  const [suffixFilter, setSuffixFilter]= useState("All");
  const [sortKey,      setSortKey]     = useState("totalOnHand");
  const [sortDir,      setSortDir]     = useState("desc");
  const [uploading,    setUploading]   = useState(null);
  const [uploadMsg,    setUploadMsg]   = useState(null);
  const [uploadMeta,   setUploadMeta]  = useState({});

  useEffect(() => {
    api.warehouseSummary().then(d => setOverviewData(d)).catch(() => null);
    api.uploadMeta().then(d => setUploadMeta(d || {})).catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    setExpandedSku(null);
    const div = division === "Golf" ? "golf" : "housewares";
    api.warehouseUnified(div, division === "Golf" ? channel : null)
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
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
        api.warehouseUnified(curDiv, division === "Golf" ? channel : null)
          .then(d => setData(d)).catch(() => {});
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
    return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  };

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const masters = data?.masters || [];

  /* All unique suffixes present in current division's data */
  const allSuffixes = [...new Set(
    masters.flatMap(m => (m.subs || []).map(s => s.suffix).filter(Boolean))
  )].sort();

  const filtered = masters.filter(m => {
    /* Suffix/type filter */
    if (suffixFilter === "Master") {
      /* Master filter: only show items that have their own inventory on the master level */
      const subSumOH = (m.subs || []).reduce((s, sub) => s + (sub.pcsOnHand || 0), 0);
      if ((m.totalOnHand || 0) - subSumOH <= 0) return false;
    } else if (suffixFilter !== "All") {
      if (!(m.subs || []).some(s => s.suffix === suffixFilter)) return false;
    }
    /* Search filter */
    if (!search) return true;
    const q = search.toLowerCase();
    return (m.baseSku || "").toLowerCase().includes(q) ||
           (m.description || "").toLowerCase().includes(q) ||
           (m.itemNumber || "").toLowerCase().includes(q);
  });
  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey] ?? "", bv = b[sortKey] ?? "";
    if (typeof av === "number" && typeof bv === "number")
      return sortDir === "desc" ? bv - av : av - bv;
    return sortDir === "desc"
      ? String(bv).localeCompare(String(av))
      : String(av).localeCompare(String(bv));
  });

  const summary          = data?.summary || {};
  const suffixBreakdown  = data?.suffixBreakdown || {};
  const channelBreakdown = data?.channelBreakdown || {};
  const showChannel      = division === "Golf";
  const divLabel         = division === "Golf" ? "Golf" : "HW";

  const suffixChartData = Object.entries(suffixBreakdown)
    .map(([name, vals]) => ({ name, pcsOnHand: vals.pcsOnHand, pcsAvailable: vals.pcsAvailable }))
    .filter(d => d.pcsOnHand > 0)
    .sort((a, b) => b.pcsOnHand - a.pcsOnHand);

  const channelChartData = showChannel
    ? Object.entries(channelBreakdown).map(([name, count]) => ({ name, value: count }))
    : [];

  return (
    <>
      {/* ══ ROW 1: Division + upload icons + Channel filter ══ */}
      <div style={{ display: "flex", gap: 10, alignItems: "center", marginBottom: 8, flexWrap: "wrap" }}>

        {/* Division tabs — each with an attached upload icon button */}
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          {DIVISIONS.map(d => {
            const divKey   = d === "Golf" ? "golf" : "housewares";
            const isActive = division === d;
            return (
              <div key={d} style={{ display: "flex", alignItems: "stretch", borderRadius: 7, overflow: "hidden",
                border: `1px solid ${isActive ? "var(--acc1)" : "var(--brd)"}`,
                background: isActive ? "rgba(46,207,170,.12)" : "var(--ibg)",
              }}>
                {/* Division select button */}
                <button
                  onClick={() => { setDivision(d); setChannel("All"); setSearch(""); setSuffixFilter("All"); }}
                  style={{
                    padding: "5px 12px 5px 10px", border: "none", cursor: "pointer",
                    background: "transparent", fontSize: 12, fontWeight: 700,
                    color: isActive ? "var(--acc1)" : "var(--txt3)", whiteSpace: "nowrap",
                  }}>
                  {d === "Golf" ? "⛳ Golf" : "🏠 HW"}
                </button>
                {/* Upload trigger — thin separator + up-arrow icon */}
                <label
                  title={`Upload ${d} inventory Excel`}
                  style={{
                    display: "inline-flex", alignItems: "center", justifyContent: "center",
                    padding: "0 8px", cursor: "pointer",
                    borderLeft: `1px solid ${isActive ? "rgba(46,207,170,.3)" : "var(--brd)"}`,
                    color: uploading === divKey ? "var(--txt3)" : (isActive ? "var(--acc1)" : "var(--txt3)"),
                    fontSize: 12, transition: "color .15s",
                    opacity: uploading !== null ? 0.5 : 1,
                  }}>
                  {uploading === divKey ? "…" : "⬆"}
                  <input type="file" accept=".xlsx,.xls"
                    onChange={e => handleUpload(e, divKey)}
                    style={{ display: "none" }} disabled={uploading !== null} />
                </label>
              </div>
            );
          })}
          {/* Last upload date hint */}
          <span style={{ fontSize: 10, color: "var(--txt3)", whiteSpace: "nowrap" }}>
            Last upload: {formatDate(uploadMeta[division === "Golf" ? "golf" : "housewares"]?.lastUpload)}
          </span>
        </div>

        {/* Divider */}
        <div style={{ width: 1, height: 22, background: "var(--brd2)", flexShrink: 0 }} />

        {/* Channel filter — Golf only */}
        {showChannel && (
          <div className="range-tabs">
            {GOLF_CHANNELS.map(ch => (
              <button key={ch} className={`range-tab ${channel === ch ? "active" : ""}`}
                onClick={() => setChannel(ch)}>
                {ch}
                {ch !== "All" && channelBreakdown[ch] != null && (
                  <span style={{ opacity: 0.6, marginLeft: 4, fontSize: 10 }}>({channelBreakdown[ch]})</span>
                )}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* ══ ROW 2: Type / suffix filter pills + search + count ══ */}
      <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>

        {/* Filter pills: All | Master | each suffix */}
        {["All", "Master", ...allSuffixes].map(sf => {
          const isActive = suffixFilter === sf;
          const isDanger = ["RETD", "DONATE", "Damage"].includes(sf);
          const activeColor   = isDanger ? "#D03030" : "var(--acc1)";
          const activeBg      = isDanger ? "rgba(208,48,48,.15)" : "rgba(46,207,170,.15)";
          const activeTxtClr  = isDanger ? "#D03030" : "#0E1F2D";
          return (
            <button key={sf} onClick={() => setSuffixFilter(sf)} style={{
              padding: "3px 11px", borderRadius: 99, fontSize: 11, fontWeight: 600,
              cursor: "pointer", border: `1px solid ${isActive ? activeColor : "var(--brd)"}`,
              background: isActive ? activeBg : "transparent",
              color: isActive ? (isDanger ? "#D03030" : activeColor) : "var(--txt3)",
              transition: "all .15s",
            }}>
              {sf}
            </button>
          );
        })}

        <div style={{ flex: 1 }} />

        {/* Search */}
        <input
          type="text" placeholder="🔍 Search SKU or description…"
          value={search} onChange={e => setSearch(e.target.value)}
          style={{
            padding: "5px 12px", border: "1px solid var(--brd)", borderRadius: 7,
            fontSize: 12, width: 220, outline: "none",
            background: "var(--ibg)", color: "var(--txt)",
          }}
        />
        <span style={{ fontSize: 11, color: "var(--txt3)", whiteSpace: "nowrap" }}>
          {sorted.length}/{masters.length}
        </span>
      </div>

      {/* ── Upload status banner ── */}
      {uploadMsg && (
        <div style={{
          padding: "8px 16px", marginBottom: 12, borderRadius: 8, fontSize: 13,
          background: uploadMsg.type === "success" ? "rgba(34,163,135,.12)" : "rgba(208,48,48,.12)",
          color:      uploadMsg.type === "success" ? "#22A387" : "#D03030",
          border:    `1px solid ${uploadMsg.type === "success" ? "rgba(34,163,135,.3)" : "rgba(208,48,48,.3)"}`,
          display: "flex", justifyContent: "space-between", alignItems: "center",
        }}>
          <span>{uploadMsg.text}</span>
          <button onClick={() => setUploadMsg(null)} style={{
            background: "none", border: "none", cursor: "pointer", fontSize: 18,
            color: "inherit", lineHeight: 1, padding: "0 4px",
          }}>×</button>
        </div>
      )}

      {/* ── KPI strip ── */}
      {!loading && (
        <div className="kpi-grid" style={{ marginBottom: 20 }}>
          <div className="kpi-card">
            <div className="kpi-label">Master SKUs</div>
            <div className="kpi-value">{summary.totalSkus || 0}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Total Items</div>
            <div className="kpi-value">{summary.totalItems || 0}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">On Hand</div>
            <div className="kpi-value" style={{ color: "var(--teal)" }}>{(summary.totalOnHand || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Allocated</div>
            <div className="kpi-value" style={{ color: "var(--gold)" }}>{(summary.totalAllocated || 0).toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Available</div>
            <div className="kpi-value pos">{(summary.totalAvailable || 0).toLocaleString()}</div>
          </div>
          {(summary.totalDamage || 0) > 0 && (
            <div className="kpi-card">
              <div className="kpi-label">Damage</div>
              <div className="kpi-value neg">{summary.totalDamage.toLocaleString()}</div>
            </div>
          )}
        </div>
      )}

      {/* ── Executive summary card: Golf vs HW + charts ── */}
      {overviewData && !loading && (
        <div className="table-card" style={{ marginBottom: 20, padding: 0, overflow: "hidden" }}>
          {/* Side-by-side division KPIs */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", borderBottom: "1px solid var(--brd)" }}>
            <div style={{
              padding: "18px 22px", borderRight: "1px solid var(--brd)",
              background: division === "Golf" ? "rgba(46,207,170,.04)" : "transparent",
            }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: "var(--teal)", textTransform: "uppercase", letterSpacing: ".5px", marginBottom: 10 }}>
                ⛳ Golf
              </div>
              <div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
                <MiniKPI label="On Hand"   value={overviewData.golf?.pcsOnHand  || 0} color="var(--teal)"  />
                <MiniKPI label="Allocated" value={overviewData.golf?.pcsAllocated||0} color="var(--gold)"  />
                <MiniKPI label="Available" value={overviewData.golf?.pcsAvailable||0} color="var(--pos)"   />
                <MiniKPI label="SKUs"      value={overviewData.golf?.skus        ||0}                      />
              </div>
            </div>
            <div style={{
              padding: "18px 22px",
              background: division === "HW" ? "rgba(62,101,140,.04)" : "transparent",
            }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: "var(--blue-active)", textTransform: "uppercase", letterSpacing: ".5px", marginBottom: 10 }}>
                🏠 HW
              </div>
              <div style={{ display: "flex", gap: 20, flexWrap: "wrap" }}>
                <MiniKPI label="On Hand"   value={overviewData.housewares?.pcsOnHand  ||0} color="var(--blue-active)" />
                <MiniKPI label="Allocated" value={overviewData.housewares?.pcsAllocated||0} color="var(--gold)"       />
                <MiniKPI label="Available" value={overviewData.housewares?.pcsAvailable||0} color="var(--pos)"        />
                <MiniKPI label="SKUs"      value={overviewData.housewares?.skus        ||0}                            />
              </div>
            </div>
          </div>

          {/* Charts row */}
          <div style={{
            display: "grid",
            gridTemplateColumns: channelChartData.length > 0 ? "1fr 300px" : "1fr",
            minHeight: 200,
          }}>
            {suffixChartData.length > 0 && (
              <div style={{ padding: "14px 18px" }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: "var(--txt2)", marginBottom: 6, fontFamily: "'Space Grotesk',sans-serif" }}>
                  {divLabel} — Inventory by Type
                </div>
                <ResponsiveContainer width="100%" height={Math.max(160, suffixChartData.length * 32 + 16)}>
                  <BarChart data={suffixChartData} layout="vertical" margin={{ top: 0, right: 36, left: 0, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="var(--brd)" />
                    <XAxis type="number" tick={{ fontSize: 10, fill: "var(--txt3)" }}
                      tickFormatter={v => v >= 1000 ? `${(v/1000).toFixed(0)}k` : v} />
                    <YAxis type="category" dataKey="name" tick={{ fontSize: 11, fill: "var(--txt2)" }} width={68} />
                    <Tooltip contentStyle={TOOLTIP_STYLE}
                      formatter={(v, n) => [v.toLocaleString(), n === "pcsOnHand" ? "On Hand" : "Available"]} />
                    <Bar dataKey="pcsOnHand" radius={[0,4,4,0]} name="On Hand">
                      {suffixChartData.map((entry, i) => (
                        <Cell key={i} fill={SUFFIX_COLORS[entry.name] || COLORS[i % COLORS.length]} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
            {channelChartData.length > 0 && (
              <div style={{ borderLeft: "1px solid var(--brd)", padding: "14px 18px", display: "flex", flexDirection: "column" }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: "var(--txt2)", marginBottom: 4, fontFamily: "'Space Grotesk',sans-serif" }}>
                  SKUs by Channel
                </div>
                <ResponsiveContainer width="100%" height={200}>
                  <PieChart>
                    <Pie data={channelChartData} cx="50%" cy="45%" innerRadius={32} outerRadius={60}
                      dataKey="value" label={false} labelLine={false}>
                      {channelChartData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                    </Pie>
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                    <Legend verticalAlign="bottom" height={32} iconType="circle" iconSize={7}
                      formatter={(value) => {
                        const item = channelChartData.find(d => d.name === value);
                        return <span style={{ fontSize: 10, color: "var(--txt2)" }}>{value} ({item?.value || 0})</span>;
                      }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── Loading ── */}
      {loading && (
        <div className="loading"><div className="spinner" /> Loading {divLabel.toLowerCase()} inventory…</div>
      )}

      {/* ── Main inventory table ── */}
      {!loading && (
        <div className="table-card" style={{ overflowX: "auto", padding: 0 }}>
          <table style={{ minWidth: showChannel ? 1280 : 1180, borderCollapse: "collapse", width: "100%" }}>
            <thead>
              <tr>
                <SortTH label="Product"   field="description"    sortKey={sortKey} sortDir={sortDir} onClick={handleSort} style={{ minWidth: 240, paddingLeft: 16 }} />
                {showChannel && <th style={{ minWidth: 90 }}>Channel</th>}
                <th style={{ minWidth: 110, whiteSpace: "nowrap" }}>Dimensions</th>
                <SortTH label="Pack"      field="pack"           sortKey={sortKey} sortDir={sortDir} onClick={handleSort} style={{ minWidth: 58, textAlign: "right" }} />
                <SortTH label="On Hand"   field="totalOnHand"    sortKey={sortKey} sortDir={sortDir} onClick={handleSort} style={{ minWidth: 78, textAlign: "right" }} />
                <SortTH label="Allocated" field="totalAllocated" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} style={{ minWidth: 78, textAlign: "right" }} />
                <SortTH label="Available" field="totalAvailable" sortKey={sortKey} sortDir={sortDir} onClick={handleSort} style={{ minWidth: 78, textAlign: "right" }} />
                <th style={{ minWidth: 80, textAlign: "right", whiteSpace: "nowrap" }}>Bad Inv</th>
                <th style={{ minWidth: 80, textAlign: "right", whiteSpace: "nowrap" }}>Good Inv</th>
                <th style={{ minWidth: 88, textAlign: "right", fontSize: 11, whiteSpace: "nowrap" }}>LM EOM</th>
                <th style={{ minWidth: 88, textAlign: "right", fontSize: 11, whiteSpace: "nowrap" }}>2M EOM</th>
                <th style={{ minWidth: 88, textAlign: "right", fontSize: 11, whiteSpace: "nowrap" }}>3M EOM</th>
                <th style={{ width: 52 }}></th>
              </tr>
            </thead>
            <tbody>
              {sorted.map(m => (
                <MasterRow
                  key={m.baseSku}
                  master={m}
                  showChannel={showChannel}
                  expanded={expandedSku === m.baseSku}
                  onToggle={() => setExpandedSku(expandedSku === m.baseSku ? null : m.baseSku)}
                />
              ))}
            </tbody>
          </table>
          {sorted.length === 0 && (
            <div style={{ textAlign: "center", padding: 40, color: "var(--txt3)" }}>
              No items found{search ? ` matching "${search}"` : ""}.
            </div>
          )}
        </div>
      )}

      {/* ── Footer note ── */}
      <div style={{
        marginTop: 14, padding: "10px 14px",
        background: "var(--card2)", border: "1px solid var(--brd)", borderRadius: 8,
        fontSize: 11, color: "var(--txt3)",
      }}>
        <strong style={{ color: "var(--txt2)" }}>Note:</strong> All quantities in pieces (units).
        Click ▼ to expand sub-items. <strong style={{ color: "var(--txt2)" }}>Bad Inv</strong> = Damaged + Returned (RETD) + Donate.{" "}
        <strong style={{ color: "var(--txt2)" }}>Good Inv</strong> = Available − Bad. EOM history columns pending snapshot data.{" "}
        Suffix codes: RB = Rebox · RETD = Returned · DONATE = Donate · FBM = FBM · HOLD = Hold · CUST = Customer.
      </div>
    </>
  );
}

/* ══════════════════════════════════════════════════════════
   MasterRow — returns a Fragment of <tr> elements so columns
   in sub-rows align perfectly with the parent table headers.
   ══════════════════════════════════════════════════════════ */
function MasterRow({ master: m, showChannel, expanded, onToggle }) {
  const numCols = showChannel ? 13 : 12;

  /* Bad inventory = all Damage + RETD pcsOnHand + DONATE pcsOnHand */
  const retdDonate = (m.subs || [])
    .filter(s => ["RETD", "DONATE"].includes(s.suffix))
    .reduce((sum, s) => sum + (s.pcsOnHand || 0), 0);
  const badInv  = (m.totalDamage || 0) + retdDonate;
  const goodInv = Math.max(0, (m.totalAvailable || 0) - badInv);

  /* Carton dimensions */
  const cL = m.cartonL || 0, cW = m.cartonW || 0, cH = m.cartonH || 0;
  const hasDims = cL > 0 && cW > 0 && cH > 0;
  const cbft    = hasDims ? ((cL * cW * cH) / 1728).toFixed(2) : null;

  /* Master's own quantities (total minus what belongs to subs) */
  const subSumOH    = (m.subs || []).reduce((s, sub) => s + (sub.pcsOnHand    || 0), 0);
  const subSumAlloc = (m.subs || []).reduce((s, sub) => s + (sub.pcsAllocated || 0), 0);
  const subSumAvail = (m.subs || []).reduce((s, sub) => s + (sub.pcsAvailable || 0), 0);
  const subSumDmg   = (m.subs || []).reduce((s, sub) => s + (sub.damage       || 0), 0);
  const masterOwnOH    = (m.totalOnHand    || 0) - subSumOH;
  const masterOwnAlloc = (m.totalAllocated || 0) - subSumAlloc;
  const masterOwnAvail = (m.totalAvailable || 0) - subSumAvail;
  const masterOwnDmg   = Math.max(0, (m.totalDamage || 0) - subSumDmg);
  const masterGood     = Math.max(0, masterOwnAvail - masterOwnDmg);

  /* Shared styles */
  const SUB_BG    = "var(--card2)";
  const SUB_BDR   = "3px solid var(--acc1)";
  const subThSt   = {
    textAlign: "right", padding: "3px 8px", fontSize: 10,
    color: "var(--txt3)", fontWeight: 700,
    background: "var(--card2)", borderBottom: "1px solid var(--brd)",
  };
  const subTdR    = { textAlign: "right", padding: "4px 8px", fontSize: 12 };

  return (
    <>
      {/* ── Master row ── */}
      <tr style={{ borderBottom: "1px solid var(--brd)" }}>

        {/* Product cell: description + GG# · ASIN · WM# */}
        <td style={{ maxWidth: 260, paddingLeft: 16 }}>
          <div style={{
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
            fontWeight: 600, color: "var(--txt)",
          }} title={m.description}>
            {m.description}
          </div>
          <div style={{
            fontSize: 10, color: "var(--txt3)", marginTop: 2,
            fontFamily: "'Space Grotesk',monospace",
            whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
          }}>
            {m.baseSku}
            {m.asin              ? ` · ${m.asin}` : ""}
            {m.walmartItemNumber ? ` · WM: ${m.walmartItemNumber}` : ""}
          </div>
        </td>

        {showChannel && <td><ChannelBadge channel={m.channel} /></td>}

        {/* Dimensions */}
        <td style={{ fontSize: 11, whiteSpace: "nowrap", color: "var(--txt2)" }}>
          {hasDims ? (
            <>
              <div>{cL}×{cW}×{cH}"</div>
              <div style={{ fontSize: 10, color: "var(--txt3)" }}>{cbft} ft³</div>
            </>
          ) : <span style={{ color: "var(--txt3)" }}>—</span>}
        </td>

        <td style={{ textAlign: "right" }}>{m.pack}</td>

        <td style={{ textAlign: "right", fontWeight: 600 }}>
          {(m.totalOnHand || 0).toLocaleString()}
        </td>

        <td style={{ textAlign: "right", color: (m.totalAllocated||0) > 0 ? "var(--gold)" : "var(--txt3)" }}>
          {(m.totalAllocated||0) > 0 ? m.totalAllocated.toLocaleString() : "—"}
        </td>

        <td style={{ textAlign: "right", fontWeight: 600,
          color: (m.totalAvailable||0) > 0 ? "var(--pos)" : "var(--neg)" }}>
          {(m.totalAvailable || 0).toLocaleString()}
        </td>

        {/* Bad Inventory */}
        <td style={{ textAlign: "right", color: badInv > 0 ? "var(--neg)" : "var(--txt3)" }}>
          {badInv > 0 ? badInv.toLocaleString() : "—"}
        </td>

        {/* Good Inventory */}
        <td style={{ textAlign: "right", fontWeight: goodInv > 0 ? 600 : 400,
          color: goodInv > 50 ? "var(--pos)" : goodInv > 0 ? "var(--gold)" : "var(--txt3)" }}>
          {goodInv > 0 ? goodInv.toLocaleString() : "—"}
        </td>

        {/* EOM placeholders */}
        <td style={{ textAlign: "right", color: "var(--txt3)", fontSize: 11 }}>—</td>
        <td style={{ textAlign: "right", color: "var(--txt3)", fontSize: 11 }}>—</td>
        <td style={{ textAlign: "right", color: "var(--txt3)", fontSize: 11 }}>—</td>

        {/* Expand toggle */}
        <td style={{ textAlign: "center", paddingRight: 8 }}>
          {m.subCount > 0 ? (
            <button onClick={onToggle} style={{
              background: "none", border: "none", cursor: "pointer", fontSize: 13,
              color: expanded ? "var(--teal)" : "var(--txt3)", fontWeight: 700,
            }} title={`${m.subCount} sub-item${m.subCount !== 1 ? "s" : ""}`}>
              {expanded ? "▲" : "▼"} <span style={{ fontSize: 10 }}>{m.subCount}</span>
            </button>
          ) : (
            <span style={{ color: "var(--txt3)", fontSize: 11 }}>—</span>
          )}
        </td>
      </tr>

      {/* ── Expanded sub-rows (inline in same table for perfect column alignment) ── */}
      {expanded && m.subCount > 0 && (
        <>
          {/* Section label banner */}
          <tr>
            <td colSpan={numCols} style={{
              padding: "3px 10px 3px 16px",
              background: SUB_BG, borderLeft: SUB_BDR,
              fontSize: 10, fontWeight: 700, color: "var(--txt3)",
              letterSpacing: ".05em", textTransform: "uppercase",
            }}>
              Sub-items — {m.subCount} variant{m.subCount !== 1 ? "s" : ""} of {m.baseSku}
            </td>
          </tr>

          {/* Sub-table column headers */}
          <tr style={{ borderLeft: SUB_BDR }}>
            <th style={{ ...subThSt, textAlign: "left", paddingLeft: 24 }}>Item Number</th>
            {showChannel && <th style={{ ...subThSt, textAlign: "left" }}>Type</th>}
            <th style={{ ...subThSt, textAlign: "left" }}>Dims</th>
            <th style={subThSt}>Pack</th>
            <th style={subThSt}>On Hand</th>
            <th style={subThSt}>Alloc</th>
            <th style={subThSt}>Avail</th>
            <th style={subThSt}>Bad</th>
            <th style={subThSt}>Good</th>
            <th style={subThSt}>LM EOM</th>
            <th style={subThSt}>2M EOM</th>
            <th style={subThSt}>3M EOM</th>
            <th style={{ ...subThSt, width: 52 }} />
          </tr>

          {/* Master's own standard-inventory row (totals minus sub quantities) */}
          <tr style={{ background: "rgba(46,207,170,.05)", borderLeft: SUB_BDR, borderBottom: "1px solid var(--brd)" }}>
            <td style={{ padding: "4px 8px 4px 24px", fontFamily: "monospace", fontWeight: 700, fontSize: 12, color: "var(--txt)" }}>
              {m.itemNumber}
            </td>
            {showChannel && (
              <td style={{ padding: "4px 8px" }}>
                <span style={{
                  display: "inline-block", padding: "1px 7px", borderRadius: 10,
                  fontSize: 10, fontWeight: 600, background: "rgba(46,207,170,.15)", color: "var(--teal)",
                }}>Master</span>
              </td>
            )}
            <td style={{ ...subTdR, textAlign: "left", color: "var(--txt3)" }}>—</td>
            <td style={subTdR}>{m.pack}</td>
            <td style={{ ...subTdR, fontWeight: 600 }}>{masterOwnOH.toLocaleString()}</td>
            <td style={{ ...subTdR, color: masterOwnAlloc > 0 ? "var(--gold)" : "var(--txt3)" }}>
              {masterOwnAlloc > 0 ? masterOwnAlloc.toLocaleString() : "—"}
            </td>
            <td style={{ ...subTdR, fontWeight: 600, color: masterOwnAvail > 0 ? "var(--pos)" : "var(--txt3)" }}>
              {masterOwnAvail.toLocaleString()}
            </td>
            <td style={{ ...subTdR, color: masterOwnDmg > 0 ? "var(--neg)" : "var(--txt3)" }}>
              {masterOwnDmg > 0 ? masterOwnDmg.toLocaleString() : "—"}
            </td>
            <td style={{ ...subTdR, color: masterGood > 0 ? "var(--pos)" : "var(--txt3)" }}>
              {masterGood > 0 ? masterGood.toLocaleString() : "—"}
            </td>
            <td style={{ ...subTdR, color: "var(--txt3)" }}>—</td>
            <td style={{ ...subTdR, color: "var(--txt3)" }}>—</td>
            <td style={{ ...subTdR, color: "var(--txt3)" }}>—</td>
            <td />
          </tr>

          {/* Sub-item rows */}
          {m.subs.map((sub, idx) => {
            /* Bad for a sub: if RETD/DONATE the entire pcsOnHand is bad; otherwise use damage field */
            const subBad  = ["RETD", "DONATE"].includes(sub.suffix)
              ? (sub.pcsOnHand || 0)
              : (sub.damage || 0);
            const subGood = Math.max(0, (sub.pcsAvailable || 0) - subBad);
            const isLast  = idx === m.subs.length - 1;

            return (
              <tr key={sub.itemNumber || idx} style={{
                background: idx % 2 === 0 ? SUB_BG : "var(--card)",
                borderLeft: SUB_BDR,
                borderBottom: isLast ? "2px solid var(--brd)" : "1px solid var(--brd)",
              }}>
                {/* Item number — if no channel col, show suffix badge inline */}
                <td style={{ padding: "4px 8px 4px 24px", fontFamily: "monospace", fontSize: 11, color: "var(--txt2)" }}>
                  {sub.itemNumber}
                  {!showChannel && sub.suffix && (
                    <span style={{ marginLeft: 8 }}><SuffixBadge suffix={sub.suffix} /></span>
                  )}
                </td>

                {/* If Golf: show suffix badge in Channel column */}
                {showChannel && (
                  <td style={{ padding: "4px 8px" }}>
                    <SuffixBadge suffix={sub.suffix || "—"} />
                  </td>
                )}

                <td style={{ ...subTdR, textAlign: "left", color: "var(--txt3)" }}>—</td>
                <td style={subTdR}>{sub.pack}</td>
                <td style={{ ...subTdR, fontWeight: 600 }}>{(sub.pcsOnHand || 0).toLocaleString()}</td>
                <td style={{ ...subTdR, color: (sub.pcsAllocated||0) > 0 ? "var(--gold)" : "var(--txt3)" }}>
                  {(sub.pcsAllocated||0) > 0 ? sub.pcsAllocated.toLocaleString() : "—"}
                </td>
                <td style={{ ...subTdR, color: (sub.pcsAvailable||0) > 0 ? "var(--pos)" : "var(--txt3)" }}>
                  {(sub.pcsAvailable || 0).toLocaleString()}
                </td>
                <td style={{ ...subTdR, color: subBad > 0 ? "var(--neg)" : "var(--txt3)" }}>
                  {subBad > 0 ? subBad.toLocaleString() : "—"}
                </td>
                <td style={{ ...subTdR, color: subGood > 0 ? "var(--pos)" : "var(--txt3)" }}>
                  {subGood > 0 ? subGood.toLocaleString() : "—"}
                </td>
                <td style={{ ...subTdR, color: "var(--txt3)" }}>—</td>
                <td style={{ ...subTdR, color: "var(--txt3)" }}>—</td>
                <td style={{ ...subTdR, color: "var(--txt3)" }}>—</td>
                <td />
              </tr>
            );
          })}
        </>
      )}
    </>
  );
}
