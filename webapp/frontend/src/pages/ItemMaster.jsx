import { useState, useEffect, useCallback } from "react";
import { api, fmt$ } from "../lib/api";
import { COLOR_BADGES } from "../lib/constants";

const COLOR_SECTIONS = ["All", "Green", "Blue", "Red", "Orange", "Accessories"];
const DIVISIONS = ["Golf", "Housewares"];
const GOLF_SUBTABS = ["All", "Amazon", "Walmart"];

function Badge({ color }) {
  const style = COLOR_BADGES[color] || COLOR_BADGES[""];
  return (
    <span style={{
      display: "inline-block", padding: "2px 10px", borderRadius: 12,
      fontSize: 12, fontWeight: 600, background: style.bg, color: style.color,
    }}>
      {color || "—"}
    </span>
  );
}

function EditableCell({ value, onSave, type = "number", prefix = "", suffix = "" }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);

  if (!editing) {
    return (
      <span
        onClick={() => { setDraft(value); setEditing(true); }}
        style={{ cursor: "pointer", borderBottom: "1px dashed rgba(0,0,0,0.2)", padding: "2px 4px" }}
        title="Click to edit"
      >
        {prefix}{type === "number" ? (value || 0).toLocaleString(undefined, { maximumFractionDigits: 2 }) : (value || "—")}{suffix}
      </span>
    );
  }

  return (
    <input
      autoFocus
      type={type === "number" ? "number" : "text"}
      value={draft}
      onChange={e => setDraft(type === "number" ? parseFloat(e.target.value) || 0 : e.target.value)}
      onBlur={() => { onSave(draft); setEditing(false); }}
      onKeyDown={e => {
        if (e.key === "Enter") { onSave(draft); setEditing(false); }
        if (e.key === "Escape") setEditing(false);
      }}
      style={{
        width: type === "number" ? 80 : 120, padding: "2px 6px",
        border: "2px solid var(--teal)", borderRadius: 4, fontSize: 13,
        outline: "none",
      }}
    />
  );
}

function CouponBadge({ state }) {
  if (!state) return null;
  const map = {
    ENABLED: { bg: "#dcfce7", color: "#166534", label: "Active" },
    SCHEDULED: { bg: "#dbeafe", color: "#1e40af", label: "Scheduled" },
    PAUSED: { bg: "#fef3c7", color: "#92400e", label: "Paused" },
    EXPIRED: { bg: "#f3f4f6", color: "#6b7280", label: "Expired" },
    ERRORED: { bg: "#fee2e2", color: "#991b1b", label: "Error" },
  };
  const style = map[state] || { bg: "#f3f4f6", color: "#6b7280", label: state };
  return (
    <span style={{
      display: "inline-block", padding: "2px 8px", borderRadius: 10,
      fontSize: 10, fontWeight: 600, background: style.bg, color: style.color,
    }}>
      {style.label}
    </span>
  );
}

function formatSyncTime(iso) {
  if (!iso) return "Never";
  const d = new Date(iso);
  const now = new Date();
  const diffMs = now - d;
  const diffMin = Math.round(diffMs / 60000);
  if (diffMin < 1) return "Just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.round(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function fmtDate(iso) {
  if (!iso) return null;
  return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

export default function ItemMaster() {
  const [division, setDivision] = useState("Golf");
  const [golfSubTab, setGolfSubTab] = useState("All");
  const [amazonItems, setAmazonItems] = useState([]);
  const [walmartItems, setWalmartItems] = useState(null);
  const [housewaresItems, setHousewaresItems] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(null);
  const [filter, setFilter] = useState("All");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState("lyRevenue");
  const [sortDir, setSortDir] = useState("desc");
  const [expandedAsin, setExpandedAsin] = useState(null);
  const [pricingStatus, setPricingStatus] = useState(null);
  const [syncing, setSyncing] = useState(false);
  const [pricingLastSync, setPricingLastSync] = useState(null);
  const [untaggedItems, setUntaggedItems] = useState([]);
  const [propagating, setPropagating] = useState(false);
  const [seeding, setSeeding] = useState(false);

  // Load Amazon items (main item master) on mount
  const loadAmazon = useCallback(() => {
    setLoading(true);
    api.itemMaster().then(d => {
      setAmazonItems(d.items || []);
      setPricingLastSync(d.pricingLastSync || null);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  useEffect(() => { loadAmazon(); }, [loadAmazon]);

  useEffect(() => {
    api.pricingStatus().then(setPricingStatus).catch(() => {});
  }, []);

  // Lazy-load Walmart and Housewares
  useEffect(() => {
    if (division === "Golf" && (golfSubTab === "All" || golfSubTab === "Walmart") && walmartItems === null) {
      api.itemMasterWalmart().then(d => setWalmartItems(d.items || [])).catch(() => setWalmartItems([]));
    }
    if (division === "Housewares" && housewaresItems === null) {
      setLoading(true);
      api.itemMasterHousewares().then(d => { setHousewaresItems(d.items || []); setLoading(false); }).catch(() => { setHousewaresItems([]); setLoading(false); });
    }
  }, [division, golfSubTab, walmartItems, housewaresItems]);

  // Load untagged ASINs from DuckDB item_master
  useEffect(() => {
    fetch("/api/item-master/untagged", { credentials: "include" })
      .then(r => r.json())
      .then(d => setUntaggedItems(d.items || []))
      .catch(() => {});
  }, []);

  const handleSeedItemMaster = async () => {
    setSeeding(true);
    try {
      const r = await fetch("/api/item-master/seed", { method: "POST", credentials: "include" });
      const d = await r.json();
      alert(`Seeded ${d.inserted} new ASINs into item_master (${d.existing} already existed)`);
      // Refresh untagged list
      fetch("/api/item-master/untagged", { credentials: "include" }).then(r => r.json()).then(d => setUntaggedItems(d.items || [])).catch(() => {});
    } catch (e) { alert("Seed failed: " + e.message); }
    setSeeding(false);
  };

  const handleSetDivisionTag = async (asin, div) => {
    try {
      await fetch(`/api/item-master/${asin}/division`, { credentials: "include",
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ division: div }),
      });
      setUntaggedItems(prev => prev.filter(i => i.asin !== asin));
    } catch (e) { alert("Failed to set division: " + e.message); }
  };

  const handleBulkSetDivision = async (div) => {
    const asins = untaggedItems.map(i => i.asin);
    if (!asins.length) return;
    if (!confirm(`Set ALL ${asins.length} untagged ASINs to "${div}"?`)) return;
    try {
      await fetch("/api/item-master/bulk-set-division", { credentials: "include",
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ asins, division: div }),
      });
      setUntaggedItems([]);
    } catch (e) { alert("Bulk set failed: " + e.message); }
  };

  const handlePropagate = async () => {
    setPropagating(true);
    try {
      const r = await fetch("/api/item-master/propagate-division", { method: "POST", credentials: "include" });
      const d = await r.json();
      const summary = Object.entries(d.updated || {}).map(([t, n]) => `${t}: ${n}`).join(", ");
      alert(`Division tags propagated to all historical data!\n${summary}`);
    } catch (e) { alert("Propagate failed: " + e.message); }
    setPropagating(false);
  };

  const renderUntaggedBanner = () => {
    // If no untagged items, show a seed button so Eric can discover new ASINs
    if (!untaggedItems || untaggedItems.length === 0) {
      return (
        <div style={{ background: "#e8f4fd", border: "1px solid #b6d4fe", borderRadius: 8, padding: 12, marginBottom: 20, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <span style={{ fontSize: 13, color: "#0d6efd" }}>All ASINs have division tags assigned.</span>
          <button onClick={handleSeedItemMaster} disabled={seeding}
            style={{ background: "#0d6efd", color: "#fff", border: "none", borderRadius: 4, padding: "6px 14px", cursor: "pointer", fontSize: 13 }}>
            {seeding ? "Scanning..." : "Scan for New ASINs"}
          </button>
        </div>
      );
    }
    return (
      <div style={{ background: "#fff3cd", border: "1px solid #ffc107", borderRadius: 8, padding: 16, marginBottom: 20 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <strong style={{ fontSize: 15 }}>{untaggedItems.length} ASIN{untaggedItems.length > 1 ? "s" : ""} need division tagging</strong>
          <div style={{ display: "flex", gap: 8 }}>
            <button onClick={() => handleBulkSetDivision("golf")}
              style={{ background: "#198754", color: "#fff", border: "none", borderRadius: 4, padding: "6px 12px", cursor: "pointer", fontSize: 13 }}>
              Set All → Golf
            </button>
            <button onClick={() => handleBulkSetDivision("housewares")}
              style={{ background: "#0d6efd", color: "#fff", border: "none", borderRadius: 4, padding: "6px 12px", cursor: "pointer", fontSize: 13 }}>
              Set All → Housewares
            </button>
          </div>
        </div>
        <div style={{ maxHeight: 200, overflowY: "auto", marginBottom: 12 }}>
          {untaggedItems.map(item => (
            <div key={item.asin} style={{ display: "flex", alignItems: "center", gap: 10, padding: "4px 0", borderBottom: "1px solid #eee" }}>
              <span style={{ fontFamily: "monospace", fontSize: 13, minWidth: 120 }}>{item.asin}</span>
              <span style={{ flex: 1, fontSize: 13, color: "#555" }}>{item.productName || "—"}</span>
              <button onClick={() => handleSetDivisionTag(item.asin, "golf")}
                style={{ background: "#198754", color: "#fff", border: "none", borderRadius: 4, padding: "3px 10px", cursor: "pointer", fontSize: 12 }}>Golf</button>
              <button onClick={() => handleSetDivisionTag(item.asin, "housewares")}
                style={{ background: "#0d6efd", color: "#fff", border: "none", borderRadius: 4, padding: "3px 10px", cursor: "pointer", fontSize: 12 }}>Housewares</button>
            </div>
          ))}
        </div>
        <button onClick={handlePropagate} disabled={propagating}
          style={{ background: "#6f42c1", color: "#fff", border: "none", borderRadius: 4, padding: "8px 16px", cursor: "pointer", fontSize: 14, width: "100%" }}>
          {propagating ? "Applying..." : "Apply Division Tags to All Historical Data"}
        </button>
      </div>
    );
  };

  const handlePricingSync = async () => {
    setSyncing(true);
    try {
      await api.triggerPricingSync();
      setTimeout(() => {
        api.pricingStatus().then(setPricingStatus).catch(() => {});
        loadAmazon();
        setSyncing(false);
      }, 15000);
    } catch (e) {
      console.error("Pricing sync failed:", e);
      setSyncing(false);
    }
  };

  const handleUpdate = async (asin, field, value) => {
    setSaving(asin);
    try {
      await api.updateItem(asin, { [field]: value });
      setAmazonItems(prev => prev.map(i => i.asin === asin ? { ...i, [field]: value } : i));
    } catch (e) {
      console.error("Save failed:", e);
    }
    setSaving(null);
  };

  const handleWalmartUpdate = async (golfgenItem, field, value) => {
    setSaving(golfgenItem);
    try {
      await api.updateWalmartItem(golfgenItem, { [field]: value });
      setWalmartItems(prev => prev.map(i => i.golfgenItem === golfgenItem ? { ...i, [field]: value } : i));
    } catch (e) {
      console.error("Save failed:", e);
    }
    setSaving(null);
  };

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const SortHeader = ({ label, field, style: s }) => (
    <th onClick={() => handleSort(field)} style={{ cursor: "pointer", userSelect: "none", ...s }}>
      {label} {sortKey === field ? (sortDir === "desc" ? "▼" : "▲") : ""}
    </th>
  );

  if (loading) return <div className="loading"><div className="spinner" /> Loading item master...</div>;

  /* ═══════════════════════════════════════════════════════════
     HOUSEWARES DIVISION
     ═══════════════════════════════════════════════════════════ */
  if (division === "Housewares") {
    const items = housewaresItems || [];
    const hw = items.filter(i => {
      if (!search) return true;
      const q = search.toLowerCase();
      return (i.itemNumber || "").toLowerCase().includes(q) ||
             (i.description || "").toLowerCase().includes(q);
    });

    const totalSkus = items.length;
    const totalOnHand = items.reduce((s, i) => s + (i.pcsOnHand || 0), 0);
    const totalAvailable = items.reduce((s, i) => s + (i.pcsAvailable || 0), 0);
    const totalAllocated = items.reduce((s, i) => s + (i.pcsAllocated || 0), 0);

    return (
      <>
        <div className="page-header">
          <h1>Item Master</h1>
          <p>Housewares &middot; {totalSkus} items</p>
        </div>

        {/* Division Tabs */}
        <div className="range-tabs" style={{ marginBottom: 20 }}>
          {DIVISIONS.map(t => (
            <button key={t} className={`range-tab ${division === t ? "active" : ""}`}
              onClick={() => { setDivision(t); setSearch(""); setFilter("All"); }}>{t}</button>
          ))}
        </div>

        {renderUntaggedBanner()}

        <div className="kpi-grid" style={{ marginBottom: 24 }}>
          <div className="kpi-card">
            <div className="kpi-label">Total SKUs</div>
            <div className="kpi-value teal">{totalSkus}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Pcs On Hand</div>
            <div className="kpi-value" style={{ color: "var(--navy)" }}>{totalOnHand.toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Pcs Available</div>
            <div className="kpi-value pos">{totalAvailable.toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Pcs Allocated</div>
            <div className="kpi-value" style={{ color: "var(--gold)" }}>{totalAllocated.toLocaleString()}</div>
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
          <input type="text" placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)}
            style={{ padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8, fontSize: 13, width: 260 }} />
          <span style={{ fontSize: 12, color: "var(--muted)", marginLeft: "auto" }}>
            {hw.length} of {totalSkus}
          </span>
        </div>

        <div className="table-card">
          <table>
            <thead>
              <tr>
                <th style={{ minWidth: 280, textAlign: "left" }}>Product</th>
                <th style={{ textAlign: "right" }}>Pack</th>
                <th style={{ textAlign: "right" }}>Pcs On Hand</th>
                <th style={{ textAlign: "right" }}>Pcs Allocated</th>
                <th style={{ textAlign: "right" }}>Pcs Available</th>
                <th style={{ textAlign: "right" }}>Non-Std</th>
                <th style={{ textAlign: "right" }}>Damage</th>
                <th style={{ textAlign: "right" }}>QC Hold</th>
              </tr>
            </thead>
            <tbody>
              {hw.map((item, i) => (
                <tr key={i}>
                  <td>
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 320 }} title={item.description}>
                      {item.description || "—"}
                    </div>
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.itemNumber || "—"}
                    </div>
                  </td>
                  <td style={{ textAlign: "right" }}>{item.pack || "—"}</td>
                  <td style={{ textAlign: "right" }}>{(item.pcsOnHand || 0).toLocaleString()}</td>
                  <td style={{ textAlign: "right", color: (item.pcsAllocated || 0) > 0 ? "var(--gold)" : "var(--muted)" }}>
                    {(item.pcsAllocated || 0) > 0 ? item.pcsAllocated.toLocaleString() : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontWeight: 600, color: (item.pcsAvailable || 0) > 0 ? "#16a34a" : "var(--muted)" }}>
                    {(item.pcsAvailable || 0).toLocaleString()}
                  </td>
                  <td style={{ textAlign: "right", color: (item.nonStandard || 0) > 0 ? "#dc2626" : "var(--muted)" }}>
                    {(item.nonStandard || 0) > 0 ? item.nonStandard.toLocaleString() : "—"}
                  </td>
                  <td style={{ textAlign: "right", color: (item.damage || 0) > 0 ? "#dc2626" : "var(--muted)" }}>
                    {(item.damage || 0) > 0 ? item.damage.toLocaleString() : "—"}
                  </td>
                  <td style={{ textAlign: "right", color: (item.qcHold || 0) > 0 ? "var(--gold)" : "var(--muted)" }}>
                    {(item.qcHold || 0) > 0 ? item.qcHold.toLocaleString() : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </>
    );
  }

  /* ═══════════════════════════════════════════════════════════
     GOLF DIVISION
     ═══════════════════════════════════════════════════════════ */

  /* ─── Golf > Walmart sub-tab ─── */
  if (golfSubTab === "Walmart") {
    const wm = walmartItems || [];
    const wmFiltered = wm.filter(i => {
      if (!search) return true;
      const q = search.toLowerCase();
      return (i.golfgenItem || i.itemNumber || "").toLowerCase().includes(q) ||
             (i.walmartItem || "").toLowerCase().includes(q) ||
             (i.description || "").toLowerCase().includes(q);
    });

    const totalSkus = wm.length;
    const totalStores = wm.reduce((s, i) => s + (i.storeCount || 0), 0);
    const avgCost = totalSkus > 0 ? wm.reduce((s, i) => s + (i.unitCost || 0), 0) / totalSkus : 0;
    const avgRetail = totalSkus > 0 ? wm.reduce((s, i) => s + (i.unitRetail || 0), 0) / totalSkus : 0;
    const totalPlannedWm = wm.reduce((s, i) => s + (i.plannedAnnualUnits || 0), 0);
    const categories = [...new Set(wm.map(i => i.subcategory || i.category).filter(Boolean))];

    return (
      <>
        <div className="page-header">
          <h1>Item Master</h1>
          <p>Golf &middot; Walmart &middot; {totalSkus} SKUs</p>
        </div>

        {/* Division Tabs */}
        <div className="range-tabs" style={{ marginBottom: 12 }}>
          {DIVISIONS.map(t => (
            <button key={t} className={`range-tab ${division === t ? "active" : ""}`}
              onClick={() => { setDivision(t); setSearch(""); setFilter("All"); }}>{t}</button>
          ))}
        </div>
        {/* Golf Sub-tabs */}
        <div className="range-tabs" style={{ marginBottom: 20 }}>
          {GOLF_SUBTABS.map(t => (
            <button key={t} className={`range-tab ${golfSubTab === t ? "active" : ""}`}
              onClick={() => { setGolfSubTab(t); setSearch(""); setFilter("All"); }}>{t}</button>
          ))}
        </div>

        {renderUntaggedBanner()}

        <div className="kpi-grid" style={{ marginBottom: 24 }}>
          <div className="kpi-card">
            <div className="kpi-label">Total SKUs</div>
            <div className="kpi-value teal">{totalSkus}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Total Stores</div>
            <div className="kpi-value" style={{ color: "var(--blue-active)" }}>{totalStores.toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Avg Unit Cost</div>
            <div className="kpi-value" style={{ color: "var(--navy)" }}>${avgCost.toFixed(2)}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Avg Unit Retail</div>
            <div className="kpi-value" style={{ color: "var(--orange)" }}>${avgRetail.toFixed(2)}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">FY26 Planned Units</div>
            <div className="kpi-value" style={{ color: "var(--orange)" }}>{totalPlannedWm.toLocaleString()}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Categories</div>
            <div className="kpi-value">{categories.length}</div>
            <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 4 }}>{categories.join(", ")}</div>
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
          <input type="text" placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)}
            style={{ padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8, fontSize: 13, width: 260 }} />
          <span style={{ fontSize: 12, color: "var(--muted)", marginLeft: "auto" }}>
            {wmFiltered.length} of {totalSkus} &middot; Click values to edit
          </span>
        </div>

        <div className="table-card">
          <table>
            <thead>
              <tr>
                <th style={{ minWidth: 260, textAlign: "left" }}>Product</th>
                <th style={{ textAlign: "left" }}>Color</th>
                <th style={{ textAlign: "left" }}>Series</th>
                <th style={{ textAlign: "left" }}>Type</th>
                <th style={{ textAlign: "right" }}>Pcs</th>
                <th style={{ textAlign: "left" }}>Hand</th>
                <th style={{ textAlign: "left" }}>Category</th>
                <th style={{ textAlign: "right" }}>Casepack</th>
                <th style={{ textAlign: "right" }}>Carton (L×W×H)</th>
                <th style={{ textAlign: "right" }}>Unit Cost</th>
                <th style={{ textAlign: "right" }}>Unit Retail</th>
                <th style={{ textAlign: "right" }}>Store Count</th>
                <th style={{ textAlign: "right" }}>FY26 Plan</th>
              </tr>
            </thead>
            <tbody>
              {wmFiltered.map((item, i) => (
                <tr key={i} style={{ background: saving === item.golfgenItem ? "rgba(46,207,170,0.08)" : undefined }}>
                  <td>
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 300 }} title={item.description}>
                      {item.description || "—"}
                    </div>
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.golfgenItem || item.itemNumber || "—"}
                      {item.walmartItem ? ` · ${item.walmartItem}` : ""}
                    </div>
                  </td>
                  <td>{item.color ? <Badge color={item.color} /> : "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.series || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.productType || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.pieceCount || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.orientation || "—"}</td>
                  <td style={{ fontSize: 12, color: "var(--muted)" }}>{item.itemCategory || item.subcategory || item.category || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.casepack || "—"}</td>
                  <td style={{ textAlign: "right", fontSize: 11, color: "var(--muted)" }}>
                    {item.cartonLength && item.cartonWidth && item.cartonHeight
                      ? `${item.cartonLength}×${item.cartonWidth}×${item.cartonHeight}`
                      : "—"}
                  </td>
                  <td style={{ textAlign: "right" }}>{item.unitCost ? `$${item.unitCost.toFixed(2)}` : "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.unitRetail ? `$${item.unitRetail.toFixed(2)}` : "—"}</td>
                  <td style={{ textAlign: "right", fontWeight: 600 }}>{(item.storeCount || 0).toLocaleString()}</td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.plannedAnnualUnits || 0}
                      onSave={v => handleWalmartUpdate(item.golfgenItem, "plannedAnnualUnits", Math.round(v))} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div style={{ marginTop: 16, padding: 12, background: "rgba(14,31,45,0.03)", borderRadius: 8, fontSize: 12, color: "var(--muted)" }}>
          <strong>Tip:</strong> Click any blue-underlined value in the FY26 Plan column to edit it inline. Changes save automatically.
        </div>
      </>
    );
  }

  /* ─── Golf > All sub-tab ─── */
  // "All" merges Amazon and Walmart items into a single unified view
  const isAllTab = golfSubTab === "All";

  // Build a unified list for the "All" tab
  let allGolfItems = [];
  if (isAllTab) {
    // Amazon items
    for (const a of amazonItems) {
      allGolfItems.push({
        id: a.asin,
        name: a.productName,
        sku: a.sku,
        secondaryId: a.asin,
        channel: "Amazon",
        color: a.color,
        brand: a.brand || "",
        series: a.series || "",
        productType: a.productType || "",
        pieceCount: a.pieceCount || "",
        orientation: a.orientation || "",
        category: a.category || "",
        casepack: a.casepack || a.cartonPack || 0,
        cartonLength: a.cartonLength || 0,
        cartonWidth: a.cartonWidth || 0,
        cartonHeight: a.cartonHeight || 0,
        unitCost: a.unitCost,
        plannedAnnualUnits: a.plannedAnnualUnits || 0,
        lyRevenue: a.lyRevenue || 0,
        lyUnits: a.lyUnits || 0,
        listPrice: a.listPrice || 0,
        salePrice: a.salePrice || 0,
        netPrice: a.netPrice || 0,
      });
    }
    // Walmart items
    for (const w of (walmartItems || [])) {
      allGolfItems.push({
        id: w.golfgenItem,
        name: w.description,
        sku: w.golfgenItem || w.itemNumber,
        secondaryId: w.walmartItem,
        channel: "Walmart",
        color: w.color || "",
        brand: w.brand || "",
        series: w.series || "",
        productType: w.productType || "",
        pieceCount: w.pieceCount || "",
        orientation: w.orientation || "",
        category: w.itemCategory || w.subcategory || "",
        casepack: w.casepack || 0,
        cartonLength: w.cartonLength || 0,
        cartonWidth: w.cartonWidth || 0,
        cartonHeight: w.cartonHeight || 0,
        unitCost: w.unitCost || 0,
        plannedAnnualUnits: w.plannedAnnualUnits || 0,
        lyRevenue: 0,
        lyUnits: 0,
        listPrice: 0,
        salePrice: 0,
        netPrice: w.unitRetail || 0,
      });
    }
  }

  // Apply search and filter
  const items = isAllTab ? allGolfItems : amazonItems;

  const filtered = items.filter(i => {
    if (isAllTab) {
      if (search) {
        const q = search.toLowerCase();
        return (i.name || "").toLowerCase().includes(q) ||
               (i.sku || "").toLowerCase().includes(q) ||
               (i.secondaryId || "").toLowerCase().includes(q);
      }
      return true;
    }
    // Amazon tab filters
    if (filter === "Accessories") return !i.color || i.color === "";
    if (filter !== "All" && i.color !== filter) return false;
    if (search) {
      const q = search.toLowerCase();
      return (i.productName || "").toLowerCase().includes(q) ||
             (i.sku || "").toLowerCase().includes(q) ||
             (i.asin || "").toLowerCase().includes(q);
    }
    return true;
  });

  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey] ?? "";
    const bv = b[sortKey] ?? "";
    if (typeof av === "number" && typeof bv === "number") return sortDir === "desc" ? bv - av : av - bv;
    return sortDir === "desc" ? String(bv).localeCompare(String(av)) : String(av).localeCompare(String(bv));
  });

  /* ─── Golf > All view ─── */
  if (isAllTab) {
    const totalAmazon = amazonItems.length;
    const totalWalmart = (walmartItems || []).length;
    const totalAll = totalAmazon + totalWalmart;
    const totalLyRev = amazonItems.reduce((s, i) => s + (i.lyRevenue || 0), 0);
    const totalPlannedAll = allGolfItems.reduce((s, i) => s + (i.plannedAnnualUnits || 0), 0);

    return (
      <>
        <div className="page-header">
          <h1>Item Master</h1>
          <p>Golf &middot; All Channels &middot; {totalAll} SKUs ({totalAmazon} Amazon + {totalWalmart} Walmart)</p>
        </div>

        {/* Division Tabs */}
        <div className="range-tabs" style={{ marginBottom: 12 }}>
          {DIVISIONS.map(t => (
            <button key={t} className={`range-tab ${division === t ? "active" : ""}`}
              onClick={() => { setDivision(t); setSearch(""); setFilter("All"); }}>{t}</button>
          ))}
        </div>
        {/* Golf Sub-tabs */}
        <div className="range-tabs" style={{ marginBottom: 20 }}>
          {GOLF_SUBTABS.map(t => (
            <button key={t} className={`range-tab ${golfSubTab === t ? "active" : ""}`}
              onClick={() => { setGolfSubTab(t); setSearch(""); setFilter("All"); setSortKey("lyRevenue"); }}>{t}</button>
          ))}
        </div>

        {renderUntaggedBanner()}

        <div className="kpi-grid" style={{ marginBottom: 24 }}>
          <div className="kpi-card">
            <div className="kpi-label">Total SKUs</div>
            <div className="kpi-value teal">{totalAll}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Amazon SKUs</div>
            <div className="kpi-value" style={{ color: "var(--blue-active)" }}>{totalAmazon}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Walmart SKUs</div>
            <div className="kpi-value" style={{ color: "var(--orange)" }}>{totalWalmart}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">Amazon LY Revenue</div>
            <div className="kpi-value" style={{ color: "var(--navy)" }}>{fmt$(totalLyRev)}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">FY26 Planned Units</div>
            <div className="kpi-value" style={{ color: "var(--orange)" }}>{totalPlannedAll.toLocaleString()}</div>
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
          <input type="text" placeholder="Search SKU, ASIN, or name..."
            value={search} onChange={e => setSearch(e.target.value)}
            style={{ padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8, fontSize: 13, width: 260 }} />
          <span style={{ fontSize: 12, color: "var(--muted)", marginLeft: "auto" }}>
            {sorted.length} of {totalAll}
          </span>
        </div>

        <div className="table-card" style={{ overflowX: "auto" }}>
          <table style={{ fontSize: 13 }}>
            <thead>
              <tr>
                <SortHeader label="Product" field="name" style={{ minWidth: 280, textAlign: "left" }} />
                <th style={{ textAlign: "left" }}>Channel</th>
                <SortHeader label="Color" field="color" style={{ textAlign: "left" }} />
                <th style={{ textAlign: "left" }}>Series</th>
                <th style={{ textAlign: "left" }}>Type</th>
                <th style={{ textAlign: "right" }}>Pcs</th>
                <th style={{ textAlign: "left" }}>Hand</th>
                <th style={{ textAlign: "left" }}>Category</th>
                <th style={{ textAlign: "right" }}>Casepack</th>
                <th style={{ textAlign: "right" }}>Carton (L×W×H)</th>
                <SortHeader label="COGS" field="unitCost" style={{ textAlign: "right" }} />
                <SortHeader label="Price" field="netPrice" style={{ textAlign: "right" }} />
                <SortHeader label="FY26 Plan" field="plannedAnnualUnits" style={{ textAlign: "right" }} />
                <SortHeader label="LY Rev" field="lyRevenue" style={{ textAlign: "right" }} />
                <SortHeader label="LY Units" field="lyUnits" style={{ textAlign: "right" }} />
              </tr>
            </thead>
            <tbody>
              {sorted.map((item, i) => (
                <tr key={item.id || i}>
                  <td style={{ maxWidth: 300 }}>
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={item.name}>
                      {item.name || "—"}
                    </div>
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.sku}{item.secondaryId ? ` · ${item.secondaryId}` : ""}
                    </div>
                  </td>
                  <td>
                    <span style={{
                      display: "inline-block", padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600,
                      background: item.channel === "Amazon" ? "rgba(62,101,140,0.1)" : "rgba(232,120,48,0.1)",
                      color: item.channel === "Amazon" ? "var(--blue-active)" : "var(--orange)",
                    }}>
                      {item.channel}
                    </span>
                  </td>
                  <td>{item.color ? <Badge color={item.color} /> : "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.series || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.productType || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.pieceCount || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.orientation || "—"}</td>
                  <td style={{ fontSize: 12, color: "var(--muted)" }}>{item.category || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.casepack || "—"}</td>
                  <td style={{ textAlign: "right", fontSize: 11, color: "var(--muted)" }}>
                    {item.cartonLength && item.cartonWidth && item.cartonHeight
                      ? `${item.cartonLength}×${item.cartonWidth}×${item.cartonHeight}`
                      : "—"}
                  </td>
                  <td style={{ textAlign: "right" }}>{item.unitCost > 0 ? `$${item.unitCost.toFixed(2)}` : "—"}</td>
                  <td style={{ textAlign: "right", fontWeight: 600 }}>{item.netPrice > 0 ? `$${item.netPrice.toFixed(2)}` : "—"}</td>
                  <td style={{ textAlign: "right" }}>{(item.plannedAnnualUnits || 0).toLocaleString()}</td>
                  <td style={{ textAlign: "right" }}>{item.lyRevenue > 0 ? fmt$(item.lyRevenue) : "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.lyUnits > 0 ? item.lyUnits.toLocaleString() : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </>
    );
  }

  /* ═══════════════════════════════════════════════════════════
     GOLF > AMAZON sub-tab (full detail view)
     ═══════════════════════════════════════════════════════════ */
  const totalItems = amazonItems.length;
  const totalPlanned = amazonItems.reduce((s, i) => s + (i.plannedAnnualUnits || 0), 0);
  const totalLyRev = amazonItems.reduce((s, i) => s + (i.lyRevenue || 0), 0);
  const colorsCount = [...new Set(amazonItems.map(i => i.color).filter(Boolean))].length;
  const livePricedCount = amazonItems.filter(i => i.liveBuyBoxPrice != null).length;
  const activeCouponCount = amazonItems.filter(i => i.liveCouponState === "ENABLED").length;

  return (
    <>
      <div className="page-header" style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h1>Item Master</h1>
          <p>Golf &middot; Amazon &middot; {totalItems} SKUs &middot; {colorsCount} colors</p>
        </div>
        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          {pricingLastSync && (
            <span style={{ fontSize: 11, color: "var(--muted)" }}>
              Pricing: {formatSyncTime(pricingLastSync)}
            </span>
          )}
          <button
            onClick={handlePricingSync}
            disabled={syncing}
            style={{
              padding: "6px 14px", borderRadius: 8, fontSize: 12, fontWeight: 600,
              background: syncing ? "rgba(14,31,45,0.05)" : "var(--teal)",
              color: syncing ? "var(--muted)" : "#fff",
              border: "none", cursor: syncing ? "default" : "pointer",
              opacity: syncing ? 0.6 : 1,
            }}
          >
            {syncing ? "Syncing..." : "Sync Pricing"}
          </button>
        </div>
      </div>

      {/* Division Tabs */}
      <div className="range-tabs" style={{ marginBottom: 12 }}>
        {DIVISIONS.map(t => (
          <button key={t} className={`range-tab ${division === t ? "active" : ""}`}
            onClick={() => { setDivision(t); setSearch(""); setFilter("All"); }}>{t}</button>
        ))}
      </div>
      {/* Golf Sub-tabs */}
      <div className="range-tabs" style={{ marginBottom: 20 }}>
        {GOLF_SUBTABS.map(t => (
          <button key={t} className={`range-tab ${golfSubTab === t ? "active" : ""}`}
            onClick={() => { setGolfSubTab(t); setSearch(""); setFilter("All"); }}>{t}</button>
        ))}
      </div>

      {renderUntaggedBanner()}

      {/* Summary Cards */}
      <div className="kpi-grid" style={{ marginBottom: 24 }}>
        <div className="kpi-card">
          <div className="kpi-label">Total SKUs</div>
          <div className="kpi-value teal">{totalItems}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">LY Revenue</div>
          <div className="kpi-value" style={{ color: "var(--navy)" }}>{fmt$(totalLyRev)}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">FY26 Planned Units</div>
          <div className="kpi-value" style={{ color: "var(--orange)" }}>{totalPlanned.toLocaleString()}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Color Groups</div>
          <div className="kpi-value" style={{ color: "#7BAED0" }}>{colorsCount}</div>
        </div>
        {livePricedCount > 0 && (
          <div className="kpi-card">
            <div className="kpi-label">Live Prices</div>
            <div className="kpi-value" style={{ color: "#16a34a" }}>{livePricedCount}</div>
            <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 2 }}>from SP-API</div>
          </div>
        )}
        {activeCouponCount > 0 && (
          <div className="kpi-card">
            <div className="kpi-label">Active Coupons</div>
            <div className="kpi-value" style={{ color: "#dc2626" }}>{activeCouponCount}</div>
            <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 2 }}>from Ads API</div>
          </div>
        )}
      </div>

      {/* Color Filters */}
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>
        <div className="range-tabs">
          {COLOR_SECTIONS.map(s => (
            <button key={s} className={`range-tab ${filter === s ? "active" : ""}`} onClick={() => setFilter(s)}>
              {s}
            </button>
          ))}
        </div>
        <div style={{ flex: 1 }} />
        <input
          type="text" placeholder="Search SKU, ASIN, or name..."
          value={search} onChange={e => setSearch(e.target.value)}
          style={{
            padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8,
            fontSize: 13, width: 240, outline: "none",
          }}
        />
        <span style={{ fontSize: 12, color: "var(--muted)", whiteSpace: "nowrap" }}>
          {sorted.length} of {totalItems} &middot; Click values to edit
        </span>
      </div>

      {/* Main Amazon Table */}
      <div className="table-card" style={{ overflowX: "auto" }}>
        <table style={{ fontSize: 13 }}>
          <thead>
            <tr>
              <SortHeader label="Product" field="productName" style={{ minWidth: 280, textAlign: "left" }} />
              <SortHeader label="Color" field="color" style={{ textAlign: "left" }} />
              <SortHeader label="Brand" field="brand" style={{ textAlign: "left" }} />
              <SortHeader label="Series" field="series" style={{ textAlign: "left" }} />
              <SortHeader label="Type" field="productType" style={{ textAlign: "left" }} />
              <SortHeader label="Pcs" field="pieceCount" style={{ textAlign: "right" }} />
              <SortHeader label="Hand" field="orientation" style={{ textAlign: "left" }} />
              <SortHeader label="Category" field="category" style={{ textAlign: "left" }} />
              <th style={{ textAlign: "right" }}>Casepack</th>
              <th style={{ textAlign: "right" }}>Carton (L×W×H)</th>
              <SortHeader label="COGS" field="unitCost" style={{ textAlign: "right" }} />
              <SortHeader label="List $" field="listPrice" style={{ textAlign: "right" }} />
              <th style={{ textAlign: "right" }}>Sale $</th>
              <th style={{ textAlign: "right" }}>Coupon</th>
              <SortHeader label="Net $" field="netPrice" style={{ textAlign: "right" }} />
              <SortHeader label="Ref Fee" field="referralFee" style={{ textAlign: "right" }} />
              <SortHeader label="FY26 Plan" field="plannedAnnualUnits" style={{ textAlign: "right" }} />
              <SortHeader label="LY Rev" field="lyRevenue" style={{ textAlign: "right" }} />
              <SortHeader label="LY Units" field="lyUnits" style={{ textAlign: "right" }} />
              <th style={{ width: 30 }}></th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(item => (
              <>
                <tr key={item.asin} style={{ background: saving === item.asin ? "rgba(46,207,170,0.08)" : undefined }}>
                  <td style={{ maxWidth: 280 }}>
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                        title={item.productName}>{item.productName}</div>
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.sku}{item.asin ? ` · ${item.asin}` : ""}
                    </div>
                  </td>
                  <td><Badge color={item.color} /></td>
                  <td style={{ fontSize: 12 }}>{item.brand || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.series || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.productType || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.pieceCount || "—"}</td>
                  <td>{item.orientation || "—"}</td>
                  <td style={{ fontSize: 12, color: "var(--muted)" }}>{item.category || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.casepack || item.cartonPack || "—"}</td>
                  <td style={{ textAlign: "right", fontSize: 11, color: "var(--muted)" }}>
                    {item.cartonLength && item.cartonWidth && item.cartonHeight
                      ? `${item.cartonLength}×${item.cartonWidth}×${item.cartonHeight}`
                      : "—"}
                  </td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.unitCost} prefix="$"
                      onSave={v => handleUpdate(item.asin, "unitCost", v)} />
                  </td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.listPrice} prefix="$"
                      onSave={v => handleUpdate(item.asin, "listPrice", v)} />
                  </td>
                  {/* Sale $ with end date from live API */}
                  <td style={{ textAlign: "right" }}>
                    <div>
                      <EditableCell value={item.salePrice} prefix="$"
                        onSave={v => handleUpdate(item.asin, "salePrice", v)} />
                    </div>
                    {item.liveSaleEndDate && (
                      <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 1 }}>
                        ends {fmtDate(item.liveSaleEndDate)}
                      </div>
                    )}
                  </td>
                  {/* Coupon with end date from Ads API */}
                  <td style={{ textAlign: "right" }}>
                    {item.liveCouponState ? (
                      <div>
                        <span style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 4 }}>
                          {item.couponValue > 0 && (
                            <span style={{ color: "#dc2626", fontSize: 12 }}>
                              {item.couponType === "%" ? `${item.couponValue}%` : `$${item.couponValue}`}
                            </span>
                          )}
                          <CouponBadge state={item.liveCouponState} />
                        </span>
                        {item.couponEndDate && (
                          <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 1, textAlign: "right" }}>
                            ends {fmtDate(item.couponEndDate)}
                          </div>
                        )}
                      </div>
                    ) : item.couponValue > 0 ? (
                      <span style={{ color: "#dc2626" }}>
                        {item.couponType === "%" ? `${item.couponValue}%` : `$${item.couponValue}`} off
                      </span>
                    ) : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontWeight: 600 }}>
                    {item.netPrice > 0 ? `$${item.netPrice.toFixed(2)}` : "—"}
                  </td>
                  <td style={{ textAlign: "right", color: "var(--muted)" }}>
                    {item.referralFee > 0 ? `$${item.referralFee.toFixed(2)}` : "—"}
                  </td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.plannedAnnualUnits}
                      onSave={v => handleUpdate(item.asin, "plannedAnnualUnits", Math.round(v))} />
                  </td>
                  <td style={{ textAlign: "right" }}>{fmt$(item.lyRevenue)}</td>
                  <td style={{ textAlign: "right" }}>{(item.lyUnits || 0).toLocaleString()}</td>
                  <td>
                    <button
                      onClick={() => setExpandedAsin(expandedAsin === item.asin ? null : item.asin)}
                      style={{ background: "none", border: "none", cursor: "pointer", fontSize: 16, color: "var(--muted)" }}
                    >
                      {expandedAsin === item.asin ? "▲" : "▼"}
                    </button>
                  </td>
                </tr>
                {expandedAsin === item.asin && (
                  <tr key={item.asin + "-detail"}>
                    <td colSpan={15} style={{ background: "rgba(14,31,45,0.02)", padding: 16 }}>
                      {/* Live Pricing & Coupon Section */}
                      {(item.liveBuyBoxPrice != null || item.liveCouponState) && (
                        <div style={{
                          display: "flex", gap: 16, marginBottom: 16, padding: 12,
                          background: "rgba(46,207,170,0.06)", borderRadius: 8, border: "1px solid rgba(46,207,170,0.15)",
                          flexWrap: "wrap",
                        }}>
                          <div style={{ fontSize: 12, fontWeight: 600, color: "var(--navy)", width: "100%", marginBottom: 4 }}>
                            Live Amazon Data
                            {item.priceFetchedAt && (
                              <span style={{ fontWeight: 400, fontSize: 10, color: "var(--muted)", marginLeft: 8 }}>
                                Updated {formatSyncTime(item.priceFetchedAt)}
                              </span>
                            )}
                          </div>
                          {item.liveBuyBoxPrice != null && (
                            <div style={{ minWidth: 100 }}>
                              <div style={{ fontSize: 10, color: "var(--muted)", marginBottom: 2 }}>Buy Box Price</div>
                              <div style={{ fontSize: 16, fontWeight: 700, color: "#16a34a", fontFamily: "'Space Grotesk', monospace" }}>
                                ${item.liveBuyBoxPrice.toFixed(2)}
                              </div>
                            </div>
                          )}
                          {item.liveListingPrice != null && (
                            <div style={{ minWidth: 100 }}>
                              <div style={{ fontSize: 10, color: "var(--muted)", marginBottom: 2 }}>Listing Price</div>
                              <div style={{ fontSize: 16, fontWeight: 700, color: "var(--navy)", fontFamily: "'Space Grotesk', monospace" }}>
                                ${item.liveListingPrice.toFixed(2)}
                              </div>
                            </div>
                          )}
                          {item.liveLandedPrice != null && (
                            <div style={{ minWidth: 100 }}>
                              <div style={{ fontSize: 10, color: "var(--muted)", marginBottom: 2 }}>Landed Price</div>
                              <div style={{ fontSize: 14, fontWeight: 600, color: "var(--muted)", fontFamily: "'Space Grotesk', monospace" }}>
                                ${item.liveLandedPrice.toFixed(2)}
                              </div>
                            </div>
                          )}
                          {item.liveCouponState && (
                            <>
                              <div style={{ borderLeft: "1px solid rgba(14,31,45,0.1)", paddingLeft: 16, minWidth: 100 }}>
                                <div style={{ fontSize: 10, color: "var(--muted)", marginBottom: 2 }}>Coupon Status</div>
                                <CouponBadge state={item.liveCouponState} />
                                {item.liveCouponType && item.liveCouponValue != null && (
                                  <div style={{ fontSize: 12, marginTop: 4, color: "#dc2626", fontWeight: 600 }}>
                                    {item.liveCouponType === "PERCENTAGE" ? `${item.liveCouponValue}% off` : `$${item.liveCouponValue} off`}
                                  </div>
                                )}
                              </div>
                              {(item.couponBudget != null || item.couponRedemptions != null) && (
                                <div style={{ minWidth: 100 }}>
                                  <div style={{ fontSize: 10, color: "var(--muted)", marginBottom: 2 }}>Budget / Redemptions</div>
                                  <div style={{ fontSize: 13, fontFamily: "'Space Grotesk', monospace" }}>
                                    {item.couponBudget != null ? `$${item.couponBudget.toLocaleString()}` : "—"}
                                    {item.couponBudgetUsed != null && item.couponBudget > 0 && (
                                      <span style={{ color: "var(--muted)", fontSize: 11 }}> ({Math.round((item.couponBudgetUsed / item.couponBudget) * 100)}% used)</span>
                                    )}
                                  </div>
                                  <div style={{ fontSize: 12, color: "var(--muted)", marginTop: 2 }}>
                                    {item.couponRedemptions != null ? `${item.couponRedemptions.toLocaleString()} redeemed` : ""}
                                  </div>
                                </div>
                              )}
                              {(item.couponStartDate || item.couponEndDate) && (
                                <div style={{ minWidth: 120 }}>
                                  <div style={{ fontSize: 10, color: "var(--muted)", marginBottom: 2 }}>Coupon Dates</div>
                                  <div style={{ fontSize: 12 }}>
                                    {fmtDate(item.couponStartDate) || "—"}
                                    {" → "}
                                    {fmtDate(item.couponEndDate) || "—"}
                                  </div>
                                </div>
                              )}
                            </>
                          )}
                        </div>
                      )}

                      {/* Detail Grid */}
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 16 }}>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Brand / Series</div>
                          <div>{item.brand} &middot; {item.series}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Category</div>
                          <div>{item.category || "—"}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Referral %</div>
                          <EditableCell value={item.referralPct} suffix="%"
                            onSave={v => handleUpdate(item.asin, "referralPct", v)} />
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Coupon Type</div>
                          <select
                            value={item.couponType}
                            onChange={e => handleUpdate(item.asin, "couponType", e.target.value)}
                            style={{ padding: "4px 8px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 4, fontSize: 13 }}
                          >
                            <option value="">None</option>
                            <option value="$">$ Off</option>
                            <option value="%">% Off</option>
                          </select>
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Coupon Value</div>
                          <EditableCell value={item.couponValue}
                            onSave={v => handleUpdate(item.asin, "couponValue", v)} />
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Carton Pack Size</div>
                          <EditableCell value={item.cartonPack}
                            onSave={v => handleUpdate(item.asin, "cartonPack", Math.round(v))} />
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Carton L×W×H (in)</div>
                          <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                            <EditableCell value={item.cartonLength}
                              onSave={v => handleUpdate(item.asin, "cartonLength", v)} />
                            <span>×</span>
                            <EditableCell value={item.cartonWidth}
                              onSave={v => handleUpdate(item.asin, "cartonWidth", v)} />
                            <span>×</span>
                            <EditableCell value={item.cartonHeight}
                              onSave={v => handleUpdate(item.asin, "cartonHeight", v)} />
                          </div>
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>Carton Weight (lbs)</div>
                          <EditableCell value={item.cartonWeight}
                            onSave={v => handleUpdate(item.asin, "cartonWeight", v)} />
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>LY Avg Unit Retail</div>
                          <div>${(item.lyAur || 0).toFixed(2)}</div>
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>LY Profit</div>
                          <div style={{ color: (item.lyProfit || 0) >= 0 ? "#16a34a" : "#dc2626" }}>
                            {fmt$(item.lyProfit)}
                          </div>
                        </div>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>FBA Stock</div>
                          <div>{(item.fbsStock || 0).toLocaleString()}</div>
                        </div>
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
        <strong>Tip:</strong> Click any blue-underlined value to edit it inline. Changes save automatically.
        Expand a row (▼) to see carton dimensions, coupon settings, and more details.
      </div>
    </>
  );
}
