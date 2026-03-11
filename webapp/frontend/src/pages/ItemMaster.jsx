import { useState, useEffect, useCallback } from "react";
import { api, fmt$ } from "../lib/api";

const COLOR_BADGES = {
  Green: { bg: "#dcfce7", color: "#166534" },
  Blue: { bg: "#dbeafe", color: "#1e40af" },
  Red: { bg: "#fee2e2", color: "#991b1b" },
  Orange: { bg: "#ffedd5", color: "#9a3412" },
  Black: { bg: "#e5e7eb", color: "#1f2937" },
  "": { bg: "#f3f4f6", color: "#6b7280" },
};

const SECTIONS = ["All", "Green", "Blue", "Red", "Orange", "Accessories"];

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

const MASTER_TABS = ["Amazon", "Walmart", "Other"];

export default function ItemMaster() {
  const [masterTab, setMasterTab] = useState("Amazon");
  const [items, setItems] = useState([]);
  const [walmartItems, setWalmartItems] = useState(null);
  const [otherItems, setOtherItems] = useState(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(null);
  const [filter, setFilter] = useState("All");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState("lyRevenue");
  const [sortDir, setSortDir] = useState("desc");
  const [expandedAsin, setExpandedAsin] = useState(null);

  const load = useCallback(() => {
    setLoading(true);
    api.itemMaster().then(d => {
      setItems(d.items || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    if (masterTab === "Walmart" && walmartItems === null) {
      setLoading(true);
      api.itemMasterWalmart().then(d => { setWalmartItems(d.items || []); setLoading(false); }).catch(() => setLoading(false));
    }
    if (masterTab === "Other" && otherItems === null) {
      setLoading(true);
      api.itemMasterOther().then(d => { setOtherItems(d.items || []); setLoading(false); }).catch(() => setLoading(false));
    }
  }, [masterTab, walmartItems, otherItems]);

  const handleUpdate = async (asin, field, value) => {
    setSaving(asin);
    try {
      await api.updateItem(asin, { [field]: value });
      setItems(prev => prev.map(i => i.asin === asin ? { ...i, [field]: value } : i));
    } catch (e) {
      console.error("Save failed:", e);
    }
    setSaving(null);
  };

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === "desc" ? "asc" : "desc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const filtered = items.filter(i => {
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

  const SortHeader = ({ label, field, style: s }) => (
    <th onClick={() => handleSort(field)} style={{ cursor: "pointer", userSelect: "none", ...s }}>
      {label} {sortKey === field ? (sortDir === "desc" ? "▼" : "▲") : ""}
    </th>
  );

  if (loading) return <div className="loading"><div className="spinner" /> Loading item master...</div>;

  /* ─────────────── Walmart Tab ─────────────── */
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

  if (masterTab === "Walmart") {
    const simpleItems = walmartItems || [];
    const simpleFiltered = simpleItems.filter(i => {
      if (!search) return true;
      const q = search.toLowerCase();
      return (i.golfgenItem || i.itemNumber || "").toLowerCase().includes(q) ||
             (i.walmartItem || "").toLowerCase().includes(q) ||
             (i.description || "").toLowerCase().includes(q);
    });

    const totalSkus = simpleItems.length;
    const totalStores = simpleItems.reduce((s, i) => s + (i.storeCount || 0), 0);
    const avgCost = totalSkus > 0 ? simpleItems.reduce((s, i) => s + (i.unitCost || 0), 0) / totalSkus : 0;
    const avgRetail = totalSkus > 0 ? simpleItems.reduce((s, i) => s + (i.unitRetail || 0), 0) / totalSkus : 0;
    const totalPlannedWm = simpleItems.reduce((s, i) => s + (i.plannedAnnualUnits || 0), 0);
    const categories = [...new Set(simpleItems.map(i => i.subcategory || i.category).filter(Boolean))];

    return (
      <>
        <div className="page-header">
          <h1>Item Master</h1>
          <p>Walmart items &middot; {totalSkus} SKUs</p>
        </div>

        <div className="range-tabs" style={{ marginBottom: 20 }}>
          {MASTER_TABS.map(t => (
            <button key={t} className={`range-tab ${masterTab === t ? "active" : ""}`}
              onClick={() => { setMasterTab(t); setSearch(""); }}>{t}</button>
          ))}
        </div>

        {/* Walmart Summary */}
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
            {simpleFiltered.length} of {totalSkus} &middot; Click values to edit
          </span>
        </div>

        <div className="table-card">
          <table>
            <thead>
              <tr>
                <th style={{ minWidth: 260, textAlign: "left" }}>Product</th>
                <th style={{ textAlign: "left" }}>Category</th>
                <th style={{ textAlign: "right" }}>Unit Cost</th>
                <th style={{ textAlign: "right" }}>Unit Retail</th>
                <th style={{ textAlign: "right" }}>Carton Size</th>
                <th style={{ textAlign: "right" }}>Store Count</th>
                <th style={{ textAlign: "right" }}>FY26 Plan</th>
              </tr>
            </thead>
            <tbody>
              {simpleFiltered.map((item, i) => (
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
                  <td style={{ fontSize: 12, color: "var(--muted)" }}>{item.subcategory || item.category || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.unitCost ? `$${item.unitCost.toFixed(2)}` : "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.unitRetail ? `$${item.unitRetail.toFixed(2)}` : "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.cartonSize || "—"}</td>
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

  /* ─────────────── Other Tab ─────────────── */
  if (masterTab === "Other") {
    const simpleItems = otherItems || [];
    const simpleFiltered = simpleItems.filter(i => {
      if (!search) return true;
      const q = search.toLowerCase();
      return (i.itemNumber || "").toLowerCase().includes(q) || (i.description || "").toLowerCase().includes(q);
    });

    const totalSkus = simpleItems.length;
    const totalOnHand = simpleItems.reduce((s, i) => s + (i.pcsOnHand || 0), 0);
    const totalAvailable = simpleItems.reduce((s, i) => s + (i.pcsAvailable || 0), 0);
    const totalAllocated = simpleItems.reduce((s, i) => s + (i.pcsAllocated || 0), 0);
    const golfCount = simpleItems.filter(i => i.source === "Golf").length;
    const hwCount = simpleItems.filter(i => i.source === "Housewares").length;

    return (
      <>
        <div className="page-header">
          <h1>Item Master</h1>
          <p>Other items (not in Amazon or Walmart master) &middot; {totalSkus} SKUs</p>
        </div>

        <div className="range-tabs" style={{ marginBottom: 20 }}>
          {MASTER_TABS.map(t => (
            <button key={t} className={`range-tab ${masterTab === t ? "active" : ""}`}
              onClick={() => { setMasterTab(t); setSearch(""); }}>{t}</button>
          ))}
        </div>

        {/* Other Summary */}
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
          <div className="kpi-card">
            <div className="kpi-label">From Golf</div>
            <div className="kpi-value">{golfCount}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">From HW</div>
            <div className="kpi-value">{hwCount}</div>
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
          <input type="text" placeholder="Search..." value={search} onChange={e => setSearch(e.target.value)}
            style={{ padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8, fontSize: 13, width: 260 }} />
          <span style={{ fontSize: 12, color: "var(--muted)", marginLeft: "auto" }}>
            {simpleFiltered.length} of {totalSkus}
          </span>
        </div>

        <div className="table-card">
          <table>
            <thead>
              <tr>
                <th style={{ minWidth: 260, textAlign: "left" }}>Product</th>
                <th style={{ textAlign: "left" }}>Source</th>
                <th style={{ textAlign: "right" }}>Pcs On Hand</th>
                <th style={{ textAlign: "right" }}>Pcs Allocated</th>
                <th style={{ textAlign: "right" }}>Pcs Available</th>
              </tr>
            </thead>
            <tbody>
              {simpleFiltered.map((item, i) => (
                <tr key={i}>
                  <td>
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 300 }} title={item.description}>
                      {item.description || "—"}
                    </div>
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.itemNumber || item.sku || "—"}
                    </div>
                  </td>
                  <td>
                    <span style={{
                      display: "inline-block", padding: "2px 10px", borderRadius: 12, fontSize: 11, fontWeight: 600,
                      background: item.source === "Golf" ? "rgba(46,207,170,0.1)" : "rgba(62,101,140,0.1)",
                      color: item.source === "Golf" ? "var(--teal)" : "var(--blue-active)",
                    }}>
                      {item.source}
                    </span>
                  </td>
                  <td style={{ textAlign: "right" }}>{(item.pcsOnHand || 0).toLocaleString()}</td>
                  <td style={{ textAlign: "right", color: (item.pcsAllocated || 0) > 0 ? "var(--gold)" : "var(--muted)" }}>
                    {(item.pcsAllocated || 0) > 0 ? item.pcsAllocated.toLocaleString() : "—"}
                  </td>
                  <td style={{ textAlign: "right", fontWeight: 600, color: (item.pcsAvailable || 0) > 0 ? "#16a34a" : "var(--muted)" }}>
                    {(item.pcsAvailable || 0).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </>
    );
  }

  /* ─────────────── Amazon (main) Item Master ─────────────── */
  const totalItems = items.length;
  const totalPlanned = items.reduce((s, i) => s + (i.plannedAnnualUnits || 0), 0);
  const totalLyRev = items.reduce((s, i) => s + (i.lyRevenue || 0), 0);
  const colorsCount = [...new Set(items.map(i => i.color).filter(Boolean))].length;

  return (
    <>
      <div className="page-header">
        <h1>Item Master</h1>
        <p>{totalItems} SKUs &middot; {colorsCount} colors &middot; Source of truth for product attributes</p>
      </div>

      <div className="range-tabs" style={{ marginBottom: 20 }}>
        {MASTER_TABS.map(t => (
          <button key={t} className={`range-tab ${masterTab === t ? "active" : ""}`}
            onClick={() => { setMasterTab(t); setSearch(""); setFilter("All"); }}>{t}</button>
        ))}
      </div>

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
      </div>

      {/* Filters */}
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>
        <div className="range-tabs">
          {SECTIONS.map(s => (
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

      {/* Main Table */}
      <div className="table-card" style={{ overflowX: "auto" }}>
        <table style={{ fontSize: 13 }}>
          <thead>
            <tr>
              <SortHeader label="Product" field="productName" style={{ minWidth: 280, textAlign: "left" }} />
              <SortHeader label="Color" field="color" style={{ textAlign: "left" }} />
              <SortHeader label="Type" field="productType" style={{ textAlign: "left" }} />
              <SortHeader label="Pcs" field="pieceCount" style={{ textAlign: "right" }} />
              <SortHeader label="Hand" field="orientation" style={{ textAlign: "left" }} />
              <SortHeader label="COGS" field="unitCost" style={{ textAlign: "right" }} />
              <SortHeader label="List $" field="listPrice" style={{ textAlign: "right" }} />
              <SortHeader label="Sale $" field="salePrice" style={{ textAlign: "right" }} />
              <SortHeader label="Coupon" field="couponValue" style={{ textAlign: "right" }} />
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
                  <td>{item.productType}</td>
                  <td style={{ textAlign: "right" }}>{item.pieceCount || "—"}</td>
                  <td>{item.orientation || "—"}</td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.unitCost} prefix="$"
                      onSave={v => handleUpdate(item.asin, "unitCost", v)} />
                  </td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.listPrice} prefix="$"
                      onSave={v => handleUpdate(item.asin, "listPrice", v)} />
                  </td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.salePrice} prefix="$"
                      onSave={v => handleUpdate(item.asin, "salePrice", v)} />
                  </td>
                  <td style={{ textAlign: "right" }}>
                    {item.couponValue > 0 ? (
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
