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

export default function ItemMaster() {
  const [items, setItems] = useState([]);
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

  // Summary stats
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

      {/* Summary Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 16, marginBottom: 24 }}>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 13, color: "var(--muted)" }}>Total SKUs</div>
          <div style={{ fontSize: 28, fontWeight: 700, color: "var(--teal)" }}>{totalItems}</div>
        </div>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 13, color: "var(--muted)" }}>LY Revenue</div>
          <div style={{ fontSize: 28, fontWeight: 700, color: "var(--navy)" }}>{fmt$(totalLyRev)}</div>
        </div>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 13, color: "var(--muted)" }}>FY26 Planned Units</div>
          <div style={{ fontSize: 28, fontWeight: 700, color: "var(--orange)" }}>{totalPlanned.toLocaleString()}</div>
        </div>
        <div className="card" style={{ padding: 16, textAlign: "center" }}>
          <div style={{ fontSize: 13, color: "var(--muted)" }}>Color Groups</div>
          <div style={{ fontSize: 28, fontWeight: 700, color: "#7BAED0" }}>{colorsCount}</div>
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
        <input
          type="text" placeholder="Search SKU, ASIN, or name..."
          value={search} onChange={e => setSearch(e.target.value)}
          style={{
            padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8,
            fontSize: 13, width: 260, outline: "none",
          }}
        />
        <span style={{ fontSize: 13, color: "var(--muted)", marginLeft: "auto" }}>
          Showing {sorted.length} of {totalItems} &middot; Click values to edit
        </span>
      </div>

      {/* Main Table */}
      <div className="card" style={{ overflowX: "auto" }}>
        <table className="data-table" style={{ fontSize: 13 }}>
          <thead>
            <tr>
              <SortHeader label="SKU" field="sku" />
              <SortHeader label="Product Name" field="productName" style={{ minWidth: 220 }} />
              <SortHeader label="Color" field="color" />
              <SortHeader label="Type" field="productType" />
              <SortHeader label="Pcs" field="pieceCount" />
              <SortHeader label="Hand" field="orientation" />
              <SortHeader label="COGS" field="unitCost" />
              <SortHeader label="List $" field="listPrice" />
              <SortHeader label="Sale $" field="salePrice" />
              <SortHeader label="Coupon" field="couponValue" />
              <SortHeader label="Net $" field="netPrice" />
              <SortHeader label="Ref Fee" field="referralFee" />
              <SortHeader label="LY Rev" field="lyRevenue" />
              <SortHeader label="LY Units" field="lyUnits" />
              <SortHeader label="FY26 Plan" field="plannedAnnualUnits" />
              <th style={{ width: 30 }}></th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(item => (
              <>
                <tr key={item.asin} style={{ background: saving === item.asin ? "rgba(46,207,170,0.08)" : undefined }}>
                  <td style={{ fontFamily: "monospace", fontSize: 11 }}>{item.sku}</td>
                  <td style={{ maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                      title={item.productName}>{item.productName}</td>
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
                  <td style={{ textAlign: "right" }}>{fmt$(item.lyRevenue)}</td>
                  <td style={{ textAlign: "right" }}>{(item.lyUnits || 0).toLocaleString()}</td>
                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.plannedAnnualUnits}
                      onSave={v => handleUpdate(item.asin, "plannedAnnualUnits", Math.round(v))} />
                  </td>
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
                    <td colSpan={16} style={{ background: "rgba(14,31,45,0.02)", padding: 16 }}>
                      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 16 }}>
                        <div>
                          <div style={{ fontSize: 11, color: "var(--muted)", marginBottom: 4 }}>ASIN</div>
                          <div style={{ fontFamily: "monospace" }}>{item.asin}</div>
                        </div>
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
