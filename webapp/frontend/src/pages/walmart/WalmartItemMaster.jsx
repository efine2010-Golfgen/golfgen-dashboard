import { useState, useEffect, useRef, useCallback } from "react";
import { api } from "../../lib/api";
import { SG, DM, Card, fN, f$ } from "./WalmartHelpers";

/* ── Inline Editable Cell ──────────────────────────────────── */
function EditCell({ value, onSave, type = "text", style = {} }) {
  const [editing, setEditing] = useState(false);
  const [val, setVal] = useState(value ?? "");
  const ref = useRef(null);
  useEffect(() => { if (editing && ref.current) ref.current.focus(); }, [editing]);
  useEffect(() => { setVal(value ?? ""); }, [value]);
  const commit = () => {
    setEditing(false);
    const v = type === "number" ? (val === "" ? 0 : Number(val)) : val;
    if (v !== value) onSave(v);
  };
  if (!editing) return (
    <span onClick={() => setEditing(true)} style={{ cursor: "pointer", minWidth: 24, display: "inline-block", ...style }}>{value ?? "—"}</span>
  );
  return (
    <input ref={ref} type={type} value={val} onChange={e => setVal(e.target.value)}
      onBlur={commit} onKeyDown={e => { if (e.key === "Enter") commit(); if (e.key === "Escape") { setVal(value ?? ""); setEditing(false); } }}
      style={{ ...SG(9), width: type === "number" ? 60 : 120, padding: "2px 4px", border: "1px solid var(--teal)", borderRadius: 3, background: "var(--bg)", color: "var(--txt)", ...style }}
    />
  );
}

/* ── Add Item Modal ────────────────────────────────────────── */
function AddNifModal({ open, onClose, onAdd, year }) {
  const [saving, setSaving] = useState(false);
  if (!open) return null;
  const handleSubmit = async (e) => {
    e.preventDefault();
    setSaving(true);
    const fd = new FormData(e.target);
    const data = Object.fromEntries(fd.entries());
    data.event_year = year;
    try { await onAdd(data); e.target.reset(); onClose(); } catch (err) { alert(err.message); } finally { setSaving(false); }
  };
  const fieldStyle = { ...SG(10), padding: "6px 8px", borderRadius: 4, border: "1px solid var(--brd)", background: "var(--bg)", color: "var(--txt)", width: "100%" };
  const labelStyle = { ...SG(9, 600), color: "var(--txt3)", marginBottom: 2 };
  return (
    <div style={{ position: "fixed", inset: 0, zIndex: 9999, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(0,0,0,0.6)" }} onClick={onClose}>
      <form onSubmit={handleSubmit} onClick={e => e.stopPropagation()} style={{ background: "var(--card)", borderRadius: 12, padding: 24, width: 480, maxHeight: "80vh", overflow: "auto", border: "1px solid var(--brd)" }}>
        <div style={{ ...DM(18), marginBottom: 16, color: "var(--txt)" }}>Add Walmart Item</div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
          <div><div style={labelStyle}>Description</div><input name="description" style={{ ...fieldStyle, gridColumn: "1/3" }} /></div>
          <div><div style={labelStyle}>Brand</div><input name="brand" style={fieldStyle} /></div>
          <div><div style={labelStyle}>WM Item #</div><input name="wmt_item_number" style={fieldStyle} /></div>
          <div><div style={labelStyle}>UPC</div><input name="upc" style={fieldStyle} /></div>
          <div><div style={labelStyle}>GG Item # (VSN)</div><input name="vendor_stock_number" style={fieldStyle} /></div>
          <div><div style={labelStyle}>Brand ID</div><input name="brand_id" style={fieldStyle} /></div>
          <div><div style={labelStyle}>Wholesale $</div><input name="wholesale_cost" type="number" step="0.01" style={fieldStyle} /></div>
          <div><div style={labelStyle}>Retail $</div><input name="walmart_retail" type="number" step="0.01" style={fieldStyle} /></div>
          <div><div style={labelStyle}>Casepack</div><input name="vendor_pack" type="number" style={fieldStyle} /></div>
          <div><div style={labelStyle}>Status</div>
            <select name="item_status" style={fieldStyle} defaultValue="new">
              <option value="new">New</option><option value="go_forward">Go Forward</option><option value="dotcom">Dotcom</option><option value="deleted">Deleted</option>
            </select>
          </div>
          <div><div style={labelStyle}>Color</div><input name="color" style={fieldStyle} /></div>
          <div><div style={labelStyle}>Dexterity</div><input name="dexterity" style={fieldStyle} /></div>
          <div><div style={labelStyle}>Category</div><input name="category" style={fieldStyle} /></div>
        </div>
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, marginTop: 16 }}>
          <button type="button" onClick={onClose} style={{ ...SG(10, 600), padding: "6px 14px", borderRadius: 6, border: "1px solid var(--brd)", background: "none", color: "var(--txt3)", cursor: "pointer" }}>Cancel</button>
          <button type="submit" disabled={saving} style={{ ...SG(10, 700), padding: "6px 14px", borderRadius: 6, border: "none", background: "var(--teal)", color: "#fff", cursor: "pointer", opacity: saving ? 0.6 : 1 }}>{saving ? "Adding..." : "Add Item"}</button>
        </div>
      </form>
    </div>
  );
}

/* ── Delete Confirmation ───────────────────────────────────── */
function DeleteNifConfirm({ item, onConfirm, onCancel }) {
  if (!item) return null;
  return (
    <div style={{ position: "fixed", inset: 0, zIndex: 9999, display: "flex", alignItems: "center", justifyContent: "center", background: "rgba(0,0,0,0.6)" }} onClick={onCancel}>
      <div onClick={e => e.stopPropagation()} style={{ background: "var(--card)", borderRadius: 12, padding: 24, width: 400, border: "1px solid var(--brd)" }}>
        <div style={{ ...DM(16), color: "#f87171", marginBottom: 12 }}>Delete Item?</div>
        <p style={{ ...SG(11), color: "var(--txt)", margin: "0 0 4px" }}>{item.description}</p>
        <p style={{ ...SG(9), color: "var(--txt3)", margin: "0 0 16px" }}>WM# {item.wmtItemNumber || "—"} · {item.vendorStockNumber || "—"}</p>
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
          <button onClick={onCancel} style={{ ...SG(10, 600), padding: "6px 14px", borderRadius: 6, border: "1px solid var(--brd)", background: "none", color: "var(--txt3)", cursor: "pointer" }}>Cancel</button>
          <button onClick={() => onConfirm(item.id)} style={{ ...SG(10, 700), padding: "6px 14px", borderRadius: 6, border: "none", background: "#f87171", color: "#fff", cursor: "pointer" }}>Delete</button>
        </div>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════
// WALMART ITEM MASTER — NIF-based item catalog with year tabs
// ═══════════════════════════════════════════════════════════════════════

const STATUS_LABELS = {
  new: "New Items",
  go_forward: "Go Forward Items",
  deleted: "Deleted Items",
  dotcom: "Dotcom / eCommerce Items",
};

const STATUS_COLORS = {
  new: "#2ecfaa",
  go_forward: "#60a5fa",
  deleted: "#f87171",
  dotcom: "#a78bfa",
};

const STATUS_ORDER = ["new", "go_forward", "dotcom", "deleted"];

const smallTab = (active) => ({
  ...SG(8, active ? 700 : 500),
  padding: "2px 8px",
  borderRadius: 3,
  cursor: "pointer",
  background: active ? "rgba(46,207,170,0.15)" : "rgba(255,255,255,0.04)",
  color: active ? "#2ecf99" : "var(--txt3)",
  border: `1px solid ${active ? "rgba(46,207,170,0.3)" : "rgba(255,255,255,0.08)"}`,
  userSelect: "none",
});

export function WalmartItemMaster({ filters = {} }) {
  const [items, setItems] = useState([]);
  const [availableYears, setAvailableYears] = useState([]);
  const [year, setYear] = useState(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState(null);
  const [collapsedSections, setCollapsedSections] = useState({});
  const [showAdd, setShowAdd] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState(null);
  const [toast, setToast] = useState(null);
  const fileRef = useRef(null);

  const showToast = (msg, ok = true) => { setToast({ msg, ok }); setTimeout(() => setToast(null), 3000); };

  const fetchItems = (y) => {
    setLoading(true);
    api.walmartNifItems(y).then((data) => {
      setItems(data.items || []);
      setAvailableYears(data.availableYears || []);
      if (!y && data.availableYears?.length) {
        setYear(data.availableYears[data.availableYears.length - 1]);
      }
    }).catch(() => {
      setItems([]);
      setAvailableYears([]);
    }).finally(() => setLoading(false));
  };

  useEffect(() => { fetchItems(year); }, [year]);

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadResult(null);
    try {
      const result = await api.walmartNifUpload(file);
      setUploadResult(result);
      // Refresh data
      fetchItems(result.event_year);
      setYear(result.event_year);
    } catch (err) {
      setUploadResult({ status: "error", message: err.message });
    } finally {
      setUploading(false);
      if (fileRef.current) fileRef.current.value = "";
    }
  };

  const toggleSection = (status) => {
    setCollapsedSections((prev) => ({ ...prev, [status]: !prev[status] }));
  };

  // snake_case (API field) → camelCase (React state key) mapping
  const snakeToCamel = (s) => s.replace(/_([a-z])/g, (_, c) => c.toUpperCase());

  const handleCellSave = async (itemId, field, value) => {
    try {
      await api.walmartNifUpdate(itemId, { [field]: value });
      const camelKey = snakeToCamel(field);
      setItems(prev => prev.map(i => i.id === itemId ? { ...i, [camelKey]: value } : i));
      showToast("Saved", true);
    } catch (err) { showToast(err.message, false); }
  };

  const handleAdd = async (formData) => {
    await api.walmartNifAdd(formData);
    showToast("Item added");
    fetchItems(year);
  };

  const handleDelete = async (id) => {
    try {
      await api.walmartNifDelete(id);
      setItems(prev => prev.filter(i => i.id !== id));
      setDeleteTarget(null);
      showToast("Item deleted");
    } catch (err) { showToast(err.message, false); }
  };

  // Filter items by search
  const filtered = items.filter((item) => {
    if (!search) return true;
    const s = search.toLowerCase();
    return (
      (item.description || "").toLowerCase().includes(s) ||
      (item.brand || "").toLowerCase().includes(s) ||
      (item.wmtItemNumber || "").toLowerCase().includes(s) ||
      (item.vendorStockNumber || "").toLowerCase().includes(s) ||
      (item.upc || "").toLowerCase().includes(s)
    );
  });

  // Filter by selected year
  const yearItems = year ? filtered.filter((i) => i.eventYear === year) : filtered;

  // Group by status
  const grouped = {};
  for (const status of STATUS_ORDER) {
    const g = yearItems.filter((i) => i.itemStatus === status);
    if (g.length > 0) grouped[status] = g;
  }

  // Effective week info
  const effWeek = yearItems.length > 0 ? yearItems[0].effectiveWeek : null;

  // Year tabs — always show 2024, 2025, 2026 (even if no data yet)
  const yearTabs = [2024, 2025, 2026];

  const toastEl = toast && (
    <div style={{ position: "fixed", top: 20, right: 20, zIndex: 10000, padding: "10px 20px", borderRadius: 8, background: toast.ok ? "#2ecfaa" : "#f87171", color: "#fff", ...SG(11, 600), boxShadow: "0 4px 12px rgba(0,0,0,0.3)" }}>{toast.msg}</div>
  );

  return (
    <div>
      {toastEl}
      <AddNifModal open={showAdd} onClose={() => setShowAdd(false)} onAdd={handleAdd} year={year} />
      <DeleteNifConfirm item={deleteTarget} onConfirm={handleDelete} onCancel={() => setDeleteTarget(null)} />

      {/* Header row: year tabs + upload button + add button + search */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
        {/* Year tabs */}
        <div style={{ display: "flex", gap: 2 }}>
          {yearTabs.map((y) => {
            const hasData = availableYears.includes(y);
            const active = year === y;
            return (
              <button
                key={y}
                onClick={() => setYear(y)}
                style={{
                  ...SG(11, active ? 700 : 500),
                  background: active ? "rgba(46,207,170,0.15)" : "none",
                  border: active ? "1px solid rgba(46,207,170,0.3)" : "1px solid var(--brd)",
                  cursor: "pointer",
                  padding: "6px 16px",
                  borderRadius: 6,
                  color: active ? "#2ecf99" : hasData ? "var(--txt2)" : "var(--txt3)",
                  opacity: hasData ? 1 : 0.5,
                }}
              >
                {y}
                {!hasData && " (no data)"}
              </button>
            );
          })}
        </div>

        {/* Upload button */}
        <input
          ref={fileRef}
          type="file"
          accept=".xlsx,.xls"
          onChange={handleUpload}
          style={{ display: "none" }}
        />
        <button
          onClick={() => fileRef.current?.click()}
          disabled={uploading}
          style={{
            ...SG(10, 600),
            padding: "6px 14px",
            borderRadius: 6,
            cursor: uploading ? "wait" : "pointer",
            background: "rgba(46,207,170,0.12)",
            border: "1px solid rgba(46,207,170,0.3)",
            color: "#2ecf99",
          }}
        >
          {uploading ? "Uploading..." : "Upload NIF File"}
        </button>

        <button
          onClick={() => setShowAdd(true)}
          style={{
            ...SG(10, 600),
            padding: "6px 14px",
            borderRadius: 6,
            cursor: "pointer",
            background: "var(--teal)",
            border: "none",
            color: "#fff",
          }}
        >
          + Add Item
        </button>

        {/* Search */}
        <input
          type="text"
          placeholder="Search items..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            ...SG(10),
            padding: "6px 12px",
            borderRadius: 6,
            border: "1px solid var(--brd)",
            background: "var(--card)",
            color: "var(--txt)",
            width: 200,
            marginLeft: "auto",
          }}
        />
      </div>

      {/* Upload result */}
      {uploadResult && (
        <Card style={{ marginBottom: 12, background: uploadResult.status === "success" ? "rgba(46,207,170,0.08)" : "rgba(248,113,113,0.08)" }}>
          <div style={{ ...SG(11, 600), color: uploadResult.status === "success" ? "#2ecfaa" : "#f87171" }}>
            {uploadResult.status === "success"
              ? `Loaded ${uploadResult.items_loaded} items for ${uploadResult.event_year} (WK${uploadResult.effective_week})`
              : `Error: ${uploadResult.message || "Upload failed"}`}
          </div>
          {uploadResult.breakdown && (
            <div style={{ ...SG(9), color: "var(--txt3)", marginTop: 4 }}>
              New: {uploadResult.breakdown.new} | Go Forward: {uploadResult.breakdown.go_forward} | Deleted: {uploadResult.breakdown.deleted} | Dotcom: {uploadResult.breakdown.dotcom}
            </div>
          )}
        </Card>
      )}

      {/* Effective week badge */}
      {effWeek && (
        <div style={{ ...SG(9, 600), color: "var(--txt3)", marginBottom: 12, display: "flex", justifyContent: "space-between" }}>
          <span>Effective: Walmart Week {effWeek} &bull; Event Year {year}</span>
          <span style={{ color: "var(--teal)" }}>{yearItems.length} items &middot; Click values to edit</span>
        </div>
      )}

      {loading ? (
        <div style={{ ...SG(12), color: "var(--txt3)", padding: 24 }}>Loading...</div>
      ) : Object.keys(grouped).length === 0 ? (
        <Card style={{ background: "rgba(46,207,170,0.05)", border: "1px solid rgba(46,207,170,0.2)" }}>
          <div style={{ ...DM(16), color: "var(--txt)", marginBottom: 8 }}>No Items Found</div>
          <p style={{ ...SG(11), color: "var(--txt3)", margin: 0, lineHeight: "1.4" }}>
            Upload a NIF (New Item Forecast) Excel file to populate this tab.
            The file should have a FCST sheet with item data.
          </p>
        </Card>
      ) : (
        /* Item sections */
        STATUS_ORDER.filter((s) => grouped[s]).map((status) => {
          const sectionItems = grouped[status];
          const collapsed = collapsedSections[status];
          return (
            <Card key={status} style={{ marginBottom: 12 }}>
              {/* Section header */}
              <div
                onClick={() => toggleSection(status)}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 8,
                  cursor: "pointer",
                  marginBottom: collapsed ? 0 : 12,
                  userSelect: "none",
                }}
              >
                <span style={{ ...SG(9), color: "var(--txt3)" }}>
                  {collapsed ? "▶" : "▼"}
                </span>
                <span
                  style={{
                    display: "inline-block",
                    width: 8,
                    height: 8,
                    borderRadius: "50%",
                    background: STATUS_COLORS[status],
                  }}
                />
                <span style={{ ...SG(12, 700), color: "var(--txt)" }}>
                  {STATUS_LABELS[status]}
                </span>
                <span style={{ ...SG(10, 500), color: "var(--txt3)" }}>
                  ({sectionItems.length} items)
                </span>
              </div>

              {/* Item table */}
              {!collapsed && (
                <div style={{ overflowX: "auto" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", ...SG(9) }}>
                    <thead>
                      <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                        <th style={thStyle}>Description</th>
                        <th style={thStyle}>Brand</th>
                        <th style={thStyle}>WM Item #</th>
                        <th style={thStyle}>UPC</th>
                        <th style={thStyle}>GG Item #</th>
                        <th style={thStyle}>Brand ID</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Wholesale</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Retail</th>
                        <th style={{ ...thStyle, textAlign: "center" }}>Casepack</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Old Stores</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>New Stores</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Diff</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>L</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>W</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>H</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>CBM</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>CBF</th>
                        <th style={thStyle}>Color</th>
                        <th style={thStyle}>Dexterity</th>
                        <th style={thStyle}>Category</th>
                        <th style={{ ...thStyle, width: 30 }}></th>
                      </tr>
                    </thead>
                    <tbody>
                      {sectionItems.map((item) => (
                        <tr
                          key={item.id}
                          style={{ borderBottom: "1px solid var(--brd)" }}
                        >
                          <td style={{ ...tdStyle, maxWidth: 200 }}><EditCell value={item.description} onSave={v => handleCellSave(item.id, "description", v)} style={{ width: 160 }} /></td>
                          <td style={tdStyle}><EditCell value={item.brand} onSave={v => handleCellSave(item.id, "brand", v)} style={{ width: 70 }} /></td>
                          <td style={tdStyle}><EditCell value={item.wmtItemNumber} onSave={v => handleCellSave(item.id, "wmt_item_number", v)} /></td>
                          <td style={tdStyle}><EditCell value={item.upc} onSave={v => handleCellSave(item.id, "upc", v)} /></td>
                          <td style={{ ...tdStyle, color: "#2ecf99" }}><EditCell value={item.vendorStockNumber} onSave={v => handleCellSave(item.id, "vendor_stock_number", v)} /></td>
                          <td style={tdStyle}><EditCell value={item.brandId} onSave={v => handleCellSave(item.id, "brand_id", v)} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.wholesaleCost} onSave={v => handleCellSave(item.id, "wholesale_cost", v)} type="number" style={{ width: 60 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.walmartRetail} onSave={v => handleCellSave(item.id, "walmart_retail", v)} type="number" style={{ width: 60 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "center" }}><EditCell value={item.vendorPack} onSave={v => handleCellSave(item.id, "vendor_pack", v)} type="number" style={{ width: 40 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.oldStoreCount} onSave={v => handleCellSave(item.id, "old_store_count", v)} type="number" style={{ width: 50 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.newStoreCount} onSave={v => handleCellSave(item.id, "new_store_count", v)} type="number" style={{ width: 50 }} /></td>
                          <td style={{
                            ...tdStyle,
                            textAlign: "right",
                            color: item.storeCountDiff > 0 ? "#2ecfaa" : item.storeCountDiff < 0 ? "#f87171" : "var(--txt3)",
                          }}>
                            {item.storeCountDiff > 0 ? "+" : ""}{fN(item.storeCountDiff)}
                          </td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.cartonLength} onSave={v => handleCellSave(item.id, "carton_length", v)} type="number" style={{ width: 40 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.cartonWidth} onSave={v => handleCellSave(item.id, "carton_width", v)} type="number" style={{ width: 40 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.cartonHeight} onSave={v => handleCellSave(item.id, "carton_height", v)} type="number" style={{ width: 40 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.cbm} onSave={v => handleCellSave(item.id, "cbm", v)} type="number" style={{ width: 40 }} /></td>
                          <td style={{ ...tdStyle, textAlign: "right" }}><EditCell value={item.cbf} onSave={v => handleCellSave(item.id, "cbf", v)} type="number" style={{ width: 40 }} /></td>
                          <td style={tdStyle}><EditCell value={item.color} onSave={v => handleCellSave(item.id, "color", v)} /></td>
                          <td style={tdStyle}><EditCell value={item.dexterity} onSave={v => handleCellSave(item.id, "dexterity", v)} /></td>
                          <td style={tdStyle}><EditCell value={item.category} onSave={v => handleCellSave(item.id, "category", v)} /></td>
                          <td style={tdStyle}><button onClick={() => setDeleteTarget(item)} style={{ background: "none", border: "none", color: "#f87171", cursor: "pointer", fontSize: 14, padding: "2px 4px" }}>✕</button></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>
          );
        })
      )}
    </div>
  );
}

const thStyle = {
  textAlign: "left",
  padding: "6px 6px",
  color: "var(--txt3)",
  fontWeight: 700,
  whiteSpace: "nowrap",
  position: "sticky",
  top: 0,
  background: "var(--card)",
  zIndex: 2,
};

const tdStyle = {
  padding: "5px 6px",
  color: "var(--txt)",
  whiteSpace: "nowrap",
};
