import { useState, useEffect, useRef } from "react";
import { api } from "../../lib/api";
import { SG, DM, Card, fN, f$ } from "./WalmartHelpers";

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
  const fileRef = useRef(null);

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

  return (
    <div>
      {/* Header row: year tabs + upload button + search */}
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
        <div style={{ ...SG(9, 600), color: "var(--txt3)", marginBottom: 12 }}>
          Effective: Walmart Week {effWeek} &bull; Event Year {year}
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
                      </tr>
                    </thead>
                    <tbody>
                      {sectionItems.map((item) => (
                        <tr
                          key={item.id}
                          style={{ borderBottom: "1px solid var(--brd)" }}
                        >
                          <td style={{ ...tdStyle, maxWidth: 200 }}>{item.description}</td>
                          <td style={tdStyle}>{item.brand}</td>
                          <td style={tdStyle}>{item.wmtItemNumber || "—"}</td>
                          <td style={tdStyle}>{item.upc || "—"}</td>
                          <td style={{ ...tdStyle, color: "#2ecf99" }}>{item.vendorStockNumber || "—"}</td>
                          <td style={tdStyle}>{item.brandId || "—"}</td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>
                            {item.wholesaleCost ? f$(item.wholesaleCost) : "—"}
                          </td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>
                            {item.walmartRetail ? f$(item.walmartRetail) : "—"}
                          </td>
                          <td style={{ ...tdStyle, textAlign: "center" }}>
                            {item.vendorPack || "—"}{item.whsePack && item.whsePack !== item.vendorPack ? ` / ${item.whsePack}` : ""}
                          </td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>{fN(item.oldStoreCount)}</td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>{fN(item.newStoreCount)}</td>
                          <td style={{
                            ...tdStyle,
                            textAlign: "right",
                            color: item.storeCountDiff > 0 ? "#2ecfaa" : item.storeCountDiff < 0 ? "#f87171" : "var(--txt3)",
                          }}>
                            {item.storeCountDiff > 0 ? "+" : ""}{fN(item.storeCountDiff)}
                          </td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>{item.cartonLength || "—"}</td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>{item.cartonWidth || "—"}</td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>{item.cartonHeight || "—"}</td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>{item.cbm || "—"}</td>
                          <td style={{ ...tdStyle, textAlign: "right" }}>{item.cbf || "—"}</td>
                          <td style={tdStyle}>{item.color || "—"}</td>
                          <td style={tdStyle}>{item.dexterity || "—"}</td>
                          <td style={tdStyle}>{item.category || "—"}</td>
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
