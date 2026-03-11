import { useState, useEffect } from "react";
import { api } from "../lib/api";

const STATUS_COLORS = {
  WORKING: { bg: "#E8F4FD", text: "#1565C0", border: "#90CAF9" },
  SHIPPED: { bg: "#E8F5E9", text: "#2E7D32", border: "#81C784" },
  RECEIVING: { bg: "#FFF3E0", text: "#E65100", border: "#FFB74D" },
  IN_TRANSIT: { bg: "#F3E5F5", text: "#6A1B9A", border: "#CE93D8" },
  CLOSED: { bg: "#F5F5F5", text: "#616161", border: "#BDBDBD" },
  CHECKED_IN: { bg: "#E0F7FA", text: "#00695C", border: "#80CBC4" },
  CANCELLED: { bg: "#FFEBEE", text: "#C62828", border: "#EF9A9A" },
};

const STATUS_ORDER = ["IN_TRANSIT", "SHIPPED", "RECEIVING", "CHECKED_IN", "WORKING", "CLOSED", "CANCELLED"];

function StatusBadge({ status }) {
  const colors = STATUS_COLORS[status] || { bg: "#F5F5F5", text: "#333", border: "#DDD" };
  return (
    <span style={{
      display: "inline-block",
      padding: "3px 10px",
      borderRadius: 12,
      fontSize: 11,
      fontWeight: 600,
      background: colors.bg,
      color: colors.text,
      border: `1px solid ${colors.border}`,
      whiteSpace: "nowrap",
    }}>
      {status.replace(/_/g, " ")}
    </span>
  );
}

function PercentBar({ shipped, received }) {
  const pct = shipped > 0 ? Math.round((received / shipped) * 100) : 0;
  const barColor = pct === 100 ? "#43A047" : pct > 50 ? "#FFA726" : pct > 0 ? "#EF5350" : "#E0E0E0";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: 6, background: "#F0F0F0", borderRadius: 3, overflow: "hidden", minWidth: 60 }}>
        <div style={{ width: `${pct}%`, height: "100%", background: barColor, borderRadius: 3, transition: "width 0.3s" }} />
      </div>
      <span style={{ fontSize: 11, fontWeight: 600, color: pct === 100 ? "#43A047" : "#666", minWidth: 32 }}>{pct}%</span>
    </div>
  );
}

export default function FBAShipments() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [statusFilter, setStatusFilter] = useState("ALL");
  const [expandedId, setExpandedId] = useState(null);
  const [itemsCache, setItemsCache] = useState({});
  const [loadingItems, setLoadingItems] = useState(null);
  const [search, setSearch] = useState("");

  const load = (refresh = false) => {
    setLoading(true);
    api.fbaShipments(refresh)
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  };
  useEffect(() => load(), []);

  const handleSync = async () => {
    setSyncing(true);
    try {
      const res = await api.fbaShipmentsSync();
      if (res.ok) load(false); // reload from cache after sync
      else alert("Sync failed: " + (res.error || "Unknown error"));
    } catch (err) { alert("Sync failed: " + err.message); }
    setSyncing(false);
  };

  const toggleExpand = async (shipmentId) => {
    if (expandedId === shipmentId) {
      setExpandedId(null);
      return;
    }
    setExpandedId(shipmentId);
    if (!itemsCache[shipmentId]) {
      setLoadingItems(shipmentId);
      try {
        const res = await api.fbaShipmentItems(shipmentId);
        setItemsCache(prev => ({ ...prev, [shipmentId]: res.items || [] }));
      } catch { setItemsCache(prev => ({ ...prev, [shipmentId]: [] })); }
      setLoadingItems(null);
    }
  };

  if (loading) return <div className="loading"><div className="spinner" /> Loading FBA Shipments...</div>;

  const shipments = data?.shipments || [];

  // Status counts for filter tabs
  const statusCounts = {};
  shipments.forEach(s => { statusCounts[s.status] = (statusCounts[s.status] || 0) + 1; });

  // Apply filters
  let filtered = statusFilter === "ALL" ? shipments : shipments.filter(s => s.status === statusFilter);
  if (search.trim()) {
    const q = search.toLowerCase();
    filtered = filtered.filter(s =>
      (s.shipmentId || "").toLowerCase().includes(q) ||
      (s.shipmentName || "").toLowerCase().includes(q) ||
      (s.destination || "").toLowerCase().includes(q)
    );
  }

  // Sort by status priority
  filtered.sort((a, b) => {
    const ai = STATUS_ORDER.indexOf(a.status);
    const bi = STATUS_ORDER.indexOf(b.status);
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });

  // KPI calculations
  const activeCount = shipments.filter(s => ["WORKING", "SHIPPED", "RECEIVING", "IN_TRANSIT", "CHECKED_IN"].includes(s.status)).length;
  const closedCount = shipments.filter(s => s.status === "CLOSED").length;
  const inTransitCount = shipments.filter(s => ["SHIPPED", "IN_TRANSIT"].includes(s.status)).length;

  return (
    <div>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div>
          <h2 style={{ fontFamily: "'Space Grotesk', sans-serif", fontSize: 22, color: "var(--navy)", margin: 0 }}>
            Shipments to FBA
          </h2>
          <p style={{ fontSize: 12, color: "var(--muted)", marginTop: 4 }}>
            Amazon FBA Inbound Shipment Plans{data?.lastSync ? ` \u2022 Last synced: ${data.lastSync}` : ""}
          </p>
        </div>
        <button onClick={handleSync} disabled={syncing}
          style={{ padding: "8px 20px", background: "var(--teal)", color: "#fff", border: "none", borderRadius: 8, fontWeight: 600, fontSize: 13, cursor: "pointer", opacity: syncing ? 0.6 : 1 }}>
          {syncing ? "Syncing..." : "\u{1F504} Sync from Amazon"}
        </button>
      </div>

      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12, marginBottom: 24 }}>
        {[
          { label: "Total Shipments", value: shipments.length, color: "#3E658C" },
          { label: "Active", value: activeCount, color: "#2ECFAA" },
          { label: "In Transit / Shipped", value: inTransitCount, color: "#E87830" },
          { label: "Closed", value: closedCount, color: "#94A3B8" },
        ].map((kpi, i) => (
          <div key={i} style={{ background: "#fff", borderRadius: 10, padding: "14px 16px", boxShadow: "var(--card-shadow)", borderLeft: `3px solid ${kpi.color}` }}>
            <div style={{ fontSize: 11, color: "var(--muted)", fontWeight: 600, letterSpacing: 0.5 }}>{kpi.label}</div>
            <div style={{ fontSize: 26, fontWeight: 700, fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginTop: 2 }}>{kpi.value}</div>
          </div>
        ))}
      </div>

      {/* Filters Row */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, gap: 12, flexWrap: "wrap" }}>
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
          <button onClick={() => setStatusFilter("ALL")}
            style={filterBtnStyle(statusFilter === "ALL")}>
            All ({shipments.length})
          </button>
          {STATUS_ORDER.filter(st => statusCounts[st]).map(st => (
            <button key={st} onClick={() => setStatusFilter(st)}
              style={filterBtnStyle(statusFilter === st)}>
              {st.replace(/_/g, " ")} ({statusCounts[st]})
            </button>
          ))}
        </div>
        <input
          type="text"
          placeholder="Search shipments..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{ padding: "7px 14px", border: "1px solid var(--border)", borderRadius: 8, fontSize: 12, width: 220, outline: "none" }}
        />
      </div>

      {/* Shipments Table */}
      <div style={{ background: "#fff", borderRadius: 12, overflow: "hidden", boxShadow: "var(--card-shadow)" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead>
            <tr style={{ background: "var(--navy)", color: "#fff" }}>
              <th style={th}></th>
              <th style={th}>Shipment Plan / ID</th>
              <th style={th}>Destination</th>
              <th style={th}>Products</th>
              <th style={th}>Status</th>
              <th style={th}>Ship From</th>
              <th style={th}>Label Prep</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 && (
              <tr>
                <td colSpan={7} style={{ padding: 40, textAlign: "center", color: "var(--muted)", fontSize: 13 }}>
                  {shipments.length === 0 ? "No FBA shipments found. Click \"Sync from Amazon\" to fetch data." : "No shipments match current filters."}
                </td>
              </tr>
            )}
            {filtered.map((s, i) => (
              <ShipmentRow
                key={s.shipmentId}
                shipment={s}
                index={i}
                isExpanded={expandedId === s.shipmentId}
                onToggle={() => toggleExpand(s.shipmentId)}
                items={itemsCache[s.shipmentId]}
                loadingItems={loadingItems === s.shipmentId}
              />
            ))}
          </tbody>
        </table>
      </div>

      {data?._note && (
        <p style={{ fontSize: 11, color: "var(--orange)", marginTop: 12, textAlign: "center" }}>{data._note}</p>
      )}
    </div>
  );
}

function ShipmentRow({ shipment: s, index, isExpanded, onToggle, items, loadingItems }) {
  const productLabel = s.itemCount > 0
    ? `${s.itemCount} product${s.itemCount !== 1 ? "s" : ""}`
    : "—";

  return (
    <>
      <tr
        onClick={onToggle}
        style={{
          borderBottom: isExpanded ? "none" : "1px solid var(--border)",
          background: index % 2 ? "#FAFBFC" : "#fff",
          cursor: "pointer",
          transition: "background 0.15s",
        }}
        onMouseEnter={e => e.currentTarget.style.background = "#F0F7FF"}
        onMouseLeave={e => e.currentTarget.style.background = index % 2 ? "#FAFBFC" : "#fff"}
      >
        <td style={{ ...td, width: 24, textAlign: "center", fontSize: 10, color: "#94A3B8" }}>
          {isExpanded ? "\u25BC" : "\u25B6"}
        </td>
        <td style={td}>
          <div style={{ fontWeight: 600, color: "var(--blue)", fontSize: 12 }}>{s.shipmentName || s.shipmentId}</div>
          <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 1, fontFamily: "monospace" }}>{s.shipmentId}</div>
        </td>
        <td style={{ ...td, fontSize: 11 }}>{s.destination || "—"}</td>
        <td style={{ ...td, fontSize: 11 }}>{productLabel}</td>
        <td style={td}><StatusBadge status={s.status} /></td>
        <td style={{ ...td, fontSize: 11, color: "var(--muted)" }}>{s.shipFrom || "—"}</td>
        <td style={{ ...td, fontSize: 10, color: "var(--muted)" }}>{s.labelPrep || "—"}</td>
      </tr>

      {/* Expanded Items Row */}
      {isExpanded && (
        <tr style={{ background: "#F8FAFC" }}>
          <td colSpan={7} style={{ padding: "0 12px 12px 36px" }}>
            {loadingItems ? (
              <div style={{ padding: 16, textAlign: "center", color: "var(--muted)", fontSize: 12 }}>
                Loading items...
              </div>
            ) : items && items.length > 0 ? (
              <div style={{ marginTop: 8 }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: "var(--navy)", marginBottom: 8 }}>
                  Shipment Items ({items.length})
                </div>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                  <thead>
                    <tr style={{ background: "#EDF2F7" }}>
                      <th style={thSub}>SKU</th>
                      <th style={thSub}>FNSKU</th>
                      <th style={{ ...thSub, textAlign: "right" }}>Shipped</th>
                      <th style={{ ...thSub, textAlign: "right" }}>Received</th>
                      <th style={{ ...thSub, width: 140 }}>% Received</th>
                    </tr>
                  </thead>
                  <tbody>
                    {items.map((item, j) => (
                      <tr key={j} style={{ borderBottom: "1px solid #E8ECF0" }}>
                        <td style={{ ...tdSub, fontWeight: 600, fontFamily: "monospace", fontSize: 10 }}>{item.sku}</td>
                        <td style={{ ...tdSub, fontFamily: "monospace", fontSize: 10, color: "var(--muted)" }}>{item.fnsku}</td>
                        <td style={{ ...tdSub, textAlign: "right" }}>{(item.quantityShipped || 0).toLocaleString()}</td>
                        <td style={{ ...tdSub, textAlign: "right" }}>{(item.quantityReceived || 0).toLocaleString()}</td>
                        <td style={tdSub}>
                          <PercentBar shipped={item.quantityShipped || 0} received={item.quantityReceived || 0} />
                        </td>
                      </tr>
                    ))}
                    {/* Totals row */}
                    <tr style={{ background: "#EDF2F7", fontWeight: 700 }}>
                      <td style={tdSub} colSpan={2}>TOTAL</td>
                      <td style={{ ...tdSub, textAlign: "right" }}>
                        {items.reduce((s, i) => s + (i.quantityShipped || 0), 0).toLocaleString()}
                      </td>
                      <td style={{ ...tdSub, textAlign: "right" }}>
                        {items.reduce((s, i) => s + (i.quantityReceived || 0), 0).toLocaleString()}
                      </td>
                      <td style={tdSub}>
                        <PercentBar
                          shipped={items.reduce((s, i) => s + (i.quantityShipped || 0), 0)}
                          received={items.reduce((s, i) => s + (i.quantityReceived || 0), 0)}
                        />
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            ) : (
              <div style={{ padding: 16, textAlign: "center", color: "var(--muted)", fontSize: 12 }}>
                No item details available for this shipment.
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

function filterBtnStyle(active) {
  return {
    padding: "5px 12px",
    borderRadius: 6,
    border: `1px solid ${active ? "var(--teal)" : "var(--border)"}`,
    background: active ? "var(--teal)" : "#fff",
    color: active ? "#fff" : "var(--body-text)",
    fontSize: 11,
    fontWeight: 600,
    cursor: "pointer",
    whiteSpace: "nowrap",
  };
}

const th = { padding: "10px 12px", textAlign: "left", fontWeight: 600, fontSize: 11, letterSpacing: 0.3 };
const td = { padding: "10px 12px" };
const thSub = { padding: "6px 10px", textAlign: "left", fontWeight: 600, fontSize: 10, letterSpacing: 0.3 };
const tdSub = { padding: "5px 10px" };
