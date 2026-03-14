import { useState, useEffect } from "react";
import { api } from "../lib/api";

export default function Inventory({ filters = {} }) {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState("fbaStock");
  const [sortDir, setSortDir] = useState("desc");

  useEffect(() => {
    setLoading(true);
    api.inventory(filters).then((inv) => {
      setItems(inv.items);
      setLoading(false);
    });
  }, [filters.division, filters.customer]);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(sortDir === "desc" ? "asc" : "desc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  const sorted = [...items].sort((a, b) => {
    const av = a[sortKey] ?? 0;
    const bv = b[sortKey] ?? 0;
    if (typeof av === "string") return sortDir === "desc" ? bv.localeCompare(av) : av.localeCompare(bv);
    return sortDir === "desc" ? bv - av : av - bv;
  });

  const lowStock = items.filter(i => i.dos < 30 && i.avgDaily > 0).length;
  const healthy = items.filter(i => i.dos >= 30 && i.dos <= 90).length;
  const overstock = items.filter(i => i.dos > 90).length;

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading Amazon inventory...</div>;
  }

  return (
    <>
      <div className="page-header">
        <h1>Amazon Inventory</h1>
        <p>{items.length} SKUs in FBA &middot; Days of supply analysis</p>
      </div>

      <div className="kpi-grid" style={{ marginBottom: 24 }}>
        <div className="kpi-card">
          <div className="kpi-label">Low Stock (&lt;30 days)</div>
          <div className={`kpi-value ${lowStock > 0 ? "neg" : ""}`}>{lowStock}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Healthy (30-90 days)</div>
          <div className="kpi-value pos">{healthy}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Overstock (&gt;90 days)</div>
          <div className="kpi-value" style={{ color: "var(--gold)" }}>{overstock}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Total FBA Units</div>
          <div className="kpi-value">{items.reduce((s, i) => s + i.fbaStock, 0).toLocaleString()}</div>
        </div>
      </div>

      <div className="table-card">
        <table>
          <thead>
            <tr>
              <TH label="Product" onClick={() => handleSort("name")} active={sortKey === "name"} dir={sortDir} />
              <TH label="FBA Stock" onClick={() => handleSort("fbaStock")} active={sortKey === "fbaStock"} dir={sortDir} />
              <TH label="Inbound" onClick={() => handleSort("inbound")} active={sortKey === "inbound"} dir={sortDir} />
              <TH label="Reserved" onClick={() => handleSort("reserved")} active={sortKey === "reserved"} dir={sortDir} />
              <TH label="Avg Daily" onClick={() => handleSort("avgDaily")} active={sortKey === "avgDaily"} dir={sortDir} />
              <TH label="Days of Supply" onClick={() => handleSort("dos")} active={sortKey === "dos"} dir={sortDir} />
              <TH label="Status" />
            </tr>
          </thead>
          <tbody>
            {sorted.map((item) => {
              const status = item.dos < 30 && item.avgDaily > 0
                ? { label: "RESTOCK", cls: "neg" }
                : item.dos > 90
                ? { label: "OVERSTOCK", cls: "", style: { color: "var(--gold)" } }
                : { label: "OK", cls: "pos" };

              return (
                <tr key={item.asin}>
                  <td style={{ maxWidth: 280 }}>
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.name}</div>
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.asin}{item.sku ? ` · ${item.sku}` : ""}
                    </div>
                  </td>
                  <td>{item.fbaStock.toLocaleString()}</td>
                  <td>{item.inbound.toLocaleString()}</td>
                  <td>{item.reserved.toLocaleString()}</td>
                  <td>{item.avgDaily}</td>
                  <td className={item.dos < 30 ? "neg" : item.dos > 90 ? "" : "pos"} style={item.dos > 90 ? { color: "var(--gold)" } : {}}>
                    {item.dos >= 999 ? "∞" : item.dos}
                  </td>
                  <td className={status.cls} style={status.style || {}}>{status.label}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </>
  );
}

function TH({ label, onClick, active, dir }) {
  return (
    <th onClick={onClick} style={active ? { color: "var(--teal-dark)" } : {}}>
      {label} {active ? (dir === "desc" ? "▼" : "▲") : ""}
    </th>
  );
}
