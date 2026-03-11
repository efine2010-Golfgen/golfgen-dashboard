import { useState, useEffect } from "react";
import { api } from "../lib/api";

export default function HousewaresWarehouse() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState("pcsOnHand");
  const [sortDir, setSortDir] = useState("desc");
  const [search, setSearch] = useState("");

  useEffect(() => {
    api.warehouseHousewares().then((d) => {
      setData(d);
      setLoading(false);
    });
  }, []);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(sortDir === "desc" ? "asc" : "desc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  if (loading || !data) {
    return <div className="loading"><div className="spinner" /> Loading housewares inventory...</div>;
  }

  const filtered = data.items.filter(i =>
    !search || i.description.toLowerCase().includes(search.toLowerCase()) || i.itemNumber.toLowerCase().includes(search.toLowerCase())
  );

  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortKey] ?? 0;
    const bv = b[sortKey] ?? 0;
    if (typeof av === "string") return sortDir === "desc" ? bv.localeCompare(av) : av.localeCompare(bv);
    return sortDir === "desc" ? bv - av : av - bv;
  });

  const { summary } = data;

  return (
    <>
      <div className="page-header">
        <h1>Housewares — Warehouse Inventory</h1>
        <p>Housewares daily inventory report &middot; {summary.totalSkus} SKUs</p>
      </div>

      <div className="kpi-grid" style={{ marginBottom: 24 }}>
        <div className="kpi-card">
          <div className="kpi-label">Total SKUs</div>
          <div className="kpi-value">{summary.totalSkus}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Pcs On Hand</div>
          <div className="kpi-value teal">{summary.totalPcsOnHand.toLocaleString()}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Pcs Available</div>
          <div className="kpi-value pos">{summary.totalPcsAvailable.toLocaleString()}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Pcs Allocated</div>
          <div className="kpi-value" style={{ color: "var(--gold)" }}>{summary.totalPcsAllocated.toLocaleString()}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Damage</div>
          <div className={`kpi-value ${summary.totalDamage > 0 ? "neg" : ""}`}>{summary.totalDamage.toLocaleString()}</div>
        </div>
      </div>

      <div style={{ marginBottom: 24 }}>
        <input
          type="text"
          placeholder="Search SKU or description..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            padding: "6px 14px", border: "1px solid rgba(14,31,45,0.15)", borderRadius: 8,
            fontSize: 13, width: 260, outline: "none",
          }}
        />
      </div>

      <div className="table-card">
        <table>
          <thead>
            <tr>
              <TH label="Item #" onClick={() => handleSort("itemNumber")} active={sortKey === "itemNumber"} dir={sortDir} />
              <TH label="Description" onClick={() => handleSort("description")} active={sortKey === "description"} dir={sortDir} />
              <TH label="Pack" onClick={() => handleSort("pack")} active={sortKey === "pack"} dir={sortDir} />
              <TH label="On-Hand" onClick={() => handleSort("onHand")} active={sortKey === "onHand"} dir={sortDir} />
              <TH label="Pcs On-Hand" onClick={() => handleSort("pcsOnHand")} active={sortKey === "pcsOnHand"} dir={sortDir} />
              <TH label="Pcs Available" onClick={() => handleSort("pcsAvailable")} active={sortKey === "pcsAvailable"} dir={sortDir} />
              <TH label="Pcs Allocated" onClick={() => handleSort("pcsAllocated")} active={sortKey === "pcsAllocated"} dir={sortDir} />
              <TH label="Standard" onClick={() => handleSort("standard")} active={sortKey === "standard"} dir={sortDir} />
              <TH label="NonStandard" onClick={() => handleSort("nonStandard")} active={sortKey === "nonStandard"} dir={sortDir} />
              <TH label="Damage" onClick={() => handleSort("damage")} active={sortKey === "damage"} dir={sortDir} />
              <TH label="QC Hold" onClick={() => handleSort("qcHold")} active={sortKey === "qcHold"} dir={sortDir} />
            </tr>
          </thead>
          <tbody>
            {sorted.map((item, idx) => (
              <tr key={idx}>
                <td style={{ fontFamily: "'Space Grotesk', monospace", fontSize: 12 }}>{item.itemNumber}</td>
                <td style={{ maxWidth: 300, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.description}</td>
                <td>{item.pack}</td>
                <td>{item.onHand.toLocaleString()}</td>
                <td style={{ fontWeight: 600 }}>{item.pcsOnHand.toLocaleString()}</td>
                <td className="pos">{item.pcsAvailable.toLocaleString()}</td>
                <td style={{ color: item.pcsAllocated > 0 ? "var(--gold)" : "inherit" }}>{item.pcsAllocated.toLocaleString()}</td>
                <td>{item.standard.toLocaleString()}</td>
                <td>{item.nonStandard.toLocaleString()}</td>
                <td className={item.damage > 0 ? "neg" : ""}>{item.damage.toLocaleString()}</td>
                <td>{item.qcHold.toLocaleString()}</td>
              </tr>
            ))}
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
