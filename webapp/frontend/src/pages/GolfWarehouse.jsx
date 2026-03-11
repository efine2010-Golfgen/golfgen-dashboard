import { useState, useEffect } from "react";
import { api } from "../lib/api";

const CHANNELS = ["All", "Amazon", "Walmart", "Walmart & Amazon", "Other"];

export default function GolfWarehouse() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [channel, setChannel] = useState("All");
  const [sortKey, setSortKey] = useState("pcsOnHand");
  const [sortDir, setSortDir] = useState("desc");
  const [search, setSearch] = useState("");

  useEffect(() => {
    setLoading(true);
    api.warehouseGolf(channel === "All" ? null : channel).then((d) => {
      setData(d);
      setLoading(false);
    });
  }, [channel]);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(sortDir === "desc" ? "asc" : "desc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  if (loading || !data) {
    return <div className="loading"><div className="spinner" /> Loading golf inventory...</div>;
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

  const { summary, channelBreakdown } = data;

  return (
    <>
      <div className="page-header">
        <h1>Golf — Warehouse Inventory</h1>
        <p>Moose 3PL daily inventory report &middot; {summary.totalSkus} SKUs</p>
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

      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 24, flexWrap: "wrap" }}>
        <div className="range-tabs">
          {CHANNELS.map(ch => (
            <button
              key={ch}
              className={`range-tab ${channel === ch ? "active" : ""}`}
              onClick={() => setChannel(ch)}
            >
              {ch}
              {channelBreakdown && ch !== "All" && (
                <span style={{ opacity: 0.7, marginLeft: 4, fontSize: 11 }}>({channelBreakdown[ch] || 0})</span>
              )}
            </button>
          ))}
        </div>
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
              <TH label="Channel" onClick={() => handleSort("channel")} active={sortKey === "channel"} dir={sortDir} />
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
            {sorted.map((item, idx) => {
              const chColor = item.channel === "Amazon" ? "var(--orange)" : item.channel === "Walmart" ? "var(--blue)" : item.channel === "Walmart & Amazon" ? "var(--sky-blue)" : "var(--muted)";
              return (
                <tr key={idx}>
                  <td style={{ fontFamily: "'Space Grotesk', monospace", fontSize: 12 }}>{item.itemNumber}</td>
                  <td style={{ maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.description}</td>
                  <td>
                    <span style={{
                      display: "inline-block", padding: "2px 10px", borderRadius: 12,
                      fontSize: 11, fontWeight: 600,
                      background: `${chColor}18`, color: chColor, border: `1px solid ${chColor}33`,
                    }}>{item.channel}</span>
                  </td>
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
