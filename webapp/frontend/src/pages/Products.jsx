import { useState, useEffect } from "react";
import { api, fmt$ } from "../lib/api";

const RANGES = [
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
];

export default function Products() {
  const [days, setDays] = useState(365);
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState("rev");
  const [sortDir, setSortDir] = useState("desc");
  const [showAll, setShowAll] = useState(false);

  useEffect(() => {
    setLoading(true);
    api.products(days).then((d) => {
      setProducts(d.products);
      setLoading(false);
    });
  }, [days]);

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(sortDir === "desc" ? "asc" : "desc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  const sorted = [...products].sort((a, b) => {
    const av = a[sortKey] ?? 0;
    const bv = b[sortKey] ?? 0;
    return sortDir === "desc" ? bv - av : av - bv;
  });

  const display = showAll ? sorted : sorted.slice(0, 25);

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading products...</div>;
  }

  return (
    <>
      <div className="page-header">
        <h1>Product Breakdown</h1>
        <p>{products.length} products &middot; Full P&L by SKU</p>
      </div>

      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 24 }}>
        <div className="range-tabs">
          {RANGES.map(r => (
            <button key={r.days} className={`range-tab ${days === r.days ? "active" : ""}`} onClick={() => setDays(r.days)}>
              {r.label}
            </button>
          ))}
        </div>
        <button
          className="range-tab"
          style={{ background: showAll ? "rgba(46,207,170,0.15)" : "transparent", color: showAll ? "var(--teal-dark)" : "var(--muted)" }}
          onClick={() => setShowAll(!showAll)}
        >
          {showAll ? `All ${products.length}` : "Top 25"}
        </button>
      </div>

      <div className="table-card">
        <table>
          <thead>
            <tr>
              <TH label="Product" onClick={() => handleSort("name")} active={sortKey === "name"} dir={sortDir} />
              <TH label="Revenue" onClick={() => handleSort("rev")} active={sortKey === "rev"} dir={sortDir} />
              <TH label="Units" onClick={() => handleSort("units")} active={sortKey === "units"} dir={sortDir} />
              <TH label="AUR" onClick={() => handleSort("price")} active={sortKey === "price"} dir={sortDir} />
              <TH label="Sessions" onClick={() => handleSort("sessions")} active={sortKey === "sessions"} dir={sortDir} />
              <TH label="Conv %" onClick={() => handleSort("convRate")} active={sortKey === "convRate"} dir={sortDir} />
              <TH label="COGS/Unit" onClick={() => handleSort("cogsPerUnit")} active={sortKey === "cogsPerUnit"} dir={sortDir} />
              <TH label="COGS Total" onClick={() => handleSort("cogsTotal")} active={sortKey === "cogsTotal"} dir={sortDir} />
              <TH label="FBA Fees" onClick={() => handleSort("fbaTotal")} active={sortKey === "fbaTotal"} dir={sortDir} />
              <TH label="Referral" onClick={() => handleSort("referralTotal")} active={sortKey === "referralTotal"} dir={sortDir} />
              <TH label="Ad Spend" onClick={() => handleSort("adSpend")} active={sortKey === "adSpend"} dir={sortDir} />
              <TH label="Net Profit" onClick={() => handleSort("net")} active={sortKey === "net"} dir={sortDir} />
              <TH label="Margin %" onClick={() => handleSort("margin")} active={sortKey === "margin"} dir={sortDir} />
            </tr>
          </thead>
          <tbody>
            {display.map((p) => (
              <tr key={p.asin}>
                <td style={{ maxWidth: 260 }}>
                  <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.name}</div>
                  <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                    {p.asin}{p.sku ? ` · ${p.sku}` : ""}
                  </div>
                </td>
                <td>{fmt$(p.rev)}</td>
                <td>{p.units.toLocaleString()}</td>
                <td>${p.price.toFixed(2)}</td>
                <td>{(p.sessions || 0).toLocaleString()}</td>
                <td>{(p.convRate || 0).toFixed(1)}%</td>
                <td>${(p.cogsPerUnit || 0).toFixed(2)}</td>
                <td>{fmt$(p.cogsTotal)}</td>
                <td>{fmt$(p.fbaTotal)}</td>
                <td>{fmt$(p.referralTotal)}</td>
                <td>{fmt$(p.adSpend)}</td>
                <td className={p.net >= 0 ? "pos" : "neg"}>{p.net >= 0 ? "" : "-"}{fmt$(Math.abs(p.net))}</td>
                <td className={p.margin >= 0 ? "pos" : "neg"}>{p.margin}%</td>
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
