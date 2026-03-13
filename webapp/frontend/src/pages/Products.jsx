import { useState, useEffect } from "react";
import {
  PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend,
} from "recharts";
import { api, fmt$ } from "../lib/api";
import { CHART_COLORS as COLORS, TOOLTIP_STYLE, COLOR_SWATCH } from "../lib/constants";

const RANGES = [
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
];

export default function Products({ filters = {} }) {
  const [days, setDays] = useState(365);
  const [products, setProducts] = useState([]);
  const [productMix, setProductMix] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState("rev");
  const [sortDir, setSortDir] = useState("desc");
  const [showAll, setShowAll] = useState(false);
  const [colorMap, setColorMap] = useState({});

  // Load item master for color data (once)
  useEffect(() => {
    api.itemMaster().then(d => {
      const map = {};
      (d.items || []).forEach(i => { if (i.asin && i.color) map[i.asin] = i.color; });
      setColorMap(map);
    }).catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    const h = filters;
    Promise.all([
      api.products(days, h),
      api.productMix(days, h),
    ]).then(([d, mix]) => {
      setProducts(d.products);
      setProductMix(mix.products || []);
      setLoading(false);
    });
  }, [days, filters.division, filters.customer]);

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

      {/* ── Product Mix Donuts (Units / Revenue / Ad Spend) ── */}
      {products.length > 0 && (() => {
        const top = [...products].sort((a, b) => b.rev - a.rev).slice(0, 10);
        const unitsMix = top.map(p => ({ name: p.name, value: p.units || 0 }));
        const revMix   = top.map(p => ({ name: p.name, value: p.rev || 0 }));
        const adMix    = top.map(p => ({ name: p.name, value: p.adSpend || 0 }));
        const fmtK = v => v >= 1000 ? `${(v/1000).toFixed(1)}k` : v.toLocaleString();
        const charts = [
          { title: "Sales Units", data: unitsMix, fmt: v => [fmtK(v), "Units"] },
          { title: "Revenue $", data: revMix, fmt: v => [fmt$(v), "Revenue"] },
          { title: "Ad Spend $", data: adMix, fmt: v => [fmt$(v), "Ad Spend"] },
        ];
        return (
          <div className="chart-card" style={{ marginBottom: 24, padding: "16px 20px 12px" }}>
            <h3 style={{ margin: "0 0 4px" }}>Product Mix (Top 10)</h3>
            <div style={{ display: "flex", gap: 0, justifyContent: "center" }}>
              {charts.map(ch => (
                <div key={ch.title} style={{ flex: "1 1 0", textAlign: "center", minWidth: 0 }}>
                  <p style={{ margin: "0 0 2px", fontSize: 12, fontWeight: 600, color: "#3E658C" }}>{ch.title}</p>
                  <ResponsiveContainer width="100%" height={180}>
                    <PieChart>
                      <Pie data={ch.data} cx="50%" cy="50%" innerRadius={38} outerRadius={72} paddingAngle={2} dataKey="value" nameKey="name" strokeWidth={1}>
                        {ch.data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                      </Pie>
                      <Tooltip contentStyle={TOOLTIP_STYLE} formatter={ch.fmt} />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              ))}
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", justifyContent: "center", gap: "4px 14px", marginTop: 4, paddingTop: 6, borderTop: "1px solid #f0f0f0" }}>
              {top.map((p, i) => (
                <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: 4, fontSize: 10, color: "#2A3D50", whiteSpace: "nowrap" }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: COLORS[i % COLORS.length], flexShrink: 0 }} />
                  {p.name.length > 30 ? p.name.slice(0, 28) + "…" : p.name}
                </span>
              ))}
            </div>
          </div>
        );
      })()}

      {/* ── Product Table ────────────────────────────── */}
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
                <td style={{ maxWidth: 280 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    {colorMap[p.asin] && (
                      <span
                        title={colorMap[p.asin]}
                        style={{
                          display: "inline-block",
                          width: 10,
                          height: 10,
                          borderRadius: "50%",
                          background: COLOR_SWATCH[colorMap[p.asin]] || "#94a3b8",
                          border: "1px solid rgba(0,0,0,0.15)",
                          flexShrink: 0,
                        }}
                      />
                    )}
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", minWidth: 0 }}>{p.name}</div>
                  </div>
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
