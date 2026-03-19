import { useState, useEffect } from "react";
import {
  AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend, ComposedChart, Line
} from "recharts";
import { api, fmt$ } from "../lib/api";
import { TOOLTIP_STYLE } from "../lib/constants";

const RANGES = [
  { label: "7D", days: 7 },
  { label: "14D", days: 14 },
  { label: "30D", days: 30 },
  { label: "60D", days: 60 },
  { label: "90D", days: 90 },
];

const TABS = [
  { key: "overview", label: "Overview" },
  { key: "funnel", label: "Conversion Funnel" },
  { key: "campaigns", label: "Campaigns" },
  { key: "keywords", label: "Keywords" },
  { key: "searchTerms", label: "Search Terms" },
  { key: "negKeywords", label: "Negative KWs" },
];

export default function Advertising({ filters = {} }) {
  const [days, setDays] = useState(30);
  const [tab, setTab] = useState("overview");
  const [summary, setSummary] = useState(null);
  const [daily, setDaily] = useState([]);
  const [campaigns, setCampaigns] = useState([]);
  const [keywords, setKeywords] = useState([]);
  const [searchTerms, setSearchTerms] = useState([]);
  const [negKeywords, setNegKeywords] = useState([]);
  const [funnelData, setFunnelData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState("spend");
  const [sortDir, setSortDir] = useState("desc");

  useEffect(() => {
    setLoading(true);
    const h = filters;
    const promises = [api.adsSummary(days, h)];

    if (tab === "overview") {
      promises.push(api.adsDaily(days, h));
    } else if (tab === "funnel") {
      promises.push(api.adsFunnel(days, h));
    } else if (tab === "campaigns") {
      promises.push(api.adsCampaigns(days, h));
    } else if (tab === "keywords") {
      promises.push(api.adsKeywords(days, 100, h));
    } else if (tab === "searchTerms") {
      promises.push(api.adsSearchTerms(days, 100, h));
    } else if (tab === "negKeywords") {
      promises.push(api.adsNegativeKeywords(h));
    }

    Promise.all(promises).then(([s, detail]) => {
      setSummary(s);
      if (tab === "overview") setDaily(detail?.data || []);
      else if (tab === "funnel") setFunnelData(detail || null);
      else if (tab === "campaigns") setCampaigns(detail?.campaigns || []);
      else if (tab === "keywords") setKeywords(detail?.keywords || []);
      else if (tab === "searchTerms") setSearchTerms(detail?.searchTerms || []);
      else if (tab === "negKeywords") setNegKeywords(detail?.negativeKeywords || []);
      setLoading(false);
    }).catch(err => {
      console.error("Ads API error:", err);
      setLoading(false);
    });
  }, [days, tab, filters.division, filters.customer]);

  const handleSort = (key) => {
    if (sortKey === key) setSortDir(sortDir === "desc" ? "asc" : "desc");
    else { setSortKey(key); setSortDir("desc"); }
  };

  const sortData = (data) => {
    return [...data].sort((a, b) => {
      const av = a[sortKey] ?? 0;
      const bv = b[sortKey] ?? 0;
      if (typeof av === "string") return sortDir === "desc" ? bv.localeCompare(av) : av.localeCompare(bv);
      return sortDir === "desc" ? bv - av : av - bv;
    });
  };

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading advertising data...</div>;
  }

  // Not connected state
  if (summary && !summary.connected) {
    return (
      <>
        <div className="page-header">
          <h1>Advertising</h1>
          <p>Amazon Ads performance &amp; optimization</p>
        </div>
        <div className="chart-card" style={{ maxWidth: 600, textAlign: "center", padding: 40 }}>
          <div style={{ fontSize: 48, marginBottom: 16 }}>📣</div>
          <h3 style={{ marginBottom: 12 }}>Amazon Ads API Not Connected</h3>
          <p style={{ color: "var(--muted)", marginBottom: 20, lineHeight: 1.7 }}>
            To see your campaign performance, keywords, search terms, and ROAS data,
            connect the Amazon Ads API by adding your credentials to
            <code style={{ background: "rgba(46,207,170,0.1)", padding: "2px 6px", borderRadius: 4, marginLeft: 4 }}>
              config/credentials.json
            </code>
          </p>
          <p style={{ color: "var(--muted)", fontSize: 13 }}>
            Then run: <code style={{ color: "var(--teal)" }}>python scripts/amazon_ads_api.py</code>
          </p>
        </div>
      </>
    );
  }

  return (
    <>
      <div className="page-header">
        <h1>Advertising</h1>
        <p>Amazon Ads performance &amp; optimization</p>
      </div>

      {/* Tab bar + date range */}
      <div style={{ display: "flex", gap: 16, alignItems: "center", marginBottom: 24, flexWrap: "wrap" }}>
        <div className="range-tabs">
          {TABS.map(t => (
            <button key={t.key} className={`range-tab ${tab === t.key ? "active" : ""}`}
              onClick={() => { setTab(t.key); setSortKey("spend"); setSortDir("desc"); }}>
              {t.label}
            </button>
          ))}
        </div>
        <div className="range-tabs">
          {RANGES.map(r => (
            <button key={r.days} className={`range-tab ${days === r.days ? "active" : ""}`}
              onClick={() => setDays(r.days)}>
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {/* KPI row — always shown */}
      {summary && (
        <div className="kpi-grid">
          <KPI label="Ad Spend" value={fmt$(summary.spend)} className="orange" />
          <KPI label="Ad Sales" value={fmt$(summary.adSales)} className="teal" />
          <KPI label="ACOS" value={`${summary.acos}%`} className={summary.acos <= 30 ? "pos" : "neg"} />
          <KPI label="ROAS" value={`${summary.roas}x`} className={summary.roas >= 3 ? "pos" : summary.roas >= 2 ? "" : "neg"} />
          <KPI label="TACOS" value={`${summary.tacos}%`} className={summary.tacos <= 15 ? "pos" : summary.tacos <= 25 ? "" : "neg"} />
          <KPI label="CPC" value={`$${summary.cpc}`} />
          <KPI label="CTR" value={`${summary.ctr}%`} />
          <KPI label="CVR" value={`${summary.cvr}%`} />
        </div>
      )}

      {/* Tab content */}
      {tab === "overview" && <OverviewTab daily={daily} />}
      {tab === "funnel" && <FunnelTab funnelData={funnelData} days={days} />}
      {tab === "campaigns" && <CampaignsTab data={sortData(campaigns)} onSort={handleSort} sortKey={sortKey} sortDir={sortDir} />}
      {tab === "keywords" && <KeywordsTab data={sortData(keywords)} onSort={handleSort} sortKey={sortKey} sortDir={sortDir} />}
      {tab === "searchTerms" && <SearchTermsTab data={sortData(searchTerms)} onSort={handleSort} sortKey={sortKey} sortDir={sortDir} />}
      {tab === "negKeywords" && <NegativeKeywordsTab data={negKeywords} />}
    </>
  );
}


// ── KPI Card ──────────────────────────────────────────

function KPI({ label, value, className = "" }) {
  return (
    <div className="kpi-card">
      <div className="kpi-label">{label}</div>
      <div className={`kpi-value ${className}`}>{value}</div>
    </div>
  );
}


// ── Overview Tab ──────────────────────────────────────

function OverviewTab({ daily }) {
  if (!daily.length) {
    return <div className="chart-card" style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>No daily data yet. Run the ads pipeline to populate.</div>;
  }

  return (
    <div className="chart-grid">
      <div className="chart-card">
        <h3>Ad Spend vs Sales</h3>
        <p className="sub">Daily spend and attributed sales</p>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={daily}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
            <XAxis dataKey="date" tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
            <YAxis tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={v => `$${(v).toFixed(0)}`} />
            <Tooltip contentStyle={TOOLTIP_STYLE}
              formatter={(v, name) => [`$${v.toFixed(2)}`, name]} />
            <Legend />
            <Bar dataKey="spend" fill="#E87830" radius={[3,3,0,0]} name="Ad Spend" />
            <Line type="monotone" dataKey="adSales" stroke="#2ECFAA" strokeWidth={2} dot={false} name="Ad Sales" />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <div className="chart-card">
        <h3>ACOS & TACOS Trend</h3>
        <p className="sub">Advertising cost efficiency over time</p>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={daily}>
            <defs>
              <linearGradient id="acosGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#F5B731" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#F5B731" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="tacosGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#7BAED0" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#7BAED0" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
            <XAxis dataKey="date" tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
            <YAxis tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={v => `${v}%`} />
            <Tooltip contentStyle={TOOLTIP_STYLE}
              formatter={(v) => [`${v}%`]} />
            <Legend />
            <Area type="monotone" dataKey="acos" stroke="#F5B731" strokeWidth={2} fill="url(#acosGrad)" name="ACOS" />
            <Area type="monotone" dataKey="tacos" stroke="#7BAED0" strokeWidth={2} fill="url(#tacosGrad)" name="TACOS" />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      <div className="chart-card">
        <h3>Clicks & Impressions</h3>
        <p className="sub">Daily click and impression volume</p>
        <ResponsiveContainer width="100%" height={300}>
          <ComposedChart data={daily}>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
            <XAxis dataKey="date" tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
            <YAxis yAxisId="left" tick={{ fill: "#6B8090", fontSize: 11 }} />
            <YAxis yAxisId="right" orientation="right" tick={{ fill: "#6B8090", fontSize: 11 }} />
            <Tooltip contentStyle={TOOLTIP_STYLE} />
            <Legend />
            <Bar yAxisId="left" dataKey="clicks" fill="#3E658C" radius={[3,3,0,0]} name="Clicks" />
            <Line yAxisId="right" type="monotone" dataKey="impressions" stroke="#E87830" strokeWidth={2} dot={false} name="Impressions" />
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      <div className="chart-card">
        <h3>ROAS Trend</h3>
        <p className="sub">Return on ad spend over time</p>
        <ResponsiveContainer width="100%" height={300}>
          <AreaChart data={daily}>
            <defs>
              <linearGradient id="roasGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#2ECFAA" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#2ECFAA" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(14,31,45,0.08)" />
            <XAxis dataKey="date" tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={d => d.slice(5)} />
            <YAxis tick={{ fill: "#6B8090", fontSize: 11 }} tickFormatter={v => `${v}x`} />
            <Tooltip contentStyle={TOOLTIP_STYLE}
              formatter={(v) => [`${v}x`, "ROAS"]} />
            <Area type="monotone" dataKey="roas" stroke="#2ECFAA" strokeWidth={2} fill="url(#roasGrad)" />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}


// ── Campaigns Tab ──────────────────────────────────────

function CampaignsTab({ data, onSort, sortKey, sortDir }) {
  if (!data.length) {
    return <div className="chart-card" style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>No campaign data yet.</div>;
  }

  return (
    <div className="table-card">
      <table>
        <thead>
          <tr>
            <TH label="Campaign" k="campaignName" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Status" k="status" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Budget" k="dailyBudget" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Spend" k="spend" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Sales" k="sales" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="ACOS" k="acos" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="ROAS" k="roas" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Impr" k="impressions" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Clicks" k="clicks" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="CPC" k="cpc" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="CTR" k="ctr" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="CVR" k="cvr" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Orders" k="orders" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
          </tr>
        </thead>
        <tbody>
          {data.map((c, i) => (
            <tr key={c.campaignId || i}>
              <td style={{ maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" }}>{c.campaignName}</td>
              <td><StatusBadge status={c.status} /></td>
              <td>{fmt$(c.dailyBudget)}/d</td>
              <td>{fmt$(c.spend)}</td>
              <td className="teal">{fmt$(c.sales)}</td>
              <td className={c.acos <= 30 ? "pos" : "neg"}>{c.acos}%</td>
              <td className={c.roas >= 3 ? "pos" : ""}>{c.roas}x</td>
              <td>{(c.impressions || 0).toLocaleString()}</td>
              <td>{(c.clicks || 0).toLocaleString()}</td>
              <td>${c.cpc}</td>
              <td>{c.ctr}%</td>
              <td>{c.cvr}%</td>
              <td>{c.orders}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


// ── Keywords Tab ──────────────────────────────────────

function KeywordsTab({ data, onSort, sortKey, sortDir }) {
  if (!data.length) {
    return <div className="chart-card" style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>No keyword data yet.</div>;
  }

  // Split into best/worst for quick view
  const best = [...data].sort((a, b) => b.roas - a.roas).slice(0, 5);
  const worst = [...data].filter(k => k.spend > 0).sort((a, b) => a.roas - b.roas).slice(0, 5);

  return (
    <>
      {/* Quick insight cards */}
      <div className="chart-grid" style={{ marginBottom: 20 }}>
        <div className="chart-card">
          <h3 style={{ color: "var(--teal)" }}>Top 5 Keywords (by ROAS)</h3>
          <p className="sub">Best performing keywords</p>
          {best.map((k, i) => (
            <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "8px 0", borderBottom: "1px solid rgba(14,31,45,0.06)" }}>
              <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k.keyword} <span style={{ color: "var(--muted)", fontSize: 11 }}>({k.matchType})</span></span>
              <span style={{ color: "var(--teal)", fontWeight: 600, minWidth: 60, textAlign: "right" }}>{k.roas}x</span>
              <span style={{ color: "var(--muted)", minWidth: 60, textAlign: "right" }}>{fmt$(k.sales)}</span>
            </div>
          ))}
        </div>
        <div className="chart-card">
          <h3 style={{ color: "var(--neg)" }}>Bottom 5 Keywords (by ROAS)</h3>
          <p className="sub">Keywords needing attention</p>
          {worst.map((k, i) => (
            <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "8px 0", borderBottom: "1px solid rgba(14,31,45,0.06)" }}>
              <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k.keyword} <span style={{ color: "var(--muted)", fontSize: 11 }}>({k.matchType})</span></span>
              <span style={{ color: "var(--neg)", fontWeight: 600, minWidth: 60, textAlign: "right" }}>{k.roas}x</span>
              <span style={{ color: "var(--muted)", minWidth: 60, textAlign: "right" }}>{fmt$(k.spend)} spent</span>
            </div>
          ))}
        </div>
      </div>

      {/* Full table */}
      <div className="table-card">
        <table>
          <thead>
            <tr>
              <TH label="Keyword" k="keyword" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="Match" k="matchType" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="Campaign" k="campaignName" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="Spend" k="spend" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="Sales" k="sales" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="ACOS" k="acos" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="ROAS" k="roas" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="Impr" k="impressions" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="Clicks" k="clicks" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="CPC" k="cpc" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="CTR" k="ctr" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="CVR" k="cvr" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
              <TH label="Orders" k="orders" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            </tr>
          </thead>
          <tbody>
            {data.map((k, i) => (
              <tr key={i}>
                <td style={{ maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis" }}>{k.keyword}</td>
                <td><MatchBadge type={k.matchType} /></td>
                <td style={{ maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", color: "var(--muted)" }}>{k.campaignName}</td>
                <td>{fmt$(k.spend)}</td>
                <td className="teal">{fmt$(k.sales)}</td>
                <td className={k.acos <= 30 ? "pos" : "neg"}>{k.acos}%</td>
                <td className={k.roas >= 3 ? "pos" : ""}>{k.roas}x</td>
                <td>{(k.impressions || 0).toLocaleString()}</td>
                <td>{k.clicks}</td>
                <td>${k.cpc}</td>
                <td>{k.ctr}%</td>
                <td>{k.cvr}%</td>
                <td>{k.orders}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}


// ── Search Terms Tab ──────────────────────────────────

function SearchTermsTab({ data, onSort, sortKey, sortDir }) {
  if (!data.length) {
    return <div className="chart-card" style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>No search term data yet.</div>;
  }

  return (
    <div className="table-card">
      <table>
        <thead>
          <tr>
            <TH label="Search Term" k="searchTerm" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Keyword" k="keyword" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Match" k="matchType" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Campaign" k="campaignName" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Spend" k="spend" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Sales" k="sales" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="ACOS" k="acos" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="ROAS" k="roas" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Impr" k="impressions" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="Clicks" k="clicks" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="CPC" k="cpc" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
            <TH label="CTR" k="ctr" onSort={onSort} sortKey={sortKey} sortDir={sortDir} />
          </tr>
        </thead>
        <tbody>
          {data.map((t, i) => (
            <tr key={i}>
              <td style={{ maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" }}>{t.searchTerm}</td>
              <td style={{ maxWidth: 160, overflow: "hidden", textOverflow: "ellipsis", color: "var(--muted)" }}>{t.keyword}</td>
              <td><MatchBadge type={t.matchType} /></td>
              <td style={{ maxWidth: 140, overflow: "hidden", textOverflow: "ellipsis", color: "var(--muted)" }}>{t.campaignName}</td>
              <td>{fmt$(t.spend)}</td>
              <td className="teal">{fmt$(t.sales)}</td>
              <td className={t.acos <= 30 ? "pos" : "neg"}>{t.acos}%</td>
              <td className={t.roas >= 3 ? "pos" : ""}>{t.roas}x</td>
              <td>{(t.impressions || 0).toLocaleString()}</td>
              <td>{t.clicks}</td>
              <td>${t.cpc}</td>
              <td>{t.ctr}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


// ── Negative Keywords Tab ─────────────────────────────

function NegativeKeywordsTab({ data }) {
  if (!data.length) {
    return <div className="chart-card" style={{ textAlign: "center", padding: 40, color: "var(--muted)" }}>No negative keywords found.</div>;
  }

  return (
    <div className="table-card">
      <table>
        <thead>
          <tr>
            <th>Keyword</th>
            <th>Match Type</th>
            <th>Campaign</th>
            <th>Ad Group</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {data.map((k, i) => (
            <tr key={i}>
              <td>{k.keyword}</td>
              <td><MatchBadge type={k.matchType} /></td>
              <td style={{ color: "var(--muted)" }}>{k.campaignName}</td>
              <td style={{ color: "var(--muted)" }}>{k.adGroupName || "—"}</td>
              <td><StatusBadge status={k.status} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}


// ── Shared Components ─────────────────────────────────

function TH({ label, k, onSort, sortKey, sortDir }) {
  return (
    <th onClick={() => onSort(k)} style={sortKey === k ? { color: "var(--teal-dark)" } : {}}>
      {label} {sortKey === k ? (sortDir === "desc" ? "▼" : "▲") : ""}
    </th>
  );
}

function StatusBadge({ status }) {
  const s = (status || "").toUpperCase();
  const color = s === "ENABLED" || s === "ACTIVE" ? "var(--teal)"
    : s === "PAUSED" ? "var(--gold)"
    : "var(--muted)";
  return (
    <span style={{ color, fontSize: 11, fontWeight: 600, letterSpacing: 0.5 }}>
      {s || "—"}
    </span>
  );
}

function MatchBadge({ type }) {
  const t = (type || "").toUpperCase();
  const colors = {
    "EXACT": "#2ECFAA",
    "PHRASE": "#F5B731",
    "BROAD": "#7BAED0",
    "NEGATIVE_EXACT": "#D03030",
    "NEGATIVE_PHRASE": "#D03030",
  };
  return (
    <span style={{
      color: colors[t] || "var(--muted)",
      fontSize: 11,
      fontWeight: 600,
      background: `${colors[t] || "var(--muted)"}15`,
      padding: "2px 8px",
      borderRadius: 4,
    }}>
      {t || "—"}
    </span>
  );
}


// ── Conversion Funnel Tab ──────────────────────────────
// Colors matching the mockup design
const C = {
  b1: "#1B4F8A", b2: "#2E6FBB", b3: "#5B9FD4",
  o2: "#E8821E", t2: "#1AA392",
  grn: "#22c55e", amb: "#f59e0b", red: "#ef4444",
  brd: "#1a2f4a", sub: "#5b7fa0", dim: "#374f66",
  card: "#122138", card2: "#0f1d33",
};

function fN(v) { return v == null ? "—" : Number(v).toLocaleString(); }
function fPct(v) { return v == null ? "—" : `${Number(v).toFixed(1)}%`; }
function f$(v) { return v == null ? "—" : `$${Number(v).toFixed(2)}`; }

function delta(ty, ly) {
  if (!ly || ly === 0) return null;
  return ((ty - ly) / ly * 100).toFixed(1);
}

function DeltaBadge({ ty, ly, invert = false }) {
  const d = delta(ty, ly);
  if (d === null) return <span style={{ color: C.sub, fontSize: 11 }}>—</span>;
  const pos = invert ? Number(d) < 0 : Number(d) >= 0;
  return (
    <span style={{
      color: pos ? C.grn : C.red,
      fontSize: 11,
      fontWeight: 600,
      background: pos ? `${C.grn}18` : `${C.red}18`,
      padding: "1px 6px",
      borderRadius: 4,
    }}>
      {Number(d) >= 0 ? "+" : ""}{d}%
    </span>
  );
}

function SparkLine({ data, key1, color = C.o2, h = 32 }) {
  if (!data || data.length < 2) return <svg width="100%" height={h}><text x="50%" y="50%" textAnchor="middle" fill={C.sub} fontSize="10">no data</text></svg>;
  const vals = data.map(d => d[key1] ?? 0);
  const mx = Math.max(...vals, 1);
  const mn = Math.min(...vals);
  const range = mx - mn || 1;
  const W = 120, H = h;
  const pts = vals.map((v, i) => `${(i / (vals.length - 1)) * W},${H - ((v - mn) / range) * (H - 4) - 2}`).join(" ");
  return (
    <svg width="100%" height={H} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.8" strokeLinejoin="round" />
    </svg>
  );
}

function FunnelStageBar({ label, value, maxVal, color, pct, showPct }) {
  const barW = maxVal > 0 ? Math.max(4, (value / maxVal) * 100) : 0;
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
        <span style={{ fontSize: 12, color: C.sub }}>{label}</span>
        <span style={{ fontSize: 13, fontWeight: 700, color: "#e8edf2" }}>{fN(value)}</span>
      </div>
      <div style={{ position: "relative", height: 22, background: `${color}20`, borderRadius: 4, overflow: "hidden" }}>
        <div style={{ width: `${barW}%`, height: "100%", background: `linear-gradient(90deg,${color}cc,${color})`, borderRadius: 4, transition: "width 0.5s" }} />
      </div>
      {showPct && pct != null && (
        <div style={{ fontSize: 11, color: C.sub, marginTop: 3, textAlign: "right" }}>{fPct(pct)} step-through</div>
      )}
    </div>
  );
}

function FunnelTab({ funnelData, days }) {
  if (!funnelData) {
    return <div className="chart-card" style={{ textAlign: "center", padding: 60, color: C.sub }}>Loading funnel data…</div>;
  }

  const ty = funnelData.ty || {};
  const ly = funnelData.ly || {};
  const daily = funnelData.daily || [];

  // Stage values
  const stages = [
    { label: "Impressions", key: "impressions", color: C.b3, icon: "👁" },
    { label: "Clicks",      key: "clicks",      color: C.b2, icon: "🖱" },
    { label: "Sessions",    key: "sessions",    color: C.b1, icon: "🌐" },
    { label: "Add to Cart", key: "atc",         color: C.o2, icon: "🛒" },
    { label: "Orders",      key: "orders",      color: C.t2, icon: "✅" },
  ];
  const maxStage = Math.max(...stages.map(s => ty[s.key] || 0), 1);

  // Step-through rates
  const stepRates = [
    null,
    ty.ctr,
    ty.clickToSession,
    ty.atcRate,
    ty.atcConv,
  ];

  // Bottleneck: find the worst step-through rate
  const rates = [ty.ctr, ty.clickToSession, ty.atcRate, ty.atcConv].filter(v => v != null && v > 0);
  const benchmarks = [2.0, 80, 12, 35]; // Amazon avg: CTR 2%, Click→Sess 80%, ATC 12%, ATC→Order 35%
  let bottleneckIdx = -1;
  let worstRatio = Infinity;
  [ty.ctr, ty.clickToSession, ty.atcRate, ty.atcConv].forEach((v, i) => {
    if (v != null && v > 0) {
      const ratio = v / benchmarks[i];
      if (ratio < worstRatio) { worstRatio = ratio; bottleneckIdx = i; }
    }
  });
  const bottleneckLabels = ["CTR (Ads→Clicks)", "Click→Session rate", "Add-to-Cart rate", "Cart→Order conversion"];
  const bottleneckName = bottleneckIdx >= 0 ? bottleneckLabels[bottleneckIdx] : null;

  // Insights bullets
  const insights = [];
  if (ty.ctr != null && ty.ctr < 2) insights.push({ color: C.amb, text: `CTR is ${fPct(ty.ctr)} — below 2% Amazon avg. Test new ad creative or tighten targeting.` });
  if (ty.sessionConv != null && ty.sessionConv < 10) insights.push({ color: C.red, text: `Session conv. ${fPct(ty.sessionConv)} is low. Review product page, images, and pricing.` });
  if (ty.atcRate != null && ty.atcRate < 8) insights.push({ color: C.amb, text: `ATC rate ${fPct(ty.atcRate)} is below avg. Check competitors' pricing and A+ content.` });
  if (insights.length === 0) insights.push({ color: C.grn, text: "All funnel stages are performing at or above Amazon average benchmarks." });

  const cardStyle = { background: C.card, border: `1px solid ${C.brd}`, borderRadius: 8, padding: "16px 20px" };
  const secHdr = { fontSize: 11, fontWeight: 700, color: C.sub, letterSpacing: "0.08em", textTransform: "uppercase", marginBottom: 12 };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>

      {/* ── KPI Row ── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12 }}>
        {stages.map((s, i) => (
          <div key={s.key} style={{ ...cardStyle, borderTop: `3px solid ${s.color}` }}>
            <div style={{ fontSize: 11, color: C.sub, marginBottom: 6 }}>{s.icon} {s.label}</div>
            <div style={{ fontSize: 22, fontWeight: 800, color: "#e8edf2", lineHeight: 1.1 }}>
              {fN(ty[s.key])}
            </div>
            <div style={{ marginTop: 6 }}>
              <DeltaBadge ty={ty[s.key]} ly={ly[s.key]} />
              <span style={{ fontSize: 10, color: C.sub, marginLeft: 6 }}>vs LY</span>
            </div>
            {i > 0 && stepRates[i] != null && (
              <div style={{ fontSize: 11, color: s.color, marginTop: 4, fontWeight: 600 }}>
                {fPct(stepRates[i])} step-through
              </div>
            )}
          </div>
        ))}
      </div>

      {/* ── Main Grid: Funnel Bars + Stage Metrics Table ── */}
      <div style={{ display: "grid", gridTemplateColumns: "300px 1fr", gap: 16 }}>

        {/* Visual Funnel */}
        <div style={cardStyle}>
          <div style={secHdr}>Funnel Stages</div>
          {stages.map((s, i) => (
            <FunnelStageBar
              key={s.key}
              label={s.label}
              value={ty[s.key] || 0}
              maxVal={maxStage}
              color={s.color}
              pct={i > 0 ? stepRates[i] : null}
              showPct={i > 0}
            />
          ))}

          {/* LY outline comparison */}
          <div style={{ marginTop: 16, paddingTop: 12, borderTop: `1px solid ${C.brd}` }}>
            <div style={{ fontSize: 11, color: C.sub, marginBottom: 8 }}>Last Year Comparison</div>
            {stages.map(s => (
              <div key={s.key} style={{ display: "flex", justifyContent: "space-between", marginBottom: 5 }}>
                <span style={{ fontSize: 11, color: C.dim }}>{s.label}</span>
                <span style={{ fontSize: 12, color: C.sub }}>{fN(ly[s.key])}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Stage Metrics Table + Benchmarks */}
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div style={cardStyle}>
            <div style={secHdr}>Stage Metrics · TY vs LY</div>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: `1px solid ${C.brd}` }}>
                  <th style={{ textAlign: "left", color: C.sub, fontWeight: 600, padding: "4px 8px 8px 0", fontSize: 11 }}>Metric</th>
                  <th style={{ textAlign: "right", color: C.sub, fontWeight: 600, padding: "4px 8px 8px", fontSize: 11 }}>This Year</th>
                  <th style={{ textAlign: "right", color: C.sub, fontWeight: 600, padding: "4px 8px 8px", fontSize: 11 }}>Last Year</th>
                  <th style={{ textAlign: "right", color: C.sub, fontWeight: 600, padding: "4px 0 8px", fontSize: 11 }}>Δ</th>
                </tr>
              </thead>
              <tbody>
                {[
                  { label: "📣 Ad Layer", header: true },
                  { label: "Impressions",   ty: ty.impressions,  ly: ly.impressions,  fmt: fN },
                  { label: "Clicks",        ty: ty.clicks,       ly: ly.clicks,       fmt: fN },
                  { label: "CTR",           ty: ty.ctr,          ly: ly.ctr,          fmt: fPct, invert: false },
                  { label: "CPC",           ty: ty.cpc,          ly: ly.cpc,          fmt: f$,   invert: true },
                  { label: "Ad Spend",      ty: ty.spend,        ly: ly.spend,        fmt: f$,   invert: true },
                  { label: "🌐 Traffic Layer", header: true },
                  { label: "Sessions",      ty: ty.sessions,     ly: ly.sessions,     fmt: fN },
                  { label: "Glance Views",  ty: ty.glanceViews,  ly: ly.glanceViews,  fmt: fN },
                  { label: "Click→Session", ty: ty.clickToSession, ly: ly.clickToSession, fmt: fPct },
                  { label: "🛍 Conversion Layer", header: true },
                  { label: "Add to Cart",   ty: ty.atc,          ly: ly.atc,          fmt: fN },
                  { label: "ATC Rate",      ty: ty.atcRate,      ly: ly.atcRate,      fmt: fPct },
                  { label: "Orders",        ty: ty.orders,       ly: ly.orders,       fmt: fN },
                  { label: "Session Conv%", ty: ty.sessionConv,  ly: ly.sessionConv,  fmt: fPct },
                  { label: "ATC Conv%",     ty: ty.atcConv,      ly: ly.atcConv,      fmt: fPct },
                ].map((row, i) => row.header ? (
                  <tr key={i}>
                    <td colSpan={4} style={{ padding: "10px 0 4px", fontSize: 11, fontWeight: 700, color: C.sub, letterSpacing: "0.05em" }}>{row.label}</td>
                  </tr>
                ) : (
                  <tr key={i} style={{ borderBottom: `1px solid ${C.brd}40` }}>
                    <td style={{ padding: "6px 8px 6px 0", color: "#c5cfd8" }}>{row.label}</td>
                    <td style={{ textAlign: "right", padding: "6px 8px", fontWeight: 600, color: "#e8edf2" }}>{row.fmt(row.ty)}</td>
                    <td style={{ textAlign: "right", padding: "6px 8px", color: C.sub }}>{row.fmt(row.ly)}</td>
                    <td style={{ textAlign: "right", padding: "6px 0" }}><DeltaBadge ty={row.ty} ly={row.ly} invert={row.invert} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Benchmark Cards */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
            {[
              { label: "Session Conv%",  val: ty.sessionConv,  bench: 12,  fmt: fPct, desc: "Amazon avg ~12%" },
              { label: "CTR",            val: ty.ctr,          bench: 2,   fmt: fPct, desc: "Amazon avg ~2%" },
              { label: "ATC Rate",       val: ty.atcRate,      bench: 12,  fmt: fPct, desc: "Amazon avg ~12%" },
            ].map((b, i) => {
              const good = b.val != null && b.val >= b.bench;
              return (
                <div key={i} style={{ ...cardStyle, borderLeft: `3px solid ${good ? C.grn : C.amb}`, padding: "12px 14px" }}>
                  <div style={{ fontSize: 11, color: C.sub }}>{b.label}</div>
                  <div style={{ fontSize: 20, fontWeight: 700, color: good ? C.grn : C.amb, margin: "4px 0 2px" }}>{fPct(b.val)}</div>
                  <div style={{ fontSize: 10, color: C.dim }}>{b.desc}</div>
                  <div style={{ marginTop: 6, height: 4, background: `${C.brd}`, borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ width: `${Math.min(100, (b.val || 0) / (b.bench * 2) * 100)}%`, height: "100%", background: good ? C.grn : C.amb, transition: "width 0.5s" }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* ── Sparklines Row ── */}
      <div style={{ ...cardStyle, padding: "16px 20px" }}>
        <div style={secHdr}>Stage Trend · {days}D</div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 16 }}>
          {[
            { label: "Impressions", k: "impressions", color: C.b3 },
            { label: "CTR %",       k: "ctr",         color: C.b2 },
            { label: "Sessions",    k: "sessions",    color: C.b1 },
            { label: "Add to Cart", k: "atc",         color: C.o2 },
            { label: "Orders",      k: "orders",      color: C.t2 },
            { label: "Conv %",      k: "convPct",     color: "#8b5cf6" },
          ].map(s => (
            <div key={s.k} style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <div style={{ fontSize: 11, color: C.sub }}>{s.label}</div>
              <SparkLine data={daily} key1={s.k} color={s.color} h={36} />
              <div style={{ fontSize: 12, fontWeight: 600, color: "#e8edf2" }}>
                {s.k === "ctr" || s.k === "convPct" ? fPct(ty[s.k === "convPct" ? "sessionConv" : s.k]) : fN(ty[s.k])}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Bottom: Insights + Trend Chart ── */}
      <div style={{ display: "grid", gridTemplateColumns: "280px 1fr", gap: 16 }}>

        {/* Insights Panel */}
        <div style={cardStyle}>
          <div style={secHdr}>Actionable Insights</div>
          {bottleneckName && (
            <div style={{ background: `${C.amb}18`, border: `1px solid ${C.amb}40`, borderRadius: 6, padding: "10px 12px", marginBottom: 14 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: C.amb, marginBottom: 4 }}>⚠ Bottleneck Detected</div>
              <div style={{ fontSize: 12, color: "#c5cfd8" }}>{bottleneckName} is the weakest funnel stage</div>
            </div>
          )}
          {insights.map((ins, i) => (
            <div key={i} style={{ display: "flex", gap: 10, marginBottom: 12, alignItems: "flex-start" }}>
              <div style={{ width: 8, height: 8, borderRadius: "50%", background: ins.color, marginTop: 4, flexShrink: 0 }} />
              <div style={{ fontSize: 12, color: "#c5cfd8", lineHeight: 1.5 }}>{ins.text}</div>
            </div>
          ))}

          {/* Spend efficiency */}
          <div style={{ marginTop: 16, paddingTop: 12, borderTop: `1px solid ${C.brd}` }}>
            <div style={{ fontSize: 11, color: C.sub, marginBottom: 8 }}>Spend Efficiency</div>
            {[
              { label: "Cost/Session",  val: ty.sessions > 0 ? f$(ty.spend / ty.sessions) : "—" },
              { label: "Cost/Order",    val: ty.orders > 0 ? f$(ty.spend / ty.orders) : "—" },
              { label: "ROAS",          val: `${ty.roas || "—"}x` },
            ].map((r, i) => (
              <div key={i} style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                <span style={{ fontSize: 12, color: C.dim }}>{r.label}</span>
                <span style={{ fontSize: 12, fontWeight: 600, color: "#e8edf2" }}>{r.val}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Trend Chart: Clicks bar + CTR line */}
        <div style={cardStyle}>
          <div style={{ marginBottom: 12 }}>
            <div style={secHdr}>Ad Clicks &amp; CTR Trend</div>
          </div>
          {daily.length > 1 ? (
            <ResponsiveContainer width="100%" height={200}>
              <ComposedChart data={daily} margin={{ top: 4, right: 40, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={`${C.brd}80`} />
                <XAxis dataKey="date" tick={{ fill: C.sub, fontSize: 10 }} tickFormatter={d => d.slice(5)} />
                <YAxis yAxisId="left" tick={{ fill: C.sub, fontSize: 10 }} />
                <YAxis yAxisId="right" orientation="right" tick={{ fill: C.sub, fontSize: 10 }} tickFormatter={v => `${v}%`} />
                <Tooltip contentStyle={TOOLTIP_STYLE}
                  formatter={(v, name) => [name === "CTR %" ? `${v}%` : v.toLocaleString(), name]} />
                <Bar yAxisId="left" dataKey="clicks" fill={C.b2} opacity={0.75} radius={[3,3,0,0]} name="Clicks" />
                <Line yAxisId="right" type="monotone" dataKey="ctr" stroke={C.o2} strokeWidth={2} dot={false} name="CTR %" />
              </ComposedChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ textAlign: "center", color: C.sub, padding: 40 }}>No daily data yet — run a full sync to populate</div>
          )}
        </div>
      </div>

    </div>
  );
}
