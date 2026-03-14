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
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState("spend");
  const [sortDir, setSortDir] = useState("desc");

  useEffect(() => {
    setLoading(true);
    const h = filters;
    const promises = [api.adsSummary(days, h)];

    if (tab === "overview") {
      promises.push(api.adsDaily(days, h));
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
