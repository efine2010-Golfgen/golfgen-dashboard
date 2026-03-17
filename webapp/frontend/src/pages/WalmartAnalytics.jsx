import { useState, useEffect } from "react";
import { api } from "../lib/api";
import { SG, DM, Card, fN } from "./walmart/WalmartHelpers";
import { SalesPage } from "./walmart/WalmartSales";
import { WalmartInventory as InventoryPage } from "./walmart/WalmartInventory";
import { WalmartScorecard as ScorecardPage } from "./walmart/WalmartScorecard";
import { WalmartEcomm as EcommPage } from "./walmart/WalmartEcomm";
import { WalmartForecast as ForecastPage } from "./walmart/WalmartForecast";
import { WalmartStoreAnalytics as StoreAnalyticsPage } from "./walmart/WalmartStoreAnalytics";

export default function WalmartAnalytics({ filters = {} }) {
  const [page, setPage] = useState("sales");
  const [availability, setAvailability] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploads, setUploads] = useState([]);
  const [showUploads, setShowUploads] = useState(false);

  const h = filters;

  // Check data availability + fetch recent uploads on mount
  useEffect(() => {
    setLoading(true);
    Promise.all([
      api.walmartAvailability(h).catch(() => null),
      api.retailSummary(h).catch(() => null),
    ]).then(([avail, summary]) => {
      setAvailability(avail);
      setUploads(summary?.uploads || []);
    }).finally(() => setLoading(false));
  }, [filters.division, filters.customer]);

  if (loading)
    return (
      <div style={{ padding: "24px", ...SG(12), color: "var(--txt3)" }}>
        Loading...
      </div>
    );

  const hasAnyData =
    availability?.hasItemData ||
    availability?.hasScorecardData ||
    availability?.hasStoreData ||
    availability?.hasEcommData ||
    availability?.hasForecastData;

  const latestWeek = availability?.latestWeek;

  const TABS = [
    { key: "sales", label: "SALES PERFORMANCE", title: "Sales Performance" },
    { key: "inventory", label: "INVENTORY HEALTH", title: "Inventory Health" },
    { key: "scorecard", label: "VENDOR SCORECARD", title: "Vendor Scorecard" },
    { key: "ecomm", label: "ECOMMERCE", title: "E-Commerce" },
    { key: "forecast", label: "ORDER FORECAST", title: "Order Forecast" },
    { key: "store-analytics", label: "STORE ANALYTICS", title: "Store Analytics" },
  ];
  const activeTab = TABS.find(t => t.key === page) || TABS[0];

  return (
    <div style={{ padding: "0 24px 40px" }}>
      {/* ── Header ── */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 16,
        }}
      >
        <div>
          <h1 style={{ ...DM(22), margin: 0, color: "var(--txt)" }}>
            {activeTab.title}
          </h1>
          <p
            style={{
              ...SG(11),
              color: "var(--txt3)",
              margin: "2px 0 0",
            }}
          >
            {latestWeek
              ? `Through Walmart Week ${latestWeek}`
              : "Walmart sell-through performance data"}
          </p>
        </div>
      </div>

      {/* ── Page nav ── */}
      <div
        style={{
          display: "flex",
          gap: 2,
          marginBottom: 16,
          borderBottom: "1px solid var(--brd)",
        }}
      >
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setPage(t.key)}
            style={{
              ...SG(11, page === t.key ? 700 : 500),
              background: "none",
              border: "none",
              cursor: "pointer",
              padding: "8px 14px",
              color:
                page === t.key
                  ? "var(--acc1)"
                  : "var(--txt3)",
              borderBottom:
                page === t.key
                  ? "2px solid var(--acc1)"
                  : "2px solid transparent",
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* ── No data state ── */}
      {!hasAnyData && (
        <Card
          style={{
            background: "rgba(165, 107, 225, 0.05)",
            border: "1px solid rgba(165, 107, 225, 0.2)",
            marginBottom: 16,
          }}
        >
          <div
            style={{
              ...DM(16),
              color: "var(--txt)",
              marginBottom: 8,
            }}
          >
            No Data Available
          </div>
          <p
            style={{
              ...SG(11),
              color: "var(--txt3)",
              margin: 0,
              lineHeight: "1.4",
            }}
          >
            Upload Walmart Scintilla reports to populate this dashboard.
          </p>
        </Card>
      )}

      {/* ── Page content — each tab fetches its own data ── */}
      {page === "sales" && <SalesPage filters={h} />}
      {page === "inventory" && <InventoryPage filters={h} />}
      {page === "scorecard" && <ScorecardPage filters={h} />}
      {page === "ecomm" && <EcommPage filters={h} />}
      {page === "forecast" && <ForecastPage filters={h} />}
      {page === "store-analytics" && <StoreAnalyticsPage filters={h} />}

      {/* ── Recent Uploads (bottom, collapsible) ── */}
      {uploads.length > 0 && (
        <div style={{ marginTop: 24 }}>
          <span
            onClick={() => setShowUploads(!showUploads)}
            style={{
              ...SG(8, 600),
              padding: "2px 8px",
              borderRadius: 3,
              cursor: "pointer",
              background: showUploads ? "rgba(46,207,170,0.15)" : "rgba(255,255,255,0.04)",
              color: showUploads ? "#2ecf99" : "var(--txt3)",
              border: `1px solid ${showUploads ? "rgba(46,207,170,0.3)" : "rgba(255,255,255,0.08)"}`,
              userSelect: "none",
              display: "inline-block",
              marginBottom: showUploads ? 8 : 0,
            }}
          >
            {showUploads ? "Hide Uploads" : "Recent Uploads"}
          </span>
          {showUploads && (
            <Card>
              <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>
                Recent Uploads
              </div>
              <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                    <th style={{ textAlign: "left", padding: "4px 8px", color: "var(--txt3)" }}>File</th>
                    <th style={{ textAlign: "left", padding: "4px 8px", color: "var(--txt3)" }}>Type</th>
                    <th style={{ textAlign: "right", padding: "4px 8px", color: "var(--txt3)" }}>Rows</th>
                    <th style={{ textAlign: "left", padding: "4px 8px", color: "var(--txt3)" }}>Status</th>
                    <th style={{ textAlign: "left", padding: "4px 8px", color: "var(--txt3)" }}>Time</th>
                  </tr>
                </thead>
                <tbody>
                  {uploads.map((u, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                      <td style={{ padding: "4px 8px", color: "var(--txt)" }}>{u.filename}</td>
                      <td style={{ padding: "4px 8px", color: "var(--txt2)" }}>{u.reportType}</td>
                      <td style={{ padding: "4px 8px", color: "var(--txt)", textAlign: "right" }}>{fN(u.rowsLoaded)}</td>
                      <td style={{ padding: "4px 8px", color: u.status === "SUCCESS" ? "#2ECFAA" : "#f87171" }}>{u.status}</td>
                      <td style={{ padding: "4px 8px", color: "var(--txt3)" }}>{u.uploadedAt}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Card>
          )}
        </div>
      )}
    </div>
  );
}
