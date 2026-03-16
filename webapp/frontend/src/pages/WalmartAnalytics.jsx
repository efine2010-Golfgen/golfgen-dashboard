import { useState, useEffect, useRef } from "react";
import { api } from "../lib/api";
import { SG, DM, Card } from "./walmart/WalmartHelpers";
import { SalesPage } from "./walmart/WalmartSales";
import { InventoryPage } from "./walmart/WalmartInventory";
import { ScorecardPage } from "./walmart/WalmartScorecard";
import { EcommPage } from "./walmart/WalmartEcomm";
import { ForecastPage } from "./walmart/WalmartForecast";

export default function WalmartAnalytics({ filters = {} }) {
  const [page, setPage] = useState("sales");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState(null);
  const fileRef = useRef(null);

  const h = filters;

  useEffect(() => {
    setLoading(true);
    setError(null);
    api
      .walmartAnalytics(h)
      .then((d) => {
        setData(d);
        setError(null);
      })
      .catch((e) => {
        setData(null);
        setError(e.message);
      })
      .finally(() => setLoading(false));
  }, [filters.division, filters.customer]);

  const handleUpload = async () => {
    const file = fileRef.current?.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadResult(null);
    try {
      const res = await api.retailUpload(file);
      setUploadResult(res);
      // Reload data after successful upload
      const fresh = await api.walmartAnalytics(h);
      setData(fresh);
    } catch (e) {
      setUploadResult({ status: "error", error: e.message });
    }
    setUploading(false);
  };

  if (loading)
    return (
      <div style={{ padding: "24px", ...SG(12), color: "var(--txt3)" }}>
        Loading...
      </div>
    );

  const isDataAvailable = data?.dataAvailable === true;

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
            Walmart Analytics
          </h1>
          <p
            style={{
              ...SG(11),
              color: "var(--txt3)",
              margin: "2px 0 0",
            }}
          >
            Sales performance, inventory health, and order forecasting
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <input
            type="file"
            ref={fileRef}
            accept=".xlsx,.xls"
            style={{ display: "none" }}
            onChange={handleUpload}
          />
          <button
            onClick={() => fileRef.current?.click()}
            disabled={uploading}
            style={{
              ...SG(11, 600),
              background: "var(--acc1)",
              color: "#fff",
              border: "none",
              borderRadius: 6,
              padding: "6px 14px",
              cursor: "pointer",
              opacity: uploading ? 0.6 : 1,
            }}
          >
            {uploading ? "Uploading..." : "Upload Report"}
          </button>
          {uploadResult && (
            <span
              style={{
                ...SG(10),
                color:
                  uploadResult.status === "ok"
                    ? "#2ECFAA"
                    : "#f87171",
              }}
            >
              {uploadResult.status === "ok"
                ? `✓ ${uploadResult.rows_loaded} rows`
                : `✗ ${uploadResult.error || "Failed"}`}
            </span>
          )}
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
        {[
          { key: "sales", label: "SALES PERFORMANCE" },
          { key: "inventory", label: "INVENTORY HEALTH" },
          { key: "scorecard", label: "VENDOR SCORECARD" },
          { key: "ecomm", label: "ECOMMERCE" },
          { key: "forecast", label: "ORDER FORECAST" },
        ].map((t) => (
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
      {!isDataAvailable && (
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
            Upload Walmart Scintilla reports to populate this dashboard. Use
            the Upload Report button above to get started.
          </p>
        </Card>
      )}

      {/* ── Page content ── */}
      {page === "sales" && <SalesPage data={data} />}
      {page === "inventory" && <InventoryPage data={data} />}
      {page === "scorecard" && <ScorecardPage data={data} />}
      {page === "ecomm" && <EcommPage data={data} />}
      {page === "forecast" && <ForecastPage data={data} />}
    </div>
  );
}
