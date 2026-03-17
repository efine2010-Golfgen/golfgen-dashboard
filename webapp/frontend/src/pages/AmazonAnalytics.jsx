import { useState } from "react";
import Sales from "./Sales";
import Products from "./Products";
import Profitability from "./Profitability";
import Inventory from "./Inventory";
import FBAShipments from "./FBAShipments";
import Advertising from "./Advertising";
import ItemPlanning from "./ItemPlanning";
import ItemMaster from "./ItemMaster";

const TABS = [
  { key: "exec-summary", label: "EXEC SUMMARY" },
  { key: "item-performance", label: "ITEM PERFORMANCE" },
  { key: "profitability", label: "PROFITABILITY" },
  { key: "fba-inventory", label: "FBA INVENTORY" },
  { key: "fba-shipments", label: "FBA SHIPMENTS" },
  { key: "advertising", label: "ADVERTISING" },
  { key: "forecasting", label: "FORECASTING" },
  { key: "item-master", label: "ITEM MASTER" },
];

const SG = (sz = 12, wt = 400) => ({
  fontFamily: "'Space Grotesk',monospace",
  fontSize: sz,
  fontWeight: wt,
});

const DM = (sz = 22) => ({
  fontFamily: "'DM Serif Display',Georgia,serif",
  fontSize: sz,
});

export default function AmazonAnalytics({ filters = {}, onMarketplaceChange }) {
  const [page, setPage] = useState("exec-summary");
  const mp = filters.marketplace || "US";

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
            Amazon Analytics
          </h1>
          <p style={{ ...SG(11), color: "var(--txt3)", margin: "2px 0 0" }}>
            Sales, profitability, inventory, advertising & forecasting
          </p>
        </div>
      </div>

      {/* ── Page nav with marketplace toggle ── */}
      <div
        style={{
          display: "flex",
          gap: 2,
          marginBottom: 16,
          borderBottom: "1px solid var(--brd)",
          alignItems: "flex-end",
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
              color: page === t.key ? "var(--acc1)" : "var(--txt3)",
              borderBottom:
                page === t.key
                  ? "2px solid var(--acc1)"
                  : "2px solid transparent",
            }}
          >
            {t.label}
          </button>
        ))}
        {/* Marketplace toggle — right side */}
        {onMarketplaceChange && (
          <div style={{ display: "flex", alignItems: "center", gap: 0, marginLeft: "auto", marginBottom: 4 }}>
            <button
              onClick={() => onMarketplaceChange("US")}
              style={{
                height: 28,
                padding: "0 10px",
                borderRadius: "6px 0 0 6px",
                border: "1px solid var(--brd)",
                borderRight: "none",
                background: mp === "US" ? "var(--acc1)" : "var(--ibg)",
                color: mp === "US" ? "#fff" : "var(--txt3)",
                fontSize: 10,
                fontWeight: 700,
                fontFamily: "'Space Grotesk',monospace",
                cursor: "pointer",
                transition: "all .2s",
                letterSpacing: ".03em",
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
              }}
            >
              🇺🇸 US
            </button>
            <button
              onClick={() => onMarketplaceChange("CA")}
              style={{
                height: 28,
                padding: "0 10px",
                borderRadius: "0 6px 6px 0",
                border: "1px solid var(--brd)",
                background: mp === "CA" ? "var(--acc1)" : "var(--ibg)",
                color: mp === "CA" ? "#fff" : "var(--txt3)",
                fontSize: 10,
                fontWeight: 700,
                fontFamily: "'Space Grotesk',monospace",
                cursor: "pointer",
                transition: "all .2s",
                letterSpacing: ".03em",
                display: "inline-flex",
                alignItems: "center",
                gap: 4,
              }}
            >
              🇨🇦 CA
            </button>
          </div>
        )}
      </div>

      {/* ── Tab content ── */}
      {page === "exec-summary" && <Sales filters={filters} />}
      {page === "item-performance" && <Products filters={filters} />}
      {page === "profitability" && <Profitability filters={filters} />}
      {page === "fba-inventory" && <Inventory filters={filters} />}
      {page === "fba-shipments" && <FBAShipments filters={filters} />}
      {page === "advertising" && <Advertising filters={filters} />}
      {page === "forecasting" && <ItemPlanning />}
      {page === "item-master" && <ItemMaster />}
    </div>
  );
}
