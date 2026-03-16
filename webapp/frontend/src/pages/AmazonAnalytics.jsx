import { useState } from "react";
import Sales from "./Sales";
import Products from "./Products";
import Profitability from "./Profitability";
import Inventory from "./Inventory";
import FBAShipments from "./FBAShipments";
import Advertising from "./Advertising";
import ItemPlanning from "./ItemPlanning";

const TABS = [
  { key: "exec-summary", label: "EXEC SUMMARY" },
  { key: "item-performance", label: "ITEM PERFORMANCE" },
  { key: "profitability", label: "PROFITABILITY" },
  { key: "fba-inventory", label: "FBA INVENTORY" },
  { key: "fba-shipments", label: "FBA SHIPMENTS" },
  { key: "advertising", label: "ADVERTISING" },
  { key: "forecasting", label: "FORECASTING" },
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

export default function AmazonAnalytics({ filters = {} }) {
  const [page, setPage] = useState("exec-summary");

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
      </div>

      {/* ── Tab content ── */}
      {page === "exec-summary" && <Sales filters={filters} />}
      {page === "item-performance" && <Products filters={filters} />}
      {page === "profitability" && <Profitability filters={filters} />}
      {page === "fba-inventory" && <Inventory filters={filters} />}
      {page === "fba-shipments" && <FBAShipments filters={filters} />}
      {page === "advertising" && <Advertising filters={filters} />}
      {page === "forecasting" && <ItemPlanning />}
    </div>
  );
}
