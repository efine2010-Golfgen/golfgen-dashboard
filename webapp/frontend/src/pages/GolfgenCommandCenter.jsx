import { useState } from "react";
import GolfGenInventory from "./GolfGenInventory";
import SupplyChain from "./SupplyChain";

const TABS = [
  { key: "exec-summary", label: "EXEC SUMMARY" },
  { key: "golfgen-inventory", label: "GOLFGEN INVENTORY" },
  { key: "logistics", label: "LOGISTICS" },
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

export default function GolfgenCommandCenter({ filters = {} }) {
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
            GolfGen Command Center
          </h1>
          <p style={{ ...SG(11), color: "var(--txt3)", margin: "2px 0 0" }}>
            Inventory, supply chain & logistics overview
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
      {page === "exec-summary" && (
        <div
          style={{
            padding: "60px 40px",
            textAlign: "center",
            color: "var(--txt3)",
            border: "1px dashed var(--brd)",
            borderRadius: 8,
            background: "var(--card)",
          }}
        >
          <h2 style={{ ...DM(20), color: "var(--txt2)", margin: "0 0 8px" }}>
            GolfGen Exec Summary
          </h2>
          <p style={{ ...SG(12), color: "var(--txt3)" }}>
            Cross-channel executive summary coming soon — will aggregate data
            across all GolfGen channels.
          </p>
        </div>
      )}
      {page === "golfgen-inventory" && <GolfGenInventory filters={filters} />}
      {page === "logistics" && <SupplyChain />}
    </div>
  );
}
