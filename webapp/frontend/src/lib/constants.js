/**
 * Shared constants used across multiple pages.
 * Import from here instead of duplicating in each page file.
 */

// ── Chart color palette (primary 10-color set used by most pages) ──
export const CHART_COLORS = [
  "#2ECFAA", "#3E658C", "#E87830", "#F5B731", "#7BAED0",
  "#22A387", "#D03030", "#8B5CF6", "#94a3b8", "#8892b0",
];

// ── Recharts tooltip style (shared across all chart pages) ──
export const TOOLTIP_STYLE = {
  background: "#fff",
  border: "1px solid rgba(14,31,45,0.1)",
  borderRadius: 8,
  color: "#2A3D50",
  boxShadow: "0 4px 12px rgba(14,31,45,0.1)",
};

// ── Color badge styles (ItemMaster, Warehouse) ──
export const COLOR_BADGES = {
  Green:  { bg: "#dcfce7", color: "#166534" },
  Blue:   { bg: "#dbeafe", color: "#1e40af" },
  Red:    { bg: "#fee2e2", color: "#991b1b" },
  Orange: { bg: "#ffedd5", color: "#9a3412" },
  Black:  { bg: "#e5e7eb", color: "#1f2937" },
  "":     { bg: "#f3f4f6", color: "#6b7280" },
};

// ── Color swatch hex values (Products page) ──
export const COLOR_SWATCH = {
  Green:  "#16a34a",
  Blue:   "#2563eb",
  Red:    "#dc2626",
  Orange: "#ea580c",
  Black:  "#1f2937",
};

// ── Recharts axis tick style ──
export const AXIS_TICK = { fill: "#6B8090", fontSize: 11 };
