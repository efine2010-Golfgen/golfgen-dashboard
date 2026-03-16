import { useRef, useEffect, useCallback } from "react";

// ═════════════════════════════════════════════════════════════════════════════
// TYPOGRAPHY HELPERS
// ═════════════════════════════════════════════════════════════════════════════

export const SG = (sz, wt = 600, c) => ({
  fontFamily: "'Space Grotesk', monospace",
  fontSize: sz,
  fontWeight: wt,
  ...(c ? { color: c } : {}),
});

export const DM = (sz, c) => ({
  fontFamily: "'DM Serif Display', Georgia, serif",
  fontSize: sz,
  ...(c ? { color: c } : {}),
});

// ═════════════════════════════════════════════════════════════════════════════
// CARD COMPONENT
// ═════════════════════════════════════════════════════════════════════════════

export const Card = ({ children, style }) => (
  <div
    style={{
      background: "var(--card)",
      borderRadius: 12,
      border: "1px solid var(--brd)",
      padding: "18px 20px",
      ...style,
    }}
  >
    {children}
  </div>
);

// ═════════════════════════════════════════════════════════════════════════════
// FORMATTING HELPERS
// ═════════════════════════════════════════════════════════════════════════════

export const fN = (v) =>
  v == null ? "0" : Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });

export const f$ = (v) =>
  v == null ? "$0" : "$" + Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });

export const fPct = (v) =>
  v == null ? "—" : (Number(v) * 100).toFixed(1) + "%";

export const delta = (ty, ly) => {
  if (!ly || ly === 0) return null;
  return (((ty - ly) / Math.abs(ly)) * 100).toFixed(1);
};

// ═════════════════════════════════════════════════════════════════════════════
// DELTA BADGE COMPONENT
// ═════════════════════════════════════════════════════════════════════════════

export const DeltaBadge = ({ ty, ly, invert }) => {
  const d = delta(ty, ly);
  if (d === null) return <span style={{ ...SG(10), color: "var(--txt3)" }}>—</span>;
  const pos = parseFloat(d) >= 0;
  const good = invert ? !pos : pos;
  return (
    <span style={{ ...SG(10, 600), color: good ? "#2ECFAA" : "#f87171" }}>
      {pos ? "▲" : "▼"} {Math.abs(d)}%
    </span>
  );
};

// ═════════════════════════════════════════════════════════════════════════════
// COLOR PALETTE
// ═════════════════════════════════════════════════════════════════════════════

export const COLORS = {
  teal: "#2ECFAA",
  orange: "#E87830",
  blue: "#7BAED0",
  purple: "#A26BE1",
  red: "#f87171",
  yellow: "#F5B731",
};

// ═════════════════════════════════════════════════════════════════════════════
// TIME PERIODS
// ═════════════════════════════════════════════════════════════════════════════

export const PERIODS = ["lw", "l4w", "l13w", "l26w", "l52w"];

export const PERIOD_LABELS = {
  lw: "Last Week",
  l4w: "L4W",
  l13w: "L13W",
  l26w: "L26W",
  l52w: "L52W",
};

// ═════════════════════════════════════════════════════════════════════════════
// KPI CARD COMPONENT
// ═════════════════════════════════════════════════════════════════════════════

export function KPICard({ label, value, delta: d, color = COLORS.teal }) {
  return (
    <div
      style={{
        background: "var(--card)",
        borderRadius: 12,
        border: `1px solid var(--brd)`,
        borderTop: `3px solid ${color}`,
        padding: "16px 18px",
      }}
    >
      <div
        style={{
          ...SG(9, 700),
          color: "var(--txt3)",
          textTransform: "uppercase",
          letterSpacing: ".08em",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <div style={{ ...DM(20), color: "var(--txt)", marginBottom: 4 }}>
        {value}
      </div>
      {d != null && (
        <span
          style={{
            ...SG(10, 600),
            color: parseFloat(d) >= 0 ? COLORS.teal : COLORS.red,
          }}
        >
          {parseFloat(d) >= 0 ? "▲" : "▼"} {Math.abs(d)}%
        </span>
      )}
    </div>
  );
}

// ═════════════════════════════════════════════════════════════════════════════
// CHART CANVAS COMPONENT
// ═════════════════════════════════════════════════════════════════════════════

export function ChartCanvas({ type, labels, datasets, periods, configKey }) {
  const canvasRef = useRef(null);
  const chartRef = useRef(null);
  const dataRef = useRef({ labels, datasets, periods });
  dataRef.current = { labels, datasets, periods };

  // Use configKey or JSON.stringify a stable key to avoid infinite re-renders
  const stableKey =
    configKey ||
    JSON.stringify({ labels, datasets: (datasets || []).map((d) => d.data) });

  useEffect(() => {
    if (!canvasRef.current || !window.Chart) return;
    if (chartRef.current) {
      chartRef.current.destroy();
      chartRef.current = null;
    }

    const { labels: l, datasets: ds, periods: p } = dataRef.current;
    const isDoughnut = type === "doughnut" || type === "pie";
    const config = {
      type,
      data: {
        labels: l || (p ? p.map((pp) => PERIOD_LABELS[pp]) : []),
        datasets: ds || [],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            labels: {
              font: { family: "'Space Grotesk', monospace", size: 10 },
              color: "rgba(255,255,255,.5)",
            },
          },
        },
        ...(isDoughnut
          ? {}
          : {
              scales: {
                x: {
                  grid: { color: "rgba(30,50,72,.5)" },
                  ticks: {
                    font: {
                      family: "'Space Grotesk', monospace",
                      size: 9,
                    },
                    color: "rgba(255,255,255,.4)",
                  },
                },
                y: {
                  grid: { color: "rgba(30,50,72,.5)" },
                  ticks: {
                    font: {
                      family: "'Space Grotesk', monospace",
                      size: 9,
                    },
                    color: "rgba(255,255,255,.4)",
                  },
                },
              },
            }),
      },
    };

    chartRef.current = new window.Chart(canvasRef.current, config);
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [type, stableKey]);

  return <canvas ref={canvasRef} style={{ height: 200 }} />;
}
