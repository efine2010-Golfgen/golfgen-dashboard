import { Fragment, useState, useEffect } from "react";
import { api } from "../../lib/api";
import {
  SG,
  DM,
  Card,
  CardHdr,
  fN,
  f$,
  fPct,
  COLORS,
  KPICard,
  ChartCanvas,
} from "./WalmartHelpers";

export function WalmartScorecard({ filters }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [data, setData] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        setLoading(true);
        setError(null);
        const result = await api.walmartScorecard(filters);
        setData(result);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    })();
  }, [filters.division, filters.customer]);

  if (loading) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>Loading...</p>
      </Card>
    );
  }
  if (error) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "#f87171" }}>Error: {error}</p>
      </Card>
    );
  }
  if (!data) {
    return (
      <Card>
        <p style={{ ...SG(12), color: "var(--txt3)" }}>No data</p>
      </Card>
    );
  }

  const scorecard = data.scorecard || [];
  const kpis = data.kpis || {};

  // Helper: detect if metric is a percentage type
  const isPctMetric = (name) =>
    /(%|margin|turns|gmroii|instock|aur)/i.test(name);

  // Smart value formatter for scorecard
  const fVal = (v, metricName) => {
    if (v == null) return "—";
    if (isPctMetric(metricName)) {
      if (Math.abs(v) < 10) return (v * 100).toFixed(1) + "%";
      return fN(v);
    }
    return fN(v);
  };

  // Extract KPIs and organize scorecard
  const kpiList = [
    { label: "POS Sales LW", key: "posSalesLw" },
    { label: "GIM % LW", key: "gimPctLw" },
    { label: "MUMD $ LW", key: "mumdLw" },
    { label: "Repl Instock % LW", key: "replInstockLw" },
    { label: "GMROII LW", key: "gmroiiLw" },
  ];

  const displayKpis = kpiList
    .map((item) => {
      const val = kpis[item.key];
      if (!val || (val.ty === 0 && val.ly === 0)) return null;
      return {
        label: item.label,
        value: fVal(val.ty, item.label),
        delta: val.diff ? (val.diff * 100).toFixed(1) : null,
      };
    })
    .filter(Boolean);

  // If no KPIs from backend extraction, build from scorecard rows directly
  if (displayKpis.length === 0 && scorecard.length > 0) {
    // Get a few key metrics from Last Week period
    const lwRows = scorecard.filter((r) => r.period === "Last Week");
    const seen = new Set();
    lwRows.forEach((r) => {
      if (displayKpis.length >= 5 || seen.has(r.metricName)) return;
      seen.add(r.metricName);
      displayKpis.push({
        label: r.metricName,
        value: fVal(r.valueTy, r.metricName),
        delta: r.valueDiff ? (r.valueDiff * 100).toFixed(1) : null,
      });
    });
  }

  // Group scorecard by section
  const sections = {};
  scorecard.forEach((row) => {
    const section = row.vendorSection || "Other";
    if (!sections[section]) sections[section] = [];
    sections[section].push(row);
  });

  // Extract unique periods from scorecard
  const periods = [
    "Last Week",
    "4-Week",
    "13-Week",
    "26-Week",
    "52-Week",
    "Year",
  ];
  const periodsInData = periods.filter((p) =>
    scorecard.some((row) => row.period === p)
  );

  // Build chart data: POS Sales TY vs LY
  const posSalesData = scorecard.filter(
    (row) => row.metricName?.includes("POS Sales")
  );
  const posPeriods = [
    ...new Set(posSalesData.map((row) => row.period)),
  ].slice(0, 4);
  const posSalesChartData = {
    labels: posPeriods,
    datasets: [
      {
        label: "TY",
        data: posPeriods.map((p) => {
          const r = posSalesData.find(
            (row) => row.period === p && row.metricName?.includes("dollars")
          );
          return r?.valueTy || 0;
        }),
        backgroundColor: COLORS.teal,
      },
      {
        label: "LY",
        data: posPeriods.map((p) => {
          const r = posSalesData.find(
            (row) => row.period === p && row.metricName?.includes("dollars")
          );
          return r?.valueLy || 0;
        }),
        backgroundColor: COLORS.orange,
      },
    ],
  };

  // Build GIM chart
  const gimData = scorecard.filter((row) =>
    row.metricName?.includes("GIM")
  );
  const gimPeriods = [...new Set(gimData.map((row) => row.period))].slice(
    0,
    4
  );
  const gimChartData = {
    labels: gimPeriods,
    datasets: [
      {
        label: "GIM %",
        data: gimPeriods.map((p) => {
          const r = gimData.find((row) => row.period === p);
          return r ? (r.valueTy * 100) : 0;
        }),
        borderColor: COLORS.blue,
        fill: false,
      },
    ],
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
      {/* KPI Cards */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(5, 1fr)",
          gap: "12px",
        }}
      >
        {displayKpis.map((kpi) => (
          <KPICard
            key={kpi.label}
            label={kpi.label}
            value={kpi.value}
            delta={kpi.delta}
          />
        ))}
      </div>

      {/* Full Scorecard Matrix (div-based CSS Grid) */}
      {Object.entries(sections).map(([section, rows]) => {
        const metricNames = [...new Set(rows.map((r) => r.metricName))];
        return (
          <Card key={section}>
            <CardHdr title={section} />
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "180px repeat(7, 1fr)",
                gap: "1px",
                backgroundColor: "var(--border)",
                padding: "1px",
              }}
            >
              {/* Header: Metric | LW TY | LW LY | CM TY | CM LY | LM TY | LM LY | YR TY */}
              <div
                style={{
                  backgroundColor: "var(--card)",
                  padding: "8px",
                  ...SG(10, 600),
                  color: "var(--txt3)",
                }}
              >
                Metric
              </div>
              {[
                "LW TY",
                "LW LY",
                "CM TY",
                "CM LY",
                "LM TY",
                "LM LY",
                "YR TY",
              ].map((hdr) => (
                <div
                  key={hdr}
                  style={{
                    backgroundColor: "var(--card)",
                    padding: "8px",
                    textAlign: "right",
                    ...SG(10, 600),
                    color: "var(--txt3)",
                  }}
                >
                  {hdr}
                </div>
              ))}

              {/* Data rows */}
              {metricNames.map((metric) => {
                const metricRows = rows.filter(
                  (r) => r.metricName === metric
                );
                const headerValues = [
                  "Last Week",
                  "Last Week",
                  "4-Week",
                  "4-Week",
                  "13-Week",
                  "13-Week",
                  "52-Week",
                ];
                const headerTypes = ["TY", "LY", "TY", "LY", "TY", "LY", "TY"];
                const cellValues = headerValues.map((period, idx) => {
                  const row = metricRows.find((r) => r.period === period);
                  const val =
                    headerTypes[idx] === "TY" ? row?.valueTy : row?.valueLy;
                  return val !== undefined && val !== null
                    ? fVal(val, metric)
                    : "—";
                });

                return (
                  <Fragment key={metric}>
                    <div
                      style={{
                        backgroundColor: "var(--card)",
                        padding: "8px",
                        ...SG(11),
                        color: "var(--txt)",
                      }}
                    >
                      {metric}
                    </div>
                    {cellValues.map((val, idx) => (
                      <div
                        key={idx}
                        style={{
                          backgroundColor: "var(--card)",
                          padding: "8px",
                          textAlign: "right",
                          ...SG(11),
                          color: "var(--txt)",
                        }}
                      >
                        {val}
                      </div>
                    ))}
                  </Fragment>
                );
              })}
            </div>
          </Card>
        );
      })}

      {/* Supporting Charts */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px" }}>
        <Card>
          <CardHdr title="POS Sales TY vs LY" />
          <ChartCanvas type="bar" data={posSalesChartData} height={250} />
        </Card>
        <Card>
          <CardHdr title="GIM % Trend" />
          <ChartCanvas type="line" data={gimChartData} height={250} />
        </Card>
      </div>
    </div>
  );
}
