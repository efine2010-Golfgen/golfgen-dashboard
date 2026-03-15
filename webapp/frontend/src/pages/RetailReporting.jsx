import { useState, useEffect, useRef } from "react";
import { api, fmt$ } from "../lib/api";

const SG = (sz, wt = 600, c) => ({ fontFamily: "'Space Grotesk', monospace", fontSize: sz, fontWeight: wt, ...(c ? { color: c } : {}) });
const DM = (sz, c) => ({ fontFamily: "'DM Serif Display', Georgia, serif", fontSize: sz, ...(c ? { color: c } : {}) });

const TABS = [
  { key: "scorecard", label: "Weekly Scorecard" },
  { key: "stores", label: "Store Performance" },
  { key: "items", label: "Item Performance" },
  { key: "ecomm", label: "eComm Sales" },
  { key: "replenishment", label: "Replenishment" },
];

const Card = ({ children, style }) => (
  <div style={{ background: "var(--card)", borderRadius: 12, border: "1px solid var(--brd)", padding: "18px 20px", ...style }}>{children}</div>
);

const fN = (v) => v == null ? "0" : Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
const f$ = (v) => v == null ? "$0" : "$" + Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
const fPct = (v) => v == null ? "—" : (Number(v) * 100).toFixed(1) + "%";
const delta = (ty, ly) => {
  if (!ly || ly === 0) return null;
  return ((ty - ly) / Math.abs(ly) * 100).toFixed(1);
};
const DeltaBadge = ({ ty, ly, invert }) => {
  const d = delta(ty, ly);
  if (d === null) return <span style={{ ...SG(10), color: "var(--txt3)" }}>—</span>;
  const pos = parseFloat(d) >= 0;
  const good = invert ? !pos : pos;
  return <span style={{ ...SG(10, 600), color: good ? "#2ECFAA" : "#f87171" }}>{pos ? "▲" : "▼"} {Math.abs(d)}%</span>;
};

export default function RetailReporting({ filters = {} }) {
  const [tab, setTab] = useState("scorecard");
  const [loading, setLoading] = useState(true);
  const [summary, setSummary] = useState(null);
  const [scorecard, setScorecard] = useState([]);
  const [stores, setStores] = useState({ stores: [], total: 0, weeks: [] });
  const [items, setItems] = useState({ items: [], total: 0 });
  const [ecomm, setEcomm] = useState({ items: [], total: 0 });
  const [forecast, setForecast] = useState({ forecast: [] });
  const [week, setWeek] = useState("");
  const [periodType, setPeriodType] = useState("weekly");
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState(null);
  const fileRef = useRef(null);

  const h = filters;

  useEffect(() => {
    setLoading(true);
    api.retailSummary(h).then(setSummary).catch(() => setSummary(null)).finally(() => setLoading(false));
  }, [filters.division, filters.customer]);

  useEffect(() => {
    if (tab === "scorecard") api.retailScorecard(h).then(d => setScorecard(d.scorecard || [])).catch(() => {});
    if (tab === "stores") api.retailStorePerformance(h, week).then(setStores).catch(() => {});
    if (tab === "items") api.retailItemPerformance(h, periodType, week).then(setItems).catch(() => {});
    if (tab === "ecomm") api.retailEcomm(h, week).then(setEcomm).catch(() => {});
    if (tab === "replenishment") api.retailOrderForecast(h).then(setForecast).catch(() => {});
  }, [tab, filters.division, filters.customer, week, periodType]);

  const handleUpload = async () => {
    const file = fileRef.current?.files?.[0];
    if (!file) return;
    setUploading(true);
    setUploadResult(null);
    try {
      const res = await api.retailUpload(file);
      setUploadResult(res);
      api.retailSummary(h).then(setSummary);
    } catch (e) {
      setUploadResult({ status: "error", error: e.message });
    }
    setUploading(false);
  };

  const tc = summary?.tableCounts || {};
  const ps = summary?.posSummary || {};

  return (
    <div style={{ padding: "0 24px 40px" }}>
      {/* ── Header ── */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <div>
          <h1 style={{ ...DM(22), margin: 0, color: "var(--txt)" }}>Retail Reporting</h1>
          <p style={{ ...SG(11), color: "var(--txt3)", margin: "2px 0 0" }}>
            Walmart POS sell-through data from Scintilla reports
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <input type="file" ref={fileRef} accept=".xlsx,.xls" style={{ display: "none" }} onChange={handleUpload} />
          <button
            onClick={() => fileRef.current?.click()}
            disabled={uploading}
            style={{ ...SG(11, 600), background: "var(--accent)", color: "#fff", border: "none", borderRadius: 6, padding: "6px 14px", cursor: "pointer", opacity: uploading ? 0.6 : 1 }}
          >
            {uploading ? "Uploading..." : "Upload Report"}
          </button>
          {uploadResult && (
            <span style={{ ...SG(10), color: uploadResult.status === "ok" ? "#2ECFAA" : "#f87171" }}>
              {uploadResult.status === "ok"
                ? `✓ ${uploadResult.report_type}: ${uploadResult.rows_loaded} rows`
                : `✗ ${uploadResult.error || "Failed"}`}
            </span>
          )}
        </div>
      </div>

      {/* ── KPI summary cards ── */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginBottom: 16 }}>
        <Card>
          <div style={{ ...SG(9, 700), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".08em" }}>POS Sales (Latest Wk)</div>
          <div style={{ ...DM(20), color: "var(--txt)", marginTop: 4 }}>{f$(ps.posSalesTy)}</div>
          <DeltaBadge ty={ps.posSalesTy} ly={ps.posSalesLy} />
        </Card>
        <Card>
          <div style={{ ...SG(9, 700), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".08em" }}>POS Units (Latest Wk)</div>
          <div style={{ ...DM(20), color: "var(--txt)", marginTop: 4 }}>{fN(ps.posQtyTy)}</div>
          <DeltaBadge ty={ps.posQtyTy} ly={ps.posQtyLy} />
        </Card>
        <Card>
          <div style={{ ...SG(9, 700), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".08em" }}>Store Rows</div>
          <div style={{ ...DM(20), color: "var(--txt)", marginTop: 4 }}>{fN(tc.walmart_store_weekly)}</div>
        </Card>
        <Card>
          <div style={{ ...SG(9, 700), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".08em" }}>Item Rows</div>
          <div style={{ ...DM(20), color: "var(--txt)", marginTop: 4 }}>{fN(tc.walmart_item_weekly)}</div>
        </Card>
        <Card>
          <div style={{ ...SG(9, 700), color: "var(--txt3)", textTransform: "uppercase", letterSpacing: ".08em" }}>Latest Week</div>
          <div style={{ ...DM(20), color: "var(--txt)", marginTop: 4 }}>{summary?.latestWeek || "—"}</div>
        </Card>
      </div>

      {/* ── Sub-nav tabs ── */}
      <div style={{ display: "flex", gap: 2, marginBottom: 16, borderBottom: "1px solid var(--brd)" }}>
        {TABS.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)}
            style={{
              ...SG(11, tab === t.key ? 700 : 500),
              background: "none", border: "none", cursor: "pointer",
              padding: "8px 14px",
              color: tab === t.key ? "var(--accent)" : "var(--txt3)",
              borderBottom: tab === t.key ? "2px solid var(--accent)" : "2px solid transparent",
            }}>{t.label}</button>
        ))}
      </div>

      {/* ── Tab content ── */}
      {tab === "scorecard" && <ScorecardTab data={scorecard} />}
      {tab === "stores" && <StoreTab data={stores} week={week} setWeek={setWeek} />}
      {tab === "items" && <ItemTab data={items} week={week} setWeek={setWeek} periodType={periodType} setPeriodType={setPeriodType} />}
      {tab === "ecomm" && <EcommTab data={ecomm} week={week} setWeek={setWeek} />}
      {tab === "replenishment" && <ReplenishmentTab data={forecast} />}

      {/* ── Upload history ── */}
      {summary?.uploads?.length > 0 && (
        <Card style={{ marginTop: 20 }}>
          <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>Recent Uploads</div>
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
              {summary.uploads.map((u, i) => (
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
  );
}

/* ═══════════════════════════════════════════════ */
/*  SCORECARD TAB                                  */
/* ═══════════════════════════════════════════════ */
function ScorecardTab({ data }) {
  if (!data.length) return <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No scorecard data. Upload a Weekly Scorecard report.</p></Card>;

  // Group by vendor → metricGroup → rows
  const vendors = {};
  data.forEach(r => {
    if (!vendors[r.vendorSection]) vendors[r.vendorSection] = {};
    if (!vendors[r.vendorSection][r.metricGroup]) vendors[r.vendorSection][r.metricGroup] = [];
    vendors[r.vendorSection][r.metricGroup].push(r);
  });

  // Get unique periods
  const periods = [...new Set(data.map(r => r.period))];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {Object.entries(vendors).map(([vendor, groups]) => (
        <Card key={vendor}>
          <div style={{ ...SG(13, 700), color: "var(--txt)", marginBottom: 10 }}>{vendor}</div>
          {Object.entries(groups).map(([group, rows]) => (
            <div key={group} style={{ marginBottom: 12 }}>
              <div style={{ ...SG(10, 700), color: "var(--accent)", textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 4 }}>{group}</div>
              <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                    <th style={{ textAlign: "left", padding: "3px 6px", color: "var(--txt3)" }}>Metric</th>
                    {periods.map(p => (
                      <th key={p} colSpan={3} style={{ textAlign: "center", padding: "3px 6px", color: "var(--txt3)" }}>{p}</th>
                    ))}
                  </tr>
                  <tr style={{ borderBottom: "1px solid var(--brd)" }}>
                    <th />
                    {periods.map(p => (
                      <>{" "}
                        <th key={p + "ty"} style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 9 }}>TY</th>
                        <th key={p + "ly"} style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 9 }}>LY</th>
                        <th key={p + "d"} style={{ textAlign: "right", padding: "2px 4px", color: "var(--txt3)", fontSize: 9 }}>Diff</th>
                      </>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {[...new Set(rows.map(r => r.metricName))].map(metric => {
                    const mRows = rows.filter(r => r.metricName === metric);
                    return (
                      <tr key={metric} style={{ borderBottom: "1px solid var(--brd)" }}>
                        <td style={{ padding: "3px 6px", color: "var(--txt)" }}>{metric}</td>
                        {periods.map(p => {
                          const r = mRows.find(x => x.period === p);
                          return (
                            <>{" "}
                              <td key={p + "ty"} style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt)" }}>{r ? fN(r.valueTy) : "—"}</td>
                              <td key={p + "ly"} style={{ textAlign: "right", padding: "3px 4px", color: "var(--txt2)" }}>{r ? fN(r.valueLy) : "—"}</td>
                              <td key={p + "d"} style={{ textAlign: "right", padding: "3px 4px" }}>
                                {r && r.valueDiff != null
                                  ? <span style={{ color: r.valueDiff >= 0 ? "#2ECFAA" : "#f87171" }}>{(r.valueDiff * 100).toFixed(1)}%</span>
                                  : "—"}
                              </td>
                            </>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ))}
        </Card>
      ))}
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  STORE PERFORMANCE TAB                          */
/* ═══════════════════════════════════════════════ */
function StoreTab({ data, week, setWeek }) {
  const { stores = [], total = 0, weeks = [] } = data;
  return (
    <div>
      <div style={{ display: "flex", gap: 8, marginBottom: 10, alignItems: "center" }}>
        <span style={{ ...SG(10, 600), color: "var(--txt3)" }}>Week:</span>
        <select value={week} onChange={e => setWeek(e.target.value)}
          style={{ ...SG(10), padding: "3px 8px", borderRadius: 4, border: "1px solid var(--brd)", background: "var(--card)", color: "var(--txt)" }}>
          <option value="">All Weeks</option>
          {weeks.map(w => <option key={w} value={w}>{w}</option>)}
        </select>
        <span style={{ ...SG(10), color: "var(--txt3)" }}>{fN(total)} rows</span>
      </div>
      {stores.length === 0
        ? <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No store data. Upload a Store Level Detail report.</p></Card>
        : <Card style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
              <thead>
                <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                  {["Week", "Store #", "Store Name", "POS Sales TY", "POS Sales LY", "Δ", "POS Qty TY", "InStock% TY", "Returns TY", "On Hand TY"].map(h => (
                    <th key={h} style={{ textAlign: h.includes("Name") ? "left" : "right", padding: "4px 6px", color: "var(--txt3)", whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {stores.map((s, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)", textAlign: "right" }}>{s.week}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{s.storeNumber}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)" }}>{s.storeName}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{f$(s.posSalesTy)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)", textAlign: "right" }}>{f$(s.posSalesLy)}</td>
                    <td style={{ padding: "4px 6px", textAlign: "right" }}><DeltaBadge ty={s.posSalesTy} ly={s.posSalesLy} /></td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{fN(s.posQtyTy)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{s.instockPctTy != null ? fPct(s.instockPctTy) : "—"}</td>
                    <td style={{ padding: "4px 6px", color: "#f87171", textAlign: "right" }}>{fN(s.returnsQtyTy)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{fN(s.onHandQtyTy)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
      }
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  ITEM PERFORMANCE TAB                           */
/* ═══════════════════════════════════════════════ */
function ItemTab({ data, week, setWeek, periodType, setPeriodType }) {
  const { items = [], total = 0 } = data;
  return (
    <div>
      <div style={{ display: "flex", gap: 8, marginBottom: 10, alignItems: "center" }}>
        <span style={{ ...SG(10, 600), color: "var(--txt3)" }}>Period:</span>
        {["weekly", "L1W", "L4W", "L13W", "L52W"].map(p => (
          <button key={p} onClick={() => setPeriodType(p)}
            style={{ ...SG(10, periodType === p ? 700 : 500), padding: "3px 8px", borderRadius: 4, cursor: "pointer",
              background: periodType === p ? "var(--accent)" : "var(--card)",
              color: periodType === p ? "#fff" : "var(--txt3)",
              border: periodType === p ? "none" : "1px solid var(--brd)" }}>{p}</button>
        ))}
        <span style={{ ...SG(10), color: "var(--txt3)", marginLeft: 8 }}>{fN(total)} rows</span>
      </div>
      {items.length === 0
        ? <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No item data. Upload a Dashboard Report 1 or 2.</p></Card>
        : <Card style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
              <thead>
                <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                  {["Week", "Item #", "Description", "Brand", "POS $ TY", "POS $ LY", "Δ", "Units TY", "U/Store TY", "InStock%", "Returns TY"].map(h => (
                    <th key={h} style={{ textAlign: ["Description", "Brand"].includes(h) ? "left" : "right", padding: "4px 6px", color: "var(--txt3)", whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {items.map((r, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)", textAlign: "right" }}>{r.week}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{r.itemNumber}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.itemDesc}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)" }}>{r.brand}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{f$(r.posSalesTy)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)", textAlign: "right" }}>{f$(r.posSalesLy)}</td>
                    <td style={{ padding: "4px 6px", textAlign: "right" }}><DeltaBadge ty={r.posSalesTy} ly={r.posSalesLy} /></td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{fN(r.posQtyTy)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{r.unitsPerStoreTy?.toFixed(1)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{r.instockPctTy != null ? fPct(r.instockPctTy) : "—"}</td>
                    <td style={{ padding: "4px 6px", color: "#f87171", textAlign: "right" }}>{fN(r.returnsQtyTy)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
      }
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  ECOMM TAB                                      */
/* ═══════════════════════════════════════════════ */
function EcommTab({ data, week, setWeek }) {
  const { items = [], total = 0 } = data;
  return (
    <div>
      <div style={{ display: "flex", gap: 8, marginBottom: 10, alignItems: "center" }}>
        <span style={{ ...SG(10), color: "var(--txt3)" }}>{fN(total)} items</span>
      </div>
      {items.length === 0
        ? <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No eComm data. Upload an eComm Sales Report.</p></Card>
        : <Card style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
              <thead>
                <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                  {["Week", "Item #", "Product", "Brand", "Auth Sales TY", "Auth Sales LY", "Δ", "Shipped $ TY", "Auth Qty TY"].map(h => (
                    <th key={h} style={{ textAlign: ["Product", "Brand"].includes(h) ? "left" : "right", padding: "4px 6px", color: "var(--txt3)", whiteSpace: "nowrap" }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {items.map((r, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)", textAlign: "right" }}>{r.week}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{r.itemNumber}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.productName}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)" }}>{r.brand}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{f$(r.authSalesTy)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)", textAlign: "right" }}>{f$(r.authSalesLy)}</td>
                    <td style={{ padding: "4px 6px", textAlign: "right" }}><DeltaBadge ty={r.authSalesTy} ly={r.authSalesLy} /></td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{f$(r.shippedSalesTy)}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)", textAlign: "right" }}>{fN(r.authQtyTy)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
      }
    </div>
  );
}

/* ═══════════════════════════════════════════════ */
/*  REPLENISHMENT TAB                              */
/* ═══════════════════════════════════════════════ */
function ReplenishmentTab({ data }) {
  const { forecast = [] } = data;
  return (
    <div>
      {forecast.length === 0
        ? <Card><p style={{ ...SG(12), color: "var(--txt3)" }}>No order forecast data. Upload an Order Forecast report.</p></Card>
        : <Card>
            <div style={{ ...SG(12, 700), color: "var(--txt)", marginBottom: 8 }}>DC Order Forecast</div>
            <table style={{ width: "100%", borderCollapse: "collapse", ...SG(10) }}>
              <thead>
                <tr style={{ borderBottom: "2px solid var(--brd)" }}>
                  <th style={{ textAlign: "left", padding: "4px 6px", color: "var(--txt3)" }}>Snapshot Date</th>
                  <th style={{ textAlign: "left", padding: "4px 6px", color: "var(--txt3)" }}>DC/Store #</th>
                  <th style={{ textAlign: "left", padding: "4px 6px", color: "var(--txt3)" }}>Type</th>
                  <th style={{ textAlign: "left", padding: "4px 6px", color: "var(--txt3)" }}>Dept #</th>
                </tr>
              </thead>
              <tbody>
                {forecast.map((r, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid var(--brd)" }}>
                    <td style={{ padding: "4px 6px", color: "var(--txt)" }}>{r.snapshotDate}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt)" }}>{r.storeDcNbr}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)" }}>{r.storeDcType}</td>
                    <td style={{ padding: "4px 6px", color: "var(--txt2)" }}>{r.vendorDeptNumber}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
      }
    </div>
  );
}
