import { useState, useEffect, useCallback } from "react";
import { api } from "../lib/api";

const STATUS_BADGE = {
  SUCCESS: { bg: "#dcfce7", color: "#166534", label: "SUCCESS" },
  completed: { bg: "#dcfce7", color: "#166534", label: "SUCCESS" },
  PARTIAL: { bg: "#fef9c3", color: "#854d0e", label: "PARTIAL" },
  FAILED: { bg: "#fee2e2", color: "#991b1b", label: "FAILED" },
  failed: { bg: "#fee2e2", color: "#991b1b", label: "FAILED" },
  in_progress: { bg: "#dbeafe", color: "#1e40af", label: "RUNNING" },
};

function Badge({ status }) {
  const s = STATUS_BADGE[status] || { bg: "#f3f4f6", color: "#6b7280", label: status || "—" };
  return (
    <span style={{
      display: "inline-block", padding: "2px 10px", borderRadius: 12,
      fontSize: 11, fontWeight: 600, background: s.bg, color: s.color,
    }}>
      {s.label}
    </span>
  );
}

export default function System() {
  const [syncLog, setSyncLog] = useState([]);
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(null);

  const load = useCallback(() => {
    Promise.all([
      api.syncLog(50).catch(() => ({ entries: [] })),
      api.health().catch(() => null),
    ]).then(([log, h]) => {
      setSyncLog(log.entries || []);
      setHealth(h);
      setLoading(false);
      setLastRefresh(new Date());
    });
  }, []);

  useEffect(() => {
    load();
    const interval = setInterval(load, 60_000); // auto-refresh every 60s
    return () => clearInterval(interval);
  }, [load]);

  const fmtTime = (ts) => {
    if (!ts || ts === "None") return "—";
    try {
      const d = new Date(ts);
      return d.toLocaleString("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit", second: "2-digit", hour12: true }) + " CT";
    } catch { return ts; }
  };

  const fmtDate = (ts) => {
    if (!ts || ts === "None") return "—";
    try {
      const d = new Date(ts);
      return d.toLocaleDateString("en-US", { timeZone: "America/Chicago", month: "short", day: "numeric" })
        + " " + d.toLocaleTimeString("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit", hour12: true });
    } catch { return ts; }
  };

  if (loading) return <div style={{ padding: 40, textAlign: "center", color: "#6B8090" }}>Loading system status...</div>;

  return (
    <div style={{ maxWidth: 1200, margin: "0 auto", padding: "24px 16px" }}>
      {/* Health Summary */}
      {health && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 16, marginBottom: 24 }}>
          <div style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
            <div style={{ fontSize: 12, color: "#6B8090", marginBottom: 4 }}>Status</div>
            <div style={{ fontSize: 20, fontWeight: 600, color: health.status === "healthy" ? "#16a34a" : "#dc2626" }}>
              {health.status === "healthy" ? "Healthy" : "Degraded"}
            </div>
          </div>
          <div style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
            <div style={{ fontSize: 12, color: "#6B8090", marginBottom: 4 }}>Database Size</div>
            <div style={{ fontSize: 20, fontWeight: 600, fontFamily: "'Space Grotesk', monospace" }}>
              {health.database?.size_mb || 0} MB
            </div>
          </div>
          {health.database?.tables && Object.entries(health.database.tables).map(([tbl, cnt]) => (
            <div key={tbl} style={{ background: "#fff", borderRadius: 12, padding: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
              <div style={{ fontSize: 12, color: "#6B8090", marginBottom: 4 }}>{tbl}</div>
              <div style={{ fontSize: 20, fontWeight: 600, fontFamily: "'Space Grotesk', monospace" }}>
                {(cnt || 0).toLocaleString()} rows
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Last Sync Summary */}
      {health?.last_sync && Object.keys(health.last_sync).length > 0 && (
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, marginBottom: 24, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
          <h3 style={{ margin: "0 0 12px", fontSize: 15, fontWeight: 600 }}>Last Sync Per Job</h3>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
            {Object.entries(health.last_sync).map(([job, info]) => (
              <div key={job} style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
                <div style={{ fontSize: 12, fontWeight: 600, color: "#2A3D50", marginBottom: 4 }}>{job}</div>
                <Badge status={info.status} />
                <div style={{ fontSize: 11, color: "#6B8090", marginTop: 4 }}>{fmtDate(info.time)}</div>
                {info.records > 0 && <div style={{ fontSize: 11, color: "#6B8090" }}>{info.records} records</div>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Sync Log Table */}
      <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h3 style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>Sync Log</h3>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {lastRefresh && (
              <span style={{ fontSize: 11, color: "#6B8090" }}>
                Last refreshed: {lastRefresh.toLocaleTimeString("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit", hour12: true })} Central
              </span>
            )}
            <button onClick={load} style={{
              padding: "6px 14px", borderRadius: 6, border: "1px solid #e2e8f0",
              background: "#fff", cursor: "pointer", fontSize: 12, fontWeight: 500,
            }}>Refresh</button>
          </div>
        </div>

        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "2px solid #e2e8f0" }}>
                <th style={th}>Job</th>
                <th style={th}>Started</th>
                <th style={th}>Duration</th>
                <th style={th}>Status</th>
                <th style={{ ...th, textAlign: "right" }}>Pulled</th>
                <th style={{ ...th, textAlign: "right" }}>Inserted</th>
                <th style={{ ...th, textAlign: "right" }}>Skipped</th>
                <th style={th}>Error</th>
              </tr>
            </thead>
            <tbody>
              {syncLog.length === 0 && (
                <tr><td colSpan={8} style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No sync log entries yet</td></tr>
              )}
              {syncLog.map((row, i) => (
                <tr key={row.id || i} style={{ borderBottom: "1px solid #f1f5f9" }}>
                  <td style={td}><span style={{ fontWeight: 500 }}>{row.job_name}</span></td>
                  <td style={td}>{fmtDate(row.started_at)}</td>
                  <td style={td}>{row.duration_seconds != null ? `${row.duration_seconds}s` : "—"}</td>
                  <td style={td}><Badge status={row.status} /></td>
                  <td style={{ ...td, textAlign: "right", fontFamily: "'Space Grotesk', monospace" }}>{row.records_pulled || 0}</td>
                  <td style={{ ...td, textAlign: "right", fontFamily: "'Space Grotesk', monospace" }}>{row.records_inserted || 0}</td>
                  <td style={{ ...td, textAlign: "right", fontFamily: "'Space Grotesk', monospace" }}>{row.records_skipped || 0}</td>
                  <td style={{ ...td, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {row.error_message ? <span style={{ color: "#dc2626", fontSize: 11 }} title={row.error_message}>{row.error_message}</span> : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

const th = { padding: "8px 12px", textAlign: "left", fontSize: 11, fontWeight: 600, color: "#6B8090", textTransform: "uppercase", letterSpacing: "0.05em" };
const td = { padding: "10px 12px", color: "#2A3D50" };
