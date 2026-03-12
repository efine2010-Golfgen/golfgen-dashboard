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
  const [backup, setBackup] = useState(null);
  const [githubBackup, setGithubBackup] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(null);
  const [backupRunning, setBackupRunning] = useState(false);
  const [githubBackupRunning, setGithubBackupRunning] = useState(false);

  const load = useCallback(() => {
    Promise.all([
      api.syncLog(50).catch(() => ({ entries: [] })),
      api.health().catch(() => null),
      api.backupStatus().catch(() => null),
      api.githubBackupStatus().catch(() => null),
    ]).then(([log, h, b, gb]) => {
      setSyncLog(log.entries || []);
      setHealth(h);
      setBackup(b);
      setGithubBackup(gb);
      setLoading(false);
      setLastRefresh(new Date());
    });
  }, []);

  useEffect(() => {
    load();
    const interval = setInterval(load, 60_000);
    return () => clearInterval(interval);
  }, [load]);

  const handleRunBackup = async () => {
    setBackupRunning(true);
    try {
      await api.triggerBackup();
      // Auto-refresh after backup completes
      setTimeout(load, 2000);
    } catch (e) {
      console.error("Backup trigger failed:", e);
    } finally {
      setBackupRunning(false);
    }
  };

  const handleRunGithubBackup = async () => {
    setGithubBackupRunning(true);
    try {
      await api.triggerGithubBackup();
      setTimeout(load, 2000);
    } catch (e) {
      console.error("GitHub backup trigger failed:", e);
    } finally {
      setGithubBackupRunning(false);
    }
  };

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

      {/* Database Backup Card */}
      <div style={{ background: "#fff", borderRadius: 12, padding: 20, marginBottom: 24, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h3 style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>Database Backup (Google Drive)</h3>
          <button
            onClick={handleRunBackup}
            disabled={backupRunning}
            style={{
              padding: "8px 16px", borderRadius: 8, border: "none",
              background: backupRunning ? "#94a3b8" : "#3E658C", color: "#fff",
              cursor: backupRunning ? "not-allowed" : "pointer",
              fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
            }}
          >
            {backupRunning ? (
              <>
                <span style={{ display: "inline-block", width: 14, height: 14, border: "2px solid #fff", borderTopColor: "transparent", borderRadius: "50%", animation: "spin 1s linear infinite" }} />
                Running...
              </>
            ) : "Run Backup Now"}
          </button>
        </div>

        {backup && !backup.configured && (
          <div style={{ padding: 12, background: "#fef3c7", borderRadius: 8, fontSize: 13, color: "#92400e", marginBottom: 12 }}>
            Google Drive backup is not configured. Set GOOGLE_SERVICE_ACCOUNT_JSON and BACKUP_DRIVE_FOLDER_ID environment variables.
          </div>
        )}

        {backup && backup.error && backup.configured && (
          <div style={{ padding: 12, background: "#fee2e2", borderRadius: 8, fontSize: 13, color: "#991b1b", marginBottom: 12 }}>
            Error: {backup.error}
          </div>
        )}

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12, marginBottom: 16 }}>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Last Backup</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: "#2A3D50" }}>
              {backup?.last_backup ? fmtDate(backup.last_backup.created) : "Never"}
            </div>
          </div>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Backup Size</div>
            <div style={{ fontSize: 14, fontWeight: 600, fontFamily: "'Space Grotesk', monospace", color: "#2A3D50" }}>
              {backup?.last_backup ? `${backup.last_backup.size_mb} MB` : "—"}
            </div>
          </div>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Total Backups</div>
            <div style={{ fontSize: 14, fontWeight: 600, fontFamily: "'Space Grotesk', monospace", color: "#2A3D50" }}>
              {backup?.total_backups ?? "—"}
            </div>
          </div>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Status</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: backup?.configured ? "#16a34a" : "#dc2626" }}>
              {backup?.configured ? "Configured" : "Not Configured"}
            </div>
          </div>
        </div>

        {/* Recent backups list */}
        {backup?.backups && backup.backups.length > 0 && (
          <div>
            <div style={{ fontSize: 12, fontWeight: 600, color: "#6B8090", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>Recent Backups</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              {backup.backups.map((b, i) => (
                <div key={i} style={{
                  display: "flex", justifyContent: "space-between", alignItems: "center",
                  padding: "8px 12px", background: "#f8fafc", borderRadius: 6, fontSize: 13,
                }}>
                  <span style={{ color: "#2A3D50", fontFamily: "'Space Grotesk', monospace", fontSize: 12 }}>{b.name}</span>
                  <span style={{ display: "flex", gap: 16, alignItems: "center" }}>
                    <span style={{ color: "#6B8090", fontSize: 12 }}>{b.size_mb} MB</span>
                    <span style={{ color: "#6B8090", fontSize: 12 }}>{fmtDate(b.created)}</span>
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* GitHub Backup Card */}
      <div style={{ background: "#fff", borderRadius: 12, padding: 20, marginBottom: 24, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h3 style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>GitHub Backup (Manifest + Docs)</h3>
          <div style={{ display: "flex", gap: 8 }}>
            {githubBackup?.repo_url && (
              <a
                href={githubBackup.repo_url}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  padding: "8px 16px", borderRadius: 8, border: "1px solid #e2e8f0",
                  background: "#fff", color: "#2A3D50", textDecoration: "none",
                  fontSize: 13, fontWeight: 500, display: "flex", alignItems: "center", gap: 4,
                }}
              >
                View on GitHub
              </a>
            )}
            <button
              onClick={handleRunGithubBackup}
              disabled={githubBackupRunning}
              style={{
                padding: "8px 16px", borderRadius: 8, border: "none",
                background: githubBackupRunning ? "#94a3b8" : "#2A3D50", color: "#fff",
                cursor: githubBackupRunning ? "not-allowed" : "pointer",
                fontSize: 13, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
              }}
            >
              {githubBackupRunning ? (
                <>
                  <span style={{ display: "inline-block", width: 14, height: 14, border: "2px solid #fff", borderTopColor: "transparent", borderRadius: "50%", animation: "spin 1s linear infinite" }} />
                  Running...
                </>
              ) : "Run GitHub Backup Now"}
            </button>
          </div>
        </div>

        {githubBackup && !githubBackup.configured && (
          <div style={{ padding: 12, background: "#fef3c7", borderRadius: 8, fontSize: 13, color: "#92400e", marginBottom: 12 }}>
            GitHub backup is not configured. Set GITHUB_TOKEN and BACKUP_GITHUB_REPO environment variables.
          </div>
        )}

        {githubBackup?.error && githubBackup.configured && (
          <div style={{ padding: 12, background: "#fee2e2", borderRadius: 8, fontSize: 13, color: "#991b1b", marginBottom: 12 }}>
            Error: {githubBackup.error}
          </div>
        )}

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 12 }}>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Last Commit</div>
            <div style={{ fontSize: 14, fontWeight: 600, color: "#2A3D50" }}>
              {githubBackup?.last_backup_time ? fmtDate(githubBackup.last_backup_time) : "Never"}
            </div>
          </div>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Files Committed</div>
            <div style={{ fontSize: 14, fontWeight: 600, fontFamily: "'Space Grotesk', monospace", color: "#2A3D50" }}>
              {githubBackup?.files_committed ?? "—"}
            </div>
          </div>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Status</div>
            <div>
              {githubBackup?.status ? <Badge status={githubBackup.status} /> : <span style={{ fontSize: 14, color: "#6B8090" }}>—</span>}
            </div>
          </div>
          <div style={{ padding: 12, background: "#f8fafc", borderRadius: 8 }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>Repository</div>
            <div style={{ fontSize: 13, fontWeight: 500, color: "#2A3D50", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {githubBackup?.repo || "Not configured"}
            </div>
          </div>
        </div>
      </div>

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

      {/* Spinner animation */}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}

const th = { padding: "8px 12px", textAlign: "left", fontSize: 11, fontWeight: 600, color: "#6B8090", textTransform: "uppercase", letterSpacing: "0.05em" };
const td = { padding: "10px 12px", color: "#2A3D50" };
