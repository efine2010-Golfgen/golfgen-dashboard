import { useState, useEffect, useCallback } from "react";
import { api } from "../lib/api";

// ── Status badge ────────────────────────────────────────────────────────────
const STATUS_META = {
  SUCCESS:    { bg: "#dcfce7", color: "#166534", label: "SUCCESS" },
  COMPLETE:   { bg: "#dcfce7", color: "#166534", label: "COMPLETE" },
  completed:  { bg: "#dcfce7", color: "#166534", label: "SUCCESS" },
  PARTIAL:    { bg: "#fef9c3", color: "#854d0e", label: "PARTIAL" },
  FAILED:     { bg: "#fee2e2", color: "#991b1b", label: "FAILED" },
  failed:     { bg: "#fee2e2", color: "#991b1b", label: "FAILED" },
  QUOTA_EXCEEDED: { bg: "#fef3c7", color: "#92400e", label: "QUOTA" },
  BUDGET_EXHAUSTED: { bg: "#fef3c7", color: "#92400e", label: "BUDGET" },
  in_progress: { bg: "#dbeafe", color: "#1e40af", label: "RUNNING" },
};

function Badge({ status }) {
  const s = STATUS_META[status] || { bg: "#f3f4f6", color: "#6b7280", label: status || "—" };
  return (
    <span style={{
      display: "inline-block", padding: "2px 10px", borderRadius: 12,
      fontSize: 11, fontWeight: 600, background: s.bg, color: s.color,
    }}>
      {s.label}
    </span>
  );
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function fmtDate(ts) {
  if (!ts || ts === "None" || ts === "null") return "—";
  try {
    const d = new Date(ts);
    return d.toLocaleDateString("en-US", { timeZone: "America/Chicago", month: "short", day: "numeric" })
      + " " + d.toLocaleTimeString("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit", hour12: true }) + " CT";
  } catch { return ts; }
}

function fmtAgo(ts) {
  if (!ts || ts === "None" || ts === "null") return "never";
  try {
    const diff = (Date.now() - new Date(ts).getTime()) / 1000;
    if (diff < 60) return `${Math.round(diff)}s ago`;
    if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
    if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
    return `${Math.round(diff / 86400)}d ago`;
  } catch { return ts; }
}

function fmtDuration(s) {
  if (s == null) return "—";
  if (s < 60) return `${s}s`;
  return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
}

// ── Month coverage box ───────────────────────────────────────────────────────
function MonthBox({ m }) {
  const [hovered, setHovered] = useState(false);
  const color = m.status === "complete" ? "#16a34a"
               : m.status === "partial" ? "#d97706"
               : "#dc2626";
  const bg = m.status === "complete" ? "#dcfce7"
            : m.status === "partial" ? "#fef9c3"
            : "#fee2e2";
  return (
    <div
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      style={{
        position: "relative",
        padding: "8px 6px", borderRadius: 8, background: bg,
        border: `1.5px solid ${color}30`, textAlign: "center",
        cursor: "default", minWidth: 64, transition: "transform 0.1s",
        transform: hovered ? "scale(1.05)" : "scale(1)",
      }}
    >
      <div style={{ fontSize: 11, fontWeight: 700, color, marginBottom: 2 }}>{m.label}</div>
      <div style={{ fontSize: 13, fontWeight: 700, color }}>
        {m.status === "empty" ? "0%" : `${m.pct}%`}
      </div>
      <div style={{ fontSize: 10, color: "#6b7280", marginTop: 1 }}>
        {m.found}/{m.expected}d
      </div>
      {hovered && (
        <div style={{
          position: "absolute", bottom: "calc(100% + 6px)", left: "50%",
          transform: "translateX(-50%)", background: "#1e293b", color: "#fff",
          padding: "4px 10px", borderRadius: 6, fontSize: 11, whiteSpace: "nowrap",
          zIndex: 10, pointerEvents: "none",
        }}>
          {m.found} of {m.expected} days present
        </div>
      )}
    </div>
  );
}

// ── Job health card ──────────────────────────────────────────────────────────
const JOB_LABELS = {
  sp_api_sync:         { label: "Full Sync", icon: "🔄", desc: "4×/day (9am,12,3,6pm CT)" },
  today_sync:          { label: "Today Sync", icon: "⚡", desc: "Hourly at :30" },
  gap_fill:            { label: "Gap Fill", icon: "🧩", desc: "Every 2h at :45" },
  ads_sync:            { label: "Ads Sync", icon: "📢", desc: "Every 2h" },
  nightly_deep_sync:   { label: "Nightly Deep", icon: "🌙", desc: "3am CT" },
  auto_backfill:       { label: "Startup Backfill", icon: "🚀", desc: "On deploy (4h throttle)" },
  pricing_sync:        { label: "Pricing Sync", icon: "💲", desc: "Hourly at :30" },
  nightly_backup_gdrive: { label: "Drive Backup", icon: "💾", desc: "2am CT" },
};

function JobCard({ jobKey, info }) {
  const meta = JOB_LABELS[jobKey] || { label: jobKey, icon: "⚙️", desc: "" };
  const successRate = info.runs_24h > 0
    ? Math.round(info.success_24h / info.runs_24h * 100) : null;
  const borderColor = info.last_status === "SUCCESS" || info.last_status === "COMPLETE" ? "#16a34a"
                    : info.last_status === "PARTIAL" ? "#d97706"
                    : info.last_status ? "#dc2626" : "#e2e8f0";
  return (
    <div style={{
      background: "#fff", borderRadius: 10, padding: "14px 16px",
      border: `1.5px solid ${borderColor}30`,
      boxShadow: "0 1px 3px rgba(0,0,0,0.06)",
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: "#2A3D50" }}>
            {meta.icon} {meta.label}
          </div>
          <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 2 }}>{meta.desc}</div>
        </div>
        {info.last_status ? <Badge status={info.last_status} /> : <span style={{ fontSize: 11, color: "#94a3b8" }}>no data</span>}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px 12px", fontSize: 11 }}>
        <div>
          <span style={{ color: "#94a3b8" }}>Last run: </span>
          <span style={{ color: "#2A3D50", fontWeight: 500 }}>{fmtAgo(info.last_run)}</span>
        </div>
        <div>
          <span style={{ color: "#94a3b8" }}>Duration: </span>
          <span style={{ color: "#2A3D50", fontWeight: 500 }}>{fmtDuration(info.last_duration_s)}</span>
        </div>
        <div>
          <span style={{ color: "#94a3b8" }}>24h runs: </span>
          <span style={{ color: "#2A3D50", fontWeight: 500 }}>
            {info.runs_24h} {successRate !== null ? `(${successRate}% ok)` : ""}
          </span>
        </div>
        <div>
          <span style={{ color: "#94a3b8" }}>Records: </span>
          <span style={{ color: "#2A3D50", fontWeight: 500, fontFamily: "monospace" }}>
            {(info.records_24h || 0).toLocaleString()}
          </span>
        </div>
      </div>
      {info.last_error && (
        <div style={{
          marginTop: 8, padding: "4px 8px", background: "#fee2e2",
          borderRadius: 4, fontSize: 10, color: "#991b1b",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }} title={info.last_error}>
          {info.last_error}
        </div>
      )}
    </div>
  );
}

// ── Main page ────────────────────────────────────────────────────────────────
const JOB_FILTER_OPTIONS = [
  { key: "all", label: "All" },
  { key: "sp_api_sync", label: "Full Sync" },
  { key: "gap_fill", label: "Gap Fill" },
  { key: "today_sync", label: "Today" },
  { key: "ads_sync", label: "Ads" },
  { key: "nightly_deep_sync", label: "Nightly" },
  { key: "auto_backfill", label: "Backfill" },
  { key: "nightly_backup_gdrive", label: "Backup" },
];

export default function System() {
  const [syncLog, setSyncLog] = useState([]);
  const [coverage, setCoverage] = useState(null);
  const [health, setHealth] = useState(null);
  const [backup, setBackup] = useState(null);
  const [githubBackup, setGithubBackup] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(null);
  const [backupRunning, setBackupRunning] = useState(false);
  const [githubBackupRunning, setGithubBackupRunning] = useState(false);
  const [gapFillRunning, setGapFillRunning] = useState(false);
  const [gapFillResult, setGapFillResult] = useState(null);
  const [logFilter, setLogFilter] = useState("all");
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState(null);

  const load = useCallback((isManual = false) => {
    if (isManual) setRefreshing(true);
    setRefreshError(null);
    return Promise.all([
      api.syncLog(150).catch(e => { console.warn("syncLog failed:", e); return null; }),
      api.dataCoverage().catch(e => { console.warn("dataCoverage failed:", e); return null; }),
      api.health().catch(e => { console.warn("health failed:", e); return null; }),
      api.backupStatus().catch(e => { console.warn("backupStatus failed:", e); return null; }),
      api.githubBackupStatus().catch(e => { console.warn("githubStatus failed:", e); return null; }),
    ]).then(([log, cov, h, b, gb]) => {
      // Only update state if the fetch returned data — preserve stale data on error
      if (log !== null) setSyncLog(log.entries || []);
      if (cov !== null) setCoverage(cov);
      if (h !== null) setHealth(h);
      if (b !== null) setBackup(b);
      if (gb !== null) setGithubBackup(gb);
      const anyFailed = [log, cov, h, b, gb].some(x => x === null);
      if (anyFailed && isManual) setRefreshError("Some data failed to load — showing last known values");
      setLoading(false);
      setLastRefresh(new Date());
    }).finally(() => {
      if (isManual) setRefreshing(false);
    });
  }, []);

  useEffect(() => {
    load(false);
    const interval = setInterval(() => load(false), 60_000);
    return () => clearInterval(interval);
  }, [load]);

  const handleRunBackup = async () => {
    setBackupRunning(true);
    try { await api.triggerBackup(); setTimeout(load, 3000); }
    catch (e) { console.error(e); }
    finally { setBackupRunning(false); }
  };

  const handleRunGithubBackup = async () => {
    setGithubBackupRunning(true);
    try { await api.triggerGithubBackup(); setTimeout(load, 2000); }
    catch (e) { console.error(e); }
    finally { setGithubBackupRunning(false); }
  };

  const handleGapFill = async () => {
    setGapFillRunning(true);
    setGapFillResult(null);
    try {
      const result = await api.triggerGapFill();
      setGapFillResult(result);
      setTimeout(load, 3000);
    } catch (e) {
      setGapFillResult({ status: "FAILED", reason: e.message });
    }
    finally { setGapFillRunning(false); }
  };

  const filteredLog = logFilter === "all"
    ? syncLog
    : syncLog.filter(r => r.job_name === logFilter);

  if (loading) return <div style={{ padding: 40, textAlign: "center", color: "#6B8090" }}>Loading system status...</div>;

  const budget = coverage?.report_budget || { used: 0, cap: 14, remaining: 14 };
  const budgetPct = Math.round(budget.used / budget.cap * 100);

  return (
    <div style={{ maxWidth: 1280, margin: "0 auto", padding: "24px 16px" }}>

      {/* ── Data Coverage ─────────────────────────────────────────────────── */}
      {coverage && (
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, marginBottom: 20, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16, flexWrap: "wrap", gap: 12 }}>
            <div>
              <h3 style={{ margin: "0 0 2px", fontSize: 15, fontWeight: 700 }}>📅 Historical Data Coverage</h3>
              <div style={{ fontSize: 12, color: "#6B8090" }}>
                Daily sales &amp; sessions · Apr 2024 → yesterday · as of {coverage.as_of}
              </div>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
              {/* Overall % pill */}
              <div style={{
                padding: "6px 16px", borderRadius: 20,
                background: coverage.coverage_pct >= 95 ? "#dcfce7" : coverage.coverage_pct >= 80 ? "#fef9c3" : "#fee2e2",
                color: coverage.coverage_pct >= 95 ? "#166534" : coverage.coverage_pct >= 80 ? "#854d0e" : "#991b1b",
                fontWeight: 700, fontSize: 18,
              }}>
                {coverage.coverage_pct}%
              </div>
              <div style={{ fontSize: 12, color: "#6B8090" }}>
                <div>{coverage.total_found.toLocaleString()} / {coverage.total_expected.toLocaleString()} days</div>
                <div style={{ color: "#dc2626", fontWeight: 600 }}>{coverage.missing_days} days missing</div>
              </div>
              <button
                onClick={handleGapFill}
                disabled={gapFillRunning}
                style={{
                  padding: "8px 16px", borderRadius: 8, border: "none",
                  background: gapFillRunning ? "#94a3b8" : "#16a34a", color: "#fff",
                  cursor: gapFillRunning ? "not-allowed" : "pointer",
                  fontSize: 12, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
                }}
              >
                {gapFillRunning
                  ? <><Spinner /> Filling gap...</>
                  : "▶ Fill Next Gap Now"
                }
              </button>
            </div>
          </div>

          {/* Gap fill result banner */}
          {gapFillResult && (
            <div style={{
              marginBottom: 14, padding: "8px 14px", borderRadius: 8,
              background: gapFillResult.status === "SUCCESS" ? "#dcfce7" : "#fef9c3",
              color: gapFillResult.status === "SUCCESS" ? "#166534" : "#854d0e",
              fontSize: 12, fontWeight: 500,
            }}>
              {gapFillResult.status === "SUCCESS"
                ? `✓ Filled ${gapFillResult.chunk} — ${gapFillResult.records?.toLocaleString()} records in ${gapFillResult.elapsed_seconds}s. ${gapFillResult.remaining_missing_days} days still missing.`
                : gapFillResult.status === "COMPLETE"
                ? `✓ Coverage is complete (${gapFillResult.coverage_pct}%) — nothing to fill.`
                : gapFillResult.status === "QUOTA_EXCEEDED" || gapFillResult.status === "BUDGET_EXHAUSTED"
                ? `⚠ API quota hit — will retry automatically at next scheduled run (2 hours).`
                : `✗ ${gapFillResult.reason || gapFillResult.status}`
              }
            </div>
          )}

          {/* Monthly grid */}
          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(72px, 1fr))",
            gap: 8, marginBottom: 16,
          }}>
            {coverage.months.map(m => <MonthBox key={m.month} m={m} />)}
          </div>

          {/* Legend */}
          <div style={{ display: "flex", gap: 16, fontSize: 11, color: "#6B8090", marginBottom: 12 }}>
            <span><span style={{ display: "inline-block", width: 10, height: 10, background: "#dcfce7", border: "1px solid #16a34a", borderRadius: 2, marginRight: 4 }} />Complete (≥95%)</span>
            <span><span style={{ display: "inline-block", width: 10, height: 10, background: "#fef9c3", border: "1px solid #d97706", borderRadius: 2, marginRight: 4 }} />Partial</span>
            <span><span style={{ display: "inline-block", width: 10, height: 10, background: "#fee2e2", border: "1px solid #dc2626", borderRadius: 2, marginRight: 4 }} />Empty (no data)</span>
          </div>

          {/* Missing ranges */}
          {coverage.missing_ranges.length > 0 && (
            <div>
              <div style={{ fontSize: 12, fontWeight: 600, color: "#2A3D50", marginBottom: 6 }}>
                Missing ranges ({coverage.missing_ranges.length}):
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                {coverage.missing_ranges.map((r, i) => (
                  <div key={i} style={{
                    padding: "4px 12px", borderRadius: 6, background: "#fee2e2",
                    border: "1px solid #fca5a5", fontSize: 12, color: "#991b1b", fontWeight: 500,
                  }}>
                    {r.label} · {r.start} → {r.end} ({r.days} days)
                  </div>
                ))}
              </div>
            </div>
          )}
          {coverage.missing_ranges.length === 0 && (
            <div style={{ fontSize: 12, color: "#16a34a", fontWeight: 600 }}>✓ No missing date ranges — full coverage</div>
          )}
        </div>
      )}

      {/* ── Sync Health + Report Budget ───────────────────────────────────── */}
      {coverage?.job_health && (
        <div style={{ background: "#fff", borderRadius: 12, padding: 20, marginBottom: 20, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14, flexWrap: "wrap", gap: 10 }}>
            <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700 }}>⚙️ Sync Job Health</h3>

            {/* Report budget gauge */}
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <div style={{ fontSize: 12, color: "#6B8090" }}>Report API budget today:</div>
              <div style={{ width: 120, height: 8, background: "#e2e8f0", borderRadius: 4, overflow: "hidden" }}>
                <div style={{
                  height: "100%", borderRadius: 4,
                  width: `${Math.min(100, budgetPct)}%`,
                  background: budgetPct < 50 ? "#16a34a" : budgetPct < 80 ? "#d97706" : "#dc2626",
                  transition: "width 0.3s",
                }} />
              </div>
              <div style={{ fontSize: 12, fontWeight: 600, color: "#2A3D50" }}>
                {budget.used}/{budget.cap} used
              </div>
              <div style={{ fontSize: 11, color: "#6B8090" }}>
                ({budget.remaining} remaining · resets midnight CT)
              </div>
            </div>
          </div>

          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
            gap: 12,
          }}>
            {Object.entries(coverage.job_health).map(([key, info]) => (
              <JobCard key={key} jobKey={key} info={info} />
            ))}
          </div>
        </div>
      )}

      {/* ── Health row counts ─────────────────────────────────────────────── */}
      {health && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12, marginBottom: 20 }}>
          <div style={{ background: "#fff", borderRadius: 10, padding: "12px 16px", boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4 }}>DB Status</div>
            <div style={{ fontSize: 18, fontWeight: 700, color: health.status === "healthy" ? "#16a34a" : "#dc2626" }}>
              {health.status === "healthy" ? "Healthy" : "Degraded"}
            </div>
          </div>
          <div style={{ background: "#fff", borderRadius: 10, padding: "12px 16px", boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
            <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4 }}>DB Size</div>
            <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace" }}>
              {health.database?.size_mb || 0} MB
            </div>
          </div>
          {health.database?.tables && Object.entries(health.database.tables).map(([tbl, cnt]) => (
            <div key={tbl} style={{ background: "#fff", borderRadius: 10, padding: "12px 16px", boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
              <div style={{ fontSize: 11, color: "#6B8090", marginBottom: 4 }}>{tbl}</div>
              <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace" }}>
                {(cnt || 0).toLocaleString()}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* ── Google Drive Backup ───────────────────────────────────────────── */}
      <div style={{ background: "#fff", borderRadius: 12, padding: 20, marginBottom: 16, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
          <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700 }}>💾 Google Drive Backup</h3>
          <button onClick={handleRunBackup} disabled={backupRunning} style={btnStyle(backupRunning, "#3E658C")}>
            {backupRunning ? <><Spinner /> Running...</> : "Run Backup Now"}
          </button>
        </div>
        {backup && !backup.configured && <Warn>Google Drive backup not configured. Set GOOGLE_SERVICE_ACCOUNT_JSON and BACKUP_DRIVE_FOLDER_ID.</Warn>}
        {backup?.error && backup.configured && <Err>{backup.error}</Err>}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 10, marginBottom: 14 }}>
          <StatBox label="Last Backup" value={backup?.last_backup ? fmtDate(backup.last_backup.created) : "Never"} />
          <StatBox label="Backup Size" value={backup?.last_backup ? `${backup.last_backup.size_mb} MB` : "—"} mono />
          <StatBox label="Total Backups" value={backup?.total_backups ?? "—"} mono />
          <StatBox label="Status" value={backup?.configured ? "Configured ✓" : "Not Configured"} color={backup?.configured ? "#16a34a" : "#dc2626"} />
        </div>
        {backup?.backups?.length > 0 && (
          <div>
            <div style={{ fontSize: 11, fontWeight: 600, color: "#6B8090", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>Recent Backups</div>
            {backup.backups.map((b, i) => (
              <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "7px 10px", background: "#f8fafc", borderRadius: 6, marginBottom: 4, fontSize: 12 }}>
                <span style={{ color: "#2A3D50", fontFamily: "monospace" }}>{b.name}</span>
                <span style={{ display: "flex", gap: 16, color: "#6B8090" }}>
                  <span>{b.size_mb} MB</span>
                  <span>{fmtDate(b.created)}</span>
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ── GitHub Backup ─────────────────────────────────────────────────── */}
      <div style={{ background: "#fff", borderRadius: 12, padding: 20, marginBottom: 20, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
          <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700 }}>🐙 GitHub Backup</h3>
          <div style={{ display: "flex", gap: 8 }}>
            {githubBackup?.repo_url && (
              <a href={githubBackup.repo_url} target="_blank" rel="noopener noreferrer"
                style={{ padding: "7px 14px", borderRadius: 8, border: "1px solid #e2e8f0", background: "#fff", color: "#2A3D50", textDecoration: "none", fontSize: 12, fontWeight: 500 }}>
                View on GitHub ↗
              </a>
            )}
            <button onClick={handleRunGithubBackup} disabled={githubBackupRunning} style={btnStyle(githubBackupRunning, "#2A3D50")}>
              {githubBackupRunning ? <><Spinner /> Running...</> : "Run GitHub Backup"}
            </button>
          </div>
        </div>
        {githubBackup && !githubBackup.configured && <Warn>GitHub backup not configured. Set GITHUB_TOKEN and BACKUP_GITHUB_REPO.</Warn>}
        {githubBackup?.error && githubBackup.configured && <Err>{githubBackup.error}</Err>}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 10 }}>
          <StatBox label="Last Commit" value={githubBackup?.last_backup_time ? fmtDate(githubBackup.last_backup_time) : "Never"} />
          <StatBox label="Files Committed" value={githubBackup?.files_committed ?? "—"} mono />
          <StatBox label="Status" value={githubBackup?.status ? <Badge status={githubBackup.status} /> : "—"} />
          <StatBox label="Repository" value={githubBackup?.repo || "Not configured"} />
        </div>
      </div>

      {/* ── Sync Log ──────────────────────────────────────────────────────── */}
      <div style={{ background: "#fff", borderRadius: 12, padding: 20, boxShadow: "0 1px 3px rgba(0,0,0,0.08)" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, flexWrap: "wrap", gap: 10 }}>
          <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700 }}>📋 Sync Log</h3>
          <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
            {lastRefresh && (
              <span style={{ fontSize: 11, color: "#6B8090" }}>
                Updated {lastRefresh.toLocaleTimeString("en-US", { timeZone: "America/Chicago", hour: "numeric", minute: "2-digit", hour12: true })} CT
                {refreshing && <span style={{ marginLeft: 6, color: "#3E658C" }}>· refreshing...</span>}
              </span>
            )}
            {refreshError && (
              <span style={{ fontSize: 11, color: "#d97706" }}>⚠ {refreshError}</span>
            )}
            <button
              onClick={() => load(true)}
              disabled={refreshing}
              style={{
                padding: "6px 14px", borderRadius: 6, border: "1px solid #e2e8f0",
                background: refreshing ? "#f8fafc" : "#fff",
                cursor: refreshing ? "not-allowed" : "pointer",
                fontSize: 12, fontWeight: 500, display: "flex", alignItems: "center", gap: 5,
                color: refreshing ? "#94a3b8" : "#2A3D50",
              }}
            >
              {refreshing ? <><Spinner2 /> Refreshing...</> : "↻ Refresh"}
            </button>
          </div>
        </div>

        {/* Filter tabs */}
        <div style={{ display: "flex", gap: 6, flexWrap: "wrap", marginBottom: 14 }}>
          {JOB_FILTER_OPTIONS.map(opt => (
            <button
              key={opt.key}
              onClick={() => setLogFilter(opt.key)}
              style={{
                padding: "4px 12px", borderRadius: 16, border: "none", cursor: "pointer",
                fontSize: 12, fontWeight: logFilter === opt.key ? 700 : 400,
                background: logFilter === opt.key ? "#3E658C" : "#f1f5f9",
                color: logFilter === opt.key ? "#fff" : "#6B8090",
              }}
            >
              {opt.label}
              <span style={{ marginLeft: 4, opacity: 0.7, fontSize: 10 }}>
                ({opt.key === "all" ? syncLog.length : syncLog.filter(r => r.job_name === opt.key).length})
              </span>
            </button>
          ))}
        </div>

        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ borderBottom: "2px solid #e2e8f0" }}>
                <th style={th}>Job</th>
                <th style={th}>Started</th>
                <th style={th}>Duration</th>
                <th style={th}>Status</th>
                <th style={{ ...th, textAlign: "right" }}>Records</th>
                <th style={th}>Error</th>
              </tr>
            </thead>
            <tbody>
              {filteredLog.length === 0 && (
                <tr><td colSpan={6} style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No entries</td></tr>
              )}
              {filteredLog.map((row, i) => (
                <tr key={row.id || i} style={{ borderBottom: "1px solid #f1f5f9" }}>
                  <td style={td}>
                    <span style={{ fontWeight: 600, color: "#2A3D50" }}>
                      {JOB_LABELS[row.job_name]?.icon || "⚙️"} {JOB_LABELS[row.job_name]?.label || row.job_name}
                    </span>
                  </td>
                  <td style={{ ...td, fontSize: 11, color: "#6B8090" }}>{fmtDate(row.started_at)}</td>
                  <td style={{ ...td, fontFamily: "monospace", fontSize: 11 }}>{fmtDuration(row.execution_time_seconds)}</td>
                  <td style={td}><Badge status={row.status} /></td>
                  <td style={{ ...td, textAlign: "right", fontFamily: "monospace" }}>
                    {(row.records_processed || 0).toLocaleString()}
                  </td>
                  <td style={{ ...td, maxWidth: 240, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {row.error_message
                      ? <span style={{ color: "#dc2626", fontSize: 11 }} title={row.error_message}>{row.error_message}</span>
                      : <span style={{ color: "#94a3b8" }}>—</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}

// ── Tiny shared components ───────────────────────────────────────────────────
function Spinner() {
  return <span style={{ display: "inline-block", width: 12, height: 12, border: "2px solid #fff", borderTopColor: "transparent", borderRadius: "50%", animation: "spin 1s linear infinite" }} />;
}
function Spinner2() {
  return <span style={{ display: "inline-block", width: 11, height: 11, border: "2px solid #94a3b8", borderTopColor: "transparent", borderRadius: "50%", animation: "spin 1s linear infinite" }} />;
}
function Warn({ children }) {
  return <div style={{ padding: "10px 14px", background: "#fef3c7", borderRadius: 8, fontSize: 12, color: "#92400e", marginBottom: 12 }}>{children}</div>;
}
function Err({ children }) {
  return <div style={{ padding: "10px 14px", background: "#fee2e2", borderRadius: 8, fontSize: 12, color: "#991b1b", marginBottom: 12 }}>{children}</div>;
}
function StatBox({ label, value, mono, color }) {
  return (
    <div style={{ padding: "10px 12px", background: "#f8fafc", borderRadius: 8 }}>
      <div style={{ fontSize: 10, color: "#6B8090", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.05em" }}>{label}</div>
      <div style={{ fontSize: 13, fontWeight: 600, fontFamily: mono ? "monospace" : undefined, color: color || "#2A3D50" }}>{value}</div>
    </div>
  );
}
function btnStyle(disabled, bg) {
  return {
    padding: "7px 14px", borderRadius: 8, border: "none",
    background: disabled ? "#94a3b8" : bg, color: "#fff",
    cursor: disabled ? "not-allowed" : "pointer",
    fontSize: 12, fontWeight: 600, display: "flex", alignItems: "center", gap: 6,
  };
}

const th = { padding: "8px 10px", textAlign: "left", fontSize: 10, fontWeight: 700, color: "#6B8090", textTransform: "uppercase", letterSpacing: "0.06em" };
const td = { padding: "9px 10px", color: "#2A3D50" };
