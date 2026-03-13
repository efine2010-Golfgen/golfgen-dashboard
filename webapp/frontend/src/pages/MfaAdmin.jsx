import { useState, useEffect } from "react";

const API = "";

/**
 * MFA Admin Panel — integrated into the Permissions page.
 * Contains three sections:
 *   1. MFA-Protected Pages (toggle which tabs require MFA)
 *   2. User MFA Status (with admin reset)
 *   3. MFA Audit Log
 *
 * Import and render inside Permissions.jsx below the existing permissions grid:
 *   import MfaAdmin from "./MfaAdmin";
 *   ...
 *   {role === "admin" && <MfaAdmin />}
 */
export default function MfaAdmin() {
  const [routes, setRoutes] = useState([]);
  const [users, setUsers] = useState([]);
  const [auditLog, setAuditLog] = useState([]);
  const [auditTotal, setAuditTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [showAudit, setShowAudit] = useState(false);

  useEffect(() => { loadAll(); }, []);

  async function loadAll() {
    setLoading(true);
    try {
      const [rRes, uRes] = await Promise.all([
        fetch(`${API}/api/mfa/protected-routes`, { credentials: "include" }),
        fetch(`${API}/api/mfa/admin/users`, { credentials: "include" }),
      ]);
      const rData = await rRes.json();
      const uData = await uRes.json();
      setRoutes(rData.routes || []);
      setUsers(uData.users || []);
    } catch (e) { console.error("MFA admin load error:", e); }
    setLoading(false);
  }

  async function toggleRoute(tabKey, currentVal) {
    try {
      await fetch(`${API}/api/mfa/protected-routes`, {
        method: "POST", credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ tab_key: tabKey, mfa_required: !currentVal }),
      });
      setRoutes(prev =>
        prev.map(r => r.tab_key === tabKey ? { ...r, mfa_required: !currentVal } : r)
      );
    } catch (e) { console.error(e); }
  }

  async function resetUser(userName) {
    if (!window.confirm(`Reset MFA for ${userName}? They will need to re-enroll.`)) return;
    try {
      await fetch(`${API}/api/mfa/admin/reset`, {
        method: "POST", credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_name: userName }),
      });
      loadAll();
    } catch (e) { console.error(e); }
  }

  async function loadAuditLog() {
    try {
      const res = await fetch(`${API}/api/mfa/audit-log?limit=50`, { credentials: "include" });
      const data = await res.json();
      setAuditLog(data.entries || []);
      setAuditTotal(data.total || 0);
    } catch (e) { console.error(e); }
  }

  if (loading) {
    return (
      <div className="section-card" style={{ textAlign: "center", padding: 40 }}>
        <div className="spinner" />
      </div>
    );
  }

  return (
    <>
      {/* ─── MFA-Protected Pages ────────────────────────────── */}
      <div className="section-card" style={{ marginTop: 24 }}>
        <h3 style={{
          fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)",
          fontSize: 16, marginBottom: 16,
        }}>
          MFA-Protected Pages
        </h3>
        <p style={{ fontSize: 13, color: "var(--muted)", marginBottom: 16 }}>
          Pages marked as MFA-protected will require two-factor authentication.
          Users without MFA enabled will not see these pages.
        </p>

        <table className="data-table" style={{ width: "100%" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>Page</th>
              <th style={{ width: 100, textAlign: "center" }}>MFA Required</th>
            </tr>
          </thead>
          <tbody>
            {routes.map(r => (
              <tr key={r.tab_key}>
                <td style={{ fontWeight: 500 }}>{r.display_name}</td>
                <td style={{ textAlign: "center" }}>
                  <button
                    className={`perm-toggle ${r.mfa_required ? "perm-on" : "perm-off"}`}
                    onClick={() => toggleRoute(r.tab_key, r.mfa_required)}
                  >
                    {r.mfa_required ? "\u2713" : "\u2715"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ─── User MFA Status ────────────────────────────────── */}
      <div className="section-card" style={{ marginTop: 24 }}>
        <h3 style={{
          fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)",
          fontSize: 16, marginBottom: 16,
        }}>
          User MFA Status
        </h3>

        <table className="data-table" style={{ width: "100%" }}>
          <thead>
            <tr>
              <th style={{ textAlign: "left" }}>User</th>
              <th style={{ width: 100, textAlign: "center" }}>MFA Enabled</th>
              <th style={{ width: 140, textAlign: "center" }}>Enrolled</th>
              <th style={{ width: 100, textAlign: "center" }}>Action</th>
            </tr>
          </thead>
          <tbody>
            {users.map(u => (
              <tr key={u.user_name}>
                <td style={{ fontWeight: 500 }}>{u.user_name}</td>
                <td style={{ textAlign: "center" }}>
                  <span style={{
                    display: "inline-block", width: 24, height: 24,
                    borderRadius: "50%", lineHeight: "24px", textAlign: "center",
                    fontSize: 13, fontWeight: 600,
                    background: u.mfa_enabled ? "var(--teal)" : "var(--off-white)",
                    color: u.mfa_enabled ? "#fff" : "var(--muted)",
                  }}>
                    {u.mfa_enabled ? "\u2713" : "\u2715"}
                  </span>
                </td>
                <td style={{ textAlign: "center", fontSize: 13, color: "var(--muted)" }}>
                  {u.enrolled_at ? new Date(u.enrolled_at).toLocaleDateString() : "\u2014"}
                </td>
                <td style={{ textAlign: "center" }}>
                  {u.mfa_enabled && (
                    <button
                      onClick={() => resetUser(u.user_name)}
                      style={{
                        background: "none", border: "1px solid var(--orange)",
                        color: "var(--orange)", borderRadius: 6,
                        padding: "4px 10px", cursor: "pointer", fontSize: 12,
                        fontFamily: "'Sora', sans-serif",
                      }}
                    >
                      Reset
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ─── MFA Audit Log ──────────────────────────────────── */}
      <div className="section-card" style={{ marginTop: 24 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <h3 style={{
            fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)",
            fontSize: 16, margin: 0,
          }}>
            MFA Audit Log
          </h3>
          <button
            onClick={() => { setShowAudit(!showAudit); if (!showAudit) loadAuditLog(); }}
            style={{
              background: "var(--off-white)", border: "1px solid var(--border)",
              borderRadius: 8, padding: "6px 14px", cursor: "pointer",
              fontSize: 13, color: "var(--navy)", fontFamily: "'Sora', sans-serif",
            }}
          >
            {showAudit ? "Hide" : "Show Log"}
          </button>
        </div>

        {showAudit && (
          <>
            <p style={{ fontSize: 13, color: "var(--muted)", marginBottom: 12 }}>
              Showing {auditLog.length} of {auditTotal} events
            </p>
            <div style={{ overflowX: "auto" }}>
              <table className="data-table" style={{ width: "100%", fontSize: 13 }}>
                <thead>
                  <tr>
                    <th style={{ textAlign: "left" }}>Time</th>
                    <th style={{ textAlign: "left" }}>User</th>
                    <th style={{ textAlign: "left" }}>Event</th>
                    <th style={{ textAlign: "left" }}>Detail</th>
                    <th style={{ textAlign: "left" }}>IP</th>
                  </tr>
                </thead>
                <tbody>
                  {auditLog.map(e => (
                    <tr key={e.id}>
                      <td style={{ whiteSpace: "nowrap", color: "var(--muted)" }}>
                        {new Date(e.created_at).toLocaleString()}
                      </td>
                      <td style={{ fontWeight: 500 }}>{e.user_name}</td>
                      <td>
                        <span style={{
                          padding: "2px 8px", borderRadius: 4, fontSize: 12,
                          background: e.event_type.includes("fail") ? "#FEE2E2"
                            : e.event_type.includes("success") || e.event_type.includes("completed") ? "#D1FAE5"
                            : "var(--off-white)",
                          color: e.event_type.includes("fail") ? "#991B1B"
                            : e.event_type.includes("success") || e.event_type.includes("completed") ? "#065F46"
                            : "var(--navy)",
                        }}>
                          {e.event_type.replace(/_/g, " ")}
                        </span>
                      </td>
                      <td style={{ color: "var(--muted)", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis" }}>
                        {e.detail || "\u2014"}
                      </td>
                      <td style={{ color: "var(--muted)", fontFamily: "'Space Grotesk', monospace", fontSize: 12 }}>
                        {e.ip_address || "\u2014"}
                      </td>
                    </tr>
                  ))}
                  {auditLog.length === 0 && (
                    <tr>
                      <td colSpan={5} style={{ textAlign: "center", color: "var(--muted)", padding: 24 }}>
                        No MFA events recorded yet
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </>
        )}
      </div>
    </>
  );
}
