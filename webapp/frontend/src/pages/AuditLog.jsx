import { useState, useEffect } from "react";
import { api } from "../lib/api";

export default function AuditLog() {
  const [entries, setEntries] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(0);
  const pageSize = 50;

  useEffect(() => {
    setLoading(true);
    api.get(`/api/audit-log?limit=${pageSize}&offset=${page * pageSize}`)
      .then((data) => {
        setEntries(data.entries || []);
        setTotal(data.total || 0);
      })
      .catch(() => setEntries([]))
      .finally(() => setLoading(false));
  }, [page]);

  const actionColor = (action) => {
    if (action.includes("login")) return "#22c55e";
    if (action.includes("logout")) return "#3b82f6";
    if (action.includes("failed") || action.includes("rejected")) return "#ef4444";
    return "var(--muted)";
  };

  return (
    <div>
      <h2 style={{ marginBottom: 16 }}>Audit Log</h2>
      <p style={{ color: "var(--muted)", fontSize: 13, marginBottom: 16 }}>
        {total} total events
      </p>
      {loading ? (
        <p>Loading...</p>
      ) : (
        <>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: "2px solid var(--border)", textAlign: "left" }}>
                <th style={{ padding: "8px 12px" }}>Time</th>
                <th style={{ padding: "8px 12px" }}>User</th>
                <th style={{ padding: "8px 12px" }}>Action</th>
                <th style={{ padding: "8px 12px" }}>Detail</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e, i) => (
                <tr key={i} style={{ borderBottom: "1px solid var(--border)" }}>
                  <td style={{ padding: "6px 12px", whiteSpace: "nowrap", color: "var(--muted)" }}>
                    {e.ts ? new Date(e.ts + "Z").toLocaleString() : "—"}
                  </td>
                  <td style={{ padding: "6px 12px" }}>
                    <div style={{ fontWeight: 500 }}>{e.user_name || "—"}</div>
                    <div style={{ fontSize: 11, color: "var(--muted)" }}>{e.user_email}</div>
                  </td>
                  <td style={{ padding: "6px 12px" }}>
                    <span style={{
                      background: actionColor(e.action) + "22",
                      color: actionColor(e.action),
                      padding: "2px 8px",
                      borderRadius: 4,
                      fontSize: 12,
                      fontWeight: 600,
                    }}>
                      {e.action}
                    </span>
                  </td>
                  <td style={{ padding: "6px 12px", color: "var(--muted)" }}>{e.detail}</td>
                </tr>
              ))}
              {entries.length === 0 && (
                <tr>
                  <td colSpan={4} style={{ padding: 24, textAlign: "center", color: "var(--muted)" }}>
                    No audit log entries yet
                  </td>
                </tr>
              )}
            </tbody>
          </table>
          <div style={{ display: "flex", gap: 8, marginTop: 16, justifyContent: "center" }}>
            <button disabled={page === 0} onClick={() => setPage(p => p - 1)}
              style={{ padding: "6px 16px", cursor: page === 0 ? "default" : "pointer" }}>
              ← Prev
            </button>
            <span style={{ padding: "6px 12px", color: "var(--muted)" }}>
              Page {page + 1} of {Math.max(1, Math.ceil(total / pageSize))}
            </span>
            <button disabled={(page + 1) * pageSize >= total} onClick={() => setPage(p => p + 1)}
              style={{ padding: "6px 16px", cursor: (page + 1) * pageSize >= total ? "default" : "pointer" }}>
              Next →
            </button>
          </div>
        </>
      )}
    </div>
  );
}
