import { useState, useEffect } from "react";
import { api } from "../lib/api";
import MfaAdmin from "./MfaAdmin";

const TAB_LABELS = {
  dashboard: "Dashboard",
  products: "Products",
  profitability: "Profitability",
  advertising: "Advertising",
  inventory: "Amazon FBA",
  "golfgen-inventory": "GolfGen Inventory",
  "item-master": "Item Master",
  "factory-po": "Factory PO",
  logistics: "OTW / Logistics",
  "supply-chain": "Supply Chain",
  "fba-shipments": "Shipments to FBA",
  "item-planning": "Item Planning",
};

const TAB_KEYS = Object.keys(TAB_LABELS);

export default function Permissions() {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(null); // "userKey:tab" while saving
  const [error, setError] = useState("");

  const load = async () => {
    try {
      const result = await api.allPermissions();
      setUsers(result.users || []);
    } catch {
      setError("Failed to load permissions. You may not have admin access.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const toggle = async (userKey, tabKey, currentVal) => {
    const saveKey = `${userKey}:${tabKey}`;
    setSaving(saveKey);
    try {
      await api.updatePermission(userKey, tabKey, !currentVal);
      // Update local state
      setUsers(prev => prev.map(u => {
        if (u.key !== userKey) return u;
        return {
          ...u,
          permissions: { ...u.permissions, [tabKey]: !currentVal },
        };
      }));
    } catch {
      setError(`Failed to update ${tabKey} for ${userKey}`);
      setTimeout(() => setError(""), 3000);
    } finally {
      setSaving(null);
    }
  };

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading permissions...</div>;
  }

  if (error && users.length === 0) {
    return <div className="section-card" style={{ padding: 32, textAlign: "center", color: "#c00" }}>{error}</div>;
  }

  return (
    <div>
      <div className="page-header">
        <h2>User Permissions</h2>
        <p style={{ color: "var(--muted)", marginTop: 4 }}>
          Control which tabs each team member can access. Admins always have full access.
        </p>
      </div>

      {error && <div className="login-error" style={{ margin: "0 0 16px", maxWidth: 600 }}>{error}</div>}

      <div className="section-card" style={{ overflowX: "auto" }}>
        <table className="data-table">
          <thead>
            <tr>
              <th style={{ position: "sticky", left: 0, background: "#f8f9fa", zIndex: 2, minWidth: 140 }}>User</th>
              {TAB_KEYS.map(k => (
                <th key={k} style={{ textAlign: "center", fontSize: 12, minWidth: 90, whiteSpace: "nowrap" }}>
                  {TAB_LABELS[k]}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {users.map(u => {
              const perms = u.permissions || {};
              return (
                <tr key={u.key}>
                  <td style={{ position: "sticky", left: 0, background: "#fff", zIndex: 1, fontWeight: 600 }}>
                    <div>{u.name}</div>
                    <div style={{ fontSize: 11, color: "var(--muted)", fontWeight: 400 }}>{(u.emails || [])[0]}</div>
                  </td>
                  {TAB_KEYS.map(tabKey => {
                    const enabled = perms[tabKey] !== false;
                    const isSaving = saving === `${u.key}:${tabKey}`;
                    return (
                      <td key={tabKey} style={{ textAlign: "center" }}>
                        <button
                          className={`perm-toggle ${enabled ? "perm-on" : "perm-off"}`}
                          onClick={() => toggle(u.key, tabKey, enabled)}
                          disabled={isSaving}
                          title={enabled ? "Click to revoke" : "Click to grant"}
                        >
                          {isSaving ? "..." : enabled ? "✓" : "✕"}
                        </button>
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* MFA Administration */}
      <MfaAdmin />
    </div>
  );
}
