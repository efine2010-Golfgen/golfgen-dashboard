import { useState } from "react";

const API = "";

/**
 * MFA Verification Challenge Page
 * Shown when a user has MFA enabled but hasn't verified this session yet.
 * Matches the Login.jsx card style exactly.
 */
export default function MfaVerify({ onVerified }) {
  const [code, setCode] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [useBackup, setUseBackup] = useState(false);

  async function handleVerify(e) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const res = await fetch(`${API}/api/mfa/verify`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code: code.trim(), use_backup: useBackup }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Verification failed");
      if (onVerified) onVerified();
    } catch (e) {
      setError(e.message);
    }
    setLoading(false);
  }

  return (
    <div className="login-page">
      <div className="login-card">
        <div style={{ textAlign: "center", marginBottom: 24 }}>
          <div style={{
            fontFamily: "'DM Serif Display', serif",
            fontSize: 28, marginBottom: 4,
          }}>
            <span style={{color:"#1A2D42"}}>Golf</span><span style={{color:"var(--teal)"}}>Gen</span>
          </div>
          <div style={{
            fontSize: 14, color: "rgba(255,255,255,0.6)",
            fontFamily: "'Sora', sans-serif",
          }}>
            {useBackup ? "Enter a backup code" : "Two-Factor Verification"}
          </div>
        </div>

        <form onSubmit={handleVerify}>
          <input
            className="login-input"
            type="text"
            value={code}
            onChange={e => setCode(
              useBackup
                ? e.target.value.toUpperCase()
                : e.target.value.replace(/\D/g, "")
            )}
            placeholder={useBackup ? "Backup code" : "6-digit code"}
            maxLength={useBackup ? 8 : 6}
            autoFocus
            style={{
              letterSpacing: useBackup ? 3 : 8,
              textAlign: "center",
              fontSize: useBackup ? 16 : 22,
              fontFamily: "'Space Grotesk', monospace",
            }}
          />

          {error && <div className="login-error">{error}</div>}

          <button className="login-btn" type="submit"
            disabled={loading || (useBackup ? code.length < 6 : code.length < 6)}>
            {loading ? "Verifying\u2026" : "Verify"}
          </button>
        </form>

        <div style={{ textAlign: "center", marginTop: 16 }}>
          <button
            onClick={() => { setUseBackup(!useBackup); setCode(""); setError(""); }}
            style={{
              background: "none", border: "none", cursor: "pointer",
              color: "var(--teal)", fontSize: 13, fontFamily: "'Sora', sans-serif",
              textDecoration: "underline", opacity: 0.8,
            }}
          >
            {useBackup ? "Use authenticator code instead" : "Use a backup code"}
          </button>
        </div>
      </div>
    </div>
  );
}
