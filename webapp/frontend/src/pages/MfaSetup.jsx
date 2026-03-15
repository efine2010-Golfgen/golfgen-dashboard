import { useState, useEffect } from "react";
import { api } from "../lib/api";

const API = "";

/* ── Passkey WebAuthn Helpers ───────────────────────────── */
function _b64urlToBytes(b64url) {
  const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/");
  const pad = b64.length % 4 === 0 ? "" : "=".repeat(4 - (b64.length % 4));
  const bin = atob(b64 + pad);
  return Uint8Array.from(bin, c => c.charCodeAt(0));
}

function _bytesToB64url(bytes) {
  const bin = String.fromCharCode(...new Uint8Array(bytes));
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function _credentialToJSON(cred) {
  const resp = cred.response;
  const result = {
    id: cred.id,
    rawId: _bytesToB64url(cred.rawId),
    type: cred.type,
    response: {},
  };
  if (resp.clientDataJSON) result.response.clientDataJSON = _bytesToB64url(resp.clientDataJSON);
  if (resp.attestationObject) result.response.attestationObject = _bytesToB64url(resp.attestationObject);
  if (resp.authenticatorData) result.response.authenticatorData = _bytesToB64url(resp.authenticatorData);
  if (resp.signature) result.response.signature = _bytesToB64url(resp.signature);
  if (resp.userHandle) result.response.userHandle = _bytesToB64url(resp.userHandle);
  return result;
}


/* ── Passkey Management Section ─────────────────────────── */
function PasskeySection() {
  const [passkeys, setPasskeys] = useState([]);
  const [loading, setLoading] = useState(true);
  const [registering, setRegistering] = useState(false);
  const [deviceName, setDeviceName] = useState("");
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  const loadPasskeys = async () => {
    try {
      const res = await api.passkeyList();
      setPasskeys(res.passkeys || []);
    } catch { }
    setLoading(false);
  };

  useEffect(() => { loadPasskeys(); }, []);

  const handleRegister = async () => {
    setError(""); setSuccess(""); setRegistering(true);
    try {
      // 1. Get registration options from server
      const options = await api.passkeyRegisterOpts();

      // 2. Convert base64url fields to ArrayBuffers for the browser API
      const publicKey = {
        ...options.publicKey || options,
        challenge: _b64urlToBytes((options.publicKey || options).challenge),
        user: {
          ...(options.publicKey || options).user,
          id: _b64urlToBytes((options.publicKey || options).user.id),
        },
      };
      if (publicKey.excludeCredentials) {
        publicKey.excludeCredentials = publicKey.excludeCredentials.map(c => ({
          ...c, id: _b64urlToBytes(c.id),
        }));
      }

      // 3. Call browser WebAuthn API
      const credential = await navigator.credentials.create({ publicKey });

      // 4. Send credential to server for verification
      const credJSON = _credentialToJSON(credential);
      const result = await api.passkeyRegisterDone(credJSON, deviceName || "My Passkey");
      if (result.ok) {
        setSuccess("Passkey registered successfully!");
        setDeviceName("");
        loadPasskeys();
      } else {
        setError(result.detail || result.message || "Registration failed");
      }
    } catch (e) {
      if (e.name === "NotAllowedError") setError("Registration was cancelled.");
      else setError(e.message || "Registration failed");
    }
    setRegistering(false);
  };

  const handleDelete = async (id) => {
    if (!confirm("Remove this passkey? You won't be able to log in with it anymore.")) return;
    try {
      await api.passkeyDelete(id);
      loadPasskeys();
    } catch (e) { setError(e.message); }
  };

  const supportsWebAuthn = !!window.PublicKeyCredential;

  return (
    <div className="section-card" style={{ marginTop: 24 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
        <div style={{
          width: 40, height: 40, borderRadius: "50%",
          background: passkeys.length > 0 ? "var(--teal)" : "var(--border)",
          display: "flex", alignItems: "center", justifyContent: "center",
          color: passkeys.length > 0 ? "#fff" : "var(--muted)", fontSize: 18,
        }}>🔑</div>
        <div>
          <div style={{ fontWeight: 600, fontSize: 16, color: "var(--navy)" }}>Passkeys</div>
          <div style={{ fontSize: 13, color: "var(--muted)" }}>
            Sign in with Face ID, Touch ID, or Windows Hello — no password needed
          </div>
        </div>
      </div>

      {!supportsWebAuthn && (
        <div style={{ padding: "10px 14px", background: "#FFF8F0", border: "1px solid var(--orange)", borderRadius: 8, marginBottom: 12, fontSize: 13, color: "var(--orange)" }}>
          Your browser doesn't support passkeys. Try Chrome, Safari, or Edge on a modern device.
        </div>
      )}

      {/* Existing passkeys */}
      {passkeys.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          {passkeys.map(pk => (
            <div key={pk.id} style={{
              display: "flex", alignItems: "center", justifyContent: "space-between",
              padding: "10px 14px", borderRadius: 8,
              border: "1px solid var(--border)", marginBottom: 6,
              background: "var(--off-white, rgba(0,0,0,.03))",
            }}>
              <div>
                <div style={{ fontSize: 14, fontWeight: 600, color: "var(--navy)" }}>
                  🔐 {pk.device_name || "Passkey"}
                </div>
                <div style={{ fontSize: 12, color: "var(--muted)" }}>
                  Added {new Date(pk.created_at).toLocaleDateString()}
                </div>
              </div>
              <button onClick={() => handleDelete(pk.id)} style={{
                padding: "4px 12px", borderRadius: 6, fontSize: 12, fontWeight: 600,
                border: "1px solid var(--orange)", background: "transparent",
                color: "var(--orange)", cursor: "pointer",
              }}>Remove</button>
            </div>
          ))}
        </div>
      )}

      {/* Register new passkey */}
      {supportsWebAuthn && (
        <div>
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8 }}>
            <input
              placeholder="Device name (e.g. MacBook, iPhone)"
              value={deviceName}
              onChange={e => setDeviceName(e.target.value)}
              className="login-input"
              style={{ flex: 1, margin: 0 }}
            />
            <button
              className="login-btn"
              onClick={handleRegister}
              disabled={registering}
              style={{ whiteSpace: "nowrap", margin: 0, flexShrink: 0 }}
            >
              {registering ? "Registering…" : "+ Add Passkey"}
            </button>
          </div>
          <div style={{ fontSize: 12, color: "var(--muted)" }}>
            Your device will prompt you to verify with biometrics or a PIN.
          </div>
        </div>
      )}

      {error && <div className="login-error" style={{ marginTop: 10 }}>{error}</div>}
      {success && <div style={{ marginTop: 10, padding: "8px 12px", borderRadius: 8, background: "rgba(46,207,170,.1)", border: "1px solid rgba(46,207,170,.3)", fontSize: 13, color: "#2ecfaa" }}>{success}</div>}
    </div>
  );
}


export default function MfaSetup() {
  const [status, setStatus] = useState(null);
  const [step, setStep] = useState("loading");
  const [qrData, setQrData] = useState(null);
  const [code, setCode] = useState("");
  const [backupCodes, setBackupCodes] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [disableCode, setDisableCode] = useState("");
  const [showDisable, setShowDisable] = useState(false);
  const [codesCount, setCodesCount] = useState(null);

  useEffect(() => { loadStatus(); }, []);

  async function loadStatus() {
    try {
      const res = await fetch(`${API}/api/mfa/setup`, { credentials: "include" });
      const data = await res.json();
      setStatus(data);
      if (data.mfa_enabled) {
        const cr = await fetch(`${API}/api/mfa/backup-codes/count`, { credentials: "include" });
        const cd = await cr.json();
        setCodesCount(cd.count);
      }
      setStep("overview");
    } catch { setStep("overview"); }
  }

  async function beginSetup() {
    setError("");
    setLoading(true);
    try {
      const res = await fetch(`${API}/api/mfa/setup/begin`, {
        method: "POST", credentials: "include",
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed");
      setQrData(data);
      setStep("scanning");
    } catch (e) { setError(e.message); }
    setLoading(false);
  }

  async function confirmSetup() {
    setError("");
    setLoading(true);
    try {
      const res = await fetch(`${API}/api/mfa/setup/confirm`, {
        method: "POST", credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ secret: qrData.secret, code }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed");
      setBackupCodes(data.backup_codes);
      setStep("done");
    } catch (e) { setError(e.message); }
    setLoading(false);
  }

  async function handleDisable() {
    setError("");
    setLoading(true);
    try {
      const res = await fetch(`${API}/api/mfa/disable`, {
        method: "POST", credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ code: disableCode }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed");
      setShowDisable(false);
      setDisableCode("");
      loadStatus();
    } catch (e) { setError(e.message); }
    setLoading(false);
  }

  return (
    <div className="page-content" style={{ maxWidth: 600, margin: "0 auto" }}>
      <div className="page-header">
        <h2>Account Security</h2>
        <p style={{ color: "var(--muted)", margin: "4px 0 0" }}>
          Manage passkeys, two-factor authentication, and account security
        </p>
      </div>

      {step === "overview" && status && (
        <div className="section-card">
          <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
            <div style={{
              width: 40, height: 40, borderRadius: "50%",
              background: status.mfa_enabled ? "var(--teal)" : "var(--border)",
              display: "flex", alignItems: "center", justifyContent: "center",
              color: status.mfa_enabled ? "#fff" : "var(--muted)",
              fontSize: 18, fontWeight: 600,
            }}>
              {status.mfa_enabled ? "\u2713" : "\u2715"}
            </div>
            <div>
              <div style={{ fontWeight: 600, fontSize: 16, color: "var(--navy)" }}>
                {status.mfa_enabled ? "MFA is enabled" : "MFA is not enabled"}
              </div>
              {status.enrolled_at && (
                <div style={{ fontSize: 13, color: "var(--muted)" }}>
                  Enrolled {new Date(status.enrolled_at).toLocaleDateString()}
                </div>
              )}
            </div>
          </div>

          {status.mfa_enabled && codesCount !== null && (
            <div style={{
              padding: "12px 16px", background: "var(--off-white)",
              borderRadius: 10, marginBottom: 16, fontSize: 14,
            }}>
              <span style={{ color: "var(--muted)" }}>Backup codes remaining: </span>
              <strong style={{ color: codesCount <= 2 ? "var(--orange)" : "var(--navy)" }}>
                {codesCount}
              </strong>
            </div>
          )}

          {!status.mfa_enabled ? (
            <button className="login-btn" onClick={beginSetup} disabled={loading}
              style={{ width: "100%" }}>
              {loading ? "Setting up\u2026" : "Enable Two-Factor Authentication"}
            </button>
          ) : (
            <div style={{ display: "flex", gap: 10 }}>
              <button className="login-btn" onClick={() => setShowDisable(!showDisable)}
                style={{
                  flex: 1, background: "transparent", color: "var(--orange)",
                  border: "1px solid var(--orange)",
                }}>
                Disable MFA
              </button>
            </div>
          )}

          {showDisable && (
            <div style={{ marginTop: 16 }}>
              <label style={{ fontSize: 13, color: "var(--muted)", display: "block", marginBottom: 6 }}>
                Enter your current authenticator code to disable MFA
              </label>
              <div style={{ display: "flex", gap: 10 }}>
                <input className="login-input" value={disableCode}
                  onChange={e => setDisableCode(e.target.value)}
                  placeholder="6-digit code" maxLength={6}
                  style={{ flex: 1, letterSpacing: 6, textAlign: "center", fontFamily: "'Space Grotesk', monospace" }}
                />
                <button className="login-btn" onClick={handleDisable}
                  disabled={loading || disableCode.length < 6}
                  style={{ background: "var(--orange)", whiteSpace: "nowrap" }}>
                  Confirm
                </button>
              </div>
            </div>
          )}

          {error && <div className="login-error" style={{ marginTop: 12 }}>{error}</div>}
        </div>
      )}

      {step === "scanning" && qrData && (
        <div className="section-card" style={{ textAlign: "center" }}>
          <h3 style={{ fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)", marginBottom: 8 }}>
            Scan with your authenticator app
          </h3>
          <p style={{ fontSize: 14, color: "var(--muted)", marginBottom: 20 }}>
            Open Microsoft Authenticator (or any TOTP app), tap +, and scan this QR code.
          </p>

          <div style={{
            display: "inline-block", padding: 16, background: "#fff",
            borderRadius: 12, border: "1px solid var(--border)",
            boxShadow: "var(--card-shadow)",
          }}>
            <img src={qrData.qr_code} alt="MFA QR Code"
              style={{ width: 200, height: 200, display: "block" }} />
          </div>

          <details style={{ marginTop: 16, textAlign: "left" }}>
            <summary style={{ cursor: "pointer", fontSize: 13, color: "var(--blue)" }}>
              Can't scan? Enter key manually
            </summary>
            <div style={{
              marginTop: 8, padding: "10px 14px", background: "var(--off-white)",
              borderRadius: 8, fontFamily: "'Space Grotesk', monospace",
              fontSize: 14, wordBreak: "break-all", letterSpacing: 2,
            }}>
              {qrData.secret}
            </div>
          </details>

          <div style={{ marginTop: 24, textAlign: "left" }}>
            <label style={{ fontSize: 13, color: "var(--muted)", display: "block", marginBottom: 6 }}>
              Enter the 6-digit code from your app to confirm setup
            </label>
            <div style={{ display: "flex", gap: 10 }}>
              <input className="login-input" value={code}
                onChange={e => setCode(e.target.value.replace(/\D/g, ""))}
                placeholder="000000" maxLength={6}
                style={{ flex: 1, letterSpacing: 8, textAlign: "center", fontSize: 22,
                         fontFamily: "'Space Grotesk', monospace" }}
                onKeyDown={e => e.key === "Enter" && code.length === 6 && confirmSetup()}
              />
              <button className="login-btn" onClick={confirmSetup}
                disabled={loading || code.length < 6}>
                {loading ? "Verifying\u2026" : "Verify"}
              </button>
            </div>
          </div>
          {error && <div className="login-error" style={{ marginTop: 12 }}>{error}</div>}
        </div>
      )}

      {step === "done" && (
        <div className="section-card">
          <div style={{ textAlign: "center", marginBottom: 20 }}>
            <div style={{
              width: 56, height: 56, borderRadius: "50%", background: "var(--teal)",
              display: "inline-flex", alignItems: "center", justifyContent: "center",
              color: "#fff", fontSize: 28, marginBottom: 12,
            }}>{"\u2713"}</div>
            <h3 style={{ fontFamily: "'Space Grotesk', sans-serif", color: "var(--navy)" }}>
              MFA Enabled Successfully
            </h3>
          </div>

          <div style={{
            padding: 16, background: "#FFF8F0",
            border: "1px solid var(--orange)", borderRadius: 10, marginBottom: 16,
          }}>
            <strong style={{ color: "var(--orange)", fontSize: 14 }}>
              Save your backup codes now
            </strong>
            <p style={{ fontSize: 13, color: "var(--navy)", margin: "6px 0 0" }}>
              These codes are single-use and will not be shown again. Store them securely.
            </p>
          </div>

          <div style={{
            display: "grid", gridTemplateColumns: "1fr 1fr",
            gap: 8, marginBottom: 20,
          }}>
            {backupCodes.map((c, i) => (
              <div key={i} style={{
                padding: "8px 12px", background: "var(--off-white)",
                borderRadius: 8, fontFamily: "'Space Grotesk', monospace",
                fontSize: 15, textAlign: "center", letterSpacing: 2,
              }}>{c}</div>
            ))}
          </div>

          <button className="login-btn" style={{ width: "100%" }}
            onClick={() => {
              const text = backupCodes.join("\n");
              navigator.clipboard.writeText(text).catch(() => {});
            }}>
            Copy Codes to Clipboard
          </button>
          <button className="login-btn"
            style={{ width: "100%", marginTop: 10, background: "var(--blue)" }}
            onClick={() => { setStep("overview"); loadStatus(); }}>
            Done
          </button>
        </div>
      )}

      {step === "loading" && (
        <div className="section-card" style={{ textAlign: "center", padding: 40 }}>
          <div className="spinner" />
        </div>
      )}

      {/* ── Passkey Section ── */}
      <PasskeySection />
    </div>
  );
}
