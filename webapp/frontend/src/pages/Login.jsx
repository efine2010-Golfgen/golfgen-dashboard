import { useState, useEffect } from "react";
import { api } from "../lib/api";

const SSO_ERRORS = {
  google_denied: "Google sign-in was cancelled or denied.",
  token_exchange_failed: "Google sign-in failed — please try again.",
  userinfo_failed: "Could not retrieve your Google account info.",
  not_authorized: "Your Google account is not authorized for this dashboard. Contact your admin.",
};

/* ── WebAuthn Helpers ─────────────────────────────────────── */
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
  const result = { id: cred.id, rawId: _bytesToB64url(cred.rawId), type: cred.type, response: {} };
  if (resp.clientDataJSON) result.response.clientDataJSON = _bytesToB64url(resp.clientDataJSON);
  if (resp.authenticatorData) result.response.authenticatorData = _bytesToB64url(resp.authenticatorData);
  if (resp.signature) result.response.signature = _bytesToB64url(resp.signature);
  if (resp.userHandle) result.response.userHandle = _bytesToB64url(resp.userHandle);
  return result;
}

export default function Login({ onLogin }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [passkeyLoading, setPasskeyLoading] = useState(false);

  const supportsWebAuthn = typeof window !== "undefined" && !!window.PublicKeyCredential;

  // Check for SSO error in URL on mount
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const authError = params.get("auth_error");
    if (authError && SSO_ERRORS[authError]) {
      setError(SSO_ERRORS[authError]);
      window.history.replaceState({}, "", window.location.pathname);
    }
  }, []);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      await api.login(email, password);
      onLogin();
    } catch {
      setError("Invalid email or password.");
    } finally {
      setLoading(false);
    }
  };

  const handlePasskeyLogin = async () => {
    setError(""); setPasskeyLoading(true);
    try {
      // 1. Get authentication options from server
      const options = await api.passkeyLoginOptions(email || "");

      // 2. Prepare options for browser API
      const publicKey = {
        ...options.publicKey || options,
        challenge: _b64urlToBytes((options.publicKey || options).challenge),
      };
      if (publicKey.allowCredentials) {
        publicKey.allowCredentials = publicKey.allowCredentials.map(c => ({
          ...c, id: _b64urlToBytes(c.id),
        }));
      }

      // 3. Call browser WebAuthn API (triggers biometric/PIN prompt)
      const credential = await navigator.credentials.get({ publicKey });

      // 4. Send to server for verification
      const credJSON = _credentialToJSON(credential);
      const result = await api.passkeyLoginVerify(credJSON, email || "");
      if (result.ok) {
        onLogin();
      } else {
        setError(result.detail || "Passkey authentication failed.");
      }
    } catch (e) {
      if (e.name === "NotAllowedError") setError("Passkey sign-in was cancelled.");
      else setError(e.message || "Passkey authentication failed.");
    }
    setPasskeyLoading(false);
  };

  return (
    <div className="login-page">
      <div className="login-card">
        <div className="brand-logo" style={{ textAlign: "center", marginBottom: 8 }}>
          <span className="golf">Golf</span>
          <span className="gen">Gen</span>
        </div>
        <p style={{ color: "var(--muted)", fontSize: 14, textAlign: "center", marginBottom: 24 }}>Dashboard Login</p>
        <form onSubmit={handleSubmit}>
          <input
            type="email"
            className="login-input"
            placeholder="Email address"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoFocus
            autoComplete="email webauthn"
          />
          <input
            type="password"
            className="login-input"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            style={{ marginTop: 12 }}
            autoComplete="current-password"
          />
          {error && <div className="login-error">{error}</div>}
          <button type="submit" className="login-btn" disabled={loading || !email || !password}>
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </form>
        <div style={{ textAlign: "center", margin: "16px 0 8px", color: "var(--muted)", fontSize: 13 }}>or</div>
        <button
          className="login-btn"
          style={{ background: "#4285f4", border: "none", display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}
          onClick={() => { window.location.href = "/api/auth/google/login"; }}
        >
          <svg width="18" height="18" viewBox="0 0 48 48"><path fill="#fff" d="M44.5 20H24v8.5h11.8C34.7 33.9 30.1 37 24 37c-7.2 0-13-5.8-13-13s5.8-13 13-13c3.1 0 5.9 1.1 8.1 2.9l6.4-6.4C34.6 4.1 29.6 2 24 2 11.8 2 2 11.8 2 24s9.8 22 22 22c11 0 21-8 21-22 0-1.3-.2-2.7-.5-4z"/></svg>
          Sign in with Google
        </button>
        {supportsWebAuthn && (
          <button
            className="login-btn"
            style={{
              marginTop: 8, background: "transparent",
              border: "1.5px solid var(--teal, #2ecfaa)",
              color: "var(--teal, #2ecfaa)",
              display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
            }}
            onClick={handlePasskeyLogin}
            disabled={passkeyLoading}
          >
            <span style={{ fontSize: 18 }}>🔑</span>
            {passkeyLoading ? "Authenticating…" : "Sign in with Passkey"}
          </button>
        )}
      </div>
    </div>
  );
}
