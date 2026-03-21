import { useState, useRef, useEffect, useCallback } from "react";

const API_BASE = import.meta.env.VITE_API_URL || "";

// ── Helpers ──────────────────────────────────────────────────────────────────

function fmtBytes(b) {
  if (!b) return "";
  if (b < 1024) return `${b} B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`;
  return `${(b / (1024 * 1024)).toFixed(1)} MB`;
}

function fmtDate(iso) {
  if (!iso) return "";
  try {
    return new Date(iso).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch { return iso.slice(0, 10); }
}

const WRITE_LABELS = {
  update_cogs:              { label: "Update COGS", endpoint: "/api/writes/cogs",                  color: "#2e6fbb" },
  set_ad_budget:            { label: "Set Ad Budget Target", endpoint: "/api/writes/ad-budget",    color: "#e8821e" },
  set_inventory_threshold:  { label: "Set Reorder Threshold", endpoint: "/api/writes/inventory-threshold", color: "#22c55e" },
  add_price_note:           { label: "Add Price Note", endpoint: "/api/writes/price-note",         color: "#a855f7" },
};

function writeActionSummary(action) {
  const intent = action._intent;
  if (intent === "update_cogs")
    return `Set COGS for ${action.asin || "?"} to $${Number(action.new_cogs || 0).toFixed(2)}`;
  if (intent === "set_ad_budget")
    return `${action.target_acos ? `Target ACOS: ${action.target_acos}%` : ""}${action.daily_budget ? ` | Daily budget: $${action.daily_budget}` : ""} for ${action.identifier || "?"}`;
  if (intent === "set_inventory_threshold")
    return `Reorder at ${action.reorder_point} units for ${action.asin || action.sku || "?"}`;
  if (intent === "add_price_note")
    return `Note for ${action.asin || action.sku || "?"}: "${(action.note || "").slice(0, 80)}"`;
  return JSON.stringify(action);
}

// ── Main Component ────────────────────────────────────────────────────────────

export default function AskClaude({ activeTab, division, customer }) {
  const [open, setOpen]       = useState(false);
  const [tab, setTab]         = useState("ask");   // "ask" | "docs"
  const [query, setQuery]     = useState("");
  const [loading, setLoading] = useState(false);
  const [answer, setAnswer]   = useState(null);
  const [error, setError]     = useState(null);
  const [history, setHistory] = useState([]);

  // Write confirmation
  const [pendingAction, setPendingAction] = useState(null);
  const [writeLoading, setWriteLoading]   = useState(false);
  const [writeResult, setWriteResult]     = useState(null);

  // Documents
  const [docs, setDocs]             = useState([]);
  const [docsLoading, setDocsLoading] = useState(false);
  const [uploadLoading, setUploadLoading] = useState(false);
  const [uploadResult, setUploadResult]   = useState(null);
  const [docTag, setDocTag]           = useState("general");
  const [docDesc, setDocDesc]         = useState("");

  const inputRef  = useRef(null);
  const panelRef  = useRef(null);
  const fileRef   = useRef(null);

  // Close panel on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e) => {
      if (panelRef.current && !panelRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  // Auto-focus input
  useEffect(() => {
    if (open && tab === "ask" && inputRef.current) inputRef.current.focus();
  }, [open, tab]);

  // Load docs when docs tab opens
  useEffect(() => {
    if (open && tab === "docs") fetchDocs();
  }, [open, tab]);

  const fetchDocs = useCallback(async () => {
    setDocsLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/documents/`, { credentials: "include" });
      if (res.ok) {
        const data = await res.json();
        setDocs(data.documents || []);
      }
    } catch {}
    setDocsLoading(false);
  }, []);

  // ── Ask ────────────────────────────────────────────────────────────────────

  const ask = async () => {
    const q = query.trim();
    if (!q || loading) return;
    setLoading(true);
    setError(null);
    setAnswer(null);
    setPendingAction(null);
    setWriteResult(null);
    try {
      const res = await fetch(`${API_BASE}/api/ask-claude`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          question: q,
          active_tab: activeTab || "sales",
          division: division || null,
          customer: customer || null,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.answer || data.detail || "Request failed");
      } else if (data.write_action) {
        // Backend detected a write intent — show confirmation
        setPendingAction(data.write_action);
      } else {
        setAnswer(data.answer);
        setHistory((h) => [
          { q, a: data.answer, tab: data.tab_context, ts: new Date() },
          ...h,
        ].slice(0, 20));
      }
    } catch {
      setError("Could not reach the server. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const confirmWrite = async () => {
    if (!pendingAction) return;
    const meta = WRITE_LABELS[pendingAction._intent];
    if (!meta) return;
    setWriteLoading(true);
    setWriteResult(null);
    // Build payload — strip _intent key
    const { _intent, ...payload } = pendingAction;
    try {
      const res = await fetch(`${API_BASE}${meta.endpoint}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (res.ok && data.ok) {
        setWriteResult({ ok: true, msg: `✓ ${meta.label} saved successfully.` });
        setPendingAction(null);
        setQuery("");
      } else {
        setWriteResult({ ok: false, msg: data.error || "Write failed." });
      }
    } catch {
      setWriteResult({ ok: false, msg: "Network error. Please try again." });
    }
    setWriteLoading(false);
  };

  const cancelWrite = () => {
    setPendingAction(null);
    setWriteResult(null);
  };

  const handleKey = (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); ask(); }
    if (e.key === "Escape") setOpen(false);
  };

  // ── Upload ─────────────────────────────────────────────────────────────────

  const handleUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploadLoading(true);
    setUploadResult(null);
    const fd = new FormData();
    fd.append("file", file);
    fd.append("tag", docTag || "general");
    fd.append("description", docDesc || "");
    try {
      const res = await fetch(`${API_BASE}/api/documents/upload`, {
        method: "POST",
        credentials: "include",
        body: fd,
      });
      const data = await res.json();
      if (res.ok && data.ok) {
        setUploadResult({ ok: true, msg: `✓ "${file.name}" uploaded (${fmtBytes(data.sizeBytes)})` });
        setDocDesc("");
        fetchDocs();
      } else {
        setUploadResult({ ok: false, msg: data.error || "Upload failed." });
      }
    } catch {
      setUploadResult({ ok: false, msg: "Network error." });
    }
    setUploadLoading(false);
    // Reset file input so same file can be re-uploaded
    if (fileRef.current) fileRef.current.value = "";
  };

  const handleDownload = async (doc) => {
    try {
      const res = await fetch(`${API_BASE}/api/documents/${doc.id}/download`, {
        credentials: "include",
      });
      if (!res.ok) { alert("Download failed."); return; }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = doc.name;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      alert("Download error.");
    }
  };

  // ── Styles ─────────────────────────────────────────────────────────────────

  const SG = (s) => ({ fontFamily: "'Space Grotesk', monospace", fontSize: s });

  const tabBtn = (key) => ({
    ...SG(10), fontWeight: 700,
    padding: "3px 10px", borderRadius: 99, border: "none", cursor: "pointer",
    background: tab === key ? "var(--accent)" : "transparent",
    color: tab === key ? "#fff" : "var(--txt3)",
    transition: "background .15s",
  });

  /* ── Collapsed trigger ── */
  if (!open) {
    return (
      <button
        onClick={() => setOpen(true)}
        style={{
          display: "flex", alignItems: "center", gap: 6,
          padding: "4px 14px", borderRadius: 20,
          border: "1px solid var(--brd2)", background: "var(--ibg)",
          cursor: "pointer", transition: "border-color .2s",
          ...SG(11), color: "var(--txt3)", fontWeight: 500,
          minWidth: 180, maxWidth: 260, height: 28,
        }}
        onMouseEnter={(e) => (e.currentTarget.style.borderColor = "var(--accent)")}
        onMouseLeave={(e) => (e.currentTarget.style.borderColor = "var(--brd2)")}
      >
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <span>Ask Claude...</span>
      </button>
    );
  }

  /* ── Expanded panel ── */
  return (
    <div ref={panelRef} style={{ position: "relative", zIndex: 900 }}>

      {/* ── Input row ── */}
      <div style={{
        display: "flex", alignItems: "center", gap: 6,
        padding: "3px 4px 3px 12px", borderRadius: 20,
        border: "1px solid var(--accent)", background: "var(--ibg)",
        minWidth: 260, maxWidth: 360, height: 28,
      }}>
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
          <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <input
          ref={inputRef}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKey}
          placeholder={tab === "ask" ? "Ask about your data..." : "Search docs..."}
          style={{ flex: 1, border: "none", outline: "none", background: "transparent", ...SG(11), color: "var(--txt)", fontWeight: 500, padding: 0, margin: 0 }}
        />
        {/* Tab switcher inside the pill */}
        <div style={{ display: "flex", gap: 2, marginRight: 2 }}>
          <button onClick={() => setTab("ask")} style={tabBtn("ask")}>Ask</button>
          <button onClick={() => setTab("docs")} style={tabBtn("docs")}>Docs</button>
        </div>
        {tab === "ask" && (
          loading ? (
            <div style={{ width: 20, height: 20, display: "flex", alignItems: "center", justifyContent: "center" }}>
              <div style={{ width: 14, height: 14, border: "2px solid var(--brd2)", borderTopColor: "var(--accent)", borderRadius: "50%", animation: "spin .8s linear infinite" }} />
            </div>
          ) : (
            <button onClick={ask} disabled={!query.trim()} style={{ width: 22, height: 22, borderRadius: "50%", border: "none", cursor: query.trim() ? "pointer" : "default", background: query.trim() ? "var(--accent)" : "var(--brd2)", display: "flex", alignItems: "center", justifyContent: "center", transition: "background .2s", flexShrink: 0 }}>
              <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round"><line x1="5" y1="12" x2="19" y2="12" /><polyline points="12 5 19 12 12 19" /></svg>
            </button>
          )
        )}
      </div>

      {/* ── Dropdown panel ── */}
      <div style={{
        position: "absolute", top: 34, left: 0,
        minWidth: 340, maxWidth: 460, width: "max-content",
        background: "var(--card)", border: "1px solid var(--brd2)",
        borderRadius: 10, boxShadow: "0 8px 32px rgba(0,0,0,.35)",
        maxHeight: 420, overflowY: "auto",
        padding: 12,
        display: (answer || error || history.length > 0 || pendingAction || writeResult || tab === "docs") ? "block" : "none",
      }}>

        {/* ══ ASK TAB ══ */}
        {tab === "ask" && (
          <>
            {/* Write confirmation */}
            {pendingAction && !writeResult && (() => {
              const meta = WRITE_LABELS[pendingAction._intent] || {};
              return (
                <div style={{ marginBottom: 10 }}>
                  <div style={{ ...SG(10), color: meta.color || "var(--accent)", fontWeight: 700, marginBottom: 6, display: "flex", alignItems: "center", gap: 5 }}>
                    <span>✏️</span>
                    <span>{meta.label || "Write Action"}</span>
                  </div>
                  <div style={{ ...SG(11), color: "var(--txt)", lineHeight: 1.5, background: "var(--surf)", borderRadius: 6, padding: "6px 10px", marginBottom: 8, border: `1px solid ${meta.color || "var(--brd2)"}20` }}>
                    {writeActionSummary(pendingAction)}
                  </div>
                  <div style={{ display: "flex", gap: 6 }}>
                    <button
                      onClick={confirmWrite}
                      disabled={writeLoading}
                      style={{ ...SG(11), fontWeight: 700, padding: "4px 14px", borderRadius: 99, border: "none", cursor: "pointer", background: meta.color || "var(--accent)", color: "#fff", opacity: writeLoading ? 0.6 : 1 }}
                    >
                      {writeLoading ? "Saving…" : "Confirm"}
                    </button>
                    <button
                      onClick={cancelWrite}
                      style={{ ...SG(11), fontWeight: 600, padding: "4px 12px", borderRadius: 99, border: "1px solid var(--brd2)", cursor: "pointer", background: "transparent", color: "var(--txt3)" }}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              );
            })()}

            {/* Write result */}
            {writeResult && (
              <div style={{ ...SG(11), color: writeResult.ok ? "#22c55e" : "#ef4444", lineHeight: 1.5, marginBottom: 8 }}>
                {writeResult.msg}
              </div>
            )}

            {/* Regular answer */}
            {error && <div style={{ ...SG(11), color: "#ef4444", lineHeight: 1.5, marginBottom: 8 }}>{error}</div>}
            {answer && (
              <div style={{ ...SG(11), color: "var(--txt)", lineHeight: 1.6, whiteSpace: "pre-wrap", marginBottom: history.length > 1 ? 10 : 0 }}>
                {answer}
              </div>
            )}

            {/* History */}
            {history.length > 1 && (
              <>
                <div style={{ height: 1, background: "var(--brd2)", margin: "8px 0" }} />
                <div style={{ ...SG(9), color: "var(--txt3)", fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 6 }}>Previous</div>
                {history.slice(1, 5).map((h, i) => (
                  <div key={i} style={{ marginBottom: 8 }}>
                    <div style={{ ...SG(10), color: "var(--accent)", fontWeight: 600, marginBottom: 2 }}>{h.q}</div>
                    <div style={{ ...SG(10), color: "var(--txt3)", lineHeight: 1.4, whiteSpace: "pre-wrap", maxHeight: 60, overflow: "hidden" }}>{h.a}</div>
                  </div>
                ))}
              </>
            )}
          </>
        )}

        {/* ══ DOCS TAB ══ */}
        {tab === "docs" && (
          <div>
            {/* Upload area */}
            <div style={{ marginBottom: 12 }}>
              <div style={{ ...SG(9), color: "var(--txt3)", fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 6 }}>Upload Document</div>
              <div style={{ display: "flex", gap: 6, marginBottom: 6 }}>
                <input
                  type="text"
                  placeholder="Tag (e.g. invoice, PO, 3PL)"
                  value={docTag}
                  onChange={(e) => setDocTag(e.target.value)}
                  style={{ flex: 1, ...SG(11), padding: "3px 8px", borderRadius: 6, border: "1px solid var(--brd2)", background: "var(--ibg)", color: "var(--txt)", outline: "none" }}
                />
              </div>
              <input
                type="text"
                placeholder="Description (optional)"
                value={docDesc}
                onChange={(e) => setDocDesc(e.target.value)}
                style={{ width: "100%", ...SG(11), padding: "3px 8px", borderRadius: 6, border: "1px solid var(--brd2)", background: "var(--ibg)", color: "var(--txt)", outline: "none", marginBottom: 6, boxSizing: "border-box" }}
              />
              <label style={{ display: "inline-flex", alignItems: "center", gap: 5, padding: "4px 14px", borderRadius: 99, border: "1px dashed var(--brd2)", cursor: "pointer", ...SG(11), color: "var(--txt3)", background: "var(--surf)", transition: "border-color .2s" }}
                onMouseEnter={(e) => e.currentTarget.style.borderColor = "var(--accent)"}
                onMouseLeave={(e) => e.currentTarget.style.borderColor = "var(--brd2)"}
              >
                {uploadLoading ? (
                  <><div style={{ width: 12, height: 12, border: "2px solid var(--brd2)", borderTopColor: "var(--accent)", borderRadius: "50%", animation: "spin .8s linear infinite" }} /><span>Uploading…</span></>
                ) : (
                  <><span>📎</span><span>Choose file…</span></>
                )}
                <input ref={fileRef} type="file" style={{ display: "none" }} onChange={handleUpload} disabled={uploadLoading} />
              </label>
              {uploadResult && (
                <div style={{ ...SG(10), color: uploadResult.ok ? "#22c55e" : "#ef4444", marginTop: 6, lineHeight: 1.4 }}>
                  {uploadResult.msg}
                </div>
              )}
            </div>

            {/* Divider */}
            <div style={{ height: 1, background: "var(--brd2)", margin: "8px 0 10px" }} />

            {/* Documents list */}
            <div style={{ ...SG(9), color: "var(--txt3)", fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 6 }}>
              Uploaded Documents {docs.length > 0 ? `(${docs.length})` : ""}
            </div>

            {docsLoading && (
              <div style={{ ...SG(11), color: "var(--txt3)" }}>Loading…</div>
            )}

            {!docsLoading && docs.length === 0 && (
              <div style={{ ...SG(11), color: "var(--txt3)" }}>No documents uploaded yet.</div>
            )}

            {docs.map((doc) => (
              <div key={doc.id} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6, padding: "5px 8px", borderRadius: 6, background: "var(--surf)", border: "1px solid var(--brd)" }}>
                <span style={{ fontSize: 14 }}>
                  {/pdf/i.test(doc.contentType) ? "📄" : /image/i.test(doc.contentType) ? "🖼️" : /spreadsheet|excel|csv/i.test(doc.contentType || doc.name) ? "📊" : /word|docx/i.test(doc.contentType || doc.name) ? "📝" : "📎"}
                </span>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ ...SG(11), color: "var(--txt)", fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{doc.name}</div>
                  <div style={{ ...SG(9), color: "var(--txt3)" }}>
                    {doc.tag && <span style={{ marginRight: 6, background: "var(--brd)", borderRadius: 4, padding: "1px 5px" }}>{doc.tag}</span>}
                    {fmtBytes(doc.sizeBytes)} · {fmtDate(doc.uploadedAt)}
                  </div>
                </div>
                <button
                  onClick={() => handleDownload(doc)}
                  title="Download"
                  style={{ padding: "3px 8px", borderRadius: 6, border: "1px solid var(--brd2)", background: "transparent", cursor: "pointer", ...SG(10), color: "var(--accent)", fontWeight: 600, flexShrink: 0 }}
                >
                  ↓
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}
