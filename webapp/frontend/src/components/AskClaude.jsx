import { useState, useRef, useEffect } from "react";

const API_BASE = import.meta.env.VITE_API_URL || "";

export default function AskClaude({ activeTab, division, customer }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [answer, setAnswer] = useState(null);
  const [error, setError] = useState(null);
  const [history, setHistory] = useState([]);
  const inputRef = useRef(null);
  const panelRef = useRef(null);

  /* Close panel on outside click */
  useEffect(() => {
    if (!open) return;
    const handler = (e) => {
      if (panelRef.current && !panelRef.current.contains(e.target)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  /* Auto-focus input when panel opens */
  useEffect(() => {
    if (open && inputRef.current) inputRef.current.focus();
  }, [open]);

  const ask = async () => {
    const q = query.trim();
    if (!q || loading) return;
    setLoading(true);
    setError(null);
    setAnswer(null);
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
      } else {
        setAnswer(data.answer);
        setHistory((h) => [
          { q, a: data.answer, tab: data.tab_context, ts: new Date() },
          ...h,
        ].slice(0, 20));
      }
    } catch (e) {
      setError("Could not reach the server. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const handleKey = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      ask();
    }
    if (e.key === "Escape") {
      setOpen(false);
    }
  };

  const SG = (s) => ({ fontFamily: "'Space Grotesk', monospace", fontSize: s });

  /* ── Collapsed: search bar trigger ── */
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
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <span>Ask Claude...</span>
      </button>
    );
  }

  /* ── Expanded: chat panel ── */
  return (
    <div ref={panelRef} style={{ position: "relative", zIndex: 900 }}>
      {/* Search input row */}
      <div style={{
        display: "flex", alignItems: "center", gap: 6,
        padding: "3px 4px 3px 12px", borderRadius: 20,
        border: "1px solid var(--accent)", background: "var(--ibg)",
        minWidth: 260, maxWidth: 360, height: 28,
      }}>
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="var(--accent)" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <input
          ref={inputRef}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKey}
          placeholder="Ask about your data..."
          style={{
            flex: 1, border: "none", outline: "none", background: "transparent",
            ...SG(11), color: "var(--txt)", fontWeight: 500,
            padding: 0, margin: 0,
          }}
        />
        {loading ? (
          <div style={{ width: 20, height: 20, display: "flex", alignItems: "center", justifyContent: "center" }}>
            <div style={{
              width: 14, height: 14, border: "2px solid var(--brd2)",
              borderTopColor: "var(--accent)", borderRadius: "50%",
              animation: "spin .8s linear infinite",
            }} />
          </div>
        ) : (
          <button
            onClick={ask}
            disabled={!query.trim()}
            style={{
              width: 22, height: 22, borderRadius: "50%",
              border: "none", cursor: query.trim() ? "pointer" : "default",
              background: query.trim() ? "var(--accent)" : "var(--brd2)",
              display: "flex", alignItems: "center", justifyContent: "center",
              transition: "background .2s", flexShrink: 0,
            }}
          >
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
              <line x1="5" y1="12" x2="19" y2="12" />
              <polyline points="12 5 19 12 12 19" />
            </svg>
          </button>
        )}
      </div>

      {/* Dropdown results panel */}
      {(answer || error || history.length > 0) && (
        <div style={{
          position: "absolute", top: 34, left: 0, right: 0,
          minWidth: 340, maxWidth: 420, width: "100%",
          background: "var(--card)", border: "1px solid var(--brd2)",
          borderRadius: 10, boxShadow: "0 8px 32px rgba(0,0,0,.35)",
          maxHeight: 380, overflowY: "auto",
          padding: 12,
        }}>
          {/* Current answer */}
          {error && (
            <div style={{ ...SG(11), color: "#ef4444", lineHeight: 1.5, marginBottom: 8 }}>
              {error}
            </div>
          )}
          {answer && (
            <div style={{ ...SG(11), color: "var(--txt)", lineHeight: 1.6, whiteSpace: "pre-wrap", marginBottom: history.length > 1 ? 10 : 0 }}>
              {answer}
            </div>
          )}

          {/* Previous Q&A history (skip first since it's the current answer) */}
          {history.length > 1 && (
            <>
              <div style={{ height: 1, background: "var(--brd2)", margin: "8px 0" }} />
              <div style={{ ...SG(9), color: "var(--txt3)", fontWeight: 700, textTransform: "uppercase", letterSpacing: ".08em", marginBottom: 6 }}>
                Previous
              </div>
              {history.slice(1, 5).map((h, i) => (
                <div key={i} style={{ marginBottom: 8 }}>
                  <div style={{ ...SG(10), color: "var(--accent)", fontWeight: 600, marginBottom: 2 }}>
                    {h.q}
                  </div>
                  <div style={{ ...SG(10), color: "var(--txt3)", lineHeight: 1.4, whiteSpace: "pre-wrap",
                    maxHeight: 60, overflow: "hidden", textOverflow: "ellipsis",
                  }}>
                    {h.a}
                  </div>
                </div>
              ))}
            </>
          )}
        </div>
      )}

      {/* Keyframe for spinner */}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  );
}
