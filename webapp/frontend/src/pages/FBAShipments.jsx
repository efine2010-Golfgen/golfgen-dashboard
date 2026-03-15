import React, { useState, useEffect } from "react";
import { api } from "../lib/api";

/* ── Style helpers (same as Inventory.jsx) ── */
const SG = (x = {}) => ({ fontFamily: "'Space Grotesk',monospace", ...x });
const DM = (x = {}) => ({ fontFamily: "'DM Serif Display',Georgia,serif", ...x });

/* ── Status color map ── */
const S_COLORS = {
  WORKING:    { bg: 'rgba(123,174,208,.15)', text: '#7BAED0', dot: '#7BAED0' },
  SHIPPED:    { bg: 'rgba(232,120,48,.15)',  text: '#E87830', dot: '#E87830' },
  IN_TRANSIT: { bg: 'rgba(232,120,48,.15)',  text: '#E87830', dot: '#E87830' },
  RECEIVING:  { bg: 'rgba(245,183,49,.14)',  text: '#F5B731', dot: '#F5B731' },
  CHECKED_IN: { bg: 'rgba(245,183,49,.14)',  text: '#F5B731', dot: '#F5B731' },
  CLOSED:     { bg: 'rgba(46,207,170,.13)',  text: '#2ECFAA', dot: '#2ECFAA' },
  CANCELLED:  { bg: 'rgba(141,174,200,.08)', text: '#4d6d8a', dot: '#4d6d8a' },
  ERROR:      { bg: 'rgba(248,113,113,.14)', text: '#f87171', dot: '#f87171' },
};
const STATUS_ORDER = ["IN_TRANSIT", "SHIPPED", "RECEIVING", "CHECKED_IN", "WORKING", "CLOSED", "CANCELLED"];
const STATUS_LABELS = { IN_TRANSIT: "In Transit", SHIPPED: "Shipped", RECEIVING: "Receiving", CHECKED_IN: "Checked In", WORKING: "Working", CLOSED: "Closed", CANCELLED: "Cancelled", ERROR: "Error" };

const PIPE_STATUSES = [
  { key: "WORKING",   label: "Working",   sub: "Created, not shipped", color: "#7BAED0", ring: "idle" },
  { key: "SHIPPED",   label: "Shipped",   sub: "In transit to FC",    color: "#E87830", ring: "active" },
  { key: "RECEIVING", label: "Receiving", sub: "FC checking in",      color: "#F5B731", ring: "warn" },
  { key: "CLOSED",    label: "Closed",    sub: "Fully received",      color: "#2ECFAA", ring: "done" },
  { key: "ERROR",     label: "Error",     sub: "Requires action",     color: "#f87171", ring: "err" },
];

const INV_VIEWS = [
  { key: "golfgen-inventory", path: "/golfgen-inventory", label: "GolfGen Inventory" },
  { key: "inventory",         path: "/inventory",         label: "Amazon Inventory" },
  { key: "fba-shipments",     path: "/fba-shipments",     label: "Shipments to FBA" },
  { key: "stranded",          path: "/stranded",          label: "Stranded & Suppressed" },
  { key: "inventory-ledger",  path: "/inventory-ledger",  label: "Inventory Ledger" },
];

/* ── Small components ── */
function StatusPill({ status }) {
  const c = S_COLORS[status] || S_COLORS.WORKING;
  const label = STATUS_LABELS[status] || status;
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 8px', borderRadius: 6, background: c.bg, ...SG({ fontSize: 9, fontWeight: 700, color: c.text, whiteSpace: 'nowrap' }) }}>
      <span style={{ width: 5, height: 5, borderRadius: '50%', background: c.dot, flexShrink: 0, ...(["SHIPPED","IN_TRANSIT","RECEIVING","CHECKED_IN"].includes(status) ? { animation: 'pulse 2s infinite' } : {}) }} />
      {label}
    </span>
  );
}

function RecvBar({ sent, recv }) {
  if (!recv || !sent) return <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>—</span>;
  const pct = Math.min(100, Math.round(recv / sent * 100));
  const col = pct >= 99 ? '#2ECFAA' : pct >= 90 ? '#F5B731' : '#f87171';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
      <div style={{ flex: 1, height: 5, borderRadius: 3, background: 'var(--brd)', overflow: 'hidden', minWidth: 32 }}>
        <div style={{ height: '100%', borderRadius: 3, background: col, width: `${pct}%`, transition: 'width .6s' }} />
      </div>
      <span style={{ ...SG({ fontSize: 9, fontWeight: 700, color: col, minWidth: 28, textAlign: 'right' }) }}>{pct}%</span>
    </div>
  );
}

function Card({ children, style }) {
  return <div style={{ background: 'var(--surf)', border: '1px solid var(--brd)', borderRadius: 13, overflow: 'hidden', transition: 'background .3s', marginBottom: 14, ...style }}>{children}</div>;
}
function CardHdr({ title, children }) {
  return (
    <div style={{ padding: '11px 16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid var(--brd)', flexWrap: 'wrap', gap: 6 }}>
      <span style={{ fontSize: 12, fontWeight: 700, color: 'var(--txt)' }}>{title}</span>
      {children && <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>{children}</div>}
    </div>
  );
}
function SecDiv({ label }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 10, margin: '20px 0 12px' }}>
      <span style={{ ...SG({ fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.14em', color: 'var(--txt3)', whiteSpace: 'nowrap' }) }}>{label}</span>
      <div style={{ flex: 1, height: 1, background: 'var(--brd)' }} />
    </div>
  );
}
function Badge({ text, type = "ok" }) {
  const colors = { ok: { bg: 'rgba(46,207,170,.14)', color: '#2ECFAA' }, warn: { bg: 'rgba(245,183,49,.14)', color: '#F5B731' }, risk: { bg: 'rgba(248,113,113,.14)', color: '#f87171' }, blue: { bg: 'rgba(123,174,208,.14)', color: '#7BAED0' } };
  const c = colors[type] || colors.ok;
  return <span style={{ ...SG({ fontSize: 9, padding: '2px 8px', borderRadius: 99, fontWeight: 700, background: c.bg, color: c.color }) }}>{text}</span>;
}
function MiniMetric({ label, value, valueColor, note, noteColor, last }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '8px 14px', borderBottom: last ? 'none' : '1px solid var(--brd)' }}>
      <div>
        <div style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', textTransform: 'uppercase', letterSpacing: '.07em' }) }}>{label}</div>
        <div style={{ ...DM({ fontSize: 15, lineHeight: 1, color: valueColor || 'var(--txt2)' }) }}>{value}</div>
      </div>
      {note && <span style={{ ...SG({ fontSize: 8, color: noteColor || 'var(--txt3)' }) }}>{note}</span>}
    </div>
  );
}

/* ══════════════════════════════════════════════════════════════════════════ */

export default function FBAShipments({ filters = {} }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [statusFilter, setStatusFilter] = useState("ALL");
  const [expandedId, setExpandedId] = useState(null);
  const [itemsCache, setItemsCache] = useState({});
  const [loadingItems, setLoadingItems] = useState(null);
  const [search, setSearch] = useState("");

  const load = (refresh = false) => {
    setLoading(true);
    api.fbaShipments(refresh)
      .then(d => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  };
  useEffect(() => load(), []);

  const handleSync = async () => {
    setSyncing(true);
    try {
      const res = await api.fbaShipmentsSync();
      if (res.ok) load(false);
      else alert("Sync failed: " + (res.error || "Unknown error"));
    } catch (err) { alert("Sync failed: " + err.message); }
    setSyncing(false);
  };

  const toggleExpand = async (shipmentId) => {
    if (expandedId === shipmentId) { setExpandedId(null); return; }
    setExpandedId(shipmentId);
    if (!itemsCache[shipmentId]) {
      setLoadingItems(shipmentId);
      try {
        const res = await api.fbaShipmentItems(shipmentId);
        setItemsCache(prev => ({ ...prev, [shipmentId]: res.items || [] }));
      } catch { setItemsCache(prev => ({ ...prev, [shipmentId]: [] })); }
      setLoadingItems(null);
    }
  };

  if (loading) return <div className="loading"><div className="spinner" /> Loading Shipments…</div>;

  const shipments = data?.shipments || [];
  const currentPath = typeof window !== "undefined" ? window.location.pathname : "/fba-shipments";

  /* ── Computed KPIs ── */
  const statusCounts = {};
  shipments.forEach(s => { statusCounts[s.status] = (statusCounts[s.status] || 0) + 1; });
  // Map IN_TRANSIT into SHIPPED for pipeline count
  const pipeCount = (key) => {
    if (key === "SHIPPED") return (statusCounts["SHIPPED"] || 0) + (statusCounts["IN_TRANSIT"] || 0);
    if (key === "RECEIVING") return (statusCounts["RECEIVING"] || 0) + (statusCounts["CHECKED_IN"] || 0);
    return statusCounts[key] || 0;
  };
  const activeStatuses = ["WORKING", "SHIPPED", "RECEIVING", "IN_TRANSIT", "CHECKED_IN"];
  const activeShipments = shipments.filter(s => activeStatuses.includes(s.status));
  const closedShipments = shipments.filter(s => s.status === "CLOSED");

  // Get items for KPI calculations — combine server-provided totals + expanded items
  const allItems = Object.values(itemsCache).flat();
  const itemsCacheSent = allItems.reduce((s, i) => s + (i.quantityShipped || 0), 0);
  const itemsCacheRecv = allItems.reduce((s, i) => s + (i.quantityReceived || 0), 0);
  // Also sum server-provided totals for shipments not yet expanded
  const serverSent = shipments.reduce((s, sh) => s + (sh.totalShipped || 0), 0);
  const serverRecv = shipments.reduce((s, sh) => s + (sh.totalReceived || 0), 0);
  // Use the larger of expanded-items vs server-provided (avoids double-counting)
  const totalSent = Math.max(itemsCacheSent, serverSent);
  const totalRecv = Math.max(itemsCacheRecv, serverRecv);
  const netDisc = totalRecv - totalSent;

  // Receive rate
  const recvRate = totalSent > 0 ? (totalRecv / totalSent * 100).toFixed(1) : "—";

  /* ── Filter logic ── */
  let filtered = statusFilter === "ALL" ? shipments
    : statusFilter === "DISC" ? shipments // handled below
    : shipments.filter(s => s.status === statusFilter);

  if (search.trim()) {
    const q = search.toLowerCase();
    filtered = filtered.filter(s =>
      (s.shipmentId || "").toLowerCase().includes(q) ||
      (s.shipmentName || "").toLowerCase().includes(q) ||
      (s.destination || "").toLowerCase().includes(q)
    );
  }
  filtered.sort((a, b) => {
    const ai = STATUS_ORDER.indexOf(a.status);
    const bi = STATUS_ORDER.indexOf(b.status);
    return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
  });

  const divRaw = (filters.division || "").toLowerCase();
  const divLabel = !divRaw ? "All Divisions" : divRaw === "golf" ? "Golf Division" : "Housewares";
  const custLabel = filters.customer ? ` · ${filters.customer.replace(/_/g, " ").replace(/\b\w/g, l => l.toUpperCase())}` : "";

  /* ── KPI card definitions ── */
  const kpis = [
    { label: "Active Shipments", value: activeShipments.length, color: "#E87830", sub: `${pipeCount("SHIPPED")} in transit · ${pipeCount("RECEIVING")} receiving` },
    { label: "Units Inbound", value: activeShipments.reduce((s, sh) => s + ((sh.totalShipped || 0) - (sh.totalReceived || 0)), 0).toLocaleString(), color: "var(--acc1)", sub: "All open shipments" },
    { label: "Shipped (All)", value: totalSent > 0 ? totalSent.toLocaleString() : "—", color: "#7BAED0", sub: "Total units sent" },
    { label: "Received (All)", value: totalRecv > 0 ? totalRecv.toLocaleString() : "—", color: "var(--acc1)", sub: "Confirmed by FC" },
    { label: "Net Discrepancy", value: netDisc !== 0 ? netDisc.toLocaleString() : totalSent > 0 ? "0" : "—", color: netDisc < 0 ? "#f87171" : "var(--acc1)", sub: "Sent vs received" },
    { label: "Avg Lead Time", value: "—", color: "#F5B731", sub: "Ship → FC received" },
    { label: "Closed", value: closedShipments.length, color: "var(--acc1)", sub: "Fully received" },
    { label: "On-Time Rate", value: "—", color: "var(--acc3, #3E658C)", sub: "Arrived by ETA" },
  ];

  /* ── Pipeline totals for proportional bar ── */
  const pipeTotal = PIPE_STATUSES.reduce((s, p) => s + pipeCount(p.key), 0) || 1;

  return (
    <div style={{ fontFamily: "'Sora',-apple-system,BlinkMacSystemFont,sans-serif", color: "var(--txt)" }}>
      {/* Pulse animation for status dots */}
      <style>{`@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(1.3)}}`}</style>

      {/* ══ Sub-nav tabs ══ */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 18, borderBottom: '2px solid var(--brd)' }}>
        {INV_VIEWS.map(v => (
          <a key={v.key} href={v.path} style={{
            display: 'inline-flex', alignItems: 'center', height: 32, padding: '0 14px',
            borderRadius: '8px 8px 0 0', fontSize: 11, fontWeight: 600, textDecoration: 'none',
            color: currentPath === v.path ? '#fff' : 'var(--txt2)',
            background: currentPath === v.path ? 'var(--atab)' : 'transparent',
            border: currentPath === v.path ? '1px solid var(--brd)' : '1px solid transparent',
            borderBottom: 'none', position: 'relative', bottom: -2, whiteSpace: 'nowrap',
            ...(currentPath === v.path ? { boxShadow: 'inset 0 -2px 0 var(--acc1)' } : {}),
          }}>{v.label}</a>
        ))}
      </div>

      {/* ══ Page Header ══ */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16, flexWrap: 'wrap', gap: 10 }}>
        <div>
          <div style={{ ...DM({ fontSize: 22, color: 'var(--acc1)' }) }}>Shipments</div>
          <div style={{ ...SG({ fontSize: 11, color: 'var(--txt3)', marginTop: 2 }) }}>
            {divLabel}{custLabel} · SP-API Fulfillment Inbound{data?.lastSync ? ` · Last sync: ${data.lastSync}` : ""} · {shipments.length} total shipments
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <a href="https://sellercentral.amazon.com/fba/sendtoamazon" target="_blank" rel="noreferrer"
              style={{ display: 'inline-flex', alignItems: 'center', gap: 5, height: 30, padding: '0 12px', borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: 'pointer', border: '1px solid var(--acc1)', background: 'rgba(46,207,170,.1)', color: 'var(--acc1)', textDecoration: 'none' }}>
              📦 New Shipment Plan
            </a>
            <span style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', marginTop: 1 }) }}>Opens Seller Central</span>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <button onClick={handleSync} disabled={syncing}
              style={{ display: 'inline-flex', alignItems: 'center', gap: 5, height: 30, padding: '0 12px', borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: 'pointer', border: '1px solid var(--acc2, #E87830)', background: 'rgba(232,120,48,.1)', color: 'var(--acc2, #E87830)', opacity: syncing ? 0.6 : 1 }}>
              🔄 {syncing ? "Syncing…" : "Sync Inbound Now"}
            </button>
            <span style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', marginTop: 1 }) }}>Auto every 4hr</span>
          </div>
          <button style={{ display: 'inline-flex', alignItems: 'center', gap: 5, height: 30, padding: '0 12px', borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: 'pointer', border: '1px solid var(--brd2)', background: 'var(--ibg)', color: 'var(--txt2)' }}>
            ⬇ Export CSV
          </button>
        </div>
      </div>

      {/* ══ Alert Banners ══ */}
      {activeShipments.length > 0 && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 14px', borderRadius: 9, marginBottom: 10, border: '1px solid rgba(245,183,49,.25)', background: 'rgba(245,183,49,.1)', ...SG({ fontSize: 11, fontWeight: 600, color: '#F5B731' }) }}>
          <span style={{ fontSize: 13, flexShrink: 0 }}>⚠️</span>
          <span style={{ whiteSpace: 'nowrap', flexShrink: 0 }}>ATTENTION —</span>
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <span style={{ padding: '2px 8px', borderRadius: 99, fontSize: 10, fontWeight: 700, background: 'rgba(245,183,49,.15)', color: '#F5B731' }}>
              {activeShipments.length} active shipment{activeShipments.length !== 1 ? "s" : ""} in pipeline
            </span>
            {pipeCount("RECEIVING") > 0 && (
              <span style={{ padding: '2px 8px', borderRadius: 99, fontSize: 10, fontWeight: 700, background: 'rgba(245,183,49,.15)', color: '#F5B731' }}>
                {pipeCount("RECEIVING")} shipment{pipeCount("RECEIVING") !== 1 ? "s" : ""} in receiving
              </span>
            )}
          </div>
        </div>
      )}

      {/* ══ KPI Cards ══ */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(8, 1fr)', gap: 8, marginBottom: 16 }}>
        {kpis.map((k, i) => (
          <div key={i} style={{
            background: 'linear-gradient(145deg, var(--card), var(--card2, var(--card)))',
            borderRadius: 11, padding: '11px 13px', border: '1px solid var(--brd)',
            position: 'relative', overflow: 'hidden', transition: 'border-color .2s',
          }}>
            <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 2, background: k.color }} />
            <div style={{ ...SG({ fontSize: 7, color: 'var(--txt3)', textTransform: 'uppercase', letterSpacing: '.09em', marginBottom: 4 }) }}>{k.label}</div>
            <div style={{ ...DM({ fontSize: 20, lineHeight: 1, color: k.color, marginBottom: 2 }) }}>{k.value}</div>
            <div style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', marginTop: 1 }) }}>{k.sub}</div>
          </div>
        ))}
      </div>

      {/* ══ Pipeline Overview ══ */}
      <SecDiv label="Pipeline Overview" />
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, marginBottom: 14 }}>
        {/* Status Pipeline */}
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Inbound Shipment Status Pipeline">
            <span style={{ fontSize: 10, color: 'var(--txt3)' }}>{shipments.length} shipments · <span style={{ color: 'var(--txt)', fontWeight: 600 }}>{activeShipments.length} active</span></span>
          </CardHdr>
          <div style={{ position: 'relative', padding: '22px 24px 14px', display: 'flex', alignItems: 'flex-start' }}>
            <div style={{ position: 'absolute', top: 35, left: 'calc(24px + 22px)', right: 'calc(24px + 22px)', height: 2, background: 'var(--brd)' }} />
            {PIPE_STATUSES.map(p => {
              const cnt = pipeCount(p.key);
              const ringStyles = {
                idle: { background: 'var(--card)', border: '2px solid var(--brd2)' },
                active: { background: p.color, border: `2px solid ${p.color}`, boxShadow: `0 0 0 4px rgba(232,120,48,.18)` },
                warn: { background: p.color, border: `2px solid ${p.color}`, boxShadow: `0 0 0 4px rgba(245,183,49,.18)` },
                done: { background: p.color, border: `2px solid ${p.color}` },
                err: { background: p.color, border: `2px solid ${p.color}`, boxShadow: `0 0 0 4px rgba(248,113,113,.18)` },
              };
              const ring = cnt > 0 ? ringStyles[p.ring] : ringStyles.idle;
              return (
                <div key={p.key} style={{ flex: 1, textAlign: 'center', position: 'relative', zIndex: 1 }}>
                  <div style={{ width: 16, height: 16, borderRadius: '50%', margin: '0 auto 8px', transition: 'all .25s', ...ring }} />
                  <div style={{ ...DM({ fontSize: 22, lineHeight: 1, color: p.color, marginBottom: 2 }) }}>{cnt}</div>
                  <div style={{ ...SG({ fontSize: 8, textTransform: 'uppercase', letterSpacing: '.07em', color: 'var(--txt3)' }) }}>{p.label}</div>
                  <div style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', marginTop: 3 }) }}>{p.sub}</div>
                </div>
              );
            })}
          </div>
          {/* Proportional bar */}
          <div style={{ padding: '0 24px 14px' }}>
            <div style={{ display: 'flex', height: 10, borderRadius: 4, overflow: 'hidden' }}>
              {PIPE_STATUSES.map(p => {
                const cnt = pipeCount(p.key);
                if (!cnt) return null;
                return <div key={p.key} style={{ width: `${cnt / pipeTotal * 100}%`, background: p.color, opacity: p.key === "CLOSED" ? 0.3 : 0.85 }} />;
              })}
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 4 }}>
              {PIPE_STATUSES.map(p => {
                const cnt = pipeCount(p.key);
                if (!cnt) return null;
                return <span key={p.key} style={{ ...SG({ fontSize: 7, color: p.color }) }}>{Math.round(cnt / pipeTotal * 100)}% {p.label}</span>;
              })}
            </div>
          </div>
        </Card>

        {/* Volume Trend placeholder */}
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Units Shipped vs Received — Weekly">
            <Badge text="Chart data builds over time" type="blue" />
          </CardHdr>
          <div style={{ padding: 40, textAlign: 'center', color: 'var(--txt3)', ...SG({ fontSize: 11 }) }}>
            Volume trend chart will populate as shipment history accumulates from SP-API syncs.
          </div>
        </Card>
      </div>

      {/* ══ Timeline placeholder ══ */}
      <SecDiv label="Active Shipment Timeline" />
      <Card>
        <CardHdr title="Shipment Gantt — Timeline View">
          <Badge text="Coming soon" type="blue" />
        </CardHdr>
        <div style={{ padding: 30, textAlign: 'center', color: 'var(--txt3)', ...SG({ fontSize: 11 }) }}>
          Gantt timeline will show ship-date to ETA for all active and recent shipments once historical data builds up.
        </div>
      </Card>

      {/* ══ Lead Time + Carrier placeholder ══ */}
      <SecDiv label="Lead Time & Carrier Performance" />
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, marginBottom: 14 }}>
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Lead Time per Shipment">
            <Badge text="Needs transit data" type="blue" />
          </CardHdr>
          <div style={{ padding: 30, textAlign: 'center', color: 'var(--txt3)', ...SG({ fontSize: 11 }) }}>
            Lead time trends will populate as closed shipments accumulate transit-time data.
          </div>
        </Card>
        <Card style={{ marginBottom: 0 }}>
          <CardHdr title="Carrier Scorecard">
            <Badge text="Needs carrier data" type="blue" />
          </CardHdr>
          <div style={{ padding: 30, textAlign: 'center', color: 'var(--txt3)', ...SG({ fontSize: 11 }) }}>
            Carrier performance will show on-time rates and lost units once carrier tracking data is available.
          </div>
        </Card>
      </div>

      {/* ══ Main Table + Sidebar ══ */}
      <SecDiv label="All Shipments — Detail & ASIN-Level Drill-Down" />

      {/* Toolbar */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 12, flexWrap: 'wrap' }}>
        <span style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.1em', color: 'var(--txt3)' }) }}>Filter:</span>
        {[
          { key: "ALL", label: `All (${shipments.length})` },
          ...STATUS_ORDER.filter(st => statusCounts[st]).map(st => ({
            key: st, label: `${STATUS_LABELS[st] || st} (${statusCounts[st]})`,
            color: S_COLORS[st]?.text,
          })),
        ].map(f => (
          <button key={f.key} onClick={() => setStatusFilter(f.key)}
            style={{
              display: 'inline-flex', alignItems: 'center', height: 24, padding: '0 9px', borderRadius: 7,
              ...SG({ fontSize: 9, fontWeight: statusFilter === f.key ? 700 : 600 }),
              border: statusFilter === f.key ? '1px solid transparent' : `1px solid ${f.color ? `${f.color}33` : 'var(--brd2)'}`,
              background: statusFilter === f.key ? 'var(--atab)' : 'var(--ibg)',
              color: statusFilter === f.key ? '#fff' : (f.color || 'var(--txt3)'),
              cursor: 'pointer', transition: 'all .15s',
            }}>{f.label}</button>
        ))}
        <input placeholder="Search ID, name, FC…" value={search} onChange={e => setSearch(e.target.value)}
          style={{ height: 24, padding: '0 9px', borderRadius: 7, border: '1px solid var(--brd2)', background: 'var(--ibg)', color: 'var(--txt)', ...SG({ fontSize: 10 }), outline: 'none', width: 200, marginLeft: 'auto' }} />
      </div>

      {/* Grid: table + sidebar */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 320px', gap: 14, marginBottom: 14 }}>

        {/* ── Main Table ── */}
        <Card style={{ overflow: 'hidden', marginBottom: 0 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                {[
                  { label: "", w: 26 },
                  { label: "Shipment", w: 200 },
                  { label: "Status", w: 100 },
                  { label: "Dest FC", w: 72 },
                  { label: "Sent", w: 64, r: true },
                  { label: "Received", w: 64, r: true },
                  { label: "Recv %", w: 90 },
                  { label: "Discrepancy", w: 76, r: true },
                  { label: "Products", w: 60 },
                ].map((col, i) => (
                  <th key={i} style={{
                    ...SG({ fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.07em', color: 'var(--txt3)' }),
                    padding: '8px 10px', textAlign: col.r ? 'right' : 'left',
                    borderBottom: '1px solid var(--brd2)', background: 'var(--card2, var(--card))',
                    whiteSpace: 'nowrap', width: col.w,
                  }}>{col.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 && (
                <tr><td colSpan={9} style={{ padding: 40, textAlign: 'center', color: 'var(--txt3)', fontSize: 13 }}>
                  {shipments.length === 0 ? 'No FBA shipments found. Click "Sync Inbound Now" to fetch data.' : 'No shipments match current filters.'}
                </td></tr>
              )}
              {filtered.map(s => {
                const isExp = expandedId === s.shipmentId;
                const items = itemsCache[s.shipmentId] || [];
                // Use server-provided totals for active shipments; compute from items if expanded
                const sent = items.length > 0
                  ? items.reduce((sum, it) => sum + (it.quantityShipped || 0), 0)
                  : (s.totalShipped || 0);
                const recv = items.length > 0
                  ? items.reduce((sum, it) => sum + (it.quantityReceived || 0), 0)
                  : (s.totalReceived || 0);
                const disc = (sent > 0 || recv > 0) ? recv - sent : null;
                const discCol = disc === null ? 'var(--txt3)' : disc < 0 ? '#f87171' : disc > 0 ? '#2ECFAA' : 'var(--txt3)';

                return (
                  <React.Fragment key={s.shipmentId}>
                    {/* Master row */}
                    <tr onClick={() => toggleExpand(s.shipmentId)} style={{ cursor: 'pointer' }}
                      onMouseEnter={e => e.currentTarget.style.background = 'var(--ibg)'}
                      onMouseLeave={e => e.currentTarget.style.background = ''}>
                      <td style={{ padding: '9px 10px' }}>
                        <button style={{
                          width: 20, height: 20, borderRadius: 4, border: `1px solid ${isExp ? 'var(--acc1)' : 'var(--brd2)'}`,
                          background: isExp ? 'rgba(46,207,170,.07)' : 'transparent',
                          color: isExp ? 'var(--acc1)' : 'var(--txt3)', fontSize: 9, cursor: 'pointer',
                          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        }}>{isExp ? '▲' : '▼'}</button>
                      </td>
                      <td style={{ padding: '9px 10px' }}>
                        <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--txt)' }}>{s.shipmentName || s.shipmentId}</div>
                        <div style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', marginTop: 1 }) }}>{s.shipmentId}{s.itemCount ? ` · ${s.itemCount} ASINs` : ""}</div>
                      </td>
                      <td style={{ padding: '9px 10px' }}><StatusPill status={s.status} /></td>
                      <td style={{ padding: '9px 10px', ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--txt2)' }) }}>{s.destination || "—"}</td>
                      <td style={{ padding: '9px 10px', textAlign: 'right', fontWeight: 700, color: 'var(--txt)' }}>{sent > 0 ? sent.toLocaleString() : "—"}</td>
                      <td style={{ padding: '9px 10px', textAlign: 'right', color: 'var(--acc1)' }}>{recv > 0 ? recv.toLocaleString() : "—"}</td>
                      <td style={{ padding: '9px 10px' }}><RecvBar sent={sent} recv={recv} /></td>
                      <td style={{ padding: '9px 10px', textAlign: 'right', ...SG({ fontSize: 11, fontWeight: 700, color: discCol }) }}>
                        {disc === null ? "—" : disc === 0 ? "0" : disc > 0 ? `+${disc.toLocaleString()}` : disc.toLocaleString()}
                      </td>
                      <td style={{ padding: '9px 10px', fontSize: 11, color: 'var(--txt3)' }}>{s.itemCount || "—"}</td>
                    </tr>

                    {/* Expanded items */}
                    {isExp && (
                      <tr style={{ background: 'var(--card2, var(--card))' }}>
                        <td colSpan={9} style={{ padding: 0 }}>
                          <div style={{ borderTop: '2px solid var(--acc1)' }}>
                            {loadingItems === s.shipmentId ? (
                              <div style={{ padding: 16, textAlign: 'center', color: 'var(--txt3)', fontSize: 12 }}>Loading items…</div>
                            ) : items.length > 0 ? (
                              <>
                                <div style={{
                                  display: 'grid', gridTemplateColumns: '32px 1.2fr 1.5fr 70px 70px 76px 70px', gap: 6,
                                  padding: '6px 14px 6px 40px', background: 'var(--card)', borderBottom: '1px solid var(--brd2)',
                                }}>
                                  {["", "SKU / FNSKU", "Description", "Sent", "Received", "Discrepancy", "Recv %"].map((h, i) => (
                                    <span key={i} style={{ ...SG({ fontSize: 7, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.07em', color: 'var(--txt3)', textAlign: i > 2 ? 'right' : 'left' }) }}>{h}</span>
                                  ))}
                                </div>
                                {items.map((it, idx) => {
                                  const iSent = it.quantityShipped || 0;
                                  const iRecv = it.quantityReceived || 0;
                                  const iDisc = iRecv > 0 ? iRecv - iSent : null;
                                  const iPct = iSent > 0 && iRecv > 0 ? Math.round(iRecv / iSent * 100) : 0;
                                  const iDiscCol = iDisc === null ? 'var(--txt3)' : iDisc < 0 ? '#f87171' : iDisc > 0 ? '#2ECFAA' : 'var(--txt3)';
                                  const iPctCol = iPct >= 99 ? '#2ECFAA' : iPct >= 90 ? '#F5B731' : '#f87171';
                                  return (
                                    <div key={idx} style={{
                                      display: 'grid', gridTemplateColumns: '32px 1.2fr 1.5fr 70px 70px 76px 70px', gap: 6,
                                      padding: '7px 14px 7px 40px', borderBottom: '1px solid var(--brd)', alignItems: 'center',
                                    }}>
                                      <div style={{ ...SG({ fontSize: 9, color: 'var(--txt3)', fontWeight: 700 }) }}>{idx + 1}</div>
                                      <div>
                                        <div style={{ ...SG({ fontSize: 8, color: 'var(--acc3, #3E658C)', fontWeight: 700 }) }}>{it.sku}</div>
                                        <div style={{ fontSize: 10, color: 'var(--txt)', fontWeight: 600, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{it.fnsku || "—"}</div>
                                      </div>
                                      <div style={{ fontSize: 10, color: 'var(--txt2)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }} title={it.productName || ""}>{it.productName || "—"}</div>
                                      <div style={{ ...SG({ fontSize: 10, textAlign: 'right' }) }}>{iSent.toLocaleString()}</div>
                                      <div style={{ ...SG({ fontSize: 10, textAlign: 'right', color: iRecv > 0 ? '#2ECFAA' : 'var(--txt3)' }) }}>{iRecv > 0 ? iRecv.toLocaleString() : "—"}</div>
                                      <div style={{ textAlign: 'right' }}>
                                        {iDisc === null ? <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>—</span>
                                          : iDisc === 0 ? <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>0</span>
                                          : <span style={{ ...SG({ fontSize: 9, fontWeight: 700, padding: '1px 5px', borderRadius: 4, background: iDisc < 0 ? 'rgba(248,113,113,.14)' : 'rgba(46,207,170,.12)', color: iDiscCol }) }}>{iDisc > 0 ? `+${iDisc.toLocaleString()}` : iDisc.toLocaleString()}</span>
                                        }
                                      </div>
                                      <div style={{ textAlign: 'right' }}>
                                        {iRecv > 0 && iSent > 0
                                          ? <RecvBar sent={iSent} recv={iRecv} />
                                          : <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>—</span>
                                        }
                                      </div>
                                    </div>
                                  );
                                })}
                              </>
                            ) : (
                              <div style={{ padding: 16, textAlign: 'center', color: 'var(--txt3)', fontSize: 12 }}>No item details available.</div>
                            )}
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </Card>

        {/* ── Sidebar ── */}
        <div>
          {/* Discrepancy Tracker */}
          <Card style={{ marginBottom: 14 }}>
            <div style={{ padding: '9px 14px', borderBottom: '1px solid var(--brd)', background: 'var(--card2, var(--card))', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--txt)' }}>Discrepancy Tracker</span>
              {netDisc !== 0 && <Badge text={`${netDisc.toLocaleString()} units`} type={netDisc < 0 ? "risk" : "ok"} />}
            </div>
            {Object.keys(itemsCache).length === 0 ? (
              <div style={{ padding: 16, textAlign: 'center', color: 'var(--txt3)', ...SG({ fontSize: 10 }) }}>
                Expand shipment rows to load item data and see discrepancies.
              </div>
            ) : (
              <div>
                {shipments.filter(s => {
                  const items = itemsCache[s.shipmentId];
                  if (!items) return false;
                  const sent = items.reduce((sum, it) => sum + (it.quantityShipped || 0), 0);
                  const recv = items.reduce((sum, it) => sum + (it.quantityReceived || 0), 0);
                  return recv > 0 && recv !== sent;
                }).slice(0, 5).map(s => {
                  const items = itemsCache[s.shipmentId];
                  const sent = items.reduce((sum, it) => sum + (it.quantityShipped || 0), 0);
                  const recv = items.reduce((sum, it) => sum + (it.quantityReceived || 0), 0);
                  const diff = recv - sent;
                  return (
                    <div key={s.shipmentId} style={{ padding: '9px 14px', borderBottom: '1px solid var(--brd)' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 3 }}>
                        <span style={{ ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--acc1)' }) }}>{s.shipmentId}</span>
                        <span style={{ ...SG({ fontSize: 12, fontWeight: 700, color: diff < 0 ? '#f87171' : '#2ECFAA' }) }}>
                          {diff > 0 ? `+${diff.toLocaleString()}` : diff.toLocaleString()}
                        </span>
                      </div>
                      <div style={{ ...SG({ fontSize: 8, color: 'var(--txt3)' }) }}>{s.shipmentName} · {s.destination}</div>
                    </div>
                  );
                })}
                {shipments.filter(s => itemsCache[s.shipmentId]).every(s => {
                  const items = itemsCache[s.shipmentId];
                  const sent = items.reduce((sum, it) => sum + (it.quantityShipped || 0), 0);
                  const recv = items.reduce((sum, it) => sum + (it.quantityReceived || 0), 0);
                  return recv === 0 || recv === sent;
                }) && (
                  <div style={{ padding: 14, textAlign: 'center', color: 'var(--txt3)', ...SG({ fontSize: 10 }) }}>No discrepancies found in loaded shipments.</div>
                )}
              </div>
            )}
          </Card>

          {/* Health Summary */}
          <Card style={{ marginBottom: 0 }}>
            <div style={{ padding: '9px 14px', borderBottom: '1px solid var(--brd)', background: 'var(--card2, var(--card))' }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: 'var(--txt)' }}>Shipment Health Summary</span>
            </div>
            <MiniMetric label="Receive Rate" value={recvRate === "—" ? "—" : `${recvRate}%`} valueColor="var(--acc1)" note={parseFloat(recvRate) >= 99 ? "Excellent" : parseFloat(recvRate) >= 95 ? "Good" : "Needs attention"} noteColor={parseFloat(recvRate) >= 99 ? "#2ECFAA" : parseFloat(recvRate) >= 95 ? "#F5B731" : "#f87171"} />
            <MiniMetric label="Total Shipments" value={shipments.length} valueColor="#7BAED0" note="All time" />
            <MiniMetric label="Active" value={activeShipments.length} valueColor="#E87830" note="In pipeline" />
            <MiniMetric label="Closed" value={closedShipments.length} valueColor="var(--acc1)" note="Fully received" />
            <MiniMetric label="Avg Products / Shipment" value={shipments.length > 0 ? (shipments.reduce((s, sh) => s + (sh.itemCount || 0), 0) / shipments.length).toFixed(1) : "—"} valueColor="var(--txt2)" note="Avg" last />
          </Card>
        </div>
      </div>

      {data?._note && (
        <p style={{ fontSize: 11, color: 'var(--acc2, #E87830)', marginTop: 12, textAlign: 'center' }}>{data._note}</p>
      )}
    </div>
  );
}
