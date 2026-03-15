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
/* ── Create Shipment Modal ── */
const DEFAULT_SHIP_FROM = {
  Name: "GolfGen LLC",
  AddressLine1: "1201 S Walton Blvd",
  AddressLine2: "",
  City: "Bentonville",
  StateOrProvinceCode: "AR",
  PostalCode: "72712",
  CountryCode: "US",
};

function CreateShipmentModal({ onClose, onCreated }) {
  const [step, setStep] = useState(1); // 1=items, 2=address, 3=review/plan, 4=confirmed
  const [products, setProducts] = useState([]);
  const [loadingProducts, setLoadingProducts] = useState(true);
  const [selectedItems, setSelectedItems] = useState([]); // [{sku, asin, productName, quantity}]
  const [shipFrom, setShipFrom] = useState({ ...DEFAULT_SHIP_FROM });
  const [labelPref, setLabelPref] = useState("SELLER_LABEL");
  const [plans, setPlans] = useState(null);
  const [creating, setCreating] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState(null);
  const [result, setResult] = useState(null);
  const [searchQ, setSearchQ] = useState("");

  useEffect(() => {
    api.fbaShipmentProducts().then(d => { setProducts(d.products || []); setLoadingProducts(false); }).catch(() => setLoadingProducts(false));
  }, []);

  const addItem = (prod) => {
    if (selectedItems.find(i => i.sku === prod.sku)) return;
    setSelectedItems(prev => [...prev, { sku: prod.sku, asin: prod.asin, productName: prod.productName, quantity: 1 }]);
  };
  const removeItem = (sku) => setSelectedItems(prev => prev.filter(i => i.sku !== sku));
  const updateQty = (sku, qty) => setSelectedItems(prev => prev.map(i => i.sku === sku ? { ...i, quantity: Math.max(1, parseInt(qty) || 1) } : i));

  const handleCreatePlan = async () => {
    setCreating(true);
    setError(null);
    try {
      const res = await api.fbaCreatePlan({
        shipFromAddress: shipFrom,
        items: selectedItems.map(i => ({ SellerSKU: i.sku, ASIN: i.asin, Quantity: i.quantity, Condition: "NewItem" })),
        labelPrepPreference: labelPref,
      });
      if (res.ok) { setPlans(res.plans); setStep(3); }
      else setError(res.error || "Failed to create plan");
    } catch (e) { setError(e.message); }
    setCreating(false);
  };

  const handleConfirmPlan = async (plan) => {
    setConfirming(true);
    setError(null);
    try {
      const res = await api.fbaConfirmPlan({
        shipmentId: plan.shipmentId,
        shipmentName: `GolfGen ${new Date().toLocaleDateString()}`,
        destinationFC: plan.destinationFC,
        items: plan.items.map(i => ({ SellerSKU: i.SellerSKU, Quantity: i.Quantity })),
        shipFromAddress: shipFrom,
        labelPrepPreference: labelPref,
      });
      if (res.ok) { setResult(res); setStep(4); }
      else setError(res.error || "Failed to confirm plan");
    } catch (e) { setError(e.message); }
    setConfirming(false);
  };

  const filteredProducts = searchQ.trim()
    ? products.filter(p => (p.productName + p.sku + p.asin).toLowerCase().includes(searchQ.toLowerCase()))
    : products;

  const overlay = { position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, background: 'rgba(0,0,0,.6)', zIndex: 9999, display: 'flex', alignItems: 'center', justifyContent: 'center' };
  const modal = { background: 'var(--card)', border: '1px solid var(--brd)', borderRadius: 16, width: '90vw', maxWidth: 860, maxHeight: '85vh', overflow: 'hidden', display: 'flex', flexDirection: 'column', boxShadow: '0 20px 60px rgba(0,0,0,.4)' };
  const hdr = { padding: '14px 20px', borderBottom: '1px solid var(--brd)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', background: 'var(--card2, var(--card))' };
  const body = { padding: '16px 20px', overflowY: 'auto', flex: 1 };
  const footer = { padding: '12px 20px', borderTop: '1px solid var(--brd)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 };
  const btnP = { display: 'inline-flex', alignItems: 'center', gap: 5, height: 34, padding: '0 18px', borderRadius: 8, ...SG({ fontSize: 11, fontWeight: 700 }), cursor: 'pointer', border: 'none', background: 'var(--acc1)', color: '#fff' };
  const btnS = { ...btnP, background: 'transparent', border: '1px solid var(--brd2)', color: 'var(--txt2)' };
  const inp = { height: 32, padding: '0 10px', borderRadius: 7, border: '1px solid var(--brd2)', background: 'var(--ibg)', color: 'var(--txt)', ...SG({ fontSize: 10 }), outline: 'none', width: '100%' };

  return (
    <div style={overlay} onClick={onClose}>
      <div style={modal} onClick={e => e.stopPropagation()}>
        {/* Header */}
        <div style={hdr}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--txt)' }}>Create FBA Shipment</div>
            <div style={{ ...SG({ fontSize: 9, color: 'var(--txt3)', marginTop: 2 }) }}>
              Step {step} of 4 — {step === 1 ? "Select Products" : step === 2 ? "Ship From Address" : step === 3 ? "Review Amazon Plan" : "Confirmed"}
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {[1,2,3,4].map(s => (
              <div key={s} style={{ width: 24, height: 4, borderRadius: 2, background: s <= step ? 'var(--acc1)' : 'var(--brd)', transition: 'background .3s' }} />
            ))}
            <button onClick={onClose} style={{ width: 28, height: 28, borderRadius: 6, border: '1px solid var(--brd2)', background: 'transparent', color: 'var(--txt3)', fontSize: 14, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', marginLeft: 8 }}>×</button>
          </div>
        </div>

        {/* Body */}
        <div style={body}>
          {error && (
            <div style={{ padding: '8px 12px', borderRadius: 8, background: 'rgba(248,113,113,.12)', border: '1px solid rgba(248,113,113,.3)', marginBottom: 12, ...SG({ fontSize: 10, color: '#f87171' }) }}>
              {error}
            </div>
          )}

          {/* Step 1: Select Products */}
          {step === 1 && (
            <>
              {/* Selected items */}
              {selectedItems.length > 0 && (
                <div style={{ marginBottom: 14 }}>
                  <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.1em', color: 'var(--txt3)', marginBottom: 6 }) }}>
                    Selected ({selectedItems.length} items · {selectedItems.reduce((s, i) => s + i.quantity, 0)} units)
                  </div>
                  {selectedItems.map(item => (
                    <div key={item.sku} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '6px 10px', borderRadius: 7, marginBottom: 4, background: 'rgba(46,207,170,.06)', border: '1px solid rgba(46,207,170,.2)' }}>
                      <div style={{ flex: 1 }}>
                        <span style={{ ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--acc1)' }) }}>{item.asin}</span>
                        <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)', marginLeft: 8 }) }}>{item.sku}</span>
                        <div style={{ fontSize: 10, color: 'var(--txt2)', marginTop: 1 }}>{item.productName}</div>
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                        <span style={{ ...SG({ fontSize: 8, color: 'var(--txt3)' }) }}>Qty:</span>
                        <input type="number" min="1" value={item.quantity} onChange={e => updateQty(item.sku, e.target.value)}
                          style={{ ...inp, width: 60, textAlign: 'center' }} />
                      </div>
                      <button onClick={() => removeItem(item.sku)} style={{ width: 22, height: 22, borderRadius: 4, border: '1px solid rgba(248,113,113,.3)', background: 'rgba(248,113,113,.08)', color: '#f87171', fontSize: 11, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>×</button>
                    </div>
                  ))}
                </div>
              )}

              {/* Product search */}
              <input placeholder="Search products by name, SKU, or ASIN…" value={searchQ} onChange={e => setSearchQ(e.target.value)}
                style={{ ...inp, marginBottom: 10 }} />

              {loadingProducts ? (
                <div style={{ padding: 20, textAlign: 'center', color: 'var(--txt3)', fontSize: 12 }}>Loading products…</div>
              ) : (
                <div style={{ maxHeight: 300, overflowY: 'auto', border: '1px solid var(--brd)', borderRadius: 8 }}>
                  {filteredProducts.length === 0 ? (
                    <div style={{ padding: 20, textAlign: 'center', color: 'var(--txt3)', fontSize: 11 }}>No products found.</div>
                  ) : filteredProducts.slice(0, 50).map(p => {
                    const isAdded = selectedItems.find(i => i.sku === p.sku);
                    return (
                      <div key={p.sku} onClick={() => !isAdded && addItem(p)}
                        style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '7px 12px', borderBottom: '1px solid var(--brd)', cursor: isAdded ? 'default' : 'pointer', opacity: isAdded ? 0.5 : 1 }}
                        onMouseEnter={e => { if (!isAdded) e.currentTarget.style.background = 'var(--ibg)'; }}
                        onMouseLeave={e => e.currentTarget.style.background = ''}>
                        <div style={{ flex: 1 }}>
                          <div>
                            <span style={{ ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--acc1)' }) }}>{p.asin}</span>
                            <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)', marginLeft: 8 }) }}>{p.sku}</span>
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--txt2)', marginTop: 1 }}>{p.productName || "—"}</div>
                        </div>
                        <div style={{ textAlign: 'right' }}>
                          <div style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>{p.division}</div>
                          <div style={{ ...SG({ fontSize: 10, fontWeight: 700, color: p.currentStock > 0 ? 'var(--acc1)' : '#f87171' }) }}>{p.currentStock} in FBA</div>
                        </div>
                        {isAdded ? <Badge text="Added" type="ok" /> : <span style={{ ...SG({ fontSize: 9, color: 'var(--acc1)', fontWeight: 700 }) }}>+ Add</span>}
                      </div>
                    );
                  })}
                </div>
              )}
            </>
          )}

          {/* Step 2: Ship From Address */}
          {step === 2 && (
            <div style={{ maxWidth: 480 }}>
              <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.1em', color: 'var(--txt3)', marginBottom: 10 }) }}>Ship From Address</div>
              {[
                { key: "Name", label: "Company / Name" },
                { key: "AddressLine1", label: "Address Line 1" },
                { key: "AddressLine2", label: "Address Line 2 (optional)" },
                { key: "City", label: "City" },
                { key: "StateOrProvinceCode", label: "State" },
                { key: "PostalCode", label: "Zip Code" },
                { key: "CountryCode", label: "Country Code" },
              ].map(f => (
                <div key={f.key} style={{ marginBottom: 8 }}>
                  <label style={{ ...SG({ fontSize: 9, color: 'var(--txt3)', display: 'block', marginBottom: 3 }) }}>{f.label}</label>
                  <input value={shipFrom[f.key] || ""} onChange={e => setShipFrom(prev => ({ ...prev, [f.key]: e.target.value }))} style={inp} />
                </div>
              ))}
              <div style={{ marginTop: 12 }}>
                <label style={{ ...SG({ fontSize: 9, color: 'var(--txt3)', display: 'block', marginBottom: 3 }) }}>Label Prep Preference</label>
                <select value={labelPref} onChange={e => setLabelPref(e.target.value)} style={{ ...inp, appearance: 'auto' }}>
                  <option value="SELLER_LABEL">Seller Label (you label items)</option>
                  <option value="AMAZON_LABEL_ONLY">Amazon Label Only</option>
                  <option value="AMAZON_LABEL_PREFERRED">Amazon Label Preferred</option>
                </select>
              </div>
            </div>
          )}

          {/* Step 3: Review Amazon Plan */}
          {step === 3 && plans && (
            <>
              <div style={{ ...SG({ fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.1em', color: 'var(--txt3)', marginBottom: 10 }) }}>
                Amazon Proposed {plans.length} Shipment{plans.length !== 1 ? "s" : ""}
              </div>
              <div style={{ padding: '8px 12px', borderRadius: 8, background: 'rgba(123,174,208,.1)', border: '1px solid rgba(123,174,208,.25)', marginBottom: 14, ...SG({ fontSize: 10, color: '#7BAED0' }) }}>
                Amazon splits your items across fulfillment centers for optimal placement. Each box below is a separate shipment you'll need to pack and ship.
              </div>
              {plans.map((plan, idx) => (
                <Card key={idx} style={{ marginBottom: 10 }}>
                  <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--brd)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--txt)' }}>{plan.shipmentId}</div>
                      <div style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>Destination: <span style={{ fontWeight: 700, color: 'var(--acc1)' }}>{plan.destinationFC}</span> · {plan.items.length} item{plan.items.length !== 1 ? "s" : ""}</div>
                    </div>
                    <button onClick={() => handleConfirmPlan(plan)} disabled={confirming}
                      style={{ ...btnP, opacity: confirming ? 0.6 : 1, fontSize: 10, height: 30 }}>
                      {confirming ? "Creating…" : "Confirm & Create"}
                    </button>
                  </div>
                  <div style={{ padding: '8px 14px' }}>
                    {plan.items.map((pi, j) => (
                      <div key={j} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '5px 0', borderBottom: j < plan.items.length - 1 ? '1px solid var(--brd)' : 'none' }}>
                        <div>
                          <span style={{ ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--txt)' }) }}>{pi.SellerSKU}</span>
                          {pi.FulfillmentNetworkSKU && <span style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', marginLeft: 6 }) }}>FNSKU: {pi.FulfillmentNetworkSKU}</span>}
                        </div>
                        <span style={{ ...SG({ fontSize: 11, fontWeight: 700, color: 'var(--acc1)' }) }}>{pi.Quantity} units</span>
                      </div>
                    ))}
                  </div>
                </Card>
              ))}
            </>
          )}

          {/* Step 4: Confirmed */}
          {step === 4 && result && (
            <div style={{ textAlign: 'center', padding: '30px 20px' }}>
              <div style={{ fontSize: 40, marginBottom: 10 }}>✅</div>
              <div style={{ fontSize: 16, fontWeight: 700, color: 'var(--acc1)', marginBottom: 6 }}>Shipment Created!</div>
              <div style={{ ...SG({ fontSize: 12, color: 'var(--txt2)', marginBottom: 16 }) }}>{result.message}</div>
              <div style={{ ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--acc3, #3E658C)', marginBottom: 16 }) }}>
                Shipment ID: {result.shipmentId}
              </div>
              {result.sellerCentralUrl && (
                <a href={result.sellerCentralUrl} target="_blank" rel="noreferrer"
                  style={{ ...btnP, textDecoration: 'none', display: 'inline-flex' }}>
                  Open in Seller Central →
                </a>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        {step < 4 && (
          <div style={footer}>
            <div>
              {step > 1 && step < 3 && <button onClick={() => setStep(step - 1)} style={btnS}>← Back</button>}
            </div>
            <div style={{ display: 'flex', gap: 8 }}>
              <button onClick={onClose} style={btnS}>Cancel</button>
              {step === 1 && (
                <button onClick={() => setStep(2)} disabled={selectedItems.length === 0}
                  style={{ ...btnP, opacity: selectedItems.length === 0 ? 0.4 : 1 }}>
                  Next: Ship From →
                </button>
              )}
              {step === 2 && (
                <button onClick={handleCreatePlan} disabled={creating}
                  style={{ ...btnP, opacity: creating ? 0.6 : 1 }}>
                  {creating ? "Submitting to Amazon…" : "Submit Plan to Amazon →"}
                </button>
              )}
            </div>
          </div>
        )}
        {step === 4 && (
          <div style={footer}>
            <div />
            <button onClick={() => { onCreated?.(); onClose(); }} style={btnP}>Done — Refresh Shipments</button>
          </div>
        )}
      </div>
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
  const [showCreate, setShowCreate] = useState(false);

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
  const cancelledShipments = shipments.filter(s => s.status === "CANCELLED");
  // Exclude cancelled from all "good" metrics
  const goodShipments = shipments.filter(s => s.status !== "CANCELLED");

  // Get items for KPI calculations — exclude cancelled shipments
  const allItems = Object.values(itemsCache).flat();
  const itemsCacheSent = allItems.reduce((s, i) => s + (i.quantityShipped || 0), 0);
  const itemsCacheRecv = allItems.reduce((s, i) => s + (i.quantityReceived || 0), 0);
  // Sum server-provided totals for non-cancelled shipments only
  const serverSent = goodShipments.reduce((s, sh) => s + (sh.totalShipped || 0), 0);
  const serverRecv = goodShipments.reduce((s, sh) => s + (sh.totalReceived || 0), 0);
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
    { label: "Expected (All)", value: totalSent > 0 ? totalSent.toLocaleString() : "—", color: "#7BAED0", sub: "Total units expected" },
    { label: "Received (All)", value: totalRecv > 0 ? totalRecv.toLocaleString() : "—", color: "var(--acc1)", sub: "Confirmed by FC" },
    { label: "Variance", value: netDisc !== 0 ? netDisc.toLocaleString() : totalSent > 0 ? "0" : "—", color: netDisc < 0 ? "#f87171" : "var(--acc1)", sub: "Expected vs received" },
    { label: "Closed", value: closedShipments.length, color: "var(--acc1)", sub: "Fully received" },
    { label: "Cancelled", value: cancelledShipments.length, color: cancelledShipments.length > 0 ? "#4d6d8a" : "var(--txt3)", sub: "Not shipped" },
    { label: "Receive Rate", value: recvRate === "—" ? "—" : `${recvRate}%`, color: parseFloat(recvRate) >= 99 ? "var(--acc1)" : parseFloat(recvRate) >= 95 ? "#F5B731" : "#f87171", sub: "Excl. cancelled" },
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
            <button onClick={() => setShowCreate(true)}
              style={{ display: 'inline-flex', alignItems: 'center', gap: 5, height: 30, padding: '0 12px', borderRadius: 8, ...SG({ fontSize: 10, fontWeight: 700 }), cursor: 'pointer', border: '1px solid var(--acc1)', background: 'rgba(46,207,170,.1)', color: 'var(--acc1)' }}>
              📦 New Shipment Plan
            </button>
            <span style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', marginTop: 1 }) }}>Create via SP-API</span>
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

      {/* ══ Gantt Timeline ══ */}
      <SecDiv label="Active Shipment Timeline" />
      <Card>
        <CardHdr title="Shipment Gantt — Status Timeline">
          <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>{activeShipments.length + closedShipments.slice(0, 8).length} shipments shown</span>
        </CardHdr>
        {(() => {
          const ganttShipments = [...activeShipments, ...closedShipments.slice(0, 8)];
          if (ganttShipments.length === 0) return (
            <div style={{ padding: 30, textAlign: 'center', color: 'var(--txt3)', ...SG({ fontSize: 11 }) }}>
              No shipments to display. Click "Sync Inbound Now" to fetch data.
            </div>
          );
          const stageMap = { WORKING: 1, SHIPPED: 2, IN_TRANSIT: 2, RECEIVING: 3, CHECKED_IN: 3, CLOSED: 4 };
          const stageColors = { 1: '#7BAED0', 2: '#E87830', 3: '#F5B731', 4: '#2ECFAA' };
          const stageLabels = { 1: 'Working', 2: 'Shipped', 3: 'Receiving', 4: 'Closed' };
          return (
            <div style={{ padding: '14px 20px', overflowX: 'auto' }}>
              {/* Stage legend */}
              <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
                {[1,2,3,4].map(st => (
                  <div key={st} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                    <div style={{ width: 10, height: 10, borderRadius: 2, background: stageColors[st] }} />
                    <span style={{ ...SG({ fontSize: 8, color: 'var(--txt3)', textTransform: 'uppercase', letterSpacing: '.05em' }) }}>{stageLabels[st]}</span>
                  </div>
                ))}
              </div>
              {/* Gantt bars */}
              {ganttShipments.map(s => {
                const stage = stageMap[s.status] || 1;
                const barWidth = (stage / 4) * 100;
                const barColor = stageColors[stage];
                const sent = s.totalShipped || 0;
                const recv = s.totalReceived || 0;
                return (
                  <div key={s.shipmentId} style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
                    <div style={{ minWidth: 140, maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      <span style={{ ...SG({ fontSize: 9, fontWeight: 700, color: 'var(--txt2)' }) }}>{s.shipmentId}</span>
                    </div>
                    <div style={{ flex: 1, position: 'relative', height: 20, background: 'var(--ibg)', borderRadius: 4, overflow: 'hidden' }}>
                      <div style={{ position: 'absolute', top: 0, left: 0, height: '100%', width: `${barWidth}%`, background: barColor, borderRadius: 4, opacity: 0.85, transition: 'width .4s' }} />
                      <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: '100%', display: 'flex', alignItems: 'center', padding: '0 8px', justifyContent: 'space-between' }}>
                        <span style={{ ...SG({ fontSize: 8, fontWeight: 700, color: '#fff', textShadow: '0 1px 2px rgba(0,0,0,.4)', zIndex: 1 }) }}>{s.destination}</span>
                        <span style={{ ...SG({ fontSize: 8, color: stage >= 3 ? '#fff' : 'var(--txt3)', textShadow: stage >= 3 ? '0 1px 2px rgba(0,0,0,.4)' : 'none', zIndex: 1 }) }}>
                          {sent > 0 ? `${recv}/${sent}` : '—'}
                        </span>
                      </div>
                    </div>
                    <div style={{ minWidth: 60 }}>
                      <StatusPill status={s.status} />
                    </div>
                  </div>
                );
              })}
            </div>
          );
        })()}
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
          <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 900 }}>
            <thead>
              <tr>
                {[
                  { label: "", w: 26 },
                  { label: "Shipment ID", w: 120 },
                  { label: "Shipment Name", w: 160 },
                  { label: "Status", w: 90 },
                  { label: "Dest FC", w: 64 },
                  { label: "Expected", w: 58, r: true },
                  { label: "Received", w: 62, r: true },
                  { label: "Recv %", w: 80 },
                  { label: "Variance", w: 72, r: true },
                  { label: "Products", w: 54, r: true },
                ].map((col, i) => (
                  <th key={i} style={{
                    ...SG({ fontSize: 8, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.07em', color: 'var(--txt3)' }),
                    padding: '8px 8px', textAlign: col.r ? 'right' : 'left',
                    borderBottom: '1px solid var(--brd2)', background: 'var(--card2, var(--card))',
                    whiteSpace: 'nowrap', width: col.w,
                  }}>{col.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 && (
                <tr><td colSpan={10} style={{ padding: 40, textAlign: 'center', color: 'var(--txt3)', fontSize: 13 }}>
                  {shipments.length === 0 ? 'No FBA shipments found. Click "Sync Inbound Now" to fetch data.' : 'No shipments match current filters.'}
                </td></tr>
              )}
              {filtered.map(s => {
                const isExp = expandedId === s.shipmentId;
                const items = itemsCache[s.shipmentId] || [];
                const sent = items.length > 0
                  ? items.reduce((sum, it) => sum + (it.quantityShipped || 0), 0)
                  : (s.totalShipped || 0);
                const recv = items.length > 0
                  ? items.reduce((sum, it) => sum + (it.quantityReceived || 0), 0)
                  : (s.totalReceived || 0);
                const disc = (sent > 0 || recv > 0) ? recv - sent : null;
                const discCol = disc === null ? 'var(--txt3)' : disc < 0 ? '#f87171' : disc > 0 ? '#2ECFAA' : 'var(--txt3)';

                const isCancelled = s.status === "CANCELLED";
                return (
                  <React.Fragment key={s.shipmentId}>
                    {/* Master row */}
                    <tr onClick={() => toggleExpand(s.shipmentId)}
                      style={{ cursor: 'pointer', opacity: isCancelled ? 0.5 : 1 }}
                      onMouseEnter={e => e.currentTarget.style.background = 'var(--ibg)'}
                      onMouseLeave={e => e.currentTarget.style.background = ''}>
                      <td style={{ padding: '9px 8px' }}>
                        <button style={{
                          width: 20, height: 20, borderRadius: 4, border: `1px solid ${isExp ? 'var(--acc1)' : 'var(--brd2)'}`,
                          background: isExp ? 'rgba(46,207,170,.07)' : 'transparent',
                          color: isExp ? 'var(--acc1)' : 'var(--txt3)', fontSize: 9, cursor: 'pointer',
                          display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        }}>{isExp ? '▲' : '▼'}</button>
                      </td>
                      <td style={{ padding: '9px 8px' }}>
                        <div style={{ ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--acc3, #3E658C)' }) }}>{s.shipmentId}</div>
                      </td>
                      <td style={{ padding: '9px 8px' }}>
                        <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--txt)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 160 }}>{s.shipmentName || "—"}</div>
                      </td>
                      <td style={{ padding: '9px 8px' }}><StatusPill status={s.status} /></td>
                      <td style={{ padding: '9px 8px', ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--txt2)' }) }}>{s.destination || "—"}</td>
                      <td style={{ padding: '9px 8px', textAlign: 'right', fontWeight: 700, color: 'var(--txt)' }}>{sent > 0 ? sent.toLocaleString() : "—"}</td>
                      <td style={{ padding: '9px 8px', textAlign: 'right', color: 'var(--acc1)' }}>{recv > 0 ? recv.toLocaleString() : "—"}</td>
                      <td style={{ padding: '9px 8px' }}><RecvBar sent={sent} recv={recv} /></td>
                      <td style={{ padding: '9px 8px', textAlign: 'right', ...SG({ fontSize: 11, fontWeight: 700, color: discCol }) }}>
                        {disc === null ? "—" : disc === 0 ? "0" : disc > 0 ? `+${disc.toLocaleString()}` : disc.toLocaleString()}
                      </td>
                      <td style={{ padding: '9px 8px', textAlign: 'right', fontSize: 11, color: 'var(--txt3)' }}>{s.itemCount || "—"}</td>
                    </tr>

                    {/* Expanded items — ASIN-level with aligned columns */}
                    {isExp && (
                      <tr style={{ background: 'var(--card2, var(--card))' }}>
                        <td colSpan={10} style={{ padding: 0 }}>
                          <div style={{ borderTop: '2px solid var(--acc1)' }}>
                            {loadingItems === s.shipmentId ? (
                              <div style={{ padding: 16, textAlign: 'center', color: 'var(--txt3)', fontSize: 12 }}>Loading items…</div>
                            ) : items.length > 0 ? (
                              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                                <thead>
                                  <tr>
                                    {[
                                      { label: "", w: 26 },
                                      { label: "ASIN", w: 120 },
                                      { label: "Product / SKU", w: 160 },
                                      { label: "FNSKU", w: 90 },
                                      { label: "FC", w: 64 },
                                      { label: "Expected", w: 58, r: true },
                                      { label: "Received", w: 62, r: true },
                                      { label: "Recv %", w: 80 },
                                      { label: "Variance", w: 72, r: true },
                                      { label: "", w: 54 },
                                    ].map((col, ci) => (
                                      <th key={ci} style={{
                                        ...SG({ fontSize: 7, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '.07em', color: 'var(--txt3)' }),
                                        padding: '6px 8px', textAlign: col.r ? 'right' : 'left',
                                        borderBottom: '1px solid var(--brd2)', background: 'var(--card)',
                                        whiteSpace: 'nowrap', width: col.w,
                                      }}>{col.label}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {items.map((it, idx) => {
                                    const iSent = it.quantityShipped || 0;
                                    const iRecv = it.quantityReceived || 0;
                                    const iDisc = (iSent > 0 || iRecv > 0) ? iRecv - iSent : null;
                                    const iDiscCol = iDisc === null ? 'var(--txt3)' : iDisc < 0 ? '#f87171' : iDisc > 0 ? '#2ECFAA' : 'var(--txt3)';
                                    return (
                                      <tr key={idx} style={{ borderBottom: '1px solid var(--brd)' }}>
                                        <td style={{ padding: '7px 8px', ...SG({ fontSize: 9, color: 'var(--txt3)', fontWeight: 700 }) }}>{idx + 1}</td>
                                        <td style={{ padding: '7px 8px' }}>
                                          <div style={{ ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--acc1)' }) }}>{it.asin || "—"}</div>
                                        </td>
                                        <td style={{ padding: '7px 8px' }}>
                                          <div style={{ fontSize: 10, fontWeight: 600, color: 'var(--txt)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 160 }} title={it.productName || ""}>{it.productName || "—"}</div>
                                          <div style={{ ...SG({ fontSize: 8, color: 'var(--acc3, #3E658C)', marginTop: 1 }) }}>{it.sku}</div>
                                        </td>
                                        <td style={{ padding: '7px 8px', ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>{it.fnsku || "—"}</td>
                                        <td style={{ padding: '7px 8px', ...SG({ fontSize: 10, fontWeight: 700, color: 'var(--txt2)' }) }}>{s.destination || "—"}</td>
                                        <td style={{ padding: '7px 8px', textAlign: 'right', ...SG({ fontSize: 10 }) }}>{iSent.toLocaleString()}</td>
                                        <td style={{ padding: '7px 8px', textAlign: 'right', ...SG({ fontSize: 10, color: iRecv > 0 ? '#2ECFAA' : 'var(--txt3)' }) }}>{iRecv > 0 ? iRecv.toLocaleString() : "—"}</td>
                                        <td style={{ padding: '7px 8px' }}>
                                          {iRecv > 0 && iSent > 0 ? <RecvBar sent={iSent} recv={iRecv} /> : <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>—</span>}
                                        </td>
                                        <td style={{ padding: '7px 8px', textAlign: 'right' }}>
                                          {iDisc === null ? <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>—</span>
                                            : iDisc === 0 ? <span style={{ ...SG({ fontSize: 9, color: 'var(--txt3)' }) }}>0</span>
                                            : <span style={{ ...SG({ fontSize: 9, fontWeight: 700, padding: '1px 5px', borderRadius: 4, background: iDisc < 0 ? 'rgba(248,113,113,.14)' : 'rgba(46,207,170,.12)', color: iDiscCol }) }}>{iDisc > 0 ? `+${iDisc.toLocaleString()}` : iDisc.toLocaleString()}</span>
                                          }
                                        </td>
                                        <td style={{ padding: '7px 8px' }}></td>
                                      </tr>
                                    );
                                  })}
                                </tbody>
                              </table>
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
          </div>
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
            <MiniMetric label="Total Shipments" value={goodShipments.length} valueColor="#7BAED0" note="Excl. cancelled" />
            <MiniMetric label="Active" value={activeShipments.length} valueColor="#E87830" note="In pipeline" />
            <MiniMetric label="Closed" value={closedShipments.length} valueColor="var(--acc1)" note="Fully received" />
            <MiniMetric label="Cancelled" value={cancelledShipments.length} valueColor="#4d6d8a" note="Not shipped" />
            <MiniMetric label="Avg Products / Shipment" value={goodShipments.length > 0 ? (goodShipments.reduce((s, sh) => s + (sh.itemCount || 0), 0) / goodShipments.length).toFixed(1) : "—"} valueColor="var(--txt2)" note="Avg (excl. cancelled)" last />
          </Card>
        </div>
      </div>

      {data?._note && (
        <p style={{ fontSize: 11, color: 'var(--acc2, #E87830)', marginTop: 12, textAlign: 'center' }}>{data._note}</p>
      )}

      {/* Create Shipment Modal */}
      {showCreate && (
        <CreateShipmentModal
          onClose={() => setShowCreate(false)}
          onCreated={() => load(true)}
        />
      )}
    </div>
  );
}
