path = "/Users/eric/Projects/golfgen-dashboard/webapp/frontend/src/pages/ItemMaster.jsx"
with open(path, "r") as f:
    src = f.read()

original = src

# ─── 1. Add ColorCell + SelectCell components before CouponBadge ─────────────
old1 = 'function CouponBadge({ state }) {'
new1 = '''function ColorCell({ value, onSave }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);
  if (!editing) return (
    <span style={{ cursor: "pointer" }} onClick={() => { setDraft(value); setEditing(true); }} title="Click to edit">
      <Badge color={value} />
    </span>
  );
  return (
    <select autoFocus value={draft} onChange={e => setDraft(e.target.value)}
      onBlur={() => { onSave(draft); setEditing(false); }}
      onKeyDown={e => { if (e.key === "Escape") setEditing(false); if (e.key === "Enter") { onSave(draft); setEditing(false); } }}
      style={{ padding: "2px 6px", border: "2px solid var(--teal)", borderRadius: 4, fontSize: 12 }}>
      {["Green", "Blue", "Red", "Orange", "Accessories", ""].map(c => (
        <option key={c} value={c}>{c || "(none)"}</option>
      ))}
    </select>
  );
}

function SelectCell({ value, options, onSave }) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);
  if (!editing) return (
    <span style={{ cursor: "pointer", borderBottom: "1px dashed rgba(0,0,0,0.2)", padding: "2px 4px", fontSize: 12 }}
      onClick={() => { setDraft(value); setEditing(true); }} title="Click to edit">
      {value || "—"}
    </span>
  );
  return (
    <select autoFocus value={draft} onChange={e => setDraft(e.target.value)}
      onBlur={() => { onSave(draft); setEditing(false); }}
      onKeyDown={e => { if (e.key === "Escape") setEditing(false); if (e.key === "Enter") { onSave(draft); setEditing(false); } }}
      style={{ padding: "2px 4px", border: "2px solid var(--teal)", borderRadius: 4, fontSize: 12 }}>
      {options.map(o => <option key={o.value !== undefined ? o.value : o} value={o.value !== undefined ? o.value : o}>{o.label || o || "(none)"}</option>)}
    </select>
  );
}

function CouponBadge({ state }) {'''

assert src.count(old1) == 1, f"old1 not unique: {src.count(old1)}"
src = src.replace(old1, new1)
print("✓ 1 ColorCell+SelectCell added")

# ─── 2. productName cell: replace plain div with EditableCell ─────────────────
old2 = '''                  <td style={{ maxWidth: 280 }}>
                    <div style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                        title={item.productName}>{item.productName}</div>
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.sku}{item.asin ? ` · ${item.asin}` : ""}
                    </div>
                  </td>'''
new2 = '''                  <td style={{ maxWidth: 280 }}>
                    <EditableCell value={item.productName || ""} type="text"
                      onSave={v => handleUpdate(item.asin, "productName", v)} />
                    <div style={{ fontSize: 11, color: "var(--muted)", marginTop: 2, fontFamily: "'Space Grotesk', monospace" }}>
                      {item.sku}{item.asin ? ` · ${item.asin}` : ""}
                    </div>
                  </td>'''
assert src.count(old2) == 1, f"old2 not unique: {src.count(old2)}"
src = src.replace(old2, new2)
print("✓ 2 productName EditableCell")

# ─── 3. color cell: replace Badge with ColorCell ──────────────────────────────
old3 = '                  <td><Badge color={item.color} /></td>'
new3 = '                  <td><ColorCell value={item.color} onSave={v => handleUpdate(item.asin, "color", v)} /></td>'
assert src.count(old3) == 1, f"old3 not unique: {src.count(old3)}"
src = src.replace(old3, new3)
print("✓ 3 color ColorCell")

# ─── 4. brand, series, productType, pieceCount, orientation, category, casepack, carton ─
old4 = '''                  <td style={{ fontSize: 12 }}>{item.brand || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.series || "—"}</td>
                  <td style={{ fontSize: 12 }}>{item.productType || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.pieceCount || "—"}</td>
                  <td>{item.orientation || "—"}</td>
                  <td style={{ fontSize: 12, color: "var(--muted)" }}>{item.category || "—"}</td>
                  <td style={{ textAlign: "right" }}>{item.casepack || item.cartonPack || "—"}</td>
                  <td style={{ textAlign: "right", fontSize: 11, color: "var(--muted)" }}>
                    {item.cartonLength && item.cartonWidth && item.cartonHeight
                      ? `${item.cartonLength}×${item.cartonWidth}×${item.cartonHeight}`
                      : "—"}
                  </td>'''
new4 = '''                  <td style={{ fontSize: 12 }}><EditableCell value={item.brand || ""} type="text" onSave={v => handleUpdate(item.asin, "brand", v)} /></td>
                  <td style={{ fontSize: 12 }}><EditableCell value={item.series || ""} type="text" onSave={v => handleUpdate(item.asin, "series", v)} /></td>
                  <td style={{ fontSize: 12 }}><EditableCell value={item.productType || ""} type="text" onSave={v => handleUpdate(item.asin, "productType", v)} /></td>
                  <td style={{ textAlign: "right" }}><EditableCell value={item.pieceCount || 0} type="number" onSave={v => handleUpdate(item.asin, "pieceCount", Math.round(v))} /></td>
                  <td><SelectCell value={item.orientation || ""} options={["RH", "LH", "Both", "Universal", ""]} onSave={v => handleUpdate(item.asin, "orientation", v)} /></td>
                  <td style={{ fontSize: 12, color: "var(--muted)" }}><EditableCell value={item.category || ""} type="text" onSave={v => handleUpdate(item.asin, "category", v)} /></td>
                  <td style={{ textAlign: "right" }}><EditableCell value={item.casepack || item.cartonPack || 0} type="number" onSave={v => handleUpdate(item.asin, "cartonPack", Math.round(v))} /></td>
                  <td style={{ textAlign: "right", fontSize: 11 }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 2 }}>
                      <EditableCell value={item.cartonLength || 0} type="number" onSave={v => handleUpdate(item.asin, "cartonLength", v)} />
                      <span style={{ color: "var(--muted)" }}>×</span>
                      <EditableCell value={item.cartonWidth || 0} type="number" onSave={v => handleUpdate(item.asin, "cartonWidth", v)} />
                      <span style={{ color: "var(--muted)" }}>×</span>
                      <EditableCell value={item.cartonHeight || 0} type="number" onSave={v => handleUpdate(item.asin, "cartonHeight", v)} />
                    </div>
                  </td>'''
assert src.count(old4) == 1, f"old4 not unique: {src.count(old4)}"
src = src.replace(old4, new4)
print("✓ 4 brand/series/type/pcs/orient/cat/casepack/carton EditableCells")

# ─── 5. Ref Fee cell: make referralPct editable (show fee below) ──────────────
old5 = '''                  <td style={{ textAlign: "right", color: "var(--muted)" }}>
                    {item.referralFee > 0 ? `$${item.referralFee.toFixed(2)}` : "—"}
                  </td>'''
new5 = '''                  <td style={{ textAlign: "right" }}>
                    <EditableCell value={item.referralPct || 0} type="number" suffix="%" onSave={v => handleUpdate(item.asin, "referralPct", v)} />
                    {item.referralFee > 0 && <div style={{ fontSize: 10, color: "var(--muted)" }}>${item.referralFee.toFixed(2)}</div>}
                  </td>'''
assert src.count(old5) == 1, f"old5 not unique: {src.count(old5)}"
src = src.replace(old5, new5)
print("✓ 5 referralPct EditableCell")

# ─── 6. Coupon cell: add editability for couponValue/couponType ───────────────
old6 = '''                  {/* Coupon with end date from Ads API */}
                  <td style={{ textAlign: "right" }}>
                    {item.liveCouponState ? (
                      <div>
                        <span style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 4 }}>
                          {item.couponValue > 0 && (
                            <span style={{ color: "#dc2626", fontSize: 12 }}>
                              {item.couponType === "%" ? `${item.couponValue}%` : `$${item.couponValue}`}
                            </span>
                          )}
                          <CouponBadge state={item.liveCouponState} />
                        </span>
                        {item.couponEndDate && (
                          <div style={{ fontSize: 10, color: "var(--muted)", marginTop: 1, textAlign: "right" }}>
                            ends {fmtDate(item.couponEndDate)}
                          </div>
                        )}
                      </div>
                    ) : item.couponValue > 0 ? (
                      <span style={{ color: "#dc2626" }}>
                        {item.couponType === "%" ? `${item.couponValue}%` : `$${item.couponValue}`} off
                      </span>
                    ) : "—"}
                  </td>'''
new6 = '''                  {/* Coupon with end date from Ads API */}
                  <td style={{ textAlign: "right" }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 3 }}>
                      <EditableCell value={item.couponValue || 0} type="number" onSave={v => handleUpdate(item.asin, "couponValue", v)} />
                      <SelectCell value={item.couponType || "$"} options={[{value:"$",label:"$"},{value:"%",label:"%"}]} onSave={v => handleUpdate(item.asin, "couponType", v)} />
                    </div>
                    {item.liveCouponState && (
                      <div style={{ fontSize: 10, marginTop: 2, display: "flex", alignItems: "center", justifyContent: "flex-end", gap: 4 }}>
                        <CouponBadge state={item.liveCouponState} />
                        {item.couponEndDate && <span style={{ color: "var(--muted)" }}>ends {fmtDate(item.couponEndDate)}</span>}
                      </div>
                    )}
                  </td>'''
assert src.count(old6) == 1, f"old6 not unique: {src.count(old6)}"
src = src.replace(old6, new6)
print("✓ 6 couponValue/couponType EditableCell+SelectCell")

# ─── 7. lyRevenue cell ────────────────────────────────────────────────────────
old7 = '                  <td style={{ textAlign: "right" }}>{fmt$(item.lyRevenue)}</td>'
new7 = '                  <td style={{ textAlign: "right" }}><EditableCell value={item.lyRevenue || 0} type="number" prefix="$" onSave={v => handleUpdate(item.asin, "lyRevenue", v)} /></td>'
assert src.count(old7) == 1, f"old7 not unique: {src.count(old7)}"
src = src.replace(old7, new7)
print("✓ 7 lyRevenue EditableCell")

# ─── 8. lyUnits cell ─────────────────────────────────────────────────────────
old8 = '                  <td style={{ textAlign: "right" }}>{(item.lyUnits || 0).toLocaleString()}</td>'
new8 = '                  <td style={{ textAlign: "right" }}><EditableCell value={item.lyUnits || 0} type="number" onSave={v => handleUpdate(item.asin, "lyUnits", Math.round(v))} /></td>'
assert src.count(old8) == 1, f"old8 not unique: {src.count(old8)}"
src = src.replace(old8, new8)
print("✓ 8 lyUnits EditableCell")

# ─── write ────────────────────────────────────────────────────────────────────
if src == original:
    print("⚠️  No changes made!")
else:
    with open(path, "w") as f:
        f.write(src)
    print(f"\n✅ Done — {src.count('EditableCell') - original.count('EditableCell')} new EditableCell usages added")
