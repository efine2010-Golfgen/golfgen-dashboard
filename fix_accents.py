#!/usr/bin/env python3
"""Fix all remaining missing accent bars in Sales.jsx"""

path = '/Users/eric/Projects/golfgen-dashboard/webapp/frontend/src/pages/Sales.jsx'

with open(path, 'r') as f:
    src = f.read()

orig = src
changes = []

# ─────────────────────────────────────────────────────────────────────────────
# 1. MetricCard component — add accent prop + auto-color lookup by label
# ─────────────────────────────────────────────────────────────────────────────
old = "function MetricCard({ label, value, ly, delta, expandContent, invert, goal, goalLabel }) {\n  const [expanded, setExpanded] = useState(false);\n  const isPos = invert ? delta < 0 : delta > 0;"
new = """function MetricCard({ label, value, ly, delta, expandContent, invert, goal, goalLabel, accent }) {
  const [expanded, setExpanded] = useState(false);
  const isPos = invert ? delta < 0 : delta > 0;
  const _lbl = typeof label === 'string' ? label : '';
  const _acc = accent ?? {'Sales $':B.o2,'Unit Sales':B.b2,'AUR':'#F5B731','COGS':B.b3,'Amazon Fees':B.b2,'Returns':'#F5B731','Gross Margin $':B.t2,'Gross Margin %':B.t2,'Sessions':B.t2,'Glance Views':B.t2,'Click Through':B.b2,'Conversion':B.b2,'Ad Spend $':'#F5B731','ROAS':B.o2,'TACOS':'#F5B731'}[_lbl];"""
if old in src:
    src = src.replace(old, new, 1)
    changes.append('MetricCard accent prop + label lookup')
else:
    print("MISS: MetricCard function signature")

# MetricCard outer div — spread accent borderTop
old = "    <div style={{flex:'1 1 0',minWidth:155,background:'linear-gradient(145deg,var(--card),var(--card2))',borderRadius:12,padding:'10px 12px 9px',border:'1px solid var(--brd)',transition:'background .3s'}}>"
new = "    <div style={{flex:'1 1 0',minWidth:155,background:'linear-gradient(145deg,var(--card),var(--card2))',borderRadius:12,padding:'10px 12px 9px',border:'1px solid var(--brd)',transition:'background .3s',...(_acc&&{borderTop:`3px solid ${_acc}`})}}>"
if old in src:
    src = src.replace(old, new, 1)
    changes.append('MetricCard outer div borderTop spread')
else:
    print("MISS: MetricCard outer div")

# ─────────────────────────────────────────────────────────────────────────────
# 2. P&L Waterfall container — add gold border-top  (unique: "P&L Waterfall" span)
# ─────────────────────────────────────────────────────────────────────────────
old = "              return (\n                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,marginBottom:12}}>\n                  <div style={{display:'grid',gridTemplateColumns:'1fr auto 1fr',alignItems:'center',marginBottom:12,gap:8}}>\n                    <span style={{fontSize:10,fontWeight:700,color:'var(--txt2)'}}>P&L Waterfall</span>"
new = "              return (\n                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:'3px solid #F5B731',borderRadius:12,padding:16,marginBottom:12}}>\n                  <div style={{display:'grid',gridTemplateColumns:'1fr auto 1fr',alignItems:'center',marginBottom:12,gap:8}}>\n                    <span style={{fontSize:10,fontWeight:700,color:'var(--txt2)'}}>P&L Waterfall</span>"
if old in src:
    src = src.replace(old, new, 1)
    changes.append('P&L Waterfall gold border-top')
else:
    print("MISS: P&L Waterfall container")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Exec tab Traffic & Conversion Pipeline container — add teal border-top
# ─────────────────────────────────────────────────────────────────────────────
old = "{/* ── Traffic & Conversion Pipeline (exec period-controlled) ── */}\n            <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,marginBottom:12}}>"
new = "{/* ── Traffic & Conversion Pipeline (exec period-controlled) ── */}\n            <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.t2}`,borderRadius:12,padding:16,marginBottom:12}}>"
if old in src:
    src = src.replace(old, new, 1)
    changes.append('Exec Traffic & Conversion teal border-top')
else:
    print("MISS: Exec Traffic & Conversion container")

# ─────────────────────────────────────────────────────────────────────────────
# 4. Daily tab Traffic & Conversion Pipeline container — add teal border-top
# ─────────────────────────────────────────────────────────────────────────────
old = "{/* ── Traffic & Conversion Pipeline ── */}\n            <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,marginBottom:12}}>"
new = "{/* ── Traffic & Conversion Pipeline ── */}\n            <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.t2}`,borderRadius:12,padding:16,marginBottom:12}}>"
if old in src:
    src = src.replace(old, new, 1)
    changes.append('Daily Traffic & Conversion teal border-top')
else:
    print("MISS: Daily Traffic & Conversion container")

# ─────────────────────────────────────────────────────────────────────────────
# 5. Business Health score cards (both exec + daily same style) — deep blue
#    Unique: flexDirection:'column',alignItems:'center',gap:14
# ─────────────────────────────────────────────────────────────────────────────
old = "<div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16,display:'flex',flexDirection:'column',alignItems:'center',gap:14}}>"
new = "<div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.b2}`,borderRadius:12,padding:16,display:'flex',flexDirection:'column',alignItems:'center',gap:14}}>"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Business Health deep-blue border-top ({count}x)')
else:
    print("MISS: Business Health card container")

# ─────────────────────────────────────────────────────────────────────────────
# 6. Actions to Take cards (both exec + daily) — gold border-top
#    Unique: 📋 Actions to Take Today in next line
# ─────────────────────────────────────────────────────────────────────────────
old = "                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>\n                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',marginBottom:12}}>📋 Actions to Take Today</div>"
new = "                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:'3px solid #F5B731',borderRadius:12,padding:16}}>\n                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.1em',color:'var(--txt3)',marginBottom:12}}>📋 Actions to Take Today</div>"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Actions to Take gold border-top ({count}x)')
else:
    print("MISS: Actions to Take container")

# ─────────────────────────────────────────────────────────────────────────────
# 7. Monthly Revenue container — orange border-top
# ─────────────────────────────────────────────────────────────────────────────
old = "      {/* Monthly Revenue YOY — independent chart, NOT controlled by the Charts period bar below */}\n      <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:16,marginBottom:12,transition:'background .3s'}}>"
new = "      {/* Monthly Revenue YOY — independent chart, NOT controlled by the Charts period bar below */}\n      <div style={{background:'var(--surf)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.o2}`,borderRadius:14,padding:16,marginBottom:12,transition:'background .3s'}}>"
if old in src:
    src = src.replace(old, new, 1)
    changes.append('Monthly Revenue orange border-top')
else:
    print("MISS: Monthly Revenue container")

# ─────────────────────────────────────────────────────────────────────────────
# 8. Traffic KPIs · MTD sidebar cards (both exec + daily) — teal border-top
# ─────────────────────────────────────────────────────────────────────────────
old = "                  <div style={{background:'var(--card2)',border:'1px solid var(--brd)',borderRadius:10,padding:'12px 14px'}}>\n                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:8}}>Traffic KPIs \u00b7 MTD</div>"
new = "                  <div style={{background:'var(--card2)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.t2}`,borderRadius:10,padding:'12px 14px'}}>\n                    <div style={{fontSize:9,fontWeight:700,textTransform:'uppercase',letterSpacing:'.08em',color:'var(--txt3)',marginBottom:8}}>Traffic KPIs \u00b7 MTD</div>"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Traffic KPIs MTD teal border-top ({count}x)')
else:
    print("MISS: Traffic KPIs MTD sidebar")

# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────
if src == orig:
    print("ERROR: No changes made!")
else:
    with open(path, 'w') as f:
        f.write(src)
    print(f"SUCCESS — {len(changes)} change groups applied:")
    for c in changes:
        print(f"  ✓ {c}")
