#!/usr/bin/env python3
"""Fix remaining missing accent bars — round 2"""

path = '/Users/eric/Projects/golfgen-dashboard/webapp/frontend/src/pages/Sales.jsx'

with open(path, 'r') as f:
    src = f.read()

orig = src
changes = []

# ─────────────────────────────────────────────────────────────────────────────
# 1. Hourly heatmap containers (exec + daily, both identical style)
#    padding:'14px 16px' distinguishes these from ChartCard (which uses padding:16)
# ─────────────────────────────────────────────────────────────────────────────
old = "background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:'14px 16px',marginBottom:10}}"
new = "background:'var(--surf)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.b2}`,borderRadius:14,padding:'14px 16px',marginBottom:10}}"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Hourly heatmap deep-blue border-top ({count}x)')
else:
    print("MISS: Hourly heatmap containers")

# ─────────────────────────────────────────────────────────────────────────────
# 2. 26-week heatmap container (unique: padding:16,marginBottom:12,transition)
#    Note: Monthly Revenue already fixed (has borderTop for orange), so this
#    is the only remaining instance of this exact style string
# ─────────────────────────────────────────────────────────────────────────────
old = "background:'var(--surf)',border:'1px solid var(--brd)',borderRadius:14,padding:16,marginBottom:12,transition:'background .3s'}}"
new = "background:'var(--surf)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.b2}`,borderRadius:14,padding:16,marginBottom:12,transition:'background .3s'}}"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'26-week heatmap deep-blue border-top ({count}x)')
else:
    print("MISS: 26-week heatmap container")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Daily tab DOS gauge card — teal (inventory health)
#    Unique context: marginBottom:12,flexWrap:'wrap',gap:10 + "Current DOS"
# ─────────────────────────────────────────────────────────────────────────────
old = "                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:12,padding:16}}>\n                    <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:12,flexWrap:'wrap',gap:10}}>\n                      <div>\n                        <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:4}}>Current DOS</div>"
new = "                  <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.t2}`,borderRadius:12,padding:16}}>\n                    <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:12,flexWrap:'wrap',gap:10}}>\n                      <div>\n                        <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:4}}>Current DOS</div>"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Daily DOS gauge teal border-top ({count}x)')
else:
    print("MISS: Daily DOS gauge container")

# ─────────────────────────────────────────────────────────────────────────────
# 4. Day-of-Week "Best Day" card — orange top accent (reinforces orange theme)
# ─────────────────────────────────────────────────────────────────────────────
old = "background:'rgba(232,130,30,.04)',border:'1px solid rgba(232,130,30,.3)',borderRadius:10,padding:'12px 14px',display:'flex',alignItems:'center',gap:14}}"
new = "background:'rgba(232,130,30,.04)',border:'1px solid rgba(232,130,30,.3)',borderTop:'3px solid #E8821E',borderRadius:10,padding:'12px 14px',display:'flex',alignItems:'center',gap:14}}"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Day-of-Week Best Day orange border-top ({count}x)')
else:
    print("MISS: Best Day card")

# ─────────────────────────────────────────────────────────────────────────────
# 5. Day-of-Week "Softest Day" card — light blue accent
# ─────────────────────────────────────────────────────────────────────────────
old = "background:'var(--card)',border:'1px solid rgba(100,116,139,.25)',borderRadius:10,padding:'12px 14px',display:'flex',alignItems:'center',gap:14}}"
new = "background:'var(--card)',border:'1px solid rgba(100,116,139,.25)',borderTop:`3px solid ${B.b3}`,borderRadius:10,padding:'12px 14px',display:'flex',alignItems:'center',gap:14}}"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Day-of-Week Softest Day light-blue border-top ({count}x)')
else:
    print("MISS: Softest Day card")

# ─────────────────────────────────────────────────────────────────────────────
# 6. 26-week heatmap inner DOW-averages panel — add subtle card accent
# ─────────────────────────────────────────────────────────────────────────────
old = "marginTop:12,padding:'12px 14px',background:'var(--card)',borderRadius:10,border:'1px solid var(--brd)'}}"
new = "marginTop:12,padding:'12px 14px',background:'var(--card)',borderRadius:10,border:'1px solid var(--brd)',borderTop:`3px solid ${B.b3}`}}"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'DOW averages panel light-blue border-top ({count}x)')
else:
    print("MISS: DOW averages inner panel")

# ─────────────────────────────────────────────────────────────────────────────
# 7. Advertising mini-cards (Ad Spend, ROAS, TACOS, etc.) in daily tab
#    These are small inline cards: borderRadius:10,padding:'10px 12px',minWidth:130,flex:1
#    We need to assign colors per card — but they're inline, not components.
#    Strategy: target each one by the label div text inside
# ─────────────────────────────────────────────────────────────────────────────
# Ad Spend card — gold
old = "                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:10,padding:'10px 12px',minWidth:130,flex:1}}>\n                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:6}}>Ad Spend $</div>"
new = "                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:'3px solid #F5B731',borderRadius:10,padding:'10px 12px',minWidth:130,flex:1}}>\n                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:6}}>Ad Spend $</div>"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'Ad Spend card gold border-top ({count}x)')
else:
    print("MISS: Ad Spend card")

# ROAS card — orange
old = "                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderRadius:10,padding:'10px 12px',minWidth:130,flex:1}}>\n                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:6}}>ROAS</div>"
new = "                <div style={{background:'var(--card)',border:'1px solid var(--brd)',borderTop:`3px solid ${B.o2}`,borderRadius:10,padding:'10px 12px',minWidth:130,flex:1}}>\n                  <div style={{fontSize:9,textTransform:'uppercase',letterSpacing:'.07em',color:'var(--txt3)',marginBottom:6}}>ROAS</div>"
count = src.count(old)
if count > 0:
    src = src.replace(old, new)
    changes.append(f'ROAS card orange border-top ({count}x)')
else:
    print("MISS: ROAS card")

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
