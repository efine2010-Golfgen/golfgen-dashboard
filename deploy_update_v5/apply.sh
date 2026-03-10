#!/bin/bash
# GolfGen Deploy Fix v5.3 — Item Master tab + returns fix + date filter fix
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5.3 ==="
echo ""
echo "Changes:"
echo "  1. NEW: Item Master tab — 30 SKUs with editable pricing, COGS, planned units, carton dims"
echo "  2. RETURNS/REFUNDS FIX: financial_events table created on startup, INSERT OR REPLACE → INSERT"
echo "  3. DATE FILTER FIX: Item-level profitability now respects 30D/90D/1Y range selector"
echo "  4. AUTO-BACKFILL: Detects missing historical data on startup, fills Apr 2024-present"
echo "  5. CONVERSION CHART FIX: Weekly aggregation was showing 4400% when sessions=0"
echo "  6. All previous fixes (CurrencyAmount, pagination, YoY, etc.)"
echo ""

cp deploy_update_v5/main.py ./webapp/backend/main.py
echo "✓ main.py updated"

cp deploy_update_v5/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

cp deploy_update_v5/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

# Copy item_master.csv to data dir
cp deploy_update_v5/item_master.csv ./data/item_master.csv
echo "✓ item_master.csv seeded"

# Sync built frontend dist to where Dockerfile expects it
if [ -d "webapp/frontend/dist" ]; then
  rm -rf webapp/backend/dist
  cp -r webapp/frontend/dist webapp/backend/dist
  echo "✓ Frontend dist synced to webapp/backend/dist"
fi

echo ""
echo "=== Done! Now run: ==="
echo "  git add -A && git commit -m 'feat: Item Master tab + returns/date filter fixes' && git push origin main"
