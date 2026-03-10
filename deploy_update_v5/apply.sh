#!/bin/bash
# GolfGen Deploy Fix v5.2 — Returns/refunds fix + auto-backfill + chart fixes
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5.2 ==="
echo ""
echo "Fixes:"
echo "  1. RETURNS/REFUNDS FIX: financial_events table created on startup, INSERT OR REPLACE → INSERT"
echo "  2. AUTO-BACKFILL: Detects missing historical data on startup, fills Apr 2024-present"
echo "  3. CONVERSION CHART FIX: Weekly aggregation was showing 4400% when sessions=0"
echo "  4. ASIN price fallback: Today's orders use historical avg price for pending items"
echo "  5. DuckDB interval syntax: CURRENT_DATE - 90 instead of INTERVAL"
echo "  6. All previous fixes (CurrencyAmount, pagination, YoY, etc.)"
echo ""

cp deploy_update_v5/main.py ./webapp/backend/main.py
echo "✓ main.py updated"

cp deploy_update_v5/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

cp deploy_update_v5/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

# Sync built frontend dist to where Dockerfile expects it
if [ -d "webapp/frontend/dist" ]; then
  rm -rf webapp/backend/dist
  cp -r webapp/frontend/dist webapp/backend/dist
  echo "✓ Frontend dist synced to webapp/backend/dist"
fi

echo ""
echo "=== Done! Now run: ==="
echo "  git add -A && git commit -m 'fix: returns/refunds data, auto-backfill, conversion chart' && git push origin main"
