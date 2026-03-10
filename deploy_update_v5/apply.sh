#!/bin/bash
# GolfGen Deploy Fix v5.4 — Item Master + GolfGen WH tabs + returns fix + date filter fix
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5.5 ==="
echo ""
echo "Changes:"
echo "  1. RETURNS FIX: _money() now handles sp_api model objects + PostedDate datetime handling"
echo "  2. RETURNS FIX: _safe_get() accessor works for dicts AND sp_api model objects"
echo "  3. RETURNS FIX: Enhanced debug endpoint shows Python types for root cause analysis"
echo "  4. NEW: Excel upload endpoint — POST /api/upload/warehouse-excel"
echo "  5. NEW: Upload button on GolfGen WH tab for weekly data refresh"
echo "  6. NEW: openpyxl added to requirements for Excel parsing"
echo "  7. All previous fixes (Item Master, GolfGen WH, date filter, backfill, etc.)"
echo ""

cp deploy_update_v5/main.py ./webapp/backend/main.py
echo "✓ main.py updated"

cp deploy_update_v5/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

cp deploy_update_v5/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

# Copy data files
cp deploy_update_v5/item_master.csv ./data/item_master.csv
echo "✓ item_master.csv seeded"

cp deploy_update_v5/warehouse.csv ./data/warehouse.csv
echo "✓ warehouse.csv seeded"

# Sync built frontend dist to where Dockerfile expects it
if [ -d "webapp/frontend/dist" ]; then
  rm -rf webapp/backend/dist
  cp -r webapp/frontend/dist webapp/backend/dist
  echo "✓ Frontend dist synced to webapp/backend/dist"
fi

echo ""
echo "=== Done! Now run: ==="
echo "  git add -A && git commit -m 'fix: returns data parsing + warehouse Excel upload' && git push origin main"
