#!/bin/bash
# GolfGen Deploy Fix v5 — Financial fixes + backfill + chart improvements
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5 ==="
echo ""
echo "Fixes:"
echo "  1. CurrencyAmount fix: returns/refunds were $0 due to wrong field name"
echo "  2. Financial Events: manual NextToken pagination (up to 20 pages)"
echo "  3. Today data: overwrite protection (never clobber good data with worse)"
echo "  4. Monthly YoY: always 3 years (2024/2025/2026) as grouped bars"
echo "  5. NEW: /api/backfill endpoint to pull historical 2024 data"
echo "  6. Traffic funnel: sessions area + orders/units bars + conv overlay"
echo "  7. All charts full-width with improved visibility"
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
echo "  git add -A && git commit -m 'fix: CurrencyAmount, overwrite protect, backfill, YoY 3 bars' && git push origin main"
