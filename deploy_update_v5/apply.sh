#!/bin/bash
# GolfGen Deploy Fix v5 — Live "Today" + correct WTD + full order revenue
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5 ==="
echo ""
echo "Fixes:"
echo "  1. Today = actual calendar date (Pacific), orders fetched immediately"
echo "  2. WTD = rolling 7 days (always meaningful, not just today on Monday)"
echo "  3. Orders with no OrderTotal (Pending) now get item-level prices"
echo "  4. Pagination: gets ALL orders, not just first 100"
echo "  5. /api/sync returns in ~10s (full sync continues in background)"
echo "  6. Auto-sync if today has no data when viewing dashboard"
echo ""

cp deploy_update_v5/main.py ./webapp/backend/main.py
echo "✓ main.py updated"

cp deploy_update_v5/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

cp deploy_update_v5/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

echo ""
echo "=== Done! Now run: ==="
echo "  git add -A && git commit -m 'fix: live Today revenue, rolling WTD, order item prices' && git push origin main"
