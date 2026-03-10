#!/bin/bash
# GolfGen Deploy Fix v5 — Live "Today" data from Orders API
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5 ==="
echo ""
echo "Fixes:"
echo "  1. 'Today' now uses actual calendar date (Pacific timezone)"
echo "     Was: anchored to MAX(date) in database"
echo "     Now: always today's real date"
echo ""
echo "  2. Today's orders sync runs FIRST at startup (~5 seconds)"
echo "     Was: waited for 5-min Sales Report before pulling orders"
echo "     Now: orders pulled immediately, Sales Report runs after"
echo ""
echo "  3. Auto-sync on dashboard load"
echo "     If today has no data when you view profitability/comparison,"
echo "     it auto-triggers a quick Orders API pull (5-min cooldown)"
echo ""
echo "  4. /api/sync is now fast (returns in ~5s, full sync continues in background)"
echo ""
echo "  5. Pagination for Orders API (gets ALL orders, not just first 100)"
echo ""

# 1. Update main.py
cp deploy_update_v5/main.py ./webapp/backend/main.py
echo "✓ main.py updated"

# 2. Update requirements.txt (unchanged, but keep in sync)
cp deploy_update_v5/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

# 3. Update Dockerfile (unchanged, but keep in sync)
cp deploy_update_v5/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

echo ""
echo "=== Done! Now run: ==="
echo "  git add -A && git commit -m 'fix: live Today data - Orders API runs first at startup' && git push origin main"
echo ""
echo "After deploy, Today will show live order data within seconds of startup."
