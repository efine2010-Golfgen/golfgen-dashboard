#!/bin/bash
# GolfGen Deploy Fix v5 — Live "Today" + correct WTD + full order revenue
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5 ==="
echo ""
echo "Fixes:"
echo "  1. Today revenue: getOrderItems for ALL orders (not just \$0 ones)"
echo "  2. Item-level prices as ground truth (OrderTotal is unreliable for FBA)"
echo "  3. Financial Events API timezone fix (PostedAfter now has Z suffix)"
echo "  4. Better rate limiting: burst 25, then 2s sleep, 5s on throttle"
echo "  5. Financial Events logging to diagnose refund data"
echo "  6. WTD = rolling 7 days, auto-sync on dashboard load"
echo ""

cp deploy_update_v5/main.py ./webapp/backend/main.py
echo "✓ main.py updated"

cp deploy_update_v5/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

cp deploy_update_v5/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

echo ""
echo "=== Done! Now run: ==="
echo "  git add -A && git commit -m 'fix: accurate Today via all order items, Financial Events tz fix' && git push origin main"
