#!/bin/bash
# GolfGen Deploy Fix v4 — Complete fix for profitability data + real-time sync
# Run from golfgen-dashboard root: bash deploy_update_v4/apply.sh
set -e

echo "=== GolfGen Deploy Fix v4 ==="
echo "Fixes:"
echo "  1. FBA fee estimates fixed (was WAY too low)"
echo "  2. Financial Events API sync (actual fees, refunds, promos)"
echo "  3. Orders API sync (real-time today data)"
echo "  4. Background sync every 2 hours on Railway"
echo ""

# 1. Update main.py (all backend fixes)
cp deploy_update_v4/main.py ./webapp/backend/main.py
echo "✓ main.py updated (Financial Events sync + FBA fee fix + Orders API)"

# 2. Update requirements.txt
cp deploy_update_v4/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

# 3. Update Dockerfile (should already be correct, but ensure it's latest)
cp deploy_update_v4/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

echo ""
echo "=== Done! Now run: ==="
echo "  git add Dockerfile webapp/backend/main.py webapp/backend/requirements.txt"
echo "  git commit -m 'fix: FBA fees, financial events sync, real-time orders'"
echo "  git push origin main"
echo ""
echo "After deploy completes on Railway, trigger a sync to pull fresh data:"
echo "  curl https://golfgen-dashboard-production.up.railway.app/api/sync"
echo ""
echo "REQUIRED: These env vars must be set in Railway dashboard:"
echo "  SP_API_REFRESH_TOKEN"
echo "  SP_API_LWA_APP_ID"
echo "  SP_API_LWA_CLIENT_SECRET"
echo "  SP_API_AWS_ACCESS_KEY"
echo "  SP_API_AWS_SECRET_KEY"
echo "  SP_API_ROLE_ARN"
