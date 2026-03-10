#!/bin/bash
# ═══════════════════════════════════════════════════════
# GolfGen Dashboard — Live Refresh
# ═══════════════════════════════════════════════════════
# Pulls fresh data from Amazon SP-API (last 7 days default)
# then regenerates the branded v3 dashboard with live data.
#
# Usage:
#   ./refresh_dashboard.sh           # Pull last 7 days + generate
#   ./refresh_dashboard.sh 1         # Pull last 1 day (today only) + generate
#   ./refresh_dashboard.sh 30        # Pull last 30 days + generate
#   ./refresh_dashboard.sh --no-pull # Skip data pull, just regenerate
# ═══════════════════════════════════════════════════════

cd "$(dirname "$0")"

DAYS="${1:-7}"
SKIP_PULL=false

if [ "$1" = "--no-pull" ]; then
    SKIP_PULL=true
fi

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   GolfGen Dashboard — Live Refresh              ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

if [ "$SKIP_PULL" = false ]; then
    echo "🔄 Step 1: Pulling fresh data from SP-API (last $DAYS days)..."
    echo "─────────────────────────────────────────────────────"
    python3 scripts/amazon_sp_api.py "$DAYS"
    echo ""

    echo "📣 Step 2: Pulling advertising data from Amazon Ads API..."
    echo "─────────────────────────────────────────────────────"
    python3 scripts/amazon_ads_api.py "$DAYS" 2>/dev/null || echo "  ⚠ Ads API not configured — skipping (see config/credentials_template.json)"
    echo ""
fi

echo "📊 Step 3: Generating dashboard with live data..."
echo "─────────────────────────────────────────────────────"
python3 scripts/generate_dashboard.py
echo ""

echo "🌐 Step 4: Opening dashboard..."
if command -v open &> /dev/null; then
    open amazon_dashboard_live.html
elif command -v xdg-open &> /dev/null; then
    xdg-open amazon_dashboard_live.html
fi

echo "✅ Done! Dashboard is live at: amazon_dashboard_live.html"
echo ""
