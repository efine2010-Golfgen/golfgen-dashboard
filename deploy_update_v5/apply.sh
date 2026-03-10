#!/bin/bash
# GolfGen Deploy Fix v5 — Live "Today" + correct WTD + full order revenue
# Run from golfgen-dashboard root: bash deploy_update_v5/apply.sh
set -e

echo "=== GolfGen Deploy Fix v5 ==="
echo ""
echo "Fixes:"
echo "  1. Monthly YoY: 12 months x 3 years (2024/2025/2026)"
echo "  2. Traffic funnel: sessions area + orders/units bars + conv overlay"
echo "  3. Conversion vs AUR: dual axis showing pricing vs conversion"
echo "  4. Revenue by Product donut moved to Products tab"
echo "  5. NEW: Sales by Color (% of total) chart on Dashboard"
echo "  6. Financial Events: pagination (up to 20 pages) + 90 days window"
echo "  7. NEW: /api/color-mix endpoint for color breakdown"
echo ""

cp deploy_update_v5/main.py ./webapp/backend/main.py
echo "✓ main.py updated"

cp deploy_update_v5/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated"

cp deploy_update_v5/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated"

echo ""
echo "=== Done! Now run: ==="
echo "  git add -A && git commit -m 'feat: YoY 12mo chart, conv vs AUR, color mix, products donut, fin events pagination' && git push origin main"
