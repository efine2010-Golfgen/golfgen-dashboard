#!/bin/bash
set -e
echo "=== GolfGen Dashboard Update v2 ==="
echo "Fixes: Background SP-API sync + Profitability tab COGS/fees"
echo ""

if [ ! -f "webapp/backend/main.py" ]; then
    echo "ERROR: Run this from the golfgen_amazon_dashboard root folder"
    exit 1
fi

DIR="$(cd "$(dirname "$0")" && pwd)"

cp "$DIR/Dockerfile" ./Dockerfile
echo "✓ Dockerfile (now copies config/ for SP-API creds)"

cp "$DIR/main.py" ./webapp/backend/main.py
echo "✓ main.py (background sync + profitability fix)"

cp "$DIR/requirements.txt" ./webapp/backend/requirements.txt
echo "✓ requirements.txt (added python-amazon-sp-api)"

mkdir -p webapp/backend/dist/assets webapp/frontend/dist/assets
cp "$DIR/index-cmySBW-i.js" ./webapp/backend/dist/assets/index-cmySBW-i.js
cp "$DIR/index-cmySBW-i.js" ./webapp/frontend/dist/assets/index-cmySBW-i.js 2>/dev/null || true
cp "$DIR/dist_index.html" ./webapp/backend/dist/index.html
cp "$DIR/dist_index.html" ./webapp/frontend/dist/index.html 2>/dev/null || true
echo "✓ Frontend dist files"

echo ""
echo "Done! Now run:"
echo "  git add -A && git commit -m 'Add live SP-API sync + fix Profitability COGS' && git push origin main"
