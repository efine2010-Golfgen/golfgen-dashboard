#!/bin/bash
set -e

echo "=== GolfGen Dashboard Update ==="
echo "Applying fixes: Dockerfile, main.py, Dashboard.jsx, frontend build"
echo ""

# Check we're in the right directory
if [ ! -f "webapp/backend/main.py" ]; then
    echo "ERROR: Run this from the golfgen_amazon_dashboard root folder"
    exit 1
fi

DIR="$(cd "$(dirname "$0")" && pwd)"

cp "$DIR/Dockerfile" ./Dockerfile
echo "✓ Updated Dockerfile"

cp "$DIR/backend_Dockerfile" ./webapp/backend/Dockerfile
echo "✓ Updated webapp/backend/Dockerfile"

cp "$DIR/main.py" ./webapp/backend/main.py
echo "✓ Updated webapp/backend/main.py"

cp "$DIR/Dashboard.jsx" ./webapp/frontend/src/pages/Dashboard.jsx
echo "✓ Updated webapp/frontend/src/pages/Dashboard.jsx"

# Update built frontend files
mkdir -p webapp/backend/dist/assets
cp "$DIR/index-cmySBW-i.js" ./webapp/backend/dist/assets/index-cmySBW-i.js
cp "$DIR/dist_index.html" ./webapp/backend/dist/index.html
echo "✓ Updated webapp/backend/dist/ (built frontend)"

mkdir -p webapp/frontend/dist/assets
cp "$DIR/fe_index-cmySBW-i.js" ./webapp/frontend/dist/assets/index-cmySBW-i.js 2>/dev/null || true
cp "$DIR/fe_dist_index.html" ./webapp/frontend/dist/index.html 2>/dev/null || true
echo "✓ Updated webapp/frontend/dist/ (built frontend)"

echo ""
echo "=== All files updated. Now run: ==="
echo "  git add -A"
echo "  git commit -m 'Add COGS, Amazon Fees, P&L waterfall; fix Dockerfile deployment'"
echo "  git push origin main"
