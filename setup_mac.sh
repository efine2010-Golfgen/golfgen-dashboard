#!/bin/bash
# ═══════════════════════════════════════════════════════
# GolfGen Amazon Dashboard — Mac Setup Script
# ═══════════════════════════════════════════════════════
# Run this once to set up your local environment.
# Usage: chmod +x setup_mac.sh && ./setup_mac.sh
# ═══════════════════════════════════════════════════════

set -e

echo ""
echo "  ╔═══════════════════════════════════════════╗"
echo "  ║   GolfGen Amazon Dashboard — Mac Setup    ║"
echo "  ╚═══════════════════════════════════════════╝"
echo ""

# Check Python 3
echo "→ Checking Python 3..."
if command -v python3 &>/dev/null; then
    PY=$(python3 --version)
    echo "  ✅ $PY"
else
    echo "  ❌ Python 3 not found."
    echo "  Install it with: brew install python3"
    echo "  Or download from: https://www.python.org/downloads/"
    exit 1
fi

# Check pip
echo "→ Checking pip..."
if python3 -m pip --version &>/dev/null; then
    echo "  ✅ pip available"
else
    echo "  ❌ pip not found. Installing..."
    python3 -m ensurepip --upgrade
fi

# Install dependencies
echo ""
echo "→ Installing Python packages..."
python3 -m pip install --upgrade pip --quiet
python3 -m pip install python-amazon-sp-api duckdb requests --quiet
echo "  ✅ Packages installed:"
echo "     - python-amazon-sp-api (Amazon SP-API client)"
echo "     - duckdb (local analytics database)"
echo "     - requests (HTTP library)"

# Create data directory
echo ""
echo "→ Creating data directory..."
mkdir -p data
echo "  ✅ data/ directory ready"

# Check for credentials
echo ""
echo "→ Checking credentials..."
if [ -f "config/credentials.json" ]; then
    echo "  ✅ credentials.json exists"

    # Check if it still has placeholder values
    if grep -q "YOUR_" config/credentials.json; then
        echo ""
        echo "  ⚠️  credentials.json still has placeholder values."
        echo "  Open it and fill in your real SP-API credentials:"
        echo "     open -e config/credentials.json"
    else
        echo "  ✅ Credentials appear to be filled in"
    fi
else
    echo "  ⚠️  No credentials.json found."
    echo "  Creating from template..."
    cp config/credentials_template.json config/credentials.json
    echo "  ✅ Created config/credentials.json"
    echo ""
    echo "  📝 Next: Open and fill in your SP-API credentials:"
    echo "     open -e config/credentials.json"
    echo ""
    echo "  See SP_API_SETUP_GUIDE.html for step-by-step instructions"
    echo "  on getting each credential."
fi

# Test the pipeline (dry run)
echo ""
echo "═══════════════════════════════════════════════"
echo "  Setup Complete!"
echo "═══════════════════════════════════════════════"
echo ""
echo "  Next steps:"
echo ""
echo "  1. Fill in your credentials:"
echo "     open -e config/credentials.json"
echo ""
echo "  2. Pull data (after adding credentials):"
echo "     python3 scripts/amazon_sp_api.py 90    # backfill 90 days"
echo ""
echo "  3. Generate dashboard:"
echo "     python3 scripts/generate_dashboard.py"
echo ""
echo "  4. Open dashboard:"
echo "     open amazon_dashboard_v3.html"
echo ""
echo "  Need help getting credentials?"
echo "     open SP_API_SETUP_GUIDE.html"
echo ""
