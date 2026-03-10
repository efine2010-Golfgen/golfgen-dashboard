#!/bin/bash
# ═══════════════════════════════════════════════════════
# GolfGen Dashboard — Start Server
# ═══════════════════════════════════════════════════════
# Launches the dashboard on http://localhost:8000
# Serves both the React frontend and the API from a
# single FastAPI server.
#
# Usage:
#   ./start.sh           # Start on default port 8000
#   PORT=3000 ./start.sh # Start on custom port
# ═══════════════════════════════════════════════════════

cd "$(dirname "$0")"

PORT="${PORT:-8000}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   GolfGen Dashboard — Starting Server           ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Kill any existing process on this port
EXISTING_PID=$(lsof -ti :$PORT 2>/dev/null)
if [ -n "$EXISTING_PID" ]; then
    echo "⚠️  Port $PORT already in use (PID: $EXISTING_PID). Stopping it..."
    kill $EXISTING_PID 2>/dev/null
    sleep 1
    # Force kill if still running
    kill -9 $EXISTING_PID 2>/dev/null 2>&1
    sleep 1
    echo "✅ Port $PORT cleared."
fi

# Check dependencies
if ! python3 -c "import fastapi, uvicorn, duckdb" 2>/dev/null; then
    echo "📦 Installing Python dependencies..."
    pip3 install fastapi uvicorn[standard] duckdb python-multipart --break-system-packages -q
fi

# Check that frontend is built
if [ ! -f "webapp/frontend/dist/index.html" ]; then
    echo "🔨 Building frontend..."
    cd webapp/frontend && npm run build && cd ../..
fi

# Check that DB exists
if [ ! -f "data/golfgen_amazon.duckdb" ]; then
    echo "⚠️  No database found at data/golfgen_amazon.duckdb"
    echo "   Run ./refresh_dashboard.sh first to pull data from Amazon SP-API."
    exit 1
fi

echo "🌐 Starting GolfGen Dashboard on http://localhost:$PORT"
echo "   Press Ctrl+C to stop"
echo ""

cd webapp/backend
PORT=$PORT python3 -c "
import uvicorn, os
port = int(os.environ.get('PORT', 8000))
uvicorn.run('main:app', host='0.0.0.0', port=port, log_level='info')
"
