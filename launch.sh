#!/bin/bash
# ═══════════════════════════════════════════════════════
# GolfGen Dashboard — Full Launch
# ═══════════════════════════════════════════════════════
# Starts the dashboard server AND the Cloudflare tunnel
# so your team can access the dashboard from anywhere.
#
# Usage:
#   ./launch.sh          # Start dashboard + tunnel
#   ./launch.sh quick    # Start dashboard + quick tunnel (temp URL)
# ═══════════════════════════════════════════════════════

cd "$(dirname "$0")"

PORT="${PORT:-8000}"
MODE="${1:-start}"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   GolfGen Dashboard — Full Launch               ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Start the dashboard server in background
echo "🚀 Starting dashboard server on port $PORT..."
PORT=$PORT ./start.sh &
DASH_PID=$!

# Wait for server to be ready
echo "⏳ Waiting for server to start..."
for i in {1..15}; do
    if curl -s "http://localhost:$PORT/api/health" > /dev/null 2>&1; then
        echo "✅ Dashboard server is running!"
        break
    fi
    sleep 1
done

echo ""

# Start tunnel
if command -v cloudflared &> /dev/null; then
    if [ "$MODE" = "quick" ]; then
        echo "🌐 Starting quick tunnel (temporary URL)..."
        cloudflared tunnel --url "http://localhost:$PORT"
    else
        echo "🌐 Starting Cloudflare tunnel..."
        ./setup_tunnel.sh start
    fi
else
    echo "⚠️  cloudflared not found. Install it with:"
    echo "   ./setup_tunnel.sh install"
    echo ""
    echo "📊 Dashboard is running locally at: http://localhost:$PORT"
    echo "   Press Ctrl+C to stop"
    wait $DASH_PID
fi

# Cleanup on exit
kill $DASH_PID 2>/dev/null
