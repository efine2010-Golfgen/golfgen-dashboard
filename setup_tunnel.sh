#!/bin/bash
# ═══════════════════════════════════════════════════════
# GolfGen Dashboard — Cloudflare Tunnel Setup
# ═══════════════════════════════════════════════════════
# Creates a permanent public URL for your dashboard so
# your team can access it from anywhere.
#
# One-time setup:
#   1. ./setup_tunnel.sh install   — Install cloudflared
#   2. ./setup_tunnel.sh login     — Login to Cloudflare (opens browser)
#   3. ./setup_tunnel.sh create    — Create tunnel + get URL
#   4. ./setup_tunnel.sh start     — Start the tunnel
#
# After setup, daily usage:
#   ./start.sh &                    — Start the dashboard
#   ./setup_tunnel.sh start         — Start the tunnel
#
# Or use the combined launcher:
#   ./launch.sh                     — Starts both dashboard + tunnel
# ═══════════════════════════════════════════════════════

cd "$(dirname "$0")"

TUNNEL_NAME="golfgen-dashboard"
PORT="${PORT:-8000}"

case "$1" in
    install)
        echo "📦 Installing cloudflared..."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            brew install cloudflare/cloudflare/cloudflared
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
            chmod +x /usr/local/bin/cloudflared
        fi
        echo "✅ cloudflared installed!"
        cloudflared --version
        ;;

    login)
        echo "🔐 Opening Cloudflare login in your browser..."
        echo "   Select the domain you want to use (or use the free .cfargotunnel.com domain)"
        cloudflared tunnel login
        ;;

    create)
        echo "🔧 Creating tunnel '$TUNNEL_NAME'..."
        cloudflared tunnel create "$TUNNEL_NAME"

        # Get tunnel ID
        TUNNEL_ID=$(cloudflared tunnel list | grep "$TUNNEL_NAME" | awk '{print $1}')

        if [ -z "$TUNNEL_ID" ]; then
            echo "❌ Failed to create tunnel"
            exit 1
        fi

        echo ""
        echo "✅ Tunnel created! ID: $TUNNEL_ID"
        echo ""

        # Create config
        mkdir -p ~/.cloudflared
        cat > ~/.cloudflared/config-golfgen.yml <<EOF
tunnel: $TUNNEL_ID
credentials-file: $HOME/.cloudflared/$TUNNEL_ID.json

ingress:
  - service: http://localhost:$PORT
EOF

        echo "📝 Config saved to ~/.cloudflared/config-golfgen.yml"
        echo ""
        echo "Your dashboard URL will be:"
        echo "   https://$TUNNEL_ID.cfargotunnel.com"
        echo ""
        echo "To use a custom domain (e.g., dashboard.golfgen.com):"
        echo "   cloudflared tunnel route dns $TUNNEL_NAME dashboard.golfgen.com"
        echo ""
        ;;

    start)
        echo "🌐 Starting Cloudflare tunnel for GolfGen Dashboard..."
        echo "   Dashboard must be running on localhost:$PORT"
        echo ""

        if [ -f "$HOME/.cloudflared/config-golfgen.yml" ]; then
            cloudflared tunnel --config "$HOME/.cloudflared/config-golfgen.yml" run "$TUNNEL_NAME"
        else
            # Quick tunnel (no config needed, temporary URL)
            echo "   No tunnel config found. Using quick tunnel (temporary URL)..."
            echo "   Run './setup_tunnel.sh create' first for a permanent URL."
            echo ""
            cloudflared tunnel --url http://localhost:$PORT
        fi
        ;;

    quick)
        echo "🚀 Starting quick tunnel (temporary public URL)..."
        echo "   Dashboard must be running on localhost:$PORT"
        echo ""
        cloudflared tunnel --url http://localhost:$PORT
        ;;

    *)
        echo "GolfGen Dashboard — Tunnel Setup"
        echo ""
        echo "Usage:"
        echo "  ./setup_tunnel.sh install   Install cloudflared CLI"
        echo "  ./setup_tunnel.sh login     Login to Cloudflare (one-time)"
        echo "  ./setup_tunnel.sh create    Create permanent tunnel"
        echo "  ./setup_tunnel.sh start     Start the tunnel"
        echo "  ./setup_tunnel.sh quick     Quick tunnel (temp URL, no login needed)"
        echo ""
        ;;
esac
