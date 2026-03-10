#!/bin/bash
# ═══════════════════════════════════════════════════════
# GolfGen Dashboard — Background Service Installer
# ═══════════════════════════════════════════════════════
# Installs launchd services so the dashboard + tunnel
# start automatically when your Mac boots — no Terminal needed.
#
# Usage:
#   bash services/install.sh          Install & start services
#   bash services/install.sh stop     Stop services
#   bash services/install.sh start    Start services
#   bash services/install.sh status   Check if running
#   bash services/install.sh url      Show current public URL
#   bash services/install.sh uninstall Remove services entirely
#   bash services/install.sh logs     Show recent server logs
# ═══════════════════════════════════════════════════════

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
SERVER_LABEL="com.golfgen.dashboard.server"
TUNNEL_LABEL="com.golfgen.dashboard.tunnel"
SERVER_PLIST="$LAUNCH_AGENTS/$SERVER_LABEL.plist"
TUNNEL_PLIST="$LAUNCH_AGENTS/$TUNNEL_LABEL.plist"
LOG_DIR="$SCRIPT_DIR/logs"

mkdir -p "$LAUNCH_AGENTS" "$LOG_DIR"

# Make wrapper scripts executable
chmod +x "$SCRIPT_DIR/golfgen-server.sh" 2>/dev/null
chmod +x "$SCRIPT_DIR/golfgen-tunnel.sh" 2>/dev/null

case "${1:-install}" in
    install)
        echo ""
        echo "╔══════════════════════════════════════════════════╗"
        echo "║   GolfGen Dashboard — Installing Services       ║"
        echo "╚══════════════════════════════════════════════════╝"
        echo ""

        # Stop existing services if running
        launchctl bootout "gui/$(id -u)/$SERVER_LABEL" 2>/dev/null
        launchctl bootout "gui/$(id -u)/$TUNNEL_LABEL" 2>/dev/null
        sleep 1

        # Kill any leftover process on port 8000
        EXISTING_PID=$(lsof -ti :8000 2>/dev/null)
        if [ -n "$EXISTING_PID" ]; then
            echo "Clearing port 8000..."
            kill $EXISTING_PID 2>/dev/null
            sleep 1
            kill -9 $EXISTING_PID 2>/dev/null 2>&1
        fi

        # ── Server plist ──
        cat > "$SERVER_PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$SERVER_LABEL</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/golfgen-server.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$PROJECT_DIR</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/server.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/server-error.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PORT</key>
        <string>8000</string>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
    <key>ThrottleInterval</key>
    <integer>5</integer>
</dict>
</plist>
PLIST
        echo "✅ Server service created"

        # ── Tunnel plist ──
        cat > "$TUNNEL_PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$TUNNEL_LABEL</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>$SCRIPT_DIR/golfgen-tunnel.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$PROJECT_DIR</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/tunnel.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/tunnel-error.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
    <key>ThrottleInterval</key>
    <integer>10</integer>
</dict>
</plist>
PLIST
        echo "✅ Tunnel service created"

        # Load and start both
        echo ""
        echo "Starting services..."
        launchctl bootstrap "gui/$(id -u)" "$SERVER_PLIST"
        sleep 3
        launchctl bootstrap "gui/$(id -u)" "$TUNNEL_PLIST"

        echo ""
        echo "⏳ Waiting for tunnel URL (up to 30 seconds)..."
        for i in {1..30}; do
            if [ -f "$LOG_DIR/tunnel-url.txt" ]; then
                URL=$(cat "$LOG_DIR/tunnel-url.txt")
                if [[ "$URL" == https://* ]]; then
                    echo ""
                    echo "╔══════════════════════════════════════════════════╗"
                    echo "║   Dashboard is LIVE!                            ║"
                    echo "╚══════════════════════════════════════════════════╝"
                    echo ""
                    echo "   Local:  http://localhost:8000"
                    echo "   Public: $URL"
                    echo ""
                    echo "   Share the public URL with your team."
                    echo "   The dashboard runs automatically when your Mac is on."
                    echo ""
                    echo "   Useful commands:"
                    echo "     bash services/install.sh url       Show current URL"
                    echo "     bash services/install.sh status    Check if running"
                    echo "     bash services/install.sh logs      View recent logs"
                    echo "     bash services/install.sh stop      Stop services"
                    echo "     bash services/install.sh uninstall Remove completely"
                    echo ""
                    exit 0
                fi
            fi
            sleep 1
        done

        echo ""
        echo "✅ Services installed and starting."
        echo "   Server: http://localhost:8000"
        echo "   Tunnel URL will appear shortly. Check with:"
        echo "     bash services/install.sh url"
        echo ""
        ;;

    stop)
        echo "Stopping GolfGen Dashboard services..."
        launchctl bootout "gui/$(id -u)/$SERVER_LABEL" 2>/dev/null
        launchctl bootout "gui/$(id -u)/$TUNNEL_LABEL" 2>/dev/null
        # Clean up port
        EXISTING_PID=$(lsof -ti :8000 2>/dev/null)
        [ -n "$EXISTING_PID" ] && kill $EXISTING_PID 2>/dev/null
        echo "✅ Services stopped."
        ;;

    start)
        echo "Starting GolfGen Dashboard services..."
        # Kill stale port
        EXISTING_PID=$(lsof -ti :8000 2>/dev/null)
        [ -n "$EXISTING_PID" ] && kill $EXISTING_PID 2>/dev/null && sleep 1

        launchctl bootstrap "gui/$(id -u)" "$SERVER_PLIST" 2>/dev/null
        sleep 2
        launchctl bootstrap "gui/$(id -u)" "$TUNNEL_PLIST" 2>/dev/null
        echo "✅ Services started. Check URL with: bash services/install.sh url"
        ;;

    status)
        echo "GolfGen Dashboard Service Status:"
        echo ""
        if launchctl print "gui/$(id -u)/$SERVER_LABEL" &>/dev/null; then
            echo "  Server:  ✅ Running"
        else
            echo "  Server:  ❌ Not running"
        fi
        if launchctl print "gui/$(id -u)/$TUNNEL_LABEL" &>/dev/null; then
            echo "  Tunnel:  ✅ Running"
        else
            echo "  Tunnel:  ❌ Not running"
        fi
        if curl -s "http://localhost:8000/api/health" > /dev/null 2>&1; then
            echo "  API:     ✅ Responding"
        else
            echo "  API:     ❌ Not responding"
        fi
        if [ -f "$LOG_DIR/tunnel-url.txt" ]; then
            URL=$(cat "$LOG_DIR/tunnel-url.txt")
            echo "  URL:     $URL"
        fi
        echo ""
        ;;

    url)
        if [ -f "$LOG_DIR/tunnel-url.txt" ]; then
            URL=$(cat "$LOG_DIR/tunnel-url.txt")
            if [[ "$URL" == https://* ]]; then
                echo "$URL"
            else
                echo "Tunnel is starting... try again in a few seconds."
            fi
        else
            echo "No tunnel URL found. Services may not be running."
            echo "Run: bash services/install.sh status"
        fi
        ;;

    uninstall)
        echo "Removing GolfGen Dashboard services..."
        launchctl bootout "gui/$(id -u)/$SERVER_LABEL" 2>/dev/null
        launchctl bootout "gui/$(id -u)/$TUNNEL_LABEL" 2>/dev/null
        rm -f "$SERVER_PLIST" "$TUNNEL_PLIST"
        EXISTING_PID=$(lsof -ti :8000 2>/dev/null)
        [ -n "$EXISTING_PID" ] && kill $EXISTING_PID 2>/dev/null
        echo "✅ Services removed. Dashboard is stopped."
        ;;

    logs)
        echo "=== Recent Server Logs ==="
        tail -20 "$LOG_DIR/server.log" 2>/dev/null || echo "(no logs yet)"
        echo ""
        echo "=== Recent Tunnel Logs ==="
        tail -20 "$LOG_DIR/tunnel.log" 2>/dev/null || echo "(no logs yet)"
        ;;

    *)
        echo "Usage: bash services/install.sh [install|stop|start|status|url|uninstall|logs]"
        ;;
esac
