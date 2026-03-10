#!/bin/bash
# GolfGen Dashboard — Background Tunnel
# Called by launchd, not meant to be run manually.
# Logs the public URL to services/logs/tunnel-url.txt

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT=8000
LOG_DIR="$SCRIPT_DIR/services/logs"
URL_FILE="$LOG_DIR/tunnel-url.txt"
mkdir -p "$LOG_DIR"

# Wait for the dashboard server to be ready (up to 30 seconds)
for i in {1..30}; do
    if curl -s "http://localhost:$PORT/api/health" > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Check if a named tunnel config exists (permanent URL)
if [ -f "$HOME/.cloudflared/config-golfgen.yml" ]; then
    echo "Starting named tunnel (permanent URL)..."
    exec cloudflared tunnel --config "$HOME/.cloudflared/config-golfgen.yml" run golfgen-dashboard
else
    # Quick tunnel — capture the URL from stderr
    echo "Starting quick tunnel (temporary URL)..."
    echo "Waiting for URL..." > "$URL_FILE"

    # cloudflared prints the URL to stderr, so we tee it to capture
    cloudflared tunnel --protocol http2 --url http://localhost:$PORT 2>&1 | while IFS= read -r line; do
        echo "$line"
        # Capture the trycloudflare URL
        if echo "$line" | grep -q "trycloudflare.com"; then
            URL=$(echo "$line" | grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com')
            if [ -n "$URL" ]; then
                echo "$URL" > "$URL_FILE"
                echo "$(date '+%Y-%m-%d %H:%M:%S') - $URL" >> "$LOG_DIR/tunnel-url-history.txt"
            fi
        fi
    done
fi
