#!/bin/bash
# GolfGen Dashboard — Background Server
# Called by launchd, not meant to be run manually.

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT=8000
LOG_DIR="$SCRIPT_DIR/services/logs"
mkdir -p "$LOG_DIR"

cd "$SCRIPT_DIR"

# Kill any existing process on this port
EXISTING_PID=$(lsof -ti :$PORT 2>/dev/null)
if [ -n "$EXISTING_PID" ]; then
    kill $EXISTING_PID 2>/dev/null
    sleep 2
    kill -9 $EXISTING_PID 2>/dev/null 2>&1
    sleep 1
fi

# Ensure Python deps are installed
if ! python3 -c "import fastapi, uvicorn, duckdb" 2>/dev/null; then
    pip3 install fastapi uvicorn[standard] duckdb python-multipart --break-system-packages -q
fi

# Start the server
cd webapp/backend
exec python3 -c "
import uvicorn
uvicorn.run('main:app', host='0.0.0.0', port=$PORT, log_level='info')
"
