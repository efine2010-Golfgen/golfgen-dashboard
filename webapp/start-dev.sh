#!/bin/bash
# GolfGen Dashboard — Local Development
# Starts both backend and frontend servers

echo "🏌️ GolfGen Dashboard — Starting..."

# Backend
echo "→ Starting API server on :8000..."
cd "$(dirname "$0")/backend"
pip install -r requirements.txt -q
python main.py &
BACKEND_PID=$!

# Frontend
echo "→ Starting frontend on :3000..."
cd "$(dirname "$0")/frontend"
npm install -q
npx vite --host 0.0.0.0 --port 3000 &
FRONTEND_PID=$!

echo ""
echo "✅ Dashboard running:"
echo "   Frontend: http://localhost:3000"
echo "   API:      http://localhost:8000/api/health"
echo ""
echo "Press Ctrl+C to stop"

trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null" EXIT
wait
