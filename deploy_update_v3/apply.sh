#!/bin/bash
# GolfGen Deploy Fix v3 — Fix Railway build failure (COPY config/)
# Run from golfgen-dashboard root: bash deploy_update_v3/apply.sh
set -e

echo "=== GolfGen Deploy Fix v3 ==="

# 1. Update Dockerfile (remove COPY config/ line)
cp deploy_update_v3/Dockerfile ./Dockerfile
echo "✓ Dockerfile updated (removed COPY config/)"

# 2. Update main.py (env var credentials)
cp deploy_update_v3/main.py ./webapp/backend/main.py
echo "✓ main.py updated (reads SP-API creds from env vars)"

# 3. Update requirements.txt (fix SP-API package version)
cp deploy_update_v3/requirements.txt ./webapp/backend/requirements.txt
echo "✓ requirements.txt updated (fixed python-amazon-sp-api version)"

echo ""
echo "=== Done! Now run: ==="
echo "  git add Dockerfile webapp/backend/main.py"
echo "  git commit -m 'fix: read SP-API creds from env vars, remove COPY config/'"
echo "  git push origin main"
echo ""
echo "IMPORTANT: After push, set these env vars in Railway dashboard:"
echo "  SP_API_REFRESH_TOKEN"
echo "  SP_API_LWA_APP_ID"
echo "  SP_API_LWA_CLIENT_SECRET"
echo "  SP_API_AWS_ACCESS_KEY"
echo "  SP_API_AWS_SECRET_KEY"
echo "  SP_API_ROLE_ARN"
