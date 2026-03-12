#!/bin/bash
# ──────────────────────────────────────────────────────
# GolfGen Dashboard — Docker Entrypoint
# Seeds the persistent volume on first boot, then starts uvicorn.
# ──────────────────────────────────────────────────────
set -e

DATA_DIR="/app/data"
SEED_DIR="/app/data-seed"

# If the data directory is empty (fresh volume), copy seed files
if [ -z "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
    echo "INFO  [entrypoint] Empty data volume detected — seeding from $SEED_DIR ..."
    cp -v "$SEED_DIR"/* "$DATA_DIR"/ 2>/dev/null || true
    echo "INFO  [entrypoint] Seed complete."
else
    echo "INFO  [entrypoint] Data volume already populated ($(ls "$DATA_DIR" | wc -l) files)."
    # Copy any NEW seed files that don't exist on the volume yet
    # (e.g. a new JSON file added after the volume was first seeded)
    for f in "$SEED_DIR"/*; do
        fname=$(basename "$f")
        if [ ! -f "$DATA_DIR/$fname" ]; then
            echo "INFO  [entrypoint] New seed file: $fname — copying to volume."
            cp -v "$f" "$DATA_DIR/$fname"
        fi
    done
fi

echo "INFO  [entrypoint] Starting uvicorn on port ${PORT:-8000} ..."
cd /app/webapp/backend
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
