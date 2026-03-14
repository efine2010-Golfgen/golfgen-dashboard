# ══════════════════════════════════════════════════════
# GolfGen Dashboard — Single-container deployment
# FastAPI backend + React frontend + DuckDB data
# ══════════════════════════════════════════════════════
FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY webapp/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend (modular structure)
COPY webapp/backend/main.py ./webapp/backend/main.py
COPY webapp/backend/core/ ./webapp/backend/core/
COPY webapp/backend/routers/ ./webapp/backend/routers/
COPY webapp/backend/services/ ./webapp/backend/services/
RUN touch ./webapp/__init__.py ./webapp/backend/__init__.py

# Copy pre-built frontend from backend/dist (fully git-tracked copy)
COPY webapp/backend/dist/ ./webapp/frontend/dist/

# SP-API credentials are passed via environment variables on Railway
# (set in Railway dashboard, NOT baked into Docker image)

# Copy data files to a SEED directory (not /app/data directly).
# When a Railway persistent volume is mounted at /app/data, it overlays
# the Docker layer — so we stage seeds separately and copy at startup
# if the volume is empty.
RUN mkdir -p ./data-seed ./data
COPY data/golfgen_amazon.duckdb* ./data-seed/
COPY data/cogs.csv ./data-seed/
COPY data/item_master.csv ./data-seed/
COPY data/warehouse.csv ./data-seed/
COPY data/golf_inventory.json ./data-seed/
COPY data/housewares_inventory.json ./data-seed/
COPY data/walmart_item_master.json ./data-seed/
COPY data/amazon_item_master.json ./data-seed/
COPY data/factory_po_summary.json ./data-seed/
COPY data/logistics_tracking.json ./data-seed/
COPY data/item_planning.json ./data-seed/
COPY data/raw_product_sales.json ./data-seed/
COPY data/raw_daily_data.json ./data-seed/
COPY data/item_plan_seed.json ./data-seed/
COPY data/factory_orders_seed.json ./data-seed/

# Startup script: seed empty volume then launch uvicorn
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Environment
ENV DB_DIR=/app/data
ENV PORT=8000

EXPOSE 8000

# Railway injects $PORT automatically
CMD ["/app/docker-entrypoint.sh"]
