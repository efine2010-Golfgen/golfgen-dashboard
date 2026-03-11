# ══════════════════════════════════════════════════════
# GolfGen Dashboard — Single-container deployment
# FastAPI backend + React frontend + DuckDB data
# ══════════════════════════════════════════════════════
FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies
COPY webapp/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend (keep the same relative structure so path logic works)
COPY webapp/backend/main.py ./webapp/backend/main.py
RUN touch ./webapp/__init__.py ./webapp/backend/__init__.py

# Copy pre-built frontend from backend/dist (fully git-tracked copy)
COPY webapp/backend/dist/ ./webapp/frontend/dist/

# SP-API credentials are passed via environment variables on Railway
# (set in Railway dashboard, NOT baked into Docker image)

# Copy data files if they exist (DuckDB database + COGS + JSON inventory)
RUN mkdir -p ./data
COPY data/golfgen_amazon.duckdb* ./data/
COPY data/cogs.csv ./data/
COPY data/item_master.csv ./data/
COPY data/warehouse.csv ./data/
COPY data/golf_inventory.json ./data/
COPY data/housewares_inventory.json ./data/
COPY data/walmart_item_master.json ./data/
COPY data/amazon_item_master.json ./data/
COPY data/factory_po_summary.json ./data/
COPY data/logistics_tracking.json ./data/

# Environment
ENV DB_DIR=/app/data
ENV PORT=8000

EXPOSE 8000

# Railway injects $PORT automatically
CMD ["sh", "-c", "uvicorn webapp.backend.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
