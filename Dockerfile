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

# Copy pre-built frontend (no need for Node.js in prod)
COPY webapp/frontend/dist/ ./webapp/frontend/dist/

# Copy data files if they exist (DuckDB database + COGS)
# Use a shell command so it doesn't fail if the file is missing
RUN mkdir -p ./data
COPY data/golfgen_amazon.duckdb* ./data/
COPY data/cogs.csv ./data/

# Environment
ENV DB_DIR=/app/data
ENV PORT=8000

EXPOSE 8000

# Railway injects $PORT automatically
CMD ["sh", "-c", "uvicorn webapp.backend.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
