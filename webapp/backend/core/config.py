"""
Configuration and constants for GolfGen Dashboard.
All paths, environment variables, constants, and user/tab data defined here.
"""

import os
import logging
import secrets
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger("golfgen")

# ── Paths ───────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # GolfGen Amazon Dashboard/
DB_DIR = Path(os.environ.get("DB_DIR", str(BASE_DIR / "data")))
DB_PATH = DB_DIR / "golfgen_amazon.duckdb"
COGS_PATH = DB_DIR / "cogs.csv"
ITEM_MASTER_PATH = DB_DIR / "amazon_item_master.json"

# ── Database Engine Detection ───────────────────────────
# Set DATABASE_URL env var to a PostgreSQL connection string to use PostgreSQL.
# When unset, DuckDB is used (current default).
DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_POSTGRES = bool(DATABASE_URL)
CONFIG_PATH = BASE_DIR / "config" / "credentials.json"
DOCS_DIR = Path("/app/docs")
PRICING_CACHE_PATH = DB_DIR / "pricing_sync.json"

# Ensure docs directory exists
DOCS_DIR.mkdir(parents=True, exist_ok=True)

# ── Environment & Sync Configuration ────────────────────
SYNC_INTERVAL_HOURS = 2
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "changeme")  # Set via Railway env vars
TIMEZONE = ZoneInfo("America/Chicago")

# ── Amazon Marketplace Configuration ─────────────────────
MARKETPLACE_IDS = {
    "US": "ATVPDKIKX0DER",
    "CA": "A2EUQ1WTGCTBG2",
}
DEFAULT_MARKETPLACE = "US"
# Marketplaces to sync — add "CA" here once SP-API credentials support Canada
SYNC_MARKETPLACES = ["US"]  # Will become ["US", "CA"] when CA is enabled

# ── Multi-User Authentication ──────────────────────────────
# All dashboard tabs (tab_key -> display label)
ALL_TABS = {
    "dashboard": "Dashboard",
    "products": "Products",
    "profitability": "Profitability",
    "advertising": "Advertising",
    "inventory": "Amazon FBA",
    "golfgen-inventory": "GolfGen Inventory",
    "item-master": "Item Master",
    "factory-po": "Factory PO",
    "logistics": "OTW / Logistics",
    "supply-chain": "Supply Chain",
    "fba-shipments": "Shipments to FBA",
    "item-planning": "Item Planning",
}

# Tab key -> list of API path prefixes that belong to that tab
TAB_API_PREFIXES = {
    "dashboard": ["/api/summary", "/api/daily", "/api/comparison", "/api/monthly-yoy", "/api/product-mix", "/api/color-mix"],
    "products": ["/api/products", "/api/product/"],
    "profitability": ["/api/pnl", "/api/profitability"],
    "advertising": ["/api/ads/"],
    "inventory": ["/api/inventory"],
    "golfgen-inventory": ["/api/warehouse"],
    "item-master": ["/api/item-master", "/api/pricing"],
    "factory-po": ["/api/factory-po"],
    "logistics": ["/api/logistics"],
    "supply-chain": ["/api/supply-chain"],
    "fba-shipments": ["/api/fba-shipments"],
    "item-planning": ["/api/item-plan", "/api/item-planning", "/api/factory-on-order", "/api/dashboard-settings"],
}

USERS = {
    "eric": {
        "name": "Eric",
        "emails": ["eric@golfgen.com", "eric@egbrands.com", "efine2010@gmail.com"],
        "password_hash": "$2b$12$CXwF3gjnEyEPwej2qV9trem4AXZi4tUVR50ifvb2dUTiNVBhHAneu",
        "role": "admin",
    },
    "ty": {
        "name": "Ty",
        "emails": ["ty@golfgen.com", "tysams@egbrands.com"],
        "password_hash": "$2b$12$jurM2OMgL16XIFjNBQu3JeZsq.phyEea08ABqvNMIxZt3ZzgFBjs6",
        "role": "staff",
    },
    "kim": {
        "name": "Kim",
        "emails": ["kim@golfgen.com", "kim@egbrands.com"],
        "password_hash": "$2b$12$84EhMgFJ072dxgZ3ChICj.K.vwVRix2tDPGcb7uqMa3haswq8zdSK",
        "role": "staff",
    },
    "ryan": {
        "name": "Ryan",
        "emails": ["ryan@golfgen.com", "ryan@egbrands.com"],
        "password_hash": "$2b$12$oP6sxNocG4Hzrek3R/SU5eWUFR/EM3bqjZFf58RhgZHC35zuKLAEC",
        "role": "staff",
    },
    "mckay": {
        "name": "McKay",
        "emails": ["riseecom21@gmail.com"],
        "password_hash": "$2b$12$XvwYb1CZMG78FpD5AXy3FOT8WgiJhuBQyUg/tbgBmMrokA6z.LJya",
        "role": "staff",
    },
}

# Build email -> user_key lookup (case-insensitive)
EMAIL_TO_USER = {}
for _ukey, _udata in USERS.items():
    for _em in _udata["emails"]:
        EMAIL_TO_USER[_em.lower()] = _ukey

# ── Anthropic API (Ask Claude feature) ────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Google SSO Configuration ──────────────────────────────
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID") or os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET") or os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = (
    os.environ.get("GOOGLE_REDIRECT_URI")
    or "https://golfgen-dashboard-production.up.railway.app/api/auth/google/callback"
)
SESSION_SECRET = os.environ.get("SESSION_SECRET", secrets.token_hex(32))

# Whitelisted emails allowed to login via Google SSO (case-insensitive)
ALLOWED_SSO_EMAILS = {em.lower() for em in [
    "eric@golfgen.com",
    "eric@egbrands.com",
    "efine2010@gmail.com",
    "ty@golfgen.com",
    "tysams@egbrands.com",
    "kim@golfgen.com",
    "kim@egbrands.com",
    "ryan@egbrands.com",
    "riseecom21@gmail.com",
]}

# Session timeouts
SESSION_MAX_AGE_HOURS = 18       # Absolute session lifetime
SESSION_IDLE_TIMEOUT_HOURS = 2   # Expire after inactivity

# ── Financial Year Configuration ──────────────────────────
# Month through which we have complete historical data (for syncing and backfilling)
ACTUALIZED_THROUGH_MONTH = 12  # December
ACTUALIZED_THROUGH_YEAR = 2024

# ── Frontend Distribution ──────────────────────────────────
def get_frontend_dir() -> Path:
    """Return the frontend dist directory (Docker or local dev)."""
    backend_dir = Path(__file__).resolve().parent.parent
    local_dist = backend_dir / "dist"
    dev_dist = backend_dir.parent / "frontend" / "dist"
    return local_dist if local_dist.exists() else dev_dist
