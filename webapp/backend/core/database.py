"""
Database initialization and connection management.
All CREATE SEQUENCE and CREATE TABLE statements are here.

Supports both DuckDB (default) and PostgreSQL (when DATABASE_URL is set).
"""

import json
import logging
import re
import duckdb
from pathlib import Path

from .config import DB_PATH, DB_DIR, USERS, ALL_TABS, DATABASE_URL, USE_POSTGRES

logger = logging.getLogger("golfgen")


# ── Dual-mode database wrapper ──────────────────────────────────────────────
# Provides a consistent interface so routers don't need to know which engine
# is active.  DuckDB uses ? placeholders; PostgreSQL uses %s.
# DuckDB's execute() returns a result object; psycopg2's returns None
# (results live on the cursor).  The wrapper normalises both behaviours.

_TABLE_PKS: dict[str, list[str]] = {
    "orders": ["order_id"],
    "daily_sales": ["date", "asin"],
    "fba_inventory": ["date", "asin", "condition"],
    "financial_events": [],  # no PK
    "advertising": ["date", "campaign_id"],
    "ads_campaigns": ["date", "campaign_id"],
    "item_master": ["asin"],
    "sessions": ["token"],
    "user_permissions": ["user_name", "tab_key"],
    "item_plan_overrides": ["sku", "field", "month"],
    "item_plan_curve_selection": ["sku"],
    "item_plan_factory_orders": ["po_number"],
    "item_plan_factory_order_items": ["id"],
    "item_plan_settings": ["key"],
    "monthly_sales_history": ["sku", "year", "month"],
    "sync_log": ["id"],
    "docs_update_log": ["id"],
    "fba_inventory_aging": ["snapshot_date", "asin"],
    "fba_stranded_inventory": ["snapshot_date", "asin"],
    "fba_reimbursements": ["reimbursement_id"],
    "fba_fc_inventory": ["snapshot_date", "fulfillment_center", "asin"],
}

# ── Dev-safety guard: tables that must never be DROPped or TRUNCATEd ────────
# Any DbConnection.execute() call containing DROP TABLE or TRUNCATE TABLE on
# one of these tables will raise immediately, preventing accidental data loss
# during development sessions.  The backup restore bypasses this guard by using
# the raw psycopg2 cursor directly (con._conn.cursor()) — that's intentional.
_PROTECTED_TABLES = {
    "orders", "daily_sales", "financial_events", "fba_inventory",
    "advertising", "ads_campaigns", "ads_keywords", "ads_search_terms",
    "ads_negative_keywords", "item_master", "monthly_sales_history",
    "staging_orders", "staging_financial_events",
    "analytics_daily", "analytics_sku", "analytics_ads",
    "item_plan_overrides", "item_plan_curve_selection",
    "item_plan_factory_orders", "item_plan_factory_order_items",
    "item_plan_settings",
    "fba_inventory_aging", "fba_stranded_inventory", "fba_reimbursements", "fba_fc_inventory",
}

_RE_DESTRUCTIVE_SQL = re.compile(
    r"\b(DROP\s+TABLE|TRUNCATE(?:\s+TABLE)?)\s+(?:IF\s+EXISTS\s+)?(['\"`]?(\w+)['\"`]?)",
    re.IGNORECASE,
)


def _check_sql_safety(sql: str) -> None:
    """Raise RuntimeError if SQL would DROP or TRUNCATE a protected data table."""
    m = _RE_DESTRUCTIVE_SQL.search(sql)
    if m:
        table = m.group(3).lower()
        if table in _PROTECTED_TABLES:
            raise RuntimeError(
                f"SAFETY GUARD: Refusing to execute '{m.group(1).upper()}' on "
                f"protected table '{table}'. If this is intentional (e.g. a "
                f"migration), temporarily remove the table from _PROTECTED_TABLES "
                f"in core/database.py."
            )


_RE_INSERT_OR_IGNORE = re.compile(
    r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", re.IGNORECASE
)
_RE_INSERT_OR_REPLACE = re.compile(
    r"\bINSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)",
    re.IGNORECASE | re.DOTALL,
)


def _translate_sql_for_pg(sql: str) -> str:
    """Convert DuckDB SQL idioms to PostgreSQL equivalents."""
    # Escape existing % characters first (e.g. ILIKE '%refund%')
    # so psycopg2 doesn't interpret them as format specifiers.
    sql = sql.replace("%", "%%")
    # ? → %s  (parameterised placeholders)
    sql = sql.replace("?", "%s")

    # DOUBLE → DOUBLE PRECISION (DuckDB type not valid in PostgreSQL)
    sql = re.sub(r'\bDOUBLE\b(?!\s+PRECISION)', 'DOUBLE PRECISION', sql)

    # CURRENT_DATE - N  →  CAST((CURRENT_DATE - INTERVAL 'N days') AS VARCHAR)
    # DuckDB allows `CURRENT_DATE - 90` but PostgreSQL does not.
    # We CAST to VARCHAR because many date columns (daily_sales.date) are VARCHAR,
    # and PostgreSQL cannot compare VARCHAR >= DATE without an explicit cast.
    sql = re.sub(
        r'\bCURRENT_DATE\s*-\s*(\d+)\b',
        r"CAST((CURRENT_DATE - INTERVAL '\1 days') AS VARCHAR)",
        sql,
    )

    # Also handle pre-written CURRENT_DATE - INTERVAL '...' that are NOT already
    # wrapped in CAST.  These appear in inventory.py and other routers that were
    # written with the INTERVAL syntax directly.  The result is a DATE which
    # cannot be compared to VARCHAR date columns without a CAST.
    # The lookbehind (?<!CAST\(\() prevents double-wrapping when the first regex
    # above already produced CAST((CURRENT_DATE - INTERVAL '...) AS VARCHAR).
    sql = re.sub(
        r"(?<!CAST\(\()CURRENT_DATE\s*-\s*INTERVAL\s*'(\d+)\s+days?'",
        r"CAST((CURRENT_DATE - INTERVAL '\1 days') AS VARCHAR)",
        sql,
    )

    # ── INSERT OR IGNORE → ON CONFLICT DO NOTHING ──────────
    if _RE_INSERT_OR_IGNORE.search(sql):
        sql = _RE_INSERT_OR_IGNORE.sub("INSERT INTO", sql)
        # Append ON CONFLICT DO NOTHING after the VALUES(...)
        sql = sql.rstrip().rstrip(";")
        sql += " ON CONFLICT DO NOTHING"

    # ── INSERT OR REPLACE → ON CONFLICT (pk) DO UPDATE SET … ──
    m = _RE_INSERT_OR_REPLACE.search(sql)
    if m:
        table = m.group(1).lower()
        cols = [c.strip().strip('"') for c in m.group(2).split(",")]
        pk_cols = _TABLE_PKS.get(table, [])
        # Replace the INSERT OR REPLACE preamble
        sql = _RE_INSERT_OR_REPLACE.sub(
            f"INSERT INTO {table} ({m.group(2)})", sql, count=1
        )
        sql = sql.rstrip().rstrip(";")
        if pk_cols:
            non_pk = [c for c in cols if c.lower() not in
                      {p.lower() for p in pk_cols}]
            conflict_target = ", ".join(pk_cols)
            if non_pk:
                set_clause = ", ".join(
                    f"{c} = EXCLUDED.{c}" for c in non_pk
                )
                sql += (f" ON CONFLICT ({conflict_target})"
                        f" DO UPDATE SET {set_clause}")
            else:
                sql += (f" ON CONFLICT ({conflict_target})"
                        f" DO NOTHING")
        else:
            # No known PK — fall back to DO NOTHING
            sql += " ON CONFLICT DO NOTHING"

    return sql


class _PgResult:
    """Wraps a psycopg2 cursor to match DuckDB's execute() return interface."""

    def __init__(self, cursor):
        self._cursor = cursor

    def fetchall(self):
        try:
            return self._cursor.fetchall()
        except Exception:
            return []

    def fetchone(self):
        try:
            return self._cursor.fetchone()
        except Exception:
            return None


class DbConnection:
    """Unified connection wrapper for DuckDB and PostgreSQL.

    Usage is identical to a raw DuckDB connection:
        con = get_db()
        rows = con.execute("SELECT * FROM t WHERE id = ?", [42]).fetchall()
        con.close()
    """

    def __init__(self, conn, is_postgres: bool = False):
        self._conn = conn
        self._is_postgres = is_postgres
        self._cursor = conn.cursor() if is_postgres else None

    # -- query interface ------------------------------------------------
    def execute(self, sql: str, params=None):
        _check_sql_safety(sql)  # raises on DROP/TRUNCATE of protected tables
        if self._is_postgres:
            stripped = sql.strip().upper()
            # Handle explicit transaction control for PostgreSQL.
            # psycopg2 with autocommit=True needs autocommit toggled off
            # for explicit transaction blocks.
            if stripped.startswith("BEGIN"):
                self._conn.autocommit = False
                return _PgResult(self._cursor)
            if stripped.startswith("COMMIT"):
                self._conn.commit()
                self._conn.autocommit = True
                return _PgResult(self._cursor)
            if stripped.startswith("ROLLBACK"):
                self._conn.rollback()
                self._conn.autocommit = True
                return _PgResult(self._cursor)

            sql = _translate_sql_for_pg(sql)
            if params:
                self._cursor.execute(sql, params)
            else:
                self._cursor.execute(sql)
            return _PgResult(self._cursor)
        else:
            if params:
                return self._conn.execute(sql, params)
            return self._conn.execute(sql)

    def executemany(self, sql: str, params_list):
        if self._is_postgres:
            sql = _translate_sql_for_pg(sql)
            self._cursor.executemany(sql, params_list)
            return _PgResult(self._cursor)
        else:
            return self._conn.executemany(sql, params_list)

    def fetchall(self):
        if self._is_postgres:
            return self._cursor.fetchall()
        return self._conn.fetchall()

    def fetchone(self):
        if self._is_postgres:
            return self._cursor.fetchone()
        return self._conn.fetchone()

    def commit(self):
        self._conn.commit()

    def close(self):
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None
        try:
            self._conn.close()
        except Exception:
            pass

    # Allow use as context manager
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def _raw_pg_conn():
    """Return a raw psycopg2 connection (import deferred so DuckDB-only
    deployments don't need psycopg2 installed)."""
    import psycopg2  # noqa: deferred import
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def get_db() -> DbConnection:
    """Return a database connection (DuckDB or PostgreSQL)."""
    if USE_POSTGRES:
        return DbConnection(_raw_pg_conn(), is_postgres=True)
    return DbConnection(duckdb.connect(str(DB_PATH), read_only=False))


def get_db_rw() -> DbConnection:
    """Return a read-write database connection (DuckDB or PostgreSQL)."""
    if USE_POSTGRES:
        return DbConnection(_raw_pg_conn(), is_postgres=True)
    return DbConnection(duckdb.connect(str(DB_PATH), read_only=False))


# ── Portable schema helpers (work with both DuckDB and PostgreSQL) ────────

def list_tables(con) -> list[str]:
    """List all user tables, compatible with both DuckDB and PostgreSQL."""
    if USE_POSTGRES:
        rows = con.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ).fetchall()
    else:
        rows = con.execute("SHOW TABLES").fetchall()
    return [r[0] for r in rows]


def get_columns(con, table: str) -> list[str]:
    """Get column names for a table, compatible with both DuckDB and PostgreSQL."""
    if USE_POSTGRES:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table],
        ).fetchall()
    else:
        rows = con.execute(f"DESCRIBE {table}").fetchall()
    return [r[0] for r in rows]


def _init_auth_tables():
    """Create tables for sessions and permissions if not exist."""
    con = get_db_rw()
    con.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token       TEXT PRIMARY KEY,
            user_email  TEXT NOT NULL,
            user_name   TEXT NOT NULL,
            role        TEXT NOT NULL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS user_permissions (
            user_name   TEXT NOT NULL,
            tab_key     TEXT NOT NULL,
            enabled     BOOLEAN DEFAULT TRUE,
            PRIMARY KEY (user_name, tab_key)
        )
    """)
    # Add session timeout columns if missing
    try:
        con.execute("ALTER TABLE sessions ADD COLUMN last_active_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    except Exception:
        pass  # Column already exists
    try:
        con.execute("ALTER TABLE sessions ADD COLUMN login_method TEXT DEFAULT 'password'")
    except Exception:
        pass

    # ── Audit log table ──────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id          INTEGER,
            ts          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_email  TEXT,
            user_name   TEXT,
            action      TEXT NOT NULL,
            detail      TEXT,
            ip_address  TEXT,
            path        TEXT
        )
    """)

    # ── Passkeys (WebAuthn) table ──────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS passkeys (
            id              TEXT PRIMARY KEY,
            user_name       TEXT NOT NULL,
            device_name     TEXT DEFAULT 'My Passkey',
            credential_id   TEXT NOT NULL UNIQUE,
            public_key      TEXT NOT NULL,
            sign_count      INTEGER DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_used_at    TIMESTAMP
        )
    """)

    # Seed default permissions for staff users (all enabled)
    for ukey, udata in USERS.items():
        if udata["role"] == "staff":
            for tab_key in ALL_TABS:
                con.execute("""
                    INSERT OR IGNORE INTO user_permissions (user_name, tab_key, enabled)
                    VALUES (?, ?, TRUE)
                """, [ukey, tab_key])
    con.close()


def _init_system_tables():
    """Create tables for sync logging and docs updates if not exist."""
    con = get_db_rw()

    # Create sequences FIRST — tables reference them in DEFAULT expressions
    try:
        con.execute("CREATE SEQUENCE IF NOT EXISTS sync_log_seq")
    except Exception:
        try:
            con.execute("CREATE SEQUENCE sync_log_seq")
        except Exception:
            pass  # Already exists

    try:
        con.execute("CREATE SEQUENCE IF NOT EXISTS docs_update_log_seq")
    except Exception:
        try:
            con.execute("CREATE SEQUENCE docs_update_log_seq")
        except Exception:
            pass  # Already exists

    con.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id BIGINT PRIMARY KEY DEFAULT nextval('sync_log_seq'),
            job_name TEXT NOT NULL,
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            status TEXT DEFAULT 'in_progress',
            records_processed BIGINT DEFAULT 0,
            error_message TEXT,
            execution_time_seconds DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS docs_update_log (
            id BIGINT PRIMARY KEY DEFAULT nextval('docs_update_log_seq'),
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            status TEXT DEFAULT 'in_progress',
            documents_updated TEXT,
            error_message TEXT,
            execution_time_seconds DOUBLE
        )
    """)

    con.close()


def _init_sales_tables():
    """Create tables for daily sales and FBA inventory if not exist."""
    con = get_db_rw()

    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR PRIMARY KEY,
            purchase_date VARCHAR,
            order_status VARCHAR,
            fulfillment_channel VARCHAR,
            sales_channel VARCHAR,
            order_total DOUBLE,
            currency_code VARCHAR,
            number_of_items INTEGER,
            ship_city VARCHAR,
            ship_state VARCHAR,
            ship_postal_code VARCHAR,
            is_business_order BOOLEAN,
            is_prime BOOLEAN,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_sales (
            date VARCHAR,
            asin VARCHAR,
            units_ordered BIGINT DEFAULT 0,
            ordered_product_sales DOUBLE DEFAULT 0,
            sessions BIGINT DEFAULT 0,
            session_percentage DOUBLE DEFAULT 0,
            page_views BIGINT DEFAULT 0,
            buy_box_percentage DOUBLE DEFAULT 0,
            unit_session_percentage DOUBLE DEFAULT 0,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL,
            PRIMARY KEY (date, asin)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fba_inventory (
            date DATE,
            asin VARCHAR,
            sku VARCHAR,
            product_name VARCHAR,
            condition VARCHAR DEFAULT 'NewItem',
            fulfillable_quantity INTEGER DEFAULT 0,
            inbound_working_quantity INTEGER DEFAULT 0,
            inbound_shipped_quantity INTEGER DEFAULT 0,
            inbound_receiving_quantity INTEGER DEFAULT 0,
            reserved_quantity INTEGER DEFAULT 0,
            unfulfillable_quantity INTEGER DEFAULT 0,
            total_quantity INTEGER DEFAULT 0,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL,
            PRIMARY KEY (date, asin, condition)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS financial_events (
            date DATE,
            asin VARCHAR,
            sku VARCHAR,
            order_id VARCHAR,
            event_type VARCHAR,
            product_charges DOUBLE DEFAULT 0,
            shipping_charges DOUBLE DEFAULT 0,
            fba_fees DOUBLE DEFAULT 0,
            commission DOUBLE DEFAULT 0,
            promotion_amount DOUBLE DEFAULT 0,
            other_fees DOUBLE DEFAULT 0,
            net_proceeds DOUBLE DEFAULT 0,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL
        )
    """)

    # ── Migration: rename transaction_type → event_type if old column exists ──
    # Production table may have been created with old column name before code rename.
    # CREATE TABLE IF NOT EXISTS does NOT alter existing schemas, so old column persists.
    try:
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='financial_events'"
        ).fetchall()]
        if "transaction_type" in cols and "event_type" not in cols:
            con.execute("ALTER TABLE financial_events RENAME COLUMN transaction_type TO event_type")
            logger.info("Migrated financial_events: transaction_type → event_type")
        elif "transaction_type" in cols and "event_type" in cols:
            # Both exist (shouldn't happen) — copy data and drop old
            con.execute("UPDATE financial_events SET event_type = transaction_type WHERE event_type IS NULL OR event_type = ''")
            logger.info("Copied transaction_type data into event_type")
        elif "event_type" not in cols:
            # Neither exists — add it
            con.execute("ALTER TABLE financial_events ADD COLUMN event_type VARCHAR")
            logger.info("Added missing event_type column to financial_events")
    except Exception as e:
        logger.warning(f"financial_events event_type migration check: {e}")

    # Ensure existing tables have hierarchy columns (safe ALTER for upgrades)
    for tbl in ["orders", "daily_sales", "fba_inventory", "financial_events"]:
        for col in ["division", "customer", "platform"]:
            try:
                con.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} TEXT DEFAULT NULL")
            except Exception:
                pass  # column already exists

    # Add marketplace column to all transactional and analytics tables (safe ALTER for upgrades)
    marketplace_tables = [
        "orders", "daily_sales", "fba_inventory", "financial_events",
        "advertising", "ads_campaigns", "ads_keywords", "ads_search_terms",
        "ads_negative_keywords", "staging_orders", "staging_financial_events",
        "analytics_daily", "analytics_sku", "analytics_ads"
    ]
    for tbl in marketplace_tables:
        try:
            con.execute(f"ALTER TABLE {tbl} ADD COLUMN marketplace VARCHAR DEFAULT 'US'")
        except Exception:
            pass  # column already exists

    # Also add product_name column to daily_sales if missing (used by reports)
    try:
        con.execute("ALTER TABLE daily_sales ADD COLUMN product_name TEXT DEFAULT NULL")
    except Exception:
        pass

    con.close()


def _init_advertising_tables():
    """Create tables for Amazon Ads data if not exist."""
    con = get_db_rw()

    con.execute("""
        CREATE TABLE IF NOT EXISTS advertising (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            impressions BIGINT DEFAULT 0,
            clicks BIGINT DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders BIGINT DEFAULT 0,
            units BIGINT DEFAULT 0,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL,
            PRIMARY KEY (date, campaign_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_campaigns (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            campaign_type VARCHAR DEFAULT 'SP',
            campaign_status VARCHAR,
            daily_budget DOUBLE DEFAULT 0,
            impressions BIGINT DEFAULT 0,
            clicks BIGINT DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders BIGINT DEFAULT 0,
            units BIGINT DEFAULT 0,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL,
            PRIMARY KEY (date, campaign_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_keywords (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_id VARCHAR,
            ad_group_name VARCHAR,
            keyword_id VARCHAR,
            keyword_text VARCHAR,
            match_type VARCHAR,
            impressions BIGINT DEFAULT 0,
            clicks BIGINT DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders BIGINT DEFAULT 0,
            units BIGINT DEFAULT 0,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_search_terms (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_name VARCHAR,
            keyword_text VARCHAR,
            match_type VARCHAR,
            search_term VARCHAR,
            impressions BIGINT DEFAULT 0,
            clicks BIGINT DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders BIGINT DEFAULT 0,
            units BIGINT DEFAULT 0,
            division TEXT DEFAULT NULL,
            customer TEXT DEFAULT NULL,
            platform TEXT DEFAULT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_negative_keywords (
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_id VARCHAR,
            negative_keyword TEXT,
            match_type VARCHAR,
            PRIMARY KEY (campaign_id, ad_group_id, negative_keyword)
        )
    """)

    con.close()


def _init_item_plan_tables():
    """Initialize Item Plan DuckDB tables and seed from JSON if they don't exist."""
    con = get_db_rw()
    try:
        # Create tables if they don't exist
        con.execute("""
            CREATE TABLE IF NOT EXISTS monthly_sales_history (
                sku TEXT,
                year INTEGER,
                month INTEGER,
                units DOUBLE,
                revenue DOUBLE,
                profit DOUBLE,
                refund_units DOUBLE,
                division TEXT DEFAULT NULL,
                customer TEXT DEFAULT NULL,
                platform TEXT DEFAULT NULL,
                PRIMARY KEY (sku, year, month)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS item_plan_overrides (
                sku TEXT,
                field TEXT,
                month INTEGER,
                value DOUBLE,
                PRIMARY KEY (sku, field, month)
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS item_plan_curve_selection (
                sku TEXT PRIMARY KEY,
                curve_type TEXT DEFAULT 'LY'
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS item_plan_factory_orders (
                po_number TEXT PRIMARY KEY,
                factory TEXT,
                payment_terms TEXT,
                total_units INTEGER,
                factory_cost DOUBLE,
                fob_date TEXT,
                est_arrival TEXT,
                wk_received TEXT,
                wk_available TEXT,
                cbm DOUBLE,
                status TEXT DEFAULT 'PENDING',
                division TEXT DEFAULT NULL,
                customer TEXT DEFAULT NULL,
                platform TEXT DEFAULT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS item_plan_factory_order_items (
                id INTEGER PRIMARY KEY,
                po_number TEXT,
                sku TEXT,
                description TEXT,
                units INTEGER,
                est_arrival TEXT,
                wk_received TEXT,
                wk_available TEXT,
                status TEXT DEFAULT 'PENDING',
                division TEXT DEFAULT NULL,
                customer TEXT DEFAULT NULL,
                platform TEXT DEFAULT NULL
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS item_plan_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # Check if we need to seed
        existing = con.execute("SELECT COUNT(*) FROM monthly_sales_history").fetchall()
        if existing[0][0] == 0:
            # Try to load seed data
            seed_path = DB_DIR / "item_plan_seed.json"
            factory_seed_path = DB_DIR / "factory_orders_seed.json"

            if seed_path.exists():
                with open(seed_path) as f:
                    seed_data = json.load(f)

                # Seed monthly_sales_history from ly_data (12 months Feb 2025 - Jan 2026)
                for sku, data in seed_data.get("ly_data", {}).items():
                    ly_units = data.get("ly_units", [])
                    ly_revenue = data.get("ly_revenue", [])
                    ly_profit = data.get("ly_profit", [])
                    ly_refund = data.get("ly_refund_units", [0] * 12)

                    # Feb 2025 through Jan 2026 = months 2-13 of 2025-2026
                    for idx in range(12):
                        month_num = idx + 2  # Feb=2, Mar=3, ..., Dec=12, Jan=13
                        if month_num > 12:
                            year = 2026
                            month = month_num - 12
                        else:
                            year = 2025
                            month = month_num

                        try:
                            con.execute("""
                                INSERT INTO monthly_sales_history
                                (sku, year, month, units, revenue, profit, refund_units,
                                 division, customer, platform)
                                VALUES (?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                            """, [
                                sku,
                                year,
                                month,
                                float(ly_units[idx]) if idx < len(ly_units) else 0,
                                float(ly_revenue[idx]) if idx < len(ly_revenue) else 0,
                                float(ly_profit[idx]) if idx < len(ly_profit) else 0,
                                float(ly_refund[idx]) if idx < len(ly_refund) else 0,
                            ])
                        except Exception:
                            pass

                # Seed curve_selection from skus data
                for sku_data in seed_data.get("skus", []):
                    sku = sku_data.get("sku", "")
                    curve = sku_data.get("curve", "LY")
                    if sku:
                        try:
                            con.execute("""
                                INSERT OR REPLACE INTO item_plan_curve_selection
                                (sku, curve_type) VALUES (?, ?)
                            """, [sku, curve])
                        except Exception:
                            pass

                # Seed overrides from skus data
                month_names = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "jan_next"]
                field_mappings = {
                    "ty_override_units": "plan_units",
                    "ty_aur_override": "aur",
                    "ty_override_refund_units": "refund_units",
                    "ty_refund_rate_override": "refund_rate",
                    "shipment_override": "shipment",
                }

                for sku_data in seed_data.get("skus", []):
                    sku = sku_data.get("sku", "")
                    if not sku:
                        continue

                    # Seed annual plan
                    annual = sku_data.get("ty_plan_annual", 0)
                    if annual:
                        try:
                            con.execute("""
                                INSERT OR REPLACE INTO item_plan_overrides
                                (sku, field, month, value) VALUES (?, ?, ?, ?)
                            """, [sku, "annual_plan_units", 0, float(annual)])
                        except Exception:
                            pass

                    # Seed monthly overrides
                    for old_field, new_field in field_mappings.items():
                        override_data = sku_data.get(old_field, {})
                        if isinstance(override_data, dict):
                            for month_name, value in override_data.items():
                                if value and value != 0:
                                    month_idx = month_names.index(month_name) if month_name in month_names else -1
                                    if month_idx >= 0:
                                        try:
                                            con.execute("""
                                                INSERT OR REPLACE INTO item_plan_overrides
                                                (sku, field, month, value) VALUES (?, ?, ?, ?)
                                            """, [sku, new_field, month_idx + 1, float(value)])
                                        except Exception:
                                            pass

            # Seed factory orders if they don't exist
            if factory_seed_path.exists():
                with open(factory_seed_path) as f:
                    factory_data = json.load(f)

                # PO-to-customer mapping for seed data
                _po_customer_map = {
                    "2100": "first_tee", "2200": "walmart_stores",
                    "2300": "walmart_stores", "2400": "walmart_stores",
                    "2500": "amazon", "2600": "amazon",
                    "2700": "walmart_stores", "2800": "walmart_stores",
                }

                for order in factory_data.get("orders", []):
                    po = order.get("po_number", "")
                    cust = _po_customer_map.get(po, "amazon")
                    try:
                        con.execute("""
                            INSERT INTO item_plan_factory_orders
                            (po_number, factory, payment_terms, total_units, factory_cost,
                             fob_date, est_arrival, wk_received, wk_available, cbm, status,
                             division, customer, platform)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', ?, 'manual_entry')
                        """, [
                            po,
                            order.get("factory", ""),
                            order.get("payment_terms", ""),
                            int(order.get("units", 0)),
                            float(order.get("factory_cost", 0)),
                            order.get("fob_date", ""),
                            order.get("est_arrival", ""),
                            order.get("wk_received", ""),
                            order.get("wk_available", ""),
                            float(order.get("cbm", 0)),
                            order.get("status", "PENDING"),
                            cust,
                        ])
                    except Exception:
                        pass

                # Walmart SKU prefixes for line-item customer determination
                _walmart_skus = {
                    'GGRTFTG2228', 'GGRTFTR2168', 'GGRTFTB2178', 'GGRTFTO2198',
                    'GGWMSS2114BM', 'GGWMSS2115BM', 'GGWMSS2451BM', 'GGWMSS2452BM',
                    'GGWMSS2116BM', 'GGWMSS2117BM', 'GGWMSS2453BM', 'GGWMSS2454BM',
                    'GGWMSS2118BM', 'GGWMSS2119BM',
                }
                _first_tee_skus = {'KIT 1', 'KIT 2'}

                for idx, item in enumerate(factory_data.get("items", []), start=1):
                    item_sku = item.get("sku", "")
                    item_po = item.get("po_number", "")
                    if item_sku in _first_tee_skus or item_po == "2100":
                        item_cust = "first_tee"
                    elif item_sku in _walmart_skus:
                        item_cust = "walmart_stores"
                    else:
                        item_cust = _po_customer_map.get(item_po, "amazon")
                    try:
                        con.execute("""
                            INSERT INTO item_plan_factory_order_items
                            (id, po_number, sku, description, units, est_arrival,
                             wk_received, wk_available, status,
                             division, customer, platform)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', ?, 'manual_entry')
                        """, [
                            idx,
                            item_po,
                            item_sku,
                            item.get("description", ""),
                            int(item.get("units", 0)),
                            item.get("est_arrival", ""),
                            item.get("wk_received", ""),
                            item.get("wk_available", ""),
                            item.get("status", "PENDING"),
                            item_cust,
                        ])
                    except Exception:
                        pass

            # Set default settings
            con.execute("""INSERT OR IGNORE INTO item_plan_settings (key, value)
                          VALUES ('actualized_through_month', '2')""")
            con.execute("""INSERT OR IGNORE INTO item_plan_settings (key, value)
                          VALUES ('fba_cover_weeks', '4')""")

        con.commit()
    finally:
        con.close()


def _init_item_master_table():
    """Create DuckDB item_master table if not exist (source of truth for division mapping).

    Auto-seeds from existing orders/order_items/daily_sales tables so that
    untagged ASINs appear immediately without a manual POST to /api/item-master/seed.
    """
    con = get_db_rw()
    con.execute("""
        CREATE TABLE IF NOT EXISTS item_master (
            asin          VARCHAR PRIMARY KEY,
            sku           VARCHAR,
            product_name  VARCHAR,
            division      VARCHAR,
            customer      VARCHAR,
            platform      VARCHAR DEFAULT 'sp_api',
            category      VARCHAR,
            brand         VARCHAR,
            unit_cost     DOUBLE PRECISION DEFAULT 0
        )
    """)

    # ── Migrate: add unit_cost column if missing (Phase 2b addition) ──
    try:
        con.execute("SELECT unit_cost FROM item_master LIMIT 1")
    except Exception:
        try:
            con.execute("ALTER TABLE item_master ADD COLUMN unit_cost DOUBLE PRECISION DEFAULT 0")
            logger.info("item_master: added unit_cost column")
        except Exception as e:
            logger.warning(f"item_master: could not add unit_cost column: {e}")

    # ── Populate unit_cost from cogs.csv if all zeros ──
    try:
        has_costs = con.execute("SELECT COUNT(*) FROM item_master WHERE unit_cost > 0").fetchone()[0]
        if has_costs == 0:
            import csv
            from core.config import DB_DIR
            cogs_path = DB_DIR / "cogs.csv"
            if cogs_path.exists():
                updated = 0
                with open(cogs_path, newline="", encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f):
                        asin = (row.get("asin") or "").strip()
                        try:
                            cost = float(row.get("cogs") or 0)
                        except (ValueError, TypeError):
                            cost = 0
                        if asin and cost > 0:
                            con.execute(
                                "UPDATE item_master SET unit_cost = ? WHERE asin = ?",
                                [cost, asin]
                            )
                            updated += 1
                if updated > 0:
                    logger.info(f"item_master: populated unit_cost for {updated} ASINs from cogs.csv")
    except Exception as e:
        logger.warning(f"item_master: unit_cost population error: {e}")

    # Auto-seed: if item_master is empty, populate from order data
    try:
        count = con.execute("SELECT COUNT(*) FROM item_master").fetchone()[0]
        if count == 0:
            logger.info("item_master is empty — auto-seeding from order data ...")
            inserted = 0

            # Seed from daily_sales (has product_name)
            try:
                rows = con.execute("""
                    SELECT DISTINCT asin, '' as sku, product_name
                    FROM daily_sales
                    WHERE asin IS NOT NULL AND asin != '' AND asin != 'ALL'
                """).fetchall()
                for r in rows:
                    try:
                        con.execute("""
                            INSERT INTO item_master (asin, sku, product_name, division, customer, platform)
                            VALUES (?, ?, ?, 'unknown', 'amazon', 'sp_api')
                        """, [r[0], r[1] or "", r[2] or ""])
                        inserted += 1
                    except Exception:
                        pass
            except Exception:
                pass

            # Seed from order_items (has sku)
            try:
                rows = con.execute("""
                    SELECT DISTINCT asin, seller_sku, title
                    FROM order_items
                    WHERE asin IS NOT NULL AND asin != ''
                """).fetchall()
                for r in rows:
                    asin = r[0]
                    try:
                        existing = con.execute(
                            "SELECT 1 FROM item_master WHERE asin = ?", [asin]
                        ).fetchone()
                        if not existing:
                            con.execute("""
                                INSERT INTO item_master (asin, sku, product_name, division, customer, platform)
                                VALUES (?, ?, ?, 'unknown', 'amazon', 'sp_api')
                            """, [asin, r[1] or "", r[2] or ""])
                            inserted += 1
                    except Exception:
                        pass
            except Exception:
                pass

            logger.info(f"item_master auto-seed complete: {inserted} ASINs inserted")
    except Exception as e:
        logger.warning(f"item_master auto-seed check failed (non-fatal): {e}")

    con.close()


def _init_staging_tables():
    """Create staging layer tables for cleaned/typed versions of raw data."""
    con = get_db_rw()

    con.execute("""
        CREATE TABLE IF NOT EXISTS staging_orders (
            staging_id        VARCHAR PRIMARY KEY,
            source_order_id   VARCHAR,
            division          VARCHAR NOT NULL,
            customer          VARCHAR NOT NULL,
            platform          VARCHAR NOT NULL,
            order_date        DATE,
            asin              VARCHAR,
            product_name      VARCHAR,
            units             INTEGER,
            gross_amount      DECIMAL(12,2),
            currency          VARCHAR DEFAULT 'USD',
            marketplace       VARCHAR,
            fulfillment_type  VARCHAR,
            status            VARCHAR,
            staged_at         TIMESTAMP,
            is_processed      BOOLEAN DEFAULT FALSE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS staging_financial_events (
            staging_id        VARCHAR PRIMARY KEY,
            source_event_id   VARCHAR,
            division          VARCHAR NOT NULL,
            customer          VARCHAR NOT NULL,
            platform          VARCHAR NOT NULL,
            event_date        DATE,
            event_type        VARCHAR,
            asin              VARCHAR,
            description       VARCHAR,
            amount            DECIMAL(12,2),
            currency          VARCHAR DEFAULT 'USD',
            fee_type          VARCHAR,
            staged_at         TIMESTAMP,
            is_processed      BOOLEAN DEFAULT FALSE
        )
    """)

    con.close()


def _init_analytics_tables():
    """Create analytics layer tables for pre-aggregated dashboard data."""
    con = get_db_rw()

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics_daily (
            id                VARCHAR PRIMARY KEY,
            date              DATE NOT NULL,
            division          VARCHAR NOT NULL,
            customer          VARCHAR NOT NULL,
            platform          VARCHAR NOT NULL,
            units_sold        INTEGER DEFAULT 0,
            gross_revenue     DECIMAL(12,2) DEFAULT 0,
            returns_units     INTEGER DEFAULT 0,
            returns_amount    DECIMAL(12,2) DEFAULT 0,
            net_revenue       DECIMAL(12,2) DEFAULT 0,
            amazon_fees       DECIMAL(12,2) DEFAULT 0,
            advertising_spend DECIMAL(12,2) DEFAULT 0,
            net_profit        DECIMAL(12,2) DEFAULT 0,
            updated_at        TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics_sku (
            id               VARCHAR PRIMARY KEY,
            asin             VARCHAR NOT NULL,
            product_name     VARCHAR,
            division         VARCHAR NOT NULL,
            customer         VARCHAR NOT NULL,
            period           VARCHAR NOT NULL,
            units_sold       INTEGER DEFAULT 0,
            revenue          DECIMAL(12,2) DEFAULT 0,
            return_rate      DECIMAL(5,2) DEFAULT 0,
            acos             DECIMAL(5,2) DEFAULT 0,
            rank_in_division INTEGER,
            updated_at       TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS analytics_ads (
            id               VARCHAR PRIMARY KEY,
            date             DATE NOT NULL,
            division         VARCHAR NOT NULL,
            customer         VARCHAR NOT NULL,
            campaign_name    VARCHAR,
            impressions      INTEGER DEFAULT 0,
            clicks           INTEGER DEFAULT 0,
            spend            DECIMAL(12,2) DEFAULT 0,
            sales            DECIMAL(12,2) DEFAULT 0,
            acos             DECIMAL(5,2) DEFAULT 0,
            roas             DECIMAL(5,2) DEFAULT 0,
            updated_at       TIMESTAMP
        )
    """)

    con.close()


def _init_inventory_snapshot_table():
    """Create the fba_inventory_snapshots table for daily inventory history.

    Required for sell-through rate trends and inventory health over time.
    One row per ASIN per snapshot_date.
    """
    con = get_db_rw()
    con.execute("""
        CREATE TABLE IF NOT EXISTS fba_inventory_snapshots (
            snapshot_date    DATE NOT NULL,
            asin             VARCHAR NOT NULL,
            sku              VARCHAR,
            product_name     VARCHAR,
            fulfillable_quantity    INTEGER DEFAULT 0,
            inbound_working_quantity INTEGER DEFAULT 0,
            inbound_shipped_quantity INTEGER DEFAULT 0,
            inbound_receiving_quantity INTEGER DEFAULT 0,
            reserved_quantity       INTEGER DEFAULT 0,
            unfulfillable_quantity  INTEGER DEFAULT 0,
            division         VARCHAR DEFAULT 'unknown',
            customer         VARCHAR DEFAULT 'amazon',
            platform         VARCHAR DEFAULT 'sp_api',
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_date, asin)
        )
    """)
    con.close()


def _init_inventory_extended_tables():
    """Create extended inventory tables for aging, stranded, reimbursements, and FC distribution."""
    con = get_db_rw()

    # Inventory aging data from GET_FBA_INVENTORY_AGED_DATA report
    con.execute("""
        CREATE TABLE IF NOT EXISTS fba_inventory_aging (
            snapshot_date    DATE NOT NULL,
            asin             VARCHAR NOT NULL,
            sku              VARCHAR,
            product_name     VARCHAR,
            qty_0_90         INTEGER DEFAULT 0,
            qty_91_180       INTEGER DEFAULT 0,
            qty_181_270      INTEGER DEFAULT 0,
            qty_271_365      INTEGER DEFAULT 0,
            qty_365_plus     INTEGER DEFAULT 0,
            total_qty        INTEGER DEFAULT 0,
            estimated_ltsf   DOUBLE DEFAULT 0,
            division         VARCHAR DEFAULT 'unknown',
            customer         VARCHAR DEFAULT 'amazon',
            platform         VARCHAR DEFAULT 'sp_api',
            PRIMARY KEY (snapshot_date, asin)
        )
    """)

    # Stranded inventory from GET_STRANDED_INVENTORY_UI_DATA report
    con.execute("""
        CREATE TABLE IF NOT EXISTS fba_stranded_inventory (
            snapshot_date       DATE NOT NULL,
            asin                VARCHAR NOT NULL,
            sku                 VARCHAR,
            product_name        VARCHAR,
            stranded_qty        INTEGER DEFAULT 0,
            stranded_reason     VARCHAR,
            estimated_value     DOUBLE DEFAULT 0,
            date_stranded       DATE,
            division            VARCHAR DEFAULT 'unknown',
            customer            VARCHAR DEFAULT 'amazon',
            platform            VARCHAR DEFAULT 'sp_api',
            PRIMARY KEY (snapshot_date, asin)
        )
    """)

    # Reimbursements from GET_REIMBURSEMENTS_DATA report
    con.execute("""
        CREATE TABLE IF NOT EXISTS fba_reimbursements (
            reimbursement_id    VARCHAR PRIMARY KEY,
            reimbursement_date  DATE,
            asin                VARCHAR,
            sku                 VARCHAR,
            product_name        VARCHAR,
            reason              VARCHAR,
            quantity             INTEGER DEFAULT 0,
            amount              DOUBLE DEFAULT 0,
            currency            VARCHAR DEFAULT 'USD',
            division            VARCHAR DEFAULT 'unknown',
            customer            VARCHAR DEFAULT 'amazon',
            platform            VARCHAR DEFAULT 'sp_api'
        )
    """)

    # FC-level inventory (from Inventories API with granularityType=FulfillmentCenter)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fba_fc_inventory (
            snapshot_date       DATE NOT NULL,
            fulfillment_center  VARCHAR NOT NULL,
            asin                VARCHAR NOT NULL,
            sku                 VARCHAR,
            fulfillable_quantity INTEGER DEFAULT 0,
            total_quantity      INTEGER DEFAULT 0,
            division            VARCHAR DEFAULT 'unknown',
            customer            VARCHAR DEFAULT 'amazon',
            platform            VARCHAR DEFAULT 'sp_api',
            PRIMARY KEY (snapshot_date, fulfillment_center, asin)
        )
    """)

    con.close()


def _init_pricing_tables():
    """Create sale_prices and coupons tables for Profitability Pricing & Coupons tab."""
    con = get_db_rw()

    # Sequence for sale_prices
    if USE_POSTGRES:
        con.execute("CREATE SEQUENCE IF NOT EXISTS sale_prices_seq START 1")

    con.execute("""
        CREATE TABLE IF NOT EXISTS sale_prices (
            id              INTEGER PRIMARY KEY,
            asin            VARCHAR NOT NULL,
            sku             VARCHAR,
            product_name    VARCHAR,
            regular_price   DOUBLE PRECISION DEFAULT 0,
            sale_price      DOUBLE PRECISION DEFAULT 0,
            start_date      DATE,
            end_date        DATE,
            marketplace     VARCHAR DEFAULT 'US',
            status          VARCHAR DEFAULT 'scheduled',
            pushed_to_amazon BOOLEAN DEFAULT FALSE,
            pushed_at       TIMESTAMP,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            division        VARCHAR DEFAULT 'unknown',
            customer        VARCHAR DEFAULT 'amazon',
            platform        VARCHAR DEFAULT 'sp_api'
        )
    """)

    # Sequence for coupons
    if USE_POSTGRES:
        con.execute("CREATE SEQUENCE IF NOT EXISTS coupons_seq START 1")

    con.execute("""
        CREATE TABLE IF NOT EXISTS coupons (
            id              INTEGER PRIMARY KEY,
            title           VARCHAR,
            coupon_type     VARCHAR DEFAULT 'percentage',
            discount_value  DOUBLE PRECISION DEFAULT 0,
            budget          DOUBLE PRECISION DEFAULT 0,
            budget_used     DOUBLE PRECISION DEFAULT 0,
            start_date      DATE,
            end_date        DATE,
            marketplace     VARCHAR DEFAULT 'US',
            status          VARCHAR DEFAULT 'scheduled',
            pushed_to_amazon BOOLEAN DEFAULT FALSE,
            pushed_at       TIMESTAMP,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            division        VARCHAR DEFAULT 'unknown',
            customer        VARCHAR DEFAULT 'amazon',
            platform        VARCHAR DEFAULT 'sp_api'
        )
    """)

    # Coupon items junction table — supports multiple items per coupon
    con.execute("""
        CREATE TABLE IF NOT EXISTS coupon_items (
            coupon_id       INTEGER NOT NULL,
            asin            VARCHAR NOT NULL,
            sku             VARCHAR,
            product_name    VARCHAR,
            PRIMARY KEY (coupon_id, asin)
        )
    """)

    con.close()


def _init_retail_tables():
    """Create tables for Walmart/retail POS data from Scintilla reports."""
    con = get_db_rw()

    # Sequences for all retail tables
    for seq in ["walmart_store_weekly_seq", "walmart_item_weekly_seq",
                "walmart_scorecard_seq", "walmart_ecomm_weekly_seq",
                "walmart_order_forecast_seq", "retail_upload_log_seq"]:
        try:
            con.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq} START 1")
        except Exception:
            try:
                con.execute(f"CREATE SEQUENCE {seq}")
            except Exception:
                pass

    # Table 1: Store-level weekly POS & inventory
    con.execute("""
        CREATE TABLE IF NOT EXISTS walmart_store_weekly (
            id                  SERIAL PRIMARY KEY,
            walmart_week        VARCHAR(10)   NOT NULL,
            walmart_year        VARCHAR(4)    NOT NULL,
            store_name          VARCHAR(120),
            store_number        VARCHAR(10)   NOT NULL,
            vendor_dept_number  VARCHAR(10),
            pos_qty_ty          DOUBLE PRECISION DEFAULT 0,
            pos_qty_ly          DOUBLE PRECISION DEFAULT 0,
            pos_sales_ty        DOUBLE PRECISION DEFAULT 0,
            pos_sales_ly        DOUBLE PRECISION DEFAULT 0,
            gross_receipt_qty_ty    DOUBLE PRECISION DEFAULT 0,
            gross_receipt_qty_ly    DOUBLE PRECISION DEFAULT 0,
            net_receipt_qty_ty      DOUBLE PRECISION DEFAULT 0,
            net_receipt_qty_ly      DOUBLE PRECISION DEFAULT 0,
            returns_qty_ty          DOUBLE PRECISION DEFAULT 0,
            returns_qty_ly          DOUBLE PRECISION DEFAULT 0,
            returns_defective_qty_ty DOUBLE PRECISION DEFAULT 0,
            returns_defective_qty_ly DOUBLE PRECISION DEFAULT 0,
            on_hand_qty_ty          DOUBLE PRECISION DEFAULT 0,
            on_hand_qty_ly          DOUBLE PRECISION DEFAULT 0,
            clearance_on_hand_qty_ty DOUBLE PRECISION DEFAULT 0,
            clearance_on_hand_qty_ly DOUBLE PRECISION DEFAULT 0,
            in_transit_qty_ty       DOUBLE PRECISION DEFAULT 0,
            in_transit_qty_ly       DOUBLE PRECISION DEFAULT 0,
            in_warehouse_qty_ty     DOUBLE PRECISION DEFAULT 0,
            in_warehouse_qty_ly     DOUBLE PRECISION DEFAULT 0,
            instock_pct_ty          DOUBLE PRECISION,
            instock_pct_ly          DOUBLE PRECISION,
            repl_instock_pct_ty     DOUBLE PRECISION,
            repl_instock_pct_ly     DOUBLE PRECISION,
            returns_to_vendor_qty_ty    DOUBLE PRECISION DEFAULT 0,
            returns_to_vendor_qty_ly    DOUBLE PRECISION DEFAULT 0,
            returns_to_return_center_qty_ty DOUBLE PRECISION DEFAULT 0,
            returns_to_return_center_qty_ly DOUBLE PRECISION DEFAULT 0,
            returns_to_dc_qty_ty        DOUBLE PRECISION DEFAULT 0,
            returns_to_dc_qty_ly        DOUBLE PRECISION DEFAULT 0,
            returns_recall_qty_ty       DOUBLE PRECISION DEFAULT 0,
            returns_recall_qty_ly       DOUBLE PRECISION DEFAULT 0,
            inv_adjustment_qty_ty       DOUBLE PRECISION DEFAULT 0,
            inv_adjustment_qty_ly       DOUBLE PRECISION DEFAULT 0,
            traited_store_count_ty      INTEGER DEFAULT 0,
            traited_store_count_ly      INTEGER DEFAULT 0,
            division    VARCHAR(20) DEFAULT 'golf',
            customer    VARCHAR(30) DEFAULT 'walmart_stores',
            platform    VARCHAR(20) DEFAULT 'scintilla',
            UNIQUE(walmart_week, store_number, vendor_dept_number)
        )
    """)

    # Table 2: Item-level weekly POS & inventory
    con.execute("""
        CREATE TABLE IF NOT EXISTS walmart_item_weekly (
            id                  SERIAL PRIMARY KEY,
            walmart_week        VARCHAR(10)   NOT NULL,
            period_type         VARCHAR(10)   NOT NULL DEFAULT 'weekly',
            brand_name          VARCHAR(60),
            brand_owner_id      VARCHAR(20),
            prime_item_number   VARCHAR(20)   NOT NULL,
            prime_item_desc     VARCHAR(120),
            walmart_upc         VARCHAR(20),
            warehouse_pack_upc  VARCHAR(20),
            product_description VARCHAR(120),
            sales_type          VARCHAR(20),
            vendor_name         VARCHAR(80),
            vendor_number       VARCHAR(10),
            vendor_pack_qty         DOUBLE PRECISION,
            vendor_pack_length      DOUBLE PRECISION,
            vendor_pack_height      DOUBLE PRECISION,
            vendor_pack_width       DOUBLE PRECISION,
            pos_sales_ty            DOUBLE PRECISION DEFAULT 0,
            pos_sales_ly            DOUBLE PRECISION DEFAULT 0,
            pos_qty_ty              DOUBLE PRECISION DEFAULT 0,
            pos_qty_ly              DOUBLE PRECISION DEFAULT 0,
            pos_store_count_ty      INTEGER DEFAULT 0,
            pos_store_count_ly      INTEGER DEFAULT 0,
            units_per_store_ty      DOUBLE PRECISION DEFAULT 0,
            units_per_store_ly      DOUBLE PRECISION DEFAULT 0,
            dollars_per_store_ty    DOUBLE PRECISION DEFAULT 0,
            dollars_per_store_ly    DOUBLE PRECISION DEFAULT 0,
            gross_receipt_qty_ty    DOUBLE PRECISION DEFAULT 0,
            gross_receipt_qty_ly    DOUBLE PRECISION DEFAULT 0,
            net_receipt_qty_ty      DOUBLE PRECISION DEFAULT 0,
            net_receipt_qty_ly      DOUBLE PRECISION DEFAULT 0,
            net_receipt_cost_ty     DOUBLE PRECISION DEFAULT 0,
            net_receipt_cost_ly     DOUBLE PRECISION DEFAULT 0,
            net_receipt_retail_ty   DOUBLE PRECISION DEFAULT 0,
            net_receipt_retail_ly   DOUBLE PRECISION DEFAULT 0,
            returns_qty_ty          DOUBLE PRECISION DEFAULT 0,
            returns_qty_ly          DOUBLE PRECISION DEFAULT 0,
            returns_amt_ty          DOUBLE PRECISION DEFAULT 0,
            returns_amt_ly          DOUBLE PRECISION DEFAULT 0,
            returns_defective_qty_ty DOUBLE PRECISION DEFAULT 0,
            returns_defective_qty_ly DOUBLE PRECISION DEFAULT 0,
            returns_defective_amt_ty DOUBLE PRECISION DEFAULT 0,
            returns_defective_amt_ly DOUBLE PRECISION DEFAULT 0,
            on_hand_qty_ty          DOUBLE PRECISION DEFAULT 0,
            on_hand_qty_ly          DOUBLE PRECISION DEFAULT 0,
            on_order_qty_ty         DOUBLE PRECISION DEFAULT 0,
            on_order_qty_ly         DOUBLE PRECISION DEFAULT 0,
            in_transit_qty_ty       DOUBLE PRECISION DEFAULT 0,
            in_transit_qty_ly       DOUBLE PRECISION DEFAULT 0,
            in_warehouse_qty_ty     DOUBLE PRECISION DEFAULT 0,
            in_warehouse_qty_ly     DOUBLE PRECISION DEFAULT 0,
            clearance_on_hand_qty_ty DOUBLE PRECISION DEFAULT 0,
            clearance_on_hand_qty_ly DOUBLE PRECISION DEFAULT 0,
            clearance_on_hand_eop_ty DOUBLE PRECISION DEFAULT 0,
            clearance_on_hand_eop_ly DOUBLE PRECISION DEFAULT 0,
            instock_pct_ty          DOUBLE PRECISION,
            instock_pct_ly          DOUBLE PRECISION,
            repl_instock_pct_ty     DOUBLE PRECISION,
            repl_instock_pct_ly     DOUBLE PRECISION,
            traited_store_count_ty  INTEGER DEFAULT 0,
            traited_store_count_ly  INTEGER DEFAULT 0,
            valid_store_count_ty    INTEGER DEFAULT 0,
            valid_store_count_ly    INTEGER DEFAULT 0,
            inv_adjustment_qty_ty   DOUBLE PRECISION DEFAULT 0,
            inv_adjustment_qty_ly   DOUBLE PRECISION DEFAULT 0,
            backroom_adj_qty        DOUBLE PRECISION DEFAULT 0,
            backroom_adj_type_code  VARCHAR(10),
            backroom_adj_type_desc  VARCHAR(60),
            division    VARCHAR(20) DEFAULT 'golf',
            customer    VARCHAR(30) DEFAULT 'walmart_stores',
            platform    VARCHAR(20) DEFAULT 'scintilla',
            UNIQUE(walmart_week, period_type, prime_item_number, sales_type, backroom_adj_type_code)
        )
    """)

    # Table 3: Vendor scorecard KPIs (pivoted)
    con.execute("""
        CREATE TABLE IF NOT EXISTS walmart_scorecard (
            id              SERIAL PRIMARY KEY,
            vendor_section  VARCHAR(120) NOT NULL,
            metric_group    VARCHAR(40)  NOT NULL,
            metric_name     VARCHAR(60)  NOT NULL,
            period          VARCHAR(20)  NOT NULL,
            period_range    VARCHAR(60),
            value_ty        DOUBLE PRECISION,
            value_ly        DOUBLE PRECISION,
            value_diff      DOUBLE PRECISION,
            report_date     DATE,
            upload_id       VARCHAR(40),
            division    VARCHAR(20) DEFAULT 'golf',
            customer    VARCHAR(30) DEFAULT 'walmart_stores',
            platform    VARCHAR(20) DEFAULT 'scintilla',
            UNIQUE(vendor_section, metric_group, metric_name, period, upload_id)
        )
    """)

    # Table 4: Walmart.com eComm item-level sales
    con.execute("""
        CREATE TABLE IF NOT EXISTS walmart_ecomm_weekly (
            id                      SERIAL PRIMARY KEY,
            walmart_week            VARCHAR(10)   NOT NULL,
            all_links_item_number   VARCHAR(20)   NOT NULL,
            product_name            VARCHAR(200),
            base_unit_retail_amount DOUBLE PRECISION,
            vendor_number           VARCHAR(20),
            vendor_name             VARCHAR(80),
            item_description        VARCHAR(120),
            fineline_description    VARCHAR(60),
            walmart_upc             VARCHAR(20),
            ecomm_upc               VARCHAR(20),
            omni_category           VARCHAR(60),
            omni_subcategory        VARCHAR(60),
            brand_name              VARCHAR(60),
            dept_description        VARCHAR(60),
            dept_number             VARCHAR(10),
            auth_based_qty_ty           DOUBLE PRECISION DEFAULT 0,
            auth_based_qty_ly           DOUBLE PRECISION DEFAULT 0,
            auth_based_net_sales_ty     DOUBLE PRECISION DEFAULT 0,
            auth_based_net_sales_ly     DOUBLE PRECISION DEFAULT 0,
            shipped_based_qty_ty        DOUBLE PRECISION DEFAULT 0,
            shipped_based_qty_ly        DOUBLE PRECISION DEFAULT 0,
            shipped_based_net_sales_ty  DOUBLE PRECISION DEFAULT 0,
            shipped_based_net_sales_ly  DOUBLE PRECISION DEFAULT 0,
            vendor_pack_cost        DOUBLE PRECISION,
            vendor_stock_id         VARCHAR(20),
            season_code             VARCHAR(10),
            season_description      VARCHAR(40),
            item_status_code        VARCHAR(10),
            division    VARCHAR(20) DEFAULT 'golf',
            customer    VARCHAR(30) DEFAULT 'walmart_stores',
            platform    VARCHAR(20) DEFAULT 'scintilla',
            UNIQUE(walmart_week, all_links_item_number, vendor_number)
        )
    """)

    # Table 5: DC order forecast snapshots
    con.execute("""
        CREATE TABLE IF NOT EXISTS walmart_order_forecast (
            id              SERIAL PRIMARY KEY,
            snapshot_date   DATE NOT NULL,
            store_dc_nbr    VARCHAR(10) NOT NULL,
            store_dc_type   VARCHAR(10),
            vendor_dept_number VARCHAR(10),
            division    VARCHAR(20) DEFAULT 'golf',
            customer    VARCHAR(30) DEFAULT 'walmart_stores',
            platform    VARCHAR(20) DEFAULT 'scintilla',
            UNIQUE(snapshot_date, store_dc_nbr, vendor_dept_number)
        )
    """)

    # Table 6: Upload tracking for all retail report uploads
    con.execute("""
        CREATE TABLE IF NOT EXISTS retail_upload_log (
            id          SERIAL PRIMARY KEY,
            upload_id   VARCHAR(40) NOT NULL,
            filename    VARCHAR(200),
            report_type VARCHAR(40) NOT NULL,
            channel     VARCHAR(30) NOT NULL,
            rows_loaded INTEGER DEFAULT 0,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            uploaded_by VARCHAR(60),
            status      VARCHAR(20) DEFAULT 'SUCCESS',
            error       TEXT
        )
    """)

    # Table 7: NIF Item Master (New Item Forecast data from Excel uploads)
    try:
        con.execute("CREATE SEQUENCE IF NOT EXISTS walmart_nif_items_seq START 1")
    except Exception:
        try:
            con.execute("CREATE SEQUENCE walmart_nif_items_seq")
        except Exception:
            pass

    con.execute("""
        CREATE TABLE IF NOT EXISTS walmart_nif_items (
            id                  SERIAL PRIMARY KEY,
            event_year          INTEGER NOT NULL,
            effective_week      INTEGER NOT NULL,
            item_status         VARCHAR(20) NOT NULL,
            description         VARCHAR(200),
            brand               VARCHAR(80),
            wmt_item_number     VARCHAR(20),
            upc                 VARCHAR(20),
            vendor_stock_number VARCHAR(40),
            brand_id            VARCHAR(20),
            wholesale_cost      DOUBLE PRECISION DEFAULT 0,
            walmart_retail      DOUBLE PRECISION DEFAULT 0,
            vendor_pack         INTEGER DEFAULT 0,
            whse_pack           INTEGER DEFAULT 0,
            old_store_count     INTEGER DEFAULT 0,
            new_store_count     INTEGER DEFAULT 0,
            store_count_diff    INTEGER DEFAULT 0,
            carton_length       DOUBLE PRECISION DEFAULT 0,
            carton_width        DOUBLE PRECISION DEFAULT 0,
            carton_height       DOUBLE PRECISION DEFAULT 0,
            cbm                 DOUBLE PRECISION DEFAULT 0,
            cbf                 DOUBLE PRECISION DEFAULT 0,
            color               VARCHAR(60),
            dexterity           VARCHAR(60),
            category            VARCHAR(80),
            mod_type            VARCHAR(40),
            division            VARCHAR(20) DEFAULT 'golf',
            customer            VARCHAR(30) DEFAULT 'walmart_stores',
            platform            VARCHAR(20) DEFAULT 'scintilla',
            UNIQUE(event_year, wmt_item_number, item_status)
        )
    """)

    con.close()


def _init_app_settings_table():
    """Create app_settings key-value table for storing API credentials, tokens, etc."""
    con = get_db_rw()
    con.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key         VARCHAR(100) PRIMARY KEY,
            value       TEXT NOT NULL,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.close()


def _fix_pg_sequences():
    """Reset PostgreSQL sequences to match the current max IDs in their tables.

    After a DuckDB → PostgreSQL migration, the sequences can get out of sync
    because migrated rows have explicit IDs that bypass nextval().  This causes
    'duplicate key value violates unique constraint' errors on INSERT.
    """
    if not USE_POSTGRES:
        return
    seq_table_map = {
        "sync_log_seq": "sync_log",
        "docs_update_log_seq": "docs_update_log",
        "sale_prices_seq": "sale_prices",
        "coupons_seq": "coupons",
        "walmart_store_weekly_seq": "walmart_store_weekly",
        "walmart_item_weekly_seq": "walmart_item_weekly",
        "walmart_scorecard_seq": "walmart_scorecard",
        "walmart_ecomm_weekly_seq": "walmart_ecomm_weekly",
        "walmart_order_forecast_seq": "walmart_order_forecast",
        "retail_upload_log_seq": "retail_upload_log",
        "walmart_nif_items_seq": "walmart_nif_items",
    }
    try:
        con = get_db_rw()
        for seq, table in seq_table_map.items():
            try:
                max_row = con.execute(
                    f"SELECT COALESCE(MAX(id), 0) FROM {table}"
                ).fetchone()
                max_id = int(max_row[0]) if max_row else 0
                con.execute(f"SELECT setval('{seq}', {max(max_id, 1)})")
                logger.info(f"Sequence {seq} reset to {max(max_id, 1)}")
            except Exception as e:
                logger.warning(f"Sequence reset {seq}: {e}")
        con.close()
    except Exception as e:
        logger.warning(f"Sequence fix error (non-fatal): {e}")


def init_all_tables():
    """Initialize all DuckDB tables on startup."""
    try:
        _init_auth_tables()
        logger.info("Auth tables initialized")
    except Exception as e:
        logger.error(f"Auth table init error: {e}")

    try:
        _init_system_tables()
        logger.info("System tables initialized")
    except Exception as e:
        logger.error(f"System table init error: {e}")

    try:
        _init_sales_tables()
        logger.info("Sales tables initialized")
    except Exception as e:
        logger.error(f"Sales table init error: {e}")

    try:
        _init_advertising_tables()
        logger.info("Advertising tables initialized")
    except Exception as e:
        logger.error(f"Advertising table init error: {e}")

    try:
        _init_item_plan_tables()
        logger.info("Item Plan tables initialized")
    except Exception as e:
        logger.error(f"Item Plan table init error: {e}")

    try:
        _init_item_master_table()
        logger.info("Item Master table initialized")
    except Exception as e:
        logger.error(f"Item Master table init error: {e}")

    try:
        _init_staging_tables()
        logger.info("Staging tables initialized")
    except Exception as e:
        logger.error(f"Staging table init error: {e}")

    try:
        _init_analytics_tables()
        logger.info("Analytics tables initialized")
    except Exception as e:
        logger.error(f"Analytics table init error: {e}")

    try:
        from .mfa_database import _init_mfa_tables
        _init_mfa_tables()
        logger.info("MFA tables initialized")
    except Exception as e:
        logger.error(f"MFA table init error: {e}")

    try:
        _init_inventory_snapshot_table()
        logger.info("Inventory snapshot table initialized")
    except Exception as e:
        logger.error(f"Inventory snapshot table init error: {e}")

    try:
        _init_inventory_extended_tables()
        logger.info("Inventory extended tables initialized")
    except Exception as e:
        logger.error(f"Inventory extended tables init error: {e}")

    try:
        _init_pricing_tables()
        logger.info("Pricing tables initialized (sale_prices, coupons, coupon_items)")
    except Exception as e:
        logger.error(f"Pricing table init error: {e}")

    try:
        _init_retail_tables()
        logger.info("Retail tables initialized (walmart_store_weekly, walmart_item_weekly, walmart_scorecard, walmart_ecomm_weekly, walmart_order_forecast, retail_upload_log)")
    except Exception as e:
        logger.error(f"Retail table init error: {e}")

    try:
        _init_app_settings_table()
        logger.info("App settings table initialized")
    except Exception as e:
        logger.error(f"App settings table init error: {e}")

    # ── Fix PostgreSQL sequences after migration ──
    try:
        _fix_pg_sequences()
    except Exception as e:
        logger.warning(f"Sequence fix error (non-fatal): {e}")

    # ── Auto-tag item_master divisions based on product names ──
    try:
        con = get_db_rw()
        # Count untagged ASINs
        untagged = con.execute(
            "SELECT COUNT(*) FROM item_master WHERE division IS NULL OR division = '' OR division = 'unknown'"
        ).fetchone()[0]
        if untagged > 0:
            logger.info(f"Auto-tagging division on {untagged} item_master ASINs...")
            # Housewares keywords (Elite Global Brands products)
            con.execute("""
                UPDATE item_master
                SET division = 'housewares'
                WHERE (division IS NULL OR division = '' OR division = 'unknown')
                  AND (
                    LOWER(product_name) LIKE '%casserole%'
                    OR LOWER(product_name) LIKE '%tumbler%'
                    OR LOWER(product_name) LIKE '%kitchen%'
                    OR LOWER(product_name) LIKE '%carrier%'
                    OR LOWER(product_name) LIKE '%insulated%'
                    OR LOWER(product_name) LIKE '%food%'
                    OR LOWER(product_name) LIKE '%baking%'
                    OR LOWER(product_name) LIKE '%oven%'
                    OR LOWER(product_name) LIKE '%cookware%'
                    OR LOWER(brand) LIKE '%elite%'
                    OR LOWER(brand) LIKE '%egb%'
                  )
            """)
            # Default remaining untagged to 'golf' (GolfGen is primary business)
            con.execute("""
                UPDATE item_master
                SET division = 'golf'
                WHERE division IS NULL OR division = '' OR division = 'unknown'
            """)
            tagged = untagged - con.execute(
                "SELECT COUNT(*) FROM item_master WHERE division IS NULL OR division = '' OR division = 'unknown'"
            ).fetchone()[0]
            logger.info(f"item_master auto-tag complete: {tagged} ASINs tagged")
        con.close()
    except Exception as e:
        logger.warning(f"item_master auto-tag error (non-fatal): {e}")

    # ── Backfill division on ALL transactional tables ──
    try:
        con = get_db_rw()

        # 1. daily_sales — backfill from item_master, then default to 'golf'
        null_count = con.execute(
            "SELECT COUNT(*) FROM daily_sales WHERE division IS NULL OR division = '' OR division = 'unknown'"
        ).fetchone()[0]
        if null_count > 0:
            logger.info(f"Backfilling division on {null_count} daily_sales rows...")
            con.execute("""
                UPDATE daily_sales
                SET division = im.division,
                    customer = COALESCE(daily_sales.customer, 'amazon'),
                    platform = COALESCE(daily_sales.platform, 'sp_api')
                FROM item_master im
                WHERE daily_sales.asin = im.asin
                  AND im.division IS NOT NULL AND im.division != '' AND im.division != 'unknown'
                  AND (daily_sales.division IS NULL OR daily_sales.division = '' OR daily_sales.division = 'unknown')
            """)
            con.execute("""
                UPDATE daily_sales
                SET division = 'golf',
                    customer = COALESCE(customer, 'amazon'),
                    platform = COALESCE(platform, 'sp_api')
                WHERE division IS NULL OR division = '' OR division = 'unknown'
            """)
            remaining = con.execute(
                "SELECT COUNT(*) FROM daily_sales WHERE division IS NULL OR division = ''"
            ).fetchone()[0]
            logger.info(f"daily_sales backfill: {null_count - remaining} updated, {remaining} still NULL")

        # 2. financial_events — backfill from item_master by asin
        null_count = con.execute(
            "SELECT COUNT(*) FROM financial_events WHERE division IS NULL OR division = '' OR division = 'unknown'"
        ).fetchone()[0]
        if null_count > 0:
            logger.info(f"Backfilling division on {null_count} financial_events rows...")
            con.execute("""
                UPDATE financial_events
                SET division = im.division,
                    customer = COALESCE(financial_events.customer, 'amazon'),
                    platform = COALESCE(financial_events.platform, 'sp_api')
                FROM item_master im
                WHERE financial_events.asin = im.asin
                  AND im.division IS NOT NULL AND im.division != '' AND im.division != 'unknown'
                  AND (financial_events.division IS NULL OR financial_events.division = '' OR financial_events.division = 'unknown')
            """)
            con.execute("""
                UPDATE financial_events
                SET division = 'golf',
                    customer = COALESCE(customer, 'amazon'),
                    platform = COALESCE(platform, 'sp_api')
                WHERE division IS NULL OR division = '' OR division = 'unknown'
            """)
            logger.info("financial_events backfill complete")

        # 3. fba_inventory — backfill from item_master by asin
        null_count = con.execute(
            "SELECT COUNT(*) FROM fba_inventory WHERE division IS NULL OR division = '' OR division = 'unknown'"
        ).fetchone()[0]
        if null_count > 0:
            logger.info(f"Backfilling division on {null_count} fba_inventory rows...")
            con.execute("""
                UPDATE fba_inventory
                SET division = im.division,
                    customer = COALESCE(fba_inventory.customer, 'amazon'),
                    platform = COALESCE(fba_inventory.platform, 'sp_api')
                FROM item_master im
                WHERE fba_inventory.asin = im.asin
                  AND im.division IS NOT NULL AND im.division != '' AND im.division != 'unknown'
                  AND (fba_inventory.division IS NULL OR fba_inventory.division = '' OR fba_inventory.division = 'unknown')
            """)
            con.execute("""
                UPDATE fba_inventory
                SET division = 'golf',
                    customer = COALESCE(customer, 'amazon'),
                    platform = COALESCE(platform, 'sp_api')
                WHERE division IS NULL OR division = '' OR division = 'unknown'
            """)
            logger.info("fba_inventory backfill complete")

        # 4. orders — backfill via order_items ASIN join (orders don't have asin column)
        null_count = con.execute(
            "SELECT COUNT(*) FROM orders WHERE division IS NULL OR division = '' OR division = 'unknown'"
        ).fetchone()[0]
        if null_count > 0:
            logger.info(f"Backfilling division on {null_count} orders rows via order_items...")
            try:
                con.execute("""
                    UPDATE orders
                    SET division = sub.division
                    FROM (
                        SELECT DISTINCT oi.order_id, im.division
                        FROM order_items oi
                        JOIN item_master im ON oi.asin = im.asin
                        WHERE im.division IS NOT NULL AND im.division != '' AND im.division != 'unknown'
                    ) sub
                    WHERE orders.order_id = sub.order_id
                      AND (orders.division IS NULL OR orders.division = '' OR orders.division = 'unknown')
                """)
            except Exception as e:
                logger.warning(f"orders backfill via order_items: {e}")
            # Default remaining to golf
            con.execute("""
                UPDATE orders
                SET division = 'golf',
                    customer = COALESCE(customer, 'amazon'),
                    platform = COALESCE(platform, 'sp_api')
                WHERE division IS NULL OR division = '' OR division = 'unknown'
            """)
            logger.info("orders backfill complete")

        con.close()
    except Exception as e:
        logger.warning(f"Division backfill error (non-fatal): {e}")


def auto_migrate_from_duckdb():
    """
    Auto-migrate data from DuckDB to PostgreSQL on first startup with Postgres.
    Only runs when:
      1. USE_POSTGRES is True (DATABASE_URL is set)
      2. PostgreSQL tables are empty (daily_sales has 0 rows)
      3. DuckDB file exists on disk (from the Railway volume)
    """
    if not USE_POSTGRES:
        return

    import psycopg2
    from psycopg2.extras import execute_batch

    duckdb_path = DB_PATH
    if not duckdb_path.exists():
        logger.info("Auto-migrate: No DuckDB file found, skipping migration")
        return

    # Check if Postgres already has data
    try:
        pg_conn = psycopg2.connect(DATABASE_URL)
        cursor = pg_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_sales")
        pg_rows = cursor.fetchone()[0]
        cursor.close()
        pg_conn.close()
        if pg_rows > 0:
            logger.info(f"Auto-migrate: PostgreSQL already has {pg_rows} daily_sales rows, skipping")
            return
    except Exception as e:
        logger.info(f"Auto-migrate: Postgres check failed ({e}), will attempt migration")

    logger.info("=" * 60)
    logger.info("AUTO-MIGRATION: DuckDB → PostgreSQL starting")
    logger.info(f"  DuckDB: {duckdb_path}")
    logger.info(f"  PostgreSQL: {DATABASE_URL[:40]}...")
    logger.info("=" * 60)

    # Tables to migrate in dependency order
    tables = [
        "sessions", "user_permissions", "audit_log", "sync_log", "docs_update_log",
        "orders", "daily_sales", "fba_inventory", "financial_events",
        "advertising", "ads_campaigns", "ads_keywords", "ads_search_terms",
        "ads_negative_keywords", "monthly_sales_history",
        "item_plan_overrides", "item_plan_curve_selection",
        "item_plan_factory_orders", "item_plan_factory_order_items",
        "item_plan_settings", "item_master",
        "staging_orders", "staging_financial_events",
        "analytics_daily", "analytics_sku", "analytics_ads",
    ]

    try:
        duck_conn = duckdb.connect(str(duckdb_path), read_only=True)
        pg_conn = psycopg2.connect(DATABASE_URL)
        pg_conn.autocommit = False

        success = 0
        failed = []

        for table in tables:
            try:
                # Get row count from DuckDB
                try:
                    duck_count = duck_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                except Exception:
                    logger.warning(f"  {table}: not found in DuckDB, skipping")
                    continue

                if duck_count == 0:
                    logger.info(f"  {table}: empty in DuckDB, skipping")
                    success += 1
                    continue

                # Get column names (PRAGMA is DuckDB-specific; use DESCRIBE as fallback)
                try:
                    schema = duck_conn.execute(f"PRAGMA table_info('{table}')").fetchall()
                    columns = [col[1] for col in schema]
                except Exception:
                    # Fallback: infer columns from a SELECT
                    sample = duck_conn.execute(f"SELECT * FROM {table} LIMIT 1")
                    columns = [desc[0] for desc in sample.description]

                # Fetch all rows
                rows = duck_conn.execute(f"SELECT * FROM {table}").fetchall()

                # Build INSERT with ON CONFLICT DO NOTHING
                placeholders = ", ".join(["%s"] * len(columns))
                insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

                # Batch insert
                cursor = pg_conn.cursor()
                batch_size = 1000
                inserted = 0
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    execute_batch(cursor, insert_sql, batch)
                    pg_conn.commit()
                    inserted += len(batch)

                cursor.close()
                logger.info(f"  {table}: migrated {inserted}/{duck_count} rows")
                success += 1

            except Exception as e:
                logger.error(f"  {table}: migration failed: {e}")
                pg_conn.rollback()
                failed.append(table)

        duck_conn.close()
        pg_conn.close()

        logger.info("=" * 60)
        logger.info(f"AUTO-MIGRATION COMPLETE: {success} tables OK, {len(failed)} failed")
        if failed:
            logger.info(f"  Failed tables: {', '.join(failed)}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Auto-migration failed: {e}")
