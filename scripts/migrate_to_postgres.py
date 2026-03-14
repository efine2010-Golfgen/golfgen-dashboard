"""
Migration script: DuckDB → PostgreSQL for GolfGen Dashboard.

Reads from DuckDB at data/golfgen_amazon.duckdb.
Writes to PostgreSQL via DATABASE_URL env var.

Usage:
    DATABASE_URL=postgres://user:pass@host/db python scripts/migrate_to_postgres.py
"""
import os
import sys
import logging
from pathlib import Path

# Add backend to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "webapp" / "backend"))

import duckdb
import psycopg2
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("migrate")

# Database paths and connection
DB_PATH = Path(__file__).parent.parent / "data" / "golfgen_amazon.duckdb"
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    logger.error("DATABASE_URL env var not set. Usage: DATABASE_URL=postgres://... python scripts/migrate_to_postgres.py")
    sys.exit(1)

if not DB_PATH.exists():
    logger.error(f"DuckDB file not found: {DB_PATH}")
    sys.exit(1)

# Tables to migrate in dependency order
TABLES_TO_MIGRATE = [
    "sessions",
    "user_permissions",
    "audit_log",
    "sync_log",
    "docs_update_log",
    "orders",
    "daily_sales",
    "fba_inventory",
    "financial_events",
    "advertising",
    "ads_campaigns",
    "ads_keywords",
    "ads_search_terms",
    "ads_negative_keywords",
    "monthly_sales_history",
    "item_plan_overrides",
    "item_plan_curve_selection",
    "item_plan_factory_orders",
    "item_plan_factory_order_items",
    "item_plan_settings",
    "item_master",
    "staging_orders",
    "staging_financial_events",
    "analytics_daily",
    "analytics_sku",
    "analytics_ads",
]

# Primary keys for each table (for CREATE TABLE statements)
TABLE_PKS = {
    "orders": ["order_id"],
    "daily_sales": ["date", "asin"],
    "fba_inventory": ["date", "asin", "condition"],
    "financial_events": [],  # no PK
    "advertising": ["date", "campaign_id"],
    "ads_campaigns": ["date", "campaign_id"],
    "ads_keywords": [],  # no PK
    "ads_search_terms": [],  # no PK
    "ads_negative_keywords": ["campaign_id", "ad_group_id", "negative_keyword"],
    "monthly_sales_history": ["sku", "year", "month"],
    "item_plan_overrides": ["sku", "field", "month"],
    "item_plan_curve_selection": ["sku"],
    "item_plan_factory_orders": ["po_number"],
    "item_plan_factory_order_items": ["id"],
    "item_plan_settings": ["key"],
    "item_master": ["asin"],
    "sessions": ["token"],
    "user_permissions": ["user_name", "tab_key"],
    "sync_log": ["id"],
    "docs_update_log": ["id"],
    "staging_orders": [],  # no PK
    "staging_financial_events": [],  # no PK
    "audit_log": [],  # no PK (has id column but no constraint)
    "analytics_daily": ["id"],
    "analytics_sku": ["id"],
    "analytics_ads": ["id"],
}


def get_duckdb_schema(duckdb_conn, table_name):
    """Fetch column names and types from DuckDB."""
    result = duckdb_conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    return result  # [(cid, name, type, notnull, dflt_value, pk), ...]


def duckdb_type_to_postgres(duckdb_type):
    """Convert DuckDB type to PostgreSQL type."""
    duckdb_type = duckdb_type.upper().strip()

    # Type mappings
    if duckdb_type == "DOUBLE":
        return "DOUBLE PRECISION"
    elif duckdb_type == "VARCHAR":
        return "TEXT"
    elif duckdb_type == "BOOLEAN":
        return "BOOLEAN"
    elif duckdb_type == "INTEGER":
        return "INTEGER"
    elif duckdb_type == "BIGINT":
        return "BIGINT"
    elif duckdb_type == "DATE":
        return "DATE"
    elif duckdb_type == "TIMESTAMP":
        return "TIMESTAMPTZ"
    elif duckdb_type == "DECIMAL":
        return "DECIMAL"
    elif "VARCHAR" in duckdb_type:
        return "TEXT"
    else:
        logger.warning(f"Unknown DuckDB type '{duckdb_type}', using TEXT")
        return "TEXT"


def build_create_table_postgres(duckdb_conn, table_name):
    """Build a PostgreSQL CREATE TABLE statement from DuckDB schema."""
    schema = get_duckdb_schema(duckdb_conn, table_name)
    
    if not schema:
        logger.error(f"  Table '{table_name}' has no columns in DuckDB")
        return None

    # Get column definitions
    columns = []
    for cid, col_name, duckdb_type, notnull, dflt_value, pk in schema:
        pg_type = duckdb_type_to_postgres(duckdb_type)

        # Handle BIGSERIAL for auto-increment ID columns
        if col_name == "id" and duckdb_type.upper() in ["BIGINT", "INTEGER"]:
            # Check if this is a sequence-backed column (sync_log, docs_update_log)
            if table_name in ["sync_log", "docs_update_log"]:
                pg_type = "BIGSERIAL"
        
        col_def = col_name + " " + pg_type
        
        # Add constraints
        if notnull and col_name != "id":  # id will be BIGSERIAL PRIMARY KEY
            col_def += " NOT NULL"
        
        if dflt_value:
            # Translate DuckDB defaults to Postgres
            if "CURRENT_TIMESTAMP" in str(dflt_value).upper():
                col_def += " DEFAULT CURRENT_TIMESTAMP"
            elif "CURRENT_DATE" in str(dflt_value).upper():
                col_def += " DEFAULT CURRENT_DATE"
            else:
                col_def += f" DEFAULT {dflt_value}"
        
        columns.append(col_def)

    # Build PRIMARY KEY constraint
    pks = TABLE_PKS.get(table_name, [])
    if pks:
        pk_cols = ", ".join(pks)
        columns.append(f"PRIMARY KEY ({pk_cols})")

    # Build CREATE TABLE statement
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        )
    """
    
    return create_sql


def fetch_all_rows(duckdb_conn, table_name):
    """Fetch all rows from a DuckDB table."""
    try:
        result = duckdb_conn.execute(f"SELECT * FROM {table_name}").fetchall()
        return result
    except Exception as e:
        logger.error(f"  Error fetching rows from {table_name}: {e}")
        return []


def get_column_names(duckdb_conn, table_name):
    """Get column names for a table."""
    schema = get_duckdb_schema(duckdb_conn, table_name)
    return [col[1] for col in schema]


def batch_insert_rows(pg_conn, table_name, columns, rows, batch_size=1000):
    """Insert rows into PostgreSQL in batches."""
    if not rows:
        logger.info(f"  {table_name}: no data to insert")
        return 0

    cursor = pg_conn.cursor()
    inserted = 0

    # Build parameterized INSERT statement
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    # Insert in batches
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        try:
            execute_batch(cursor, insert_sql, batch)
            pg_conn.commit()
            inserted += len(batch)
            logger.info(f"  {table_name}: inserted {inserted}/{len(rows)} rows")
        except Exception as e:
            logger.error(f"  {table_name}: batch insert failed at row {i}: {e}")
            pg_conn.rollback()
            return inserted

    cursor.close()
    return inserted


def migrate_table(duckdb_conn, pg_conn, table_name):
    """Migrate a single table from DuckDB to PostgreSQL."""
    logger.info(f"Migrating {table_name}...")

    # 1. Get DuckDB row count
    try:
        duckdb_count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except Exception as e:
        logger.error(f"  Error getting row count from DuckDB: {e}")
        return False

    logger.info(f"  DuckDB: {duckdb_count} rows")

    # 2. Create table in PostgreSQL
    try:
        create_sql = build_create_table_postgres(duckdb_conn, table_name)
        if not create_sql:
            logger.error(f"  Failed to build CREATE TABLE for {table_name}")
            return False
        
        cursor = pg_conn.cursor()
        cursor.execute(create_sql)
        pg_conn.commit()
        cursor.close()
        logger.info(f"  PostgreSQL: table created")
    except Exception as e:
        logger.error(f"  Error creating table in PostgreSQL: {e}")
        pg_conn.rollback()
        return False

    # 3. Fetch all rows from DuckDB
    rows = fetch_all_rows(duckdb_conn, table_name)
    if not rows:
        logger.info(f"  {table_name}: no data to migrate")
        return True

    # 4. Insert rows into PostgreSQL
    columns = get_column_names(duckdb_conn, table_name)
    inserted = batch_insert_rows(pg_conn, table_name, columns, rows)
    
    logger.info(f"  {table_name}: migration complete ({inserted}/{duckdb_count} rows)")
    return inserted > 0 or duckdb_count == 0


def main():
    """Main migration orchestration."""
    logger.info("Starting DuckDB → PostgreSQL migration")
    logger.info(f"  DuckDB: {DB_PATH}")
    logger.info(f"  PostgreSQL: {DATABASE_URL}")

    # Connect to DuckDB
    try:
        duckdb_conn = duckdb.connect(str(DB_PATH), read_only=True)
        logger.info("Connected to DuckDB")
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)

    # Connect to PostgreSQL
    try:
        pg_conn = psycopg2.connect(DATABASE_URL)
        pg_conn.autocommit = False
        logger.info("Connected to PostgreSQL")
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        duckdb_conn.close()
        sys.exit(1)

    # Migrate each table
    success_count = 0
    failed_tables = []

    for table_name in TABLES_TO_MIGRATE:
        try:
            if migrate_table(duckdb_conn, pg_conn, table_name):
                success_count += 1
            else:
                failed_tables.append(table_name)
        except Exception as e:
            logger.error(f"Unexpected error migrating {table_name}: {e}")
            failed_tables.append(table_name)

    # Cleanup
    duckdb_conn.close()
    pg_conn.close()

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"Migration Summary")
    logger.info(f"{'='*60}")
    logger.info(f"Total tables: {len(TABLES_TO_MIGRATE)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {len(failed_tables)}")
    if failed_tables:
        logger.info(f"Failed tables: {', '.join(failed_tables)}")
    logger.info(f"{'='*60}")

    return 0 if len(failed_tables) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
