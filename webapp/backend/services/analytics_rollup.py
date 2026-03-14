"""
Analytics rollup service.
Runs nightly at 2:30am Central after backup completes.

Pipeline: Raw tables → Staging tables → Analytics tables → Dashboard queries.

Stage 1: populate_staging() — Cleans/types raw data into staging_orders & staging_financial_events
Stage 2: run_daily_rollup() — Reads staging tables, aggregates into analytics_daily
Stage 3: run_sku_rollup() — Reads staging + item_master, aggregates into analytics_sku
"""

from core.database import get_db_rw
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from core.config import DB_PATH

logger = logging.getLogger("golfgen")


# ── Stage 1: Populate Staging Tables ────────────────────────────


def populate_staging_orders(con, target_date: date = None):
    """ETL: raw orders + order_items → staging_orders for a given date."""
    if target_date is None:
        target_date = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=1)).date()

    now = datetime.now(ZoneInfo("America/Chicago"))

    # Check tables exist
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if 'orders' not in tables or 'staging_orders' not in tables:
            logger.warning(f"Staging orders skipped: missing tables. Have: {tables}")
            return 0
    except Exception:
        return 0

    # Delete existing staging rows for this date (idempotent)
    con.execute("DELETE FROM staging_orders WHERE order_date = ?", [target_date])

    # Insert from orders + order_items joined with item_master for division
    inserted = con.execute("""
        INSERT INTO staging_orders
        (staging_id, source_order_id, division, customer, platform,
         order_date, asin, product_name, units, gross_amount,
         currency, marketplace, fulfillment_type, status, staged_at, is_processed)
        SELECT
            CONCAT(o.order_id, '_', COALESCE(oi.asin, 'unknown')) as staging_id,
            o.order_id,
            COALESCE(NULLIF(o.division, ''), NULLIF(o.division, 'unknown'),
                     NULLIF(im.division, ''), NULLIF(im.division, 'unknown'), 'golf') as division,
            COALESCE(NULLIF(o.customer, ''), 'amazon') as customer,
            COALESCE(NULLIF(o.platform, ''), 'sp_api') as platform,
            CAST(o.purchase_date AS DATE),
            oi.asin,
            COALESCE(im.product_name, oi.title, oi.asin),
            CAST(COALESCE(oi.quantity, 1) AS INTEGER),
            CAST(COALESCE(oi.item_price, 0) AS DECIMAL(12,2)),
            'USD',
            o.marketplace_id,
            o.fulfillment_channel,
            o.order_status,
            ?,
            FALSE
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        LEFT JOIN item_master im ON oi.asin = im.asin
        WHERE CAST(o.purchase_date AS DATE) = ?
    """, [now, target_date]).fetchone()

    count = con.execute(
        "SELECT COUNT(*) FROM staging_orders WHERE order_date = ?", [target_date]
    ).fetchone()[0]

    logger.info(f"staging_orders populated for {target_date}: {count} rows")
    return count


def populate_staging_financial(con, target_date: date = None):
    """ETL: raw financial_events → staging_financial_events for a given date."""
    if target_date is None:
        target_date = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=1)).date()

    now = datetime.now(ZoneInfo("America/Chicago"))

    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if 'financial_events' not in tables or 'staging_financial_events' not in tables:
            logger.warning(f"Staging financial skipped: missing tables.")
            return 0
    except Exception:
        return 0

    # Delete existing staging rows for this date (idempotent)
    con.execute("DELETE FROM staging_financial_events WHERE event_date = ?", [target_date])

    # Insert from financial_events joined with item_master for division
    con.execute("""
        INSERT INTO staging_financial_events
        (staging_id, source_event_id, division, customer, platform,
         event_date, event_type, asin, description, amount,
         currency, fee_type, staged_at, is_processed)
        SELECT
            CONCAT(COALESCE(fe.order_id, 'unknown'), '_', fe.event_type, '_', ROW_NUMBER() OVER()) as staging_id,
            fe.order_id,
            COALESCE(NULLIF(fe.division, ''), NULLIF(fe.division, 'unknown'),
                     NULLIF(im.division, ''), NULLIF(im.division, 'unknown'), 'golf') as division,
            COALESCE(NULLIF(fe.customer, ''), 'amazon') as customer,
            COALESCE(NULLIF(fe.platform, ''), 'sp_api') as platform,
            CAST(fe.date AS DATE),
            fe.event_type,
            fe.asin,
            fe.event_type,
            CAST(COALESCE(fe.product_charges, 0) AS DECIMAL(12,2)),
            'USD',
            fe.event_type,
            ?,
            FALSE
        FROM financial_events fe
        LEFT JOIN item_master im ON fe.asin = im.asin
        WHERE CAST(fe.date AS DATE) = ?
    """, [now, target_date])

    count = con.execute(
        "SELECT COUNT(*) FROM staging_financial_events WHERE event_date = ?", [target_date]
    ).fetchone()[0]

    logger.info(f"staging_financial_events populated for {target_date}: {count} rows")
    return count


def populate_staging(con, days_back: int = 7):
    """Populate staging tables for the last N days."""
    now = datetime.now(ZoneInfo("America/Chicago"))
    total_orders = 0
    total_financial = 0
    for i in range(1, days_back + 1):
        target = (now - timedelta(days=i)).date()
        total_orders += populate_staging_orders(con, target)
        total_financial += populate_staging_financial(con, target)
    return {"staging_orders": total_orders, "staging_financial": total_financial}


# ── Stage 2: Daily Rollup (reads staging) ───────────────────────


def run_daily_rollup(con, target_date: date = None):
    """Roll up analytics_daily for a given date.

    Reads from staging_orders + staging_financial_events (preferred)
    or falls back to raw tables if staging is empty.
    """
    if target_date is None:
        target_date = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=1)).date()

    now = datetime.now(ZoneInfo("America/Chicago"))

    # Check required tables exist
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if 'analytics_daily' not in tables:
            logger.warning(f"Daily rollup skipped: analytics_daily missing")
            return 0
    except Exception as e:
        logger.warning(f"Daily rollup check failed: {e}")
        return 0

    # Try staging tables first; fall back to raw if empty
    use_staging = False
    try:
        staging_count = con.execute(
            "SELECT COUNT(*) FROM staging_orders WHERE order_date = ?", [target_date]
        ).fetchone()[0]
        use_staging = staging_count > 0
    except Exception:
        pass

    if use_staging:
        # Read from staging tables (clean, typed data)
        orders_data = con.execute("""
            SELECT
                division, customer, platform,
                COUNT(*) as order_count,
                SUM(gross_amount) as gross_revenue,
                SUM(units) as units_sold
            FROM staging_orders
            WHERE order_date = ?
              AND division != 'unknown'
            GROUP BY division, customer, platform
        """, [target_date]).fetchall()

        returns_data = con.execute("""
            SELECT
                division, customer, platform,
                COUNT(*) as return_count,
                ABS(SUM(amount)) as returns_amount
            FROM staging_financial_events
            WHERE event_date = ?
              AND event_type ILIKE '%refund%'
              AND division != 'unknown'
            GROUP BY division, customer, platform
        """, [target_date]).fetchall()
        logger.info(f"Daily rollup using STAGING tables for {target_date}")
    else:
        # Fallback: read from raw tables
        try:
            cols = [r[0] for r in con.execute("DESCRIBE orders").fetchall()]
            if 'division' not in cols:
                logger.warning("Daily rollup skipped: orders table missing division column")
                return 0
        except Exception:
            return 0

        orders_data = con.execute("""
            SELECT
                division, customer, platform,
                COUNT(*) as order_count,
                SUM(CAST(order_total AS DECIMAL)) as gross_revenue,
                SUM(CAST(number_of_items AS INTEGER)) as units_sold
            FROM orders
            WHERE CAST(purchase_date AS DATE) = ?
              AND division != 'unknown'
            GROUP BY division, customer, platform
        """, [target_date]).fetchall()

        returns_data = con.execute("""
            SELECT
                division, customer, platform,
                COUNT(*) as return_count,
                ABS(SUM(CAST(product_charges AS DECIMAL))) as returns_amount
            FROM financial_events
            WHERE CAST(date AS DATE) = ?
              AND event_type ILIKE '%refund%'
              AND division != 'unknown'
            GROUP BY division, customer, platform
        """, [target_date]).fetchall()
        logger.info(f"Daily rollup using RAW tables for {target_date} (staging empty)")

    # Build returns lookup
    returns_lookup = {}
    for row in returns_data:
        key = (row[0], row[1], row[2])
        returns_lookup[key] = {'units': row[3], 'amount': float(row[4] or 0)}

    # Upsert analytics_daily rows
    for row in orders_data:
        division, customer, platform = row[0], row[1], row[2]
        gross_revenue = float(row[4] or 0)
        units_sold = int(row[5] or 0)

        key = (division, customer, platform)
        ret = returns_lookup.get(key, {'units': 0, 'amount': 0.0})
        net_revenue = gross_revenue - ret['amount']

        row_id = f"{target_date}_{division}_{customer}"

        try:
            con.execute("""
                DELETE FROM analytics_daily
                WHERE date = ? AND division = ? AND customer = ?
            """, [target_date, division, customer])

            con.execute("""
                INSERT INTO analytics_daily
                (id, date, division, customer, platform,
                 units_sold, gross_revenue, returns_units, returns_amount,
                 net_revenue, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [row_id, target_date, division, customer, platform,
                  units_sold, gross_revenue, ret['units'], ret['amount'],
                  net_revenue, now])
        except Exception as e:
            logger.error(f"Error inserting analytics_daily for {row_id}: {e}")

    logger.info(f"analytics_daily rollup complete for {target_date}: {len(orders_data)} division/customer rows")
    return len(orders_data)


# ── Stage 3: SKU Rollup ─────────────────────────────────────────


def run_sku_rollup(con, period: str = 'last_30d'):
    """Roll up analytics_sku for a given period."""
    now = datetime.now(ZoneInfo("America/Chicago"))

    period_days = {'last_7d': 7, 'last_30d': 30, 'last_90d': 90}
    days = period_days.get(period, 30)
    cutoff = (now - timedelta(days=days)).date()

    # Check if required tables exist
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if 'item_master' not in tables or 'daily_sales' not in tables:
            logger.warning(f"SKU rollup skipped: missing tables. Have: {tables}")
            return 0
    except Exception:
        pass

    sku_data = con.execute("""
        SELECT
            ds.asin,
            COALESCE(im.product_name, ds.asin) as product_name,
            ds.division,
            ds.customer,
            SUM(CAST(ds.units_ordered AS INTEGER)) as units_sold,
            SUM(CAST(ds.ordered_product_sales AS DECIMAL)) as revenue
        FROM daily_sales ds
        LEFT JOIN item_master im ON ds.asin = im.asin
        WHERE CAST(ds.date AS DATE) >= ?
          AND ds.division != 'unknown'
          AND ds.asin != 'ALL'
        GROUP BY ds.asin, im.product_name, ds.division, ds.customer
        ORDER BY revenue DESC
    """, [cutoff]).fetchall()

    # Calculate ranks within each division
    division_rank = {}
    for row in sku_data:
        div = row[2]
        division_rank[div] = division_rank.get(div, 0) + 1
        rank = division_rank[div]

        row_id = f"{period}_{row[0]}_{row[2]}_{row[3]}"
        try:
            con.execute("DELETE FROM analytics_sku WHERE id = ?", [row_id])
            con.execute("""
                INSERT INTO analytics_sku
                (id, asin, product_name, division, customer, period,
                 units_sold, revenue, rank_in_division, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [row_id, row[0], row[1], row[2], row[3], period,
                  int(row[4] or 0), float(row[5] or 0), rank, now])
        except Exception as e:
            logger.error(f"Error inserting analytics_sku for {row_id}: {e}")

    logger.info(f"analytics_sku rollup complete for {period}: {len(sku_data)} SKU/division rows")
    return len(sku_data)


def _has_hierarchy_columns(con, table: str) -> bool:
    """Check if a table has division/customer/platform columns."""
    try:
        cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
        return 'division' in cols and 'customer' in cols
    except Exception:
        return False


def run_full_rollup():
    """Run all rollups. Called by scheduler nightly at 2:30am Central.

    Pipeline: populate staging → daily rollup → SKU rollup.
    """
    logger.info(f"Starting full rollup, DB_PATH={DB_PATH}")
    con = get_db_rw()
    # Verify we have the right database
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        logger.info(f"Tables in DB: {tables}")
    except Exception as e:
        logger.error(f"Cannot list tables: {e}")
        tables = []
    try:
        # Check if hierarchy columns exist — if not, run migration first
        if not _has_hierarchy_columns(con, 'orders'):
            logger.warning("orders table missing hierarchy columns — running inline migration")
            for table in ['orders', 'order_items', 'financial_events', 'fba_inventory',
                          'daily_sales', 'advertising', 'ads_campaigns']:
                for col, default in [('division', 'unknown'), ('customer', 'unknown'), ('platform', 'unknown')]:
                    try:
                        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} VARCHAR DEFAULT '{default}'")
                    except Exception:
                        pass  # column already exists
            # Tag existing rows as amazon/sp_api
            for table in ['orders', 'financial_events', 'fba_inventory', 'daily_sales']:
                try:
                    con.execute(f"UPDATE {table} SET customer = 'amazon', platform = 'sp_api' WHERE customer = 'unknown' OR customer IS NULL")
                except Exception:
                    pass
            # Add division to item_master
            for col in ['division', 'category', 'brand']:
                try:
                    con.execute(f"ALTER TABLE item_master ADD COLUMN {col} VARCHAR")
                except Exception:
                    pass
            logger.info("Inline migration complete")

        # Stage 1: Populate staging tables (last 7 days)
        staging_result = populate_staging(con, days_back=7)
        logger.info(f"Staging populated: {staging_result}")

        # Stage 2: Roll up last 7 days of daily analytics
        now = datetime.now(ZoneInfo("America/Chicago"))
        daily_total = 0
        for i in range(1, 8):
            target = (now - timedelta(days=i)).date()
            daily_total += run_daily_rollup(con, target)

        # Stage 3: Roll up SKU performance for all periods
        sku_total = 0
        for period in ['last_7d', 'last_30d', 'last_90d']:
            sku_total += run_sku_rollup(con, period)

        logger.info("Full analytics rollup complete")
        return {"daily_rows": daily_total, "sku_rows": sku_total,
                "staging_orders": staging_result["staging_orders"],
                "staging_financial": staging_result["staging_financial"]}
    finally:
        con.close()


if __name__ == "__main__":
    result = run_full_rollup()
    print(f"Rollup result: {result}")
