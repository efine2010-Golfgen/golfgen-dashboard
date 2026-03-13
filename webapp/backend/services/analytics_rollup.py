"""
Analytics rollup service.
Runs nightly at 2:30am Central after backup completes.
Reads from raw tables, writes to analytics tables.
Dashboard routers should ONLY query analytics tables.
"""

import duckdb
import logging
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from core.config import DB_PATH

logger = logging.getLogger("golfgen")


def run_daily_rollup(con, target_date: date = None):
    """Roll up analytics_daily for a given date (default: yesterday)."""
    if target_date is None:
        target_date = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=1)).date()

    now = datetime.now(ZoneInfo("America/Chicago"))

    # Get all division+customer combinations that had activity on this date
    # Pull from orders (units + revenue)
    orders_data = con.execute("""
        SELECT
            division,
            customer,
            platform,
            COUNT(*) as order_count,
            SUM(CAST(order_total AS DECIMAL)) as gross_revenue,
            SUM(CAST(number_of_items AS INTEGER)) as units_sold
        FROM orders
        WHERE CAST(purchase_date AS DATE) = ?
          AND division != 'unknown'
        GROUP BY division, customer, platform
    """, [target_date]).fetchall()

    # Pull returns from financial_events
    returns_data = con.execute("""
        SELECT
            division,
            customer,
            platform,
            COUNT(*) as return_count,
            ABS(SUM(CAST(product_charges AS DECIMAL))) as returns_amount
        FROM financial_events
        WHERE CAST(date AS DATE) = ?
          AND event_type ILIKE '%refund%'
          AND division != 'unknown'
        GROUP BY division, customer, platform
    """, [target_date]).fetchall()

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


def run_sku_rollup(con, period: str = 'last_30d'):
    """Roll up analytics_sku for a given period."""
    now = datetime.now(ZoneInfo("America/Chicago"))

    period_days = {'last_7d': 7, 'last_30d': 30, 'last_90d': 90}
    days = period_days.get(period, 30)
    cutoff = (now - timedelta(days=days)).date()

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
    """Run all rollups. Called by scheduler nightly at 2:30am Central."""
    from core.database import get_db
    con = get_db()
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
        # Roll up last 7 days of daily analytics (catch any missed days)
        now = datetime.now(ZoneInfo("America/Chicago"))
        daily_total = 0
        for i in range(1, 8):
            target = (now - timedelta(days=i)).date()
            daily_total += run_daily_rollup(con, target)

        # Roll up SKU performance for all periods
        sku_total = 0
        for period in ['last_7d', 'last_30d', 'last_90d']:
            sku_total += run_sku_rollup(con, period)

        logger.info("Full analytics rollup complete")
        return {"daily_rows": daily_total, "sku_rows": sku_total}
    finally:
        con.close()


if __name__ == "__main__":
    result = run_full_rollup()
    print(f"Rollup result: {result}")
