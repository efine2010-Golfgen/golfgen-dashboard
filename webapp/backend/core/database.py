"""
Database initialization and connection management.
All CREATE SEQUENCE and CREATE TABLE statements are here.
"""

import json
import logging
import duckdb
from pathlib import Path

from .config import DB_PATH, DB_DIR, USERS, ALL_TABS

logger = logging.getLogger("golfgen")


def get_db():
    """Return a read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def get_db_rw():
    """Return a read-write DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=False)


def _init_auth_tables():
    """Create DuckDB tables for sessions and permissions if not exist."""
    con = duckdb.connect(str(DB_PATH))
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
    """Create DuckDB tables for sync logging and docs updates if not exist."""
    con = duckdb.connect(str(DB_PATH))

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
            records_pulled BIGINT DEFAULT 0,
            records_inserted BIGINT DEFAULT 0,
            records_skipped BIGINT DEFAULT 0,
            error_message TEXT,
            duration_seconds DOUBLE
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
    """Create DuckDB tables for daily sales and FBA inventory if not exist."""
    con = duckdb.connect(str(DB_PATH))

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
            is_prime BOOLEAN
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
            net_proceeds DOUBLE DEFAULT 0
        )
    """)

    con.close()


def _init_advertising_tables():
    """Create DuckDB tables for Amazon Ads data if not exist."""
    con = duckdb.connect(str(DB_PATH))

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
            units BIGINT DEFAULT 0
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
            units BIGINT DEFAULT 0
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
                status TEXT DEFAULT 'PENDING'
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
                status TEXT DEFAULT 'PENDING'
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
                                (sku, year, month, units, revenue, profit, refund_units)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
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

                for order in factory_data.get("orders", []):
                    try:
                        con.execute("""
                            INSERT INTO item_plan_factory_orders
                            (po_number, factory, payment_terms, total_units, factory_cost,
                             fob_date, est_arrival, wk_received, wk_available, cbm, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            order.get("po_number", ""),
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
                        ])
                    except Exception:
                        pass

                for idx, item in enumerate(factory_data.get("items", []), start=1):
                    try:
                        con.execute("""
                            INSERT INTO item_plan_factory_order_items
                            (id, po_number, sku, description, units, est_arrival,
                             wk_received, wk_available, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            idx,
                            item.get("po_number", ""),
                            item.get("sku", ""),
                            item.get("description", ""),
                            int(item.get("units", 0)),
                            item.get("est_arrival", ""),
                            item.get("wk_received", ""),
                            item.get("wk_available", ""),
                            item.get("status", "PENDING"),
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
