"""
SP-API service functions for GolfGen Dashboard.
Handles Amazon SP-API synchronization including orders, financial events, inventory, and sales reports.
"""

import os
import json
import logging
import threading
import time as _time_mod
from core.database import get_db, get_db_rw
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional
from collections import defaultdict
from functools import wraps

from core.config import DB_PATH, DB_DIR, CONFIG_PATH, TIMEZONE, COGS_PATH

logger = logging.getLogger("golfgen")

# ── Sync mutex — prevents overlapping sync operations ──────────
_sync_lock = threading.Lock()
_sync_running: str | None = None  # name of currently-running sync, or None


def is_sync_running() -> str | None:
    """Return the name of the currently-running sync, or None."""
    return _sync_running


# ── Retry with exponential backoff ─────────────────────────────
def retry_with_backoff(max_retries: int = 5, base_delay: float = 2.0, max_delay: float = 120.0):
    """Decorator: retry on throttle / QuotaExceeded / 429 with exponential backoff.

    Usage:
        @retry_with_backoff(max_retries=5)
        def call_api():
            ...
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    err_str = str(e).lower()
                    is_throttle = any(k in err_str for k in ("throttl", "429", "quotaexceeded", "too many request", "rate limit"))
                    if not is_throttle or attempt == max_retries:
                        raise
                    last_exc = e
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"  Throttled (attempt {attempt+1}/{max_retries}), retrying in {delay:.1f}s: {e}")
                    _time_mod.sleep(delay)
            raise last_exc  # should never reach here
        return wrapper
    return decorator


# ── Division lookup cache ──────────────────────────────────────
_division_cache: dict[str, str] = {}


def _get_division_for_asin(asin: str, con=None) -> str:
    """Look up the division for an ASIN from item_master.

    Returns the division string ('golf' or 'housewares').
    Falls back to 'golf' if the ASIN isn't in item_master yet.
    Results are cached in-memory for the lifetime of the process.
    """
    if not asin or asin == "ALL":
        return "golf"

    # Check cache first
    if asin in _division_cache:
        return _division_cache[asin]

    # Query item_master
    close_con = False
    try:
        if con is None:
            con = get_db()
            close_con = True
        row = con.execute(
            "SELECT division FROM item_master WHERE asin = ?", [asin]
        ).fetchone()
        division = row[0] if row and row[0] else "golf"
        _division_cache[asin] = division
        if close_con:
            con.close()
        return division
    except Exception:
        _division_cache[asin] = "golf"
        if close_con:
            try:
                con.close()
            except Exception:
                pass
        return "golf"


def _load_sp_api_credentials() -> dict | None:
    """Load SP-API credentials from env vars (Railway) or config file (local dev).

    Supports both naming conventions for env vars:
      - New: SP_API_REFRESH_TOKEN, SP_API_LWA_APP_ID, SP_API_LWA_CLIENT_SECRET, ...
      - Legacy: SP_API_REFRESH_TOKEN, LWA_APP_ID, LWA_CLIENT_SECRET, ...
    """
    # Priority 1: Environment variables (used on Railway)
    env_refresh = os.environ.get("SP_API_REFRESH_TOKEN", "")
    logger.info("SP-API creds check: refresh=%s lwa_id=%s lwa_secret=%s", bool(env_refresh), bool(os.environ.get("SP_API_LWA_APP_ID") or os.environ.get("SP_API_CLIENT_ID") or os.environ.get("LWA_APP_ID")), bool(os.environ.get("SP_API_LWA_CLIENT_SECRET") or os.environ.get("SP_API_CLIENT_SECRET") or os.environ.get("LWA_CLIENT_SECRET")))
    if env_refresh:
        return {
            "refresh_token": env_refresh,
            "lwa_app_id": os.environ.get("SP_API_LWA_APP_ID") or os.environ.get("SP_API_CLIENT_ID") or os.environ.get("LWA_APP_ID", ""),
            "lwa_client_secret": os.environ.get("SP_API_LWA_CLIENT_SECRET") or os.environ.get("SP_API_CLIENT_SECRET") or os.environ.get("LWA_CLIENT_SECRET", ""),
            "aws_access_key": os.environ.get("SP_API_AWS_ACCESS_KEY", ""),
            "aws_secret_key": os.environ.get("SP_API_AWS_SECRET_KEY", ""),
            "role_arn": os.environ.get("SP_API_ROLE_ARN", ""),
        }

    # Priority 2: Config file (local development)
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            creds_json = json.load(f)
        sp = creds_json.get("AMAZON_SP_API", {})
        creds = {
            "refresh_token": sp.get("refresh_token"),
            "lwa_app_id": sp.get("lwa_app_id"),
            "lwa_client_secret": sp.get("lwa_client_secret"),
            "aws_access_key": sp.get("aws_access_key"),
            "aws_secret_key": sp.get("aws_secret_key"),
            "role_arn": sp.get("role_arn"),
        }
        if creds.get("refresh_token"):
            return creds

    return None


# Track last today-sync so we don't hammer the API
_last_today_sync = 0.0


def _sync_today_orders():
    """Fast sync: pull today's orders from Amazon Orders API.

    This is the critical path for showing live "Today" data.
    Runs at startup, on /api/sync, and inline if today has no data.

    Strategy: Use getOrderItems for EVERY order to get accurate item-level
    prices.  OrderTotal is unreliable (missing for Pending, sometimes $0
    for FBA orders).  Item-level prices are the ground truth.
    """
    global _sync_running
    if not _sync_lock.acquire(blocking=False):
        logger.warning(f"  Skipping today-sync — '{_sync_running}' already running")
        return False
    _sync_running = "today_orders"
    try:
        return _sync_today_orders_inner()
    finally:
        _sync_running = None
        _sync_lock.release()


def _sync_today_orders_inner():
    """Inner implementation of today-orders sync (called under mutex)."""
    try:
        from sp_api.api import Orders as OrdersAPI
        from sp_api.base import Marketplaces
    except ImportError:
        logger.warning("SP-API library not installed — skipping today sync")
        return False

    credentials = _load_sp_api_credentials()
    if not credentials:
        return False

    logger.info("  Syncing today's orders (real-time)...")
    try:
        import time as _t
        # Get orders from last 7 days to catch delayed/pending orders
        now_utc = datetime.now(ZoneInfo("UTC"))
        after_date = (now_utc - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
        orders_api = OrdersAPI(credentials=credentials, marketplace=Marketplaces.US)
        response = orders_api.get_orders(
            CreatedAfter=after_date,
            MarketplaceIds=["ATVPDKIKX0DER"],
            MaxResultsPerPage=100
        )

        order_list = response.payload.get("Orders", [])

        # Handle pagination — get ALL orders, not just first 100
        next_token = response.payload.get("NextToken")
        while next_token:
            try:
                next_resp = orders_api.get_orders(
                    NextToken=next_token,
                    MarketplaceIds=["ATVPDKIKX0DER"],
                )
                more_orders = next_resp.payload.get("Orders", [])
                order_list.extend(more_orders)
                next_token = next_resp.payload.get("NextToken")
            except Exception:
                break

        logger.info(f"  Got {len(order_list)} orders from last 7 days")

        con = get_db_rw()
        con.execute("BEGIN TRANSACTION")

        # Store raw orders
        for order in order_list:
            order_total = order.get("OrderTotal", {})
            con.execute("""
                INSERT OR REPLACE INTO orders
                (order_id, purchase_date, order_status, fulfillment_channel,
                 sales_channel, order_total, currency_code, number_of_items,
                 ship_city, ship_state, ship_postal_code,
                 is_business_order, is_prime,
                 division, customer, platform)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
            """, [
                order.get("AmazonOrderId"),
                order.get("PurchaseDate"),
                order.get("OrderStatus"),
                order.get("FulfillmentChannel"),
                order.get("SalesChannel"),
                float(order_total.get("Amount", 0)) if order_total else 0,
                order_total.get("CurrencyCode", "USD") if order_total else "USD",
                order.get("NumberOfItemsShipped", 0) + order.get("NumberOfItemsUnshipped", 0),
                order.get("ShippingAddress", {}).get("City", ""),
                order.get("ShippingAddress", {}).get("StateOrRegion", ""),
                order.get("ShippingAddress", {}).get("PostalCode", ""),
                order.get("IsBusinessOrder", False),
                order.get("IsPrime", False),
            ])

        # ── Get item-level prices for ALL non-cancelled orders ──
        # OrderTotal is unreliable.  getOrderItems gives us actual item prices.
        # Rate limit: burst 30, then 0.5/sec.  We handle this with adaptive sleep.
        now_central = datetime.now(ZoneInfo("America/Chicago"))
        today_str = now_central.strftime("%Y-%m-%d")
        yesterday_str = (now_central - timedelta(days=1)).strftime("%Y-%m-%d")
        # Build set of recent dates we want item-level detail for (today + yesterday only)
        recent_dates = set()
        for d in range(2):
            recent_dates.add((now_central - timedelta(days=d)).strftime("%Y-%m-%d"))

        # Build ASIN price lookup from historical data for fallback
        asin_prices = {}
        try:
            price_rows = con.execute("""
                SELECT asin,
                       SUM(ordered_product_sales) / NULLIF(SUM(units_ordered), 0) AS avg_price
                FROM daily_sales
                WHERE asin != 'ALL' AND units_ordered > 0
                  AND date >= CURRENT_DATE - 90
                GROUP BY asin
            """).fetchall()
            for pr in price_rows:
                if pr[1] and pr[1] > 0:
                    asin_prices[pr[0]] = float(pr[1])
        except Exception:
            pass
        logger.info(f"  ASIN price fallback: {len(asin_prices)} ASINs with known prices")

        items_fetched = 0
        items_failed = 0
        burst_count = 0

        for order in order_list:
            status = order.get("OrderStatus", "")
            if status in ("Canceled", "Cancelled"):
                continue

            # Only fetch items for today and yesterday orders
            purchase_date = order.get("PurchaseDate", "")
            if not purchase_date:
                continue
            try:
                if "T" in purchase_date:
                    dt = datetime.fromisoformat(purchase_date.replace("Z", "+00:00"))
                    dt = dt.astimezone(ZoneInfo("America/Chicago"))
                    order_day = dt.strftime("%Y-%m-%d")
                else:
                    order_day = purchase_date[:10]
            except Exception:
                order_day = purchase_date[:10]

            if order_day not in recent_dates:
                continue

            order_id = order.get("AmazonOrderId")
            if not order_id:
                continue

            # Cap at 50 orders to prevent long-running syncs
            if items_fetched + items_failed >= 50:
                logger.info(f"  Reached 50-order cap for today sync, stopping item fetch")
                break

            try:
                @retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=15.0)
                def _fetch_items(oid):
                    return orders_api.get_order_items(oid)

                items_resp = _fetch_items(order_id)
                item_list = items_resp.payload.get("OrderItems", [])
                total_from_items = 0.0
                total_qty = 0
                for item in item_list:
                    asin = item.get("ASIN", "")
                    qty = 0
                    try:
                        qty = int(item.get("QuantityOrdered", 0))
                    except (ValueError, TypeError):
                        pass

                    # ItemPrice is the total for this line (price × qty)
                    price = item.get("ItemPrice")
                    item_amount = 0.0
                    if price and isinstance(price, dict):
                        amt_str = price.get("Amount", "0")
                        try:
                            item_amount = float(amt_str)
                        except (ValueError, TypeError):
                            pass

                    # Fallback: if ItemPrice is 0 or missing (Pending orders),
                    # use historical average price for this ASIN
                    if item_amount == 0 and qty > 0 and asin in asin_prices:
                        item_amount = round(asin_prices[asin] * qty, 2)
                        logger.info(f"    {order_id}: used fallback price for {asin} = ${item_amount:.2f} ({qty} × ${asin_prices[asin]:.2f})")

                    total_from_items += item_amount
                    total_qty += qty

                # Always use item-level total (more accurate than OrderTotal)
                order["_item_revenue"] = total_from_items
                order["_item_qty"] = total_qty
                items_fetched += 1

                # Adaptive rate limiting: burst first 15, then sleep
                burst_count += 1
                if burst_count > 15:
                    _time_mod.sleep(1.0)  # Stay well under rate limit
            except Exception as e:
                items_failed += 1
                logger.warning(f"  getOrderItems failed for {order_id} after retries: {e}")

        logger.info(f"  Fetched items for {items_fetched} orders ({items_failed} failed)")

        # ── Aggregate by date ──
        daily_agg = defaultdict(lambda: {"revenue": 0.0, "units": 0, "orders": 0})

        for order in order_list:
            status = order.get("OrderStatus", "")
            if status in ("Canceled", "Cancelled"):
                continue
            purchase_date = order.get("PurchaseDate", "")
            if not purchase_date:
                continue
            try:
                if "T" in purchase_date:
                    dt = datetime.fromisoformat(purchase_date.replace("Z", "+00:00"))
                    dt = dt.astimezone(ZoneInfo("America/Chicago"))
                    day_str = dt.strftime("%Y-%m-%d")
                else:
                    day_str = purchase_date[:10]
            except Exception:
                day_str = purchase_date[:10]

            # Prefer item-level revenue (most accurate), fall back to OrderTotal
            if "_item_revenue" in order:
                amount = order["_item_revenue"]
                n_items = order["_item_qty"]
            else:
                order_total = order.get("OrderTotal", {})
                amount = float(order_total.get("Amount", 0)) if order_total else 0
                n_items = order.get("NumberOfItemsShipped", 0) + order.get("NumberOfItemsUnshipped", 0)

            daily_agg[day_str]["revenue"] += amount
            daily_agg[day_str]["units"] += n_items
            daily_agg[day_str]["orders"] += 1

        # ── Write to daily_sales ──
        for day_str in sorted(daily_agg.keys()):
            agg = daily_agg[day_str]
            existing = con.execute(
                "SELECT ordered_product_sales FROM daily_sales WHERE date = ? AND asin = 'ALL'",
                [day_str]
            ).fetchone()
            existing_rev = float(existing[0]) if existing and existing[0] else 0.0

            # Always use the HIGHER of Orders API vs existing data.
            # Sales & Traffic Report data is more accurate for historical days,
            # Orders API may undercount (missing item prices on Pending orders).
            # Only overwrite if new revenue is >= existing — never lose data.
            if agg["revenue"] >= existing_rev:
                con.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, asin, units_ordered, ordered_product_sales,
                     sessions, session_percentage, page_views,
                     buy_box_percentage, unit_session_percentage,
                     division, customer, platform)
                    VALUES (?, 'ALL', ?, ?, 0, 0, 0, 0, 0, 'golf', 'amazon', 'sp_api')
                """, [day_str, agg["units"], agg["revenue"]])
                logger.info(f"  {day_str}: ${agg['revenue']:.2f} rev, {agg['units']} units, {agg['orders']} orders (was ${existing_rev:.2f})")

        con.execute("COMMIT")
        con.close()

        # Log summary for debugging
        for ds in sorted(daily_agg.keys()):
            a = daily_agg[ds]
            logger.info(f"  Date {ds}: ${a['revenue']:.2f} / {a['units']}u / {a['orders']} orders")

        logger.info(f"  Today sync complete: {len(order_list)} orders, {items_fetched} items fetched")
        return True
    except Exception as e:
        logger.error(f"  Today orders sync error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        try:
            con.execute("ROLLBACK")
            con.close()
            logger.info("  Today orders rolled back due to error")
        except Exception:
            pass
        return False


def _ensure_today_data():
    """If today has no data or stale data, do a quick Orders API pull.

    Called inline by profitability/comparison endpoints so the user
    sees the most current data available. Refreshes if:
    - Today has no data ($0), OR
    - Last sync was more than 30 minutes ago (data may be stale)
    Throttled to at most once per 3 minutes.
    """
    global _last_today_sync
    import time as _time
    now = _time.time()
    if now - _last_today_sync < 180:          # 3-minute cooldown
        return

    today_str = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
    try:
        con = get_db_rw()
        row = con.execute(
            "SELECT ordered_product_sales FROM daily_sales WHERE date = ? AND asin = 'ALL'",
            [today_str]
        ).fetchone()
        con.close()
        has_data = row and row[0] and float(row[0]) > 0
    except Exception:
        has_data = False

    # Refresh if no data, OR if last sync was over 30 minutes ago
    stale = (now - _last_today_sync) > 1800  # 30 minutes
    if not has_data or stale:
        reason = "no data" if not has_data else "stale (>30min)"
        logger.info(f"Today ({today_str}) needs refresh ({reason}) — triggering Orders API sync")
        _last_today_sync = now
        _sync_today_orders()


def _run_sp_api_sync():
    """Pull latest data from Amazon SP-API into DuckDB (runs in background thread).

    Order of operations (fastest first):
    1. Today's Orders (fast, ~5 seconds) — critical for live "Today" data
    2. Financial Events (fast, ~5 seconds) — actual fees/refunds
    3. FBA Inventory (fast, ~5 seconds)
    4. Sales & Traffic Report (SLOW, 2-5 minutes) — historical data with sessions
    """
    global _sync_running
    if not _sync_lock.acquire(blocking=False):
        logger.warning(f"  Skipping full SP-API sync — '{_sync_running}' already running")
        return
    _sync_running = "sp_api_full"
    try:
        _run_sp_api_sync_inner()
    finally:
        _sync_running = None
        _sync_lock.release()


def _run_sp_api_sync_inner():
    """Inner implementation of full SP-API sync (called under mutex)."""
    try:
        from sp_api.api import Reports, Orders, Inventories, Finances
        from sp_api.base import Marketplaces, ReportType
    except ImportError:
        logger.warning("SP-API library not installed — skipping sync")
        return

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("No SP-API credentials found (env vars or config file) — skipping sync")
        return

    logger.info("Starting SP-API background sync...")
    import time, gzip, requests as req

    # ── Ensure financial_events table exists ──────────────────────
    try:
        _con = get_db_rw()
        _con.execute("""
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
        _con.close()
    except Exception as e:
        logger.warning(f"  Could not ensure financial_events table: {e}")

    # ── 1. TODAY'S ORDERS (fast, real-time) ──────────────────────
    global _last_today_sync
    import time as _time
    _last_today_sync = _time.time()
    _sync_today_orders()

    # ── 2. FINANCIAL EVENTS (actual fees, refunds, promos) ──────
    try:
        from sp_api.api import Finances as FinancesAPI
        logger.info("  Pulling financial events (fees, refunds)...")

        fin_start = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
        finances = FinancesAPI(credentials=credentials, marketplace=Marketplaces.US)

        # Manual NextToken pagination (more reliable than @load_all_pages)
        all_shipment_events = []
        all_refund_events = []
        page = 0
        next_token = None

        while page < 20:
            page += 1
            kwargs = {"PostedAfter": fin_start, "MaxResultsPerPage": 100}
            if next_token:
                kwargs["NextToken"] = next_token

            try:
                @retry_with_backoff(max_retries=3, base_delay=2.0, max_delay=30.0)
                def _fetch_financial_page(**kw):
                    return finances.list_financial_events(**kw)

                resp = _fetch_financial_page(**kwargs)
            except Exception as api_err:
                logger.error(f"  Financial events API call error (page {page}): {api_err}")
                import traceback
                logger.error(traceback.format_exc())
                break

            payload = resp.payload if hasattr(resp, 'payload') else (resp if isinstance(resp, dict) else {})
            events = payload.get("FinancialEvents", {})

            if page == 1:
                logger.info(f"  Financial events payload keys: {list(payload.keys())}")
                logger.info(f"  Financial events event keys: {list(events.keys()) if events else 'EMPTY'}")
                # Log ALL event lists and their counts
                for k, v in events.items():
                    if isinstance(v, list):
                        logger.info(f"    {k}: {len(v)} items")

            shipments = events.get("ShipmentEventList", []) or []
            refunds = events.get("RefundEventList", []) or []
            adjustments = events.get("AdjustmentEventList", []) or []
            all_shipment_events.extend(shipments)
            all_refund_events.extend(refunds)
            # AdjustmentEventList often contains return-related adjustments
            for adj in adjustments:
                adj_type = adj.get("AdjustmentType", "") if isinstance(adj, dict) else getattr(adj, "AdjustmentType", "")
                adj_type_str = str(adj_type or "").lower()
                if "return" in adj_type_str or "refund" in adj_type_str or "reversal" in adj_type_str:
                    all_refund_events.append(adj)
            logger.info(f"  Financial events page {page}: {len(shipments)} shipments, {len(refunds)} refunds, {len(adjustments)} adjustments")

            # Check for next page
            next_token = payload.get("NextToken")
            if not next_token:
                break

        logger.info(f"  Total financial events: {len(all_shipment_events)} shipments, {len(all_refund_events)} refunds")
        con = get_db_rw()
        con.execute("BEGIN TRANSACTION")
        fin_records = 0

        # Helper: Amazon uses CurrencyAmount (not Amount) in money objects
        # sp_api may return model objects, dicts, Decimals, or strings
        # The python-amazon-sp-api library may use PascalCase OR snake_case
        # attributes depending on version, so we check both patterns.
        _money_debug_logged = False

        def _money(amt_obj):
            nonlocal _money_debug_logged
            if amt_obj is None:
                return 0.0
            # Plain dict — check multiple key conventions
            if isinstance(amt_obj, dict):
                for key in ("CurrencyAmount", "Amount", "currency_amount", "amount", "value"):
                    v = amt_obj.get(key)
                    if v is not None:
                        try:
                            return float(v)
                        except (ValueError, TypeError):
                            continue
                # Dict but no known keys — log once for diagnosis
                if not _money_debug_logged:
                    logger.warning(f"  _money: unknown dict keys: {list(amt_obj.keys())}, values: {list(amt_obj.values())[:3]}")
                    _money_debug_logged = True
                # Last resort: try first numeric value in dict
                for v in amt_obj.values():
                    try:
                        fv = float(v)
                        if fv != 0:
                            return fv
                    except (ValueError, TypeError):
                        continue
                return 0.0
            # Try converting model object to dict first (most reliable)
            if hasattr(amt_obj, "to_dict"):
                try:
                    d = amt_obj.to_dict()
                    if isinstance(d, dict):
                        for key in ("CurrencyAmount", "Amount", "currency_amount", "amount", "value"):
                            v = d.get(key)
                            if v is not None:
                                try:
                                    return float(v)
                                except (ValueError, TypeError):
                                    continue
                        # to_dict worked but no matching key — log and try any numeric
                        if not _money_debug_logged:
                            logger.warning(f"  _money: to_dict keys: {list(d.keys())}")
                            _money_debug_logged = True
                        for v in d.values():
                            try:
                                fv = float(v)
                                if fv != 0:
                                    return fv
                            except (ValueError, TypeError):
                                continue
                except Exception:
                    pass
            # sp_api model object — check PascalCase then snake_case attributes
            for attr in ("CurrencyAmount", "currency_amount", "Amount", "amount", "value"):
                if hasattr(amt_obj, attr):
                    try:
                        v = getattr(amt_obj, attr)
                        if v is not None:
                            return float(v)
                    except (ValueError, TypeError):
                        continue
            # Try __dict__ as last resort for model objects
            if hasattr(amt_obj, "__dict__"):
                d = amt_obj.__dict__
                for key in ("CurrencyAmount", "currency_amount", "Amount", "amount", "value",
                            "_currency_amount", "_amount"):
                    v = d.get(key)
                    if v is not None:
                        try:
                            return float(v)
                        except (ValueError, TypeError):
                            continue
                # Log unknown model attrs once
                if not _money_debug_logged:
                    non_private = {k: v for k, v in d.items() if not k.startswith("__")}
                    logger.warning(f"  _money: model __dict__ keys: {list(non_private.keys())}")
                    _money_debug_logged = True
            # Already a number (Decimal, float, int)
            try:
                return float(amt_obj)
            except Exception:
                if not _money_debug_logged:
                    logger.warning(f"  _money: could not parse type={type(amt_obj).__name__}, repr={repr(amt_obj)[:200]}")
                    _money_debug_logged = True
                return 0.0

        # Helper: extract string date from PostedDate (may be str or datetime)
        def _posted_date_str(pd_val):
            if pd_val is None:
                return None
            if isinstance(pd_val, str):
                return pd_val[:10] if len(pd_val) >= 10 else pd_val
            # datetime object
            if hasattr(pd_val, 'strftime'):
                return pd_val.strftime("%Y-%m-%d")
            return str(pd_val)[:10]

        # Helper: safely get dict-like value (works for dicts AND sp_api model objects)
        def _safe_get(obj, key, default=None):
            if obj is None:
                return default
            if isinstance(obj, dict):
                return obj.get(key, default)
            # sp_api model object — try attribute access
            return getattr(obj, key, default)

        # Clear old financial events for re-sync (avoids duplicates without needing PK)
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%d")
            con.execute("DELETE FROM financial_events WHERE date >= ?", [cutoff_date])
            logger.info(f"  Cleared financial_events from {cutoff_date} for re-sync")
        except Exception as e:
            logger.warning(f"  Could not clear old financial_events: {e}")

        # Process shipment events (sales fees)
        first_shipment_logged = False
        for event in all_shipment_events:
            order_id = _safe_get(event, "AmazonOrderId", "")
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)

            for item in (_safe_get(event, "ShipmentItemList") or []):
                sku = _safe_get(item, "SellerSKU", "")
                asin_val = _safe_get(item, "ASIN", sku)

                product_charges = 0.0
                shipping_ch = 0.0
                charge_list = _safe_get(item, "ItemChargeList") or []
                for c in charge_list:
                    val = _money(_safe_get(c, "ChargeAmount"))
                    ct = _safe_get(c, "ChargeType", "")
                    if ct == "Principal":
                        product_charges += val
                    elif ct in ("ShippingCharge", "Shipping"):
                        shipping_ch += val

                # Log first shipment's charge structure for debugging
                if not first_shipment_logged and charge_list:
                    first_c = charge_list[0]
                    ca = _safe_get(first_c, "ChargeAmount")
                    # Log the raw charge item completely
                    if isinstance(first_c, dict):
                        logger.info(f"  DEBUG charge item (dict): {first_c}")
                    else:
                        logger.info(f"  DEBUG charge item type={type(first_c).__name__}, repr={repr(first_c)[:500]}")
                        if hasattr(first_c, "to_dict"):
                            try:
                                logger.info(f"  DEBUG charge item to_dict={first_c.to_dict()}")
                            except Exception:
                                pass
                        if hasattr(first_c, "__dict__"):
                            logger.info(f"  DEBUG charge item __dict__={first_c.__dict__}")
                    # Log ChargeAmount specifically
                    logger.info(f"  DEBUG ChargeAmount type={type(ca).__name__}, repr={repr(ca)[:300]}, parsed={_money(ca)}")
                    # Also log the ShipmentItem itself
                    first_item = (_safe_get(event, "ShipmentItemList") or [None])[0]
                    if first_item:
                        if isinstance(first_item, dict):
                            logger.info(f"  DEBUG ShipmentItem keys={list(first_item.keys())}")
                        else:
                            logger.info(f"  DEBUG ShipmentItem type={type(first_item).__name__}")
                            if hasattr(first_item, "to_dict"):
                                try:
                                    logger.info(f"  DEBUG ShipmentItem to_dict={first_item.to_dict()}")
                                except Exception:
                                    pass
                    first_shipment_logged = True

                fba_fees_val = 0.0
                commission_val = 0.0
                other_val = 0.0
                for f in (_safe_get(item, "ItemFeeList") or []):
                    val = abs(_money(_safe_get(f, "FeeAmount")))
                    ft = _safe_get(f, "FeeType", "")
                    if "FBA" in ft or "Fulfillment" in ft:
                        fba_fees_val += val
                    elif ft in ("Commission", "ReferralFee"):
                        commission_val += val
                    else:
                        other_val += val

                promo_val = 0.0
                for p in (_safe_get(item, "PromotionList") or []):
                    promo_val += _money(_safe_get(p, "PromotionAmount"))

                net = product_charges + shipping_ch - fba_fees_val - commission_val + promo_val

                try:
                    _div = _get_division_for_asin(asin_val)
                    con.execute("""
                        INSERT INTO financial_events
                        (date, asin, sku, order_id, event_type,
                         product_charges, shipping_charges, fba_fees,
                         commission, promotion_amount, other_fees, net_proceeds,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                    """, [
                        date_str, asin_val, sku, order_id, "Shipment",
                        product_charges, shipping_ch,
                        fba_fees_val, commission_val, promo_val, other_val, net,
                        _div
                    ])
                    fin_records += 1
                except Exception as ins_err:
                    logger.warning(f"  Shipment insert error: {ins_err}")

        # Process refund events (from RefundEventList + return-related AdjustmentEventList)
        first_refund_logged = False
        for event in all_refund_events:
            order_id = _safe_get(event, "AmazonOrderId", "")
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)

            # Try multiple possible item list keys (different event types use different keys)
            item_list = (_safe_get(event, "ShipmentItemAdjustmentList")
                        or _safe_get(event, "ShipmentItemList")
                        or _safe_get(event, "AdjustmentItemList")
                        or [])

            if not first_refund_logged:
                logger.info(f"  DEBUG refund event keys: {list(event.keys()) if isinstance(event, dict) else dir(event)}")
                logger.info(f"  DEBUG refund item_list length: {len(item_list)}, type: {type(item_list).__name__}")
                if item_list:
                    first_item = item_list[0]
                    logger.info(f"  DEBUG refund first item keys: {list(first_item.keys()) if isinstance(first_item, dict) else 'model'}")
                first_refund_logged = True

            for item in item_list:
                sku = _safe_get(item, "SellerSKU", "")
                asin_val = _safe_get(item, "ASIN", sku)

                refund_amount = 0.0
                refund_fba = 0.0
                refund_comm = 0.0
                refund_other = 0.0
                charge_adj_list = (_safe_get(item, "ItemChargeAdjustmentList")
                                  or _safe_get(item, "ItemChargeList") or [])
                for c in charge_adj_list:
                    val = _money(_safe_get(c, "ChargeAmount"))
                    ct = _safe_get(c, "ChargeType", "")
                    if ct == "Principal":
                        refund_amount += val
                    else:
                        refund_other += abs(val)

                # Also capture fee adjustments for refunds
                fee_adj_list = (_safe_get(item, "ItemFeeAdjustmentList")
                               or _safe_get(item, "ItemFeeList") or [])
                for f in fee_adj_list:
                    val = _money(_safe_get(f, "FeeAmount"))
                    ft = _safe_get(f, "FeeType", "")
                    if "FBA" in ft or "Fulfillment" in ft:
                        refund_fba += abs(val)
                    elif ft in ("Commission", "ReferralFee"):
                        refund_comm += abs(val)

                try:
                    _div = _get_division_for_asin(asin_val)
                    con.execute("""
                        INSERT INTO financial_events
                        (date, asin, sku, order_id, event_type,
                         product_charges, shipping_charges, fba_fees,
                         commission, promotion_amount, other_fees, net_proceeds,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                    """, [
                        date_str,
                        asin_val, sku, order_id, "Refund",
                        abs(refund_amount), 0, refund_fba, refund_comm, 0, refund_other,
                        abs(refund_amount),
                        _div
                    ])
                    fin_records += 1
                    logger.info(f"  Inserted refund: order={order_id}, asin={asin_val}, amount=${abs(refund_amount):.2f}")
                except Exception as ins_err:
                    logger.error(f"  Refund insert error for {order_id}/{asin_val}: {ins_err}")

        con.execute("COMMIT")
        con.close()
        logger.info(f"  Financial events sync done: {fin_records} records (shipments + refunds)")
    except Exception as e:
        logger.error(f"  Financial events sync error: {e}")
        # Rollback on failure so partial data doesn't corrupt the table
        try:
            con.execute("ROLLBACK")
            con.close()
            logger.info("  Financial events rolled back due to error")
        except Exception:
            pass

    # ── 3. FBA INVENTORY (with retry + transaction wrapping) ────
    try:
        logger.info("  Pulling FBA inventory...")
        inventory_api = Inventories(credentials=credentials, marketplace=Marketplaces.US)

        @retry_with_backoff(max_retries=3, base_delay=2.0, max_delay=30.0)
        def _fetch_inventory():
            return inventory_api.get_inventory_summary_marketplace(
                marketplaceIds=["ATVPDKIKX0DER"],
                granularityType="Marketplace",
                granularityId="ATVPDKIKX0DER"
            )

        response = _fetch_inventory()
        summaries = response.payload.get("inventorySummaries", [])
        if summaries:
            con = get_db_rw()
            con.execute("BEGIN TRANSACTION")
            try:
                today = datetime.now(ZoneInfo("America/Chicago")).date()
                for item in summaries:
                    inv = item.get("inventoryDetails", {})
                    fulfillable = inv.get("fulfillableQuantity", 0) or item.get("totalQuantity", 0)
                    reserved = inv.get("reservedQuantity", {})
                    reserved_qty = reserved.get("totalReservedQuantity", 0) if isinstance(reserved, dict) else int(reserved or 0)
                    _inv_asin = item.get("asin", "")
                    _div = _get_division_for_asin(_inv_asin)
                    con.execute("""
                        INSERT OR REPLACE INTO fba_inventory
                        (date, asin, sku, product_name, condition,
                         fulfillable_quantity, inbound_working_quantity,
                         inbound_shipped_quantity, inbound_receiving_quantity,
                         reserved_quantity, unfulfillable_quantity, total_quantity,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                    """, [
                        today, _inv_asin,
                        item.get("sellerSku", item.get("fnSku", "")),
                        item.get("productName", "")[:200],
                        item.get("condition", "NewItem"),
                        int(fulfillable or 0),
                        int(inv.get("inboundWorkingQuantity", 0) or 0),
                        int(inv.get("inboundShippedQuantity", 0) or 0),
                        int(inv.get("inboundReceivingQuantity", 0) or 0),
                        int(reserved_qty or 0), 0,
                        int(item.get("totalQuantity", 0)),
                        _div
                    ])
                con.execute("COMMIT")
                logger.info(f"  Inventory sync done: {len(summaries)} items")
            except Exception as e:
                logger.error(f"  Inventory transaction error: {e}")
                try:
                    con.execute("ROLLBACK")
                    logger.info("  Inventory rolled back due to error")
                except Exception:
                    pass
            finally:
                con.close()
    except Exception as e:
        logger.error(f"  Inventory sync error: {e}")

    # ── 4. SALES & TRAFFIC REPORT (slow, 2-5 min polling) ──────
    try:
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=3)
        logger.info(f"  Requesting Sales report {start_date} to {end_date}...")

        reports = Reports(credentials=credentials, marketplace=Marketplaces.US)
        report_response = reports.create_report(
            reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
            dataStartTime=start_date.isoformat(),
            dataEndTime=end_date.isoformat(),
            reportOptions={"dateGranularity": "DAY", "asinGranularity": "CHILD"}
        )
        report_id = report_response.payload.get("reportId")

        report_data = None
        for attempt in range(30):
            time.sleep(10)
            status_response = reports.get_report(report_id)
            status = status_response.payload.get("processingStatus")
            if status == "DONE":
                doc_id = status_response.payload.get("reportDocumentId")
                doc_response = reports.get_report_document(doc_id)
                report_data = doc_response.payload
                break
            elif status in ("CANCELLED", "FATAL"):
                logger.error(f"  Report failed: {status}")
                break

        if report_data:
            if isinstance(report_data, dict) and "url" in report_data:
                is_gzipped = report_data.get("compressionAlgorithm", "").upper() == "GZIP"
                resp = req.get(report_data["url"])
                if is_gzipped:
                    import gzip as gz
                    report_text = gz.decompress(resp.content).decode("utf-8")
                else:
                    report_text = resp.text
                report_data = json.loads(report_text)

            if isinstance(report_data, str):
                report_data = json.loads(report_data)

            con = get_db_rw()
            con.execute("BEGIN TRANSACTION")
            records = 0

            for day_entry in report_data.get("salesAndTrafficByDate", []):
                entry_date = day_entry.get("date", "")
                traffic = day_entry.get("trafficByDate", {})
                sales_info = day_entry.get("salesByDate", {})
                ordered_sales = sales_info.get("orderedProductSales", {})
                sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                con.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, asin, units_ordered, ordered_product_sales,
                     sessions, session_percentage, page_views,
                     buy_box_percentage, unit_session_percentage,
                     division, customer, platform)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                """, [
                    entry_date, "ALL",
                    int(sales_info.get("unitsOrdered", 0)),
                    sales_amount,
                    int(traffic.get("sessions", 0)),
                    float(traffic.get("sessionPercentage", 0)),
                    int(traffic.get("pageViews", 0)),
                    float(traffic.get("buyBoxPercentage", 0)),
                    float(traffic.get("unitSessionPercentage", 0)),
                ])
                records += 1

            for asin_entry in report_data.get("salesAndTrafficByAsin", []):
                asin = asin_entry.get("childAsin") or asin_entry.get("parentAsin", "")
                traffic = asin_entry.get("trafficByAsin", {})
                sales_info = asin_entry.get("salesByAsin", {})
                entry_date = asin_entry.get("date", str(start_date))
                ordered_sales = sales_info.get("orderedProductSales", {})
                sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                _div = _get_division_for_asin(asin)
                con.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, asin, units_ordered, ordered_product_sales,
                     sessions, session_percentage, page_views,
                     buy_box_percentage, unit_session_percentage,
                     division, customer, platform)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                """, [
                    entry_date, asin,
                    int(sales_info.get("unitsOrdered", 0)),
                    sales_amount,
                    int(traffic.get("sessions", 0)),
                    float(traffic.get("sessionPercentage", 0)),
                    int(traffic.get("pageViews", 0)),
                    float(traffic.get("buyBoxPercentage", 0)),
                    float(traffic.get("unitSessionPercentage", 0)),
                    _div
                ])
                records += 1

            con.execute("COMMIT")
            con.close()
            logger.info(f"  Sales sync done: {records} records stored")

    except Exception as e:
        logger.error(f"  Sales sync error: {e}")
        try:
            con.execute("ROLLBACK")
            con.close()
            logger.info("  Sales sync rolled back due to error")
        except Exception:
            pass

    logger.info("SP-API background sync complete.")


def _backfill_orders(days: int = 90):
    """Backfill historical orders (mutex-protected wrapper)."""
    global _sync_running
    if not _sync_lock.acquire(blocking=False):
        logger.warning(f"  Skipping order backfill — '{_sync_running}' already running")
        return {"status": "skipped", "reason": f"'{_sync_running}' already running"}
    _sync_running = "order_backfill"
    try:
        return _backfill_orders_inner(days)
    finally:
        _sync_running = None
        _sync_lock.release()


def _backfill_orders_inner(days: int = 90):
    """Backfill historical orders using the Orders API.

    Quota-safe: burst 25 getOrderItems calls, then 2s sleep.
    Pulls ALL orders from the last `days` days, inserts into orders table,
    fetches item-level pricing, and re-aggregates daily_sales.
    """
    import time as _t

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.error("Order backfill: no SP-API credentials")
        return {"error": "No credentials"}

    try:
        from sp_api.api import Orders as OrdersAPI
        from sp_api.base import Marketplaces
    except ImportError:
        logger.error("Order backfill: SP-API library not installed")
        return {"error": "SP-API not installed"}

    now_utc = datetime.now(ZoneInfo("UTC"))
    after_date = (now_utc - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.info(f"Order backfill: pulling orders from last {days} days...")

    orders_api = OrdersAPI(credentials=credentials, marketplace=Marketplaces.US)

    # Fetch all orders with pagination
    all_orders = []
    try:
        response = orders_api.get_orders(
            CreatedAfter=after_date,
            MarketplaceIds=["ATVPDKIKX0DER"],
            MaxResultsPerPage=100
        )
        all_orders.extend(response.payload.get("Orders", []))
        next_token = response.payload.get("NextToken")

        page = 1
        while next_token and page < 50:
            page += 1
            _t.sleep(1)
            try:
                next_resp = orders_api.get_orders(
                    NextToken=next_token,
                    MarketplaceIds=["ATVPDKIKX0DER"],
                )
                all_orders.extend(next_resp.payload.get("Orders", []))
                next_token = next_resp.payload.get("NextToken")
            except Exception as e:
                logger.warning(f"Order backfill pagination error page {page}: {e}")
                break
    except Exception as e:
        logger.error(f"Order backfill: Orders API call failed: {e}")
        return {"error": str(e)}

    logger.info(f"Order backfill: got {len(all_orders)} orders total")

    # Insert all orders into the orders table
    con = get_db_rw()
    con.execute("BEGIN TRANSACTION")
    inserted = 0
    try:
        for order in all_orders:
            try:
                order_total = order.get("OrderTotal", {})
                con.execute("""
                    INSERT OR REPLACE INTO orders
                    (order_id, purchase_date, order_status, fulfillment_channel,
                     sales_channel, order_total, currency_code, number_of_items,
                     ship_city, ship_state, ship_postal_code,
                     is_business_order, is_prime,
                     division, customer, platform)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                """, [
                    order.get("AmazonOrderId"),
                    order.get("PurchaseDate"),
                    order.get("OrderStatus"),
                    order.get("FulfillmentChannel"),
                    order.get("SalesChannel"),
                    float(order_total.get("Amount", 0)) if order_total else 0,
                    order_total.get("CurrencyCode", "USD") if order_total else "USD",
                    order.get("NumberOfItemsShipped", 0) + order.get("NumberOfItemsUnshipped", 0),
                    order.get("ShippingAddress", {}).get("City", ""),
                    order.get("ShippingAddress", {}).get("StateOrRegion", ""),
                    order.get("ShippingAddress", {}).get("PostalCode", ""),
                    order.get("IsBusinessOrder", False),
                    order.get("IsPrime", False),
                ])
                inserted += 1
            except Exception as e:
                logger.warning(f"Order insert error: {e}")
    
        # Fetch item-level detail for accurate revenue
        daily_agg = defaultdict(lambda: {"revenue": 0.0, "units": 0, "orders": 0})
        items_fetched = 0
        burst_count = 0
    
        for order in all_orders:
            status = order.get("OrderStatus", "")
            if status in ("Canceled", "Cancelled"):
                continue
    
            purchase_date = order.get("PurchaseDate", "")
            if not purchase_date:
                continue
            try:
                if "T" in purchase_date:
                    dt = datetime.fromisoformat(purchase_date.replace("Z", "+00:00"))
                    dt = dt.astimezone(ZoneInfo("America/Chicago"))
                    day_str = dt.strftime("%Y-%m-%d")
                else:
                    day_str = purchase_date[:10]
            except Exception:
                day_str = purchase_date[:10]
    
            order_id = order.get("AmazonOrderId", "")
            total_from_items = 0.0
            total_qty = 0
    
            try:
                @retry_with_backoff(max_retries=5, base_delay=2.0, max_delay=60.0)
                def _fetch_backfill_items(oid):
                    return orders_api.get_order_items(oid)
    
                items_resp = _fetch_backfill_items(order_id)
                item_list = items_resp.payload.get("OrderItems", [])
                for item in item_list:
                    qty = int(item.get("QuantityOrdered", 0) or 0)
                    price = item.get("ItemPrice")
                    item_amount = 0.0
                    if price and isinstance(price, dict):
                        try:
                            item_amount = float(price.get("Amount", "0"))
                        except (ValueError, TypeError):
                            pass
                    total_from_items += item_amount
                    total_qty += qty
                items_fetched += 1
                burst_count += 1
                if burst_count > 25:
                    _t.sleep(2.0)
                    burst_count = 0
            except Exception:
                order_total = order.get("OrderTotal", {})
                total_from_items = float(order_total.get("Amount", 0)) if order_total else 0
                total_qty = order.get("NumberOfItemsShipped", 0) + order.get("NumberOfItemsUnshipped", 0)
    
            daily_agg[day_str]["revenue"] += total_from_items
            daily_agg[day_str]["units"] += total_qty
            daily_agg[day_str]["orders"] += 1
    
        # Write daily aggregates — only update if Orders API revenue >= existing
        # Never overwrite with lower data — Sales & Traffic Report is more accurate
        days_updated = 0
        for day_str in sorted(daily_agg.keys()):
            agg = daily_agg[day_str]
            if agg["revenue"] <= 0:
                continue
            existing = con.execute(
                "SELECT ordered_product_sales FROM daily_sales WHERE date = ? AND asin = 'ALL'",
                [day_str]
            ).fetchone()
            existing_rev = float(existing[0]) if existing and existing[0] else 0.0
    
            if agg["revenue"] >= existing_rev:
                con.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, asin, units_ordered, ordered_product_sales,
                     sessions, session_percentage, page_views,
                     buy_box_percentage, unit_session_percentage,
                     division, customer, platform)
                    VALUES (?, 'ALL', ?, ?, 0, 0, 0, 0, 0, 'golf', 'amazon', 'sp_api')
                """, [day_str, agg["units"], agg["revenue"]])
                days_updated += 1

        con.execute("COMMIT")
        con.close()
        result = {
            "status": "done",
            "orders_found": len(all_orders),
            "orders_inserted": inserted,
            "items_fetched": items_fetched,
            "days_updated": days_updated,
        }
        logger.info(f"Order backfill complete: {result}")
        return result
    except Exception as e:
        logger.error(f"Order backfill DB error: {e}")
        try:
            con.execute("ROLLBACK")
            con.close()
            logger.info("  Order backfill rolled back due to error")
        except Exception:
            pass
        return {"error": str(e)}
