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
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional
from collections import defaultdict
from functools import wraps

from core.config import DB_PATH, DB_DIR, CONFIG_PATH, TIMEZONE, COGS_PATH

logger = logging.getLogger("golfgen")

# In-memory ring buffer for recent log messages (accessible via /api/debug/logs)
import collections
_log_buffer = collections.deque(maxlen=200)

class _BufferHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            _log_buffer.append(msg)
        except Exception:
            pass

_buf_handler = _BufferHandler()
_buf_handler.setLevel(logging.DEBUG)
_buf_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(_buf_handler)

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

    Hard timeout of 120 seconds to prevent blocking other syncs.
    """
    global _sync_running
    if not _sync_lock.acquire(blocking=False):
        logger.warning(f"  Skipping today-sync — '{_sync_running}' already running")
        return False
    _sync_running = "today_orders"
    try:
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_sync_today_orders_inner)
            try:
                return future.result(timeout=120)  # 2-minute hard timeout
            except concurrent.futures.TimeoutError:
                logger.error("  Today sync TIMED OUT after 120s — releasing lock for other syncs")
                return False
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
        _sync_start = _t.time()
        # Get orders from last 7 days to catch delayed/pending orders
        now_utc = datetime.now(ZoneInfo("UTC"))
        after_date = (now_utc - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"  Creating Orders API client...")
        orders_api = OrdersAPI(credentials=credentials, marketplace=Marketplaces.US)
        logger.info(f"  Calling get_orders(CreatedAfter={after_date})...")
        response = orders_api.get_orders(
            CreatedAfter=after_date,
            MarketplaceIds=["ATVPDKIKX0DER"],
            MaxResultsPerPage=100
        )
        logger.info(f"  get_orders returned in {_t.time()-_sync_start:.1f}s")

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
        except Exception as price_err:
            logger.warning(f"  ASIN price lookup failed (non-fatal): {price_err}")
            # If we're inside a transaction that got poisoned, recover it
            try:
                con.execute("ROLLBACK")
                con.execute("BEGIN TRANSACTION")
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
            # CRITICAL: Preserve existing session/traffic data — Orders API doesn't have it.
            if agg["revenue"] >= existing_rev:
                existing_full = con.execute(
                    "SELECT sessions, session_percentage, page_views, buy_box_percentage, unit_session_percentage FROM daily_sales WHERE date = ? AND asin = 'ALL'",
                    [day_str]
                ).fetchone()
                ex_sess = int(existing_full[0]) if existing_full and existing_full[0] else 0
                ex_sess_pct = float(existing_full[1]) if existing_full and existing_full[1] else 0
                ex_pv = int(existing_full[2]) if existing_full and existing_full[2] else 0
                ex_bb = float(existing_full[3]) if existing_full and existing_full[3] else 0
                ex_usp = float(existing_full[4]) if existing_full and existing_full[4] else 0
                con.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, asin, units_ordered, ordered_product_sales,
                     sessions, session_percentage, page_views,
                     buy_box_percentage, unit_session_percentage,
                     division, customer, platform)
                    VALUES (?, 'ALL', ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                """, [day_str, agg["units"], agg["revenue"],
                      ex_sess, ex_sess_pct, ex_pv, ex_bb, ex_usp])
                logger.info(f"  {day_str}: ${agg['revenue']:.2f} rev, {agg['units']} units, {agg['orders']} orders (was ${existing_rev:.2f}, preserved sessions={ex_sess})")

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

    Returns list of (section, error) tuples — empty = all sections succeeded.
    """
    global _sync_running
    if not _sync_lock.acquire(blocking=False):
        logger.warning(f"  Skipping full SP-API sync — '{_sync_running}' already running")
        return []
    _sync_running = "sp_api_full"
    try:
        return _run_sp_api_sync_inner() or []
    finally:
        _sync_running = None
        _sync_lock.release()


def _run_sp_api_sync_inner():
    """Inner implementation of full SP-API sync (called under mutex).

    Returns a list of (section, error_message) tuples for any sections that
    failed. Empty list means all sections succeeded. The wrapper uses this
    to write SUCCESS / PARTIAL / FAILED to sync_log accurately.
    """
    _section_errors = []   # collect (section_name, error_str) for each failure

    try:
        from sp_api.api import Reports, Orders, Inventories, Finances
        from sp_api.base import Marketplaces, ReportType
    except ImportError:
        logger.warning("SP-API library not installed — skipping sync")
        return _section_errors

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("No SP-API credentials found (env vars or config file) — skipping sync")
        return _section_errors

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
    # Call _inner directly — we already hold the mutex, so the wrapper would
    # always skip (it tries to acquire the same non-reentrant lock).
    global _last_today_sync
    import time as _time
    _last_today_sync = _time.time()
    _sync_today_orders_inner()

    # ── 2. FINANCIAL EVENTS (actual fees, refunds, promos) ──────
    try:
        from sp_api.api import Finances as FinancesAPI
        logger.info("  Pulling financial events (fees, refunds)...")

        # Smart sync window: if we have < 12 months of history, do a deep 400-day
        # backfill so LY comparisons work. Otherwise only re-pull the last 35 days
        # (enough to catch late-posted events) and keep the historical rows intact.
        # NOTE: The 400-day window is handled in a separate gap-fill job to avoid
        # deleting recent data that can't be re-fetched due to API pagination limits.
        _con_check = get_db()
        _fin_count_row = _con_check.execute(
            "SELECT COUNT(*), MIN(date) FROM financial_events"
        ).fetchone()
        _con_check.close()
        _fin_count = int(_fin_count_row[0] or 0) if _fin_count_row else 0
        _oldest_date = _fin_count_row[1] if _fin_count_row and _fin_count_row[1] else None
        _oldest_days = (datetime.utcnow().date() - date.fromisoformat(str(_oldest_date)[:10])).days \
            if _oldest_date else 0

        if _oldest_days < 380:  # don't have 12+ months — do backfill
            fin_days = 400  # covers LY for full YTD comparison
            logger.info(f"  Financial events: backfill mode (oldest={_oldest_date}, "
                        f"count={_fin_count}) — pulling {fin_days} days")
        else:
            fin_days = 35   # regular refresh: catch late-posted events
            logger.info(f"  Financial events: regular mode (oldest={_oldest_date}) "
                        f"— pulling {fin_days} days")

        fin_start = (datetime.utcnow() - timedelta(days=fin_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        finances = FinancesAPI(credentials=credentials, marketplace=Marketplaces.US)

        # Manual NextToken pagination (more reliable than @load_all_pages)
        all_shipment_events = []
        all_refund_events = []
        all_fee_adj_events = []      # storage fees, placement fees, other AdjustmentEventList charges
        all_guarantee_events = []    # A-to-Z Guarantee claims
        all_chargeback_events = []   # credit card chargebacks
        all_service_fee_events = []  # misc Amazon service fees
        all_safet_reimb_events = []  # SAFET reimbursements (Amazon credits back to seller)
        all_liquidation_events = []  # FBA liquidation proceeds/fees
        all_coupon_payment_events = []  # coupon clip fees ($0.60/redemption)
        all_removal_events = []      # removal/disposal order fees
        page = 0
        next_token = None

        while page < 200:  # 200 pages × 100/page = 20,000 max events (covers 18 months)
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
            # Route adjustments: returns/refunds go to all_refund_events,
            # fee charges (storage, placement, etc.) go to all_fee_adj_events
            for adj in adjustments:
                adj_type = adj.get("AdjustmentType", "") if isinstance(adj, dict) else getattr(adj, "AdjustmentType", "")
                adj_type_str = str(adj_type or "").lower()
                if "return" in adj_type_str or "refund" in adj_type_str or "reversal" in adj_type_str:
                    all_refund_events.append(adj)
                else:
                    all_fee_adj_events.append(adj)
            # Capture additional account-level event lists
            all_guarantee_events.extend(events.get("GuaranteeClaimEventList", []) or [])
            all_chargeback_events.extend(events.get("ChargebackEventList", []) or [])
            all_service_fee_events.extend(events.get("ServiceFeeEventList", []) or [])
            all_safet_reimb_events.extend(events.get("SAFETReimbursementEventList", []) or [])
            all_liquidation_events.extend(events.get("FBALiquidationEventList", []) or [])
            all_coupon_payment_events.extend(events.get("CouponPaymentEventList", []) or [])
            all_removal_events.extend(events.get("RemovalShipmentEventList", []) or [])
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

        # Only clear events in the re-sync window if we got new data.
        # IMPORTANT: only delete the window we actually fetched (fin_days) so that
        # historical data outside that window (e.g. LY events from 300+ days ago)
        # is preserved for YOY comparisons. Uses ON CONFLICT DO NOTHING on INSERT
        # to avoid duplicates if we accidentally overlap.
        if all_shipment_events or all_refund_events:
            try:
                cutoff_date = (datetime.utcnow() - timedelta(days=fin_days)).strftime("%Y-%m-%d")
                con.execute("DELETE FROM financial_events WHERE date >= ?", [cutoff_date])
                logger.info(f"  Cleared financial_events from {cutoff_date} for re-sync "
                            f"({len(all_shipment_events)} shipments + {len(all_refund_events)} refunds to insert)")
            except Exception as e:
                logger.warning(f"  Could not clear old financial_events: {e}")
        else:
            logger.warning("  Financial events: API returned NO data — keeping existing records to prevent data loss")

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

        # Process fee adjustment events: storage fees, placement fees, and other
        # Amazon account-level charges posted via AdjustmentEventList.
        # AdjustmentType values for storage: StorageRenewalBilling, FBALongTermStorageFeeCharge
        # AdjustmentType values for other fees: FBAInventoryPlacementServiceFee,
        # InventoryPlacementServiceFee, FBAInboundConvenienceFee, etc.
        _STORAGE_ADJ_KEYWORDS = ("storagebilling", "storagerenewal", "storagefee",
                                  "longtermstoragefecharge", "longtermstorage")
        logger.info(f"  Processing {len(all_fee_adj_events)} fee adjustment events")
        for event in all_fee_adj_events:
            adj_type = _safe_get(event, "AdjustmentType", "") or ""
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            adj_norm = adj_type.lower().replace("_", "").replace("-", "")
            is_storage = any(kw in adj_norm for kw in _STORAGE_ADJ_KEYWORDS)
            ev_type = "StorageFee" if is_storage else "FeeAdjustment"

            item_list = _safe_get(event, "AdjustmentItemList") or []
            if item_list:
                # Per-ASIN adjustment items
                for item in item_list:
                    sku = _safe_get(item, "SellerSKU", "") or ""
                    asin_val = _safe_get(item, "ASIN", "") or sku
                    total_amt = abs(_money(_safe_get(item, "TotalAmount")))
                    if total_amt == 0:
                        continue
                    try:
                        _div = _get_division_for_asin(asin_val)
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, ?, ?, '', ?, 0, 0, 0, 0, 0, ?, ?, ?, 'amazon', 'sp_api')
                        """, [date_str, asin_val, sku, ev_type,
                              total_amt, -total_amt, _div])
                        fin_records += 1
                    except Exception as ins_err:
                        logger.warning(f"  Fee adj insert error ({adj_type}): {ins_err}")
            else:
                # Aggregate (no item list) — store as asin='ALL'
                total_amt = abs(_money(
                    _safe_get(event, "TotalAmount") or _safe_get(event, "AdjustmentTotalAmount")
                ))
                if total_amt > 0:
                    try:
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, 'ALL', '', '', ?, 0, 0, 0, 0, 0, ?, ?, 'unknown', 'amazon', 'sp_api')
                        """, [date_str, ev_type, total_amt, -total_amt])
                        fin_records += 1
                    except Exception as ins_err:
                        logger.warning(f"  Fee adj (agg) insert error ({adj_type}): {ins_err}")

        # ── A-to-Z Guarantee Claims ──────────────────────────────────────────
        logger.info(f"  Processing {len(all_guarantee_events)} A-to-Z guarantee claim events")
        for event in all_guarantee_events:
            order_id = _safe_get(event, "AmazonOrderId", "") or ""
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            item_list = _safe_get(event, "GuaranteeClaimItemList") or []
            if item_list:
                for item in item_list:
                    sku = _safe_get(item, "SellerSKU", "") or ""
                    asin_val = _safe_get(item, "ASIN", "") or sku
                    amt = sum(abs(_money(_safe_get(c, "ChargeAmount")))
                              for c in (_safe_get(item, "ItemChargeList") or []))
                    if amt == 0:
                        continue
                    try:
                        _div = _get_division_for_asin(asin_val)
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, ?, ?, ?, 'AtoZ', 0, 0, 0, 0, 0, ?, ?, ?, 'amazon', 'sp_api')
                        """, [date_str, asin_val, sku, order_id, amt, -amt, _div])
                        fin_records += 1
                    except Exception as ins_err:
                        logger.warning(f"  AtoZ insert error: {ins_err}")
            else:
                amt = abs(_money(_safe_get(event, "TotalAmount") or _safe_get(event, "GuaranteeClaimTotalAmount")))
                if amt > 0:
                    try:
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, 'ALL', '', ?, 'AtoZ', 0, 0, 0, 0, 0, ?, ?, 'unknown', 'amazon', 'sp_api')
                        """, [date_str, order_id, amt, -amt])
                        fin_records += 1
                    except Exception as ins_err:
                        logger.warning(f"  AtoZ agg insert error: {ins_err}")

        # ── Chargebacks ──────────────────────────────────────────────────────
        logger.info(f"  Processing {len(all_chargeback_events)} chargeback events")
        for event in all_chargeback_events:
            order_id = _safe_get(event, "AmazonOrderId", "") or ""
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            item_list = _safe_get(event, "ChargebackItemList") or []
            if item_list:
                for item in item_list:
                    sku = _safe_get(item, "SellerSKU", "") or ""
                    asin_val = _safe_get(item, "ASIN", "") or sku
                    amt = sum(abs(_money(_safe_get(c, "ChargeAmount")))
                              for c in (_safe_get(item, "ItemChargeList") or []))
                    if amt == 0:
                        continue
                    try:
                        _div = _get_division_for_asin(asin_val)
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, ?, ?, ?, 'Chargeback', 0, 0, 0, 0, 0, ?, ?, ?, 'amazon', 'sp_api')
                        """, [date_str, asin_val, sku, order_id, amt, -amt, _div])
                        fin_records += 1
                    except Exception as ins_err:
                        logger.warning(f"  Chargeback insert error: {ins_err}")
            else:
                amt = abs(_money(_safe_get(event, "ChargebackAmount") or _safe_get(event, "TotalAmount")))
                if amt > 0:
                    try:
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, 'ALL', '', ?, 'Chargeback', 0, 0, 0, 0, 0, ?, ?, 'unknown', 'amazon', 'sp_api')
                        """, [date_str, order_id, amt, -amt])
                        fin_records += 1
                    except Exception as ins_err:
                        logger.warning(f"  Chargeback agg insert error: {ins_err}")

        # ── Service Fees ─────────────────────────────────────────────────────
        logger.info(f"  Processing {len(all_service_fee_events)} service fee events")
        _svc_fee_logged = False
        _svc_fee_skipped = 0
        for event in all_service_fee_events:
            # Diagnostic: log the first event's keys to understand structure
            if not _svc_fee_logged:
                try:
                    if isinstance(event, dict):
                        logger.info(f"  ServiceFeeEvent sample keys: {list(event.keys())}")
                        fee_list_sample = event.get("FeeList") or event.get("fee_list")
                        logger.info(f"  ServiceFeeEvent FeeList sample: {fee_list_sample[:2] if fee_list_sample else 'EMPTY/NONE'}")
                    else:
                        logger.info(f"  ServiceFeeEvent type: {type(event).__name__}, attrs: {[a for a in dir(event) if not a.startswith('_')][:15]}")
                except Exception as _diag_err:
                    logger.info(f"  ServiceFeeEvent diag error: {_diag_err}")
                _svc_fee_logged = True
            order_id = _safe_get(event, "AmazonOrderId", "") or ""
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            sku = _safe_get(event, "SellerSKU", "") or ""
            asin_val = _safe_get(event, "ASIN", "") or sku or "ALL"
            # Try FeeList (PascalCase dict) and fee_list (snake_case model object)
            fee_list = (_safe_get(event, "FeeList") or
                        _safe_get(event, "fee_list") or [])
            amt = sum(abs(_money(_safe_get(f, "FeeAmount") or _safe_get(f, "fee_amount")))
                      for f in fee_list)
            if amt == 0:
                _svc_fee_skipped += 1
                continue
            try:
                _div = _get_division_for_asin(asin_val)
                con.execute("""
                    INSERT INTO financial_events
                    (date, asin, sku, order_id, event_type,
                     product_charges, shipping_charges, fba_fees,
                     commission, promotion_amount, other_fees, net_proceeds,
                     division, customer, platform)
                    VALUES (?, ?, ?, ?, 'ServiceFee', 0, 0, 0, 0, 0, ?, ?, ?, 'amazon', 'sp_api')
                """, [date_str, asin_val, sku, order_id, amt, -amt, _div])
                fin_records += 1
            except Exception as ins_err:
                logger.warning(f"  ServiceFee insert error: {ins_err}")
        logger.info(f"  ServiceFee: skipped {_svc_fee_skipped} zero-amt events")

        # ── SAFET Reimbursements (Amazon credits back to seller) ─────────────
        # Stored as positive product_charges + net_proceeds (it's income, not a cost)
        logger.info(f"  Processing {len(all_safet_reimb_events)} SAFET reimbursement events")
        for event in all_safet_reimb_events:
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            item_list = _safe_get(event, "SAFETReimbursementItemList") or []
            total_reimb = abs(_money(_safe_get(event, "ReimbursedAmount") or
                                     _safe_get(event, "TotalAmount")))
            if item_list:
                for item in item_list:
                    sku = _safe_get(item, "SellerSKU", "") or ""
                    asin_val = _safe_get(item, "ASIN", "") or sku or "ALL"
                    item_amt = sum(abs(_money(_safe_get(c, "ChargeAmount")))
                                   for c in (_safe_get(item, "ItemChargeList") or []))
                    if item_amt == 0 and total_reimb > 0:
                        item_amt = round(total_reimb / max(len(item_list), 1), 2)
                    if item_amt == 0:
                        continue
                    try:
                        _div = _get_division_for_asin(asin_val)
                        # Reimbursement is a credit — store as positive product_charges
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, ?, ?, '', 'SAFETReimbursement', ?, 0, 0, 0, 0, 0, ?, ?, 'amazon', 'sp_api')
                        """, [date_str, asin_val, sku, item_amt, item_amt, _div])
                        fin_records += 1
                    except Exception as ins_err:
                        logger.warning(f"  SAFET insert error: {ins_err}")
            elif total_reimb > 0:
                try:
                    con.execute("""
                        INSERT INTO financial_events
                        (date, asin, sku, order_id, event_type,
                         product_charges, shipping_charges, fba_fees,
                         commission, promotion_amount, other_fees, net_proceeds,
                         division, customer, platform)
                        VALUES (?, 'ALL', '', '', 'SAFETReimbursement', ?, 0, 0, 0, 0, 0, ?, 'unknown', 'amazon', 'sp_api')
                    """, [date_str, total_reimb, total_reimb])
                    fin_records += 1
                except Exception as ins_err:
                    logger.warning(f"  SAFET agg insert error: {ins_err}")

        # ── FBA Liquidation ──────────────────────────────────────────────────
        logger.info(f"  Processing {len(all_liquidation_events)} FBA liquidation events")
        for event in all_liquidation_events:
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            order_id = _safe_get(event, "OriginalRemovalOrderId", "") or ""
            proceeds = abs(_money(_safe_get(event, "LiquidationProceedsAmount")))
            fee = abs(_money(_safe_get(event, "LiquidationFeeAmount")))
            if proceeds == 0 and fee == 0:
                continue
            net = round(proceeds - fee, 2)
            try:
                con.execute("""
                    INSERT INTO financial_events
                    (date, asin, sku, order_id, event_type,
                     product_charges, shipping_charges, fba_fees,
                     commission, promotion_amount, other_fees, net_proceeds,
                     division, customer, platform)
                    VALUES (?, 'ALL', '', ?, 'FBALiquidation', ?, 0, 0, 0, 0, ?, ?, 'unknown', 'amazon', 'sp_api')
                """, [date_str, order_id, proceeds, fee, net])
                fin_records += 1
            except Exception as ins_err:
                logger.warning(f"  FBALiquidation insert error: {ins_err}")

        # ── Coupon Payment Events (clip fee = $0.60 per redemption) ──────────
        logger.info(f"  Processing {len(all_coupon_payment_events)} coupon payment events")
        for event in all_coupon_payment_events:
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            # FeeComponent is the clip fee charged to seller
            fee_comp = _safe_get(event, "FeeComponent")
            charge_comp = _safe_get(event, "ChargeComponent")
            # Try FeeComponent first, then ChargeComponent, then TotalAmount
            amt = abs(_money(fee_comp)) or abs(_money(charge_comp)) or abs(_money(_safe_get(event, "TotalAmount")))
            if amt == 0:
                continue
            coupon_id = _safe_get(event, "CouponId", "") or ""
            try:
                con.execute("""
                    INSERT INTO financial_events
                    (date, asin, sku, order_id, event_type,
                     product_charges, shipping_charges, fba_fees,
                     commission, promotion_amount, other_fees, net_proceeds,
                     division, customer, platform)
                    VALUES (?, 'ALL', '', ?, 'CouponFee', 0, 0, 0, 0, 0, ?, ?, 'unknown', 'amazon', 'sp_api')
                """, [date_str, coupon_id, amt, -amt])
                fin_records += 1
            except Exception as ins_err:
                logger.warning(f"  CouponFee insert error: {ins_err}")

        # ── Removal / Disposal Shipment Events ───────────────────────────────
        logger.info(f"  Processing {len(all_removal_events)} removal/disposal events")
        _removal_logged = False
        _removal_skipped = 0
        for event in all_removal_events:
            # Diagnostic: log the first event's keys
            if not _removal_logged:
                try:
                    if isinstance(event, dict):
                        logger.info(f"  RemovalEvent sample keys: {list(event.keys())}")
                        item_list_sample = event.get("RemovalShipmentItemList") or event.get("removal_shipment_item_list")
                        if item_list_sample:
                            logger.info(f"  RemovalEvent item[0] keys: {list(item_list_sample[0].keys()) if item_list_sample else 'EMPTY'}")
                    else:
                        logger.info(f"  RemovalEvent type: {type(event).__name__}")
                except Exception as _diag_err:
                    logger.info(f"  RemovalEvent diag error: {_diag_err}")
                _removal_logged = True
            posted_date = _safe_get(event, "PostedDate", "")
            date_str = _posted_date_str(posted_date)
            if not date_str:
                continue
            order_id = (_safe_get(event, "OrderId") or
                        _safe_get(event, "order_id") or "") 
            item_list = (_safe_get(event, "RemovalShipmentItemList") or
                         _safe_get(event, "removal_shipment_item_list") or [])
            for item in item_list:
                sku = _safe_get(item, "SellerSKU", "") or ""
                asin_val = _safe_get(item, "ASIN", "") or sku or "ALL"
                fee_items = (_safe_get(item, "RemovalShipmentItemFeeList") or
                             _safe_get(item, "removal_shipment_item_fee_list") or [])
                amt = sum(abs(_money(_safe_get(f, "FeeAmount") or _safe_get(f, "fee_amount")))
                          for f in fee_items)
                if amt == 0:
                    _removal_skipped += 1
                    continue
                try:
                    _div = _get_division_for_asin(asin_val)
                    con.execute("""
                        INSERT INTO financial_events
                        (date, asin, sku, order_id, event_type,
                         product_charges, shipping_charges, fba_fees,
                         commission, promotion_amount, other_fees, net_proceeds,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, 'RemovalFee', 0, 0, 0, 0, 0, ?, ?, ?, 'amazon', 'sp_api')
                    """, [date_str, asin_val, sku, order_id, amt, -amt, _div])
                    fin_records += 1
                except Exception as ins_err:
                    logger.warning(f"  RemovalFee insert error: {ins_err}")
        logger.info(f"  RemovalFee: skipped {_removal_skipped} zero-amt items")

        con.execute("COMMIT")
        con.close()
        logger.info(f"  Financial events sync done: {fin_records} records "
                    f"(shipments + refunds + fee adjustments + claims + reimbursements)")
    except Exception as e:
        logger.error(f"  Financial events sync error: {e}")
        _section_errors.append(("financial_events", str(e)))
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
        _section_errors.append(("inventory", str(e)))

    # ── 4. SALES & TRAFFIC REPORT (slow, 2-5 min polling) ──────
    try:
        end_date = datetime.utcnow().date()
        # Determine lookback: if we have fewer than 30 days of daily_sales data,
        # do a 182-day (26-week) backfill so the heatmap & charts are populated.
        # Otherwise, only refresh the last 3 days.
        lookback_days = 3
        try:
            con_check = get_db()
            days_count = con_check.execute(
                "SELECT COUNT(DISTINCT date) FROM daily_sales WHERE asin = 'ALL'"
            ).fetchone()
            con_check.close()
            existing_days = int(days_count[0]) if days_count else 0
            if existing_days < 30:
                lookback_days = 182  # 26 weeks for initial backfill
                logger.info(f"  S&T backfill mode: only {existing_days} days in DB → requesting {lookback_days} days")
        except Exception:
            pass
        start_date = end_date - timedelta(days=lookback_days)
        logger.info(f"  Requesting Sales report {start_date} to {end_date} ({lookback_days}d lookback)...")

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

            today_str = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
            yesterday_str = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=1)).strftime("%Y-%m-%d")

            for day_entry in report_data.get("salesAndTrafficByDate", []):
                entry_date = day_entry.get("date", "")
                traffic = day_entry.get("trafficByDate", {})
                sales_info = day_entry.get("salesByDate", {})
                ordered_sales = sales_info.get("orderedProductSales", {})
                sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                # For today and yesterday: S&T report may return partial/stale data.
                # Preserve the higher revenue value already in the database (which may
                # come from the Today Orders sync or a previous more-complete report run).
                if entry_date in (today_str, yesterday_str):
                    existing = con.execute(
                        "SELECT ordered_product_sales FROM daily_sales WHERE date = ? AND asin = 'ALL'",
                        [entry_date]
                    ).fetchone()
                    existing_rev = float(existing[0]) if existing and existing[0] else 0.0
                    if sales_amount < existing_rev:
                        # S&T report has lower value — keep existing revenue, only update traffic
                        con.execute("""
                            UPDATE daily_sales
                            SET sessions=?, session_percentage=?, page_views=?,
                                buy_box_percentage=?, unit_session_percentage=?
                            WHERE date=? AND asin='ALL'
                        """, [
                            int(traffic.get("sessions", 0)),
                            float(traffic.get("sessionPercentage", 0)),
                            int(traffic.get("pageViews", 0)),
                            float(traffic.get("buyBoxPercentage", 0)),
                            float(traffic.get("unitSessionPercentage", 0)),
                            entry_date
                        ])
                        records += 1
                        logger.info(f"  S&T {entry_date}: kept existing ${existing_rev:.2f} (S&T returned ${sales_amount:.2f})")
                        continue

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
        err_str = str(e)
        # Tag quota errors explicitly so the scheduler can classify them
        if any(k in err_str.lower() for k in ("quota", "quotaexceeded", "429")):
            _section_errors.append(("sales_report", f"QuotaExceeded: {err_str[:120]}"))
        else:
            _section_errors.append(("sales_report", err_str[:120]))
        try:
            con.execute("ROLLBACK")
            con.close()
            logger.info("  Sales sync rolled back due to error")
        except Exception:
            pass

    # ── 5. INVENTORY SNAPSHOTS (daily capture for trend charts) ──
    try:
        _sync_inventory_snapshots()
    except Exception as e:
        logger.warning(f"  Inventory snapshot error (non-fatal): {e}")

    # ── 6. EXTENDED INVENTORY REPORTS (aging, stranded, reimbursements, FC) ──
    # These are slower reports; run them after the core sync is done.
    try:
        _sync_aging_report()
    except Exception as e:
        logger.warning(f"  Aging report error (non-fatal): {e}")
        _section_errors.append(("aging_report", str(e)[:120]))

    try:
        _sync_stranded_report()
    except Exception as e:
        logger.warning(f"  Stranded report error (non-fatal): {e}")
        _section_errors.append(("stranded_report", str(e)[:120]))

    try:
        _sync_reimbursements_report()
    except Exception as e:
        logger.warning(f"  Reimbursements report error (non-fatal): {e}")
        _section_errors.append(("reimbursements_report", str(e)[:120]))

    try:
        _sync_fc_inventory()
    except Exception as e:
        logger.warning(f"  FC inventory error (non-fatal): {e}")
        _section_errors.append(("fc_inventory", str(e)[:120]))

    logger.info(f"SP-API background sync complete. Sections failed: {len(_section_errors)}")
    return _section_errors


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


def _sync_inventory_snapshots():
    """Capture today's fba_inventory into fba_inventory_snapshots for trend tracking.

    Called at the end of each full sync to build 90-day inventory history.
    """
    try:
        con = get_db_rw()
        today = datetime.now(ZoneInfo("America/Chicago")).date()

        # Check if we already have today's snapshot
        existing = con.execute(
            "SELECT COUNT(*) FROM fba_inventory_snapshots WHERE snapshot_date = ?",
            [today]
        ).fetchone()
        if existing and existing[0] > 0:
            logger.info(f"  Inventory snapshot for {today} already exists ({existing[0]} rows), skipping")
            con.close()
            return

        # Copy current inventory into snapshots
        con.execute("""
            INSERT INTO fba_inventory_snapshots
            (snapshot_date, asin, sku, product_name, fulfillable_quantity,
             inbound_working_quantity, inbound_shipped_quantity, inbound_receiving_quantity,
             reserved_quantity, unfulfillable_quantity, division, customer, platform)
            SELECT ?, asin, sku, product_name, fulfillable_quantity,
                   inbound_working_quantity, inbound_shipped_quantity, inbound_receiving_quantity,
                   reserved_quantity, unfulfillable_quantity, division, customer, platform
            FROM fba_inventory
        """, [today])

        count = con.execute(
            "SELECT COUNT(*) FROM fba_inventory_snapshots WHERE snapshot_date = ?", [today]
        ).fetchone()[0]

        # Clean up snapshots older than 120 days
        cutoff = (today - timedelta(days=120)).isoformat()
        con.execute("DELETE FROM fba_inventory_snapshots WHERE snapshot_date < ?", [cutoff])

        con.close()
        logger.info(f"  Inventory snapshot captured: {count} ASINs for {today}")
    except Exception as e:
        logger.warning(f"  Inventory snapshot error (non-fatal): {e}")


def _sync_aging_report():
    """Pull GET_FBA_INVENTORY_AGED_DATA report and store in fba_inventory_aging.

    This report provides inventory aging brackets needed for LTSF forecasting.
    """
    try:
        from sp_api.api import Reports
        from sp_api.base import Marketplaces, ReportType
    except ImportError:
        logger.warning("SP-API library not installed — skipping aging sync")
        return

    credentials = _load_sp_api_credentials()
    if not credentials:
        return

    logger.info("  Requesting FBA Inventory Aged Data report...")
    import time, gzip, requests as req

    try:
        reports = Reports(credentials=credentials, marketplace=Marketplaces.US)

        # Request the aging report
        report_response = reports.create_report(
            reportType="GET_FBA_INVENTORY_AGED_DATA"
        )
        report_id = report_response.payload.get("reportId")

        report_data = None
        for attempt in range(20):
            time.sleep(10)
            status_response = reports.get_report(report_id)
            status = status_response.payload.get("processingStatus")
            if status == "DONE":
                doc_id = status_response.payload.get("reportDocumentId")
                doc_response = reports.get_report_document(doc_id)
                report_data = doc_response.payload
                break
            elif status in ("CANCELLED", "FATAL"):
                logger.error(f"  Aging report failed: {status}")
                return

        if not report_data:
            logger.warning("  Aging report timed out after polling")
            return

        # Download and parse the report (TSV format)
        if isinstance(report_data, dict) and "url" in report_data:
            is_gzipped = report_data.get("compressionAlgorithm", "").upper() == "GZIP"
            resp = req.get(report_data["url"])
            if is_gzipped:
                import gzip as gz
                report_text = gz.decompress(resp.content).decode("utf-8")
            else:
                report_text = resp.text
        else:
            report_text = str(report_data)

        # Parse TSV
        import csv as csv_mod
        reader = csv_mod.DictReader(report_text.splitlines(), delimiter='\t')

        con = get_db_rw()
        today = datetime.now(ZoneInfo("America/Chicago")).date()
        records = 0

        for row in reader:
            asin = row.get("asin", "").strip()
            if not asin:
                continue

            sku = row.get("sku", "").strip()
            product_name = row.get("product-name", row.get("product_name", "")).strip()

            # Parse aging quantities from report columns
            qty_0_90 = int(float(row.get("inv-age-0-to-90-days", row.get("qty-with-removals-in-progress", 0)) or 0))
            qty_91_180 = int(float(row.get("inv-age-91-to-180-days", 0) or 0))
            qty_181_270 = int(float(row.get("inv-age-181-to-270-days", 0) or 0))
            qty_271_365 = int(float(row.get("inv-age-271-to-365-days", 0) or 0))
            qty_365_plus = int(float(row.get("inv-age-365-plus-days", 0) or 0))

            total = qty_0_90 + qty_91_180 + qty_181_270 + qty_271_365 + qty_365_plus

            # Estimate LTSF (Long-Term Storage Fee) — units aged 181+ days
            estimated_ltsf = round(
                (qty_181_270 + qty_271_365) * 0.50 + qty_365_plus * 6.90, 2
            )

            _div = _get_division_for_asin(asin, con)

            try:
                con.execute("""
                    INSERT OR REPLACE INTO fba_inventory_aging
                    (snapshot_date, asin, sku, product_name,
                     qty_0_90, qty_91_180, qty_181_270, qty_271_365, qty_365_plus,
                     total_qty, estimated_ltsf, division, customer, platform)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                """, [today, asin, sku, product_name[:200] if product_name else "",
                      qty_0_90, qty_91_180, qty_181_270, qty_271_365, qty_365_plus,
                      total, estimated_ltsf, _div])
                records += 1
            except Exception as e:
                logger.warning(f"  Aging insert error for {asin}: {e}")

        con.close()
        logger.info(f"  Aging report sync done: {records} ASINs")
    except Exception as e:
        logger.error(f"  Aging report sync error: {e}")
        import traceback
        logger.error(traceback.format_exc())


def _sync_stranded_report():
    """Pull GET_STRANDED_INVENTORY_UI_DATA report and store in fba_stranded_inventory."""
    try:
        from sp_api.api import Reports
        from sp_api.base import Marketplaces
    except ImportError:
        logger.warning("SP-API library not installed — skipping stranded sync")
        return

    credentials = _load_sp_api_credentials()
    if not credentials:
        return

    logger.info("  Requesting Stranded Inventory report...")
    import time, gzip, requests as req

    try:
        reports = Reports(credentials=credentials, marketplace=Marketplaces.US)

        report_response = reports.create_report(
            reportType="GET_STRANDED_INVENTORY_UI_DATA"
        )
        report_id = report_response.payload.get("reportId")

        report_data = None
        for attempt in range(20):
            time.sleep(10)
            status_response = reports.get_report(report_id)
            status = status_response.payload.get("processingStatus")
            if status == "DONE":
                doc_id = status_response.payload.get("reportDocumentId")
                doc_response = reports.get_report_document(doc_id)
                report_data = doc_response.payload
                break
            elif status in ("CANCELLED", "FATAL"):
                logger.error(f"  Stranded report failed: {status}")
                return

        if not report_data:
            logger.warning("  Stranded report timed out")
            return

        if isinstance(report_data, dict) and "url" in report_data:
            is_gzipped = report_data.get("compressionAlgorithm", "").upper() == "GZIP"
            resp = req.get(report_data["url"])
            if is_gzipped:
                import gzip as gz
                report_text = gz.decompress(resp.content).decode("utf-8")
            else:
                report_text = resp.text
        else:
            report_text = str(report_data)

        import csv as csv_mod
        reader = csv_mod.DictReader(report_text.splitlines(), delimiter='\t')

        con = get_db_rw()
        today = datetime.now(ZoneInfo("America/Chicago")).date()

        # Clear today's stranded data before re-inserting
        con.execute("DELETE FROM fba_stranded_inventory WHERE snapshot_date = ?", [today])

        records = 0
        for row in reader:
            asin = row.get("asin", "").strip()
            if not asin:
                continue

            sku = row.get("sku", "").strip()
            product_name = row.get("product-name", row.get("product_name", "")).strip()
            stranded_qty = int(float(row.get("Quantity in stranded", row.get("quantity-in-stranded", 0)) or 0))
            stranded_reason = row.get("Primary stranded reason", row.get("primary-stranded-reason", "")).strip()

            # Estimate value — use your-price if available
            your_price = float(row.get("your-price", row.get("Your price", 0)) or 0)
            estimated_value = round(your_price * stranded_qty, 2) if your_price > 0 else 0

            date_stranded = row.get("Date stranded", row.get("date-stranded", "")).strip()
            date_stranded = date_stranded[:10] if date_stranded else None

            _div = _get_division_for_asin(asin, con)

            try:
                con.execute("""
                    INSERT OR REPLACE INTO fba_stranded_inventory
                    (snapshot_date, asin, sku, product_name, stranded_qty,
                     stranded_reason, estimated_value, date_stranded,
                     division, customer, platform)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                """, [today, asin, sku, product_name[:200] if product_name else "",
                      stranded_qty, stranded_reason, estimated_value, date_stranded, _div])
                records += 1
            except Exception as e:
                logger.warning(f"  Stranded insert error for {asin}: {e}")

        con.close()
        logger.info(f"  Stranded report sync done: {records} items")
    except Exception as e:
        logger.error(f"  Stranded report sync error: {e}")


def _sync_reimbursements_report():
    """Pull GET_REIMBURSEMENTS_DATA report and store in fba_reimbursements."""
    try:
        from sp_api.api import Reports
        from sp_api.base import Marketplaces
    except ImportError:
        logger.warning("SP-API library not installed — skipping reimbursements sync")
        return

    credentials = _load_sp_api_credentials()
    if not credentials:
        return

    logger.info("  Requesting Reimbursements report...")
    import time, gzip, requests as req

    try:
        reports = Reports(credentials=credentials, marketplace=Marketplaces.US)

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=90)

        report_response = reports.create_report(
            reportType="GET_REIMBURSEMENTS_DATA",
            dataStartTime=start_date.isoformat(),
            dataEndTime=end_date.isoformat()
        )
        report_id = report_response.payload.get("reportId")

        report_data = None
        for attempt in range(20):
            time.sleep(10)
            status_response = reports.get_report(report_id)
            status = status_response.payload.get("processingStatus")
            if status == "DONE":
                doc_id = status_response.payload.get("reportDocumentId")
                doc_response = reports.get_report_document(doc_id)
                report_data = doc_response.payload
                break
            elif status in ("CANCELLED", "FATAL"):
                logger.error(f"  Reimbursements report failed: {status}")
                return

        if not report_data:
            logger.warning("  Reimbursements report timed out")
            return

        if isinstance(report_data, dict) and "url" in report_data:
            is_gzipped = report_data.get("compressionAlgorithm", "").upper() == "GZIP"
            resp = req.get(report_data["url"])
            if is_gzipped:
                import gzip as gz
                report_text = gz.decompress(resp.content).decode("utf-8")
            else:
                report_text = resp.text
        else:
            report_text = str(report_data)

        import csv as csv_mod
        reader = csv_mod.DictReader(report_text.splitlines(), delimiter='\t')

        con = get_db_rw()
        records = 0

        for row in reader:
            reimb_id = row.get("reimbursement-id", "").strip()
            if not reimb_id:
                continue

            asin = row.get("asin", "").strip()
            sku = row.get("sku", row.get("merchant-sku", "")).strip()
            product_name = row.get("product-name", "").strip()
            reason = row.get("reason", row.get("case-id", "")).strip()
            quantity = int(float(row.get("quantity-reimbursed-inventory", row.get("quantity", 0)) or 0))

            amount_str = row.get("amount-total", row.get("amount", "0"))
            try:
                amount = float(amount_str or 0)
            except (ValueError, TypeError):
                amount = 0.0

            currency = row.get("currency-unit", "USD").strip()

            reimb_date = row.get("approval-date", row.get("reimbursement-date", "")).strip()
            reimb_date = reimb_date[:10] if reimb_date and len(reimb_date) >= 10 else None

            _div = _get_division_for_asin(asin, con) if asin else "golf"

            try:
                con.execute("""
                    INSERT OR REPLACE INTO fba_reimbursements
                    (reimbursement_id, reimbursement_date, asin, sku, product_name,
                     reason, quantity, amount, currency,
                     division, customer, platform)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                """, [reimb_id, reimb_date, asin, sku, product_name[:200] if product_name else "",
                      reason, quantity, amount, currency, _div])
                records += 1
            except Exception as e:
                logger.warning(f"  Reimbursement insert error for {reimb_id}: {e}")

        con.close()
        logger.info(f"  Reimbursements sync done: {records} records")
    except Exception as e:
        logger.error(f"  Reimbursements sync error: {e}")


# ── LY Same-Time Sales — SP-API Sales.getOrderMetrics ─────────────────────────
_ly_same_time_cache: dict = {}
_LY_CACHE_TTL = 1800  # seconds (30 minutes — LY data is historical, never changes)


def get_ly_same_time_sales(ly_date, cutoff_dt) -> tuple:
    """Fetch LY sales through the same time-of-day as now via SP-API getOrderMetrics.

    Args:
        ly_date  : datetime.date — the LY equivalent date (364 days ago)
        cutoff_dt: datetime (tz-aware, Central) — current wall-clock time

    Returns:
        (total_sales: float, unit_count: int)
        Falls back to (0.0, 0) on any error.

    Cached per (ly_date, hour) for 5 minutes to avoid hammering the API.
    """
    import time as _t
    global _ly_same_time_cache

    cache_key = (str(ly_date), cutoff_dt.hour)
    now_ts = _t.time()
    if cache_key in _ly_same_time_cache:
        cached_val, cached_at = _ly_same_time_cache[cache_key]
        if now_ts - cached_at < _LY_CACHE_TTL:
            logger.debug(f"get_ly_same_time_sales: cache hit {cache_key}")
            return cached_val

    try:
        from sp_api.api import Sales as SalesAPI
        from sp_api.base import Marketplaces
        from sp_api.base.sales_enum import Granularity
    except ImportError:
        logger.warning("get_ly_same_time_sales: sp_api library not installed")
        return (0.0, 0)

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("get_ly_same_time_sales: no credentials")
        return (0.0, 0)

    try:
        CENTRAL = ZoneInfo("America/Chicago")
        ly_start = datetime(ly_date.year, ly_date.month, ly_date.day,
                            0, 0, 0, tzinfo=CENTRAL)
        # Ensure at least 1-minute interval so API doesn't reject zero-duration
        safe_cutoff = datetime(ly_date.year, ly_date.month, ly_date.day,
                               cutoff_dt.hour, max(cutoff_dt.minute, 1), 0,
                               tzinfo=CENTRAL)

        # SP-API expects interval as a tuple of two datetimes; library joins with "--"
        # granularity must be the Granularity enum, not a string
        @retry_with_backoff(max_retries=2, base_delay=1.0, max_delay=10.0)
        def _call_api():
            api = SalesAPI(credentials=credentials, marketplace=Marketplaces.US)
            return api.get_order_metrics(
                interval=(ly_start, safe_cutoff),
                granularity=Granularity.TOTAL,
            )

        response = _call_api()
        metrics = response.payload or []

        if not metrics:
            result = (0.0, 0, 0)
        else:
            m = metrics[0]
            ts = m.get('totalSales') or {}
            amount = float(ts.get('amount', 0) or 0) if isinstance(ts, dict) else float(ts or 0)
            units  = int(m.get('unitCount', 0) or 0)
            ords   = int(m.get('orderCount', 0) or 0)
            result = (round(amount, 2), units, ords)

        _ly_same_time_cache[cache_key] = (result, now_ts)
        logger.info(f"get_ly_same_time_sales: {ly_date} through {cutoff_dt.hour}:{cutoff_dt.minute:02d} CT → ${result[0]:,.2f} / {result[1]} units / {result[2]} orders")
        return result

    except Exception as e:
        logger.warning(f"get_ly_same_time_sales error: {e}")
        return (0.0, 0, 0)


# ── Hourly Sales — SP-API Sales.getOrderMetrics with HOUR granularity ────────
_hourly_sales_cache: dict = {}
_HOURLY_CACHE_TTL_TODAY = 300   # 5 min for today (data still flowing in)
_HOURLY_CACHE_TTL_PAST  = 86400 # 24 hr for past dates (data is final)


def get_hourly_sales(target_date) -> list:
    """Fetch hourly sales breakdown for a given date via SP-API getOrderMetrics.

    Returns a list of 24 dicts {hour, sales, units, orders}, indexed 0–23 in
    Central Time.  Hours with no sales have sales=0/units=0/orders=0.
    Falls back to [] on any error.

    Cached per date: 5 min for today, 24 hr for past dates.
    """
    import time as _t
    global _hourly_sales_cache

    date_str   = str(target_date)
    CENTRAL    = ZoneInfo("America/Chicago")
    today_ct   = datetime.now(CENTRAL).date()
    is_today   = (target_date == today_ct)

    now_ts = _t.time()
    if date_str in _hourly_sales_cache:
        cached_val, cached_at = _hourly_sales_cache[date_str]
        ttl = _HOURLY_CACHE_TTL_TODAY if is_today else _HOURLY_CACHE_TTL_PAST
        if now_ts - cached_at < ttl:
            return cached_val

    try:
        from sp_api.api import Sales as SalesAPI
        from sp_api.base import Marketplaces
        from sp_api.base.sales_enum import Granularity
    except ImportError:
        logger.warning("get_hourly_sales: sp_api library not installed")
        return []

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("get_hourly_sales: no credentials")
        return []

    try:
        day_start = datetime(target_date.year, target_date.month, target_date.day,
                             0, 0, 0, tzinfo=CENTRAL)
        if is_today:
            now_ct    = datetime.now(CENTRAL)
            day_end   = datetime(target_date.year, target_date.month, target_date.day,
                                 now_ct.hour, max(now_ct.minute, 1), 0, tzinfo=CENTRAL)
        else:
            day_end   = datetime(target_date.year, target_date.month, target_date.day,
                                 23, 59, 0, tzinfo=CENTRAL)

        @retry_with_backoff(max_retries=2, base_delay=1.0, max_delay=10.0)
        def _call():
            api = SalesAPI(credentials=credentials, marketplace=Marketplaces.US)
            return api.get_order_metrics(
                interval=(day_start, day_end),
                granularity=Granularity.HOUR,
            )

        response = _call()
        metrics  = response.payload or []

        # Build hour→data map; default all 24 hours to zero
        hour_map = {h: {'hour': h, 'sales': 0.0, 'units': 0, 'orders': 0}
                    for h in range(24)}

        for m in metrics:
            # SP-API returns interval as a string "2026-03-20T08:00:00Z--2026-03-20T09:00:00Z"
            # NOT a dict — parse the start portion directly from the string.
            interval  = m.get('interval') or ''
            start_str = ''
            if isinstance(interval, dict):
                start_str = interval.get('startTime', '') or interval.get('start', '')
            elif isinstance(interval, str):
                # Format: "startISO--endISO"  or "startISO/endISO"
                if '--' in interval:
                    start_str = interval.split('--')[0].strip()
                elif '/' in interval:
                    start_str = interval.split('/')[0].strip()
                else:
                    start_str = interval.strip()

            if start_str:
                try:
                    start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                    h_ct     = start_dt.astimezone(CENTRAL).hour
                except Exception:
                    h_ct = -1
            else:
                h_ct = -1

            if h_ct < 0 or h_ct > 23:
                continue

            ts     = m.get('totalSales') or {}
            # totalSales can be a dict with lowercase 'amount' or uppercase 'Amount'
            if isinstance(ts, dict):
                sales_ = float(ts.get('amount', 0) or ts.get('Amount', 0) or 0)
            else:
                sales_ = float(ts or 0)
            units_ = int(m.get('unitCount', 0) or 0)
            ords_  = int(m.get('orderCount', 0) or 0)
            hour_map[h_ct] = {'hour': h_ct, 'sales': round(sales_, 2),
                              'units': units_, 'orders': ords_}

        result = [hour_map[h] for h in range(24)]
        _hourly_sales_cache[date_str] = (result, now_ts)
        total = sum(r['sales'] for r in result)
        logger.info(f"get_hourly_sales({target_date}): {len(metrics)} hourly points, total=${total:,.2f}")
        return result

    except Exception as e:
        logger.warning(f"get_hourly_sales({target_date}) error: {e}")
        return []


_ly_full_day_cache: dict = {}
_LY_FULL_DAY_CACHE_TTL = 86400  # 24 hours (LY date is always a past date)


def get_ly_full_day_sales(ly_date) -> tuple:
    """Fetch LY full-day sales/units/orders for a date via SP-API getOrderMetrics (TOTAL).

    Used as a fallback when daily_sales table has no entry for the LY date.
    Returns (total_sales: float, unit_count: int, order_count: int).
    Cached for 24 hours.
    """
    import time as _t
    global _ly_full_day_cache

    date_str = str(ly_date)
    now_ts = _t.time()
    if date_str in _ly_full_day_cache:
        cached_val, cached_at = _ly_full_day_cache[date_str]
        if now_ts - cached_at < _LY_FULL_DAY_CACHE_TTL:
            logger.debug(f"get_ly_full_day_sales: cache hit {date_str}")
            return cached_val

    try:
        from sp_api.api import Sales as SalesAPI
        from sp_api.base import Marketplaces
        from sp_api.base.sales_enum import Granularity
    except ImportError:
        logger.warning("get_ly_full_day_sales: sp_api library not installed")
        return (0.0, 0, 0)

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("get_ly_full_day_sales: no credentials")
        return (0.0, 0, 0)

    try:
        CENTRAL = ZoneInfo("America/Chicago")
        day_start = datetime(ly_date.year, ly_date.month, ly_date.day,
                             0, 0, 0, tzinfo=CENTRAL)
        day_end   = datetime(ly_date.year, ly_date.month, ly_date.day,
                             23, 59, 0, tzinfo=CENTRAL)

        @retry_with_backoff(max_retries=2, base_delay=1.0, max_delay=10.0)
        def _call():
            api = SalesAPI(credentials=credentials, marketplace=Marketplaces.US)
            return api.get_order_metrics(
                interval=(day_start, day_end),
                granularity=Granularity.TOTAL,
            )

        response = _call()
        metrics  = response.payload or []

        if not metrics:
            result = (0.0, 0, 0)
        else:
            m      = metrics[0]
            ts     = m.get('totalSales') or {}
            sales_ = float(ts.get('amount', 0) or 0) if isinstance(ts, dict) else float(ts or 0)
            units_ = int(m.get('unitCount', 0) or 0)
            ords_  = int(m.get('orderCount', 0) or 0)
            result = (round(sales_, 2), units_, ords_)

        _ly_full_day_cache[date_str] = (result, now_ts)
        logger.info(f"get_ly_full_day_sales({ly_date}): ${result[0]:,.2f} / {result[1]} units / {result[2]} orders")
        return result

    except Exception as e:
        logger.warning(f"get_ly_full_day_sales error: {e}")
        return (0.0, 0, 0)


# ── TY same-time-of-day via getOrderMetrics ────────────────────────────────────
_ty_same_time_cache: dict = {}
_TY_CACHE_TTL = 300  # 5 minutes (today's data changes constantly)


def get_ty_same_time_sales(ty_date, cutoff_dt) -> tuple:
    """Fetch TY sales-so-far today via SP-API getOrderMetrics (TOTAL granularity).

    Matches what Amazon Seller Central "Today's Sales" shows — more reliable than
    summing the orders table which has a 50-order sync cap and estimates Pending prices.

    Args:
        ty_date   : datetime.date — today in Central time
        cutoff_dt : datetime (tz-aware, Central) — current wall-clock time

    Returns:
        (total_sales: float, unit_count: int, order_count: int)
        Falls back to (0.0, 0, 0) on any error.

    Cached per (ty_date, hour) for 5 minutes.
    """
    import time as _t
    global _ty_same_time_cache

    cache_key = (str(ty_date), cutoff_dt.hour, cutoff_dt.minute // 5)  # bucket per 5 min
    now_ts = _t.time()
    if cache_key in _ty_same_time_cache:
        cached_val, cached_at = _ty_same_time_cache[cache_key]
        if now_ts - cached_at < _TY_CACHE_TTL:
            logger.debug(f"get_ty_same_time_sales: cache hit {cache_key}")
            return cached_val

    try:
        from sp_api.api import Sales as SalesAPI
        from sp_api.base import Marketplaces
        from sp_api.base.sales_enum import Granularity
    except ImportError:
        logger.warning("get_ty_same_time_sales: sp_api library not installed")
        return (0.0, 0, 0)

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("get_ty_same_time_sales: no credentials")
        return (0.0, 0, 0)

    try:
        CENTRAL = ZoneInfo("America/Chicago")
        day_start  = datetime(ty_date.year, ty_date.month, ty_date.day,
                              0, 0, 0, tzinfo=CENTRAL)
        safe_cutoff = datetime(ty_date.year, ty_date.month, ty_date.day,
                               cutoff_dt.hour, max(cutoff_dt.minute, 1), 0,
                               tzinfo=CENTRAL)

        @retry_with_backoff(max_retries=2, base_delay=1.0, max_delay=10.0)
        def _call():
            api = SalesAPI(credentials=credentials, marketplace=Marketplaces.US)
            return api.get_order_metrics(
                interval=(day_start, safe_cutoff),
                granularity=Granularity.TOTAL,
            )

        response = _call()
        metrics  = response.payload or []

        if not metrics:
            result = (0.0, 0, 0)
        else:
            m      = metrics[0]
            ts     = m.get('totalSales') or {}
            amount = float(ts.get('amount', 0) or 0) if isinstance(ts, dict) else float(ts or 0)
            units  = int(m.get('unitCount', 0) or 0)
            ords   = int(m.get('orderCount', 0) or 0)
            result = (round(amount, 2), units, ords)

        _ty_same_time_cache[cache_key] = (result, now_ts)
        logger.info(f"get_ty_same_time_sales: {ty_date} through {cutoff_dt.hour}:{cutoff_dt.minute:02d} CT → ${result[0]:,.2f} / {result[1]} units / {result[2]} orders")
        return result

    except Exception as e:
        logger.warning(f"get_ty_same_time_sales error: {e}")
        return (0.0, 0, 0)


def _sync_fc_inventory():
    """Pull FC-level inventory using Inventories API with granularityType=FulfillmentCenter."""
    try:
        from sp_api.api import Inventories
        from sp_api.base import Marketplaces
    except ImportError:
        logger.warning("SP-API library not installed — skipping FC inventory sync")
        return

    credentials = _load_sp_api_credentials()
    if not credentials:
        return

    logger.info("  Pulling FC-level inventory...")

    try:
        inventory_api = Inventories(credentials=credentials, marketplace=Marketplaces.US)

        @retry_with_backoff(max_retries=3, base_delay=2.0, max_delay=30.0)
        def _fetch_fc_inventory():
            return inventory_api.get_inventory_summary_marketplace(
                marketplaceIds=["ATVPDKIKX0DER"],
                granularityType="Marketplace",
                granularityId="ATVPDKIKX0DER",
                details=True
            )

        response = _fetch_fc_inventory()
        summaries = response.payload.get("inventorySummaries", [])

        if not summaries:
            logger.warning("  FC inventory: no summaries returned")
            return

        con = get_db_rw()
        today = datetime.now(ZoneInfo("America/Chicago")).date()

        # Clear today's FC data before re-inserting
        con.execute("DELETE FROM fba_fc_inventory WHERE snapshot_date = ?", [today])

        records = 0
        # We get marketplace-level data; aggregate by FC if available in details
        # If FC-level isn't available via this endpoint, we synthesize from available data
        for item in summaries:
            asin = item.get("asin", "")
            sku = item.get("sellerSku", item.get("fnSku", ""))
            inv_details = item.get("inventoryDetails", {})
            fulfillable = int(inv_details.get("fulfillableQuantity", 0) or item.get("totalQuantity", 0))
            total = int(item.get("totalQuantity", 0))

            _div = _get_division_for_asin(asin, con)

            # Store as marketplace-level (we'll distribute across known FCs in the API layer)
            try:
                con.execute("""
                    INSERT OR REPLACE INTO fba_fc_inventory
                    (snapshot_date, fulfillment_center, asin, sku,
                     fulfillable_quantity, total_quantity,
                     division, customer, platform)
                    VALUES (?, 'ALL', ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                """, [today, asin, sku, fulfillable, total, _div])
                records += 1
            except Exception as e:
                logger.warning(f"  FC inventory insert error for {asin}: {e}")

        con.close()
        logger.info(f"  FC inventory sync done: {records} items")
    except Exception as e:
        logger.error(f"  FC inventory sync error: {e}")



def _fill_financial_events_gaps(months_back: int = 14):
    """Additive gap-fill for financial_events: finds completely empty calendar months
    in the last `months_back` months and fetches them from the SP-API without touching existing data.

    Unlike the main sync which deletes-then-reinserts, this function ONLY inserts into
    months that have zero existing rows, so it can never overwrite or lose existing data.
    Safe to run repeatedly.
    """
    import calendar

    logger.info("═══ FINANCIAL EVENTS GAP FILL STARTED ═══")

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("Gap fill: no SP-API credentials, skipping")
        return {"status": "SKIPPED", "reason": "No credentials"}

    try:
        from sp_api.api import Finances as FinancesAPI
        from sp_api.base import Marketplaces
    except ImportError:
        logger.warning("Gap fill: SP-API library not installed, skipping")
        return {"status": "SKIPPED", "reason": "SP-API not installed"}

    # ── Step 1: Detect completely empty months in the last 14 months ──────────
    today = datetime.utcnow().date()
    con_check = get_db()
    try:
        rows = con_check.execute("""
            SELECT TO_CHAR(date::date, 'YYYY-MM') as month
            FROM financial_events
            WHERE date >= CURRENT_DATE - INTERVAL '%(mb)s months'
            GROUP BY 1
        """ % {"mb": months_back}).fetchall()
        months_with_data = {r[0] for r in rows}
    except Exception as e:
        logger.error(f"Gap fill: could not query existing months: {e}")
        con_check.close()
        return {"status": "FAILED", "reason": str(e)}
    finally:
        con_check.close()

    # Build list of all months in lookback window
    all_months = []
    for i in range(months_back):
        # Work backwards from last month (don't gap-fill current month — it's still accumulating)
        yr = today.year
        mo = today.month - 1 - i
        while mo <= 0:
            mo += 12
            yr -= 1
        all_months.append(f"{yr:04d}-{mo:02d}")

    missing_months = sorted([m for m in all_months if m not in months_with_data])
    logger.info(f"Gap fill: months with data={sorted(months_with_data)}, missing={missing_months}")

    if not missing_months:
        logger.info("Gap fill: no missing months, nothing to do")
        return {"status": "SUCCESS", "missing_months": 0, "filled": 0}

    # ── Step 2: For each missing month, fetch and insert financial events ──────
    finances = FinancesAPI(credentials=credentials, marketplace=Marketplaces.US)
    total_inserted = 0
    months_filled = 0
    months_failed = 0

    for month_str in missing_months:
        yr, mo = int(month_str[:4]), int(month_str[5:7])
        last_day = calendar.monthrange(yr, mo)[1]
        month_start = f"{yr:04d}-{mo:02d}-01T00:00:00Z"
        month_end = f"{yr:04d}-{mo:02d}-{last_day:02d}T23:59:59Z"
        logger.info(f"Gap fill: fetching {month_str} ({month_start} to {month_end})")

        month_shipments = []
        month_refunds = []
        page = 0
        next_token = None

        try:
            while page < 50:  # 50 pages max per month (~5000 events)
                page += 1
                kwargs = {"PostedAfter": month_start, "PostedBefore": month_end, "MaxResultsPerPage": 100}
                if next_token:
                    kwargs = {"NextToken": next_token, "MaxResultsPerPage": 100}

                @retry_with_backoff(max_retries=3, base_delay=2.0, max_delay=30.0)
                def _fetch_page(**kw):
                    return finances.list_financial_events(**kw)

                resp = _fetch_page(**kwargs)
                payload = resp.payload if hasattr(resp, 'payload') else {}
                events = payload.get("FinancialEvents", {})
                shipments = events.get("ShipmentEventList", []) or []
                refunds = events.get("RefundEventList", []) or []
                month_shipments.extend(shipments)
                month_refunds.extend(refunds)
                logger.info(f"  {month_str} page {page}: {len(shipments)} shipments, {len(refunds)} refunds")

                next_token = payload.get("NextToken")
                if not next_token:
                    break

            logger.info(f"  {month_str}: {len(month_shipments)} total shipments, {len(month_refunds)} refunds")

            if not month_shipments and not month_refunds:
                logger.info(f"  {month_str}: no events from API — month may be genuinely empty")
                months_filled += 1  # count as "checked"
                continue

            # Insert additively — no DELETE since month is empty
            con = get_db_rw()
            con.execute("BEGIN TRANSACTION")
            inserted = 0

            def _money_gf(amt_obj):
                if amt_obj is None:
                    return 0.0
                if isinstance(amt_obj, dict):
                    for key in ("CurrencyAmount", "Amount", "currency_amount", "amount"):
                        v = amt_obj.get(key)
                        if v is not None:
                            try:
                                return float(v)
                            except (ValueError, TypeError):
                                continue
                try:
                    return float(amt_obj)
                except Exception:
                    return 0.0

            def _get_str(obj, key, default=""):
                if isinstance(obj, dict):
                    return obj.get(key, default) or default
                return getattr(obj, key, default) or default

            def _pd_str(pd_val):
                if pd_val is None:
                    return None
                if isinstance(pd_val, str):
                    return pd_val[:10]
                if hasattr(pd_val, 'strftime'):
                    return pd_val.strftime("%Y-%m-%d")
                return str(pd_val)[:10]

            for event in month_shipments:
                order_id = _get_str(event, "AmazonOrderId")
                date_str = _pd_str(_get_str(event, "PostedDate") or None)
                for item in ((_get_str(event, "ShipmentItemList") if isinstance(event, dict) else getattr(event, "ShipmentItemList", [])) or []):
                    sku = _get_str(item, "SellerSKU")
                    asin_val = _get_str(item, "ASIN") or sku

                    product_charges = shipping_ch = fba_val = comm_val = promo_val = other_val = 0.0
                    for c in (item.get("ItemChargeList", []) if isinstance(item, dict) else []):
                        val = _money_gf(c.get("ChargeAmount"))
                        ct = c.get("ChargeType", "")
                        if ct == "Principal":
                            product_charges += val
                        elif ct in ("ShippingCharge", "Shipping"):
                            shipping_ch += val
                    for f in (item.get("ItemFeeList", []) if isinstance(item, dict) else []):
                        val = abs(_money_gf(f.get("FeeAmount")))
                        ft = f.get("FeeType", "")
                        if "FBA" in ft or "Fulfillment" in ft:
                            fba_val += val
                        elif ft in ("Commission", "ReferralFee"):
                            comm_val += val
                        else:
                            other_val += val
                    for p in (item.get("PromotionList", []) if isinstance(item, dict) else []):
                        promo_val += _money_gf(p.get("PromotionAmount"))

                    _div = _get_division_for_asin(asin_val)
                    try:
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                        """, [date_str, asin_val, sku, order_id, "Shipment",
                              product_charges, shipping_ch, fba_val, comm_val, promo_val, other_val,
                              product_charges + shipping_ch - fba_val - comm_val + promo_val, _div])
                        inserted += 1
                    except Exception as ie:
                        logger.warning(f"  Gap fill insert error: {ie}")

            for event in month_refunds:
                order_id = _get_str(event, "AmazonOrderId")
                date_str = _pd_str(_get_str(event, "PostedDate") or None)
                item_list = (event.get("ShipmentItemAdjustmentList") or event.get("ShipmentItemList") or [] if isinstance(event, dict) else [])
                for item in item_list:
                    sku = _get_str(item, "SellerSKU")
                    asin_val = _get_str(item, "ASIN") or sku
                    refund_amt = 0.0
                    for c in (item.get("ItemChargeAdjustmentList") or item.get("ItemChargeList") or [] if isinstance(item, dict) else []):
                        val = _money_gf(c.get("ChargeAmount"))
                        if c.get("ChargeType", "") == "Principal":
                            refund_amt += val
                    _div = _get_division_for_asin(asin_val)
                    try:
                        con.execute("""
                            INSERT INTO financial_events
                            (date, asin, sku, order_id, event_type,
                             product_charges, shipping_charges, fba_fees,
                             commission, promotion_amount, other_fees, net_proceeds,
                             division, customer, platform)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                        """, [date_str, asin_val, sku, order_id, "Refund",
                              abs(refund_amt), 0, 0, 0, 0, 0, abs(refund_amt), _div])
                        inserted += 1
                    except Exception as ie:
                        logger.warning(f"  Gap fill refund insert error: {ie}")

            con.execute("COMMIT")
            con.close()
            total_inserted += inserted
            months_filled += 1
            logger.info(f"  Gap fill {month_str}: inserted {inserted} events")

        except Exception as e:
            logger.error(f"Gap fill {month_str} error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            months_failed += 1
            try:
                con.execute("ROLLBACK")
                con.close()
            except Exception:
                pass
            import time as _t
            _t.sleep(5)

    status = "SUCCESS" if months_failed == 0 else ("PARTIAL" if months_filled > 0 else "FAILED")
    logger.info(f"═══ GAP FILL {status}: {months_filled} months filled, {total_inserted} events inserted, {months_failed} failed ═══")
    return {"status": status, "months_filled": months_filled, "inserted": total_inserted, "failed": months_failed}
