"""
GolfGen Dashboard API — FastAPI backend serving Amazon SP-API data from DuckDB.
Includes background data sync from Amazon SP-API (runs every 2 hours on Railway).
"""

import os
import csv
import json
import asyncio
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

import duckdb
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

logger = logging.getLogger("golfgen")

# ── Paths ───────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # GolfGen Amazon Dashboard/
DB_DIR = Path(os.environ.get("DB_DIR", str(BASE_DIR / "data")))
DB_PATH = DB_DIR / "golfgen_amazon.duckdb"
COGS_PATH = DB_DIR / "cogs.csv"
CONFIG_PATH = BASE_DIR / "config" / "credentials.json"

# ── Background SP-API Sync ──────────────────────────────
SYNC_INTERVAL_HOURS = 2


def _load_sp_api_credentials() -> dict | None:
    """Load SP-API credentials from env vars (Railway) or config file (local dev)."""
    # Priority 1: Environment variables (used on Railway)
    env_refresh = os.environ.get("SP_API_REFRESH_TOKEN", "")
    if env_refresh:
        return {
            "refresh_token": env_refresh,
            "lwa_app_id": os.environ.get("SP_API_LWA_APP_ID", ""),
            "lwa_client_secret": os.environ.get("SP_API_LWA_CLIENT_SECRET", ""),
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


def _sync_today_orders():
    """Fast sync: pull today's orders from Amazon Orders API.

    This is the critical path for showing live "Today" data.
    Runs at startup, on /api/sync, and inline if today has no data.

    Strategy: Use getOrderItems for EVERY order to get accurate item-level
    prices.  OrderTotal is unreliable (missing for Pending, sometimes $0
    for FBA orders).  Item-level prices are the ground truth.
    """
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
        # Get orders from last 2 days to catch any we missed
        after_date = (datetime.utcnow() - timedelta(days=2)).isoformat()
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

        logger.info(f"  Got {len(order_list)} orders from last 2 days")

        con = duckdb.connect(str(DB_PATH), read_only=False)

        # Store raw orders
        for order in order_list:
            order_total = order.get("OrderTotal", {})
            con.execute("""
                INSERT OR REPLACE INTO orders
                (order_id, purchase_date, order_status, fulfillment_channel,
                 sales_channel, order_total, currency_code, number_of_items,
                 ship_city, ship_state, ship_postal_code,
                 is_business_order, is_prime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        today_str = (datetime.utcnow() - timedelta(hours=7)).strftime("%Y-%m-%d")
        yesterday_str = (datetime.utcnow() - timedelta(hours=7) - timedelta(days=1)).strftime("%Y-%m-%d")

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
                    dt = dt - timedelta(hours=7)
                    order_day = dt.strftime("%Y-%m-%d")
                else:
                    order_day = purchase_date[:10]
            except Exception:
                order_day = purchase_date[:10]

            if order_day not in (today_str, yesterday_str):
                continue

            order_id = order.get("AmazonOrderId")
            if not order_id:
                continue

            try:
                items_resp = orders_api.get_order_items(order_id)
                item_list = items_resp.payload.get("OrderItems", [])
                total_from_items = 0.0
                total_qty = 0
                for item in item_list:
                    # ItemPrice is the total for this line (price × qty)
                    price = item.get("ItemPrice")
                    if price and isinstance(price, dict):
                        amt_str = price.get("Amount", "0")
                        try:
                            total_from_items += float(amt_str)
                        except (ValueError, TypeError):
                            pass
                    qty = 0
                    try:
                        qty = int(item.get("QuantityOrdered", 0))
                    except (ValueError, TypeError):
                        pass
                    total_qty += qty

                # Always use item-level total (more accurate than OrderTotal)
                order["_item_revenue"] = total_from_items
                order["_item_qty"] = total_qty
                items_fetched += 1

                # Adaptive rate limiting: burst first 25, then sleep
                burst_count += 1
                if burst_count > 25:
                    _t.sleep(2.0)  # Stay well under rate limit
            except Exception as e:
                items_failed += 1
                logger.warning(f"  getOrderItems failed for {order_id}: {e}")
                # On rate limit, back off
                if "throttl" in str(e).lower() or "429" in str(e):
                    _t.sleep(5.0)

        logger.info(f"  Fetched items for {items_fetched} orders ({items_failed} failed)")

        # ── Aggregate by date ──
        from collections import defaultdict
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
                    dt = dt - timedelta(hours=7)
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
        for day_str in [today_str, yesterday_str]:
            if day_str not in daily_agg:
                continue
            agg = daily_agg[day_str]
            existing = con.execute(
                "SELECT ordered_product_sales FROM daily_sales WHERE date = ? AND asin = 'ALL'",
                [day_str]
            ).fetchone()
            existing_rev = float(existing[0]) if existing and existing[0] else 0.0

            # For today: always use order data (Sales Report won't have it)
            # For yesterday: only if order data is higher (report may have caught up)
            if day_str == today_str or agg["revenue"] > existing_rev:
                con.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, asin, units_ordered, ordered_product_sales,
                     sessions, session_percentage, page_views,
                     buy_box_percentage, unit_session_percentage)
                    VALUES (?, 'ALL', ?, ?, 0, 0, 0, 0, 0)
                """, [day_str, agg["units"], agg["revenue"]])
                logger.info(f"  {day_str}: ${agg['revenue']:.2f} rev, {agg['units']} units, {agg['orders']} orders")

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
        return False


# Track last today-sync so we don't hammer the API
_last_today_sync = 0.0


def _ensure_today_data():
    """If today has no data in daily_sales, do a quick Orders API pull.

    Called inline by profitability/comparison endpoints so the user
    never sees Today = $0 when Amazon has live data.
    Throttled to at most once per 5 minutes.
    """
    global _last_today_sync
    import time as _time
    now = _time.time()
    if now - _last_today_sync < 300:          # 5-minute cooldown
        return

    today_str = (datetime.utcnow() - timedelta(hours=7)).strftime("%Y-%m-%d")
    try:
        con = duckdb.connect(str(DB_PATH), read_only=False)
        row = con.execute(
            "SELECT ordered_product_sales FROM daily_sales WHERE date = ? AND asin = 'ALL'",
            [today_str]
        ).fetchone()
        con.close()
        has_data = row and row[0] and float(row[0]) > 0
    except Exception:
        has_data = False

    if not has_data:
        logger.info("Today has no data — triggering quick Orders API sync")
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
                resp = finances.list_financial_events(**kwargs)
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

            shipments = events.get("ShipmentEventList", [])
            refunds = events.get("RefundEventList", [])
            all_shipment_events.extend(shipments)
            all_refund_events.extend(refunds)
            logger.info(f"  Financial events page {page}: {len(shipments)} shipments, {len(refunds)} refunds")

            # Check for next page
            next_token = payload.get("NextToken")
            if not next_token:
                break

        logger.info(f"  Total financial events: {len(all_shipment_events)} shipments, {len(all_refund_events)} refunds")
        con = duckdb.connect(str(DB_PATH), read_only=False)
        fin_records = 0

        # Helper: Amazon uses CurrencyAmount (not Amount) in money objects
        def _money(amt_obj):
            if not isinstance(amt_obj, dict):
                return 0.0
            return float(amt_obj.get("CurrencyAmount", amt_obj.get("Amount", 0)) or 0)

        # Process shipment events (sales fees)
        for event in all_shipment_events:
            order_id = event.get("AmazonOrderId", "")
            posted_date = event.get("PostedDate", "")

            for item in event.get("ShipmentItemList", []):
                sku = item.get("SellerSKU", "")
                asin_val = item.get("ASIN", sku)

                product_charges = 0.0
                shipping_ch = 0.0
                for c in item.get("ItemChargeList", []):
                    val = _money(c.get("ChargeAmount", {}))
                    ct = c.get("ChargeType", "")
                    if ct == "Principal":
                        product_charges += val
                    elif ct in ("ShippingCharge", "Shipping"):
                        shipping_ch += val

                fba_fees_val = 0.0
                commission_val = 0.0
                other_val = 0.0
                for f in item.get("ItemFeeList", []):
                    val = abs(_money(f.get("FeeAmount", {})))
                    ft = f.get("FeeType", "")
                    if "FBA" in ft or "Fulfillment" in ft:
                        fba_fees_val += val
                    elif ft in ("Commission", "ReferralFee"):
                        commission_val += val
                    else:
                        other_val += val

                promo_val = 0.0
                for p in item.get("PromotionList", []):
                    promo_val += _money(p.get("PromotionAmount", {}))

                net = product_charges + shipping_ch - fba_fees_val - commission_val + promo_val

                con.execute("""
                    INSERT OR REPLACE INTO financial_events
                    (date, asin, sku, order_id, event_type,
                     product_charges, shipping_charges, fba_fees,
                     commission, promotion_amount, other_fees, net_proceeds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    posted_date[:10] if posted_date else None,
                    asin_val, sku, order_id, "Shipment",
                    product_charges, shipping_ch,
                    fba_fees_val, commission_val, promo_val, other_val, net
                ])
                fin_records += 1

        # Process refund events
        for event in all_refund_events:
            order_id = event.get("AmazonOrderId", "")
            posted_date = event.get("PostedDate", "")

            for item in event.get("ShipmentItemAdjustmentList", event.get("ShipmentItemList", [])):
                sku = item.get("SellerSKU", "")
                asin_val = item.get("ASIN", sku)

                refund_amount = 0.0
                for c in item.get("ItemChargeAdjustmentList", item.get("ItemChargeList", [])):
                    refund_amount += _money(c.get("ChargeAmount", {}))

                con.execute("""
                    INSERT OR REPLACE INTO financial_events
                    (date, asin, sku, order_id, event_type,
                     product_charges, shipping_charges, fba_fees,
                     commission, promotion_amount, other_fees, net_proceeds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    posted_date[:10] if posted_date else None,
                    asin_val, sku, order_id, "Refund",
                    refund_amount, 0, 0, 0, 0, 0, refund_amount
                ])
                fin_records += 1

        con.close()
        logger.info(f"  Financial events sync done: {fin_records} records")
    except Exception as e:
        logger.error(f"  Financial events sync error: {e}")

    # ── 3. FBA INVENTORY ────────────────────────────────────────
    try:
        logger.info("  Pulling FBA inventory...")
        inventory_api = Inventories(credentials=credentials, marketplace=Marketplaces.US)
        response = inventory_api.get_inventory_summary_marketplace(
            marketplaceIds=["ATVPDKIKX0DER"],
            granularityType="Marketplace",
            granularityId="ATVPDKIKX0DER"
        )
        summaries = response.payload.get("inventorySummaries", [])
        if summaries:
            con = duckdb.connect(str(DB_PATH), read_only=False)
            today = datetime.now().date()
            for item in summaries:
                inv = item.get("inventoryDetails", {})
                fulfillable = inv.get("fulfillableQuantity", 0) or item.get("totalQuantity", 0)
                reserved = inv.get("reservedQuantity", {})
                reserved_qty = reserved.get("totalReservedQuantity", 0) if isinstance(reserved, dict) else int(reserved or 0)
                con.execute("""
                    INSERT OR REPLACE INTO fba_inventory
                    (date, asin, sku, product_name, condition,
                     fulfillable_quantity, inbound_working_quantity,
                     inbound_shipped_quantity, inbound_receiving_quantity,
                     reserved_quantity, unfulfillable_quantity, total_quantity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    today, item.get("asin", ""),
                    item.get("sellerSku", item.get("fnSku", "")),
                    item.get("productName", "")[:200],
                    item.get("condition", "NewItem"),
                    int(fulfillable or 0),
                    int(inv.get("inboundWorkingQuantity", 0) or 0),
                    int(inv.get("inboundShippedQuantity", 0) or 0),
                    int(inv.get("inboundReceivingQuantity", 0) or 0),
                    int(reserved_qty or 0), 0,
                    int(item.get("totalQuantity", 0)),
                ])
            con.close()
            logger.info(f"  Inventory sync done: {len(summaries)} items")
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

            con = duckdb.connect(str(DB_PATH), read_only=False)
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
                     buy_box_percentage, unit_session_percentage)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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

                con.execute("""
                    INSERT OR REPLACE INTO daily_sales
                    (date, asin, units_ordered, ordered_product_sales,
                     sessions, session_percentage, page_views,
                     buy_box_percentage, unit_session_percentage)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    entry_date, asin,
                    int(sales_info.get("unitsOrdered", 0)),
                    sales_amount,
                    int(traffic.get("sessions", 0)),
                    float(traffic.get("sessionPercentage", 0)),
                    int(traffic.get("pageViews", 0)),
                    float(traffic.get("buyBoxPercentage", 0)),
                    float(traffic.get("unitSessionPercentage", 0)),
                ])
                records += 1

            con.close()
            logger.info(f"  Sales sync done: {records} records stored")

    except Exception as e:
        logger.error(f"  Sales sync error: {e}")

    logger.info("SP-API background sync complete.")


async def _sync_loop():
    """Run SP-API sync in background.

    Sequence:
    1. Immediately sync today's orders (fast, ~5s) — so "Today" works right away
    2. Wait 10s, then run full sync (slow, includes Sales Report polling)
    3. Repeat full sync every SYNC_INTERVAL_HOURS
    """
    # Immediately pull today's orders — no delay
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_today_orders)
        logger.info("Startup: today's orders synced")
    except Exception as e:
        logger.error(f"Startup today-sync error: {e}")

    # Short delay before full sync (let server finish starting)
    await asyncio.sleep(10)
    while True:
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _run_sp_api_sync)
        except Exception as e:
            logger.error(f"Sync loop error: {e}")
        await asyncio.sleep(SYNC_INTERVAL_HOURS * 3600)


@asynccontextmanager
async def lifespan(app):
    """Start background sync on app startup."""
    task = asyncio.create_task(_sync_loop())
    logger.info("Background SP-API sync scheduled (today orders → full sync → every 2 hours)")
    yield
    task.cancel()


app = FastAPI(title="GolfGen Dashboard API", version="1.0.0", lifespan=lifespan)

# Allow frontend (dev on :5173, prod wherever)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ─────────────────────────────────────────────

def get_db():
    """Return a read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def load_cogs() -> dict:
    """Load COGS data from CSV. Returns {asin: {cogs, product_name, sku}}."""
    cogs = {}
    if not COGS_PATH.exists():
        return cogs
    with open(COGS_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            asin = (row.get("asin") or "").strip()
            if not asin:
                continue
            try:
                cogs_val = float(row.get("cogs") or 0)
            except ValueError:
                cogs_val = 0
            cogs[asin] = {
                "cogs": cogs_val,
                "product_name": (row.get("product_name") or "").strip(),
                "sku": (row.get("sku") or "").strip(),
            }
    return cogs


def fmt_date(d) -> str:
    """Safely format a date value to string."""
    if isinstance(d, str):
        return d
    if hasattr(d, "strftime"):
        return d.strftime("%Y-%m-%d")
    return str(d)


def get_today(con) -> datetime:
    """Return today's actual calendar date in US-Pacific timezone.

    Railway servers run in UTC.  The business operates in Pacific time,
    so we subtract 7 hours (PDT) from UTC to get the real calendar date.
    'Today' always means today — if there's no data yet it shows $0,
    which is correct until Orders API sync populates it.
    """
    now_utc = datetime.utcnow()
    now_pacific = now_utc - timedelta(hours=7)          # PDT (UTC-7)
    return now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)


# ── API Routes ──────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "db": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
        "port": os.environ.get("PORT", "8000"),
        "has_sp_api_creds": _load_sp_api_credentials() is not None,
    }


@app.post("/api/sync")
async def trigger_sync():
    """Manually trigger an SP-API data sync."""
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_sp_api_sync)
    return {"status": "sync_complete"}


@app.get("/api/sync")
async def trigger_sync_get():
    """Manually trigger sync. Fast: returns after today's orders are synced.
    Full sync (Sales Report) continues in background."""
    import asyncio
    loop = asyncio.get_event_loop()
    # Fast: sync today's orders first (returns in ~5s)
    await loop.run_in_executor(None, _sync_today_orders)
    # Kick off full sync in background (don't wait for 5-min report poll)
    asyncio.get_event_loop().run_in_executor(None, _run_sp_api_sync)
    return {"status": "sync_complete", "today_synced": True}


@app.get("/api/summary")
def summary(days: int = Query(365, description="Number of days to include")):
    """High-level KPIs: revenue, units, orders, sessions, AUR, conv rate."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    row = con.execute("""
        SELECT
            COALESCE(SUM(ordered_product_sales), 0) AS revenue,
            COALESCE(SUM(units_ordered), 0) AS units,
            COALESCE(SUM(sessions), 0) AS sessions
        FROM daily_sales
        WHERE date >= ? AND asin = 'ALL'
    """, [cutoff]).fetchone()

    revenue, units, sessions = row
    orders = units  # total_order_items is not populated; use units as proxy
    aur = round(revenue / units, 2) if units else 0
    conv_rate = round(units / sessions * 100, 1) if sessions else 0

    # Net profit: estimate from known cost ratios in per-ASIN data
    products = _build_product_list(con, cutoff)
    prod_rev = sum(p["rev"] for p in products)
    prod_net = sum(p["net"] for p in products)

    # Scale product P&L to actual revenue if per-ASIN data is incomplete
    if prod_rev > 0 and revenue > 0:
        margin_pct = prod_net / prod_rev  # known margin from per-ASIN data
        total_net = round(revenue * margin_pct, 2)
        margin = round(margin_pct * 100)
    else:
        total_net = 0
        margin = 0

    con.close()
    return {
        "days": days,
        "revenue": round(revenue, 2),
        "units": units,
        "orders": orders,
        "sessions": sessions,
        "aur": aur,
        "convRate": conv_rate,
        "netProfit": total_net,
        "margin": margin,
    }


@app.get("/api/daily")
def daily_sales(days: int = Query(365), granularity: str = Query("daily")):
    """Time-series sales data for charts. Granularity: daily or weekly."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = con.execute("""
        SELECT
            date,
            COALESCE(ordered_product_sales, 0) AS revenue,
            COALESCE(units_ordered, 0) AS units,
            COALESCE(sessions, 0) AS sessions
        FROM daily_sales
        WHERE date >= ? AND asin = 'ALL'
        ORDER BY date
    """, [cutoff]).fetchall()
    con.close()

    data = []
    for r in rows:
        revenue_val = r[1]
        units_val = r[2] or 0
        sessions_val = r[3] or 0
        data.append({
            "date": fmt_date(r[0]),
            "revenue": round(revenue_val, 2),
            "units": units_val,
            "orders": units_val,  # total_order_items not populated; use units
            "sessions": sessions_val,
            "aur": round(revenue_val / units_val, 2) if units_val else 0,
            "convRate": round(units_val / sessions_val * 100, 1) if sessions_val else 0,
        })

    if granularity == "weekly" and data:
        data = _aggregate_weekly(data)

    return {"granularity": granularity, "days": days, "data": data}


@app.get("/api/products")
def products(days: int = Query(365)):
    """Product breakdown with profitability metrics."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    product_list = _build_product_list(con, cutoff)
    con.close()
    return {"days": days, "products": product_list}


@app.get("/api/inventory")
def inventory():
    """Current FBA inventory with days-of-supply calculations."""
    con = get_db()
    cogs_data = load_cogs()

    inv_rows = con.execute("""
        SELECT asin, sku, product_name,
               COALESCE(fulfillable_quantity, 0) AS fba_stock,
               COALESCE(inbound_working_quantity, 0) + COALESCE(inbound_shipped_quantity, 0) + COALESCE(inbound_receiving_quantity, 0) AS inbound,
               COALESCE(reserved_quantity, 0) AS reserved
        FROM fba_inventory
    """).fetchall()

    # Get avg daily units for last 30 days
    velocity = {}
    vel_rows = con.execute("""
        SELECT asin, SUM(units_ordered) AS units
        FROM daily_sales
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
          AND asin != 'ALL'
        GROUP BY asin
    """).fetchall()
    for vr in vel_rows:
        velocity[vr[0]] = round(vr[1] / 30, 1)

    con.close()

    items = []
    for r in inv_rows:
        asin = r[0]
        avg_daily = velocity.get(asin, 0)
        fba_stock = r[3]
        dos = round(fba_stock / avg_daily) if avg_daily > 0 else 999

        # Get name from COGS file first, then inventory table
        cogs_name = cogs_data.get(asin, {}).get("product_name", "")
        # Exclude names that are just the ASIN repeated
        if cogs_name and cogs_name.strip().upper() == asin.upper():
            cogs_name = ""
        name = (cogs_name or r[2] or asin)

        items.append({
            "asin": asin,
            "sku": r[1] or "",
            "name": name,
            "fbaStock": fba_stock,
            "inbound": r[4],
            "reserved": r[5],
            "avgDaily": avg_daily,
            "dos": dos,
        })

    items.sort(key=lambda x: x["fbaStock"], reverse=True)
    return {"items": items}


@app.get("/api/product/{asin}")
def product_detail(asin: str, days: int = Query(365)):
    """Detailed view for a single product including daily trend."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    cogs_data = load_cogs()

    # Daily trend for this ASIN
    rows = con.execute("""
        SELECT date,
               COALESCE(ordered_product_sales, 0) AS revenue,
               COALESCE(units_ordered, 0) AS units,
               COALESCE(sessions, 0) AS sessions
        FROM daily_sales
        WHERE date >= ? AND asin = ?
        ORDER BY date
    """, [cutoff, asin]).fetchall()

    trend = [{"date": fmt_date(r[0]), "revenue": round(r[1], 2), "units": r[2], "sessions": r[3]} for r in rows]

    # Inventory
    inv = con.execute("""
        SELECT COALESCE(fulfillable_quantity, 0),
               COALESCE(inbound_working_quantity, 0) + COALESCE(inbound_shipped_quantity, 0) + COALESCE(inbound_receiving_quantity, 0),
               COALESCE(reserved_quantity, 0)
        FROM fba_inventory WHERE asin = ?
    """, [asin]).fetchone()

    con.close()

    cogs_info = cogs_data.get(asin, {})
    total_rev = sum(r["revenue"] for r in trend)
    total_units = sum(r["units"] for r in trend)

    return {
        "asin": asin,
        "name": cogs_info.get("product_name", asin),
        "sku": cogs_info.get("sku", ""),
        "cogs": cogs_info.get("cogs", 0),
        "totalRevenue": round(total_rev, 2),
        "totalUnits": total_units,
        "trend": trend,
        "inventory": {
            "fbaStock": inv[0] if inv else 0,
            "inbound": inv[1] if inv else 0,
            "reserved": inv[2] if inv else 0,
        },
    }


@app.get("/api/pnl")
def pnl_waterfall(days: int = Query(365)):
    """Profit & Loss waterfall data, scaled to actual revenue from ALL rows."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Actual revenue from 'ALL' daily rows
    actual_rev = con.execute("""
        SELECT COALESCE(SUM(ordered_product_sales), 0)
        FROM daily_sales WHERE date >= ? AND asin = 'ALL'
    """, [cutoff]).fetchone()[0]

    # Cost breakdown from per-ASIN data
    products = _build_product_list(con, cutoff)
    con.close()

    prod_rev = sum(p["rev"] for p in products)
    prod_cogs = sum(p["cogsTotal"] for p in products)
    prod_fba = sum(p["fbaTotal"] for p in products)
    prod_referral = sum(p["referralTotal"] for p in products)
    prod_ad = sum(p["adSpend"] for p in products)
    prod_net = sum(p["net"] for p in products)

    # Scale cost ratios to actual revenue
    scale = actual_rev / prod_rev if prod_rev > 0 else 1
    cogs = round(prod_cogs * scale, 2)
    fba = round(prod_fba * scale, 2)
    referral = round(prod_referral * scale, 2)
    ad_spend = round(prod_ad * scale, 2)
    net = round(actual_rev - cogs - fba - referral - ad_spend, 2)

    return {
        "days": days,
        "revenue": round(actual_rev, 2),
        "cogs": cogs,
        "fbaFees": fba,
        "referralFees": referral,
        "adSpend": ad_spend,
        "netProfit": net,
    }


# ── Internal helpers ────────────────────────────────────

def _build_product_list(con, cutoff: str) -> list:
    """Build product-level P&L. Core business logic."""
    cogs_data = load_cogs()

    # Get product names/SKUs from fba_inventory as fallback
    inv_names = {}
    try:
        inv_rows = con.execute(
            "SELECT asin, sku, product_name FROM fba_inventory"
        ).fetchall()
        for ir in inv_rows:
            inv_names[ir[0]] = {"sku": ir[1] or "", "product_name": ir[2] or ""}
    except Exception:
        pass

    # Product-level sales — use per-ASIN data (excluding aggregate 'ALL' row)
    rows = con.execute("""
        SELECT asin,
               MAX(sku) AS sku,
               MAX(product_name) AS product_name,
               COALESCE(SUM(ordered_product_sales), 0) AS revenue,
               COALESCE(SUM(units_ordered), 0) AS units,
               COALESCE(SUM(sessions), 0) AS sessions,
               COALESCE(SUM(total_order_items), 0) AS orders
        FROM daily_sales
        WHERE asin != 'ALL'
        GROUP BY asin
        ORDER BY SUM(ordered_product_sales) DESC
    """).fetchall()

    # Financial data (actual fees if available)
    fin_rows = con.execute("""
        SELECT asin,
               SUM(ABS(fba_fees)) AS fba_fees,
               SUM(ABS(commission)) AS commission
        FROM financial_events
        GROUP BY asin
    """).fetchall()
    fin_by_asin = {r[0]: {"fba_fees": r[1], "commission": r[2]} for r in fin_rows}

    products = []
    for r in rows:
        asin, sku, api_name, revenue, units, sessions, orders = r
        if units == 0:
            continue

        # Use units as proxy for orders if total_order_items is empty
        if not orders:
            orders = units

        aur = round(revenue / units, 2)

        # COGS: from file, by ASIN or SKU
        cogs_info = cogs_data.get(asin) or cogs_data.get(sku or "") or {}
        cogs_per_unit = cogs_info.get("cogs", 0)
        if cogs_per_unit == 0:
            cogs_per_unit = round(aur * 0.35, 2)  # estimate

        # Name: COGS file > inventory table > API > ASIN
        inv_info = inv_names.get(asin, {})
        cogs_name = cogs_info.get("product_name", "")
        # Exclude names that are just the ASIN repeated
        if cogs_name and cogs_name.strip().upper() == asin.upper():
            cogs_name = ""
        name = (cogs_name
                or api_name
                or inv_info.get("product_name")
                or asin)

        # SKU: COGS file > daily_sales > inventory table
        resolved_sku = (sku
                        or cogs_info.get("sku", "")
                        or inv_info.get("sku", ""))

        # FBA fees
        fin = fin_by_asin.get(asin, {})
        actual_fba = fin.get("fba_fees", 0)
        # Better estimate for golf equipment (oversized/bulky items):
        # Amazon FBA fees are typically 10-15% of selling price for large items
        # Old formula ($5 + 3% of AUR) was WAY too low
        est_fba_total = round(revenue * 0.12, 2)  # ~12% of revenue for oversized
        fba_total = actual_fba if actual_fba > 0 else est_fba_total

        # Referral fees (15% is standard for Sports & Outdoors)
        actual_commission = fin.get("commission", 0)
        referral_total = round(actual_commission, 2) if actual_commission > 0 else round(revenue * 0.15, 2)

        # Ad spend — pull from advertising if available
        ad_spend = 0
        try:
            ad_row = con.execute("""
                SELECT COALESCE(SUM(spend), 0)
                FROM advertising
                WHERE asin = ? AND date >= ?
            """, [asin, cutoff]).fetchone()
            if ad_row and ad_row[0] > 0:
                ad_spend = round(ad_row[0], 2)
        except Exception:
            # Table might not exist yet
            pass

        cogs_total = units * cogs_per_unit
        net = round(revenue - cogs_total - fba_total - referral_total - ad_spend, 2)
        margin = round(net / revenue * 100) if revenue else 0
        conv_rate = round(units / sessions * 100, 1) if sessions else 0

        products.append({
            "asin": asin,
            "sku": resolved_sku,
            "name": name,
            "rev": round(revenue, 2),
            "units": units,
            "price": aur,
            "sessions": sessions,
            "convRate": conv_rate,
            "cogsPerUnit": cogs_per_unit,
            "cogsTotal": round(cogs_total, 2),
            "fbaTotal": round(fba_total, 2),
            "referralTotal": round(referral_total, 2),
            "adSpend": ad_spend,
            "net": net,
            "margin": margin,
        })

    return products


def _aggregate_weekly(daily_data: list) -> list:
    """Aggregate daily data into weekly buckets."""
    from collections import defaultdict
    weeks = defaultdict(lambda: {"revenue": 0, "units": 0, "orders": 0, "sessions": 0})
    for d in daily_data:
        dt = datetime.strptime(d["date"], "%Y-%m-%d")
        week_start = dt - timedelta(days=dt.weekday())
        key = week_start.strftime("%Y-%m-%d")
        weeks[key]["revenue"] += d["revenue"]
        weeks[key]["units"] += d["units"]
        weeks[key]["orders"] += d["orders"]
        weeks[key]["sessions"] += d["sessions"]

    result = []
    for date_key in sorted(weeks.keys()):
        w = weeks[date_key]
        units = w["units"] or 1
        sessions = w["sessions"] or 1
        result.append({
            "date": date_key,
            "revenue": round(w["revenue"], 2),
            "units": w["units"],
            "orders": w["orders"],
            "sessions": w["sessions"],
            "aur": round(w["revenue"] / units, 2),
            "convRate": round(w["orders"] / sessions * 100, 1),
        })
    return result


# ── Advertising API Routes ─────────────────────────────────

def _safe_ads_query(con, query, params=None):
    """Run an ads query, returning empty list if tables don't exist yet."""
    try:
        return con.execute(query, params or []).fetchall()
    except Exception:
        return []


@app.get("/api/ads/summary")
def ads_summary(days: int = Query(30)):
    """Ads KPIs: total spend, sales, ACOS, ROAS, TACOS, CPC, CTR."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    row = _safe_ads_query(con, """
        SELECT
            COALESCE(SUM(spend), 0) AS spend,
            COALESCE(SUM(sales), 0) AS ad_sales,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(orders), 0) AS ad_orders,
            COALESCE(SUM(units), 0) AS ad_units
        FROM advertising
        WHERE date >= ?
    """, [cutoff])

    if not row or not row[0]:
        con.close()
        return {
            "days": days, "connected": False,
            "spend": 0, "adSales": 0, "impressions": 0, "clicks": 0,
            "orders": 0, "units": 0, "acos": 0, "roas": 0, "tacos": 0,
            "cpc": 0, "ctr": 0, "cvr": 0,
        }

    spend, ad_sales, impressions, clicks, orders, units = row[0]

    # Get total organic revenue for TACOS
    org_row = con.execute("""
        SELECT COALESCE(SUM(ordered_product_sales), 0)
        FROM daily_sales
        WHERE date >= ? AND asin = 'ALL'
    """, [cutoff]).fetchone()
    total_revenue = org_row[0] if org_row else 0

    con.close()

    acos = round(spend / ad_sales * 100, 2) if ad_sales > 0 else 0
    roas = round(ad_sales / spend, 2) if spend > 0 else 0
    tacos = round(spend / total_revenue * 100, 2) if total_revenue > 0 else 0
    cpc = round(spend / clicks, 2) if clicks > 0 else 0
    ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
    cvr = round(orders / clicks * 100, 2) if clicks > 0 else 0

    return {
        "days": days, "connected": True,
        "spend": round(spend, 2),
        "adSales": round(ad_sales, 2),
        "impressions": impressions,
        "clicks": clicks,
        "orders": orders,
        "units": units,
        "acos": acos,
        "roas": roas,
        "tacos": tacos,
        "cpc": cpc,
        "ctr": ctr,
        "cvr": cvr,
    }


@app.get("/api/ads/daily")
def ads_daily(days: int = Query(30)):
    """Daily ads performance time series."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT date,
               COALESCE(SUM(spend), 0),
               COALESCE(SUM(sales), 0),
               COALESCE(SUM(impressions), 0),
               COALESCE(SUM(clicks), 0),
               COALESCE(SUM(orders), 0),
               0, 0, 0, 0, 0
        FROM advertising
        WHERE date >= ?
        GROUP BY date
        ORDER BY date
    """, [cutoff])

    con.close()

    data = []
    for r in rows:
        data.append({
            "date": fmt_date(r[0]),
            "spend": round(r[1], 2),
            "adSales": round(r[2], 2),
            "impressions": r[3],
            "clicks": r[4],
            "orders": r[5],
            "acos": r[6],
            "roas": r[7],
            "tacos": r[8],
            "cpc": r[9],
            "ctr": r[10],
        })

    return {"days": days, "data": data}


@app.get("/api/ads/campaigns")
def ads_campaigns(days: int = Query(30)):
    """Campaign-level performance."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            MAX(campaign_type) AS campaign_type,
            MAX(campaign_status) AS campaign_status,
            MAX(daily_budget) AS daily_budget,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_campaigns
        WHERE date >= ?
        GROUP BY campaign_id
        ORDER BY SUM(spend) DESC
    """, [cutoff])

    con.close()

    campaigns = []
    for r in rows:
        impressions, clicks, spend, sales = r[5], r[6], r[7], r[8]
        orders, units = r[9], r[10]

        campaigns.append({
            "campaignId": r[0],
            "campaignName": r[1],
            "campaignType": r[2],
            "status": r[3],
            "dailyBudget": round(r[4] or 0, 2),
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
            "cvr": round(orders / clicks * 100, 2) if clicks > 0 else 0,
        })

    return {"days": days, "campaigns": campaigns}


@app.get("/api/ads/keywords")
def ads_keywords(days: int = Query(30), sort: str = Query("spend"), limit: int = Query(50)):
    """Top keywords by spend, sales, or ACOS."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT
            keyword_text,
            MAX(match_type) AS match_type,
            MAX(campaign_name) AS campaign_name,
            MAX(ad_group_name) AS ad_group_name,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_keywords
        WHERE date >= ?
        GROUP BY keyword_text
        ORDER BY SUM(spend) DESC
        LIMIT ?
    """, [cutoff, limit])

    con.close()

    keywords = []
    for r in rows:
        impressions, clicks, spend, sales = r[4], r[5], r[6], r[7]
        orders, units = r[8], r[9]

        keywords.append({
            "keyword": r[0],
            "matchType": r[1],
            "campaignName": r[2],
            "adGroupName": r[3],
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
            "cvr": round(orders / clicks * 100, 2) if clicks > 0 else 0,
        })

    return {"days": days, "keywords": keywords}


@app.get("/api/ads/search-terms")
def ads_search_terms(days: int = Query(30), limit: int = Query(50)):
    """Top search terms by spend."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT
            search_term,
            MAX(keyword_text) AS keyword,
            MAX(match_type) AS match_type,
            MAX(campaign_name) AS campaign_name,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_search_terms
        WHERE date >= ?
        GROUP BY search_term
        ORDER BY SUM(spend) DESC
        LIMIT ?
    """, [cutoff, limit])

    con.close()

    terms = []
    for r in rows:
        impressions, clicks, spend, sales = r[4], r[5], r[6], r[7]
        orders, units = r[8], r[9]

        terms.append({
            "searchTerm": r[0],
            "keyword": r[1],
            "matchType": r[2],
            "campaignName": r[3],
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
        })

    return {"days": days, "searchTerms": terms}


@app.get("/api/ads/negative-keywords")
def ads_negative_keywords():
    """List all negative keywords."""
    con = get_db()

    rows = _safe_ads_query(con, """
        SELECT keyword_text, match_type, campaign_name, ad_group_name, keyword_status
        FROM ads_negative_keywords
        ORDER BY campaign_name, keyword_text
    """)

    con.close()

    keywords = [
        {
            "keyword": r[0],
            "matchType": r[1],
            "campaignName": r[2],
            "adGroupName": r[3],
            "status": r[4],
        }
        for r in rows
    ]

    return {"negativeKeywords": keywords}


# ── Debug: Financial Events ───────────────────────────────

@app.get("/api/debug/financial-events")
def debug_financial_events():
    """Call Financial Events API directly and return raw summary for debugging."""
    try:
        from sp_api.api import Finances as FinancesAPI
        from sp_api.base import Marketplaces as _Mp
        creds = _load_sp_api_credentials()
        if not creds:
            return {"error": "No SP-API credentials found"}
        fin_start = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
        finances = FinancesAPI(credentials=creds, marketplace=_Mp.US)

        resp = finances.list_financial_events(PostedAfter=fin_start, MaxResultsPerPage=100)
        payload = resp.payload if hasattr(resp, 'payload') else (resp if isinstance(resp, dict) else {})
        events = payload.get("FinancialEvents", {})

        summary = {}
        sample_charge = None
        for k, v in events.items():
            if isinstance(v, list):
                summary[k] = len(v)
                # Grab a sample charge structure from first shipment
                if k == "ShipmentEventList" and len(v) > 0 and not sample_charge:
                    items = v[0].get("ShipmentItemList", [])
                    if items:
                        charges = items[0].get("ItemChargeList", [])
                        if charges:
                            sample_charge = charges[0]
                if k == "RefundEventList" and len(v) > 0:
                    first = v[0]
                    summary["refund_sample_keys"] = list(first.keys())
                    summary["refund_sample_order_id"] = first.get("AmazonOrderId", "N/A")
                    adj_items = first.get("ShipmentItemAdjustmentList", [])
                    if adj_items:
                        adj_charges = adj_items[0].get("ItemChargeAdjustmentList", [])
                        summary["refund_sample_charge"] = adj_charges[0] if adj_charges else "no charges"
            else:
                summary[k] = str(v)[:100]

        # Also check what's in the DB
        con = duckdb.connect(str(DB_PATH), read_only=False)
        db_counts = con.execute("""
            SELECT event_type, COUNT(*), SUM(ABS(product_charges))
            FROM financial_events
            GROUP BY event_type
        """).fetchall()
        con.close()

        has_next = "NextToken" in payload

        return {
            "api_date_range": fin_start,
            "payload_keys": list(payload.keys()),
            "event_type_counts": summary,
            "sample_charge_object": sample_charge,
            "has_next_page": has_next,
            "db_records": [{"type": r[0], "count": r[1], "total_charges": round(r[2], 2)} for r in db_counts]
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


# ── Period Comparison & Analytics ──────────────────────────

@app.get("/api/comparison")
def period_comparison(view: str = Query("realtime")):
    """Return KPI comparison across multiple time periods.

    Views:
    - realtime: Today / WTD / MTD / YTD
    - weekly: Last Week / 4 Weeks / 13 Weeks / 26 Weeks
    - monthly: Last Month / 2 Mo Ago / 3 Mo Ago / Last 12 Mo
    - yearly: 2026 YTD / 2025 YTD (comp) / 2025 Full / 2024 Full
    - monthly2026: Jan / Feb / ... / current month / YTD total
    """
    # Ensure today has live data before building the response
    _ensure_today_data()

    con = get_db()
    cogs_data = load_cogs()

    today_start = get_today(con)
    today_start = today_start.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = (today_start + timedelta(days=1)).strftime("%Y-%m-%d")
    yr = today_start.year
    month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

    if view == "weekly":
        wd = today_start.weekday()  # Monday=0
        last_week_start = today_start - timedelta(days=wd + 7)
        last_week_end = today_start - timedelta(days=wd)
        periods = [
            {"label": "Last Week",
             "start": last_week_start.strftime("%Y-%m-%d"),
             "end": last_week_end.strftime("%Y-%m-%d")},
            {"label": "4 Weeks",
             "start": (today_start - timedelta(days=28)).strftime("%Y-%m-%d"),
             "end": tomorrow},
            {"label": "13 Weeks",
             "start": (today_start - timedelta(days=91)).strftime("%Y-%m-%d"),
             "end": tomorrow},
            {"label": "26 Weeks",
             "start": (today_start - timedelta(days=182)).strftime("%Y-%m-%d"),
             "end": tomorrow},
        ]
    elif view == "monthly":
        periods = []
        for i in range(1, 4):
            m_start = today_start.replace(day=1)
            # Go back i months
            m = m_start.month - i
            y = m_start.year
            while m <= 0:
                m += 12
                y -= 1
            m_start_actual = datetime(y, m, 1)
            m_end_actual = datetime(y if m < 12 else y + 1, m + 1 if m < 12 else 1, 1)
            label = "Last Month" if i == 1 else f"{i} Mo Ago" if i == 2 else "3 Mo Ago"
            periods.append({
                "label": label,
                "sub": month_names[m - 1] + " " + str(y),
                "start": m_start_actual.strftime("%Y-%m-%d"),
                "end": m_end_actual.strftime("%Y-%m-%d"),
            })
        periods.append({
            "label": "Last 12 Mo",
            "start": (today_start - timedelta(days=365)).strftime("%Y-%m-%d"),
            "end": tomorrow,
        })
    elif view == "yearly":
        ytd_comp_end = datetime(yr - 1, today_start.month, today_start.day) + timedelta(days=1)
        periods = [
            {"label": f"{yr} YTD",
             "start": f"{yr}-01-01",
             "end": tomorrow},
            {"label": f"{yr-1} YTD",
             "sub": "same window comp",
             "start": f"{yr-1}-01-01",
             "end": ytd_comp_end.strftime("%Y-%m-%d")},
            {"label": f"{yr-1} Full",
             "start": f"{yr-1}-01-01",
             "end": f"{yr}-01-01"},
            {"label": f"{yr-2} Full",
             "start": f"{yr-2}-01-01",
             "end": f"{yr-1}-01-01"},
        ]
    elif view == "monthly2026":
        periods = []
        for m in range(1, today_start.month + 1):
            m_start = datetime(yr, m, 1)
            m_end = tomorrow if m == today_start.month else datetime(yr, m + 1, 1).strftime("%Y-%m-%d")
            if isinstance(m_end, datetime):
                m_end = m_end.strftime("%Y-%m-%d")
            periods.append({
                "label": month_names[m - 1],
                "sub": str(yr),
                "start": m_start.strftime("%Y-%m-%d"),
                "end": m_end,
            })
        periods.append({
            "label": f"{yr} Total",
            "sub": "YTD",
            "start": f"{yr}-01-01",
            "end": tomorrow,
        })
    else:
        # Default: realtime (Today / WTD / MTD / YTD)
        # WTD: rolling 7 days so it's always meaningful (not empty on Monday)
        week_start = today_start - timedelta(days=6)
        month_start = today_start.replace(day=1)
        year_start = today_start.replace(month=1, day=1)

        periods = [
            {"label": "Today",
             "start": today_start.strftime("%Y-%m-%d"),
             "end": tomorrow},
            {"label": "WTD",
             "start": week_start.strftime("%Y-%m-%d"),
             "end": tomorrow},
            {"label": "MTD",
             "start": month_start.strftime("%Y-%m-%d"),
             "end": tomorrow},
            {"label": "YTD",
             "start": year_start.strftime("%Y-%m-%d"),
             "end": tomorrow},
        ]

    def chg(cur, prev):
        if prev == 0:
            return 0
        return round((cur - prev) / prev * 100, 1)

    # Compute cost ratios from full product list
    full_products = _build_product_list(con, f"{yr-1}-01-01")
    full_prod_rev = sum(pp["rev"] for pp in full_products)
    full_prod_net = sum(pp["net"] for pp in full_products)
    full_prod_cogs = sum(pp["cogsTotal"] for pp in full_products)
    full_prod_fba = sum(pp["fbaTotal"] for pp in full_products)
    full_prod_referral = sum(pp["referralTotal"] for pp in full_products)

    margin_pct = full_prod_net / full_prod_rev if full_prod_rev > 0 else 0
    cogs_pct = full_prod_cogs / full_prod_rev if full_prod_rev > 0 else 0
    fba_pct = full_prod_fba / full_prod_rev if full_prod_rev > 0 else 0
    referral_pct = full_prod_referral / full_prod_rev if full_prod_rev > 0 else 0

    results = []
    for p in periods:
        row = con.execute("""
            SELECT
                COALESCE(SUM(ordered_product_sales), 0),
                COALESCE(SUM(units_ordered), 0),
                COALESCE(SUM(sessions), 0)
            FROM daily_sales
            WHERE date >= ? AND date < ? AND asin = 'ALL'
        """, [p["start"], p["end"]]).fetchone()

        rev, units, sessions = row
        orders = units
        aur = round(rev / units, 2) if units else 0
        conv = round(units / sessions * 100, 1) if sessions else 0

        # Ad spend for period
        ad_spend = 0
        try:
            ad_row = con.execute("""
                SELECT COALESCE(SUM(spend), 0)
                FROM advertising
                WHERE date >= ? AND date < ?
            """, [p["start"], p["end"]]).fetchone()
            ad_spend = round(ad_row[0], 2) if ad_row else 0
        except Exception:
            pass

        tacos = round(ad_spend / rev * 100, 1) if rev > 0 else 0

        # Cost breakdown using known ratios
        cogs = round(rev * cogs_pct, 2)
        fba_fees = round(rev * fba_pct, 2)
        referral_fees = round(rev * referral_pct, 2)
        amazon_fees = round(fba_fees + referral_fees, 2)
        net = round(rev - cogs - amazon_fees - ad_spend, 2)
        margin_val = round(net / rev * 100, 1) if rev > 0 else 0

        # Compute last-year equivalents
        try:
            ly_start = datetime.strptime(p["start"], "%Y-%m-%d").replace(year=datetime.strptime(p["start"], "%Y-%m-%d").year - 1)
            ly_end = datetime.strptime(p["end"], "%Y-%m-%d").replace(year=datetime.strptime(p["end"], "%Y-%m-%d").year - 1)
        except ValueError:
            ly_start = datetime.strptime(p["start"], "%Y-%m-%d") - timedelta(days=365)
            ly_end = datetime.strptime(p["end"], "%Y-%m-%d") - timedelta(days=365)

        ly_row = con.execute("""
            SELECT
                COALESCE(SUM(ordered_product_sales), 0),
                COALESCE(SUM(units_ordered), 0),
                COALESCE(SUM(sessions), 0)
            FROM daily_sales
            WHERE date >= ? AND date < ? AND asin = 'ALL'
        """, [ly_start.strftime("%Y-%m-%d"), ly_end.strftime("%Y-%m-%d")]).fetchone()

        ly_rev, ly_units, ly_sessions = ly_row
        ly_orders = ly_units

        ly_ad = 0
        try:
            ly_ad_row = con.execute("""
                SELECT COALESCE(SUM(spend), 0)
                FROM advertising
                WHERE date >= ? AND date < ?
            """, [ly_start.strftime("%Y-%m-%d"), ly_end.strftime("%Y-%m-%d")]).fetchone()
            ly_ad = round(ly_ad_row[0], 2) if ly_ad_row else 0
        except Exception:
            pass

        results.append({
            "label": p["label"],
            "sub": p.get("sub", ""),
            "revenue": round(rev, 2),
            "units": units,
            "orders": orders,
            "aur": aur,
            "sessions": sessions,
            "convRate": conv,
            "adSpend": ad_spend,
            "tacos": tacos,
            "cogs": cogs,
            "amazonFees": amazon_fees,
            "fbaFees": fba_fees,
            "referralFees": referral_fees,
            "netProfit": net,
            "margin": margin_val,
            "revChg": chg(rev, ly_rev),
            "unitChg": chg(units, ly_units),
            "orderChg": chg(orders, ly_orders),
            "sessChg": chg(sessions, ly_sessions),
            "adChg": chg(ad_spend, ly_ad),
        })

    con.close()
    return {"view": view, "periods": results}


@app.get("/api/monthly-yoy")
def monthly_yoy():
    """Monthly revenue broken down by year for YoY comparison.
    Always returns 12 months with bars for 2024, 2025, 2026."""
    con = get_db()

    rows = con.execute("""
        SELECT
            EXTRACT(YEAR FROM date) AS yr,
            EXTRACT(MONTH FROM date) AS mo,
            COALESCE(SUM(ordered_product_sales), 0) AS revenue
        FROM daily_sales
        WHERE asin = 'ALL'
          AND EXTRACT(YEAR FROM date) >= 2024
        GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
        ORDER BY yr, mo
    """).fetchall()

    con.close()

    months_map = {}
    for r in rows:
        yr, mo, rev = int(r[0]), int(r[1]), round(r[2], 2)
        if mo not in months_map:
            months_map[mo] = {}
        months_map[mo][yr] = rev

    # Always include 2024, 2025, 2026
    years = [2024, 2025, 2026]
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    data = []
    for mo in range(1, 13):
        entry = {"month": month_names[mo - 1]}
        for yr in years:
            entry[str(yr)] = months_map.get(mo, {}).get(yr, 0)
        data.append(entry)

    return {"years": years, "data": data}


@app.get("/api/product-mix")
def product_mix(days: int = Query(365)):
    """Top 10 products by revenue for donut chart."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    products = _build_product_list(con, cutoff)
    con.close()

    products.sort(key=lambda p: p["rev"], reverse=True)
    top10 = products[:10]
    other_rev = sum(p["rev"] for p in products[10:])

    result = [{"name": p["name"][:30], "value": p["rev"]} for p in top10]
    if other_rev > 0:
        result.append({"name": "Other", "value": round(other_rev, 2)})

    return {"products": result}


@app.get("/api/color-mix")
def color_mix(days: int = Query(365)):
    """Sales breakdown by color extracted from product names."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    products = _build_product_list(con, cutoff)
    con.close()

    import re
    color_keywords = ["Orange", "Blue", "Green", "Red", "Grey", "White", "Black", "Pink", "Purple", "Yellow", "Ombre"]
    color_totals = {}

    for p in products:
        name = p.get("name", "")
        matched = False
        for color in color_keywords:
            if re.search(r'\b' + color + r'\b', name, re.IGNORECASE):
                color_totals[color] = color_totals.get(color, 0) + p["rev"]
                matched = True
                break
        if not matched:
            color_totals["Other"] = color_totals.get("Other", 0) + p["rev"]

    total_rev = sum(color_totals.values())
    result = []
    for color, rev in sorted(color_totals.items(), key=lambda x: x[1], reverse=True):
        pct = round(rev / total_rev * 100, 1) if total_rev > 0 else 0
        result.append({"color": color, "revenue": round(rev, 2), "pct": pct})

    return {"colors": result, "total": round(total_rev, 2)}


# ── Profitability (Sellerboard-style) ─────────────────────

@app.get("/api/profitability")
def profitability(view: str = Query("realtime")):
    """Sellerboard-style profit waterfall with same period views as comparison.

    Waterfall: Sales - Promo - Ads - Shipping - Refunds - Amazon Fees - COGS - Indirect = Net Profit
    Returns both account-level waterfall and per-ASIN item breakdown.
    """
    # Ensure today has live data before building the response
    _ensure_today_data()

    con = get_db()
    cogs_data = load_cogs()

    today_start = get_today(con)
    today_start = today_start.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = (today_start + timedelta(days=1)).strftime("%Y-%m-%d")
    yr = today_start.year
    month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

    # Build periods (same logic as comparison endpoint)
    if view == "weekly":
        wd = today_start.weekday()
        last_week_start = today_start - timedelta(days=wd + 7)
        last_week_end = today_start - timedelta(days=wd)
        periods = [
            {"label": "Last Week", "start": last_week_start.strftime("%Y-%m-%d"), "end": last_week_end.strftime("%Y-%m-%d")},
            {"label": "4 Weeks", "start": (today_start - timedelta(days=28)).strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "13 Weeks", "start": (today_start - timedelta(days=91)).strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "26 Weeks", "start": (today_start - timedelta(days=182)).strftime("%Y-%m-%d"), "end": tomorrow},
        ]
    elif view == "monthly":
        periods = []
        for i in range(1, 4):
            m_start = today_start.replace(day=1)
            m = m_start.month - i
            y = m_start.year
            while m <= 0:
                m += 12; y -= 1
            m_start_actual = datetime(y, m, 1)
            m_end_actual = datetime(y if m < 12 else y + 1, m + 1 if m < 12 else 1, 1)
            label = "Last Month" if i == 1 else f"{i} Mo Ago" if i == 2 else "3 Mo Ago"
            periods.append({"label": label, "sub": month_names[m-1] + " " + str(y),
                            "start": m_start_actual.strftime("%Y-%m-%d"), "end": m_end_actual.strftime("%Y-%m-%d")})
        periods.append({"label": "Last 12 Mo", "start": (today_start - timedelta(days=365)).strftime("%Y-%m-%d"), "end": tomorrow})
    elif view == "yearly":
        try:
            ytd_comp_end = datetime(yr - 1, today_start.month, today_start.day) + timedelta(days=1)
        except ValueError:
            ytd_comp_end = datetime(yr - 1, today_start.month, 28) + timedelta(days=1)
        periods = [
            {"label": f"{yr} YTD", "start": f"{yr}-01-01", "end": tomorrow},
            {"label": f"{yr-1} YTD", "sub": "same window comp", "start": f"{yr-1}-01-01", "end": ytd_comp_end.strftime("%Y-%m-%d")},
            {"label": f"{yr-1} Full", "start": f"{yr-1}-01-01", "end": f"{yr}-01-01"},
            {"label": f"{yr-2} Full", "start": f"{yr-2}-01-01", "end": f"{yr-1}-01-01"},
        ]
    elif view == "monthly2026":
        periods = []
        for m in range(1, today_start.month + 1):
            m_start = datetime(yr, m, 1)
            m_end = tomorrow if m == today_start.month else datetime(yr, m + 1, 1).strftime("%Y-%m-%d")
            if isinstance(m_end, datetime):
                m_end = m_end.strftime("%Y-%m-%d")
            periods.append({"label": month_names[m-1], "sub": str(yr),
                            "start": m_start.strftime("%Y-%m-%d"), "end": m_end})
        periods.append({"label": f"{yr} Total", "sub": "YTD", "start": f"{yr}-01-01", "end": tomorrow})
    else:
        # WTD: rolling 7 days so it's always meaningful (not empty on Monday)
        week_start = today_start - timedelta(days=6)
        month_start = today_start.replace(day=1)
        year_start = today_start.replace(month=1, day=1)
        periods = [
            {"label": "Today", "start": today_start.strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "WTD", "start": week_start.strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "MTD", "start": month_start.strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "YTD", "start": year_start.strftime("%Y-%m-%d"), "end": tomorrow},
        ]

    results = []
    for p in periods:
        wf = _build_waterfall(con, cogs_data, p["start"], p["end"])
        wf["label"] = p["label"]
        wf["sub"] = p.get("sub", "")
        results.append(wf)

    con.close()
    return {"view": view, "periods": results}


@app.get("/api/profitability/items")
def profitability_items(days: int = Query(365)):
    """Per-ASIN profitability breakdown matching Sellerboard item view."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    products = _build_product_list(con, cutoff)

    # Get per-ASIN financial event data for the period
    fin_by_asin = {}
    try:
        fin_rows = con.execute("""
            SELECT asin,
                   SUM(ABS(promotion_amount)) AS promo,
                   SUM(ABS(shipping_charges)) AS shipping,
                   SUM(ABS(other_fees)) AS other,
                   SUM(CASE WHEN event_type LIKE '%Refund%' OR event_type LIKE '%Return%'
                       THEN ABS(product_charges) ELSE 0 END) AS refund_amt,
                   COUNT(CASE WHEN event_type LIKE '%Refund%' OR event_type LIKE '%Return%'
                       THEN 1 END) AS refund_count
            FROM financial_events
            WHERE date >= ?
            GROUP BY asin
        """, [cutoff]).fetchall()
        fin_by_asin = {r[0]: {"promo": r[1], "shipping": r[2], "other": r[3],
                                "refund_amt": r[4], "refund_count": r[5]} for r in fin_rows}
    except Exception:
        pass

    # Get per-ASIN ad spend from advertising table
    ad_by_asin = {}
    try:
        ad_rows = con.execute("""
            SELECT asin, COALESCE(SUM(spend), 0) AS spend
            FROM advertising
            WHERE date >= ?
            GROUP BY asin
        """, [cutoff]).fetchall()
        ad_by_asin = {r[0]: r[1] for r in ad_rows}
    except Exception:
        pass

    # Enhance with Sellerboard-style metrics
    items = []
    for p in products:
        rev = p["rev"]
        cogs_total = p["cogsTotal"]
        net = p["net"]
        amazon_fees = p["fbaTotal"] + p["referralTotal"]
        margin = round(net / rev * 100, 1) if rev > 0 else 0
        roi = round(net / cogs_total * 100, 1) if cogs_total > 0 else 0

        asin = p["asin"]
        fin = fin_by_asin.get(asin, {})
        ad_spend = ad_by_asin.get(asin, p["adSpend"])
        refund_units = fin.get("refund_count", 0)
        return_pct = round(refund_units / p["units"] * 100, 1) if p["units"] > 0 else 0

        items.append({
            "asin": asin,
            "sku": p["sku"],
            "name": p["name"],
            "units": p["units"],
            "sales": rev,
            "promo": round(fin.get("promo", 0), 2),
            "adSpend": round(ad_spend, 2),
            "shipping": round(fin.get("shipping", 0), 2),
            "refunds": round(fin.get("refund_amt", 0), 2),
            "refundUnits": refund_units,
            "returnPct": return_pct,
            "amazonFees": round(amazon_fees, 2),
            "fbaFees": p["fbaTotal"],
            "referralFees": p["referralTotal"],
            "otherFees": round(fin.get("other", 0), 2),
            "cogs": cogs_total,
            "cogsPerUnit": p["cogsPerUnit"],
            "indirect": 0,
            "netProfit": net,
            "margin": margin,
            "roi": roi,
            "aur": p["price"],
        })

    items.sort(key=lambda x: x["sales"], reverse=True)
    con.close()
    return {"days": days, "items": items}


def _build_waterfall(con, cogs_data, start: str, end: str) -> dict:
    """Build Sellerboard-style waterfall for a single period."""
    # Account-level revenue
    row = con.execute("""
        SELECT COALESCE(SUM(ordered_product_sales), 0),
               COALESCE(SUM(units_ordered), 0)
        FROM daily_sales
        WHERE date >= ? AND date < ? AND asin = 'ALL'
    """, [start, end]).fetchone()
    sales, units = row[0], row[1]

    # Per-ASIN cost breakdown
    asin_rows = con.execute("""
        SELECT asin, MAX(sku) AS sku,
               COALESCE(SUM(ordered_product_sales), 0) AS revenue,
               COALESCE(SUM(units_ordered), 0) AS units
        FROM daily_sales
        WHERE date >= ? AND date < ? AND asin <> 'ALL'
        GROUP BY asin
    """, [start, end]).fetchall()

    # Financial data (date-filtered for the period)
    fin_rows = []
    try:
        fin_rows = con.execute("""
            SELECT asin,
                   SUM(ABS(fba_fees)) AS fba,
                   SUM(ABS(commission)) AS comm,
                   SUM(ABS(promotion_amount)) AS promo,
                   SUM(ABS(shipping_charges)) AS shipping,
                   SUM(ABS(other_fees)) AS other,
                   SUM(CASE WHEN event_type LIKE '%Refund%' OR event_type LIKE '%Return%'
                       THEN ABS(product_charges) ELSE 0 END) AS refund_amt,
                   COUNT(CASE WHEN event_type LIKE '%Refund%' OR event_type LIKE '%Return%'
                       THEN 1 END) AS refund_count
            FROM financial_events
            WHERE date >= ? AND date < ?
            GROUP BY asin
        """, [start, end]).fetchall()
    except Exception:
        pass
    fin_by_asin = {r[0]: {"fba": r[1], "comm": r[2], "promo": r[3],
                           "shipping": r[4], "other": r[5],
                           "refund_amt": r[6], "refund_count": r[7]} for r in fin_rows}

    # Ad spend from the *advertising* table (the actual table name in this DB)
    total_ad = 0
    try:
        ad_row = con.execute("""
            SELECT COALESCE(SUM(spend), 0)
            FROM advertising
            WHERE date >= ? AND date < ?
        """, [start, end]).fetchone()
        total_ad = ad_row[0] if ad_row else 0
    except Exception:
        pass

    # Compute costs
    total_cogs = 0
    total_fba = 0
    total_referral = 0
    total_promo = 0
    total_shipping = 0
    total_refunds = 0
    total_other_fees = 0
    total_refund_units = 0
    prod_rev = 0

    for ar in asin_rows:
        asin, sku, rev, u = ar
        if u == 0:
            continue
        prod_rev += rev
        aur = rev / u if u else 0

        # COGS
        ci = cogs_data.get(asin) or cogs_data.get(sku or "") or {}
        cpu = ci.get("cogs", 0)
        if cpu == 0:
            cpu = round(aur * 0.35, 2)
        total_cogs += u * cpu

        # Financial data for this ASIN
        fin = fin_by_asin.get(asin, {})
        actual_fba = fin.get("fba", 0)
        actual_comm = fin.get("comm", 0)

        # FBA fees: use actual if available, else estimate ~12% of revenue (oversized golf equipment)
        if actual_fba > 0:
            total_fba += actual_fba
        else:
            est_fba = round(rev * 0.12, 2)
            total_fba += est_fba

        # Referral fees: use actual commission if available, else 15%
        if actual_comm > 0:
            total_referral += actual_comm
        else:
            total_referral += rev * 0.15

        # Promo, shipping, refunds from financial events
        total_promo += fin.get("promo", 0)
        total_shipping += fin.get("shipping", 0)
        total_refunds += fin.get("refund_amt", 0)
        total_other_fees += fin.get("other", 0)
        total_refund_units += fin.get("refund_count", 0)

    # If we have per-ASIN data for this period, scale to actual sales
    # If not, fall back to global cost ratios from _build_product_list
    if prod_rev > 0 and total_cogs > 0:
        scale = sales / prod_rev if prod_rev > 0 else 1
        cogs = round(total_cogs * scale, 2)
        fba_fees = round(total_fba * scale, 2)
        referral_fees = round(total_referral * scale, 2)
        promo = round(total_promo * scale, 2)
        shipping = round(total_shipping * scale, 2)
        refunds = round(total_refunds * scale, 2)
        other_fees = round(total_other_fees * scale, 2)
    elif sales > 0:
        # No per-ASIN data for this period — use global cost ratios
        # This happens when SP-API per-ASIN report data has different dates
        all_products = _build_product_list(con, "2020-01-01")
        total_prd_rev = sum(p["rev"] for p in all_products)
        if total_prd_rev > 0:
            cogs_pct = sum(p["cogsTotal"] for p in all_products) / total_prd_rev
            fba_pct = sum(p["fbaTotal"] for p in all_products) / total_prd_rev
            ref_pct = sum(p["referralTotal"] for p in all_products) / total_prd_rev
        else:
            cogs_pct, fba_pct, ref_pct = 0.35, 0.12, 0.15
        cogs = round(sales * cogs_pct, 2)
        fba_fees = round(sales * fba_pct, 2)
        referral_fees = round(sales * ref_pct, 2)
        promo = 0
        shipping = 0
        refunds = 0
        other_fees = 0
    else:
        cogs = fba_fees = referral_fees = promo = shipping = refunds = other_fees = 0

    amazon_fees = round(fba_fees + referral_fees, 2)
    ad_spend = round(total_ad, 2)  # already account-level, no scaling needed
    refund_units = total_refund_units
    return_pct = round(refund_units / units * 100, 1) if units > 0 else 0
    indirect = 0    # Not configured

    gross_profit = round(sales - promo - ad_spend - shipping - refunds - amazon_fees - cogs, 2)
    net_profit = round(gross_profit - indirect, 2)
    margin = round(net_profit / sales * 100, 1) if sales > 0 else 0
    roi = round(net_profit / cogs * 100, 1) if cogs > 0 else 0
    real_acos = round(ad_spend / sales * 100, 1) if sales > 0 else 0

    return {
        "sales": round(sales, 2),
        "units": units,
        "promo": promo,
        "adSpend": ad_spend,
        "shipping": shipping,
        "refunds": refunds,
        "refundUnits": refund_units,
        "returnPct": return_pct,
        "amazonFees": amazon_fees,
        "fbaFees": fba_fees,
        "referralFees": referral_fees,
        "otherFees": other_fees,
        "cogs": cogs,
        "indirect": indirect,
        "grossProfit": gross_profit,
        "netProfit": net_profit,
        "margin": margin,
        "roi": roi,
        "realAcos": real_acos,
    }


# ── Static Frontend ────────────────────────────────────────
# Serve the built React frontend from the same server.
# Check local dist/ first (Docker), then ../frontend/dist/ (local dev)
_local_dist = Path(__file__).resolve().parent / "dist"
_dev_dist = Path(__file__).resolve().parent.parent / "frontend" / "dist"
FRONTEND_DIR = _local_dist if _local_dist.exists() else _dev_dist

if FRONTEND_DIR.exists():
    # Mount static assets (JS, CSS, images) — must come AFTER api routes
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR / "assets")), name="static-assets")

    # Catch-all for SPA: serve index.html for any non-API, non-asset route
    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        # If a real file exists (like vite.svg), serve it
        file_path = FRONTEND_DIR / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        # Otherwise serve index.html (SPA routing)
        return FileResponse(str(FRONTEND_DIR / "index.html"))


# ── Run ─────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
