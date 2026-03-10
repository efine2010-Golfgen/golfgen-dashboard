"""
GolfGen Amazon SP-API Data Pipeline
====================================
Pulls sales, orders, inventory, and financial data from Amazon SP-API.
Stores results in DuckDB for dashboard consumption.

Requirements:
    pip install python-amazon-sp-api duckdb requests

Setup:
    1. Copy config/credentials_template.json to config/credentials.json
    2. Fill in your Amazon SP-API credentials (see SETUP_GUIDE.md)
    3. Run: python scripts/amazon_sp_api.py
"""

import gzip
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

try:
    from sp_api.api import (
        Orders,
        Reports,
        CatalogItems,
        Inventories,
        Finances,
        Sales,
    )
    from sp_api.base import Marketplaces, ReportType
    from sp_api.base.exceptions import SellingApiException
except ImportError:
    print("ERROR: python-amazon-sp-api not installed. Run: pip install python-amazon-sp-api")
    sys.exit(1)


def load_credentials():
    cred_path = PROJECT_ROOT / "config" / "credentials.json"
    if not cred_path.exists():
        print("ERROR: config/credentials.json not found.")
        print("Copy config/credentials_template.json to config/credentials.json and fill in your credentials.")
        sys.exit(1)
    with open(cred_path) as f:
        return json.load(f)


def get_sp_api_credentials(creds):
    sp = creds["AMAZON_SP_API"]
    return {
        "refresh_token": sp["refresh_token"],
        "lwa_app_id": sp["lwa_app_id"],
        "lwa_client_secret": sp["lwa_client_secret"],
        "aws_access_key": sp["aws_access_key"],
        "aws_secret_key": sp["aws_secret_key"],
        "role_arn": sp["role_arn"],
    }


def init_database(db_path):
    con = duckdb.connect(str(db_path))

    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_sales (
            date DATE,
            asin VARCHAR,
            sku VARCHAR,
            product_name VARCHAR,
            units_ordered INTEGER,
            units_ordered_b2b INTEGER,
            ordered_product_sales DOUBLE,
            ordered_product_sales_b2b DOUBLE,
            total_order_items INTEGER,
            average_selling_price DOUBLE,
            sessions INTEGER,
            session_percentage DOUBLE,
            page_views INTEGER,
            buy_box_percentage DOUBLE,
            unit_session_percentage DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, asin)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR PRIMARY KEY,
            purchase_date TIMESTAMP,
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
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_id VARCHAR,
            order_item_id VARCHAR,
            asin VARCHAR,
            sku VARCHAR,
            title VARCHAR,
            quantity INTEGER,
            item_price DOUBLE,
            item_tax DOUBLE,
            promotion_discount DOUBLE,
            is_gift BOOLEAN,
            PRIMARY KEY (order_id, order_item_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS fba_inventory (
            date DATE,
            asin VARCHAR,
            sku VARCHAR,
            product_name VARCHAR,
            condition VARCHAR,
            fulfillable_quantity INTEGER,
            inbound_working_quantity INTEGER,
            inbound_shipped_quantity INTEGER,
            inbound_receiving_quantity INTEGER,
            reserved_quantity INTEGER,
            unfulfillable_quantity INTEGER,
            total_quantity INTEGER,
            days_of_supply INTEGER,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, asin)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS financial_events (
            date DATE,
            asin VARCHAR,
            sku VARCHAR,
            order_id VARCHAR,
            event_type VARCHAR,
            product_charges DOUBLE,
            product_charges_tax DOUBLE,
            shipping_charges DOUBLE,
            shipping_charges_tax DOUBLE,
            giftwrap_charges DOUBLE,
            fba_fees DOUBLE,
            commission DOUBLE,
            promotion_amount DOUBLE,
            other_fees DOUBLE,
            net_proceeds DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS advertising (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            campaign_type VARCHAR,
            ad_group_name VARCHAR,
            targeting VARCHAR,
            asin VARCHAR,
            impressions INTEGER,
            clicks INTEGER,
            spend DOUBLE,
            sales DOUBLE,
            orders INTEGER,
            units INTEGER,
            acos DOUBLE,
            roas DOUBLE,
            cpc DOUBLE,
            ctr DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS pull_log_seq START 1
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS pull_log (
            pull_id INTEGER DEFAULT nextval('pull_log_seq'),
            pull_type VARCHAR,
            start_date DATE,
            end_date DATE,
            records_pulled INTEGER,
            status VARCHAR,
            error_message VARCHAR,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.close()
    print(f"Database initialized at {db_path}")


# ─────────────────────────────────────────────────────
# DATA PULL FUNCTIONS
# ─────────────────────────────────────────────────────

def pull_sales_report(credentials, db_path, days_back=7):
    """Pull Sales & Traffic report (Business Reports) from SP-API."""
    print(f"\n{'='*60}")
    print("PULLING SALES & TRAFFIC DATA")
    print(f"{'='*60}")

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)
    print(f"Date range: {start_date} to {end_date}")

    try:
        reports = Reports(credentials=credentials, marketplace=Marketplaces.US)

        # Request the Sales and Traffic report by ASIN (child)
        report_response = reports.create_report(
            reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
            dataStartTime=start_date.isoformat(),
            dataEndTime=end_date.isoformat(),
            reportOptions={"dateGranularity": "DAY", "asinGranularity": "CHILD"}
        )

        report_id = report_response.payload.get("reportId")
        print(f"Report requested: {report_id}")

        # Poll for report completion
        for attempt in range(30):
            time.sleep(10)
            status_response = reports.get_report(report_id)
            status = status_response.payload.get("processingStatus")
            print(f"  Status: {status}")

            if status == "DONE":
                doc_id = status_response.payload.get("reportDocumentId")
                doc_response = reports.get_report_document(doc_id)
                report_data = doc_response.payload
                break
            elif status in ("CANCELLED", "FATAL"):
                print(f"  Report failed with status: {status}")
                return 0
        else:
            print("  Timed out waiting for report")
            return 0

        # Parse and store the report data
        con = duckdb.connect(str(db_path))
        records = 0

        # Debug: show what we got back
        print(f"  Report data type: {type(report_data)}")
        if isinstance(report_data, dict):
            print(f"  Top-level keys: {list(report_data.keys())}")
            if "url" in report_data:
                print(f"  Report has download URL - fetching...")
                import requests as req
                is_gzipped = report_data.get("compressionAlgorithm", "").upper() == "GZIP"
                resp = req.get(report_data["url"])
                if is_gzipped:
                    print(f"  Downloaded {len(resp.content)} bytes (GZIP compressed)")
                    try:
                        decompressed = gzip.decompress(resp.content)
                        report_text = decompressed.decode("utf-8")
                        print(f"  Decompressed to {len(report_text)} chars")
                    except Exception as gz_err:
                        print(f"  GZIP decompression failed: {gz_err}")
                        con.close()
                        return 0
                else:
                    report_text = resp.text
                    print(f"  Downloaded {len(report_text)} chars (plain text)")
                try:
                    report_data = json.loads(report_text)
                    print(f"  Parsed as JSON. Keys: {list(report_data.keys()) if isinstance(report_data, dict) else 'N/A'}")
                except json.JSONDecodeError:
                    print(f"  Data is not JSON. Preview: {report_text[:300]}")
                    con.close()
                    return 0

        # Handle string response
        if isinstance(report_data, str):
            try:
                report_data = json.loads(report_data)
            except json.JSONDecodeError:
                print(f"  Could not parse report data as JSON")
                print(f"  Data preview: {str(report_data)[:300]}")
                con.close()
                return 0

        if isinstance(report_data, dict):
            # Try salesAndTrafficByAsin format
            asin_data_list = report_data.get("salesAndTrafficByAsin", [])
            date_data_list = report_data.get("salesAndTrafficByDate", [])
            print(f"  ASIN entries: {len(asin_data_list)}, Date entries: {len(date_data_list)}")

            if asin_data_list:
                for asin_entry in asin_data_list:
                    asin = asin_entry.get("childAsin") or asin_entry.get("parentAsin", "")
                    traffic = asin_entry.get("trafficByAsin", {})
                    sales_info = asin_entry.get("salesByAsin", {})

                    # Get the date from the entry or use start_date
                    entry_date = asin_entry.get("date", str(start_date))

                    ordered_sales = sales_info.get("orderedProductSales", {})
                    if isinstance(ordered_sales, dict):
                        sales_amount = float(ordered_sales.get("amount", 0))
                    else:
                        sales_amount = float(ordered_sales or 0)

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

            if date_data_list:
                for day_entry in date_data_list:
                    entry_date = day_entry.get("date", "")
                    traffic = day_entry.get("trafficByDate", {})
                    sales_info = day_entry.get("salesByDate", {})

                    ordered_sales = sales_info.get("orderedProductSales", {})
                    if isinstance(ordered_sales, dict):
                        sales_amount = float(ordered_sales.get("amount", 0))
                    else:
                        sales_amount = float(ordered_sales or 0)

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
        else:
            print(f"  Unexpected report data type: {type(report_data)}")
            print(f"  Data preview: {str(report_data)[:200]}")

        con.close()
        print(f"Stored {records} records")
        return records

    except SellingApiException as e:
        print(f"SP-API Error: {e}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 0


def pull_orders(credentials, db_path, days_back=7):
    """Pull recent orders from SP-API Orders endpoint."""
    print(f"\n{'='*60}")
    print("PULLING ORDERS DATA")
    print(f"{'='*60}")

    after_date = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
    print(f"Orders after: {after_date}")

    try:
        orders_api = Orders(credentials=credentials, marketplace=Marketplaces.US)
        response = orders_api.get_orders(
            CreatedAfter=after_date,
            MarketplaceIds=["ATVPDKIKX0DER"],
            MaxResultsPerPage=100
        )

        order_list = response.payload.get("Orders", [])
        con = duckdb.connect(str(db_path))
        records = 0

        for order in order_list:
            order_id = order.get("AmazonOrderId")
            order_total = order.get("OrderTotal", {})

            con.execute("""
                INSERT OR REPLACE INTO orders
                (order_id, purchase_date, order_status, fulfillment_channel,
                 sales_channel, order_total, currency_code, number_of_items,
                 ship_city, ship_state, ship_postal_code,
                 is_business_order, is_prime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                order_id,
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
            records += 1

            # Pull order items with rate limiting
            try:
                time.sleep(2)  # Rate limit: wait 2 seconds between item requests
                items_response = orders_api.get_order_items(order_id)
                for item in items_response.payload.get("OrderItems", []):
                    item_price = item.get("ItemPrice", {})
                    item_tax = item.get("ItemTax", {})
                    promo = item.get("PromotionDiscount", {})

                    con.execute("""
                        INSERT OR REPLACE INTO order_items
                        (order_id, order_item_id, asin, sku, title,
                         quantity, item_price, item_tax, promotion_discount, is_gift)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        order_id,
                        item.get("OrderItemId"),
                        item.get("ASIN"),
                        item.get("SellerSKU"),
                        item.get("Title", "")[:200],
                        item.get("QuantityOrdered", 0),
                        float(item_price.get("Amount", 0)) if item_price else 0,
                        float(item_tax.get("Amount", 0)) if item_tax else 0,
                        float(promo.get("Amount", 0)) if promo else 0,
                        item.get("IsGift", False),
                    ])
            except SellingApiException as e:
                if "QuotaExceeded" in str(e):
                    print(f"  Rate limited on order items. Waiting 30s...")
                    time.sleep(30)
                else:
                    print(f"  Error pulling items for {order_id}: {e}")
            except Exception as e:
                print(f"  Error pulling items for {order_id}: {e}")

        con.close()
        print(f"Stored {records} orders")
        return records

    except SellingApiException as e:
        print(f"SP-API Error: {e}")
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 0


def pull_fba_inventory(credentials, db_path):
    """Pull FBA inventory levels from SP-API."""
    print(f"\n{'='*60}")
    print("PULLING FBA INVENTORY DATA")
    print(f"{'='*60}")

    # First try via Inventories API (real-time)
    try:
        print("  Trying Inventories API (real-time)...")
        inventory_api = Inventories(credentials=credentials, marketplace=Marketplaces.US)
        response = inventory_api.get_inventory_summary_marketplace(
            marketplaceIds=["ATVPDKIKX0DER"],
            granularityType="Marketplace",
            granularityId="ATVPDKIKX0DER"
        )

        summaries = response.payload.get("inventorySummaries", [])
        if summaries:
            con = duckdb.connect(str(db_path))
            today = datetime.now().date()
            records = 0

            for item in summaries:
                inv = item.get("inventoryDetails", {})
                fulfillable = inv.get("fulfillableQuantity", 0) or item.get("totalQuantity", 0)
                inbound_working = inv.get("inboundWorkingQuantity", 0)
                inbound_shipped = inv.get("inboundShippedQuantity", 0)
                inbound_receiving = inv.get("inboundReceivingQuantity", 0)
                reserved = inv.get("reservedQuantity", {})
                reserved_qty = 0
                if isinstance(reserved, dict):
                    reserved_qty = reserved.get("totalReservedQuantity", 0)
                elif isinstance(reserved, (int, float)):
                    reserved_qty = reserved
                unfulfillable = inv.get("unfulfillableQuantity", {})
                unfulfillable_qty = 0
                if isinstance(unfulfillable, dict):
                    unfulfillable_qty = unfulfillable.get("totalUnfulfillableQuantity", 0)
                elif isinstance(unfulfillable, (int, float)):
                    unfulfillable_qty = unfulfillable

                con.execute("""
                    INSERT OR REPLACE INTO fba_inventory
                    (date, asin, sku, product_name, condition,
                     fulfillable_quantity, inbound_working_quantity,
                     inbound_shipped_quantity, inbound_receiving_quantity,
                     reserved_quantity, unfulfillable_quantity, total_quantity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    today,
                    item.get("asin", ""),
                    item.get("sellerSku", item.get("fnSku", "")),
                    item.get("productName", "")[:200],
                    item.get("condition", "NewItem"),
                    int(fulfillable or 0),
                    int(inbound_working or 0),
                    int(inbound_shipped or 0),
                    int(inbound_receiving or 0),
                    int(reserved_qty or 0),
                    int(unfulfillable_qty or 0),
                    int(item.get("totalQuantity", 0)),
                ])
                records += 1

            con.close()
            print(f"  Stored {records} inventory records via Inventories API")
            return records

    except Exception as e:
        print(f"  Inventories API error: {e}")
        print(f"  Falling back to report-based approach...")

    # Fallback: try report
    try:
        reports = Reports(credentials=credentials, marketplace=Marketplaces.US)

        report_response = reports.create_report(
            reportType="GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA"
        )
        report_id = report_response.payload.get("reportId")
        print(f"Report requested: {report_id}")

        for attempt in range(30):
            time.sleep(10)
            status_response = reports.get_report(report_id)
            status = status_response.payload.get("processingStatus")
            print(f"  Status: {status}")

            if status == "DONE":
                doc_id = status_response.payload.get("reportDocumentId")
                doc_response = reports.get_report_document(doc_id, decrypt=True)

                # Parse TSV report - handle both string and dict payloads
                import csv
                import io

                payload = doc_response.payload
                if isinstance(payload, dict):
                    # Try to get the document content from the dict
                    doc_content = payload.get("document", payload.get("body", ""))
                    if not doc_content and "url" in payload:
                        import requests as req
                        resp = req.get(payload["url"])
                        if payload.get("compressionAlgorithm", "").upper() == "GZIP":
                            doc_content = gzip.decompress(resp.content).decode("utf-8")
                        else:
                            doc_content = resp.text
                    if not doc_content:
                        doc_content = str(payload)
                else:
                    doc_content = str(payload)

                reader = csv.DictReader(io.StringIO(doc_content), delimiter='\t')

                con = duckdb.connect(str(db_path))
                today = datetime.now().date()
                records = 0

                for row in reader:
                    con.execute("""
                        INSERT OR REPLACE INTO fba_inventory
                        (date, asin, sku, product_name, condition,
                         fulfillable_quantity, inbound_working_quantity,
                         inbound_shipped_quantity, inbound_receiving_quantity,
                         reserved_quantity, unfulfillable_quantity, total_quantity)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        today,
                        row.get("asin", ""),
                        row.get("sku", ""),
                        row.get("product-name", "")[:200],
                        row.get("condition", ""),
                        int(row.get("afn-fulfillable-quantity", 0) or 0),
                        int(row.get("afn-inbound-working-quantity", 0) or 0),
                        int(row.get("afn-inbound-shipped-quantity", 0) or 0),
                        int(row.get("afn-inbound-receiving-quantity", 0) or 0),
                        int(row.get("afn-reserved-quantity", 0) or 0),
                        int(row.get("afn-unsellable-quantity", 0) or 0),
                        int(row.get("afn-fulfillable-quantity", 0) or 0) +
                        int(row.get("afn-reserved-quantity", 0) or 0) +
                        int(row.get("afn-inbound-shipped-quantity", 0) or 0) +
                        int(row.get("afn-inbound-receiving-quantity", 0) or 0),
                    ])
                    records += 1

                con.close()
                print(f"Stored {records} inventory records")
                return records

            elif status in ("CANCELLED", "FATAL"):
                print(f"  Report failed: {status}")
                return 0

        print("  Timed out")
        return 0

    except Exception as e:
        print(f"Error: {e}")
        return 0


def pull_financial_events(credentials, db_path, days_back=7):
    """Pull financial events (fees, commissions, net proceeds) from SP-API."""
    print(f"\n{'='*60}")
    print("PULLING FINANCIAL EVENTS")
    print(f"{'='*60}")

    start_date = datetime.utcnow() - timedelta(days=days_back)
    print(f"Financial events after: {start_date.date()}")

    try:
        finances = Finances(credentials=credentials, marketplace=Marketplaces.US)
        response = finances.list_financial_events(
            PostedAfter=start_date.isoformat(),
            MaxResultsPerPage=100
        )

        events = response.payload.get("FinancialEvents", {})
        shipment_events = events.get("ShipmentEventList", [])

        con = duckdb.connect(str(db_path))
        records = 0

        for event in shipment_events:
            order_id = event.get("AmazonOrderId", "")
            posted_date = event.get("PostedDate", "")

            for item in event.get("ShipmentItemList", []):
                sku = item.get("SellerSKU", "")

                # Sum up charge components
                product_charges = sum(
                    float(c.get("ChargeAmount", {}).get("Amount", 0))
                    for c in item.get("ItemChargeList", [])
                    if c.get("ChargeType") == "Principal"
                )
                shipping_charges = sum(
                    float(c.get("ChargeAmount", {}).get("Amount", 0))
                    for c in item.get("ItemChargeList", [])
                    if c.get("ChargeType") == "ShippingCharge"
                )
                fba_fees = sum(
                    abs(float(f.get("FeeAmount", {}).get("Amount", 0)))
                    for f in item.get("ItemFeeList", [])
                    if "FBA" in f.get("FeeType", "")
                )
                commission = sum(
                    abs(float(f.get("FeeAmount", {}).get("Amount", 0)))
                    for f in item.get("ItemFeeList", [])
                    if f.get("FeeType") == "Commission"
                )
                promo = sum(
                    float(p.get("PromotionAmount", {}).get("Amount", 0))
                    for p in item.get("PromotionList", [])
                )

                net = product_charges + shipping_charges - fba_fees - commission + promo

                con.execute("""
                    INSERT INTO financial_events
                    (date, asin, sku, order_id, event_type,
                     product_charges, shipping_charges, fba_fees,
                     commission, promotion_amount, net_proceeds)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    posted_date[:10] if posted_date else None,
                    item.get("SellerSKU", ""),
                    sku, order_id, "Shipment",
                    product_charges, shipping_charges,
                    fba_fees, commission, promo, net
                ])
                records += 1

        con.close()
        print(f"Stored {records} financial event records")
        return records

    except Exception as e:
        print(f"Error: {e}")
        return 0


def pull_advertising_report(credentials, db_path, days_back=7):
    """Pull Sponsored Products advertising report via SP-API Reports."""
    print(f"\n{'='*60}")
    print("PULLING ADVERTISING DATA (via SP-API Reports)")
    print(f"{'='*60}")

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days_back)
    print(f"Date range: {start_date} to {end_date}")

    try:
        reports = Reports(credentials=credentials, marketplace=Marketplaces.US)

        # Request Sponsored Products report
        # Try multiple report types as availability varies
        report_types = [
            "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL",
        ]

        # First try the ads-specific approach via financial events
        print("  Attempting to extract ad data from financial events...")
        result = pull_advertising_from_financials(credentials, db_path, days_back)
        if result > 0:
            return result

        # If no financial ad data, note that Ads API is needed
        print("  No advertising data found in financial events.")
        print("  For full ad metrics (impressions, clicks, ACOS), set up the Amazon Ads API.")
        return 0

        # Below is placeholder for future Ads API report type
        report_response = reports.create_report(
            reportType="GET_EPR_MONTHLY_BY_ASIN",
            dataStartTime=start_date.isoformat(),
            dataEndTime=end_date.isoformat(),
        )

        report_id = report_response.payload.get("reportId")
        print(f"Report requested: {report_id}")

        # Poll for report completion
        for attempt in range(30):
            time.sleep(10)
            status_response = reports.get_report(report_id)
            status = status_response.payload.get("processingStatus")
            print(f"  Status: {status}")

            if status == "DONE":
                doc_id = status_response.payload.get("reportDocumentId")
                doc_response = reports.get_report_document(doc_id, decrypt=True)
                break
            elif status in ("CANCELLED", "FATAL"):
                print(f"  Report failed with status: {status}")
                print("  Note: Advertising reports may require Ads API access.")
                print("  Trying alternative report type...")
                return pull_advertising_from_financials(credentials, db_path, days_back)
        else:
            print("  Timed out waiting for report")
            return pull_advertising_from_financials(credentials, db_path, days_back)

        # Parse TSV report
        import csv
        import io
        reader = csv.DictReader(io.StringIO(doc_response.payload), delimiter='\t')

        con = duckdb.connect(str(db_path))
        records = 0

        for row in reader:
            impressions = int(row.get("impressions", 0) or 0)
            clicks = int(row.get("clicks", 0) or 0)
            spend = float(row.get("spend", 0) or 0)
            sales_val = float(row.get("sales", row.get("attributedSales14d", 0)) or 0)
            orders_val = int(row.get("orders", row.get("attributedConversions14d", 0)) or 0)
            units = int(row.get("unitsSold", row.get("attributedUnitsOrdered14d", 0)) or 0)

            acos = (spend / sales_val * 100) if sales_val > 0 else 0
            roas = (sales_val / spend) if spend > 0 else 0
            cpc = (spend / clicks) if clicks > 0 else 0
            ctr = (clicks / impressions * 100) if impressions > 0 else 0

            con.execute("""
                INSERT INTO advertising
                (date, campaign_id, campaign_name, campaign_type,
                 asin, impressions, clicks, spend, sales, orders,
                 units, acos, roas, cpc, ctr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                row.get("date", str(start_date)),
                row.get("campaignId", ""),
                row.get("campaignName", ""),
                row.get("campaignType", "Sponsored Products"),
                row.get("asin", row.get("advertisedAsin", "")),
                impressions, clicks, spend, sales_val, orders_val,
                units, acos, roas, cpc, ctr
            ])
            records += 1

        con.close()
        print(f"Stored {records} advertising records")
        return records

    except SellingApiException as e:
        print(f"SP-API Error: {e}")
        print("Falling back to financial events for ad spend estimation...")
        return pull_advertising_from_financials(credentials, db_path, days_back)
    except Exception as e:
        print(f"Error: {e}")
        print("Falling back to financial events for ad spend estimation...")
        return pull_advertising_from_financials(credentials, db_path, days_back)


def pull_advertising_from_financials(credentials, db_path, days_back=7):
    """Fallback: Estimate ad spend from financial events (ServiceFeeEventList)."""
    print(f"\n  FALLBACK: Extracting ad spend from financial events...")

    start_date = datetime.utcnow() - timedelta(days=days_back)

    try:
        finances = Finances(credentials=credentials, marketplace=Marketplaces.US)
        response = finances.list_financial_events(
            PostedAfter=start_date.isoformat(),
            MaxResultsPerPage=100
        )

        events = response.payload.get("FinancialEvents", {})
        service_fees = events.get("ServiceFeeEventList", [])

        con = duckdb.connect(str(db_path))
        records = 0
        daily_spend = {}

        for fee in service_fees:
            fee_reason = fee.get("FeeReason", "")
            if "Advertising" in fee_reason or "Sponsored" in fee_reason:
                posted = fee.get("PostedDate", "")[:10]
                fee_list = fee.get("FeeList", [])
                for f in fee_list:
                    amount = abs(float(f.get("FeeAmount", {}).get("Amount", 0)))
                    if posted in daily_spend:
                        daily_spend[posted] += amount
                    else:
                        daily_spend[posted] = amount

        for date_str, spend in daily_spend.items():
            con.execute("""
                INSERT INTO advertising
                (date, campaign_id, campaign_name, campaign_type,
                 impressions, clicks, spend, sales, orders, units,
                 acos, roas, cpc, ctr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                date_str, "aggregate", "All Campaigns (from financials)",
                "Sponsored Products", 0, 0, spend, 0, 0, 0, 0, 0, 0, 0
            ])
            records += 1

        con.close()
        if records > 0:
            print(f"  Stored {records} daily ad spend records from financial events")
        else:
            print("  No advertising fee events found in this period")
        return records

    except Exception as e:
        print(f"  Fallback error: {e}")
        return 0


# ─────────────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("GOLFGEN AMAZON DATA PIPELINE")
    print(f"Run time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    creds = load_credentials()
    sp_creds = get_sp_api_credentials(creds)
    db_path = PROJECT_ROOT / creds["DATABASE"]["path"]

    # Ensure data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize database
    init_database(db_path)

    # Determine lookback period
    days_back = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    print(f"\nLookback period: {days_back} days")

    # Pull all data sources
    results = {}
    results["sales"] = pull_sales_report(sp_creds, db_path, days_back)
    results["orders"] = pull_orders(sp_creds, db_path, days_back)
    results["inventory"] = pull_fba_inventory(sp_creds, db_path)
    results["financials"] = pull_financial_events(sp_creds, db_path, days_back)
    results["advertising"] = pull_advertising_report(sp_creds, db_path, days_back)

    # Summary
    print(f"\n{'='*60}")
    print("PULL SUMMARY")
    print(f"{'='*60}")
    for source, count in results.items():
        status = "OK" if count > 0 else "FAILED/EMPTY"
        print(f"  {source:15s}: {count:6d} records  [{status}]")

    # Log the pull
    con = duckdb.connect(str(db_path))
    con.execute("""
        INSERT INTO pull_log (pull_type, start_date, end_date, records_pulled, status)
        VALUES (?, ?, ?, ?, ?)
    """, [
        "full_pull",
        (datetime.utcnow() - timedelta(days=days_back)).date(),
        datetime.utcnow().date(),
        sum(results.values()),
        "success" if all(v > 0 for v in results.values()) else "partial"
    ])
    con.close()

    print(f"\nDone. Database: {db_path}")


if __name__ == "__main__":
    main()
