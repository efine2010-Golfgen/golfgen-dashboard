"""
core/data_contracts.py — GolfGen Analytics Platform Field Mapping & Data Contracts
====================================================================================
Canonical mapping between our database columns and the Amazon SP-API fields
they originated from. This file answers: "what does this column actually mean?"

RULES:
  1. Every column in every table has an entry here.
  2. When you add a new column, add it here first.
  3. When a bug occurs because a column name was wrong (event_type vs
     transaction_type, asin vs sku), this file is where you would have
     found the correct name without debugging.
  4. 'valid_values' lists every value the column can contain.
     If a value appears that is not listed here, it is a data quality issue.
  5. 'gotchas' documents every non-obvious behavior. Read these before
     writing any query against the column.

Sections:
  A. Transactional tables (orders, daily_sales, financial_events, etc.)
  B. Reference tables (item_master, fba_inventory)
  C. Analytics / staging tables
  D. System tables
  E. Cross-table join keys
  F. Enum value registries

Version 1.0 — March 14, 2026
"""

from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class ColumnDef:
    column_name:     str
    our_type:        str          # SQL type in PostgreSQL
    amazon_api_field: str         # Exact field name from Amazon API
    amazon_object:   str          # Which API object/report contains it
    data_source:     str          # SOURCES key from metrics.py
    definition:      str          # What it means
    valid_values:    list = field(default_factory=list)   # empty = free-form
    gotchas:         str  = ""    # Non-obvious behavior — READ BEFORE QUERYING
    example:         str  = ""


# ══════════════════════════════════════════════════════════════════════════════
# A. TRANSACTIONAL TABLES
# ══════════════════════════════════════════════════════════════════════════════

# ── TABLE: orders ─────────────────────────────────────────────────────────────
ORDERS = {
    "order_id": ColumnDef(
        column_name      = "order_id",
        our_type         = "VARCHAR PRIMARY KEY",
        amazon_api_field = "AmazonOrderId",
        amazon_object    = "Orders API — Order object",
        data_source      = "orders_api",
        definition       = "Unique Amazon-assigned order identifier. Format: 3-7-7 (e.g., 902-3159896-1390916).",
        gotchas          = "Always use as join key for order-level queries. Never join on seller_order_id.",
        example          = "902-3159896-1390916",
    ),
    "purchase_date": ColumnDef(
        column_name      = "purchase_date",
        our_type         = "VARCHAR",  # stored as ISO8601 string
        amazon_api_field = "PurchaseDate",
        amazon_object    = "Orders API — Order object",
        data_source      = "orders_api",
        definition       = "When the order was placed by the customer. Stored as UTC ISO8601 string.",
        gotchas          = (
            "CRITICAL: Stored as UTC. NEVER compare directly to a date string like '2026-03-14'. "
            "Always build Central-timezone boundaries: "
            "s_iso = datetime(y, m, d, 0, 0, 0, tzinfo=ZoneInfo('America/Chicago')).isoformat() "
            "and compare: purchase_date >= s_iso AND purchase_date <= e_iso. "
            "String comparison without timezone conversion causes wrong orders to appear in 'today'."
        ),
        example          = "2026-03-14T14:22:10Z",
    ),
    "order_status": ColumnDef(
        column_name      = "order_status",
        our_type         = "VARCHAR",
        amazon_api_field = "OrderStatus",
        amazon_object    = "Orders API — Order object",
        data_source      = "orders_api",
        definition       = "Current state of the order.",
        valid_values     = [
            "Pending", "Unshipped", "PartiallyShipped", "Shipped",
            "Canceled", "Unfulfillable", "InvoiceUnconfirmed", "PendingAvailability",
        ],
        gotchas          = (
            "Pending orders have OrderTotal = {} (empty dict) — NO revenue. "
            "To include Pending in today totals (matching Amazon SC behavior): "
            "estimate revenue using ASIN avg price × qty. See _orders_supplement() in sales.py. "
            "Filter confirmed revenue: order_status NOT IN ('Cancelled', 'Pending'). "
            "Note: Amazon uses 'Canceled' (one 'l') not 'Cancelled' (two 'l's) in the API."
        ),
        example          = "Shipped",
    ),
    "order_total": ColumnDef(
        column_name      = "order_total",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "OrderTotal.Amount",
        amazon_object    = "Orders API — Order object",
        data_source      = "orders_api",
        definition       = "The total charge for the order.",
        gotchas          = (
            "EMPTY ({}) for Pending orders — will be NULL or 0.0 in our DB. "
            "OPEN QUESTION: Does this include marketplace tax or not? "
            "Amazon's orderedProductSales (S&T report) EXCLUDES tax. "
            "Verify: SELECT AVG(order_total / number_of_items) for a known ASIN "
            "and compare to its list price. If higher, tax is included."
        ),
        example          = "29.99",
    ),
    "asin": ColumnDef(
        column_name      = "asin",
        our_type         = "VARCHAR",
        amazon_api_field = "ASIN",
        amazon_object    = "Orders API — OrderItem object (from GetOrderItems)",
        data_source      = "orders_api",
        definition       = "Amazon Standard Identification Number for the ordered product.",
        gotchas          = (
            "ASIN NOT sku — the orders table uses asin as the product identifier. "
            "SKU varies by seller account and is not reliable for joins. "
            "Use asin to join to item_master for division lookup. "
            "May be NULL if order has multiple items and GetOrderItems wasn't called."
        ),
        example          = "B09X4KJL2M",
    ),
    "number_of_items": ColumnDef(
        column_name      = "number_of_items",
        our_type         = "INTEGER",
        amazon_api_field = "NumberOfItems",
        amazon_object    = "Orders API — Order object",
        data_source      = "orders_api",
        definition       = "Total number of items in the order (sum across all order lines).",
        gotchas          = "May differ from unitsOrdered in S&T report due to multi-ASIN orders.",
        example          = "2",
    ),
    "division": ColumnDef(
        column_name      = "division",
        our_type         = "VARCHAR",
        amazon_api_field = "N/A — derived from item_master lookup",
        amazon_object    = "Internal",
        data_source      = "item_master",
        definition       = "Business division this order belongs to.",
        valid_values     = ["golf", "housewares", "unknown"],
        gotchas          = (
            "NEVER hardcode division. Always look up via item_master using asin. "
            "If ASIN not in item_master → 'unknown' → flag for Eric to tag."
        ),
        example          = "golf",
    ),
    "customer": ColumnDef(
        column_name      = "customer",
        our_type         = "VARCHAR",
        amazon_api_field = "N/A — set by sync function",
        amazon_object    = "Internal",
        data_source      = "orders_api",
        definition       = "The sales channel / retailer this order came from.",
        valid_values     = [
            "amazon", "walmart_marketplace", "walmart_stores", "shopify",
            "first_tee", "belk", "hobby_lobby", "albertsons", "family_dollar",
        ],
        example          = "amazon",
    ),
    "platform": ColumnDef(
        column_name      = "platform",
        our_type         = "VARCHAR",
        amazon_api_field = "N/A — set by sync function",
        amazon_object    = "Internal",
        data_source      = "orders_api",
        definition       = "The data source / integration method for this order.",
        valid_values     = ["sp_api", "walmart_api", "shopify_api", "scintilla", "excel_upload", "email"],
        example          = "sp_api",
    ),
}


# ── TABLE: daily_sales ────────────────────────────────────────────────────────
DAILY_SALES = {
    "date": ColumnDef(
        column_name      = "date",
        our_type         = "VARCHAR",  # stored as 'YYYY-MM-DD' string
        amazon_api_field = "date",
        amazon_object    = "GET_SALES_AND_TRAFFIC_REPORT — salesAndTrafficByDate",
        data_source      = "sales_and_traffic_report",
        definition       = "The calendar date this row represents. Central time.",
        gotchas          = (
            "Stored as VARCHAR 'YYYY-MM-DD'. Cast when doing date arithmetic: CAST(date AS DATE). "
            "Two writers: (1) Today Orders sync writes asin='ALL' row for today/yesterday. "
            "(2) S&T report sync writes all dates from report. "
            "CONFLICT RULE: For today/yesterday, S&T MUST NOT overwrite a higher orders_api value. "
            "See SOURCE_PRIORITY in metrics.py."
        ),
        example          = "2026-03-14",
    ),
    "asin": ColumnDef(
        column_name      = "asin",
        our_type         = "VARCHAR",
        amazon_api_field = "parentAsin / childAsin / skuAsin",
        amazon_object    = "GET_SALES_AND_TRAFFIC_REPORT — salesAndTrafficByAsin",
        data_source      = "sales_and_traffic_report",
        definition       = "The ASIN for this row. Special value 'ALL' = aggregate across all ASINs for the date.",
        valid_values     = ["ALL", "<any valid ASIN>"],
        gotchas          = (
            "asin='ALL' is the aggregate date row — used for today/yesterday revenue totals. "
            "asin=<specific> rows come from per-ASIN S&T report — have 24hr lag. "
            "For today's total revenue, use asin='ALL'. "
            "For per-ASIN revenue today, use orders table (asin != 'ALL' rows will be 0 for today)."
        ),
        example          = "ALL",
    ),
    "ordered_product_sales": ColumnDef(
        column_name      = "ordered_product_sales",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "orderedProductSales.amount",
        amazon_object    = "GET_SALES_AND_TRAFFIC_REPORT",
        data_source      = "sales_and_traffic_report",
        definition       = (
            "Revenue from ordered items = price × qty ordered. "
            "EXCLUDES shipping and tax per Amazon definition."
        ),
        gotchas          = (
            "For today: populated by Today Orders sync (orders_api), NOT S&T report. "
            "S&T value for today would be partial or missing. "
            "May be tax-exclusive while our orders.order_total might be tax-inclusive — verify."
        ),
        example          = "1878.39",
    ),
    "units_ordered": ColumnDef(
        column_name      = "units_ordered",
        our_type         = "INTEGER",
        amazon_api_field = "unitsOrdered",
        amazon_object    = "GET_SALES_AND_TRAFFIC_REPORT",
        data_source      = "sales_and_traffic_report",
        definition       = "Total units ordered by customers in the period.",
        gotchas          = "For today: use SUM(number_of_items) from orders table instead.",
        example          = "11",
    ),
    "sessions": ColumnDef(
        column_name      = "sessions",
        our_type         = "INTEGER",
        amazon_api_field = "sessions",
        amazon_object    = "GET_SALES_AND_TRAFFIC_REPORT",
        data_source      = "sales_and_traffic_report",
        definition       = "Unique visitors to the product listings.",
        gotchas          = "24hr lag. Will be 0 or NULL for today — show dash, never estimate.",
        example          = "342",
    ),
    "page_views": ColumnDef(
        column_name      = "page_views",
        our_type         = "INTEGER",
        amazon_api_field = "pageViews",
        amazon_object    = "GET_SALES_AND_TRAFFIC_REPORT",
        data_source      = "sales_and_traffic_report",
        definition       = "Total page view events (glance views).",
        gotchas          = "24hr lag. Never show for today.",
        example          = "510",
    ),
}


# ── TABLE: financial_events ───────────────────────────────────────────────────
FINANCIAL_EVENTS = {
    "event_type": ColumnDef(
        column_name      = "event_type",
        our_type         = "VARCHAR",
        amazon_api_field = "Event category (inferred from which list the event appears in)",
        amazon_object    = "Finances API v0 — ListFinancialEvents response",
        data_source      = "finances_api",
        definition       = "The category of financial event. Maps to Amazon's event collection name.",
        valid_values     = [
            "ShipmentEvent",
            "RefundEvent",
            "ServiceFeeEvent",
            "AdjustmentEvent",
            "SellerDealPaymentEvent",
            "ProductAdsPaymentEvent",
            "RentalTransactionEvent",
        ],
        gotchas          = (
            "CRITICAL: This column is event_type NOT transaction_type. "
            "This was bug #1 in Phase 1. "
            "To filter refunds: event_type ILIKE '%refund%' → matches 'RefundEvent'. "
            "ServiceFeeEvents include monthly storage fees — may be filtered out "
            "by current queries that look for revenue/fee events only."
        ),
        example          = "ShipmentEvent",
    ),
    "charge_type": ColumnDef(
        column_name      = "charge_type",
        our_type         = "VARCHAR",
        amazon_api_field = "ChargeType (from ItemCharges list within ShipmentEvent)",
        amazon_object    = "Finances API v0 — ShipmentEvent.ItemCharges",
        data_source      = "finances_api",
        definition       = "The specific type of charge within a ShipmentEvent.",
        valid_values     = [
            "Principal", "Tax", "MarketplaceFacilitatorTax-Principal",
            "MarketplaceFacilitatorTax-Shipping", "ShippingCharge", "GiftWrap", "Discount",
        ],
        gotchas          = "Principal = the selling price. Tax is remitted by Amazon (MFT) — not our revenue.",
        example          = "Principal",
    ),
    "commission": ColumnDef(
        column_name      = "commission",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "FeeType: ReferralFee (also appears as 'Commission' in some events)",
        amazon_object    = "Finances API v0 — ShipmentEvent.ItemFees",
        data_source      = "finances_api",
        definition       = "Amazon referral fee (their sales commission). Typically 8-17% of selling price.",
        gotchas          = (
            "Stored as a NEGATIVE number (money out). Use ABS(commission) in SUM. "
            "Maps to Amazon's FeeType: ReferralFee. "
            "Amazon uses both 'ReferralFee' and 'Commission' as FeeType names for the same charge."
        ),
        example          = "-4.50",
    ),
    "fba_fees": ColumnDef(
        column_name      = "fba_fees",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "FeeType: FBAPerUnitFulfillmentFee + FBAPerOrderFulfillmentFee + FBAWeightBasedFee",
        amazon_object    = "Finances API v0 — ShipmentEvent.ItemFees",
        data_source      = "finances_api",
        definition       = "FBA fulfillment fees charged for picking, packing, and shipping the order.",
        gotchas          = (
            "Stored as a NEGATIVE number. Use ABS(fba_fees) in SUM. "
            "CURRENTLY TOO BROAD: lumps per-unit + per-order + weight-based fees together. "
            "Does NOT include storage fees (ServiceFeeEvents) — those are separate events "
            "and may not be captured in this column at all. "
            "Phase 4 improvement: split into fba_fulfillment_fee vs fba_storage_fee columns."
        ),
        example          = "-3.85",
    ),
    "amount": ColumnDef(
        column_name      = "amount",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "CurrencyAmount (extracted by _money() helper)",
        amazon_object    = "Finances API v0 — various Money objects",
        data_source      = "finances_api",
        definition       = "The monetary value of this financial event.",
        gotchas          = (
            "Parsed by _money() helper in sp_api.py which extracts CurrencyAmount from dict. "
            "Refund amounts are NEGATIVE (money returned to buyer)."
        ),
        example          = "29.99",
    ),
}


# ── TABLE: fba_inventory ──────────────────────────────────────────────────────
FBA_INVENTORY = {
    "asin": ColumnDef(
        column_name      = "asin",
        our_type         = "VARCHAR",
        amazon_api_field = "asin",
        amazon_object    = "FBA Inventory API — InventorySummary",
        data_source      = "fba_inventory_api",
        definition       = "Amazon ASIN for this inventory item.",
        example          = "B09X4KJL2M",
    ),
    "sku": ColumnDef(
        column_name      = "sku",
        our_type         = "VARCHAR",
        amazon_api_field = "sellerSku",
        amazon_object    = "FBA Inventory API — InventorySummary",
        data_source      = "fba_inventory_api",
        definition       = "Seller's own product identifier. Use asin for joins — sku is display only.",
        gotchas          = "sku != asin. Never join on sku across tables. Always join on asin.",
        example          = "GGWMSS2115BM",
    ),
    "product_name": ColumnDef(
        column_name      = "product_name",
        our_type         = "VARCHAR",
        amazon_api_field = "productName",
        amazon_object    = "FBA Inventory API — InventorySummary",
        data_source      = "fba_inventory_api",
        definition       = "Full product name as stored in Amazon's fulfillment system.",
        example          = "PGA TOUR 12 PC Foam Ball Set",
    ),
    "fulfillable_quantity": ColumnDef(
        column_name      = "fulfillable_quantity",
        our_type         = "INTEGER",
        amazon_api_field = "inventoryDetails.fulfillableQuantity",
        amazon_object    = "FBA Inventory API — InventoryDetails",
        data_source      = "fba_inventory_api",
        definition       = "Units available to ship immediately from Amazon FCs.",
        gotchas          = (
            "CRITICAL: FBA Inventory API returns CURRENT STATE ONLY — no history. "
            "Must snapshot daily to build history for Sell-Through Rate, trend charts. "
            "Add nightly snapshot job in scheduler.py (Phase 4)."
        ),
        example          = "142",
    ),
    "inbound_working_quantity": ColumnDef(
        column_name      = "inbound_working_quantity",
        our_type         = "INTEGER",
        amazon_api_field = "inventoryDetails.inboundWorkingQuantity",
        amazon_object    = "FBA Inventory API",
        data_source      = "fba_inventory_api",
        definition       = "Units in shipments that Amazon has not yet received.",
        example          = "288",
    ),
    "inbound_shipped_quantity": ColumnDef(
        column_name      = "inbound_shipped_quantity",
        our_type         = "INTEGER",
        amazon_api_field = "inventoryDetails.inboundShippedQuantity",
        amazon_object    = "FBA Inventory API",
        data_source      = "fba_inventory_api",
        definition       = "Units in shipments Amazon has received but not checked in.",
        example          = "0",
    ),
    "inbound_receiving_quantity": ColumnDef(
        column_name      = "inbound_receiving_quantity",
        our_type         = "INTEGER",
        amazon_api_field = "inventoryDetails.inboundReceivingQuantity",
        amazon_object    = "FBA Inventory API",
        data_source      = "fba_inventory_api",
        definition       = "Units currently being checked in at Amazon FCs.",
        example          = "0",
    ),
    "reserved_quantity": ColumnDef(
        column_name      = "reserved_quantity",
        our_type         = "INTEGER",
        amazon_api_field = "inventoryDetails.reservedQuantity.totalReservedQuantity",
        amazon_object    = "FBA Inventory API",
        data_source      = "fba_inventory_api",
        definition       = "Units reserved for pending customer orders or FC transfers.",
        example          = "5",
    ),
    "unfulfillable_quantity": ColumnDef(
        column_name      = "unfulfillable_quantity",
        our_type         = "INTEGER",
        amazon_api_field = "inventoryDetails.unfulfillableQuantity.totalUnfulfillableQuantity",
        amazon_object    = "FBA Inventory API",
        data_source      = "fba_inventory_api",
        definition       = "Units that cannot be sold (damaged, defective, expired, etc.).",
        example          = "3",
    ),
}


# ── TABLE: advertising / ads_campaigns ────────────────────────────────────────
ADVERTISING = {
    "campaign_id": ColumnDef(
        column_name      = "campaign_id",
        our_type         = "VARCHAR",
        amazon_api_field = "campaignId",
        amazon_object    = "Amazon Advertising API — Campaign object",
        data_source      = "ads_api",
        definition       = "Unique ID for the ad campaign.",
        example          = "123456789",
    ),
    "spend": ColumnDef(
        column_name      = "spend",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "spend",
        amazon_object    = "Amazon Advertising API — Campaign metrics",
        data_source      = "ads_api",
        definition       = "Total ad spend for the period.",
        gotchas          = "1-3 hour lag. Today spend may be incomplete.",
        example          = "45.82",
    ),
    "attributed_sales": ColumnDef(
        column_name      = "attributed_sales",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "attributedSales14d",
        amazon_object    = "Amazon Advertising API — Campaign metrics",
        data_source      = "ads_api",
        definition       = "Sales attributed to ads within a 14-day attribution window.",
        gotchas          = "14-day attribution window — today's attributed sales will grow for 14 days.",
        example          = "312.45",
    ),
    "impressions": ColumnDef(
        column_name      = "impressions",
        our_type         = "INTEGER",
        amazon_api_field = "impressions",
        amazon_object    = "Amazon Advertising API",
        data_source      = "ads_api",
        definition       = "Number of times ads were displayed.",
        example          = "4820",
    ),
    "clicks": ColumnDef(
        column_name      = "clicks",
        our_type         = "INTEGER",
        amazon_api_field = "clicks",
        amazon_object    = "Amazon Advertising API",
        data_source      = "ads_api",
        definition       = "Number of clicks on the ads.",
        example          = "112",
    ),
}


# ── TABLE: item_master ────────────────────────────────────────────────────────
ITEM_MASTER = {
    "asin": ColumnDef(
        column_name      = "asin",
        our_type         = "VARCHAR PRIMARY KEY",
        amazon_api_field = "ASIN",
        amazon_object    = "Multiple — FBA Inventory, Orders API, S&T Report",
        data_source      = "item_master",
        definition       = "Primary key. The ASIN that all other tables join to for division lookup.",
        gotchas          = "SOURCE OF TRUTH for division mapping. If an ASIN is not here, division = 'unknown'.",
        example          = "B09X4KJL2M",
    ),
    "division": ColumnDef(
        column_name      = "division",
        our_type         = "VARCHAR NOT NULL",
        amazon_api_field = "N/A — manually assigned by Eric",
        amazon_object    = "Internal",
        data_source      = "item_master",
        definition       = "Business division this ASIN belongs to.",
        valid_values     = ["golf", "housewares"],
        gotchas          = "Must be set by Eric via the Item Master tab. New ASINs default to 'unknown'.",
        example          = "golf",
    ),
}


# ══════════════════════════════════════════════════════════════════════════════
# B. SYSTEM TABLES
# ══════════════════════════════════════════════════════════════════════════════

SYNC_LOG = {
    "records_processed": ColumnDef(
        column_name      = "records_processed",
        our_type         = "INTEGER",
        amazon_api_field = "N/A — internal counter",
        amazon_object    = "Internal",
        data_source      = "orders_api",
        definition       = "Number of records processed in this sync run.",
        gotchas          = "Column is records_processed NOT records_inserted. This was a known bug.",
        example          = "47",
    ),
    "execution_time_seconds": ColumnDef(
        column_name      = "execution_time_seconds",
        our_type         = "DOUBLE PRECISION",
        amazon_api_field = "N/A — internal timer",
        amazon_object    = "Internal",
        data_source      = "orders_api",
        definition       = "How long the sync took to run.",
        gotchas          = "Column is execution_time_seconds NOT duration_seconds.",
        example          = "12.4",
    ),
}


# ══════════════════════════════════════════════════════════════════════════════
# C. JOIN KEY REGISTRY
#    When joining across tables, always use these keys.
#    NEVER join on sku, seller_sku, or product_name — these change.
# ══════════════════════════════════════════════════════════════════════════════

JOIN_KEYS = {
    "orders → item_master":             ("orders.asin", "item_master.asin"),
    "orders → financial_events":        ("orders.order_id", "financial_events.order_id"),
    "daily_sales → item_master":        ("daily_sales.asin", "item_master.asin"),
    "fba_inventory → item_master":      ("fba_inventory.asin", "item_master.asin"),
    "advertising → orders":             "No direct join — advertising uses ASIN-level attribution",
    "financial_events → daily_sales":   "No direct join — use date + division for aggregation only",
}


# ══════════════════════════════════════════════════════════════════════════════
# D. ENUM VALUE REGISTRIES
#    Complete list of valid values for categorical columns.
#    If a value appears that is not listed here → data quality issue.
# ══════════════════════════════════════════════════════════════════════════════

ORDER_STATUS_VALUES = [
    "Pending",              # No OrderTotal. Revenue unknown. Estimate using ASIN avg price.
    "Unshipped",            # Confirmed. OrderTotal present.
    "PartiallyShipped",     # Confirmed. OrderTotal present.
    "Shipped",              # Confirmed and fulfilled. OrderTotal present.
    "Canceled",             # No revenue. Exclude from all revenue queries.
    "Unfulfillable",        # Cannot be fulfilled. Exclude from revenue.
    "InvoiceUnconfirmed",   # B2B — invoice not yet confirmed. Include in totals.
    "PendingAvailability",  # Pre-order not yet released. Treat like Pending.
]

EVENT_TYPE_VALUES = [
    "ShipmentEvent",            # Per-order charges and fees — the primary revenue event
    "RefundEvent",              # Customer return/refund — negative revenue
    "ServiceFeeEvent",          # Monthly storage, subscription, removal fees
    "AdjustmentEvent",          # Reimbursements, reserve events, postage
    "SellerDealPaymentEvent",   # Lightning deal fees
    "ProductAdsPaymentEvent",   # Ad charges (rare — usually via Ads API)
    "RentalTransactionEvent",   # Not applicable for us
]

DIVISION_VALUES = ["golf", "housewares", "unknown"]

CUSTOMER_VALUES = [
    "amazon", "walmart_marketplace", "walmart_stores", "shopify",
    "first_tee", "belk", "hobby_lobby", "albertsons", "family_dollar",
]

PLATFORM_VALUES = [
    "sp_api",          # Amazon Seller Central via SP-API
    "walmart_api",     # Walmart Marketplace API (Phase 5)
    "shopify_api",     # Shopify API (Phase 5)
    "scintilla",       # Walmart Stores report tool (Phase 4)
    "excel_upload",    # Manual Excel/CSV upload
    "email",           # Email-based ingestion
]


# ══════════════════════════════════════════════════════════════════════════════
# E. KNOWN BUGS RESOLVED — Reference for future developers
#    When the same bug class reappears, check here first.
# ══════════════════════════════════════════════════════════════════════════════

RESOLVED_BUGS = [
    {
        "bug_id": "P1-001",
        "symptom": "Returns showing $0 on profitability tab",
        "root_cause": "Query used event_type = 'transaction_type' — column does not exist.",
        "fix": "Use financial_events.event_type ILIKE '%refund%'",
        "file": "sales.py, profitability.py",
        "column_affected": "financial_events.event_type",
    },
    {
        "bug_id": "P1-003",
        "symptom": "500 error on SKU queries in sales tab",
        "root_cause": "Query referenced orders.sku — column does not exist. Orders table uses asin.",
        "fix": "Use orders.asin NOT orders.sku",
        "file": "sales.py — _build_product_list()",
        "column_affected": "orders.asin",
    },
    {
        "bug_id": "P2-001",
        "symptom": "Today revenue showed $149 after S&T sync ran",
        "root_cause": "S&T report sync did INSERT OR REPLACE on daily_sales asin='ALL' for today, "
                      "overwriting the higher value written by Today Orders sync.",
        "fix": "Added guard: if new_value < existing_value for today/yesterday, skip revenue update.",
        "file": "services/sp_api.py — S&T report sync loop",
        "column_affected": "daily_sales.ordered_product_sales WHERE asin='ALL'",
    },
    {
        "bug_id": "P2-002",
        "symptom": "Today shows $605 but Amazon SC shows $1,878",
        "root_cause": "Pending orders have no OrderTotal. Only Shipped orders counted.",
        "fix": "Added Pending estimation in _orders_supplement(): ASIN avg price × qty from orders table.",
        "file": "routers/sales.py — _orders_supplement()",
        "column_affected": "orders.order_status = 'Pending', orders.order_total",
    },
    {
        "bug_id": "P2-003",
        "symptom": "sync_log queries failing with 'column not found'",
        "root_cause": "Column is records_processed not records_inserted; execution_time_seconds not duration_seconds.",
        "fix": "See SYNC_LOG column definitions above.",
        "file": "routers/system.py",
        "column_affected": "sync_log.records_processed, sync_log.execution_time_seconds",
    },
]


# ══════════════════════════════════════════════════════════════════════════════
# F. HELPER ACCESSOR
# ══════════════════════════════════════════════════════════════════════════════

# All table contracts in one dict for easy lookup
ALL_TABLES = {
    "orders":           ORDERS,
    "daily_sales":      DAILY_SALES,
    "financial_events": FINANCIAL_EVENTS,
    "fba_inventory":    FBA_INVENTORY,
    "advertising":      ADVERTISING,
    "item_master":      ITEM_MASTER,
    "sync_log":         SYNC_LOG,
}


def get_column(table: str, column: str) -> ColumnDef:
    """Look up a column definition. Raises KeyError if not found."""
    tbl = ALL_TABLES.get(table)
    if not tbl:
        raise KeyError(f"Table '{table}' not in data_contracts. Available: {list(ALL_TABLES.keys())}")
    col = tbl.get(column)
    if not col:
        raise KeyError(
            f"Column '{column}' not in data_contracts for table '{table}'. "
            f"Available: {list(tbl.keys())}"
        )
    return col


def column_gotcha(table: str, column: str) -> str:
    """Quick access to the gotcha notes for a column. Returns '' if not defined."""
    try:
        return get_column(table, column).gotchas
    except KeyError:
        return ""
