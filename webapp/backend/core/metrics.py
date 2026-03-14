"""
core/metrics.py — GolfGen Analytics Platform Metric Registry
=============================================================
SINGLE SOURCE OF TRUTH for every KPI, data source, formula, conflict rule,
and data freshness expectation in the platform.

RULES (enforced by convention — violations become bugs):
  1. Every metric displayed on the frontend is defined here.
  2. Every formula lives here and NOWHERE ELSE. If a router calculates
     something not defined here, move the formula here.
  3. When two data sources report the same metric, SOURCE_PRIORITY controls
     which wins. Never hard-code conflict resolution in a router.
  4. All date comparisons use CENTRAL timezone. Always.
  5. 'today_available' drives what the UI shows for real-time periods.
     Never guess — check this dict first.

Sections:
  A. Source registry
  B. Source priority / conflict resolution rules
  C. P1 Metric definitions (revenue, units, fees, ads)
  D. P1 Inventory KPIs
  E. P1 Quality / account health KPIs
  F. P2 Derived / advanced KPIs
  G. Helper accessors

Version 1.0 — March 14, 2026
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any

# ══════════════════════════════════════════════════════════════════════════════
# A. DATA SOURCES
#    Canonical names used in SOURCE_PRIORITY and metric definitions.
# ══════════════════════════════════════════════════════════════════════════════

SOURCES = {
    # Real-time sources (2–5 min latency)
    "orders_api": {
        "label": "Orders API (GetOrders + GetOrderItems)",
        "latency_min": 2,
        "latency_max": 5,
        "today_available": True,
        "refresh_recommended_min": 15,
        "historical_depth_years": 2,
        "notes": (
            "Near real-time. OrderTotal is EMPTY ({}) for Pending orders — revenue "
            "only confirmed on Unshipped/Shipped. Use GetOrderItems for item-level prices."
        ),
    },
    "fba_inventory_api": {
        "label": "FBA Inventory API (GET /fba/inventory/v1/summaries)",
        "latency_min": 0,
        "latency_max": 1,
        "today_available": True,
        "refresh_recommended_min": 60,
        "historical_depth_years": 0,  # current state ONLY — must snapshot daily
        "notes": (
            "CRITICAL: Returns current state only. Snapshot daily to build history. "
            "Use details=true param for unfulfillable/reserved/inbound breakdowns."
        ),
    },
    "sales_and_traffic_report": {
        "label": "GET_SALES_AND_TRAFFIC_REPORT",
        "latency_min": 1440,   # ~24 hours in minutes
        "latency_max": 4320,   # up to 72 hours for DAY granularity
        "today_available": False,  # yesterday is the earliest
        "refresh_recommended_min": 1440,
        "historical_depth_years": 2,
        "notes": (
            "Amazon recalculates metrics for 7-14 days. Always pull last 14d lookback. "
            "orderedProductSales EXCLUDES shipping and tax. "
            "NEVER use for today — use orders_api instead."
        ),
    },
    "finances_api": {
        "label": "Finances API v0 (GET /finances/v0/financialEvents)",
        "latency_min": 30,
        "latency_max": 60,
        "today_available": "partial",  # ~1hr delay; last 48hr may be incomplete
        "refresh_recommended_min": 60,
        "historical_depth_years": 2,
        "notes": (
            "Financial events may NOT include last 48 hours. Always use 48hr lookback "
            "window for reconciliation. event_type maps to Amazon event categories: "
            "ShipmentEvent, RefundEvent, ServiceFeeEvent, AdjustmentEvent."
        ),
    },
    "finances_api_v2": {
        "label": "Finances API v2024-06-19 (transaction-level)",
        "latency_min": 30,
        "latency_max": 60,
        "today_available": "partial",
        "refresh_recommended_min": 60,
        "historical_depth_years": 2,
        "notes": (
            "New transaction-level API. Supports filter by order ID. "
            "More granular than v0. Phase 5 target for per-order P&L."
        ),
        "implementation_status": "planned_phase5",
    },
    "ads_api": {
        "label": "Amazon Advertising API",
        "latency_min": 60,
        "latency_max": 180,
        "today_available": "partial",
        "refresh_recommended_min": 120,
        "historical_depth_years": 2,
        "notes": "Separate from SP-API. Syncs every 2 hours.",
    },
    "brand_analytics": {
        "label": "Brand Analytics Reports",
        "latency_min": 4320,   # 3 days in minutes
        "latency_max": 7200,   # 5 days
        "today_available": False,
        "refresh_recommended_min": 10080,  # weekly
        "historical_depth_years": 1,
        "notes": "Requires Brand Registry + Brand Analytics SP-API role. Phase 6.",
        "implementation_status": "planned_phase6",
    },
    "sales_api_get_order_metrics": {
        "label": "Sales API (getOrderMetrics)",
        "latency_min": 2,
        "latency_max": 5,
        "today_available": True,
        "refresh_recommended_min": 15,
        "historical_depth_years": 2,
        "notes": (
            "Purpose-built aggregation endpoint. Returns today revenue in ~2 min. "
            "Sellerboard likely uses this for real-time today view. "
            "Evaluate as replacement for Pending estimation in Phase 5."
        ),
        "implementation_status": "planned_phase5",
    },
    "excel_upload": {
        "label": "Manual Excel/CSV Upload",
        "latency_min": None,
        "latency_max": None,
        "today_available": False,
        "notes": "Walmart Stores (Scintilla), Belk, Hobby Lobby, Albertsons, Family Dollar, First Tee.",
    },
    "item_master": {
        "label": "Item Master (internal reference table)",
        "latency_min": 0,
        "latency_max": 0,
        "today_available": True,
        "notes": "Source of truth for division mapping. Every ASIN lookup goes here first.",
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# B. SOURCE PRIORITY — CONFLICT RESOLUTION RULES
#    When two sources report the same metric, this table determines who wins.
#
#    Conflict rules:
#      "use_max"      — take the higher value (prevents underreporting)
#      "use_primary"  — trust the first source in the priority list
#      "use_finances" — use Finances API if > 0, else estimate
#      "estimate"     — no authoritative source; use defined estimation method
#      "no_data"      — do not show this metric for this period (show dash)
# ══════════════════════════════════════════════════════════════════════════════

SOURCE_PRIORITY = {
    # metric_name: {period_bucket: {"sources": [...], "rule": "...", "note": "..."}}
    #
    # period_bucket values:
    #   "today"     — current calendar day in Central time
    #   "yesterday" — previous calendar day in Central time
    #   "recent"    — 2–6 days ago (S&T still catching up)
    #   "historical"— 7+ days ago (S&T is fully reconciled)

    "ordered_product_sales": {
        "today": {
            "sources": ["orders_api", "sales_and_traffic_report"],
            "rule": "use_max",
            "note": (
                "S&T has 24hr lag — never use alone for today. "
                "Orders API is real-time but excludes Pending revenue. "
                "Use MAX of (orders_api shipped + pending_estimation) vs S&T. "
                "S&T MUST NOT overwrite a higher orders_api value — this was the bug."
            ),
            "pending_estimation": True,
        },
        "yesterday": {
            "sources": ["orders_api", "sales_and_traffic_report"],
            "rule": "use_max",
            "note": "S&T may not be finalized for yesterday. MAX still applies.",
            "pending_estimation": True,
        },
        "recent": {
            "sources": ["sales_and_traffic_report", "orders_api"],
            "rule": "use_primary",
            "note": "S&T is becoming authoritative after 24hr. Prefer S&T.",
        },
        "historical": {
            "sources": ["sales_and_traffic_report"],
            "rule": "use_primary",
            "note": "S&T is fully reconciled for historical dates. Sole source.",
        },
    },

    "units_ordered": {
        "today": {
            "sources": ["orders_api"],
            "rule": "use_primary",
            "note": "Sum number_of_items from orders table. S&T not available.",
        },
        "yesterday": {
            "sources": ["orders_api", "sales_and_traffic_report"],
            "rule": "use_max",
        },
        "historical": {
            "sources": ["sales_and_traffic_report"],
            "rule": "use_primary",
        },
    },

    "sessions": {
        "today": {
            "sources": [],
            "rule": "no_data",
            "note": "S&T is the ONLY source of session data and has 24hr lag. Show dash (—) for today.",
        },
        "yesterday": {
            "sources": ["sales_and_traffic_report"],
            "rule": "use_primary",
            "note": "S&T may not have yesterday yet — show dash if absent.",
        },
        "historical": {
            "sources": ["sales_and_traffic_report"],
            "rule": "use_primary",
        },
    },

    "page_views": {
        "today": {"sources": [], "rule": "no_data"},
        "yesterday": {"sources": ["sales_and_traffic_report"], "rule": "use_primary"},
        "historical": {"sources": ["sales_and_traffic_report"], "rule": "use_primary"},
    },

    "amazon_fees": {
        "today": {
            "sources": ["finances_api"],
            "rule": "use_finances",
            "note": (
                "Finances API has 30-60min lag. If Finances shows $0 for today "
                "and revenue > 0, use estimated_fee_rate (27%) as fallback. "
                "Never show $0 fees when revenue is positive."
            ),
            "fallback_estimate_pct": 0.27,
        },
        "historical": {
            "sources": ["finances_api"],
            "rule": "use_primary",
            "note": "48hr lookback required for reconciliation — always re-pull last 2 days.",
        },
    },

    "ad_spend": {
        "today": {
            "sources": ["ads_api"],
            "rule": "use_primary",
            "note": "Ads API has 1-3hr lag. Today spend may be incomplete.",
        },
        "historical": {
            "sources": ["ads_api"],
            "rule": "use_primary",
        },
    },

    "fba_inventory": {
        "now": {
            "sources": ["fba_inventory_api"],
            "rule": "use_primary",
            "note": "Real-time. Always use live API value, never a cached date-based value.",
        },
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# C. METRIC DEFINITIONS
#    Each entry defines a KPI completely:
#      - canonical_name: used in code everywhere
#      - display_name:   shown to the user
#      - category:       grouping for the UI (Sales, Advertising, Inventory, etc.)
#      - dashboard_level: Executive / Product / Advertising / Inventory / Quality
#      - priority:       P1 = must ship, P2 = important, P3 = nice-to-have
#      - formula:        SQL expression (use canonical column names)
#      - source_fields:  list of (source, field_name) pairs
#      - today_available: True / False / "estimate" / "no_data"
#      - null_rule:      "show_zero" / "show_dash" / "use_estimate" / "use_fallback_pct"
#      - alert_threshold: optional dict for anomaly detection (Phase 6)
#      - implementation_status: "live" / "partial" / "planned_phaseN"
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class MetricDef:
    canonical_name:        str
    display_name:          str
    category:              str
    dashboard_level:       str
    priority:              str           # P1 / P2 / P3
    formula:               str           # SQL expression
    source_fields:         list[tuple]   # [(source_key, amazon_field_name), ...]
    today_available:       Any           # True / False / "estimate" / "no_data"
    null_rule:             str           # show_zero / show_dash / use_estimate / use_fallback_pct
    unit:                  str = "USD"   # USD / count / pct / ratio / days / weeks
    implementation_status: str = "live"
    alert_threshold:       dict = field(default_factory=dict)
    notes:                 str = ""


METRICS: dict[str, MetricDef] = {}

def _reg(m: MetricDef):
    METRICS[m.canonical_name] = m
    return m


# ── C1. SALES METRICS ─────────────────────────────────────────────────────────

_reg(MetricDef(
    canonical_name   = "ordered_product_sales",
    display_name     = "Ordered Product Sales",
    category         = "Sales",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "SUM(ordered_product_sales)",  # from daily_sales or orders supplement
    source_fields    = [
        ("sales_and_traffic_report", "orderedProductSales"),
        ("orders_api",               "OrderTotal.Amount"),
    ],
    today_available  = "estimate",
    null_rule        = "use_estimate",
    notes            = (
        "For today/yesterday: use MAX(orders_api_shipped + pending_est, S&T asin=ALL). "
        "orderedProductSales EXCLUDES shipping and tax per Amazon definition. "
        "Verify whether our order_total is tax-inclusive (it may be). "
        "See SOURCE_PRIORITY['ordered_product_sales'] for conflict rules."
    ),
))

_reg(MetricDef(
    canonical_name   = "net_revenue",
    display_name     = "Net Revenue",
    category         = "Sales",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "SUM(ordered_product_sales) - SUM(refunded_product_sales)",
    source_fields    = [
        ("sales_and_traffic_report", "orderedProductSales"),
        ("sales_and_traffic_report", "N/A — compute from financial_events RefundEvents"),
        ("finances_api",             "RefundEvent.ItemChargeAdjustments"),
    ],
    today_available  = "estimate",
    null_rule        = "use_estimate",
    notes            = "Use financial_events WHERE event_type ILIKE '%refund%' for refund amounts.",
))

_reg(MetricDef(
    canonical_name   = "units_ordered",
    display_name     = "Units Ordered",
    category         = "Sales",
    dashboard_level  = "Product",
    priority         = "P1",
    formula          = "SUM(units_ordered)",
    source_fields    = [
        ("sales_and_traffic_report", "unitsOrdered"),
        ("orders_api",               "NumberOfItems"),  # fallback for today
    ],
    today_available  = True,
    null_rule        = "show_zero",
))

_reg(MetricDef(
    canonical_name   = "average_selling_price",
    display_name     = "Average Selling Price (ASP)",
    category         = "Sales",
    dashboard_level  = "Product",
    priority         = "P1",
    formula          = "SUM(ordered_product_sales) / NULLIF(SUM(units_ordered), 0)",
    source_fields    = [("sales_and_traffic_report", "orderedProductSales / unitsOrdered")],
    today_available  = "estimate",
    null_rule        = "show_dash",
    unit             = "USD",
    alert_threshold  = {"below_expected": True},
))

_reg(MetricDef(
    canonical_name   = "average_order_value",
    display_name     = "Average Order Value (AOV)",
    category         = "Sales",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "SUM(ordered_product_sales) / NULLIF(COUNT(DISTINCT order_id), 0)",
    source_fields    = [("orders_api", "OrderTotal.Amount")],
    today_available  = "estimate",
    null_rule        = "show_dash",
    unit             = "USD",
))

_reg(MetricDef(
    canonical_name   = "revenue_growth_vs_ly",
    display_name     = "Revenue Growth vs. Last Year",
    category         = "Sales",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "(ty_sales - ly_sales) / NULLIF(ly_sales, 0)",
    source_fields    = [("sales_and_traffic_report", "orderedProductSales (year-over-year)"),
                        ("orders_api",               "OrderTotal — for today/yesterday")],
    today_available  = "estimate",
    null_rule        = "show_dash",
    unit             = "pct",
    alert_threshold  = {"negative": "alert"},
))

_reg(MetricDef(
    canonical_name   = "sales_velocity_7d",
    display_name     = "Sales Velocity (7-day avg)",
    category         = "Sales",
    dashboard_level  = "Inventory",
    priority         = "P1",
    formula          = "SUM(ordered_product_sales WHERE date >= today-7) / 7.0",
    source_fields    = [("sales_and_traffic_report", "orderedProductSales (daily)")],
    today_available  = True,
    null_rule        = "show_zero",
    unit             = "USD",
    notes            = "Used as basis for Days of Cover and Reorder Point calculations.",
))

_reg(MetricDef(
    canonical_name   = "sales_velocity_30d",
    display_name     = "Sales Velocity (30-day avg)",
    category         = "Sales",
    dashboard_level  = "Inventory",
    priority         = "P1",
    formula          = "SUM(ordered_product_sales WHERE date >= today-30) / 30.0",
    source_fields    = [("sales_and_traffic_report", "orderedProductSales (daily)")],
    today_available  = True,
    null_rule        = "show_zero",
    unit             = "USD",
))

_reg(MetricDef(
    canonical_name   = "units_velocity_7d",
    display_name     = "Units Velocity (7-day avg daily units)",
    category         = "Sales",
    dashboard_level  = "Inventory",
    priority         = "P1",
    formula          = "SUM(units_ordered WHERE date >= today-7) / 7.0",
    source_fields    = [("sales_and_traffic_report", "unitsOrdered (daily)")],
    today_available  = True,
    null_rule        = "show_zero",
    unit             = "count",
    notes            = "Primary input for Days of Cover and Reorder Point.",
))


# ── C2. TRAFFIC / CONVERSION METRICS ─────────────────────────────────────────

_reg(MetricDef(
    canonical_name   = "sessions",
    display_name     = "Sessions",
    category         = "Traffic",
    dashboard_level  = "Product",
    priority         = "P2",
    formula          = "SUM(sessions)",
    source_fields    = [("sales_and_traffic_report", "sessions")],
    today_available  = False,  # 24hr lag — show dash for today
    null_rule        = "show_dash",
    unit             = "count",
    notes            = "NEVER estimate sessions. Only S&T has this data. Show — for today.",
))

_reg(MetricDef(
    canonical_name   = "page_views",
    display_name     = "Page Views (Glance Views)",
    category         = "Traffic",
    dashboard_level  = "Product",
    priority         = "P2",
    formula          = "SUM(page_views)",
    source_fields    = [("sales_and_traffic_report", "pageViews")],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "count",
))

_reg(MetricDef(
    canonical_name   = "conversion_rate",
    display_name     = "Conversion Rate (Unit Session %)",
    category         = "Conversion",
    dashboard_level  = "Product",
    priority         = "P1",
    formula          = "SUM(units_ordered) / NULLIF(SUM(sessions), 0)",
    source_fields    = [("sales_and_traffic_report", "unitSessionPercentage")],
    today_available  = False,  # no sessions for today
    null_rule        = "show_dash",
    unit             = "pct",
))

_reg(MetricDef(
    canonical_name   = "buy_box_percentage",
    display_name     = "Buy Box %",
    category         = "Conversion",
    dashboard_level  = "Product",
    priority         = "P1",
    formula          = "AVG(featured_offer_percentage)",
    source_fields    = [("sales_and_traffic_report", "featuredOfferPercentage")],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "pct",
    alert_threshold  = {"below": 0.80},
))


# ── C3. PROFITABILITY / FEE METRICS ──────────────────────────────────────────

_reg(MetricDef(
    canonical_name   = "referral_fee",
    display_name     = "Referral Fee (Amazon Commission)",
    category         = "Profitability",
    dashboard_level  = "Product",
    priority         = "P1",
    formula          = "SUM(ABS(commission))",  # commission column = ReferralFee in SP-API
    source_fields    = [
        ("finances_api", "FeeType: ReferralFee"),
        ("finances_api", "FeeType: Commission"),  # alias
    ],
    today_available  = "partial",
    null_rule        = "use_fallback_pct",
    notes            = (
        "Our column 'commission' maps to Amazon's FeeType: ReferralFee. "
        "Typically 8-17% of selling price for sporting goods. "
        "Fallback estimate: 15% of ordered_product_sales."
    ),
))

_reg(MetricDef(
    canonical_name   = "fba_fulfillment_fee",
    display_name     = "FBA Fulfillment Fee",
    category         = "Profitability",
    dashboard_level  = "Product",
    priority         = "P1",
    formula          = "SUM(ABS(fba_fees))",
    source_fields    = [
        ("finances_api", "FeeType: FBAPerUnitFulfillmentFee"),
        ("finances_api", "FeeType: FBAPerOrderFulfillmentFee"),
        ("finances_api", "FeeType: FBAWeightBasedFee"),
    ],
    today_available  = "partial",
    null_rule        = "use_fallback_pct",
    notes            = (
        "Our 'fba_fees' column currently lumps per-unit + per-order + weight-based fees. "
        "Phase 4 improvement: split into fba_fulfillment_fee vs fba_storage_fee. "
        "Does NOT include storage fees (those are ServiceFeeEvents)."
    ),
))

_reg(MetricDef(
    canonical_name   = "fba_storage_fee",
    display_name     = "FBA Storage Fee",
    category         = "Profitability",
    dashboard_level  = "Product",
    priority         = "P2",
    formula          = "SUM(ABS(storage_fee))",  # future column
    source_fields    = [
        ("finances_api", "ServiceFeeEvent: FBAStorageFee"),
        ("finances_api", "ServiceFeeEvent: AgedInventorySurcharge"),
        ("finances_api", "ServiceFeeEvent: FBALongTermStorageFee"),
    ],
    today_available  = False,  # monthly charge
    null_rule        = "show_zero",
    implementation_status = "planned_phase4",
    notes            = (
        "Currently NOT captured in financial_events — these are ServiceFeeEvents. "
        "May be filtered out by current event_type query. Phase 4: add storage_fee column."
    ),
))

_reg(MetricDef(
    canonical_name   = "fba_reimbursement",
    display_name     = "FBA Inventory Reimbursement",
    category         = "Profitability",
    dashboard_level  = "Product",
    priority         = "P2",
    formula          = "SUM(reimbursement_amount)",  # future column
    source_fields    = [("finances_api", "AdjustmentEvent: FBAInventoryReimbursement")],
    today_available  = False,
    null_rule        = "show_zero",
    implementation_status = "planned_phase4",
    notes            = "When Amazon loses/damages inventory. Currently not captured — missing from P&L.",
))

_reg(MetricDef(
    canonical_name   = "total_amazon_fees",
    display_name     = "Total Amazon Fees",
    category         = "Profitability",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "SUM(ABS(fba_fees)) + SUM(ABS(commission))",
    source_fields    = [("finances_api", "All FeeTypes in ShipmentEvent")],
    today_available  = "partial",
    null_rule        = "use_fallback_pct",
    notes            = "Fallback estimate: 27% of ordered_product_sales (referral ~15% + FBA ~12%).",
))

_reg(MetricDef(
    canonical_name   = "contribution_margin",
    display_name     = "Contribution Margin",
    category         = "Profitability",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "ordered_product_sales - referral_fee - fba_fulfillment_fee - ad_spend",
    source_fields    = [
        ("finances_api", "ReferralFee + FBAPerUnitFulfillmentFee"),
        ("ads_api",      "spend"),
    ],
    today_available  = "estimate",
    null_rule        = "use_estimate",
    unit             = "USD",
))

_reg(MetricDef(
    canonical_name   = "contribution_margin_pct",
    display_name     = "Contribution Margin %",
    category         = "Profitability",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "contribution_margin / NULLIF(ordered_product_sales, 0)",
    source_fields    = [("finances_api", "derived")],
    today_available  = "estimate",
    null_rule        = "show_dash",
    unit             = "pct",
))

_reg(MetricDef(
    canonical_name   = "gross_margin",
    display_name     = "Gross Margin",
    category         = "Profitability",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "ordered_product_sales - total_amazon_fees - cogs",
    source_fields    = [
        ("finances_api", "All fees"),
        ("item_master",  "unit_cogs (seller-provided)"),
    ],
    today_available  = "estimate",
    null_rule        = "use_estimate",
    unit             = "USD",
    notes            = "COGS from cogs.csv / item_master. Must be kept current by seller.",
))

_reg(MetricDef(
    canonical_name   = "return_rate",
    display_name     = "Return Rate",
    category         = "Quality",
    dashboard_level  = "Operations",
    priority         = "P1",
    formula          = "SUM(units_refunded) / NULLIF(SUM(units_ordered), 0)",
    source_fields    = [
        ("sales_and_traffic_report", "unitsRefunded / unitsOrdered"),
        ("finances_api",             "RefundEvent.ItemChargeAdjustments (for $ amount)"),
    ],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "pct",
    alert_threshold  = {"above": 0.10},
    notes            = "Filter financial_events: event_type ILIKE '%refund%'",
))


# ── C4. ADVERTISING METRICS ───────────────────────────────────────────────────

_reg(MetricDef(
    canonical_name   = "ad_spend",
    display_name     = "Ad Spend",
    category         = "Advertising",
    dashboard_level  = "Advertising",
    priority         = "P1",
    formula          = "SUM(spend)",
    source_fields    = [("ads_api", "spend")],
    today_available  = "partial",
    null_rule        = "show_zero",
    unit             = "USD",
))

_reg(MetricDef(
    canonical_name   = "acos",
    display_name     = "ACoS (Ad Cost of Sales)",
    category         = "Advertising",
    dashboard_level  = "Advertising",
    priority         = "P1",
    formula          = "SUM(ad_spend) / NULLIF(SUM(ad_attributed_sales), 0)",
    source_fields    = [("ads_api", "spend / attributedSales")],
    today_available  = "partial",
    null_rule        = "show_dash",
    unit             = "pct",
    alert_threshold  = {"above": 0.30},
))

_reg(MetricDef(
    canonical_name   = "tacos",
    display_name     = "TACoS (Total Ad Cost of Sales)",
    category         = "Advertising",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "SUM(ad_spend) / NULLIF(SUM(ordered_product_sales), 0)",
    source_fields    = [
        ("ads_api",                  "spend"),
        ("sales_and_traffic_report", "orderedProductSales"),
    ],
    today_available  = "estimate",
    null_rule        = "show_dash",
    unit             = "pct",
    notes            = "Total ad spend as % of ALL sales (not just ad-attributed). Key executive KPI.",
))

_reg(MetricDef(
    canonical_name   = "roas",
    display_name     = "ROAS (Return on Ad Spend)",
    category         = "Advertising",
    dashboard_level  = "Advertising",
    priority         = "P1",
    formula          = "SUM(ad_attributed_sales) / NULLIF(SUM(ad_spend), 0)",
    source_fields    = [("ads_api", "attributedSales / spend")],
    today_available  = "partial",
    null_rule        = "show_dash",
    unit             = "ratio",
))

_reg(MetricDef(
    canonical_name   = "ad_spend_pct_of_revenue",
    display_name     = "Ad Spend % of Revenue",
    category         = "Advertising",
    dashboard_level  = "Executive",
    priority         = "P1",
    formula          = "SUM(ad_spend) / NULLIF(SUM(ordered_product_sales), 0)",
    source_fields    = [("ads_api", "spend"), ("sales_and_traffic_report", "orderedProductSales")],
    today_available  = "estimate",
    null_rule        = "show_dash",
    unit             = "pct",
))


# ── D. INVENTORY KPIS ─────────────────────────────────────────────────────────

_reg(MetricDef(
    canonical_name   = "days_of_cover",
    display_name     = "Days of Cover",
    category         = "Inventory",
    dashboard_level  = "Operations",
    priority         = "P1",
    formula          = "fulfillable_quantity / NULLIF(units_velocity_7d, 0)",
    source_fields    = [
        ("fba_inventory_api",        "fulfillableQuantity"),
        ("sales_and_traffic_report", "unitsOrdered (7-day avg)"),
    ],
    today_available  = True,
    null_rule        = "show_dash",
    unit             = "days",
    implementation_status = "live",
    alert_threshold  = {"below": 14},
    notes            = "Surfaced via /api/inventory/kpis endpoint. Uses fba_inventory + daily_sales 7d velocity.",
))

_reg(MetricDef(
    canonical_name   = "weeks_of_cover",
    display_name     = "Weeks of Cover",
    category         = "Inventory",
    dashboard_level  = "Operations",
    priority         = "P1",
    formula          = "fulfillable_quantity / NULLIF(units_velocity_7d, 0) / 7.0",
    source_fields    = [("fba_inventory_api", "fulfillableQuantity")],
    today_available  = True,
    null_rule        = "show_dash",
    unit             = "weeks",
    implementation_status = "live",
    alert_threshold  = {"below": 2},
))

_reg(MetricDef(
    canonical_name   = "reorder_point",
    display_name     = "Reorder Point",
    category         = "Inventory",
    dashboard_level  = "Operations",
    priority         = "P1",
    formula          = "(units_velocity_7d * lead_time_days) + safety_stock_units",
    source_fields    = [
        ("sales_and_traffic_report", "unitsOrdered (velocity)"),
        ("item_master",              "lead_time_days, safety_stock_units (seller-set)"),
    ],
    today_available  = True,
    null_rule        = "show_dash",
    unit             = "count",
    implementation_status = "live",
    notes            = "Surfaced via /api/inventory/kpis. Default lead_time=45d, safety_stock=14d. Override via item_master.",
))

_reg(MetricDef(
    canonical_name   = "stockout_probability",
    display_name     = "Stockout Probability",
    category         = "Inventory",
    dashboard_level  = "Operations",
    priority         = "P1",
    formula          = "CASE WHEN days_of_cover < lead_time_days THEN 1 ELSE 0 END",
    source_fields    = [("fba_inventory_api", "fulfillableQuantity"), ("item_master", "lead_time_days")],
    today_available  = True,
    null_rule        = "show_dash",
    unit             = "pct",
    implementation_status = "live",
    alert_threshold  = {"equals": 1},
))

_reg(MetricDef(
    canonical_name   = "sell_through_rate",
    display_name     = "Sell-Through Rate (STR)",
    category         = "Inventory",
    dashboard_level  = "Operations",
    priority         = "P1",
    formula          = "SUM(units_ordered_90d) / NULLIF(AVG(fulfillable_quantity_snapshot_90d), 0)",
    source_fields    = [
        ("sales_and_traffic_report", "unitsOrdered (90-day rolling)"),
        ("fba_inventory_api",        "fulfillableQuantity (requires daily snapshots)"),
    ],
    today_available  = True,
    null_rule        = "show_dash",
    unit             = "pct",
    implementation_status = "live",
    notes            = "Surfaced via /api/inventory/kpis. Uses current fulfillable as denominator (snapshot job adds history).",
))

_reg(MetricDef(
    canonical_name   = "inventory_value",
    display_name     = "Inventory Value",
    category         = "Inventory",
    dashboard_level  = "Finance",
    priority         = "P2",
    formula          = "SUM(fulfillable_quantity * unit_cogs)",
    source_fields    = [
        ("fba_inventory_api", "fulfillableQuantity"),
        ("item_master",       "unit_cogs (from cogs.csv)"),
    ],
    today_available  = True,
    null_rule        = "show_zero",
    unit             = "USD",
))

_reg(MetricDef(
    canonical_name   = "aged_inventory_pct",
    display_name     = "Aged Inventory % (>180 days)",
    category         = "Inventory",
    dashboard_level  = "Operations",
    priority         = "P1",
    formula          = "units_aged_gt_180d / NULLIF(total_fulfillable, 0)",
    source_fields    = [("sales_and_traffic_report", "GET_FBA_INVENTORY_AGED_DATA")],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "pct",
    implementation_status = "planned_phase5",
    alert_threshold  = {"above": 0.15},
))


# ── E. QUALITY / ACCOUNT HEALTH ───────────────────────────────────────────────

_reg(MetricDef(
    canonical_name   = "negative_feedback_rate",
    display_name     = "Negative Feedback Rate",
    category         = "Quality",
    dashboard_level  = "Account Health",
    priority         = "P1",
    formula          = "negative_feedback_received / NULLIF(feedback_received, 0)",
    source_fields    = [("sales_and_traffic_report", "negativeFeedbackReceived / feedbackReceived")],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "pct",
    alert_threshold  = {"above": 0.01},
))

_reg(MetricDef(
    canonical_name   = "cancellation_rate",
    display_name     = "Cancellation Rate",
    category         = "Quality",
    dashboard_level  = "Account Health",
    priority         = "P1",
    formula          = "COUNT(cancelled_orders) / NULLIF(COUNT(DISTINCT order_id), 0)",
    source_fields    = [("orders_api", "OrderStatus = 'Canceled'")],
    today_available  = True,
    null_rule        = "show_zero",
    unit             = "pct",
    alert_threshold  = {"above": 0.025},
))


# ── F. PHASE 6 / ADVANCED KPIs (planned) ─────────────────────────────────────

_reg(MetricDef(
    canonical_name   = "share_of_voice",
    display_name     = "Share of Voice",
    category         = "Brand Analytics",
    dashboard_level  = "Competitive",
    priority         = "P2",
    formula          = "AVG(click_share) WHERE clicked_asin = our_asin",
    source_fields    = [("brand_analytics", "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT.clickShare")],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "pct",
    implementation_status = "planned_phase6",
    notes            = "Requires Brand Registry + Brand Analytics SP-API role.",
))

_reg(MetricDef(
    canonical_name   = "repeat_purchase_rate",
    display_name     = "Repeat Purchase Rate",
    category         = "Brand Analytics",
    dashboard_level  = "Executive",
    priority         = "P2",
    formula          = "repeat_customers / NULLIF(unique_customers, 0)",
    source_fields    = [("brand_analytics", "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT")],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "pct",
    implementation_status = "planned_phase6",
))

_reg(MetricDef(
    canonical_name   = "b2b_revenue_share",
    display_name     = "B2B Revenue Share",
    category         = "B2B",
    dashboard_level  = "Executive",
    priority         = "P2",
    formula          = "ordered_product_sales_b2b / NULLIF(ordered_product_sales, 0)",
    source_fields    = [("sales_and_traffic_report", "orderedProductSalesB2B / orderedProductSales")],
    today_available  = False,
    null_rule        = "show_dash",
    unit             = "pct",
    implementation_status = "planned_phase5",
))


# ══════════════════════════════════════════════════════════════════════════════
# G. HELPER ACCESSORS
# ══════════════════════════════════════════════════════════════════════════════

def get_metric(name: str) -> MetricDef:
    """Return a metric definition. Raises KeyError if not registered."""
    if name not in METRICS:
        raise KeyError(
            f"Metric '{name}' not in registry. "
            f"Available: {', '.join(sorted(METRICS.keys()))}"
        )
    return METRICS[name]


def get_source_priority(metric_name: str, period_bucket: str) -> dict:
    """Return the source priority config for a given metric + period.

    period_bucket: "today" | "yesterday" | "recent" | "historical" | "now"

    Returns dict with keys: sources, rule, note (and optionally: pending_estimation,
    fallback_estimate_pct).
    Returns a sensible default if not defined.
    """
    cfg = SOURCE_PRIORITY.get(metric_name, {})
    if period_bucket in cfg:
        return cfg[period_bucket]
    # Default: use primary source, no estimation
    return {"sources": [], "rule": "use_primary", "note": "No specific priority rule defined."}


def is_today_available(metric_name: str) -> any:
    """Quick check: can this metric be shown for today?

    Returns:
        True     — real-time data available
        "estimate" — data is estimated (show with caveat)
        "partial"  — incomplete data (show with caveat)
        False    — no data for today (show dash)
        "no_data"  — explicitly no data (show dash)
    """
    m = METRICS.get(metric_name)
    return m.today_available if m else False


def metrics_by_priority(priority: str = "P1") -> list[MetricDef]:
    """Return all metrics at or above a given priority level."""
    order = {"P1": 0, "P2": 1, "P3": 2}
    cutoff = order.get(priority, 99)
    return [m for m in METRICS.values() if order.get(m.priority, 99) <= cutoff]


def metrics_by_status(status: str = "live") -> list[MetricDef]:
    """Return all metrics with a given implementation status."""
    return [m for m in METRICS.values() if m.implementation_status == status]


# ══════════════════════════════════════════════════════════════════════════════
# QUICK REFERENCE — FEE ESTIMATION FALLBACKS
# When Finances API returns $0 for recent data, use these rates × revenue.
# Update periodically from actual Finances API data.
# ══════════════════════════════════════════════════════════════════════════════

FEE_FALLBACK_RATES = {
    # (division, category_broad) → estimated rate as fraction of revenue
    "default": {
        "referral_fee":       0.15,   # ~15% for sporting goods
        "fba_fulfillment":    0.12,   # ~12% for avg-size golf/housewares item
        "total_fees":         0.27,   # referral + fulfillment combined
        "storage_monthly":    0.00,   # too variable — do not estimate
    },
    "golf": {
        "referral_fee":       0.15,
        "fba_fulfillment":    0.13,
        "total_fees":         0.28,
    },
    "housewares": {
        "referral_fee":       0.15,
        "fba_fulfillment":    0.11,
        "total_fees":         0.26,
    },
}


def get_fee_fallback(division: str = "default", fee_type: str = "total_fees") -> float:
    """Return the fallback fee rate (as decimal fraction) for a given division + fee type."""
    rates = FEE_FALLBACK_RATES.get(division, FEE_FALLBACK_RATES["default"])
    return rates.get(fee_type, FEE_FALLBACK_RATES["default"].get(fee_type, 0.27))


# ══════════════════════════════════════════════════════════════════════════════
# TIMEZONE RULE — enforced everywhere
# ══════════════════════════════════════════════════════════════════════════════

CANONICAL_TIMEZONE = "America/Chicago"
"""All date boundaries for 'today', 'yesterday', etc. use this timezone.
   purchase_date is stored as UTC ISO8601 strings. Always convert to Central
   before building date range filters:
       s_iso = datetime(y, m, d, 0, 0, 0, tzinfo=ZoneInfo(CANONICAL_TIMEZONE)).isoformat()
       e_iso = datetime(y, m, d, 23, 59, 59, tzinfo=ZoneInfo(CANONICAL_TIMEZONE)).isoformat()
   Never use date.today() — Railway runs UTC. Use _today_central() in sales.py.
"""
