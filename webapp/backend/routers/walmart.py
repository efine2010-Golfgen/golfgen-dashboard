"""
Walmart Analytics Routes — Modular endpoints for frontend tabs.

Implements 7 endpoints serving Sales, Inventory, Scorecard, eCommerce,
Forecast, and Store Analytics tabs. All queries optimized to avoid N+1,
using aggregate queries with CASE-WHEN for period pivoting.

Architecture:
1. /api/walmart/availability — Quick check which tabs have data
2. /api/walmart/sales — Sales Performance (item-level, all periods in one query)
3. /api/walmart/inventory — Inventory Health (instock trends, OH distribution)
4. /api/walmart/scorecard — Vendor Scorecard (KPI matrix)
5. /api/walmart/ecommerce — eCommerce tab
6. /api/walmart/forecast — Order Forecast tab
7. /api/walmart/store-analytics — Store-level drill-down with sorting/pagination
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict

from fastapi import APIRouter, Query

from core.database import get_db
from core.hierarchy import hierarchy_filter

logger = logging.getLogger("golfgen")
router = APIRouter()

CT = ZoneInfo("America/Chicago")


# ── Helper functions ───────────────────────────────────────────────────────


def _n(v):
    """Coerce value to float, returning 0.0 for None/empty."""
    if v is None or v == "":
        return 0.0
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(v):
    """Coerce value to int, returning 0 for None/empty."""
    if v is None or v == "":
        return 0
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _map_period(period_type: str) -> str:
    """Map database period_type to frontend period name.

    Database stores:
      L1W (last 1 week), L4W, L13W, L26W, L52W
    Frontend expects:
      LW (last week), L4W, L13W, L26W, L52W
    """
    if period_type == "L1W":
        return "LW"
    return period_type


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 1: Availability — Quick check which tabs have data
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/availability")
def get_availability(division: str = None, customer: str = None):
    """
    Quick check which tabs have data for the given division/customer.

    Returns:
    {
      "hasItemData": bool,
      "hasScorecardData": bool,
      "hasStoreData": bool,
      "hasEcommData": bool,
      "hasForecastData": bool,
      "latestWeek": "202604" or null
    }
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # Check each table for data
        item_rows = con.execute(
            f"SELECT COUNT(*) FROM walmart_item_weekly WHERE 1=1 {hw}",
            hp
        ).fetchone()
        has_item = _safe_int(item_rows[0]) > 0 if item_rows else False

        scorecard_rows = con.execute(
            f"SELECT COUNT(*) FROM walmart_scorecard WHERE 1=1 {hw}",
            hp
        ).fetchone()
        has_scorecard = _safe_int(scorecard_rows[0]) > 0 if scorecard_rows else False

        store_rows = con.execute(
            f"SELECT COUNT(*) FROM walmart_store_weekly WHERE 1=1 {hw}",
            hp
        ).fetchone()
        has_store = _safe_int(store_rows[0]) > 0 if store_rows else False

        ecomm_rows = con.execute(
            f"SELECT COUNT(*) FROM walmart_ecomm_weekly WHERE 1=1 {hw}",
            hp
        ).fetchone()
        has_ecomm = _safe_int(ecomm_rows[0]) > 0 if ecomm_rows else False

        forecast_rows = con.execute(
            f"SELECT COUNT(*) FROM walmart_order_forecast WHERE 1=1 {hw}",
            hp
        ).fetchone()
        has_forecast = _safe_int(forecast_rows[0]) > 0 if forecast_rows else False

        # Get latest week across all tables
        latest_week = None
        week_row = con.execute(
            f"""
            SELECT MAX(walmart_week) FROM (
                SELECT walmart_week FROM walmart_item_weekly WHERE 1=1 {hw}
                UNION ALL
                SELECT walmart_week FROM walmart_store_weekly WHERE 1=1 {hw}
                UNION ALL
                SELECT walmart_week FROM walmart_ecomm_weekly WHERE 1=1 {hw}
            ) t
            """,
            hp
        ).fetchone()
        if week_row and week_row[0]:
            latest_week = str(week_row[0])

        return {
            "hasItemData": has_item,
            "hasScorecardData": has_scorecard,
            "hasStoreData": has_store,
            "hasEcommData": has_ecomm,
            "hasForecastData": has_forecast,
            "latestWeek": latest_week,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 2: Sales Performance — Item-level with all periods in one query
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/sales")
def get_sales(division: str = None, customer: str = None):
    """
    Sales Performance tab.

    Returns KPIs by period (LW/L4W/L13W/L26W/L52W), sales breakdown by type,
    and per-item performance data.

    Uses a SINGLE query with GROUP BY + CASE WHEN to get all items and periods
    at once, avoiding N+1 queries.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # ── Query 1: KPI aggregates by period (single query with multiple CASE WHEN)
        kpi_query = f"""
            SELECT
              CASE WHEN period_type = 'L1W' THEN 'LW' ELSE period_type END as period,
              SUM(CASE WHEN period_type IN ('L1W','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_sales_ty, 0) ELSE 0 END) as pos_sales_ty,
              SUM(CASE WHEN period_type IN ('L1W','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_sales_ly, 0) ELSE 0 END) as pos_sales_ly,
              SUM(CASE WHEN period_type IN ('L1W','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_qty_ty, 0) ELSE 0 END) as pos_qty_ty,
              SUM(CASE WHEN period_type IN ('L1W','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_qty_ly, 0) ELSE 0 END) as pos_qty_ly,
              SUM(CASE WHEN period_type IN ('L1W','L4W','L13W','L26W','L52W')
                    THEN COALESCE(returns_qty_ty, 0) ELSE 0 END) as returns_ty,
              SUM(CASE WHEN period_type IN ('L1W','L4W','L13W','L26W','L52W')
                    THEN COALESCE(returns_qty_ly, 0) ELSE 0 END) as returns_ly
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','L4W','L13W','L26W','L52W') {hw}
            GROUP BY period_type
        """
        kpi_rows = con.execute(kpi_query, hp).fetchall()
        kpis = {}
        for row in kpi_rows:
            period = row[0]
            kpis[period] = {
                "posSalesTy": _n(row[1]),
                "posSalesLy": _n(row[2]),
                "posQtyTy": _n(row[3]),
                "posQtyLy": _n(row[4]),
                "returnsTy": _n(row[5]),
                "returnsLy": _n(row[6]),
            }

        # ── Query 2: Sales by type breakdown (Regular, Clearance, CVP, etc.)
        sales_type_query = f"""
            SELECT
              CASE WHEN period_type = 'L1W' THEN 'LW' ELSE period_type END as period,
              COALESCE(sales_type, 'Other') as sales_type,
              SUM(COALESCE(pos_sales_ty, 0)) as sales_ty,
              SUM(COALESCE(pos_sales_ly, 0)) as sales_ly,
              SUM(COALESCE(pos_qty_ty, 0)) as qty_ty,
              SUM(COALESCE(pos_qty_ly, 0)) as qty_ly
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','L4W','L13W','L26W','L52W') {hw}
            GROUP BY period_type, sales_type
        """
        sales_type_rows = con.execute(sales_type_query, hp).fetchall()
        sales_by_type = defaultdict(dict)
        for row in sales_type_rows:
            period, sales_type, sales_ty, sales_ly, qty_ty, qty_ly = row
            if period not in sales_by_type:
                sales_by_type[period] = {}
            sales_by_type[period][sales_type] = {
                "salesTy": _n(sales_ty),
                "salesLy": _n(sales_ly),
                "qtyTy": _n(qty_ty),
                "qtyLy": _n(qty_ly),
            }

        # ── Query 3: Per-item performance across all periods
        # This is the heavy lifting: all items with all period data in one result set
        item_query = f"""
            SELECT
              prime_item_desc,
              brand_name,
              CASE WHEN period_type = 'L1W' THEN 'LW' ELSE period_type END as period,
              pos_sales_ty, pos_sales_ly, pos_qty_ty, pos_qty_ly,
              COALESCE(instock_pct_ty, 0) as instock_pct_ty,
              COALESCE(on_hand_qty_ty, 0) as on_hand_qty_ty,
              COALESCE(on_hand_qty_ly, 0) as on_hand_qty_ly
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','L4W','L13W','L26W','L52W') {hw}
            ORDER BY prime_item_desc, period_type
        """
        item_rows = con.execute(item_query, hp).fetchall()

        # Pivot per-item data by period (since we have multiple rows per item, one per period)
        items_dict = {}
        for row in item_rows:
            item_name, brand, period, pos_sales_ty, pos_sales_ly, pos_qty_ty, pos_qty_ly, instock_pct_ty, oh_ty, oh_ly = row
            if item_name not in items_dict:
                items_dict[item_name] = {
                    "name": item_name,
                    "brand": brand,
                    "lw": {}, "l4w": {}, "l13w": {}, "l26w": {}, "l52w": {}
                }

            period_key = period.lower()
            items_dict[item_name][period_key] = {
                "posTy": _n(pos_sales_ty),
                "posLy": _n(pos_sales_ly),
                "qtyTy": _n(pos_qty_ty),
                "qtyLy": _n(pos_qty_ly),
                "instockPct": _n(instock_pct_ty),
                "ohTy": _n(oh_ty),
                "ohLy": _n(oh_ly),
            }

        items = list(items_dict.values())

        return {
            "kpis": kpis,
            "salesByType": dict(sales_by_type),
            "items": items,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 3: Inventory Health — Instock trends and OH distribution
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/inventory")
def get_inventory(division: str = None, customer: str = None):
    """
    Inventory Health tab.

    Returns KPIs (total OH, instock %, weeks of supply), instock trend
    across windows, and OH distribution.

    Uses multiple optimized queries to avoid N+1 pattern.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # ── Query 1: Aggregate KPIs
        kpi_query = f"""
            SELECT
              SUM(COALESCE(on_hand_qty_ty, 0)) as total_oh,
              AVG(COALESCE(instock_pct_ty, 0)) as avg_instock_pct,
              SUM(CASE WHEN COALESCE(pos_qty_ty, 0) > 0
                    THEN COALESCE(on_hand_qty_ty, 0) / COALESCE(pos_qty_ty, 0)
                    ELSE 0 END) as weeks_of_supply
            FROM walmart_item_weekly
            WHERE period_type = 'L1W' {hw}
        """
        kpi_row = con.execute(kpi_query, hp).fetchone()
        kpis = {
            "totalOhUnits": _n(kpi_row[0]) if kpi_row else 0.0,
            "avgInstockPct": _n(kpi_row[1]) if kpi_row else 0.0,
            "weeksOfSupply": _n(kpi_row[2]) if kpi_row else 0.0,
        }

        # ── Query 2: Instock trend by item across all periods
        trend_query = f"""
            SELECT
              prime_item_desc,
              CASE WHEN period_type = 'L1W' THEN 'LW' ELSE period_type END as period,
              AVG(COALESCE(instock_pct_ty, 0)) as avg_instock
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','L4W','L13W','L26W','L52W') {hw}
            GROUP BY prime_item_desc, period_type
            ORDER BY prime_item_desc, period_type
        """
        trend_rows = con.execute(trend_query, hp).fetchall()

        # Pivot instock trend: items → periods → instock %
        trend_dict = {}
        for row in trend_rows:
            item_name, period, instock_pct = row
            if item_name not in trend_dict:
                trend_dict[item_name] = {}
            period_key = period.lower()
            trend_dict[item_name][period_key] = _n(instock_pct)

        # ── Query 3: OH distribution by sales type
        oh_dist_query = f"""
            SELECT
              COALESCE(sales_type, 'Other') as sales_type,
              SUM(COALESCE(on_hand_qty_ty, 0)) as total_oh
            FROM walmart_item_weekly
            WHERE period_type = 'L1W' {hw}
            GROUP BY sales_type
        """
        oh_dist_rows = con.execute(oh_dist_query, hp).fetchall()
        oh_distribution = {}
        for row in oh_dist_rows:
            sales_type, total_oh = row
            oh_distribution[sales_type] = _n(total_oh)

        return {
            "kpis": kpis,
            "instockTrend": trend_dict,
            "ohDistribution": oh_distribution,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 4: Vendor Scorecard — KPI matrix by vendor and period
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/scorecard")
def get_scorecard(division: str = None, customer: str = None):
    """
    Vendor Scorecard tab.

    Returns full scorecard data matrix plus extracted KPIs for Last Week period.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # Fetch all scorecard rows
        scorecard_query = f"""
            SELECT vendor_section, metric_group, metric_name, period, period_range,
                   value_ty, value_ly, value_diff
            FROM walmart_scorecard
            WHERE 1=1 {hw}
            ORDER BY vendor_section, metric_group, metric_name, period
        """
        scorecard_rows = con.execute(scorecard_query, hp).fetchall()

        scorecard = [
            {
                "vendorSection": r[0],
                "metricGroup": r[1],
                "metricName": r[2],
                "period": r[3],
                "periodRange": r[4],
                "valueTy": _n(r[5]),
                "valueLy": _n(r[6]),
                "valueDiff": _n(r[7]),
            }
            for r in scorecard_rows
        ]

        # Extract KPIs from Last Week scorecard data (where vendor_section != 'All Vendors')
        kpis = {
            "posSalesLw": {"ty": 0.0, "ly": 0.0, "diff": 0.0},
            "gimPctLw": {"ty": 0.0, "ly": 0.0, "diff": 0.0},
            "mumdLw": {"ty": 0.0, "ly": 0.0, "diff": 0.0},
            "replInstockLw": {"ty": 0.0, "ly": 0.0, "diff": 0.0},
            "gmroiiLw": {"ty": 0.0, "ly": 0.0, "diff": 0.0},
        }
        for row in scorecard:
            if row["period"] == "Last Week" and row["vendorSection"] != "All Vendors":
                metric_name = row["metricName"].lower().replace(" ", "_").replace("%", "pct")
                if "pos_sales" in metric_name:
                    kpis["posSalesLw"]["ty"] = max(kpis["posSalesLw"]["ty"], _n(row["valueTy"]))
                    kpis["posSalesLw"]["ly"] = max(kpis["posSalesLw"]["ly"], _n(row["valueLy"]))
                    kpis["posSalesLw"]["diff"] = max(kpis["posSalesLw"]["diff"], _n(row["valueDiff"]))

        return {
            "scorecard": scorecard,
            "kpis": kpis,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 5: eCommerce — Online sales data
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/ecommerce")
def get_ecommerce(division: str = None, customer: str = None):
    """
    eCommerce tab.

    Returns eCommerce KPIs and per-item data from walmart_ecomm_weekly table.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # ── Query 1: Aggregate KPIs
        kpi_query = f"""
            SELECT
              SUM(COALESCE(auth_sales_ty, 0)) as auth_sales_ty,
              SUM(COALESCE(auth_sales_ly, 0)) as auth_sales_ly,
              SUM(COALESCE(shipped_sales_ty, 0)) as shipped_sales_ty,
              SUM(COALESCE(shipped_sales_ly, 0)) as shipped_sales_ly,
              COUNT(DISTINCT product_name) as total_items
            FROM walmart_ecomm_weekly
            WHERE 1=1 {hw}
        """
        kpi_row = con.execute(kpi_query, hp).fetchone()
        kpis = {
            "authSalesTy": _n(kpi_row[0]) if kpi_row else 0.0,
            "authSalesLy": _n(kpi_row[1]) if kpi_row else 0.0,
            "shippedSalesTy": _n(kpi_row[2]) if kpi_row else 0.0,
            "shippedSalesLy": _n(kpi_row[3]) if kpi_row else 0.0,
            "totalItems": _safe_int(kpi_row[4]) if kpi_row else 0,
        }

        # ── Query 2: Per-item data
        items_query = f"""
            SELECT
              product_name, fineline_number,
              retail_amount_ty, retail_amount_ly,
              auth_sales_ty, auth_sales_ly,
              shipped_sales_ty, shipped_sales_ly,
              units_shipped_ty, units_shipped_ly
            FROM walmart_ecomm_weekly
            WHERE 1=1 {hw}
            ORDER BY retail_amount_ty DESC
        """
        items_rows = con.execute(items_query, hp).fetchall()
        items = [
            {
                "productName": r[0],
                "fineline": r[1],
                "retailAmountTy": _n(r[2]),
                "retailAmountLy": _n(r[3]),
                "authSalesTy": _n(r[4]),
                "authSalesLy": _n(r[5]),
                "shippedSalesTy": _n(r[6]),
                "shippedSalesLy": _n(r[7]),
                "unitsShippedTy": _n(r[8]),
                "unitsShippedLy": _n(r[9]),
            }
            for r in items_rows
        ]

        return {
            "kpis": kpis,
            "items": items,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 6: Order Forecast — DC-level forecast data
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/forecast")
def get_forecast(division: str = None, customer: str = None):
    """
    Order Forecast tab.

    Returns forecast KPIs and detailed forecast data from walmart_order_forecast table.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # ── Query 1: Aggregate KPIs
        kpi_query = f"""
            SELECT
              COUNT(DISTINCT store_dc_nbr) as dc_count,
              MAX(snapshot_date) as latest_date,
              SUM(COALESCE(cost_on_order_ty, 0)) as cost_on_order_ty,
              SUM(COALESCE(cost_on_order_ly, 0)) as cost_on_order_ly
            FROM walmart_order_forecast
            WHERE 1=1 {hw}
        """
        kpi_row = con.execute(kpi_query, hp).fetchone()
        latest_date = str(kpi_row[1]) if kpi_row and kpi_row[1] else None
        kpis = {
            "dcCount": _safe_int(kpi_row[0]) if kpi_row else 0,
            "latestDate": latest_date,
            "costOnOrderTy": _n(kpi_row[2]) if kpi_row else 0.0,
            "costOnOrderLy": _n(kpi_row[3]) if kpi_row else 0.0,
        }

        # ── Query 2: Per-record forecast data
        items_query = f"""
            SELECT
              snapshot_date, store_dc_nbr, store_dc_type, store_dc_name,
              receipt_plan_cost_ty, receipt_plan_cost_ly,
              receipt_plan_qty_ty, receipt_plan_qty_ly
            FROM walmart_order_forecast
            WHERE 1=1 {hw}
            ORDER BY snapshot_date DESC, store_dc_nbr
        """
        items_rows = con.execute(items_query, hp).fetchall()
        items = [
            {
                "snapshotDate": str(r[0]) if r[0] else None,
                "storeDcNbr": r[1],
                "storeDcType": r[2],
                "storeDcName": r[3],
                "receiptPlanCostTy": _n(r[4]),
                "receiptPlanCostLy": _n(r[5]),
                "receiptPlanQtyTy": _n(r[6]),
                "receiptPlanQtyLy": _n(r[7]),
            }
            for r in items_rows
        ]

        return {
            "kpis": kpis,
            "items": items,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 7: Store Analytics — Store-level drill-down with pagination
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/store-analytics")
def get_store_analytics(
    division: str = None,
    customer: str = None,
    week: str = None,
    sort_by: str = "pos_sales_ty",
    sort_dir: str = "desc",
    limit: int = 100,
    offset: int = 0,
):
    """
    Store Analytics tab.

    Returns store-level weekly data with sorting and pagination.

    Query params:
      - division, customer: hierarchy filters
      - week: optional filter by walmart_week (e.g. "202604")
      - sort_by: column to sort by (pos_sales_ty, pos_qty_ty, instock_pct_ty, etc.)
      - sort_dir: "asc" or "desc" (default: desc)
      - limit: page size (default: 100, max: 500)
      - offset: skip N results (default: 0)
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        where_clause = f"WHERE 1=1 {hw}"
        if week:
            where_clause += " AND walmart_week = ?"
            hp.append(week)

        # Validate sort column (whitelist)
        allowed_sorts = {
            "pos_sales_ty", "pos_qty_ty", "instock_pct_ty", "returns_qty_ty",
            "on_hand_qty_ty", "store_name", "store_number", "walmart_week",
        }
        sort_col = sort_by if sort_by in allowed_sorts else "pos_sales_ty"
        sort_direction = "ASC" if sort_dir.lower() == "asc" else "DESC"

        # Clamp limit
        limit = min(max(limit, 1), 500)

        # ── Query 1: Total count for pagination
        count_query = f"SELECT COUNT(*) FROM walmart_store_weekly {where_clause}"
        count_row = con.execute(count_query, hp).fetchone()
        total = _safe_int(count_row[0]) if count_row else 0

        # ── Query 2: Paginated store data
        data_query = f"""
            SELECT
              walmart_week, walmart_year, store_name, store_number,
              pos_qty_ty, pos_qty_ly, pos_sales_ty, pos_sales_ly,
              returns_qty_ty, returns_qty_ly,
              on_hand_qty_ty, on_hand_qty_ly,
              instock_pct_ty, instock_pct_ly,
              repl_instock_pct_ty, repl_instock_pct_ly,
              in_transit_qty_ty, in_warehouse_qty_ty,
              traited_store_count_ty
            FROM walmart_store_weekly
            {where_clause}
            ORDER BY {sort_col} {sort_direction}
            LIMIT ? OFFSET ?
        """
        data_rows = con.execute(data_query, hp + [limit, offset]).fetchall()

        stores = [
            {
                "walmartWeek": r[0],
                "walmartYear": r[1],
                "storeName": r[2],
                "storeNumber": r[3],
                "posQtyTy": _n(r[4]),
                "posQtyLy": _n(r[5]),
                "posSalesTy": _n(r[6]),
                "posSalesLy": _n(r[7]),
                "returnsQtyTy": _n(r[8]),
                "returnsQtyLy": _n(r[9]),
                "onHandQtyTy": _n(r[10]),
                "onHandQtyLy": _n(r[11]),
                "instockPctTy": _n(r[12]),
                "instockPctLy": _n(r[13]),
                "replInstockPctTy": _n(r[14]),
                "replInstockPctLy": _n(r[15]),
                "inTransitQtyTy": _n(r[16]),
                "inWarehouseQtyTy": _n(r[17]),
                "traitedStoreCountTy": _safe_int(r[18]),
            }
            for r in data_rows
        ]

        # ── Query 3: Available weeks for filter dropdown
        weeks_query = f"""
            SELECT DISTINCT walmart_week
            FROM walmart_store_weekly
            {where_clause}
            ORDER BY walmart_week DESC
        """
        weeks_rows = con.execute(weeks_query, hp).fetchall()
        weeks = [str(r[0]) for r in weeks_rows if r[0]]

        return {
            "kpis": {
                "totalStores": total,
            },
            "stores": stores,
            "total": total,
            "weeks": weeks,
        }
    finally:
        con.close()
