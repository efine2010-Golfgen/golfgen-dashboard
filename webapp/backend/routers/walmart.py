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
            hp + hp + hp
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
              CASE WHEN period_type IN ('L1W','LW') THEN 'LW' ELSE period_type END as period,
              SUM(CASE WHEN period_type IN ('L1W','LW','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_sales_ty, 0) ELSE 0 END) as pos_sales_ty,
              SUM(CASE WHEN period_type IN ('L1W','LW','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_sales_ly, 0) ELSE 0 END) as pos_sales_ly,
              SUM(CASE WHEN period_type IN ('L1W','LW','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_qty_ty, 0) ELSE 0 END) as pos_qty_ty,
              SUM(CASE WHEN period_type IN ('L1W','LW','L4W','L13W','L26W','L52W')
                    THEN COALESCE(pos_qty_ly, 0) ELSE 0 END) as pos_qty_ly,
              SUM(CASE WHEN period_type IN ('L1W','LW','L4W','L13W','L26W','L52W')
                    THEN COALESCE(returns_qty_ty, 0) ELSE 0 END) as returns_ty,
              SUM(CASE WHEN period_type IN ('L1W','LW','L4W','L13W','L26W','L52W')
                    THEN COALESCE(returns_qty_ly, 0) ELSE 0 END) as returns_ly
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','LW','L4W','L13W','L26W','L52W') {hw}
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
              CASE WHEN period_type IN ('L1W','LW') THEN 'LW' ELSE period_type END as period,
              COALESCE(sales_type, 'Other') as sales_type,
              SUM(COALESCE(pos_sales_ty, 0)) as sales_ty,
              SUM(COALESCE(pos_sales_ly, 0)) as sales_ly,
              SUM(COALESCE(pos_qty_ty, 0)) as qty_ty,
              SUM(COALESCE(pos_qty_ly, 0)) as qty_ly
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','LW','L4W','L13W','L26W','L52W') {hw}
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
              prime_item_number,
              CASE WHEN period_type IN ('L1W','LW') THEN 'LW' ELSE period_type END as period,
              pos_sales_ty, pos_sales_ly, pos_qty_ty, pos_qty_ly,
              COALESCE(instock_pct_ty, 0) as instock_pct_ty,
              COALESCE(on_hand_qty_ty, 0) as on_hand_qty_ty,
              COALESCE(on_hand_qty_ly, 0) as on_hand_qty_ly
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','LW','L4W','L13W','L26W','L52W') {hw}
            ORDER BY prime_item_desc, period_type
        """
        item_rows = con.execute(item_query, hp).fetchall()

        # Pivot per-item data by period (since we have multiple rows per item, one per period)
        items_dict = {}
        for row in item_rows:
            item_name, brand, item_number, period, pos_sales_ty, pos_sales_ly, pos_qty_ty, pos_qty_ly, instock_pct_ty, oh_ty, oh_ly = row
            if item_name not in items_dict:
                items_dict[item_name] = {
                    "name": item_name,
                    "brand": brand,
                    "walmartItemNumber": item_number,
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

        # ── Query 1: Aggregate KPIs (exclude zero instock from average)
        kpi_query = f"""
            SELECT
              SUM(COALESCE(on_hand_qty_ty, 0)) as total_oh,
              AVG(CASE WHEN COALESCE(instock_pct_ty, 0) > 0 THEN instock_pct_ty END) as avg_instock_pct,
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

        # ── Query 2: Instock trend by item across all periods (exclude zero)
        trend_query = f"""
            SELECT
              prime_item_desc,
              CASE WHEN period_type IN ('L1W','LW') THEN 'LW' ELSE period_type END as period,
              AVG(CASE WHEN COALESCE(instock_pct_ty, 0) > 0 THEN instock_pct_ty END) as avg_instock
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W','LW','L4W','L13W','L26W','L52W') {hw}
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

        # Metric name normalization map (handle old names from data)
        _METRIC_NORMALIZE = {
            "POS Sales in dollars": "POS Sales $",
            "POS Sales in Units": "POS Units",
            "Comp Sales in dollars": "Comp Sales $",
            "Store Returns in dollars": "Returns $",  
            "Store Returns as % of POS Sales": "Returns %",
            "Gross Initial Margin %": "GIM%",
            "Maintain Margin %": "Maintain Margin%",
            "Store Weeks of Supply/ Weeks on Hand": "Weeks of Supply",
            "Store Weeks of Supply/Weeks on Hand": "Weeks of Supply",
            "Warehouse Weeks of Supply/Weeks on Hand": "Warehouse Weeks of Supply",
            "MUMD Percent To Sales": "MUMD %",
            "Retail turns": "Retail Turns",
            "Ships at cost": "Ships at Cost",
        }

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
                "metricName": _METRIC_NORMALIZE.get(r[2], r[2]),
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
              SUM(COALESCE(auth_based_net_sales_ty, 0)) as auth_sales_ty,
              SUM(COALESCE(auth_based_net_sales_ly, 0)) as auth_sales_ly,
              SUM(COALESCE(shipped_based_net_sales_ty, 0)) as shipped_sales_ty,
              SUM(COALESCE(shipped_based_net_sales_ly, 0)) as shipped_sales_ly,
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
              product_name, fineline_description,
              base_unit_retail_amount, base_unit_retail_amount,
              auth_based_net_sales_ty, auth_based_net_sales_ly,
              shipped_based_net_sales_ty, shipped_based_net_sales_ly,
              shipped_based_qty_ty, shipped_based_qty_ly
            FROM walmart_ecomm_weekly
            WHERE 1=1 {hw}
            ORDER BY auth_based_net_sales_ty DESC
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
              MAX(snapshot_date) as latest_date
            FROM walmart_order_forecast
            WHERE 1=1 {hw}
        """
        kpi_row = con.execute(kpi_query, hp).fetchone()
        latest_date = str(kpi_row[1]) if kpi_row and kpi_row[1] else None
        kpis = {
            "dcCount": _safe_int(kpi_row[0]) if kpi_row else 0,
            "latestDate": latest_date,
        }

        # ── Query 2: Per-record forecast data
        items_query = f"""
            SELECT DISTINCT
              snapshot_date, store_dc_nbr, store_dc_type, vendor_dept_number
            FROM walmart_order_forecast
            WHERE 1=1 {hw}
            ORDER BY snapshot_date DESC, store_dc_nbr
            LIMIT 200
        """
        items_rows = con.execute(items_query, hp).fetchall()
        items = [
            {
                "snapshotDate": str(r[0]) if r[0] else None,
                "storeDcNbr": r[1],
                "storeDcType": r[2],
                "vendorDeptNumber": r[3],
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
            "pos_sales_ty", "pos_sales_ly", "pos_qty_ty", "pos_qty_ly",
            "instock_pct_ty", "returns_qty_ty",
            "on_hand_qty_ty", "store_name", "store_number", "walmart_week",
        }
        sort_col = sort_by if sort_by in allowed_sorts else "pos_sales_ty"
        sort_direction = "ASC" if sort_dir.lower() == "asc" else "DESC"

        # Clamp limit
        limit = min(max(limit, 1), 500)

        # ── Query 1: Total count + aggregate KPIs for pagination
        count_query = f"""
            SELECT COUNT(*),
                   SUM(COALESCE(pos_sales_ty, 0)),
                   SUM(COALESCE(returns_qty_ty, 0))
            FROM walmart_store_weekly {where_clause}
        """
        count_row = con.execute(count_query, hp).fetchone()
        total = _safe_int(count_row[0]) if count_row else 0
        total_pos_sales = _n(count_row[1]) if count_row else 0.0
        total_returns = _n(count_row[2]) if count_row else 0.0
        avg_pos_per_store = total_pos_sales / total if total > 0 else 0.0

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
                "posSalesTy": total_pos_sales,
                "avgPosSalesPerStore": round(avg_pos_per_store, 2),
                "returnsQtyTy": total_returns,
            },
            "stores": stores,
            "total": total,
            "weeks": weeks,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  FIX 2 TODO: Store Analytics Aggregation (State/City Level)
# ═════════════════════════════════════════════════════════════════════════════
# LIMITATION: walmart_store_weekly table does NOT have store_state or store_city columns.
# To implement period-based aggregation by state/city for WalmartStoreAnalytics.jsx:
# 1. Add store_state VARCHAR(2) and store_city VARCHAR(50) columns to walmart_store_weekly
# 2. Add logic to Scintilla import to extract state/city from store_name field
# 3. Create aggregation endpoint: GET /api/walmart/store-analytics-summary?period=L13W
#    Returns: {states: [...], cities: [...], period: "L13W", weeks: 13}
# 4. Update WalmartStoreAnalytics.jsx to fetch this data and merge with WM_DATA/CITY_DATA
# 5. Add period selector buttons (L4W, L13W, L26W, L52W) with useEffect for data fetching
#
# Currently, /api/walmart/store-geography returns latest-week store-level data,
# and frontend uses static WM_DATA constants for geographic/heatmap rendering.
# ═════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 8: Weekly Trend — 52 individual weeks for TY/LY chart
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/weekly-trend")
def get_weekly_trend(division: str = None, customer: str = None):
    """
    Returns up to 52 individual weeks of aggregate POS data for the
    bar+line chart (TY bars, LY line overlay) plus sales_type breakdowns
    (Clearance, Regular, Rollback, Store Tab) and returns $ from
    walmart_item_weekly.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # ── Store-level totals (already aggregated, no sales_type split)
        query = f"""
            SELECT
              walmart_week,
              SUM(COALESCE(pos_sales_ty, 0)) as pos_sales_ty,
              SUM(COALESCE(pos_sales_ly, 0)) as pos_sales_ly,
              SUM(COALESCE(pos_qty_ty, 0)) as pos_qty_ty,
              SUM(COALESCE(pos_qty_ly, 0)) as pos_qty_ly,
              SUM(COALESCE(returns_qty_ty, 0)) as returns_qty_ty,
              SUM(COALESCE(returns_qty_ly, 0)) as returns_qty_ly
            FROM walmart_store_weekly
            WHERE walmart_week IS NOT NULL {hw}
            GROUP BY walmart_week
            ORDER BY walmart_week ASC
            LIMIT 52
        """
        rows = con.execute(query, hp).fetchall()

        # Build week map from store-level data
        week_map = {}
        for r in rows:
            wk = str(r[0]) if r[0] else ""
            week_map[wk] = {
                "week": wk,
                "posSalesTy": _n(r[1]),
                "posSalesLy": _n(r[2]),
                "posQtyTy": _n(r[3]),
                "posQtyLy": _n(r[4]),
                "returnsQtyTy": _n(r[5]),
                "returnsQtyLy": _n(r[6]),
                "clearanceSalesTy": 0.0,
                "clearanceSalesLy": 0.0,
                "regularSalesTy": 0.0,
                "regularSalesLy": 0.0,
                "clearanceQtyTy": 0.0,
                "clearanceQtyLy": 0.0,
                "regularQtyTy": 0.0,
                "regularQtyLy": 0.0,
                "returnsAmtTy": 0.0,
                "returnsAmtLy": 0.0,
            }

        # ── Item-level: sales_type breakdowns + returns $ ──
        item_query = f"""
            SELECT
              walmart_week,
              COALESCE(sales_type, 'Regular') as sales_type,
              SUM(COALESCE(pos_sales_ty, 0)) as pos_sales_ty,
              SUM(COALESCE(pos_sales_ly, 0)) as pos_sales_ly,
              SUM(COALESCE(pos_qty_ty, 0)) as pos_qty_ty,
              SUM(COALESCE(pos_qty_ly, 0)) as pos_qty_ly,
              SUM(COALESCE(returns_amt_ty, 0)) as returns_amt_ty,
              SUM(COALESCE(returns_amt_ly, 0)) as returns_amt_ly
            FROM walmart_item_weekly
            WHERE walmart_week IS NOT NULL {hw}
            GROUP BY walmart_week, sales_type
            ORDER BY walmart_week ASC
        """
        item_rows = con.execute(item_query, hp).fetchall()

        for r in item_rows:
            wk = str(r[0]) if r[0] else ""
            st = (r[1] or "Regular").strip()
            if wk not in week_map:
                continue
            entry = week_map[wk]
            entry["returnsAmtTy"] += _n(r[6])
            entry["returnsAmtLy"] += _n(r[7])
            if st.lower() == "clearance":
                entry["clearanceSalesTy"] += _n(r[2])
                entry["clearanceSalesLy"] += _n(r[3])
                entry["clearanceQtyTy"] += _n(r[4])
                entry["clearanceQtyLy"] += _n(r[5])
            elif st.lower() == "regular":
                entry["regularSalesTy"] += _n(r[2])
                entry["regularSalesLy"] += _n(r[3])
                entry["regularQtyTy"] += _n(r[4])
                entry["regularQtyLy"] += _n(r[5])

        # ── Per-week overall instock % and per-item instock % ──
        instock_query = f"""
            SELECT
              walmart_week,
              AVG(CASE WHEN COALESCE(instock_pct_ty, 0) > 0 THEN instock_pct_ty END) as avg_instock_pct
            FROM walmart_item_weekly
            WHERE walmart_week IS NOT NULL
              AND (period_type = 'weekly' OR period_type IS NULL OR period_type = '') {hw}
            GROUP BY walmart_week
            ORDER BY walmart_week ASC
        """
        try:
            instock_rows = con.execute(instock_query, hp).fetchall()
        except Exception:
            instock_rows = []
        instock_by_week = {}
        for r in instock_rows:
            wk = str(r[0]) if r[0] else ""
            instock_by_week[wk] = _n(r[1])

        # If no weekly-granularity instock found, try period_type IN (L1W, etc)
        if not instock_by_week:
            instock_fallback_q = f"""
                SELECT
                  walmart_week,
                  AVG(CASE WHEN COALESCE(instock_pct_ty, 0) > 0 THEN instock_pct_ty END) as avg_instock_pct
                FROM walmart_item_weekly
                WHERE walmart_week IS NOT NULL {hw}
                GROUP BY walmart_week
                ORDER BY walmart_week ASC
            """
            try:
                fb_rows = con.execute(instock_fallback_q, hp).fetchall()
                for r in fb_rows:
                    wk = str(r[0]) if r[0] else ""
                    instock_by_week[wk] = _n(r[1])
            except Exception:
                pass

        # Per-item per-week instock %
        item_instock_query = f"""
            SELECT
              walmart_week,
              prime_item_desc,
              AVG(CASE WHEN COALESCE(instock_pct_ty, 0) > 0 THEN instock_pct_ty END) as instock_pct
            FROM walmart_item_weekly
            WHERE walmart_week IS NOT NULL
              AND prime_item_desc IS NOT NULL {hw}
            GROUP BY walmart_week, prime_item_desc
            ORDER BY walmart_week ASC
        """
        try:
            item_instock_rows = con.execute(item_instock_query, hp).fetchall()
        except Exception:
            item_instock_rows = []

        # Build item instock map: {item_name: {week: pct}}
        item_instock_map = {}
        for r in item_instock_rows:
            wk = str(r[0]) if r[0] else ""
            item = r[1] or ""
            pct = _n(r[2])
            if item not in item_instock_map:
                item_instock_map[item] = {}
            item_instock_map[item][wk] = pct

        # Add instock to week_map entries
        for wk in week_map:
            week_map[wk]["instockPct"] = instock_by_week.get(wk, 0)

        # Return in sorted order
        sorted_weeks = sorted(week_map.keys())
        weeks = [week_map[wk] for wk in sorted_weeks]
        return {
            "weeks": weeks,
            "itemInstock": item_instock_map,
            "weekOrder": sorted_weeks,
        }
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 8b: Item Store Detail — Per-item store inventory metrics (2026)
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/item-store-detail")
def get_item_store_detail(division: str = None, customer: str = None):
    """
    Returns per-item store inventory + sales metrics for current 2026 items.
    Pivots by period_type so each item gets one row with columns for each period.

    Metrics per item:
      vendorItemNumber — prime_item_number (GolfGen/vendor item #)
      wmtItemNumber    — Walmart item # from walmart_nif_items (if available)
      ohUnits          — Walmart OH Units (L1W/LW/weekly row)
      onOrderUnits     — Walmart On Order Units
      traitedStores    — # Traited Stores
      storesWithInv    — # Stores with Inventory > 0 (traited × instock_pct)
      traitedZeroInv   — # Traited Stores with 0 Inventory
      storesOneUnit    — # Stores with 1 Unit OH (not in schema → 0)
      storesWithSales  — # Stores with Sales (pos_store_count_ty from L1W row)
      salesLW          — Sales Units Last Week
      salesL4W         — Sales Units Last 4 Weeks
      salesL8W         — Sales Units Last 8 Weeks
      salesL13W        — Sales Units Last 13 Weeks

    Filters to walmart_week >= 202601 (2026 data only).
    Uses most-recent walmart_week per item × period_type to avoid double-counting.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # Step 1: Get the most-recent walmart_week per item × period_type
        #   then fetch the matching row's metrics.
        #   hp is used twice (once in inner subquery, once in outer WHERE).
        rows = con.execute(f"""
            SELECT
              w.prime_item_number,
              w.prime_item_desc,
              UPPER(w.period_type)          AS period_type,
              COALESCE(w.pos_qty_ty, 0)     AS pos_qty,
              COALESCE(w.pos_store_count_ty, 0) AS pos_store_count,
              COALESCE(w.on_hand_qty_ty, 0) AS oh_units,
              COALESCE(w.on_order_qty_ty, 0) AS on_order_units,
              COALESCE(w.traited_store_count_ty, 0) AS traited_stores,
              COALESCE(w.valid_store_count_ty, 0)   AS valid_stores,
              w.instock_pct_ty
            FROM walmart_item_weekly w
            INNER JOIN (
              SELECT prime_item_number, period_type, MAX(walmart_week) AS max_wk
              FROM walmart_item_weekly
              WHERE prime_item_desc IS NOT NULL
                AND CAST(walmart_week AS VARCHAR) >= '202601'
                {hw}
              GROUP BY prime_item_number, period_type
            ) latest
              ON w.prime_item_number = latest.prime_item_number
             AND w.period_type      = latest.period_type
             AND w.walmart_week     = latest.max_wk
            WHERE w.prime_item_desc IS NOT NULL
              AND CAST(w.walmart_week AS VARCHAR) >= '202601'
              {hw}
            ORDER BY w.prime_item_desc, w.period_type
        """, hp + hp).fetchall()

        # Step 2: Pull GolfGen vendor# → Walmart item# mapping from NIF items
        try:
            nif_rows = con.execute("""
                SELECT DISTINCT vendor_stock_number, wmt_item_number
                FROM walmart_nif_items
                WHERE vendor_stock_number IS NOT NULL AND vendor_stock_number <> ''
            """).fetchall()
            nif_map = {r[0]: (r[1] or "") for r in nif_rows}
        except Exception:
            nif_map = {}

        # Step 3: Pivot by period_type — build one dict entry per item
        # L1W/LW/WEEKLY = most-recent inventory snapshot (preferred for OH/traited)
        # L4W/L8W/L13W  = rolling sales windows (sales units only)
        # Fallback: if no L1W row exists, use inventory values from next-best period
        L1W_TYPES = {"L1W", "LW", "WEEKLY"}
        # Inventory fallback priority: shorter windows are more current
        INV_FALLBACK_ORDER = ["L4W", "L8W", "L13W", "L26W", "L52W"]

        items_dict: dict = {}   # prime_item_number → output dict
        inv_cache: dict  = {}   # prime_item_number → {period: (oh, on_order, traited, valid_st, instock_raw, pos_sc)}

        def _fill_inv(d, oh, on_order, traited, valid_st, instock_raw, pos_sc):
            """Populate inventory-snapshot fields from one period's row."""
            d["ohUnits"]       = oh
            d["onOrderUnits"]  = on_order
            d["traitedStores"] = traited
            d["storesWithSales"] = pos_sc
            if instock_raw is not None and traited > 0:
                instock = (float(instock_raw) / 100.0) if float(instock_raw) > 1 else float(instock_raw)
                stores_with_inv = round(traited * instock)
            else:
                stores_with_inv = valid_st
            d["storesWithInv"]  = stores_with_inv
            # traitedZeroInv = traited stores with 0 OH (traited-only, not all stores)
            d["traitedZeroInv"] = max(0, traited - stores_with_inv)

        for r in rows:
            item_num    = r[0] or ""
            item_desc   = r[1] or "Unknown"
            period      = r[2] or ""
            pos_qty     = _n(r[3])
            pos_sc      = _safe_int(r[4])
            oh          = _n(r[5])
            on_order    = _n(r[6])
            traited     = _safe_int(r[7])
            valid_st    = _safe_int(r[8])
            instock_raw = r[9]

            if item_num not in items_dict:
                items_dict[item_num] = {
                    "itemName":         item_desc,
                    "vendorItemNumber": item_num,
                    "wmtItemNumber":    nif_map.get(item_num, ""),
                    "ohUnits":          0,
                    "onOrderUnits":     0,
                    "traitedStores":    0,
                    "storesWithInv":    0,
                    "traitedZeroInv":   0,   # traited stores with 0 OH only
                    "storesOneUnit":    0,   # not available at weekly granularity
                    "storesWithSales":  0,
                    "onOrderZeroOH":    None,  # requires store-item level data
                    "salesLW":          0,
                    "salesL4W":         0,
                    "salesL8W":         0,
                    "salesL13W":        0,
                    "_has_inv":         False,
                }
                inv_cache[item_num] = {}

            d = items_dict[item_num]

            # Always cache this period's inventory values for fallback
            inv_cache[item_num][period] = (oh, on_order, traited, valid_st, instock_raw, pos_sc)

            if period in L1W_TYPES:
                # Primary: use this row for all inventory-snapshot fields
                _fill_inv(d, oh, on_order, traited, valid_st, instock_raw, pos_sc)
                d["salesLW"]   = pos_qty
                d["_has_inv"]  = True
            elif period == "L4W":
                d["salesL4W"]  = pos_qty
            elif period == "L8W":
                d["salesL8W"]  = pos_qty
            elif period == "L13W":
                d["salesL13W"] = pos_qty

        # Fallback: items that had NO L1W/weekly row get inventory from next-best period
        for item_num, d in items_dict.items():
            if not d["_has_inv"]:
                cache = inv_cache.get(item_num, {})
                for fallback_pt in INV_FALLBACK_ORDER:
                    if fallback_pt in cache:
                        _fill_inv(d, *cache[fallback_pt])
                        break
            del d["_has_inv"]  # strip internal flag before returning

        items = sorted(items_dict.values(), key=lambda x: x["itemName"])
        return {"items": items}
    finally:
        con.close()


# ═════════════════════════════════════════════════════════════════════════════
#  ENDPOINT 9: Store Geography — All stores for latest week (geography page)
# ═════════════════════════════════════════════════════════════════════════════


@router.get("/api/walmart/store-geography")
def get_store_geography(division: str = None, customer: str = None):
    """
    Returns all store rows for the latest week to power the geography page.
    Frontend handles state/city/region mapping via its own lookup tables.
    """
    con = get_db()
    try:
        hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

        # Find latest week
        wk_row = con.execute(
            f"SELECT MAX(walmart_week) FROM walmart_store_weekly WHERE 1=1 {hw}", hp
        ).fetchone()
        latest_week = str(wk_row[0]) if wk_row and wk_row[0] else None
        if not latest_week:
            return {"stores": [], "latestWeek": None, "totalPosSales": 0,
                    "totalStores": 0, "traitedCount": 0}

        # Fetch all stores for latest week
        query = f"""
            SELECT store_number, store_name,
                   COALESCE(pos_sales_ty, 0), COALESCE(pos_sales_ly, 0),
                   COALESCE(pos_qty_ty, 0), COALESCE(pos_qty_ly, 0),
                   COALESCE(on_hand_qty_ty, 0), COALESCE(on_hand_qty_ly, 0),
                   COALESCE(in_transit_qty_ty, 0), COALESCE(in_warehouse_qty_ty, 0),
                   COALESCE(instock_pct_ty, 0), COALESCE(instock_pct_ly, 0),
                   COALESCE(returns_qty_ty, 0), COALESCE(returns_qty_ly, 0),
                   COALESCE(traited_store_count_ty, 0)
            FROM walmart_store_weekly
            WHERE walmart_week = ? {hw}
            ORDER BY pos_sales_ty DESC
        """
        rows = con.execute(query, [latest_week] + hp).fetchall()

        stores = []
        total_pos = 0.0
        for r in rows:
            pos = _n(r[2])
            total_pos += pos
            stores.append({
                "storeNumber": str(r[0]),
                "storeName": r[1] or "",
                "posSalesTy": pos,
                "posSalesLy": _n(r[3]),
                "posQtyTy": _n(r[4]),
                "posQtyLy": _n(r[5]),
                "ohTy": _n(r[6]),
                "ohLy": _n(r[7]),
                "inTransitTy": _n(r[8]),
                "inWarehouseTy": _n(r[9]),
                "instockPctTy": _n(r[10]),
                "instockPctLy": _n(r[11]),
                "returnsQtyTy": _n(r[12]),
                "returnsQtyLy": _n(r[13]),
                "traitedCount": _safe_int(r[14]),
            })

        # Get traited store count (first non-zero value)
        traited = 0
        for s in stores:
            if s["traitedCount"] > 0:
                traited = s["traitedCount"]
                break

        return {
            "stores": stores,
            "latestWeek": latest_week,
            "totalPosSales": round(total_pos, 2),
            "totalStores": len(stores),
            "traitedCount": traited or len(stores),
        }
    finally:
        con.close()

