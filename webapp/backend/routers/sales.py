"""Sales analytics routes."""
import csv
import json
import logging
import re
from pathlib import Path
from collections import defaultdict
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query

from core.config import DB_PATH, DB_DIR, COGS_PATH
from core.database import get_db
from core.hierarchy import hierarchy_filter as _hierarchy_filter

logger = logging.getLogger("golfgen")
router = APIRouter()


# ── Helper Functions ────────────────────────────────────


def load_json(filename: str) -> list:
    """Load a JSON data file from the data directory."""
    path = DB_DIR / filename
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(filename: str, data):
    """Save data to a JSON file in the data directory."""
    path = DB_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


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
    """Return today's actual calendar date in US/Central timezone."""
    now_utc = datetime.now(ZoneInfo("UTC"))
    now_central = now_utc.astimezone(ZoneInfo("America/Chicago"))
    return now_central.replace(hour=0, minute=0, second=0, microsecond=0)


def _build_product_list(con, cutoff: str) -> list:
    """Build product-level P&L. Core business logic."""
    cogs_data = load_cogs()
    inv_names = {}
    try:
        inv_rows = con.execute(
            "SELECT asin, sku, product_name FROM fba_inventory"
        ).fetchall()
        for ir in inv_rows:
            inv_names[ir[0]] = {"sku": ir[1] or "", "product_name": ir[2] or ""}
    except Exception:
        pass

    rows = con.execute("""
        SELECT asin,
               COALESCE(SUM(ordered_product_sales), 0) AS revenue,
               COALESCE(SUM(units_ordered), 0) AS units,
               COALESCE(SUM(sessions), 0) AS sessions,
               COALESCE(SUM(page_views), 0) AS glance_views
        FROM daily_sales
        WHERE asin != 'ALL' AND date >= ?
        GROUP BY asin
        ORDER BY SUM(ordered_product_sales) DESC
    """, [cutoff]).fetchall()

    fin_rows = con.execute("""
        SELECT asin,
               SUM(ABS(fba_fees)) AS fba_fees,
               SUM(ABS(commission)) AS commission
        FROM financial_events
        WHERE date >= ?
        GROUP BY asin
    """, [cutoff]).fetchall()
    fin_by_asin = {r[0]: {"fba_fees": r[1], "commission": r[2]} for r in fin_rows}

    products = []
    for r in rows:
        asin, revenue, units, sessions, glance_views = r
        orders = units  # proxy until per-ASIN order count is available
        api_name = inv_names.get(asin, {}).get("product_name", "")
        if units == 0:
            continue
        if not orders:
            orders = units
        aur = round(revenue / units, 2)

        cogs_info = cogs_data.get(asin) or {}
        cogs_per_unit = cogs_info.get("cogs", 0)
        if cogs_per_unit == 0:
            cogs_per_unit = round(aur * 0.35, 2)

        inv_info = inv_names.get(asin, {})
        cogs_name = cogs_info.get("product_name", "")
        if cogs_name and cogs_name.strip().upper() == asin.upper():
            cogs_name = ""
        name = (cogs_name
                or api_name
                or inv_info.get("product_name")
                or asin)
        resolved_sku = (cogs_info.get("sku", "")
                        or inv_info.get("sku", ""))

        fin = fin_by_asin.get(asin, {})
        actual_fba = fin.get("fba_fees", 0)
        est_fba_total = round(revenue * 0.12, 2)
        fba_total = actual_fba if actual_fba > 0 else est_fba_total

        actual_commission = fin.get("commission", 0)
        referral_total = round(actual_commission, 2) if actual_commission > 0 else round(revenue * 0.15, 2)

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
        sessions = w["sessions"]
        result.append({
            "date": date_key,
            "revenue": round(w["revenue"], 2),
            "units": w["units"],
            "orders": w["orders"],
            "sessions": sessions,
            "aur": round(w["revenue"] / units, 2),
            "convRate": round(w["orders"] / sessions * 100, 1) if sessions else 0,
        })
    return result


# ══════════════════════════════════════════════════════════════
# NEW SALES TAB — Period helpers and 11 API endpoints
# ══════════════════════════════════════════════════════════════


def _period_to_dates(period: str, start: str = None, end: str = None):
    """Convert period label to (start_date, end_date) tuple."""
    today = date.today()
    if period == 'today':
        return today, today
    elif period == 'yesterday':
        y = today - timedelta(days=1)
        return y, y
    elif period == '2_days_ago':
        d = today - timedelta(days=2)
        return d, d
    elif period == '3_days_ago':
        d = today - timedelta(days=3)
        return d, d
    elif period == '4_days_ago':
        d = today - timedelta(days=4)
        return d, d
    elif period == '5_days_ago':
        d = today - timedelta(days=5)
        return d, d
    elif period == '6_days_ago':
        d = today - timedelta(days=6)
        return d, d
    elif period == 'wtd':
        dow = today.weekday()
        return today - timedelta(days=dow), today
    elif period == 'mtd':
        return today.replace(day=1), today
    elif period == 'ytd' or period == '2026_ytd':
        return today.replace(month=1, day=1), today
    elif period == 'last_7d':
        return today - timedelta(days=7), today
    elif period == 'last_30d':
        return today - timedelta(days=30), today
    elif period == 'last_60d':
        return today - timedelta(days=60), today
    elif period == 'last_90d':
        return today - timedelta(days=90), today
    elif period == 'last_180d':
        return today - timedelta(days=180), today
    elif period == 'last_week':
        dow = today.weekday()
        end_lw = today - timedelta(days=dow + 1)
        return end_lw - timedelta(days=6), end_lw
    elif period == 'last_4w':
        return today - timedelta(weeks=4), today
    elif period == 'last_8w':
        return today - timedelta(weeks=8), today
    elif period == 'last_13w':
        return today - timedelta(weeks=13), today
    elif period == 'last_26w':
        return today - timedelta(weeks=26), today
    elif period == 'last_month':
        first = today.replace(day=1)
        last_m_end = first - timedelta(days=1)
        return last_m_end.replace(day=1), last_m_end
    elif period == '2_months_ago':
        # Full calendar month 2 months before current month
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)          # end of last month
        first_prev = last_prev.replace(day=1)               # start of last month
        end_2m = first_prev - timedelta(days=1)             # end of 2-months-ago month
        return end_2m.replace(day=1), end_2m
    elif period == '3_months_ago':
        # Full calendar month 3 months before current month
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        end_2m = first_prev - timedelta(days=1)
        first_2m = end_2m.replace(day=1)
        end_3m = first_2m - timedelta(days=1)
        return end_3m.replace(day=1), end_3m
    elif period == 'last_3m':
        return today - timedelta(days=90), today
    elif period == 'last_12m':
        return today - timedelta(days=365), today
    elif period == '2025_ytd':
        try:
            same_day_2025 = today.replace(year=2025)
        except ValueError:
            same_day_2025 = date(2025, 2, 28)
        return date(2025, 1, 1), same_day_2025
    elif period == '2025_full':
        return date(2025, 1, 1), date(2025, 12, 31)
    elif period == '2024_full':
        return date(2024, 1, 1), date(2024, 12, 31)
    elif period == 'custom' and start and end:
        return datetime.strptime(start, '%Y-%m-%d').date(), datetime.strptime(end, '%Y-%m-%d').date()
    else:
        return today - timedelta(days=30), today


def _hier_where(division=None, customer=None, table_alias=''):
    """Build division/customer WHERE clause for new sales endpoints."""
    prefix = table_alias + '.' if table_alias else ''
    clauses, params = [], []
    if division and division != 'all':
        clauses.append(f"{prefix}division = ?")
        params.append(division)
    if customer and customer != 'all' and customer != 'All Channels':
        clauses.append(f"{prefix}customer = ?")
        params.append(customer)
    return ('AND ' + ' AND '.join(clauses) if clauses else ''), params


# ── ENDPOINT 1: Sales summary metrics ──────────────────────
@router.get("/api/sales/summary")
def sales_summary(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """All KPI cards for the selected period, with LY equivalents."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer)

        # LY: shift back 364 days for weekday alignment
        ly_sd = sd - timedelta(days=364)
        ly_ed = ed - timedelta(days=364)

        def _sum_sales(s, e, extra_params):
            row = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0),
                       COALESCE(SUM(units_ordered), 0),
                       COALESCE(SUM(sessions), 0),
                       COALESCE(SUM(page_views), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            """, [str(s), str(e)] + extra_params).fetchone()
            return float(row[0]), int(row[1]), int(row[2]), int(row[3])

        def _count_orders(s, e, extra_params):
            """Count distinct orders from orders table for true AOV."""
            try:
                r = con.execute(f"""
                    SELECT COUNT(DISTINCT order_id)
                    FROM orders
                    WHERE purchase_date >= ? AND purchase_date <= ? {hw}
                """, [str(s), str(e)] + extra_params).fetchone()
                return int(r[0]) if r else 0
            except Exception:
                return 0

        ty_sales, ty_units, ty_sessions, ty_gv = _sum_sales(sd, ed, hp)

        # Auto-fallback: if period is 'today' and no TY data, use yesterday
        fell_back = False
        if period == 'today' and ty_sales == 0 and ty_units == 0 and ty_sessions == 0:
            sd = sd - timedelta(days=1)
            ed = ed - timedelta(days=1)
            ly_sd = sd - timedelta(days=364)
            ly_ed = ed - timedelta(days=364)
            ty_sales, ty_units, ty_sessions, ty_gv = _sum_sales(sd, ed, hp)
            fell_back = True

        ly_sales, ly_units, ly_sessions, ly_gv = _sum_sales(ly_sd, ly_ed, hp)

        ty_aur = round(ty_sales / ty_units, 2) if ty_units else 0
        ly_aur = round(ly_sales / ly_units, 2) if ly_units else 0
        ty_orders = _count_orders(sd, ed, hp)
        ly_orders = _count_orders(ly_sd, ly_ed, hp)
        # Fallback: if orders table has no data, use units as proxy
        if ty_orders == 0:
            ty_orders = ty_units
        if ly_orders == 0:
            ly_orders = ly_units
        ty_aov = round(ty_sales / ty_orders, 2) if ty_orders else 0
        ly_aov = round(ly_sales / ly_orders, 2) if ly_orders else 0
        ty_conv = round(ty_units / ty_sessions, 4) if ty_sessions else 0
        ly_conv = round(ly_units / ly_sessions, 4) if ly_sessions else 0
        # CTR from advertising table (clicks / impressions); 0 when ads data absent
        def _ads_ctr(s, e, extra_params):
            try:
                r = con.execute(f"""
                    SELECT COALESCE(SUM(clicks), 0), COALESCE(SUM(impressions), 0)
                    FROM advertising
                    WHERE date BETWEEN ? AND ? {hw}
                """, [s, e] + extra_params).fetchone()
                clicks, imps = int(r[0]), int(r[1])
                return round(clicks / imps, 4) if imps else 0
            except Exception:
                return 0
        ty_ctr = _ads_ctr(sd, ed, hp)
        ly_ctr = _ads_ctr(ly_sd, ly_ed, hp)

        # Amazon fees from financial_events
        def _sum_fees(s, e, extra_params):
            try:
                r = con.execute(f"""
                    SELECT COALESCE(SUM(ABS(fba_fees)) + SUM(ABS(commission)), 0)
                    FROM financial_events
                    WHERE date >= ? AND date <= ? {hw}
                """, [str(s), str(e)] + extra_params).fetchone()
                return float(r[0]) if r else 0
            except Exception:
                return 0

        ty_fees = _sum_fees(sd, ed, hp)
        ly_fees = _sum_fees(ly_sd, ly_ed, hp)

        # Ad spend
        def _sum_ads(s, e):
            try:
                r = con.execute("""
                    SELECT COALESCE(SUM(spend), 0), COALESCE(SUM(sales), 0)
                    FROM advertising
                    WHERE date >= ? AND date <= ?
                """, [str(s), str(e)]).fetchone()
                return float(r[0]), float(r[1])
            except Exception:
                return 0, 0

        ty_ad_spend, ty_ad_sales = _sum_ads(sd, ed)
        ly_ad_spend, ly_ad_sales = _sum_ads(ly_sd, ly_ed)

        ty_roas = round(ty_ad_sales / ty_ad_spend, 2) if ty_ad_spend else 0
        ly_roas = round(ly_ad_sales / ly_ad_spend, 2) if ly_ad_spend else 0
        ty_tacos = round(ty_ad_spend / ty_sales, 4) if ty_sales else 0
        ly_tacos = round(ly_ad_spend / ly_sales, 4) if ly_sales else 0

        # COGS: Phase 6 — return 0 for now
        ty_cogs = 0
        ly_cogs = 0
        ty_gm = round(ty_sales - ty_fees - ty_cogs, 2)
        ly_gm = round(ly_sales - ly_fees - ly_cogs, 2)
        ty_gm_pct = round(ty_gm / ty_sales, 4) if ty_sales else 0
        ly_gm_pct = round(ly_gm / ly_sales, 4) if ly_sales else 0

        # Inventory: current stock and days of supply
        try:
            inv_row = con.execute("""
                SELECT COALESCE(SUM(fulfillable_quantity), 0),
                       COALESCE(SUM(total_quantity), 0)
                FROM fba_inventory
                WHERE date = (SELECT MAX(date) FROM fba_inventory)
            """).fetchone()
            stock_units = int(inv_row[0]) if inv_row else 0
            # Avg daily units last 30 days for DOS
            avg_row = con.execute(f"""
                SELECT COALESCE(AVG(daily_units), 0) FROM (
                    SELECT date, SUM(units_ordered) AS daily_units
                    FROM daily_sales
                    WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
                    GROUP BY date
                )
            """, [str(ed - timedelta(days=30)), str(ed)] + hp).fetchone()
            avg_daily = float(avg_row[0]) if avg_row else 0
            dos = round(stock_units / avg_daily) if avg_daily > 0 else 0
        except Exception:
            stock_units = 0
            dos = 0

        con.close()
        return {
            "sales": round(ty_sales, 2), "unit_sales": ty_units, "aur": ty_aur,
            "cogs": ty_cogs, "amazon_fees": round(ty_fees, 2),
            "gross_margin": ty_gm, "gross_margin_pct": ty_gm_pct,
            "sessions": ty_sessions, "glance_views": ty_gv,
            "ctr": ty_ctr, "conversion": ty_conv,
            "orders": ty_orders, "aov": ty_aov,
            "ad_spend": round(ty_ad_spend, 2), "roas": ty_roas, "tacos": ty_tacos,
            "ly_sales": round(ly_sales, 2), "ly_unit_sales": ly_units, "ly_aur": ly_aur,
            "ly_cogs": ly_cogs, "ly_amazon_fees": round(ly_fees, 2),
            "ly_gross_margin": ly_gm, "ly_gross_margin_pct": ly_gm_pct,
            "ly_sessions": ly_sessions, "ly_glance_views": ly_gv,
            "ly_ctr": ly_ctr, "ly_conversion": ly_conv,
            "ly_orders": ly_orders, "ly_aov": ly_aov,
            "ly_ad_spend": round(ly_ad_spend, 2), "ly_roas": ly_roas, "ly_tacos": ly_tacos,
            "dos": dos, "stock_units": stock_units,
            "fell_back": fell_back,
            "period_label": f"Yesterday ({sd.strftime('%b %d')})" if fell_back else None,
        }
    except Exception as e:
        logger.error(f"sales/summary error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 2: Daily trend data ───────────────────────────
@router.get("/api/sales/trend")
def sales_trend(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Daily rows for TY and LY for line charts."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer)

        rows = con.execute(f"""
            SELECT date,
                   COALESCE(SUM(ordered_product_sales), 0),
                   COALESCE(SUM(sessions), 0),
                   COALESCE(SUM(units_ordered), 0)
            FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            GROUP BY date ORDER BY date
        """, [str(sd), str(ed)] + hp).fetchall()

        # Build LY lookup: shift back 364 days
        ly_sd = sd - timedelta(days=364)
        ly_ed = ed - timedelta(days=364)
        ly_rows = con.execute(f"""
            SELECT date,
                   COALESCE(SUM(ordered_product_sales), 0),
                   COALESCE(SUM(sessions), 0),
                   COALESCE(SUM(units_ordered), 0)
            FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            GROUP BY date ORDER BY date
        """, [str(ly_sd), str(ly_ed)] + hp).fetchall()
        con.close()

        ly_map = {}
        for r in ly_rows:
            d = fmt_date(r[0])
            ly_map[d] = {"sales": float(r[1]), "sessions": int(r[2]), "units": int(r[3])}

        result = []
        for r in rows:
            d_str = fmt_date(r[0])
            ty_s = float(r[1])
            ty_sess = int(r[2])
            ty_u = int(r[3])
            # Find LY equivalent date (364 days prior)
            try:
                d_obj = datetime.strptime(d_str, "%Y-%m-%d").date()
                ly_d = (d_obj - timedelta(days=364)).strftime("%Y-%m-%d")
            except Exception:
                ly_d = None
            ly = ly_map.get(ly_d, {})
            ty_aur = round(ty_s / ty_u, 2) if ty_u else None
            ty_conv = round(ty_u / ty_sess, 4) if ty_sess else None
            ly_aur = round(ly.get("sales", 0) / ly.get("units", 1), 2) if ly.get("units") else None
            ly_conv = round(ly.get("units", 0) / ly.get("sessions", 1), 4) if ly.get("sessions") else None
            result.append({
                "date": d_str,
                "ty_sales": round(ty_s, 2),
                "ly_sales": round(ly.get("sales", 0), 2) if ly else None,
                "ty_sessions": ty_sess,
                "ly_sessions": ly.get("sessions") if ly else None,
                "ty_aur": ty_aur,
                "ly_aur": ly_aur,
                "ty_conv": ty_conv,
                "ly_conv": ly_conv,
            })
        return result
    except Exception as e:
        logger.error(f"sales/trend error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 3: Period comparison columns ──────────────────
@router.get("/api/sales/period-comparison")
def sales_period_comparison(
    view: str = Query("exec"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
):
    """Return KPI columns for the selected view tab."""
    try:
        con = get_db()
        hw, hp = _hier_where(division, customer)
        today = date.today()

        view_periods = {
            "exec": {
                "Today": "today", "Yesterday": "yesterday",
                "WTD": "wtd", "MTD": "mtd", "YTD": "ytd",
            },
            "daily": {
                "Today": "today", "Yesterday": "yesterday",
            },
            "weekly": {
                "WTD": "wtd", "Last Week": "last_week",
                "4 Weeks": "last_4w", "8 Weeks": "last_8w",
                "13 Weeks": "last_13w", "26 Weeks": "last_26w",
            },
            "monthly": {
                "MTD": "mtd", "Last Month": "last_month",
                "Last 12 Months": "last_12m",
            },
            "yearly": {
                "2026 YTD": "2026_ytd",
                "2025 YTD (Same Period)": "2025_ytd",
                "2025 Full Year": "2025_full",
                "2024 Full Year": "2024_full",
            },
        }

        periods_map = view_periods.get(view, view_periods["exec"])
        result = {}

        for label, period_key in periods_map.items():
            sd, ed = _period_to_dates(period_key)
            ly_sd = sd - timedelta(days=364)
            ly_ed = ed - timedelta(days=364)
            row = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0),
                       COALESCE(SUM(units_ordered), 0),
                       COALESCE(SUM(sessions), 0),
                       COALESCE(SUM(page_views), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            """, [str(sd), str(ed)] + hp).fetchone()
            sales, units, sessions, glance_views = float(row[0]), int(row[1]), int(row[2]), int(row[3])
            ly_row = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0),
                       COALESCE(SUM(units_ordered), 0),
                       COALESCE(SUM(sessions), 0),
                       COALESCE(SUM(page_views), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            """, [str(ly_sd), str(ly_ed)] + hp).fetchone()
            ly_sales, ly_units, ly_sessions, ly_gv = float(ly_row[0]), int(ly_row[1]), int(ly_row[2]), int(ly_row[3])
            # True order count from orders table
            try:
                o_row = con.execute(f"""
                    SELECT COUNT(DISTINCT order_id)
                    FROM orders
                    WHERE purchase_date >= ? AND purchase_date <= ? {hw}
                """, [str(sd), str(ed)] + hp).fetchone()
                orders = int(o_row[0]) if o_row and o_row[0] else units
            except Exception:
                orders = units
            try:
                ly_o_row = con.execute(f"""
                    SELECT COUNT(DISTINCT order_id)
                    FROM orders
                    WHERE purchase_date >= ? AND purchase_date <= ? {hw}
                """, [str(ly_sd), str(ly_ed)] + hp).fetchone()
                ly_orders = int(ly_o_row[0]) if ly_o_row and ly_o_row[0] else ly_units
            except Exception:
                ly_orders = ly_units
            aur = round(sales / units, 2) if units else 0
            aov = round(sales / orders, 2) if orders else 0
            conv = round(units / sessions, 4) if sessions else 0
            ctr = 0
            ly_aur = round(ly_sales / ly_units, 2) if ly_units else 0
            ly_aov = round(ly_sales / ly_orders, 2) if ly_orders else 0
            ly_conv = round(ly_units / ly_sessions, 4) if ly_sessions else 0
            result[label] = {
                "sales": round(sales, 2), "units": units, "aur": aur,
                "orders": orders, "aov": aov,
                "sessions": sessions, "glance_views": glance_views,
                "ctr": ctr, "conversion": conv,
                "ly_sales": round(ly_sales, 2), "ly_units": ly_units,
                "ly_aur": ly_aur, "ly_orders": ly_orders, "ly_aov": ly_aov,
                "ly_sessions": ly_sessions, "ly_glance_views": ly_gv,
                "ly_conversion": ly_conv,
            }

        con.close()
        return result
    except Exception as e:
        logger.error(f"sales/period-comparison error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 4: Monthly YOY bar chart data ─────────────────
@router.get("/api/sales/monthly-yoy")
def sales_monthly_yoy(
    division: str | None = Query(None),
    customer: str | None = Query(None),
):
    """12 months x 3 years for YOY grouped bar chart."""
    try:
        con = get_db()
        hw, hp = _hier_where(division, customer)
        month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

        rows = con.execute(f"""
            SELECT EXTRACT(YEAR FROM date) AS yr,
                   EXTRACT(MONTH FROM date) AS mo,
                   COALESCE(SUM(ordered_product_sales), 0) AS revenue
            FROM daily_sales
            WHERE asin = 'ALL' AND date IS NOT NULL AND date >= '2024-01-01' {hw}
            GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
            ORDER BY yr, mo
        """, hp).fetchall()
        con.close()

        months_map = {}
        for r in rows:
            yr, mo, rev = int(r[0]), int(r[1]), round(float(r[2]), 2)
            if mo not in months_map:
                months_map[mo] = {}
            months_map[mo][yr] = rev

        current_month = date.today().month
        current_year = date.today().year
        result = []
        for mo in range(1, 13):
            entry = {
                "month": month_names[mo - 1], "month_num": mo,
                "y2024": months_map.get(mo, {}).get(2024, 0),
                "y2025": months_map.get(mo, {}).get(2025, 0),
                "y2026": months_map.get(mo, {}).get(2026, None) if (current_year > 2026 or (current_year == 2026 and mo <= current_month)) else None,
            }
            result.append(entry)
        return result
    except Exception as e:
        logger.error(f"sales/monthly-yoy error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 5: Revenue by channel (stacked bar) ──────────
@router.get("/api/sales/by-channel")
def sales_by_channel(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Weekly revenue split by customer/channel."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)

        # Get weekly revenue by customer
        rows = con.execute("""
            SELECT customer,
                   date,
                   COALESCE(SUM(ordered_product_sales), 0) AS revenue
            FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ?
              AND date IS NOT NULL
            GROUP BY customer, date
            ORDER BY date
        """, [str(sd), str(ed)]).fetchall()
        con.close()

        # Bucket into weeks
        weekly = defaultdict(lambda: defaultdict(float))
        for r in rows:
            cust = r[0] or 'amazon'
            d = fmt_date(r[1])
            rev = float(r[2])
            try:
                dt = datetime.strptime(d, "%Y-%m-%d")
                week_start = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")
            except Exception:
                continue
            # Map customer names to canonical channel keys
            c_key = cust.lower().replace("'", "").replace(" ", "_")
            if c_key not in ('amazon', 'walmart_marketplace', 'walmart_stores', 'shopify'):
                c_key = 'store_other'
            weekly[week_start][c_key] += rev

        result = []
        for i, ws in enumerate(sorted(weekly.keys())):
            entry = {
                "week": f"W-{len(weekly) - i}",
                "week_start": ws,
                "amazon": round(weekly[ws].get("amazon", 0), 2),
                "walmart_marketplace": round(weekly[ws].get("walmart_marketplace", 0), 2),
                "walmart_stores": round(weekly[ws].get("walmart_stores", 0), 2),
                "shopify": round(weekly[ws].get("shopify", 0), 2),
                "store_other": round(weekly[ws].get("store_other", 0), 2),
            }
            result.append(entry)
        return result
    except Exception as e:
        logger.error(f"sales/by-channel error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 6: Rolling 4-week data ───────────────────────
@router.get("/api/sales/rolling")
def sales_rolling(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Weekly rolling 4-week averages for TY and LY."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer)
        # Extend window 4 weeks back for rolling calc
        ext_sd = sd - timedelta(weeks=4)
        ly_ext_sd = ext_sd - timedelta(days=364)
        ly_ed = ed - timedelta(days=364)

        def _weekly_rev(s, e, params):
            rows = con.execute(f"""
                SELECT date, COALESCE(SUM(ordered_product_sales), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
                GROUP BY date ORDER BY date
            """, [str(s), str(e)] + params).fetchall()
            weeks = defaultdict(float)
            for r in rows:
                d = fmt_date(r[0])
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                    ws = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")
                except Exception:
                    continue
                weeks[ws] += float(r[1])
            return dict(sorted(weeks.items()))

        ty_weeks = _weekly_rev(ext_sd, ed, hp)
        ly_weeks = _weekly_rev(ly_ext_sd, ly_ed, hp)
        con.close()

        # Compute rolling 4-week averages
        ty_keys = sorted(ty_weeks.keys())
        ly_keys = sorted(ly_weeks.keys())
        result = []
        for i, wk in enumerate(ty_keys):
            if i < 3:
                continue  # need 4 weeks
            window = ty_keys[i-3:i+1]
            ty_rolling = round(sum(ty_weeks[w] for w in window) / 4, 2)
            # Find corresponding LY week
            ly_rolling = None
            if len(ly_keys) > i:
                ly_window = ly_keys[max(0, i-3):i+1]
                if len(ly_window) == 4:
                    ly_rolling = round(sum(ly_weeks.get(w, 0) for w in ly_window) / 4, 2)
            result.append({
                "week": f"W-{len(ty_keys) - i}",
                "ty_rolling": ty_rolling,
                "ly_rolling": ly_rolling,
            })
        return result
    except Exception as e:
        logger.error(f"sales/rolling error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 7: Units heatmap data ────────────────────────
@router.get("/api/sales/heatmap")
def sales_heatmap(
    division: str | None = Query(None),
    customer: str | None = Query(None),
):
    """26 weeks x 7 days heatmap of units sold."""
    try:
        con = get_db()
        hw, hp = _hier_where(division, customer)
        today = date.today()
        start_date = today - timedelta(weeks=26)

        rows = con.execute(f"""
            SELECT date, COALESCE(SUM(units_ordered), 0)
            FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            GROUP BY date ORDER BY date
        """, [str(start_date), str(today)] + hp).fetchall()
        con.close()

        result = []
        for r in rows:
            d_str = fmt_date(r[0])
            units = int(r[1])
            try:
                dt = datetime.strptime(d_str, "%Y-%m-%d").date()
            except Exception:
                continue
            days_ago = (today - dt).days
            week = days_ago // 7
            day = dt.weekday()  # 0=Mon, 6=Sun
            day_names = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
            if week < 26:
                result.append({
                    "week": week, "day": day,
                    "day_name": day_names[day], "units": units,
                })
        return result
    except Exception as e:
        logger.error(f"sales/heatmap error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 8: Conversion funnel ─────────────────────────
@router.get("/api/sales/funnel")
def sales_funnel(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Conversion funnel: GV -> Sessions -> Add to Cart -> Orders."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer)
        ly_sd = sd - timedelta(days=364)
        ly_ed = ed - timedelta(days=364)

        def _funnel(s, e, params):
            row = con.execute(f"""
                SELECT COALESCE(SUM(page_views), 0),
                       COALESCE(SUM(sessions), 0),
                       COALESCE(SUM(units_ordered), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            """, [str(s), str(e)] + params).fetchone()
            gv = int(row[0])
            sess = int(row[1]) if row[1] else gv  # sessions fallback to page_views
            units = int(row[2])
            # Estimate add-to-cart as ~3x orders (industry average)
            atc = int(units * 3.2) if units else 0
            return gv, sess, atc, units

        ty_gv, ty_sess, ty_atc, ty_orders = _funnel(sd, ed, hp)
        ly_gv, ly_sess, ly_atc, ly_orders = _funnel(ly_sd, ly_ed, hp)
        con.close()

        return [
            {"label": "Glance Views", "ty": ty_gv, "ly": ly_gv},
            {"label": "Sessions", "ty": ty_sess, "ly": ly_sess},
            {"label": "Add to Cart", "ty": ty_atc, "ly": ly_atc},
            {"label": "Orders", "ty": ty_orders, "ly": ly_orders},
        ]
    except Exception as e:
        logger.error(f"sales/funnel error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 9: Ad efficiency quadrant data ────────────────
@router.get("/api/sales/ad-efficiency")
def sales_ad_efficiency(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """ACOS/ROAS quadrant data for TY vs LY."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        ly_sd = sd - timedelta(days=364)
        ly_ed = ed - timedelta(days=364)

        def _ad_metrics(s, e):
            try:
                row = con.execute("""
                    SELECT COALESCE(SUM(spend), 0),
                           COALESCE(SUM(sales), 0)
                    FROM advertising
                    WHERE date >= ? AND date <= ?
                """, [str(s), str(e)]).fetchone()
                spend = float(row[0])
                sales = float(row[1])
            except Exception:
                spend, sales = 0, 0
            # Get total revenue for TACOS calculation
            try:
                rev_row = con.execute(f"""
                    SELECT COALESCE(SUM(ordered_product_sales), 0)
                    FROM daily_sales
                    WHERE asin = 'ALL' AND date >= ? AND date <= ?
                """, [str(s), str(e)]).fetchone()
                total_rev = float(rev_row[0]) if rev_row else 0
            except Exception:
                total_rev = 0

            acos = round(spend / sales, 4) if sales else 0
            roas = round(sales / spend, 2) if spend else 0
            tacos = round(spend / total_rev, 4) if total_rev else 0
            return {
                "acos": acos, "roas": roas, "tacos": tacos,
                "ad_spend": round(spend, 2), "ad_sales": round(sales, 2),
            }

        ty = _ad_metrics(sd, ed)
        ly = _ad_metrics(ly_sd, ly_ed)
        con.close()
        return {"ty": ty, "ly": ly}
    except Exception as e:
        logger.error(f"sales/ad-efficiency error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 10: Fee breakdown ─────────────────────────────
@router.get("/api/sales/fee-breakdown")
def sales_fee_breakdown(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Break down Amazon fees by type."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer)

        # Sum FBA fees and commission separately
        row = con.execute(f"""
            SELECT COALESCE(SUM(ABS(fba_fees)), 0),
                   COALESCE(SUM(ABS(commission)), 0),
                   COALESCE(SUM(ABS(other_fees)), 0)
            FROM financial_events
            WHERE date >= ? AND date <= ? {hw}
        """, [str(sd), str(ed)] + hp).fetchone()
        con.close()

        fba = round(float(row[0]), 2)
        referral = round(float(row[1]), 2)
        other = round(float(row[2]), 2)
        # Estimate storage from other_fees or use a portion
        storage = round(other * 0.6, 2) if other else 0
        misc = round(other - storage, 2) if other else 0

        result = [
            {"type": "FBA Fulfillment", "amount": fba},
            {"type": "Referral Fee", "amount": referral},
            {"type": "Storage Fees", "amount": storage},
            {"type": "Other Fees", "amount": misc},
        ]
        # Filter out zero entries
        return [r for r in result if r["amount"] > 0] or result
    except Exception as e:
        logger.error(f"sales/fee-breakdown error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 11: Ad spend breakdown ────────────────────────
@router.get("/api/sales/ad-breakdown")
def sales_ad_breakdown(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Break down ad spend by campaign type."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)

        # Try ads_campaigns which has campaign_type
        try:
            rows = con.execute("""
                SELECT COALESCE(campaign_type, 'SP') AS ctype,
                       COALESCE(SUM(spend), 0),
                       COALESCE(SUM(sales), 0)
                FROM ads_campaigns
                WHERE date >= ? AND date <= ?
                GROUP BY COALESCE(campaign_type, 'SP')
            """, [str(sd), str(ed)]).fetchall()
        except Exception:
            rows = []

        con.close()

        type_labels = {
            'SP': 'Sponsored Products',
            'SB': 'Sponsored Brands',
            'SD': 'Sponsored Display',
            'DSP': 'DSP',
        }

        result = []
        for r in rows:
            ctype = r[0] or 'SP'
            spend = round(float(r[1]), 2)
            sales = round(float(r[2]), 2)
            roas = round(sales / spend, 2) if spend else 0
            result.append({
                "type": type_labels.get(ctype, ctype),
                "spend": spend,
                "sales": sales,
                "roas": roas,
            })

        # If no data from ads_campaigns, try advertising table as single bucket
        if not result:
            try:
                con2 = get_db()
                row = con2.execute("""
                    SELECT COALESCE(SUM(spend), 0), COALESCE(SUM(sales), 0)
                    FROM advertising
                    WHERE date >= ? AND date <= ?
                """, [str(sd), str(ed)]).fetchone()
                con2.close()
                if row:
                    spend = round(float(row[0]), 2)
                    sales = round(float(row[1]), 2)
                    roas = round(sales / spend, 2) if spend else 0
                    if spend > 0:
                        result.append({
                            "type": "Sponsored Products",
                            "spend": spend,
                            "sales": sales,
                            "roas": roas,
                        })
            except Exception:
                pass

        return result
    except Exception as e:
        logger.error(f"sales/ad-breakdown error: {e}")
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════
# LEGACY ENDPOINTS — used by Dashboard.jsx and other pages
# ══════════════════════════════════════════════════════════════


@router.get("/api/summary")
def summary(
    days: int = Query(365, description="Number of days to include"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
):
    """High-level KPIs: revenue, units, orders, sessions, AUR, conv rate."""
    con = get_db()
    cutoff = (get_today(con) - timedelta(days=days)).strftime("%Y-%m-%d") if days > 0 else get_today(con).strftime("%Y-%m-%d")
    hf, hp = _hierarchy_filter(division, customer, platform)

    revenue = 0
    units = 0
    used_analytics = False
    try:
        analytics_row = con.execute(f"""
            SELECT
                COALESCE(SUM(gross_revenue), 0) AS revenue,
                COALESCE(SUM(units_sold), 0) AS units
            FROM analytics_daily
            WHERE date >= ?{hf}
        """, [cutoff] + hp).fetchone()
        if analytics_row and analytics_row[0] > 0:
            revenue, units = float(analytics_row[0]), int(analytics_row[1])
            used_analytics = True
    except Exception:
        pass

    if not used_analytics:
        row = con.execute(f"""
            SELECT
                COALESCE(SUM(ordered_product_sales), 0) AS revenue,
                COALESCE(SUM(units_ordered), 0) AS units
            FROM daily_sales
            WHERE date >= ? AND asin = 'ALL'{hf}
        """, [cutoff] + hp).fetchone()
        revenue, units = float(row[0]), int(row[1])

    sessions_row = con.execute(f"""
        SELECT COALESCE(SUM(sessions), 0), COALESCE(SUM(page_views), 0)
        FROM daily_sales
        WHERE date >= ? AND asin = 'ALL'{hf}
    """, [cutoff] + hp).fetchone()
    sessions = int(sessions_row[0])
    glance_views = int(sessions_row[1])

    # True order count from orders table
    try:
        o_row = con.execute(f"""
            SELECT COUNT(DISTINCT order_id)
            FROM orders
            WHERE purchase_date >= ?{hf}
        """, [cutoff] + hp).fetchone()
        orders = int(o_row[0]) if o_row and o_row[0] else units
    except Exception:
        orders = units
    aur = round(revenue / units, 2) if units else 0
    conv_rate = round(units / sessions * 100, 1) if sessions else 0

    products = _build_product_list(con, cutoff)
    prod_rev = sum(p["rev"] for p in products)
    prod_net = sum(p["net"] for p in products)

    if prod_rev > 0 and revenue > 0:
        margin_pct = prod_net / prod_rev
        total_net = round(revenue * margin_pct, 2)
        margin = round(margin_pct * 100)
    elif revenue > 0:
        # Fallback: estimate costs from financial_events + defaults
        try:
            fee_row = con.execute(f"""
                SELECT COALESCE(SUM(ABS(fba_fees)), 0),
                       COALESCE(SUM(ABS(commission)), 0)
                FROM financial_events
                WHERE date >= ?{hf}
            """, [cutoff] + hp).fetchone()
            est_fba = float(fee_row[0]) if fee_row and fee_row[0] > 0 else revenue * 0.12
            est_ref = float(fee_row[1]) if fee_row and fee_row[1] > 0 else revenue * 0.15
        except Exception:
            est_fba = revenue * 0.12
            est_ref = revenue * 0.15
        est_cogs = revenue * 0.35
        total_net = round(revenue - est_cogs - est_fba - est_ref, 2)
        margin = round(total_net / revenue * 100)
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


@router.get("/api/daily")
def get_daily_sales(
    days: int = Query(365),
    granularity: str = Query("daily"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
):
    """Time-series sales data for charts."""
    con = get_db()
    cutoff = (get_today(con) - timedelta(days=days)).strftime("%Y-%m-%d")
    hf, hp = _hierarchy_filter(division, customer, platform)

    analytics_data = {}
    try:
        analytics_rows = con.execute(f"""
            SELECT date, SUM(gross_revenue) AS revenue, SUM(units_sold) AS units
            FROM analytics_daily
            WHERE date >= ?{hf}
            GROUP BY date ORDER BY date
        """, [cutoff] + hp).fetchall()
        if analytics_rows and len(analytics_rows) > 0:
            for r in analytics_rows:
                analytics_data[str(r[0])] = {"revenue": float(r[1] or 0), "units": int(r[2] or 0)}
    except Exception:
        pass

    rows = con.execute(f"""
        SELECT date,
               COALESCE(ordered_product_sales, 0) AS revenue,
               COALESCE(units_ordered, 0) AS units,
               COALESCE(sessions, 0) AS sessions
        FROM daily_sales
        WHERE date >= ? AND asin = 'ALL'{hf}
        ORDER BY date
    """, [cutoff] + hp).fetchall()
    con.close()

    data = []
    for r in rows:
        date_str = fmt_date(r[0])
        if date_str in analytics_data:
            revenue_val = analytics_data[date_str]["revenue"]
            units_val = analytics_data[date_str]["units"]
        else:
            revenue_val = float(r[1])
            units_val = int(r[2] or 0)
        sessions_val = int(r[3] or 0)

        data.append({
            "date": date_str,
            "revenue": round(revenue_val, 2),
            "units": units_val,
            "orders": units_val,
            "sessions": sessions_val,
            "aur": round(revenue_val / units_val, 2) if units_val else 0,
            "convRate": round(units_val / sessions_val * 100, 1) if sessions_val else 0,
        })

    if granularity == "weekly" and data:
        data = _aggregate_weekly(data)

    return {"granularity": granularity, "days": days, "data": data}


@router.get("/api/products")
def products(
    days: int = Query(365),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
):
    """Product breakdown with profitability metrics."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    product_list = _build_product_list(con, cutoff)
    con.close()
    return {"days": days, "products": product_list}


@router.get("/api/product/{asin}")
def product_detail(asin: str, days: int = Query(365)):
    """Detailed view for a single product including daily trend."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    cogs_data = load_cogs()

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


@router.get("/api/comparison")
def period_comparison(
    view: str = Query("realtime"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
):
    """Return KPI comparison across multiple time periods (legacy)."""
    con = get_db()
    cogs_data = load_cogs()

    today_start = get_today(con)
    today_start = today_start.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = (today_start + timedelta(days=1)).strftime("%Y-%m-%d")
    yr = today_start.year
    month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

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
            periods.append({
                "label": month_names[m - 1], "sub": str(yr),
                "start": m_start.strftime("%Y-%m-%d"), "end": m_end,
            })
        periods.append({
            "label": f"{yr} Total", "sub": "YTD",
            "start": f"{yr}-01-01", "end": tomorrow,
        })
    else:
        week_start = today_start - timedelta(days=6)
        month_start = today_start.replace(day=1)
        year_start = today_start.replace(month=1, day=1)
        periods = [
            {"label": "Today", "start": today_start.strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "WTD", "start": week_start.strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "MTD", "start": month_start.strftime("%Y-%m-%d"), "end": tomorrow},
            {"label": "YTD", "start": year_start.strftime("%Y-%m-%d"), "end": tomorrow},
        ]

    hf, hp = _hierarchy_filter(division, customer, platform)

    def chg(cur, prev):
        if prev == 0:
            return 0
        return round((cur - prev) / prev * 100, 1)

    full_products = _build_product_list(con, f"{yr-1}-01-01")
    full_prod_rev = sum(pp["rev"] for pp in full_products)
    full_prod_net = sum(pp["net"] for pp in full_products)
    full_prod_cogs = sum(pp["cogsTotal"] for pp in full_products)
    full_prod_fba = sum(pp["fbaTotal"] for pp in full_products)
    full_prod_referral = sum(pp["referralTotal"] for pp in full_products)

    if full_prod_rev > 0:
        margin_pct = full_prod_net / full_prod_rev
        cogs_pct = full_prod_cogs / full_prod_rev
        fba_pct = full_prod_fba / full_prod_rev
        referral_pct = full_prod_referral / full_prod_rev
    else:
        # Fallback estimates when no per-ASIN data is available
        cogs_pct = 0.35   # 35% of revenue
        fba_pct = 0.12    # 12% FBA fulfillment estimate
        referral_pct = 0.15  # 15% referral fee estimate
        margin_pct = 1.0 - cogs_pct - fba_pct - referral_pct  # ~38% margin

    results = []
    for p in periods:
        row = con.execute(f"""
            SELECT
                COALESCE(SUM(ordered_product_sales), 0),
                COALESCE(SUM(units_ordered), 0),
                COALESCE(SUM(sessions), 0)
            FROM daily_sales
            WHERE date >= ? AND date < ? AND asin = 'ALL'{hf}
        """, [p["start"], p["end"]] + hp).fetchone()

        rev, units, sessions = row
        # True order count from orders table
        try:
            o_row = con.execute(f"""
                SELECT COUNT(DISTINCT order_id) FROM orders
                WHERE purchase_date >= ? AND purchase_date < ?{hf}
            """, [p["start"], p["end"]] + hp).fetchone()
            orders = int(o_row[0]) if o_row and o_row[0] else units
        except Exception:
            orders = units
        aur = round(rev / units, 2) if units else 0
        conv = round(units / sessions * 100, 1) if sessions else 0

        ad_spend = 0
        try:
            ad_row = con.execute("""
                SELECT COALESCE(SUM(spend), 0)
                FROM advertising WHERE date >= ? AND date < ?
            """, [p["start"], p["end"]]).fetchone()
            ad_spend = round(ad_row[0], 2) if ad_row else 0
        except Exception:
            pass

        tacos = round(ad_spend / rev * 100, 1) if rev > 0 else 0

        # Try actual fees from financial_events first, then fall back to ratio estimates
        actual_fba, actual_referral = 0, 0
        try:
            fee_row = con.execute(f"""
                SELECT COALESCE(SUM(ABS(fba_fees)), 0),
                       COALESCE(SUM(ABS(commission)), 0)
                FROM financial_events
                WHERE date >= ? AND date < ?{hf}
            """, [p["start"], p["end"]] + hp).fetchone()
            if fee_row:
                actual_fba = round(float(fee_row[0]), 2)
                actual_referral = round(float(fee_row[1]), 2)
        except Exception:
            pass

        # Use actual fees if available, otherwise use ratio-based estimates
        if actual_fba > 0 or actual_referral > 0:
            fba_fees = actual_fba
            referral_fees = actual_referral
        else:
            fba_fees = round(rev * fba_pct, 2)
            referral_fees = round(rev * referral_pct, 2)
        amazon_fees = round(fba_fees + referral_fees, 2)
        cogs = round(rev * cogs_pct, 2)
        net = round(rev - cogs - amazon_fees - ad_spend, 2)
        margin_val = round(net / rev * 100, 1) if rev > 0 else 0

        try:
            ly_start = datetime.strptime(p["start"], "%Y-%m-%d").replace(year=datetime.strptime(p["start"], "%Y-%m-%d").year - 1)
            ly_end = datetime.strptime(p["end"], "%Y-%m-%d").replace(year=datetime.strptime(p["end"], "%Y-%m-%d").year - 1)
        except ValueError:
            ly_start = datetime.strptime(p["start"], "%Y-%m-%d") - timedelta(days=365)
            ly_end = datetime.strptime(p["end"], "%Y-%m-%d") - timedelta(days=365)

        ly_row = con.execute(f"""
            SELECT
                COALESCE(SUM(ordered_product_sales), 0),
                COALESCE(SUM(units_ordered), 0),
                COALESCE(SUM(sessions), 0)
            FROM daily_sales
            WHERE date >= ? AND date < ? AND asin = 'ALL'{hf}
        """, [ly_start.strftime("%Y-%m-%d"), ly_end.strftime("%Y-%m-%d")] + hp).fetchone()

        ly_rev, ly_units, ly_sessions = ly_row
        try:
            ly_o_row = con.execute(f"""
                SELECT COUNT(DISTINCT order_id) FROM orders
                WHERE purchase_date >= ? AND purchase_date < ?{hf}
            """, [ly_start.strftime("%Y-%m-%d"), ly_end.strftime("%Y-%m-%d")] + hp).fetchone()
            ly_orders = int(ly_o_row[0]) if ly_o_row and ly_o_row[0] else ly_units
        except Exception:
            ly_orders = ly_units

        ly_ad = 0
        try:
            ly_ad_row = con.execute("""
                SELECT COALESCE(SUM(spend), 0)
                FROM advertising WHERE date >= ? AND date < ?
            """, [ly_start.strftime("%Y-%m-%d"), ly_end.strftime("%Y-%m-%d")]).fetchone()
            ly_ad = round(ly_ad_row[0], 2) if ly_ad_row else 0
        except Exception:
            pass

        results.append({
            "label": p["label"], "sub": p.get("sub", ""),
            "revenue": round(rev, 2), "units": units, "orders": orders,
            "aur": aur, "sessions": sessions, "convRate": conv,
            "adSpend": ad_spend, "tacos": tacos,
            "cogs": cogs, "amazonFees": amazon_fees,
            "fbaFees": fba_fees, "referralFees": referral_fees,
            "netProfit": net, "margin": margin_val,
            "revChg": chg(rev, ly_rev), "unitChg": chg(units, ly_units),
            "orderChg": chg(orders, ly_orders), "sessChg": chg(sessions, ly_sessions),
            "adChg": chg(ad_spend, ly_ad),
        })

    con.close()
    return {"view": view, "periods": results}


@router.get("/api/monthly-yoy")
def monthly_yoy(
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
):
    """Monthly revenue broken down by year for YoY comparison (legacy)."""
    con = get_db()
    hf, hp = _hierarchy_filter(division, customer, platform)
    rows = None

    try:
        analytics_rows = con.execute(f"""
            SELECT EXTRACT(YEAR FROM date) AS yr, EXTRACT(MONTH FROM date) AS mo,
                   COALESCE(SUM(gross_revenue), 0) AS revenue
            FROM analytics_daily
            WHERE date >= '2024-01-01'{hf}
            GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
            ORDER BY yr, mo
        """, hp).fetchall()
        if analytics_rows and len(analytics_rows) > 0 and any(r[2] > 0 for r in analytics_rows):
            rows = analytics_rows
    except Exception:
        pass

    if rows is None:
        try:
            rows = con.execute(f"""
                SELECT EXTRACT(YEAR FROM date) AS yr, EXTRACT(MONTH FROM date) AS mo,
                       COALESCE(SUM(ordered_product_sales), 0) AS revenue
                FROM daily_sales
                WHERE asin = 'ALL' AND date IS NOT NULL AND date >= '2024-01-01'{hf}
                GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
                ORDER BY yr, mo
            """, hp).fetchall()
        except Exception as e:
            con.close()
            logger.error(f"monthly-yoy query error: {e}")
            return {"years": [2024, 2025, 2026], "data": [
                {"month": m, "2024": 0, "2025": 0, "2026": 0}
                for m in ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
            ]}

    con.close()

    months_map = {}
    for r in rows:
        yr, mo, rev = int(r[0]), int(r[1]), round(r[2], 2)
        if mo not in months_map:
            months_map[mo] = {}
        months_map[mo][yr] = rev

    years = [2024, 2025, 2026]
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    data = []
    for mo in range(1, 13):
        entry = {"month": month_names[mo - 1]}
        for yr in years:
            entry[str(yr)] = months_map.get(mo, {}).get(yr, 0)
        data.append(entry)

    return {"years": years, "data": data}


@router.get("/api/product-mix")
def product_mix(
    days: int = Query(365),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
):
    """Top 10 products by revenue for donut chart."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    products = _build_product_list(con, cutoff)
    con.close()

    products.sort(key=lambda p: p["rev"], reverse=True)
    top10 = products[:10]
    other_rev = sum(p["rev"] for p in products[10:])

    result = [{"name": p["name"][:30], "value": p["rev"]} for p in top10]
    if other_rev > 0:
        result.append({"name": "Other", "value": round(other_rev, 2)})

    return {"products": result}


@router.get("/api/color-mix")
def color_mix(
    days: int = Query(365),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
):
    """Sales breakdown by color extracted from product names."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    products = _build_product_list(con, cutoff)
    con.close()

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
