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
from routers._cogs_helper import compute_cogs_for_range, load_unit_costs, load_item_master_names

logger = logging.getLogger("golfgen")
router = APIRouter()

# ── Module-level timezone constant — used everywhere, never re-defined inline ──
CENTRAL = ZoneInfo("America/Chicago")


def _today_central() -> date:
    """Current calendar date in US/Central time.
    Always use this instead of date.today() — Railway container runs UTC."""
    return datetime.now(CENTRAL).date()


def _orders_supplement(con, sd: date, ed: date, hw: str, hp: list, include_pending: bool = True):
    """Pull sales/units from orders table using exact Central-time day boundaries.

    Handles the S&T report lag for recent periods (today, yesterday, last few days).
    When include_pending=True (default): includes both confirmed revenue AND estimated
    revenue for Pending orders (Amazon Seller Central includes Pending in their totals).
    When include_pending=False: returns confirmed/shipped orders only — use this for the
    TY Now figure so it matches Amazon Seller Central's confirmed-order view.
    Returns (sales_float, units_int, order_count_int). Returns (0, 0, 0) on any failure.
    """
    try:
        s_iso = datetime(sd.year, sd.month, sd.day, 0, 0, 0, tzinfo=CENTRAL).isoformat()
        e_iso = datetime(sd.year, sd.month, sd.day, 23, 59, 59, tzinfo=CENTRAL).isoformat()

        # Step 1: Confirmed (Shipped/Unshipped) orders — actual revenue
        r = con.execute(f"""
            SELECT COALESCE(SUM(order_total), 0),
                   COALESCE(SUM(number_of_items), 0),
                   COUNT(DISTINCT order_id)
            FROM orders
            WHERE purchase_date >= ? AND purchase_date <= ?
              AND (order_status IS NULL OR order_status NOT IN ('Cancelled','Pending'))
              {hw}
        """, [s_iso, e_iso] + hp).fetchone()
        shipped_sales = float(r[0] or 0) if r else 0.0
        shipped_units = int(r[1] or 0) if r else 0
        total_cnt     = int(r[2] or 0) if r else 0

        # When confirmed-only mode: return here, skip pending estimation
        if not include_pending:
            logger.debug(f"_orders_supplement {sd}→{ed} (confirmed-only): ${shipped_sales:.2f}, {shipped_units} units")
            return shipped_sales, shipped_units, total_cnt

        # Step 2: Pending orders — SP-API returns OrderTotal={} for these.
        # Estimate revenue using the ASIN's average price from last 14 days.
        try:
            pending_rows = con.execute(f"""
                SELECT asin, COALESCE(number_of_items, 1) AS qty
                FROM orders
                WHERE purchase_date >= ? AND purchase_date <= ?
                  AND order_status = 'Pending'
                  AND asin IS NOT NULL AND asin != ''
                  {hw}
            """, [s_iso, e_iso] + hp).fetchall()

            pending_sales = 0.0
            pending_units = 0
            cutoff_14d = (sd - timedelta(days=14)).isoformat()
            for (asin, qty) in pending_rows:
                total_cnt += 1
                pending_units += qty
                price_row = con.execute("""
                    SELECT AVG(order_total / NULLIF(number_of_items, 0))
                    FROM orders
                    WHERE asin = ? AND order_total > 0
                      AND order_status NOT IN ('Cancelled', 'Pending')
                      AND purchase_date >= ?
                """, [asin, cutoff_14d]).fetchone()
                avg_price = float(price_row[0]) if price_row and price_row[0] else 0.0
                pending_sales += avg_price * qty
        except Exception:
            pending_sales = 0.0
            pending_units = 0

        sales = shipped_sales + pending_sales
        units = shipped_units + pending_units
        logger.debug(f"_orders_supplement {sd}→{ed}: ${sales:.2f} (shipped ${shipped_sales:.2f} + pending_est ${pending_sales:.2f}), {units} units")
        return sales, units, total_cnt
    except Exception as exc:
        logger.warning(f"_orders_supplement({sd}→{ed}) failed: {exc}")
    return 0.0, 0, 0


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
    # Item Master CSV names — user-edited clean descriptions (highest priority)
    im_names = load_item_master_names()
    # Prefer item_master unit_cost over cogs.csv values
    _unit_costs = load_unit_costs()
    inv_names = {}
    try:
        inv_rows = con.execute(
            "SELECT asin, sku, product_name FROM fba_inventory"
        ).fetchall()
        for ir in inv_rows:
            inv_names[ir[0]] = {"sku": ir[1] or "", "product_name": ir[2] or ""}
    except Exception:
        pass

    # Division lookup from item_master (source of truth)
    division_map = {}
    try:
        im_rows = con.execute(
            "SELECT asin, division FROM item_master WHERE asin IS NOT NULL"
        ).fetchall()
        for im in im_rows:
            division_map[im[0]] = im[1] or "unknown"
    except Exception:
        pass

    # Refund data per ASIN from financial_events
    refund_map = {}
    try:
        ref_rows = con.execute("""
            SELECT asin,
                   COUNT(*) AS refund_count,
                   COALESCE(SUM(ABS(product_charges)), 0) AS refund_amt
            FROM financial_events
            WHERE date >= ? AND (event_type ILIKE '%%refund%%' OR event_type ILIKE '%%return%%')
            GROUP BY asin
        """, [cutoff]).fetchall()
        for rr in ref_rows:
            refund_map[rr[0]] = {"count": int(rr[1] or 0), "amount": float(rr[2] or 0)}
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
        asin = r[0]
        # PostgreSQL may return Decimal — coerce to native Python types
        revenue = float(r[1] or 0)
        units = int(r[2] or 0)
        sessions = int(r[3] or 0)
        glance_views = int(r[4] or 0)
        orders = units  # proxy until per-ASIN order count is available
        api_name = inv_names.get(asin, {}).get("product_name", "")
        if units == 0:
            continue
        if not orders:
            orders = units
        aur = round(float(revenue) / units, 2)

        cogs_info = cogs_data.get(asin) or {}
        # Priority: item_master unit_cost > cogs.csv > 35% of AUR estimate
        cogs_per_unit = _unit_costs.get(asin, 0) or cogs_info.get("cogs", 0)
        if cogs_per_unit == 0:
            cogs_per_unit = round(aur * 0.35, 2)

        inv_info = inv_names.get(asin, {})
        im_name = im_names.get(asin, "")
        cogs_name = cogs_info.get("product_name", "")
        if cogs_name and cogs_name.strip().upper() == asin.upper():
            cogs_name = ""
        name = (im_name
                or cogs_name
                or api_name
                or inv_info.get("product_name")
                or asin)
        resolved_sku = (cogs_info.get("sku", "")
                        or inv_info.get("sku", ""))

        fin = fin_by_asin.get(asin, {})
        actual_fba = fin.get("fba_fees", 0)
        est_fba_total = round(float(revenue) * 0.12, 2)
        fba_total = float(actual_fba) if actual_fba > 0 else est_fba_total

        actual_commission = float(fin.get("commission", 0))
        referral_total = round(actual_commission, 2) if actual_commission > 0 else round(float(revenue) * 0.15, 2)

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

        cogs_total = units * float(cogs_per_unit)
        net = round(float(revenue) - cogs_total - float(fba_total) - float(referral_total) - float(ad_spend), 2)
        margin = round(net / float(revenue) * 100) if revenue else 0
        conv_rate = round(float(units) / sessions * 100, 1) if sessions else 0

        # Refund rate for this ASIN
        ref_info = refund_map.get(asin, {})
        refund_rate = round(ref_info.get("count", 0) / units * 100, 1) if units > 0 else 0

        products.append({
            "asin": asin,
            "sku": resolved_sku,
            "name": name,
            "division": division_map.get(asin, "unknown"),
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
            "refundRate": refund_rate,
            "refundCount": ref_info.get("count", 0),
            "refundAmount": round(ref_info.get("amount", 0), 2),
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
            "aur": round(float(w["revenue"]) / units, 2),
            "convRate": round(float(w["orders"]) / sessions * 100, 1) if sessions else 0,
        })
    return result


# ══════════════════════════════════════════════════════════════
# NEW SALES TAB — Period helpers and 11 API endpoints
# ══════════════════════════════════════════════════════════════


def _period_to_dates(period: str, start: str = None, end: str = None):
    """Convert period label to (start_date, end_date) tuple.
    Uses _today_central() — always Central time, never UTC date.today()."""
    today = _today_central()
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


def _hier_where(division=None, customer=None, table_alias='', marketplace=None):
    """Build division/customer/marketplace WHERE clause for new sales endpoints."""
    prefix = table_alias + '.' if table_alias else ''
    clauses, params = [], []
    if division and division != 'all':
        clauses.append(f"{prefix}division = ?")
        params.append(division)
    if customer and customer != 'all' and customer != 'All Channels':
        clauses.append(f"{prefix}customer = ?")
        params.append(customer)
    if marketplace:
        clauses.append(f"{prefix}marketplace = ?")
        params.append(marketplace.upper())
    return ('AND ' + ' AND '.join(clauses) if clauses else ''), params


# ── ENDPOINT 1: Sales summary metrics ──────────────────────
@router.get("/api/sales/summary")
def sales_summary(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """All KPI cards for the selected period, with LY equivalents."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer, marketplace=marketplace)
        # financial_events has NO marketplace column — build separate filter
        fw, fp = _hier_where(division, customer, marketplace=None)

        # LY: shift back 364 days for weekday alignment
        ly_sd = sd - timedelta(days=364)
        ly_ed = ed - timedelta(days=364)

        def _sum_sales(s, e, extra_params, use_individual=False):
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

        # Supplement TY from orders table for recent periods where S&T report lags.
        # Use include_pending=False so Pending orders don't inflate the count —
        # Amazon Seller Central only shows confirmed (Shipped/Unshipped) orders.
        # Only override daily_sales when it shows zero (no S&T data yet for today).
        fell_back = False
        if period in ('today', 'yesterday'):
            o_sales, o_units, o_cnt = _orders_supplement(con, sd, ed, hw, hp, include_pending=False)
            if ty_sales == 0 and (o_sales > 0 or o_cnt > 0):
                ty_sales, ty_units = o_sales, o_units

        ly_sales, ly_units, ly_sessions, ly_gv = _sum_sales(ly_sd, ly_ed, hp)

        # Supplement LY from orders table — only if S&T data is absent.
        if period in ('today', 'yesterday'):
            lo_sales, lo_units, lo_cnt = _orders_supplement(con, ly_sd, ly_ed, hw, hp, include_pending=False)
            if ly_sales == 0 and (lo_sales > 0 or lo_cnt > 0):
                ly_sales, ly_units = lo_sales, lo_units

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
        def _ads_metrics(s, e, extra_params):
            """Returns (clicks, impressions, ctr)."""
            try:
                r = con.execute(f"""
                    SELECT COALESCE(SUM(clicks), 0), COALESCE(SUM(impressions), 0)
                    FROM advertising
                    WHERE date BETWEEN ? AND ? {hw}
                """, [str(s), str(e)] + extra_params).fetchone()
                clicks, imps = int(r[0]), int(r[1])
                ctr = round(clicks / imps, 4) if imps else 0
                return clicks, imps, ctr
            except Exception:
                return 0, 0, 0
        ty_clicks, ty_impressions, ty_ctr = _ads_metrics(sd, ed, hp)
        ly_clicks, ly_impressions, ly_ctr = _ads_metrics(ly_sd, ly_ed, hp)

        # Amazon fees from financial_events — exclude refund events to avoid
        # double-counting fee reversals (refund rows have positive fba_fees/commission
        # which represent fee credits back, ABS() would incorrectly add them as fees).
        # NOTE: Only include actual Amazon fees: fba_fees + commission + other_fees.
        # shipping_charges are CREDITS (money to seller), NOT fees.
        # promotion_amount is a discount that reduces revenue, not an Amazon service fee.
        def _sum_fees(s, e, extra_params):
            try:
                r = con.execute(f"""
                    SELECT COALESCE(SUM(ABS(fba_fees)), 0) + COALESCE(SUM(ABS(commission)), 0),
                           COALESCE(SUM(ABS(other_fees)), 0)
                    FROM financial_events
                    WHERE date >= ? AND date <= ?
                      AND (event_type IS NULL OR event_type NOT ILIKE '%%refund%%') {fw}
                """, [str(s), str(e)] + extra_params).fetchone()
                fba_ref = float(r[0]) if r else 0
                other = float(r[1]) if r else 0
                return fba_ref + other
            except Exception as fee_err:
                logger.error(f"_sum_fees error: {fee_err} | s={s} e={e} fw={fw}")
                return 0

        ty_fees = _sum_fees(sd, ed, fp)
        ly_fees = _sum_fees(ly_sd, ly_ed, fp)

        # financial_events stores PostedDate (settlement, ~14 days after order).
        # Fall back to estimated 27% of sales ONLY when financial_events
        # returned truly zero fees (no data for the period at all).
        if ty_sales > 0 and ty_fees == 0:
            ty_fees = round(ty_sales * 0.27, 2)
        if ly_sales > 0 and ly_fees == 0:
            ly_fees = round(ly_sales * 0.27, 2)

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

        # COGS: computed from item_master unit_cost per ASIN × units sold
        ty_cogs = compute_cogs_for_range(con, sd, ed, hw, hp)
        ly_cogs = compute_cogs_for_range(con, ly_sd, ly_ed, hw, hp)
        ty_gm = round(ty_sales - ty_fees - ty_cogs, 2)
        ly_gm = round(ly_sales - ly_fees - ly_cogs, 2)
        ty_gm_pct = round(ty_gm / ty_sales, 4) if ty_sales else 0
        ly_gm_pct = round(ly_gm / ly_sales, 4) if ly_sales else 0

        # Returns/refunds from financial_events
        # Amazon's PostedDate is the settlement date — typically 1-2 days AFTER
        # the refund is processed. Add a +2 day buffer on end date so returns
        # are not missed just because they settled one day into the next period.
        def _sum_refunds(s, e, extra_params):
            try:
                e_buf = e + timedelta(days=2)
                r = con.execute(f"""
                    SELECT COALESCE(COUNT(*), 0),
                           COALESCE(SUM(ABS(product_charges)), 0)
                    FROM financial_events
                    WHERE date >= ? AND date <= ?
                      AND (event_type ILIKE '%%refund%%' OR event_type ILIKE '%%return%%') {fw}
                """, [str(s), str(e_buf)] + extra_params).fetchone()
                return int(r[0]) if r else 0, round(float(r[1]), 2) if r else 0
            except Exception:
                return 0, 0

        ty_return_units, ty_return_amt = _sum_refunds(sd, ed, fp)
        ly_return_units, ly_return_amt = _sum_refunds(ly_sd, ly_ed, fp)

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
            # Uses ALL-aggregate rows; falls back to per-ASIN sum if none exist
            avg_row = con.execute(f"""
                SELECT COALESCE(AVG(daily_units), 0) FROM (
                    SELECT date, SUM(units_ordered) AS daily_units
                    FROM daily_sales
                    WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
                    GROUP BY date
                ) AS dsub
            """, [str(ed - timedelta(days=30)), str(ed)] + hp).fetchone()
            avg_daily = float(avg_row[0]) if avg_row else 0
            # Fallback: if no ALL-aggregate rows, sum individual ASINs
            if avg_daily == 0:
                avg_row2 = con.execute(f"""
                    SELECT COALESCE(AVG(daily_units), 0) FROM (
                        SELECT date, SUM(units_ordered) AS daily_units
                        FROM daily_sales
                        WHERE asin != 'ALL' AND units_ordered > 0
                          AND date >= ? AND date <= ? {hw}
                        GROUP BY date
                    ) AS dsub2
                """, [str(ed - timedelta(days=30)), str(ed)] + hp).fetchone()
                avg_daily = float(avg_row2[0]) if avg_row2 else 0
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
            "impressions": ty_impressions, "clicks": ty_clicks,
            "ctr": ty_ctr, "conversion": ty_conv,
            "orders": ty_orders, "aov": ty_aov,
            "ad_spend": round(ty_ad_spend, 2), "roas": ty_roas, "tacos": ty_tacos,
            "ly_sales": round(ly_sales, 2), "ly_unit_sales": ly_units, "ly_aur": ly_aur,
            "ly_cogs": ly_cogs, "ly_amazon_fees": round(ly_fees, 2),
            "ly_gross_margin": ly_gm, "ly_gross_margin_pct": ly_gm_pct,
            "ly_sessions": ly_sessions, "ly_glance_views": ly_gv,
            "ly_impressions": ly_impressions, "ly_clicks": ly_clicks,
            "ly_ctr": ly_ctr, "ly_conversion": ly_conv,
            "ly_orders": ly_orders, "ly_aov": ly_aov,
            "ly_ad_spend": round(ly_ad_spend, 2), "ly_roas": ly_roas, "ly_tacos": ly_tacos,
            "dos": dos, "stock_units": stock_units,
            "returns": ty_return_units, "returns_amount": round(ty_return_amt, 2),
            "ly_returns": ly_return_units, "ly_returns_amount": round(ly_return_amt, 2),
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
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Daily rows for TY and LY for line charts."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer, marketplace=marketplace)

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
                "ty_units": ty_u,
                "ly_sales": round(ly.get("sales", 0), 2) if ly else None,
                "ly_units": ly.get("units") if ly else None,
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
    marketplace: str | None = Query(None),
):
    """Return KPI columns for the selected view tab."""
    try:
        con = get_db()
        hw, hp = _hier_where(division, customer, marketplace=marketplace)
        # financial_events has NO marketplace column — build separate filter
        fw, fp = _hier_where(division, customer, marketplace=None)

        view_periods = {
            "exec": {
                "Today": "today", "Yesterday": "yesterday",
                "WTD": "wtd", "MTD": "mtd", "YTD": "ytd",
            },
            "sales summary": {
                "Today": "today", "Yesterday": "yesterday",
                "WTD": "wtd", "MTD": "mtd", "YTD": "ytd",
            },
            "daily": {
                "Today": "today", "Yesterday": "yesterday",
                "2 Days Ago": "2_days_ago", "3 Days Ago": "3_days_ago",
                "4 Days Ago": "4_days_ago", "5 Days Ago": "5_days_ago",
                "6 Days Ago": "6_days_ago",
            },
            "weekly": {
                "WTD": "wtd", "Last Week": "last_week",
                "4 Weeks": "last_4w", "8 Weeks": "last_8w",
                "13 Weeks": "last_13w", "26 Weeks": "last_26w",
            },
            "monthly": {
                "MTD": "mtd", "Last Month": "last_month",
                "2 Months Ago": "2_months_ago", "3 Months Ago": "3_months_ago",
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

        def _daily_sales_sum(s, e, extra_params):
            r = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0),
                       COALESCE(SUM(units_ordered), 0),
                       COALESCE(SUM(sessions), 0),
                       COALESCE(SUM(page_views), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            """, [str(s), str(e)] + extra_params).fetchone()
            return float(r[0]), int(r[1]), int(r[2]), int(r[3])

        today = _today_central()
        for label, period_key in periods_map.items():
            sd, ed = _period_to_dates(period_key)
            sales, units, sessions, glance_views = _daily_sales_sum(sd, ed, hp)

            if period_key == 'today':
                # Only use orders supplement when S&T has no data yet for today.
                # Use include_pending=False to match Amazon Seller Central's view
                # (confirmed orders only — Pending orders inflate the count).
                o_sales, o_units, o_cnt = _orders_supplement(con, sd, ed, hw, hp, include_pending=False)
                if sales == 0 and (o_sales > 0 or o_cnt > 0):
                    sales, units = o_sales, o_units

            elif period_key == 'yesterday':
                # S&T report is usually complete for yesterday — only supplement if empty.
                o_sales, o_units, o_cnt = _orders_supplement(con, sd, ed, hw, hp, include_pending=False)
                if sales == 0 and o_sales > 0:
                    sales, units = o_sales, o_units

            elif ed >= today and sd < today:
                # Multi-day period that includes today (WTD, MTD, YTD).
                # S&T report lags for today — supplement with orders data for today.
                today_sales_row = con.execute(f"""
                    SELECT COALESCE(SUM(ordered_product_sales), 0),
                           COALESCE(SUM(units_ordered), 0)
                    FROM daily_sales
                    WHERE asin = 'ALL' AND date = ? {hw}
                """, [str(today)] + hp).fetchone()
                today_ds_sales = float(today_sales_row[0]) if today_sales_row else 0
                if today_ds_sales == 0:
                    o_sales, o_units, o_cnt = _orders_supplement(con, today, today, hw, hp, include_pending=False)
                    if o_sales > 0 or o_cnt > 0:
                        sales += o_sales
                        units += o_units

            if sales == 0 and units == 0:
                # For any period still at zero: step back up to 3 days if no data
                for step in range(1, 4):
                    sd_fb = sd - timedelta(days=step)
                    ed_fb = ed - timedelta(days=step)
                    s_fb, u_fb, sess_fb, gv_fb = _daily_sales_sum(sd_fb, ed_fb, hp)
                    if s_fb > 0 or u_fb > 0:
                        sd, ed = sd_fb, ed_fb
                        sales, units, sessions, glance_views = s_fb, u_fb, sess_fb, gv_fb
                        break

            ly_sd = sd - timedelta(days=364)
            ly_ed = ed - timedelta(days=364)
            ly_sales, ly_units, ly_sessions, ly_gv = _daily_sales_sum(ly_sd, ly_ed, hp)
            # True order count from orders table — use ISO datetime bounds (same as
            # _orders_supplement) so VARCHAR purchase_date comparison works correctly.
            def _iso_bounds(s, e):
                s_iso = datetime(s.year, s.month, s.day, 0, 0, 0, tzinfo=CENTRAL).isoformat()
                e_iso = datetime(e.year, e.month, e.day, 23, 59, 59, tzinfo=CENTRAL).isoformat()
                return s_iso, e_iso
            try:
                s_iso, e_iso = _iso_bounds(sd, ed)
                o_row = con.execute(f"""
                    SELECT COUNT(DISTINCT order_id)
                    FROM orders
                    WHERE purchase_date >= ? AND purchase_date <= ?
                      AND (order_status IS NULL
                           OR order_status NOT IN ('Cancelled','Canceled')) {hw}
                """, [s_iso, e_iso] + hp).fetchone()
                orders = int(o_row[0]) if o_row and o_row[0] else (units if units else 0)
                # Orders can never exceed units — cap it
                if units > 0 and orders > units:
                    orders = units
            except Exception:
                orders = units
            try:
                ly_s_iso, ly_e_iso = _iso_bounds(ly_sd, ly_ed)
                ly_o_row = con.execute(f"""
                    SELECT COUNT(DISTINCT order_id)
                    FROM orders
                    WHERE purchase_date >= ? AND purchase_date <= ?
                      AND (order_status IS NULL
                           OR order_status NOT IN ('Cancelled','Canceled')) {hw}
                """, [ly_s_iso, ly_e_iso] + hp).fetchone()
                ly_orders = int(ly_o_row[0]) if ly_o_row and ly_o_row[0] else ly_units
                if ly_units > 0 and ly_orders > ly_units:
                    ly_orders = ly_units
            except Exception:
                ly_orders = ly_units
            aur = round(float(sales) / units, 2) if units else 0
            aov = round(float(sales) / orders, 2) if orders else 0
            conv = round(float(units) / sessions, 4) if sessions else 0
            ctr = 0
            ly_aur = round(float(ly_sales) / ly_units, 2) if ly_units else 0
            ly_aov = round(float(ly_sales) / ly_orders, 2) if ly_orders else 0
            ly_conv = round(float(ly_units) / ly_sessions, 4) if ly_sessions else 0

            # Amazon fees — exclude refund events to avoid double-counting fee reversals
            # Only fba_fees + commission + other_fees. NOT shipping_charges (credits) or
            # promotion_amount (discounts already reflected in lower revenue).
            def _pc_fees(s, e, extra_params):
                try:
                    r = con.execute(f"""
                        SELECT COALESCE(SUM(ABS(fba_fees)), 0) + COALESCE(SUM(ABS(commission)), 0),
                               COALESCE(SUM(ABS(other_fees)), 0)
                        FROM financial_events
                        WHERE date >= ? AND date <= ?
                          AND (event_type IS NULL OR event_type NOT ILIKE '%%refund%%') {fw}
                    """, [str(s), str(e)] + extra_params).fetchone()
                    return round(float(r[0]) + float(r[1]), 2)
                except Exception:
                    return 0

            ty_fees = _pc_fees(sd, ed, fp)
            ly_fees_val = _pc_fees(ly_sd, ly_ed, fp)
            # Fallback: estimate fees at 27% when no financial_events data
            if sales > 0 and ty_fees == 0:
                ty_fees = round(sales * 0.27, 2)
            if ly_sales > 0 and ly_fees_val == 0:
                ly_fees_val = round(ly_sales * 0.27, 2)

            # Returns / refunds
            # Amazon's PostedDate is the settlement date — typically 1-2 days AFTER
            # the refund is processed. Add a +2 day buffer on end date so TY returns
            # are not missed just because they settled one day into the next period.
            def _pc_refunds(s, e, extra_params):
                try:
                    e_buf = e + timedelta(days=2)
                    r = con.execute(f"""
                        SELECT COALESCE(COUNT(*), 0),
                               COALESCE(SUM(ABS(COALESCE(product_charges, 0))), 0)
                        FROM financial_events
                        WHERE date >= ? AND date <= ?
                          AND (event_type ILIKE '%%refund%%' OR event_type ILIKE '%%return%%') {fw}
                    """, [str(s), str(e_buf)] + extra_params).fetchone()
                    return int(r[0]) if r else 0, round(float(r[1]), 2) if r else 0
                except Exception as _ref_ex:
                    logger.warning(f"_pc_refunds({s}→{e}) error: {_ref_ex}")
                    return 0, 0

            ty_ret_units, ty_ret_amt = _pc_refunds(sd, ed, fp)
            ly_ret_units, ly_ret_amt = _pc_refunds(ly_sd, ly_ed, fp)

            # ── TODAY-ONLY: LY same-time-of-day snapshot + TY full-day forecast ──
            # Uses orders table (timestamped) to slice LY at the same hour as now.
            # ly_eod_* = existing ly_sales/units (S&T full-day actuals from daily_sales).
            # ty_forecast = TY_so_far × (LY_full_day / LY_same_time) — pacing ratio.
            ly_same_time_sales   = 0.0
            ly_same_time_units   = 0
            ly_same_time_orders  = 0
            ty_forecast          = None
            ty_units_forecast    = None
            snapshot_time        = None
            if period_key == 'today':
                now_ct = datetime.now(CENTRAL)
                snapshot_time = now_ct.strftime("%-I:%M %p CT")

                # ── TY NOW: SP-API getOrderMetrics for exact match with Amazon SC ──────
                # getOrderMetrics returns the canonical "Today's Sales" figure —
                # same source Amazon Seller Central uses. Preferred over the orders table
                # which has a 50-order sync cap and estimates Pending order prices.
                # Falls back to the orders table value (already set above) on any error.
                try:
                    from services.sp_api import get_ty_same_time_sales
                    _ty_st = get_ty_same_time_sales(sd, now_ct)
                    if _ty_st[0] > 0:
                        sales  = _ty_st[0]
                        units  = max(_ty_st[1], units)   # take higher (API vs orders table)
                        orders = max(_ty_st[2], orders) if _ty_st[2] > 0 else orders
                        aur    = round(float(sales) / units, 2) if units else 0
                        aov    = round(float(sales) / orders, 2) if orders else 0
                        logger.info(f"today TY NOW: SP-API → ${sales:,.2f} / {units} units / {orders} orders")
                except Exception as _ex:
                    logger.warning(f"get_ty_same_time_sales failed, using orders table: {_ex}")

                # ── LY full-day SP-API fallback ──────────────────────────────────────
                # daily_sales often has no row for the LY date (364 days ago), leaving
                # ly_sales=0 which makes ly_fees/ly_orders/ly_aur all zero.
                # If daily_sales has no LY data, fetch the full LY day from SP-API so
                # LY EOD Sales, Fees, Units, Orders, and AUR all populate correctly.
                if ly_sales == 0:
                    try:
                        from services.sp_api import get_ly_full_day_sales
                        _ly_fd = get_ly_full_day_sales(ly_sd)
                        if _ly_fd[0] > 0:
                            ly_sales  = _ly_fd[0]
                            ly_units  = max(_ly_fd[1], ly_units)
                            ly_orders = max(_ly_fd[2], ly_orders) if _ly_fd[2] > 0 else (ly_units if ly_units else 0)
                            ly_aur    = round(float(ly_sales) / ly_units, 2) if ly_units else 0
                            ly_aov    = round(float(ly_sales) / ly_orders, 2) if ly_orders else 0
                            # Recompute fees now that ly_sales is populated
                            ly_fees_val = _pc_fees(ly_sd, ly_ed, fp)
                            if ly_fees_val == 0:
                                ly_fees_val = round(ly_sales * 0.27, 2)
                            logger.info(f"today LY EOD: SP-API fallback → ${ly_sales:,.2f} / {ly_units} units / {ly_orders} orders")
                    except Exception as _ex:
                        logger.warning(f"get_ly_full_day_sales fallback failed: {_ex}")

                # Use SP-API Sales.getOrderMetrics for LY same-time data.
                # The orders table only has recent history and won't have LY rows.
                try:
                    from services.sp_api import get_ly_same_time_sales
                    _ly_st = get_ly_same_time_sales(ly_sd, now_ct)
                    ly_same_time_sales  = _ly_st[0]
                    ly_same_time_units  = _ly_st[1]
                    ly_same_time_orders = _ly_st[2] if len(_ly_st) > 2 else 0
                except Exception as _ex:
                    logger.warning(f"get_ly_same_time_sales failed: {_ex}")
                    ly_same_time_sales, ly_same_time_units, ly_same_time_orders = 0.0, 0, 0

                # ── Time-fraction fallback ────────────────────────────────────────
                # If SP-API returned 0 (throttle / no data) but we have LY full-day
                # data, estimate LY same-time using a linear time fraction.
                # e.g. at 9 AM (37.5% through the day) → ly_same_time ≈ ly_eod × 0.375
                if ly_same_time_sales == 0 and ly_sales > 0:
                    day_frac = (now_ct.hour * 60 + now_ct.minute) / (24 * 60)
                    ly_same_time_sales  = round(float(ly_sales) * day_frac, 2)
                    ly_same_time_units  = round(float(ly_units) * day_frac) if ly_units else 0
                    ly_same_time_orders = round(float(ly_orders) * day_frac) if ly_orders else 0
                    logger.info(f"LY NOW time-fraction fallback: {day_frac:.1%} of day → ${ly_same_time_sales:,.2f} / {ly_same_time_units} units")
                # ── Sales forecast: LY pacing ratio ──────────────────────────────
                # TY_so_far × (LY_full_day / LY_same_time) captures intraday patterns
                # (peak hours, promotions). S&T dollar data is reliable for this.
                if ly_same_time_sales > 0 and ly_sales > 0:
                    ty_forecast = round(sales * (ly_sales / ly_same_time_sales), 2)

                # ── Unit forecast: LY pacing ratio ───────────────────────────────
                # Formula: TY_EOD = TY_NOW / (LY_NOW / LY_EOD)
                # i.e. if LY had sold 90% of its units by this same time of day,
                # and TY has 26 units so far, forecast = 26 / 0.90 ≈ 29 units.
                # Guard: result is always >= max(0, units) so it never goes below
                # what's already sold, and never negative.
                _safe_units = max(0, units)
                if ly_same_time_units > 0 and ly_units > 0:
                    _ly_ratio = float(ly_same_time_units) / float(ly_units)
                    _raw = round(_safe_units / _ly_ratio)
                    ty_units_forecast = max(_safe_units, _raw)
                elif _safe_units > 0:
                    # Fallback: time-fraction if no LY unit data
                    _day_frac_u = (now_ct.hour * 60 + now_ct.minute) / (24 * 60)
                    if _day_frac_u >= 0.04:
                        ty_units_forecast = max(_safe_units, round(_safe_units / _day_frac_u))

            result[label] = {
                "sales": round(sales, 2), "units": units, "aur": aur,
                "orders": orders, "aov": aov,
                "sessions": sessions, "glance_views": glance_views,
                "ctr": ctr, "conversion": conv,
                "amazon_fees": ty_fees, "returns": ty_ret_units,
                "returns_amount": ty_ret_amt,
                "ly_sales": round(ly_sales, 2), "ly_units": ly_units,
                "ly_aur": ly_aur, "ly_orders": ly_orders, "ly_aov": ly_aov,
                "ly_sessions": ly_sessions, "ly_glance_views": ly_gv,
                "ly_conversion": ly_conv,
                "ly_amazon_fees": ly_fees_val, "ly_returns": ly_ret_units,
                "ly_returns_amount": ly_ret_amt,
                # Today-only fields (0 / None for all other periods)
                "ly_same_time_sales":   ly_same_time_sales,
                "ly_same_time_units":   ly_same_time_units,
                "ly_same_time_orders":  ly_same_time_orders,
                "ly_eod_sales":        round(ly_sales, 2),
                "ly_eod_units":        ly_units,
                "ty_forecast":         ty_forecast,
                "ty_units_forecast":   ty_units_forecast,
                "snapshot_time":       snapshot_time,
            }

        con.close()
        return result
    except Exception as e:
        logger.error(f"sales/period-comparison error: {e}")
        return {"error": str(e)}


# Manually-provided 2024 monthly revenue data (user-supplied actuals)
_Y2024_FALLBACK = {1: 29790, 2: 46186, 3: 56133}


# ── ENDPOINT: Hourly sales breakdown ───────────────────────
@router.get("/api/sales/hourly")
def sales_hourly(
    date: str | None = Query(None),          # YYYY-MM-DD target day (default: today)
    compare_date: str | None = Query(None),   # YYYY-MM-DD comparison day (default: LY -364d)
    division: str | None = Query(None),
    customer: str | None = Query(None),
    marketplace: str | None = Query(None),
):
    """Hourly sales breakdown for a day vs. a comparison day.

    TY data: pulled from orders table when available (recent, Central-time-accurate),
             then supplemented/replaced by SP-API getOrderMetrics if needed.
    LY data: SP-API getOrderMetrics only (orders table doesn't retain LY history).

    Returns {date, compare_date, ty:[{hour,sales,units,orders}×24],
             ly:[{hour,sales,units,orders}×24], ty_total, ly_total}
    """
    try:
        from services.sp_api import get_hourly_sales
        con = get_db()
        hw, hp = _hier_where(division, customer, marketplace=marketplace)
        today_ct = _today_central()

        # Parse dates
        try:
            target_date = datetime.strptime(date, '%Y-%m-%d').date() if date else today_ct
        except Exception:
            target_date = today_ct
        try:
            comp_date = datetime.strptime(compare_date, '%Y-%m-%d').date() \
                if compare_date else (target_date - timedelta(days=364))
        except Exception:
            comp_date = target_date - timedelta(days=364)

        def _hourly_from_orders(d) -> list:
            """Pull hourly sales from orders table — Central-time hours, 24 slots."""
            try:
                s_iso = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=CENTRAL).isoformat()
                e_iso = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=CENTRAL).isoformat()
                # PostgreSQL: cast VARCHAR purchase_date to timestamptz then shift to Central
                rows = con.execute(f"""
                    SELECT
                        EXTRACT(HOUR FROM (
                            CAST(purchase_date AS TIMESTAMPTZ) AT TIME ZONE 'America/Chicago'
                        ))::int AS hr,
                        COALESCE(SUM(order_total), 0),
                        COALESCE(SUM(number_of_items), 0),
                        COUNT(DISTINCT order_id)
                    FROM orders
                    WHERE purchase_date >= ? AND purchase_date <= ?
                      AND (order_status IS NULL
                           OR order_status NOT IN ('Cancelled', 'Pending'))
                      {hw}
                    GROUP BY 1
                    ORDER BY 1
                """, [s_iso, e_iso] + hp).fetchall()
                if not rows:
                    return []
                hour_map = {h: {'hour': h, 'sales': 0.0, 'units': 0, 'orders': 0}
                            for h in range(24)}
                for (hr, sales, units, ords) in rows:
                    h = int(hr or 0)
                    if 0 <= h <= 23:
                        hour_map[h] = {'hour': h, 'sales': round(float(sales or 0), 2),
                                       'units': int(units or 0), 'orders': int(ords or 0)}
                return [hour_map[h] for h in range(24)]
            except Exception as ex:
                logger.warning(f"_hourly_from_orders({d}) failed: {ex}")
                return []

        # TY: try orders table first (richer data for recent days)
        ty_hours = _hourly_from_orders(target_date)
        ty_from_db = bool(ty_hours and sum(h['sales'] for h in ty_hours) > 0)
        if not ty_from_db:
            ty_hours = get_hourly_sales(target_date)

        # LY: orders table rarely has data 364 days ago → go straight to SP-API
        ly_hours = _hourly_from_orders(comp_date)
        if not (ly_hours and sum(h['sales'] for h in ly_hours) > 0):
            ly_hours = get_hourly_sales(comp_date)

        # Ensure both lists are exactly 24 elements
        if not ty_hours: ty_hours = [{'hour': h, 'sales': 0.0, 'units': 0, 'orders': 0} for h in range(24)]
        if not ly_hours: ly_hours = [{'hour': h, 'sales': 0.0, 'units': 0, 'orders': 0} for h in range(24)]

        ty_total = round(sum(h['sales'] for h in ty_hours), 2)
        ly_total = round(sum(h['sales'] for h in ly_hours), 2)

        con.close()
        return {
            'date': str(target_date),
            'compare_date': str(comp_date),
            'ty': ty_hours,
            'ly': ly_hours,
            'ty_total': ty_total,
            'ly_total': ly_total,
            'ty_source': 'orders_db' if ty_from_db else 'sp_api',
        }
    except Exception as e:
        logger.error(f"sales/hourly error: {e}")
        return {'error': str(e), 'ty': [], 'ly': [], 'ty_total': 0, 'ly_total': 0}


# ── ENDPOINT 4: Monthly YOY bar chart data ─────────────────
@router.get("/api/sales/monthly-yoy")
def sales_monthly_yoy(
    division: str | None = Query(None),
    customer: str | None = Query(None),
    marketplace: str | None = Query(None),
):
    """12 months x 3 years for YOY grouped bar chart."""
    try:
        con = get_db()
        hw, hp = _hier_where(division, customer, marketplace=marketplace)
        month_names = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

        rows = con.execute(f"""
            SELECT EXTRACT(YEAR FROM CAST(date AS DATE)) AS yr,
                   EXTRACT(MONTH FROM CAST(date AS DATE)) AS mo,
                   COALESCE(SUM(ordered_product_sales), 0) AS revenue
            FROM daily_sales
            WHERE asin = 'ALL' AND date IS NOT NULL AND date >= '2024-01-01' {hw}
            GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE)), EXTRACT(MONTH FROM CAST(date AS DATE))
            ORDER BY yr, mo
        """, hp).fetchall()

        months_map = {}
        for r in rows:
            yr, mo, rev = int(r[0]), int(r[1]), round(float(r[2]), 2)
            if mo not in months_map:
                months_map[mo] = {}
            months_map[mo][yr] = rev

        _today = _today_central()
        current_month = _today.month
        current_year  = _today.year
        today_day     = _today.day
        ly_year       = current_year - 1
        compare_day   = max(1, today_day - 1)  # S&T has ~1d lag

        # LY MTD: LY sales through same day-of-month as today
        ly_mtd_row = con.execute(f"""
            SELECT COALESCE(SUM(ordered_product_sales), 0)
            FROM daily_sales
            WHERE asin = 'ALL'
              AND EXTRACT(YEAR  FROM CAST(date AS DATE)) = {ly_year}
              AND EXTRACT(MONTH FROM CAST(date AS DATE)) = {current_month}
              AND EXTRACT(DAY   FROM CAST(date AS DATE)) <= {compare_day}
              {hw}
        """, hp).fetchone()
        ly_mtd = round(float(ly_mtd_row[0] or 0), 2)

        con.close()

        result = []
        for mo in range(1, 13):
            entry = {
                "month": month_names[mo - 1], "month_num": mo,
                "y2024": months_map.get(mo, {}).get(2024, 0) or _Y2024_FALLBACK.get(mo, 0),
                "y2025": months_map.get(mo, {}).get(2025, 0),
                "y2026": months_map.get(mo, {}).get(2026, None) if (current_year > 2026 or (current_year == 2026 and mo <= current_month)) else None,
            }
            result.append(entry)

        return {
            "months": result,
            "meta": {
                "current_month": current_month,
                "current_year":  current_year,
                "today_day":     today_day,
                "ly_mtd":        ly_mtd,
            }
        }
    except Exception as e:
        logger.error(f"sales/monthly-yoy error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 5: Revenue by channel (stacked bar) ──────────
@router.get("/api/sales/by-channel")
def sales_by_channel(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Weekly revenue split by customer/channel."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, None, marketplace=marketplace)

        # Get weekly revenue by customer
        rows = con.execute(f"""
            SELECT customer,
                   date,
                   COALESCE(SUM(ordered_product_sales), 0) AS revenue
            FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ?
              AND date IS NOT NULL {hw}
            GROUP BY customer, date
            ORDER BY date
        """, [str(sd), str(ed)] + hp).fetchall()
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
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Weekly rolling 4-week averages for TY and LY."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer, marketplace=marketplace)
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


# ── ENDPOINT 7: Units/Sales/Returns heatmap data ──────────
@router.get("/api/sales/heatmap")
def sales_heatmap(
    division: str | None = Query(None),
    customer: str | None = Query(None),
    marketplace: str | None = Query(None),
    weeks: int = Query(26, ge=13, le=52),
):
    """N weeks x 7 days heatmap of units sold, sales $, and return units.
    weeks param controls history depth (13, 26, or 52).
    Returns rows with: week, day, day_name, units, sales, returns
    """
    try:
        con = get_db()
        hw, hp = _hier_where(division, customer, marketplace=marketplace)
        today = _today_central()
        start_date = today - timedelta(weeks=weeks)
        expected_days = (today - start_date).days + 1

        # Try asin='ALL' first (has full historical coverage from S&T report backfill)
        rows = con.execute(f"""
            SELECT date, COALESCE(units_ordered, 0), COALESCE(ordered_product_sales, 0)
            FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            ORDER BY date
        """, [str(start_date), str(today)] + hp).fetchall()

        # Fallback: if ALL rows are sparse, aggregate from per-ASIN rows
        if len(rows) < (expected_days * 0.3):
            agg_rows = con.execute(f"""
                SELECT date, COALESCE(SUM(units_ordered), 0), COALESCE(SUM(ordered_product_sales), 0)
                FROM daily_sales
                WHERE asin != 'ALL' AND date >= ? AND date <= ? {hw}
                GROUP BY date ORDER BY date
            """, [str(start_date), str(today)] + hp).fetchall()
            if len(agg_rows) > len(rows):
                # Merge: prefer 'ALL' rows, fill gaps with per-ASIN aggregates
                all_map = {fmt_date(r[0]): r for r in rows}
                merged = []
                for r in agg_rows:
                    ds = fmt_date(r[0])
                    merged.append(all_map.get(ds, r))
                # Also add any ALL rows not in agg_rows
                agg_dates = {fmt_date(r[0]) for r in agg_rows}
                for r in rows:
                    if fmt_date(r[0]) not in agg_dates:
                        merged.append(r)
                rows = sorted(merged, key=lambda r: fmt_date(r[0]))

        # Supplement today's data from orders table if daily_sales hasn't synced yet
        # (S&T report lags behind — the orders sync is more current for today)
        today_str = str(today)
        today_in_data = any(fmt_date(r[0]) == today_str for r in rows)
        if not today_in_data:
            try:
                s_iso = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=CENTRAL).isoformat()
                e_iso = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=CENTRAL).isoformat()
                o_row = con.execute(f"""
                    SELECT COALESCE(SUM(number_of_items), 0), COALESCE(SUM(order_total), 0)
                    FROM orders
                    WHERE purchase_date >= ? AND purchase_date <= ?
                      AND (order_status IS NULL OR order_status NOT IN ('Cancelled')) {hw}
                """, [s_iso, e_iso] + hp).fetchone()
                today_units = int(o_row[0]) if o_row and o_row[0] else 0
                today_sales = float(o_row[1]) if o_row and o_row[1] else 0.0
                if today_units > 0:
                    rows = list(rows) + [(today_str, today_units, today_sales)]
            except Exception:
                pass

        # Build returns map: date → return_units from financial_events
        returns_map: dict = {}
        try:
            ret_rows = con.execute(f"""
                SELECT date, COUNT(*)
                FROM financial_events
                WHERE date >= ? AND date <= ?
                  AND (event_type ILIKE '%refund%' OR event_type ILIKE '%return%') {hw}
                GROUP BY date
            """, [str(start_date), str(today)] + hp).fetchall()
            for rr in ret_rows:
                returns_map[fmt_date(rr[0])] = int(rr[1])
        except Exception:
            pass

        con.close()

        result = []
        day_names = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
        for r in rows:
            d_str = fmt_date(r[0])
            units = int(r[1])
            sales = float(r[2]) if len(r) > 2 and r[2] else 0.0
            returns_ct = returns_map.get(d_str, 0)
            try:
                dt = datetime.strptime(d_str, "%Y-%m-%d").date()
            except Exception:
                continue
            days_ago = (today - dt).days
            week = days_ago // 7
            day = dt.weekday()  # 0=Mon, 6=Sun
            if week < weeks:
                result.append({
                    "week": week, "day": day,
                    "day_name": day_names[day],
                    "units": units,
                    "sales": round(sales, 2),
                    "returns": returns_ct,
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
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Conversion funnel: GV -> Sessions -> Add to Cart -> Orders."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer, marketplace=marketplace)
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
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """ACOS/ROAS quadrant data for TY vs LY."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        ly_sd = sd - timedelta(days=364)
        ly_ed = ed - timedelta(days=364)
        hw, hp = _hier_where(division, customer, marketplace=marketplace)

        def _ad_metrics(s, e):
            try:
                row = con.execute(f"""
                    SELECT COALESCE(SUM(spend), 0),
                           COALESCE(SUM(sales), 0)
                    FROM advertising
                    WHERE date >= ? AND date <= ? {hw}
                """, [str(s), str(e)] + hp).fetchone()
                spend = float(row[0])
                sales = float(row[1])
            except Exception:
                spend, sales = 0, 0
            # Get total revenue for TACOS calculation
            try:
                rev_row = con.execute(f"""
                    SELECT COALESCE(SUM(ordered_product_sales), 0)
                    FROM daily_sales
                    WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
                """, [str(s), str(e)] + hp).fetchone()
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
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Break down Amazon fees by type — all fee columns from financial_events."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer, marketplace=marketplace)

        # Sum every fee column separately for granular breakdown
        # Exclude refund events — fee reversals on refunds would inflate totals
        row = con.execute(f"""
            SELECT COALESCE(SUM(ABS(fba_fees)), 0),
                   COALESCE(SUM(ABS(commission)), 0),
                   COALESCE(SUM(ABS(other_fees)), 0),
                   COALESCE(SUM(ABS(promotion_amount)), 0),
                   COALESCE(SUM(ABS(shipping_charges)), 0)
            FROM financial_events
            WHERE date >= ? AND date <= ?
              AND (event_type IS NULL OR event_type NOT ILIKE '%%refund%%') {hw}
        """, [str(sd), str(ed)] + hp).fetchone()

        fba = round(float(row[0]), 2)
        referral = round(float(row[1]), 2)
        other = round(float(row[2]), 2)
        promo = round(float(row[3]), 2)
        shipping = round(float(row[4]), 2)

        actual_total = fba + referral + other + promo + shipping
        estimated = False

        # financial_events PostedDate lag — recent periods may return 0.
        # Estimate from daily_sales revenue when actuals are suspiciously low.
        if actual_total == 0:
            sales_row = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            """, [str(sd), str(ed)] + hp).fetchone()
            est_sales = float(sales_row[0]) if sales_row else 0
            if est_sales > 0:
                fba = round(est_sales * 0.12, 2)
                referral = round(est_sales * 0.15, 2)
                estimated = True

        con.close()

        result = [
            {"type": "FBA Fulfillment", "amount": fba},
            {"type": "Referral Fee", "amount": referral},
            {"type": "Promotions", "amount": promo},
            {"type": "Shipping Credits", "amount": shipping},
            {"type": "Other Fees", "amount": other},
        ]
        # Filter out zero entries
        items = [r for r in result if r["amount"] > 0] or result
        return {"items": items, "total": round(sum(i["amount"] for i in items), 2), "estimated": estimated}
    except Exception as e:
        logger.error(f"sales/fee-breakdown error: {e}")
        return {"error": str(e)}


# ── ENDPOINT 11: Ad spend breakdown ────────────────────────
@router.get("/api/sales/ad-breakdown")
def sales_ad_breakdown(
    period: str = Query("last_30d"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    marketplace: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
):
    """Break down ad spend by campaign type."""
    try:
        con = get_db()
        sd, ed = _period_to_dates(period, start, end)
        hw, hp = _hier_where(division, customer, marketplace=marketplace)

        # Try ads_campaigns which has campaign_type
        try:
            rows = con.execute(f"""
                SELECT COALESCE(campaign_type, 'SP') AS ctype,
                       COALESCE(SUM(spend), 0),
                       COALESCE(SUM(sales), 0)
                FROM ads_campaigns
                WHERE date >= ? AND date <= ? {hw}
                GROUP BY COALESCE(campaign_type, 'SP')
            """, [str(sd), str(ed)] + hp).fetchall()
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
                row = con2.execute(f"""
                    SELECT COALESCE(SUM(spend), 0), COALESCE(SUM(sales), 0)
                    FROM advertising
                    WHERE date >= ? AND date <= ? {hw}
                """, [str(sd), str(ed)] + hp).fetchone()
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


# ── DEBUG: Today sales diagnostic (no auth) ────────────────
@router.get("/api/sales/debug-today")
def sales_debug_today():
    """Show exactly what the Today query returns and why. No auth required."""
    try:
        con = get_db()
        today_c = _today_central()
        s_iso = datetime(today_c.year, today_c.month, today_c.day, 0, 0, 0, tzinfo=CENTRAL).isoformat()
        e_iso = datetime(today_c.year, today_c.month, today_c.day, 23, 59, 59, tzinfo=CENTRAL).isoformat()

        # What does the orders table return for today?
        r = con.execute("""
            SELECT COUNT(DISTINCT order_id) AS cnt,
                   COALESCE(SUM(order_total), 0) AS total,
                   COALESCE(SUM(number_of_items), 0) AS units,
                   MIN(purchase_date) AS earliest,
                   MAX(purchase_date) AS latest
            FROM orders
            WHERE purchase_date >= ? AND purchase_date <= ?
              AND (order_status IS NULL OR order_status NOT IN ('Cancelled','Pending'))
        """, [s_iso, e_iso]).fetchone()

        # What are the most recent 5 orders in the table at all?
        recent = con.execute("""
            SELECT order_id, purchase_date, order_total, order_status
            FROM orders
            ORDER BY purchase_date DESC
            LIMIT 5
        """).fetchall()
        con.close()

        return {
            "central_today": str(today_c),
            "query_start_iso": s_iso,
            "query_end_iso": e_iso,
            "today_orders_count": int(r[0]) if r else 0,
            "today_sales_total": float(r[1]) if r else 0,
            "today_units": int(r[2]) if r else 0,
            "today_earliest_order": str(r[3]) if r and r[3] else None,
            "today_latest_order": str(r[4]) if r and r[4] else None,
            "most_recent_5_orders": [
                {"order_id": row[0], "purchase_date": str(row[1]),
                 "order_total": row[2], "status": row[3]}
                for row in recent
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/debug/today-full")
def sales_debug_today_full():
    """Full Today pipeline trace — shows daily_sales AND orders results, no auth."""
    from core.config import DATABASE_URL, USE_POSTGRES
    try:
        con = get_db()
        today_c = _today_central()
        sd = today_c
        s_iso = datetime(sd.year, sd.month, sd.day, 0, 0, 0, tzinfo=CENTRAL).isoformat()
        e_iso = datetime(sd.year, sd.month, sd.day, 23, 59, 59, tzinfo=CENTRAL).isoformat()

        # Step 1: daily_sales asin='ALL' for today
        ds_row = con.execute("""
            SELECT COALESCE(SUM(ordered_product_sales),0),
                   COALESCE(SUM(units_ordered),0),
                   COUNT(*)
            FROM daily_sales WHERE asin='ALL' AND date=?
        """, [str(today_c)]).fetchone()

        # Step 2: orders supplement
        o_row = con.execute("""
            SELECT COALESCE(SUM(order_total),0),
                   COALESCE(SUM(number_of_items),0),
                   COUNT(DISTINCT order_id)
            FROM orders
            WHERE purchase_date >= ? AND purchase_date <= ?
              AND (order_status IS NULL OR order_status NOT IN ('Cancelled','Pending'))
        """, [s_iso, e_iso]).fetchone()

        # Step 3: all orders today (including pending)
        all_today = con.execute("""
            SELECT order_status, COUNT(*) AS cnt, COALESCE(SUM(order_total),0) AS rev
            FROM orders
            WHERE purchase_date >= ? AND purchase_date <= ?
            GROUP BY order_status
        """, [s_iso, e_iso]).fetchall()

        con.close()
        return {
            "db_mode": "postgres" if USE_POSTGRES else "duckdb",
            "db_url_set": bool(DATABASE_URL),
            "central_today": str(today_c),
            "query_start": s_iso, "query_end": e_iso,
            "daily_sales_ALL": {
                "sales": float(ds_row[0]), "units": int(ds_row[1]), "rows": int(ds_row[2])
            },
            "orders_supplement": {
                "sales": float(o_row[0]), "units": int(o_row[1]), "count": int(o_row[2])
            },
            "what_dashboard_shows": {
                "sales": float(o_row[0]) if o_row[0] > 0 or o_row[2] > 0 else float(ds_row[0]),
                "units": int(o_row[1]) if o_row[0] > 0 or o_row[2] > 0 else int(ds_row[1]),
            },
            "all_today_orders_by_status": [
                {"status": r[0], "count": int(r[1]), "revenue": float(r[2])}
                for r in all_today
            ],
        }
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/debug/summary-today")
def debug_summary_today():
    """Runs exact same logic as /api/sales/summary?period=today — no auth required.
    Calls the real _orders_supplement (includes Pending estimation) so you can see
    shipped vs pending breakdown and verify the number matches Amazon Seller Central."""
    try:
        con = get_db()
        sd = _today_central()
        ed = sd
        hw, hp = "", []

        # Step 1: daily_sales asin='ALL' row (populated by Today Orders sync + S&T report)
        ds_row = con.execute("""
            SELECT COALESCE(SUM(ordered_product_sales), 0),
                   COALESCE(SUM(units_ordered), 0)
            FROM daily_sales
            WHERE asin = 'ALL' AND date = ?
        """, [str(sd)]).fetchone()
        ty_sales = float(ds_row[0])
        ty_units = int(ds_row[1])

        # Step 2: orders supplement — calls the REAL helper (includes Pending estimation)
        o_sales, o_units, o_cnt = _orders_supplement(con, sd, ed, hw, hp)

        # Shipped-only breakdown for transparency
        s_iso = datetime(sd.year, sd.month, sd.day, 0, 0, 0, tzinfo=CENTRAL).isoformat()
        e_iso = datetime(sd.year, sd.month, sd.day, 23, 59, 59, tzinfo=CENTRAL).isoformat()
        shipped_row = con.execute("""
            SELECT COALESCE(SUM(order_total), 0), COUNT(DISTINCT order_id)
            FROM orders
            WHERE purchase_date >= ? AND purchase_date <= ?
              AND (order_status IS NULL OR order_status NOT IN ('Cancelled','Pending'))
        """, [s_iso, e_iso]).fetchone()
        shipped_sales = float(shipped_row[0]) if shipped_row else 0.0
        shipped_cnt   = int(shipped_row[1]) if shipped_row else 0

        pending_rows = con.execute("""
            SELECT order_id, asin, order_status, order_total, number_of_items, purchase_date
            FROM orders
            WHERE purchase_date >= ? AND purchase_date <= ?
              AND order_status = 'Pending'
        """, [s_iso, e_iso]).fetchall()
        pending_detail = [
            {"order_id": r[0], "asin": r[1], "status": r[2],
             "order_total": float(r[3] or 0), "qty": r[4], "purchase_date": str(r[5])}
            for r in pending_rows
        ]

        # asin_prices availability check (same dict the sync uses)
        try:
            asin_prices_row = con.execute("""
                SELECT COUNT(*), COUNT(DISTINCT asin)
                FROM daily_sales
                WHERE asin != 'ALL' AND units_ordered > 0
                  AND date >= CURRENT_DATE - INTERVAL '90 days'
            """).fetchone()
            asin_prices_count = int(asin_prices_row[1]) if asin_prices_row else 0
        except Exception:
            asin_prices_count = -1

        # Supplement: use orders table if it has MORE revenue than daily_sales
        supplement_triggered = False
        if o_sales > ty_sales or (ty_sales == 0 and (o_sales > 0 or o_cnt > 0)):
            ty_sales = o_sales
            ty_units = o_units
            supplement_triggered = True

        # Step 3: fees estimate
        ty_fees_raw = 0.0
        try:
            fee_row = con.execute("""
                SELECT COALESCE(SUM(ABS(fba_fees)) + SUM(ABS(commission)), 0)
                FROM financial_events
                WHERE date >= ? AND date <= ?
            """, [str(sd), str(ed)]).fetchone()
            ty_fees_raw = float(fee_row[0]) if fee_row else 0.0
        except Exception:
            pass
        ty_fees = ty_fees_raw if ty_fees_raw > 0 else round(ty_sales * 0.27, 2)

        con.close()
        pending_est = round(o_sales - shipped_sales, 2)
        return {
            "period": "today",
            "central_date": str(sd),
            "central_window": {"start": s_iso, "end": e_iso},
            "step1_daily_sales_ALL": {"sales": float(ds_row[0]), "units": int(ds_row[1])},
            "step2_orders_supplement": {
                "total_sales": round(o_sales, 2),
                "total_units": o_units,
                "total_orders": o_cnt,
                "shipped_sales": round(shipped_sales, 2),
                "shipped_orders": shipped_cnt,
                "pending_estimated_revenue": pending_est,
                "pending_orders": len(pending_detail),
                "pending_detail": pending_detail,
                "asin_prices_available_in_daily_sales": asin_prices_count,
            },
            "supplement_triggered": supplement_triggered,
            "final_sales": round(ty_sales, 2),
            "final_units": ty_units,
            "final_fees_est": ty_fees,
            "gross_margin_est": round(ty_sales - ty_fees, 2),
            "note": (
                "final_sales = MAX(daily_sales_ALL, shipped + pending_est). "
                "Compare final_sales to Amazon Seller Central 'Today so far'. "
                "If pending_estimated_revenue=0 but pending_orders>0, the asin_prices "
                "lookup has no data — run a full sync to populate per-ASIN daily_sales rows."
            )
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


# ══════════════════════════════════════════════════════════════
# LEGACY ENDPOINTS — used by Dashboard.jsx and other pages
# ══════════════════════════════════════════════════════════════


@router.get("/api/summary")
def summary(
    days: int = Query(365, description="Number of days to include"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
    marketplace: str | None = Query(None),
):
    """High-level KPIs: revenue, units, orders, sessions, AUR, conv rate."""
    con = get_db()
    cutoff = (get_today(con) - timedelta(days=days)).strftime("%Y-%m-%d") if days > 0 else get_today(con).strftime("%Y-%m-%d")
    hf, hp = _hierarchy_filter(division, customer, platform, marketplace)

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
    aur = round(float(revenue) / units, 2) if units else 0
    conv_rate = round(float(units) / sessions * 100, 1) if sessions else 0

    products = _build_product_list(con, cutoff)
    prod_rev = sum(p["rev"] for p in products)
    prod_net = sum(p["net"] for p in products)

    if prod_rev > 0 and revenue > 0:
        margin_pct = float(prod_net) / float(prod_rev)
        total_net = round(float(revenue) * margin_pct, 2)
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
            est_fba = float(fee_row[0]) if fee_row and fee_row[0] > 0 else float(revenue) * 0.12
            est_ref = float(fee_row[1]) if fee_row and fee_row[1] > 0 else float(revenue) * 0.15
        except Exception:
            est_fba = float(revenue) * 0.12
            est_ref = float(revenue) * 0.15
        est_cogs = float(revenue) * 0.35
        total_net = round(float(revenue) - est_cogs - est_fba - est_ref, 2)
        margin = round(total_net / float(revenue) * 100)
    else:
        total_net = 0
        margin = 0

    # ── Returns/refunds from financial_events ──
    returns_units = 0
    returns_amount = 0.0
    try:
        ret_row = con.execute(f"""
            SELECT COALESCE(COUNT(*), 0),
                   COALESCE(SUM(ABS(product_charges)), 0)
            FROM financial_events
            WHERE date >= ? AND event_type = 'Refund'{hf}
        """, [cutoff] + hp).fetchone()
        if ret_row:
            returns_units = int(ret_row[0])
            returns_amount = round(float(ret_row[1]), 2)
    except Exception as e:
        logger.debug(f"Returns query: {e}")

    # Also try analytics_daily for returns if available
    if used_analytics:
        try:
            ret_analytics = con.execute(f"""
                SELECT COALESCE(SUM(returns_units), 0),
                       COALESCE(SUM(returns_amount), 0)
                FROM analytics_daily
                WHERE date >= ?{hf}
            """, [cutoff] + hp).fetchone()
            if ret_analytics and int(ret_analytics[0]) > 0:
                returns_units = int(ret_analytics[0])
                returns_amount = round(float(ret_analytics[1]), 2)
        except Exception:
            pass

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
        "returns": returns_units,
        "returnsAmount": returns_amount,
    }


@router.get("/api/daily")
def get_daily_sales(
    days: int = Query(365),
    granularity: str = Query("daily"),
    division: str | None = Query(None),
    customer: str | None = Query(None),
    platform: str | None = Query(None),
    marketplace: str | None = Query(None),
):
    """Time-series sales data for charts."""
    con = get_db()
    cutoff = (get_today(con) - timedelta(days=days)).strftime("%Y-%m-%d")
    hf, hp = _hierarchy_filter(division, customer, platform, marketplace)

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

    # ── Fallback: if asin='ALL' rows are sparse, aggregate from per-ASIN rows ──
    # This happens when the Sales & Traffic Report only has a few days of 'ALL'
    # but per-ASIN rows exist from backfill or financial events.
    if len(rows) < (days * 0.3):
        try:
            agg_rows = con.execute(f"""
                SELECT date,
                       COALESCE(SUM(ordered_product_sales), 0) AS revenue,
                       COALESCE(SUM(units_ordered), 0) AS units,
                       COALESCE(SUM(sessions), 0) AS sessions
                FROM daily_sales
                WHERE date >= ? AND asin != 'ALL'{hf}
                GROUP BY date
                ORDER BY date
            """, [cutoff] + hp).fetchall()
            if len(agg_rows) > len(rows):
                # Merge: use agg_rows as base, overlay with 'ALL' rows where they exist
                all_rows_map = {}
                for r in rows:
                    all_rows_map[fmt_date(r[0])] = r
                merged = []
                for r in agg_rows:
                    ds = fmt_date(r[0])
                    if ds in all_rows_map:
                        merged.append(all_rows_map[ds])
                    else:
                        merged.append(r)
                rows = merged
        except Exception as e:
            logger.warning(f"/api/daily per-ASIN fallback: {e}")

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
            "aur": round(float(revenue_val) / units_val, 2) if units_val else 0,
            "convRate": round(float(units_val) / sessions_val * 100, 1) if sessions_val else 0,
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
    marketplace: str | None = Query(None),
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
        "name": load_item_master_names().get(asin) or cogs_info.get("product_name") or asin,
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
    marketplace: str | None = Query(None),
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

    hf, hp = _hierarchy_filter(division, customer, platform, marketplace)

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

        # PostgreSQL may return Decimal — coerce to native Python types
        rev, units, sessions = float(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
        # True order count from orders table — exclude Cancelled
        try:
            o_row = con.execute(f"""
                SELECT COUNT(DISTINCT order_id) FROM orders
                WHERE purchase_date >= ? AND purchase_date < ?
                  AND (order_status IS NULL
                       OR order_status NOT IN ('Cancelled','Canceled')){hf}
            """, [p["start"], p["end"]] + hp).fetchone()
            orders = int(o_row[0]) if o_row and o_row[0] else units
            # Orders can never exceed units — cap it
            if units > 0 and orders > units:
                orders = units
        except Exception:
            orders = units
        aur = round(float(rev) / units, 2) if units else 0
        conv = round(float(units) / sessions * 100, 1) if sessions else 0

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

        # Use actual fees if they look reasonable, otherwise use ratio-based estimates.
        # financial_events often only covers ~90 days of settlements, so for longer
        # periods (YTD, full year) the actual fees may cover only a fraction of revenue.
        # Sanity check: actual fees should be at least 15% of what the ratio estimates
        # predict; if lower, the coverage is incomplete and estimates are more accurate.
        estimated_total = rev * (fba_pct + referral_pct)
        if (actual_fba > 0 or actual_referral > 0) and (actual_fba + actual_referral) >= estimated_total * 0.15:
            fba_fees = actual_fba
            referral_fees = actual_referral
        else:
            fba_fees = round(rev * fba_pct, 2)
            referral_fees = round(rev * referral_pct, 2)
        amazon_fees = round(fba_fees + referral_fees, 2)
        # Use compute_cogs_for_range for accurate per-ASIN COGS
        cogs = compute_cogs_for_range(con, p["start"], p["end"], hf, hp)
        if cogs == 0 and rev > 0:
            cogs = round(rev * cogs_pct, 2)  # fallback to ratio estimate
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

        ly_rev, ly_units, ly_sessions = float(ly_row[0] or 0), int(ly_row[1] or 0), int(ly_row[2] or 0)
        try:
            ly_o_row = con.execute(f"""
                SELECT COUNT(DISTINCT order_id) FROM orders
                WHERE purchase_date >= ? AND purchase_date < ?
                  AND (order_status IS NULL
                       OR order_status NOT IN ('Cancelled','Canceled')){hf}
            """, [ly_start.strftime("%Y-%m-%d"), ly_end.strftime("%Y-%m-%d")] + hp).fetchone()
            ly_orders = int(ly_o_row[0]) if ly_o_row and ly_o_row[0] else ly_units
            if ly_units > 0 and ly_orders > ly_units:
                ly_orders = ly_units
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
    marketplace: str | None = Query(None),
):
    """Monthly revenue broken down by year for YoY comparison (legacy)."""
    con = get_db()
    hf, hp = _hierarchy_filter(division, customer, platform, marketplace)
    rows = None

    try:
        analytics_rows = con.execute(f"""
            SELECT EXTRACT(YEAR FROM CAST(date AS DATE)) AS yr, EXTRACT(MONTH FROM CAST(date AS DATE)) AS mo,
                   COALESCE(SUM(gross_revenue), 0) AS revenue
            FROM analytics_daily
            WHERE date >= '2024-01-01'{hf}
            GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE)), EXTRACT(MONTH FROM CAST(date AS DATE))
            ORDER BY yr, mo
        """, hp).fetchall()
        if analytics_rows and len(analytics_rows) > 0 and any(r[2] > 0 for r in analytics_rows):
            rows = analytics_rows
    except Exception:
        pass

    if rows is None:
        try:
            rows = con.execute(f"""
                SELECT EXTRACT(YEAR FROM CAST(date AS DATE)) AS yr, EXTRACT(MONTH FROM CAST(date AS DATE)) AS mo,
                       COALESCE(SUM(ordered_product_sales), 0) AS revenue
                FROM daily_sales
                WHERE asin = 'ALL' AND date IS NOT NULL AND date >= '2024-01-01'{hf}
                GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE)), EXTRACT(MONTH FROM CAST(date AS DATE))
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
    marketplace: str | None = Query(None),
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
    marketplace: str | None = Query(None),
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


# ──────────────────────────────────────────────────────────────────────────────
# HOURLY SALES HEATMAP — 30-day × 24-hour grid
# ──────────────────────────────────────────────────────────────────────────────

def save_hourly_to_db(target_date=None):
    """Fetch hourly SP-API data for target_date and upsert into hourly_sales table.

    Called by the scheduler every hour to accumulate historical hourly data.
    Also called on-demand when the heatmap endpoint finds missing days.
    """
    try:
        from services.sp_api import get_hourly_sales
        from core.database import get_db_rw

        if target_date is None:
            target_date = _today_central()

        rows = get_hourly_sales(target_date)
        if not rows:
            logger.debug(f"save_hourly_to_db({target_date}): no data from SP-API")
            return 0

        con = get_db_rw()
        saved = 0
        now_ts = datetime.now(CENTRAL).isoformat()
        for r in rows:
            h = r.get("hour", 0)
            s = float(r.get("sales", 0) or 0)
            u = int(r.get("units", 0) or 0)
            o = int(r.get("orders", 0) or 0)
            con.execute("""
                INSERT INTO hourly_sales (sale_date, hour, sales, units, orders, division, customer, platform, synced_at)
                VALUES (?, ?, ?, ?, ?, 'all', 'amazon', 'sp_api', ?)
                ON CONFLICT (sale_date, hour, division, customer, platform)
                DO UPDATE SET sales=EXCLUDED.sales, units=EXCLUDED.units, orders=EXCLUDED.orders, synced_at=EXCLUDED.synced_at
            """, [str(target_date), h, s, u, o, now_ts])
            saved += 1
        con.close()
        total_sales = sum(float(r.get("sales", 0) or 0) for r in rows)
        logger.info(f"save_hourly_to_db({target_date}): saved {saved} hours, total=${total_sales:,.2f}")
        return saved
    except Exception as ex:
        logger.warning(f"save_hourly_to_db({target_date}) error: {ex}")
        return 0


@router.get("/api/sales/hourly-heatmap")
def hourly_heatmap(
    days: int = Query(30, ge=1, le=90),
    division: str = Query(""),
    customer: str = Query(""),
):
    """30-day × 24-hour sales heatmap for the Sales page.

    Returns `days` calendar days (newest = last element) with 24 hourly buckets each.
    Pulls from hourly_sales table. If a day has no DB rows (gap), fetches from SP-API
    on-demand for dates in the past 7 days (avoids hammering API for old dates).

    Response shape:
    {
      "days": [
        {
          "date": "2026-03-01",
          "label": "Mar 1",
          "dayOfWeek": "Sun",
          "isToday": false,
          "hours": [ {hour, sales, units, orders}, ... ×24 ]
        }, ...
      ],
      "maxSales": 1234.56,
      "maxUnits": 45,
      "lastUpdated": "2026-03-18T12:05:00"
    }
    """
    today_ct = _today_central()
    con = get_db()
    hw, hp = _hier_where(division, customer)

    # Build date range: today back `days` days
    dates = [today_ct - timedelta(days=i) for i in range(days - 1, -1, -1)]  # oldest→newest

    # ── Pull all stored rows for the date range ──
    range_start = str(dates[0])
    range_end   = str(dates[-1])
    try:
        stored_rows = con.execute("""
            SELECT sale_date, hour, sales, units, orders
            FROM hourly_sales
            WHERE sale_date >= ? AND sale_date <= ?
              AND division = 'all' AND customer = 'amazon' AND platform = 'sp_api'
        """, [range_start, range_end]).fetchall()
    except Exception:
        stored_rows = []
    con.close()

    # Index stored data: {date_str: {hour: {sales, units, orders}}}
    db_map: dict = {}
    for row in stored_rows:
        ds = str(row[0])
        h  = int(row[1])
        if ds not in db_map:
            db_map[ds] = {}
        db_map[ds][h] = {
            "hour":   h,
            "sales":  float(row[2] or 0),
            "units":  int(row[3] or 0),
            "orders": int(row[4] or 0),
        }

    # ── Populate today's data synchronously if missing OR all-zero (stale bad data) ──
    today_str = str(today_ct)
    today_total = sum(v.get('sales', 0) for v in db_map.get(today_str, {}).values())
    if today_str not in db_map or not db_map[today_str] or today_total == 0:
        try:
            from services.sp_api import get_hourly_sales
            live_rows = get_hourly_sales(today_ct)
            if live_rows:
                db_map[today_str] = {r['hour']: r for r in live_rows}
                logger.info(f"hourly_heatmap: live-fetched today ({today_str}) — {len(live_rows)} hours")
                import threading
                threading.Thread(target=save_hourly_to_db, args=(today_ct,), daemon=True).start()
            else:
                logger.warning(f"hourly_heatmap: live fetch for today returned no rows")
        except Exception as _hm_ex:
            logger.warning(f"hourly_heatmap: live fetch failed: {_hm_ex}")

    # Back-fill recent days that are missing OR have all-zero sales (bad cached data)
    recent_cutoff = today_ct - timedelta(days=7)
    for _past_d in sorted(dates, reverse=True):
        if _past_d >= today_ct:
            continue
        if _past_d < recent_cutoff:
            break
        _past_str = str(_past_d)
        _day_total = sum(v.get('sales', 0) for v in db_map.get(_past_str, {}).values())
        if _past_str not in db_map or not db_map.get(_past_str) or _day_total == 0:
            import threading
            threading.Thread(target=save_hourly_to_db, args=(_past_d,), daemon=True).start()
            logger.info(f"hourly_heatmap: kicked off background fill for {_past_d} (total was ${_day_total:.2f})")
            break  # one background fill per request

    # ── Build response ──
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    now_ct = datetime.now(CENTRAL)
    current_hour = now_ct.hour

    result_days = []
    max_sales = 0.0
    max_units = 0

    for d in dates:
        ds = str(d)
        hours_data = db_map.get(ds, {})
        is_today = (d == today_ct)

        hours_list = []
        for h in range(24):
            cell = hours_data.get(h, {"hour": h, "sales": 0.0, "units": 0, "orders": 0})
            # For today: future hours are null (no data yet)
            if is_today and h > current_hour:
                cell = {"hour": h, "sales": None, "units": None, "orders": None}
            else:
                s = float(cell.get("sales") or 0)
                u = int(cell.get("units") or 0)
                if s > max_sales:
                    max_sales = s
                if u > max_units:
                    max_units = u
                cell = {"hour": h, "sales": round(s, 2), "units": u, "orders": int(cell.get("orders") or 0)}
            hours_list.append(cell)

        result_days.append({
            "date":      ds,
            "label":     f"{month_names[d.month - 1]} {d.day}",
            "dayOfWeek": day_names[d.weekday()],
            "isToday":   is_today,
            "hours":     hours_list,
        })

    return {
        "days":        result_days,
        "maxSales":    round(max_sales, 2),
        "maxUnits":    max_units,
        "lastUpdated": now_ct.strftime("%Y-%m-%dT%H:%M:%S"),
    }
