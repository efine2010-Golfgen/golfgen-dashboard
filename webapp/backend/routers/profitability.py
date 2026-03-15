"""Profitability and P&L routes — Profitability Command Center.

Endpoints:
  GET /api/profitability          — Sellerboard-style waterfall by period
  GET /api/profitability/items    — Per-ASIN profitability table
  GET /api/profitability/overview — KPIs + waterfall + margin trend + fee donut
  GET /api/profitability/fee-detail — Complete Amazon fee breakdown
  GET /api/profitability/aur      — AUR trend by SKU + bubble chart data
  CRUD /api/profitability/sale-prices — Sale price management
  CRUD /api/profitability/coupons    — Coupon management (multi-item)
  POST /api/profitability/push-price  — Push sale price to Amazon SP-API
  GET /api/pnl                    — Legacy P&L waterfall
"""
import csv
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, Query, Body
from pydantic import BaseModel

from core.config import DB_PATH, COGS_PATH, TIMEZONE, USE_POSTGRES
from core.database import get_db, get_db_rw
from core.hierarchy import hierarchy_filter

logger = logging.getLogger("golfgen")
router = APIRouter()

# ── Helpers ──────────────────────────────────────────────────────────────

def _n(v):
    """Coerce Decimal/None to float for safe JSON serialization."""
    if v is None:
        return 0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0


def get_today(con) -> datetime:
    """Get today's date from the latest row in database."""
    try:
        row = con.execute("""
            SELECT MAX(date) FROM daily_sales
        """).fetchone()
        if row and row[0]:
            return datetime.fromisoformat(str(row[0]))
    except Exception:
        pass
    return datetime.now(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)


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


def _load_pricing_cache() -> dict:
    """Load pricing sync cache from JSON."""
    from core.config import PRICING_CACHE_PATH
    cache = {}
    if PRICING_CACHE_PATH.exists():
        try:
            with open(PRICING_CACHE_PATH) as f:
                cache = json.load(f)
        except Exception:
            pass
    return cache


def _next_id(con, table: str, seq_name: str = None) -> int:
    """Get next ID for a table — uses sequence on Postgres, MAX(id)+1 on DuckDB."""
    if USE_POSTGRES and seq_name:
        row = con.execute(f"SELECT nextval('{seq_name}')").fetchone()
        return int(row[0])
    else:
        row = con.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()
        return int(row[0])


def _build_product_list(con, cutoff: str, division=None, customer=None, platform=None, marketplace=None) -> list:
    """Build per-ASIN product list with revenue, units, COGS, fees, and ad spend."""
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)
    filters = "WHERE date >= ? AND asin <> 'ALL'" + hf
    params = [cutoff] + hp
    asin_rows = con.execute(f"""
        SELECT asin,
               COALESCE(SUM(ordered_product_sales), 0) AS revenue,
               COALESCE(SUM(units_ordered), 0) AS units
        FROM daily_sales
        {filters}
        GROUP BY asin
        ORDER BY revenue DESC
    """, params).fetchall()

    cogs_data = load_cogs()
    inv_names = {}
    try:
        inv_rows = con.execute("SELECT asin, sku, product_name FROM fba_inventory").fetchall()
        for ir in inv_rows:
            inv_names[ir[0]] = {"sku": ir[1] or "", "product_name": ir[2] or ""}
    except Exception:
        pass

    # Get item_master for division info
    im_div = {}
    try:
        im_rows = con.execute("SELECT asin, division FROM item_master").fetchall()
        for imr in im_rows:
            im_div[imr[0]] = imr[1] or "unknown"
    except Exception:
        pass

    products = []
    for r in asin_rows:
        asin, rev, units = r[0], _n(r[1]), int(r[2] or 0)
        inv = inv_names.get(asin, {})
        sku = inv.get("sku", "")
        name = inv.get("product_name", "") or cogs_data.get(asin, {}).get("product_name", "")
        if units == 0:
            continue

        aur = rev / units if units else 0
        ci = cogs_data.get(asin) or cogs_data.get(sku or "") or {}
        cpu = ci.get("cogs", 0)
        if cpu == 0:
            cpu = round(aur * 0.35, 2)

        cogsTotal = units * cpu
        cogsPerUnit = cpu

        # Financial data
        fin_filter = "WHERE asin = ? AND date >= ?" + hf
        fin_params = [asin, cutoff] + hp
        fin_row = con.execute(f"""
            SELECT COALESCE(SUM(ABS(fba_fees)), 0),
                   COALESCE(SUM(ABS(commission)), 0)
            FROM financial_events
            {fin_filter}
        """, fin_params).fetchone()
        fba_actual, comm_actual = _n(fin_row[0]), _n(fin_row[1]) if fin_row else (0, 0)

        fbaTotal = fba_actual if fba_actual > 0 else round(rev * 0.12, 2)
        referralTotal = comm_actual if comm_actual > 0 else round(rev * 0.15, 2)

        # Ad spend
        adSpend = 0
        try:
            ad_filter = "WHERE asin = ? AND date >= ?"
            ad_params = [asin, cutoff]
            if division:
                ad_filter += " AND division = ?"
                ad_params.append(division)
            if customer:
                ad_filter += " AND customer = ?"
                ad_params.append(customer)
            ad_row = con.execute(f"""
                SELECT COALESCE(SUM(spend), 0)
                FROM advertising
                {ad_filter}
            """, ad_params).fetchone()
            adSpend = _n(ad_row[0]) if ad_row else 0
        except Exception:
            pass

        net = round(rev - cogsTotal - fbaTotal - referralTotal - adSpend, 2)

        products.append({
            "asin": asin,
            "sku": sku,
            "name": name,
            "units": units,
            "rev": rev,
            "price": aur,
            "cogsPerUnit": cogsPerUnit,
            "cogsTotal": cogsTotal,
            "fbaTotal": fbaTotal,
            "referralTotal": referralTotal,
            "adSpend": adSpend,
            "net": net,
            "division": im_div.get(asin, "unknown"),
        })

    return products


def _build_waterfall(con, cogs_data, start, end, division=None, customer=None, platform=None, marketplace=None):
    """Build Sellerboard-style waterfall for a single period."""
    div_cust_sql, div_cust_params = hierarchy_filter(division, customer, platform, marketplace)

    row = con.execute(f"""
        SELECT COALESCE(SUM(ordered_product_sales), 0),
               COALESCE(SUM(units_ordered), 0)
        FROM daily_sales
        WHERE date >= ? AND date < ? AND asin = 'ALL'{div_cust_sql}
    """, [start, end] + div_cust_params).fetchone()
    sales, units = _n(row[0]), int(row[1] or 0)

    asin_rows = con.execute(f"""
        SELECT asin,
               COALESCE(SUM(ordered_product_sales), 0) AS revenue,
               COALESCE(SUM(units_ordered), 0) AS units
        FROM daily_sales
        WHERE date >= ? AND date < ? AND asin <> 'ALL'{div_cust_sql}
        GROUP BY asin
    """, [start, end] + div_cust_params).fetchall()

    # Account-level financial totals
    acct_fin = {"fba": 0, "comm": 0, "promo": 0, "shipping": 0, "other": 0,
                "refund_amt": 0, "refund_count": 0}
    try:
        acct_row = con.execute(f"""
            SELECT SUM(ABS(fba_fees)),
                   SUM(ABS(commission)),
                   SUM(ABS(promotion_amount)),
                   SUM(ABS(shipping_charges)),
                   SUM(ABS(other_fees)),
                   SUM(CASE WHEN event_type ILIKE '%refund%' OR event_type ILIKE '%return%'
                       THEN ABS(product_charges) ELSE 0 END),
                   COUNT(CASE WHEN event_type ILIKE '%refund%' OR event_type ILIKE '%return%'
                       THEN 1 END)
            FROM financial_events
            WHERE date >= ? AND date < ?{div_cust_sql}
        """, [start, end] + div_cust_params).fetchone()
        if acct_row and acct_row[0] is not None:
            acct_fin = {"fba": _n(acct_row[0]), "comm": _n(acct_row[1]),
                        "promo": _n(acct_row[2]), "shipping": _n(acct_row[3]),
                        "other": _n(acct_row[4]), "refund_amt": _n(acct_row[5]),
                        "refund_count": int(acct_row[6] or 0)}
    except Exception:
        pass

    # Ad spend
    total_ad = 0
    try:
        ad_row = con.execute(f"""
            SELECT COALESCE(SUM(spend), 0)
            FROM advertising
            WHERE date >= ? AND date < ?{div_cust_sql}
        """, [start, end] + div_cust_params).fetchone()
        total_ad = _n(ad_row[0]) if ad_row else 0
    except Exception:
        pass

    # Per-ASIN costs
    total_cogs = total_fba = total_referral = total_promo = 0
    total_shipping = total_refunds = total_other_fees = 0
    total_refund_units = 0
    prod_rev = 0

    # Per-ASIN financial data
    fin_by_asin = {}
    try:
        fin_rows = con.execute(f"""
            SELECT asin,
                   SUM(ABS(fba_fees)) AS fba,
                   SUM(ABS(commission)) AS comm,
                   SUM(ABS(promotion_amount)) AS promo,
                   SUM(ABS(shipping_charges)) AS shipping,
                   SUM(ABS(other_fees)) AS other,
                   SUM(CASE WHEN event_type ILIKE '%refund%' OR event_type ILIKE '%return%'
                       THEN ABS(product_charges) ELSE 0 END) AS refund_amt,
                   COUNT(CASE WHEN event_type ILIKE '%refund%' OR event_type ILIKE '%return%'
                       THEN 1 END) AS refund_count
            FROM financial_events
            WHERE date >= ? AND date < ?{div_cust_sql}
            GROUP BY asin
        """, [start, end] + div_cust_params).fetchall()
        # SKU→ASIN resolution
        sku_to_asin = {}
        try:
            inv_map = con.execute("SELECT sku, asin FROM fba_inventory WHERE sku IS NOT NULL AND asin IS NOT NULL").fetchall()
            for im in inv_map:
                if im[0] and im[1]:
                    sku_to_asin[im[0]] = im[1]
                    base = im[0].rsplit("-", 1)[0] if "-" in im[0] else im[0]
                    if base not in sku_to_asin:
                        sku_to_asin[base] = im[1]
        except Exception:
            pass
        for r in fin_rows:
            raw_key = r[0]
            resolved = sku_to_asin.get(raw_key, raw_key)
            entry = {"fba": _n(r[1]), "comm": _n(r[2]), "promo": _n(r[3]),
                     "shipping": _n(r[4]), "other": _n(r[5]),
                     "refund_amt": _n(r[6]), "refund_count": int(r[7] or 0)}
            if resolved in fin_by_asin:
                for k in entry:
                    fin_by_asin[resolved][k] = fin_by_asin[resolved].get(k, 0) + entry[k]
            else:
                fin_by_asin[resolved] = entry
    except Exception:
        pass

    for ar in asin_rows:
        asin, rev, u = ar[0], _n(ar[1]), int(ar[2] or 0)
        if u == 0:
            continue
        prod_rev += rev
        aur = rev / u if u else 0

        ci = cogs_data.get(asin, {})
        cpu = ci.get("cogs", 0)
        if cpu == 0:
            cpu = round(aur * 0.35, 2)
        total_cogs += u * cpu

        fin = fin_by_asin.get(asin, {})
        actual_fba = fin.get("fba", 0)
        actual_comm = fin.get("comm", 0)
        total_fba += actual_fba if actual_fba > 0 else round(rev * 0.12, 2)
        total_referral += actual_comm if actual_comm > 0 else rev * 0.15
        total_promo += fin.get("promo", 0)
        total_shipping += fin.get("shipping", 0)
        total_refunds += fin.get("refund_amt", 0)
        total_other_fees += fin.get("other", 0)
        total_refund_units += fin.get("refund_count", 0)

    # Use account-level totals as fallback
    if total_refunds == 0 and acct_fin["refund_amt"] > 0:
        total_refunds = acct_fin["refund_amt"]
        total_refund_units = acct_fin["refund_count"]
    if total_fba == 0 and acct_fin["fba"] > 0:
        total_fba = acct_fin["fba"]
    if total_referral == 0 and acct_fin["comm"] > 0:
        total_referral = acct_fin["comm"]
    if total_promo == 0 and acct_fin["promo"] > 0:
        total_promo = acct_fin["promo"]
    if total_shipping == 0 and acct_fin["shipping"] > 0:
        total_shipping = acct_fin["shipping"]

    # Scale to actual sales
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
        cogs = round(sales * 0.35, 2)
        fba_fees = round(sales * 0.12, 2)
        referral_fees = round(sales * 0.15, 2)
        promo = shipping = refunds = other_fees = 0
    else:
        cogs = fba_fees = referral_fees = promo = shipping = refunds = other_fees = 0

    amazon_fees = round(fba_fees + referral_fees, 2)
    ad_spend = round(total_ad, 2)
    refund_units = total_refund_units
    return_pct = round(refund_units / units * 100, 1) if units > 0 else 0

    gross_profit = round(sales - promo - ad_spend - shipping - refunds - amazon_fees - cogs, 2)
    net_profit = gross_profit
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
        "storageFees": round(other_fees * 0.5, 2),  # Estimate storage as ~50% of other
        "cogs": cogs,
        "indirect": 0,
        "grossProfit": gross_profit,
        "netProfit": net_profit,
        "margin": margin,
        "roi": roi,
        "realAcos": real_acos,
    }


# ── Existing endpoints (kept for backward compat) ────────────────────────

@router.get("/api/pnl")
def pnl_waterfall(days: int = Query(365), division: Optional[str] = None,
                  customer: Optional[str] = None, platform: Optional[str] = None,
                  marketplace: Optional[str] = None):
    """Legacy P&L waterfall."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)
    rev_filter = "WHERE date >= ? AND asin = 'ALL'" + hf
    rev_params = [cutoff] + hp
    actual_rev = _n(con.execute(f"""
        SELECT COALESCE(SUM(ordered_product_sales), 0)
        FROM daily_sales {rev_filter}
    """, rev_params).fetchone()[0])

    products = _build_product_list(con, cutoff, division, customer, platform, marketplace)
    prod_rev = sum(p["rev"] for p in products)
    prod_cogs = sum(p["cogsTotal"] for p in products)
    prod_fba = sum(p["fbaTotal"] for p in products)
    prod_referral = sum(p["referralTotal"] for p in products)
    prod_ad = sum(p["adSpend"] for p in products)

    if prod_rev > 0:
        scale = actual_rev / prod_rev
        cogs = round(prod_cogs * scale, 2)
        fba = round(prod_fba * scale, 2)
        referral = round(prod_referral * scale, 2)
        ad_spend = round(prod_ad * scale, 2)
    else:
        fba = round(actual_rev * 0.12, 2)
        referral = round(actual_rev * 0.15, 2)
        cogs = round(actual_rev * 0.35, 2)
        ad_spend = 0

    con.close()
    net = round(actual_rev - cogs - fba - referral - ad_spend, 2)
    return {
        "days": days, "revenue": round(actual_rev, 2), "cogs": cogs,
        "fbaFees": fba, "referralFees": referral, "adSpend": ad_spend, "netProfit": net,
    }


@router.get("/api/profitability")
def profitability(view: str = Query("realtime"), division: Optional[str] = None,
                  customer: Optional[str] = None, platform: Optional[str] = None,
                  marketplace: Optional[str] = None):
    """Sellerboard-style profit waterfall with period comparison."""
    con = get_db()
    cogs_data = load_cogs()

    today_start = get_today(con)
    today_start = today_start.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = (today_start + timedelta(days=1)).strftime("%Y-%m-%d")
    yr = today_start.year
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

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
            periods.append({"label": label, "sub": month_names[m - 1] + " " + str(y),
                            "start": m_start_actual.strftime("%Y-%m-%d"), "end": m_end_actual.strftime("%Y-%m-%d")})
        periods.append({"label": "Last 12 Mo", "start": (today_start - timedelta(days=365)).strftime("%Y-%m-%d"), "end": tomorrow})
    elif view == "yearly":
        try:
            ytd_comp_end = datetime(yr - 1, today_start.month, today_start.day) + timedelta(days=1)
        except ValueError:
            ytd_comp_end = datetime(yr - 1, today_start.month, 28) + timedelta(days=1)
        periods = [
            {"label": f"{yr} YTD", "start": f"{yr}-01-01", "end": tomorrow},
            {"label": f"{yr - 1} YTD", "sub": "same window comp", "start": f"{yr - 1}-01-01", "end": ytd_comp_end.strftime("%Y-%m-%d")},
            {"label": f"{yr - 1} Full", "start": f"{yr - 1}-01-01", "end": f"{yr}-01-01"},
            {"label": f"{yr - 2} Full", "start": f"{yr - 2}-01-01", "end": f"{yr - 1}-01-01"},
        ]
    elif view == "monthly2026":
        periods = []
        for m in range(1, today_start.month + 1):
            m_start = datetime(yr, m, 1)
            m_end = tomorrow if m == today_start.month else datetime(yr, m + 1, 1).strftime("%Y-%m-%d")
            if isinstance(m_end, datetime):
                m_end = m_end.strftime("%Y-%m-%d")
            periods.append({"label": month_names[m - 1], "sub": str(yr),
                            "start": m_start.strftime("%Y-%m-%d"), "end": m_end})
        periods.append({"label": f"{yr} Total", "sub": "YTD", "start": f"{yr}-01-01", "end": tomorrow})
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

    results = []
    for p in periods:
        wf = _build_waterfall(con, cogs_data, p["start"], p["end"], division, customer, platform, marketplace)
        wf["label"] = p["label"]
        wf["sub"] = p.get("sub", "")
        results.append(wf)

    con.close()
    return {"view": view, "periods": results}


@router.get("/api/profitability/items")
def profitability_items(days: int = Query(365), division: Optional[str] = None,
                        customer: Optional[str] = None, platform: Optional[str] = None,
                        marketplace: Optional[str] = None):
    """Per-ASIN profitability breakdown matching Sellerboard item view."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    products = _build_product_list(con, cutoff, division, customer, platform, marketplace)

    hf, hp = hierarchy_filter(division, customer, platform, marketplace)
    fin_filter = "WHERE date >= ?" + hf
    fin_params = [cutoff] + hp
    fin_by_asin = {}
    try:
        fin_rows = con.execute(f"""
            SELECT asin,
                   SUM(ABS(promotion_amount)) AS promo,
                   SUM(ABS(shipping_charges)) AS shipping,
                   SUM(ABS(other_fees)) AS other,
                   SUM(CASE WHEN event_type ILIKE '%refund%' OR event_type ILIKE '%return%'
                       THEN ABS(product_charges) ELSE 0 END) AS refund_amt,
                   COUNT(CASE WHEN event_type ILIKE '%refund%' OR event_type ILIKE '%return%'
                       THEN 1 END) AS refund_count
            FROM financial_events
            {fin_filter}
            GROUP BY asin
        """, fin_params).fetchall()
        fin_by_asin = {r[0]: {"promo": _n(r[1]), "shipping": _n(r[2]), "other": _n(r[3]),
                               "refund_amt": _n(r[4]), "refund_count": int(r[5] or 0)} for r in fin_rows}
    except Exception:
        pass

    ad_by_asin = {}
    try:
        ad_rows = con.execute(f"""
            SELECT asin, COALESCE(SUM(spend), 0)
            FROM advertising
            {fin_filter}
            GROUP BY asin
        """, fin_params).fetchall()
        ad_by_asin = {r[0]: _n(r[1]) for r in ad_rows}
    except Exception:
        pass

    # Pricing/coupon enrichment
    pricing_cache = _load_pricing_cache()
    live_prices = pricing_cache.get("prices", {})
    live_coupons = pricing_cache.get("coupons", {})

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

        # Coupon data
        coupon = live_coupons.get(asin)
        coupon_status = coupon_end_date = coupon_discount = coupon_type = None
        if coupon:
            raw_state = coupon.get("state")
            state_map = {"ENABLED": "ACTIVE", "SCHEDULED": "SCHEDULED",
                         "PAUSED": "PAUSED", "EXPIRED": "EXPIRED"}
            coupon_status = state_map.get(raw_state, raw_state)
            coupon_end_date = coupon.get("endDate")
            coupon_discount = coupon.get("discountValue")
            coupon_type = coupon.get("discountType")

        # Live pricing
        live = live_prices.get(asin)
        sale_price = list_price = None
        if live:
            list_price = live.get("listingPrice")
            buy_box = live.get("buyBoxPrice")
            if list_price and buy_box and buy_box < list_price:
                sale_price = buy_box

        # Score: A (margin>=40), B (20-39), C (<20)
        score = "A" if margin >= 40 else "B" if margin >= 20 else "C"

        # COGS as % of revenue
        cogs_pct = round(cogs_total / rev * 100, 1) if rev > 0 else 0
        # Total fee % of revenue
        total_fee_pct = round(amazon_fees / rev * 100, 1) if rev > 0 else 0
        # Storage estimate (portion of other fees)
        storage = round(fin.get("other", 0) * 0.5, 2)

        items.append({
            "asin": asin,
            "sku": p["sku"],
            "name": p["name"],
            "division": p.get("division", "unknown"),
            "units": p["units"],
            "sales": rev,
            "promo": round(fin.get("promo", 0), 2),
            "adSpend": round(ad_spend, 2),
            "shipping": round(fin.get("shipping", 0), 2),
            "refunds": round(fin.get("refund_amt", 0), 2),
            "refundUnits": refund_units,
            "returnPct": return_pct,
            "amazonFees": round(amazon_fees, 2),
            "fbaFees": round(p["fbaTotal"], 2),
            "referralFees": round(p["referralTotal"], 2),
            "otherFees": round(fin.get("other", 0), 2),
            "storageFees": storage,
            "cogs": round(cogs_total, 2),
            "cogsPerUnit": round(p["cogsPerUnit"], 2),
            "cogsPct": cogs_pct,
            "totalFeePct": total_fee_pct,
            "indirect": 0,
            "netProfit": net,
            "margin": margin,
            "grossMargin": round((rev - cogs_total) / rev * 100, 1) if rev > 0 else 0,
            "roi": roi,
            "aur": round(p["price"], 2),
            "netPerUnit": round(net / p["units"], 2) if p["units"] > 0 else 0,
            "score": score,
            "couponStatus": coupon_status,
            "couponEndDate": coupon_end_date,
            "couponDiscount": coupon_discount,
            "couponType": coupon_type,
            "salePrice": sale_price,
            "listPrice": list_price,
        })

    items.sort(key=lambda x: x["sales"], reverse=True)
    con.close()
    return {"days": days, "items": items}


# ── NEW: Overview endpoint for P&L Overview tab ──────────────────────────

@router.get("/api/profitability/overview")
def profitability_overview(days: int = Query(30), division: Optional[str] = None,
                           customer: Optional[str] = None, platform: Optional[str] = None,
                           marketplace: Optional[str] = None):
    """Return all data for the P&L Overview tab: KPIs, waterfall rows, margin trend, fee donut."""
    con = get_db()
    cogs_data = load_cogs()
    now = datetime.now(ZoneInfo("America/Chicago"))
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    end = (now + timedelta(days=1)).strftime("%Y-%m-%d")

    # Main waterfall for the period
    wf = _build_waterfall(con, cogs_data, cutoff, end, division, customer, platform, marketplace)

    # Margin trend — 8 weekly buckets
    margin_trend = []
    for i in range(8, 0, -1):
        wk_end = (now - timedelta(days=(i - 1) * 7)).strftime("%Y-%m-%d")
        wk_start = (now - timedelta(days=i * 7)).strftime("%Y-%m-%d")
        wk = _build_waterfall(con, cogs_data, wk_start, wk_end, division, customer, platform, marketplace)
        gross_margin = round((wk["sales"] - wk["cogs"]) / wk["sales"] * 100, 1) if wk["sales"] > 0 else 0
        fee_ratio = round(wk["amazonFees"] / wk["sales"] * 100, 1) if wk["sales"] > 0 else 0
        margin_trend.append({
            "label": f"Wk-{i}",
            "grossMargin": gross_margin,
            "netMargin": wk["margin"],
            "feeRatio": fee_ratio,
        })

    # KPIs
    gross_rev = wf["sales"]
    total_fees = wf["amazonFees"]
    net_rev = round(gross_rev - total_fees, 2)
    gross_margin = round((gross_rev - wf["cogs"]) / gross_rev * 100, 1) if gross_rev > 0 else 0
    fee_pct = round(total_fees / gross_rev * 100, 1) if gross_rev > 0 else 0
    cogs_pct = round(wf["cogs"] / gross_rev * 100, 1) if gross_rev > 0 else 0

    # Units for avg unit cost / contribution per unit
    total_units = wf["units"]
    avg_unit_cost = round(wf["cogs"] / total_units, 2) if total_units > 0 else 0
    contribution_per_unit = round(wf["netProfit"] / total_units, 2) if total_units > 0 else 0

    # Fee breakdown for donut
    fee_donut = [
        {"name": "Referral", "value": round(wf["referralFees"], 2),
         "pct": round(wf["referralFees"] / total_fees * 100, 1) if total_fees > 0 else 0},
        {"name": "FBA Fulfillment", "value": round(wf["fbaFees"], 2),
         "pct": round(wf["fbaFees"] / total_fees * 100, 1) if total_fees > 0 else 0},
        {"name": "Storage", "value": round(wf["storageFees"], 2),
         "pct": round(wf["storageFees"] / total_fees * 100, 1) if total_fees > 0 else 0},
        {"name": "Other", "value": round(wf["otherFees"] - wf["storageFees"], 2),
         "pct": round((wf["otherFees"] - wf["storageFees"]) / total_fees * 100, 1) if total_fees > 0 else 0},
    ]

    con.close()
    return {
        "days": days,
        "kpis": {
            "grossRevenue": round(gross_rev, 2),
            "netRevenue": net_rev,
            "grossMargin": gross_margin,
            "totalFees": round(total_fees, 2),
            "feePct": fee_pct,
            "totalCogs": round(wf["cogs"], 2),
            "cogsPct": cogs_pct,
            "netProfit": round(wf["netProfit"], 2),
            "avgUnitCost": avg_unit_cost,
            "contributionPerUnit": contribution_per_unit,
            "units": total_units,
        },
        "waterfall": wf,
        "marginTrend": margin_trend,
        "feeDonut": fee_donut,
    }


# ── NEW: Fee Detail ────────────────────────────────────────────────────

@router.get("/api/profitability/fee-detail")
def fee_detail(days: int = Query(30), division: Optional[str] = None,
               customer: Optional[str] = None, platform: Optional[str] = None,
               marketplace: Optional[str] = None):
    """Complete Amazon fee breakdown by category."""
    con = get_db()
    now = datetime.now(ZoneInfo("America/Chicago"))
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)

    # Aggregate fee totals from financial_events
    fin_filter = f"WHERE date >= ?" + hf
    fin_params = [cutoff] + hp

    row = con.execute(f"""
        SELECT COALESCE(SUM(ABS(commission)), 0) AS referral,
               COALESCE(SUM(ABS(fba_fees)), 0) AS fba,
               COALESCE(SUM(principal), 0) AS principal
        FROM financial_events
        {fin_filter}
    """, fin_params).fetchone()

    referral_total = _n(row[0])
    fba_total = _n(row[1])
    revenue = _n(row[2])

    # Storage fees — estimate from inventory
    storage_est = 0
    try:
        inv_row = con.execute("""
            SELECT COALESCE(SUM(estimated_monthly_storage_fee), 0) FROM fba_inventory
        """).fetchone()
        if inv_row:
            storage_est = _n(inv_row[0]) * (days / 30.0)
    except Exception:
        pass

    # Other fees estimate (IPF, subscription, etc.) — 2% of revenue as fallback
    other_est = round(revenue * 0.02, 2) if revenue > 0 else 0
    total_fees = referral_total + fba_total + storage_est + other_est

    # Build categories
    categories = [
        {
            "name": "Referral Fees",
            "total": round(referral_total, 2),
            "pct_of_rev": round(referral_total / revenue * 100, 1) if revenue > 0 else 0,
            "items": [
                {"name": "Standard Referral (15%)", "amount": round(referral_total * 0.85, 2),
                 "pct_of_rev": round(referral_total * 0.85 / revenue * 100, 1) if revenue > 0 else 0},
                {"name": "Category Adjustments", "amount": round(referral_total * 0.15, 2),
                 "pct_of_rev": round(referral_total * 0.15 / revenue * 100, 1) if revenue > 0 else 0},
            ],
        },
        {
            "name": "FBA Fulfillment",
            "total": round(fba_total, 2),
            "pct_of_rev": round(fba_total / revenue * 100, 1) if revenue > 0 else 0,
            "items": [
                {"name": "Pick & Pack", "amount": round(fba_total * 0.65, 2),
                 "pct_of_rev": round(fba_total * 0.65 / revenue * 100, 1) if revenue > 0 else 0},
                {"name": "Weight Handling", "amount": round(fba_total * 0.25, 2),
                 "pct_of_rev": round(fba_total * 0.25 / revenue * 100, 1) if revenue > 0 else 0},
                {"name": "Inbound Placement", "amount": round(fba_total * 0.10, 2),
                 "pct_of_rev": round(fba_total * 0.10 / revenue * 100, 1) if revenue > 0 else 0},
            ],
        },
        {
            "name": "Storage Fees",
            "total": round(storage_est, 2),
            "pct_of_rev": round(storage_est / revenue * 100, 1) if revenue > 0 else 0,
            "items": [
                {"name": "Monthly Storage", "amount": round(storage_est * 0.8, 2),
                 "pct_of_rev": round(storage_est * 0.8 / revenue * 100, 1) if revenue > 0 else 0},
                {"name": "Long-Term Storage (LTSF)", "amount": round(storage_est * 0.2, 2),
                 "pct_of_rev": round(storage_est * 0.2 / revenue * 100, 1) if revenue > 0 else 0},
            ],
        },
        {
            "name": "Other Fees",
            "total": round(other_est, 2),
            "pct_of_rev": round(other_est / revenue * 100, 1) if revenue > 0 else 0,
            "items": [
                {"name": "Subscription / Professional", "amount": round(other_est * 0.4, 2),
                 "pct_of_rev": round(other_est * 0.4 / revenue * 100, 1) if revenue > 0 else 0},
                {"name": "Prep / Labeling", "amount": round(other_est * 0.3, 2),
                 "pct_of_rev": round(other_est * 0.3 / revenue * 100, 1) if revenue > 0 else 0},
                {"name": "Reimbursements (credit)", "amount": round(-other_est * 0.1, 2),
                 "pct_of_rev": round(other_est * 0.1 / revenue * 100, 1) if revenue > 0 else 0},
            ],
        },
    ]

    con.close()
    return {
        "days": days,
        "total_fees": round(total_fees, 2),
        "revenue": round(revenue, 2),
        "categories": categories,
    }


# ── NEW: AUR Analysis ───────────────────────────────────────────────────

@router.get("/api/profitability/aur")
def aur_analysis(division: Optional[str] = None, customer: Optional[str] = None,
                 platform: Optional[str] = None, marketplace: Optional[str] = None):
    """AUR trend by SKU — 8 weekly buckets + bubble chart data."""
    con = get_db()
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)
    now = datetime.now(ZoneInfo("America/Chicago"))

    # Get all ASINs with sales in last 60 days
    cutoff_60 = (now - timedelta(days=60)).strftime("%Y-%m-%d")
    asin_rows = con.execute(f"""
        SELECT asin,
               COALESCE(SUM(ordered_product_sales), 0) AS rev,
               COALESCE(SUM(units_ordered), 0) AS units
        FROM daily_sales
        WHERE date >= ? AND asin <> 'ALL'{hf}
        GROUP BY asin
        HAVING SUM(units_ordered) > 0
        ORDER BY rev DESC
    """, [cutoff_60] + hp).fetchall()

    # Get names / divisions
    inv_names = {}
    try:
        for ir in con.execute("SELECT asin, sku, product_name FROM fba_inventory").fetchall():
            inv_names[ir[0]] = {"sku": ir[1] or "", "name": ir[2] or ""}
    except Exception:
        pass
    im_div = {}
    try:
        for imr in con.execute("SELECT asin, division FROM item_master").fetchall():
            im_div[imr[0]] = imr[1] or "unknown"
    except Exception:
        pass

    cogs_data = load_cogs()
    results = []

    for row in asin_rows:
        asin, total_rev, total_units = row[0], _n(row[1]), int(row[2] or 0)
        if total_units == 0:
            continue
        inv = inv_names.get(asin, {})
        name = inv.get("name", "") or cogs_data.get(asin, {}).get("product_name", "")
        sku = inv.get("sku", "")
        div = im_div.get(asin, "unknown")
        overall_aur = round(total_rev / total_units, 2) if total_units > 0 else 0

        # Weekly AUR for 8 weeks
        weekly_aur = []
        for i in range(8, 0, -1):
            wk_end = (now - timedelta(days=(i - 1) * 7)).strftime("%Y-%m-%d")
            wk_start = (now - timedelta(days=i * 7)).strftime("%Y-%m-%d")
            wk_row = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0),
                       COALESCE(SUM(units_ordered), 0)
                FROM daily_sales
                WHERE date >= ? AND date < ? AND asin = ?{hf}
            """, [wk_start, wk_end, asin] + hp).fetchone()
            wk_rev, wk_units = _n(wk_row[0]), int(wk_row[1] or 0)
            wk_aur = round(wk_rev / wk_units, 2) if wk_units > 0 else None
            weekly_aur.append(wk_aur)

        # Net margin for bubble chart
        ci = cogs_data.get(asin) or cogs_data.get(sku or "") or {}
        cpu = ci.get("cogs", 0)
        if cpu == 0:
            cpu = round(overall_aur * 0.35, 2)
        est_fees_pct = 0.27  # ~27% average Amazon fees
        net_per_unit = round(overall_aur - cpu - (overall_aur * est_fees_pct), 2)
        net_margin = round(net_per_unit / overall_aur * 100, 1) if overall_aur > 0 else 0

        results.append({
            "asin": asin,
            "sku": sku,
            "name": name,
            "division": div,
            "totalUnits": total_units,
            "aur": overall_aur,
            "weeklyAur": weekly_aur,
            "netMargin": net_margin,
        })

    con.close()
    return {"skus": results}


# ── NEW: Sale Prices CRUD ────────────────────────────────────────────────

class SalePriceCreate(BaseModel):
    asin: str
    sku: str = ""
    product_name: str = ""
    regular_price: float
    sale_price: float
    start_date: str
    end_date: str
    marketplace: str = "US"


@router.get("/api/profitability/sale-prices")
def list_sale_prices(marketplace: Optional[str] = None):
    """List all sale prices."""
    con = get_db()
    where = ""
    params = []
    if marketplace:
        where = " WHERE marketplace = ?"
        params = [marketplace.upper()]
    rows = con.execute(f"""
        SELECT id, asin, sku, product_name, regular_price, sale_price,
               start_date, end_date, marketplace, status, pushed_to_amazon, pushed_at,
               created_at, updated_at
        FROM sale_prices{where}
        ORDER BY created_at DESC
    """, params).fetchall()
    con.close()
    today = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
    result = []
    for r in rows:
        start = str(r[6]) if r[6] else ""
        end = str(r[7]) if r[7] else ""
        # Auto-compute status from dates
        if end and end < today:
            status = "ended"
        elif start and start > today:
            status = "scheduled"
        elif start and start <= today and (not end or end >= today):
            status = "active"
        else:
            status = r[9] or "scheduled"
        discount_pct = round((1 - _n(r[5]) / _n(r[4])) * 100, 1) if _n(r[4]) > 0 and _n(r[5]) > 0 else 0
        result.append({
            "id": r[0], "asin": r[1], "sku": r[2] or "", "productName": r[3] or "",
            "regularPrice": _n(r[4]), "salePrice": _n(r[5]),
            "discountPct": discount_pct,
            "startDate": start, "endDate": end,
            "marketplace": r[8] or "US", "status": status,
            "pushedToAmazon": bool(r[10]), "pushedAt": str(r[11]) if r[11] else None,
        })
    return {"salePrices": result}


@router.post("/api/profitability/sale-prices")
def create_sale_price(data: SalePriceCreate):
    """Create a new sale price record."""
    con = get_db_rw()
    new_id = _next_id(con, "sale_prices", "sale_prices_seq")
    con.execute("""
        INSERT INTO sale_prices (id, asin, sku, product_name, regular_price, sale_price,
                                  start_date, end_date, marketplace, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'scheduled')
    """, [new_id, data.asin, data.sku, data.product_name, data.regular_price,
          data.sale_price, data.start_date, data.end_date, data.marketplace.upper()])
    con.commit()
    con.close()
    return {"ok": True, "id": new_id, "message": "Sale price created"}


@router.put("/api/profitability/sale-prices/{price_id}")
def update_sale_price(price_id: int, data: SalePriceCreate):
    """Update an existing sale price."""
    con = get_db_rw()
    con.execute("""
        UPDATE sale_prices
        SET regular_price = ?, sale_price = ?, start_date = ?, end_date = ?,
            marketplace = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, [data.regular_price, data.sale_price, data.start_date, data.end_date,
          data.marketplace.upper(), price_id])
    con.commit()
    con.close()
    return {"ok": True, "message": "Sale price updated"}


@router.delete("/api/profitability/sale-prices/{price_id}")
def delete_sale_price(price_id: int):
    """Delete a sale price record."""
    con = get_db_rw()
    con.execute("DELETE FROM sale_prices WHERE id = ?", [price_id])
    con.commit()
    con.close()
    return {"ok": True, "message": "Sale price deleted"}


# ── NEW: Coupons CRUD (multi-item support) ───────────────────────────────

class CouponItemInput(BaseModel):
    asin: str
    sku: str = ""
    product_name: str = ""


class CouponCreate(BaseModel):
    title: str = ""
    coupon_type: str = "percentage"  # "percentage" or "amount"
    discount_value: float = 0
    budget: float = 0
    start_date: str = ""
    end_date: str = ""
    marketplace: str = "US"
    items: list[CouponItemInput] = []


@router.get("/api/profitability/coupons")
def list_coupons(marketplace: Optional[str] = None):
    """List all coupons with their items."""
    con = get_db()
    where = ""
    params = []
    if marketplace:
        where = " WHERE marketplace = ?"
        params = [marketplace.upper()]
    rows = con.execute(f"""
        SELECT id, title, coupon_type, discount_value, budget, budget_used,
               start_date, end_date, marketplace, status, pushed_to_amazon, pushed_at,
               created_at
        FROM coupons{where}
        ORDER BY created_at DESC
    """, params).fetchall()

    today = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
    result = []
    for r in rows:
        coupon_id = r[0]
        start = str(r[6]) if r[6] else ""
        end = str(r[7]) if r[7] else ""
        if end and end < today:
            status = "ended"
        elif start and start > today:
            status = "scheduled"
        elif start and start <= today and (not end or end >= today):
            status = "active"
        else:
            status = r[9] or "scheduled"

        # Get items for this coupon
        item_rows = con.execute("""
            SELECT asin, sku, product_name FROM coupon_items WHERE coupon_id = ?
        """, [coupon_id]).fetchall()
        items = [{"asin": ir[0], "sku": ir[1] or "", "productName": ir[2] or ""} for ir in item_rows]

        budget = _n(r[4])
        used = _n(r[5])
        budget_pct = round(used / budget * 100, 1) if budget > 0 else 0

        result.append({
            "id": coupon_id, "title": r[1] or "",
            "couponType": r[2] or "percentage",
            "discountValue": _n(r[3]),
            "budget": budget, "budgetUsed": used, "budgetPct": budget_pct,
            "startDate": start, "endDate": end,
            "marketplace": r[8] or "US", "status": status,
            "pushedToAmazon": bool(r[10]),
            "pushedAt": str(r[11]) if r[11] else None,
            "items": items,
        })
    con.close()
    return {"coupons": result}


@router.post("/api/profitability/coupons")
def create_coupon(data: CouponCreate):
    """Create a new coupon with multiple items."""
    con = get_db_rw()
    new_id = _next_id(con, "coupons", "coupons_seq")
    con.execute("""
        INSERT INTO coupons (id, title, coupon_type, discount_value, budget,
                             start_date, end_date, marketplace, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'scheduled')
    """, [new_id, data.title, data.coupon_type, data.discount_value, data.budget,
          data.start_date, data.end_date, data.marketplace.upper()])

    # Insert coupon items
    for item in data.items:
        con.execute("""
            INSERT INTO coupon_items (coupon_id, asin, sku, product_name)
            VALUES (?, ?, ?, ?)
        """, [new_id, item.asin, item.sku, item.product_name])

    con.commit()
    con.close()
    return {"ok": True, "id": new_id, "message": "Coupon created"}


@router.put("/api/profitability/coupons/{coupon_id}")
def update_coupon(coupon_id: int, data: CouponCreate):
    """Update an existing coupon and its items."""
    con = get_db_rw()
    con.execute("""
        UPDATE coupons
        SET title = ?, coupon_type = ?, discount_value = ?, budget = ?,
            start_date = ?, end_date = ?, marketplace = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, [data.title, data.coupon_type, data.discount_value, data.budget,
          data.start_date, data.end_date, data.marketplace.upper(), coupon_id])

    # Replace items
    con.execute("DELETE FROM coupon_items WHERE coupon_id = ?", [coupon_id])
    for item in data.items:
        con.execute("""
            INSERT INTO coupon_items (coupon_id, asin, sku, product_name)
            VALUES (?, ?, ?, ?)
        """, [coupon_id, item.asin, item.sku, item.product_name])

    con.commit()
    con.close()
    return {"ok": True, "message": "Coupon updated"}


@router.delete("/api/profitability/coupons/{coupon_id}")
def delete_coupon(coupon_id: int):
    """Delete a coupon and its items."""
    con = get_db_rw()
    con.execute("DELETE FROM coupon_items WHERE coupon_id = ?", [coupon_id])
    con.execute("DELETE FROM coupons WHERE id = ?", [coupon_id])
    con.commit()
    con.close()
    return {"ok": True, "message": "Coupon deleted"}


# ── NEW: Push to Amazon ──────────────────────────────────────────────────

@router.post("/api/profitability/push-price/{price_id}")
def push_price_to_amazon(price_id: int):
    """Push a sale price to Amazon via SP-API Listings endpoint.

    This creates a price update via the Listings Items API (patchListingsItem)
    which sets the sale price, start date, and end date on Amazon.
    """
    con = get_db()
    row = con.execute("""
        SELECT asin, sku, sale_price, start_date, end_date, marketplace
        FROM sale_prices WHERE id = ?
    """, [price_id]).fetchone()
    con.close()

    if not row:
        return {"ok": False, "error": "Sale price not found"}

    asin, sku, sale_price, start_date, end_date, mp = row

    # Try to push via SP-API
    try:
        from services.sp_api import push_sale_price
        result = push_sale_price(
            sku=sku or asin,
            sale_price=_n(sale_price),
            start_date=str(start_date),
            end_date=str(end_date),
            marketplace=mp or "US",
        )
        # Mark as pushed
        con = get_db_rw()
        con.execute("""
            UPDATE sale_prices
            SET pushed_to_amazon = TRUE, pushed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, [price_id])
        con.commit()
        con.close()
        return {"ok": True, "message": "Sale price pushed to Amazon", "result": result}
    except ImportError:
        return {"ok": False, "error": "SP-API push_sale_price not implemented yet. Price saved locally."}
    except Exception as e:
        logger.error(f"Push sale price error: {e}")
        return {"ok": False, "error": str(e)}
