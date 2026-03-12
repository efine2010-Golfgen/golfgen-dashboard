"""Profitability and P&L routes."""
import csv
import logging
import duckdb
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from fastapi import APIRouter, Query

from core.config import DB_PATH, COGS_PATH, TIMEZONE

logger = logging.getLogger("golfgen")
router = APIRouter()


def get_db():
    """Get DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=False)


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


def _build_product_list(con, cutoff: str) -> list:
    """Build per-ASIN product list with revenue, units, COGS, fees, and ad spend."""
    asin_rows = con.execute("""
        SELECT asin,
               COALESCE(SUM(ordered_product_sales), 0) AS revenue,
               COALESCE(SUM(units_ordered), 0) AS units
        FROM daily_sales
        WHERE date >= ? AND asin <> 'ALL'
        GROUP BY asin
        ORDER BY revenue DESC
    """, [cutoff]).fetchall()

    cogs_data = load_cogs()
    # Get names/SKUs from inventory table
    inv_names = {}
    try:
        inv_rows = con.execute("SELECT asin, sku, product_name FROM fba_inventory").fetchall()
        for ir in inv_rows:
            inv_names[ir[0]] = {"sku": ir[1] or "", "product_name": ir[2] or ""}
    except Exception:
        pass
    products = []

    for r in asin_rows:
        asin, rev, units = r[0], r[1], r[2]
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
        fin_row = con.execute("""
            SELECT COALESCE(SUM(ABS(fba_fees)), 0),
                   COALESCE(SUM(ABS(commission)), 0)
            FROM financial_events
            WHERE asin = ? AND date >= ?
        """, [asin, cutoff]).fetchone()
        fba_actual, comm_actual = fin_row if fin_row else (0, 0)

        fbaTotal = fba_actual if fba_actual > 0 else round(rev * 0.12, 2)
        referralTotal = comm_actual if comm_actual > 0 else round(rev * 0.15, 2)

        # Ad spend (advertising table may not have asin column)
        adSpend = 0
        try:
            ad_row = con.execute("""
                SELECT COALESCE(SUM(spend), 0)
                FROM advertising
                WHERE asin = ? AND date >= ?
            """, [asin, cutoff]).fetchone()
            adSpend = ad_row[0] if ad_row else 0
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
        })

    return products


def _load_pricing_cache() -> dict:
    """Load pricing sync cache from JSON."""
    from core.config import PRICING_CACHE_PATH
    import json
    cache = {}
    if PRICING_CACHE_PATH.exists():
        try:
            with open(PRICING_CACHE_PATH) as f:
                cache = json.load(f)
        except Exception:
            pass
    return cache


def load_item_master() -> list:
    """Load item master from CSV."""
    from core.config import DB_DIR
    ITEM_MASTER_PATH = DB_DIR / "item_master.csv"
    items = []
    if not ITEM_MASTER_PATH.exists():
        return items
    with open(ITEM_MASTER_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                items.append({
                    "asin": (row.get("asin") or "").strip(),
                    "sku": (row.get("sku") or "").strip(),
                    "product_name": (row.get("product_name") or "").strip(),
                    "listPrice": float(row.get("listPrice") or 0),
                    "salePrice": float(row.get("salePrice") or 0),
                })
            except (ValueError, KeyError):
                pass
    return items


def _ensure_today_data():
    """Ensure today's data exists in database."""
    con = get_db()
    try:
        today = datetime.now(TIMEZONE).strftime("%Y-%m-%d")
        result = con.execute("""
            SELECT COUNT(*) FROM daily_sales WHERE date = ? AND asin = 'ALL'
        """, [today]).fetchone()
        if result and result[0] == 0:
            # Insert today's row if missing
            con.execute("""
                INSERT INTO daily_sales (date, asin, ordered_product_sales, units_ordered)
                VALUES (?, 'ALL', 0, 0)
            """, [today])
            con.commit()
    except Exception:
        pass
    finally:
        con.close()


@router.get("/api/pnl")
def pnl_waterfall(days: int = Query(365)):
    """Profit & Loss waterfall data, scaled to actual revenue from ALL rows."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")

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


@router.get("/api/profitability")
def profitability(view: str = Query("realtime")):
    """Sellerboard-style profit waterfall with same period views as comparison.

    Waterfall: Sales - Promo - Ads - Shipping - Refunds - Amazon Fees - COGS - Indirect = Net Profit
    Returns both account-level waterfall and per-ASIN item breakdown.
    """
    # Ensure today has live data before building the response
    _ensure_today_data()

    con = get_db()
    cogs_data = load_cogs()
    # Get names/SKUs from inventory table
    inv_names = {}
    try:
        inv_rows = con.execute("SELECT asin, sku, product_name FROM fba_inventory").fetchall()
        for ir in inv_rows:
            inv_names[ir[0]] = {"sku": ir[1] or "", "product_name": ir[2] or ""}
    except Exception:
        pass

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


@router.get("/api/profitability/items")
def profitability_items(days: int = Query(365)):
    """Per-ASIN profitability breakdown matching Sellerboard item view."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
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

    # Load live pricing & coupon cache for enrichment
    pricing_cache = _load_pricing_cache()
    live_prices = pricing_cache.get("prices", {})
    live_coupons = pricing_cache.get("coupons", {})

    # Also load item master for static sale_price / list_price
    master_items = load_item_master()
    master_by_asin = {m["asin"]: m for m in master_items}

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

        # Coupon data from pricing_sync.json cache
        coupon = live_coupons.get(asin)
        coupon_status = None
        coupon_end_date = None
        coupon_discount = None
        coupon_type = None
        if coupon:
            raw_state = coupon.get("state")
            # Map Amazon Ads coupon states to display labels
            state_map = {"ENABLED": "ACTIVE", "SCHEDULED": "SCHEDULED",
                         "PAUSED": "PAUSED", "EXPIRED": "EXPIRED"}
            coupon_status = state_map.get(raw_state, raw_state)
            coupon_end_date = coupon.get("endDate")
            coupon_discount = coupon.get("discountValue")
            coupon_type = coupon.get("discountType")

        # Live pricing data from pricing_sync.json cache
        live = live_prices.get(asin)
        sale_price = None
        sale_price_end_date = None
        list_price = None
        if live:
            list_price = live.get("listingPrice")
            # If listing price differs from buy box, the lower is likely a sale
            buy_box = live.get("buyBoxPrice")
            if list_price and buy_box and buy_box < list_price:
                sale_price = buy_box
        # Fall back to static item master sale_price if no live data
        master = master_by_asin.get(asin, {})
        if sale_price is None and master.get("salePrice", 0) > 0:
            sale_price = master["salePrice"]
        if list_price is None and master.get("listPrice", 0) > 0:
            list_price = master["listPrice"]
        # sale_price_end_date: not available from getCompetitivePricing API
        # Would require SP-API getListingOffers for sale schedule data

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
            # New coupon/pricing fields
            "couponStatus": coupon_status,
            "couponEndDate": coupon_end_date,
            "couponDiscount": coupon_discount,
            "couponType": coupon_type,
            "salePrice": sale_price,
            "listPrice": list_price,
            "salePriceEndDate": sale_price_end_date,
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
        SELECT asin,
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
        asin, rev, u = ar[0], ar[1], ar[2]
        sku = ""
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
