"""Sales analytics routes."""
import csv
import json
import logging
import re
import duckdb
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query

from core.config import DB_PATH, DB_DIR, COGS_PATH

logger = logging.getLogger("golfgen")
router = APIRouter()


# ── Helper Functions ────────────────────────────────────
def get_db():
    """Return a read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


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
    """Return today's actual calendar date in US/Central timezone.

    Railway servers run in UTC. The business operates in Central time,
    so we convert UTC to Central (America/Chicago) to get the real calendar date.
    'Today' always means today — if there's no data yet it shows $0,
    which is correct until Orders API sync populates it.
    """
    now_utc = datetime.now(ZoneInfo("UTC"))
    now_central = now_utc.astimezone(ZoneInfo("America/Chicago"))
    return now_central.replace(hour=0, minute=0, second=0, microsecond=0)


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
        WHERE asin != 'ALL' AND date >= ?
        GROUP BY asin
        ORDER BY SUM(ordered_product_sales) DESC
    """, [cutoff]).fetchall()

    # Financial data (actual fees if available)
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


# ── API Routes ──────────────────────────────────────────


@router.get("/api/summary")
def summary(days: int = Query(365, description="Number of days to include")):
    """High-level KPIs: revenue, units, orders, sessions, AUR, conv rate."""
    con = get_db()
    cutoff = (get_today(con) - timedelta(days=days)).strftime("%Y-%m-%d") if days > 0 else get_today(con).strftime("%Y-%m-%d")

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


@router.get("/api/daily")
def get_daily_sales(days: int = Query(365), granularity: str = Query("daily")):
    """Time-series sales data for charts. Granularity: daily or weekly."""
    con = get_db()
    cutoff = (get_today(con) - timedelta(days=days)).strftime("%Y-%m-%d")

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


@router.get("/api/products")
def products(days: int = Query(365)):
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


@router.get("/api/comparison")
def period_comparison(view: str = Query("realtime")):
    """Return KPI comparison across multiple time periods.

    Views:
    - realtime: Today / WTD / MTD / YTD
    - weekly: Last Week / 4 Weeks / 13 Weeks / 26 Weeks
    - monthly: Last Month / 2 Mo Ago / 3 Mo Ago / Last 12 Mo
    - yearly: 2026 YTD / 2025 YTD (comp) / 2025 Full / 2024 Full
    - monthly2026: Jan / Feb / ... / current month / YTD total
    """
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


@router.get("/api/monthly-yoy")
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

    # Always show 3 years for comparison
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
def product_mix(days: int = Query(365)):
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
def color_mix(days: int = Query(365)):
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
