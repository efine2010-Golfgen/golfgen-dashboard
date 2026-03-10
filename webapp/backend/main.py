"""
GolfGen Dashboard API — FastAPI backend serving Amazon SP-API data from DuckDB.
"""

import os
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import duckdb
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

# ── Paths ───────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # GolfGen Amazon Dashboard/
DB_DIR = Path(os.environ.get("DB_DIR", str(BASE_DIR / "data")))
DB_PATH = DB_DIR / "golfgen_amazon.duckdb"
COGS_PATH = DB_DIR / "cogs.csv"

app = FastAPI(title="GolfGen Dashboard API", version="1.0.0")

# Allow frontend (dev on :5173, prod wherever)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ─────────────────────────────────────────────

def get_db():
    """Return a read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


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


# ── API Routes ──────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "status": "ok",
        "db": str(DB_PATH),
        "db_exists": DB_PATH.exists(),
        "port": os.environ.get("PORT", "8000"),
    }


@app.get("/api/summary")
def summary(days: int = Query(365, description="Number of days to include")):
    """High-level KPIs: revenue, units, orders, sessions, AUR, conv rate."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

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


@app.get("/api/daily")
def daily_sales(days: int = Query(365), granularity: str = Query("daily")):
    """Time-series sales data for charts. Granularity: daily or weekly."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

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


@app.get("/api/products")
def products(days: int = Query(365)):
    """Product breakdown with profitability metrics."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    product_list = _build_product_list(con, cutoff)
    con.close()
    return {"days": days, "products": product_list}


@app.get("/api/inventory")
def inventory():
    """Current FBA inventory with days-of-supply calculations."""
    con = get_db()
    cogs_data = load_cogs()

    inv_rows = con.execute("""
        SELECT asin, sku, product_name,
               COALESCE(fulfillable_quantity, 0) AS fba_stock,
               COALESCE(inbound_working_quantity, 0) + COALESCE(inbound_shipped_quantity, 0) + COALESCE(inbound_receiving_quantity, 0) AS inbound,
               COALESCE(reserved_quantity, 0) AS reserved
        FROM fba_inventory
    """).fetchall()

    # Get avg daily units for last 30 days
    velocity = {}
    vel_rows = con.execute("""
        SELECT asin, SUM(units_ordered) AS units
        FROM daily_sales
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
          AND asin != 'ALL'
        GROUP BY asin
    """).fetchall()
    for vr in vel_rows:
        velocity[vr[0]] = round(vr[1] / 30, 1)

    con.close()

    items = []
    for r in inv_rows:
        asin = r[0]
        avg_daily = velocity.get(asin, 0)
        fba_stock = r[3]
        dos = round(fba_stock / avg_daily) if avg_daily > 0 else 999

        # Get name from COGS file first, then inventory table
        cogs_name = cogs_data.get(asin, {}).get("product_name", "")
        # Exclude names that are just the ASIN repeated
        if cogs_name and cogs_name.strip().upper() == asin.upper():
            cogs_name = ""
        name = (cogs_name or r[2] or asin)

        items.append({
            "asin": asin,
            "sku": r[1] or "",
            "name": name,
            "fbaStock": fba_stock,
            "inbound": r[4],
            "reserved": r[5],
            "avgDaily": avg_daily,
            "dos": dos,
        })

    items.sort(key=lambda x: x["fbaStock"], reverse=True)
    return {"items": items}


@app.get("/api/product/{asin}")
def product_detail(asin: str, days: int = Query(365)):
    """Detailed view for a single product including daily trend."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
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


@app.get("/api/pnl")
def pnl_waterfall(days: int = Query(365)):
    """Profit & Loss waterfall data, scaled to actual revenue from ALL rows."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

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


# ── Internal helpers ────────────────────────────────────

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
        WHERE asin != 'ALL'
        GROUP BY asin
        ORDER BY SUM(ordered_product_sales) DESC
    """).fetchall()

    # Financial data (actual fees if available)
    fin_rows = con.execute("""
        SELECT asin,
               SUM(ABS(fba_fees)) AS fba_fees,
               SUM(ABS(commission)) AS commission
        FROM financial_events
        GROUP BY asin
    """).fetchall()
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
        est_fba_per_unit = round(5.0 + aur * 0.03, 2)
        fba_total = actual_fba if actual_fba > 0 else round(units * est_fba_per_unit, 2)

        # Referral fees
        actual_commission = fin.get("commission", 0)
        referral_total = round(actual_commission, 2) if actual_commission > 0 else round(revenue * 0.15, 2)

        # Ad spend — pull from ads_product_performance if available
        ad_spend = 0
        try:
            ad_row = con.execute("""
                SELECT COALESCE(SUM(spend), 0)
                FROM ads_product_performance
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
    from collections import defaultdict
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
        sessions = w["sessions"] or 1
        result.append({
            "date": date_key,
            "revenue": round(w["revenue"], 2),
            "units": w["units"],
            "orders": w["orders"],
            "sessions": w["sessions"],
            "aur": round(w["revenue"] / units, 2),
            "convRate": round(w["orders"] / sessions * 100, 1),
        })
    return result


# ── Advertising API Routes ─────────────────────────────────

def _safe_ads_query(con, query, params=None):
    """Run an ads query, returning empty list if tables don't exist yet."""
    try:
        return con.execute(query, params or []).fetchall()
    except Exception:
        return []


@app.get("/api/ads/summary")
def ads_summary(days: int = Query(30)):
    """Ads KPIs: total spend, sales, ACOS, ROAS, TACOS, CPC, CTR."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    row = _safe_ads_query(con, """
        SELECT
            COALESCE(SUM(total_spend), 0) AS spend,
            COALESCE(SUM(total_sales), 0) AS ad_sales,
            COALESCE(SUM(total_impressions), 0) AS impressions,
            COALESCE(SUM(total_clicks), 0) AS clicks,
            COALESCE(SUM(total_orders), 0) AS orders,
            COALESCE(SUM(total_units), 0) AS units
        FROM ads_daily_summary
        WHERE date >= ?
    """, [cutoff])

    if not row or not row[0]:
        con.close()
        return {
            "days": days, "connected": False,
            "spend": 0, "adSales": 0, "impressions": 0, "clicks": 0,
            "orders": 0, "units": 0, "acos": 0, "roas": 0, "tacos": 0,
            "cpc": 0, "ctr": 0, "cvr": 0,
        }

    spend, ad_sales, impressions, clicks, orders, units = row[0]

    # Get total organic revenue for TACOS
    org_row = con.execute("""
        SELECT COALESCE(SUM(ordered_product_sales), 0)
        FROM daily_sales
        WHERE date >= ? AND asin = 'ALL'
    """, [cutoff]).fetchone()
    total_revenue = org_row[0] if org_row else 0

    con.close()

    acos = round(spend / ad_sales * 100, 2) if ad_sales > 0 else 0
    roas = round(ad_sales / spend, 2) if spend > 0 else 0
    tacos = round(spend / total_revenue * 100, 2) if total_revenue > 0 else 0
    cpc = round(spend / clicks, 2) if clicks > 0 else 0
    ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
    cvr = round(orders / clicks * 100, 2) if clicks > 0 else 0

    return {
        "days": days, "connected": True,
        "spend": round(spend, 2),
        "adSales": round(ad_sales, 2),
        "impressions": impressions,
        "clicks": clicks,
        "orders": orders,
        "units": units,
        "acos": acos,
        "roas": roas,
        "tacos": tacos,
        "cpc": cpc,
        "ctr": ctr,
        "cvr": cvr,
    }


@app.get("/api/ads/daily")
def ads_daily(days: int = Query(30)):
    """Daily ads performance time series."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT date, total_spend, total_sales, total_impressions,
               total_clicks, total_orders, acos, roas, tacos, avg_cpc, avg_ctr
        FROM ads_daily_summary
        WHERE date >= ?
        ORDER BY date
    """, [cutoff])

    con.close()

    data = []
    for r in rows:
        data.append({
            "date": fmt_date(r[0]),
            "spend": round(r[1], 2),
            "adSales": round(r[2], 2),
            "impressions": r[3],
            "clicks": r[4],
            "orders": r[5],
            "acos": r[6],
            "roas": r[7],
            "tacos": r[8],
            "cpc": r[9],
            "ctr": r[10],
        })

    return {"days": days, "data": data}


@app.get("/api/ads/campaigns")
def ads_campaigns(days: int = Query(30)):
    """Campaign-level performance."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            MAX(campaign_type) AS campaign_type,
            MAX(campaign_status) AS campaign_status,
            MAX(daily_budget) AS daily_budget,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_campaigns
        WHERE date >= ?
        GROUP BY campaign_id
        ORDER BY SUM(spend) DESC
    """, [cutoff])

    con.close()

    campaigns = []
    for r in rows:
        impressions, clicks, spend, sales = r[5], r[6], r[7], r[8]
        orders, units = r[9], r[10]

        campaigns.append({
            "campaignId": r[0],
            "campaignName": r[1],
            "campaignType": r[2],
            "status": r[3],
            "dailyBudget": round(r[4] or 0, 2),
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
            "cvr": round(orders / clicks * 100, 2) if clicks > 0 else 0,
        })

    return {"days": days, "campaigns": campaigns}


@app.get("/api/ads/keywords")
def ads_keywords(days: int = Query(30), sort: str = Query("spend"), limit: int = Query(50)):
    """Top keywords by spend, sales, or ACOS."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT
            keyword_text,
            MAX(match_type) AS match_type,
            MAX(campaign_name) AS campaign_name,
            MAX(ad_group_name) AS ad_group_name,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_keywords
        WHERE date >= ?
        GROUP BY keyword_text
        ORDER BY SUM(spend) DESC
        LIMIT ?
    """, [cutoff, limit])

    con.close()

    keywords = []
    for r in rows:
        impressions, clicks, spend, sales = r[4], r[5], r[6], r[7]
        orders, units = r[8], r[9]

        keywords.append({
            "keyword": r[0],
            "matchType": r[1],
            "campaignName": r[2],
            "adGroupName": r[3],
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
            "cvr": round(orders / clicks * 100, 2) if clicks > 0 else 0,
        })

    return {"days": days, "keywords": keywords}


@app.get("/api/ads/search-terms")
def ads_search_terms(days: int = Query(30), limit: int = Query(50)):
    """Top search terms by spend."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    rows = _safe_ads_query(con, """
        SELECT
            search_term,
            MAX(keyword_text) AS keyword,
            MAX(match_type) AS match_type,
            MAX(campaign_name) AS campaign_name,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_search_terms
        WHERE date >= ?
        GROUP BY search_term
        ORDER BY SUM(spend) DESC
        LIMIT ?
    """, [cutoff, limit])

    con.close()

    terms = []
    for r in rows:
        impressions, clicks, spend, sales = r[4], r[5], r[6], r[7]
        orders, units = r[8], r[9]

        terms.append({
            "searchTerm": r[0],
            "keyword": r[1],
            "matchType": r[2],
            "campaignName": r[3],
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
        })

    return {"days": days, "searchTerms": terms}


@app.get("/api/ads/negative-keywords")
def ads_negative_keywords():
    """List all negative keywords."""
    con = get_db()

    rows = _safe_ads_query(con, """
        SELECT keyword_text, match_type, campaign_name, ad_group_name, keyword_status
        FROM ads_negative_keywords
        ORDER BY campaign_name, keyword_text
    """)

    con.close()

    keywords = [
        {
            "keyword": r[0],
            "matchType": r[1],
            "campaignName": r[2],
            "adGroupName": r[3],
            "status": r[4],
        }
        for r in rows
    ]

    return {"negativeKeywords": keywords}


# ── Period Comparison & Analytics ──────────────────────────

@app.get("/api/comparison")
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

    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
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
        week_start = today_start - timedelta(days=today_start.weekday())
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

    # Compute a single margin rate from full product list (to avoid calling _build_product_list per period)
    full_products = _build_product_list(con, f"{yr-1}-01-01")
    full_prod_rev = sum(pp["rev"] for pp in full_products)
    full_prod_net = sum(pp["net"] for pp in full_products)
    margin_pct = full_prod_net / full_prod_rev if full_prod_rev > 0 else 0

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
                SELECT COALESCE(SUM(total_spend), 0)
                FROM ads_daily_summary
                WHERE date >= ? AND date < ?
            """, [p["start"], p["end"]]).fetchone()
            ad_spend = round(ad_row[0], 2) if ad_row else 0
        except Exception:
            pass

        tacos = round(ad_spend / rev * 100, 1) if rev > 0 else 0
        net = round(rev * margin_pct, 2)
        margin_val = round(margin_pct * 100)

        # Compute last-year equivalents
        try:
            ly_start = datetime.strptime(p["start"], "%Y-%m-%d").replace(year=datetime.strptime(p["start"], "%Y-%m-%d").year - 1)
            ly_end = datetime.strptime(p["end"], "%Y-%m-%d").replace(year=datetime.strptime(p["end"], "%Y-%m-%d").year - 1)
        except ValueError:
            # Feb 29 edge case
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
                SELECT COALESCE(SUM(total_spend), 0)
                FROM ads_daily_summary
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


@app.get("/api/monthly-yoy")
def monthly_yoy():
    """Monthly revenue broken down by year for YoY comparison."""
    con = get_db()

    rows = con.execute("""
        SELECT
            EXTRACT(YEAR FROM date) AS yr,
            EXTRACT(MONTH FROM date) AS mo,
            COALESCE(SUM(ordered_product_sales), 0) AS revenue
        FROM daily_sales
        WHERE asin = 'ALL'
        GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
        ORDER BY yr, mo
    """).fetchall()

    con.close()

    months_map = {}
    years = set()
    for r in rows:
        yr, mo, rev = int(r[0]), int(r[1]), round(r[2], 2)
        years.add(yr)
        if mo not in months_map:
            months_map[mo] = {}
        months_map[mo][yr] = rev

    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    data = []
    for mo in range(1, 13):
        entry = {"month": month_names[mo - 1]}
        for yr in sorted(years):
            entry[str(yr)] = months_map.get(mo, {}).get(yr, 0)
        data.append(entry)

    return {"years": sorted(years), "data": data}


@app.get("/api/product-mix")
def product_mix(days: int = Query(365)):
    """Top 10 products by revenue for donut chart."""
    con = get_db()
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    products = _build_product_list(con, cutoff)
    con.close()

    products.sort(key=lambda p: p["rev"], reverse=True)
    top10 = products[:10]
    other_rev = sum(p["rev"] for p in products[10:])

    result = [{"name": p["name"][:30], "value": p["rev"]} for p in top10]
    if other_rev > 0:
        result.append({"name": "Other", "value": round(other_rev, 2)})

    return {"products": result}


# ── Static Frontend ────────────────────────────────────────
# Serve the built React frontend from the same server.
# The frontend dist/ folder should be at webapp/frontend/dist/
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if FRONTEND_DIR.exists():
    # Mount static assets (JS, CSS, images) — must come AFTER api routes
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR / "assets")), name="static-assets")

    # Catch-all for SPA: serve index.html for any non-API, non-asset route
    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        # If a real file exists (like vite.svg), serve it
        file_path = FRONTEND_DIR / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        # Otherwise serve index.html (SPA routing)
        return FileResponse(str(FRONTEND_DIR / "index.html"))


# ── Run ─────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
