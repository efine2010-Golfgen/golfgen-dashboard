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

from core.config import DB_PATH, COGS_PATH, ITEM_MASTER_PATH, TIMEZONE, USE_POSTGRES
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


def load_item_master() -> dict:
    """Load amazon_item_master.json → {asin: {productName, sku, ...}, sku: {productName, asin, ...}}."""
    master = {}
    if not ITEM_MASTER_PATH.exists():
        return master
    try:
        with open(ITEM_MASTER_PATH, encoding="utf-8") as f:
            items = json.load(f)
        for item in items:
            asin = (item.get("asin") or "").strip()
            sku  = (item.get("sku")  or "").strip()
            name = (item.get("productName") or "").strip()
            if asin:
                master[asin] = {"productName": name, "sku": sku}
            if sku:
                master[sku] = {"productName": name, "asin": asin}
    except Exception:
        pass
    return master


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
    # financial_events has NO marketplace column — build separate filter
    ff, fp = hierarchy_filter(division, customer, platform, marketplace=None)
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
    item_master = load_item_master()
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
        # Priority: amazon_item_master.json productName → fba_inventory → cogs csv
        name = (item_master.get(asin, {}).get("productName", "")
                or item_master.get(sku, {}).get("productName", "")
                or inv.get("product_name", "")
                or cogs_data.get(asin, {}).get("product_name", ""))
        if units == 0:
            continue

        aur = rev / units if units else 0
        ci = cogs_data.get(asin) or cogs_data.get(sku or "") or {}
        cpu = ci.get("cogs", 0)
        if cpu == 0:
            cpu = round(aur * 0.35, 2)

        cogsTotal = units * cpu
        cogsPerUnit = cpu

        # Financial data — use ff/fp (no marketplace) since financial_events lacks that column
        fin_filter = "WHERE asin = ? AND date >= ?" + ff
        fin_params = [asin, cutoff] + fp
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
    # financial_events has NO marketplace column — build separate filter
    fin_sql, fin_params = hierarchy_filter(division, customer, platform, marketplace=None)

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
                "refund_amt": 0, "refund_count": 0,
                "storage": 0, "placement": 0, "atoz": 0, "chargeback": 0,
                "coupon_clip": 0, "removal": 0, "service_fee": 0,
                "safet_reimb": 0, "liquidation_proceeds": 0, "liquidation_fees": 0}
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
                       THEN 1 END),
                   SUM(CASE WHEN event_type = 'StorageFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'FeeAdjustment' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'AtoZ' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'Chargeback' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'CouponFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'RemovalFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'ServiceFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'SAFETReimbursement' THEN product_charges ELSE 0 END),
                   SUM(CASE WHEN event_type = 'FBALiquidation' THEN product_charges ELSE 0 END),
                   SUM(CASE WHEN event_type = 'FBALiquidation' THEN ABS(other_fees) ELSE 0 END)
            FROM financial_events
            WHERE date >= ? AND date < ?{fin_sql}
        """, [start, end] + fin_params).fetchone()
        if acct_row and acct_row[0] is not None:
            acct_fin = {
                "fba": _n(acct_row[0]), "comm": _n(acct_row[1]),
                "promo": _n(acct_row[2]), "shipping": _n(acct_row[3]),
                "other": _n(acct_row[4]), "refund_amt": _n(acct_row[5]),
                "refund_count": int(acct_row[6] or 0),
                "storage": _n(acct_row[7]), "placement": _n(acct_row[8]),
                "atoz": _n(acct_row[9]), "chargeback": _n(acct_row[10]),
                "coupon_clip": _n(acct_row[11]), "removal": _n(acct_row[12]),
                "service_fee": _n(acct_row[13]),
                "safet_reimb": _n(acct_row[14]),
                "liquidation_proceeds": _n(acct_row[15]),
                "liquidation_fees": _n(acct_row[16]),
            }
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
    total_storage = total_placement = total_atoz = total_chargeback = 0
    total_coupon_clip = total_removal = total_service_fee = total_safet_reimb = 0
    total_liquidation_proceeds = total_liquidation_fees = 0
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
                       THEN 1 END) AS refund_count,
                   SUM(CASE WHEN event_type = 'StorageFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'FeeAdjustment' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'AtoZ' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'Chargeback' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'CouponFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'RemovalFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'ServiceFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'SAFETReimbursement' THEN product_charges ELSE 0 END)
            FROM financial_events
            WHERE date >= ? AND date < ?{fin_sql}
            GROUP BY asin
        """, [start, end] + fin_params).fetchall()
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
                     "refund_amt": _n(r[6]), "refund_count": int(r[7] or 0),
                     "storage": _n(r[8]), "placement": _n(r[9]),
                     "atoz": _n(r[10]), "chargeback": _n(r[11]),
                     "coupon_clip": _n(r[12]), "removal": _n(r[13]),
                     "service_fee": _n(r[14]), "safet_reimb": _n(r[15])}
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
        total_storage += fin.get("storage", 0)
        total_placement += fin.get("placement", 0)
        total_atoz += fin.get("atoz", 0)
        total_chargeback += fin.get("chargeback", 0)
        total_coupon_clip += fin.get("coupon_clip", 0)
        total_removal += fin.get("removal", 0)
        total_service_fee += fin.get("service_fee", 0)
        total_safet_reimb += fin.get("safet_reimb", 0)

    # Use account-level totals as fallback
    if total_refunds == 0 and acct_fin["refund_amt"] > 0:
        total_refunds = acct_fin["refund_amt"]
        total_refund_units = acct_fin["refund_count"]
    # Always prefer account-level FBA and referral — they're the ground truth from financial_events
    # (per-ASIN resolution often misses events due to SKU→ASIN mapping gaps)
    if acct_fin["fba"] > 0:
        total_fba = acct_fin["fba"]
    if acct_fin["comm"] > 0:
        total_referral = acct_fin["comm"]
    if total_promo == 0 and acct_fin["promo"] > 0:
        total_promo = acct_fin["promo"]
    if total_shipping == 0 and acct_fin["shipping"] > 0:
        total_shipping = acct_fin["shipping"]
    # Account-level fallback for per-type fees (these don't need scaling — they're account-level)
    if total_storage == 0 and acct_fin["storage"] > 0:
        total_storage = acct_fin["storage"]
    if total_placement == 0 and acct_fin["placement"] > 0:
        total_placement = acct_fin["placement"]
    if total_atoz == 0 and acct_fin["atoz"] > 0:
        total_atoz = acct_fin["atoz"]
    if total_chargeback == 0 and acct_fin["chargeback"] > 0:
        total_chargeback = acct_fin["chargeback"]
    if total_coupon_clip == 0 and acct_fin["coupon_clip"] > 0:
        total_coupon_clip = acct_fin["coupon_clip"]
    if total_removal == 0 and acct_fin["removal"] > 0:
        total_removal = acct_fin["removal"]
    if total_service_fee == 0 and acct_fin["service_fee"] > 0:
        total_service_fee = acct_fin["service_fee"]
    if total_safet_reimb == 0 and acct_fin["safet_reimb"] > 0:
        total_safet_reimb = acct_fin["safet_reimb"]
    total_liquidation_proceeds = acct_fin["liquidation_proceeds"]
    total_liquidation_fees = acct_fin["liquidation_fees"]

    # Scale per-order fees to actual sales; account-level fees are already totals
    if prod_rev > 0 and total_cogs > 0:
        scale = sales / prod_rev if prod_rev > 0 else 1
        cogs = round(total_cogs * scale, 2)
        # FBA and referral are account-level totals — no scaling needed
        fba_fees = round(total_fba, 2)
        referral_fees = round(total_referral, 2)
        promo = round(total_promo * scale, 2)
        shipping = round(total_shipping * scale, 2)
        refunds = round(total_refunds * scale, 2)
        other_fees = round(total_other_fees * scale, 2)
    elif sales > 0:
        cogs = round(sales * 0.35, 2)
        # Use account-level if available, otherwise fall back to estimates
        fba_fees = round(total_fba, 2) if total_fba > 0 else round(sales * 0.12, 2)
        referral_fees = round(total_referral, 2) if total_referral > 0 else round(sales * 0.15, 2)
        promo = shipping = refunds = other_fees = 0
    else:
        cogs = fba_fees = referral_fees = promo = shipping = refunds = other_fees = 0

    storage_fees = round(total_storage, 2)
    placement_fees = round(total_placement, 2)
    atoz_fees = round(total_atoz, 2)
    chargeback_fees = round(total_chargeback, 2)
    coupon_clip_fees = round(total_coupon_clip, 2)
    removal_fees = round(total_removal, 2)
    service_fees = round(total_service_fee, 2)
    safet_reimbursements = round(total_safet_reimb, 2)
    liquidation_proceeds = round(total_liquidation_proceeds, 2)
    liquidation_fees = round(total_liquidation_fees, 2)

    amazon_fees = round(fba_fees + referral_fees + storage_fees + placement_fees +
                        atoz_fees + chargeback_fees + coupon_clip_fees +
                        removal_fees + service_fees + liquidation_fees -
                        safet_reimbursements, 2)
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
        "storageFees": storage_fees,
        "placementFees": placement_fees,
        "atozFees": atoz_fees,
        "chargebackFees": chargeback_fees,
        "couponClipFees": coupon_clip_fees,
        "removalFees": removal_fees,
        "serviceFees": service_fees,
        "safetReimbursements": safet_reimbursements,
        "liquidationProceeds": liquidation_proceeds,
        "liquidationFees": liquidation_fees,
        "otherFees": round(other_fees, 2),  # misc per-order fees from ShipmentEventList
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

    # financial_events has NO marketplace column — exclude it from filter
    ff, fp = hierarchy_filter(division, customer, platform, marketplace=None)
    fin_filter = "WHERE date >= ?" + ff
    fin_params = [cutoff] + fp
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
                       THEN 1 END) AS refund_count,
                   SUM(CASE WHEN event_type = 'StorageFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'FeeAdjustment' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'AtoZ' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'Chargeback' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'CouponFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'RemovalFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'ServiceFee' THEN ABS(other_fees) ELSE 0 END),
                   SUM(CASE WHEN event_type = 'SAFETReimbursement' THEN product_charges ELSE 0 END)
            FROM financial_events
            {fin_filter}
            GROUP BY asin
        """, fin_params).fetchall()
        # financial_events.asin contains SKUs (e.g. GG2SS2226BM), not ASINs.
        # Build SKU→ASIN map from fba_inventory to resolve to real ASINs.
        sku_to_asin = {}
        try:
            inv_map = con.execute(
                "SELECT sku, asin FROM fba_inventory WHERE sku IS NOT NULL AND asin IS NOT NULL"
            ).fetchall()
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
            entry = {"promo": _n(r[1]), "shipping": _n(r[2]), "other": _n(r[3]),
                     "refund_amt": _n(r[4]), "refund_count": int(r[5] or 0),
                     "storage": _n(r[6]), "placement": _n(r[7]),
                     "atoz": _n(r[8]), "chargeback": _n(r[9]),
                     "coupon_clip": _n(r[10]), "removal": _n(r[11]),
                     "service_fee": _n(r[12]), "safet_reimb": _n(r[13])}
            if resolved in fin_by_asin:
                for k in entry:
                    fin_by_asin[resolved][k] = fin_by_asin[resolved].get(k, 0) + entry[k]
            else:
                fin_by_asin[resolved] = entry
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
        # Per-type fee breakdown from financial_events
        item_storage = round(fin.get("storage", 0), 2)
        item_placement = round(fin.get("placement", 0), 2)
        item_atoz = round(fin.get("atoz", 0), 2)
        item_chargeback = round(fin.get("chargeback", 0), 2)
        item_coupon_clip = round(fin.get("coupon_clip", 0), 2)
        item_removal = round(fin.get("removal", 0), 2)
        item_service_fee = round(fin.get("service_fee", 0), 2)
        item_safet_reimb = round(fin.get("safet_reimb", 0), 2)
        item_misc_other = round(fin.get("other", 0), 2)  # misc per-order fees

        all_amazon_fees = round(
            amazon_fees + item_storage + item_placement + item_atoz + item_chargeback +
            item_coupon_clip + item_removal + item_service_fee - item_safet_reimb, 2
        )
        total_fee_pct = round(all_amazon_fees / rev * 100, 1) if rev > 0 else 0

        items.append({
            "asin": asin,
            "sku": p["sku"],
            "name": p["name"],
            "division": p.get("division", "unknown"),
            "units": p["units"],
            "rev": rev,
            "sales": rev,
            "promo": round(fin.get("promo", 0), 2),
            "adSpend": round(ad_spend, 2),
            "shipping": round(fin.get("shipping", 0), 2),
            "refunds": round(fin.get("refund_amt", 0), 2),
            "refundUnits": refund_units,
            "returnPct": return_pct,
            "amazonFees": all_amazon_fees,
            "fbaFees": round(p["fbaTotal"], 2),
            "referralFees": round(p["referralTotal"], 2),
            "storageFees": item_storage,
            "placementFees": item_placement,
            "atozFees": item_atoz,
            "chargebackFees": item_chargeback,
            "couponClipFees": item_coupon_clip,
            "removalFees": item_removal,
            "serviceFees": item_service_fee,
            "safetReimbursements": item_safet_reimb,
            "otherFees": item_misc_other,
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
    def _donut_entry(name, val):
        return {"name": name, "value": round(val, 2),
                "pct": round(val / total_fees * 100, 1) if total_fees > 0 else 0}

    fee_donut_raw = [
        _donut_entry("Referral", wf.get("referralFees", 0)),
        _donut_entry("FBA Fulfillment", wf.get("fbaFees", 0)),
        _donut_entry("Storage", wf.get("storageFees", 0)),
        _donut_entry("Placement", wf.get("placementFees", 0)),
        _donut_entry("A-to-Z", wf.get("atozFees", 0)),
        _donut_entry("Chargebacks", wf.get("chargebackFees", 0)),
        _donut_entry("Coupon Clip", wf.get("couponClipFees", 0)),
        _donut_entry("Removal", wf.get("removalFees", 0)),
        _donut_entry("Service Fees", wf.get("serviceFees", 0)),
        _donut_entry("Misc Other", wf.get("otherFees", 0)),
    ]
    fee_donut = [e for e in fee_donut_raw if e["value"] > 0]

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
    # financial_events has NO marketplace column — exclude it from filter
    hf, hp = hierarchy_filter(division, customer, platform, marketplace=None)
    # daily_sales may have marketplace — use full filter for it
    ds_hf, ds_hp = hierarchy_filter(division, customer, platform, marketplace)

    # Aggregate fee totals from financial_events
    fin_filter = f"WHERE date >= ?" + hf
    fin_params = [cutoff] + hp

    row = con.execute(f"""
        SELECT COALESCE(SUM(ABS(commission)), 0),
               COALESCE(SUM(ABS(fba_fees)), 0),
               COALESCE(SUM(CASE WHEN event_type NOT ILIKE '%refund%' AND event_type NOT ILIKE '%return%'
                             THEN product_charges ELSE 0 END), 0),
               COUNT(*),
               COALESCE(SUM(CASE WHEN event_type='StorageFee' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='FeeAdjustment' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='AtoZ' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='Chargeback' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='CouponFee' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='RemovalFee' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='ServiceFee' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='SAFETReimbursement' THEN product_charges ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='FBALiquidation' THEN product_charges ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='FBALiquidation' THEN ABS(other_fees) ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN event_type='Shipment' AND other_fees>0 THEN other_fees ELSE 0 END), 0)
        FROM financial_events
        {fin_filter}
    """, fin_params).fetchone()

    referral_total = _n(row[0])
    fba_total = _n(row[1])
    fin_revenue = _n(row[2])
    fin_row_count = int(row[3] or 0)
    storage_total = _n(row[4])
    placement_total = _n(row[5])
    atoz_total = _n(row[6])
    chargeback_total = _n(row[7])
    coupon_clip_total = _n(row[8])
    removal_total = _n(row[9])
    service_fee_total = _n(row[10])
    safet_reimb_total = _n(row[11])
    liquidation_proceeds = _n(row[12])
    liquidation_fees = _n(row[13])
    misc_other_total = _n(row[14])

    # Revenue from daily_sales (most current); fallback to financial_events
    ds_revenue = 0
    try:
        ds_row = con.execute(f"""
            SELECT COALESCE(SUM(ordered_product_sales), 0)
            FROM daily_sales
            WHERE date >= ? AND asin = 'ALL'{ds_hf}
        """, [cutoff] + ds_hp).fetchone()
        ds_revenue = _n(ds_row[0]) if ds_row else 0
    except Exception:
        pass
    revenue = ds_revenue if ds_revenue > 0 else fin_revenue

    # Fallback estimates only for the core per-order fees
    if referral_total == 0 and revenue > 0:
        referral_total = round(revenue * 0.15, 2)
    if fba_total == 0 and revenue > 0:
        fba_total = round(revenue * 0.12, 2)

    def _pct(amt):
        return round(amt / revenue * 100, 1) if revenue > 0 else 0

    total_fees = round(
        referral_total + fba_total + storage_total + placement_total +
        atoz_total + chargeback_total + coupon_clip_total +
        removal_total + service_fee_total + liquidation_fees +
        misc_other_total - safet_reimb_total, 2
    )

    fee_source = "financial_events" if fin_row_count > 0 else "estimated"

    # Build categories — only include non-zero ones
    categories = []

    def _cat(name, total, sub_items):
        if total <= 0:
            return
        categories.append({
            "name": name,
            "total": round(total, 2),
            "pct_of_rev": _pct(total),
            "items": [i for i in sub_items if abs(i["amount"]) > 0],
        })

    _cat("Referral Fees", referral_total, [
        {"name": "Referral Fee", "amount": round(referral_total, 2), "pct_of_rev": _pct(referral_total)},
    ])
    _cat("FBA Fulfillment", fba_total, [
        {"name": "FBA Fulfillment Fee", "amount": round(fba_total, 2), "pct_of_rev": _pct(fba_total)},
    ])
    _cat("Storage Fees", storage_total, [
        {"name": "Monthly / Renewal Storage", "amount": round(storage_total, 2), "pct_of_rev": _pct(storage_total)},
    ])
    _cat("Inbound Placement", placement_total, [
        {"name": "Inbound Placement Service Fee", "amount": round(placement_total, 2), "pct_of_rev": _pct(placement_total)},
    ])
    _cat("A-to-Z Claims", atoz_total, [
        {"name": "Guarantee Claim Paid", "amount": round(atoz_total, 2), "pct_of_rev": _pct(atoz_total)},
    ])
    _cat("Chargebacks", chargeback_total, [
        {"name": "Chargeback Debit", "amount": round(chargeback_total, 2), "pct_of_rev": _pct(chargeback_total)},
    ])
    _cat("Coupon Clip Fees", coupon_clip_total, [
        {"name": "Clip Fee ($0.60/redemption)", "amount": round(coupon_clip_total, 2), "pct_of_rev": _pct(coupon_clip_total)},
    ])
    _cat("Removal / Disposal", removal_total, [
        {"name": "Removal Order Fee", "amount": round(removal_total, 2), "pct_of_rev": _pct(removal_total)},
    ])
    _cat("Service Fees", service_fee_total, [
        {"name": "Amazon Service Fee", "amount": round(service_fee_total, 2), "pct_of_rev": _pct(service_fee_total)},
    ])
    if liquidation_proceeds > 0 or liquidation_fees > 0:
        net_liq = round(liquidation_proceeds - liquidation_fees, 2)
        categories.append({
            "name": "FBA Liquidation",
            "total": round(liquidation_fees, 2),
            "pct_of_rev": _pct(liquidation_fees),
            "items": [
                {"name": "Liquidation Proceeds", "amount": round(liquidation_proceeds, 2), "pct_of_rev": _pct(liquidation_proceeds)},
                {"name": "Liquidation Fee", "amount": -round(liquidation_fees, 2), "pct_of_rev": _pct(liquidation_fees)},
                {"name": "Net Liquidation", "amount": net_liq, "pct_of_rev": _pct(abs(net_liq))},
            ],
        })
    _cat("Misc Order Fees", misc_other_total, [
        {"name": "Other Per-Order Fees", "amount": round(misc_other_total, 2), "pct_of_rev": _pct(misc_other_total)},
    ])
    if safet_reimb_total > 0:
        categories.append({
            "name": "SAFET Reimbursements",
            "total": -round(safet_reimb_total, 2),
            "pct_of_rev": _pct(safet_reimb_total),
            "items": [{"name": "Amazon Reimbursement (credit)", "amount": round(safet_reimb_total, 2), "pct_of_rev": _pct(safet_reimb_total)}],
        })

    con.close()
    return {
        "days": days,
        "total_fees": total_fees,
        "revenue": round(revenue, 2),
        "categories": categories,
        "fee_source": fee_source,
        "fin_events_in_range": fin_row_count,
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


# ── Amazon Live Pricing + Coupons (read-only from SP-API cache) ──────────

@router.get("/api/profitability/amazon-pricing")
def get_amazon_pricing():
    """Return live Amazon pricing (list, buy box, landed) and active coupons
    from the hourly SP-API / Ads API sync cache.  Two sections:
      - amazonPricing: per-ASIN list of listPrice, buyBoxPrice, landedPrice, salePrice
      - amazonCoupons: per-coupon list with ASINs, discount, budget, state
    """
    pricing_cache = _load_pricing_cache()
    live_prices = pricing_cache.get("prices", {})
    live_coupons = pricing_cache.get("coupons", {})
    last_sync = pricing_cache.get("lastSync")

    # Enrich with item_master names
    con = get_db()
    try:
        im_rows = con.execute(
            "SELECT asin, sku, product_name, division FROM item_master"
        ).fetchall()
    except Exception:
        im_rows = []
    try:
        inv_rows = con.execute(
            "SELECT asin, sku, product_name FROM fba_inventory"
        ).fetchall()
    except Exception:
        inv_rows = []
    con.close()

    names = {}
    for r in im_rows:
        names[r[0]] = {"sku": r[1] or "", "name": r[2] or "", "division": r[3] or "unknown"}
    for r in inv_rows:
        if r[0] not in names:
            names[r[0]] = {"sku": r[1] or "", "name": r[2] or "", "division": "unknown"}

    # Build Amazon Pricing section
    pricing_rows = []
    for asin, data in live_prices.items():
        info = names.get(asin, {})
        listing = data.get("listingPrice")
        buy_box = data.get("buyBoxPrice")
        landed = data.get("landedPrice")
        regular = data.get("regularPrice")
        sale = data.get("salePrice")
        # Fall back: if API didn't return explicit sale price, infer from buy box < listing
        if sale is None and listing and buy_box and buy_box < listing:
            sale = buy_box
        # Use regular price as the "full" price if listing is not set
        display_list = listing or regular
        disc_pct = round((1 - buy_box / display_list) * 100, 1) if display_list and buy_box and display_list > 0 else 0
        pricing_rows.append({
            "asin": asin,
            "sku": info.get("sku", ""),
            "productName": info.get("name", ""),
            "division": info.get("division", "unknown"),
            "listPrice": _n(display_list),
            "regularPrice": _n(regular),
            "buyBoxPrice": _n(buy_box),
            "landedPrice": _n(landed),
            "salePrice": _n(sale),
            "discountPct": disc_pct,
            "fetchedAt": data.get("fetchedAt"),
        })
    pricing_rows.sort(key=lambda x: x["productName"] or x["asin"])

    # Build Amazon Coupons section — group by couponId
    seen_coupons = {}
    for asin, data in live_coupons.items():
        cid = data.get("couponId", asin)
        if cid not in seen_coupons:
            seen_coupons[cid] = {
                "couponId": cid,
                "state": data.get("state", "UNKNOWN"),
                "discountType": data.get("discountType", "%"),
                "discountValue": _n(data.get("discountValue", 0)),
                "budgetAmount": _n(data.get("budgetAmount", 0)),
                "budgetUsed": _n(data.get("budgetUsed", 0)),
                "redemptions": data.get("redemptions", 0),
                "startDate": data.get("startDate", ""),
                "endDate": data.get("endDate", ""),
                "asins": [],
            }
        info = names.get(asin, {})
        seen_coupons[cid]["asins"].append({
            "asin": asin,
            "sku": info.get("sku", ""),
            "productName": info.get("name", ""),
        })

    coupon_rows = sorted(seen_coupons.values(), key=lambda x: x.get("state", ""))

    return {
        "amazonPricing": pricing_rows,
        "amazonCoupons": coupon_rows,
        "lastSync": last_sync,
        "pricingCount": len(pricing_rows),
        "couponCount": len(coupon_rows),
    }


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
