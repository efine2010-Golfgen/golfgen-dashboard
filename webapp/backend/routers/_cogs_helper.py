"""Unified COGS helper — reads unit_cost from item_master.csv, falls back to cogs.csv.

Used by sales.py and any other router that needs COGS per ASIN.
"""
import csv
import logging
from pathlib import Path

from core.config import DB_DIR, COGS_PATH

logger = logging.getLogger("golfgen")

ITEM_MASTER_PATH = DB_DIR / "item_master.csv"


def load_item_master_names() -> dict:
    """Load clean product names from Item Master CSV (user-edited, highest priority).

    Returns {asin: product_name} for all ASINs that have a non-empty name
    that isn't just the ASIN itself.
    """
    names = {}
    if not ITEM_MASTER_PATH.exists():
        return names
    try:
        with open(ITEM_MASTER_PATH, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                asin = (row.get("asin") or "").strip()
                name = (row.get("product_name") or "").strip()
                if asin and name and name.upper() != asin.upper():
                    names[asin] = name
    except Exception as e:
        logger.warning(f"load_item_master_names: CSV read error: {e}")
    return names


def load_unit_costs() -> dict:
    """Load per-ASIN unit cost.

    Priority:
    1. item_master DB table → unit_cost column (primary, auto-populated from cogs.csv)
    2. cogs.csv → cogs column (legacy fallback if DB has no costs)

    Returns {asin: float} where float is the COGS per unit.
    """
    costs = {}

    # ── Source 1: item_master database table (preferred) ──
    try:
        from core.database import get_db
        con = get_db()
        rows = con.execute("SELECT asin, unit_cost FROM item_master WHERE unit_cost > 0").fetchall()
        for r in rows:
            asin = (r[0] or "").strip()
            if asin:
                costs[asin] = float(r[1])
        con.close()
        if costs:
            return costs  # DB had data, use it
    except Exception as e:
        logger.warning(f"load_unit_costs: DB query error (falling back to CSV): {e}")

    # ── Fallback: Legacy cogs.csv ──
    if COGS_PATH.exists():
        try:
            with open(COGS_PATH, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    asin = (row.get("asin") or "").strip()
                    if not asin:
                        continue
                    try:
                        val = float(row.get("cogs") or 0)
                    except (ValueError, TypeError):
                        val = 0
                    if val > 0:
                        costs[asin] = val
        except Exception as e:
            logger.warning(f"load_unit_costs: cogs.csv error: {e}")

    return costs


def compute_cogs_for_range(con, sd, ed, hw: str = "", hp: list = None,
                           fallback_pct: float = 0.35) -> float:
    """Compute total COGS for a date range by summing (units × unit_cost) per ASIN.

    Strategy (tries three sources in order):
    1. daily_sales per-ASIN rows — best granularity (but only ~10 days of data)
    2. orders table — has per-ASIN data for recent orders
    3. daily_sales asin='ALL' total revenue × fallback_pct — last resort

    Parameters:
        con: database connection (from get_db())
        sd, ed: start/end dates (date objects or strings)
        hw: hierarchy WHERE clause fragment (starts with " AND ..." or "")
        hp: hierarchy params list
        fallback_pct: fraction of revenue to use when no per-ASIN cost found (default 35%)

    Returns total COGS as float.
    """
    if hp is None:
        hp = []

    unit_costs = load_unit_costs()

    # ── Source 1: daily_sales per-ASIN rows ──
    total_cogs = 0
    has_data = False
    try:
        rows = con.execute(f"""
            SELECT asin,
                   COALESCE(SUM(units_ordered), 0) AS units,
                   COALESCE(SUM(ordered_product_sales), 0) AS revenue
            FROM daily_sales
            WHERE asin != 'ALL' AND date >= ? AND date <= ? {hw}
            GROUP BY asin
        """, [str(sd), str(ed)] + hp).fetchall()
        if rows and len(rows) > 0:
            for r in rows:
                asin = r[0]
                units = int(r[1] or 0)
                revenue = float(r[2] or 0)
                if units == 0 and revenue == 0:
                    continue
                has_data = True
                cost = unit_costs.get(asin, 0)
                if cost > 0:
                    total_cogs += units * cost
                elif revenue > 0:
                    total_cogs += revenue * fallback_pct
        logger.info(f"compute_cogs Source1: sd={sd} ed={ed} rows={len(rows) if rows else 0} has_data={has_data} cogs={total_cogs}")
    except Exception as e:
        logger.warning(f"compute_cogs: daily_sales per-ASIN query error: {e}")

    if has_data and total_cogs > 0:
        # ── Coverage check: per-ASIN rows may only cover part of the date range ──
        # (S&T report only keeps ~10 days of per-ASIN data)
        # Compare per-ASIN revenue to ALL-aggregate revenue for the full range.
        # If per-ASIN covers <90%, scale COGS proportionally.
        try:
            per_asin_rev = sum(float(r[2] or 0) for r in rows if float(r[2] or 0) > 0)
            all_row = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
            """, [str(sd), str(ed)] + hp).fetchone()
            total_rev = float(all_row[0]) if all_row else 0
            if total_rev > 0 and per_asin_rev > 0 and per_asin_rev < total_rev * 0.90:
                scale = total_rev / per_asin_rev
                scaled_cogs = total_cogs * scale
                logger.info(f"compute_cogs coverage: per_asin_rev={per_asin_rev:.2f} total_rev={total_rev:.2f} scale={scale:.2f} scaled_cogs={scaled_cogs:.2f}")
                return round(scaled_cogs, 2)
        except Exception as cov_err:
            logger.warning(f"compute_cogs coverage check error: {cov_err}")
        return round(total_cogs, 2)

    # ── Source 2: (removed — orders table has no asin column) ──

    # ── Source 3: daily_sales ALL-aggregate revenue × fallback % ──
    try:
        all_row = con.execute(f"""
            SELECT COALESCE(SUM(ordered_product_sales), 0)
            FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ? {hw}
        """, [str(sd), str(ed)] + hp).fetchone()
        total_rev = float(all_row[0]) if all_row else 0
        if total_rev > 0:
            return round(total_rev * fallback_pct, 2)
    except Exception as e:
        logger.warning(f"compute_cogs: ALL-aggregate fallback error: {e}")

    return 0
