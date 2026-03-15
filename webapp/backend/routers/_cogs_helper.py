"""Unified COGS helper — reads unit_cost from item_master.csv, falls back to cogs.csv.

Used by sales.py and any other router that needs COGS per ASIN.
"""
import csv
import logging
from pathlib import Path

from core.config import DB_DIR, COGS_PATH

logger = logging.getLogger("golfgen")

ITEM_MASTER_PATH = DB_DIR / "item_master.csv"


def load_unit_costs() -> dict:
    """Load per-ASIN unit cost.

    Priority:
    1. item_master.csv → unit_cost column (primary, editable via Item Master page)
    2. cogs.csv → cogs column (legacy fallback)

    Returns {asin: float} where float is the COGS per unit.
    """
    costs = {}

    # ── Legacy cogs.csv (loaded first, item_master overwrites) ──
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

    # ── item_master.csv (overwrites cogs.csv values) ──
    if ITEM_MASTER_PATH.exists():
        try:
            with open(ITEM_MASTER_PATH, newline="", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    asin = (row.get("asin") or "").strip()
                    if not asin:
                        continue
                    try:
                        val = float(row.get("unit_cost") or 0)
                    except (ValueError, TypeError):
                        val = 0
                    if val > 0:
                        costs[asin] = val
        except Exception as e:
            logger.warning(f"load_unit_costs: item_master.csv error: {e}")

    return costs


def compute_cogs_for_range(con, sd, ed, hw: str = "", hp: list = None,
                           fallback_pct: float = 0.35) -> float:
    """Compute total COGS for a date range by summing (units × unit_cost) per ASIN.

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

    # Get per-ASIN units and revenue for the date range
    try:
        rows = con.execute(f"""
            SELECT asin,
                   COALESCE(SUM(units_ordered), 0) AS units,
                   COALESCE(SUM(ordered_product_sales), 0) AS revenue
            FROM daily_sales
            WHERE asin != 'ALL' AND date >= ? AND date <= ? {hw}
            GROUP BY asin
        """, [str(sd), str(ed)] + hp).fetchall()
    except Exception as e:
        logger.error(f"compute_cogs_for_range query error: {e}")
        return 0

    total_cogs = 0
    for r in rows:
        asin = r[0]
        units = int(r[1] or 0)
        revenue = float(r[2] or 0)

        cost = unit_costs.get(asin, 0)
        if cost > 0:
            total_cogs += units * cost
        elif revenue > 0:
            # Fallback: estimate COGS as % of revenue
            total_cogs += revenue * fallback_pct

    return round(total_cogs, 2)
