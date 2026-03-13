"""Item Planning routes."""
import csv
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import JSONResponse

from core.config import DB_PATH, DB_DIR, TIMEZONE
from core.database import get_db, get_db_rw

logger = logging.getLogger("golfgen")
router = APIRouter()

# ── Database & Auth Helpers ────────────────────────

def _duck_rw():
    """Return a read-write connection for item plan operations."""
    return get_db_rw()


def _duck():
    """Return a connection for item plan queries."""
    return get_db()


def _require_auth(request: Request):
    """Validate session cookie or raise 401."""
    token = request.cookies.get("golfgen_session")
    sess = _get_session(token)
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return sess


def _get_session(token: str):
    """Look up session from DuckDB. Returns dict or None."""
    if not token:
        return None
    con = _duck()
    try:
        rows = con.execute("SELECT token, user_email, user_name, role FROM sessions WHERE token = ?", [token]).fetchall()
        if rows:
            return {"token": rows[0][0], "user_email": rows[0][1], "user_name": rows[0][2], "role": rows[0][3]}
    finally:
        con.close()
    return None


# ── Item Master Loading (from main.py) ────────────────────────

ITEM_MASTER_PATH = DB_DIR / "item_master.csv"


def load_item_master() -> list:
    """Load item master from CSV. Returns list of dicts."""
    items = []
    if not ITEM_MASTER_PATH.exists():
        return items
    with open(ITEM_MASTER_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            asin = (row.get("asin") or "").strip()
            if not asin:
                continue
            items.append({
                "asin": asin,
                "sku": (row.get("sku") or "").strip(),
                "productName": (row.get("product_name") or "").strip(),
                "color": (row.get("color") or "").strip(),
                "brand": (row.get("brand") or "").strip(),
                "series": (row.get("series") or "").strip(),
                "productType": (row.get("product_type") or "").strip(),
                "pieceCount": int(float(row.get("piece_count") or 0)),
                "division": (row.get("division") or "").strip(),
                "customer": (row.get("customer") or "").strip(),
            })
    return items


# ── Constants ────────────────────────

_MONTH_KEYS = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "jan_next"]

# DB field name → frontend override field name
_OVERRIDE_DB_TO_FE = {
    "plan_units": "ty_override_units",
    "aur": "ty_aur_override",
    "refund_units": "ty_override_refund_units",
    "refund_rate": "ty_refund_rate_override",
    "shipment": "shipment_override",
}


# ── Helper Functions ────────────────────────

def _db_month_to_key(month_num: int) -> str:
    """Convert DB month number (1-13) to frontend month key."""
    if 1 <= month_num <= 13:
        return _MONTH_KEYS[month_num - 1]
    return "jan"


def _array_to_month_obj(arr: list) -> dict:
    """Convert 12-element array (FY order: Feb..Jan) to month-keyed object."""
    # arr[0]=Feb, arr[1]=Mar, ..., arr[10]=Dec, arr[11]=Jan(next)
    # But we also need jan (pre-FY) — not in LY arrays, default 0
    obj = {"jan": 0}
    fy_keys = ["feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "jan_next"]
    for i, k in enumerate(fy_keys):
        obj[k] = float(arr[i]) if i < len(arr) else 0
    return obj


def _compute_master_curve() -> dict:
    """Compute master sales curve as percentage distribution across 13 months.
    Returns a dict with month keys: {jan: 0.05, feb: 0.03, ...}"""
    con = _duck()
    try:
        # Get total units per (year, month) across all SKUs for LY
        try:
            result = con.execute("""
                SELECT year, month, SUM(units) as total_units
                FROM monthly_sales_history
                WHERE year IN (2025, 2026)
                GROUP BY year, month
                ORDER BY year, month
            """).fetchall()
        except Exception as e:
            logger.warning(f"Master curve: monthly_sales_history query failed: {e}")
            result = []

        # Calculate total annual
        annual_total = sum(row[2] for row in result)

        curve = {k: 0 for k in _MONTH_KEYS}
        if annual_total == 0:
            return curve

        for year, month, total_units in result:
            # Map (year, month) to FY month key
            if year == 2025 and 2 <= month <= 12:
                key = _MONTH_KEYS[month - 1]  # feb=idx1, mar=idx2, ..., dec=idx11
            elif year == 2026 and month == 1:
                key = "jan_next"  # idx 12
            elif year == 2025 and month == 1:
                key = "jan"  # pre-FY
            else:
                continue
            curve[key] = round(float(total_units) / annual_total, 4)

        return curve
    finally:
        con.close()


def _month_key_to_db(key: str) -> int:
    """Convert frontend month key to DB month number (1-13)."""
    try:
        return _MONTH_KEYS.index(key) + 1
    except ValueError:
        # If it's already an int or int-string, return as-is
        try:
            return int(key)
        except (ValueError, TypeError):
            return 0


_OVERRIDE_FE_TO_DB = {v: k for k, v in _OVERRIDE_DB_TO_FE.items()}


# ── Endpoints ────────────────────────

@router.get("/api/item-plan")
async def get_item_plan(request: Request):
    """Return item plan data shaped for the React frontend.

    Frontend expects:
      data.skus[]: each has ly_units/ly_revenue/etc as month-keyed objects,
                   plus curve, ty_plan_annual, fba_beginning, wh_beginning
      data.overrides: {sku: {fe_field_name: {month_key: value}}}
      data.settings, data.factory_orders, data.factory_order_items
    """
    _require_auth(request)
    con = _duck()
    try:
        # ── Settings ──
        settings = {}
        try:
            for row in con.execute("SELECT key, value FROM item_plan_settings").fetchall():
                settings[row[0]] = row[1]
        except Exception as e:
            logger.warning(f"Item Plan settings query failed: {e}")

        # ── Seed JSON for metadata ──
        seed_data = {}
        seed_path = DB_DIR / "item_plan_seed.json"
        if seed_path.exists():
            with open(seed_path) as f:
                seed_data = json.load(f)

        # Build metadata map from seed skus
        sku_meta = {}
        for si in seed_data.get("skus", []):
            sk = si.get("sku", "")
            sku_meta[sk] = si  # full seed record

        # ── Build canonical SKU set from item_master.csv ──
        # Extension SKUs (-FBM, -1, " - FBA", etc.) are mapped to their base
        import re
        master_items = load_item_master()
        canonical_skus = {m["sku"] for m in master_items}
        _EXT_SUFFIXES = re.compile(
            r'\s*[-/]\s*(?:FBM|FBA|RB|DONATE|RETD|HOLD|Damage|CUST|Transfer|\d+)$',
            re.IGNORECASE)

        def _base_sku(raw_sku: str) -> str:
            """Strip extension suffixes to find the canonical base SKU."""
            s = raw_sku.strip()
            # Direct match first
            if s in canonical_skus:
                return s
            # Try stripping known suffixes
            stripped = _EXT_SUFFIXES.sub('', s).strip()
            if stripped in canonical_skus:
                return stripped
            return ""  # no match — skip this SKU

        # Map every monthly_sales_history SKU to a canonical base SKU
        # Gracefully handle missing table (returns empty plan if no history data)
        try:
            all_hist_skus = con.execute(
                "SELECT DISTINCT sku FROM monthly_sales_history ORDER BY sku"
            ).fetchall()
        except Exception as e:
            logger.warning(f"Item Plan: monthly_sales_history query failed (table may not exist): {e}")
            all_hist_skus = []

        # Build {base_sku: [raw_sku1, raw_sku2, ...]}
        base_to_raw = {}
        for (raw,) in all_hist_skus:
            base = _base_sku(raw)
            if base:
                base_to_raw.setdefault(base, []).append(raw)

        skus_list = []
        all_overrides = {}  # top-level {sku: {fe_field: {month_key: val}}}

        for sku in sorted(base_to_raw.keys()):
            raw_skus = base_to_raw[sku]
            # ── LY monthly data — aggregate all raw SKUs into base ──
            ly_units_arr = [0.0] * 12
            ly_rev_arr = [0.0] * 12
            ly_prof_arr = [0.0] * 12
            ly_ref_arr = [0.0] * 12

            for m in range(2, 14):  # Feb(2)..Jan(13)
                yr = 2025 if m <= 12 else 2026
                am = m if m <= 12 else m - 12
                idx = m - 2
                # Sum across all raw SKU variants for this base
                placeholders = ",".join(["?"] * len(raw_skus))
                row = con.execute(
                    f"SELECT SUM(units), SUM(revenue), SUM(profit), SUM(refund_units) "
                    f"FROM monthly_sales_history WHERE sku IN ({placeholders}) "
                    f"AND year=? AND month=?",
                    raw_skus + [yr, am],
                ).fetchone()
                if row:
                    ly_units_arr[idx] = float(row[0] or 0)
                    ly_rev_arr[idx] = float(row[1] or 0)
                    ly_prof_arr[idx] = float(row[2] or 0)
                    ly_ref_arr[idx] = float(row[3] or 0)

            # Convert to month-keyed objects
            ly_units = _array_to_month_obj(ly_units_arr)
            ly_revenue = _array_to_month_obj(ly_rev_arr)
            ly_profit = _array_to_month_obj(ly_prof_arr)
            ly_refund_units = _array_to_month_obj(ly_ref_arr)

            # Compute derived: ly_aur = revenue / units per month
            ly_aur = {}
            ly_refund_rate = {}
            for k in _MONTH_KEYS:
                u = ly_units.get(k, 0)
                r = ly_revenue.get(k, 0)
                ref = ly_refund_units.get(k, 0)
                ly_aur[k] = round(r / u, 2) if u > 0 else 0
                ly_refund_rate[k] = round(ref / u, 4) if u > 0 else 0

            # ── Curve selection ──
            crow = con.execute(
                "SELECT curve_type FROM item_plan_curve_selection WHERE sku=?", [sku]
            ).fetchone()
            curve = crow[0] if crow else "LY"

            # ── Overrides (DB → frontend field names & month keys) ──
            ov_rows = con.execute(
                "SELECT field, month, value FROM item_plan_overrides WHERE sku=?", [sku]
            ).fetchall()
            sku_ov = {}
            annual_plan_from_ov = 0
            for db_field, month_num, value in ov_rows:
                if db_field == "annual_plan_units":
                    annual_plan_from_ov = float(value)
                    continue
                fe_field = _OVERRIDE_DB_TO_FE.get(db_field, db_field)
                if fe_field not in sku_ov:
                    sku_ov[fe_field] = {}
                mk = _db_month_to_key(int(month_num))
                sku_ov[fe_field][mk] = float(value)
            all_overrides[sku] = sku_ov

            # ── Metadata from seed ──
            meta = sku_meta.get(sku, {})
            ty_plan_annual = annual_plan_from_ov or meta.get("ty_plan_annual", 0)

            # fba_beginning / wh_beginning: seed stores dicts, frontend wants a number
            fba_raw = meta.get("fba_beginning", 0)
            fba_beginning = fba_raw.get("jan", 0) if isinstance(fba_raw, dict) else float(fba_raw or 0)
            wh_raw = meta.get("wh_beginning", 0)
            wh_beginning = wh_raw.get("feb", 0) if isinstance(wh_raw, dict) else float(wh_raw or 0)

            # ty_aur plan: use LY AUR as the plan baseline (frontend references sku.ty_aur)
            ty_aur = {k: ly_aur[k] for k in _MONTH_KEYS}

            # ty_plan_units: frontend uses sku.ty_plan_units?.fy_total as fallback
            ty_plan_units = {"fy_total": int(ty_plan_annual)}

            skus_list.append({
                "sku": sku,
                "asin": meta.get("asin", ""),
                "product_name": meta.get("product_name", ""),
                "tab": meta.get("tab", ""),
                "curve": curve,
                "ty_plan_annual": int(ty_plan_annual),
                "ty_plan_units": ty_plan_units,
                "ty_aur": ty_aur,
                "fba_beginning": int(fba_beginning),
                "wh_beginning": int(wh_beginning),
                "ly_units": ly_units,
                "ly_revenue": ly_revenue,
                "ly_gross_profit": ly_profit,
                "ly_refund_units": ly_refund_units,
                "ly_aur": ly_aur,
                "ly_refund_rate": ly_refund_rate,
                "lly_units": {},
                "lly_revenue": {},
                "lly_gross_profit": {},
                "lly_refund_units": {},
            })

        return {
            "settings": settings,
            "skus": skus_list,
            "overrides": all_overrides,
        }
    finally:
        con.close()


@router.post("/api/item-plan/override")
async def post_item_plan_override(request: Request):
    _require_auth(request)
    body = await request.json()
    sku = body.get("sku", "")
    fe_field = body.get("field", "")
    month_raw = body.get("month", 0)
    value = body.get("value")

    # Map frontend field name → DB field name
    db_field = _OVERRIDE_FE_TO_DB.get(fe_field, fe_field)
    # Map frontend month key → DB month integer
    db_month = _month_key_to_db(month_raw) if isinstance(month_raw, str) else int(month_raw)

    con = _duck_rw()
    try:
        if value is None:
            # Delete override
            con.execute("""
                DELETE FROM item_plan_overrides WHERE sku = ? AND field = ? AND month = ?
            """, [sku, db_field, db_month])
        else:
            # Upsert override
            con.execute("""
                INSERT OR REPLACE INTO item_plan_overrides (sku, field, month, value)
                VALUES (?, ?, ?, ?)
            """, [sku, db_field, db_month, float(value)])
        con.commit()
        return {"status": "ok"}
    finally:
        con.close()


@router.post("/api/item-plan/curve")
async def post_item_plan_curve(request: Request):
    _require_auth(request)
    body = await request.json()
    sku = body.get("sku", "")
    curve_type = body.get("curve_type", "LY")

    con = _duck_rw()
    try:
        con.execute("""
            INSERT OR REPLACE INTO item_plan_curve_selection (sku, curve_type)
            VALUES (?, ?)
        """, [sku, curve_type])
        con.commit()
        return {"status": "ok"}
    finally:
        con.close()


@router.get("/api/item-plan/sales-curves")
async def get_item_plan_sales_curves(request: Request):
    """Return sales curves in the shape the frontend expects:
    {master: {jan: 0.05, ...}, bySku: {sku: {jan: 0.05, ...}}}"""
    _require_auth(request)
    master_curve = _compute_master_curve()

    # Build per-SKU LY curves — canonical SKUs only, aggregate extensions
    import re as _re_curves
    con = _duck()
    try:
        master_items_c = load_item_master()
        canon_c = {m["sku"] for m in master_items_c}
        _ext_re = _re_curves.compile(
            r'\s*[-/]\s*(?:FBM|FBA|RB|DONATE|RETD|HOLD|Damage|CUST|Transfer|\d+)$',
            _re_curves.IGNORECASE)

        def _base_c(raw):
            s = raw.strip()
            if s in canon_c:
                return s
            stripped = _ext_re.sub('', s).strip()
            return stripped if stripped in canon_c else ""

        try:
            all_raw = con.execute(
                "SELECT DISTINCT sku FROM monthly_sales_history"
            ).fetchall()
        except Exception:
            all_raw = []
        base_map = {}
        for (raw,) in all_raw:
            b = _base_c(raw)
            if b:
                base_map.setdefault(b, []).append(raw)

        by_sku = {}
        for base, raws in sorted(base_map.items()):
            placeholders = ",".join(["?"] * len(raws))
            rows = con.execute(
                f"SELECT year, month, SUM(units) FROM monthly_sales_history "
                f"WHERE sku IN ({placeholders}) AND year IN (2025,2026) "
                f"GROUP BY year, month", raws
            ).fetchall()
            total = sum(float(r[2] or 0) for r in rows)
            curve = {k: 0 for k in _MONTH_KEYS}
            if total > 0:
                for yr, mo, units in rows:
                    if yr == 2025 and 2 <= mo <= 12:
                        curve[_MONTH_KEYS[mo - 1]] = round(float(units or 0) / total, 4)
                    elif yr == 2026 and mo == 1:
                        curve["jan_next"] = round(float(units or 0) / total, 4)
                    elif yr == 2025 and mo == 1:
                        curve["jan"] = round(float(units or 0) / total, 4)
            by_sku[base] = curve
        return {"master": master_curve, "bySku": by_sku}
    finally:
        con.close()


@router.get("/api/factory-on-order")
async def get_factory_on_order(request: Request):
    _require_auth(request)
    con = _duck()
    try:
        orders = []
        order_rows = con.execute("""
            SELECT po_number, factory, payment_terms, total_units, factory_cost,
                   fob_date, est_arrival, wk_received, wk_available, cbm, status,
                   division, customer, platform
            FROM item_plan_factory_orders
            ORDER BY po_number
        """).fetchall()

        for row in order_rows:
            orders.append({
                "po_number": row[0],
                "factory": row[1],
                "payment_terms": row[2],
                "units": int(row[3]),
                "factory_cost": float(row[4]),
                "fob_date": row[5],
                "est_arrival": row[6],
                "wk_received": row[7],
                "wk_available": row[8],
                "cbm": float(row[9]),
                "status": row[10],
                "division": row[11],
                "customer": row[12],
                "platform": row[13],
            })

        items = []
        item_rows = con.execute("""
            SELECT id, po_number, sku, description, units, est_arrival,
                   wk_received, wk_available, status,
                   division, customer, platform
            FROM item_plan_factory_order_items
            ORDER BY id
        """).fetchall()

        for row in item_rows:
            items.append({
                "id": int(row[0]),
                "po_number": row[1],
                "sku": row[2],
                "description": row[3],
                "units": int(row[4]),
                "est_arrival": row[5],
                "wk_received": row[6],
                "wk_available": row[7],
                "status": row[8],
                "division": row[9],
                "customer": row[10],
                "platform": row[11],
            })

        return {"orders": orders, "items": items}
    finally:
        con.close()


@router.post("/api/factory-on-order")
async def post_factory_on_order(request: Request):
    _require_auth(request)
    body = await request.json()

    con = _duck_rw()
    try:
        # Determine if this is an order or item
        if "po_number" in body and "factory" in body:
            # It's an order
            con.execute("""
                INSERT OR REPLACE INTO item_plan_factory_orders
                (po_number, factory, payment_terms, total_units, factory_cost,
                 fob_date, est_arrival, wk_received, wk_available, cbm, status,
                 division, customer, platform)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                body.get("po_number", ""),
                body.get("factory", ""),
                body.get("payment_terms", ""),
                int(body.get("units", 0)),
                float(body.get("factory_cost", 0)),
                body.get("fob_date", ""),
                body.get("est_arrival", ""),
                body.get("wk_received", ""),
                body.get("wk_available", ""),
                float(body.get("cbm", 0)),
                body.get("status", "PENDING"),
                body.get("division", "golf"),
                body.get("customer", ""),
                body.get("platform", "manual_entry"),
            ])
        else:
            # It's an item
            con.execute("""
                INSERT OR REPLACE INTO item_plan_factory_order_items
                (po_number, sku, description, units, est_arrival,
                 wk_received, wk_available, status,
                 division, customer, platform)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                body.get("po_number", ""),
                body.get("sku", ""),
                body.get("description", ""),
                int(body.get("units", 0)),
                body.get("est_arrival", ""),
                body.get("wk_received", ""),
                body.get("wk_available", ""),
                body.get("status", "PENDING"),
                body.get("division", "golf"),
                body.get("customer", ""),
                body.get("platform", "manual_entry"),
            ])

        con.commit()
        return {"status": "ok"}
    finally:
        con.close()


@router.get("/api/dashboard-settings")
async def get_dashboard_settings(request: Request):
    _require_auth(request)
    con = _duck()
    try:
        settings = {}
        for row in con.execute("SELECT key, value FROM item_plan_settings").fetchall():
            settings[row[0]] = row[1]
        return settings
    finally:
        con.close()


@router.post("/api/dashboard-settings")
async def post_dashboard_settings(request: Request):
    _require_auth(request)
    body = await request.json()
    key = body.get("key", "")
    value = body.get("value", "")

    con = _duck_rw()
    try:
        con.execute("""
            INSERT OR REPLACE INTO item_plan_settings (key, value)
            VALUES (?, ?)
        """, [key, str(value)])
        con.commit()
        return {"status": "ok"}
    finally:
        con.close()
