"""
GolfGen Write API — allows Ask Claude and direct UI calls to mutate data.

Endpoints:
  POST /api/writes/cogs                — update unit_cost for an ASIN
  POST /api/writes/ad-budget           — set ad budget target / ACOS goal
  POST /api/writes/inventory-threshold — set reorder threshold for a SKU/ASIN
  POST /api/writes/price-note          — add a price note / override flag
  GET  /api/writes/ad-budgets          — list all ad budget targets
  GET  /api/writes/inventory-thresholds — list all inventory thresholds
  GET  /api/writes/price-notes         — list all price notes
"""

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.database import get_db, get_db_rw

logger = logging.getLogger("golfgen")
router = APIRouter()
CENTRAL = ZoneInfo("America/Chicago")


# ── Auth helper ──────────────────────────────────────────────────────────────

def _check_auth(request: Request, allow_viewer: bool = False):
    """Return (user_email, role) or raise JSONResponse on failure."""
    token = request.cookies.get("golfgen_session")
    if not token:
        raise JSONResponse(status_code=401, content={"error": "Not authenticated."})
    try:
        con = get_db()
        row = con.execute(
            "SELECT user_email, role FROM sessions WHERE token = ?", [token]
        ).fetchone()
        con.close()
    except Exception as e:
        raise JSONResponse(status_code=401, content={"error": "Auth error."})
    if not row:
        raise JSONResponse(status_code=401, content={"error": "Session expired."})
    user_email, role = row[0], (row[1] or "staff")
    if not allow_viewer and role.lower() == "viewer":
        raise JSONResponse(status_code=403, content={"error": "Write access not available for Viewer accounts."})
    return user_email, role


def _audit(con_rw, user_email: str, action: str, detail: str):
    try:
        con_rw.execute(
            "INSERT INTO audit_log (timestamp, user_email, action, detail) VALUES (?, ?, ?, ?)",
            [datetime.now(CENTRAL).isoformat(), user_email, action, detail],
        )
    except Exception as e:
        logger.warning(f"audit_log write error: {e}")


# ── 1. Update COGS ────────────────────────────────────────────────────────────

class UpdateCogsRequest(BaseModel):
    asin: str
    new_cogs: float
    note: str = ""


@router.post("/api/writes/cogs")
async def update_cogs(req: UpdateCogsRequest, request: Request):
    try:
        user_email, role = _check_auth(request)
    except JSONResponse as r:
        return r

    if req.new_cogs < 0:
        return JSONResponse(status_code=400, content={"error": "COGS cannot be negative."})

    try:
        con_rw = get_db_rw()
        # Ensure column exists (graceful on both DuckDB and PG)
        try:
            con_rw.execute("ALTER TABLE item_master ADD COLUMN IF NOT EXISTS unit_cost DOUBLE")
        except Exception:
            pass
        # Try UPDATE; if 0 rows affected we don't insert (ASIN must already exist in item_master)
        con_rw.execute(
            "UPDATE item_master SET unit_cost = ? WHERE asin = ?",
            [req.new_cogs, req.asin.strip().upper()],
        )
        _audit(con_rw, user_email, "update_cogs",
               f"ASIN={req.asin} new_cogs={req.new_cogs} note={req.note[:80]}")
        con_rw.close()
    except Exception as e:
        logger.error(f"update_cogs error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

    return {"ok": True, "asin": req.asin, "new_cogs": req.new_cogs}


# ── 2. Ad Budget Target ───────────────────────────────────────────────────────

class SetAdBudgetRequest(BaseModel):
    identifier: str           # ASIN, campaign name, or "ALL"
    identifier_type: str = "asin"   # "asin" | "campaign" | "portfolio" | "all"
    target_acos: float | None = None
    daily_budget: float | None = None
    monthly_budget: float | None = None
    note: str = ""


@router.post("/api/writes/ad-budget")
async def set_ad_budget(req: SetAdBudgetRequest, request: Request):
    try:
        user_email, role = _check_auth(request)
    except JSONResponse as r:
        return r

    try:
        con_rw = get_db_rw()
        con_rw.execute("""
            CREATE TABLE IF NOT EXISTS ad_budget_targets (
                id          INTEGER,
                identifier  TEXT NOT NULL,
                id_type     TEXT NOT NULL DEFAULT 'asin',
                target_acos DOUBLE,
                daily_budget  DOUBLE,
                monthly_budget DOUBLE,
                note        TEXT,
                updated_by  TEXT,
                updated_at  TEXT,
                PRIMARY KEY (identifier, id_type)
            )
        """)
        # Upsert pattern compatible with both DuckDB and PG
        existing = con_rw.execute(
            "SELECT id FROM ad_budget_targets WHERE identifier = ? AND id_type = ?",
            [req.identifier, req.identifier_type],
        ).fetchone()
        now = datetime.now(CENTRAL).isoformat()
        if existing:
            con_rw.execute("""
                UPDATE ad_budget_targets
                SET target_acos=?, daily_budget=?, monthly_budget=?, note=?, updated_by=?, updated_at=?
                WHERE identifier=? AND id_type=?
            """, [req.target_acos, req.daily_budget, req.monthly_budget,
                  req.note, user_email, now, req.identifier, req.identifier_type])
        else:
            con_rw.execute("""
                INSERT INTO ad_budget_targets
                  (identifier, id_type, target_acos, daily_budget, monthly_budget, note, updated_by, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [req.identifier, req.identifier_type, req.target_acos,
                  req.daily_budget, req.monthly_budget, req.note, user_email, now])
        _audit(con_rw, user_email, "set_ad_budget",
               f"{req.identifier_type}={req.identifier} acos={req.target_acos} daily={req.daily_budget}")
        con_rw.close()
    except Exception as e:
        logger.error(f"set_ad_budget error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

    return {"ok": True, "identifier": req.identifier, "target_acos": req.target_acos,
            "daily_budget": req.daily_budget}


@router.get("/api/writes/ad-budgets")
async def list_ad_budgets(request: Request):
    try:
        user_email, role = _check_auth(request, allow_viewer=True)
    except JSONResponse as r:
        return r
    try:
        con = get_db()
        rows = con.execute(
            "SELECT identifier, id_type, target_acos, daily_budget, monthly_budget, note, updated_by, updated_at FROM ad_budget_targets ORDER BY updated_at DESC"
        ).fetchall()
        con.close()
        return {"budgets": [
            {"identifier": r[0], "idType": r[1], "targetAcos": r[2],
             "dailyBudget": r[3], "monthlyBudget": r[4], "note": r[5],
             "updatedBy": r[6], "updatedAt": r[7]} for r in rows
        ]}
    except Exception:
        return {"budgets": []}


# ── 3. Inventory Threshold ────────────────────────────────────────────────────

class SetInventoryThresholdRequest(BaseModel):
    asin: str = ""
    sku: str = ""
    reorder_point: int          # units — alert fires when FBA drops below this
    target_weeks_cover: float | None = None
    note: str = ""


@router.post("/api/writes/inventory-threshold")
async def set_inventory_threshold(req: SetInventoryThresholdRequest, request: Request):
    try:
        user_email, role = _check_auth(request)
    except JSONResponse as r:
        return r

    if not req.asin and not req.sku:
        return JSONResponse(status_code=400, content={"error": "Provide asin or sku."})
    if req.reorder_point < 0:
        return JSONResponse(status_code=400, content={"error": "reorder_point must be >= 0."})

    identifier = (req.asin or req.sku).strip().upper()

    try:
        con_rw = get_db_rw()
        con_rw.execute("""
            CREATE TABLE IF NOT EXISTS inventory_thresholds (
                identifier       TEXT PRIMARY KEY,
                id_type          TEXT NOT NULL DEFAULT 'asin',
                reorder_point    INTEGER NOT NULL,
                target_weeks_cover DOUBLE,
                note             TEXT,
                updated_by       TEXT,
                updated_at       TEXT
            )
        """)
        id_type = "asin" if req.asin else "sku"
        existing = con_rw.execute(
            "SELECT identifier FROM inventory_thresholds WHERE identifier = ?",
            [identifier],
        ).fetchone()
        now = datetime.now(CENTRAL).isoformat()
        if existing:
            con_rw.execute("""
                UPDATE inventory_thresholds
                SET reorder_point=?, target_weeks_cover=?, note=?, updated_by=?, updated_at=?
                WHERE identifier=?
            """, [req.reorder_point, req.target_weeks_cover, req.note, user_email, now, identifier])
        else:
            con_rw.execute("""
                INSERT INTO inventory_thresholds
                  (identifier, id_type, reorder_point, target_weeks_cover, note, updated_by, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [identifier, id_type, req.reorder_point,
                  req.target_weeks_cover, req.note, user_email, now])
        _audit(con_rw, user_email, "set_inventory_threshold",
               f"{identifier} reorder_point={req.reorder_point} weeks={req.target_weeks_cover}")
        con_rw.close()
    except Exception as e:
        logger.error(f"set_inventory_threshold error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

    return {"ok": True, "identifier": identifier, "reorder_point": req.reorder_point}


@router.get("/api/writes/inventory-thresholds")
async def list_inventory_thresholds(request: Request):
    try:
        user_email, role = _check_auth(request, allow_viewer=True)
    except JSONResponse as r:
        return r
    try:
        con = get_db()
        rows = con.execute(
            "SELECT identifier, id_type, reorder_point, target_weeks_cover, note, updated_by, updated_at FROM inventory_thresholds ORDER BY updated_at DESC"
        ).fetchall()
        con.close()
        return {"thresholds": [
            {"identifier": r[0], "idType": r[1], "reorderPoint": r[2],
             "targetWeeksCover": r[3], "note": r[4], "updatedBy": r[5],
             "updatedAt": r[6]} for r in rows
        ]}
    except Exception:
        return {"thresholds": []}


# ── 4. Price Note ─────────────────────────────────────────────────────────────

class AddPriceNoteRequest(BaseModel):
    asin: str = ""
    sku: str = ""
    note: str
    price_override: float | None = None   # optional MAP or target price
    note_type: str = "general"            # "map_exception" | "price_hold" | "clearance" | "general"


@router.post("/api/writes/price-note")
async def add_price_note(req: AddPriceNoteRequest, request: Request):
    try:
        user_email, role = _check_auth(request)
    except JSONResponse as r:
        return r

    if not req.asin and not req.sku:
        return JSONResponse(status_code=400, content={"error": "Provide asin or sku."})
    if not req.note.strip():
        return JSONResponse(status_code=400, content={"error": "Note cannot be empty."})

    identifier = (req.asin or req.sku).strip().upper()

    try:
        con_rw = get_db_rw()
        con_rw.execute("""
            CREATE TABLE IF NOT EXISTS price_notes (
                id             INTEGER,
                identifier     TEXT NOT NULL,
                id_type        TEXT NOT NULL DEFAULT 'asin',
                note           TEXT NOT NULL,
                note_type      TEXT DEFAULT 'general',
                price_override DOUBLE,
                created_by     TEXT,
                created_at     TEXT
            )
        """)
        # Auto-increment ID (compatible with both engines)
        try:
            max_row = con_rw.execute("SELECT COALESCE(MAX(id), 0) FROM price_notes").fetchone()
            next_id = int(max_row[0]) + 1
        except Exception:
            next_id = 1
        id_type = "asin" if req.asin else "sku"
        now = datetime.now(CENTRAL).isoformat()
        con_rw.execute("""
            INSERT INTO price_notes (id, identifier, id_type, note, note_type, price_override, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [next_id, identifier, id_type, req.note.strip(),
              req.note_type, req.price_override, user_email, now])
        _audit(con_rw, user_email, "add_price_note",
               f"{identifier} type={req.note_type} override={req.price_override}")
        con_rw.close()
    except Exception as e:
        logger.error(f"add_price_note error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

    return {"ok": True, "identifier": identifier, "note_type": req.note_type}


@router.get("/api/writes/price-notes")
async def list_price_notes(request: Request, asin: str = "", limit: int = 50):
    try:
        user_email, role = _check_auth(request, allow_viewer=True)
    except JSONResponse as r:
        return r
    try:
        con = get_db()
        if asin:
            rows = con.execute(
                "SELECT id, identifier, id_type, note, note_type, price_override, created_by, created_at FROM price_notes WHERE identifier = ? ORDER BY created_at DESC LIMIT ?",
                [asin.upper(), limit]
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT id, identifier, id_type, note, note_type, price_override, created_by, created_at FROM price_notes ORDER BY created_at DESC LIMIT ?",
                [limit]
            ).fetchall()
        con.close()
        return {"notes": [
            {"id": r[0], "identifier": r[1], "idType": r[2], "note": r[3],
             "noteType": r[4], "priceOverride": r[5], "createdBy": r[6],
             "createdAt": r[7]} for r in rows
        ]}
    except Exception:
        return {"notes": []}
