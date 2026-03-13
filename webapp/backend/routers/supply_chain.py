"""
Supply Chain Report — Unified PO / OTW / Invoice views.

Data Store: JSON file (supply_chain_store.json) with flat denormalized records.
Each record = one SKU × PO × Container × Invoice row.

Upload: Single Excel file with 2 tabs:
  Tab 1 "PO & Invoice Data" — PO#, Factory, Invoice#, Container#, SKU, Desc, Units, CBM, Est.Arrival, Status
  Tab 2 "Shipment Data"     — Container#, PO#, Shipper, HB#, Type, Vessel, ETD, ETD Port, ETA, ETA Port,
                               ETA Delivery, Delivery Location, Actual Delivery

Three GET views derive from the same store:
  /api/supply-chain/otw      — container-level OTW summary
  /api/supply-chain/po       — PO-level summary with ordered vs shipped
  /api/supply-chain/invoices — invoice-level summary
  /api/supply-chain/upload   — single-file upload (POST)
"""

from __future__ import annotations
import json, os, re
from datetime import datetime, date
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
from fastapi import APIRouter, UploadFile, File, Depends
import openpyxl

from core.config import DB_DIR
from core.auth import require_auth

router = APIRouter(prefix="/api/supply-chain", tags=["supply-chain"])

STORE_PATH = os.path.join(DB_DIR, "supply_chain_store.json")
TZ = ZoneInfo("America/Chicago")


# ── helpers ──────────────────────────────────────────────────────────────

def _load_store() -> dict:
    """Load the data store. Returns {"records": [...], "lastUpload": ..., "sourceFile": ...}"""
    if os.path.exists(STORE_PATH):
        with open(STORE_PATH, "r") as f:
            return json.load(f)
    return {"records": [], "lastUpload": None, "sourceFile": None}


def _save_store(data: dict):
    with open(STORE_PATH, "w") as f:
        json.dump(data, f, default=str)


def _dash(v):
    """Return '—' for empty/None values."""
    if v is None or (isinstance(v, str) and v.strip() in ("", "—", "-", "N/A", "n/a", "(blank)")):
        return "—"
    return v


def _parse_date(v):
    """Parse various date formats to ISO string or '—'."""
    if v is None:
        return "—"
    if isinstance(v, (datetime, date)):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, str):
        v = v.strip()
        if v in ("—", "-", "", "(blank)", "N/A"):
            return "—"
        # Try common formats
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%d-%b-%Y", "%b %d, %Y"):
            try:
                return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return v  # return as-is if can't parse
    return "—"


def _derive_eta_month(eta_str: str) -> str:
    """Derive 'Jan 2026' style month from an ISO date string."""
    if eta_str == "—" or not eta_str:
        return "—"
    try:
        dt = datetime.strptime(eta_str, "%Y-%m-%d")
        return dt.strftime("%b %Y")
    except (ValueError, TypeError):
        return "—"


def _cell_val(ws, row, col):
    """Get cell value from openpyxl worksheet."""
    cell = ws.cell(row=row, column=col)
    return cell.value


def _str(v):
    if v is None:
        return ""
    return str(v).strip()


def _float_or_zero(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _int_or_zero(v):
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return 0


# ── Upload endpoint ──────────────────────────────────────────────────────

@router.post("/upload")
async def upload_supply_chain(file: UploadFile = File(...), _user=Depends(require_auth)):
    """
    Upload a single Excel file with 2 tabs:
      Tab 1: PO & Invoice Data (SKU-level)
      Tab 2: Shipment Data (container-level logistics)
    Merges into the data store by record_id = {PO#}_{Container#}_{SKU}.
    """
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    content = await file.read()
    tmp.write(content)
    tmp.close()

    try:
        wb = openpyxl.load_workbook(tmp.name, data_only=True)
    except Exception as e:
        os.unlink(tmp.name)
        return {"status": "error", "message": f"Cannot read Excel file: {e}"}

    # ── Find the two tabs ──
    sheet_names = wb.sheetnames
    po_sheet = None
    ship_sheet = None

    inv_sheet = None  # optional separate Invoice Summary tab

    for name in sheet_names:
        lower = name.lower()
        if "po" in lower and ("invoice" in lower or "data" in lower):
            po_sheet = wb[name]
        elif lower.strip() in ("po summary", "po_summary", "po data", "purchase orders"):
            if po_sheet is None:
                po_sheet = wb[name]
        elif "ship" in lower or "logistics" in lower or "otw" in lower:
            ship_sheet = wb[name]
        elif "invoice" in lower and "summary" in lower:
            inv_sheet = wb[name]

    # Fallback: try by position if we have exactly 2 sheets
    if po_sheet is None and ship_sheet is None and len(sheet_names) >= 2:
        po_sheet = wb[sheet_names[0]]
        ship_sheet = wb[sheet_names[1]]
    elif po_sheet is None and len(sheet_names) >= 1:
        po_sheet = wb[sheet_names[0]]

    if po_sheet is None:
        os.unlink(tmp.name)
        return {"status": "error", "message": "Could not find PO & Invoice Data tab in the workbook."}

    # ── Parse Tab 1: PO & Invoice Data ──
    # Find header row (look for PO # in first 10 rows)
    po_header_row = None
    po_col_map = {}
    for r in range(1, min(po_sheet.max_row + 1, 15)):
        row_vals = [_str(_cell_val(po_sheet, r, c)).lower() for c in range(1, po_sheet.max_column + 1)]
        # Check if this looks like a header row
        joined = " ".join(row_vals)
        if "po" in joined and ("sku" in joined or "item" in joined or "unit" in joined):
            po_header_row = r
            # Map column names
            for c in range(1, po_sheet.max_column + 1):
                hdr = _str(_cell_val(po_sheet, r, c)).lower()
                if not hdr:
                    continue
                if "po" in hdr and "#" in hdr or hdr == "po #" or hdr == "po#" or hdr == "po number":
                    po_col_map["po_number"] = c
                elif "factory" in hdr or "supplier" in hdr:
                    po_col_map["factory"] = c
                elif "invoice" in hdr and ("#" in hdr or "number" in hdr or "num" in hdr):
                    po_col_map["invoice_number"] = c
                elif "container" in hdr and ("#" in hdr or "number" in hdr or "num" in hdr):
                    po_col_map["container_number"] = c
                elif hdr in ("sku", "item #", "item#", "item number", "item no"):
                    po_col_map["sku"] = c
                elif "description" in hdr or "desc" in hdr:
                    po_col_map["description"] = c
                elif hdr in ("units", "qty", "quantity") or ("unit" in hdr and "order" not in hdr):
                    po_col_map["units"] = c
                elif "cbm" in hdr:
                    po_col_map["cbm"] = c
                elif "fob" in hdr and ("date" in hdr or hdr == "fob"):
                    po_col_map["fob_date"] = c
                elif "payment" in hdr and "term" in hdr:
                    po_col_map["payment_terms"] = c
                elif "arrival" in hdr or "eta" in hdr or "est" in hdr:
                    po_col_map["est_arrival"] = c
                elif "status" in hdr:
                    po_col_map["status"] = c
            break

    if po_header_row is None:
        os.unlink(tmp.name)
        return {"status": "error", "message": "Could not find header row in PO & Invoice Data tab. Expected columns: PO #, SKU, Units, etc."}

    # Parse PO data rows
    po_records = []
    for r in range(po_header_row + 1, po_sheet.max_row + 1):
        po_num = _str(_cell_val(po_sheet, r, po_col_map.get("po_number", 1)))
        sku = _str(_cell_val(po_sheet, r, po_col_map.get("sku", 6)))
        if not po_num and not sku:
            continue  # skip empty rows
        if not po_num or po_num.lower() in ("total", "subtotal", "grand total"):
            continue

        factory = _dash(_str(_cell_val(po_sheet, r, po_col_map.get("factory", 2))))
        payment_terms = _dash(_str(_cell_val(po_sheet, r, po_col_map.get("payment_terms", 0)))) if po_col_map.get("payment_terms") else "—"
        invoice = _dash(_str(_cell_val(po_sheet, r, po_col_map.get("invoice_number", 3))))
        container = _dash(_str(_cell_val(po_sheet, r, po_col_map.get("container_number", 4))))
        desc = _dash(_str(_cell_val(po_sheet, r, po_col_map.get("description", 7))))
        units = _int_or_zero(_cell_val(po_sheet, r, po_col_map.get("units", 8)))
        cbm = _float_or_zero(_cell_val(po_sheet, r, po_col_map.get("cbm", 9)))
        fob_date = _parse_date(_cell_val(po_sheet, r, po_col_map.get("fob_date", 0))) if po_col_map.get("fob_date") else "—"
        est_arrival = _parse_date(_cell_val(po_sheet, r, po_col_map.get("est_arrival", 10)))
        status = _dash(_str(_cell_val(po_sheet, r, po_col_map.get("status", 11))))

        # Normalize status
        if status != "—":
            sl = status.lower()
            if "deliver" in sl:
                status = "Delivered"
            elif "transit" in sl or "ship" in sl:
                status = "In Transit"
            elif "open" in sl or "pending" in sl or "order" in sl:
                status = "Open"

        # Generate record_id
        container_key = container if container != "—" else "UNSHIPPED"
        record_id = f"{po_num}_{container_key}_{sku}"

        po_records.append({
            "record_id": record_id,
            "po_number": po_num,
            "factory": factory,
            "payment_terms": payment_terms,
            "invoice_number": invoice,
            "invoice_date": "—",  # will be enriched from Invoice Summary tab
            "container_number": container,
            "sku": sku,
            "description": desc,
            "units": units,
            "cbm": cbm,
            "x_factory_date": fob_date,
            "container_eta": est_arrival,
            "eta_month": _derive_eta_month(est_arrival),
            "status": status,
            # Shipment fields — will be enriched from Tab 2
            "shipper": "—",
            "hb_number": "—",
            "container_type": "—",
            "vessel": "—",
            "etd": "—",
            "etd_port": "—",
            "eta_port": "—",
            "eta_delivery": "—",
            "delivery_location": "—",
            "actual_delivery": "—",
            "last_updated": datetime.now(TZ).isoformat(),
        })

    # ── Parse Tab 2: Shipment Data (if present) ──
    shipment_map = {}  # keyed by (container_number, po_number)
    if ship_sheet is not None:
        ship_header_row = None
        ship_col_map = {}
        for r in range(1, min(ship_sheet.max_row + 1, 15)):
            row_vals = [_str(_cell_val(ship_sheet, r, c)).lower() for c in range(1, ship_sheet.max_column + 1)]
            joined = " ".join(row_vals)
            if "container" in joined and ("po" in joined or "vessel" in joined or "shipper" in joined):
                ship_header_row = r
                for c in range(1, ship_sheet.max_column + 1):
                    hdr = _str(_cell_val(ship_sheet, r, c)).lower()
                    if not hdr:
                        continue
                    if ("container" in hdr and "#" in hdr) or hdr in ("cntr#", "cntr #", "container#"):
                        ship_col_map["container_number"] = c
                    elif "po" in hdr and ("invoice" not in hdr):
                        ship_col_map["po_number"] = c
                    elif "shipper" in hdr:
                        ship_col_map["shipper"] = c
                    elif "hb" in hdr or "hbl" in hdr or "house bill" in hdr:
                        ship_col_map["hb_number"] = c
                    elif "type" in hdr or "mode" in hdr:
                        ship_col_map["container_type"] = c
                    elif "vessel" in hdr:
                        ship_col_map["vessel"] = c
                    elif ("etd" in hdr and "port" not in hdr) or "departure" in hdr and "port" not in hdr:
                        ship_col_map["etd"] = c
                    elif ("etd" in hdr and "port" in hdr) or "departure" in hdr and "port" in hdr:
                        ship_col_map["etd_port"] = c
                    elif ("arrival" in hdr and "port" in hdr) or ("eta" in hdr and "port" in hdr and "deliv" not in hdr and "dest" not in hdr and "final" not in hdr):
                        ship_col_map["eta_port"] = c
                    elif "eta" in hdr and ("deliv" in hdr or "dest" in hdr or "final" in hdr):
                        ship_col_map["eta_delivery"] = c
                    elif ("eta" in hdr or "discharge" in hdr) and "port" not in hdr and "deliv" not in hdr and "dest" not in hdr and "final" not in hdr:
                        ship_col_map["eta"] = c
                    elif ("delivery" in hdr and "location" in hdr) or "final location" in hdr or "final" in hdr and "location" in hdr:
                        ship_col_map["delivery_location"] = c
                    elif ("actual" in hdr and "delivery" in hdr) or "delivery date" in hdr:
                        ship_col_map["actual_delivery"] = c
                    elif "factory" in hdr and "invoice" in hdr:
                        ship_col_map["invoice_number"] = c
                    elif "invoice" in hdr:
                        ship_col_map["invoice_number"] = c
                    elif "unit" in hdr:
                        ship_col_map["units"] = c
                    elif "cbm" in hdr:
                        ship_col_map["cbm"] = c
                    elif "status" in hdr:
                        ship_col_map["status"] = c
                break

        if ship_header_row:
            for r in range(ship_header_row + 1, ship_sheet.max_row + 1):
                cntr = _str(_cell_val(ship_sheet, r, ship_col_map.get("container_number", 1)))
                po = _str(_cell_val(ship_sheet, r, ship_col_map.get("po_number", 2)))
                if not cntr or cntr.lower() in ("total", "subtotal", "..."):
                    continue

                key = (cntr, po)
                shipment_map[key] = {
                    "shipper": _dash(_str(_cell_val(ship_sheet, r, ship_col_map.get("shipper", 3)))),
                    "hb_number": _dash(_str(_cell_val(ship_sheet, r, ship_col_map.get("hb_number", 4)))),
                    "container_type": _dash(_str(_cell_val(ship_sheet, r, ship_col_map.get("container_type", 5)))),
                    "vessel": _dash(_str(_cell_val(ship_sheet, r, ship_col_map.get("vessel", 6)))),
                    "etd": _parse_date(_cell_val(ship_sheet, r, ship_col_map.get("etd", 7))),
                    "etd_port": _dash(_str(_cell_val(ship_sheet, r, ship_col_map.get("etd_port", 8)))),
                    "eta_port": _dash(_str(_cell_val(ship_sheet, r, ship_col_map.get("eta_port", 10)))),
                    "eta_delivery": _parse_date(_cell_val(ship_sheet, r, ship_col_map.get("eta_delivery", 11))),
                    "delivery_location": _dash(_str(_cell_val(ship_sheet, r, ship_col_map.get("delivery_location", 12)))),
                    "actual_delivery": _parse_date(_cell_val(ship_sheet, r, ship_col_map.get("actual_delivery", 13))),
                }
                # Also use ETA from shipment tab if available
                eta_val = _parse_date(_cell_val(ship_sheet, r, ship_col_map.get("eta", 9)))
                if eta_val != "—":
                    shipment_map[key]["container_eta"] = eta_val

    # ── Enrich PO records with shipment data ──
    for rec in po_records:
        cntr = rec["container_number"]
        po = rec["po_number"]
        if cntr != "—":
            ship_data = shipment_map.get((cntr, po))
            if ship_data is None:
                # Try matching by container only (some POs share containers)
                for (c, p), sd in shipment_map.items():
                    if c == cntr:
                        ship_data = sd
                        break
            if ship_data:
                for field in ("shipper", "hb_number", "container_type", "vessel", "etd", "etd_port", "eta_port", "eta_delivery", "delivery_location", "actual_delivery"):
                    if ship_data.get(field, "—") != "—":
                        rec[field] = ship_data[field]
                if ship_data.get("container_eta", "—") != "—":
                    rec["container_eta"] = ship_data["container_eta"]
                    rec["eta_month"] = _derive_eta_month(ship_data["container_eta"])

    # ── Enrich from Invoice Summary sheet (if present) ──
    if inv_sheet is not None:
        inv_header_row = None
        inv_col_map = {}
        for r in range(1, min(inv_sheet.max_row + 1, 15)):
            row_vals = [_str(_cell_val(inv_sheet, r, c)).lower() for c in range(1, inv_sheet.max_column + 1)]
            joined = " ".join(row_vals)
            if "invoice" in joined and ("po" in joined or "date" in joined or "unit" in joined):
                inv_header_row = r
                for c in range(1, inv_sheet.max_column + 1):
                    hdr = _str(_cell_val(inv_sheet, r, c)).lower()
                    if not hdr:
                        continue
                    if "po" in hdr and "#" in hdr or hdr in ("po#", "po #", "po number"):
                        inv_col_map["po_number"] = c
                    elif "container" in hdr and ("#" in hdr or "number" in hdr or "num" in hdr):
                        inv_col_map["container_number"] = c
                    elif ("invoice" in hdr and ("#" in hdr or "number" in hdr or "num" in hdr)) or hdr in ("invoice #", "invoice#"):
                        inv_col_map["invoice_number"] = c
                    elif "invoice" in hdr and "date" in hdr:
                        inv_col_map["invoice_date"] = c
                    elif "unit" in hdr or "qty" in hdr:
                        inv_col_map["inv_units"] = c
                    elif "cbm" in hdr:
                        inv_col_map["inv_cbm"] = c
                    elif "shipper" in hdr:
                        inv_col_map["invoice_shipper"] = c
                    elif "sold" in hdr:
                        inv_col_map["sold_to"] = c
                break

        if inv_header_row and inv_col_map.get("invoice_date"):
            # Build lookup: (po_number, container_number) -> invoice_date
            inv_date_map = {}
            for r in range(inv_header_row + 1, inv_sheet.max_row + 1):
                po = _str(_cell_val(inv_sheet, r, inv_col_map.get("po_number", 1)))
                cntr = _str(_cell_val(inv_sheet, r, inv_col_map.get("container_number", 2)))
                inv_num = _str(_cell_val(inv_sheet, r, inv_col_map.get("invoice_number", 3)))
                inv_date = _parse_date(_cell_val(inv_sheet, r, inv_col_map.get("invoice_date", 4)))
                if not po or po.lower() in ("total", "subtotal"):
                    continue
                # Key by PO + container
                if cntr:
                    inv_date_map[(po, cntr)] = {"invoice_date": inv_date, "invoice_number": inv_num}
                # Also key by PO + invoice for matching
                if inv_num:
                    inv_date_map[("inv", po, inv_num)] = {"invoice_date": inv_date}

            # Enrich PO records with invoice dates
            for rec in po_records:
                po = rec["po_number"]
                cntr = rec["container_number"]
                inv_num = rec["invoice_number"]
                # Try by PO + container first
                inv_data = inv_date_map.get((po, cntr))
                if inv_data is None and inv_num != "—":
                    inv_data = inv_date_map.get(("inv", po, inv_num))
                if inv_data:
                    if inv_data.get("invoice_date", "—") != "—":
                        rec["invoice_date"] = inv_data["invoice_date"]
                    if inv_data.get("invoice_number") and rec["invoice_number"] == "—":
                        rec["invoice_number"] = inv_data["invoice_number"]

    # Also enrich invoice_number from shipment data if present
    for rec in po_records:
        if rec["invoice_number"] == "—":
            cntr = rec["container_number"]
            po = rec["po_number"]
            if cntr != "—":
                ship_data = shipment_map.get((cntr, po))
                if ship_data is None:
                    for (c, p), sd in shipment_map.items():
                        if c == cntr:
                            ship_data = sd
                            break
                if ship_data and ship_data.get("invoice_number", "—") != "—":
                    rec["invoice_number"] = ship_data["invoice_number"]

    # ── Merge into data store ──
    store = _load_store()
    existing = {r["record_id"]: r for r in store.get("records", [])}

    for rec in po_records:
        existing[rec["record_id"]] = rec  # upsert

    store["records"] = list(existing.values())
    store["lastUpload"] = datetime.now(TZ).isoformat()
    store["sourceFile"] = file.filename
    _save_store(store)

    os.unlink(tmp.name)

    return {
        "status": "ok",
        "records_parsed": len(po_records),
        "total_records": len(store["records"]),
        "lastUpload": store["lastUpload"],
        "sourceFile": store["sourceFile"],
    }


# ── Also support legacy upload endpoints (backward compat) ──

@router.post("/v2/upload")
async def upload_v2(file: UploadFile = File(...), _user=Depends(require_auth)):
    """Alias for the main upload endpoint."""
    return await upload_supply_chain(file, _user)


# ── GET endpoints ────────────────────────────────────────────────────────

@router.get("/otw")
async def get_otw_summary(_user=Depends(require_auth)):
    """
    View 1: OTW Summary — container-level.
    One row per Container × PO. Leads with shipping/logistics data.
    """
    store = _load_store()
    records = store.get("records", [])

    # Group by (container_number, po_number) for container-level view
    container_groups: dict[str, dict] = {}
    item_detail: list[dict] = []  # for item pivot table

    for rec in records:
        cntr = rec.get("container_number", "—")
        po = rec.get("po_number", "—")
        key = f"{cntr}|{po}"

        if key not in container_groups:
            container_groups[key] = {
                "container_number": cntr,
                "hb_number": rec.get("hb_number", "—"),
                "vessel": rec.get("vessel", "—"),
                "etd": rec.get("etd", "—"),
                "etd_port": rec.get("etd_port", "—"),
                "container_eta": rec.get("container_eta", "—"),
                "eta_port": rec.get("eta_port", "—"),
                "eta_delivery": rec.get("eta_delivery", "—"),
                "delivery_location": rec.get("delivery_location", "—"),
                "actual_delivery": rec.get("actual_delivery", "—"),
                "units": 0,
                "cbm": 0.0,
                "po_number": po,
                "invoice_number": rec.get("invoice_number", "—"),
                "status": rec.get("status", "—"),
                "eta_month": rec.get("eta_month", "—"),
                "container_type": rec.get("container_type", "—"),
                "shipper": rec.get("shipper", "—"),
                "x_factory_date": rec.get("x_factory_date", "—"),
            }

        container_groups[key]["units"] += rec.get("units", 0)
        container_groups[key]["cbm"] += rec.get("cbm", 0.0)

        # Collect item detail
        item_detail.append({
            "sku": rec.get("sku", "—"),
            "description": rec.get("description", "—"),
            "container_number": cntr,
            "units": rec.get("units", 0),
        })

    rows = list(container_groups.values())
    # Round CBM
    for r in rows:
        r["cbm"] = round(r["cbm"], 1)

    # Sort: Delivered first, then In Transit, then Open; within each by ETA
    status_order = {"Delivered": 0, "In Transit": 1, "Open": 2, "—": 3}
    rows.sort(key=lambda x: (status_order.get(x["status"], 3), x.get("container_eta", "zzz")))

    # KPI summary
    total_containers = len(set(r["container_number"] for r in rows if r["container_number"] != "—"))
    total_units = sum(r["units"] for r in rows)
    total_cbm = round(sum(r["cbm"] for r in rows), 1)
    delivered = len([r for r in rows if r["status"] == "Delivered"])
    in_transit = len([r for r in rows if r["status"] == "In Transit"])
    open_count = len([r for r in rows if r["status"] == "Open"])

    # Build item pivot: rows = SKUs, columns = container numbers
    all_containers = list(dict.fromkeys(r["container_number"] for r in rows))
    sku_map: dict[str, dict] = {}
    for item in item_detail:
        sku = item["sku"]
        if sku not in sku_map:
            sku_map[sku] = {"sku": sku, "description": item["description"], "containers": {}, "total": 0}
        cntr = item["container_number"]
        sku_map[sku]["containers"][cntr] = sku_map[sku]["containers"].get(cntr, 0) + item["units"]
        sku_map[sku]["total"] += item["units"]

    item_pivot = list(sku_map.values())
    item_pivot.sort(key=lambda x: x["sku"])

    return {
        "rows": rows,
        "summary": {
            "totalContainers": total_containers,
            "totalUnits": total_units,
            "totalCBM": total_cbm,
            "delivered": delivered,
            "inTransit": in_transit,
            "open": open_count,
        },
        "itemPivot": item_pivot,
        "allContainers": all_containers,
        "lastUpload": store.get("lastUpload"),
        "sourceFile": store.get("sourceFile"),
    }


@router.get("/po")
async def get_po_summary(_user=Depends(require_auth)):
    """
    View 2: PO Summary — PO-level.
    One row per PO × Container/Invoice. Shows ordered vs shipped.
    """
    store = _load_store()
    records = store.get("records", [])

    # Group by PO to calculate totals, then expand per container
    po_totals: dict[str, int] = {}  # po_number -> total units ordered
    for rec in records:
        po = rec.get("po_number", "—")
        po_totals[po] = po_totals.get(po, 0) + rec.get("units", 0)

    # Group by (po_number, container_number) for row-level
    po_container_groups: dict[str, dict] = {}
    item_detail: list[dict] = []

    for rec in records:
        po = rec.get("po_number", "—")
        cntr = rec.get("container_number", "—")
        key = f"{po}|{cntr}"

        if key not in po_container_groups:
            po_container_groups[key] = {
                "po_number": po,
                "factory": rec.get("factory", "—"),
                "units_ordered": 0,  # will set on first row of PO
                "units_shipped": 0,
                "difference": 0,
                "cbm": 0.0,
                "container_number": cntr,
                "invoice_number": rec.get("invoice_number", "—"),
                "payment_terms": rec.get("payment_terms", "—"),
                "x_factory_date": rec.get("x_factory_date", "—"),
                "etd": rec.get("etd", "—"),
                "etd_port": rec.get("etd_port", "—"),
                "eta_month": rec.get("eta_month", "—"),
                "container_eta": rec.get("container_eta", "—"),
                "eta_port": rec.get("eta_port", "—"),
                "status": rec.get("status", "—"),
            }

        po_container_groups[key]["units_shipped"] += rec.get("units", 0)
        po_container_groups[key]["cbm"] += rec.get("cbm", 0.0)

        item_detail.append({
            "sku": rec.get("sku", "—"),
            "description": rec.get("description", "—"),
            "po_number": po,
            "units": rec.get("units", 0),
        })

    rows = list(po_container_groups.values())
    for r in rows:
        r["cbm"] = round(r["cbm"], 1)

    # Set units_ordered on first row of each PO, '—' on subsequent
    seen_pos = set()
    rows.sort(key=lambda x: (x["po_number"], x["container_number"]))
    for r in rows:
        po = r["po_number"]
        if po not in seen_pos:
            r["units_ordered"] = po_totals.get(po, 0)
            seen_pos.add(po)
        else:
            r["units_ordered"] = "—"

    # Compute difference for each PO (first row only)
    po_shipped: dict[str, int] = {}
    for r in rows:
        po = r["po_number"]
        if r["status"] in ("Delivered", "In Transit"):
            po_shipped[po] = po_shipped.get(po, 0) + r["units_shipped"]

    for r in rows:
        if r["units_ordered"] != "—":
            r["difference"] = r["units_ordered"] - po_shipped.get(r["po_number"], 0)

    # KPI summary
    unique_pos = set(r["po_number"] for r in rows)
    total_ordered = sum(po_totals.values())
    total_shipped = sum(r["units_shipped"] for r in rows if r["status"] in ("Delivered", "In Transit"))
    status_counts = {}
    for r in rows:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    # Build item pivot: rows = SKUs, columns = PO numbers
    all_pos = list(dict.fromkeys(r["po_number"] for r in rows))
    sku_map: dict[str, dict] = {}
    for item in item_detail:
        sku = item["sku"]
        if sku not in sku_map:
            sku_map[sku] = {"sku": sku, "description": item["description"], "pos": {}, "total": 0}
        po = item["po_number"]
        sku_map[sku]["pos"][po] = sku_map[sku]["pos"].get(po, 0) + item["units"]
        sku_map[sku]["total"] += item["units"]

    item_pivot = list(sku_map.values())
    item_pivot.sort(key=lambda x: x["sku"])

    return {
        "rows": rows,
        "summary": {
            "totalPOs": len(unique_pos),
            "unitsOrdered": total_ordered,
            "unitsShipped": total_shipped,
            "delivered": status_counts.get("Delivered", 0),
            "inTransit": status_counts.get("In Transit", 0),
            "open": status_counts.get("Open", 0),
        },
        "itemPivot": item_pivot,
        "allPOs": all_pos,
        "lastUpload": store.get("lastUpload"),
        "sourceFile": store.get("sourceFile"),
    }


@router.get("/invoices")
async def get_invoice_summary(_user=Depends(require_auth)):
    """
    View 3: Invoice Summary — invoice-level.
    One row per Invoice × PO × Container.
    """
    store = _load_store()
    records = store.get("records", [])

    # Group by (invoice_number, po_number, container_number)
    inv_groups: dict[str, dict] = {}
    item_detail: list[dict] = []

    for rec in records:
        inv = rec.get("invoice_number", "—")
        po = rec.get("po_number", "—")
        cntr = rec.get("container_number", "—")
        key = f"{inv}|{po}|{cntr}"

        if key not in inv_groups:
            inv_groups[key] = {
                "invoice_number": inv,
                "invoice_date": rec.get("invoice_date", "—"),
                "total_units": 0,
                "cbm": 0.0,
                "po_number": po,
                "container_number": cntr,
                "container_eta": rec.get("container_eta", "—"),
                "eta_port": rec.get("eta_port", "—"),
                "factory": rec.get("factory", "—"),
                "status": rec.get("status", "—"),
                "eta_month": rec.get("eta_month", "—"),
            }

        inv_groups[key]["total_units"] += rec.get("units", 0)
        inv_groups[key]["cbm"] += rec.get("cbm", 0.0)

        item_detail.append({
            "sku": rec.get("sku", "—"),
            "description": rec.get("description", "—"),
            "invoice_number": inv,
            "units": rec.get("units", 0),
        })

    rows = list(inv_groups.values())
    for r in rows:
        r["cbm"] = round(r["cbm"], 1)

    # Sort by status then ETA
    status_order = {"Delivered": 0, "In Transit": 1, "Open": 2, "—": 3}
    rows.sort(key=lambda x: (status_order.get(x["status"], 3), x.get("container_eta", "zzz")))

    # KPI
    unique_invoices = set(r["invoice_number"] for r in rows if r["invoice_number"] != "—")
    total_units = sum(r["total_units"] for r in rows)
    total_cbm = round(sum(r["cbm"] for r in rows), 1)
    status_counts = {}
    for r in rows:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    # Build item pivot: rows = SKUs, columns = invoice numbers
    all_invoices = list(dict.fromkeys(r["invoice_number"] for r in rows))
    sku_map: dict[str, dict] = {}
    for item in item_detail:
        sku = item["sku"]
        if sku not in sku_map:
            sku_map[sku] = {"sku": sku, "description": item["description"], "invoices": {}, "total": 0}
        inv = item["invoice_number"]
        sku_map[sku]["invoices"][inv] = sku_map[sku]["invoices"].get(inv, 0) + item["units"]
        sku_map[sku]["total"] += item["units"]

    item_pivot = list(sku_map.values())
    item_pivot.sort(key=lambda x: x["sku"])

    return {
        "rows": rows,
        "summary": {
            "totalInvoices": len(unique_invoices),
            "totalUnits": total_units,
            "totalCBM": total_cbm,
            "delivered": status_counts.get("Delivered", 0),
            "inTransit": status_counts.get("In Transit", 0),
            "open": status_counts.get("Open", 0),
        },
        "itemPivot": item_pivot,
        "allInvoices": all_invoices,
        "lastUpload": store.get("lastUpload"),
        "sourceFile": store.get("sourceFile"),
    }


@router.get("/store")
async def get_raw_store(_user=Depends(require_auth)):
    """Debug endpoint: return the raw data store."""
    store = _load_store()
    return {
        "total_records": len(store.get("records", [])),
        "lastUpload": store.get("lastUpload"),
        "sourceFile": store.get("sourceFile"),
        "records": store.get("records", [])[:200],  # cap at 200 for debug
    }
