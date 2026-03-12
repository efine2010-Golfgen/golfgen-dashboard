"""Inventory, warehouse, and FBA shipment routes."""
import os
import json
import csv
import logging
import duckdb
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, Query, Request, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

from core.config import DB_PATH, DB_DIR, COGS_PATH, CONFIG_PATH, TIMEZONE

logger = logging.getLogger("golfgen")
router = APIRouter()

# ── Constants ────────────────────────────────────────────
ITEM_MASTER_PATH = DB_DIR / "item_master.csv"
WAREHOUSE_PATH = DB_DIR / "warehouse.csv"
_FBA_SHIPMENTS_CACHE_PATH = DB_DIR / "fba_shipments_cache.json"


# ── Helper Functions ────────────────────────────────────────────

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
            if asin:
                cogs[asin] = {
                    "cogs": float(row.get("cogs") or 0),
                    "product_name": (row.get("product_name") or "").strip(),
                    "sku": (row.get("sku") or "").strip(),
                }
    return cogs


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
                "orientation": (row.get("orientation") or "").strip(),
                "category": (row.get("category") or "").strip(),
                "unitCost": float(row.get("unit_cost") or 0),
                "fbsStock": int(float(row.get("fba_stock") or 0)),
                "lyAur": round(float(row.get("ly_aur") or 0), 2),
                "lyRevenue": round(float(row.get("ly_revenue") or 0), 2),
                "lyUnits": int(float(row.get("ly_units") or 0)),
                "lyProfit": round(float(row.get("ly_profit") or 0), 2),
                "plannedAnnualUnits": int(float(row.get("planned_annual_units") or 0)),
                "listPrice": float(row.get("list_price") or 0),
                "salePrice": float(row.get("sale_price") or 0),
                "referralPct": float(row.get("referral_pct") or 15),
                "couponType": (row.get("coupon_type") or "").strip(),
                "couponValue": float(row.get("coupon_value") or 0),
                "cartonPack": int(float(row.get("carton_pack") or 0)),
                "cartonLength": float(row.get("carton_length") or 0),
                "cartonWidth": float(row.get("carton_width") or 0),
                "cartonHeight": float(row.get("carton_height") or 0),
                "cartonWeight": float(row.get("carton_weight") or 0),
            })
    return items


def save_item_master(items: list):
    """Save item master list back to CSV."""
    fields = [
        "asin", "sku", "product_name", "color", "brand", "series",
        "product_type", "piece_count", "orientation", "category", "unit_cost",
        "fba_stock", "ly_aur", "ly_revenue", "ly_units", "ly_profit",
        "planned_annual_units", "list_price", "sale_price", "referral_pct",
        "coupon_type", "coupon_value", "carton_pack", "carton_length",
        "carton_width", "carton_height", "carton_weight",
    ]
    with open(ITEM_MASTER_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for item in items:
            w.writerow({
                "asin": item.get("asin", ""),
                "sku": item.get("sku", ""),
                "product_name": item.get("productName", ""),
                "color": item.get("color", ""),
                "brand": item.get("brand", ""),
                "series": item.get("series", ""),
                "product_type": item.get("productType", ""),
                "piece_count": item.get("pieceCount", 0),
                "orientation": item.get("orientation", ""),
                "category": item.get("category", ""),
                "unit_cost": item.get("unitCost", 0),
                "fba_stock": item.get("fbsStock", 0),
                "ly_aur": item.get("lyAur", 0),
                "ly_revenue": item.get("lyRevenue", 0),
                "ly_units": item.get("lyUnits", 0),
                "ly_profit": item.get("lyProfit", 0),
                "planned_annual_units": item.get("plannedAnnualUnits", 0),
                "list_price": item.get("listPrice", 0),
                "sale_price": item.get("salePrice", 0),
                "referral_pct": item.get("referralPct", 15),
                "coupon_type": item.get("couponType", ""),
                "coupon_value": item.get("couponValue", 0),
                "carton_pack": item.get("cartonPack", 0),
                "carton_length": item.get("cartonLength", 0),
                "carton_width": item.get("cartonWidth", 0),
                "carton_height": item.get("cartonHeight", 0),
                "carton_weight": item.get("cartonWeight", 0),
            })


# ── Routes ──────────────────────────────────────────────


@router.get("/api/inventory")
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


@router.post("/api/upload/warehouse-excel")
async def upload_warehouse_excel(file: UploadFile = File(...)):
    """Upload an Excel file to refresh warehouse data and optionally item master.

    Looks for sheets named 'Raw Warehouse Data' and 'Item Master'.
    Parses each into the corresponding CSV file (warehouse.csv, item_master.csv).
    Returns a summary of what was updated.
    """
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl not installed. Add it to requirements.txt.")

    # Save upload temporarily
    import tempfile
    tmp_path = None
    try:
        contents = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(contents)
            tmp_path = tmp.name

        wb = openpyxl.load_workbook(tmp_path, data_only=True)
        result = {"filename": file.filename, "sheets_found": list(wb.sheetnames)}

        # ── Parse Raw Warehouse Data ──
        wh_sheet_name = None
        for name in wb.sheetnames:
            if "warehouse" in name.lower() or "raw" in name.lower():
                wh_sheet_name = name
                break

        if wh_sheet_name:
            ws = wb[wh_sheet_name]
            rows = list(ws.iter_rows(values_only=True))
            if len(rows) > 1:
                headers = [str(h or "").strip() for h in rows[0]]
                # Map Excel headers to our CSV column names
                col_map = {}
                for i, h in enumerate(headers):
                    hl = h.lower().replace(" ", "_").replace("-", "_")
                    if hl == "item_number" or hl == "item_no" or hl == "item":
                        col_map["item_number"] = i
                    elif hl == "description" or hl == "desc":
                        col_map["description"] = i
                    elif hl == "pack":
                        col_map["pack"] = i
                    elif hl in ("on_hand", "on hand", "oh"):
                        col_map["on_hand"] = i
                    elif hl in ("damage", "damaged", "dam"):
                        col_map["damage"] = i
                    elif hl in ("qc_hold", "qc hold", "qchold"):
                        col_map["qc_hold"] = i
                    elif hl in ("pcs_on_hand", "pcs on hand", "pcs_on_hand"):
                        col_map["pcs_on_hand"] = i
                    elif hl in ("pcs_allocated", "pcs allocated", "pcs_alloc"):
                        col_map["pcs_allocated"] = i
                    elif hl in ("pcs_available", "pcs available", "pcs_avail"):
                        col_map["pcs_available"] = i
                    elif hl in ("item_ref", "item ref", "itemref"):
                        col_map["item_ref"] = i

                # Auto-detect column positions by header name patterns
                # Also try exact header match as fallback
                header_exact = {h.strip(): i for i, h in enumerate(headers)}
                for csv_col, excel_header in [
                    ("item_number", "Item Number"), ("description", "Description"),
                    ("pack", "Pack"), ("on_hand", "On-Hand"),
                    ("damage", "Damage"), ("qc_hold", "QC Hold"),
                    ("pcs_on_hand", "Pcs On-Hand"), ("pcs_allocated", "Pcs Allocated"),
                    ("pcs_available", "Pcs Available"), ("item_ref", "Item Ref"),
                ]:
                    if csv_col not in col_map and excel_header in header_exact:
                        col_map[csv_col] = header_exact[excel_header]

                logger.info(f"  Warehouse Excel: {len(rows)-1} data rows, mapped columns: {col_map}")

                # Filter to GG-prefix items and write CSV
                wh_rows = []
                for row in rows[1:]:
                    item_num = str(row[col_map.get("item_number", 0)] or "").strip()
                    if not item_num.startswith("GG"):
                        continue
                    wh_rows.append({
                        "item_number": item_num,
                        "description": str(row[col_map.get("description", 1)] or "").strip(),
                        "pack": int(float(row[col_map.get("pack", 2)] or 1)),
                        "on_hand": int(float(row[col_map.get("on_hand", 3)] or 0)),
                        "damage": int(float(row[col_map.get("damage", 5)] or 0)),
                        "qc_hold": int(float(row[col_map.get("qc_hold", 7)] or 0)),
                        "pcs_on_hand": int(float(row[col_map.get("pcs_on_hand", 8)] or 0)),
                        "pcs_allocated": int(float(row[col_map.get("pcs_allocated", 9)] or 0)),
                        "pcs_available": int(float(row[col_map.get("pcs_available", 10)] or 0)),
                        "item_ref": str(row[col_map.get("item_ref", 11)] or "").strip(),
                    })

                if wh_rows:
                    fieldnames = ["item_number", "description", "pack", "on_hand", "damage",
                                 "qc_hold", "pcs_on_hand", "pcs_allocated", "pcs_available", "item_ref"]
                    with open(WAREHOUSE_PATH, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(wh_rows)
                    result["warehouse"] = {"status": "updated", "rows": len(wh_rows)}
                    logger.info(f"  Warehouse CSV refreshed: {len(wh_rows)} items")
                else:
                    result["warehouse"] = {"status": "no_gg_items_found", "rows": 0}
        else:
            result["warehouse"] = {"status": "sheet_not_found"}

        # ── Parse Item Master (if present) ──
        im_sheet_name = None
        for name in wb.sheetnames:
            if "item master" in name.lower() or "itemmaster" in name.lower():
                im_sheet_name = name
                break

        if im_sheet_name:
            ws = wb[im_sheet_name]
            rows = list(ws.iter_rows(values_only=True))
            if len(rows) > 1:
                headers = [str(h or "").strip() for h in rows[0]]
                # Check if this looks like our item master format
                header_lower = [h.lower() for h in headers]
                has_asin = any("asin" in h for h in header_lower)
                has_sku = any("sku" in h or "item" in h for h in header_lower)

                if has_asin or has_sku:
                    # Load existing item master to merge/update
                    existing = load_item_master()
                    existing_by_sku = {i.get("sku", ""): i for i in existing}

                    # Find column positions
                    hmap = {h.lower().strip(): i for i, h in enumerate(headers)}
                    asin_col = next((i for h, i in hmap.items() if "asin" in h), None)
                    sku_col = next((i for h, i in hmap.items() if "sku" in h or h == "item number"), None)
                    name_col = next((i for h, i in hmap.items() if "product" in h or "name" in h), None)
                    color_col = next((i for h, i in hmap.items() if "color" in h), None)

                    updated_count = 0
                    for row in rows[1:]:
                        sku_val = str(row[sku_col] or "").strip() if sku_col is not None else ""
                        if not sku_val or not sku_val.startswith("GG"):
                            continue
                        if sku_val in existing_by_sku:
                            # Update product name and color if present in Excel
                            if name_col is not None and row[name_col]:
                                existing_by_sku[sku_val]["productName"] = str(row[name_col]).strip()
                            if color_col is not None and row[color_col]:
                                existing_by_sku[sku_val]["color"] = str(row[color_col]).strip()
                            updated_count += 1

                    if updated_count > 0:
                        save_item_master(list(existing_by_sku.values()))
                    result["itemMaster"] = {"status": "updated", "matched": updated_count}
                else:
                    result["itemMaster"] = {"status": "unrecognized_format"}
        else:
            result["itemMaster"] = {"status": "sheet_not_found"}

        wb.close()
        return result

    except Exception as e:
        import traceback
        logger.error(f"Excel upload error: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


@router.post("/api/refresh-warehouse")
async def refresh_warehouse_from_file():
    """Check for a dropped Excel file in the data directory and refresh warehouse data.

    Looks for any .xlsx file in /data/ that contains a 'Raw Warehouse Data' sheet.
    This allows users to drop an Excel file into the data folder and hit this endpoint.
    """
    try:
        import openpyxl
    except ImportError:
        raise HTTPException(status_code=500, detail="openpyxl not installed")

    # Look for Excel files in the data directory
    xlsx_files = sorted(DB_DIR.glob("*.xlsx"), key=lambda f: f.stat().st_mtime, reverse=True)
    if not xlsx_files:
        return {"status": "no_xlsx_found", "data_dir": str(DB_DIR)}

    # Use the most recently modified Excel file
    excel_path = xlsx_files[0]
    logger.info(f"  Found Excel file for warehouse refresh: {excel_path.name}")

    # Re-use the upload logic by calling it with the file
    from starlette.datastructures import UploadFile as StarletteUpload
    from io import BytesIO

    with open(excel_path, "rb") as f:
        content = f.read()

    # Create a mock UploadFile
    mock_file = UploadFile(filename=excel_path.name, file=BytesIO(content))
    result = await upload_warehouse_excel(mock_file)
    result["source_file"] = excel_path.name
    return result


@router.get("/api/warehouse")
def get_warehouse():
    """Return warehouse inventory grouped by master item with sub-items."""
    if not WAREHOUSE_PATH.exists():
        return {"masters": [], "count": 0}

    # Load raw warehouse rows
    raw_items = []
    with open(WAREHOUSE_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_items.append({
                "itemNumber": (row.get("item_number") or "").strip(),
                "description": (row.get("description") or "").strip(),
                "pack": int(float(row.get("pack") or 1)),
                "onHand": int(float(row.get("on_hand") or 0)),
                "damage": int(float(row.get("damage") or 0)),
                "qcHold": int(float(row.get("qc_hold") or 0)),
                "pcsOnHand": int(float(row.get("pcs_on_hand") or 0)),
                "pcsAllocated": int(float(row.get("pcs_allocated") or 0)),
                "pcsAvailable": int(float(row.get("pcs_available") or 0)),
                "itemRef": (row.get("item_ref") or "").strip(),
            })

    # Load item master for ASIN + product name lookup
    item_master = load_item_master()
    im_lookup = {i["sku"]: i for i in item_master}

    # Group by item_ref
    groups = {}
    for item in raw_items:
        ref = item["itemRef"]
        if ref not in groups:
            groups[ref] = {"master": None, "subs": []}
        if item["itemNumber"] == ref:
            groups[ref]["master"] = item
        else:
            groups[ref]["subs"].append(item)

    # Build response: each master has aggregated totals + list of subs
    masters = []
    for ref, group in groups.items():
        master = group["master"]
        subs = group["subs"]

        # If no master record exists, create a virtual one from the ref
        if not master:
            master = {
                "itemNumber": ref,
                "description": subs[0]["description"] if subs else "",
                "pack": subs[0]["pack"] if subs else 1,
                "onHand": 0, "damage": 0, "qcHold": 0,
                "pcsOnHand": 0, "pcsAllocated": 0, "pcsAvailable": 0,
                "itemRef": ref,
            }

        # Aggregate totals across master + all subs
        all_items = [master] + subs
        total_pcs_oh = sum(i["pcsOnHand"] for i in all_items)
        total_pcs_alloc = sum(i["pcsAllocated"] for i in all_items)
        total_pcs_avail = sum(i["pcsAvailable"] for i in all_items)
        total_damage = sum(i["damage"] for i in all_items)
        total_qc = sum(i["qcHold"] for i in all_items)

        # Lookup from Item Master
        im_info = im_lookup.get(ref, {})
        asin = im_info.get("asin", "")
        im_name = im_info.get("productName", "")
        color = im_info.get("color", "")

        # Suffix label for sub-items
        for sub in subs:
            suffix = sub["itemNumber"].replace(ref, "", 1)
            sub["suffix"] = suffix if suffix else sub["itemNumber"]

        masters.append({
            "itemRef": ref,
            "itemNumber": master["itemNumber"],
            "asin": asin,
            "description": im_name if im_name else master["description"],
            "whDescription": master["description"],
            "color": color,
            "pack": master["pack"],
            "totalOnHand": total_pcs_oh,
            "totalDamage": total_damage,
            "totalQcHold": total_qc,
            "totalAllocated": total_pcs_alloc,
            "totalAvailable": total_pcs_avail,
            "subCount": len(subs),
            "subs": subs,
        })

    # Sort: items with ASIN first (known products), then by total on-hand desc
    masters.sort(key=lambda m: (0 if m["asin"] else 1, -m["totalOnHand"]))

    return {"masters": masters, "count": len(masters)}


@router.get("/api/warehouse/golf")
def warehouse_golf(channel: Optional[str] = Query(None, description="Filter by channel: Amazon, Walmart, Walmart & Amazon, Other")):
    """Golf warehouse inventory with optional channel filter."""
    items = load_json("golf_inventory.json")
    if not items:
        return {"items": [], "summary": {"totalSkus": 0, "totalPcsOnHand": 0, "totalPcsAvailable": 0, "totalPcsAllocated": 0, "totalDamage": 0}, "channelBreakdown": {}}
    if channel:
        items = [i for i in items if i.get("channel", "").lower() == channel.lower()]

    total_pcs = sum(i.get("pcsOnHand", 0) for i in items)
    total_available = sum(i.get("pcsAvailable", 0) for i in items)
    total_allocated = sum(i.get("pcsAllocated", 0) for i in items)
    total_damage = sum(i.get("damage", 0) for i in items)

    channel_counts = {}
    all_items = load_json("golf_inventory.json")
    for i in all_items:
        ch = i.get("channel", "Other")
        channel_counts[ch] = channel_counts.get(ch, 0) + 1

    return {
        "items": items,
        "summary": {
            "totalSkus": len(items),
            "totalPcsOnHand": total_pcs,
            "totalPcsAvailable": total_available,
            "totalPcsAllocated": total_allocated,
            "totalDamage": total_damage,
        },
        "channelBreakdown": channel_counts,
    }


@router.get("/api/warehouse/housewares")
def warehouse_housewares():
    """Housewares warehouse inventory."""
    items = load_json("housewares_inventory.json")
    if not items:
        return {"items": [], "summary": {"totalSkus": 0, "totalPcsOnHand": 0, "totalPcsAvailable": 0, "totalPcsAllocated": 0, "totalDamage": 0}}

    total_pcs = sum(i.get("pcsOnHand", 0) for i in items)
    total_available = sum(i.get("pcsAvailable", 0) for i in items)
    total_allocated = sum(i.get("pcsAllocated", 0) for i in items)
    total_damage = sum(i.get("damage", 0) for i in items)

    return {
        "items": items,
        "summary": {
            "totalSkus": len(items),
            "totalPcsOnHand": total_pcs,
            "totalPcsAvailable": total_available,
            "totalPcsAllocated": total_allocated,
            "totalDamage": total_damage,
        },
    }


@router.get("/api/warehouse/unified")
def warehouse_unified(division: str = Query("golf", description="golf or housewares"),
                      channel: Optional[str] = Query(None, description="All, Amazon, Walmart, Walmart & Amazon, Other (golf only)")):
    """Unified warehouse inventory with master/sub grouping, suffix breakdown, and summary.
    Used by the unified Inventory page."""
    filename = "golf_inventory.json" if division == "golf" else "housewares_inventory.json"
    items = load_json(filename)
    if not items:
        return {"masters": [], "summary": {}, "suffixBreakdown": {}, "channelBreakdown": {}}

    # For golf, apply channel filter
    if division == "golf" and channel and channel != "All":
        items = [i for i in items if (i.get("channel", "") or "").lower() == channel.lower()]

    # Channel breakdown (from full data, before channel filter)
    channel_counts = {}
    if division == "golf":
        all_items = load_json("golf_inventory.json")
        for i in all_items:
            ch = i.get("channel", "Other")
            channel_counts[ch] = channel_counts.get(ch, 0) + 1

    # Known suffixes for grouping
    SUFFIX_PATTERNS = ["/RB", "/RETD", "/DONATE", "/DMGD", "/DAM", "/HOLD", "/CUST", "/INBD", "/1",
                       "FBM", "-RB", "-DONATE", "-RETD", "-FBM", "-HOLD", "-Damage", "-CUST", "-Transfer"]

    def _get_base_sku(sku):
        """Strip prefixes (T-) and suffixes to get the base/master SKU.
        Strips iteratively to handle multi-suffix SKUs like GGWMSS2238BM/1/RB."""
        s = sku.strip()
        # Strip T- prefix
        if s.startswith("T-"):
            s = s[2:]
        # Strip known suffixes iteratively (handles /1/RB, /1/DONATE, etc.)
        changed = True
        while changed:
            changed = False
            for pat in SUFFIX_PATTERNS:
                if s.endswith(pat):
                    s = s[:-len(pat)]
                    changed = True
                    break
                if pat in s and pat.startswith("/"):
                    idx = s.find(pat)
                    s = s[:idx]
                    changed = True
                    break
        return s

    def _get_suffix_label(sku, base):
        """Determine suffix type for a variant SKU."""
        s = sku.strip()
        if s.startswith("T-"):
            return "T-"
        if "FBM" in s and "FBM" not in base:
            return "FBM"
        remainder = s.replace(base, "", 1)
        if "/RB" in remainder or "-RB" in remainder:
            return "RB"
        if "/RETD" in remainder or "-RETD" in remainder:
            return "RETD"
        if "/DONATE" in remainder or "-DONATE" in remainder:
            return "DONATE"
        if "/DMGD" in remainder or "/DAM" in remainder or "-Damage" in remainder:
            return "Damage"
        if "/HOLD" in remainder or "-HOLD" in remainder:
            return "HOLD"
        if "/CUST" in remainder or "-CUST" in remainder:
            return "CUST"
        if "/INBD" in remainder:
            return "INBD"
        if "/1" in remainder:
            return "Each"
        if remainder:
            return remainder.strip("/- ")
        return "Standard"

    # ASIN lookup from item master
    item_master = load_item_master()
    im_lookup = {i["sku"]: i for i in item_master}

    # Group items by base SKU
    groups = {}
    for item in items:
        sku = item.get("itemNumber", "").strip()
        base = _get_base_sku(sku)
        if base not in groups:
            groups[base] = {"master": None, "subs": []}
        if sku == base:
            groups[base]["master"] = item
        else:
            item["suffix"] = _get_suffix_label(sku, base)
            groups[base]["subs"].append(item)

    # Build master list
    masters = []
    for base, group in groups.items():
        master = group["master"]
        subs = group["subs"]
        if not master:
            # Create virtual master from first sub
            ref_item = subs[0] if subs else {}
            master = {
                "itemNumber": base,
                "description": ref_item.get("description", ""),
                "pack": ref_item.get("pack", 1),
                "onHand": 0, "pcsOnHand": 0, "pcsAllocated": 0,
                "pcsAvailable": 0, "damage": 0, "qcHold": 0,
                "channel": ref_item.get("channel", "Other"),
            }

        all_in_group = [master] + subs
        total_oh = sum(i.get("pcsOnHand", 0) for i in all_in_group)
        total_alloc = sum(i.get("pcsAllocated", 0) for i in all_in_group)
        total_avail = sum(i.get("pcsAvailable", 0) for i in all_in_group)
        total_dmg = sum(i.get("damage", 0) for i in all_in_group)
        total_qc = sum(i.get("qcHold", 0) for i in all_in_group)

        # ASIN lookup
        im_info = im_lookup.get(base, {})
        asin = im_info.get("asin", "")
        im_name = im_info.get("productName", "")

        masters.append({
            "baseSku": base,
            "itemNumber": master.get("itemNumber", base),
            "asin": asin,
            "description": im_name if im_name else master.get("description", ""),
            "whDescription": master.get("description", ""),
            "pack": master.get("pack", 1),
            "channel": master.get("channel", "Other") if division == "golf" else None,
            "totalOnHand": total_oh,
            "totalAllocated": total_alloc,
            "totalAvailable": total_avail,
            "totalDamage": total_dmg,
            "totalQcHold": total_qc,
            "subCount": len(subs),
            "subs": subs,
        })

    masters.sort(key=lambda m: -m["totalOnHand"])

    # Summary totals
    summary = {
        "totalSkus": len(masters),
        "totalItems": len(items),
        "totalOnHand": sum(m["totalOnHand"] for m in masters),
        "totalAllocated": sum(m["totalAllocated"] for m in masters),
        "totalAvailable": sum(m["totalAvailable"] for m in masters),
        "totalDamage": sum(m["totalDamage"] for m in masters),
    }

    # Suffix breakdown (across all items in this division, not filtered by channel)
    suffix_buckets = {}
    all_div_items = load_json(filename)
    for item in all_div_items:
        sku = item.get("itemNumber", "").strip()
        base = _get_base_sku(sku)
        if sku == base:
            suf = "Standard"
        else:
            suf = _get_suffix_label(sku, base)
        if suf not in suffix_buckets:
            suffix_buckets[suf] = {"skus": 0, "pcsOnHand": 0, "pcsAllocated": 0, "pcsAvailable": 0}
        suffix_buckets[suf]["skus"] += 1
        suffix_buckets[suf]["pcsOnHand"] += item.get("pcsOnHand", 0)
        suffix_buckets[suf]["pcsAllocated"] += item.get("pcsAllocated", 0)
        suffix_buckets[suf]["pcsAvailable"] += item.get("pcsAvailable", 0)

    return {
        "masters": masters,
        "summary": summary,
        "suffixBreakdown": suffix_buckets,
        "channelBreakdown": channel_counts,
    }


@router.get("/api/warehouse/summary")
def warehouse_summary():
    """Combined warehouse inventory summary — Golf vs Housewares totals + suffix breakdown."""
    golf_items = load_json("golf_inventory.json")
    hw_items = load_json("housewares_inventory.json")

    def _summarize(items):
        return {
            "skus": len(items),
            "pcsOnHand": sum(i.get("pcsOnHand", 0) for i in items),
            "pcsAvailable": sum(i.get("pcsAvailable", 0) for i in items),
            "pcsAllocated": sum(i.get("pcsAllocated", 0) for i in items),
            "damage": sum(i.get("damage", 0) for i in items),
        }

    def _suffix_breakdown(items):
        buckets = {}
        for item in items:
            sku = item.get("itemNumber", "").strip()
            suffix = "Standard"
            if sku.startswith("T-"):
                suffix = "T-"
            elif sku.endswith("FBM"):
                suffix = "FBM"
            elif "/RB" in sku:
                suffix = "RB"
            elif "/RETD" in sku:
                suffix = "RETD"
            elif "/DONATE" in sku:
                suffix = "DONATE"
            elif "/DMGD" in sku or "/DAM" in sku:
                suffix = "Damage"
            elif "/HOLD" in sku:
                suffix = "HOLD"
            elif "/CUST" in sku:
                suffix = "CUST"
            if suffix not in buckets:
                buckets[suffix] = {"skus": 0, "pcsOnHand": 0, "pcsAvailable": 0, "pcsAllocated": 0}
            buckets[suffix]["skus"] += 1
            buckets[suffix]["pcsOnHand"] += item.get("pcsOnHand", 0)
            buckets[suffix]["pcsAvailable"] += item.get("pcsAvailable", 0)
            buckets[suffix]["pcsAllocated"] += item.get("pcsAllocated", 0)
        return buckets

    golf_summary = _summarize(golf_items)
    hw_summary = _summarize(hw_items)

    combined = {
        "skus": golf_summary["skus"] + hw_summary["skus"],
        "pcsOnHand": golf_summary["pcsOnHand"] + hw_summary["pcsOnHand"],
        "pcsAvailable": golf_summary["pcsAvailable"] + hw_summary["pcsAvailable"],
        "pcsAllocated": golf_summary["pcsAllocated"] + hw_summary["pcsAllocated"],
        "damage": golf_summary["damage"] + hw_summary["damage"],
    }

    return {
        "combined": combined,
        "golf": golf_summary,
        "housewares": hw_summary,
        "golfSuffixes": _suffix_breakdown(golf_items),
        "housewaresSuffixes": _suffix_breakdown(hw_items),
    }


# ── FBA Shipments ──────────────────────────────────────────────

def _load_fba_shipments_cache():
    if _FBA_SHIPMENTS_CACHE_PATH.exists():
        with open(_FBA_SHIPMENTS_CACHE_PATH) as f:
            return json.load(f)
    return None


def _save_fba_shipments_cache(data):
    with open(_FBA_SHIPMENTS_CACHE_PATH, "w") as f:
        json.dump(data, f, indent=2)


def _fetch_fba_shipments_from_api(statuses=None, days_back=90):
    """Fetch FBA inbound shipments from Amazon SP-API."""
    try:
        from sp_api.api import FulfillmentInbound
        from sp_api.base import Marketplaces
    except ImportError:
        logger.warning("SP-API library not installed — cannot fetch FBA shipments")
        return None

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.warning("No SP-API credentials — cannot fetch FBA shipments")
        return None

    if statuses is None:
        statuses = ["WORKING", "SHIPPED", "RECEIVING", "CLOSED", "IN_TRANSIT"]

    try:
        fba = FulfillmentInbound(
            credentials=credentials,
            marketplace=Marketplaces.US,
        )

        all_shipments = []

        # Fetch by status list
        resp = fba.get_shipments(
            QueryType="SHIPMENT_STATUS",
            ShipmentStatusList=",".join(statuses),
            MarketplaceId="ATVPDKIKX0DER",
        )

        payload = resp.payload or {}
        shipment_data = payload.get("ShipmentData", [])
        all_shipments.extend(shipment_data)

        # Handle pagination
        next_token = payload.get("NextToken")
        page = 1
        while next_token and page < 10:  # safety limit
            page += 1
            resp = fba.get_shipments(
                QueryType="NEXT_TOKEN",
                NextToken=next_token,
                MarketplaceId="ATVPDKIKX0DER",
            )
            payload = resp.payload or {}
            shipment_data = payload.get("ShipmentData", [])
            all_shipments.extend(shipment_data)
            next_token = payload.get("NextToken")

        logger.info(f"FBA Shipments: fetched {len(all_shipments)} shipments from SP-API")

        # Normalize shipment data for frontend
        normalized = []
        for s in all_shipments:
            ship_id = s.get("ShipmentId", "")
            ship_name = s.get("ShipmentName", "")
            status = s.get("ShipmentStatus", "")
            dest = s.get("DestinationFulfillmentCenterId", "")
            label_prep = s.get("LabelPrepType", "")
            are_cases_required = s.get("AreCasesRequired", False)

            # Address info
            ship_from = s.get("ShipFromAddress", {})
            ship_from_str = ""
            if ship_from:
                parts = [ship_from.get("City", ""), ship_from.get("StateOrProvinceCode", "")]
                ship_from_str = ", ".join(p for p in parts if p)

            # Items count from InboundShipmentInfo
            items = s.get("Items", [])

            normalized.append({
                "shipmentId": ship_id,
                "shipmentName": ship_name,
                "status": status,
                "destination": dest,
                "labelPrep": label_prep,
                "casesRequired": are_cases_required,
                "shipFrom": ship_from_str,
                "itemCount": len(items) if items else 0,
            })

        result = {
            "shipments": normalized,
            "lastSync": datetime.now(TIMEZONE).strftime("%m/%d/%Y %I:%M %p"),
            "totalShipments": len(normalized),
        }

        _save_fba_shipments_cache(result)
        return result

    except Exception as e:
        logger.error(f"FBA Shipments SP-API error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def _enrich_shipment_items(shipment_id):
    """Fetch items for a specific shipment from SP-API."""
    try:
        from sp_api.api import FulfillmentInbound
        from sp_api.base import Marketplaces
    except ImportError:
        return []

    credentials = _load_sp_api_credentials()
    if not credentials:
        return []

    try:
        fba = FulfillmentInbound(
            credentials=credentials,
            marketplace=Marketplaces.US,
        )
        resp = fba.shipment_items_by_shipment(shipment_id, MarketplaceId="ATVPDKIKX0DER")
        payload = resp.payload or {}
        items = payload.get("ItemData", [])

        result = []
        for item in items:
            result.append({
                "sku": item.get("SellerSKU", ""),
                "fnsku": item.get("FulfillmentNetworkSKU", ""),
                "quantityShipped": item.get("QuantityShipped", 0),
                "quantityReceived": item.get("QuantityReceived", 0),
                "quantityInCase": item.get("QuantityInCase", 0),
            })

        # Handle pagination
        next_token = payload.get("NextToken")
        page = 1
        while next_token and page < 10:
            page += 1
            resp = fba.shipment_items_by_shipment(
                shipment_id,
                MarketplaceId="ATVPDKIKX0DER",
                NextToken=next_token,
            )
            payload = resp.payload or {}
            for item in payload.get("ItemData", []):
                result.append({
                    "sku": item.get("SellerSKU", ""),
                    "fnsku": item.get("FulfillmentNetworkSKU", ""),
                    "quantityShipped": item.get("QuantityShipped", 0),
                    "quantityReceived": item.get("QuantityReceived", 0),
                    "quantityInCase": item.get("QuantityInCase", 0),
                })
            next_token = payload.get("NextToken")

        return result
    except Exception as e:
        logger.error(f"FBA shipment items error for {shipment_id}: {e}")
        return []


def _load_sp_api_credentials() -> dict | None:
    """Load SP-API credentials from env vars or config file."""
    import os
    # Priority 1: Environment variables (used on Railway)
    env_refresh = os.environ.get("SP_API_REFRESH_TOKEN", "")
    if env_refresh:
        return {
            "refresh_token": env_refresh,
            "lwa_app_id": os.environ.get("SP_API_LWA_APP_ID") or os.environ.get("LWA_APP_ID", ""),
            "lwa_client_secret": os.environ.get("SP_API_LWA_CLIENT_SECRET") or os.environ.get("LWA_CLIENT_SECRET", ""),
            "aws_access_key": os.environ.get("SP_API_AWS_ACCESS_KEY", ""),
            "aws_secret_key": os.environ.get("SP_API_AWS_SECRET_KEY", ""),
            "role_arn": os.environ.get("SP_API_ROLE_ARN", ""),
        }

    # Priority 2: Config file (local development)
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            creds_json = json.load(f)
        sp = creds_json.get("AMAZON_SP_API", {})
        creds = {
            "refresh_token": sp.get("refresh_token"),
            "lwa_app_id": sp.get("lwa_app_id"),
            "lwa_client_secret": sp.get("lwa_client_secret"),
            "aws_access_key": sp.get("aws_access_key"),
            "aws_secret_key": sp.get("aws_secret_key"),
            "role_arn": sp.get("role_arn"),
        }
        if creds.get("refresh_token"):
            return creds

    return None


@router.get("/api/fba-shipments")
async def get_fba_shipments(request: Request, refresh: bool = False):
    """Get FBA inbound shipments. Uses cache unless refresh=true."""
    from core.auth import require_auth
    require_auth(request)

    if not refresh:
        cached = _load_fba_shipments_cache()
        if cached:
            return cached

    # Try fetching fresh data from SP-API
    data = _fetch_fba_shipments_from_api()
    if data:
        return data

    # Fall back to cache even if refresh was requested
    cached = _load_fba_shipments_cache()
    if cached:
        cached["_note"] = "Using cached data — SP-API sync failed"
        return cached

    return {"shipments": [], "lastSync": None, "totalShipments": 0}


@router.post("/api/fba-shipments/sync")
async def sync_fba_shipments(request: Request):
    """Force refresh of FBA shipment data from SP-API."""
    from core.auth import require_auth
    require_auth(request)
    data = _fetch_fba_shipments_from_api()
    if data:
        return {"ok": True, "totalShipments": data["totalShipments"], "lastSync": data["lastSync"]}
    return JSONResponse(status_code=500, content={"ok": False, "error": "SP-API sync failed. Check credentials."})


@router.get("/api/fba-shipments/{shipment_id}/items")
async def get_fba_shipment_items(request: Request, shipment_id: str):
    """Get items for a specific FBA inbound shipment."""
    from core.auth import require_auth
    require_auth(request)
    items = _enrich_shipment_items(shipment_id)
    return {"shipmentId": shipment_id, "items": items, "totalItems": len(items)}
