"""OTW / Logistics routes."""
import json
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, Request, UploadFile, File, HTTPException

from core.config import DB_DIR, TIMEZONE
from core.auth import require_auth

logger = logging.getLogger("golfgen")
router = APIRouter()


def _load_logistics():
    fp = DB_DIR / "logistics_tracking.json"
    if fp.exists():
        with open(fp) as f:
            return json.load(f)
    return {"shipments": [], "itemsByContainer": [], "lastUpload": None}

def _save_logistics(data):
    fp = DB_DIR / "logistics_tracking.json"
    with open(fp, "w") as f:
        json.dump(data, f, indent=2)

@router.get("/api/logistics")
async def get_logistics(request: Request):
    require_auth(request)
    data = _load_logistics()
    _SHIP_STOP = {"SHIPMENT STATUS SUMMARY", "STATUS SUMMARY", "UPCOMING ARRIVALS",
                  "TOTAL", "TOTALS", "ITEM SUMMARY", "PO#", "STATUS", "IN TRANSIT",
                  "DELIVERED", "PENDING SHIPMENT", "PENDING"}
    if data.get("shipments"):
        clean = []
        for s in data["shipments"]:
            shipper = (s.get("shipper") or "").strip()
            if not shipper or shipper.upper() in _SHIP_STOP:
                continue
            if any(sw in shipper.upper() for sw in ("STATUS SUMMARY", "UPCOMING ARRIVALS", "ITEM SUMMARY")):
                continue
            hbl = (s.get("hbl") or "").strip()
            if not hbl or len(hbl) < 5:
                continue
            # Normalize old field names to new field names
            if "mode" in s and "containerType" not in s:
                s["containerType"] = s.pop("mode")
            if "vessel" in s and "vesselVoyage" not in s:
                s["vesselVoyage"] = s.pop("vessel")
            # Ensure status field exists
            if not s.get("status"):
                s["status"] = ""
            clean.append(s)
        data["shipments"] = clean
    # Clean items: filter out Container Total, NON-GOLF, etc.
    if data.get("itemsByContainer"):
        data["itemsByContainer"] = [
            item for item in data["itemsByContainer"]
            if item.get("sku") and str(item["sku"]).strip()
            and "NON-GOLF" not in str(item.get("containerNumber", "")).upper()
            and "CONTAINER TOTAL" not in str(item.get("description", "")).upper()
            and "GRAND TOTAL" not in str(item.get("description", "")).upper()
        ]

    # ── Enrich items with container status from shipments ──
    container_status = {}
    container_eta = {}
    for s in data.get("shipments", []):
        cn = (s.get("containerNumber") or "").strip()
        if cn:
            container_status[cn] = s.get("status") or ""
            container_eta[cn] = s.get("deliveryDate") or s.get("etaFinal") or s.get("etaDischarge") or ""
    for item in data.get("itemsByContainer", []):
        cn = (item.get("containerNumber") or "").strip()
        item["status"] = container_status.get(cn, "")

    # ── If no itemsByContainer from logistics sheet, build from Factory PO Summary ──
    if not data.get("itemsByContainer"):
        try:
            fp = DB_DIR / "factory_po_summary.json"
            if fp.exists():
                with open(fp) as f:
                    po_data = json.load(f)
                # Build PO → container mapping from shipments (by matching HBL/PO)
                # Use logistics items if available in PO data
                logistics_items = po_data.get("logisticsItems", [])
                if logistics_items:
                    for item in logistics_items:
                        cn = (item.get("containerNumber") or "").strip()
                        item["status"] = container_status.get(cn, "")
                        item["arrivalDate"] = container_eta.get(cn, item.get("eta", ""))
                    data["itemsByContainer"] = logistics_items
                else:
                    # Build from unitsByItem + container mapping
                    units = po_data.get("unitsByItem", [])
                    # Build SKU → containers from PO containers
                    sku_po_map = {}  # sku -> list of PO numbers from byPO
                    for u in units:
                        sku = (u.get("sku") or "").strip()
                        if not sku:
                            continue
                        for po_num in (u.get("byPO") or {}).keys():
                            sku_po_map.setdefault(sku, set()).add(po_num)

                    # Build PO → containers from purchaseOrders
                    po_container_map = {}
                    for po in po_data.get("purchaseOrders", []):
                        po_num = (po.get("poNumber") or "").strip()
                        for c in po.get("containers", []):
                            cn = (c.get("containerNumber") or "").strip()
                            if cn:
                                po_container_map.setdefault(po_num, []).append(cn)

                    items = []
                    for u in units:
                        sku = (u.get("sku") or "").strip()
                        desc = u.get("description", "")
                        total_qty = u.get("total") or u.get("quantity") or 0
                        # Find containers for this SKU via PO mapping
                        containers_for_sku = set()
                        for po_num in sku_po_map.get(sku, set()):
                            for cn in po_container_map.get(po_num, []):
                                containers_for_sku.add(cn)
                        if containers_for_sku:
                            for cn in sorted(containers_for_sku):
                                items.append({
                                    "containerNumber": cn,
                                    "sku": sku,
                                    "description": desc,
                                    "qty": total_qty,
                                    "eta": container_eta.get(cn, ""),
                                    "status": container_status.get(cn, ""),
                                    "arrivalDate": container_eta.get(cn, ""),
                                })
                        else:
                            items.append({
                                "containerNumber": "",
                                "sku": sku,
                                "description": desc,
                                "qty": total_qty,
                                "eta": "",
                                "status": "",
                                "arrivalDate": "",
                            })
                    data["itemsByContainer"] = items
        except Exception as e:
            logger.error(f"Error building items from Factory PO: {e}")

    return data

@router.post("/api/logistics/upload")
async def upload_logistics(request: Request, file: UploadFile = File(...)):
    """Standalone logistics upload — delegates to the same logic as combined supply-chain upload."""
    require_auth(request)
    import openpyxl
    from io import BytesIO
    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Empty file uploaded")
    try:
        wb = openpyxl.load_workbook(BytesIO(contents), data_only=True)
    except Exception as e:
        logger.error(f"Failed to open Excel file: {e}")
        raise HTTPException(400, f"Could not read Excel file: {e}. Make sure it is a valid .xlsx file.")
    # Case-insensitive sheet name matching
    logistics_sheet = None
    for sn in wb.sheetnames:
        if sn.strip().lower() == "logistics tracking":
            logistics_sheet = sn
            break
    if not logistics_sheet:
        raise HTTPException(400, f"Sheet 'Logistics Tracking' not found. Available sheets: {wb.sheetnames}")
    ws = wb[logistics_sheet]
    rows = []
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row):
        r = {}
        for c in row:
            if hasattr(c, 'column_letter') and c.value is not None:
                r[c.column_letter] = c.value
        rows.append(r)

    def _to_str(v):
        if v is None: return ""
        from datetime import datetime as _dt, date as _d
        if isinstance(v, (_dt, _d)):
            return v.strftime("%Y-%m-%d")
        return str(v)

    def _sr(v, n=2):
        try: return round(float(v), n)
        except: return 0

    def _si(v):
        try: return int(float(v))
        except: return 0

    _STOP_WORDS = {"SHIPMENT STATUS SUMMARY", "STATUS SUMMARY", "UPCOMING ARRIVALS",
                    "TOTAL", "TOTALS", "ITEM SUMMARY", "PO#"}

    # ── Parse shipments ──
    shipments = []
    header_row = None
    for i, r in enumerate(rows):
        vals = [str(v).upper() for v in r.values()]
        joined = " ".join(vals)
        if ("SHIPPER" in joined or "FACTORY" in joined) and ("HBL" in joined or "HB" in joined or "MODE" in joined):
            header_row = i
            break

    if header_row is not None:
        empty_count = 0
        for i in range(header_row + 1, len(rows)):
            r = rows[i]
            shipper = r.get("B")
            if not shipper:
                empty_count += 1
                if empty_count >= 2: break
                continue
            empty_count = 0
            shipper_str = str(shipper).strip()
            shipper_upper = shipper_str.upper()
            if any(sw in shipper_upper for sw in _STOP_WORDS): break
            if shipper_upper in ["SHIPPER", "FACTORY", "STATUS", ""] or len(shipper_str) <= 3: continue
            hbl = str(r.get("D") or "").strip()
            if not hbl or len(hbl) < 5: continue
            # Infer status from delivery date
            delivery_raw = r.get("N")
            if delivery_raw and str(delivery_raw).strip():
                status = "Delivered"
            else:
                eta_raw = r.get("J")
                if eta_raw:
                    status = "In Transit"
                else:
                    status = "Pending"
            shipments.append({
                "shipper": shipper_str,                            # B = Factory/Shipper
                "factory": str(r.get("C") or "").strip(),          # C = Shipped (brand)
                "hbl": hbl,                                        # D = HB#
                "containerType": str(r.get("E") or "").strip(),    # E = Type
                "containerNumber": str(r.get("F") or "").strip(),  # F = CNTR#
                "vesselVoyage": str(r.get("G") or "").strip(),     # G = Vessel
                "etdOrigin": _to_str(r.get("H")),                  # H = ETD
                "departurePort": str(r.get("I") or "").strip(),    # I = ETD PORT
                "etaDischarge": _to_str(r.get("J")),               # J = ETA
                "arrivalPort": str(r.get("K") or "").strip(),      # K = ETA PORT
                "etaFinal": _to_str(r.get("L")),                   # L = ETA Dest. (date)
                "finalLocation": str(r.get("M") or "").strip(),    # M = ETA Dest. (location)
                "deliveryDate": _to_str(r.get("N")),               # N = Actual Delivery
                "comments": str(r.get("O") or "").strip(),         # O = Comments
                "status": status,
            })

    # ── Parse items by container ──
    item_header_row = None
    for i, r in enumerate(rows):
        for _k, v in r.items():
            if v and "container" in str(v).lower() and "item" in str(v).lower():
                item_header_row = i; break
        if item_header_row is not None: break

    items = []
    if item_header_row is not None:
        col_hdr = item_header_row + 1
        for ci in range(item_header_row, min(item_header_row + 4, len(rows))):
            vals_up = [str(v).upper() for v in rows[ci].values()]
            joined = " ".join(vals_up)
            if "CONTAINER" in joined and ("ITEM" in joined or "SKU" in joined):
                col_hdr = ci; break
        cur_container = ""
        for i in range(col_hdr + 1, len(rows)):
            r = rows[i]
            container_val = r.get("B"); sku_val = r.get("F"); desc_val = r.get("G"); qty_val = r.get("J")
            if not container_val and not sku_val and not desc_val: continue
            container_str = str(container_val or "").strip()
            sku_str = str(sku_val or "").strip()
            desc_str = str(desc_val or "").strip()
            skip_words = ["CONTAINER TOTAL", "GOLF TOTAL", "GRAND TOTAL", "NON-GOLF", "CONTAINER #", "ITEM NUMBER", "ITEM SUMMARY"]
            combined_upper = f"{container_str} {sku_str} {desc_str}".upper()
            if any(sw in combined_upper for sw in skip_words): continue
            if container_val and len(container_str) >= 8 and container_str[0].isalpha():
                cur_container = container_str
            if not sku_val or len(sku_str) < 3: continue
            items.append({
                "containerNumber": cur_container, "invoice": str(r.get("C") or "").strip(),
                "eta": _to_str(r.get("D")), "po": str(r.get("E") or "").strip(),
                "sku": sku_str, "description": desc_str, "qty": _si(qty_val),
            })

    from datetime import date as _date
    data = {"shipments": shipments, "itemsByContainer": items,
        "lastUpload": _date.today().isoformat(), "sourceFile": file.filename}
    _save_logistics(data)
    return {"status": "ok", "shipments": len(shipments), "items": len(items), "lastUpload": data["lastUpload"]}


@router.post("/api/supply-chain/upload")
async def upload_supply_chain(request: Request, file: UploadFile = File(...)):
    """Combined upload: parses both Factory PO Summary and Logistics Tracking from one Excel file"""
    require_auth(request)
    import openpyxl
    from io import BytesIO
    contents = await file.read()
    if not contents:
        raise HTTPException(400, "Empty file uploaded")
    try:
        wb = openpyxl.load_workbook(BytesIO(contents), data_only=True)
    except Exception as e:
        logger.error(f"Failed to open Excel file: {e}")
        raise HTTPException(400, f"Could not read Excel file: {e}. Make sure it is a valid .xlsx file.")
    results = {"status": "ok", "sourceFile": file.filename}

    # Try Factory PO (case-insensitive)
    factory_sheet = None
    for sn in wb.sheetnames:
        if sn.strip().lower() == "factory po summary":
            factory_sheet = sn
            break
    if factory_sheet:
        ws = wb[factory_sheet]
        rows = []
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row):
            r = {}
            for c in row:
                if hasattr(c, 'column_letter') and c.value is not None:
                    r[c.column_letter] = c.value
            rows.append(r)

        def safe_float(v, default=0):
            try: return float(v)
            except (ValueError, TypeError): return default

        def safe_round(v, n=2):
            return round(safe_float(v), n)

        def _to_date_str(v):
            from datetime import datetime as _dt, date as _d
            if isinstance(v, (_dt, _d)):
                return v.strftime("%Y-%m-%d")
            s = str(v).strip() if v else ""
            return s if s else ""

        # ── Parse Purchase Orders (rows 8-18 in current sheet) ──
        # Find header row: must have BOTH "Factory" AND "PO" in same row
        po_header_row = None
        for i in range(0, min(15, len(rows))):
            vals_upper = [str(v).upper() for v in rows[i].values()]
            joined = " ".join(vals_upper)
            if "FACTORY" in joined and ("PO#" in joined or "PO #" in joined):
                po_header_row = i
                break
        if po_header_row is None:
            po_header_row = 6  # fallback

        purchase_orders = []
        for i in range(po_header_row + 1, len(rows)):
            r = rows[i]
            po_val = r.get("E")
            factory = r.get("B") or r.get("A")
            units_val = r.get("F")

            # Stop at TOTALS row or empty rows section
            factory_str = str(factory or "").strip().upper()
            if factory_str == "TOTALS" or factory_str == "TOTAL":
                break

            # Skip rows without a PO number in column E
            if not po_val:
                continue

            po_str = str(po_val).strip()
            # PO numbers must start with T- or be a numeric PO like 2100
            # Skip if it looks like a pure float/money value (from payment calendar)
            if not (po_str.startswith("T-") or po_str.startswith("Z") or
                    po_str.startswith("GG") or po_str.startswith("PO") or
                    (po_str.isdigit() and len(po_str) <= 6)):
                continue

            # Must have a factory name (not a PO number in col B from payment section)
            if not factory or str(factory).startswith("T-"):
                continue

            terms = str(r.get("D") or "").strip()
            purchase_orders.append({
                "factory": str(factory),
                "poNumber": po_str,
                "paymentTerms": terms,
                "units": safe_round(units_val, 0),
                "totalCost": safe_round(r.get("G"), 2),
                "fobDate": _to_date_str(r.get("I")),
                "estArrival": _to_date_str(r.get("J")),
                "cbm": safe_round(r.get("K"), 1),
                "landedCost": safe_round(r.get("U"), 2),
            })

        # ── Parse Units by Item (starts at "UNITS BY ITEM" header) ──
        units_by_item = []
        units_header_row = None
        for i, r in enumerate(rows):
            for v in r.values():
                if "UNITS BY ITEM" in str(v).upper() and "BY PURCHASE ORDER" in str(v).upper():
                    units_header_row = i
                    break
            if units_header_row is not None:
                break

        if units_header_row is not None:
            # Next row is the column header (SKU, Description, PO columns)
            col_header_row = units_header_row + 1
            # Build PO column mapping from the header row
            header_r = rows[col_header_row] if col_header_row < len(rows) else {}
            po_cols = {}  # col_letter -> PO name
            for k, v in header_r.items():
                if k not in ["A", "B", "C"] and v:
                    po_cols[k] = str(v)

            # Skip X-Factory date row if present
            start_row = col_header_row + 1
            if start_row < len(rows):
                first_b = str(rows[start_row].get("B", "")).upper()
                if "X-FACTORY" in first_b or "FACTORY" in first_b:
                    start_row += 1

            for i in range(start_row, len(rows)):
                r = rows[i]
                sku = r.get("B")
                if not sku:
                    continue
                sku_str = str(sku).strip()
                if sku_str.upper() in ["TOTAL", "TOTALS", "", "SKU"]:
                    break  # end of section
                desc = str(r.get("C") or "")
                by_po = {}
                total = 0
                for k in po_cols:
                    val = r.get(k)
                    if val is not None:
                        try:
                            n = int(float(val))
                            by_po[po_cols[k]] = n
                            total += n
                        except (ValueError, TypeError):
                            pass
                # Also check col P for TOTAL
                p_val = r.get("P")
                if p_val is not None:
                    try:
                        total = int(float(p_val))
                    except (ValueError, TypeError):
                        pass
                units_by_item.append({"sku": sku_str, "description": desc, "byPO": by_po, "total": total})

        # ── Parse Arrival Schedule (starts at "EST. ARRIVAL") ──
        arrival_schedule = []
        arrival_header_row = None
        for i, r in enumerate(rows):
            for v in r.values():
                vup = str(v).upper()
                if "EST" in vup and "ARRIVAL" in vup:
                    arrival_header_row = i
                    break
            if arrival_header_row is not None:
                break

        if arrival_header_row is not None:
            # Next row should be column headers: SKU, Description, month names
            arr_col_row = arrival_header_row + 1
            arr_header = rows[arr_col_row] if arr_col_row < len(rows) else {}
            month_cols = {}  # col_letter -> month name
            for k, v in arr_header.items():
                if k not in ["A", "B", "C"] and v:
                    v_str = str(v).strip()
                    if v_str.upper() not in ["TOTAL", "TOTALS", "SKU"]:
                        month_cols[k] = v_str
            total_col = None
            for k, v in arr_header.items():
                if str(v).upper() in ["TOTAL", "TOTALS"]:
                    total_col = k

            for i in range(arr_col_row + 1, len(rows)):
                r = rows[i]
                sku = r.get("B")
                if not sku:
                    continue
                sku_str = str(sku).strip()
                if sku_str.upper() in ["TOTAL", "TOTALS", ""]:
                    break  # end of section
                desc = str(r.get("C") or "")
                monthly = {}
                for k, mname in month_cols.items():
                    val = r.get(k)
                    if val is not None:
                        try:
                            monthly[mname] = int(float(val))
                        except (ValueError, TypeError):
                            pass
                total = 0
                if total_col and r.get(total_col) is not None:
                    try:
                        total = int(float(r[total_col]))
                    except (ValueError, TypeError):
                        total = sum(monthly.values())
                else:
                    total = sum(monthly.values())
                arrival_schedule.append({
                    "sku": sku_str, "description": desc,
                    "monthlyArrivals": monthly, "total": total,
                })

        from datetime import date as _date
        po_data = {
            "purchaseOrders": purchase_orders, "unitsByItem": units_by_item,
            "arrivalSchedule": arrival_schedule,
            "lastUpload": _date.today().isoformat(), "sourceFile": file.filename
        }
        _save_factory_po(po_data)
        results["factoryPO"] = {"pos": len(purchase_orders), "items": len(units_by_item)}
        results["factoryPOLastUpload"] = po_data["lastUpload"]

    # Try Logistics
    # Case-insensitive sheet name matching for Logistics
    logistics_sheet = None
    for sn in wb.sheetnames:
        if sn.strip().lower() == "logistics tracking":
            logistics_sheet = sn
            break
    if logistics_sheet:
        ws = wb[logistics_sheet]
        rows = []
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row):
            r = {}
            for c in row:
                if hasattr(c, 'column_letter') and c.value is not None:
                    r[c.column_letter] = c.value
            rows.append(r)

        def to_str(v):
            if v is None: return ""
            from datetime import datetime, date
            if isinstance(v, (datetime, date)):
                return v.strftime("%Y-%m-%d")
            return str(v)

        def safe_round_l(v, n=2):
            try: return round(float(v), n)
            except: return 0

        def safe_int(v):
            try: return int(float(v))
            except: return 0

        # ── STOP WORDS: these indicate we've left the shipments section ──
        _STOP_WORDS = {"SHIPMENT STATUS SUMMARY", "STATUS SUMMARY", "UPCOMING ARRIVALS",
                        "TOTAL", "TOTALS", "ITEM SUMMARY", "PO#"}

        # ── Parse Shipments ──
        shipments = []
        header_row = None
        for i, r in enumerate(rows):
            vals = [str(v).upper() for v in r.values()]
            joined = " ".join(vals)
            if ("SHIPPER" in joined or "FACTORY" in joined) and ("HBL" in joined or "HB" in joined or "MODE" in joined):
                header_row = i
                break

        if header_row is not None:
            empty_count = 0
            for i in range(header_row + 1, len(rows)):
                r = rows[i]
                shipper = r.get("B")
                if not shipper:
                    empty_count += 1
                    if empty_count >= 2:
                        break  # two empty rows in a row = end of section
                    continue
                empty_count = 0

                shipper_str = str(shipper).strip()
                shipper_upper = shipper_str.upper()

                # Stop at summary/status rows
                if any(sw in shipper_upper for sw in _STOP_WORDS):
                    break
                # Skip re-header rows
                if shipper_upper in ["SHIPPER", "FACTORY", "STATUS", ""]:
                    continue
                # A real shipper name should be > 3 chars and not a number
                if len(shipper_str) <= 3:
                    continue

                hbl = str(r.get("D") or "").strip()
                # HBL should look like a tracking number (letters+digits, > 5 chars)
                if not hbl or len(hbl) < 5:
                    continue

                # Infer status from delivery date
                delivery_raw = r.get("N")
                if delivery_raw and str(delivery_raw).strip():
                    status = "Delivered"
                else:
                    eta_raw = r.get("J")
                    if eta_raw:
                        status = "In Transit"
                    else:
                        status = "Pending"

                shipments.append({
                    "shipper": shipper_str,                            # B = Factory/Shipper
                    "factory": str(r.get("C") or "").strip(),          # C = Shipped (brand)
                    "hbl": hbl,                                        # D = HB#
                    "containerType": str(r.get("E") or "").strip(),    # E = Type
                    "containerNumber": str(r.get("F") or "").strip(),  # F = CNTR#
                    "vesselVoyage": str(r.get("G") or "").strip(),     # G = Vessel
                    "etdOrigin": to_str(r.get("H")),                   # H = ETD
                    "departurePort": str(r.get("I") or "").strip(),    # I = ETD PORT
                    "etaDischarge": to_str(r.get("J")),                # J = ETA
                    "arrivalPort": str(r.get("K") or "").strip(),      # K = ETA PORT
                    "etaFinal": to_str(r.get("L")),                    # L = ETA Dest. (date)
                    "finalLocation": str(r.get("M") or "").strip(),    # M = ETA Dest. (location)
                    "deliveryDate": to_str(r.get("N")),                # N = Actual Delivery
                    "comments": str(r.get("O") or "").strip(),         # O = Comments
                    "status": status,
                })

        # ── Parse Items by Container ──
        # Find the "ITEM SUMMARY BY CONTAINER" header row
        item_header_row = None
        for i, r in enumerate(rows):
            for _k, v in r.items():
                if v and "container" in str(v).lower() and "item" in str(v).lower():
                    item_header_row = i
                    break
            if item_header_row is not None:
                break

        items = []
        if item_header_row is not None:
            # Find the column-header row (Container #, Invoice, ETA, PO#, Item Number, Description, ...)
            col_hdr = item_header_row + 1
            # Scan for actual column header with "Container" and "Item"
            for ci in range(item_header_row, min(item_header_row + 4, len(rows))):
                vals_up = [str(v).upper() for v in rows[ci].values()]
                joined = " ".join(vals_up)
                if "CONTAINER" in joined and ("ITEM" in joined or "SKU" in joined):
                    col_hdr = ci
                    break

            cur_container = ""
            for i in range(col_hdr + 1, len(rows)):
                r = rows[i]
                container_val = r.get("B")
                sku_val = r.get("F")
                desc_val = r.get("G")
                qty_val = r.get("J")

                # Skip completely empty rows
                if not container_val and not sku_val and not desc_val:
                    continue

                # Build string versions for checking
                container_str = str(container_val or "").strip()
                sku_str = str(sku_val or "").strip()
                desc_str = str(desc_val or "").strip()

                # Skip section headers and summary rows
                skip_words = ["CONTAINER TOTAL", "GOLF TOTAL", "GRAND TOTAL",
                              "NON-GOLF", "CONTAINER #", "ITEM NUMBER",
                              "ITEM SUMMARY"]
                combined_upper = f"{container_str} {sku_str} {desc_str}".upper()
                if any(sw in combined_upper for sw in skip_words):
                    # But update current container if this looks like a section header with a container #
                    continue

                # Update current container number (long alphanumeric string)
                if container_val and len(container_str) >= 8 and container_str[0].isalpha():
                    cur_container = container_str

                # Must have an actual SKU to be a valid item row
                if not sku_val or len(sku_str) < 3:
                    continue

                items.append({
                    "containerNumber": cur_container,
                    "invoice": str(r.get("C") or "").strip(),
                    "eta": to_str(r.get("D")),
                    "po": str(r.get("E") or "").strip(),
                    "sku": sku_str,
                    "description": desc_str,
                    "qty": safe_int(qty_val),
                })

        from datetime import date as _date
        log_data = {"shipments": shipments, "itemsByContainer": items,
            "lastUpload": _date.today().isoformat(), "sourceFile": file.filename}
        _save_logistics(log_data)
        results["logistics"] = {"shipments": len(shipments), "items": len(items)}
        results["logisticsLastUpload"] = log_data["lastUpload"]

    if "factoryPO" not in results and "logistics" not in results:
        raise HTTPException(400, f"No 'Factory PO Summary' or 'Logistics Tracking' sheet found. Available sheets: {wb.sheetnames}")

    results["lastUpload"] = _date.today().isoformat()
    return results


def _save_factory_po(data):
    fp = DB_DIR / "factory_po_summary.json"
    with open(fp, "w") as f:
        json.dump(data, f, indent=2)
