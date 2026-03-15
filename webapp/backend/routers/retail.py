"""
Retail Reporting router — Walmart POS data from Scintilla reports.

Upload endpoints parse Excel files and load into walmart_* tables.
Query endpoints serve data to the Retail Reporting frontend page.
"""
import logging
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO

from fastapi import APIRouter, UploadFile, File, Query, HTTPException
from core.database import get_db, get_db_rw

logger = logging.getLogger("golfgen")
router = APIRouter()

CT = ZoneInfo("America/Chicago")

# ── Brand → division mapping ────────────────────────────────────────────
BRAND_DIVISION = {
    "pga tour": "golf",
    "lpga": "golf",
    "holiday time": "housewares",
    "way to celebrate": "housewares",
}

def _brand_to_division(brand_name: str) -> str:
    if not brand_name:
        return "golf"
    return BRAND_DIVISION.get(brand_name.strip().lower(), "golf")

def _n(v):
    """Coerce value to float, returning 0 for None/empty."""
    if v is None or v == "":
        return 0.0
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
        return float(v)
    except (TypeError, ValueError):
        return 0.0

def _safe_int(v):
    if v is None or v == "":
        return 0
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0

def _safe_str(v, max_len=None):
    if v is None:
        return None
    s = str(v).strip()
    if max_len:
        s = s[:max_len]
    return s or None


# ═══════════════════════════════════════════════════════════════════════
#  UPLOAD ENDPOINT — auto-detect report type and parse
# ═══════════════════════════════════════════════════════════════════════

@router.post("/api/retail/upload")
async def upload_retail_report(file: UploadFile = File(...)):
    """Upload a Scintilla Excel report. Auto-detects report type."""
    import openpyxl

    upload_id = str(uuid.uuid4())[:8]
    fname = file.filename or "unknown.xlsx"
    data = await file.read()
    wb = openpyxl.load_workbook(BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    header_set = set(h for h in headers if h)

    # Auto-detect report type from headers
    report_type = None
    if "store_number" in header_set and "walmart_calendar_week" in header_set:
        report_type = "store_weekly"
    elif "prime_item_number" in header_set and "walmart_calendar_week" in header_set:
        report_type = "item_weekly"
    elif "prime_item_number" in header_set and "wm_time_window_week" in header_set:
        report_type = "item_period"
    elif "all_links_item_number" in header_set:
        report_type = "ecomm"
    elif "store_dc_nbr" in header_set:
        report_type = "order_forecast"
    else:
        # Check for scorecard (pivoted format — row 3 has TY/LY/Diff)
        row3 = [c.value for c in list(ws.iter_rows(min_row=3, max_row=3))[0]]
        if "TY" in row3 and "LY" in row3:
            report_type = "scorecard"

    wb.close()

    if not report_type:
        raise HTTPException(400, "Could not auto-detect report type from headers")

    # Re-open for parsing
    wb = openpyxl.load_workbook(BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    try:
        if report_type == "store_weekly":
            rows = _parse_store_weekly(ws, headers)
        elif report_type == "item_weekly":
            rows = _parse_item_weekly(ws, headers, period_type="weekly")
        elif report_type == "item_period":
            rows = _parse_item_weekly(ws, headers, period_type=None)  # read from data
        elif report_type == "scorecard":
            rows = _parse_scorecard(ws, upload_id)
        elif report_type == "ecomm":
            rows = _parse_ecomm(ws, headers)
        elif report_type == "order_forecast":
            rows = _parse_order_forecast(ws, headers)
        else:
            rows = 0
    except Exception as e:
        wb.close()
        _log_upload(upload_id, fname, report_type, 0, "ERROR", str(e))
        raise HTTPException(500, f"Parse error: {e}")

    wb.close()
    _log_upload(upload_id, fname, report_type, rows, "SUCCESS", None)

    return {
        "status": "ok",
        "upload_id": upload_id,
        "report_type": report_type,
        "rows_loaded": rows,
        "filename": fname,
    }


def _log_upload(upload_id, fname, report_type, rows, status, error):
    try:
        con = get_db_rw()
        con.execute("""
            INSERT INTO retail_upload_log
                (upload_id, filename, report_type, channel, rows_loaded, uploaded_by, status, error)
            VALUES (?, ?, ?, 'walmart_stores', ?, 'upload', ?, ?)
        """, [upload_id, fname, report_type, rows, status, error])
        con.close()
    except Exception as e:
        logger.error(f"Upload log error: {e}")


# ═══════════════════════════════════════════════════════════════════════
#  PARSERS
# ═══════════════════════════════════════════════════════════════════════

def _parse_store_weekly(ws, headers):
    """Parse Store Level Detail → walmart_store_weekly."""
    h = {v: i for i, v in enumerate(headers) if v}
    con = get_db_rw()
    count = 0
    batch = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        wk = _safe_str(row[h.get("walmart_calendar_week", 0)])
        yr = _safe_str(row[h.get("walmart_calendar_year", 1)])
        sn = _safe_str(row[h.get("store_number", 3)])
        if not wk or not sn:
            continue
        batch.append((
            wk, yr,
            _safe_str(row[h.get("store_name", 2)], 120),
            sn,
            _safe_str(row[h.get("vendor_department_number", 4)]),
            _n(row[h["pos_quantity_this_year"]]) if "pos_quantity_this_year" in h else 0,
            _n(row[h["pos_quantity_last_year"]]) if "pos_quantity_last_year" in h else 0,
            _n(row[h["pos_sales_this_year"]]) if "pos_sales_this_year" in h else 0,
            _n(row[h["pos_sales_last_year"]]) if "pos_sales_last_year" in h else 0,
            _n(row[h["gross_receipt_quantity_this_year"]]) if "gross_receipt_quantity_this_year" in h else 0,
            _n(row[h["gross_receipt_quantity_last_year"]]) if "gross_receipt_quantity_last_year" in h else 0,
            _n(row[h["net_receipt_quantity_this_year"]]) if "net_receipt_quantity_this_year" in h else 0,
            _n(row[h["net_receipt_quantity_last_year"]]) if "net_receipt_quantity_last_year" in h else 0,
            _n(row[h["total_store_customer_returns_quantity_this_year"]]) if "total_store_customer_returns_quantity_this_year" in h else 0,
            _n(row[h["total_store_customer_returns_quantity_last_year"]]) if "total_store_customer_returns_quantity_last_year" in h else 0,
            _n(row[h["total_store_customer_returns_quantity_defective_this_year"]]) if "total_store_customer_returns_quantity_defective_this_year" in h else 0,
            _n(row[h["total_store_customer_returns_quantity_defective_last_year"]]) if "total_store_customer_returns_quantity_defective_last_year" in h else 0,
            _n(row[h["store_on_hand_quantity_this_year"]]) if "store_on_hand_quantity_this_year" in h else 0,
            _n(row[h["store_on_hand_quantity_last_year"]]) if "store_on_hand_quantity_last_year" in h else 0,
            _n(row[h["current_clearance_on_hand_quantity_this_year"]]) if "current_clearance_on_hand_quantity_this_year" in h else 0,
            _n(row[h["current_clearance_on_hand_quantity_last_year"]]) if "current_clearance_on_hand_quantity_last_year" in h else 0,
            _n(row[h["store_in_transit_quantity_this_year"]]) if "store_in_transit_quantity_this_year" in h else 0,
            _n(row[h["store_in_transit_quantity_last_year"]]) if "store_in_transit_quantity_last_year" in h else 0,
            _n(row[h["store_in_warehouse_quantity_this_year"]]) if "store_in_warehouse_quantity_this_year" in h else 0,
            _n(row[h["store_in_warehouse_quantity_last_year"]]) if "store_in_warehouse_quantity_last_year" in h else 0,
            _n(row[h["instock_percentage_this_year"]]) if "instock_percentage_this_year" in h else None,
            _n(row[h["instock_percentage_last_year"]]) if "instock_percentage_last_year" in h else None,
            _n(row[h["repl_instock_percentage_this_year"]]) if "repl_instock_percentage_this_year" in h else None,
            _n(row[h["repl_instock_percentage_last_year"]]) if "repl_instock_percentage_last_year" in h else None,
            _n(row[h["store_returns_quantity_to_vendor_this_year"]]) if "store_returns_quantity_to_vendor_this_year" in h else 0,
            _n(row[h["store_returns_quantity_to_vendor_last_year"]]) if "store_returns_quantity_to_vendor_last_year" in h else 0,
            _n(row[h["store_returns_quantity_to_return_center_this_year"]]) if "store_returns_quantity_to_return_center_this_year" in h else 0,
            _n(row[h["store_returns_quantity_to_return_center_last_year"]]) if "store_returns_quantity_to_return_center_last_year" in h else 0,
            _n(row[h["store_returns_quantity_to_dc_this_year"]]) if "store_returns_quantity_to_dc_this_year" in h else 0,
            _n(row[h["store_returns_quantity_to_dc_last_year"]]) if "store_returns_quantity_to_dc_last_year" in h else 0,
            _n(row[h["store_returns_quantity_recall_this_year"]]) if "store_returns_quantity_recall_this_year" in h else 0,
            _n(row[h["store_returns_quantity_recall_last_year"]]) if "store_returns_quantity_recall_last_year" in h else 0,
            _n(row[h["inventory_adjustment_quantity_this_year"]]) if "inventory_adjustment_quantity_this_year" in h else 0,
            _n(row[h["inventory_adjustment_quantity_last_year"]]) if "inventory_adjustment_quantity_last_year" in h else 0,
            _safe_int(row[h["traited_store_count_this_year"]]) if "traited_store_count_this_year" in h else 0,
            _safe_int(row[h["traited_store_count_last_year"]]) if "traited_store_count_last_year" in h else 0,
        ))
        count += 1
        if len(batch) >= 500:
            _insert_store_weekly_batch(con, batch)
            batch = []

    if batch:
        _insert_store_weekly_batch(con, batch)
    con.close()
    return count


def _insert_store_weekly_batch(con, batch):
    for r in batch:
        con.execute("""
            INSERT OR IGNORE INTO walmart_store_weekly
                (walmart_week, walmart_year, store_name, store_number, vendor_dept_number,
                 pos_qty_ty, pos_qty_ly, pos_sales_ty, pos_sales_ly,
                 gross_receipt_qty_ty, gross_receipt_qty_ly,
                 net_receipt_qty_ty, net_receipt_qty_ly,
                 returns_qty_ty, returns_qty_ly,
                 returns_defective_qty_ty, returns_defective_qty_ly,
                 on_hand_qty_ty, on_hand_qty_ly,
                 clearance_on_hand_qty_ty, clearance_on_hand_qty_ly,
                 in_transit_qty_ty, in_transit_qty_ly,
                 in_warehouse_qty_ty, in_warehouse_qty_ly,
                 instock_pct_ty, instock_pct_ly,
                 repl_instock_pct_ty, repl_instock_pct_ly,
                 returns_to_vendor_qty_ty, returns_to_vendor_qty_ly,
                 returns_to_return_center_qty_ty, returns_to_return_center_qty_ly,
                 returns_to_dc_qty_ty, returns_to_dc_qty_ly,
                 returns_recall_qty_ty, returns_recall_qty_ly,
                 inv_adjustment_qty_ty, inv_adjustment_qty_ly,
                 traited_store_count_ty, traited_store_count_ly)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, list(r))


def _parse_item_weekly(ws, headers, period_type=None):
    """Parse Dashboard Report 1 or 2 → walmart_item_weekly."""
    h = {v: i for i, v in enumerate(headers) if v}
    # Detect week column: 'walmart_calendar_week' (Report 2) or 'wm_time_window_week' (Report 1)
    wk_col = "walmart_calendar_week" if "walmart_calendar_week" in h else "wm_time_window_week"
    con = get_db_rw()
    count = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        wk_val = _safe_str(row[h[wk_col]])
        if not wk_val:
            continue
        item = _safe_str(row[h.get("prime_item_number", 3)])
        if not item:
            continue

        # Determine period_type
        pt = period_type or wk_val  # Report 1: wk_val is 'L13W', 'L4W', etc.
        if pt not in ("weekly", "L1W", "L4W", "L13W", "L52W"):
            if wk_val and len(wk_val) >= 6 and wk_val[:4].isdigit():
                pt = "weekly"  # numeric week like 202507
            else:
                pt = wk_val  # keep as-is

        brand = _safe_str(row[h.get("brand_name", 1)])
        division = _brand_to_division(brand)
        sales_type = _safe_str(row[h.get("sales_type_description", 12)])
        adj_code = _safe_str(row[h.get("backroom_inventory_adjustment_type_code", 15)])

        con.execute("""
            INSERT OR IGNORE INTO walmart_item_weekly
                (walmart_week, period_type, brand_name, brand_owner_id,
                 prime_item_number, prime_item_desc, walmart_upc, warehouse_pack_upc,
                 product_description, sales_type, vendor_name, vendor_number,
                 vendor_pack_qty, vendor_pack_length, vendor_pack_height, vendor_pack_width,
                 pos_sales_ty, pos_sales_ly, pos_qty_ty, pos_qty_ly,
                 pos_store_count_ty, pos_store_count_ly,
                 units_per_store_ty, units_per_store_ly,
                 dollars_per_store_ty, dollars_per_store_ly,
                 gross_receipt_qty_ty, gross_receipt_qty_ly,
                 net_receipt_qty_ty, net_receipt_qty_ly,
                 net_receipt_cost_ty, net_receipt_cost_ly,
                 net_receipt_retail_ty, net_receipt_retail_ly,
                 returns_qty_ty, returns_qty_ly,
                 returns_amt_ty, returns_amt_ly,
                 returns_defective_qty_ty, returns_defective_qty_ly,
                 returns_defective_amt_ty, returns_defective_amt_ly,
                 on_hand_qty_ty, on_hand_qty_ly,
                 on_order_qty_ty, on_order_qty_ly,
                 in_transit_qty_ty, in_transit_qty_ly,
                 in_warehouse_qty_ty, in_warehouse_qty_ly,
                 clearance_on_hand_qty_ty, clearance_on_hand_qty_ly,
                 clearance_on_hand_eop_ty, clearance_on_hand_eop_ly,
                 instock_pct_ty, instock_pct_ly,
                 repl_instock_pct_ty, repl_instock_pct_ly,
                 traited_store_count_ty, traited_store_count_ly,
                 valid_store_count_ty, valid_store_count_ly,
                 inv_adjustment_qty_ty, inv_adjustment_qty_ly,
                 backroom_adj_qty, backroom_adj_type_code, backroom_adj_type_desc,
                 division, customer, platform)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [
            wk_val, pt, brand, _safe_str(row[h.get("brand_owner_id", 2)]),
            item, _safe_str(row[h.get("prime_item_description", 4)], 120),
            _safe_str(row[h.get("walmart_upc_number", 5)]),
            _safe_str(row[h.get("warehouse_pack_upc_number", 6)]),
            _safe_str(row[h.get("product_description", 7)], 120),
            sales_type,
            _safe_str(row[h.get("vendor_name", 13)], 80),
            _safe_str(row[h.get("vendor_number", 14)]),
            _n(row[h.get("vendor_pack_quantity", 8)]) if "vendor_pack_quantity" in h else None,
            _n(row[h.get("vendor_pack_length_quantity", 9)]) if "vendor_pack_length_quantity" in h else None,
            _n(row[h.get("vendor_pack_height_quantity", 10)]) if "vendor_pack_height_quantity" in h else None,
            _n(row[h.get("vendor_pack_width_quantity", 11)]) if "vendor_pack_width_quantity" in h else None,
            # POS
            _n(row[h["pos_sales_this_year"]]) if "pos_sales_this_year" in h else 0,
            _n(row[h["pos_sales_last_year"]]) if "pos_sales_last_year" in h else 0,
            _n(row[h["pos_quantity_this_year"]]) if "pos_quantity_this_year" in h else 0,
            _n(row[h["pos_quantity_last_year"]]) if "pos_quantity_last_year" in h else 0,
            _safe_int(row[h["pos_store_count_this_year"]]) if "pos_store_count_this_year" in h else 0,
            _safe_int(row[h["pos_store_count_last_year"]]) if "pos_store_count_last_year" in h else 0,
            # Velocity
            _n(row[h["units_per_str_with_sales_per_week_or_per_day_ty"]]) if "units_per_str_with_sales_per_week_or_per_day_ty" in h else 0,
            _n(row[h["units_per_str_with_sales_per_week_or_per_day_ly"]]) if "units_per_str_with_sales_per_week_or_per_day_ly" in h else 0,
            _n(row[h["dollar_per_str_with_sales_per_week_or_per_day_ty"]]) if "dollar_per_str_with_sales_per_week_or_per_day_ty" in h else 0,
            _n(row[h["dollar_per_str_with_sales_per_week_or_per_day_ly"]]) if "dollar_per_str_with_sales_per_week_or_per_day_ly" in h else 0,
            # Receipts
            _n(row[h["gross_receipt_quantity_this_year"]]) if "gross_receipt_quantity_this_year" in h else 0,
            _n(row[h["gross_receipt_quantity_last_year"]]) if "gross_receipt_quantity_last_year" in h else 0,
            _n(row[h["net_receipt_quantity_this_year"]]) if "net_receipt_quantity_this_year" in h else 0,
            _n(row[h["net_receipt_quantity_last_year"]]) if "net_receipt_quantity_last_year" in h else 0,
            _n(row[h["net_rcpt_cost_amnt_this_year"]]) if "net_rcpt_cost_amnt_this_year" in h else 0,
            _n(row[h["net_rcpt_cost_amnt_last_year"]]) if "net_rcpt_cost_amnt_last_year" in h else 0,
            _n(row[h["net_receipt_retail_amount_this_year"]]) if "net_receipt_retail_amount_this_year" in h else 0,
            _n(row[h["net_receipt_retail_amount_last_year"]]) if "net_receipt_retail_amount_last_year" in h else 0,
            # Returns
            _n(row[h["total_store_customer_returns_quantity_this_year"]]) if "total_store_customer_returns_quantity_this_year" in h else 0,
            _n(row[h["total_store_customer_returns_quantity_last_year"]]) if "total_store_customer_returns_quantity_last_year" in h else 0,
            _n(row[h["total_store_customer_returns_amount_this_year"]]) if "total_store_customer_returns_amount_this_year" in h else 0,
            _n(row[h["total_store_customer_returns_amount_last_year"]]) if "total_store_customer_returns_amount_last_year" in h else 0,
            _n(row[h["total_store_customer_returns_quantity_defective_this_year"]]) if "total_store_customer_returns_quantity_defective_this_year" in h else 0,
            _n(row[h["total_store_customer_returns_quantity_defective_last_year"]]) if "total_store_customer_returns_quantity_defective_last_year" in h else 0,
            _n(row[h["total_store_customer_returns_amount_defective_this_year"]]) if "total_store_customer_returns_amount_defective_this_year" in h else 0,
            _n(row[h["total_store_customer_returns_amount_defective_last_year"]]) if "total_store_customer_returns_amount_defective_last_year" in h else 0,
            # Inventory
            _n(row[h["store_on_hand_quantity_this_year"]]) if "store_on_hand_quantity_this_year" in h else 0,
            _n(row[h["store_on_hand_quantity_last_year"]]) if "store_on_hand_quantity_last_year" in h else 0,
            _n(row[h["store_on_order_quantity_this_year"]]) if "store_on_order_quantity_this_year" in h else 0,
            _n(row[h["store_on_order_quantity_last_year"]]) if "store_on_order_quantity_last_year" in h else 0,
            _n(row[h["store_in_transit_quantity_this_year"]]) if "store_in_transit_quantity_this_year" in h else 0,
            _n(row[h["store_in_transit_quantity_last_year"]]) if "store_in_transit_quantity_last_year" in h else 0,
            _n(row[h["store_in_warehouse_quantity_this_year"]]) if "store_in_warehouse_quantity_this_year" in h else 0,
            _n(row[h["store_in_warehouse_quantity_last_year"]]) if "store_in_warehouse_quantity_last_year" in h else 0,
            _n(row[h["current_clearance_on_hand_quantity_this_year"]]) if "current_clearance_on_hand_quantity_this_year" in h else 0,
            _n(row[h["current_clearance_on_hand_quantity_last_year"]]) if "current_clearance_on_hand_quantity_last_year" in h else 0,
            _n(row[h["current_clearance_on_hand_quantity_this_year_eop"]]) if "current_clearance_on_hand_quantity_this_year_eop" in h else 0,
            _n(row[h["current_clearance_on_hand_quantity_last_year_eop"]]) if "current_clearance_on_hand_quantity_last_year_eop" in h else 0,
            # Metrics
            _n(row[h["instock_percentage_this_year"]]) if "instock_percentage_this_year" in h else None,
            _n(row[h["instock_percentage_last_year"]]) if "instock_percentage_last_year" in h else None,
            _n(row[h["repl_instock_percentage_this_year"]]) if "repl_instock_percentage_this_year" in h else None,
            _n(row[h["repl_instock_percentage_last_year"]]) if "repl_instock_percentage_last_year" in h else None,
            _safe_int(row[h["traited_store_count_this_year"]]) if "traited_store_count_this_year" in h else 0,
            _safe_int(row[h["traited_store_count_last_year"]]) if "traited_store_count_last_year" in h else 0,
            _safe_int(row[h["valid_store_count_this_year"]]) if "valid_store_count_this_year" in h else 0,
            _safe_int(row[h["valid_store_count_last_year"]]) if "valid_store_count_last_year" in h else 0,
            # Adjustments
            _n(row[h["inventory_adjustment_quantity_this_year"]]) if "inventory_adjustment_quantity_this_year" in h else 0,
            _n(row[h["inventory_adjustment_quantity_last_year"]]) if "inventory_adjustment_quantity_last_year" in h else 0,
            _n(row[h["backroom_adjustment_quantity"]]) if "backroom_adjustment_quantity" in h else 0,
            adj_code,
            _safe_str(row[h.get("backroom_inventory_adjustment_type_description", 16)], 60),
            # Hierarchy
            division, "walmart_stores", "scintilla",
        ])
        count += 1
    con.close()
    return count


def _parse_scorecard(ws, upload_id):
    """Parse Weekly Scorecard (pivoted) → walmart_scorecard.

    Layout:
    Row 1: [None, 'Last Week', None, None, 'Current Month', ...]
    Row 2: [None, '(202552-202552 / ...)', None, None, '(202601-202604 / ...)', ...]
    Row 3: [None, 'TY', 'LY', 'Diff', 'TY', 'LY', 'Diff', ...]
    Row 4+: Section headers and metric rows
    """
    all_rows = list(ws.iter_rows(values_only=True))
    if len(all_rows) < 4:
        return 0

    # Parse period headers from rows 1-2
    period_row = all_rows[0]
    range_row = all_rows[1]
    periods = []
    for i in range(1, len(period_row), 3):
        pname = period_row[i]
        if pname:
            prange = range_row[i] if i < len(range_row) else None
            periods.append((pname, _safe_str(prange), i))

    con = get_db_rw()
    count = 0
    current_vendor = "All Vendors"
    current_group = ""

    for row in all_rows[3:]:
        label = _safe_str(row[0]) if row[0] else None
        if not label:
            continue

        # Detect vendor section header
        if label.startswith("Vendor Scorecard"):
            current_vendor = label.replace("Vendor Scorecard  - ", "").replace("Vendor Scorecard - ", "").strip()
            continue

        # Detect metric group
        if label in ("Store Sales", "Store Inventory", "DC Inventory", "Margins and Markdowns",
                      "Store Turns / GMROII", "eCommerce", "Supply Chain"):
            current_group = label
            continue

        # Otherwise it's a metric row
        for pname, prange, col_start in periods:
            ty_val = row[col_start] if col_start < len(row) else None
            ly_val = row[col_start + 1] if col_start + 1 < len(row) else None
            diff_val = row[col_start + 2] if col_start + 2 < len(row) else None

            if ty_val is None and ly_val is None:
                continue

            con.execute("""
                INSERT OR IGNORE INTO walmart_scorecard
                    (vendor_section, metric_group, metric_name, period, period_range,
                     value_ty, value_ly, value_diff, report_date, upload_id,
                     division, customer, platform)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'walmart_stores', 'scintilla')
            """, [
                current_vendor, current_group, label, pname, prange,
                _n(ty_val) if ty_val is not None else None,
                _n(ly_val) if ly_val is not None else None,
                _n(diff_val) if diff_val is not None else None,
                datetime.now(CT).date().isoformat(),
                upload_id,
            ])
            count += 1

    con.close()
    return count


def _parse_ecomm(ws, headers):
    """Parse eComm Sales Report → walmart_ecomm_weekly."""
    h = {v: i for i, v in enumerate(headers) if v}
    con = get_db_rw()
    count = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        wk = _safe_str(row[h.get("walmart_calendar_week", 0)])
        item_num = _safe_str(row[h.get("all_links_item_number", 1)])
        if not wk or not item_num:
            continue

        brand = _safe_str(row[h.get("brand_name", 12)])
        division = _brand_to_division(brand)
        vendor_num = _safe_str(row[h.get("vendor_number_9_digit", 4)]) or _safe_str(row[h.get("vendor_number", 92)])

        con.execute("""
            INSERT OR IGNORE INTO walmart_ecomm_weekly
                (walmart_week, all_links_item_number, product_name,
                 base_unit_retail_amount, vendor_number, vendor_name,
                 item_description, fineline_description,
                 walmart_upc, ecomm_upc,
                 omni_category, omni_subcategory, brand_name,
                 dept_description, dept_number,
                 auth_based_qty_ty, auth_based_qty_ly,
                 auth_based_net_sales_ty, auth_based_net_sales_ly,
                 shipped_based_qty_ty, shipped_based_qty_ly,
                 shipped_based_net_sales_ty, shipped_based_net_sales_ly,
                 vendor_pack_cost, vendor_stock_id,
                 season_code, season_description, item_status_code,
                 division, customer, platform)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [
            wk, item_num,
            _safe_str(row[h.get("product_name", 2)], 200),
            _n(row[h.get("base_unit_retail_amount", 3)]) if "base_unit_retail_amount" in h else None,
            vendor_num,
            _safe_str(row[h.get("vendor_name", 5)], 80),
            _safe_str(row[h.get("all_links_item_description", 6)], 120),
            _safe_str(row[h.get("fineline_description", 7)], 60),
            _safe_str(row[h.get("walmart_upc_number", 8)]),
            _safe_str(row[h.get("ecomm_upc_number", 9)]),
            _safe_str(row[h.get("omni_category_description", 10)], 60),
            _safe_str(row[h.get("omni_subcategory_description", 11)], 60),
            brand,
            _safe_str(row[h.get("accounting_department_description", 13)], 60),
            _safe_str(row[h.get("accounting_department_number", 14)]),
            # Sales metrics
            _n(row[h["auth_based_item_quantity_this_year"]]) if "auth_based_item_quantity_this_year" in h else 0,
            _n(row[h["auth_based_item_quantity_last_year"]]) if "auth_based_item_quantity_last_year" in h else 0,
            _n(row[h["auth_based_net_sales_amount_this_year"]]) if "auth_based_net_sales_amount_this_year" in h else 0,
            _n(row[h["auth_based_net_sales_amount_last_year"]]) if "auth_based_net_sales_amount_last_year" in h else 0,
            _n(row[h["shipped_based_quantity_this_year"]]) if "shipped_based_quantity_this_year" in h else 0,
            _n(row[h["shipped_based_quantity_last_year"]]) if "shipped_based_quantity_last_year" in h else 0,
            _n(row[h["shipped_based_net_sales_amount_this_year"]]) if "shipped_based_net_sales_amount_this_year" in h else 0,
            _n(row[h["shipped_based_net_sales_amount_last_year"]]) if "shipped_based_net_sales_amount_last_year" in h else 0,
            _n(row[h["vendor_pack_cost_amount"]]) if "vendor_pack_cost_amount" in h else None,
            _safe_str(row[h.get("vendor_stock_id", 95)]),
            _safe_str(row[h.get("season_code", 55)]),
            _safe_str(row[h.get("season_description", 56)], 40),
            _safe_str(row[h.get("item_status_code", 40)]),
            division, "walmart_stores", "scintilla",
        ])
        count += 1
    con.close()
    return count


def _parse_order_forecast(ws, headers):
    """Parse Order Forecast → walmart_order_forecast."""
    h = {v: i for i, v in enumerate(headers) if v}
    con = get_db_rw()
    today = datetime.now(CT).date().isoformat()
    count = 0
    for row in ws.iter_rows(min_row=2, values_only=True):
        dc = _safe_str(row[h.get("store_dc_nbr", 0)])
        if not dc:
            continue
        con.execute("""
            INSERT OR IGNORE INTO walmart_order_forecast
                (snapshot_date, store_dc_nbr, store_dc_type, vendor_dept_number)
            VALUES (?, ?, ?, ?)
        """, [
            today, dc,
            _safe_str(row[h.get("store_dc_type", 1)]),
            _safe_str(row[h.get("vendor_department_number", 2)]),
        ])
        count += 1
    con.close()
    return count


# ═══════════════════════════════════════════════════════════════════════
#  QUERY ENDPOINTS — serve data to frontend
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/retail/scorecard")
def get_scorecard(division: str = None, customer: str = None):
    """Return scorecard KPIs grouped by vendor and period."""
    con = get_db()
    from core.hierarchy import hierarchy_filter
    hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")
    rows = con.execute(f"""
        SELECT vendor_section, metric_group, metric_name, period, period_range,
               value_ty, value_ly, value_diff
        FROM walmart_scorecard
        WHERE 1=1 {hw}
        ORDER BY vendor_section, metric_group, metric_name, period
    """, hp).fetchall()
    con.close()
    return {"scorecard": [
        {"vendorSection": r[0], "metricGroup": r[1], "metricName": r[2],
         "period": r[3], "periodRange": r[4],
         "valueTy": _n(r[5]), "valueLy": _n(r[6]), "valueDiff": _n(r[7])}
        for r in rows
    ]}


@router.get("/api/retail/store-performance")
def get_store_performance(
    division: str = None, customer: str = None,
    week: str = None, limit: int = 100, offset: int = 0,
    sort_by: str = "pos_sales_ty", sort_dir: str = "desc",
):
    """Return store-level weekly data for the Store Performance tab."""
    con = get_db()
    from core.hierarchy import hierarchy_filter
    hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

    where = f"WHERE 1=1 {hw}"
    if week:
        where += " AND walmart_week = ?"
        hp.append(week)

    allowed_sorts = {
        "pos_sales_ty", "pos_qty_ty", "instock_pct_ty", "returns_qty_ty",
        "on_hand_qty_ty", "store_name", "store_number", "walmart_week",
    }
    sb = sort_by if sort_by in allowed_sorts else "pos_sales_ty"
    sd = "ASC" if sort_dir.lower() == "asc" else "DESC"

    rows = con.execute(f"""
        SELECT walmart_week, walmart_year, store_name, store_number,
               pos_qty_ty, pos_qty_ly, pos_sales_ty, pos_sales_ly,
               returns_qty_ty, returns_qty_ly,
               on_hand_qty_ty, on_hand_qty_ly,
               instock_pct_ty, instock_pct_ly,
               repl_instock_pct_ty, repl_instock_pct_ly,
               in_transit_qty_ty, in_warehouse_qty_ty,
               traited_store_count_ty
        FROM walmart_store_weekly
        {where}
        ORDER BY {sb} {sd}
        LIMIT ? OFFSET ?
    """, hp + [limit, offset]).fetchall()

    total = con.execute(f"SELECT COUNT(*) FROM walmart_store_weekly {where}", hp).fetchone()[0]
    con.close()

    # Get distinct weeks for filter dropdown
    con2 = get_db()
    weeks = [r[0] for r in con2.execute(
        "SELECT DISTINCT walmart_week FROM walmart_store_weekly ORDER BY walmart_week DESC"
    ).fetchall()]
    con2.close()

    return {
        "stores": [
            {"week": r[0], "year": r[1], "storeName": r[2], "storeNumber": r[3],
             "posQtyTy": _n(r[4]), "posQtyLy": _n(r[5]),
             "posSalesTy": _n(r[6]), "posSalesLy": _n(r[7]),
             "returnsQtyTy": _n(r[8]), "returnsQtyLy": _n(r[9]),
             "onHandQtyTy": _n(r[10]), "onHandQtyLy": _n(r[11]),
             "instockPctTy": _n(r[12]), "instockPctLy": _n(r[13]),
             "replInstockPctTy": _n(r[14]), "replInstockPctLy": _n(r[15]),
             "inTransitQtyTy": _n(r[16]), "inWarehouseQtyTy": _n(r[17]),
             "traitedStoreCountTy": _n(r[18])}
            for r in rows
        ],
        "total": int(total),
        "weeks": weeks,
    }


@router.get("/api/retail/item-performance")
def get_item_performance(
    division: str = None, customer: str = None,
    period_type: str = "weekly", week: str = None,
    limit: int = 100, offset: int = 0,
    sort_by: str = "pos_sales_ty", sort_dir: str = "desc",
):
    """Return item-level data for the Item Performance tab."""
    con = get_db()
    from core.hierarchy import hierarchy_filter
    hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

    where = f"WHERE period_type = ? {hw}"
    params = [period_type] + hp
    if week:
        where += " AND walmart_week = ?"
        params.append(week)

    allowed_sorts = {
        "pos_sales_ty", "pos_qty_ty", "units_per_store_ty", "returns_qty_ty",
        "on_hand_qty_ty", "instock_pct_ty", "prime_item_number",
    }
    sb = sort_by if sort_by in allowed_sorts else "pos_sales_ty"
    sd = "ASC" if sort_dir.lower() == "asc" else "DESC"

    rows = con.execute(f"""
        SELECT walmart_week, prime_item_number, prime_item_desc, brand_name,
               sales_type, vendor_name,
               pos_sales_ty, pos_sales_ly, pos_qty_ty, pos_qty_ly,
               pos_store_count_ty, units_per_store_ty, units_per_store_ly,
               dollars_per_store_ty, dollars_per_store_ly,
               returns_qty_ty, returns_qty_ly,
               on_hand_qty_ty, on_hand_qty_ly,
               instock_pct_ty, instock_pct_ly,
               repl_instock_pct_ty, repl_instock_pct_ly,
               net_receipt_qty_ty, net_receipt_qty_ly
        FROM walmart_item_weekly
        {where}
        ORDER BY {sb} {sd}
        LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()

    total = con.execute(f"SELECT COUNT(*) FROM walmart_item_weekly {where}", params).fetchone()[0]
    con.close()

    return {
        "items": [
            {"week": r[0], "itemNumber": r[1], "itemDesc": r[2], "brand": r[3],
             "salesType": r[4], "vendor": r[5],
             "posSalesTy": _n(r[6]), "posSalesLy": _n(r[7]),
             "posQtyTy": _n(r[8]), "posQtyLy": _n(r[9]),
             "posStoreCountTy": _n(r[10]),
             "unitsPerStoreTy": _n(r[11]), "unitsPerStoreLy": _n(r[12]),
             "dollarsPerStoreTy": _n(r[13]), "dollarsPerStoreLy": _n(r[14]),
             "returnsQtyTy": _n(r[15]), "returnsQtyLy": _n(r[16]),
             "onHandQtyTy": _n(r[17]), "onHandQtyLy": _n(r[18]),
             "instockPctTy": _n(r[19]), "instockPctLy": _n(r[20]),
             "replInstockPctTy": _n(r[21]), "replInstockPctLy": _n(r[22]),
             "netReceiptQtyTy": _n(r[23]), "netReceiptQtyLy": _n(r[24])}
            for r in rows
        ],
        "total": int(total),
    }


@router.get("/api/retail/ecomm")
def get_ecomm(
    division: str = None, customer: str = None,
    week: str = None, limit: int = 100, offset: int = 0,
):
    """Return eComm sales data."""
    con = get_db()
    from core.hierarchy import hierarchy_filter
    hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

    where = f"WHERE 1=1 {hw}"
    if week:
        where += " AND walmart_week = ?"
        hp.append(week)

    rows = con.execute(f"""
        SELECT walmart_week, all_links_item_number, product_name, brand_name,
               base_unit_retail_amount,
               auth_based_qty_ty, auth_based_qty_ly,
               auth_based_net_sales_ty, auth_based_net_sales_ly,
               shipped_based_qty_ty, shipped_based_qty_ly,
               shipped_based_net_sales_ty, shipped_based_net_sales_ly
        FROM walmart_ecomm_weekly
        {where}
        ORDER BY auth_based_net_sales_ty DESC
        LIMIT ? OFFSET ?
    """, hp + [limit, offset]).fetchall()

    total = con.execute(f"SELECT COUNT(*) FROM walmart_ecomm_weekly {where}", hp).fetchone()[0]
    con.close()

    return {
        "items": [
            {"week": r[0], "itemNumber": r[1], "productName": r[2], "brand": r[3],
             "retailAmount": _n(r[4]),
             "authQtyTy": _n(r[5]), "authQtyLy": _n(r[6]),
             "authSalesTy": _n(r[7]), "authSalesLy": _n(r[8]),
             "shippedQtyTy": _n(r[9]), "shippedQtyLy": _n(r[10]),
             "shippedSalesTy": _n(r[11]), "shippedSalesLy": _n(r[12])}
            for r in rows
        ],
        "total": int(total),
    }


@router.get("/api/retail/order-forecast")
def get_order_forecast(division: str = None, customer: str = None):
    """Return latest order forecast snapshot."""
    con = get_db()
    rows = con.execute("""
        SELECT snapshot_date, store_dc_nbr, store_dc_type, vendor_dept_number
        FROM walmart_order_forecast
        ORDER BY snapshot_date DESC, store_dc_nbr
        LIMIT 200
    """).fetchall()
    con.close()
    return {"forecast": [
        {"snapshotDate": str(r[0]), "storeDcNbr": r[1],
         "storeDcType": r[2], "vendorDeptNumber": r[3]}
        for r in rows
    ]}


@router.get("/api/retail/summary")
def get_retail_summary(division: str = None, customer: str = None):
    """Return high-level KPIs for the retail reporting overview."""
    con = get_db()
    from core.hierarchy import hierarchy_filter
    hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

    # Total rows loaded
    counts = {}
    for tbl in ["walmart_store_weekly", "walmart_item_weekly", "walmart_scorecard",
                "walmart_ecomm_weekly", "walmart_order_forecast"]:
        try:
            r = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
            counts[tbl] = int(r[0]) if r else 0
        except Exception:
            counts[tbl] = 0

    # Latest week with data
    latest_week = None
    try:
        r = con.execute("SELECT MAX(walmart_week) FROM walmart_store_weekly").fetchone()
        latest_week = r[0] if r else None
    except Exception:
        pass

    # Aggregate POS for latest week
    pos_summary = {"posSalesTy": 0, "posQtyTy": 0, "posSalesLy": 0, "posQtyLy": 0}
    if latest_week:
        try:
            r = con.execute(f"""
                SELECT SUM(pos_sales_ty), SUM(pos_qty_ty), SUM(pos_sales_ly), SUM(pos_qty_ly)
                FROM walmart_store_weekly
                WHERE walmart_week = ? {hw}
            """, [latest_week] + hp).fetchone()
            if r:
                pos_summary = {
                    "posSalesTy": _n(r[0]), "posQtyTy": _n(r[1]),
                    "posSalesLy": _n(r[2]), "posQtyLy": _n(r[3]),
                }
        except Exception:
            pass

    # Upload log
    uploads = []
    try:
        rows = con.execute("""
            SELECT upload_id, filename, report_type, rows_loaded, uploaded_at, status
            FROM retail_upload_log
            ORDER BY uploaded_at DESC
            LIMIT 20
        """).fetchall()
        uploads = [{"uploadId": r[0], "filename": r[1], "reportType": r[2],
                     "rowsLoaded": r[3], "uploadedAt": str(r[4]), "status": r[5]}
                    for r in rows]
    except Exception:
        pass

    con.close()
    return {
        "tableCounts": counts,
        "latestWeek": latest_week,
        "posSummary": pos_summary,
        "uploads": uploads,
    }


# ═══════════════════════════════════════════════════════════════════════
#  WALMART ANALYTICS COMPREHENSIVE ENDPOINT
# ═══════════════════════════════════════════════════════════════════════

@router.get("/api/retail/walmart-analytics")
def get_walmart_analytics(division: str = None, customer: str = None):
    """Return comprehensive data for the Walmart Analytics mockup page.

    This endpoint aggregates data from walmart_item_weekly, walmart_store_weekly,
    walmart_scorecard, walmart_ecomm_weekly, and walmart_order_forecast tables
    to support all 5 sections of the analytics page:
    1. Sales Performance (KPIs, sales by type, item-level, category breakdown)
    2. Inventory Health (KPIs, instock trend, OH per store, instock by type)
    3. Vendor Scorecard (grouped metrics)
    4. eCommerce (KPIs, item-level breakdown)
    5. Order Forecast (KPIs, forecasts)
    """
    con = get_db()
    from core.hierarchy import hierarchy_filter
    hw, hp = hierarchy_filter(division=division, customer=customer or "walmart_stores")

    # Determine if data is available
    data_available = True
    try:
        total_items = con.execute(
            f"SELECT COUNT(*) FROM walmart_item_weekly WHERE 1=1 {hw}", hp
        ).fetchone()
        data_available = total_items and total_items[0] > 0
    except Exception:
        data_available = False

    # ── 1. SALES PERFORMANCE ─────────────────────────────────────────────

    sales_performance = {
        "kpis": {},
        "salesByType": {},
        "items": [],
        "categories": {},
    }

    if data_available:
        # KPIs by period type (L52W, L26W, L13W, L4W, L1W/LW)
        period_types = ["L52W", "L26W", "L13W", "L4W", "L1W"]
        for pt in period_types:
            ui_period = "L52W" if pt == "L52W" else "L26W" if pt == "L26W" else "L13W" if pt == "L13W" else "L4W" if pt == "L4W" else "LW"

            r = con.execute(f"""
                SELECT
                    SUM(pos_sales_ty), SUM(pos_sales_ly),
                    SUM(pos_qty_ty), SUM(pos_qty_ly),
                    SUM(CASE WHEN sales_type IN ('Regular', 'Rollback') THEN pos_sales_ty ELSE 0 END) as regular_ty,
                    SUM(CASE WHEN sales_type IN ('Regular', 'Rollback') THEN pos_sales_ly ELSE 0 END) as regular_ly,
                    SUM(CASE WHEN sales_type = 'Clearance' THEN pos_sales_ty ELSE 0 END) as clearance_ty,
                    SUM(CASE WHEN sales_type = 'Clearance' THEN pos_sales_ly ELSE 0 END) as clearance_ly
                FROM walmart_item_weekly
                WHERE period_type = ? AND 1=1 {hw}
            """, [pt] + hp).fetchone()

            if r:
                sales_performance["kpis"][ui_period] = {
                    "posSalesTy": _n(r[0]),
                    "posSalesLy": _n(r[1]),
                    "posQtyTy": _n(r[2]),
                    "posQtyLy": _n(r[3]),
                    "regularSalesTy": _n(r[4]),
                    "regularSalesLy": _n(r[5]),
                    "clearanceSalesTy": _n(r[6]),
                    "clearanceSalesLy": _n(r[7]),
                }

        # Sales by type breakdown for key periods
        for pt in ["L4W", "L13W", "L52W", "L1W"]:
            ui_period = "L52W" if pt == "L52W" else "L26W" if pt == "L26W" else "L13W" if pt == "L13W" else "L4W" if pt == "L4W" else "LW"

            rows = con.execute(f"""
                SELECT
                    COALESCE(sales_type, 'Other') as st,
                    SUM(pos_sales_ty) as sales_ty,
                    SUM(pos_sales_ly) as sales_ly,
                    SUM(pos_qty_ty) as qty_ty
                FROM walmart_item_weekly
                WHERE period_type = ? AND 1=1 {hw}
                GROUP BY COALESCE(sales_type, 'Other')
                ORDER BY sales_ty DESC
            """, [pt] + hp).fetchall()

            sales_performance["salesByType"][ui_period] = {}
            for row in rows:
                st = row[0] or "Other"
                sales_performance["salesByType"][ui_period][st] = {
                    "salesTy": _n(row[1]),
                    "salesLy": _n(row[2]),
                    "qtyTy": _n(row[3]),
                }

        # Item-level performance: aggregate across all periods
        item_rows = con.execute(f"""
            SELECT
                prime_item_desc,
                SUM(CASE WHEN period_type = 'L1W' THEN pos_sales_ty ELSE 0 END) as pos_sales_lw,
                SUM(CASE WHEN period_type = 'L1W' THEN pos_sales_ly ELSE 0 END) as pos_sales_lw_ly,
                SUM(CASE WHEN period_type = 'L1W' THEN pos_qty_ty ELSE 0 END) as pos_qty_lw,
                SUM(CASE WHEN period_type = 'L1W' THEN pos_qty_ly ELSE 0 END) as pos_qty_lw_ly,
                SUM(CASE WHEN period_type = 'L4W' THEN traited_store_count_ty ELSE 0 END) as traited_stores,
                SUM(CASE WHEN period_type = 'L4W' THEN on_hand_qty_ty ELSE 0 END) as on_hand_ty,
                SUM(CASE WHEN period_type = 'L4W' THEN on_hand_qty_ly ELSE 0 END) as on_hand_ly,
                SUM(CASE WHEN period_type = 'L4W' THEN instock_pct_ty ELSE 0 END) as instock_avg
            FROM walmart_item_weekly
            WHERE prime_item_desc IS NOT NULL AND 1=1 {hw}
            GROUP BY prime_item_desc
            ORDER BY pos_sales_lw DESC
            LIMIT 50
        """, hp).fetchall()

        for row in item_rows:
            aur = _n(row[1]) / _n(row[3]) if _n(row[3]) > 0 else 0
            aur_ly = _n(row[2]) / _n(row[4]) if _n(row[4]) > 0 else 0

            sales_performance["items"].append({
                "itemDesc": row[0],
                "posSalesTy": _n(row[1]),
                "posSalesLy": _n(row[2]),
                "posQtyTy": _n(row[3]),
                "posQtyLy": _n(row[4]),
                "traitedStores": _safe_int(row[5]),
                "onHandTy": _n(row[6]),
                "onHandLy": _n(row[7]),
                "aur": aur,
                "aurLy": aur_ly,
                "instockPct": _n(row[8]),
            })

        # Category breakdown based on item descriptions
        category_keywords = {
            "Junior Clubs": ["junior", "youth", "kid"],
            "Tee-Up Practice": ["tee", "practice", "range", "training"],
            "Balls": ["ball", "golf ball"],
        }

        category_data = {}
        for item in sales_performance["items"]:
            desc_lower = (item["itemDesc"] or "").lower()
            found_cat = "Other"
            for cat, keywords in category_keywords.items():
                if any(kw in desc_lower for kw in keywords):
                    found_cat = cat
                    break

            if found_cat not in category_data:
                category_data[found_cat] = {"salesTy": 0, "salesLy": 0, "qtyTy": 0, "count": 0}
            category_data[found_cat]["salesTy"] += item["posSalesTy"]
            category_data[found_cat]["salesLy"] += item["posSalesLy"]
            category_data[found_cat]["qtyTy"] += item["posQtyTy"]
            category_data[found_cat]["count"] += 1

        sales_performance["categories"] = category_data

    # ── 2. INVENTORY HEALTH ──────────────────────────────────────────────

    inventory_health = {
        "kpis": {},
        "instockTrend": {},
        "ohPerStore": {},
        "instockByType": {},
    }

    if data_available:
        # Inventory KPIs
        r = con.execute(f"""
            SELECT
                SUM(on_hand_qty_ty),
                AVG(instock_pct_ty),
                SUM(on_hand_qty_ty * 0.5),
                COUNT(DISTINCT prime_item_desc)
            FROM walmart_item_weekly
            WHERE period_type = 'L4W' AND 1=1 {hw}
        """, hp).fetchone()

        if r:
            inventory_health["kpis"] = {
                "totalStoreOh": _n(r[0]),
                "instockPct": _n(r[1]) if r[1] else 0,
                "retailInventory": _n(r[2]),
                "weeksOfSupply": 4.0,
            }

        # Instock trend by item across windows (L52W → L1W)
        items_trend = con.execute(f"""
            SELECT DISTINCT prime_item_desc
            FROM walmart_item_weekly
            WHERE period_type IN ('L1W', 'L4W', 'L13W', 'L26W', 'L52W') AND 1=1 {hw}
            LIMIT 30
        """, hp).fetchall()

        for item_row in items_trend:
            item_desc = item_row[0]
            if not item_desc:
                continue

            trend_vals = []
            for pt in ["L52W", "L26W", "L13W", "L4W", "L1W"]:
                r = con.execute(f"""
                    SELECT AVG(instock_pct_ty)
                    FROM walmart_item_weekly
                    WHERE period_type = ? AND prime_item_desc = ? AND 1=1 {hw}
                """, [pt] + hp).fetchone()
                trend_vals.append(_n(r[0]) if r and r[0] else 0)

            inventory_health["instockTrend"][item_desc] = trend_vals

        # OH per traited store by window
        for pt in ["L1W", "L4W", "L13W", "L52W"]:
            ui_period = "LW" if pt == "L1W" else pt

            rows = con.execute(f"""
                SELECT
                    prime_item_desc,
                    on_hand_qty_ty / NULLIF(traited_store_count_ty, 0) as oh_per_store_ty,
                    on_hand_qty_ly / NULLIF(traited_store_count_ly, 0) as oh_per_store_ly
                FROM walmart_item_weekly
                WHERE period_type = ? AND traited_store_count_ty > 0 AND 1=1 {hw}
                ORDER BY on_hand_qty_ty DESC
                LIMIT 20
            """, [pt] + hp).fetchall()

            if rows:
                inventory_health["ohPerStore"][ui_period] = {
                    "ty": [_n(r[1]) for r in rows],
                    "ly": [_n(r[2]) for r in rows],
                }

        # Instock by sales type
        for pt in ["L4W", "L13W"]:
            rows = con.execute(f"""
                SELECT
                    COALESCE(sales_type, 'Other') as st,
                    AVG(instock_pct_ty) as instock_avg
                FROM walmart_item_weekly
                WHERE period_type = ? AND 1=1 {hw}
                GROUP BY COALESCE(sales_type, 'Other')
            """, [pt] + hp).fetchall()

            inventory_health["instockByType"][pt] = {}
            for row in rows:
                inventory_health["instockByType"][pt][row[0]] = _n(row[1])

    # ── 3. VENDOR SCORECARD ──────────────────────────────────────────────

    scorecard_rows = con.execute(f"""
        SELECT
            vendor_section, metric_group, metric_name, period, period_range,
            value_ty, value_ly, value_diff
        FROM walmart_scorecard
        WHERE 1=1 {hw}
        ORDER BY vendor_section, metric_group, metric_name, period
    """, hp).fetchall()

    scorecard = [
        {
            "vendorSection": r[0],
            "metricGroup": r[1],
            "metricName": r[2],
            "period": r[3],
            "periodRange": r[4],
            "valueTy": _n(r[5]),
            "valueLy": _n(r[6]),
            "valueDiff": _n(r[7]),
        }
        for r in scorecard_rows
    ]

    # ── 4. eCOMMERCE ─────────────────────────────────────────────────────

    ecommerce = {
        "kpis": {},
        "items": [],
    }

    if data_available:
        # eComm KPIs
        r = con.execute(f"""
            SELECT
                SUM(auth_based_net_sales_ty),
                SUM(auth_based_net_sales_ly),
                SUM(shipped_based_net_sales_ty),
                SUM(shipped_based_net_sales_ly),
                COUNT(DISTINCT all_links_item_number)
            FROM walmart_ecomm_weekly
            WHERE 1=1 {hw}
        """, hp).fetchone()

        if r:
            ecommerce["kpis"] = {
                "authSalesTy": _n(r[0]),
                "authSalesLy": _n(r[1]),
                "shippedSalesTy": _n(r[2]),
                "shippedSalesLy": _n(r[3]),
                "totalItems": _safe_int(r[4]),
            }

        # Item-level eComm data
        ecomm_items = con.execute(f"""
            SELECT
                product_name,
                brand_name,
                base_unit_retail_amount,
                auth_based_net_sales_ty,
                auth_based_net_sales_ly,
                shipped_based_net_sales_ty,
                shipped_based_net_sales_ly
            FROM walmart_ecomm_weekly
            WHERE 1=1 {hw}
            ORDER BY auth_based_net_sales_ty DESC
            LIMIT 50
        """, hp).fetchall()

        ecommerce["items"] = [
            {
                "productName": r[0],
                "brand": r[1],
                "retailAmount": _n(r[2]),
                "authSalesTy": _n(r[3]),
                "authSalesLy": _n(r[4]),
                "shippedSalesTy": _n(r[5]),
                "shippedSalesLy": _n(r[6]),
            }
            for r in ecomm_items
        ]

    # ── 5. ORDER FORECAST ────────────────────────────────────────────────

    order_forecast = {
        "kpis": {},
        "items": [],
    }

    if data_available:
        # Latest snapshot
        r = con.execute("""
            SELECT COUNT(DISTINCT store_dc_nbr) as dc_count
            FROM walmart_order_forecast
            ORDER BY snapshot_date DESC
            LIMIT 1
        """).fetchone()

        if r:
            order_forecast["kpis"] = {
                "dcCount": _safe_int(r[0]),
            }

        # Forecast items
        forecast_items = con.execute("""
            SELECT DISTINCT snapshot_date, store_dc_nbr, store_dc_type
            FROM walmart_order_forecast
            ORDER BY snapshot_date DESC
            LIMIT 100
        """).fetchall()

        order_forecast["items"] = [
            {
                "snapshotDate": str(r[0]),
                "storeDcNbr": r[1],
                "storeDcType": r[2],
            }
            for r in forecast_items
        ]

    # ── Latest week for reference ────────────────────────────────────────
    latest_week = None
    try:
        r = con.execute("SELECT MAX(walmart_week) FROM walmart_item_weekly").fetchone()
        latest_week = r[0] if r else None
    except Exception:
        pass

    con.close()

    return {
        "salesPerformance": sales_performance,
        "inventoryHealth": inventory_health,
        "scorecard": scorecard,
        "ecommerce": ecommerce,
        "orderForecast": order_forecast,
        "latestWeek": latest_week,
        "dataAvailable": data_available,
    }


@router.post("/api/retail/pull-from-drive")
async def pull_from_drive():
    """Pull latest Scintilla reports from Google Drive folder.

    Stub for now — will implement Google Drive integration later.
    """
    return {
        "status": "not_implemented",
        "message": "Google Drive integration coming soon. Use manual upload for now.",
    }
