"""System, health, debug, and backup routes. v2"""
import os
import json
import logging
import asyncio
from core.database import get_db, get_db_rw
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from core.config import DB_PATH, DB_DIR, TIMEZONE, DOCS_DIR, USE_POSTGRES, DATABASE_URL

logger = logging.getLogger("golfgen")
router = APIRouter()


def _load_sp_api_credentials():
    """Load SP-API credentials from environment (matches sp_api.py logic)."""
    from services.sp_api import _load_sp_api_credentials as _load_creds
    return _load_creds()


@router.get("/api/health")
def health():
    now = datetime.now(ZoneInfo("America/Chicago"))
    db_engine = "postgresql" if USE_POSTGRES else "duckdb"
    db_info = {"engine": db_engine, "tables": {}}
    if USE_POSTGRES:
        # Show masked connection string (hide password)
        import re
        masked = re.sub(r'://[^@]+@', '://***@', DATABASE_URL) if DATABASE_URL else ""
        db_info["connection"] = masked
    else:
        db_info["path"] = str(DB_PATH)
        db_info["size_mb"] = round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0
    result = {
        "status": "healthy",
        "timestamp": now.isoformat(),
        "database": db_info,
        "last_sync": {},
        "has_sp_api_creds": _load_sp_api_credentials() is not None,
    }

    try:
        con = get_db_rw()
        # Table row counts
        for tbl in ["orders", "daily_sales", "financial_events", "fba_inventory", "advertising", "ads_campaigns"]:
            try:
                cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                result["database"]["tables"][tbl] = cnt
            except Exception:
                result["database"]["tables"][tbl] = 0

        # Last sync per job
        rows = con.execute("""
            SELECT job_name, started_at, status, records_processed
            FROM sync_log
            WHERE id IN (
                SELECT MAX(id) FROM sync_log GROUP BY job_name
            )
        """).fetchall()
        for row in rows:
            result["last_sync"][row[0]] = {
                "time": str(row[1])[:19] if row[1] else None,
                "status": row[2],
                "records": row[3] or 0,
            }
        con.close()
    except Exception as e:
        result["status"] = "degraded"
        result["error"] = str(e)

    return result


@router.post("/api/sync")
async def trigger_sync():
    """Manually trigger an SP-API data sync."""
    from services.sp_api import _run_sp_api_sync
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_sp_api_sync)
    return {"status": "sync_complete"}


@router.get("/api/sync")
async def trigger_sync_get():
    """Manually trigger sync. Fast: returns after today's orders are synced.
    Full sync (Sales Report) continues in background."""
    from services.sp_api import _sync_today_orders, _run_sp_api_sync
    import asyncio
    loop = asyncio.get_event_loop()
    # Fast: sync today's orders first (returns in ~5s)
    await loop.run_in_executor(None, _sync_today_orders)
    # Kick off full sync in background (don't wait for 5-min report poll)
    asyncio.get_event_loop().run_in_executor(None, _run_sp_api_sync)
    return {"status": "sync_complete", "today_synced": True}


@router.get("/api/sync/deep")
async def trigger_deep_sync():
    """Manually trigger the nightly deep sync (fills all data gaps + re-pulls last 30 days).
    WARNING: This can take 10-30 minutes depending on gap count. Runs in background."""
    from services.sync_engine import run_nightly_deep_sync
    import asyncio
    asyncio.get_event_loop().run_in_executor(None, run_nightly_deep_sync)
    return {"status": "deep_sync_started", "note": "Running in background. Check /api/debug/logs for progress."}


@router.get("/api/debug/today-orders")
def debug_today_orders():
    """Show raw order data for today to debug pricing issues."""
    try:
        from sp_api.api import Orders as OrdersAPI
        from sp_api.base import Marketplaces
        import time as _t

        creds = _load_sp_api_credentials()
        orders_api = OrdersAPI(credentials=creds, marketplace=Marketplaces.US)

        today_str = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y-%m-%d")
        after_date = (datetime.utcnow() - timedelta(days=1)).isoformat()

        response = orders_api.get_orders(
            CreatedAfter=after_date,
            MarketplaceIds=["ATVPDKIKX0DER"],
            MaxResultsPerPage=50
        )
        order_list = response.payload.get("Orders", [])

        results = []
        for order in order_list[:10]:  # limit to 10
            order_id = order.get("AmazonOrderId")
            status = order.get("OrderStatus")
            purchase_date = order.get("PurchaseDate", "")
            order_total = order.get("OrderTotal", {})

            # Try to get items
            items_info = []
            try:
                _t.sleep(0.5)
                items_resp = orders_api.get_order_items(order_id)
                for item in items_resp.payload.get("OrderItems", []):
                    items_info.append({
                        "asin": item.get("ASIN"),
                        "title": (item.get("Title") or "")[:60],
                        "qty": item.get("QuantityOrdered"),
                        "ItemPrice": item.get("ItemPrice"),
                        "ItemTax": item.get("ItemTax"),
                        "PromotionDiscount": item.get("PromotionDiscount"),
                    })
            except Exception as e:
                items_info = [{"error": str(e)}]

            results.append({
                "order_id": order_id,
                "status": status,
                "purchase_date": purchase_date,
                "OrderTotal": order_total,
                "items": items_info,
            })

        return {
            "today_pacific": today_str,
            "total_orders_fetched": len(order_list),
            "sample_orders": results,
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@router.get("/api/debug/financial-events")
def debug_financial_events():
    """Call Financial Events API directly and return raw summary for debugging."""
    try:
        from sp_api.api import Finances as FinancesAPI
        from sp_api.base import Marketplaces as _Mp
        creds = _load_sp_api_credentials()
        if not creds:
            return {"error": "No SP-API credentials found"}
        fin_start = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
        finances = FinancesAPI(credentials=creds, marketplace=_Mp.US)

        resp = finances.list_financial_events(PostedAfter=fin_start, MaxResultsPerPage=100)
        payload = resp.payload if hasattr(resp, 'payload') else (resp if isinstance(resp, dict) else {})
        events = payload.get("FinancialEvents", {})

        summary = {}
        sample_charge = None
        type_debug = {}  # Shows Python types for debugging
        for k, v in events.items():
            if isinstance(v, list):
                summary[k] = len(v)
                # Grab a sample charge structure from first shipment
                if k == "ShipmentEventList" and len(v) > 0 and not sample_charge:
                    first_event = v[0]
                    type_debug["shipment_event_type"] = type(first_event).__name__
                    items = first_event.get("ShipmentItemList", []) if isinstance(first_event, dict) else getattr(first_event, "ShipmentItemList", [])
                    if items:
                        first_item = items[0]
                        type_debug["shipment_item_type"] = type(first_item).__name__
                        charges = first_item.get("ItemChargeList", []) if isinstance(first_item, dict) else getattr(first_item, "ItemChargeList", [])
                        if charges:
                            first_charge = charges[0]
                            type_debug["charge_type"] = type(first_charge).__name__
                            ca = first_charge.get("ChargeAmount", None) if isinstance(first_charge, dict) else getattr(first_charge, "ChargeAmount", None)
                            type_debug["charge_amount_type"] = type(ca).__name__
                            type_debug["charge_amount_value"] = str(ca)
                            type_debug["charge_amount_is_dict"] = isinstance(ca, dict)
                            type_debug["charge_amount_has_attr"] = hasattr(ca, "CurrencyAmount")
                            sample_charge = first_charge
                            # Try to get the actual parsed value
                            if isinstance(ca, dict):
                                type_debug["parsed_amount"] = float(ca.get("CurrencyAmount", 0) or 0)
                            elif hasattr(ca, "CurrencyAmount"):
                                type_debug["parsed_amount"] = float(ca.CurrencyAmount or 0)
                            else:
                                type_debug["parsed_amount"] = f"unparseable: {ca}"

                        fees = first_item.get("ItemFeeList", []) if isinstance(first_item, dict) else getattr(first_item, "ItemFeeList", [])
                        if fees:
                            first_fee = fees[0]
                            fa = first_fee.get("FeeAmount", None) if isinstance(first_fee, dict) else getattr(first_fee, "FeeAmount", None)
                            type_debug["fee_amount_type"] = type(fa).__name__
                            type_debug["fee_amount_value"] = str(fa)

                    # Check PostedDate type
                    pd = first_event.get("PostedDate", None) if isinstance(first_event, dict) else getattr(first_event, "PostedDate", None)
                    type_debug["posted_date_type"] = type(pd).__name__
                    type_debug["posted_date_value"] = str(pd)[:30]

                if k == "RefundEventList" and len(v) > 0:
                    first = v[0]
                    summary["refund_sample_keys"] = list(first.keys()) if isinstance(first, dict) else [a for a in dir(first) if not a.startswith('_')]
                    summary["refund_sample_order_id"] = first.get("AmazonOrderId", "N/A") if isinstance(first, dict) else getattr(first, "AmazonOrderId", "N/A")
                    adj_items = (first.get("ShipmentItemAdjustmentList", []) if isinstance(first, dict)
                                else getattr(first, "ShipmentItemAdjustmentList", [])) or []
                    if adj_items:
                        first_adj = adj_items[0]
                        adj_charges = (first_adj.get("ItemChargeAdjustmentList", []) if isinstance(first_adj, dict)
                                      else getattr(first_adj, "ItemChargeAdjustmentList", [])) or []
                        summary["refund_sample_charge"] = adj_charges[0] if adj_charges else "no charges"
                        summary["refund_item_type"] = type(first_adj).__name__
            else:
                summary[k] = str(v)[:100]

        # Also check what's in the DB
        con = get_db_rw()
        db_counts = con.execute("""
            SELECT event_type, COUNT(*), SUM(ABS(product_charges)),
                   SUM(ABS(fba_fees)), SUM(ABS(commission))
            FROM financial_events
            GROUP BY event_type
        """).fetchall()
        con.close()

        has_next = "NextToken" in payload

        return {
            "api_date_range": fin_start,
            "payload_keys": list(payload.keys()),
            "event_type_counts": summary,
            "sample_charge_object": sample_charge,
            "type_debug": type_debug,
            "has_next_page": has_next,
            "db_records": [{"type": r[0], "count": r[1], "total_charges": round(r[2], 2),
                           "fba_fees": round(r[3], 2), "commission": round(r[4], 2)} for r in db_counts]
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@router.get("/api/backfill")
def backfill_historical(
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
):
    """Backfill historical Sales & Traffic data for a date range.
    Amazon limits report ranges to ~30 days, so this endpoint
    breaks the range into 30-day chunks and requests each one.
    This is a SLOW endpoint (each chunk takes 2-5 minutes for polling)."""
    from datetime import date as dt_date
    import time, gzip as gz, requests as req
    from sp_api.api import Reports
    from sp_api.base import Marketplaces, ReportType

    creds = _load_sp_api_credentials()
    reports = Reports(credentials=creds, marketplace=Marketplaces.US)

    start_dt = dt_date.fromisoformat(start)
    end_dt = dt_date.fromisoformat(end)

    total_records = 0
    chunks_done = 0
    errors = []

    chunk_start = start_dt
    while chunk_start < end_dt:
        chunk_end = min(chunk_start + timedelta(days=29), end_dt)
        logger.info(f"  Backfill requesting report {chunk_start} to {chunk_end}...")

        try:
            report_response = reports.create_report(
                reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
                dataStartTime=chunk_start.isoformat(),
                dataEndTime=chunk_end.isoformat(),
                reportOptions={"dateGranularity": "DAY", "asinGranularity": "CHILD"}
            )
            report_id = report_response.payload.get("reportId")

            report_data = None
            for attempt in range(30):
                time.sleep(10)
                status_response = reports.get_report(report_id)
                status = status_response.payload.get("processingStatus")
                if status == "DONE":
                    doc_id = status_response.payload.get("reportDocumentId")
                    doc_response = reports.get_report_document(doc_id)
                    report_data = doc_response.payload
                    break
                elif status in ("CANCELLED", "FATAL"):
                    errors.append(f"{chunk_start}-{chunk_end}: Report {status}")
                    break

            if report_data:
                if isinstance(report_data, dict) and "url" in report_data:
                    is_gzipped = report_data.get("compressionAlgorithm", "").upper() == "GZIP"
                    resp = req.get(report_data["url"])
                    if is_gzipped:
                        report_text = gz.decompress(resp.content).decode("utf-8")
                    else:
                        report_text = resp.text
                    report_data = json.loads(report_text)

                if isinstance(report_data, str):
                    report_data = json.loads(report_data)

                con = get_db_rw()
                records = 0

                for day_entry in report_data.get("salesAndTrafficByDate", []):
                    entry_date = day_entry.get("date", "")
                    traffic = day_entry.get("trafficByDate", {})
                    sales_info = day_entry.get("salesByDate", {})
                    ordered_sales = sales_info.get("orderedProductSales", {})
                    sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                    con.execute("""
                        INSERT OR REPLACE INTO daily_sales
                        (date, asin, units_ordered, ordered_product_sales,
                         sessions, session_percentage, page_views,
                         buy_box_percentage, unit_session_percentage)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        entry_date, "ALL",
                        int(sales_info.get("unitsOrdered", 0)),
                        sales_amount,
                        int(traffic.get("sessions", 0)),
                        float(traffic.get("sessionPercentage", 0)),
                        int(traffic.get("pageViews", 0)),
                        float(traffic.get("buyBoxPercentage", 0)),
                        float(traffic.get("unitSessionPercentage", 0)),
                    ])
                    records += 1

                for asin_entry in report_data.get("salesAndTrafficByAsin", []):
                    asin = asin_entry.get("childAsin") or asin_entry.get("parentAsin", "")
                    traffic = asin_entry.get("trafficByAsin", {})
                    sales_info = asin_entry.get("salesByAsin", {})
                    entry_date = asin_entry.get("date", str(chunk_start))
                    ordered_sales = sales_info.get("orderedProductSales", {})
                    sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                    con.execute("""
                        INSERT OR REPLACE INTO daily_sales
                        (date, asin, units_ordered, ordered_product_sales,
                         sessions, session_percentage, page_views,
                         buy_box_percentage, unit_session_percentage)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        entry_date, asin,
                        int(sales_info.get("unitsOrdered", 0)),
                        sales_amount,
                        int(traffic.get("sessions", 0)),
                        float(traffic.get("sessionPercentage", 0)),
                        int(traffic.get("pageViews", 0)),
                        float(traffic.get("buyBoxPercentage", 0)),
                        float(traffic.get("unitSessionPercentage", 0)),
                    ])
                    records += 1

                con.close()
                total_records += records
                chunks_done += 1
                logger.info(f"  Backfill chunk {chunk_start}-{chunk_end}: {records} records")

        except Exception as e:
            errors.append(f"{chunk_start}-{chunk_end}: {str(e)}")
            logger.error(f"  Backfill error {chunk_start}-{chunk_end}: {e}")

        chunk_start = chunk_end + timedelta(days=1)

    return {
        "status": "done",
        "chunks_processed": chunks_done,
        "total_records": total_records,
        "errors": errors,
        "range": f"{start} to {end}",
    }


@router.post("/api/sync/backfill-orders")
async def backfill_orders(
    request: Request,
    days: int = Query(90, description="How many days back to backfill")
):
    """Backfill historical orders using the Orders API (authenticated endpoint)."""
    from core.auth import get_session
    from services.sp_api import _backfill_orders

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, lambda: _backfill_orders(days=days))
    return result


@router.get("/api/system/status")
async def get_system_status(request: Request):
    """Get system status including last sync times, scheduler status, and recent logs."""
    from core.auth import get_session
    from core.scheduler import scheduler

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    con = get_db_rw()
    try:
        # Get last sync log entry
        last_sync = con.execute("""
            SELECT job_name, completed_at, status, execution_time_seconds
            FROM sync_log
            WHERE status IN ('completed', 'SUCCESS')
            ORDER BY completed_at DESC
            LIMIT 1
        """).fetchone()

        # Get last docs update
        last_docs = con.execute("""
            SELECT completed_at, status, documents_updated
            FROM docs_update_log
            WHERE status = 'completed'
            ORDER BY completed_at DESC
            LIMIT 1
        """).fetchone()

        # Get recent sync log entries (last 10)
        recent_syncs = con.execute("""
            SELECT job_name, started_at, completed_at, status, execution_time_seconds, error_message
            FROM sync_log
            ORDER BY started_at DESC
            LIMIT 10
        """).fetchall()

        # Get next scheduled jobs from APScheduler
        scheduler_status = {}
        if scheduler and scheduler.running:
            for job in scheduler.get_jobs():
                scheduler_status[job.id] = {
                    "next_run_time": str(job.next_run_time),
                    "trigger": str(job.trigger)
                }

        return {
            "status": "ok",
            "last_sync": {
                "job_name": last_sync[0] if last_sync else None,
                "completed_at": str(last_sync[1]) if last_sync else None,
                "status": last_sync[2] if last_sync else None,
                "execution_time_seconds": last_sync[3] if last_sync else None
            },
            "last_docs_update": {
                "completed_at": str(last_docs[0]) if last_docs else None,
                "status": last_docs[1] if last_docs else None,
                "documents_updated": last_docs[2] if last_docs else None
            },
            "recent_syncs": [
                {
                    "job_name": row[0],
                    "started_at": str(row[1]),
                    "completed_at": str(row[2]),
                    "status": row[3],
                    "execution_time_seconds": row[4],
                    "error_message": row[5]
                }
                for row in recent_syncs
            ],
            "scheduler_status": scheduler_status,
            "scheduler_running": scheduler.running if scheduler else False
        }
    finally:
        con.close()


@router.get("/api/system/sync-log")
async def get_sync_log(request: Request, limit: int = Query(50, ge=1, le=500)):
    """Get sync log entries."""
    from core.auth import get_session
    from fastapi import HTTPException

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")

    con = get_db()
    try:
        rows = con.execute("""
            SELECT id, job_name, started_at, completed_at, status,
                   records_processed, error_message, execution_time_seconds
            FROM sync_log
            ORDER BY started_at DESC
            LIMIT ?
        """, [limit]).fetchall()

        return {
            "entries": [
                {
                    "id": row[0],
                    "job_name": row[1],
                    "started_at": str(row[2]),
                    "completed_at": str(row[3]),
                    "status": row[4],
                    "records_processed": row[5] or 0,
                    "error_message": row[6],
                    "execution_time_seconds": round(row[7], 1) if row[7] else None,
                }
                for row in rows
            ]
        }
    finally:
        con.close()


@router.get("/api/system/data-coverage")
async def get_data_coverage(request: Request):
    """Return monthly data coverage grid, sync job health, and API report budget."""
    import calendar as cal
    from datetime import date as dt_date
    from core.auth import get_session
    from fastapi import HTTPException

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")

    today = datetime.now(ZoneInfo("America/Chicago")).date()
    yesterday = today - timedelta(days=1)
    backfill_start = dt_date(2024, 4, 1)

    con = get_db()
    try:
        # ── All daily_sales dates (ALL-level aggregates) ──────────────────────
        rows = con.execute(
            "SELECT DISTINCT date FROM daily_sales WHERE asin = 'ALL' AND date >= '2024-04-01'"
        ).fetchall()
        existing_dates: set = set()
        for r in rows:
            d = r[0]
            if isinstance(d, str):
                d = datetime.strptime(d, "%Y-%m-%d").date()
            elif hasattr(d, "date"):
                d = d.date()
            existing_dates.add(d)

        # ── Monthly coverage grid ─────────────────────────────────────────────
        months = []
        cur = backfill_start.replace(day=1)
        while cur <= today.replace(day=1):
            _, days_in_month = cal.monthrange(cur.year, cur.month)
            month_end = dt_date(cur.year, cur.month, days_in_month)
            cutoff = min(month_end, yesterday)
            expected = max(0, (cutoff - cur).days + 1) if cutoff >= cur else 0
            found = sum(
                1 for d in existing_dates
                if d.year == cur.year and d.month == cur.month
            )
            pct = round(found / expected * 100, 1) if expected > 0 else 0.0
            months.append({
                "month": cur.strftime("%Y-%m"),
                "label": cur.strftime("%b %Y"),
                "expected": expected,
                "found": found,
                "pct": pct,
                "status": "complete" if pct >= 95 else ("partial" if pct > 0 else "empty"),
            })
            # advance to next month
            if cur.month == 12:
                cur = dt_date(cur.year + 1, 1, 1)
            else:
                cur = dt_date(cur.year, cur.month + 1, 1)

        # ── Overall coverage ──────────────────────────────────────────────────
        total_expected = max(0, (yesterday - backfill_start).days + 1)
        total_found = len(existing_dates)
        coverage_pct = round(total_found / total_expected * 100, 1) if total_expected > 0 else 0.0

        # ── Missing date ranges ───────────────────────────────────────────────
        all_expected: set = set()
        d = backfill_start
        while d <= yesterday:
            all_expected.add(d)
            d += timedelta(days=1)

        missing = sorted(all_expected - existing_dates)
        missing_ranges = []
        if missing:
            rs = re = missing[0]
            for md in missing[1:]:
                if (md - re).days <= 1:
                    re = md
                else:
                    missing_ranges.append({
                        "start": rs.isoformat(),
                        "end": re.isoformat(),
                        "days": (re - rs).days + 1,
                        "label": rs.strftime("%b %Y") if rs.strftime("%b %Y") == re.strftime("%b %Y")
                                 else f"{rs.strftime('%b %Y')} – {re.strftime('%b %Y')}",
                    })
                    rs = re = md
            missing_ranges.append({
                "start": rs.isoformat(),
                "end": re.isoformat(),
                "days": (re - rs).days + 1,
                "label": rs.strftime("%b %Y") if rs.strftime("%b %Y") == re.strftime("%b %Y")
                         else f"{rs.strftime('%b %Y')} – {re.strftime('%b %Y')}",
            })

        # ── Per-job health (last run + 24h stats) ─────────────────────────────
        cutoff_24h = datetime.now(ZoneInfo("America/Chicago")) - timedelta(hours=24)
        job_names = [
            "sp_api_sync", "today_sync", "gap_fill",
            "ads_sync", "nightly_deep_sync", "auto_backfill",
            "pricing_sync", "nightly_backup_gdrive",
        ]
        job_health = {}
        for job in job_names:
            last = con.execute("""
                SELECT started_at, status, records_processed,
                       execution_time_seconds, error_message
                FROM sync_log WHERE job_name = ?
                ORDER BY started_at DESC LIMIT 1
            """, [job]).fetchone()

            stats = con.execute("""
                SELECT COUNT(*),
                       SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END),
                       COALESCE(SUM(records_processed), 0)
                FROM sync_log
                WHERE job_name = ? AND started_at >= ?
            """, [job, cutoff_24h]).fetchone()

            job_health[job] = {
                "last_run": str(last[0])[:19] if last and last[0] else None,
                "last_status": last[1] if last else None,
                "last_records": int(last[2] or 0) if last else 0,
                "last_duration_s": round(last[3], 1) if last and last[3] else None,
                "last_error": last[4] if last else None,
                "runs_24h": int(stats[0] or 0) if stats else 0,
                "success_24h": int(stats[1] or 0) if stats else 0,
                "records_24h": int(stats[2] or 0) if stats else 0,
            }

        # ── Report budget (in-memory counter from sync_engine) ────────────────
        try:
            from services.sync_engine import _report_budget, _HISTORICAL_REPORT_BUDGET
            b_date = _report_budget.get("date")
            b_used = _report_budget.get("used", 0) if b_date == today else 0
            budget = {"used": b_used, "cap": _HISTORICAL_REPORT_BUDGET,
                      "remaining": _HISTORICAL_REPORT_BUDGET - b_used}
        except Exception:
            budget = {"used": 0, "cap": 14, "remaining": 14}

        return {
            "months": months,
            "total_expected": total_expected,
            "total_found": total_found,
            "coverage_pct": coverage_pct,
            "missing_ranges": missing_ranges,
            "missing_days": len(missing),
            "job_health": job_health,
            "report_budget": budget,
            "as_of": yesterday.isoformat(),
        }
    finally:
        con.close()


@router.post("/api/sync/gap-fill")
async def trigger_gap_fill(request: Request):
    """Manually trigger one incremental gap-fill chunk."""
    from core.auth import get_session
    from fastapi import HTTPException
    from services.sync_engine import run_incremental_gap_fill

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, run_incremental_gap_fill)
    return result


@router.post("/api/backup/trigger")
async def trigger_backup(request: Request):
    """Manually trigger a Google Drive database backup."""
    from core.auth import get_session
    from core.scheduler import _run_duckdb_backup

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_duckdb_backup)
    return {"status": "backup_complete", "message": "Backup uploaded to Google Drive"}


@router.get("/api/backup/status")
async def backup_status(request: Request):
    """Get Google Drive backup status — last backup, total count, recent list."""
    from core.auth import get_session
    from services.backup import get_backup_status as _get_backup_status

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    result = _get_backup_status()
    return result


@router.post("/api/backup/github-trigger")
async def trigger_github_backup(request: Request):
    """Manually trigger a GitHub manifest/docs backup."""
    from core.auth import get_session
    from services.backup import run_github_backup
    from services.sync_engine import _write_sync_log

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    started_at = datetime.now(ZoneInfo("America/Chicago"))
    result = run_github_backup()
    _write_sync_log(
        "nightly_backup_github", started_at,
        result.get("status", "FAILED"),
        inserted=len(result.get("files_committed", [])),
        error=result.get("error"),
    )
    return {
        "status": result.get("status"),
        "files_committed": result.get("files_committed", []),
        "error": result.get("error"),
    }


@router.get("/api/backup/github-status")
async def github_backup_status(request: Request):
    """Get GitHub backup status from sync_log."""
    from core.auth import get_session
    from services.backup import get_github_backup_status as _get_gh_status

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    return _get_gh_status()


@router.post("/api/docs/update")
async def trigger_docs_update(request: Request):
    """Manually trigger a docs update."""
    from core.auth import get_session
    from core.scheduler import _run_scheduled_docs_update

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_scheduled_docs_update)
    return {"status": "docs_update_started"}


@router.get("/api/docs/status")
async def get_docs_status(request: Request):
    """Get status of recent docs updates from docs_update_log."""
    from core.auth import get_session

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    con = get_db_rw()
    try:
        rows = con.execute("""
            SELECT id, started_at, completed_at, status, execution_time_seconds, error_message, documents_updated
            FROM docs_update_log
            ORDER BY started_at DESC
            LIMIT 10
        """).fetchall()

        return {
            "updates": [
                {
                    "id": row[0],
                    "started_at": str(row[1]),
                    "completed_at": str(row[2]),
                    "status": row[3],
                    "execution_time_seconds": row[4],
                    "error_message": row[5],
                    "documents_updated": row[6]
                }
                for row in rows
            ]
        }
    finally:
        con.close()


@router.post("/api/analytics/rollup")
async def trigger_analytics_rollup(request: Request):
    """Manually trigger analytics rollup to populate analytics tables."""
    from core.auth import get_session
    from services.analytics_rollup import run_full_rollup

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_full_rollup)
        now = datetime.now(ZoneInfo("America/Chicago"))
        return {
            "status": "rollup_complete",
            "daily_rows": result.get("daily_rows", 0),
            "sku_rows": result.get("sku_rows", 0),
            "completed_at": now.isoformat(),
        }
    except Exception as e:
        import traceback
        return JSONResponse(
            status_code=500,
            content={"detail": str(e), "traceback": traceback.format_exc()},
        )


@router.get("/api/docs/architecture")
async def get_architecture_doc(request: Request):
    """Retrieve the generated architecture guide. (Generated by Prompt 2)"""
    from core.auth import get_session

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    docs_dir = Path("/app/docs")
    arch_file = docs_dir / "architecture_guide.md"

    if arch_file.exists():
        with open(arch_file, "r") as f:
            return {
                "status": "ok",
                "content": f.read(),
                "last_updated": datetime.fromtimestamp(arch_file.stat().st_mtime).isoformat()
            }
    else:
        return {
            "status": "not_found",
            "content": "Architecture guide not yet generated. Run /api/docs/update to generate.",
            "last_updated": None
        }


@router.get("/api/docs/disaster-recovery")
async def get_disaster_recovery_doc(request: Request):
    """Retrieve the generated disaster recovery plan. (Generated by Prompt 2)"""
    from core.auth import get_session

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    docs_dir = Path("/app/docs")
    dr_file = docs_dir / "disaster_recovery_plan.md"

    if dr_file.exists():
        with open(dr_file, "r") as f:
            return {
                "status": "ok",
                "content": f.read(),
                "last_updated": datetime.fromtimestamp(dr_file.stat().st_mtime).isoformat()
            }
    else:
        return {
            "status": "not_found",
            "content": "Disaster recovery plan not yet generated. Run /api/docs/update to generate.",
            "last_updated": None
        }


@router.post("/api/admin/init-tables")
def init_tables(request: Request):
    """Re-run all CREATE TABLE IF NOT EXISTS statements to ensure schema is current."""
    sess = request.cookies.get("session_id")
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        from core.database import init_all_tables
        init_all_tables()
        return {"status": "ok", "message": "All tables initialized successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/admin/pg-ready")
def pg_ready():
    """Check PostgreSQL migration readiness."""
    from pathlib import Path
    try:
        con = get_db()
        if USE_POSTGRES:
            tables = [r[0] for r in con.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
            ).fetchall()]
        else:
            tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        con.close()
        export_path = Path("/app/data/pg_export")
        manifest_path = export_path / "MANIFEST.txt"
        return {
            "duckdb_tables": len(tables),
            "table_list": tables,
            "export_ready": export_path.exists(),
            "export_path": str(export_path),
            "manifest_exists": manifest_path.exists(),
        }
    except Exception as e:
        return {
            "duckdb_tables": 0,
            "export_ready": False,
            "export_path": "/app/data/pg_export",
            "manifest_exists": False,
            "error": str(e),
        }


@router.get("/api/debug/db-diagnostic")
def db_diagnostic():
    """Return recent daily_sales and financial_events data for debugging (no SP-API call needed)."""
    try:
        con = get_db()
        # Recent daily_sales (ALL aggregates)
        daily_rows = con.execute("""
            SELECT date, units_ordered, ordered_product_sales
            FROM daily_sales
            WHERE asin = 'ALL'
            ORDER BY date DESC
            LIMIT 14
        """).fetchall()

        # Financial events summary
        fe_total = con.execute("SELECT COUNT(*) FROM financial_events").fetchone()[0]
        fe_nonzero = con.execute("""
            SELECT COUNT(*) FROM financial_events
            WHERE product_charges != 0 OR fba_fees != 0 OR commission != 0
        """).fetchone()[0]
        fe_sample = con.execute("""
            SELECT date, asin, event_type, product_charges, fba_fees, commission, net_proceeds, order_id
            FROM financial_events
            ORDER BY date DESC
            LIMIT 5
        """).fetchall()

        # Sync log (last 10 entries)
        sync_entries = []
        try:
            sync_rows = con.execute("""
                SELECT job_name, started_at, status, records_processed, error_message
                FROM sync_log
                ORDER BY started_at DESC
                LIMIT 10
            """).fetchall()
            for sr in sync_rows:
                sync_entries.append({
                    "job": sr[0], "started": str(sr[1])[:19],
                    "status": sr[2], "rows": sr[3],
                    "error": (sr[4] or "")[:200]
                })
        except Exception:
            sync_entries = [{"error": "sync_log table not found"}]

        # Monthly YOY test
        monthly_yoy_data = []
        try:
            yoy_rows = con.execute("""
                SELECT EXTRACT(YEAR FROM CAST(date AS DATE)) AS yr,
                       EXTRACT(MONTH FROM CAST(date AS DATE)) AS mo,
                       COALESCE(SUM(ordered_product_sales), 0) AS revenue
                FROM daily_sales
                WHERE asin = 'ALL' AND date IS NOT NULL AND date >= '2024-01-01'
                GROUP BY EXTRACT(YEAR FROM CAST(date AS DATE)),
                         EXTRACT(MONTH FROM CAST(date AS DATE))
                ORDER BY yr, mo
            """).fetchall()
            monthly_yoy_data = [{"year": int(r[0]), "month": int(r[1]), "revenue": round(float(r[2]), 2)} for r in yoy_rows]
        except Exception as yoy_e:
            monthly_yoy_data = [{"error": str(yoy_e)}]

        con.close()

        return {
            "daily_sales_recent": [
                {"date": str(r[0]), "units": r[1], "revenue": float(r[2]) if r[2] else 0}
                for r in daily_rows
            ],
            "financial_events": {
                "total_rows": fe_total,
                "nonzero_rows": fe_nonzero,
                "samples": [
                    {"date": str(r[0]), "asin": r[1], "type": r[2],
                     "charges": float(r[3]) if r[3] else 0, "fees": float(r[4]) if r[4] else 0,
                     "commission": float(r[5]) if r[5] else 0, "net": float(r[6]) if r[6] else 0,
                     "order": r[7]}
                    for r in fe_sample
                ]
            },
            "sync_log": sync_entries,
            "monthly_yoy": monthly_yoy_data,
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@router.get("/api/debug/test-financial-parse")
def test_financial_parse():
    """Fetch one financial event, parse it the same way the sync does, and return the result."""
    try:
        from services.sp_api import _load_sp_api_credentials, is_sync_running
        from sp_api.api import Finances as FinancesAPI
        from sp_api.base import Marketplaces as _Mp

        # Check if a sync is currently running
        running = is_sync_running()

        creds = _load_sp_api_credentials()
        if not creds:
            return {"error": "No SP-API credentials found"}

        fin_start = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        finances = FinancesAPI(credentials=creds, marketplace=_Mp.US)
        resp = finances.list_financial_events(PostedAfter=fin_start, MaxResultsPerPage=10)

        payload = resp.payload if hasattr(resp, 'payload') else (resp if isinstance(resp, dict) else {})
        events = payload.get("FinancialEvents", {})
        shipments = events.get("ShipmentEventList", []) or []

        if not shipments:
            return {"error": "No shipment events found", "sync_running": running}

        first_event = shipments[0]
        items = first_event.get("ShipmentItemList", []) if isinstance(first_event, dict) else getattr(first_event, "ShipmentItemList", []) or []

        results = []
        for item in items[:3]:
            charges = item.get("ItemChargeList", []) if isinstance(item, dict) else getattr(item, "ItemChargeList", []) or []
            fees = item.get("ItemFeeList", []) if isinstance(item, dict) else getattr(item, "ItemFeeList", []) or []

            charge_details = []
            for c in charges:
                ca = c.get("ChargeAmount") if isinstance(c, dict) else getattr(c, "ChargeAmount", None)
                ct = c.get("ChargeType", "") if isinstance(c, dict) else getattr(c, "ChargeType", "")

                # Parse exactly as _money() does
                parsed_val = 0.0
                parse_method = "unknown"
                if ca is None:
                    parsed_val = 0.0
                    parse_method = "None"
                elif isinstance(ca, dict):
                    for key in ("CurrencyAmount", "Amount", "currency_amount", "amount", "value"):
                        v = ca.get(key)
                        if v is not None:
                            try:
                                parsed_val = float(v)
                                parse_method = f"dict[{key}]"
                                break
                            except (ValueError, TypeError):
                                continue
                    if parse_method == "unknown":
                        parse_method = f"dict_no_match_keys={list(ca.keys())}"

                charge_details.append({
                    "charge_type": ct,
                    "raw_charge_amount": str(ca)[:200],
                    "ca_type": type(ca).__name__,
                    "ca_is_dict": isinstance(ca, dict),
                    "parsed_value": parsed_val,
                    "parse_method": parse_method,
                })

            fee_details = []
            for f in fees[:3]:
                fa = f.get("FeeAmount") if isinstance(f, dict) else getattr(f, "FeeAmount", None)
                ft = f.get("FeeType", "") if isinstance(f, dict) else getattr(f, "FeeType", "")
                parsed_fee = 0.0
                if fa and isinstance(fa, dict):
                    for key in ("CurrencyAmount", "Amount"):
                        v = fa.get(key)
                        if v is not None:
                            try:
                                parsed_fee = abs(float(v))
                                break
                            except (ValueError, TypeError):
                                pass
                fee_details.append({"fee_type": ft, "raw": str(fa)[:200], "parsed": parsed_fee})

            results.append({
                "sku": item.get("SellerSKU", "") if isinstance(item, dict) else getattr(item, "SellerSKU", ""),
                "asin": item.get("ASIN", "") if isinstance(item, dict) else getattr(item, "ASIN", ""),
                "charges": charge_details,
                "fees": fee_details,
            })

        return {
            "sync_running": running,
            "shipment_count": len(shipments),
            "first_event_order": first_event.get("AmazonOrderId", "?") if isinstance(first_event, dict) else getattr(first_event, "AmazonOrderId", "?"),
            "first_event_items_count": len(items),
            "parsed_items": results,
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@router.get("/api/debug/logs")
def debug_logs(n: int = Query(100, ge=1, le=200)):
    """Return recent in-memory log messages from the sync engine."""
    try:
        from services.sp_api import _log_buffer
        entries = list(_log_buffer)
        return {"count": len(entries), "logs": entries[-n:]}
    except ImportError:
        return {"error": "_log_buffer not available"}
    except Exception as e:
        return {"error": str(e)}


@router.get("/api/debug/daily-sales-coverage")
def debug_daily_sales_coverage():
    """Check daily_sales data coverage for heatmap debugging."""
    try:
        con = get_db()
        # Total rows
        total = con.execute("SELECT COUNT(*) FROM daily_sales").fetchone()[0]
        # ALL rows
        all_count = con.execute("SELECT COUNT(*) FROM daily_sales WHERE asin = 'ALL'").fetchone()[0]
        # per-ASIN rows
        per_asin = con.execute("SELECT COUNT(*) FROM daily_sales WHERE asin != 'ALL'").fetchone()[0]
        # Date range for ALL
        all_range = con.execute("""
            SELECT MIN(date), MAX(date) FROM daily_sales WHERE asin = 'ALL'
        """).fetchone()
        # Date range for per-ASIN
        asin_range = con.execute("""
            SELECT MIN(date), MAX(date) FROM daily_sales WHERE asin != 'ALL'
        """).fetchone()
        # Last 26 weeks ALL coverage
        from datetime import date as dt_date, timedelta
        from zoneinfo import ZoneInfo
        from datetime import datetime
        today = datetime.now(ZoneInfo("America/Chicago")).date()
        start_26w = today - timedelta(weeks=26)
        all_26w = con.execute("""
            SELECT COUNT(DISTINCT date) FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ?
        """, [str(start_26w), str(today)]).fetchone()[0]
        asin_26w = con.execute("""
            SELECT COUNT(DISTINCT date) FROM daily_sales
            WHERE asin != 'ALL' AND date >= ? AND date <= ?
        """, [str(start_26w), str(today)]).fetchone()[0]
        # Sample of ALL rows with 0 units in last 26 weeks
        zero_units = con.execute("""
            SELECT date, units_ordered FROM daily_sales
            WHERE asin = 'ALL' AND date >= ? AND date <= ? AND (units_ordered IS NULL OR units_ordered = 0)
            ORDER BY date LIMIT 20
        """, [str(start_26w), str(today)]).fetchall()
        # Division distribution for ALL rows
        div_dist = con.execute("""
            SELECT division, COUNT(*) FROM daily_sales WHERE asin = 'ALL' GROUP BY division
        """).fetchall()
        con.close()
        return {
            "total_rows": total,
            "asin_ALL_rows": all_count,
            "per_asin_rows": per_asin,
            "ALL_date_range": {"min": str(all_range[0]), "max": str(all_range[1])} if all_range else None,
            "per_asin_date_range": {"min": str(asin_range[0]), "max": str(asin_range[1])} if asin_range else None,
            "today": str(today),
            "start_26w": str(start_26w),
            "ALL_distinct_days_26w": all_26w,
            "per_asin_distinct_days_26w": asin_26w,
            "expected_days_26w": (today - start_26w).days + 1,
            "zero_unit_ALL_rows_sample": [{"date": str(r[0]), "units": r[1]} for r in zero_units],
            "division_distribution_ALL": [{"division": r[0], "count": r[1]} for r in div_dist],
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}
