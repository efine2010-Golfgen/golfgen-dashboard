"""System, health, debug, and backup routes."""
import os
import json
import logging
import asyncio
import duckdb
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from fastapi import APIRouter, Query, Request

from core.config import DB_PATH, DB_DIR, TIMEZONE, DOCS_DIR

logger = logging.getLogger("golfgen")
router = APIRouter()


def _load_sp_api_credentials():
    """Load SP-API credentials from environment."""
    refresh_token = os.environ.get("SP_API_REFRESH_TOKEN")
    if not refresh_token:
        return None
    return {
        "refresh_token": refresh_token,
        "client_id": os.environ.get("LWA_APP_ID"),
        "client_secret": os.environ.get("LWA_CLIENT_SECRET"),
        "marketplace_id": os.environ.get("MARKETPLACE_ID", "ATVPDKIKX0DER"),
    }


@router.get("/api/health")
def health():
    now = datetime.now(ZoneInfo("America/Chicago"))
    result = {
        "status": "healthy",
        "timestamp": now.isoformat(),
        "database": {
            "path": str(DB_PATH),
            "size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0,
            "tables": {},
        },
        "last_sync": {},
        "has_sp_api_creds": _load_sp_api_credentials() is not None,
    }

    try:
        con = duckdb.connect(str(DB_PATH), read_only=False)
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
        con = duckdb.connect(str(DB_PATH), read_only=False)
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

                con = duckdb.connect(str(DB_PATH), read_only=False)
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

    con = duckdb.connect(str(DB_PATH), read_only=False)
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

    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Not authenticated")

    con = duckdb.connect(str(DB_PATH), read_only=False)
    try:
        rows = con.execute("""
            SELECT id, job_name, started_at, completed_at, status,
                   records_processed, records_processed, 
                   error_message, execution_time_seconds
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
                    "records_processed": row[6] or 0,
                    "": row[7] or 0,
                    "error_message": row[8],
                    "execution_time_seconds": round(row[9], 2) if row[9] else None,
                }
                for row in rows
            ]
        }
    finally:
        con.close()


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

    return _get_backup_status()


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

    con = duckdb.connect(str(DB_PATH), read_only=False)
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


@router.get("/api/debug/db-query")
def debug_db_query(sql: str = Query(..., description="Read-only SQL query")):
    """Run a read-only SQL query against the database for debugging."""
    # Only allow SELECT queries
    cleaned = sql.strip().upper()
    if not cleaned.startswith("SELECT"):
        return {"error": "Only SELECT queries allowed"}
    try:
        con = duckdb.connect(str(DB_PATH), read_only=False)
        rows = con.execute(sql).fetchall()
        cols = [desc[0] for desc in con.description]
        con.close()
        return {"columns": cols, "rows": [list(r) for r in rows[:500]], "count": len(rows)}
    except Exception as e:
        return {"error": str(e)}
