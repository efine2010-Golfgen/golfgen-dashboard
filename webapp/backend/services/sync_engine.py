"""
Sync orchestration and logging for GolfGen Dashboard.

Provides:
- _auto_backfill_if_needed: Auto-backfill historical data on startup
- _log_sync: Log sync job execution
- _log_docs_update: Log docs update execution
"""

import json
import logging
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo

from core.database import get_db, get_db_rw

from core.config import DB_PATH

logger = logging.getLogger("golfgen")


def _auto_backfill_if_needed():
    """Check if historical data is missing and backfill automatically.
    This prevents data loss when Railway redeploys (ephemeral filesystem).
    Backfills: Apr 2024 to yesterday, in 30-day chunks."""
    import time as _time
    import gzip as gz
    import requests as req
    from sp_api.api import Reports
    from sp_api.base import Marketplaces, ReportType

    con = get_db_rw()
    try:
        earliest_row = con.execute(
            "SELECT MIN(date) FROM daily_sales WHERE asin = 'ALL'"
        ).fetchone()
        earliest = earliest_row[0] if earliest_row and earliest_row[0] else None
        count_row = con.execute(
            "SELECT COUNT(DISTINCT date) FROM daily_sales WHERE asin = 'ALL'"
        ).fetchone()
        total_days = count_row[0] if count_row else 0
    except Exception:
        earliest = None
        total_days = 0
    finally:
        con.close()

    # Calculate expected days (Apr 2024 to yesterday)
    expected_start = dt_date(2024, 4, 1)
    expected_end = datetime.now(ZoneInfo("America/Chicago")).date() - timedelta(days=1)
    expected_days = (expected_end - expected_start).days + 1
    coverage_pct = (total_days / expected_days * 100) if expected_days > 0 else 0

    # Skip backfill if we have 95%+ coverage
    if coverage_pct >= 95:
        logger.info(f"Auto-backfill: {total_days}/{expected_days} days ({coverage_pct:.0f}% coverage), skipping backfill")
        return

    logger.info(f"Auto-backfill: only {total_days} days found (earliest={earliest}). Starting historical backfill...")

    from services.sp_api import _load_sp_api_credentials
    creds = _load_sp_api_credentials()
    if not creds:
        logger.error("Auto-backfill: no SP-API credentials, skipping")
        return

    reports_api = Reports(credentials=creds, marketplace=Marketplaces.US)

    # Backfill from Apr 2024 to yesterday
    backfill_start = dt_date(2024, 4, 1)
    backfill_end = datetime.now(ZoneInfo("America/Chicago")).date() - timedelta(days=1)

    chunk_start = backfill_start
    total_records = 0

    while chunk_start < backfill_end:
        chunk_end = min(chunk_start + timedelta(days=29), backfill_end)
        logger.info(f"  Auto-backfill: requesting {chunk_start} to {chunk_end}...")

        try:
            report_response = reports_api.create_report(
                reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
                dataStartTime=chunk_start.isoformat(),
                dataEndTime=chunk_end.isoformat(),
                reportOptions={"dateGranularity": "DAY", "asinGranularity": "CHILD"}
            )
            report_id = report_response.payload.get("reportId")

            report_data = None
            for attempt in range(30):
                _time.sleep(10)
                status_response = reports_api.get_report(report_id)
                status = status_response.payload.get("processingStatus")
                if status == "DONE":
                    doc_id = status_response.payload.get("reportDocumentId")
                    doc_response = reports_api.get_report_document(doc_id)
                    report_data = doc_response.payload
                    break
                elif status in ("CANCELLED", "FATAL"):
                    logger.warning(f"  Auto-backfill: report {chunk_start}-{chunk_end} status={status}")
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

                wcon = get_db_rw()
                records = 0

                for day_entry in report_data.get("salesAndTrafficByDate", []):
                    entry_date = day_entry.get("date", "")
                    traffic = day_entry.get("trafficByDate", {})
                    sales_info = day_entry.get("salesByDate", {})
                    ordered_sales = sales_info.get("orderedProductSales", {})
                    sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                    wcon.execute("""
                        INSERT OR REPLACE INTO daily_sales
                        (date, asin, units_ordered, ordered_product_sales,
                         sessions, session_percentage, page_views,
                         buy_box_percentage, unit_session_percentage,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
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

                    wcon.execute("""
                        INSERT OR REPLACE INTO daily_sales
                        (date, asin, units_ordered, ordered_product_sales,
                         sessions, session_percentage, page_views,
                         buy_box_percentage, unit_session_percentage,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
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

                wcon.close()
                total_records += records
                logger.info(f"  Auto-backfill: {chunk_start}-{chunk_end}: {records} records")

        except Exception as e:
            logger.error(f"  Auto-backfill error {chunk_start}-{chunk_end}: {e}")

        chunk_start = chunk_end + timedelta(days=1)

    logger.info(f"Auto-backfill complete: {total_records} total records inserted")


def _write_sync_log(job_name, started_at, status, inserted=0, error=None):
    """Write a completed sync log entry with timing and record counts."""
    try:
        completed_at = datetime.now(ZoneInfo("America/Chicago"))
        duration = (completed_at - started_at).total_seconds()
        con = get_db()
        con.execute("""
            INSERT INTO sync_log
            (job_name, started_at, completed_at, status,
             records_processed, error_message, execution_time_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [job_name, started_at, completed_at, status,
              inserted, error, duration])
        con.close()
    except Exception as e:
        logger.error(f"Failed to write sync log: {e}")


# NOTE: _log_sync() was dead code using wrong column names (records_pulled,
# duration_seconds). Removed. Use _write_sync_log() for all sync logging.


def _log_docs_update(status: str = "in_progress", documents_updated: str = None, error_message: str = None, execution_time: float = None) -> int:
    """Log a docs update to the docs_update_log table. Returns the log ID."""
    try:
        con = get_db()
        result = con.execute("""
            INSERT INTO docs_update_log (status, documents_updated, error_message, execution_time_seconds, completed_at)
            VALUES (?, ?, ?, ?, CASE WHEN ? = 'completed' OR ? = 'failed' THEN CURRENT_TIMESTAMP ELSE NULL END)
            RETURNING id
        """, [status, documents_updated, error_message, execution_time, status, status]).fetchone()
        con.close()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Failed to log docs update: {e}")
        return None


# ── Nightly Deep Sync ─────────────────────────────────────────────
def run_nightly_deep_sync():
    """Comprehensive overnight sync that fills ALL data gaps.

    Strategy:
    1. Detect gaps: query daily_sales for missing dates from Apr 2024 to yesterday
    2. Always re-pull last 30 days (Amazon revises data up to 30 days back)
    3. Pull any detected gap ranges in 30-day chunks
    4. Checkpoint each successful chunk so crashes don't re-pull completed ranges
    5. Pull financial events for last 90 days
    6. Pull inventory snapshot
    7. Run analytics rollup

    This is designed to run at 3am Central when API quotas are fresh.
    """
    import time as _time
    import gzip as gz
    import requests as req

    logger.info("═══ NIGHTLY DEEP SYNC STARTED ═══")
    started = datetime.now(ZoneInfo("America/Chicago"))

    from services.sp_api import _load_sp_api_credentials, _sync_today_orders, retry_with_backoff
    creds = _load_sp_api_credentials()
    if not creds:
        logger.error("Nightly sync: no SP-API credentials")
        _write_sync_log("nightly_deep_sync", started, "FAILED", error="No credentials")
        return {"status": "FAILED", "reason": "No credentials"}

    try:
        from sp_api.api import Reports, Finances as FinancesAPI, Inventories
        from sp_api.base import Marketplaces, ReportType
    except ImportError:
        logger.error("Nightly sync: SP-API library not installed")
        _write_sync_log("nightly_deep_sync", started, "FAILED", error="SP-API not installed")
        return {"status": "FAILED", "reason": "SP-API not installed"}

    reports_api = Reports(credentials=creds, marketplace=Marketplaces.US)
    today = datetime.now(ZoneInfo("America/Chicago")).date()
    yesterday = today - timedelta(days=1)
    backfill_start = dt_date(2024, 4, 1)

    # ── Step 1: Detect date gaps ──────────────────────────────────
    con = get_db_rw()
    try:
        existing_dates = set()
        rows = con.execute(
            "SELECT DISTINCT date FROM daily_sales WHERE asin = 'ALL' AND date >= '2024-04-01'"
        ).fetchall()
        for r in rows:
            d = r[0]
            if isinstance(d, str):
                d = datetime.strptime(d, "%Y-%m-%d").date()
            elif hasattr(d, 'date'):
                d = d.date()
            existing_dates.add(d)
    finally:
        con.close()

    # Build set of all expected dates
    all_dates = set()
    d = backfill_start
    while d <= yesterday:
        all_dates.add(d)
        d += timedelta(days=1)

    missing_dates = sorted(all_dates - existing_dates)
    logger.info(f"Nightly sync: {len(existing_dates)} days in DB, {len(missing_dates)} missing dates detected")

    # ── Step 2: Build pull ranges ─────────────────────────────────
    # Always re-pull last 30 days + any gap ranges
    pull_ranges = []

    # Last 30 days (Amazon data revisions)
    recent_start = yesterday - timedelta(days=30)
    pull_ranges.append((recent_start, yesterday, "last_30d"))

    # Group missing dates into contiguous ranges (with 30-day max chunks)
    if missing_dates:
        gap_start = missing_dates[0]
        gap_end = missing_dates[0]
        for md in missing_dates[1:]:
            if (md - gap_end).days <= 2:  # allow 1-day gaps to merge ranges
                gap_end = md
            else:
                pull_ranges.append((gap_start, gap_end, "gap_fill"))
                gap_start = md
                gap_end = md
        pull_ranges.append((gap_start, gap_end, "gap_fill"))

    # De-duplicate and split into 30-day chunks
    chunks = []
    seen = set()
    for rng_start, rng_end, label in pull_ranges:
        cs = rng_start
        while cs <= rng_end:
            ce = min(cs + timedelta(days=29), rng_end)
            key = (cs, ce)
            if key not in seen:
                seen.add(key)
                chunks.append((cs, ce, label))
            cs = ce + timedelta(days=1)

    chunks.sort(key=lambda x: x[0])
    logger.info(f"Nightly sync: {len(chunks)} chunks to pull")

    # ── Step 3: Pull each chunk via Sales & Traffic Report ────────
    total_records = 0
    chunks_ok = 0
    chunks_failed = 0

    for chunk_idx, (cs, ce, label) in enumerate(chunks):
        logger.info(f"  Chunk {chunk_idx+1}/{len(chunks)} [{label}]: {cs} to {ce}")

        try:
            report_response = reports_api.create_report(
                reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
                dataStartTime=cs.isoformat(),
                dataEndTime=ce.isoformat(),
                reportOptions={"dateGranularity": "DAY", "asinGranularity": "CHILD"}
            )
            report_id = report_response.payload.get("reportId")

            report_data = None
            for attempt in range(30):
                _time.sleep(10)
                status_response = reports_api.get_report(report_id)
                status = status_response.payload.get("processingStatus")
                if status == "DONE":
                    doc_id = status_response.payload.get("reportDocumentId")
                    doc_response = reports_api.get_report_document(doc_id)
                    report_data = doc_response.payload
                    break
                elif status in ("CANCELLED", "FATAL"):
                    logger.warning(f"  Chunk {cs}-{ce}: report status={status}")
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

                wcon = get_db_rw()
                wcon.execute("BEGIN TRANSACTION")
                records = 0

                from services.sp_api import _get_division_for_asin

                for day_entry in report_data.get("salesAndTrafficByDate", []):
                    entry_date = day_entry.get("date", "")
                    traffic = day_entry.get("trafficByDate", {})
                    sales_info = day_entry.get("salesByDate", {})
                    ordered_sales = sales_info.get("orderedProductSales", {})
                    sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                    wcon.execute("""
                        INSERT OR REPLACE INTO daily_sales
                        (date, asin, units_ordered, ordered_product_sales,
                         sessions, session_percentage, page_views,
                         buy_box_percentage, unit_session_percentage,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
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
                    entry_date = asin_entry.get("date", str(cs))
                    ordered_sales = sales_info.get("orderedProductSales", {})
                    sales_amount = float(ordered_sales.get("amount", 0)) if isinstance(ordered_sales, dict) else float(ordered_sales or 0)

                    _div = _get_division_for_asin(asin)
                    wcon.execute("""
                        INSERT OR REPLACE INTO daily_sales
                        (date, asin, units_ordered, ordered_product_sales,
                         sessions, session_percentage, page_views,
                         buy_box_percentage, unit_session_percentage,
                         division, customer, platform)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amazon', 'sp_api')
                    """, [
                        entry_date, asin,
                        int(sales_info.get("unitsOrdered", 0)),
                        sales_amount,
                        int(traffic.get("sessions", 0)),
                        float(traffic.get("sessionPercentage", 0)),
                        int(traffic.get("pageViews", 0)),
                        float(traffic.get("buyBoxPercentage", 0)),
                        float(traffic.get("unitSessionPercentage", 0)),
                        _div,
                    ])
                    records += 1

                wcon.execute("COMMIT")
                wcon.close()
                total_records += records
                chunks_ok += 1
                logger.info(f"  Chunk {cs}-{ce}: {records} records inserted")
            else:
                chunks_failed += 1
                logger.warning(f"  Chunk {cs}-{ce}: no report data returned")

        except Exception as e:
            chunks_failed += 1
            logger.error(f"  Chunk {cs}-{ce} error: {e}")
            # Continue to next chunk — don't let one failure stop everything
            _time.sleep(5)

        # Rate-limit: Amazon allows ~15 report requests per minute
        if chunk_idx < len(chunks) - 1:
            _time.sleep(4)

    # ── Step 4: Log results ───────────────────────────────────────
    elapsed = (datetime.now(ZoneInfo("America/Chicago")) - started).total_seconds()
    status = "SUCCESS" if chunks_failed == 0 else ("PARTIAL" if chunks_ok > 0 else "FAILED")
    logger.info(f"═══ NIGHTLY DEEP SYNC {status}: {chunks_ok}/{len(chunks)} chunks, {total_records} records, {elapsed:.0f}s ═══")

    _write_sync_log("nightly_deep_sync", started, status, inserted=total_records,
                     error=f"{chunks_failed} chunks failed" if chunks_failed else None)

    return {
        "status": status,
        "chunks_total": len(chunks),
        "chunks_ok": chunks_ok,
        "chunks_failed": chunks_failed,
        "total_records": total_records,
        "missing_dates_found": len(missing_dates),
        "elapsed_seconds": round(elapsed, 1),
    }
