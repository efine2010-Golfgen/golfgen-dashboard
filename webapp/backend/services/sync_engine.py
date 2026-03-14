"""
Sync orchestration and logging for GolfGen Dashboard.

Provides:
- _auto_backfill_if_needed: Auto-backfill historical data on startup
- _write_sync_log: Write a completed sync log entry
- _log_docs_update: Log docs update execution
- run_nightly_deep_sync: Comprehensive overnight gap-fill sync
"""

import json
import logging
from datetime import datetime, timedelta, date as dt_date
from zoneinfo import ZoneInfo

from core.database import get_db, get_db_rw

from core.config import DB_PATH

logger = logging.getLogger("golfgen")


def _auto_backfill_if_needed():
    """Check if historical data is missing and backfill a small batch automatically.

    Conservative strategy — fills data in small pieces so it never blows quota:
    - Skips if another backfill ran within THROTTLE_HOURS (prevents multi-deploy pile-up)
    - Finds the oldest actual date gaps instead of always restarting from Apr 2024
    - Processes at most MAX_CHUNKS_PER_RUN chunks per startup call
    - Stops immediately on QuotaExceeded (don't hammer 14 dead chunks)
    - The nightly deep sync (3am) is the real workhorse for full gap-fill
    """
    MAX_CHUNKS_PER_RUN = 3      # How many 30-day chunks to pull per startup call
    THROTTLE_HOURS = 4          # Skip if a backfill ran within this many hours
    import time as _time
    import gzip as gz
    import requests as req
    from sp_api.api import Reports
    from sp_api.base import Marketplaces, ReportType

    started = datetime.now(ZoneInfo("America/Chicago"))

    # ── Throttle: skip if we already ran recently ────────────────────────────
    try:
        con = get_db()
        row = con.execute(
            "SELECT MAX(completed_at) FROM sync_log WHERE job_name = 'auto_backfill'"
        ).fetchone()
        con.close()
        if row and row[0]:
            last_dt = row[0]
            if hasattr(last_dt, "tzinfo") and last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=ZoneInfo("America/Chicago"))
            hours_ago = (started - last_dt).total_seconds() / 3600
            if hours_ago < THROTTLE_HOURS:
                logger.info(
                    f"Auto-backfill: last run {hours_ago:.1f}h ago "
                    f"(throttle={THROTTLE_HOURS}h), skipping"
                )
                return
    except Exception as te:
        logger.warning(f"Auto-backfill: throttle check error: {te}")

    # ── Coverage check ────────────────────────────────────────────────────────
    expected_start = dt_date(2024, 4, 1)
    expected_end = datetime.now(ZoneInfo("America/Chicago")).date() - timedelta(days=1)
    expected_days = (expected_end - expected_start).days + 1

    con = get_db()
    try:
        count_row = con.execute(
            "SELECT COUNT(DISTINCT date) FROM daily_sales WHERE asin = 'ALL'"
        ).fetchone()
        total_days = count_row[0] if count_row else 0

        existing_rows = con.execute(
            "SELECT DISTINCT date FROM daily_sales WHERE asin = 'ALL' AND date >= '2024-04-01'"
        ).fetchall()
        existing_dates: set = set()
        for r in existing_rows:
            d = r[0]
            if isinstance(d, str):
                d = datetime.strptime(d, "%Y-%m-%d").date()
            elif hasattr(d, "date"):
                d = d.date()
            existing_dates.add(d)
    except Exception:
        total_days = 0
        existing_dates = set()
    finally:
        con.close()

    coverage_pct = (total_days / expected_days * 100) if expected_days > 0 else 0
    if coverage_pct >= 95:
        logger.info(
            f"Auto-backfill: {total_days}/{expected_days} days "
            f"({coverage_pct:.0f}% coverage), skipping"
        )
        return

    # ── Find missing date gaps → build chunks ────────────────────────────────
    all_expected: set = set()
    d = expected_start
    while d <= expected_end:
        all_expected.add(d)
        d += timedelta(days=1)

    missing_dates = sorted(all_expected - existing_dates)
    if not missing_dates:
        logger.info("Auto-backfill: no missing dates detected, skipping")
        return

    # Group contiguous missing dates into 30-day chunks
    chunks: list = []
    gap_start = missing_dates[0]
    gap_end = missing_dates[0]
    for md in missing_dates[1:]:
        if (md - gap_end).days <= 2:
            gap_end = md
        else:
            cs = gap_start
            while cs <= gap_end:
                ce = min(cs + timedelta(days=29), gap_end)
                chunks.append((cs, ce))
                cs = ce + timedelta(days=1)
            gap_start = md
            gap_end = md
    cs = gap_start
    while cs <= gap_end:
        ce = min(cs + timedelta(days=29), gap_end)
        chunks.append((cs, ce))
        cs = ce + timedelta(days=1)

    total_chunks = len(chunks)
    run_chunks = chunks[:MAX_CHUNKS_PER_RUN]
    logger.info(
        f"Auto-backfill: {len(missing_dates)} missing days across {total_chunks} chunks. "
        f"Processing {len(run_chunks)} this startup (max={MAX_CHUNKS_PER_RUN}). "
        f"Remaining {total_chunks - len(run_chunks)} will fill via nightly sync."
    )

    from services.sp_api import _load_sp_api_credentials, _get_division_for_asin
    creds = _load_sp_api_credentials()
    if not creds:
        logger.error("Auto-backfill: no SP-API credentials, skipping")
        return

    reports_api = Reports(credentials=creds, marketplace=Marketplaces.US)
    total_records = 0
    chunks_ok = 0
    quota_hit = False

    for chunk_start, chunk_end in run_chunks:
        if quota_hit:
            break
        logger.info(f"  Auto-backfill: chunk {chunk_start} → {chunk_end}...")
        try:
            report_response = reports_api.create_report(
                reportType=ReportType.GET_SALES_AND_TRAFFIC_REPORT,
                dataStartTime=chunk_start.isoformat(),
                dataEndTime=chunk_end.isoformat(),
                reportOptions={"dateGranularity": "DAY", "asinGranularity": "CHILD"}
            )
            report_id = report_response.payload.get("reportId")

            report_data = None
            for _ in range(30):
                _time.sleep(10)
                status_response = reports_api.get_report(report_id)
                status = status_response.payload.get("processingStatus")
                if status == "DONE":
                    doc_id = status_response.payload.get("reportDocumentId")
                    doc_response = reports_api.get_report_document(doc_id)
                    report_data = doc_response.payload
                    break
                elif status in ("CANCELLED", "FATAL"):
                    logger.warning(
                        f"  Auto-backfill: report {chunk_start}-{chunk_end} "
                        f"status={status}"
                    )
                    break

            if report_data:
                if isinstance(report_data, dict) and "url" in report_data:
                    is_gzipped = (
                        report_data.get("compressionAlgorithm", "").upper() == "GZIP"
                    )
                    resp = req.get(report_data["url"])
                    report_text = (
                        gz.decompress(resp.content).decode("utf-8")
                        if is_gzipped
                        else resp.text
                    )
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
                    sales_amount = (
                        float(ordered_sales.get("amount", 0))
                        if isinstance(ordered_sales, dict)
                        else float(ordered_sales or 0)
                    )
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
                    sales_amount = (
                        float(ordered_sales.get("amount", 0))
                        if isinstance(ordered_sales, dict)
                        else float(ordered_sales or 0)
                    )
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

                wcon.close()
                total_records += records
                chunks_ok += 1
                logger.info(
                    f"  Auto-backfill: {chunk_start}-{chunk_end}: {records} records"
                )

        except Exception as e:
            err_str = str(e).lower()
            if "quota" in err_str or "quotaexceeded" in err_str or "429" in err_str:
                quota_hit = True
                logger.warning(
                    f"  Auto-backfill: QuotaExceeded on {chunk_start}-{chunk_end}. "
                    f"Stopping this run — nightly sync will retry after quota resets."
                )
            else:
                logger.error(f"  Auto-backfill error {chunk_start}-{chunk_end}: {e}")

        # Small pause between chunks to be gentle on rate limits
        _time.sleep(2)

    status = "SUCCESS" if chunks_ok == len(run_chunks) and not quota_hit else \
             ("PARTIAL" if chunks_ok > 0 else "FAILED")
    stop_reason = " (quota exceeded — will retry at nightly sync)" if quota_hit else ""
    logger.info(
        f"Auto-backfill complete: {chunks_ok}/{len(run_chunks)} chunks, "
        f"{total_records} records inserted{stop_reason}"
    )
    _write_sync_log(
        "auto_backfill", started, status, inserted=total_records,
        error=f"quota_exceeded after {chunks_ok} chunks" if quota_hit else None
    )


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
            err_str = str(e).lower()
            if "quota" in err_str or "quotaexceeded" in err_str or "429" in err_str:
                logger.warning(
                    f"  Chunk {cs}-{ce}: QuotaExceeded — stopping nightly sync early. "
                    f"Remaining {len(chunks) - chunk_idx - 1} chunks will run tomorrow."
                )
                break  # No point hammering more chunks — quota is exhausted
            else:
                logger.error(f"  Chunk {cs}-{ce} error: {e}")
                _time.sleep(5)  # Brief pause before continuing to next chunk

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
