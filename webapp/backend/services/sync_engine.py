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

import duckdb

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

    con = duckdb.connect(str(DB_PATH), read_only=True)
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

    # If we already have 500+ days of data, skip backfill
    if total_days >= 500:
        logger.info(f"Auto-backfill: {total_days} days of data found, skipping backfill")
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

                wcon = duckdb.connect(str(DB_PATH), read_only=False)
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

                    wcon.execute("""
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

                wcon.close()
                total_records += records
                logger.info(f"  Auto-backfill: {chunk_start}-{chunk_end}: {records} records")

        except Exception as e:
            logger.error(f"  Auto-backfill error {chunk_start}-{chunk_end}: {e}")

        chunk_start = chunk_end + timedelta(days=1)

    logger.info(f"Auto-backfill complete: {total_records} total records inserted")


def _write_sync_log(job_name, started_at, status, pulled=0, inserted=0, skipped=0, error=None):
    """Write a completed sync log entry with timing and record counts."""
    try:
        completed_at = datetime.now(ZoneInfo("America/Chicago"))
        duration = (completed_at - started_at).total_seconds()
        con = duckdb.connect(str(DB_PATH))
        con.execute("""
            INSERT INTO sync_log
            (job_name, started_at, completed_at, status, records_pulled,
             records_inserted, records_skipped, error_message, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [job_name, started_at, completed_at, status, pulled,
              inserted, skipped, error, duration])
        con.close()
    except Exception as e:
        logger.error(f"Failed to write sync log: {e}")


def _log_sync(job_name: str, status: str = "in_progress", records_processed: int = 0, error_message: str = None, execution_time: float = None) -> int:
    """Legacy sync log helper. Returns the log ID."""
    try:
        con = duckdb.connect(str(DB_PATH))
        result = con.execute("""
            INSERT INTO sync_log (job_name, status, records_pulled, error_message, duration_seconds, completed_at)
            VALUES (?, ?, ?, ?, ?, CASE WHEN ? = 'completed' OR ? = 'failed' THEN CURRENT_TIMESTAMP ELSE NULL END)
            RETURNING id
        """, [job_name, status, records_processed, error_message, execution_time, status, status]).fetchone()
        con.close()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Failed to log sync: {e}")
        return None


def _log_docs_update(status: str = "in_progress", documents_updated: str = None, error_message: str = None, execution_time: float = None) -> int:
    """Log a docs update to the docs_update_log table. Returns the log ID."""
    try:
        con = duckdb.connect(str(DB_PATH))
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
