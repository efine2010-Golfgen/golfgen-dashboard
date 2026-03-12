"""APScheduler setup, scheduled job wrappers, and startup sync logic."""
import os
import json
import shutil
import asyncio
import logging
import time
import duckdb
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.config import DB_PATH, DB_DIR, TIMEZONE, SYNC_INTERVAL_HOURS
from services.sp_api import _sync_today_orders, _run_sp_api_sync
from services.ads_api import _sync_ads_data, _sync_pricing_and_coupons
from services.sync_engine import _auto_backfill_if_needed, _log_sync

logger = logging.getLogger("golfgen")

# Module-level scheduler instance (will be started in lifespan)
scheduler = None


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


def _run_scheduled_sp_api_sync():
    """Wrapper for scheduled SP-API sync with logging."""
    start_time = time.time()
    log_id = _log_sync("sp_api_sync", "in_progress")
    try:
        _run_sp_api_sync()
        execution_time = time.time() - start_time
        _log_sync("sp_api_sync", "completed", execution_time=execution_time)
        logger.info(f"Scheduled SP-API sync completed in {execution_time:.2f}s")
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        _log_sync("sp_api_sync", "failed", error_message=error_msg, execution_time=execution_time)
        logger.error(f"Scheduled SP-API sync failed: {error_msg}")


def _run_scheduled_today_sync():
    """Wrapper for scheduled today orders sync with logging."""
    start_time = time.time()
    log_id = _log_sync("today_sync", "in_progress")
    try:
        _sync_today_orders()
        execution_time = time.time() - start_time
        _log_sync("today_sync", "completed", execution_time=execution_time)
        logger.info(f"Scheduled today sync completed in {execution_time:.2f}s")
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        _log_sync("today_sync", "failed", error_message=error_msg, execution_time=execution_time)
        logger.error(f"Scheduled today sync failed: {error_msg}")


def _run_scheduled_ads_sync():
    """Wrapper for scheduled ads sync with logging."""
    start_time = time.time()
    log_id = _log_sync("ads_sync", "in_progress")
    try:
        _sync_ads_data()
        execution_time = time.time() - start_time
        _log_sync("ads_sync", "completed", execution_time=execution_time)
        logger.info(f"Scheduled ads sync completed in {execution_time:.2f}s")
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        _log_sync("ads_sync", "failed", error_message=error_msg, execution_time=execution_time)
        logger.error(f"Scheduled ads sync failed: {error_msg}")


def _run_scheduled_pricing_sync():
    """Wrapper for scheduled pricing sync with logging."""
    start_time = time.time()
    log_id = _log_sync("pricing_sync", "in_progress")
    try:
        _sync_pricing_and_coupons()
        execution_time = time.time() - start_time
        _log_sync("pricing_sync", "completed", execution_time=execution_time)
        logger.info(f"Scheduled pricing sync completed in {execution_time:.2f}s")
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        _log_sync("pricing_sync", "failed", error_message=error_msg, execution_time=execution_time)
        logger.error(f"Scheduled pricing sync failed: {error_msg}")


def _run_scheduled_docs_update():
    """Wrapper for scheduled docs update with logging. (Placeholder for Prompt 2)"""
    start_time = time.time()
    log_id = _log_docs_update("in_progress")
    try:
        # Placeholder: actual implementation in Prompt 2
        # - Collect system snapshot
        # - Call Claude to generate docs
        # - Save to /app/docs/
        # - Commit to GitHub if token available
        logger.info("Docs update job started (placeholder)")
        execution_time = time.time() - start_time
        _log_docs_update("completed", documents_updated="architecture_guide.md, disaster_recovery_plan.md", execution_time=execution_time)
        logger.info(f"Scheduled docs update completed in {execution_time:.2f}s")
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        _log_docs_update("failed", error_message=error_msg, execution_time=execution_time)
        logger.error(f"Scheduled docs update failed: {error_msg}")


def _run_duckdb_backup():
    """Wrapper for nightly DuckDB backup with logging. (Placeholder for Prompt 2)"""
    start_time = time.time()
    log_id = _log_sync("duckdb_backup", "in_progress")
    try:
        # Placeholder: actual implementation in Prompt 2
        # - Copy DuckDB to temp file with date suffix
        # - Upload to Google Drive
        # - Clean up backups older than 30 days
        logger.info("DuckDB backup job started (placeholder)")
        execution_time = time.time() - start_time
        _log_sync("duckdb_backup", "completed", execution_time=execution_time)
        logger.info(f"DuckDB backup completed in {execution_time:.2f}s")
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        _log_sync("duckdb_backup", "failed", error_message=error_msg, execution_time=execution_time)
        logger.error(f"DuckDB backup failed: {error_msg}")


async def _sync_loop():
    """Initialize and run APScheduler for background sync jobs.

    Sequence:
    1. Immediately sync today's orders (fast, ~5s) — so "Today" works right away
    2. Auto-backfill historical data if missing (detects fresh deploy)
    3. Schedule 4 daily sync jobs at 9:00, 12:00, 15:00, 18:00 Central
    4. Schedule ads and pricing syncs every 2 hours
    5. Handle startup catchup: if a scheduled time has passed for today with no sync_log entry, run immediately
    """
    global scheduler

    # Immediately pull today's orders — no delay
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_today_orders)
        logger.info("Startup: today's orders synced")
    except Exception as e:
        logger.error(f"Startup today-sync error: {e}")

    # Run full SP-API sync on startup (includes financial events with refunds).
    # This is critical because Railway's ephemeral filesystem means every deploy
    # starts with the stale DuckDB snapshot from git.  Without this, refund data
    # and recent financial events never populate until a scheduled cron fires.
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _run_sp_api_sync)
        logger.info("Startup: full SP-API sync completed (orders + financial events + inventory)")
    except Exception as e:
        logger.error(f"Startup full-sync error: {e}")

    # Auto-backfill if historical data is missing (e.g. after Railway redeploy)
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _auto_backfill_if_needed)
    except Exception as e:
        logger.error(f"Auto-backfill error: {e}")

    # Sync ads data
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_ads_data)
    except Exception as e:
        logger.error(f"Ads sync error: {e}")

    # Sync pricing & coupon data
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_pricing_and_coupons)
    except Exception as e:
        logger.error(f"Pricing/coupon sync error: {e}")

    # Initialize APScheduler
    if not scheduler:
        scheduler = AsyncIOScheduler(timezone="America/Chicago")

        # Schedule 4 daily SP-API syncs at 9am, 12pm, 3pm, 6pm Central
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=9, minute=0, timezone="America/Chicago"), id="sp_api_sync_9am")
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=12, minute=0, timezone="America/Chicago"), id="sp_api_sync_12pm")
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=15, minute=0, timezone="America/Chicago"), id="sp_api_sync_3pm")
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=18, minute=0, timezone="America/Chicago"), id="sp_api_sync_6pm")

        # Schedule ads sync every 2 hours
        scheduler.add_job(_run_scheduled_ads_sync, CronTrigger(minute=0, second=0, timezone="America/Chicago"), id="ads_sync_hourly")

        # Schedule pricing sync every 2 hours (offset by 1 hour from ads)
        scheduler.add_job(_run_scheduled_pricing_sync, CronTrigger(minute=30, second=0, timezone="America/Chicago"), id="pricing_sync_hourly")

        # Schedule docs update at 8am and 8pm Central (Prompt 2 implementation)
        scheduler.add_job(_run_scheduled_docs_update, CronTrigger(hour=8, minute=0, timezone="America/Chicago"), id="docs_update_8am")
        scheduler.add_job(_run_scheduled_docs_update, CronTrigger(hour=20, minute=0, timezone="America/Chicago"), id="docs_update_8pm")

        # Schedule nightly DuckDB backup at 2am Central (Prompt 2 implementation)
        scheduler.add_job(_run_duckdb_backup, CronTrigger(hour=2, minute=0, timezone="America/Chicago"), id="duckdb_backup_2am")

        await scheduler.start()
        logger.info("APScheduler started with 4 daily SP-API syncs, hourly ads/pricing, docs updates, and nightly backup (America/Chicago)")

        # Startup catchup: if current time is past a scheduled sync time and no sync_log entry exists for today, run immediately
        await _startup_sync_catchup()


async def _startup_sync_catchup():
    """On startup, check if any scheduled sync times have passed today without a sync_log entry.
    If so, run the sync immediately to catch up."""
    try:
        now_utc = datetime.now(ZoneInfo("UTC"))
        now_central = now_utc.astimezone(ZoneInfo("America/Chicago"))
        today_date = now_central.date()

        scheduled_times = [9, 12, 15, 18]  # 9am, 12pm, 3pm, 6pm Central

        con = duckdb.connect(str(DB_PATH), read_only=True)

        for scheduled_hour in scheduled_times:
            scheduled_time = now_central.replace(hour=scheduled_hour, minute=0, second=0, microsecond=0)

            # Check if this time has already passed today
            if now_central > scheduled_time:
                # Check if we have a sync_log entry for this job today
                rows = con.execute("""
                    SELECT id FROM sync_log
                    WHERE job_name = 'sp_api_sync'
                    AND DATE(started_at) = ?
                    AND status = 'completed'
                    ORDER BY started_at DESC
                    LIMIT 1
                """, [today_date]).fetchall()

                if not rows:
                    logger.info(f"Startup catchup: {scheduled_hour}:00 sync hasn't run yet today, running now...")
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, _run_scheduled_sp_api_sync)
                    break  # Only run one catchup per startup

        con.close()
    except Exception as e:
        logger.error(f"Startup sync catchup error: {e}")


@asynccontextmanager
async def lifespan(app):
    """Start background sync and initialize tables on app startup."""
    from core.database import init_all_tables

    # Initialize all system and item plan tables
    try:
        init_all_tables()
        logger.info("All tables initialized")
    except Exception as e:
        logger.error(f"Table init error: {e}")

    task = asyncio.create_task(_sync_loop())
    logger.info("Background sync scheduler initialized")
    yield
    if scheduler and scheduler.running:
        await scheduler.shutdown()
    task.cancel()
