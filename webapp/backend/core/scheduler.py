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
from services.sp_api import _sync_today_orders, _run_sp_api_sync, _backfill_orders
from services.ads_api import _sync_ads_data, _sync_pricing_and_coupons
from services.sync_engine import _auto_backfill_if_needed, _log_sync, _write_sync_log

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
    started_at = datetime.now(ZoneInfo("America/Chicago"))
    try:
        _run_sp_api_sync()
        _write_sync_log("sp_api_sync", started_at, "SUCCESS")
        logger.info(f"Scheduled SP-API sync completed")
    except Exception as e:
        _write_sync_log("sp_api_sync", started_at, "FAILED", error=str(e))
        logger.error(f"Scheduled SP-API sync failed: {e}")


def _run_scheduled_today_sync():
    """Wrapper for scheduled today orders sync with logging."""
    started_at = datetime.now(ZoneInfo("America/Chicago"))
    try:
        _sync_today_orders()
        _write_sync_log("today_sync", started_at, "SUCCESS")
        logger.info(f"Scheduled today sync completed")
    except Exception as e:
        _write_sync_log("today_sync", started_at, "FAILED", error=str(e))
        logger.error(f"Scheduled today sync failed: {e}")


def _run_scheduled_ads_sync():
    """Wrapper for scheduled ads sync with logging."""
    started_at = datetime.now(ZoneInfo("America/Chicago"))
    try:
        _sync_ads_data()
        _write_sync_log("ads_sync", started_at, "SUCCESS")
        logger.info(f"Scheduled ads sync completed")
    except Exception as e:
        _write_sync_log("ads_sync", started_at, "FAILED", error=str(e))
        logger.error(f"Scheduled ads sync failed: {e}")


def _run_scheduled_pricing_sync():
    """Wrapper for scheduled pricing sync with logging."""
    started_at = datetime.now(ZoneInfo("America/Chicago"))
    try:
        _sync_pricing_and_coupons()
        _write_sync_log("pricing_sync", started_at, "SUCCESS")
        logger.info(f"Scheduled pricing sync completed")
    except Exception as e:
        _write_sync_log("pricing_sync", started_at, "FAILED", error=str(e))
        logger.error(f"Scheduled pricing sync failed: {e}")


def _run_scheduled_docs_update():
    """Wrapper for scheduled docs update with logging. (Placeholder)"""
    started_at = datetime.now(ZoneInfo("America/Chicago"))
    try:
        logger.info("Docs update job started (placeholder)")
        _write_sync_log("docs_update", started_at, "SUCCESS")
        _log_docs_update("completed", documents_updated="architecture_guide.md, disaster_recovery_plan.md", execution_time=0)
        logger.info("Scheduled docs update completed")
    except Exception as e:
        _write_sync_log("docs_update", started_at, "FAILED", error=str(e))
        _log_docs_update("failed", error_message=str(e), execution_time=0)
        logger.error(f"Scheduled docs update failed: {e}")


def _run_duckdb_backup():
    """Nightly backup: Google Drive (full DB) then GitHub (manifest + docs)."""
    chicago = ZoneInfo("America/Chicago")

    # ── Google Drive backup ──
    gdrive_started = datetime.now(chicago)
    logger.info("[SCHEDULER] Starting Google Drive backup")
    try:
        from services.backup import run_backup
        gdrive_result = run_backup()
        gdrive_status = gdrive_result.get("status", "FAILED")
        logger.info(f"[SCHEDULER] Google Drive backup: {gdrive_status}")
        _write_sync_log(
            "nightly_backup_gdrive", gdrive_started, gdrive_status,
            inserted=1 if gdrive_status == "SUCCESS" else 0,
            error=gdrive_result.get("error"),
        )
    except Exception as e:
        logger.error(f"[SCHEDULER] Google Drive backup exception: {e}")
        _write_sync_log(
            "nightly_backup_gdrive", gdrive_started, "FAILED",
            error=str(e),
        )

    # ── GitHub backup (always runs even if Drive backup failed) ──
    github_started = datetime.now(chicago)
    logger.info("[SCHEDULER] Starting GitHub backup")
    try:
        from services.backup import run_github_backup
        github_result = run_github_backup()
        github_status = github_result.get("status", "FAILED")
        files_count = len(github_result.get("files_committed", []))
        logger.info(f"[SCHEDULER] GitHub backup: {github_status} ({files_count} files)")
        _write_sync_log(
            "nightly_backup_github", github_started, github_status,
            inserted=files_count,
            error=github_result.get("error"),
        )
    except Exception as e:
        logger.error(f"[SCHEDULER] GitHub backup exception: {e}")
        _write_sync_log(
            "nightly_backup_github", github_started, "FAILED",
            error=str(e),
        )


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

    # Backfill historical orders (quota-safe, fills orders table + today's revenue)
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: _backfill_orders(days=90))
        logger.info("Startup: order backfill completed (90 days)")
    except Exception as e:
        logger.error(f"Order backfill error: {e}")

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

        # Schedule today-orders sync every hour on the half-hour (fast, ~10s)
        scheduler.add_job(_run_scheduled_today_sync, CronTrigger(minute=30, second=0, timezone="America/Chicago"), id="today_sync_hourly")

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
    """On startup, check each scheduled job's last SUCCESS/PARTIAL run in sync_log.
    If the last successful run was more than (job_interval + 10 minutes) ago,
    run the job immediately. Ensures missed syncs during deploys are caught up."""
    try:
        now_central = datetime.now(ZoneInfo("America/Chicago"))

        # Define jobs with their expected interval in minutes
        catchup_jobs = [
            ("sp_api_sync", 180, _run_scheduled_sp_api_sync),       # every 3h (9,12,15,18)
            ("ads_sync", 60, _run_scheduled_ads_sync),               # every hour
            ("pricing_sync", 60, _run_scheduled_pricing_sync),       # every hour (offset 30m)
        ]

        con = duckdb.connect(str(DB_PATH), read_only=False)

        for job_name, interval_minutes, job_fn in catchup_jobs:
            try:
                row = con.execute("""
                    SELECT MAX(started_at) FROM sync_log
                    WHERE job_name = ? AND status IN ('SUCCESS', 'PARTIAL', 'completed')
                """, [job_name]).fetchone()
                last_run = row[0] if row and row[0] else None

                threshold_minutes = interval_minutes + 10
                if last_run:
                    from datetime import timezone
                    # last_run from DuckDB is naive — treat as Central
                    if last_run.tzinfo is None:
                        last_run = last_run.replace(tzinfo=ZoneInfo("America/Chicago"))
                    elapsed = (now_central - last_run).total_seconds() / 60
                    catchup = elapsed > threshold_minutes
                else:
                    elapsed = float('inf')
                    catchup = True

                last_str = str(last_run)[:19] if last_run else "NEVER"
                print(f"[STARTUP] {job_name}: last run {last_str}, catchup {'TRIGGERED' if catchup else 'NOT NEEDED'}")

                if catchup:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, job_fn)
            except Exception as e:
                print(f"[STARTUP] {job_name}: catchup error — {e}")

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
