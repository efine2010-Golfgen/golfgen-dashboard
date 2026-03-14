"""
GolfGen Analytics Worker — separate process for sync jobs.

Runs independently from the API server. Connects to Postgres via DATABASE_URL.
Handles:
- Startup: restore from backup, initial SP-API sync
- Scheduled: 4x daily SP-API, hourly today-sync, 2-hourly ads, nightly deep sync
- Nightly: backup to Google Drive, analytics rollup
"""
import os
import sys
import asyncio
import logging
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from contextlib import asynccontextmanager

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "webapp", "backend"))

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.config import DB_PATH, DB_DIR, TIMEZONE
from services.sp_api import _sync_today_orders, _run_sp_api_sync, _backfill_orders
from services.ads_api import _sync_ads_data, _sync_pricing_and_coupons
from services.sync_engine import _auto_backfill_if_needed, _write_sync_log, run_nightly_deep_sync
from services.analytics_rollup import run_full_rollup
from services.backup import restore_from_latest_backup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [worker] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("golfgen")

# Module-level scheduler instance
scheduler = None

# Scheduler-level locks to prevent overlapping scheduled runs of the same job type
_scheduler_locks = {
    "sp_api": threading.Lock(),
    "today": threading.Lock(),
    "ads": threading.Lock(),
    "pricing": threading.Lock(),
    "analytics": threading.Lock(),
    "backup": threading.Lock(),
    "docs": threading.Lock(),
}


def _run_scheduled_sp_api_sync():
    """Wrapper for scheduled SP-API sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["sp_api"].acquire(blocking=False):
        logger.warning("Skipping scheduled SP-API sync — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            _run_sp_api_sync()
            _write_sync_log("sp_api_sync", started_at, "SUCCESS")
            logger.info("Scheduled SP-API sync completed")
        except Exception as e:
            _write_sync_log("sp_api_sync", started_at, "FAILED", error=str(e))
            logger.error(f"Scheduled SP-API sync failed: {e}")
    finally:
        _scheduler_locks["sp_api"].release()


def _run_scheduled_today_sync():
    """Wrapper for scheduled today orders sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["today"].acquire(blocking=False):
        logger.warning("Skipping scheduled today sync — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            _sync_today_orders()
            _write_sync_log("today_sync", started_at, "SUCCESS")
            logger.info("Scheduled today sync completed")
        except Exception as e:
            _write_sync_log("today_sync", started_at, "FAILED", error=str(e))
            logger.error(f"Scheduled today sync failed: {e}")
    finally:
        _scheduler_locks["today"].release()


def _run_scheduled_ads_sync():
    """Wrapper for scheduled ads sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["ads"].acquire(blocking=False):
        logger.warning("Skipping scheduled ads sync — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            _sync_ads_data()
            _write_sync_log("ads_sync", started_at, "SUCCESS")
            logger.info("Scheduled ads sync completed")
        except Exception as e:
            _write_sync_log("ads_sync", started_at, "FAILED", error=str(e))
            logger.error(f"Scheduled ads sync failed: {e}")
    finally:
        _scheduler_locks["ads"].release()


def _run_scheduled_pricing_sync():
    """Wrapper for scheduled pricing/coupon sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["pricing"].acquire(blocking=False):
        logger.warning("Skipping scheduled pricing sync — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            _sync_pricing_and_coupons()
            _write_sync_log("pricing_sync", started_at, "SUCCESS")
            logger.info("Scheduled pricing sync completed")
        except Exception as e:
            _write_sync_log("pricing_sync", started_at, "FAILED", error=str(e))
            logger.error(f"Scheduled pricing sync failed: {e}")
    finally:
        _scheduler_locks["pricing"].release()


def _run_scheduled_nightly_deep_sync():
    """Wrapper for nightly deep sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["sp_api"].acquire(blocking=False):
        logger.warning("Skipping nightly deep sync — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            run_nightly_deep_sync()
            _write_sync_log("nightly_deep_sync", started_at, "SUCCESS")
            logger.info("Nightly deep sync completed")
        except Exception as e:
            _write_sync_log("nightly_deep_sync", started_at, "FAILED", error=str(e))
            logger.error(f"Nightly deep sync failed: {e}")
    finally:
        _scheduler_locks["sp_api"].release()


def _run_scheduled_analytics_rollup():
    """Wrapper for analytics rollup with logging and scheduler-level mutex."""
    if not _scheduler_locks["analytics"].acquire(blocking=False):
        logger.warning("Skipping analytics rollup — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            run_full_rollup()
            _write_sync_log("analytics_rollup", started_at, "SUCCESS")
            logger.info("Analytics rollup completed")
        except Exception as e:
            _write_sync_log("analytics_rollup", started_at, "FAILED", error=str(e))
            logger.error(f"Analytics rollup failed: {e}")
    finally:
        _scheduler_locks["analytics"].release()


def _run_scheduled_backup():
    """Wrapper for nightly backup (Google Drive + GitHub) with logging and mutex."""
    if not _scheduler_locks["backup"].acquire(blocking=False):
        logger.warning("Skipping scheduled backup — previous run still active")
        return
    try:
        chicago = ZoneInfo("America/Chicago")

        # Google Drive backup
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

        # GitHub backup (always runs even if Drive backup failed)
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
    finally:
        _scheduler_locks["backup"].release()


async def _sync_loop():
    """Initialize and run APScheduler for background sync jobs.

    Sequence:
    1. Immediately sync today's orders (fast, ~5s)
    2. Auto-backfill historical data if missing (detects fresh deploy)
    3. Schedule 4 daily sync jobs at 9:00, 12:00, 15:00, 18:00 Central
    4. Schedule ads and pricing syncs every 2 hours
    5. Handle startup catchup: if a scheduled time has passed for today with no sync_log entry, run immediately
    """
    global scheduler

    # FIRST: Restore latest Google Drive backup if available.
    # This prevents data loss on Railway redeploys (ephemeral filesystem).
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, restore_from_latest_backup)
        logger.info(f"Startup: DB restore result: {result.get('status')} — {result}")
    except Exception as e:
        logger.error(f"Startup DB restore error (continuing with seed): {e}")

    # Run full SP-API sync on startup (includes financial events with refunds).
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

    # Run analytics rollup on startup so analytics tables are populated immediately
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, run_full_rollup)
        logger.info("Startup: analytics rollup completed — analytics tables populated")
    except Exception as e:
        logger.error(f"Startup analytics rollup error: {e}")

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

        # Schedule ads sync every 2 hours (0, 2, 4, 6, 8, ... hours)
        scheduler.add_job(_run_scheduled_ads_sync, CronTrigger(hour='*/2', minute=0, second=0, timezone="America/Chicago"), id="ads_sync_2hourly")

        # Schedule pricing sync every hour on the half-hour
        scheduler.add_job(_run_scheduled_pricing_sync, CronTrigger(minute=30, second=0, timezone="America/Chicago"), id="pricing_sync_hourly")

        # Schedule nightly backup at 2am Central (Google Drive + GitHub)
        scheduler.add_job(_run_scheduled_backup, CronTrigger(hour=2, minute=0, timezone="America/Chicago"), id="nightly_backup_2am")

        # Schedule nightly deep sync at 3am Central — fills all data gaps + re-pulls last 30 days
        scheduler.add_job(_run_scheduled_nightly_deep_sync, CronTrigger(hour=3, minute=0, timezone="America/Chicago"), id="nightly_deep_sync_3am", misfire_grace_time=7200)

        # Schedule analytics rollup at 2:30am Central (after backup completes)
        scheduler.add_job(_run_scheduled_analytics_rollup, CronTrigger(hour=2, minute=30, timezone="America/Chicago"), id="analytics_rollup_230am", misfire_grace_time=3600)

        await scheduler.start()
        logger.info("APScheduler started with 4 daily SP-API syncs, hourly ads/pricing, and nightly backup (America/Chicago)")

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
            ("ads_sync", 60, _run_scheduled_ads_sync),               # every hour (every 2h)
            ("pricing_sync", 60, _run_scheduled_pricing_sync),       # every hour (offset 30m)
        ]

        from core.database import get_db_rw
        con = get_db_rw()

        for job_name, interval_minutes, job_fn in catchup_jobs:
            try:
                row = con.execute("""
                    SELECT MAX(started_at) FROM sync_log
                    WHERE job_name = ? AND status IN ('SUCCESS', 'PARTIAL', 'completed')
                """, [job_name]).fetchone()
                last_run = row[0] if row and row[0] else None

                threshold_minutes = interval_minutes + 10
                if last_run:
                    # last_run from database is naive — treat as Central
                    if last_run.tzinfo is None:
                        last_run = last_run.replace(tzinfo=ZoneInfo("America/Chicago"))
                    elapsed = (now_central - last_run).total_seconds() / 60
                    catchup = elapsed > threshold_minutes
                else:
                    elapsed = float('inf')
                    catchup = True

                last_str = str(last_run)[:19] if last_run else "NEVER"
                logger.info(f"[STARTUP] {job_name}: last run {last_str}, catchup {'TRIGGERED' if catchup else 'NOT NEEDED'}")

                if catchup:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, job_fn)
            except Exception as e:
                logger.error(f"[STARTUP] {job_name}: catchup error — {e}")

        con.close()
    except Exception as e:
        logger.error(f"Startup sync catchup error: {e}")


async def main():
    """Initialize database and run the sync scheduler."""
    from core.database import init_all_tables

    # Initialize all system and item plan tables
    try:
        init_all_tables()
        logger.info("All tables initialized")
    except Exception as e:
        logger.error(f"Table init error: {e}")

    # Start sync loop
    try:
        await _sync_loop()
        if scheduler and scheduler.running:
            # Keep the event loop running indefinitely
            await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Worker shutting down...")
        if scheduler and scheduler.running:
            await scheduler.shutdown()
    except Exception as e:
        logger.error(f"Worker error: {e}")
        if scheduler and scheduler.running:
            await scheduler.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
