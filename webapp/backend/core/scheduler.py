"""APScheduler setup, scheduled job wrappers, and startup sync logic."""
import os
import json
import shutil
import asyncio
import logging
import time
import threading
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.config import DB_PATH, DB_DIR, TIMEZONE, SYNC_INTERVAL_HOURS
from services.sp_api import _sync_today_orders, _run_sp_api_sync, _backfill_orders
from services.ads_api import _sync_ads_data, _sync_pricing_and_coupons
from services.sync_engine import _auto_backfill_if_needed, _write_sync_log, run_nightly_deep_sync, run_incremental_gap_fill, run_fba_inventory_snapshot
from services.analytics_rollup import run_full_rollup
from services.backup import restore_from_latest_backup

logger = logging.getLogger("golfgen")

# Module-level scheduler instance (will be started in lifespan)
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
    "inventory_snapshot": threading.Lock(),
    "hourly_sales": threading.Lock(),
}


def _log_docs_update(status: str = "in_progress", documents_updated: str = None, error_message: str = None, execution_time: float = None) -> int:
    """Log a docs update to the docs_update_log table. Returns the log ID."""
    try:
        from .database import get_db
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


def _run_scheduled_sp_api_sync():
    """Wrapper for scheduled SP-API sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["sp_api"].acquire(blocking=False):
        logger.warning("Skipping scheduled SP-API sync — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            section_errors = _run_sp_api_sync() or []
            if not section_errors:
                status = "SUCCESS"
                error_str = None
            elif len(section_errors) == 4:
                # All 4 sections failed — complete failure
                status = "FAILED"
                error_str = "; ".join(f"{s}: {e[:60]}" for s, e in section_errors[:2])
            else:
                # Some sections failed — partial success
                status = "PARTIAL"
                error_str = "; ".join(f"{s}: {e[:60]}" for s, e in section_errors)
            _write_sync_log("sp_api_sync", started_at, status, error=error_str)
            logger.info(f"Scheduled SP-API sync {status} ({len(section_errors)} section failures)")
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
            logger.info(f"Scheduled today sync completed")
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
            logger.info(f"Scheduled ads sync completed")
        except Exception as e:
            _write_sync_log("ads_sync", started_at, "FAILED", error=str(e))
            logger.error(f"Scheduled ads sync failed: {e}")
    finally:
        _scheduler_locks["ads"].release()


def _run_scheduled_pricing_sync():
    """Wrapper for scheduled pricing sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["pricing"].acquire(blocking=False):
        logger.warning("Skipping scheduled pricing sync — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            _sync_pricing_and_coupons()
            _write_sync_log("pricing_sync", started_at, "SUCCESS")
            logger.info(f"Scheduled pricing sync completed")
        except Exception as e:
            _write_sync_log("pricing_sync", started_at, "FAILED", error=str(e))
            logger.error(f"Scheduled pricing sync failed: {e}")
    finally:
        _scheduler_locks["pricing"].release()


def _run_scheduled_docs_update():
    """Wrapper for scheduled docs update with logging and scheduler-level mutex. (Placeholder)"""
    if not _scheduler_locks["docs"].acquire(blocking=False):
        logger.warning("Skipping scheduled docs update — previous run still active")
        return
    try:
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
    finally:
        _scheduler_locks["docs"].release()


def _run_scheduled_analytics_rollup():
    """Wrapper for scheduled analytics rollup with logging and scheduler-level mutex."""
    if not _scheduler_locks["analytics"].acquire(blocking=False):
        logger.warning("Skipping scheduled analytics rollup — previous run still active")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            result = run_full_rollup()
            _write_sync_log("analytics_rollup", started_at, "SUCCESS",
                            inserted=(result.get("daily_rows", 0) + result.get("sku_rows", 0)))
            logger.info(f"Scheduled analytics rollup completed: {result}")
        except Exception as e:
            _write_sync_log("analytics_rollup", started_at, "FAILED", error=str(e))
            logger.error(f"Scheduled analytics rollup failed: {e}")
    finally:
        _scheduler_locks["analytics"].release()


def _run_scheduled_nightly_deep_sync():
    """Wrapper for nightly deep sync with logging and scheduler-level mutex."""
    if not _scheduler_locks["sp_api"].acquire(blocking=False):
        logger.warning("Skipping nightly deep sync — SP-API sync already running")
        _write_sync_log("nightly_deep_sync",
                        datetime.now(ZoneInfo("America/Chicago")), "SKIPPED",
                        error="SP-API sync already running — mutex unavailable")
        return
    try:
        result = run_nightly_deep_sync()
        logger.info(f"Nightly deep sync result: {result}")
    except Exception as e:
        import traceback
        logger.error(f"Nightly deep sync failed: {e}\n{traceback.format_exc()}")
        _write_sync_log("nightly_deep_sync",
                        datetime.now(ZoneInfo("America/Chicago")), "FAILED",
                        error=str(e)[:500])
    finally:
        _scheduler_locks["sp_api"].release()


def _run_scheduled_gap_fill():
    """Pull exactly one missing historical chunk (30 days) every 2 hours.

    Fills historical gaps gradually without exhausting quota:
    - 12 gap-fill calls/day + 4 full sync calls = 16 total createReport calls/day
    - Amazon burst limit is 15; spacing at 2-hour intervals keeps us safe
    - No-op when coverage >= 97% so it self-deactivates once gaps are filled
    - Skips if main SP-API sync is currently running (shares sp_api lock)
    """
    if not _scheduler_locks["sp_api"].acquire(blocking=False):
        logger.info("Gap-fill: skipping — main SP-API sync is running")
        return
    try:
        result = run_incremental_gap_fill()
        status = result.get("status", "UNKNOWN")
        if status == "SUCCESS":
            logger.info(
                f"Gap-fill: filled {result.get('chunk')} — "
                f"{result.get('records')} records, "
                f"{result.get('remaining_missing_days')} days still missing"
            )
        elif status == "COMPLETE":
            logger.info(f"Gap-fill: coverage {result.get('coverage_pct')}% — nothing to fill")
        else:
            logger.warning(f"Gap-fill: {status} — {result.get('reason') or result.get('retry_in')}")
    except Exception as e:
        logger.error(f"Gap-fill job failed: {e}")
    finally:
        _scheduler_locks["sp_api"].release()


def _run_scheduled_inventory_snapshot():
    """Wrapper for daily FBA inventory snapshot with scheduler-level mutex."""
    if not _scheduler_locks["inventory_snapshot"].acquire(blocking=False):
        logger.warning("Skipping inventory snapshot — previous run still active")
        return
    try:
        result = run_fba_inventory_snapshot()
        logger.info(f"Daily FBA inventory snapshot: {result}")
    except Exception as e:
        logger.error(f"FBA inventory snapshot failed: {e}")
    finally:
        _scheduler_locks["inventory_snapshot"].release()


def _run_scheduled_hourly_sales_save():
    """Hourly job: save today's + yesterday's hourly SP-API sales to hourly_sales table.

    Runs every hour at :05 past the hour so data has settled since the prior hour.
    Accumulates 30+ days of hourly data for the Sales heatmap chart.
    """
    if not _scheduler_locks["hourly_sales"].acquire(blocking=False):
        logger.warning("Skipping hourly sales save — previous run still active")
        return
    try:
        from routers.sales import save_hourly_to_db
        from datetime import date, timedelta
        from zoneinfo import ZoneInfo
        today_ct = datetime.now(ZoneInfo("America/Chicago")).date()
        yesterday = today_ct - timedelta(days=1)
        saved_today = save_hourly_to_db(today_ct)
        saved_yday  = save_hourly_to_db(yesterday)
        logger.info(f"Hourly sales save: today={saved_today} hours, yesterday={saved_yday} hours")
    except Exception as e:
        logger.error(f"Hourly sales save failed: {e}")
    finally:
        _scheduler_locks["hourly_sales"].release()


def _backfill_hourly_history(max_days: int = 30, max_fills_per_run: int = 5):
    """Backfill hourly_sales for the last `max_days` days.

    Checks which dates are missing or have fewer than 12 recorded hours,
    then fetches from SP-API (via save_hourly_to_db) for up to
    `max_fills_per_run` dates per invocation to avoid API overload.

    Called: at startup + daily at 4:30am CT.
    """
    try:
        from routers.sales import save_hourly_to_db
        from .database import get_db
        today_ct = datetime.now(ZoneInfo("America/Chicago")).date()

        # Build list of past dates to check (exclude today — hourly save handles today)
        dates_to_check = [today_ct - timedelta(days=i) for i in range(1, max_days + 1)]

        # Query which dates already have >= 12 hours of data
        con = get_db()
        try:
            rows = con.execute("""
                SELECT sale_date, COUNT(*) as hour_count
                FROM hourly_sales
                WHERE sale_date >= %s AND sale_date < %s
                  AND division = 'all' AND customer = 'amazon'
                GROUP BY sale_date
            """, [str(dates_to_check[-1]), str(today_ct)]).fetchall()
        except Exception:
            rows = []
        con.close()

        # Build set of dates with adequate data (>= 12 hours)
        covered = {str(r[0]) for r in rows if int(r[1]) >= 12}

        # Find missing dates (oldest first so we fill in chronological order)
        missing = [d for d in reversed(dates_to_check) if str(d) not in covered]

        if not missing:
            logger.info(f"_backfill_hourly_history: all {max_days} days have data — nothing to backfill")
            return

        logger.info(f"_backfill_hourly_history: {len(missing)} dates need backfill (will process up to {max_fills_per_run})")
        filled = 0
        for d in missing[:max_fills_per_run]:
            try:
                saved = save_hourly_to_db(d)
                logger.info(f"  backfill {d}: {saved} hours saved")
                filled += 1
            except Exception as ex:
                logger.warning(f"  backfill {d} failed: {ex}")

        logger.info(f"_backfill_hourly_history: filled {filled} dates, {len(missing) - filled} still pending")
    except Exception as ex:
        logger.warning(f"_backfill_hourly_history error: {ex}")


def _run_scheduled_hourly_backfill():
    """Daily job: backfill any missing hourly sales history for the past 30 days."""
    if not _scheduler_locks["hourly_sales"].acquire(blocking=False):
        logger.warning("Skipping hourly backfill — hourly_sales lock held")
        return
    try:
        _backfill_hourly_history(max_days=30, max_fills_per_run=7)
    except Exception as e:
        logger.error(f"Hourly backfill job failed: {e}")
    finally:
        _scheduler_locks["hourly_sales"].release()


def _run_scheduled_backup_verification():
    """Weekly backup verification — downloads latest backup and compares to live DB."""
    if not _scheduler_locks["backup"].acquire(blocking=False):
        logger.warning("Skipping backup verification — backup job is running")
        return
    try:
        started_at = datetime.now(ZoneInfo("America/Chicago"))
        try:
            from services.backup import verify_latest_backup
            result = verify_latest_backup()
            status = result.get("status", "FAILED")
            _write_sync_log("backup_verification", started_at, status,
                            inserted=result.get("tables_ok", 0),
                            error="; ".join(result.get("warnings", [])[:3]) or None)
            logger.info(f"Backup verification: {status} — {result.get('tables_ok', 0)}/{result.get('tables_total', 0)} tables OK")
        except Exception as e:
            _write_sync_log("backup_verification", started_at, "FAILED", error=str(e))
            logger.error(f"Backup verification failed: {e}")
    finally:
        _scheduler_locks["backup"].release()


def _run_duckdb_backup():
    """Nightly backup: Google Drive (full DB) then GitHub (manifest + docs). Scheduler-level mutex."""
    if not _scheduler_locks["backup"].acquire(blocking=False):
        logger.warning("Skipping scheduled backup — previous run still active")
        return
    try:
        _run_duckdb_backup_inner()
    finally:
        _scheduler_locks["backup"].release()


def _run_duckdb_backup_inner():
    """Actual backup logic — called inside mutex."""
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

    When USE_POSTGRES is True and WORKER_MODE env var is not set,
    skip all sync — the separate worker service handles it.

    Sequence:
    1. Immediately sync today's orders (fast, ~5s) — so "Today" works right away
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

    # NOTE: _run_sp_api_sync already calls _sync_today_orders internally as step 1.
    # No need to call it separately — that was causing a double-sync where the second
    # run (inside _run_sp_api_sync) could overwrite data from the first.

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

    # Backfill historical orders — only when table is thin (fresh deploy / first run)
    # Skipped if we already have a healthy order history, to protect Orders API quota.
    try:
        from core.database import get_db as _get_db
        _con = _get_db()
        _order_count = _con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        _con.close()
        if _order_count < 50:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: _backfill_orders(days=90))
            logger.info("Startup: order backfill completed (90 days)")
        else:
            logger.info(f"Startup: skipping order backfill — {_order_count} orders already in table")
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
        import traceback
        logger.error(f"Startup analytics rollup error: {e}\n{traceback.format_exc()}")

    # Initialize APScheduler
    # IMPORTANT: Use ZoneInfo object (not string) for reliable timezone handling.
    # APScheduler 3.x with string timezone was firing jobs at UTC times instead
    # of Central, causing all nightly jobs to run 5 hours late (2am→7am, 3am→8am).
    _tz = ZoneInfo("America/Chicago")

    if not scheduler:
        scheduler = AsyncIOScheduler(timezone=_tz)

        # Schedule 4 daily SP-API syncs at 9am, 12pm, 3pm, 6pm Central
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=9, minute=0, timezone=_tz), id="sp_api_sync_9am", misfire_grace_time=3600)
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=12, minute=0, timezone=_tz), id="sp_api_sync_12pm", misfire_grace_time=3600)
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=15, minute=0, timezone=_tz), id="sp_api_sync_3pm", misfire_grace_time=3600)
        scheduler.add_job(_run_scheduled_sp_api_sync, CronTrigger(hour=18, minute=0, timezone=_tz), id="sp_api_sync_6pm", misfire_grace_time=3600)

        # Schedule today-orders sync every 15 minutes (fast, ~10s)
        scheduler.add_job(_run_scheduled_today_sync, CronTrigger(minute='*/15', second=0, timezone=_tz), id="today_sync_15min")

        # Schedule ads sync every 2 hours (0, 2, 4, 6, 8, ... hours)
        scheduler.add_job(_run_scheduled_ads_sync, CronTrigger(hour='*/2', minute=0, second=0, timezone=_tz), id="ads_sync_2hourly")

        # Schedule pricing sync every 2 hours (offset by 1 hour from ads)
        scheduler.add_job(_run_scheduled_pricing_sync, CronTrigger(minute=30, second=0, timezone=_tz), id="pricing_sync_hourly")

        # Schedule docs update at 8am and 8pm Central
        scheduler.add_job(_run_scheduled_docs_update, CronTrigger(hour=8, minute=0, timezone=_tz), id="docs_update_8am")
        scheduler.add_job(_run_scheduled_docs_update, CronTrigger(hour=20, minute=0, timezone=_tz), id="docs_update_8pm")

        # Schedule backups every 6 hours (2am, 8am, 2pm, 8pm Central)
        scheduler.add_job(_run_duckdb_backup, CronTrigger(hour="2,8,14,20", minute=0, timezone=_tz), id="backup_6hourly", misfire_grace_time=7200)

        # Schedule nightly deep sync at 3am Central — fills all data gaps + re-pulls last 30 days
        scheduler.add_job(_run_scheduled_nightly_deep_sync, CronTrigger(hour=3, minute=0, timezone=_tz), id="nightly_deep_sync_3am", misfire_grace_time=7200)

        # Schedule incremental gap-fill every 2 hours at :45 (avoids :00 full syncs)
        scheduler.add_job(_run_scheduled_gap_fill, CronTrigger(hour='*/2', minute=45, second=0, timezone=_tz), id="gap_fill_2hourly")

        # Schedule hourly sales save at :05 past every hour (accumulates heatmap data)
        scheduler.add_job(_run_scheduled_hourly_sales_save, CronTrigger(minute=5, second=0, timezone=_tz), id="hourly_sales_save", misfire_grace_time=1800)

        # Schedule 30-day hourly history backfill at 4:30am Central daily
        scheduler.add_job(_run_scheduled_hourly_backfill, CronTrigger(hour=4, minute=30, timezone=_tz), id="hourly_backfill_430am", misfire_grace_time=3600)

        # Schedule daily FBA inventory snapshot at 11pm Central (after all syncs, before backup)
        scheduler.add_job(_run_scheduled_inventory_snapshot, CronTrigger(hour=23, minute=0, timezone=_tz), id="fba_inventory_snapshot_11pm", misfire_grace_time=3600)

        # Schedule analytics rollup at 2:30am Central (after backup completes)
        scheduler.add_job(_run_scheduled_analytics_rollup, CronTrigger(hour=2, minute=30, timezone=_tz), id="analytics_rollup_230am", misfire_grace_time=3600)

        # Schedule weekly backup verification — Sundays at 4am Central
        scheduler.add_job(_run_scheduled_backup_verification, CronTrigger(day_of_week="sun", hour=4, minute=0, timezone=_tz), id="backup_verification_weekly", misfire_grace_time=7200)

        scheduler.start()
        logger.info("APScheduler started — all CronTriggers use ZoneInfo('America/Chicago') for correct Central Time scheduling")

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
            ("nightly_backup_gdrive", 1440, _run_duckdb_backup),     # daily at 2am
            ("nightly_deep_sync", 1440, _run_scheduled_nightly_deep_sync),  # daily at 3am
        ]

        from .database import get_db_rw
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
