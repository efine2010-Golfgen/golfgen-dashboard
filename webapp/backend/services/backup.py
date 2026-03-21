"""
Google Drive database backup service for GolfGen Dashboard.

Provides:
- run_backup: Export database → compress → upload to Google Drive → enforce retention
  (PostgreSQL: pg_dump-style CSV export; DuckDB: EXPORT DATABASE)
- get_backup_status: Return latest backup info from Google Drive
- _get_drive_service: Authenticate with Google Drive via service account
- _enforce_retention: Delete backups older than 30 days
"""

import os
import json
import tarfile
import tempfile
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from core.database import get_db, get_db_rw
from github import Github
import base64

from core.config import DB_PATH, DB_DIR, USE_POSTGRES

logger = logging.getLogger("golfgen")

# ── Configuration ──────────────────────────────────────────
RETENTION_DAYS = 10
BACKUP_FOLDER_ID = os.environ.get("BACKUP_DRIVE_FOLDER_ID", "")


def _get_settings():
    """Return backup configuration from environment variables."""
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    oauth_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", "")
    oauth_client = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
    oauth_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
    folder_id = os.environ.get("BACKUP_DRIVE_FOLDER_ID", "")
    has_oauth = bool(oauth_token and oauth_client and oauth_secret)
    return {
        "has_credentials": has_oauth or bool(sa_json),
        "has_folder_id": bool(folder_id),
        "folder_id": folder_id,
        "retention_days": RETENTION_DAYS,
    }


def _get_drive_service():
    """Authenticate with Google Drive using OAuth2 refresh token (user account).

    Falls back to service account if OAuth2 credentials are not configured.
    OAuth2 is preferred because service accounts have no storage quota on
    personal Google Drive.
    """
    from googleapiclient.discovery import build

    # Prefer OAuth2 user credentials (has storage quota on personal Drive)
    # Service accounts cannot upload to personal Drive (no storage quota).
    refresh_token = os.environ.get("GOOGLE_DRIVE_REFRESH_TOKEN", "") or os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", "")
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")

    if refresh_token and client_id and client_secret:
        from google.oauth2.credentials import Credentials
        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            client_id=client_id,
            client_secret=client_secret,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        logger.info("Drive auth: using OAuth2 user credentials")
        return build("drive", "v3", credentials=credentials, cache_discovery=False)

    # Fallback to service account
    from google.oauth2 import service_account
    sa_json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json_str:
        raise RuntimeError("No Google Drive credentials configured (need GOOGLE_OAUTH_REFRESH_TOKEN or GOOGLE_SERVICE_ACCOUNT_JSON)")

    try:
        sa_info = json.loads(sa_json_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    logger.info("Drive auth: using service account (warning: no storage quota on personal Drive)")
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def run_backup() -> dict:
    """Execute a full database backup to Google Drive.

    Steps:
    1. Export DuckDB database to Parquet files in a temp directory
    2. Compress the export into a .tar.gz archive
    3. Upload the archive to Google Drive
    4. Enforce retention policy (delete backups older than RETENTION_DAYS)

    Returns a dict with backup details (filename, size, drive_file_id, etc.)
    """
    settings = _get_settings()
    if not settings["has_credentials"]:
        raise RuntimeError("Google service account credentials not configured")
    if not settings["has_folder_id"]:
        raise RuntimeError("BACKUP_DRIVE_FOLDER_ID not configured")

    now = datetime.now(ZoneInfo("America/Chicago"))
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"golfgen_backup_{timestamp}.tar.gz"

    with tempfile.TemporaryDirectory() as tmpdir:
        export_dir = Path(tmpdir) / "export"
        export_dir.mkdir()
        archive_path = Path(tmpdir) / filename

        # Step 1: Export database
        import shutil
        if USE_POSTGRES:
            # PostgreSQL: export each table as CSV using raw psycopg2.
            # Using raw connection (not DbConnection wrapper) because we need
            # reliable cursor.description access for column names.
            import csv
            import psycopg2
            logger.info(f"Backup: Exporting PostgreSQL tables to CSV in {export_dir}")
            pg_conn = psycopg2.connect(os.environ.get("DATABASE_URL", ""))
            pg_conn.autocommit = True
            try:
                cur = pg_conn.cursor()
                cur.execute("""
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public'
                    ORDER BY tablename
                """)
                tables = cur.fetchall()
                exported_count = 0
                for (table_name,) in tables:
                    try:
                        cur.execute(f'SELECT * FROM "{table_name}"')
                        cols = [desc[0] for desc in cur.description]
                        rows = cur.fetchall()
                        csv_path = export_dir / f"{table_name}.csv"
                        with open(csv_path, "w", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow(cols)
                            writer.writerows(rows)
                        exported_count += 1
                        logger.info(f"Backup: Exported {table_name}: {len(rows)} rows")
                    except Exception as e:
                        logger.warning(f"Backup: Failed to export {table_name}: {e}")
                        import traceback
                        logger.warning(traceback.format_exc())
                cur.close()
                logger.info(f"Backup: {exported_count}/{len(tables)} tables exported to CSV")
            finally:
                pg_conn.close()
        else:
            # DuckDB: native EXPORT DATABASE
            logger.info(f"Backup: Exporting DuckDB to Parquet in {export_dir}")
            con = get_db_rw()
            try:
                con.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
            finally:
                con.close()
            # Also include the raw .duckdb file for completeness
            raw_copy = export_dir / "golfgen_amazon.duckdb"
            if DB_PATH.exists():
                shutil.copy2(str(DB_PATH), str(raw_copy))

        # Step 2: Compress into tar.gz
        logger.info(f"Backup: Compressing to {archive_path}")
        with tarfile.open(str(archive_path), "w:gz") as tar:
            tar.add(str(export_dir), arcname="golfgen_export")

        archive_size = archive_path.stat().st_size
        archive_size_mb = round(archive_size / (1024 * 1024), 2)
        logger.info(f"Backup: Archive size = {archive_size_mb} MB")

        # Step 3: Pre-upload: authenticate, free space, then upload
        logger.info(f"Backup: Uploading {filename} to Google Drive folder {settings['folder_id']}")
        drive = _get_drive_service()

        # ── Run retention cleanup BEFORE upload to free space ──
        pre_deleted = _enforce_retention(drive)
        if pre_deleted:
            logger.info(f"Backup: Pre-upload retention deleted {pre_deleted} old file(s)")

        # ── Quota-aware purge if Drive is still tight ──
        purged = _free_space_for_upload(drive, archive_size)
        if purged:
            logger.info(f"Backup: Pre-upload purge freed space by deleting {purged} extra file(s)")

        from googleapiclient.http import MediaFileUpload

        file_metadata = {
            "name": filename,
            "parents": [settings["folder_id"]],
            "description": f"GolfGen DuckDB backup — {now.strftime('%Y-%m-%d %H:%M CT')} — {archive_size_mb} MB",
        }
        media = MediaFileUpload(str(archive_path), mimetype="application/gzip", resumable=True)
        uploaded = drive.files().create(
            body=file_metadata, media_body=media, fields="id,name,size,createdTime",
            supportsAllDrives=True
        ).execute()

        drive_file_id = uploaded.get("id", "")
        logger.info(f"Backup: Uploaded successfully — Drive file ID: {drive_file_id}")

        # Step 4: Second-pass retention (catches any files created between pre-upload check and now)
        deleted_count = _enforce_retention(drive)

    return {
        "status": "SUCCESS",
        "filename": filename,
        "size_mb": archive_size_mb,
        "drive_file_id": drive_file_id,
        "timestamp": now.isoformat(),
        "retention_deleted": deleted_count,
    }


def _enforce_retention(drive) -> int:
    """Delete Google Drive backups older than RETENTION_DAYS.

    Only deletes files matching the naming pattern 'golfgen_backup_*.tar.gz'
    in the configured backup folder.

    Returns the number of files deleted.
    """
    folder_id = os.environ.get("BACKUP_DRIVE_FOLDER_ID", "")
    if not folder_id:
        return 0

    cutoff = datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=RETENTION_DAYS)
    cutoff_rfc = cutoff.strftime("%Y-%m-%dT%H:%M:%S")

    try:
        query = (
            f"'{folder_id}' in parents "
            f"and name contains 'golfgen_backup_' "
            f"and createdTime < '{cutoff_rfc}' "
            f"and trashed = false"
        )
        results = drive.files().list(
            q=query, fields="files(id,name,createdTime)", pageSize=100
        ).execute()
        old_files = results.get("files", [])

        deleted = 0
        for f in old_files:
            try:
                drive.files().delete(fileId=f["id"]).execute()
                logger.info(f"Backup retention: deleted {f['name']} (created {f['createdTime']})")
                deleted += 1
            except Exception as e:
                logger.warning(f"Backup retention: failed to delete {f['name']}: {e}")

        return deleted
    except Exception as e:
        logger.error(f"Backup retention check failed: {e}")
        return 0


def _free_space_for_upload(drive, estimated_size_bytes: int = 0) -> int:
    """Pre-upload quota check: delete oldest backups if Drive is nearly full.

    Runs BEFORE uploading so we never hit the quota wall mid-upload.
    Deletes oldest golfgen backups until at least 2× the estimated upload
    size (or 500 MB minimum) is free.

    Returns number of files deleted.
    """
    deleted = 0
    folder_id = os.environ.get("BACKUP_DRIVE_FOLDER_ID", "")
    if not folder_id:
        return 0

    headroom_needed = max(estimated_size_bytes * 2, 500 * 1024 * 1024)  # min 500 MB

    try:
        # Check current quota
        about = drive.about().get(fields="storageQuota").execute()
        quota = about.get("storageQuota", {})
        total = int(quota.get("limit", 0))
        used  = int(quota.get("usage", 0))
        free  = total - used if total > 0 else headroom_needed + 1

        logger.info(
            f"Backup quota check: {round(used/1e9,2)} GB used / "
            f"{round(total/1e9,2)} GB total / {round(free/1e9,2)} GB free"
        )

        if free >= headroom_needed:
            return 0  # plenty of room

        logger.warning(
            f"Backup: Drive only has {round(free/1e6,0)} MB free — "
            f"need {round(headroom_needed/1e6,0)} MB. Purging old backups."
        )

        # List ALL backups sorted oldest first
        query = (
            f"'{folder_id}' in parents "
            f"and name contains 'golfgen_backup_' "
            f"and trashed = false"
        )
        results = drive.files().list(
            q=query,
            fields="files(id,name,size,createdTime)",
            orderBy="createdTime asc",
            pageSize=200,
        ).execute()
        old_files = results.get("files", [])

        for f in old_files:
            if free >= headroom_needed:
                break
            try:
                file_size = int(f.get("size", 0))
                drive.files().delete(fileId=f["id"]).execute()
                free += file_size
                deleted += 1
                logger.info(
                    f"Backup pre-purge: deleted {f['name']} "
                    f"({round(file_size/1e6,1)} MB freed)"
                )
            except Exception as e:
                logger.warning(f"Backup pre-purge: failed to delete {f['name']}: {e}")

    except Exception as e:
        logger.warning(f"Backup quota check error: {e}")

    return deleted


def verify_latest_backup() -> dict:
    """Download the latest Google Drive backup and verify its integrity.

    Checks:
    1. Archive can be downloaded and extracted
    2. CSV files are present for all expected tables
    3. Row counts in backup match or exceed live PostgreSQL row counts (within 10% tolerance)
    4. No empty CSV files (except system tables)

    Returns a dict with verification status, table-by-table comparison, and any warnings.
    Logged to sync_log as 'backup_verification'.
    """
    settings = _get_settings()
    if not settings["has_credentials"] or not settings["has_folder_id"]:
        return {"status": "SKIPPED", "reason": "Google Drive not configured"}

    chicago = ZoneInfo("America/Chicago")
    started_at = datetime.now(chicago)
    warnings = []
    table_results = {}

    try:
        drive = _get_drive_service()
        folder_id = settings["folder_id"]

        # Find latest backup
        query = (
            f"'{folder_id}' in parents "
            f"and name contains 'golfgen_backup_' "
            f"and trashed = false"
        )
        results = drive.files().list(
            q=query, fields="files(id,name,size,createdTime)",
            orderBy="createdTime desc", pageSize=1,
        ).execute()
        files = results.get("files", [])

        if not files:
            return {"status": "FAILED", "reason": "No backups found on Google Drive"}

        latest = files[0]
        backup_name = latest["name"]
        backup_age_hours = (datetime.now(chicago) - datetime.fromisoformat(
            latest["createdTime"].replace("Z", "+00:00")
        ).astimezone(chicago)).total_seconds() / 3600

        if backup_age_hours > 8:
            warnings.append(f"Latest backup is {backup_age_hours:.1f}h old (expected <8h with 6-hour schedule)")

        logger.info(f"Backup verification: downloading {backup_name} ({latest.get('size', 0)} bytes)")

        # Download
        from googleapiclient.http import MediaIoBaseDownload
        import io
        request = drive.files().get_media(fileId=latest["id"])
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)

        # Extract
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / "backup.tar.gz"
            with open(archive_path, "wb") as f:
                f.write(buffer.read())

            try:
                with tarfile.open(str(archive_path), "r:gz") as tar:
                    tar.extractall(path=tmpdir)
            except Exception as e:
                return {"status": "FAILED", "reason": f"Archive extraction failed: {e}",
                        "backup_name": backup_name}

            export_dir = Path(tmpdir) / "golfgen_export"
            csv_files = sorted(export_dir.glob("*.csv")) if export_dir.exists() else []

            if not csv_files:
                return {"status": "FAILED", "reason": "No CSV files found in backup archive",
                        "backup_name": backup_name}

            # Get live row counts from PostgreSQL
            live_counts = {}
            try:
                con = get_db()
                if USE_POSTGRES:
                    tables_raw = con.execute("""
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public' ORDER BY tablename
                    """).fetchall()
                else:
                    tables_raw = con.execute("SHOW TABLES").fetchall()
                for (tbl,) in tables_raw:
                    try:
                        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
                        live_counts[tbl] = cnt
                    except Exception:
                        live_counts[tbl] = -1
                con.close()
            except Exception as e:
                warnings.append(f"Could not query live DB: {e}")

            # Compare backup CSV row counts to live
            backup_tables = set()
            for csv_file in csv_files:
                tbl = csv_file.stem
                backup_tables.add(tbl)
                try:
                    with open(csv_file, "r") as f:
                        csv_rows = sum(1 for _ in f) - 1  # subtract header
                except Exception:
                    csv_rows = -1

                live_cnt = live_counts.get(tbl, -1)
                # Allow 10% tolerance (backup may be slightly behind live)
                if csv_rows < 0:
                    status = "ERROR"
                    note = "Could not read CSV"
                elif live_cnt < 0:
                    status = "WARN"
                    note = "Table not in live DB"
                elif csv_rows == 0 and tbl not in ("sessions", "audit_log", "user_permissions", "docs_update_log"):
                    status = "WARN"
                    note = "Empty backup (0 rows)"
                    warnings.append(f"{tbl}: backup has 0 rows but live has {live_cnt}")
                elif live_cnt > 0 and csv_rows < live_cnt * 0.9:
                    status = "WARN"
                    note = f"Backup {csv_rows} < 90% of live {live_cnt}"
                    warnings.append(f"{tbl}: backup ({csv_rows}) significantly behind live ({live_cnt})")
                else:
                    status = "OK"
                    note = None

                table_results[tbl] = {
                    "backup_rows": csv_rows,
                    "live_rows": live_cnt,
                    "status": status,
                    "note": note,
                }

            # Check for missing tables
            for tbl in live_counts:
                if tbl not in backup_tables:
                    table_results[tbl] = {
                        "backup_rows": -1, "live_rows": live_counts[tbl],
                        "status": "MISSING", "note": "Not in backup archive",
                    }
                    if live_counts[tbl] > 0:
                        warnings.append(f"{tbl}: missing from backup but has {live_counts[tbl]} live rows")

        ok_count = sum(1 for t in table_results.values() if t["status"] == "OK")
        total = len(table_results)
        elapsed = round((datetime.now(chicago) - started_at).total_seconds(), 1)

        overall_status = "SUCCESS" if not warnings else "PARTIAL"
        logger.info(f"Backup verification: {overall_status} — {ok_count}/{total} tables OK, "
                     f"{len(warnings)} warnings, {elapsed}s")

        return {
            "status": overall_status,
            "backup_name": backup_name,
            "backup_age_hours": round(backup_age_hours, 1),
            "tables_ok": ok_count,
            "tables_total": total,
            "warnings": warnings,
            "tables": table_results,
            "elapsed_seconds": elapsed,
            "verified_at": datetime.now(chicago).isoformat(),
        }

    except Exception as e:
        logger.error(f"Backup verification failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"status": "FAILED", "reason": str(e)}


def get_backup_status() -> dict:
    """Query Google Drive for recent backup files and return status summary.

    Returns:
    {
        "configured": bool,
        "last_backup": { "name", "size_mb", "created", "drive_id" } | None,
        "total_backups": int,
        "backups": [ ... last 10 ... ]
    }
    """
    settings = _get_settings()
    if not settings["has_credentials"] or not settings["has_folder_id"]:
        return {
            "configured": False,
            "error": f"Google Drive backup not configured (creds={settings['has_credentials']}, folder={settings['has_folder_id']})",
            "last_backup": None,
            "total_backups": 0,
            "backups": [],
        }

    try:
        drive = _get_drive_service()
        folder_id = settings["folder_id"]

        query = (
            f"'{folder_id}' in parents "
            f"and name contains 'golfgen_backup_' "
            f"and trashed = false"
        )
        results = drive.files().list(
            q=query,
            fields="files(id,name,size,createdTime)",
            orderBy="createdTime desc",
            pageSize=10,
        ).execute()
        files = results.get("files", [])

        backups = []
        for f in files:
            size_bytes = int(f.get("size", 0))
            backups.append({
                "name": f["name"],
                "size_mb": round(size_bytes / (1024 * 1024), 2),
                "created": f.get("createdTime"),
                "drive_id": f["id"],
            })

        # Get total count
        count_results = drive.files().list(
            q=query,
            fields="files(id)",
            pageSize=1000,
        ).execute()
        total = len(count_results.get("files", []))

        return {
            "configured": True,
            "last_backup": backups[0] if backups else None,
            "total_backups": total,
            "backups": backups,
        }
    except Exception as e:
        logger.error(f"Failed to get backup status from Google Drive: {e}")
        return {
            "configured": True,
            "error": str(e),
            "last_backup": None,
            "total_backups": 0,
            "backups": [],
        }


# ══════════════════════════════════════════════════════════════
# GitHub Backup — manifest + docs committed to a private repo
# ══════════════════════════════════════════════════════════════

BACKUP_GITHUB_REPO = os.environ.get("BACKUP_GITHUB_REPO", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")


def run_github_backup() -> dict:
    """Commit database manifest and docs to a private GitHub repo.

    Commits:
    1. A JSON manifest: all table names and row counts
    2. All files in /app/docs/ if they exist
    3. An updated backup_log.json (last 90 entries)
    4. A README.md if it does not exist yet

    Does NOT commit the full DuckDB binary — too large for GitHub.
    The Google Drive backup holds the full binary.
    GitHub holds the manifest and docs for fast reference and DR.

    Returns dict: status, files_committed, error
    """
    chicago = ZoneInfo("America/Chicago")
    now = datetime.now(chicago)
    date_str = now.strftime("%Y-%m-%d")

    result = {
        "status": "FAILED",
        "files_committed": [],
        "error": None,
    }

    try:
        token = os.environ.get("GITHUB_TOKEN", "")
        repo_name = os.environ.get("BACKUP_GITHUB_REPO", "")

        if not token:
            raise RuntimeError("GITHUB_TOKEN environment variable is not set")
        if not repo_name:
            raise RuntimeError("BACKUP_GITHUB_REPO environment variable is not set")

        g = Github(token)
        repo = g.get_repo(repo_name)
        files_to_commit = {}

        # ── File 1: Database manifest ──
        con = get_db()
        if USE_POSTGRES:
            tables_raw = con.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
            """).fetchall()
        else:
            tables_raw = con.execute("SHOW TABLES").fetchall()
        manifest = {
            "backup_time": now.isoformat(),
            "db_engine": "postgresql" if USE_POSTGRES else "duckdb",
            "db_path": str(DB_PATH),
            "tables": {},
        }
        for (table_name,) in tables_raw:
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                manifest["tables"][table_name] = {"row_count": count}
            except Exception as e:
                manifest["tables"][table_name] = {"row_count": "ERROR", "error": str(e)}
        con.close()

        manifest_json = json.dumps(manifest, indent=2)
        files_to_commit[f"manifests/manifest_{date_str}.json"] = manifest_json
        files_to_commit["manifests/latest.json"] = manifest_json
        logger.info(f"[GITHUB BACKUP] Manifest built: {len(manifest['tables'])} tables")

        # ── File 2: Docs ──
        docs_dir = "/app/docs"
        if os.path.exists(docs_dir):
            for doc_file in os.listdir(docs_dir):
                doc_path = os.path.join(docs_dir, doc_file)
                if os.path.isfile(doc_path):
                    try:
                        with open(doc_path, "rb") as f:
                            content = f.read().decode("utf-8", errors="replace")
                        files_to_commit[f"docs/{doc_file}"] = content
                        logger.info(f"[GITHUB BACKUP] Added doc: {doc_file}")
                    except Exception as e:
                        logger.warning(f"[GITHUB BACKUP] Skipped doc {doc_file}: {e}")

        # ── File 3: Update backup log ──
        new_entry = {
            "date": date_str,
            "time": now.isoformat(),
            "table_counts": {
                k: v["row_count"] for k, v in manifest["tables"].items()
            },
        }
        try:
            existing = repo.get_contents("backup_log.json")
            log_data = json.loads(existing.decoded_content.decode())
            if not isinstance(log_data, list):
                log_data = []
        except Exception:
            log_data = []

        log_data.insert(0, new_entry)
        log_data = log_data[:90]  # Keep last 90 entries
        files_to_commit["backup_log.json"] = json.dumps(log_data, indent=2)

        # ── File 4: README (only if it does not exist) ──
        try:
            repo.get_contents("README.md")
        except Exception:
            files_to_commit["README.md"] = (
                "# GolfGen Dashboard \u2014 Backup Repository\n\n"
                "This repository contains automated nightly backups of the\n"
                "GolfGen dashboard database.\n\n"
                "## Contents\n\n"
                "- `/manifests/` \u2014 Daily JSON manifests with table names and row counts\n"
                "- `/manifests/latest.json` \u2014 Most recent manifest (always current)\n"
                "- `/docs/` \u2014 Auto-generated architecture and disaster recovery docs\n"
                "- `/backup_log.json` \u2014 Log of all backup runs (last 90 days)\n\n"
                "## Restore Process\n\n"
                "See `/docs/` for the full disaster recovery plan.\n\n"
                "Quick reference:\n"
                "1. Restore full DuckDB binary from Google Drive backup folder\n"
                "2. Verify row counts against nearest manifest in `/manifests/`\n"
                "3. Run SP-API backfill for any gap period since last backup\n\n"
                "## Backup Schedule\n\n"
                "Runs nightly at 2:00 AM Central time.\n"
                "- Google Drive: full DuckDB export (compressed tar.gz)\n"
                "- GitHub: manifest + docs (this repo)\n\n"
                "## Row Count History\n\n"
                "See backup_log.json for daily row count history across all tables.\n"
            )

        # ── Commit all files to GitHub ──
        commit_message = (
            f"backup: {date_str} automated nightly backup \u2014 "
            f"{len(manifest['tables'])} tables"
        )

        committed = []
        for file_path, content in files_to_commit.items():
            try:
                try:
                    existing_file = repo.get_contents(file_path)
                    repo.update_file(file_path, commit_message, content, existing_file.sha)
                except Exception:
                    repo.create_file(file_path, commit_message, content)
                committed.append(file_path)
                logger.info(f"[GITHUB BACKUP] Committed: {file_path}")
            except Exception as e:
                logger.warning(f"[GITHUB BACKUP] Failed {file_path}: {e}")

        result["status"] = "SUCCESS" if committed else "FAILED"
        result["files_committed"] = committed
        logger.info(f"[GITHUB BACKUP] Complete: {len(committed)} files committed")
        return result

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"[GITHUB BACKUP] FAILED: {e}")
        return result


def get_github_backup_status() -> dict:
    """Return last GitHub backup info from sync_log.

    Used by GET /api/backup/github-status endpoint.
    """
    repo_name = os.environ.get("BACKUP_GITHUB_REPO", "")
    try:
        con = get_db_rw()
        row = con.execute("""
            SELECT started_at, completed_at, status,
                   records_processed, error_message
            FROM sync_log
            WHERE job_name = 'nightly_backup_github'
            ORDER BY started_at DESC
            LIMIT 1
        """).fetchone()
        con.close()

        if not row:
            return {
                "status": "NO_BACKUP_FOUND",
                "last_backup_time": None,
                "files_committed": 0,
                "repo": repo_name,
                "configured": bool(os.environ.get("GITHUB_TOKEN")) and bool(repo_name),
                "note": "Backup has not run yet or log entry missing",
            }

        return {
            "status": row[2],
            "last_backup_time": str(row[0]),
            "completed_at": str(row[1]),
            "files_committed": row[3] or 0,
            "error": row[4],
            "repo": repo_name,
            "repo_url": f"https://github.com/{repo_name}" if repo_name else None,
            "configured": True,
        }
    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e),
            "repo": repo_name,
            "configured": bool(os.environ.get("GITHUB_TOKEN")) and bool(repo_name),
        }


# ── Restore from Google Drive backup on startup ─────────────────
def restore_from_latest_backup() -> dict:
    """Download the latest Google Drive backup and restore DuckDB from it.

    Called on container startup BEFORE any sync jobs. If a backup exists and
    is newer than the seed database, the raw .duckdb file inside the archive
    replaces the seed. This ensures every deploy starts with the most recent
    data rather than the stale git-committed snapshot.

    Returns a dict with restore status and details.
    """
    settings = _get_settings()
    if not settings["has_credentials"] or not settings["has_folder_id"]:
        logger.info("Restore: Google Drive not configured — using seed DB")
        return {"status": "SKIPPED", "reason": "Google Drive not configured"}

    try:
        drive = _get_drive_service()
        folder_id = settings["folder_id"]

        # Find the most recent backup
        query = (
            f"'{folder_id}' in parents "
            f"and name contains 'golfgen_backup_' "
            f"and trashed = false"
        )
        results = drive.files().list(
            q=query,
            fields="files(id,name,size,createdTime)",
            orderBy="createdTime desc",
            pageSize=1,
        ).execute()
        files = results.get("files", [])

        if not files:
            logger.info("Restore: No backups found on Google Drive — using seed DB")
            return {"status": "SKIPPED", "reason": "No backups found"}

        latest = files[0]
        backup_name = latest["name"]
        backup_id = latest["id"]
        backup_size = int(latest.get("size", 0))
        backup_size_mb = round(backup_size / (1024 * 1024), 2)
        logger.info(f"Restore: Found backup {backup_name} ({backup_size_mb} MB) — downloading...")

        # Download the backup archive
        from googleapiclient.http import MediaIoBaseDownload
        import io

        request = drive.files().get_media(fileId=backup_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        buffer.seek(0)
        logger.info(f"Restore: Downloaded {backup_size_mb} MB")

        # Extract the raw .duckdb file from the archive
        import shutil
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / "backup.tar.gz"
            with open(archive_path, "wb") as f:
                f.write(buffer.read())

            with tarfile.open(str(archive_path), "r:gz") as tar:
                tar.extractall(path=tmpdir)

            # Look for the .duckdb file in the extracted archive
            duckdb_file = None
            for root, dirs, fnames in os.walk(tmpdir):
                for fname in fnames:
                    if fname.endswith(".duckdb"):
                        duckdb_file = Path(root) / fname
                        break
                if duckdb_file:
                    break

            if not duckdb_file:
                logger.error("Restore: No .duckdb file found in backup archive")
                return {"status": "FAILED", "reason": "No .duckdb in archive"}

            restored_size = duckdb_file.stat().st_size
            restored_size_mb = round(restored_size / (1024 * 1024), 2)

            if USE_POSTGRES:
                # PostgreSQL mode: look for CSV files in the archive and
                # restore any table whose backup has more rows than current.
                # This handles disaster recovery (empty DB after a Postgres
                # service reset) while safely no-oping on routine redeploys.
                export_dir_path = Path(tmpdir) / "golfgen_export"
                csv_files = sorted(export_dir_path.glob("*.csv")) if export_dir_path.exists() else []

                if not csv_files:
                    logger.info(
                        "Restore: PostgreSQL mode — no CSV files in archive "
                        "(likely an older DuckDB-format backup). Skipping."
                    )
                    return {
                        "status": "SKIPPED",
                        "reason": "PostgreSQL mode — no CSV files in backup archive (older backup format)",
                    }

                # Tables that must never be overwritten during restore
                SKIP_TABLES = {"sessions", "audit_log", "user_permissions"}

                restored_tables: list[str] = []
                skipped_tables: list[str] = []

                con = get_db_rw()
                try:
                    raw_conn = con._conn  # raw psycopg2 connection

                    for csv_file in csv_files:
                        table_name = csv_file.stem  # strip .csv

                        if table_name in SKIP_TABLES:
                            skipped_tables.append(f"{table_name} (security — always skipped)")
                            continue

                        try:
                            # Count rows in backup CSV (subtract 1 for header)
                            with open(csv_file, "r") as f:
                                csv_row_count = sum(1 for _ in f) - 1

                            # Count rows in live PostgreSQL table
                            try:
                                current_count = con.execute(
                                    f"SELECT COUNT(*) FROM {table_name}"
                                ).fetchone()[0]
                            except Exception:
                                current_count = 0  # table may not exist yet

                            if csv_row_count <= current_count and current_count > 0:
                                skipped_tables.append(
                                    f"{table_name} (backup {csv_row_count} ≤ current {current_count})"
                                )
                                continue

                            logger.info(
                                f"Restore: {table_name}: backup={csv_row_count} rows, "
                                f"current={current_count} rows — restoring"
                            )

                            # Disable autocommit so TRUNCATE + COPY are one unit
                            raw_conn.autocommit = False
                            cursor = raw_conn.cursor()
                            try:
                                cursor.execute(f"TRUNCATE TABLE {table_name}")
                                with open(csv_file, "r") as f:
                                    cursor.copy_expert(
                                        f"COPY {table_name} FROM STDIN CSV HEADER",
                                        f,
                                    )
                                raw_conn.commit()
                                restored_tables.append(f"{table_name} ({csv_row_count} rows)")
                                logger.info(f"Restore: {table_name} — OK ({csv_row_count} rows loaded)")
                            except Exception as e:
                                raw_conn.rollback()
                                skipped_tables.append(f"{table_name} (error: {e})")
                                logger.warning(f"Restore: {table_name} — FAILED: {e}")
                            finally:
                                raw_conn.autocommit = True

                        except Exception as e:
                            skipped_tables.append(f"{table_name} (outer error: {e})")
                            logger.warning(f"Restore: {table_name} outer error — {e}")
                finally:
                    con.close()

                if restored_tables:
                    logger.info(
                        f"Restore: SUCCESS — {len(restored_tables)} tables restored from {backup_name}"
                    )
                    return {
                        "status": "SUCCESS",
                        "backup_name": backup_name,
                        "backup_size_mb": backup_size_mb,
                        "restored_tables": restored_tables,
                        "skipped_tables": skipped_tables,
                    }
                else:
                    logger.info("Restore: SKIPPED — PostgreSQL already has equal or more data than backup")
                    return {
                        "status": "SKIPPED",
                        "reason": "PostgreSQL already up-to-date (all tables have ≥ backup row counts)",
                        "skipped_tables": skipped_tables,
                    }

            # DuckDB mode: compare row counts and restore if backup is newer
            current_size = DB_PATH.stat().st_size if DB_PATH.exists() else 0
            current_size_mb = round(current_size / (1024 * 1024), 2)

            # Count rows in current DB
            try:
                import duckdb
                cur_con = duckdb.connect(str(DB_PATH), read_only=True)
                cur_rows = cur_con.execute("SELECT COUNT(*) FROM daily_sales").fetchone()[0]
                cur_con.close()
            except Exception:
                cur_rows = 0

            # Count rows in backup DB
            try:
                import duckdb
                bak_con = duckdb.connect(str(duckdb_file), read_only=True)
                bak_rows = bak_con.execute("SELECT COUNT(*) FROM daily_sales").fetchone()[0]
                bak_con.close()
            except Exception:
                bak_rows = 0

            if bak_rows <= cur_rows and cur_rows > 0:
                logger.info(f"Restore: Backup DB ({bak_rows} rows, {restored_size_mb} MB) <= current ({cur_rows} rows, {current_size_mb} MB) — keeping current")
                return {"status": "SKIPPED", "reason": f"Backup ({bak_rows} rows) not larger than current ({cur_rows} rows)"}

            logger.info(f"Restore: Backup has more data ({bak_rows} rows vs {cur_rows} rows)")

            # Replace the database file
            logger.info(f"Restore: Replacing DB ({current_size_mb} MB → {restored_size_mb} MB)")
            shutil.copy2(str(duckdb_file), str(DB_PATH))

            # Also remove any .wal file that might be stale
            wal_path = DB_PATH.with_suffix(".duckdb.wal")
            if wal_path.exists():
                wal_path.unlink()
                logger.info("Restore: Removed stale .wal file")

            logger.info(f"Restore: SUCCESS — restored from {backup_name}")
            return {
                "status": "SUCCESS",
                "backup_name": backup_name,
                "backup_size_mb": backup_size_mb,
                "restored_db_size_mb": restored_size_mb,
                "previous_db_size_mb": current_size_mb,
            }

    except Exception as e:
        logger.error(f"Restore: Failed — {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"status": "FAILED", "reason": str(e)}
