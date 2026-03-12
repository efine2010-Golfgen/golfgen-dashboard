"""
Google Drive database backup service for GolfGen Dashboard.

Provides:
- run_backup: Export DuckDB → compress → upload to Google Drive → enforce retention
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

import duckdb

from core.config import DB_PATH, DB_DIR

logger = logging.getLogger("golfgen")

# ── Configuration ──────────────────────────────────────────
RETENTION_DAYS = 30
BACKUP_FOLDER_ID = os.environ.get("BACKUP_DRIVE_FOLDER_ID", "")


def _get_settings():
    """Return backup configuration from environment variables."""
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    folder_id = os.environ.get("BACKUP_DRIVE_FOLDER_ID", "")
    return {
        "has_credentials": bool(sa_json),
        "has_folder_id": bool(folder_id),
        "folder_id": folder_id,
        "retention_days": RETENTION_DAYS,
    }


def _get_drive_service():
    """Authenticate with Google Drive using service account JSON from env var."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    sa_json_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not sa_json_str:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON environment variable is not set")

    try:
        sa_info = json.loads(sa_json_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
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

        # Step 1: Export DuckDB to Parquet
        logger.info(f"Backup: Exporting DuckDB to Parquet in {export_dir}")
        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            con.execute(f"EXPORT DATABASE '{export_dir}' (FORMAT PARQUET)")
        finally:
            con.close()

        # Also include the raw .duckdb file for completeness
        import shutil
        raw_copy = export_dir / "golfgen_amazon.duckdb"
        shutil.copy2(str(DB_PATH), str(raw_copy))

        # Step 2: Compress into tar.gz
        logger.info(f"Backup: Compressing to {archive_path}")
        with tarfile.open(str(archive_path), "w:gz") as tar:
            tar.add(str(export_dir), arcname="golfgen_export")

        archive_size = archive_path.stat().st_size
        archive_size_mb = round(archive_size / (1024 * 1024), 2)
        logger.info(f"Backup: Archive size = {archive_size_mb} MB")

        # Step 3: Upload to Google Drive
        logger.info(f"Backup: Uploading {filename} to Google Drive folder {settings['folder_id']}")
        drive = _get_drive_service()

        from googleapiclient.http import MediaFileUpload

        file_metadata = {
            "name": filename,
            "parents": [settings["folder_id"]],
            "description": f"GolfGen DuckDB backup — {now.strftime('%Y-%m-%d %H:%M CT')} — {archive_size_mb} MB",
        }
        media = MediaFileUpload(str(archive_path), mimetype="application/gzip", resumable=True)
        uploaded = drive.files().create(
            body=file_metadata, media_body=media, fields="id,name,size,createdTime"
        ).execute()

        drive_file_id = uploaded.get("id", "")
        logger.info(f"Backup: Uploaded successfully — Drive file ID: {drive_file_id}")

        # Step 4: Enforce retention
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
            "error": "Google Drive backup not configured (missing credentials or folder ID)",
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
