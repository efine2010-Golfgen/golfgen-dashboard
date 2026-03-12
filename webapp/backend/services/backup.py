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
from github import Github
import base64

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
        con = duckdb.connect(str(DB_PATH), read_only=True)
        tables_raw = con.execute("SHOW TABLES").fetchall()
        manifest = {
            "backup_time": now.isoformat(),
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
        con = duckdb.connect(str(DB_PATH), read_only=True)
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
