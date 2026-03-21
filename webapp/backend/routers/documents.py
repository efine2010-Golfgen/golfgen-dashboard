"""
GolfGen Documents API — upload, list, and download business documents.

Endpoints:
  POST   /api/documents/upload         — upload a file (multipart/form-data)
  GET    /api/documents/               — list documents (with optional tag filter)
  GET    /api/documents/{doc_id}/download — download a specific document
  DELETE /api/documents/{doc_id}       — delete a document (admin only)
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import JSONResponse, Response

from core.database import get_db, get_db_rw
from core.config import DB_DIR

logger = logging.getLogger("golfgen")
router = APIRouter()
CENTRAL = ZoneInfo("America/Chicago")

# Documents stored on disk in DB_DIR/documents/
DOCS_DIR = DB_DIR / "documents"
DOCS_DIR.mkdir(parents=True, exist_ok=True)

MAX_FILE_SIZE = 50 * 1024 * 1024   # 50 MB


# ── Auth helper ──────────────────────────────────────────────────────────────

def _check_auth(request: Request, allow_viewer: bool = False):
    token = request.cookies.get("golfgen_session")
    if not token:
        raise JSONResponse(status_code=401, content={"error": "Not authenticated."})
    try:
        con = get_db()
        row = con.execute(
            "SELECT user_email, role FROM sessions WHERE token = ?", [token]
        ).fetchone()
        con.close()
    except Exception:
        raise JSONResponse(status_code=401, content={"error": "Auth error."})
    if not row:
        raise JSONResponse(status_code=401, content={"error": "Session expired."})
    user_email, role = row[0], (row[1] or "staff")
    if not allow_viewer and role.lower() == "viewer":
        raise JSONResponse(status_code=403, content={"error": "Upload not available for Viewer accounts."})
    return user_email, role


def _ensure_table(con_rw):
    con_rw.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_documents (
            id           INTEGER,
            filename     TEXT NOT NULL,
            original_name TEXT NOT NULL,
            content_type TEXT,
            size_bytes   INTEGER,
            tag          TEXT DEFAULT 'general',
            description  TEXT,
            uploaded_by  TEXT,
            uploaded_at  TEXT,
            PRIMARY KEY (id)
        )
    """)


def _next_id(con_rw) -> int:
    try:
        row = con_rw.execute("SELECT COALESCE(MAX(id), 0) FROM dashboard_documents").fetchone()
        return int(row[0]) + 1
    except Exception:
        return 1


# ── Upload ────────────────────────────────────────────────────────────────────

@router.post("/api/documents/upload")
async def upload_document(
    request: Request,
    file: UploadFile = File(...),
    tag: str = Form("general"),
    description: str = Form(""),
):
    try:
        user_email, role = _check_auth(request)
    except JSONResponse as r:
        return r

    # Read file content
    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        return JSONResponse(status_code=413, content={"error": "File too large (max 50 MB)."})

    original_name = file.filename or "upload"
    content_type = file.content_type or "application/octet-stream"
    now = datetime.now(CENTRAL).isoformat()

    try:
        con_rw = get_db_rw()
        _ensure_table(con_rw)
        doc_id = _next_id(con_rw)
        # Build safe stored filename:  {id}_{sanitized_original}
        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in original_name)
        stored_filename = f"{doc_id}_{safe_name}"
        # Write to disk
        file_path = DOCS_DIR / stored_filename
        file_path.write_bytes(content)
        # Insert metadata
        con_rw.execute("""
            INSERT INTO dashboard_documents
              (id, filename, original_name, content_type, size_bytes, tag, description, uploaded_by, uploaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [doc_id, stored_filename, original_name, content_type,
              len(content), tag.strip() or "general", description.strip()[:500],
              user_email, now])
        con_rw.close()
    except Exception as e:
        logger.error(f"upload_document error: {e}")
        # Clean up file if DB write failed
        try:
            if 'file_path' in locals():
                file_path.unlink(missing_ok=True)
        except Exception:
            pass
        return JSONResponse(status_code=500, content={"error": str(e)})

    return {
        "ok": True,
        "id": doc_id,
        "originalName": original_name,
        "sizeBytes": len(content),
        "tag": tag,
        "uploadedAt": now,
    }


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("/api/documents/")
async def list_documents(request: Request, tag: str = "", limit: int = 100):
    try:
        user_email, role = _check_auth(request, allow_viewer=True)
    except JSONResponse as r:
        return r
    try:
        con = get_db()
        _ensure_table(con)
        if tag:
            rows = con.execute(
                "SELECT id, original_name, content_type, size_bytes, tag, description, uploaded_by, uploaded_at FROM dashboard_documents WHERE tag = ? ORDER BY uploaded_at DESC LIMIT ?",
                [tag, limit]
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT id, original_name, content_type, size_bytes, tag, description, uploaded_by, uploaded_at FROM dashboard_documents ORDER BY uploaded_at DESC LIMIT ?",
                [limit]
            ).fetchall()
        con.close()
        return {"documents": [
            {
                "id": r[0],
                "name": r[1],
                "contentType": r[2],
                "sizeBytes": r[3],
                "tag": r[4],
                "description": r[5],
                "uploadedBy": r[6],
                "uploadedAt": r[7],
            } for r in rows
        ]}
    except Exception as e:
        logger.error(f"list_documents error: {e}")
        return {"documents": []}


# ── Download ──────────────────────────────────────────────────────────────────

@router.get("/api/documents/{doc_id}/download")
async def download_document(doc_id: int, request: Request):
    try:
        user_email, role = _check_auth(request, allow_viewer=True)
    except JSONResponse as r:
        return r
    try:
        con = get_db()
        _ensure_table(con)
        row = con.execute(
            "SELECT filename, original_name, content_type FROM dashboard_documents WHERE id = ?",
            [doc_id]
        ).fetchone()
        con.close()
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    if not row:
        return JSONResponse(status_code=404, content={"error": "Document not found."})

    stored_filename, original_name, content_type = row
    file_path = DOCS_DIR / stored_filename

    if not file_path.exists():
        return JSONResponse(status_code=404, content={"error": "File not found on disk."})

    file_bytes = file_path.read_bytes()
    return Response(
        content=file_bytes,
        media_type=content_type or "application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{original_name}"',
            "Content-Length": str(len(file_bytes)),
        }
    )


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/api/documents/{doc_id}")
async def delete_document(doc_id: int, request: Request):
    try:
        user_email, role = _check_auth(request)
    except JSONResponse as r:
        return r

    if role.lower() not in ("admin", "owner", "staff"):
        return JSONResponse(status_code=403, content={"error": "Insufficient permissions."})

    try:
        con_rw = get_db_rw()
        _ensure_table(con_rw)
        row = con_rw.execute(
            "SELECT filename FROM dashboard_documents WHERE id = ?", [doc_id]
        ).fetchone()
        if not row:
            con_rw.close()
            return JSONResponse(status_code=404, content={"error": "Document not found."})
        stored_filename = row[0]
        con_rw.execute("DELETE FROM dashboard_documents WHERE id = ?", [doc_id])
        con_rw.close()
        # Remove file from disk
        (DOCS_DIR / stored_filename).unlink(missing_ok=True)
    except Exception as e:
        logger.error(f"delete_document error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

    return {"ok": True, "deleted_id": doc_id}
