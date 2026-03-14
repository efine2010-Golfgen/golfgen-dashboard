"""
GolfGen Dashboard API — FastAPI backend with PostgreSQL (or DuckDB fallback).
Includes background data sync from Amazon SP-API (runs every 2 hours on Railway).
"""
import os
import logging
import secrets
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import bcrypt as _bcrypt
from fastapi import FastAPI, Request, HTTPException, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from core.config import (
    DB_PATH, DB_DIR, TIMEZONE, ALL_TABS, TAB_API_PREFIXES,
    USERS, EMAIL_TO_USER, DASHBOARD_PASSWORD, get_frontend_dir,
    GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REDIRECT_URI,
    SESSION_SECRET, ALLOWED_SSO_EMAILS,
    SESSION_MAX_AGE_HOURS, SESSION_IDLE_TIMEOUT_HOURS,
)
from core.database import init_all_tables, auto_migrate_from_duckdb, get_db, get_db_rw
from core.config import USE_POSTGRES, DATABASE_URL

logger = logging.getLogger("golfgen")

# ── Startup Diagnostics ──────────────────────────────────
_env_keys = [
    "DATABASE_URL", "DASHBOARD_PASSWORD",
    "SP_API_REFRESH_TOKEN", "SP_API_CLIENT_ID",
    "GOOGLE_OAUTH_REFRESH_TOKEN", "GOOGLE_OAUTH_CLIENT_ID",
    "BACKUP_DRIVE_FOLDER_ID", "GOOGLE_SERVICE_ACCOUNT_JSON",
]
for _k in _env_keys:
    _v = os.environ.get(_k, "")
    logger.info(f"[STARTUP ENV] {_k}: {'SET' if _v else 'MISSING'} (len={len(_v)})")
logger.info(f"[STARTUP] USE_POSTGRES={USE_POSTGRES}, DB_PATH={DB_PATH}")


# ── Session & Permission Helpers ─────────────────────────
# These stay in main.py because the middleware and auth endpoints use them,
# and they must be available before any router is imported.

def _find_user_by_email(email: str):
    """Look up user key by email (case-insensitive)."""
    return EMAIL_TO_USER.get(email.lower().strip())


def _get_session(token: str):
    """Look up session from DuckDB. Returns dict or None."""
    if not token:
        return None
    con = get_db_rw()
    rows = con.execute(
        "SELECT token, user_email, user_name, role FROM sessions WHERE token = ?",
        [token],
    ).fetchall()
    con.close()
    if rows:
        return {"token": rows[0][0], "user_email": rows[0][1],
                "user_name": rows[0][2], "role": rows[0][3]}
    return None


def _get_user_permissions(user_name: str):
    """Return set of enabled tab_keys for a user."""
    con = get_db_rw()
    rows = con.execute(
        "SELECT tab_key FROM user_permissions WHERE user_name = ? AND enabled = TRUE",
        [user_name],
    ).fetchall()
    con.close()
    return {r[0] for r in rows}


def _tab_key_for_path(path: str):
    """Return the tab_key for a given API path, or None if not tab-gated."""
    for tab_key, prefixes in TAB_API_PREFIXES.items():
        for prefix in prefixes:
            if path == prefix or path.startswith(prefix + "/") or path.startswith(prefix + "?"):
                return tab_key
    return None


def _require_auth(request: Request):
    """Raise 401 if user is not authenticated."""
    sess = _get_session(request.cookies.get("golfgen_session"))
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return sess


def _write_audit(user_email: str, user_name: str, action: str, detail: str = "",
                 ip_address: str = "", path: str = ""):
    """Write one row to the audit_log table."""
    try:
        con = get_db()
        con.execute(
            "INSERT INTO audit_log (user_email, user_name, action, detail, ip_address, path) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [user_email, user_name, action, detail, ip_address, path],
        )
        con.close()
    except Exception as e:
        logger.warning(f"Audit log write failed: {e}")


def _check_session_timeouts(token: str) -> bool:
    """Return True if session is still valid (not expired). Deletes expired sessions."""
    if not token:
        return False
    try:
        con = get_db()
        row = con.execute(
            "SELECT created_at, last_active_at FROM sessions WHERE token = ?", [token]
        ).fetchone()
        if not row:
            con.close()
            return False
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        created_at = row[0]
        last_active = row[1] or created_at

        # 18-hour absolute session lifetime
        if created_at and (now - created_at) > timedelta(hours=SESSION_MAX_AGE_HOURS):
            con.execute("DELETE FROM sessions WHERE token = ?", [token])
            con.close()
            return False

        # 2-hour idle timeout
        if last_active and (now - last_active) > timedelta(hours=SESSION_IDLE_TIMEOUT_HOURS):
            con.execute("DELETE FROM sessions WHERE token = ?", [token])
            con.close()
            return False

        # Touch last_active_at
        con.execute("UPDATE sessions SET last_active_at = CURRENT_TIMESTAMP WHERE token = ?", [token])
        con.close()
        return True
    except Exception as e:
        logger.warning(f"Session timeout check error: {e}")
        return True  # On error, allow through


# ── Lifespan ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB tables and start background sync on startup."""
    import asyncio

    # Initialize all tables (DuckDB or PostgreSQL)
    init_all_tables()
    logger.info("All tables initialized")

    # Auto-migrate DuckDB → PostgreSQL if Postgres is empty and DuckDB exists
    try:
        auto_migrate_from_duckdb()
    except Exception as e:
        logger.error(f"Auto-migration error (non-fatal): {e}")

    # Start background sync (imports scheduler module which imports services)
    task = None
    try:
        from core.scheduler import _sync_loop, scheduler
        task = asyncio.create_task(_sync_loop())
        logger.info("Background sync scheduler initialized")
    except Exception as e:
        logger.error(f"Failed to start background sync: {e}")

    yield

    # Shutdown
    try:
        from core.scheduler import scheduler
        if scheduler and scheduler.running:
            scheduler.shutdown()
    except Exception:
        pass
    if task:
        task.cancel()
    logger.info("Shutdown complete")


# ── FastAPI App ──────────────────────────────────────────
app = FastAPI(title="GolfGen Dashboard API", version="1.0.0", lifespan=lifespan)

# ── CORS Middleware ──────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000",
        "https://golfgen-dashboard-production.up.railway.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Tab Permission Middleware ────────────────────────────
@app.middleware("http")
async def tab_permission_middleware(request: Request, call_next):
    """Enforce tab-based permissions on all /api/* data endpoints."""
    path = request.url.path
    method = request.method

    # Always allow OPTIONS (CORS preflight) and non-API paths
    if method == "OPTIONS" or not path.startswith("/api/"):
        return await call_next(request)

    # Skip auth endpoints, system endpoints, upload endpoints, MFA endpoints
    if (path.startswith("/api/auth/") or
        path.startswith("/api/me") or
        path.startswith("/api/permissions") or
        path.startswith("/api/mfa/") or
        path.startswith("/api/upload/") or
        path.startswith("/api/debug/") or
        path == "/api/sales/debug-today" or
        path in ("/api/health", "/api/sync", "/api/backfill") or
        path.startswith("/api/refresh")):
        return await call_next(request)

    # Check session
    token = request.cookies.get("golfgen_session")
    try:
        sess = _get_session(token)
    except Exception as e:
        # DB error (table not exist, lock, etc.) — let request through
        logger.warning(f"Session lookup error (allowing request): {e}")
        return await call_next(request)

    if not sess:
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

    # Enforce session timeouts (18hr absolute, 2hr idle)
    if not _check_session_timeouts(token):
        response = JSONResponse(status_code=401, content={"detail": "Session expired"})
        response.delete_cookie("golfgen_session")
        return response

    # For tab-gated endpoints, check permission
    tab_key = _tab_key_for_path(path)
    if tab_key and sess["role"] != "admin":
        user_key = _find_user_by_email(sess["user_email"])
        if user_key:
            try:
                enabled = _get_user_permissions(user_key)
                if tab_key not in enabled:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "You do not have access to this page"},
                    )
            except Exception as e:
                logger.warning(f"Permission check error for {user_key}/{tab_key}: {e}")

    # ── MFA enforcement ──────────────────────────────────────
    # Skip MFA check for MFA endpoints themselves, auth, and static
    mfa_skip_prefixes = ("/api/auth/", "/api/mfa/", "/api/me")
    if not any(path.startswith(p) for p in mfa_skip_prefixes):
        try:
            from core.mfa import get_mfa_settings, is_session_mfa_verified, get_mfa_protected_routes

            tab_key = _tab_key_for_path(path)
            if tab_key:
                mfa_routes = get_mfa_protected_routes()
                if mfa_routes.get(tab_key, False):
                    # This route requires MFA
                    user_settings = get_mfa_settings(sess["user_name"])
                    # Only enforce MFA if user has actually enrolled
                    if user_settings and user_settings.get("mfa_enabled"):
                        token = request.cookies.get("golfgen_session", "")
                        if not is_session_mfa_verified(token):
                            # MFA enabled but session not verified → prompt for MFA
                            return JSONResponse(
                                {"detail": "MFA verification required", "mfa_required": True},
                                status_code=403,
                            )
                    # If user hasn't enrolled in MFA, let them through
        except Exception as e:
            logger.debug(f"MFA check skipped: {e}")  # MFA tables may not exist yet

    return await call_next(request)


# ── Auth Endpoints ────────────────────────────────────────
class MultiLoginRequest(BaseModel):
    email: str
    password: str


@app.post("/api/auth/login")
def login(req: MultiLoginRequest):
    """Validate email + password and create a DuckDB session."""
    user_key = _find_user_by_email(req.email)
    if not user_key:
        # Legacy single-password fallback
        if req.password != DASHBOARD_PASSWORD:
            _write_audit(req.email, "", "login_failed", "Invalid legacy password")
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        # Create session for legacy login
        token = secrets.token_hex(32)
        con = get_db()
        con.execute(
            "INSERT INTO sessions (token, user_email, user_name, role, login_method) VALUES (?, ?, ?, ?, 'legacy')",
            [token, req.email.lower().strip(), req.email.split("@")[0], "admin"],
        )
        con.close()
        _write_audit(req.email, req.email.split("@")[0], "login", "Legacy password login")
        response = JSONResponse({"ok": True, "name": req.email.split("@")[0], "role": "admin"})
        response.set_cookie(
            key="golfgen_session", value=token,
            httponly=True, samesite="none", secure=True,
            max_age=60 * 60 * SESSION_MAX_AGE_HOURS,
        )
        return response

    user = USERS[user_key]
    if not _bcrypt.checkpw(req.password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        _write_audit(req.email, "", "login_failed", "Invalid password")
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    token = secrets.token_hex(32)
    con = get_db()
    con.execute(
        "INSERT INTO sessions (token, user_email, user_name, role, login_method) VALUES (?, ?, ?, ?, 'password')",
        [token, req.email.lower().strip(), user["name"], user["role"]],
    )
    con.close()
    _write_audit(req.email, user["name"], "login", "Password login")
    response = JSONResponse({"ok": True, "name": user["name"], "role": user["role"]})
    response.set_cookie(
        key="golfgen_session", value=token,
        httponly=True, samesite="none", secure=True,
        max_age=60 * 60 * SESSION_MAX_AGE_HOURS,
    )
    return response


@app.get("/api/auth/check")
def auth_check(golfgen_session: Optional[str] = Cookie(None)):
    """Check if the current session is valid."""
    sess = _get_session(golfgen_session)
    if sess:
        return {"authenticated": True, "name": sess["user_name"], "role": sess["role"]}
    raise HTTPException(status_code=401, detail="Not authenticated")


@app.post("/api/auth/logout")
def logout(request: Request, golfgen_session: Optional[str] = Cookie(None)):
    """Clear the session from DuckDB."""
    if golfgen_session:
        sess = _get_session(golfgen_session)
        con = get_db()
        con.execute("DELETE FROM sessions WHERE token = ?", [golfgen_session])
        con.close()
        if sess:
            _write_audit(sess["user_email"], sess["user_name"], "logout", "User logout")
    response = JSONResponse({"ok": True})
    response.delete_cookie("golfgen_session")
    return response


# ── Google SSO Endpoints ──────────────────────────────────

@app.get("/api/auth/google/login")
def google_login():
    """Redirect user to Google's OAuth consent screen."""
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(status_code=501, detail="Google SSO not configured")
    import urllib.parse
    # Build Google authorization URL directly
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "select_account",
        "state": secrets.token_hex(16),
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url)


@app.get("/api/auth/google/callback")
async def google_callback(code: str = "", error: str = ""):
    """Handle the OAuth callback from Google."""
    from fastapi.responses import RedirectResponse
    if error or not code:
        return RedirectResponse("/?auth_error=google_denied")
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(status_code=501, detail="Google SSO not configured")
    import httpx
    # Exchange code for tokens
    async with httpx.AsyncClient() as client:
        token_resp = await client.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        })
        if token_resp.status_code != 200:
            logger.error(f"Google token exchange failed: {token_resp.text}")
            return RedirectResponse("/?auth_error=token_exchange_failed")
        tokens = token_resp.json()
        # Get user info
        userinfo_resp = await client.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
        )
        if userinfo_resp.status_code != 200:
            return RedirectResponse("/?auth_error=userinfo_failed")
        userinfo = userinfo_resp.json()

    email = userinfo.get("email", "").lower().strip()
    name = userinfo.get("name", email.split("@")[0])

    # Check whitelist
    if email not in ALLOWED_SSO_EMAILS:
        logger.warning(f"Google SSO rejected: {email} not in whitelist")
        _write_audit(email, name, "sso_rejected", f"Email not in whitelist: {email}")
        return RedirectResponse("/?auth_error=not_authorized")

    # Find matching user in USERS config (for role/permissions)
    user_key = EMAIL_TO_USER.get(email)
    role = "admin" if user_key and USERS[user_key]["role"] == "admin" else "staff"
    user_name = USERS[user_key]["name"] if user_key else name

    # Create session
    session_token = secrets.token_hex(32)
    con = get_db()
    con.execute(
        "INSERT INTO sessions (token, user_email, user_name, role, login_method) VALUES (?, ?, ?, ?, 'google_sso')",
        [session_token, email, user_name, role],
    )
    con.close()

    _write_audit(email, user_name, "login_sso", f"Google SSO login")

    response = RedirectResponse("/")
    response.set_cookie(
        key="golfgen_session", value=session_token,
        httponly=True, samesite="none", secure=True,
        max_age=60 * 60 * SESSION_MAX_AGE_HOURS,
    )
    return response


@app.get("/api/me")
def get_me(request: Request):
    """Return current user info."""
    sess = _get_session(request.cookies.get("golfgen_session"))
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"name": sess["user_name"], "email": sess["user_email"], "role": sess["role"]}


@app.get("/api/permissions/me")
def get_my_permissions(request: Request):
    """Return list of tab keys the current user can access."""
    sess = _get_session(request.cookies.get("golfgen_session"))
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if sess["role"] == "admin":
        return {"tabs": list(ALL_TABS.keys()), "role": "admin", "allTabs": ALL_TABS}
    user_key = _find_user_by_email(sess["user_email"])
    if not user_key:
        return {"tabs": list(ALL_TABS.keys()), "role": sess["role"], "allTabs": ALL_TABS}
    enabled = _get_user_permissions(user_key)
    return {"tabs": [t for t in ALL_TABS if t in enabled], "role": sess["role"], "allTabs": ALL_TABS}


@app.get("/api/permissions")
def get_all_permissions(request: Request):
    """Admin only: return full permissions grid."""
    sess = _get_session(request.cookies.get("golfgen_session"))
    if not sess or sess["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    con = get_db_rw()
    rows = con.execute(
        "SELECT user_name, tab_key, enabled FROM user_permissions ORDER BY user_name, tab_key"
    ).fetchall()
    con.close()
    grid = {}
    for user_name, tab_key, enabled in rows:
        if user_name not in grid:
            grid[user_name] = {}
        grid[user_name][tab_key] = enabled
    users_list = []
    for ukey, udata in USERS.items():
        if udata["role"] == "staff":
            users_list.append({
                "key": ukey, "name": udata["name"],
                "emails": udata["emails"],
                "permissions": grid.get(ukey, {}),
            })
    return {"users": users_list, "allTabs": ALL_TABS}


class PermissionUpdate(BaseModel):
    user: str
    tab: str
    enabled: bool


@app.post("/api/permissions")
def update_permission(req: PermissionUpdate, request: Request):
    """Admin only: update one toggle."""
    sess = _get_session(request.cookies.get("golfgen_session"))
    if not sess or sess["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    if req.user not in USERS or req.tab not in ALL_TABS:
        raise HTTPException(status_code=400, detail="Invalid user or tab")
    con = get_db()
    con.execute("DELETE FROM user_permissions WHERE user_name = ? AND tab_key = ?",
                [req.user, req.tab])
    con.execute("INSERT INTO user_permissions (user_name, tab_key, enabled) VALUES (?, ?, ?)",
                [req.user, req.tab, req.enabled])
    con.close()
    return {"ok": True}


# ── Audit Log Endpoint ────────────────────────────────────

@app.get("/api/audit-log")
def get_audit_log(request: Request, limit: int = 200, offset: int = 0):
    """Admin only: return recent audit log entries."""
    sess = _get_session(request.cookies.get("golfgen_session"))
    if not sess or sess["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    con = get_db()
    try:
        rows = con.execute(
            "SELECT ts, user_email, user_name, action, detail, ip_address, path "
            "FROM audit_log ORDER BY ts DESC LIMIT ? OFFSET ?",
            [limit, offset],
        ).fetchall()
        total = con.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    finally:
        con.close()
    return {
        "total": total,
        "entries": [
            {
                "ts": str(r[0]) if r[0] else "",
                "user_email": r[1] or "",
                "user_name": r[2] or "",
                "action": r[3] or "",
                "detail": r[4] or "",
                "ip_address": r[5] or "",
                "path": r[6] or "",
            }
            for r in rows
        ],
    }


# ── Include Routers ──────────────────────────────────────
from routers.sales import router as sales_router
from routers.profitability import router as profitability_router
from routers.advertising import router as advertising_router
from routers.inventory import router as inventory_router
from routers.item_master import router as item_master_router
from routers.factory_po import router as factory_po_router
from routers.otw import router as otw_router
from routers.item_plan import router as item_plan_router
from routers.system import router as system_router
from routers.mfa import router as mfa_router
from routers.supply_chain import router as supply_chain_router

app.include_router(sales_router)
app.include_router(profitability_router)
app.include_router(advertising_router)
app.include_router(inventory_router)
app.include_router(item_master_router)
app.include_router(factory_po_router)
app.include_router(otw_router)
app.include_router(item_plan_router)
app.include_router(system_router)
app.include_router(mfa_router)
app.include_router(supply_chain_router)


# ── Static Frontend (React SPA) ──────────────────────────
FRONTEND_DIR = get_frontend_dir()

if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR / "assets")),
              name="static-assets")

    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        """Catch-all for SPA: serve index.html for any non-API route."""
        file_path = FRONTEND_DIR / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(FRONTEND_DIR / "index.html"))


# ── Run ──────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
