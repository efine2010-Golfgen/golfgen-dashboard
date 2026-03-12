"""
GolfGen Dashboard API — FastAPI backend serving Amazon SP-API data from DuckDB.
Includes background data sync from Amazon SP-API (runs every 2 hours on Railway).
"""
import os
import logging
import secrets
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

import duckdb
import bcrypt as _bcrypt
from fastapi import FastAPI, Request, HTTPException, Cookie
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from core.config import (
    DB_PATH, DB_DIR, TIMEZONE, ALL_TABS, TAB_API_PREFIXES,
    USERS, EMAIL_TO_USER, DASHBOARD_PASSWORD, get_frontend_dir,
)
from core.database import init_all_tables

logger = logging.getLogger("golfgen")


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
    con = duckdb.connect(str(DB_PATH), read_only=False)
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
    con = duckdb.connect(str(DB_PATH), read_only=False)
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


# ── Lifespan ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB tables and start background sync on startup."""
    import asyncio

    # Initialize all DuckDB tables
    init_all_tables()
    logger.info("All tables initialized")

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
    allow_origins=["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000"],
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

    # Skip auth endpoints, system endpoints, upload endpoints
    if (path.startswith("/api/auth/") or
        path.startswith("/api/me") or
        path.startswith("/api/permissions") or
        path.startswith("/api/upload/") or
        path.startswith("/api/debug/") or
        path in ("/api/health", "/api/sync", "/api/backfill") or
        path.startswith("/api/refresh") or
        path in ("/api/backup/run", "/api/backup/run-status")):
        return await call_next(request)

    # Check session
    token = request.cookies.get("golfgen_session")
    try:
        sess = _get_session(token)
    except Exception:
        # DuckDB error (table not exist, lock, etc.) — let request through
        return await call_next(request)

    if not sess:
        return JSONResponse(status_code=401, content={"detail": "Not authenticated"})

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
            except Exception:
                pass  # DuckDB error — allow through

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
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        # Create session for legacy login
        token = secrets.token_hex(32)
        con = duckdb.connect(str(DB_PATH))
        con.execute(
            "INSERT INTO sessions (token, user_email, user_name, role) VALUES (?, ?, ?, ?)",
            [token, req.email.lower().strip(), req.email.split("@")[0], "admin"],
        )
        con.close()
        response = JSONResponse({"ok": True, "name": req.email.split("@")[0], "role": "admin"})
        response.set_cookie(
            key="golfgen_session", value=token,
            httponly=True, samesite="none", secure=True,
            max_age=60 * 60 * 24 * 7,
        )
        return response

    user = USERS[user_key]
    if not _bcrypt.checkpw(req.password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    token = secrets.token_hex(32)
    con = duckdb.connect(str(DB_PATH))
    con.execute(
        "INSERT INTO sessions (token, user_email, user_name, role) VALUES (?, ?, ?, ?)",
        [token, req.email.lower().strip(), user["name"], user["role"]],
    )
    con.close()
    response = JSONResponse({"ok": True, "name": user["name"], "role": user["role"]})
    response.set_cookie(
        key="golfgen_session", value=token,
        httponly=True, samesite="none", secure=True,
        max_age=60 * 60 * 24 * 7,
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
def logout(golfgen_session: Optional[str] = Cookie(None)):
    """Clear the session from DuckDB."""
    if golfgen_session:
        con = duckdb.connect(str(DB_PATH))
        con.execute("DELETE FROM sessions WHERE token = ?", [golfgen_session])
        con.close()
    response = JSONResponse({"ok": True})
    response.delete_cookie("golfgen_session")
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
    con = duckdb.connect(str(DB_PATH), read_only=False)
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
    con = duckdb.connect(str(DB_PATH))
    con.execute("DELETE FROM user_permissions WHERE user_name = ? AND tab_key = ?",
                [req.user, req.tab])
    con.execute("INSERT INTO user_permissions (user_name, tab_key, enabled) VALUES (?, ?, ?)",
                [req.user, req.tab, req.enabled])
    con.close()
    return {"ok": True}


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

app.include_router(sales_router)
app.include_router(profitability_router)
app.include_router(advertising_router)
app.include_router(inventory_router)
app.include_router(item_master_router)
app.include_router(factory_po_router)
app.include_router(otw_router)
app.include_router(item_plan_router)
app.include_router(system_router)


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
