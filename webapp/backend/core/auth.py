"""
Authentication and permission management for GolfGen Dashboard.
Handles multi-user sessions, bcrypt password validation, and tab-based permissions.
"""

import logging
import secrets
from typing import Optional
from pydantic import BaseModel
import bcrypt as _bcrypt
from fastapi import HTTPException, Request, Cookie
from fastapi.responses import JSONResponse

from .database import get_db
from .config import USERS, EMAIL_TO_USER, ALL_TABS, TAB_API_PREFIXES, DB_PATH

logger = logging.getLogger("golfgen")


def find_user_by_email(email: str):
    """Look up user key by email (case-insensitive)."""
    return EMAIL_TO_USER.get(email.lower().strip())


def get_session(token: str):
    """Look up session from DuckDB. Returns dict or None."""
    if not token:
        return None
    con = get_db()
    try:
        rows = con.execute("SELECT token, user_email, user_name, role FROM sessions WHERE token = ?", [token]).fetchall()
        if rows:
            return {"token": rows[0][0], "user_email": rows[0][1], "user_name": rows[0][2], "role": rows[0][3]}
    finally:
        con.close()
    return None


def get_user_permissions(user_name: str):
    """Return set of enabled tab_keys for a user."""
    con = get_db()
    try:
        rows = con.execute("SELECT tab_key FROM user_permissions WHERE user_name = ? AND enabled = TRUE", [user_name]).fetchall()
        return {r[0] for r in rows}
    finally:
        con.close()


def require_auth(request: Request):
    """Validate session cookie or raise 401."""
    token = request.cookies.get("golfgen_session")
    sess = get_session(token)
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return sess


def require_tab_access(request: Request, tab_key: str):
    """Validate session + tab permission. Returns session dict."""
    sess = require_auth(request)
    if sess["role"] == "admin":
        return sess
    user_key = find_user_by_email(sess["user_email"])
    if user_key:
        enabled = get_user_permissions(user_key)
        if tab_key not in enabled:
            raise HTTPException(status_code=403, detail="You do not have access to this page")
    return sess


def tab_key_for_path(path: str):
    """Return the tab_key for a given API path, or None if not tab-gated."""
    for tab_key, prefixes in TAB_API_PREFIXES.items():
        for prefix in prefixes:
            if path == prefix or path.startswith(prefix + "/") or path.startswith(prefix + "?"):
                return tab_key
    return None


class MultiLoginRequest(BaseModel):
    email: str
    password: str


class PermissionUpdate(BaseModel):
    user: str
    tab: str
    enabled: bool


def login_handler(req: MultiLoginRequest):
    """Validate email + password and create a DuckDB session."""
    user_key = find_user_by_email(req.email)
    if not user_key:
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    user = USERS[user_key]
    if not _bcrypt.checkpw(req.password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    token = secrets.token_hex(32)
    con = get_db()  # This will be a read-only connection, so we need to use DB_PATH directly
    import duckdb
    con_rw = duckdb.connect(str(DB_PATH))
    con_rw.execute("INSERT INTO sessions (token, user_email, user_name, role) VALUES (?, ?, ?, ?)",
                   [token, req.email.lower().strip(), user["name"], user["role"]])
    con_rw.close()
    response = JSONResponse({"ok": True, "name": user["name"], "role": user["role"]})
    response.set_cookie(
        key="golfgen_session",
        value=token,
        httponly=True,
        samesite="none",
        secure=True,
        max_age=60 * 60 * 24 * 7,
    )
    return response


def logout_handler(golfgen_session: Optional[str]):
    """Clear the session from DuckDB."""
    if golfgen_session:
        import duckdb
        con = duckdb.connect(str(DB_PATH))
        con.execute("DELETE FROM sessions WHERE token = ?", [golfgen_session])
        con.close()
    response = JSONResponse({"ok": True})
    response.delete_cookie("golfgen_session")
    return response


def get_me_handler(request: Request):
    """Return current user info."""
    sess = get_session(request.cookies.get("golfgen_session"))
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return {"name": sess["user_name"], "email": sess["user_email"], "role": sess["role"]}


def get_my_permissions_handler(request: Request):
    """Return list of tab keys the current user can access."""
    sess = get_session(request.cookies.get("golfgen_session"))
    if not sess:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if sess["role"] == "admin":
        return {"tabs": list(ALL_TABS.keys()), "role": "admin", "allTabs": ALL_TABS}
    user_key = find_user_by_email(sess["user_email"])
    if not user_key:
        return {"tabs": list(ALL_TABS.keys()), "role": sess["role"], "allTabs": ALL_TABS}
    enabled = get_user_permissions(user_key)
    return {"tabs": [t for t in ALL_TABS if t in enabled], "role": sess["role"], "allTabs": ALL_TABS}


def get_all_permissions_handler(request: Request):
    """Admin only: return full permissions grid."""
    sess = get_session(request.cookies.get("golfgen_session"))
    if not sess or sess["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    con = get_db()
    try:
        rows = con.execute("SELECT user_name, tab_key, enabled FROM user_permissions ORDER BY user_name, tab_key").fetchall()
        grid = {}
        for user_name, tab_key, enabled in rows:
            if user_name not in grid:
                grid[user_name] = {}
            grid[user_name][tab_key] = enabled
        # Include user display names
        users_list = []
        for ukey, udata in USERS.items():
            if udata["role"] == "staff":
                users_list.append({
                    "key": ukey, "name": udata["name"],
                    "emails": udata["emails"],
                    "permissions": grid.get(ukey, {}),
                })
        return {"users": users_list, "allTabs": ALL_TABS}
    finally:
        con.close()


def update_permission_handler(req: PermissionUpdate, request: Request):
    """Admin only: update one toggle."""
    sess = get_session(request.cookies.get("golfgen_session"))
    if not sess or sess["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    if req.user not in USERS or req.tab not in ALL_TABS:
        raise HTTPException(status_code=400, detail="Invalid user or tab")
    import duckdb
    con = duckdb.connect(str(DB_PATH))
    # Upsert
    con.execute("DELETE FROM user_permissions WHERE user_name = ? AND tab_key = ?", [req.user, req.tab])
    con.execute("INSERT INTO user_permissions (user_name, tab_key, enabled) VALUES (?, ?, ?)",
                [req.user, req.tab, req.enabled])
    con.close()
    return {"ok": True}
