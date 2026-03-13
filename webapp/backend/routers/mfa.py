"""
MFA API Router — all MFA endpoints in one self-contained module.

Mount in main.py:
    from routers.mfa import router as mfa_router
    app.include_router(mfa_router)
"""
import io
import json
import base64
from datetime import datetime

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

import qrcode

from core.config import USERS, ALL_TABS
from core.auth import get_session
from core.mfa import (
    generate_mfa_secret, get_totp_uri, verify_totp,
    generate_backup_codes, verify_backup_code,
    get_mfa_settings, save_mfa_enrollment, disable_mfa,
    update_backup_codes,
    mark_session_mfa_verified, is_session_mfa_verified,
    get_mfa_protected_routes, set_route_mfa,
    log_mfa_event, get_audit_log, get_audit_log_count,
)

router = APIRouter(prefix="/api/mfa", tags=["mfa"])


# ─── helpers ────────────────────────────────────────────────────────────────

def _user_from_request(request: Request) -> tuple[dict | None, str | None]:
    """Extract session user. Returns (session_dict, error_message)."""
    token = request.cookies.get("golfgen_session", "")
    session = get_session(token)
    if not session:
        return None, "Not authenticated"
    return session, None


def _client_info(request: Request) -> tuple[str, str]:
    ip = request.client.host if request.client else ""
    ua = request.headers.get("user-agent", "")
    return ip, ua


# ─── Enrollment ─────────────────────────────────────────────────────────────

@router.get("/setup")
async def mfa_setup_status(request: Request):
    """Check if user already has MFA enabled."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    user_name = session["user_name"]
    settings = get_mfa_settings(user_name)
    return {
        "mfa_enabled": bool(settings and settings.get("mfa_enabled")),
        "enrolled_at": settings["mfa_enrolled_at"] if settings else None,
    }


@router.post("/setup/begin")
async def mfa_setup_begin(request: Request):
    """
    Start MFA enrollment: generate secret + QR code.
    Returns base64 PNG of QR and the secret for manual entry.
    Does NOT save yet — user must confirm with a valid code first.
    """
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    user_name = session["user_name"]
    secret = generate_mfa_secret()
    uri = get_totp_uri(secret, user_name)

    # Generate QR code as base64 PNG
    qr = qrcode.QRCode(version=1, box_size=6, border=2,
                        error_correction=qrcode.constants.ERROR_CORRECT_M)
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="#0E1F2D", back_color="#FFFFFF")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    qr_b64 = base64.b64encode(buf.getvalue()).decode()

    ip, ua = _client_info(request)
    log_mfa_event(user_name, "enrollment_started", "", ip, ua)

    return {
        "secret": secret,
        "qr_code": f"data:image/png;base64,{qr_b64}",
        "otpauth_uri": uri,
    }


@router.post("/setup/confirm")
async def mfa_setup_confirm(request: Request):
    """
    Confirm enrollment by verifying the user's first TOTP code.
    On success, saves the secret and generates backup codes.
    """
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    body = await request.json()
    secret = body.get("secret", "")
    code = body.get("code", "")
    user_name = session["user_name"]
    ip, ua = _client_info(request)

    if not secret or not code:
        return JSONResponse({"error": "Missing secret or code"}, status_code=400)

    if not verify_totp(secret, code):
        log_mfa_event(user_name, "enrollment_failed", "Invalid code", ip, ua)
        return JSONResponse({"error": "Invalid code. Check your authenticator and try again."}, status_code=400)

    # Generate backup codes
    backup_codes = generate_backup_codes()
    save_mfa_enrollment(user_name, secret, backup_codes)

    # Also mark current session as MFA-verified
    token = request.cookies.get("golfgen_session", "")
    if token:
        mark_session_mfa_verified(token)

    log_mfa_event(user_name, "enrollment_completed", "", ip, ua)

    return {
        "success": True,
        "backup_codes": backup_codes,
        "message": "MFA enabled successfully. Save your backup codes — they won't be shown again.",
    }


@router.post("/disable")
async def mfa_self_disable(request: Request):
    """User disables their own MFA (requires current TOTP code)."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    body = await request.json()
    code = body.get("code", "")
    user_name = session["user_name"]
    ip, ua = _client_info(request)

    settings = get_mfa_settings(user_name)
    if not settings or not settings.get("mfa_enabled"):
        return JSONResponse({"error": "MFA is not enabled"}, status_code=400)

    if not verify_totp(settings["mfa_secret"], code):
        log_mfa_event(user_name, "disable_failed", "Invalid code", ip, ua)
        return JSONResponse({"error": "Invalid code"}, status_code=400)

    disable_mfa(user_name)
    log_mfa_event(user_name, "mfa_disabled", "Self-disabled", ip, ua)
    return {"success": True}


# ─── Verification (challenge page) ─────────────────────────────────────────

@router.get("/verify/status")
async def mfa_verify_status(request: Request):
    """Check if current session needs MFA verification."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    user_name = session["user_name"]
    settings = get_mfa_settings(user_name)
    token = request.cookies.get("golfgen_session", "")

    mfa_enabled = bool(settings and settings.get("mfa_enabled"))
    session_verified = is_session_mfa_verified(token) if token else False

    return {
        "mfa_enabled": mfa_enabled,
        "session_verified": session_verified,
        "needs_verification": mfa_enabled and not session_verified,
    }


@router.post("/verify")
async def mfa_verify(request: Request):
    """Verify TOTP code or backup code to unlock the session."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    body = await request.json()
    code = body.get("code", "").strip()
    use_backup = body.get("use_backup", False)
    user_name = session["user_name"]
    ip, ua = _client_info(request)

    settings = get_mfa_settings(user_name)
    if not settings or not settings.get("mfa_enabled"):
        return JSONResponse({"error": "MFA not enabled"}, status_code=400)

    if use_backup:
        stored = json.loads(settings.get("mfa_backup_codes") or "[]")
        matched, remaining = verify_backup_code(code, stored)
        if not matched:
            log_mfa_event(user_name, "verify_failed", "Invalid backup code", ip, ua)
            return JSONResponse({"error": "Invalid backup code"}, status_code=400)
        update_backup_codes(user_name, remaining)
        log_mfa_event(user_name, "verify_backup_code",
                      f"{len(remaining)} codes remaining", ip, ua)
    else:
        if not verify_totp(settings["mfa_secret"], code):
            log_mfa_event(user_name, "verify_failed", "Invalid TOTP", ip, ua)
            return JSONResponse({"error": "Invalid code"}, status_code=400)
        log_mfa_event(user_name, "verify_success", "", ip, ua)

    # Mark session as MFA-verified
    token = request.cookies.get("golfgen_session", "")
    if token:
        mark_session_mfa_verified(token)

    return {"success": True, "session_verified": True}


# ─── Backup Codes Management ──────────────────────────────────────────────

@router.get("/backup-codes/count")
async def backup_codes_count(request: Request):
    """Return how many backup codes the user has left."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    settings = get_mfa_settings(session["user_name"])
    if not settings or not settings.get("mfa_enabled"):
        return {"count": 0}
    codes = json.loads(settings.get("mfa_backup_codes") or "[]")
    return {"count": len(codes)}


@router.post("/backup-codes/regenerate")
async def regenerate_backup_codes(request: Request):
    """Generate new backup codes (requires current TOTP code)."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    body = await request.json()
    code = body.get("code", "")
    user_name = session["user_name"]
    ip, ua = _client_info(request)

    settings = get_mfa_settings(user_name)
    if not settings or not settings.get("mfa_enabled"):
        return JSONResponse({"error": "MFA not enabled"}, status_code=400)

    if not verify_totp(settings["mfa_secret"], code):
        log_mfa_event(user_name, "regen_codes_failed", "Invalid code", ip, ua)
        return JSONResponse({"error": "Invalid code"}, status_code=400)

    from core.mfa import hash_backup_code
    new_codes = generate_backup_codes()
    hashed = [hash_backup_code(c) for c in new_codes]
    update_backup_codes(user_name, hashed)

    log_mfa_event(user_name, "backup_codes_regenerated", "", ip, ua)
    return {"backup_codes": new_codes}


# ─── Admin: Protected Routes ──────────────────────────────────────────────

@router.get("/protected-routes")
async def list_protected_routes(request: Request):
    """Admin: list all tabs and their MFA requirement status."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)
    if session.get("role") != "admin":
        return JSONResponse({"error": "Admin only"}, status_code=403)

    mfa_routes = get_mfa_protected_routes()
    result = []
    for tab_key, display_name in ALL_TABS.items():
        result.append({
            "tab_key": tab_key,
            "display_name": display_name,
            "mfa_required": mfa_routes.get(tab_key, False),
        })
    return {"routes": result}


@router.post("/protected-routes")
async def toggle_protected_route(request: Request):
    """Admin: toggle MFA requirement for a tab."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)
    if session.get("role") != "admin":
        return JSONResponse({"error": "Admin only"}, status_code=403)

    body = await request.json()
    tab_key = body.get("tab_key", "")
    mfa_required = body.get("mfa_required", False)
    ip, ua = _client_info(request)

    if tab_key not in ALL_TABS:
        return JSONResponse({"error": "Invalid tab_key"}, status_code=400)

    set_route_mfa(tab_key, mfa_required)
    log_mfa_event(
        session["user_name"], "route_protection_changed",
        f"{tab_key} → {'required' if mfa_required else 'not required'}",
        ip, ua,
    )
    return {"success": True, "tab_key": tab_key, "mfa_required": mfa_required}


# ─── Admin: User MFA Management ──────────────────────────────────────────

@router.get("/admin/users")
async def admin_list_mfa_users(request: Request):
    """Admin: list all users and their MFA enrollment status."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)
    if session.get("role") != "admin":
        return JSONResponse({"error": "Admin only"}, status_code=403)

    result = []
    for uname in USERS:
        settings = get_mfa_settings(uname)
        result.append({
            "user_name": uname,
            "mfa_enabled": bool(settings and settings.get("mfa_enabled")),
            "enrolled_at": settings["mfa_enrolled_at"] if settings else None,
        })
    return {"users": result}


@router.post("/admin/reset")
async def admin_reset_mfa(request: Request):
    """Admin: reset (disable) MFA for another user."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)
    if session.get("role") != "admin":
        return JSONResponse({"error": "Admin only"}, status_code=403)

    body = await request.json()
    target_user = body.get("user_name", "")
    ip, ua = _client_info(request)

    if target_user not in USERS:
        return JSONResponse({"error": "Unknown user"}, status_code=400)

    disable_mfa(target_user)
    log_mfa_event(
        session["user_name"], "admin_reset_mfa",
        f"Reset MFA for {target_user}", ip, ua,
    )
    return {"success": True, "user_name": target_user}


# ─── Admin: Audit Log ─────────────────────────────────────────────────────

@router.get("/audit-log")
async def admin_audit_log(request: Request):
    """Admin: retrieve MFA audit log."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)
    if session.get("role") != "admin":
        return JSONResponse({"error": "Admin only"}, status_code=403)

    limit = int(request.query_params.get("limit", "100"))
    offset = int(request.query_params.get("offset", "0"))
    entries = get_audit_log(limit, offset)
    total = get_audit_log_count()

    return {"entries": entries, "total": total, "limit": limit, "offset": offset}
