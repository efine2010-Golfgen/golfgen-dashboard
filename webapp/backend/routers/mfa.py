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


# ═══════════════════════════════════════════════════════════════════════════
#  PASSKEY (WebAuthn) ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════

import uuid
import hashlib
from webauthn import (
    generate_registration_options,
    verify_registration_response,
    generate_authentication_options,
    verify_authentication_response,
    options_to_json,
)
from webauthn.helpers.structs import (
    PublicKeyCredentialDescriptor,
    AuthenticatorSelectionCriteria,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)
from webauthn.helpers import bytes_to_base64url, base64url_to_bytes
from core.database import get_db, get_db_rw

# RP configuration — must match the domain the browser is on
import os
_RP_ID = os.environ.get("WEBAUTHN_RP_ID", "golfgen-dashboard-production-ce30.up.railway.app")
_RP_NAME = "GolfGen Dashboard"
_RP_ORIGIN = os.environ.get("WEBAUTHN_ORIGIN", "https://golfgen-dashboard-production-ce30.up.railway.app")

# In-memory challenge store (short-lived, keyed by user_name)
_challenges: dict[str, bytes] = {}


@router.get("/passkeys")
async def passkey_list(request: Request):
    """List all passkeys for the current user."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    con = get_db()
    try:
        rows = con.execute(
            "SELECT id, device_name, created_at, last_used_at FROM passkeys WHERE user_name = ? ORDER BY created_at DESC",
            [session["user_name"]]
        ).fetchall()
    except Exception:
        rows = []
    con.close()

    return {
        "passkeys": [
            {"id": r[0], "device_name": r[1], "created_at": str(r[2]), "last_used_at": str(r[3]) if r[3] else None}
            for r in rows
        ]
    }


@router.get("/passkeys/register-options")
async def passkey_register_options(request: Request):
    """Generate WebAuthn registration options for the current user."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    user_name = session["user_name"]
    user_email = session.get("user_email", user_name)

    # Get existing credentials to exclude
    con = get_db()
    try:
        rows = con.execute(
            "SELECT credential_id FROM passkeys WHERE user_name = ?", [user_name]
        ).fetchall()
        exclude = [
            PublicKeyCredentialDescriptor(id=base64url_to_bytes(r[0]))
            for r in rows
        ]
    except Exception:
        exclude = []
    con.close()

    # Generate a stable user ID from the user_name
    user_id = hashlib.sha256(user_name.encode()).digest()[:32]

    options = generate_registration_options(
        rp_id=_RP_ID,
        rp_name=_RP_NAME,
        user_id=user_id,
        user_name=user_email,
        user_display_name=user_name,
        exclude_credentials=exclude,
        authenticator_selection=AuthenticatorSelectionCriteria(
            resident_key=ResidentKeyRequirement.PREFERRED,
            user_verification=UserVerificationRequirement.PREFERRED,
        ),
    )

    # Store challenge for verification
    _challenges[user_name] = options.challenge

    return JSONResponse(content=json.loads(options_to_json(options)))


@router.post("/passkeys/register-done")
async def passkey_register_done(request: Request):
    """Verify registration response and store the new passkey."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    user_name = session["user_name"]
    body = await request.json()
    credential_json = body.get("credential", {})
    device_name = body.get("device_name", "My Passkey")

    challenge = _challenges.pop(user_name, None)
    if not challenge:
        return JSONResponse({"ok": False, "detail": "No pending registration challenge"}, status_code=400)

    try:
        verification = verify_registration_response(
            credential=credential_json,
            expected_challenge=challenge,
            expected_rp_id=_RP_ID,
            expected_origin=_RP_ORIGIN,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "detail": str(e)}, status_code=400)

    # Store credential
    cred_id_b64 = bytes_to_base64url(verification.credential_id)
    pub_key_b64 = bytes_to_base64url(verification.credential_public_key)
    pk_id = str(uuid.uuid4())

    con = get_db_rw()
    try:
        con.execute(
            """INSERT INTO passkeys (id, user_name, device_name, credential_id, public_key, sign_count)
               VALUES (?, ?, ?, ?, ?, ?)""",
            [pk_id, user_name, device_name, cred_id_b64, pub_key_b64, verification.sign_count]
        )
    except Exception as e:
        con.close()
        return JSONResponse({"ok": False, "detail": f"Failed to save: {e}"}, status_code=500)
    con.close()

    ip, ua = _client_info(request)
    log_mfa_event(user_name, "passkey_registered", f"Device: {device_name}", ip, ua)

    return {"ok": True, "id": pk_id, "device_name": device_name}


@router.delete("/passkeys/{passkey_id}")
async def passkey_delete(request: Request, passkey_id: str):
    """Delete a passkey."""
    session, err = _user_from_request(request)
    if err:
        return JSONResponse({"error": err}, status_code=401)

    con = get_db_rw()
    con.execute(
        "DELETE FROM passkeys WHERE id = ? AND user_name = ?",
        [passkey_id, session["user_name"]]
    )
    con.close()

    ip, ua = _client_info(request)
    log_mfa_event(session["user_name"], "passkey_deleted", f"ID: {passkey_id}", ip, ua)
    return {"ok": True}


# ── Passkey Authentication (login flow) ────────────────────────────────

@router.post("/passkeys/login-options")
async def passkey_login_options(request: Request):
    """Generate authentication options for passkey login (no session required)."""
    body = await request.json()
    email_hint = body.get("email", "")

    # Find credentials for this user (if email provided)
    allow_credentials = []
    if email_hint:
        # Look up user_name from email — check all emails in the "emails" list
        from core.config import USERS, EMAIL_TO_USER
        user_key = EMAIL_TO_USER.get(email_hint.lower().strip())
        if user_key:
            # Passkeys are stored with display name (e.g. "Eric"), look up both
            display_name = USERS[user_key]["name"]
            con = get_db()
            try:
                rows = con.execute(
                    "SELECT credential_id FROM passkeys WHERE user_name = ? OR user_name = ?",
                    [user_key, display_name]
                ).fetchall()
                allow_credentials = [
                    PublicKeyCredentialDescriptor(id=base64url_to_bytes(r[0]))
                    for r in rows
                ]
            except Exception:
                pass
            con.close()

    options = generate_authentication_options(
        rp_id=_RP_ID,
        allow_credentials=allow_credentials if allow_credentials else None,
        user_verification=UserVerificationRequirement.PREFERRED,
    )

    # Store challenge keyed by email or "anonymous"
    _challenges[f"login:{email_hint or 'anon'}"] = options.challenge

    return JSONResponse(content=json.loads(options_to_json(options)))


@router.post("/passkeys/login-verify")
async def passkey_login_verify(request: Request, response: Response):
    """Verify authentication response and create a session."""
    body = await request.json()
    credential_json = body.get("credential", {})
    email_hint = body.get("email", "")

    challenge = _challenges.pop(f"login:{email_hint or 'anon'}", None)
    if not challenge:
        return JSONResponse({"ok": False, "detail": "No pending login challenge"}, status_code=400)

    # Find the credential in our database
    cred_id = credential_json.get("id", "")
    con = get_db()
    try:
        row = con.execute(
            "SELECT user_name, credential_id, public_key, sign_count FROM passkeys WHERE credential_id = ?",
            [cred_id]
        ).fetchone()
    except Exception:
        row = None
    con.close()

    if not row:
        return JSONResponse({"ok": False, "detail": "Unknown credential"}, status_code=400)

    user_name, stored_cred_id, pub_key_b64, stored_sign_count = row

    try:
        verification = verify_authentication_response(
            credential=credential_json,
            expected_challenge=challenge,
            expected_rp_id=_RP_ID,
            expected_origin=_RP_ORIGIN,
            credential_public_key=base64url_to_bytes(pub_key_b64),
            credential_current_sign_count=stored_sign_count or 0,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "detail": str(e)}, status_code=400)

    # Update sign count and last_used_at
    con = get_db_rw()
    con.execute(
        "UPDATE passkeys SET sign_count = ?, last_used_at = CURRENT_TIMESTAMP WHERE credential_id = ?",
        [verification.new_sign_count, cred_id]
    )
    con.close()

    # Create a session for this user
    # Passkeys store user_name as display name (e.g. "Eric"), but USERS dict
    # is keyed by lowercase (e.g. "eric"). Try lowercase key first, then
    # match by display name (case-insensitive).
    from core.config import EMAIL_TO_USER
    user_data = USERS.get(user_name)  # Try direct (e.g. "eric")
    user_key = user_name
    if not user_data:
        user_data = USERS.get(user_name.lower())  # Try lowercase (e.g. "Eric" -> "eric")
        user_key = user_name.lower()
    if not user_data:
        # Try matching by display name (case-insensitive)
        for ukey, udata in USERS.items():
            if udata["name"].lower() == user_name.lower():
                user_data = udata
                user_key = ukey
                break
    if not user_data:
        return JSONResponse({"ok": False, "detail": "User not found"}, status_code=400)

    import secrets
    token = secrets.token_hex(32)
    user_email = user_data["emails"][0] if user_data.get("emails") else ""
    con2 = get_db_rw()
    con2.execute(
        "INSERT INTO sessions (token, user_email, user_name, role, login_method) VALUES (?, ?, ?, ?, ?)",
        [token, user_email, user_data["name"], user_data["role"], "passkey"]
    )
    con2.close()
    response = JSONResponse({"ok": True, "user": user_data["name"], "role": user_data["role"]})
    response.set_cookie(
        "golfgen_session", token,
        httponly=True, samesite="none", secure=True,
        max_age=18 * 3600,  # 18hr session
    )

    # Mark session as MFA-verified (passkey counts as strong auth)
    mark_session_mfa_verified(token)

    ip, ua = _client_info(request)
    log_mfa_event(user_name, "passkey_login", "", ip, ua)

    return response
