"""
MFA Core Logic — TOTP generation, verification, backup codes, audit logging.
"""
import json
import secrets
import hashlib
from datetime import datetime
from typing import Optional

from .database import get_db_rw
import pyotp

from .config import DB_PATH


# ─── TOTP ──────────────────────────────────────────────────────────────────

def generate_mfa_secret() -> str:
    """Generate a new TOTP secret for a user."""
    return pyotp.random_base32()


def get_totp_uri(secret: str, user_name: str) -> str:
    """Build an otpauth:// URI for QR code generation."""
    return pyotp.totp.TOTP(secret).provisioning_uri(
        name=user_name,
        issuer_name="GolfGen Dashboard",
    )


def verify_totp(secret: str, code: str) -> bool:
    """Verify a TOTP code with a ±1 window for clock skew."""
    totp = pyotp.TOTP(secret)
    return totp.verify(code, valid_window=1)


# ─── Backup Codes ──────────────────────────────────────────────────────────

def generate_backup_codes(count: int = 8) -> list[str]:
    """Generate a set of single-use backup codes."""
    return [secrets.token_hex(4).upper() for _ in range(count)]


def hash_backup_code(code: str) -> str:
    """Hash a backup code for storage."""
    return hashlib.sha256(code.strip().upper().encode()).hexdigest()


def verify_backup_code(code: str, hashed_codes: list[str]) -> tuple[bool, list[str]]:
    """
    Check a backup code against stored hashes.
    Returns (matched, remaining_hashes) — the used code is removed.
    """
    h = hash_backup_code(code)
    if h in hashed_codes:
        remaining = [c for c in hashed_codes if c != h]
        return True, remaining
    return False, hashed_codes


# ─── DB helpers ────────────────────────────────────────────────────────────

def get_mfa_settings(user_name: str) -> Optional[dict]:
    """Fetch MFA settings for a user, or None if not enrolled."""
    con = get_db_rw()
    try:
        row = con.execute(
            "SELECT * FROM mfa_user_settings WHERE user_name = ?",
            [user_name],
        ).fetchone()
        if not row:
            return None
        cols = [d[0] for d in con.description]
        return dict(zip(cols, row))
    finally:
        con.close()


def save_mfa_enrollment(user_name: str, secret: str, backup_codes: list[str]):
    """Persist a new MFA enrollment."""
    hashed = json.dumps([hash_backup_code(c) for c in backup_codes])
    now = datetime.utcnow().isoformat()
    con = get_db_rw()
    try:
        existing = con.execute(
            "SELECT user_name FROM mfa_user_settings WHERE user_name = ?",
            [user_name],
        ).fetchone()
        if existing:
            con.execute("""
                UPDATE mfa_user_settings
                SET mfa_secret = ?, mfa_enabled = TRUE,
                    mfa_enrolled_at = ?, mfa_backup_codes = ?,
                    updated_at = ?
                WHERE user_name = ?
            """, [secret, now, hashed, now, user_name])
        else:
            con.execute("""
                INSERT INTO mfa_user_settings
                    (user_name, mfa_secret, mfa_enabled, mfa_enrolled_at,
                     mfa_backup_codes, created_at, updated_at)
                VALUES (?, ?, TRUE, ?, ?, ?, ?)
            """, [user_name, secret, now, hashed, now, now])
    finally:
        con.close()


def disable_mfa(user_name: str):
    """Admin reset — disable MFA for a user."""
    now = datetime.utcnow().isoformat()
    con = get_db_rw()
    try:
        con.execute("""
            UPDATE mfa_user_settings
            SET mfa_enabled = FALSE, mfa_secret = NULL,
                mfa_backup_codes = NULL, updated_at = ?
            WHERE user_name = ?
        """, [now, user_name])
    finally:
        con.close()


def update_backup_codes(user_name: str, hashed_codes: list[str]):
    """Update remaining backup codes after one is used."""
    now = datetime.utcnow().isoformat()
    con = get_db_rw()
    try:
        con.execute("""
            UPDATE mfa_user_settings
            SET mfa_backup_codes = ?, updated_at = ?
            WHERE user_name = ?
        """, [json.dumps(hashed_codes), now, user_name])
    finally:
        con.close()


# ─── Protected Routes ─────────────────────────────────────────────────────

def get_mfa_protected_routes() -> dict[str, bool]:
    """Return {tab_key: mfa_required} for all routes."""
    con = get_db_rw()
    try:
        rows = con.execute(
            "SELECT tab_key, mfa_required FROM mfa_protected_routes"
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        con.close()


def set_route_mfa(tab_key: str, mfa_required: bool):
    """Toggle MFA requirement for a route."""
    now = datetime.utcnow().isoformat()
    con = get_db_rw()
    try:
        existing = con.execute(
            "SELECT tab_key FROM mfa_protected_routes WHERE tab_key = ?",
            [tab_key],
        ).fetchone()
        if existing:
            con.execute("""
                UPDATE mfa_protected_routes
                SET mfa_required = ?, updated_at = ?
                WHERE tab_key = ?
            """, [mfa_required, now, tab_key])
        else:
            con.execute("""
                INSERT INTO mfa_protected_routes (tab_key, mfa_required, created_at, updated_at)
                VALUES (?, ?, ?, ?)
            """, [tab_key, mfa_required, now, now])
    finally:
        con.close()


# ─── Session MFA flag ─────────────────────────────────────────────────────

def mark_session_mfa_verified(session_token: str):
    """Set the mfa_verified_at timestamp on the session row."""
    now = datetime.utcnow().isoformat()
    con = get_db_rw()
    try:
        con.execute("""
            UPDATE sessions SET mfa_verified_at = ? WHERE token = ?
        """, [now, session_token])
    finally:
        con.close()


def is_session_mfa_verified(session_token: str) -> bool:
    """Check whether this session has passed MFA."""
    con = get_db_rw()
    try:
        row = con.execute(
            "SELECT mfa_verified_at FROM sessions WHERE token = ?",
            [session_token],
        ).fetchone()
        return row is not None and row[0] is not None
    finally:
        con.close()


# ─── Audit Logging ─────────────────────────────────────────────────────────

def log_mfa_event(
    user_name: str,
    event_type: str,
    detail: str = "",
    ip_address: str = "",
    user_agent: str = "",
):
    """Write an MFA audit log entry."""
    con = get_db_rw()
    try:
        con.execute("""
            INSERT INTO mfa_audit_log
                (id, user_name, event_type, detail, ip_address, user_agent, created_at)
            VALUES (nextval('mfa_audit_id_seq'), ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [user_name, event_type, detail, ip_address, user_agent])
    finally:
        con.close()


def get_audit_log(limit: int = 100, offset: int = 0) -> list[dict]:
    """Retrieve recent MFA audit log entries."""
    con = get_db_rw()
    try:
        rows = con.execute("""
            SELECT id, user_name, event_type, detail, ip_address,
                   user_agent, created_at
            FROM mfa_audit_log
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """, [limit, offset]).fetchall()
        cols = ["id", "user_name", "event_type", "detail", "ip_address",
                "user_agent", "created_at"]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        con.close()


def get_audit_log_count() -> int:
    """Total number of audit entries."""
    con = get_db_rw()
    try:
        row = con.execute("SELECT COUNT(*) FROM mfa_audit_log").fetchone()
        return row[0] if row else 0
    finally:
        con.close()
