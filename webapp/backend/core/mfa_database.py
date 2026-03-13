"""
MFA Database Tables for DuckDB
Called from init_all_tables() in database.py
"""
import duckdb
from .config import DB_PATH


def _init_mfa_tables():
    """Initialize MFA-related tables in DuckDB."""
    con = duckdb.connect(str(DB_PATH))
    try:
        # MFA user settings — keyed by user_name since users are in config.py, not DB
        con.execute("""
            CREATE TABLE IF NOT EXISTS mfa_user_settings (
                user_name VARCHAR PRIMARY KEY,
                mfa_secret VARCHAR,
                mfa_enabled BOOLEAN DEFAULT FALSE,
                mfa_enrolled_at TIMESTAMP,
                mfa_verified_at TIMESTAMP,
                mfa_backup_codes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # MFA-protected routes — admin controls which pages require MFA
        con.execute("""
            CREATE SEQUENCE IF NOT EXISTS mfa_route_id_seq START 1
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS mfa_protected_routes (
                id INTEGER DEFAULT nextval('mfa_route_id_seq'),
                tab_key VARCHAR NOT NULL UNIQUE,
                mfa_required BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # MFA audit log
        con.execute("""
            CREATE SEQUENCE IF NOT EXISTS mfa_audit_id_seq START 1
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS mfa_audit_log (
                id INTEGER DEFAULT nextval('mfa_audit_id_seq'),
                user_name VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                detail VARCHAR,
                ip_address VARCHAR,
                user_agent VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Add mfa_verified_at column to sessions table if not exists
        try:
            con.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS mfa_verified_at TIMESTAMP")
        except Exception:
            pass  # Column may already exist
    finally:
        con.close()
