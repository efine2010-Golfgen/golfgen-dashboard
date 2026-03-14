# GolfGen Dashboard — Technical Architecture Guide

## Overview
The GolfGen Commerce Dashboard backend was refactored from a monolithic 7,335-line `main.py` into a modular architecture with clear separation of concerns. The refactoring is **COMPLETE** — all 19 modules are live in production on Railway with PostgreSQL.

## Current Architecture (as of March 14, 2026)

```
webapp/backend/
  main.py                    ← Slim FastAPI app (~150 lines): imports, middleware, lifespan, static files
  requirements.txt           ← All Python dependencies
  dist/                      ← Compiled React frontend (copied from frontend build)
  core/
    __init__.py
    config.py                ← All env vars, paths, constants, user config
    database.py              ← DbConnection dual-mode wrapper (PostgreSQL primary, DuckDB fallback)
    scheduler.py             ← APScheduler jobs, startup sync, catchup logic
    auth.py                  ← Session management, login/logout, permissions, Google SSO
    hierarchy.py             ← Shared hierarchy_filter() helper for division/customer/platform
    mfa.py                   ← MFA/TOTP enrollment and verification logic
    mfa_database.py          ← MFA database table initialization
  routers/
    __init__.py
    sales.py                 ← /api/sales/*, /api/orders/*
    inventory.py             ← /api/inventory/*, /api/warehouse/*
    advertising.py           ← /api/ads/*
    item_plan.py             ← /api/item-plan/*
    factory_po.py            ← /api/factory-po/*
    otw.py                   ← /api/otw/*
    profitability.py         ← /api/profitability/*, /api/pnl
    item_master.py           ← /api/item-master/*
    permissions.py           ← /api/permissions/*
    mfa.py                   ← /api/mfa/*
    supply_chain.py          ← /api/supply-chain/*
    system.py                ← /api/sync/*, /api/backup/*, /api/docs/*, /api/debug/*
  services/
    __init__.py
    sync_engine.py           ← Sync orchestration, _write_sync_log
    sp_api.py                ← SP-API logic (LIVE) — mutex, retry, backoff, timeout
    ads_api.py               ← Amazon Ads API (LIVE) — mutex, retry
    analytics_rollup.py      ← Nightly rollup to analytics tables
    backup.py                ← Google Drive + GitHub backup functions
  scripts/
    export_for_postgres.py   ← DuckDB → CSV export utility
    postgres_schema.py       ← PostgreSQL CREATE TABLE statements
    migrate_phase2.py        ← Phase 2 migration script (division/customer/platform columns)
```

## Module Completion Status

### All 19 Modules: COMPLETE ✅

| # | Module | Status | Description |
|---|--------|--------|-------------|
| 1 | `core/config.py` | ✅ COMPLETE | All constants, paths, env vars, DATABASE_URL detection |
| 2 | `core/database.py` | ✅ COMPLETE | DbConnection wrapper, PostgreSQL + DuckDB dual-mode, SQL translator, auto-migration |
| 3 | `core/auth.py` | ✅ COMPLETE | Session management, Google SSO, permissions |
| 4 | `core/scheduler.py` | ✅ COMPLETE | APScheduler setup, job wrappers, startup sync, catchup |
| 5 | `core/hierarchy.py` | ✅ COMPLETE | Shared hierarchy_filter() for all routers |
| 6 | `services/sp_api.py` | ✅ COMPLETE | SP-API integration, sync mutex, retry/backoff, timeout |
| 7 | `services/ads_api.py` | ✅ COMPLETE | Amazon Ads API, pricing sync, coupon sync |
| 8 | `services/sync_engine.py` | ✅ COMPLETE | Sync orchestration, backfill, pricing/coupon coordination |
| 9 | `services/analytics_rollup.py` | ✅ COMPLETE | Nightly rollup to analytics_daily, analytics_sku, analytics_ads |
| 10 | `services/backup.py` | ✅ COMPLETE | Google Drive + GitHub backup functions |
| 11 | `routers/sales.py` | ✅ COMPLETE | Sales analytics, daily/monthly/YOY endpoints |
| 12 | `routers/profitability.py` | ✅ COMPLETE | P&L waterfall, item-level profitability |
| 13 | `routers/inventory.py` | ✅ COMPLETE | FBA inventory, warehouse, Excel upload |
| 14 | `routers/advertising.py` | ✅ COMPLETE | Ads analytics, campaigns, keywords, search terms |
| 15 | `routers/item_master.py` | ✅ COMPLETE | Item management, pricing status |
| 16 | `routers/item_plan.py` | ✅ COMPLETE | Item planning, sales curves, factory on-order |
| 17 | `routers/factory_po.py` | ✅ COMPLETE | Factory PO management |
| 18 | `routers/otw.py` | ✅ COMPLETE | Logistics / OTW tracking |
| 19 | `routers/system.py` | ✅ COMPLETE | Health, sync triggers, backup, debug endpoints |
| — | `routers/permissions.py` | ✅ COMPLETE | Auth endpoint wrappers (thin layer over core/auth.py) |
| — | `routers/mfa.py` | ✅ COMPLETE | MFA enrollment/verification endpoints |
| — | `routers/supply_chain.py` | ✅ COMPLETE | Supply chain data upload |
| — | `main.py` | ✅ COMPLETE | Slim app init, router registration, middleware, static files |

## Database Architecture

### PostgreSQL (PRIMARY — LIVE since March 14, 2026)
- Hosted on Railway as a managed Postgres service
- Connected via `DATABASE_URL` env var (set to `${{Postgres.DATABASE_URL}}` on Railway)
- All 29 tables live and receiving data

### DuckDB (FALLBACK)
- File still exists on Railway volume at `/app/data/golfgen_amazon.duckdb`
- Used automatically if `DATABASE_URL` is not set
- Serves as emergency fallback if Postgres is unavailable

### Dual-Mode Wrapper (`core/database.py`)
The `DbConnection` class auto-detects which engine to use:
- `USE_POSTGRES = bool(DATABASE_URL)` in config.py
- `get_db()` → read-only connection (PostgreSQL or DuckDB)
- `get_db_rw()` → read-write connection (PostgreSQL or DuckDB)
- `_raw_pg_conn()` → direct psycopg2 connection for PostgreSQL

### SQL Translator (`_translate_sql_for_pg()`)
Converts DuckDB SQL idioms to PostgreSQL equivalents:
- `?` → `%s` (parameterized placeholders)
- `DOUBLE` → `DOUBLE PRECISION` (type not valid in bare form in PostgreSQL)
- `INSERT OR IGNORE` → `INSERT INTO ... ON CONFLICT DO NOTHING`
- All SQL goes through this translator when `USE_POSTGRES=True`

### Auto-Migration (`auto_migrate_from_duckdb()`)
Runs on startup when PostgreSQL is empty and DuckDB file exists:
1. Checks if `daily_sales` table has 0 rows in PostgreSQL
2. If so, iterates all DuckDB tables
3. Reads each table into pandas DataFrame
4. Writes to PostgreSQL via `DataFrame.to_sql()`
5. Logs migration results

## Key Design Patterns

### 1. Database Connection Pattern
```python
from core.database import get_db, get_db_rw

# Read-only queries
with get_db() as con:
    rows = con.execute("SELECT ...", params).fetchall()

# Write operations
with get_db_rw() as con:
    con.execute("INSERT INTO ...", params)
```
Never call `duckdb.connect()` or `psycopg2.connect()` directly. Always use the wrapper.

### 2. Hierarchy Filter Pattern
```python
from core.hierarchy import hierarchy_filter

# In any router endpoint:
hf_sql, hf_params = hierarchy_filter(division, customer, platform)
sql = f"SELECT ... FROM table WHERE 1=1 {hf_sql}"
rows = con.execute(sql, hf_params).fetchall()
```
Never build division/customer/platform filter SQL manually.

### 3. Sync Mutex Pattern
```python
# In sp_api.py — global lock prevents overlapping syncs
_sync_lock = threading.Lock()
_sync_running = None  # "today_orders" or "full_sync"
```
Never modify `_sync_lock` or `_sync_running` outside the established pattern.

### 4. Timezone Pattern
```python
from zoneinfo import ZoneInfo
# Business logic — always Central time
now = datetime.now(ZoneInfo("America/Chicago"))
# Never use datetime.utcnow() for business logic
```

## Critical Rules (unchanged from original)

1. Database access via `get_db()` / `get_db_rw()` ONLY
2. `DB_PATH` defined ONCE in `core/config.py`
3. All `datetime.now()` must use `ZoneInfo("America/Chicago")`
4. Every `CREATE TABLE` must have `IF NOT EXISTS`
5. Never use `DROP TABLE` or `DELETE FROM` on existing data tables
6. All `CREATE SEQUENCE` before `CREATE TABLE` in database.py
7. After frontend changes: `npm run build` → copy dist → commit → push
8. `sync_log` columns: `records_processed`, `execution_time_seconds`
9. `financial_events`: `event_type` NOT `transaction_type`
10. `orders` table: `asin` NOT `sku`
11. Division always from `item_master` lookup — never hardcoded
12. Every transactional table must have `division` + `customer` + `platform`
13. All routers use `core.hierarchy.hierarchy_filter()`
14. Auto-save debounced 500ms
15. All formula math client-side in React
16. SQL must be dialect-aware — use DbConnection wrapper
17. Never modify sync mutex outside established pattern in sp_api.py

## Deployment Pipeline

```
Code change → npm run build (frontend) → cp dist to backend → git push origin main → Railway auto-deploys
```

Railway detects the push to `main` and rebuilds the container. PostgreSQL data persists independently of container deploys — no data loss on redeploy.

## History

- **Original**: Single 7,335-line `main.py` with all logic inline
- **Phase 1** (Complete): Stability + Security fixes (returns bug, YOY bug, sync mutex, Google SSO, etc.)
- **Phase 2** (Complete): Modular refactoring + division hierarchy + PostgreSQL migration
- **March 14, 2026**: PostgreSQL migration completed, all 29 tables live, auto-migration from DuckDB verified
