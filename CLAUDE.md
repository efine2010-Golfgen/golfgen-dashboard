# GolfGen Dashboard — Cowork Context File

Read this entire file before doing anything else in every session.

## Metric Registry & Data Contracts — READ BEFORE WRITING ANY QUERY OR KPI

Two files are the source of truth for all data definitions. Check them before
writing any new query, router, or frontend calculation:

- **`webapp/backend/core/metrics.py`** — Every KPI: definition, formula, data source,
  conflict rule (which source wins when two disagree), today_available flag,
  null handling rule, fee fallback rates, CANONICAL_TIMEZONE.
- **`webapp/backend/core/data_contracts.py`** — Every table column: Amazon API field name
  it maps to, valid values, gotchas (e.g. "event_type NOT transaction_type",
  "asin NOT sku", "Pending orders have no OrderTotal"), join keys, resolved bugs.

### Non-negotiable rules enforced by the registry:
- Every metric formula lives in `metrics.py` and NOWHERE ELSE. Routers read
  analytics tables only — they do NOT calculate. Frontend displays only.
- When two sources report the same metric, `SOURCE_PRIORITY` in `metrics.py`
  controls which wins. The S&T overwrite bug happened because this didn't exist.
- All date range filters use `CANONICAL_TIMEZONE = "America/Chicago"` from
  `metrics.py`. Never use `date.today()` — Railway runs UTC.
- Sessions/page_views are NEVER estimated for today. Show dash (—).
  See `metrics.py` → `sessions.today_available = False`.
- Fee fallback rate when Finances API shows $0: use `get_fee_fallback()` from
  `metrics.py`. Default: 27% of revenue (referral 15% + FBA 12%).

## Project Overview
GolfGen Commerce Dashboard — internal tool for GolfGen LLC /
Elite Global Brands, Bentonville AR.
Live URL: https://golfgen-dashboard-production.up.railway.app
GitHub: https://github.com/efine2010-Golfgen/golfgen-dashboard
Backup Repo: https://github.com/efine2010-Golfgen/golfgen-backups

## How Deployment Works
Auto-deploys to Railway when code is pushed to main branch on GitHub.
1. Make code changes
2. cd webapp/frontend && npm run build
3. cp -r webapp/frontend/dist webapp/backend/dist
4. git add relevant files && git commit -m "your message"
5. git push origin main
6. Railway auto-deploys within 2 minutes7. Verify: curl -s https://golfgen-dashboard-production.up.railway.app/api/health | python3 -m json.tool

NEVER manually restart Railway. NEVER use Railway CLI.

## Business Structure — TOP LEVEL HIERARCHY
Every table, query, and dashboard filter follows this hierarchy:

```
Division (Golf / Housewares)
     ↓
Customer / Channel
     ↓
Platform / Data Source
     ↓
SKU / ASIN
```

### Division Definitions
- golf — GolfGen LLC products (golf training, accessories, on-course)
- housewares — Elite Global Brands products (casserole carriers, tumblers, kitchen)

### Complete Channel Map (12 channels)
| Division    | Customer             | Platform / Source  | Sync Method        | Status       |
|-------------|----------------------|--------------------|--------------------|--------------|
| golf        | amazon               | sp_api             | Auto (scheduled)   | LIVE ✓       |
| golf        | walmart_marketplace  | walmart_api        | Auto               | Phase 5      |
| golf        | walmart_stores       | scintilla          | Report upload      | Phase 4      |
| golf        | shopify              | shopify_api        | Auto               | Phase 5      || golf        | first_tee            | excel_upload       | Manual upload      | Phase 4      |
| housewares  | amazon               | sp_api             | Auto (scheduled)   | LIVE ✓       |
| housewares  | walmart_marketplace  | walmart_api        | Auto               | Phase 5      |
| housewares  | walmart_stores       | scintilla          | Report upload      | Phase 4      |
| housewares  | belk                 | excel_upload       | Upload or email    | Phase 4      |
| housewares  | hobby_lobby          | excel_upload       | Upload or email    | Phase 4      |
| housewares  | albertsons           | excel_upload       | Upload or email    | Phase 4      |
| housewares  | family_dollar        | excel_upload       | Upload or email    | Phase 4      |

### Three Mandatory Columns on Every Transactional Table
- division    VARCHAR  -- 'golf' | 'housewares' | 'unknown'
- customer    VARCHAR  -- 'amazon' | 'walmart_marketplace' | 'walmart_stores' |
                          'shopify' | 'first_tee' | 'belk' | 'hobby_lobby' |
                          'albertsons' | 'family_dollar'
- platform    VARCHAR  -- 'sp_api' | 'walmart_api' | 'shopify_api' |
                          'scintilla' | 'excel_upload' | 'email'

### Division Mapping — How Syncs Tag Data
Every sync determines division by looking up ASIN in item_master.
1. Pull order from API
2. Look up ASIN in item_master → get division
3. Tag row with division, customer, platform
4. Insert into table
If ASIN not found → division = 'unknown', flag for review.

### Hierarchy Filter (shared helper)
All routers use `from core.hierarchy import hierarchy_filter` for consistent
division/customer/platform filtering. Never build filter SQL manually.
The helper returns (sql_fragment, params) where sql_fragment starts with " AND ...".
## File Structure
```
webapp/
  backend/
    main.py              ← FastAPI app init, router registration, Google SSO endpoints
    requirements.txt     ← all Python dependencies
    dist/               ← compiled React frontend (copy here after build)
    core/
      __init__.py
      database.py        ← DbConnection wrapper (DuckDB ↔ PostgreSQL), get_db()/get_db_rw()
      config.py          ← all env vars and constants
      scheduler.py       ← APScheduler jobs, startup sync, catchup
      auth.py            ← login, logout, sessions, permissions
      hierarchy.py       ← shared hierarchy_filter() helper for all routers
      mfa.py             ← MFA/TOTP logic
      mfa_database.py    ← MFA database tables
    routers/
      __init__.py
      sales.py           ← /api/sales/* and /api/orders/*
      inventory.py       ← /api/inventory/*, /api/warehouse/*
      advertising.py     ← /api/ads/*
      item_plan.py       ← /api/item-plan/*
      factory_po.py      ← /api/factory-po/*
      otw.py             ← /api/otw/*
      profitability.py   ← /api/profitability/*, /api/pnl
      item_master.py     ← /api/item-master/*
      permissions.py     ← /api/permissions/*
      mfa.py             ← /api/mfa/*
      supply_chain.py    ← /api/supply-chain/*
      system.py          ← /api/sync/*, /api/backup/*, /api/docs/*, /api/debug/*    services/
      __init__.py
      sync_engine.py     ← sync orchestration, _write_sync_log
      sp_api.py          ← all SP-API logic (LIVE) — mutex, retry, backoff, timeout
      ads_api.py         ← Amazon Ads API (LIVE) — mutex, retry
      analytics_rollup.py ← nightly rollup to analytics tables
      backup.py          ← Google Drive + GitHub backup functions
    scripts/
      export_for_postgres.py  ← DuckDB → CSV export
      postgres_schema.py      ← PostgreSQL CREATE TABLE statements
      migrate_phase2.py       ← Phase 2 migration script
  frontend/
    src/
      App.jsx            ← main app, routing, passes filters to all pages
      components/
        HierarchyFilter.jsx  ← Golf/Housewares/Customer filter in header
      lib/
        api.js           ← ALL fetch() calls, _hq() hierarchy query builder
        constants.js     ← shared constants
      pages/             ← one file per dashboard tab (22 pages)
```

## Database
- Engine: PostgreSQL (LIVE on Railway as of March 14, 2026)
- Previous engine: DuckDB (file still on Railway volume as fallback at /app/data/golfgen_amazon.duckdb)
- Dual-mode wrapper: core/database.py DbConnection class auto-detects DuckDB vs PostgreSQL
- When DATABASE_URL env var is set → uses PostgreSQL; otherwise → uses DuckDB
- DATABASE_URL is set on Railway via `${{Postgres.DATABASE_URL}}` reference
- Railway Postgres service: b6075866-2c10-484c-ba4f-3c50182c65d0
- Access via get_db() (read-only) and get_db_rw() (read-write) from core.database
- SQL translator: _translate_sql_for_pg() converts ? → %s, INSERT OR IGNORE → ON CONFLICT DO NOTHING, DOUBLE → DOUBLE PRECISION, etc.
- Auto-migration: auto_migrate_from_duckdb() in database.py runs on startup if Postgres is empty and DuckDB file exists — copies all 26 tables
- Health endpoint (/api/health) still shows DB_PATH in "path" field even in Postgres mode — this is cosmetic only, queries go to PostgreSQL
### All Tables (27 total)
Transactional (all have division + customer + platform):
  orders, daily_sales, financial_events, fba_inventory, advertising,
  ads_campaigns, ads_keywords, ads_search_terms, ads_negative_keywords

Staging (cleaned, typed, deduped):
  staging_orders, staging_financial_events

Analytics (pre-aggregated, fast queries):
  analytics_daily, analytics_sku, analytics_ads

Reference:
  item_master (SOURCE OF TRUTH for division mapping)

Planning/Operations:
  monthly_sales_history, item_plan_overrides, item_plan_curve_selection,
  item_plan_factory_orders, item_plan_factory_order_items, item_plan_settings

System:
  sessions, user_permissions, audit_log, sync_log, docs_update_log

### Critical Column Facts — NEVER get these wrong
Full definitions in `core/data_contracts.py`. Quick reference:
- financial_events uses `event_type` NOT `transaction_type`
- Refund filter: `event_type ILIKE '%refund%'`
- sync_log uses `records_processed` NOT `records_inserted`
- sync_log uses `execution_time_seconds` NOT `duration_seconds`
- orders table has `asin` NOT `sku`
- orders.order_total is NULL/empty for Pending orders — estimate via ASIN avg price
- daily_sales asin='ALL' = date aggregate; asin=<ASIN> = per-product (24hr lag)
- fba_fees column is NEGATIVE — always use ABS(fba_fees) in SUM
- commission column is NEGATIVE — always use ABS(commission) in SUM
- purchase_date stored as UTC ISO string — always compare using Central TZ boundaries

### Current Data Counts (as of March 14, 2026 — now in PostgreSQL)
- orders: 136 (recent window)
- daily_sales: 937+ (migrated from DuckDB + new sync data)
- financial_events: 1,353 (1,276 nonzero)
- fba_inventory: 70
- All 29 tables confirmed in PostgreSQL
## Sync Architecture

### Sync Mutex
Global `_sync_lock` (threading.Lock) in sp_api.py prevents overlapping syncs.
`_sync_running` tracks which sync holds the lock ("today_orders" or "full_sync").

### Today Sync (hourly at :30)
- Fetches orders from past 2 days via Orders API
- 50-order cap to prevent long-running syncs
- 3 retries with max 15s delay on throttle
- 120-second hard timeout via concurrent.futures
- If timeout → releases lock so full sync can proceed

### Full Sync (9am, 12pm, 3pm, 6pm Central)
- Orders + Financial Events + Inventory + Sales & Traffic Report
- Financial events parsed via `_money()` helper (extracts CurrencyAmount from dicts)
- This is the sync that populates financial_events, daily_sales, refunds

### In-Memory Log Buffer
Ring buffer (200 entries) with custom logging.Handler in sp_api.py.
Accessible via GET /api/debug/logs for remote debugging without Railway CLI.

## Debug Endpoints (no auth required)
- GET /api/debug/db-diagnostic — recent daily_sales, financial_events summary, sync_log
- GET /api/debug/test-financial-parse — fetches one financial event from API, shows parsed output
- GET /api/debug/logs — recent in-memory log messages from sync engine
- GET /api/health — row counts, DB size, uptime

## Scheduled Jobs (all in core/scheduler.py)
- SP-API full sync: 9am, 12pm, 3pm, 6pm Central
- Today quick sync: every hour at :30
- Ads sync: every 2 hours
- Pricing sync: every hour at :30
- Docs update: 8am + 8pm Central
- DuckDB backup to Google Drive: 2am Central
- Analytics rollup: 2:30am Central (after backup)
## Environment Variables (set in Railway)
SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, SP_API_REFRESH_TOKEN,
SP_API_MARKETPLACE_ID (ATVPDKIKX0DER), SP_API_AWS_ACCESS_KEY,
SP_API_AWS_SECRET_KEY, SP_API_ROLE_ARN,
ADS_API_CLIENT_ID, ADS_API_CLIENT_SECRET, ADS_API_REFRESH_TOKEN,
GOOGLE_SERVICE_ACCOUNT_JSON, BACKUP_DRIVE_FOLDER_ID,
GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET,
GITHUB_TOKEN, BACKUP_GITHUB_REPO (efine2010-Golfgen/golfgen-backups),
DB_PATH (/app/data/golfgen_amazon.duckdb),
DATABASE_URL (${{Postgres.DATABASE_URL}} — LIVE, references Railway Postgres service)

## Users
- Eric (eric@egbrands.com) — Admin, full access
- Ty (tysams@egbrands.com) — Staff
- Kim (kim@egbrands.com) — Staff
- Ryan (ryan@egbrands.com) — Staff
- McKay (riseecom21@gmail.com) — Staff

## Security (all LIVE)
- Session-based auth with login/logout
- Google SSO with whitelisted email list
- MFA/TOTP enrollment and verification
- 18hr session expiry + 2hr idle timeout
- User permissions per tab
- Audit log table + viewer

## Critical Rules — Follow in Every Session
1. Database access via get_db() / get_db_rw() from core.database ONLY — never call duckdb.connect() directly
2. DB_PATH defined ONCE in core/config.py only
3. All datetime.now() must use ZoneInfo("America/Chicago") — never utcnow()
4. Every CREATE TABLE must have IF NOT EXISTS5. Never use DROP TABLE or DELETE FROM on existing data tables
6. All CREATE SEQUENCE before CREATE TABLE in database.py
7. After any frontend change: npm run build → copy dist → commit → push
8. sync_log columns: records_processed, execution_time_seconds
9. financial_events: event_type NOT transaction_type — refund filter: event_type ILIKE '%refund%'
10. orders table: asin NOT sku
11. Division always from item_master lookup — never hardcoded
12. Every transactional table must have division + customer + platform
13. All routers use core.hierarchy.hierarchy_filter() — never build filter SQL manually
14. Auto-save debounced 500ms — never save on every keystroke
15. All formula math client-side in React — no server round-trips
16. SQL must be dialect-aware — use DbConnection wrapper which auto-translates for PostgreSQL
17. Never modify _sync_lock or _sync_running outside the established mutex pattern in sp_api.py

## Roadmap Status (as of March 13, 2026)

### Phase 1 — Stability + Security: COMPLETE ✓
All 10 items done: returns bug, YOY bug, SKU bug, ads sync, sync mutex,
transaction rollback, retry/backoff, Google SSO, session timeout, audit log.

### Phase 2 — Data Layer + Division Hierarchy + PostgreSQL Migration: COMPLETE ✓
Done: division/customer/platform columns on all tables, item_master seeded,
SP-API auto-tagging, hierarchy filter on all routers (shared helper),
HierarchyFilter.jsx in header, staging tables created, analytics tables created,
nightly analytics rollup at 2:30am, PostgreSQL dual-mode wrapper (DbConnection),
SQL translator for DuckDB↔PostgreSQL (including DOUBLE→DOUBLE PRECISION),
all 56+ duckdb.connect() calls eliminated, PostgreSQL provisioned on Railway,
DATABASE_URL set, auto-migration from DuckDB completed, all 29 tables live in Postgres.
### Phase 3 — Redundancy + Backup Hardening: PARTIAL
Done: Nightly Google Drive backup (2am), nightly GitHub backup.
Remaining: Hot standby, weekly backup verification, 6-hour snapshots.

### Phase 4 — File Upload + Store Channels: PARTIAL
Done: Excel upload for warehouse data.
Remaining: Scintilla, First Tee, Belk, Hobby Lobby, Albertsons, Family Dollar
ingestion. Google Drive watched folder. Email ingestion.

### Phase 5 — Multi-Platform APIs: PLANNED
Walmart Marketplace API, Shopify API, cross-platform views.

### Phase 6 — Intelligence + Forecasting: PLANNED
Custom date ranges, daily email summary, anomaly alerts, forecast tab, exec dashboard.

## Backup System
- Google Drive: nightly 2am Central, 30-day retention
  Manual trigger: POST /api/backup/trigger
  List backups: GET /api/backup/list-drive
- GitHub: nightly after Drive backup
  Repo: github.com/efine2010-Golfgen/golfgen-backups
  Manual trigger: POST /api/backup/github-trigger
- Restore: Dashboard → System tab → Restore from Drive
  Or: POST /api/backup/restore (empty body = most recent)

## Dev-Safety Guard (DO NOT BYPASS)
`DbConnection.execute()` intercepts any `DROP TABLE` or `TRUNCATE TABLE` against
the 20 protected data tables and raises `RuntimeError` immediately. This prevents
accidental data loss during development.
- Protected tables defined in `_PROTECTED_TABLES` in `core/database.py`
- Backup restore bypasses this intentionally via `con._conn.cursor()` (raw psycopg2)
- If you genuinely need to reset a table during schema work:
  1. Temporarily add it to SKIP_TABLES in the restore path OR
  2. Remove it from `_PROTECTED_TABLES`, make your change, then add it back
  3. NEVER remove tables from `_PROTECTED_TABLES` in a commit that also modifies data

## Crash Recovery
With PostgreSQL, data persists independently of container deploys — no restore needed
for routine redeploys. DuckDB file still on volume as emergency fallback.

### PostgreSQL DR (data lost from Railway Postgres service):
- POST /api/backup/restore — auto-downloads latest Google Drive backup, compares
  row counts table-by-table, TRUNCATE+COPY any table where backup > current
- Sessions, audit_log, user_permissions are always skipped during restore
- Verify: GET /api/health → non-zero row counts

### DuckDB fallback (last resort):
Remove DATABASE_URL env var → Railway redeploys using DuckDB file.
Re-add DATABASE_URL → auto-migration from DuckDB back to Postgres runs on startup.
Option A: Dashboard → System tab → Backup → Restore from Drive
Option B: POST /api/backup/restore with body {}
After restore verify: GET /api/health → non-zero row counts