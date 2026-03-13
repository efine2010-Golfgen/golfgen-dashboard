# GolfGen Dashboard — Cowork Context File

Read this entire file before doing anything else in every session.

## Project Overview
GolfGen Commerce Dashboard — internal tool for GolfGen LLC /
Elite Global Brands, Bentonville AR.
Live URL: https://golfgen-dashboard-production-ce30.up.railway.app
GitHub: https://github.com/efine2010-Golfgen/golfgen-dashboard
Backup Repo: https://github.com/efine2010-Golfgen/golfgen-backups

## How Deployment Works
Auto-deploys to Railway when code is pushed to main branch on GitHub.
1. Make code changes
2. cd webapp/frontend && npm run build
3. cp -r webapp/frontend/dist webapp/backend/dist
4. git add relevant files && git commit -m "your message"
5. git push origin main
6. Railway auto-deploys within 2 minutes
7. Verify: curl -s https://golfgen-dashboard-production-ce30.up.railway.app/api/health | python3 -m json.tool

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
| golf        | shopify              | shopify_api        | Auto               | Phase 5      |
| golf        | first_tee            | excel_upload       | Manual upload      | Phase 4      |
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
      database.py        ← DuckDB connection, DB_PATH, all CREATE TABLE
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
      system.py          ← /api/sync/*, /api/backup/*, /api/docs/*
    services/
      __init__.py
      sync_engine.py     ← sync orchestration, _write_sync_log
      sp_api.py          ← all SP-API logic (LIVE) — mutex, retry, backoff
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
- Engine: DuckDB (migrating to PostgreSQL — next priority)
- Live path on Railway: /app/data/golfgen_amazon.duckdb
- DB_PATH env var controls which path is used
- Single connection only — defined in core/database.py
- Never open a second duckdb.connect() anywhere

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
- financial_events uses `event_type` NOT `transaction_type`
- Refund filter: `event_type ILIKE '%refund%'`
- sync_log uses `records_processed` NOT `records_inserted`
- sync_log uses `execution_time_seconds` NOT `duration_seconds`
- orders table has `asin` NOT `sku`

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
DB_PATH (/app/data/golfgen_amazon.duckdb)

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
1. DuckDB connection defined ONCE in core/database.py — never open duckdb.connect() elsewhere
2. DB_PATH defined ONCE in core/config.py only
3. All datetime.now() must use ZoneInfo("America/Chicago") — never utcnow()
4. Every CREATE TABLE must have IF NOT EXISTS
5. Never use DROP TABLE or DELETE FROM on existing data tables
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

## Roadmap Status (as of March 13, 2026)

### Phase 1 — Stability + Security: COMPLETE ✓
All 10 items done: returns bug, YOY bug, SKU bug, ads sync, sync mutex,
transaction rollback, retry/backoff, Google SSO, session timeout, audit log.

### Phase 2 — Data Layer + Division Hierarchy: ~90% COMPLETE
Done: division/customer/platform columns on all tables, item_master seeded,
SP-API auto-tagging, hierarchy filter on all routers (shared helper),
HierarchyFilter.jsx in header, staging tables created, analytics tables created,
nightly analytics rollup at 2:30am.
Remaining: PostgreSQL migration (scripts exist, not yet executed).

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

## Crash Recovery
Option A: Dashboard → System tab → Backup → Restore from Drive
Option B: POST /api/backup/restore with body {}
Option C: Tell Cowork to call POST /api/backup/restore
After restore verify: GET /api/health → non-zero row counts
