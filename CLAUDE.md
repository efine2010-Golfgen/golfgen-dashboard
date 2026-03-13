# GolfGen Dashboard — Cowork Context File

Read this entire file before doing anything else in every session.

## Project Overview
GolfGen Amazon Commerce Dashboard — internal tool for GolfGen LLC /
Elite Global Brands, Bentonville AR.
Live URL: https://golfgen-dashboard-production-ce30.up.railway.app
GitHub: https://github.com/efine2010-Golfgen/golfgen-dashboard
Backup Repo: https://github.com/efine2010-Golfgen/golfgen-backups

## How Deployment Works
This project auto-deploys to Railway when code is pushed to the
main branch on GitHub.

To deploy any change:
1. Make code changes
2. Run: cd webapp/frontend && npm run build
3. Copy build output: cp -r webapp/frontend/dist webapp/backend/dist
4. Commit all changes: git add -A && git commit -m "your message"
5. Push to GitHub: git push origin main
6. Railway auto-deploys within 2 minutes
7. Confirm live URL loads correctly

NEVER manually restart Railway. NEVER use Railway CLI.
Just push to GitHub and Railway handles the rest.

## Business Structure — TOP LEVEL HIERARCHY
This is the most important structural concept in the entire data model.
Every table, every query, every dashboard filter follows this hierarchy:

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

### Complete Channel Map

| Division    | Customer             | Platform / Source  | Sync Method        |
|-------------|----------------------|--------------------|--------------------|
| golf        | amazon               | sp_api             | Auto every 4hrs    |
| golf        | walmart_marketplace  | walmart_api        | Auto every 4hrs    |
| golf        | walmart_stores       | scintilla          | Report upload      |
| golf        | shopify              | shopify_api        | Auto every 4hrs    |
| housewares  | amazon               | sp_api             | Auto every 4hrs    |
| housewares  | walmart_marketplace  | walmart_api        | Auto every 4hrs    |
| housewares  | walmart_stores       | scintilla          | Report upload      |
| housewares  | belk                 | excel_upload       | Upload or email    |
| housewares  | albertsons           | excel_upload       | Upload or email    |
| housewares  | family_dollar        | excel_upload       | Upload or email    |
| both        | future_customer      | tbd                | Expandable         |

### Three Columns on Every Core Table
Every table that holds transactional or sales data MUST have these three columns:
- division    VARCHAR  -- 'golf' | 'housewares'
- customer    VARCHAR  -- 'amazon' | 'walmart_marketplace' | 'walmart_stores' |
                          'shopify' | 'belk' | 'albertsons' | 'family_dollar'
- platform    VARCHAR  -- 'sp_api' | 'walmart_api' | 'shopify_api' |
                          'scintilla' | 'excel_upload' | 'email'

### Division Mapping — How Syncs Tag Data
The SP-API sync (and all other auto syncs) determine division by looking up
the ASIN in item_master. The item_master table is the source of truth for
which division each SKU/ASIN belongs to.

Example logic in every sync function:
  1. Pull order from API
  2. Look up ASIN in item_master → get division
  3. Tag order row with correct division, customer, platform
  4. Insert into table

If ASIN not found in item_master → division = 'unknown', flag for review.

### Dashboard Filter Hierarchy
Top level: [ Golf ] [ Housewares ] [ All Divisions ]
Second level: [ Amazon ] [ Walmart Mkt ] [ Walmart Stores ] [ Shopify ]
              [ Belk ] [ Albertsons ] [ Family Dollar ] [ All Customers ]
All tabs, charts, KPIs filter automatically based on selection.

Adding a future customer = new value in customer column, zero code changes.

## File Structure
```
webapp/
  backend/
    main.py              ← FastAPI app init, router registration only
    requirements.txt     ← all Python dependencies
    dist/               ← compiled React frontend (copy here after build)
    core/
      __init__.py
      database.py        ← DuckDB connection, DB_PATH, all CREATE TABLE
      config.py          ← all env vars and constants
      scheduler.py       ← APScheduler jobs, startup sync, catchup
      auth.py            ← login, logout, sessions, permissions
    routers/
      __init__.py
      sales.py           ← /api/sales/* and /api/orders/*
      inventory.py       ← /api/inventory/*
      advertising.py     ← /api/advertising/* and /api/coupons/*
      item_plan.py       ← /api/item-plan/*
      factory_po.py      ← /api/factory-po/*
      otw.py             ← /api/otw/*
      profitability.py   ← /api/profitability/*
      item_master.py     ← /api/item-master/*
      permissions.py     ← /api/permissions/*
      system.py          ← /api/sync/*, /api/backup/*, /api/docs/*
    services/
      __init__.py
      sync_engine.py     ← sync orchestration, _write_sync_log
      backup.py          ← Google Drive + GitHub backup functions
      platforms/         ← one file per data source
        __init__.py
        amazon.py        ← all SP-API logic (LIVE)
        walmart.py       ← Walmart API (STUB - Phase 5)
        shopify.py       ← Shopify API (STUB - Phase 5)
        scintilla.py     ← Scintilla report ingestion (Phase 4)
        belk.py          ← Belk report ingestion (Phase 4)
        albertsons.py    ← Albertsons report ingestion (Phase 4)
        family_dollar.py ← Family Dollar report ingestion (Phase 4)
  frontend/
    src/
      App.jsx            ← main app, routing
      services/
        api.js           ← ALL fetch() calls go here only
        metrics.js       ← all client-side formula calculations
      hooks/
        useAutoSave.js   ← 500ms debounce save
        usePolling.js    ← setInterval data refresh
        useSyncStatus.js ← watches sync_log
      components/
        layout/
          Sidebar.jsx
          Header.jsx
        shared/
          KPICard.jsx
          StatusPill.jsx
          DataTable.jsx
          LoadingState.jsx
          ErrorState.jsx
          DivisionPicker.jsx   ← Golf / Housewares / All filter
          CustomerPicker.jsx   ← Customer/channel filter
      pages/             ← one file per dashboard tab
```

## Database
- Engine: DuckDB (migrating to PostgreSQL in Phase 2)
- Live path on Railway: /app/data/golfgen.duckdb
- Local dev path: data/golfgen_amazon.duckdb
- DB_PATH env var controls which path is used
- Single connection only — defined in core/database.py
- Never open a second duckdb.connect() anywhere

## Key Database Tables

### Transactional Tables (all have division + customer + platform columns)
- orders — Amazon orders from SP-API
  IMPORTANT: division and customer determined by item_master lookup on ASIN
- order_items — line items per order
- financial_events — refunds, fees, adjustments
  IMPORTANT: column is event_type NOT transaction_type
  IMPORTANT: refund filter is event_type ILIKE '%refund%'
- fba_inventory — FBA stock levels
- daily_sales — aggregated daily sales
- advertising — Amazon Ads campaign data

### Lookup / Reference Tables
- item_master — product catalog, SOURCE OF TRUTH for division mapping
  Columns: asin, sku, product_name, division, customer, category, brand
  CRITICAL: Every ASIN must have division set before syncs can tag it correctly
- amazon_coupons — coupon status per ASIN

### Planning / Operations Tables
- item_plan_overrides — monthly unit/AUR overrides per SKU
- item_plan_curve_selection — curve type per SKU
- factory_on_order — factory purchase orders
- factory_on_order_items — line items per PO
- otw_shipments — inbound container tracking

### System Tables
- sync_log — record of every sync run
  Columns: job_name, started_at, completed_at, status,
  records_processed, error_message, execution_time_seconds
  IMPORTANT: column is records_processed NOT records_inserted
  IMPORTANT: column is execution_time_seconds NOT duration_seconds
- sessions — user login sessions (18hr expiration)
- user_permissions — per-user tab access controls
- audit_log — every login, access, and admin action (Phase 1)

### Future Analytics Tables (Phase 2)
- staging_orders — cleaned orders with all three hierarchy columns
- staging_financial_events — normalized across all platforms
- analytics_daily — pre-rolled: date + division + customer + platform
- analytics_sku — per-ASIN: division + customer + metrics
- analytics_ads — campaign performance rolled up

## Environment Variables (set in Railway)
SP_API_CLIENT_ID — Amazon SP-API client ID
SP_API_CLIENT_SECRET — Amazon SP-API client secret
SP_API_REFRESH_TOKEN — Amazon SP-API refresh token
SP_API_MARKETPLACE_ID — ATVPDKIKX0DER (US)
SP_API_AWS_ACCESS_KEY — AWS access key for role assumption
SP_API_AWS_SECRET_KEY — AWS secret key for role assumption
SP_API_ROLE_ARN — arn:aws:iam::855246887724:role/GolfGenSPAPIRole
ADS_API_CLIENT_ID — Amazon Ads API client ID
ADS_API_CLIENT_SECRET — Amazon Ads API client secret
ADS_API_REFRESH_TOKEN — Amazon Ads API refresh token
GOOGLE_SERVICE_ACCOUNT_JSON — Google service account for Drive backup
BACKUP_DRIVE_FOLDER_ID — Google Drive folder ID for backups
GITHUB_TOKEN — GitHub personal access token for backup repo
BACKUP_GITHUB_REPO — efine2010-Golfgen/golfgen-backups
DB_PATH — /app/data/golfgen.duckdb

## Users
- Eric (eric@egbrands.com) — Admin, full access
- Ty (tysams@egbrands.com) — Staff
- Kim (kim@egbrands.com) — Staff
- Ryan (ryan@egbrands.com) — Staff
- McKay (riseecom21@gmail.com) — Staff

## Critical Rules — Follow in Every Session
1. DuckDB connection defined ONCE in core/database.py only
   Never open duckdb.connect() anywhere else
2. DB_PATH defined ONCE in core/config.py only
3. All datetime.now() must use ZoneInfo("America/Chicago")
   Never use utcnow() or hardcode UTC offsets
4. Every CREATE TABLE must have IF NOT EXISTS
5. Never use DROP TABLE or DELETE FROM on existing data tables
6. All CREATE SEQUENCE before CREATE TABLE in database.py
7. Auto-save debounced 500ms — never save on every keystroke
8. All formula math client-side in React — no server round-trips
9. After any frontend change: npm run build in webapp/frontend/
   then copy dist/ to webapp/backend/dist/
10. Every sync run writes to sync_log with correct column names:
    records_processed, execution_time_seconds
11. EVERY transactional table must have division, customer, platform columns
12. Division is always determined by item_master lookup — never hardcoded
13. event_type not transaction_type in financial_events
14. Refund filter: event_type ILIKE '%refund%'

## How to Confirm Deployment Worked
After pushing to GitHub wait 2 minutes then run:
curl -s https://golfgen-dashboard-production-ce30.up.railway.app/api/health | python3 -m json.tool

Should return JSON with status: healthy and non-zero table row counts.

## Current Known Issues (as of March 2026)
- Monthly YOY tab returns 500 error — date_part CAST fix needed
- Returns showing $0 — event_type column fix needed
- Advertising sync showing 0 records — ads API fix needed
- Item Plan tab returns 500 error — needs investigation
- OTW tab missing ETD/ETA/port data — needs fix
- division/customer/platform columns not yet added to tables — Phase 2

## Architecture Roadmap Summary
Phase 1 (This Week): Bug fixes + security (Google SSO, 18hr sessions, audit log)
Phase 2 (2-3 Weeks): PostgreSQL + staging/analytics layers + division/customer/platform columns
Phase 3 (3-4 Weeks): Hot standby + backup verification + enhanced backup schedule
Phase 4 (1 Month): File uploads + Google Drive automation + Scintilla/Belk/Albertsons/FD ingestion
Phase 5 (6-8 Weeks): Walmart + Shopify + all platform integrations
Phase 6 (2-3 Months): Advanced features + forecasting + anomaly alerts

## Backup System
- Google Drive: nightly 2am Central, 30-day retention
  Manual trigger: POST /api/backup/trigger
  Status: GET /api/backup/status
- GitHub: nightly after Drive backup
  Repo: github.com/efine2010-Golfgen/golfgen-backups
  Manual trigger: POST /api/backup/github-trigger
  Status: GET /api/backup/github-status

## Sync Schedule
- SP-API orders + financial events + inventory: every 4 hours
- Amazon Ads API: every 4 hours
- Startup catchup: runs any missed sync on container start
- Analytics rollup (Phase 2): nightly 2:30am Central
- Backup verification (Phase 3): weekly Sunday 3am Central
