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
      sp_api.py          ← all SP-API calls
      ads_api.py         ← all Amazon Ads API calls
      sync_engine.py     ← sync orchestration, _write_sync_log
      backup.py          ← Google Drive + GitHub backup functions
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
      pages/             ← one file per dashboard tab
```

## Database
- Engine: DuckDB
- Live path on Railway: /app/data/golfgen.duckdb
- Local dev path: data/golfgen_amazon.duckdb
- DB_PATH env var controls which path is used
- Single connection only — defined in core/database.py
- Never open a second duckdb.connect() anywhere

## Key Database Tables
- orders — Amazon orders from SP-API
- order_items — line items per order
- financial_events — refunds, fees, adjustments from SP-API
  IMPORTANT: column is event_type NOT transaction_type
  IMPORTANT: refund filter is event_type ILIKE '%refund%'
- fba_inventory — FBA stock levels from SP-API
- daily_sales — aggregated daily sales
- advertising — Amazon Ads campaign data
- sync_log — record of every sync run
  Columns: job_name, started_at, completed_at, status,
  records_processed, error_message, execution_time_seconds
  IMPORTANT: column is records_processed NOT records_inserted
  IMPORTANT: column is execution_time_seconds NOT duration_seconds
- item_master — product catalog
- item_plan_overrides — monthly unit/AUR overrides per SKU
- item_plan_curve_selection — curve type per SKU
- factory_on_order — factory purchase orders
- factory_on_order_items — line items per PO
- otw_shipments — inbound container tracking
- sessions — user login sessions
- user_permissions — per-user tab access controls
- amazon_coupons — coupon status per ASIN

## Environment Variables (set in Railway)
SP_API_REFRESH_TOKEN — Amazon SP-API refresh token
SP_API_CLIENT_ID — Amazon SP-API client ID
SP_API_CLIENT_SECRET — Amazon SP-API client secret
SP_API_MARKETPLACE_ID — Amazon marketplace ID
ADS_API_CLIENT_ID — Amazon Ads API client ID
ADS_API_CLIENT_SECRET — Amazon Ads API client secret
ADS_API_REFRESH_TOKEN — Amazon Ads API refresh token
GOOGLE_SERVICE_ACCOUNT_JSON — Google service account for Drive backup
BACKUP_DRIVE_FOLDER_ID — Google Drive folder ID for backups
GITHUB_TOKEN — GitHub personal access token for backup repo
BACKUP_GITHUB_REPO — efine2010-Golfgen/golfgen-backups
DB_PATH — /app/data/golfgen.duckdb

## Users
- Eric (eric@golfgen.com / eric@egbrands.com) — Admin
- Ty (ty@golfgen.com / tysams@egbrands.com) — Staff
- Kim (kim@golfgen.com / kim@egbrands.com) — Staff
- Ryan (ryan@golfgen.com / ryan@egbrands.com) — Staff
- McKay (riseecom21@gmail.com) — Staff

## Critical Rules — Follow These in Every Session
1. DuckDB connection defined ONCE in core/database.py only
   Never open duckdb.connect() anywhere else
2. DB_PATH defined ONCE in core/config.py only
   Never hardcode the path in two places
3. All datetime.now() must use ZoneInfo("America/Chicago")
   Never use utcnow() or hardcode UTC offsets
4. Every CREATE TABLE must have IF NOT EXISTS
5. Never use DROP TABLE or DELETE FROM on existing data tables
6. All CREATE SEQUENCE before CREATE TABLE in database.py
7. Auto-save debounced 500ms — never save on every keystroke
8. All formula math client-side in React — no server 
   round-trips for calculations
9. After any frontend change: npm run build in webapp/frontend/
   then copy dist/ to webapp/backend/dist/
10. Every sync run writes to sync_log with correct column names:
    records_processed, execution_time_seconds (not records_inserted,
    not duration_seconds)

## How to Confirm Deployment Worked
After pushing to GitHub wait 2 minutes then run:
curl -s https://golfgen-dashboard-production-ce30.up.railway.app/api/health

Should return JSON with status: healthy and table row counts.

## Current Known Issues (as of March 2026)
- Item Plan tab returns 500 error — needs fix
- OTW tab missing ETD/ETA/port data — needs fix
- Factory PO column order wrong — needs fix
- Coupon status not showing on Profitability — needs fix

## Backup System
- Google Drive: nightly 2am Central, 30-day retention
  Manual trigger: POST /api/backup/trigger
  Status: GET /api/backup/status
- GitHub: nightly after Drive backup
  Repo: github.com/efine2010-Golfgen/golfgen-backups
  Manual trigger: POST /api/backup/github-trigger
  Status: GET /api/backup/github-status

## Sync Schedule
- SP-API orders: every 4 hours
- SP-API financial events: every 4 hours  
- SP-API inventory: every 4 hours
- Ads API: every 4 hours
- Startup catchup: runs any missed sync on container start
```

---

Commit message:
```
docs: update CLAUDE.md with full project context and rules
