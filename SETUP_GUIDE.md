# GolfGen Commerce Dashboard — Setup & Operations Guide

## What This Is

A full-stack commerce analytics dashboard for GolfGen LLC / Elite Global Brands. It pulls live data from Amazon SP-API and Amazon Ads API on a scheduled basis and displays sales, advertising, inventory, profitability, and planning data in an interactive React frontend.

**Live URL:** https://golfgen-dashboard-production.up.railway.app
**GitHub:** https://github.com/efine2010-Golfgen/golfgen-dashboard

---

## Architecture at a Glance

- **Frontend:** React + Vite (builds to static files served by backend)
- **Backend:** Python FastAPI
- **Database:** PostgreSQL (managed by Railway)
- **Hosting:** Railway (auto-deploys from GitHub `main` branch)
- **Data Sources:** Amazon SP-API (orders, inventory, financials), Amazon Ads API (campaigns, keywords)
- **Backup:** Nightly to Google Drive + GitHub

---

## Deployment

The dashboard auto-deploys to Railway whenever code is pushed to the `main` branch on GitHub. There is no manual deployment step.

### Deploy Workflow
1. Make code changes locally
2. Build the frontend:
   ```bash
   cd webapp/frontend && npm run build
   ```
3. Copy built files to backend:
   ```bash
   cp -r webapp/frontend/dist webapp/backend/dist
   ```
4. Commit and push:
   ```bash
   git add <relevant files>
   git commit -m "your message"
   git push origin main
   ```
5. Railway auto-deploys within ~2 minutes
6. Verify:
   ```bash
   curl -s https://golfgen-dashboard-production.up.railway.app/api/health | python3 -m json.tool
   ```

**Important:** Never manually restart Railway. Never use Railway CLI.

---

## Local Development

### Prerequisites
- Python 3.8+
- Node.js 18+ (for frontend builds)
- Git

### Backend Setup
```bash
cd webapp/backend
pip install -r requirements.txt
```

Without `DATABASE_URL` set, the backend falls back to DuckDB (file-based). To develop against PostgreSQL locally, set `DATABASE_URL` to a local Postgres connection string.

### Frontend Setup
```bash
cd webapp/frontend
npm install
npm run dev    # Dev server with hot reload (proxies API to backend)
```

### Running the Backend Locally
```bash
cd webapp/backend
python -m uvicorn main:app --reload --port 8000
```

---

## Database

### PostgreSQL (Primary — LIVE)
- Managed PostgreSQL on Railway
- Connected via `DATABASE_URL` environment variable
- All 29 tables live and receiving synced data
- Data persists independently of container redeploys

### DuckDB (Fallback)
- File at `/app/data/golfgen_amazon.duckdb` on Railway volume
- Used automatically if `DATABASE_URL` is not set
- Emergency fallback: remove `DATABASE_URL` env var on Railway to switch back

### Tables (29 total)

**Transactional** (all have division + customer + platform columns):
orders, daily_sales, financial_events, fba_inventory, advertising, ads_campaigns, ads_keywords, ads_search_terms, ads_negative_keywords

**Staging:** staging_orders, staging_financial_events

**Analytics** (pre-aggregated): analytics_daily, analytics_sku, analytics_ads

**Reference:** item_master (source of truth for division mapping)

**Planning/Operations:** monthly_sales_history, item_plan_overrides, item_plan_curve_selection, item_plan_factory_orders, item_plan_factory_order_items, item_plan_settings

**System:** sessions, user_permissions, audit_log, sync_log, docs_update_log

---

## Data Sync Schedule

All scheduled jobs run automatically via APScheduler (configured in `core/scheduler.py`). Times are Central (America/Chicago).

| Job | Schedule | What It Does |
|-----|----------|--------------|
| Full SP-API sync | 9am, 12pm, 3pm, 6pm | Orders + Financial Events + Inventory + Sales Report |
| Today quick sync | Every hour at :30 | Last 2 days of orders (50-order cap, 120s timeout) |
| Ads sync | Every 2 hours | Campaign, keyword, search term reports |
| Pricing sync | Every hour at :30 | Current prices from SP-API |
| Analytics rollup | 2:30am | Pre-aggregate into analytics tables |
| Google Drive backup | 2am | DuckDB file backup, 30-day retention |
| GitHub backup | After Drive backup | Push to backup repository |
| Docs update | 8am + 8pm | Documentation auto-update |

---

## Environment Variables (set on Railway)

**Amazon SP-API:**
SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, SP_API_REFRESH_TOKEN, SP_API_MARKETPLACE_ID (ATVPDKIKX0DER), SP_API_AWS_ACCESS_KEY, SP_API_AWS_SECRET_KEY, SP_API_ROLE_ARN

**Amazon Ads API:**
ADS_API_CLIENT_ID, ADS_API_CLIENT_SECRET, ADS_API_REFRESH_TOKEN

**Google (SSO + Backup):**
GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, GOOGLE_SERVICE_ACCOUNT_JSON, BACKUP_DRIVE_FOLDER_ID

**GitHub Backup:**
GITHUB_TOKEN, BACKUP_GITHUB_REPO (efine2010-Golfgen/golfgen-backups)

**Database:**
DATABASE_URL (${{Postgres.DATABASE_URL}} — references Railway Postgres service), DB_PATH (/app/data/golfgen_amazon.duckdb — DuckDB fallback path)

---

## Users & Access

| User | Email | Role |
|------|-------|------|
| Eric | eric@egbrands.com | Admin (full access) |
| Ty | tysams@egbrands.com | Staff |
| Kim | kim@egbrands.com | Staff |
| Ryan | ryan@egbrands.com | Staff |
| McKay | riseecom21@gmail.com | Staff |

Authentication is via Google SSO with whitelisted emails. MFA/TOTP is available. Sessions expire after 18 hours with a 2-hour idle timeout.

---

## Debug & Monitoring

These endpoints require no authentication:

| Endpoint | What It Shows |
|----------|---------------|
| `GET /api/health` | Row counts, DB size, uptime |
| `GET /api/debug/db-diagnostic` | Recent daily_sales, financial_events summary, sync_log |
| `GET /api/debug/test-financial-parse` | Fetches one financial event from API, shows parsed output |
| `GET /api/debug/logs` | Recent in-memory log messages (200-entry ring buffer) |

---

## Backup & Recovery

### Backups
- **Google Drive:** Nightly 2am Central, 30-day retention. Manual trigger: `POST /api/backup/trigger`
- **GitHub:** Nightly after Drive backup. Manual trigger: `POST /api/backup/github-trigger`
- **List backups:** `GET /api/backup/list-drive`

### Recovery
With PostgreSQL, data persists independently of container deploys — routine redeploys don't affect data.

**If PostgreSQL data is lost:**
1. Remove `DATABASE_URL` env var on Railway → app falls back to DuckDB
2. Re-add `DATABASE_URL` → triggers auto-migration from DuckDB to PostgreSQL

**Legacy DuckDB restore (if needed):**
- Dashboard → System tab → Backup → Restore from Drive
- Or: `POST /api/backup/restore` with body `{}`

---

## Dashboard Features

- **22 pages** covering sales, orders, advertising, inventory, profitability, item planning, factory POs, logistics, supply chain, item master, and system admin
- **Hierarchy filter** in header: filter by Division (Golf/Housewares), Customer, Platform
- **KPI cards** with period-over-period trends
- **Charts:** daily revenue, monthly YOY, ad performance, profit waterfall
- **Tables:** sortable product performance, FBA inventory with days-of-supply
- **Planning tools:** item plan with sales curves, factory on-order tracking
- **Admin:** user permissions, audit log, sync status, backup controls

---

## Folder Structure

```
webapp/
  backend/
    main.py              ← FastAPI app init, router registration, Google SSO
    requirements.txt     ← Python dependencies
    dist/                ← Compiled React frontend (copy here after build)
    core/                ← Shared business logic (config, database, auth, scheduler)
    routers/             ← API endpoint handlers (12 router files)
    services/            ← External API integrations and background jobs
    scripts/             ← Migration and export utilities
  frontend/
    src/
      App.jsx            ← Main app, routing, passes filters to all pages
      components/        ← Shared components (HierarchyFilter, etc.)
      lib/               ← API client (api.js), constants
      pages/             ← One file per dashboard tab (22 pages)
    package.json
    vite.config.js
```

---

## Roadmap

### Completed
- **Phase 1:** Stability + Security (bug fixes, sync mutex, Google SSO, audit log)
- **Phase 2:** Modular architecture, division hierarchy, PostgreSQL migration

### In Progress
- **Phase 3:** Backup hardening (hot standby, weekly verification, 6-hour snapshots)
- **Phase 4:** File upload for store channels (Scintilla, Belk, Hobby Lobby, etc.)

### Planned
- **Phase 5:** Multi-platform APIs (Walmart Marketplace, Shopify)
- **Phase 6:** Intelligence + Forecasting (anomaly alerts, forecast tab, exec dashboard)
