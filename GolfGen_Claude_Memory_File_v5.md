# GolfGen Dashboard — Claude Memory File
**Last Updated:** March 13, 2026
**Updated By:** Eric / Claude session
**Version:** 1.4

---

> **HOW TO USE THIS FILE**
> After any Claude crash or new session, paste this entire file into the
> Claude.ai chat and say: "This is my project memory file. Read it completely
> and confirm you understand the project before we continue."

---

## 1. WHO WE ARE

**Company:** Elite Global Brands (EGB) / GolfGen LLC
**Location:** Bentonville, AR
**Eric Fine** — eric@egbrands.com — Admin, project owner

**Two divisions, one dashboard:**
- **Golf** — GolfGen LLC (training aids, accessories, on-course items)
- **Housewares** — Elite Global Brands (casserole carriers, tumblers, kitchen)

---

## 2. THE PROJECT

**What it is:** Internal commerce dashboard — tracks sales, inventory,
orders, advertising, profitability, and operations across all channels.
**Live URL:** https://golfgen-dashboard-production.up.railway.app
**GitHub:** https://github.com/efine2010-Golfgen/golfgen-dashboard
**Backup Repo:** https://github.com/efine2010-Golfgen/golfgen-backups
**Stack:** FastAPI + DuckDB (PostgreSQL ready) + React, single container on Railway
**Deploy:** Push to GitHub main → Railway auto-deploys in 2 minutes

---

## 3. THE MOST IMPORTANT CONCEPT — BUSINESS HIERARCHY

```
Division (Golf / Housewares)
     ↓
Customer / Channel
     ↓
Platform / Data Source
     ↓
SKU / ASIN
```

### Every transactional table has THREE mandatory columns:
| Column | Values |
|--------|--------|
| division | 'golf' \| 'housewares' \| 'unknown' |
| customer | 'amazon' \| 'walmart_marketplace' \| 'walmart_stores' \| 'shopify' \| 'first_tee' \| 'belk' \| 'hobby_lobby' \| 'albertsons' \| 'family_dollar' |
| platform | 'sp_api' \| 'walmart_api' \| 'shopify_api' \| 'scintilla' \| 'excel_upload' \| 'email' |

### Division is ALWAYS determined by item_master lookup on ASIN.
Never hardcode division. Look it up. If not found → 'unknown'.

---
## 4. COMPLETE CHANNEL MAP (12 channels)

| Division | Customer | Platform | Sync Method | Status |
|----------|----------|----------|-------------|--------|
| golf | amazon | sp_api | Auto (scheduled) | LIVE ✓ |
| golf | walmart_marketplace | walmart_api | Auto | Phase 5 |
| golf | walmart_stores | scintilla | Report upload | Phase 4 |
| golf | shopify | shopify_api | Auto | Phase 5 |
| golf | first_tee | excel_upload | Manual upload | Phase 4 |
| housewares | amazon | sp_api | Auto (scheduled) | LIVE ✓ |
| housewares | walmart_marketplace | walmart_api | Auto | Phase 5 |
| housewares | walmart_stores | scintilla | Report upload | Phase 4 |
| housewares | belk | excel_upload | Upload/email | Phase 4 |
| housewares | hobby_lobby | excel_upload | Upload/email | Phase 4 |
| housewares | albertsons | excel_upload | Upload/email | Phase 4 |
| housewares | family_dollar | excel_upload | Upload/email | Phase 4 |

---

## 5. CURRENT DATA STATUS

**Database size:** ~11.8 MB
**Last confirmed:** March 13, 2026

| Table | Row Count | Notes |
|-------|-----------|-------|
| orders | 200 | Recent window, SP-API auto-tags |
| daily_sales | 1,072 | Through March 13 — data flowing daily ✓ |
| financial_events | 1,357 | 1,280 nonzero — $0 bug FIXED ✓ |
| fba_inventory | 70 | SP-API auto-tags |
| advertising | 0 | Ads returning empty, campaigns under investigation |
**Financial breakdown:**
- Refunds: 182 rows / $26,377 total
- Shipments: 1,175 rows / $173,003 in charges

**SP-API sync:** Running on schedule (9am, 12pm, 3pm, 6pm + hourly today sync) ✓
**Today sync:** Scoped to 2 days, 50-order cap, 120s timeout ✓
**Ads sync:** Every 2 hours — credentials fixed, reports returning empty
**Backup:** Nightly to Google Drive + GitHub ✓
**Analytics rollup:** Nightly at 2:30am Central ✓

---

## 6. WHAT HAS BEEN BUILT (COMPLETED)

### Infrastructure ✓
- FastAPI + DuckDB + React on Railway — deployed and running
- SP-API credentials configured in Railway
- Auto-deploy from GitHub — push to main = live in 2 min
- Nightly backup to Google Drive + GitHub backup repo
- GitHub backup status display fixed in System tab

### Security ✓ (all live)
- Session-based auth with login/logout
- User permissions per tab
- Google SSO with whitelisted email list
- MFA/TOTP enrollment and verification
- 18hr session expiry + 2hr idle timeout
- Audit log table + viewer in System tab
### Sync Hardening ✓
- Sync mutex (threading.Lock) prevents overlapping syncs
- Transaction wrapping — rollback on failure
- Retry with exponential backoff on throttle/429 (3 retries, max 15s delay)
- Startup catchup runs missed syncs on container start
- Today sync: 2-day scope, 50-order cap, 120s hard timeout
- In-memory log buffer (200 entries) for remote debugging

### PostgreSQL Migration — CODE COMPLETE ✓
- DbConnection wrapper in core/database.py auto-detects DuckDB vs PostgreSQL
- When DATABASE_URL env var is set → PostgreSQL mode; otherwise → DuckDB mode
- SQL translator: ? → %s, INSERT OR IGNORE → ON CONFLICT DO NOTHING,
  INSERT OR REPLACE → ON CONFLICT (pk) DO UPDATE SET ...
- All 56+ direct duckdb.connect() calls eliminated across 16 files
- Access via get_db() (read-only) and get_db_rw() (read-write)
- **Next step:** Eric provisions PostgreSQL on Railway + sets DATABASE_URL

### Division Hierarchy ✓
- division/customer/platform columns on ALL transactional tables
- item_master seeded with division tags, tagging UI live
- SP-API auto-tags division from item_master on every insert
- Shared hierarchy_filter() helper in core/hierarchy.py
- All routers (sales, profitability, inventory, advertising) use shared helper
- HierarchyFilter.jsx component in header, wired to all pages
- All frontend API calls pass filters through

### Analytics ✓
- staging_orders and staging_financial_events tables created
- analytics_daily, analytics_sku, analytics_ads tables created
- Nightly analytics rollup at 2:30am Central
- Sales router reads from analytics_daily when available
### Debug Endpoints ✓
- GET /api/debug/db-diagnostic — recent daily_sales, financial_events summary, sync_log
- GET /api/debug/test-financial-parse — fetches one financial event, shows parsed output
- GET /api/debug/logs — recent in-memory log messages from sync engine
- GET /api/health — row counts, DB size, uptime

### Dashboard Tabs (22 pages)
- Dashboard — working ✓
- Products — working ✓
- Sales / Orders — working ✓
- Inventory (Amazon FBA) — working ✓
- GolfGen Inventory — working ✓
- Golf Warehouse / Housewares Warehouse — working ✓
- Advertising — working (no data yet, ads returning empty) ✓
- Profitability / P&L — working ✓
- Item Planning — working ✓
- Factory PO — working ✓
- Supply Chain / Logistics Tracking — working ✓
- FBA Shipments — working ✓
- Item Master — working, division tagging UI live ✓
- Permissions — working ✓
- MFA Setup / MFA Admin / MFA Verify — working ✓
- Audit Log — working ✓
- System — working, backup/restore/sync controls ✓

### Recovery System ✓
- POST /api/backup/restore — restore DB from Drive
- GET /api/backup/list-drive — list available backups
- Restore button in System tab UI
- This memory file — Claude context recovery
---

## 7. THE 6-PHASE ROADMAP

### Phase 1 — Stability + Security: COMPLETE ✓
All 10 items done and deployed.

### Phase 2 — Division Hierarchy + Data Layer: COMPLETE ✓
- ✅ Division/customer/platform columns on all tables
- ✅ Item master seeded, tagging UI live
- ✅ SP-API auto-tags division on every insert
- ✅ Shared hierarchy_filter() on all routers
- ✅ HierarchyFilter.jsx in header, all pages wired
- ✅ Staging tables created (staging_orders, staging_financial_events)
- ✅ Analytics tables created (analytics_daily, analytics_sku, analytics_ads)
- ✅ Nightly analytics rollup at 2:30am Central
- ✅ PostgreSQL dual-mode wrapper (DbConnection) — code complete
- ✅ SQL translator for DuckDB↔PostgreSQL — code complete
- ✅ All 56+ duckdb.connect() calls eliminated
- **Action item:** Eric provisions PostgreSQL on Railway + sets DATABASE_URL

### Phase 3 — Redundancy + Backup Hardening: PARTIAL
- ✅ Nightly Google Drive backup (2am Central)
- ✅ Nightly GitHub backup
- ❌ Hot standby / second Railway region
- ❌ Weekly backup verification
- ❌ 6-hour snapshots + monthly archives
### Phase 4 — File Upload + Store Channel Ingestion: PARTIAL
- ✅ Excel upload for warehouse data
- ❌ Scintilla report ingestion (walmart_stores)
- ❌ First Tee, Belk, Hobby Lobby, Albertsons, Family Dollar ingestion
- ❌ Google Drive watched folder
- ❌ Email ingestion endpoint

### Phase 5 — Multi-Platform APIs: PLANNED
- ❌ Walmart Marketplace API
- ❌ Shopify API
- ❌ Cross-platform revenue + inventory views

### Phase 6 — Intelligence + Forecasting: PLANNED
- ❌ Custom date range picker
- ❌ Daily email summary
- ❌ Anomaly alerts
- ❌ Forecast tab
- ❌ Executive home dashboard

---

## 8. CRITICAL RULES — NEVER BREAK THESE

1. Database access via get_db() / get_db_rw() from core.database ONLY — never call duckdb.connect() directly
2. DB_PATH defined ONCE in core/config.py only
3. All datetime.now() uses ZoneInfo("America/Chicago") — never utcnow()
4. Every CREATE TABLE has IF NOT EXISTS
5. Never DROP TABLE or DELETE FROM on existing data tables
6. After any frontend change: npm run build → copy dist → commit → push
7. sync_log columns: records_processed, execution_time_seconds8. financial_events: event_type not transaction_type
9. Refund filter: event_type ILIKE '%refund%'
10. orders table: asin not sku
11. Division always from item_master lookup — never hardcoded
12. Every transactional table must have division + customer + platform
13. All routers use core.hierarchy.hierarchy_filter() — never build filter SQL manually
14. SQL must be dialect-aware — DbConnection wrapper auto-translates for PostgreSQL
15. Never modify _sync_lock or _sync_running outside the mutex pattern in sp_api.py

---

## 9. DEPLOY + VERIFY COMMANDS

### Deploy any change:
```
cd webapp/frontend && npm run build
cp -r dist ../backend/dist
cd ../..
git add -A && git commit -m "your message" && git push origin main
# Wait 2 minutes
curl -s https://golfgen-dashboard-production.up.railway.app/api/health | python3 -m json.tool
```

### Crash recovery:
```
# Option A: Dashboard → System tab → Backup → Restore from Drive
# Option B: POST /api/backup/restore with body {}
```

---

## 10. USERS

| Name | Email | Role |
|------|-------|------|| Eric Fine | eric@egbrands.com | Admin — full access |
| Ty | tysams@egbrands.com | Staff |
| Kim | kim@egbrands.com | Staff |
| Ryan | ryan@egbrands.com | Staff |
| McKay | riseecom21@gmail.com | Staff |

---

## 11. ENVIRONMENT VARIABLES (all set in Railway)

SP_API_CLIENT_ID, SP_API_CLIENT_SECRET, SP_API_REFRESH_TOKEN,
SP_API_MARKETPLACE_ID (ATVPDKIKX0DER), SP_API_AWS_ACCESS_KEY,
SP_API_AWS_SECRET_KEY, SP_API_ROLE_ARN,
ADS_API_CLIENT_ID, ADS_API_CLIENT_SECRET, ADS_API_REFRESH_TOKEN,
GOOGLE_SERVICE_ACCOUNT_JSON, BACKUP_DRIVE_FOLDER_ID,
GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET,
GITHUB_TOKEN, BACKUP_GITHUB_REPO (efine2010-Golfgen/golfgen-backups),
DB_PATH (/app/data/golfgen_amazon.duckdb),
DATABASE_URL (not yet set — provision PostgreSQL on Railway then set this)

---

## 12. SESSION LOG — WHAT WAS DONE AND WHEN

### Session 5 (March 13, 2026) — Sync Fix + PostgreSQL Migration Complete
- **Root cause found:** Today sync was hanging (fetching 100+ order items with aggressive retries),
  holding global _sync_lock forever, blocking full sync which handles financial_events + daily_sales
- **Fix applied:** Reduced today sync scope from 7→2 days, added 50-order cap,
  reduced retries 5→3 / max_delay 60s→15s, added 120s hard timeout via concurrent.futures
- **Result:** All three dashboard data issues resolved:  - financial_events: 1,357 rows, 1,280 nonzero (was 84 all zeros)
  - daily_sales: populated through March 13 (was stopped at March 8)
  - Refunds: 182 rows / $26,377 (was 0 rows)
  - Shipments: 1,175 rows / $173,003 in charges
- **PostgreSQL migration marked code-complete:** DbConnection wrapper, SQL translator,
  all 56+ duckdb.connect() calls eliminated — awaiting Eric to provision PG on Railway
- **Debug endpoints added:** /api/debug/db-diagnostic, /api/debug/test-financial-parse, /api/debug/logs
- **In-memory log buffer added:** 200-entry ring buffer for remote sync debugging
- **Fixed debug credential loading:** system.py was returning wrong key names for SP-API auth
- **Enhanced _money() parser:** Added diagnostic logging and fallback parsing for financial amounts
- **5 commits:** af30c1c, 39beba2, 61dd8c1, ad8df07, 36fdbc8

### Session 4 (March 13, 2026) — Hierarchy Filter Completion + Context Update
- Verified actual codebase against docs — found docs were stale
- Created shared core/hierarchy.py helper
- Wired hierarchy filter into all routers (sales, profitability, inventory, advertising)
- Added division/customer/platform params to 4 ads endpoints that were missing them
- Updated Advertising.jsx to pass filters to all API calls
- Built, committed (a63a29c), pushed, deployed — health check passed
- Updated CLAUDE.md, Memory File v4, and Architecture Roadmap to match reality

### Session 3 (March 13, 2026) — Customer List Expansion + Phase 2 Build
- Added First Tee (golf) and Hobby Lobby (housewares) as new customers
- Updated all valid customer values throughout all docs and CLAUDE.md
- Built complete Phase 2 Cowork prompt (Prompt F)
- Updated Architecture Roadmap to v3 with all 12 channels

### Session 2 (March 13, 2026) — Architecture + Recovery
- Fixed GitHub backup status display
- Confirmed live data flowing
- Created CLAUDE.md, Architecture Roadmap, Memory File
- Built restore-from-Drive endpoint and UI
- Created all Cowork prompts (A through E)
### Session 1 (early March 2026) — Initial Build
- Built initial dashboard — FastAPI + DuckDB + React on Railway
- SP-API connected and syncing
- Basic tabs, backup to Google Drive + GitHub

### NEXT SESSION — Start Here:
1. Eric: Provision PostgreSQL on Railway and set DATABASE_URL env var
2. Verify PostgreSQL mode activates correctly after DATABASE_URL is set
3. Then Phase 4 store channel ingestion or Phase 3 backup hardening
4. Investigate ads sync returning empty (campaigns under investigation)

---

*Update this file at the end of every session. Save back to Google Drive.*