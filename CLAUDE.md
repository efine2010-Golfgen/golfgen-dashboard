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
Live URL: https://golfgen-dashboard-production-ce30.up.railway.app
GitHub: https://github.com/efine2010-Golfgen/golfgen-dashboard
Backup Repo: https://github.com/efine2010-Golfgen/golfgen-backups

## How Deployment Works
Auto-deploys to Railway when code is pushed to main branch on GitHub.
CRITICAL: Dockerfile copies from webapp/backend/dist/ — NOT webapp/frontend/dist/.
1. Make code changes
2. cd webapp/frontend && npm run build
3. rm -rf webapp/backend/dist && cp -r webapp/frontend/dist webapp/backend/dist
4. git add -f webapp/backend/dist webapp/frontend/src/... && git commit -m "your message"
5. git push origin main
6. Railway auto-deploys within 2 minutes
7. Verify live site in browser at https://golfgen-dashboard-production-ce30.up.railway.app

IMPORTANT: webapp/frontend/dist is in .gitignore — must use `git add -f webapp/backend/dist`
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
      ask_claude.py      ← /api/ask-claude (AI assistant)
      retail.py          ← /api/retail/* (Walmart POS upload + query endpoints)
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
        walmart/           ← Walmart Analytics sub-components (split for stability)
          WalmartHelpers.jsx   ← shared: SG, DM, Card, fN, f$, fPct, delta, KPICard, ChartCanvas
          WalmartSales.jsx     ← Sales Performance tab (KPIs, charts, item table)
          WalmartInventory.jsx ← Inventory Health tab
          WalmartScorecard.jsx ← Vendor Scorecard tab (metric matrix by vendor/group/period)
          WalmartEcomm.jsx     ← eCommerce tab
          WalmartForecast.jsx  ← Order Forecast tab
```

## Database
- Engine: PostgreSQL (LIVE on Railway since March 14, 2026)
- Previous engine: DuckDB (file still on Railway volume as fallback at /app/data/golfgen_amazon.duckdb)
- Dual-mode wrapper: core/database.py DbConnection class auto-detects DuckDB vs PostgreSQL
- When DATABASE_URL env var is set → uses PostgreSQL; otherwise → uses DuckDB
- DATABASE_URL is set on Railway via `${{Postgres.DATABASE_URL}}` reference
- Railway Postgres service: b6075866-2c10-484c-ba4f-3c50182c65d0
- Access via get_db() (read-only) and get_db_rw() (read-write) from core.database
- SQL translator: _translate_sql_for_pg() converts ? → %s, INSERT OR IGNORE → ON CONFLICT DO NOTHING, DOUBLE → DOUBLE PRECISION, etc.
- Auto-migration: auto_migrate_from_duckdb() in database.py runs on startup if Postgres is empty and DuckDB file exists — copies all 26 tables
- Health endpoint (/api/health) still shows DB_PATH in "path" field even in Postgres mode — this is cosmetic only, queries go to PostgreSQL
- All Decimal/float type mismatches fixed: _n() helper coerces Decimal to float for JSON serialization
### All Tables (30 total)
Transactional (all have division + customer + platform):
  orders, daily_sales, financial_events, fba_inventory, advertising,
  ads_campaigns, ads_keywords, ads_search_terms, ads_negative_keywords

Staging (cleaned, typed, deduped):
  staging_orders, staging_financial_events

Analytics (pre-aggregated, fast queries):
  analytics_daily, analytics_sku, analytics_ads

Reference:
  item_master (SOURCE OF TRUTH for division mapping)

Pricing/Coupons:
  sale_prices, coupons, coupon_items (junction table for multi-item coupons)

Planning/Operations:
  monthly_sales_history, item_plan_overrides, item_plan_curve_selection,
  item_plan_factory_orders, item_plan_factory_order_items, item_plan_settings

Retail POS (Walmart/Scintilla — all have division + customer + platform):
  walmart_store_weekly, walmart_item_weekly, walmart_scorecard,
  walmart_ecomm_weekly, walmart_order_forecast, retail_upload_log

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

### Today Sync (every 15 minutes)
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
- Today quick sync: every 15 minutes
- Ads sync: every 2 hours
- Pricing sync: every hour at :30
- Docs update: 8am + 8pm Central
- FBA inventory snapshot: 11pm Central (daily, captures inventory history)
- Google Drive + GitHub backup: every 6 hours (2am, 8am, 2pm, 8pm Central)
- Nightly deep sync: 3am Central (fills all gaps + re-pulls last 30 days)
- Gap-fill: every 2 hours at :45 (one 30-day chunk per run)
- Analytics rollup: 2:30am Central (after backup)
- Backup verification: weekly Sunday 4am Central (download + compare to live DB)
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

## Ask Claude — AI Assistant (LIVE)
- Header search bar integrated into filter-bar row on every tab
- Backend: routers/ask_claude.py → POST /api/ask-claude
- Frontend: components/AskClaude.jsx
- Tab-aware: builds data snapshot from PostgreSQL based on active_tab parameter
- Supported tabs: inventory, sales, exec-summary, advertising, profitability, supply-chain/otw
- Auth: requires valid session, Admin + Staff only (Viewer denied)
- Audit: every question logged to audit_log table
- API key: ANTHROPIC_API_KEY env var on Railway (claude-sonnet-4-6 model, max_tokens=1000)
- System prompt includes today's date, active tab, division/customer filters
- Returns {answer, tab_context, data_snapshot_summary}
- Graceful fallback if API key missing or API call fails

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

## Roadmap Status (as of March 15, 2026)

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

### Phase 2b — Inventory Command Center Overhaul: COMPLETE ✓
Full mockup-aligned rebuild of the Inventory page (March 15, 2026):
- Backend: /api/inventory/command-center endpoint serves all data
  - Pipeline with 5 stages: Sellable, Inbound, Reserved, FC Transfer, Unfulfillable
  - Reserved Breakdown (Customer Orders, FC Transfer, FC Processing)
  - Unfulfillable Breakdown (Customer Dmg, Warehouse Dmg, Defective, Carrier Dmg)
  - FC Distribution (8 FCs: PHX3, LAS2, ONT8, DFW7, MDW6, BDL1, SDF8, Other)
  - Return Rate by SKU table (top 10 by return rate, 90-day window)
  - Storage Fee Forecast (6-month projection with Q3 surcharges)
  - KPI Deltas (7d avg vs 30d avg velocity comparison)
  - Expanded Health Metrics (turnover, stranded, researching, reserved%, defect rate, cancellation rate)
  - Stranded Inventory data + Recent Reimbursements
- Frontend: Inventory.jsx full mockup alignment
  - 5 sub-nav tabs: GolfGen Inventory, Amazon Inventory, Shipments to FBA, Stranded & Suppressed, Inventory Ledger
  - Action buttons: Sync FBA, Create Shipment, Export, Alerts
  - 8 KPI cards with delta indicators (▲/▼ percentages)
  - 30-Day Trends: Daily Sales Velocity + Buy Box % charts
  - 90-Day View: FBA Sellable vs Daily Sales Velocity overlay
  - Pipeline card with 5 stages, Reserved + Unfulfillable breakdowns, progress bar
  - Aging Distribution with Monthly Storage Fee Forecast bar chart
  - FC Distribution & Health 3-column grid (FC bars, Return Rate table, Health Metrics with score circle)
  - Replenishment Forecast + Reorder Timeline cards
  - SKU Command Center in grid-main-side layout (1fr 320px)
    - Main: SKU table with risk filters + search
    - Side: Stranded Inventory card + Recent Reimbursements card
- Mockup reference: GolfGen_Amazon_Inventory_v2.html (in uploads)
- Key helpers: SG() / DM() for font styles, Card/CardHdr/SecDiv/Badge/LegItem components
- _n() helper in backend coerces Decimal/None to float for safe JSON serialization

### Phase 2c — Profitability Command Center Overhaul: COMPLETE ✓
Full mockup-aligned rebuild of the Profitability page (March 15, 2026):
- Backend: routers/profitability.py — all endpoints
  - GET /api/profitability/overview — 8 KPIs, waterfall P&L, margin trend, fee donut
  - GET /api/profitability/fee-detail — Fee breakdown by 4 categories (Referral, FBA, Storage, Other) with sub-items
  - GET /api/profitability/items — Item-level profitability with score grades (A/B/C)
  - GET /api/profitability/aur — AUR trend by SKU, scatter/bubble data
  - Sale Prices CRUD: GET/POST/PUT/DELETE /api/profitability/sale-prices
  - Coupons CRUD: GET/POST/PUT/DELETE /api/profitability/coupons (multi-item via coupon_items junction table)
  - POST /api/profitability/push-price/{id} — Push sale price to Amazon (graceful fallback if SP-API not yet implemented)
  - COGS loaded from CSV (COGS_PATH), fallback to 35% of AUR when missing
  - Fee fallback: referral 15%, FBA 12% when financial_events has no data
  - All responses use camelCase keys (grossRevenue, marginTrend, feeDonut, salePrices, etc.)
- Frontend: Profitability.jsx — 5 sub-tab structure
  - P&L Overview: 8 KPI cards, waterfall chart + tabular rows, margin trend chart, fee donut
  - Amazon Fee Detail: 4-category fee breakdown with sub-items, revenue % calculations
  - Item Profitability: SKU-level table with score badges (A/B/C), filter by division/margin health, sortable
  - AUR Analysis: AUR trend table, scatter/bubble chart
  - Pricing & Coupons: Two-section layout:
    - Top: Amazon Active Pricing (read-only from SP-API pricing cache) + Amazon Active Coupons (read-only from Ads API coupon cache)
    - Bottom: Managed Sale Prices CRUD + Managed Coupons CRUD, multi-item coupon support, push-to-Amazon button
  - GET /api/profitability/amazon-pricing — reads pricing_sync.json cache, enriches with item_master names
    - Returns: amazonPricing (per-ASIN listPrice/buyBoxPrice/landedPrice/salePrice/discountPct), amazonCoupons (grouped by couponId), lastSync timestamp
- Database tables (created in core/database.py):
  - sale_prices: id, asin, sku, product_name, regular_price, sale_price, start_date, end_date, marketplace, status, pushed_to_amazon, pushed_at, division, customer, platform
  - coupons: id, title, coupon_type, discount_value, budget, budget_used, start_date, end_date, marketplace, status, pushed_to_amazon, pushed_at, division, customer, platform
  - coupon_items: coupon_id, asin, sku, product_name (PK: coupon_id + asin) — junction table for multi-item coupons
  - PostgreSQL sequences: sale_prices_seq, coupons_seq
- Pydantic models: SalePriceCreate, CouponCreate for POST/PUT validation (snake_case input)
- Mockup reference: GolfGen_Profitability_v1.html (in uploads)
- Key pattern: Backend returns camelCase, frontend reads camelCase, forms submit snake_case (Pydantic models)

### Phase 3 — Redundancy + Backup Hardening: COMPLETE ✓
Done (March 15, 2026):
- Google Drive + GitHub backup upgraded from nightly to every 6 hours (2am, 8am, 2pm, 8pm CT)
- Weekly automated backup verification (Sundays 4am CT): downloads latest backup, extracts CSVs,
  compares row counts table-by-table to live PostgreSQL (10% tolerance)
- Manual verification trigger: POST /api/backup/verify
- DR readiness dashboard: GET /api/backup/dr-status checks backup age, verification status,
  PostgreSQL health (critical table row counts), DuckDB fallback file presence
- System page: new DR Readiness card with green/yellow/red status, verify button, issue list
- Failover path: DuckDB file on Railway volume as emergency fallback (remove DATABASE_URL → auto-switch)

### Phase 4 — File Upload + Store Channels: IN PROGRESS
Done: Excel upload for warehouse data. Walmart Scintilla report ingestion (6 new tables,
5 report type parsers, auto-detect upload, Retail Reporting nav + page with 5 sub-tabs).
Architecture doc: Walmart_Retail_Data_Architecture.md.
Scorecard data loaded (648 rows) with all 8 periods via embedded migration.
WalmartAnalytics.jsx split into 7 sub-components in walmart/ directory for stability.
- POST /api/retail/import-scorecard — JSON import for pre-parsed scorecard data
  (bypasses Excel upload when file parsing has issues)
- dataAvailable flag checks walmart_item_weekly, walmart_scorecard, AND walmart_store_weekly
- has_item_data flag gates item/ecomm/forecast sections separately from scorecard

#### Scorecard Overhaul (March 16, 2026): COMPLETE ✓
- Backend: scripts/scorecard_migration.py — embedded compressed JSON data (648 rows)
  - Runs on startup in main.py lifespan(), checks if Q1 data exists before loading
  - Clears old scorecard data and re-imports with normalized metric names
  - 8 periods: Last Week, Current Month, Last Month, Year, Q1, Q2, Q3, Q4
  - 3 vendor sections: All Vendors, Vendor 77893, Vendor 79010
  - 27 metrics across 4 groups: Store Sales, Store Inventory, DC Inventory, Margins & Markdowns
  - Metric names normalized (e.g. "POS Sales in dollars" → "POS Sales $")
- Frontend: WalmartScorecard.jsx — full rewrite
  - Period view selector: All Periods, Recent, Quarterly
  - Vendor tab selector: All Vendors, V-77893, V-79010
  - 4 summary charts: POS Sales TY vs LY, Margin Metrics, Supply Chain $, Turns & Instock
  - Full scorecard table with metric group headers, sticky metric column, color-coded diffs
  - Vendor Comparison table: Year-to-date side-by-side for key metrics across all vendors
  - Period ranges shown from data (e.g. Q1 = 202501-202513)

#### Sales Chart Improvements (March 16, 2026): COMPLETE ✓
- Returns chart: Math.abs() on returnsAmtTy/Ly so bars go upward (were negative from source)
- Sales/Units chart: Lines renamed "LY Revenue"/"LY Units" (was "Total Sales"), thicker (3px),
  brighter colors (#f59e0b amber, #ef4444 red, #3b82f6 blue), TY bars semi-transparent
- Clearance/Regular lines: renamed "Clearance TY"/"Regular TY", thicker (2.5px)

#### Scorecard Charts & Formatting (March 16, 2026): COMPLETE ✓
- Replaced 4 bar charts with: 4 quarterly pie/doughnut charts (POS $, POS Units, Gross Margin $, Return $)
  + bar graph: Maintained Margin vs Gross Margin by quarter
  + bar graph: Replenishment In Stock % TY vs LY by quarter
- Fixed metric formatting: Store Returns as dollar (not %), Unit Turns/Weeks of Supply as 1-decimal numbers,
  Retail Turns as number (not %). New getMetricFormat() with NUMBER_1D_METRICS and DOLLAR_OVERRIDE_METRICS arrays.
- Added vendor comparison hide/show toggle button (showComparison state)
- ChartCanvas: Added options prop with deep merge for plugins, scales, and top-level overrides (indexAxis)

#### eCommerce & Inventory Fixes (March 16, 2026): COMPLETE ✓
- eCommerce Top Items chart: horizontal bars (indexAxis: "y"), labels up to 40 chars, increased height (320px)
- Inventory Weekly Instock chart: bars now show dept instock % (left axis, 0-100% scale),
  colored lines show per-item instock % by week. OH Breakdown doughnut chart deleted.
- Backend: /api/walmart/weekly-trend now returns instockPct per week, itemInstock map (per-item per-week), weekOrder

#### Store Analytics Map Overhaul (March 16, 2026): COMPLETE ✓
- WM_DATA expanded to 46 states with correct FIPS codes + returns field
- CITY_DATA expanded to 100 cities with lat/lng and returns field
- 20 METRO_AREAS with center coordinates and radius (DFW, Houston, Atlanta, Phoenix, LA, etc.)
- Smooth D3 gradient heat map (scaleLinear, dark blue → light blue → red) instead of discrete tiers
- Metric toggle: Sales $ / Sales U / Return $ — updates heat map colors for state, city, and metro views
- City name labels shown when Cities checkbox is ON
- Metro outlines (dashed purple circles) when Metro checkbox is ON
- REGIONS now computed from WM_DATA, STATE_TO_REGION auto-built from REGIONS.states arrays

Remaining: First Tee, Belk, Hobby Lobby, Albertsons, Family Dollar
ingestion (same table schemas, different channel value). Google Drive watched folder. Email ingestion.

### Phase 5 — Multi-Platform APIs: PLANNED
Walmart Marketplace API, Shopify API, cross-platform views.

### Phase 6 — Intelligence + Forecasting: PLANNED
Custom date ranges, daily email summary, anomaly alerts, forecast tab, exec dashboard.

### Shipment Marketplace Fix (March 15, 2026): COMPLETE ✓
- Fixed: Canadian shipments (e.g. FBA194ZKBQXB → YYZ9) were incorrectly appearing in US view
- Root causes fixed in inventory.py:
  - create_shipment_plan(): Now reads marketplace from request body, passes to FulfillmentInbound + MarketplaceId
  - confirm_shipment_plan(): Same marketplace detection + cache refresh with correct marketplace param
  - _fetch_fba_shipments_from_api(): Added FC-based marketplace detection (Canadian FCs start with "Y" prefix: YYZ9, YOW1, YVR3, YHM1)
  - Post-fetch filtering ensures only matching-marketplace shipments go into each cache file
- Marketplace IDs: US = ATVPDKIKX0DER, CA = A2EUQ1WTGCTBG2
- Cache files: fba_shipments_cache.json (US), fba_shipments_cache_ca.json (CA)

### Known Issues / Future Work
- Stranded & Suppressed tab (/stranded) — links exist but no dedicated page built yet
- Inventory Ledger tab (/inventory-ledger) — links exist but no dedicated page built yet
- FC Distribution data is estimated (percentages), not from per-FC SP-API data
- Return Rate uses financial_events refund count as proxy for actual returns
- Storage Fee Forecast uses base LTSF × seasonal multiplier (estimated)
- KPI deltas compare 7d vs 30d velocity — not true YoY comparison
- Daily/weekly unit sales by day may still have data gaps
- SP-API push_sale_price() function not yet implemented in services/sp_api.py — graceful fallback returns error message
- Fee Detail sub-items (Pick & Pack, Weight Handling, etc.) are estimated percentages, not from actual API breakdowns
- Storage fee estimate in fee-detail uses fba_inventory.estimated_monthly_storage_fee scaled by days/30
- COGS data requires manual CSV upload to COGS_PATH — no UI for COGS management yet

## Backup System
- Google Drive: every 6 hours (2am, 8am, 2pm, 8pm Central), 30-day retention
  Manual trigger: POST /api/backup/trigger
  List backups: GET /api/backup/status
- GitHub: every 6 hours (runs after Drive backup)
  Repo: github.com/efine2010-Golfgen/golfgen-backups
  Manual trigger: POST /api/backup/github-trigger
- Verification: weekly Sunday 4am CT (auto), or POST /api/backup/verify (manual)
  Status: GET /api/backup/verification-status
- DR readiness: GET /api/backup/dr-status (backup age, verification, DB health, DuckDB fallback)
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