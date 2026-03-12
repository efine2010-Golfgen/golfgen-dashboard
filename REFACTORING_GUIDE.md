# GolfGen Dashboard Backend Refactoring Guide

## Overview
The original 7335-line `main.py` has been refactored into a modular structure following the target architecture. This guide documents the refactoring strategy and the current state.

## Completed Modules

### 1. `core/config.py` ✅
- All paths (DB_PATH, COGS_PATH, CONFIG_PATH, DOCS_DIR, PRICING_CACHE_PATH)
- All constants (SYNC_INTERVAL_HOURS, DASHBOARD_PASSWORD, TIMEZONE)
- User credentials dictionary (USERS)
- Tab configuration (ALL_TABS, TAB_API_PREFIXES)
- EMAIL_TO_USER lookup dictionary
- Financial year configuration (ACTUALIZED_THROUGH_MONTH/YEAR)

### 2. `core/database.py` ✅
- `get_db()` - read-only connection
- `get_db_rw()` - read-write connection
- All `CREATE SEQUENCE` statements (sync_log_seq, docs_update_log_seq)
- All `CREATE TABLE` statements organized by category:
  - Auth tables: `sessions`, `user_permissions`
  - System tables: `sync_log`, `docs_update_log`
  - Sales tables: `orders`, `daily_sales`, `fba_inventory`, `financial_events`
  - Advertising tables: `advertising`, `ads_campaigns`, `ads_keywords`, `ads_search_terms`, `ads_negative_keywords`
  - Item Plan tables: `monthly_sales_history`, `item_plan_overrides`, `item_plan_curve_selection`, `item_plan_factory_orders`, `item_plan_factory_order_items`, `item_plan_settings`
- `_init_auth_tables()` - create auth tables
- `_init_system_tables()` - create system tables (with sequence handling)
- `_init_sales_tables()` - create sales/inventory tables
- `_init_advertising_tables()` - create ads tables
- `_init_item_plan_tables()` - create item plan tables with seed data loading
- `init_all_tables()` - unified initialization function

### 3. `core/auth.py` ✅
**Session Management:**
- `get_session(token)` - lookup session from DuckDB
- `find_user_by_email(email)` - case-insensitive user lookup
- `get_user_permissions(user_name)` - get enabled tabs for user
- `require_auth(request)` - validate session or raise 401
- `require_tab_access(request, tab_key)` - validate session + tab permission
- `tab_key_for_path(path)` - map API path to tab_key

**Pydantic Models:**
- `MultiLoginRequest` - {email, password}
- `PermissionUpdate` - {user, tab, enabled}

**Handler Functions:**
- `login_handler(req)` - authenticate + create session + set cookie
- `logout_handler(golfgen_session)` - delete session + clear cookie
- `get_me_handler(request)` - return current user info
- `get_my_permissions_handler(request)` - return user's accessible tabs
- `get_all_permissions_handler(request)` - admin: get full permissions grid
- `update_permission_handler(req, request)` - admin: update user tab access

## Remaining Work

### 4. `core/scheduler.py` (PENDING)
Should contain:
- Global `scheduler` variable initialization
- `_run_scheduled_sp_api_sync()` - wrapper with logging
- `_run_scheduled_today_sync()` - wrapper with logging
- `_run_scheduled_ads_sync()` - wrapper with logging
- `_run_scheduled_pricing_sync()` - wrapper with logging
- `_run_scheduled_docs_update()` - placeholder for Prompt 2
- `_run_duckdb_backup()` - placeholder for Prompt 2
- `async _sync_loop()` - initialize APScheduler + startup jobs
- `async _startup_sync_catchup()` - catch up missed scheduled times
- `async lifespan(app)` - FastAPI lifespan context manager

**Dependencies:** Needs to import from:
- `services.sp_api._run_sp_api_sync`, `_sync_today_orders`
- `services.ads_api._sync_ads_data`
- `services.sync_engine._sync_pricing_and_coupons`, `_auto_backfill_if_needed`
- `core.database._log_sync`, `_log_docs_update`

### 5. `services/sp_api.py` (PENDING)
Should contain all SP-API integration:
- `_load_sp_api_credentials()` - load from env vars or config file
- `_sync_today_orders()` - fast real-time orders sync
- `_ensure_today_data()` - inline check + fast sync if missing
- `_run_sp_api_sync()` - full sync including financial events, FBA inventory, sales report
- Helper functions for financial event processing:
  - `_money()` - parse money values from sp_api objects
  - `_posted_date_str()` - extract date from PostedDate
  - `_safe_get()` - dict-like access for sp_api model objects

**Implementation Details:**
- Uses `datetime.utcnow()` for UTC-based API calls (correct per spec)
- Uses `ZoneInfo("America/Chicago")` for business logic (correct per spec)
- Rate limiting with adaptive sleep (burst 25, then 2sec sleep)
- Pagination handling for Orders API and Finances API
- Stores order + item-level prices in DuckDB

### 6. `services/ads_api.py` (PENDING)
Should contain Amazon Ads API integration:
- `_load_ads_credentials()` - load from env vars or config file
- `_sync_ads_data()` - full ads sync
- `_pull_ads_report()` - generic report puller with pagination
- `_handle_campaign_report(data)` - insert campaign data into ads_campaigns + advertising
- `_handle_targeting_report(data)` - insert keyword/ad group data into ads_keywords
- `_handle_search_term_report(data)` - insert search terms into ads_search_terms
- `_sync_pricing_data()` - pull current prices from SP-API (20 ASIN batches)
- `_sync_coupon_data()` - pull coupons from Amazon Ads API
- `_load_pricing_cache()` / `_save_pricing_cache()` - cache pricing/coupon data

### 7. `services/sync_engine.py` (PENDING)
Should contain sync orchestration:
- `_auto_backfill_if_needed()` - detect fresh deploy + backfill historical data
- `_sync_pricing_and_coupons()` - orchestrate pricing + coupon sync
- Sync status/log tracking utilities

### 8. `routers/sales.py` (PENDING)
Should contain sales analytics endpoints:
- `@app.get("/api/summary")` - KPI summary
- `@app.get("/api/daily")` - time series sales data
- `@app.get("/api/products")` - product breakdown
- `@app.get("/api/product/{asin}")` - single product detail
- `@app.get("/api/comparison")` - period-over-period comparison
- `@app.get("/api/monthly-yoy")` - month-over-year comparison
- `@app.get("/api/product-mix")` - product mix analysis
- `@app.get("/api/color-mix")` - color breakdown
- Helper: `_build_product_list(con, cutoff)` - product data aggregation
- Helper: `_aggregate_weekly(daily_data)` - weekly aggregation

### 9. `routers/profitability.py` (PENDING)
Should contain profitability endpoints:
- `@app.get("/api/pnl")` - P&L waterfall
- `@app.get("/api/profitability")` - profitability overview
- `@app.get("/api/profitability/items")` - item-level profitability
- Helper: `_build_waterfall(con, cogs_data, start, end)` - waterfall aggregation

### 10. `routers/inventory.py` (PENDING)
Should contain warehouse & inventory endpoints:
- `@app.get("/api/inventory")` - FBA inventory with days-of-supply
- `@app.get("/api/warehouse")` - original grouped warehouse
- `@app.get("/api/warehouse/golf")` - golf channel inventory
- `@app.get("/api/warehouse/housewares")` - housewares inventory
- `@app.get("/api/warehouse/unified")` - unified inventory
- `@app.get("/api/warehouse/summary")` - golf vs housewares totals
- `@app.post("/api/upload/warehouse-excel")` - Excel upload
- `@app.post("/api/refresh-warehouse")` - reload from CSV
- Helpers:
  - `_classify_golf_channel(item_number, walmart_skus, amazon_skus)` - channel classification
  - `_load_upload_meta()` / `_save_upload_meta()` - upload metadata persistence

### 11. `routers/advertising.py` (PENDING)
Should contain Amazon Ads analytics endpoints:
- `@app.get("/api/ads/summary")` - ads KPI summary
- `@app.get("/api/ads/daily")` - daily ads metrics
- `@app.get("/api/ads/campaigns")` - campaign breakdown
- `@app.get("/api/ads/keywords")` - keyword performance
- `@app.get("/api/ads/search-terms")` - search term analysis
- `@app.get("/api/ads/negative-keywords")` - negative keyword list
- `@app.get("/api/ads/profiles")` - ads profiles
- `@app.post("/api/ads/sync")` - manual ads sync trigger
- Helper: `_safe_ads_query()` - safe DuckDB query execution for ads tables

### 12. `routers/item_master.py` (PENDING)
Should contain item management endpoints:
- `@app.get("/api/item-master")` - unified item list with live pricing
- `@app.put("/api/item-master/{asin}")` - update single item field
- `@app.post("/api/item-master/bulk-update")` - bulk update items
- `@app.get("/api/item-master/amazon")` - Amazon items (139 SKUs)
- `@app.get("/api/item-master/walmart")` - Walmart items (14 SKUs)
- `@app.put("/api/item-master/walmart/{golfgen_item}")` - update Walmart item
- `@app.get("/api/item-master/housewares")` - housewares items
- `@app.get("/api/item-master/other")` - items not in Amazon/Walmart
- `@app.get("/api/pricing/status")` - pricing cache status
- `@app.post("/api/pricing/sync")` - manual pricing sync
- Helpers:
  - `load_item_master()` - load from item_master.csv
  - `save_item_master()` - save to item_master.csv

### 13. `routers/factory_po.py` (PENDING)
Should contain factory PO endpoints:
- `@app.get("/api/factory-po")` - list factory POs
- `@app.post("/api/factory-po/upload")` - upload PO Excel file
- Helpers:
  - `_load_factory_po()` - load from JSON
  - `_save_factory_po()` - save to JSON

### 14. `routers/otw.py` (Logistics) (PENDING)
Should contain OTW/logistics endpoints:
- `@app.get("/api/logistics")` - logistics/OTW status
- `@app.post("/api/logistics/upload")` - upload logistics file
- `@app.post("/api/supply-chain/upload")` - supply chain data upload
- Helpers:
  - `_load_logistics()` - load from JSON
  - `_save_logistics()` - save to JSON

### 15. `routers/item_plan.py` (PENDING)
Should contain item planning endpoints:
- `@app.get("/api/item-plan")` - item plan data + sales curves
- `@app.post("/api/item-plan/override")` - set override values
- `@app.post("/api/item-plan/curve")` - select curve type
- `@app.get("/api/item-plan/sales-curves")` - get computed curves
- `@app.get("/api/factory-on-order")` - factory on-order data
- `@app.post("/api/factory-on-order")` - update factory orders
- `@app.get("/api/dashboard-settings")` - get settings
- `@app.post("/api/dashboard-settings")` - save settings
- Helpers:
  - `_compute_master_curve()` - compute master sales curve
  - `_db_month_to_key()` - DB month number → frontend key
  - `_array_to_month_obj()` - array → month-keyed object
  - Various month/FY conversion utilities

### 16. `routers/fba_shipments.py` (PENDING)
Should contain FBA shipments endpoints:
- `@app.get("/api/fba-shipments")` - list shipments
- `@app.post("/api/fba-shipments/sync")` - sync from SP-API
- `@app.get("/api/fba-shipments/{shipment_id}/items")` - shipment items
- Helpers:
  - `_fetch_fba_shipments_from_api()` - fetch from SP-API
  - `_enrich_shipment_items()` - enrich with ASIN/product details

### 17. `routers/permissions.py` (PENDING)
Should contain permission management endpoints:
- `@app.post("/api/auth/login")` - user login
- `@app.get("/api/auth/check")` - check session status
- `@app.post("/api/auth/logout")` - user logout
- `@app.get("/api/me")` - current user info
- `@app.get("/api/permissions/me")` - user's accessible tabs
- `@app.get("/api/permissions")` - admin: full permissions grid
- `@app.post("/api/permissions")` - admin: update user tab access

These are just wrappers calling functions from `core/auth.py`.

### 18. `routers/system.py` (PENDING)
Should contain system endpoints:
- `@app.get("/api/health")` - health check
- `@app.post("/api/sync")` - manual SP-API sync (POST)
- `@app.get("/api/sync")` - manual SP-API sync (GET, fast)
- `@app.get("/api/backfill")` - trigger historical backfill
- `@app.get("/api/debug/today-orders")` - debug today's orders
- `@app.get("/api/debug/financial-events")` - debug financial events
- `@app.get("/api/system/status")` - system status + sync log
- `@app.get("/api/system/sync-log")` - recent sync logs
- `@app.post("/api/backup/trigger")` - manual backup (Prompt 2)
- `@app.get("/api/backup/status")` - backup status (Prompt 2)
- `@app.post("/api/docs/update")` - manual docs update (Prompt 2)
- `@app.get("/api/docs/status")` - docs update status (Prompt 2)
- `@app.get("/api/docs/architecture")` - architecture doc (Prompt 2)
- `@app.get("/api/docs/disaster-recovery")` - disaster recovery doc (Prompt 2)

### 19. `main.py` (PENDING - Slim New Version)
Should contain:
- Import all modules and routers
- FastAPI app initialization
- CORS middleware configuration
- Tab permission middleware
- Lifespan context manager (calls `_sync_loop` from `core/scheduler`)
- Static file mounting + SPA fallback
- Include all routers with `/api` prefix

## Critical Refactoring Rules (PRESERVED)

1. **DuckDB Connection Pattern:**
   - `get_db()` returns read-only connection
   - `get_db_rw()` returns read-write connection
   - Every module imports these and calls when needed
   - NO passing connections between modules

2. **Timezone Handling (CRITICAL):**
   - ALL `datetime.now()` for business logic uses `ZoneInfo("America/Chicago")`
   - SP-API calls use `datetime.utcnow()` (correct - UTC-based APIs)
   - Financial events use `ZoneInfo("UTC")` for API → Chicago conversion

3. **CREATE SEQUENCE/TABLE Order:**
   - Sequences created FIRST in database.py
   - Tables created SECOND (some reference sequences in DEFAULT)

4. **Zero Logic Changes:**
   - Every SQL query copied exactly
   - Every endpoint response shape preserved
   - Every business rule maintained
   - Every variable name kept the same

5. **Imports Per Module:**
   - Each module imports only what it needs
   - `logger = logging.getLogger("golfgen")` in each module that logs
   - No circular imports

## Migration Checklist

- [x] `core/config.py` - All constants and paths
- [x] `core/database.py` - All table creation and initialization
- [x] `core/auth.py` - Session + permission management
- [ ] `core/scheduler.py` - APScheduler setup + job wrappers
- [ ] `services/sp_api.py` - SP-API integration
- [ ] `services/ads_api.py` - Amazon Ads integration + pricing
- [ ] `services/sync_engine.py` - Sync orchestration
- [ ] `routers/sales.py` - Sales analytics (11 lines → router)
- [ ] `routers/profitability.py` - P&L analysis
- [ ] `routers/inventory.py` - Warehouse + FBA inventory
- [ ] `routers/advertising.py` - Amazon Ads analytics
- [ ] `routers/item_master.py` - Item management + pricing/coupons
- [ ] `routers/factory_po.py` - Factory PO management
- [ ] `routers/otw.py` - Logistics / OTW management
- [ ] `routers/item_plan.py` - Item planning (FY curves)
- [ ] `routers/fba_shipments.py` - FBA shipment tracking
- [ ] `routers/permissions.py` - Auth endpoints (thin wrappers)
- [ ] `routers/system.py` - System/health endpoints
- [ ] `main.py` - New slim FastAPI app + imports all routers

## File Size Estimates

- `main.py` (new): ~100-150 lines (app creation + middleware + lifespan)
- `core/config.py`: ~120 lines ✅
- `core/database.py`: ~500 lines ✅
- `core/auth.py`: ~180 lines ✅
- `core/scheduler.py`: ~200 lines
- `services/sp_api.py`: ~1100 lines
- `services/ads_api.py`: ~800 lines
- `services/sync_engine.py`: ~200 lines
- `routers/sales.py`: ~600 lines
- `routers/profitability.py`: ~400 lines
- `routers/inventory.py`: ~1000 lines
- `routers/advertising.py`: ~600 lines
- `routers/item_master.py`: ~700 lines
- `routers/factory_po.py`: ~250 lines
- `routers/otw.py`: ~400 lines
- `routers/item_plan.py`: ~1200 lines
- `routers/fba_shipments.py`: ~500 lines
- `routers/permissions.py`: ~50 lines (thin wrappers)
- `routers/system.py`: ~600 lines

**Total:** ~9,000+ lines (vs 7,335 original - slight increase due to import statements, docstrings, and module organization overhead is normal and acceptable)

## Next Steps

1. Complete `core/scheduler.py` - highest priority (required for app startup)
2. Complete `services/*.py` - required by scheduler + routers
3. Complete `routers/*.py` - in order of dependency
4. Create new `main.py` that ties everything together
5. Test imports and ensure all connections work
6. Run `pytest` if test suite exists
7. Deploy to Railway and verify logs
