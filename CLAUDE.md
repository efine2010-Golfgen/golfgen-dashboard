# GolfGen Dashboard — Project Reference for Claude

> **Purpose:** This file gives Claude full context on the GolfGen Dashboard project so Eric never has to re-explain it. Read this file at the start of any session involving this project.

---

## What Is This?

The GolfGen Dashboard is a live, production web application for GolfGen — a consumer products company selling golf accessories and housewares on Amazon and Walmart. The dashboard tracks sales, inventory, advertising, profitability, and warehouse operations across both channels.

**Live URL:** https://golfgen-dashboard-production-ce30.up.railway.app
**GitHub Repo:** https://github.com/efine2010-Golfgen/golfgen-dashboard
**Owner:** Eric Fine (efine2010@gmail.com)
**Login Password:** Golfgen2026

---

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Frontend | React (SPA) | 19.2 |
| Bundler | Vite | 7.3 |
| Routing | React Router | v7 |
| Charts | Recharts | 3.8 |
| Backend | FastAPI (Python) | 0.115 |
| Database | DuckDB (file-based) | 1.5 |
| Amazon Data | python-amazon-sp-api | ≥2.1 |
| Amazon Ads | python-amazon-ad-api | 0.4.2 |
| Hosting | Railway (Docker) | auto-deploy from GitHub main |
| Excel Upload | openpyxl | ≥3.1 |

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│  Railway (Docker container)                          │
│                                                      │
│  FastAPI (uvicorn :8000)                             │
│  ├── /api/* endpoints (REST)                         │
│  ├── SP-API background sync (orders, inventory)      │
│  ├── Amazon Ads sync                                 │
│  └── Static file serving (React SPA from /dist)      │
│                                                      │
│  Data layer:                                         │
│  ├── data/golfgen_amazon.duckdb  (sales, orders)     │
│  ├── data/item_master.csv                            │
│  ├── data/cogs.csv                                   │
│  ├── data/warehouse.csv                              │
│  ├── data/golf_inventory.json                        │
│  ├── data/housewares_inventory.json                  │
│  ├── data/walmart_item_master.json                   │
│  └── data/amazon_item_master.json                    │
└──────────────────────────────────────────────────────┘
```

**How it deploys:** Push to `main` branch on GitHub → Railway detects → builds Docker image → deploys automatically. No CI/CD config needed beyond the Dockerfile.

---

## File Structure

```
golfgen-dashboard/
├── Dockerfile                          # Single-container build
├── CLAUDE.md                           # THIS FILE - project context
├── data/
│   ├── golfgen_amazon.duckdb           # DuckDB database (SP-API data)
│   ├── item_master.csv                 # Amazon product catalog
│   ├── cogs.csv                        # Cost of goods sold
│   ├── warehouse.csv                   # Original warehouse data
│   ├── golf_inventory.json             # Golf 3PL warehouse inventory
│   ├── housewares_inventory.json       # Housewares 3PL warehouse inventory
│   ├── walmart_item_master.json        # Walmart items (14 SKUs)
│   └── amazon_item_master.json         # Amazon items (139 SKUs)
├── webapp/
│   ├── backend/
│   │   ├── main.py                     # ~3,980 lines — ALL backend logic
│   │   ├── requirements.txt            # Python dependencies
│   │   └── dist/                       # Pre-built frontend (git-tracked)
│   └── frontend/
│       ├── package.json
│       ├── vite.config.js              # Dev proxy /api → localhost:8000
│       └── src/
│           ├── App.jsx                 # Auth gate + routing + header/nav
│           ├── App.css                 # All styles (light theme)
│           ├── lib/api.js              # API client (all fetch calls)
│           └── pages/
│               ├── Dashboard.jsx       # Sales overview + charts
│               ├── Products.jsx        # Product-level analytics
│               ├── Profitability.jsx   # P&L waterfall
│               ├── Inventory.jsx       # FBA inventory + 3PL summary
│               ├── Advertising.jsx     # Amazon Ads analytics
│               ├── Warehouse.jsx       # Original warehouse (grouped)
│               ├── GolfWarehouse.jsx   # Golf 3PL with channel filter
│               ├── HousewaresWarehouse.jsx  # Housewares 3PL
│               ├── ItemMaster.jsx      # Amazon/Walmart/Other tabs
│               └── Login.jsx           # Password login page
```

---

## API Endpoints (Complete List)

### Authentication
- `POST /api/auth/login` — password login, sets httponly cookie
- `GET /api/auth/check` — validate session
- `POST /api/auth/logout` — clear session

### Sales & Analytics
- `GET /api/summary?days=365` — KPI summary
- `GET /api/daily?days=365&granularity=daily|weekly|monthly` — time series
- `GET /api/products?days=365` — product breakdown
- `GET /api/product/{asin}?days=365` — single product detail
- `GET /api/comparison` — period-over-period comparison
- `GET /api/monthly-yoy` — month-over-year
- `GET /api/product-mix` — product mix analysis
- `GET /api/color-mix` — color breakdown

### Profitability
- `GET /api/pnl?days=365` — P&L waterfall
- `GET /api/profitability` — profitability overview
- `GET /api/profitability/items` — item-level profitability

### Inventory
- `GET /api/inventory` — FBA inventory with days-of-supply

### Advertising
- `GET /api/ads/summary?days=30`
- `GET /api/ads/daily?days=30`
- `GET /api/ads/campaigns?days=30`
- `GET /api/ads/keywords?days=30`
- `GET /api/ads/search-terms?days=30`
- `GET /api/ads/negative-keywords`
- `GET /api/ads/profiles`
- `POST /api/ads/sync`

### Warehouse
- `GET /api/warehouse` — original grouped warehouse
- `GET /api/warehouse/golf?channel=all|amazon|walmart|both|other`
- `GET /api/warehouse/housewares`
- `GET /api/warehouse/summary` — Golf vs Housewares totals
- `POST /api/upload/warehouse-excel` — upload Excel file
- `POST /api/refresh-warehouse` — reload from CSV

### Item Master
- `GET /api/item-master` — Amazon items (editable)
- `PUT /api/item-master/{asin}` — update single item field
- `POST /api/item-master/bulk-update` — bulk update items
- `GET /api/item-master/walmart`
- `GET /api/item-master/amazon`
- `GET /api/item-master/other`

### System
- `GET /api/health`
- `POST /api/sync` — trigger SP-API sync
- `GET /api/backfill` — backfill historical data
- `GET /api/debug/today-orders`
- `GET /api/debug/financial-events`

---

## Design System

**Theme:** Light background with dark navy header
**Fonts:** DM Serif Display (logo), Sora (body), Space Grotesk (numbers/headings)
**Key Colors:**
- Teal: #2ECFAA (brand accent)
- Navy: #0E1F2D (header background)
- Blue: #3E658C (active nav, buttons)
- Orange: #E87830 (secondary accent)
- Gold: #F5B731 (highlights)

**Layout:** Sticky header with gradient → gradient accent bar → white nav bar with tabs → content area (max-width 1400px)

---

## Authentication

Session-based with httponly cookies. Password is hardcoded as `Golfgen2026` in main.py (`DASHBOARD_PASSWORD` constant). Sessions stored in-memory (`_sessions` set). All frontend fetch calls include `credentials: "include"` for cookie transport.

---

## Channel Classification Logic (Golf Warehouse)

SKUs are classified as Amazon, Walmart, or Other:
1. Strip `T-` prefix from SKU (transfer items)
2. Strip suffixes: `-RB`, `-DONATE`, `-RETD`, `-FBM`, `-HOLD`, `-Damage`, `-CUST`, `-Transfer`
3. Match base SKU against Walmart master set (14 items) and Amazon master set (36 items)
4. If matches both → "Walmart & Amazon"; one → that channel; neither → "Other"

**Suffix types displayed:** Standard, RB (Rebate), DONATE, RETD (Returned), Transfer (T-prefix), FBM, HOLD, Damage, CUST (Customer)

---

## How to Deploy Changes

1. Make code changes in the repo
2. If frontend was changed: `cd webapp/frontend && npm install && npx vite build --outDir ../../temp-build --emptyOutDir`
3. Copy build output: replace `webapp/backend/dist/` with the build output
4. Commit: `git add . && git commit -m "description"`
5. Push: `git push origin main`
6. Railway auto-deploys in ~2-3 minutes

---

## How to Run Locally

```bash
# Backend
cd webapp/backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

# Frontend (separate terminal)
cd webapp/frontend
npm install
npm run dev    # starts on :3000, proxies /api to :8000
```

Note: SP-API sync requires environment variables (`SP_API_REFRESH_TOKEN`, `LWA_APP_ID`, `LWA_CLIENT_SECRET`, `MARKETPLACE_ID`, `SELLER_ID`). Without these, the dashboard works but won't pull fresh Amazon data.

---

## Railway Environment Variables

Set these in Railway dashboard (Settings → Variables):
- `SP_API_REFRESH_TOKEN`
- `LWA_APP_ID`
- `LWA_CLIENT_SECRET`
- `MARKETPLACE_ID` (default: ATVPDKIKX0DER for US)
- `SELLER_ID`
- `ADS_REFRESH_TOKEN` (for Amazon Ads)
- `ADS_CLIENT_ID`
- `ADS_CLIENT_SECRET`
- `ADS_PROFILE_ID`
- `DB_DIR` (default: /app/data)
- `PORT` (Railway sets automatically)

---

## Common Tasks for Claude

### "Update the dashboard with new features"
→ Edit files in this repo, rebuild frontend, commit and push to GitHub

### "Fix a bug on the live site"
→ Read main.py or the relevant .jsx file, make the fix, rebuild + push

### "Add new data/inventory"
→ Update the JSON files in data/, add COPY line to Dockerfile if new files, rebuild + push

### "Change the password"
→ Edit `DASHBOARD_PASSWORD` in main.py (search for `Golfgen2026`)

### "The site is down"
→ Check Railway dashboard for build/deploy errors. Check /api/health endpoint. Common issues: Dockerfile COPY fails for missing files, Python import errors in main.py, DuckDB lock issues.

---

## Important Notes

- **main.py is ~3,980 lines** — be careful with edits, don't overwrite unrelated sections
- **Frontend dist is git-tracked** in `webapp/backend/dist/` — this is what the Docker image serves
- **DuckDB file** (`golfgen_amazon.duckdb`) contains all historical sales data — do NOT delete
- **All fetch calls** must include `credentials: "include"` for auth cookies to work
- **React Router v7** — uses `<BrowserRouter>`, `<Routes>`, `<Route>`, `<NavLink>`
- **No separate CSS files per component** — everything is in App.css
