# GolfGen Amazon Dashboard — Setup Guide

## What You're Getting

A complete Amazon analytics dashboard that pulls live data from your Seller Central account via API and displays it in a professional, interactive HTML dashboard. The system refreshes hourly and tracks sales, advertising, inventory, and profitability across all your PGA Tour Golf products.

---

## Prerequisites

1. **Python 3.8+** — You likely already have this with VS Code installed
2. **VS Code** — Your code editor (already installed)
3. **Amazon Seller Central Account** — Active seller account with API access
4. **AWS Account** — Free tier is sufficient (needed for SP-API authentication)

---

## Step 1: Install Python Dependencies

Open a terminal in VS Code (Terminal > New Terminal) and run:

```bash
pip install python-amazon-sp-api duckdb requests
```

---

## Step 2: Get Your Amazon SP-API Credentials

This is the most involved step, but you only do it once.

### 2a. Register as a Developer in Seller Central

1. Go to **Seller Central** > **Apps & Services** > **Develop Apps**
2. Click **Register** to register yourself as a developer
3. Fill in the form (use your company name, GolfGen LLC)
4. Amazon will approve this within 24-48 hours

### 2b. Create an AWS IAM User

1. Go to **AWS Console** > **IAM** > **Users** > **Add Users**
2. Create a user named `golfgen-sp-api`
3. Attach the policy: **Allow all SP-API actions** (or create a custom policy)
4. Save the **Access Key ID** and **Secret Access Key**

### 2c. Create an IAM Role

1. In AWS IAM > **Roles** > **Create Role**
2. Select **Another AWS account** and enter your own account ID
3. Name it: `golfgen-sp-api-role`
4. Save the **Role ARN** (looks like: `arn:aws:iam::123456789:role/golfgen-sp-api-role`)

### 2d. Create the App in Seller Central

1. Back in **Seller Central** > **Develop Apps** > **Add new app client**
2. Select **SP API** as the API type
3. For IAM ARN, enter the Role ARN from step 2c
4. Select these roles:
   - **Selling Partner Insights** (for sales data)
   - **Finance and Accounting** (for financial events)
   - **Inventory and Order Management** (for orders & inventory)
   - **Direct-to-Consumer Shipping** (for FBA data)
5. After creation, you'll get:
   - **LWA Client ID** (starts with `amzn1.application-oa2-client.`)
   - **LWA Client Secret**
6. Click **Authorize** to self-authorize and get the **Refresh Token**

### 2e. Amazon Ads API (Optional, for PPC data)

1. Go to **advertising.amazon.com/developer**
2. Register for API access
3. Create an app and get Client ID, Client Secret
4. Authorize and get a Refresh Token
5. Find your Profile ID by calling the Profiles endpoint

---

## Step 3: Configure Credentials

1. Copy the template file:
   ```bash
   cp config/credentials_template.json config/credentials.json
   ```

2. Open `config/credentials.json` in VS Code and fill in all the values from Step 2

**IMPORTANT:** Never commit `credentials.json` to git or share it. The `.gitignore` file already excludes it.

---

## Step 4: Run Your First Data Pull

```bash
# From the golfgen_amazon_dashboard folder:

# Test with demo data first (no credentials needed):
python scripts/generate_dashboard.py --demo

# When credentials are configured, do a full pull:
python scripts/amazon_sp_api.py 30    # Pull last 30 days

# Generate the dashboard from live data:
python scripts/generate_dashboard.py
```

Open `amazon_dashboard.html` in your browser to see the dashboard.

---

## Step 5: Set Up Hourly Auto-Refresh

### Windows (Task Scheduler)

1. Open **Task Scheduler** (search in Start menu)
2. Click **Create Basic Task**
3. Name: `GolfGen Amazon Pull`
4. Trigger: **Daily**, start at 6:00 AM
5. Action: **Start a program**
   - Program: `python`
   - Arguments: `scripts/scheduler.py`
   - Start in: `C:\path\to\golfgen_amazon_dashboard`
6. Check **Repeat task every 1 hour** for a duration of 18 hours

### Mac (cron)

```bash
# Open crontab
crontab -e

# Add this line (runs every hour from 6am to midnight):
0 6-23 * * * cd /path/to/golfgen_amazon_dashboard && python3 scripts/scheduler.py
```

---

## Folder Structure

```
golfgen_amazon_dashboard/
├── amazon_dashboard.html      ← Open this in your browser
├── SETUP_GUIDE.md             ← You are here
├── config/
│   ├── credentials_template.json  ← Template (safe to share)
│   └── credentials.json          ← YOUR credentials (DO NOT SHARE)
├── data/
│   └── golfgen_amazon.duckdb     ← Local database (created on first run)
├── scripts/
│   ├── amazon_sp_api.py          ← SP-API data pull (sales, orders, inventory, finance)
│   ├── generate_dashboard.py     ← Dashboard HTML generator
│   └── scheduler.py              ← Automated hourly runner
└── templates/                    ← (reserved for future Tableau templates)
```

---

## Dashboard Features

- **6 KPI Cards:** Revenue, Units, Net Profit, Margin, TACOS, AOV with period-over-period trends
- **Daily Revenue Chart:** 90-day trend line for revenue and net profit
- **Monthly Revenue vs Profit:** Bar chart comparing revenue to net profit by month
- **Ad Performance:** Monthly ad spend vs ad-attributed sales
- **Profit Waterfall:** Visual breakdown of where revenue goes (COGS, fees, ads, profit)
- **Product Performance Table:** Sortable table with per-product revenue, units, profit, margin
- **FBA Inventory Status:** Current stock levels with days-of-supply and restock alerts
- **Filters:** Time range (30/60/90/180/365 days) and product filter
- **Print-ready:** Clean print layout for executive reports

---

## Troubleshooting

**"Access Denied" from SP-API:**
- Check your IAM Role ARN matches what's in Seller Central
- Verify your Refresh Token hasn't expired (re-authorize if needed)
- Ensure all required API roles are selected in the app

**"No data" in dashboard:**
- Run `python scripts/amazon_sp_api.py 30` to pull 30 days of data
- Check the terminal output for error messages
- Verify credentials.json is properly formatted

**Dashboard won't open:**
- Right-click the HTML file > Open With > Chrome/Edge/Firefox
- If charts don't load, ensure you have internet (Chart.js loads from CDN)

---

## Next Steps (Future Phases)

1. **Connect Tableau:** Point Tableau Desktop at `data/golfgen_amazon.duckdb` for advanced analytics
2. **Add Walmart data:** Phase 2 will add Walmart Marketplace API + Scintilla imports
3. **Add Shopify:** Phase 2 will add Shopify API for DTC channel
4. **Financial integration:** Phase 3 will sync with your GolfGen Cash Flow Excel
5. **Invoice processing:** Phase 3 will add PDF invoice scanning from a drop folder
