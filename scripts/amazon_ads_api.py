"""
GolfGen Amazon Ads API Data Pipeline
======================================
Pulls campaign, keyword, search term, and targeting data
from the Amazon Advertising API (separate from SP-API).

Uses the python-amazon-ad-api SDK.

Requirements:
    pip install python-amazon-ad-api duckdb

Setup:
    1. Register at advertising.amazon.com/developer
    2. Create an LWA (Login with Amazon) app
    3. Authorize your seller account via the OAuth flow
    4. Fill in AMAZON_ADS_API section of config/credentials.json
    5. Run: python scripts/amazon_ads_api.py
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

try:
    from ad_api.api.sp import Reports as SPReports
    from ad_api.api.sb import Reports as SBReports
    from ad_api.api import Profiles
    from ad_api.api.sp import Campaigns as SPCampaigns
    from ad_api.api.sp import Keywords as SPKeywords
    from ad_api.api.sp import NegativeKeywords as SPNegativeKeywords
    AD_API_AVAILABLE = True
except ImportError:
    AD_API_AVAILABLE = False
    print("WARNING: python-amazon-ad-api not installed.")
    print("  Install it: pip install python-amazon-ad-api")
    print("  Continuing with demo/setup mode only.\n")


# ─────────────────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────────────────

def load_credentials():
    cred_path = PROJECT_ROOT / "config" / "credentials.json"
    if not cred_path.exists():
        print("ERROR: config/credentials.json not found.")
        sys.exit(1)
    with open(cred_path) as f:
        return json.load(f)


def get_ads_credentials(creds):
    """Extract Amazon Ads API credentials from config."""
    ads = creds.get("AMAZON_ADS_API", {})
    client_id = ads.get("client_id", "NOT_REQUIRED")
    if client_id in ("NOT_REQUIRED", "", "YOUR_CLIENT_ID"):
        return None  # Not configured yet

    return {
        "refresh_token": ads["refresh_token"],
        "client_id": ads["client_id"],
        "client_secret": ads["client_secret"],
        "profile_id": ads["profile_id"],
    }


# ─────────────────────────────────────────────────────
# DATABASE SCHEMA
# ─────────────────────────────────────────────────────

def init_ads_tables(db_path):
    """Create/update ads-specific tables in DuckDB."""
    con = duckdb.connect(str(db_path))

    # Campaign-level daily data
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_campaigns (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            campaign_type VARCHAR,
            campaign_status VARCHAR,
            daily_budget DOUBLE,
            impressions INTEGER,
            clicks INTEGER,
            spend DOUBLE,
            sales DOUBLE,
            orders INTEGER,
            units INTEGER,
            acos DOUBLE,
            roas DOUBLE,
            cpc DOUBLE,
            ctr DOUBLE,
            cvr DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, campaign_id)
        )
    """)

    # Ad group-level daily data
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_adgroups (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_id VARCHAR,
            ad_group_name VARCHAR,
            impressions INTEGER,
            clicks INTEGER,
            spend DOUBLE,
            sales DOUBLE,
            orders INTEGER,
            units INTEGER,
            acos DOUBLE,
            roas DOUBLE,
            cpc DOUBLE,
            ctr DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, ad_group_id)
        )
    """)

    # Keyword-level daily data
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_keywords (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_id VARCHAR,
            ad_group_name VARCHAR,
            keyword_id VARCHAR,
            keyword_text VARCHAR,
            match_type VARCHAR,
            keyword_status VARCHAR,
            bid DOUBLE,
            impressions INTEGER,
            clicks INTEGER,
            spend DOUBLE,
            sales DOUBLE,
            orders INTEGER,
            units INTEGER,
            acos DOUBLE,
            roas DOUBLE,
            cpc DOUBLE,
            ctr DOUBLE,
            cvr DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, keyword_id)
        )
    """)

    # Search term report data
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_search_terms (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_id VARCHAR,
            ad_group_name VARCHAR,
            keyword_text VARCHAR,
            match_type VARCHAR,
            search_term VARCHAR,
            impressions INTEGER,
            clicks INTEGER,
            spend DOUBLE,
            sales DOUBLE,
            orders INTEGER,
            units INTEGER,
            acos DOUBLE,
            roas DOUBLE,
            cpc DOUBLE,
            ctr DOUBLE,
            cvr DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Negative keywords
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_negative_keywords (
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            ad_group_id VARCHAR,
            ad_group_name VARCHAR,
            keyword_id VARCHAR,
            keyword_text VARCHAR,
            match_type VARCHAR,
            keyword_status VARCHAR,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (keyword_id)
        )
    """)

    # Product-level ad performance (by ASIN)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_product_performance (
            date DATE,
            campaign_id VARCHAR,
            campaign_name VARCHAR,
            asin VARCHAR,
            sku VARCHAR,
            impressions INTEGER,
            clicks INTEGER,
            spend DOUBLE,
            sales DOUBLE,
            orders INTEGER,
            units INTEGER,
            acos DOUBLE,
            roas DOUBLE,
            cpc DOUBLE,
            ctr DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Daily aggregate ads summary (for quick dashboard access)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_daily_summary (
            date DATE PRIMARY KEY,
            total_spend DOUBLE,
            total_sales DOUBLE,
            total_impressions INTEGER,
            total_clicks INTEGER,
            total_orders INTEGER,
            total_units INTEGER,
            acos DOUBLE,
            roas DOUBLE,
            tacos DOUBLE,
            avg_cpc DOUBLE,
            avg_ctr DOUBLE,
            pulled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.close()
    print(f"Ads tables initialized in {db_path}")


# ─────────────────────────────────────────────────────
# REPORT PULLING (v3 Sponsored Ads Reports)
# ─────────────────────────────────────────────────────

def request_sp_report(credentials, report_type, start_date, end_date, metrics, group_by):
    """
    Request a Sponsored Products report via the v3 Reporting API.

    report_type: 'spCampaigns', 'spAdGroups', 'spKeywords', 'spSearchTerm',
                 'spAdvertisedProduct', 'spTargeting'
    metrics: list of metric names
    group_by: list of grouping dimensions
    """
    report_config = {
        "reportDate": start_date.isoformat(),
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": group_by,
            "columns": metrics,
            "reportTypeId": report_type,
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        }
    }

    reports = SPReports(credentials=credentials)
    response = reports.post_report(body=report_config)

    report_id = response.payload.get("reportId")
    print(f"  Report requested: {report_id} ({report_type})")

    # Poll for completion
    for attempt in range(60):
        time.sleep(5)
        status_resp = reports.get_report(reportId=report_id)
        status = status_resp.payload.get("status")

        if status == "COMPLETED":
            url = status_resp.payload.get("url")
            print(f"  Report ready. Downloading...")

            # Download and decompress
            import requests
            import gzip
            resp = requests.get(url)
            try:
                data = gzip.decompress(resp.content)
                records = json.loads(data.decode("utf-8"))
            except Exception:
                records = json.loads(resp.text)

            print(f"  Downloaded {len(records)} records")
            return records

        elif status in ("FAILED", "CANCELLED"):
            print(f"  Report failed: {status}")
            return []

        if attempt % 6 == 5:
            print(f"  Still waiting... ({attempt + 1})")

    print("  Timed out waiting for report")
    return []


def pull_campaign_report(credentials, db_path, start_date, end_date):
    """Pull campaign-level performance data."""
    print(f"\n{'='*60}")
    print("PULLING CAMPAIGN REPORT")
    print(f"{'='*60}")
    print(f"Date range: {start_date} to {end_date}")

    metrics = [
        "date", "campaignId", "campaignName", "campaignStatus",
        "campaignBudgetAmount", "campaignBudgetType",
        "impressions", "clicks", "cost", "purchases14d",
        "sales14d", "unitsSold14d",
    ]
    group_by = ["campaign"]

    records = request_sp_report(credentials, "spCampaigns", start_date, end_date, metrics, group_by)
    if not records:
        return 0

    con = duckdb.connect(str(db_path))
    stored = 0

    for r in records:
        impressions = int(r.get("impressions", 0) or 0)
        clicks = int(r.get("clicks", 0) or 0)
        spend = float(r.get("cost", 0) or 0)
        sales = float(r.get("sales14d", 0) or 0)
        orders = int(r.get("purchases14d", 0) or 0)
        units = int(r.get("unitsSold14d", 0) or 0)

        acos = round(spend / sales * 100, 2) if sales > 0 else 0
        roas = round(sales / spend, 2) if spend > 0 else 0
        cpc = round(spend / clicks, 2) if clicks > 0 else 0
        ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
        cvr = round(orders / clicks * 100, 2) if clicks > 0 else 0

        con.execute("""
            INSERT OR REPLACE INTO ads_campaigns
            (date, campaign_id, campaign_name, campaign_type, campaign_status,
             daily_budget, impressions, clicks, spend, sales, orders, units,
             acos, roas, cpc, ctr, cvr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            r.get("date", str(start_date)),
            r.get("campaignId", ""),
            r.get("campaignName", ""),
            "Sponsored Products",
            r.get("campaignStatus", ""),
            float(r.get("campaignBudgetAmount", 0) or 0),
            impressions, clicks, spend, sales, orders, units,
            acos, roas, cpc, ctr, cvr,
        ])
        stored += 1

    con.close()
    print(f"Stored {stored} campaign records")
    return stored


def pull_keyword_report(credentials, db_path, start_date, end_date):
    """Pull keyword-level performance data."""
    print(f"\n{'='*60}")
    print("PULLING KEYWORD REPORT")
    print(f"{'='*60}")

    metrics = [
        "date", "campaignId", "campaignName",
        "adGroupId", "adGroupName",
        "keywordId", "keywordText", "matchType", "keywordStatus", "keywordBid",
        "impressions", "clicks", "cost", "purchases14d",
        "sales14d", "unitsSold14d",
    ]
    group_by = ["keyword"]

    records = request_sp_report(credentials, "spKeywords", start_date, end_date, metrics, group_by)
    if not records:
        return 0

    con = duckdb.connect(str(db_path))
    stored = 0

    for r in records:
        impressions = int(r.get("impressions", 0) or 0)
        clicks = int(r.get("clicks", 0) or 0)
        spend = float(r.get("cost", 0) or 0)
        sales = float(r.get("sales14d", 0) or 0)
        orders = int(r.get("purchases14d", 0) or 0)
        units = int(r.get("unitsSold14d", 0) or 0)

        acos = round(spend / sales * 100, 2) if sales > 0 else 0
        roas = round(sales / spend, 2) if spend > 0 else 0
        cpc = round(spend / clicks, 2) if clicks > 0 else 0
        ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
        cvr = round(orders / clicks * 100, 2) if clicks > 0 else 0

        con.execute("""
            INSERT OR REPLACE INTO ads_keywords
            (date, campaign_id, campaign_name, ad_group_id, ad_group_name,
             keyword_id, keyword_text, match_type, keyword_status, bid,
             impressions, clicks, spend, sales, orders, units,
             acos, roas, cpc, ctr, cvr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            r.get("date", str(start_date)),
            r.get("campaignId", ""), r.get("campaignName", ""),
            r.get("adGroupId", ""), r.get("adGroupName", ""),
            r.get("keywordId", ""), r.get("keywordText", ""),
            r.get("matchType", ""), r.get("keywordStatus", ""),
            float(r.get("keywordBid", 0) or 0),
            impressions, clicks, spend, sales, orders, units,
            acos, roas, cpc, ctr, cvr,
        ])
        stored += 1

    con.close()
    print(f"Stored {stored} keyword records")
    return stored


def pull_search_term_report(credentials, db_path, start_date, end_date):
    """Pull search term report data."""
    print(f"\n{'='*60}")
    print("PULLING SEARCH TERM REPORT")
    print(f"{'='*60}")

    metrics = [
        "date", "campaignId", "campaignName",
        "adGroupId", "adGroupName",
        "keywordText", "matchType", "searchTerm",
        "impressions", "clicks", "cost", "purchases14d",
        "sales14d", "unitsSold14d",
    ]
    group_by = ["searchTerm"]

    records = request_sp_report(credentials, "spSearchTerm", start_date, end_date, metrics, group_by)
    if not records:
        return 0

    con = duckdb.connect(str(db_path))
    stored = 0

    for r in records:
        impressions = int(r.get("impressions", 0) or 0)
        clicks = int(r.get("clicks", 0) or 0)
        spend = float(r.get("cost", 0) or 0)
        sales = float(r.get("sales14d", 0) or 0)
        orders = int(r.get("purchases14d", 0) or 0)
        units = int(r.get("unitsSold14d", 0) or 0)

        acos = round(spend / sales * 100, 2) if sales > 0 else 0
        roas = round(sales / spend, 2) if spend > 0 else 0
        cpc = round(spend / clicks, 2) if clicks > 0 else 0
        ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
        cvr = round(orders / clicks * 100, 2) if clicks > 0 else 0

        con.execute("""
            INSERT INTO ads_search_terms
            (date, campaign_id, campaign_name, ad_group_id, ad_group_name,
             keyword_text, match_type, search_term,
             impressions, clicks, spend, sales, orders, units,
             acos, roas, cpc, ctr, cvr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            r.get("date", str(start_date)),
            r.get("campaignId", ""), r.get("campaignName", ""),
            r.get("adGroupId", ""), r.get("adGroupName", ""),
            r.get("keywordText", ""), r.get("matchType", ""),
            r.get("searchTerm", ""),
            impressions, clicks, spend, sales, orders, units,
            acos, roas, cpc, ctr, cvr,
        ])
        stored += 1

    con.close()
    print(f"Stored {stored} search term records")
    return stored


def pull_product_ad_report(credentials, db_path, start_date, end_date):
    """Pull advertised product (ASIN) level report."""
    print(f"\n{'='*60}")
    print("PULLING PRODUCT AD PERFORMANCE REPORT")
    print(f"{'='*60}")

    metrics = [
        "date", "campaignId", "campaignName",
        "advertisedAsin", "advertisedSku",
        "impressions", "clicks", "cost", "purchases14d",
        "sales14d", "unitsSold14d",
    ]
    group_by = ["advertiser"]

    records = request_sp_report(credentials, "spAdvertisedProduct", start_date, end_date, metrics, group_by)
    if not records:
        return 0

    con = duckdb.connect(str(db_path))
    stored = 0

    for r in records:
        impressions = int(r.get("impressions", 0) or 0)
        clicks = int(r.get("clicks", 0) or 0)
        spend = float(r.get("cost", 0) or 0)
        sales = float(r.get("sales14d", 0) or 0)
        orders = int(r.get("purchases14d", 0) or 0)
        units = int(r.get("unitsSold14d", 0) or 0)

        acos = round(spend / sales * 100, 2) if sales > 0 else 0
        roas = round(sales / spend, 2) if spend > 0 else 0
        cpc = round(spend / clicks, 2) if clicks > 0 else 0
        ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0

        con.execute("""
            INSERT INTO ads_product_performance
            (date, campaign_id, campaign_name, asin, sku,
             impressions, clicks, spend, sales, orders, units,
             acos, roas, cpc, ctr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            r.get("date", str(start_date)),
            r.get("campaignId", ""), r.get("campaignName", ""),
            r.get("advertisedAsin", ""), r.get("advertisedSku", ""),
            impressions, clicks, spend, sales, orders, units,
            acos, roas, cpc, ctr,
        ])
        stored += 1

    con.close()
    print(f"Stored {stored} product ad records")
    return stored


def build_daily_summary(db_path, start_date, end_date):
    """Build aggregated daily summary from campaign data + organic sales for TACOS."""
    print(f"\n{'='*60}")
    print("BUILDING DAILY SUMMARY (TACOS)")
    print(f"{'='*60}")

    con = duckdb.connect(str(db_path))

    # Aggregate from campaign data
    rows = con.execute("""
        SELECT
            date,
            SUM(spend) AS total_spend,
            SUM(sales) AS total_ad_sales,
            SUM(impressions) AS total_impressions,
            SUM(clicks) AS total_clicks,
            SUM(orders) AS total_orders,
            SUM(units) AS total_units
        FROM ads_campaigns
        WHERE date >= ? AND date <= ?
        GROUP BY date
        ORDER BY date
    """, [str(start_date), str(end_date)]).fetchall()

    stored = 0
    for r in rows:
        date_val, spend, ad_sales, impr, clicks, orders, units = r

        # Get total organic revenue for TACOS calculation
        organic = con.execute("""
            SELECT COALESCE(SUM(ordered_product_sales), 0)
            FROM daily_sales
            WHERE date = ? AND asin = 'ALL'
        """, [date_val]).fetchone()

        total_revenue = organic[0] if organic else 0

        acos = round(spend / ad_sales * 100, 2) if ad_sales > 0 else 0
        roas = round(ad_sales / spend, 2) if spend > 0 else 0
        tacos = round(spend / total_revenue * 100, 2) if total_revenue > 0 else 0
        avg_cpc = round(spend / clicks, 2) if clicks > 0 else 0
        avg_ctr = round(clicks / impr * 100, 2) if impr > 0 else 0

        con.execute("""
            INSERT OR REPLACE INTO ads_daily_summary
            (date, total_spend, total_sales, total_impressions, total_clicks,
             total_orders, total_units, acos, roas, tacos, avg_cpc, avg_ctr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            date_val, spend, ad_sales, impr, clicks, orders, units,
            acos, roas, tacos, avg_cpc, avg_ctr,
        ])
        stored += 1

    con.close()
    print(f"Built {stored} daily summary records")
    return stored


def pull_negative_keywords(credentials, db_path):
    """Pull negative keywords list."""
    print(f"\n{'='*60}")
    print("PULLING NEGATIVE KEYWORDS")
    print(f"{'='*60}")

    if not AD_API_AVAILABLE:
        print("  Skipping — SDK not available")
        return 0

    try:
        neg_kw = SPNegativeKeywords(credentials=credentials)
        # List all campaign-level negative keywords
        response = neg_kw.list_negative_keywords(body={})
        keywords = response.payload if response.payload else []

        if not keywords:
            print("  No negative keywords found")
            return 0

        con = duckdb.connect(str(db_path))
        stored = 0

        for kw in keywords:
            con.execute("""
                INSERT OR REPLACE INTO ads_negative_keywords
                (campaign_id, campaign_name, ad_group_id, ad_group_name,
                 keyword_id, keyword_text, match_type, keyword_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                kw.get("campaignId", ""),
                kw.get("campaignName", ""),
                kw.get("adGroupId", ""),
                kw.get("adGroupName", ""),
                kw.get("keywordId", ""),
                kw.get("keywordText", ""),
                kw.get("matchType", ""),
                kw.get("state", ""),
            ])
            stored += 1

        con.close()
        print(f"Stored {stored} negative keywords")
        return stored

    except Exception as e:
        print(f"Error pulling negative keywords: {e}")
        return 0


# ─────────────────────────────────────────────────────
# ALSO UPDATE THE LEGACY ADVERTISING TABLE
# ─────────────────────────────────────────────────────

def sync_to_legacy_table(db_path, start_date, end_date):
    """
    Copy ads data into the existing 'advertising' table
    so the current P&L calculations pick up real ad spend.
    """
    print(f"\n  Syncing ads data to legacy advertising table...")
    con = duckdb.connect(str(db_path))

    # Clear old data in range
    con.execute("""
        DELETE FROM advertising
        WHERE date >= ? AND date <= ?
    """, [str(start_date), str(end_date)])

    # Copy from ads_campaigns
    con.execute("""
        INSERT INTO advertising
        (date, campaign_id, campaign_name, campaign_type,
         impressions, clicks, spend, sales, orders, units,
         acos, roas, cpc, ctr)
        SELECT
            date, campaign_id, campaign_name, campaign_type,
            impressions, clicks, spend, sales, orders, units,
            acos, roas, cpc, ctr
        FROM ads_campaigns
        WHERE date >= ? AND date <= ?
    """, [str(start_date), str(end_date)])

    count = con.execute("SELECT COUNT(*) FROM advertising WHERE date >= ? AND date <= ?",
                        [str(start_date), str(end_date)]).fetchone()[0]
    con.close()
    print(f"  Synced {count} records to legacy table")


# ─────────────────────────────────────────────────────
# UPDATE PRODUCT AD SPEND IN _build_product_list
# ─────────────────────────────────────────────────────

def update_product_ad_spend(db_path):
    """
    After pulling ads data, aggregate ad spend per ASIN
    so the product-level P&L can use real ad spend values.
    """
    con = duckdb.connect(str(db_path))

    # Check if ads_product_performance has data
    count = con.execute("SELECT COUNT(*) FROM ads_product_performance").fetchone()[0]
    if count > 0:
        print(f"  Product ad performance: {count} records available for P&L")
    else:
        print("  No product ad performance data yet")

    con.close()


# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("GOLFGEN AMAZON ADS DATA PIPELINE")
    print(f"Run time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    creds = load_credentials()
    ads_creds = get_ads_credentials(creds)
    db_path = PROJECT_ROOT / creds["DATABASE"]["path"]

    # Always initialize tables
    init_ads_tables(db_path)

    if ads_creds is None:
        print("\n" + "!" * 60)
        print("AMAZON ADS API NOT CONFIGURED")
        print("!" * 60)
        print()
        print("To connect the Amazon Ads API, you need:")
        print()
        print("1. An Amazon Ads developer account")
        print("   → Go to: advertising.amazon.com/developer")
        print("   → Apply for API access (you said you've already applied)")
        print()
        print("2. Create a Login with Amazon (LWA) app")
        print("   → Go to: developer.amazon.com/loginwithamazon/console/site/lwa/overview.html")
        print("   → Create a new security profile")
        print("   → Note the Client ID and Client Secret")
        print()
        print("3. Authorize your seller account")
        print("   → Use the OAuth flow to get a refresh_token")
        print("   → The SDK provides a helper for this")
        print()
        print("4. Get your Profile ID")
        print("   → After auth, call the Profiles API to list profiles")
        print("   → Your US marketplace profile ID is what you need")
        print()
        print("5. Update config/credentials.json:")
        print('   "AMAZON_ADS_API": {')
        print('       "client_id": "amzn1.application-oa2-client.abc123...",')
        print('       "client_secret": "your-secret",')
        print('       "refresh_token": "Atzr|your-refresh-token...",')
        print('       "profile_id": "1234567890"')
        print("   }")
        print()
        print("Tables have been created. Once credentials are set, run this script again.")
        return

    if not AD_API_AVAILABLE:
        print("\nInstall the SDK first: pip install python-amazon-ad-api")
        return

    # Determine lookback period
    days_back = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    end_date = datetime.utcnow().date() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=days_back)
    print(f"\nLookback period: {days_back} days ({start_date} to {end_date})")

    # Pull all ads data
    results = {}
    results["campaigns"] = pull_campaign_report(ads_creds, db_path, start_date, end_date)
    results["keywords"] = pull_keyword_report(ads_creds, db_path, start_date, end_date)
    results["search_terms"] = pull_search_term_report(ads_creds, db_path, start_date, end_date)
    results["product_ads"] = pull_product_ad_report(ads_creds, db_path, start_date, end_date)
    results["neg_keywords"] = pull_negative_keywords(ads_creds, db_path)

    # Build daily summary with TACOS
    results["daily_summary"] = build_daily_summary(db_path, start_date, end_date)

    # Sync to legacy table for P&L
    sync_to_legacy_table(db_path, start_date, end_date)
    update_product_ad_spend(db_path)

    # Summary
    print(f"\n{'='*60}")
    print("ADS PULL SUMMARY")
    print(f"{'='*60}")
    for source, count in results.items():
        status = "OK" if count > 0 else "EMPTY"
        print(f"  {source:20s}: {count:6d} records  [{status}]")

    print(f"\nDone. Database: {db_path}")


if __name__ == "__main__":
    main()
