"""
Amazon Ads API and Pricing Service Integration.

Handles SP-API pricing sync, Amazon Ads coupon data, and Ads reporting.
"""

import os
import json
import csv
import logging
import threading
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from core.database import get_db_rw

from core.config import DB_PATH, DB_DIR, CONFIG_PATH, PRICING_CACHE_PATH, TIMEZONE

logger = logging.getLogger("golfgen")

# ── Sync mutex — prevent overlapping ads/pricing syncs ────────────────────
_ads_sync_lock = threading.Lock()
_ads_sync_running: str | None = None

def is_ads_sync_running() -> str | None:
    """Return the name of the currently-running ads sync, or None."""
    return _ads_sync_running


# ── Helper Functions (direct imports from modular structure) ──────────────
from services.sp_api import _load_sp_api_credentials, retry_with_backoff
from routers.item_master import load_item_master


# ── Pricing Cache Functions ──────────────────────────────────────────────

def _load_pricing_cache() -> dict:
    """Load cached pricing/coupon data."""
    if PRICING_CACHE_PATH.exists():
        with open(PRICING_CACHE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"prices": {}, "coupons": {}, "lastSync": None}


def _save_pricing_cache(data: dict):
    """Save pricing/coupon cache."""
    with open(PRICING_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _sync_pricing_data():
    """Pull current listing prices from SP-API Product Pricing API.

    Uses getCompetitivePricing to get Buy Box and listed prices per ASIN.
    Falls back gracefully if SP-API is not configured.
    """
    try:
        from sp_api.api import ProductPricing as ProductsAPI
        from sp_api.base import Marketplaces
    except ImportError:
        logger.info("Pricing sync: sp_api not installed, skipping")
        return

    credentials = _load_sp_api_credentials()
    if not credentials:
        logger.info("Pricing sync: no SP-API credentials, skipping")
        return

    # Get all ASINs from item master
    items = load_item_master()
    asins = [i["asin"] for i in items if i.get("asin")]
    if not asins:
        return

    logger.info(f"Pricing sync: fetching prices for {len(asins)} ASINs...")
    import time as _t

    cache = _load_pricing_cache()
    prices = cache.get("prices", {})

    # SP-API allows up to 20 ASINs per getCompetitivePricing call
    products_api = ProductsAPI(credentials=credentials, marketplace=Marketplaces.US)
    batch_size = 20

    for i in range(0, len(asins), batch_size):
        batch = asins[i:i + batch_size]
        try:
            result = products_api.get_competitive_pricing(
                Asins=batch,
                MarketplaceId="ATVPDKIKX0DER",
            )
            for item_data in (result.payload or []):
                asin = item_data.get("ASIN", "")
                if not asin:
                    continue
                comp_prices = item_data.get("Product", {}).get("CompetitivePricing", {})
                number_of_offers = comp_prices.get("NumberOfOfferListings", [])
                comp_price_list = comp_prices.get("CompetitivePrices", [])

                listing_price = None
                landed_price = None
                buy_box_price = None

                for cp in comp_price_list:
                    condition = cp.get("condition", "")
                    belongs_to = cp.get("belongsToRequester", False)
                    price_info = cp.get("Price", {})
                    lp = price_info.get("ListingPrice", {})
                    landed = price_info.get("LandedPrice", {})

                    if lp.get("Amount"):
                        listing_price = float(lp["Amount"])
                    if landed.get("Amount"):
                        landed_price = float(landed["Amount"])

                    comp_type = cp.get("CompetitivePriceId", "")
                    if comp_type == "1":  # Buy Box price
                        if landed.get("Amount"):
                            buy_box_price = float(landed["Amount"])
                        elif lp.get("Amount"):
                            buy_box_price = float(lp["Amount"])

                prices[asin] = {
                    "listingPrice": listing_price,
                    "landedPrice": landed_price,
                    "buyBoxPrice": buy_box_price,
                    "fetchedAt": datetime.utcnow().isoformat(),
                }

            # Rate limit: SP-API throttles at ~10 calls/sec for pricing
            if i + batch_size < len(asins):
                _t.sleep(0.5)

        except Exception as e:
            logger.error(f"Pricing sync batch error (ASINs {i}-{i+batch_size}): {e}")
            _t.sleep(2)

    cache["prices"] = prices
    cache["lastSync"] = datetime.utcnow().isoformat()
    _save_pricing_cache(cache)
    logger.info(f"Pricing sync complete: {len(prices)} ASINs cached")


def _sync_coupon_data():
    """Pull active coupon data from Amazon Ads Coupons API.

    Uses the Coupons API (part of python-amazon-ad-api) to list all coupons
    and their statuses. Maps coupon ASINs back to item master entries.
    """
    ads_creds = _load_ads_credentials()
    if not ads_creds:
        logger.info("Coupon sync: no Ads API credentials, skipping")
        return

    profile_id = ads_creds.get("profile_id", "")
    if not profile_id:
        logger.info("Coupon sync: no profile_id, skipping")
        return

    try:
        from ad_api.api import Coupons
    except ImportError:
        logger.info("Coupon sync: ad_api Coupons not available, skipping")
        return

    logger.info("Coupon sync: fetching coupons from Amazon Ads API...")

    cache = _load_pricing_cache()
    coupons_by_asin = {}

    try:
        coupons_api = Coupons(credentials=ads_creds)

        # List coupons with pagination
        page_token = None
        total_fetched = 0
        max_pages = 10

        for page in range(max_pages):
            body = {
                "stateFilter": {
                    "includes": ["ENABLED", "SCHEDULED"]
                },
                "pageSize": 100,
            }
            if page_token:
                body["pageToken"] = page_token

            result = coupons_api.list_coupons(body=json.dumps(body))
            payload = result.payload or {}
            coupon_list = payload.get("coupons", [])
            total_fetched += len(coupon_list)

            for coupon in coupon_list:
                coupon_id = coupon.get("couponId", "")
                state = coupon.get("state", "UNKNOWN")
                discount = coupon.get("discount", {})
                disc_type = discount.get("discountType", "")  # PERCENTAGE or AMOUNT
                disc_value = float(discount.get("discountValue", 0))

                # Budget and redemption info
                budget = coupon.get("budget", {})
                budget_amount = float(budget.get("budgetAmount", 0))
                budget_used = float(budget.get("budgetConsumed", 0))
                redemptions = coupon.get("totalRedemptions", 0)

                start_date = coupon.get("startDate", "")
                end_date = coupon.get("endDate", "")

                # Map to ASINs
                asin_list = coupon.get("asins", [])
                product_criteria = coupon.get("productCriteria", {})
                if not asin_list and product_criteria:
                    asin_list = product_criteria.get("asins", [])

                coupon_info = {
                    "couponId": coupon_id,
                    "state": state,
                    "discountType": "%" if disc_type == "PERCENTAGE" else "$",
                    "discountValue": disc_value,
                    "budgetAmount": budget_amount,
                    "budgetUsed": budget_used,
                    "redemptions": redemptions,
                    "startDate": start_date,
                    "endDate": end_date,
                    "fetchedAt": datetime.utcnow().isoformat(),
                }

                for asin in asin_list:
                    if isinstance(asin, str) and asin:
                        # Keep the most recently fetched coupon per ASIN
                        # If multiple coupons exist, prefer ENABLED over SCHEDULED
                        existing = coupons_by_asin.get(asin)
                        if not existing or (state == "ENABLED" and existing.get("state") != "ENABLED"):
                            coupons_by_asin[asin] = coupon_info

            # Check for next page
            page_token = payload.get("nextPageToken")
            if not page_token or not coupon_list:
                break

        logger.info(f"Coupon sync: {total_fetched} coupons fetched, mapped to {len(coupons_by_asin)} ASINs")

    except Exception as e:
        logger.error(f"Coupon sync error: {e}")

    cache["coupons"] = coupons_by_asin
    cache["lastSync"] = datetime.utcnow().isoformat()
    _save_pricing_cache(cache)


def _sync_pricing_and_coupons():
    """Run both pricing and coupon syncs. Called from the sync loop."""
    global _ads_sync_running
    if not _ads_sync_lock.acquire(blocking=False):
        logger.warning(f"  Skipping pricing/coupon sync — '{_ads_sync_running}' already running")
        return
    _ads_sync_running = "pricing_coupons"
    try:
        try:
            _sync_pricing_data()
        except Exception as e:
            logger.error(f"Pricing sync error: {e}")
        try:
            _sync_coupon_data()
        except Exception as e:
            logger.error(f"Coupon sync error: {e}")
    finally:
        _ads_sync_running = None
        _ads_sync_lock.release()


# ── Ads API Functions ────────────────────────────────────────────────────

def _get_db_setting(key: str) -> str:
    """Read a value from app_settings table (helper for credential loading)."""
    try:
        from core.database import get_db
        con = get_db()
        row = con.execute("SELECT value FROM app_settings WHERE key = ?", [key]).fetchone()
        con.close()
        return row[0] if row else ""
    except Exception:
        return ""


def _load_ads_credentials() -> dict | None:
    """Load Amazon Ads API credentials from env vars, DB, or config file.

    Priority order for each credential:
      1. Environment variables (AMAZON_ADS_*, ADS_API_*, ADS_*)
      2. Database app_settings table (set via OAuth flow)
      3. Config file (credentials.json)
    """
    # Support all three naming conventions used across deployments:
    #   AMAZON_ADS_*   (new canonical)
    #   ADS_API_*      (Railway env vars as documented in CLAUDE.md)
    #   ADS_*          (legacy short form)
    env_client_id = (
        os.environ.get("AMAZON_ADS_CLIENT_ID")
        or os.environ.get("ADS_API_CLIENT_ID")
        or os.environ.get("ADS_CLIENT_ID", "")
    )
    env_client_secret = (
        os.environ.get("AMAZON_ADS_CLIENT_SECRET")
        or os.environ.get("ADS_API_CLIENT_SECRET")
        or os.environ.get("ADS_CLIENT_SECRET", "")
    )
    env_refresh_token = (
        os.environ.get("AMAZON_ADS_REFRESH_TOKEN")
        or os.environ.get("ADS_API_REFRESH_TOKEN")
        or os.environ.get("ADS_REFRESH_TOKEN", "")
    )
    env_profile_id = (
        os.environ.get("AMAZON_ADS_PROFILE_ID")
        or os.environ.get("ADS_API_PROFILE_ID")
        or os.environ.get("ADS_PROFILE_ID", "")
    )

    # Merge env vars with DB-stored values (env takes priority)
    client_id = env_client_id or _get_db_setting("ads_client_id")
    client_secret = env_client_secret or _get_db_setting("ads_client_secret")
    refresh_token = env_refresh_token or _get_db_setting("ads_refresh_token")
    profile_id = env_profile_id or _get_db_setting("ads_profile_id")

    if client_id:
        creds = {
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
            "profile_id": profile_id,
        }
        if creds["refresh_token"] and creds["client_secret"]:
            logger.info(
                f"Ads creds loaded: client_id={client_id[:8]}... "
                f"profile_id={creds['profile_id'] or '(will discover)'}"
            )
            return creds
        logger.warning(
            f"Ads creds: client_id found ({client_id[:8]}...) "
            f"but refresh_token or client_secret is missing"
        )
        return None

    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            creds_json = json.load(f)
        ads = creds_json.get("AMAZON_ADS_API", {})
        creds = {
            "refresh_token": ads.get("refresh_token", ""),
            "client_id": ads.get("client_id", ""),
            "client_secret": ads.get("client_secret", ""),
            "profile_id": ads.get("profile_id", ""),
        }
        if creds["refresh_token"] and creds["client_id"]:
            return creds

    return None


def _ensure_ads_tables():
    """Ensure ads DuckDB tables exist and have hierarchy columns.

    NOTE: The primary table definitions live in core/database.py (init_all_tables).
    This function only ensures hierarchy columns exist for upgrades and creates
    tables as fallback if database.py hasn't run yet. Uses DATE type to match
    the canonical definitions in database.py.
    """
    con = get_db_rw()

    # Fallback CREATE IF NOT EXISTS — uses DATE type to match database.py
    con.execute("""
        CREATE TABLE IF NOT EXISTS advertising (
            date DATE,
            campaign_id TEXT,
            campaign_name TEXT,
            impressions INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders INTEGER DEFAULT 0,
            units INTEGER DEFAULT 0,
            division VARCHAR DEFAULT 'golf',
            customer VARCHAR DEFAULT 'amazon',
            platform VARCHAR DEFAULT 'sp_api'
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_campaigns (
            date DATE,
            campaign_id TEXT,
            campaign_name TEXT,
            campaign_type TEXT DEFAULT 'SP',
            campaign_status TEXT DEFAULT '',
            daily_budget DOUBLE DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders INTEGER DEFAULT 0,
            units INTEGER DEFAULT 0,
            division VARCHAR DEFAULT 'golf',
            customer VARCHAR DEFAULT 'amazon',
            platform VARCHAR DEFAULT 'sp_api'
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_keywords (
            date DATE,
            campaign_id TEXT,
            campaign_name TEXT,
            ad_group_id TEXT,
            ad_group_name TEXT,
            keyword_id TEXT,
            keyword_text TEXT,
            match_type TEXT,
            impressions INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders INTEGER DEFAULT 0,
            units INTEGER DEFAULT 0,
            division VARCHAR DEFAULT 'golf',
            customer VARCHAR DEFAULT 'amazon',
            platform VARCHAR DEFAULT 'sp_api'
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_search_terms (
            date DATE,
            campaign_id TEXT,
            campaign_name TEXT,
            ad_group_name TEXT,
            keyword_text TEXT,
            match_type TEXT,
            search_term TEXT,
            impressions INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0,
            spend DOUBLE DEFAULT 0,
            sales DOUBLE DEFAULT 0,
            orders INTEGER DEFAULT 0,
            units INTEGER DEFAULT 0,
            division VARCHAR DEFAULT 'golf',
            customer VARCHAR DEFAULT 'amazon',
            platform VARCHAR DEFAULT 'sp_api'
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS ads_negative_keywords (
            keyword_text TEXT,
            match_type TEXT,
            campaign_name TEXT,
            ad_group_name TEXT,
            keyword_status TEXT DEFAULT 'ENABLED'
        )
    """)
    # Ensure existing tables have the hierarchy columns (safe ALTER for upgrades)
    for tbl in ["advertising", "ads_campaigns", "ads_keywords", "ads_search_terms"]:
        for col, default in [("division", "'golf'"), ("customer", "'amazon'"), ("platform", "'sp_api'")]:
            try:
                con.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} VARCHAR DEFAULT {default}")
            except Exception:
                pass  # column already exists
    con.close()
    logger.info("Ads tables verified/created")


def _sync_ads_data():
    """Pull Sponsored Products reporting data from Amazon Ads API v3."""
    global _ads_sync_running
    if not _ads_sync_lock.acquire(blocking=False):
        logger.warning(f"  Skipping ads sync — '{_ads_sync_running}' already running")
        return
    _ads_sync_running = "ads_data"
    try:
        _sync_ads_data_inner()
    finally:
        _ads_sync_running = None
        _ads_sync_lock.release()


def _sync_ads_data_inner():
    """Inner implementation of ads sync (called under mutex)."""
    import time as _time
    import gzip as gz
    import requests as req

    ads_creds = _load_ads_credentials()
    if not ads_creds:
        logger.info("Ads sync: no credentials configured, skipping")
        return

    _ensure_ads_tables()

    profile_id = ads_creds.get("profile_id", "")
    if not profile_id:
        logger.warning("Ads sync: no profile_id set, attempting to discover profiles...")
        try:
            from ad_api.api import Profiles
            result = Profiles(credentials=ads_creds).get_profiles()
            profiles = result.payload
            if profiles:
                for p in profiles:
                    if p.get("accountInfo", {}).get("marketplaceStringId") == "ATVPDKIKX0DER":
                        profile_id = str(p.get("profileId", ""))
                        break
                if not profile_id:
                    profile_id = str(profiles[0].get("profileId", ""))
                logger.info(f"Ads sync: discovered profile_id={profile_id}")
                ads_creds["profile_id"] = profile_id
            else:
                logger.error("Ads sync: no profiles found")
                return
        except Exception as e:
            logger.error(f"Ads sync: failed to discover profiles: {e}")
            return

    # Pull last 14 days of data (shorter range = faster report generation)
    today = datetime.now(ZoneInfo("America/Chicago"))
    start_date = (today - timedelta(days=14)).strftime("%Y-%m-%d")
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    # v3 API: each reportTypeId has its own allowed columns and groupBy values.
    # Dimension columns are returned automatically — only specify metric columns.
    # ── Report 1: Campaign-level daily data ──
    _pull_ads_report(
        ads_creds, "spCampaigns",
        columns=["date", "impressions", "clicks", "spend",
                 "purchases7d", "unitsSoldClicks7d", "sales7d"],
        start_date=start_date, end_date=end_date,
        handler=_handle_campaign_report,
        group_by=["campaign"],
    )

    # ── Report 2: Targeting/Keywords daily data ──
    # Note: spTargeting uses "cost" not "spend"
    _pull_ads_report(
        ads_creds, "spTargeting",
        columns=["date", "impressions", "clicks", "cost",
                 "purchases7d", "unitsSoldClicks7d", "sales7d"],
        start_date=start_date, end_date=end_date,
        handler=_handle_targeting_report,
        group_by=["targeting"],
    )

    # ── Report 3: Search terms ──
    _pull_ads_report(
        ads_creds, "spSearchTerm",
        columns=["date", "impressions", "clicks", "spend",
                 "purchases7d", "unitsSoldClicks7d", "sales7d"],
        start_date=start_date, end_date=end_date,
        handler=_handle_search_term_report,
        group_by=["searchTerm"],
    )

    logger.info("Ads sync complete")


def _pull_ads_report(creds, report_type_id, columns, start_date, end_date, handler, group_by=None):
    """Create, poll, download, and process a single Amazon Ads v3 report."""
    import time as _time
    import gzip as gz
    import requests as req
    from ad_api.api import reports as ads_reports

    body = {
        "name": f"{report_type_id}_{start_date}_{end_date}",
        "startDate": start_date,
        "endDate": end_date,
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": group_by or ["advertiser"],
            "columns": columns,
            "reportTypeId": report_type_id,
            "timeUnit": "DAILY",
            "format": "GZIP_JSON",
        }
    }

    try:
        logger.info(f"Ads sync: creating {report_type_id} report ({start_date} to {end_date})...")
        reports_api = ads_reports.Reports(credentials=creds)

        # Use direct HTTP for v3 reporting API (library has JSON parse issues)
        # First get an access token
        token_resp = req.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": creds["refresh_token"],
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
        }, timeout=30)
        token_data = token_resp.json()
        access_token = token_data.get("access_token", "")
        if not access_token:
            logger.error(f"Ads sync: token refresh failed: {token_data}")
            return

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": creds["client_id"],
            "Amazon-Advertising-API-Scope": str(creds["profile_id"]),
            "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
            "Accept": "application/vnd.createasyncreportrequest.v3+json",
        }

        create_resp = req.post(
            "https://advertising-api.amazon.com/reporting/reports",
            headers=headers,
            json=body,
            timeout=30,
        )
        logger.info(f"Ads sync: create report response: {create_resp.status_code} {create_resp.text[:500]}")

        if create_resp.status_code not in (200, 202):
            logger.error(f"Ads sync: create report failed for {report_type_id}: {create_resp.status_code} {create_resp.text[:500]}")
            return

        create_data = create_resp.json()
        report_id = create_data.get("reportId")

        if not report_id:
            logger.error(f"Ads sync: no reportId returned for {report_type_id}, response: {create_data}")
            return

        # Poll for completion (max ~10 min)
        download_url = None
        for attempt in range(60):
            _time.sleep(10)
            poll_resp = req.get(
                f"https://advertising-api.amazon.com/reporting/reports/{report_id}",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Amazon-Advertising-API-ClientId": creds["client_id"],
                    "Amazon-Advertising-API-Scope": str(creds["profile_id"]),
                },
                timeout=30,
            )
            poll_data = poll_resp.json()
            status = poll_data.get("status")
            extra = ""
            if status in ("FAILED", "CANCELLED"):
                extra = f" reason={poll_data.get('failureReason', 'unknown')}"
            elif attempt == 0:
                extra = f" full_resp={str(poll_data)[:300]}"
            logger.info(f"Ads sync: poll {report_type_id} attempt {attempt+1}: {status}{extra}")

            if status == "COMPLETED":
                download_url = poll_data.get("url")
                break
            elif status in ("FAILED", "CANCELLED"):
                logger.error(f"Ads sync: {report_type_id} report {status}: {poll_data.get('failureReason', '')}")
                return

        if not download_url:
            logger.error(f"Ads sync: {report_type_id} report timed out")
            return

        # Download and decompress
        resp = req.get(download_url)
        try:
            data = json.loads(gz.decompress(resp.content).decode("utf-8"))
        except Exception:
            data = json.loads(resp.text)

        if isinstance(data, str):
            data = json.loads(data)

        handler(data)
        logger.info(f"Ads sync: {report_type_id} — {len(data) if isinstance(data, list) else 'N/A'} rows processed")

    except Exception as e:
        logger.error(f"Ads sync error ({report_type_id}): {e}")


def _handle_campaign_report(data):
    """Insert campaign-level ads data into DuckDB with transaction wrapping."""
    if not isinstance(data, list):
        logger.warning(f"Campaign report: expected list, got {type(data)}")
        return

    con = get_db_rw()
    con.execute("BEGIN TRANSACTION")
    inserted = 0
    errors = 0
    try:
        for row in data:
            try:
                date = row.get("date", "")
                campaign_id = str(row.get("campaignId", ""))
                campaign_name = row.get("campaignName", "")
                spend = float(row.get("spend", row.get("cost", 0)) or 0)
                sales = float(row.get("sales7d", row.get("sales", 0)) or 0)
                impressions = int(row.get("impressions", 0) or 0)
                clicks = int(row.get("clicks", 0) or 0)
                orders = int(row.get("purchases7d", row.get("purchases", 0)) or 0)
                units = int(row.get("unitsSoldClicks7d", row.get("unitsSold", 0)) or 0)
                status = row.get("campaignStatus", "")
                budget = float(row.get("campaignBudgetAmount", 0) or 0)

                if not date or not campaign_id:
                    continue

                # Aggregate table — DELETE+INSERT (DuckDB doesn't support INSERT OR REPLACE)
                con.execute("DELETE FROM advertising WHERE date = CAST(? AS DATE) AND campaign_id = ?",
                            [date, campaign_id])
                con.execute("""
                    INSERT INTO advertising
                    (date, campaign_id, campaign_name, impressions, clicks, spend, sales, orders, units,
                     division, customer, platform)
                    VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                """, [date, campaign_id, campaign_name, impressions, clicks, spend, sales, orders, units])

                # Campaign detail table
                con.execute("DELETE FROM ads_campaigns WHERE date = CAST(? AS DATE) AND campaign_id = ?",
                            [date, campaign_id])
                con.execute("""
                    INSERT INTO ads_campaigns
                    (date, campaign_id, campaign_name, campaign_type, campaign_status,
                     daily_budget, impressions, clicks, spend, sales, orders, units,
                     division, customer, platform)
                    VALUES (CAST(? AS DATE), ?, ?, 'SP', ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                """, [date, campaign_id, campaign_name, status, budget,
                      impressions, clicks, spend, sales, orders, units])
                inserted += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.error(f"Campaign insert error: {e} (row date={row.get('date')})")

        con.execute("COMMIT")
    except Exception as e:
        logger.error(f"Campaign report transaction error: {e}")
        try:
            con.execute("ROLLBACK")
            logger.info("Campaign report rolled back due to error")
        except Exception:
            pass
    finally:
        con.close()
    logger.info(f"Campaign report handler: {inserted} inserted, {errors} errors out of {len(data)} rows")


def _handle_targeting_report(data):
    """Insert keyword/targeting data into DuckDB with transaction wrapping."""
    if not isinstance(data, list):
        logger.warning(f"Targeting report: expected list, got {type(data)}")
        return

    con = get_db_rw()
    con.execute("BEGIN TRANSACTION")
    inserted = 0
    errors = 0
    try:
        for row in data:
            try:
                date = row.get("date", "")
                keyword_id = str(row.get("keywordId", row.get("targetId", "")))
                if not date or not keyword_id:
                    continue

                con.execute("DELETE FROM ads_keywords WHERE date = CAST(? AS DATE) AND keyword_id = ?",
                            [date, keyword_id])
                con.execute("""
                    INSERT INTO ads_keywords
                    (date, campaign_id, campaign_name, ad_group_id, ad_group_name,
                     keyword_id, keyword_text, match_type,
                     impressions, clicks, spend, sales, orders, units,
                     division, customer, platform)
                    VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                """, [
                    date,
                    str(row.get("campaignId", "")),
                    row.get("campaignName", ""),
                    str(row.get("adGroupId", "")),
                    row.get("adGroupName", ""),
                    keyword_id,
                    row.get("keyword", row.get("keywordText", row.get("targeting", ""))),
                    row.get("matchType", ""),
                    int(row.get("impressions", 0) or 0),
                    int(row.get("clicks", 0) or 0),
                    float(row.get("spend", row.get("cost", 0)) or 0),
                    float(row.get("sales7d", row.get("sales", 0)) or 0),
                    int(row.get("purchases7d", row.get("purchases", 0)) or 0),
                    int(row.get("unitsSoldClicks7d", row.get("unitsSold", 0)) or 0),
                ])
                inserted += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.error(f"Targeting insert error: {e} (row date={row.get('date')})")

        con.execute("COMMIT")
    except Exception as e:
        logger.error(f"Targeting report transaction error: {e}")
        try:
            con.execute("ROLLBACK")
            logger.info("Targeting report rolled back due to error")
        except Exception:
            pass
    finally:
        con.close()
    logger.info(f"Targeting report handler: {inserted} inserted, {errors} errors out of {len(data)} rows")


def _handle_search_term_report(data):
    """Insert search term data into DuckDB with transaction wrapping."""
    if not isinstance(data, list):
        logger.warning(f"Search term report: expected list, got {type(data)}")
        return

    con = get_db_rw()
    con.execute("BEGIN TRANSACTION")
    inserted = 0
    errors = 0
    try:
        for row in data:
            try:
                date = row.get("date", "")
                search_term = row.get("searchTerm", "")
                campaign_id = str(row.get("campaignId", ""))
                if not date or not search_term:
                    continue

                con.execute("""DELETE FROM ads_search_terms
                    WHERE date = CAST(? AS DATE) AND search_term = ? AND campaign_id = ?""",
                    [date, search_term, campaign_id])
                con.execute("""
                    INSERT INTO ads_search_terms
                    (date, campaign_id, campaign_name, ad_group_name,
                     keyword_text, match_type, search_term,
                     impressions, clicks, spend, sales, orders, units,
                     division, customer, platform)
                    VALUES (CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'amazon', 'sp_api')
                """, [
                    date, campaign_id,
                    row.get("campaignName", ""),
                    row.get("adGroupName", ""),
                    row.get("keyword", row.get("keywordText", "")),
                    row.get("matchType", ""),
                    search_term,
                    int(row.get("impressions", 0) or 0),
                    int(row.get("clicks", 0) or 0),
                    float(row.get("spend", row.get("cost", 0)) or 0),
                    float(row.get("sales7d", row.get("sales", 0)) or 0),
                    int(row.get("purchases7d", row.get("purchases", 0)) or 0),
                    int(row.get("unitsSoldClicks7d", row.get("unitsSold", 0)) or 0),
                ])
                inserted += 1
            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.error(f"Search term insert error: {e} (row date={row.get('date')})")

        con.execute("COMMIT")
    except Exception as e:
        logger.error(f"Search term report transaction error: {e}")
        try:
            con.execute("ROLLBACK")
            logger.info("Search term report rolled back due to error")
        except Exception:
            pass
    finally:
        con.close()
    logger.info(f"Search term report handler: {inserted} inserted, {errors} errors out of {len(data)} rows")


