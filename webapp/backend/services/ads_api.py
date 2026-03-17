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


def _parse_price_amount(obj) -> float | None:
    """Extract numeric amount from a pricing dict or model object."""
    if obj is None:
        return None
    if hasattr(obj, "to_dict"):
        try:
            obj = obj.to_dict()
        except Exception:
            pass
    if isinstance(obj, dict):
        for key in ("Amount", "amount", "CurrencyAmount", "currency_amount", "value"):
            v = obj.get(key)
            if v is not None:
                try:
                    return float(v)
                except (ValueError, TypeError):
                    continue
    try:
        return float(obj)
    except (ValueError, TypeError):
        return None


def _to_dict_safe(obj) -> dict:
    """Convert a model object or dict to a plain dict."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict"):
        try:
            return obj.to_dict() or {}
        except Exception:
            pass
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return {}


def _sync_pricing_data():
    """Pull current listing prices from SP-API Product Pricing API.

    Uses getPricing (seller's own prices) NOT getCompetitivePricing.
    getCompetitivePricing returns empty for private-label ASINs with no
    competition — these products are the only seller so there's nothing
    'competitive' to report.  getPricing returns the actual listed price.
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
        logger.warning("Pricing sync: no ASINs in item_master, skipping")
        return

    logger.info(f"Pricing sync: fetching prices for {len(asins)} ASINs via getPricing...")
    import time as _t

    cache = _load_pricing_cache()
    prices = cache.get("prices", {})

    # SP-API allows up to 20 ASINs per getPricing call
    products_api = ProductsAPI(credentials=credentials, marketplace=Marketplaces.US)
    batch_size = 20

    for i in range(0, len(asins), batch_size):
        batch = asins[i:i + batch_size]
        batch_num = i // batch_size + 1
        try:
            result = products_api.get_pricing(
                ItemType="Asin",
                Asins=batch,
                MarketplaceId="ATVPDKIKX0DER",
            )
            payload = result.payload if hasattr(result, "payload") else result

            if payload is None:
                logger.warning(f"  Batch {batch_num}: API returned None payload")
                continue

            if not isinstance(payload, list):
                logger.warning(f"  Batch {batch_num}: unexpected payload type={type(payload).__name__} — wrapping")
                payload = [payload] if payload else []

            logger.info(f"  Batch {batch_num}: {len(payload)} items returned")

            for item_resp in payload:
                item_dict = _to_dict_safe(item_resp)
                asin = item_dict.get("ASIN") or item_dict.get("asin") or ""
                status = item_dict.get("status", "Success")

                if not asin:
                    logger.warning(f"  Batch {batch_num}: item missing ASIN: {list(item_dict.keys())[:5]}")
                    continue
                if status != "Success":
                    logger.warning(f"  {asin}: status={status}, skipping")
                    continue

                product = _to_dict_safe(item_dict.get("Product") or item_dict.get("product"))
                offers_raw = product.get("Offers") or product.get("offers") or []
                offers = list(offers_raw) if offers_raw else []

                listing_price = None
                sale_price = None
                regular_price = None
                buy_box_price = None

                for offer in offers[:1]:  # Use first offer (our own FBA offer)
                    o = _to_dict_safe(offer)
                    buying_price = _to_dict_safe(o.get("BuyingPrice") or o.get("buying_price"))
                    lp = _to_dict_safe(buying_price.get("ListingPrice") or buying_price.get("listing_price"))
                    sp = _to_dict_safe(buying_price.get("SalePrice") or buying_price.get("sale_price"))
                    landed = _to_dict_safe(buying_price.get("LandedPrice") or buying_price.get("landed_price"))
                    rp = _to_dict_safe(o.get("RegularPrice") or o.get("regular_price"))

                    listing_price = _parse_price_amount(lp)
                    sale_price = _parse_price_amount(sp)
                    regular_price = _parse_price_amount(rp)
                    buy_box_price = _parse_price_amount(landed) or _parse_price_amount(lp)

                prices[asin] = {
                    "listingPrice": listing_price,
                    "regularPrice": regular_price,
                    "salePrice": sale_price,
                    "buyBoxPrice": buy_box_price,
                    "landedPrice": buy_box_price,
                    "fetchedAt": datetime.utcnow().isoformat(),
                }
                logger.info(f"  {asin}: list=${listing_price}, sale=${sale_price}, reg=${regular_price}, buybox=${buy_box_price}")

            # Rate limit: SP-API throttles pricing calls
            if i + batch_size < len(asins):
                _t.sleep(0.5)

        except Exception as e:
            logger.error(f"Pricing sync batch {batch_num} error: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
    """Inner implementation of ads sync (called under mutex).

    Two-phase approach:
      Phase A: Check previously created v3 reports (from last sync) — download
               any that have completed since then.
      Phase B: Create new v3 reports for the date range. Save report IDs to
               pending file so the NEXT sync can download them.
      Phase C: Sync campaign metadata via list API (always works).

    v3 reports take 15-60+ minutes to complete, so we never wait — we create
    them and pick up completed ones on the next sync cycle.
    """
    import time as _time
    import gzip as gz
    import requests as req
    import re

    ads_creds = _load_ads_credentials()
    if not ads_creds:
        logger.info("Ads sync: no credentials configured, skipping")
        return

    _ensure_ads_tables()

    profile_id = ads_creds.get("profile_id", "")
    if not profile_id:
        logger.warning("Ads sync: no profile_id set, attempting to discover profiles...")
        try:
            access_token = _get_ads_access_token(ads_creds)
            if not access_token:
                logger.error("Ads sync: cannot get token to discover profiles")
                return
            prof_resp = req.get(
                "https://advertising-api.amazon.com/v2/profiles",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Amazon-Advertising-API-ClientId": ads_creds["client_id"],
                },
                timeout=30,
            )
            profiles = prof_resp.json() if prof_resp.status_code == 200 else []
            if profiles and isinstance(profiles, list):
                for p in profiles:
                    if p.get("accountInfo", {}).get("marketplaceStringId") == "ATVPDKIKX0DER":
                        profile_id = str(p.get("profileId", ""))
                        break
                if not profile_id:
                    profile_id = str(profiles[0].get("profileId", ""))
                logger.info(f"Ads sync: discovered profile_id={profile_id}")
                ads_creds["profile_id"] = profile_id
            else:
                logger.error(f"Ads sync: no profiles found (status={prof_resp.status_code})")
                return
        except Exception as e:
            logger.error(f"Ads sync: failed to discover profiles: {e}")
            return

    access_token = _get_ads_access_token(ads_creds)
    if not access_token:
        logger.error("Ads sync: failed to get access token")
        return

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": ads_creds["client_id"],
        "Amazon-Advertising-API-Scope": str(profile_id),
    }

    today = datetime.now(ZoneInfo("America/Chicago"))

    # ── Phase A: Check pending reports from previous sync ──
    pending_file = Path(DB_DIR) / "ads_pending_reports.json"
    pending_reports = _load_pending_reports(pending_file)
    completed_count = 0

    if pending_reports:
        logger.info(f"Ads sync: checking {len(pending_reports)} pending reports...")
        still_pending = []
        for rpt in pending_reports:
            report_id = rpt.get("report_id", "")
            report_type = rpt.get("report_type", "")
            created_at = rpt.get("created_at", "")
            if not report_id:
                continue

            # Skip reports older than 24h (they won't complete)
            try:
                created = datetime.fromisoformat(created_at)
                if (today - created).total_seconds() > 86400:
                    logger.info(f"Ads sync: dropping expired report {report_id} ({report_type})")
                    continue
            except Exception:
                pass

            try:
                poll_resp = req.get(
                    f"https://advertising-api.amazon.com/reporting/reports/{report_id}",
                    headers=headers,
                    timeout=30,
                )
                pdata = poll_resp.json()
                status = pdata.get("status", "UNKNOWN")

                if status == "COMPLETED":
                    download_url = pdata.get("url")
                    if download_url:
                        dl_resp = req.get(download_url, timeout=60)
                        try:
                            data = json.loads(gz.decompress(dl_resp.content).decode("utf-8"))
                        except Exception:
                            data = json.loads(dl_resp.text)
                        if isinstance(data, str):
                            data = json.loads(data)

                        # Route to appropriate handler
                        if report_type == "spCampaigns":
                            _handle_campaign_report(data)
                        elif report_type == "spTargeting":
                            _handle_targeting_report(data)
                        elif report_type == "spSearchTerm":
                            _handle_search_term_report(data)

                        completed_count += 1
                        logger.info(f"Ads sync: downloaded completed {report_type} report — "
                                   f"{len(data) if isinstance(data, list) else '?'} rows")
                elif status in ("FAILED", "CANCELLED"):
                    logger.warning(f"Ads sync: report {report_id} ({report_type}) {status}")
                else:
                    # Still PENDING — keep it
                    still_pending.append(rpt)
            except Exception as e:
                logger.error(f"Ads sync: error checking report {report_id}: {e}")
                still_pending.append(rpt)  # keep and retry next time

        pending_reports = still_pending
    else:
        logger.info("Ads sync: no pending reports to check")

    # ── Phase B: Create new v3 reports ──
    # Create one report per type covering 30 days (API max is 31 days)
    start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    report_configs = [
        {
            "report_type": "spCampaigns",
            "columns": ["date", "campaignId", "campaignName", "campaignStatus",
                        "campaignBudgetAmount",
                        "impressions", "clicks", "spend",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["campaign"],
        },
        {
            "report_type": "spTargeting",
            "columns": ["date", "campaignId", "campaignName",
                        "adGroupId", "adGroupName",
                        "keywordId", "keyword", "matchType",
                        "impressions", "clicks", "cost",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["targeting"],
        },
        {
            "report_type": "spSearchTerm",
            "columns": ["date", "campaignId", "campaignName",
                        "adGroupName", "keyword", "matchType", "searchTerm",
                        "impressions", "clicks", "spend",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["searchTerm"],
        },
    ]

    headers_create = {
        **headers,
        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
        "Accept": "application/vnd.createasyncreportrequest.v3+json",
    }

    new_reports_created = 0
    for cfg in report_configs:
        rt = cfg["report_type"]

        # Skip if we already have a pending report of this type
        if any(r["report_type"] == rt for r in pending_reports):
            logger.info(f"Ads sync: skipping {rt} — already pending")
            continue

        body = {
            "name": f"{rt}_{start_date}_{end_date}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": cfg["group_by"],
                "columns": cfg["columns"],
                "reportTypeId": rt,
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            }
        }

        try:
            create_resp = req.post(
                "https://advertising-api.amazon.com/reporting/reports",
                headers=headers_create,
                json=body,
                timeout=30,
            )

            report_id = None
            if create_resp.status_code in (200, 202):
                report_id = create_resp.json().get("reportId")
            elif create_resp.status_code == 425:
                # Duplicate — extract existing report ID and track it
                match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',
                                  create_resp.text)
                if match:
                    report_id = match.group(1)
                    logger.info(f"Ads sync: {rt} duplicate, tracking existing {report_id}")
            else:
                logger.warning(f"Ads sync: create {rt} failed: {create_resp.status_code} {create_resp.text[:300]}")

            if report_id:
                pending_reports.append({
                    "report_id": report_id,
                    "report_type": rt,
                    "start_date": start_date,
                    "end_date": end_date,
                    "created_at": today.isoformat(),
                })
                new_reports_created += 1
                logger.info(f"Ads sync: created {rt} report {report_id}")

        except Exception as e:
            logger.error(f"Ads sync: error creating {rt} report: {e}")

    # Save pending reports for next sync cycle
    _save_pending_reports(pending_file, pending_reports)
    logger.info(f"Ads sync: {new_reports_created} new reports created, "
                f"{len(pending_reports)} pending, {completed_count} completed")

    # ── Phase C: Sync campaign metadata ──
    _sync_campaign_metadata(headers, today)

    logger.info("Ads sync complete")


def _load_pending_reports(path):
    """Load pending report IDs from JSON file."""
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load pending reports: {e}")
    return []


def _save_pending_reports(path, reports):
    """Save pending report IDs to JSON file."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(reports, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"Failed to save pending reports: {e}")


def _sync_campaign_metadata(headers, today):
    """Sync campaign names, budgets, and status via campaigns list API."""
    import requests as req
    import time as _time

    logger.info("Ads sync: fetching campaign metadata...")
    all_campaigns = []
    next_token = None

    for page in range(10):
        list_body = {
            "maxResults": 100,
            "stateFilter": {"include": ["ENABLED", "PAUSED"]},
        }
        if next_token:
            list_body["nextToken"] = next_token

        try:
            camp_resp = req.post(
                "https://advertising-api.amazon.com/sp/campaigns/list",
                headers={**headers, "Accept": "application/vnd.spCampaign.v3+json",
                         "Content-Type": "application/vnd.spCampaign.v3+json"},
                json=list_body,
                timeout=30,
            )
            if camp_resp.status_code != 200:
                logger.warning(f"Ads sync: campaign list page {page} failed: {camp_resp.status_code}")
                break

            data = camp_resp.json()
            campaigns = data.get("campaigns", [])
            all_campaigns.extend(campaigns)
            next_token = data.get("nextToken")
            if not next_token:
                break
            _time.sleep(1)
        except Exception as e:
            logger.error(f"Ads sync: campaign list error: {e}")
            break

    if not all_campaigns:
        logger.warning("Ads sync: no campaigns from list API")
        return

    # Insert enabled campaigns into ads_campaigns for today (metadata only)
    con = get_db_rw()
    today_str = today.strftime("%Y-%m-%d")
    inserted = 0

    try:
        con.execute("BEGIN TRANSACTION")
        for c in all_campaigns:
            cid = str(c.get("campaignId", ""))
            name = c.get("name", "")
            state = c.get("state", "")
            budget = float(c.get("budget", {}).get("budget", 0))

            if state != "ENABLED":
                continue

            # Only insert if no report-based data exists for today
            existing = con.execute(
                "SELECT 1 FROM ads_campaigns WHERE date = CAST(? AS DATE) AND campaign_id = ? AND impressions > 0",
                [today_str, cid]
            ).fetchone()
            if existing:
                continue  # Report data is richer — don't overwrite

            con.execute("DELETE FROM ads_campaigns WHERE date = CAST(? AS DATE) AND campaign_id = ?",
                        [today_str, cid])
            con.execute("""
                INSERT INTO ads_campaigns
                (date, campaign_id, campaign_name, campaign_type, campaign_status,
                 daily_budget, impressions, clicks, spend, sales, orders, units,
                 division, customer, platform)
                VALUES (CAST(? AS DATE), ?, ?, 'SP', ?, ?, 0, 0, 0, 0, 0, 0, 'golf', 'amazon', 'sp_api')
            """, [today_str, cid, name, state, budget])

            # Also ensure advertising table has a row
            existing_ad = con.execute(
                "SELECT 1 FROM advertising WHERE date = CAST(? AS DATE) AND campaign_id = ? AND impressions > 0",
                [today_str, cid]
            ).fetchone()
            if not existing_ad:
                con.execute("DELETE FROM advertising WHERE date = CAST(? AS DATE) AND campaign_id = ?",
                            [today_str, cid])
                con.execute("""
                    INSERT INTO advertising
                    (date, campaign_id, campaign_name, impressions, clicks, spend, sales, orders, units,
                     division, customer, platform)
                    VALUES (CAST(? AS DATE), ?, ?, 0, 0, 0, 0, 0, 0, 'golf', 'amazon', 'sp_api')
                """, [today_str, cid, name])

            inserted += 1

        con.execute("COMMIT")
    except Exception as e:
        logger.error(f"Ads sync: campaign metadata insert error: {e}")
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    finally:
        con.close()

    logger.info(f"Ads sync: {inserted} campaign metadata rows inserted (of {len(all_campaigns)} total)")


def _get_ads_access_token(creds):
    """Get a fresh access token from Amazon Ads OAuth."""
    import requests as req
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
    return access_token


def _pull_ads_report(creds, report_type_id, columns, start_date, end_date, handler, group_by=None):
    """Pull ads data using v3 reporting API (day-by-day for reliability).

    Creates one v3 async report per day (smaller = faster to complete).
    If a day returns 425 (duplicate), extracts the existing report ID and polls it.
    """
    _pull_ads_report_v3_daywise(creds, report_type_id, columns, start_date, end_date, handler, group_by)


def _pull_ads_report_v3_daywise(creds, report_type_id, columns, start_date, end_date, handler, group_by=None):
    """Create one v3 report per day for reliability. Handles 425 duplicates."""
    import time as _time
    import gzip as gz
    import requests as req
    import re

    access_token = _get_ads_access_token(creds)
    if not access_token:
        return

    headers_create = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": creds["client_id"],
        "Amazon-Advertising-API-Scope": str(creds["profile_id"]),
        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
        "Accept": "application/vnd.createasyncreportrequest.v3+json",
    }
    headers_poll = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": creds["client_id"],
        "Amazon-Advertising-API-Scope": str(creds["profile_id"]),
    }

    from datetime import datetime as _dt
    all_rows = []
    d_start = _dt.strptime(start_date, "%Y-%m-%d")
    d_end = _dt.strptime(end_date, "%Y-%m-%d")
    current = d_start

    while current <= d_end:
        day_str = current.strftime("%Y-%m-%d")
        current += timedelta(days=1)

        # SUMMARY mode doesn't support "date" column — strip it since we
        # inject the date from the request params anyway
        safe_columns = [c for c in columns if c != "date"]

        body = {
            "name": f"{report_type_id}_{day_str}",
            "startDate": day_str,
            "endDate": day_str,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": group_by or ["campaign"],
                "columns": safe_columns,
                "reportTypeId": report_type_id,
                "timeUnit": "SUMMARY",
                "format": "GZIP_JSON",
            }
        }

        try:
            logger.info(f"Ads v3 daywise: creating {report_type_id} for {day_str}...")
            create_resp = req.post(
                "https://advertising-api.amazon.com/reporting/reports",
                headers=headers_create,
                json=body,
                timeout=30,
            )

            report_id = None
            if create_resp.status_code in (200, 202):
                report_id = create_resp.json().get("reportId")
            elif create_resp.status_code == 425:
                # Duplicate — extract existing report ID
                match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',
                                  create_resp.text)
                if match:
                    report_id = match.group(1)
                    logger.info(f"Ads v3 daywise: {day_str} duplicate, using existing {report_id}")
            elif create_resp.status_code == 429:
                logger.warning(f"Ads v3 daywise: throttled on {day_str}, waiting 60s...")
                _time.sleep(60)
                continue  # retry on next iteration
            else:
                logger.warning(f"Ads v3 daywise: create failed {day_str}: {create_resp.status_code} {create_resp.text[:200]}")
                continue

            if not report_id:
                logger.warning(f"Ads v3 daywise: no reportId for {day_str}")
                continue

            # Poll for completion (up to 3 minutes per day report)
            download_url = None
            last_status = "UNKNOWN"
            for attempt in range(18):
                _time.sleep(10)
                poll_resp = req.get(
                    f"https://advertising-api.amazon.com/reporting/reports/{report_id}",
                    headers=headers_poll,
                    timeout=30,
                )
                pdata = poll_resp.json()
                last_status = pdata.get("status", "UNKNOWN")

                if attempt == 0 or attempt % 6 == 0:
                    logger.info(f"Ads v3 daywise: poll {day_str} #{attempt+1}: {last_status}")

                if last_status == "COMPLETED":
                    download_url = pdata.get("url")
                    break
                elif last_status in ("FAILED", "CANCELLED"):
                    logger.warning(f"Ads v3 daywise: {day_str} {last_status}: {pdata.get('failureReason', '')}")
                    break

            if not download_url:
                logger.warning(f"Ads v3 daywise: {day_str} no download (last_status={last_status})")
                continue

            # Download and decompress
            dl_resp = req.get(download_url, timeout=60)
            try:
                day_data = json.loads(gz.decompress(dl_resp.content).decode("utf-8"))
            except Exception:
                try:
                    day_data = json.loads(dl_resp.text)
                except Exception:
                    logger.warning(f"Ads v3 daywise: failed to parse {day_str}")
                    continue

            if isinstance(day_data, str):
                day_data = json.loads(day_data)

            if isinstance(day_data, list):
                # Inject date if not present (SUMMARY mode omits it)
                for row in day_data:
                    if "date" not in row:
                        row["date"] = day_str
                all_rows.extend(day_data)
                logger.info(f"Ads v3 daywise: {report_type_id} {day_str} → {len(day_data)} rows")

            # Small delay between days
            _time.sleep(2)

        except Exception as e:
            logger.error(f"Ads v3 daywise: error on {day_str}: {e}")
            continue

    if all_rows:
        handler(all_rows)
        logger.info(f"Ads v3 daywise: {report_type_id} total — {len(all_rows)} rows")
    else:
        logger.warning(f"Ads v3 daywise: {report_type_id} — 0 rows across date range")


def _pull_ads_report_v2(creds, report_type_id, columns, start_date, end_date, handler):
    """Pull ads data using Amazon Ads v2 reporting API.

    v2 endpoints:
      POST /v2/sp/campaigns/report  (recordType=campaigns)
      POST /v2/sp/keywords/report   (recordType=keywords)
      POST /v2/sp/targets/report    (recordType=targets)

    Returns True if data was successfully pulled and handled, False otherwise.
    """
    import time as _time
    import gzip as gz
    import requests as req

    access_token = _get_ads_access_token(creds)
    if not access_token:
        return False

    # Map v3 reportTypeId → v2 recordType
    record_type_map = {
        "spCampaigns": "campaigns",
        "spTargeting": "targets",
        "spSearchTerm": "keywords",  # v2 uses keywords for search term data
    }
    record_type = record_type_map.get(report_type_id)
    if not record_type:
        logger.error(f"Ads v2: unknown report type mapping for {report_type_id}")
        return False

    # Map v3 column names → v2 metric names
    # v2 uses different metric names than v3
    v2_metrics_map = {
        "campaigns": [
            "impressions", "clicks", "cost", "attributedSales7d",
            "attributedConversions7d", "attributedUnitsOrdered7d",
        ],
        "targets": [
            "impressions", "clicks", "cost", "attributedSales7d",
            "attributedConversions7d", "attributedUnitsOrdered7d",
        ],
        "keywords": [
            "impressions", "clicks", "cost", "attributedSales7d",
            "attributedConversions7d", "attributedUnitsOrdered7d",
        ],
    }

    v2_metrics = v2_metrics_map.get(record_type, [])

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": creds["client_id"],
        "Amazon-Advertising-API-Scope": str(creds["profile_id"]),
        "Content-Type": "application/json",
    }

    # v2 creates one report per date — but we can request a date range via
    # reportDate (single date) or use the segment=query approach.
    # Actually, v2 only supports a single reportDate per request.
    # We'll iterate day-by-day for the date range.
    from datetime import datetime as _dt
    all_rows = []

    # Parse date range
    d_start = _dt.strptime(start_date, "%Y-%m-%d")
    d_end = _dt.strptime(end_date, "%Y-%m-%d")
    current = d_start

    while current <= d_end:
        report_date = current.strftime("%Y%m%d")  # v2 uses YYYYMMDD format
        current += timedelta(days=1)

        body = {
            "reportDate": report_date,
            "metrics": ",".join(v2_metrics),
        }

        # Add segment for search terms
        if report_type_id == "spSearchTerm":
            body["segment"] = "query"

        url = f"https://advertising-api.amazon.com/v2/sp/{record_type}/report"
        logger.info(f"Ads v2: creating {record_type} report for {report_date}...")

        try:
            create_resp = req.post(url, headers=headers, json=body, timeout=30)

            if create_resp.status_code == 429:
                logger.warning(f"Ads v2: throttled on {report_date}, waiting 60s...")
                _time.sleep(60)
                create_resp = req.post(url, headers=headers, json=body, timeout=30)

            if create_resp.status_code not in (200, 202):
                logger.warning(f"Ads v2: create report failed for {record_type} {report_date}: "
                             f"{create_resp.status_code} {create_resp.text[:300]}")
                continue

            create_data = create_resp.json()
            report_id = create_data.get("reportId")
            if not report_id:
                logger.warning(f"Ads v2: no reportId for {record_type} {report_date}: {create_data}")
                continue

            # Poll for completion — v2 reports typically complete in <60 seconds
            download_url = None
            for attempt in range(30):  # up to 5 minutes per report
                _time.sleep(10)
                poll_resp = req.get(
                    f"https://advertising-api.amazon.com/v2/reports/{report_id}",
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Amazon-Advertising-API-ClientId": creds["client_id"],
                        "Amazon-Advertising-API-Scope": str(creds["profile_id"]),
                    },
                    timeout=30,
                )
                poll_data = poll_resp.json()
                status = poll_data.get("status")

                if attempt == 0:
                    logger.info(f"Ads v2: poll {record_type} {report_date} #{attempt+1}: {status} "
                              f"resp={str(poll_data)[:200]}")
                elif attempt % 5 == 0:
                    logger.info(f"Ads v2: poll {record_type} {report_date} #{attempt+1}: {status}")

                if status == "SUCCESS":
                    download_url = poll_data.get("location")
                    break
                elif status in ("FAILURE", "CANCELLED"):
                    logger.warning(f"Ads v2: {record_type} {report_date} {status}: "
                                 f"{poll_data.get('statusDetails', '')}")
                    break

            if not download_url:
                if status != "SUCCESS":
                    logger.warning(f"Ads v2: {record_type} {report_date} did not complete (last status={status})")
                continue

            # Download — v2 returns gzipped JSON
            dl_resp = req.get(download_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Amazon-Advertising-API-ClientId": creds["client_id"],
                "Amazon-Advertising-API-Scope": str(creds["profile_id"]),
            }, timeout=60)

            try:
                day_data = json.loads(gz.decompress(dl_resp.content).decode("utf-8"))
            except Exception:
                try:
                    day_data = json.loads(dl_resp.text)
                except Exception:
                    logger.warning(f"Ads v2: failed to parse {record_type} {report_date} response")
                    continue

            if isinstance(day_data, str):
                day_data = json.loads(day_data)

            if isinstance(day_data, list):
                # v2 doesn't include date in each row — inject it
                date_formatted = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:8]}"
                for row in day_data:
                    row["date"] = date_formatted
                    # Map v2 field names → v3 field names for handler compatibility
                    if "cost" in row and "spend" not in row:
                        row["spend"] = row["cost"]
                    if "attributedSales7d" in row:
                        row["sales7d"] = row["attributedSales7d"]
                    if "attributedConversions7d" in row:
                        row["purchases7d"] = row["attributedConversions7d"]
                    if "attributedUnitsOrdered7d" in row:
                        row["unitsSoldClicks7d"] = row["attributedUnitsOrdered7d"]
                    # Search term data from segment=query
                    if "query" in row and "searchTerm" not in row:
                        row["searchTerm"] = row["query"]
                all_rows.extend(day_data)
                logger.info(f"Ads v2: {record_type} {report_date} → {len(day_data)} rows")
            else:
                logger.warning(f"Ads v2: {record_type} {report_date} unexpected data type: {type(day_data)}")

            # Small delay between date requests to avoid throttling
            _time.sleep(2)

        except Exception as e:
            logger.error(f"Ads v2: error pulling {record_type} {report_date}: {e}")
            continue

    if all_rows:
        handler(all_rows)
        logger.info(f"Ads v2: {report_type_id} total — {len(all_rows)} rows processed across date range")
        return True
    else:
        logger.warning(f"Ads v2: {report_type_id} — 0 rows across entire date range")
        return False


def _pull_ads_report_v3(creds, report_type_id, columns, start_date, end_date, handler, group_by=None):
    """Fallback: Create, poll, download a single Amazon Ads v3 async report."""
    import time as _time
    import gzip as gz
    import requests as req

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
        logger.info(f"Ads v3: creating {report_type_id} report ({start_date} to {end_date})...")

        access_token = _get_ads_access_token(creds)
        if not access_token:
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
        logger.info(f"Ads v3: create report response: {create_resp.status_code} {create_resp.text[:500]}")

        if create_resp.status_code not in (200, 202):
            logger.error(f"Ads v3: create report failed for {report_type_id}: {create_resp.status_code} {create_resp.text[:500]}")
            return

        create_data = create_resp.json()
        report_id = create_data.get("reportId")

        if not report_id:
            logger.error(f"Ads v3: no reportId returned for {report_type_id}, response: {create_data}")
            return

        # Poll for completion — reduced to 3 minutes (v3 often stalls forever)
        download_url = None
        for attempt in range(18):
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
            logger.info(f"Ads v3: poll {report_type_id} attempt {attempt+1}: {status}{extra}")

            if status == "COMPLETED":
                download_url = poll_data.get("url")
                break
            elif status in ("FAILED", "CANCELLED"):
                logger.error(f"Ads v3: {report_type_id} report {status}: {poll_data.get('failureReason', '')}")
                return

        if not download_url:
            logger.error(f"Ads v3: {report_type_id} report timed out after 3 min")
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
        logger.info(f"Ads v3: {report_type_id} — {len(data) if isinstance(data, list) else 'N/A'} rows processed")

    except Exception as e:
        logger.error(f"Ads v3 error ({report_type_id}): {e}")


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
        for idx, row in enumerate(data):
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

                if not date:
                    continue
                # If campaignId missing (old reports without dimension columns),
                # use a synthetic ID so data still gets inserted
                if not campaign_id:
                    campaign_id = f"unknown_{idx}"
                    campaign_name = campaign_name or f"Campaign {idx}"

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
        for idx, row in enumerate(data):
            try:
                date = row.get("date", "")
                keyword_id = str(row.get("keywordId", row.get("targetId", "")))
                if not date:
                    continue
                if not keyword_id:
                    keyword_id = f"unknown_{idx}"

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
        for idx, row in enumerate(data):
            try:
                date = row.get("date", "")
                search_term = row.get("searchTerm", "") or f"unknown_{idx}"
                campaign_id = str(row.get("campaignId", ""))
                if not date:
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


def ads_backfill_30days(days=90):
    """Create v3 reports for the last N days and poll until they complete.

    Amazon Ads v3 API has a 60-day max per report, so for >60 days we split
    into multiple chunks (e.g. 90 days = days 31-90 + days 1-30).

    This is a BLOCKING call — it polls every 60 seconds for up to 60 minutes
    per chunk. Designed to be called from a background thread/executor.

    Returns a dict with status info for each report type and chunk.
    """
    import time as _time
    import gzip as gz
    import requests as req
    import re

    ads_creds = _load_ads_credentials()
    if not ads_creds:
        return {"error": "No Amazon Ads credentials configured"}

    _ensure_ads_tables()

    profile_id = ads_creds.get("profile_id", "")
    if not profile_id:
        return {"error": "No profile_id configured"}

    access_token = _get_ads_access_token(ads_creds)
    if not access_token:
        return {"error": "Failed to get access token"}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": ads_creds["client_id"],
        "Amazon-Advertising-API-Scope": str(profile_id),
    }

    today = datetime.now(ZoneInfo("America/Chicago"))

    # Split into 60-day chunks (API limit)
    # E.g. 90 days → chunk1: day 91 to day 31, chunk2: day 30 to day 1
    chunks = []
    remaining_days = days
    offset = 1  # start from yesterday
    while remaining_days > 0:
        chunk_size = min(remaining_days, 60)
        chunk_end = (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        chunk_start = (today - timedelta(days=offset + chunk_size - 1)).strftime("%Y-%m-%d")
        chunks.append((chunk_start, chunk_end))
        offset += chunk_size
        remaining_days -= chunk_size

    # Reverse so we process oldest chunk first
    chunks.reverse()

    report_configs = [
        {
            "report_type": "spCampaigns",
            "columns": ["date", "campaignId", "campaignName", "campaignStatus",
                        "campaignBudgetAmount",
                        "impressions", "clicks", "spend",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["campaign"],
            "handler": _handle_campaign_report,
        },
        {
            "report_type": "spTargeting",
            "columns": ["date", "campaignId", "campaignName",
                        "adGroupId", "adGroupName",
                        "keywordId", "keyword", "matchType",
                        "impressions", "clicks", "cost",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["targeting"],
            "handler": _handle_targeting_report,
        },
        {
            "report_type": "spSearchTerm",
            "columns": ["date", "campaignId", "campaignName",
                        "adGroupName", "keyword", "matchType", "searchTerm",
                        "impressions", "clicks", "spend",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["searchTerm"],
            "handler": _handle_search_term_report,
        },
    ]

    headers_create = {
        **headers,
        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
        "Accept": "application/vnd.createasyncreportrequest.v3+json",
    }

    all_results = {}
    total_rows = 0

    for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
        chunk_label = f"chunk{chunk_idx + 1} ({chunk_start} to {chunk_end})"
        logger.info(f"Backfill: starting {chunk_label}")

        # Create reports for this chunk
        created = {}
        chunk_results = {}

        for cfg in report_configs:
            rt = cfg["report_type"]
            key = f"{rt}_{chunk_label}"
            body = {
                "name": f"backfill_{rt}_{chunk_start}_{chunk_end}",
                "startDate": chunk_start,
                "endDate": chunk_end,
                "configuration": {
                    "adProduct": "SPONSORED_PRODUCTS",
                    "groupBy": cfg["group_by"],
                    "columns": cfg["columns"],
                    "reportTypeId": rt,
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                }
            }

            try:
                resp = req.post(
                    "https://advertising-api.amazon.com/reporting/reports",
                    headers=headers_create,
                    json=body,
                    timeout=30,
                )

                report_id = None
                if resp.status_code in (200, 202):
                    report_id = resp.json().get("reportId")
                elif resp.status_code == 425:
                    match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', resp.text)
                    if match:
                        report_id = match.group(1)
                        logger.info(f"Backfill: {rt} duplicate, using existing {report_id}")
                else:
                    chunk_results[key] = {"error": f"Create failed: {resp.status_code} {resp.text[:200]}"}
                    logger.warning(f"Backfill: create {rt} failed: {resp.status_code}")

                if report_id:
                    created[rt] = {"report_id": report_id, "handler": cfg["handler"], "key": key}
                    chunk_results[key] = {"report_id": report_id, "status": "CREATED"}
                    logger.info(f"Backfill: created {rt} report {report_id} for {chunk_label}")
            except Exception as e:
                chunk_results[key] = {"error": str(e)}
                logger.error(f"Backfill: error creating {rt}: {e}")

        if not created:
            all_results.update(chunk_results)
            continue

        # Poll until all reports in this chunk complete (up to 60 min)
        max_polls = 60
        poll_interval = 60
        remaining = dict(created)

        for poll_num in range(max_polls):
            if not remaining:
                break

            _time.sleep(poll_interval)

            # Refresh token every 20 minutes
            if poll_num > 0 and poll_num % 20 == 0:
                new_token = _get_ads_access_token(ads_creds)
                if new_token:
                    access_token = new_token
                    headers["Authorization"] = f"Bearer {access_token}"
                    headers_create["Authorization"] = f"Bearer {access_token}"
                    logger.info("Backfill: refreshed access token")

            completed_this_round = []
            for rt, info in remaining.items():
                rid = info["report_id"]
                handler = info["handler"]
                key = info["key"]

                try:
                    poll_resp = req.get(
                        f"https://advertising-api.amazon.com/reporting/reports/{rid}",
                        headers=headers,
                        timeout=30,
                    )
                    pdata = poll_resp.json()
                    status = pdata.get("status", "UNKNOWN")

                    if status == "COMPLETED":
                        download_url = pdata.get("url")
                        if download_url:
                            dl_resp = req.get(download_url, timeout=120)
                            try:
                                data = json.loads(gz.decompress(dl_resp.content).decode("utf-8"))
                            except Exception:
                                data = json.loads(dl_resp.text)
                            if isinstance(data, str):
                                data = json.loads(data)

                            handler(data)
                            row_count = len(data) if isinstance(data, list) else 0
                            total_rows += row_count
                            chunk_results[key] = {
                                "report_id": rid,
                                "status": "INGESTED",
                                "rows": row_count,
                                "poll_minutes": (poll_num + 1),
                            }
                            logger.info(f"Backfill: {rt} {chunk_label} completed — {row_count} rows "
                                       f"(after {poll_num + 1} min)")
                        completed_this_round.append(rt)

                    elif status in ("FAILED", "CANCELLED"):
                        chunk_results[key] = {"report_id": rid, "status": status}
                        completed_this_round.append(rt)
                        logger.warning(f"Backfill: {rt} report {status}")
                    else:
                        chunk_results[key]["status"] = f"PENDING (poll #{poll_num + 1})"
                        if poll_num % 5 == 0:  # Log every 5 min to reduce noise
                            logger.info(f"Backfill: {rt} still {status} (poll #{poll_num + 1})")

                except Exception as e:
                    logger.error(f"Backfill: error polling {rt}: {e}")

            for rt in completed_this_round:
                del remaining[rt]

        # Mark timed-out reports
        for rt, info in remaining.items():
            chunk_results[info["key"]]["status"] = "TIMEOUT (60 min)"
            logger.warning(f"Backfill: {rt} {chunk_label} timed out")

        all_results.update(chunk_results)

    full_start = chunks[0][0] if chunks else "?"
    full_end = chunks[-1][1] if chunks else "?"
    logger.info(f"Backfill complete: {days} days, {len(chunks)} chunks, {total_rows} total rows")
    return {
        "status": "backfill_complete",
        "days": days,
        "chunks": len(chunks),
        "date_range": f"{full_start} to {full_end}",
        "total_rows": total_rows,
        "results": all_results,
    }


def ads_backfill_range(start_date: str, end_date: str):
    """Create v3 reports for an exact date range and poll until complete.

    Max 60 days per call (Amazon API limit). BLOCKING — polls up to 60 min.
    """
    import time as _time
    import gzip as gz
    import requests as req
    import re

    ads_creds = _load_ads_credentials()
    if not ads_creds:
        return {"error": "No Amazon Ads credentials configured"}

    _ensure_ads_tables()

    profile_id = ads_creds.get("profile_id", "")
    if not profile_id:
        return {"error": "No profile_id configured"}

    access_token = _get_ads_access_token(ads_creds)
    if not access_token:
        return {"error": "Failed to get access token"}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": ads_creds["client_id"],
        "Amazon-Advertising-API-Scope": str(profile_id),
    }

    report_configs = [
        {
            "report_type": "spCampaigns",
            "columns": ["date", "campaignId", "campaignName", "campaignStatus",
                        "campaignBudgetAmount",
                        "impressions", "clicks", "spend",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["campaign"],
            "handler": _handle_campaign_report,
        },
        {
            "report_type": "spTargeting",
            "columns": ["date", "campaignId", "campaignName",
                        "adGroupId", "adGroupName",
                        "keywordId", "keyword", "matchType",
                        "impressions", "clicks", "cost",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["targeting"],
            "handler": _handle_targeting_report,
        },
        {
            "report_type": "spSearchTerm",
            "columns": ["date", "campaignId", "campaignName",
                        "adGroupName", "keyword", "matchType", "searchTerm",
                        "impressions", "clicks", "spend",
                        "purchases7d", "unitsSoldClicks7d", "sales7d"],
            "group_by": ["searchTerm"],
            "handler": _handle_search_term_report,
        },
    ]

    headers_create = {
        **headers,
        "Content-Type": "application/vnd.createasyncreportrequest.v3+json",
        "Accept": "application/vnd.createasyncreportrequest.v3+json",
    }

    created = {}
    results = {}
    total_rows = 0

    logger.info(f"Backfill range: creating reports for {start_date} to {end_date}")

    for cfg in report_configs:
        rt = cfg["report_type"]
        body = {
            "name": f"backfill_range_{rt}_{start_date}_{end_date}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": cfg["group_by"],
                "columns": cfg["columns"],
                "reportTypeId": rt,
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            }
        }

        try:
            resp = req.post(
                "https://advertising-api.amazon.com/reporting/reports",
                headers=headers_create,
                json=body,
                timeout=30,
            )

            report_id = None
            if resp.status_code in (200, 202):
                report_id = resp.json().get("reportId")
            elif resp.status_code == 425:
                match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', resp.text)
                if match:
                    report_id = match.group(1)
                    logger.info(f"Backfill range: {rt} duplicate, using existing {report_id}")
            else:
                results[rt] = {"error": f"Create failed: {resp.status_code} {resp.text[:200]}"}
                logger.warning(f"Backfill range: create {rt} failed: {resp.status_code}")

            if report_id:
                created[rt] = {"report_id": report_id, "handler": cfg["handler"]}
                results[rt] = {"report_id": report_id, "status": "CREATED"}
                logger.info(f"Backfill range: created {rt} report {report_id}")
        except Exception as e:
            results[rt] = {"error": str(e)}
            logger.error(f"Backfill range: error creating {rt}: {e}")

    if not created:
        return {"status": "no_reports_created", "date_range": f"{start_date} to {end_date}", "results": results}

    # Poll every 60 seconds for up to 60 minutes
    max_polls = 60
    remaining = dict(created)

    for poll_num in range(max_polls):
        if not remaining:
            break

        _time.sleep(60)

        if poll_num > 0 and poll_num % 20 == 0:
            new_token = _get_ads_access_token(ads_creds)
            if new_token:
                access_token = new_token
                headers["Authorization"] = f"Bearer {access_token}"

        completed_this_round = []
        for rt, info in remaining.items():
            rid = info["report_id"]
            handler = info["handler"]

            try:
                poll_resp = req.get(
                    f"https://advertising-api.amazon.com/reporting/reports/{rid}",
                    headers=headers,
                    timeout=30,
                )
                pdata = poll_resp.json()
                status = pdata.get("status", "UNKNOWN")

                if status == "COMPLETED":
                    download_url = pdata.get("url")
                    if download_url:
                        dl_resp = req.get(download_url, timeout=120)
                        try:
                            data = json.loads(gz.decompress(dl_resp.content).decode("utf-8"))
                        except Exception:
                            data = json.loads(dl_resp.text)
                        if isinstance(data, str):
                            data = json.loads(data)

                        handler(data)
                        row_count = len(data) if isinstance(data, list) else 0
                        total_rows += row_count
                        results[rt] = {
                            "report_id": rid, "status": "INGESTED",
                            "rows": row_count, "poll_minutes": poll_num + 1,
                        }
                        logger.info(f"Backfill range: {rt} ingested — {row_count} rows (after {poll_num + 1} min)")
                    completed_this_round.append(rt)

                elif status in ("FAILED", "CANCELLED"):
                    results[rt] = {"report_id": rid, "status": status}
                    completed_this_round.append(rt)
                else:
                    results[rt]["status"] = f"PENDING (poll #{poll_num + 1})"
                    if poll_num % 5 == 0:
                        logger.info(f"Backfill range: {rt} still {status} (poll #{poll_num + 1})")

            except Exception as e:
                logger.error(f"Backfill range: error polling {rt}: {e}")

        for rt in completed_this_round:
            del remaining[rt]

    for rt in remaining:
        results[rt]["status"] = "TIMEOUT (60 min)"

    logger.info(f"Backfill range complete: {start_date} to {end_date}, {total_rows} total rows")
    return {
        "status": "backfill_complete",
        "date_range": f"{start_date} to {end_date}",
        "total_rows": total_rows,
        "results": results,
    }


