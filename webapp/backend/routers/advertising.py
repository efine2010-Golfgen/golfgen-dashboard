"""Amazon Advertising routes."""
import os
import logging
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
from fastapi import APIRouter, Query, Request
from fastapi.responses import RedirectResponse, HTMLResponse
import requests as http_requests

from core.config import DB_PATH, TIMEZONE
from core.database import get_db, get_db_rw
from core.hierarchy import hierarchy_filter
from services.ads_api import _sync_ads_data, _load_ads_credentials

logger = logging.getLogger("golfgen")
router = APIRouter()

# ── App Settings Helpers ─────────────────────────────────────────
def _get_setting(key: str) -> str | None:
    """Read a value from app_settings table."""
    try:
        con = get_db()
        row = con.execute("SELECT value FROM app_settings WHERE key = ?", [key]).fetchone()
        con.close()
        return row[0] if row else None
    except Exception:
        return None

def _set_setting(key: str, value: str):
    """Upsert a value into app_settings table."""
    con = get_db_rw()
    try:
        existing = con.execute("SELECT 1 FROM app_settings WHERE key = ?", [key]).fetchone()
        if existing:
            con.execute("UPDATE app_settings SET value = ?, updated_at = CURRENT_TIMESTAMP WHERE key = ?", [value, key])
        else:
            con.execute("INSERT INTO app_settings (key, value) VALUES (?, ?)", [key, value])
    finally:
        con.close()


# ── Ads API OAuth Flow ───────────────────────────────────────────
# These endpoints handle the Login with Amazon OAuth dance to obtain
# a refresh token for the Amazon Ads API.

AMAZON_AUTH_URL = "https://www.amazon.com/ap/oa"
AMAZON_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

def _get_ads_client_id() -> str:
    return (
        os.environ.get("ADS_API_CLIENT_ID")
        or os.environ.get("AMAZON_ADS_CLIENT_ID")
        or _get_setting("ads_client_id")
        or ""
    )

def _get_ads_client_secret() -> str:
    return (
        os.environ.get("ADS_API_CLIENT_SECRET")
        or os.environ.get("AMAZON_ADS_CLIENT_SECRET")
        or _get_setting("ads_client_secret")
        or ""
    )

def _get_ads_redirect_uri(request: Request) -> str:
    """Build the OAuth redirect URI from the current request's base URL."""
    # Railway terminates SSL at the proxy — request.base_url is http://
    # Force https:// for production (Amazon requires exact match)
    base = str(request.base_url).rstrip("/")
    if base.startswith("http://") and "localhost" not in base and "127.0.0.1" not in base:
        base = "https://" + base[7:]
    return f"{base}/api/ads/auth/callback"


@router.get("/api/ads/auth/start")
def ads_auth_start(request: Request):
    """Redirect the user to Amazon's OAuth consent page to authorize Ads API."""
    client_id = _get_ads_client_id()
    if not client_id:
        return HTMLResponse(
            "<h2>Missing Ads API Client ID</h2>"
            "<p>Set <code>ADS_API_CLIENT_ID</code> environment variable on Railway first.</p>",
            status_code=400,
        )

    redirect_uri = _get_ads_redirect_uri(request)
    auth_url = (
        f"{AMAZON_AUTH_URL}"
        f"?client_id={client_id}"
        f"&scope=advertising::campaign_management"
        f"&response_type=code"
        f"&redirect_uri={redirect_uri}"
    )
    logger.info(f"Ads OAuth: redirecting to Amazon (redirect_uri={redirect_uri})")
    return RedirectResponse(url=auth_url)


@router.get("/api/ads/auth/callback")
def ads_auth_callback(request: Request, code: str = Query(None), error: str = Query(None)):
    """Handle the OAuth callback from Amazon — exchange code for tokens."""
    if error:
        return HTMLResponse(
            f"<h2>Authorization Failed</h2><p>Amazon returned error: {error}</p>",
            status_code=400,
        )
    if not code:
        return HTMLResponse(
            "<h2>No Authorization Code</h2><p>Amazon did not return an authorization code.</p>",
            status_code=400,
        )

    client_id = _get_ads_client_id()
    client_secret = _get_ads_client_secret()
    redirect_uri = _get_ads_redirect_uri(request)

    if not client_id or not client_secret:
        return HTMLResponse(
            "<h2>Missing Credentials</h2>"
            "<p>Set <code>ADS_API_CLIENT_ID</code> and <code>ADS_API_CLIENT_SECRET</code> on Railway.</p>",
            status_code=400,
        )

    # Exchange authorization code for tokens
    try:
        token_resp = http_requests.post(AMAZON_TOKEN_URL, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        }, timeout=30)
        token_data = token_resp.json()

        if "error" in token_data:
            logger.error(f"Ads OAuth token error: {token_data}")
            return HTMLResponse(
                f"<h2>Token Exchange Failed</h2>"
                f"<p>Error: {token_data.get('error_description', token_data.get('error'))}</p>",
                status_code=400,
            )

        refresh_token = token_data.get("refresh_token", "")
        access_token = token_data.get("access_token", "")

        if not refresh_token:
            return HTMLResponse(
                "<h2>No Refresh Token</h2><p>Amazon did not return a refresh token.</p>",
                status_code=400,
            )

        # Save refresh token to DB
        _set_setting("ads_refresh_token", refresh_token)
        logger.info(f"Ads OAuth: refresh token obtained and saved (len={len(refresh_token)})")

        # Try to discover the advertising profile ID
        profile_msg = ""
        try:
            from ad_api.api import Profiles
            creds = {
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
                "profile_id": "",
            }
            result = Profiles(credentials=creds).get_profiles()
            profiles = result.payload or []
            us_profile = None
            for p in profiles:
                if p.get("accountInfo", {}).get("marketplaceStringId") == "ATVPDKIKX0DER":
                    us_profile = p
                    break
            if not us_profile and profiles:
                us_profile = profiles[0]

            if us_profile:
                profile_id = str(us_profile.get("profileId", ""))
                account_name = us_profile.get("accountInfo", {}).get("name", "")
                _set_setting("ads_profile_id", profile_id)
                logger.info(f"Ads OAuth: discovered profile_id={profile_id} ({account_name})")
                profile_msg = f"<p>✅ Discovered Ads Profile: <strong>{account_name}</strong> (ID: {profile_id})</p>"

                # Show all profiles found
                if len(profiles) > 1:
                    profile_msg += "<p>All profiles found:</p><ul>"
                    for p in profiles:
                        pid = p.get("profileId")
                        pname = p.get("accountInfo", {}).get("name", "")
                        pmkt = p.get("accountInfo", {}).get("marketplaceStringId", "")
                        profile_msg += f"<li>{pname} — {pmkt} (ID: {pid})</li>"
                    profile_msg += "</ul>"
            else:
                profile_msg = "<p>⚠️ No advertising profiles found. You may need to create one in the Amazon Ads Console.</p>"
        except Exception as e:
            profile_msg = f"<p>⚠️ Could not auto-discover profile: {e}</p>"
            logger.warning(f"Ads OAuth: profile discovery failed: {e}")

        return HTMLResponse(
            "<html><head><style>"
            "body { font-family: -apple-system, sans-serif; max-width: 600px; margin: 60px auto; "
            "background: #0f172a; color: #e2e8f0; padding: 20px; }"
            "h2 { color: #2ECFAA; } code { background: #1e293b; padding: 2px 6px; border-radius: 4px; }"
            ".card { background: #1e293b; border-radius: 12px; padding: 20px; margin: 16px 0; "
            "border: 1px solid #334155; }"
            "a { color: #2ECFAA; }"
            "</style></head><body>"
            "<h2>✅ Amazon Ads API Connected!</h2>"
            "<div class='card'>"
            "<p>Refresh token has been saved to the database.</p>"
            f"{profile_msg}"
            "<p>The ads sync will start pulling data on the next scheduled run (every 2 hours), "
            "or you can trigger it manually from the System page.</p>"
            f"<p><a href='/'>← Return to Dashboard</a></p>"
            "</div>"
            "</body></html>",
            status_code=200,
        )

    except Exception as e:
        logger.error(f"Ads OAuth callback error: {e}")
        return HTMLResponse(
            f"<h2>Error</h2><p>{str(e)}</p>",
            status_code=500,
        )


@router.get("/api/ads/auth/status")
def ads_auth_status():
    """Check the current Ads API connection status."""
    client_id = _get_ads_client_id()
    client_secret = _get_ads_client_secret()
    refresh_token = (
        os.environ.get("ADS_API_REFRESH_TOKEN")
        or os.environ.get("AMAZON_ADS_REFRESH_TOKEN")
        or _get_setting("ads_refresh_token")
        or ""
    )
    profile_id = (
        os.environ.get("ADS_API_PROFILE_ID")
        or os.environ.get("AMAZON_ADS_PROFILE_ID")
        or _get_setting("ads_profile_id")
        or ""
    )

    has_client_id = bool(client_id)
    has_client_secret = bool(client_secret)
    has_refresh_token = bool(refresh_token)
    has_profile_id = bool(profile_id)

    connected = has_client_id and has_client_secret and has_refresh_token

    # Check if there's actual ads data in the DB
    data_count = 0
    try:
        con = get_db()
        row = con.execute("SELECT COUNT(*) FROM advertising").fetchone()
        data_count = row[0] if row else 0
        con.close()
    except Exception:
        pass

    return {
        "connected": connected,
        "hasClientId": has_client_id,
        "hasClientSecret": has_client_secret,
        "hasRefreshToken": has_refresh_token,
        "hasProfileId": has_profile_id,
        "profileId": profile_id if has_profile_id else None,
        "dataRows": data_count,
        "clientIdPrefix": client_id[:12] + "..." if client_id else None,
    }


# ── Utility Functions ────────────────────────────────────────────


def fmt_date(d) -> str:
    """Safely format a date value to string."""
    if isinstance(d, str):
        return d
    if hasattr(d, "strftime"):
        return d.strftime("%Y-%m-%d")
    return str(d)


def _safe_ads_query(con, query, params=None):
    """Run an ads query, returning empty list if tables don't exist yet."""
    try:
        return con.execute(query, params or []).fetchall()
    except Exception as e:
        logger.warning(f"Ads query error: {e}")
        return []


# ── Ads: Summary & Daily Performance ────────────────────────────


@router.get("/api/ads/summary")
def ads_summary(days: int = Query(30), division: Optional[str] = None, customer: Optional[str] = None, platform: Optional[str] = None, marketplace: Optional[str] = None):
    """Ads KPIs: total spend, sales, ACOS, ROAS, TACOS, CPC, CTR."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")

    div_cust_sql, div_cust_params = hierarchy_filter(division, customer, platform, marketplace)

    row = _safe_ads_query(con, f"""
        SELECT
            COALESCE(SUM(spend), 0) AS spend,
            COALESCE(SUM(sales), 0) AS ad_sales,
            COALESCE(SUM(impressions), 0) AS impressions,
            COALESCE(SUM(clicks), 0) AS clicks,
            COALESCE(SUM(orders), 0) AS ad_orders,
            COALESCE(SUM(units), 0) AS ad_units
        FROM advertising
        WHERE date >= ?{div_cust_sql}
    """, [cutoff] + div_cust_params)

    if not row or not row[0]:
        con.close()
        return {
            "days": days, "connected": False,
            "spend": 0, "adSales": 0, "impressions": 0, "clicks": 0,
            "orders": 0, "units": 0, "acos": 0, "roas": 0, "tacos": 0,
            "cpc": 0, "ctr": 0, "cvr": 0,
        }

    spend, ad_sales, impressions, clicks, orders, units = row[0]

    # Get total organic revenue for TACOS
    org_row = con.execute(f"""
        SELECT COALESCE(SUM(ordered_product_sales), 0)
        FROM daily_sales
        WHERE date >= ? AND asin = 'ALL'{div_cust_sql}
    """, [cutoff] + div_cust_params).fetchone()
    total_revenue = org_row[0] if org_row else 0

    con.close()

    acos = round(spend / ad_sales * 100, 2) if ad_sales > 0 else 0
    roas = round(ad_sales / spend, 2) if spend > 0 else 0
    tacos = round(spend / total_revenue * 100, 2) if total_revenue > 0 else 0
    cpc = round(spend / clicks, 2) if clicks > 0 else 0
    ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
    cvr = round(orders / clicks * 100, 2) if clicks > 0 else 0

    return {
        "days": days, "connected": True,
        "spend": round(spend, 2),
        "adSales": round(ad_sales, 2),
        "impressions": impressions,
        "clicks": clicks,
        "orders": orders,
        "units": units,
        "acos": acos,
        "roas": roas,
        "tacos": tacos,
        "cpc": cpc,
        "ctr": ctr,
        "cvr": cvr,
    }


@router.get("/api/ads/daily")
def ads_daily(days: int = Query(30), division: Optional[str] = None, customer: Optional[str] = None, platform: Optional[str] = None, marketplace: Optional[str] = None):
    """Daily ads performance time series."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")

    div_cust_sql, div_cust_params = hierarchy_filter(division, customer, platform, marketplace)

    rows = _safe_ads_query(con, f"""
        SELECT date,
               COALESCE(SUM(spend), 0),
               COALESCE(SUM(sales), 0),
               COALESCE(SUM(impressions), 0),
               COALESCE(SUM(clicks), 0),
               COALESCE(SUM(orders), 0),
               0, 0, 0, 0, 0
        FROM advertising
        WHERE date >= ?{div_cust_sql}
        GROUP BY date
        ORDER BY date
    """, [cutoff] + div_cust_params)

    con.close()

    data = []
    for r in rows:
        data.append({
            "date": fmt_date(r[0]),
            "spend": round(r[1], 2),
            "adSales": round(r[2], 2),
            "impressions": r[3],
            "clicks": r[4],
            "orders": r[5],
            "acos": r[6],
            "roas": r[7],
            "tacos": r[8],
            "cpc": r[9],
            "ctr": r[10],
        })

    return {"days": days, "data": data}


# ── Ads: Campaign & Keyword Analysis ────────────────────────────


@router.get("/api/ads/campaigns")
def ads_campaigns(days: int = Query(30), division: Optional[str] = None, customer: Optional[str] = None, platform: Optional[str] = None, marketplace: Optional[str] = None):
    """Campaign-level performance."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)

    rows = _safe_ads_query(con, f"""
        SELECT
            campaign_id,
            MAX(campaign_name) AS campaign_name,
            MAX(campaign_type) AS campaign_type,
            MAX(campaign_status) AS campaign_status,
            MAX(daily_budget) AS daily_budget,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_campaigns
        WHERE date >= ?{hf}
        GROUP BY campaign_id
        ORDER BY SUM(spend) DESC
    """, [cutoff] + hp)

    con.close()

    campaigns = []
    for r in rows:
        impressions, clicks, spend, sales = r[5], r[6], r[7], r[8]
        orders, units = r[9], r[10]

        campaigns.append({
            "campaignId": r[0],
            "campaignName": r[1],
            "campaignType": r[2],
            "status": r[3],
            "dailyBudget": round(r[4] or 0, 2),
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
            "cvr": round(orders / clicks * 100, 2) if clicks > 0 else 0,
        })

    return {"days": days, "campaigns": campaigns}


@router.get("/api/ads/keywords")
def ads_keywords(days: int = Query(30), sort: str = Query("spend"), limit: int = Query(50), division: Optional[str] = None, customer: Optional[str] = None, platform: Optional[str] = None, marketplace: Optional[str] = None):
    """Top keywords by spend, sales, or ACOS."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)

    rows = _safe_ads_query(con, f"""
        SELECT
            keyword_text,
            MAX(match_type) AS match_type,
            MAX(campaign_name) AS campaign_name,
            MAX(ad_group_name) AS ad_group_name,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_keywords
        WHERE date >= ?{hf}
        GROUP BY keyword_text
        ORDER BY SUM(spend) DESC
        LIMIT ?
    """, [cutoff] + hp + [limit])

    con.close()

    keywords = []
    for r in rows:
        impressions, clicks, spend, sales = r[4], r[5], r[6], r[7]
        orders, units = r[8], r[9]

        keywords.append({
            "keyword": r[0],
            "matchType": r[1],
            "campaignName": r[2],
            "adGroupName": r[3],
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
            "cvr": round(orders / clicks * 100, 2) if clicks > 0 else 0,
        })

    return {"days": days, "keywords": keywords}


@router.get("/api/ads/search-terms")
def ads_search_terms(days: int = Query(30), limit: int = Query(50), division: Optional[str] = None, customer: Optional[str] = None, platform: Optional[str] = None, marketplace: Optional[str] = None):
    """Top search terms by spend."""
    con = get_db()
    cutoff = (datetime.now(ZoneInfo("America/Chicago")) - timedelta(days=days)).strftime("%Y-%m-%d")
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)

    rows = _safe_ads_query(con, f"""
        SELECT
            search_term,
            MAX(keyword_text) AS keyword,
            MAX(match_type) AS match_type,
            MAX(campaign_name) AS campaign_name,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(spend) AS spend,
            SUM(sales) AS sales,
            SUM(orders) AS orders,
            SUM(units) AS units
        FROM ads_search_terms
        WHERE date >= ?{hf}
        GROUP BY search_term
        ORDER BY SUM(spend) DESC
        LIMIT ?
    """, [cutoff] + hp + [limit])

    con.close()

    terms = []
    for r in rows:
        impressions, clicks, spend, sales = r[4], r[5], r[6], r[7]
        orders, units = r[8], r[9]

        terms.append({
            "searchTerm": r[0],
            "keyword": r[1],
            "matchType": r[2],
            "campaignName": r[3],
            "impressions": impressions,
            "clicks": clicks,
            "spend": round(spend, 2),
            "sales": round(sales, 2),
            "orders": orders,
            "units": units,
            "acos": round(spend / sales * 100, 2) if sales > 0 else 0,
            "roas": round(sales / spend, 2) if spend > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "ctr": round(clicks / impressions * 100, 2) if impressions > 0 else 0,
        })

    return {"days": days, "searchTerms": terms}


@router.get("/api/ads/negative-keywords")
def ads_negative_keywords(division: Optional[str] = None, customer: Optional[str] = None, platform: Optional[str] = None, marketplace: Optional[str] = None):
    """List all negative keywords."""
    con = get_db()
    hf, hp = hierarchy_filter(division, customer, platform, marketplace)
    where_clause = (" WHERE " + hf.lstrip(" AND ")) if hf else ""

    rows = _safe_ads_query(con, f"""
        SELECT keyword_text, match_type, campaign_name, ad_group_name, keyword_status
        FROM ads_negative_keywords{where_clause}
        ORDER BY campaign_name, keyword_text
    """, hp)

    con.close()

    keywords = [
        {
            "keyword": r[0],
            "matchType": r[1],
            "campaignName": r[2],
            "adGroupName": r[3],
            "status": r[4],
        }
        for r in rows
    ]

    return {"negativeKeywords": keywords}


# ── Ads: Profile Discovery + Manual Sync ────────────────────


@router.get("/api/ads/profiles")
def ads_profiles():
    """Discover Amazon Ads profiles for this account."""
    ads_creds = _load_ads_credentials()
    if not ads_creds:
        return {"error": "No Amazon Ads credentials configured. Set AMAZON_ADS_CLIENT_ID, AMAZON_ADS_CLIENT_SECRET, AMAZON_ADS_REFRESH_TOKEN env vars."}

    try:
        from ad_api.api import Profiles
        result = Profiles(credentials=ads_creds).get_profiles()
        profiles = []
        for p in result.payload:
            profiles.append({
                "profileId": p.get("profileId"),
                "countryCode": p.get("countryCode"),
                "accountName": p.get("accountInfo", {}).get("name", ""),
                "marketplace": p.get("accountInfo", {}).get("marketplaceStringId", ""),
                "type": p.get("accountInfo", {}).get("type", ""),
            })
        return {"profiles": profiles, "configured_profile_id": ads_creds.get("profile_id", "")}
    except Exception as e:
        return {"error": str(e)}


@router.post("/api/ads/sync")
async def trigger_ads_sync():
    """Manually trigger an ads data sync."""
    ads_creds = _load_ads_credentials()
    if not ads_creds:
        return {"error": "No Amazon Ads credentials configured"}
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _sync_ads_data)
        return {"status": "ads_sync_complete"}
    except Exception as e:
        return {"error": str(e)}
