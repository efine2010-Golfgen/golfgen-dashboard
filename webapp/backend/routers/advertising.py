"""Amazon Advertising routes."""
import logging
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
from fastapi import APIRouter, Query

from core.config import DB_PATH, TIMEZONE
from core.database import get_db
from core.hierarchy import hierarchy_filter
from services.ads_api import _sync_ads_data, _load_ads_credentials

logger = logging.getLogger("golfgen")
router = APIRouter()


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
