"""Ask Claude — tab-aware AI assistant endpoint for the GolfGen dashboard."""

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from core.database import get_db, get_db_rw
from core.config import ANTHROPIC_API_KEY
from core.hierarchy import hierarchy_filter

logger = logging.getLogger("golfgen")
router = APIRouter()

CENTRAL = ZoneInfo("America/Chicago")


def _n(v):
    """Coerce Decimal/None to float."""
    if v is None:
        return 0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0


def _fmt_dollar(v):
    """Format a number as dollar string."""
    v = _n(v)
    if abs(v) >= 1000:
        return f"${v:,.0f}"
    return f"${v:,.2f}"


class AskClaudeRequest(BaseModel):
    question: str
    active_tab: str = "sales"
    division: str | None = None
    customer: str | None = None


def _build_snapshot(con, active_tab: str, division: str | None, customer: str | None) -> str:
    """Build a plain-text data snapshot based on the active tab."""
    today = datetime.now(CENTRAL).date()
    hf_sql, hf_params = hierarchy_filter(division, customer)

    lines = []

    if active_tab == "inventory":
        try:
            row = con.execute(f"""
                SELECT COALESCE(SUM(fulfillable_quantity), 0),
                       COALESCE(SUM(total_quantity), 0),
                       COUNT(*)
                FROM fba_inventory
                WHERE date = (SELECT MAX(date) FROM fba_inventory) {hf_sql}
            """, hf_params).fetchone()
            lines.append(f"FBA Sellable Units: {int(row[0]):,}")
            lines.append(f"FBA Total Units: {int(row[1]):,}")
            lines.append(f"Active SKUs: {int(row[2]):,}")
        except Exception:
            lines.append("Inventory data: unavailable")

        try:
            top = con.execute(f"""
                SELECT asin, sku, fulfillable_quantity
                FROM fba_inventory
                WHERE date = (SELECT MAX(date) FROM fba_inventory) {hf_sql}
                ORDER BY fulfillable_quantity DESC LIMIT 10
            """, hf_params).fetchall()
            if top:
                lines.append("\nTop 10 SKUs by sellable quantity:")
                for r in top:
                    lines.append(f"  {r[1] or r[0]}: {int(r[2] or 0):,} units")
        except Exception:
            pass

    elif active_tab in ("sales", "exec-summary"):
        for label, days in [("7-day", 7), ("30-day", 30)]:
            try:
                sd = str(today - __import__('datetime').timedelta(days=days))
                row = con.execute(f"""
                    SELECT COALESCE(SUM(ordered_product_sales), 0),
                           COALESCE(SUM(units_ordered), 0)
                    FROM daily_sales
                    WHERE asin = 'ALL' AND date >= ? AND date <= ? {hf_sql}
                """, [sd, str(today)] + hf_params).fetchone()
                lines.append(f"{label} Revenue: {_fmt_dollar(row[0])}")
                lines.append(f"{label} Units: {int(row[1]):,}")
            except Exception:
                lines.append(f"{label} sales data: unavailable")

        try:
            from datetime import timedelta
            sd30 = str(today - timedelta(days=30))
            top = con.execute(f"""
                SELECT asin, SUM(ordered_product_sales) AS rev
                FROM daily_sales
                WHERE asin != 'ALL' AND date >= ? AND date <= ? {hf_sql}
                GROUP BY asin ORDER BY rev DESC LIMIT 5
            """, [sd30, str(today)] + hf_params).fetchall()
            if top:
                lines.append("\nTop 5 ASINs by revenue (last 30 days):")
                for r in top:
                    lines.append(f"  {r[0]}: {_fmt_dollar(r[1])}")
        except Exception:
            pass

        try:
            from datetime import timedelta
            sd30 = str(today - timedelta(days=30))
            ref = con.execute(f"""
                SELECT COALESCE(SUM(ABS(product_charges)), 0)
                FROM financial_events
                WHERE event_type ILIKE '%%refund%%' AND date >= ? {hf_sql}
            """, [sd30] + hf_params).fetchone()
            lines.append(f"\n30-day Refunds: {_fmt_dollar(ref[0])}")
        except Exception:
            pass

    elif active_tab == "advertising":
        try:
            row = con.execute(f"""
                SELECT COUNT(DISTINCT campaign_name),
                       COALESCE(SUM(spend), 0),
                       COALESCE(SUM(sales), 0),
                       COALESCE(SUM(impressions), 0),
                       COALESCE(SUM(clicks), 0)
                FROM advertising {hf_sql.replace(' AND ', ' WHERE ', 1) if hf_sql else ''}
            """, hf_params).fetchone()
            campaigns = int(row[0])
            if campaigns == 0:
                lines.append("Ads data sync under investigation — no campaign data available yet.")
            else:
                spend = _n(row[1])
                sales = _n(row[2])
                acos = round(spend / sales * 100, 1) if sales else 0
                lines.append(f"Active Campaigns: {campaigns}")
                lines.append(f"Total Spend: {_fmt_dollar(spend)}")
                lines.append(f"Ad Sales: {_fmt_dollar(sales)}")
                lines.append(f"ACOS: {acos}%")
                lines.append(f"Impressions: {int(row[3]):,}")
                lines.append(f"Clicks: {int(row[4]):,}")
        except Exception:
            lines.append("Advertising data: unavailable")

    elif active_tab == "profitability":
        try:
            from datetime import timedelta
            sd30 = str(today - timedelta(days=30))
            row = con.execute(f"""
                SELECT COALESCE(SUM(ordered_product_sales), 0),
                       COALESCE(SUM(units_ordered), 0)
                FROM daily_sales
                WHERE asin = 'ALL' AND date >= ? AND date <= ? {hf_sql}
            """, [sd30, str(today)] + hf_params).fetchone()
            rev = _n(row[0])
            lines.append(f"30-day Revenue: {_fmt_dollar(rev)}")

            fee_row = con.execute(f"""
                SELECT COALESCE(SUM(ABS(fba_fees)), 0) + COALESCE(SUM(ABS(commission)), 0)
                FROM financial_events
                WHERE date >= ? AND date <= ? {hf_sql}
            """, [sd30, str(today)] + hf_params).fetchone()
            fees = _n(fee_row[0])
            lines.append(f"30-day Amazon Fees: {_fmt_dollar(fees)}")
            if rev > 0:
                lines.append(f"Fee % of Revenue: {round(fees/rev*100, 1)}%")
        except Exception:
            lines.append("Profitability data: unavailable")

    elif active_tab in ("supply-chain", "otw"):
        try:
            row = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'otw'").fetchone()
            if row and row[0] > 0:
                otw = con.execute("SELECT COUNT(*), SUM(units), MIN(eta), MAX(eta) FROM otw").fetchone()
                lines.append(f"Containers: {int(otw[0])}")
                lines.append(f"Total units inbound: {int(otw[1] or 0):,}")
                lines.append(f"Earliest ETA: {otw[2]}")
                lines.append(f"Latest ETA: {otw[3]}")
            else:
                lines.append("OTW (On the Water) table not yet created.")
        except Exception:
            lines.append("Supply chain data: unavailable")

    # Default / fallback — general summary
    if not lines:
        try:
            from datetime import timedelta
            sd7 = str(today - timedelta(days=7))
            ord_row = con.execute(f"""
                SELECT COUNT(DISTINCT order_id)
                FROM orders
                WHERE purchase_date >= ? {hf_sql}
            """, [sd7] + hf_params).fetchone()
            lines.append(f"Orders last 7 days: {int(ord_row[0]):,}")
        except Exception:
            pass
        try:
            inv_row = con.execute("""
                SELECT COALESCE(SUM(fulfillable_quantity), 0)
                FROM fba_inventory
                WHERE date = (SELECT MAX(date) FROM fba_inventory)
            """).fetchone()
            lines.append(f"FBA Sellable Units: {int(inv_row[0]):,}")
        except Exception:
            pass
        if division:
            lines.append(f"Active division filter: {division}")

    return "\n".join(lines) if lines else "No data available for this tab."


@router.post("/api/ask-claude")
async def ask_claude(req: AskClaudeRequest, request: Request):
    """Tab-aware AI assistant powered by Claude."""
    # ── Auth check ──
    session = request.cookies.get("session_id")
    if not session:
        return JSONResponse(status_code=401, content={"answer": "Please log in to use Ask Claude."})

    try:
        con_auth = get_db()
        sess_row = con_auth.execute(
            "SELECT user_email, role FROM sessions WHERE session_id = ? AND expires_at > ?",
            [session, datetime.now(CENTRAL).isoformat()]
        ).fetchone()
        con_auth.close()
        if not sess_row:
            return JSONResponse(status_code=401, content={"answer": "Session expired. Please log in again."})
        user_email = sess_row[0]
        role = sess_row[1] or "staff"
        if role.lower() == "viewer":
            return {"answer": "Ask Claude is not available for Viewer accounts."}
    except Exception:
        return JSONResponse(status_code=401, content={"answer": "Authentication error."})

    # ── Check API key ──
    if not ANTHROPIC_API_KEY:
        return {
            "answer": "Ask Claude is not configured yet. Contact Eric to add the API key.",
            "tab_context": req.active_tab,
            "data_snapshot_summary": "",
        }

    # ── Build data snapshot ──
    try:
        con = get_db()
        snapshot = _build_snapshot(con, req.active_tab, req.division, req.customer)
        con.close()
    except Exception as e:
        logger.error(f"ask_claude snapshot error: {e}")
        snapshot = "Data snapshot unavailable."

    # ── Call Anthropic API ──
    today_str = datetime.now(CENTRAL).strftime("%A, %B %d, %Y")
    system_prompt = f"""You are the GolfGen Commerce Assistant — an AI built
into the GolfGen / Elite Global Brands internal dashboard used by the
operations team in Bentonville, AR. You have access to live data from
the dashboard pulled from PostgreSQL in real time.

Answer questions clearly and concisely. Use bullet points for lists.
Bold key numbers. If data is only from Amazon (SP-API), note that —
other channels (Walmart stores, Belk, Hobby Lobby, etc.) are coming
in a future phase. Never make up numbers. Only report what is in the
data snapshot provided. If the data snapshot is empty or a table has
no rows, say so clearly.

Today's date: {today_str} (Central Time)
Active tab: {req.active_tab}
Division filter: {req.division or 'All'}
Customer filter: {req.customer or 'All'}"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=system_prompt,
            messages=[{
                "role": "user",
                "content": f"Live data snapshot:\n{snapshot}\n\nQuestion: {req.question}"
            }]
        )
        answer = message.content[0].text
    except Exception as e:
        logger.error(f"ask_claude API error: {e}")
        answer = "Claude is temporarily unavailable. Please try again in a moment."

    # ── Audit log ──
    try:
        con_rw = get_db_rw()
        detail = req.question[:100] + " | tab: " + req.active_tab
        con_rw.execute(
            "INSERT INTO audit_log (timestamp, user_email, action, detail) VALUES (?, ?, ?, ?)",
            [datetime.now(CENTRAL).isoformat(), user_email, "ask_claude", detail]
        )
        con_rw.close()
    except Exception as e:
        logger.warning(f"ask_claude audit log error: {e}")

    return {
        "answer": answer,
        "tab_context": req.active_tab,
        "data_snapshot_summary": snapshot[:200] + "..." if len(snapshot) > 200 else snapshot,
    }
