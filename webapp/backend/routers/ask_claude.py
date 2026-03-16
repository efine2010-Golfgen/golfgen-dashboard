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

    elif active_tab in ("walmart-analytics", "walmart", "retail", "retail-reporting"):
        # ── Walmart Store / Retail data (comprehensive snapshot) ──

        # --- Item-Level Sales by Period ---
        for period_label, period_code in [("L4W", "L4W"), ("L13W", "L13W"), ("L52W", "L52W")]:
            try:
                wm_row = con.execute("""
                    SELECT SUM(COALESCE(pos_sales_ty, 0)), SUM(COALESCE(pos_sales_ly, 0)),
                           SUM(COALESCE(pos_qty_ty, 0)), SUM(COALESCE(pos_qty_ly, 0)),
                           SUM(COALESCE(returns_amt_ty, 0)), SUM(COALESCE(returns_qty_ty, 0)),
                           AVG(COALESCE(instock_pct_ty, 0)), AVG(COALESCE(repl_instock_pct_ty, 0)),
                           SUM(COALESCE(on_hand_qty_ty, 0)), SUM(COALESCE(clearance_on_hand_qty_ty, 0)),
                           SUM(COALESCE(on_order_qty_ty, 0)), SUM(COALESCE(in_transit_qty_ty, 0)),
                           SUM(COALESCE(in_warehouse_qty_ty, 0))
                    FROM walmart_item_weekly
                    WHERE period_type = %s
                """, [period_code]).fetchone()
                if wm_row and _n(wm_row[0]) > 0:
                    ty_sales = _n(wm_row[0])
                    ly_sales = _n(wm_row[1])
                    lines.append(f"\n=== {period_label} Item Performance ===")
                    lines.append(f"POS Sales TY: {_fmt_dollar(ty_sales)}")
                    lines.append(f"POS Sales LY: {_fmt_dollar(ly_sales)}")
                    if ly_sales > 0:
                        chg = (ty_sales - ly_sales) / ly_sales * 100
                        lines.append(f"Sales Change: {chg:+.1f}%")
                    lines.append(f"POS Units TY: {int(wm_row[2]):,}")
                    lines.append(f"POS Units LY: {int(wm_row[3]):,}")
                    lines.append(f"Returns $: {_fmt_dollar(abs(_n(wm_row[4])))}")
                    lines.append(f"Returns Units: {int(abs(_n(wm_row[5]))):,}")
                    avg_instock = _n(wm_row[6])
                    avg_repl = _n(wm_row[7])
                    if avg_instock > 0:
                        lines.append(f"Avg Instock %: {avg_instock:.1f}%")
                    if avg_repl > 0:
                        lines.append(f"Avg Repl Instock %: {avg_repl:.1f}%")
                    on_hand = int(_n(wm_row[8]))
                    if on_hand > 0:
                        lines.append(f"On Hand Units: {on_hand:,}")
                    clearance = int(_n(wm_row[9]))
                    if clearance > 0:
                        lines.append(f"Clearance On Hand: {clearance:,}")
                    on_order = int(_n(wm_row[10]))
                    in_transit = int(_n(wm_row[11]))
                    in_wh = int(_n(wm_row[12]))
                    if on_order > 0:
                        lines.append(f"On Order: {on_order:,}")
                    if in_transit > 0:
                        lines.append(f"In Transit: {in_transit:,}")
                    if in_wh > 0:
                        lines.append(f"In Warehouse: {in_wh:,}")
            except Exception:
                pass

        # --- Top 10 Items by Sales (L4W) ---
        try:
            top_items = con.execute("""
                SELECT prime_item_desc, pos_sales_ty, pos_qty_ty,
                       pos_sales_ly, instock_pct_ty, on_hand_qty_ty
                FROM walmart_item_weekly
                WHERE period_type = 'L4W' AND pos_sales_ty > 0
                ORDER BY pos_sales_ty DESC LIMIT 10
            """).fetchall()
            if top_items:
                lines.append("\n=== Top 10 Items by L4W Sales ===")
                for r in top_items:
                    desc = (r[0] or "Unknown")[:40]
                    ty = _fmt_dollar(r[1])
                    units = int(_n(r[2]))
                    ly = _fmt_dollar(r[3])
                    instock = _n(r[4])
                    oh = int(_n(r[5]))
                    lines.append(f"  {desc}: {ty} ({units:,} units) | LY: {ly} | Instock: {instock:.0f}% | OH: {oh:,}")
        except Exception:
            pass

        # --- Store-Level Summary (latest week) ---
        try:
            store_row = con.execute("""
                SELECT COUNT(DISTINCT store_number),
                       SUM(COALESCE(pos_sales_ty, 0)),
                       SUM(COALESCE(pos_sales_ly, 0)),
                       SUM(COALESCE(pos_qty_ty, 0)),
                       SUM(COALESCE(returns_qty_ty, 0)),
                       AVG(COALESCE(instock_pct_ty, 0)),
                       AVG(COALESCE(repl_instock_pct_ty, 0)),
                       SUM(COALESCE(on_hand_qty_ty, 0)),
                       SUM(COALESCE(clearance_on_hand_qty_ty, 0)),
                       SUM(COALESCE(in_warehouse_qty_ty, 0)),
                       MAX(walmart_week)
                FROM walmart_store_weekly
                WHERE walmart_week = (SELECT MAX(walmart_week) FROM walmart_store_weekly)
            """).fetchone()
            if store_row and _n(store_row[0]) > 0:
                lines.append(f"\n=== Store-Level Data (Week {store_row[10]}) ===")
                lines.append(f"Active Stores: {int(store_row[0]):,}")
                lines.append(f"Store Sales TY: {_fmt_dollar(store_row[1])}")
                lines.append(f"Store Sales LY: {_fmt_dollar(store_row[2])}")
                ty_s = _n(store_row[1])
                ly_s = _n(store_row[2])
                if ly_s > 0:
                    lines.append(f"Store Sales Change: {((ty_s - ly_s) / ly_s * 100):+.1f}%")
                lines.append(f"Store Units TY: {int(_n(store_row[3])):,}")
                lines.append(f"Store Returns Units: {int(abs(_n(store_row[4]))):,}")
                avg_is = _n(store_row[5])
                avg_ri = _n(store_row[6])
                if avg_is > 0:
                    lines.append(f"Avg Store Instock %: {avg_is:.1f}%")
                if avg_ri > 0:
                    lines.append(f"Avg Store Repl Instock %: {avg_ri:.1f}%")
                oh = int(_n(store_row[7]))
                cl = int(_n(store_row[8]))
                wh = int(_n(store_row[9]))
                if oh > 0:
                    lines.append(f"Total On Hand: {oh:,}")
                if cl > 0:
                    lines.append(f"Clearance On Hand: {cl:,}")
                if wh > 0:
                    lines.append(f"In Warehouse: {wh:,}")
        except Exception:
            pass

        # --- Top / Bottom Stores ---
        try:
            top_stores = con.execute("""
                SELECT store_number, store_name, pos_sales_ty, pos_qty_ty, instock_pct_ty
                FROM walmart_store_weekly
                WHERE walmart_week = (SELECT MAX(walmart_week) FROM walmart_store_weekly)
                  AND pos_sales_ty > 0
                ORDER BY pos_sales_ty DESC LIMIT 5
            """).fetchall()
            if top_stores:
                lines.append("\nTop 5 Stores by Sales:")
                for r in top_stores:
                    name = (r[1] or f"Store #{r[0]}")[:30]
                    lines.append(f"  {name}: {_fmt_dollar(r[2])} ({int(_n(r[3])):,} units, {_n(r[4]):.0f}% instock)")
        except Exception:
            pass

        try:
            bottom_stores = con.execute("""
                SELECT store_number, store_name, pos_sales_ty, instock_pct_ty
                FROM walmart_store_weekly
                WHERE walmart_week = (SELECT MAX(walmart_week) FROM walmart_store_weekly)
                  AND instock_pct_ty IS NOT NULL AND instock_pct_ty > 0
                ORDER BY instock_pct_ty ASC LIMIT 5
            """).fetchall()
            if bottom_stores:
                lines.append("\nBottom 5 Stores by Instock %:")
                for r in bottom_stores:
                    name = (r[1] or f"Store #{r[0]}")[:30]
                    lines.append(f"  {name}: {_n(r[3]):.1f}% instock, {_fmt_dollar(r[2])} sales")
        except Exception:
            pass

        # --- Scorecard KPIs (All Vendors, key periods) ---
        try:
            sc_rows = con.execute("""
                SELECT metric_name, period, value_ty, value_ly, value_diff
                FROM walmart_scorecard
                WHERE vendor_section = 'All Vendors'
                  AND period IN ('Last Week', 'Year', 'Q1', 'Q2', 'Q3', 'Q4')
                ORDER BY metric_name, period
            """).fetchall()
            if sc_rows:
                lines.append("\n=== Vendor Scorecard (All Vendors) ===")
                current_metric = None
                for r in sc_rows:
                    metric = r[0]
                    period = r[1]
                    ty = _n(r[2])
                    ly = _n(r[3])
                    diff = _n(r[4])
                    if metric != current_metric:
                        current_metric = metric
                        lines.append(f"\n  {metric}:")
                    # Format based on metric type
                    if any(k in metric.lower() for k in ['%', 'margin', 'instock', 'mumd']):
                        lines.append(f"    {period}: TY {ty:.1f}% | LY {ly:.1f}% | Diff {diff:+.1f}pp")
                    elif any(k in metric.lower() for k in ['$', 'sales', 'returns', 'receipt', 'cost', 'ships']):
                        lines.append(f"    {period}: TY {_fmt_dollar(ty)} | LY {_fmt_dollar(ly)} | Diff {_fmt_dollar(diff)}")
                    elif any(k in metric.lower() for k in ['turns', 'weeks', 'gmroii', 'aur']):
                        lines.append(f"    {period}: TY {ty:.2f} | LY {ly:.2f} | Diff {diff:+.2f}")
                    else:
                        lines.append(f"    {period}: TY {ty:,.0f} | LY {ly:,.0f} | Diff {diff:+,.0f}")
        except Exception:
            pass

        # --- eCommerce ---
        try:
            ecomm_row = con.execute("""
                SELECT SUM(COALESCE(auth_based_net_sales_ty, 0)),
                       SUM(COALESCE(auth_based_net_sales_ly, 0)),
                       SUM(COALESCE(auth_based_qty_ty, 0)),
                       SUM(COALESCE(shipped_based_net_sales_ty, 0)),
                       SUM(COALESCE(shipped_based_qty_ty, 0)),
                       COUNT(DISTINCT all_links_item_number)
                FROM walmart_ecomm_weekly
            """).fetchone()
            if ecomm_row and _n(ecomm_row[0]) > 0:
                lines.append(f"\n=== eCommerce ===")
                lines.append(f"Auth-Based Net Sales TY: {_fmt_dollar(ecomm_row[0])}")
                lines.append(f"Auth-Based Net Sales LY: {_fmt_dollar(ecomm_row[1])}")
                ty_e = _n(ecomm_row[0])
                ly_e = _n(ecomm_row[1])
                if ly_e > 0:
                    lines.append(f"eComm Sales Change: {((ty_e - ly_e) / ly_e * 100):+.1f}%")
                lines.append(f"Auth-Based Units TY: {int(_n(ecomm_row[2])):,}")
                lines.append(f"Shipped Net Sales TY: {_fmt_dollar(ecomm_row[3])}")
                lines.append(f"Shipped Units TY: {int(_n(ecomm_row[4])):,}")
                lines.append(f"Active eComm Items: {int(_n(ecomm_row[5])):,}")
        except Exception:
            pass

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


@router.get("/api/debug/test-claude")
async def test_claude():
    """Debug endpoint — test Anthropic API connectivity (no auth required)."""
    result = {"key_present": bool(ANTHROPIC_API_KEY), "key_length": len(ANTHROPIC_API_KEY)}
    if ANTHROPIC_API_KEY:
        result["key_prefix"] = ANTHROPIC_API_KEY[:7] + "..."
    try:
        import anthropic
        result["sdk_version"] = anthropic.__version__
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=50,
            messages=[{"role": "user", "content": "Say hello in one word."}]
        )
        result["status"] = "ok"
        result["response"] = message.content[0].text
    except Exception as e:
        import traceback
        result["status"] = "error"
        result["error_type"] = type(e).__name__
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    return result


@router.post("/api/ask-claude")
async def ask_claude(req: AskClaudeRequest, request: Request):
    """Tab-aware AI assistant powered by Claude."""
    # ── Auth check (same pattern as core.auth) ──
    session_token = request.cookies.get("golfgen_session")
    if not session_token:
        return JSONResponse(status_code=401, content={"answer": "Please log in to use Ask Claude."})

    try:
        con_auth = get_db()
        sess_row = con_auth.execute(
            "SELECT user_email, user_name, role FROM sessions WHERE token = ?",
            [session_token]
        ).fetchone()
        con_auth.close()
        if not sess_row:
            return JSONResponse(status_code=401, content={"answer": "Session expired. Please log in again."})
        user_email = sess_row[0]
        role = sess_row[2] or "staff"
        if role.lower() == "viewer":
            return {"answer": "Ask Claude is not available for Viewer accounts."}
    except Exception as auth_err:
        logger.error(f"ask_claude auth error: {auth_err}")
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
Bold key numbers. Data is available from both Amazon (SP-API) and
Walmart Stores (Scintilla POS reports). Amazon data covers orders,
sales, inventory, advertising, and financial events. Walmart data
covers store-level POS sales (by store, with instock % and on-hand),
item-level performance (L4W/L13W/L52W with returns, inventory, instock),
vendor scorecard KPIs (margins, turns, GMROII, AUR, instock across periods),
eCommerce (auth-based and shipped-based sales), and inventory health
(on-hand, clearance, in-transit, in-warehouse). When the Walmart tab is
active you receive a comprehensive snapshot with top/bottom stores,
top items, and scorecard metrics by period. Other channels (Belk,
Hobby Lobby, Shopify, etc.) are coming in a future phase.
Never make up numbers. Only report what is in the data snapshot
provided. If the data snapshot is empty or a table has no rows,
say so clearly.

Today's date: {today_str} (Central Time)
Active tab: {req.active_tab}
Division filter: {req.division or 'All'}
Customer filter: {req.customer or 'All'}"""

    try:
        import anthropic
        logger.info(f"ask_claude: SDK version={anthropic.__version__}, key_len={len(ANTHROPIC_API_KEY)}, key_prefix={ANTHROPIC_API_KEY[:7]}...")
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            system=system_prompt,
            messages=[{
                "role": "user",
                "content": f"Live data snapshot:\n{snapshot}\n\nQuestion: {req.question}"
            }]
        )
        answer = message.content[0].text
    except Exception as e:
        logger.error(f"ask_claude API error [{type(e).__name__}]: {e}")
        import traceback
        logger.error(f"ask_claude traceback: {traceback.format_exc()}")
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
