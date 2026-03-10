"""
GolfGen Amazon Dashboard Generator (v3)
========================================
Reads real data from DuckDB and injects it into the branded v3 dashboard template.
The output is a self-contained HTML file with live SP-API data.

Usage:
    python scripts/generate_dashboard.py           # Generate with live DuckDB data
    python scripts/generate_dashboard.py --demo     # Generate with demo data (template as-is)
    python scripts/generate_dashboard.py --refresh  # Pull fresh data then generate
"""

import json
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas")
    sys.exit(1)


# ─────────────────────────────────────────────────────────
# DuckDB Data Queries
# ─────────────────────────────────────────────────────────

def query_daily_data(con):
    """Get daily totals (the 'ALL' aggregate rows from sales & traffic report).

    NOTE: total_order_items is NULL in SP-API Sales & Traffic reports,
    so we use units_ordered as the orders proxy. The unit_session_percentage
    field from Amazon is the real conversion rate.
    """
    df = con.execute("""
        SELECT
            CAST(date AS VARCHAR) as date,
            COALESCE(units_ordered, 0) as units,
            COALESCE(ordered_product_sales, 0) as revenue,
            COALESCE(sessions, 0) as sessions,
            COALESCE(units_ordered, 0) as orders,
            COALESCE(unit_session_percentage, 0) as conv_rate
        FROM daily_sales
        WHERE asin = 'ALL'
        ORDER BY date
    """).fetchdf()
    return df.to_dict('records') if len(df) > 0 else []


def build_product_name_lookup(con):
    """Build an ASIN -> product name lookup from fba_inventory and order_items.

    The Sales & Traffic report doesn't include product names, so we cross-reference
    from other tables that do have them. Priority: order_items.title > fba_inventory.product_name.
    """
    lookup = {}

    # First pass: fba_inventory (broader coverage)
    try:
        inv = con.execute("""
            SELECT DISTINCT asin, product_name
            FROM fba_inventory
            WHERE product_name IS NOT NULL AND product_name != ''
        """).fetchdf()
        for _, row in inv.iterrows():
            lookup[row['asin']] = row['product_name']
    except Exception:
        pass

    # Second pass: order_items (overrides with potentially better names)
    try:
        oi = con.execute("""
            SELECT DISTINCT asin, title as product_name
            FROM order_items
            WHERE title IS NOT NULL AND title != ''
        """).fetchdf()
        for _, row in oi.iterrows():
            lookup[row['asin']] = row['product_name']
    except Exception:
        pass

    return lookup


def query_asin_data(con):
    """Get per-ASIN sales data (non-ALL rows)."""
    df = con.execute("""
        SELECT
            CAST(date AS VARCHAR) as date,
            asin,
            COALESCE(sku, '') as sku,
            COALESCE(product_name, '') as product_name,
            COALESCE(units_ordered, 0) as units,
            COALESCE(ordered_product_sales, 0) as revenue,
            COALESCE(sessions, 0) as sessions,
            COALESCE(average_selling_price, 0) as avg_price,
            COALESCE(unit_session_percentage, 0) as conv_rate
        FROM daily_sales
        WHERE asin != 'ALL'
        ORDER BY date, asin
    """).fetchdf()
    return df.to_dict('records') if len(df) > 0 else []


def query_financials(con):
    """Get financial events aggregated by date."""
    df = con.execute("""
        SELECT
            CAST(date AS VARCHAR) as date,
            SUM(COALESCE(product_charges, 0)) as product_charges,
            SUM(COALESCE(fba_fees, 0)) as fba_fees,
            SUM(COALESCE(commission, 0)) as commission,
            SUM(COALESCE(net_proceeds, 0)) as net_proceeds,
            SUM(COALESCE(other_fees, 0)) as other_fees
        FROM financial_events
        GROUP BY date
        ORDER BY date
    """).fetchdf()
    result = {}
    if len(df) > 0:
        for row in df.to_dict('records'):
            result[row['date']] = row
    return result


def query_financials_by_asin(con):
    """Get financial events per ASIN for product-level P&L."""
    df = con.execute("""
        SELECT
            asin,
            SUM(COALESCE(fba_fees, 0)) as fba_fees,
            SUM(COALESCE(commission, 0)) as commission,
            SUM(COALESCE(net_proceeds, 0)) as net_proceeds
        FROM financial_events
        WHERE asin IS NOT NULL AND asin != ''
        GROUP BY asin
    """).fetchdf()
    result = {}
    if len(df) > 0:
        for row in df.to_dict('records'):
            result[row['asin']] = row
    return result


def query_inventory(con):
    """Get latest FBA inventory snapshot."""
    df = con.execute("""
        SELECT
            asin, sku, product_name,
            COALESCE(fulfillable_quantity, 0) as fulfillable,
            COALESCE(inbound_shipped_quantity, 0) as inbound_shipped,
            COALESCE(inbound_receiving_quantity, 0) as inbound_receiving,
            COALESCE(reserved_quantity, 0) as reserved,
            COALESCE(unfulfillable_quantity, 0) as unfulfillable,
            COALESCE(total_quantity, 0) as total
        FROM fba_inventory
        WHERE date = (SELECT MAX(date) FROM fba_inventory)
        ORDER BY fulfillable_quantity DESC
    """).fetchdf()
    return df.to_dict('records') if len(df) > 0 else []


def query_velocity(con):
    """Get avg daily units per ASIN from last 30 days."""
    df = con.execute("""
        SELECT asin,
            CASE WHEN COUNT(DISTINCT date) > 0
                THEN SUM(units_ordered)::FLOAT / COUNT(DISTINCT date)
                ELSE 0 END as avg_daily
        FROM daily_sales
        WHERE asin != 'ALL' AND date >= CURRENT_DATE - INTERVAL '30' DAY
        GROUP BY asin
    """).fetchdf()
    result = {}
    if len(df) > 0:
        for row in df.to_dict('records'):
            result[row['asin']] = row['avg_daily']
    return result


def query_ad_data(con):
    """Get advertising data if available."""
    try:
        df = con.execute("""
            SELECT
                CAST(date AS VARCHAR) as date,
                SUM(COALESCE(spend, 0)) as ad_spend,
                SUM(COALESCE(sales, 0)) as ad_sales
            FROM advertising
            GROUP BY date
            ORDER BY date
        """).fetchdf()
        result = {}
        if len(df) > 0:
            for row in df.to_dict('records'):
                result[row['date']] = row
        return result
    except Exception:
        return {}


def query_ad_data_by_asin(con):
    """Get advertising data per ASIN if available."""
    try:
        df = con.execute("""
            SELECT
                asin,
                SUM(COALESCE(spend, 0)) as ad_spend,
                SUM(COALESCE(sales, 0)) as ad_sales
            FROM advertising
            WHERE asin IS NOT NULL AND asin != ''
            GROUP BY asin
        """).fetchdf()
        result = {}
        if len(df) > 0:
            for row in df.to_dict('records'):
                result[row['asin']] = row
        return result
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────
# Build JS Data Structures
# ─────────────────────────────────────────────────────────

def build_daily_js_data(daily_totals, financials, ad_data):
    """
    Build the DATA array matching the v3 dashboard format:
    { date, dateObj, revenue, units, sessions, orders, adSpend, aur, convRate }

    Uses Amazon's unit_session_percentage as convRate (already stored as conv_rate in query).
    Uses units_ordered as orders proxy (total_order_items is NULL in SP-API reports).
    """
    js_data = []
    for row in daily_totals:
        d = str(row['date'])
        fin = financials.get(d, {})
        ad = ad_data.get(d, {})

        revenue = round(row['revenue'], 2)
        units = int(row['units'])
        sessions = int(row['sessions'])
        orders = int(row.get('orders', 0))
        ad_spend = round(ad.get('ad_spend', 0), 2)
        aur = round(revenue / units, 2) if units > 0 else 0
        # Use Amazon's actual conv rate (unit_session_percentage) rather than computing orders/sessions
        conv_rate = round(row.get('conv_rate', 0), 2)

        js_data.append({
            'date': d,
            'revenue': revenue,
            'units': units,
            'sessions': sessions,
            'orders': orders,
            'adSpend': ad_spend,
            'aur': aur,
            'convRate': conv_rate,
        })

    return js_data


def build_products_js(asin_data, inventory, velocity_map, fin_by_asin, ad_by_asin, name_lookup=None):
    """
    Build the PRODUCTS array matching v3 dashboard format:
    { id, name, sku, price, cogs, fba }

    Plus pre-computed product metrics for the product table.

    name_lookup: dict mapping ASIN -> product name (from build_product_name_lookup)
    """
    from collections import defaultdict
    name_lookup = name_lookup or {}

    # Aggregate per-ASIN across all dates
    asin_agg = defaultdict(lambda: {
        'revenue': 0, 'units': 0, 'sessions': 0, 'name': '', 'sku': '', 'prices': []
    })

    for row in asin_data:
        asin = row['asin']
        asin_agg[asin]['revenue'] += row['revenue']
        asin_agg[asin]['units'] += row['units']
        asin_agg[asin]['sessions'] += row.get('sessions', 0)
        if row.get('product_name'):
            asin_agg[asin]['name'] = row['product_name']
        if row.get('sku'):
            asin_agg[asin]['sku'] = row['sku']
        if row.get('avg_price', 0) > 0:
            asin_agg[asin]['prices'].append(row['avg_price'])

    # Apply name lookup for ASINs that don't have names from daily_sales
    for asin, data in asin_agg.items():
        if not data['name'] and asin in name_lookup:
            data['name'] = name_lookup[asin]
        # Also check inventory for SKU if missing
        if not data['sku']:
            inv_item = next((i for i in inventory if i['asin'] == asin), None)
            if inv_item and inv_item.get('sku'):
                data['sku'] = inv_item['sku']

    products = []
    product_metrics = []

    # Sort by revenue descending
    sorted_asins = sorted(asin_agg.items(), key=lambda x: x[1]['revenue'], reverse=True)

    for idx, (asin, data) in enumerate(sorted_asins):
        avg_price = round(sum(data['prices']) / len(data['prices']), 2) if data['prices'] else 0
        if avg_price == 0 and data['units'] > 0:
            avg_price = round(data['revenue'] / data['units'], 2)

        # Estimate COGS (~35% of price) and FBA fee (~8% of price) as defaults
        # These can be refined with actual data later
        est_cogs = round(avg_price * 0.35, 2)
        est_fba = round(avg_price * 0.08, 2)

        # Use actual financial data if available
        fin = fin_by_asin.get(asin, {})
        actual_fba = abs(fin.get('fba_fees', 0))
        actual_commission = abs(fin.get('commission', 0))

        ad = ad_by_asin.get(asin, {})
        ad_spend = round(ad.get('ad_spend', 0), 2)

        # Calculate net profit
        total_rev = data['revenue']
        total_units = data['units']
        cogs_total = total_units * est_cogs
        fba_total = actual_fba if actual_fba > 0 else total_units * est_fba
        net = round(total_rev - cogs_total - fba_total - actual_commission - ad_spend, 2)
        margin = round(net / total_rev * 100) if total_rev > 0 else 0

        # Inventory
        inv_item = next((i for i in inventory if i['asin'] == asin), None)
        fba_stock = inv_item['fulfillable'] if inv_item else 0
        inbound = (inv_item.get('inbound_shipped', 0) + inv_item.get('inbound_receiving', 0)) if inv_item else 0
        reserved = inv_item.get('reserved', 0) if inv_item else 0
        avg_daily = velocity_map.get(asin, 0)
        dos = int(fba_stock / avg_daily) if avg_daily > 0 else 999

        products.append({
            'id': idx + 1,
            'name': data['name'] or asin,
            'sku': data['sku'] or '',
            'price': avg_price,
            'cogs': est_cogs,
            'fba': est_fba,
        })

        product_metrics.append({
            'asin': asin,
            'name': data['name'] or asin,
            'sku': data['sku'] or '',
            'rev': round(total_rev, 2),
            'units': total_units,
            'price': avg_price,
            'adSpend': ad_spend,
            'acos': round(ad_spend / total_rev * 100) if total_rev > 0 and ad_spend > 0 else 0,
            'tacos': round(ad_spend / total_rev * 100) if total_rev > 0 and ad_spend > 0 else 0,
            'net': net,
            'margin': margin,
            'fbaStock': fba_stock,
            'inbound': inbound,
            'reserved': reserved,
            'avgDaily': round(avg_daily, 1),
            'dos': dos,
        })

    return products, product_metrics


# ─────────────────────────────────────────────────────────
# Template Injection
# ─────────────────────────────────────────────────────────

def inject_data_into_template(template_html, daily_data, products, product_metrics, generated_at):
    """
    Replace the demo generateData() and PRODUCTS in the v3 template
    with real data from DuckDB.
    """
    # Find the script block boundaries
    script_start_marker = "// DATA GENERATION (DEMO)"
    script_end_marker = "const DATA = generateData();"

    start_idx = template_html.find(script_start_marker)
    end_idx = template_html.find(script_end_marker)

    if start_idx == -1 or end_idx == -1:
        print("WARNING: Could not find data injection markers in template.")
        print("  Looked for:", script_start_marker)
        print("  And:", script_end_marker)
        return template_html

    # Find the beginning of the comment line containing the marker
    line_start = template_html.rfind('\n', 0, start_idx) + 1
    # Find the end of the generateData() line
    line_end = end_idx + len(script_end_marker)

    # Build the replacement JS code
    daily_json = json.dumps(daily_data, indent=None)
    products_json = json.dumps(products, indent=None)
    metrics_json = json.dumps(product_metrics, indent=None)

    replacement = f"""// ============================================================
// LIVE DATA — Injected from DuckDB via SP-API ({generated_at})
// ============================================================

const PRODUCTS = {products_json};

// Pre-computed product metrics from real data
const PRODUCT_METRICS = {metrics_json};

// Raw daily data from SP-API Sales & Traffic Report
const _RAW_DATA = {daily_json};

// Convert raw data to the format the dashboard expects
const DATA = _RAW_DATA.map(d => ({{
    date: d.date,
    dateObj: new Date(d.date + 'T00:00:00'),
    revenue: d.revenue,
    units: d.units,
    sessions: d.sessions,
    orders: d.orders,
    adSpend: d.adSpend,
    aur: d.aur,
    convRate: d.convRate
}}));

// Seeded random for reproducibility (used by projection/estimates)
let _seed = 42;
function srand() {{ _seed = (_seed * 16807) % 2147483647; return (_seed - 1) / 2147483646; }}

function seasonFactor(date) {{
    const m = date.getMonth();
    const factors = [0.55, 0.50, 0.70, 1.10, 1.60, 1.85, 1.90, 1.75, 1.20, 0.80, 1.30, 1.50];
    return factors[m];
}}"""

    result = template_html[:line_start] + replacement + template_html[line_end:]

    # Replace the productBreakdown function to use real PRODUCT_METRICS
    old_pb_start = "function productBreakdown(days, products) {"
    old_pb_end_marker = "return { ...p, rev, units: u, ad, acos, tacos, net, margin, trendPct, fbaStock, inbound, reserved, avgDaily, dos };\n    });\n}"

    pb_start_idx = result.find(old_pb_start)
    pb_end_idx = result.find(old_pb_end_marker)

    if pb_start_idx != -1 and pb_end_idx != -1:
        pb_end_idx += len(old_pb_end_marker)
        new_pb = """function productBreakdown(days, products) {
    // Use pre-computed real metrics from DuckDB
    if (typeof PRODUCT_METRICS !== 'undefined' && PRODUCT_METRICS.length > 0) {
        return PRODUCT_METRICS.map((pm, idx) => ({
            ...products[idx] || {},
            id: idx + 1,
            name: pm.name,
            sku: pm.sku,
            rev: pm.rev,
            units: pm.units,
            price: pm.price,
            ad: pm.adSpend,
            acos: pm.acos,
            tacos: pm.tacos,
            net: pm.net,
            margin: pm.margin,
            trendPct: 0,  // TODO: calculate from recent vs prior period
            fbaStock: pm.fbaStock,
            inbound: pm.inbound,
            reserved: pm.reserved,
            avgDaily: pm.avgDaily,
            dos: pm.dos
        }));
    }
    // Fallback: estimate from daily data (for demo mode)
    const weights = products.map((p, i) => {
        const priceWeight = p.price / 50;
        const posWeight = Math.max(0.3, 1 - i * 0.025);
        return priceWeight * posWeight * (0.7 + srand() * 0.6);
    });
    const totalW = weights.reduce((a, b) => a + b, 0);
    return products.map((p, idx) => {
        const share = weights[idx] / totalW;
        const rev = Math.round(days.reduce((s, d) => s + d.revenue, 0) * share);
        const u = Math.max(1, Math.round(rev / p.price));
        const ad = Math.round(rev * (0.10 + srand() * 0.18));
        const cogsTotal = u * p.cogs;
        const fbaTotal = u * p.fba;
        const net = rev - cogsTotal - fbaTotal - ad;
        const margin = Math.round(net / rev * 100);
        const acos = Math.round(ad / rev * 100);
        const tacos = Math.round(ad / rev * 100);
        const trendPct = Math.round(-8 + srand() * 30);
        const fbaStock = Math.round(50 + srand() * 800);
        const inbound = Math.round(srand() * 400);
        const reserved = Math.round(srand() * fbaStock * 0.15);
        const avgDaily = Math.max(1, Math.round(u / Math.max(1, days.length)));
        const dos = Math.round(fbaStock / avgDaily);
        return { ...p, rev, units: u, ad, acos, tacos, net, margin, trendPct, fbaStock, inbound, reserved, avgDaily, dos };
    });
}"""
        result = result[:pb_start_idx] + new_pb + result[pb_end_idx:]

    # Replace "Demo Data" badge with "Live Data" + timestamp
    result = result.replace(
        '<span class="demo-badge">Demo Data</span>',
        f'<span class="demo-badge" style="background:rgba(46,207,170,0.25);border-color:rgba(46,207,170,0.5);">LIVE DATA</span>'
        f'<span class="demo-badge" style="font-size:9px;letter-spacing:1px;">Updated {generated_at}</span>'
    )

    # Update footer
    result = result.replace(
        'Dashboard v3.0 &bull; Data refreshed via Amazon SP-API',
        f'Dashboard v3.0 &bull; Live data refreshed {generated_at} via Amazon SP-API'
    )

    return result


def inline_chartjs(html):
    """Replace the CDN Chart.js script tag with inline version if available."""
    chartjs_path = PROJECT_ROOT / 'lib' / 'chart.umd.min.js'
    if chartjs_path.exists():
        chartjs_content = chartjs_path.read_text()
        html = html.replace(
            '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>',
            f'<script>{chartjs_content}</script>'
        )
    return html


# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────

def main():
    demo_mode = '--demo' in sys.argv
    refresh_mode = '--refresh' in sys.argv

    db_path = PROJECT_ROOT / 'data' / 'golfgen_amazon.duckdb'
    template_path = PROJECT_ROOT / 'amazon_dashboard_v3.html'
    output_path = PROJECT_ROOT / 'amazon_dashboard_live.html'
    share_output = PROJECT_ROOT / 'amazon_dashboard_live_share.html'

    if not template_path.exists():
        print(f"ERROR: Template not found: {template_path}")
        sys.exit(1)

    if demo_mode:
        print("📋 Demo mode — copying template as-is")
        import shutil
        shutil.copy2(template_path, output_path)
        print(f"✅ Dashboard saved to: {output_path}")
        return

    if refresh_mode:
        print("🔄 Refreshing data from SP-API first...")
        import subprocess
        result = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / 'scripts' / 'amazon_sp_api.py'), '7'],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            print(f"WARNING: Data refresh had issues: {result.stderr}")

    if not db_path.exists():
        print(f"ERROR: DuckDB database not found: {db_path}")
        print("Run amazon_sp_api.py first to pull data.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  GolfGen Dashboard Generator (v3 Live)")
    print(f"{'='*60}\n")

    # Connect to DuckDB
    con = duckdb.connect(str(db_path), read_only=True)

    # Query all data
    print("📊 Querying DuckDB...")
    name_lookup = build_product_name_lookup(con)
    daily_totals = query_daily_data(con)
    asin_data = query_asin_data(con)
    financials = query_financials(con)
    fin_by_asin = query_financials_by_asin(con)
    inventory = query_inventory(con)
    velocity = query_velocity(con)
    ad_data = query_ad_data(con)
    ad_by_asin = query_ad_data_by_asin(con)

    con.close()

    print(f"   Name lookup:  {len(name_lookup)} ASINs with product names")

    print(f"   Daily totals: {len(daily_totals)} days")
    print(f"   ASIN rows:    {len(asin_data)} records")
    print(f"   Financials:   {len(financials)} days")
    print(f"   Inventory:    {len(inventory)} items")
    print(f"   Ad data:      {len(ad_data)} days")

    if len(daily_totals) == 0:
        print("\n⚠️  No daily sales data found. Run amazon_sp_api.py first.")
        sys.exit(1)

    # Build JS data structures
    print("\n🔧 Building dashboard data...")
    daily_js = build_daily_js_data(daily_totals, financials, ad_data)
    products_js, product_metrics = build_products_js(
        asin_data, inventory, velocity, fin_by_asin, ad_by_asin, name_lookup
    )

    print(f"   DATA array:    {len(daily_js)} entries")
    print(f"   PRODUCTS:      {len(products_js)} items")

    # Date range
    dates = [d['date'] for d in daily_js]
    date_range = f"{min(dates)} to {max(dates)}" if dates else "N/A"
    total_rev = sum(d['revenue'] for d in daily_js)
    total_units = sum(d['units'] for d in daily_js)
    print(f"   Date range:    {date_range}")
    print(f"   Total revenue: ${total_rev:,.2f}")
    print(f"   Total units:   {total_units:,}")

    # Read template
    print("\n📄 Reading v3 template...")
    template_html = template_path.read_text(encoding='utf-8')

    # Inject data
    generated_at = datetime.now().strftime('%b %d, %Y %I:%M %p')
    output_html = inject_data_into_template(
        template_html, daily_js, products_js, product_metrics, generated_at
    )

    # Write output (CDN version)
    output_path.write_text(output_html, encoding='utf-8')
    print(f"\n✅ Dashboard saved: {output_path}")

    # Also create self-contained share version with inlined Chart.js
    share_html = inline_chartjs(output_html)
    share_output.write_text(share_html, encoding='utf-8')
    print(f"✅ Share version:  {share_output}")

    # Summary
    print(f"\n{'='*60}")
    print(f"  Dashboard generated with LIVE data!")
    print(f"  {len(daily_js)} days | {len(products_js)} products | ${total_rev:,.2f} revenue")
    print(f"  Open amazon_dashboard_live.html in your browser")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
