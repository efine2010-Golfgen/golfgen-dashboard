import duckdb, os
from datetime import datetime
from zoneinfo import ZoneInfo

db_path = os.environ.get('DB_PATH', '/app/data/golfgen.duckdb')
con = duckdb.connect(db_path, read_only=False)
now = datetime.now(ZoneInfo("America/Chicago")).isoformat()
print(f"Starting Phase 2 migration at {now}")
print(f"DB path: {db_path}")

# Tables that need the three hierarchy columns
tables = [
    'orders', 'order_items', 'financial_events',
    'fba_inventory', 'daily_sales', 'advertising', 'ads_campaigns'
]

for table in tables:
    for col, default in [
        ('division', 'unknown'),
        ('customer', 'unknown'),
        ('platform', 'unknown')
    ]:
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {col} VARCHAR DEFAULT '{default}'")
            print(f"  ✓ Added {col} to {table}")
        except Exception as e:
            if 'already exists' in str(e).lower():
                print(f"  — {col} already exists in {table}")
            else:
                print(f"  ✗ ERROR adding {col} to {table}: {e}")

# Tag all existing Amazon rows with customer='amazon', platform='sp_api'
# Division will be set later when Eric tags item_master
for table in ['orders', 'financial_events', 'fba_inventory', 'daily_sales']:
    try:
        con.execute(f"""
            UPDATE {table}
            SET customer = 'amazon', platform = 'sp_api'
            WHERE customer = 'unknown' OR customer IS NULL
        """)
        print(f"  ✓ Tagged existing {table} rows: customer=amazon, platform=sp_api")
    except Exception as e:
        print(f"  ✗ ERROR tagging {table}: {e}")

# Add division + category + brand to item_master
for col, typ in [('division','VARCHAR'), ('category','VARCHAR'), ('brand','VARCHAR')]:
    try:
        con.execute(f"ALTER TABLE item_master ADD COLUMN {col} {typ}")
        print(f"  ✓ Added {col} to item_master")
    except Exception as e:
        if 'already exists' in str(e).lower():
            print(f"  — {col} already exists in item_master")
        else:
            print(f"  ✗ ERROR: {e}")

# Verify
print("\n=== VERIFICATION ===")
for table in ['orders', 'financial_events', 'daily_sales', 'item_master']:
    try:
        cols = [r[0] for r in con.execute(f'DESCRIBE {table}').fetchall()]
        has = {c: c in cols for c in ['division','customer','platform']}
        count = con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        print(f"  {table}: rows={count}, division={has['division']}, customer={has['customer']}, platform={has['platform']}")
    except Exception as e:
        print(f"  {table}: ERROR - {e}")

con.close()
print("\nPhase 2 migration complete.")
