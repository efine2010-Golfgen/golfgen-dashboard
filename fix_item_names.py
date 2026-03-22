import re

# ─── 1. _cogs_helper.py: add load_item_master_names() ────────────────────────
cogs_path = "/Users/eric/Projects/golfgen-dashboard/webapp/backend/routers/_cogs_helper.py"
with open(cogs_path) as f:
    src = f.read()

old1 = 'def load_unit_costs() -> dict:'
new1 = '''def load_item_master_names() -> dict:
    """Load clean product names from Item Master CSV (user-edited, highest priority).

    Returns {asin: product_name} for all ASINs that have a non-empty name
    that isn\'t just the ASIN itself.
    """
    names = {}
    if not ITEM_MASTER_PATH.exists():
        return names
    try:
        with open(ITEM_MASTER_PATH, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                asin = (row.get("asin") or "").strip()
                name = (row.get("product_name") or "").strip()
                if asin and name and name.upper() != asin.upper():
                    names[asin] = name
    except Exception as e:
        logger.warning(f"load_item_master_names: CSV read error: {e}")
    return names


def load_unit_costs() -> dict:'''

assert src.count(old1) == 1, f"old1 match: {src.count(old1)}"
src = src.replace(old1, new1)
with open(cogs_path, "w") as f:
    f.write(src)
print("✓ 1  _cogs_helper.py: load_item_master_names() added")

# ─── 2. sales.py: import load_item_master_names ───────────────────────────────
sales_path = "/Users/eric/Projects/golfgen-dashboard/webapp/backend/routers/sales.py"
with open(sales_path) as f:
    src = f.read()

old2 = 'from routers._cogs_helper import compute_cogs_for_range, load_unit_costs'
new2 = 'from routers._cogs_helper import compute_cogs_for_range, load_unit_costs, load_item_master_names'
assert src.count(old2) == 1
src = src.replace(old2, new2)
print("✓ 2  sales.py: import updated")

# ─── 3. sales.py: call load_item_master_names() at top of _build_product_list ─
old3 = '''    cogs_data = load_cogs()
    # Prefer item_master unit_cost over cogs.csv values
    _unit_costs = load_unit_costs()'''
new3 = '''    cogs_data = load_cogs()
    # Item Master CSV names — user-edited clean descriptions (highest priority)
    im_names = load_item_master_names()
    # Prefer item_master unit_cost over cogs.csv values
    _unit_costs = load_unit_costs()'''
assert src.count(old3) == 1, f"old3 match: {src.count(old3)}"
src = src.replace(old3, new3)
print("✓ 3  sales.py: im_names loaded in _build_product_list")

# ─── 4. sales.py: use im_names as highest-priority name source ────────────────
old4 = '''        cogs_name = cogs_info.get("product_name", "")
        if cogs_name and cogs_name.strip().upper() == asin.upper():
            cogs_name = ""
        name = (cogs_name
                or api_name
                or inv_info.get("product_name")
                or asin)'''
new4 = '''        im_name = im_names.get(asin, "")
        cogs_name = cogs_info.get("product_name", "")
        if cogs_name and cogs_name.strip().upper() == asin.upper():
            cogs_name = ""
        name = (im_name
                or cogs_name
                or api_name
                or inv_info.get("product_name")
                or asin)'''
assert src.count(old4) == 1, f"old4 match: {src.count(old4)}"
src = src.replace(old4, new4)
print("✓ 4  sales.py: im_name used as top priority in name resolution")

# ─── 5. sales.py: fix product_detail endpoint name resolution ─────────────────
old5 = '        "name": cogs_info.get("product_name", asin),'
new5 = '        "name": load_item_master_names().get(asin) or cogs_info.get("product_name") or asin,'
assert src.count(old5) == 1, f"old5 match: {src.count(old5)}"
src = src.replace(old5, new5)
print("✓ 5  sales.py: product_detail endpoint name fixed")

with open(sales_path, "w") as f:
    f.write(src)

# ─── 6. item_master.py: sync product_name to DB on every PUT save ────────────
im_path = "/Users/eric/Projects/golfgen-dashboard/webapp/backend/routers/item_master.py"
with open(im_path) as f:
    src = f.read()

old6 = '''    save_item_master(items)
    return {"status": "ok", "asin": asin}'''
new6 = '''    save_item_master(items)
    # Sync product_name to DB item_master table so _build_product_list picks it up immediately
    try:
        from core.database import get_db_rw
        updated_item = next((i for i in items if i["asin"] == asin), None)
        if updated_item and "productName" in body:
            con_rw = get_db_rw()
            con_rw.execute(
                "UPDATE item_master SET product_name = ? WHERE asin = ?",
                [updated_item.get("productName", ""), asin]
            )
            con_rw.close()
    except Exception as _e:
        logger.warning(f"item_master PUT: DB sync warning: {_e}")
    return {"status": "ok", "asin": asin}'''
assert src.count(old6) == 1, f"old6 match: {src.count(old6)}"
src = src.replace(old6, new6)

with open(im_path, "w") as f:
    f.write(src)
print("✓ 6  item_master.py: PUT endpoint syncs product_name to DB")

print("\n✅ All changes applied")
