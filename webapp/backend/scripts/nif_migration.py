"""NIF Item Master embedded migration — loads seed data on startup."""
import json
import zlib
import base64
import logging

logger = logging.getLogger("golfgen")

# Compressed NIF items (69 items across 2024/2025/2026)
# Keys: ey=event_year, ew=effective_week, st=item_status, br=brand, mt=mod_type,
#        de=description, wi=wmt_item_number, up=upc, vs=vendor_stock_number,
#        bi=brand_id, wc=wholesale_cost, wr=walmart_retail, vp=vendor_pack,
#        wp=whse_pack, os=old_store_count, ns=new_store_count, sd=store_count_diff,
#        cl=carton_length, cw=carton_width, ch=carton_height, cb=cbm, cf=cbf,
#        dx=dexterity, ca=category, co=color
_DATA = "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"


def _load_items() -> list[dict]:
    """Decompress and return embedded NIF items."""
    try:
        return json.loads(zlib.decompress(base64.b64decode(_DATA)))
    except Exception as e:
        logger.error(f"NIF migration: failed to decompress data: {e}")
        return []


def run_nif_migration():
    """Load NIF items into walmart_nif_items if table is empty."""
    from core.database import get_db, get_db_rw

    con = get_db()
    needs_reload = False
    try:
        # Check if table exists and has data
        try:
            row = con.execute("SELECT COUNT(*) FROM walmart_nif_items").fetchone()
            if row and row[0] > 0:
                # Check if existing data has category/color (v2 migration)
                cat_row = con.execute(
                    "SELECT COUNT(*) FROM walmart_nif_items WHERE category IS NOT NULL AND category != ''"
                ).fetchone()
                if cat_row and cat_row[0] > 0:
                    logger.info(f"NIF migration: table has {row[0]} rows with category data, skipping")
                    return
                else:
                    logger.info("NIF migration: existing data lacks category/color — will reload")
                    needs_reload = True
        except Exception:
            logger.info("NIF migration: table doesn't exist yet, will create on next startup")
            return
    finally:
        con.close()

    items = _load_items()
    if not items:
        logger.warning("NIF migration: no embedded data found")
        return

    con = get_db_rw()
    try:
        if needs_reload:
            con.execute("DELETE FROM walmart_nif_items")
            logger.info("NIF migration: cleared old data for v2 reload")
        count = 0
        for item in items:
            try:
                con.execute("""
                    INSERT INTO walmart_nif_items (
                        event_year, effective_week, item_status, description, brand,
                        wmt_item_number, upc, vendor_stock_number, brand_id,
                        wholesale_cost, walmart_retail, vendor_pack, whse_pack,
                        old_store_count, new_store_count, store_count_diff,
                        carton_length, carton_width, carton_height, cbm, cbf,
                        color, dexterity, category, mod_type,
                        division, customer, platform
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'golf', 'walmart_stores', 'scintilla')
                """, [
                    item.get("ey"), item.get("ew"), item.get("st"),
                    item.get("de"), item.get("br"),
                    item.get("wi"), item.get("up"), item.get("vs"), item.get("bi"),
                    item.get("wc", 0), item.get("wr", 0),
                    item.get("vp", 0), item.get("wp", 0),
                    item.get("os", 0), item.get("ns", 0), item.get("sd", 0),
                    item.get("cl", 0), item.get("cw", 0), item.get("ch", 0),
                    item.get("cb", 0), item.get("cf", 0),
                    item.get("co"), item.get("dx"),
                    item.get("ca"), item.get("mt"),
                ])
                count += 1
            except Exception as e:
                logger.warning(f"NIF migration: skip item {item.get('de')}: {e}")

        logger.info(f"NIF migration: loaded {count}/{len(items)} items")
    finally:
        con.close()
