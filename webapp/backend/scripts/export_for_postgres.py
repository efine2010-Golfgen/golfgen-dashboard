"""Export all DuckDB tables to CSV for PostgreSQL import."""
import duckdb, os
from pathlib import Path

db_path = os.environ.get('DB_PATH', '/app/data/golfgen.duckdb')
export_dir = Path('/app/data/pg_export')
export_dir.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(db_path, read_only=True)
tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

print(f"Exporting {len(tables)} tables to {export_dir}")
summary = {}
for table in tables:
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        path = export_dir / f"{table}.csv"
        con.execute(f"COPY {table} TO '{path}' (HEADER, DELIMITER ',')")
        size_kb = path.stat().st_size // 1024
        summary[table] = {'rows': count, 'size_kb': size_kb}
        print(f"  ✓ {table}: {count} rows, {size_kb}KB")
    except Exception as e:
        print(f"  ✗ {table}: {e}")

con.close()

# Write manifest file
manifest = "\n".join([f"{t}: {v['rows']} rows, {v['size_kb']}KB" for t, v in summary.items()])
(export_dir / 'MANIFEST.txt').write_text(manifest)
print(f"\nExport complete. Manifest written to {export_dir}/MANIFEST.txt")
print("Ready for PostgreSQL import when Railway PostgreSQL is provisioned.")
