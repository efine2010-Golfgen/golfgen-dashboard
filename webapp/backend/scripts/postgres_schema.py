"""
Generates PostgreSQL-compatible CREATE TABLE statements from DuckDB schema.
Run this to get the schema ready before provisioning PostgreSQL.
"""
import duckdb, os

db_path = os.environ.get('DB_PATH', '/app/data/golfgen.duckdb')
con = duckdb.connect(db_path, read_only=True)
tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]

# DuckDB to PostgreSQL type mapping
type_map = {
    'VARCHAR': 'TEXT',
    'INTEGER': 'INTEGER',
    'BIGINT': 'BIGINT',
    'DECIMAL': 'NUMERIC',
    'BOOLEAN': 'BOOLEAN',
    'TIMESTAMP': 'TIMESTAMP',
    'DATE': 'DATE',
    'DOUBLE': 'DOUBLE PRECISION',
    'FLOAT': 'REAL',
}

print("-- PostgreSQL schema generated from DuckDB")
print("-- Run this in PostgreSQL after provisioning\n")

for table in tables:
    cols = con.execute(f"DESCRIBE {table}").fetchall()
    print(f"CREATE TABLE IF NOT EXISTS {table} (")
    col_lines = []
    for col in cols:
        name, dtype = col[0], col[1]
        # Map type
        pg_type = type_map.get(dtype.split('(')[0].upper(), 'TEXT')
        if 'DECIMAL' in dtype or 'NUMERIC' in dtype:
            pg_type = dtype.replace('DECIMAL', 'NUMERIC')
        col_lines.append(f"    {name} {pg_type}")
    print(",\n".join(col_lines))
    print(");\n")

con.close()
