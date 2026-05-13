import duckdb
import os
import glob
import hashlib
from datetime import datetime

DB_PATH = "storage/pipeline.duckdb"
DATA_DIR = "data"

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS file_lineage (
            id INTEGER PRIMARY KEY,
            filename TEXT UNIQUE,
            table_name TEXT,
            ingested_at TIMESTAMP,
            row_count INTEGER
        );
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_lineage_id START 1;")

def get_schema_hash(conn, file_path):
    # Get column names without loading the whole file
    try:
        if file_path.endswith('.csv'):
            res = conn.execute(f"DESCRIBE SELECT * FROM read_csv_auto('{file_path}') LIMIT 0").fetchall()
        elif file_path.endswith('.json'):
            res = conn.execute(f"DESCRIBE SELECT * FROM read_json_auto('{file_path}') LIMIT 0").fetchall()
        else:
            return None
        
        # Create a sorted string of column names to represent the schema
        cols = sorted([row[0] for row in res])
        schema_str = ",".join(cols)
        return hashlib.md5(schema_str.encode()).hexdigest()[:12]
    except Exception as e:
        print(f"Error fingerprinting {file_path}: {e}")
        return None

def ingest_file(conn, file_path):
    filename = os.path.basename(file_path)
    
    exists = conn.execute("SELECT 1 FROM file_lineage WHERE filename = ?", [filename]).fetchone()
    if exists:
        print(f"Skipping {filename}: already ingested.")
        return

    schema_hash = get_schema_hash(conn, file_path)
    if not schema_hash:
        return

    table_name = f"raw_schema_{schema_hash}"
    lineage_id = conn.execute("SELECT nextval('seq_lineage_id')").fetchone()[0]

    print(f"Ingesting {filename} into {table_name}...")
    
    try:
        # 1. Ensure table exists with correct schema + lineage column
        if not conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]:
            if filename.endswith('.csv'):
                conn.execute(f"CREATE TABLE {table_name} AS SELECT *, 0 as _lineage_id FROM read_csv_auto('{file_path}', all_varchar=True) LIMIT 0")
            else:
                conn.execute(f"CREATE TABLE {table_name} AS SELECT *, 0 as _lineage_id FROM read_json_auto('{file_path}') LIMIT 0")

        # 2. Insert data
        if filename.endswith('.csv'):
            conn.execute(f"INSERT INTO {table_name} SELECT *, {lineage_id} FROM read_csv_auto('{file_path}', all_varchar=True)")
        else:
            conn.execute(f"INSERT INTO {table_name} SELECT *, {lineage_id} FROM read_json_auto('{file_path}')")
        
        row_count = conn.execute(f"SELECT count(*) FROM {table_name} WHERE _lineage_id = ?", [lineage_id]).fetchone()[0]
        
        conn.execute("""
            INSERT INTO file_lineage (id, filename, table_name, ingested_at, row_count)
            VALUES (?, ?, ?, ?, ?)
        """, [lineage_id, filename, table_name, datetime.now(), row_count])
        
        print(f"Successfully ingested {filename} ({row_count} rows).")
        
    except Exception as e:
        print(f"Error ingesting {filename}: {e}")

def main():
    if not os.path.exists("storage"): os.makedirs("storage")
    conn = duckdb.connect(DB_PATH)
    init_db(conn)
    for f in glob.glob(os.path.join(DATA_DIR, "*.*")):
        if f.endswith(('.csv', '.json')): ingest_file(conn, f)
    conn.close()

if __name__ == "__main__":
    main()
