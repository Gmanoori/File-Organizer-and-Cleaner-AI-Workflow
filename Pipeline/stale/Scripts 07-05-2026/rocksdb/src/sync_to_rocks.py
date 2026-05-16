import duckdb
import json
import os
from rocksdict import Rdict, Options

DUCKDB_PATH = "storage/pipeline.duckdb"
ROCKSDB_PATH = "storage/rocks_db"

def sync():
    if not os.path.exists(DUCKDB_PATH):
        print("DuckDB not found. Run ingestion first.")
        return

    conn = duckdb.connect(DUCKDB_PATH)
    
    # Get all unique tables from lineage
    tables = conn.execute("SELECT DISTINCT table_name FROM file_lineage").fetchall()
    if not tables:
        print("No tables found in file_lineage.")
        return

    opts = Options()
    opts.create_if_missing(True)
    db = Rdict(ROCKSDB_PATH, options=opts)

    total_count = 0
    total_dupes = 0

    for (table_name,) in tables:
        print(f"\nSyncing table: {table_name}")
        
        cursor = conn.execute(f"SELECT * FROM {table_name}")
        columns = [desc[0] for desc in cursor.description]
        
        phone_col = next((c for c in columns if 'phone' in c.lower()), None)
        if not phone_col:
            print(f"Skipping {table_name}: No 'phone' column found. Cols: {columns}")
            continue

        count = 0
        dupes = 0
        while True:
            row = cursor.fetchone()
            if not row: break
            
            row_dict = dict(zip(columns, row))
            key = str(row_dict.get(phone_col, ""))
            if not key: continue
                
            if key in db: dupes += 1
            db[key] = json.dumps(row_dict, default=str)
            count += 1
            if count % 10000 == 0: print(f"  Synced {count} records...")

        print(f"  Table Complete. Processed: {count}, Duplicates: {dupes}")
        total_count += count
        total_dupes += dupes

    db.close()
    conn.close()
    print(f"\nFinal Sync Result:")
    print(f"Total Processed: {total_count}")
    print(f"Total Duplicates: {total_dupes}")

if __name__ == "__main__":
    sync()
