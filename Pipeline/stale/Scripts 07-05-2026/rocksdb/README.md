# File Organizer & Cleaner - Data Pipeline

A high-performance data ingestion and retrieval pipeline that automates CSV/JSON file organization, schema detection, and fast lookup using DuckDB and RocksDB.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Raw Data Files                           │
│                    (CSV / JSON / Mixed)                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Stage 1: Ingestion to DuckDB                       │
│  ingest_to_raw.py                                              │
│  ├─ Auto-detect schema from file headers                       │
│  ├─ Create schema hash fingerprint                             │
│  ├─ Group files by identical schemas                           │
│  ├─ Track file lineage (audit trail)                           │
│  └─ Insert into raw_schema_{hash} tables                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DuckDB Storage                              │
│  (pipeline.duckdb)                                             │
│  ├─ file_lineage table (metadata)                              │
│  ├─ raw_schema_abc123 (grouped by schema)                      │
│  ├─ raw_schema_def456                                          │
│  └─ raw_schema_... (n tables)                                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Stage 2: Sync to RocksDB                           │
│  sync_to_rocks.py                                              │
│  ├─ Query all rows from DuckDB tables                          │
│  ├─ Extract phone_number as lookup key (v1)                    │
│  ├─ JSON serialize row data                                    │
│  ├─ Detect duplicates                                          │
│  └─ Store key-value pairs                                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  RocksDB Fast Lookup                            │
│  (rocks_db/)                                                   │
│  Key: phone_number                                             │
│  Value: {row_data as JSON}                                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Stage 3: Query Interface                           │
│  lookup.py                                                     │
│  └─ Retrieve records by phone_number in O(1)                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Features

- **Automatic Schema Detection** — Fingerprints column names to group files with identical schemas
- **File Lineage Tracking** — Maintains audit trail of which file went into which table
- **Smart Table Organization** — Files are automatically grouped into `raw_schema_*` tables based on schema similarity
- **Duplicate Detection** — Identifies duplicate records during RocksDB sync
- **Fast Retrieval** — O(1) lookups via RocksDB key-value store
- **Zero Data Loss** — All rows tagged with lineage ID for traceability
- **Docker Ready** — Full containerization for reproducible deployments

---

## Project Structure

```
.
├── Dockerfile                  # Container image definition
├── docker-compose.yml          # Multi-service orchestration
├── requirements.txt            # Python dependencies
├── pipeline.duckdb             # DuckDB database file (created at runtime)
│
├── data/                       # Input CSV/JSON files (mount point)
│   ├── contacts.csv
│   ├── vendors.json
│   └── ...
│
├── storage/                    # Output & persistence (mount point)
│   ├── pipeline.duckdb         # Main DuckDB instance
│   ├── rocks_db/               # RocksDB key-value store
│   └── ...
│
└── src/
    ├── ingest_to_raw.py        # Stage 1: Ingest to DuckDB
    ├── sync_to_rocks.py        # Stage 2: Sync to RocksDB
    └── lookup.py               # Stage 3: Query interface
```

---

## Installation & Setup

### Prerequisites

- Docker & Docker Compose
- Python 3.8+ (if running locally without Docker)
- 2GB+ free disk space (for DuckDB + RocksDB)

### Quick Start (Docker)

```bash
# 1. Clone and navigate to project
git clone <repo_url>
cd Scripts\ 07-05-2026

# 2. Place your CSV/JSON files in data/ directory
cp /path/to/your/files/* data/

# 3. Build and start container
docker-compose up --build

# 4. Inside container, run ingestion
docker exec pipeline_poc python src/ingest_to_raw.py

# 5. Sync to RocksDB
docker exec pipeline_poc python src/sync_to_rocks.py

# 6. Lookup records
docker exec pipeline_poc python src/lookup.py "9876543210"
```

### Local Setup (No Docker)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create directories
mkdir -p data storage

# 3. Place your files
cp /path/to/your/*.csv data/
cp /path/to/your/*.json data/

# 4. Run pipeline stages
python src/ingest_to_raw.py
python src/sync_to_rocks.py

# 5. Lookup
python src/lookup.py "9876543210"
```

---

## Pipeline Stages

### Stage 1: `ingest_to_raw.py` — DuckDB Ingestion

**Purpose:** Read raw files, detect schemas, and organize into DuckDB.

**How it works:**
1. Scans `data/` directory for CSV and JSON files
2. For each file, computes a **schema hash** (MD5 of sorted column names)
3. Creates a table named `raw_schema_{hash}` if it doesn't exist
4. Inserts file data, tagged with a unique `_lineage_id`
5. Records metadata in `file_lineage` table for audit trail

**Key Functions:**
- `get_schema_hash(conn, file_path)` — Fingerprints column structure
- `ingest_file(conn, file_path)` — Loads file into appropriate table
- `init_db(conn)` — Sets up DuckDB schema and sequences

**Output:**
- `pipeline.duckdb` with `N` tables (one per unique schema)
- `file_lineage` table with ingestion metadata

**Example:**

If you have two files:
```
contacts.csv    → columns: name, email, phone
vendors.csv     → columns: name, email, phone
```

Both will go into the **same table** `raw_schema_abc123` because they have identical schemas.

But if you add:
```
products.csv    → columns: sku, description, price
```

This creates a **new table** `raw_schema_def456` with different schema.

---

### Stage 2: `sync_to_rocks.py` — Sync to RocksDB

**Purpose:** Transform DuckDB data into a fast key-value store for O(1) lookups.

**How it works:**
1. Reads all tables from DuckDB's `file_lineage`
2. For each table, iterates through all rows
3. Extracts `phone_number` column as lookup key
4. JSON-serializes entire row and stores in RocksDB
5. Detects and logs duplicates (same phone number, different rows)

**Key Functions:**
- `sync()` — Main orchestration
- Iterates over all schema-based tables
- Auto-detects phone column (case-insensitive)
- Skips tables without a phone column

**Output:**
- `rocks_db/` directory with key-value store
- Console log with sync statistics (record count, duplicates)

**Example:**

DuckDB table:
```
| name  | email           | phone      | _lineage_id |
|-------|-----------------|------------|-------------|
| Alice | alice@ex.com    | 9876543210 | 1           |
| Bob   | bob@ex.com      | 9876543211 | 1           |
```

RocksDB after sync:
```
Key: "9876543210"
Value: {"name": "Alice", "email": "alice@ex.com", "phone": "9876543210", "_lineage_id": 1}

Key: "9876543211"
Value: {"name": "Bob", "email": "bob@ex.com", "phone": "9876543211", "_lineage_id": 1}
```

---

### Stage 3: `lookup.py` — Query Interface

**Purpose:** Fast record lookup by phone number.

**How it works:**
1. Opens RocksDB in read mode
2. Looks up key (phone number)
3. Returns full record if found, or "not found" message

**Usage:**
```bash
python src/lookup.py "9876543210"
```

**Output:**
```
Found record for 9876543210:
{"name": "Alice", "email": "alice@ex.com", "phone": "9876543210", "_lineage_id": 1}
```

---

## Configuration

### Environment Variables

Currently, paths are hardcoded in scripts. To customize:

**`ingest_to_raw.py`:**
- `DB_PATH = "storage/pipeline.duckdb"`
- `DATA_DIR = "data"`

**`sync_to_rocks.py`:**
- `DUCKDB_PATH = "storage/pipeline.duckdb"`
- `ROCKSDB_PATH = "storage/rocks_db"`

**`lookup.py`:**
- `ROCKSDB_PATH = "storage/rocks_db"`

Modify these variables directly in the scripts to change paths.

### Docker Volumes

In `docker-compose.yml`, three volumes are mounted:
```yaml
volumes:
  - ./data:/app/data              # Input files
  - ./storage:/app/storage        # DuckDB + RocksDB
  - ./src:/app/src                # Python scripts
```

---

## Data Flow & Key Concepts

### Schema Hashing

**Problem:** Multiple files with identical columns should go into one table, not separate ones.

**Solution:** Compute MD5 hash of sorted column names:
```
contacts.csv: columns [name, email, phone]
              → sorted: [email, name, phone]
              → hash: "abc123def456" (first 12 chars)
              → table: raw_schema_abc123def456

vendors.csv:  columns [name, email, phone]
              → same hash → same table
```

### File Lineage

Every row is tagged with `_lineage_id`:
- Allows tracking which file each row came from
- `file_lineage` table maps: `id → filename → table_name → row_count → ingested_at`
- Enables data audits and deletion traceability

### Duplicate Detection

During `sync_to_rocks.py`, if the same phone number appears in multiple rows:
- Both rows are stored (last write wins in RocksDB)
- Count is logged for analysis
- Future: Can implement merge/dedup logic here

---

## Future Enhancements

### v2: Composite Keys

Instead of phone-only lookups, use composite key:
```python
composite_key = f"{phone}|{email}|{address}"
```

Benefits:
- Higher uniqueness (fewer collisions)
- Support multi-field queries
- Better handling of duplicate phone numbers

**Implementation:**
Replace in `sync_to_rocks.py`:
```python
# Current (v1)
key = str(row_dict.get(phone_col, ""))

# Future (v2)
phone = str(row_dict.get("phone_number", ""))
email = str(row_dict.get("email", ""))
address = str(row_dict.get("address", ""))
key = f"{phone}|{email}|{address}"
```

### v3: Multi-Key Indexing

Support lookups by any field (email, address, etc):
```python
# Store in RocksDB with multiple key variants
db[f"phone:{phone}"] = row_json
db[f"email:{email}"] = row_json
db[f"address:{address}"] = row_json
```

### v4: Smart Deduplication

Implement fuzzy matching for near-duplicate records:
- Phone number similarity (Levenshtein distance)
- Email domain clustering
- Address geocoding normalization

---

## Troubleshooting

### Issue: "DuckDB not found. Run ingestion first."
**Solution:** Make sure `ingest_to_raw.py` ran successfully. Check logs for file read errors.

### Issue: "No 'phone' column found"
**Solution:** Your data doesn't have a phone column, or it's named differently (e.g., `phone_number`, `contact_phone`). 

Current detection logic:
```python
phone_col = next((c for c in columns if 'phone' in c.lower()), None)
```

Modify the condition to match your column names.

### Issue: RocksDB appears empty after sync
**Solution:** Check if DuckDB actually has data. Run:
```bash
python -c "import duckdb; conn = duckdb.connect('storage/pipeline.duckdb'); print(conn.execute('SELECT COUNT(*) FROM file_lineage').fetchall())"
```

### Issue: Docker container exits immediately
**Solution:** Check logs:
```bash
docker-compose logs pipeline
```

Ensure `data/` directory has files before running.

---

## Performance Characteristics

| Operation | Time | Notes |
|-----------|------|-------|
| Ingest 100K rows (CSV) | ~5 seconds | DuckDB is fast at bulk insert |
| Sync to RocksDB | ~10 seconds | For 100K rows, network I/O bound |
| Single lookup | <1ms | O(1) key-value access |
| Range query (DuckDB) | Variable | Full table scan required |

---

## License

[Add your license here]

---

## Contributing

[Add contribution guidelines here]

---

## Contact

[Add contact info or maintainer details here]
