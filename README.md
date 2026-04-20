# File Organizer and Data Integrity Workflow

A complete pipeline for organizing files, cataloging them, inferring schemas, and scanning data integrity. This system combines bash automation with Python data analysis to provide end-to-end visibility into file organization and quality metrics.

## Project Overview

This project implements a three-stage workflow:

1. **File Organization & Cataloging** (bash)
2. **Data Integrity Analysis** (Python)
3. **Schema Detection & Classification** (Python)

All stages work together seamlessly with minimal manual intervention.

---

## Stage 1: File Organization and CSV Inventory

### Script: `file_organizer.sh`

**Purpose:** Recursively scan a directory, categorize files by type, organize them into folders, and automatically generate a CSV inventory.

**What it does:**
- Scans all subdirectories recursively
- Identifies and counts: CSV, XLSX, XLS, JSON files
- Creates organized folders (CSV/, XLSX/, XLS/, JSON/)
- Moves files into appropriate folders
- Automatically calls `csv_builder.sh` to generate inventory
- Generates `file_analysis_report.md`
- Handles archives and unexpected file types
- No files are deleted, only moved

**Usage:**

```bash
chmod +x file_organizer.sh
chmod +x csv_builder.sh

# Dry-run (preview without moving)
./file_organizer.sh -s /path/to/source -d

# Full run (organizes files and creates inventory)
./file_organizer.sh -s /path/to/source
```

**Output:**
- Organized folders: `CSV/`, `XLSX/`, `XLS/`, `JSON/`
- `file_analysis_report.md` - Summary of organization results
- `file_inventory.csv` - Catalog with schema: `serial_number, filename, file_type, file_path`

**Options:**
- `-s SOURCE_DIR` - Source directory to scan (required)
- `-d` - Dry-run mode (preview changes without executing)
- `-v` - Verbose mode (detailed logging)
- `-o OUTPUT_DIR` - Custom output directory (optional)
- `-m` - Merge Excel files (XLS/XLSX, use with caution)

**Important Notes:**
- XLS (binary) and XLSX (XML) are different formats
- Excel files with macros may lose VBA code when merged
- Always dry-run first with `-d` flag
- Requires `csv_builder.sh` to be in the same directory

---

## Stage 2: Data Integrity Analysis

### Script: `add_schema_to_csv.py`

**Purpose:** Analyze files from the raw inventory and compute data quality metrics.

**What it does:**
- Reads the CSV inventory from Stage 1
- For each data file, computes:
  - **null_pct** - Percentage of null/empty/NaN cells
  - **field_deviation_pct** - % of rows with field count != modal count (Strategy 1)
  - **type_deviation_pct** - % of cells whose inferred type deviates from column mode (Strategy 2)
  - **entropy_delta_pct** - Shannon entropy deviation from baseline
  - **total_rows** - Data row count (excludes header)
  - **total_cells** - Total cell count
  - **error** - Any parse/IO error message (blank if clean)

**Header Detection Strategy:**
- Looks for common patterns: email addresses, phone numbers, numeric IDs
- If >20% of first row contains pure numbers, treats as data not headers
- Detects column naming conventions (snake_case, camelCase, etc.)
- Flags inconsistencies in type distributions across columns
- Provides confidence scores and detection reasoning

**Usage - Core Python:**

```bash
# Analyze with core Python (sufficient for local systems)
python data_integrity_scanner.py /path/to/file_inventory.csv

# Custom output path
python data_integrity_scanner.py /path/to/file_inventory.csv /path/to/output.csv
```

**Usage - Apache Spark:**

```bash
# Analyze with Spark (for large datasets or cloud deployments)
python data_integrity_scanner_spark.py /path/to/file_inventory.csv

# Custom output path
python data_integrity_scanner_spark.py /path/to/file_inventory.csv /path/to/output.csv
```

**Environment Variables:**

Set which LLM to use for ambiguous cases:

```bash
# Use Gemini (default: Gemma if not set)
export API_CHOICE=gemini
python add_schema_to_csv.py file_inventory.csv

# Use Gemma
export API_CHOICE=gemma
python add_schema_to_csv.py file_inventory.csv
```

**Output Columns:**
All input columns plus:
- `null_pct` - 0-100, percentage of empty/null cells
- `field_deviation_pct` - 0-100, comma injection or row structural issues
- `type_deviation_pct` - 0-100, type consistency issues
- `entropy_delta_pct` - 0-100, entropy deviation from uniform baseline
- `total_rows` - Integer, count of data rows
- `total_cells` - Integer, total cells = rows × columns
- `error` - String, error message if file couldn't be read

**Example Output Schema:**
```json
[
  {"name": "user_id", "type": "long", "nullable": false},
  {"name": "email", "type": "string", "nullable": true},
  {"name": "signup_date", "type": "timestamp", "nullable": true}
]
```

**Limitations & Considerations:**
- Requires the CSV output from Stage 2 (has `file_path` column)
- LLM calls are made for ambiguous cases (requires API keys for Gemini/Gemma)
- Preserves all existing columns from input CSV
- Depends on: `call_gemini_chat.py` or `call_gemma_chat.py`

---

## Stage 3: Schema Detection and Classification

### Scripts: `data_integrity_scanner.py` or `data_integrity_scanner_spark.py`

**Purpose:** Analyze the CSV inventory from Stage 2, detect headers in each file, infer column schemas, and handle ambiguous cases via LLM. Preserves existing columns (like integrity metrics).

Two implementations available:

1. **`data_integrity_scanner.py`** - Core Python (pandas, numpy)
2. **`data_integrity_scanner_spark.py`** - Apache Spark for distributed processing

Both produce identical metrics. On a local system, performance is the same. On cloud/cluster environments, Spark version scales horizontally.

**What it does:**
- Reads the CSV inventory from Stage 2 (with metrics)
- For each file:
  - Detects if it has a header row
  - Flags ambiguous cases (e.g., first row contains data that looks like headers)
  - Infers schema for files with headers
  - Generates header suggestions for files without headers
- Sends ambiguous cases to LLM (Gemini or Gemma) for manual review
- Appends schema results while preserving all existing columns

**Supported File Types:** CSV, TSV, XLSX, XLS, JSON

**Usage:**

```bash
# Required: Input CSV from data_integrity_scanner
python add_schema_to_csv.py /path/to/file_inventory_deviation.csv

# Optional: Specify custom output path
python add_schema_to_csv.py /path/to/file_inventory_deviation.csv --output /path/to/output.csv
```

**Output Columns:**
All input columns (from Stage 2) plus:
- `has_header` - Boolean: true if header row detected
- `header_confidence` - 0.0 to 1.0 confidence score
- `needs_llm_review` - Boolean: true if ambiguous
- `detection_reason` - Human-readable reasoning
- `schema` - JSON array of column definitions with type and nullable info
- `generated_headers` - JSON array of LLM-suggested headers (for headerless files)

**Example Interpretation:**
```
file_type=CSV, null_pct=2.1, field_deviation_pct=0.5, type_deviation_pct=3.2
→ Clean data: low null %, almost no structural issues, minor type inconsistencies
```

**Performance Notes:**
- **Local System (Single Driver):** Both implementations run at similar speed
- **Cloud/Cluster (Distributed):** Spark version can parallelize across workers
- Spark version is beneficial when processing 100s of files or GBs of data

---

## Complete Workflow Example

### Step 1: Organize and Catalog

```bash
cd /path/to/project
chmod +x file_organizer.sh csv_builder.sh

# Preview organization
./file_organizer.sh -s ~/data/raw -d

# Execute organization
./file_organizer.sh -s ~/data/raw
```

**Generates:**
- Organized folders: `CSV/`, `XLSX/`, `XLS/`, `JSON/`
- `file_inventory.csv` - Raw catalog

### Step 2: Scan Data Integrity

```bash
# First, compute data quality metrics
python data_integrity_scanner_spark.py file_inventory.csv --output file_inventory_deviation.csv
```

**Output:** `file_inventory_deviation.csv` - Inventory with integrity metrics

### Step 3: Detect Schemas

```bash
# Then, enrich with schema and header info (preserves existing metrics)
export API_CHOICE=gemini
python add_schema_to_csv.py file_inventory_deviation.csv --output file_inventory_schema.csv
```

**Output:** `file_inventory_schema.csv` - Full inventory with metrics + schema, confidence scores, and LLM flags

Check for LLM reviews:

```bash
# View files that need manual review
grep "true" file_inventory_schema.csv | grep "needs_llm_review"
```

### Analyze Results

```bash
# View high-risk files (null% > 10% or deviation% > 5%)
python -c "
import pandas as pd
df = pd.read_csv('file_inventory_schema.csv')
risk = df[(df['null_pct'] > 10) | (df['field_deviation_pct'] > 5)]
print(risk[['filename', 'null_pct', 'field_deviation_pct', 'error']])
"
```

---

## Directory Structure

```
File-Organizer-and-Cleaner-AI-Workflow/
├── file_organizer.sh                  # Stage 1: Organize files
├── csv_builder.sh                     # Called by file_organizer.sh
├── add_schema_to_csv.py               # Stage 2: Detect schemas
├── call_gemini_chat.py                # LLM integration (Gemini)
├── call_gemma_chat.py                 # LLM integration (Gemma)
├── data_integrity_scanner.py          # Stage 3: Analyze (Python)
├── data_integrity_scanner_spark.py    # Stage 3: Analyze (Spark)
├── README.md                          # This file
├── Deviation Plan.txt                 # Design & strategy notes
├── docker-compose.yml                 # For containerized execution
├── Data/                              # Sample data directories
├── Docs/                              # Documentation
└── Sample/                            # Example outputs
```

---

## Requirements

### For Bash Scripts
- Bash 4.0+
- Standard Unix tools: find, grep, sed, awk, date
- Read/write permissions on source directory

### For Python Scripts
- Python 3.8+
- Core Python: `data_integrity_scanner.py`
  - pandas
  - numpy
  - openpyxl (for Excel files)
- Apache Spark: `data_integrity_scanner_spark.py`
  - pyspark
  - pandas
- LLM Integration: `add_schema_to_csv.py`
  - gemini-api or similar (depends on API_CHOICE)
  - requests

### Optional
- Docker & Docker Compose (if using containerized execution)

---

## Troubleshooting

**Script won't run (permission denied)**
```bash
chmod +x file_organizer.sh csv_builder.sh
```

**Path not found**
- Use full absolute paths: `/home/user/data` not `~/data`
- Verify directory exists: `ls -la /path/to/dir`

**CSV inventory missing**
- Ensure `csv_builder.sh` is executable and in same directory
- Check write permissions: `touch /path/to/dir/test.txt`

**LLM API fails in Stage 2**
- Verify API key is set: `echo $API_CHOICE`
- Check API credentials in `call_gemini_chat.py` or `call_gemma_chat.py`
- For ambiguous files, manually review detection_reason column

**Data Integrity Scanner fails**
- Ensure CSV inventory from Stage 2 has `file_path` column
- Verify file paths are still valid (files not moved)
- Check file permissions and encoding

---

## Data Flow Diagram

```
Raw Files (scattered)
        |
        v
[file_organizer.sh]
        |
        +-> Organized folders (CSV/, XLSX/, etc.)
        +-> file_inventory.csv (raw catalog)
        |
        v
[add_schema_to_csv.py]
        |
        +-> Calls LLM for ambiguous headers
        +-> file_inventory_schema.csv (enriched)
        |
        v
[data_integrity_scanner.py / .spark.py]
        |
        +-> Quality metrics computed
        +-> file_inventory_integrity.csv (final)
        |
        v
Analysis & Reporting
```

---

## Notes

- Stage 1 requires no API keys or external services
- Stage 2 requires LLM API credentials (Gemini or Gemma)
- Stage 3 works offline; both Python and Spark versions produce identical results
- For distributed processing or cloud deployment, use Spark version of Stage 3
- All stages are idempotent; safe to re-run without data loss

