# Data Processing and Integrity Workflow

A comprehensive pipeline for organizing files, analyzing data integrity, detecting schemas, and cleaning data. Combines bash automation with Python data analysis for end-to-end file processing and quality assurance.

## Overview

This project provides a multi-stage workflow for data processing:

1. File Organization and Cataloging
2. Data Integrity Analysis
3. Schema Detection and Classification
4. Data Cleaning and Standardization

## Directory Structure

```
700GB Cleaning Shi/
├── Scripts/
│   ├── Bash/
│   │   ├── csv_builder.sh              # CSV inventory generation
│   │   └── file_organizer.sh           # File organization and cataloging
│   └── Python/
│       ├── add_schema_to_csv.py        # Schema detection with LLM
│       ├── call_gemini_chat.py         # Gemini API integration
│       ├── call_gemma_chat.py          # Gemma API integration
│       ├── data_cleaner_spark.py       # Data cleaning and standardization
│       ├── data_integrity_scanner.py   # Integrity analysis (Python)
│       └── data_integrity_scanner_spark.py # Integrity analysis (Spark)
├── Data/                               # Source data directories
├── Cleaned/                            # Cleaned data output
├── cleaned_data/                       # Alternative cleaned data output
├── Output/                             # Analysis reports and logs
├── Sample/                             # Example data and outputs
├── Docs/                               # Documentation
├── reqs.txt                            # Python dependencies
├── docker-compose.yml                  # Container configuration
├── Cleaning Plan.txt                   # Processing strategy notes
├── Deviation Plan.txt                  # Analysis methodology
└── README.md                           # This file
```

## Stage 1: File Organization

**Scripts:** `Scripts/Bash/file_organizer.sh`, `Scripts/Bash/csv_builder.sh`

Organizes scattered files into categorized folders and generates CSV inventory.

**Usage:**
```bash
cd Scripts/Bash
chmod +x file_organizer.sh csv_builder.sh
./file_organizer.sh -s /path/to/data
```

**Output:** Organized folders in Data/, file_inventory.csv

## Stage 2: Data Integrity Analysis

**Scripts:** `Scripts/Python/data_integrity_scanner.py` or `data_integrity_scanner_spark.py`

Analyzes data quality metrics including null percentages, field deviations, and type consistency.

**Usage:**
```bash
python Scripts/Python/data_integrity_scanner_spark.py file_inventory.csv
```

**Output:** file_inventory_deviation.csv with integrity metrics

## Stage 3: Schema Detection

**Script:** `Scripts/Python/add_schema_to_csv.py`

Detects headers, infers schemas, and uses LLM for ambiguous cases.

**Usage:**
```bash
export API_CHOICE=gemini
python Scripts/Python/add_schema_to_csv.py file_inventory_deviation.csv
```

**Output:** file_inventory_schema.csv with schema information

## Stage 4: Data Cleaning

**Script:** `Scripts/Python/data_cleaner_spark.py`

Applies type-specific cleaning rules based on detected schemas.

**Usage:**
```bash
python Scripts/Python/data_cleaner_spark.py file_inventory_schema.csv --output-dir cleaned_data
```

**Output:** Cleaned CSV files in specified output directory

## Requirements

- Python 3.8+
- Bash 4.0+
- Apache Spark (optional, for distributed processing)
- LLM API keys (for schema detection)

## Complete Workflow

1. Organize files: `./Scripts/Bash/file_organizer.sh -s Data/`
2. Analyze integrity: `python Scripts/Python/data_integrity_scanner_spark.py Output/file_inventory.csv`
3. Detect schemas: `python Scripts/Python/add_schema_to_csv.py Output/file_inventory_deviation.csv`
4. Clean data: `python Scripts/Python/data_cleaner_spark.py Output/file_inventory_schema.csv`

