# 🚀 Kestra AI-Powered Data Cleaning Pipeline - Build Plan

**Status:** Ready for implementation  
**Approach:** Manual bash → Kestra orchestration (Phased, low-cost)  
**Data Size:** 10GB-100GB  
**Target:** 10-14 days to production  

---

## 📋 Table of Contents
1. [Architecture](#architecture)
2. [Tech Stack](#tech-stack)
3. [Pipeline Stages](#pipeline-stages)
4. [Implementation Guide](#implementation-guide)
5. [Time & Cost Breakdown](#time--cost-breakdown)
6. [Failure Handling](#failure-handling)

---

## 🏗️ Architecture

### **Three-Layer Data Pipeline (Bronze → Silver → Gold)**

```
┌──────────────────────────────────────────────────────────────────┐
│                       YOUR LAPTOP (Bronze)                        │
│  $ ./file_organizer.sh -s ~/raw_data                              │
│  Output: ~/organized_data/CSV/, ~/organized_data/JSON/, etc.      │
│          + file_analysis_report.md (metadata)                     │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         │ (You manually trigger after script finishes)
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                    KESTRA (OSS - Local or Cloud)                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 1: Read organized files from ~/organized_data/*       │  │
│  │         Task: io.kestra.plugin.scripts.python.Script       │  │
│  └────────────────────────────────────────────────────────────┘  │
│                         ↓                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 2: Spark Job - Header Detection                       │  │
│  │         - Read first row of each file                      │  │
│  │         - Check if row contains headers or data            │  │
│  │         Task: io.kestra.plugin.spark.SparkSubmit           │  │
│  └────────────────────────────────────────────────────────────┘  │
│                         ↓                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 3: Conditional Branch (Switch)                        │  │
│  │   IF has_headers = true → Skip to Step 5                   │  │
│  │   ELSE → Step 4 (LLM inference)                            │  │
│  │         Task: io.kestra.plugin.core.flow.Switch            │  │
│  └────────────────────────────────────────────────────────────┘  │
│                         ↓                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 4: LLM Header Inference (for files without headers)   │  │
│  │         - Send first 10 rows to Claude API                 │  │
│  │         - Request: Infer column names & schema             │  │
│  │         Task: io.kestra.plugin.core.http.Request           │  │
│  └────────────────────────────────────────────────────────────┘  │
│                         ↓                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 5: Build Metadata CSV (for ALL files)                 │  │
│  │         Columns: serial_number, filename, file_type,       │  │
│  │                  file_path, has_header, schema             │  │
│  │         Task: io.kestra.plugin.scripts.python.Script       │  │
│  └────────────────────────────────────────────────────────────┘  │
│                         ↓                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 6: Data Cleaning (ELT - Transform in MongoDB)         │  │
│  │         - Deduplication (exact match)                      │  │
│  │         - Null handling (fill or drop)                     │  │
│  │         - Type conversion (infer schema)                   │  │
│  │         - Whitespace trimming                              │  │
│  │         Task: io.kestra.plugin.spark.SparkSubmit           │  │
│  └────────────────────────────────────────────────────────────┘  │
│                         ↓                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 7: Load to MongoDB                                    │  │
│  │         - Insert cleaned data into collections             │  │
│  │         - Collection per file type (csv, json, xlsx)       │  │
│  │         Task: io.kestra.plugin.scripts.python.Script       │  │
│  └────────────────────────────────────────────────────────────┘  │
│                         ↓                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ STEP 8: Generate Report                                    │  │
│  │         - Summary: files processed, rows cleaned, errors   │  │
│  │         - Output as artifact (downloadable)                │  │
│  │         Task: io.kestra.plugin.scripts.python.Script       │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────────────┐
│                    MongoDB (Gold/Final Data)                      │
│  Collections:                                                     │
│    - csv_cleaned                                                  │
│    - json_cleaned                                                 │
│    - xlsx_cleaned                                                 │
│    - metadata (file metadata + schema)                            │
│    - execution_logs (audit trail)                                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Component | Technology | Cost | Notes |
|-------|-----------|-----------|------|-------|
| **Bronze** | File Organization | Bash script | Free | Already done manually |
| **Orchestration** | Workflow Engine | Kestra (OSS) | Free | Run locally or Docker |
| **Processing** | Big Data | Apache Spark (local) | Free | spark-submit on local/VM |
| **LLM** | Header Inference | Claude API (Haiku) | ~$0.02/file | Cheap model for simple task |
| **Storage** | Final Data | MongoDB Atlas | Free* | Free tier: 512MB, perfect for testing |
| **Metadata** | Header/Schema Info | CSV + MongoDB | Free | Append to existing CSV |
| **Monitoring** | Execution Logs | Kestra native | Free | Built-in audit trail |

**Cost Estimate (First Run):**
- Kestra: $0
- Claude API (500 files @ $0.02/call): ~$10
- MongoDB Atlas free tier: $0
- Spark: $0 (local)
- **Total: ~$10-15** (very cheap!)

---

## 📊 Pipeline Stages (In Detail)

### **Stage 1: File Ingestion (Manual Pre-Step)**

**What you do:**
```bash
cd ~/data-cleanup
chmod +x file_organizer.sh
./file_organizer.sh -s ~/raw_data_2011_2025

# Output structure:
# ~/organized_data/
#   ├── CSV/
#   │   ├── sales_2020.csv
#   │   ├── inventory_2021.csv
#   │   └── ...
#   ├── JSON/
#   │   ├── logs.json
#   │   └── ...
#   ├── XLSX/
#   │   └── reports.xlsx
#   └── file_analysis_report.md
```

**Kestra Input:**
```yaml
inputs:
  - id: data_path
    type: STRING
    description: "Path to organized data (e.g., ~/organized_data)"
    default: "/home/user/organized_data"
```

---

### **Stage 2: Header Detection (Spark Job)**

**Input:** Organized files from bronze layer  
**Logic:**
```python
# Check if first row is header or data
def has_header(file_path, file_type):
    if file_type == "csv":
        df = pd.read_csv(file_path, nrows=1)
    elif file_type == "json":
        df = pd.read_json(file_path, lines=True, nrows=1)
    elif file_type == "xlsx":
        df = pd.read_excel(file_path, nrows=1)
    
    # Heuristics:
    # - If all values are strings + no numbers: likely header
    # - If values are numeric/mixed: likely data
    # - If values match column names: header
    
    first_row = df.iloc[0]
    col_names = df.columns.tolist()
    
    # Naive check: if first_row == col_names or all strings → header
    has_header = all(isinstance(v, str) for v in first_row)
    return has_header
```

**Output:**
```json
{
  "file_path": "/organized_data/CSV/sales_2020.csv",
  "has_header": true,
  "inferred_columns": ["id", "date", "amount", "customer"]
}
```

---

### **Stage 3: Conditional Branch**

**Decision Logic:**
```yaml
type: io.kestra.plugin.core.flow.Switch
value: "{{ outputs.header_detection.has_header }}"
cases:
  "true":
    - id: skip_llm
      type: io.kestra.plugin.core.log.Log
      message: "File has headers, skipping LLM"
  "false":
    - id: call_llm
      type: io.kestra.plugin.core.http.Request
      uri: "https://api.anthropic.com/v1/messages"
      # ...
```

---

### **Stage 4: LLM Header Inference (Only if needed)**

**API Call:**
```bash
curl -X POST https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-3-5-haiku-20241022",
    "max_tokens": 500,
    "messages": [{
      "role": "user",
      "content": "Given this CSV data with NO headers:\n\n<CSV_DATA>\n1,2021-01-01,500,John\n2,2021-01-02,750,Jane\n\nInfer column names and return JSON:\n{\"columns\": [\"col1_name\", \"col2_name\", ...]}"
    }]
  }'
```

**Response Parsing:**
```python
response = json.loads(api_response.body)
content = response['content'][0]['text']
schema = json.loads(content)
columns = schema['columns']  # ["id", "date", "amount", "customer"]
```

**Cost Per File:** ~$0.02 (Haiku is cheap!)

---

### **Stage 5: Build Metadata CSV**

**Append to existing CSV:**
```csv
serial_number,filename,file_type,file_path,has_header,schema
1,sales_2020.csv,csv,/organized_data/CSV/sales_2020.csv,true,"id,date,amount,customer"
2,logs.json,json,/organized_data/JSON/logs.json,false,"timestamp,level,message,user_id"
3,reports.xlsx,xlsx,/organized_data/XLSX/reports.xlsx,true,"report_id,quarter,revenue,status"
```

**Python Task:**
```python
import pandas as pd
import json
from pathlib import Path

# Read existing CSV
existing_df = pd.read_csv("~/metadata.csv")

# Add new records
new_records = []
for file_info in outputs.header_detection:
    new_records.append({
        "serial_number": len(existing_df) + 1,
        "filename": file_info['filename'],
        "file_type": file_info['file_type'],
        "file_path": file_info['file_path'],
        "has_header": file_info['has_header'],
        "schema": ",".join(file_info['inferred_columns'])
    })

new_df = pd.DataFrame(new_records)
merged_df = pd.concat([existing_df, new_df])
merged_df.to_csv("~/metadata.csv", index=False)
```

---

### **Stage 6: Data Cleaning (ELT)**

**Why ELT, not ETL?**
- Your data is messy (13 years of accumulation)
- Schema varies per file (some have headers, some don't)
- Easier to iterate: load raw → clean in MongoDB

**PySpark Cleaning Logic:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

def clean_data(file_path, file_type, schema, has_header):
    # Read based on file type
    if file_type == "csv":
        df = spark.read.csv(file_path, header=has_header, inferSchema=False)
    elif file_type == "json":
        df = spark.read.json(file_path)
    elif file_type == "xlsx":
        df = spark.read.format("com.crealytics:spark-excel_2.12:0.13.8")\
            .option("header", has_header).load(file_path)
    
    # 1. Rename columns if schema provided
    if schema and not has_header:
        old_names = df.columns
        new_names = schema
        for old, new in zip(old_names, new_names):
            df = df.withColumnRenamed(old, new)
    
    # 2. Deduplication (exact match)
    df = df.dropDuplicates()
    
    # 3. Null handling
    # Drop rows where key columns are null
    key_cols = df.columns[:1]  # First column as key
    df = df.dropna(subset=key_cols)
    
    # Fill remaining nulls with "UNKNOWN"
    df = df.fillna("UNKNOWN")
    
    # 4. Type conversion (try to infer)
    for col_name in df.columns:
        try:
            # Try numeric conversion
            df = df.withColumn(col_name, col(col_name).cast("double"))
        except:
            # Keep as string if fails
            pass
    
    # 5. Trim whitespace (strings only)
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(col_name, trim(col(col_name)))
    
    # Add metadata columns
    df = df.withColumn("_file_source", lit(file_path))\
           .withColumn("_cleaned_at", lit(datetime.now()))
    
    return df

# Example execution
cleaned_df = clean_data(
    file_path="/organized_data/CSV/sales.csv",
    file_type="csv",
    schema=["id", "date", "amount", "customer"],
    has_header=True
)
```

---

### **Stage 7: Load to MongoDB**

**Setup:**

```python
from pymongo import MongoClient
import os

# Connection string (MongoDB Atlas free tier)
MONGO_URL = os.getenv("MONGO_CONNECTION_STRING")
# Format: mongodb+srv://user:pass@cluster0.mongodb.net/?retryWrites=true

client = MongoClient(MONGO_URL)
db = client["data_cleaning"]

# Collections per file type
collections = {
    "csv": db["csv_cleaned"],
    "json": db["json_cleaned"],
    "xlsx": db["xlsx_cleaned"],
    "metadata": db["metadata"]
}
```

**Insert Cleaned Data:**

```python
def insert_to_mongodb(cleaned_df, file_type, collection_name="csv_cleaned"):
    collection = collections[file_type]
    
    # Convert Spark DF to list of dicts
    records = cleaned_df.toPandas().to_dict("records")
    
    # Insert with upsert (don't duplicate if re-run)
    for record in records:
        # Use first column as unique ID
        filter_key = {list(record.keys())[0]: record[list(record.keys())[0]]}
        collection.update_one(filter_key, {"$set": record}, upsert=True)
    
    print(f"✓ Inserted {len(records)} records into {collection_name}")
```

---

### **Stage 8: Execution Report**

**Output:**

```markdown
# Data Cleaning Execution Report
**Date:** 2025-04-14  
**Status:** ✅ SUCCESS

## Summary
- **Files Processed:** 247
- **Files with Headers:** 182 (73.7%)
- **Files Without Headers (LLM inferred):** 65 (26.3%)
- **Total Rows Cleaned:** 15,847,392
- **Duplicates Removed:** 243,521
- **Nulls Filled:** 1,287,543

## By File Type
| Type | Count | Rows | Cleaned | Storage |
|------|-------|------|---------|---------|
| CSV | 180 | 12.5M | 12.1M | 2.3GB |
| JSON | 45 | 2.8M | 2.7M | 1.1GB |
| XLSX | 22 | 0.547M | 0.523M | 412MB |

## MongoDB Collections
- `csv_cleaned`: 12.1M docs
- `json_cleaned`: 2.7M docs
- `xlsx_cleaned`: 0.523M docs
- `metadata`: 247 docs

## Errors (if any)
- None!

## Next Steps
- Query MongoDB for insights
- Build analytics dashboard
- Schedule recurring runs
```

---

## 🔨 Implementation Guide

### **Phase 1: Setup (Days 1-2)**

#### **1.1 Install Kestra Locally**

```bash
# Option A: Docker Compose (Recommended)
curl -o docker-compose.yml https://raw.githubusercontent.com/kestra-io/kestra/develop/docker-compose.yml
docker compose up -d

# Access UI: http://localhost:8080

# Option B: Direct installation (Linux/Mac)
# Follow: https://kestra.io/docs/installation
```

#### **1.2 Install Spark (Local)**

```bash
# Mac (Homebrew)
brew install apache-spark

# Linux (APT)
sudo apt install apache-spark

# Verify
spark-submit --version
```

#### **1.3 Set Up MongoDB Atlas (Free Tier)**

```bash
# 1. Go to https://www.mongodb.com/cloud/atlas
# 2. Create free account
# 3. Create cluster (US region, shared tier)
# 4. Get connection string:
#    mongodb+srv://user:pass@cluster0.mongodb.net/?retryWrites=true

# 5. Add IP whitelist (allow all for testing: 0.0.0.0/0)
# 6. Create database: "data_cleaning"

# Test connection
python3 << 'EOF'
from pymongo import MongoClient
client = MongoClient("mongodb+srv://user:pass@cluster0.mongodb.net/?retryWrites=true")
print(client.server_info())  # Should print server info
EOF
```

#### **1.4 Get Claude API Key**

```bash
# 1. Go to https://console.anthropic.com
# 2. Create API key
# 3. Save as environment variable:
export ANTHROPIC_API_KEY="sk-ant-..."

# In Kestra, store as secret:
# Admin → Secrets → Create:
#   Key: anthropic_api_key
#   Value: sk-ant-...
```

---

### **Phase 2: Build Kestra Workflow (Days 3-7)**

#### **2.1 Create Flow YAML Structure**

```bash
# Create file:
cat > ~/kestra-flows/data-cleaning.yml << 'EOF'
id: data_cleaning_pipeline
namespace: company.datalake
description: "Automated data cleaning with LLM-powered schema detection"

inputs:
  - id: data_path
    type: STRING
    description: "Path to organized data (e.g., /home/user/organized_data)"
    default: "/tmp/organized_data"

tasks:
  # TASK 1: Scan directory for files
  - id: scan_files
    type: io.kestra.plugin.scripts.python.Script
    script: |
      import os
      import json
      from pathlib import Path
      
      data_path = """{{ inputs.data_path }}"""
      files = []
      
      for ext in ["csv", "json", "xlsx", "xls"]:
        dir_path = os.path.join(data_path, ext.upper())
        if os.path.exists(dir_path):
          for file in os.listdir(dir_path):
            if file.endswith(f".{ext}"):
              files.append({
                "filename": file,
                "file_type": ext,
                "file_path": os.path.join(dir_path, file)
              })
      
      print(json.dumps(files))
    
  # TASK 2: Header detection (Spark job)
  # [See detailed code below]
  
  # TASK 3: Conditional branch
  # [See detailed code below]
  
  # TASK 4: LLM inference (HTTP call)
  # [See detailed code below]
  
  # TASK 5: Build metadata CSV
  # [See detailed code below]
  
  # TASK 6: Data cleaning (Spark)
  # [See detailed code below]
  
  # TASK 7: Load to MongoDB
  # [See detailed code below]
  
  # TASK 8: Generate report
  # [See detailed code below]
EOF
```

#### **2.2 Detailed Task Implementations**

See "Implementation Code Examples" section below.

---

### **Phase 3: Testing & Validation (Days 8-10)**

#### **3.1 Run Workflow with Sample Data**

```bash
# Create test data
mkdir -p /tmp/organized_data/CSV
mkdir -p /tmp/organized_data/JSON

# Sample CSV (with headers)
cat > /tmp/organized_data/CSV/test1.csv << 'EOF'
id,date,amount
1,2021-01-01,100
2,2021-01-02,200
1,2021-01-01,100
EOF

# Sample CSV (without headers)
cat > /tmp/organized_data/CSV/test2.csv << 'EOF'
101,2021-01-15,500
102,2021-01-16,600
EOF

# Trigger workflow in Kestra UI:
# 1. Navigate to http://localhost:8080
# 2. Click "Create Execution"
# 3. Set data_path: /tmp/organized_data
# 4. Click "Execute"
# 5. Watch the flow run
```

#### **3.2 Validate Each Stage**

```python
# Check header detection
from pymongo import MongoClient
client = MongoClient("mongodb+srv://user:pass@cluster0.mongodb.net/?retryWrites=true")
db = client["data_cleaning"]

# View metadata
metadata = db["metadata"].find_one()
print(f"✓ File: {metadata['filename']}")
print(f"  Has header: {metadata['has_header']}")
print(f"  Schema: {metadata['schema']}")

# View cleaned data
csv_data = db["csv_cleaned"].find_one()
print(f"✓ Cleaned record: {csv_data}")
```

---

### **Phase 4: Scale & Monitor (Days 11-14)**

#### **4.1 Run on Full Dataset**

```bash
# Point to your real organized data
# In Kestra UI: Create Execution with:
data_path: /home/user/organized_data
```

#### **4.2 Schedule Recurring Runs**

```yaml
# Add to flow YAML
triggers:
  - id: daily_cleanup
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 2 * * *"  # 2 AM daily
```

#### **4.3 Monitor Execution Logs**

```bash
# In Kestra UI:
# Dashboard → Execution Logs
# Filter by flow: data_cleaning_pipeline
# Check for errors, warnings, timing
```

---

## ⏱️ Time & Cost Breakdown

### **Learning Curve**

| Topic | Time | Difficulty |
|-------|------|-----------|
| Kestra basics (YAML, tasks) | 2-3 hrs | Easy ⭐ |
| Spark fundamentals | 3-4 hrs | Medium ⭐⭐ |
| MongoDB setup + Python driver | 2-3 hrs | Easy ⭐ |
| Claude API integration | 1-2 hrs | Easy ⭐ |
| Debugging your workflow | 2-3 hrs | Medium ⭐⭐ |
| **Total** | **10-15 hrs** | **Medium** |

### **Implementation Timeline**

- **Phase 1 (Setup):** 2 days
  - Kestra installation: 1 hr
  - Spark setup: 1 hr
  - MongoDB + Claude keys: 1 hr
  
- **Phase 2 (Build):** 4-5 days
  - Write YAML tasks: 2 days
  - Test each stage: 2 days
  - Debug/refine: 1 day
  
- **Phase 3 (Test):** 2 days
  - Sample data testing: 1 day
  - Validation: 1 day
  
- **Phase 4 (Production):** 1-2 days
  - Full dataset run: 1 day
  - Scheduling: 0.5 day

**Total:** **10-14 days** (assuming 4-5 hrs/day)

### **Cost Breakdown**

| Service | Usage | Cost |
|---------|-------|------|
| Kestra (OSS) | Unlimited | $0 |
| Spark (local) | 10GB-100GB | $0 |
| Claude API | 500 files @ ~$0.02/call | $10 |
| MongoDB Atlas | Free tier (512MB) | $0 |
| **Total (First Run)** | | **~$10** |
| **Total (Monthly Maintenance)** | Recurring cleanup | **$10-20** |

---

## 🔄 Failure Handling

### **Task-Level Retries**

```yaml
- id: llm_inference
  type: io.kestra.plugin.core.http.Request
  # ... API call config ...
  retry:
    type: CONSTANT
    interval: PT5S  # 5 seconds
    maxAttempts: 3
  timeout: PT30S
```

### **Error Handling - Best Practices**

```yaml
- id: error_handler
  type: io.kestra.plugin.core.flow.Fail
  allowFailure: true
  errors:
    - message: "LLM API rate limited"
      value: "{{ outputs.llm_inference.statusCode == 429 }}"
```

### **Data Validation Checkpoints**

```python
# After each major stage
def validate_stage(df, stage_name):
    assert df.count() > 0, f"{stage_name}: No rows after processing"
    assert df.columns, f"{stage_name}: No columns found"
    print(f"✓ {stage_name} validation passed: {df.count()} rows")
```

---

## 📝 Execution Checklist

- [ ] Kestra running locally (http://localhost:8080)
- [ ] Spark installed and accessible
- [ ] MongoDB Atlas cluster created and connection tested
- [ ] Claude API key added as Kestra secret
- [ ] Sample flow created and tested
- [ ] All 8 tasks implemented and working
- [ ] Metadata CSV appending working
- [ ] MongoDB collections populated
- [ ] Execution report generating
- [ ] Full dataset run completed
- [ ] Monitoring/alerts set up
- [ ] Documentation created

---

## 🚨 Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| "Spark command not found" | Spark not installed | Install via Homebrew/APT |
| MongoDB connection error | IP not whitelisted | Add 0.0.0.0/0 to Atlas |
| Claude API 429 (rate limit) | Too many concurrent calls | Add retry logic, reduce batch size |
| File not found in Kestra | Path issue | Use absolute paths, not `~` |
| Metadata CSV not appending | Wrong schema | Check column names match |
| Out of memory (Spark) | Dataset too large | Partition files, process in chunks |

---

## 📚 Resources

- **Kestra Docs:** https://kestra.io/docs
- **Spark Setup:** https://spark.apache.org/docs/latest/index.html
- **MongoDB Python:** https://pymongo.readthedocs.io/
- **Claude API:** https://docs.anthropic.com/
- **Kestra YAML Examples:** https://kestra.io/blueprints

---

## 🎯 Next Steps After MVP

1. **Cost Optimization:**
   - Use Spark partitioning (process 1GB chunks)
   - Batch LLM calls (5-10 files per call)
   - Estimated savings: 40-50%

2. **Production Hardening:**
   - Add comprehensive error alerts (email/Slack)
   - Implement data validation framework
   - Add data quality metrics (row counts, schema validation)

3. **Scaling:**
   - Move Kestra to cloud VM
   - Use cloud storage (S3/GCS) for file staging
   - Implement dynamic task parallelization

4. **Automation:**
   - Watch directory for new files
   - Auto-trigger workflow on file arrival
   - Incremental processing (only new files)

---

**Build Plan Version:** 1.0  
**Last Updated:** 2025-04-14  
**Status:** Ready for implementation
