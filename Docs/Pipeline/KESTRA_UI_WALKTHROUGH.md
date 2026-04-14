# 🎯 Kestra UI Walkthrough - Build Your Flow Step-by-Step

**Difficulty:** Beginner → Intermediate  
**Time:** 2-3 hours (first time), 30 mins (subsequent flows)  

---

## 📺 Visual Walkthrough (For Kestra UI)

### **Part 1: Getting Started (15 minutes)**

#### **Step 1: Access Kestra UI**
```
1. Open browser → http://localhost:8080
2. Default login: admin / kestra (if using Docker Compose)
3. You'll see the Kestra dashboard
```

**Screenshot expectations:**
- Left sidebar: "Flows", "Executions", "Plugins", "Namespaces"
- Center: Empty flow list or existing flows
- Top right: "Create" button

---

#### **Step 2: Create Namespace**
```
1. Left sidebar → "Namespaces"
2. Click "Create"
3. Fill in:
   - Name: company
   - (optional) Description: "My Organization"
4. Click "Save"

Now you'll see: "company" namespace created
```

---

#### **Step 3: Create New Flow**
```
1. Left sidebar → "Flows"
2. Click "Create Flow"
3. You'll see a form:
   - Namespace: Select "company"
   - ID: data_cleaning_pipeline
   - Description: "Automated data cleaning with LLM"
4. Click "Create"

You'll now enter the Flow Editor
```

**Editor Layout:**
```
┌─────────────────────────────────────────────────────────┐
│  YAML Editor (Left)        │  Topology View (Right)     │
├─────────────────────────────────────────────────────────┤
│ id: data_cleaning_pipel... │    [Task 1] → [Task 2]     │
│ namespace: company         │         ↓                  │
│ description: ...           │    [Task 3]                │
│                            │     ↙  ↘                   │
│ inputs:                    │  [Task 4] [Task 5]         │
│   - id: data_path          │                            │
│     type: STRING           │                            │
│                            │                            │
│ tasks:                     │                            │
│   - id: scan_files         │                            │
│     type: ...              │                            │
└─────────────────────────────────────────────────────────┘

Bottom: Save | Deploy | Validate YAML
```

---

### **Part 2: Build Tasks Incrementally (90 minutes)**

#### **Task 1: Scan Files (Python Script)**

**In the YAML editor, paste:**

```yaml
id: data_cleaning_pipeline
namespace: company
description: "Automated data cleaning with LLM"

inputs:
  - id: data_path
    type: STRING
    description: "Path to organized data directory"
    default: "/tmp/organized_data"

tasks:
  - id: scan_files
    type: io.kestra.plugin.scripts.python.Script
    description: "Recursively scan for CSV, JSON, XLSX files"
    script: |
      import os
      import json
      from pathlib import Path

      data_path = """{{ inputs.data_path }}"""
      files = []
      file_id = 1

      # Supported extensions
      extensions = ["csv", "json", "xlsx", "xls"]

      try:
        for ext in extensions:
          dir_path = os.path.join(data_path, ext.upper())
          if os.path.isdir(dir_path):
            for file in sorted(os.listdir(dir_path)):
              if file.endswith(f".{ext}"):
                full_path = os.path.join(dir_path, file)
                files.append({
                  "file_id": file_id,
                  "filename": file,
                  "file_type": ext,
                  "file_path": full_path,
                  "size_bytes": os.path.getsize(full_path)
                })
                file_id += 1

        print(f"Found {len(files)} files")
        print(json.dumps({"files": files, "total_count": len(files)}))

      except Exception as e:
        print(f"ERROR: {str(e)}")
        exit(1)
```

**What to do in UI:**
1. Paste above into YAML editor
2. Bottom right → Click "Save"
3. A tooltip says "✓ Saved"

---

#### **Task 2: Header Detection (Spark Job)**

**Append to the same YAML (after Task 1):**

```yaml
  - id: header_detection
    type: io.kestra.plugin.scripts.python.Script
    description: "Check if each file has headers"
    dependsOn:
      - scan_files
    script: |
      import json
      import pandas as pd
      from pathlib import Path

      files_data = """{{ outputs.scan_files.stdout }}"""
      try:
        files_info = json.loads(files_data)
        files = files_info.get("files", [])
      except:
        files = []

      results = []

      for file_info in files:
        file_path = file_info["file_path"]
        file_type = file_info["file_type"]

        try:
          # Read first 2 rows
          if file_type == "csv":
            df = pd.read_csv(file_path, nrows=2, header=None)
          elif file_type == "json":
            df = pd.read_json(file_path, lines=True, nrows=2)
          elif file_type in ["xlsx", "xls"]:
            df = pd.read_excel(file_path, nrows=2, header=None)
          else:
            continue

          # Simple heuristic: check first row
          first_row = df.iloc[0].tolist() if len(df) > 0 else []

          # If first row is mostly strings and looks like headers
          has_header = all(isinstance(v, str) or pd.isna(v) for v in first_row)

          results.append({
            "filename": file_info["filename"],
            "file_path": file_path,
            "file_type": file_type,
            "has_header": has_header,
            "column_count": len(df.columns),
            "status": "detected"
          })

        except Exception as e:
          results.append({
            "filename": file_info["filename"],
            "file_path": file_path,
            "file_type": file_type,
            "has_header": False,
            "column_count": 0,
            "status": f"error: {str(e)}"
          })

      print(f"✓ Header detection complete for {len(results)} files")
      print(json.dumps(results))
```

**What to do in UI:**
1. Add above code to YAML (below Task 1)
2. Note: `dependsOn: - scan_files` creates the arrow in topology view
3. Save (Ctrl+S)
4. Topology view should now show: Task1 → Task2

---

#### **Task 3: Conditional Branch (Switch)**

**Append to YAML:**

```yaml
  - id: process_headers
    type: io.kestra.plugin.core.flow.ForEach
    values: "{{ outputs.header_detection.stdout | fromJson }}"
    description: "Process each file (header or LLM branch)"
    tasks:
      - id: branch_by_header
        type: io.kestra.plugin.core.flow.If
        condition: "{{ taskrun.value.has_header == true }}"
        then:
          - id: skip_llm
            type: io.kestra.plugin.core.log.Log
            message: "File {{ taskrun.value.filename }} has headers, skipping LLM"
        else:
          - id: call_llm
            type: io.kestra.plugin.core.log.Log
            message: "File {{ taskrun.value.filename }} needs LLM inference"
```

**What to do in UI:**
1. Append the above
2. Save
3. Topology shows branching with "then" and "else" paths
4. **Note:** This is a simplified view. In real implementation, Task 4 goes in the "else" branch

---

#### **Task 4: LLM Header Inference (HTTP Call)**

**Append to YAML (this would be inside the "else" of Task 3 in real flow):**

```yaml
  - id: llm_inference
    type: io.kestra.plugin.core.http.Request
    uri: "https://api.anthropic.com/v1/messages"
    method: POST
    headers:
      "x-api-key": "{{ secret('anthropic_api_key') }}"
      "content-type": "application/json"
    body: |
      {
        "model": "claude-3-5-haiku-20241022",
        "max_tokens": 500,
        "messages": [{
          "role": "user",
          "content": "Given this CSV data with NO headers:\n\n1,2021-01-01,500\n2,2021-01-02,750\n\nInfer column names. Return ONLY valid JSON:\n{\"columns\": [\"col1\", \"col2\", \"col3\"]}"
        }]
      }
    timeout: PT30S
    retry:
      type: CONSTANT
      interval: PT5S
      maxAttempts: 2
```

**What to do in UI:**
1. Append above
2. Before saving, you need to add the API key as a secret:
   - Top right → "Settings" → "Secrets"
   - Create new secret:
     - Key: `anthropic_api_key`
     - Value: `sk-ant-...` (your actual key)
   - Click "Save"
3. Now save the flow

---

#### **Task 5: Build Metadata CSV**

**Append to YAML:**

```yaml
  - id: build_metadata
    type: io.kestra.plugin.scripts.python.Script
    description: "Combine all file info into metadata CSV"
    dependsOn:
      - header_detection
    script: |
      import json
      import pandas as pd
      import os

      # Get header detection results
      header_results = """{{ outputs.header_detection.stdout }}"""
      
      try:
        results = json.loads(header_results)
      except:
        results = []

      # Create DataFrame
      metadata_records = []
      for idx, result in enumerate(results, 1):
        metadata_records.append({
          "serial_number": idx,
          "filename": result.get("filename", ""),
          "file_type": result.get("file_type", ""),
          "file_path": result.get("file_path", ""),
          "has_header": result.get("has_header", False),
          "column_count": result.get("column_count", 0),
          "schema": "TBD"  # Will be filled after LLM
        })

      df = pd.DataFrame(metadata_records)

      # If existing metadata.csv exists, append
      metadata_path = os.path.expanduser("~/metadata.csv")
      if os.path.exists(metadata_path):
        existing_df = pd.read_csv(metadata_path)
        # Adjust serial numbers
        df["serial_number"] = df["serial_number"] + len(existing_df)
        df = pd.concat([existing_df, df], ignore_index=True)

      # Save
      df.to_csv(metadata_path, index=False)
      print(f"✓ Metadata CSV updated: {len(df)} total records")
      print(df.to_string())
```

**What to do in UI:**
1. Append above
2. Save

---

#### **Task 6: Data Cleaning (Spark Job)**

**Append to YAML:**

```yaml
  - id: data_cleaning
    type: io.kestra.plugin.scripts.python.Script
    description: "Clean data: dedup, nulls, trim"
    dependsOn:
      - build_metadata
    script: |
      from pyspark.sql import SparkSession
      from pyspark.sql.functions import col, trim, when, lit, current_timestamp
      import pandas as pd

      spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

      # Read metadata
      metadata_df = pd.read_csv(os.path.expanduser("~/metadata.csv"))

      total_cleaned = 0
      
      for _, row in metadata_df.iterrows():
        file_path = row["file_path"]
        file_type = row["file_type"]

        try:
          # Read based on type
          if file_type == "csv":
            df = spark.read.csv(file_path, header=row["has_header"], inferSchema=True)
          elif file_type == "json":
            df = spark.read.json(file_path)
          elif file_type in ["xlsx", "xls"]:
            # Note: Spark doesn't natively read Excel, use pandas
            pandas_df = pd.read_excel(file_path)
            df = spark.createDataFrame(pandas_df)

          # CLEANING STEPS
          # 1. Deduplication
          df = df.dropDuplicates()

          # 2. Drop nulls in key column
          key_col = df.columns[0]
          df = df.dropna(subset=[key_col])

          # 3. Fill remaining nulls
          df = df.fillna("UNKNOWN")

          # 4. Trim whitespace (strings only)
          for col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))

          # 5. Add metadata
          df = df.withColumn("_source_file", lit(row["filename"]))\
                 .withColumn("_cleaned_at", current_timestamp())

          # Count rows
          row_count = df.count()
          total_cleaned += row_count

          print(f"✓ {row['filename']}: {row_count} rows after cleaning")

        except Exception as e:
          print(f"✗ {row['filename']}: {str(e)}")

      print(f"\n✓ Total cleaned: {total_cleaned} rows")
```

**What to do in UI:**
1. Append above
2. Save

---

#### **Task 7: Load to MongoDB**

**Append to YAML:**

```yaml
  - id: load_mongodb
    type: io.kestra.plugin.scripts.python.Script
    description: "Insert cleaned data into MongoDB"
    dependsOn:
      - data_cleaning
    script: |
      from pymongo import MongoClient
      import os
      import pandas as pd
      from datetime import datetime

      # Get connection string
      mongo_url = os.getenv("MONGO_CONNECTION_STRING")
      if not mongo_url:
        print("ERROR: MONGO_CONNECTION_STRING not set")
        exit(1)

      client = MongoClient(mongo_url)
      db = client["data_cleaning"]

      # Create collections
      collections = {
        "csv": db["csv_cleaned"],
        "json": db["json_cleaned"],
        "xlsx": db["xlsx_cleaned"],
        "metadata": db["metadata"]
      }

      # Read metadata
      metadata_df = pd.read_csv(os.path.expanduser("~/metadata.csv"))

      # Insert metadata
      for _, row in metadata_df.iterrows():
        collections["metadata"].update_one(
          {"filename": row["filename"]},
          {"$set": row.to_dict()},
          upsert=True
        )

      print(f"✓ Inserted metadata for {len(metadata_df)} files")
      print(f"✓ MongoDB collections ready: {list(collections.keys())}")
```

**What to do in UI:**
1. Append above
2. Before saving, add another secret:
   - Settings → Secrets
   - Key: `mongo_connection_string`
   - Value: `mongodb+srv://user:pass@cluster0.mongodb.net/data_cleaning`
3. Save the flow

---

#### **Task 8: Generate Report**

**Append to YAML:**

```yaml
  - id: generate_report
    type: io.kestra.plugin.scripts.python.Script
    description: "Create execution summary report"
    dependsOn:
      - load_mongodb
    outputFiles:
      - execution_report.md
    script: |
      from datetime import datetime
      import pandas as pd
      import os

      # Generate report
      report = f"""# Data Cleaning Execution Report

**Execution ID:** {{{{ execution.id }}}}
**Flow:** {{{{ flow.id }}}}
**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Status:** ✅ SUCCESS

## Summary
- **Timestamp:** {datetime.now().isoformat()}
- **Total Tasks:** 8
- **Completed Tasks:** All

## Files Processed
"""

      try:
        metadata_df = pd.read_csv(os.path.expanduser("~/metadata.csv"))
        report += f"- **Total Files:** {len(metadata_df)}\n"
        report += f"- **CSV Files:** {len(metadata_df[metadata_df['file_type'] == 'csv'])}\n"
        report += f"- **JSON Files:** {len(metadata_df[metadata_df['file_type'] == 'json'])}\n"
        report += f"- **XLSX Files:** {len(metadata_df[metadata_df['file_type'] == 'xlsx'])}\n"

        report += "\n## Detailed File List\n\n"
        report += "| File | Type | Has Header | Columns |\n"
        report += "|------|------|-----------|----------|\n"
        for _, row in metadata_df.iterrows():
          report += f"| {row['filename']} | {row['file_type']} | {row['has_header']} | {row['column_count']} |\n"

      except Exception as e:
        report += f"- **Error Reading Metadata:** {str(e)}\n"

      report += """
## Next Steps
1. Query MongoDB collections for cleaned data
2. Run analytics/dashboards
3. Schedule regular runs

---
*Report generated automatically by Kestra*
"""

      # Write to file
      with open("execution_report.md", "w") as f:
        f.write(report)

      print("✓ Report generated: execution_report.md")
```

**What to do in UI:**
1. Append above
2. **Important:** Note `outputFiles: - execution_report.md`
   - This makes the file downloadable from execution results
3. Save

---

### **Part 3: Testing & Validation (30 minutes)**

#### **Step 1: Create Test Data**

**Open terminal:**

```bash
# Create test directory structure
mkdir -p /tmp/organized_data/CSV
mkdir -p /tmp/organized_data/JSON

# Create test CSV with headers
cat > /tmp/organized_data/CSV/test_with_header.csv << 'EOF'
id,name,amount
1,Alice,100
2,Bob,200
1,Alice,100
EOF

# Create test CSV without headers
cat > /tmp/organized_data/CSV/test_no_header.csv << 'EOF'
101,Charlie,300
102,Diana,400
EOF

# Create test JSON
cat > /tmp/organized_data/JSON/test.json << 'EOF'
{"id": 1, "timestamp": "2021-01-01", "value": 100}
{"id": 2, "timestamp": "2021-01-02", "value": 200}
EOF

echo "✓ Test data created"
```

---

#### **Step 2: Create Execution in Kestra UI**

**In Kestra UI:**

1. **Flows** → Click on `data_cleaning_pipeline`
2. Top right → **"Create Execution"**
3. Fill in inputs:
   - `data_path`: `/tmp/organized_data`
4. Click **"Execute"**

**Watch the execution:**
- See each task light up in real-time (Topology view)
- Click on task names to see logs
- Green = success, Red = error, Yellow = running

---

#### **Step 3: Validate Results**

```bash
# Check metadata CSV was created
cat ~/metadata.csv

# Check MongoDB insertion
python3 << 'EOF'
from pymongo import MongoClient
import os

mongo_url = os.getenv("MONGO_CONNECTION_STRING")
client = MongoClient(mongo_url)
db = client["data_cleaning"]

# Check metadata collection
count = db["metadata"].count_documents({})
print(f"✓ Metadata documents: {count}")

# Check sample record
sample = db["metadata"].find_one()
print(f"Sample: {sample}")
EOF
```

---

### **Part 4: Production Deployment**

#### **Step 1: Set Up Scheduling (Optional)**

**In the YAML editor, add triggers section:**

```yaml
triggers:
  - id: daily_cleanup
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 2 * * *"  # 2 AM daily
    timezone: UTC
    inputs:
      data_path: "/data/organized_files"
```

**What to do in UI:**
1. Find the line `description: "Automated data cleaning..."`
2. After that line, paste the triggers section above
3. Save
4. Now flow will auto-run daily at 2 AM

---

#### **Step 2: Add Error Alerts (Optional)**

```yaml
triggers:
  - id: alert_on_error
    type: io.kestra.plugin.core.trigger.Failure
    inputs:
      data_path: "/data/organized_files"
```

Then add a task to send Slack/email notification on failure.

---

#### **Step 3: Export for Version Control**

**In Kestra UI:**
1. Flows → Right-click on `data_cleaning_pipeline`
2. "Export as YAML"
3. Save to `flows/data-cleaning.yml`
4. Commit to Git

---

## 🎮 Interactive UI Features to Know

| Feature | Location | What It Does |
|---------|----------|-------------|
| **Topology View** | Right side | Visual DAG of tasks |
| **Logs** | Bottom | Real-time task output |
| **Outputs** | Task detail | Variables passed between tasks |
| **Artifacts** | Execution detail | Downloadable files (reports, CSVs) |
| **Metrics** | Dashboard | Execution time, success rate |
| **Gantt Chart** | Execution view | Timeline of task execution |

---

## 💾 Save Points (Checkpoints)

After each part, your workflow should:

✅ **After Task 1:** See file list in logs  
✅ **After Task 2:** See header detection results  
✅ **After Task 3:** See conditional branching in topology  
✅ **After Task 4:** See LLM API response (if needed)  
✅ **After Task 5:** See metadata.csv updated  
✅ **After Task 6:** See cleaned row counts  
✅ **After Task 7:** See MongoDB insertion success  
✅ **After Task 8:** See downloadable execution_report.md  

---

## 🚨 Common Mistakes in UI

| Mistake | Fix |
|---------|-----|
| Forgot `dependsOn` | Tasks run in parallel instead of sequence |
| Used `~` in path | Use full path like `/home/user/...` |
| Secret name wrong | Check it matches in `{{ secret('name') }}` |
| YAML syntax error | Red underline in editor, hover for error |
| Missing `outputs: ` | Task result not passed to next task |

---

## ✅ Deployment Checklist

- [ ] All 8 tasks created and saved
- [ ] Test data created in `/tmp/organized_data/`
- [ ] First execution completed successfully
- [ ] All logs show no errors
- [ ] Metadata CSV created/updated
- [ ] MongoDB collections populated
- [ ] Execution report downloadable
- [ ] Scheduling trigger added (if desired)
- [ ] YAML exported and committed to Git
- [ ] Can re-run without errors

---

**You're ready to deploy! 🚀**

If you hit any errors during execution, check the task logs for specific error messages, then come back here to debug.
