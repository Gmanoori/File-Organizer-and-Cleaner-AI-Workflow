# 🔧 VSCode Claude Code Prompt - Kestra Data Cleaning Pipeline

**Use this prompt in Claude Code (VSCode Extension) to generate the complete workflow**

---

## 📋 Complete Prompt (Copy-Paste Ready)

```
I'm building a data cleaning pipeline in Kestra (open-source orchestration platform) 
using the following architecture:

**SETUP:**
- Manual input: User runs bash script locally to organize files into ~/organized_data/{CSV,JSON,XLSX}/
- Kestra orchestrates the rest (all tasks below)
- Spark (local): For header detection and data cleaning
- MongoDB Atlas (free tier): For storing cleaned data
- Claude API (Haiku model): For inferring headers when files lack them
- Target data size: 10GB-100GB

**PIPELINE STEPS (8 tasks in Kestra YAML):**

1. **Scan Files**: Python script to recursively find all files in ~/organized_data/ 
   - Output JSON: [{filename, file_type, file_path}, ...]

2. **Header Detection**: PySpark job to check if first row is header or data
   - Logic: Check if first row contains only strings (likely header) or mixed types (data)
   - Output: JSON with {has_header: bool, inferred_columns: [list]}

3. **Conditional Branch**: Switch task based on has_header
   - If true: Skip to step 5
   - If false: Go to step 4 (LLM inference)

4. **LLM Header Inference**: HTTP call to Claude API (Haiku)
   - Send first 10 data rows to Claude
   - Prompt: "Given this CSV data with NO headers, infer column names and return JSON: {columns: [...]}"
   - Parse response and extract column names

5. **Build Metadata CSV**: Python script to append to existing CSV
   - Columns: serial_number, filename, file_type, file_path, has_header, schema
   - Match by filename, append has_header + schema columns
   - Output: Updated CSV file

6. **Data Cleaning**: PySpark job to clean data (ELT approach)
   - Steps:
     a. Rename columns if schema provided (for files without headers)
     b. Deduplication (exact match dropDuplicates)
     c. Null handling (drop nulls in key columns, fill others with "UNKNOWN")
     d. Type conversion (try to infer numeric vs string)
     e. Trim whitespace
   - Add metadata columns: _file_source, _cleaned_at
   - Output: PySpark DataFrame

7. **Load to MongoDB**: Python script to insert cleaned data
   - Create collections per file type: csv_cleaned, json_cleaned, xlsx_cleaned
   - Also create metadata collection
   - Use upsert (don't duplicate if re-run)

8. **Generate Report**: Python script to create markdown execution summary
   - Show: Files processed, rows cleaned, duplicates removed, nulls filled
   - By file type breakdown
   - MongoDB collection stats
   - Output as Kestra artifact (downloadable)

**CONSTRAINTS:**
- Use Kestra's native tasks where possible (not custom plugins)
- Use local Spark (spark-submit), not Databricks/EMR
- All code must be Python 3.9+ compatible
- Minimize API calls (batch them if possible)
- Handle errors gracefully (retries for LLM, skip problematic files)

**DELIVERABLES (in order):**

1. Complete Kestra flow YAML (data-cleaning.yml)
   - All 8 tasks with proper sequencing
   - Inputs, outputs, error handling
   - Comments explaining each task

2. PySpark script for header detection (header_detection.py)
   - Can be embedded in Kestra task or separate file

3. Python script for LLM inference (llm_header_inference.py)
   - Function to call Claude API, parse response

4. PySpark script for data cleaning (data_cleaning.py)
   - Dedup, null handling, type conversion, trimming
   - Add metadata columns

5. Python script for MongoDB loading (mongodb_load.py)
   - Insert cleaned data, handle upsert

6. Python script for report generation (generate_report.py)
   - Summary markdown with statistics

**ASSUMPTIONS:**
- Kestra running on localhost:8080 (Docker)
- MongoDB Atlas connection string available as env var: MONGO_CONNECTION_STRING
- Claude API key available as env var: ANTHROPIC_API_KEY
- Files organized as: ~/organized_data/{CSV,JSON,XLSX}/filename.ext
- Existing metadata CSV at: ~/metadata.csv

**OPTIONAL ENHANCEMENTS (if time permits):**
- Add retry logic for LLM API (handle rate limiting)
- Add data quality checks (row count validation)
- Implement file-level error handling (skip bad files, continue)
- Add logging to each task

**OUTPUT FORMAT:**
For each file, provide:
1. Complete code (copy-paste ready)
2. Brief explanation of what it does
3. Any configuration needed
4. Expected output/behavior

Please generate all files now. Start with the Kestra YAML, then the supporting scripts.
```

---

## 🎯 How to Use This in VSCode

### **Step 1: Open Claude Code Extension**
```
1. Open VSCode
2. Press Ctrl+Shift+P (Cmd+Shift+P on Mac)
3. Search "Claude Code"
4. Click "Claude Code: Start New Chat"
```

### **Step 2: Paste the Prompt**
- Copy the prompt above
- Paste into Claude Code chat
- Press Enter

### **Step 3: Wait for Generation**
Claude will generate all files (~5-10 minutes)

### **Step 4: Review & Organize**
```bash
# Create project structure
mkdir -p ~/kestra-project/{flows,scripts,docs}

# Move generated files
# - Kestra YAML → flows/
# - Python scripts → scripts/
# - Documentation → docs/
```

---

## 📂 Expected File Structure After Generation

```
~/kestra-project/
├── flows/
│   └── data-cleaning.yml          # Main Kestra workflow
├── scripts/
│   ├── header_detection.py        # Spark header detection
│   ├── llm_header_inference.py    # Claude API integration
│   ├── data_cleaning.py           # PySpark cleaning logic
│   ├── mongodb_load.py            # MongoDB insertion
│   └── generate_report.py         # Report generation
├── docs/
│   ├── SETUP.md                   # Installation guide
│   ├── TROUBLESHOOTING.md         # Common issues
│   └── API_EXAMPLES.md            # API call examples
└── README.md                       # Project overview
```

---

## 🚀 Next Steps After Generation

### **1. Copy Generated Code**
From Claude Code chat:
- Copy each file one by one
- Paste into appropriate directories

### **2. Update Configuration**
```bash
# Create .env file
cat > ~/kestra-project/.env << 'EOF'
MONGO_CONNECTION_STRING="mongodb+srv://user:pass@cluster0.mongodb.net/data_cleaning"
ANTHROPIC_API_KEY="sk-ant-..."
DATA_PATH="/home/user/organized_data"
EOF
```

### **3. Deploy to Kestra**
```bash
# Copy YAML to Kestra UI or via API
curl -X POST http://localhost:8080/api/v1/flows \
  -H "Content-Type: application/yaml" \
  -d @flows/data-cleaning.yml
```

### **4. Test with Sample Data**
```bash
# Create test data
mkdir -p /tmp/test_data/CSV
cat > /tmp/test_data/CSV/test.csv << 'EOF'
id,name,value
1,Alice,100
2,Bob,200
EOF

# Trigger in Kestra UI
# Input: data_path = /tmp/test_data
```

---

## 💡 Tips for Using Claude Code

### **Tip 1: Ask for Refinements**
If you want to modify something after generation:
```
"Can you update the data_cleaning.py script to also handle outlier removal 
using IQR method? Show the updated function."
```

### **Tip 2: Request Explanations**
```
"Explain the deduplication logic in data_cleaning.py. Why are we using 
dropDuplicates() instead of window functions?"
```

### **Tip 3: Generate Missing Pieces**
```
"I need a test script to validate the MongoDB insertion. Can you generate 
a pytest-based test file that checks if records were inserted correctly?"
```

### **Tip 4: Code Review**
```
"Review the header_detection.py script for:
1. Edge cases (empty files, single row, etc.)
2. Performance (will it handle 10GB+ files?)
3. Any bugs or improvements?"
```

---

## 🔍 What to Check After Generation

### **YAML Syntax**
```bash
# Validate Kestra YAML
python3 << 'EOF'
import yaml
with open('flows/data-cleaning.yml') as f:
    flow = yaml.safe_load(f)
    print("✓ Valid YAML")
    print(f"  Flow: {flow['id']}")
    print(f"  Tasks: {len(flow['tasks'])}")
EOF
```

### **Python Syntax**
```bash
# Check all Python files
for f in scripts/*.py; do
  python3 -m py_compile "$f" && echo "✓ $f" || echo "✗ $f"
done
```

### **Dependencies**
```bash
# Check required libraries
pip3 install --dry-run pyspark pandas pymongo anthropic

# Or create requirements.txt from generation
echo "pyspark==3.4.0
pandas==2.0.0
pymongo==4.6.0
anthropic==0.7.0" > requirements.txt
```

---

## 🐛 Common Generation Issues & Fixes

### **Issue 1: Incomplete Code**
**Problem:** Claude stops generation mid-file  
**Solution:** Ask "Continue from where you left off, starting with the load_to_mongodb function"

### **Issue 2: Missing Dependencies**
**Problem:** Generated code uses undefined imports  
**Solution:** Ask "Add all necessary imports at the top of header_detection.py"

### **Issue 3: Hardcoded Values**
**Problem:** API keys, paths hardcoded  
**Solution:** Ask "Update the code to read configuration from environment variables"

### **Issue 4: Type Hints Missing**
**Problem:** No type annotations in functions  
**Solution:** Ask "Add Python type hints to all functions in data_cleaning.py"

---

## 📝 Customization Prompts (Use These Later)

Once you have the base code, you can refine with these:

```
"Add support for Parquet files (currently only CSV, JSON, XLSX)"

"Implement batch LLM calls (process 5 files per API request instead of 1)"

"Add email notifications when pipeline completes (success/failure)"

"Create a dashboard in MongoDB Compass to visualize data cleaning stats"

"Add a dry-run mode that shows what would be cleaned without modifying data"

"Implement incremental processing (only process new files since last run)"

"Add support for custom transformation rules (user-configurable)"
```

---

## ✅ Validation Checklist

After Claude Code generates everything:

- [ ] All 8 Kestra tasks defined in YAML
- [ ] Proper task sequencing (dependencies correct)
- [ ] Switch/conditional branching for LLM
- [ ] Python scripts are syntactically valid
- [ ] All imports are standard library or specified
- [ ] Error handling in each task (try/catch or allowFailure)
- [ ] Metadata CSV appending logic works
- [ ] MongoDB connection string configurable
- [ ] Claude API integration with proper error handling
- [ ] Report generation creates markdown artifact
- [ ] Comments explaining each section

---

## 🎓 Learning Resources (If Generation Needs Clarification)

- **Kestra Task Types:** https://kestra.io/plugins
- **PySpark Examples:** https://spark.apache.org/docs/latest/api/python/
- **MongoDB Upsert:** https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.update_one
- **Claude API Reference:** https://docs.anthropic.com/en/api/messages

---

## 🚨 Pre-Deployment Checklist

Before running on real data:

1. **Test with 10 sample files** (1 of each type)
2. **Verify MongoDB connection** (insert test doc)
3. **Check Claude API quota** (won't exceed budget)
4. **Run Spark job locally** (no cluster issues)
5. **Validate metadata CSV** (correct schema)
6. **Test error paths** (what happens on bad file?)
7. **Check logs** (Kestra captures everything)
8. **Dry-run on prod data** (without committing to MongoDB)

---

**Ready to generate? Paste the main prompt into Claude Code and watch the magic! 🚀**
