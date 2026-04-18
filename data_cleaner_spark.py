"""
data_cleaner_spark.py
─────────────────────────────────────────────────────────────────────────────
Post-processing data cleaner that reads the master CSV (output from 
data_integrity_scanner_spark.py) and cleans each file based on detected schema types.

Applies conditional cleaning rules:
- Phone numbers, emails, currency, dates, names, addresses, URLs, zip codes, etc.

Usage:
  python data_cleaner_spark.py master_with_metrics.csv
  python data_cleaner_spark.py master_with_metrics.csv --output cleaned_master.csv
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T


# ─────────────────────────────────────────────────────────────────────────────
# REGEX PATTERNS
# ─────────────────────────────────────────────────────────────────────────────

PHONE_PATTERN = r"(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})"
EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(com|org|net|edu|in|co\.uk|gov|de|fr|au)$"
CURRENCY_PATTERN = r"[\$€¥£]|,(?=\d{3})"
URL_PATTERN = r"^https?://[^\s]+$"
ZIP_US_PATTERN = r"^\d{5}(-\d{4})?$"
DATE_PATTERNS = [
    (r"^(\d{4})[/-](\d{2})[/-](\d{2})$", "YYYY-MM-DD"),  # 2024-01-15
    (r"^(\d{2})[/-](\d{2})[/-](\d{4})$", "MM-DD-YYYY"),  # 01/15/2024
    (r"^(\d{2})[/-](\d{1,2})[/-](\d{4})$", "DD-MM-YYYY"),  # 15-01-2024
]


# ─────────────────────────────────────────────────────────────────────────────
# CLEANING FUNCTIONS (PySpark UDFs)
# ─────────────────────────────────────────────────────────────────────────────

def clean_phone_number(value):
    """Extract and format phone number (last 10 digits)."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    match = re.search(PHONE_PATTERN, value)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    # Try to extract any 10 consecutive digits
    digits = re.sub(r"\D", "", value)
    if len(digits) >= 10:
        return f"{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
    return None


def clean_email(value):
    """Validate and normalize email address."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip().lower()
    if re.match(EMAIL_PATTERN, value):
        return value
    return None


def clean_currency(value):
    """Remove currency symbols and commas, convert to float."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    # Handle negative values
    is_negative = "-" in value or value.startswith("(")
    # Remove currency symbols, commas, parentheses
    cleaned = re.sub(r"[\$€¥£,\s()]", "", value)
    try:
        result = float(cleaned)
        return -result if is_negative else result
    except ValueError:
        return None


def clean_percentage(value):
    """Extract numeric value from percentage, validate 0-100."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    match = re.search(r"([-+]?\d+\.?\d*)\s*%?", value)
    if match:
        try:
            pct = float(match.group(1))
            if 0 <= pct <= 100:
                return pct
        except ValueError:
            pass
    return None


def clean_date(value):
    """Standardize date to YYYY-MM-DD format."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    
    for pattern, fmt in DATE_PATTERNS:
        match = re.match(pattern, value)
        if match:
            try:
                if fmt == "YYYY-MM-DD":
                    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                elif fmt == "MM-DD-YYYY":
                    month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                else:  # DD-MM-YYYY
                    day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                
                # Validate date
                date_obj = datetime(year, month, day)
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def clean_name(value):
    """Normalize name: trim, title case, remove special chars except hyphens/apostrophes."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    # Remove extra whitespace
    value = re.sub(r"\s+", " ", value)
    # Remove special characters except hyphens and apostrophes
    value = re.sub(r"[^a-zA-Z\s\-']", "", value)
    # Title case
    return value.title() if value else None


def clean_url(value):
    """Validate and normalize URL."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip().lower()
    if re.match(URL_PATTERN, value):
        # Remove trailing slashes
        return value.rstrip("/")
    return None


def clean_zip_code(value):
    """Validate US ZIP code format."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    if re.match(ZIP_US_PATTERN, value):
        return value
    return None


def clean_address(value):
    """Normalize address: trim, standardize abbreviations."""
    if not value or pd.isna(value):
        return None
    value = str(value).strip()
    
    # Normalize common abbreviations
    abbreviations = {
        r"\bSt\.?\b": "Street",
        r"\bAve\.?\b": "Avenue",
        r"\bBlvd\.?\b": "Boulevard",
        r"\bRd\.?\b": "Road",
        r"\bDr\.?\b": "Drive",
        r"\bLn\.?\b": "Lane",
        r"\bPl\.?\b": "Place",
        r"\bPkwy\.?\b": "Parkway",
        r"\bApt\.?\b": "Apartment",
        r"\bSuite\.?\b": "Suite",
        r"\bNo\.?\b": "Number",
    }
    
    for abbrev, full in abbreviations.items():
        value = re.sub(abbrev, full, value, flags=re.IGNORECASE)
    
    # Normalize whitespace
    value = re.sub(r"\s+", " ", value)
    return value.strip()


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA TYPE DETECTION & CLEANING DISPATCH
# ─────────────────────────────────────────────────────────────────────────────

def infer_field_type_from_schema(schema_str, column_name):
    """
    Parse the schema JSON string and detect data type intent.
    Returns: 'phone', 'email', 'currency', 'date', 'name', 'address', 'url', 'zip', 'percentage', or None
    """
    try:
        schema = json.loads(schema_str) if isinstance(schema_str, str) else schema_str
        if not isinstance(schema, list):
            return None
        
        # Find matching field in schema
        for field in schema:
            if isinstance(field, dict):
                field_name = field.get("name", "").lower()
                field_type = field.get("type", "").lower()
                
                # Heuristic matching based on column name and type
                col_lower = column_name.lower()
                
                if any(x in col_lower for x in ["phone", "tel", "mobile", "cell"]):
                    return "phone"
                if any(x in col_lower for x in ["email", "mail", "address", "contact"]):
                    return "email"
                if any(x in col_lower for x in ["price", "cost", "amount", "salary", "revenue", "income", "total"]):
                    return "currency"
                if any(x in col_lower for x in ["date", "dob", "created", "updated", "birth"]):
                    return "date"
                if any(x in col_lower for x in ["fname", "lname", "name", "author", "person"]):
                    return "name"
                if any(x in col_lower for x in ["address", "street", "location", "addr"]):
                    return "address"
                if any(x in col_lower for x in ["url", "website", "link", "website"]):
                    return "url"
                if any(x in col_lower for x in ["zip", "postal", "zipcode"]):
                    return "zip"
                if any(x in col_lower for x in ["percent", "pct", "rate"]):
                    return "percentage"
    except Exception as e:
        print(f"Warning: Failed to parse schema: {e}")
    
    return None


def clean_column(df, column_name, field_type):
    """Apply appropriate cleaning function based on detected field type."""
    if field_type == "phone":
        return df.withColumn(column_name, F.udf(clean_phone_number)(F.col(column_name)))
    elif field_type == "email":
        return df.withColumn(column_name, F.udf(clean_email)(F.col(column_name)))
    elif field_type == "currency":
        return df.withColumn(column_name, F.udf(clean_currency)(F.col(column_name)).cast(T.DoubleType()))
    elif field_type == "percentage":
        return df.withColumn(column_name, F.udf(clean_percentage)(F.col(column_name)).cast(T.DoubleType()))
    elif field_type == "date":
        return df.withColumn(column_name, F.udf(clean_date)(F.col(column_name)))
    elif field_type == "name":
        return df.withColumn(column_name, F.udf(clean_name)(F.col(column_name)))
    elif field_type == "address":
        return df.withColumn(column_name, F.udf(clean_address)(F.col(column_name)))
    elif field_type == "url":
        return df.withColumn(column_name, F.udf(clean_url)(F.col(column_name)))
    elif field_type == "zip":
        return df.withColumn(column_name, F.udf(clean_zip_code)(F.col(column_name)))
    return df


def clean_data_file(spark, file_path, schema_json, output_path=None):
    """
    Clean a single data file based on detected schema.
    Applies conditional cleaning rules per column.
    """
    if not os.path.exists(file_path):
        print(f"[WARN] File not found: {file_path}")
        return None
    
    file_ext = Path(file_path).suffix.lower()
    
    try:
        # Read file
        if file_ext == ".csv":
            df = spark.read.option("header", "true").option("inferSchema", "false").csv(file_path)
        elif file_ext in {".xlsx", ".xls"}:
            df_pd = pd.read_excel(file_path)
            df = spark.createDataFrame(df_pd)
        elif file_ext == ".json":
            df = spark.read.option("inferSchema", "true").json(file_path)
        else:
            print(f"[WARN] Unsupported file type: {file_ext}")
            return None
        
        print(f"[INFO] Loaded {df.count()} rows from {file_path}")
        
        # Parse schema and apply cleaning
        try:
            schema = json.loads(schema_json) if isinstance(schema_json, str) else schema_json
        except (json.JSONDecodeError, TypeError):
            schema = []
        
        # Apply cleaning per column
        for column in df.columns:
            field_type = infer_field_type_from_schema(schema, column)
            if field_type:
                print(f"  └─ Cleaning '{column}' as {field_type}")
                df = clean_column(df, column, field_type)
        
        # Write cleaned output
        output_path = output_path or f"{file_path}_cleaned.csv"
        df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path.replace(".csv", "_temp"))
        
        # Move temp file to final location
        import shutil
        temp_dir = output_path.replace(".csv", "_temp")
        csv_file = [f for f in os.listdir(temp_dir) if f.endswith(".csv")][0]
        shutil.move(os.path.join(temp_dir, csv_file), output_path)
        shutil.rmtree(temp_dir)
        
        print(f"[SUCCESS] Cleaned output → {output_path}\n")
        return output_path
    
    except Exception as e:
        print(f"[ERROR] Failed to clean {file_path}: {e}\n")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Clean data files based on schema types detected by data_integrity_scanner_spark.py"
    )
    parser.add_argument("master_csv", help="Path to master CSV (output from scanner)")
    parser.add_argument("--output-dir", default="./cleaned_data", help="Output directory for cleaned files")
    args = parser.parse_args()
    
    if not os.path.exists(args.master_csv):
        print(f"[ERROR] Master CSV not found: {args.master_csv}")
        sys.exit(1)
    
    os.makedirs(args.output_dir, exist_ok=True)
    
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    
    spark = SparkSession.builder \
        .appName("DataCleanerSpark") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    # Load master CSV
    master_df = pd.read_csv(args.master_csv)
    total = len(master_df)
    
    print(f"\nCleaning {total} file(s) based on detected schemas…\n")
    
    cleaned_files = []
    for idx, row in master_df.iterrows():
        file_path = row.get("file_path", "").strip()
        schema = row.get("schema", "")
        filename = row.get("filename", Path(file_path).name)
        
        if not file_path or pd.isna(schema):
            continue
        
        print(f"[{idx + 1:>4}/{total}] {filename}")
        
        output_path = os.path.join(args.output_dir, f"{Path(file_path).stem}_cleaned.csv")
        result = clean_data_file(spark, file_path, schema, output_path)
        
        if result:
            cleaned_files.append(result)
    
    spark.stop()
    
    print(f"\n[SUMMARY] Successfully cleaned {len(cleaned_files)}/{total} files")
    print(f"Output directory: {args.output_dir}\n")


if __name__ == "__main__":
    main()