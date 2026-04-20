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
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session():
    """Create a new Spark session with optimized settings."""
    return SparkSession.builder \
        .appName("DataCleanerSpark") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def is_spark_session_healthy(spark):
    """Check if Spark session is still responsive."""
    try:
        # Simple operation to test session health
        test_df = spark.createDataFrame([(1,)], ["test"])
        count = test_df.count()
        test_df.unpersist()
        return count == 1
    except Exception:
        return False


def recover_spark_session(spark):
    """Attempt to recover or recreate Spark session."""
    try:
        if spark:
            spark.stop()
    except:
        pass

    print("[INFO] Recreating Spark session...")
    return create_spark_session()


def clean_phone_number(value):
    """Extract and format phone number (last 10 digits)."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip()
        if not value:
            return None
        match = re.search(PHONE_PATTERN, value)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        # Try to extract any 10 consecutive digits
        digits = re.sub(r"\D", "", value)
        if len(digits) >= 10:
            return f"{digits[-10:-7]}-{digits[-7:-4]}-{digits[-4:]}"
        return None
    except Exception:
        return None


def clean_email(value):
    """Validate and normalize email address."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip().lower()
        if not value:
            return None
        if re.match(EMAIL_PATTERN, value):
            return value
        return None
    except Exception:
        return None


def clean_currency(value):
    """Remove currency symbols and commas, convert to float."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip()
        if not value:
            return None
        # Handle negative values
        is_negative = "-" in value or value.startswith("(")
        # Remove currency symbols, commas, parentheses
        cleaned = re.sub(r"[\$€¥£,\s()]", "", value)
        if not cleaned:
            return None
        result = float(cleaned)
        return -result if is_negative else result
    except (ValueError, TypeError):
        return None


def clean_percentage(value):
    """Extract numeric value from percentage, validate 0-100."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip()
        if not value:
            return None
        match = re.search(r"([-+]?\d+\.?\d*)\s*%?", value)
        if match:
            pct = float(match.group(1))
            if 0 <= pct <= 100:
                return pct
        return None
    except (ValueError, TypeError):
        return None


def clean_date(value):
    """Standardize date to YYYY-MM-DD format."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip()
        if not value:
            return None

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
    except Exception:
        return None


def clean_name(value):
    """Normalize name: trim, title case, remove special chars except hyphens/apostrophes."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip()
        if not value:
            return None
        # Remove extra whitespace
        value = re.sub(r"\s+", " ", value)
        # Remove special characters except hyphens and apostrophes
        value = re.sub(r"[^a-zA-Z\s\-']", "", value)
        # Title case
        return value.title() if value else None
    except Exception:
        return None


def clean_url(value):
    """Validate and normalize URL."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip().lower()
        if not value:
            return None
        if re.match(URL_PATTERN, value):
            # Remove trailing slashes
            return value.rstrip("/")
        return None
    except Exception:
        return None


def clean_zip_code(value):
    """Validate US ZIP code format."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip()
        if not value:
            return None
        if re.match(ZIP_US_PATTERN, value):
            return value
        return None
    except Exception:
        return None


def clean_address(value):
    """Normalize address: trim, standardize abbreviations."""
    try:
        if not value or pd.isna(value):
            return None
        value = str(value).strip()
        if not value:
            return None

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
    except Exception:
        return None


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
                
                if any(x in col_lower for x in ["phone", "tel", "mobile", "cell", "contact"]):
                    return "phone"
                if any(x in col_lower for x in ["price", "cost", "amount", "salary", "revenue", "income", "total", "rate"]):
                    return "currency"
                if any(x in col_lower for x in ["date", "dob", "created", "updated", "birth"]):
                    return "date"
                if any(x in col_lower for x in ["fname", "lname", "name", "author", "person"]):
                    return "name"
                if any(x in col_lower for x in ["address", "street", "location", "addr"]):
                    return "address"
                if any(x in col_lower for x in ["email", "mail"]):
                    return "email"
                if any(x in col_lower for x in ["url", "website", "link"]):
                    return "url"
                if any(x in col_lower for x in ["zip", "postal", "zipcode"]):
                    return "zip"
                if any(x in col_lower for x in ["percent", "pct"]):
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


def clean_data_file_pandas(file_path, schema_json, output_path=None, has_header=True, generated_headers=None):
    """
    Clean a single data file based on detected schema using pandas.
    This is more stable than Spark UDFs for complex cleaning operations.
    """
    if not os.path.exists(file_path):
        print(f"[WARN] File not found: {file_path}")
        return None

    file_ext = Path(file_path).suffix.lower()

    try:
        headers = []
        if generated_headers:
            try:
                headers = json.loads(generated_headers) if isinstance(generated_headers, str) else generated_headers
            except (json.JSONDecodeError, TypeError):
                headers = []

            if not isinstance(headers, list):
                headers = []

        # Read file with pandas
        if file_ext == ".csv":
            if not has_header and headers:
                df = pd.read_csv(file_path, header=None, dtype=str)
                if len(headers) == df.shape[1]:
                    df.columns = headers
                else:
                    extra_columns = [f"column_{i}" for i in range(df.shape[1] - len(headers))]
                    df.columns = headers[: df.shape[1]] + extra_columns
            elif not has_header:
                df = pd.read_csv(file_path, header=None, dtype=str)
            else:
                df = pd.read_csv(file_path, dtype=str)
        elif file_ext in {".xlsx", ".xls"}:
            df = pd.read_excel(file_path, header=None if not has_header else 0, dtype=str)
            if not has_header and headers and len(headers) == df.shape[1]:
                df.columns = headers
        elif file_ext == ".json":
            df = pd.read_json(file_path)
        else:
            print(f"[WARN] Unsupported file type: {file_ext}")
            return None

        print(f"[INFO] Loaded {len(df)} rows from {file_path}")

        # Parse schema and apply cleaning
        try:
            schema = json.loads(schema_json) if isinstance(schema_json, str) else schema_json
        except (json.JSONDecodeError, TypeError):
            schema = []

        # Apply cleaning per column
        for column in df.columns:
            try:
                field_type = infer_field_type_from_schema(schema, column)
                if field_type:
                    print(f"  └─ Cleaning '{column}' as {field_type}")
                    if field_type == "phone":
                        df[column] = df[column].apply(clean_phone_number)
                    elif field_type == "email":
                        df[column] = df[column].apply(clean_email)
                    elif field_type == "currency":
                        df[column] = df[column].apply(clean_currency)
                    elif field_type == "percentage":
                        df[column] = df[column].apply(clean_percentage)
                    elif field_type == "date":
                        df[column] = df[column].apply(clean_date)
                    elif field_type == "name":
                        df[column] = df[column].apply(clean_name)
                    elif field_type == "address":
                        df[column] = df[column].apply(clean_address)
                    elif field_type == "url":
                        df[column] = df[column].apply(clean_url)
                    elif field_type == "zip":
                        df[column] = df[column].apply(clean_zip_code)
            except Exception as col_err:
                print(f"[WARN] Failed to clean column '{column}': {col_err}")
                continue

        # Write cleaned output
        output_path = output_path or f"{file_path}_cleaned.csv"
        df.to_csv(output_path, index=False)
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
    parser.add_argument("--output-dir", default="Cleaned/", help="Output directory for cleaned files")
    parser.add_argument("--max-files", type=int, default=None, help="Maximum number of files to process (for testing)")
    parser.add_argument("--use-spark", action="store_true", help="Use Spark for processing (default: pandas)")
    args = parser.parse_args()

    if not os.path.exists(args.master_csv):
        print(f"[ERROR] Master CSV not found: {args.master_csv}")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    # Load master CSV
    master_df = pd.read_csv(args.master_csv)
    if args.max_files:
        master_df = master_df.head(args.max_files)

    total = len(master_df)

    print(f"\nCleaning {total} file(s) based on detected schemas using {'Spark' if args.use_spark else 'pandas'}…\n")

    if args.use_spark:
        # Set up environment variables for Spark
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        spark = None
        try:
            # Create Spark session with better configuration
            spark = SparkSession.builder \
                .appName("DataCleanerSpark") \
                .master("local[*]") \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "1g") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.default.parallelism", "4") \
                .config("spark.network.timeout", "300s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", "true") \
                .getOrCreate()
        except Exception as spark_err:
            print(f"[ERROR] Failed to create Spark session: {spark_err}")
            print("[INFO] Falling back to pandas processing...")
            args.use_spark = False

    cleaned_files = []
    failed_files = []

    for idx, row in master_df.iterrows():
        file_path = row.get("file_path", "").strip()
        schema = row.get("schema", "")
        filename = row.get("filename", Path(file_path).name)

        if not file_path or pd.isna(schema):
            continue

        print(f"[{idx + 1:>4}/{total}] {filename}")

        output_path = os.path.join(args.output_dir, f"{Path(file_path).stem}_cleaned.csv")
        has_header_raw = row.get("has_header", True)
        has_header = str(has_header_raw).strip().lower() == "true" if isinstance(has_header_raw, str) else bool(has_header_raw)
        generated_headers = row.get("generated_headers", None)

        try:
            if args.use_spark and 'spark' in locals():
                result = clean_data_file(spark, file_path, schema, output_path)
            else:
                result = clean_data_file_pandas(
                    file_path,
                    schema,
                    output_path,
                    has_header=has_header,
                    generated_headers=generated_headers,
                )
            
            if result:
                cleaned_files.append(result)
                print(f"[SUCCESS] Cleaned {filename}\n")
            else:
                failed_files.append(filename)
                print(f"[FAILED] {filename}\n")
                
        except Exception as e:
            failed_files.append(filename)
            print(f"[ERROR] {filename}: {e}\n")

    if args.use_spark and 'spark' in locals():
        try:
            spark.stop()
        except:
            pass

    print(f"\n[SUMMARY] Successfully cleaned {len(cleaned_files)}/{total} files")
    if failed_files:
        print(f"[FAILED] {len(failed_files)} files: {', '.join(failed_files[:5])}{'...' if len(failed_files) > 5 else ''}")
    print(f"Output directory: {args.output_dir}\n")


if __name__ == "__main__":
    main()