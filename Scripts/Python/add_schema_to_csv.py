import argparse
import csv
import json
import os
import re
import urllib.error
import urllib.request
import pandas as pd
from pyspark.sql import SparkSession


# Choose API
api_choice = os.environ.get("API_CHOICE", "gemma").lower()
if api_choice == "gemini":
    from call_gemini_chat import call_gemini_chat as call_chat
elif api_choice == "gemma":
    from call_gemma_chat import call_gemma_chat as call_chat
else:
    raise ValueError("API_CHOICE must be 'gemini' or 'gemma'")


def pandas_dtype_to_string(dtype):
    """Convert pandas dtype to a string representation."""
    dtype_str = str(dtype)
    if "int" in dtype_str:
        return "long" if "64" in dtype_str else "integer"
    elif "float" in dtype_str:
        return "double" if "64" in dtype_str else "float"
    elif "object" in dtype_str:
        return "string"
    elif "bool" in dtype_str:
        return "boolean"
    elif "datetime" in dtype_str:
        return "timestamp"
    else:
        return dtype_str


HEADER_TOKEN_PATTERNS = re.compile(
    r"^(?:[a-z]+(?:_[a-z0-9]+)+|[a-z]+(?:[A-Z][a-z0-9]+)+|[A-Z][a-z0-9]+(?:[A-Z][a-z0-9]+)*|[A-Z0-9]+(?:_[A-Z0-9]+)+)$"
)
COMMON_HEADER_ABBREVIATIONS = {
    "dob",
    "id",
    "qty",
    "amt",
    "url",
    "ssn",
    "empid",
    "fname",
    "lname",
    "email",
    "phone",
    "addr",
    "zip",
    "city",
    "state",
    "country",
    "date",
    "year",
    "month",
    "day",
}


def normalize_value(value):
    return str(value).strip()


def is_blank(value):
    return not normalize_value(value)


def infer_cell_type(value):
    value = normalize_value(value)
    if not value:
        return None

    lower = value.lower()
    if lower in {"true", "false"}:
        return "bool"
    if re.fullmatch(r"[+-]?\d+", value):
        return "int"
    if re.fullmatch(r"[+-]?\d*\.\d+(?:[eE][+-]?\d+)?", value):
        return "float"
    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", value):
        return "date"
    if re.fullmatch(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", value):
        return "date"
    if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", lower):
        return "date"
    return "string"


def header_token_score(token):
    token = normalize_value(token)
    if not token:
        return 0.0

    lower = token.lower()
    if lower in COMMON_HEADER_ABBREVIATIONS:
        return 1.0
    if HEADER_TOKEN_PATTERNS.match(token):
        return 0.9
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9_ ]{0,30}", token) and " " in token:
        return 0.6
    return 0.0


def row_metrics(values):
    values = [normalize_value(value) for value in values if normalize_value(value)]
    if not values:
        return None

    lengths = [len(value) for value in values]
    digit_ratios = [sum(1 for ch in value if ch.isdigit()) / max(len(value), 1) for value in values]
    uppercase_ratios = [sum(1 for ch in value if ch.isupper()) / max(len(value), 1) for value in values]

    return {
        "avg_length": sum(lengths) / len(lengths),
        "avg_digit_ratio": sum(digit_ratios) / len(digit_ratios),
        "avg_upper_ratio": sum(uppercase_ratios) / len(uppercase_ratios),
    }


def compare_metrics(reference, candidate):
    if not reference or not candidate:
        return 0.0
    diffs = []
    for key in ("avg_length", "avg_digit_ratio", "avg_upper_ratio"):
        ref_val = reference[key]
        cand_val = candidate[key]
        if ref_val == 0:
            diffs.append(abs(cand_val - ref_val))
        else:
            diffs.append(abs(cand_val - ref_val) / ref_val)
    return sum(diffs) / len(diffs)


def has_data_patterns(values):
    """Detect if values look like actual data (email, phone, locations, etc.) not headers."""
    email_pattern = r"[\w\.-]+@[\w\.-]+\.\w+"
    phone_pattern = r"^\d{7,15}$"
    id_pattern = r"^\d{3,}$"
    
    email_count = sum(1 for v in values if re.search(email_pattern, v))
    phone_count = sum(1 for v in values if re.match(phone_pattern, v))
    id_count = sum(1 for v in values if re.match(id_pattern, v))
    
    return (email_count > 0) or (phone_count > 0) or (id_count > 0)


def detect_header_from_sample(sample_rows):
    if not sample_rows or len(sample_rows) < 2:
        return True

    row0 = [normalize_value(cell) for cell in sample_rows[0]]
    data_rows = [[normalize_value(cell) for cell in row] for row in sample_rows[1:]]
    column_count = len(row0)
    if column_count == 0:
        return False

    if all(not value for value in row0):
        return False
    if all(re.fullmatch(r"[+-]?\d+(?:\.\d+)?", value) for value in row0 if value):
        return False
    if any(value.lower().startswith("unnamed") for value in row0 if value):
        return False

    # NEW: Strong indicator - if first row contains data patterns, it's likely NOT a header
    if has_data_patterns(row0):
        return False
    
    # NEW: Count numeric cells in first row (pure numbers like IDs, phone numbers)
    numeric_cells = sum(1 for value in row0 if value and re.fullmatch(r"[+-]?\d+(?:\.\d+)?", value))
    numeric_ratio = numeric_cells / max(1, column_count)
    
    # If >20% of first row are pure numeric, it's likely data not headers
    if numeric_ratio > 0.2:
        return False

    header_scores = [header_token_score(value) for value in row0]
    avg_header_score = sum(header_scores) / max(1, column_count)

    repeat_count = 0
    type_shift_count = 0
    structural_count = 0
    non_empty_headers = [value.lower() for value in row0 if value]
    unique_header_ratio = len(set(non_empty_headers)) / max(1, len(non_empty_headers))

    for col_index in range(column_count):
        header_value = row0[col_index]
        data_values = [row[col_index] for row in data_rows if col_index < len(row) and row[col_index]]
        if not data_values:
            continue

        if header_value and any(header_value.lower() == data_value.lower() for data_value in data_values):
            repeat_count += 1

        header_type = infer_cell_type(header_value)
        data_types = [infer_cell_type(value) for value in data_values if infer_cell_type(value)]
        if data_types:
            numeric_like = sum(1 for t in data_types if t in {"int", "float", "date", "bool"})
            if header_type == "string" and numeric_like / len(data_types) >= 0.6:
                type_shift_count += 1

        header_metrics = row_metrics([header_value])
        data_metrics = row_metrics(data_values)
        if header_metrics and data_metrics:
            similarity = compare_metrics(data_metrics, header_metrics)
            if similarity >= 0.35:
                structural_count += 1

    repeat_ratio = repeat_count / max(1, column_count)
    type_shift_ratio = type_shift_count / max(1, column_count)
    structural_ratio = structural_count / max(1, column_count)

    score = (
        avg_header_score * 1.2
        + type_shift_ratio * 2.0
        + (1.0 - repeat_ratio) * 0.8
        + structural_ratio * 0.5
        + unique_header_ratio * 0.2
    )

    if score > 1.2:
        return True
    if repeat_ratio > 0.4 and avg_header_score < 0.25:
        return False
    if type_shift_ratio > 0.3:
        return True
    if avg_header_score > 0.6 and repeat_ratio < 0.3:
        return True

    return score >= 0.9


def detect_header_with_confidence(sample_rows):
    """
    Detect header status and return (has_header, confidence_score, needs_llm_review, reason).
    Flags ambiguous cases where first row contains numbers but isn't clearly data or header.
    """
    if not sample_rows or len(sample_rows) < 2:
        return True, 0.95, False, "Insufficient rows"

    row0 = [normalize_value(cell) for cell in sample_rows[0]]
    column_count = len(row0)
    
    if column_count == 0:
        return False, 0.9, False, "Empty row"

    if all(not value for value in row0):
        return False, 0.95, False, "All blank"
    
    # Check for pure numeric row (all numbers)
    if all(re.fullmatch(r"[+-]?\d+(?:\.\d+)?", value) for value in row0 if value):
        return False, 0.95, False, "All numeric"
    
    if any(value.lower().startswith("unnamed") for value in row0 if value):
        return False, 0.9, False, "Contains 'unnamed'"

    # Check for data patterns (email, phone, locations)
    if has_data_patterns(row0):
        return False, 0.85, True, "Contains email/phone/location patterns - flagged for LLM review"
    
    # Count numeric cells
    numeric_cells = sum(1 for value in row0 if value and re.fullmatch(r"[+-]?\d+(?:\.\d+)?", value))
    numeric_ratio = numeric_cells / max(1, column_count)
    
    # Strong indicator: >20% numeric values
    if numeric_ratio > 0.2:
        return False, 0.8, True, f"Contains {numeric_cells}/{column_count} numeric values - likely data, flagged for LLM review"
    
    # Run original detection logic
    has_header = detect_header_from_sample(sample_rows)
    
    # If detection is borderline, flag for LLM
    if numeric_ratio > 0.1 and not has_header:
        return has_header, 0.7, True, "Borderline case with some numbers - flagged for LLM review"
    
    confidence = 0.9 if has_header else 0.85
    return has_header, confidence, False, "Normal detection"


def detect_csv_header(file_path):
    try:
        sample_rows = read_sample_rows(file_path, "CSV", max_rows=6)
        if sample_rows:
            has_header, _, _, _ = detect_header_with_confidence(sample_rows)
            return has_header
    except Exception:
        pass

    try:
        with open(file_path, newline="", encoding="utf-8", errors="ignore") as csvfile:
            sample = csvfile.read(8192)
            return csv.Sniffer().has_header(sample)
    except Exception:
        return True


def detect_excel_header(file_path):
    try:
        sample_rows = read_sample_rows(file_path, "XLSX", max_rows=6)
        if sample_rows:
            has_header, _, _, _ = detect_header_with_confidence(sample_rows)
            return has_header
    except Exception:
        pass

    try:
        df = pd.read_excel(file_path, header=None, nrows=5)
        first_row = df.iloc[0]
        if first_row.isnull().all():
            return False
        if all(isinstance(x, (int, float)) for x in first_row.dropna()):
            return False
        if any(str(x).strip().lower().startswith("unnamed") for x in first_row.astype(str)):
            return False
        return True
    except Exception:
        return True


def read_sample_rows(file_path, file_type, max_rows=10):
    # print("Here2")
    if file_type == "CSV":
        try:
            with open(file_path, newline="", encoding="utf-8", errors="replace") as f:
                df = pd.read_csv(f, header=None, nrows=max_rows, engine="python")
        except UnicodeDecodeError:
            with open(file_path, newline="", encoding="latin-1", errors="replace") as f:
                df = pd.read_csv(f, header=None, nrows=max_rows, engine="python")
    else:
        df = pd.read_excel(file_path, header=None, nrows=max_rows)
    return df.fillna("").astype(str).values.tolist()


def format_sample_rows(sample_rows):
    return "\n".join(
        [" | ".join(str(value) for value in row) for row in sample_rows]
    )


def parse_json_array(text):
    text = text.strip()
    if not text:
        return []

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                pass
    return []


def generate_header_suggestions(sample_rows, model=None):
    if not sample_rows:
        return []

    column_count = len(sample_rows[0])
    sample_text = format_sample_rows(sample_rows)
    system_message = {
        "role": "system",
        "content": (
            "You are a helpful assistant that suggests column headers for tabular data. "
            "Return only a JSON array of header names."
        ),
    }
    user_message = {
        "role": "user",
        "content": (
            f"I have an unlabeled table with {column_count} columns and up to {len(sample_rows)} rows. "
            "Suggest concise header names for each column based on the sample values. "
            "If a column value looks like a name, email, phone, or location, use that meaning. "
            "Return exactly one JSON array of strings, one header name per column. "
            "Here are the sample rows:\n" + sample_text
        ),
    }

    try:
        completion_text = call_chat([system_message, user_message], model=model)
        headers = parse_json_array(completion_text)
        if isinstance(headers, list) and all(isinstance(item, str) for item in headers):
            return headers[:column_count]
    except Exception as exc:
        print(f"Warning: failed to generate suggested headers: {exc}")

    return [f"col_{i}" for i in range(column_count)]


def infer_schema_for_file(spark, file_path, file_type):
    file_type = file_type.upper()

    if file_type == "CSV":
        df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
        return [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
            for field in df.schema.fields
        ]
    elif file_type == "JSON":
        df = spark.read.option("inferSchema", True).json(file_path)
        return [
            {
                "name": field.name,
                "type": field.dataType.simpleString(),
                "nullable": field.nullable,
            }
            for field in df.schema.fields
        ]
    elif file_type in {"XLSX", "XLS"}:
        df = pd.read_excel(file_path, sheet_name=0)
        return [
            {
                "name": str(col),
                "type": pandas_dtype_to_string(df[col].dtype),
                "nullable": True,
            }
            for col in df.columns
        ]
    else:
        raise ValueError(f"Unsupported file_type for schema inference: {file_type}")


def build_schema_inventory(spark, inventory_path, output_path=None):
    # print("Here1")
    inventory_df = spark.read.option("header", True).csv(inventory_path)
    inventory_rows = inventory_df.collect()
    result_rows = []

    for row in inventory_rows:
        print(row["serial_number"])
        file_path = row["file_path"]
        file_type = row["file_type"]
        serial_number = row["serial_number"]
        filename = row["filename"]

        has_header = True
        schema_array = []
        generated_headers = []
        confidence = 0.9
        needs_llm_review = False
        detection_reason = "Unknown"

        if file_type == "CSV":
            try:
                sample_rows = read_sample_rows(file_path, file_type, max_rows=6)
                has_header, confidence, needs_llm_review, detection_reason = detect_header_with_confidence(sample_rows)
            except Exception as e:
                detection_reason = f"Error during detection: {str(e)}"
        elif file_type in {"XLSX", "XLS"}:
            try:
                sample_rows = read_sample_rows(file_path, file_type, max_rows=6)
                has_header, confidence, needs_llm_review, detection_reason = detect_header_with_confidence(sample_rows)
            except Exception as e:
                detection_reason = f"Error during detection: {str(e)}"

        if not file_path or not os.path.isfile(file_path):
            schema_array = []
        elif not has_header:
            # No header detected - generate header suggestions
            sample_rows = read_sample_rows(file_path, file_type, max_rows=5)
            generated_headers = generate_header_suggestions(sample_rows)
        elif needs_llm_review:
            # Header detected but ambiguous - ask LLM to verify the first row interpretation
            sample_rows = read_sample_rows(file_path, file_type, max_rows=5)
            print(f" Flagged for LLM review (confidence={confidence:.2f}): {detection_reason}")
            generated_headers = generate_header_suggestions(sample_rows, model=None)
        else:
            # Clear header - infer schema normally
            try:
                schema_array = infer_schema_for_file(spark, file_path, file_type)
            except Exception as exc:
                schema_array = [{"error": str(exc)}]

        row_dict = row.asDict()
        row_dict.update({
            "has_header": has_header,
            "header_confidence": confidence,
            "needs_llm_review": needs_llm_review,
            "detection_reason": detection_reason,
            "schema": json.dumps(schema_array, ensure_ascii=False) if has_header and not needs_llm_review else json.dumps([], ensure_ascii=False),
            "generated_headers": json.dumps(generated_headers, ensure_ascii=False),
        })
        result_rows.append(row_dict)

    output_path = output_path or inventory_path
    schema_df = pd.DataFrame(result_rows)

    schema_df.to_csv(
        output_path,
        index=False,
        encoding="utf-8",
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        doublequote=True,
        lineterminator="\n",
    )

    print(f"Wrote schema-enhanced inventory to: {output_path}")
    
    # Summary
    flagged_count = sum(1 for row in result_rows if row["needs_llm_review"])
    if flagged_count > 0:
        print(f" {flagged_count} file(s) flagged for LLM review due to ambiguous header detection")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append schema metadata to a file inventory CSV.")
    parser.add_argument("inventory_csv", help="Path to the inventory CSV file")
    parser.add_argument("--output", help="Path to write the enriched CSV output (optional)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("AddSchemaToInventory").getOrCreate()

    build_schema_inventory(spark, args.inventory_csv, args.output)

    spark.stop()
