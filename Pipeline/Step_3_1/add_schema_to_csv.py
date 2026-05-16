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

FILE_SIZE_THRESHOLD_MB = 0.5  # use Spark if file is larger than this

def read_sample_rows_spark(spark, file_path, file_type, max_rows=10):
    """
    Spark-based sample reader for large files.
    Reads only max_rows using Spark's limit() — avoids loading full file into memory.
    Falls back to pandas read_sample_rows if Spark fails.
    """
    try:
        file_type = file_type.upper()
        if file_type == "CSV":
            sdf = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .csv(file_path)
            )
        elif file_type == "JSON":
            sdf = (
                spark.read
                .option("inferSchema", "false")
                .json(file_path)
            )
        elif file_type in {"XLSX", "XLS"}:
            # Spark has no native Excel reader — fall back to pandas for Excel regardless of size
            return read_sample_rows(file_path, file_type, max_rows=max_rows)
        else:
            return read_sample_rows(file_path, file_type, max_rows=max_rows)

        sample_df = sdf.limit(max_rows).toPandas().fillna("").astype(str)
        # Re-insert header as row 0 so callers get the same shape as read_sample_rows
        header_row = list(sample_df.columns)
        data_rows  = sample_df.values.tolist()
        return [header_row] + data_rows

    except Exception as e:
        print(f"  Spark sample read failed for {file_path}: {e} — falling back to pandas")
        return read_sample_rows(file_path, file_type, max_rows=max_rows)


def read_sample_rows_auto(file_path, file_type, max_rows=10, spark=None):
    """
    Router: uses Spark for files above FILE_SIZE_THRESHOLD_MB, pandas otherwise.
    Drop-in replacement for read_sample_rows anywhere spark is available.
    """
    if spark is not None:
        try:
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if size_mb > FILE_SIZE_THRESHOLD_MB:
                print(f"  Large file ({size_mb:.1f} MB) — using Spark reader")
                return read_sample_rows_spark(spark, file_path, file_type, max_rows=max_rows)
        except OSError:
            pass  # can't stat file, fall through to pandas
    return read_sample_rows(file_path, file_type, max_rows=max_rows)


def format_sample_rows(sample_rows, max_cell_len=50):
    rows = []
    for row in sample_rows:
        cells = []
        for v in row:
            val = str(v).strip()
            # Normalize empties/nulls to explicit placeholder
            if val.lower() in ("", "nan", "none", "null", "n/a"):
                cells.append("<empty>")
            else:
                cells.append(val.replace("|", "").strip()[:max_cell_len])
        rows.append(" | ".join(cells))
    return "\n".join(rows)


def parse_json_response(text):
    text = text.strip()
    if not text:
        return [], 0.0

    try:
        data = json.loads(text)
        if isinstance(data, dict) and "headers" in data and "confidence" in data:
            headers = data["headers"]
            confidence = data["confidence"]
            if isinstance(headers, list) and all(isinstance(h, str) for h in headers) and isinstance(confidence, (int, float)):
                return headers, float(confidence)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                data = json.loads(text[start : end + 1])
                if isinstance(data, dict) and "headers" in data and "confidence" in data:
                    headers = data["headers"]
                    confidence = data["confidence"]
                    if isinstance(headers, list) and all(isinstance(h, str) for h in headers) and isinstance(confidence, (int, float)):
                        return headers, float(confidence)
            except json.JSONDecodeError:
                pass
    return [], 0.0


def generate_header_suggestions(sample_rows, model=None, filename=None, confidence=1.0):
    if not sample_rows:
        return [], 0.0

    column_count = len(sample_rows[0])
    sample_text = format_sample_rows(sample_rows)
    # print(sample_rows)
    system_message = {
        "role": "system",
        "content": (
        "You are a data expert. Generate column headers from sample rows. "
        "The following rows are malformed due to column shifts, embedded newlines, URL-encoded characters (%20 etc), or web scraping artifacts."
        "Empty cells are shown as <empty> — they still occupy a column position."
        "Do NOT skip or merge columns because a cell is <empty>. "
        """ Return ONLY a JSON object with 'headers' as an array and 'confidence' as a float 0.1-1.0. As such: {"headers": [...], "confidence": 0.0–1.0} """
        ),
    }
    
    # Build user message with filename context if available
    user_content = (
        f"Analyze these {len(sample_rows)} sample rows from a file named '{filename}'. "
        f"The table has EXACTLY {column_count} columns. "
        f"Look at the actual values in each column position and infer what that column represents. "
        f"Row 0 is sample data, not headers. "
        f"Return exactly {column_count} header names.\n\n"
        f"Sample data:\n{sample_text}\n\n"
        f"Think column by column — what do the values in column 1 look like? Column 2? etc."
    )
    if filename:
        user_content += f" The data comes from a file named '{filename}'. Use this context to infer likely column meanings (e.g., 'contacts.csv' suggests names/emails)."
    user_content += (
        " Suggest concise header names for each column based on the sample values. "
        "If a column value looks like a name, email, phone, or location, use that meaning. "
    )
    if confidence <= 0.85:
        user_content += " Note: Detection confidence is low, so be cautious with interpretations and prefer generic headers if ambiguous. "
    user_content += (
        "Return exactly one JSON object with 'headers' as an array of strings (one per column) and 'confidence' as a number from 0.1 to 1.0 indicating your certainty. Please attempt to generate the headers again. "
        # "Here are the sample rows:\n" + sample_text
    )
    
    user_message = {
        "role": "user",
        "content": user_content,
    }

    try:
        completion_text = call_chat([system_message, user_message], model=model)
        print("\n--- RAW LLM OUTPUT ---\n")
        print(completion_text)
        print("\n----------------------\n")
        headers, llm_confidence = parse_json_response(completion_text)
        print(f"  LLM suggested headers: {headers} with confidence {llm_confidence:.2f}")
        if headers and not is_generic_headers(headers):
            return headers[:column_count], llm_confidence
        # LLM returned generic headers — treat as failure
        print(f"  LLM returned generic headers, skipping prepend for this attempt.")
    except Exception as exc:
        print(f"Warning: failed to generate suggested headers: {exc}")

    # Fallback — generic, caller should check with is_generic_headers()
    return [f"col_{i}" for i in range(column_count)], 0.0  # confidence 0.0 signals failure


def is_generic_headers(headers: list) -> bool:
    """
    Returns True if headers look like fallback col_N labels — 
    either from the LLM or from our own fallback.
    We don't want to write these into the actual file.
    """
    if not headers:
        return True
    generic_pattern = re.compile(r"^col[_\s]?\d+$", re.IGNORECASE)
    generic_count = sum(1 for h in headers if generic_pattern.match(str(h).strip()))
    # Flag if more than half are generic
    return generic_count / len(headers) > 0.1

def generate_header_suggestions_with_retry(file_path, file_type, initial_rows, filename, confidence, model=None, low_confidence_threshold=0.75, retry_extra_rows=7, spark=None):
    """Call LLM for header suggestions; if confidence is low, retry with more rows."""
    headers, llm_confidence = generate_header_suggestions(initial_rows, model=model, filename=filename, confidence=confidence)

    if llm_confidence < low_confidence_threshold:
        total_rows = len(initial_rows) + retry_extra_rows
        extended_rows = read_sample_rows(file_path, file_type, max_rows=total_rows)
        print(f"  Low LLM confidence ({llm_confidence:.2f}), retrying with {total_rows} rows...")
        headers, llm_confidence = generate_header_suggestions(extended_rows, model=model, filename=filename, confidence=confidence)

    return headers, llm_confidence


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
        print(f"Adding Schema for Serial Number: {row['serial_number']}")
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
                sample_rows = read_sample_rows_auto(file_path, file_type, max_rows=6)
                has_header, confidence, needs_llm_review, detection_reason = detect_header_with_confidence(sample_rows)
            except Exception as e:
                detection_reason = f"Error during detection: {str(e)}"
        elif file_type in {"XLSX", "XLS"}:
            try:
                sample_rows = read_sample_rows_auto(file_path, file_type, max_rows=6)
                has_header, confidence, needs_llm_review, detection_reason = detect_header_with_confidence(sample_rows)
            except Exception as e:
                detection_reason = f"Error during detection: {str(e)}"

        if not file_path or not os.path.isfile(file_path):
            schema_array = []
        elif not has_header:
            sample_rows = read_sample_rows_auto(file_path, file_type, max_rows=5, spark=spark)
            generated_headers, llm_confidence = generate_header_suggestions_with_retry(
                file_path, file_type, sample_rows, filename=filename, confidence=confidence, spark=spark
            )
            confidence = llm_confidence

            # Only write if LLM gave real headers (confidence > 0 and not generic)
            if generated_headers and llm_confidence > 0.0 and not is_generic_headers(generated_headers):
                write_headers_to_file(file_path, file_type, generated_headers)
            else:
                print(f"  Skipping header write — headers are generic or LLM failed for {filename}")
        elif needs_llm_review:
            # Header detected but ambiguous - ask LLM to verify the first row interpretation
            sample_rows = read_sample_rows_auto(file_path, file_type, max_rows=10, spark=spark)
            print(f" Flagged for LLM review (confidence={confidence:.2f}): {detection_reason}")
            generated_headers, llm_confidence = generate_header_suggestions_with_retry(file_path, file_type, sample_rows, filename=filename, confidence=confidence, model=None, spark=spark)
            confidence = llm_confidence  # Use LLM's confidence
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


def write_headers_to_file(file_path, file_type, headers):
    """
    Prepend generated headers directly into the data file in-place.
    After this runs, the file is a normal headed CSV — data_cleaner needs no special logic.
    """
    file_type = file_type.upper()
    try:
        if file_type == "CSV":
            df = pd.read_csv(file_path, header=None, dtype=str, encoding="utf-8",
                             on_bad_lines="skip")
            if len(headers) == df.shape[1]:
                df.columns = headers
            else:
                # Pad or trim to match actual column count
                padded = (headers + [f"col_{i}" for i in range(df.shape[1])])[:df.shape[1]]
                df.columns = padded
            df.to_csv(file_path, index=False, encoding="utf-8")
            print(f"  Headers written to file: {file_path}")

        elif file_type in {"XLSX", "XLS"}:
            df = pd.read_excel(file_path, header=None, dtype=str)
            if len(headers) == df.shape[1]:
                df.columns = headers
            else:
                padded = (headers + [f"col_{i}" for i in range(df.shape[1])])[:df.shape[1]]
                df.columns = padded
            df.to_excel(file_path, index=False)
            print(f"  Headers written to file: {file_path}")

        else:
            print(f"  Skipping header write for unsupported type: {file_type}")

    except Exception as e:
        print(f"  [WARN] Could not write headers to {file_path}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Append schema metadata to a file inventory CSV.")
    parser.add_argument("inventory_csv", help="Path to the inventory CSV file")
    parser.add_argument("--output", help="Path to write the enriched CSV output (optional)")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("AddSchemaToInventory").master("local[*]") \
        .config("spark.scheduler.mode", "FAIR").getOrCreate()

    build_schema_inventory(spark, args.inventory_csv, args.output)

    spark.stop()
