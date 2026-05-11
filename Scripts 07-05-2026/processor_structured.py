import os
import re
import csv
import json
import threading
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

# =========================================================
# CONFIG
# =========================================================

INPUT_DIRECTORY = "../Sample/Sorted//csv/schema/"

OUTPUT_FILE = "structured_extracted_data.csv"
SUMMARY_FILE = "structured_summary.csv"

MAX_WORKERS = min(32, (os.cpu_count() or 4) * 2)
# MAX_WORKERS = min(32, (os.cpu_count() or 4) * 2)

SUPPORTED_FORMATS = {
    ".csv",
    ".xlsx",
    ".xls",
    ".json"
}

# =========================================================
# THREAD LOCK
# =========================================================

write_lock = threading.Lock()

# =========================================================
# REGEX
# =========================================================

EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
)

PHONE_REGEX = re.compile(
    r"(?:\+?\d{1,3}[\s\-]?)?"
    r"(?:\(?\d{3,5}\)?[\s\-]?)?"
    r"\d{5}[\s\-]?\d{5}"
)

# =========================================================
# COLUMN DETECTION
# =========================================================

EMAIL_HINTS = [
    "email",
    "mail"
]

PHONE_HINTS = [
    "phone",
    "mobile",
    "contact",
    "tel",
    "telephone"
]

USER_HINTS = [
    "name",
    "user",
    "customer",
    "client"
]

ADDRESS_HINTS = [
    "address",
    "city",
    "state",
    "zip",
    "zipcode",
    "country",
    "pincode"
]


# =========================================================
# BUCKET DESCRIPTIONS
# =========================================================

BUCKET_OUTPUT_DIR = "bucket_outputs"

bucket_serial_counters = defaultdict(int)

bucket_file_locks = defaultdict(threading.Lock)

# =========================================================
# FILE INITIALIZATION
# =========================================================

def initialize_files():

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "filename",
            "full_path",
            "parent_directory",
            "extension",
            "bucket",
            "email",
            "phone",
            "user_address_info"
        ])

    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "bucket_id",
            "description",
            "count"
        ])

# =========================================================
# HELPERS
# =========================================================

def normalize_text(value):

    if pd.isna(value):
        return ""

    return str(value).strip()

def normalize_email(email):

    return normalize_text(email).lower()

def normalize_phone(phone):

    phone = re.sub(r"\D", "", normalize_text(phone))

    if len(phone) < 10:
        return ""

    return phone

def build_record(
    file_path,
    bucket,
    email="",
    phone="",
    other=""
):

    abs_path = os.path.abspath(file_path)

    return [
        os.path.basename(file_path),
        abs_path,
        os.path.dirname(abs_path),
        Path(file_path).suffix.lower(),
        bucket,
        email,
        phone,
        other
    ]

def append_results(results):

    if not results:
        return

    with write_lock:

        with open(
            OUTPUT_FILE,
            "a",
            newline="",
            encoding="utf-8"
        ) as f:

            writer = csv.writer(f)
            writer.writerows(results)

# =========================================================
# BUCKETING
# =========================================================

def initialize_bucket_files():

    os.makedirs(BUCKET_OUTPUT_DIR, exist_ok=True)

    bucket_descriptions = {
        1: "emails_only",
        2: "phones_only",
        3: "user_address_only",
        4: "mixed_contact_data"
    }

    for bucket_id, bucket_name in bucket_descriptions.items():

        output_file = os.path.join(
            BUCKET_OUTPUT_DIR,
            f"bucket_{bucket_id}_{bucket_name}.csv"
        )

        with open(
            output_file,
            "w",
            newline="",
            encoding="utf-8"
        ) as f:

            writer = csv.writer(f)

            writer.writerow([
                "serial_number",
                "filename",
                "full_path",
                "parent_directory",
                "extension"
            ])

def append_bucket_file(records):

    if not records:
        return

    bucket_descriptions = {
        1: "emails_only",
        2: "phones_only",
        3: "user_address_only",
        4: "mixed_contact_data"
    }

    grouped_records = defaultdict(list)

    for record in records:

        bucket_id = record[4]

        grouped_records[bucket_id].append(record)

    for bucket_id, bucket_records in grouped_records.items():

        bucket_name = bucket_descriptions.get(
            bucket_id,
            "unknown"
        )

        output_file = os.path.join(
            BUCKET_OUTPUT_DIR,
            f"bucket_{bucket_id}_{bucket_name}.csv"
        )

        with bucket_file_locks[bucket_id]:

            rows_to_write = []

            for record in bucket_records:

                bucket_serial_counters[bucket_id] += 1

                serial_number = bucket_serial_counters[bucket_id]

                rows_to_write.append([
                    serial_number,
                    record[0],  # filename
                    record[1],  # full_path
                    record[2],  # parent_directory
                    record[3]   # extension
                ])

            with open(
                output_file,
                "a",
                newline="",
                encoding="utf-8"
            ) as f:

                writer = csv.writer(f)

                writer.writerows(rows_to_write)


def get_bucket(email, phone, other):

    has_email = bool(email)
    has_phone = bool(phone)
    has_other = bool(other)

    if has_email and not has_phone and not has_other:
        return 1

    if has_phone and not has_email and not has_other:
        return 2

    if has_other and not has_email and not has_phone:
        return 3

    if (has_email or has_phone) and has_other:
        return 4

    if has_email or has_phone:
        return 4

    return 0

# =========================================================
# DATAFRAME PROCESSOR
# =========================================================

def process_dataframe(df, file_path):

    extracted = []

    try:

        df.columns = [str(c).strip() for c in df.columns]

        email_cols = [
            c for c in df.columns
            if any(h in c.lower() for h in EMAIL_HINTS)
        ]

        phone_cols = [
            c for c in df.columns
            if any(h in c.lower() for h in PHONE_HINTS)
        ]

        other_cols = [
            c for c in df.columns
            if any(h in c.lower() for h in USER_HINTS + ADDRESS_HINTS)
        ]

        for _, row in df.iterrows():

            emails = set()
            phones = set()
            others = []

            # -----------------------------
            # EMAILS
            # -----------------------------

            for col in email_cols:

                value = normalize_text(row[col])

                matches = EMAIL_REGEX.findall(value)

                for m in matches:
                    emails.add(normalize_email(m))

            # -----------------------------
            # PHONES
            # -----------------------------

            for col in phone_cols:

                value = normalize_text(row[col])

                matches = PHONE_REGEX.findall(value)

                for m in matches:

                    cleaned = normalize_phone(m)

                    if cleaned:
                        phones.add(cleaned)

            # -----------------------------
            # USER / ADDRESS INFO
            # -----------------------------

            for col in other_cols:

                value = normalize_text(row[col])

                if value:
                    others.append(f"{col}: {value}")

            # -----------------------------
            # FINALIZE
            # -----------------------------

            if emails or phones or others:

                email_str = ";".join(sorted(emails))
                phone_str = ";".join(sorted(phones))
                other_str = " | ".join(others)

                bucket = get_bucket(
                    email_str,
                    phone_str,
                    other_str
                )

                if bucket > 0:

                    extracted.append(
                        build_record(
                            file_path,
                            bucket,
                            email_str,
                            phone_str,
                            other_str
                        )
                    )

    except Exception as e:

        print(f"[ERROR] DataFrame processing failed: {file_path}")
        print(e)

    return extracted

# =========================================================
# CSV PROCESSOR
# =========================================================
# count = 0
def process_csv(file_path):

    try:

        df = pd.read_csv(
            file_path,
            low_memory=False,
            dtype=str,
            nrows=100000
        )

        return process_dataframe(df, file_path)

    except Exception as e:

        print(f"[ERROR] CSV processing failed: {file_path}")
        print(e)
        # count += 1

        return []

# =========================================================
# EXCEL PROCESSOR
# =========================================================

def process_excel(file_path):

    extracted = []

    try:

        excel_file = pd.ExcelFile(file_path)

        for sheet in excel_file.sheet_names:

            try:

                df = pd.read_excel(
                    excel_file,
                    sheet_name=sheet,
                    dtype=str,
                    nrows=100000
                )

                results = process_dataframe(df, file_path)

                extracted.extend(results)

            except Exception as sheet_error:

                print(f"[WARN] Sheet failed: {sheet}")
                print(sheet_error)

    except Exception as e:

        print(f"[ERROR] Excel processing failed: {file_path}")
        print(e)

    return extracted

# =========================================================
# JSON PROCESSOR
# =========================================================

def flatten_json(
    data,
    parent_key="",
    sep="."
):

    items = []

    if isinstance(data, dict):

        for k, v in data.items():

            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            items.extend(
                flatten_json(v, new_key, sep=sep).items()
            )

    elif isinstance(data, list):

        for i, v in enumerate(data):

            new_key = f"{parent_key}[{i}]"

            items.extend(
                flatten_json(v, new_key, sep=sep).items()
            )

    else:

        items.append((parent_key, data))

    return dict(items)

def process_json(file_path):

    extracted = []

    try:

        with open(
            file_path,
            "r",
            encoding="utf-8",
            errors="ignore"
        ) as f:

            data = json.load(f)

        rows = []

        if isinstance(data, list):

            for item in data:

                if isinstance(item, dict):
                    rows.append(flatten_json(item))

        elif isinstance(data, dict):

            rows.append(flatten_json(data))

        if rows:

            df = pd.DataFrame(rows)

            extracted.extend(
                process_dataframe(df, file_path)
            )

    except Exception as e:

        print(f"[ERROR] JSON processing failed: {file_path}")
        print(e)

    return extracted

# =========================================================
# FILE DISPATCHER
# =========================================================

def process_file(file_path):

    extension = Path(file_path).suffix.lower()

    if extension == ".csv":
        results = process_csv(file_path)

    elif extension in [".xlsx", ".xls"]:
        results = process_excel(file_path)

    elif extension == ".json":
        results = process_json(file_path)

    else:
        return

    append_bucket_file(results)

# =========================================================
# FILE DISCOVERY
# =========================================================

def discover_files(directory):

    all_files = []

    for root, _, files in os.walk(directory):

        for file in files:

            full_path = os.path.join(root, file)

            extension = Path(full_path).suffix.lower()

            if extension in SUPPORTED_FORMATS:
                all_files.append(full_path)

    return all_files

# =========================================================
# SUMMARY
# =========================================================

def generate_summary():

    try:

        df = pd.read_csv(OUTPUT_FILE)

        summary = (
            df["bucket"]
            .value_counts()
            .reset_index()
        )

        summary.columns = [
            "bucket_id",
            "count"
        ]

        descriptions = {
            1: "just emails",
            2: "just phone numbers",
            3: "just users + addresses",
            4: "mixed user/contact data"
        }

        summary["description"] = (
            summary["bucket_id"]
            .map(descriptions)
        )

        summary[
            [
                "bucket_id",
                "description",
                "count"
            ]
        ].to_csv(
            SUMMARY_FILE,
            index=False
        )

    except Exception as e:

        print("[ERROR] Summary generation failed")
        print(e)

# =========================================================
# MAIN
# =========================================================

def main():

    initialize_bucket_files()
    
    all_files = discover_files(INPUT_DIRECTORY)

    print(f"Discovered {len(all_files)} supported files.")

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = [
            executor.submit(process_file, file_path)
            for file_path in all_files
        ]

        for future in as_completed(futures):

            try:
                future.result()

            except Exception as e:
                print(f"[THREAD ERROR] {e}")

    generate_summary()

    print("\nProcessing Complete.")
    print(f"Total Errors: {count}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Summary: {SUMMARY_FILE}")

# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":
    main()