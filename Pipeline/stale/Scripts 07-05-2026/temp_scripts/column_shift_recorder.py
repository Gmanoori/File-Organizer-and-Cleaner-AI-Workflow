import csv
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================================================
# CONFIG
# =========================================================

# Use absolute paths relative to the script location
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DATA_DIR = PROJECT_ROOT / "rocksdb" / "data"
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "malformed_rows.csv"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ENCODINGS = [
    "utf-8",
    "utf-8-sig",
    "cp1252",
    "latin1"
]

MAX_WORKERS = min(32, (os.cpu_count() or 4) * 2)

# =========================================================
# HELPERS
# =========================================================

def try_open_file(file_path):
    """
    Attempt multiple encodings until one works.
    """
    for encoding in ENCODINGS:
        try:
            f = open(
                file_path,
                mode="r",
                encoding=encoding,
                newline="",
                errors="replace"
            )

            # Test small read
            f.readline()
            f.seek(0)

            return f, encoding

        except Exception:
            continue

    return None, None


def process_file(file_path):
    """
    Process one CSV file and return malformed rows.
    """
    malformed_rows = []

    file_handle, encoding_used = try_open_file(file_path)

    if not file_handle:
        malformed_rows.append({
            "source_file": str(file_path),
            "line_number": -1,
            "error_type": "ENCODING_FAILURE",
            "header": "",
            "raw_row": ""
        })
        return malformed_rows

    try:
        reader = csv.reader(file_handle)

        try:
            header = next(reader)
            expected_columns = len(header)

        except Exception as e:
            malformed_rows.append({
                "source_file": str(file_path),
                "line_number": 1,
                "error_type": f"HEADER_PARSE_ERROR: {str(e)}",
                "header": "",
                "raw_row": ""
            })
            return malformed_rows

        line_number = 1

        for row in reader:
            line_number += 1

            try:
                if len(row) != expected_columns:
                    malformed_rows.append({
                        "source_file": str(file_path),
                        "line_number": line_number,
                        "error_type": (
                            f"COLUMN_MISMATCH "
                            f"(expected={expected_columns}, found={len(row)})"
                        ),
                        "header": ",".join(header),
                        "raw_row": ",".join(row)
                    })

            except Exception as e:
                malformed_rows.append({
                    "source_file": str(file_path),
                    "line_number": line_number,
                    "error_type": f"ROW_PARSE_ERROR: {str(e)}",
                    "header": ",".join(header),
                    "raw_row": str(row)
                })

    except Exception as e:
        malformed_rows.append({
            "source_file": str(file_path),
            "line_number": -1,
            "error_type": f"FILE_LEVEL_PARSE_ERROR: {str(e)}",
            "header": "",
            "raw_row": ""
        })

    finally:
        file_handle.close()

    return malformed_rows


# =========================================================
# MAIN
# =========================================================

def main():

    # Discover CSV files, but exclude the output directory
    all_csvs = list(Path(DATA_DIR).rglob("*.csv"))
    csv_files = [f for f in all_csvs if OUTPUT_DIR not in f.parents]

    print(f"\nFound {len(csv_files)} CSV files")
    print(f"Using {MAX_WORKERS} threads\n")

    all_malformed_rows = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = {
            executor.submit(process_file, file_path): file_path
            for file_path in csv_files
        }

        for future in as_completed(futures):

            file_path = futures[future]

            try:
                malformed = future.result()

                if malformed:
                    all_malformed_rows.extend(malformed)

                print(
                    f"[DONE] {file_path.name} "
                    f"| malformed rows: {len(malformed)}"
                )

            except Exception as e:
                print(f"[ERROR] {file_path.name}: {e}")

    # =====================================================
    # WRITE OUTPUT
    # =====================================================

    with open(
        OUTPUT_FILE,
        mode="w",
        newline="",
        encoding="utf-8"
    ) as outfile:

        writer = csv.writer(outfile)

        writer.writerow([
            "source_file",
            "line_number",
            "error_type",
            "header",
            "raw_row"
        ])

        for item in all_malformed_rows:
            writer.writerow([
                item["source_file"],
                item["line_number"],
                item["error_type"],
                item["header"],
                item["raw_row"]
            ])

    print("\n=================================================")
    print(f"Malformed rows collected: {len(all_malformed_rows)}")
    print(f"Output written to: {OUTPUT_FILE}")
    print("=================================================\n")


if __name__ == "__main__":
    main()