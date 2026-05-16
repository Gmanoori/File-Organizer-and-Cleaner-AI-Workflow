import sys
import pandas as pd

# import your function
from add_schema_to_csv import generate_header_suggestions  # <-- update this import


def get_sample_rows(csv_path, n=5):
    try:
        df = pd.read_csv(csv_path, dtype=str, nrows=n)
        df = df.fillna("<empty>")
        return df.values.tolist()
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []


def main():
    if len(sys.argv) != 2:
        print("Usage: python test_header_gen.py <csv_file_path>")
        sys.exit(1)

    csv_path = sys.argv[1]

    print(f"\nReading sample rows from: {csv_path}\n")

    sample_rows = get_sample_rows(csv_path, n=5)

    if not sample_rows:
        print("No valid rows found.")
        return

    print("Sample rows:")
    for row in sample_rows:
        print(row)

    print("\n--- Calling LLM for header generation ---\n")

    headers, confidence = generate_header_suggestions(
        sample_rows=sample_rows,
        filename=csv_path
    )

    print("\n--- RESULT ---")
    print("Headers:", headers)
    print("Confidence:", confidence)
    print("----------------\n")
    


if __name__ == "__main__":
    main()