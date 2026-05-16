import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
MODE = "csv_list"  # Options: "directory" or "csv_list"

# For MODE = "directory"
INPUT_DIR = r"C:\programs\700GB Cleaning Shi\Scripts 07-05-2026\bucket_outputs"

# For MODE = "csv_list"
INPUT_CSV_LIST = r"C:\programs\700GB Cleaning Shi\Scripts 07-05-2026\bucket_outputs\bucket_3_user_address_only.csv"  # Schema: serial_number,filename,full_path,parent_directory,extension

# OUTPUT FILE
OUTPUT_CSV = "generalize_headers/schema_report.csv"


def process_file(file_path):
    try:
        if not os.path.exists(file_path):
            return {
                "file_name": os.path.basename(file_path),
                "columns": "ERROR: File not found",
                "column_count": 0,
                "full_path": file_path
            }

        df = pd.read_csv(file_path, nrows=0)  
        # nrows=0 reads only headers -> VERY FAST

        return {
            "file_name": os.path.basename(file_path),
            "columns": ",".join(df.columns.tolist()),
            "column_count": len(df.columns),
            "full_path": file_path
        }

    except Exception as e:
        return {
            "file_name": os.path.basename(file_path),
            "columns": f"ERROR: {str(e)}",
            "column_count": 0,
            "full_path": file_path
        }


def process_from_directory():
    if not os.path.exists(INPUT_DIR):
        print(f"Directory not found: {INPUT_DIR}")
        return []
    
    return [
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.endswith(".csv")
    ]


def process_from_csv_list():
    if not os.path.exists(INPUT_CSV_LIST):
        print(f"CSV list file not found: {INPUT_CSV_LIST}")
        return []
    
    try:
        df_list = pd.read_csv(INPUT_CSV_LIST)
        # Expected columns: serial_number,filename,full_path,parent_directory,extension
        if "full_path" not in df_list.columns:
            print(f"Error: 'full_path' column missing in {INPUT_CSV_LIST}")
            return []
        
        # Deduplicate based on full_path
        unique_paths = df_list["full_path"].unique().tolist()
        print(f"Found {len(df_list)} entries, {len(unique_paths)} unique files.")
        return unique_paths
    except Exception as e:
        print(f"Error reading CSV list: {e}")
        return []


def main():
    if MODE == "directory":
        print(f"Mode: Directory Scan ({INPUT_DIR})")
        files = process_from_directory()
    elif MODE == "csv_list":
        print(f"Mode: CSV List ({INPUT_CSV_LIST})")
        files = process_from_csv_list()
    else:
        print(f"Invalid MODE: {MODE}")
        return

    if not files:
        print("No files to process.")
        return

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    results = []
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(process_file, f) for f in files]

        for future in as_completed(futures):
            results.append(future.result())

    final_df = pd.DataFrame(results)
    final_df.to_csv(OUTPUT_CSV, index=False)

    print(f"Processed {len(results)} files.")
    print(f"Saved schema report -> {OUTPUT_CSV}")


if __name__ == "__main__":
    main()