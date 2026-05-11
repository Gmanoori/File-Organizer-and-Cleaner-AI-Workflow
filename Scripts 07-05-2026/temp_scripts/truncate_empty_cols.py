import os
import csv
from concurrent.futures import ThreadPoolExecutor

def get_non_empty_column_indices(file_path):
    """
    Scans the entire file to identify indices of columns that have at least
    one non-empty value (excluding the header).
    """
    non_empty_indices = set()
    try:
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                return None, set()
            
            for row in reader:
                for i, value in enumerate(row):
                    if value.strip(): # Check if value is not just whitespace or empty
                        non_empty_indices.add(i)
            
            return header, sorted(list(non_empty_indices))
    except Exception as e:
        print(f"Error scanning {file_path}: {e}")
        return None, set()

def truncate_file(file_path):
    if not file_path.lower().endswith('.csv') or file_path.endswith('.tmp'):
        return

    # Phase 1: Scan for data
    header, valid_indices = get_non_empty_column_indices(file_path)
    
    if header is None:
        return

    # If all columns (except header) are empty, we might still want to keep the header?
    # Based on user's example, if we have data, we keep that column.
    # If a column is empty in ALL rows, it gets dropped.
    
    if len(valid_indices) == len(header):
        # No empty columns to drop
        return

    print(f"Truncating {file_path}: {len(header)} cols -> {len(valid_indices)} cols")
    
    temp_path = file_path + ".trunc.tmp"
    try:
        # Phase 2: Rewrite file with only valid columns
        with open(file_path, 'r', encoding='utf-8', newline='') as f_in:
            reader = csv.reader(f_in)
            next(reader) # skip original header
            
            with open(temp_path, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out)
                
                # Write new header
                new_header = [header[i] for i in valid_indices]
                writer.writerow(new_header)
                
                # Write rows
                for row in reader:
                    new_row = [row[i] if i < len(row) else "" for i in valid_indices]
                    writer.writerow(new_row)
        
        os.replace(temp_path, file_path)
        print(f"SUCCESS: Truncated {file_path}")
        
    except Exception as e:
        print(f"Error truncating {file_path}: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

def main():
    # Demonstrating on the requested sample file first
    sample_file = "FILE_002526_4142146b_new.csv"
    if os.path.exists(sample_file):
        print(f"--- Processing Sample File: {sample_file} ---")
        truncate_file(sample_file)
        print("--- Sample Processing Complete ---\n")

    # Now handle all files
    root_dir = "."
    csv_files = []
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(".csv") and not file.endswith(".tmp"):
                csv_files.append(os.path.join(root, file))
    
    print(f"Found {len(csv_files)} files to check. Starting truncation...")
    with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        executor.map(truncate_file, csv_files)

if __name__ == "__main__":
    main()
