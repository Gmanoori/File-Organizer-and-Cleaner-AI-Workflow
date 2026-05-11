import os
import csv
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

def fix_header(headers):
    """
    Renames duplicate headers by appending _1, _2, etc.
    Ensures that the newly generated names do not conflict with existing ones.
    """
    seen = set()
    new_headers = []
    for h in headers:
        if h not in seen:
            new_headers.append(h)
            seen.add(h)
        else:
            # Duplicate found, find a unique name
            count = 1
            new_name = f"{h}_{count}"
            while new_name in seen or new_name in headers:
                count += 1
                new_name = f"{h}_{count}"
            new_headers.append(new_name)
            seen.add(new_name)
    return new_headers

def process_file(file_path):
    # Only process CSV files
    if not file_path.lower().endswith('.csv') or file_path.endswith('.tmp'):
        return

    temp_path = file_path + ".tmp"
    
    try:
        # Read the original file
        with open(file_path, 'r', encoding='utf-8', newline='') as f_in:
            reader = csv.reader(f_in)
            try:
                original_headers = next(reader)
            except StopIteration:
                return # Empty file

            new_headers = fix_header(original_headers)
            
            # Only rewrite if headers actually changed
            if new_headers == original_headers:
                return

            print(f"Fixing headers in: {file_path}")
            # Write to a temporary file
            with open(temp_path, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(new_headers)
                # Copy the rest of the data
                for row in reader:
                    writer.writerow(row)
        
        # Replace original with fixed version (atomic on most systems)
        os.replace(temp_path, file_path)
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)

def main():
    # Use current directory and recurse
    root_dir = "."
    csv_files = []
    
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(".csv") and not file.endswith(".tmp"):
                csv_files.append(os.path.join(root, file))
    
    print(f"Found {len(csv_files)} CSV files. Processing...")
    
    # ThreadPoolExecutor is ideal for I/O tasks like this.
    # We use a pool of threads to handle multiple files in parallel.
    with ThreadPoolExecutor(max_workers=os.cpu_count() * 2) as executor:
        executor.map(process_file, csv_files)
    
    print("Done.")

if __name__ == "__main__":
    main()
