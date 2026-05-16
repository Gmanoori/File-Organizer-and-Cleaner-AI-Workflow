import os
import shutil
import hashlib
import csv
import argparse
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Global lock for thread-safe CSV writing and counter updates
csv_lock = threading.Lock()
counter_lock = threading.Lock()
global_counter = 1

def generate_unique_id(file_path, counter):
    """Generates a unique ID using a counter and a hash of the original path."""
    path_hash = hashlib.md5(str(file_path).encode('utf-8')).hexdigest()[:8]
    return f"FILE_{counter:06d}_{path_hash}"

def process_single_file(file_info):
    """Worker function to process a single file."""
    global global_counter
    
    file = file_info['file']
    dest_path = file_info['dest_path']
    writer = file_info['writer']
    
    try:
        # Get unique counter value
        with counter_lock:
            current_count = global_counter
            global_counter += 1
        
        # Handle extensions
        extension = file.suffix.lstrip('.').lower()
        if not extension:
            extension = "no_ext"
        
        file_type = extension
        target_folder = dest_path / file_type
        
        # Ensure target folder exists (makedirs is thread-safe in Python 3.4.1+)
        target_folder.mkdir(parents=True, exist_ok=True)

        # Generate Unique ID and new filename
        unique_id = generate_unique_id(file, current_count)
        new_filename = f"{unique_id}{file.suffix}"
        target_file_path = target_folder / new_filename

        # Perform the copy
        shutil.copy2(file, target_file_path)

        # Write to inventory (thread-safe)
        with csv_lock:
            writer.writerow({
                'unique_id': unique_id,
                'original_path': str(file),
                'destination_path': str(target_file_path),
                'file_type': file_type,
                'extension': file.suffix.lstrip('.')
            })

        return True
    except Exception as e:
        print(f"Error processing {file}: {e}")
        return False

def organize_files(source_dir, dest_dir, inventory_file, max_workers=8):
    source_path = Path(source_dir).resolve()
    dest_path = Path(dest_dir).resolve()
    inventory_path = Path(inventory_file).resolve()

    if not source_path.exists():
        print(f"Error: Source directory '{source_path}' does not exist.")
        return

    dest_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize inventory CSV
    headers = ['unique_id', 'original_path', 'destination_path', 'file_type', 'extension']
    
    print(f"Scanning source: {source_path}")
    all_files = list(source_path.rglob('*'))
    files_to_process = [f for f in all_files if f.is_file()]
    total_files = len(files_to_process)
    
    print(f"Found {total_files} files to process.")
    print(f"Using {max_workers} threads for parallel processing.")
    print(f"Inventory will be saved to: {inventory_path}")

    with open(inventory_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()

        # Prepare tasks
        tasks = []
        for file in files_to_process:
            tasks.append({
                'file': file,
                'dest_path': dest_path,
                'writer': writer
            })

        # Execute multithreaded processing
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(process_single_file, task): task['file'] for task in tasks}
            
            completed = 0
            for future in as_completed(future_to_file):
                if future.result():
                    success_count += 1
                completed += 1
                
                if completed % 100 == 0 or completed == total_files:
                    print(f"Progress: {completed} / {total_files} files processed...")

    print(f"\nProcessing complete!")
    print(f"Successfully processed: {success_count} / {total_files}")
    print(f"Organized files in: {dest_path}")
    print(f"Inventory file: {inventory_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multithreaded File Organizer and Inventory Generator")
    parser.add_argument("-s", "--source", required=True, help="Source directory to scan")
    parser.add_argument("-d", "--dest", required=True, help="Destination directory for organized files")
    parser.add_argument("-i", "--inventory", help="Name of the inventory CSV file")
    parser.add_argument("-w", "--workers", type=int, default=8, help="Number of threads (default: 8)")
    
    args = parser.parse_args()
    
    # Default inventory name if not provided
    if not args.inventory:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.inventory = f"file_inventory_{timestamp}.csv"
        
    organize_files(args.source, args.dest, args.inventory, max_workers=args.workers)
