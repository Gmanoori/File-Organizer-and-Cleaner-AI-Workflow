import csv
import time
from concurrent.futures import ThreadPoolExecutor

from rocksdict import Rdict

CSV_FILE = "people-1000000.csv"

THREADS = 8

# ----------------------------
# OPEN DB
# ----------------------------

db = Rdict("rocks_db")

# ----------------------------
# LOAD CSV
# ----------------------------

rows = []

with open(CSV_FILE, newline='', encoding='utf-8') as f:

    reader = csv.reader(f)

    # Skip header
    next(reader)

    for row in reader:
        rows.append(row)

print(f"Loaded {len(rows)} rows")

# ----------------------------
# SPLIT WORK
# ----------------------------

chunk_size = len(rows) // THREADS

chunks = [
    rows[i:i + chunk_size]
    for i in range(0, len(rows), chunk_size)
]

# ----------------------------
# WORKER FUNCTION
# ----------------------------

def process_chunk(chunk):

    local_duplicates = 0

    for row in chunk:

        user_id = row[1]

        # Duplicate check
        if user_id in db:
            local_duplicates += 1

        else:
            db[user_id] = str(row)

    return local_duplicates

# ----------------------------
# START TIMER
# ----------------------------

start = time.time()

# ----------------------------
# THREAD EXECUTION
# ----------------------------

duplicates = 0

with ThreadPoolExecutor(max_workers=THREADS) as executor:

    results = executor.map(process_chunk, chunks)

    duplicates = sum(results)

# ----------------------------
# END TIMER
# ----------------------------

end = time.time()

print("\nDone.")
print(f"Processed: {len(rows)}")
print(f"Duplicates: {duplicates}")
print(f"Time Taken: {end - start:.2f} sec")
print(f"Throughput: {len(rows)/(end-start):.2f} rows/sec")