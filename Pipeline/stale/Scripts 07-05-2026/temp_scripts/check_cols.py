import os
import csv

cleaned_dir = 'cleaned'
files = [f for f in os.listdir(cleaned_dir) if f.endswith('.csv')][:10]

for f in files:
    path = os.path.join(cleaned_dir, f)
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
            print(f"{f}: {len(header)}")
    except Exception as e:
        print(f"{f}: Error {e}")
