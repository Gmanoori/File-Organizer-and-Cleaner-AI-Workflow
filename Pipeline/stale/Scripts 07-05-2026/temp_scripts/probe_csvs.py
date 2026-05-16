import csv
import os
import glob

def probe_csv(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            rows = []
            for i, row in enumerate(reader):
                rows.append(row)
                if i > 5:
                    break
            
            if not rows:
                return "Empty"
            
            # Analyze column counts
            counts = [len(r) for r in rows]
            
            # Look for "FREE" or "PAID" to see where it sits
            free_indices = []
            for r in rows:
                for idx, val in enumerate(r):
                    if val in ['FREE', 'PAID']:
                        free_indices.append(idx)
            
            return {
                "file": os.path.basename(file_path),
                "row_counts": counts,
                "free_indices": list(set(free_indices)),
                "sample_row_0": rows[0][:10] if rows else [],
                "sample_row_1": rows[1][:10] if len(rows) > 1 else []
            }
    except Exception as e:
        return f"Error: {str(e)}"

files = glob.glob("*.csv")
# Filter out _new files if they already exist
files = [f for f in files if not f.endswith("_new.csv")]
results = []
for f in sorted(files)[:20]: # Sample 20
    results.append(probe_csv(f))

for res in results:
    print(res)
