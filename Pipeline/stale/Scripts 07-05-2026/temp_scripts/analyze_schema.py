import os
import csv

schema_dir = 'schema'
files = [f for f in os.listdir(schema_dir) if f.endswith('.csv')]

report = []

for filename in files:
    filepath = os.path.join(schema_dir, filename)
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            line1 = next(reader, None)
            line2 = next(reader, None)
            line3 = next(reader, None)
            
            if not line1:
                continue
                
            has_generic_header = line1[0].startswith('data_col_0')
            
            # Count columns
            c1 = len(line1)
            c2 = len(line2) if line2 is not None else 0
            c3 = len(line3) if line3 is not None else 0
            
            # Check if line2 looks like a header (mostly non-empty, contains letters)
            line2_is_header = False
            if line2:
                non_empty = [x for x in line2 if x.strip()]
                if len(non_empty) > 0:
                    # If line2 has words and line3 has data, line2 might be a header
                    if any(any(c.isalpha() for c in x) for x in non_empty):
                        line2_is_header = True
            
            if has_generic_header or (c1 != c2 and c2 != 0) or (c1 != c3 and c3 != 0):
                report.append({
                    'file': filename,
                    'line1_generic': has_generic_header,
                    'line2_is_header': line2_is_header,
                    'c1': c1,
                    'c2': c2,
                    'c3': c3
                })
    except Exception as e:
        report.append({'file': filename, 'error': str(e)})

# Print summarized report
print(f"Total files analyzed: {len(files)}")
print(f"Files with issues: {len(report)}")
for item in report[:20]: # Show first 20
    print(item)
if len(report) > 20:
    print(f"... and {len(report)-20} more")
