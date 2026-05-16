import os
import csv
import re
import json

def is_email(s):
    return bool(re.match(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', s))

def is_url(s):
    return bool(re.match(r'^(https?://|www\.)', s, re.I))

def is_phone(s):
    return bool(re.match(r'^\+?[\d\s\-\(\)]{7,20}$', s))

def is_id(s):
    return bool(re.match(r'^[a-f0-9\-]{32,36}$', s, re.I) or (s.isdigit() and len(s) > 8))

def is_slug(s):
    return bool(re.match(r'^[a-z0-9]+(?:-[a-z0-9]+)*$', s))

def is_pincode(s):
    return bool(re.match(r'^\d{6}$', s))

GENERIC_TERMS = {'col', 'column', 'field', 'data', 'index', 'unnamed', 'col_', 'data_col_'}

def is_generic_header(headers):
    if not headers: return True
    matches = 0
    for h in headers:
        h_l = str(h).lower().strip()
        if any(term in h_l for term in GENERIC_TERMS) or not h_l or h_l.isdigit():
            matches += 1
    return matches / len(headers) > 0.5

def detect_and_refine(headers, rows):
    new_headers = list(headers)
    if not rows: return new_headers, rows

    # Check for IndiaMart pattern
    if len(headers) > 50:
        slug_count = sum(1 for row in rows[:5] if len(row) > 10 and is_slug(str(row[10])))
        email_count = sum(1 for row in rows[:5] if len(row) > 20 and is_email(str(row[20])))
        if slug_count >= 2 or email_count >= 1:
            for idx, name in INDIAMART_MAPPING.items():
                if idx < len(new_headers): new_headers[idx] = name
            return new_headers, rows

    # Check for Pattern C (Embedded headers in first 5 rows)
    header_keywords = {'company', 'contact', 'email', 'address', 'phone', 'mobile', 'website', 'city'}
    for i in range(min(5, len(rows))):
        row = rows[i]
        match_count = sum(1 for val in row if any(k in str(val).lower() for k in header_keywords))
        if match_count >= 3:
            return [str(v).strip() if str(v).strip() else f"col_{j}" for j, v in enumerate(row)], rows[i+1:]

    # Check for Pattern B (Generic/Missing headers)
    if is_generic_header(headers):
        for i in range(len(headers)):
            col_data = [str(row[i]) for row in rows[:20] if i < len(row) and str(row[i]).strip()]
            if not col_data: continue
            
            if all(is_email(d) for d in col_data[:5]): new_headers[i] = 'email'
            elif all(is_url(d) for d in col_data[:5]): new_headers[i] = 'website'
            elif all(is_phone(d) for d in col_data[:5]): new_headers[i] = 'phone'
            elif all(is_pincode(d) for d in col_data[:5]): new_headers[i] = 'pincode'
            elif all(is_id(d) for d in col_data[:5]): new_headers[i] = 'id'
            elif any(d.lower() in {'mumbai', 'delhi', 'bangalore', 'pune'} for d in col_data): new_headers[i] = 'city'

    return new_headers, rows

def process_file(input_path, output_path):
    try:
        with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                return
            rows = list(reader)

        new_headers, refined_rows = detect_and_refine(headers, rows)

        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(new_headers)
            writer.writerows(refined_rows)
        return f"Processed {os.path.basename(input_path)}"
    except Exception as e:
        return f"Error {os.path.basename(input_path)}: {e}"

def main():
    # Local config for standalone run
    INPUT_DIR = r'C:\programs\700GB Cleaning Shi\Sample\Sorted\csv\cleaned'
    OUTPUT_DIR = r'C:\programs\700GB Cleaning Shi\Sample\Sorted\csv\schema'
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    for f in files:
        res = process_file(os.path.join(INPUT_DIR, f), os.path.join(OUTPUT_DIR, f))
        print(res)

if __name__ == '__main__':
    main()
