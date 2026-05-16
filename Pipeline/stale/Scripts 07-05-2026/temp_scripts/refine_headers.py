import os
import csv
import re

def is_email(s):
    return bool(re.match(r'[^@]+@[^@]+\.[^@]+', s))

def is_slug(s):
    return bool(re.match(r'^[a-z0-9]+(?:-[a-z0-9]+)*$', s))

def is_pincode(s):
    return bool(re.match(r'^\d{6}$', s))

INDIAMART_MAPPING = {
    2: 'id_internal',
    3: 'subscription_type',
    4: 'subscription_amount',
    5: 'extra_flag',
    6: 'company_id',
    7: 'verification_status',
    8: 'category_code',
    9: 'listing_type',
    10: 'slug',
    11: 'short_description',
    12: 'long_description',
    14: 'contact_first_name',
    15: 'contact_last_name',
    16: 'designation',
    20: 'email_1',
    21: 'email_2',
    24: 'company_name',
    25: 'country',
    26: 'country_code',
    27: 'address_line_1',
    28: 'address_line_2',
    29: 'city',
    30: 'locality',
    32: 'state_id',
    33: 'state',
    34: 'city_id',
    35: 'pincode',
    36: 'website',
    37: 'country_dial_code',
    38: 'area_code',
    39: 'landline_1',
    42: 'mobile_1',
    43: 'mobile_2',
    53: 'verification_method',
    54: 'trust_score',
    55: 'indiamart_url'
}

def detect_and_refine(headers, rows):
    new_headers = list(headers)
    
    # Check for IndiaMart pattern
    if len(headers) > 55:
        # Check column 10 for slug, 20 for email, 24 for company
        slug_count = 0
        email_count = 0
        for row in rows[:5]:
            if len(row) > 10 and is_slug(row[10]): slug_count += 1
            if len(row) > 20 and is_email(row[20]): email_count += 1
        
        if slug_count >= 2 or email_count >= 1:
            # Apply IndiaMart mapping
            for idx, name in INDIAMART_MAPPING.items():
                if idx < len(new_headers):
                    new_headers[idx] = name
            return new_headers, rows

    # Check for embedded headers (Pattern C)
    if len(rows) > 1:
        row2 = rows[1]
        header_keywords = ['company', 'contact', 'email', 'address', 'phone', 'tel']
        match_count = sum(1 for h in row2 if any(k in h.lower() for k in header_keywords))
        if match_count >= 3:
            # Promote row 2
            new_headers = row2
            for i in range(len(new_headers)):
                if not new_headers[i].strip():
                    new_headers[i] = f'col_{i}'
            return new_headers, rows[2:]

    # Check for File 4/5 style (Pattern B)
    # Generic headers but data has specific fields
    if any(h.startswith('col_') or h.startswith('data_col_') for h in headers[:10]):
        email_idx = -1
        pincode_idx = -1
        city_idx = -1
        for i in range(len(headers)):
            col_data = [row[i] for row in rows[:10] if i < len(row)]
            if any(is_email(d) for d in col_data): email_idx = i
            if any(is_pincode(d) for d in col_data): pincode_idx = i
            if any(d.lower() == 'mumbai' for d in col_data): city_idx = i
        
        if email_idx != -1: new_headers[email_idx] = 'email'
        if pincode_idx != -1: new_headers[pincode_idx] = 'pincode'
        if city_idx != -1: new_headers[city_idx] = 'city'

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
        print(f'Processed {os.path.basename(input_path)}')
    except PermissionError:
        print(f'Permission denied for {os.path.basename(input_path)}')
    except Exception as e:
        print(f'Error processing {os.path.basename(input_path)}: {e}')

def main():
    input_dir = r'C:\programs\700GB Cleaning Shi\Sample\Sorted\csv\cleaned'
    output_dir = r'C:\programs\700GB Cleaning Shi\Sample\Sorted\csv\schema'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    for f in files:
        if os.path.exists(os.path.join(output_dir, f)):
            continue # Skip already processed files
        process_file(os.path.join(input_dir, f), os.path.join(output_dir, f))

if __name__ == '__main__':
    main()
