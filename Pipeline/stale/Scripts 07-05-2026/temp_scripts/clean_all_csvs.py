import csv
import os
import glob
import re
import Sample.Sorted.csv.temp_scripts.ai_schema_refiner as ai_schema_refiner

MASTER_HEADER = [
    "index", "status_code", "plan_type", "plan_value", "account_type", "company_id", 
    "verification_level", "category_code", "list_type", "slug", "description_1", 
    "description_2", "first_name", "last_name", "designation", "contact_first_name", 
    "contact_last_name", "email_1", "email_2", "email_3", "email_4", "company_name", 
    "country", "country_code", "address_line_1", "address_line_2", "city", "locality", 
    "landmark", "pincode", "state", "region_code", "area_code", "website", 
    "country_phone_code", "landline_1", "landline_2", "landline_3", "landline_4", 
    "landline_5", "mobile_1", "mobile_2", "mobile_code", "mobile_extra_1", 
    "mobile_extra_2", "mobile_extra_3", "mobile_code_2", "mobile_extra_4", 
    "mobile_extra_5", "mobile_extra_6", "email_verified", "phone_verified", 
    "indiamart_url", "url_extra_1", "url_extra_2", "url_extra_3", "url_extra_4", 
    "formatted_phone_1", "formatted_phone_2", "phone_extra_1", "phone_extra_2", 
    "phone_extra_3", "phone_extra_4", "phone_extra_5", "alternate_phone", 
    "full_address_1", "full_address_2", "address_extra_1", "address_extra_2", 
    "address_extra_3", "score", "score_extra_1", "rating", "rating_extra_1", 
    "created_at", "updated_at", "extra_col_1"
]

HEADER_MAPPING = {
    "CONTACT PERSON": "first_name",
    "COMPANY NAME": "company_name",
    "ADDRESSS": "address_line_1",
    "ADDRESS": "address_line_1",
    "CITY": "city",
    "PINDOCE": "pincode",
    "PIN": "pincode",
    "PHONE1": "mobile_1",
    "PHONE2": "mobile_2",
    "EMAIL": "email_1",
    "CONTACT_FIRST_NAME": "contact_first_name",
    "CONTACT_LAST_NAME": "contact_last_name",
    "EMAIL_PRIMARY": "email_1",
    "MOBILE_NUMBER": "mobile_1",
    "TEL NO.": "landline_1",
    "PRODUCT/SERVICE": "description_1",
}

def is_binary(file_path):
    try:
        with open(file_path, 'rb') as f:
            chunk = f.read(1024)
            return b'\x00' in chunk
    except:
        return True

def clean_csv(file_path):
    if is_binary(file_path):
        print(f"Skipping binary file: {file_path}")
        return

    output_path = file_path.replace(".csv", "_new.csv")
    print(f"Processing: {file_path} -> {output_path}")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = list(csv.reader(f))
            if not reader:
                return

            # Find anchor
            anchor_idx = -1
            anchor_row_idx = -1
            for r_idx, row in enumerate(reader):
                for c_idx, val in enumerate(row):
                    if val in ['FREE', 'PAID']:
                        anchor_idx = c_idx
                        anchor_row_idx = r_idx
                        break
                if anchor_idx != -1:
                    break

            new_rows = []
            if anchor_idx != -1:
                # Calculate offset to align FREE/PAID with index 3 (plan_value)
                offset = 3 - anchor_idx
                for row in reader[anchor_row_idx:]:
                    if len(row) < abs(offset) and offset < 0:
                        continue
                    
                    new_row = [""] * len(MASTER_HEADER)
                    for i, val in enumerate(row):
                        target_idx = i + offset
                        if 0 <= target_idx < len(MASTER_HEADER):
                            new_row[target_idx] = val
                    new_rows.append(new_row)
            else:
                # Use Refiner for Pattern B (Generic) and Pattern C (Embedded)
                header, rows = reader[0], reader[1:]
                refined_header, refined_rows = ai_schema_refiner.detect_and_refine(header, rows)
                
                mapping = {}
                for i, col in enumerate(refined_header):
                    col_clean = str(col).strip().upper()
                    if col_clean in HEADER_MAPPING:
                        mapping[i] = MASTER_HEADER.index(HEADER_MAPPING[col_clean])
                    elif col_clean == 'EMAIL': mapping[i] = MASTER_HEADER.index('email_1')
                    elif col_clean == 'WEBSITE': mapping[i] = MASTER_HEADER.index('website')
                    elif col_clean == 'PINCODE': mapping[i] = MASTER_HEADER.index('pincode')
                    elif col_clean == 'PHONE': mapping[i] = MASTER_HEADER.index('mobile_1')
                    elif col_clean == 'CITY': mapping[i] = MASTER_HEADER.index('city')
                
                if mapping:
                    for row in refined_rows:
                        new_row = [""] * len(MASTER_HEADER)
                        for i, val in enumerate(row):
                            if i in mapping and 0 <= i < len(row):
                                new_row[mapping[i]] = val
                        new_rows.append(new_row)
                else:
                    # Final fallback: Pad to master header
                    for row in reader:
                        new_row = row + [""] * (len(MASTER_HEADER) - len(row))
                        new_rows.append(new_row[:len(MASTER_HEADER)])

            with open(output_path, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(MASTER_HEADER)
                writer.writerows(new_rows)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

files = glob.glob("*.csv")
files = [f for f in files if not f.endswith("_new.csv") and f != "probe_csvs.py"]

for f in sorted(files):
    clean_csv(f)
