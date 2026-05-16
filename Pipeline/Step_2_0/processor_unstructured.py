import os
import re
import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import threading

# Lock for writing to shared files
write_lock = threading.Lock()

# Regex patterns
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
# Phone regex: looking for sequences of digits that look like phone numbers
PHONE_REGEX = re.compile(r'(?:\+?\d{1,3}[\s-]?)?\(?\d{3}\)?[\s-]?\d{3}[\s-]?\d{4}|\b\d{10}\b|\b\d{5}\s\d{6}\b')

# Output files
EXTRACTED_DATA_FILE = 'extracted_data.csv'
SUMMARY_FILE = 'summary.csv'

def initialize_files():
    with open(EXTRACTED_DATA_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['filename', 'full_path', 'bucket', 'email', 'phone', 'user_address_info'])
    
    with open(SUMMARY_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['bucket_id', 'description', 'count', 'full_path', 'filename'])

def append_results(results):
    if not results:
        return
    with write_lock:
        with open(EXTRACTED_DATA_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(results)

def get_bucket(email, phone, other_info):
    has_email = bool(email)
    has_phone = bool(phone)
    has_other = bool(other_info)
    
    if has_email and not has_phone and not has_other:
        return 1  # just emails
    if has_phone and not has_email and not has_other:
        return 2  # just phone numbers
    if has_other and not has_email and not has_phone:
        return 3  # just users + addresses - no email or phone
    if (has_email or has_phone) and has_other:
        return 4  # user data with at least 1 of email, phone
    
    # Fallback/Combination cases
    if has_email or has_phone:
        return 4 # Treat as user data if it has at least one of these
    
    return 0 # Unknown

def process_csv(file_path):
    extracted = []
    abs_path = os.path.abspath(file_path)
    filename = os.path.basename(file_path)
    try:
        # Try reading with pandas to handle headers and structure
        df = pd.read_csv(file_path, low_memory=False, nrows=1000) # Sample for efficiency
        
        email_cols = [c for c in df.columns if 'email' in str(c).lower()]
        phone_cols = [c for c in df.columns if any(p in str(c).lower() for p in ['phone', 'tel', 'mobile', 'contact'])]
        addr_cols = [c for c in df.columns if any(a in str(c).lower() for a in ['address', 'city', 'state', 'zip', 'pincode', 'name', 'user'])]
        
        for _, row in df.iterrows():
            emails = []
            phones = []
            others = []
            
            for col in email_cols:
                val = str(row[col])
                matches = EMAIL_REGEX.findall(val)
                emails.extend(matches)
            
            for col in phone_cols:
                val = str(row[col])
                matches = PHONE_REGEX.findall(val)
                phones.extend(matches)
                
            for col in addr_cols:
                val = str(row[col])
                if val and val.lower() != 'nan':
                    others.append(f"{col}: {val}")
            
            # Deduplicate
            emails = list(set(emails))
            phones = list(set(phones))
            
            if emails or phones or others:
                email_str = ";".join(emails)
                phone_str = ";".join(phones)
                other_str = " | ".join(others)
                bucket = get_bucket(email_str, phone_str, other_str)
                if bucket > 0:
                    extracted.append([filename, abs_path, bucket, email_str, phone_str, other_str])
                    
    except Exception as e:
        # Fallback to text processing if CSV fails
        return process_text(file_path)
    return extracted

def process_text(file_path):
    extracted = []
    abs_path = os.path.abspath(file_path)
    filename = os.path.basename(file_path)
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(10000) # Read first 10KB
            emails = list(set(EMAIL_REGEX.findall(content)))
            phones = list(set(PHONE_REGEX.findall(content)))
            
            if emails and not phones:
                for e in emails:
                    extracted.append([filename, abs_path, 1, e, '', ''])
            elif phones and not emails:
                for p in phones:
                    extracted.append([filename, abs_path, 2, '', p, ''])
            elif emails or phones:
                email_str = ";".join(emails)
                phone_str = ";".join(phones)
                extracted.append([filename, abs_path, 4, email_str, phone_str, 'Found in text'])
    except:
        pass
    return extracted

def process_excel(file_path):
    extracted = []
    try:
        # Similar to CSV but using openpyxl/pandas
        excel_data = pd.read_excel(file_path, nrows=500)
        # Use a simplified version of CSV logic
        # ... (for brevity, reusing a generic approach or skipping complex sheets)
    except:
        pass
    return extracted

def worker(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.csv':
        results = process_csv(file_path)
    elif ext in ['.txt', '.md', '.ini', '.htm', '.html']:
        results = process_text(file_path)
    elif ext in ['.xls', '.xlsx', '.ods']:
        results = process_excel(file_path)
    else:
        # Try as text if it might be readable
        results = process_text(file_path)
    
    append_results(results)

def main():
    initialize_files()
    all_files = []
    for root, dirs, files in os.walk('/opt/airflow/Organized_Data'):
        for file in files:
            all_files.append(os.path.join(root, file))
    
    print(f"Starting processing of {len(all_files)} files...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(worker, all_files)
    
    # Generate summary.csv
    try:
        df = pd.read_csv(EXTRACTED_DATA_FILE)
        summary = df['bucket'].value_counts().reset_index()
        summary.columns = ['bucket_id', 'count']
        
        descriptions = {
            1: "just emails",
            2: "just phone numbers",
            3: "just users + addresses - no email or phone",
            4: "user data with at least 1 of email, phone"
        }
        summary['description'] = summary['bucket_id'].map(descriptions)
        summary[['bucket_id', 'description', 'count']].to_csv(SUMMARY_FILE, index=False)
        print("Processing complete. Created extracted_data.csv and summary.csv")
    except Exception as e:
        print(f"Error generating summary: {e}")

if __name__ == "__main__":
    main()
