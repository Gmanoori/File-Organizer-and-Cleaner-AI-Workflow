import pandas as pd
import os
import re
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def clean_phone(val):
    """Strips leading dashes and non-numeric characters from phone numbers."""
    if pd.isna(val) or val == '':
        return val
    s = str(val).strip()
    if s.startswith('-'):
        s = s[1:]
    return re.sub(r'\D', '', s)

def normalize_generic_headers(df, file_path):
    """
    Attempts to map 'data_col_N' to known semantic headers based on column count.
    Ref: Indiamart standard 75-column schema.
    """
    col_count = len(df.columns)
    
    # Mapping for the common 75-ish column Indiamart schema
    if 65 <= col_count <= 80:
        mapping = {
            'data_col_18': 'contact_first_name_alt',
            'data_col_19': 'contact_last_name_alt',
            'data_col_31': 'landmark',
            'data_col_68': 'full_address_reconstructed'
        }
        # Only rename if the column exists and doesn't conflict
        actual_mapping = {k: v for k, v in mapping.items() if k in df.columns and v not in df.columns}
        if actual_mapping:
            logging.info(f"Normalizing generic headers for {os.path.basename(file_path)}")
            df = df.rename(columns=actual_mapping)
            
    return df

def process_file(file_path, output_dir):
    try:
        df = pd.read_csv(file_path, low_memory=False)
        
        # 1. Clean Phone Columns
        phone_cols = [c for c in df.columns if 'phone' in c.lower() or 'mobile' in c.lower() or 'landline' in c.lower()]
        for col in phone_cols:
            df[col] = df[col].apply(clean_phone)
            
        # 2. Normalize Headers
        df = normalize_generic_headers(df, file_path)
        
        # 3. Handle Pincodes (ensure string to keep leading zeros)
        pincode_cols = [c for c in df.columns if 'pincode' in c.lower()]
        for col in pincode_cols:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(6)
            df[col] = df[col].replace('000nan', '') # Clean up NaN artifacts from zfill
            
        # Save processed file
        base_name = os.path.basename(file_path)
        output_path = os.path.join(output_dir, base_name)
        df.to_csv(output_path, index=False)
        return True
    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        return False

if __name__ == "__main__":
    # Example usage - paths would be dynamic in a real run
    SOURCE_DIR = r"C:\programs\700GB Cleaning Shi\Sample\Sorted\csv\schema"
    OUTPUT_DIR = r"C:\programs\700GB Cleaning Shi\Scripts 07-05-2026\normalized_outputs"
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    # For demonstration, we'll just show the logic. 
    # In a full run, we would iterate through schema_report.csv
    print(f"Script initialized. Ready to process files into {OUTPUT_DIR}")
