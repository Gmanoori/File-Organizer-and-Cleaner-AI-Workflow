import pandas as pd
import numpy as np
import os
import re
import uuid
import logging
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class HeaderMapper:
    """Handles mapping of various source headers to a normalized schema."""
    def __init__(self):
        self.mapping = {
            'email': ['email_1', 'email_2', 'email_address', 'email', 'email [1-4]'],
            'phone': ['phone_number', 'mobile_1', 'mobile_2', 'phone_number_1', 'landline_1', 'phone_number_alt_1', 'mobile_alt_1', 'tel no.', 'tel (*)'],
            'company_id': ['company_id', 'id_internal'],
            'company_name': ['company_name', 'company', 'bill_company', 'company_name'],
            'full_name': ['contact person', 'contact_first_name', 'contact_last_name', 'alt_first_name', 'alt_last_name', 'name'],
            'pincode': ['pincode', 'pincode_1', 'city_state_pincode'],
            'last_updated': ['last_updated', 'date_timestamp', 'date']
        }
        self.reverse_map = {}
        for target, aliases in self.mapping.items():
            for alias in aliases:
                self.reverse_map[alias.lower()] = target

    def normalize_headers(self, df):
        new_cols = {}
        for col in df.columns:
            clean_col = str(col).lower().strip()
            if clean_col in self.reverse_map:
                target = self.reverse_map[clean_col]
                # Avoid collision if multiple source cols map to same target (e.g. email_1, email_2)
                if target in new_cols or target in df.columns:
                    suffix = 1
                    while f"{target}_{suffix}" in new_cols or f"{target}_{suffix}" in df.columns:
                        suffix += 1
                    new_cols[col] = f"{target}_{suffix}"
                else:
                    new_cols[col] = target
        return df.rename(columns=new_cols)

class MasterJoiner:
    def __init__(self, output_file, quarantine_file):
        self.output_file = output_file
        self.quarantine_file = quarantine_file
        self.master_df = pd.DataFrame()
        self.identity_map = {} # key -> master_id
        self.header_mapper = HeaderMapper()

    def normalize_phone(self, val):
        if pd.isna(val) or val == '': return ""
        s = str(val).strip().replace('-', '').replace(' ', '')
        return re.sub(r'\D', '', s)

    def normalize_date(self, val):
        if pd.isna(val) or val == '': return None
        # Try a few common formats
        for fmt in ('%d-%m-%Y %H:%M', '%Y-%m-%d', '%d/%m/%Y', '%d-%b-%y', '%d %b %Y'):
            try:
                return datetime.strptime(str(val), fmt)
            except ValueError:
                continue
        return None

    def calculate_completeness(self, row):
        non_null = row.count()
        return non_null / len(row) if len(row) > 0 else 0

    def process_file(self, file_path):
        logging.info(f"Processing {os.path.basename(file_path)}...")
        try:
            df = pd.read_csv(file_path, low_memory=False)
            df = self.header_mapper.normalize_headers(df)
            
            # Normalize key columns
            if 'phone' in df.columns: df['phone'] = df['phone'].apply(self.normalize_phone)
            if 'email' in df.columns: df['email'] = df['email'].astype(str).str.lower().str.strip()
            if 'last_updated' in df.columns: df['last_updated'] = df['last_updated'].apply(self.normalize_date)
            
            # Track lineage
            df['source_origins'] = os.path.basename(file_path)

            for idx, row in df.iterrows():
                self.merge_row(row)
                
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")

    def merge_row(self, row):
        # 1. Identify Identity Keys
        keys = []
        if not pd.isna(row.get('company_id')): keys.append(f"cid_{row['company_id']}")
        if not pd.isna(row.get('email')) and row['email'] != 'nan': keys.append(f"eml_{row['email']}")
        if not pd.isna(row.get('phone')) and row['phone'] != '': keys.append(f"phn_{row['phone']}")

        # 2. Check for Identity Match
        match_id = None
        for k in keys:
            if k in self.identity_map:
                match_id = self.identity_map[k]
                break
        
        if match_id is not None:
            # JOIN / ENRICH attempt
            existing_idx = self.master_df.index[self.master_df['master_id'] == match_id][0]
            existing_row = self.master_df.loc[existing_idx]
            
            # Conflict Check
            has_conflict = False
            for col in row.index:
                if col in ['master_id', 'source_origins', 'last_updated']: continue
                if not pd.isna(row.get(col)) and not pd.isna(existing_row.get(col)):
                    if str(row[col]).strip() != str(existing_row[col]).strip():
                        has_conflict = True
                        break
            
            # Resolution Logic
            if has_conflict:
                # If timestamp exists, we can resolve
                if 'last_updated' in row and 'last_updated' in existing_row:
                    if not pd.isna(row['last_updated']) and not pd.isna(existing_row['last_updated']):
                        if row['last_updated'] > existing_row['last_updated']:
                            # New row is better, overwrite non-identity fields
                            for col in row.index:
                                if col not in ['master_id', 'source_origins']:
                                    self.master_df.at[existing_idx, col] = row[col]
                            self.master_df.at[existing_idx, 'source_origins'] += f", {row['source_origins']}"
                            return # Resolved
                        else:
                            return # Existing row is better, ignore new one (or we could union, but Plan says Latest Wins)
                
                # If no timestamp or still conflict, UNION both (User request)
                new_id = str(uuid.uuid4())
                row['master_id'] = new_id
                # Note: We DON'T update identity_map with this new_id to avoid hijacking the primary identity
                self.master_df = pd.concat([self.master_df, pd.DataFrame([row])], ignore_index=True)
            else:
                # No conflict, just fill gaps (Enrich)
                for col in row.index:
                    if pd.isna(existing_row.get(col)) or str(existing_row.get(col)) == '':
                        self.master_df.at[existing_idx, col] = row[col]
                # Update origins
                origins = str(existing_row.get('source_origins', ''))
                if row['source_origins'] not in origins:
                    self.master_df.at[existing_idx, 'source_origins'] = origins + ", " + row['source_origins']
        else:
            # Check for Schema Overlap (UNION) vs Quarantine
            overlap = row.count()
            if overlap > 3:
                # NEW RECORD
                new_id = str(uuid.uuid4())
                row['master_id'] = new_id
                for k in keys:
                    self.identity_map[k] = new_id
                self.master_df = pd.concat([self.master_df, pd.DataFrame([row])], ignore_index=True)
            else:
                # QUARANTINE
                self.quarantine_row(row)

    def quarantine_row(self, row):
        # Save to a separate file later or handle here
        with open(self.quarantine_file, 'a', newline='', encoding='utf-8') as f:
            row.to_frame().T.to_csv(f, header=f.tell()==0, index=False)

    def finalize(self):
        if not self.master_df.empty:
            # Final completeness score
            self.master_df['record_completeness'] = self.master_df.apply(self.calculate_completeness, axis=1)
            self.master_df.to_csv(self.output_file, index=False)
            logging.info(f"Master file saved to {self.output_file}")
            logging.info(f"Quarantine file at {self.quarantine_file}")

if __name__ == "__main__":
    SCHEMA_REPORT = r"C:\programs\700GB Cleaning Shi\Scripts 07-05-2026\generalize_headers\schema_report.csv"
    OUTPUT = r"C:\programs\700GB Cleaning Shi\Scripts 07-05-2026\generalize_headers\master_consolidated_data.csv"
    QUARANTINE = r"C:\programs\700GB Cleaning Shi\Scripts 07-05-2026\generalize_headers\quarantined_fragments.csv"
    
    joiner = MasterJoiner(OUTPUT, QUARANTINE)
    
    # Read schema report to get files
    try:
        report_df = pd.read_csv(SCHEMA_REPORT)
        for _, row in report_df.iterrows():
            # Use absolute path from report
            file_path = row['full_path']
            if os.path.exists(file_path):
                joiner.process_file(file_path)
            else:
                logging.warning(f"File not found: {file_path}")
    except Exception as e:
        logging.error(f"Failed to read schema report: {e}")
        
    joiner.finalize()
