import os

def update_header(file_path, mappings, remove_line_2=False):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    
    if not lines:
        return

    if remove_line_2 and len(lines) > 1:
        # Check if line 2 is a redundant header
        if 'Email Address' in lines[1] or 'Phone Number' in lines[1]:
            lines.pop(1)
            print(f"Removed redundant line 2 from {file_path}")

    existing_header = lines[0].strip().split(',')
    new_header = [f"Column_{i}" for i in range(len(existing_header))]
    for i, col in enumerate(existing_header):
        col_clean = col.strip().replace('"', '')
        if col_clean and 'data_col_' not in col_clean and 'col_' not in col_clean:
             new_header[i] = col_clean
    
    for idx, name in mappings.items():
        if idx < len(new_header):
            new_header[idx] = name
            
    lines[0] = ','.join(new_header) + '\n'
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    print(f"Updated {file_path}")

base_dir = r"Sample\Sorted\csv\schema"

updates = {
    "FILE_003166_5e7ef399_new.csv": {17: "Email_Address"},
    "FILE_003167_594d3eb2_new.csv": {17: "Email_Address"},
    "FILE_003169_4da3f11d_new.csv": ({0: "Email_Address"}, True),
    "FILE_003183_d0af9956_new.csv": {21: "Amount"},
    "FILE_003185_7e4625d1_new.csv": {17: "Email_Address", 26: "City"},
    "FILE_003190_1096a48f_new.csv": {26: "City"},
    "FILE_004445_d60fa0ac_new.csv": {17: "Email_Address"},
    "FILE_004447_d199f474_new.csv": {17: "Email_Address"},
    "FILE_004449_e270510e_new.csv": {17: "Email_Address"},
    "FILE_004451_57e58734_new.csv": {17: "Email_Address"},
}

for filename, val in updates.items():
    if isinstance(val, tuple):
        update_header(os.path.join(base_dir, filename), val[0], val[1])
    else:
        update_header(os.path.join(base_dir, filename), val)
