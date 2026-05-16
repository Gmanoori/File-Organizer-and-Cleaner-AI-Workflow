#!/bin/bash

################################################################################
# FILE ORGANIZER V3
# 
# Purpose: Recursively scan a source directory, copy files to a destination
#          organized by file type, rename them with a unique ID, and generate
#          a comprehensive CSV inventory.
#
# Usage: ./file_organizer_v3.sh -s <source_dir> -d <dest_dir> [-i <inventory_csv>]
################################################################################

set -uo pipefail

# ============================================================================
# COLORS & FORMATTING
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
BOLD='\033[1m'

# ============================================================================
# VARIABLES & DEFAULTS
# ============================================================================
SOURCE_DIR=""
DEST_DIR=""
INVENTORY_FILE="file_inventory_$(date +%Y%m%d_%H%M%S).csv"
COUNTER=1

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[✓]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

print_help() {
    cat << EOF
Usage: $0 -s <source_dir> -d <dest_dir> [-i <inventory_csv>]

Options:
  -s, --source      Source directory to scan (required)
  -d, --dest        Destination directory for organized files (required)
  -i, --inventory   Name of the inventory CSV file (default: file_inventory_TIMESTAMP.csv)
  -h, --help        Show this help message
EOF
}

generate_unique_id() {
    # Generates a unique ID using a counter and a hash of the original path
    local file_path="$1"
    local hash
    hash=$(echo -n "$file_path" | md5sum | cut -c1-8)
    printf "FILE_%06d_%s" "$COUNTER" "$hash"
}

# ============================================================================
# ARGUMENT PARSING
# ============================================================================
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--source) SOURCE_DIR="$2"; shift 2 ;;
        -d|--dest) DEST_DIR="$2"; shift 2 ;;
        -i|--inventory) INVENTORY_FILE="$2"; shift 2 ;;
        -h|--help) print_help; exit 0 ;;
        *) log_error "Unknown option: $1"; print_help; exit 1 ;;
    esac
done

# ============================================================================
# VALIDATION
# ============================================================================
if [[ -z "$SOURCE_DIR" || -z "$DEST_DIR" ]]; then
    log_error "Source and Destination directories are required."
    print_help
    exit 1
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
    log_error "Source directory does not exist: $SOURCE_DIR"
    exit 1
fi

# Create Destination Directory if it doesn't exist
mkdir -p "$DEST_DIR" || { log_error "Failed to create destination directory"; exit 1; }

# Get absolute paths
SOURCE_DIR=$(realpath "$SOURCE_DIR")
DEST_DIR=$(realpath "$DEST_DIR")

# ============================================================================
# INITIALIZE INVENTORY
# ============================================================================
echo ",original_path,destination_path,file_type,extension" > "$INVENTORY_FILE"
log_info "Inventory will be saved to: $INVENTORY_FILE"

# ============================================================================
# MAIN PROCESSING LOOP
# ============================================================================
log_info "Scanning source: $SOURCE_DIR"

# Count total files for progress tracking (optional but helpful)
TOTAL_FILES=$(find "$SOURCE_DIR" -type f | wc -l)
log_info "Found $TOTAL_FILES files to process."

# Use find -print0 to handle special characters and spaces safely
find "$SOURCE_DIR" -type f -print0 | while IFS= read -r -d '' file; do
    # Get filename and extension
    filename=$(basename "$file")
    
    # Handle files with or without extensions
    if [[ "$filename" == *.* ]]; then
        extension="${filename##*.}"
        extension_lower=$(echo "$extension" | tr '[:upper:]' '[:lower:]')
    else
        extension="no_ext"
        extension_lower="no_ext"
    fi

    # Categorize by extension
    file_type="$extension_lower"
    target_folder="${DEST_DIR}/${file_type}"
    mkdir -p "$target_folder"

    # Generate Unique ID and new filename
    unique_id=$(generate_unique_id "$file")
    new_filename="${unique_id}.${extension}"
    dest_path="${target_folder}/${new_filename}"

    # Perform the copy (using cp to keep source intact, change to mv if needed)
    if cp "$file" "$dest_path"; then
        # Append to CSV (escaping quotes if necessary, though paths here are absolute)
        echo "\"$unique_id\",\"$file\",\"$dest_path\",\"$file_type\",\"$extension\"" >> "$INVENTORY_FILE"
        
        # Progress update
        if (( COUNTER % 100 == 0 )); then
            log_info "Processed $COUNTER / $TOTAL_FILES files..."
        fi
        
        ((COUNTER++))
    else
        log_error "Failed to copy: $file"
    fi
done

log_success "Processing complete!"
log_success "Total files processed: $((COUNTER - 1))"
log_success "Inventory file: $INVENTORY_FILE"
log_success "Organized files in: $DEST_DIR"
