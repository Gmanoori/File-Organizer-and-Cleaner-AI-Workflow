#!/bin/bash

################################################################################
# CSV BUILDER
#
# Purpose: Generate a CSV inventory of all organized files
# Schema: serial_number,filename,file_type,file_path
#
# Usage: ./csv_builder.sh TARGET_DIR
#
# Author: CSV Builder Script
# Version: 1.0
################################################################################

set -uo pipefail

# ============================================================================
# CONFIGURATION
# ============================================================================

CSV_FILE="file_inventory.csv"
SERIAL_NUM=1

# ============================================================================
# FUNCTIONS
# ============================================================================

log_info() {
    echo "[INFO] $1"
}

# Escape CSV field: wrap in quotes and escape internal quotes
escape_csv_field() {
    local field="$1"
    # Replace " with ""
    field=$(echo "$field" | sed 's/"/""/g')
    echo "\"$field\""
}

# ============================================================================
# MAIN LOGIC
# ============================================================================

main() {
    local target_dir="$1"
    local csv_path="${target_dir}/${CSV_FILE}"

    # Check if target directory exists
    if [ ! -d "$target_dir" ]; then
        echo "Error: Target directory does not exist: $target_dir"
        exit 1
    fi

    log_info "Building CSV inventory in: $csv_path"

    # Write CSV header
    echo "serial_number,filename,file_type,file_path" > "$csv_path"

    # Define directories to scan
    local dirs=("CSV" "XLSX" "XLS" "JSON" "ARCHIVES/ZIP" "ARCHIVES/RAR" "EXTRACTED/ZIP" "EXTRACTED/RAR")

    # Scan each directory
    for dir in "${dirs[@]}"; do
        local full_dir="${target_dir}/${dir}"

        if [ -d "$full_dir" ]; then
            # Find all files in this directory recursively
            while IFS= read -r -d '' file; do
                local filename=$(basename "$file")
                local file_type="$dir"
                local file_path="$file"

                # Escape fields for CSV
                local serial_escaped=$(escape_csv_field "$SERIAL_NUM")
                local filename_escaped=$(escape_csv_field "$filename")
                local file_type_escaped=$(escape_csv_field "$file_type")
                local file_path_escaped=$(escape_csv_field "$file_path")

                # Write to CSV
                echo "${serial_escaped},${filename_escaped},${file_type_escaped},${file_path_escaped}" >> "$csv_path"

                ((SERIAL_NUM++))
            done < <(find "$full_dir" -type f -print0)
        fi
    done

    log_info "CSV inventory created with $((SERIAL_NUM - 1)) entries"
}

# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    if [ $# -ne 1 ]; then
        echo "Usage: $0 TARGET_DIR"
        exit 1
    fi
    main "$1"
fi