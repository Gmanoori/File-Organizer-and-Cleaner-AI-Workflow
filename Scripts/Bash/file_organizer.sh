#!/bin/bash

################################################################################
# FILE ORGANIZER & ANALYZER
# 
# Purpose: Recursively scan directories, categorize files by extension,
#          count them, move to organized folders, and generate analysis report
#
# Usage: ./file_organizer.sh -s SOURCE_DIR [OPTIONS]
#
# Author: Data Organization Script
# Version: 1.0
################################################################################

set -uo pipefail

# ============================================================================
# COLORS & FORMATTING
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ============================================================================
# CONFIGURATION & VARIABLES
# ============================================================================


DRY_RUN=false
VERBOSE=true
MERGE_EXCEL=false
REPORT_FILE="file_analysis_report.md"
CURRENT_TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
START_TIME=$(date +%s)

# Counters for main file types
COUNT_CSV=0
COUNT_XLSX=0
COUNT_XLS=0
COUNT_JSON=0
COUNT_TOTAL=0
COUNT_ARCHIVES=0
COUNT_EXTRACTED=0
COUNT_EXTRACT_FAILED=0

# Arrays for unexpected files (type -> array of files)
declare -A UNEXPECTED_FILES
declare -a UNEXPECTED_TYPES

# Directory mappings
declare -A DIR_MAPPING=(
    ["csv"]="CSV"
    ["xlsx"]="XLSX"
    ["xls"]="XLS"
    ["json"]="JSON"
    ["zip"]="ZIP"
    ["rar"]="RAR"
)

# Archive/unexpected extensions to track
ARCHIVE_EXTENSIONS=("tar" "gz" "7z" "iso" "dmg" "exe" "pdf" "docx" "doc" "txt" "ppt" "pptx" "rtf" "dat" "bin" "sql")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================



log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_verbose() {
    if [ "$VERBOSE" = true ]; then
        echo -e "${CYAN}[VERBOSE]${NC} $1"
    fi
}

# Print a nice box
print_box() {
    local text="$1"
    local length=${#text}
    local padding=$((length + 4))
    
    echo ""
    echo -e "${BOLD}$(printf '╔%.0s' $(seq 1 $padding))╗${NC}"
    echo -e "${BOLD}║${NC}  ${CYAN}${BOLD}${text}${NC}  ${BOLD}║${NC}"
    echo -e "${BOLD}$(printf '╚%.0s' $(seq 1 $padding))╝${NC}"
    echo ""
}

print_help() {
    cat << EOF

${BOLD}FILE ORGANIZER & ANALYZER${NC}

Usage: ./file_organizer.sh -s SOURCE_DIR [OPTIONS]

Options:
  ${CYAN}-s, --source${NC}        Source directory to scan and organize (required)
  ${CYAN}-t, --target${NC}        Target directory for organized output (defaults to source)
  ${CYAN}-m, --merge-excel${NC}   Merge .XLS files into the XLSX folder
  ${CYAN}-d, --dry-run${NC}       Show actions without moving any files
  ${CYAN}-v, --verbose${NC}       Enable verbose logging
  ${CYAN}-r, --report${NC}        Custom report file name (markdown)
  ${CYAN}-h, --help${NC}          Display this help message and exit

Functions:
  ${CYAN}scan_files()${NC}         Recursively finds files by extension and counts them
  ${CYAN}move_files()${NC}         Moves matched files into organized folders
  ${CYAN}extract_archives()${NC}   Extracts ZIP/RAR archives into EXTRACTED/
  ${CYAN}generate_report()${NC}    Creates a markdown analysis report

Examples:
  ./file_organizer.sh -s "/path/to/source"
  ./file_organizer.sh -s "/path/to/source" -t "/path/to/target" -d
  ./file_organizer.sh -s "/path/to/source" -m -r "output_report.md"

EOF
}

# ============================================================================
# PARSE COMMAND LINE ARGUMENTS
# ============================================================================

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--source)
                SOURCE_DIR="$2"
                shift 2
                ;;
            -t|--target)
                TARGET_DIR="$2"
                shift 2
                ;;
            -m|--merge-excel)
                MERGE_EXCEL=true
                shift
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -r|--report)
                REPORT_FILE="$2"
                shift 2
                ;;
            -h|--help)
                print_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                print_help
                exit 1
                ;;
        esac
    done
}

# ============================================================================
# VALIDATION
# ============================================================================

validate_inputs() {
    # Check if source directory is provided
    if [ -z "$SOURCE_DIR" ]; then
        log_error "Source directory is required"
        print_help
        exit 1
    fi
    
    # Check if source directory exists
    if [ ! -d "$SOURCE_DIR" ]; then
        log_error "Source directory does not exist: $SOURCE_DIR"
        exit 1
    fi
    
    # Set target directory (default to source if not provided)
    if [ -z "$TARGET_DIR" ]; then
        TARGET_DIR="$SOURCE_DIR"
    fi
    
    # Check if target directory exists
    if [ ! -d "$TARGET_DIR" ]; then
        log_error "Output directory does not exist: $TARGET_DIR"
        exit 1
    fi
    
    log_verbose "Source directory: $SOURCE_DIR"
    log_verbose "Output directory: $TARGET_DIR"
    log_verbose "Merge Excel: $MERGE_EXCEL"
    log_verbose "Dry Run: $DRY_RUN"
}

# ============================================================================
# DIRECTORY CREATION
# ============================================================================

create_directories() {
    print_box "Creating Directories"
    
    local dirs_to_create=()
    
    # Always create main directories
    dirs_to_create+=("CSV" "JSON" "ARCHIVES" "EXTRACTED")
    dirs_to_create+=("ARCHIVES/ZIP" "ARCHIVES/RAR" "EXTRACTED/ZIP" "EXTRACTED/RAR")
    
    # Handle Excel folders based on merge flag
    if [ "$MERGE_EXCEL" = true ]; then
        log_info "Merge Excel enabled: XLS files will go to XLSX folder"
        dirs_to_create+=("XLSX")
    else
        dirs_to_create+=("XLSX" "XLS")
    fi
    
    for dir in "${dirs_to_create[@]}"; do
        local full_path="${TARGET_DIR}/${dir}"
        
        if [ -d "$full_path" ]; then
            log_verbose "Directory already exists: $full_path"
        else
            if [ "$DRY_RUN" = false ]; then
                mkdir -p "$full_path"
                log_success "Created directory: ${BLUE}$dir${NC}"
            else
                log_info "[DRY-RUN] Would create: ${BLUE}$dir${NC}"
            fi
        fi
    done
}

# ============================================================================
# FILE SCANNING & COUNTING
# ============================================================================

scan_files() {
    print_box "Scanning Files Recursively"
    
    log_info "Scanning: ${CYAN}${SOURCE_DIR}${NC}"
    
    # Array to store found files
    declare -a csv_files
    declare -a xlsx_files
    declare -a xls_files
    declare -a json_files
    declare -a archive_files
    
    # Scan for main extensions (case-insensitive)
    while IFS= read -r -d '' file; do
    echo "Working still"

    filename=$(basename "$file")   # ❗ removed local
    extension="${filename##*.}"
    extension=$(echo "$extension" | tr '[:upper:]' '[:lower:]')

    log_verbose "Found: $file (.$extension)"

    case "$extension" in
        csv)
            ((COUNT_CSV++))
            csv_files+=("$file")
            ;;
        xlsx)
            ((COUNT_XLSX++))
            xlsx_files+=("$file")
            ;;
        xls)
            ((COUNT_XLS++))
            xls_files+=("$file")
            ;;
        json)
            ((COUNT_JSON++))
            json_files+=("$file")
            ;;
        zip)
            ((COUNT_ARCHIVES++))
            archive_files+=("$file")
            ;;
        rar)
            ((COUNT_ARCHIVES++))
            archive_files+=("$file")
            ;;
            esac

    ((COUNT_TOTAL++))

    done < <(find "$SOURCE_DIR" -type f \( -iname "*.csv" -o -iname "*.xlsx" -o -iname "*.xls" -o -iname "*.json" -o -iname "*.zip" -o -iname "*.rar" \) -print0)
    # Now scan for unexpected files
    scan_unexpected_files
    
    # Print counters
    echo ""
    echo -e "${BOLD}File Count Summary:${NC}"
    echo -e "  ${CYAN}CSV${NC}        files: ${GREEN}${COUNT_CSV}${NC}"
    echo -e "  ${CYAN}XLSX${NC}       files: ${GREEN}${COUNT_XLSX}${NC}"
    echo -e "  ${CYAN}XLS${NC}        files: ${GREEN}${COUNT_XLS}${NC}"
    echo -e "  ${CYAN}JSON${NC}       files: ${GREEN}${COUNT_JSON}${NC}"
    echo -e "  ${YELLOW}ARCHIVES${NC} files: ${GREEN}${COUNT_ARCHIVES}${NC} (zip/rar)"
    echo -e "  ${BOLD}Total:${NC}      ${GREEN}${COUNT_TOTAL}${NC}"
    echo ""
    
    # Export arrays globally for use in move_files
    declare -g CSV_FILES=("${csv_files[@]}")
    declare -g XLSX_FILES=("${xlsx_files[@]}")
    declare -g XLS_FILES=("${xls_files[@]}")
    declare -g JSON_FILES=("${json_files[@]}")
    declare -g ARCHIVE_FILES=("${archive_files[@]}")
}

scan_unexpected_files() {
    log_info "Scanning for unexpected file types..."
    
    # Create temporary associative array for type counts
    declare -gA UNEXPECTED_COUNT
    
    # Find all files and check against expected extensions
    while IFS= read -r -d '' file; do
        local filename=$(basename "$file")
        local extension="${filename##*.}"
        extension=$(echo "$extension" | tr '[:upper:]' '[:lower:]')
        
        # Skip if it's a main extension
        case "$extension" in
            csv|xlsx|xls|json|zip|rar)
                continue
                ;;
        esac
        
        # Check if it's an archive/unexpected type
        local is_unexpected=false
        for arch_ext in "${ARCHIVE_EXTENSIONS[@]}"; do
            if [ "$extension" = "$arch_ext" ]; then
                is_unexpected=true
                break
            fi
        done
        
        if [ "$is_unexpected" = true ]; then
            # Initialize counter if needed
            if [[ ! -v UNEXPECTED_COUNT[$extension] ]]; then
                UNEXPECTED_COUNT[$extension]=0
                UNEXPECTED_TYPES+=("$extension")
            fi
            
            # Increment counter
            ((UNEXPECTED_COUNT[$extension]++))
            
            # Store filename
            if [[ ! -v UNEXPECTED_FILES[$extension] ]]; then
                UNEXPECTED_FILES[$extension]=""
            else
                UNEXPECTED_FILES[$extension]+=$'\n'
            fi
            UNEXPECTED_FILES[$extension]+="$file"
            
            log_verbose "Unexpected: $file (.$extension)"
        fi
    done < <(find "$SOURCE_DIR" -type f -print0)
    
    # Sort and deduplicate unexpected types
    IFS=$'\n' read -rd '' -a UNEXPECTED_TYPES < <(printf '%s\n' "${UNEXPECTED_TYPES[@]}" | sort -u) || true
    
    # Print unexpected files summary
    if [ ${#UNEXPECTED_TYPES[@]} -gt 0 ]; then
        echo ""
        echo -e "${YELLOW}Unexpected File Types Found:${NC}"
        for type in "${UNEXPECTED_TYPES[@]}"; do
            count=${UNEXPECTED_COUNT[$type]:-0}
            echo -e "  ${YELLOW}.$type${NC}: ${BOLD}${count}${NC} file(s)"
        done
        echo ""
    fi
}

# ============================================================================
# FILE MOVEMENT
# ============================================================================

move_files() {
    print_box "Moving Files to Organized Folders"
    
    local moved_count=0
    
    # Move CSV files
    if [ ${COUNT_CSV} -gt 0 ]; then
        log_info "Moving ${CYAN}${COUNT_CSV}${NC} CSV files..."
        for file in "${CSV_FILES[@]}"; do
            move_file_to_folder "$file" "CSV"
            ((moved_count++))
        done
    fi
    
    # Move XLSX files
    if [ ${COUNT_XLSX} -gt 0 ]; then
        log_info "Moving ${CYAN}${COUNT_XLSX}${NC} XLSX files..."
        for file in "${XLSX_FILES[@]}"; do
            move_file_to_folder "$file" "XLSX"
            ((moved_count++))
        done
    fi
    
    # Move XLS files
    if [ ${COUNT_XLS} -gt 0 ]; then
        local target_folder="XLS"
        if [ "$MERGE_EXCEL" = true ]; then
            target_folder="XLSX"
            log_info "Moving ${CYAN}${COUNT_XLS}${NC} XLS files to XLSX (merge enabled)..."
        else
            log_info "Moving ${CYAN}${COUNT_XLS}${NC} XLS files..."
        fi
        
        for file in "${XLS_FILES[@]}"; do
            move_file_to_folder "$file" "$target_folder"
            ((moved_count++))
        done
    fi
    
    # Move JSON files
    if [ ${COUNT_JSON} -gt 0 ]; then
        log_info "Moving ${CYAN}${COUNT_JSON}${NC} JSON files..."
        for file in "${JSON_FILES[@]}"; do
            move_file_to_folder "$file" "JSON"
            ((moved_count++))
        done
    fi

    # Move archive files
    if [ ${#ARCHIVE_FILES[@]} -gt 0 ]; then
        log_info "Moving ${CYAN}${#ARCHIVE_FILES[@]}${NC} archive files..."

        for file in "${ARCHIVE_FILES[@]}"; do
            local extension="${file##*.}"
            extension=$(echo "$extension" | tr '[:upper:]' '[:lower:]')

            if [ "$extension" = "zip" ]; then
                move_file_to_folder "$file" "ARCHIVES/ZIP"
            elif [ "$extension" = "rar" ]; then
                move_file_to_folder "$file" "ARCHIVES/RAR"
            else
                move_file_to_folder "$file" "ARCHIVES"
            fi
            ((moved_count++))
        done
    fi
    
    echo ""
    log_success "Total files moved: ${GREEN}${moved_count}${NC}"
    echo ""
}

move_file_to_folder() {
    local file="$1"
    local folder="$2"
    local dest_path="${TARGET_DIR}/${folder}/$(basename "$file")"
    
    # Check if file already exists at destination
    if [ -f "$dest_path" ]; then
        log_warning "File already exists (skipping): $(basename "$file")"
        return
    fi
    
    if [ "$DRY_RUN" = false ]; then
        # Actually move the file
        mv "$file" "$dest_path" 2>/dev/null || {
            log_error "Failed to move: $file"
            return 1
        }
        log_verbose "Moved: $(basename "$file") → $folder/"
    else
        log_info "[DRY-RUN] Would move: $(basename "$file") → $folder/"
    fi
}



# ============================================================================
# EXTRACT ARCHIVES
# ============================================================================

extract_archives() {
    print_box "Extracting Archives"

    local archive_dir="${TARGET_DIR}/ARCHIVES"
    local extract_dir="${TARGET_DIR}/EXTRACTED"

    mkdir -p "$extract_dir"

    find "$archive_dir" -type f \( -iname "*.zip" -o -iname "*.rar" \) -print0 | \
    while IFS= read -r -d '' file; do

        local filename=$(basename "$file")
        local extension="${filename##*.}"
        extension=${extension^^}
        local name="${filename%.*}"
        local dest="${extract_dir}/${extension}/${name}"

        mkdir -p "$dest"

        case "$file" in
            *.zip)
                log_info "Extracting ZIP: $filename"
                if unzip -o "$file" -d "$dest" >/dev/null 2>&1; then
                    ((COUNT_EXTRACTED++))
                else
                    ((COUNT_EXTRACT_FAILED++))
                    log_error "ZIP extraction failed: $file"
                fi
                ;;
            *.rar)
                log_info "Extracting RAR: $filename"
                # unrar x -ro+ "$file" "$dest"
                
                if unrar x -ro+ "$file" "$dest" >/dev/null 2>&1; then
                    ((COUNT_EXTRACTED++))
                else
                    ((COUNT_EXTRACT_FAILED++))
                    log_error "RAR extraction failed: $file"
                fi
                ;;
        esac
    done

    echo ""
    echo -e "${BOLD}Extraction Summary:${NC}"
    echo -e "  ${CYAN}Archives Processed:${NC} ${COUNT_ARCHIVES}"
    echo -e "  ${GREEN}Successfully Extracted:${NC} ${COUNT_EXTRACTED}"
    echo -e "  ${RED}Failed:${NC} ${COUNT_EXTRACT_FAILED}"
    echo ""
}

# ============================================================================
# REPORT GENERATION
# ============================================================================

generate_report() {
    print_box "Generating Analysis Report"
    
    local report_path="${TARGET_DIR}/${REPORT_FILE}"
    
    # Create markdown report
    {
        cat << EOF
# File Organization Analysis Report

**Generated**: ${CURRENT_TIMESTAMP}  
**Source Directory**: \`${SOURCE_DIR}\`  
**Output Directory**: \`${TARGET_DIR}\`  
**Total Files Analyzed**: ${COUNT_TOTAL}

---

## Summary

| File Type | Count | Status |
|-----------|-------|--------|
| CSV       | ${COUNT_CSV} | ✓ Moved to \`CSV/\` |
| XLSX      | ${COUNT_XLSX} | ✓ Moved to \`XLSX/\` |
| XLS       | ${COUNT_XLS} | $([ "$MERGE_EXCEL" = true ] && echo "✓ Moved to \`XLSX/\` (merged)" || echo "✓ Moved to \`XLS/\`") |
| JSON      | ${COUNT_JSON} | ✓ Moved to \`JSON/\` |
| Archives (ZIP/RAR) | ${COUNT_ARCHIVES} | ✓ Moved to \`ARCHIVES/ZIP\` and \`ARCHIVES/RAR\` |

**Total Main Files**: ${COUNT_TOTAL}

---

## Archive Extraction Summary

| Metric | Count |
|--------|------|
| Archives Found | ${COUNT_ARCHIVES} |
| Successfully Extracted | ${COUNT_EXTRACTED} |
| Failed Extractions | ${COUNT_EXTRACT_FAILED} |


EOF

        # Add unexpected files section if any found
        if [ ${#UNEXPECTED_TYPES[@]} -gt 0 ]; then
            cat << EOF
## Unexpected File Types

**Total Unexpected Files**: $(echo "${UNEXPECTED_TYPES[@]}" | wc -w)

| Type | Count | Filenames |
|------|-------|-----------|
EOF
            
            for type in "${UNEXPECTED_TYPES[@]}"; do
                local count=${UNEXPECTED_COUNT[$type]:-0}
                local files="${UNEXPECTED_FILES[$type]}"
                
                # Format filenames (first 3 shown, rest truncated)
                local file_list=$(echo "$files" | head -3 | sed 's/^/`/' | sed 's/$/`/')
                local file_count=$(echo "$files" | wc -l)
                
                if [ "$file_count" -gt 3 ]; then
                    file_list="${file_list}, ... and $((file_count - 3)) more"
                fi
                
                # Remove newlines from file_list for table
                file_list=$(echo "$file_list" | tr '\n' ' ')
                
                echo "| .$type | $count | $file_list |"
            done
            
            cat << EOF

---

EOF
        fi

        # Notes about XLS/XLSX merging
        if [ "$MERGE_EXCEL" = true ]; then
            cat << EOF
## Excel Files Merge Notes

XLS files have been merged with XLSX files in the \`XLSX/\` folder.

### Red Flags for XLS/XLSX Merging:
- ⚠️ **Format Difference**: XLSX is modern XML-based, XLS is legacy binary
- ⚠️ **Data Loss Risk**: Some features may not transfer perfectly
- ⚠️ **Macros**: .xlsm (macro-enabled) files may lose VBA code
- ⚠️ **Compatibility**: Older Excel versions may not read XLSX files
- ⚠️ **Encoding**: Legacy XLS files might have encoding issues in XLSX

### Recommendation:
Keep XLS and XLSX separate unless you've verified compatibility of all files.

---

EOF
        fi

        # Execution statistics
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        
        cat << EOF
## Execution Details

- **Script Mode**: $([ "$DRY_RUN" = true ] && echo "Dry Run (No Changes)" || echo "Production")
- **Merge Excel**: $([ "$MERGE_EXCEL" = true ] && echo "Enabled" || echo "Disabled")
- **Execution Time**: ${DURATION} seconds
- **Report File**: \`${REPORT_FILE}\`

---

*Generated by File Organizer & Analyzer v1.0*

EOF
    } > "$report_path"
    
    log_success "Report generated: ${CYAN}${REPORT_FILE}${NC}"
    log_verbose "Report location: $report_path"
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
    print_box "FILE ORGANIZER & ANALYZER"
    
    # Parse arguments
    parse_arguments "$@"
    
    # Validate inputs
    validate_inputs
    
    # Create directories
    create_directories
    
    # Scan files
    scan_files
    
    # Move files
    if [ "$DRY_RUN" = false ]; then
        move_files
        extract_archives
    else
        echo ""
        log_warning "DRY RUN MODE: No files were actually moved"
        echo ""
    fi
    
    # Generate report
    generate_report
    
    # Build CSV inventory
    log_info "Building CSV inventory..."
    ./csv_builder.sh "$TARGET_DIR"
    
    # Final summary
    print_box "✓ Operation Complete"
    
    if [ "$DRY_RUN" = true ]; then
        log_warning "This was a DRY RUN. Run without -d flag to actually move files."
    fi
}

# ============================================================================
# SCRIPT ENTRY POINT
# ============================================================================

if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi