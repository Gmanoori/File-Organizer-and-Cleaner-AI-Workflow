# File Organizer - Troubleshooting & Advanced Usage

For quick start, see: `00_START_HERE.md`

---

## Troubleshooting Guide

### Script Issues

#### "command not found: file_organizer.sh"
```bash
# Make sure you're in the right directory
pwd
ls file_organizer.sh

# Run with ./
./file_organizer.sh -s ~/Downloads

# Not:
file_organizer.sh -s ~/Downloads
```

#### "Permission denied"
```bash
# Make executable
chmod +x file_organizer.sh

# Verify it worked
ls -l file_organizer.sh
# Should show: -rwxr-xr-x (with x)
```

#### "No such file or directory"
```bash
# The source directory doesn't exist
# Check the path
ls /your/path

# Find correct path
find ~ -name "Downloads" -type d
```

---

### File Movement Issues

#### Files aren't moving in dry-run
```bash
# THIS IS NORMAL!
# Dry-run shows what WOULD happen
# Output: "[DRY-RUN] Would move: file.csv → CSV/"

# This means it WILL move when you remove -d flag
./file_organizer.sh -s ~/Downloads  # Remove -d
```

#### "File already exists (skipping)"
```bash
# Destination already has a file with same name
# Three options:

# Option 1: Use different output directory
./file_organizer.sh -s /source -o /output2

# Option 2: Rename the existing file
mv CSV/duplicate.csv CSV/duplicate_backup.csv
./file_organizer.sh -s ~/Downloads

# Option 3: Check what's there
ls CSV/
```

#### Some files not moving
```bash
# Check file extensions
ls -la /your/path

# Check for hidden characters in filenames
ls | od -c  # Shows non-printable chars

# Run with verbose to see what's being found
./file_organizer.sh -s ~/Downloads -v
```

---

### Report Issues

#### Report not created
```bash
# Check write permissions
touch /your/path/test.txt  # Should work
rm /your/path/test.txt

# Check disk space
df -h /your/path

# Try custom report location
./file_organizer.sh -s ~/Downloads -r ~/my_report.md

# Check if it was created elsewhere
find ~ -name "file_analysis_report.md"
```

#### Report is empty or incomplete
```bash
# Try running verbose
./file_organizer.sh -s ~/Downloads -v

# Check report file
cat file_analysis_report.md | head -20

# Check file size
ls -lh file_analysis_report.md
```

---

### Performance Issues

#### Script running slow
```bash
# Show progress (verbose)
./file_organizer.sh -s ~/Downloads -v

# Estimate time based on file count
find ~/Downloads -type f | wc -l

# For 100K+ files, run in background
./file_organizer.sh -s /massive -v > process.log 2>&1 &

# Monitor progress
tail -f process.log
ps aux | grep file_organizer
```

#### Out of memory
```bash
# Less likely, but possible with 100K+ files
# Close other applications
# Try smaller directory first
./file_organizer.sh -s ~/Downloads  # Smaller test
```

#### Disk space error
```bash
# Check available space
df -h /your/path

# Files are moved, not copied
# So you need: source_size + 5% buffer minimum

# If low on space:
# 1. Delete some files first
# 2. Run on external drive
# 3. Move files in batches
```

---

## Advanced Usage

### Automation with Cron

```bash
# Edit crontab
crontab -e

# Add line to run daily at 2 AM
0 2 * * * /home/user/file_organizer.sh -s ~/Downloads >> /var/log/organizer.log 2>&1

# List your cron jobs
crontab -l

# Stop running cron job
crontab -r
```

### Batch Processing Multiple Directories

```bash
# Run on multiple folders
for dir in ~/Downloads ~/Documents ~/Desktop; do
  echo "Processing: $dir"
  ./file_organizer.sh -s "$dir"
done
```

### Integration with Backup

```bash
# Organize, then backup
./file_organizer.sh -s ~/Downloads
tar -czf ~/backup_organized.tar.gz ~/Downloads/CSV ~/Downloads/XLSX ~/Downloads/JSON

# Or: Backup before organizing
cp -r ~/Downloads ~/Downloads_backup
./file_organizer.sh -s ~/Downloads
```

### Parallel Processing (Advanced)

```bash
# For very large directories, process in parallel
# (Use with caution - test first!)

# Find files and process in parallel (4 jobs)
find ~/Downloads -type f -iname "*.csv" | \
  xargs -P 4 -I {} mv {} ~/Downloads/CSV/

# WARNING: Risky! Use script instead for safety
```

---

## Special Cases

### Files with Spaces in Names
```bash
# The script handles this correctly
# Example: "My File Name.csv" → CSV/
# Uses proper quoting internally
```

### Unicode/International Characters
```bash
# Supported: é, ñ, 中文, 🚀, etc.
# The script uses UTF-8 properly

# If issues occur:
# 1. Check terminal encoding: echo $LANG
# 2. Set to UTF-8: export LANG=en_US.UTF-8
# 3. Re-run script
```

### Symbolic Links
```bash
# Symlinks are followed by default
# If file is at /real/path and linked at /link/path
# The script will move the actual file

# To verify:
ls -l  # Shows L if symlink
file /path/to/file
```

### Very Long Filenames
```bash
# Max filename: 255 characters
# Script handles this automatically
# Older filesystems may have issues
# Check filesystem: mount | grep Downloads
```

---

## Configuration Options

### Custom Directory Names

```bash
# Currently hardcoded: CSV/, XLSX/, XLS/, JSON/
# To change, edit the script:

# Find this section:
# DIR_MAPPING=(
#     ["csv"]="CSV"
#     ["xlsx"]="XLSX"
#     ...
# )

# Change to your preference:
# DIR_MAPPING=(
#     ["csv"]="DataCSV"
#     ["xlsx"]="ExcelData"
#     ...
# )
```

### Add New Extensions

```bash
# To track additional extensions:
# Edit the script and add to:

# 1. The find command:
# -o -iname "*.txt" -o -iname "*.pdf"

# 2. The case statement:
# txt)
#   ((COUNT_TXT++))
#   ;;

# 3. Initialize counter:
# COUNT_TXT=0

# Then add folder creation
```

### Change Report Filename

```bash
# Use -r flag
./file_organizer.sh -s ~/Downloads -r analysis_2024_12_15.md

# Or default: file_analysis_report.md
./file_organizer.sh -s ~/Downloads
```

---

## Dry-Run Deep Dive

### What Dry-Run Shows

```bash
./file_organizer.sh -s ~/Downloads -d

# Output will show:
# [DRY-RUN] Would create: CSV
# [DRY-RUN] Would create: XLSX
# [DRY-RUN] Would move: file.csv → CSV/
# [DRY-RUN] Would move: report.xlsx → XLSX/

# This is a 100% accurate preview of what will happen
```

### Comparing Dry-Run to Actual Run

```bash
# Save dry-run output
./file_organizer.sh -s ~/Downloads -d > dry_run.txt 2>&1

# Run actual
./file_organizer.sh -s ~/Downloads > actual.txt 2>&1

# Compare (they should be similar, minus dry-run messages)
diff dry_run.txt actual.txt
```

---

## Excel File Handling Deep Dive

### XLS vs XLSX Difference

| Aspect | XLS | XLSX |
|--------|-----|------|
| **Year** | 1997 | 2007 |
| **Format** | Binary OLE2 | XML ZIP |
| **Size** | Larger | Smaller |
| **Macros** | Native | .xlsm needed |
| **Encoding** | ANSI | UTF-8 |
| **Excel Version** | 97-2003 | 2007+ |

### When to Merge (Safe)

```bash
# Safe to merge if:
# ✓ No macros (VBA) in any file
# ✓ All users have Excel 2007+
# ✓ No special formatting/pivot tables
# ✓ You tested with sample file first

./file_organizer.sh -s ~/Downloads -m
```

### When NOT to Merge (Risky)

```bash
# DON'T merge if:
# ✗ Any file contains macros (.xls with VBA)
# ✗ Old Excel versions used (97, 2000, 2003)
# ✗ Complex conditional formatting
# ✗ Pivot tables present
# ✗ Special international characters

# Keep separate:
./file_organizer.sh -s ~/Downloads  # No -m flag
```

---

## Report Analysis

### Understanding the Report

```markdown
# Section 1: Summary
Shows counts for each main type
Use this to verify: "I had 5 CSVs, report shows 5 ✓"

# Section 2: Unexpected Files
Lists files that aren't CSV/XLSX/XLS/JSON
"Oh, I forgot I had ZIP files there"

# Section 3: Execution Details
Confirms mode, time taken, report filename
Useful for automation/logging
```

### Using Report for Audit Trail

```bash
# Save reports over time
./file_organizer.sh -s ~/Downloads -r report_2024_01_01.md
./file_organizer.sh -s ~/Downloads -r report_2024_02_01.md

# Compare changes
diff report_2024_01_01.md report_2024_02_01.md

# Track unexpected files
grep -A 20 "Unexpected" report_*.md
```

---

## Performance Optimization

### For 100K+ Files

```bash
# Run at off-hours
# Low system load
# Monitoring enabled

# Run in background
./file_organizer.sh -s /massive -v > organizer.log 2>&1 &

# Monitor with
watch -n 5 'wc -l organizer.log'

# Monitor full system
watch -n 5 'ps aux | grep file_organizer'
```

### Monitoring Disk Space

```bash
# Before running
du -sh ~/Downloads

# During (separate terminal)
watch -n 5 'du -sh ~/Downloads/*/'

# After
du -sh ~/Downloads/CSV ~/Downloads/XLSX ~/Downloads/JSON
```

---

## Rollback (If Something Goes Wrong)

### Manual Rollback (Move Back)

```bash
# If files moved to wrong folder:
# Move them back manually

mv CSV/wrong_file.xlsx ../XLSX/
mv XLSX/wrong_file.csv ../CSV/

# Or move entire folder back
cp -r CSV ~/Downloads_backup/
```

### From Backup

```bash
# If you made a backup first
cp -r ~/Downloads_backup/* ~/Downloads/

# Remove organized folders
rm -rf CSV/ XLSX/ XLS/ JSON/

# Restore original
```

---

## Exit Codes (For Scripting)

```bash
# Success
./file_organizer.sh -s ~/Downloads
echo $?  # Returns 0

# Error (invalid input)
./file_organizer.sh -s /nonexistent
echo $?  # Returns 1

# In scripts:
if ./file_organizer.sh -s ~/Downloads; then
  echo "Success!"
else
  echo "Failed!"
  exit 1
fi
```

---

## Common Mistakes

### ❌ Mistake 1: Wrong path
```bash
# WRONG:
./file_organizer.sh -s Downloads  # Missing ~

# RIGHT:
./file_organizer.sh -s ~/Downloads
./file_organizer.sh -s /Users/username/Downloads
```

### ❌ Mistake 2: Skipping dry-run
```bash
# WRONG: Jump straight to execution
./file_organizer.sh -s ~/Downloads

# RIGHT: Always preview first
./file_organizer.sh -s ~/Downloads -d
# Review output
./file_organizer.sh -s ~/Downloads
```

### ❌ Mistake 3: Not checking report
```bash
# WRONG: Run and forget
./file_organizer.sh -s ~/Downloads

# RIGHT: Check the report
cat file_analysis_report.md
# Verify counts match expectations
```

### ❌ Mistake 4: Merging without testing
```bash
# WRONG: Blind merge
./file_organizer.sh -s ~/Downloads -m

# RIGHT: Test first
# 1. Open one .xls in Excel → Save as XLSX
# 2. Verify nothing breaks
# 3. Then merge all:
./file_organizer.sh -s ~/Downloads -m
```

---

## Quick Reference

**See `00_START_HERE.md` for quick commands**

**Most common issues**: Check script is executable (`chmod +x`), preview with `-d`

**Stuck?** Run: `./file_organizer.sh -h`

