# MongoDB Import Scripts

This directory contains scripts to bulk import cleaned CSV data into MongoDB.

## Files

- `Scripts/Python/import_to_mongodb.py` - Python script for importing CSV files to MongoDB
- `import_to_mongodb.bat` - Windows batch script for the same purpose

## Prerequisites

1. **MongoDB Installation**: MongoDB must be installed and running
   - Download from: https://www.mongodb.com/try/download/community
   - Follow installation instructions for your platform
2. **mongoimport**: Must be available in PATH (comes with MongoDB installation)
   - Test with: `mongoimport --version`
3. **Python Dependencies** (for Python script):
   - `pymongo` (optional, for connection testing): `pip install pymongo`

## Usage

### Python Script

```bash
# Basic usage - import all CSV files from Cleaned/ directory
python Scripts/Python/import_to_mongodb.py

# Specify custom MongoDB connection
python Scripts/Python/import_to_mongodb.py --uri "mongodb://localhost:27017" --database "mydata"

# Import from custom directory
python Scripts/Python/import_to_mongodb.py --dir "/path/to/cleaned/data"

# Import specific files only
python Scripts/Python/import_to_mongodb.py --files "file1.csv,file2.csv"

# Drop existing collections before importing
python Scripts/Python/import_to_mongodb.py --drop

# Dry run (show what would be imported)
python Scripts/Python/import_to_mongodb.py --dry-run

# Limit number of files (for testing)
python Scripts/Python/import_to_mongodb.py --max-files 5
```

### Batch Script (Windows)

```cmd
# Edit the configuration variables at the top of import_to_mongodb.bat
# Then run:
import_to_mongodb.bat
```

## Configuration

### Environment Variables

You can set these environment variables instead of using command-line arguments:

- `MONGODB_URI` - MongoDB connection string (default: `mongodb://localhost:27017`)
- `MONGODB_DATABASE` - Target database name (default: `cleaned_data`)
- `CLEANED_DATA_DIR` - Directory containing CSV files (default: `Cleaned/`)

### Collection Naming

- Collection names are automatically derived from CSV filenames
- Invalid characters are replaced with underscores
- Names are converted to lowercase
- Maximum length is 120 characters
- Names starting with numbers get prefixed with `col_`

## Examples

### Import all cleaned data to local MongoDB

```bash
cd "C:\programs\700GB Cleaning Shi"
python Scripts/Python/import_to_mongodb.py
```

### Import to remote MongoDB instance

```bash
python Scripts/Python/import_to_mongodb.py \
    --uri "mongodb+srv://username:password@cluster.mongodb.net/" \
    --database "production_data"
```

### Test import with limited files

```bash
python Scripts/Python/import_to_mongodb.py --max-files 3 --dry-run
```

## Troubleshooting

### Connection Issues

- Ensure MongoDB is running: `mongod` or MongoDB service
- Check connection string format
- Verify network access for remote MongoDB

### Import Failures

- Check CSV file format (must have headers in first row)
- Ensure CSV files are not corrupted
- Check MongoDB user permissions for database writes
- Review MongoDB logs for detailed error messages

### Collection Name Issues

- MongoDB collection names cannot contain certain characters
- The script automatically sanitizes names, but very long filenames may be truncated

## Output

The scripts will show progress for each file:

```
📥 Importing file.csv -> database.collection_name
✅ imported 1234 documents
```

At the end, a summary shows total successful and failed imports.