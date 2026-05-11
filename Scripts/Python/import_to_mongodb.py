#!/usr/bin/env python3
"""
MongoDB Bulk Import Script for Cleaned Data Files

This script imports all cleaned CSV files from the data_cleaner_spark.py output directory
into MongoDB using mongoimport for bulk operations.

Usage:
    python import_to_mongodb.py [options]

Requirements:
    - MongoDB installed and running
    - mongoimport in PATH (comes with MongoDB installation)
    - Python packages: pymongo (optional, for connection testing)

Environment Variables:
    MONGODB_URI: MongoDB connection string (default: mongodb://localhost:27017)
    MONGODB_DATABASE: Target database name (default: cleaned_data)
    CLEANED_DATA_DIR: Directory containing cleaned CSV files (default: Cleaned/)
"""

import os
import sys
import subprocess
import argparse
import glob
from pathlib import Path
import json

def test_mongodb_connection(uri, database):
    """Test MongoDB connection using pymongo if available."""
    try:
        import pymongo
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[database]
        print(f"✅ MongoDB connection successful: {uri} -> {database}")
        client.close()
        return True
    except ImportError:
        print("⚠️  pymongo not available, skipping connection test")
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return False

def sanitize_collection_name(filename):
    """Convert filename to valid MongoDB collection name."""
    # Remove file extension
    name = Path(filename).stem

    # Replace invalid characters with underscores
    import re
    name = re.sub(r'[^\w]', '_', name)

    # Remove multiple consecutive underscores
    name = re.sub(r'_+', '_', name)

    # Remove leading/trailing underscores
    name = name.strip('_')

    # Ensure name is not empty and doesn't start with number
    if not name:
        name = "collection"
    if name[0].isdigit():
        name = f"col_{name}"

    # Limit length (MongoDB collection names can be up to 120 characters)
    if len(name) > 120:
        name = name[:117] + "..."

    return name.lower()

def get_csv_header(file_path):
    """Get the first line of CSV file to use as field names."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            first_line = f.readline().strip()
            # Split by comma and clean up field names
            fields = [field.strip().strip('"') for field in first_line.split(',')]
            return fields
    except Exception as e:
        print(f"⚠️  Could not read header from {file_path}: {e}")
        return None

def import_csv_to_mongodb(csv_file, collection_name, mongodb_uri, database, drop_existing=False):
    """Import a single CSV file to MongoDB using mongoimport."""

    # Build mongoimport command
    cmd = [
        'mongoimport',
        '--uri', mongodb_uri,
        '--db', database,
        '--collection', collection_name,
        '--type', 'csv',
        '--file', csv_file,
        '--headerline'  # Use first line as field names
    ]

    if drop_existing:
        cmd.extend(['--drop'])

    print(f"📥 Importing {csv_file} -> {database}.{collection_name}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.returncode == 0:
            # Parse the output to get import statistics
            output_lines = result.stdout.strip().split('\n')
            for line in output_lines:
                if 'imported' in line.lower():
                    print(f"✅ {line.strip()}")
                    break
            else:
                print(f"✅ Successfully imported {csv_file}")
        else:
            print(f"❌ Failed to import {csv_file}")
            print(f"Error: {result.stderr.strip()}")
            return False

    except subprocess.TimeoutExpired:
        print(f"⏰ Timeout importing {csv_file}")
        return False
    except FileNotFoundError:
        print("❌ mongoimport not found. Please ensure MongoDB is installed and mongoimport is in PATH")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error importing {csv_file}: {e}")
        return False

    return True

def main():
    parser = argparse.ArgumentParser(
        description="Bulk import cleaned CSV files to MongoDB using mongoimport",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python import_to_mongodb.py
  python import_to_mongodb.py --uri "mongodb://localhost:27017" --database "mydata"
  python import_to_mongodb.py --dir "/path/to/cleaned" --drop
  python import_to_mongodb.py --files "file1.csv,file2.csv"
        """
    )

    parser.add_argument(
        '--uri',
        default=os.environ.get('MONGODB_URI', 'mongodb://localhost:27017'),
        help='MongoDB connection URI (default: mongodb://localhost:27017)'
    )

    parser.add_argument(
        '--database',
        default=os.environ.get('MONGODB_DATABASE', 'cleaned_data'),
        help='Target database name (default: cleaned_data)'
    )

    parser.add_argument(
        '--dir',
        default=os.environ.get('CLEANED_DATA_DIR', 'Cleaned/'),
        help='Directory containing cleaned CSV files (default: Cleaned/)'
    )

    parser.add_argument(
        '--files',
        help='Comma-separated list of specific CSV files to import (overrides --dir)'
    )

    parser.add_argument(
        '--drop',
        action='store_true',
        help='Drop existing collections before importing'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be imported without actually importing'
    )

    parser.add_argument(
        '--max-files',
        type=int,
        help='Maximum number of files to import (for testing)'
    )

    args = parser.parse_args()

    # Test MongoDB connection
    if not test_mongodb_connection(args.uri, args.database):
        print("❌ Cannot proceed without MongoDB connection")
        sys.exit(1)

    # Get list of CSV files
    if args.files:
        # Specific files provided
        csv_files = [f.strip() for f in args.files.split(',')]
        # Convert to absolute paths
        csv_files = [os.path.abspath(f) for f in csv_files]
    else:
        # Scan directory
        csv_pattern = os.path.join(args.dir, "*.csv")
        csv_files = glob.glob(csv_pattern)
        csv_files.sort()  # Sort for consistent order

    if not csv_files:
        print(f"❌ No CSV files found in {args.dir}")
        sys.exit(1)

    if args.max_files:
        csv_files = csv_files[:args.max_files]

    print(f"📋 Found {len(csv_files)} CSV files to import")
    print(f"🎯 Target: {args.uri} -> {args.database}")
    print(f"📁 Source: {args.dir}")
    print(f"🔄 Drop existing: {args.drop}")
    print(f"🧪 Dry run: {args.dry_run}")
    print()

    # Import each file
    successful_imports = 0
    failed_imports = 0

    for csv_file in csv_files:
        if not os.path.exists(csv_file):
            print(f"❌ File not found: {csv_file}")
            failed_imports += 1
            continue

        filename = os.path.basename(csv_file)
        collection_name = sanitize_collection_name(filename)

        if args.dry_run:
            print(f"📋 Would import: {filename} -> {collection_name}")
            successful_imports += 1
            continue

        if import_csv_to_mongodb(csv_file, collection_name, args.uri, args.database, args.drop):
            successful_imports += 1
        else:
            failed_imports += 1

    print()
    print("📊 Import Summary:")
    print(f"✅ Successful: {successful_imports}")
    print(f"❌ Failed: {failed_imports}")
    print(f"📈 Success Rate: {(successful_imports / (successful_imports + failed_imports) * 100):.1f}%" if (successful_imports + failed_imports) > 0 else "N/A")

if __name__ == '__main__':
    main()