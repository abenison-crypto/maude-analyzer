#!/usr/bin/env python
"""
Test script to verify the new parsing pipeline works correctly.

This script:
1. Parses a few records from each file type
2. Verifies column mapping is correct
3. Shows sample MDR keys for cross-table matching verification
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, SCS_PRODUCT_CODES
from config.logging_config import setup_logging, get_logger
from config.schema_registry import get_fda_columns, DEVICE_COLUMNS_FDA
from config.column_mappings import COLUMN_MAPPINGS, map_record_columns
from src.ingestion.parser import MAUDEParser
from src.ingestion.transformer import DataTransformer

setup_logging()
logger = get_logger("test_parsing")


def test_file_parsing(data_dir: Path, file_type: str, max_records: int = 5):
    """Test parsing a file type and show results."""
    parser = MAUDEParser()
    transformer = DataTransformer()

    # Find files of this type
    patterns = {
        "master": "mdrfoi*.txt",
        "device": "foidev*.txt",
        "patient": "patient*.txt",
        "text": "foitext*.txt",
        "problem": "*problem*.txt",
    }

    pattern = patterns.get(file_type, "*.txt")
    files = sorted(data_dir.glob(pattern))

    # Exclude problem files from device
    if file_type == "device":
        files = [f for f in files if "problem" not in f.name.lower()]

    if not files:
        print(f"No {file_type} files found")
        return None

    # Use the first file
    filepath = files[0]
    print(f"\n{'='*60}")
    print(f"Testing {file_type} file: {filepath.name}")
    print(f"{'='*60}")

    # Detect schema
    schema = parser.detect_schema_from_header(filepath, file_type)
    print(f"\nSchema Detection:")
    print(f"  Column count: {schema.column_count}")
    print(f"  Has header: {schema.has_header}")
    print(f"  Valid: {schema.is_valid}")
    print(f"  Message: {schema.validation_message}")
    print(f"  First 5 columns: {schema.columns[:5]}")

    # Parse a few records
    print(f"\nSample records (first {max_records}):")
    mdr_keys = []
    product_codes = []

    count = 0
    for record in parser.parse_file_dynamic(
        filepath,
        schema=schema,
        file_type=file_type,
        map_to_db_columns=True,
        limit=max_records
    ):
        count += 1

        # Show first record in detail
        if count == 1:
            print(f"\n  Record 1 (first 10 fields):")
            for key, value in list(record.items())[:10]:
                if value:
                    display_val = str(value)[:50] + "..." if len(str(value)) > 50 else value
                    print(f"    {key}: {display_val}")

        # Collect MDR keys for verification
        mdr_key = record.get("mdr_report_key")
        if mdr_key:
            mdr_keys.append(mdr_key)

        # Collect product codes for device files
        if file_type == "device":
            pc = record.get("device_report_product_code")
            if pc:
                product_codes.append(pc)

    print(f"\n  Total records parsed: {count}")

    if mdr_keys:
        print(f"  Sample MDR keys: {mdr_keys[:5]}")

    if product_codes:
        unique_codes = list(set(product_codes))[:10]
        print(f"  Product codes found: {unique_codes}")
        scs_matches = [pc for pc in product_codes if pc in SCS_PRODUCT_CODES]
        print(f"  SCS product code matches: {len(scs_matches)}/{len(product_codes)}")

    return mdr_keys


def test_cross_table_matching(data_dir: Path):
    """Test if MDR keys from devices can be found in master/patient/text files."""
    parser = MAUDEParser()

    print(f"\n{'='*60}")
    print("Cross-Table MDR Key Matching Test")
    print(f"{'='*60}")

    # Get MDR keys from device file (filtered by SCS product codes)
    device_files = [f for f in sorted(data_dir.glob("foidev*.txt")) if "problem" not in f.name.lower()]
    if not device_files:
        print("No device files found")
        return

    # Parse device records with product code filter
    device_mdr_keys = set()
    device_file = device_files[0]
    print(f"\nParsing device file: {device_file.name}")

    for record in parser.parse_file_dynamic(
        device_file,
        file_type="device",
        map_to_db_columns=True,
    ):
        pc = record.get("device_report_product_code")
        if pc in SCS_PRODUCT_CODES:
            mdr_key = record.get("mdr_report_key")
            if mdr_key:
                device_mdr_keys.add(mdr_key)

    print(f"  Device MDR keys (SCS filtered): {len(device_mdr_keys)}")
    print(f"  Sample keys: {list(device_mdr_keys)[:5]}")

    if not device_mdr_keys:
        print("  WARNING: No SCS devices found!")
        return

    # Check master file for matching MDR keys
    master_files = sorted(data_dir.glob("mdrfoi*.txt"))
    if master_files:
        master_file = master_files[0]
        print(f"\nChecking master file: {master_file.name}")

        matching_count = 0
        total_checked = 0
        sample_keys = list(device_mdr_keys)[:1000]  # Check first 1000 keys

        for record in parser.parse_file_dynamic(
            master_file,
            file_type="master",
            map_to_db_columns=True,
            limit=100000,  # Check first 100K records
        ):
            total_checked += 1
            mdr_key = record.get("mdr_report_key")
            if mdr_key in device_mdr_keys:
                matching_count += 1
                if matching_count <= 3:
                    print(f"    Found matching MDR key: {mdr_key}")

        print(f"  Checked {total_checked:,} master records")
        print(f"  Matching MDR keys found: {matching_count}")


def main():
    """Run parsing tests."""
    data_dir = config.data.raw_path

    print(f"Data directory: {data_dir}")
    print(f"SCS Product codes: {SCS_PRODUCT_CODES}")

    # Test each file type
    mdr_keys_by_type = {}
    for file_type in ["device", "master", "patient", "text", "problem"]:
        mdr_keys = test_file_parsing(data_dir, file_type)
        if mdr_keys:
            mdr_keys_by_type[file_type] = mdr_keys

    # Test cross-table matching
    test_cross_table_matching(data_dir)

    print("\n" + "="*60)
    print("Parsing test complete!")
    print("="*60)


if __name__ == "__main__":
    main()
