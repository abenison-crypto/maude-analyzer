#!/usr/bin/env python3
"""
Diagnostic script to understand the transaction abort issue in MAUDELoader.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
import pandas as pd
from datetime import datetime

from config import config
from src.database import initialize_database
from src.ingestion.parser import MAUDEParser

def test_direct_insert(db_path: Path):
    """Test direct insert without MAUDELoader."""
    print("\n=== Test 1: Direct Insert ===")
    conn = duckdb.connect(str(db_path))

    try:
        conn.execute("BEGIN TRANSACTION")
        print("Transaction started")

        # Create test DataFrame
        df = pd.DataFrame([{
            "mdr_report_key": "99999999",
            "event_key": "TEST",
            "report_number": "TEST-001",
            "source_file": "test.txt",
        }])

        # Simple insert
        conn.execute("""
            INSERT OR REPLACE INTO master_events (mdr_report_key, event_key, report_number, source_file)
            SELECT "mdr_report_key", "event_key", "report_number", "source_file" FROM df
        """)
        print("Insert successful")

        conn.execute("COMMIT")
        print("Commit successful")

        # Verify
        count = conn.execute("SELECT COUNT(*) FROM master_events WHERE mdr_report_key = '99999999'").fetchone()[0]
        print(f"Verified: {count} record(s) found")

        # Cleanup
        conn.execute("DELETE FROM master_events WHERE mdr_report_key = '99999999'")
        print("Test record cleaned up")

    except Exception as e:
        print(f"ERROR: {e}")
        try:
            conn.execute("ROLLBACK")
        except:
            pass
    finally:
        conn.close()


def test_file_audit_insert(db_path: Path):
    """Test file_audit insert with CURRENT_TIMESTAMP."""
    print("\n=== Test 2: File Audit Insert ===")
    conn = duckdb.connect(str(db_path))

    try:
        # Check if file_audit table exists
        try:
            conn.execute("SELECT 1 FROM file_audit LIMIT 1")
            print("file_audit table exists")
        except Exception as e:
            print(f"file_audit table check failed: {e}")
            return

        # Try the exact SQL from loader.py
        conn.execute("BEGIN TRANSACTION")
        print("Transaction started")

        conn.execute("""
            INSERT INTO file_audit (
                filename, file_type, source_record_count, loaded_record_count,
                skipped_record_count, error_record_count, column_mismatch_count,
                load_status, schema_version, detected_column_count,
                load_started, load_completed, error_message, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (filename) DO UPDATE SET
                source_record_count = EXCLUDED.source_record_count,
                loaded_record_count = EXCLUDED.loaded_record_count,
                updated_at = CURRENT_TIMESTAMP
        """, [
            "test_diagnostic.txt",
            "master",
            1000,
            1000,
            0,
            0,
            0,
            "COMPLETED",
            "v1",
            30,
            datetime.now(),
            datetime.now(),
            None,
        ])
        print("File audit insert successful")

        conn.execute("COMMIT")
        print("Commit successful")

        # Cleanup
        conn.execute("DELETE FROM file_audit WHERE filename = 'test_diagnostic.txt'")
        print("Test record cleaned up")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.execute("ROLLBACK")
        except:
            pass
    finally:
        conn.close()


def test_parse_and_insert(db_path: Path, data_dir: Path):
    """Test parsing a master file and inserting records."""
    print("\n=== Test 3: Parse and Insert Master File ===")

    # Find a master file
    master_files = list(data_dir.glob("mdrfoi*.txt"))
    if not master_files:
        print("No master files found")
        return

    master_file = master_files[0]
    print(f"Using file: {master_file.name}")

    parser = MAUDEParser()
    conn = duckdb.connect(str(db_path))

    try:
        # Detect schema
        schema = parser.detect_schema_from_header(master_file, "master")
        print(f"Schema detected: {schema.column_count} columns")

        conn.execute("BEGIN TRANSACTION")
        print("Transaction started")

        # Parse just 100 records
        records_gen = parser.parse_file_dynamic(
            master_file,
            schema=schema,
            file_type="master",
            map_to_db_columns=True,
        )

        batch = []
        count = 0
        for record in records_gen:
            mdr_key = record.get("mdr_report_key", "")
            if not mdr_key or not str(mdr_key).isdigit():
                continue
            batch.append(record)
            count += 1
            if count >= 100:
                break

        print(f"Parsed {len(batch)} valid records")

        if batch:
            # Show first record columns
            first_record = batch[0]
            print(f"First record keys (first 10): {list(first_record.keys())[:10]}")

            # Create DataFrame with specific columns for master
            from src.ingestion.loader import MASTER_INSERT_COLUMNS

            rows = []
            for record in batch:
                row = {col: record.get(col) for col in MASTER_INSERT_COLUMNS}
                rows.append(row)

            df = pd.DataFrame(rows, columns=MASTER_INSERT_COLUMNS)
            print(f"DataFrame shape: {df.shape}")
            print(f"DataFrame columns (first 10): {list(df.columns[:10])}")

            # Try insert
            col_names = ", ".join(MASTER_INSERT_COLUMNS)
            select_cols = ", ".join([f'"{c}"' for c in MASTER_INSERT_COLUMNS])

            conn.execute(f"""
                INSERT OR REPLACE INTO master_events ({col_names})
                SELECT {select_cols} FROM df
            """)
            print(f"Inserted {len(df)} records")

        conn.execute("COMMIT")
        print("Commit successful")

        # Verify
        total = conn.execute("SELECT COUNT(*) FROM master_events").fetchone()[0]
        print(f"Total records in master_events: {total}")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.execute("ROLLBACK")
        except:
            pass
    finally:
        conn.close()


def test_full_loader(db_path: Path, data_dir: Path):
    """Test the full MAUDELoader with a small batch."""
    print("\n=== Test 4: Full MAUDELoader ===")

    from src.ingestion.loader import MAUDELoader

    # Find a master file
    master_files = list(data_dir.glob("mdrfoi.txt"))
    if not master_files:
        master_files = list(data_dir.glob("mdrfoi*.txt"))
    if not master_files:
        print("No master files found")
        return

    master_file = master_files[0]
    print(f"Using file: {master_file.name}")

    # Create loader with small batch size
    loader = MAUDELoader(
        db_path=db_path,
        batch_size=100,  # Very small for testing
        enable_transaction_safety=True,
        enable_validation=True,
    )

    try:
        result = loader.load_file(master_file, "master")
        print(f"Load result:")
        print(f"  Loaded: {result.records_loaded}")
        print(f"  Skipped: {result.records_skipped}")
        print(f"  Errors: {result.records_errors}")
        print(f"  Transaction committed: {result.transaction_committed}")
        if result.error_messages:
            print(f"  First error: {result.error_messages[0]}")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=config.database.path)
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--test", type=int, choices=[1, 2, 3, 4], help="Run specific test only")
    args = parser.parse_args()

    print(f"Database: {args.db}")
    print(f"Data dir: {args.data_dir}")

    # Ensure database exists
    conn = duckdb.connect(str(args.db))
    initialize_database(conn)
    conn.close()
    print("Database initialized")

    if args.test:
        if args.test == 1:
            test_direct_insert(args.db)
        elif args.test == 2:
            test_file_audit_insert(args.db)
        elif args.test == 3:
            test_parse_and_insert(args.db, args.data_dir)
        elif args.test == 4:
            test_full_loader(args.db, args.data_dir)
    else:
        test_direct_insert(args.db)
        test_file_audit_insert(args.db)
        test_parse_and_insert(args.db, args.data_dir)
        test_full_loader(args.db, args.data_dir)


if __name__ == "__main__":
    main()
