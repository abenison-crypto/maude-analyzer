#!/usr/bin/env python3
"""Quick test of the loader fixes."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
from src.database import initialize_database
from src.ingestion.loader import MAUDELoader

def test_loader(data_dir: Path, db_path: Path):
    """Test the loader with fixes."""

    # Create fresh database
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    initialize_database(conn)
    conn.close()
    print(f"Created fresh database: {db_path}")

    # Find a master file
    master_files = list(data_dir.glob("mdrfoi.txt"))
    if not master_files:
        master_files = list(data_dir.glob("mdrfoi*.txt"))
    if not master_files:
        print("No master files found")
        return

    master_file = master_files[0]
    print(f"\nTesting with: {master_file.name}")

    # Create loader with small batch size
    loader = MAUDELoader(
        db_path=db_path,
        batch_size=5000,  # Small batch for testing
        enable_transaction_safety=True,
        enable_validation=True,
    )

    print("\nLoading file...")
    try:
        result = loader.load_file(master_file, "master")
        print(f"\n=== Load Result ===")
        print(f"Records loaded: {result.records_loaded:,}")
        print(f"Records skipped: {result.records_skipped:,}")
        print(f"Records errors: {result.records_errors}")
        print(f"Transaction committed: {result.transaction_committed}")

        if result.error_messages:
            print(f"\nFirst few errors:")
            for msg in result.error_messages[:5]:
                print(f"  - {msg}")

        # Verify counts
        conn = duckdb.connect(str(db_path), read_only=True)
        count = conn.execute("SELECT COUNT(*) FROM master_events").fetchone()[0]
        print(f"\nDatabase count: {count:,}")
        conn.close()

        return result.records_loaded > 0

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--db", type=Path, default=Path("/tmp/test_loader_fix.duckdb"))
    args = parser.parse_args()

    success = test_loader(args.data_dir, args.db)
    print(f"\n{'SUCCESS' if success else 'FAILED'}")


if __name__ == "__main__":
    main()
