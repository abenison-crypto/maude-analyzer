#!/usr/bin/env python3
"""
Simple reload script that loads data without transaction wrapping.
This is more robust for large files where individual batch failures
shouldn't abort the entire load.
"""

import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection, initialize_database
from src.ingestion.loader import MAUDELoader
from src.ingestion.parser import count_physical_lines

logger = get_logger("simple_reload")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--db", type=Path, default=config.database.path)
    parser.add_argument("--types", nargs="+", default=["device", "master", "patient", "text", "problem"])
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("SIMPLE RELOAD (no transaction wrapping)")
    logger.info("=" * 60)

    # Initialize database
    with get_connection(args.db) as conn:
        initialize_database(conn)

    # File patterns
    patterns = {
        "device": ["foidev*.txt", "device*.txt"],
        "master": ["mdrfoi*.txt"],
        "patient": ["patient*.txt"],
        "text": ["foitext*.txt"],
        "problem": ["foidevproblem*.txt"],
    }

    # Create loader WITHOUT transaction safety
    loader = MAUDELoader(
        db_path=args.db,
        batch_size=args.batch_size,
        enable_transaction_safety=False,  # Key: disable transactions
        enable_validation=True,
    )

    results = {}
    start_time = datetime.now()

    for ftype in args.types:
        files = []
        for pattern in patterns.get(ftype, []):
            files.extend(args.data_dir.glob(pattern))

        if ftype == "device":
            files = [f for f in files if "problem" not in f.name.lower()]
        if ftype == "patient":
            files = [f for f in files if "problem" not in f.name.lower()]

        files = sorted(set(files))
        if not files:
            logger.warning(f"No {ftype} files found")
            continue

        logger.info(f"\n{'='*50}")
        logger.info(f"Loading {len(files)} {ftype.upper()} files")
        logger.info("=" * 50)

        type_results = []
        for filepath in files:
            logger.info(f"\n{filepath.name}...")
            try:
                # Count physical lines
                total, valid, orphans = count_physical_lines(filepath)
                logger.info(f"  Physical: {valid:,} valid lines, {orphans} orphans")

                # Load
                result = loader.load_file(filepath, ftype)
                logger.info(f"  Loaded: {result.records_loaded:,}, Skipped: {result.records_skipped:,}, Errors: {result.records_errors}")

                type_results.append({
                    'file': filepath.name,
                    'expected': valid,
                    'loaded': result.records_loaded,
                    'errors': result.records_errors,
                })
            except Exception as e:
                logger.error(f"  FAILED: {e}")
                type_results.append({
                    'file': filepath.name,
                    'expected': 0,
                    'loaded': 0,
                    'errors': 1,
                    'error_msg': str(e),
                })

        results[ftype] = type_results

    # Populate master from devices
    if "device" in args.types and "master" in args.types:
        logger.info("\nPopulating master_events with manufacturer data...")
        try:
            with get_connection(args.db) as conn:
                loader.populate_master_from_devices(conn)
        except Exception as e:
            logger.error(f"Failed: {e}")

    # Summary
    duration = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Duration: {duration/60:.1f} minutes")

    for ftype, type_results in results.items():
        expected = sum(r['expected'] for r in type_results)
        loaded = sum(r['loaded'] for r in type_results)
        errors = sum(r['errors'] for r in type_results)
        variance = ((expected - loaded) / expected * 100) if expected > 0 else 0
        print(f"\n{ftype.upper()}: {len(type_results)} files")
        print(f"  Expected: {expected:,}, Loaded: {loaded:,}, Errors: {errors}")
        print(f"  Variance: {variance:.1f}%")

    # DB counts
    print("\n" + "-" * 40)
    print("DATABASE COUNTS:")
    with get_connection(args.db, read_only=True) as conn:
        for table in ["master_events", "devices", "patients", "mdr_text", "device_problems"]:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,}")


if __name__ == "__main__":
    main()
