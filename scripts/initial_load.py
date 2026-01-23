#!/usr/bin/env python
"""
Initial data load script for MAUDE Analyzer.

This script performs the one-time full data load:
1. Downloads FDA MAUDE files (if not already present)
2. Migrates database schema to v2 if needed
3. Parses and transforms the data with dynamic schema detection
4. Loads into DuckDB database
5. Validates data quality
6. Builds indexes and aggregates

Usage:
    python scripts/initial_load.py [options]

Options:
    --skip-download     Skip download step (use existing files)
    --skip-validation   Skip validation step
    --sample            Only load sample data for testing
    --all-products      Load all products (not just SCS codes)
    --year YEAR         Only load specific year
    --type TYPE         Only load specific file type
    --data-dir PATH     Custom data directory
    --db PATH           Custom database path
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection, initialize_database, get_table_counts
from src.database.schema import get_schema_version, SCHEMA_VERSION
from src.ingestion import (
    MAUDEDownloader,
    MAUDELoader,
    DataValidator,
    load_lookup_tables,
    print_validation_report,
)
from src.ingestion.download import download_sample_data


def check_and_migrate_schema(db_path: Path, logger) -> None:
    """
    Check and migrate database schema if needed.

    Args:
        db_path: Path to database.
        logger: Logger instance.
    """
    if not db_path.exists():
        logger.info("Database does not exist, will create new schema.")
        return

    try:
        with get_connection(db_path) as conn:
            current_version = get_schema_version(conn)
            if current_version and current_version != SCHEMA_VERSION:
                logger.info(f"Schema migration needed: {current_version} -> {SCHEMA_VERSION}")
                logger.info("Running migration...")

                # Import and run migration
                from scripts.migrate_schema_v2 import generate_alter_statements

                statements = generate_alter_statements(conn)
                for stmt in statements:
                    try:
                        conn.execute(stmt)
                    except Exception as e:
                        # Ignore "already exists" errors
                        if "already exists" not in str(e).lower():
                            logger.debug(f"Migration statement skipped: {e}")

                # Update version
                conn.execute(
                    "INSERT OR REPLACE INTO app_settings (key, value) VALUES ('schema_version', ?)",
                    [SCHEMA_VERSION]
                )
                logger.info("Migration complete.")
            elif current_version:
                logger.info(f"Database schema is current (v{current_version})")
            else:
                logger.info("No schema version found, will initialize fresh schema.")
    except Exception as e:
        logger.warning(f"Could not check schema version: {e}")


def main():
    """Main entry point for initial data load."""
    parser = argparse.ArgumentParser(
        description="Load FDA MAUDE data into DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing files)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation step",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only load sample data for testing",
    )
    parser.add_argument(
        "--filter-codes",
        type=str,
        default=None,
        help="Comma-separated product codes to filter (e.g., 'GZB,LGW,PMP'). Default: load all",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Only load specific year",
    )
    parser.add_argument(
        "--type",
        choices=["master", "device", "patient", "text", "problem"],
        help="Only load specific file type",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Custom data directory",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Custom database path",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Force re-download even if files exist",
    )
    parser.add_argument(
        "--include-asr",
        action="store_true",
        help="Include ASR (Alternative Summary Reports) data (1999-2019)",
    )
    parser.add_argument(
        "--include-den",
        action="store_true",
        help="Include DEN (Device Experience Network) legacy data (1984-1997)",
    )
    parser.add_argument(
        "--include-all",
        action="store_true",
        help="Include all data types including ASR and DEN",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)
    logger = get_logger("initial_load")

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("MAUDE Analyzer - Initial Data Load")
    logger.info(f"Started at: {start_time}")
    logger.info("=" * 60)

    # Parse filter codes
    filter_codes = None
    if args.filter_codes:
        filter_codes = [c.strip() for c in args.filter_codes.split(",")]

    # Configuration summary
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Database path: {args.db}")
    logger.info(f"Product codes: {filter_codes if filter_codes else 'All'}")
    logger.info(f"Sample mode: {args.sample}")

    # Ensure directories exist
    args.data_dir.mkdir(parents=True, exist_ok=True)
    args.db.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Step 1: Download files
        if not args.skip_download:
            logger.info("\n" + "-" * 40)
            logger.info("STEP 1: Downloading FDA MAUDE files")
            logger.info("-" * 40)

            downloader = MAUDEDownloader(output_dir=args.data_dir)

            if args.sample:
                # Download only sample data
                logger.info("Downloading sample data (current year only)...")
                results = download_sample_data(args.data_dir)
                for ftype, res_list in results.items():
                    success = sum(1 for r in res_list if r.success)
                    logger.info(f"  {ftype}: {success}/{len(res_list)} files downloaded")
            else:
                # Check for missing files
                missing = downloader.check_for_updates()

                if missing or args.force_download:
                    total_missing = sum(len(v) for v in missing.values()) if missing else 0
                    logger.info(f"Found {total_missing} missing files")

                    # Filter by year if specified
                    years = [args.year] if args.year else None

                    # Filter by type if specified
                    file_types = [args.type] if args.type else list(missing.keys()) if missing else None

                    results = downloader.download_all(
                        file_types=file_types,
                        years=years,
                        force=args.force_download
                    )

                    for ftype, res_list in results.items():
                        success = sum(1 for r in res_list if r.success)
                        total_size = sum(r.size_bytes for r in res_list if r.success)
                        logger.info(
                            f"  {ftype}: {success}/{len(res_list)} files, "
                            f"{total_size / 1024 / 1024:.1f} MB"
                        )
                else:
                    logger.info("All files already downloaded")
        else:
            logger.info("\n[Skipping download - using existing files]")

        # Step 2: Check and migrate schema
        logger.info("\n" + "-" * 40)
        logger.info("STEP 2: Initializing/migrating database")
        logger.info("-" * 40)

        check_and_migrate_schema(args.db, logger)

        with get_connection(args.db) as conn:
            initialize_database(conn)
            logger.info("Database schema initialized")

            # Load lookup tables
            load_lookup_tables(conn, config.data.lookups_path)
            logger.info("Lookup tables loaded")

        # Step 3: Load data
        logger.info("\n" + "-" * 40)
        logger.info("STEP 3: Loading data into database")
        logger.info("-" * 40)

        loader = MAUDELoader(
            db_path=args.db,
            filter_product_codes=filter_codes,
        )

        # Check if there are files to load
        existing_files = list(args.data_dir.glob("*.txt"))
        if not existing_files:
            logger.warning(f"No .txt files found in {args.data_dir}")
            logger.warning("Please download FDA MAUDE files first.")
            logger.warning("Run: python scripts/initial_load.py (without --skip-download)")
            return 1

        # Determine which file types to load
        # IMPORTANT: Device must be first when using product code filtering
        # because only device files have PRODUCT_CODE field
        if args.type:
            file_types = [args.type]
        else:
            # Base file types (always loaded)
            file_types = ["device", "master", "patient", "text", "problem", "patient_problem"]

            # Add optional file types
            if args.include_all or args.include_asr:
                file_types.extend(["asr", "asr_ppc"])
            if args.include_all or args.include_den:
                file_types.extend(["den", "disclaimer"])

        # Load files
        results = loader.load_all_files(args.data_dir, file_types)

        # Print summary
        logger.info("\nLoad Summary:")
        total_loaded = 0
        total_errors = 0
        for file_type, res_list in results.items():
            type_loaded = sum(r.records_loaded for r in res_list)
            type_errors = sum(r.records_errors for r in res_list)
            type_skipped = sum(r.records_skipped for r in res_list)
            total_loaded += type_loaded
            total_errors += type_errors

            # Log schema info for first file
            if res_list and res_list[0].schema_info:
                schema = res_list[0].schema_info
                logger.info(
                    f"  {file_type}: {type_loaded:,} loaded, "
                    f"{type_skipped:,} skipped, {type_errors:,} errors "
                    f"(schema: {schema.column_count} cols)"
                )
            else:
                logger.info(
                    f"  {file_type}: {type_loaded:,} loaded, "
                    f"{type_skipped:,} skipped, {type_errors:,} errors"
                )

        logger.info(f"\nTotal: {total_loaded:,} records loaded, {total_errors:,} errors")

        # Step 3b: Populate master_events from devices
        # The FDA stores manufacturer/product_code in device files, not master files
        logger.info("\n" + "-" * 40)
        logger.info("STEP 3b: Populating master_events from devices")
        logger.info("-" * 40)

        with get_connection(args.db) as conn:
            mfr_added, product_added = loader.populate_master_from_devices(conn)
            logger.info(f"  Manufacturer records added: {mfr_added:,}")
            logger.info(f"  Product code records added: {product_added:,}")

        # Step 3c: Populate lookup tables
        logger.info("\n" + "-" * 40)
        logger.info("STEP 3c: Populating lookup tables")
        logger.info("-" * 40)

        try:
            from scripts.populate_lookup_tables import (
                populate_problem_codes,
                populate_patient_problem_codes,
                populate_product_codes_from_data,
                populate_manufacturers_from_data,
            )

            with get_connection(args.db) as conn:
                problem_count = populate_problem_codes(conn, args.data_dir, logger)
                logger.info(f"  Problem codes: {problem_count:,}")

                patient_problem_count = populate_patient_problem_codes(conn, args.data_dir, logger)
                logger.info(f"  Patient problem codes: {patient_problem_count:,}")

                product_count = populate_product_codes_from_data(conn, logger)
                logger.info(f"  Product codes: {product_count:,}")

                mfr_count = populate_manufacturers_from_data(conn, logger)
                logger.info(f"  Manufacturers: {mfr_count:,}")
        except ImportError:
            logger.warning("Could not import lookup population functions")

        # Step 4: Validate data
        if not args.skip_validation:
            logger.info("\n" + "-" * 40)
            logger.info("STEP 4: Validating data quality")
            logger.info("-" * 40)

            validator = DataValidator(db_path=args.db)
            report = validator.run_all_checks()
            print_validation_report(report)

            if not report.passed:
                logger.warning("Some validation checks failed")
        else:
            logger.info("\n[Skipping validation]")

        # Step 5: Final summary
        logger.info("\n" + "-" * 40)
        logger.info("STEP 5: Final Summary")
        logger.info("-" * 40)

        with get_connection(args.db) as conn:
            counts = get_table_counts(conn)
            logger.info("Table row counts:")
            for table, count in counts.items():
                logger.info(f"  {table}: {count:,}")

            # Schema version
            version = get_schema_version(conn)
            logger.info(f"\nSchema version: {version}")

            # Database file size
            db_size = args.db.stat().st_size if args.db.exists() else 0
            logger.info(f"Database size: {db_size / 1024 / 1024:.1f} MB")

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"\nCompleted at: {end_time}")
        logger.info(f"Total duration: {duration}")

        logger.info("\n" + "=" * 60)
        logger.info("Initial load complete!")
        logger.info("=" * 60)
        logger.info("\nNext steps:")
        logger.info("  1. Run: streamlit run app/main.py")
        logger.info("  2. Open: http://localhost:8501")

        return 0

    except KeyboardInterrupt:
        logger.warning("\nLoad interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\nError during load: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
