#!/usr/bin/env python
"""
Initial data load script for MAUDE Analyzer.

This script performs the one-time full data load:
1. Downloads FDA MAUDE files (if not already present)
2. Parses and transforms the data
3. Loads into DuckDB database
4. Validates data quality

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

from config import config, SCS_PRODUCT_CODES
from config.logging_config import setup_logging, get_logger
from src.database import get_connection, initialize_database, get_table_counts
from src.ingestion import (
    MAUDEDownloader,
    MAUDELoader,
    DataValidator,
    load_lookup_tables,
    print_validation_report,
)


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
        "--all-products",
        action="store_true",
        help="Load all products (not just SCS codes)",
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

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)
    logger = get_logger("initial_load")

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("MAUDE Analyzer - Initial Data Load")
    logger.info(f"Started at: {start_time}")
    logger.info("=" * 60)

    # Configuration summary
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Database path: {args.db}")
    logger.info(f"Product codes: {'All' if args.all_products else SCS_PRODUCT_CODES}")
    logger.info(f"Sample mode: {args.sample}")

    try:
        # Step 1: Download files
        if not args.skip_download:
            logger.info("\n" + "-" * 40)
            logger.info("STEP 1: Downloading FDA MAUDE files")
            logger.info("-" * 40)

            downloader = MAUDEDownloader(output_dir=args.data_dir)

            if args.sample:
                # Download only sample data
                logger.info("Downloading sample data (current year changes only)...")
                from src.ingestion.download import download_sample_data
                results = download_sample_data(args.data_dir)
            else:
                # Check for missing files
                missing = downloader.check_for_updates()
                if missing:
                    logger.info(f"Found {sum(len(v) for v in missing.values())} missing files")

                    # Filter by year if specified
                    years = [args.year] if args.year else None

                    # Filter by type if specified
                    file_types = [args.type] if args.type else list(missing.keys())

                    results = downloader.download_all(file_types=file_types, years=years)

                    for ftype, res_list in results.items():
                        success = sum(1 for r in res_list if r.success)
                        logger.info(f"  {ftype}: {success}/{len(res_list)} files downloaded")
                else:
                    logger.info("All files already downloaded")
        else:
            logger.info("\n[Skipping download - using existing files]")

        # Step 2: Initialize database
        logger.info("\n" + "-" * 40)
        logger.info("STEP 2: Initializing database")
        logger.info("-" * 40)

        with get_connection(args.db) as conn:
            initialize_database(conn)
            logger.info("Database schema created")

            # Load lookup tables
            load_lookup_tables(conn, config.data.lookups_path)
            logger.info("Lookup tables loaded")

        # Step 3: Load data
        logger.info("\n" + "-" * 40)
        logger.info("STEP 3: Loading data into database")
        logger.info("-" * 40)

        filter_codes = None if args.all_products else SCS_PRODUCT_CODES
        loader = MAUDELoader(
            db_path=args.db,
            filter_product_codes=filter_codes,
        )

        # Check if there are files to load
        existing_files = list(args.data_dir.glob("*.txt"))
        if not existing_files:
            logger.warning(f"No .txt files found in {args.data_dir}")
            logger.warning("Please download FDA MAUDE files first.")
            logger.warning("See: data/raw/README.md for instructions")
            return 1

        # Determine which file types to load
        file_types = [args.type] if args.type else ["master", "device", "patient", "text", "problem"]

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
            logger.info(
                f"  {file_type}: {type_loaded:,} loaded, "
                f"{type_skipped:,} skipped, {type_errors:,} errors"
            )

        logger.info(f"\nTotal: {total_loaded:,} records loaded, {total_errors:,} errors")

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

            # Database file size
            db_size = args.db.stat().st_size if args.db.exists() else 0
            logger.info(f"\nDatabase size: {db_size / 1024 / 1024:.1f} MB")

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
