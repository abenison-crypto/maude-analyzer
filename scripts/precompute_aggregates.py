#!/usr/bin/env python3
"""Pre-compute aggregate statistics for faster dashboard queries.

This script calculates and stores pre-aggregated data for common queries,
reducing response times for dashboard views.

Usage:
    python scripts/precompute_aggregates.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.database import get_db


def create_aggregate_tables(db):
    """Create aggregate tables if they don't exist."""
    print("Creating aggregate tables...")

    # Daily event counts by type
    db.execute("""
        CREATE TABLE IF NOT EXISTS agg_daily_events (
            date DATE PRIMARY KEY,
            total INTEGER,
            deaths INTEGER,
            injuries INTEGER,
            malfunctions INTEGER,
            other INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Monthly event counts by manufacturer
    db.execute("""
        CREATE TABLE IF NOT EXISTS agg_monthly_manufacturer (
            month DATE,
            manufacturer VARCHAR,
            total INTEGER,
            deaths INTEGER,
            injuries INTEGER,
            malfunctions INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (month, manufacturer)
        )
    """)

    # Monthly event counts by product code
    db.execute("""
        CREATE TABLE IF NOT EXISTS agg_monthly_product_code (
            month DATE,
            product_code VARCHAR,
            total INTEGER,
            deaths INTEGER,
            injuries INTEGER,
            malfunctions INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (month, product_code)
        )
    """)

    # Yearly summary statistics
    db.execute("""
        CREATE TABLE IF NOT EXISTS agg_yearly_summary (
            year INTEGER PRIMARY KEY,
            total INTEGER,
            deaths INTEGER,
            injuries INTEGER,
            malfunctions INTEGER,
            unique_manufacturers INTEGER,
            unique_products INTEGER,
            computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    print("Aggregate tables created/verified.")


def compute_daily_aggregates(db):
    """Compute daily event aggregates."""
    print("Computing daily aggregates...")

    # Clear and recompute
    db.execute("DELETE FROM agg_daily_events")

    db.execute("""
        INSERT INTO agg_daily_events (date, total, deaths, injuries, malfunctions, other)
        SELECT
            date_received as date,
            COUNT(*) as total,
            COUNT(CASE WHEN event_type = 'D' THEN 1 END) as deaths,
            COUNT(CASE WHEN event_type = 'IN' THEN 1 END) as injuries,
            COUNT(CASE WHEN event_type = 'M' THEN 1 END) as malfunctions,
            COUNT(CASE WHEN event_type = 'O' THEN 1 END) as other
        FROM master_events
        WHERE date_received IS NOT NULL
        GROUP BY date_received
        ORDER BY date_received
    """)

    count = db.fetch_one("SELECT COUNT(*) FROM agg_daily_events")[0]
    print(f"  Computed {count:,} daily aggregates")


def compute_monthly_manufacturer_aggregates(db):
    """Compute monthly manufacturer aggregates."""
    print("Computing monthly manufacturer aggregates...")

    # Clear and recompute
    db.execute("DELETE FROM agg_monthly_manufacturer")

    db.execute("""
        INSERT INTO agg_monthly_manufacturer (month, manufacturer, total, deaths, injuries, malfunctions)
        SELECT
            DATE_TRUNC('month', date_received) as month,
            manufacturer_clean as manufacturer,
            COUNT(*) as total,
            COUNT(CASE WHEN event_type = 'D' THEN 1 END) as deaths,
            COUNT(CASE WHEN event_type = 'IN' THEN 1 END) as injuries,
            COUNT(CASE WHEN event_type = 'M' THEN 1 END) as malfunctions
        FROM master_events
        WHERE date_received IS NOT NULL
        AND manufacturer_clean IS NOT NULL
        GROUP BY DATE_TRUNC('month', date_received), manufacturer_clean
    """)

    count = db.fetch_one("SELECT COUNT(*) FROM agg_monthly_manufacturer")[0]
    print(f"  Computed {count:,} monthly manufacturer aggregates")


def compute_monthly_product_code_aggregates(db):
    """Compute monthly product code aggregates."""
    print("Computing monthly product code aggregates...")

    # Clear and recompute
    db.execute("DELETE FROM agg_monthly_product_code")

    db.execute("""
        INSERT INTO agg_monthly_product_code (month, product_code, total, deaths, injuries, malfunctions)
        SELECT
            DATE_TRUNC('month', date_received) as month,
            product_code,
            COUNT(*) as total,
            COUNT(CASE WHEN event_type = 'D' THEN 1 END) as deaths,
            COUNT(CASE WHEN event_type = 'IN' THEN 1 END) as injuries,
            COUNT(CASE WHEN event_type = 'M' THEN 1 END) as malfunctions
        FROM master_events
        WHERE date_received IS NOT NULL
        AND product_code IS NOT NULL
        GROUP BY DATE_TRUNC('month', date_received), product_code
    """)

    count = db.fetch_one("SELECT COUNT(*) FROM agg_monthly_product_code")[0]
    print(f"  Computed {count:,} monthly product code aggregates")


def compute_yearly_summary(db):
    """Compute yearly summary statistics."""
    print("Computing yearly summaries...")

    # Clear and recompute
    db.execute("DELETE FROM agg_yearly_summary")

    db.execute("""
        INSERT INTO agg_yearly_summary (year, total, deaths, injuries, malfunctions, unique_manufacturers, unique_products)
        SELECT
            EXTRACT(YEAR FROM date_received)::INTEGER as year,
            COUNT(*) as total,
            COUNT(CASE WHEN event_type = 'D' THEN 1 END) as deaths,
            COUNT(CASE WHEN event_type = 'IN' THEN 1 END) as injuries,
            COUNT(CASE WHEN event_type = 'M' THEN 1 END) as malfunctions,
            COUNT(DISTINCT manufacturer_clean) as unique_manufacturers,
            COUNT(DISTINCT product_code) as unique_products
        FROM master_events
        WHERE date_received IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM date_received)
        ORDER BY year
    """)

    count = db.fetch_one("SELECT COUNT(*) FROM agg_yearly_summary")[0]
    print(f"  Computed {count:,} yearly summaries")


def main():
    """Run all aggregate computations."""
    print(f"Starting aggregate computation at {datetime.now().isoformat()}")
    print("=" * 60)

    db = get_db()

    try:
        create_aggregate_tables(db)
        compute_daily_aggregates(db)
        compute_monthly_manufacturer_aggregates(db)
        compute_monthly_product_code_aggregates(db)
        compute_yearly_summary(db)

        print("=" * 60)
        print(f"Aggregate computation completed at {datetime.now().isoformat()}")

    except Exception as e:
        print(f"Error during computation: {e}")
        raise


if __name__ == "__main__":
    main()
