#!/usr/bin/env python3
"""Find records with dates that would fail CHECK constraints."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from datetime import date, datetime
from src.ingestion.parser import MAUDEParser
from src.ingestion.transformer import DataTransformer

def find_bad_dates(filepath: Path, max_records: int = 50000):
    """Find records with dates outside valid range."""
    parser = MAUDEParser()
    transformer = DataTransformer()

    # Detect schema
    schema = parser.detect_schema_from_header(filepath, "master")
    print(f"Parsing {filepath.name} ({schema.column_count} columns)")

    bad_records = []
    record_count = 0
    date_stats = {
        'total': 0,
        'has_event_date': 0,
        'event_date_parsed': 0,
        'event_year_invalid': 0,
        'has_received_date': 0,
        'received_date_parsed': 0,
        'received_year_invalid': 0,
    }

    # Parse records
    for record in parser.parse_file_dynamic(
        filepath,
        schema=schema,
        file_type="master",
        map_to_db_columns=True,
    ):
        record_count += 1
        date_stats['total'] += 1

        # Check mdr_report_key validity
        mdr_key = record.get("mdr_report_key", "")
        if not mdr_key or not str(mdr_key).isdigit():
            continue

        # Transform the record
        transformed = transformer.transform_master_record(record, filepath.name)

        # Check date_of_event
        raw_date_of_event = record.get("date_of_event")
        if raw_date_of_event:
            date_stats['has_event_date'] += 1

        event_date = transformed.get("date_of_event")
        if isinstance(event_date, date):
            date_stats['event_date_parsed'] += 1
            event_year = transformed.get("event_year")
            if event_year is not None:
                if event_year < 1980 or event_year > 2100:
                    date_stats['event_year_invalid'] += 1
                    if len(bad_records) < 20:
                        bad_records.append({
                            'mdr_report_key': mdr_key,
                            'issue': 'event_year',
                            'raw_date': raw_date_of_event,
                            'parsed_date': str(event_date),
                            'year': event_year,
                        })

        # Check date_received
        raw_date_received = record.get("date_received")
        if raw_date_received:
            date_stats['has_received_date'] += 1

        received_date = transformed.get("date_received")
        if isinstance(received_date, date):
            date_stats['received_date_parsed'] += 1
            received_year = transformed.get("received_year")
            if received_year is not None:
                if received_year < 1980 or received_year > 2100:
                    date_stats['received_year_invalid'] += 1
                    if len(bad_records) < 20:
                        bad_records.append({
                            'mdr_report_key': mdr_key,
                            'issue': 'received_year',
                            'raw_date': raw_date_received,
                            'parsed_date': str(received_date),
                            'year': received_year,
                        })

        if record_count >= max_records:
            break

        if record_count % 10000 == 0:
            print(f"  Processed {record_count:,} records...")

    print(f"\n=== Results for {filepath.name} ===")
    print(f"Total records scanned: {date_stats['total']:,}")
    print(f"\nEvent dates:")
    print(f"  Has event date: {date_stats['has_event_date']:,}")
    print(f"  Event date parsed: {date_stats['event_date_parsed']:,}")
    print(f"  Event year invalid (<1980 or >2100): {date_stats['event_year_invalid']:,}")
    print(f"\nReceived dates:")
    print(f"  Has received date: {date_stats['has_received_date']:,}")
    print(f"  Received date parsed: {date_stats['received_date_parsed']:,}")
    print(f"  Received year invalid (<1980 or >2100): {date_stats['received_year_invalid']:,}")

    if bad_records:
        print(f"\n=== Bad Records (first {len(bad_records)}) ===")
        for r in bad_records:
            print(f"  mdr_report_key={r['mdr_report_key']}: {r['issue']}={r['year']} (raw='{r['raw_date']}', parsed='{r['parsed_date']}')")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--max-records", type=int, default=100000)
    args = parser.parse_args()

    # Find master files
    master_files = sorted(args.data_dir.glob("mdrfoi*.txt"))
    if not master_files:
        print("No master files found")
        return

    for f in master_files[:3]:  # Check first 3 files
        find_bad_dates(f, args.max_records)


if __name__ == "__main__":
    main()
