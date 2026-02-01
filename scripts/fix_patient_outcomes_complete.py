#!/usr/bin/env python3
"""
Complete fix for patient outcome data.

The issue: sequence_number_outcome was defined as INTEGER but FDA data contains
string codes like 'R', 'O', 'D', 'H', etc. All values became NULL during loading.

This script:
1. Alters sequence_number_outcome to VARCHAR
2. Reloads patient data from raw FDA files
3. Parses outcome codes into boolean fields

Usage:
    python scripts/fix_patient_outcomes_complete.py
"""

import csv
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import duckdb

# Outcome code mapping (from FDA documentation)
OUTCOME_CODES = {
    'D': 'death',
    'L': 'life_threatening',
    'H': 'hospitalization',
    'DS': 'disability',
    'CA': 'congenital_anomaly',
    'RI': 'required_intervention',
    'RI': 'required_intervention',
    'R': 'required_intervention',  # R is alias for RI
    'O': 'other',
    'OT': 'other',
}


def parse_age(age_str: str):
    """Parse patient age string into numeric value and unit."""
    if not age_str or not age_str.strip():
        return None, None

    age_str = age_str.strip().upper()

    # Common patterns: "65 YR", "6 MO", "3 WK", "2 DA", "65", etc.
    import re
    match = re.match(r'(\d+(?:\.\d+)?)\s*(YR|MO|WK|DA|DY|HR|YEAR|YEARS|MONTH|MONTHS|WEEK|WEEKS|DAY|DAYS)?', age_str)
    if match:
        value = float(match.group(1))
        unit = match.group(2) or 'YR'

        # Normalize units
        unit_map = {
            'YR': 'years', 'YEAR': 'years', 'YEARS': 'years',
            'MO': 'months', 'MONTH': 'months', 'MONTHS': 'months',
            'WK': 'weeks', 'WEEK': 'weeks', 'WEEKS': 'weeks',
            'DA': 'days', 'DY': 'days', 'DAY': 'days', 'DAYS': 'days',
            'HR': 'hours',
        }
        unit_normalized = unit_map.get(unit, 'years')

        # Convert to years for numeric field
        if unit_normalized == 'months':
            value = value / 12
        elif unit_normalized == 'weeks':
            value = value / 52
        elif unit_normalized == 'days':
            value = value / 365
        elif unit_normalized == 'hours':
            value = value / 8760

        return value, unit_normalized

    return None, None


def parse_date(date_str: str):
    """Parse date in MM/DD/YYYY format."""
    if not date_str or not date_str.strip():
        return None
    try:
        return datetime.strptime(date_str.strip(), '%m/%d/%Y').date()
    except:
        return None


def parse_int(val: str):
    """Parse integer, return None if empty or invalid."""
    if not val or not val.strip():
        return None
    try:
        return int(val.strip())
    except:
        return None


def main():
    db_path = PROJECT_ROOT / 'data' / 'maude.duckdb'
    data_dir = PROJECT_ROOT / 'data' / 'raw'

    print("=" * 70)
    print("  COMPLETE PATIENT OUTCOMES FIX")
    print("=" * 70)
    print(f"\nDatabase: {db_path}")
    print(f"Data directory: {data_dir}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    conn = duckdb.connect(str(db_path))

    # Step 1: Check current state
    print("\n" + "=" * 70)
    print("  STEP 1: Analyzing Current State")
    print("=" * 70)

    result = conn.execute("SELECT COUNT(*) FROM patients").fetchone()
    current_count = result[0]
    print(f"\nCurrent patient records: {current_count:,}")

    # Check column type
    cols = conn.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'patients' AND column_name = 'sequence_number_outcome'
    """).fetchone()
    print(f"sequence_number_outcome type: {cols[1]}")

    # Step 2: Alter column type if needed
    print("\n" + "=" * 70)
    print("  STEP 2: Schema Migration")
    print("=" * 70)

    # Check both columns
    treatment_col = conn.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'patients' AND column_name = 'sequence_number_treatment'
    """).fetchone()

    if cols[1] == 'INTEGER' or (treatment_col and treatment_col[1] == 'INTEGER'):
        print("\nAltering sequence_number columns from INTEGER to VARCHAR...")
        print(f"  sequence_number_outcome: {cols[1]}")
        print(f"  sequence_number_treatment: {treatment_col[1] if treatment_col else 'N/A'}")

        # DuckDB doesn't support ALTER COLUMN TYPE directly
        # Need to recreate the table
        print("  Creating new patients table with VARCHAR types...")

        conn.execute("""
            CREATE TABLE patients_new AS
            SELECT
                id,
                mdr_report_key,
                patient_sequence_number,
                date_received,
                CAST(sequence_number_treatment AS VARCHAR) as sequence_number_treatment,
                CAST(sequence_number_outcome AS VARCHAR) as sequence_number_outcome,
                patient_age,
                patient_sex,
                patient_weight,
                patient_ethnicity,
                patient_race,
                patient_age_numeric,
                patient_age_unit,
                outcome_codes_raw,
                treatment_codes_raw,
                outcome_death,
                outcome_life_threatening,
                outcome_hospitalization,
                outcome_disability,
                outcome_congenital_anomaly,
                outcome_required_intervention,
                outcome_other,
                created_at,
                source_file,
                treatment_drug,
                treatment_device,
                treatment_surgery,
                treatment_other,
                treatment_unknown,
                treatment_no_information,
                treatment_blood_products,
                treatment_hospitalization,
                treatment_physical_therapy,
                updated_at
            FROM patients
        """)

        print("  Dropping old table...")
        conn.execute("DROP TABLE patients")

        print("  Renaming new table...")
        conn.execute("ALTER TABLE patients_new RENAME TO patients")

        print("  Recreating indexes...")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_patients_mdr_key ON patients(mdr_report_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_patients_outcomes ON patients(outcome_death, outcome_hospitalization)")

        print("  Schema migration complete!")
    else:
        print(f"Column already VARCHAR, skipping schema migration")

    # Step 3: Delete existing patients and reload
    print("\n" + "=" * 70)
    print("  STEP 3: Reloading Patient Data")
    print("=" * 70)

    # Find patient files (exclude patientproblemcode.txt which has different structure)
    patient_files = sorted([
        f for f in data_dir.glob('patient*.txt')
        if 'problemcode' not in f.name.lower()
    ])
    print(f"\nFound {len(patient_files)} patient files:")
    for f in patient_files:
        print(f"  - {f.name}")

    if not patient_files:
        print("ERROR: No patient files found!")
        return 1

    # Clear existing data
    print("\nDeleting existing patient records...")
    conn.execute("DELETE FROM patients")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='patients'") if False else None

    # Reset sequence
    try:
        conn.execute("SELECT setval('patients_id_seq', 1, false)")
    except:
        pass

    csv.field_size_limit(sys.maxsize)

    total_loaded = 0
    batch_size = 50000

    for filepath in patient_files:
        print(f"\nLoading {filepath.name}...")
        start_time = time.time()
        records = []
        file_loaded = 0

        with open(filepath, 'r', encoding='latin-1') as f:
            reader = csv.reader(f, delimiter='|')
            header = [h.upper().strip() for h in next(reader)]

            print(f"  Columns: {len(header)}")
            print(f"  Header: {header}")

            for row in reader:
                if len(row) < 5:
                    continue

                # Parse fields
                mdr_report_key = row[0].strip() if len(row) > 0 else None
                patient_seq = parse_int(row[1]) if len(row) > 1 else None
                date_received = parse_date(row[2]) if len(row) > 2 else None
                seq_treatment = row[3].strip() if len(row) > 3 and row[3].strip() else None
                seq_outcome = row[4].strip() if len(row) > 4 and row[4].strip() else None
                patient_age = row[5].strip() if len(row) > 5 else None
                patient_sex = row[6].strip() if len(row) > 6 else None
                patient_weight = row[7].strip() if len(row) > 7 else None
                patient_ethnicity = row[8].strip() if len(row) > 8 else None
                patient_race = row[9].strip() if len(row) > 9 else None

                # Parse age
                age_numeric, age_unit = parse_age(patient_age)

                # Parse outcome codes into booleans
                outcome_death = False
                outcome_life_threatening = False
                outcome_hospitalization = False
                outcome_disability = False
                outcome_congenital_anomaly = False
                outcome_required_intervention = False
                outcome_other = False

                if seq_outcome:
                    outcome_upper = seq_outcome.upper()
                    # Check for each code
                    if 'D' in outcome_upper and 'DS' not in outcome_upper:
                        outcome_death = True
                    if 'L' in outcome_upper:
                        outcome_life_threatening = True
                    if 'H' in outcome_upper:
                        outcome_hospitalization = True
                    if 'DS' in outcome_upper:
                        outcome_disability = True
                    if 'CA' in outcome_upper:
                        outcome_congenital_anomaly = True
                    if 'RI' in outcome_upper or outcome_upper == 'R':
                        outcome_required_intervention = True
                    if 'O' in outcome_upper or 'OT' in outcome_upper:
                        if outcome_upper not in ('D', 'L', 'H', 'DS', 'CA', 'RI', 'R'):
                            outcome_other = True

                record = (
                    mdr_report_key,
                    patient_seq,
                    date_received,
                    seq_treatment,
                    seq_outcome,  # Now stored as VARCHAR
                    patient_age,
                    patient_sex,
                    patient_weight,
                    patient_ethnicity,
                    patient_race,
                    age_numeric,
                    age_unit,
                    seq_outcome,  # outcome_codes_raw
                    seq_treatment,  # treatment_codes_raw
                    outcome_death,
                    outcome_life_threatening,
                    outcome_hospitalization,
                    outcome_disability,
                    outcome_congenital_anomaly,
                    outcome_required_intervention,
                    outcome_other,
                    filepath.name,
                )
                records.append(record)

                if len(records) >= batch_size:
                    conn.executemany("""
                        INSERT INTO patients (
                            mdr_report_key, patient_sequence_number, date_received,
                            sequence_number_treatment, sequence_number_outcome,
                            patient_age, patient_sex, patient_weight,
                            patient_ethnicity, patient_race,
                            patient_age_numeric, patient_age_unit,
                            outcome_codes_raw, treatment_codes_raw,
                            outcome_death, outcome_life_threatening, outcome_hospitalization,
                            outcome_disability, outcome_congenital_anomaly,
                            outcome_required_intervention, outcome_other,
                            source_file
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, records)
                    file_loaded += len(records)
                    records = []
                    print(f"  Loaded {file_loaded:,} records...", end='\r')

        # Insert remaining records
        if records:
            conn.executemany("""
                INSERT INTO patients (
                    mdr_report_key, patient_sequence_number, date_received,
                    sequence_number_treatment, sequence_number_outcome,
                    patient_age, patient_sex, patient_weight,
                    patient_ethnicity, patient_race,
                    patient_age_numeric, patient_age_unit,
                    outcome_codes_raw, treatment_codes_raw,
                    outcome_death, outcome_life_threatening, outcome_hospitalization,
                    outcome_disability, outcome_congenital_anomaly,
                    outcome_required_intervention, outcome_other,
                    source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            file_loaded += len(records)

        elapsed = time.time() - start_time
        rate = file_loaded / elapsed if elapsed > 0 else 0
        print(f"  Loaded {file_loaded:,} records in {elapsed:.1f}s ({rate:,.0f} rec/s)")
        total_loaded += file_loaded

    conn.execute("CHECKPOINT")

    # Step 4: Verify results
    print("\n" + "=" * 70)
    print("  STEP 4: Verification")
    print("=" * 70)

    result = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(sequence_number_outcome) as has_outcome_code,
            SUM(CASE WHEN outcome_death THEN 1 ELSE 0 END) as death_count,
            SUM(CASE WHEN outcome_life_threatening THEN 1 ELSE 0 END) as life_threatening_count,
            SUM(CASE WHEN outcome_hospitalization THEN 1 ELSE 0 END) as hosp_count,
            SUM(CASE WHEN outcome_disability THEN 1 ELSE 0 END) as disability_count,
            SUM(CASE WHEN outcome_congenital_anomaly THEN 1 ELSE 0 END) as ca_count,
            SUM(CASE WHEN outcome_required_intervention THEN 1 ELSE 0 END) as ri_count,
            SUM(CASE WHEN outcome_other THEN 1 ELSE 0 END) as other_count
        FROM patients
    """).fetchone()

    total = result[0]
    has_code = result[1]

    print(f"\nTotal patient records: {total:,}")
    print(f"With outcome code: {has_code:,} ({100*has_code/total:.1f}%)")
    print(f"\nOutcome breakdown:")
    print(f"  Death: {result[2]:,}")
    print(f"  Life-threatening: {result[3]:,}")
    print(f"  Hospitalization: {result[4]:,}")
    print(f"  Disability: {result[5]:,}")
    print(f"  Congenital Anomaly: {result[6]:,}")
    print(f"  Required Intervention: {result[7]:,}")
    print(f"  Other: {result[8]:,}")

    # Show distribution of outcome codes
    print("\nTop outcome code values:")
    dist = conn.execute("""
        SELECT sequence_number_outcome, COUNT(*) as cnt
        FROM patients
        WHERE sequence_number_outcome IS NOT NULL
        GROUP BY sequence_number_outcome
        ORDER BY cnt DESC
        LIMIT 15
    """).fetchall()
    for code, cnt in dist:
        print(f"  '{code}': {cnt:,}")

    conn.close()

    print("\n" + "=" * 70)
    print("  COMPLETE!")
    print("=" * 70)
    print(f"\nTotal records loaded: {total_loaded:,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
