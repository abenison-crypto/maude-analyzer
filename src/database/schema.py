"""DuckDB schema definitions for MAUDE database.

This schema supports all 135+ FDA MAUDE fields across file types:
- Master Events: 86 FDA columns + derived fields
- Devices: 28 FDA columns + derived fields
- Patients: 10 FDA columns + derived/outcome fields
- MDR Text: 6 FDA columns
- Device Problems: 2 FDA columns
"""

from typing import Optional
import duckdb
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger

logger = get_logger("schema")

# Schema version for migrations
SCHEMA_VERSION = "2.1"  # Added CHECK constraints and documentation

# =============================================================================
# CONSTRAINT NOTES
# =============================================================================
# DuckDB does not enforce foreign key constraints but allows them for
# documentation and query optimization hints.
#
# Key Relationships:
# - devices.mdr_report_key -> master_events.mdr_report_key
# - patients.mdr_report_key -> master_events.mdr_report_key
# - mdr_text.mdr_report_key -> master_events.mdr_report_key
# - device_problems.mdr_report_key -> master_events.mdr_report_key
# - patient_problems.mdr_report_key -> master_events.mdr_report_key
#
# Valid Flag Values (Y/N or specific codes):
# - adverse_event_flag: Y/N
# - product_problem_flag: Y/N
# - health_professional: Y/N
# - single_use_flag: Y/N
# - implant_flag: Y/N
# - date_removed_flag: Y/N
# - event_type: D (Death), IN (Injury), M (Malfunction), O (Other), * (Unknown)
# =============================================================================

# =============================================================================
# MASTER EVENTS TABLE (86 FDA columns + derived fields)
# =============================================================================

CREATE_MASTER_EVENTS = """
CREATE TABLE IF NOT EXISTS master_events (
    -- Primary Key
    mdr_report_key VARCHAR PRIMARY KEY,

    -- Key Identifiers
    event_key VARCHAR,
    report_number VARCHAR,
    report_source_code VARCHAR,
    manufacturer_link_flag_old VARCHAR,

    -- Event Counts
    number_devices_in_event INTEGER,
    number_patients_in_event INTEGER,

    -- Key Dates
    date_received DATE,
    date_report DATE,
    date_of_event DATE,

    -- Event Flags (Y/N)
    adverse_event_flag VARCHAR,
    product_problem_flag VARCHAR,
    reprocessed_and_reused_flag VARCHAR,

    -- Reporter Information
    reporter_occupation_code VARCHAR,
    health_professional VARCHAR,
    initial_report_to_fda VARCHAR,
    reporter_state_code VARCHAR,
    reporter_country_code VARCHAR,

    -- Facility Dates
    date_facility_aware DATE,
    report_date DATE,
    report_to_fda VARCHAR,
    date_report_to_fda DATE,

    -- Event Details
    event_location VARCHAR,
    event_type VARCHAR,

    -- Report to Manufacturer
    date_report_to_manufacturer DATE,
    report_to_manufacturer VARCHAR,
    date_manufacturer_received DATE,

    -- Manufacturer Contact Info
    manufacturer_contact_title VARCHAR,
    manufacturer_contact_first_name VARCHAR,
    manufacturer_contact_last_name VARCHAR,
    manufacturer_contact_address_1 VARCHAR,
    manufacturer_contact_address_2 VARCHAR,
    manufacturer_contact_city VARCHAR,
    manufacturer_contact_state VARCHAR,
    manufacturer_contact_zip VARCHAR,
    manufacturer_contact_zip_ext VARCHAR,
    manufacturer_contact_country VARCHAR,
    manufacturer_contact_postal VARCHAR,
    -- Phone fields (FDA splits into components)
    manufacturer_contact_area_code VARCHAR,
    manufacturer_contact_exchange VARCHAR,
    manufacturer_contact_phone_no VARCHAR,
    manufacturer_contact_extension VARCHAR,
    manufacturer_contact_pcountry VARCHAR,
    manufacturer_contact_pcity VARCHAR,
    manufacturer_contact_plocal VARCHAR,

    -- Global Manufacturer (G1) Info
    manufacturer_g1_name VARCHAR,
    manufacturer_g1_street_1 VARCHAR,
    manufacturer_g1_street_2 VARCHAR,
    manufacturer_g1_city VARCHAR,
    manufacturer_g1_state VARCHAR,
    manufacturer_g1_zip VARCHAR,
    manufacturer_g1_zip_ext VARCHAR,
    manufacturer_g1_country VARCHAR,
    manufacturer_g1_postal VARCHAR,

    -- Device Manufacturing
    device_date_of_manufacture DATE,

    -- Device Flags
    single_use_flag VARCHAR,
    remedial_action VARCHAR,
    previous_use_code VARCHAR,
    removal_correction_number VARCHAR,
    manufacturer_link_flag VARCHAR,

    -- Distributor Info
    distributor_name VARCHAR,
    distributor_address_1 VARCHAR,
    distributor_address_2 VARCHAR,
    distributor_city VARCHAR,
    distributor_state VARCHAR,
    distributor_zip VARCHAR,
    distributor_zip_ext VARCHAR,

    -- Report Type
    type_of_report VARCHAR,

    -- Main Manufacturer Info
    manufacturer_name VARCHAR,
    manufacturer_address_1 VARCHAR,
    manufacturer_address_2 VARCHAR,
    manufacturer_city VARCHAR,
    manufacturer_state VARCHAR,
    manufacturer_zip VARCHAR,
    manufacturer_zip_ext VARCHAR,
    manufacturer_country VARCHAR,
    manufacturer_postal VARCHAR,

    -- Report Classification
    mfr_report_type VARCHAR,
    source_type VARCHAR,

    -- Metadata Dates
    date_added DATE,
    date_changed DATE,

    -- Product Identification
    product_code VARCHAR,
    pma_pmn_number VARCHAR,
    exemption_number VARCHAR,
    summary_report_flag VARCHAR,

    -- Supplemental Info
    noe_summarized VARCHAR,
    supplemental_dates_fda_received VARCHAR,
    supplemental_dates_mfr_received VARCHAR,
    baseline_report_number VARCHAR,
    schema_version VARCHAR,

    -- Derived fields for analysis
    manufacturer_clean VARCHAR,
    event_year INTEGER,
    event_month INTEGER,
    received_year INTEGER,
    received_month INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR,

    -- CHECK constraints for data validation
    CONSTRAINT chk_master_adverse_flag CHECK (adverse_event_flag IS NULL OR adverse_event_flag IN ('Y', 'N', '')),
    CONSTRAINT chk_master_product_flag CHECK (product_problem_flag IS NULL OR product_problem_flag IN ('Y', 'N', '')),
    CONSTRAINT chk_master_health_prof CHECK (health_professional IS NULL OR health_professional IN ('Y', 'N', '')),
    CONSTRAINT chk_master_single_use CHECK (single_use_flag IS NULL OR single_use_flag IN ('Y', 'N', '')),
    CONSTRAINT chk_master_event_year CHECK (event_year IS NULL OR (event_year >= 1980 AND event_year <= 2100)),
    CONSTRAINT chk_master_received_year CHECK (received_year IS NULL OR (received_year >= 1980 AND received_year <= 2100)),
    CONSTRAINT chk_master_devices_count CHECK (number_devices_in_event IS NULL OR number_devices_in_event >= 0),
    CONSTRAINT chk_master_patients_count CHECK (number_patients_in_event IS NULL OR number_patients_in_event >= 0)
)
"""

# =============================================================================
# DEVICES TABLE (28 FDA columns + derived fields)
# Informational FK: mdr_report_key -> master_events.mdr_report_key
# =============================================================================

CREATE_DEVICES = """
CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY DEFAULT nextval('devices_id_seq'),

    -- Foreign Key
    mdr_report_key VARCHAR NOT NULL,

    -- Device Identifiers
    device_event_key VARCHAR,
    device_sequence_number INTEGER,

    -- Device Flags
    implant_flag VARCHAR,
    date_removed_flag VARCHAR,

    -- Dates
    date_received DATE,
    expiration_date_of_device DATE,
    date_returned_to_manufacturer DATE,

    -- Device Details
    brand_name VARCHAR,
    generic_name VARCHAR,
    model_number VARCHAR,
    catalog_number VARCHAR,
    lot_number VARCHAR,
    other_id_number VARCHAR,
    device_age_text VARCHAR,

    -- Manufacturer Info
    manufacturer_d_name VARCHAR,
    manufacturer_d_address_1 VARCHAR,
    manufacturer_d_address_2 VARCHAR,
    manufacturer_d_city VARCHAR,
    manufacturer_d_state VARCHAR,
    manufacturer_d_zip VARCHAR,
    manufacturer_d_zip_ext VARCHAR,
    manufacturer_d_country VARCHAR,
    manufacturer_d_postal VARCHAR,

    -- Device Status
    device_operator VARCHAR,
    device_availability VARCHAR,
    device_evaluated_by_manufacturer VARCHAR,

    -- Product Code
    device_report_product_code VARCHAR,

    -- Derived fields
    manufacturer_d_clean VARCHAR,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR,

    -- CHECK constraints for data validation
    CONSTRAINT chk_devices_implant_flag CHECK (implant_flag IS NULL OR implant_flag IN ('Y', 'N', '')),
    CONSTRAINT chk_devices_removed_flag CHECK (date_removed_flag IS NULL OR date_removed_flag IN ('Y', 'N', '')),
    CONSTRAINT chk_devices_seq_num CHECK (device_sequence_number IS NULL OR device_sequence_number > 0)
)
"""

# =============================================================================
# PATIENTS TABLE (10 FDA columns + derived/outcome fields)
# Informational FK: mdr_report_key -> master_events.mdr_report_key
# =============================================================================

CREATE_PATIENTS = """
CREATE TABLE IF NOT EXISTS patients (
    id INTEGER PRIMARY KEY DEFAULT nextval('patients_id_seq'),

    -- Foreign Key
    mdr_report_key VARCHAR NOT NULL,

    -- Patient Identifiers
    patient_sequence_number INTEGER,

    -- Date
    date_received DATE,

    -- Sequence Numbers
    sequence_number_treatment INTEGER,
    sequence_number_outcome INTEGER,

    -- Patient Demographics (NEW FDA fields)
    patient_age VARCHAR,
    patient_sex VARCHAR,
    patient_weight VARCHAR,
    patient_ethnicity VARCHAR,
    patient_race VARCHAR,

    -- Derived age fields for analytics
    patient_age_numeric DECIMAL,
    patient_age_unit VARCHAR,

    -- Raw concatenated fields
    outcome_codes_raw VARCHAR,
    treatment_codes_raw VARCHAR,

    -- Parsed individual outcomes (boolean flags)
    outcome_death BOOLEAN DEFAULT FALSE,
    outcome_life_threatening BOOLEAN DEFAULT FALSE,
    outcome_hospitalization BOOLEAN DEFAULT FALSE,
    outcome_disability BOOLEAN DEFAULT FALSE,
    outcome_congenital_anomaly BOOLEAN DEFAULT FALSE,
    outcome_required_intervention BOOLEAN DEFAULT FALSE,
    outcome_other BOOLEAN DEFAULT FALSE,

    -- Parsed individual treatments (boolean flags)
    treatment_drug BOOLEAN DEFAULT FALSE,
    treatment_device BOOLEAN DEFAULT FALSE,
    treatment_surgery BOOLEAN DEFAULT FALSE,
    treatment_other BOOLEAN DEFAULT FALSE,
    treatment_unknown BOOLEAN DEFAULT FALSE,
    treatment_no_information BOOLEAN DEFAULT FALSE,
    treatment_blood_products BOOLEAN DEFAULT FALSE,
    treatment_hospitalization BOOLEAN DEFAULT FALSE,
    treatment_physical_therapy BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR,

    -- CHECK constraints for data validation
    CONSTRAINT chk_patients_sex CHECK (patient_sex IS NULL OR patient_sex IN ('M', 'F', 'U', 'Male', 'Female', 'Unknown', '')),
    CONSTRAINT chk_patients_age CHECK (patient_age_numeric IS NULL OR (patient_age_numeric >= 0 AND patient_age_numeric <= 200)),
    CONSTRAINT chk_patients_seq_num CHECK (patient_sequence_number IS NULL OR patient_sequence_number > 0)
)
"""

# =============================================================================
# MDR TEXT TABLE (6 FDA columns)
# Informational FK: mdr_report_key -> master_events.mdr_report_key
# =============================================================================

CREATE_MDR_TEXT = """
CREATE TABLE IF NOT EXISTS mdr_text (
    id INTEGER PRIMARY KEY DEFAULT nextval('mdr_text_id_seq'),

    -- Foreign Key
    mdr_report_key VARCHAR NOT NULL,

    -- Text Identifiers
    mdr_text_key VARCHAR,
    text_type_code VARCHAR,
    patient_sequence_number INTEGER,

    -- Date
    date_report DATE,

    -- Content
    text_content TEXT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR,

    -- CHECK constraints for data validation
    CONSTRAINT chk_text_type_code CHECK (text_type_code IS NULL OR text_type_code IN ('D', 'E', 'N', 'H', 'A', 'B', 'C', 'F', 'R', ''))
    -- Text type codes: D=Device description, E=Event description, N=Narrative,
    -- H=History, A=Additional, B=Background, C=Conclusion, F=Follow-up, R=Report
)
"""

# =============================================================================
# DEVICE PROBLEMS TABLE (2 FDA columns)
# Informational FK: mdr_report_key -> master_events.mdr_report_key
# =============================================================================

CREATE_DEVICE_PROBLEMS = """
CREATE TABLE IF NOT EXISTS device_problems (
    id INTEGER PRIMARY KEY DEFAULT nextval('device_problems_id_seq'),

    -- Foreign Key
    mdr_report_key VARCHAR NOT NULL,

    -- Problem Code
    device_problem_code VARCHAR,

    -- Date (derived from master)
    date_added DATE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

# =============================================================================
# PATIENT PROBLEMS TABLE (from patientproblemcode.zip)
# Informational FK: mdr_report_key -> master_events.mdr_report_key
# =============================================================================

CREATE_PATIENT_PROBLEMS = """
CREATE TABLE IF NOT EXISTS patient_problems (
    id INTEGER PRIMARY KEY DEFAULT nextval('patient_problems_id_seq'),

    -- Foreign Key
    mdr_report_key VARCHAR NOT NULL,

    -- Patient sequence
    patient_sequence_number INTEGER,

    -- Patient Problem Code
    patient_problem_code VARCHAR,

    -- Dates
    date_added TIMESTAMP,
    date_changed TIMESTAMP,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

# =============================================================================
# ASR REPORTS TABLE (Alternative Summary Reports 1999-2019)
# =============================================================================

CREATE_ASR_REPORTS = """
CREATE TABLE IF NOT EXISTS asr_reports (
    id INTEGER PRIMARY KEY DEFAULT nextval('asr_reports_id_seq'),

    -- Identifiers
    report_id VARCHAR,
    report_year INTEGER,

    -- Device Information
    brand_name VARCHAR,
    generic_name VARCHAR,
    manufacturer_name VARCHAR,
    product_code VARCHAR,
    device_class VARCHAR,

    -- Event Counts
    report_count INTEGER,
    event_count INTEGER,
    death_count INTEGER,
    injury_count INTEGER,
    malfunction_count INTEGER,

    -- Date Range
    date_start DATE,
    date_end DATE,

    -- Additional Info
    exemption_number VARCHAR,
    pma_pmn_number VARCHAR,
    submission_type VARCHAR,

    -- Narrative Summary
    summary_text TEXT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR,

    -- CHECK constraints for ASR data validation
    CONSTRAINT chk_asr_year CHECK (report_year IS NULL OR (report_year >= 1999 AND report_year <= 2019)),
    CONSTRAINT chk_asr_death_count CHECK (death_count IS NULL OR death_count >= 0),
    CONSTRAINT chk_asr_injury_count CHECK (injury_count IS NULL OR injury_count >= 0),
    CONSTRAINT chk_asr_malfunction_count CHECK (malfunction_count IS NULL OR malfunction_count >= 0)
)
"""

# =============================================================================
# ASR PATIENT PROBLEM CODES (from ASR_PPC.zip)
# Informational FK: report_id -> asr_reports.report_id
# =============================================================================

CREATE_ASR_PATIENT_PROBLEMS = """
CREATE TABLE IF NOT EXISTS asr_patient_problems (
    id INTEGER PRIMARY KEY DEFAULT nextval('asr_patient_problems_id_seq'),

    -- Foreign Key to ASR report
    report_id VARCHAR,

    -- Problem Code
    patient_problem_code VARCHAR,

    -- Count
    occurrence_count INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

# =============================================================================
# DEN REPORTS TABLE (Device Experience Network Legacy 1984-1997)
# =============================================================================

CREATE_DEN_REPORTS = """
CREATE TABLE IF NOT EXISTS den_reports (
    id INTEGER PRIMARY KEY DEFAULT nextval('den_reports_id_seq'),

    -- Legacy Identifiers
    mdr_report_key VARCHAR,
    report_number VARCHAR,
    report_source VARCHAR,

    -- Dates
    date_received DATE,
    date_of_event DATE,
    date_report DATE,

    -- Device Information
    brand_name VARCHAR,
    generic_name VARCHAR,
    model_number VARCHAR,
    catalog_number VARCHAR,
    lot_number VARCHAR,
    device_operator VARCHAR,

    -- Manufacturer Information
    manufacturer_name VARCHAR,
    manufacturer_city VARCHAR,
    manufacturer_state VARCHAR,
    manufacturer_country VARCHAR,

    -- Event Information
    event_type VARCHAR,
    event_description TEXT,
    patient_outcome VARCHAR,

    -- Additional FDA Fields (legacy format may vary)
    additional_info TEXT,

    -- Metadata
    report_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR,

    -- CHECK constraints for DEN data validation
    CONSTRAINT chk_den_year CHECK (report_year IS NULL OR (report_year >= 1984 AND report_year <= 1997))
)
"""

# =============================================================================
# MANUFACTURER DISCLAIMERS TABLE (from disclaim.zip)
# =============================================================================

CREATE_MANUFACTURER_DISCLAIMERS = """
CREATE TABLE IF NOT EXISTS manufacturer_disclaimers (
    id INTEGER PRIMARY KEY DEFAULT nextval('manufacturer_disclaimers_id_seq'),

    -- Manufacturer Information
    manufacturer_name VARCHAR,

    -- Disclaimer Content
    disclaimer_text TEXT,
    effective_date DATE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

# =============================================================================
# PATIENT PROBLEM CODES LOOKUP TABLE (from patientproblemdata.zip)
# =============================================================================

CREATE_PATIENT_PROBLEM_CODES = """
CREATE TABLE IF NOT EXISTS patient_problem_codes (
    problem_code VARCHAR PRIMARY KEY,
    description VARCHAR,
    category VARCHAR,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# =============================================================================
# LOOKUP TABLES
# =============================================================================

CREATE_PRODUCT_CODES = """
CREATE TABLE IF NOT EXISTS product_codes (
    product_code VARCHAR PRIMARY KEY,
    device_name VARCHAR,
    device_class VARCHAR,
    regulation_number VARCHAR,
    regulation_name VARCHAR,
    review_panel VARCHAR,
    medical_specialty VARCHAR,
    submission_type VARCHAR,
    definition TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_PROBLEM_CODES = """
CREATE TABLE IF NOT EXISTS problem_codes (
    problem_code VARCHAR PRIMARY KEY,
    description VARCHAR,
    category VARCHAR,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_MANUFACTURERS = """
CREATE TABLE IF NOT EXISTS manufacturers (
    id INTEGER PRIMARY KEY,
    raw_name VARCHAR,
    clean_name VARCHAR,
    parent_company VARCHAR,
    is_scs_manufacturer BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# =============================================================================
# OPERATIONAL TABLES
# =============================================================================

CREATE_INGESTION_LOG = """
CREATE TABLE IF NOT EXISTS ingestion_log (
    id INTEGER PRIMARY KEY,
    file_name VARCHAR,
    file_type VARCHAR,
    source VARCHAR,
    records_processed INTEGER,
    records_loaded INTEGER,
    records_errors INTEGER,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    status VARCHAR,
    error_message TEXT,
    schema_info JSON,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_SAVED_QUERIES = """
CREATE TABLE IF NOT EXISTS saved_queries (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    query_params JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_run TIMESTAMP,
    run_count INTEGER DEFAULT 0
)
"""

CREATE_APP_SETTINGS = """
CREATE TABLE IF NOT EXISTS app_settings (
    key VARCHAR PRIMARY KEY,
    value VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_DOWNLOAD_STATE = """
CREATE TABLE IF NOT EXISTS download_state (
    filename VARCHAR PRIMARY KEY,
    file_type VARCHAR,
    url VARCHAR,
    size_bytes BIGINT,
    checksum VARCHAR,
    download_started TIMESTAMP,
    download_completed TIMESTAMP,
    status VARCHAR,
    error_message TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# =============================================================================
# DATA FRESHNESS TRACKING TABLE
# =============================================================================

CREATE_DATA_FRESHNESS = """
CREATE TABLE IF NOT EXISTS data_freshness (
    table_name VARCHAR PRIMARY KEY,
    last_download_check TIMESTAMP,
    last_successful_load TIMESTAMP,
    latest_record_date DATE,
    days_since_update INTEGER,
    record_count BIGINT,
    status VARCHAR,  -- 'CURRENT', 'STALE', 'VERY_STALE'

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# =============================================================================
# AGGREGATE TABLES
# =============================================================================

CREATE_DAILY_AGGREGATES = """
CREATE TABLE IF NOT EXISTS daily_aggregates (
    date DATE,
    product_code VARCHAR,
    manufacturer_clean VARCHAR,
    event_type VARCHAR,
    event_count INTEGER,
    death_count INTEGER,
    injury_count INTEGER,
    malfunction_count INTEGER,

    PRIMARY KEY (date, product_code, manufacturer_clean, event_type)
)
"""

# =============================================================================
# INDEX DEFINITIONS
# =============================================================================

CREATE_INDEXES = [
    # Master Events indexes
    "CREATE INDEX IF NOT EXISTS idx_master_product_code ON master_events(product_code)",
    "CREATE INDEX IF NOT EXISTS idx_master_manufacturer ON master_events(manufacturer_clean)",
    "CREATE INDEX IF NOT EXISTS idx_master_date_received ON master_events(date_received)",
    "CREATE INDEX IF NOT EXISTS idx_master_date_event ON master_events(date_of_event)",
    "CREATE INDEX IF NOT EXISTS idx_master_event_type ON master_events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_master_year_month ON master_events(received_year, received_month)",
    "CREATE INDEX IF NOT EXISTS idx_master_adverse_flag ON master_events(adverse_event_flag)",
    "CREATE INDEX IF NOT EXISTS idx_master_product_flag ON master_events(product_problem_flag)",
    "CREATE INDEX IF NOT EXISTS idx_master_report_number ON master_events(report_number)",
    "CREATE INDEX IF NOT EXISTS idx_master_date_added ON master_events(date_added)",

    # Devices indexes
    "CREATE INDEX IF NOT EXISTS idx_devices_mdr_key ON devices(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_devices_brand ON devices(brand_name)",
    "CREATE INDEX IF NOT EXISTS idx_devices_manufacturer ON devices(manufacturer_d_clean)",
    "CREATE INDEX IF NOT EXISTS idx_devices_product_code ON devices(device_report_product_code)",
    "CREATE INDEX IF NOT EXISTS idx_devices_event_key ON devices(device_event_key)",
    "CREATE INDEX IF NOT EXISTS idx_devices_model ON devices(model_number)",

    # Patients indexes
    "CREATE INDEX IF NOT EXISTS idx_patients_mdr_key ON patients(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_patients_outcomes ON patients(outcome_death, outcome_hospitalization)",
    "CREATE INDEX IF NOT EXISTS idx_patients_sex ON patients(patient_sex)",
    "CREATE INDEX IF NOT EXISTS idx_patients_age ON patients(patient_age_numeric)",

    # Text indexes
    "CREATE INDEX IF NOT EXISTS idx_text_mdr_key ON mdr_text(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_text_type ON mdr_text(text_type_code)",
    "CREATE INDEX IF NOT EXISTS idx_text_key ON mdr_text(mdr_text_key)",

    # Problems indexes
    "CREATE INDEX IF NOT EXISTS idx_problems_mdr_key ON device_problems(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_problems_code ON device_problems(device_problem_code)",

    # Lookup indexes
    "CREATE INDEX IF NOT EXISTS idx_manufacturers_raw ON manufacturers(raw_name)",
    "CREATE INDEX IF NOT EXISTS idx_manufacturers_clean ON manufacturers(clean_name)",

    # Aggregate indexes
    "CREATE INDEX IF NOT EXISTS idx_daily_agg_date ON daily_aggregates(date)",
    "CREATE INDEX IF NOT EXISTS idx_daily_agg_product ON daily_aggregates(product_code)",
    "CREATE INDEX IF NOT EXISTS idx_daily_agg_manufacturer ON daily_aggregates(manufacturer_clean)",

    # Patient problems indexes
    "CREATE INDEX IF NOT EXISTS idx_patient_problems_mdr_key ON patient_problems(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_patient_problems_code ON patient_problems(patient_problem_code)",

    # ASR indexes
    "CREATE INDEX IF NOT EXISTS idx_asr_report_id ON asr_reports(report_id)",
    "CREATE INDEX IF NOT EXISTS idx_asr_year ON asr_reports(report_year)",
    "CREATE INDEX IF NOT EXISTS idx_asr_manufacturer ON asr_reports(manufacturer_name)",
    "CREATE INDEX IF NOT EXISTS idx_asr_product_code ON asr_reports(product_code)",
    "CREATE INDEX IF NOT EXISTS idx_asr_patient_problems_report ON asr_patient_problems(report_id)",

    # DEN legacy indexes
    "CREATE INDEX IF NOT EXISTS idx_den_mdr_key ON den_reports(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_den_date_received ON den_reports(date_received)",
    "CREATE INDEX IF NOT EXISTS idx_den_manufacturer ON den_reports(manufacturer_name)",
    "CREATE INDEX IF NOT EXISTS idx_den_year ON den_reports(report_year)",
]


def create_all_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create all database tables.

    Args:
        conn: DuckDB connection.
    """
    # Create sequences for auto-increment IDs
    sequences = [
        "devices_id_seq",
        "patients_id_seq",
        "mdr_text_id_seq",
        "device_problems_id_seq",
        "patient_problems_id_seq",
        "asr_reports_id_seq",
        "asr_patient_problems_id_seq",
        "den_reports_id_seq",
        "manufacturer_disclaimers_id_seq",
    ]
    for seq_name in sequences:
        try:
            conn.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name}")
        except Exception:
            pass  # Sequence might already exist

    tables = [
        ("master_events", CREATE_MASTER_EVENTS),
        ("devices", CREATE_DEVICES),
        ("patients", CREATE_PATIENTS),
        ("mdr_text", CREATE_MDR_TEXT),
        ("device_problems", CREATE_DEVICE_PROBLEMS),
        ("patient_problems", CREATE_PATIENT_PROBLEMS),
        ("asr_reports", CREATE_ASR_REPORTS),
        ("asr_patient_problems", CREATE_ASR_PATIENT_PROBLEMS),
        ("den_reports", CREATE_DEN_REPORTS),
        ("manufacturer_disclaimers", CREATE_MANUFACTURER_DISCLAIMERS),
        ("product_codes", CREATE_PRODUCT_CODES),
        ("problem_codes", CREATE_PROBLEM_CODES),
        ("patient_problem_codes", CREATE_PATIENT_PROBLEM_CODES),
        ("manufacturers", CREATE_MANUFACTURERS),
        ("ingestion_log", CREATE_INGESTION_LOG),
        ("saved_queries", CREATE_SAVED_QUERIES),
        ("app_settings", CREATE_APP_SETTINGS),
        ("download_state", CREATE_DOWNLOAD_STATE),
        ("data_freshness", CREATE_DATA_FRESHNESS),
        ("daily_aggregates", CREATE_DAILY_AGGREGATES),
    ]

    for table_name, create_sql in tables:
        try:
            conn.execute(create_sql)
            logger.info(f"Created table: {table_name}")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise


def create_all_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create all database indexes.

    Args:
        conn: DuckDB connection.
    """
    for index_sql in CREATE_INDEXES:
        try:
            conn.execute(index_sql)
        except Exception as e:
            # Index might already exist
            logger.debug(f"Index creation note: {e}")

    logger.info(f"Created {len(CREATE_INDEXES)} indexes")


def initialize_database(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Initialize the database with all tables and indexes.

    Args:
        conn: DuckDB connection.
    """
    logger.info("Initializing database schema...")
    create_all_tables(conn)
    create_all_indexes(conn)

    # Store schema version
    try:
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value) VALUES ('schema_version', ?)",
            [SCHEMA_VERSION]
        )
    except Exception:
        pass

    logger.info("Database initialization complete")


def get_schema_version(conn: duckdb.DuckDBPyConnection) -> Optional[str]:
    """
    Get the current schema version.

    Args:
        conn: DuckDB connection.

    Returns:
        Schema version string or None.
    """
    try:
        result = conn.execute(
            "SELECT value FROM app_settings WHERE key = 'schema_version'"
        ).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def get_table_counts(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Get row counts for all main tables.

    Args:
        conn: DuckDB connection.

    Returns:
        Dictionary mapping table names to row counts.
    """
    tables = [
        "master_events",
        "devices",
        "patients",
        "mdr_text",
        "device_problems",
        "patient_problems",
        "asr_reports",
        "asr_patient_problems",
        "den_reports",
        "manufacturer_disclaimers",
        "product_codes",
        "problem_codes",
        "patient_problem_codes",
        "manufacturers",
        "ingestion_log",
    ]

    counts = {}
    for table in tables:
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = result[0] if result else 0
        except Exception:
            counts[table] = 0

    return counts


def drop_all_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Drop all tables (use with caution!).

    Args:
        conn: DuckDB connection.
    """
    tables = [
        "daily_aggregates",
        "download_state",
        "app_settings",
        "saved_queries",
        "ingestion_log",
        "manufacturers",
        "problem_codes",
        "patient_problem_codes",
        "product_codes",
        "manufacturer_disclaimers",
        "den_reports",
        "asr_patient_problems",
        "asr_reports",
        "patient_problems",
        "device_problems",
        "mdr_text",
        "patients",
        "devices",
        "master_events",
    ]

    for table in tables:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"Dropped table: {table}")
        except Exception as e:
            logger.warning(f"Could not drop table {table}: {e}")


def get_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list:
    """
    Get column names for a table.

    Args:
        conn: DuckDB connection.
        table_name: Name of the table.

    Returns:
        List of column names.
    """
    try:
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        return [row[0] for row in result]
    except Exception:
        return []
