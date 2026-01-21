"""DuckDB schema definitions for MAUDE database."""

from typing import Optional
import duckdb
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger

logger = get_logger("schema")


# SQL statements for creating tables
CREATE_MASTER_EVENTS = """
CREATE TABLE IF NOT EXISTS master_events (
    mdr_report_key VARCHAR PRIMARY KEY,
    event_key VARCHAR,
    report_number VARCHAR,
    report_source_code VARCHAR,
    manufacturer_link_flag VARCHAR,
    number_devices_in_event INTEGER,
    number_patients_in_event INTEGER,
    date_received DATE,
    date_report DATE,
    date_of_event DATE,
    reprocessed_flag VARCHAR,
    reporter_occupation_code VARCHAR,
    health_professional VARCHAR,
    initial_report_to_fda VARCHAR,
    distributor_name VARCHAR,
    distributor_address_1 VARCHAR,
    distributor_address_2 VARCHAR,
    distributor_city VARCHAR,
    distributor_state VARCHAR,
    distributor_zip VARCHAR,
    distributor_zip_ext VARCHAR,
    date_facility_aware DATE,
    report_date DATE,
    report_to_fda VARCHAR,
    date_report_to_fda DATE,
    event_location VARCHAR,
    report_to_manufacturer VARCHAR,
    date_report_to_manufacturer DATE,
    date_manufacturer_received DATE,
    type_of_report VARCHAR,
    product_problem_flag VARCHAR,
    adverse_event_flag VARCHAR,
    single_use_flag VARCHAR,
    remedial_action VARCHAR,
    removal_correction_number VARCHAR,
    event_type VARCHAR,
    manufacturer_contact_name VARCHAR,
    manufacturer_contact_address_1 VARCHAR,
    manufacturer_contact_address_2 VARCHAR,
    manufacturer_contact_city VARCHAR,
    manufacturer_contact_state VARCHAR,
    manufacturer_contact_zip VARCHAR,
    manufacturer_contact_zip_ext VARCHAR,
    manufacturer_contact_country VARCHAR,
    manufacturer_contact_postal VARCHAR,
    manufacturer_contact_phone VARCHAR,
    manufacturer_contact_extension VARCHAR,
    manufacturer_contact_email VARCHAR,
    manufacturer_name VARCHAR,
    manufacturer_address_1 VARCHAR,
    manufacturer_address_2 VARCHAR,
    manufacturer_city VARCHAR,
    manufacturer_state VARCHAR,
    manufacturer_zip VARCHAR,
    manufacturer_zip_ext VARCHAR,
    manufacturer_country VARCHAR,
    manufacturer_postal VARCHAR,
    product_code VARCHAR,
    pma_pmn_number VARCHAR,
    exemption_number VARCHAR,
    summary_report_flag VARCHAR,

    -- Derived fields for analysis
    manufacturer_clean VARCHAR,
    event_year INTEGER,
    event_month INTEGER,
    received_year INTEGER,
    received_month INTEGER,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

CREATE_DEVICES = """
CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY,
    mdr_report_key VARCHAR NOT NULL,
    device_sequence_number INTEGER,
    date_received DATE,
    brand_name VARCHAR,
    generic_name VARCHAR,
    manufacturer_d_name VARCHAR,
    manufacturer_d_address_1 VARCHAR,
    manufacturer_d_address_2 VARCHAR,
    manufacturer_d_city VARCHAR,
    manufacturer_d_state VARCHAR,
    manufacturer_d_zip VARCHAR,
    manufacturer_d_zip_ext VARCHAR,
    manufacturer_d_country VARCHAR,
    manufacturer_d_postal VARCHAR,
    device_report_product_code VARCHAR,
    model_number VARCHAR,
    catalog_number VARCHAR,
    lot_number VARCHAR,
    other_id_number VARCHAR,
    device_operator VARCHAR,
    device_availability VARCHAR,
    device_evaluated_by_manufacturer VARCHAR,
    date_returned_to_manufacturer DATE,
    device_age_text VARCHAR,
    combination_product_flag VARCHAR,
    implant_flag VARCHAR,
    date_removed_flag VARCHAR,
    expiration_date_of_device DATE,

    -- Derived fields
    manufacturer_d_clean VARCHAR,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

CREATE_PATIENTS = """
CREATE TABLE IF NOT EXISTS patients (
    id INTEGER PRIMARY KEY,
    mdr_report_key VARCHAR NOT NULL,
    patient_sequence_number INTEGER,
    date_received DATE,
    sequence_number_treatment INTEGER,
    sequence_number_outcome INTEGER,

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

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

CREATE_MDR_TEXT = """
CREATE TABLE IF NOT EXISTS mdr_text (
    id INTEGER PRIMARY KEY,
    mdr_report_key VARCHAR NOT NULL,
    text_type_code VARCHAR,
    patient_sequence_number INTEGER,
    date_received DATE,
    text_content TEXT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

CREATE_DEVICE_PROBLEMS = """
CREATE TABLE IF NOT EXISTS device_problems (
    id INTEGER PRIMARY KEY,
    mdr_report_key VARCHAR NOT NULL,
    device_problem_code VARCHAR,
    date_added DATE,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR
)
"""

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

# Index creation statements
CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_master_product_code ON master_events(product_code)",
    "CREATE INDEX IF NOT EXISTS idx_master_manufacturer ON master_events(manufacturer_clean)",
    "CREATE INDEX IF NOT EXISTS idx_master_date_received ON master_events(date_received)",
    "CREATE INDEX IF NOT EXISTS idx_master_date_event ON master_events(date_of_event)",
    "CREATE INDEX IF NOT EXISTS idx_master_event_type ON master_events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_master_year_month ON master_events(received_year, received_month)",
    "CREATE INDEX IF NOT EXISTS idx_devices_mdr_key ON devices(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_devices_brand ON devices(brand_name)",
    "CREATE INDEX IF NOT EXISTS idx_devices_manufacturer ON devices(manufacturer_d_clean)",
    "CREATE INDEX IF NOT EXISTS idx_devices_product_code ON devices(device_report_product_code)",
    "CREATE INDEX IF NOT EXISTS idx_patients_mdr_key ON patients(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_patients_outcomes ON patients(outcome_death, outcome_hospitalization)",
    "CREATE INDEX IF NOT EXISTS idx_text_mdr_key ON mdr_text(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_text_type ON mdr_text(text_type_code)",
    "CREATE INDEX IF NOT EXISTS idx_problems_mdr_key ON device_problems(mdr_report_key)",
    "CREATE INDEX IF NOT EXISTS idx_problems_code ON device_problems(device_problem_code)",
    "CREATE INDEX IF NOT EXISTS idx_manufacturers_raw ON manufacturers(raw_name)",
    "CREATE INDEX IF NOT EXISTS idx_manufacturers_clean ON manufacturers(clean_name)",
    "CREATE INDEX IF NOT EXISTS idx_daily_agg_date ON daily_aggregates(date)",
    "CREATE INDEX IF NOT EXISTS idx_daily_agg_product ON daily_aggregates(product_code)",
    "CREATE INDEX IF NOT EXISTS idx_daily_agg_manufacturer ON daily_aggregates(manufacturer_clean)",
]


def create_all_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Create all database tables.

    Args:
        conn: DuckDB connection.
    """
    tables = [
        ("master_events", CREATE_MASTER_EVENTS),
        ("devices", CREATE_DEVICES),
        ("patients", CREATE_PATIENTS),
        ("mdr_text", CREATE_MDR_TEXT),
        ("device_problems", CREATE_DEVICE_PROBLEMS),
        ("product_codes", CREATE_PRODUCT_CODES),
        ("problem_codes", CREATE_PROBLEM_CODES),
        ("manufacturers", CREATE_MANUFACTURERS),
        ("ingestion_log", CREATE_INGESTION_LOG),
        ("saved_queries", CREATE_SAVED_QUERIES),
        ("app_settings", CREATE_APP_SETTINGS),
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
    logger.info("Database initialization complete")


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
        "product_codes",
        "problem_codes",
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
        "app_settings",
        "saved_queries",
        "ingestion_log",
        "manufacturers",
        "problem_codes",
        "product_codes",
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
