"""Pytest configuration and fixtures for MAUDE Analyzer tests."""

import pytest
import duckdb
import pandas as pd
from pathlib import Path


@pytest.fixture
def test_db():
    """Create in-memory DuckDB for testing."""
    conn = duckdb.connect(":memory:")

    # Create schema
    conn.execute("""
        CREATE TABLE master_events (
            mdr_report_key VARCHAR PRIMARY KEY,
            event_key VARCHAR,
            report_number VARCHAR,
            date_received DATE,
            date_of_event DATE,
            event_type VARCHAR,
            type_of_report VARCHAR,
            product_code VARCHAR,
            manufacturer_name VARCHAR,
            manufacturer_clean VARCHAR,
            product_problem_flag VARCHAR,
            adverse_event_flag VARCHAR,
            report_source_code VARCHAR,
            event_location VARCHAR,
            pma_pmn_number VARCHAR,
            received_year INTEGER,
            received_month INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE devices (
            id INTEGER PRIMARY KEY,
            mdr_report_key VARCHAR,
            brand_name VARCHAR,
            generic_name VARCHAR,
            model_number VARCHAR,
            manufacturer_d_name VARCHAR,
            manufacturer_d_clean VARCHAR,
            device_report_product_code VARCHAR
        )
    """)

    # Insert test data
    conn.execute("""
        INSERT INTO master_events (
            mdr_report_key, event_key, report_number, date_received, date_of_event,
            event_type, type_of_report, product_code, manufacturer_name, manufacturer_clean,
            product_problem_flag, adverse_event_flag, report_source_code, event_location,
            pma_pmn_number, received_year, received_month
        ) VALUES
        ('TEST001', 'EVT001', 'RPT001', '2024-01-15', '2024-01-10', 'IN', 'Initial', 'GZB', 'ABBOTT NEUROMODULATION', 'Abbott', 'Y', 'Y', 'MFR', 'Hospital', 'P123456', 2024, 1),
        ('TEST002', 'EVT002', 'RPT002', '2024-01-16', '2024-01-12', 'M', 'Initial', 'GZB', 'MEDTRONIC, INC.', 'Medtronic', 'Y', 'N', 'MFR', 'Home', 'P234567', 2024, 1),
        ('TEST003', 'EVT003', 'RPT003', '2024-01-17', '2024-01-14', 'D', 'Initial', 'LGW', 'ABBOTT NEUROMODULATION', 'Abbott', 'N', 'Y', 'USR', 'Hospital', 'P123456', 2024, 1),
        ('TEST004', 'EVT004', 'RPT004', '2024-02-01', '2024-01-28', 'IN', 'Supplemental', 'GZB', 'BOSTON SCIENTIFIC CORP', 'Boston Scientific', 'Y', 'Y', 'MFR', 'Hospital', 'P345678', 2024, 2),
        ('TEST005', 'EVT005', 'RPT005', '2024-02-15', '2024-02-10', 'M', 'Initial', 'PMP', 'NEVRO CORP', 'Nevro', 'Y', 'N', 'MFR', 'Clinic', 'P456789', 2024, 2)
    """)

    conn.execute("""
        INSERT INTO devices VALUES
        (1, 'TEST001', 'Proclaim XR', 'Spinal Cord Stimulator', 'A12345', 'ABBOTT NEUROMODULATION', 'Abbott', 'GZB'),
        (2, 'TEST002', 'Intellis', 'Spinal Cord Stimulator', 'M67890', 'MEDTRONIC, INC.', 'Medtronic', 'GZB'),
        (3, 'TEST003', 'Eon Mini', 'Spinal Cord Stimulator', 'A11111', 'ABBOTT NEUROMODULATION', 'Abbott', 'LGW'),
        (4, 'TEST004', 'WaveWriter Alpha', 'Spinal Cord Stimulator', 'B22222', 'BOSTON SCIENTIFIC CORP', 'Boston Scientific', 'GZB'),
        (5, 'TEST005', 'Senza', 'HF10 Therapy System', 'N33333', 'NEVRO CORP', 'Nevro', 'PMP')
    """)

    yield conn
    conn.close()


@pytest.fixture
def sample_master_data():
    """Sample master events DataFrame."""
    return pd.DataFrame({
        "mdr_report_key": ["TEST001", "TEST002", "TEST003", "TEST004", "TEST005"],
        "date_received": pd.to_datetime([
            "2024-01-15", "2024-01-16", "2024-01-17", "2024-02-01", "2024-02-15"
        ]),
        "manufacturer_clean": ["Abbott", "Medtronic", "Abbott", "Boston Scientific", "Nevro"],
        "product_code": ["GZB", "GZB", "LGW", "GZB", "PMP"],
        "event_type": ["IN", "M", "D", "IN", "M"],
    })


@pytest.fixture
def sample_device_data():
    """Sample device DataFrame."""
    return pd.DataFrame({
        "mdr_report_key": ["TEST001", "TEST002", "TEST003"],
        "brand_name": ["Proclaim XR", "Intellis", "Eon Mini"],
        "generic_name": ["Spinal Cord Stimulator"] * 3,
        "model_number": ["A12345", "M67890", "A11111"],
        "manufacturer_d_clean": ["Abbott", "Medtronic", "Abbott"],
    })


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary data directory structure."""
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir()
    processed_dir.mkdir()
    return tmp_path
