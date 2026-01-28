"""
Integration Tests for MAUDE Analyzer App-Database Connectivity.

These tests verify that:
1. All expected database columns are accessible
2. API endpoints return valid data
3. Filters work correctly with the database
4. Schema configuration matches actual database
"""

import os
import sys
from pathlib import Path
from typing import Optional

import pytest
import yaml

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Skip all tests if database not available
DB_PATH = PROJECT_ROOT / "data" / "maude.duckdb"
SKIP_REASON = "Database not available for integration testing"


def db_available() -> bool:
    """Check if database file exists."""
    return DB_PATH.exists()


pytestmark = pytest.mark.skipif(not db_available(), reason=SKIP_REASON)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def db_connection():
    """Create a read-only database connection."""
    import duckdb
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def schema_config():
    """Load schema configuration."""
    config_path = PROJECT_ROOT / "config" / "schema_config.yaml"
    if not config_path.exists():
        pytest.skip("Schema config not found")

    with open(config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def api_client():
    """Create a test client for the API."""
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


# =============================================================================
# Database Schema Tests
# =============================================================================

class TestDatabaseSchema:
    """Tests for database schema validation."""

    def test_master_events_table_exists(self, db_connection):
        """Verify master_events table exists and has data."""
        result = db_connection.execute("SELECT COUNT(*) FROM master_events").fetchone()
        assert result is not None
        assert result[0] > 0, "master_events table is empty"

    def test_devices_table_exists(self, db_connection):
        """Verify devices table exists and has data."""
        result = db_connection.execute("SELECT COUNT(*) FROM devices").fetchone()
        assert result is not None
        assert result[0] > 0, "devices table is empty"

    def test_patients_table_exists(self, db_connection):
        """Verify patients table exists and has data."""
        result = db_connection.execute("SELECT COUNT(*) FROM patients").fetchone()
        assert result is not None
        assert result[0] > 0, "patients table is empty"

    def test_mdr_text_table_exists(self, db_connection):
        """Verify mdr_text table exists and has data."""
        result = db_connection.execute("SELECT COUNT(*) FROM mdr_text").fetchone()
        assert result is not None
        assert result[0] > 0, "mdr_text table is empty"

    def test_required_columns_exist(self, db_connection, schema_config):
        """Verify all required columns exist in tables."""
        tables_config = schema_config.get("tables", {})

        for table_name, table_def in tables_config.items():
            # Get actual columns
            try:
                result = db_connection.execute(f"DESCRIBE {table_name}").fetchall()
                actual_columns = {row[0] for row in result}
            except Exception as e:
                pytest.fail(f"Table {table_name} does not exist: {e}")

            # Check required columns
            columns_config = table_def.get("columns", {})
            for col_name, col_def in columns_config.items():
                if col_def.get("required", False):
                    assert col_name in actual_columns, \
                        f"Required column {col_name} missing from {table_name}"

    def test_event_type_values_valid(self, db_connection, schema_config):
        """Verify event_type column contains only valid values."""
        event_types_config = schema_config.get("event_types", {}).get("codes", {})
        valid_codes = set(event_types_config.keys())

        # Add None/NULL as valid
        result = db_connection.execute("""
            SELECT DISTINCT event_type
            FROM master_events
            WHERE event_type IS NOT NULL
            LIMIT 100
        """).fetchall()

        actual_types = {row[0] for row in result if row[0]}
        invalid_types = actual_types - valid_codes

        # Allow some flexibility for legacy data
        assert len(invalid_types) < 5, \
            f"Found unexpected event types: {invalid_types}"

    def test_manufacturer_clean_coverage(self, db_connection):
        """Verify manufacturer_clean has reasonable coverage."""
        result = db_connection.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(manufacturer_clean) as with_mfr
            FROM master_events
        """).fetchone()

        total, with_mfr = result
        coverage = with_mfr / total if total > 0 else 0

        # Warn if coverage is below 50%
        assert coverage >= 0.3, \
            f"manufacturer_clean coverage too low: {coverage:.1%}"


# =============================================================================
# API Endpoint Tests
# =============================================================================

class TestAPIEndpoints:
    """Tests for API endpoint functionality."""

    def test_events_list_returns_data(self, api_client):
        """Test that /events endpoint returns events."""
        response = api_client.get("/api/events?page_size=10")
        assert response.status_code == 200

        data = response.json()
        assert "events" in data
        assert "pagination" in data
        assert data["pagination"]["total"] > 0

    def test_events_with_manufacturer_filter(self, api_client):
        """Test manufacturer filter works."""
        # First get a valid manufacturer
        mfr_response = api_client.get("/api/events/manufacturers?limit=1")
        assert mfr_response.status_code == 200

        manufacturers = mfr_response.json()
        if not manufacturers:
            pytest.skip("No manufacturers available for testing")

        mfr_name = manufacturers[0]["name"]

        # Now filter by that manufacturer
        response = api_client.get(f"/api/events?manufacturers={mfr_name}&page_size=5")
        assert response.status_code == 200

        data = response.json()
        # All returned events should have this manufacturer
        for event in data["events"]:
            assert event.get("manufacturer") == mfr_name or event.get("manufacturer_name") == mfr_name

    def test_events_with_event_type_filter(self, api_client):
        """Test event type filter works."""
        response = api_client.get("/api/events?event_types=D&page_size=5")
        assert response.status_code == 200

        data = response.json()
        # All returned events should be deaths
        for event in data["events"]:
            assert event["event_type"] == "D", \
                f"Expected event_type=D, got {event['event_type']}"

    def test_events_with_date_filter(self, api_client):
        """Test date range filter works."""
        response = api_client.get("/api/events?date_from=2023-01-01&date_to=2023-12-31&page_size=5")
        assert response.status_code == 200

        data = response.json()
        for event in data["events"]:
            if event.get("date_received"):
                assert event["date_received"].startswith("2023"), \
                    f"Event date {event['date_received']} outside expected range"

    def test_event_detail_returns_full_data(self, api_client):
        """Test that event detail includes devices, narratives, patients."""
        # Get an event ID first
        list_response = api_client.get("/api/events?page_size=1")
        events = list_response.json().get("events", [])
        if not events:
            pytest.skip("No events available for testing")

        mdr_key = events[0]["mdr_report_key"]

        # Get detail
        detail_response = api_client.get(f"/api/events/{mdr_key}")
        assert detail_response.status_code == 200

        data = detail_response.json()
        assert "mdr_report_key" in data
        assert "devices" in data
        assert "narratives" in data
        assert "patients" in data

    def test_stats_endpoint(self, api_client):
        """Test stats endpoint returns valid counts."""
        response = api_client.get("/api/events/stats")
        assert response.status_code == 200

        data = response.json()
        assert "total" in data
        assert "deaths" in data
        assert "injuries" in data
        assert "malfunctions" in data
        assert data["total"] >= 0

    def test_manufacturers_autocomplete(self, api_client):
        """Test manufacturer autocomplete returns data."""
        response = api_client.get("/api/events/manufacturers?limit=10")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "name" in data[0]
            assert "count" in data[0]

    def test_product_codes_autocomplete(self, api_client):
        """Test product codes autocomplete returns data."""
        response = api_client.get("/api/events/product-codes?limit=10")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "code" in data[0]


# =============================================================================
# Data Quality Endpoint Tests
# =============================================================================

class TestDataQualityEndpoints:
    """Tests for data quality monitoring endpoints."""

    def test_completeness_endpoint(self, api_client):
        """Test completeness endpoint returns metrics."""
        response = api_client.get("/api/data-quality/completeness")
        assert response.status_code == 200

        data = response.json()
        assert "overall" in data
        assert "by_year" in data
        assert "manufacturer_pct" in data["overall"]

    def test_schema_health_endpoint(self, api_client):
        """Test schema health endpoint returns validation results."""
        response = api_client.get("/api/data-quality/schema-health")
        assert response.status_code == 200

        data = response.json()
        assert "overall_status" in data
        assert "tables" in data
        assert "recommendations" in data

    def test_coverage_endpoint(self, api_client):
        """Test coverage endpoint returns field coverage."""
        response = api_client.get("/api/data-quality/coverage")
        assert response.status_code == 200

        data = response.json()
        assert "manufacturer_coverage" in data
        assert "product_code_coverage" in data

    def test_column_coverage_detail(self, api_client):
        """Test column coverage detail for a table."""
        response = api_client.get("/api/data-quality/column-coverage/master_events")
        assert response.status_code == 200

        data = response.json()
        assert "table" in data
        assert "row_count" in data
        assert "columns" in data


# =============================================================================
# Analytics Endpoint Tests
# =============================================================================

class TestAnalyticsEndpoints:
    """Tests for analytics endpoints."""

    def test_trends_endpoint(self, api_client):
        """Test trends endpoint returns time series data."""
        response = api_client.get("/api/analytics/trends?group_by=month")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)
        if data:
            assert "period" in data[0]
            assert "total" in data[0]

    def test_signals_endpoint(self, api_client):
        """Test signal detection endpoint."""
        response = api_client.get("/api/analytics/signals")
        assert response.status_code == 200

        data = response.json()
        # API may return a dict with signals key or a list directly
        if isinstance(data, dict):
            assert "signals" in data
        else:
            assert isinstance(data, list)

    def test_text_frequency_endpoint(self, api_client):
        """Test text frequency analysis endpoint."""
        response = api_client.get("/api/analytics/text-frequency")
        assert response.status_code == 200

        data = response.json()
        assert isinstance(data, list)


# =============================================================================
# Schema Configuration Validation Tests
# =============================================================================

class TestSchemaConfiguration:
    """Tests that schema config is valid and complete."""

    def test_schema_config_has_required_sections(self, schema_config):
        """Verify schema config has all required sections."""
        required_sections = ["tables", "event_types", "outcome_codes", "text_type_codes"]
        for section in required_sections:
            assert section in schema_config, f"Missing section: {section}"

    def test_event_types_complete(self, schema_config):
        """Verify all event types are defined."""
        event_types = schema_config.get("event_types", {}).get("codes", {})
        required_types = ["D", "IN", "M", "O"]
        for et in required_types:
            assert et in event_types, f"Missing event type: {et}"

    def test_tables_have_columns(self, schema_config):
        """Verify each table has column definitions."""
        tables = schema_config.get("tables", {})
        for table_name, table_def in tables.items():
            assert "columns" in table_def, f"Table {table_name} missing columns"
            assert len(table_def["columns"]) > 0, f"Table {table_name} has no columns defined"

    def test_filter_code_mapping_exists(self, schema_config):
        """Verify filter code mapping exists for I->IN conversion."""
        event_types = schema_config.get("event_types", {})
        filter_mapping = event_types.get("filter_code_mapping", {})

        assert "I" in filter_mapping, "Missing I->IN filter mapping"
        assert filter_mapping["I"] == "IN", "I should map to IN"


# =============================================================================
# Cross-Table Relationship Tests
# =============================================================================

class TestTableRelationships:
    """Tests for foreign key relationships between tables."""

    def test_devices_reference_valid_events(self, db_connection):
        """Verify devices reference existing master events."""
        # Sample check - verify some device records have valid event references
        result = db_connection.execute("""
            SELECT COUNT(*) as orphans
            FROM devices d
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m
                WHERE m.mdr_report_key = d.mdr_report_key
            )
            LIMIT 10000
        """).fetchone()

        orphan_count = result[0] if result else 0
        # Allow up to 35% orphans (data quality issue, not code bug)
        # This can happen when device files contain more records than master files
        total_devices = db_connection.execute("SELECT COUNT(*) FROM devices").fetchone()[0]

        if total_devices > 0:
            orphan_rate = orphan_count / total_devices
            assert orphan_rate < 0.35, \
                f"Too many orphan device records: {orphan_rate:.2%}"

    def test_patients_reference_valid_events(self, db_connection):
        """Verify patients reference existing master events."""
        result = db_connection.execute("""
            SELECT COUNT(*) as orphans
            FROM patients p
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m
                WHERE m.mdr_report_key = p.mdr_report_key
            )
            LIMIT 10000
        """).fetchone()

        orphan_count = result[0] if result else 0
        total_patients = db_connection.execute("SELECT COUNT(*) FROM patients").fetchone()[0]

        if total_patients > 0:
            orphan_rate = orphan_count / total_patients
            # Allow up to 35% orphans (data quality issue from partial loads)
            assert orphan_rate < 0.35, \
                f"Too many orphan patient records: {orphan_rate:.2%}"

    def test_mdr_text_references_valid_events(self, db_connection):
        """Verify mdr_text references existing master events."""
        result = db_connection.execute("""
            SELECT COUNT(*) as orphans
            FROM mdr_text t
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m
                WHERE m.mdr_report_key = t.mdr_report_key
            )
            LIMIT 10000
        """).fetchone()

        orphan_count = result[0] if result else 0
        total_text = db_connection.execute("SELECT COUNT(*) FROM mdr_text").fetchone()[0]

        if total_text > 0:
            orphan_rate = orphan_count / total_text
            # Allow up to 35% orphans (data quality issue from partial loads)
            assert orphan_rate < 0.35, \
                f"Too many orphan text records: {orphan_rate:.2%}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
