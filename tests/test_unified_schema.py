"""Tests for the Unified Schema Registry."""

import pytest
from config.unified_schema import (
    get_schema_registry,
    SCHEMA_VERSION,
    EVENT_TYPES,
    EVENT_TYPE_FILTER_TO_DB,
    EVENT_TYPE_DB_TO_FILTER,
    OUTCOME_CODES,
    TEXT_TYPE_CODES,
    SchemaEvolution,
    get_event_type_code,
    get_event_type_name,
    convert_filter_event_types,
    validate_table_columns,
)


class TestSchemaVersion:
    """Test schema version."""

    def test_schema_version_exists(self):
        """Test that schema version is defined."""
        assert SCHEMA_VERSION is not None
        assert isinstance(SCHEMA_VERSION, str)

    def test_schema_version_format(self):
        """Test schema version format."""
        # Should be like "2.1" or "2.0"
        parts = SCHEMA_VERSION.split(".")
        assert len(parts) >= 2
        assert all(p.isdigit() for p in parts)


class TestEventTypes:
    """Test event type definitions and conversions."""

    def test_all_event_types_defined(self):
        """Test that all expected event types are defined."""
        expected_codes = ["D", "IN", "M", "O", "*"]
        for code in expected_codes:
            assert code in EVENT_TYPES
            assert EVENT_TYPES[code].db_code == code

    def test_event_type_filter_codes(self):
        """Test filter code mapping."""
        # I -> IN for injury
        assert EVENT_TYPE_FILTER_TO_DB["I"] == "IN"
        assert EVENT_TYPE_DB_TO_FILTER["IN"] == "I"

        # Others map to themselves
        assert EVENT_TYPE_FILTER_TO_DB["D"] == "D"
        assert EVENT_TYPE_FILTER_TO_DB["M"] == "M"
        assert EVENT_TYPE_FILTER_TO_DB["O"] == "O"

    def test_get_event_type_code_conversion(self):
        """Test filter to DB code conversion function."""
        assert get_event_type_code("I") == "IN"
        assert get_event_type_code("D") == "D"
        assert get_event_type_code("M") == "M"
        assert get_event_type_code("O") == "O"
        # Unknown codes pass through
        assert get_event_type_code("X") == "X"

    def test_get_event_type_name(self):
        """Test event type name lookup."""
        assert get_event_type_name("D") == "Death"
        assert get_event_type_name("IN") == "Injury"
        assert get_event_type_name("M") == "Malfunction"
        assert get_event_type_name("O") == "Other"
        # Unknown returns the code
        assert get_event_type_name("X") == "X"

    def test_convert_filter_event_types(self):
        """Test batch conversion of filter codes."""
        result = convert_filter_event_types(["D", "I", "M"])
        assert result == ["D", "IN", "M"]

    def test_event_type_has_styling(self):
        """Test that event types have styling info."""
        for code, et in EVENT_TYPES.items():
            assert et.color is not None
            assert et.bg_class is not None
            assert et.text_class is not None
            assert et.severity >= 1


class TestOutcomeCodes:
    """Test patient outcome definitions."""

    def test_outcome_codes_defined(self):
        """Test that outcome codes are defined."""
        expected_codes = ["D", "L", "H", "DS"]
        for code in expected_codes:
            assert code in OUTCOME_CODES

    def test_outcome_has_db_field(self):
        """Test that outcomes have database field mappings."""
        for code, outcome in OUTCOME_CODES.items():
            assert outcome.db_field is not None
            assert outcome.db_field.startswith("outcome_")


class TestTextTypes:
    """Test text type definitions."""

    def test_text_types_defined(self):
        """Test that text types are defined."""
        expected_codes = ["D", "H", "M", "E", "N"]
        for code in expected_codes:
            assert code in TEXT_TYPE_CODES

    def test_text_type_has_priority(self):
        """Test that text types have priority."""
        for code, tt in TEXT_TYPE_CODES.items():
            assert tt.priority >= 1


class TestSchemaRegistry:
    """Test the unified schema registry."""

    def test_get_registry(self):
        """Test getting the registry singleton."""
        registry = get_schema_registry()
        assert registry is not None
        assert registry.VERSION == SCHEMA_VERSION

    def test_registry_has_tables(self):
        """Test that registry has table definitions."""
        registry = get_schema_registry()
        expected_tables = [
            "master_events",
            "devices",
            "patients",
            "mdr_text",
            "device_problems",
            "patient_problems",
        ]
        for table in expected_tables:
            assert table in registry.tables
            assert registry.get_table(table) is not None

    def test_table_has_columns(self):
        """Test that tables have column definitions."""
        registry = get_schema_registry()

        # Master events should have key columns
        master = registry.get_table("master_events")
        assert master is not None
        assert master.has_column("mdr_report_key")
        assert master.has_column("event_type")
        assert master.has_column("date_received")
        assert master.has_column("manufacturer_clean")

    def test_validate_columns(self):
        """Test column validation."""
        result = validate_table_columns("master_events", [
            "mdr_report_key",
            "event_type",
            "nonexistent_column",
        ])
        assert result["mdr_report_key"] is True
        assert result["event_type"] is True
        assert result["nonexistent_column"] is False

    def test_get_available_columns(self):
        """Test filtering to available columns."""
        registry = get_schema_registry()
        requested = ["mdr_report_key", "event_type", "fake_column"]
        available = registry.get_available_columns("master_events", requested)
        assert "mdr_report_key" in available
        assert "event_type" in available
        assert "fake_column" not in available

    def test_export_to_dict(self):
        """Test exporting registry to dict."""
        registry = get_schema_registry()
        data = registry.export_to_dict()

        assert "version" in data
        assert "tables" in data
        assert "event_types" in data
        assert "outcome_codes" in data
        assert "text_type_codes" in data

    def test_export_to_json(self):
        """Test exporting registry to JSON."""
        registry = get_schema_registry()
        json_str = registry.export_to_json()

        import json
        data = json.loads(json_str)
        assert data["version"] == SCHEMA_VERSION


class TestSchemaEvolution:
    """Test schema evolution tracking."""

    def test_master_versions(self):
        """Test master file version tracking."""
        assert 84 in SchemaEvolution.MASTER_VERSIONS
        assert 86 in SchemaEvolution.MASTER_VERSIONS

    def test_device_versions(self):
        """Test device file version tracking."""
        assert 28 in SchemaEvolution.DEVICE_VERSIONS
        assert 34 in SchemaEvolution.DEVICE_VERSIONS

    def test_optional_columns_master(self):
        """Test getting optional columns for master table."""
        optional = SchemaEvolution.get_optional_columns("master_events")
        # These columns vary between 84 and 86 column formats
        assert "mfr_report_type" in optional
        assert "reporter_state_code" in optional

    def test_optional_columns_devices(self):
        """Test getting optional columns for devices table."""
        optional = SchemaEvolution.get_optional_columns("devices")
        # These columns are in 34-column format but not 28-column
        assert "udi_di" in optional
        assert "udi_public" in optional
        assert "combination_product_flag" in optional

    def test_get_version_for_column_count(self):
        """Test getting version info by column count."""
        v84 = SchemaEvolution.get_version_for_column_count("master", 84)
        assert v84 is not None
        assert v84.column_count == 84

        v86 = SchemaEvolution.get_version_for_column_count("master", 86)
        assert v86 is not None
        assert v86.column_count == 86
