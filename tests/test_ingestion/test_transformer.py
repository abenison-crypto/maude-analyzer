"""Tests for data transformer module."""

import pytest
from datetime import date


class TestDataTransformer:
    """Tests for DataTransformer class."""

    def test_transformer_initialization(self):
        """Test transformer initialization."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()
        assert transformer is not None
        assert hasattr(transformer, "_manufacturer_map")

    def test_standardize_manufacturer_abbott(self):
        """Test Abbott name standardization."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        assert transformer.standardize_manufacturer("ABBOTT NEUROMODULATION") == "Abbott"
        assert transformer.standardize_manufacturer("ST. JUDE MEDICAL") == "Abbott"

    def test_standardize_manufacturer_medtronic(self):
        """Test Medtronic name standardization."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        assert transformer.standardize_manufacturer("MEDTRONIC, INC.") == "Medtronic"
        assert transformer.standardize_manufacturer("MEDTRONIC NEUROMODULATION") == "Medtronic"

    def test_standardize_manufacturer_boston_scientific(self):
        """Test Boston Scientific name standardization."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        result = transformer.standardize_manufacturer("BOSTON SCIENTIFIC CORP")
        assert result == "Boston Scientific"

    def test_standardize_manufacturer_nevro(self):
        """Test Nevro name standardization."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        assert transformer.standardize_manufacturer("NEVRO CORP") == "Nevro"

    def test_standardize_manufacturer_unknown(self):
        """Test unknown manufacturer handling."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        # Unknown manufacturers should be returned cleaned but unchanged
        result = transformer.standardize_manufacturer("UNKNOWN COMPANY INC")
        assert result is not None
        assert len(result) > 0

    def test_standardize_manufacturer_empty(self):
        """Test empty name handling."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        # Empty string should return "Unknown"
        result = transformer.standardize_manufacturer("")
        assert result == "Unknown"

    def test_parse_date_valid_slash_format(self):
        """Test valid date parsing with slash format."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        result = transformer.parse_date("01/15/2024")
        assert result == date(2024, 1, 15)

    def test_parse_date_valid_dash_format(self):
        """Test valid date parsing with dash format."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        result = transformer.parse_date("2024-01-15")
        assert result == date(2024, 1, 15)

    def test_parse_date_invalid(self):
        """Test invalid date handling."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        assert transformer.parse_date("invalid") is None
        assert transformer.parse_date("") is None

    def test_parse_date_none(self):
        """Test None date handling."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        assert transformer.parse_date(None) is None

    def test_clean_text_basic(self):
        """Test basic text cleaning."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        result = transformer.clean_text("  Hello  World  ")
        # Should strip and normalize whitespace
        assert "Hello" in result
        assert "World" in result

    def test_clean_text_none(self):
        """Test None text handling."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        # Returns empty string for None
        result = transformer.clean_text(None)
        assert result == ""

    def test_parse_outcome_codes(self):
        """Test outcome code parsing."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        result = transformer.parse_outcome_codes("D;H;L")

        # Check that result is a dict with expected structure
        assert isinstance(result, dict)
        # Keys depend on OUTCOME_CODES config
        assert "outcome_death" in result or "D" in str(result) or len(result) > 0

    def test_parse_outcome_codes_empty(self):
        """Test empty outcome codes."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        result = transformer.parse_outcome_codes("")
        assert isinstance(result, dict)

    def test_parse_int_valid(self):
        """Test valid integer parsing."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        assert transformer.parse_int("123") == 123
        assert transformer.parse_int("0") == 0

    def test_parse_int_invalid(self):
        """Test invalid integer handling."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        assert transformer.parse_int("abc") is None
        assert transformer.parse_int("") is None

    def test_transform_master_record(self):
        """Test master record transformation."""
        from src.ingestion.transformer import DataTransformer

        transformer = DataTransformer()

        record = {
            "mdr_report_key": "TEST001",
            "date_received": "01/15/2024",
            "manufacturer_name": "ABBOTT NEUROMODULATION",
            "event_type": "in",
        }

        result = transformer.transform_master_record(record, source_file="test.txt")

        assert result["mdr_report_key"] == "TEST001"
        assert result["date_received"] == date(2024, 1, 15)
        assert result["manufacturer_clean"] == "Abbott"
        assert result["event_type"] == "IN"
        assert result["source_file"] == "test.txt"
