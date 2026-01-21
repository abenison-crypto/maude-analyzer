"""Tests for data export module."""

import pytest
import pandas as pd
import io


class TestDataExporter:
    """Tests for DataExporter class."""

    def test_exporter_initialization(self):
        """Test exporter initialization."""
        from src.analysis.export import DataExporter

        exporter = DataExporter()
        assert exporter is not None

    def test_export_to_csv_buffer(self, sample_master_data):
        """Test CSV export to buffer."""
        from src.analysis.export import DataExporter

        exporter = DataExporter()
        buffer = exporter.export_to_csv_buffer(sample_master_data)

        assert isinstance(buffer, io.StringIO)

        # Read back and verify
        buffer.seek(0)
        content = buffer.read()
        assert "mdr_report_key" in content
        assert "TEST001" in content

    def test_export_to_csv_buffer_with_columns(self, sample_master_data):
        """Test CSV export with specific columns."""
        from src.analysis.export import DataExporter

        exporter = DataExporter()
        columns = ["mdr_report_key", "manufacturer_clean"]
        buffer = exporter.export_to_csv_buffer(sample_master_data, columns=columns)

        buffer.seek(0)
        content = buffer.read()
        assert "mdr_report_key" in content
        assert "manufacturer_clean" in content
        # Should not include other columns
        lines = content.strip().split("\n")
        assert len(lines[0].split(",")) == 2

    def test_export_to_excel_buffer(self, sample_master_data):
        """Test Excel export to buffer."""
        from src.analysis.export import DataExporter

        exporter = DataExporter()
        buffer = exporter.export_to_excel_buffer(sample_master_data)

        assert isinstance(buffer, io.BytesIO)
        assert buffer.getbuffer().nbytes > 0

    def test_export_to_excel_with_summary(self, sample_master_data):
        """Test Excel export with summary sheet."""
        from src.analysis.export import DataExporter

        exporter = DataExporter()
        buffer = exporter.export_to_excel_buffer(
            sample_master_data,
            include_summary_sheet=True
        )

        assert isinstance(buffer, io.BytesIO)
        assert buffer.getbuffer().nbytes > 0

    def test_export_empty_dataframe(self):
        """Test export of empty DataFrame."""
        from src.analysis.export import DataExporter

        exporter = DataExporter()
        empty_df = pd.DataFrame()

        buffer = exporter.export_to_csv_buffer(empty_df)
        buffer.seek(0)
        content = buffer.read()

        # Should still produce valid (empty) CSV
        assert content == "" or content == "\n"
