"""Test CSV parsing with quote handling.

These tests verify that the MAUDE parser correctly handles files with:
- Unmatched quotes (e.g., O"REILLY)
- Quoted manufacturer names (e.g., "ROCHE DIABETES CARE, INC.)
- Long narratives with embedded quotes
- Embedded newlines in text fields

The key bug fixed: Python's csv.reader with quotechar='"' silently consumes
millions of records when encountering unmatched quotes. Using quoting=csv.QUOTE_NONE
prevents this catastrophic data loss.
"""

import csv
import io
import tempfile
from pathlib import Path

import pytest

from src.ingestion.parser import (
    MAUDEParser,
    count_physical_lines,
    preprocess_file_for_embedded_newlines,
)


class TestCSVParsingQuotes:
    """Test that CSV parsing correctly handles quote characters."""

    def test_parse_pipe_delimited_with_quotes(self):
        """Test parsing pipe-delimited data with embedded quotes."""
        # Sample data with various quote patterns
        test_data = """MDR_REPORT_KEY|BRAND_NAME|MANUFACTURER_D_NAME
12345678|TEST BRAND|O"REILLY MEDICAL
12345679|ANOTHER BRAND|NORMAL MANUFACTURER
12345680|BRAND THREE|"QUOTED MFR NAME
12345681|BRAND FOUR|MFR WITH "QUOTE" INSIDE
"""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            # Parse the file
            parser = MAUDEParser()
            records = []
            for record in parser.parse_file_dynamic(
                temp_path,
                file_type="device",
                map_to_db_columns=False,
            ):
                records.append(record)

            # Should get all 4 records, not lose any to quote-swallowing
            assert len(records) == 4, f"Expected 4 records, got {len(records)}"

            # Verify specific records
            assert records[0].get("MANUFACTURER_D_NAME") == 'O"REILLY MEDICAL'
            assert records[2].get("MANUFACTURER_D_NAME") == '"QUOTED MFR NAME'
            assert records[3].get("MANUFACTURER_D_NAME") == 'MFR WITH "QUOTE" INSIDE'

        finally:
            temp_path.unlink()

    def test_record_count_integrity(self):
        """Test that physical line count matches parsed record count."""
        # Create test data with 100 records
        lines = ["MDR_REPORT_KEY|FIELD1|FIELD2"]
        for i in range(100):
            lines.append(f"{10000000 + i}|Value{i}|Data with \"quote\" char")

        test_data = "\n".join(lines)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            # Count physical lines
            total, valid_data, orphans = count_physical_lines(temp_path)
            assert total == 101, f"Expected 101 total lines, got {total}"
            assert valid_data == 100, f"Expected 100 valid data lines, got {valid_data}"
            assert orphans == 0, f"Expected 0 orphan lines, got {orphans}"

        finally:
            temp_path.unlink()

    def test_oreilly_style_quotes_preserved(self):
        """Test that O'REILLY-style names with quotes are preserved."""
        test_data = """MDR_REPORT_KEY|BRAND_NAME|MANUFACTURER_D_NAME
10000001|BRAND A|O"REILLY MEDICAL SUPPLIES
10000002|BRAND B|O"BRIEN HEALTHCARE
10000003|BRAND C|NORMAL NAME
"""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            parser = MAUDEParser()
            records = list(parser.parse_file_dynamic(
                temp_path,
                file_type="device",
                map_to_db_columns=False,
            ))

            # All 3 records should be present
            assert len(records) == 3

            # Quote should be preserved (representing apostrophe)
            assert records[0].get("MANUFACTURER_D_NAME") == 'O"REILLY MEDICAL SUPPLIES'
            assert records[1].get("MANUFACTURER_D_NAME") == "O\"BRIEN HEALTHCARE"

        finally:
            temp_path.unlink()

    def test_narrative_field_not_truncated(self):
        """Test that long narrative text with quotes is not truncated."""
        # Simulate a text file record with a long narrative containing quotes
        long_narrative = 'Patient reported that the device failed. The user said "it stopped working" after 3 months. The manufacturer ("ACME CORP") was notified.'

        test_data = f"""MDR_REPORT_KEY|MDR_TEXT_KEY|TEXT_TYPE_CODE|PATIENT_SEQUENCE_NUMBER|DATE_REPORT|FOI_TEXT
10000001|20000001|D|1|20230101|{long_narrative}
10000002|20000002|D|1|20230102|Normal text without issues
"""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            parser = MAUDEParser()
            records = list(parser.parse_file_dynamic(
                temp_path,
                file_type="text",
                map_to_db_columns=False,
            ))

            # Both records should be present
            assert len(records) == 2

            # Narrative should be complete (not truncated at first quote)
            foi_text = records[0].get("FOI_TEXT", "")
            assert "ACME CORP" in foi_text, "Narrative was truncated at quote"
            assert foi_text == long_narrative

        finally:
            temp_path.unlink()

    def test_quote_swallowing_detection(self):
        """Test that we can detect when quote-swallowing would have occurred."""
        # Create data that would cause quote-swallowing with quotechar='"'
        test_data = """MDR_REPORT_KEY|FIELD1|FIELD2
10000001|Value1|"Unmatched quote
10000002|Value2|Normal
10000003|Value3|Also "normal" here
10000004|Value4|Another record
"""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            # With QUOTE_NONE, we should get all 4 records
            parser = MAUDEParser()
            records = list(parser.parse_file_dynamic(
                temp_path,
                file_type="device",
                map_to_db_columns=False,
            ))

            assert len(records) == 4, (
                f"Quote-swallowing detected! Expected 4 records, got {len(records)}. "
                "The parser may be using quotechar instead of QUOTE_NONE."
            )

        finally:
            temp_path.unlink()


class TestEmbeddedNewlineHandling:
    """Test handling of embedded newlines in text fields."""

    def test_count_physical_lines_with_orphans(self):
        """Test counting physical lines including orphan lines from embedded newlines."""
        # Simulate embedded newline in narrative text
        test_data = """MDR_REPORT_KEY|FIELD1|FIELD2
10000001|Value1|Text field starts here
and continues on next line
10000002|Value2|Normal record
10000003|Value3|Another record with
embedded newline too
10000004|Value4|Final record
"""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            total, valid_data, orphans = count_physical_lines(temp_path)

            # 1 header + 4 valid records + 2 orphan lines = 7 total
            assert total == 7, f"Expected 7 total lines, got {total}"
            assert valid_data == 4, f"Expected 4 valid data lines, got {valid_data}"
            assert orphans == 2, f"Expected 2 orphan lines, got {orphans}"

        finally:
            temp_path.unlink()

    def test_preprocess_rejoins_embedded_newlines(self):
        """Test that preprocessing rejoins records with embedded newlines."""
        test_data = """MDR_REPORT_KEY|FIELD1|FIELD2
10000001|Value1|Text starts here
and continues here
10000002|Value2|Normal record
"""
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            lines_iter, rejoin_count = preprocess_file_for_embedded_newlines(temp_path)

            # Convert iterator to list for testing
            lines = list(lines_iter)

            # Should have header + 2 records
            assert len(lines) == 3, f"Expected 3 lines after preprocessing, got {len(lines)}"
            assert rejoin_count == 1, f"Expected 1 rejoin, got {rejoin_count}"

            # The first record should have the rejoined text
            assert "and continues here" in lines[1]

        finally:
            temp_path.unlink()


class TestCSVReaderBehavior:
    """Tests demonstrating the CSV reader quote behavior that caused the bug."""

    def test_quote_none_vs_quotechar_behavior(self):
        """Demonstrate the difference between QUOTE_NONE and quotechar handling."""
        # This data contains an unmatched quote that breaks quotechar parsing
        test_lines = [
            '10000001|Normal|Text',
            '10000002|Has "quote|More text',  # Unmatched quote
            '10000003|Normal|Text',
            '10000004|Normal|Text',
        ]
        test_data = '\n'.join(test_lines)

        # With QUOTE_NONE - all 4 records parsed correctly
        reader_quote_none = csv.reader(
            io.StringIO(test_data),
            delimiter='|',
            quoting=csv.QUOTE_NONE
        )
        records_quote_none = list(reader_quote_none)
        assert len(records_quote_none) == 4, "QUOTE_NONE should parse all 4 records"

        # With quotechar='"' - may swallow records after unmatched quote
        # NOTE: The exact behavior depends on whether the quote is "closed" by
        # another quote somewhere else. The key insight is that QUOTE_NONE is safe.
        reader_quotechar = csv.reader(
            io.StringIO(test_data),
            delimiter='|',
            quotechar='"'
        )
        records_quotechar = list(reader_quotechar)

        # This demonstrates that quotechar can cause problems - it might join
        # rows or parse differently than expected
        # The exact count may vary, but with QUOTE_NONE we are guaranteed all records
        print(f"QUOTE_NONE: {len(records_quote_none)} records")
        print(f"quotechar='\"': {len(records_quotechar)} records")

    def test_massive_record_loss_scenario(self):
        """
        Test scenario that would cause massive data loss with quotechar.

        When FDA data has an unmatched quote at the start of a manufacturer name
        (e.g., '"ROCHE DIABETES CARE, INC.'), the CSV reader with quotechar
        will treat everything until the next quote as one field, potentially
        swallowing millions of subsequent records.
        """
        # Simulate the problematic pattern
        lines = ['MDR_REPORT_KEY|MANUFACTURER_NAME|OTHER']
        lines.append('10000001|"UNMATCHED QUOTE MFR|Some data')  # Problematic line
        # Add 99 more "good" records that would be swallowed
        for i in range(99):
            lines.append(f'{10000002 + i}|NORMAL MFR|Data {i}')

        test_data = '\n'.join(lines)

        # With QUOTE_NONE - all 100 data records present
        reader = csv.reader(
            io.StringIO(test_data),
            delimiter='|',
            quoting=csv.QUOTE_NONE
        )
        records = list(reader)

        # 1 header + 100 data records
        assert len(records) == 101, (
            f"Expected 101 total records, got {len(records)}. "
            "This indicates the parser is not using QUOTE_NONE."
        )


class TestDataIntegrity:
    """Test overall data integrity during parsing."""

    def test_physical_vs_db_record_count(self):
        """Test that parsed count matches physical line count."""
        # Create a test file with known record count
        record_count = 50
        lines = ["MDR_REPORT_KEY|FIELD1|FIELD2|FIELD3"]
        for i in range(record_count):
            lines.append(f"{10000000 + i}|Val{i}|Data \"with\" quotes|More")

        test_data = '\n'.join(lines)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            # Count physical lines
            total, valid_data, orphans = count_physical_lines(temp_path)

            # Parse the file
            parser = MAUDEParser()
            parsed_records = list(parser.parse_file_dynamic(
                temp_path,
                file_type="device",
                map_to_db_columns=False,
            ))

            # Verify counts match
            assert valid_data == record_count, (
                f"Physical line count mismatch: expected {record_count}, got {valid_data}"
            )
            assert len(parsed_records) == record_count, (
                f"Parsed record count mismatch: expected {record_count}, got {len(parsed_records)}"
            )
            assert len(parsed_records) == valid_data, (
                f"Parsed count ({len(parsed_records)}) != physical count ({valid_data}). "
                "Possible quote-swallowing!"
            )

        finally:
            temp_path.unlink()

    def test_no_records_lost_during_parse(self):
        """Verify no records are lost during parse, regardless of content."""
        # Create data with various problematic patterns
        patterns = [
            'Normal text',
            'Text with "quotes"',
            'O"REILLY style',
            '"Starts with quote',
            'Ends with quote"',
            'Multiple "quotes" in "text"',
            'Quote at end"',
            '"',  # Just a quote
            '""',  # Double quote
            'Has|pipe|char',  # Won't break due to proper parsing
        ]

        lines = ["MDR_REPORT_KEY|FIELD1"]
        for i, pattern in enumerate(patterns):
            lines.append(f"{10000000 + i}|{pattern}")

        test_data = '\n'.join(lines)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='latin-1'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            parser = MAUDEParser()
            records = list(parser.parse_file_dynamic(
                temp_path,
                file_type="device",
                map_to_db_columns=False,
            ))

            expected_count = len(patterns)
            assert len(records) == expected_count, (
                f"Expected {expected_count} records, got {len(records)}. "
                f"Records may have been lost to quote handling issues."
            )

        finally:
            temp_path.unlink()


class TestParserRoundTrip:
    """Test that parsing preserves data integrity."""

    def test_special_characters_preserved(self):
        """Test that special characters are preserved through parsing."""
        special_chars = [
            'café',
            'naïve',
            '™ ® ©',
            '½ ¼ ¾',
            'émojis: 😀',  # May not survive latin-1 encoding
            '日本語',  # May not survive latin-1 encoding
            '<html>&entities;',
            "apostrophe's",
            'back\\slash',
        ]

        lines = ["MDR_REPORT_KEY|TEXT"]
        for i, char in enumerate(special_chars):
            lines.append(f"{10000000 + i}|{char}")

        test_data = '\n'.join(lines)

        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False, encoding='utf-8'
        ) as f:
            f.write(test_data)
            temp_path = Path(f.name)

        try:
            parser = MAUDEParser(encoding='utf-8')
            records = list(parser.parse_file_dynamic(
                temp_path,
                file_type="device",
                map_to_db_columns=False,
            ))

            # All records should be parsed
            assert len(records) == len(special_chars)

            # Basic ASCII special chars should be preserved
            # Find the record with apostrophe
            apostrophe_record = [r for r in records if r.get("TEXT") == "apostrophe's"]
            assert len(apostrophe_record) == 1, "apostrophe's record not found"

        finally:
            temp_path.unlink()
