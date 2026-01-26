"""
Pre-load validation for FDA MAUDE files.

This module validates files before loading into the database:
- Column structure validation
- Date format validation
- Required field checking
- Data type validation
- Encoding detection

Usage:
    from src.ingestion.validators import FileValidator

    validator = FileValidator()
    result = validator.validate_device_file(filepath)
    if not result.is_valid:
        print(f"Validation failed: {result.errors}")
"""

import csv
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime
import chardet

import sys
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger
from config.schema_registry import (
    get_fda_columns,
    get_expected_column_count,
    get_alternative_column_counts,
    is_headerless_file,
    DATE_COLUMNS,
    INTEGER_COLUMNS,
    FLAG_COLUMNS,
)

logger = get_logger("validators")


@dataclass
class ValidationResult:
    """Result of a file validation."""
    filepath: str
    file_type: str
    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    stats: Dict = field(default_factory=dict)

    def add_error(self, message: str) -> None:
        """Add an error and mark as invalid."""
        self.errors.append(message)
        self.is_valid = False

    def add_warning(self, message: str) -> None:
        """Add a warning (doesn't affect validity)."""
        self.warnings.append(message)


@dataclass
class ColumnValidation:
    """Result of column-level validation."""
    column_name: str
    expected_type: str
    null_count: int = 0
    invalid_count: int = 0
    sample_invalid: List[str] = field(default_factory=list)
    is_valid: bool = True


class FileValidator:
    """Validates FDA MAUDE files before loading."""

    # Date formats accepted by FDA files
    DATE_PATTERNS = [
        r"^\d{1,2}/\d{1,2}/\d{4}$",     # MM/DD/YYYY or M/D/YYYY
        r"^\d{4}-\d{2}-\d{2}$",          # YYYY-MM-DD
        r"^\d{8}$",                       # YYYYMMDD
        r"^\d{1,2}-\d{1,2}-\d{4}$",      # MM-DD-YYYY
        r"^\d{4}/\d{2}/\d{2}$",          # YYYY/MM/DD
    ]

    # Compiled regex patterns
    DATE_REGEXES = [re.compile(p) for p in DATE_PATTERNS]

    def __init__(
        self,
        encoding: str = "latin-1",
        sample_size: int = 1000,
    ):
        """
        Initialize the validator.

        Args:
            encoding: Default file encoding.
            sample_size: Number of rows to sample for validation.
        """
        self.encoding = encoding
        self.sample_size = sample_size

    def detect_encoding(self, filepath: Path) -> str:
        """
        Detect file encoding.

        Args:
            filepath: Path to file.

        Returns:
            Detected encoding string.
        """
        try:
            with open(filepath, "rb") as f:
                raw = f.read(10000)  # Read first 10KB
                result = chardet.detect(raw)
                return result.get("encoding", self.encoding) or self.encoding
        except Exception as e:
            logger.warning(f"Could not detect encoding: {e}")
            return self.encoding

    def validate_file_structure(
        self,
        filepath: Path,
        file_type: str,
    ) -> ValidationResult:
        """
        Validate basic file structure (columns, delimiter, etc).

        Args:
            filepath: Path to file.
            file_type: Type of MAUDE file.

        Returns:
            ValidationResult with structure validation.
        """
        result = ValidationResult(
            filepath=str(filepath),
            file_type=file_type,
        )

        if not filepath.exists():
            result.add_error(f"File not found: {filepath}")
            return result

        # Check file size
        file_size = filepath.stat().st_size
        result.stats["file_size_mb"] = round(file_size / 1024 / 1024, 2)

        if file_size == 0:
            result.add_error("File is empty")
            return result

        # Detect encoding
        detected_encoding = self.detect_encoding(filepath)
        result.stats["detected_encoding"] = detected_encoding

        try:
            with open(filepath, "r", encoding=detected_encoding, errors="replace") as f:
                # Read first line
                first_line = f.readline().strip()

                if not first_line:
                    result.add_error("File has no content")
                    return result

                # Detect delimiter
                pipe_count = first_line.count("|")
                comma_count = first_line.count(",")

                if pipe_count > comma_count:
                    delimiter = "|"
                else:
                    delimiter = ","

                result.stats["delimiter"] = delimiter

                # Parse first line
                parts = first_line.split(delimiter)
                detected_columns = len(parts)
                result.stats["detected_columns"] = detected_columns

                # Check against expected
                expected_counts = get_alternative_column_counts(file_type)
                if not expected_counts:
                    expected_counts = [get_expected_column_count(file_type)]

                if detected_columns not in expected_counts and abs(detected_columns - expected_counts[0]) > 2:
                    result.add_warning(
                        f"Column count {detected_columns} differs from expected {expected_counts}"
                    )

                # Check if header present
                has_header = not is_headerless_file(file_type)
                if has_header:
                    first_val = parts[0].strip().upper()
                    # Headers usually contain KEY, REPORT, DATE, etc.
                    header_indicators = ["KEY", "REPORT", "DATE", "NAME", "CODE"]
                    if not any(ind in first_val for ind in header_indicators):
                        if first_val.isdigit():
                            result.add_warning("First row appears to be data, not header")
                            has_header = False

                result.stats["has_header"] = has_header

                # Count total rows (sample)
                row_count = 1  # Already read first line
                for _ in f:
                    row_count += 1
                    if row_count > 1000000:
                        result.stats["row_count"] = "1M+"
                        break
                else:
                    result.stats["row_count"] = row_count

        except Exception as e:
            result.add_error(f"Error reading file: {e}")

        return result

    def validate_device_file(self, filepath: Path) -> ValidationResult:
        """
        Validate a device file.

        Device files are critical for manufacturer data.

        Args:
            filepath: Path to device file.

        Returns:
            ValidationResult with device-specific validation.
        """
        result = self.validate_file_structure(filepath, "device")

        if not result.is_valid:
            return result

        # Device-specific validations
        try:
            encoding = result.stats.get("detected_encoding", self.encoding)
            delimiter = result.stats.get("delimiter", "|")

            with open(filepath, "r", encoding=encoding, errors="replace") as f:
                reader = csv.reader(f, delimiter=delimiter)

                # Get header
                header = next(reader)
                header_upper = [col.strip().upper() for col in header]

                # Check for critical columns
                critical_columns = [
                    "MDR_REPORT_KEY",
                    "MANUFACTURER_D_NAME",
                    "DEVICE_REPORT_PRODUCT_CODE",
                ]

                for col in critical_columns:
                    if col not in header_upper:
                        result.add_warning(f"Missing expected column: {col}")

                # Find column indices
                mdr_key_idx = header_upper.index("MDR_REPORT_KEY") if "MDR_REPORT_KEY" in header_upper else 0
                mfr_idx = header_upper.index("MANUFACTURER_D_NAME") if "MANUFACTURER_D_NAME" in header_upper else -1
                product_idx = header_upper.index("DEVICE_REPORT_PRODUCT_CODE") if "DEVICE_REPORT_PRODUCT_CODE" in header_upper else -1

                # Sample validation
                sample_count = 0
                null_mdr_keys = 0
                null_manufacturers = 0
                null_product_codes = 0
                invalid_mdr_keys = 0

                for row in reader:
                    sample_count += 1

                    # Validate MDR_REPORT_KEY
                    mdr_key = row[mdr_key_idx].strip() if mdr_key_idx < len(row) else ""
                    if not mdr_key:
                        null_mdr_keys += 1
                    elif not mdr_key.isdigit():
                        invalid_mdr_keys += 1

                    # Check manufacturer
                    if mfr_idx >= 0 and mfr_idx < len(row):
                        if not row[mfr_idx].strip():
                            null_manufacturers += 1

                    # Check product code
                    if product_idx >= 0 and product_idx < len(row):
                        if not row[product_idx].strip():
                            null_product_codes += 1

                    if sample_count >= self.sample_size:
                        break

                result.stats["sample_count"] = sample_count
                result.stats["null_mdr_keys"] = null_mdr_keys
                result.stats["invalid_mdr_keys"] = invalid_mdr_keys
                result.stats["null_manufacturers"] = null_manufacturers
                result.stats["null_product_codes"] = null_product_codes

                # Evaluate results
                if sample_count > 0:
                    null_mdr_pct = (null_mdr_keys / sample_count) * 100
                    if null_mdr_pct > 5:
                        result.add_error(
                            f"{null_mdr_pct:.1f}% of sample has NULL MDR_REPORT_KEY"
                        )

                    invalid_mdr_pct = (invalid_mdr_keys / sample_count) * 100
                    if invalid_mdr_pct > 5:
                        result.add_warning(
                            f"{invalid_mdr_pct:.1f}% of sample has invalid MDR_REPORT_KEY"
                        )

                    null_mfr_pct = (null_manufacturers / sample_count) * 100
                    result.stats["manufacturer_null_pct"] = round(null_mfr_pct, 1)

        except Exception as e:
            result.add_error(f"Validation error: {e}")

        return result

    def validate_master_file(self, filepath: Path) -> ValidationResult:
        """
        Validate a master (MDR) file.

        Args:
            filepath: Path to master file.

        Returns:
            ValidationResult with master-specific validation.
        """
        result = self.validate_file_structure(filepath, "master")

        if not result.is_valid:
            return result

        try:
            encoding = result.stats.get("detected_encoding", self.encoding)
            delimiter = result.stats.get("delimiter", "|")

            with open(filepath, "r", encoding=encoding, errors="replace") as f:
                reader = csv.reader(f, delimiter=delimiter)

                header = next(reader)
                header_upper = [col.strip().upper() for col in header]

                # Check for key columns
                key_columns = [
                    "MDR_REPORT_KEY",
                    "DATE_RECEIVED",
                    "EVENT_TYPE",
                ]

                for col in key_columns:
                    if col not in header_upper:
                        result.add_warning(f"Missing expected column: {col}")

                # Find indices
                mdr_key_idx = header_upper.index("MDR_REPORT_KEY") if "MDR_REPORT_KEY" in header_upper else 0
                date_idx = header_upper.index("DATE_RECEIVED") if "DATE_RECEIVED" in header_upper else -1
                event_type_idx = header_upper.index("EVENT_TYPE") if "EVENT_TYPE" in header_upper else -1

                # Sample validation
                sample_count = 0
                seen_keys: Set[str] = set()
                duplicate_keys = 0
                null_dates = 0
                invalid_dates = 0
                event_types: Dict[str, int] = {}

                for row in reader:
                    sample_count += 1

                    # Check MDR_REPORT_KEY uniqueness
                    mdr_key = row[mdr_key_idx].strip() if mdr_key_idx < len(row) else ""
                    if mdr_key in seen_keys:
                        duplicate_keys += 1
                    seen_keys.add(mdr_key)

                    # Validate date
                    if date_idx >= 0 and date_idx < len(row):
                        date_val = row[date_idx].strip()
                        if not date_val:
                            null_dates += 1
                        elif not self._is_valid_date(date_val):
                            invalid_dates += 1

                    # Count event types
                    if event_type_idx >= 0 and event_type_idx < len(row):
                        event_type = row[event_type_idx].strip() or "NULL"
                        event_types[event_type] = event_types.get(event_type, 0) + 1

                    if sample_count >= self.sample_size:
                        break

                result.stats["sample_count"] = sample_count
                result.stats["duplicate_keys"] = duplicate_keys
                result.stats["null_dates"] = null_dates
                result.stats["invalid_dates"] = invalid_dates
                result.stats["event_type_distribution"] = event_types

                # Evaluate results
                if sample_count > 0:
                    if duplicate_keys > 0:
                        result.add_warning(
                            f"{duplicate_keys} duplicate MDR_REPORT_KEY in sample"
                        )

                    null_date_pct = (null_dates / sample_count) * 100
                    if null_date_pct > 10:
                        result.add_warning(
                            f"{null_date_pct:.1f}% of sample has NULL date_received"
                        )

        except Exception as e:
            result.add_error(f"Validation error: {e}")

        return result

    def validate_text_file(self, filepath: Path) -> ValidationResult:
        """
        Validate a text (narrative) file.

        Args:
            filepath: Path to text file.

        Returns:
            ValidationResult with text-specific validation.
        """
        result = self.validate_file_structure(filepath, "text")

        if not result.is_valid:
            return result

        try:
            encoding = result.stats.get("detected_encoding", self.encoding)
            delimiter = result.stats.get("delimiter", "|")

            with open(filepath, "r", encoding=encoding, errors="replace") as f:
                reader = csv.reader(f, delimiter=delimiter)

                header = next(reader)

                # Sample validation
                sample_count = 0
                text_type_counts: Dict[str, int] = {}
                null_text = 0
                avg_text_length = 0
                max_text_length = 0

                for row in reader:
                    sample_count += 1

                    # Get text type (column 3)
                    if len(row) > 2:
                        text_type = row[2].strip() or "NULL"
                        text_type_counts[text_type] = text_type_counts.get(text_type, 0) + 1

                    # Get text content (column 6)
                    if len(row) > 5:
                        text_content = row[5].strip() if row[5] else ""
                        if not text_content:
                            null_text += 1
                        else:
                            text_len = len(text_content)
                            avg_text_length += text_len
                            max_text_length = max(max_text_length, text_len)

                    if sample_count >= self.sample_size:
                        break

                result.stats["sample_count"] = sample_count
                result.stats["text_type_distribution"] = text_type_counts
                result.stats["null_text_count"] = null_text

                if sample_count > null_text:
                    result.stats["avg_text_length"] = round(
                        avg_text_length / (sample_count - null_text)
                    )
                    result.stats["max_text_length"] = max_text_length

        except Exception as e:
            result.add_error(f"Validation error: {e}")

        return result

    def _is_valid_date(self, value: str) -> bool:
        """
        Check if a value looks like a valid date.

        Args:
            value: String to check.

        Returns:
            True if looks like a date.
        """
        if not value:
            return False

        for regex in self.DATE_REGEXES:
            if regex.match(value):
                return True

        return False

    def validate_file(
        self,
        filepath: Path,
        file_type: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate any MAUDE file.

        Args:
            filepath: Path to file.
            file_type: Type of file (auto-detected if None).

        Returns:
            ValidationResult with validation results.
        """
        # Auto-detect file type if needed
        if file_type is None:
            filename = filepath.name.lower()
            if "mdrfoi" in filename and "problem" not in filename:
                file_type = "master"
            elif ("foidev" in filename or filename.startswith("device")) and "problem" not in filename:
                file_type = "device"
            elif filename.startswith("patient") and "problem" not in filename:
                file_type = "patient"
            elif "foitext" in filename:
                file_type = "text"
            else:
                file_type = "unknown"

        # Route to appropriate validator
        if file_type == "device":
            return self.validate_device_file(filepath)
        elif file_type == "master":
            return self.validate_master_file(filepath)
        elif file_type == "text":
            return self.validate_text_file(filepath)
        else:
            return self.validate_file_structure(filepath, file_type)


def validate_all_files(
    data_dir: Path,
    file_types: Optional[List[str]] = None,
) -> Dict[str, List[ValidationResult]]:
    """
    Validate all files in a directory.

    Args:
        data_dir: Directory containing files.
        file_types: Types to validate (None = all).

    Returns:
        Dictionary mapping file type to list of results.
    """
    validator = FileValidator()
    results: Dict[str, List[ValidationResult]] = {}

    patterns = {
        "master": "mdrfoi*.txt",
        "device": ["foidev*.txt", "device*.txt"],
        "patient": "patient*.txt",
        "text": "foitext*.txt",
    }

    for file_type, pattern in patterns.items():
        if file_types and file_type not in file_types:
            continue

        results[file_type] = []

        # Handle multiple patterns
        if isinstance(pattern, list):
            files = []
            for p in pattern:
                files.extend(data_dir.glob(p))
        else:
            files = list(data_dir.glob(pattern))

        # Exclude problem files from device
        if file_type == "device":
            files = [f for f in files if "problem" not in f.name.lower()]

        for filepath in sorted(files):
            result = validator.validate_file(filepath, file_type)
            results[file_type].append(result)
            logger.info(
                f"Validated {filepath.name}: {'PASS' if result.is_valid else 'FAIL'}"
            )

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Validate MAUDE files")
    parser.add_argument("file", type=Path, help="File to validate")
    parser.add_argument("--type", help="File type (auto-detected if not specified)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    validator = FileValidator()
    result = validator.validate_file(args.file, args.type)

    print(f"\nValidation Result: {'PASS' if result.is_valid else 'FAIL'}")
    print(f"File: {result.filepath}")
    print(f"Type: {result.file_type}")

    print("\nStats:")
    for key, value in result.stats.items():
        print(f"  {key}: {value}")

    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"  - {error}")

    if result.warnings:
        print("\nWarnings:")
        for warning in result.warnings:
            print(f"  - {warning}")
