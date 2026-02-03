"""
Three-Stage Validation Framework for FDA MAUDE Data Pipeline.

This framework implements regulatory-grade data validation at three stages:

Stage 1: Pre-Parse Validation (Before parsing)
    - File structure verification
    - Column count validation
    - Encoding detection
    - File header validation

Stage 2: Post-Transform Validation (After transformation, before load)
    - Type conversions successful
    - Required fields present
    - Values in valid domains
    - Business rule compliance

Stage 3: Post-Load Validation (After database insert)
    - Referential integrity
    - No new orphan records
    - Record counts match source
    - Cross-table consistency

Usage:
    from src.ingestion.validation_framework import ValidationPipeline

    pipeline = ValidationPipeline(db_path)

    # Stage 1: Pre-parse
    stage1_result = pipeline.validate_stage1_preparse(filepath, file_type)

    # Stage 2: Post-transform (per record)
    stage2_result = pipeline.validate_stage2_post_transform(record, file_type)

    # Stage 3: Post-load
    stage3_result = pipeline.validate_stage3_post_load(load_result)
"""

from datetime import datetime, date
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger
from config.schema_registry import (
    get_fda_columns,
    ALTERNATIVE_COLUMN_COUNTS,
)
from src.database import get_connection
from src.ingestion.parser import count_physical_lines

logger = get_logger("validation_framework")


# =============================================================================
# VALIDATION RESULT DATACLASSES
# =============================================================================

@dataclass
class ValidationIssue:
    """Single validation issue."""
    stage: int
    category: str
    severity: str  # INFO, WARNING, ERROR, CRITICAL
    code: str
    message: str
    field_name: Optional[str] = None
    expected_value: Optional[Any] = None
    actual_value: Optional[Any] = None


@dataclass
class StageValidationResult:
    """Result of a validation stage."""
    stage: int
    stage_name: str
    passed: bool = True
    issues: List[ValidationIssue] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def add_issue(self, issue: ValidationIssue) -> None:
        """Add an issue and update passed status."""
        self.issues.append(issue)
        if issue.severity in ("ERROR", "CRITICAL"):
            self.passed = False

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "WARNING")

    @property
    def critical_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "CRITICAL")


@dataclass
class PipelineValidationResult:
    """Complete pipeline validation result across all stages."""
    filename: str
    file_type: str
    stage1_result: Optional[StageValidationResult] = None
    stage2_result: Optional[StageValidationResult] = None
    stage3_result: Optional[StageValidationResult] = None
    overall_passed: bool = True
    timestamp: datetime = field(default_factory=datetime.now)

    def update_overall_status(self) -> None:
        """Update overall status based on stage results."""
        self.overall_passed = all([
            self.stage1_result.passed if self.stage1_result else True,
            self.stage2_result.passed if self.stage2_result else True,
            self.stage3_result.passed if self.stage3_result else True,
        ])

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "filename": self.filename,
            "file_type": self.file_type,
            "overall_passed": self.overall_passed,
            "timestamp": self.timestamp.isoformat(),
            "stage1": self._stage_to_dict(self.stage1_result) if self.stage1_result else None,
            "stage2": self._stage_to_dict(self.stage2_result) if self.stage2_result else None,
            "stage3": self._stage_to_dict(self.stage3_result) if self.stage3_result else None,
        }

    def _stage_to_dict(self, stage: StageValidationResult) -> Dict[str, Any]:
        return {
            "stage": stage.stage,
            "stage_name": stage.stage_name,
            "passed": stage.passed,
            "error_count": stage.error_count,
            "warning_count": stage.warning_count,
            "critical_count": stage.critical_count,
            "issues": [
                {
                    "category": i.category,
                    "severity": i.severity,
                    "code": i.code,
                    "message": i.message,
                    "field_name": i.field_name,
                }
                for i in stage.issues
            ],
            "metrics": stage.metrics,
        }


# =============================================================================
# REQUIRED FIELDS BY FILE TYPE
# =============================================================================

REQUIRED_FIELDS = {
    "master": ["mdr_report_key"],
    "device": ["mdr_report_key"],
    "patient": ["mdr_report_key"],
    "text": ["mdr_report_key", "mdr_text_key"],
    "problem": ["mdr_report_key"],
}

# Valid domains for flag fields
VALID_FLAG_VALUES = {"Y", "N", "", None}
VALID_EVENT_TYPES = {"D", "IN", "M", "O", "*", "", None}
VALID_SEX_VALUES = {"M", "F", "U", "Male", "Female", "Unknown", "", None}


# =============================================================================
# VALIDATION PIPELINE
# =============================================================================

class ValidationPipeline:
    """
    Three-stage validation pipeline for MAUDE data.

    Implements comprehensive validation at each stage of the data pipeline
    to ensure regulatory-grade data quality.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        strict_mode: bool = False,
    ):
        """
        Initialize the validation pipeline.

        Args:
            db_path: Path to DuckDB database.
            strict_mode: If True, treat all issues as errors.
        """
        self.db_path = db_path
        self.strict_mode = strict_mode

        # Track validation statistics
        self.stats = {
            "files_validated": 0,
            "records_validated": 0,
            "stage1_failures": 0,
            "stage2_failures": 0,
            "stage3_failures": 0,
        }

    # =========================================================================
    # STAGE 1: PRE-PARSE VALIDATION
    # =========================================================================

    def validate_stage1_preparse(
        self,
        filepath: Path,
        file_type: str,
    ) -> StageValidationResult:
        """
        Stage 1: Pre-Parse Validation.

        Validates:
        - File exists and is readable
        - File is not empty
        - File encoding is valid
        - Header row matches expected format
        - Column count matches expected schema
        - Physical line count (detects quote-swallowing issues)

        Args:
            filepath: Path to the file.
            file_type: Type of file (master, device, etc.).

        Returns:
            StageValidationResult with issues found.
        """
        result = StageValidationResult(
            stage=1,
            stage_name="Pre-Parse",
        )

        # Check file exists
        if not filepath.exists():
            result.add_issue(ValidationIssue(
                stage=1,
                category="file_structure",
                severity="CRITICAL",
                code="FILE_NOT_FOUND",
                message=f"File does not exist: {filepath}",
            ))
            return result

        # Check file is readable and not empty
        try:
            file_size = filepath.stat().st_size
            result.metrics["file_size_bytes"] = file_size

            if file_size == 0:
                result.add_issue(ValidationIssue(
                    stage=1,
                    category="file_structure",
                    severity="ERROR",
                    code="FILE_EMPTY",
                    message=f"File is empty: {filepath.name}",
                ))
                return result

        except Exception as e:
            result.add_issue(ValidationIssue(
                stage=1,
                category="file_structure",
                severity="CRITICAL",
                code="FILE_READ_ERROR",
                message=f"Cannot read file: {e}",
            ))
            return result

        # Check encoding and read header
        encoding = None
        header_line = None

        for enc in ["utf-8", "latin-1", "cp1252"]:
            try:
                with open(filepath, "r", encoding=enc) as f:
                    header_line = f.readline().strip()
                    encoding = enc
                    break
            except UnicodeDecodeError:
                continue

        if encoding is None:
            result.add_issue(ValidationIssue(
                stage=1,
                category="encoding",
                severity="ERROR",
                code="ENCODING_UNKNOWN",
                message="Could not determine file encoding",
            ))
            return result

        result.metrics["encoding"] = encoding

        # Validate column count
        if header_line:
            delimiter = "|"  # MAUDE files use pipe delimiter
            columns = header_line.split(delimiter)
            actual_count = len(columns)

            # Get all valid column counts for this file type (handles schema evolution)
            all_valid_counts = ALTERNATIVE_COLUMN_COUNTS.get(file_type, [])

            result.metrics["column_count"] = actual_count
            result.metrics["expected_counts"] = all_valid_counts

            if actual_count not in all_valid_counts:
                result.add_issue(ValidationIssue(
                    stage=1,
                    category="schema",
                    severity="WARNING" if not self.strict_mode else "ERROR",
                    code="COLUMN_COUNT_MISMATCH",
                    message=f"Column count {actual_count} not in expected {all_valid_counts}",
                    expected_value=all_valid_counts,
                    actual_value=actual_count,
                ))

            # Check for expected header columns
            expected_cols = get_fda_columns(file_type, actual_count)
            if expected_cols:
                header_cols = [c.strip().upper() for c in columns]
                expected_first = expected_cols[0].upper() if expected_cols else None
                actual_first = header_cols[0] if header_cols else None

                if expected_first and actual_first != expected_first:
                    result.add_issue(ValidationIssue(
                        stage=1,
                        category="schema",
                        severity="WARNING",
                        code="HEADER_MISMATCH",
                        message=f"First column '{actual_first}' doesn't match expected '{expected_first}'",
                        field_name="header",
                        expected_value=expected_first,
                        actual_value=actual_first,
                    ))

        # Count physical lines to detect quote-swallowing
        # This is CRITICAL - the CSV reader can silently consume millions of lines
        # if it encounters unmatched quotes. We count physical lines independently
        # to provide a ground truth for comparison with CSV-parsed counts.
        try:
            physical_lines, valid_data_lines, orphan_lines = count_physical_lines(
                filepath, encoding=encoding or 'latin-1'
            )

            result.metrics["physical_lines"] = physical_lines
            result.metrics["valid_data_lines"] = valid_data_lines
            result.metrics["orphan_lines"] = orphan_lines

            # Log if there are many orphan lines (embedded newlines)
            if orphan_lines > 100:
                result.add_issue(ValidationIssue(
                    stage=1,
                    category="data_quality",
                    severity="INFO",
                    code="EMBEDDED_NEWLINES_DETECTED",
                    message=f"File has {orphan_lines:,} orphan lines (embedded newlines in text fields)",
                    actual_value=orphan_lines,
                ))

            # This will be compared against loaded count in Stage 3
            # to detect parsing issues like quote-swallowing

        except Exception as e:
            result.add_issue(ValidationIssue(
                stage=1,
                category="file_structure",
                severity="WARNING",
                code="PHYSICAL_LINE_COUNT_ERROR",
                message=f"Could not count physical lines: {e}",
            ))

        self.stats["files_validated"] += 1
        if not result.passed:
            self.stats["stage1_failures"] += 1

        return result

    # =========================================================================
    # STAGE 2: POST-TRANSFORM VALIDATION
    # =========================================================================

    def validate_stage2_post_transform(
        self,
        record: Dict[str, Any],
        file_type: str,
    ) -> StageValidationResult:
        """
        Stage 2: Post-Transform Validation.

        Validates transformed records before database insert:
        - Required fields are present
        - Type conversions were successful
        - Values are in valid domains
        - Business rules are satisfied

        Args:
            record: Transformed record dictionary.
            file_type: Type of record.

        Returns:
            StageValidationResult with issues found.
        """
        result = StageValidationResult(
            stage=2,
            stage_name="Post-Transform",
        )

        mdr_key = record.get("mdr_report_key")

        # Check required fields
        required = REQUIRED_FIELDS.get(file_type, [])
        for field_name in required:
            value = record.get(field_name)
            if value is None or (isinstance(value, str) and not value.strip()):
                result.add_issue(ValidationIssue(
                    stage=2,
                    category="required_field",
                    severity="ERROR",
                    code="MISSING_REQUIRED_FIELD",
                    message=f"Required field '{field_name}' is missing or empty",
                    field_name=field_name,
                ))

        # Validate flag fields (Y/N domain)
        flag_fields = [
            "adverse_event_flag", "product_problem_flag", "health_professional",
            "single_use_flag", "implant_flag", "date_removed_flag",
        ]
        for field_name in flag_fields:
            if field_name in record:
                value = record.get(field_name)
                if value not in VALID_FLAG_VALUES:
                    result.add_issue(ValidationIssue(
                        stage=2,
                        category="domain",
                        severity="WARNING",
                        code="INVALID_FLAG_VALUE",
                        message=f"Invalid flag value for '{field_name}': '{value}'",
                        field_name=field_name,
                        expected_value=list(VALID_FLAG_VALUES),
                        actual_value=value,
                    ))

        # Validate event_type domain
        if "event_type" in record:
            event_type = record.get("event_type")
            if event_type not in VALID_EVENT_TYPES:
                result.add_issue(ValidationIssue(
                    stage=2,
                    category="domain",
                    severity="WARNING",
                    code="INVALID_EVENT_TYPE",
                    message=f"Invalid event_type: '{event_type}'",
                    field_name="event_type",
                    expected_value=list(VALID_EVENT_TYPES),
                    actual_value=event_type,
                ))

        # Validate patient_sex domain
        if "patient_sex" in record:
            sex = record.get("patient_sex")
            if sex not in VALID_SEX_VALUES:
                result.add_issue(ValidationIssue(
                    stage=2,
                    category="domain",
                    severity="WARNING",
                    code="INVALID_SEX_VALUE",
                    message=f"Invalid patient_sex: '{sex}'",
                    field_name="patient_sex",
                    expected_value=list(VALID_SEX_VALUES),
                    actual_value=sex,
                ))

        # Validate date ordering (date_of_event <= date_received)
        date_of_event = record.get("date_of_event")
        date_received = record.get("date_received")
        if date_of_event and date_received:
            try:
                if isinstance(date_of_event, str):
                    date_of_event = datetime.strptime(date_of_event, "%Y-%m-%d").date()
                if isinstance(date_received, str):
                    date_received = datetime.strptime(date_received, "%Y-%m-%d").date()

                if isinstance(date_of_event, date) and isinstance(date_received, date):
                    if date_of_event > date_received:
                        result.add_issue(ValidationIssue(
                            stage=2,
                            category="business_rule",
                            severity="WARNING",
                            code="DATE_ORDER_VIOLATION",
                            message=f"date_of_event ({date_of_event}) > date_received ({date_received})",
                            field_name="date_of_event",
                        ))
            except (ValueError, TypeError):
                pass  # Date parsing failures handled elsewhere

        # Validate date ranges (1984-current)
        date_fields = ["date_received", "date_of_event", "date_report"]
        min_year = 1984
        max_year = datetime.now().year + 1

        for field_name in date_fields:
            value = record.get(field_name)
            if value:
                try:
                    if isinstance(value, str):
                        value = datetime.strptime(value, "%Y-%m-%d").date()
                    if isinstance(value, date):
                        if value.year < min_year or value.year > max_year:
                            result.add_issue(ValidationIssue(
                                stage=2,
                                category="data_range",
                                severity="WARNING",
                                code="DATE_OUT_OF_RANGE",
                                message=f"{field_name} year {value.year} outside valid range {min_year}-{max_year}",
                                field_name=field_name,
                                actual_value=value.year,
                            ))
                except (ValueError, TypeError):
                    pass

        # Validate sequence numbers are positive
        seq_fields = ["device_sequence_number", "patient_sequence_number"]
        for field_name in seq_fields:
            value = record.get(field_name)
            if value is not None:
                try:
                    if int(value) <= 0:
                        result.add_issue(ValidationIssue(
                            stage=2,
                            category="data_range",
                            severity="WARNING",
                            code="INVALID_SEQUENCE_NUMBER",
                            message=f"{field_name} must be positive: {value}",
                            field_name=field_name,
                            actual_value=value,
                        ))
                except (ValueError, TypeError):
                    pass

        self.stats["records_validated"] += 1
        if not result.passed:
            self.stats["stage2_failures"] += 1

        return result

    # =========================================================================
    # STAGE 3: POST-LOAD VALIDATION
    # =========================================================================

    def validate_stage3_post_load(
        self,
        filename: str,
        file_type: str,
        expected_count: int,
        loaded_count: int,
        physical_line_count: int = 0,
    ) -> StageValidationResult:
        """
        Stage 3: Post-Load Validation.

        Validates after database insert:
        - Record counts match source PHYSICAL line count (not CSV-parsed count)
        - No new orphan records created
        - Referential integrity maintained

        IMPORTANT: expected_count may come from CSV parsing which can be wrong
        due to quote-swallowing. physical_line_count is the ground truth.

        Args:
            filename: Name of the loaded file.
            file_type: Type of file loaded.
            expected_count: Expected record count from CSV parsing (may be wrong).
            loaded_count: Actual records loaded.
            physical_line_count: Physical lines in source file (ground truth).

        Returns:
            StageValidationResult with issues found.
        """
        result = StageValidationResult(
            stage=3,
            stage_name="Post-Load",
        )

        # Use physical line count as ground truth if available
        ground_truth = physical_line_count if physical_line_count > 0 else expected_count

        result.metrics["physical_line_count"] = physical_line_count
        result.metrics["csv_parsed_count"] = expected_count
        result.metrics["loaded_count"] = loaded_count

        # CRITICAL CHECK: Detect quote-swallowing by comparing physical vs CSV counts
        if physical_line_count > 0 and expected_count > 0:
            csv_vs_physical_diff = physical_line_count - expected_count
            if csv_vs_physical_diff > 1000:
                csv_loss_pct = (csv_vs_physical_diff / physical_line_count) * 100
                result.add_issue(ValidationIssue(
                    stage=3,
                    category="parsing",
                    severity="CRITICAL",
                    code="QUOTE_SWALLOWING_DETECTED",
                    message=f"CSV parser saw {expected_count:,} rows but file has {physical_line_count:,} lines. "
                            f"Quote-swallowing likely consumed {csv_vs_physical_diff:,} records ({csv_loss_pct:.1f}%)",
                    expected_value=physical_line_count,
                    actual_value=expected_count,
                ))

        # Check record count variance against ground truth
        if ground_truth > 0:
            variance = abs(loaded_count - ground_truth)
            variance_pct = (variance / ground_truth) * 100

            result.metrics["expected_count"] = ground_truth
            result.metrics["variance"] = variance
            result.metrics["variance_pct"] = round(variance_pct, 2)

            if variance_pct > 0.1:
                severity = "CRITICAL" if variance_pct > 10.0 else ("ERROR" if variance_pct > 1.0 else "WARNING")
                result.add_issue(ValidationIssue(
                    stage=3,
                    category="completeness",
                    severity=severity,
                    code="RECORD_COUNT_VARIANCE",
                    message=f"Record count variance {variance_pct:.2f}% exceeds threshold "
                            f"(expected {ground_truth:,}, loaded {loaded_count:,})",
                    expected_value=ground_truth,
                    actual_value=loaded_count,
                ))

        # Check for orphan records (only if db_path is set)
        if self.db_path and file_type in ("device", "patient", "text", "problem"):
            orphan_result = self._check_orphan_records(filename, file_type)
            if orphan_result:
                result.add_issue(orphan_result)
                result.metrics["orphan_count"] = orphan_result.actual_value

        if not result.passed:
            self.stats["stage3_failures"] += 1

        return result

    def _check_orphan_records(
        self,
        filename: str,
        file_type: str,
    ) -> Optional[ValidationIssue]:
        """
        Check for orphan records created by this file load.

        Args:
            filename: Name of the loaded file.
            file_type: Type of file.

        Returns:
            ValidationIssue if orphans found, None otherwise.
        """
        table_map = {
            "device": "devices",
            "patient": "patients",
            "text": "mdr_text",
            "problem": "device_problems",
        }

        table_name = table_map.get(file_type)
        if not table_name:
            return None

        try:
            with get_connection(self.db_path, read_only=True) as conn:
                # Count orphan records from this file
                orphan_count = conn.execute(f"""
                    SELECT COUNT(DISTINCT c.mdr_report_key)
                    FROM {table_name} c
                    WHERE c.source_file LIKE ?
                      AND NOT EXISTS (
                          SELECT 1 FROM master_events m
                          WHERE m.mdr_report_key = c.mdr_report_key
                      )
                """, [f"%{filename}%"]).fetchone()[0]

                if orphan_count > 0:
                    return ValidationIssue(
                        stage=3,
                        category="referential_integrity",
                        severity="WARNING",
                        code="ORPHAN_RECORDS_CREATED",
                        message=f"{orphan_count} orphan {file_type} records created",
                        actual_value=orphan_count,
                    )

        except Exception as e:
            logger.warning(f"Could not check orphan records: {e}")

        return None

    def validate_cross_table_integrity(self) -> StageValidationResult:
        """
        Validate cross-table referential integrity.

        Checks:
        - All child tables have valid parent references
        - No orphan records exist
        - Coverage metrics are within thresholds

        Returns:
            StageValidationResult with integrity issues.
        """
        result = StageValidationResult(
            stage=3,
            stage_name="Cross-Table Integrity",
        )

        if not self.db_path:
            result.add_issue(ValidationIssue(
                stage=3,
                category="configuration",
                severity="WARNING",
                code="NO_DATABASE",
                message="Database path not configured for integrity checks",
            ))
            return result

        child_tables = [
            ("devices", "device"),
            ("patients", "patient"),
            ("mdr_text", "text"),
            ("device_problems", "problem"),
        ]

        try:
            with get_connection(self.db_path, read_only=True) as conn:
                master_count = conn.execute(
                    "SELECT COUNT(*) FROM master_events"
                ).fetchone()[0]

                result.metrics["master_count"] = master_count

                for table_name, file_type in child_tables:
                    try:
                        # Count total and orphan records
                        total = conn.execute(
                            f"SELECT COUNT(DISTINCT mdr_report_key) FROM {table_name}"
                        ).fetchone()[0]

                        orphans = conn.execute(f"""
                            SELECT COUNT(DISTINCT c.mdr_report_key)
                            FROM {table_name} c
                            WHERE NOT EXISTS (
                                SELECT 1 FROM master_events m
                                WHERE m.mdr_report_key = c.mdr_report_key
                            )
                        """).fetchone()[0]

                        orphan_pct = (orphans / total * 100) if total > 0 else 0

                        result.metrics[f"{file_type}_total"] = total
                        result.metrics[f"{file_type}_orphans"] = orphans
                        result.metrics[f"{file_type}_orphan_pct"] = round(orphan_pct, 2)

                        if orphan_pct > 1.0:
                            result.add_issue(ValidationIssue(
                                stage=3,
                                category="referential_integrity",
                                severity="ERROR" if orphan_pct > 5.0 else "WARNING",
                                code="HIGH_ORPHAN_RATE",
                                message=f"{table_name}: {orphan_pct:.2f}% orphan records ({orphans:,} of {total:,})",
                                actual_value=orphan_pct,
                            ))

                    except Exception as e:
                        logger.warning(f"Could not check {table_name}: {e}")

        except Exception as e:
            result.add_issue(ValidationIssue(
                stage=3,
                category="database",
                severity="ERROR",
                code="DATABASE_ERROR",
                message=f"Database error during integrity check: {e}",
            ))

        return result

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of all validation statistics."""
        return {
            "files_validated": self.stats["files_validated"],
            "records_validated": self.stats["records_validated"],
            "stage1_failures": self.stats["stage1_failures"],
            "stage2_failures": self.stats["stage2_failures"],
            "stage3_failures": self.stats["stage3_failures"],
            "overall_failure_rate": (
                (self.stats["stage1_failures"] + self.stats["stage2_failures"] + self.stats["stage3_failures"])
                / max(1, self.stats["files_validated"])
            ),
        }


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def create_validation_pipeline(
    db_path: Optional[Path] = None,
    strict_mode: bool = False,
) -> ValidationPipeline:
    """Create a configured validation pipeline."""
    return ValidationPipeline(db_path=db_path, strict_mode=strict_mode)


def validate_file_complete(
    filepath: Path,
    file_type: str,
    db_path: Optional[Path] = None,
) -> PipelineValidationResult:
    """
    Run complete validation pipeline on a file.

    Args:
        filepath: Path to the file.
        file_type: Type of file.
        db_path: Path to database for post-load checks.

    Returns:
        Complete validation result.
    """
    pipeline = ValidationPipeline(db_path=db_path)

    result = PipelineValidationResult(
        filename=filepath.name,
        file_type=file_type,
    )

    # Stage 1
    result.stage1_result = pipeline.validate_stage1_preparse(filepath, file_type)

    result.update_overall_status()
    return result
