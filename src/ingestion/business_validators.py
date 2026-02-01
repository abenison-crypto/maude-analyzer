"""
Business Rule Validators for FDA MAUDE Data.

Implements validation rules for regulatory-quality data assurance:
1. Death events consistency (event_type=D -> outcome_death=True)
2. Date ordering (date_of_event <= date_received)
3. Date range validation (1984-current)
4. Product code format validation

These validators run during data transformation (Stage 2: Post-Transform).
"""

from datetime import date, datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
import re

from config.logging_config import get_logger

logger = get_logger("business_validators")


@dataclass
class ValidationResult:
    """Result of a validation check."""
    field_name: str
    rule_name: str
    passed: bool
    severity: str = "WARNING"  # INFO, WARNING, ERROR
    message: str = ""
    original_value: Any = None
    corrected_value: Any = None


@dataclass
class RecordValidationResult:
    """Aggregated validation results for a record."""
    mdr_report_key: Optional[str] = None
    is_valid: bool = True
    errors: List[ValidationResult] = field(default_factory=list)
    warnings: List[ValidationResult] = field(default_factory=list)
    corrections: List[ValidationResult] = field(default_factory=list)

    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result."""
        if not result.passed:
            if result.severity == "ERROR":
                self.errors.append(result)
                self.is_valid = False
            elif result.severity == "WARNING":
                self.warnings.append(result)
        if result.corrected_value is not None:
            self.corrections.append(result)


# =============================================================================
# DATE VALIDATORS
# =============================================================================

# Valid date range for MAUDE data
MIN_VALID_DATE = date(1984, 1, 1)  # MAUDE data starts in 1984
MAX_VALID_YEAR = datetime.now().year + 1  # Allow for future-dated reports


def validate_date_range(
    value: Any,
    field_name: str,
    min_date: date = MIN_VALID_DATE,
    max_year: int = None
) -> ValidationResult:
    """
    Validate that a date falls within acceptable range.

    Args:
        value: Date value to validate (date, datetime, or string).
        field_name: Name of the field being validated.
        min_date: Minimum acceptable date.
        max_year: Maximum acceptable year.

    Returns:
        ValidationResult with pass/fail status.
    """
    max_year = max_year or MAX_VALID_YEAR

    if value is None:
        return ValidationResult(
            field_name=field_name,
            rule_name="date_range",
            passed=True,
            message="Date is null (acceptable)"
        )

    # Convert to date if needed
    try:
        if isinstance(value, str):
            # Try common formats
            for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"]:
                try:
                    value = datetime.strptime(value, fmt).date()
                    break
                except ValueError:
                    continue
            else:
                return ValidationResult(
                    field_name=field_name,
                    rule_name="date_range",
                    passed=False,
                    severity="WARNING",
                    message=f"Could not parse date: {value}",
                    original_value=value
                )
        elif isinstance(value, datetime):
            value = value.date()
        elif not isinstance(value, date):
            return ValidationResult(
                field_name=field_name,
                rule_name="date_range",
                passed=False,
                severity="WARNING",
                message=f"Invalid date type: {type(value)}",
                original_value=value
            )
    except Exception as e:
        return ValidationResult(
            field_name=field_name,
            rule_name="date_range",
            passed=False,
            severity="WARNING",
            message=f"Date conversion error: {e}",
            original_value=value
        )

    # Check range
    if value < min_date:
        return ValidationResult(
            field_name=field_name,
            rule_name="date_range",
            passed=False,
            severity="WARNING",
            message=f"Date {value} is before minimum {min_date}",
            original_value=value
        )

    if value.year > max_year:
        return ValidationResult(
            field_name=field_name,
            rule_name="date_range",
            passed=False,
            severity="WARNING",
            message=f"Date {value} year exceeds maximum {max_year}",
            original_value=value
        )

    return ValidationResult(
        field_name=field_name,
        rule_name="date_range",
        passed=True,
        message=f"Date {value} is within valid range"
    )


def validate_date_ordering(
    earlier_date: Any,
    later_date: Any,
    earlier_field: str,
    later_field: str
) -> ValidationResult:
    """
    Validate that one date is not after another.

    Example: date_of_event should be <= date_received

    Args:
        earlier_date: Date that should be earlier or equal.
        later_date: Date that should be later or equal.
        earlier_field: Name of the earlier date field.
        later_field: Name of the later date field.

    Returns:
        ValidationResult with pass/fail status.
    """
    # If either date is None, skip validation
    if earlier_date is None or later_date is None:
        return ValidationResult(
            field_name=f"{earlier_field}_vs_{later_field}",
            rule_name="date_ordering",
            passed=True,
            message="Date comparison skipped (null value)"
        )

    # Convert to dates if needed
    try:
        if isinstance(earlier_date, str):
            earlier_date = datetime.strptime(earlier_date, "%Y-%m-%d").date()
        elif isinstance(earlier_date, datetime):
            earlier_date = earlier_date.date()

        if isinstance(later_date, str):
            later_date = datetime.strptime(later_date, "%Y-%m-%d").date()
        elif isinstance(later_date, datetime):
            later_date = later_date.date()

    except (ValueError, TypeError) as e:
        return ValidationResult(
            field_name=f"{earlier_field}_vs_{later_field}",
            rule_name="date_ordering",
            passed=True,  # Don't fail on parse errors
            message=f"Date comparison skipped: {e}"
        )

    # Check ordering
    if earlier_date > later_date:
        return ValidationResult(
            field_name=f"{earlier_field}_vs_{later_field}",
            rule_name="date_ordering",
            passed=False,
            severity="WARNING",
            message=f"{earlier_field} ({earlier_date}) is after {later_field} ({later_date})",
            original_value=f"{earlier_date} > {later_date}"
        )

    return ValidationResult(
        field_name=f"{earlier_field}_vs_{later_field}",
        rule_name="date_ordering",
        passed=True,
        message="Date ordering is correct"
    )


# =============================================================================
# EVENT TYPE VALIDATORS
# =============================================================================

def validate_death_event_consistency(
    event_type: str,
    outcome_death: Any,
    record: Dict[str, Any]
) -> ValidationResult:
    """
    Validate that death events are consistent.

    If event_type is 'D' (Death), outcome_death should be True.
    Conversely, if outcome_death is True, event_type should be 'D'.

    Args:
        event_type: Event type code (D, IN, M, O, *).
        outcome_death: Patient death outcome flag.
        record: Full record for context.

    Returns:
        ValidationResult with pass/fail status and potential correction.
    """
    # Normalize values
    event_type = str(event_type).upper().strip() if event_type else ""
    outcome_death_bool = outcome_death in (True, "Y", "1", 1, "TRUE")

    # Check consistency
    if event_type == "D" and not outcome_death_bool:
        return ValidationResult(
            field_name="death_consistency",
            rule_name="death_event_consistency",
            passed=False,
            severity="WARNING",
            message="Event type is 'D' (Death) but outcome_death is not True",
            original_value={"event_type": event_type, "outcome_death": outcome_death},
            corrected_value={"outcome_death": True}
        )

    # Note: We don't fail if outcome_death=True but event_type != D
    # because a patient can die from an injury (IN) event

    return ValidationResult(
        field_name="death_consistency",
        rule_name="death_event_consistency",
        passed=True,
        message="Death event consistency check passed"
    )


def validate_event_type(event_type: str) -> ValidationResult:
    """
    Validate event type code.

    Valid codes: D (Death), IN (Injury), M (Malfunction), O (Other), * (Unknown)

    Args:
        event_type: Event type code to validate.

    Returns:
        ValidationResult with pass/fail status.
    """
    valid_codes = {"D", "IN", "M", "O", "*", ""}

    if event_type is None:
        return ValidationResult(
            field_name="event_type",
            rule_name="event_type_valid",
            passed=True,
            message="Event type is null (acceptable)"
        )

    normalized = str(event_type).upper().strip()

    if normalized in valid_codes:
        return ValidationResult(
            field_name="event_type",
            rule_name="event_type_valid",
            passed=True,
            message=f"Event type '{normalized}' is valid"
        )

    return ValidationResult(
        field_name="event_type",
        rule_name="event_type_valid",
        passed=False,
        severity="WARNING",
        message=f"Unknown event type: '{event_type}'",
        original_value=event_type
    )


# =============================================================================
# PRODUCT CODE VALIDATORS
# =============================================================================

# Product code pattern: 3 uppercase letters
PRODUCT_CODE_PATTERN = re.compile(r'^[A-Z]{3}$')


def validate_product_code_format(product_code: str) -> ValidationResult:
    """
    Validate FDA product code format.

    Product codes should be exactly 3 uppercase letters.

    Args:
        product_code: Product code to validate.

    Returns:
        ValidationResult with pass/fail status.
    """
    if product_code is None or product_code == "":
        return ValidationResult(
            field_name="product_code",
            rule_name="product_code_format",
            passed=True,
            message="Product code is empty (acceptable)"
        )

    normalized = str(product_code).upper().strip()

    if PRODUCT_CODE_PATTERN.match(normalized):
        return ValidationResult(
            field_name="product_code",
            rule_name="product_code_format",
            passed=True,
            message=f"Product code '{normalized}' is valid format"
        )

    # Check if it's close to valid format
    if len(normalized) == 3 and normalized.isalpha():
        return ValidationResult(
            field_name="product_code",
            rule_name="product_code_format",
            passed=True,
            corrected_value=normalized,
            message=f"Product code normalized to '{normalized}'"
        )

    return ValidationResult(
        field_name="product_code",
        rule_name="product_code_format",
        passed=False,
        severity="WARNING",
        message=f"Invalid product code format: '{product_code}'",
        original_value=product_code
    )


# =============================================================================
# FLAG VALIDATORS
# =============================================================================

VALID_FLAG_VALUES = {"Y", "N", "", None}


def validate_flag_value(value: Any, field_name: str) -> ValidationResult:
    """
    Validate Y/N flag field.

    Args:
        value: Flag value to validate.
        field_name: Name of the flag field.

    Returns:
        ValidationResult with pass/fail status.
    """
    if value is None:
        return ValidationResult(
            field_name=field_name,
            rule_name="flag_value",
            passed=True,
            message="Flag is null (acceptable)"
        )

    normalized = str(value).upper().strip()

    if normalized in VALID_FLAG_VALUES:
        return ValidationResult(
            field_name=field_name,
            rule_name="flag_value",
            passed=True,
            message=f"Flag value '{normalized}' is valid"
        )

    # Try to normalize common variations
    if normalized in ("YES", "TRUE", "1"):
        return ValidationResult(
            field_name=field_name,
            rule_name="flag_value",
            passed=True,
            corrected_value="Y",
            message=f"Flag value normalized from '{value}' to 'Y'"
        )
    if normalized in ("NO", "FALSE", "0"):
        return ValidationResult(
            field_name=field_name,
            rule_name="flag_value",
            passed=True,
            corrected_value="N",
            message=f"Flag value normalized from '{value}' to 'N'"
        )

    return ValidationResult(
        field_name=field_name,
        rule_name="flag_value",
        passed=False,
        severity="WARNING",
        message=f"Invalid flag value: '{value}'",
        original_value=value
    )


# =============================================================================
# COMPOSITE VALIDATORS
# =============================================================================

def validate_master_record(record: Dict[str, Any]) -> RecordValidationResult:
    """
    Run all validations on a master event record.

    Args:
        record: Master event record dictionary.

    Returns:
        RecordValidationResult with all validation results.
    """
    result = RecordValidationResult(
        mdr_report_key=record.get("mdr_report_key")
    )

    # Date validations
    date_fields = [
        "date_received", "date_report", "date_of_event",
        "date_facility_aware", "date_report_to_fda",
        "date_manufacturer_received", "date_added", "date_changed"
    ]
    for field_name in date_fields:
        if field_name in record:
            result.add_result(validate_date_range(record[field_name], field_name))

    # Date ordering validations
    if "date_of_event" in record and "date_received" in record:
        result.add_result(validate_date_ordering(
            record.get("date_of_event"),
            record.get("date_received"),
            "date_of_event",
            "date_received"
        ))

    # Event type validation
    if "event_type" in record:
        result.add_result(validate_event_type(record.get("event_type")))

    # Flag validations
    flag_fields = [
        "adverse_event_flag", "product_problem_flag",
        "health_professional", "single_use_flag"
    ]
    for field_name in flag_fields:
        if field_name in record:
            result.add_result(validate_flag_value(record[field_name], field_name))

    return result


def validate_device_record(record: Dict[str, Any]) -> RecordValidationResult:
    """
    Run all validations on a device record.

    Args:
        record: Device record dictionary.

    Returns:
        RecordValidationResult with all validation results.
    """
    result = RecordValidationResult(
        mdr_report_key=record.get("mdr_report_key")
    )

    # Date validations
    date_fields = ["date_received", "expiration_date_of_device", "date_returned_to_manufacturer"]
    for field_name in date_fields:
        if field_name in record:
            result.add_result(validate_date_range(record[field_name], field_name))

    # Product code validation
    product_code = record.get("device_report_product_code")
    if product_code:
        result.add_result(validate_product_code_format(product_code))

    # Flag validations
    flag_fields = ["implant_flag", "date_removed_flag"]
    for field_name in flag_fields:
        if field_name in record:
            result.add_result(validate_flag_value(record[field_name], field_name))

    return result


def validate_patient_record(record: Dict[str, Any]) -> RecordValidationResult:
    """
    Run all validations on a patient record.

    Args:
        record: Patient record dictionary.

    Returns:
        RecordValidationResult with all validation results.
    """
    result = RecordValidationResult(
        mdr_report_key=record.get("mdr_report_key")
    )

    # Date validations
    if "date_received" in record:
        result.add_result(validate_date_range(record["date_received"], "date_received"))

    # Sex validation
    sex = record.get("patient_sex")
    if sex:
        valid_sex = {"M", "F", "U", "MALE", "FEMALE", "UNKNOWN", ""}
        normalized = str(sex).upper().strip() if sex else ""
        if normalized not in valid_sex:
            result.add_result(ValidationResult(
                field_name="patient_sex",
                rule_name="patient_sex_valid",
                passed=False,
                severity="WARNING",
                message=f"Invalid patient sex: '{sex}'",
                original_value=sex
            ))

    return result


# =============================================================================
# BATCH VALIDATION
# =============================================================================

class BusinessValidator:
    """Business rule validator for batch processing."""

    def __init__(self):
        self.total_validated = 0
        self.total_errors = 0
        self.total_warnings = 0
        self.errors_by_rule = {}
        self.warnings_by_rule = {}

    def validate_record(
        self,
        record: Dict[str, Any],
        file_type: str
    ) -> RecordValidationResult:
        """
        Validate a single record based on file type.

        Args:
            record: Record dictionary.
            file_type: Type of record (master, device, patient).

        Returns:
            RecordValidationResult with all validation results.
        """
        self.total_validated += 1

        if file_type == "master":
            result = validate_master_record(record)
        elif file_type == "device":
            result = validate_device_record(record)
        elif file_type == "patient":
            result = validate_patient_record(record)
        else:
            result = RecordValidationResult(
                mdr_report_key=record.get("mdr_report_key")
            )

        # Track statistics
        for error in result.errors:
            self.total_errors += 1
            self.errors_by_rule[error.rule_name] = \
                self.errors_by_rule.get(error.rule_name, 0) + 1

        for warning in result.warnings:
            self.total_warnings += 1
            self.warnings_by_rule[warning.rule_name] = \
                self.warnings_by_rule.get(warning.rule_name, 0) + 1

        return result

    def get_summary(self) -> Dict[str, Any]:
        """Get validation summary statistics."""
        return {
            "total_validated": self.total_validated,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "error_rate": self.total_errors / self.total_validated if self.total_validated > 0 else 0,
            "warning_rate": self.total_warnings / self.total_validated if self.total_validated > 0 else 0,
            "errors_by_rule": self.errors_by_rule,
            "warnings_by_rule": self.warnings_by_rule,
        }
