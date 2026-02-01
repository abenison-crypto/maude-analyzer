"""Patient outcome code parser for MAUDE data.

Parses the sequence_number_outcome field to extract individual outcome codes
and populate the boolean outcome flags in the patients table.

The sequence_number_outcome field contains codes like:
- D: Death
- L: Life Threatening
- H: Hospitalization
- DS: Disability
- CA: Congenital Anomaly
- RI: Required Intervention
- OT: Other

These may appear as single codes or comma-separated lists.
"""

from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
import re
import duckdb

from config.logging_config import get_logger

# Try to import config for code mappings
try:
    from config.config_loader import get_outcome_codes
    _CONFIG_AVAILABLE = True
except ImportError:
    _CONFIG_AVAILABLE = False

logger = get_logger("outcome_parser")

# Outcome code to boolean field mapping
OUTCOME_CODE_MAP = {
    "D": "outcome_death",
    "L": "outcome_life_threatening",
    "H": "outcome_hospitalization",
    "DS": "outcome_disability",
    "CA": "outcome_congenital_anomaly",
    "RI": "outcome_required_intervention",
    "OT": "outcome_other",
}

# All valid outcome codes
VALID_OUTCOME_CODES = set(OUTCOME_CODE_MAP.keys())

# Alternative representations found in data
OUTCOME_ALIASES = {
    "DEATH": "D",
    "LIFE THREATENING": "L",
    "HOSPITALIZATION": "H",
    "DISABILITY": "DS",
    "CONGENITAL ANOMALY": "CA",
    "REQUIRED INTERVENTION": "RI",
    "OTHER": "OT",
    "1": "D",  # Some older data uses numeric codes
    "2": "L",
    "3": "H",
    "4": "DS",
    "5": "CA",
    "6": "RI",
    "7": "OT",
}


@dataclass
class ParsedOutcome:
    """Result of parsing an outcome string."""
    raw_value: str
    codes: Set[str]
    death: bool = False
    life_threatening: bool = False
    hospitalization: bool = False
    disability: bool = False
    congenital_anomaly: bool = False
    required_intervention: bool = False
    other: bool = False
    parse_error: Optional[str] = None

    def to_dict(self) -> Dict[str, bool]:
        """Convert to dictionary of boolean fields."""
        return {
            "outcome_death": self.death,
            "outcome_life_threatening": self.life_threatening,
            "outcome_hospitalization": self.hospitalization,
            "outcome_disability": self.disability,
            "outcome_congenital_anomaly": self.congenital_anomaly,
            "outcome_required_intervention": self.required_intervention,
            "outcome_other": self.other,
        }


def parse_outcome_string(value: Optional[str]) -> ParsedOutcome:
    """
    Parse an outcome string into individual outcome codes.

    Args:
        value: Raw outcome string (may be comma-separated, semicolon-separated, etc.)

    Returns:
        ParsedOutcome with parsed codes and boolean flags
    """
    if value is None or str(value).strip() == "":
        return ParsedOutcome(raw_value="", codes=set())

    raw = str(value).strip().upper()
    codes = set()
    parse_error = None

    # Split on common delimiters
    parts = re.split(r"[,;|\s]+", raw)

    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Check if it's a valid code
        if part in VALID_OUTCOME_CODES:
            codes.add(part)
        elif part in OUTCOME_ALIASES:
            codes.add(OUTCOME_ALIASES[part])
        else:
            # Try to extract codes from longer strings
            for code in VALID_OUTCOME_CODES:
                if code in part:
                    codes.add(code)

            if not codes:
                # Log unrecognized values
                if parse_error is None:
                    parse_error = f"Unrecognized outcome code: {part}"
                logger.debug(f"Unrecognized outcome value: {part} in '{raw}'")

    # Build result
    result = ParsedOutcome(
        raw_value=raw,
        codes=codes,
        death="D" in codes,
        life_threatening="L" in codes,
        hospitalization="H" in codes,
        disability="DS" in codes,
        congenital_anomaly="CA" in codes,
        required_intervention="RI" in codes,
        other="OT" in codes,
        parse_error=parse_error,
    )

    return result


def analyze_outcome_distribution(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """
    Analyze the distribution of outcome values in the patients table.

    Args:
        conn: DuckDB connection

    Returns:
        Dict mapping raw values to their counts
    """
    try:
        # First check which column has data
        result = conn.execute("""
            SELECT
                sequence_number_outcome,
                COUNT(*) as cnt
            FROM patients
            WHERE sequence_number_outcome IS NOT NULL
            GROUP BY sequence_number_outcome
            ORDER BY cnt DESC
            LIMIT 100
        """).fetchall()

        distribution = {}
        for row in result:
            value = row[0] if row[0] else "(empty)"
            distribution[value] = row[1]

        return distribution

    except Exception as e:
        logger.error(f"Error analyzing outcome distribution: {e}")

        # Try outcome_codes_raw as fallback
        try:
            result = conn.execute("""
                SELECT
                    outcome_codes_raw,
                    COUNT(*) as cnt
                FROM patients
                WHERE outcome_codes_raw IS NOT NULL
                GROUP BY outcome_codes_raw
                ORDER BY cnt DESC
                LIMIT 100
            """).fetchall()

            distribution = {}
            for row in result:
                value = row[0] if row[0] else "(empty)"
                distribution[value] = row[1]

            return distribution

        except Exception as e2:
            logger.error(f"Error analyzing outcome_codes_raw: {e2}")
            return {}


def get_outcome_coverage(conn: duckdb.DuckDBPyConnection) -> Dict[str, float]:
    """
    Get coverage percentages for outcome boolean fields.

    Args:
        conn: DuckDB connection

    Returns:
        Dict mapping field name to coverage percentage
    """
    fields = [
        "outcome_death",
        "outcome_life_threatening",
        "outcome_hospitalization",
        "outcome_disability",
        "outcome_congenital_anomaly",
        "outcome_required_intervention",
        "outcome_other",
    ]

    coverage = {}

    try:
        total = conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
        if total == 0:
            return {f: 0.0 for f in fields}

        for field in fields:
            try:
                # Count where the boolean is TRUE
                true_count = conn.execute(f"""
                    SELECT COUNT(*) FROM patients WHERE {field} = TRUE
                """).fetchone()[0]
                coverage[field] = (true_count / total) * 100
            except Exception:
                coverage[field] = 0.0

    except Exception as e:
        logger.error(f"Error getting outcome coverage: {e}")
        return {f: 0.0 for f in fields}

    return coverage


def detect_outcome_source_column(conn: duckdb.DuckDBPyConnection) -> Optional[str]:
    """
    Detect which column contains the raw outcome codes.

    Returns the column name that has the most non-null values.
    """
    candidate_columns = [
        "sequence_number_outcome",
        "outcome_codes_raw",
    ]

    best_column = None
    best_count = 0

    for col in candidate_columns:
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM patients WHERE {col} IS NOT NULL
            """).fetchone()
            count = result[0] if result else 0

            if count > best_count:
                best_count = count
                best_column = col

        except Exception:
            continue

    return best_column


def parse_outcomes_batch(
    values: List[Optional[str]],
) -> List[ParsedOutcome]:
    """
    Parse a batch of outcome strings.

    Args:
        values: List of raw outcome strings

    Returns:
        List of ParsedOutcome objects
    """
    return [parse_outcome_string(v) for v in values]


def validate_outcome_parsing(
    conn: duckdb.DuckDBPyConnection,
    sample_size: int = 1000,
) -> Dict[str, any]:
    """
    Validate outcome parsing logic against sample data.

    Args:
        conn: DuckDB connection
        sample_size: Number of records to sample

    Returns:
        Validation report with statistics
    """
    source_col = detect_outcome_source_column(conn)
    if not source_col:
        return {"error": "No outcome source column found"}

    try:
        # Get sample of raw values
        result = conn.execute(f"""
            SELECT {source_col}
            FROM patients
            WHERE {source_col} IS NOT NULL
            USING SAMPLE {sample_size}
        """).fetchall()

        # Parse each value
        total = len(result)
        successful = 0
        with_errors = 0
        code_counts = {code: 0 for code in VALID_OUTCOME_CODES}
        unrecognized = []

        for row in result:
            parsed = parse_outcome_string(row[0])

            if parsed.codes:
                successful += 1
                for code in parsed.codes:
                    code_counts[code] = code_counts.get(code, 0) + 1

            if parsed.parse_error:
                with_errors += 1
                if len(unrecognized) < 20:
                    unrecognized.append(parsed.raw_value)

        return {
            "source_column": source_col,
            "total_sampled": total,
            "successfully_parsed": successful,
            "with_errors": with_errors,
            "parse_success_rate": (successful / total * 100) if total > 0 else 0,
            "code_distribution": code_counts,
            "sample_unrecognized": unrecognized[:10],
        }

    except Exception as e:
        logger.error(f"Error validating outcome parsing: {e}")
        return {"error": str(e)}


def generate_update_sql(
    source_column: str = "sequence_number_outcome",
) -> str:
    """
    Generate SQL to update outcome boolean fields from source column.

    This uses DuckDB's CASE expressions to parse the codes.
    """
    sql = f"""
    UPDATE patients SET
        outcome_death = CASE
            WHEN {source_column} IS NULL THEN FALSE
            WHEN {source_column} ILIKE '%D%' AND {source_column} NOT ILIKE '%DS%' THEN TRUE
            WHEN {source_column} = 'D' THEN TRUE
            ELSE FALSE
        END,
        outcome_life_threatening = CASE
            WHEN {source_column} IS NULL THEN FALSE
            WHEN {source_column} ILIKE '%L%' THEN TRUE
            ELSE FALSE
        END,
        outcome_hospitalization = CASE
            WHEN {source_column} IS NULL THEN FALSE
            WHEN {source_column} ILIKE '%H%' THEN TRUE
            ELSE FALSE
        END,
        outcome_disability = CASE
            WHEN {source_column} IS NULL THEN FALSE
            WHEN {source_column} ILIKE '%DS%' THEN TRUE
            ELSE FALSE
        END,
        outcome_congenital_anomaly = CASE
            WHEN {source_column} IS NULL THEN FALSE
            WHEN {source_column} ILIKE '%CA%' THEN TRUE
            ELSE FALSE
        END,
        outcome_required_intervention = CASE
            WHEN {source_column} IS NULL THEN FALSE
            WHEN {source_column} ILIKE '%RI%' THEN TRUE
            ELSE FALSE
        END,
        outcome_other = CASE
            WHEN {source_column} IS NULL THEN FALSE
            WHEN {source_column} ILIKE '%OT%' THEN TRUE
            ELSE FALSE
        END
    WHERE {source_column} IS NOT NULL
    """
    return sql
