"""
Schema Validation Middleware for MAUDE Analyzer API.

This middleware provides:
1. Startup validation - Compares database schema against registry
2. X-Schema-Version header - Added to all responses
3. Runtime schema change detection

Usage in api/main.py:
    from api.middleware.schema_validation import (
        SchemaVersionMiddleware,
        validate_schema_on_startup
    )

    app.add_middleware(SchemaVersionMiddleware)

    @app.on_event("startup")
    async def startup():
        validate_schema_on_startup()
"""

from typing import Optional, List, Dict, Any
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from config.unified_schema import get_schema_registry, SCHEMA_VERSION
from config.logging_config import get_logger

logger = get_logger("schema_validation")


class SchemaVersionMiddleware(BaseHTTPMiddleware):
    """
    Middleware that adds X-Schema-Version header to all responses.

    This allows the frontend to detect schema version mismatches
    and warn users or trigger type regeneration.
    """

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        # Add schema version header to all responses
        response.headers["X-Schema-Version"] = SCHEMA_VERSION

        return response


class SchemaValidationResult:
    """Result of schema validation."""

    def __init__(self):
        self.is_valid = True
        self.missing_tables: List[str] = []
        self.missing_columns: Dict[str, List[str]] = {}
        self.extra_columns: Dict[str, List[str]] = {}
        self.type_mismatches: Dict[str, List[Dict[str, str]]] = {}
        self.warnings: List[str] = []

    def add_missing_table(self, table: str):
        self.is_valid = False
        self.missing_tables.append(table)

    def add_missing_column(self, table: str, column: str):
        if table not in self.missing_columns:
            self.missing_columns[table] = []
        self.missing_columns[table].append(column)

    def add_extra_column(self, table: str, column: str):
        if table not in self.extra_columns:
            self.extra_columns[table] = []
        self.extra_columns[table].append(column)

    def add_type_mismatch(self, table: str, column: str, expected: str, actual: str):
        if table not in self.type_mismatches:
            self.type_mismatches[table] = []
        self.type_mismatches[table].append({
            "column": column,
            "expected": expected,
            "actual": actual
        })
        self.warnings.append(f"Type mismatch in {table}.{column}: expected {expected}, got {actual}")

    def add_warning(self, message: str):
        self.warnings.append(message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "schema_version": SCHEMA_VERSION,
            "missing_tables": self.missing_tables,
            "missing_columns": self.missing_columns,
            "extra_columns": self.extra_columns,
            "type_mismatches": self.type_mismatches,
            "warnings": self.warnings,
        }

    def log_results(self):
        """Log validation results."""
        if self.is_valid and not self.warnings:
            logger.info(f"Schema validation passed (version {SCHEMA_VERSION})")
            return

        if self.missing_tables:
            logger.error(f"Missing tables: {', '.join(self.missing_tables)}")

        for table, columns in self.missing_columns.items():
            logger.warning(f"Missing columns in {table}: {', '.join(columns)}")

        for table, columns in self.extra_columns.items():
            logger.info(f"Extra columns in {table} (not in registry): {', '.join(columns)}")

        for warning in self.warnings:
            logger.warning(warning)


def validate_schema_against_database(conn) -> SchemaValidationResult:
    """
    Validate the database schema against the registry.

    Args:
        conn: DuckDB database connection

    Returns:
        SchemaValidationResult with validation details
    """
    registry = get_schema_registry()
    result = SchemaValidationResult()

    try:
        # Get list of tables in database
        db_tables = set()
        tables_result = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()
        db_tables = {row[0] for row in tables_result}

        # Check each table in the registry
        for table_name, table_def in registry.tables.items():
            if table_name not in db_tables:
                result.add_missing_table(table_name)
                continue

            # Get columns from database
            try:
                describe_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                db_columns = {row[0]: row[1] for row in describe_result}
            except Exception as e:
                result.add_warning(f"Could not describe table {table_name}: {e}")
                continue

            # Check required columns (non-optional)
            for col in table_def.columns:
                if col.is_optional:
                    # Optional columns are allowed to be missing
                    if col.db_name not in db_columns:
                        logger.debug(f"Optional column {table_name}.{col.db_name} not present")
                else:
                    if col.db_name not in db_columns:
                        result.add_missing_column(table_name, col.db_name)

            # Check for extra columns not in registry
            registry_columns = {col.db_name for col in table_def.columns}
            for db_col in db_columns.keys():
                if db_col not in registry_columns:
                    result.add_extra_column(table_name, db_col)

    except Exception as e:
        result.is_valid = False
        result.add_warning(f"Schema validation error: {e}")

    return result


def validate_schema_on_startup():
    """
    Validate schema at application startup.

    This should be called from the FastAPI startup event.
    Logs warnings for any schema mismatches but does not prevent startup.
    """
    try:
        from api.services.database import get_db
        db = get_db()

        logger.info(f"Validating database schema (registry version {SCHEMA_VERSION})...")

        result = validate_schema_against_database(db.conn)
        result.log_results()

        if not result.is_valid:
            logger.warning("Schema validation found issues - some features may not work correctly")

        return result

    except Exception as e:
        logger.error(f"Could not validate schema on startup: {e}")
        return None


def get_schema_info() -> Dict[str, Any]:
    """
    Get schema information for the admin/status endpoint.

    Returns:
        Dictionary with schema version and validation status
    """
    registry = get_schema_registry()

    return {
        "schema_version": SCHEMA_VERSION,
        "tables": list(registry.tables.keys()),
        "event_types": list(registry.get_all_event_types().keys()),
    }


async def validate_request_columns(table: str, columns: List[str]) -> Dict[str, bool]:
    """
    Validate that requested columns exist in the schema.

    This can be used by endpoints to validate column parameters.

    Args:
        table: Table name
        columns: List of column names to validate

    Returns:
        Dict mapping column name to existence boolean
    """
    registry = get_schema_registry()
    return registry.validate_columns_exist(table, columns)
