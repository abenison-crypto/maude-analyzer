"""
Data Quality API Router.

Provides endpoints for monitoring data completeness and quality:
- GET /completeness - % with manufacturer, device, text by year
- GET /file-status - which files loaded, row counts, last update
- GET /coverage - manufacturer and product code coverage metrics
- GET /gaps - identify data gaps by year
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, List, Optional, Any
from datetime import datetime
import sys
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from src.database import get_connection

router = APIRouter(prefix="/data-quality", tags=["Data Quality"])


# =============================================================================
# Response Models
# =============================================================================

class YearlyCompleteness(BaseModel):
    """Completeness metrics for a single year."""
    year: int
    total_events: int
    with_manufacturer: int
    manufacturer_pct: float
    with_product_code: int
    product_code_pct: float
    with_device: int
    device_pct: float
    with_text: int
    text_pct: float


class CompletenessResponse(BaseModel):
    """Response for completeness endpoint."""
    timestamp: str
    overall: Dict[str, float]
    by_year: List[YearlyCompleteness]


class FileStatus(BaseModel):
    """Status of a loaded file."""
    file_name: str
    file_type: str
    records_loaded: int
    loaded_at: Optional[str]
    status: str


class FileStatusResponse(BaseModel):
    """Response for file status endpoint."""
    timestamp: str
    total_files: int
    files: List[FileStatus]
    by_type: Dict[str, Dict]


class CoverageMetric(BaseModel):
    """Coverage metric details."""
    total: int
    covered: int
    missing: int
    coverage_pct: float
    missing_by_year: Dict[int, int]


class CoverageResponse(BaseModel):
    """Response for coverage endpoint."""
    timestamp: str
    manufacturer_coverage: CoverageMetric
    product_code_coverage: CoverageMetric
    device_coverage: CoverageMetric


class DataGap(BaseModel):
    """Information about a data gap."""
    year: int
    issue: str
    severity: str  # "warning", "critical"
    affected_records: int
    recommendation: str


class GapsResponse(BaseModel):
    """Response for gaps endpoint."""
    timestamp: str
    total_gaps: int
    critical_gaps: int
    gaps: List[DataGap]


# =============================================================================
# Endpoints
# =============================================================================

@router.get("/completeness", response_model=CompletenessResponse)
async def get_completeness(
    start_year: Optional[int] = Query(None, description="Start year filter"),
    end_year: Optional[int] = Query(None, description="End year filter"),
):
    """
    Get data completeness metrics by year.

    Returns percentage of events with:
    - manufacturer_clean populated
    - product_code populated
    - device records linked
    - text records linked
    """
    try:
        with get_connection(read_only=True) as conn:
            # Build year filter
            year_filter = ""
            if start_year:
                year_filter += f" AND EXTRACT(YEAR FROM date_received) >= {start_year}"
            if end_year:
                year_filter += f" AND EXTRACT(YEAR FROM date_received) <= {end_year}"

            # Overall metrics
            overall = conn.execute(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(manufacturer_clean) as with_mfr,
                    COUNT(product_code) as with_product
                FROM master_events
                WHERE date_received IS NOT NULL {year_filter}
            """).fetchone()

            # Device coverage
            device_coverage = conn.execute(f"""
                SELECT COUNT(*) FROM master_events m
                WHERE EXISTS (
                    SELECT 1 FROM devices d WHERE d.mdr_report_key = m.mdr_report_key
                )
                AND m.date_received IS NOT NULL {year_filter}
            """).fetchone()[0]

            # Text coverage
            text_coverage = conn.execute(f"""
                SELECT COUNT(*) FROM master_events m
                WHERE EXISTS (
                    SELECT 1 FROM mdr_text t WHERE t.mdr_report_key = m.mdr_report_key
                )
                AND m.date_received IS NOT NULL {year_filter}
            """).fetchone()[0]

            total = overall[0] if overall[0] else 1

            overall_metrics = {
                "total_events": total,
                "manufacturer_pct": round((overall[1] / total) * 100, 1),
                "product_code_pct": round((overall[2] / total) * 100, 1),
                "device_pct": round((device_coverage / total) * 100, 1),
                "text_pct": round((text_coverage / total) * 100, 1),
            }

            # By year
            by_year_query = f"""
                SELECT
                    EXTRACT(YEAR FROM m.date_received)::INTEGER as year,
                    COUNT(*) as total_events,
                    COUNT(m.manufacturer_clean) as with_mfr,
                    COUNT(m.product_code) as with_product,
                    COUNT(DISTINCT CASE WHEN d.mdr_report_key IS NOT NULL THEN m.mdr_report_key END) as with_device,
                    COUNT(DISTINCT CASE WHEN t.mdr_report_key IS NOT NULL THEN m.mdr_report_key END) as with_text
                FROM master_events m
                LEFT JOIN devices d ON m.mdr_report_key = d.mdr_report_key
                LEFT JOIN mdr_text t ON m.mdr_report_key = t.mdr_report_key
                WHERE m.date_received IS NOT NULL {year_filter}
                GROUP BY 1
                ORDER BY 1
            """

            by_year_results = conn.execute(by_year_query).fetchall()

            yearly_data = []
            for row in by_year_results:
                if row[0]:
                    total_yr = row[1] if row[1] else 1
                    yearly_data.append(YearlyCompleteness(
                        year=row[0],
                        total_events=row[1],
                        with_manufacturer=row[2],
                        manufacturer_pct=round((row[2] / total_yr) * 100, 1),
                        with_product_code=row[3],
                        product_code_pct=round((row[3] / total_yr) * 100, 1),
                        with_device=row[4],
                        device_pct=round((row[4] / total_yr) * 100, 1),
                        with_text=row[5],
                        text_pct=round((row[5] / total_yr) * 100, 1),
                    ))

            return CompletenessResponse(
                timestamp=datetime.now().isoformat(),
                overall=overall_metrics,
                by_year=yearly_data,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/file-status", response_model=FileStatusResponse)
async def get_file_status():
    """
    Get status of loaded files from ingestion log.

    Returns information about which files have been loaded,
    record counts, and last update times.
    """
    try:
        with get_connection(read_only=True) as conn:
            # Get ingestion log
            files_query = """
                SELECT
                    file_name,
                    file_type,
                    records_loaded,
                    completed_at,
                    status
                FROM ingestion_log
                ORDER BY completed_at DESC
                LIMIT 100
            """

            files_result = conn.execute(files_query).fetchall()

            files = []
            for row in files_result:
                files.append(FileStatus(
                    file_name=row[0],
                    file_type=row[1],
                    records_loaded=row[2] or 0,
                    loaded_at=str(row[3]) if row[3] else None,
                    status=row[4] or "UNKNOWN",
                ))

            # Aggregate by type
            by_type_query = """
                SELECT
                    file_type,
                    COUNT(*) as file_count,
                    SUM(records_loaded) as total_records,
                    MAX(completed_at) as last_load
                FROM ingestion_log
                GROUP BY file_type
            """

            by_type_result = conn.execute(by_type_query).fetchall()

            by_type = {}
            for row in by_type_result:
                by_type[row[0]] = {
                    "file_count": row[1],
                    "total_records": row[2] or 0,
                    "last_load": str(row[3]) if row[3] else None,
                }

            return FileStatusResponse(
                timestamp=datetime.now().isoformat(),
                total_files=len(files),
                files=files,
                by_type=by_type,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/coverage", response_model=CoverageResponse)
async def get_coverage():
    """
    Get detailed coverage metrics for key fields.

    Returns breakdown of coverage for:
    - manufacturer_clean
    - product_code
    - device records
    """
    try:
        with get_connection(read_only=True) as conn:
            # Manufacturer coverage
            mfr_stats = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(manufacturer_clean) as covered
                FROM master_events
            """).fetchone()

            mfr_by_year = conn.execute("""
                SELECT
                    EXTRACT(YEAR FROM date_received)::INTEGER as year,
                    COUNT(*) - COUNT(manufacturer_clean) as missing
                FROM master_events
                WHERE date_received IS NOT NULL
                GROUP BY 1
                HAVING COUNT(*) - COUNT(manufacturer_clean) > 0
                ORDER BY 1
            """).fetchall()

            mfr_coverage = CoverageMetric(
                total=mfr_stats[0],
                covered=mfr_stats[1],
                missing=mfr_stats[0] - mfr_stats[1],
                coverage_pct=round((mfr_stats[1] / mfr_stats[0]) * 100, 1) if mfr_stats[0] > 0 else 0,
                missing_by_year={int(row[0]): row[1] for row in mfr_by_year if row[0]},
            )

            # Product code coverage
            product_stats = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(product_code) as covered
                FROM master_events
            """).fetchone()

            product_by_year = conn.execute("""
                SELECT
                    EXTRACT(YEAR FROM date_received)::INTEGER as year,
                    COUNT(*) - COUNT(product_code) as missing
                FROM master_events
                WHERE date_received IS NOT NULL
                GROUP BY 1
                HAVING COUNT(*) - COUNT(product_code) > 0
                ORDER BY 1
            """).fetchall()

            product_coverage = CoverageMetric(
                total=product_stats[0],
                covered=product_stats[1],
                missing=product_stats[0] - product_stats[1],
                coverage_pct=round((product_stats[1] / product_stats[0]) * 100, 1) if product_stats[0] > 0 else 0,
                missing_by_year={int(row[0]): row[1] for row in product_by_year if row[0]},
            )

            # Device coverage
            total_events = mfr_stats[0]
            with_device = conn.execute("""
                SELECT COUNT(DISTINCT m.mdr_report_key)
                FROM master_events m
                WHERE EXISTS (
                    SELECT 1 FROM devices d WHERE d.mdr_report_key = m.mdr_report_key
                )
            """).fetchone()[0]

            device_by_year = conn.execute("""
                SELECT
                    EXTRACT(YEAR FROM m.date_received)::INTEGER as year,
                    COUNT(*) as missing
                FROM master_events m
                WHERE NOT EXISTS (
                    SELECT 1 FROM devices d WHERE d.mdr_report_key = m.mdr_report_key
                )
                AND m.date_received IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """).fetchall()

            device_coverage = CoverageMetric(
                total=total_events,
                covered=with_device,
                missing=total_events - with_device,
                coverage_pct=round((with_device / total_events) * 100, 1) if total_events > 0 else 0,
                missing_by_year={int(row[0]): row[1] for row in device_by_year if row[0]},
            )

            return CoverageResponse(
                timestamp=datetime.now().isoformat(),
                manufacturer_coverage=mfr_coverage,
                product_code_coverage=product_coverage,
                device_coverage=device_coverage,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gaps", response_model=GapsResponse)
async def get_gaps():
    """
    Identify data gaps by year.

    Returns years with significant data quality issues
    and recommendations for remediation.
    """
    try:
        gaps: List[DataGap] = []

        with get_connection(read_only=True) as conn:
            # Check manufacturer coverage by year
            mfr_by_year = conn.execute("""
                SELECT
                    EXTRACT(YEAR FROM date_received)::INTEGER as year,
                    COUNT(*) as total,
                    COUNT(manufacturer_clean) as with_mfr
                FROM master_events
                WHERE date_received IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """).fetchall()

            for row in mfr_by_year:
                if row[0] and row[1] > 0:
                    year = int(row[0])
                    total = row[1]
                    with_mfr = row[2]
                    pct = (with_mfr / total) * 100

                    if pct < 50:
                        gaps.append(DataGap(
                            year=year,
                            issue=f"Only {pct:.1f}% have manufacturer data",
                            severity="critical",
                            affected_records=total - with_mfr,
                            recommendation=f"Download device files for {year} and run populate_master_from_devices()",
                        ))
                    elif pct < 90:
                        gaps.append(DataGap(
                            year=year,
                            issue=f"Low manufacturer coverage: {pct:.1f}%",
                            severity="warning",
                            affected_records=total - with_mfr,
                            recommendation=f"Verify device files for {year} are fully loaded",
                        ))

            # Check for years with no device records
            device_by_year = conn.execute("""
                SELECT
                    EXTRACT(YEAR FROM m.date_received)::INTEGER as year,
                    COUNT(*) as total,
                    COUNT(DISTINCT CASE WHEN d.mdr_report_key IS NOT NULL THEN m.mdr_report_key END) as with_device
                FROM master_events m
                LEFT JOIN devices d ON m.mdr_report_key = d.mdr_report_key
                WHERE m.date_received IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """).fetchall()

            for row in device_by_year:
                if row[0] and row[1] > 0:
                    year = int(row[0])
                    total = row[1]
                    with_device = row[2]
                    pct = (with_device / total) * 100

                    if pct < 10:
                        gaps.append(DataGap(
                            year=year,
                            issue=f"Almost no device records ({pct:.1f}%)",
                            severity="critical",
                            affected_records=total - with_device,
                            recommendation=f"Device file for {year} is missing - download foidev{year}.zip",
                        ))

            critical_count = sum(1 for g in gaps if g.severity == "critical")

            return GapsResponse(
                timestamp=datetime.now().isoformat(),
                total_gaps=len(gaps),
                critical_gaps=critical_count,
                gaps=gaps,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_summary():
    """
    Get a quick summary of database health.

    Returns key metrics for monitoring dashboard.
    """
    try:
        with get_connection(read_only=True) as conn:
            # Get basic counts
            counts = conn.execute("""
                SELECT
                    (SELECT COUNT(*) FROM master_events) as events,
                    (SELECT COUNT(*) FROM devices) as devices,
                    (SELECT COUNT(*) FROM patients) as patients,
                    (SELECT COUNT(*) FROM mdr_text) as text_records
            """).fetchone()

            # Get manufacturer coverage
            mfr_stats = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(manufacturer_clean) as with_mfr
                FROM master_events
            """).fetchone()

            # Get date range
            date_range = conn.execute("""
                SELECT
                    MIN(date_received),
                    MAX(date_received)
                FROM master_events
                WHERE date_received IS NOT NULL
            """).fetchone()

            total = mfr_stats[0] if mfr_stats[0] else 1

            return {
                "timestamp": datetime.now().isoformat(),
                "counts": {
                    "events": counts[0],
                    "devices": counts[1],
                    "patients": counts[2],
                    "text_records": counts[3],
                },
                "manufacturer_coverage_pct": round((mfr_stats[1] / total) * 100, 1),
                "date_range": {
                    "earliest": str(date_range[0]) if date_range[0] else None,
                    "latest": str(date_range[1]) if date_range[1] else None,
                },
                "health_status": "healthy" if (mfr_stats[1] / total) > 0.9 else "degraded",
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Schema Health Endpoint
# =============================================================================

class ColumnCoverage(BaseModel):
    """Coverage information for a column."""
    name: str
    exists: bool
    coverage_pct: Optional[float] = None
    threshold: Optional[float] = None
    passes_threshold: bool = True
    is_sparse: bool = False


class TableHealth(BaseModel):
    """Health information for a table."""
    name: str
    exists: bool
    row_count: int = 0
    expected_min_rows: int = 0
    passes_row_threshold: bool = True
    missing_columns: List[str] = []
    sparse_columns: List[str] = []
    column_coverage: List[ColumnCoverage] = []
    issues: List[str] = []


class SchemaHealthResponse(BaseModel):
    """Response for schema health endpoint."""
    timestamp: str
    config_version: str
    overall_status: str  # "healthy", "warning", "critical"
    tables: List[TableHealth]
    total_issues: int
    recommendations: List[str]


@router.get("/schema-health", response_model=SchemaHealthResponse)
async def get_schema_health(
    check_coverage: bool = Query(True, description="Check column coverage thresholds"),
):
    """
    Validate database schema against schema_config.yaml.

    Returns:
    - Missing columns
    - Column coverage metrics
    - Sparse columns (<20% populated)
    - Row count validation
    - Recommendations for improvement
    """
    try:
        # Load schema config
        config_path = PROJECT_ROOT / "config" / "schema_config.yaml"
        if not config_path.exists():
            raise HTTPException(status_code=500, detail="Schema config not found")

        with open(config_path) as f:
            schema_config = yaml.safe_load(f)

        tables_health: List[TableHealth] = []
        all_issues: List[str] = []
        recommendations: List[str] = []

        with get_connection(read_only=True) as conn:
            # Validate each table defined in config
            tables_config = schema_config.get("tables", {})

            for table_name, table_def in tables_config.items():
                table_health = TableHealth(
                    name=table_name,
                    exists=False,
                    expected_min_rows=table_def.get("row_count_threshold", 0),
                )

                # Check if table exists
                try:
                    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    table_health.exists = True
                    table_health.row_count = result[0] if result else 0
                except Exception:
                    table_health.issues.append(f"Table {table_name} does not exist")
                    all_issues.append(f"Missing table: {table_name}")
                    tables_health.append(table_health)
                    continue

                # Check row count threshold
                if table_health.row_count < table_health.expected_min_rows:
                    table_health.passes_row_threshold = False
                    table_health.issues.append(
                        f"Row count ({table_health.row_count:,}) below threshold ({table_health.expected_min_rows:,})"
                    )

                # Get actual columns
                actual_cols = conn.execute(f"DESCRIBE {table_name}").fetchall()
                actual_col_names = {row[0] for row in actual_cols}

                # Check expected columns
                columns_config = table_def.get("columns", {})
                for col_name, col_def in columns_config.items():
                    col_coverage = ColumnCoverage(
                        name=col_name,
                        exists=col_name in actual_col_names,
                    )

                    if not col_coverage.exists:
                        table_health.missing_columns.append(col_name)
                        if col_def.get("required", False):
                            table_health.issues.append(f"Missing required column: {col_name}")
                            all_issues.append(f"{table_name}: missing required column {col_name}")
                    elif check_coverage and table_health.row_count > 0:
                        # Calculate coverage
                        try:
                            coverage_result = conn.execute(f"""
                                SELECT COUNT(*)
                                FROM {table_name}
                                WHERE {col_name} IS NOT NULL
                                  AND CAST({col_name} AS VARCHAR) != ''
                            """).fetchone()
                            covered = coverage_result[0] if coverage_result else 0
                            col_coverage.coverage_pct = round((covered / table_health.row_count) * 100, 1)

                            # Check threshold
                            threshold = col_def.get("coverage_threshold")
                            if threshold is not None:
                                col_coverage.threshold = threshold * 100
                                col_coverage.passes_threshold = (col_coverage.coverage_pct / 100) >= threshold
                                if not col_coverage.passes_threshold:
                                    table_health.issues.append(
                                        f"Column {col_name} coverage ({col_coverage.coverage_pct}%) below threshold ({col_coverage.threshold}%)"
                                    )

                            # Mark sparse columns
                            if col_coverage.coverage_pct < 20:
                                col_coverage.is_sparse = True
                                if col_name not in schema_config.get("data_quality", {}).get("known_sparse_columns", []):
                                    table_health.sparse_columns.append(col_name)

                        except Exception:
                            pass  # Column might not be text-castable

                    table_health.column_coverage.append(col_coverage)

                tables_health.append(table_health)

        # Determine overall status
        critical_issues = sum(1 for t in tables_health if not t.exists or t.missing_columns)
        warning_issues = sum(1 for t in tables_health if t.issues and t.exists)

        if critical_issues > 0:
            overall_status = "critical"
        elif warning_issues > 0:
            overall_status = "warning"
        else:
            overall_status = "healthy"

        # Generate recommendations
        for table in tables_health:
            if not table.exists:
                recommendations.append(f"Create missing table: {table.name}")
            elif table.missing_columns:
                recommendations.append(f"Add missing columns to {table.name}: {', '.join(table.missing_columns)}")
            if table.sparse_columns:
                recommendations.append(f"Investigate sparse columns in {table.name}: {', '.join(table.sparse_columns[:3])}")

        return SchemaHealthResponse(
            timestamp=datetime.now().isoformat(),
            config_version=schema_config.get("schema_version", "unknown"),
            overall_status=overall_status,
            tables=tables_health,
            total_issues=len(all_issues),
            recommendations=recommendations[:10],  # Limit to top 10
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/column-coverage/{table_name}")
async def get_column_coverage(
    table_name: str,
    min_coverage: float = Query(0.0, ge=0, le=1, description="Minimum coverage filter"),
):
    """
    Get detailed column coverage for a specific table.

    Returns coverage percentage for every column in the table,
    optionally filtered by minimum coverage threshold.
    """
    try:
        with get_connection(read_only=True) as conn:
            # Verify table exists
            try:
                total_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                total_rows = total_result[0] if total_result else 0
            except Exception:
                raise HTTPException(status_code=404, detail=f"Table {table_name} not found")

            if total_rows == 0:
                return {
                    "timestamp": datetime.now().isoformat(),
                    "table": table_name,
                    "row_count": 0,
                    "columns": [],
                }

            # Get all columns
            columns = conn.execute(f"DESCRIBE {table_name}").fetchall()

            coverage_data = []
            for col in columns:
                col_name = col[0]
                col_type = col[1]

                try:
                    covered_result = conn.execute(f"""
                        SELECT COUNT(*)
                        FROM {table_name}
                        WHERE {col_name} IS NOT NULL
                          AND CAST({col_name} AS VARCHAR) != ''
                    """).fetchone()
                    covered = covered_result[0] if covered_result else 0
                    coverage_pct = covered / total_rows

                    if coverage_pct >= min_coverage:
                        coverage_data.append({
                            "name": col_name,
                            "type": col_type,
                            "non_null_count": covered,
                            "null_count": total_rows - covered,
                            "coverage_pct": round(coverage_pct * 100, 2),
                            "is_sparse": coverage_pct < 0.2,
                            "is_well_populated": coverage_pct >= 0.8,
                        })
                except Exception:
                    # Some columns might not support casting
                    coverage_data.append({
                        "name": col_name,
                        "type": col_type,
                        "non_null_count": None,
                        "null_count": None,
                        "coverage_pct": None,
                        "is_sparse": None,
                        "is_well_populated": None,
                    })

            # Sort by coverage descending
            coverage_data.sort(key=lambda x: x.get("coverage_pct") or 0, reverse=True)

            return {
                "timestamp": datetime.now().isoformat(),
                "table": table_name,
                "row_count": total_rows,
                "column_count": len(columns),
                "columns": coverage_data,
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
