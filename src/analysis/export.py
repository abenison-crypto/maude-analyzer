"""Data export functionality for MAUDE data."""

import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime
import io
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger

logger = get_logger("export")


# Default columns for exports
DEFAULT_COLUMNS = [
    "mdr_report_key",
    "date_received",
    "date_of_event",
    "manufacturer_name",
    "manufacturer_clean",
    "product_code",
    "event_type",
    "report_number",
    "type_of_report",
    "event_location",
    "pma_pmn_number",
]

# Extended columns including narratives
EXTENDED_COLUMNS = DEFAULT_COLUMNS + [
    "product_problem_flag",
    "adverse_event_flag",
    "report_source_code",
    "health_professional",
    "initial_report_to_fda",
    "received_year",
    "received_month",
]

# Excel sheet configurations
EXCEL_CONFIG = {
    "Summary": {
        "columns": [
            "mdr_report_key",
            "date_received",
            "manufacturer_clean",
            "product_code",
            "event_type",
            "type_of_report",
        ],
        "column_widths": {
            "mdr_report_key": 15,
            "date_received": 12,
            "manufacturer_clean": 20,
            "product_code": 12,
            "event_type": 12,
            "type_of_report": 20,
        },
    },
    "Full Details": {
        "columns": EXTENDED_COLUMNS,
        "column_widths": {
            "mdr_report_key": 15,
            "date_received": 12,
            "manufacturer_name": 30,
            "manufacturer_clean": 20,
            "event_location": 25,
        },
    },
}


class DataExporter:
    """Export MAUDE data to various formats."""

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize exporter.

        Args:
            output_dir: Directory for file exports.
        """
        self.output_dir = output_dir or config.data.exports_path
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_to_csv(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        columns: Optional[List[str]] = None,
        include_index: bool = False,
    ) -> Path:
        """
        Export DataFrame to CSV file.

        Args:
            df: DataFrame to export.
            filename: Output filename (generated if None).
            columns: Columns to include (all if None).
            include_index: Whether to include row index.

        Returns:
            Path to exported file.
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"maude_export_{timestamp}.csv"

        if columns:
            # Only include columns that exist in the DataFrame
            existing_cols = [c for c in columns if c in df.columns]
            df = df[existing_cols]

        filepath = self.output_dir / filename
        df.to_csv(filepath, index=include_index, encoding="utf-8")

        logger.info(f"Exported {len(df)} records to {filepath}")
        return filepath

    def export_to_csv_buffer(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
    ) -> io.StringIO:
        """
        Export DataFrame to CSV in-memory buffer (for Streamlit download).

        Args:
            df: DataFrame to export.
            columns: Columns to include.

        Returns:
            StringIO buffer with CSV data.
        """
        if columns:
            existing_cols = [c for c in columns if c in df.columns]
            df = df[existing_cols]

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, encoding="utf-8")
        buffer.seek(0)
        return buffer

    def export_to_excel(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        include_summary_sheet: bool = True,
        include_narratives: bool = False,
        narratives_df: Optional[pd.DataFrame] = None,
    ) -> Path:
        """
        Export DataFrame to formatted Excel file.

        Args:
            df: Main DataFrame to export.
            filename: Output filename (generated if None).
            include_summary_sheet: Add summary statistics sheet.
            include_narratives: Add narratives sheet.
            narratives_df: DataFrame with narrative text.

        Returns:
            Path to exported file.
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"maude_export_{timestamp}.xlsx"

        filepath = self.output_dir / filename

        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            # Summary sheet
            if include_summary_sheet:
                summary_cols = EXCEL_CONFIG["Summary"]["columns"]
                existing_cols = [c for c in summary_cols if c in df.columns]
                summary_df = df[existing_cols].copy()
                summary_df.to_excel(writer, sheet_name="Summary", index=False)
                self._format_sheet(
                    writer.sheets["Summary"],
                    EXCEL_CONFIG["Summary"]["column_widths"],
                )

            # Full details sheet
            detail_cols = EXCEL_CONFIG["Full Details"]["columns"]
            existing_cols = [c for c in detail_cols if c in df.columns]
            detail_df = df[existing_cols].copy()
            detail_df.to_excel(writer, sheet_name="Full Details", index=False)
            self._format_sheet(
                writer.sheets["Full Details"],
                EXCEL_CONFIG["Full Details"]["column_widths"],
            )

            # Narratives sheet
            if include_narratives and narratives_df is not None:
                narratives_df.to_excel(writer, sheet_name="Narratives", index=False)
                # Set text wrap for narrative column
                ws = writer.sheets["Narratives"]
                ws.column_dimensions["C"].width = 100  # text_content column

            # Statistics sheet
            stats_df = self._create_statistics_df(df)
            stats_df.to_excel(writer, sheet_name="Statistics", index=False)

        logger.info(f"Exported {len(df)} records to Excel: {filepath}")
        return filepath

    def export_to_excel_buffer(
        self,
        df: pd.DataFrame,
        include_summary_sheet: bool = True,
    ) -> io.BytesIO:
        """
        Export DataFrame to Excel in-memory buffer (for Streamlit download).

        Args:
            df: DataFrame to export.
            include_summary_sheet: Add summary statistics sheet.

        Returns:
            BytesIO buffer with Excel data.
        """
        buffer = io.BytesIO()

        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            if include_summary_sheet:
                summary_cols = EXCEL_CONFIG["Summary"]["columns"]
                existing_cols = [c for c in summary_cols if c in df.columns]
                if existing_cols:
                    summary_df = df[existing_cols].copy()
                    summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # Full data
            df.to_excel(writer, sheet_name="Full Data", index=False)

            # Statistics
            stats_df = self._create_statistics_df(df)
            stats_df.to_excel(writer, sheet_name="Statistics", index=False)

        buffer.seek(0)
        return buffer

    def _format_sheet(
        self,
        worksheet,
        column_widths: Dict[str, int],
    ) -> None:
        """Apply formatting to Excel worksheet."""
        from openpyxl.styles import Font, PatternFill, Alignment

        # Header formatting
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")

        for cell in worksheet[1]:
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center")

        # Column widths
        for col_name, width in column_widths.items():
            # Find column letter by header name
            for cell in worksheet[1]:
                if cell.value == col_name:
                    col_letter = cell.column_letter
                    worksheet.column_dimensions[col_letter].width = width
                    break

        # Freeze header row
        worksheet.freeze_panes = "A2"

        # Auto-filter
        worksheet.auto_filter.ref = worksheet.dimensions

    def _create_statistics_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create summary statistics DataFrame."""
        stats = []

        stats.append({"Metric": "Total Records", "Value": len(df)})

        if "event_type" in df.columns:
            event_counts = df["event_type"].value_counts()
            for event_type, count in event_counts.items():
                label = {
                    "D": "Deaths",
                    "IN": "Injuries",
                    "M": "Malfunctions",
                    "O": "Other",
                }.get(event_type, event_type)
                stats.append({"Metric": f"Event Type: {label}", "Value": count})

        if "manufacturer_clean" in df.columns:
            stats.append({
                "Metric": "Unique Manufacturers",
                "Value": df["manufacturer_clean"].nunique(),
            })

        if "product_code" in df.columns:
            stats.append({
                "Metric": "Unique Product Codes",
                "Value": df["product_code"].nunique(),
            })

        if "date_received" in df.columns:
            df["date_received"] = pd.to_datetime(df["date_received"], errors="coerce")
            valid_dates = df["date_received"].dropna()
            if len(valid_dates) > 0:
                stats.append({
                    "Metric": "Date Range Start",
                    "Value": valid_dates.min().strftime("%Y-%m-%d"),
                })
                stats.append({
                    "Metric": "Date Range End",
                    "Value": valid_dates.max().strftime("%Y-%m-%d"),
                })

        stats.append({
            "Metric": "Export Date",
            "Value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

        return pd.DataFrame(stats)

    def generate_filename(
        self,
        base_name: str = "maude_export",
        extension: str = "csv",
        include_timestamp: bool = True,
    ) -> str:
        """Generate export filename."""
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"{base_name}_{timestamp}.{extension}"
        return f"{base_name}.{extension}"
