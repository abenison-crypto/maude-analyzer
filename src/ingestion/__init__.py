"""Data ingestion module for loading FDA MAUDE data."""

from .download import MAUDEDownloader, download_sample_data, DownloadResult
from .parser import MAUDEParser, ParseResult, parse_all_files
from .transformer import DataTransformer, transform_record
from .loader import MAUDELoader, LoadResult, load_lookup_tables
from .validator import DataValidator, ValidationReport, print_validation_report
from .openfda import OpenFDAClient, OpenFDAResult, fetch_recent_updates
from .updater import (
    DataUpdater,
    DataStatus,
    UpdateStatus,
    UpdateSource,
    get_update_status,
    run_incremental_update,
)

__all__ = [
    # Download
    "MAUDEDownloader",
    "download_sample_data",
    "DownloadResult",
    # Parser
    "MAUDEParser",
    "ParseResult",
    "parse_all_files",
    # Transformer
    "DataTransformer",
    "transform_record",
    # Loader
    "MAUDELoader",
    "LoadResult",
    "load_lookup_tables",
    # Validator
    "DataValidator",
    "ValidationReport",
    "print_validation_report",
    # openFDA
    "OpenFDAClient",
    "OpenFDAResult",
    "fetch_recent_updates",
    # Updater
    "DataUpdater",
    "DataStatus",
    "UpdateStatus",
    "UpdateSource",
    "get_update_status",
    "run_incremental_update",
]
