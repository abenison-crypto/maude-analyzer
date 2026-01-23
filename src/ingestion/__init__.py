"""Data ingestion module for loading FDA MAUDE data."""

from .download import (
    MAUDEDownloader,
    download_sample_data,
    download_historical_data,
    DownloadResult,
    DownloadTracker,
    INCREMENTAL_FILES,
)
from .parser import MAUDEParser, ParseResult, SchemaInfo, parse_all_files, FILE_COLUMNS
from .transformer import DataTransformer, SchemaAwareTransformer, transform_record
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
from .fda_discovery import (
    FDADiscovery,
    FileInfo,
    DiscoveryResult,
    check_fda_updates,
)
from .change_processor import (
    ChangeProcessor,
    ChangeResult,
    process_change_file,
    process_all_change_files,
)

__all__ = [
    # Download
    "MAUDEDownloader",
    "download_sample_data",
    "download_historical_data",
    "DownloadResult",
    "DownloadTracker",
    "INCREMENTAL_FILES",
    # Parser
    "MAUDEParser",
    "ParseResult",
    "SchemaInfo",
    "parse_all_files",
    "FILE_COLUMNS",
    # Transformer
    "DataTransformer",
    "SchemaAwareTransformer",
    "transform_record",
    # Loader
    "MAUDELoader",
    "LoadResult",
    "load_lookup_tables",
    # Validator
    "DataValidator",
    "ValidationReport",
    "print_validation_report",
    # openFDA (DEPRECATED for data ingestion - use for queries only)
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
    # FDA Discovery (new file detection)
    "FDADiscovery",
    "FileInfo",
    "DiscoveryResult",
    "check_fda_updates",
    # Change Processor (CHANGE file handling)
    "ChangeProcessor",
    "ChangeResult",
    "process_change_file",
    "process_all_change_files",
]
