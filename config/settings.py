"""Application settings and configuration."""

from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent


@dataclass
class DatabaseConfig:
    """Database configuration settings."""

    path: Path = field(
        default_factory=lambda: Path(
            os.getenv("MAUDE_DB_PATH", str(PROJECT_ROOT / "data" / "maude.duckdb"))
        )
    )
    read_only: bool = False
    memory_limit: str = "8GB"
    threads: int = -1  # Use all available threads


@dataclass
class APIConfig:
    """FDA API configuration settings."""

    fda_api_key: Optional[str] = field(default_factory=lambda: os.getenv("FDA_API_KEY"))
    base_url: str = "https://api.fda.gov/device/event.json"

    @property
    def rate_limit_per_minute(self) -> int:
        """Rate limit depends on whether API key is configured."""
        return 240 if self.fda_api_key else 40

    @property
    def rate_limit_per_day(self) -> int:
        """Daily rate limit depends on whether API key is configured."""
        return 120000 if self.fda_api_key else 1000

    max_results_per_call: int = 1000
    max_total_results: int = 25000


@dataclass
class DataConfig:
    """Data paths configuration."""

    raw_path: Path = field(
        default_factory=lambda: Path(
            os.getenv("RAW_DATA_PATH", str(PROJECT_ROOT / "data" / "raw"))
        )
    )
    processed_path: Path = field(
        default_factory=lambda: Path(
            os.getenv("PROCESSED_DATA_PATH", str(PROJECT_ROOT / "data" / "processed"))
        )
    )
    lookups_path: Path = field(
        default_factory=lambda: PROJECT_ROOT / "data" / "lookups"
    )
    exports_path: Path = field(
        default_factory=lambda: PROJECT_ROOT / "data" / "exports"
    )


@dataclass
class AppConfig:
    """Application configuration settings."""

    name: str = field(default_factory=lambda: os.getenv("APP_NAME", "MAUDE Analyzer"))
    version: str = field(default_factory=lambda: os.getenv("APP_VERSION", "1.0.0"))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    query_timeout: int = field(
        default_factory=lambda: int(os.getenv("QUERY_TIMEOUT_SECONDS", "60"))
    )
    max_export_records: int = field(
        default_factory=lambda: int(os.getenv("MAX_EXPORT_RECORDS", "1000000"))
    )
    cache_ttl: int = field(
        default_factory=lambda: int(os.getenv("CACHE_TTL_SECONDS", "3600"))
    )


@dataclass
class Config:
    """Main configuration container."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    data: DataConfig = field(default_factory=DataConfig)
    app: AppConfig = field(default_factory=AppConfig)

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.data.raw_path.mkdir(parents=True, exist_ok=True)
        self.data.processed_path.mkdir(parents=True, exist_ok=True)
        self.data.lookups_path.mkdir(parents=True, exist_ok=True)
        self.database.path.parent.mkdir(parents=True, exist_ok=True)


# Global config instance
config = Config()
