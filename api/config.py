"""FastAPI application settings."""

from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache
import os


def _parse_cors_origins() -> list[str]:
    """Parse CORS origins from environment variable."""
    cors_env = os.getenv("MAUDE_CORS_ORIGINS", "")
    if cors_env:
        return [origin.strip() for origin in cors_env.split(",")]
    # Default development origins
    return ["http://localhost:3000", "http://localhost:5173", "http://localhost:3002"]


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # App info
    app_name: str = "MAUDE Analyzer API"
    version: str = "2.0.0"
    debug: bool = os.getenv("MAUDE_DEBUG", "false").lower() == "true"

    # Database
    database_path: Path = Path(
        os.getenv("MAUDE_DB_PATH", str(Path(__file__).parent.parent / "data" / "maude.duckdb"))
    )

    # CORS - configurable via environment variable
    cors_origins: list[str] = _parse_cors_origins()

    # Pagination
    default_page_size: int = 50
    max_page_size: int = 1000

    # Cache
    cache_ttl_seconds: int = int(os.getenv("MAUDE_CACHE_TTL", "3600"))

    # Production mode
    production: bool = os.getenv("MAUDE_PRODUCTION", "false").lower() == "true"

    class Config:
        env_prefix = "MAUDE_"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
