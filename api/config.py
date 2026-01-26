"""FastAPI application settings."""

from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache
import os


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # App info
    app_name: str = "MAUDE Analyzer API"
    version: str = "2.0.0"
    debug: bool = False

    # Database
    database_path: Path = Path(
        os.getenv("MAUDE_DB_PATH", str(Path(__file__).parent.parent / "data" / "maude.duckdb"))
    )

    # CORS
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:5173"]

    # Pagination
    default_page_size: int = 50
    max_page_size: int = 1000

    # Cache
    cache_ttl_seconds: int = 3600

    class Config:
        env_prefix = "MAUDE_"


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
