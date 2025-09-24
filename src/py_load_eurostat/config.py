"""
Configuration module for the py-load-eurostat package.

This module uses pydantic-settings to manage application configuration,
allowing settings to be loaded from environment variables or a .env file.
"""

from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseType(str, Enum):
    """Enumeration for the supported database types."""

    POSTGRES = "postgres"
    SQLITE = "sqlite"


class DatabaseSettings(BaseSettings):
    """
    Defines the configuration for the target database connection.
    """

    host: str = Field(default="localhost", description="Database host address.")
    port: int = Field(default=5432, description="Database port number.")
    user: str = Field(
        default="postgres", description="Username for database authentication."
    )
    password: Optional[str] = Field(
        default=None, description="Password for database authentication."
    )
    name: str = Field(
        default="eurostat", description="The name of the database to connect to."
    )
    use_unlogged_tables: bool = Field(
        default=True,
        description=(
            "For PostgreSQL, use UNLOGGED tables for staging to improve performance."
        ),
    )


class CacheSettings(BaseSettings):
    """Defines the configuration for the data caching mechanism."""

    path: Path = Field(
        default_factory=lambda: Path.home() / ".cache" / "py-load-eurostat",
        description="The filesystem path for storing cached downloads.",
    )
    enabled: bool = Field(
        default=True, description="A flag to enable or disable caching."
    )


class LoggingSettings(BaseSettings):
    """Defines the logging configuration."""

    level: str = Field(
        default="INFO",
        description="The logging level, e.g., DEBUG, INFO, WARNING, ERROR.",
    )


class EurostatSettings(BaseSettings):
    """
    Defines settings related to the Eurostat source APIs.
    """

    base_url: HttpUrl = Field(
        default=HttpUrl("https://ec.europa.eu/eurostat/api/dissemination"),
        description="The base URL for the Eurostat Dissemination API.",
    )
    # The new API uses different endpoints for different types of metadata
    sdmx_api_version: str = Field(default="2.1", description="The SDMX API version.")
    sdmx_agency_id: str = Field(default="ESTAT", description="The SDMX agency ID.")


class AppSettings(BaseSettings):
    """
    The main application settings model.
    """

    model_config = SettingsConfigDict(
        env_prefix="PY_LOAD_EUROSTAT_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
    )
    db_type: DatabaseType = Field(
        default=DatabaseType.POSTGRES,
        description="The type of database to connect to.",
    )
    managed_datasets_path: Path = Field(
        default=Path("managed_datasets.yml"),
        description="Path to the YAML file listing datasets to manage.",
    )
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    log: LoggingSettings = Field(default_factory=LoggingSettings)
    eurostat: EurostatSettings = Field(default_factory=EurostatSettings)


settings = AppSettings()
