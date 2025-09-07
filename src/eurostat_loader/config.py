"""
Configuration module for the eurostat-loader package.

This module uses pydantic-settings to manage application configuration,
allowing settings to be loaded from environment variables or a .env file.
"""
from pathlib import Path
from functools import lru_cache
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class DatabaseSettings(BaseSettings):
    """
    Defines the configuration for the target database connection.
    """
    host: str = Field(default="localhost", description="Database host address.")
    port: int = Field(default=5432, description="Database port number.")
    user: str = Field(default="postgres", description="Username for database authentication.")
    password: Optional[str] = Field(default=None, description="Password for database authentication.")
    name: str = Field(default="eurostat", description="The name of the database to connect to.")

class CacheSettings(BaseSettings):
    """Defines the configuration for the data caching mechanism."""
    path: Path = Field(
        default_factory=lambda: Path.home() / ".cache" / "eurostat-loader",
        description="The filesystem path for storing cached downloads."
    )
    enabled: bool = Field(default=True, description="A flag to enable or disable caching.")

class LoggingSettings(BaseSettings):
    """Defines the logging configuration."""
    level: str = Field(
        default="INFO",
        description="The logging level, e.g., DEBUG, INFO, WARNING, ERROR."
    )

class AppSettings(BaseSettings):
    """
    The main application settings model.
    """
    model_config = SettingsConfigDict(
        env_prefix='EUROSTAT_LOADER_',
        env_nested_delimiter='__'
    )
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    log: LoggingSettings = Field(default_factory=LoggingSettings)

@lru_cache
def get_settings() -> AppSettings:
    """
    Returns a cached instance of the application settings.
    """
    return AppSettings()
