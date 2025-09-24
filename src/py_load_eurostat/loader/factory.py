# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Factory module for creating database loader instances.

This module provides a centralized way to instantiate the correct
database loader based on the application's configuration.
"""

from ..config import AppSettings, DatabaseType
from .base import LoaderInterface
from .postgresql import PostgresLoader
from .sqlite import SQLiteLoader


def get_loader(settings: AppSettings) -> LoaderInterface:
    """
    Instantiates and returns the appropriate database loader.

    Based on the `db_type` specified in the settings, this function
    returns a concrete implementation of the `LoaderInterface`.

    Args:
        settings: The application settings object.

    Returns:
        An instance of a class that implements the LoaderInterface.

    Raises:
        ValueError: If an unsupported `db_type` is provided.
    """
    if settings.db_type == DatabaseType.POSTGRES:
        return PostgresLoader(settings.db)
    elif settings.db_type == DatabaseType.SQLITE:
        return SQLiteLoader(settings.db)
    else:
        # This case should ideally not be reachable if pydantic validation is working
        raise ValueError(f"Unsupported database type: {settings.db_type}")
