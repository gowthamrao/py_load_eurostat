# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


from unittest.mock import MagicMock

import pytest

from py_load_eurostat.config import AppSettings, DatabaseSettings, DatabaseType
from py_load_eurostat.loader.factory import get_loader
from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.loader.sqlite import SQLiteLoader


def test_get_loader_postgres(mocker):
    """Test that get_loader returns a PostgresLoader for db_type 'postgres'."""
    mocker.patch("py_load_eurostat.loader.postgresql.PostgresLoader._create_connection")
    settings = AppSettings(
        db_type=DatabaseType.POSTGRES,
        db=DatabaseSettings(
            host="localhost",
            port=5432,
            user="user",
            password="password",
            name="db",
        ),
    )
    loader = get_loader(settings)
    assert isinstance(loader, PostgresLoader)


def test_get_loader_sqlite(mocker):
    """Test that get_loader returns a SQLiteLoader for db_type 'sqlite'."""
    mocker.patch("py_load_eurostat.loader.sqlite.SQLiteLoader._create_connection")
    settings = AppSettings(
        db_type=DatabaseType.SQLITE,
        db=DatabaseSettings(
            host="localhost",
            port=5432,
            user="user",
            password="password",
            name="db",
        ),
    )
    loader = get_loader(settings)
    assert isinstance(loader, SQLiteLoader)


def test_get_loader_unsupported_db_type():
    """
    Test that get_loader raises a ValueError for an unsupported database type.
    """
    mock_settings = MagicMock(spec=AppSettings)
    mock_settings.db_type = "invalid"
    with pytest.raises(ValueError, match="Unsupported database type: invalid"):
        get_loader(mock_settings)
