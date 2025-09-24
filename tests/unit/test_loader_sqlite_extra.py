# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import sqlite3

import pytest

from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.sqlite import SQLiteLoader
from py_load_eurostat.models import DSD, Dimension, Measure


@pytest.fixture
def db_settings_extra(tmp_path):
    """Fixture for a temporary database for extra tests."""
    db_file = tmp_path / "test_extra.db"
    return DatabaseSettings(name=str(db_file))


def test_get_required_columns_no_primary_measure(db_settings_extra):
    """
    Test _get_required_columns when the DSD's primary_measure_id is not in measures.
    This should cover the 'else' block for the primary measure.
    """
    loader = SQLiteLoader(db_settings_extra)
    dsd_no_measure = DSD(
        id="DSD_NO_MEASURE",
        name="DSD with no matching primary measure",
        version="1.0",
        dimensions=[Dimension(id="geo", name="Geo", position=0, data_type="String")],
        measures=[Measure(id="WRONG_ID", name="Some other measure")],
        primary_measure_id="OBS_VALUE",
        attributes=[],
    )

    columns = loader._get_required_columns(dsd_no_measure)

    # Check that the primary measure is added with a default type 'REAL'
    assert "OBS_VALUE" in columns
    assert columns["OBS_VALUE"] == "REAL"
    assert "geo" in columns


def test_prepare_schema_rollback_on_error(db_settings_extra, sample_dsd):
    """
    Test that prepare_schema rolls back a transaction if a real DB error occurs,
    without using mocks. This test now forces an error by using an invalid table name.
    """
    loader = SQLiteLoader(db_settings_extra)
    # An invalid table name with a dot will cause a syntax error
    invalid_table_name = "invalid.table"
    schema = "main"

    with pytest.raises(sqlite3.OperationalError) as excinfo:
        loader.prepare_schema(sample_dsd, invalid_table_name, schema, "", "")

    assert "syntax error" in str(excinfo.value).lower()

    # Verify that the invalid table was not created
    conn = loader.conn
    cursor = conn.cursor()
    # The fqn will be 'main__invalid.table', which is not a valid identifier
    # We can check if any table was created with a name like 'main__invalid'
    cursor.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name LIKE 'main__invalid%'"
    )
    tables = cursor.fetchall()
    assert len(tables) == 0
