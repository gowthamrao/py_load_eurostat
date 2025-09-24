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

from py_load_eurostat.loader.postgresql import PostgresLoader


@pytest.mark.parametrize(
    "input_type, expected_type",
    [
        ("character varying(255)", "text"),
        ("char(10)", "text"),
        ("text", "text"),
        ("float8", "double precision"),
        ("double precision", "double precision"),
        ("int8", "bigint"),
        ("bigint", "bigint"),
        ("int4", "integer"),
        ("integer", "integer"),
        ("int2", "smallint"),
        ("smallint", "smallint"),
        ("timestamp with time zone", "timestamptz"),
        ("timestamp without time zone", "timestamptz"),
        ("NUMERIC", "numeric"),
    ],
)
def test_normalize_pg_type(input_type, expected_type):
    """Test that _normalize_pg_type correctly normalizes various PG type strings."""
    # We don't need a real connection for this test
    loader = PostgresLoader.__new__(PostgresLoader)
    assert loader._normalize_pg_type(input_type) == expected_type


def test_finalize_merge_raises_error_if_dsd_is_not_set():
    """Test that _finalize_merge raises a RuntimeError if DSD is not set."""
    # Create a loader instance without a real connection
    loader = PostgresLoader.__new__(PostgresLoader)
    loader.dsd = None

    with pytest.raises(RuntimeError, match="DSD must be set to perform a merge."):
        loader._finalize_merge("staging_table", "target_table", "schema")


def test_close_connection_handles_closed_and_none_connections():
    """Test that close_connection can be called multiple times safely."""
    # Create a loader instance without a real connection for the first part
    loader = PostgresLoader.__new__(PostgresLoader)

    # Test with conn = None
    loader.conn = None
    loader.close_connection()  # Should not raise

    # Test with a mock connection
    mock_conn = MagicMock()
    mock_conn.closed = False
    loader.conn = mock_conn

    loader.close_connection()
    assert mock_conn.close.call_count == 1

    # Make the mock connection appear closed
    mock_conn.closed = True
    loader.close_connection()
    # The close method should not be called again
    assert mock_conn.close.call_count == 1
