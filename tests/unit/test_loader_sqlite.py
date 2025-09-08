import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from py_load_eurostat.loader.sqlite import SqliteLoader
from py_load_eurostat.models import (
    DSD,
    Attribute,
    Code,
    Codelist,
    Dimension,
    Observation,
)


@pytest.fixture
def sample_dsd():
    """A sample DSD fixture for testing."""
    return DSD(
        id="SAMPLE_DSD",
        version="1.0",
        dimensions=[
            Dimension(id="geo", position=0, codelist_id="CL_GEO"),
            Dimension(id="freq", position=1, codelist_id="CL_FREQ"),
        ],
        attributes=[Attribute(id="OBS_FLAG")],
        primary_measure_id="OBS_VALUE",
    )


@pytest.fixture
def sample_codelists():
    """A sample codelists fixture."""
    return {
        "CL_GEO": Codelist(
            id="CL_GEO",
            version="1.0",
            codes={
                "DE": Code(id="DE", name="Germany"),
                "FR": Code(id="FR", name="France"),
            },
        )
    }


@pytest.fixture
def sample_data_stream():
    """A generator fixture for a stream of observations."""

    def _generator():
        yield Observation(
            dimensions={"geo": "DE", "freq": "A"},
            time_period="2022",
            value=100.1,
            flags="p",
        )
        yield Observation(
            dimensions={"geo": "FR", "freq": "A"},
            time_period="2022",
            value=200.2,
            flags="e",
        )

    return _generator()


class TestSqliteLoader:
    def test_full_load_cycle_in_memory(self, sample_dsd, sample_codelists, sample_data_stream):
        """
        Tests the full data loading cycle using an in-memory SQLite database.
        This specifically tests the executemany() fallback path.
        """
        loader = SqliteLoader(db_name=":memory:")
        schema = "eurostat_data"
        table_name = "sample_table"

        # 1. Prepare Schema
        loader.prepare_schema(sample_dsd, table_name, schema)

        # Assert schema was created
        conn = loader.conn
        res = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{schema}_{table_name}'")
        assert res.fetchone() is not None

        # 2. Manage Codelists
        loader.manage_codelists(sample_codelists, "eurostat_meta")

        # Assert codelists loaded
        res = conn.execute("SELECT COUNT(*) FROM eurostat_meta_cl_geo").fetchone()
        assert res[0] == 2
        res = conn.execute("SELECT label_en FROM eurostat_meta_cl_geo WHERE code='DE'").fetchone()
        assert res[0] == "Germany"

        # 3. Bulk Load Staging
        staging_table, row_count = loader.bulk_load_staging(table_name, schema, sample_data_stream)
        assert row_count == 2
        assert staging_table == f"staging_{schema}_{table_name}"

        # Assert data is in staging table
        res = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()
        assert res[0] == 2

        # 4. Finalize Load
        loader.finalize_load(staging_table, table_name, schema)

        # Assert data is in final table and staging table is gone
        res = conn.execute(f"SELECT COUNT(*) FROM {schema}_{table_name}").fetchone()
        assert res[0] == 2
        res = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{staging_table}'")
        assert res.fetchone() is None

        loader.close_connection()

    def test_bulk_load_with_file_db(self, tmp_path, sample_dsd, sample_data_stream):
        """
        Tests the bulk loading mechanism using a file-based database to ensure
        the subprocess/CLI import path is triggered and works correctly.
        """
        db_file = tmp_path / "test.db"
        loader = SqliteLoader(db_name=str(db_file))
        schema = "eurostat_data"
        table_name = "sample_table"

        loader.prepare_schema(sample_dsd, table_name, schema)

        staging_table, row_count = loader.bulk_load_staging(table_name, schema, sample_data_stream)
        assert row_count == 2

        # Manually connect to verify data
        conn = sqlite3.connect(db_file)
        res = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()
        assert res[0] == 2
        conn.close()

        loader.close_connection()
