from pathlib import Path

import pytest

from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.sqlite import SQLiteLoader
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
    from py_load_eurostat.models import Measure

    return DSD(
        id="SAMPLE_DSD",
        name="Sample DSD",
        version="1.0",
        dimensions=[
            Dimension(id="geo", name="Geo", position=0, codelist_id="CL_GEO"),
            Dimension(id="freq", name="Frequency", position=1, codelist_id="CL_FREQ"),
        ],
        attributes=[Attribute(id="OBS_FLAG", name="Observation Flag")],
        measures=[Measure(id="OBS_VALUE", name="Observation Value")],
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


@pytest.fixture
def db_settings(tmp_path: Path) -> DatabaseSettings:
    """Fixture to create DatabaseSettings pointing to a temporary file."""
    db_file = tmp_path / "test_unit.db"
    return DatabaseSettings(name=str(db_file))


class TestSQLiteLoader:
    def test_full_load_cycle(
        self, db_settings, sample_dsd, sample_codelists, sample_data_stream
    ):
        """
        Tests the full data loading cycle using the refactored SQLite loader.
        """
        loader = SQLiteLoader(db_settings)
        data_schema = "eurostat_data"
        meta_schema = "eurostat_meta"
        table_name = "sample_table"
        data_table_fqn = f"{data_schema}__{table_name}"
        codelist_table_fqn = f"{meta_schema}__cl_geo"

        try:
            # 1. Prepare Schema
            loader.prepare_schema(sample_dsd, table_name, data_schema)
            conn = loader.conn
            res = conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{data_table_fqn}'"
            )
            assert res.fetchone() is not None

            # 2. Manage Codelists
            loader.manage_codelists(sample_codelists, meta_schema)
            res = conn.execute(f"SELECT COUNT(*) FROM {codelist_table_fqn}").fetchone()
            assert res[0] == 2
            res = conn.execute(
                f"SELECT label_en FROM {codelist_table_fqn} WHERE code='DE'"
            ).fetchone()
            assert res[0] == "Germany"

            # 3. Bulk Load Staging
            staging_table, row_count = loader.bulk_load_staging(
                table_name, data_schema, sample_data_stream
            )
            assert row_count == 2
            assert staging_table == f"staging_{data_table_fqn}"
            res = conn.execute(f"SELECT COUNT(*) FROM {staging_table}").fetchone()
            assert res[0] == 2

            # 4. Finalize Load
            loader.finalize_load(staging_table, table_name, data_schema)
            res = conn.execute(f"SELECT COUNT(*) FROM {data_table_fqn}").fetchone()
            assert res[0] == 2
            res = conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{staging_table}'"
            )
            assert res.fetchone() is None

        finally:
            loader.close_connection()
