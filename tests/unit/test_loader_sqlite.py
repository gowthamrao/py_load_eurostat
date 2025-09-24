import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.sqlite import SQLiteLoader
from py_load_eurostat.models import (
    Code,
    Codelist,
    Observation,
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
            loader.prepare_schema(
                sample_dsd,
                table_name,
                data_schema,
                representation="Standard",
                meta_schema=meta_schema,
            )
            conn = loader.conn
            res = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (data_table_fqn,),
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
            loader.finalize_load(
                staging_table, table_name, data_schema, strategy="swap"
            )
            res = conn.execute(f"SELECT COUNT(*) FROM {data_table_fqn}").fetchone()
            assert res[0] == 2
            res = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (staging_table,),
            )
            assert res.fetchone() is None

        finally:
            loader.close_connection()

    def test_create_connection_error(self, mocker):
        """Test that a connection error is handled."""
        mocker.patch("sqlite3.connect", side_effect=sqlite3.Error("Connection failed"))
        with pytest.raises(sqlite3.Error):
            SQLiteLoader(DatabaseSettings(name="dummy.db"))

    def test_prepare_schema_exception_rolls_back(self, db_settings, sample_dsd, mocker):
        """Test that an exception during schema preparation triggers a rollback."""
        loader = SQLiteLoader(db_settings)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.execute.side_effect = [
            # First call for "BEGIN"
            None,
            # Second call for "_table_exists"
            None,
            # Third call for "CREATE TABLE"
            Exception("Boom"),
            # Fourth call for "ROLLBACK"
            None,
        ]
        loader.conn = mock_conn

        with pytest.raises(Exception, match="Boom"):
            loader.prepare_schema(sample_dsd, "my_data", "public", "", "")

        mock_cursor.execute.assert_any_call("ROLLBACK")

    def test_manage_codelists_empty(self, db_settings):
        """Test that manage_codelists handles empty codelists gracefully."""
        loader = SQLiteLoader(db_settings)
        loader.manage_codelists(
            {"EMPTY_CL": Codelist(id="EMPTY_CL", version="1.0", codes={})}, "meta"
        )
        # No assertion needed, just checking that it doesn't crash

    def test_manage_codelists_error_rolls_back(self, db_settings, mocker):
        """Test that an error during codelist management triggers a rollback."""
        loader = SQLiteLoader(db_settings)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.executemany.side_effect = sqlite3.Error("DB error")
        loader.conn = mock_conn

        codelist = Codelist(
            id="CL_TEST", version="1.0", codes={"a": Code(id="a", name="A")}
        )
        with pytest.raises(sqlite3.Error):
            loader.manage_codelists({"CL_TEST": codelist}, "meta")

        mock_cursor.execute.assert_any_call("ROLLBACK")

    def test_bulk_load_staging_no_dsd(self, db_settings):
        """Test that bulk_load_staging raises an error if DSD is not set."""
        loader = SQLiteLoader(db_settings)
        with pytest.raises(RuntimeError, match="DSD must be set"):
            loader.bulk_load_staging("my_data", "public", iter([]))

    def test_finalize_load_invalid_strategy(self, db_settings):
        """Test that finalize_load raises an error for an invalid strategy."""
        loader = SQLiteLoader(db_settings)
        with pytest.raises(ValueError, match="only supports 'swap'"):
            loader.finalize_load("staging", "target", "public", "merge")

    def test_finalize_load_error_rolls_back(self, db_settings, mocker):
        """Test that an error during finalization triggers a rollback."""
        loader = SQLiteLoader(db_settings)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        def execute_side_effect(sql):
            if "DROP TABLE" in sql:
                raise Exception("DB error")
            return None

        mock_cursor.execute.side_effect = execute_side_effect
        loader.conn = mock_conn

        with pytest.raises(Exception):
            loader.finalize_load("staging", "target", "public", "swap")

        mock_cursor.execute.assert_any_call("ROLLBACK")

    def test_get_ingestion_state_no_history_table(self, db_settings):
        """Test getting ingestion state when the history table does not exist."""
        loader = SQLiteLoader(db_settings)
        state = loader.get_ingestion_state("some_dataset", "public")
        assert state is None

    def test_save_ingestion_state_error_rolls_back(self, db_settings, mocker):
        """Test that an error during saving ingestion state triggers a rollback."""
        from py_load_eurostat.models import IngestionHistory

        loader = SQLiteLoader(db_settings)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        def execute_side_effect(sql, *args):
            if "INSERT INTO" in sql:
                raise Exception("DB error")
            return None

        mock_cursor.execute.side_effect = execute_side_effect
        loader.conn = mock_conn

        record = IngestionHistory(
            dataset_id="test",
            dsd_hash="hash",
            load_strategy="Full",
            representation="Standard",
        )

        with pytest.raises(Exception):
            loader.save_ingestion_state(record, "public")

        mock_cursor.execute.assert_any_call("ROLLBACK")

    def test_get_required_columns_no_primary_measure(self, db_settings, sample_dsd):
        """Test that _get_required_columns handles a DSD with no primary measure."""
        loader = SQLiteLoader(db_settings)
        sample_dsd.primary_measure_id = "NON_EXISTENT"
        sample_dsd.measures = []
        columns = loader._get_required_columns(sample_dsd)
        assert columns["NON_EXISTENT"] == "REAL"

    def test_get_ingestion_state_returns_none(self, db_settings, mocker):
        """Test that get_ingestion_state returns None when no record is found."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mocker.patch("sqlite3.connect", return_value=mock_conn)
        loader = SQLiteLoader(db_settings)
        state = loader.get_ingestion_state("some_dataset", "public")
        assert state is None
