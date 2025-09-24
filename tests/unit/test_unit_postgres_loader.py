from unittest.mock import MagicMock

import pytest

from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.models import DSD, Attribute, Dimension, Measure


@pytest.fixture
def mock_db_settings():
    """Fixture for mock database settings."""
    settings = MagicMock()
    settings.password = "mock_password"
    settings.host = "localhost"
    settings.port = 5432
    settings.user = "test"
    settings.name = "testdb"
    return settings


def test_get_required_columns_dynamic_types(mocker, mock_db_settings):
    """
    Unit test for the _get_required_columns method.
    It verifies that SDMX data types are correctly mapped to PostgreSQL types.
    """
    # 1. Mock the database connection to isolate the method
    mocker.patch("py_load_eurostat.loader.postgresql.PostgresLoader._create_connection")

    # 2. Create a mock DSD object with various data types
    mock_dsd = DSD(
        id="mock_dsd",
        name="Mock DSD for Testing",
        version="1.0",
        dimensions=[
            Dimension(
                id="dim_string", name="String Dim", position=1, data_type="String"
            ),
            Dimension(
                id="dim_integer", name="Integer Dim", position=2, data_type="Integer"
            ),
            Dimension(id="dim_long", name="Long Dim", position=3, data_type="Long"),
            Dimension(
                id="dim_nodtype", name="No DType Dim", position=4, data_type=None
            ),
            Dimension(
                id="dim_unknown",
                name="Unknown DType Dim",
                position=5,
                data_type="UnknownType",
            ),
        ],
        measures=[
            Measure(id="obs_value", name="Observation Value", data_type="Double")
        ],
        attributes=[
            Attribute(id="OBS_FLAG", name="Observation Flag", data_type="String")
        ],
        primary_measure_id="obs_value",
    )

    # 3. Instantiate the loader
    loader = PostgresLoader(db_settings=mock_db_settings)

    # 4. Call the method under test
    required_columns = loader._get_required_columns(mock_dsd)

    # 5. Assert the results
    expected_types = {
        "dim_string": "TEXT",
        "dim_integer": "INTEGER",
        "dim_long": "BIGINT",
        "dim_nodtype": "TEXT",  # Should default to TEXT
        "dim_unknown": "TEXT",  # Should default to TEXT for unknown types
        "obs_value": "DOUBLE PRECISION",
        "OBS_FLAG": "TEXT",
        "time_period": "TEXT",
    }

    assert required_columns == expected_types


def test_init_raises_error_if_no_password(mock_db_settings):
    """Test that ValueError is raised if the password is not provided."""
    import pytest

    mock_db_settings.password = None
    with pytest.raises(ValueError, match="password is required"):
        PostgresLoader(mock_db_settings)


def test_connection_error_handling(mock_db_settings, mocker):
    """Test that operational errors during connection are handled."""
    import psycopg

    mocker.patch("psycopg.connect", side_effect=psycopg.OperationalError("conn failed"))
    with pytest.raises(psycopg.OperationalError):
        PostgresLoader(mock_db_settings)


def test_missing_primary_measure_warning(mock_db_settings, mocker):
    """Test that a warning is logged if the primary measure is not in the DSD."""
    mocker.patch("py_load_eurostat.loader.postgresql.PostgresLoader._create_connection")
    mock_logger = mocker.patch("py_load_eurostat.loader.postgresql.logger.warning")
    mock_dsd = DSD(
        id="mock_dsd",
        version="1.0",
        dimensions=[],
        attributes=[],
        measures=[],
        primary_measure_id="NON_EXISTENT",
    )
    loader = PostgresLoader(db_settings=mock_db_settings)
    loader._get_required_columns(mock_dsd)
    mock_logger.assert_called_once()
    assert "Primary measure 'NON_EXISTENT' not found" in mock_logger.call_args[0][0]


def test_prepare_schema_type_mismatch_error(mock_db_settings, mocker):
    """Test that a data type mismatch raises NotImplementedError."""
    mocker.patch("py_load_eurostat.loader.postgresql.PostgresLoader._create_connection")
    loader = PostgresLoader(db_settings=mock_db_settings)
    mock_dsd = DSD(
        id="mock_dsd",
        version="1.0",
        dimensions=[Dimension(id="geo", position=1, data_type="String")],
        attributes=[],
        measures=[],
    )
    # Simulate an existing table with a mismatched column type
    mocker.patch.object(loader, "_table_exists", return_value=True)
    mocker.patch.object(
        loader, "_get_existing_column_types", return_value={"geo": "INTEGER"}
    )
    with pytest.raises(NotImplementedError, match="Data type mismatch"):
        loader.prepare_schema(mock_dsd, "test", "public", "", "meta")


def test_finalize_merge_no_dsd_error(mock_db_settings, mocker):
    """Test that _finalize_merge raises an error if DSD is not set."""
    mocker.patch("py_load_eurostat.loader.postgresql.PostgresLoader._create_connection")
    loader = PostgresLoader(db_settings=mock_db_settings)
    loader.dsd = None  # Ensure DSD is not set
    with pytest.raises(RuntimeError, match="DSD must be set"):
        loader._finalize_merge("staging", "target", "public")


def test_finalize_load_unknown_strategy(mock_db_settings, mocker):
    """Test that finalize_load raises an error for an unknown strategy."""
    mocker.patch("py_load_eurostat.loader.postgresql.PostgresLoader._create_connection")
    loader = PostgresLoader(db_settings=mock_db_settings)
    with pytest.raises(ValueError, match="Unknown finalization strategy"):
        loader.finalize_load("staging", "target", "public", "invalid_strategy")
