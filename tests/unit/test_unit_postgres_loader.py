import pytest
from unittest.mock import MagicMock

from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.models import DSD, Dimension, Measure, Attribute


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
    mocker.patch(
        "py_load_eurostat.loader.postgresql.PostgresLoader._create_connection"
    )

    # 2. Create a mock DSD object with various data types
    mock_dsd = DSD(
        id="mock_dsd",
        name="Mock DSD for Testing",
        version="1.0",
        dimensions=[
            Dimension(id="dim_string", name="String Dim", position=1, data_type="String"),
            Dimension(id="dim_integer", name="Integer Dim", position=2, data_type="Integer"),
            Dimension(id="dim_long", name="Long Dim", position=3, data_type="Long"),
            Dimension(id="dim_nodtype", name="No DType Dim", position=4, data_type=None),
            Dimension(id="dim_unknown", name="Unknown DType Dim", position=5, data_type="UnknownType"),
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
