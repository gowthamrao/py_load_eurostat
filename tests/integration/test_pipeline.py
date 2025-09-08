"""
Integration tests for the main pipeline using an in-memory SQLite DB.

NOTE: This test was adapted to use SQLite due to environmental constraints
(Docker rate limiting) that prevented the use of a PostgreSQL container.
This still provides a good end-to-end test of the core application logic.
"""

import pytest

from py_load_eurostat.loader.sqlite import SqliteLoader
from py_load_eurostat.pipeline import run_pipeline


@pytest.fixture(scope="module", autouse=True)
def check_network():
    """Skips integration tests if network is unavailable."""
    try:
        import httpx
        httpx.get("https://ec.europa.eu", timeout=5)
    except (httpx.ConnectError, httpx.TimeoutException):
        pytest.skip("Network not available, skipping integration tests.")

@pytest.mark.integration
def test_full_pipeline_with_sqlite(mocker):
    """
    Tests the full end-to-end pipeline using an in-memory SQLite database
    and REAL network calls to the Eurostat API.
    """
    # Use a small, stable dataset for the test. 'teibs010' is small and simple.
    dataset_id = "teibs010"
    sqlite_loader = None
    try:
        # 1. Instantiate our SQLite loader
        sqlite_loader = SqliteLoader()

        # 2. Use a mock to replace the PostgresLoader with our SqliteLoader
        mocker.patch("py_load_eurostat.pipeline.PostgresLoader", return_value=sqlite_loader)

        # 3. Run the pipeline. It will now use the SQLite loader.
        run_pipeline(dataset_id=dataset_id, representation="Standard", load_strategy="Full")

        # 4. Assert the results directly against the in-memory SQLite database
        conn = sqlite_loader.conn
        with conn:
            # Assert data table content
            data_table = f"eurostat_data_data_{dataset_id}"
            cur = conn.execute(f"SELECT COUNT(*) FROM {data_table};")
            # This is a small dataset, so we expect a small number of rows.
            assert cur.fetchone()[0] > 10

            # Assert codelist table content. The main dimension is 'geo'.
            codelist_table = "eurostat_meta_cl_geo"
            cur = conn.execute(f"SELECT label_en FROM {codelist_table} WHERE code = 'EA';")
            result = cur.fetchone()
            assert result is not None
            assert "Euro area" in result[0]

    finally:
        if sqlite_loader:
            sqlite_loader.close_connection()
