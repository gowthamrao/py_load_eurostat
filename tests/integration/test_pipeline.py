"""
Integration tests for the main pipeline.
"""
from pathlib import Path
import pytest
from eurostat_loader.config import DatabaseSettings, settings
from eurostat_loader.loader.postgresql import PostgresLoader
from eurostat_loader.pipeline import run_pipeline

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

@pytest.mark.integration
def test_full_pipeline_run_happy_path(db_settings: DatabaseSettings, mocker):
    """
    Tests the full end-to-end pipeline using a live database container
    and mocked network calls.
    """
    # 1. Mock the fetcher to return local fixture files instead of making network calls
    mocker.patch(
        "eurostat_loader.fetcher.fetcher.get_dsd_xml",
        return_value=FIXTURES_DIR / "dsd_tps00001.xml",
    )
    mocker.patch(
        "eurostat_loader.fetcher.fetcher.get_codelist_xml",
        return_value=FIXTURES_DIR / "codelist_geo.xml",
    )
    mocker.patch(
        "eurostat_loader.fetcher.fetcher.get_dataset_tsv",
        return_value=FIXTURES_DIR / "tps00001.tsv.gz",
    )

    # 2. Override the application's database settings with the test container's settings
    mocker.patch.object(settings, 'db', db_settings)

    # 3. Run the pipeline
    dataset_id = "tps00001"
    run_pipeline(dataset_id=dataset_id, representation="Standard", load_strategy="Full")

    # 4. Assert the results in the database
    # Create a new loader to connect to the test DB and inspect the state
    loader = PostgresLoader(db_settings)
    try:
        with loader.conn.cursor() as cur:
            # Check that the main data table was created and has the correct number of rows
            # The source file has 4 data points, but one is missing (':'), so 3 should be loaded.
            table_name = f"data_{dataset_id}"
            cur.execute(f"SELECT COUNT(*) FROM eurostat_data.{table_name};")
            assert cur.fetchone()[0] == 3

            # Check a specific row to ensure data and flags were parsed correctly
            cur.execute(f"SELECT \"OBS_VALUE\", \"OBS_FLAG\" FROM eurostat_data.{table_name} WHERE \"GEO\" = 'DE' AND \"time_period\" = '2022';")
            obs_value, obs_flag = cur.fetchone()
            assert obs_value == 12.5
            assert obs_flag == "p"

            # Check that the ingestion history was recorded successfully
            history_table = "_ingestion_history"
            cur.execute(f"SELECT status, rows_loaded FROM eurostat_data.{history_table} WHERE dataset_id = %s;", (dataset_id,))
            status, rows_loaded = cur.fetchone()
            assert status == "SUCCESS"
            assert rows_loaded == 3

    finally:
        loader.close_connection()
