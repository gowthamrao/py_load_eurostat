"""
Integration tests for the main pipeline.
"""
from pathlib import Path
import pytest
from py_load_eurostat.config import DatabaseSettings, settings
from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.pipeline import run_pipeline

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

def cleanup_db(loader: PostgresLoader, dataset_id: str):
    """Helper to truncate tables between test runs."""
    try:
        with loader.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE eurostat_data.data_{dataset_id} CASCADE;")
            cur.execute("TRUNCATE TABLE eurostat_data._ingestion_history CASCADE;")
            loader.conn.commit()
    except Exception as e:
        # This might fail if the tables don't exist yet, which is fine.
        print(f"Cleanup failed (this might be expected): {e}")
        loader.conn.rollback()


@pytest.mark.integration
def test_full_pipeline_run_happy_path(db_settings: DatabaseSettings, mocker):
    """
    Tests the full end-to-end pipeline using a live database container
    and mocked network calls.
    """
    loader = PostgresLoader(db_settings)
    dataset_id = "tps00001"
    try:
        # 1. Mock the fetcher
        mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml", return_value=FIXTURES_DIR / "dsd_tps00001.xml")
        mocker.patch("py_load_eurostat.fetcher.Fetcher.get_codelist_xml", return_value=FIXTURES_DIR / "codelist_geo.xml")
        mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dataset_tsv", return_value=FIXTURES_DIR / "tps00001.tsv.gz")
        mocker.patch("py_load_eurostat.fetcher.Fetcher.get_toc_xml", return_value=FIXTURES_DIR / "sample_toc.xml")
        mocker.patch.object(settings, 'db', db_settings)

        # 2. Run the pipeline
        run_pipeline(dataset_id=dataset_id, representation="Standard", load_strategy="Full")

        # 3. Assert the results
        with loader.conn.cursor() as cur:
            table_name = f"data_{dataset_id}"
            cur.execute(f"SELECT COUNT(*) FROM eurostat_data.{table_name};")
            assert cur.fetchone()[0] == 5
            cur.execute(f"SELECT \"obs_value\", \"obs_flag\" FROM eurostat_data.{table_name} WHERE \"geo\" = 'DE' AND \"time_period\" = '2022';")
            obs_value, obs_flag = cur.fetchone()
            assert obs_value == 12.5
            assert obs_flag == "p"
            history_table = "_ingestion_history"
            cur.execute(f"SELECT status, rows_loaded FROM eurostat_data.{history_table} WHERE dataset_id = %s;", (dataset_id,))
            status, rows_loaded = cur.fetchone()
            assert status == "SUCCESS"
            assert rows_loaded == 5
    finally:
        cleanup_db(loader, dataset_id)
        loader.close_connection()


@pytest.mark.integration
def test_delta_load_logic(db_settings: DatabaseSettings, mocker):
    """
    Tests that the delta load strategy correctly skips up-to-date datasets
    and loads updated ones.
    """
    loader = PostgresLoader(db_settings)
    dataset_id = "tps00001"
    try:
        # 1. Mock the fetcher
        mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml", return_value=FIXTURES_DIR / "dsd_tps00001.xml")
        mocker.patch("py_load_eurostat.fetcher.Fetcher.get_codelist_xml", return_value=FIXTURES_DIR / "codelist_geo.xml")
        mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dataset_tsv", return_value=FIXTURES_DIR / "tps00001.tsv.gz")
        mock_get_toc = mocker.patch("py_load_eurostat.fetcher.Fetcher.get_toc_xml", return_value=FIXTURES_DIR / "sample_toc.xml")
        mocker.patch.object(settings, 'db', db_settings)

        # 2. Run initial full load
        run_pipeline(dataset_id=dataset_id, representation="Standard", load_strategy="Full")

        # 3. Run a delta load with the SAME timestamp - should be skipped
        run_pipeline(dataset_id=dataset_id, representation="Standard", load_strategy="Delta")

        # 4. Assert that the second run was skipped
        with loader.conn.cursor() as cur:
            cur.execute("SELECT status, rows_loaded FROM eurostat_data._ingestion_history WHERE dataset_id = %s ORDER BY start_time;", (dataset_id,))
            records = cur.fetchall()
            assert len(records) == 2
            assert records[0] == ("SUCCESS", 5)
            assert records[1] == ("SUCCESS", 0)

        # 5. Mock the TOC to point to the NEWER file and run again
        mock_get_toc.return_value = FIXTURES_DIR / "sample_toc_new.xml"
        run_pipeline(dataset_id=dataset_id, representation="Standard", load_strategy="Delta")

        # 6. Assert that the third run was executed
        with loader.conn.cursor() as cur:
            cur.execute("SELECT status, rows_loaded FROM eurostat_data._ingestion_history WHERE dataset_id = %s ORDER BY start_time;", (dataset_id,))
            records = cur.fetchall()
            assert len(records) == 3
            assert records[2] == ("SUCCESS", 5)
    finally:
        cleanup_db(loader, dataset_id)
        loader.close_connection()
