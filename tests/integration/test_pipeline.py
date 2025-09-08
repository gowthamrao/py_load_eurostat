"""
Integration tests for the main pipeline using an in-memory SQLite DB.

NOTE: This test was adapted to use SQLite due to environmental constraints
(Docker rate limiting) that prevented the use of a PostgreSQL container.
This still provides a good end-to-end test of the core application logic.
"""

from pathlib import Path
import pytest

from py_load_eurostat.loader.sqlite import SqliteLoader
from py_load_eurostat.pipeline import run_pipeline
from py_load_eurostat.fetcher import Fetcher

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

@pytest.mark.integration
def test_full_pipeline_with_sqlite_and_mocks(mocker):
    """
    Tests the full end-to-end pipeline using a mocked Fetcher and a
    local SQLite database. This verifies that all components (parser,
    transformer, loader) work together correctly without making real
    network calls.
    """
    dataset_id = "tps00001"  # Matches our fixture files
    sqlite_loader = None

    try:
        # 1. Mock all methods of the Fetcher class
        mocker.patch.object(Fetcher, "get_toc", return_value=FIXTURES_DIR / "sample_toc.tsv")
        mocker.patch.object(Fetcher, "get_dsd_xml", return_value=FIXTURES_DIR / "dsd_tps00001.xml")
        mocker.patch.object(Fetcher, "get_codelist_xml", return_value=FIXTURES_DIR / "codelist_geo.xml")
        mocker.patch.object(Fetcher, "get_dataset_tsv", return_value=FIXTURES_DIR / "tps00001.tsv.gz")

        # 2. Instantiate our SQLite loader and mock the PostgresLoader
        sqlite_loader = SqliteLoader()
        mocker.patch("py_load_eurostat.pipeline.PostgresLoader", return_value=sqlite_loader)

        # 3. Run the pipeline. It will use the mocked Fetcher and SQLite loader.
        run_pipeline(dataset_id=dataset_id, representation="Standard", load_strategy="Full")

        # 4. Assert the results directly against the in-memory SQLite database
        conn = sqlite_loader.conn
        with conn:
            # Assert data table content
            data_table = f"eurostat_data_data_{dataset_id}"
            cur = conn.execute(f"SELECT COUNT(*) FROM {data_table};")
            # This fixture contains 3 rows of data
            assert cur.fetchone()[0] == 3

            # Assert codelist table content
            codelist_table = "eurostat_meta_cl_geo"
            cur = conn.execute(f"SELECT label_en FROM {codelist_table} WHERE code = 'DE';")
            result = cur.fetchone()
            assert result is not None
            assert result[0] == "Germany"

    finally:
        if sqlite_loader:
            sqlite_loader.close_connection()
