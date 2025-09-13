"""
End-to-end tests for the entire pipeline, using a real HTTP server
to simulate Eurostat's API and a real PostgreSQL database.
"""

import logging
from pathlib import Path

import pytest
from psycopg.rows import dict_row
from pytest_httpserver import HTTPServer

from py_load_eurostat import pipeline
from py_load_eurostat.config import AppSettings, DatabaseSettings
from py_load_eurostat.loader.postgresql import PostgresLoader

# Set a logger for debugging the test
logger = logging.getLogger(__name__)

# Path to the test fixtures directory
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.mark.integration
def test_full_pipeline_e2e_without_mocks(
    db_settings: DatabaseSettings,
    httpserver: HTTPServer,
    tmp_path: Path,
    monkeypatch,
):
    """
    Tests the entire pipeline end-to-end using real fixture files for
    inventory, DSD, and codelists.
    """
    # -- 1. Setup: Clean database and configure test HTTP server --
    loader = PostgresLoader(db_settings)
    with loader.conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
    loader.close_connection()

    dataset_id = "tps00001"
    codelist_id = "cl_geo"

    # Setup the HTTP server to serve all needed files
    httpserver.expect_request(
        "/files/inventory", query_string="type=data"
    ).respond_with_data((FIXTURES_DIR / "sample_inventory.tsv").read_bytes())
    httpserver.expect_request(
        f"/sdmx/2.1/dataflow/ESTAT/{dataset_id.upper()}/latest",
        query_string="references=datastructure",
    ).respond_with_data((FIXTURES_DIR / "dsd_tps00001_lower.xml").read_bytes())
    httpserver.expect_request(
        f"/sdmx/2.1/codelist/ESTAT/{codelist_id.upper()}/latest"
    ).respond_with_data((FIXTURES_DIR / "codelist_geo.xml").read_bytes())
    httpserver.expect_request("/data/tps00001.tsv.gz").respond_with_data(
        (FIXTURES_DIR / "tps00001.tsv.gz").read_bytes()
    )

    # -- 2. Execute: Run the main pipeline function --
    # Use a temporary directory for the cache to avoid conflicts
    monkeypatch.setenv("PY_LOAD_EUROSTAT_CACHE__PATH", str(tmp_path))
    test_settings = AppSettings()
    test_settings.eurostat.base_url = httpserver.url_for("/")
    test_settings.db = db_settings

    pipeline.run_pipeline(
        dataset_id=dataset_id,
        representation="Standard",
        load_strategy="Full",
        settings=test_settings,
    )

    # -- 3. Assert: Verify the database state --
    logger.info("Pipeline run finished. Starting assertions.")
    loader = PostgresLoader(db_settings)
    try:
        with loader.conn.cursor(row_factory=dict_row) as cur:
            # Check data table
            cur.execute("SELECT COUNT(*) as row_count FROM eurostat_data.data_tps00001")
            assert cur.fetchone()["row_count"] == 5

            # Check a specific data point
            cur.execute(
                "SELECT * FROM eurostat_data.data_tps00001 "
                "WHERE geo = 'DE' AND time_period = '2022'"
            )
            de_data = cur.fetchone()
            assert de_data is not None
            assert de_data["obs_value"] == 12.5
    finally:
        # Clean up
        with loader.conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
        loader.close_connection()
