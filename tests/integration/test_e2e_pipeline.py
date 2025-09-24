# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
End-to-end tests for the entire pipeline, using a real HTTP server
to simulate Eurostat's API and a real PostgreSQL database.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest
from psycopg.rows import class_row, dict_row
from pytest_httpserver import HTTPServer

from py_load_eurostat import pipeline
from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.models import (
    DSD,
    Attribute,
    Code,
    Codelist,
    Dimension,
    IngestionHistory,
    Measure,
)

# Set a logger for debugging the test
logger = logging.getLogger(__name__)

# Path to the test fixtures directory
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def tps00001_dsd_fixture() -> DSD:
    """
    Provides a DSD object for the tps00001 dataset.
    Column IDs should be lowercase to match database conventions.
    """
    return DSD(
        id="DSD_TPS00001",
        version="1.0",
        name="Population on 1 January",
        dimensions=[
            Dimension(
                id="geo",
                name="Geo",
                position=1,
                codelist_id="cl_geo",
                data_type="String",
            )
        ],
        attributes=[
            Attribute(
                id="obs_flag",
                name="Observation Flags",
                codelist_id="cl_obs_flag",
                data_type="String",
            )
        ],
        measures=[
            Measure(id="obs_value", name="Observation Value", data_type="Double")
        ],
        primary_measure_id="obs_value",
    )


@pytest.fixture
def sample_geo_codelist_fixture() -> Codelist:
    """Provides a sample 'geo' codelist."""
    return Codelist(
        id="cl_geo",
        version="1.0",
        codes={
            "DE": Code(id="DE", name="Germany"),
            "FR": Code(id="FR", name="France"),
            "EU27_2020": Code(
                id="EU27_2020", name="European Union - 27 countries (from 2020)"
            ),
        },
    )


@pytest.mark.integration
def test_full_pipeline_e2e_with_mocked_parser(
    db_settings: DatabaseSettings,
    httpserver: HTTPServer,
    mocker,
    tps00001_dsd_fixture: DSD,
    sample_geo_codelist_fixture: Codelist,
):
    """
    Tests the entire pipeline end-to-end, but with the SDMX and Inventory
    parsers mocked to bypass issues with file parsing/caching in the test env.
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
    ).respond_with_data(b"<xml></xml>")
    httpserver.expect_request(
        f"/sdmx/2.1/codelist/ESTAT/{codelist_id.upper()}/latest"
    ).respond_with_data(b"<xml></xml>")
    httpserver.expect_request("/data/tps00001.tsv.gz").respond_with_data(
        (FIXTURES_DIR / "tps00001.tsv.gz").read_bytes()
    )

    # Mock the parser functions
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_dsd_from_dataflow",
        return_value=tps00001_dsd_fixture,
    )
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_codelist",
        return_value=sample_geo_codelist_fixture,
    )
    mocker.patch(
        "py_load_eurostat.parser.InventoryParser.get_download_url",
        return_value="/data/tps00001.tsv.gz",
    )
    mocker.patch(
        "py_load_eurostat.parser.InventoryParser.get_last_update_timestamp",
        return_value=datetime.now(timezone.utc),
    )

    # -- 2. Execute: Run the main pipeline function --
    # Create a new settings object for this specific test run
    from py_load_eurostat.config import AppSettings

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
            assert de_data["obs_flag"] == "p"

            # Check codelist table
            cur.execute("SELECT COUNT(*) as row_count FROM eurostat_meta.cl_geo")
            assert cur.fetchone()["row_count"] == 3

        # Check ingestion history in a new cursor with the correct row factory
        with loader.conn.cursor(row_factory=class_row(IngestionHistory)) as history_cur:
            history_cur.execute("SELECT * FROM eurostat_meta._ingestion_history")
            history: IngestionHistory = history_cur.fetchone()
            assert history is not None
            assert history.status == "SUCCESS"
            assert history.rows_loaded == 5
    finally:
        # Clean up
        with loader.conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
        loader.close_connection()
