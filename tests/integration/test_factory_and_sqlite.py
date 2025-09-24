# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Integration tests for the SQLite loader and the main pipeline factory function.
"""

import sqlite3
from pathlib import Path

import pytest

from py_load_eurostat import pipeline
from py_load_eurostat.config import AppSettings
from py_load_eurostat.fetcher import Fetcher
from py_load_eurostat.models import DSD, Attribute, Dimension, Measure

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def correct_dsd_for_tps00001() -> DSD:
    """
    A fixture that provides a DSD for the tps00001 dataset that matches
    the simplified TSV data in the fixtures.
    """
    return DSD(
        id="TPS00001",
        name="GDP per capita in PPS",
        version="1.0",
        dimensions=[
            Dimension(id="geo", name="Geo", position=0, codelist_id="CL_GEO"),
        ],
        attributes=[Attribute(id="OBS_FLAG", name="Observation Flag")],
        measures=[Measure(id="OBS_VALUE", name="Observation Value")],
        primary_measure_id="OBS_VALUE",
    )


@pytest.mark.parametrize(
    "representation, expected_geo, expected_rows",
    [
        ("Standard", "DE", 5),
        ("Full", "Germany", 5),
    ],
)
@pytest.mark.integration
def test_full_pipeline_with_sqlite_via_factory(
    monkeypatch,
    tmp_path,
    mocker,
    representation,
    expected_geo,
    expected_rows,
    correct_dsd_for_tps00001,
):
    """
    Tests the full pipeline using the SQLite loader, selected by the factory.
    """
    # 1. Configure for SQLite
    db_file = tmp_path / "test_eurostat.db"
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB_TYPE", "sqlite")
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB__NAME", str(db_file))

    # 2. Mock Fetcher and Parser
    dataset_id = "tps00001"
    mocker.patch.object(
        Fetcher, "get_toc", return_value=FIXTURES_DIR / "sample_inventory.tsv"
    )
    # Mock the parser to return our correct DSD, bypassing the broken fixture file
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_dsd_from_dataflow",
        return_value=correct_dsd_for_tps00001,
    )
    # Mock the fetcher so it doesn't try to download the DSD
    mocker.patch.object(
        Fetcher, "get_dsd_xml", return_value=FIXTURES_DIR / "dsd_tps00001.xml"
    )
    mocker.patch.object(
        Fetcher, "get_codelist_xml", return_value=FIXTURES_DIR / "codelist_geo.xml"
    )
    mocker.patch.object(
        Fetcher, "get_dataset_tsv", return_value=FIXTURES_DIR / f"{dataset_id}.tsv.gz"
    )

    # 3. Run the pipeline
    new_settings = AppSettings()
    pipeline.run_pipeline(
        dataset_id=dataset_id,
        representation=representation,
        load_strategy="Full",
        settings=new_settings,
    )

    # 4. Assert the results
    assert db_file.exists()
    conn = sqlite3.connect(db_file)
    try:
        with conn:
            data_table_name = f"data_{dataset_id.lower()}"
            schema = "eurostat_data"
            cur = conn.execute(f"SELECT COUNT(*) FROM {schema}__{data_table_name};")
            assert cur.fetchone()[0] == expected_rows

            cur = conn.execute(
                f"SELECT geo FROM {schema}__{data_table_name} "
                "WHERE time_period = '2022' AND obs_value = 12.5"
            )
            assert cur.fetchone()[0] == expected_geo
    finally:
        conn.close()
