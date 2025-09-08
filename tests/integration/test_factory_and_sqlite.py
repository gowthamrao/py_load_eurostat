"""
Integration test for the loader factory and the SQLite loader.

This test validates that the pipeline can be configured to use the
SQLite loader via environment variables and that the loader works
correctly from end-to-end.
"""
import sqlite3
from pathlib import Path

import pytest
from py_load_eurostat import pipeline
from py_load_eurostat.config import AppSettings
from py_load_eurostat.fetcher import Fetcher

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.mark.parametrize(
    "representation, expected_geo, expected_rows",
    [
        ("Standard", "DE", 5),
        ("Full", "Germany", 5),
    ],
)
@pytest.mark.integration
def test_full_pipeline_with_sqlite_via_factory(
    monkeypatch, tmp_path, mocker, representation, expected_geo, expected_rows
):
    """
    Tests the full pipeline using the SQLite loader, selected by the factory.

    It mocks the network-facing Fetcher but provides a true end-to-end test
    for the parsing, transforming, and loading logic using the file-based
    SQLite loader. It is parametrized to test both 'Standard' and 'Full'
    representations.
    """
    # 1. Configure the environment for the SQLite loader
    db_file = tmp_path / "test_eurostat.db"
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB_TYPE", "sqlite")
    monkeypatch.setenv("PY_LOAD_EUROSTAT_DB__NAME", str(db_file))

    # 2. Create a new settings object that will pick up the new env vars
    #    and monkeypatch it into the pipeline module.
    new_settings = AppSettings()
    monkeypatch.setattr(pipeline, "settings", new_settings)

    assert new_settings.db_type.value == "sqlite"
    assert new_settings.db.name == str(db_file)

    # 3. Mock the Fetcher to avoid network calls
    dataset_id = "tps00001"
    mocker.patch.object(Fetcher, "get_toc", return_value=FIXTURES_DIR / "sample_toc.tsv")
    mocker.patch.object(Fetcher, "get_dsd_xml", return_value=FIXTURES_DIR / "dsd_tps00001_simple.xml")
    mocker.patch.object(Fetcher, "get_codelist_xml", return_value=FIXTURES_DIR / "codelist_geo.xml")
    mocker.patch.object(Fetcher, "get_dataset_tsv", return_value=FIXTURES_DIR / f"{dataset_id}.tsv.gz")

    # 4. Run the pipeline
    pipeline.run_pipeline(
        dataset_id=dataset_id, representation=representation, load_strategy="Full"
    )

    # 5. Assert the results directly against the output SQLite database file
    assert db_file.exists()
    conn = sqlite3.connect(db_file)
    try:
        with conn:
            # Assert data table content
            data_table_name = f"data_{dataset_id.lower()}"
            schema = "eurostat_data"
            cur = conn.execute(f"SELECT COUNT(*) FROM {schema}__{data_table_name};")
            assert cur.fetchone()[0] == expected_rows

            # Assert a specific value, checking for code or label based on representation
            cur = conn.execute(
                f"SELECT geo FROM {schema}__{data_table_name} WHERE time_period = '2022' AND obs_value = 12.5"
            )
            assert cur.fetchone()[0] == expected_geo

            # Assert codelist table content (should be loaded regardless of representation)
            codelist_table_name = "eurostat_meta__cl_geo"
            cur = conn.execute(f"SELECT label_en FROM {codelist_table_name} WHERE code = 'DE';")
            assert cur.fetchone()[0] == "Germany"

            # Assert ingestion history
            history_table_name = "eurostat_meta___ingestion_history"
            cur = conn.execute(
                f"SELECT status, rows_loaded FROM {history_table_name} WHERE dataset_id = ?",
                (dataset_id,)
            )
            status, rows_loaded = cur.fetchone()
            assert status == "SUCCESS"
            assert rows_loaded == expected_rows

    finally:
        conn.close()
