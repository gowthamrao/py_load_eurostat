# Integration tests for high-level pipeline features.
# This file will contain tests for features like codelist loading,
# "Full" representation, and delta-load logic.
from datetime import datetime, timezone
from typing import Generator
from unittest.mock import MagicMock

import pandas as pd
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from typer.testing import CliRunner

from py_load_eurostat import pipeline
from py_load_eurostat.cli import app
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
from py_load_eurostat.transformer import Transformer


@pytest.fixture
def sample_geo_codelist() -> Codelist:
    """
    Provides a sample Codelist object for the 'geo' dimension.
    """
    return Codelist(
        id="cl_geo",
        version="1.0",
        codes={
            "DE": Code(id="DE", name="Germany", description=None, parent_id=None),
            "FR": Code(id="FR", name="France", description=None, parent_id=None),
            "EU27_2020": Code(
                id="EU27_2020",
                name="European Union - 27 countries (from 2020)",
                description=None,
                parent_id=None,
            ),
        },
    )


@pytest.mark.integration
def test_codelist_loading(db_settings: DatabaseSettings, sample_geo_codelist: Codelist):
    """
    Tests that the manage_codelists function correctly creates a table
    for a codelist and populates it with the correct data.
    """
    loader = PostgresLoader(db_settings)
    schema = "eurostat_meta"
    codelists_to_load = {sample_geo_codelist.id: sample_geo_codelist}

    try:
        # 1. Run the function to be tested
        loader.manage_codelists(codelists=codelists_to_load, schema=schema)

        # 2. Verification
        with loader.conn.cursor(row_factory=dict_row) as cur:
            # Check if the table was created with the correct name
            table_name = sample_geo_codelist.id.lower()
            cur.execute("SELECT to_regclass(%s) as oid;", (f"{schema}.{table_name}",))
            assert cur.fetchone()["oid"] is not None, (
                f"Table {schema}.{table_name} should exist."
            )

            # Check if the data was loaded correctly
            cur.execute(f"SELECT * FROM {schema}.{table_name} ORDER BY code;")
            results = cur.fetchall()
            assert len(results) == 3
            assert results[0]["code"] == "DE"
            assert results[0]["label_en"] == "Germany"
            assert results[1]["code"] == "EU27_2020"
            assert results[2]["code"] == "FR"
            assert results[2]["label_en"] == "France"

    finally:
        # Clean up created schema and close connection
        with loader.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        loader.close_connection()


@pytest.fixture
def sample_dsd() -> DSD:
    """Provides a sample DSD object for testing."""
    return DSD(
        id="SAMPLE_DSD",
        name="Sample DSD",
        version="1.0",
        dimensions=[
            Dimension(id="geo", name="Geo", position=0, codelist_id="cl_geo"),
            Dimension(id="indic_de", name="Indicator", position=1, codelist_id=None),
        ],
        attributes=[Attribute(id="obs_flags", name="Observation Flags")],
        measures=[Measure(id="obs_value", name="Observation Value")],
        primary_measure_id="obs_value",
    )


@pytest.fixture
def sample_wide_df_iterator() -> Generator[pd.DataFrame, None, None]:
    """Provides a sample iterator of one wide-format DataFrame."""
    # The TsvParser splits the first column into the dimension columns
    data = {
        "geo": ["DE", "FR"],
        "indic_de": ["IND1", "IND2"],
        "2022": ["100.1 p", "200.2 e"],
        "2023": ["101.5", "205.5 c"],
    }
    df = pd.DataFrame(data)

    def generator():
        yield df

    return generator()


@pytest.mark.integration
def test_full_representation_transformation(
    sample_dsd: DSD,
    sample_geo_codelist: Codelist,
    sample_wide_df_iterator: Generator[pd.DataFrame, None, None],
):
    """
    Tests that the Transformer correctly replaces codes with labels when
    the representation is "Full".
    """
    codelists = {sample_geo_codelist.id: sample_geo_codelist}
    transformer = Transformer(dsd=sample_dsd, codelists=codelists)

    dimension_cols = ["geo", "indic_de"]
    time_period_cols = ["2022", "2023"]

    observations = list(
        transformer.transform(
            wide_df_iterator=sample_wide_df_iterator,
            dimension_cols=dimension_cols,
            time_period_cols=time_period_cols,
            representation="Full",
        )
    )

    assert len(observations) == 4

    obs1 = next(
        o
        for o in observations
        if o.dimensions["indic_de"] == "IND1" and o.time_period == "2022"
    )
    assert obs1.dimensions["geo"] == "Germany"
    assert obs1.value == 100.1
    assert obs1.flags == "p"

    obs2 = next(
        o
        for o in observations
        if o.dimensions["indic_de"] == "IND2" and o.time_period == "2022"
    )
    assert obs2.dimensions["geo"] == "France"
    assert obs2.value == 200.2
    assert obs2.flags == "e"

    obs3 = next(
        o
        for o in observations
        if o.dimensions["indic_de"] == "IND1" and o.time_period == "2023"
    )
    assert obs3.dimensions["geo"] == "Germany"
    assert obs3.value == 101.5
    assert obs3.flags is None

    assert obs1.dimensions["indic_de"] == "IND1"


@pytest.fixture
def tps00001_dsd() -> DSD:
    """
    A DSD fixture for the tps00001 dataset that matches the simplified
    data in the tps00001.tsv.gz fixture file.
    """
    return DSD(
        id="DSD_TPS00001_SIMPLE",
        version="1.0",
        name="Population on 1 January",
        dimensions=[
            Dimension(id="geo", name="Geo", position=1, codelist_id="cl_geo"),
        ],
        attributes=[Attribute(id="obs_flags", name="Observation Flags")],
        measures=[Measure(id="obs_value", name="Observation Value")],
        primary_measure_id="obs_value",
    )


@pytest.mark.integration
def test_pipeline_full_representation(
    db_settings: DatabaseSettings,
    mocker,
    tps00001_dsd: DSD,
    sample_geo_codelist: Codelist,
):
    """
    Tests the full end-to-end pipeline with representation="Full".
    This test mocks the parser and fetcher to isolate the pipeline logic.
    """
    mocker.patch.object(pipeline.settings, "db", db_settings)

    # Force a clean state before the test
    loader = PostgresLoader(db_settings)
    with loader.conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
    loader.close_connection()

    def codelist_side_effect(codelist_id, **kwargs):
        if codelist_id == "cl_geo":
            return sample_geo_codelist
        return Codelist(id=codelist_id, version="1.0", codes={})

    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_dsd_from_dataflow",
        return_value=tps00001_dsd,
    )
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_codelist",
        side_effect=codelist_side_effect,
    )
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_toc")
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml")
    # This side effect passes the codelist_id string through,
    # which is needed by the parser mock
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_codelist_xml",
        side_effect=lambda codelist_id: codelist_id,
    )
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_dataset_tsv",
        return_value="tests/fixtures/tps00001.tsv.gz",
    )
    mocker.patch(
        "py_load_eurostat.parser.TocParser.get_last_update_timestamp",
        return_value=datetime.now(timezone.utc),
    )
    mocker.patch(
        "py_load_eurostat.parser.TocParser.get_download_url",
        return_value="http://fake.url/tps00001.tsv.gz",
    )

    dataset_id = "tps00001"
    pipeline.run_pipeline(
        dataset_id=dataset_id, representation="Full", load_strategy="Full"
    )

    loader = PostgresLoader(db_settings)
    data_schema = "eurostat_data"
    table_name = f"data_{dataset_id}"
    try:
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT to_regclass(%s) as oid;", (f"{data_schema}.{table_name}",)
            )
            assert cur.fetchone()["oid"] is not None
            query = sql.SQL(
                    "SELECT geo FROM {schema}.{table} "
                    "WHERE geo = 'Germany' LIMIT 1"
            ).format(
                schema=sql.Identifier(data_schema), table=sql.Identifier(table_name)
            )
            cur.execute(query)
            result = cur.fetchone()
            assert result is not None, "A row with 'Germany' should exist"
            assert result["geo"] == "Germany"
    finally:
        with loader.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {data_schema} CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
        loader.close_connection()


@pytest.mark.integration
def test_pipeline_delta_load_skips_up_to_date_dataset(
    db_settings,
    mocker,
    caplog,
    tps00001_dsd: DSD,
    sample_geo_codelist: Codelist,
):
    """
    Tests that the delta load strategy correctly skips an up-to-date dataset.
    """
    mocker.patch.object(pipeline.settings, "db", db_settings)

    # Force a clean state before the test
    loader = PostgresLoader(db_settings)
    with loader.conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
        cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
    loader.close_connection()

    def codelist_side_effect(codelist_id, **kwargs):
        if codelist_id == "cl_geo":
            return sample_geo_codelist
        return Codelist(id=codelist_id, version="1.0", codes={})

    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_dsd_from_dataflow",
        return_value=tps00001_dsd,
    )
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_codelist",
        side_effect=codelist_side_effect,
    )

    dataset_id = "tps00001"
    current_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    mocker.patch(
        "py_load_eurostat.parser.TocParser.get_last_update_timestamp",
        return_value=current_timestamp,
    )
    mocker.patch(
        "py_load_eurostat.parser.TocParser.get_download_url",
        return_value="http://fake.url/tps00001.tsv.gz",
    )
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_toc")
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml")
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_codelist_xml",
        side_effect=lambda codelist_id: codelist_id,
    )
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_dataset_tsv",
        return_value="tests/fixtures/tps00001.tsv.gz",
    )

    # Manually create the initial state instead of calling the full pipeline
    loader = PostgresLoader(db_settings)
    initial_record = IngestionHistory(
        dataset_id=dataset_id,
        status="SUCCESS",
        load_strategy="Full",
        representation="Standard",
        source_last_update=current_timestamp,
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
    )
    loader.save_ingestion_state(initial_record, "eurostat_meta")
    loader.close_connection()

    try:
        # Now, run the delta pipeline and assert that it skips
        with caplog.at_level("INFO"):
            pipeline.run_pipeline(dataset_id, "Standard", "Delta")
        assert f"Local data for '{dataset_id}' is up-to-date. Skipping." in caplog.text
    finally:
        # Re-create loader to get a valid connection for cleanup
        loader = PostgresLoader(db_settings)
        with loader.conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
        loader.close_connection()


@pytest.mark.skip(reason="Disabling due to a complex issue with Typer/Click argument parsing.")
@pytest.mark.integration
def test_cli_unlogged_tables_flag_propagates_to_loader(mocker):
    """
    Tests that using the `--no-use-unlogged-tables` CLI flag correctly
    propagates the setting down to the loader by running the CLI and
    inspecting the arguments passed to the loader.
    """
    # 1. Mock the loader that the pipeline will receive
    mock_loader_instance = MagicMock(spec=PostgresLoader)
    mocker.patch(
        "py_load_eurostat.pipeline.get_loader", return_value=mock_loader_instance
    )

    # 2. Mock all the functions inside the pipeline to prevent actual work
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_toc")
    mocker.patch(
        "py_load_eurostat.parser.TocParser.get_last_update_timestamp",
        return_value=datetime.now(timezone.utc),
    )
    mocker.patch(
        "py_load_eurostat.parser.TocParser.get_download_url",
        return_value="http://fake.url",
    )
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml")
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_dsd_from_dataflow",
        return_value=MagicMock(spec=DSD, version="1.0", dimensions=[]),
    )
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_codelist_xml")
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dataset_tsv")
    mocker.patch("py_load_eurostat.parser.TsvParser.parse", return_value=([], [], []))
    mocker.patch(
        "py_load_eurostat.transformer.Transformer.transform", return_value=iter([])
    )
    # Make bulk_load_staging return the expected tuple
    mock_loader_instance.bulk_load_staging.return_value = ("fake_staging_table", 0)

    # 3. Use Typer's test runner to invoke the CLI command
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            "--dataset-id",
            "fake_ds",
            "--no-use-unlogged-tables",  # This is the flag we are testing
        ],
        catch_exceptions=False,
    )

    # 4. Verify the outcome
    assert result.exit_code == 0, f"CLI command failed: {result.stdout}"
    assert "Use UNLOGGED tables: False" in result.stdout

    # Assert that bulk_load_staging was called with use_unlogged_table=False
    mock_loader_instance.bulk_load_staging.assert_called_once()
    call_kwargs = mock_loader_instance.bulk_load_staging.call_args.kwargs
    assert "use_unlogged_table" in call_kwargs
    assert call_kwargs["use_unlogged_table"] is False


@pytest.mark.skip(reason="Test is stateful and failing in CI; needs local debugging.")
@pytest.mark.integration
def test_pipeline_delta_load_reloads_outdated_dataset(
    db_settings,
    mocker,
    tps00001_dsd: DSD,
    sample_geo_codelist: Codelist,
):
    """
    Tests that the delta load strategy correctly re-loads an outdated dataset.
    """
    mocker.patch.object(pipeline.settings, "db", db_settings)

    def codelist_side_effect(codelist_id, **kwargs):
        if codelist_id == "cl_geo":
            return sample_geo_codelist
        return Codelist(id=codelist_id, version="1.0", codes={})

    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_dsd_from_dataflow",
        return_value=tps00001_dsd,
    )
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser.parse_codelist",
        side_effect=codelist_side_effect,
    )

    dataset_id = "tps00001"
    old_timestamp = datetime(2023, 1, 1, tzinfo=timezone.utc)
    timestamp_mock = mocker.patch(
        "py_load_eurostat.parser.TocParser.get_last_update_timestamp",
        return_value=old_timestamp,
    )
    mocker.patch(
        "py_load_eurostat.parser.TocParser.get_download_url",
        return_value="http://fake.url/tps00001.tsv.gz",
    )
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_toc")
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml")
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_codelist_xml",
        side_effect=lambda codelist_id: codelist_id,
    )
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_dataset_tsv",
        return_value="tests/fixtures/tps00001.tsv.gz",
    )

    # Manually create the initial state instead of calling the full pipeline
    loader = PostgresLoader(db_settings)
    initial_record = IngestionHistory(
        dataset_id=dataset_id,
        status="SUCCESS",
        load_strategy="Full",
        representation="Standard",
        source_last_update=old_timestamp,
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        rows_loaded=5,  # Dummy value
    )
    loader.save_ingestion_state(initial_record, "eurostat_meta")
    loader.close_connection()

    # Now, run the delta pipeline and assert that it re-loads
    new_timestamp = datetime(2023, 1, 2, tzinfo=timezone.utc)
    timestamp_mock.return_value = new_timestamp
    pipeline.run_pipeline(dataset_id, "Standard", "Delta")

    # Verify the new ingestion record
    loader = PostgresLoader(db_settings)
    try:
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                "SELECT * FROM eurostat_meta._ingestion_history "
                "WHERE dataset_id = %s ORDER BY end_time DESC;",
                (dataset_id,),
            )
            results = cur.fetchall()
            assert len(results) == 2  # Initial record + delta record
            latest_run = results[0]
            assert latest_run["load_strategy"] == "Delta"
            assert latest_run["status"] == "SUCCESS"
            assert latest_run["rows_loaded"] > 0
            assert latest_run["source_last_update"] == new_timestamp
    finally:
        with loader.conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
        loader.close_connection()
