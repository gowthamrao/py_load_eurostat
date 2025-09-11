# Integration tests for high-level pipeline features.
from datetime import datetime, timezone
from pathlib import Path

import pytest
from psycopg import sql
from psycopg.rows import dict_row

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

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def sample_geo_codelist() -> Codelist:
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
def test_codelist_loading(db_settings: DatabaseSettings, sample_geo_codelist: Codelist):
    loader = PostgresLoader(db_settings)
    schema = "eurostat_meta"
    codelists_to_load = {sample_geo_codelist.id: sample_geo_codelist}
    try:
        loader.manage_codelists(codelists=codelists_to_load, schema=schema)
        with loader.conn.cursor(row_factory=dict_row) as cur:
            table_name = sample_geo_codelist.id.lower()
            cur.execute("SELECT to_regclass(%s) as oid;", (f"{schema}.{table_name}",))
            assert cur.fetchone()["oid"] is not None
            cur.execute(f"SELECT * FROM {schema}.{table_name} ORDER BY code;")
            results = cur.fetchall()
            assert len(results) == 3
            assert results[0]["code"] == "DE"
            assert results[0]["label_en"] == "Germany"
    finally:
        with loader.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        loader.close_connection()


@pytest.fixture
def tps00001_dsd() -> DSD:
    return DSD(
        id="DSD_TPS00001_SIMPLE",
        version="1.0",
        name="Population on 1 January",
        dimensions=[Dimension(id="geo", name="Geo", position=1, codelist_id="cl_geo")],
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
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_toc",
        return_value=FIXTURES_DIR / "sample_inventory.tsv",
    )
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml")
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_codelist_xml",
        side_effect=lambda codelist_id: codelist_id,
    )
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_dataset_tsv",
        return_value=FIXTURES_DIR / "tps00001.tsv.gz",
    )

    dataset_id = "tps00001"

    from py_load_eurostat.config import AppSettings

    test_settings = AppSettings()
    test_settings.db = db_settings

    pipeline.run_pipeline(
        dataset_id=dataset_id,
        representation="Full",
        load_strategy="Full",
        settings=test_settings,
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
                "SELECT geo FROM {schema}.{table} WHERE geo = 'Germany' LIMIT 1"
            ).format(
                schema=sql.Identifier(data_schema), table=sql.Identifier(table_name)
            )
            cur.execute(query)
            result = cur.fetchone()
            assert result is not None and result["geo"] == "Germany"
    finally:
        with loader.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {data_schema} CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
        loader.close_connection()


@pytest.mark.integration
def test_pipeline_delta_load_skips_up_to_date_dataset(
    db_settings, mocker, caplog, tps00001_dsd: DSD, sample_geo_codelist: Codelist
):
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
        "py_load_eurostat.parser.InventoryParser.get_last_update_timestamp",
        return_value=current_timestamp,
    )
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_toc",
        return_value=FIXTURES_DIR / "sample_inventory.tsv",
    )
    mocker.patch("py_load_eurostat.fetcher.Fetcher.get_dsd_xml")
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_codelist_xml",
        side_effect=lambda codelist_id: codelist_id,
    )
    mocker.patch(
        "py_load_eurostat.fetcher.Fetcher.get_dataset_tsv",
        return_value=FIXTURES_DIR / "tps00001.tsv.gz",
    )

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
        with caplog.at_level("INFO"):
            from py_load_eurostat.config import AppSettings

            test_settings = AppSettings()
            test_settings.db = db_settings
            pipeline.run_pipeline(
                dataset_id, "Standard", "Delta", settings=test_settings
            )
        assert f"Local data for '{dataset_id}' is up-to-date. Skipping." in caplog.text
    finally:
        loader = PostgresLoader(db_settings)
        with loader.conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS eurostat_data CASCADE;")
            cur.execute("DROP SCHEMA IF EXISTS eurostat_meta CASCADE;")
        loader.close_connection()
