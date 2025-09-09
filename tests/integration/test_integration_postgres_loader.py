"""
Integration test for the PostgresLoader using a live PostgreSQL container.

This test validates the loader's interaction with a real PostgreSQL database,
ensuring that schema creation, bulk loading (COPY), and the atomic table
swap logic in finalize_load work as expected.
"""

import pytest
from psycopg.rows import dict_row
from testcontainers.postgres import PostgresContainer

from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.models import (
    DSD,
    Attribute,
    Code,
    Codelist,
    Dimension,
    Measure,
    Observation,
)


@pytest.fixture(scope="module")
def postgres_container():
    """
    Spins up a PostgreSQL container for the test module.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def db_settings(postgres_container: PostgresContainer) -> DatabaseSettings:
    """
    Provides DatabaseSettings for the running test container.
    """
    return DatabaseSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        name=postgres_container.dbname,
    )


@pytest.fixture
def sample_dsd() -> DSD:
    """
    Provides a sample DSD object for testing with varied data types.
    Using original casing as found in Eurostat DSDs.
    """
    return DSD(
        id="SAMPLE_DSD",
        name="Sample DSD",
        version="1.0",
        dimensions=[
            Dimension(
                id="geo",
                name="Geo",
                position=0,
                codelist_id="CL_GEO",
                data_type="String",
            ),
            Dimension(
                id="indic_de",
                name="Indicator",
                position=1,
                codelist_id="CL_INDIC",
                data_type="String",
            ),
            Dimension(
                id="COUNT_OBS",
                name="Count of Observations",
                position=2,
                data_type="Integer",
            ),
        ],
        attributes=[
            Attribute(id="OBS_FLAG", name="Observation Flag", data_type="String")
        ],
        measures=[
            Measure(id="OBS_VALUE", name="Observation Value", data_type="Double")
        ],
        primary_measure_id="OBS_VALUE",
    )


@pytest.fixture
def sample_data_stream(sample_dsd: DSD):
    """
    Provides a sample generator of Observation objects.
    """
    observations = [
        Observation(
            dimensions={"geo": "DE", "indic_de": "IND1", "COUNT_OBS": "10"},
            time_period="2022",
            value=100.1,
            flags="p",
        ),
        Observation(
            dimensions={"geo": "FR", "indic_de": "IND2", "COUNT_OBS": "20"},
            time_period="2023",
            value=200.2,
            flags="e",
        ),
    ]

    def generator():
        yield from observations

    return generator()


@pytest.mark.integration
def test_postgres_loader_end_to_end(
    db_settings: DatabaseSettings, sample_dsd: DSD, sample_data_stream
):
    """
    Tests the full lifecycle of the PostgresLoader:
    1. Prepare schema and table, including verifying column data types.
    2. Bulk load data into a staging table.
    3. Finalize the load with an atomic table swap.
    """
    loader = PostgresLoader(db_settings)
    schema = "test_data"
    table_name = "sample_data"

    try:
        # 1. Prepare schema
        loader.prepare_schema(dsd=sample_dsd, table_name=table_name, schema=schema)

        # --- Schema Verification Logic ---
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s;
                """,
                (schema, table_name),
            )
            columns_in_db = {
                row["column_name"]: row["data_type"] for row in cur.fetchall()
            }

        expected_types = {
            "geo": "text",
            "indic_de": "text",
            "COUNT_OBS": "integer",
            "time_period": "text",
            "OBS_VALUE": "double precision",
            "OBS_FLAG": "text",
        }

        assert columns_in_db == expected_types
        # --- End of Schema Verification Logic ---

        # 2. Bulk load to staging
        staging_table, rows_loaded = loader.bulk_load_staging(
            table_name=table_name,
            schema=schema,
            data_stream=sample_data_stream,
            use_unlogged_table=False,
        )
        assert rows_loaded == 2
        assert staging_table.startswith("staging_")

        # 3. Finalize load
        loader.finalize_load(staging_table, table_name, schema)

        # 4. Verification
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT * FROM {schema}.{table_name} ORDER BY geo;")
            results = cur.fetchall()
            assert len(results) == 2
            assert results[0]["geo"] == "DE"
            assert results[0]["OBS_VALUE"] == 100.1
            assert results[1]["geo"] == "FR"
            assert results[1]["OBS_VALUE"] == 200.2

            # Check if staging table was dropped
            cur.execute(
                "SELECT to_regclass(%s) as oid;", (f"{schema}.{staging_table}",)
            )
            assert cur.fetchone()["oid"] is None

            # Check if backup table was dropped
            backup_table = f"{table_name}_old"
            cur.execute("SELECT to_regclass(%s) as oid;", (f"{schema}.{backup_table}",))
            assert cur.fetchone()["oid"] is None

    finally:
        # Clean up created schema
        if loader.conn and not loader.conn.closed:
            with loader.conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            loader.close_connection()


@pytest.mark.integration
def test_manage_codelists_insert_and_update(db_settings: DatabaseSettings):
    """
    Tests the `manage_codelists` method for both initial insertion and
    subsequent updates (upsert behavior).
    """
    loader = PostgresLoader(db_settings)
    schema = "test_meta"
    codelist_id = "CL_GEO"
    table_name = codelist_id.lower()

    # 1. Initial codelist data
    initial_codelist = Codelist(
        id=codelist_id,
        version="1.0",
        codes={
            "DE": Code(id="DE", name="Germany", description="Federal Republic of Germany"),
            "FR": Code(id="FR", name="France", description=None),
        },
    )

    try:
        # 2. First run: Insert new codelists
        loader.manage_codelists(codelists={codelist_id: initial_codelist}, schema=schema)

        # 3. Verification of insert
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT * FROM {schema}.{table_name} ORDER BY code;")
            results = cur.fetchall()
            assert len(results) == 2
            assert results[0]["code"] == "DE"
            assert results[0]["label_en"] == "Germany"
            assert results[1]["code"] == "FR"
            assert results[1]["label_en"] == "France"
            assert results[1]["description_en"] is None

        # 4. Updated codelist data (update DE, keep FR, add IT)
        updated_codelist = Codelist(
            id=codelist_id,
            version="1.1",
            codes={
                "DE": Code(id="DE", name="Germany (updated)", description="Federal Republic of Germany"),
                "FR": Code(id="FR", name="France", description=None),
                "IT": Code(id="IT", name="Italy", description="Italian Republic"),
            },
        )

        # 5. Second run: Update existing and insert new
        loader.manage_codelists(codelists={codelist_id: updated_codelist}, schema=schema)

        # 6. Verification of update and insert
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT * FROM {schema}.{table_name} ORDER BY code;")
            results = cur.fetchall()
            assert len(results) == 3
            # Check that DE was updated
            assert results[0]["code"] == "DE"
            assert results[0]["label_en"] == "Germany (updated)"
            # Check that FR is unchanged
            assert results[1]["code"] == "FR"
            assert results[1]["label_en"] == "France"
            # Check that IT was inserted
            assert results[2]["code"] == "IT"
            assert results[2]["label_en"] == "Italy"

    finally:
        # Clean up
        if loader.conn and not loader.conn.closed:
            with loader.conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            loader.close_connection()
