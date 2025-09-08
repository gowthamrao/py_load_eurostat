"""
Integration test for the PostgresLoader using a live PostgreSQL container.

This test validates the loader's interaction with a real PostgreSQL database,
ensuring that schema creation, bulk loading (COPY), and the atomic table
swap logic in finalize_load work as expected.
"""
import pytest
from testcontainers.postgres import PostgresContainer
from psycopg.rows import dict_row

from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.models import DSD, Dimension, Attribute, Observation


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
    Provides a sample DSD object for testing.
    """
    return DSD(
        id="SAMPLE_DSD",
        version="1.0",
        dimensions=[
            Dimension(id="geo", position=0, codelist_id="cl_geo"),
            Dimension(id="indic_de", position=1, codelist_id="cl_indic"),
        ],
        attributes=[Attribute(id="OBS_FLAG")],
        primary_measure_id="OBS_VALUE",
    )


@pytest.fixture
def sample_data_stream():
    """
    Provides a sample generator of Observation objects.
    """
    observations = [
        Observation(
            dimensions={"geo": "DE", "indic_de": "IND1"},
            time_period="2022",
            value=100.1,
            flags="p",
        ),
        Observation(
            dimensions={"geo": "FR", "indic_de": "IND2"},
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
    1. Prepare schema and table
    2. Bulk load data into a staging table
    3. Finalize the load with an atomic table swap
    """
    loader = PostgresLoader(db_settings)
    schema = "test_data"
    table_name = "sample_data"

    try:
        # 1. Prepare schema
        loader.prepare_schema(dsd=sample_dsd, table_name=table_name, schema=schema)

        # 2. Bulk load to staging
        staging_table, rows_loaded = loader.bulk_load_staging(
            table_name=table_name,
            schema=schema,
            data_stream=sample_data_stream,
            use_unlogged_table=False,  # Use logged table for simplicity in test
        )
        assert rows_loaded == 2
        assert staging_table.startswith("staging_")

        # 3. Finalize load
        loader.finalize_load(staging_table, table_name, schema)

        # 4. Verification
        with loader.conn.cursor(row_factory=dict_row) as cur:
            # Check if target table exists and has correct data
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
            result = cur.fetchone()
            assert result["oid"] is None

            # Check if backup table was dropped
            backup_table = f"{table_name}_old"
            cur.execute(
                "SELECT to_regclass(%s) as oid;", (f"{schema}.{backup_table}",)
            )
            result = cur.fetchone()
            # The backup table should definitely not exist and return a row
            assert result["oid"] is None

    finally:
        # Clean up created schema
        with loader.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        loader.close_connection()
