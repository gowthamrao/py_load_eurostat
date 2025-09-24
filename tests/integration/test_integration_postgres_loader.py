# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Integration test for the PostgresLoader using a live PostgreSQL container.

This test validates the loader's interaction with a real PostgreSQL database,
ensuring that schema creation, bulk loading (COPY), and the atomic table
swap logic in finalize_load work as expected.
"""

from datetime import datetime, timezone

import pytest
from psycopg.rows import dict_row
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

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
    Observation,
)


@pytest.fixture(scope="module")
def postgres_container():
    """
    Spins up a PostgreSQL container for the test module.
    """
    postgres = PostgresContainer("postgres:16-alpine")
    postgres.waiting_for(
        LogMessageWaitStrategy("database system is ready to accept connections")
    )
    with postgres:
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
        loader.prepare_schema(
            dsd=sample_dsd,
            table_name=table_name,
            schema=schema,
            representation="Full",  # FKs not tested here, so Full is fine
            meta_schema="test_meta",
        )

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
        loader.finalize_load(staging_table, table_name, schema, strategy="swap")

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
def test_prepare_schema_is_idempotent_with_fks(
    db_settings: DatabaseSettings, sample_dsd: DSD
):
    """
    Tests that calling prepare_schema multiple times with FKs is idempotent.
    This specifically covers the "foreign key already exists" branch.
    """
    loader = PostgresLoader(db_settings)
    data_schema = "test_idempotent_data"
    meta_schema = "test_idempotent_meta"
    table_name = "sample_data_idempotent"

    codelists = {
        "CL_GEO": Codelist(id="CL_GEO", version="1.0", codes={}),
        "CL_INDIC": Codelist(id="CL_INDIC", version="1.0", codes={}),
    }

    try:
        # 1. Create codelist tables so FKs can be created
        loader.manage_codelists(codelists, meta_schema)

        # 2. First call to prepare_schema, which creates the FKs
        loader.prepare_schema(
            dsd=sample_dsd,
            table_name=table_name,
            schema=data_schema,
            representation="Standard",
            meta_schema=meta_schema,
        )

        # 3. Second call to prepare_schema
        # This should execute the code path where the FKs are found to exist
        # and not raise any errors.
        loader.prepare_schema(
            dsd=sample_dsd,
            table_name=table_name,
            schema=data_schema,
            representation="Standard",
            meta_schema=meta_schema,
        )

        # 4. Verify that the table and FKs still exist
        with loader.conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = %s AND table_schema = %s",
                (table_name, data_schema),
            )
            assert cur.fetchone() is not None, "Table should still exist"

            cur.execute(
                "SELECT 1 FROM information_schema.table_constraints "
                "WHERE constraint_name = %s",
                (f"fk_{table_name}_geo",),
            )
            assert cur.fetchone() is not None, "Foreign key should still exist"

    finally:
        # Clean up
        if loader.conn and not loader.conn.closed:
            with loader.conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {data_schema} CASCADE;")
                cur.execute(f"DROP SCHEMA IF EXISTS {meta_schema} CASCADE;")
            loader.close_connection()


@pytest.mark.integration
def test_foreign_key_constraint(db_settings: DatabaseSettings, sample_dsd: DSD):
    """
    Tests that foreign key constraints are created for 'Standard' representation
    and that they are enforced by the database.
    """
    import psycopg

    loader = PostgresLoader(db_settings)
    data_schema = "test_fk_data"
    meta_schema = "test_fk_meta"
    table_name = "sample_data_with_fk"

    # Codelist that matches the 'geo' dimension in sample_dsd
    codelists = {
        "CL_GEO": Codelist(
            id="CL_GEO",
            version="1.0",
            codes={"DE": Code(id="DE", name="Germany")},
        ),
        "CL_INDIC": Codelist(
            id="CL_INDIC",
            version="1.0",
            codes={"IND1": Code(id="IND1", name="Indicator 1")},
        ),
    }

    def bad_data_stream():
        """Generator for data with an invalid 'geo' code."""
        yield Observation(
            dimensions={"geo": "XX", "indic_de": "IND1", "COUNT_OBS": "99"},
            time_period="2025",
            value=999.9,
            flags="z",
        )

    try:
        # 1. Create codelist tables first
        loader.manage_codelists(codelists, meta_schema)

        # 2. Prepare schema with 'Standard' representation to trigger FK creation
        loader.prepare_schema(
            dsd=sample_dsd,
            table_name=table_name,
            schema=data_schema,
            representation="Standard",
            meta_schema=meta_schema,
        )

        # 3. Verify the foreign key constraint exists
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT conname FROM pg_constraint
                WHERE conrelid = %s::regclass
                AND contype = 'f'
                """,
                (f"{data_schema}.{table_name}",),
            )
            constraints = [row["conname"] for row in cur.fetchall()]
            assert f"fk_{table_name}_geo" in constraints

        # 4. Attempt to load data with an invalid foreign key
        staging_table, _ = loader.bulk_load_staging(
            table_name, data_schema, bad_data_stream()
        )

        # 5. Assert that finalizing the load raises a ForeignKeyViolation
        with pytest.raises(psycopg.errors.ForeignKeyViolation) as excinfo:
            loader.finalize_load(
                staging_table, table_name, data_schema, strategy="merge"
            )

        assert f'violates foreign key constraint "fk_{table_name}_geo"' in str(
            excinfo.value
        )

    finally:
        # Clean up
        if loader.conn and not loader.conn.closed:
            with loader.conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {data_schema} CASCADE;")
                cur.execute(f"DROP SCHEMA IF EXISTS {meta_schema} CASCADE;")
            loader.close_connection()


@pytest.fixture
def tps00001_dsd() -> DSD:
    """A simplified DSD for the tps00001 dataset."""
    return DSD(
        id="TPS00001",
        name="Test Dataset",
        version="1.0",
        dimensions=[
            Dimension(id="geo", name="Geo", position=0, data_type="String"),
        ],
        attributes=[
            Attribute(id="obs_flags", name="Observation Flag", data_type="String")
        ],
        measures=[
            Measure(id="obs_value", name="Observation Value", data_type="Double")
        ],
        primary_measure_id="obs_value",
    )


def tps00001_initial_stream():
    """Data stream for the initial load of tps00001."""
    observations = [
        Observation(
            dimensions={"geo": "EU27_2020"}, time_period="2022", value=10.0, flags=None
        ),
        Observation(
            dimensions={"geo": "EU27_2020"}, time_period="2021", value=9.5, flags=None
        ),
        Observation(
            dimensions={"geo": "DE"}, time_period="2022", value=12.5, flags="p"
        ),
        Observation(
            dimensions={"geo": "DE"}, time_period="2021", value=11.8, flags="c"
        ),
        Observation(
            dimensions={"geo": "FR"}, time_period="2021", value=8.2, flags=None
        ),
    ]
    yield from observations


def tps00001_modified_stream():
    """Data stream for the modified (delta) load of tps00001."""
    observations = [
        # DE 2022 is updated from 12.5 to 15.0
        Observation(
            dimensions={"geo": "DE"}, time_period="2022", value=15.0, flags="p"
        ),
        Observation(
            dimensions={"geo": "DE"}, time_period="2021", value=11.8, flags="c"
        ),
        # FR is unchanged
        Observation(
            dimensions={"geo": "FR"}, time_period="2021", value=8.2, flags=None
        ),
        # IT is a new geo
        Observation(
            dimensions={"geo": "IT"}, time_period="2022", value=7.5, flags=None
        ),
        Observation(
            dimensions={"geo": "IT"}, time_period="2021", value=7.0, flags=None
        ),
    ]
    yield from observations


@pytest.mark.integration
def test_delta_load_with_merge_strategy(
    db_settings: DatabaseSettings, tps00001_dsd: DSD
):
    """
    Tests that the 'merge' finalization strategy correctly updates existing
    rows and inserts new ones, without deleting old ones.
    """
    loader = PostgresLoader(db_settings)
    schema = "test_delta"
    table_name = "data_tps00001"

    try:
        # --- 1. Initial Full Load (using SWAP) ---
        loader.prepare_schema(
            dsd=tps00001_dsd,
            table_name=table_name,
            schema=schema,
            representation="Standard",
            meta_schema="test_meta",
        )
        staging_table_1, rows_1 = loader.bulk_load_staging(
            table_name, schema, tps00001_initial_stream()
        )
        loader.finalize_load(staging_table_1, table_name, schema, strategy="swap")

        # --- Verification of initial state ---
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"SELECT * FROM {schema}.{table_name};")
            results = cur.fetchall()
            assert len(results) == 5  # Initial data has 5 observations

        # --- 2. Delta Load (using MERGE) ---
        staging_table_2, rows_2 = loader.bulk_load_staging(
            table_name, schema, tps00001_modified_stream()
        )
        loader.finalize_load(staging_table_2, table_name, schema, strategy="merge")

        # --- 3. Final Verification ---
        with loader.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT * FROM {schema}.{table_name} ORDER BY geo, time_period;"
            )
            final_results = {
                (r["geo"], r["time_period"]): (r["obs_value"], r["obs_flags"])
                for r in cur.fetchall()
            }

        # Assert total rows: original (5) - removed (2) + added (2) = 5.
        # MERGE does not delete, so EU27_2020 remains.
        # Initial: EU27(2), DE(2), FR(1) = 5
        # Modified: DE(2), FR(1), IT(2) = 5
        # After Merge: EU27(2), DE(2, updated), FR(1), IT(2) = 7
        assert len(final_results) == 7

        # Assert DE 2022 was updated
        assert final_results[("DE", "2022")] == (15.0, "p")
        # Assert DE 2021 is unchanged
        assert final_results[("DE", "2021")] == (11.8, "c")
        # Assert FR is unchanged
        assert final_results[("FR", "2021")] == (8.2, None)
        # Assert IT (new) was inserted
        assert final_results[("IT", "2022")] == (7.5, None)
        # Assert EU27_2020 (not in 2nd load) still exists
        assert final_results[("EU27_2020", "2022")] == (10.0, None)

    finally:
        # Clean up
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
            "DE": Code(
                id="DE", name="Germany", description="Federal Republic of Germany"
            ),
            "FR": Code(id="FR", name="France", description=None),
        },
    )

    try:
        # 2. First run: Insert new codelists
        loader.manage_codelists(
            codelists={codelist_id: initial_codelist}, schema=schema
        )

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
                "DE": Code(
                    id="DE",
                    name="Germany (updated)",
                    description="Federal Republic of Germany",
                ),
                "FR": Code(id="FR", name="France", description=None),
                "IT": Code(id="IT", name="Italy", description="Italian Republic"),
            },
        )

        # 5. Second run: Update existing and insert new
        loader.manage_codelists(
            codelists={codelist_id: updated_codelist}, schema=schema
        )

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


@pytest.mark.integration
def test_schema_evolution_raises_on_type_mismatch_in_code(
    db_settings: DatabaseSettings,
):
    """
    Verify that prepare_schema detects a data type mismatch between DSD versions
    and raises a NotImplementedError, using DSD objects created in code.
    """
    loader = PostgresLoader(db_settings)
    schema = "test_evolution_in_code"
    table_name = "data_test_dsd"
    dataset_id = "test_dsd"

    # 1. DSD v1 object
    dsd_v1 = DSD(
        id="TEST_DSD",
        name="Test DSD",
        version="1.0",
        dimensions=[
            Dimension(id="geo", name="Geo", position=0, data_type="String"),
            Dimension(id="freq", name="Frequency", position=1, data_type="String"),
        ],
        attributes=[Attribute(id="obs_flag", name="Flag", data_type="String")],
        measures=[Measure(id="obs_value", name="Value", data_type="Double")],
        primary_measure_id="obs_value",
    )

    try:
        # 2. Initial schema preparation with DSD v1
        loader.prepare_schema(
            dsd=dsd_v1,
            table_name=table_name,
            schema=schema,
            representation="Standard",
            meta_schema="test_meta",
        )

        # 3. Simulate a previous successful ingestion record for DSD v1
        last_ingestion = IngestionHistory(
            dataset_id=dataset_id,
            dsd_version="1.0",
            status="SUCCESS",
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            load_strategy="Full",
            representation="Standard",
        )

        # 4. DSD v2 object with a data type change for 'geo'
        dsd_v2 = DSD(
            id="TEST_DSD",
            name="Test DSD",
            version="2.0",
            dimensions=[
                Dimension(id="geo", name="Geo", position=0, data_type="Integer"),
                Dimension(id="freq", name="Frequency", position=1, data_type="String"),
            ],
            attributes=[Attribute(id="obs_flag", name="Flag", data_type="String")],
            measures=[Measure(id="obs_value", name="Value", data_type="Double")],
            primary_measure_id="obs_value",
        )

        # 5. Call prepare_schema again and assert that it raises the correct error
        with pytest.raises(NotImplementedError) as excinfo:
            loader.prepare_schema(
                dsd=dsd_v2,
                table_name=table_name,
                schema=schema,
                representation="Standard",
                meta_schema="test_meta",
                last_ingestion=last_ingestion,
            )

        assert "Data type mismatch for column 'geo'" in str(excinfo.value)
        assert (
            "Existing type 'text' is not compatible with required type 'INTEGER'"
            in str(excinfo.value)
        )
    finally:
        # Clean up
        if loader.conn and not loader.conn.closed:
            with loader.conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            loader.close_connection()
