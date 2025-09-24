"""
Integration tests for the schema evolution feature in the SQLiteLoader.
"""

import sqlite3
from pathlib import Path

import pytest

from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.sqlite import SQLiteLoader
from py_load_eurostat.models import DSD, Dimension, Observation

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def db_settings(tmp_path: Path) -> DatabaseSettings:
    """Returns database settings configured to connect to a temporary SQLite DB."""
    db_file = tmp_path / "test_evolution.db"
    return DatabaseSettings(name=str(db_file))


def test_schema_evolution_and_data_loading(db_settings: DatabaseSettings):
    """
    Verifies that the SQLiteLoader can:
    1. Create an initial table.
    2. Load data into it.
    3. Evolve the schema by adding a new column.
    4. Load new data into the evolved table.
    5. Correctly retrieve data from both loads.
    """
    # 1. Define initial and evolved DSDs
    from py_load_eurostat.models import Measure

    initial_dsd = DSD(
        id="test_dsd",
        name="Test DSD",
        version="1.0",
        dimensions=[
            Dimension(id="dim1", name="Dimension 1", codelist_id="cl1", position=1),
        ],
        attributes=[],
        measures=[Measure(id="obs_value", name="Observation Value")],
        primary_measure_id="obs_value",
    )

    evolved_dsd = DSD(
        id="test_dsd",
        name="Test DSD",
        version="2.0",
        dimensions=[
            Dimension(id="dim1", name="Dimension 1", codelist_id="cl1", position=1),
            Dimension(
                id="new_dim", name="New Dimension", codelist_id="cl2", position=2
            ),  # New dimension
        ],
        attributes=[],
        measures=[Measure(id="obs_value", name="Observation Value")],
        primary_measure_id="obs_value",
    )

    table_name = "test_evolution_table"
    schema = "test_schema"
    table_fqn = f"{schema}__{table_name}"
    loader = SQLiteLoader(db_settings)

    # 2. Initial Load
    loader.prepare_schema(
        initial_dsd,
        table_name,
        schema,
        representation="Standard",
        meta_schema="test_meta",
    )

    def stream_gen_v1():
        yield Observation(
            dimensions={"dim1": "A"}, time_period="2022", value=10.0, flags="p"
        )

    staging_v1, rows_v1 = loader.bulk_load_staging(table_name, schema, stream_gen_v1())
    loader.finalize_load(staging_v1, table_name, schema, strategy="swap")
    assert rows_v1 == 1

    # 3. Verify initial data
    conn = sqlite3.connect(db_settings.name)
    cur = conn.cursor()
    cur.execute(f"SELECT dim1 FROM {table_fqn} WHERE time_period = '2022'")
    assert cur.fetchone()[0] == "A"

    # Check that the new column doesn't exist yet
    cur.execute(f"PRAGMA table_info({table_fqn});")
    cols = {row[1] for row in cur.fetchall()}
    assert "new_dim" not in cols

    # 4. Evolved Load
    # Re-initialize loader with the evolved DSD for subsequent loading steps
    loader.dsd = evolved_dsd
    loader.prepare_schema(
        evolved_dsd,
        table_name,
        schema,
        representation="Standard",
        meta_schema="test_meta",
    )

    def stream_gen_v2():
        yield Observation(
            dimensions={"dim1": "B", "new_dim": "X"},
            time_period="2023",
            value=20.0,
            flags="e",
        )

    # The `finalize_load` process replaces the table, so we load all data again.
    # A real delta load would merge, but for this test, replacement is fine.
    # We'll load both old and new data into the new table structure.
    def stream_gen_full():
        yield Observation(
            dimensions={"dim1": "A", "new_dim": None},
            time_period="2022",
            value=10.0,
            flags="p",
        )
        yield Observation(
            dimensions={"dim1": "B", "new_dim": "X"},
            time_period="2023",
            value=20.0,
            flags="e",
        )

    staging_v2, rows_v2 = loader.bulk_load_staging(
        table_name, schema, stream_gen_full()
    )
    loader.finalize_load(staging_v2, table_name, schema, strategy="swap")
    assert rows_v2 == 2

    # 5. Verify evolved state and data integrity
    cur.execute(f"PRAGMA table_info({table_fqn});")
    cols = {row[1] for row in cur.fetchall()}
    assert "new_dim" in cols, "Schema evolution should have added 'new_dim'"

    # Check old data point
    cur.execute(f"SELECT dim1, new_dim FROM {table_fqn} WHERE time_period = '2022'")
    row = cur.fetchone()
    assert row[0] == "A"
    assert row[1] is None, "Old data should have NULL for the new column"

    # Check new data point
    cur.execute(f"SELECT dim1, new_dim FROM {table_fqn} WHERE time_period = '2023'")
    row = cur.fetchone()
    assert row[0] == "B"
    assert row[1] == "X", "New data should be correctly inserted into the new column"

    conn.close()
    loader.close_connection()
