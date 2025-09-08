# Integration tests for high-level pipeline features.
# This file will contain tests for features like codelist loading,
# "Full" representation, and delta-load logic.
from typing import Generator

import pandas as pd
import pytest
from psycopg.rows import dict_row

from py_load_eurostat.config import DatabaseSettings
from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.models import Attribute, Code, Codelist, DSD, Dimension, Observation
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
def test_codelist_loading(
    db_settings: DatabaseSettings, sample_geo_codelist: Codelist
):
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
            cur.execute(
                "SELECT to_regclass(%s) as oid;", (f"{schema}.{table_name}",)
            )
            assert cur.fetchone()["oid"] is not None, f"Table {schema}.{table_name} should exist."

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
        version="1.0",
        dimensions=[
            Dimension(id="geo", position=0, codelist_id="cl_geo"),
            Dimension(id="indic_de", position=1, codelist_id=None),
        ],
        attributes=[Attribute(id="obs_flags")],
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
