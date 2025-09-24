# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Unit tests for the transformer module.
"""

from pathlib import Path

import pytest

from py_load_eurostat.models import DSD, Code, Codelist, Dimension
from py_load_eurostat.parser import TsvParser
from py_load_eurostat.transformer import Transformer

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def mock_dsd() -> DSD:
    """Provides a mock DSD object for testing."""
    from py_load_eurostat.models import Measure

    return DSD(
        id="DSD_TPS00001",
        name="Test DSD",
        version="1.0",
        dimensions=[Dimension(id="geo", name="Geo", codelist_id="CL_GEO", position=1)],
        attributes=[],
        measures=[Measure(id="OBS_VALUE", name="Observation Value")],
        primary_measure_id="OBS_VALUE",
    )


@pytest.mark.parametrize(
    "raw_input, expected_value, expected_flag",
    [
        ("12.34 p", 12.34, "p"),
        ("12.34", 12.34, None),
        ("-5.0", -5.0, None),
        (": ", None, ":"),
        (":", None, ":"),
        ("c", None, "c"),
        (" 12.34 p ", 12.34, "p"),
        (None, None, None),
        ("", None, None),
        ("1.2.3 p", None, "1.2.3 p"),
    ],
)
def test_transformer_parse_value(raw_input, expected_value, expected_flag, mock_dsd):
    """Tests the _parse_value method with various inputs."""
    # Transformer requires a DSD, even if not used in this specific method
    transformer = Transformer(dsd=mock_dsd, codelists={})
    value, flag = transformer._parse_value(raw_input)
    assert value == expected_value
    assert flag == expected_flag


def test_transformer_transform(mock_dsd):
    """Tests the main transform generator method."""
    # 1. Setup: Parse the file first to get the wide dataframe
    tsv_path = FIXTURES_DIR / "tps00001.tsv.gz"
    parser = TsvParser(tsv_path)
    wide_df, dim_cols, time_cols = parser.parse()

    # 2. Execution: Transform the parsed data
    transformer = Transformer(dsd=mock_dsd, codelists={})
    observations = list(transformer.transform(wide_df, dim_cols, time_cols))

    # 3. Assertions
    # The source file has 3 data rows and 2 time periods, with one missing value.
    # So we expect (3 * 2) - 1 = 5 observations.
    assert len(observations) == 5

    # Spot-check a few observations to ensure correctness

    # Find the observation for DE in 2022
    de_2022 = next(
        (
            obs
            for obs in observations
            if obs.dimensions.get("geo") == "DE" and obs.time_period == "2022"
        ),
        None,
    )
    assert de_2022 is not None
    assert de_2022.value == 12.5
    assert de_2022.flags == "p"

    # Find the observation for FR in 2021
    fr_2021 = next(
        (
            obs
            for obs in observations
            if obs.dimensions.get("geo") == "FR" and obs.time_period == "2021"
        ),
        None,
    )
    assert fr_2021 is not None
    assert fr_2021.value == 8.2
    assert fr_2021.flags is None

    # Ensure the missing value for FR in 2022 was not included
    fr_2022 = next(
        (
            obs
            for obs in observations
            if obs.dimensions.get("geo") == "FR" and obs.time_period == "2022"
        ),
        None,
    )
    assert fr_2022 is None


def test_transformer_transform_full_representation(mock_dsd):
    """
    Tests the transform method with 'Full' representation.

    This ensures that codes are replaced by labels.
    """
    # 1. Setup: Create mock codelists
    geo_codelist = Codelist(
        id="CL_GEO",
        version="1.0",
        codes={
            "DE": Code(
                id="DE", name="Germany", description="Federal Republic of Germany"
            ),
            "FR": Code(id="FR", name="France", description="French Republic"),
        },
    )
    mock_codelists = {"CL_GEO": geo_codelist}

    # Parse the file to get the wide dataframe
    tsv_path = FIXTURES_DIR / "tps00001.tsv.gz"
    parser = TsvParser(tsv_path)
    wide_df, dim_cols, time_cols = parser.parse()

    # 2. Execution: Transform with "Full" representation
    transformer = Transformer(dsd=mock_dsd, codelists=mock_codelists)
    observations = list(
        transformer.transform(wide_df, dim_cols, time_cols, representation="Full")
    )

    # 3. Assertions
    assert len(observations) == 5

    # Spot-check that the 'geo' dimension now contains labels instead of codes
    de_2022 = next(
        (
            obs
            for obs in observations
            if obs.dimensions.get("geo") == "Germany" and obs.time_period == "2022"
        ),
        None,
    )
    assert de_2022 is not None
    assert de_2022.value == 12.5

    fr_2021 = next(
        (
            obs
            for obs in observations
            if obs.dimensions.get("geo") == "France" and obs.time_period == "2021"
        ),
        None,
    )
    assert fr_2021 is not None
    assert fr_2021.value == 8.2

    # Check that a code that wasn't in the codelist is passed through unchanged
    eu_obs = next(
        (obs for obs in observations if obs.dimensions.get("geo") == "EU27_2020"), None
    )
    assert eu_obs is not None


def test_transformer_transform_full_representation_unknown_code(mock_dsd):
    """
    Tests that transform with 'Full' representation handles unknown codes.
    """
    # 1. Setup: Create mock codelists
    geo_codelist = Codelist(
        id="CL_GEO",
        version="1.0",
        codes={"DE": Code(id="DE", name="Germany")},
    )
    mock_codelists = {"CL_GEO": geo_codelist}

    # Parse the file to get the wide dataframe
    tsv_path = FIXTURES_DIR / "tps00001.tsv.gz"
    parser = TsvParser(tsv_path)
    wide_df, dim_cols, time_cols = parser.parse()

    # 2. Execution: Transform with "Full" representation
    transformer = Transformer(dsd=mock_dsd, codelists=mock_codelists)
    observations = list(
        transformer.transform(wide_df, dim_cols, time_cols, representation="Full")
    )

    # 3. Assertions
    # Find the observation for FR, which is not in the codelist
    fr_obs = next(
        (obs for obs in observations if obs.dimensions.get("geo") == "FR"), None
    )
    assert fr_obs is not None
