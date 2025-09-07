"""
Unit tests for the transformer module.
"""
from pathlib import Path
import pytest
from eurostat_loader.models import DSD, CodeList, Dimension
from eurostat_loader.transformer import Transformer

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

@pytest.fixture
def mock_dsd() -> DSD:
    """Provides a mock DSD object for testing."""
    return DSD(
        id="DSD_TPS00001",
        version="1.0",
        dimensions=[Dimension(id="geo", codelist_id="CL_GEO", position=1)],
        attributes=[],
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
    transformer = Transformer(dsd=mock_dsd, codelists={})
    tsv_path = FIXTURES_DIR / "tps00001.tsv.gz"

    observations = list(transformer.transform(tsv_path))

    # We expect 3 observations, as one value in the source file is missing (':')
    assert len(observations) == 3

    # Check the first observation
    obs1 = observations[0]
    assert obs1.dimensions["geo"] == "EU27_2020"
    assert obs1.time_period == "2022"
    assert obs1.value == 10.0
    assert obs1.flags is None

    # Check the second observation
    obs2 = observations[1]
    assert obs2.dimensions["geo"] == "EU27_2020"
    assert obs2.time_period == "2021"
    assert obs2.value == 9.5
    assert obs2.flags is None

    # Check the third observation (from the second row of the file)
    obs3 = observations[2]
    assert obs3.dimensions["geo"] == "DE"
    assert obs3.time_period == "2022"
    assert obs3.value == 12.5
    assert obs3.flags == "p"
