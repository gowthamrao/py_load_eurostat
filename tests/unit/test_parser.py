"""
Unit tests for the parser module.
"""

import pytest
from pathlib import Path

from py_load_eurostat.parser import SdmxParser
from pysdmx.model.dataflow import Component, DataStructureDefinition, Role
from pysdmx.model.code import Codelist as PysdmxCodelist, Code as PysdmxCode
from pysdmx.model.message import Message

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def test_sdmx_parser_dsd(mocker):
    """
    Tests that the SdmxParser correctly maps a pysdmx DSD object to the
    internal DSD model.
    """
    # 1. Arrange: Create mock pysdmx objects
    mock_pysdmx_dsd = DataStructureDefinition(
        id="DSD_ID",
        version="1.0",
        name="Test DSD",
        agency="ESTAT",
        components=[
            Component(
                id="GEO",
                required=True,
                role=Role.DIMENSION,
                concept="geo",
                local_codes="CL_GEO",
            ),
            Component(
                id="TIME_PERIOD", required=True, role=Role.DIMENSION, concept="time"
            ),
            Component(
                id="OBS_VALUE", required=True, role=Role.MEASURE, concept="obs_value"
            ),
            Component(
                id="OBS_FLAG",
                required=False,
                role=Role.ATTRIBUTE,
                concept="obs_flag",
                attachment_level="O",
            ),
        ],
    )
    mock_message = Message(structures=[mock_pysdmx_dsd])

    # Mock the read_sdmx function to return our mock message
    mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)
    # Also mock the XML parsing fallback to isolate the test
    mocker.patch(
        "py_load_eurostat.parser.SdmxParser._extract_codelist_map_from_xml",
        return_value={"geo": "CL_GEO"},
    )

    # 2. Act
    parser = SdmxParser()
    # The path doesn't matter anymore because read_sdmx is mocked
    dsd = parser.parse_dsd_from_dataflow(Path("dummy_path.xml"))

    # 3. Assert
    assert dsd.id == "DSD_ID"
    assert dsd.name == "Test DSD"
    assert len(dsd.dimensions) == 2
    assert dsd.dimensions[0].id == "GEO"
    assert dsd.dimensions[0].codelist_id == "CL_GEO"
    assert dsd.dimensions[1].id == "TIME_PERIOD"
    assert dsd.primary_measure_id == "OBS_VALUE"
    assert len(dsd.attributes) == 1
    assert dsd.attributes[0].id == "OBS_FLAG"
    assert len(dsd.measures) == 1
    assert dsd.measures[0].id == "OBS_VALUE"


def test_sdmx_parser_codelist(mocker):
    """
    Tests that the SdmxParser correctly maps a pysdmx Codelist object to the
    internal Codelist model.
    """
    # 1. Arrange: Create mock pysdmx objects
    mock_pysdmx_codelist = PysdmxCodelist(
        id="CL_GEO",
        version="1.0",
        name="Geopolitical entities",
        agency="ESTAT",
        items=[
            PysdmxCode(id="DE", name="Germany"),
            PysdmxCode(id="FR", name="France"),
        ],
    )
    mock_message = Message(structures=[mock_pysdmx_codelist])
    mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)

    # 2. Act
    parser = SdmxParser()
    codelist = parser.parse_codelist(Path("dummy_path.xml"))

    # 3. Assert
    assert codelist.id == "CL_GEO"
    assert len(codelist.codes) == 2
    assert "DE" in codelist.codes
    assert codelist.codes["DE"].name == "Germany"


from datetime import datetime, timezone

import pytest

from py_load_eurostat.parser import TocParser


@pytest.fixture
def sample_toc_path(tmp_path: Path) -> Path:
    """Creates a sample TOC file for testing."""
    # This content mimics the real TOC file format
    content = (
        "title\tcode\ttype\tlastUpdate\tlastModified\tvalues\tdownloadLink\n"
        "some metadata line that should be ignored\n"
        "data\ttps00001\ttable\t2024-07-26T23:00:00Z\t2024-07-27T04:15:33.123Z\t2\t/data/tps00001.tsv.gz\n"
        "data\tanother_dataset\ttable\t2024-07-25T23:00:00Z\t2024-07-26T04:15:33.123Z\t2\t/data/another_dataset.tsv.gz\n"
    )
    toc_file = tmp_path / "sample_toc.tsv"
    toc_file.write_text(content, encoding="utf-8")
    return toc_file


def test_toc_parser(sample_toc_path: Path):
    """Tests that the TocParser correctly parses the TOC file."""
    parser = TocParser(sample_toc_path)

    # Test getting a valid download URL
    expected_url = (
        "https://ec.europa.eu/eurostat/api/dissemination/data/tps00001.tsv.gz"
    )
    assert parser.get_download_url("tps00001") == expected_url
    assert (
        parser.get_download_url("TPS00001") == expected_url
    )  # Test case-insensitivity

    # Test getting a non-existent dataset
    assert parser.get_download_url("non_existent_dataset") is None

    # Test getting a valid timestamp
    expected_ts = datetime(2024, 7, 26, 23, 0, 0, tzinfo=timezone.utc)
    assert parser.get_last_update_timestamp("tps00001") == expected_ts

    # Test getting a timestamp for a non-existent dataset
    assert parser.get_last_update_timestamp("non_existent_dataset") is None
