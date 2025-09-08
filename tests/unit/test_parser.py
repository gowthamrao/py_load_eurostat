"""
Unit tests for the parser module.
"""
from pathlib import Path

from py_load_eurostat.parser import SdmxParser

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

def test_sdmx_parser_dsd():
    """Tests that the SdmxParser correctly parses a DSD from a dataflow file."""
    parser = SdmxParser()
    dsd_path = FIXTURES_DIR / "dsd_tps00001.xml"
    dsd = parser.parse_dsd_from_dataflow(dsd_path)

    assert dsd.id == "dsd_tps00001"
    assert len(dsd.dimensions) == 1
    assert dsd.dimensions[0].id == "geo"
    assert dsd.dimensions[0].codelist_id == "CL_GEO"
    assert dsd.primary_measure_id == "obs_value"

def test_sdmx_parser_codelist():
    """Tests that the SdmxParser correctly parses a Codelist file."""
    parser = SdmxParser()
    codelist_path = FIXTURES_DIR / "codelist_geo.xml"
    codelist = parser.parse_codelist(codelist_path)

    assert codelist.id == "CL_GEO"
    assert len(codelist.codes) == 3
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
    expected_url = "https://ec.europa.eu/eurostat/api/dissemination/data/tps00001.tsv.gz"
    assert parser.get_download_url("tps00001") == expected_url
    assert parser.get_download_url("TPS00001") == expected_url  # Test case-insensitivity

    # Test getting a non-existent dataset
    assert parser.get_download_url("non_existent_dataset") is None

    # Test getting a valid timestamp
    expected_ts = datetime(2024, 7, 26, 23, 0, 0, tzinfo=timezone.utc)
    assert parser.get_last_update_timestamp("tps00001") == expected_ts

    # Test getting a timestamp for a non-existent dataset
    assert parser.get_last_update_timestamp("non_existent_dataset") is None
