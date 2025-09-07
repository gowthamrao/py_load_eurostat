"""
Unit tests for the parser module.
"""
import math
from pathlib import Path
from py_load_eurostat.parser import SdmxParser, TsvParser

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

def test_toc_parser_success():
    """Tests that TOCParser correctly finds a dataset and its update time."""
    from py_load_eurostat.parser import TOCParser
    from datetime import datetime, timezone

    parser = TOCParser()
    toc_path = FIXTURES_DIR / "sample_toc.xml"

    # Test finding an existing dataset
    update_time = parser.get_last_update_timestamp(toc_path, "DSET_TWO")
    assert update_time is not None
    assert update_time == datetime(2024, 3, 15, 0, 0, tzinfo=timezone.utc)

def test_toc_parser_not_found():
    """Tests that TOCParser returns None for a non-existent dataset."""
    from py_load_eurostat.parser import TOCParser

    parser = TOCParser()
    toc_path = FIXTURES_DIR / "sample_toc.xml"

    # Test for a dataset that is not in the file
    update_time = parser.get_last_update_timestamp(toc_path, "DSET_NONEXISTENT")
    assert update_time is None

def test_toc_parser_malformed_file(tmp_path, caplog):
    """Tests that TOCParser handles a malformed XML file gracefully."""
    from py_load_eurostat.parser import TOCParser

    parser = TOCParser()
    malformed_path = tmp_path / "malformed.xml"
    malformed_path.write_text("<xml><unclosed>")

    update_time = parser.get_last_update_timestamp(malformed_path, "DSET_ONE")
    assert update_time is None
    assert "Failed to parse TOC file" in caplog.text
