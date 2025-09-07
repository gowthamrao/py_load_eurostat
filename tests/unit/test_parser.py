"""
Unit tests for the parser module.
"""
from pathlib import Path
from eurostat_loader.parser import SdmxParser, TsvParser

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

def test_sdmx_parser_dsd():
    """Tests that the SdmxParser correctly parses a DSD from a dataflow file."""
    parser = SdmxParser()
    dsd_path = FIXTURES_DIR / "dsd_tps00001.xml"
    dsd = parser.parse_dsd_from_dataflow(dsd_path)

    assert dsd.id == "DSD_TPS00001"
    assert len(dsd.dimensions) == 1
    assert dsd.dimensions[0].id == "GEO"
    assert dsd.dimensions[0].codelist_id == "CL_GEO"
    assert dsd.primary_measure_id == "OBS_VALUE"

def test_sdmx_parser_codelist():
    """Tests that the SdmxParser correctly parses a Codelist file."""
    parser = SdmxParser()
    codelist_path = FIXTURES_DIR / "codelist_geo.xml"
    codelist = parser.parse_codelist(codelist_path)

    assert codelist.id == "CL_GEO"
    assert len(codelist.codes) == 3
    assert "DE" in codelist.codes
    assert codelist.codes["DE"].name == "Germany"

def test_tsv_parser():
    """Tests that the TsvParser correctly parses the header and data rows."""
    tsv_path = FIXTURES_DIR / "tps00001.tsv.gz"
    parser = TsvParser(tsv_path)

    # The parser is an iterator, convert to list to inspect it
    rows = list(parser)

    # Check header parsing
    assert parser.dimension_cols == ["geo"]
    assert parser.time_period_cols == ["2022", "2021"]

    # Check row content
    assert len(rows) == 3 # The raw file has 3 lines of data
    assert rows[0]["geo"] == "EU27_2020"
    assert rows[0]["2022"] == "10.0"
    assert rows[1]["geo"] == "DE"
    assert rows[1]["2022"] == "12.5 p"
    assert rows[2]["2021"] == "8.2"
    assert rows[2]["2022"] == ":" # Check missing value representation
