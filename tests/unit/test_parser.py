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

def test_inventory_parser():
    """
    Tests that the InventoryParser correctly finds a dataset and its update time.
    """
    from py_load_eurostat.parser import InventoryParser
    from datetime import datetime, timezone

    inventory_path = FIXTURES_DIR / "sample_inventory.csv"
    parser = InventoryParser(inventory_path)

    # 1. Test finding an existing dataset
    update_time = parser.get_last_update_timestamp("tps00001")
    assert update_time is not None
    assert update_time == datetime(2023, 10, 26, 10, 0, 0, tzinfo=timezone.utc)

    # 2. Test for a dataset that is not in the file
    update_time = parser.get_last_update_timestamp("non_existent_dataset")
    assert update_time is None

    # 3. Test that the parser is case-insensitive for the dataset_id
    update_time = parser.get_last_update_timestamp("TPS00001")
    assert update_time is not None
    assert update_time == datetime(2023, 10, 26, 10, 0, 0, tzinfo=timezone.utc)
