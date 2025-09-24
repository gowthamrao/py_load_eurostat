# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Unit tests for the parser module.
"""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pysdmx.model.code import Code as PysdmxCode
from pysdmx.model.code import Codelist as PysdmxCodelist
from pysdmx.model.dataflow import Component, DataStructureDefinition, Role
from pysdmx.model.message import Message

from py_load_eurostat.parser import InventoryParser, SdmxParser, TsvParser

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
    mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)

    # Mock the enumeration property to return a mock with an id
    mocker.patch.object(
        Component, "enumeration", new_callable=mocker.PropertyMock
    ).return_value = MagicMock(id="CL_GEO")

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


@pytest.fixture
def sample_inventory_path(tmp_path: Path) -> Path:
    """Creates a sample inventory file for testing."""
    # This content mimics the new inventory file format
    content = (
        "Code\tType\tSource dataset\tLast data change\t"
        "Last structural change\tData download url (tsv)\n"
        "tps00001\tDATASET\t-\t2024-07-26T23:00:00+0200\t"
        "2024-03-13T23:00:00+0100\thttps://example.com/data/tps00001.tsv.gz\n"
        "another_dataset\tDATASET\t-\t2024-07-25T23:00:00+0200\t"
        "2024-03-13T23:00:00+0100\thttps://example.com/data/another_dataset.tsv.gz\n"
    )
    inventory_file = tmp_path / "sample_inventory.tsv"
    inventory_file.write_text(content, encoding="utf-8")
    return inventory_file


def test_inventory_parser(sample_inventory_path: Path):
    """Tests that the InventoryParser correctly parses the inventory file."""
    parser = InventoryParser(sample_inventory_path)

    # Test getting a valid download URL
    expected_url = "https://example.com/data/tps00001.tsv.gz"
    assert parser.get_download_url("tps00001") == expected_url
    assert (
        parser.get_download_url("TPS00001") == expected_url
    )  # Test case-insensitivity

    # Test getting a non-existent dataset
    assert parser.get_download_url("non_existent_dataset") is None

    # Test getting a valid timestamp
    # The sample data is '2024-07-26T23:00:00+0200', which is 21:00 UTC.
    expected_ts = datetime(2024, 7, 26, 21, 0, 0, tzinfo=timezone.utc)
    assert parser.get_last_update_timestamp("tps00001") == expected_ts

    # Test getting a timestamp for a non-existent dataset
    assert parser.get_last_update_timestamp("non_existent_dataset") is None


def test_inventory_parser_file_not_found():
    """Tests that InventoryParser raises FileNotFoundError for a missing file."""
    with pytest.raises(FileNotFoundError):
        InventoryParser(Path("non_existent_file.tsv"))


def test_inventory_parser_unparsable_file(tmp_path: Path):
    """
    Tests that InventoryParser raises an exception for a malformed file.
    """
    unparsable_file = tmp_path / "unparsable.tsv"
    unparsable_file.write_text('"a" "b" "c"\n"d" "e')  # Malformed CSV
    with pytest.raises(Exception):
        InventoryParser(unparsable_file)


def test_tsv_parser_bad_header():
    """Tests that TsvParser raises ValueError for a malformed header."""
    bad_header_path = FIXTURES_DIR / "bad_header.tsv.gz"
    parser = TsvParser(bad_header_path)
    with pytest.raises(ValueError, match="Invalid TSV header format"):
        parser.parse()


def test_tsv_parser_missing_dims():
    """
    Tests that TsvParser handles rows with missing dimension values gracefully.
    """
    missing_dims_path = FIXTURES_DIR / "missing_dims.tsv.gz"
    parser = TsvParser(missing_dims_path)
    chunk_iterator, _, _ = parser.parse()
    # Consume the iterator to trigger the parsing logic
    chunks = list(chunk_iterator)
    # The main assertion is that this runs without error.
    # We can also check the output.
    assert len(chunks) == 1
    df = chunks[0]
    # The second row's dimension 'freq' should be None (or NaN)
    assert pd.isna(df.iloc[1]["freq"])


# --- SdmxParser Error Tests ---


def test_sdmx_parser_empty_file_dsd():
    """Tests that parsing an empty file for a DSD raises a ValueError."""
    parser = SdmxParser()
    empty_path = FIXTURES_DIR / "empty.xml"
    with pytest.raises(ValueError, match="Failed to parse SDMX file"):
        parser.parse_dsd_from_dataflow(empty_path)


def test_sdmx_parser_empty_file_codelist():
    """Tests that parsing an empty file for a Codelist raises a ValueError."""
    parser = SdmxParser()
    empty_path = FIXTURES_DIR / "empty.xml"
    with pytest.raises(ValueError, match="Failed to parse SDMX file"):
        parser.parse_codelist(empty_path)


def test_sdmx_parser_wrong_type_dsd():
    """
    Tests that parsing a Codelist file as a DSD raises a TypeError.
    """
    parser = SdmxParser()
    # Pass a codelist file where a DSD is expected
    codelist_path = FIXTURES_DIR / "codelist_geo.xml"
    with pytest.raises(TypeError, match="Could not find a valid"):
        parser.parse_dsd_from_dataflow(codelist_path)


def test_sdmx_parser_wrong_type_codelist():
    """
    Tests that parsing a DSD file as a Codelist raises a ValueError because
    the underlying pysdmx library will fail to parse it correctly.
    """
    parser = SdmxParser()
    # Pass a DSD file where a Codelist is expected
    dsd_path = FIXTURES_DIR / "dsd_tps00001.xml"
    with pytest.raises(ValueError, match="Failed to parse SDMX file"):
        parser.parse_codelist(dsd_path)


class TestSdmxParserErrorCases:
    def test_parse_dsd_no_structures(self, tmp_path, mocker):
        from unittest.mock import MagicMock

        mock_message = MagicMock()
        mock_message.structures = []
        mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)
        parser = SdmxParser()
        file = tmp_path / "no_structs.xml"
        file.touch()
        with pytest.raises(ValueError, match="No structures found"):
            parser.parse_dsd_from_dataflow(file)

    def test_parse_dsd_no_dsd_node_no_dataflow(self, tmp_path, mocker):
        from unittest.mock import MagicMock

        mock_message = MagicMock()
        mock_message.structures = [MagicMock()]  # Not a DSD
        mock_message.dataflow = {}
        mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)
        parser = SdmxParser()
        file = tmp_path / "no_dsd.xml"
        file.touch()
        with pytest.raises(TypeError, match="Could not find a valid"):
            parser.parse_dsd_from_dataflow(file)

    def test_parse_codelist_no_structures(self, tmp_path, mocker):
        from unittest.mock import MagicMock

        mock_message = MagicMock()
        mock_message.structures = []
        mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)
        parser = SdmxParser()
        file = tmp_path / "no_structs_cl.xml"
        file.touch()
        with pytest.raises(ValueError, match="No structures found"):
            parser.parse_codelist(file)


def test_tsv_parser_malformed_dim_string(mocker, tmp_path):
    """
    Tests that TsvParser handles rows with malformed dimension strings (NaN).
    """
    # 1. Arrange: Mock pandas.read_csv to return a chunk with a NaN value
    mock_chunk = pd.DataFrame({"unit,geo\\time": [pd.NA], "2022": [100.0]})
    mocker.patch("pandas.read_csv", return_value=iter([mock_chunk]))

    # We still need a valid-looking file for the header parsing to work
    dummy_path = tmp_path / "dummy.tsv.gz"
    import gzip

    with gzip.open(dummy_path, "wt") as f:
        f.write("unit,geo\\time\t2022\n")

    # 2. Act
    parser = TsvParser(dummy_path)
    chunk_iterator, _, _ = parser.parse()
    processed_chunks = list(chunk_iterator)
    result_df = processed_chunks[0]

    # 3. Assert
    # The row with the NaN dimension string should result in NaN/None values
    # for the individual dimension columns.
    assert result_df.shape[0] == 1
    assert pd.isna(result_df.iloc[0]["unit"])
    assert pd.isna(result_df.iloc[0]["geo"])
