# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import gzip
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from py_load_eurostat.parser import SdmxParser, TsvParser


@pytest.fixture
def malformed_tsv_file(tmp_path: Path) -> Path:
    """Create a gzipped TSV file with a non-string value in the dimensions column."""
    header = "unit,sex,age,geo\\time\t2020\t2019\n"
    # This file contains a single row with a numeric value in the first column.
    # pandas will infer this column as numeric, triggering the type check.
    content = header + "123\t15.0\t25.0\n"

    file_path = tmp_path / "malformed.tsv.gz"
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        f.write(content)
    return file_path


@pytest.fixture
def dsd_xml_with_codelist_map(tmp_path: Path) -> Path:
    """Create a dummy DSD SDMX file with a codelist mapping."""
    content = """<?xml version="1.0" encoding="UTF-8"?>
<message:Structure xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
                   xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                   xmlns:c="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
    <message:Structures>
        <s:DataStructureDefinition id="DSD_ID" version="1.0">
            <s:DataStructureComponents>
                <s:Dimension id="DIM1">
                    <s:Enumeration>
                        <c:Ref id="CL_DIM1" />
                    </s:Enumeration>
                </s:Dimension>
                <s:Dimension id="DIM2">
                    <s:Enumeration>
                        <c:Ref id="CL_DIM2" />
                    </s:Enumeration>
                </s:Dimension>
            </s:DataStructureComponents>
        </s:DataStructureDefinition>
    </message:Structures>
</message:Structure>
"""
    file_path = tmp_path / "dsd_with_map.sdmx"
    file_path.write_text(content)
    return file_path


def test_parse_dsd_from_dataflow_no_structures(mocker):
    """
    Test that parse_dsd_from_dataflow raises ValueError when SDMX has no structures.
    """
    mock_message = MagicMock()
    mock_message.structures = []
    mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)
    parser = SdmxParser()

    with pytest.raises(ValueError, match="No structures found in the SDMX message"):
        parser.parse_dsd_from_dataflow(Path("dummy/path.sdmx"))


def test_parse_codelist_no_structures(mocker):
    """Test that parse_codelist raises ValueError when SDMX has no structures."""
    mock_message = MagicMock()
    mock_message.structures = []
    mocker.patch("py_load_eurostat.parser.read_sdmx", return_value=mock_message)
    parser = SdmxParser()

    with pytest.raises(ValueError, match="No structures found in the SDMX message"):
        parser.parse_codelist(Path("dummy/path.sdmx"))


def test_extract_codelist_map_from_xml(dsd_xml_with_codelist_map: Path):
    """Test the _extract_codelist_map_from_xml method directly."""
    parser = SdmxParser()
    codelist_map = parser._extract_codelist_map_from_xml(dsd_xml_with_codelist_map)
    assert codelist_map == {
        "dim1": "CL_DIM1",
        "dim2": "CL_DIM2",
    }


def test_tsv_parser_with_malformed_row(malformed_tsv_file: Path):
    """Test that the TsvParser can handle rows with non-string dimension values."""
    parser = TsvParser(malformed_tsv_file)
    chunk_generator, _, _ = parser.parse()

    processed_chunks = list(chunk_generator)

    assert len(processed_chunks) == 1
    df = processed_chunks[0]

    # The row's dimensions should be a list of Nones
    assert df.iloc[0]["unit"] is None
    assert df.iloc[0]["sex"] is None
    assert df.iloc[0]["age"] is None
    assert df.iloc[0]["geo"] is None
