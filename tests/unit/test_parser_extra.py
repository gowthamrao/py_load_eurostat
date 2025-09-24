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
from typing import Callable

import pytest

from py_load_eurostat.models import DSD, Dimension
from py_load_eurostat.parser import TsvParser
from py_load_eurostat.transformer import Transformer


@pytest.fixture
def sample_dsd_for_parser():
    """A sample DSD fixture for parser tests."""
    return DSD(
        id="TEST_DSD",
        name="Test DSD",
        version="1.0",
        dimensions=[
            Dimension(id="geo", name="Geo", position=1),
            Dimension(id="freq", name="Frequency", position=0),
        ],
        attributes=[],
        measures=[],
        primary_measure_id="OBS_VALUE",
    )


@pytest.fixture
def tsv_file_factory(tmp_path: Path) -> Callable[[bytes], Path]:
    """A factory to create temporary gzipped TSV files for testing."""

    def _factory(content: bytes) -> Path:
        file_path = tmp_path / "test.tsv.gz"
        with gzip.open(file_path, "wb") as f:
            f.write(content)
        return file_path

    return _factory


def test_parse_tsv_with_missing_values(sample_dsd_for_parser, tsv_file_factory):
    """Test parsing a TSV file with ':' for missing values.
    The transformer should drop these values."""
    tsv_content = b"freq,geo\\TIME_PERIOD\t2022\t2023\nA,DE\t100.0\t:\nA,FR\t:\t200.0\n"
    tsv_path = tsv_file_factory(tsv_content)
    parser = TsvParser(tsv_path)
    transformer = Transformer(sample_dsd_for_parser, {})

    wide_df_iterator, dim_cols, time_cols = parser.parse()
    data_stream = transformer.transform(
        wide_df_iterator, dim_cols, time_cols, "Standard"
    )
    data = list(data_stream)

    # The transformer drops missing values, so we expect only 2 records.
    assert len(data) == 2

    assert data[0].value == 100.0
    assert data[0].dimensions["geo"] == "DE"
    assert data[0].time_period == "2022"

    assert data[1].value == 200.0
    assert data[1].dimensions["geo"] == "FR"
    assert data[1].time_period == "2023"


def test_parse_tsv_with_status_flags(sample_dsd_for_parser, tsv_file_factory):
    """Test parsing a TSV file with status flags."""
    tsv_content = b"freq,geo\\TIME_PERIOD\t2022\nA,DE\t100.0 p\nA,FR\t200.0 e\n"
    tsv_path = tsv_file_factory(tsv_content)
    parser = TsvParser(tsv_path)
    transformer = Transformer(sample_dsd_for_parser, {})

    wide_df_iterator, dim_cols, time_cols = parser.parse()
    data_stream = transformer.transform(
        wide_df_iterator, dim_cols, time_cols, "Standard"
    )
    data = list(data_stream)

    assert len(data) == 2
    assert data[0].value == 100.0
    assert data[0].flags == "p"
    assert data[1].value == 200.0
    assert data[1].flags == "e"


def test_parse_tsv_with_trailing_spaces(sample_dsd_for_parser, tsv_file_factory):
    """Test parsing a TSV file with trailing spaces after values."""
    tsv_content = b"freq,geo\\TIME_PERIOD\t2022\nA,DE\t100.0 \nA,FR\t200.0  \n"
    tsv_path = tsv_file_factory(tsv_content)
    parser = TsvParser(tsv_path)
    transformer = Transformer(sample_dsd_for_parser, {})

    wide_df_iterator, dim_cols, time_cols = parser.parse()
    data_stream = transformer.transform(
        wide_df_iterator, dim_cols, time_cols, "Standard"
    )
    data = list(data_stream)

    assert len(data) == 2
    assert data[0].value == 100.0
    assert data[0].flags is None
    assert data[1].value == 200.0
    assert data[1].flags is None


def test_parse_tsv_with_multiple_frequencies(sample_dsd_for_parser, tsv_file_factory):
    """Test parsing a TSV with mixed annual and quarterly data."""
    tsv_content = (
        b"freq,geo\\TIME_PERIOD\t2022\t2022-Q1\t2022-Q2\n"
        b"A,DE\t100.0\t:\t:\n"
        b"Q,DE\t:\t25.0\t26.0\n"
    )
    tsv_path = tsv_file_factory(tsv_content)
    parser = TsvParser(tsv_path)
    transformer = Transformer(sample_dsd_for_parser, {})

    wide_df_iterator, dim_cols, time_cols = parser.parse()
    data_stream = transformer.transform(
        wide_df_iterator, dim_cols, time_cols, "Standard"
    )
    data = list(data_stream)

    # The transformer will drop missing values, so we expect 3 records.
    assert len(data) == 3

    # The order is not guaranteed after melting, so we check for existence
    values_and_periods = {(d.value, d.time_period) for d in data}

    assert (100.0, "2022") in values_and_periods
    assert (25.0, "2022-Q1") in values_and_periods
    assert (26.0, "2022-Q2") in values_and_periods
