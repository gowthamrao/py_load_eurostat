"""
Parser module for interpreting raw data from Eurostat.

This module contains parsers for two types of files:
1.  SDMX-ML (XML) files for metadata (DSDs, Codelists), using the `pysdmx` library.
2.  Eurostat's specific gzipped TSV format for observational data.
"""
import gzip
import logging
from pathlib import Path
from typing import Dict, List, Iterator, Any

import pandas as pd
import pysdmx
from pysdmx.model import Dataflow, CodeList as SdmxCodeList, DataStructureDefinition

from .models import DSD, Dimension, Attribute, CodeList as AppCodeList, Code as AppCode

logger = logging.getLogger(__name__)

class SdmxParser:
    """Parses SDMX-ML files into the application's Pydantic models."""

    def parse_dsd_from_dataflow(self, xml_path: Path) -> DSD:
        """
        Parses a DSD from a Dataflow SDMX file.

        Args:
            xml_path: Path to the SDMX-ML file containing the Dataflow definition.

        Returns:
            A DSD Pydantic model.
        """
        logger.info(f"Parsing DSD from dataflow file: {xml_path}")
        message = pysdmx.read_sdmx(xml_path)

        # We expect the dataflow file to contain one dataflow, which in turn
        # references one DSD.
        dataflow: Dataflow = message.structure.dataflows[0]
        dsd: DataStructureDefinition = dataflow.structure

        dimensions = [
            Dimension(id=dim.id, codelist_id=dim.representation.id, position=dim.order)
            for dim in dsd.dimensions
        ]
        attributes = [
            Attribute(id=attr.id, codelist_id=getattr(attr.representation, 'id', None))
            for attr in dsd.attributes
        ]

        return DSD(
            id=dsd.id,
            version=dsd.version,
            dimensions=dimensions,
            attributes=attributes,
            primary_measure_id=dsd.measure.id
        )

    def parse_codelist(self, xml_path: Path) -> AppCodeList:
        """
        Parses a Codelist from an SDMX file.

        Args:
            xml_path: Path to the SDMX-ML file containing the Codelist.

        Returns:
            A CodeList Pydantic model.
        """
        logger.info(f"Parsing codelist file: {xml_path}")
        message = pysdmx.read_sdmx(xml_path)
        sdmx_codelist: SdmxCodeList = message.structure.codelists[0]

        codes = {
            code.id: AppCode(
                id=code.id,
                name=str(code.name), # Name is a LocalisedString
                description=str(code.description) if code.description else None,
                parent_id=code.parent if code.parent else None,
            )
            for code in sdmx_codelist.codes
        }

        return AppCodeList(
            id=sdmx_codelist.id,
            version=sdmx_codelist.version,
            codes=codes,
        )


class TsvParser:
    """
    A memory-efficient parser for Eurostat's gzipped TSV data files.

    This parser works as an iterator, yielding one raw data row at a time
    as a dictionary.
    """
    def __init__(self, tsv_gz_path: Path):
        self.path = tsv_gz_path
        self.dimension_cols: List[str] = []
        self.time_period_cols: List[str] = []
        self.header_fields: List[str] = []

    def _parse_header(self, header_line: str):
        """Parses the unique Eurostat TSV header."""
        dim_block, *time_periods = header_line.strip().split('\t')
        # The last dimension is conjoined with '\time'
        dim_str, _ = dim_block.split('\\')
        self.dimension_cols = dim_str.split(',')
        self.time_period_cols = [p.strip() for p in time_periods]
        self.header_fields = self.dimension_cols + self.time_period_cols
        logger.debug(f"Parsed dimensions: {self.dimension_cols}")
        logger.debug(f"Parsed time periods: {self.time_period_cols}")

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Reads the TSV file row-by-row, yielding a dictionary for each.
        """
        logger.info(f"Streaming and parsing TSV file: {self.path}")
        with gzip.open(self.path, "rt", encoding="utf-8") as f:
            self._parse_header(f.readline())

            # Use pandas to read the rest of the file in chunks for efficiency.
            # This is much faster than manual line-by-line parsing in Python.
            chunk_iter = pd.read_csv(
                f,
                sep='\t',
                names=self.header_fields,
                chunksize=20000,
                engine='c',
                na_values=[': ', ':'] # Eurostat uses ':' for missing values
            )
            for chunk_df in chunk_iter:
                # itertuples is much faster than iterrows
                for row in chunk_df.itertuples(index=False):
                    yield dict(zip(self.header_fields, row))

        logger.info(f"Finished parsing TSV file: {self.path}")
