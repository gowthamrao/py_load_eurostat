"""
Parsers for handling Eurostat data and metadata formats.

This module contains:
- SdmxParser: For parsing SDMX-ML metadata files (DSDs, Codelists).
- TsvParser: For parsing the unique structure of Eurostat TSV files.
- TOCParser: For parsing the Eurostat Table of Contents to find dataset update times.
"""
import gzip
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional
import xml.etree.ElementTree as ET

import pandas as pd

from .models import DSD, Attribute, Codelist, Code, Dimension

logger = logging.getLogger(__name__)

class SdmxParser:
    """Parses SDMX-ML files for DSDs and Codelists using manual XML parsing."""

    def _get_ns(self):
        return {
            'm': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            's': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'c': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
            'xml': 'http://www.w3.org/XML/1998/namespace',
        }

    def parse_dsd_from_dataflow(self, sdmx_path: Path) -> DSD:
        """Parses a DSD from a dataflow SDMX file."""
        logger.info(f"Parsing DSD from {sdmx_path}")
        tree = ET.parse(sdmx_path)
        root = tree.getroot()
        ns = self._get_ns()

        dsd_node = root.find('.//s:DataStructureDefinition', ns)
        if dsd_node is None:
            raise ValueError("No DataStructureDefinition found in the file")

        dimensions = [
            Dimension(
                id=dim.attrib['id'].lower(),
                codelist_id=dim.find('.//s:Representation/Ref', ns).attrib['id'],
                position=int(dim.attrib['position'])
            )
            for dim in dsd_node.findall('.//s:Dimension', ns)
        ]

        attributes = [
            Attribute(
                id=attr.attrib['id'].lower(),
                codelist_id=getattr(attr.find('.//s:Representation/Ref', ns), 'attrib', {}).get('id')
            )
            for attr in dsd_node.findall('.//s:Attribute', ns)
        ]

        measure = dsd_node.find('.//s:PrimaryMeasure', ns)

        return DSD(
            id=dsd_node.attrib['id'].lower(),
            version=dsd_node.attrib['version'],
            dimensions=sorted(dimensions, key=lambda d: d.position),
            attributes=attributes,
            primary_measure_id=measure.attrib['id'].lower() if measure is not None else 'obs_value',
        )

    def parse_codelist(self, sdmx_path: Path) -> Codelist:
        """Parses a Codelist from an SDMX file."""
        logger.info(f"Parsing Codelist from {sdmx_path}")
        tree = ET.parse(sdmx_path)
        root = tree.getroot()
        ns = self._get_ns()

        cl_node = root.find('.//s:Codelist', ns)
        if cl_node is None:
            raise ValueError("No Codelist found in the file")

        codes = {}
        for code_node in cl_node.findall('.//s:Code', ns):
            name_node = code_node.find('c:Name[@xml:lang="en"]', ns)
            name = name_node.text if name_node is not None else code_node.attrib['id']

            desc_node = code_node.find('c:Description[@xml:lang="en"]', ns)
            description = desc_node.text if desc_node is not None else None

            code_id = code_node.attrib['id']
            codes[code_id] = Code(
                id=code_id,
                name=name,
                description=description,
                parent_id=code_node.attrib.get('parentCode')
            )

        return Codelist(
            id=cl_node.attrib['id'],
            version=cl_node.attrib['version'],
            codes=codes,
        )

class TsvParser:
    """
    An iterator that parses a Eurostat gzipped TSV file, handles the unique
    header format, and yields data rows as dictionaries.
    """
    def __init__(self, tsv_path: Path):
        self.tsv_path = tsv_path
        self.dimension_cols: List[str] = []
        self.time_period_cols: List[str] = []
        self._iterator = self._init_iterator()

    def _init_iterator(self) -> Generator[Dict, None, None]:
        # 1. Read header line to get dimension and time period columns
        with gzip.open(self.tsv_path, 'rt', encoding='utf-8') as f:
            header_line = f.readline().strip()

        header_parts = header_line.split('\t')
        dim_header = header_parts[0]
        self.dimension_cols = [d.strip() for d in dim_header.split(',')]
        if self.dimension_cols and '\\' in self.dimension_cols[-1]:
            self.dimension_cols[-1] = self.dimension_cols[-1].split('\\')[0]

        self.time_period_cols = [p.strip() for p in header_parts[1:]]

        # 2. Read the data using pandas, skipping the original header
        df = pd.read_csv(
            self.tsv_path,
            compression='gzip',
            sep='\t',
            header=0,
            na_values=[': ', ':']
        )

        # 3. Process the first column which contains all dimensions
        first_col_name = df.columns[0]
        # Split the first column into separate dimension columns
        dims_df = df[first_col_name].str.split(',', expand=True)
        dims_df.columns = self.dimension_cols[:dims_df.shape[1]]

        # 4. Combine the new dimension columns with the time period data
        full_df = pd.concat([dims_df, df[self.time_period_cols]], axis=1)

        # 5. Melt the dataframe to transform from wide to long format
        melted_df = full_df.melt(
            id_vars=self.dimension_cols,
            value_vars=self.time_period_cols,
            var_name='time_period',
            value_name='value'
        )

        # 6. Drop rows with missing values, which were not loaded by pandas
        melted_df.dropna(subset=['value'], inplace=True)

        # 7. Yield rows as dictionaries
        yield from melted_df.to_dict(orient='records')

    def __iter__(self) -> Generator[Dict, None, None]:
        return self._iterator

    def __next__(self):
        return next(self._iterator)

class TOCParser:
    """Parses the Eurostat Table of Contents (TOC) file."""

    def get_last_update_timestamp(
        self, toc_path: Path, dataset_id: str
    ) -> Optional[datetime]:
        """
        Finds the last update timestamp for a specific dataset from the TOC file
        by manually parsing the XML to avoid pysdmx issues with TOC-only files.
        """
        logger.info(f"Parsing TOC file {toc_path} for dataset '{dataset_id}'")
        try:
            tree = ET.parse(toc_path)
            root = tree.getroot()

            # Define the namespaces used in the SDMX file
            ns = {
                'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
                'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            }

            # Find all Dataflow elements within the Structures -> Dataflows path
            dataflows = root.findall('.//structure:Dataflow', ns)

            for dataflow in dataflows:
                if dataflow.attrib.get('id', '').lower() == dataset_id.lower():
                    timestamp_str = dataflow.attrib.get('validFrom')
                    if timestamp_str:
                        # Parse the ISO 8601 timestamp string
                        # It might have a 'Z' for UTC, which Python < 3.11 doesn't like
                        if timestamp_str.endswith('Z'):
                            timestamp_str = timestamp_str[:-1] + '+00:00'
                        dt = datetime.fromisoformat(timestamp_str)
                        logger.info(f"Found dataflow for '{dataset_id}' with validFrom date: {dt}")
                        return dt

            logger.warning(f"Dataset '{dataset_id}' not found in the TOC file.")
            return None
        except (ET.ParseError, KeyError, ValueError) as e:
            logger.error(f"Failed to parse TOC file {toc_path}: {e}", exc_info=True)
            return None
