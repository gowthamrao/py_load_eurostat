"""
Parsers for handling Eurostat data and metadata formats.

This module contains:
- SdmxParser: For parsing SDMX-ML metadata files (DSDs, Codelists).
- TsvParser: For parsing the unique structure of Eurostat TSV files.
- InventoryParser: For parsing the bulk download inventory to find dataset update times.
"""
import gzip
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .models import DSD, Attribute, Code, Codelist, Dimension

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

from typing import Tuple


class TsvParser:
    """
    Parses a Eurostat gzipped TSV file, returning the raw data in a wide
    format DataFrame and the parsed dimension/time columns.
    """
    def __init__(self, tsv_path: Path):
        self.tsv_path = tsv_path

    def parse(self) -> Tuple[pd.DataFrame, List[str], List[str]]:
        """
        Parses the TSV file into a pandas DataFrame without transformation.

        Returns:
            A tuple containing:
            - The raw data in a wide DataFrame.
            - A list of the dimension column names.
            - A list of the time period column names.
        """
        # 1. Read header line to get dimension and time period columns
        with gzip.open(self.tsv_path, 'rt', encoding='utf-8') as f:
            header_line = f.readline().strip()

        header_parts = header_line.split('\t')
        dim_header = header_parts[0]
        dimension_cols = [d.strip() for d in dim_header.split(',')]
        # Clean up the `geo\time` part
        if dimension_cols and '\\' in dimension_cols[-1]:
            last_dim_parts = dimension_cols[-1].split('\\')
            dimension_cols[-1] = last_dim_parts[0]
            # The part after the slash is the name of the time dimension itself
            time_dim_name = last_dim_parts[1] if len(last_dim_parts) > 1 else 'time'
        else:
            time_dim_name = 'time' # Fallback name

        time_period_cols = [p.strip() for p in header_parts[1:]]

        # 2. Read the data using pandas, using the parsed header
        df = pd.read_csv(
            self.tsv_path,
            compression='gzip',
            sep='\t',
            header=0,
            na_values=[': ', ':']
        )
        df.rename(columns={df.columns[0]: 'dimensions_combined'}, inplace=True)

        # 3. Split the combined dimension column into separate columns
        dims_df = df['dimensions_combined'].str.split(',', expand=True)
        # Assign names to the new dimension columns, ensuring we don't overrun
        dims_df.columns = dimension_cols[:dims_df.shape[1]]

        # 4. Combine the new dimension columns with the time period data
        raw_wide_df = pd.concat([dims_df, df[time_period_cols]], axis=1)

        logger.info(f"Parsed {self.tsv_path} into a wide DataFrame with {len(raw_wide_df)} rows.")
        return raw_wide_df, dimension_cols, time_period_cols

class TocParser:
    """
    Parses the Eurostat Table of Contents (TOC) file.

    The TOC provides metadata about all available bulk download files, including
    dataset codes, titles, update times, and download URLs.
    """
    def __init__(self, toc_path: Path):
        self.toc_path = toc_path
        self._toc_data: Dict[str, Dict] = {}
        self._load_toc()

    def _load_toc(self) -> None:
        """
        Loads the tab-separated TOC file into a dictionary for easy lookup.
        The dictionary maps dataset codes to their metadata.
        """
        logger.info(f"Loading and parsing Table of Contents file: {self.toc_path}")
        try:
            with open(self.toc_path, "r", encoding="utf-8") as f:
                # Skip header line
                next(f, None)
                for line in f:
                    # Strip quotes and whitespace from each part
                    parts = [p.strip().strip('"') for p in line.strip().split('\t')]
                    # The 'type' column (index 2) tells us if it's a downloadable dataset
                    if len(parts) < 7 or parts[2] not in ("table", "dataset"):
                        continue

                    code = parts[1]
                    # We only care about datasets, which have a 'code'
                    if not code:
                        continue

                    # The download URL is the last column
                    url_part = parts[-1]
                    if not url_part.endswith(".tsv.gz"):
                        continue

                    self._toc_data[code.lower()] = {
                        'url': f"https://ec.europa.eu/eurostat/api/dissemination{url_part}",
                        'last_update': pd.to_datetime(parts[3], utc=True)
                    }
            logger.info(f"Successfully parsed {len(self._toc_data)} dataset entries from TOC.")
        except FileNotFoundError:
            logger.error(f"TOC file not found at {self.toc_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to parse TOC file {self.toc_path}: {e}", exc_info=True)


    def get_last_update_timestamp(self, dataset_id: str) -> Optional[datetime]:
        """
        Gets the last update timestamp for a specific dataset from the TOC.

        Args:
            dataset_id: The code of the dataset (e.g., 'nama_10_gdp').

        Returns:
            A timezone-aware datetime object or None if the dataset is not found.
        """
        dataset_info = self._toc_data.get(dataset_id.lower())
        return dataset_info['last_update'] if dataset_info else None

    def get_download_url(self, dataset_id: str) -> Optional[str]:
        """
        Gets the full download URL for a specific dataset from the TOC.

        Args:
            dataset_id: The code of the dataset (e.g., 'nama_10_gdp').

        Returns:
            The full download URL string or None if not found.
        """
        dataset_info = self._toc_data.get(dataset_id.lower())
        return dataset_info['url'] if dataset_info else None
