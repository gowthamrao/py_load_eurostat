"""
Parsers for handling Eurostat data and metadata formats.

This module contains:
- SdmxParser: For parsing SDMX-ML metadata files (DSDs, Codelists).
- TsvParser: For parsing the unique structure of Eurostat TSV files.
- InventoryParser: For parsing the bulk download inventory to find dataset update times.
"""

import gzip
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, Iterator, List, Optional, Tuple

import pandas as pd
from pysdmx.io import read_sdmx
from pysdmx.model.code import Codelist as PysdmxCodelist
from pysdmx.model.dataflow import (
    Component,
    DataStructureDefinition as PysdmxDSD,
    Role,
)

from .models import DSD, Attribute, Code, Codelist, Dimension

logger = logging.getLogger(__name__)


class SdmxParser:
    """Parses SDMX-ML files for DSDs and Codelists using the pysdmx library."""

    def parse_dsd_from_dataflow(self, sdmx_path: Path) -> DSD:
        """Parses a DSD from a dataflow SDMX file."""
        logger.info(f"Parsing DSD from {sdmx_path} using pysdmx")
        message = read_sdmx(sdmx_path, validate=False)

        if not message.structures:
            raise ValueError("No structures found in the SDMX message")

        # A structure message can contain a DSD directly or a Dataflow that
        # refers to a DSD. The parser must handle both cases.

        # 1. Try to find a DSD directly in the message structures.
        dsd_node = next(
            (s for s in message.structures if isinstance(s, PysdmxDSD)), None
        )

        # 2. If no direct DSD, try to find it via a Dataflow reference.
        if not dsd_node:
            if hasattr(message, "dataflow") and message.dataflow:
                # Take the first dataflow found
                dataflow = list(message.dataflow.values())[0]
                dsd_node = dataflow.structure
            else:
                # Fallback for messages that might just contain structures
                # but not a dataflow attribute. This is a bit of a guess.
                pass

        if not isinstance(dsd_node, PysdmxDSD):
            raise TypeError(
                "Could not find a valid DataStructureDefinition in the SDMX message, "
                "either directly or referenced from a Dataflow."
            )

        dimensions: list[Dimension] = []
        attributes: list[Attribute] = []
        primary_measure_id = "obs_value"  # Default

        # Hotfix: pysdmx does not seem to reliably expose the codelist reference
        # on the component object when parsing from a file. We will parse the
        # XML manually to extract this mapping as a fallback.
        dim_to_cl_map = self._extract_codelist_map_from_xml(sdmx_path)

        for i, component in enumerate(dsd_node.components):
            if component.role == Role.DIMENSION:
                dim_id_lower = component.id.lower()
                dimensions.append(
                    Dimension(
                        id=dim_id_lower,
                        codelist_id=dim_to_cl_map.get(dim_id_lower),
                        position=i,
                    )
                )
            elif component.role == Role.ATTRIBUTE:
                attributes.append(
                    Attribute(
                        id=component.id.lower(),
                        codelist_id=component.enumeration.id
                        if component.enumeration
                        else None,
                    )
                )
            elif component.role == Role.MEASURE:
                primary_measure_id = component.id.lower()

        return DSD(
            id=dsd_node.id.lower(),
            version=dsd_node.version,
            dimensions=sorted(dimensions, key=lambda d: d.position),
            attributes=attributes,
            primary_measure_id=primary_measure_id,
        )

    def _extract_codelist_map_from_xml(self, sdmx_path: Path) -> Dict[str, str]:
        """
        Parses an SDMX-ML DSD file to extract the mapping between dimension IDs
        and their associated codelist IDs.
        """
        import xml.etree.ElementTree as ET

        tree = ET.parse(sdmx_path)
        root = tree.getroot()
        ns = {
            "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
            "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
        }

        mapping = {}
        # Find all Dimension elements within the DataStructure
        for dim in root.findall(".//s:Dimension", ns):
            dim_id = dim.get("id")
            # Find the Codelist reference within the dimension
            codelist_ref = dim.find(".//s:Enumeration/c:Ref", ns)
            if dim_id and codelist_ref is not None:
                codelist_id = codelist_ref.get("id")
                if codelist_id:
                    mapping[dim_id.lower()] = codelist_id

        logger.debug(f"Extracted dimension-codelist map: {mapping}")
        return mapping

    def parse_codelist(self, sdmx_path: Path) -> Codelist:
        """Parses a Codelist from an SDMX file."""
        logger.info(f"Parsing Codelist from {sdmx_path} using pysdmx")
        message = read_sdmx(sdmx_path, validate=False)
        if not message.structures:
            raise ValueError("No structures found in the SDMX message")

        cl_node = message.structures[0]
        if not isinstance(cl_node, PysdmxCodelist):
            raise TypeError(f"Expected Codelist, but got {type(cl_node)}")

        codes = {}
        for item in cl_node.items:
            codes[item.id] = Code(
                id=item.id,
                name=item.name,
                description=item.description,
                parent_id=None,
            )

        return Codelist(
            id=cl_node.id,
            version=cl_node.version,
            codes=codes,
        )


CHUNK_SIZE = 100_000


class TsvParser:
    """
    Parses a Eurostat gzipped TSV file in a memory-efficient, streaming manner.
    """

    def __init__(self, tsv_path: Path):
        self.tsv_path = tsv_path

    def parse(
        self,
    ) -> Tuple[Iterator[pd.DataFrame], List[str], List[str]]:
        """
        Parses the TSV file, yielding chunks of data as pandas DataFrames.

        This method reads the header to determine the column structure and then
        streams the rest of the file in chunks to keep memory usage low.

        Returns:
            A tuple containing:
            - An iterator yielding wide-format DataFrames (chunks).
            - A list of the dimension column names.
            - A list of the time period column names.
        """
        # 1. Read header line to get dimension and time period columns
        with gzip.open(self.tsv_path, "rt", encoding="utf-8") as f:
            header_line = f.readline().strip()

        header_parts = header_line.split("\t")
        dim_header = header_parts[0]
        dimension_cols = [d.strip() for d in dim_header.split(",")]
        if dimension_cols and "\\" in dimension_cols[-1]:
            last_dim_parts = dimension_cols[-1].split("\\")
            dimension_cols[-1] = last_dim_parts[0]

        time_period_cols = [p.strip() for p in header_parts[1:]]

        # 2. Create a streaming reader (iterator) for the data
        df_iterator = pd.read_csv(
            self.tsv_path,
            compression="gzip",
            sep="\t",
            header=0,
            na_values=[": ", ":"],
            chunksize=CHUNK_SIZE,
        )

        # 3. Define a generator to process each chunk
        def chunk_processor(
            iterator: Iterator[pd.DataFrame],
        ) -> Generator[pd.DataFrame, None, None]:
            logger.info(f"Begin streaming chunks from {self.tsv_path}")
            for i, chunk in enumerate(iterator):
                # Rename the first column which contains all dimensions
                chunk.rename(
                    columns={chunk.columns[0]: "dimensions_combined"}, inplace=True
                )
                # Split the combined dimension column into separate columns
                dims_df = chunk["dimensions_combined"].str.split(",", expand=True)
                dims_df.columns = dimension_cols[: dims_df.shape[1]]

                # Combine the new dimension columns with the time period data
                processed_chunk = pd.concat([dims_df, chunk[time_period_cols]], axis=1)
                logger.debug(f"Processed chunk {i} with {len(processed_chunk)} rows.")
                yield processed_chunk
            logger.info("Finished streaming all chunks.")

        return chunk_processor(df_iterator), dimension_cols, time_period_cols


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
            with open(self.toc_path, "r", encoding="utf-8-sig") as f:
                # Skip header line
                next(f, None)
                for line in f:
                    # Strip quotes and whitespace from each part
                    parts = [p.strip().strip('"') for p in line.strip().split("\t")]
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
                        "url": f"https://ec.europa.eu/eurostat/api/dissemination{url_part}",
                        "last_update": pd.to_datetime(parts[3], utc=True),
                    }
            logger.info(
                f"Successfully parsed {len(self._toc_data)} dataset entries from TOC."
            )
        except FileNotFoundError:
            logger.error(f"TOC file not found at {self.toc_path}")
            raise
        except Exception as e:
            logger.error(
                f"Failed to parse TOC file {self.toc_path}: {e}", exc_info=True
            )

    def get_last_update_timestamp(self, dataset_id: str) -> Optional[datetime]:
        """
        Gets the last update timestamp for a specific dataset from the TOC.

        Args:
            dataset_id: The code of the dataset (e.g., 'nama_10_gdp').

        Returns:
            A timezone-aware datetime object or None if the dataset is not found.
        """
        dataset_info = self._toc_data.get(dataset_id.lower())
        return dataset_info["last_update"] if dataset_info else None

    def get_download_url(self, dataset_id: str) -> Optional[str]:
        """
        Gets the full download URL for a specific dataset from the TOC.

        Args:
            dataset_id: The code of the dataset (e.g., 'nama_10_gdp').

        Returns:
            The full download URL string or None if not found.
        """
        dataset_info = self._toc_data.get(dataset_id.lower())
        return dataset_info["url"] if dataset_info else None
