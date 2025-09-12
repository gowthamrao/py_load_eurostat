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
from pysdmx.errors import Invalid
from pysdmx.io import read_sdmx
from pysdmx.model.code import Codelist as PysdmxCodelist
from pysdmx.model.dataflow import (
    DataStructureDefinition as PysdmxDSD,
)
from pysdmx.model.dataflow import (
    Role,
)

from .models import DSD, Attribute, Code, Codelist, Dimension, Measure

logger = logging.getLogger(__name__)


class SdmxParser:
    """Parses SDMX-ML files for DSDs and Codelists using the pysdmx library."""

    def parse_dsd_from_dataflow(self, sdmx_path: Path) -> DSD:
        """Parses a DSD from a dataflow SDMX file."""
        logger.info(f"Parsing DSD from {sdmx_path} using pysdmx")
        try:
            message = read_sdmx(sdmx_path, validate=False)
        except (Invalid, KeyError) as e:
            # Catching KeyError for pysdmx internal issues with wrong file types
            logger.error(f"pysdmx failed to parse {sdmx_path}: {e}")
            raise ValueError(
                f"Failed to parse SDMX file {sdmx_path}. "
                "It may be empty, malformed, or not a DSD."
            ) from e

        if not message.structures:
            raise ValueError("No structures found in the SDMX message")

        dsd_node = next(
            (s for s in message.structures if isinstance(s, PysdmxDSD)), None
        )

        if not dsd_node:
            if hasattr(message, "dataflow") and message.dataflow:
                dataflow = list(message.dataflow.values())[0]
                dsd_node = dataflow.structure
            else:
                pass

        if not isinstance(dsd_node, PysdmxDSD):
            raise TypeError(
                "Could not find a valid DataStructureDefinition in the SDMX message."
            )

        dimensions: list[Dimension] = []
        attributes: list[Attribute] = []
        measures: list[Measure] = []
        primary_measure_id = "obs_value"

        for i, component in enumerate(dsd_node.components):
            data_type = str(component.dtype) if component.dtype else "String"

            if component.role == Role.DIMENSION:
                dimensions.append(
                    Dimension(
                        id=component.id,
                        name=component.name,
                        codelist_id=component.enumeration.id
                        if component.enumeration
                        else None,
                        position=i,
                        data_type=data_type,
                    )
                )
            elif component.role == Role.ATTRIBUTE:
                attributes.append(
                    Attribute(
                        id=component.id,
                        name=component.name,
                        codelist_id=component.enumeration.id
                        if component.enumeration
                        else None,
                        data_type=data_type,
                    )
                )
            elif component.role == Role.MEASURE:
                primary_measure_id = component.id
                measures.append(
                    Measure(
                        id=primary_measure_id,
                        name=component.name,
                        data_type=data_type,
                    )
                )

        return DSD(
            id=dsd_node.id,
            name=dsd_node.name,
            version=dsd_node.version,
            dimensions=sorted(dimensions, key=lambda d: d.position),
            attributes=attributes,
            measures=measures,
            primary_measure_id=primary_measure_id,
        )

    def _extract_codelist_map_from_xml(self, sdmx_path: Path) -> Dict[str, str]:
        import xml.etree.ElementTree as ET

        tree = ET.parse(sdmx_path)
        root = tree.getroot()
        ns = {
            "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
            "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
        }
        mapping = {}
        for dim in root.findall(".//s:Dimension", ns):
            dim_id = dim.get("id")
            codelist_ref = dim.find(".//s:Enumeration/c:Ref", ns)
            if dim_id and codelist_ref is not None:
                codelist_id = codelist_ref.get("id")
                if codelist_id:
                    mapping[dim_id.lower()] = codelist_id
        logger.debug(f"Extracted dimension-codelist map: {mapping}")
        return mapping

    def parse_codelist(self, sdmx_path: Path) -> Codelist:
        logger.info(f"Parsing Codelist from {sdmx_path} using pysdmx")
        try:
            message = read_sdmx(sdmx_path, validate=False)
        except (Invalid, KeyError) as e:
            logger.error(f"pysdmx failed to parse {sdmx_path}: {e}")
            raise ValueError(
                f"Failed to parse SDMX file {sdmx_path}. "
                "It may be empty, malformed, or not a codelist."
            ) from e

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
    def __init__(self, tsv_path: Path):
        self.tsv_path = tsv_path

    def parse(self) -> Tuple[Iterator[pd.DataFrame], List[str], List[str]]:
        with gzip.open(self.tsv_path, "rt", encoding="utf-8") as f:
            header_line = f.readline().strip()

        if "\t" not in header_line:
            raise ValueError(f"Invalid TSV header format: {header_line}")
        dim_header_part, time_header_part = header_line.split("\t", 1)

        if "\\" not in dim_header_part:
            raise ValueError(f"Invalid TSV header format: {header_line}")

        dims_only_str = dim_header_part.split("\\")[0]
        dimension_cols = [d.strip() for d in dims_only_str.split(",")]
        time_period_cols = [p.strip() for p in time_header_part.split("\t")]

        df_iterator = pd.read_csv(
            self.tsv_path,
            compression="gzip",
            sep="\t",
            header=0,
            na_values=[": ", ":"],
            chunksize=CHUNK_SIZE,
        )

        def chunk_processor(
            iterator: Iterator[pd.DataFrame],
        ) -> Generator[pd.DataFrame, None, None]:
            logger.info(f"Begin streaming chunks from {self.tsv_path}")
            for i, chunk in enumerate(iterator):
                chunk.rename(
                    columns={chunk.columns[0]: "dimensions_combined"}, inplace=True
                )

                def parse_eurostat_dims(dim_string: str) -> list[str]:
                    import csv
                    from io import StringIO

                    if not isinstance(dim_string, str):
                        return [None] * len(dimension_cols)
                    return next(csv.reader(StringIO(dim_string)))

                parsed_dims = chunk["dimensions_combined"].apply(parse_eurostat_dims)
                dims_df = pd.DataFrame(
                    parsed_dims.tolist(),
                    index=chunk.index,
                    columns=dimension_cols,
                )
                processed_chunk = pd.concat([dims_df, chunk[time_period_cols]], axis=1)
                logger.debug(f"Processed chunk {i} with {len(processed_chunk)} rows.")
                yield processed_chunk
            logger.info("Finished streaming all chunks.")

        return chunk_processor(df_iterator), dimension_cols, time_period_cols


class InventoryParser:
    """
    Parses the Eurostat data inventory file.
    """

    def __init__(self, inventory_path: Path):
        self.inventory_path = inventory_path
        self._inventory_data: Dict[str, Dict] = {}
        self._load_inventory()

    def _load_inventory(self) -> None:
        logger.info(f"Loading and parsing inventory file: {self.inventory_path}")
        try:
            # Use pandas for robust TSV parsing
            df = pd.read_csv(self.inventory_path, sep="\t", header=0)

            # Rename columns for easier access
            df.columns = [col.strip() for col in df.columns]
            df = df.rename(
                columns={
                    "Code": "code",
                    "Last data change": "last_update",
                    "Data download url (tsv)": "download_url",
                }
            )

            # Filter for datasets and required columns
            df = df[df["Type"] == "DATASET"][["code", "last_update", "download_url"]]
            df = df.dropna(subset=["code", "last_update", "download_url"])

            # Convert last_update to timezone-aware datetime objects
            df["last_update"] = pd.to_datetime(df["last_update"], utc=True)

            # Set the dataset code as the index for fast lookups
            df = df.set_index(df["code"].str.lower())

            self._inventory_data = df.to_dict("index")
            logger.info(
                f"Successfully parsed {len(self._inventory_data)} "
                "dataset entries from inventory."
            )
        except FileNotFoundError:
            logger.error(f"Inventory file not found at {self.inventory_path}")
            raise
        except Exception as e:
            logger.error(
                f"Failed to parse inventory file {self.inventory_path}: {e}",
                exc_info=True,
            )
            raise

    def get_last_update_timestamp(self, dataset_id: str) -> Optional[datetime]:
        dataset_info = self._inventory_data.get(dataset_id.lower())
        return dataset_info["last_update"] if dataset_info else None

    def get_download_url(self, dataset_id: str) -> Optional[str]:
        dataset_info = self._inventory_data.get(dataset_id.lower())
        return dataset_info["download_url"] if dataset_info else None
