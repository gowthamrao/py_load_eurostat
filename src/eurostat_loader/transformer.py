"""
Transformer module for processing parsed Eurostat data.

This module takes the raw, parsed data from the `parser` module and
transforms it into a clean, normalized (tidy) format, ready for loading.
"""
import logging
import re
from pathlib import Path
from typing import Dict, Generator, Tuple, Optional

from .models import DSD, CodeList, Observation
from .parser import TsvParser

logger = logging.getLogger(__name__)

# Regex to separate a numeric value from optional trailing flags (non-digit characters)
# It handles integers, floats, and scientific notation.
VALUE_FLAG_RE = re.compile(r"^\s*(-?[\d.eE+-]+)\s*([a-zA-Z\s]*)\s*$")

class Transformer:
    """
    Transforms wide-format Eurostat TSV data into a long-format stream
    of Observation objects.
    """

    def __init__(self, dsd: DSD, codelists: Dict[str, CodeList]):
        """
        Initializes the Transformer.

        Args:
            dsd: The Data Structure Definition for the dataset being processed.
            codelists: A dictionary mapping codelist IDs to parsed CodeList objects.
                       This is required for the 'Full' representation.
        """
        self.dsd = dsd
        self.codelists = codelists
        # Create a map from dimension ID to the corresponding codelist for quick lookups
        self.dim_to_codelist_map = {
            dim.id: self.codelists[dim.codelist_id]
            for dim in self.dsd.dimensions
            if dim.codelist_id in self.codelists
        }

    def _parse_value(self, raw_value: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
        """
        Parses a raw observation string, separating the numeric value from flags.

        Example: "123.45 p" -> (123.45, "p")
                 " - " -> (None, "-")
                 "123.45" -> (123.45, None)

        Args:
            raw_value: The string value from the TSV file.

        Returns:
            A tuple containing the numeric value (or None) and flags (or None).
        """
        if raw_value is None or pd.isna(raw_value):
            return None, None

        raw_value = str(raw_value).strip()
        if not raw_value:
            return None, None

        match = VALUE_FLAG_RE.match(raw_value)
        if match:
            try:
                value = float(match.group(1))
                flags = match.group(2).strip() or None
                return value, flags
            except (ValueError, IndexError):
                # This can happen if the numeric part is not a valid float.
                # In this case, we treat the whole string as flags.
                return None, raw_value
        else:
            # If no numeric part is found, the whole string is considered flags.
            return None, raw_value

    def transform(
        self, tsv_path: Path, representation: str = "Standard"
    ) -> Generator[Observation, None, None]:
        """
        Creates a generator that yields transformed Observation objects.

        This method streams the data from the TSV file, unpivots it,
        and transforms it into a clean, long format.

        Args:
            tsv_path: Path to the gzipped TSV data file.
            representation: The desired output format, "Standard" (coded) or
                            "Full" (labeled).

        Yields:
            A stream of Observation Pydantic models.
        """
        logger.info(f"Starting transformation for {tsv_path} with '{representation}' representation.")
        parser = TsvParser(tsv_path)

        for raw_row in parser:
            # Extract the dimension values from the raw row
            base_dimensions = {
                dim_id: raw_row.get(dim_id) for dim_id in parser.dimension_cols
            }

            # Unpivot: iterate through the time periods in the raw row
            for time_period in parser.time_period_cols:
                raw_value = raw_row.get(time_period)

                # Skip if the value is missing
                if raw_value is None:
                    continue

                obs_value, obs_flags = self._parse_value(raw_value)

                # If both value and flags are None after parsing, it's an empty cell
                if obs_value is None and obs_flags is None:
                    continue

                # Handle the data representation (Standard vs. Full)
                final_dimensions = {}
                if representation.lower() == "full":
                    for dim_id, code_val in base_dimensions.items():
                        codelist = self.dim_to_codelist_map.get(dim_id)
                        if codelist and code_val in codelist.codes:
                            final_dimensions[dim_id] = codelist.codes[code_val].name
                        else:
                            final_dimensions[dim_id] = code_val # Fallback to code
                else: # Standard representation
                    final_dimensions = base_dimensions

                yield Observation(
                    dimensions=final_dimensions,
                    time_period=time_period,
                    value=obs_value,
                    flags=obs_flags,
                )

        logger.info(f"Finished transformation for {tsv_path}.")
