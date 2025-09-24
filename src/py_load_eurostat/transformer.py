# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Transformer module for processing parsed Eurostat data.

This module takes the raw, parsed data from the `parser` module and
transforms it into a clean, normalized (tidy) format, ready for loading.
"""

import logging
import re
from typing import Dict, Generator, Optional, Tuple

import pandas as pd

from .models import DSD, Codelist, Observation

logger = logging.getLogger(__name__)

# Regex to separate a numeric value from optional trailing flags (non-digit characters)
# It handles integers, floats, and scientific notation.
VALUE_FLAG_RE = re.compile(r"^\s*(-?[\d.eE+-]+)\s*([a-zA-Z\s]*)\s*$")


class Transformer:
    """
    Transforms wide-format Eurostat TSV data into a long-format stream
    of Observation objects.
    """

    def __init__(self, dsd: DSD, codelists: Dict[str, Codelist]):
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

    def _parse_value(
        self, raw_value: Optional[str]
    ) -> Tuple[Optional[float], Optional[str]]:
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
        self,
        wide_df_iterator: Generator[pd.DataFrame, None, None],
        dimension_cols: list[str],
        time_period_cols: list[str],
        representation: str = "Standard",
    ) -> Generator[Observation, None, None]:
        """
        Transforms a stream of wide-format DataFrames into a generator of
        Observation objects.

        This method processes each chunk from the iterator, performs the
        unpivot (melt) operation, and then applies final transformations
        like value/flag parsing and code-to-label replacement.

        Args:
            wide_df_iterator: An iterator yielding wide-format DataFrames (chunks).
            dimension_cols: A list of the dimension column names.
            time_period_cols: A list of the time period column names.
            representation: The desired output format, "Standard" or "Full".

        Yields:
            A stream of Observation Pydantic models.
        """
        logger.info(f"Starting transformation with '{representation}' representation.")

        for chunk in wide_df_iterator:
            # 1. Melt the chunk to transform from wide to long format
            long_df = chunk.melt(
                id_vars=dimension_cols,
                value_vars=time_period_cols,
                var_name="time_period",
                value_name="value",
            )

            # 2. Drop rows with missing values
            long_df.dropna(subset=["value"], inplace=True)

            # 3. Iterate over the long-format chunk to yield Observations
            for _, raw_obs in long_df.iterrows():
                obs_value, obs_flags = self._parse_value(raw_obs.get("value"))

                if obs_value is None and obs_flags is None:
                    continue

                base_dimensions = {
                    dim.id: raw_obs.get(dim.id) for dim in self.dsd.dimensions
                }

                if representation.lower() == "full":
                    final_dimensions = {}
                    for dim_id, code_val in base_dimensions.items():
                        codelist = self.dim_to_codelist_map.get(dim_id)
                        if codelist and code_val in codelist.codes:
                            final_dimensions[dim_id] = codelist.codes[code_val].name
                        else:
                            final_dimensions[dim_id] = code_val
                else:
                    final_dimensions = base_dimensions

                yield Observation(
                    dimensions=final_dimensions,
                    time_period=raw_obs.get("time_period"),
                    value=obs_value,
                    flags=obs_flags,
                )
        logger.info("Finished transformation.")
