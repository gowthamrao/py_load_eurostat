# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Core data models for the py-load-eurostat package.

This module defines Pydantic models that represent the core domain objects
of the application, such as SDMX metadata structures, code lists, and
observational data. These models are used as Data Transfer Objects (DTOs)
between the different layers of the pipeline.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

# === SDMX Metadata Models ===


class Code(BaseModel):
    """Represents a single code in an SDMX Code List."""

    id: str = Field(description="The unique identifier for the code (e.g., 'DE').")
    name: str = Field(
        description="The human-readable name of the code (e.g., 'Germany')."
    )
    description: Optional[str] = Field(
        default=None, description="An optional detailed description of the code."
    )
    parent_id: Optional[str] = Field(
        default=None,
        description="The ID of the parent code in a hierarchical code list.",
    )


class Codelist(BaseModel):
    """Represents an SDMX Codelist, a collection of codes for a dimension."""

    id: str = Field(
        description="The unique identifier for the codelist (e.g., 'CL_GEO')."
    )
    version: str = Field(description="The version of the code list.")
    codes: Dict[str, Code] = Field(
        description="A mapping from code IDs to Code objects."
    )


class Dimension(BaseModel):
    """Represents a dimension in an SDMX Data Structure Definition (DSD)."""

    id: str = Field(description="The unique ID of the dimension (e.g., 'GEO').")
    name: Optional[str] = Field(
        default=None, description="The human-readable name of the dimension."
    )
    codelist_id: Optional[str] = Field(
        default=None,
        description="The ID of the code list associated with this dimension, if any.",
    )
    position: int = Field(
        description="The order of the dimension in the dataset's key."
    )
    data_type: Optional[str] = Field(
        default=None,
        description="The SDMX data type of the dimension (e.g., 'String').",
    )


class Attribute(BaseModel):
    """Represents an attribute in an SDMX Data Structure Definition (DSD)."""

    id: str = Field(description="The unique ID of the attribute (e.g., 'OBS_FLAG').")
    name: Optional[str] = Field(
        default=None, description="The human-readable name of the attribute."
    )
    codelist_id: Optional[str] = Field(
        default=None, description="The ID of the code list for this attribute, if any."
    )
    data_type: Optional[str] = Field(
        default=None,
        description="The SDMX data type of the attribute (e.g., 'String').",
    )


class Measure(BaseModel):
    """Represents a measure in an SDMX Data Structure Definition (DSD)."""

    id: str = Field(description="The unique ID of the measure (e.g., 'OBS_VALUE').")
    name: Optional[str] = Field(
        default=None, description="The human-readable name of the measure."
    )
    data_type: Optional[str] = Field(
        default=None, description="The SDMX data type of the measure (e.g., 'Double')."
    )


class DSD(BaseModel):
    """Represents a Data Structure Definition (DSD)."""

    id: str = Field(description="The unique ID of the DSD.")
    name: Optional[str] = Field(
        default=None, description="The human-readable name of the DSD."
    )
    version: str = Field(description="The version of the DSD.")
    dimensions: List[Dimension] = Field(
        description="The list of dimensions in the DSD."
    )
    attributes: List[Attribute] = Field(
        description="The list of attributes in the DSD."
    )
    measures: List[Measure] = Field(
        default_factory=list, description="The list of measures in the DSD."
    )
    primary_measure_id: str = Field(
        default="OBS_VALUE",
        description="The ID of the primary measure (observation value).",
    )


# === Observational Data Models ===


class Observation(BaseModel):
    """
    Represents a single observation in a tidy (long) format dataset.
    The dimension values are stored in a flexible dictionary.
    """

    dimensions: Dict[str, Optional[str]] = Field(
        description="A dictionary mapping dimension IDs to their code values."
    )
    time_period: str = Field(
        description="The time period for the observation (e.g., '2023' or '2023-Q1')."
    )
    value: Optional[float] = Field(description="The numeric observation value.")
    flags: Optional[str] = Field(
        description="A string containing any flags associated with the observation."
    )


# === Ingestion History Models ===


class IngestionStatus(str, Enum):
    """Enum for the status of an ingestion process."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class IngestionHistory(BaseModel):
    """
    Represents a record in the ingestion history table.
    Corresponds to the schema defined in FRD section 4.1.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    ingestion_id: Optional[int] = Field(
        default=None, description="Primary key for the ingestion record."
    )
    dataset_id: str = Field(
        description="The Eurostat dataset identifier (e.g., 'nama_10_gdp')."
    )
    dsd_version: Optional[str] = Field(
        default=None, description="The version of the DSD used for this load."
    )
    load_strategy: str = Field(
        description="The load strategy used, e.g., 'FULL' or 'DELTA'."
    )
    representation: str = Field(
        description="The data representation, e.g., 'STANDARD' or 'FULL'."
    )
    status: IngestionStatus = Field(
        default=IngestionStatus.PENDING,
        description="The current status of the ingestion.",
    )
    start_time: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="The start time of the ingestion process.",
    )
    end_time: Optional[datetime] = Field(
        default=None, description="The end time of the ingestion process."
    )
    rows_loaded: Optional[int] = Field(
        default=None, description="The total number of observation rows loaded."
    )
    source_last_update: Optional[datetime] = Field(
        default=None,
        description="The 'last_update' timestamp from the Eurostat source.",
    )
    error_details: Optional[str] = Field(
        default=None, description="Detailed error information if the ingestion failed."
    )
