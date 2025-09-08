"""
Base interface for all database loaders.

This module defines the Abstract Base Class (ABC) for loaders,
establishing a contract that all concrete database loader implementations
must adhere to.
"""
from abc import ABC, abstractmethod
from typing import Dict, Generator, Optional, Tuple

from ..models import DSD, Codelist, IngestionHistory, Observation


class LoaderInterface(ABC):
    """
    Abstract interface for a database loader.
    """

    @abstractmethod
    def prepare_schema(self, dsd: DSD, table_name: str, schema: str) -> None:
        """
        Ensures the required database schema and tables exist.
        This includes the main data table, metadata tables, and the
        ingestion history table. This method must be idempotent.

        Args:
            dsd: The Data Structure Definition of the dataset.
            table_name: The name of the target table for the dataset.
            schema: The database schema to create tables in.
        """
        pass

    @abstractmethod
    def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
        """
        Bulk loads or updates SDMX Code Lists into the database.

        Args:
            codelists: A dictionary of Codelist objects to load.
            schema: The database schema for metadata tables.
        """
        pass

    @abstractmethod
    def bulk_load_staging(
        self,
        table_name: str,
        schema: str,
        data_stream: Generator[Observation, None, None],
        use_unlogged_table: bool = True
    ) -> Tuple[str, int]:
        """
        Loads a stream of data into a new staging table using a native,
        high-performance bulk loading method (e.g., COPY).

        Args:
            table_name: The base name for the target table.
            schema: The database schema to create the staging table in.
            data_stream: A generator yielding Observation objects.
            use_unlogged_table: If the database supports it, use an unlogged
                                table for maximum ingestion speed.

        Returns:
            A tuple containing:
                - The name of the created staging table.
                - The number of rows loaded.
        """
        pass

    @abstractmethod
    def finalize_load(self, staging_table: str, target_table: str, schema: str) -> None:
        """
        Atomically replaces the data in the target table with the data
        from the staging table within a transaction.

        Args:
            staging_table: The name of the staging table.
            target_table: The name of the final target table.
            schema: The database schema where the tables reside.
        """
        pass

    @abstractmethod
    def get_ingestion_state(
        self, dataset_id: str, schema: str
    ) -> Optional[IngestionHistory]:
        """
        Retrieves the most recent successful IngestionHistory record for a given
        dataset.

        Args:
            dataset_id: The ID of the dataset to check.
            schema: The database schema for the history table.

        Returns:
            An IngestionHistory object or None if no record exists.
        """
        pass

    @abstractmethod
    def save_ingestion_state(
        self, history_record: IngestionHistory, schema: str
    ) -> None:
        """
        Saves or updates an IngestionHistory record in the database.

        Args:
            history_record: The IngestionHistory object to save.
            schema: The database schema for the history table.
        """
        pass

    @abstractmethod
    def close_connection(self) -> None:
        """
        Closes any open database connections.
        """
        pass
