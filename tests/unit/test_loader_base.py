# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


from typing import Dict, Generator, Optional, Tuple

from py_load_eurostat.loader.base import LoaderInterface
from py_load_eurostat.models import DSD, Codelist, IngestionHistory, Observation


class ConcreteLoader(LoaderInterface):
    """A concrete implementation of LoaderInterface for testing purposes."""

    def prepare_schema(
        self,
        dsd: DSD,
        table_name: str,
        schema: str,
        representation: str,
        meta_schema: str,
        last_ingestion: Optional[IngestionHistory] = None,
    ) -> None:
        pass

    def manage_codelists(self, codelists: Dict[str, Codelist], schema: str) -> None:
        pass

    def bulk_load_staging(
        self,
        table_name: str,
        schema: str,
        data_stream: Generator[Observation, None, None],
        use_unlogged_table: bool = True,
    ) -> Tuple[str, int]:
        return "staging_table", 0

    def finalize_load(
        self, staging_table: str, target_table: str, schema: str, strategy: str
    ) -> None:
        pass

    def get_ingestion_state(
        self, dataset_id: str, schema: str
    ) -> Optional[IngestionHistory]:
        return None

    def save_ingestion_state(
        self, history_record: IngestionHistory, schema: str
    ) -> None:
        pass

    def close_connection(self) -> None:
        pass


def test_concrete_loader_instantiation_and_method_calls():
    """
    Test that a concrete implementation of LoaderInterface can be instantiated
    and that its methods can be called without raising errors.
    """
    loader = ConcreteLoader()
    assert isinstance(loader, LoaderInterface)

    # Create valid model instances
    dsd = DSD(id="test_dsd", version="1.0", dimensions=[], attributes=[])
    history_record = IngestionHistory(
        dataset_id="test_dataset",
        load_strategy="Full",
        representation="Standard",
        dsd_hash="somehash",
    )

    # Call each method to ensure it's "covered"
    loader.prepare_schema(dsd, "table", "schema", "rep", "meta_schema")
    loader.manage_codelists({}, "schema")
    result = loader.bulk_load_staging("table", "schema", (i for i in []))
    assert result == ("staging_table", 0)
    loader.finalize_load("staging", "target", "schema", "strategy")
    ingestion_state = loader.get_ingestion_state("dataset", "schema")
    assert ingestion_state is None
    loader.save_ingestion_state(history_record, "schema")
    loader.close_connection()


def test_incomplete_loader_raises_type_error():
    """
    Tests that instantiating a class that inherits from LoaderInterface but
    does not implement all abstract methods raises a TypeError.
    """
    import pytest

    with pytest.raises(TypeError) as excinfo:

        class IncompleteLoader(LoaderInterface):
            def prepare_schema(self, dsd, table_name, schema, rep, meta_schema) -> None:
                pass

        IncompleteLoader()

    assert "Can't instantiate abstract class" in str(excinfo.value)
