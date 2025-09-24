"""
Integration tests for the batch update processing feature.

This test validates the `update-all` command and the underlying
`run_batch_update` function, ensuring it correctly identifies and
processes datasets that require updates.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from testcontainers.postgres import PostgresContainer

from py_load_eurostat.config import AppSettings, DatabaseSettings
from py_load_eurostat.loader.postgresql import PostgresLoader
from py_load_eurostat.models import IngestionHistory, IngestionStatus
from py_load_eurostat.pipeline import run_batch_update

# Pytest marker for all tests in this file
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    """Spins up a PostgreSQL container for the test module."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def db_settings(postgres_container: PostgresContainer) -> DatabaseSettings:
    """Provides DatabaseSettings for the running test container."""
    return DatabaseSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        name=postgres_container.dbname,
    )


@pytest.fixture
def managed_datasets_file(tmp_path: Path) -> Path:
    """Creates a temporary managed_datasets.yml file."""
    datasets = {
        "datasets": [
            "DS_UP_TO_DATE",  # This one should be skipped
            "DS_OUTDATED",  # This one should be updated
            "DS_NEW",  # This one should be updated
            "DS_NOT_IN_REMOTE",  # This one should be skipped/fail
        ]
    }
    file_path = tmp_path / "managed_datasets.yml"
    with open(file_path, "w") as f:
        yaml.dump(datasets, f)
    return file_path


@pytest.fixture
def mock_inventory_file(tmp_path: Path) -> Path:
    """
    Creates a mock inventory TSV file that mimics the real format from Eurostat.
    """
    now = datetime.now(timezone.utc)
    header = (
        "Code\tType\tSource dataset\tLast data change\t"
        "Last structural change\tData download url (tsv)"
    )
    up_to_date_ts = (now - timedelta(days=2)).isoformat()
    now_ts = now.isoformat()
    lines = [
        header,
        (
            f"DS_UP_TO_DATE\tDATASET\t-\t{up_to_date_ts}\t"
            "2024-01-01T00:00:00Z\t/data/DS_UP_TO_DATE.tsv.gz"
        ),
        (
            f"DS_OUTDATED\tDATASET\t-\t{now_ts}\t"
            "2024-01-01T00:00:00Z\t/data/DS_OUTDATED.tsv.gz"
        ),
        (f"DS_NEW\tDATASET\t-\t{now_ts}\t2024-01-01T00:00:00Z\t/data/DS_NEW.tsv.gz"),
        (f"DS_TABLE\tTABLE\t-\t{now_ts}\t2024-01-01T00:00:00Z\t/data/DS_TABLE.tsv.gz"),
    ]
    inventory_content = "\n".join(lines)
    file_path = tmp_path / "mock_inventory.tsv"
    file_path.write_text(inventory_content)
    return file_path


def setup_database_state(db_settings: DatabaseSettings):
    """Pre-populates the database with ingestion history."""
    loader = PostgresLoader(db_settings)
    schema = "eurostat_meta"
    now = datetime.now(timezone.utc)

    # Dataset that is up-to-date
    history_up_to_date = IngestionHistory(
        dataset_id="DS_UP_TO_DATE",
        status=IngestionStatus.SUCCESS,
        source_last_update=now - timedelta(days=1),  # Remote is 2 days old
        start_time=now,
        end_time=now,
        load_strategy="Delta",
        representation="Standard",
    )

    # Dataset that is outdated
    history_outdated = IngestionHistory(
        dataset_id="DS_OUTDATED",
        status=IngestionStatus.SUCCESS,
        source_last_update=now - timedelta(days=5),  # Remote is now
        start_time=now,
        end_time=now,
        load_strategy="Delta",
        representation="Standard",
    )

    try:
        loader.save_ingestion_state(history_up_to_date, schema)
        loader.save_ingestion_state(history_outdated, schema)
    finally:
        loader.close_connection()


@patch("py_load_eurostat.pipeline.run_pipeline")
@patch("py_load_eurostat.fetcher.Fetcher.get_toc")
def test_run_batch_update_logic(
    mock_get_toc,
    mock_run_pipeline,
    db_settings,
    managed_datasets_file,
    mock_inventory_file,
):
    """
    Tests the `run_batch_update` logic, verifying it calls the pipeline
    for the correct datasets based on their ingestion history.
    """
    # Arrange
    setup_database_state(db_settings)
    mock_get_toc.return_value = mock_inventory_file

    # Create a settings object specifically for this test
    test_settings = AppSettings()
    test_settings.db = db_settings
    test_settings.managed_datasets_path = managed_datasets_file

    # Act
    run_batch_update(managed_datasets_file, settings=test_settings)

    # Assert
    # Verify that run_pipeline was called correctly
    assert mock_run_pipeline.call_count == 2

    # Call args are tuples, so we check them like this
    call_args_list = mock_run_pipeline.call_args_list

    # Check call for the outdated dataset
    called_with_outdated = any(
        call.kwargs["dataset_id"] == "DS_OUTDATED"
        and call.kwargs["load_strategy"] == "Delta"
        for call in call_args_list
    )
    assert called_with_outdated, "run_pipeline was not called for DS_OUTDATED"

    # Check call for the new dataset
    called_with_new = any(
        call.kwargs["dataset_id"] == "DS_NEW"
        and call.kwargs["load_strategy"] == "Delta"
        for call in call_args_list
    )
    assert called_with_new, "run_pipeline was not called for DS_NEW"
