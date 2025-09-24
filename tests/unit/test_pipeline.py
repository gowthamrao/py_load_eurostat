from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from py_load_eurostat.config import AppSettings
from py_load_eurostat.pipeline import run_batch_update, run_pipeline


@pytest.fixture
def mock_settings(tmp_path):
    """Fixture for AppSettings."""
    return AppSettings(cache={"path": tmp_path, "enabled": True})


@patch("py_load_eurostat.pipeline.get_loader")
@patch("py_load_eurostat.pipeline.Fetcher")
@patch("py_load_eurostat.pipeline.InventoryParser")
@patch("py_load_eurostat.pipeline.run_pipeline")
def test_run_batch_update_file_not_found(
    mock_run_pipeline,
    mock_inventory_parser,
    mock_fetcher,
    mock_get_loader,
    mock_settings,
):
    """Test that FileNotFoundError is raised if the datasets file does not exist."""
    with pytest.raises(FileNotFoundError):
        run_batch_update(Path("non_existent_file.yml"), mock_settings)


@patch("py_load_eurostat.pipeline.get_loader")
@patch("py_load_eurostat.pipeline.Fetcher")
@patch("py_load_eurostat.pipeline.InventoryParser")
@patch("py_load_eurostat.pipeline.run_pipeline")
def test_run_batch_update_empty_datasets_file(
    mock_run_pipeline,
    mock_inventory_parser,
    mock_fetcher,
    mock_get_loader,
    mock_settings,
    tmp_path,
):
    """Test that the function exits gracefully if the datasets file is empty."""
    datasets_file = tmp_path / "empty.yml"
    with open(datasets_file, "w") as f:
        yaml.dump({"datasets": []}, f)
    run_batch_update(datasets_file, mock_settings)
    mock_run_pipeline.assert_not_called()


@patch("py_load_eurostat.pipeline.get_loader")
@patch("py_load_eurostat.pipeline.Fetcher")
@patch("py_load_eurostat.pipeline.InventoryParser")
@patch("py_load_eurostat.pipeline.run_pipeline")
def test_run_batch_update_dataset_not_found(
    mock_run_pipeline,
    mock_inventory_parser,
    mock_fetcher,
    mock_get_loader,
    mock_settings,
    tmp_path,
):
    """Test that a dataset not found in the inventory is skipped."""
    datasets_file = tmp_path / "datasets.yml"
    with open(datasets_file, "w") as f:
        yaml.dump({"datasets": ["not_found_dataset"]}, f)

    mock_inventory_parser.return_value.get_last_update_timestamp.return_value = None

    run_batch_update(datasets_file, mock_settings)
    mock_run_pipeline.assert_not_called()


@patch("py_load_eurostat.pipeline.get_loader")
@patch("py_load_eurostat.pipeline.Fetcher")
@patch("py_load_eurostat.pipeline.InventoryParser")
@patch("py_load_eurostat.pipeline.run_pipeline")
def test_run_batch_update_exception_handling(
    mock_run_pipeline,
    mock_inventory_parser,
    mock_fetcher,
    mock_get_loader,
    mock_settings,
    tmp_path,
):
    """Test that an exception during dataset processing is handled."""
    datasets_file = tmp_path / "datasets.yml"
    with open(datasets_file, "w") as f:
        yaml.dump({"datasets": ["failing_dataset"]}, f)

    mock_inventory_parser.return_value.get_last_update_timestamp.return_value = (
        "2024-01-01"
    )
    mock_run_pipeline.side_effect = Exception("Pipeline failed")

    # This should not raise an exception
    run_batch_update(datasets_file, mock_settings)


@patch("py_load_eurostat.pipeline.get_loader")
@patch("py_load_eurostat.pipeline.Fetcher")
@patch("py_load_eurostat.pipeline.InventoryParser")
def test_run_pipeline_dataset_not_in_inventory(
    mock_inventory_parser, mock_fetcher, mock_get_loader, mock_settings
):
    """Test that an error is raised if the dataset is not in the inventory."""
    mock_inventory_parser.return_value.get_last_update_timestamp.return_value = None
    mock_inventory_parser.return_value.get_download_url.return_value = None

    with pytest.raises(RuntimeError, match="Could not find dataset"):
        run_pipeline("test_ds", "Standard", "Full", mock_settings)


@patch("py_load_eurostat.pipeline.get_loader")
@patch("py_load_eurostat.pipeline.Fetcher")
@patch("py_load_eurostat.pipeline.InventoryParser")
@patch("py_load_eurostat.pipeline.SdmxParser")
@patch("py_load_eurostat.pipeline.TsvParser")
@patch("py_load_eurostat.pipeline.Transformer")
def test_run_pipeline_save_state_fails(
    mock_transformer,
    mock_tsv_parser,
    mock_sdmx_parser,
    mock_inventory_parser,
    mock_fetcher,
    mock_get_loader,
    mock_settings,
):
    """Test that a failure to save the ingestion state is logged."""
    mock_loader = MagicMock()
    mock_loader.save_ingestion_state.side_effect = Exception("DB error")
    mock_get_loader.return_value = mock_loader

    # Mock other components to run through the pipeline
    mock_inventory_parser.return_value.get_last_update_timestamp.return_value = (
        "2024-01-01"
    )
    mock_inventory_parser.return_value.get_download_url.return_value = (
        "http://example.com"
    )
    mock_tsv_parser.return_value.parse.return_value = (iter([]), [], [])
    mock_loader.bulk_load_staging.return_value = ("staging_table", 0)

    with patch("py_load_eurostat.pipeline.logger.error") as mock_logger_error:
        run_pipeline("test_ds", "Standard", "Full", mock_settings)
        mock_logger_error.assert_called_once_with(
            "CRITICAL: Failed to save final ingestion state: DB error"
        )
