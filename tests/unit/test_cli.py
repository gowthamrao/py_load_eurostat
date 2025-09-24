# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import pytest
from typer.testing import CliRunner

from py_load_eurostat.cli import app


# Fixture for the CLI runner
@pytest.fixture
def runner():
    return CliRunner()


# Mock the pipeline and batch update functions to avoid running the full process
@pytest.fixture(autouse=True)
def mock_pipeline_functions(mocker):
    mocker.patch("py_load_eurostat.cli.run_pipeline", return_value=None)
    mocker.patch("py_load_eurostat.cli.run_batch_update", return_value=None)


def test_run_command_success(runner, mock_pipeline_functions):
    """Test the 'run' command with valid arguments."""
    result = runner.invoke(
        app,
        [
            "run",
            "--dataset-id",
            "test_dataset",
            "--representation",
            "Full",
            "--load-strategy",
            "Delta",
        ],
    )
    assert result.exit_code == 0
    assert "Starting pipeline for dataset: test_dataset" in result.stdout
    assert "Representation: Full" in result.stdout
    assert "Load Strategy: Delta" in result.stdout
    assert "Pipeline for test_dataset completed successfully." in result.stdout


def test_run_command_failure(runner, mocker):
    """Test the 'run' command when the pipeline raises an exception."""
    mocker.patch(
        "py_load_eurostat.cli.run_pipeline",
        side_effect=Exception("Pipeline error"),
    )
    result = runner.invoke(app, ["run", "--dataset-id", "fail_dataset"])
    assert result.exit_code == 1
    assert "Pipeline for fail_dataset failed." in result.stdout


def test_update_all_command_success(runner, mock_pipeline_functions):
    """Test the 'update-all' command."""
    result = runner.invoke(app, ["update-all"])
    assert result.exit_code == 0
    assert "Starting batch update for all managed datasets" in result.stdout
    assert "Batch update process completed." in result.stdout


def test_update_all_command_file_not_found(runner, mocker):
    """Test the 'update-all' command when the managed datasets file is not found."""
    mocker.patch(
        "py_load_eurostat.cli.run_batch_update",
        side_effect=FileNotFoundError("File not found"),
    )
    result = runner.invoke(app, ["update-all"])
    assert result.exit_code == 1
    assert "Error: Managed datasets file not found" in result.stdout


def test_update_all_command_generic_failure(runner, mocker):
    """Test the 'update-all' command with a generic exception."""
    mocker.patch(
        "py_load_eurostat.cli.run_batch_update",
        side_effect=Exception("Generic error"),
    )
    result = runner.invoke(app, ["update-all"])
    assert result.exit_code == 1
    assert "Batch update process failed." in result.stdout


def test_run_with_unlogged_tables_option(runner, mocker):
    """Test that the --use-unlogged-tables flag correctly overrides the setting."""
    mock_run_pipeline = mocker.patch("py_load_eurostat.cli.run_pipeline")

    # Test with the flag enabled
    result_with_flag = runner.invoke(
        app, ["run", "--dataset-id", "test_ds", "--use-unlogged-tables"]
    )
    assert result_with_flag.exit_code == 0
    assert "Use UNLOGGED tables: True" in result_with_flag.stdout

    # Check if the settings object passed to the pipeline has the correct value
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs["settings"].db.use_unlogged_tables is True

    # Test with the flag disabled
    result_without_flag = runner.invoke(
        app, ["run", "--dataset-id", "test_ds", "--no-use-unlogged-tables"]
    )
    assert result_without_flag.exit_code == 0
    assert "Use UNLOGGED tables: False" in result_without_flag.stdout

    # Check the settings object again
    _, kwargs = mock_run_pipeline.call_args
    assert kwargs["settings"].db.use_unlogged_tables is False
