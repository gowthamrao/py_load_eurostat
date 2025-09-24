# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


"""
Command-line interface for the py-load-eurostat application.

This module uses Typer to create a CLI for running the ingestion pipeline.
"""

import logging

import typer

from .config import AppSettings
from .pipeline import run_batch_update, run_pipeline

# Create a Typer application
app = typer.Typer(
    name="py-load-eurostat",
    help="A CLI tool to download, transform, and load Eurostat data into a database.",
    add_completion=False,
)


@app.command()
def run(
    dataset_id: str = typer.Option(
        ...,
        "--dataset-id",
        "-d",
        help="The Eurostat dataset identifier (e.g., 'nama_10_gdp').",
    ),
    representation: str = typer.Option(
        "Standard",
        "--representation",
        "-r",
        help="The data representation: 'Standard' (coded) or 'Full' (labeled).",
    ),
    load_strategy: str = typer.Option(
        "Full",
        "--load-strategy",
        "-s",
        help=(
            "The load strategy: 'Full' (replaces entire dataset) or 'Delta' "
            "(loads if source is newer)."
        ),
    ),
    use_unlogged_tables: bool = typer.Option(
        True,
        "--use-unlogged-tables/--no-use-unlogged-tables",
        help=(
            "Enable/disable using UNLOGGED tables for staging in PostgreSQL. "
            "Overrides env var."
        ),
    ),
) -> None:
    """
    Run the full ingestion pipeline for a single Eurostat dataset.
    """
    # Basic logging setup for the CLI
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)

    # Instantiate settings here to ensure env vars are loaded correctly
    settings = AppSettings()

    # The CLI option takes precedence over the environment variable.
    # We directly override the setting that the pipeline will use.
    if use_unlogged_tables is not None:
        settings.db.use_unlogged_tables = use_unlogged_tables

    typer.echo(f"Starting pipeline for dataset: {dataset_id}")
    typer.echo(f"  - Representation: {representation}")
    typer.echo(f"  - Load Strategy: {load_strategy}")
    typer.echo(f"  - Use UNLOGGED tables: {settings.db.use_unlogged_tables}")

    # Here we will call the main pipeline orchestrator
    try:
        run_pipeline(dataset_id, representation, load_strategy, settings=settings)
        typer.secho(
            f"Pipeline for {dataset_id} completed successfully.", fg=typer.colors.GREEN
        )
    except Exception as e:
        # The pipeline function will handle its own detailed error logging.
        # This is a final catch-all for the CLI.
        logging.error(f"A critical error occurred: {e}", exc_info=True)
        typer.secho(f"Pipeline for {dataset_id} failed.", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@app.command()
def update_all() -> None:
    """
    Run the ingestion pipeline for all managed datasets.

    This command reads the list of datasets from the file specified by the
    `managed_datasets_path` setting (or PY_LOAD_EUROSTAT_MANAGED_DATASETS_PATH
    environment variable). It then checks each dataset for updates and runs
    the pipeline only for those that are new or have been updated.
    """
    # Basic logging setup for the CLI
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)

    # Instantiate settings here
    settings = AppSettings()

    typer.echo(
        "Starting batch update for all managed datasets from file: "
        f"'{settings.managed_datasets_path}'"
    )
    try:
        run_batch_update(settings.managed_datasets_path, settings=settings)
        typer.secho("Batch update process completed.", fg=typer.colors.GREEN)
    except FileNotFoundError:
        typer.secho(
            "Error: Managed datasets file not found at "
            f"'{settings.managed_datasets_path}'.",
            fg=typer.colors.RED,
        )
        typer.echo(
            "Please create this file or set the "
            "PY_LOAD_EUROSTAT_MANAGED_DATASETS_PATH environment variable."
        )
        raise typer.Exit(code=1)
    except Exception as e:
        logging.error(
            f"A critical error occurred during batch update: {e}", exc_info=True
        )
        typer.secho("Batch update process failed.", fg=typer.colors.RED)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
