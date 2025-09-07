"""
Command-line interface for the eurostat-loader application.

This module uses Typer to create a CLI for running the ingestion pipeline.
"""
import logging
from typing_extensions import Annotated

import typer

# Create a Typer application
app = typer.Typer(
    name="eurostat-loader",
    help="A CLI tool to download, transform, and load Eurostat data into a database.",
    add_completion=False,
)

# Define common options as Annotated types for reuse and clarity
DatasetIDOption = typer.Option(
    ..., # ... means this is a required option
    "--dataset-id",
    "-d",
    help="The Eurostat dataset identifier (e.g., 'nama_10_gdp')."
)

RepresentationOption = typer.Option(
    "Standard",
    "--representation",
    "-r",
    help="The data representation: 'Standard' (coded) or 'Full' (labeled)."
)

LoadStrategyOption = typer.Option(
    "Full",
    "--load-strategy",
    "-s",
    help="The load strategy: 'Full' (replaces entire dataset) or 'Delta' (loads if source is newer)."
)

@app.command()
def run(
    dataset_id: Annotated[str, DatasetIDOption],
    representation: Annotated[str, RepresentationOption],
    load_strategy: Annotated[str, LoadStrategyOption],
):
    """
    Run the full ingestion pipeline for a single Eurostat dataset.
    """
    # Basic logging setup for the CLI
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    typer.echo(f"Starting pipeline for dataset: {dataset_id}")
    typer.echo(f"  - Representation: {representation}")
    typer.echo(f"  - Load Strategy: {load_strategy}")

    # Here we will call the main pipeline orchestrator
    # For now, this is a placeholder.
    try:
        from .pipeline import run_pipeline
        run_pipeline(dataset_id, representation, load_strategy)
        typer.secho(f"Pipeline for {dataset_id} completed successfully.", fg=typer.colors.GREEN)
    except Exception as e:
        # The pipeline function will handle its own detailed error logging.
        # This is a final catch-all for the CLI.
        logging.error(f"A critical error occurred: {e}", exc_info=True)
        typer.secho(f"Pipeline for {dataset_id} failed.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
