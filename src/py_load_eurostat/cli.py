"""
Command-line interface for the py-load-eurostat application.

This module uses Typer to create a CLI for running the ingestion pipeline.
"""

import logging

import typer
from typing_extensions import Annotated

# Create a Typer application
app = typer.Typer(
    name="py-load-eurostat",
    help="A CLI tool to download, transform, and load Eurostat data into a database.",
    add_completion=False,
)

@app.command()
def run(
    dataset_id: Annotated[
        str,
        typer.Option(
            ...,
            "--dataset-id",
            "-d",
            help="The Eurostat dataset identifier (e.g., 'nama_10_gdp').",
        ),
    ],
    representation: Annotated[
        str,
        typer.Option(
            "Standard",
            "--representation",
            "-r",
            help="The data representation: 'Standard' (coded) or 'Full' (labeled).",
        ),
    ],
    load_strategy: Annotated[
        str,
        typer.Option(
            "Full",
            "--load-strategy",
            "-s",
            help=(
                "The load strategy: 'Full' (replaces entire dataset) or 'Delta' "
                "(loads if source is newer)."
            ),
        ),
    ],
    use_unlogged_tables: Annotated[
        bool,
        typer.Option(
            True,
            "--use-unlogged-tables/--no-use-unlogged-tables",
            help=(
                "Enable/disable using UNLOGGED tables for staging in PostgreSQL. "
                "Overrides env var."
            ),
        ),
    ],
) -> None:
    """
    Run the full ingestion pipeline for a single Eurostat dataset.
    """
    # Import settings here to allow CLI to override them
    from .config import settings

    # Basic logging setup for the CLI
    log_format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)

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
        from .pipeline import run_pipeline

        run_pipeline(dataset_id, representation, load_strategy)
        typer.secho(
            f"Pipeline for {dataset_id} completed successfully.", fg=typer.colors.GREEN
        )
    except Exception as e:
        # The pipeline function will handle its own detailed error logging.
        # This is a final catch-all for the CLI.
        logging.error(f"A critical error occurred: {e}", exc_info=True)
        typer.secho(f"Pipeline for {dataset_id} failed.", fg=typer.colors.RED)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
